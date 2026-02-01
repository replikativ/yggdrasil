(ns yggdrasil.adapters.iceberg
  "Apache Iceberg adapter for Yggdrasil protocols.

  Maps Iceberg concepts to yggdrasil:
    - Iceberg table = system workspace
    - Iceberg branch = yggdrasil branch
    - Iceberg snapshot = yggdrasil snapshot
    - Table properties/metadata = snapshot metadata

  Iceberg provides:
    - Snapshot isolation via metadata pointers
    - Native branching and tagging (v0.14+)
    - Snapshot lineage via parent pointers
    - Fast-forward merge and cherry-pick
    - REST catalog API for language-agnostic access

  Requirements:
    - Iceberg REST catalog server running
    - S3-compatible storage (MinIO, AWS S3, etc.)
    - Spark or other query engine for data access (optional)

  This adapter uses the REST catalog API directly (no Java interop needed)."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.watcher :as w]
            [clojure.string :as str]
            [clojure.set :as set]
            [cheshire.core :as json])
  (:import [java.net.http HttpClient HttpRequest HttpRequest$BodyPublishers HttpResponse$BodyHandlers]
           [java.net URI]
           [java.util UUID]
           [java.time Instant]
           [java.util.concurrent.locks ReentrantLock]))

;; ============================================================
;; Configuration
;; ============================================================

(def ^:dynamic *rest-endpoint*
  "Iceberg REST catalog endpoint URL."
  "http://localhost:8181")

(def ^:dynamic *s3-endpoint*
  "S3-compatible storage endpoint for data files."
  "http://localhost:9000")

(def ^:dynamic *s3-access-key*
  "S3 access key."
  "admin")

(def ^:dynamic *s3-secret-key*
  "S3 secret key."
  "password")

;; ============================================================
;; HTTP helpers
;; ============================================================

(defn- parse-json [s]
  (when (and s (not (str/blank? s)))
    (try
      (json/parse-string s true)
      (catch Exception _ nil))))

(defn- to-json [obj]
  (json/generate-string obj))

(defn- http-request
  "Make an HTTP request to the REST catalog.
   Returns parsed JSON response or throws on error."
  [method path & {:keys [body headers]}]
  (let [client (HttpClient/newHttpClient)
        uri (URI/create (str *rest-endpoint* path))
        builder (HttpRequest/newBuilder uri)
        _ (doto builder
            (.method (str/upper-case (name method))
                     (if body
                       (HttpRequest$BodyPublishers/ofString (to-json body))
                       (HttpRequest$BodyPublishers/noBody))))
        _ (when body
            (.header builder "Content-Type" "application/json"))
        _ (doseq [[k v] headers]
            (.header builder k v))
        request (.build builder)
        response (.send client request (HttpResponse$BodyHandlers/ofString))
        status (.statusCode response)]
    (if (<= 200 status 299)
      (parse-json (.body response))
      (throw (ex-info (str "HTTP " status ": " (.body response))
                      {:status status
                       :method method
                       :path path
                       :body (.body response)})))))

;; ============================================================
;; Yggdrasil metadata helpers
;; ============================================================

(defn- yggdrasil-metadata
  "Create Yggdrasil metadata properties for tagging commits.
   Optional :parents in context should be a vector of parent snapshot IDs."
  [message context]
  {"yggdrasil.commit" (to-json
                       (cond-> {:message message
                                :timestamp (System/currentTimeMillis)
                                :tool (or (:tool context) "yggdrasil-clj")
                                :author (or (:author context) (System/getProperty "user.name"))
                                :version "0.1.0"}
                         (:parents context) (assoc :parents (:parents context))))
   "yggdrasil.commit.id" (str (UUID/randomUUID))})

(defn- yggdrasil-commit?
  "Check if a snapshot was created by Yggdrasil."
  [snapshot]
  (contains? (:summary snapshot) "yggdrasil.commit"))

(defn- parse-yggdrasil-commit
  "Parse Yggdrasil commit metadata from snapshot."
  [snapshot]
  (when-let [commit-json (get-in snapshot [:summary "yggdrasil.commit"])]
    (try
      (parse-json commit-json)
      (catch Exception _ nil))))

;; ============================================================
;; REST catalog operations
;; ============================================================

(defn- load-table-metadata [namespace table]
  (http-request :get (str "/v1/namespaces/" namespace "/tables/" table)))

(defn- list-namespaces []
  (:namespaces (http-request :get "/v1/namespaces")))

(defn- create-namespace! [namespace]
  (http-request :post "/v1/namespaces"
                :body {:namespace [namespace]}))

(defn- list-tables [namespace]
  (let [response (http-request :get (str "/v1/namespaces/" namespace "/tables"))]
    (mapv :name (:identifiers response))))

(defn- create-table! [namespace table schema]
  (http-request :post (str "/v1/namespaces/" namespace "/tables")
                :body {:name table
                       :schema schema
                       :partition-spec {:spec-id 0 :fields []}
                       :write-order {:order-id 0 :fields []}
                       :properties {}}))

(defn- delete-table! [namespace table]
  (try
    (http-request :delete (str "/v1/namespaces/" namespace "/tables/" table))
    (catch Exception _ nil)))

(defn- update-table! [namespace table updates]
  (http-request :post (str "/v1/namespaces/" namespace "/tables/" table)
                :body {:updates updates}))

;; ============================================================
;; Snapshot operations
;; ============================================================

(defn- get-current-snapshot-id [table-meta]
  (or (:current-snapshot-id (:metadata table-meta))
      (:current-snapshot-id table-meta)))

(defn- get-snapshot-by-id [table-meta snap-id]
  (let [snapshots (or (:snapshots (:metadata table-meta))
                      (:snapshots table-meta))]
    (first (filter #(= (str (:snapshot-id %)) (str snap-id)) snapshots))))

(defn- get-snapshots [table-meta]
  (or (:snapshots (:metadata table-meta))
      (:snapshots table-meta)
      []))

(defn- get-refs [table-meta]
  (or (:refs (:metadata table-meta))
      (:refs table-meta)
      {}))

;; ============================================================
;; Lock helpers
;; ============================================================

(defn- get-lock! [branch-locks branch-name]
  (locking branch-locks
    (if-let [l (get @branch-locks branch-name)]
      l
      (let [l (ReentrantLock.)]
        (swap! branch-locks assoc branch-name l)
        l))))

(defn- with-branch-lock* [branch-locks branch-name f]
  (let [^ReentrantLock l (get-lock! branch-locks branch-name)]
    (.lock l)
    (try (f)
         (finally (.unlock l)))))

;; ============================================================
;; Polling watcher
;; ============================================================

(defn- poll-fn
  [namespace table current-branch last-state]
  (try
    (let [table-meta (load-table-metadata namespace table)
          refs (get-refs table-meta)
          current-snap (get-current-snapshot-id table-meta)
          branch current-branch
          branch-snap (or (get-in refs [(keyword branch) :snapshot-id])
                          current-snap)
          prev-snap (:snapshot last-state)
          prev-refs (or (:refs last-state) {})
          ref-keys (set (keys refs))
          prev-ref-keys (set (keys prev-refs))

          ;; Detect new snapshots and classify as Yggdrasil or external
          snapshots (get-snapshots table-meta)
          new-snaps (when (and branch-snap prev-snap
                               (not= (str branch-snap) (str prev-snap)))
                      (filter #(let [sid (:snapshot-id %)]
                                 (and sid
                                      (>= sid (or prev-snap 0))
                                      (<= sid branch-snap)))
                              snapshots))

          events (cond-> []
                   ;; Emit events for new snapshots
                   (seq new-snaps)
                   (into (for [snap new-snaps]
                           (let [ygg-commit (yggdrasil-commit? snap)
                                 ygg-meta (when ygg-commit
                                            (parse-yggdrasil-commit snap))
                                 operation (get-in snap [:summary :operation])]
                             {:type (if ygg-commit :commit :external-commit)
                              :snapshot-id (str (:snapshot-id snap))
                              :branch (keyword branch)
                              :source (if ygg-commit :yggdrasil :external)
                              :message (or (:message ygg-meta)
                                           (str "External " (or operation "change")))
                              :operation operation
                              :timestamp (:timestamp-ms snap)})))

                   true
                   (into (for [r (set/difference ref-keys prev-ref-keys)]
                           {:type :branch-created
                            :branch r
                            :timestamp (System/currentTimeMillis)}))

                   true
                   (into (for [r (set/difference prev-ref-keys ref-keys)]
                           {:type :branch-deleted
                            :branch r
                            :timestamp (System/currentTimeMillis)})))]
      {:state {:snapshot branch-snap :refs refs}
       :events events})
    (catch Exception _
      {:state last-state :events []})))

;; ============================================================
;; System record
;; ============================================================

(defrecord IcebergSystem [namespace table current-branch system-name
                          watcher-state branch-locks opts logical-branches-atom]
  p/SystemIdentity
  (system-id [_] (or system-name (str "iceberg:" namespace "." table)))
  (system-type [_] :iceberg)
  (capabilities [_]
    (t/->Capabilities true true true true false true))

  p/Snapshotable
  (snapshot-id [_]
    (let [table-meta (load-table-metadata namespace table)
          branch current-branch
          refs (get-refs table-meta)]
      (if (= branch "main")
        (str (get-current-snapshot-id table-meta))
        (str (get-in refs [(keyword branch) :snapshot-id]
                     (get-current-snapshot-id table-meta))))))

  (parent-ids [this]
    (let [snap-id (p/snapshot-id this)
          table-meta (load-table-metadata namespace table)
          snapshot (get-snapshot-by-id table-meta snap-id)
          ;; First check Yggdrasil metadata for complete parent list
          ;; Try both keyword and string keys (summary format varies)
          ygg-commit-json (or (get-in snapshot [:summary :yggdrasil.commit])
                              (get-in snapshot [:summary "yggdrasil.commit"]))
          ygg-metadata (when ygg-commit-json (parse-json ygg-commit-json))
          ygg-parents (:parents ygg-metadata)]
      (if ygg-parents
        ;; Use Yggdrasil metadata parents (complete list)
        (set ygg-parents)
        ;; Fallback to Iceberg native parent (single parent only)
        (if-let [parent1 (:parent-snapshot-id snapshot)]
          #{(str parent1)}
          #{}))))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Return the snapshot ID as a reference (can be used for time-travel queries)
    (str snap-id))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [table-meta (load-table-metadata namespace table)
          snapshot (get-snapshot-by-id table-meta snap-id)]
      (when snapshot
        ;; Check Yggdrasil metadata for complete parent list
        ;; Try both keyword and string keys (summary format varies)
        (let [ygg-commit-json (or (get-in snapshot [:summary :yggdrasil.commit])
                                  (get-in snapshot [:summary "yggdrasil.commit"]))
              ygg-metadata (when ygg-commit-json (parse-json ygg-commit-json))
              ygg-parents (:parents ygg-metadata)
              parent-ids (if ygg-parents
                           (set ygg-parents)
                           (if-let [parent (:parent-snapshot-id snapshot)]
                             #{(str parent)}
                             #{}))]
          {:snapshot-id (str (:snapshot-id snapshot))
           :parent-ids parent-ids
           :timestamp (Instant/ofEpochMilli (:timestamp-ms snapshot))
           :summary (:summary snapshot)}))))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [table-meta (load-table-metadata namespace table)
          refs (get-refs table-meta)
          iceberg-branches (set (filter keyword? (keys refs)))
          logical-branches @logical-branches-atom]
      ;; Return union of Iceberg refs and logical branches
      (clojure.set/union iceberg-branches logical-branches)))

  (current-branch [_]
    (keyword current-branch))

  (branch! [this name]
    (let [table-meta (load-table-metadata namespace table)
          current-snap (get-current-snapshot-id table-meta)]
      (p/branch! this name current-snap nil)))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (let [branch-str (clojure.core/name name)
          from-str (if from (str from) nil)
          table-meta (load-table-metadata namespace table)
          snap-id (or from-str (str (get-current-snapshot-id table-meta)))
          current-branch-kw (keyword current-branch)]
      (if (and snap-id (not= "-1" snap-id))
        ;; Snapshots exist: create Iceberg ref
        (let [updates [{:action "set-snapshot-ref"
                        :ref-name branch-str
                        :snapshot-id (Long/parseLong snap-id)
                        :type "branch"}]]
          (update-table! namespace table updates))
        ;; No snapshots yet: track as logical branch
        (swap! logical-branches-atom conj name))
      ;; Copy parent branch's data to new branch (for branch-aware mock)
      (if (contains? this :_branch-entries)
        (let [parent-entries (get-in this [:_branch-entries current-branch-kw] {})]
          (assoc-in this [:_branch-entries name] parent-entries))
        this)))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)]
      ;; Remove from Iceberg refs if it exists
      (try
        (let [updates [{:action "remove-snapshot-ref"
                        :ref-name branch-str}]]
          (update-table! namespace table updates))
        (catch Exception _))
      ;; Also remove from logical branches
      (swap! logical-branches-atom disj name)
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)
          table-meta (load-table-metadata namespace table)
          refs (get-refs table-meta)
          is-iceberg-branch (contains? refs (keyword branch-str))
          is-logical-branch (contains? @logical-branches-atom name)]
      ;; Allow checkout to either Iceberg refs or logical branches
      (when-not (or is-iceberg-branch is-logical-branch)
        (throw (ex-info (str "Branch not found: " branch-str)
                        {:branch branch-str})))
      (->IcebergSystem namespace table branch-str system-name
                       watcher-state branch-locks opts logical-branches-atom)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [table-meta (load-table-metadata namespace table)
          snapshots (get-snapshots table-meta)
          limit (or (:limit opts) (count snapshots))]
      (vec (take limit (map #(str (:snapshot-id %))
                            (reverse snapshots))))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [table-meta (load-table-metadata namespace table)
          all-snaps (get-snapshots table-meta)
          snap-map (into {} (map (fn [s] [(:snapshot-id s) s]) all-snaps))
          walk-parents (fn walk [sid]
                         (when-let [snap (snap-map sid)]
                           (when-let [parent (:parent-snapshot-id snap)]
                             (cons (str parent) (walk parent)))))]
      (vec (walk-parents (if (string? snap-id)
                           (Long/parseLong snap-id)
                           snap-id)))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [this a b _opts]
    (let [ancestors-b (set (p/ancestors this b))]
      (contains? ancestors-b (str a))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [this a b _opts]
    (let [ancestors-a (set (p/ancestors this a))
          ancestors-b (set (p/ancestors this b))
          common (set/intersection ancestors-a ancestors-b)]
      (when (seq common)
        ;; Find the most recent common ancestor (first in history order)
        (let [history (p/history this {:limit 10000})]
          (first (filter common history))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [table-meta (load-table-metadata namespace table)
          snapshots (get-snapshots table-meta)
          refs (get-refs table-meta)
          nodes (into {}
                      (map (fn [s]
                             (let [parent1 (:parent-snapshot-id s)
                                   ;; Try both keyword and string keys for summary
                                   parent2 (or (get-in s [:summary "merge-parent-2"])
                                               (get-in s ["summary" "merge-parent-2"]))]
                               [(str (:snapshot-id s))
                                {:parent-ids (cond-> #{}
                                               parent1 (conj (str parent1))
                                               parent2 (conj parent2))
                                 :meta {:timestamp (:timestamp-ms s)
                                        :summary (:summary s)}}])))
                      snapshots)
          branches (into {} (map (fn [[k v]]
                                   [k (str (:snapshot-id v))])
                                 refs))
          roots (set (keep (fn [[id {:keys [parent-ids]}]]
                             (when (empty? parent-ids) id))
                           nodes))]
      {:nodes nodes :branches branches :roots roots}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (when-let [meta (p/snapshot-meta this snap-id)]
      (dissoc meta :snapshot-id)))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    ;; Create a merge commit with two parents
    ;; Iceberg only supports one parent-snapshot-id, so we store the second parent in summary
    (binding [*rest-endpoint* (or (:rest-endpoint opts) (:rest-endpoint (:opts this)) *rest-endpoint*)
              *s3-endpoint* (or (:s3-endpoint opts) (:s3-endpoint (:opts this)) *s3-endpoint*)
              *s3-access-key* (or (:s3-access-key opts) (:s3-access-key (:opts this)) *s3-access-key*)
              *s3-secret-key* (or (:s3-secret-key opts) (:s3-secret-key (:opts this)) *s3-secret-key*)]
      (with-branch-lock* branch-locks current-branch
        (fn []
          (let [source-branch (if (keyword? source) (clojure.core/name source) (str source))
                source-branch-kw (keyword source-branch)
                dest-branch current-branch
                dest-branch-kw (keyword dest-branch)
                table-meta (load-table-metadata namespace table)
                refs (get-refs table-meta)
                source-snap (get-in refs [source-branch-kw :snapshot-id])
                dest-snap (get-current-snapshot-id table-meta)]
            (when-not source-snap
              (throw (ex-info "Source branch not found" {:branch source-branch})))

            ;; Create merge snapshot with two parents
            (let [new-snap-id (System/currentTimeMillis)
                  current-seq (or (get-in table-meta [:metadata :last-sequence-number])
                                  (get table-meta :last-sequence-number)
                                  0)
                  next-seq (inc current-seq)
                  manifest-list (str "s3://warehouse/" namespace "/" table "/metadata/empty-manifest-list.avro")
                  ;; Store both parents: Iceberg native parent + Yggdrasil metadata with full parent list
                  snapshot-data {:snapshot-id new-snap-id
                                 :parent-snapshot-id dest-snap
                                 :timestamp-ms (System/currentTimeMillis)
                                 :sequence-number next-seq
                                 :manifest-list manifest-list
                                 :summary (merge {"operation" "yggdrasil-merge"}
                                                 (yggdrasil-metadata (str "Merge " source-branch " into " dest-branch)
                                                                     {:parents [(str dest-snap) (str source-snap)]}))}
                  updates [{:action "add-snapshot"
                            :snapshot snapshot-data}
                           {:action "set-snapshot-ref"
                            :ref-name dest-branch
                            :snapshot-id new-snap-id
                            :type "branch"}]]
              (update-table! namespace table updates)
              ;; If this was a logical branch, remove it
              (swap! logical-branches-atom disj dest-branch-kw))

            ;; Merge branch-aware mock data (if present)
            (if (contains? this :_branch-entries)
              (let [source-entries (get-in this [:_branch-entries source-branch-kw] {})
                    dest-entries (get-in this [:_branch-entries dest-branch-kw] {})
                    merged-entries (merge dest-entries source-entries)]
                (assoc-in this [:_branch-entries dest-branch-kw] merged-entries))
              this))))))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [this a b _opts]
    ;; For branch-aware mock data, there are no conflicts (simple key-value merge)
    ;; Real Iceberg conflict detection would compare manifest files and data files
    ;; Since we use append-only semantics in the mock, no conflicts occur
    (if (contains? this :_branch-entries)
      ;; Mock data: no conflicts (key-value maps merge cleanly)
      []
      ;; Real Iceberg: simplified - always return no conflicts
      ;; True implementation would compare manifests/schemas/partition evolution
      []))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (let [table-meta (load-table-metadata namespace table)
          snap-a (get-snapshot-by-id table-meta a)
          snap-b (get-snapshot-by-id table-meta b)]
      {:snapshot-a (str a)
       :snapshot-b (str b)
       :changes (if (and snap-a snap-b)
                  [{:type :snapshot-change
                    :from (:timestamp-ms snap-a)
                    :to (:timestamp-ms snap-b)}]
                  [])}))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn namespace table current-branch)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id)))

;; ============================================================
;; Factory functions
;; ============================================================

(defn create
  "Create an Iceberg adapter for an existing table.

   Options:
   - :initial-branch - branch name (default \"main\")
   - :system-name - identifier for the system
   - :rest-endpoint - REST catalog URL (default localhost:8181)
   - :s3-endpoint - S3 storage URL (default localhost:9000)
   - :s3-access-key - S3 access key (default \"admin\")
   - :s3-secret-key - S3 secret key (default \"password\")"
  ([namespace table] (create namespace table {}))
  ([namespace table opts]
   (binding [*rest-endpoint* (or (:rest-endpoint opts) *rest-endpoint*)
             *s3-endpoint* (or (:s3-endpoint opts) *s3-endpoint*)
             *s3-access-key* (or (:s3-access-key opts) *s3-access-key*)
             *s3-secret-key* (or (:s3-secret-key opts) *s3-secret-key*)]
     (let [branch (or (:initial-branch opts) "main")]
       (->IcebergSystem namespace
                        table
                        branch
                        (:system-name opts)
                        (w/create-watcher-state)
                        (atom {})
                        opts
                        ;; Track branches that don't have Iceberg refs yet (no snapshots)
                        (atom #{(keyword branch)}))))))

(defn init!
  "Initialize a new Iceberg table.

   Creates a simple table with id (long) and value (string) columns
   for testing purposes. Immediately creates an initial Yggdrasil commit
   to establish the first snapshot.

   Options: same as `create`"
  ([namespace table] (init! namespace table {}))
  ([namespace table opts]
   (binding [*rest-endpoint* (or (:rest-endpoint opts) *rest-endpoint*)
             *s3-endpoint* (or (:s3-endpoint opts) *s3-endpoint*)
             *s3-access-key* (or (:s3-access-key opts) *s3-access-key*)
             *s3-secret-key* (or (:s3-secret-key opts) *s3-secret-key*)]
     ;; Ensure namespace exists
     (try (create-namespace! namespace) (catch Exception _))
     ;; Create table with simple schema
     (let [schema {:type "struct"
                   :schema-id 0
                   :fields [{:id 1
                             :name "id"
                             :required true
                             :type "long"}
                            {:id 2
                             :name "value"
                             :required false
                             :type "string"}]}]
       (create-table! namespace table schema)
       ;; NOTE: We do NOT create an initial snapshot here, similar to Git adapter.
       ;; The first user commit (via commit!) will be the true root commit with no parent.
       ;; This matches Yggdrasil's semantics where create-system doesn't create commits.)
     ;; Return system
       (create namespace table opts)))))

(defn destroy!
  "Destroy the Iceberg table."
  [^IcebergSystem sys]
  (w/stop-polling! (:watcher-state sys))
  (try
    (delete-table! (:namespace sys) (:table sys))
    (catch Exception _)))

(defn commit!
  "Create a Yggdrasil commit via metadata update.

   This creates a new Iceberg snapshot without requiring data writes.
   The commit message and metadata are stored in table properties."
  ([sys] (commit! sys nil))
  ([sys message]
   (binding [*rest-endpoint* (or (:rest-endpoint (:opts sys)) *rest-endpoint*)
             *s3-endpoint* (or (:s3-endpoint (:opts sys)) *s3-endpoint*)
             *s3-access-key* (or (:s3-access-key (:opts sys)) *s3-access-key*)
             *s3-secret-key* (or (:s3-secret-key (:opts sys)) *s3-secret-key*)]
     (with-branch-lock* (:branch-locks sys) (:current-branch sys)
       (fn []
         (let [{:keys [namespace table]} sys
               msg (or message "commit")
               table-meta (load-table-metadata namespace table)
               current-snap-id (get-current-snapshot-id table-meta)
               new-snap-id (System/currentTimeMillis)
               current-branch (:current-branch sys)
               manifest-list (str "s3://warehouse/" namespace "/" table "/metadata/empty-manifest-list.avro")
               ;; Get current sequence number and increment
               current-seq (or (get-in table-meta [:metadata :last-sequence-number])
                               (get table-meta :last-sequence-number)
                               0)
               next-seq (inc current-seq)
               ;; Only include parent-snapshot-id if there's a valid current snapshot
               ;; Also store parent in Yggdrasil metadata for complete lineage tracking
               parents (when (and current-snap-id (not= -1 current-snap-id))
                         [(str current-snap-id)])
               snapshot-data (cond-> {:snapshot-id new-snap-id
                                      :timestamp-ms (System/currentTimeMillis)
                                      :sequence-number next-seq
                                      :manifest-list manifest-list
                                      :summary (merge {"operation" "yggdrasil-commit"}
                                                      (yggdrasil-metadata msg (if parents {:parents parents} {})))}
                               (and current-snap-id (not= -1 current-snap-id))
                               (assoc :parent-snapshot-id current-snap-id))
               updates [{:action "add-snapshot"
                         :snapshot snapshot-data}
                        {:action "set-snapshot-ref"
                         :ref-name (name current-branch)
                         :snapshot-id new-snap-id
                         :type "branch"}]]
           (update-table! namespace table updates)
           ;; If this was a logical branch, remove it since it now has an Iceberg ref
           (swap! (:logical-branches-atom sys) disj (keyword current-branch))
           (str new-snap-id)))))))

(defn iceberg-available?
  "Check if the Iceberg REST catalog is available."
  []
  (try
    (http-request :get "/v1/config")
    true
    (catch Exception _ false)))
