(ns yggdrasil.adapters.lakefs
  "lakeFS adapter for Yggdrasil protocols.

  Maps lakeFS (git-for-data) concepts to yggdrasil:
    - lakeFS repository = system workspace
    - lakeFS branch = yggdrasil branch
    - lakeFS commit = yggdrasil snapshot
    - Objects under entries/ prefix = key-value storage

  lakeFS provides:
    - Zero-copy branching on object storage
    - Three-way merge
    - Full commit DAG
    - REST API accessed via lakectl CLI

  Requirements:
    - `lakectl` binary on PATH
    - Running lakeFS server (local or remote)
    - ~/.lakectl.yaml configured with credentials and endpoint"
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.watcher :as w]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.set :as set])
  (:import [java.io File]
           [java.util UUID]
           [java.util.concurrent.locks ReentrantLock]))

;; ============================================================
;; Configuration
;; ============================================================

(def ^:dynamic *lakectl-binary*
  "Path to the lakectl binary."
  "lakectl")

(def ^:dynamic *lakectl-config*
  "Path to lakectl config file. nil uses default (~/.lakectl.yaml)."
  nil)

;; ============================================================
;; CLI helpers
;; ============================================================

(defn- lakectl
  "Run a lakectl command. Returns trimmed stdout on success, throws on failure."
  [& args]
  (let [base-args (if *lakectl-config*
                    [*lakectl-binary* "-c" *lakectl-config* "--no-color"]
                    [*lakectl-binary* "--no-color"])
        cmd (into (vec base-args) args)
        result (apply sh cmd)]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (throw (ex-info (str "lakectl error: " (str/trim (:err result)))
                      {:args (vec args) :exit (:exit result)
                       :err (str/trim (:err result))})))))

(defn- repo-uri [repo-name]
  (str "lakefs://" repo-name))

(defn- branch-uri [repo-name branch-name]
  (str "lakefs://" repo-name "/" branch-name))

(defn- object-uri [repo-name branch-name path]
  (str "lakefs://" repo-name "/" branch-name "/" path))

(defn- ref-uri [repo-name ref]
  (str "lakefs://" repo-name "/" ref))

;; ============================================================
;; Output parsing helpers
;; ============================================================

(defn- parse-log-entry
  "Parse a single log entry block from lakectl log output.
   Returns {:id, :message, :date, :parents}."
  [block]
  (let [lines (str/split-lines block)
        id (some->> lines
                    (filter #(str/starts-with? % "ID:"))
                    first
                    (re-find #"ID:\s+(\S+)")
                    second)
        date (some->> lines
                      (filter #(str/starts-with? % "Date:"))
                      first
                      (re-find #"Date:\s+(.+)")
                      second
                      str/trim)
        parents (some->> lines
                         (filter #(str/starts-with? % "Parents:"))
                         first
                         (re-find #"Parents:\s+(.+)")
                         second
                         str/trim)
        ;; Message is the indented line after the blank line
        msg-lines (->> lines
                       (drop-while #(not (str/blank? %)))
                       (drop 1)
                       (map str/trim)
                       (remove str/blank?))]
    (when id
      {:id id
       :message (str/join " " msg-lines)
       :date date
       :parents (when (and parents (not (str/blank? parents)))
                  (set (str/split parents #",\s*")))})))

(defn- parse-log
  "Parse lakectl log output into a sequence of commit entries."
  [output]
  (when-not (str/blank? output)
    ;; Split on blank lines between entries
    (let [blocks (str/split output #"\n\n(?=ID:)")]
      (keep parse-log-entry blocks))))

(defn- parse-branch-list
  "Parse lakectl branch list output into a map of {name -> commit-hash}."
  [output]
  (when-not (str/blank? output)
    (->> (str/split-lines output)
         (map #(str/split % #"\t"))
         (filter #(= 2 (count %)))
         (into {} (map (fn [[name hash]] [name (str/trim hash)]))))))

(defn- parse-diff
  "Parse lakectl diff output into a list of changes."
  [output]
  (when-not (str/blank? output)
    (->> (str/split-lines output)
         (remove #(or (str/starts-with? % "Left ref:")
                      (str/starts-with? % "Right ref:")))
         (map (fn [line]
                (let [[op path] (str/split (str/trim line) #"\s+" 2)]
                  (when (and op path)
                    {:op (case (str/replace op "+" "added")
                           "added" :added
                           "removed" :removed
                           "modified" :modified
                           :unknown)
                     :path path}))))
         (remove nil?))))

(defn- parse-dot-parents
  "Parse lakectl log --dot output to extract parent->child edges.
   Returns a map of {child-id #{parent-ids...}}."
  [output]
  (when-not (str/blank? output)
    (->> (str/split-lines output)
         (keep #(when-let [[_ parent child] (re-find #"\"([a-f0-9]+)\"\s*->\s*\"([a-f0-9]+)\"" %)]
                  [child parent]))
         (reduce (fn [m [child parent]]
                   (update m child (fnil conj #{}) parent))
                 {}))))

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
;; Core helpers
;; ============================================================

(defn- current-head [repo-name branch-name]
  (let [output (lakectl "log" (branch-uri repo-name branch-name)
                        "--amount" "1")]
    (when-let [entries (parse-log output)]
      (:id (first entries)))))

(defn- list-branches [repo-name]
  (let [output (lakectl "branch" "list" (repo-uri repo-name))]
    (parse-branch-list output)))

(defn- get-log [repo-name branch-name & {:keys [amount] :or {amount 1000}}]
  (let [amt (min amount 1000)
        output (lakectl "log" (branch-uri repo-name branch-name)
                        "--amount" (str amt))
        entries (parse-log output)
        ;; Get parent relationships via dot format
        dot-output (lakectl "log" (branch-uri repo-name branch-name)
                            "--amount" (str amt) "--dot")
        parent-map (or (parse-dot-parents dot-output) {})]
    (map (fn [entry]
           (assoc entry :parents (get parent-map (:id entry) #{})))
         entries)))

;; ============================================================
;; Polling watcher
;; ============================================================

(defn- poll-fn
  [repo-name current-branch last-state]
  (try
    (let [branch current-branch
          head (current-head repo-name branch)
          branches (set (keys (list-branches repo-name)))
          prev-head (:head last-state)
          prev-branches (or (:branches last-state) #{})
          events (cond-> []
                   (and head prev-head (not= head prev-head))
                   (conj {:type :commit
                          :snapshot-id head
                          :branch branch
                          :timestamp (System/currentTimeMillis)})

                   true
                   (into (for [b (set/difference branches prev-branches)]
                           {:type :branch-created
                            :branch b
                            :timestamp (System/currentTimeMillis)}))

                   true
                   (into (for [b (set/difference prev-branches branches)]
                           {:type :branch-deleted
                            :branch b
                            :timestamp (System/currentTimeMillis)})))]
      {:state {:head head :branches branches}
       :events events})
    (catch Exception _
      {:state last-state :events []})))

;; ============================================================
;; System record
;; ============================================================

(defrecord LakeFSSystem [repo-name current-branch system-name
                         watcher-state branch-locks init-commit]
  p/SystemIdentity
  (system-id [_] (or system-name (str "lakefs:" repo-name)))
  (system-type [_] :lakefs)
  (capabilities [_]
    (t/->Capabilities true true true true false true true))

  p/Snapshotable
  (snapshot-id [_]
    (current-head repo-name current-branch))

  (parent-ids [_]
    (let [entries (get-log repo-name current-branch :amount 1)]
      (if-let [entry (first entries)]
        (disj (or (:parents entry) #{}) init-commit)
        #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Return the commit hash as a ref (can be used in lakefs://repo/ref/path URIs)
    (str snap-id))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    ;; Look up commit in log
    (let [entries (get-log repo-name current-branch :amount 1000)]
      (when-let [entry (first (filter #(= (:id %) (str snap-id)) entries))]
        {:snapshot-id (:id entry)
         :parent-ids (disj (or (:parents entry) #{}) init-commit)
         :timestamp (:date entry)
         :message (:message entry)})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (set (map keyword (keys (list-branches repo-name)))))

  (current-branch [_]
    (keyword current-branch))

  (branch! [this name]
    (let [branch-str (clojure.core/name name)]
      (lakectl "branch" "create"
               (branch-uri repo-name branch-str)
               "--source" (branch-uri repo-name current-branch))
      this))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (let [branch-str (clojure.core/name name)]
      (lakectl "branch" "create"
               (branch-uri repo-name branch-str)
               "--source" (ref-uri repo-name (str from)))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (lakectl "branch" "delete" (branch-uri repo-name branch-str) "--yes")
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)
          branches-map (list-branches repo-name)]
      (when-not (contains? branches-map branch-str)
        (throw (ex-info (str "Branch not found: " branch-str)
                        {:branch branch-str})))
      ;; Use assoc to preserve any extra keys (e.g., test config)
      (assoc this :current-branch branch-str)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [limit (or (:limit opts) 1000)
          entries (get-log repo-name current-branch :amount limit)
          ids (filterv #(not= % init-commit) (mapv :id entries))]
      (vec (take limit ids))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [entries (get-log repo-name current-branch :amount 1000)
          ids (filterv #(not= % init-commit) (mapv :id entries))
          idx (.indexOf ids (str snap-id))]
      (if (>= idx 0)
        (vec (drop (inc idx) ids))
        [])))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    ;; a is ancestor of b if a appears after b in the log
    (let [entries (get-log repo-name current-branch :amount 1000)
          ids (mapv :id entries)
          idx-a (.indexOf ids (str a))
          idx-b (.indexOf ids (str b))]
      (and (>= idx-a 0) (>= idx-b 0) (> idx-a idx-b))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    ;; Find common ancestor by intersecting ancestor sets
    ;; Look up which branch each ref is on and compare histories
    (let [branches-map (list-branches repo-name)
          all-branch-names (keys branches-map)
          find-history (fn [ref]
                         (try
                           (let [entries (get-log repo-name ref :amount 1000)]
                             (set (mapv :id entries)))
                           (catch Exception _
                             ;; Try each branch to find the ref
                             (loop [brs all-branch-names]
                               (when-let [br (first brs)]
                                 (let [entries (get-log repo-name br :amount 1000)
                                       ids (set (mapv :id entries))]
                                   (if (contains? ids (str ref))
                                     ids
                                     (recur (rest brs)))))))))
          history-a (find-history a)
          history-b (find-history b)]
      (when (and history-a history-b)
        (let [common (set/difference
                      (set/intersection history-a history-b)
                      #{init-commit})]
          (when (seq common)
            ;; Find the latest common commit (earliest in either branch's log)
            (let [entries (get-log repo-name current-branch :amount 1000)
                  ordered (filter #(common (:id %)) entries)]
              (:id (first ordered))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [entries (get-log repo-name current-branch :amount 1000)
          nodes (into {}
                      (map (fn [e]
                             [(:id e) {:parent-ids (disj (or (:parents e) #{}) init-commit)
                                       :meta {:message (:message e)
                                              :date (:date e)}}])
                           (remove #(= (:id %) init-commit) entries)))
          branches-map (list-branches repo-name)
          branches (into {} (map (fn [[name hash]] [(keyword name) hash]) branches-map))
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
    (let [source-branch (if (keyword? source) (clojure.core/name source) (str source))
          dest-branch current-branch]
      (lakectl "merge" (branch-uri repo-name source-branch)
               (branch-uri repo-name dest-branch))
      this))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    ;; Use diff to detect conflicts (same path modified on both sides)
    (try
      (let [;; Find common ancestor
            entries-a (get-log repo-name current-branch :amount 1000)
            history-a (set (mapv :id entries-a))
            ;; Get branches for each ref
            branches-map (list-branches repo-name)
            all-branch-names (keys branches-map)
            find-branch (fn [ref]
                          (first (filter
                                  (fn [br]
                                    (let [es (get-log repo-name br :amount 1000)]
                                      (some #(= (:id %) (str ref)) es)))
                                  all-branch-names)))
            branch-a (or (find-branch a) current-branch)
            branch-b (or (find-branch b) current-branch)
            ;; Diff each branch from their common point
            diff-a (parse-diff (lakectl "diff"
                                        (ref-uri repo-name (str a))
                                        (branch-uri repo-name branch-a)))
            diff-b (parse-diff (lakectl "diff"
                                        (ref-uri repo-name (str b))
                                        (branch-uri repo-name branch-b)))
            ;; Modified/added paths on each side
            paths-a (set (map :path (remove #(= :removed (:op %)) diff-a)))
            paths-b (set (map :path (remove #(= :removed (:op %)) diff-b)))
            ;; Conflicts = paths changed on both sides
            conflicts (set/intersection paths-a paths-b)]
        (mapv (fn [path]
                {:path (str/split path #"/")
                 :ours path
                 :theirs path})
              conflicts))
      (catch Exception _ [])))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (try
      (let [changes (parse-diff (lakectl "diff"
                                         (ref-uri repo-name (str a))
                                         (ref-uri repo-name (str b))))]
        {:snapshot-a (str a)
         :snapshot-b (str b)
         :changes changes})
      (catch Exception _
        {:snapshot-a (str a) :snapshot-b (str b) :changes []})))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn repo-name current-branch)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id))

  p/GarbageCollectable
  (gc-roots [_]
    (let [branches-map (list-branches repo-name)]
      (set (vals branches-map))))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this _snapshot-ids _opts]
    ;; No-op: LakeFS manages GC server-side via retention policies
    this))

;; ============================================================
;; Factory functions
;; ============================================================

(defn create
  "Create a lakeFS adapter for an existing repository."
  ([repo-name] (create repo-name {}))
  ([repo-name opts]
   (let [branch (or (:initial-branch opts) "main")
         init-hash (or (:init-commit opts)
                       ;; Find the root commit (first commit in the repo)
                       (let [entries (get-log repo-name branch :amount 1000)]
                         (:id (last entries))))]
     (->LakeFSSystem repo-name
                     branch
                     (:system-name opts)
                     (w/create-watcher-state)
                     (atom {})
                     init-hash))))

(defn init!
  "Initialize a new lakeFS repository.
   Creates the repo with local:// storage namespace."
  ([repo-name] (init! repo-name {}))
  ([repo-name opts]
   (let [storage-ns (or (:storage-namespace opts)
                        (str "local://" repo-name))]
     (lakectl "repo" "create" (repo-uri repo-name) storage-ns)
     ;; Get the init commit hash
     (let [entries (get-log repo-name "main" :amount 1)
           init-hash (:id (first entries))]
       (create repo-name (assoc opts :init-commit init-hash))))))

(defn destroy!
  "Destroy the lakeFS repository."
  [^LakeFSSystem sys]
  (w/stop-polling! (:watcher-state sys))
  (try
    (lakectl "repo" "delete" (repo-uri (:repo-name sys)) "--yes")
    (catch Exception _)))

(defn commit!
  "Create a commit on the current branch."
  ([sys] (commit! sys nil))
  ([sys message]
   (with-branch-lock* (:branch-locks sys) (:current-branch sys)
     (fn []
       (let [{:keys [repo-name current-branch]} sys
             branch current-branch
             msg (or message "commit")]
         (let [output (lakectl "commit" (branch-uri repo-name branch)
                               "-m" msg)]
           ;; Parse commit ID from output
           (when-let [[_ id] (re-find #"ID:\s+(\S+)" output)]
             id)))))))

(defn write-entry!
  "Write a key-value entry as an object under entries/ prefix."
  [sys key value]
  (let [{:keys [repo-name current-branch]} sys
        branch current-branch
        ;; Write to temp file, then upload
        tmp (File/createTempFile "ygg-lakefs-" ".tmp")]
    (try
      (spit tmp (str value))
      (lakectl "fs" "upload"
               (object-uri repo-name branch (str "entries/" key))
               "-s" (.getPath tmp)
               "--no-progress")
      (finally
        (.delete tmp)))))

(defn read-entry
  "Read a value by key from the entries/ prefix."
  [sys key]
  (let [{:keys [repo-name current-branch]} sys
        branch current-branch]
    (try
      (lakectl "fs" "cat" (object-uri repo-name branch (str "entries/" key)))
      (catch Exception _ nil))))

(defn delete-entry!
  "Delete an entry by key."
  [sys key]
  (let [{:keys [repo-name current-branch]} sys
        branch current-branch]
    (try
      (lakectl "fs" "rm" (object-uri repo-name branch (str "entries/" key)))
      (catch Exception _))))

(defn count-entries
  "Count entries under the entries/ prefix."
  [sys]
  (let [{:keys [repo-name current-branch]} sys
        branch current-branch]
    (try
      (let [output (lakectl "fs" "ls" (object-uri repo-name branch "entries/"))]
        (if (str/blank? output)
          0
          (count (filter #(str/starts-with? (str/trim %) "object")
                         (str/split-lines output)))))
      (catch Exception _ 0))))

(defn lakefs-available?
  "Check if the lakectl binary is available and can connect to a server."
  []
  (try
    (let [result (sh *lakectl-binary* "--version")]
      (zero? (:exit result)))
    (catch Exception _ false)))

(defn server-available?
  "Check if a lakeFS server is reachable."
  []
  (try
    (let [cmd (if *lakectl-config*
                [*lakectl-binary* "-c" *lakectl-config* "--no-color" "doctor"]
                [*lakectl-binary* "--no-color" "doctor"])
          result (apply sh cmd)]
      (zero? (:exit result)))
    (catch Exception _ false)))
