(ns yggdrasil.adapters.ipfs
  "Yggdrasil adapter for IPFS (InterPlanetary File System).

   Provides Git-like version control semantics on top of IPFS:
   - Commits are IPLD DAG nodes (content-addressed)
   - Branches are IPNS names (mutable pointers)
   - User data stored at :root CID (Yggdrasil doesn't prescribe format)

   Usage:
     (require '[yggdrasil.adapters.ipfs :as ipfs])
     (def sys (ipfs/init! {:system-name \"my-project\"}))

     ;; User adds data to IPFS
     (shell \"ipfs add -r my-data/\")  ; => QmXxx...

     ;; Yggdrasil tracks version
     (ipfs/commit! sys \"Initial commit\" {:root \"QmXxx...\"})

     ;; Create branch, make changes
     (p/branch! sys :feature)
     (p/checkout sys :feature)
     (ipfs/commit! sys \"Feature work\" {:root \"QmYyy...\"})

     ;; Merge
     (p/checkout sys :main)
     (p/merge! sys :feature {:root \"QmMerged...\" :message \"Merge feature\"})"
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.set :as set]
            [clojure.edn :as edn]
            [cheshire.core :as json])
  (:import [java.time Instant]))

;; ============================================================
;; Configuration & State
;; ============================================================

(def ^:dynamic *ipfs-bin* "ipfs")

(defn- state-dir
  "Get state directory for a system."
  [system-name]
  (let [home (System/getProperty "user.home")
        dir (io/file home ".yggdrasil" "ipfs" system-name)]
    (.mkdirs dir)
    dir))

(defn- state-file
  "Get state file path for a system."
  [system-name]
  (io/file (state-dir system-name) "state.edn"))

(defn- load-state
  "Load state from disk, or return default."
  [system-name]
  (let [file (state-file system-name)]
    (if (.exists file)
      (edn/read-string (slurp file))
      {:system-name system-name
       :current-branch :main
       :branches {}
       :ipfs-api "http://127.0.0.1:5001"})))

(defn- save-state!
  "Persist state to disk."
  [state]
  (let [file (state-file (:system-name state))]
    (spit file (pr-str state))))

;; ============================================================
;; IPFS CLI Helpers
;; ============================================================

(defn- ipfs
  "Execute IPFS CLI command.
   Returns {:out ... :err ... :exit ...}"
  [& args]
  (let [result (apply sh *ipfs-bin* args)]
    (when-not (zero? (:exit result))
      (throw (ex-info (str "IPFS command failed: " (str/join " " args))
                      {:args args
                       :exit (:exit result)
                       :stderr (:err result)})))
    result))

(defn- ipfs-available?
  "Check if IPFS daemon is running."
  []
  (try
    (ipfs "id")
    true
    (catch Exception _ false)))

(defn- dag-put
  "Store a Clojure map as DAG-JSON in IPFS.
   Returns the CID."
  [data]
  (let [json-str (json/generate-string data)
        result (ipfs "dag" "put" "--input-codec" "json" "--store-codec" "dag-json"
                     :in json-str)]
    (str/trim (:out result))))

(defn- dag-get
  "Retrieve a DAG node by CID.
   Returns parsed Clojure map."
  [cid]
  (let [result (ipfs "dag" "get" cid)]
    (json/parse-string (:out result) true)))

(defn- key-list
  "List all IPNS keys.
   Returns map of {key-name key-id}."
  []
  (let [result (ipfs "key" "list" "-l")
        lines (str/split-lines (:out result))]
    (into {}
          (for [line lines
                :when (not (str/blank? line))]
            (let [[id name] (str/split line #"\s+")]
              [name id])))))

(defn- key-gen
  "Generate a new IPNS key, or return existing key ID if it already exists.
   Returns the key ID."
  [key-name]
  (let [existing-keys (key-list)]
    (if-let [existing-id (get existing-keys key-name)]
      existing-id
      (let [result (ipfs "key" "gen" key-name)]
        (str/trim (:out result))))))

(defn- name-publish
  "Publish a CID to an IPNS name.
   Returns the IPNS name.
   Uses --allow-offline for faster local publishing.

   Output format: 'Published to k51qzi5uqu5...: /ipfs/QmXxx...'"
  [cid key-name]
  (let [result (ipfs "name" "publish" "--allow-offline" "--key" key-name cid)
        output (str/trim (:out result))]
    ;; Extract k51... from "Published to k51...: /ipfs/..."
    (if-let [[_ ipns-key] (re-find #"Published to ([^:]+):" output)]
      (str "/ipns/" ipns-key)
      ;; Fallback: if format changes, try to extract k51... directly
      (if-let [[_ ipns-key] (re-find #"(k51\w+)" output)]
        (str "/ipns/" ipns-key)
        (throw (ex-info "Could not parse IPNS name from publish output"
                        {:output output}))))))

(defn- name-resolve
  "Resolve an IPNS name to a CID.
   Returns CID or nil if not found."
  [ipns-name]
  (try
    (let [result (ipfs "name" "resolve" ipns-name)]
      (-> (:out result)
          str/trim
          (str/replace #"^/ipfs/" "")))
    (catch Exception _ nil)))

;; ============================================================
;; Commit Management
;; ============================================================

(defn- get-author
  "Get commit author from environment."
  []
  (or (System/getenv "YGGDRASIL_AUTHOR")
      (str (System/getProperty "user.name") "@" (.getHostName (java.net.InetAddress/getLocalHost)))))

(defn- create-commit-object
  "Create a commit object map."
  [opts]
  {:type "yggdrasil-commit"
   :version "0.1.0"
   :parents (or (:parents opts) [])
   :root (or (:root opts) "")
   :message (or (:message opts) "commit")
   :author (or (:author opts) (get-author))
   :timestamp (or (:timestamp opts) (System/currentTimeMillis))})

(defn- store-commit!
  "Create and store a commit DAG node.
   Returns the commit CID."
  [commit-obj]
  (dag-put commit-obj))

(defn- read-commit
  "Read a commit object by CID."
  [cid]
  (dag-get cid))

(defn- get-branch-head
  "Get the current commit CID for a branch.
   Returns CID or nil if branch is empty."
  [state branch]
  (let [branch-data (get-in state [:branches branch])
        ipns-name (:ipns-name branch-data)]
    (when ipns-name
      (:last-resolved-cid branch-data))))

(defn- update-branch!
  "Update a branch to point to a new commit CID.
   Returns updated state."
  [state branch commit-cid]
  (let [branch-data (get-in state [:branches branch])
        ipns-key (:ipns-key branch-data)
        ipns-name (name-publish commit-cid (name branch))]
    (-> state
        (assoc-in [:branches branch :last-resolved-cid] commit-cid)
        (assoc-in [:branches branch :last-resolved-at] (System/currentTimeMillis))
        (assoc-in [:branches branch :ipns-name] ipns-name))))

;; ============================================================
;; System Record
;; ============================================================

(defrecord IPFSSystem [system-name state-atom current-branch]

  p/SystemIdentity
  (system-type [_] :ipfs)
  (system-id [_] system-name)
  (capabilities [_]
    (t/->Capabilities true true true true false true true))

  p/Snapshotable
  (snapshot-id [_]
    (let [state @state-atom]
      (get-branch-head state current-branch)))

  (parent-ids [this]
    (let [snap-id (p/snapshot-id this)]
      (if snap-id
        (let [commit (read-commit snap-id)]
          (set (:parents commit)))
        #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Return snapshot ID (can be used to read commit/data)
    snap-id)

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (when snap-id
      (let [commit (read-commit snap-id)]
        {:snapshot-id snap-id
         :parent-ids (set (:parents commit))
         :timestamp (Instant/ofEpochMilli (:timestamp commit))
         :message (:message commit)
         :author (:author commit)
         :root (:root commit)})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [state @state-atom]
      (set (keys (:branches state)))))

  (current-branch [_]
    current-branch)

  (branch! [this name] (p/branch! this name nil nil))
  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (let [state @state-atom
          branch-name (keyword name)
          from-branch (if (keyword? from) from current-branch)
          from-cid (if from
                     (if (keyword? from)
                       (get-branch-head state from)
                       from)
                     (get-branch-head state current-branch))]

      ;; Generate IPNS key for new branch
      (let [key-name (clojure.core/name branch-name)
            key-id (key-gen key-name)
            ipns-name (str "/ipns/" key-id)]

        ;; If there's a from commit, publish it to the new branch
        (when from-cid
          (name-publish from-cid key-name))

        ;; Update state
        (swap! state-atom assoc-in [:branches branch-name]
               {:ipns-key key-id
                :ipns-name ipns-name
                :last-resolved-cid from-cid
                :last-resolved-at (System/currentTimeMillis)})
        (save-state! @state-atom))

      ;; Copy parent branch's mock data to new branch (for compliance testing)
      (if (contains? this :_branch-entries)
        (let [parent-entries (get-in this [:_branch-entries from-branch] {})]
          (assoc-in this [:_branch-entries branch-name] parent-entries))
        this)))

  (checkout [this branch] (p/checkout this branch nil))
  (checkout [this branch _opts]
    (let [state @state-atom
          branch-kw (keyword branch)]
      (when-not (contains? (:branches state) branch-kw)
        (throw (ex-info "Branch not found" {:branch branch})))

      ;; Resolve latest commit for branch
      (let [branch-data (get-in state [:branches branch-kw])
            ipns-name (:ipns-name branch-data)
            resolved-cid (name-resolve ipns-name)]

        ;; Update branch metadata (last-resolved-cid/at) but not current-branch
        (swap! state-atom
               (fn [s]
                 (-> s
                     (assoc-in [:branches branch-kw :last-resolved-cid] resolved-cid)
                     (assoc-in [:branches branch-kw :last-resolved-at] (System/currentTimeMillis)))))
        (save-state! @state-atom))

      ;; Return NEW system with updated current-branch (value semantics)
      ;; Use assoc to preserve any extra keys (e.g., test mock entries)
      (assoc this :current-branch branch-kw)))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [state @state-atom
          branch-kw (keyword name)]
      (when (= branch-kw current-branch)
        (throw (ex-info "Cannot delete current branch" {:branch name})))

      ;; Remove IPNS key
      (try
        (ipfs "key" "rm" (clojure.core/name branch-kw))
        (catch Exception e
          (println "Warning: failed to remove IPNS key:" (.getMessage e))))

      ;; Update state
      (swap! state-atom update :branches dissoc branch-kw)
      (save-state! @state-atom))

    this)

  p/Graphable
  (history [this] (p/history this nil))
  (history [this opts]
    (let [snap-id (p/snapshot-id this)
          limit (:limit opts)]
      (loop [cid snap-id
             result []
             seen #{}]
        (if (or (nil? cid)
                (contains? seen cid)
                (and limit (>= (count result) limit)))
          result
          (let [commit (read-commit cid)
                parents (:parents commit)
                parent (first parents)]  ; Follow first parent for linear history
            (recur parent
                   (conj result cid)
                   (conj seen cid)))))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (loop [to-visit [snap-id]
           visited #{}]
      (if (empty? to-visit)
        visited
        (let [cid (first to-visit)
              rest-visit (rest to-visit)]
          (if (contains? visited cid)
            (recur rest-visit visited)
            (let [commit (read-commit cid)
                  parents (:parents commit)]
              (recur (concat rest-visit parents)
                     (conj visited cid))))))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [this a b _opts]
    (contains? (p/ancestors this b) a))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [this a b _opts]
    (let [ancestors-a (p/ancestors this a)
          ancestors-b (p/ancestors this b)
          common (set/intersection ancestors-a ancestors-b)]
      (first common)))  ; TODO: find actual merge-base (lowest common ancestor)

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    ;; commit-info is the same as snapshot-meta for IPFS
    (p/snapshot-meta this snap-id))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [state @state-atom
          branches (:branches state)
          ;; Get all commits reachable from all branches
          all-commits (reduce
                       (fn [acc [branch-name branch-data]]
                         (if-let [head (:last-resolved-cid branch-data)]
                           (set/union acc (p/ancestors this head) #{head})
                           acc))
                       #{}
                       branches)
          ;; Build nodes map
          nodes (into {}
                      (for [cid all-commits]
                        (let [commit (read-commit cid)]
                          [cid {:parents (set (:parents commit))
                                :timestamp (:timestamp commit)
                                :message (:message commit)}])))
          ;; Find roots (commits with no parents)
          roots (set (filter (fn [cid]
                               (empty? (get-in nodes [cid :parents])))
                             all-commits))
          ;; Map branches to their head commits
          branch-map (into {}
                           (for [[branch-name branch-data] branches
                                 :when (:last-resolved-cid branch-data)]
                             [branch-name (:last-resolved-cid branch-data)]))]
      {:nodes nodes
       :branches branch-map
       :roots roots}))

  p/Mergeable
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source opts]
    (let [state @state-atom
          source-branch (keyword source)
          current-cid (get-branch-head state current-branch)
          source-cid (get-branch-head state source-branch)]

      (when-not source-cid
        (throw (ex-info "Source branch has no commits" {:branch source})))

      (when-not current-cid
        (throw (ex-info "Current branch has no commits" {:branch current-branch})))

      ;; Create merge commit with explicit root (or use current root)
      (let [current-commit (read-commit current-cid)
            merge-root (or (:root opts) (:root current-commit))
            commit-obj (create-commit-object
                        {:parents [current-cid source-cid]
                         :root merge-root
                         :message (or (:message opts) (str "Merge " (name source-branch) " into " (name current-branch)))
                         :author (:author opts)
                         :timestamp (:timestamp opts)})
            merge-cid (store-commit! commit-obj)]

        ;; Update current branch
        (swap! state-atom update-branch! current-branch merge-cid)
        (save-state! @state-atom))

      ;; Merge branch-aware mock data (for compliance testing)
      (if (contains? this :_branch-entries)
        (let [source-entries (get-in this [:_branch-entries source-branch] {})
              dest-entries (get-in this [:_branch-entries current-branch] {})
              merged-entries (merge dest-entries source-entries)]
          (assoc-in this [:_branch-entries current-branch] merged-entries))
        this)))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    ;; For initial implementation, return empty
    ;; User handles conflicts manually before merge
    ;; Future: compare DAG trees at root CIDs
    [])

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    ;; Placeholder: could use ipfs dag diff in future
    {:a a :b b :note "IPFS dag diff not yet implemented"})

  p/GarbageCollectable
  (gc-roots [_]
    ;; All branch head CIDs are GC roots
    (let [state @state-atom]
      (->> (:branches state)
           (map (fn [[_ branch-data]] (:last-resolved-cid branch-data)))
           (remove nil?)
           set)))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids _opts]
    ;; Unpin CIDs and run repo GC to reclaim storage
    (doseq [cid snapshot-ids]
      (try
        (ipfs "pin" "rm" (str cid))
        (catch Exception _)))
    (try
      (ipfs "repo" "gc")
      (catch Exception _))
    this)

  p/Watchable
  (watch! [this callback] (p/watch! this callback nil))
  (watch! [this callback opts]
    ;; Simple polling implementation
    (let [watch-id (str (java.util.UUID/randomUUID))
          poll-interval (or (:poll-interval-ms opts) 5000)
          watched-branch current-branch  ; Capture current branch value
          running (atom true)

          poll-fn (fn []
                    (while @running
                      (Thread/sleep poll-interval)
                      (try
                        (let [state @state-atom
                              branch-data (get-in state [:branches watched-branch])
                              ipns-name (:ipns-name branch-data)
                              old-cid (:last-resolved-cid branch-data)
                              new-cid (name-resolve ipns-name)]

                          (when (and new-cid (not= old-cid new-cid))
                           ;; Update state
                            (swap! state-atom
                                   (fn [s]
                                     (-> s
                                         (assoc-in [:branches watched-branch :last-resolved-cid] new-cid)
                                         (assoc-in [:branches watched-branch :last-resolved-at] (System/currentTimeMillis)))))

                           ;; Trigger callback
                            (callback {:type :commit
                                       :branch watched-branch
                                       :snapshot-id new-cid})))
                        (catch Exception e
                          (println "Watch poll error:" (.getMessage e))))))

          watcher-thread (Thread. poll-fn)]

      ;; Start watcher
      (.start watcher-thread)

      ;; Store watcher info
      (swap! state-atom assoc-in [:watchers watch-id] {:thread watcher-thread
                                                       :running running})

      watch-id))

  (unwatch! [this watch-id]
    (let [state @state-atom
          watcher (get-in state [:watchers watch-id])]
      (when watcher
        (reset! (:running watcher) false)
        (swap! state-atom update :watchers dissoc watch-id)))
    this))

;; ============================================================
;; Public API
;; ============================================================

(defn init!
  "Initialize a new IPFS-backed Yggdrasil system.

   Options:
   - :system-name - unique identifier (default: 'default')
   - :branch - initial branch name (default: :main)
   - :root - optional initial data CID

   Returns IPFSSystem instance."
  ([] (init! {}))
  ([opts]
   (when-not (ipfs-available?)
     (throw (ex-info "IPFS daemon not running" {})))

   (let [system-name (or (:system-name opts) "default")
         state (load-state system-name)
         state-atom (atom state)
         ;; Use branch from opts, or from loaded state, or default to :main
         branch (keyword (or (:branch opts) (:current-branch state) :main))]

     ;; Validate/create IPNS keys for all branches in state
     ;; (state may persist between runs, but keys may have been deleted)
     (let [existing-keys (key-list)
           branches-to-fix (filter
                            (fn [[branch-name _]]
                              (not (contains? existing-keys (clojure.core/name branch-name))))
                            (:branches state))]
       (doseq [[branch-name branch-data] branches-to-fix]
         (let [key-name (clojure.core/name branch-name)
               key-id (key-gen key-name)
               ipns-name (str "/ipns/" key-id)]
           (swap! state-atom assoc-in [:branches branch-name :ipns-key] key-id)
           (swap! state-atom assoc-in [:branches branch-name :ipns-name] ipns-name)))
       (when (seq branches-to-fix)
         (save-state! @state-atom)))

     ;; Create system with branch field
     (let [sys (->IPFSSystem system-name state-atom branch)]
       ;; Create initial branch if needed
       (when-not (contains? (:branches state) branch)
         (p/branch! sys branch))

       sys))))

(defn commit!
  "Create a commit pointing to a data root CID.

   Required in opts:
   - :root - CID of the data (e.g., 'QmXxx...')

   Optional in opts:
   - :message - commit message (default: 'commit')
   - :author - override author
   - :timestamp - override timestamp

   Returns the system (updated)."
  ([sys message opts]
   (when-not (:root opts)
     (throw (ex-info "Commit requires :root CID in opts" {})))

   (let [state @(:state-atom sys)
         current-branch (:current-branch sys)
         current-cid (get-branch-head state current-branch)
         parents (if current-cid [current-cid] [])

         commit-obj (create-commit-object
                     (merge opts {:parents parents :message message}))
         commit-cid (store-commit! commit-obj)]

     ;; Update branch
     (swap! (:state-atom sys) update-branch! current-branch commit-cid)
     (save-state! @(:state-atom sys))

     sys)))

(defn destroy!
  "Clean up system resources.
   Optionally deletes state file and IPNS keys.

   Options:
   - :delete-state? - delete state file (default: false)
   - :delete-keys? - delete IPNS keys (default: false)"
  ([sys] (destroy! sys {}))
  ([sys opts]
   ;; Stop all watchers
   (let [state @(:state-atom sys)
         watchers (:watchers state)]
     (doseq [[watch-id _] watchers]
       (p/unwatch! sys watch-id)))

   ;; Delete IPNS keys if requested
   (when (:delete-keys? opts)
     (let [state @(:state-atom sys)
           branches (:branches state)]
       (doseq [[branch-name _] branches]
         (try
           (ipfs "key" "rm" (clojure.core/name branch-name))
           (catch Exception e
             (println "Warning: failed to remove IPNS key" branch-name ":" (.getMessage e)))))))

   ;; Delete state file if requested
   (when (:delete-state? opts)
     (let [state @(:state-atom sys)
           file (state-file (:system-name state))]
       (when (.exists file)
         (.delete file))))

   sys))
