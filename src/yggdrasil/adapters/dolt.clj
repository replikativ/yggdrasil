(ns yggdrasil.adapters.dolt
  "Dolt adapter for Yggdrasil protocols.

  Maps Dolt (git-for-data) concepts to yggdrasil:
    - Dolt repository = system workspace
    - Dolt branch = yggdrasil branch
    - Dolt commit = yggdrasil snapshot
    - entries table (k VARCHAR, v TEXT) = key-value storage

  Dolt provides:
    - O(1) branching via structural sharing (Prolly Trees)
    - Cell-level three-way merge
    - Full commit DAG with merge-base
    - SQL interface for structured queries

  Requirements:
    - `dolt` binary on PATH
    - No server needed (CLI operates on local repo directory)"
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

(def ^:dynamic *dolt-binary*
  "Path to the dolt binary."
  "dolt")

(def ^:dynamic *author*
  "Default commit author for dolt commits."
  "yggdrasil <yggdrasil@localhost>")

;; ============================================================
;; CLI helpers
;; ============================================================

(defn- dolt
  "Run a dolt command in the given repo directory.
   Returns trimmed stdout on success, throws on failure."
  [repo-path & args]
  (let [cmd (into [*dolt-binary*] args)
        result (apply sh (concat cmd [:dir repo-path
                                      :env (merge (into {} (System/getenv))
                                                  {"NO_COLOR" "1"
                                                   "DOLT_SILENCE_USER_REQ_FOR_TESTING" "1"})]))]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (throw (ex-info (str "dolt error: " (str/trim (:err result)))
                      {:args (vec args) :exit (:exit result)
                       :err (str/trim (:err result))})))))

(defn- dolt-sql
  "Run a SQL query via dolt and return CSV rows as vectors of strings.
   First row is headers, rest are data rows."
  [repo-path query]
  (let [output (dolt repo-path "sql" "-q" query "-r" "csv")]
    (when-not (str/blank? output)
      (mapv #(str/split % #",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
            (str/split-lines output)))))

(defn- dolt-sql-values
  "Run a SQL query and return data rows only (skip header)."
  [repo-path query]
  (let [rows (dolt-sql repo-path query)]
    (when (and rows (> (count rows) 1))
      (subvec rows 1))))

;; ============================================================
;; Path helpers
;; ============================================================

(defn- parse-author
  "Parse 'Name <email>' into [name email]."
  [author-str]
  (if-let [[_ name email] (re-matches #"(.+?)\s*<(.+?)>" author-str)]
    [name email]
    ["yggdrasil" "yggdrasil@localhost"]))

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

(defn- current-head
  "Get the current HEAD commit. If branch is provided, get HEAD for that specific branch."
  ([repo-path] (current-head repo-path nil))
  ([repo-path branch]
   (let [query (if branch
                 (str "SELECT commit_hash FROM dolt_log('" branch "') LIMIT 1")
                 "SELECT commit_hash FROM dolt_log LIMIT 1")
         rows (dolt-sql-values repo-path query)]
     (when (seq rows)
       (first (first rows))))))

(defn- list-branches [repo-path]
  (let [rows (dolt-sql-values repo-path
                              "SELECT name FROM dolt_branches")]
    (set (map first rows))))

(defn- poll-fn
  [repo-path current-branch last-state]
  (let [branch current-branch
        head (current-head repo-path)
        branches (list-branches repo-path)
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
     :events events}))

;; ============================================================
;; System record
;; ============================================================

(defrecord DoltSystem [repo-path current-branch system-name
                       watcher-state branch-locks init-commit]
  p/SystemIdentity
  (system-id [_] (or system-name (str "dolt:" repo-path)))
  (system-type [_] :dolt)
  (capabilities [_]
    (t/->Capabilities true true true true false true))

  p/Snapshotable
  (snapshot-id [_]
    (current-head repo-path current-branch))

  (parent-ids [_]
    (let [head (current-head repo-path current-branch)]
      (if head
        (let [rows (dolt-sql-values repo-path
                                    (str "SELECT parent_hash FROM dolt_commit_ancestors "
                                         "WHERE commit_hash = '" head "' ORDER BY parent_index"))
              parents (set (remove str/blank? (map first rows)))]
          ;; Filter out the init commit (transparent bootstrap)
          (disj parents init-commit))
        #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Dolt doesn't have separate snapshot directories;
    ;; return the commit hash as a ref that can be used with `dolt checkout <hash>`
    (let [rows (dolt-sql-values repo-path
                                (str "SELECT commit_hash FROM dolt_log WHERE commit_hash = '"
                                     snap-id "'"))]
      (when (seq rows)
        (str snap-id))))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [rows (dolt-sql-values repo-path
                                (str "SELECT commit_hash, message, date FROM dolt_log "
                                     "WHERE commit_hash = '" snap-id "'"))]
      (when (seq rows)
        (let [[hash msg date] (first rows)
              parents (dolt-sql-values repo-path
                                       (str "SELECT parent_hash FROM dolt_commit_ancestors "
                                            "WHERE commit_hash = '" snap-id "' ORDER BY parent_index"))]
          {:snapshot-id hash
           :parent-ids (set (remove str/blank? (map first parents)))
           :timestamp date
           :message msg}))))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (set (map keyword (list-branches repo-path))))

  (current-branch [_]
    (keyword current-branch))

  (branch! [this name]
    (let [branch-str (clojure.core/name name)]
      (dolt repo-path "branch" branch-str)
      this))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (let [branch-str (clojure.core/name name)]
      (dolt repo-path "branch" branch-str (str from))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (dolt repo-path "branch" "-D" branch-str)
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)]
      ;; Just return a new system pointing to the branch
      ;; Actual checkout happens during write operations
      (->DoltSystem repo-path branch-str system-name
                    watcher-state branch-locks init-commit)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [limit (or (:limit opts) 1000)
          rows (dolt-sql-values repo-path
                                (str "SELECT commit_hash FROM dolt_log LIMIT " (+ limit 10)))
          ids (filterv #(not= % init-commit) (mapv first rows))]
      (vec (take limit ids))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    ;; All ancestors reachable from snap-id (excluding itself and init-commit)
    (let [all-rows (dolt-sql-values repo-path
                                    "SELECT commit_hash FROM dolt_log")
          all-ids (filterv #(not= % init-commit) (mapv first all-rows))
          idx (.indexOf all-ids (str snap-id))]
      (if (>= idx 0)
        (vec (drop (inc idx) all-ids))
        [])))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    ;; a is ancestor of b if a appears after b in the log
    (let [result (dolt repo-path "merge-base" (str a) (str b))]
      (= (str/trim result) (str a))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (try
      (let [result (dolt repo-path "merge-base" (str a) (str b))]
        (when-not (str/blank? result)
          result))
      (catch Exception _ nil)))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [log-rows (dolt-sql-values repo-path
                                    "SELECT commit_hash, message FROM dolt_log")
          ancestor-rows (dolt-sql-values repo-path
                                         "SELECT commit_hash, parent_hash FROM dolt_commit_ancestors")
          parent-map (reduce (fn [m [hash parent]]
                               (if (str/blank? parent)
                                 m
                                 (update m hash (fnil conj #{}) parent)))
                             {} ancestor-rows)
          nodes (into {}
                      (map (fn [[hash msg]]
                             [hash {:parent-ids (or (get parent-map hash) #{})
                                    :meta {:message msg}}])
                           log-rows))
          branch-rows (dolt-sql-values repo-path
                                       "SELECT name, hash FROM dolt_branches")
          branches (into {} (map (fn [[name hash]] [(keyword name) hash]) branch-rows))
          roots (set (keep (fn [[hash {:keys [parent-ids]}]]
                             (when (empty? parent-ids) hash))
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
          [author-name author-email] (parse-author *author*)]
      ;; Checkout target branch before merging
      (dolt repo-path "checkout" current-branch)
      (dolt repo-path "merge" source-branch
            "--author" (str author-name " <" author-email ">"))
      this))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    ;; Check for actual row-level conflicts: same key modified on both sides
    (try
      (let [base (str/trim (dolt repo-path "merge-base" (str a) (str b)))
            ;; Get keys modified on each side since the common ancestor
            keys-a (when-not (str/blank? base)
                     (let [rows (dolt-sql-values repo-path
                                                 (str "SELECT to_k FROM dolt_diff_entries "
                                                      "WHERE from_commit = '" base
                                                      "' AND to_commit = '" a "'"))]
                       (set (map first rows))))
            keys-b (when-not (str/blank? base)
                     (let [rows (dolt-sql-values repo-path
                                                 (str "SELECT to_k FROM dolt_diff_entries "
                                                      "WHERE from_commit = '" base
                                                      "' AND to_commit = '" b "'"))]
                       (set (map first rows))))
            conflicts (if (and keys-a keys-b)
                        (set/intersection keys-a keys-b)
                        #{})]
        (mapv (fn [k] {:path ["entries" k] :ours k :theirs k}) conflicts))
      (catch Exception _ [])))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (try
      (let [output (dolt repo-path "diff" (str a) (str b) "--stat")]
        {:snapshot-a (str a)
         :snapshot-b (str b)
         :stat output})
      (catch Exception _
        {:snapshot-a (str a) :snapshot-b (str b) :stat ""})))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn repo-path current-branch)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id)))

;; ============================================================
;; Factory functions
;; ============================================================

(defn- find-init-commit
  "Find the root commit (the one with no parents) in the repo."
  [repo-path]
  (let [rows (dolt-sql-values repo-path
                              (str "SELECT ca.commit_hash FROM dolt_log ca "
                                   "WHERE NOT EXISTS (SELECT 1 FROM dolt_commit_ancestors a "
                                   "WHERE a.commit_hash = ca.commit_hash AND a.parent_hash != '')"))]
    (when (seq rows)
      (first (first rows)))))

(defn create
  "Create a Dolt adapter for an existing repository."
  ([repo-path] (create repo-path {}))
  ([repo-path opts]
   (let [branch (or (:initial-branch opts) "main")
         init-hash (or (:init-commit opts) (find-init-commit repo-path))]
     ;; Ensure we're on the right branch
     (try (dolt repo-path "checkout" branch)
          (catch Exception _))
     (->DoltSystem repo-path
                   branch
                   (:system-name opts)
                   (w/create-watcher-state)
                   (atom {})
                   init-hash))))

(defn init!
  "Initialize a new Dolt repository with an entries table.
   Creates the repo directory if it doesn't exist."
  ([repo-path] (init! repo-path {}))
  ([repo-path opts]
   (let [dir (File. repo-path)
         [author-name author-email] (parse-author *author*)]
     (.mkdirs dir)
     ;; Initialize dolt repo (creates bootstrap commit)
     (dolt repo-path "init" "--name" author-name "--email" author-email)
     (let [init-hash (current-head repo-path)]
       ;; Create entries table (left uncommitted - first user commit picks it up)
       (dolt repo-path "sql" "-q"
             "CREATE TABLE entries (k VARCHAR(255) PRIMARY KEY, v TEXT)")
       (create repo-path (assoc opts :init-commit init-hash))))))

(defn destroy!
  "Destroy the Dolt repository (deletes the repo directory)."
  [^DoltSystem sys]
  (w/stop-polling! (:watcher-state sys))
  (let [dir (File. (:repo-path sys))]
    (when (.exists dir)
      (doseq [f (reverse (sort-by #(.getPath %) (file-seq dir)))]
        (.delete f)))))

(defn commit!
  "Create a commit on the current branch.
   Stages all changes and commits."
  ([sys] (commit! sys nil))
  ([sys message]
   (with-branch-lock* (:branch-locks sys) (:current-branch sys)
     (fn []
       (let [repo (:repo-path sys)
             branch (:current-branch sys)
             [author-name author-email] (parse-author *author*)
             msg (or message "commit")]
         ;; Checkout the branch first
         (dolt repo "checkout" branch)
         ;; Stage all changes
         (dolt repo "add" ".")
         ;; Commit
         (dolt repo "commit" "-m" msg
               "--author" (str author-name " <" author-email ">")
               "--allow-empty")
         (current-head repo branch))))))

(defn write-entry!
  "Write a key-value entry to the entries table."
  [sys key value]
  (let [repo (:repo-path sys)
        branch (:current-branch sys)
        escaped-key (str/replace key "'" "''")
        escaped-val (str/replace (str value) "'" "''")]
    ;; Checkout branch before write
    (dolt repo "checkout" branch)
    (dolt repo "sql" "-q"
          (str "REPLACE INTO entries (k, v) VALUES ('" escaped-key "', '" escaped-val "')"))))

(defn read-entry
  "Read a value by key from the entries table."
  [sys key]
  (let [repo (:repo-path sys)
        branch (:current-branch sys)
        escaped-key (str/replace key "'" "''")]
    ;; Checkout branch to read current working tree state
    (dolt repo "checkout" branch)
    (let [rows (dolt-sql-values repo
                                (str "SELECT v FROM entries WHERE k = '" escaped-key "'"))]
      (when (seq rows)
        (first (first rows))))))

(defn delete-entry!
  "Delete an entry by key from the entries table."
  [sys key]
  (let [repo (:repo-path sys)
        branch (:current-branch sys)
        escaped-key (str/replace key "'" "''")]
    ;; Checkout branch before write
    (dolt repo "checkout" branch)
    (dolt repo "sql" "-q"
          (str "DELETE FROM entries WHERE k = '" escaped-key "'"))))

(defn count-entries
  "Count the number of entries in the entries table."
  [sys]
  (let [repo (:repo-path sys)
        branch (:current-branch sys)]
    ;; Checkout branch to read current working tree state
    (dolt repo "checkout" branch)
    (let [rows (dolt-sql-values repo "SELECT COUNT(*) FROM entries")]
      (if (seq rows)
        (Integer/parseInt (first (first rows)))
        0))))

(defn dolt-available?
  "Check if the dolt binary is available."
  []
  (try
    (zero? (:exit (sh *dolt-binary* "version")))
    (catch Exception _ false)))
