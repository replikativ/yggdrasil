(ns yggdrasil.adapters.btrfs
  "Btrfs adapter for Yggdrasil protocols.

  Maps Btrfs COW concepts to yggdrasil:
    - Subvolume = branch (writable working copy)
    - Read-only snapshot = commit
    - Writable snapshot = branch creation

  Directory layout on Btrfs filesystem:
    <base-path>/
      branches/
        main/                 <- Btrfs subvolume (writable)
          entries/            <- user data
          .yggdrasil/
            commits.edn      <- commit log
        feature/              <- another branch subvolume
      snapshots/
        main/<uuid>/          <- read-only Btrfs snapshots
        feature/<uuid>/

  Key constraints:
    - Keep active snapshot count under ~100 (Btrfs degrades above this)
    - Automatic pruning when *max-snapshots* exceeded
    - Never enable qgroups on the filesystem
    - Pin to LTS kernels for stability"
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.watcher :as w]
            [clojure.java.shell :refer [sh]]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.edn :as edn]
            [clojure.set :as set])
  (:import [java.io File]
           [java.util UUID]
           [java.time Instant]
           [java.util.concurrent.locks ReentrantLock]))

;; ============================================================
;; Configuration
;; ============================================================

(def ^:dynamic *use-sudo*
  "When true, prefix btrfs commands with sudo. Default false since
   Btrfs supports user_subvol_rm_allowed mount option.
   Set to true if the filesystem lacks this option."
  false)

(def ^:dynamic *btrfs-binary*
  "Path to the btrfs binary."
  "btrfs")

(def ^:dynamic *max-snapshots*
  "Maximum snapshots per branch before pruning old ones.
   Set to 0 to disable pruning. Default 50."
  50)

;; ============================================================
;; CLI helpers
;; ============================================================

(defn- btrfs
  "Run a btrfs subcommand. Returns trimmed stdout on success.
   Uses sudo if *use-sudo* is true."
  [& args]
  (let [cmd (if *use-sudo*
              (into ["sudo" *btrfs-binary*] args)
              (into [*btrfs-binary*] args))
        result (apply sh cmd)]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (let [err (str/trim (:err result))]
        (throw (ex-info
                (cond
                  (str/includes? err "Permission denied")
                  (str "Btrfs permission denied. Ensure the filesystem is mounted with "
                       "'user_subvol_rm_allowed' or set *use-sudo* to true. Error: " err)

                  (str/includes? err "not a btrfs filesystem")
                  (str "Path is not on a Btrfs filesystem: " err)

                  :else
                  (str "btrfs error: " err))
                {:args (vec args) :exit (:exit result) :err err}))))))

(defn- subvolume-exists?
  "Check if a path is an existing Btrfs subvolume.
   Uses inode number check (subvolume roots always have inode 256)
   which works without elevated permissions."
  [path]
  (let [f (File. (str path))]
    (when (.exists f)
      (let [result (sh "stat" "--format=%i" (str path))]
        (and (zero? (:exit result))
             (= "256" (str/trim (:out result))))))))

(defn- delete-subvolume!
  "Delete a Btrfs subvolume. Handles read-only snapshots by first
   setting ro=false (required for unprivileged deletion)."
  [path]
  (when (subvolume-exists? path)
    ;; Make writable first (no-op if already writable, required for ro snapshots)
    (try (btrfs "property" "set" "-ts" path "ro" "false")
         (catch Exception _))
    (btrfs "subvolume" "delete" path)))

;; ============================================================
;; Path helpers
;; ============================================================

(defn- branch-path [base-path branch-name]
  (str base-path "/branches/" branch-name))

(defn- snapshots-path [base-path branch-name]
  (str base-path "/snapshots/" branch-name))

(defn- entries-path [base-path branch-name]
  (str (branch-path base-path branch-name) "/entries"))

(defn- meta-dir [base-path branch-name]
  (str (branch-path base-path branch-name) "/.yggdrasil"))

(defn- commits-file [base-path branch-name]
  (str (meta-dir base-path branch-name) "/commits.edn"))

;; ============================================================
;; Filesystem helpers
;; ============================================================

(defn- ensure-dirs! [& paths]
  (doseq [p paths]
    (.mkdirs (File. (str p)))))

(defn- delete-recursive! [path]
  (let [dir (File. (str path))]
    (when (.exists dir)
      (doseq [f (reverse (sort-by #(.getPath %) (file-seq dir)))]
        (.delete f)))))

(defn- list-files
  "List all regular files under dir, returning relative paths."
  [dir-path]
  (let [dir (File. (str dir-path))
        prefix-len (inc (count (.getPath dir)))]
    (if (.exists dir)
      (->> (file-seq dir)
           (filter #(.isFile %))
           (map #(subs (.getPath %) prefix-len))
           set)
      #{})))

;; ============================================================
;; Commit log management
;; ============================================================

(defn- read-commits [base-path branch-name]
  (let [f (File. (commits-file base-path branch-name))]
    (if (.exists f)
      (edn/read-string (slurp f))
      [])))

(defn- write-commits! [base-path branch-name commits]
  (ensure-dirs! (meta-dir base-path branch-name))
  (spit (commits-file base-path branch-name) (pr-str commits)))

(defn- latest-commit [base-path branch-name]
  (last (read-commits base-path branch-name)))

;; ============================================================
;; Snapshot pruning
;; ============================================================

(defn- prune-snapshots!
  "Delete oldest snapshot subvolumes when count exceeds max.
   Keeps commit entries in EDN for history tracking."
  [base-path branch-name max-snaps]
  (when (pos? max-snaps)
    (let [commits (read-commits base-path branch-name)
          snap-dir (snapshots-path base-path branch-name)]
      (when (> (count commits) max-snaps)
        (let [to-prune (take (- (count commits) max-snaps) commits)]
          (doseq [c to-prune]
            (let [snap-path (str snap-dir "/" (:id c))]
              (try (delete-subvolume! snap-path)
                   (catch Exception _)))))))))

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
  [base-path current-branch last-state]
  (let [branch current-branch
        current-commits (read-commits base-path branch)
        current-branches (->> (File. (str base-path "/branches"))
                              (.listFiles)
                              (filter #(.isDirectory %))
                              (map #(.getName %))
                              set)
        prev-commits (or (:commits last-state) [])
        prev-branches (or (:branches last-state) #{})
        new-commits (drop (count prev-commits) current-commits)
        events (cond-> []
                 (seq new-commits)
                 (into (for [c new-commits]
                         {:type :commit
                          :snapshot-id (:id c)
                          :branch branch
                          :timestamp (System/currentTimeMillis)}))

                 true
                 (into (for [b (set/difference current-branches prev-branches)]
                         {:type :branch-created
                          :branch b
                          :timestamp (System/currentTimeMillis)}))

                 true
                 (into (for [b (set/difference prev-branches current-branches)]
                         {:type :branch-deleted
                          :branch b
                          :timestamp (System/currentTimeMillis)})))]
    {:state {:commits current-commits
             :branches current-branches}
     :events events}))

;; ============================================================
;; System record
;; ============================================================

(defrecord BtrfsSystem [base-path current-branch system-name
                        watcher-state branch-locks max-snapshots]
  p/SystemIdentity
  (system-id [_] (or system-name (str "btrfs:" base-path)))
  (system-type [_] :btrfs)
  (capabilities [_]
    (t/->Capabilities true true true true false true))

  p/Snapshotable
  (snapshot-id [_]
    (:id (latest-commit base-path current-branch)))

  (parent-ids [_]
    (or (:parent-ids (latest-commit base-path current-branch)) #{}))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Return the snapshot subvolume path for read-only access
    (let [branch current-branch
          commits (read-commits base-path branch)
          commit (first (filter #(= (:id %) (str snap-id)) commits))]
      (when commit
        (let [snap-path (str (snapshots-path base-path branch) "/" (:id commit))]
          (when (.exists (File. snap-path))
            snap-path)))))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [branch current-branch
          commits (read-commits base-path branch)
          commit (first (filter #(= (:id %) (str snap-id)) commits))]
      (when commit
        {:snapshot-id (:id commit)
         :parent-ids (or (:parent-ids commit) #{})
         :timestamp (:timestamp commit)
         :message (:message commit)})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [dir (File. (str base-path "/branches"))]
      (if (.exists dir)
        (->> (.listFiles dir)
             (filter #(.isDirectory %))
             (map #(keyword (.getName %)))
             set)
        #{:main})))

  (current-branch [_]
    (keyword current-branch))

  (branch! [this name]
    ;; Create writable Btrfs snapshot of current branch subvolume
    (let [branch-str (clojure.core/name name)
          source-branch current-branch
          src-subvol (branch-path base-path source-branch)
          dest-subvol (branch-path base-path branch-str)]
      (ensure-dirs! (snapshots-path base-path branch-str))
      (btrfs "subvolume" "snapshot" src-subvol dest-subvol)
      ;; Copy snapshot subvolumes from source so as-of works
      (let [source-commits (read-commits base-path source-branch)
            source-snap-dir (snapshots-path base-path source-branch)
            dest-snap-dir (snapshots-path base-path branch-str)]
        (doseq [c source-commits]
          (let [src-snap (str source-snap-dir "/" (:id c))
                dst-snap (str dest-snap-dir "/" (:id c))]
            (when (and (subvolume-exists? src-snap)
                       (not (subvolume-exists? dst-snap)))
              ;; Create read-only snapshot of the source snapshot
              (btrfs "subvolume" "snapshot" "-r" src-snap dst-snap)))))
      this))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    ;; Branch from a specific snapshot
    (let [branch-str (clojure.core/name name)
          source-branch current-branch
          snap-path (str (snapshots-path base-path source-branch) "/" from)
          dest-subvol (branch-path base-path branch-str)]
      (ensure-dirs! (snapshots-path base-path branch-str))
      (if (subvolume-exists? snap-path)
        (do
          ;; Snapshot a read-only snap -> creates read-only copy; make writable
          (btrfs "subvolume" "snapshot" snap-path dest-subvol)
          (btrfs "property" "set" "-ts" dest-subvol "ro" "false"))
        ;; Fallback: snapshot current branch and truncate history
        (btrfs "subvolume" "snapshot"
               (branch-path base-path source-branch) dest-subvol))
      ;; Truncate commits to only include up to 'from'
      (let [source-commits (read-commits base-path source-branch)
            idx (inc (.indexOf (mapv :id source-commits) (str from)))
            inherited (if (pos? idx) (vec (take idx source-commits)) [])]
        (write-commits! base-path branch-str inherited)
        ;; Copy relevant snapshot subvolumes
        (let [source-snap-dir (snapshots-path base-path source-branch)
              dest-snap-dir (snapshots-path base-path branch-str)]
          (doseq [c inherited]
            (let [src-snap (str source-snap-dir "/" (:id c))
                  dst-snap (str dest-snap-dir "/" (:id c))]
              (when (and (subvolume-exists? src-snap)
                         (not (subvolume-exists? dst-snap)))
                (btrfs "subvolume" "snapshot" "-r" src-snap dst-snap))))))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)
          snap-dir (snapshots-path base-path branch-str)
          subvol (branch-path base-path branch-str)]
      ;; Delete all snapshot subvolumes first
      (when (.exists (File. snap-dir))
        (doseq [f (.listFiles (File. snap-dir))]
          (try (delete-subvolume! (.getPath f))
               (catch Exception _))))
      ;; Delete the branch subvolume itself
      (try (delete-subvolume! subvol)
           (catch Exception _))
      ;; Clean up snapshot directory
      (delete-recursive! snap-dir)
      ;; Clean up branch directory (in case subvolume delete failed or left empty dir)
      (delete-recursive! subvol)
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (when-not (.exists (File. (branch-path base-path branch-str)))
        (throw (ex-info (str "Branch not found: " branch-str)
                        {:branch branch-str})))
      (->BtrfsSystem base-path branch-str system-name
                     watcher-state branch-locks max-snapshots)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [commits (read-commits base-path current-branch)
          commits (vec (rseq (vec commits)))
          commits (if-let [limit (:limit opts)]
                    (vec (take limit commits))
                    commits)]
      (mapv :id commits)))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [commits (read-commits base-path current-branch)
          idx (.indexOf (mapv :id commits) (str snap-id))]
      (if (pos? idx)
        (mapv :id (take idx commits))
        [])))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [commits (read-commits base-path current-branch)
          ids (mapv :id commits)
          idx-a (.indexOf ids (str a))
          idx-b (.indexOf ids (str b))]
      (and (>= idx-a 0) (>= idx-b 0) (< idx-a idx-b))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    ;; Search across all branches for common ancestor
    (let [all-branches (->> (File. (str base-path "/branches"))
                            (.listFiles)
                            (filter #(.isDirectory %))
                            (map #(.getName %)))
          find-history (fn [snap-id]
                         (let [branch (first (filter
                                              #(some (fn [c] (= (:id c) (str snap-id)))
                                                     (read-commits base-path %))
                                              all-branches))]
                           (when branch
                             (let [commits (read-commits base-path branch)
                                   idx (.indexOf (mapv :id commits) (str snap-id))]
                               (when (>= idx 0)
                                 (set (map :id (take (inc idx) commits))))))))
          history-a (find-history a)
          history-b (find-history b)]
      (when (and history-a history-b)
        (let [common (set/intersection history-a history-b)]
          (when (seq common)
            (let [any-branch (first (filter
                                     #(some (fn [c] (common (:id c)))
                                            (read-commits base-path %))
                                     all-branches))
                  commits (read-commits base-path any-branch)
                  ordered (filter #(common (:id %)) commits)]
              (:id (last ordered))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [commits (read-commits base-path current-branch)
          nodes (into {}
                      (map (fn [c]
                             [(:id c) {:parent-ids (or (:parent-ids c) #{})
                                       :meta (dissoc c :id :parent-ids)}])
                           commits))]
      {:nodes nodes
       :branches {(keyword current-branch)
                  (:id (last commits))}
       :roots (if (seq commits) #{(:id (first commits))} #{})}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (when-let [meta (p/snapshot-meta this snap-id)]
      (dissoc meta :snapshot-id)))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    ;; File-level merge: copy entries from source branch to current
    (let [source-branch (if (keyword? source) (clojure.core/name source) (str source))
          dest-branch current-branch
          source-entries (entries-path base-path source-branch)
          dest-entries (entries-path base-path dest-branch)]
      (when (.exists (File. source-entries))
        (let [result (sh "cp" "-a" (str source-entries "/.") (str dest-entries "/"))]
          (when-not (zero? (:exit result))
            (throw (ex-info "merge cp failed" {:err (:err result)})))))
      ;; Auto-commit the merge
      (let [source-commits (read-commits base-path source-branch)
            dest-commits (read-commits base-path dest-branch)
            merge-id (str (UUID/randomUUID))
            parent-ids (set (remove nil?
                                    [(:id (last dest-commits))
                                     (:id (last source-commits))]))
            merge-commit {:id merge-id
                          :parent-ids parent-ids
                          :message (or (:message opts) (str "Merge " source-branch))
                          :timestamp (str (Instant/now))
                          :branch dest-branch}
            new-commits (conj dest-commits merge-commit)]
        ;; Write metadata before snapshot
        (write-commits! base-path dest-branch new-commits)
        ;; Create read-only snapshot
        (let [snap-dir (snapshots-path base-path dest-branch)
              snap-path (str snap-dir "/" merge-id)]
          (ensure-dirs! snap-dir)
          (btrfs "subvolume" "snapshot" "-r"
                 (branch-path base-path dest-branch) snap-path))
        ;; Prune if needed
        (prune-snapshots! base-path dest-branch (or max-snapshots *max-snapshots*))
        this)))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [this a b _opts]
    ;; Files modified on both sides since their common ancestor
    (let [all-branches (->> (File. (str base-path "/branches"))
                            (.listFiles)
                            (filter #(.isDirectory %))
                            (map #(.getName %)))
          find-branch (fn [snap-id]
                        (first (filter
                                #(some (fn [c] (= (:id c) (str snap-id)))
                                       (read-commits base-path %))
                                all-branches)))
          branch-a (find-branch a)
          branch-b (find-branch b)]
      (if (and branch-a branch-b)
        (let [ancestor-id (p/common-ancestor this a b)
              ancestor-files (if ancestor-id
                               (let [ancestor-branch (find-branch ancestor-id)
                                     snap-path (str (snapshots-path base-path ancestor-branch)
                                                    "/" ancestor-id "/entries")]
                                 (list-files snap-path))
                               #{})
              files-a (set/difference
                       (list-files (entries-path base-path branch-a))
                       ancestor-files)
              files-b (set/difference
                       (list-files (entries-path base-path branch-b))
                       ancestor-files)
              conflicts (set/intersection files-a files-b)]
          (mapv (fn [path] {:path (str/split path #"/")
                            :ours path
                            :theirs path})
                conflicts))
        [])))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (let [all-branches (->> (File. (str base-path "/branches"))
                            (.listFiles)
                            (filter #(.isDirectory %))
                            (map #(.getName %)))
          find-branch (fn [snap-id]
                        (first (filter
                                #(some (fn [c] (= (:id c) (str snap-id)))
                                       (read-commits base-path %))
                                all-branches)))
          branch-a (find-branch a)
          branch-b (find-branch b)]
      {:snapshot-a (str a)
       :snapshot-b (str b)
       :files-a (when branch-a (list-files (entries-path base-path branch-a)))
       :files-b (when branch-b (list-files (entries-path base-path branch-b)))}))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn base-path current-branch)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id)))

;; ============================================================
;; Factory functions
;; ============================================================

(defn create
  "Create a Btrfs adapter for an existing workspace."
  ([base-path] (create base-path {}))
  ([base-path opts]
   (->BtrfsSystem base-path
                  (or (:initial-branch opts) "main")
                  (:system-name opts)
                  (w/create-watcher-state)
                  (atom {})
                  (or (:max-snapshots opts) *max-snapshots*))))

(defn init!
  "Initialize a new Btrfs workspace.
   base-path must be on a Btrfs filesystem.
   Creates the directory structure and 'main' branch subvolume."
  ([base-path] (init! base-path {}))
  ([base-path opts]
   (ensure-dirs! (str base-path "/branches")
                 (str base-path "/snapshots")
                 (str base-path "/snapshots/main"))
   ;; Create main branch as a Btrfs subvolume
   (when-not (subvolume-exists? (branch-path base-path "main"))
     (btrfs "subvolume" "create" (branch-path base-path "main")))
   ;; Create entries and metadata dirs inside the subvolume
   (ensure-dirs! (entries-path base-path "main")
                 (meta-dir base-path "main"))
   ;; Initialize empty commits log
   (write-commits! base-path "main" [])
   (create base-path opts)))

(defn destroy!
  "Destroy the entire workspace. Deletes all subvolumes and snapshots."
  [^BtrfsSystem sys]
  (w/stop-polling! (:watcher-state sys))
  (let [base (:base-path sys)]
    ;; Delete all snapshot subvolumes first
    (let [snap-base (str base "/snapshots")]
      (when (.exists (File. snap-base))
        (doseq [branch-dir (.listFiles (File. snap-base))]
          (when (.isDirectory branch-dir)
            (doseq [snap (.listFiles branch-dir)]
              (try (delete-subvolume! (.getPath snap))
                   (catch Exception _)))))))
    ;; Delete all branch subvolumes
    (let [branch-base (str base "/branches")]
      (when (.exists (File. branch-base))
        (doseq [branch (.listFiles (File. branch-base))]
          (try (delete-subvolume! (.getPath branch))
               (catch Exception _)))))
    ;; Clean up regular directories
    (delete-recursive! base)))

(defn commit!
  "Create a commit on the current branch.
   Takes a read-only Btrfs snapshot and records metadata."
  ([sys] (commit! sys nil))
  ([sys message]
   (with-branch-lock* (:branch-locks sys) (:current-branch sys)
     (fn []
       (let [{:keys [base-path current-branch max-snapshots]} sys
             branch current-branch
             commit-id (str (UUID/randomUUID))
             commits (read-commits base-path branch)
             parent-ids (if-let [prev (last commits)]
                          #{(:id prev)}
                          #{})
             commit {:id commit-id
                     :parent-ids parent-ids
                     :message (or message "")
                     :timestamp (str (Instant/now))
                     :branch branch}
             branch-subvol (branch-path base-path branch)
             snap-dir (snapshots-path base-path branch)
             snap-path (str snap-dir "/" commit-id)]
         ;; Write metadata before snapshot so snapshot captures it
         (write-commits! base-path branch (conj commits commit))
         ;; Create read-only Btrfs snapshot
         (ensure-dirs! snap-dir)
         (btrfs "subvolume" "snapshot" "-r" branch-subvol snap-path)
         ;; Prune old snapshots if needed
         (prune-snapshots! base-path branch (or max-snapshots *max-snapshots*))
         commit-id)))))

(defn write-file!
  "Write a file to the current branch's entries directory."
  [sys relative-path content]
  (let [branch (:current-branch sys)
        f (File. (str (entries-path (:base-path sys) branch) "/" relative-path))]
    (.mkdirs (.getParentFile f))
    (spit f content)))

(defn read-file
  "Read a file from the current branch's entries directory."
  [sys relative-path]
  (let [branch (:current-branch sys)
        f (File. (str (entries-path (:base-path sys) branch) "/" relative-path))]
    (when (.exists f)
      (slurp f))))

(defn delete-file!
  "Delete a file from the current branch's entries directory."
  [sys relative-path]
  (let [branch (:current-branch sys)
        f (File. (str (entries-path (:base-path sys) branch) "/" relative-path))]
    (when (.exists f)
      (.delete f))))

(defn btrfs-available?
  "Check if btrfs CLI is available and a path is on a Btrfs filesystem."
  ([]
   (try
     (zero? (:exit (sh *btrfs-binary* "--version")))
     (catch Exception _ false)))
  ([test-path]
   (try
     (and (btrfs-available?)
          (let [cmd (if *use-sudo*
                      ["sudo" *btrfs-binary* "filesystem" "usage" test-path]
                      [*btrfs-binary* "filesystem" "usage" test-path])]
            (zero? (:exit (apply sh cmd)))))
     (catch Exception _ false))))
