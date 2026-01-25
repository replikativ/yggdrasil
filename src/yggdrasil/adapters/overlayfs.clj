(ns yggdrasil.adapters.overlayfs
  "OverlayFS + Bubblewrap adapter for Yggdrasil protocols.

  Provides copy-on-write filesystem branching with optional process isolation
  via bubblewrap (bwrap). Each branch gets a separate 'upper' directory overlaid
  on a shared read-only base directory.

  Architecture:
    - base-path: Read-only lower layer (shared across all branches)
    - workspace-path/branches/<name>/upper: Per-branch writable layer
    - workspace-path/branches/<name>/work: OverlayFS workdir
    - workspace-path/branches/<name>/snapshots/<uuid>: Archived commit states
    - workspace-path/branches/<name>/commits.edn: Commit metadata

  Process isolation via bubblewrap:
    - exec! runs commands in a user-namespace sandbox
    - Overlay is mounted inside the sandbox (no root required)
    - Network, PID, and mount isolation configurable

  Merge strategy:
    - Files in upper override base (standard overlayfs semantics)
    - Whiteout files (char device 0/0) represent deletions
    - Three-way merge: diff upper vs base, apply non-conflicting to target"
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

(defn- copy-recursive!
  "Copy directory tree from src to dest."
  [src dest]
  (let [src-file (File. (str src))
        dest-file (File. (str dest))]
    (when (.exists src-file)
      (.mkdirs dest-file)
      (let [result (sh "cp" "-a" (str src "/.")  (str dest "/"))]
        (when-not (zero? (:exit result))
          (throw (ex-info "cp failed" {:src src :dest dest :err (:err result)})))))))

(defn- list-upper-files
  "List all files in the upper directory relative to it.
   Returns a set of relative paths."
  [upper-path]
  (let [upper (File. (str upper-path))
        prefix-len (inc (count (.getPath upper)))]
    (if (.exists upper)
      (->> (file-seq upper)
           (filter #(.isFile %))
           (map #(subs (.getPath %) prefix-len))
           set)
      #{})))

(defn- is-whiteout?
  "Check if a file is an overlayfs whiteout (char device 0/0).
   In user-namespace overlays, whiteouts are represented as zero-byte
   files with a specific xattr, or as char(0,0) devices."
  [path]
  (let [f (File. (str path))]
    (and (.exists f)
         (.isFile f)
         (zero? (.length f))
         ;; Check for overlayfs trusted.overlay.whiteout xattr
         ;; In practice, user-ns whiteouts are just zero-byte files
         ;; with the overlay.whiteout xattr set
         (let [result (sh "getfattr" "-n" "trusted.overlay.whiteout"
                          (str path) :env {"LANG" "C"})]
           (zero? (:exit result))))))

;; ============================================================
;; Branch directory management
;; ============================================================

(defn- branch-dir [workspace-path branch-name]
  (str workspace-path "/branches/" branch-name))

(defn- upper-dir [workspace-path branch-name]
  (str (branch-dir workspace-path branch-name) "/upper"))

(defn- work-dir [workspace-path branch-name]
  (str (branch-dir workspace-path branch-name) "/work"))

(defn- snapshots-dir [workspace-path branch-name]
  (str (branch-dir workspace-path branch-name) "/snapshots"))

(defn- commits-file [workspace-path branch-name]
  (str (branch-dir workspace-path branch-name) "/commits.edn"))

;; ============================================================
;; Commit log management
;; ============================================================

(defn- read-commits [workspace-path branch-name]
  (let [f (File. (commits-file workspace-path branch-name))]
    (if (.exists f)
      (edn/read-string (slurp f))
      [])))

(defn- write-commits! [workspace-path branch-name commits]
  (spit (commits-file workspace-path branch-name) (pr-str commits)))

(defn- latest-commit [workspace-path branch-name]
  (last (read-commits workspace-path branch-name)))

;; ============================================================
;; Execution via bubblewrap
;; ============================================================

(defn exec!
  "Execute a command inside a bubblewrap sandbox with the overlay mounted.

   The overlay merges the branch's upper directory on top of base-path,
   presenting a unified view at mount-point (default: base-path).

   Options:
     :net?      - allow network access (default: false)
     :env       - environment variables map
     :dir       - working directory inside sandbox
     :timeout   - timeout in milliseconds

   Returns: {:exit int, :out string, :err string}"
  [sys cmd & {:keys [net? env dir timeout]
              :or {net? false}}]
  (let [{:keys [base-path workspace-path current-branch-atom]} sys
        branch @current-branch-atom
        upper (upper-dir workspace-path branch)
        work (work-dir workspace-path branch)]
    ;; Ensure work dir is clean (overlayfs requires empty workdir)
    (delete-recursive! work)
    (ensure-dirs! work)
    (let [bwrap-args (cond-> ["bwrap"
                              "--die-with-parent"
                              ;; Mount proc and dev
                              "--proc" "/proc"
                              "--dev" "/dev"
                              ;; Bind /tmp
                              "--tmpfs" "/tmp"
                              ;; Set up overlay: base as lower, upper as rw
                              "--overlay-src" base-path
                              "--overlay" upper work base-path]
                      ;; Network isolation
                       (not net?)
                       (conj "--unshare-net")
                      ;; Bind common system dirs read-only
                       true
                       (into ["--ro-bind" "/usr" "/usr"
                              "--ro-bind" "/lib" "/lib"
                              "--ro-bind" "/lib64" "/lib64"
                              "--ro-bind" "/bin" "/bin"
                              "--ro-bind" "/sbin" "/sbin"
                              "--ro-bind" "/etc" "/etc"])
                      ;; Working directory
                       dir
                       (into ["--chdir" dir])
                      ;; The command
                       true
                       (into ["--" "sh" "-c" (str/join " " cmd)]))]
      (let [env-map (merge {"HOME" "/tmp"
                            "PATH" "/usr/local/bin:/usr/bin:/bin"}
                           env)
            result (apply sh (concat bwrap-args
                                     [:env env-map]))]
        {:exit (:exit result)
         :out (str/trim (:out result))
         :err (str/trim (:err result))}))))

;; ============================================================
;; Polling watcher
;; ============================================================

(defn- poll-fn
  [workspace-path current-branch-atom last-state]
  (let [branch @current-branch-atom
        current-commits (read-commits workspace-path branch)
        current-branches (->> (File. (str workspace-path "/branches"))
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
;; Lock helpers
;; ============================================================

(defn- get-lock! [branch-locks branch-name]
  (locking branch-locks
    (if-let [l (get @branch-locks branch-name)]
      l
      (let [l (ReentrantLock.)]
        (swap! branch-locks assoc branch-name l)
        l))))

;; ============================================================
;; System record
;; ============================================================

(defrecord OverlayFSSystem [base-path workspace-path current-branch-atom
                            system-name watcher-state branch-locks]
  p/SystemIdentity
  (system-id [_] (or system-name (str "overlayfs:" workspace-path)))
  (system-type [_] :overlayfs)
  (capabilities [_]
    (t/->Capabilities true true true true false true))

  p/Snapshotable
  (snapshot-id [_]
    (let [commit (latest-commit workspace-path @current-branch-atom)]
      (:id commit)))

  (parent-ids [_]
    (let [commit (latest-commit workspace-path @current-branch-atom)]
      (or (:parent-ids commit) #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Return the snapshot directory path for read-only access
    (let [branch @current-branch-atom
          commits (read-commits workspace-path branch)
          commit (first (filter #(= (:id %) (str snap-id)) commits))]
      (when commit
        (str (snapshots-dir workspace-path branch) "/" (:id commit)))))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [branch @current-branch-atom
          commits (read-commits workspace-path branch)
          commit (first (filter #(= (:id %) (str snap-id)) commits))]
      (when commit
        {:snapshot-id (:id commit)
         :parent-ids (or (:parent-ids commit) #{})
         :timestamp (:timestamp commit)
         :message (:message commit)})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [dir (File. (str workspace-path "/branches"))]
      (if (.exists dir)
        (->> (.listFiles dir)
             (filter #(.isDirectory %))
             (map #(keyword (.getName %)))
             set)
        #{:main})))

  (current-branch [_]
    (keyword @current-branch-atom))

  (branch! [this name]
    ;; Branch from current branch's latest state (copy upper dir)
    (let [branch-str (clojure.core/name name)
          source-branch @current-branch-atom
          new-upper (upper-dir workspace-path branch-str)
          new-work (work-dir workspace-path branch-str)
          source-upper (upper-dir workspace-path source-branch)
          source-snaps (snapshots-dir workspace-path source-branch)
          new-snaps (snapshots-dir workspace-path branch-str)]
      (ensure-dirs! new-upper new-work new-snaps)
      ;; Copy current state from source branch
      (when (.exists (File. source-upper))
        (copy-recursive! source-upper new-upper))
      ;; Copy commit history and snapshots (branch inherits parent's history)
      (let [source-commits (read-commits workspace-path source-branch)]
        (write-commits! workspace-path branch-str source-commits)
        ;; Copy snapshot data so ancestor lookups work on new branch
        (doseq [c source-commits]
          (let [src-snap (str source-snaps "/" (:id c))
                dst-snap (str new-snaps "/" (:id c))]
            (when (and (.exists (File. src-snap))
                       (not (.exists (File. dst-snap))))
              (copy-recursive! src-snap dst-snap)))))
      this))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    ;; Branch from a specific snapshot
    (let [branch-str (clojure.core/name name)
          source-branch @current-branch-atom
          source-snaps (snapshots-dir workspace-path source-branch)
          snap-path (str source-snaps "/" from)
          new-upper (upper-dir workspace-path branch-str)
          new-snaps (snapshots-dir workspace-path branch-str)]
      (ensure-dirs! new-upper
                    (work-dir workspace-path branch-str)
                    new-snaps)
      (when (.exists (File. (str snap-path)))
        (copy-recursive! snap-path new-upper))
      ;; Copy commits and snapshots up to the snapshot point
      (let [source-commits (read-commits workspace-path source-branch)
            idx (inc (.indexOf (mapv :id source-commits) (str from)))
            inherited (if (pos? idx) (vec (take idx source-commits)) [])]
        (write-commits! workspace-path branch-str inherited)
        (doseq [c inherited]
          (let [src-snap (str source-snaps "/" (:id c))
                dst-snap (str new-snaps "/" (:id c))]
            (when (and (.exists (File. src-snap))
                       (not (.exists (File. dst-snap))))
              (copy-recursive! src-snap dst-snap)))))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (delete-recursive! (branch-dir workspace-path branch-str))
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (when-not (.exists (File. (branch-dir workspace-path branch-str)))
        (throw (ex-info (str "Branch not found: " branch-str)
                        {:branch branch-str})))
      (reset! current-branch-atom branch-str)
      this))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [commits (read-commits workspace-path @current-branch-atom)
          commits (vec (rseq (vec commits)))
          commits (if-let [limit (:limit opts)]
                    (vec (take limit commits))
                    commits)]
      (mapv :id commits)))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [commits (read-commits workspace-path @current-branch-atom)
          idx (.indexOf (mapv :id commits) (str snap-id))]
      (if (pos? idx)
        (mapv :id (take idx commits))
        [])))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [commits (read-commits workspace-path @current-branch-atom)
          ids (mapv :id commits)
          idx-a (.indexOf ids (str a))
          idx-b (.indexOf ids (str b))]
      (and (>= idx-a 0) (>= idx-b 0) (< idx-a idx-b))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    ;; Search across all branches for common ancestor
    (let [all-branches (->> (File. (str workspace-path "/branches"))
                            (.listFiles)
                            (filter #(.isDirectory %))
                            (map #(.getName %)))
          ;; Build id->set-of-commit-ids-in-history for each snapshot
          find-history (fn [snap-id]
                         (let [branch (first (filter
                                              #(some (fn [c] (= (:id c) (str snap-id)))
                                                     (read-commits workspace-path %))
                                              all-branches))]
                           (when branch
                             (let [commits (read-commits workspace-path branch)
                                   idx (.indexOf (mapv :id commits) (str snap-id))]
                               (when (>= idx 0)
                                 (set (map :id (take (inc idx) commits))))))))
          history-a (find-history a)
          history-b (find-history b)]
      (when (and history-a history-b)
        ;; Common ancestor = latest commit in both histories
        (let [common (set/intersection history-a history-b)]
          (when (seq common)
            ;; Find the latest one (highest index in either branch)
            (let [any-branch (first (filter
                                     #(some (fn [c] (common (:id c)))
                                            (read-commits workspace-path %))
                                     all-branches))
                  commits (read-commits workspace-path any-branch)
                  ordered (filter #(common (:id %)) commits)]
              (:id (last ordered))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [commits (read-commits workspace-path @current-branch-atom)
          nodes (into {}
                      (map (fn [c]
                             [(:id c) {:parent-ids (or (:parent-ids c) #{})
                                       :meta (dissoc c :id :parent-ids)}])
                           commits))]
      {:nodes nodes
       :branches {(keyword @current-branch-atom)
                  (:id (last commits))}
       :roots (if (seq commits) #{(:id (first commits))} #{})}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (when-let [meta (p/snapshot-meta this snap-id)]
      (dissoc meta :snapshot-id)))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    ;; Add-only merge: copy all files from source's upper to current upper
    (let [source-branch (if (keyword? source) (clojure.core/name source) (str source))
          dest-branch @current-branch-atom
          source-upper (upper-dir workspace-path source-branch)
          dest-upper (upper-dir workspace-path dest-branch)]
      (when (.exists (File. source-upper))
        (copy-recursive! source-upper dest-upper))
      ;; Auto-commit the merge
      (let [source-commits (read-commits workspace-path source-branch)
            dest-commits (read-commits workspace-path dest-branch)
            merge-id (str (UUID/randomUUID))
            parent-ids (set (remove nil?
                                    [(:id (last dest-commits))
                                     (:id (last source-commits))]))
            merge-commit {:id merge-id
                          :parent-ids parent-ids
                          :message (or (:message opts) (str "Merge " source-branch))
                          :timestamp (str (Instant/now))
                          :branch dest-branch}]
        ;; Snapshot the merged state
        (let [snap-dir (str (snapshots-dir workspace-path dest-branch) "/" merge-id)]
          (ensure-dirs! snap-dir)
          (copy-recursive! dest-upper snap-dir))
        (write-commits! workspace-path dest-branch
                        (conj dest-commits merge-commit))
        this)))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [this a b _opts]
    ;; Check if both branches modified the same files AFTER their fork point
    (let [all-branches (->> (File. (str workspace-path "/branches"))
                            (.listFiles)
                            (filter #(.isDirectory %))
                            (map #(.getName %)))
          find-branch (fn [snap-id]
                        (first (filter
                                #(some (fn [c] (= (:id c) (str snap-id)))
                                       (read-commits workspace-path %))
                                all-branches)))
          branch-a (find-branch a)
          branch-b (find-branch b)]
      (if (and branch-a branch-b)
        (let [;; Find common ancestor snapshot's files
              ancestor-id (p/common-ancestor this a b)
              ancestor-files (if ancestor-id
                               (let [ancestor-branch (find-branch ancestor-id)
                                     snap-path (str (snapshots-dir workspace-path ancestor-branch)
                                                    "/" ancestor-id)]
                                 (list-upper-files snap-path))
                               #{})
              ;; Files in each branch that weren't in the ancestor
              files-a (set/difference
                       (list-upper-files (upper-dir workspace-path branch-a))
                       ancestor-files)
              files-b (set/difference
                       (list-upper-files (upper-dir workspace-path branch-b))
                       ancestor-files)
              conflicts (set/intersection files-a files-b)]
          (mapv (fn [path] {:path (str/split path #"/")
                            :ours path
                            :theirs path})
                conflicts))
        [])))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (let [all-branches (->> (File. (str workspace-path "/branches"))
                            (.listFiles)
                            (filter #(.isDirectory %))
                            (map #(.getName %)))
          find-branch (fn [snap-id]
                        (first (filter
                                #(some (fn [c] (= (:id c) (str snap-id)))
                                       (read-commits workspace-path %))
                                all-branches)))
          branch-a (find-branch a)
          branch-b (find-branch b)]
      {:snapshot-a (str a)
       :snapshot-b (str b)
       :files-a (when branch-a (list-upper-files (upper-dir workspace-path branch-a)))
       :files-b (when branch-b (list-upper-files (upper-dir workspace-path branch-b)))}))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn workspace-path current-branch-atom)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id)))

;; ============================================================
;; Factory functions
;; ============================================================

(defn create
  "Create an OverlayFS adapter for an existing workspace.
   base-path: Read-only base directory (lower layer)
   workspace-path: Directory for branch state (upper dirs, commits)"
  ([base-path workspace-path] (create base-path workspace-path {}))
  ([base-path workspace-path opts]
   (->OverlayFSSystem base-path workspace-path
                      (atom (or (:initial-branch opts) "main"))
                      (:system-name opts)
                      (w/create-watcher-state)
                      (atom {}))))

(defn init!
  "Initialize a new overlay workspace.
   Creates base-path with an entries/ directory and workspace with main branch."
  ([base-path workspace-path] (init! base-path workspace-path {}))
  ([base-path workspace-path opts]
   (ensure-dirs! base-path
                 (str base-path "/entries")
                 (str workspace-path "/branches/main/upper")
                 (str workspace-path "/branches/main/work")
                 (str workspace-path "/branches/main/snapshots"))
   (write-commits! workspace-path "main" [])
   (create base-path workspace-path opts)))

(defn destroy!
  "Clean up the workspace (all branch state). Does not delete base-path."
  [^OverlayFSSystem sys]
  (w/stop-polling! (:watcher-state sys))
  (delete-recursive! (:workspace-path sys)))

(defn commit!
  "Create a commit on the current branch.
   Snapshots the upper directory state and records metadata."
  ([sys] (commit! sys nil))
  ([sys message]
   (let [{:keys [workspace-path current-branch-atom]} sys
         branch @current-branch-atom
         upper (upper-dir workspace-path branch)
         commit-id (str (UUID/randomUUID))
         commits (read-commits workspace-path branch)
         parent-ids (if-let [prev (last commits)]
                      #{(:id prev)}
                      #{})
         commit {:id commit-id
                 :parent-ids parent-ids
                 :message (or message "")
                 :timestamp (str (Instant/now))
                 :branch branch}
         snap-dir (str (snapshots-dir workspace-path branch) "/" commit-id)]
     ;; Archive upper dir state
     (ensure-dirs! snap-dir)
     (when (.exists (File. upper))
       (copy-recursive! upper snap-dir))
     ;; Record commit
     (write-commits! workspace-path branch (conj commits commit))
     commit-id)))

(defn merged-path
  "Get the path where the overlay would be mounted for a branch.
   This is the base-path since bubblewrap mounts the overlay there.
   For reading outside of a sandbox, use read-file or the snapshot paths."
  [sys]
  (:base-path sys))

(defn read-file
  "Read a file from the current branch's effective state.
   Checks upper first (branch-specific changes), then base (shared)."
  [sys relative-path]
  (let [{:keys [base-path workspace-path current-branch-atom]} sys
        branch @current-branch-atom
        upper-file (File. (str (upper-dir workspace-path branch) "/" relative-path))
        base-file (File. (str base-path "/" relative-path))]
    (cond
      (.exists upper-file) (slurp upper-file)
      (.exists base-file) (slurp base-file)
      :else nil)))

(defn write-file!
  "Write a file to the current branch's upper directory."
  [sys relative-path content]
  (let [{:keys [workspace-path current-branch-atom]} sys
        branch @current-branch-atom
        f (File. (str (upper-dir workspace-path branch) "/" relative-path))]
    (.mkdirs (.getParentFile f))
    (spit f content)))

(defn delete-file!
  "Delete a file from the current branch.
   If the file exists in base, creates a whiteout marker in upper."
  [sys relative-path]
  (let [{:keys [base-path workspace-path current-branch-atom]} sys
        branch @current-branch-atom
        upper-file (File. (str (upper-dir workspace-path branch) "/" relative-path))
        base-file (File. (str base-path "/" relative-path))]
    (when (.exists upper-file)
      (.delete upper-file))
    ;; If file exists in base, mark as deleted with empty file
    ;; (In real overlayfs, this would be a whiteout device)
    (when (.exists base-file)
      (.mkdirs (.getParentFile upper-file))
      (spit upper-file ""))))
