(ns yggdrasil.adapters.zfs
  "ZFS adapter for Yggdrasil protocols.

  Maps ZFS concepts to yggdrasil:
    - Dataset = branch
    - Snapshot = commit
    - Clone = branch creation
    - No merge support (ZFS doesn't have three-way merge)

  Stateless wrapper around zfs CLI — always queries actual ZFS state.
  Supports polling watcher for detecting external snapshot/clone operations."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.watcher :as w]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.set :as set])
  (:import [java.util.concurrent.locks ReentrantLock]))

(def ^:dynamic *use-sudo*
  "When true, prefix zfs commands with sudo. Default true on Linux
   (unprivileged mounts not supported). Set to false if using ZFS delegation
   on FreeBSD/Solaris or if passwordless sudo is not available."
  true)

(defn- zfs
  "Run a zfs command. Returns trimmed stdout on success.
   Uses sudo if *use-sudo* is true, otherwise relies on ZFS delegation."
  [& args]
  (let [cmd (if *use-sudo*
              (into ["sudo" "zfs"] args)
              (into ["zfs"] args))
        result (apply sh cmd)]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (throw (ex-info (str "zfs error: " (str/trim (:err result)))
                      {:args (vec args) :exit (:exit result)})))))

(defn- zfs-lines
  "Run zfs command, return output as vector of non-empty lines."
  [& args]
  (let [out (apply zfs args)]
    (if (str/blank? out) [] (str/split-lines out))))

(defn- dataset-snapshots
  "List snapshots for a dataset, oldest first."
  [dataset]
  (zfs-lines "list" "-t" "snapshot" "-H" "-o" "name" "-s" "creation" "-r" dataset))

(defn- snapshot-short-name
  "Extract snapshot name after @ from full name."
  [full-name]
  (second (str/split full-name #"@" 2)))

(defn- dataset-mount-point
  "Get mount point for a dataset."
  [dataset]
  (zfs "get" "-H" "-o" "value" "mountpoint" dataset))

(defn- entry-dir
  "Path to entries directory on the mounted dataset."
  [dataset]
  (str (dataset-mount-point dataset) "/entries"))

(defn- dataset-exists?
  "Check if a ZFS dataset exists."
  [dataset]
  (let [cmd (if *use-sudo*
              ["sudo" "zfs" "list" "-H" dataset]
              ["zfs" "list" "-H" dataset])]
    (zero? (:exit (apply sh cmd)))))

(defn- list-child-datasets
  "List direct child datasets (branches) of the base dataset's parent."
  [base-pool prefix]
  (let [parent (str base-pool "/" prefix)]
    (->> (zfs-lines "list" "-H" "-o" "name" "-r" parent)
         (filter #(and (not= % parent)
                       (= 1 (count (str/split (subs % (inc (count parent))) #"/")))))
         (map #(last (str/split % #"/"))))))

(defn- poll-fn
  "Poll function for ZFS watcher. Detects new snapshots and datasets."
  [base-pool prefix current-branch-atom last-state]
  (let [current-ds (str base-pool "/" prefix "/" @current-branch-atom)
        current-snaps (set (dataset-snapshots current-ds))
        current-branches (set (list-child-datasets base-pool prefix))
        prev-snaps (or (:snapshots last-state) #{})
        prev-branches (or (:branches last-state) #{})
        new-snaps (set/difference current-snaps prev-snaps)
        events (cond-> []
                 (seq new-snaps)
                 (into (for [s new-snaps]
                         {:type :commit
                          :snapshot-id s
                          :branch @current-branch-atom
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
    {:state {:snapshots current-snaps
             :branches current-branches}
     :events events}))

(defn- chmod-777!
  "Make a directory world-writable. Uses sudo chmod for ZFS mount points."
  [path]
  (sh "sudo" "chmod" "777" path))

(defn- get-lock!
  "Get or create a lock for a branch."
  [branch-locks branch-name]
  (locking branch-locks
    (if-let [l (get @branch-locks branch-name)]
      l
      (let [l (ReentrantLock.)]
        (swap! branch-locks assoc branch-name l)
        l))))

(defn- with-branch-lock*
  "Execute f while holding the lock for branch-name."
  [branch-locks branch-name f]
  (let [^ReentrantLock l (get-lock! branch-locks branch-name)]
    (.lock l)
    (try (f)
         (finally (.unlock l)))))

(defrecord ZFSSystem [base-pool prefix mount-base current-branch-atom system-name watcher-state branch-locks]
  p/SystemIdentity
  (system-id [_] (or system-name (str "zfs:" base-pool "/" prefix)))
  (system-type [_] :zfs)
  (capabilities [_]
    (t/->Capabilities true true true false false true))

  p/Snapshotable
  (snapshot-id [_]
    (let [ds (str base-pool "/" prefix "/" @current-branch-atom)
          snaps (dataset-snapshots ds)]
      (last snaps)))

  (parent-ids [_]
    (let [ds (str base-pool "/" prefix "/" @current-branch-atom)
          snaps (dataset-snapshots ds)]
      (if (< (count snaps) 2)
        #{}
        #{(nth snaps (- (count snaps) 2))})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Return the .zfs/snapshot path for read-only access
    (let [snap-name (snapshot-short-name (str snap-id))
          ds (str base-pool "/" prefix "/" @current-branch-atom)
          mount (dataset-mount-point ds)]
      (str mount "/.zfs/snapshot/" snap-name)))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (try
      (let [creation (zfs "get" "-H" "-o" "value" "creation" (str snap-id))
            ;; Try to get user properties
            msg (try (zfs "get" "-H" "-o" "value" "yggdrasil:message" (str snap-id))
                     (catch Exception _ "-"))
            uuid (try (zfs "get" "-H" "-o" "value" "yggdrasil:uuid" (str snap-id))
                      (catch Exception _ "-"))
            parent (try (zfs "get" "-H" "-o" "value" "yggdrasil:parent" (str snap-id))
                        (catch Exception _ "-"))]
        {:snapshot-id (str snap-id)
         :parent-ids (if (or (= parent "-") (str/blank? parent))
                       #{}
                       #{parent})
         :timestamp creation
         :message (when-not (= msg "-") msg)})
      (catch Exception _ nil)))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (set (map keyword (list-child-datasets base-pool prefix))))

  (current-branch [_]
    (keyword @current-branch-atom))

  (branch! [this name]
    ;; Clone from latest snapshot of current branch
    (let [branch-str (clojure.core/name name)
          source-ds (str base-pool "/" prefix "/" @current-branch-atom)
          snaps (dataset-snapshots source-ds)
          latest-snap (last snaps)
          new-ds (str base-pool "/" prefix "/" branch-str)
          new-mount (str mount-base "/" prefix "/" branch-str)]
      (when-not latest-snap
        (throw (ex-info "Cannot branch: no snapshots on current dataset"
                        {:dataset source-ds})))
      (zfs "clone" "-o" (str "mountpoint=" new-mount) latest-snap new-ds)
      (chmod-777! new-mount)
      ;; Create entries dir on new clone
      (let [edir (java.io.File. (str new-mount "/entries"))]
        (when-not (.exists edir)
          (.mkdirs edir)))
      this))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    ;; Clone from a specific snapshot
    (let [branch-str (clojure.core/name name)
          new-ds (str base-pool "/" prefix "/" branch-str)
          new-mount (str mount-base "/" prefix "/" branch-str)]
      (zfs "clone" "-o" (str "mountpoint=" new-mount) (str from) new-ds)
      (chmod-777! new-mount)
      (.mkdirs (java.io.File. (str new-mount "/entries")))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)
          ds (str base-pool "/" prefix "/" branch-str)]
      ;; Destroy dataset and all its snapshots
      (zfs "destroy" "-r" ds)
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)
          ds (str base-pool "/" prefix "/" branch-str)]
      (when-not (dataset-exists? ds)
        (throw (ex-info (str "Branch not found: " branch-str)
                        {:branch branch-str})))
      (reset! current-branch-atom branch-str)
      this))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [ds (str base-pool "/" prefix "/" @current-branch-atom)
          snaps (vec (rseq (vec (dataset-snapshots ds))))
          snaps (if-let [limit (:limit opts)]
                  (vec (take limit snaps))
                  snaps)]
      snaps))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [ds (str base-pool "/" prefix "/" @current-branch-atom)
          snaps (dataset-snapshots ds)
          idx (.indexOf snaps (str snap-id))]
      (if (pos? idx)
        (vec (take idx snaps))
        [])))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [ds (str base-pool "/" prefix "/" @current-branch-atom)
          snaps (dataset-snapshots ds)
          idx-a (.indexOf snaps (str a))
          idx-b (.indexOf snaps (str b))]
      (and (>= idx-a 0) (>= idx-b 0) (< idx-a idx-b))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    ;; Find common ancestor across branches using ZFS origin tracking
    (let [snap-a (str a)
          snap-b (str b)
          ds-a (first (str/split snap-a #"@" 2))
          ds-b (first (str/split snap-b #"@" 2))]
      (if (= ds-a ds-b)
        ;; Same dataset: earlier snapshot is the ancestor
        (let [snaps (dataset-snapshots ds-a)
              idx-a (.indexOf snaps snap-a)
              idx-b (.indexOf snaps snap-b)]
          (when (and (>= idx-a 0) (>= idx-b 0))
            (nth snaps (min idx-a idx-b))))
        ;; Different datasets: find clone origin
        (let [origin-b (try (let [o (zfs "get" "-H" "-o" "value" "origin" ds-b)]
                              (when-not (= o "-") o))
                            (catch Exception _ nil))
              origin-a (try (let [o (zfs "get" "-H" "-o" "value" "origin" ds-a)]
                              (when-not (= o "-") o))
                            (catch Exception _ nil))]
          (cond
            ;; b's dataset was cloned from a snapshot on a's dataset
            (and origin-b (= ds-a (first (str/split origin-b #"@" 2))))
            origin-b
            ;; a's dataset was cloned from a snapshot on b's dataset
            (and origin-a (= ds-b (first (str/split origin-a #"@" 2))))
            origin-a
            :else nil)))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [ds (str base-pool "/" prefix "/" @current-branch-atom)
          snaps (dataset-snapshots ds)
          nodes (into {}
                      (map-indexed
                       (fn [i s]
                         [s {:parent-ids (if (zero? i) #{} #{(nth snaps (dec i))})
                             :meta (p/snapshot-meta this s)}])
                       snaps))]
      {:nodes nodes
       :branches {(keyword @current-branch-atom) (last snaps)}
       :roots (if (seq snaps) #{(first snaps)} #{})}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (when-let [meta (p/snapshot-meta this snap-id)]
      (dissoc meta :snapshot-id)))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (java.util.UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn base-pool prefix current-branch-atom)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id)))

(def ^:dynamic *mount-base*
  "Base directory for ZFS mount points. Datasets are mounted under
   <mount-base>/<prefix>/... so the user controls file permissions.
   Defaults to /tmp/yggdrasil."
  "/tmp/yggdrasil")

(defn create
  "Create a ZFS adapter for an existing dataset hierarchy.
   base-pool: ZFS pool name (e.g., \"tank\")
   prefix: namespace prefix within the pool (e.g., \"yggdrasil/test-123\")
   The current branch defaults to \"main\".

   Expects datasets like: tank/yggdrasil/test-123/main"
  ([base-pool prefix] (create base-pool prefix {}))
  ([base-pool prefix opts]
   (let [mb (or (:mount-base opts) *mount-base*)]
     (->ZFSSystem base-pool prefix mb
                  (atom (or (:initial-branch opts) "main"))
                  (:system-name opts)
                  (w/create-watcher-state)
                  (atom {})))))

(defn init!
  "Initialize a new ZFS workspace: creates the prefix dataset and a 'main' branch dataset.
   Mounts datasets under *mount-base*/<prefix>/... for user-writable access.
   Requires passwordless sudo for: /usr/sbin/zfs, /bin/chmod 777 <mount-base>/*
   Returns a ZFSSystem."
  ([base-pool prefix] (init! base-pool prefix {}))
  ([base-pool prefix opts]
   (let [parent-ds (str base-pool "/" prefix)
         main-ds (str parent-ds "/main")
         mount-base (or (:mount-base opts) *mount-base*)
         parent-mount (str mount-base "/" prefix)
         main-mount (str parent-mount "/main")]
     ;; Create parent namespace dataset with explicit mountpoint
     (when-not (dataset-exists? parent-ds)
       (zfs "create" "-o" (str "mountpoint=" parent-mount) "-p" parent-ds)
       (chmod-777! parent-mount))
     ;; Create main branch dataset with explicit mountpoint
     (when-not (dataset-exists? main-ds)
       (zfs "create" "-o" (str "mountpoint=" main-mount) main-ds)
       (chmod-777! main-mount))
     ;; Create entries directory (mount is now writable)
     (.mkdirs (java.io.File. (str main-mount "/entries")))
     (create base-pool prefix opts))))

(defn destroy!
  "Destroy the entire workspace (all datasets and snapshots under prefix).
   Also cleans up mount point directories."
  [^ZFSSystem sys]
  (w/stop-polling! (:watcher-state sys))
  (let [parent-ds (str (:base-pool sys) "/" (:prefix sys))
        parent-mount (str (:mount-base sys) "/" (:prefix sys))]
    (try (zfs "destroy" "-r" parent-ds)
         (catch Exception _))
    ;; Clean up empty mount point directories
    (try
      (let [parent-dir (java.io.File. parent-mount)]
        (when (.exists parent-dir)
          ;; Remove child dirs first, then parent
          (doseq [f (reverse (sort (file-seq parent-dir)))]
            (try (.delete f) (catch Exception _)))))
      (catch Exception _))))
