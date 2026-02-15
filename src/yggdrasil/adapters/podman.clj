(ns yggdrasil.adapters.podman
  "Podman adapter for Yggdrasil protocols.

  Uses Podman containers for COW filesystem isolation with process namespacing.
  Each branch is a running container with its own overlay filesystem layer.

  Architecture:
    - Base image: The starting point (built or pulled)
    - Branch = running container from a committed image
    - Snapshot = podman commit (creates new image layer)
    - Fork = podman run from branch's latest image
    - Merge = podman diff + file copy between containers

  Commit metadata is tracked in a workspace directory on the host.
  Container naming: ygg-<system-id>-<branch>
  Image naming: ygg-<system-id>/<branch>:<commit-uuid>

  Rootless operation via podman (no daemon, no root required)."
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
;; Podman CLI helpers
;; ============================================================

(defn- podman
  "Run a podman command. Returns trimmed stdout on success."
  [& args]
  (let [result (apply sh "podman" args)]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (throw (ex-info (str "podman error: " (str/trim (:err result)))
                      {:args (vec args) :exit (:exit result)})))))

(defn- podman-ok?
  "Run podman command, return true if exit 0."
  [& args]
  (zero? (:exit (apply sh "podman" args))))

(defn- podman-lines
  "Run podman command, return output as lines."
  [& args]
  (let [out (apply podman args)]
    (if (str/blank? out) [] (str/split-lines out))))

;; ============================================================
;; Naming conventions
;; ============================================================

(defn- container-name [system-id branch-name]
  (str "ygg-" system-id "-" branch-name))

(defn- image-name
  "Image name for a branch. Tag is :latest or a commit UUID."
  ([system-id branch-name] (image-name system-id branch-name "latest"))
  ([system-id branch-name tag]
   (str "ygg-" system-id "/" branch-name ":" tag)))

;; ============================================================
;; Metadata management (host-side)
;; ============================================================

(defn- ensure-dirs! [& paths]
  (doseq [p paths]
    (.mkdirs (File. (str p)))))

(defn- commits-file [workspace-path branch-name]
  (str workspace-path "/metadata/" branch-name "/commits.edn"))

(defn- read-commits [workspace-path branch-name]
  (let [f (File. (commits-file workspace-path branch-name))]
    (if (.exists f)
      (edn/read-string (slurp f))
      [])))

(defn- write-commits! [workspace-path branch-name commits]
  (let [f (File. (commits-file workspace-path branch-name))]
    (.mkdirs (.getParentFile f))
    (spit f (pr-str commits))))

(defn- latest-commit [workspace-path branch-name]
  (last (read-commits workspace-path branch-name)))

;; ============================================================
;; Container lifecycle
;; ============================================================

(defn- container-running? [cname]
  (podman-ok? "container" "inspect" "--format" "{{.State.Running}}" cname))

(defn- container-exists? [cname]
  (podman-ok? "container" "exists" cname))

(defn- image-exists? [iname]
  (podman-ok? "image" "exists" iname))

(defn- start-container!
  "Start a container for a branch from its latest image.
   The container runs sleep infinity and is accessed via exec."
  [system-id branch-name image]
  (let [cname (container-name system-id branch-name)]
    ;; Remove existing container if present
    (when (container-exists? cname)
      (try (podman "rm" "-f" cname) (catch Exception _)))
    ;; Create and start container
    (podman "run" "-d"
            "--name" cname
            "--label" (str "ygg.system=" system-id)
            "--label" (str "ygg.branch=" branch-name)
            image
            "sleep" "infinity")
    cname))

(defn- stop-container! [system-id branch-name]
  (let [cname (container-name system-id branch-name)]
    (when (container-exists? cname)
      (try (podman "stop" "-t" "1" cname) (catch Exception _))
      (try (podman "rm" "-f" cname) (catch Exception _)))))

;; ============================================================
;; Exec inside container
;; ============================================================

(defn exec!
  "Execute a command inside the branch's container.
   Returns {:exit int, :out string, :err string}."
  [sys cmd & {:keys [env dir]}]
  (let [{:keys [system-id current-branch]} sys
        branch current-branch
        cname (container-name system-id branch)
        exec-args (cond-> ["exec"]
                    dir (into ["-w" dir])
                    env (into (mapcat (fn [[k v]] ["-e" (str k "=" v)]) env))
                    true (conj cname)
                    true (into (if (string? cmd) ["sh" "-c" cmd] cmd)))
        result (apply sh "podman" exec-args)]
    {:exit (:exit result)
     :out (str/trim (:out result))
     :err (str/trim (:err result))}))

;; ============================================================
;; Watcher poll function
;; ============================================================

(defn- poll-fn
  [system-id workspace-path current-branch last-state]
  (let [branch current-branch
        current-commits (read-commits workspace-path branch)
        ;; List branches from metadata dirs
        meta-dir (File. (str workspace-path "/metadata"))
        current-branches (if (.exists meta-dir)
                           (->> (.listFiles meta-dir)
                                (filter #(.isDirectory %))
                                (map #(.getName %))
                                set)
                           #{"main"})
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

(defrecord PodmanSystem [system-id base-image workspace-path
                         current-branch watcher-state branch-locks]
  p/SystemIdentity
  (system-id [_] system-id)
  (system-type [_] :podman)
  (capabilities [_]
    (t/->Capabilities true true true true false true true))

  p/Snapshotable
  (snapshot-id [_]
    (let [commit (latest-commit workspace-path current-branch)]
      (:id commit)))

  (parent-ids [_]
    (let [commit (latest-commit workspace-path current-branch)]
      (or (:parent-ids commit) #{})))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; Return info for accessing historical state (create temp container from image)
    (let [branch current-branch
          img (image-name system-id branch (str snap-id))]
      (when (image-exists? img)
        {:image img :commit-id (str snap-id)})))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [branch current-branch
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
    (let [meta-dir (File. (str workspace-path "/metadata"))]
      (if (.exists meta-dir)
        (->> (.listFiles meta-dir)
             (filter #(.isDirectory %))
             (map #(keyword (.getName %)))
             set)
        #{:main})))

  (current-branch [_]
    (keyword current-branch))

  (branch! [this name]
    ;; Fork from current branch's latest committed image
    (let [branch-str (clojure.core/name name)
          source-branch current-branch
          source-commit (latest-commit workspace-path source-branch)
          source-img (if source-commit
                       (image-name system-id source-branch (:id source-commit))
                       (image-name system-id source-branch))
          new-img (image-name system-id branch-str)]
      ;; Tag source image as the new branch's base
      (podman "tag" source-img new-img)
      ;; Start container for new branch
      (start-container! system-id branch-str new-img)
      ;; Copy commit history
      (let [source-commits (read-commits workspace-path source-branch)]
        (write-commits! workspace-path branch-str source-commits))
      this))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    ;; Fork from a specific snapshot
    (let [branch-str (clojure.core/name name)
          source-branch current-branch
          source-img (image-name system-id source-branch (str from))
          new-img (image-name system-id branch-str)]
      (podman "tag" source-img new-img)
      (start-container! system-id branch-str new-img)
      ;; Copy commits up to that snapshot
      (let [source-commits (read-commits workspace-path source-branch)
            idx (inc (.indexOf (mapv :id source-commits) (str from)))
            inherited (if (pos? idx) (vec (take idx source-commits)) [])]
        (write-commits! workspace-path branch-str inherited))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)]
      ;; Stop and remove container
      (stop-container! system-id branch-str)
      ;; Remove branch images
      (let [commits (read-commits workspace-path branch-str)]
        (doseq [c commits]
          (let [img (image-name system-id branch-str (:id c))]
            (when (image-exists? img)
              (try (podman "rmi" img) (catch Exception _)))))
        (let [latest-img (image-name system-id branch-str)]
          (when (image-exists? latest-img)
            (try (podman "rmi" latest-img) (catch Exception _)))))
      ;; Remove metadata
      (let [meta-dir (File. (str workspace-path "/metadata/" branch-str))]
        (when (.exists meta-dir)
          (doseq [f (reverse (file-seq meta-dir))]
            (.delete f))))
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (let [branch-str (clojure.core/name name)]
      (when-not (.exists (File. (str workspace-path "/metadata/" branch-str)))
        (throw (ex-info (str "Branch not found: " branch-str)
                        {:branch branch-str})))
      ;; Ensure container is running
      (let [cname (container-name system-id branch-str)]
        (when-not (container-exists? cname)
          (let [commit (latest-commit workspace-path branch-str)
                img (if commit
                      (image-name system-id branch-str (:id commit))
                      (image-name system-id branch-str))]
            (start-container! system-id branch-str img))))
      ;; Return NEW system with updated branch (value semantics)
      (->PodmanSystem system-id base-image workspace-path
                      branch-str watcher-state branch-locks)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [commits (read-commits workspace-path current-branch)
          commits (vec (rseq (vec commits)))
          commits (if-let [limit (:limit opts)]
                    (vec (take limit commits))
                    commits)]
      (mapv :id commits)))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [commits (read-commits workspace-path current-branch)
          idx (.indexOf (mapv :id commits) (str snap-id))]
      (if (pos? idx)
        (mapv :id (take idx commits))
        [])))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (let [commits (read-commits workspace-path current-branch)
          ids (mapv :id commits)
          idx-a (.indexOf ids (str a))
          idx-b (.indexOf ids (str b))]
      (and (>= idx-a 0) (>= idx-b 0) (< idx-a idx-b))))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [meta-dir (File. (str workspace-path "/metadata"))
          all-branches (when (.exists meta-dir)
                         (->> (.listFiles meta-dir)
                              (filter #(.isDirectory %))
                              (map #(.getName %))))
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
        (let [common (set/intersection history-a history-b)]
          (when (seq common)
            (let [any-branch (first (filter
                                     #(some (fn [c] (common (:id c)))
                                            (read-commits workspace-path %))
                                     all-branches))
                  commits (read-commits workspace-path any-branch)
                  ordered (filter #(common (:id %)) commits)]
              (:id (last ordered))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (let [commits (read-commits workspace-path current-branch)
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
    ;; Copy changed files from source container to dest container
    (let [source-branch (if (keyword? source) (clojure.core/name source) (str source))
          dest-branch current-branch
          source-cname (container-name system-id source-branch)
          dest-cname (container-name system-id dest-branch)
          ;; Get list of changed files in source container
          diff-lines (podman-lines "diff" source-cname)
          ;; Filter to only added/changed files (A or C prefix)
          changed-files (->> diff-lines
                             (filter #(or (str/starts-with? % "A ")
                                          (str/starts-with? % "C ")))
                             (map #(subs % 2))
                             ;; Skip system dirs
                             (remove #(or (str/starts-with? % "/proc")
                                          (str/starts-with? % "/sys")
                                          (str/starts-with? % "/dev")
                                          (str/starts-with? % "/run")
                                          (str/starts-with? % "/tmp"))))]
      ;; Copy each changed file from source to dest
      (doseq [f changed-files]
        (try
          (let [tmp (str "/tmp/ygg-merge-" (System/nanoTime))]
            (podman "cp" (str source-cname ":" f) tmp)
            (podman "cp" tmp (str dest-cname ":" f))
            (.delete (File. tmp)))
          (catch Exception _)))
      ;; Create merge commit
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
        ;; Commit the container state
        (podman "commit" dest-cname
                (image-name system-id dest-branch merge-id))
        (podman "tag"
                (image-name system-id dest-branch merge-id)
                (image-name system-id dest-branch))
        (write-commits! workspace-path dest-branch
                        (conj dest-commits merge-commit))
        this)))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [this a b _opts]
    (let [meta-dir (File. (str workspace-path "/metadata"))
          all-branches (when (.exists meta-dir)
                         (->> (.listFiles meta-dir)
                              (filter #(.isDirectory %))
                              (map #(.getName %))))
          find-branch (fn [snap-id]
                        (first (filter
                                #(some (fn [c] (= (:id c) (str snap-id)))
                                       (read-commits workspace-path %))
                                all-branches)))
          branch-a (find-branch a)
          branch-b (find-branch b)
          ;; Filter to only regular files (not directories)
          file-diffs (fn [cname]
                       (->> (podman-lines "diff" cname)
                            (filter #(or (str/starts-with? % "A ")
                                         (str/starts-with? % "C ")))
                            (map #(subs % 2))
                            ;; Check each path is a file (not directory)
                            (filter (fn [path]
                                      (zero? (:exit (sh "podman" "exec" cname
                                                        "test" "-f" path)))))
                            set))]
      (if (and branch-a branch-b)
        (let [diff-a (file-diffs (container-name system-id branch-a))
              diff-b (file-diffs (container-name system-id branch-b))
              conflicts (set/intersection diff-a diff-b)]
          (mapv (fn [path] {:path (str/split path #"/")
                            :ours path
                            :theirs path})
                conflicts))
        [])))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    (let [meta-dir (File. (str workspace-path "/metadata"))
          all-branches (when (.exists meta-dir)
                         (->> (.listFiles meta-dir)
                              (filter #(.isDirectory %))
                              (map #(.getName %))))
          find-branch (fn [snap-id]
                        (first (filter
                                #(some (fn [c] (= (:id c) (str snap-id)))
                                       (read-commits workspace-path %))
                                all-branches)))
          branch-a (find-branch a)
          branch-b (find-branch b)]
      {:snapshot-a (str a)
       :snapshot-b (str b)
       :diff-a (when branch-a
                 (try (podman-lines "diff" (container-name system-id branch-a))
                      (catch Exception _ [])))
       :diff-b (when branch-b
                 (try (podman-lines "diff" (container-name system-id branch-b))
                      (catch Exception _ [])))}))

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn system-id workspace-path current-branch)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id))

  p/GarbageCollectable
  (gc-roots [_]
    (let [meta-dir (File. (str workspace-path "/metadata"))]
      (if (.exists meta-dir)
        (->> (.listFiles meta-dir)
             (filter #(.isDirectory %))
             (keep (fn [d]
                     (let [commit (latest-commit workspace-path (.getName d))]
                       (:id commit))))
             set)
        #{})))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids _opts]
    (let [meta-dir (File. (str workspace-path "/metadata"))]
      (when (.exists meta-dir)
        (doseq [branch-dir (.listFiles meta-dir)
                :when (.isDirectory branch-dir)]
          (let [branch (.getName branch-dir)]
            (doseq [snap-id snapshot-ids]
              (let [img (image-name system-id branch (str snap-id))]
                (when (image-exists? img)
                  (try (podman "rmi" img) (catch Exception _)))))))))
    this))

;; ============================================================
;; Factory functions
;; ============================================================

(defn create
  "Create a Podman adapter for an existing system.
   Expects containers and images to already exist."
  ([system-id base-image workspace-path]
   (create system-id base-image workspace-path {}))
  ([system-id base-image workspace-path opts]
   (->PodmanSystem system-id base-image workspace-path
                   (or (:initial-branch opts) "main")
                   (w/create-watcher-state)
                   (atom {}))))

(defn init!
  "Initialize a new Podman workspace.
   Creates the base image (from an existing image or Dockerfile),
   starts a main branch container, and sets up metadata tracking.

   Options:
     :base-image   - existing image to use (default: 'ubuntu:24.04')
     :system-name  - override system-id"
  ([workspace-path] (init! workspace-path {}))
  ([workspace-path opts]
   (let [system-id (or (:system-name opts)
                       (str "ygg-" (subs (str (UUID/randomUUID)) 0 8)))
         base-img (or (:base-image opts) "ubuntu:24.04")
         main-img (image-name system-id "main")]
     ;; Ensure base image is available
     (when-not (image-exists? base-img)
       (podman "pull" base-img))
     ;; Tag as our main branch image
     (podman "tag" base-img main-img)
     ;; Create workspace metadata
     (ensure-dirs! (str workspace-path "/metadata/main"))
     (write-commits! workspace-path "main" [])
     ;; Start main container
     (start-container! system-id "main" main-img)
     ;; Create entries directory inside container
     (let [cname (container-name system-id "main")]
       (sh "podman" "exec" cname "mkdir" "-p" "/entries"))
     (create system-id base-img workspace-path
             (assoc opts :system-name system-id)))))

(defn destroy!
  "Destroy all containers, images, and metadata for this system."
  [^PodmanSystem sys]
  (w/stop-polling! (:watcher-state sys))
  (let [{:keys [system-id workspace-path]} sys
        meta-dir (File. (str workspace-path "/metadata"))]
    ;; Stop and remove all branch containers
    (when (.exists meta-dir)
      (doseq [branch-dir (.listFiles meta-dir)]
        (when (.isDirectory branch-dir)
          (let [branch (.getName branch-dir)]
            (stop-container! system-id branch)
            ;; Remove branch images
            (let [commits (read-commits workspace-path branch)]
              (doseq [c commits]
                (let [img (image-name system-id branch (:id c))]
                  (when (image-exists? img)
                    (try (podman "rmi" "-f" img) (catch Exception _)))))
              (let [latest-img (image-name system-id branch)]
                (when (image-exists? latest-img)
                  (try (podman "rmi" "-f" latest-img) (catch Exception _)))))))))
    ;; Remove workspace
    (when (.exists (File. workspace-path))
      (doseq [f (reverse (file-seq (File. workspace-path)))]
        (.delete f)))))

(defn commit!
  "Commit the current container state as a new snapshot."
  ([sys] (commit! sys nil))
  ([sys message]
   (let [{:keys [system-id workspace-path current-branch]} sys
         branch current-branch
         cname (container-name system-id branch)
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
         img (image-name system-id branch commit-id)]
     ;; Commit container to image
     (podman "commit" cname img)
     ;; Update latest tag
     (podman "tag" img (image-name system-id branch))
     ;; Record commit
     (write-commits! workspace-path branch (conj commits commit))
     commit-id)))

(defn write-file!
  "Write a file inside the current branch's container."
  [sys relative-path content]
  (let [{:keys [system-id current-branch]} sys
        branch current-branch
        cname (container-name system-id branch)
        ;; Write to temp file, then copy in
        tmp (str "/tmp/ygg-write-" (System/nanoTime))]
    (spit tmp content)
    (try
      ;; Ensure parent directory exists
      (let [parent (.getParent (File. (str "/" relative-path)))]
        (when parent
          (sh "podman" "exec" cname "mkdir" "-p" parent)))
      (podman "cp" tmp (str cname ":/" relative-path))
      (finally
        (.delete (File. tmp))))))

(defn read-file
  "Read a file from the current branch's container."
  [sys relative-path]
  (let [{:keys [system-id current-branch]} sys
        branch current-branch
        cname (container-name system-id branch)
        result (sh "podman" "exec" cname "cat" (str "/" relative-path))]
    (when (zero? (:exit result))
      (:out result))))

(defn delete-file!
  "Delete a file inside the current branch's container."
  [sys relative-path]
  (let [{:keys [system-id current-branch]} sys
        branch current-branch
        cname (container-name system-id branch)]
    (sh "podman" "exec" cname "rm" "-f" (str "/" relative-path))))
