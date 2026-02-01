(ns yggdrasil.adapters.git
  "Git adapter for Yggdrasil protocols.

  Uses git worktrees for concurrent branch access — each branch gets its own
  working directory and index, so operations on different branches don't
  conflict. Supports both orchestrator mode (yggdrasil drives git) and
  observer mode (external git operations detected via polling watcher)."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.watcher :as w]
            [clojure.java.shell :refer [sh]]
            [clojure.string :as str]
            [clojure.set :as set])
  (:import [java.util.concurrent.locks ReentrantLock]))

(defn- git
  "Run a git command in the given directory. Returns trimmed stdout on success."
  [dir & args]
  (let [result (apply sh "git" "-C" dir args)]
    (if (zero? (:exit result))
      (str/trim (:out result))
      (throw (ex-info (str "git error: " (str/trim (:err result)))
                      {:args (vec args) :exit (:exit result) :err (:err result)})))))

(defn- git-ok?
  "Run a git command, return true if exit 0, false otherwise."
  [dir & args]
  (zero? (:exit (apply sh "git" "-C" dir args))))

(defn- git-lines
  "Run git command, return output as vector of lines."
  [dir & args]
  (let [out (apply git dir args)]
    (if (str/blank? out) [] (str/split-lines out))))

(defn- branch-path
  "Get the working directory for a branch.
   Main branch lives at repo-path, others in worktrees-dir."
  [repo-path worktrees-dir branch-name]
  (if (= branch-name "main")
    repo-path
    (str worktrees-dir "/" branch-name)))

(defn- current-wt
  "Get the worktree path for the current branch."
  [repo-path worktrees-dir current-branch]
  (branch-path repo-path worktrees-dir current-branch))

(defn- head-sha
  "Get HEAD sha for a worktree directory."
  [wt-path]
  (try (git wt-path "rev-parse" "HEAD")
       (catch Exception _ nil)))

(defn- branch-list
  "List all branches in the repo."
  [repo-path]
  (try (git-lines repo-path "branch" "--list" "--format=%(refname:short)")
       (catch Exception _ [])))

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

(defmacro ^:private with-branch-lock
  [branch-locks branch-name & body]
  `(with-branch-lock* ~branch-locks ~branch-name (fn [] ~@body)))

(defn- poll-fn
  "Poll function for git watcher. Detects new commits, branch changes,
   and external checkout operations (for observer mode).
   Note: Does not mutate system state - only emits events."
  [repo-path worktrees-dir current-branch last-state]
  (let [;; Detect external branch switches on the main worktree
        actual-branch (try (git repo-path "rev-parse" "--abbrev-ref" "HEAD")
                           (catch Exception _ "main"))
        ;; Use actual branch from git (may differ from system's current-branch)
        wt (current-wt repo-path worktrees-dir actual-branch)
        current-head (head-sha wt)
        current-branches (set (branch-list repo-path))
        prev-head (:head last-state)
        prev-branches (or (:branches last-state) #{})
        prev-branch (:branch last-state)
        events (cond-> []
                 (and current-head (not= current-head prev-head))
                 (conj {:type :commit
                        :snapshot-id current-head
                        :branch actual-branch
                        :timestamp (System/currentTimeMillis)})

                 (and prev-branch (not= actual-branch prev-branch))
                 (conj {:type :checkout
                        :branch actual-branch
                        :timestamp (System/currentTimeMillis)})

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
    {:state {:head current-head
             :branches current-branches
             :branch actual-branch}
     :events events}))

(defrecord GitSystem [repo-path worktrees-dir current-branch
                      system-name watcher-state branch-locks]
  p/SystemIdentity
  (system-id [_] (or system-name (str "git:" repo-path)))
  (system-type [_] :git)
  (capabilities [_]
    (t/->Capabilities true true true true false true))

  p/Snapshotable
  (snapshot-id [_]
    (let [wt (current-wt repo-path worktrees-dir current-branch)]
      (head-sha wt)))

  (parent-ids [_]
    (let [wt (current-wt repo-path worktrees-dir current-branch)
          out (try (git wt "log" "-1" "--format=%P" "HEAD")
                   (catch Exception _ ""))]
      (if (str/blank? out)
        #{}
        (set (str/split out #" ")))))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    {:repo-path repo-path :commit (str snap-id)})

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (let [format "%H%n%P%n%an <%ae>%n%at%n%s"
          lines (try (git-lines repo-path "log" "-1" (str "--format=" format) (str snap-id))
                     (catch Exception _ []))]
      (when (>= (count lines) 5)
        {:snapshot-id (nth lines 0)
         :parent-ids (if (str/blank? (nth lines 1))
                       #{}
                       (set (str/split (nth lines 1) #" ")))
         :author (nth lines 2)
         :timestamp (nth lines 3)
         :message (nth lines 4)})))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (let [listed (set (map keyword (branch-list repo-path)))
          current-kw (keyword current-branch)]
      (conj listed current-kw)))

  (current-branch [_]
    (keyword current-branch))

  (branch! [this name]
    (let [branch-str (clojure.core/name name)
          wt-path (branch-path repo-path worktrees-dir branch-str)
          source-wt (current-wt repo-path worktrees-dir current-branch)]
      ;; Create worktree for the new branch from current branch's HEAD
      (git source-wt "worktree" "add" "-b" branch-str wt-path)
      ;; Ensure entries dir exists
      (.mkdirs (java.io.File. (str wt-path "/entries")))
      this))

  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (let [branch-str (clojure.core/name name)
          wt-path (branch-path repo-path worktrees-dir branch-str)]
      ;; Create worktree from a specific commit
      (git repo-path "worktree" "add" "-b" branch-str wt-path (str from))
      (.mkdirs (java.io.File. (str wt-path "/entries")))
      this))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (let [branch-str (clojure.core/name name)
          wt-path (branch-path repo-path worktrees-dir branch-str)]
      ;; Remove worktree first, then the branch
      (git repo-path "worktree" "remove" "--force" wt-path)
      (git repo-path "branch" "-D" branch-str)
      this))

  (checkout [this name] (p/checkout this name nil))
  (checkout [_ name _opts]
    (let [branch-str (clojure.core/name name)
          wt-path (branch-path repo-path worktrees-dir branch-str)]
      ;; Verify the worktree exists
      (when-not (.exists (java.io.File. wt-path))
        (throw (ex-info (str "Branch worktree not found: " branch-str)
                        {:branch branch-str :path wt-path})))
      ;; Return NEW GitSystem with updated branch (value semantics)
      ;; This enables fork-safe usage where each fork gets independent state
      (->GitSystem repo-path worktrees-dir
                   branch-str  ; Plain value, not atom
                   system-name watcher-state branch-locks)))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ opts]
    (let [wt (current-wt repo-path worktrees-dir current-branch)
          args (cond-> ["log" "--format=%H"]
                 (:limit opts) (conj (str "-" (:limit opts)))
                 (:since opts) (conj (str (:since opts) "..HEAD")))]
      (apply git-lines wt args)))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [all (git-lines repo-path "rev-list" (str snap-id))]
      (vec (rest all))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [_ a b _opts]
    (git-ok? repo-path "merge-base" "--is-ancestor" (str a) (str b)))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (try
      (git repo-path "merge-base" (str a) (str b))
      (catch Exception _ nil)))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [this _opts]
    (let [all-commits (git-lines repo-path "rev-list" "--all")
          nodes (into {}
                      (for [c all-commits]
                        (let [meta (p/snapshot-meta this c)]
                          [c {:parent-ids (or (:parent-ids meta) #{})
                              :meta (dissoc meta :snapshot-id :parent-ids)}])))
          branch-heads (into {}
                             (for [b (p/branches this)]
                               [b (git repo-path "rev-parse" (clojure.core/name b))]))]
      {:nodes nodes
       :branches branch-heads
       :roots (set (filter #(empty? (get-in nodes [% :parent-ids])) all-commits))}))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts]
    (when-let [meta (p/snapshot-meta this snap-id)]
      (dissoc meta :snapshot-id)))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source opts]
    (let [branch-name (if (keyword? source)
                        (clojure.core/name source)
                        (str source))
          wt (current-wt repo-path worktrees-dir current-branch)
          msg (or (:message opts) (str "Merge " branch-name))
          args (concat ["merge" "--no-edit" "-m" msg] [branch-name])]
      (with-branch-lock branch-locks current-branch
        (apply git wt args))
      this))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    (let [base (try (git repo-path "merge-base" (str a) (str b))
                    (catch Exception _ nil))]
      (if base
        (let [result (sh "git" "-C" repo-path "merge-tree" base (str a) (str b))]
          (if (zero? (:exit result))
            []
            [{:path [:merge-tree] :base base :ours (str a) :theirs (str b)}]))
        [])))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b _opts]
    {:snapshot-a (str a)
     :snapshot-b (str b)
     :diff (try (git repo-path "diff" "--stat" (str a) (str b))
                (catch Exception _ ""))})

  p/Watchable
  (watch! [this callback] (p/watch! this callback {}))
  (watch! [_ callback opts]
    (let [interval (or (:poll-interval-ms opts) 1000)
          watch-id (str (java.util.UUID/randomUUID))]
      (w/add-callback! watcher-state watch-id callback)
      (w/start-polling! watcher-state
                        (partial poll-fn repo-path worktrees-dir current-branch)
                        interval)
      watch-id))

  (unwatch! [this watch-id] (p/unwatch! this watch-id nil))
  (unwatch! [_ watch-id _opts]
    (w/remove-callback! watcher-state watch-id)))

(defn create
  "Create a Git adapter for an existing repository with worktree support.
   (create \"/path/to/repo\")
   (create \"/path/to/repo\" {:system-name \"my-repo\"})

   The worktrees-dir defaults to <repo-path>-worktrees."
  ([repo-path] (create repo-path {}))
  ([repo-path opts]
   (let [wt-dir (or (:worktrees-dir opts) (str repo-path "-worktrees"))]
     (.mkdirs (java.io.File. wt-dir))
     (->GitSystem repo-path wt-dir
                  (or (:initial-branch opts) "main")
                  (:system-name opts)
                  (w/create-watcher-state)
                  (atom {})))))

(defn init!
  "Initialize a new git repo at path and return a GitSystem.
   Creates the directory and runs git init. No initial commit is made
   so that the first user commit is the true root commit.
   Additional branches will use worktrees in <path>-worktrees/."
  ([path] (init! path {}))
  ([path opts]
   (let [dir (java.io.File. path)]
     (.mkdirs dir)
     (git path "init" "--initial-branch=main")
     (git path "config" "user.email" "yggdrasil@test")
     (git path "config" "user.name" "Yggdrasil")
     ;; Create entries directory (for data ops)
     (.mkdirs (java.io.File. (str path "/entries")))
     (create path opts))))
