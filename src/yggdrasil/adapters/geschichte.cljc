(ns yggdrasil.adapters.geschichte
  "Geschichte adapter for Yggdrasil's two-level version model.

  Yggdrasil `Branchable` maps to logical Geschichte refs. `Overlayable` maps
  to structurally shared Datahike workspace branches, keeping those physical
  branches out of the user-visible Git branch namespace.

  Requires Geschichte on the classpath. Only load this namespace when
  Geschichte is available as a dependency — it is an OPTIONAL adapter dep,
  exactly like `yggdrasil.adapters.datahike`.

  Portability: every Geschichte namespace this adapter touches is `.cljc`
  (`repo` `query` `diff` `workspace` `merge.core` `bytes` `async`), so the
  adapter compiles and runs on both platforms. In particular the three-tree
  merge planner lives in the portable `geschichte.merge.core`; the JVM-only
  `geschichte.merge` is a Datahike-native-BFS optimization of the same
  algorithm and is deliberately NOT used here."
  (:refer-clojure :exclude [await ancestors])
  (:require [clojure.set :as set]
            [clojure.string :as str]
            [datahike.api :as d]
            [geschichte.async :as execution]
            [geschichte.bytes :as gbytes]
            [geschichte.diff :as gdiff]
            [geschichte.merge.core :as graph]
            [geschichte.query :as query]
            [geschichte.repo :as repo]
            [geschichte.workspace :as workspace]
            [is.simm.partial-cps.async :refer [await]]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t])
  #?(:cljs (:require-macros [geschichte.macros :refer [platform-async]]))
  #?(:clj (:require [geschichte.macros :refer [platform-async]])))

(defn- branch-ref [name]
  (let [name (clojure.core/name name)]
    (if (str/starts-with? name "refs/") name (str "refs/heads/" name))))

(defn- branch-name [ref]
  (keyword (str/replace ref #"^refs/heads/" "")))

(defn- head [conn]
  (repo/head-commit conn))

(defn- commit-index [conn]
  (into {} (map (juxt :geschichte.commit/id identity)) (query/commits @conn)))

(defn- parents-of [commits id]
  (mapv :geschichte.commit/id (:geschichte.commit/parents (get commits id))))

(defn- resolve-commit [conn value]
  (cond
    (nil? value) (head conn)
    (map? value) value
    (keyword? value) (some->> (get (repo/refs conn) (branch-ref value))
                              (repo/commit-by-id conn))
    (uuid? value) (repo/commit-by-id conn value)
    :else (some #(when (= (str (:geschichte.commit/id %)) (str value)) %)
                (query/commits @conn))))

;; ============================================================
;; File-level diff
;;
;; Tree entries are `{:content <content-id> :size n :mode m}` — stable logical
;; metadata, so the CHANGE SET is a pure comparison of two path maps and costs
;; no blob reads. Only rendering the patch reads content, which is why it is
;; separately gated by `:patch?` and `:max-patch-bytes`.
;; ============================================================

(def ^:private default-max-patch-bytes
  "Per-side byte ceiling above which a file is summarized instead of rendered."
  262144)

(defn- classify-files
  "Change set between two trees, in the `files` shape shared with `GitDiff`."
  [a-tree b-tree]
  (into []
        (keep (fn [path]
                (let [a (get a-tree path)
                      b (get b-tree path)]
                  (cond
                    (nil? a) {:status :added :path path}
                    (nil? b) {:status :deleted :path path}
                    (not= (select-keys a [:content :mode])
                          (select-keys b [:content :mode]))
                    {:status :modified :path path}
                    :else nil))))
        (sort (set/union (set (keys a-tree)) (set (keys b-tree))))))

(defn- side-text
  "Decode one side of a file diff. Returns `{:text s}`, or `{:skip reason}` when
  the content is binary or over `max-bytes` — never a silently empty side, which
  would render a bogus whole-file deletion."
  [conn commit path entry max-bytes]
  (platform-async
   (cond
     (nil? entry) {:text ""}
     (> (or (:size entry) 0) max-bytes) {:skip :too-large}
     :else
     (let [bs (await (repo/read-at conn commit path))
           text (when bs (gbytes/decode-utf8 bs))]
       (cond
         (nil? text) {:text ""}
         ;; Git's own heuristic: a NUL byte means "not text". UTF-8 decoding
         ;; maps a 0 byte to NUL, so this survives the decode.
         (str/includes? text "\u0000") {:skip :binary}
         :else {:text text})))))

(defn- patch-header [status path]
  (let [a-name (if (= status :added) "/dev/null" (str "a/" path))
        b-name (if (= status :deleted) "/dev/null" (str "b/" path))]
    [a-name b-name (str "diff --geschichte a/" path " b/" path "\n")]))

(defn- file-patch
  "Unified patch plus insertion/deletion counts for one changed path."
  [conn a-commit b-commit a-tree b-tree {:keys [status path]}
   {:keys [context max-patch-bytes] :or {context 3}}]
  (platform-async
   (let [max-bytes (or max-patch-bytes default-max-patch-bytes)
         [a-name b-name header] (patch-header status path)
         a (await (side-text conn a-commit path (get a-tree path) max-bytes))
         b (await (side-text conn b-commit path (get b-tree path) max-bytes))
         skip (or (:skip a) (:skip b))]
     (if skip
       {:path path
        :insertions 0
        :deletions 0
        :patch (str header
                    (case skip
                      :binary "Binary files differ\n"
                      :too-large "File exceeds the patch size limit\n"))}
       (let [result (gdiff/diff-text (:text a) (:text b))
             ops (gdiff/operations result)]
         {:path path
          :insertions (count (filter #(= :add (first %)) ops))
          :deletions (count (filter #(= :del (first %)) ops))
          :patch (str header (gdiff/unified result {:a-name a-name
                                                    :b-name b-name
                                                    :context context}))})))))

(defn- stat-graph [insertions deletions]
  (let [total (+ insertions deletions)
        cap 40
        [i d] (if (<= total cap)
                [insertions deletions]
                ;; Scale proportionally, but never render a change as zero marks.
                (let [scale (/ cap (double total))]
                  [(max (if (pos? insertions) 1 0) (int (* insertions scale)))
                   (max (if (pos? deletions) 1 0) (int (* deletions scale)))]))]
    (str (str/join (repeat i "+")) (str/join (repeat d "-")))))

(defn- plural [n word]
  (str n " " word (when (not= 1 n) "s")))

(defn- render-stat
  "Git-shaped `--stat` summary over the per-file counts."
  [per-file]
  (if (empty? per-file)
    ""
    (let [width (reduce max 0 (map (comp count :path) per-file))
          lines (map (fn [{:keys [path insertions deletions]}]
                       (str " " path
                            (str/join (repeat (- width (count path)) " "))
                            " | " (+ insertions deletions) " "
                            (stat-graph insertions deletions)))
                     per-file)
          insertions (reduce + (map :insertions per-file))
          deletions (reduce + (map :deletions per-file))]
      (str (str/join "\n" lines)
           "\n " (plural (count per-file) "file") " changed"
           (when (pos? insertions) (str ", " (plural insertions "insertion") "(+)"))
           (when (pos? deletions) (str ", " (plural deletions "deletion") "(-)"))
           "\n"))))

;; ============================================================
;; Three-tree merge planning
;; ============================================================

(defn- merge-plan
  "Plan a three-tree merge between two resolved commits.

  A merge base is found over the portable commit graph. When there is NO common
  ancestor the plan is computed against an EMPTY base, which makes every path
  both sides changed differently a conflict — conservative by construction, so
  `conflicts` can never answer `[]` for unrelated histories."
  [conn ours-commit theirs-commit]
  (platform-async
   (let [commits (commit-index conn)
         ours (:geschichte.commit/id ours-commit)
         theirs (:geschichte.commit/id theirs-commit)
         base-id (graph/merge-base #(parents-of commits %) ours theirs)
         base-tree (if base-id
                     (await (repo/tree-at conn (repo/commit-by-id conn base-id)))
                     {})
         ours-tree (await (repo/tree-at conn ours-commit))
         theirs-tree (await (repo/tree-at conn theirs-commit))]
     (assoc (graph/plan-trees base-id ours theirs base-tree ours-tree theirs-tree
                              ;; Content-level merge: geschichte resolves a path
                              ;; from its tree entries, and where both sides
                              ;; changed it, this merges the two blobs line by
                              ;; line rather than declaring a conflict. Without
                              ;; it every path both sides touched conflicts,
                              ;; overlapping or not — which for concurrent
                              ;; writers is most of them.
                              ;;
                              ;; JVM ONLY, and that is structural rather than an
                              ;; omission: `plan-trees` is a synchronous pure
                              ;; function, while on ClojureScript reading and
                              ;; storing content are partial-cps computations. A
                              ;; resolver cannot be awaited from inside a sync
                              ;; planner, so cljs keeps the file-level behaviour
                              ;; — conservative, never wrong, just coarser.
                              #?(:clj {:resolve-content (repo/content-merger conn)}
                                 :cljs nil))
            :baseless? (nil? base-id)))))

(declare ->GeschichteSystem)

(defrecord GeschichteOverlay
           [parent local-writes workspace-branch base-snapshot mode]
  p/Overlayable
  (base-ref [_] base-snapshot)
  (peek-parent [_] parent)
  (peek-parent [_ _] parent)
  (overlay-writes [_]
    {:base base-snapshot
     :worktree (query/worktree @(:conn @local-writes))
     :stage (query/stage @(:conn @local-writes))})
  (advance! [this] (p/advance! this nil))
  (advance! [this _opts]
    (platform-async
     (await (workspace/advance! (:conn parent) (:conn @local-writes)))
     this))
  (merge-down! [this] (p/merge-down! this nil))
  (merge-down! [_ opts]
    (platform-async
     (let [parent-status (await (repo/status (:conn parent)))]
       (when-not (:clean? parent-status)
         (throw (ex-info "Cannot merge a Geschichte workspace into a dirty parent"
                         {:status parent-status})))
       (let [{:keys [new]}
             (await (workspace/publish! (:conn parent) (:conn @local-writes)
                                        (select-keys opts [:ref :commit :force?])))]
         ;; Publication intentionally transfers only immutable history + the
         ;; canonical ref. A live parent workspace also needs its index/worktree
         ;; advanced so virtual filesystem readers observe the merge immediately.
         (await (repo/reset! (:conn parent) new {:mode :hard}))))
     ;; Spindel/Yggdrasil deliberately calls discard! after merge-down! to
     ;; dispose the overlay. Keep cleanup in that single lifecycle phase; doing
     ;; it here made merge-to-parent! attempt to delete the branch twice.
     parent))
  (discard! [this] (p/discard! this nil))
  (discard! [_ _opts]
    (platform-async
     (d/release (:conn @local-writes))
     (await (workspace/remove! (:conn parent) workspace-branch))
     parent)))

(defrecord GeschichteSystem [conn system-name]
  p/SystemIdentity
  (system-id [_]
    (or system-name
        (str "geschichte:" (get-in @conn [:config :store :id]))))
  (system-type [_] :geschichte)
  (capabilities [_]
    (t/->Capabilities true true true true true false true false true))

  p/Snapshotable
  (snapshot-id [_] (some-> (head conn) :geschichte.commit/id str))
  (parent-ids [_]
    ;; `repo/head-commit` pulls the REF TARGET, and that pull pattern carries
    ;; only :db/id, :geschichte.commit/id and :geschichte.commit/snapshot —
    ;; NOT :geschichte.commit/parents. Reading parents off it answers #{} for
    ;; every repository, so re-resolve the tip as a full commit.
    (let [tip (some->> (head conn) :geschichte.commit/id (repo/commit-by-id conn))]
      (set (map (comp str :geschichte.commit/id)
                (:geschichte.commit/parents tip)))))
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (platform-async
     (let [commit (or (resolve-commit conn snap-id)
                      (throw (ex-info "Unknown Geschichte snapshot"
                                      {:snapshot snap-id})))]
       (await (repo/tree-at conn commit)))))
  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (some-> (resolve-commit conn snap-id)
            (select-keys [:geschichte.commit/id :geschichte.commit/message
                          :geschichte.commit/author :geschichte.commit/time
                          :geschichte.commit/snapshot])))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (into #{} (comp (filter #(str/starts-with? % "refs/heads/"))
                    (map branch-name))
          (keys (repo/refs conn))))
  (current-branch [_] (branch-name (repo/current-ref conn)))
  (branch! [this name] (p/branch! this name nil nil))
  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (platform-async
     (let [commit (or (resolve-commit conn from)
                      (throw (ex-info "Unknown branch point" {:from from})))]
       (await (repo/create-ref! conn (branch-ref name) commit))
       this)))
  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name opts]
    (platform-async
     (await (repo/delete-branch! conn (branch-ref name)
                                 (select-keys opts [:force?])))
     this))
  (checkout [this name] (p/checkout this name nil))
  (checkout [_ name opts]
    (platform-async
     (await (repo/checkout! conn (branch-ref name)
                            (select-keys opts [:force?])))
     (->GeschichteSystem conn system-name)))

  p/Graphable
  (history [this] (p/history this nil))
  (history [_ opts]
    (mapv (comp str :geschichte.commit/id)
          (repo/log conn {:limit (or (:limit opts) 100)})))
  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (let [commit (resolve-commit conn snap-id)
          commits (commit-index conn)]
      (set (map str (keys (graph/ancestor-distances
                           #(parents-of commits %)
                           (:geschichte.commit/id commit)))))))
  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [this a b _opts]
    (contains? (p/ancestors this b) (str (:geschichte.commit/id
                                          (resolve-commit conn a)))))
  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [_ a b _opts]
    (let [commits (commit-index conn)
          a (:geschichte.commit/id (resolve-commit conn a))
          b (:geschichte.commit/id (resolve-commit conn b))]
      (some-> (graph/merge-base #(parents-of commits %) a b) str)))
  (commit-graph [_ _opts]
    (let [commits (commit-index conn)]
      {:nodes (into {}
                    (map (fn [[id commit]]
                           [(str id)
                            {:parent-ids
                             (set (map (comp str :geschichte.commit/id)
                                       (:geschichte.commit/parents commit)))
                             :meta (select-keys
                                    commit
                                    [:geschichte.commit/message
                                     :geschichte.commit/author
                                     :geschichte.commit/time])}]))
                    commits)
       :branches (into {}
                       (comp (filter (fn [[ref _]]
                                       (str/starts-with? ref "refs/heads/")))
                             (map (fn [[ref id]] [(branch-name ref) (str id)])))
                       (repo/refs conn))
       :roots (into #{}
                    (keep (fn [[id commit]]
                            (when (empty? (:geschichte.commit/parents commit))
                              (str id))))
                    commits)}))
  (commit-graph [this] (p/commit-graph this nil))
  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [this snap-id _opts] (p/snapshot-meta this snap-id))

  p/Mergeable
  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b opts]
    (platform-async
     (let [ca (resolve-commit conn a)
           cb (resolve-commit conn b)]
       (if-not (and ca cb)
         (t/diff-error (str a) (str b) "Unknown Geschichte snapshot")
         (let [a-tree (await (repo/tree-at conn ca))
               b-tree (await (repo/tree-at conn cb))
               files (classify-files a-tree b-tree)
               ;; Patch rendering is the only part that reads blobs; callers
               ;; listing a change set can turn it off.
               render? (not (false? (:patch? opts)))
               per-file (volatile! [])]
           (when render?
             (doseq [file files]
               (let [rendered (await (file-patch conn ca cb a-tree b-tree
                                                 file opts))]
                 (vswap! per-file conj rendered))))
           (t/->GeschichteDiff
            (str (:geschichte.commit/id ca))
            (str (:geschichte.commit/id cb))
            (render-stat @per-file)
            (str/join (map :patch @per-file))
            files
            {:files-changed (count files)
             :insertions (reduce + 0 (map :insertions @per-file))
             :deletions (reduce + 0 (map :deletions @per-file))}))))))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b _opts]
    (platform-async
     (let [ca (resolve-commit conn a)
           cb (resolve-commit conn b)]
       (if-not (and ca cb)
         []
         (let [{:keys [conflicts]} (await (merge-plan conn ca cb))]
           (mapv (fn [[path {:keys [base ours theirs]}]]
                   (t/->Conflict path base ours theirs))
                 conflicts))))))

  (merge! [this source] (p/merge! this source nil))
  (merge! [this source opts]
    (platform-async
     (let [theirs (or (resolve-commit conn source)
                      (throw (ex-info "Unknown Geschichte merge source"
                                      {:source source})))
           ours (or (head conn)
                    (throw (ex-info "Cannot merge into an empty repository"
                                    {:source source})))
           {:keys [kind base clean? conflicts baseless?] :as plan}
           (await (merge-plan conn ours theirs))]
       (cond
         ;; Nothing to do: identical tips, or theirs is already an ancestor of
         ;; ours. Geschichte's own planner would build an empty merge commit
         ;; for the second case; a no-op is the honest answer.
         (or (= kind :up-to-date) (= base (:geschichte.commit/id theirs)))
         this

         baseless?
         (throw (ex-info "Geschichte commits have no common ancestor"
                         {:ours (:geschichte.commit/id ours)
                          :theirs (:geschichte.commit/id theirs)}))

         (not clean?)
         (throw (ex-info "Geschichte merge has unresolved conflicts"
                         {:type :geschichte/merge-conflict
                          :conflicts conflicts}))

         :else
         (do (await (repo/prepare-merge! conn plan))
             (await (repo/commit!
                     conn {:message (or (:message opts)
                                        (str "Merge " (if (keyword? source)
                                                        (name source)
                                                        (:geschichte.commit/id theirs))))
                           :author (or (:author opts) "unknown")
                           :time (:time opts)}))
             this)))))

  p/Committable
  (commit! [this] (p/commit! this nil nil))
  (commit! [this message] (p/commit! this message nil))
  (commit! [this message opts]
    (platform-async
     (await (repo/commit! conn {:message (or message "")
                                :author (or (:author opts) "unknown")
                                :time (:time opts)}))
     this))

  p/GarbageCollectable
  (gc-roots [_]
    ;; Every ref tip, as the snapshot it names. These are the git refs whose
    ;; commits must survive collection.
    ;; `repo/refs` is {logical-ref -> commit-id-or-nil}; a ref with no target
    ;; (a freshly created branch) contributes nothing.
    (->> (repo/refs conn)
         vals
         (remove nil?)
         (keep (fn [cid] (:geschichte.commit/snapshot (repo/commit-by-id conn cid))))
         (map str)
         set))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [_ _snapshot-ids opts]
    ;; Reclaim orphaned Datahike index blobs. Datahike computes its own
    ;; reachability, so the coordinator's snapshot-ids are ignored — same
    ;; contract as the datahike adapter.
    ;;
    ;; A NON-EPOCH `:remove-before` IS REFUSED, and that is the whole point of
    ;; this method existing rather than letting the generic adapter run.
    ;; `:geschichte.commit/snapshot` resolves through `d/commit-as-db`, so a
    ;; Datahike commit record IS a Git tree — and Datahike follows ancestry only
    ;; while a record is newer than the cutoff, because its own liveness rule is
    ;; reachability AND recency. Geschichte's refs are ordinary datoms, invisible
    ;; to that rule, so every commit looks like unreferenced old history.
    ;;
    ;; Measured: three commits plus a non-epoch cutoff reclaimed 75 keys, after
    ;; which `repo/tree`, `repo/status` and `repo/tree-at` all throw "Geschichte
    ;; commit names a missing Datahike checkpoint" while `repo/read` still works
    ;; — a silently half-destroyed repository. Refusing is strictly better than
    ;; reclaiming and bricking.
    ;;
    ;; Epoch (the default) is safe and reclaims writes published to Konserve
    ;; that no transaction ever made reachable: interrupted pack imports, killed
    ;; agents, aborted large writes.
    (let [opts (t/async-gc-opts "geschichte/gc-sweep!" opts)
          rb   (:remove-before opts)
          epoch (#?(:clj java.util.Date. :cljs js/Date.) 0)]
      (when (and rb (pos? (inst-ms rb)))
        (throw (ex-info (str "Refusing a non-epoch :remove-before on a Geschichte repository. "
                             "Geschichte's refs are datoms, so Datahike cannot see them as "
                             "roots; a cutoff would delete the commit snapshots the refs name "
                             "and silently brick the repository.")
                        {:type :geschichte/unsafe-gc-cutoff
                         :remove-before rb
                         :system system-name})))
      (platform-async
       (if (:dry-run? opts)
         {:system-id system-name :dry-run? true}
         (let [removed (await (execution/io-result (d/gc-storage conn epoch)
                                                   execution/default-opts))]
           {:system-id system-name
            :reclaimed (if (counted? removed) (count removed) 0)})))))

  p/Overlayable
  (overlay [this _opts]
    (platform-async
     (let [branch (workspace/branch-key (random-uuid))
           base (p/snapshot-id this)
           _ (await (workspace/fork! conn branch))
           cfg (assoc (:config @conn) :branch branch)
           child-conn (await (execution/io-result
                              (d/connect cfg #?(:clj {:sync? true}
                                                :cljs {:sync? false}))
                              execution/default-opts))]
       (->GeschichteOverlay this
                            (atom (->GeschichteSystem child-conn system-name))
                            branch base :frozen)))))

(defn create
  "Wrap an initialized Geschichte connection as a Yggdrasil system."
  ([conn] (create conn {}))
  ([conn {:keys [system-name]}]
   (->GeschichteSystem conn system-name)))

(defn connection
  "Return the active Geschichte workspace connection represented by a
  Yggdrasil system or overlay. This is the virtual-filesystem integration seam:
  consumers receive a branch-local connection without assuming that the
  workspace has a physical `working-path`."
  [system]
  (cond
    (instance? GeschichteSystem system) (:conn system)
    (instance? GeschichteOverlay system) (some-> system :local-writes deref :conn)
    :else nil))

(defn workspace-id
  "Stable identity for the active virtual workspace. The repository store id is
  shared by structurally related workspaces; the Datahike branch distinguishes
  one fork from another."
  [system]
  (when-let [conn (connection system)]
    [(get-in @conn [:config :store :id])
     (get-in @conn [:config :branch] :db)]))
