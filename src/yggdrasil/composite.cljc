(ns yggdrasil.composite
  "Pullback system: coordinates N sub-systems as one logical unit.

   Categorically, this is a fiber product (pullback) over the shared branch
   space. Given systems A and B both mapping to a branch space S via their
   `current-branch` function, the CompositeSystem is the pullback A ×_S B —
   pairs (a, b) where branch(a) = branch(b).

   All protocol operations are applied componentwise, constrained to preserve
   the fiber condition (shared branch). The construction is monoidal:
     pullback(pullback(A, B), C) ≅ pullback(A, B, C)

   Key aggregation strategies:
     branches  → intersection (only branches valid in ALL sub-systems)
     conflicts → union        (any sub-system conflict is a composite conflict)
     gc-roots  → union        (any sub-system root is a composite root)

   Uses VALUE SEMANTICS: mutating operations return new CompositeSystem values.

   **Transactional by default.** `merge!`/`commit!` flush every sub-system
   durable and THEN write `:composite/root` LAST (one konserve k-assoc = the
   atomic flip, exactly like datahike's `:db`). So a reader resolving through
   `:composite/root` never sees a half-merge; a crash before the root write
   leaves the previous committed composite as latest. With konserve-sync
   shipping the root keyword-last, the same gate gives network-atomic
   propagation.

   **Cross-platform (`async+sync`).** The composite carries its sync-mode
   (`opts`) and the history index is an in-memory `{snap-id → entry}` map, so
   every history *read* (snapshot-meta / ancestors / history / …) is plain sync
   on both platforms. Only the methods that touch sub-systems
   (snapshot-id / as-of / commit! / merge! / gc-*) or persist the index dispatch
   through `async+sync` — sync on JVM, async/CPS (you `await`) on cljs. The
   index persists as a content-addressed PSS over the shared `KonserveStorage`,
   rooted at `:composite/root`.

   Two constructor flavors:
     `composite`/`pullback`  — wrap already-built systems; the composite keeps
                               its own store for the index (subs may have their
                               own backends — datahike/git/scriptum).
     `composite-store`       — homogeneous durable-CRDT composite hosting all
                               subs in ONE store, so `:composite/root` is the
                               lone causal gate (the network-atomic, browser/
                               local-first form)."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.storage :as store]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.convergent.durable :as d]
            [clojure.set :as set]
            [clojure.string :as str]
            [hasch.core :as hasch]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

;; The composite's own causal cells (distinct from any sub's :crdt/roots, so
;; subs can co-habit the composite's store under [:crdt/roots id]). The ROOT is
;; written LAST on every persist — the transactional gate.
(def ^:private composite-root-key :composite/root)
(def ^:private composite-freed-key :composite/freed)

;; ============================================================
;; Composite snapshot-id (async+sync — awaits each sub's snapshot-id)
;; ============================================================

(defn- composite-snapshot-id
  "Deterministic UUID from sorted [system-id snapshot-id] pairs. Same combination
   of sub-system states always yields the same ID. (async+sync — sub snapshot-id
   is async on cljs.)"
  [systems opts]
  (async+sync (:sync? opts)
              (async
               (let [pairs (loop [ss (sort-by key systems) acc []]
                             (if (seq ss)
                               (let [[sid sys] (first ss)]
                                 (recur (rest ss) (conj acc [sid (await (p/snapshot-id sys))])))
                               acc))]
                 (str (hasch/uuid (pr-str pairs)))))))

;; ============================================================
;; Capability intersection
;; ============================================================

(defn- intersect-capabilities
  "Intersection of capabilities across all sub-systems."
  [systems]
  (let [caps (map #(p/capabilities (val %)) systems)]
    (t/->Capabilities
     (every? :snapshotable caps)
     (every? :branchable caps)
     (every? :graphable caps)
     (every? :mergeable caps)
     (every? :overlayable caps)
     (every? :watchable caps)
     (every? :garbage-collectable caps)
     (every? :addressable caps)
     (every? :committable caps))))

;; ============================================================
;; History index — an in-memory {snap-id → entry} map, persisted as a
;; content-addressed PSS rooted at :composite/root (the causal gate).
;; ============================================================

(def ^:private entry-comparator
  "Order composite history entries by composite-snap-id (deterministic PSS layout
   → content-addressed root stable across peers)."
  (fn [a b] (compare (:composite-snap-id a) (:composite-snap-id b))))

(defn- lookup-entry
  "Point-lookup a composite history entry by snap-id (sync — in-memory map)."
  [index-atom snap-id]
  (get @index-atom (str snap-id)))

(defn- resolve-sub-snapshots
  "Given a composite snapshot ID, resolve to {system-id → snapshot-id}, or nil."
  [index-atom snap-id]
  (:sub-snapshots (lookup-entry index-atom (str snap-id))))

(defn- resolve-ref
  "Resolve a composite-level ref `x` to the ref for sub-system `sid`: a known
   composite snapshot-id → that system's snapshot-id; a branch keyword / unknown
   id → passed through (branch names are shared across sub-systems)."
  [index-atom sid x]
  (if-let [sub (resolve-sub-snapshots index-atom x)]
    (get sub sid x)
    x))

(defn- load-index
  "Drain the persisted composite index PSS into an in-memory {snap-id → entry}
   map. Empty when ephemeral (no store) or no root yet. (async+sync)"
  [kv-store storage opts]
  (async+sync (:sync? opts)
              (async
               (if (nil? kv-store)
                 {}
                 (let [root (await (kb/k-get kv-store composite-root-key opts))]
                   (if root
                     (let [entries (await (d/set->clj (d/restore-set entry-comparator root storage opts) opts))]
                       (into {} (map (fn [e] [(:composite-snap-id e) e])) entries))
                     {}))))))

(defn- persist-index!
  "Rebuild the index PSS from the in-memory map, write its nodes, then write
   `:composite/freed` and FINALLY `:composite/root` — the causal gate, last. A
   no-op when ephemeral (no kv-store). (async+sync)"
  [kv-store storage index-atom opts]
  (async+sync (:sync? opts)
              (async
               (when kv-store
                 (let [entries (sort-by :composite-snap-id (vals @index-atom))
                       pss (loop [es entries s (d/empty-set storage entry-comparator)]
                             (if (seq es)
                               (recur (rest es) (await (d/set-conj s (first es) entry-comparator opts)))
                               s))
                       root (await (d/store-set! pss storage opts))]
                   (await (kb/k-assoc kv-store composite-freed-key @(:freed-atom storage) opts))
                   ;; ROOT LAST — never half-visible; konserve-sync ships it keyword-last.
                   (await (kb/k-assoc kv-store composite-root-key root opts))))
               nil)))

(defn- register-initial-snapshot!
  "Record the initial composite snapshot if absent (idempotent on reopen).
   (async+sync — awaits sub snapshot-ids + persist.)"
  [index-atom kv-store storage sys-map snap opts]
  (async+sync (:sync? opts)
              (async
               (when-not (lookup-entry index-atom snap)
                 (let [sub-snaps (loop [ss (seq sys-map) acc {}]
                                   (if ss
                                     (let [[sid s] (first ss)]
                                       (recur (next ss) (assoc acc sid (await (p/snapshot-id s)))))
                                     acc))]
                   (swap! index-atom assoc (str snap)
                          {:composite-snap-id (str snap)
                           :parent-ids #{}
                           :timestamp (t/now-ms)
                           :sub-snapshots sub-snaps})
                   (await (persist-index! kv-store storage index-atom opts)))))))

;; ============================================================
;; CompositeSystem (fiber product / pullback)
;; ============================================================

(defrecord CompositeSystem
           [systems              ;; {system-id → system}
            current-branch-name  ;; keyword — shared branch
            composite-name       ;; string — system-id for the composite
            index-atom           ;; atom of {snap-id → entry} (in-memory history)
            kv-store             ;; konserve store (nil for ephemeral)
            store-config         ;; konserve store config (nil for ephemeral)
            storage              ;; shared content-addressed KonserveStorage (nil ephemeral)
            opts]                ;; the composite's sync-mode {:sync? bool}

  p/SystemIdentity
  (system-id [_] composite-name)
  (system-type [_] :composite)
  (capabilities [_] (intersect-capabilities systems))

  p/Snapshotable
  (snapshot-id [_] (composite-snapshot-id systems opts))

  (parent-ids [_]
    (async+sync (:sync? opts)
                (async
                 (let [snap (await (composite-snapshot-id systems opts))
                       entry (lookup-entry index-atom snap)]
                   (or (:parent-ids entry) #{})))))

  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (async+sync (:sync? opts)
                (async
                 (if-let [sub-snaps (resolve-sub-snapshots index-atom snap-id)]
                   (loop [ss (seq systems) acc {}]
                     (if ss
                       (let [[sid sys] (first ss)]
                         (recur (next ss) (assoc acc sid (await (p/as-of sys (get sub-snaps sid))))))
                       acc))
                   nil))))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id nil))
  (snapshot-meta [_ snap-id _opts]
    (when-let [entry (lookup-entry index-atom (str snap-id))]
      {:snapshot-id (str snap-id)
       :parent-ids (:parent-ids entry)
       :timestamp (:timestamp entry)
       :message (:message entry)
       :sub-snapshots (:sub-snapshots entry)}))

  p/Branchable
  (branches [this] (p/branches this nil))
  (branches [_ _opts]
    (if (empty? systems)
      #{}
      (reduce set/intersection
              (map (fn [[_ sys]] (p/branches sys)) systems))))

  (current-branch [_] current-branch-name)

  (branch! [this name]
    (assoc this :systems (reduce-kv (fn [acc sid sys] (assoc acc sid (p/branch! sys name))) {} systems)))
  (branch! [this name from] (p/branch! this name from nil))
  (branch! [this name from _opts]
    (assoc this :systems (reduce-kv (fn [acc sid sys] (assoc acc sid (p/branch! sys name from))) {} systems)))

  (delete-branch! [this name] (p/delete-branch! this name nil))
  (delete-branch! [this name _opts]
    (assoc this :systems (reduce-kv (fn [acc sid sys] (assoc acc sid (p/delete-branch! sys name))) {} systems)))

  (checkout [this name] (p/checkout this name nil))
  (checkout [this name _opts]
    (assoc this
           :systems (reduce-kv (fn [acc sid sys] (assoc acc sid (p/checkout sys name))) {} systems)
           :current-branch-name (keyword name)))

  p/Committable
  (commit! [this] (p/commit! this nil nil))
  (commit! [this message] (p/commit! this message nil))
  (commit! [this message _opts]
    (async+sync (:sync? opts)
                (async
                 (let [old-snap (await (composite-snapshot-id systems opts))
                       new-systems (loop [ss (seq systems) acc {}]
                                     (if ss
                                       (let [[sid sys] (first ss)]
                                         (recur (next ss)
                                                (assoc acc sid (if (satisfies? p/Committable sys)
                                                                 (await (p/commit! sys message))
                                                                 sys))))
                                       acc))
                       new-snap (await (composite-snapshot-id new-systems opts))
                       sub-snaps (loop [ss (seq new-systems) acc {}]
                                   (if ss
                                     (let [[sid sys] (first ss)]
                                       (recur (next ss) (assoc acc sid (await (p/snapshot-id sys)))))
                                     acc))]
                   (swap! index-atom assoc (str new-snap)
                          {:composite-snap-id (str new-snap)
                           :parent-ids #{(str old-snap)}
                           :timestamp (t/now-ms)
                           :message message
                           :sub-snapshots sub-snaps})
                   (await (persist-index! kv-store storage index-atom opts))
                   (assoc this :systems new-systems)))))

  p/Graphable
  (history [this] (p/history this {}))
  (history [_ hopts]
    (async+sync (:sync? opts)
                (async
                 (let [limit (or (:limit hopts) 100)]
                   (loop [snap-id (await (composite-snapshot-id systems opts)) result [] visited #{}]
                     (cond
                       (visited snap-id) result
                       (nil? snap-id) result
                       (>= (count result) limit) result
                       :else
                       (let [entry (lookup-entry index-atom snap-id)
                             parents (or (:parent-ids entry) #{})]
                         (recur (first parents) (conj result snap-id) (conj visited snap-id)))))))))

  (ancestors [this snap-id] (p/ancestors this snap-id nil))
  (ancestors [_ snap-id _opts]
    (loop [queue [(str snap-id)] visited #{}]
      (if (empty? queue)
        (vec (disj visited (str snap-id)))
        (let [current (first queue) rest-q (subvec (vec queue) 1)]
          (if (visited current)
            (recur rest-q visited)
            (let [entry (lookup-entry index-atom current)
                  parents (or (:parent-ids entry) #{})]
              (recur (into rest-q parents) (conj visited current))))))))

  (ancestor? [this a b] (p/ancestor? this a b nil))
  (ancestor? [this a b _opts]
    (contains? (set (p/ancestors this b)) (str a)))

  (common-ancestor [this a b] (p/common-ancestor this a b nil))
  (common-ancestor [this a b _opts]
    (let [ancestors-a (conj (set (p/ancestors this a)) (str a))]
      (loop [queue [(str b)] visited #{}]
        (if (empty? queue)
          nil
          (let [current (first queue) rest-q (subvec (vec queue) 1)]
            (cond
              (visited current) (recur rest-q visited)
              (ancestors-a current) current
              :else
              (let [entry (lookup-entry index-atom current)
                    parents (vec (or (:parent-ids entry) #{}))]
                (recur (into rest-q parents) (conj visited current)))))))))

  (commit-graph [this] (p/commit-graph this nil))
  (commit-graph [_ _opts]
    (async+sync (:sync? opts)
                (async
                 (let [entries (vals @index-atom)
                       cur (await (composite-snapshot-id systems opts))]
                   {:nodes (into {}
                                 (map (fn [e]
                                        [(:composite-snap-id e)
                                         {:parent-ids (:parent-ids e)
                                          :meta {:timestamp (:timestamp e) :message (:message e)}}]))
                                 entries)
                    :branches {current-branch-name cur}
                    :roots (set (keep (fn [e] (when (empty? (or (:parent-ids e) #{})) (:composite-snap-id e))) entries))}))))

  (commit-info [this snap-id] (p/commit-info this snap-id nil))
  (commit-info [_ snap-id _opts]
    (when-let [entry (lookup-entry index-atom (str snap-id))]
      {:parent-ids (:parent-ids entry)
       :timestamp (:timestamp entry)
       :message (:message entry)
       :sub-snapshots (:sub-snapshots entry)}))

  p/Mergeable
  (merge! [this source] (p/merge! this source {}))
  (merge! [this source mopts]
    ;; TRANSACTIONAL: merge every sub-system, then commit! (flush each sub durable,
    ;; then write :composite/root LAST). A crash mid-merge leaves the previous
    ;; committed composite as latest; a reader never sees a half-merge.
    (async+sync (:sync? opts)
                (async
                 (let [new-systems (loop [ss (seq systems) acc {}]
                                     (if ss
                                       (let [[sid sys] (first ss)]
                                         (recur (next ss) (assoc acc sid (await (p/merge! sys source mopts)))))
                                       acc))]
                   (await (p/commit! (assoc this :systems new-systems)
                                     (or (and (map? mopts) (:message mopts)) "merge")
                                     mopts))))))

  (conflicts [this a b] (p/conflicts this a b nil))
  (conflicts [_ a b copts]
    (into []
          (mapcat (fn [[sid sys]]
                    (when (satisfies? p/Mergeable sys)
                      (map (fn [c] (assoc c :system sid))
                           (p/conflicts sys
                                        (resolve-ref index-atom sid a)
                                        (resolve-ref index-atom sid b)
                                        (or copts {}))))))
          systems))

  (diff [this a b] (p/diff this a b nil))
  (diff [_ a b dopts]
    (into {}
          (keep (fn [[sid sys]]
                  (when (satisfies? p/Mergeable sys)
                    [sid (p/diff sys
                                 (resolve-ref index-atom sid a)
                                 (resolve-ref index-atom sid b)
                                 (or dopts {}))])))
          systems))

  p/GarbageCollectable
  (gc-roots [_]
    (async+sync (:sync? opts)
                (async
                 (if (empty? systems)
                   #{}
                   (loop [ss (seq systems) acc #{}]
                     (if ss
                       (recur (next ss) (set/union acc (await (p/gc-roots (val (first ss))))))
                       acc))))))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [_ snapshot-ids gopts]
    (async+sync (:sync? opts)
                (async
                 (if (and kv-store
                          (seq systems)
                          (every? (fn [[_ sys]] (identical? kv-store (:kv-store sys))) systems))
                   ;; SHARED store: a per-sub sweep would delete sibling + index nodes
                   ;; (unreachable from one sub's roots). A correct unified sweep over
                   ;; the union of all roots is a follow-up — skip rather than corrupt.
                   {:skipped :shared-store-gc-todo
                    :reason "subs co-habit the composite store; unified sweep not yet implemented"}
                   ;; SEPARATE stores: each sub owns its store — safe to fan out.
                   (loop [ss (seq systems) acc {}]
                     (if ss
                       (let [[sid sys] (first ss)]
                         (recur (next ss) (assoc acc sid (await (p/gc-sweep! sys snapshot-ids gopts)))))
                       acc)))))))

;; ============================================================
;; Lifecycle
;; ============================================================

(defn flush!
  "Persist the composite history index (rebuild PSS + write :composite/root last).
   No-op when ephemeral. (async+sync)"
  ([composite] (flush! composite (:opts composite)))
  ([composite opts]
   (persist-index! (:kv-store composite) (:storage composite) (:index-atom composite) opts)))

(defn close!
  "Flush and close the composite's store. Safe on ephemeral composites. (async+sync)"
  ([composite] (close! composite (:opts composite)))
  ([composite opts]
   (async+sync (:sync? opts)
               (async
                (await (flush! composite opts))
                (when (and (:kv-store composite) (:store-config composite))
                  (store/close! (:kv-store composite) (:store-config composite)))
                composite))))

;; ============================================================
;; Constructors
;;
;; ONE regular interface: each element of `subs` is a SUB-PROVIDER — either an
;; already-built system (its own backend — datahike/git/scriptum, or a durable
;; CRDT on its own store) or an opener `(fn [kv-store opts] → system)` that opens
;; its sub ON the composite's store. Openers let durable-CRDT subs co-habit one
;; store under their own `[:crdt/roots id]` cells, making `:composite/root` the
;; lone causal gate (the network-atomic, browser/local-first form). Heterogeneous
;; backends are just systems (auto-adopted), index co-located in the composite's
;; own store. No constructor flavors, no same-branch coupling.
;; ============================================================

(defn adopt
  "Make an already-built `system` a composite sub-provider that brings its own
   backend (ignores the composite's store). The composite auto-adopts any
   non-fn element, so this is only needed to be explicit."
  [system] (fn [_kv-store _opts] system))

(defn- ->opener [x] (if (fn? x) x (adopt x)))

(defn- open-subs
  "Run each sub-provider against the composite's `kv-store`, keyed by system-id.
   (async+sync)"
  [subs kv-store opts]
  (async+sync (:sync? opts)
              (async
               (loop [ss (seq subs) acc {}]
                 (if ss
                   (let [sub (await ((->opener (first ss)) kv-store opts))]
                     (recur (next ss) (assoc acc (p/system-id sub) sub)))
                   acc)))))

(defn- build-composite
  "Open the index store, open the subs on it, load the in-memory index, build the
   record, register the initial snapshot. `name` nil → auto from sorted sub-ids
   (`<prefix><id>sep<id>…`). (async+sync)"
  [subs branch name prefix sep store-config sync?]
  (let [opts {:sync? sync?}]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store storage]} (if store-config
                                                    (await (d/open store-config (assoc opts :freed-key composite-freed-key)))
                                                    {:kv-store nil :storage nil})
                       sys-map (await (open-subs subs kv-store opts))
                       composite-name (or name (str prefix (str/join sep (sort (keys sys-map)))))
                       index-atom (atom (await (load-index kv-store storage opts)))
                       sys (->CompositeSystem sys-map branch composite-name
                                              index-atom kv-store store-config storage opts)
                       snap (await (composite-snapshot-id sys-map opts))]
                   (await (register-initial-snapshot! index-atom kv-store storage sys-map snap opts))
                   sys)))))

(defn composite
  "Coordinate N sub-systems as one transactional unit (no same-branch
   constraint — datahike `:db`, scriptum `:main` may differ). `subs` is a seq of
   sub-providers (built systems and/or openers — see the section comment).
   (async+sync — `:sync? false` on cljs and `await` the result.)

   opts:
     :name         — composite system name (default auto)
     :branch       — logical branch (default :main)
     :store-config — konserve store for the index + any co-located CRDT subs
                     (nil = ephemeral, in-memory index)
     :store-path   — (legacy) directory for a file store
     :sync?        — sync mode (default true)"
  [subs & {:keys [name branch store-config store-path sync?] :or {branch :main sync? true}}]
  (build-composite subs branch name "composite:" "+"
                   (or store-config (when store-path {:backend :file :id (random-uuid) :path store-path}))
                   sync?))

(defn pullback
  "Fiber-product variant of `composite`: requires every sub-system on the SAME
   logical branch (the pullback A ×_S B over the shared branch space). Monoidal:
   pullback(pullback(A,B), C) ≅ pullback(A,B,C). Pass `:branch` to name the
   shared logical branch when native names differ. Same sub-provider interface +
   `async+sync` as `composite`."
  [subs & {:keys [name branch store-config store-path sync?] :or {sync? true}}]
  (let [store-config (or store-config (when store-path {:backend :file :id (random-uuid) :path store-path}))]
    (async+sync sync?
                (async
                 (let [c (await (build-composite subs (or branch :main) name "pullback:" "×" store-config sync?))
                       sys-map (:systems c)]
                   (if branch
                     c
                     (let [branches-list (map p/current-branch (vals sys-map))]
                       (when-not (apply = branches-list)
                         (throw (ex-info "Pullback requires all sub-systems on the same branch (fiber condition violated). Use :branch to name the logical branch when native names differ."
                                         {:branches (into {} (map (fn [[sid s]] [sid (p/current-branch s)])) sys-map)})))
                       (assoc c :current-branch-name (first branches-list)))))))))

;; ============================================================
;; Accessors
;; ============================================================

(defn get-subsystem
  "Get a sub-system by its system-id from a CompositeSystem."
  [composite system-id]
  (get (:systems composite) system-id))
