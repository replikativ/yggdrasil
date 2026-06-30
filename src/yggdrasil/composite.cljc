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

   **Cross-platform (`async+sync`).** The composite does NOT carry a sync-mode —
   each op takes an OPTIONAL trailing `opts` ({:sync?}, default `c/default-opts`).
   The history index is an in-memory `{snap-id → entry}` map, so
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
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            #?(:clj [yggdrasil.fressian :as yf])
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
;; written LAST on every persist — the transactional gate. The SUBS manifest
;; lists each co-located sub's roots/freed cells so konserve-sync's composite
;; walker can ship the whole store with :composite/root keyword-last.
(def ^:private composite-root-key :composite/root)
(def ^:private composite-freed-key :composite/freed)
(def ^:private composite-subs-key :composite/subs)

(defn- colocated-subs-manifest
  "The {:roots-key :freed-key} of every sub that co-habits the composite's
   `kv-store` (a durable CRDT opened on it) — for the konserve-sync walker.
   Non-co-located subs (own backend — datahike/git) sync via their own stores."
  [kv-store sys-map]
  (->> (vals sys-map)
       (keep (fn [s]
               (when (and kv-store
                          (identical? kv-store (:kv-store s))
                          (get-in s [:config :roots-key]))
                 {:roots-key (get-in s [:config :roots-key])
                  :freed-key (get-in s [:config :freed-key])})))
       vec))

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

(defn- colocated?
  "Whether `sys` co-habits `kv-store` as a durable CRDT (its own roots cell)."
  [kv-store sys]
  (boolean (and kv-store (identical? kv-store (:kv-store sys)) (get-in sys [:config :roots-key]))))

(defn- unified-gc!
  "Shared-store composite GC: flush the index + every co-located sub, then ONE
   mark-and-sweep over the UNION of the composite index root and every sub root,
   sparing all pointer cells (per-sub [:crdt/roots id]/[:crdt/freed id] + the
   manifest). A per-sub gc! would delete siblings (unreachable from one sub's
   roots), so the composite must sweep as a whole. (async+sync) Returns the
   reclamation report {:deleted <set-of-keys>}."
  [kv-store storage index-atom systems opts]
  (let [opts (t/async-gc-opts "composite/unified-gc!" opts)]
    (async+sync (:sync? opts)
                (async
               ;; persist the latest state we mean to keep
                 (loop [ss (seq systems)]
                   (when ss
                     (when (satisfies? p/Committable (val (first ss)))
                     ;; thread the async-only gc opts (`:sync? false`) so the sub's
                     ;; commit!/flush! runs async and returns an `await`-able CPS
                     ;; (the no-opts arity defaults to c/default-opts = :sync? true
                     ;; on the JVM → a plain value that can't be awaited here).
                       (await (p/commit! (val (first ss)) nil opts)))
                     (recur (next ss))))
                 (await (persist-index! kv-store storage index-atom opts))
                 (let [manifest  (colocated-subs-manifest kv-store systems)
                       comp-root (await (kb/k-get kv-store composite-root-key opts))
                       sub-roots (loop [ss (seq systems) acc []]
                                   (if ss
                                     (let [s (val (first ss))]
                                       (recur (next ss)
                                              (into acc (vals (await (d/load-roots (:kv-store s) (:config s) opts))))))
                                     acc))
                       roots (filterv some? (cons comp-root sub-roots))
                       spare (into #{composite-subs-key}
                                   (mapcat (fn [m] [(:roots-key m) (:freed-key m)]))
                                   manifest)
                       deleted (await (d/gc! kv-store roots
                                             {:roots-key composite-root-key
                                              :freed-key composite-freed-key}
                                             (-> opts
                                                 (assoc :spare-keys spare))))]
                   {:deleted deleted})))))

;; ============================================================
;; CompositeOverlay — the composite's isolated workspace
;; ============================================================
;; A composite overlay is a MAP of per-sub overlays (not a single clone), so its
;; methods fan out. `merge-down!` merges each sub-overlay down and assoc's the
;; results back into the PARENT composite — preserving the parent's store + index
;; (the convergent composite `-join` rebuilds an ephemeral composite, which would
;; drop them, so we don't use it here).

;; `mode` is the REQUESTED mode; each sub negotiates its own (a CRDT sub grants
;; :following, a datahike/git sub degrades to :frozen) — see each sub-overlay's
;; `:mode`. So a composite overlay is honestly mixed-mode.
(defrecord CompositeOverlay [parent sub-overlays mode]
  p/Overlayable
  (base-ref [_] nil)
  (peek-parent [ov] (:parent ov)) (peek-parent [ov _] (:parent ov))
  (overlay-writes [ov] (:sub-overlays ov))

  (advance! [ov] (p/advance! ov c/default-opts))
  (advance! [ov opts]
    (async+sync (:sync? opts)
                (async
                 (loop [ss (seq sub-overlays)]
                   (when ss (await (p/advance! (val (first ss)))) (recur (next ss))))
                 ov)))

  ;; merge each sub-overlay back into the parent's matching sub, keeping the
  ;; parent composite's store/index. (CRDT sub-overlays -join, never fail; the
  ;; staged-with-rollback compose helper matters once VERSIONED subs join in.)
  (merge-down! [ov] (p/merge-down! ov c/default-opts))
  (merge-down! [_ opts]
    (async+sync (:sync? opts)
                (async
                 (let [merged (loop [ss (seq sub-overlays) acc (:systems parent)]
                                (if ss
                                  (let [[sid sub-ov] (first ss)]
                                    (recur (next ss) (assoc acc sid (await (p/merge-down! sub-ov)))))
                                  acc))]
                   (assoc parent :systems merged)))))

  (discard! [ov] (p/discard! ov c/default-opts))
  (discard! [_ _]
    (doseq [[_ sub-ov] sub-overlays] (p/discard! sub-ov))
    nil))

(defn overlay-subsystem
  "The writable clone VALUE for sub-system `sid` inside a CompositeOverlay — read
   it with that sub's normal ops. Mutate via `overlay-subsystem-swap!`."
  [composite-overlay sid]
  (ovl/overlay-system (get (:sub-overlays composite-overlay) sid)))

(defn overlay-subsystem-swap!
  "Apply value-semantic mutator `f` to sub-system `sid`'s overlay clone and
   re-seat it (the sub-overlay is a conn over `:local-writes`). Returns the
   composite-overlay. (async+sync)"
  [composite-overlay sid f]
  (async+sync (:sync? c/default-opts)
              (async
               (await (ovl/overlay-swap! (get (:sub-overlays composite-overlay) sid) f))
               composite-overlay)))

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
            storage]             ;; shared content-addressed KonserveStorage (nil ephemeral)

  p/SystemIdentity
  (system-id [_] composite-name)
  (system-type [_] :composite)
  (capabilities [_] (intersect-capabilities systems))

  p/Snapshotable
  (snapshot-id [_] (composite-snapshot-id systems c/default-opts))

  (parent-ids [_]
    (async+sync (:sync? c/default-opts)
                (async
                 (let [snap (await (composite-snapshot-id systems c/default-opts))
                       entry (lookup-entry index-atom snap)]
                   (or (:parent-ids entry) #{})))))

  (as-of [this snap-id] (p/as-of this snap-id c/default-opts))
  (as-of [_ snap-id opts]
    (let [opts (merge c/default-opts opts)]   ; opts may be a DOMAIN map without :sync?
      (async+sync (:sync? opts)
                  (async
                   (if-let [sub-snaps (resolve-sub-snapshots index-atom snap-id)]
                     (loop [ss (seq systems) acc {}]
                       (if ss
                         (let [[sid sys] (first ss)]
                           (recur (next ss) (assoc acc sid (await (p/as-of sys (get sub-snaps sid))))))
                         acc))
                     nil)))))

  (snapshot-meta [this snap-id] (p/snapshot-meta this snap-id c/default-opts))
  (snapshot-meta [_ snap-id _opts]
    (when-let [entry (lookup-entry index-atom (str snap-id))]
      {:snapshot-id (str snap-id)
       :parent-ids (:parent-ids entry)
       :timestamp (:timestamp entry)
       :message (:message entry)
       :sub-snapshots (:sub-snapshots entry)}))

  p/Branchable
  (branches [this] (p/branches this c/default-opts))
  (branches [_ _opts]
    (if (empty? systems)
      #{}
      (reduce set/intersection
              (map (fn [[_ sys]] (p/branches sys)) systems))))

  (current-branch [_] current-branch-name)

  (branch! [this name]
    (assoc this :systems (reduce-kv (fn [acc sid sys] (assoc acc sid (p/branch! sys name))) {} systems)))
  (branch! [this name from] (p/branch! this name from c/default-opts))
  (branch! [this name from _opts]
    ;; `from` may be a composite SNAPSHOT-ID → branch each sub from ITS recorded
    ;; sub-snapshot (freeze+isolate the whole composite at a fixed version); or a
    ;; branch keyword / per-sub ref, passed through unchanged.
    (let [sub-snaps (resolve-sub-snapshots index-atom from)]
      (assoc this :systems
             (reduce-kv (fn [acc sid sys]
                          (assoc acc sid (p/branch! sys name (if sub-snaps (get sub-snaps sid) from))))
                        {} systems))))

  (delete-branch! [this name] (p/delete-branch! this name c/default-opts))
  (delete-branch! [this name _opts]
    (assoc this :systems (reduce-kv (fn [acc sid sys] (assoc acc sid (p/delete-branch! sys name))) {} systems)))

  (checkout [this name] (p/checkout this name c/default-opts))
  (checkout [this name _opts]
    (assoc this
           :systems (reduce-kv (fn [acc sid sys] (assoc acc sid (p/checkout sys name))) {} systems)
           :current-branch-name (keyword name)))

  p/Committable
  (commit! [this] (p/commit! this nil c/default-opts))
  (commit! [this message] (p/commit! this message c/default-opts))
  (commit! [this message opts]
    (let [opts (merge c/default-opts opts)]
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
                     (assoc this :systems new-systems))))))

  p/Graphable
  (history [this] (p/history this c/default-opts))
  (history [_ hopts]
    (let [hopts (merge c/default-opts hopts)]
      (async+sync (:sync? hopts)
                  (async
                   (let [limit (or (:limit hopts) 100)]
                     (loop [snap-id (await (composite-snapshot-id systems hopts)) result [] visited #{}]
                       (cond
                         (visited snap-id) result
                         (nil? snap-id) result
                         (>= (count result) limit) result
                         :else
                         (let [entry (lookup-entry index-atom snap-id)
                               parents (or (:parent-ids entry) #{})]
                           (recur (first parents) (conj result snap-id) (conj visited snap-id))))))))))

  (ancestors [this snap-id] (p/ancestors this snap-id c/default-opts))
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

  (ancestor? [this a b] (p/ancestor? this a b c/default-opts))
  (ancestor? [this a b _opts]
    (contains? (set (p/ancestors this b)) (str a)))

  (common-ancestor [this a b] (p/common-ancestor this a b c/default-opts))
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

  (commit-graph [this] (p/commit-graph this c/default-opts))
  (commit-graph [_ opts]
    (let [opts (merge c/default-opts opts)]
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
                      :roots (set (keep (fn [e] (when (empty? (or (:parent-ids e) #{})) (:composite-snap-id e))) entries))})))))

  (commit-info [this snap-id] (p/commit-info this snap-id c/default-opts))
  (commit-info [_ snap-id _opts]
    (when-let [entry (lookup-entry index-atom (str snap-id))]
      {:parent-ids (:parent-ids entry)
       :timestamp (:timestamp entry)
       :message (:message entry)
       :sub-snapshots (:sub-snapshots entry)}))

  p/Mergeable
  (merge! [this source] (p/merge! this source c/default-opts))
  (merge! [this source mopts]
    ;; TRANSACTIONAL: merge every sub-system, then commit! (flush each sub durable,
    ;; then write :composite/root LAST). A crash mid-merge leaves the previous
    ;; committed composite as latest; a reader never sees a half-merge. `mopts` is a
    ;; DOMAIN merge map (e.g. {:message}) that may omit :sync? — fill the platform
    ;; default so the mode threads to the sub-merges + commit!.
    (let [mopts (merge c/default-opts mopts)]
      (async+sync (:sync? mopts)
                  (async
                   (let [new-systems (loop [ss (seq systems) acc {}]
                                       (if ss
                                         (let [[sid sys] (first ss)]
                                           (recur (next ss) (assoc acc sid (await (p/merge! sys source mopts)))))
                                         acc))]
                     (await (p/commit! (assoc this :systems new-systems)
                                       (or (and (map? mopts) (:message mopts)) "merge")
                                       mopts)))))))

  (conflicts [this a b] (p/conflicts this a b c/default-opts))
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

  (diff [this a b] (p/diff this a b c/default-opts))
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
    (async+sync (:sync? c/default-opts)
                (async
                 (if (empty? systems)
                   #{}
                   (loop [ss (seq systems) acc #{}]
                     (if ss
                       (recur (next ss) (set/union acc (await (p/gc-roots (val (first ss))))))
                       acc))))))

  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [_ snapshot-ids gopts]
    ;; gopts = GC window. GC is async-only — coerce to `:sync? false` (throws on
    ;; explicit `:sync? true`) for OUR dispatch + the unified path; forward the coerced
    ;; gopts to sub gc-sweep!s (each self-normalizes / re-coerces idempotently).
    (let [gopts (t/async-gc-opts "composite/gc-sweep!" gopts)]
      (async+sync (:sync? (merge c/default-opts gopts))
                  (async
                   (if (and kv-store (seq systems)
                            (every? (fn [[_ sys]] (colocated? kv-store sys)) systems))
                   ;; SHARED store → ONE unified mark-and-sweep over the union of roots
                   ;; (a per-sub sweep would delete siblings/index nodes).
                     (await (unified-gc! kv-store storage index-atom systems (merge c/default-opts gopts)))
                   ;; SEPARATE stores → each sub owns its store, safe to fan out.
                     (loop [ss (seq systems) acc {}]
                       (if ss
                         (let [[sid sys] (first ss)]
                           (recur (next ss) (assoc acc sid (await (p/gc-sweep! sys snapshot-ids gopts)))))
                         acc)))))))

  p/Overlayable
  ;; overlay every sub-system → a CompositeOverlay (a map of sub-overlays). The
  ;; composite advertises `:overlayable` only when EVERY sub is (intersection),
  ;; so this fans out cleanly. Mutate via `overlay-subsystem`; `merge-down!`
  ;; joins every sub back into the parent composite (store + index preserved).
  (overlay [this o]
    (->CompositeOverlay this
                        (reduce-kv (fn [acc sid sys] (assoc acc sid (p/overlay sys o))) {} systems)
                        (or (:mode o) :frozen))))

;; ============================================================
;; Lifecycle
;; ============================================================

(defn flush!
  "Persist the composite history index (rebuild PSS + write :composite/root last).
   No-op when ephemeral. (async+sync)"
  ([composite] (flush! composite c/default-opts))
  ([composite opts]
   (persist-index! (:kv-store composite) (:storage composite) (:index-atom composite) opts)))

(defn close!
  "Flush and close the composite's store. Safe on ephemeral composites. (async+sync)"
  ([composite] (close! composite c/default-opts))
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
   non-fn element, so this is only needed to be explicit.

   Returns the system wrapped in `async+sync` so `open-subs` can `await` it
   UNIFORMLY with opener results (which are genuinely async on cljs). Without
   this, `(await system)` on cljs awaits a plain record — not a CPS — and throws
   (the opener path returns a CPS, so it worked; the adopt path didn't)."
  [system] (fn [_kv-store opts] (async+sync (:sync? opts) (async system))))

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
                                                    (await (d/open store-config {:freed-key composite-freed-key} opts))
                                                    {:kv-store nil :storage nil})
                       sys-map (await (open-subs subs kv-store opts))
                       composite-name (or name (str prefix (str/join sep (sort (keys sys-map)))))
                       index-atom (atom (await (load-index kv-store storage opts)))
                       sys (->CompositeSystem sys-map branch composite-name
                                              index-atom kv-store store-config storage)
                       snap (await (composite-snapshot-id sys-map opts))]
                   ;; manifest of co-located subs (for the konserve-sync composite walker)
                   (when kv-store
                     (await (kb/k-assoc kv-store composite-subs-key
                                        (colocated-subs-manifest kv-store sys-map) opts)))
                   (await (register-initial-snapshot! index-atom kv-store storage sys-map snap opts))
                   sys)))))

(defn composite
  "Coordinate N sub-systems as one transactional unit (no same-branch
   constraint — datahike `:db`, scriptum `:main` may differ). `subs` is a seq of
   sub-providers (built systems and/or openers — see the section comment).
   (async+sync — `:sync? false` on cljs and `await` the result.)

   DEFAULT-BRANCH CAVEAT: adapters disagree on the default branch — datahike has NO
   `:main` (its branch is the conn's, conventionally `:db`); git/CRDT/composite default
   `:main`. `composite`'s `:branch` names the COMPOSITE's logical branch and fans out to
   subs on `checkout`, but on construction each sub stays on its own native branch. Do
   not assume a sub is on `:main`. `pullback` (the fiber-product variant) THROWS if subs
   are on different native branches unless you pass `:branch` to name the shared one.

   opts:
     :name         — composite system name (default auto)
     :branch       — logical branch (default :main)
     :store-config — konserve store for the index + any co-located CRDT subs
                     (nil = ephemeral, in-memory index)
     :store-path   — (legacy) directory for a file store
     :sync?        — sync mode (default true)"
  ([subs] (composite subs {} {:sync? true}))
  ([subs config] (composite subs config {:sync? true}))
  ([subs {:keys [name branch store-config store-path] :or {branch :main}}
    {:keys [sync?] :or {sync? true}}]
   (build-composite subs branch name "composite:" "+"
                    (or store-config (when store-path {:backend :file :id (random-uuid) :path store-path}))
                    sync?)))

(defn pullback
  "Fiber-product variant of `composite`: requires every sub-system on the SAME
   logical branch (the pullback A ×_S B over the shared branch space). Monoidal:
   pullback(pullback(A,B), C) ≅ pullback(A,B,C). Pass `:branch` to name the
   shared logical branch when native names differ. Same sub-provider interface +
   `async+sync` as `composite`."
  ([subs] (pullback subs {} {:sync? true}))
  ([subs config] (pullback subs config {:sync? true}))
  ([subs {:keys [name branch store-config store-path]}
    {:keys [sync?] :or {sync? true}}]
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
                        (assoc c :current-branch-name (first branches-list))))))))))

;; ============================================================
;; Accessors
;; ============================================================

(defn get-subsystem
  "Get a sub-system VALUE by its system-id from a CompositeSystem."
  [composite system-id]
  (get (:systems composite) system-id))

(defn update-subsystem
  "Apply value-semantic mutator `f` (sub-system → new sub-system, possibly async)
   to sub-system `id` and RE-SEAT the result. Returns a NEW composite. (async+sync)
   The value-semantic way to evolve a sub inside a composite — a `get-subsystem`
   handle is an immutable value, so mutating it in place is a no-op."
  [composite id f]
  (async+sync (:sync? c/default-opts)
              (async
               (assoc-in composite [:systems id]
                         (await (f (get (:systems composite) id)))))))

;; Register the composite with the system value codec (JVM). COMPOSITIONAL: project
;; only the WRAPPER — the `:systems` map's child records ride the SAME `ygg/system`
;; handler (fressian recurses on writeObject), so each child is serialized + reopened
;; by its own project/reconstruct automatically; we just place the map and rebuild
;; the wrapper. Children are co-located on the composite's store, so they reconstruct
;; against the same `resolve-storage`. (index-atom = fresh in-memory history.)
#?(:clj
   (yf/register-system!
    :composite CompositeSystem
    (fn [{:keys [systems current-branch-name composite-name store-config]}]
      {:composite-name composite-name :current-branch-name current-branch-name
       :store-config store-config :systems systems})
    (fn [blob storage opts]
      (->CompositeSystem (:systems blob) (:current-branch-name blob) (:composite-name blob)
                         (atom {}) (when storage (:kv-store storage)) (:store-config blob)
                         storage))))

;; ============================================================
;; CompositeSystem as a CONVERGENT system (peer-mergeable)
;; ============================================================
;; Make a CompositeSystem itself conflict-free-mergeable: joining two PEER composites
;; fans `-join` out to matching sub-systems — convergent subs `-join` (symmetric, no
;; ancestor), versioned (datahike/git) subs 3-way `merge!` on the shared branch.
;; "Merge peers" with NO new interface: merging two contexts IS merging their two
;; workspace composites via the existing `PConvergent`/`-join`. (Lives HERE, with the
;; CompositeSystem defrecord, so the extension is ALWAYS present in production — a
;; separate ns would only be loaded if a consumer happened to require it.)
;;
;; The composite is a STATE container with NO op-δ: a δ is the change a LOCAL mutation
;; made, but a composite isn't mutated directly — you mutate a SUB (which returns a new
;; δ-carrying record the composite doesn't capture). The OP path is therefore per-LEAF
;; (sync each sub as its own ygg-signal); forcing an aggregate δ onto the composite
;; would be a duplication against the grain.
(extend-type CompositeSystem
  c/PConvergent
  (-join
    ([this other] (c/-join this other c/default-opts))
    ([this other opts]
     (let [others (:systems other)
           joined (reduce
                   (fn [m [id sys]]
                     (let [o (get others id)]
                       (assoc m id
                              (cond
                                (nil? o) sys                          ; system only on `this`
                                (c/convergent? sys) (c/-join sys o opts) ; CRDT: symmetric 2-way join
                               ;; versioned (datahike/git): 3-way merge — merge `other`'s
                               ;; branch into `this`'s on the shared store; conflicts surface
                               ;; via the composite's `conflicts` (the merge-review seam), not
                               ;; auto-resolved here. Needs a resolvable common ancestor.
                                (satisfies? p/Mergeable sys)
                                (-> sys
                                    (p/checkout (p/current-branch sys))
                                    (p/merge! (p/current-branch o) opts))
                                :else sys))))
                   {} (:systems this))]
       ;; IDEMPOTENCE: when every sub-join changed nothing (each returns the SAME sub),
       ;; the composite is unchanged → return `this` identical, so a composite-valued
       ;; signal doesn't re-fire / re-publish on a no-op.
       (if (= joined (:systems this))
         this
         (composite (vec (vals joined))
                    {:name (:composite-name this)
                     :branch (:current-branch-name this)}
                    opts)))))
  (-conflict-free? [this]
    (every? c/convergent? (vals (:systems this)))))
