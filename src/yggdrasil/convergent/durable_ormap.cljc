(ns yggdrasil.convergent.durable-ormap
  "Observed-Remove Map (OR-Map) — and its merging variant — as a DURABLE
   conflict-free yggdrasil system, on the SAME PSS+konserve substrate as the
   durable sets.

   FLATTENED LAYOUT: the nested `{k {uid v}}` of the in-memory `ormap` is flattened
   into a grow-only PSS set of `[k uid v]` TRIPLES — so a durable OR-Map is
   structurally a `durable-orset` of triples (two halves `:adds`/`:removals` under
   `:crdt/roots`, add-wins, `set-union` join). The ONLY difference is the read
   projection: group live triples by `k`.

   ONE record serves both maps, distinguished by `merge-fn`:
   - merge-fn = nil  → plain OR-Map: `get k` returns the live value-SET (multi-value
     under concurrent writers; one value in the common case).
   - merge-fn ≠ nil  → Merging-OR-Map: `get k` FOLDS the live values through merge-fn
     to ONE value. merge-fn MUST be commutative/associative/idempotent (a lattice
     lub — LWW-by-ts, max, set-union, …); it is wrapped to absorb nils.

   `assoc` ALWAYS mints a FRESH uid (so re-assoc accumulates, like the plain OR-Map);
   the merging variant folds those concurrent values at READ, which is value-
   equivalent to replikativ's in-place uid-reuse but works in a grow-only PSS (you
   cannot supersede a value at a fixed uid in an append-only set). Triples are
   ordered by their uid (a globally-unique uuid ⇒ a total order over distinct
   entries, and any k / value type is storable since the comparator never touches
   them). Liveness is keyed by uid: an add is live iff its uid is not tombstoned.

   **Fully cross-platform** via `async+sync` (sync on JVM, CPS on cljs), exactly
   like the durable sets it is built on."
  (:refer-clojure :exclude [assoc dissoc get keys])
  (:require [clojure.set :as set]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->DurableORMap flush! apply-delta)

(def ^:private adds-branch :adds)
(def ^:private removals-branch :removals)

(defn uid-compare
  "Order `[k uid v]` triples by their uid (globally-unique uuid ⇒ total order over
   distinct entries; any k/value type storable — the comparator never touches them)."
  [t1 t2]
  (compare (nth t1 1) (nth t2 1)))

(defn- wrap
  "Absorb nils so a merge-fn seeds the read-time reduce."
  [merge-fn]
  (when merge-fn (fn [a b] (cond (nil? a) b (nil? b) a :else (merge-fn a b)))))

(defn- half-accrue [a b] (merge-with into a b))

(defn- live-triples
  "async+sync: the live `[k uid v]` triples — in adds, uid not tombstoned in removals."
  [o opts]
  (async+sync (:sync? opts)
              (async
               (let [adds (await (d/set->clj (:adds o) opts))
                     rems (await (d/set->clj (:removals o) opts))
                     rm-uids (into #{} (map #(nth % 1)) rems)]
                 (remove (fn [t] (rm-uids (nth t 1))) adds)))))

;; ============================================================
;; Record
;; ============================================================

(defrecord DurableORMap
           [id kv-store store-config storage comparator merge-fn
            adds        ; immutable PSS set of [k uid v]
            removals    ; immutable PSS set of [k uid v]
            dirty
            opts]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] (if merge-fn :merging-ormap :ormap))
  (capabilities [_] {:snapshotable true :branchable false :mergeable false
                     :garbage-collectable true :overlayable true :graphable false})

  p/Snapshotable
  ;; snapshot = content-addressed commit {:adds <root> :removals <root>} (like the OR-Set)
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async
                 (let [a (await (d/store-set! adds storage opts))
                       r (await (d/store-set! removals storage opts))]
                   (str (await (d/store-commit! kv-store {:adds a :removals r} opts)))))))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; restore the FROZEN map value {k value-or-set} at that commit
    (async+sync (:sync? opts)
                (async
                 (let [commit (await (d/read-commit kv-store (parse-uuid (str snap-id)) opts))
                       adds*  (await (d/set->clj (d/restore-set comparator (:adds commit) storage opts) opts))
                       rems*  (await (d/set->clj (d/restore-set comparator (:removals commit) storage opts) opts))
                       rm-uids (into #{} (map #(nth % 1)) rems*)
                       live   (remove (fn [t] (rm-uids (nth t 1))) adds*)]
                   (reduce (fn [m [k _ v]]
                             (if merge-fn
                               (update m k merge-fn v)
                               (update m k (fnil conj #{}) v)))
                           {} live)))))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  (branches [_] #{:main}) (branches [_ _] #{:main})
  (current-branch [_] :main)
  (branch! [this _] this) (branch! [this _ _] this) (branch! [this _ _ _] this)
  (delete-branch! [this _] this) (delete-branch! [this _ _] this)
  (checkout [this _] this) (checkout [this _ _] this)

  p/Mergeable
  (merge! [this _] this) (merge! [this _ _] this)
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  p/Committable
  (commit! [this] (flush! this))
  (commit! [this _message] (flush! this))
  (commit! [this _message _opts] (flush! this))

  c/PConvergent
  ;; PURE same-store join: union both grow-only halves with the peer. (async+sync)
  (-join [this other]
    (async+sync (:sync? opts)
                (async
                 (let [adds' (await (d/set-union adds (:adds other) comparator opts))
                       rems' (await (d/set-union removals (:removals other) comparator opts))]
                   (if (and (= adds' adds) (= rems' removals))
                     this
                     (->DurableORMap id kv-store store-config storage comparator merge-fn
                                     adds' rems' true opts))))))
  (-conflict-free? [_] true)

  c/PDeltaApply
  (-apply-delta [this delta] (apply-delta this delta))

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids gc-opts]
    (async+sync (:sync? opts)
                (async
                 (await (flush! this))
                 (let [commit-addrs (map #(parse-uuid (str %)) snapshot-ids)
                       retain-roots (loop [cs (seq commit-addrs) acc []]
                                      (if cs
                                        (let [c (await (d/read-commit kv-store (first cs) opts))]
                                          (recur (next cs) (clojure.core/conj acc (:adds c) (:removals c))))
                                        acc))]
                   (await (d/gc! kv-store (vals (await (d/load-roots kv-store opts)))
                                 (merge gc-opts opts {:retain-roots retain-roots
                                                      :retain-keys commit-addrs})))))))

  p/Overlayable
  (overlay [this opts]
    (let [mode (or (:mode opts) :frozen)
          lw   (if (= :following mode)
                 (clojure.core/assoc this :adds (d/empty-set storage comparator)
                                     :removals (d/empty-set storage comparator) :dirty false)
                 (clojure.core/assoc this :dirty false))]
      (ovl/convergent-overlay this mode lw))))

;; ============================================================
;; Value ops — each returns a NEW record (value-semantic)
;; ============================================================

(defn assoc
  "Assoc `v` under `k` with a FRESH uid (local op). Re-assoc accumulates (plain
   OR-Map multi-value); the merging variant folds at read. (async+sync) NEW o."
  [o k v]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [triple [k (random-uuid) v]
                       s'     (await (d/set-conj (:adds o) triple (:comparator o) opts))]
                   (c/with-delta (clojure.core/assoc o :adds s' :dirty true)
                                 half-accrue {:adds #{triple}}))))))

(defn dissoc
  "Observed-remove `k`: tombstone exactly the triples currently live for it.
   Concurrent (unobserved) adds survive — add-wins. (async+sync) NEW o."
  [o k]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [tombstones (filter #(= k (first %)) (await (live-triples o opts)))
                       removals'  (loop [r (:removals o) ts (seq tombstones)]
                                    (if ts
                                      (recur (await (d/set-conj r (first ts) (:comparator o) opts)) (next ts))
                                      r))]
                   (c/with-delta (clojure.core/assoc o :removals removals' :dirty true)
                                 half-accrue {:removals (set tombstones)}))))))

(defn apply-delta
  "Consume a peer's δ ({:adds #{triples} :removals #{triples}}) by unioning each
   half into the local halves — the OP-path apply (cf. -join the STATE-path).
   Returns a NEW o. (async+sync)"
  [o delta]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [adds (loop [a (:adds o) ps (seq (:adds delta))]
                              (if ps (recur (await (d/set-conj a (first ps) (:comparator o) opts)) (next ps)) a))
                       rems (loop [r (:removals o) ps (seq (:removals delta))]
                              (if ps (recur (await (d/set-conj r (first ps) (:comparator o) opts)) (next ps)) r))]
                   (c/clear-delta (clojure.core/assoc o :adds adds :removals rems :dirty true)))))))

(defn get
  "The value for `k`: a SET of live values (plain OR-Map) or the merge-fn FOLD of
   them (merging variant), or nil if absent. (async+sync)"
  [o k]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [vs (->> (await (live-triples o opts))
                               (filter #(= k (first %)))
                               (map #(nth % 2)))]
                   (when (seq vs)
                     (if-let [mfn (:merge-fn o)]
                       (reduce mfn nil (sort-by hash vs))   ; deterministic fold order
                       (set vs))))))))

(defn keys
  "The set of keys with ≥1 live value. (async+sync)"
  [o]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async (into #{} (map first) (await (live-triples o opts)))))))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves, update :crdt/roots + freed. NEW o, dirty cleared. (async+sync)"
  ([o] (flush! o (:opts o)))
  ([o opts]
   (async+sync (:sync? opts)
               (async
                (if (:dirty o)
                  (let [storage       (:storage o)
                        adds-root     (await (d/store-set! (:adds o) storage opts))
                        removals-root (await (d/store-set! (:removals o) storage opts))]
                    (await (d/save-roots! (:kv-store o)
                                          {adds-branch adds-root removals-branch removals-root} opts))
                    (await (d/save-freed! (:kv-store o) storage opts))
                    (clojure.core/assoc o :dirty false))
                  o)))))

(defn merge-peer!
  "Cross-store -join: ship the peer's nodes into this store and union both halves.
   Returns a NEW o. (async+sync)"
  [o other]
  (let [opts (:opts o) cmp (:comparator o)]
    (async+sync (:sync? opts)
                (async
                 (let [ostorage (:storage other)
                       a-root (await (d/store-set! (:adds other) ostorage opts))
                       r-root (await (d/store-set! (:removals other) ostorage opts))]
                   (await (d/ship! (:kv-store other) (:kv-store o) a-root opts))
                   (await (d/ship! (:kv-store other) (:kv-store o) r-root opts))
                   (let [adds' (await (d/set-union (:adds o) (d/restore-set cmp a-root (:storage o) opts) cmp opts))
                         rems' (await (d/set-union (:removals o) (d/restore-set cmp r-root (:storage o) opts) cmp opts))]
                     (clojure.core/assoc o :adds adds' :removals rems' :dirty true)))))))

(defn gc!
  ([o] (p/gc-sweep! o nil nil))
  ([o opts] (p/gc-sweep! o nil opts)))

;; ============================================================
;; Factories
;; ============================================================

(defn- open-ormap
  [id merge-fn {:keys [store-config comparator sync? kv-store roots-key freed-key]
                :or {comparator uid-compare sync? true}}]
  (let [freed-key (or freed-key (when (vector? roots-key) (clojure.core/assoc roots-key 0 :crdt/freed)))
        opts (cond-> {:sync? sync?}
               kv-store  (clojure.core/assoc :kv-store kv-store)
               roots-key (clojure.core/assoc :roots-key roots-key)
               freed-key (clojure.core/assoc :freed-key freed-key))]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store storage]} (await (d/open store-config opts))
                       roots   (await (d/load-roots kv-store opts))
                       restore (fn [branch] (if-let [root (clojure.core/get roots branch)]
                                              (d/restore-set comparator root storage opts)
                                              (d/empty-set storage comparator)))]
                   (->DurableORMap id kv-store store-config storage comparator (wrap merge-fn)
                                   (restore adds-branch) (restore removals-branch)
                                   false opts))))))

(defn durable-ormap
  "Open (or create) a durable OR-Map (multi-value: `get` returns the live value-set).
     (durable-ormap \"m\" :store-config {:backend :memory :id (random-uuid)})"
  [id & {:as opts}]
  (open-ormap id nil opts))

(defn durable-merging-ormap
  "Open (or create) a durable Merging-OR-Map: concurrent per-key values FOLD via
   `merge-fn` (commutative/associative/idempotent) to a single value on `get`.
     (durable-merging-ormap \"m\" max :store-config {:backend :memory :id (random-uuid)})"
  [id merge-fn & {:as opts}]
  (open-ormap id merge-fn opts))
