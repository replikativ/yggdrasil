(ns yggdrasil.convergent.ormap
  "Observed-Remove Map (OR-Map) — and its merging variant — as a DURABLE
   conflict-free yggdrasil system, on the SAME PSS+konserve substrate as the
   durable sets, READ ON THE FLY (datahike-style): the value is NEVER materialized
   in memory; a key read is a PSS range SLICE (lazy + node-cached), not a full drain.

   FLATTENED LAYOUT: the nested `{k {uid v}}` of the in-memory `ormap` is flattened
   into a grow-only PSS set of `[hk uid k v]` ENTRIES, where `hk = (hasch/uuid k)`.
   Entries are ordered by the DEFAULT `compare` over the 4-vector — which orders by
   `hk` first (so a key's entries are CONTIGUOUS ⇒ range-sliceable), then by `uid`
   (a globally-unique uuid). Because uid is unique, two DISTINCT entries never tie on
   `[hk uid]`, so `compare` never reaches the `k`/`v` positions — hence ANY key and
   value type is storable (the comparator never compares them). A read for key `k`
   slices `[hk MIN-UUID]…[hk MAX-UUID]` — O(log n + matches) nodes, then filters
   stored-`k = k` (hash-collision safety) and diffs the removals slice by uid.

   So a durable OR-Map is structurally a `orset` of entries (two halves
   `:adds`/`:removals` under `:crdt/roots`, add-wins, `set-union` join); the ONLY
   differences are the flattened entry + the SLICE-based per-key projection.

   ONE record serves both maps via `merge-fn`:
   - merge-fn = nil → plain OR-Map: `get k` returns the live value-SET.
   - merge-fn ≠ nil → Merging-OR-Map: `get k` FOLDS the live values via merge-fn
     (wrapped to absorb nils; must be commutative/associative/idempotent).

   `assoc` ALWAYS mints a FRESH uid (re-assoc accumulates, like plain OR-Map); the
   merging variant folds at READ — value-equivalent to replikativ's in-place uid-
   reuse but correct in an append-only PSS. `keys` / `as-of` are full scans (listing
   everything is inherently O(n)); only the per-key `get`/`dissoc` slice.

   **Fully cross-platform** via `async+sync` (sync on JVM, CPS on cljs)."
  (:refer-clojure :exclude [assoc dissoc get keys])
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            [hasch.core :as hasch]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->DurableORMap flush! apply-delta)

(def ^:private adds-branch :adds)
(def ^:private removals-branch :removals)

;; The sort prefix (hk) and uid are stored as STRINGS, not uuids: default `compare`
;; then orders them LEXICALLY — identical on JVM and cljs. (uuid OBJECTS compare by
;; SIGNED 64-bit longs on the JVM — so `ffff…` is NEGATIVE, not the max — but by
;; STRING on cljs: divergent orders + no usable min/max sentinel. Strings dodge it.)
(def ^:private min-str "00000000-0000-0000-0000-000000000000")   ; lexical min uuid-string
(def ^:private max-str "ffffffff-ffff-ffff-ffff-ffffffffffff")   ; lexical max uuid-string
(defn- hk [k] (str (hasch/uuid k)))           ; content-hash sort prefix of key k (string)
;; Slice bounds MUST be the SAME LENGTH as entries — Clojure vector `compare` is
;; LENGTH-FIRST, so a short bound sorts before every full entry (empty slice). The
;; uid sentinel at position 1 always decides (real uid-strings sit strictly between
;; min-str and max-str), so the nil k/v placeholders at 2-3 are never compared.
(defn- key-lo [k] [(hk k) min-str nil nil])
(defn- key-hi [k] [(hk k) max-str nil nil])

(defn- wrap
  "Absorb nils so a merge-fn seeds the read-time reduce."
  [merge-fn]
  (when merge-fn (fn [a b] (cond (nil? a) b (nil? b) a :else (merge-fn a b)))))

(defn- half-accrue [a b] (merge-with into a b))

(defn- live-for-key
  "async+sync: the live `[hk uid k v]` entries for key `k` — a SLICE of its
   contiguous range in both halves (NOT a full drain), removals diffed by uid, and
   filtered to stored-k = k (hash-collision safety)."
  [o k opts]
  (async+sync (:sync? opts)
              (async
               (let [lo (key-lo k) hi (key-hi k)
                     adds (await (d/slice->clj (:adds o) lo hi opts))
                     rems (await (d/slice->clj (:removals o) lo hi opts))
                     rm-uids (into #{} (map #(nth % 1)) rems)]
                 (->> adds
                      (remove (fn [t] (rm-uids (nth t 1))))
                      (filter (fn [t] (= k (nth t 2)))))))))

(defn- all-live
  "async+sync: EVERY live entry (full scan — for `keys`/`as-of`, inherently O(n))."
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
            adds        ; immutable PSS set of [hk uid k v]
            removals    ; immutable PSS set of [hk uid k v]
            dirty
            opts]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] (if merge-fn :merging-ormap :ormap))
  (capabilities [_] {:snapshotable true :branchable false :mergeable false
                     :garbage-collectable true :overlayable true :graphable false})

  p/Snapshotable
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async
                 (let [a (await (d/store-set! adds storage opts))
                       r (await (d/store-set! removals storage opts))]
                   (str (await (d/store-commit! kv-store {:adds a :removals r} opts)))))))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    (async+sync (:sync? opts)
                (async
                 (let [commit (await (d/read-commit kv-store (parse-uuid (str snap-id)) opts))
                       adds*  (await (d/set->clj (d/restore-set comparator (:adds commit) storage opts) opts))
                       rems*  (await (d/set->clj (d/restore-set comparator (:removals commit) storage opts) opts))
                       rm-uids (into #{} (map #(nth % 1)) rems*)
                       live   (remove (fn [t] (rm-uids (nth t 1))) adds*)]
                   (reduce (fn [m [_hk _uid k v]]
                             (if merge-fn (update m k merge-fn v) (update m k (fnil conj #{}) v)))
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
  "Assoc `v` under `k` with a FRESH uid (local op). (async+sync) NEW o."
  [o k v]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [entry [(hk k) (str (random-uuid)) k v]
                       s'    (await (d/set-conj (:adds o) entry (:comparator o) opts))]
                   (c/with-delta (clojure.core/assoc o :adds s' :dirty true)
                     half-accrue {:adds #{entry}}))))))

(defn dissoc
  "Observed-remove `k`: tombstone exactly the entries currently live for it
   (slice-scoped, add-wins). (async+sync) NEW o."
  [o k]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [tombstones (await (live-for-key o k opts))
                       removals'  (loop [r (:removals o) ts (seq tombstones)]
                                    (if ts
                                      (recur (await (d/set-conj r (first ts) (:comparator o) opts)) (next ts))
                                      r))]
                   (c/with-delta (clojure.core/assoc o :removals removals' :dirty true)
                     half-accrue {:removals (set tombstones)}))))))

(defn apply-delta
  "Consume a peer's δ ({:adds #{entries} :removals #{entries}}) — union each half
   into the local halves (OP-path; cf. -join the STATE-path). NEW o. (async+sync)"
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
  "The value for `k`: a SET of live values (plain) or the merge-fn FOLD of them
   (merging), or nil. A range SLICE — reads only `k`'s nodes. (async+sync)"
  [o k]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [vs (map #(nth % 3) (await (live-for-key o k opts)))]
                   (when (seq vs)
                     (if-let [mfn (:merge-fn o)]
                       (reduce mfn nil (sort-by hash vs))
                       (set vs))))))))

(defn keys
  "The set of keys with ≥1 live value (full scan — inherently O(n)). (async+sync)"
  [o]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async (into #{} (map #(nth % 2)) (await (all-live o opts)))))))

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
                :or {comparator compare sync? true}}]
  (let [store-config (or store-config (when-not kv-store (d/mem-store-config)))
        freed-key (or freed-key (when (vector? roots-key) (clojure.core/assoc roots-key 0 :crdt/freed)))
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

(defn ormap
  "Open (or create) a durable OR-Map (multi-value: `get` returns the live value-set)."
  [id & {:as opts}]
  (open-ormap id nil opts))

(defn merging-ormap
  "Open (or create) a durable Merging-OR-Map: concurrent per-key values FOLD via
   `merge-fn` (commutative/associative/idempotent) to a single value on `get`."
  [id merge-fn & {:as opts}]
  (open-ormap id merge-fn opts))
