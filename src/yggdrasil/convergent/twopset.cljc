(ns yggdrasil.convergent.twopset
  "Two-Phase Set (2P-Set) as a DURABLE conflict-free yggdrasil system.

   TWO grow-only, content-addressed PSS sets of BARE elements —
     adds      every added element        (root under :crdt/roots :adds)
     removals  tombstones for removed els  (… :crdt/roots :removals)
   with live(e) ⇔ e ∈ adds ∧ e ∉ removals. Convergent add+remove where REMOVE
   is permanent: re-adding a removed element keeps it removed (the content-add
   re-enters `adds`, but it is also in `removals`). For re-add-after-remove use
   the OR-Set (`orset`).

   Unlike the OR-Set this stores BARE elements (no `[elem tag]` pairs), so a
   key-level storage codec (`:key-encode`/`:key-decode` — e.g. the registry's
   entry<->map) applies cleanly and there is no doubled storage. Elements may be
   any konserve-native value or, with a codec, records; a `:comparator` orders
   them (default `compare`).

   VALUE SEMANTICS: the record holds the two halves (`adds`/`removals`, immutable
   PSS-set values) and a `dirty` flag as PLAIN fields; every mutator returns a NEW
   record (never mutates in place). The mutable cell lives in the HOLDER — the
   registry conn or a spindel signal-atom — which swaps this value.

   Implements SystemIdentity / Snapshotable / Overlayable / PConvergent — so a
   2P-Set (and the registry, a lens over one) is a first-class yggdrasil system
   that another yggdrasil can itself track, fork, and merge. Content-addressing
   gives it a stable snapshot identity."
  (:refer-clojure :exclude [conj disj contains? into])
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

(declare ->TwoPSet flush! apply-delta)

(defn- half-accrue
  "Accrue δ half-maps {:adds #{..} :removals #{..}} by unioning each half."
  [a b]
  (merge-with clojure.core/into a b))

(defrecord TwoPSet
           [id kv-store store-config storage comparator
            adds         ; immutable PSS set of elements
            removals     ; immutable PSS set of tombstoned elements
            dirty        ; boolean — content changed since last flush
            opts]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :2p-set)
  ;; single logical branch (replicas are separate stores synced by konserve-sync):
  ;; isolation is via Overlayable, peer-merge via -join (PConvergent). branch!/
  ;; merge! (the hierarchical tier) are no-ops here, so we do NOT advertise them —
  ;; else composite cap-aggregation / compliance would treat a no-op as working.
  (capabilities [_] {:snapshotable true :branchable false :mergeable false
                     :garbage-collectable true :overlayable true
                     :graphable false})

  p/Snapshotable
  ;; addressable snapshot = a content-addressed COMMIT {:adds :removals} over the
  ;; two halves' roots (shared two-half machinery); `as-of` projects the FROZEN
  ;; value (adds − removals).
  (snapshot-id [this] (d/two-half-snapshot-id this opts))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [this snap-id _opts]
    (async+sync (:sync? opts)
                (async
                 (let [[a r] (await (d/two-half-restore-halves this snap-id opts))]
                   (set/difference a r)))))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  ;; replicas are separate stores synced by konserve-sync; one logical branch.
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
  ;; "commit" = make current state durable (flush); identity is content-addressed.
  (commit! [this] (flush! this))
  (commit! [this _message] (flush! this))
  (commit! [this _message _opts] (flush! this))

  c/PConvergent
  ;; PURE same-store join: union both halves with the peer (shared). (async+sync)
  (-join [this other] (d/two-half-join this other opts))
  (-conflict-free? [_] true)

  c/PDeltaApply
  (-apply-delta [this delta] (apply-delta this delta))

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this snapshot-ids gc-opts] (d/two-half-gc-sweep! this snapshot-ids gc-opts opts))

  p/Overlayable
  ;; :frozen → carry BOTH halves (immutable PSS values; isolated by value).
  ;; :following → empty delta halves; `ovl/overlay-value` joins with the LIVE
  ;; parent on read. Mutate the clone via `ovl/overlay-swap!`.
  (overlay [this opts]
    (let [mode (or (:mode opts) :frozen)
          lw   (if (= :following mode)
                 (assoc this :adds (d/empty-set storage comparator)
                        :removals (d/empty-set storage comparator) :dirty false)
                 (assoc this :dirty false))]
      (ovl/convergent-overlay this mode lw))))

;; ============================================================
;; Value ops — each returns a NEW 2P-Set value (value-semantic)
;; ============================================================

(defn- conj-into!
  "async+sync: conj `elem` onto the PSS set in field `field` (:adds or :removals)
   of `s`, mark dirty, and record the op as a local δ. Returns a NEW s."
  [s field elem opts]
  (async+sync (:sync? opts)
              (async
               (let [s' (await (d/set-conj (get s field) elem (:comparator s) opts))]
                 (c/with-delta (assoc s field s' :dirty true)
                   half-accrue
                   (if (= field :adds) {:adds #{elem}} {:removals #{elem}}))))))

(defn conj
  "Add `elem`. (async+sync) Returns a NEW s."
  ([s elem] (conj s elem (:opts s)))
  ([s elem opts] (conj-into! s :adds elem opts)))

(defn into
  "Add many elements. (async+sync) Returns a NEW s."
  ([s elems] (into s elems (:opts s)))
  ([s elems opts]
   (async+sync (:sync? opts)
               (async
                (loop [s s es (seq elems)]
                  (if es
                    (recur (await (conj-into! s :adds (first es) opts)) (next es))
                    s))))))

(defn disj
  "Tombstone `elem` (permanent — 2P-Set semantics). (async+sync) Returns a NEW s."
  ([s elem] (disj s elem (:opts s)))
  ([s elem opts] (conj-into! s :removals elem opts)))

(defn apply-delta
  "Consume a peer's 2P-Set δ ({:adds #{..} :removals #{..}}) by unioning each half
   into the local halves — the OP-path apply (O(δ); cf. -join the STATE-path).
   Returns a NEW s. (async+sync)"
  [s delta]
  (let [opts (:opts s)]
    (async+sync (:sync? opts)
                (async
                 (let [adds (loop [a (:adds s) es (seq (:adds delta))]
                              (if es (recur (await (d/set-conj a (first es) (:comparator s) opts)) (next es)) a))
                       rems (loop [r (:removals s) es (seq (:removals delta))]
                              (if es (recur (await (d/set-conj r (first es) (:comparator s) opts)) (next es)) r))]
                   ;; clear δ: a remote-integrated value re-propagates nothing.
                   (c/clear-delta (assoc s :adds adds :removals rems :dirty true)))))))

(defn elements
  "Live elements: adds − removals. (async+sync)"
  ([s] (elements s (:opts s)))
  ([s opts]
   (async+sync (:sync? opts)
               (async (set/difference (await (d/set->clj (:adds s) opts))
                                      (await (d/set->clj (:removals s) opts)))))))

(defn contains?
  "Whether `elem` is live. (async+sync)"
  ([s elem] (contains? s elem (:opts s)))
  ([s elem opts]
   (async+sync (:sync? opts)
               (async (clojure.core/contains? (await (elements s opts)) elem)))))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves + the :crdt/roots cell + freed; clear `dirty`. (async+sync)"
  ([s] (d/two-half-flush! s (:opts s)))
  ([s opts] (d/two-half-flush! s opts)))

(defn merge-peer!
  "Cross-store -join: ship the peer's nodes here and union both halves. (async+sync)"
  [s other] (d/two-half-merge-peer! s other (:opts s)))

(defn gc!
  "Reclaim PSS nodes unreachable from the live adds/removals roots (mark-and-sweep).
   Returns the set of deleted node keys."
  ([s] (p/gc-sweep! s nil nil))
  ([s opts] (p/gc-sweep! s nil opts)))

;; ============================================================
;; Factory
;; ============================================================

(defn twopset
  "Open (or create) a durable 2P-Set on a per-system konserve store.

   :comparator             element order (default compare)
   :key-encode/:key-decode  node-key element codec (default identity; the
                            registry passes its entry<->map codec)
   Restores both halves from the store's :crdt/roots cell when present."
  [id & {:keys [comparator key-encode key-decode sync? store-config kv-store roots-key freed-key]
         :or {comparator compare sync? true}}]
  (async+sync sync?
              (async
               (let [{:keys [kv-store storage store-config opts adds removals]}
                     (await (d/two-half-open store-config
                                             {:comparator comparator :sync? sync? :kv-store kv-store
                                              :roots-key roots-key :freed-key freed-key
                                              :key-encode key-encode :key-decode key-decode}))]
                 (->TwoPSet id kv-store store-config storage comparator adds removals false opts)))))
