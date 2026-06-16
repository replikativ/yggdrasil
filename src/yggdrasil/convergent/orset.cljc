(ns yggdrasil.convergent.orset
  "Observed-Remove Set (OR-Set) as a DURABLE conflict-free yggdrasil system —
   the registry's true shape (add AND convergent remove), where a grow-only
   G-Set is too restrictive.

   An OR-Set is TWO grow-only sets of `[element tag]` pairs:
     adds      every observed addition (element + a unique tag)
     removals  tombstones for the add-tags a remove OBSERVED
   live(element) ⇔ ∃ tag: [element tag] ∈ adds ∧ [element tag] ∉ removals.
   So a concurrent add (fresh tag) survives a remove that didn't observe it —
   **add-wins**, and removal converges (unlike a G-Set's union, which resurrects).

   Both halves are content-addressed durable PSS sets
   (`yggdrasil.convergent.durable`), stored as two branches `:adds`/`:removals`
   in ONE konserve store under the `:crdt/roots` cell — so konserve-sync's crdt
   walker (which walks every root in `:crdt/roots`) syncs both unchanged.

   `tag-fn` controls the add tag: the default `(random-uuid)` gives a true
   OR-Set (re-adding an element makes a distinct live tag); pass a content-hash
   `tag-fn` for an idempotent add (re-adding the same element is a no-op) — the
   shape the snapshot registry wants.

   VALUE SEMANTICS: the record holds the two halves (`adds`/`removals`, immutable
   PSS-set values) and a `dirty` flag as PLAIN fields; every mutator returns a NEW
   record (never mutates in place). The mutable cell lives in the HOLDER — a
   spindel signal-atom or the registry conn — which swaps this value.

   Implements `PConvergent` (-join) + `Overlayable`. **Fully cross-platform**: the
   record carries its sync-mode (`opts`), so EVERY content-touching op — the
   functional API AND the protocol methods (-join/snapshot-id/as-of/gc-sweep!) —
   dispatches through `async+sync` (sync on JVM, async/CPS on cljs)."
  (:refer-clojure :exclude [conj disj contains?])
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

(declare ->ORSet flush! apply-delta)

(defn- half-accrue
  "Accrue δ half-maps {:adds #{[e tag]..} :removals #{[e tag]..}} by union."
  [a b]
  (merge-with into a b))

(defrecord ORSet
           [id kv-store store-config storage comparator tag-fn
            adds        ; immutable PSS set of [element tag]
            removals    ; immutable PSS set of [element tag]
            dirty       ; boolean — content changed since last flush
            opts]       ; the record's sync-mode

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :orset)
  ;; single logical branch (replicas are separate stores synced by konserve-sync):
  ;; isolation is via Overlayable, peer-merge via -join (PConvergent). branch!/
  ;; merge! (the hierarchical tier) are no-ops here, so we do NOT advertise them.
  (capabilities [_] {:snapshotable true :branchable false :mergeable false
                     :garbage-collectable true :overlayable true
                     :graphable false})

  p/Snapshotable
  ;; addressable snapshot = a content-addressed COMMIT {:adds :removals} over the
  ;; two halves' roots (shared two-half machinery); `as-of` projects the FROZEN
  ;; live elements (add-wins: adds − removals, by first).
  (snapshot-id [this] (d/two-half-snapshot-id this opts))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [this snap-id _opts]
    (async+sync (:sync? opts)
                (async
                 (let [[a r] (await (d/two-half-restore-halves this snap-id opts))]
                   (into #{} (map first) (set/difference a r))))))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  ;; OR-Set replicas are separate stores synced by konserve-sync; the system
  ;; itself has one logical branch. branch! returns this (no-op replica handle).
  (branches [_] #{:main})
  (branches [_ _] #{:main})
  (current-branch [_] :main)
  (branch! [this _] this) (branch! [this _ _] this) (branch! [this _ _ _] this)
  (delete-branch! [this _] this) (delete-branch! [this _ _] this)
  (checkout [this _] this) (checkout [this _ _] this)

  p/Mergeable
  (merge! [this _] this)
  (merge! [this _ _] this)
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  p/Committable
  ;; "commit" = make current state durable (flush); identity is content-addressed.
  (commit! [this] (flush! this))
  (commit! [this _message] (flush! this))
  (commit! [this _message _opts] (flush! this))

  c/PConvergent
  ;; PURE same-store join: union both grow-only halves with the peer (shared). (async+sync)
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
  ;; :following → empty delta halves; `overlay-value` joins with the LIVE parent
  ;; on read (add-wins convergent). Mutate the clone via `ovl/overlay-swap!`.
  (overlay [this opts]
    (let [mode (or (:mode opts) :frozen)
          lw   (if (= :following mode)
                 (assoc this :adds (d/empty-set storage comparator)
                        :removals (d/empty-set storage comparator) :dirty false)
                 (assoc this :dirty false))]
      (ovl/convergent-overlay this mode lw))))

;; ============================================================
;; Value ops — each returns a NEW OR-Set value (value-semantic)
;; ============================================================

(defn conj
  "Add `elem` with a fresh tag (from `tag-fn`). (async+sync) Returns a NEW o."
  ([o elem] (conj o elem (:opts o)))
  ([o elem opts]
   (async+sync (:sync? opts)
               (async
                (let [pair [elem ((:tag-fn o) elem)]
                      s'   (await (d/set-conj (:adds o) pair (:comparator o) opts))]
                  (c/with-delta (assoc o :adds s' :dirty true) half-accrue {:adds #{pair}}))))))

(defn- live-pairs
  "async+sync: the live [elem tag] add-pairs (in adds, not removals)."
  [o opts]
  (async+sync (:sync? opts)
              (async
               (set/difference (await (d/set->clj (:adds o) opts))
                               (await (d/set->clj (:removals o) opts))))))

(defn disj
  "Observed-remove `elem`: tombstone exactly the add-tags currently observed for
   it. A concurrent (unobserved) add survives — add-wins. (async+sync) Returns a
   NEW o."
  ([o elem] (disj o elem (:opts o)))
  ([o elem opts]
   (async+sync (:sync? opts)
               (async
                (let [tombstones (filter (fn [[e _]] (= e elem)) (await (live-pairs o opts)))
                      removals'  (loop [r (:removals o) ts (seq tombstones)]
                                   (if ts
                                     (recur (await (d/set-conj r (first ts) (:comparator o) opts)) (next ts))
                                     r))]
                  (c/with-delta (assoc o :removals removals' :dirty true)
                    half-accrue {:removals (set tombstones)}))))))

(defn apply-delta
  "Consume a peer's OR-Set δ ({:adds #{[e tag]..} :removals #{[e tag]..}}) by
   unioning each half of [elem tag] pairs into the local halves — the OP-path
   apply (O(δ); cf. -join the STATE-path). Returns a NEW o. (async+sync)"
  [o delta]
  (let [opts (:opts o)]
    (async+sync (:sync? opts)
                (async
                 (let [adds (loop [a (:adds o) ps (seq (:adds delta))]
                              (if ps (recur (await (d/set-conj a (first ps) (:comparator o) opts)) (next ps)) a))
                       rems (loop [r (:removals o) ps (seq (:removals delta))]
                              (if ps (recur (await (d/set-conj r (first ps) (:comparator o) opts)) (next ps)) r))]
                   ;; clear δ: a remote-integrated value re-propagates nothing.
                   (c/clear-delta (assoc o :adds adds :removals rems :dirty true)))))))

(defn elements
  "Live elements: those with ≥1 add-tag not tombstoned. (async+sync)"
  ([o] (elements o (:opts o)))
  ([o opts]
   (async+sync (:sync? opts)
               (async (into #{} (map first) (await (live-pairs o opts)))))))

(defn contains?
  "Whether `elem` is live. (async+sync)"
  ([o elem] (contains? o elem (:opts o)))
  ([o elem opts]
   (async+sync (:sync? opts)
               (async (clojure.core/contains? (await (elements o opts)) elem)))))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves + the :crdt/roots cell + freed; clear `dirty`. (async+sync)"
  ([o] (d/two-half-flush! o (:opts o)))
  ([o opts] (d/two-half-flush! o opts)))

(defn merge-peer!
  "Cross-store -join: ship the peer's nodes into this store and union both halves.
   Returns a NEW o. (async+sync)"
  [o other] (d/two-half-merge-peer! o other (:opts o)))

(defn gc!
  "Reclaim PSS nodes superseded by prior flushes (mark-and-sweep). Returns the
   set of deleted node keys."
  ([o] (p/gc-sweep! o nil nil))
  ([o opts] (p/gc-sweep! o nil opts)))

;; ============================================================
;; Factory
;; ============================================================

(defn orset
  "Open (or create) a durable OR-Set on a per-system konserve store.

     (orset \"reg\" :store-config {:backend :memory :id (random-uuid)})

   :tag-fn  element -> tag (default: ignore element, fresh random-uuid → true
            OR-Set). Pass a content-hash fn for idempotent add (registry shape).
   Restores both halves from the store's :crdt/roots cell when present."
  [id & {:keys [comparator tag-fn sync? store-config kv-store roots-key freed-key]
         :or {comparator compare tag-fn (fn [_] (random-uuid)) sync? true}}]
  (async+sync sync?
              (async
               (let [{:keys [kv-store storage store-config opts adds removals]}
                     (await (d/two-half-open store-config
                                             {:comparator comparator :sync? sync? :kv-store kv-store
                                              :roots-key roots-key :freed-key freed-key}))]
                 (->ORSet id kv-store store-config storage comparator tag-fn adds removals false opts)))))
