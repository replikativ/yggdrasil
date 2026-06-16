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

(declare ->DurableORSet flush! apply-delta)

(defn- half-accrue
  "Accrue δ half-maps {:adds #{[e tag]..} :removals #{[e tag]..}} by union."
  [a b]
  (merge-with into a b))

(def ^:private adds-branch :adds)
(def ^:private removals-branch :removals)

(defrecord DurableORSet
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
  ;; addressable snapshot = a content-addressed COMMIT object {:adds :removals}
  ;; over the two halves' PSS roots (re-openable via `as-of`, the freeze handle).
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async
                 (let [adds-root (await (d/store-set! adds storage opts))
                       rem-root  (await (d/store-set! removals storage opts))]
                   (str (await (d/store-commit! kv-store {:adds adds-root :removals rem-root} opts)))))))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; restore the FIXED live elements (add-wins: adds − removals, by first) at
    ;; that commit, not the current state.
    (async+sync (:sync? opts)
                (async
                 (let [commit (await (d/read-commit kv-store (parse-uuid (str snap-id)) opts))]
                   (into #{} (map first)
                         (set/difference
                          (await (d/set->clj (d/restore-set comparator (:adds commit) storage opts) opts))
                          (await (d/set->clj (d/restore-set comparator (:removals commit) storage opts) opts))))))))
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
  ;; PURE same-store join: union both grow-only halves with the peer. (async+sync)
  (-join [this other]
    (async+sync (:sync? opts)
                (async
                 (let [adds' (await (d/set-union adds (:adds other) comparator opts))
                       rems' (await (d/set-union removals (:removals other) comparator opts))]
                   ;; IDEMPOTENCE: a no-op join returns the receiver identical.
                   (if (and (= adds' adds) (= rems' removals))
                     this
                     (->DurableORSet id kv-store store-config storage comparator tag-fn
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
                 ;; retain held snapshots: an OR-Set snapshot-id is a COMMIT addr
                 ;; ({:adds r :removals r}); keep the commit object + both halves'
                 ;; nodes so as-of/frozen on that id survives GC. The cutoff
                 ;; (`:remove-before`/`:grace-period-ms`) rides in `gc-opts`.
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
  "Persist both halves, update the :crdt/roots cell ({:adds :removals}) + freed.
   Returns a NEW o with `dirty` cleared (callers must ADOPT it). (async+sync)"
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
                    (assoc o :dirty false))
                  o)))))

(defn merge-peer!
  "Cross-store -join: ship the peer's adds+removals nodes into this store and
   union both halves. Returns a NEW o. (async+sync)"
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
                     (assoc o :adds adds' :removals rems' :dirty true)))))))

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
  [id & {:keys [store-config comparator tag-fn sync? kv-store roots-key freed-key]
         :or {comparator compare tag-fn (fn [_] (random-uuid)) sync? true}}]
  (let [store-config (or store-config (when-not kv-store (d/mem-store-config)))
        freed-key (or freed-key (when (vector? roots-key) (assoc roots-key 0 :crdt/freed)))
        opts (cond-> {:sync? sync?}
               kv-store  (assoc :kv-store kv-store)
               roots-key (assoc :roots-key roots-key)
               freed-key (assoc :freed-key freed-key))]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store storage]} (await (d/open store-config opts))
                       roots (await (d/load-roots kv-store opts))
                       restore (fn [branch] (if-let [root (get roots branch)]
                                              (d/restore-set comparator root storage opts)
                                              (d/empty-set storage comparator)))]
                   (->DurableORSet id kv-store store-config storage comparator tag-fn
                                   (restore adds-branch)
                                   (restore removals-branch)
                                   false opts))))))
