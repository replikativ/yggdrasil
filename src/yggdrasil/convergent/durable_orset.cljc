(ns yggdrasil.convergent.durable-orset
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

   Implements `PConvergent` (-join) + `Branchable`. **Fully cross-platform**: the
   record carries its sync-mode (`opts`), so EVERY content-touching op — the
   functional API AND the protocol methods (-join/snapshot-id/as-of/gc-sweep!) —
   dispatches through `async+sync` (sync on JVM, async/CPS on cljs)."
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

(declare ->DurableORSet flush!)

(def ^:private adds-branch :adds)
(def ^:private removals-branch :removals)

(defrecord DurableORSet
           [id kv-store store-config storage comparator tag-fn
            adds-atom      ; atom of PSS set of [element tag]
            removals-atom  ; atom of PSS set of [element tag]
            dirty-atom     ; atom boolean
            opts]          ; the record's sync-mode

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :orset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true :overlayable true
                     :graphable false})

  p/Snapshotable
  ;; addressable snapshot = a content-addressed COMMIT object {:adds :removals}
  ;; over the two halves' PSS roots (re-openable via `as-of`, the freeze handle).
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async
                 (let [adds-root (await (d/store-set! @adds-atom storage opts))
                       rem-root  (await (d/store-set! @removals-atom storage opts))]
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
  (-join [_ other]
    (async+sync (:sync? opts)
                (async
                 (->DurableORSet id kv-store store-config storage comparator tag-fn
                                 (atom (await (d/set-union @adds-atom @(:adds-atom other) comparator opts)))
                                 (atom (await (d/set-union @removals-atom @(:removals-atom other) comparator opts)))
                                 (atom true) opts))))
  (-conflict-free? [_] true)

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  (gc-sweep! [this _snapshot-ids before]
    (async+sync (:sync? opts)
                (async
                 (await (flush! this))
                 (await (d/gc! kv-store (vals (await (d/load-roots kv-store opts)))
                               (or before #?(:clj (java.util.Date.) :cljs (js/Date.))) opts)))))

  p/Overlayable
  ;; uniform isolate (the residue fix): a fresh-atoms clone of BOTH halves at the
  ;; current value. `merge-down!` joins both halves back (add-wins convergent).
  (overlay [this _opts]
    (ovl/convergent-overlay this :frozen nil
                            (fn [s] (assoc s :adds-atom (atom @(:adds-atom s))
                                           :removals-atom (atom @(:removals-atom s))
                                           :dirty-atom (atom false))))))

;; ============================================================
;; Value ops
;; ============================================================

(defn add
  "Add `elem` with a fresh tag (from `tag-fn`). Marks dirty. (async+sync) Returns o."
  ([o elem] (add o elem (:opts o)))
  ([o elem opts]
   (async+sync (:sync? opts)
               (async
                (let [pair [elem ((:tag-fn o) elem)]
                      s'   (await (d/set-conj @(:adds-atom o) pair (:comparator o) opts))]
                  (reset! (:adds-atom o) s')
                  (reset! (:dirty-atom o) true)
                  o)))))

(defn- live-pairs
  "async+sync: the live [elem tag] add-pairs (in adds, not removals)."
  [o opts]
  (async+sync (:sync? opts)
              (async
               (set/difference (await (d/set->clj @(:adds-atom o) opts))
                               (await (d/set->clj @(:removals-atom o) opts))))))

(defn remove-elem
  "Observed-remove `elem`: tombstone exactly the add-tags currently observed for
   it. A concurrent (unobserved) add survives — add-wins. (async+sync) Returns o."
  ([o elem] (remove-elem o elem (:opts o)))
  ([o elem opts]
   (async+sync (:sync? opts)
               (async
                (let [tombstones (filter (fn [[e _]] (= e elem)) (await (live-pairs o opts)))]
                  (loop [ts (seq tombstones)]
                    (when ts
                      (let [s' (await (d/set-conj @(:removals-atom o) (first ts) (:comparator o) opts))]
                        (reset! (:removals-atom o) s')
                        (reset! (:dirty-atom o) true)
                        (recur (next ts)))))
                  o)))))

(defn elements
  "Live elements: those with ≥1 add-tag not tombstoned. (async+sync)"
  ([o] (elements o (:opts o)))
  ([o opts]
   (async+sync (:sync? opts)
               (async (into #{} (map first) (await (live-pairs o opts)))))))

(defn contains-elem?
  "Whether `elem` is live. (async+sync)"
  ([o elem] (contains-elem? o elem (:opts o)))
  ([o elem opts]
   (async+sync (:sync? opts)
               (async (contains? (await (elements o opts)) elem)))))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves, update the :crdt/roots cell ({:adds :removals}) + freed.
   (async+sync)"
  ([o] (flush! o (:opts o)))
  ([o opts]
   (async+sync (:sync? opts)
               (async
                (when @(:dirty-atom o)
                  (let [storage       (:storage o)
                        adds-root     (await (d/store-set! @(:adds-atom o) storage opts))
                        removals-root (await (d/store-set! @(:removals-atom o) storage opts))]
                    (await (d/save-roots! (:kv-store o)
                                          {adds-branch adds-root removals-branch removals-root} opts))
                    (await (d/save-freed! (:kv-store o) storage opts))
                    (reset! (:dirty-atom o) false)))
                o))))

(defn merge-peer!
  "Cross-store -join: ship the peer's adds+removals nodes into this store and
   union both halves. (async+sync) Returns o."
  [o other]
  (let [opts (:opts o) cmp (:comparator o)]
    (async+sync (:sync? opts)
                (async
                 (let [ostorage (:storage other)
                       a-root (await (d/store-set! @(:adds-atom other) ostorage opts))
                       r-root (await (d/store-set! @(:removals-atom other) ostorage opts))]
                   (await (d/ship! (:kv-store other) (:kv-store o) a-root opts))
                   (await (d/ship! (:kv-store other) (:kv-store o) r-root opts))
                   (reset! (:adds-atom o)
                           (await (d/set-union @(:adds-atom o) (d/restore-set cmp a-root (:storage o) opts) cmp opts)))
                   (reset! (:removals-atom o)
                           (await (d/set-union @(:removals-atom o) (d/restore-set cmp r-root (:storage o) opts) cmp opts)))
                   (reset! (:dirty-atom o) true)
                   o)))))

(defn gc!
  "Reclaim PSS nodes superseded by prior flushes (mark-and-sweep). Returns the
   set of deleted node keys."
  ([o] (p/gc-sweep! o nil nil))
  ([o before] (p/gc-sweep! o nil before)))

;; ============================================================
;; Factory
;; ============================================================

(defn durable-orset
  "Open (or create) a durable OR-Set on a per-system konserve store.

     (durable-orset \"reg\" :store-config {:backend :memory :id (random-uuid)})

   :tag-fn  element -> tag (default: ignore element, fresh random-uuid → true
            OR-Set). Pass a content-hash fn for idempotent add (registry shape).
   Restores both halves from the store's :crdt/roots cell when present."
  [id & {:keys [store-config comparator tag-fn sync? kv-store roots-key freed-key]
         :or {comparator compare tag-fn (fn [_] (random-uuid)) sync? true}}]
  (let [freed-key (or freed-key (when (vector? roots-key) (assoc roots-key 0 :crdt/freed)))
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
                                   (atom (restore adds-branch))
                                   (atom (restore removals-branch))
                                   (atom false) opts))))))
