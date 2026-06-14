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

   Implements `PConvergent` (-join) + `Branchable`. JVM-only for now."
  (:require [clojure.set :as set]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]))

(declare ->DurableORSet flush!)

(def ^:private adds-branch :adds)
(def ^:private removals-branch :removals)

(defn- ->pss-union [a b] (into a (seq b)))

(defrecord DurableORSet
           [id kv-store store-config storage comparator tag-fn
            adds-atom      ; atom of PSS set of [element tag]
            removals-atom  ; atom of PSS set of [element tag]
            dirty-atom]    ; atom boolean

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :orset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true
                     :graphable false :overlayable false})

  p/Snapshotable
  (snapshot-id [_] (str (hash [(set (seq @adds-atom)) (set (seq @removals-atom))])))
  (parent-ids [_] #{})
  (as-of [_ _] (let [a (set (seq @adds-atom)) r (set (seq @removals-atom))]
                 (into #{} (map first) (set/difference a r))))
  (as-of [this t _] (p/as-of this t))
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

  c/PConvergent
  ;; PURE same-store join: union both grow-only halves with the peer.
  (-join [_ other]
    (->DurableORSet id kv-store store-config storage comparator tag-fn
                    (atom (->pss-union @adds-atom @(:adds-atom other)))
                    (atom (->pss-union @removals-atom @(:removals-atom other)))
                    (atom true)))
  (-conflict-free? [_] true)

  p/GarbageCollectable
  (gc-roots [this] #{(p/snapshot-id this)})
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids {}))
  (gc-sweep! [this _snapshot-ids before]
    (flush! this)
    (d/gc! kv-store (vals (d/load-roots kv-store))
           (or before (java.util.Date.)))))

;; ============================================================
;; Value ops
;; ============================================================

(defn add
  "Add `elem` with a fresh tag (from `tag-fn`). Marks dirty. Returns o."
  [o elem]
  (swap! (:adds-atom o) conj [elem ((:tag-fn o) elem)])
  (reset! (:dirty-atom o) true)
  o)

(defn- observed-tags
  "The [elem tag] add-pairs for `elem` currently live (in adds, not removals)."
  [o elem]
  (let [rem (set (seq @(:removals-atom o)))]
    (->> (seq @(:adds-atom o))
         (filter (fn [[e _ :as pair]] (and (= e elem) (not (contains? rem pair))))))))

(defn remove-elem
  "Observed-remove `elem`: tombstone exactly the add-tags currently observed for
   it. A concurrent (unobserved) add survives the merge — add-wins. Returns o."
  [o elem]
  (let [tombstones (observed-tags o elem)]
    (when (seq tombstones)
      (swap! (:removals-atom o) (fn [s] (reduce conj s tombstones)))
      (reset! (:dirty-atom o) true)))
  o)

(defn elements
  "Live elements: those with ≥1 add-tag not tombstoned."
  [o]
  (let [adds (set (seq @(:adds-atom o)))
        rem  (set (seq @(:removals-atom o)))]
    (into #{} (map first) (set/difference adds rem))))

(defn contains-elem? [o elem] (contains? (elements o) elem))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves, update the :crdt/roots cell ({:adds :removals}) + freed."
  [o]
  (when @(:dirty-atom o)
    (let [storage (:storage o)
          adds-root     (d/store-set! @(:adds-atom o) storage)
          removals-root (d/store-set! @(:removals-atom o) storage)]
      (d/save-roots! (:kv-store o) {adds-branch adds-root removals-branch removals-root})
      (d/save-freed! (:kv-store o) storage)
      (reset! (:dirty-atom o) false)))
  o)

(defn merge-peer!
  "Cross-store -join: ship the peer's adds+removals nodes into this store and
   union both halves. Returns o."
  [o other]
  (let [ostorage (:storage other)
        a-root (d/store-set! @(:adds-atom other) ostorage)
        r-root (d/store-set! @(:removals-atom other) ostorage)]
    (d/ship! (:kv-store other) (:kv-store o) a-root)
    (d/ship! (:kv-store other) (:kv-store o) r-root)
    (swap! (:adds-atom o) ->pss-union (d/restore-set (:comparator o) a-root (:storage o)))
    (swap! (:removals-atom o) ->pss-union (d/restore-set (:comparator o) r-root (:storage o)))
    (reset! (:dirty-atom o) true)
    o))

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
  [id & {:keys [store-config comparator tag-fn]
         :or {comparator compare tag-fn (fn [_] (random-uuid))}}]
  (let [{:keys [kv-store storage]} (d/open store-config)
        roots (d/load-roots kv-store)
        restore (fn [branch] (if-let [root (get roots branch)]
                               (d/restore-set comparator root storage)
                               (d/empty-set storage comparator)))]
    (->DurableORSet id kv-store store-config storage comparator tag-fn
                    (atom (restore adds-branch))
                    (atom (restore removals-branch))
                    (atom false))))
