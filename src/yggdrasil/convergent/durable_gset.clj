(ns yggdrasil.convergent.durable-gset
  "Grow-only Set (G-Set) as a DURABLE conflict-free yggdrasil system.

   Same algebra as the in-memory `yggdrasil.convergent.gset` (value = a set,
   join = union) but the value is a persistent-sorted-set over konserve — the
   exact PSS+KonserveStorage substrate the snapshot registry runs on (see
   `yggdrasil.convergent.durable`). The registry IS a durable G-Set of content-
   addressed DAG nodes; this is that pattern as a reusable conflict-free system.

     value         a PSS sorted-set, durable + structurally shared
     -join         set union — ships only the NEW nodes (incremental)
     branch!       an independent replica head over the same PSS root (O(1));
                   replicas diverge locally and reconverge by -join (always clean)

   Implements `PConvergent` (-join) and `Branchable` — the branch/merge pair the
   distributed model rests on. -join is the PURE, same-store join; cross-store
   peers reconcile with `merge-peer!` (ship nodes, then join).

   JVM-only for now (synchronous konserve). cljs uses the ephemeral
   `yggdrasil.convergent.gset`, kept in sync by shipping nodes — the durable
   cljs (async) path is deferred."
  (:require [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]))

(declare ->DurableGSet flush!)

(defn- ->pss-union
  "Union of two PSS sets: conj all of `b` into `a` (new nodes land in a's
   storage, structurally sharing the rest)."
  [a b]
  (into a (seq b)))

(defrecord DurableGSet
           [id kv-store store-config storage comparator
            roots-atom   ; atom {branch -> pss-set} — live heads
            current      ; current branch
            dirty-atom]  ; atom #{dirty-branch …}

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :gset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true
                     :graphable false :overlayable false})

  p/Snapshotable
  (snapshot-id [_] (str (hash (into #{} (get @roots-atom current)))))
  (parent-ids [_] #{})
  (as-of [_ _] (set (get @roots-atom current)))
  (as-of [_ _ _] (set (get @roots-atom current)))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  p/Branchable
  (branches [_] (set (keys @roots-atom)))
  (branches [_ _] (set (keys @roots-atom)))
  (current-branch [_] current)
  ;; O(1): the new branch head shares the same PSS root value; divergence is
  ;; local conj onto that branch's set.
  (branch! [this name] (swap! roots-atom assoc name
                              (get @roots-atom current (d/empty-set storage comparator)))
    this)
  (branch! [this name from] (swap! roots-atom assoc name
                                   (get @roots-atom from (d/empty-set storage comparator)))
    this)
  (branch! [this name from _] (p/branch! this name from))
  (delete-branch! [this name] (swap! roots-atom dissoc name) this)
  (delete-branch! [this name _] (swap! roots-atom dissoc name) this)
  (checkout [this name] (assoc this :current name))
  (checkout [this name _] (assoc this :current name))

  p/Mergeable
  ;; branch-merge = value join (conflict-free); source is another branch.
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source _]
    (swap! roots-atom update current
           (fn [s] (->pss-union (or s (d/empty-set storage comparator))
                                (get @roots-atom source (d/empty-set storage comparator)))))
    (swap! dirty-atom conj current)
    this)
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  c/PConvergent
  ;; PURE same-store peer join: per-branch union of the two replicas' heads.
  ;; (Cross-store peers must `merge-peer!` first so the nodes are present.)
  (-join [_ other]
    (->DurableGSet id kv-store store-config storage comparator
                   (atom (merge-with ->pss-union @roots-atom @(:roots-atom other)))
                   current (atom (into #{} (keys @roots-atom)))))
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
  "Add `x` to the current branch's set (local op). Marks the branch dirty."
  [g x]
  (swap! (:roots-atom g) update (:current g)
         (fn [s] (conj (or s (d/empty-set (:storage g) (:comparator g))) x)))
  (swap! (:dirty-atom g) conj (:current g))
  g)

(defn elements
  "Read the current branch's set as a plain Clojure set."
  [g]
  (set (get @(:roots-atom g) (:current g))))

(defn added
  "Element-level join-delta: elements in `g` not in peer `other` (current
   branch). The convenience companion to node-level shipping."
  [g other]
  (clojure.set/difference (elements g) (elements other)))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist every dirty branch's set, update the roots cell + freed-set. Returns g."
  [g]
  (when (seq @(:dirty-atom g))
    (let [roots (reduce-kv (fn [m branch s] (assoc m branch (d/store-set! s (:storage g))))
                           {} @(:roots-atom g))]
      (d/save-roots! (:kv-store g) roots)
      (d/save-freed! (:kv-store g) (:storage g))
      (reset! (:dirty-atom g) #{})))
  g)

(defn merge-peer!
  "Reconcile with a peer G-Set living in a DIFFERENT store: ship the peer's
   current-branch nodes into this store, restore, and union into the current
   branch. The durable, cross-store form of -join. Returns g."
  [g other]
  (let [oroot (d/store-set! (get @(:roots-atom other) (:current other)) (:storage other))]
    (d/ship! (:kv-store other) (:kv-store g) oroot)
    (let [orestored (d/restore-set (:comparator g) oroot (:storage g))]
      (swap! (:roots-atom g) update (:current g)
             (fn [s] (->pss-union (or s (d/empty-set (:storage g) (:comparator g)))
                                  orestored)))
      (swap! (:dirty-atom g) conj (:current g))
      g)))

(defn gc!
  "Reclaim PSS nodes superseded by prior flushes (mark-and-sweep). Returns the
   set of deleted node keys."
  ([g] (p/gc-sweep! g nil nil))
  ([g before] (p/gc-sweep! g nil before)))

;; ============================================================
;; Factory
;; ============================================================

(defn durable-gset
  "Open (or create) a durable G-Set on a per-system konserve store.

     (durable-gset \"kb\" :store-config {:backend :memory :id (random-uuid)})
     (durable-gset \"kb\" :store-config {:backend :file :id (random-uuid) :path \"/tmp/g\"})

   Restores existing branch heads from the store's roots cell when present."
  [id & {:keys [store-config comparator branch] :or {comparator compare branch :main}}]
  (let [{:keys [kv-store storage]} (d/open store-config)
        roots (d/load-roots kv-store)
        roots-atom (if (seq roots)
                     (atom (reduce-kv (fn [m b addr]
                                        (assoc m b (d/restore-set comparator addr storage)))
                                      {} roots))
                     (atom {branch (d/empty-set storage comparator)}))
        cur-branch (if (seq roots) (or (some #{branch} (keys roots))
                                       (first (keys roots)))
                       branch)]
    (->DurableGSet id kv-store store-config storage comparator
                   roots-atom cur-branch (atom #{}))))
