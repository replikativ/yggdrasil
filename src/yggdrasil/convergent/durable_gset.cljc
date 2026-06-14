(ns yggdrasil.convergent.durable-gset
  "Grow-only Set (G-Set) as a DURABLE conflict-free yggdrasil system.

   Same algebra as the in-memory `yggdrasil.convergent.gset` (value = a set,
   join = union) but the value is a persistent-sorted-set over konserve — the
   exact PSS+KonserveStorage substrate the snapshot registry runs on (see
   `yggdrasil.convergent.durable`).

     value         a PSS sorted-set, durable + structurally shared
     -join         set union — ships only the NEW nodes (incremental)
     branch!       an independent replica head over the same PSS root (O(1))

   **Cross-platform via `async+sync`.** The browser-reachable functional ops —
   factory / `add` / `elements` / `contains-elem?` / `flush!` — take `opts`
   (default `{:sync? true}`) and run sync on the JVM and async (CPS) on cljs over
   a lazy storage-backed PSS set. The protocol methods (SystemIdentity /
   Snapshotable / Branchable / Mergeable / PConvergent) and the server-only ops
   (`merge-peer!` / `gc!`) stay synchronous — they're the JVM/coordinator surface
   (a browser receives state via konserve-sync; it never ships/gcs/peer-merges)."
  (:require [clojure.set :as set]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->DurableGSet flush!)

(defn- ->pss-union
  "Union of two PSS sets — SYNC (JVM/in-memory): conj all of `b` into `a`. Used by
   the protocol `-join` and `merge-peer!` (server-side)."
  [a b]
  (into a (seq b)))

;; ============================================================
;; Record
;; ============================================================

(defrecord DurableGSet
           [id kv-store store-config storage comparator
            roots-atom current dirty-atom]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :gset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true
                     :graphable false :overlayable false})

  ;; Protocol methods are the JVM/coordinator surface — synchronous, operating
  ;; on in-memory/loaded sets. (cljs callers use the async functional API.)
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
           (or before #?(:clj (java.util.Date.) :cljs (js/Date.))))))

;; ============================================================
;; Value ops — browser-reachable, async+sync
;; ============================================================

(defn add
  "Add `x` to the current branch's set (local op). Marks the branch dirty.
   (async+sync) Returns g."
  ([g x] (add g x {:sync? true}))
  ([g x opts]
   (async+sync (:sync? opts)
               (async
                (let [cur  (:current g)
                      base (or (get @(:roots-atom g) cur)
                               (d/empty-set (:storage g) (:comparator g)))
                      s'   (await (d/set-conj base x (:comparator g) opts))]
                  (swap! (:roots-atom g) assoc cur s')
                  (swap! (:dirty-atom g) conj cur)
                  g)))))

(defn elements
  "Read the current branch's set as a plain Clojure set. (async+sync)"
  ([g] (elements g {:sync? true}))
  ([g opts]
   (async+sync (:sync? opts)
               (async (await (d/set->clj (get @(:roots-atom g) (:current g)) opts))))))

(defn contains-elem?
  "Whether `x` is in the current branch's set. (async+sync)"
  ([g x] (contains-elem? g x {:sync? true}))
  ([g x opts]
   (async+sync (:sync? opts)
               (async
                (let [s (get @(:roots-atom g) (:current g))]
                  (await (d/set-contains? s x opts)))))))

(defn added
  "Element-level join-delta: elements in `g` not in peer `other`. (async+sync)"
  ([g other] (added g other {:sync? true}))
  ([g other opts]
   (async+sync (:sync? opts)
               (async (set/difference (await (elements g opts))
                                      (await (elements other opts)))))))

;; ============================================================
;; Persistence — browser-reachable, async+sync
;; ============================================================

(defn flush!
  "Persist every dirty branch's set, update the roots cell + freed-set.
   (async+sync) Returns g."
  ([g] (flush! g {:sync? true}))
  ([g opts]
   (async+sync (:sync? opts)
               (async
                (when (seq @(:dirty-atom g))
                  (let [roots (loop [bs (seq @(:roots-atom g)) acc {}]
                                (if bs
                                  (let [[branch s] (first bs)]
                                    (recur (next bs)
                                           (assoc acc branch (await (d/store-set! s (:storage g) opts)))))
                                  acc))]
                    (await (d/save-roots! (:kv-store g) roots opts))
                    (await (d/save-freed! (:kv-store g) (:storage g) opts))
                    (reset! (:dirty-atom g) #{})))
                g))))

;; ============================================================
;; Cross-store sync + GC — server-side, synchronous
;; ============================================================

(defn merge-peer!
  "Reconcile with a peer G-Set in a DIFFERENT store: for EVERY branch the peer
   has, ship its nodes here, restore, and union into the same-named branch
   (creating it if absent). The durable, cross-store form of -join. Server-side
   (synchronous): a browser receives state via konserve-sync, never peer-merges.
   Returns g."
  [g other]
  (doseq [[branch oset] @(:roots-atom other)]
    (let [oroot (d/store-set! oset (:storage other))]
      (d/ship! (:kv-store other) (:kv-store g) oroot)
      (let [orestored (d/restore-set (:comparator g) oroot (:storage g))]
        (swap! (:roots-atom g) update branch
               (fn [s] (->pss-union (or s (d/empty-set (:storage g) (:comparator g)))
                                    orestored)))
        (swap! (:dirty-atom g) conj branch))))
  g)

(defn gc!
  "Reclaim PSS nodes superseded by prior flushes (mark-and-sweep). Returns the
   set of deleted node keys."
  ([g] (p/gc-sweep! g nil nil))
  ([g before] (p/gc-sweep! g nil before)))

;; ============================================================
;; Factory — async+sync
;; ============================================================

(defn durable-gset
  "Open (or create) a durable G-Set on a per-system konserve store. (async+sync —
   pass `:sync? false` on cljs and `await` the result.)

     (durable-gset \"kb\" :store-config {:backend :memory :id (random-uuid)})

   Restores existing branch heads from the store's roots cell when present."
  [id & {:keys [store-config comparator branch sync?]
         :or {comparator compare branch :main sync? true}}]
  (let [opts {:sync? sync?}]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store storage]} (await (d/open store-config opts))
                       roots (await (d/load-roots kv-store opts))
                       roots-atom (if (seq roots)
                                    (atom (reduce-kv
                                           (fn [m b addr]
                                             (assoc m b (d/restore-set comparator addr storage opts)))
                                           {} roots))
                                    (atom {branch (d/empty-set storage comparator)}))
                       cur-branch (if (seq roots)
                                    (or (some #{branch} (keys roots)) (first (keys roots)))
                                    branch)]
                   (->DurableGSet id kv-store store-config storage comparator
                                  roots-atom cur-branch (atom #{})))))))
