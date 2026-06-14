(ns yggdrasil.convergent.durable-gset
  "Grow-only Set (G-Set) as a DURABLE conflict-free yggdrasil system.

   Same algebra as the in-memory `yggdrasil.convergent.gset` (value = a set,
   join = union) but the value is a persistent-sorted-set over konserve — the
   exact PSS+KonserveStorage substrate the snapshot registry runs on.

   **Fully cross-platform.** The record carries the sync-mode (`opts`) its store
   was opened in, so EVERY content-touching op — functional (`add`/`elements`/
   `flush!`/…) AND the protocol methods (`-join`/`snapshot-id`/`as-of`/`merge!`/
   `gc-sweep!`) — dispatches through `async+sync`: a JVM-opened set is sync, a
   browser-opened (`:sync? false`) set is async (CPS you `await`). Only the
   structural ops that touch the branch map, not the set contents
   (`branches`/`checkout`/`branch!`/…), are plain sync on both."
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

(defrecord DurableGSet
           [id kv-store store-config storage comparator
            roots-atom current dirty-atom opts]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :gset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true
                     :graphable false :overlayable false})

  p/Snapshotable
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async (str (hash (await (d/set->clj (get @roots-atom current) opts)))))))
  (parent-ids [_] #{})
  (as-of [_ _]
    (async+sync (:sync? opts)
                (async (await (d/set->clj (get @roots-atom current) opts)))))
  (as-of [this t _] (p/as-of this t))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  ;; structural ops — branch map only, sync on both platforms
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
  ;; branch-merge = value union of another branch into the current one
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source _]
    (async+sync (:sync? opts)
                (async
                 (let [src (get @roots-atom source (d/empty-set storage comparator))
                       cur (or (get @roots-atom current) (d/empty-set storage comparator))
                       u   (await (d/set-union cur src comparator opts))]
                   (swap! roots-atom assoc current u)
                   (swap! dirty-atom conj current)
                   this))))
  (conflicts [_ _ _] []) (conflicts [_ _ _ _] [])
  (diff [_ _ _] {}) (diff [_ _ _ _] {})

  p/Committable
  ;; "commit" a durable CRDT = make its current state durable (flush). Identity
  ;; is content-addressed, so this advances nothing — it's the persist step the
  ;; composite's transactional commit needs. (async+sync via flush!)
  (commit! [this] (flush! this))
  (commit! [this _message] (flush! this))
  (commit! [this _message _opts] (flush! this))

  c/PConvergent
  (-join [_ other]
    (async+sync (:sync? opts)
                (async
                 (let [branches (set (concat (keys @roots-atom) (keys @(:roots-atom other))))
                       joined   (loop [bs (seq branches) acc {}]
                                  (if bs
                                    (let [b     (first bs)
                                          a-set (or (get @roots-atom b) (d/empty-set storage comparator))
                                          b-set (get @(:roots-atom other) b)
                                          u     (if b-set (await (d/set-union a-set b-set comparator opts)) a-set)]
                                      (recur (next bs) (assoc acc b u)))
                                    acc))]
                   (->DurableGSet id kv-store store-config storage comparator
                                  (atom joined) current (atom (into #{} (keys @roots-atom))) opts)))))
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
                               (or before #?(:clj (java.util.Date.) :cljs (js/Date.))) opts))))))

;; ============================================================
;; Value ops — cross-platform (dispatch on the record's mode)
;; ============================================================

(defn add
  "Add `x` to the current branch's set; mark dirty. (async+sync) Returns g."
  [g x]
  (let [opts (:opts g)]
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
  [g]
  (d/set->clj (get @(:roots-atom g) (:current g)) (:opts g)))

(defn contains-elem?
  "Whether `x` is in the current branch's set. (async+sync)"
  [g x]
  (d/set-contains? (get @(:roots-atom g) (:current g)) x (:opts g)))

(defn added
  "Element-level join-delta: elements in `g` not in peer `other`. (async+sync)"
  [g other]
  (let [opts (:opts g)]
    (async+sync (:sync? opts)
                (async (set/difference (await (elements g)) (await (elements other)))))))

;; ============================================================
;; Persistence — cross-platform
;; ============================================================

(defn flush!
  "Persist every dirty branch's set, update the roots cell + freed-set.
   (async+sync) Returns g."
  [g]
  (let [opts (:opts g)]
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
;; Cross-store sync + GC — cross-platform
;; ============================================================

(defn merge-peer!
  "Reconcile with a peer G-Set in a DIFFERENT store: for EVERY branch the peer
   has, ship its nodes here, restore, and union into the same-named branch
   (creating it if absent). The durable, cross-store form of -join. (async+sync)
   Returns g."
  [g other]
  (let [opts (:opts g)]
    (async+sync (:sync? opts)
                (async
                 (loop [bs (seq @(:roots-atom other))]
                   (when bs
                     (let [[branch oset] (first bs)
                           oroot     (await (d/store-set! oset (:storage other) opts))
                           _         (await (d/ship! (:kv-store other) (:kv-store g) oroot opts))
                           orestored (d/restore-set (:comparator g) oroot (:storage g) opts)
                           cur       (or (get @(:roots-atom g) branch)
                                         (d/empty-set (:storage g) (:comparator g)))
                           u         (await (d/set-union cur orestored (:comparator g) opts))]
                       (swap! (:roots-atom g) assoc branch u)
                       (swap! (:dirty-atom g) conj branch)
                       (recur (next bs)))))
                 g))))

(defn gc!
  "Reclaim PSS nodes superseded by prior flushes (mark-and-sweep). (async+sync)"
  ([g] (p/gc-sweep! g nil nil))
  ([g before] (p/gc-sweep! g nil before)))

;; ============================================================
;; Factory — cross-platform
;; ============================================================

(defn durable-gset
  "Open (or create) a durable G-Set on a per-system konserve store. (async+sync —
   pass `:sync? false` on cljs and `await`; the returned record then carries that
   mode, so all its ops are async too.)

     (durable-gset \"kb\" :store-config {:backend :memory :id (random-uuid)})"
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
                                  roots-atom cur-branch (atom #{}) opts))))))
