(ns yggdrasil.convergent.gset
  "Grow-only Set (G-Set) as a DURABLE conflict-free yggdrasil system.

   Same algebra as the in-memory `yggdrasil.convergent.gset` (value = a set,
   join = union) but the value is a persistent-sorted-set over konserve — the
   exact PSS+KonserveStorage substrate the snapshot registry runs on.

   VALUE SEMANTICS: the record holds `roots` ({branch → immutable PSS set}) and
   `dirty` (the set of branches changed since last flush) as PLAIN fields; every
   mutator returns a NEW record (never mutates in place). The mutable cell lives
   in the HOLDER — a spindel signal-atom or an overlay's `:local-writes` — which
   swaps this value.

   **Fully cross-platform.** The record carries the sync-mode (`opts`) its store
   was opened in, so EVERY content-touching op — functional (`add`/`elements`/
   `flush!`/…) AND the protocol methods (`-join`/`snapshot-id`/`as-of`/`merge!`/
   `gc-sweep!`) — dispatches through `async+sync`: a JVM-opened set is sync, a
   browser-opened (`:sync? false`) set is async (CPS you `await`)."
  (:refer-clojure :exclude [conj contains?])
  (:require [clojure.set :as set]
            [yggdrasil.protocols :as p]
            [yggdrasil.types :as t]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->GSet flush! apply-delta)

(defrecord GSet
           [id kv-store store-config storage comparator
            roots       ; {branch → immutable PSS set}
            current     ; current branch keyword
            dirty       ; #{branch} changed since last flush
            opts]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :gset)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true :overlayable true
                     :graphable false})

  p/Snapshotable
  ;; snapshot-id = the content-addressed PSS ROOT of the current branch (an
  ;; addressable value handle, stable across peers), NOT a bare content hash — so
  ;; `as-of`/`branch!` can re-open it (freeze + run in isolation).
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async (str (await (d/store-set! (get roots current) storage opts))))))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; restore the immutable set rooted at `snap-id` (a content root address) —
    ;; the FIXED value at that snapshot, not the live current branch.
    (async+sync (:sync? opts)
                (async (await (d/set->clj (d/restore-set comparator (parse-uuid (str snap-id)) storage opts) opts)))))
  (snapshot-meta [_ _] {}) (snapshot-meta [_ _ _] {})

  ;; structural ops — value-semantic: return a NEW system over a new roots map
  p/Branchable
  (branches [_] (set (keys roots)))
  (branches [_ _] (set (keys roots)))
  (current-branch [_] current)
  (branch! [this name] (assoc this :roots (assoc roots name (get roots current (d/empty-set storage comparator)))))
  (branch! [this name from] (p/branch! this name from nil))
  ;; `from` = a branch keyword (branch off that head) OR a snapshot-id string
  ;; (branch off a FIXED content root — the freeze+isolate primitive). The new
  ;; branch shares the immutable content-addressed nodes; writes go to fresh
  ;; nodes, so it is isolated by construction.
  (branch! [this name from _]
    (assoc this :roots (assoc roots name
                              (if (keyword? from)
                                (get roots from (d/empty-set storage comparator))
                                (d/restore-set comparator (parse-uuid (str from)) storage opts)))))
  (delete-branch! [this name] (assoc this :roots (dissoc roots name)))
  (delete-branch! [this name _] (assoc this :roots (dissoc roots name)))
  (checkout [this name] (assoc this :current name))
  (checkout [this name _] (assoc this :current name))

  p/Mergeable
  ;; branch-merge = value union of another branch into the current one
  (merge! [this source] (p/merge! this source nil))
  (merge! [this source _]
    (async+sync (:sync? opts)
                (async
                 (let [src (get roots source (d/empty-set storage comparator))
                       cur (or (get roots current) (d/empty-set storage comparator))
                       u   (await (d/set-union cur src comparator opts))]
                   (assoc this :roots (assoc roots current u) :dirty (clojure.core/conj dirty current))))))
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
  (-join [this other]
    (async+sync (:sync? opts)
                (async
                 (let [branches (set (concat (keys roots) (keys (:roots other))))
                       joined   (loop [bs (seq branches) acc {}]
                                  (if bs
                                    (let [b     (first bs)
                                          a-set (or (get roots b) (d/empty-set storage comparator))
                                          b-set (get (:roots other) b)
                                          u     (if b-set (await (d/set-union a-set b-set comparator opts)) a-set)]
                                      (recur (next bs) (assoc acc b u)))
                                    acc))]
                   ;; IDEMPOTENCE: a join that adds nothing returns the receiver
                   ;; UNCHANGED (identical) — else a signal holding this CRDT sees a
                   ;; new (≠) value on every join and mutually-synced peers re-publish
                   ;; forever. Only mark genuinely-changed branches dirty.
                   (if (= joined roots)
                     this
                     (->GSet id kv-store store-config storage comparator
                             joined current
                             (into #{} (remove #(= (get joined %) (get roots %)))
                                   (keys joined))
                             opts))))))
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
                 ;; retain held snapshots: a G-Set snapshot-id IS a PSS root address,
                 ;; so keep its reachable nodes (else as-of/frozen on it breaks post-GC).
                 ;; The cutoff (`:remove-before`/`:grace-period-ms`) rides in `gc-opts`
                 ;; → d/gc!'s `t/gc-cutoff` (default epoch ⇒ reclaim nothing).
                 (await (d/gc! kv-store (vals (await (d/load-roots kv-store opts)))
                               (merge gc-opts opts {:retain-roots (seq snapshot-ids)}))))))

  p/Overlayable
  ;; :frozen → carry the current roots (immutable PSS values; isolated by value).
  ;; :following → an EMPTY current branch; `ovl/overlay-value` joins it with the
  ;; LIVE parent on read. Mutate the clone via `ovl/overlay-swap!`; `merge-down!`
  ;; joins it back.
  (overlay [this opts]
    (let [mode (or (:mode opts) :frozen)
          lw   (if (= :following mode)
                 (assoc this :roots {current (d/empty-set storage comparator)} :dirty #{})
                 (assoc this :dirty #{}))]
      (ovl/convergent-overlay this mode lw))))

;; ============================================================
;; Value ops — each returns a NEW G-Set value (value-semantic)
;; ============================================================

(defn conj
  "Add `x` to the current branch's set; RECORD the op as a local δ (`{x}`) so a
   synced signal can ship just the op. (async+sync) Returns a NEW (δ-carrying) g."
  [g x]
  (let [opts (:opts g)]
    (async+sync (:sync? opts)
                (async
                 (let [cur  (:current g)
                       base (or (get (:roots g) cur)
                                (d/empty-set (:storage g) (:comparator g)))
                       s'   (await (d/set-conj base x (:comparator g) opts))]
                   (c/with-delta (assoc g :roots (assoc (:roots g) cur s')
                                        :dirty (clojure.core/conj (:dirty g) cur))
                     set/union #{x}))))))

(defn apply-delta
  "Consume a peer's G-Set δ — a set of added elements — by unioning it into the
   current branch. The OP-path apply (cheap: O(δ), no full -join, no diffing); the
   counterpart to -join (the STATE-path). Returns a NEW g. (async+sync)"
  [g delta]
  (let [opts (:opts g)]
    (async+sync (:sync? opts)
                (async
                 (let [cur  (:current g)
                       base (or (get (:roots g) cur)
                                (d/empty-set (:storage g) (:comparator g)))
                       s'   (loop [s base es (seq delta)]
                              (if es
                                (recur (await (d/set-conj s (first es) (:comparator g) opts)) (next es))
                                s))]
                   ;; clear δ: a remote-integrated value re-propagates nothing
                   ;; (the receiver's own ops shipped at their mutation).
                   (c/clear-delta (assoc g :roots (assoc (:roots g) cur s')
                                         :dirty (clojure.core/conj (:dirty g) cur))))))))

(defn elements
  "Read the current branch's set as a plain Clojure set. (async+sync)"
  [g]
  (d/set->clj (get (:roots g) (:current g)) (:opts g)))

(defn contains?
  "Whether `x` is in the current branch's set. (async+sync)"
  [g x]
  (d/set-contains? (get (:roots g) (:current g)) x (:opts g)))

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
  "Persist every dirty branch's set, update the roots cell + freed-set. Returns a
   NEW g with `dirty` cleared (callers must ADOPT it). (async+sync)"
  [g]
  (let [opts (:opts g)]
    (async+sync (:sync? opts)
                (async
                 (if (seq (:dirty g))
                   (let [roots (loop [bs (seq (:roots g)) acc {}]
                                 (if bs
                                   (let [[branch s] (first bs)]
                                     (recur (next bs)
                                            (assoc acc branch (await (d/store-set! s (:storage g) opts)))))
                                   acc))]
                     (await (d/save-roots! (:kv-store g) roots opts))
                     (await (d/save-freed! (:kv-store g) (:storage g) opts))
                     (assoc g :dirty #{}))
                   g)))))

;; ============================================================
;; Cross-store sync + GC — cross-platform
;; ============================================================

(defn merge-peer!
  "Reconcile with a peer G-Set in a DIFFERENT store: for EVERY branch the peer
   has, ship its nodes here, restore, and union into the same-named branch
   (creating it if absent). The durable, cross-store form of -join. Returns a
   NEW g. (async+sync)"
  [g other]
  (let [opts (:opts g)]
    (async+sync (:sync? opts)
                (async
                 (loop [bs (seq (:roots other)) roots (:roots g) dirty (:dirty g)]
                   (if bs
                     (let [[branch oset] (first bs)
                           oroot     (await (d/store-set! oset (:storage other) opts))
                           _         (await (d/ship! (:kv-store other) (:kv-store g) oroot opts))
                           orestored (d/restore-set (:comparator g) oroot (:storage g) opts)
                           cur       (or (get roots branch)
                                         (d/empty-set (:storage g) (:comparator g)))
                           u         (await (d/set-union cur orestored (:comparator g) opts))]
                       (recur (next bs) (assoc roots branch u) (clojure.core/conj dirty branch)))
                     (assoc g :roots roots :dirty dirty)))))))

(defn gc!
  "Reclaim PSS nodes superseded by prior flushes (mark-and-sweep). SAFE BY DEFAULT
   (reclaims nothing); pass a window in `opts` (`:remove-before`/`:grace-period-ms`).
   (async+sync)"
  ([g] (p/gc-sweep! g nil nil))
  ([g opts] (p/gc-sweep! g nil opts)))

;; ============================================================
;; Factory — cross-platform
;; ============================================================

(defn gset
  "Open (or create) a durable G-Set on a per-system konserve store. (async+sync —
   pass `:sync? false` on cljs and `await`; the returned record then carries that
   mode, so all its ops are async too.)

     (gset \"kb\" {:store-config {:backend :memory :id (random-uuid)}})"
  ([id] (gset id {}))
  ([id {:keys [store-config comparator branch sync? kv-store roots-key freed-key]
        :or {comparator compare branch :main sync? true}}]
   (let [store-config (or store-config (when-not kv-store (d/mem-store-config)))
        freed-key (or freed-key (when (vector? roots-key) (assoc roots-key 0 :crdt/freed)))
        opts (cond-> {:sync? sync?}
               kv-store  (assoc :kv-store kv-store)
               roots-key (assoc :roots-key roots-key)
               freed-key (assoc :freed-key freed-key))]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store storage]} (await (d/open store-config opts))
                       loaded (await (d/load-roots kv-store opts))
                       roots  (if (seq loaded)
                                (reduce-kv
                                 (fn [m b addr]
                                   (assoc m b (d/restore-set comparator addr storage opts)))
                                 {} loaded)
                                {branch (d/empty-set storage comparator)})
                       cur-branch (if (seq loaded)
                                    (or (some #{branch} (keys loaded)) (first (keys loaded)))
                                    branch)]
                   (->GSet id kv-store store-config storage comparator
                           roots cur-branch #{} opts)))))))
