(ns yggdrasil.convergent.durable-2pset
  "Two-Phase Set (2P-Set) as a DURABLE conflict-free yggdrasil system.

   TWO grow-only, content-addressed PSS sets of BARE elements —
     adds      every added element        (root under :crdt/roots :adds)
     removals  tombstones for removed els  (… :crdt/roots :removals)
   with live(e) ⇔ e ∈ adds ∧ e ∉ removals. Convergent add+remove where REMOVE
   is permanent: re-adding a removed element keeps it removed (the content-add
   re-enters `adds`, but it is also in `removals`). For re-add-after-remove use
   the OR-Set (`durable-orset`).

   Unlike the OR-Set this stores BARE elements (no `[elem tag]` pairs), so a
   key-level storage codec (`:key-encode`/`:key-decode` — e.g. the registry's
   entry<->map) applies cleanly and there is no doubled storage. Elements may be
   any konserve-native value or, with a codec, records; a `:comparator` orders
   them (default `compare`).

   Implements SystemIdentity / Snapshotable / Branchable / Mergeable /
   PConvergent — so a 2P-Set (and the registry, a lens over one) is a
   first-class yggdrasil system that another yggdrasil can itself track, fork,
   and merge. Content-addressing gives it a stable snapshot identity."
  (:require [clojure.set :as set]
            [hasch.core :as hasch]
            [yggdrasil.protocols :as p]
            [yggdrasil.convergent :as c]
            [yggdrasil.convergent.durable :as d]
            [yggdrasil.convergent.overlay :as ovl]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(declare ->Durable2PSet flush!)

(def ^:private adds-branch :adds)
(def ^:private removals-branch :removals)

(defrecord Durable2PSet
           [id kv-store store-config storage comparator
            adds-atom removals-atom dirty-atom opts]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :2p-set)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true :overlayable true
                     :graphable false})

  p/Snapshotable
  ;; addressable snapshot = a content-addressed COMMIT object {:adds :removals}
  ;; over the two halves' PSS roots (stable across peers — equal content ⇒ equal
  ;; id — AND re-openable via `as-of`, the freeze handle).
  (snapshot-id [_]
    (async+sync (:sync? opts)
                (async
                 (let [adds-root (await (d/store-set! @adds-atom storage opts))
                       rem-root  (await (d/store-set! @removals-atom storage opts))]
                   (str (await (d/store-commit! kv-store {:adds adds-root :removals rem-root} opts)))))))
  (parent-ids [_] #{})
  (as-of [this snap-id] (p/as-of this snap-id nil))
  (as-of [_ snap-id _opts]
    ;; restore the FIXED value (adds − removals) at that commit, not the live set.
    (async+sync (:sync? opts)
                (async
                 (let [commit (await (d/read-commit kv-store (parse-uuid (str snap-id)) opts))]
                   (set/difference
                    (await (d/set->clj (d/restore-set comparator (:adds commit) storage opts) opts))
                    (await (d/set->clj (d/restore-set comparator (:removals commit) storage opts) opts)))))))
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
  ;; PURE same-store join: union both halves with the peer. (async+sync)
  (-join [_ other]
    (async+sync (:sync? opts)
                (async
                 (->Durable2PSet id kv-store store-config storage comparator
                                 (atom (await (d/set-union @adds-atom @(:adds-atom other) comparator opts)))
                                 (atom (await (d/set-union @removals-atom @(:removals-atom other) comparator opts)))
                                 (atom true) opts))))
  (-conflict-free? [_] true)

  p/GarbageCollectable
  (gc-roots [this]
    (async+sync (:sync? opts) (async #{(await (p/snapshot-id this))})))
  (gc-sweep! [this snapshot-ids] (p/gc-sweep! this snapshot-ids nil))
  ;; node reclamation: flush, then mark-and-sweep nodes unreachable from the
  ;; live adds/removals roots. (async+sync)
  (gc-sweep! [this _snapshot-ids before]
    (async+sync (:sync? opts)
                (async
                 (await (flush! this))
                 (await (d/gc! kv-store (vals (await (d/load-roots kv-store opts)))
                               (or before #?(:clj (java.util.Date.) :cljs (js/Date.))) opts)))))

  p/Overlayable
  ;; uniform isolate (the residue fix): a fresh-atoms clone of BOTH halves at the
  ;; current value. Mutate via `ovl/overlay-system`; `merge-down!` joins both
  ;; halves back (convergent), `discard!` drops it.
  (overlay [this _opts]
    (ovl/convergent-overlay this :frozen nil
                            (fn [s] (assoc s :adds-atom (atom @(:adds-atom s))
                                           :removals-atom (atom @(:removals-atom s))
                                           :dirty-atom (atom false))))))

;; ============================================================
;; Value ops
;; ============================================================

(defn- conj-into!
  "async+sync: conj `elem` onto the PSS set in `atom-kw` of `s`, mark dirty."
  [s atom-kw elem opts]
  (async+sync (:sync? opts)
              (async
               (let [s' (await (d/set-conj @(atom-kw s) elem (:comparator s) opts))]
                 (reset! (atom-kw s) s')
                 (reset! (:dirty-atom s) true)
                 s))))

(defn add
  "Add `elem`. (async+sync)"
  ([s elem] (add s elem (:opts s)))
  ([s elem opts] (conj-into! s :adds-atom elem opts)))

(defn add-all
  "Add many elements. (async+sync)"
  ([s elems] (add-all s elems (:opts s)))
  ([s elems opts]
   (async+sync (:sync? opts)
               (async
                (loop [es (seq elems)]
                  (if es
                    (do (await (conj-into! s :adds-atom (first es) opts)) (recur (next es)))
                    s))))))

(defn remove-elem
  "Tombstone `elem` (permanent — 2P-Set semantics). (async+sync)"
  ([s elem] (remove-elem s elem (:opts s)))
  ([s elem opts] (conj-into! s :removals-atom elem opts)))

(defn elements
  "Live elements: adds − removals. (async+sync)"
  ([s] (elements s (:opts s)))
  ([s opts]
   (async+sync (:sync? opts)
               (async (set/difference (await (d/set->clj @(:adds-atom s) opts))
                                      (await (d/set->clj @(:removals-atom s) opts)))))))

(defn contains-elem?
  "Whether `elem` is live. (async+sync)"
  ([s elem] (contains-elem? s elem (:opts s)))
  ([s elem opts]
   (async+sync (:sync? opts)
               (async (contains? (await (elements s opts)) elem)))))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves + the :crdt/roots cell ({:adds :removals}) + freed.
   (async+sync)"
  ([s] (flush! s (:opts s)))
  ([s opts]
   (async+sync (:sync? opts)
               (async
                (when @(:dirty-atom s)
                  (let [storage       (:storage s)
                        adds-root     (await (d/store-set! @(:adds-atom s) storage opts))
                        removals-root (await (d/store-set! @(:removals-atom s) storage opts))]
                    (await (d/save-roots! (:kv-store s)
                                          {adds-branch adds-root removals-branch removals-root} opts))
                    (await (d/save-freed! (:kv-store s) storage opts))
                    (reset! (:dirty-atom s) false)))
                s))))

(defn merge-peer!
  "Cross-store -join: ship the peer's adds+removals nodes here and union both.
   (async+sync)"
  [s other]
  (let [opts (:opts s) cmp (:comparator s)]
    (async+sync (:sync? opts)
                (async
                 (let [ostorage (:storage other)
                       a-root (await (d/store-set! @(:adds-atom other) ostorage opts))
                       r-root (await (d/store-set! @(:removals-atom other) ostorage opts))]
                   (await (d/ship! (:kv-store other) (:kv-store s) a-root opts))
                   (await (d/ship! (:kv-store other) (:kv-store s) r-root opts))
                   (reset! (:adds-atom s)
                           (await (d/set-union @(:adds-atom s)
                                               (d/restore-set cmp a-root (:storage s) opts) cmp opts)))
                   (reset! (:removals-atom s)
                           (await (d/set-union @(:removals-atom s)
                                               (d/restore-set cmp r-root (:storage s) opts) cmp opts)))
                   (reset! (:dirty-atom s) true)
                   s)))))

(defn gc!
  "Reclaim PSS nodes unreachable from the live adds/removals roots (mark-and-sweep).
   Returns the set of deleted node keys."
  ([s] (p/gc-sweep! s nil nil))
  ([s before] (p/gc-sweep! s nil before)))

;; ============================================================
;; Factory
;; ============================================================

(defn durable-2pset
  "Open (or create) a durable 2P-Set on a per-system konserve store.

   :comparator             element order (default compare)
   :key-encode/:key-decode  node-key element codec (default identity; the
                            registry passes its entry<->map codec)
   Restores both halves from the store's :crdt/roots cell when present."
  [id & {:keys [store-config comparator key-encode key-decode sync? kv-store roots-key freed-key]
         :or {comparator compare sync? true}}]
  (let [freed-key (or freed-key (when (vector? roots-key) (assoc roots-key 0 :crdt/freed)))
        opts (cond-> {:sync? sync?}
               kv-store  (assoc :kv-store kv-store)
               roots-key (assoc :roots-key roots-key)
               freed-key (assoc :freed-key freed-key))]
    (async+sync sync?
                (async
                 (let [{:keys [kv-store storage]} (await (d/open store-config (assoc opts :key-encode key-encode
                                                                                    :key-decode key-decode)))
                       roots (await (d/load-roots kv-store opts))
                       restore (fn [branch] (if-let [root (get roots branch)]
                                              (d/restore-set comparator root storage opts)
                                              (d/empty-set storage comparator)))]
                   (->Durable2PSet id kv-store store-config storage comparator
                                   (atom (restore adds-branch))
                                   (atom (restore removals-branch))
                                   (atom false) opts))))))
