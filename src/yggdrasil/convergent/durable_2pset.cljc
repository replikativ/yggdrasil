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
            [yggdrasil.convergent.durable :as d]))

(declare ->Durable2PSet)

(def ^:private adds-branch :adds)
(def ^:private removals-branch :removals)

(defn- ->pss-union [a b] (into a (seq b)))

(defrecord Durable2PSet
           [id kv-store store-config storage comparator
            adds-atom removals-atom dirty-atom]

  p/SystemIdentity
  (system-id [_] id)
  (system-type [_] :2p-set)
  (capabilities [_] {:snapshotable true :branchable true :mergeable true
                     :garbage-collectable true
                     :graphable false :overlayable false})

  p/Snapshotable
  ;; content-addressed identity: the hasch UUID of the live two-set state, so
  ;; equal content → equal id (what lets another system pin/track this one).
  (snapshot-id [_] (str (hasch/uuid {:adds (set (seq @adds-atom))
                                     :removals (set (seq @removals-atom))})))
  (parent-ids [_] #{})
  (as-of [_ _] (set/difference (set (seq @adds-atom)) (set (seq @removals-atom))))
  (as-of [this t _] (p/as-of this t))
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

  c/PConvergent
  ;; PURE same-store join: union both halves with the peer.
  (-join [_ other]
    (->Durable2PSet id kv-store store-config storage comparator
                    (atom (->pss-union @adds-atom @(:adds-atom other)))
                    (atom (->pss-union @removals-atom @(:removals-atom other)))
                    (atom true)))
  (-conflict-free? [_] true))

;; ============================================================
;; Value ops
;; ============================================================

(defn add [s elem]
  (swap! (:adds-atom s) conj elem)
  (reset! (:dirty-atom s) true)
  s)

(defn add-all [s elems]
  (swap! (:adds-atom s) into elems)
  (reset! (:dirty-atom s) true)
  s)

(defn remove-elem
  "Tombstone `elem` (permanent — 2P-Set semantics)."
  [s elem]
  (swap! (:removals-atom s) conj elem)
  (reset! (:dirty-atom s) true)
  s)

(defn elements
  "Live elements: adds − removals."
  [s]
  (set/difference (set (seq @(:adds-atom s))) (set (seq @(:removals-atom s)))))

(defn contains-elem? [s elem] (contains? (elements s) elem))

;; ============================================================
;; Persistence + cross-store sync
;; ============================================================

(defn flush!
  "Persist both halves + the :crdt/roots cell ({:adds :removals}) + freed."
  [s]
  (when @(:dirty-atom s)
    (let [storage (:storage s)
          adds-root     (d/store-set! @(:adds-atom s) storage)
          removals-root (d/store-set! @(:removals-atom s) storage)]
      (d/save-roots! (:kv-store s) {adds-branch adds-root removals-branch removals-root})
      (d/save-freed! (:kv-store s) storage)
      (reset! (:dirty-atom s) false)))
  s)

(defn merge-peer!
  "Cross-store -join: ship the peer's adds+removals nodes here and union both."
  [s other]
  (let [ostorage (:storage other)
        a-root (d/store-set! @(:adds-atom other) ostorage)
        r-root (d/store-set! @(:removals-atom other) ostorage)]
    (d/ship! (:kv-store other) (:kv-store s) a-root)
    (d/ship! (:kv-store other) (:kv-store s) r-root)
    (swap! (:adds-atom s) ->pss-union (d/restore-set (:comparator s) a-root (:storage s)))
    (swap! (:removals-atom s) ->pss-union (d/restore-set (:comparator s) r-root (:storage s)))
    (reset! (:dirty-atom s) true)
    s))

;; ============================================================
;; Factory
;; ============================================================

(defn durable-2pset
  "Open (or create) a durable 2P-Set on a per-system konserve store.

   :comparator             element order (default compare)
   :key-encode/:key-decode  node-key element codec (default identity; the
                            registry passes its entry<->map codec)
   Restores both halves from the store's :crdt/roots cell when present."
  [id & {:keys [store-config comparator key-encode key-decode]
         :or {comparator compare}}]
  (let [{:keys [kv-store storage]} (d/open store-config {:key-encode key-encode
                                                         :key-decode key-decode})
        roots (d/load-roots kv-store)
        restore (fn [branch] (if-let [root (get roots branch)]
                               (d/restore-set comparator root storage)
                               (d/empty-set storage comparator)))]
    (->Durable2PSet id kv-store store-config storage comparator
                    (atom (restore adds-branch))
                    (atom (restore removals-branch))
                    (atom false))))
