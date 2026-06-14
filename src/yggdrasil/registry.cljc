(ns yggdrasil.registry
  "Cross-system snapshot registry — a content-addressed 2P-Set of RegistryEntry.

   The registry is a conflict-free yggdrasil system (see
   `yggdrasil.convergent`): it grows monotonically and reconciles by union, so
   it IS a CRDT. But it also needs REMOVE (deregister a head, GC an old
   snapshot), which a grow-only G-Set can't do convergently. So the registry is
   a **2P-Set**: two grow-only, content-addressed PSS sets —

     adds      every registered entry        (root under :registry/roots :adds)
     removals  tombstones for deregistered entries     (… :registry/roots :removals)

   with live(entry) ⇔ entry ∈ adds ∧ entry ∉ removals. Entries are
   content-addressed (`:content-addressed? true`), so re-registering the same
   entry is idempotent and a deregistered (content-)entry stays gone — exactly
   what a registry wants (a GC'd snapshot's content is gone; new work is a new
   snapshot-id, never a re-add of a dead one). When richer re-add-after-remove
   semantics are needed, use the full OR-Set (`yggdrasil.convergent.durable-orset`).

   Both halves are tsbs-sorted (`[hlc system-id branch-name snapshot-id]`) PSS
   B-trees over KonserveStorage. Queries full-scan the live set — fast enough at
   the expected scale (thousands of entries). Sync rides konserve-sync's
   reachability walk over `:registry/roots` (which walks BOTH roots); a
   subscriber projects live = adds − removals."
  (:require [clojure.set :as set]
            [yggdrasil.types :as t]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.storage :as store]
            [org.replikativ.persistent-sorted-set :as pss]))

;; ============================================================
;; Comparator
;; ============================================================

(defn- safe-compare [a b]
  (cond (and (nil? a) (nil? b)) 0
        (nil? a) -1
        (nil? b) 1
        :else (compare a b)))

(defn- hlc-cmp [a b]
  (cond (and (nil? a) (nil? b)) 0
        (nil? a) -1
        (nil? b) 1
        :else (t/hlc-compare a b)))

(def ^:private tsbs-comparator
  "Sort by [hlc system-id branch-name snapshot-id]."
  (fn [a b]
    (let [c (hlc-cmp (:hlc a) (:hlc b))]
      (if-not (zero? c) c
              (let [c (safe-compare (:system-id a) (:system-id b))]
                (if-not (zero? c) c
                        (let [c (safe-compare (:branch-name a) (:branch-name b))]
                          (if-not (zero? c) c
                                  (safe-compare (:snapshot-id a) (:snapshot-id b))))))))))

(def ^:private roots-key :registry/roots)
(def ^:private freed-key :registry/freed)
(def ^:private branching-factor 64)

;; ============================================================
;; PSS helpers
;; ============================================================

(defn- empty-set [storage]
  (pss/sorted-set* {:comparator tsbs-comparator :storage storage
                    :branching-factor branching-factor}))

(defn- restore-set [root storage]
  (pss/restore-by tsbs-comparator root storage {:branching-factor branching-factor}))

;; ============================================================
;; Registry record
;; ============================================================

(defrecord Registry
           [adds-atom        ; atom of PSS — registered entries
            removals-atom    ; atom of PSS — tombstones
            kv-store         ; konserve store (nil = in-memory only, never here:
                             ; ephemeral uses a :memory konserve store)
            store-config
            storage          ; KonserveStorage (content-addressed, entry codec)
            dirty-atom])

(defn- live-entries
  "The currently-live entry set: adds minus removals."
  [^Registry registry]
  (set/difference (set (seq @(:adds-atom registry)))
                  (set (seq @(:removals-atom registry)))))

;; ============================================================
;; CRUD
;; ============================================================

(defn register!
  "Add a RegistryEntry (idempotent — content-addressed). Marks dirty."
  [^Registry registry entry]
  (swap! (:adds-atom registry) conj entry)
  (reset! (:dirty-atom registry) true)
  registry)

(defn register-batch!
  "Register multiple entries at once."
  [^Registry registry entries]
  (swap! (:adds-atom registry) into entries)
  (reset! (:dirty-atom registry) true)
  registry)

(defn deregister!
  "Observed-remove a RegistryEntry: tombstone it in the removals set. Convergent
   (unlike a G-Set disj). Permanent per content (a re-registered identical entry
   stays removed — the registry semantics)."
  [^Registry registry entry]
  (swap! (:removals-atom registry) conj entry)
  (reset! (:dirty-atom registry) true)
  registry)

(defn flush!
  "Persist both halves to konserve + the {:adds :removals} roots cell + freed."
  [^Registry registry]
  (when (and (:storage registry) @(:dirty-atom registry))
    (let [storage (:storage registry)
          adds-root     (pss/store @(:adds-atom registry) storage)
          removals-root (pss/store @(:removals-atom registry) storage)]
      (kb/k-assoc (:kv-store registry) roots-key
                  {:adds adds-root :removals removals-root} {:sync? true})
      (kb/k-assoc (:kv-store registry) freed-key @(:freed-atom storage) {:sync? true})
      (reset! (:dirty-atom registry) false)))
  registry)

;; ============================================================
;; Query — temporal (as-of-T)
;; ============================================================

(defn as-of
  "World state at time T: the latest live entry per [system-id branch-name] with
   HLC <= T. Result: {[system-id branch-name] -> RegistryEntry}."
  [^Registry registry hlc]
  (->> (live-entries registry)
       (filter #(<= (hlc-cmp (:hlc %) hlc) 0))
       (sort tsbs-comparator)
       (group-by (juxt :system-id :branch-name))
       (reduce-kv (fn [m k vs] (assoc m k (last vs))) {})))

(defn entries-in-range
  "All live entries with HLC between from-hlc and to-hlc (inclusive)."
  [^Registry registry from-hlc to-hlc]
  (seq (->> (live-entries registry)
            (filter #(and (<= (hlc-cmp from-hlc (:hlc %)) 0)
                          (<= (hlc-cmp (:hlc %) to-hlc) 0)))
            (sort tsbs-comparator))))

;; ============================================================
;; Query — per-system history
;; ============================================================

(defn system-history
  "History for a system+branch, newest first. opts: {:limit n :since snapshot-id}."
  ([^Registry registry system-id branch-name]
   (system-history registry system-id branch-name {}))
  ([^Registry registry system-id branch-name opts]
   (cond->> (->> (live-entries registry)
                 (filter #(and (= (:system-id %) system-id)
                               (= (:branch-name %) branch-name)))
                 (sort tsbs-comparator)
                 reverse)
     (:limit opts) (take (:limit opts))
     (:since opts) (take-while #(not= (:snapshot-id %) (:since opts))))))

(defn system-branches
  "All branches known for a given system-id."
  [^Registry registry system-id]
  (into #{} (comp (filter #(= (:system-id %) system-id)) (map :branch-name))
        (live-entries registry)))

;; ============================================================
;; Query — snapshot refs
;; ============================================================

(defn snapshot-refs
  "All live references to a given snapshot-id, or nil if none."
  [^Registry registry snapshot-id]
  (seq (filter #(= (:snapshot-id %) snapshot-id) (live-entries registry))))

(defn snapshot-systems
  "Which systems reference a given snapshot-id (set of system-id)."
  [^Registry registry snapshot-id]
  (into #{} (map :system-id) (or (snapshot-refs registry snapshot-id) [])))

;; ============================================================
;; Bulk
;; ============================================================

(defn all-entries
  "All live entries."
  [^Registry registry]
  (live-entries registry))

(defn entry-count
  "Number of live entries."
  [^Registry registry]
  (count (live-entries registry)))

(defn max-hlc
  "Maximum HLC among live entries, or nil if empty."
  [^Registry registry]
  (let [live (live-entries registry)]
    (when (seq live)
      (:hlc (last (sort-by :hlc hlc-cmp live))))))

;; ============================================================
;; Factory
;; ============================================================

(defn- ephemeral-store-config []
  {:backend :memory :id (random-uuid)})

(defn create-registry
  "Create a registry.

     {:store-config {:backend :file   :id (random-uuid) :path \"/tmp/reg\"}}
     {:store-config {:backend :memory :id (random-uuid)}}
     {:ephemeral true}   ; a fresh in-memory konserve store

   A no-arg call is ephemeral."
  ([] (create-registry {:ephemeral true}))
  ([opts]
   (let [store-config (cond
                        (:store-config opts) (:store-config opts)
                        (:store-path opts) {:backend :file :id (random-uuid)
                                            :path (:store-path opts)}
                        (:ephemeral opts) (ephemeral-store-config)
                        :else (throw (ex-info
                                      (str "Registry requires an explicit persistence choice: "
                                           "{:store-config …} | {:ephemeral true}")
                                      {:opts opts})))
         kv-store (store/open-store store-config)
         storage  (store/create-storage kv-store {:key-encode store/entry->map
                                                  :key-decode store/map->entry
                                                  :content-addressed? true})
         roots (kb/k-get kv-store roots-key {:sync? true})
         freed (or (kb/k-get kv-store freed-key {:sync? true}) {})
         _ (reset! (:freed-atom storage) freed)
         restore (fn [root] (if root (restore-set root storage) (empty-set storage)))]
     (->Registry (atom (restore (:adds roots)))
                 (atom (restore (:removals roots)))
                 kv-store store-config storage (atom false)))))

(defn close!
  "Flush and close the registry."
  [^Registry registry]
  (flush! registry)
  (when (and (:kv-store registry) (:store-config registry))
    (store/close! (:kv-store registry) (:store-config registry))))
