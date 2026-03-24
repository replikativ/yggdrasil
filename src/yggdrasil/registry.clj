(ns yggdrasil.registry
  "Cross-system snapshot registry with a single PSS index.

   Maintains one persistent-sorted-set index over RegistryEntry records,
   sorted by [hlc system-id branch-name snapshot-id] for efficient
   temporal queries (as-of-T).

   Other access patterns (per-system history, GC ref-counting) use
   full scans of the same index — fast enough for the expected scale
   (thousands of entries, not millions). Additional indices can be
   added later if profiling shows they're needed.

   Each index node is a PSS B-tree node backed by KonserveStorage
   for durable, lazy-loading from disk."
  (:require [yggdrasil.types :as t]
            [yggdrasil.storage :as store]
            [org.replikativ.persistent-sorted-set :as pss]))

;; ============================================================
;; Comparator
;; ============================================================

(defn- safe-compare
  "Compare two values, treating nil as less than any value."
  [a b]
  (cond
    (and (nil? a) (nil? b)) 0
    (nil? a) -1
    (nil? b) 1
    :else (compare a b)))

(defn- hlc-cmp
  "Compare two HLC values, nil-safe."
  [a b]
  (cond
    (and (nil? a) (nil? b)) 0
    (nil? a) -1
    (nil? b) 1
    :else (t/hlc-compare a b)))

(def ^:private tsbs-comparator
  "Sort by [hlc system-id branch-name snapshot-id]"
  (fn [a b]
    (let [c (hlc-cmp (:hlc a) (:hlc b))]
      (if-not (zero? c) c
              (let [c (safe-compare (:system-id a) (:system-id b))]
                (if-not (zero? c) c
                        (let [c (safe-compare (:branch-name a) (:branch-name b))]
                          (if-not (zero? c) c
                                  (safe-compare (:snapshot-id a) (:snapshot-id b))))))))))

;; ============================================================
;; Probe entries for range queries
;; ============================================================

(def ^:private min-hlc (t/->HLC 0 0))
(def ^:private max-hlc (t/->HLC Long/MAX_VALUE Integer/MAX_VALUE))
(def ^:private max-str "\uffff")

(defn- probe
  "Create a probe entry for range queries.
   Unspecified fields default to minimum values."
  [& {:keys [snapshot-id system-id branch-name hlc]
      :or {snapshot-id "" system-id "" branch-name "" hlc min-hlc}}]
  (t/->RegistryEntry snapshot-id system-id branch-name hlc nil nil nil))

;; ============================================================
;; Index construction
;; ============================================================

(defn- build-index
  "Build a PSS index from entries with the given comparator.
   When storage is provided, the index is backed by konserve."
  ([comparator entries]
   (into (pss/sorted-set-by comparator) entries))
  ([comparator entries storage]
   (into (pss/sorted-set* {:comparator comparator
                           :storage storage
                           :branching-factor 64})
         entries)))

;; ============================================================
;; Registry record
;; ============================================================

(defrecord Registry
           [index-atom       ; atom of PSS sorted by [hlc sys branch snap]
            kv-store         ; konserve store (or nil for in-memory only)
            store-config     ; konserve store config map (or nil for in-memory only)
            storage          ; KonserveStorage (or nil for in-memory only)
            dirty-atom])     ; atom of boolean — true if unsaved changes

;; ============================================================
;; CRUD operations
;; ============================================================

(defn register!
  "Add a RegistryEntry to the index.
   Marks registry as dirty for deferred persistence."
  [^Registry registry entry]
  (swap! (:index-atom registry) conj entry)
  (reset! (:dirty-atom registry) true)
  registry)

(defn deregister!
  "Remove a RegistryEntry from the index."
  [^Registry registry entry]
  (swap! (:index-atom registry) disj entry)
  (reset! (:dirty-atom registry) true)
  registry)

(defn flush!
  "Persist current index state to konserve.
   Stores the PSS tree and saves the root address."
  [^Registry registry]
  (when-let [storage (:storage registry)]
    (when @(:dirty-atom registry)
      (let [root (pss/store @(:index-atom registry) storage)]
        (store/save-roots! (:kv-store registry) {:tsbs root})
        ;; Persist freed addresses
        (store/save-freed! (:kv-store registry)
                           @(:freed-atom storage))
        (reset! (:dirty-atom registry) false))))
  registry)

;; ============================================================
;; Query operations — temporal (as-of-T)
;; ============================================================

(defn as-of
  "Find the world state at time T.
   Returns the latest entry per [system-id branch-name] with HLC <= T.
   Result: {[system-id branch-name] -> RegistryEntry}"
  [^Registry registry hlc]
  (let [upper (probe :hlc hlc :system-id max-str
                     :branch-name max-str :snapshot-id max-str)
        entries (pss/slice @(:index-atom registry)
                           (probe)
                           upper)]
    (->> entries
         (group-by (juxt :system-id :branch-name))
         (map (fn [[k vs]] [k (last vs)]))
         (into {}))))

(defn entries-in-range
  "Find all entries with HLC between from-hlc and to-hlc (inclusive)."
  [^Registry registry from-hlc to-hlc]
  (let [lower (probe :hlc from-hlc)
        upper (probe :hlc to-hlc :system-id max-str
                     :branch-name max-str :snapshot-id max-str)]
    (seq (pss/slice @(:index-atom registry) lower upper))))

;; ============================================================
;; Query operations — per-system history (full scan)
;; ============================================================

(defn system-history
  "Get history for a specific system and branch, newest first.
   opts: {:limit n, :since snapshot-id}"
  ([^Registry registry system-id branch-name]
   (system-history registry system-id branch-name {}))
  ([^Registry registry system-id branch-name opts]
   (let [all (seq @(:index-atom registry))]
     (cond->> (filter #(and (= (:system-id %) system-id)
                            (= (:branch-name %) branch-name))
                      all)
       true reverse
       (:limit opts) (take (:limit opts))
       (:since opts) (take-while #(not= (:snapshot-id %) (:since opts)))))))

(defn system-branches
  "List all branches known in the registry for a given system-id."
  [^Registry registry system-id]
  (into #{} (comp (filter #(= (:system-id %) system-id))
                  (map :branch-name))
        (seq @(:index-atom registry))))

;; ============================================================
;; Query operations — snapshot refs (full scan)
;; ============================================================

(defn snapshot-refs
  "Find all references to a given snapshot-id across all systems.
   Returns seq of RegistryEntry or nil if none."
  [^Registry registry snapshot-id]
  (seq (filter #(= (:snapshot-id %) snapshot-id)
               (seq @(:index-atom registry)))))

(defn snapshot-systems
  "Find which systems reference a given snapshot-id.
   Returns set of system-id strings."
  [^Registry registry snapshot-id]
  (into #{} (map :system-id) (or (snapshot-refs registry snapshot-id) [])))

;; ============================================================
;; Bulk operations
;; ============================================================

(defn all-entries
  "Return all entries in the registry."
  [^Registry registry]
  (set (seq @(:index-atom registry))))

(defn entry-count
  "Return the number of entries in the registry."
  [^Registry registry]
  (count @(:index-atom registry)))

(defn register-batch!
  "Register multiple entries at once."
  [^Registry registry entries]
  (swap! (:index-atom registry) into entries)
  (reset! (:dirty-atom registry) true)
  registry)

;; ============================================================
;; Max HLC query
;; ============================================================

(defn max-hlc
  "Return the maximum HLC in the registry, or nil if empty.
   O(1) since the PSS index is sorted by HLC first — the last
   entry has the highest HLC."
  [^Registry registry]
  (let [idx @(:index-atom registry)]
    (when (pos? (count idx))
      (:hlc (last (seq idx))))))

;; ============================================================
;; Factory
;; ============================================================

(defn create-registry
  "Create a new registry.

   Requires an explicit persistence choice:
     {:store-config {:backend :file :id #uuid \"...\" :path \"/tmp/reg\"}}
     {:store-config {:backend :memory :id #uuid \"...\"}}
     {:ephemeral true}

   For backward compatibility, a no-arg call creates an ephemeral registry.
   However, callers should prefer the explicit {:ephemeral true} form."
  ([] (create-registry {:ephemeral true}))
  ([opts]
   (cond
     ;; Persistent registry with konserve store config
     (:store-config opts)
     (let [store-config (:store-config opts)
           kv-store (store/open-store store-config)
           storage (store/create-storage kv-store)
           roots (store/load-roots kv-store)
           freed (store/load-freed kv-store)
           _ (reset! (:freed-atom storage) freed)]
       (if roots
         ;; Restore from existing root
         (let [idx (pss/restore-by tsbs-comparator (:tsbs roots) storage
                                   {:branching-factor 64})]
           (->Registry (atom idx) kv-store store-config storage (atom false)))
         ;; Fresh registry with storage
         (let [idx (build-index tsbs-comparator [] storage)]
           (->Registry (atom idx) kv-store store-config storage (atom false)))))

     ;; Explicit ephemeral
     (:ephemeral opts)
     (let [idx (build-index tsbs-comparator [])]
       (->Registry (atom idx) nil nil nil (atom false)))

     ;; Legacy: :store-path sugar — convert to file store config
     (:store-path opts)
     (create-registry {:store-config {:backend :file
                                      :id (java.util.UUID/randomUUID)
                                      :path (:store-path opts)}})

     :else
     (throw (ex-info
             (str "Registry requires explicit persistence choice.\n"
                  "  {:store-config {:backend :file :id (random-uuid) :path \"/tmp/reg\"}}\n"
                  "  {:store-config {:backend :memory :id (random-uuid)}}\n"
                  "  {:ephemeral true}")
             {:opts opts})))))

(defn close!
  "Flush and close the registry."
  [^Registry registry]
  (flush! registry)
  (when (and (:kv-store registry) (:store-config registry))
    (store/close! (:kv-store registry) (:store-config registry))))
