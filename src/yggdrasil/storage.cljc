(ns yggdrasil.storage
  "Persistent storage layer for the snapshot registry — cross-platform.

   Implements PSS IStorage backed by konserve for durable B-tree indices. Each
   PSS node (Leaf or Branch) is serialized as a PLAIN MAP
   ({:level :keys :addresses}); konserve round-trips plain maps natively on both
   platforms, so the format is portable JVM<->cljs with NO fressian/transit
   handlers (unlike datahike, which serializes node objects and needs handlers).
   The only platform-specific part is reconstructing the node from the map.

   - JVM: implements the Java `IStorage` interface (synchronous restore(addr),
     reconstructed via the Java Branch./Leaf. constructors). Konserve via
     {:sync? true}.
   - cljs: implements PSS's cljs `IStorage` protocol (store/restore [_ _ opts] →
     a partial-cps `async` value via `async+sync` + the konserve→partial-cps
     bridge), reconstructed via `branch/from-map` / `Leaf.`.

   Registry entries are RegistryEntry records on JVM (converted to/from maps at
   the storage boundary) and plain maps on cljs; the stored representation is a
   map either way."
  (:require [konserve.store :as kstore]
            [yggdrasil.kbridge :as kb]
            [yggdrasil.types :as t]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]])
            #?@(:cljs [[org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]
                       [org.replikativ.persistent-sorted-set.impl.node :as node]
                       [org.replikativ.persistent-sorted-set.leaf :refer [Leaf]]
                       [org.replikativ.persistent-sorted-set.branch :refer [Branch] :as branch]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set
                    ANode Branch IStorage Leaf Settings])))

;; ============================================================
;; Store lifecycle
;; ============================================================

(defn open-store
  "Open or create a konserve store from a store-config map. Sync on JVM; on cljs
   pass {:sync? false} (returns a channel)."
  ([store-config] (open-store store-config {:sync? true}))
  ([store-config opts]
   (if (kstore/store-exists? store-config opts)
     (kstore/connect-store store-config opts)
     (kstore/create-store store-config opts))))

;; ============================================================
;; Record <-> map conversion for safe serialization (JVM records only)
;; ============================================================

#?(:clj
   (defn- entry->map [entry]
     (when entry
       {:snapshot-id  (:snapshot-id entry)
        :system-id    (:system-id entry)
        :branch-name  (:branch-name entry)
        :hlc          (when-let [h (:hlc entry)]
                        {:physical (:physical h) :logical (:logical h)})
        :content-hash (:content-hash entry)
        :parent-ids   (:parent-ids entry)
        :metadata     (:metadata entry)})))

#?(:clj
   (defn- map->entry [m]
     (when m
       (t/->RegistryEntry
        (:snapshot-id m) (:system-id m) (:branch-name m)
        (when-let [h (:hlc m)] (t/->HLC (:physical h) (:logical h)))
        (:content-hash m) (:parent-ids m) (:metadata m)))))

;; ============================================================
;; KonserveStorage — IStorage for PSS B-tree nodes
;; ============================================================

(defrecord KonserveStorage [kv-store settings cache freed-atom]
  IStorage
  #?@(:clj
      [(store [_ node]
         (let [^ANode node node
               address (random-uuid)
               node-data {:level     (.level node)
                          :keys      (mapv entry->map (.keys node))
                          :addresses (when (instance? Branch node)
                                       (vec (.addresses ^Branch node)))}]
           (kb/k-assoc kv-store address node-data {:sync? true})
           (swap! cache assoc address node)
           address))

       (restore [_ address]
         (or (get @cache address)
             (let [node-data (kb/k-get kv-store address {:sync? true})
                   keys (mapv map->entry (:keys node-data))
                   addresses (:addresses node-data)
                   node (if addresses
                          (Branch. (int (:level node-data))
                                   ^java.util.List keys
                                   ^java.util.List (vec addresses)
                                   settings)
                          (Leaf. ^java.util.List keys settings))]
               (swap! cache assoc address node)
               node)))

       (accessed [_ _address] nil)
       (markFreed [_ address] (when address (swap! freed-atom assoc address (t/now-ms))))
       (isFreed [_ address] (contains? @freed-atom address))
       (freedInfo [_ address] (get @freed-atom address))]

      :cljs
      [(store [_ node opts]
         (async+sync (:sync? opts)
                     (async
                      (let [address (random-uuid)
                            node-data {:level     (node/level node)
                                       :keys      (vec (.-keys node))
                                       :addresses (when (instance? Branch node)
                                                    (vec (.-addresses node)))}]
                        (await (kb/k-assoc kv-store address node-data opts))
                        (swap! cache assoc address node)
                        address))))

       (restore [_ address opts]
         (async+sync (:sync? opts)
                     (async
                      (or (get @cache address)
                          (let [node-data (await (kb/k-get kv-store address opts))
                                node (if (:addresses node-data)
                                       (branch/from-map (assoc node-data :settings settings))
                                       (Leaf. (:keys node-data) settings (:measure node-data)))]
                            (swap! cache assoc address node)
                            node)))))

       (accessed [_ _address] nil)
       (delete [_ _addresses] nil)
       (markFreed [_ address] (when address (swap! freed-atom assoc address (t/now-ms))))
       (isFreed [_ address] (contains? @freed-atom address))
       (freedInfo [_ address] (get @freed-atom address))]))

(defn create-storage
  "Create a KonserveStorage backed by a konserve store."
  ([kv-store]
   (create-storage kv-store #?(:clj (Settings.) :cljs {:branching-factor 512 :diff-buf-size 0})))
  ([kv-store settings]
   (->KonserveStorage kv-store settings (atom {}) (atom {}))))

;; ============================================================
;; Index root + freed persistence — async+sync (sync on JVM, async on cljs)
;; ============================================================

(defn save-roots!
  ([kv-store roots] (save-roots! kv-store roots {:sync? true}))
  ([kv-store roots opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-assoc kv-store :registry/roots roots opts))))))

(defn load-roots
  ([kv-store] (load-roots kv-store {:sync? true}))
  ([kv-store opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-get kv-store :registry/roots opts))))))

(defn save-freed!
  ([kv-store freed] (save-freed! kv-store freed {:sync? true}))
  ([kv-store freed opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-assoc kv-store :registry/freed freed opts))))))

(defn load-freed
  ([kv-store] (load-freed kv-store {:sync? true}))
  ([kv-store opts]
   (async+sync (:sync? opts)
               (async (or (await (kb/k-get kv-store :registry/freed opts)) {})))))

(defn sweep-freed!
  "Delete freed nodes older than grace-period-ms. JVM/sync only (GC is a
   server-side concern)."
  [kv-store freed-atom grace-period-ms]
  (let [now (t/now-ms)
        cutoff (- now grace-period-ms)
        to-sweep (filter (fn [[_ ts]] (< ts cutoff)) @freed-atom)]
    (doseq [[address _] to-sweep]
      (kb/k-dissoc kv-store address {:sync? true}))
    (swap! freed-atom #(apply dissoc % (map first to-sweep)))
    (count to-sweep)))

;; ============================================================
;; Store cleanup
;; ============================================================

(defn close!
  ([store store-config] (close! store store-config {:sync? true}))
  ([store store-config opts]
   (when (and store store-config)
     (kstore/release-store store-config store opts))))
