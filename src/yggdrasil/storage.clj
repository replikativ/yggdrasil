(ns yggdrasil.storage
  "Persistent storage layer for the snapshot registry.

   Implements PSS IStorage backed by konserve for durable B-tree indices.
   Each PSS node (Leaf or Branch) is serialized and stored in konserve
   under a UUID address. Nodes are loaded lazily on access.

   RegistryEntry and HLC records are converted to plain maps at the
   serialization boundary to avoid incognito registration requirements.
   This ensures correct round-tripping through konserve's serializer.

   Key namespace prefixes in the store:
     :registry/roots   - root addresses of the three indices
     :registry/freed   - map of freed addresses to timestamps"
  (:require [konserve.core :as k]
            [konserve.store :as kstore]
            [clojure.core.async :refer [<!!]]
            [yggdrasil.types :as t])
  (:import [org.replikativ.persistent_sorted_set
            ANode Branch IStorage Leaf Settings]))

;; ============================================================
;; Store lifecycle
;; ============================================================

(defn open-store
  "Open or create a konserve store from a store-config map.

   Uses the konserve unified store factory (konserve.store).
   Tries connect-store first (existing store); falls back to
   create-store if the store does not exist yet.

   store-config must include :backend and :id, e.g.:
     {:backend :file :id #uuid \"...\" :path \"/tmp/yggdrasil\"}
     {:backend :memory :id #uuid \"...\"}

   Returns a konserve store instance."
  [store-config]
  (let [opts {:sync? true}]
    (if (kstore/store-exists? store-config opts)
      (kstore/connect-store store-config opts)
      (kstore/create-store store-config opts))))

;; ============================================================
;; Record ↔ map conversion for safe serialization
;; ============================================================

(defn- entry->map
  "Convert a RegistryEntry record to a plain map for serialization."
  [entry]
  (when entry
    {:snapshot-id  (:snapshot-id entry)
     :system-id    (:system-id entry)
     :branch-name  (:branch-name entry)
     :hlc          (when-let [h (:hlc entry)]
                     {:physical (:physical h) :logical (:logical h)})
     :content-hash (:content-hash entry)
     :parent-ids   (:parent-ids entry)
     :metadata     (:metadata entry)}))

(defn- map->entry
  "Reconstruct a RegistryEntry record from a deserialized map."
  [m]
  (when m
    (t/->RegistryEntry
     (:snapshot-id m)
     (:system-id m)
     (:branch-name m)
     (when-let [h (:hlc m)]
       (t/->HLC (:physical h) (:logical h)))
     (:content-hash m)
     (:parent-ids m)
     (:metadata m))))

;; ============================================================
;; KonserveStorage — IStorage for PSS B-tree nodes
;; ============================================================

(defrecord KonserveStorage [kv-store ^Settings settings cache freed-atom]
  IStorage
  (store [_ node]
    (let [^ANode node node
          address (random-uuid)
          node-data {:level     (.level node)
                     :keys      (mapv entry->map (.keys node))
                     :addresses (when (instance? Branch node)
                                  (vec (.addresses ^Branch node)))}]
      (<!! (k/assoc kv-store address node-data))
      (swap! cache assoc address node)
      address))

  (restore [_ address]
    (or (get @cache address)
        (let [node-data (<!! (k/get kv-store address))
              raw-keys (:keys node-data)
              keys (mapv map->entry raw-keys)
              addresses (:addresses node-data)
              node (if addresses
                     (Branch. (int (:level node-data))
                              ^java.util.List keys
                              ^java.util.List (vec addresses)
                              settings)
                     (Leaf. ^java.util.List keys settings))]
          (swap! cache assoc address node)
          node)))

  (accessed [_ address]
    nil)

  (markFreed [_ address]
    (when address
      (swap! freed-atom assoc address (System/currentTimeMillis))))

  (isFreed [_ address]
    (contains? @freed-atom address))

  (freedInfo [_ address]
    (get @freed-atom address)))

(defn create-storage
  "Create a KonserveStorage backed by a konserve store.
   The Settings object configures the PSS branching factor."
  ([kv-store]
   (create-storage kv-store (Settings.)))
  ([kv-store ^Settings settings]
   (->KonserveStorage kv-store settings (atom {}) (atom {}))))

;; ============================================================
;; Index root persistence
;; ============================================================

(defn save-roots!
  "Persist the root addresses of the indices."
  [kv-store roots]
  (<!! (k/assoc kv-store :registry/roots roots)))

(defn load-roots
  "Load the root addresses of the indices.
   Returns nil if no roots stored."
  [kv-store]
  (<!! (k/get kv-store :registry/roots)))

;; ============================================================
;; Freed node tracking
;; ============================================================

(defn save-freed!
  "Persist freed node addresses with their timestamps."
  [kv-store freed]
  (<!! (k/assoc kv-store :registry/freed freed)))

(defn load-freed
  "Load freed node addresses."
  [kv-store]
  (or (<!! (k/get kv-store :registry/freed)) {}))

(defn sweep-freed!
  "Delete freed nodes older than grace-period-ms from konserve.
   Returns the number of swept addresses."
  [kv-store freed-atom grace-period-ms]
  (let [now (System/currentTimeMillis)
        cutoff (- now grace-period-ms)
        freed @freed-atom
        to-sweep (filter (fn [[_ ts]] (< ts cutoff)) freed)]
    (doseq [[address _] to-sweep]
      (<!! (k/dissoc kv-store address)))
    (swap! freed-atom #(apply dissoc % (map first to-sweep)))
    (count to-sweep)))

;; ============================================================
;; Store cleanup
;; ============================================================

(defn close!
  "Close the store via konserve release.
   Safe to call on any store type."
  [store store-config]
  (when (and store store-config)
    (kstore/release-store store-config store {:sync? true})))
