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
            [hasch.core :as hasch]
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
  "Open or create a konserve store from a store-config map. async+sync: sync on
   JVM ({:sync? true}); on cljs pass {:sync? false} → a partial-cps async value
   (`await` it). The `store-exists?` check is itself async on cljs, so it must be
   awaited before branching (a bare `if` over the channel is always truthy)."
  ([store-config] (open-store store-config {:sync? true}))
  ([store-config opts]
   (async+sync (:sync? opts)
               (async
                (if (await (kb/sync-or-cps (kstore/store-exists? store-config opts) opts))
                  (await (kb/sync-or-cps (kstore/connect-store store-config opts) opts))
                  (await (kb/sync-or-cps (kstore/create-store store-config opts) opts)))))))

;; ============================================================
;; Record <-> map conversion for safe serialization (JVM records only)
;; ============================================================

;; The registry's element codec: RegistryEntry is a record, converted to/from a
;; plain map at the node-key boundary so konserve round-trips it without
;; fressian/transit handlers. Other element types (keywords, strings, vectors —
;; e.g. a durable G-Set) are konserve-native and use the identity codec; see
;; `create-storage`. Public + cljc so the registry can opt into them and cljs
;; compiles the reference.
(defn entry->map [entry]
  (when entry
    {:snapshot-id  (:snapshot-id entry)
     :system-id    (:system-id entry)
     :branch-name  (:branch-name entry)
     :hlc          (when-let [h (:hlc entry)]
                     {:physical (:physical h) :logical (:logical h)})
     :content-hash (:content-hash entry)
     :parent-ids   (:parent-ids entry)
     :metadata     (:metadata entry)}))

(defn map->entry [m]
  (when m
    (t/->RegistryEntry
     (:snapshot-id m) (:system-id m) (:branch-name m)
     (when-let [h (:hlc m)] (t/->HLC (:physical h) (:logical h)))
     (:content-hash m) (:parent-ids m) (:metadata m))))

;; ============================================================
;; KonserveStorage — IStorage for PSS B-tree nodes
;; ============================================================

(def ^:private cache-limit
  "Max decoded PSS nodes held in a storage's in-memory node cache. Bounds memory
   for read-on-the-fly access (datahike-style): hot nodes stay resident, cold ones
   are evicted and re-fetched+decoded from konserve on demand. ~100k nodes."
  100000)

;; Insertion-recency LRU node cache — the `datahike.lru` pattern, inlined as a
;; functional value over a plain map (no deftype / core.cache: core.cache's
;; protocol machinery is JVM-only, and datahike itself hand-rolls this for the
;; same cross-platform node cache). `:kv` is address→node; `:gen->key` (a
;; sorted-map) + `:key->gen` track INSERTION order via a monotonic `:gen`. A
;; lookup does NOT bump recency (a B-tree's hot upper nodes stay resident under
;; any policy, and the read/cache-hit path stays allocation-free); `cache-put!`
;; bumps the gen and evicts the lowest gen (oldest-inserted) when over the limit.
(def ^:private empty-cache {:kv {} :gen->key (sorted-map) :key->gen {} :gen 0})

(defn- cache-lookup [cache address]
  (get (:kv @cache) address))

(defn- cache-put! [cache address node]
  (swap! cache
         (fn [{:keys [kv gen->key key->gen gen]}]
           (let [prev (get key->gen address)              ; re-put? drop its old gen slot
                 c {:kv       (assoc kv address node)
                    :gen->key (assoc (cond-> gen->key prev (dissoc prev)) gen address)
                    :key->gen (assoc key->gen address gen)
                    :gen      (inc gen)}]
             (if (> (count (:kv c)) cache-limit)
               (let [[lo k] (first (:gen->key c))]         ; lowest gen = oldest-inserted
                 (-> c (update :kv dissoc k)
                       (update :gen->key dissoc lo)
                       (update :key->gen dissoc k)))
               c)))))

(defrecord KonserveStorage [kv-store settings cache freed-atom key-encode key-decode content-addressed?]
  IStorage
  #?@(:clj
      [(store [_ node]
         (let [^ANode node node
               node-data {:level     (.level node)
                          :keys      (mapv key-encode (.keys node))
                          :addresses (when (instance? Branch node)
                                       (vec (.addresses ^Branch node)))}
               ;; content-addressed: the address IS the hasch UUID of the node
               ;; content. Branch content includes child addresses (themselves
               ;; content hashes) ⇒ a Merkle tree: identical subtrees share an
               ;; address, so successive versions ship incrementally and peers
               ;; dedup. Else a random UUID (registry default — unchanged).
               address (if content-addressed? (hasch/uuid node-data) (random-uuid))]
           (kb/k-assoc kv-store address node-data {:sync? true})
           (cache-put! cache address node)
           address))

       (restore [_ address]
         (or (cache-lookup cache address)
             (let [node-data (kb/k-get kv-store address {:sync? true})
                   keys (mapv key-decode (:keys node-data))
                   addresses (:addresses node-data)
                   node (if addresses
                          (Branch. (int (:level node-data))
                                   ^java.util.List keys
                                   ^java.util.List (vec addresses)
                                   settings)
                          (Leaf. ^java.util.List keys settings))]
               (cache-put! cache address node)
               node)))

       (accessed [_ _address] nil)
       (markFreed [_ address] (when address (swap! freed-atom assoc address (t/now-ms))))
       (isFreed [_ address] (contains? @freed-atom address))
       (freedInfo [_ address] (get @freed-atom address))]

      :cljs
      [(store [_ node opts]
         (async+sync (:sync? opts)
                     (async
                      (let [node-data {:level     (node/level node)
                                       :keys      (mapv key-encode (.-keys node))
                                       :addresses (when (instance? Branch node)
                                                    (vec (.-addresses node)))}
                            address (if content-addressed? (hasch/uuid node-data) (random-uuid))]
                        (await (kb/k-assoc kv-store address node-data opts))
                        (cache-put! cache address node)
                        address))))

       (restore [_ address opts]
         (async+sync (:sync? opts)
                     (async
                      (or (cache-lookup cache address)
                          ;; PSS cljs nodes hold their `keys`/`addresses` as JS
                          ;; ARRAYS (`.slice`/`aget`/`aconcat` are used on them).
                          ;; konserve round-trips them as Clojure vectors, so the
                          ;; cache-MISS rebuild MUST `to-array` them — else the
                          ;; reconstructed Leaf/Branch iterates a cljs vector via
                          ;; JS-array ops and silently reads as empty. (The
                          ;; cache-HIT path returns the original PSS node, so this
                          ;; only bites a true cache miss: ship-to-fresh-store or
                          ;; reopen — caught by the cross-peer/merge-peer! tests.)
                          (let [node-data (await (kb/k-get kv-store address opts))
                                node (if (:addresses node-data)
                                       (branch/from-map (assoc node-data
                                                               :keys (to-array (mapv key-decode (:keys node-data)))
                                                               :addresses (to-array (:addresses node-data))
                                                               :settings settings))
                                       (Leaf. (to-array (mapv key-decode (:keys node-data))) settings (:measure node-data)))]
                            (cache-put! cache address node)
                            node)))))

       (accessed [_ _address] nil)
       (delete [_ _addresses] nil)
       (markFreed [_ address] (when address (swap! freed-atom assoc address (t/now-ms))))
       (isFreed [_ address] (contains? @freed-atom address))
       (freedInfo [_ address] (get @freed-atom address))]))

(defn create-storage
  "Create a KonserveStorage backed by a konserve store.

   opts:
     :key-encode / :key-decode  transcode element values at the node-key
       boundary (default identity — for konserve-native values like
       keywords/strings/vectors, e.g. a durable G-Set). The registry opts into
       `entry->map`/`map->entry`.
     :content-addressed?  when true (DEFAULT), a node's address is the hasch
       UUID of its content (a Merkle tree) — required for clean cross-peer
       merge + dedup + incremental sync. Set false for stores of heavy values
       where hashing the content outweighs the dedup win (then addresses are
       random UUIDs and only same-store structural sharing applies).
     :settings  PSS settings (default = platform default)."
  ([kv-store] (create-storage kv-store {}))
  ([kv-store {:keys [settings key-encode key-decode content-addressed?]
              :or {content-addressed? true}}]
   ;; coerce in the body (not via :or) so an explicit nil — e.g. from
   ;; durable/open threading {:key-encode nil} — still defaults to identity.
   (->KonserveStorage kv-store
                      (or settings #?(:clj (Settings.) :cljs {:branching-factor 512 :diff-buf-size 0}))
                      (atom empty-cache) (atom {})
                      (or key-encode identity) (or key-decode identity) content-addressed?)))

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
