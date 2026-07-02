(ns ^:no-doc yggdrasil.storage
  "Persistent storage layer for the convergent CRDT catalog + snapshot registry —
   cross-platform.

   Implements PSS `IStorage` backed by konserve for durable B-tree indices. Nodes are
   stored as PSS Leaf/Branch OBJECTS and (de)serialized by the canonical, SHARED
   `org.replikativ.persistent-sorted-set.fressian` handlers attached to the konserve
   store (`attach-pss-serializer!`) — the same node wire/storage form datahike,
   proximum and stratum use, so there is one codec and no bespoke node↔map conversion
   here. A konserve MEMORY store holds the node object as-is (no serialization, no
   handlers); FILE (JVM) / IndexedDB (cljs) serialize it via those handlers. The node's
   content address is `(hasch/uuid (pss-fress/node->map node))`.

   - JVM: the Java `IStorage` interface (synchronous), konserve `{:sync? true}`.
   - cljs: PSS's cljs `IStorage` protocol (store/restore `[_ _ opts]` → a partial-cps
     `async` value).

   Registry entries are RegistryEntry records on JVM (serialized via a RegistryEntry
   fressian element handler the registry attaches alongside the node handlers) and plain
   maps on cljs (fressian-native)."
  (:require [konserve.store :as kstore]
            [konserve.core :as kc]
            [konserve.serializers :as kser]
            [hasch.core :as hasch]
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress]
            [org.replikativ.persistent-sorted-set.boundary :as pss-bnd]
            [yggdrasil.hashref :as hashref]
            [yggdrasil.kbridge :as kb]
            [is.simm.partial-cps.core-async :as ca]
            [yggdrasil.types :as t]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]])
            #?(:cljs [org.replikativ.persistent-sorted-set.impl.storage :refer [IStorage]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set IStorage Settings IBoundary])))

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
                (if (await (ca/sync-or-cps (kstore/store-exists? store-config opts) opts))
                  (await (ca/sync-or-cps (kstore/connect-store store-config opts) opts))
                  (await (ca/sync-or-cps (kstore/create-store store-config opts) opts)))))))

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

(defn node-child-addresses
  "Child addresses of a stored PSS node (Leaf → nil, Branch → its addresses).
   The `:addresses-fn` to hand `konserve-sync.walkers.pss/make-pss-sync-opts` for
   ANY store backed by `KonserveStorage`: `k/get` returns PSS Leaf/Branch OBJECTS
   here (memory holds the object as-is; file/idb deserialize back to one), so the
   walker must project via the canonical codec — bare keyword access yields nil on
   an object, collapsing the reachability walk to the root alone."
  [node]
  (when node (:addresses (pss-fress/node->map node))))

;; Nodes are stored as PSS Leaf/Branch OBJECTS, (de)serialized by the canonical
;; `org.replikativ.persistent-sorted-set.fressian` handlers attached to the konserve
;; store (see `attach-pss-serializer!`). A konserve MEMORY store holds the object as-is
;; (no serialization); FILE/IndexedDB serialize it via those handlers. The address is
;; content-derived from `pss-fress/node->map` (the node's canonical projection — a
;; Merkle hash over level/keys/addresses, so identical subtrees share an address and
;; sync incrementally), or a random UUID when `content-addressed?` is false.
;; `pending` (atom, default nil = IMMEDIATE): when the flush activates BUFFERING (resets
;; it to []), `store` accumulates `[address node]` here instead of writing — so the flush
;; can drain everything EXCEPT the fused root in one batch (root fusion). It still caches,
;; so reads-back during the flush hit the cache. Per-storage + single-writer-per-cell, so
;; no cross-op race; the flush scopes it with try/finally.
(defrecord KonserveStorage [kv-store settings cache freed-atom content-addressed? pending]
  IStorage
  #?@(:clj
      [(store [_ node]
              (let [address (if content-addressed? (hasch/uuid (pss-fress/node->map node)) (random-uuid))]
                ;; a stored node is WRITE-ONCE (its address never re-binds) → mark it
                ;; immutable so a sync peer can skip a node it already holds.
                (if (some? @pending)
                  (swap! pending conj [address node])
                  (kb/k-assoc kv-store address node kb/immutable-meta {:sync? true}))
                (cache-put! cache address node)
                address))

       (restore [_ address]
                (or (cache-lookup cache address)
                    (let [node (kb/k-get kv-store address {:sync? true})]
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
                           (let [address (if content-addressed? (hasch/uuid (pss-fress/node->map node)) (random-uuid))]
                             ;; write-once node → mark immutable (see clj branch); BUFFER when active.
                             (if (some? @pending)
                               (swap! pending conj [address node])
                               (await (kb/k-assoc kv-store address node kb/immutable-meta opts)))
                             (cache-put! cache address node)
                             address))))

       (restore [_ address opts]
                (async+sync (:sync? opts)
                            (async
                             (or (cache-lookup cache address)
                                 (let [node (await (kb/k-get kv-store address opts))]
                                   (cache-put! cache address node)
                                   node)))))

       (accessed [_ _address] nil)
       (delete [_ _addresses] nil)
       (markFreed [_ address] (when address (swap! freed-atom assoc address (t/now-ms))))
       (isFreed [_ address] (contains? @freed-atom address))
       (freedInfo [_ address] (get @freed-atom address))]))

(def ^:dynamic *mst-lzpl*
  "MST boundary parameter for the convergent CRDTs: leading-zeros-per-level (avg node
   fanout ≈ 2^lzpl). When set, `default-settings` uses a content-defined **MST** boundary
   → history-independent trees (same key set ⇒ same content-addressed root, regardless of
   insertion order) ⇒ true cross-peer root convergence + maximal node dedup. Bind to `nil`
   to fall back to the historical count B-tree (e.g. for A/B experiments)."
  6)

(defn default-settings
  "Platform-default PSS settings — the same value the storage and its serializer must
   share so reconstructed nodes match. JVM: a `Settings`; cljs: a settings map. Uses an MST
   content-defined boundary when `*mst-lzpl*` is set (the default)."
  []
  #?(:clj  (let [s (Settings.)]
             (if *mst-lzpl* (.withBoundary s (pss-bnd/mst-boundary *mst-lzpl*)) s))
     :cljs (cond-> {:branching-factor 512 :diff-buf-size 0}
             *mst-lzpl* (assoc :boundary (pss-bnd/mst-boundary *mst-lzpl*)))))

(defn settings-boundary
  "The content-defined (MST) boundary of `settings`, or nil for the count B-tree. Pass to
   `pss/sorted-set*` `:boundary` so a FRESH set splits history-independently. PSS auto-adopts
   the boundary only on RESTORE (from the root node's self-describing descriptor); for a NEW
   set there is no root to read, so the consumer MUST thread it explicitly."
  [settings]
  #?(:clj  (let [b (.boundary ^Settings settings)] (when (.contentDefined ^IBoundary b) b))
     :cljs (:boundary settings)))

(defn create-storage
  "Create a KonserveStorage backed by a konserve store.

   opts:
     :content-addressed?  when true (DEFAULT), a node's address is the hasch
       UUID of its content (a Merkle tree) — required for clean cross-peer
       merge + dedup + incremental sync. Set false for stores of heavy values
       where hashing the content outweighs the dedup win (then addresses are
       random UUIDs and only same-store structural sharing applies).
     :settings  PSS settings (default = platform default).

   Element handling: node elements (G-Set values, OR-Map triples, registry
   entries, …) are serialized by the konserve store's serializer — for durable
   backends, the canonical PSS node handlers + any consumer ELEMENT handler the
   caller attaches (e.g. a RegistryEntry handler). Memory stores hold the node
   object as-is, so no handlers are needed there."
  ([kv-store] (create-storage kv-store {}))
  ([kv-store {:keys [settings content-addressed?]
              :or {content-addressed? true}}]
   (->KonserveStorage kv-store (or settings (default-settings))
                      (atom empty-cache) (atom {}) content-addressed? (atom nil))))

(defn node-address
  "The content-address of a PSS node — the hasch of its canonical projection, i.e. the
   key it is (or would be) stored under (content-addressed stores only)."
  [node]
  (hasch/uuid (pss-fress/node->map node)))

(defn seed-node!
  "Pin `node` in `storage`'s cache under its content-address and return that address, so a
   subsequent `restore` resolves it WITHOUT a store read — the seed for a FUSED root
   (inlined in the head cell, not written to the store). The cache is bounded, so a lazily-
   restored set retained past eviction would miss it; a pinned-root restore is the robust
   form (hardening follow-up)."
  [storage node]
  (let [addr (node-address node)]
    (cache-put! (:cache storage) addr node)
    addr))

(defn attach-pss-serializer!
  "Return `kv-store` with a FressianSerializer carrying the canonical PSS node AND root
   handlers (`org.replikativ.persistent-sorted-set.fressian`, parameterized by `settings`)
   plus any consumer ELEMENT handlers, so durable backends (file / IndexedDB) (de)serialize
   the stored node OBJECTS. A memory store ignores serializers (holds objects as-is), so
   this is a harmless no-op there. `element-read-handlers` is `{tag rh}`; on the JVM
   `element-write-handlers` is `{Class {tag wh}}`, on cljs `{Type fn}` (the shapes the
   konserve FressianSerializer expects, same as the PSS node handlers).

   The root (`pss/set`) handler is included for codec COMPLETENESS/uniformity: yggdrasil
   persists a set's root as a bare content-addressed UUID (see `save-roots!`), so it never
   emits a `pss/set` itself — but registering the canonical root handler means this store
   covers EVERY PSS type exactly once (Leaf/Branch/root), so it can share a serializer/socket
   with a consumer that DOES serialize roots (datahike's fused root) without a type clash, and
   can read a `pss/set` pointer should one land here. A root read resolves storage to a
   KonserveStorage over THIS kv-store (a node address in this store resolves against this
   store); comparator defaults to nil (re-stamped lazily on descent, like every read here)."
  ([kv-store settings] (attach-pss-serializer! kv-store settings nil nil))
  ([kv-store settings element-read-handlers element-write-handlers]
   ;; This serializer is attached PER kv-store and only ever reads THIS store's blobs, so its
   ;; reconstruction scope is fixed to this store (storage over this kv-store; node `settings`;
   ;; comparator nil — re-stamped on descent). That's per-store, not a forced singleton: a
   ;; different store is a different kv-store with its OWN serializer. Cross-store / shared-wire
   ;; resolution (a value spanning several stores) is the kabel PEER serializer's job — it
   ;; resolves by `:store-id` via `pss-fress/scope-registry`, assembled separately at the wire
   ;; layer, not here. (yggdrasil's durable roots are content-addressed UUIDs, not `pss/set`
   ;; konserve values, so this root handler is for codec completeness — it doesn't fire locally.)
   ;; delayed: create-storage wraps the very kv-store we're attaching to. LEXICAL resolvers
   ;; (storage = a storage over this kv-store; no comparator/measure). bf now self-describes per
   ;; node from the blob; `default-bf` only backstops pre-bf blobs.
   (let [root-storage    (delay (create-storage kv-store {:settings settings}))
         resolve-storage (fn [_] @root-storage)
         default-bf      #?(:clj (.branchingFactor ^Settings settings) :cljs (:branching-factor settings))
         ;; element-read-handlers may be a plain `{tag rh}` map OR a FUNCTION of the
         ;; lexical `resolve-storage` — so a consumer handler (e.g. yggdrasil.fressian's
         ;; system codec) is handed the very SAME resolver the PSS root handler uses,
         ;; instead of building a redundant one. Both reconstruct against this store.
         el-read         (if (fn? element-read-handlers)
                           (element-read-handlers resolve-storage)
                           element-read-handlers)]
     (kc/assoc-serializers
      kv-store
      {:FressianSerializer
       (kser/fressian-serializer
        (merge (pss-fress/read-handlers {:default-bf default-bf})
               {pss-fress/set-tag (pss-fress/root-read-handler {:resolve-storage resolve-storage
                                                                :default-bf      default-bf})}
               hashref/read-handlers      ; hasch HashRef pointers (content-address refs)
               el-read)
        (merge pss-fress/write-handlers
               pss-fress/root-write-handlers
               hashref/write-handlers
               element-write-handlers))}))))

;; ============================================================
;; Store cleanup
;; ============================================================

(defn close!
  ([store store-config] (close! store store-config {:sync? true}))
  ([store store-config opts]
   (when (and store store-config)
     (kstore/release-store store-config store opts))))
