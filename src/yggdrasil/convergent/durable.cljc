(ns yggdrasil.convergent.durable
  "Reusable DURABLE backing for conflict-free systems — the registry's
   PSS+konserve+root-cell+reachability pattern, lifted out so the catalog can
   share it.

   The snapshot registry IS a durable grow-only set of content-addressed DAG
   nodes (PSS over KonserveStorage, a root cell, synced by a reachability walk).
   This namespace factors that substrate so a durable G-Set / OR-Map is a few
   lines on top of it, and proves the unification: the registry was an instance
   of this all along.

   Storage layout in a (per-system) konserve store:
     <uuid>        -> a PSS B-tree node {:level :keys :addresses}  (KonserveStorage)
     :crdt/roots   -> {branch -> root-address}  (the live heads — one cell)
     :registry/freed -> {address -> ts}         (markFreed bookkeeping; reused)

   delta-first: `reachable-addresses` is the ship-set — the SAME value that is
   (a) konserve-sync's incremental transport set and (b) the basis of the
   element-level join-delta. `ship!` is the store-to-store sync primitive; it
   copies only the nodes the destination is missing (incremental).

   JVM-only for now (synchronous konserve {:sync? true}); the cljs durable path
   would thread storage.cljc's async+sync — deferred. See doc gaps."
  (:require [yggdrasil.kbridge :as kb]
            [yggdrasil.storage :as store]
            [org.replikativ.persistent-sorted-set :as pss]))

(def ^:private roots-key :crdt/roots)
(def ^:private freed-key :crdt/freed)
(def ^:private branching-factor 64)

;; ============================================================
;; Store lifecycle
;; ============================================================

(defn open
  "Open the konserve store for a durable CRDT and load its freed-set into a
   fresh content-addressed KonserveStorage. Returns {:kv-store :store-config
   :storage}.

   opts may carry `:key-encode`/`:key-decode` — a node-key element codec (default
   identity). Bare-element CRDTs (G-Set/2P-Set of records) pass the entry codec
   here so records round-trip; element-pair CRDTs leave it identity."
  ([store-config] (open store-config {}))
  ([store-config {:keys [key-encode key-decode]}]
   (let [kv-store (store/open-store store-config)
         storage  (store/create-storage kv-store {:content-addressed? true
                                                  :key-encode key-encode
                                                  :key-decode key-decode})
         freed    (or (kb/k-get kv-store freed-key {:sync? true}) {})]
     (reset! (:freed-atom storage) freed)
     {:kv-store kv-store :store-config store-config :storage storage})))

;; ============================================================
;; PSS set helpers
;; ============================================================

(defn empty-set
  "An empty PSS sorted-set backed by `storage`."
  [storage comparator]
  (pss/sorted-set* {:comparator comparator
                    :storage storage
                    :branching-factor branching-factor}))

(defn store-set!
  "Persist `s` to its storage and return the root address."
  [s storage]
  (pss/store s storage))

(defn restore-set
  "Lazily restore a PSS set from `root` under `comparator`+`storage`."
  [comparator root storage]
  (pss/restore-by comparator root storage {:branching-factor branching-factor}))

;; ============================================================
;; Root cell + freed persistence
;; ============================================================

(defn load-roots
  "Read the {branch -> root-address} cell, or nil when none yet."
  [kv-store]
  (kb/k-get kv-store roots-key {:sync? true}))

(defn save-roots!
  "Write the {branch -> root-address} cell."
  [kv-store roots]
  (kb/k-assoc kv-store roots-key roots {:sync? true}))

(defn save-freed!
  "Persist a KonserveStorage's freed-set under the CRDT freed cell."
  [kv-store storage]
  (kb/k-assoc kv-store freed-key @(:freed-atom storage) {:sync? true}))

;; ============================================================
;; Reachability walk — the ship-set (delta-first sync primitive)
;; ============================================================

(defn reachable-addresses
  "Every node address reachable from `root` in `kv-store` (root + transitive
   `:addresses`). This IS the ship-set: the nodes a peer must hold to restore
   the set rooted at `root`. konserve-sync computes the same set structurally;
   here it's a direct walk so the durable layer is sync-provable on its own."
  [kv-store root]
  (loop [to-visit [root] seen #{}]
    (if-let [addr (first to-visit)]
      (if (contains? seen addr)
        (recur (rest to-visit) seen)
        (let [node (kb/k-get kv-store addr {:sync? true})]
          (recur (into (vec (rest to-visit)) (:addresses node))
                 (conj seen addr))))
      seen)))

(defn ship!
  "Copy every node reachable from `root` in `src-store` that `dst-store` is
   MISSING. Returns the count copied — incremental: 0 when already in sync.
   The durable G-Set's sync primitive (the store-to-store form of -join's
   transport). Idempotent: re-shipping copies nothing."
  [src-store dst-store root]
  (reduce (fn [n addr]
            (if (some? (kb/k-get dst-store addr {:sync? true}))
              n
              (do (kb/k-assoc dst-store addr
                              (kb/k-get src-store addr {:sync? true})
                              {:sync? true})
                  (inc n))))
          0
          (reachable-addresses src-store root)))
