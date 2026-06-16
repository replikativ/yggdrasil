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
     :crdt/freed   -> {address -> ts}           (GC bookkeeping)

   **Cross-platform via `async+sync`** (mirrors persistent-sorted-set): every
   storage-touching fn takes `opts` (default `{:sync? true}`) and is written once
   with partial-cps `async`/`await`; the macro emits a SYNC body (JVM, values)
   and an ASYNC body (cljs, CPS over konserve channels). JVM callers are
   behaviour-identical to before; a browser passes `{:sync? false}` and `await`s.

   delta-first: `reachable-addresses` is the ship-set — the SAME value that is
   (a) konserve-sync's incremental transport set and (b) the basis of the
   element-level join-delta. `ship!` is the store-to-store sync primitive; it
   copies only the nodes the destination is missing (incremental)."
  (:require [yggdrasil.kbridge :as kb]
            [yggdrasil.storage :as store]
            [yggdrasil.types :as t]
            [konserve.gc :as kgc]
            [hasch.core :as hasch]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]])
            [org.replikativ.persistent-sorted-set :as pss]
            #?(:cljs [is.simm.partial-cps.sequence :as aseq]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(def ^:private roots-key :crdt/roots)
(def ^:private freed-key :crdt/freed)
(def ^:private branching-factor 64)

;; When several CRDTs share ONE konserve store (the single-causal-root composite,
;; option a — `:composite/root` as the lone gate, like datahike's `:db`), each
;; needs its OWN roots/freed cells so they don't clobber each other. Pass
;; `:roots-key`/`:freed-key` in opts (e.g. `[:crdt/roots sub-id]`); content-
;; addressed nodes (uuid keys) are still safely shared (dedup across subs).
;; Default = the single-store cells, so every existing CRDT is unchanged.
(defn- rk [opts] (:roots-key opts roots-key))
(defn- fk [opts] (:freed-key opts freed-key))

;; ============================================================
;; Store lifecycle
;; ============================================================

(defn mem-store-config
  "A fresh in-memory konserve store-config — the default backing when a CRDT is
   constructed WITHOUT an explicit `:store-config`/`:kv-store`. The 'memory'
   variant is just the durable CRDT over a memory store (read on the fly via the
   PSS + node-cache, no separate in-memory representation)."
  []
  {:backend :memory :id (random-uuid)})

(defn open
  "Open the konserve store for a durable CRDT and load its freed-set into a
   fresh content-addressed KonserveStorage. Returns (async+sync)
   {:kv-store :store-config :storage}.

   opts may carry `:key-encode`/`:key-decode` — a node-key element codec (default
   identity). Bare-element CRDTs (G-Set/2P-Set of records) pass the entry codec
   here so records round-trip; element-pair CRDTs leave it identity."
  ([store-config] (open store-config {:sync? true}))
  ([store-config opts]
   (let [opts (merge {:sync? true} opts)     ; codec-only callers default to sync
         {:keys [key-encode key-decode]} opts]
     (async+sync (:sync? opts)
                 (async
                  (let [;; reuse a pre-opened store (the single-store composite passes
                        ;; its own kv-store so subs co-habit it) or open store-config.
                        kv-store (or (:kv-store opts) (await (store/open-store store-config opts)))
                        storage  (store/create-storage kv-store {:content-addressed? true
                                                                 :key-encode key-encode
                                                                 :key-decode key-decode})
                        freed    (or (await (kb/k-get kv-store (fk opts) opts)) {})]
                    (reset! (:freed-atom storage) freed)
                    {:kv-store kv-store :store-config store-config :storage storage}))))))

;; ============================================================
;; PSS set helpers
;; ============================================================

(defn empty-set
  "An empty PSS sorted-set backed by `storage` (pure — no storage IO)."
  [storage comparator]
  (pss/sorted-set* {:comparator comparator
                    :storage storage
                    :branching-factor branching-factor}))

(defn store-set!
  "Persist `s` to its storage and return its root address (async+sync — PSS
   writes nodes to konserve)."
  ([s storage] (store-set! s storage {:sync? true}))
  ([s storage opts]
   #?(:clj  (pss/store s storage)            ; JVM IStorage is synchronous
      :cljs (pss/store s storage opts))))

(defn restore-set
  "Build a LAZY PSS set rooted at `root` (sync — traversal loads nodes lazily,
   each op then threads its own `opts`).

   JVM uses `restore-by` (custom comparator + bare address). cljs uses the public
   `restore` with a `{:root-address :comparator}` info-map — PSS's `restore-by`
   is JVM-only, and cljs `restore` builds the BTSet with count -1 (lazily filled),
   which is fine for our drain/`contains?` reads (we never read the cached count;
   datahike stores it only for accurate `(count db)`)."
  ([comparator root storage] (restore-set comparator root storage {:sync? true}))
  ([comparator root storage opts]
   #?(:clj  (pss/restore-by comparator root storage (assoc opts :branching-factor branching-factor))
      :cljs (pss/restore {:root-address root :comparator comparator :branching-factor branching-factor}
                         storage opts))))

;; ---- cross-platform PSS element ops (shared by the catalog) ----------------

(defn set->clj
  "Materialize a (possibly lazy) PSS set into a Clojure set. (async+sync). On
   cljs a storage-backed set is drained one node at a time via its AsyncSeq (the
   partial-cps sequence protocol PSS itself uses)."
  [s opts]
  (async+sync (:sync? opts)
              (async
               (if (nil? s) #{}
                   #?(:clj  (into #{} s)
                      :cljs (loop [items (await (pss/seq s opts)) acc (transient #{})]
                              (if-some [x (await (aseq/first items))]
                                (recur (await (aseq/rest items)) (conj! acc x))
                                (persistent! acc))))))))

(defn slice->clj
  "Materialize the INCLUSIVE PSS range `[from to]` into a Clojure vector — reads
   ONLY the nodes the range spans (lazy traversal + the storage node-cache), NOT a
   full drain. This is the datahike-style read-on-the-fly primitive: a key-scoped
   read touches O(log n + matches) nodes, not the whole set. (async+sync). On cljs
   the slice yields an AsyncSeq drained one node at a time (the same partial-cps
   sequence protocol `set->clj` uses); the 4-arity `pss/slice` passes the set's own
   comparator and threads `opts` (`:sync? false` ⇒ CPS)."
  [s from to opts]
  (async+sync (:sync? opts)
              (async
               (if (nil? s) []
                   #?(:clj  (into [] (pss/slice s from to))
                      :cljs (loop [items (await (pss/slice s from to opts)) acc (transient [])]
                              (if-some [x (await (aseq/first items))]
                                (recur (await (aseq/rest items)) (conj! acc x))
                                (persistent! acc))))))))

(defn set-conj
  "conj `x` onto PSS set `s` under comparator `cmp`. (async+sync)"
  [s x cmp opts]
  (async+sync (:sync? opts)
              (async
               #?(:clj  (conj s x)
                  :cljs (await (pss/conj s x cmp opts))))))

(defn set-contains?
  "Whether PSS set `s` contains `x`. (async+sync)"
  [s x opts]
  (async+sync (:sync? opts)
              (async
               (if (nil? s) false
                   #?(:clj  (contains? s x)
                      :cljs (await (pss/contains? s x opts)))))))

(defn set-union
  "Union PSS set `b` into `a` (conj every element of `b`, ordered by `cmp`).
   The cross-platform core of -join / merge / merge-peer!. (async+sync)"
  [a b cmp opts]
  (async+sync (:sync? opts)
              (async
               #?(:clj  (into a (seq b))
                  :cljs (loop [items (await (pss/seq b opts)) acc a]
                          (if-some [x (await (aseq/first items))]
                            (recur (await (aseq/rest items)) (await (pss/conj acc x cmp opts)))
                            acc))))))

;; ============================================================
;; Root cell + freed persistence
;; ============================================================

(defn load-roots
  "Read the {branch -> root-address} cell, or nil when none yet (async+sync)."
  ([kv-store] (load-roots kv-store {:sync? true}))
  ([kv-store opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-get kv-store (rk opts) opts))))))

(defn save-roots!
  "MERGE `roots` into the {branch -> root-address} cell — a convergent (grow-map)
   write: a writer that knows only some branches never clobbers branches it
   doesn't (e.g. ones a synced peer added to the store). For a shared branch the
   incoming root wins; that's safe because mutually-merged peers compute the SAME
   content-addressed root per branch, so the cell converges rather than losing a
   branch under blind LWW.

   The merge is done ATOMICALLY (konserve `update`, go-locked per key) — NOT
   get-then-assoc — so two concurrent flushes, or a synced peer adding a branch
   between the read and the write, cannot lose an interleaved branch (TOCTOU).
   (async+sync)"
  ([kv-store roots] (save-roots! kv-store roots {:sync? true}))
  ([kv-store roots opts]
   (async+sync (:sync? opts)
               (async
                (await (kb/k-update kv-store (rk opts)
                                    (fn [existing] (merge (or existing {}) roots))
                                    opts))))))

(defn save-freed!
  "Persist a KonserveStorage's freed-set under the CRDT freed cell, MERGED
   ATOMICALLY (konserve `update`) so a concurrent writer's freed entries are not
   lost to a blind overwrite. (async+sync)"
  ([kv-store storage] (save-freed! kv-store storage {:sync? true}))
  ([kv-store storage opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-update kv-store (fk opts)
                                          (fn [existing] (merge (or existing {}) @(:freed-atom storage)))
                                          opts))))))

;; ============================================================
;; Commit objects — addressable snapshots of a MULTI-root CRDT
;; ============================================================
;; A single-root CRDT (G-Set) uses its PSS root address directly as the snapshot
;; handle. A multi-root CRDT (2P-Set/OR-Set = two halves) instead stores a tiny
;; content-addressed "commit" map {:adds <root> :removals <root>} and uses ITS
;; address as the snapshot-id — so `as-of`/`open-at` can re-open both halves at a
;; fixed version. Content-addressed → equal halves ⇒ equal snapshot-id.

(defn store-commit!
  "Store a small content-addressed commit map (e.g. {:adds r :removals r}) and
   return its address — the addressable snapshot handle. (async+sync)"
  ([kv-store commit] (store-commit! kv-store commit {:sync? true}))
  ([kv-store commit opts]
   (async+sync (:sync? opts)
               (async
                (let [addr (hasch/uuid commit)]
                  (await (kb/k-assoc kv-store addr commit opts))
                  addr)))))

(defn read-commit
  "Read a commit map by its address. (async+sync)"
  ([kv-store addr] (read-commit kv-store addr {:sync? true}))
  ([kv-store addr opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-get kv-store addr opts))))))

;; ============================================================
;; Reachability walk — the ship-set (delta-first sync primitive)
;; ============================================================

(defn reachable-addresses
  "Every node address reachable from `root` in `kv-store` (root + transitive
   `:addresses`). This IS the ship-set: the nodes a peer must hold to restore the
   set rooted at `root`. (async+sync — the recursive walk awaits each `k-get`,
   exactly like PSS's `walk-addresses`.)"
  ([kv-store root] (reachable-addresses kv-store root {:sync? true}))
  ([kv-store root opts]
   (async+sync (:sync? opts)
               (async
                (loop [to-visit [root] seen #{}]
                  (if-let [addr (first to-visit)]
                    (if (contains? seen addr)
                      (recur (rest to-visit) seen)
                      (let [node (await (kb/k-get kv-store addr opts))]
                        (recur (into (vec (rest to-visit)) (:addresses node))
                               (conj seen addr))))
                    seen))))))

(defn ship!
  "Copy every node reachable from `root` in `src-store` that `dst-store` is
   MISSING. Returns the count copied — incremental: 0 when already in sync. The
   store-to-store form of -join's transport; idempotent. (async+sync)"
  ([src-store dst-store root] (ship! src-store dst-store root {:sync? true}))
  ([src-store dst-store root opts]
   (async+sync (:sync? opts)
               (async
                (let [addrs (await (reachable-addresses src-store root opts))]
                  (loop [as (seq addrs) n 0]
                    (if as
                      (let [addr (first as)]
                        (if (some? (await (kb/k-get dst-store addr opts)))
                          (recur (next as) n)
                          (let [v (await (kb/k-get src-store addr opts))]
                            (await (kb/k-assoc dst-store addr v opts))
                            (recur (next as) (inc n)))))
                      n)))))))

;; ============================================================
;; Mark-and-sweep GC (datahike-style: whitelist reachable, sweep the rest)
;; ============================================================
;; CONCURRENCY MODEL (the lazy-read-vs-GC question). A durable CRDT is a VALUE
;; held in ONE mutable cell — a spindel signal-atom or an overlay's `:local-writes`
;; — so:
;;   - single-writer-per-cell is STRUCTURAL: mutations go through that cell's
;;     atomic `swap!`, so there is no mutation/mutation or mutation/flush race
;;     (co-located sub-stores are safe too: the roots cell is updated atomically
;;     via `save-roots!`, and nodes are content-addressed → idempotent + shared);
;;   - readers read the CURRENT value (FRP re-derivation) — a deref'd value is an
;;     immutable snapshot at a LIVE root, which `gc!` never reclaims.
;; The only residual hazard is a reader RETAINING a now-superseded value and
;; lazily draining it (each node = a `k-get`) WHILE a concurrent `gc!` sweeps the
;; orphaned nodes. The GC cutoff (`t/gc-cutoff` — `:remove-before`/`:grace-period-ms`)
;; below bounds that: a node written after the cutoff is kept, so a deref'd value
;; is drainable for ≥ the window (epoch default ⇒ keep everything ⇒ safe).

(defn gc!
  "Reclaim unreferenced PSS nodes by mark-and-sweep, mirroring datahike's
   `gc-storage!`:
     MARK   reachable = every node reachable from each root in `roots` ∪
            `(:retain-roots opts)` (extra PSS roots to KEEP — e.g. outstanding
            `as-of`/frozen-overlay snapshots, so GC never reclaims a held
            snapshot's nodes) ∪ the mutable pointer keys (:crdt/roots,
            :crdt/freed) ∪ `(:spare-keys opts)` ∪ `(:retain-keys opts)` (bare
            addresses to spare, e.g. retained commit objects).
     SWEEP  `konserve.gc/sweep!` deletes every store key NOT in the reachable set
            whose last-write is before the cutoff (reachable nodes + anything
            written after the cutoff are spared).

   SAFE BY DEFAULT — the sweep cutoff is `(t/gc-cutoff opts)`, the ONE shared GC
   convention: `:remove-before` (a Date) ∨ `:grace-period-ms` (a window ⇒ now − ms)
   ∨ EPOCH ⇒ reclaim NOTHING. With no window, `gc!` never sweeps a node an in-flight
   lazy read might hold — but superseded trees then accumulate without bound
   (`-join`/flush orphan the old root each time), so pass a window in production
   (≥ your longest lazy-read drain; ~60 s tight, hours/a day looser). Returns the
   set of deleted keys. (async+sync)"
  ([kv-store roots] (gc! kv-store roots {:sync? true}))
  ([kv-store roots opts]
   (async+sync (:sync? opts)
               (async
                (let [before (t/gc-cutoff opts)
                      ;; A SHARED-store composite sweeps ONCE over the UNION of every
                      ;; sub's roots (a per-sub gc! would delete siblings, unreachable
                      ;; from one sub's roots) and passes `:spare-keys` = all the extra
                      ;; pointer cells to keep (each sub's [:crdt/roots id]/[:crdt/freed
                      ;; id] + the composite manifest). Standalone CRDTs leave it nil.
                      reachable (loop [rs (seq (concat roots (:retain-roots opts)))
                                       acc (into #{(rk opts) (fk opts)}
                                                 (concat (:spare-keys opts) (:retain-keys opts)))]
                                  (if rs
                                    (recur (next rs)
                                           (into acc (await (reachable-addresses kv-store (first rs) opts))))
                                    acc))]
                  (await (kb/await-chan (kgc/sweep! kv-store reachable before) opts)))))))
