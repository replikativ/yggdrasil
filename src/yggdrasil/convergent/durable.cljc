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
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress]
            #?(:cljs [is.simm.partial-cps.sequence :as aseq]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]])))

(def ^:private roots-key :crdt/roots)
(def ^:private freed-key :crdt/freed)
(def ^:private branching-factor 64)

;; When several CRDTs share ONE konserve store (the single-causal-root composite,
;; option a — `:composite/root` as the lone gate, like datahike's `:db`), each
;; needs its OWN roots/freed cells so they don't clobber each other. Pass
;; `:roots-key`/`:freed-key` in the CONFIG map (e.g. `[:crdt/roots sub-id]`);
;; content-addressed nodes (uuid keys) are still safely shared (dedup across subs).
;; Default = the single-store cells, so every existing CRDT is unchanged.
;;
;; SEPARATION OF CONCERNS: throughout this layer `config` carries the DOMAIN of a
;; durable store (its cell-keys + open-time `:kv-store`/`:settings`/element handlers),
;; while `opts` carries ONLY the runtime mode `:sync?` (+, for `gc!`, the operation's
;; own params). The cell-keys (`config`) are constant for a store instance and are
;; persisted on the CRDT record's `:config` field; `:sync?` (`opts`) is the per-call
;; execution mode. So `opts` never carries store/domain arguments.
(defn- rk [config] (:roots-key config roots-key))
(defn- fk [config] (:freed-key config freed-key))

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

   `config` (DOMAIN) may carry `:kv-store` (reuse a pre-opened store), `:settings`,
   `:roots-key`/`:freed-key` (cell-keys), and `:element-read-handlers`/
   `:element-write-handlers` — fressian handlers for the CRDT's element type,
   attached alongside the canonical PSS node handlers so durable backends round-trip
   non-fressian-native elements (e.g. the registry's RegistryEntry). Bare-element
   CRDTs need none. `opts` (RUNTIME) carries only `:sync?`."
  ([store-config] (open store-config {} {:sync? true}))
  ([store-config config opts]
   (let [settings (or (:settings config) (store/default-settings))]
     (async+sync (:sync? opts)
                 (async
                  (let [;; reuse a pre-opened store (the single-store composite passes
                        ;; its own kv-store so subs co-habit it — already serializer-attached)
                        ;; or open store-config and attach the canonical PSS node serializer
                        ;; (no-op for memory; (de)serializes node objects for file/IndexedDB).
                        ;; `:element-read-handlers`/`:element-write-handlers` let a caller add an
                        ;; element handler (e.g. the registry's RegistryEntry); the bare catalog
                        ;; needs none (its elements are fressian-native).
                        kv-store (if-let [pre (:kv-store config)]
                                   pre
                                   (store/attach-pss-serializer!
                                    (await (store/open-store store-config opts)) settings
                                    (:element-read-handlers config) (:element-write-handlers config)))
                        storage  (store/create-storage kv-store {:content-addressed? true
                                                                 :settings settings})
                        freed    (or (await (kb/k-get kv-store (fk config) opts)) {})]
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
  "Materialize a (possibly lazy) PSS set into a Clojure set. (async+sync). cljs
   `pss/seq` returns a plain `Iter` (an `ISeq`) under `:sync? true` — drained by
   `clojure.core/into` — and a continuation yielding an `AsyncSeq` under
   `:sync? false`, drained by the partial-cps transducer `aseq/into`."
  [s opts]
  (async+sync (:sync? opts)
              (async
               (if (nil? s) #{}
                   #?(:clj  (into #{} s)
                      :cljs (if (:sync? opts)
                              (into #{} (pss/seq s opts))
                              (await (aseq/into #{} (await (pss/seq s opts))))))))))

(defn slice->clj
  "Materialize the INCLUSIVE PSS range `[from to]` into a Clojure vector — reads
   ONLY the nodes the range spans (lazy traversal + the storage node-cache), NOT a
   full drain. This is the datahike-style read-on-the-fly primitive: a key-scoped
   read touches O(log n + matches) nodes, not the whole set. (async+sync). On cljs
   the slice is a plain `Iter` under `:sync? true` (drained by `clojure.core/into`)
   and an `AsyncSeq` under `:sync? false` (drained by `aseq/into`); the 4-arity
   `pss/slice` passes the set's own comparator and threads `opts`."
  [s from to opts]
  (async+sync (:sync? opts)
              (async
               (if (nil? s) []
                   #?(:clj  (into [] (pss/slice s from to))
                      :cljs (if (:sync? opts)
                              (into [] (pss/slice s from to opts))
                              (await (aseq/into [] (await (pss/slice s from to opts))))))))))

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
                  ;; conj each elem of `b` into `a` via the async-or-sync `pss/conj`,
                  ;; threading the result — so a plain `reduce` over the sync `Iter`,
                  ;; or a single-`anext` drain over the `AsyncSeq` (no double-pull).
                  :cljs (if (:sync? opts)
                          (reduce (fn [acc x] (pss/conj acc x cmp opts)) a (pss/seq b opts))
                          (loop [aseq (await (pss/seq b opts)) acc a]
                            (if-let [[x rst] (await (aseq/anext aseq))]
                              (recur rst (await (pss/conj acc x cmp opts)))
                              acc)))))))

;; ============================================================
;; Root cell + freed persistence
;; ============================================================

(defn load-roots
  "Read the {branch -> root-address} cell, or nil when none yet (async+sync)."
  ([kv-store] (load-roots kv-store {} {:sync? true}))
  ([kv-store config opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-get kv-store (rk config) opts))))))

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
  ([kv-store roots] (save-roots! kv-store roots {} {:sync? true}))
  ([kv-store roots config opts]
   (async+sync (:sync? opts)
               (async
                (await (kb/k-update kv-store (rk config)
                                    (fn [existing] (merge (or existing {}) roots))
                                    opts))))))

(defn save-freed!
  "Persist a KonserveStorage's freed-set under the CRDT freed cell, MERGED
   ATOMICALLY (konserve `update`) so a concurrent writer's freed entries are not
   lost to a blind overwrite. (async+sync)"
  ([kv-store storage] (save-freed! kv-store storage {} {:sync? true}))
  ([kv-store storage config opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-update kv-store (fk config)
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
                        ;; a stored node is a PSS Leaf/Branch OBJECT, not a map —
                        ;; `(:addresses obj)` is nil; the portable child-address read
                        ;; is `node->map` (Branch → {… :addresses …}, Leaf → no
                        ;; :addresses). Without this the walk stops at the root and
                        ;; ship!/gc! see only ONE node (silently broken for any
                        ;; multi-node tree; single-leaf sets masked it).
                        (recur (into (vec (rest to-visit))
                                     (when node (:addresses (pss-fress/node->map node))))
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
  ([kv-store roots] (gc! kv-store roots {} {:sync? true}))
  ([kv-store roots config opts]
   (async+sync (:sync? opts)
               (async
                (let [before (t/gc-cutoff opts)
                      ;; A SHARED-store composite sweeps ONCE over the UNION of every
                      ;; sub's roots (a per-sub gc! would delete siblings, unreachable
                      ;; from one sub's roots) and passes `:spare-keys` = all the extra
                      ;; pointer cells to keep (each sub's [:crdt/roots id]/[:crdt/freed
                      ;; id] + the composite manifest). Standalone CRDTs leave it nil.
                      ;; `config` carries the cell-keys to spare; `opts` carries the gc
                      ;; operation params (cutoff / retain / spare) + `:sync?`.
                      reachable (loop [rs (seq (concat roots (:retain-roots opts)))
                                       acc (into #{(rk config) (fk config)}
                                                 (concat (:spare-keys opts) (:retain-keys opts)))]
                                  (if rs
                                    (recur (next rs)
                                           (into acc (await (reachable-addresses kv-store (first rs) opts))))
                                    acc))]
                  (await (kb/await-chan (kgc/sweep! kv-store reachable before) opts)))))))

;; ============================================================
;; Two-half CRDT shared logic (OR-Set / 2P-Set / OR-Map)
;; ============================================================
;; Each is TWO grow-only PSS sets under `:crdt/roots {:adds :removals}`, add-wins,
;; `set-union` join. The record carries `[… storage comparator adds removals dirty
;; opts]` (+ a per-type field: tag-fn / merge-fn / none); only the VALUE-OPS and the
;; `as-of` projection differ. The persistence / join / snapshot / gc machinery is
;; identical and lives here — records delegate, reconstructing via `assoc this`
;; (which preserves their concrete type AND their extra field).

(defn two-half-snapshot-id
  "Content-addressed snapshot = a commit `{:adds <root> :removals <root>}`; returns
   its address string. (async+sync)"
  [{:keys [adds removals storage kv-store]} opts]
  (async+sync (:sync? opts)
              (async
               (let [a (await (store-set! adds storage opts))
                     r (await (store-set! removals storage opts))]
                 (str (await (store-commit! kv-store {:adds a :removals r} opts)))))))

(defn two-half-restore-halves
  "Restore + drain both halves of the commit at `snap-id` → `[adds-clj removals-clj]`.
   The caller projects (live elements / value-set / map). (async+sync)"
  [{:keys [kv-store comparator storage]} snap-id opts]
  (async+sync (:sync? opts)
              (async
               (let [commit (await (read-commit kv-store (parse-uuid (str snap-id)) opts))]
                 [(await (set->clj (restore-set comparator (:adds commit) storage opts) opts))
                  (await (set->clj (restore-set comparator (:removals commit) storage opts) opts))]))))

(defn two-half-join
  "Symmetric same-store join — union both grow-only halves with `other`. Returns the
   receiver UNCHANGED on a no-op (idempotence — a signal holding it won't re-fire),
   else `assoc`-s the new halves + `:dirty` (preserving the record's type/fields).
   (async+sync)"
  [{:keys [adds removals comparator] :as this} other opts]
  (async+sync (:sync? opts)
              (async
               (let [a' (await (set-union adds (:adds other) comparator opts))
                     r' (await (set-union removals (:removals other) comparator opts))]
                 (if (and (= a' adds) (= r' removals))
                   this
                   (assoc this :adds a' :removals r' :dirty true))))))

(defn two-half-flush!
  "Persist both halves, update `:crdt/roots {:adds :removals}` + freed; clear
   `:dirty`. Returns the clean record (a no-op when not dirty). (async+sync)"
  [{:keys [adds removals storage kv-store dirty config] :as this} opts]
  (async+sync (:sync? opts)
              (async
               (if dirty
                 (let [a (await (store-set! adds storage opts))
                       r (await (store-set! removals storage opts))]
                   (await (save-roots! kv-store {:adds a :removals r} config opts))
                   (await (save-freed! kv-store storage config opts))
                   (assoc this :dirty false))
                 this))))

(defn two-half-merge-peer!
  "Cross-store join — ship `other`'s nodes into this store, then union both halves.
   Returns the new record. (async+sync)"
  [{:keys [adds removals comparator storage kv-store] :as this} other opts]
  (async+sync (:sync? opts)
              (async
               (let [ostorage (:storage other)
                     a-root   (await (store-set! (:adds other) ostorage opts))
                     r-root   (await (store-set! (:removals other) ostorage opts))]
                 (await (ship! (:kv-store other) kv-store a-root opts))
                 (await (ship! (:kv-store other) kv-store r-root opts))
                 (assoc this
                        :adds (await (set-union adds (restore-set comparator a-root storage opts) comparator opts))
                        :removals (await (set-union removals (restore-set comparator r-root storage opts) comparator opts))
                        :dirty true)))))

(defn two-half-gc-sweep!
  "Flush, then mark-and-sweep nodes unreachable from the live roots — retaining the
   commit objects (and both halves' nodes) named by `snapshot-ids` so a frozen
   `as-of` survives. (async+sync)"
  [{:keys [kv-store config] :as this} snapshot-ids gc-opts opts]
  (async+sync (:sync? opts)
              (async
               (await (two-half-flush! this opts))
               (let [commit-addrs (map #(parse-uuid (str %)) snapshot-ids)
                     retain-roots (loop [cs (seq commit-addrs) acc []]
                                    (if cs
                                      (let [c (await (read-commit kv-store (first cs) opts))]
                                        (recur (next cs) (conj acc (:adds c) (:removals c))))
                                      acc))]
                 (await (gc! kv-store (vals (await (load-roots kv-store config opts)))
                             config
                             (merge gc-opts opts {:retain-roots retain-roots
                                                  :retain-keys commit-addrs})))))))

(defn two-half-open
  "Open a two-half CRDT's store and restore both halves from `:crdt/roots`. Returns
   `{:kv-store :storage :store-config :config :comparator :adds :removals}` — where
   `:config` is the persistent DOMAIN map (cell-keys) the record carries. `store-config`
   defaults to a fresh in-memory store. `config` (DOMAIN) may carry `:comparator`,
   `:kv-store`, `:roots-key`/`:freed-key`, and `:element-read-handlers`/
   `:element-write-handlers` (a fressian handler for the CRDT's element type — e.g.
   the registry's RegistryEntry; bare-element CRDTs need none). `opts` (RUNTIME)
   carries only `:sync?`. (async+sync)"
  [store-config {:keys [comparator kv-store roots-key freed-key
                        element-read-handlers element-write-handlers]
                 :or {comparator compare}}
   opts]
  (let [store-config (or store-config (when-not kv-store (mem-store-config)))
        freed-key    (or freed-key (when (vector? roots-key) (assoc roots-key 0 :crdt/freed)))
        ;; the PERSISTENT domain the record carries: just the cell-keys (constant
        ;; per instance; the mutation fns read them). Default = the single-store cells.
        cell-config (cond-> {}
                      roots-key (assoc :roots-key roots-key)
                      freed-key (assoc :freed-key freed-key))
        ;; the OPEN-TIME domain: cell-keys + the store-opening params (consumed by
        ;; `open`, not persisted on the record).
        open-config (cond-> cell-config
                      kv-store               (assoc :kv-store kv-store)
                      element-read-handlers  (assoc :element-read-handlers element-read-handlers)
                      element-write-handlers (assoc :element-write-handlers element-write-handlers))]
    (async+sync (:sync? opts)
                (async
                 (let [{:keys [kv-store storage]} (await (open store-config open-config opts))
                       roots   (await (load-roots kv-store cell-config opts))
                       restore (fn [b] (if-let [root (get roots b)]
                                         (restore-set comparator root storage opts)
                                         (empty-set storage comparator)))]
                   {:kv-store kv-store :storage storage :store-config store-config
                    :config cell-config :comparator comparator
                    :adds (restore :adds) :removals (restore :removals)})))))
