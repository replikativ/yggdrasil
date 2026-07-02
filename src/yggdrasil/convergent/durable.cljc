(ns ^:no-doc yggdrasil.convergent.durable
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
            [yggdrasil.convergent.dag :as dag]
            [yggdrasil.types :as t]
            [konserve.gc :as kgc]
            [is.simm.partial-cps.core-async :as ca]
            [hasch.core :as hasch]
            #?(:clj  [is.simm.partial-cps.async :refer [async await]]
               :cljs [is.simm.partial-cps.async :refer [await]])
            #?(:clj [yggdrasil.macros :refer [async+sync]])
            [org.replikativ.persistent-sorted-set :as pss]
            [org.replikativ.persistent-sorted-set.fressian :as pss-fress]
            #?(:cljs [is.simm.partial-cps.sequence :as aseq])
            #?(:cljs [org.replikativ.persistent-sorted-set.btset :refer [BTSet]]))
  #?(:cljs (:require-macros [yggdrasil.macros :refer [async+sync]]
                            [is.simm.partial-cps.async :refer [async]]))
  #?(:clj (:import [org.replikativ.persistent_sorted_set PersistentSortedSet])))

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

;; PER-BRANCH head cells (the branchable-at-scale layout): one grow-set REGISTRY of branch
;; names + one mutable HEAD cell per branch, so a flush (and an open) touches only the
;; current branch, independent of branch count. Keys are NAMESPACED KEYWORDS — never
;; vectors: konserve-sync's `keyword-last` fetch-gate sorts a vector as CONTENT, so a vector
;; head key would ship before its nodes and break the gate. Co-located composite subs fold
;; their id into the namespace (`:cell-ns`) to stay a single namespaced keyword.
(def ^:private branches-key :crdt/branches)
(defn- bk [config]
  (or (:branches-key config)
      (if-let [ns (:cell-ns config)] (keyword "crdt.branches" (str ns)) branches-key)))
(defn- head-key [config branch]
  ;; `(subs (str branch) 1)` strips the leading ':' — round-trips a namespaced branch name
  ;; (`:feature/x`) where `(name branch)` would drop the `feature/` part.
  (let [b (subs (str branch) 1)]
    (if-let [ns (:cell-ns config)]
      (keyword "crdt.head" (str ns "::" b))
      (keyword "crdt.head" b))))

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
  "An empty PSS sorted-set backed by `storage` (pure — no storage IO). Threads the storage
   settings' boundary so a FRESH set splits MST-history-independently when configured (PSS
   only auto-derives the boundary on RESTORE, from the root node — a new set has no root)."
  [storage comparator]
  (let [boundary (store/settings-boundary (:settings storage))]
    (pss/sorted-set* (cond-> {:comparator comparator
                              :storage storage
                              :branching-factor branching-factor}
                       boundary (assoc :boundary boundary)))))

(defn store-set!
  "Persist `s` to its storage and return its root address (async+sync — PSS
   writes nodes to konserve)."
  ([s storage] (store-set! s storage {:sync? true}))
  ([s storage opts]
   #?(:clj  (if (:sync? opts)
              (pss/store s storage)          ; JVM IStorage is synchronous → a value
              ;; async-only callers (e.g. the async-only gc path) on the JVM: PSS
              ;; store is synchronous, so hand back an already-resolved partial-cps
              ;; CPS so the root address is `await`-able like every other async op.
              (let [root (pss/store s storage)] (fn [resolve _reject] (resolve root))))
      :cljs (pss/store s storage opts))))

(defn materialize-root!
  "Ensure flushed set `s`'s root is a durable store object at its content-address key,
   and return that address — the stable snapshot / ship handle. Called at every handle-
   EXPORT boundary (`snapshot-id` / `merge-peer!` / `ship!` / commit halves), as opposed
   to the per-op FLUSH which INLINES the root into the head cell (fusion) and skips its
   separate node write. Force-writes the root node so an export never references an
   inlined-but-unstored root. (async+sync)"
  ([s storage] (materialize-root! s storage {:sync? true}))
  ([s storage opts]
   (async+sync (:sync? opts)
               (async
                ;; realize the address (+ write children immediately — buffering is off on the
                ;; export path); then FORCE-write the root node, which the fused flush inlines
                ;; and skips (`pss/store` no-ops once the address is realized). Idempotent.
                (let [addr      (await (store-set! s storage opts))
                      root-node #?(:clj (.root ^PersistentSortedSet s) :cljs (.-root s))]
                  (await (kb/k-assoc (:kv-store storage) addr root-node kb/immutable-meta opts))
                  addr)))))

(defn flush-set-fused!
  "Persist set `s`'s DIRTY nodes EXCEPT its root, returning `{:root-addr :root-node}` — the
   caller INLINES the root node into the head cell (fusion) instead of writing it. Scopes the
   storage's BUFFERING (`store` accumulates), drains all-but-root, restores immediate mode.
   A single-leaf set writes ZERO nodes (its leaf IS the root). (async+sync)

   NB self-healing: buffering is (re)set to `[]` at entry and back to `nil` at exit; an error
   mid-flush leaves it buffering, but the next `flush-set-fused!` resets it."
  [s storage opts]
  (async+sync (:sync? opts)
              (async
               (let [pending (:pending storage)]
                 (reset! pending [])
                 (let [root-addr (await (store-set! s storage opts))       ; buffers dirty nodes
                       buffered  @pending
                       root-node (or (some (fn [[a n]] (when (= a root-addr) n)) buffered)
                                     #?(:clj (.root ^PersistentSortedSet s) :cljs (.-root s)))]
                   ;; drain every buffered node EXCEPT the root → children to konserve
                   (loop [ps (seq buffered)]
                     (when-let [[a n] (first ps)]
                       (when (not= a root-addr)
                         (await (kb/k-assoc (:kv-store storage) a n kb/immutable-meta opts)))
                       (recur (next ps))))
                   (reset! pending nil)
                   {:root-addr root-addr :root-node root-node})))))

(defn node?
  "Is `x` a stored PSS root value that is a NODE (fused, inlined) rather than a bare content
   ADDRESS (uuid/string, legacy/unfused)? Node ⇒ cache-seed on restore."
  [x]
  (not (or (uuid? x) (string? x) (nil? x))))

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

(defn restore-fused
  "Restore a set from a fused head value: a root NODE (inlined — `seed-node!` it so the lazy
   restore resolves it without a store read), a bare ADDRESS (legacy), or nil (empty branch).
   (sync-returning like `restore-set`)."
  [comparator root storage opts]
  (cond
    (nil? root)  (empty-set storage comparator)
    (node? root) (restore-set comparator (store/seed-node! storage root) storage opts)
    :else        (restore-set comparator root storage opts)))

(defn root-node-blob
  "Project a PSS set to its WIRE root for the fused value codec: the root NODE (inlined — the
   receiver `seed`s it, no store fetch) for a non-empty set, or nil for an empty one. The
   fused counterpart to `root-blob` (which ships the address); used because a fused root is
   NOT a separate store object. Reads the resident root (`.root`) — flush before shipping."
  [s]
  (when (pos? (count s))
    #?(:clj (.root ^PersistentSortedSet s) :cljs (.-root s))))

(defn root-address
  "The realized content-address of an ALREADY-FLUSHED PSS set `s`, read SYNCHRONOUSLY
   off its root node (`._address` on the JVM, `.address` on cljs). This is datahike's
   `safe-root`/`mark` pattern: the flush is async and happens UPSTREAM (the mutation or
   the wire's publish/handshake seam); reading the realized root for serialization is
   then a pure sync field read, so it works inside a synchronous fressian write handler
   on BOTH platforms. Throws if `s` isn't flushed (address nil) — the same
   flush-before-serialize contract `pss-fress/root-write-handler` enforces."
  [s]
  (let [addr #?(:clj  (.-_address ^PersistentSortedSet s)
                :cljs (.-address ^BTSet s))]
    (when (nil? addr)
      (throw (ex-info "PSS set must be flushed before reading its root address (flush async upstream first)"
                      {:type :must-be-flushed})))
    addr))

(defn root-blob
  "Project a PSS set to its wire/blob root for the `ygg/system` value codec:
     - a flushed set  → its realized content-address (string);
     - an EMPTY set   → nil (the receiver reconstructs an empty set — an empty branch
                        has no root to realize, and `flush!` no-ops on it);
     - a NON-empty UNFLUSHED set → throws (via `root-address`) — the fail-fast for a
                        forgotten `flush!` after an explicit `{:flush? false}` (auto-flush
                        would have committed it). Uniform + synchronous on both platforms."
  [s]
  (try (str (root-address s))
       (catch #?(:clj Throwable :cljs :default) e
         (if (zero? (count s)) nil (throw e)))))

(defn restore-or-empty
  "Reconstruct the PSS set at wire root `addr` with `comparator`, or an EMPTY set when
   `addr` is nil (an empty branch, projected as nil by `root-blob`). The reconstruct
   counterpart to `root-blob`."
  [comparator addr storage opts]
  (if addr
    (restore-set comparator (parse-uuid (str addr)) storage opts)
    (empty-set storage comparator)))

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

;; ---- per-branch head cells (the branchable-at-scale substrate; supersedes the old
;; single `:crdt/roots` grow-map — every CRDT now keys its heads per branch) ----

(defn load-branches
  "The branch REGISTRY — a grow-set of branch names, or #{} when none. (async+sync)"
  ([kv-store] (load-branches kv-store {} {:sync? true}))
  ([kv-store config opts]
   (async+sync (:sync? opts)
               (async (or (await (kb/k-get kv-store (bk config) opts)) #{})))))

(defn register-branch!
  "Add `branch` to the registry — an atomic GROW-SET union (konserve `update`, go-locked)
   so a concurrent flush or a synced peer's newly-added branch is never clobbered (TOCTOU,
   as `save-roots!`). Idempotent; call only when a branch is first seen. (async+sync)"
  ([kv-store branch] (register-branch! kv-store branch {} {:sync? true}))
  ([kv-store branch config opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-update kv-store (bk config)
                                          (fn [existing] (conj (or existing #{}) branch))
                                          opts))))))

(defn load-head
  "Read a branch's HEAD cell — the whole head value ({:root node …} for a single-root CRDT,
   {:adds node :removals node …} for a two-half CRDT, each with optional :parents) — or nil
   when the branch has no persisted head. (async+sync)"
  ([kv-store branch] (load-head kv-store branch {} {:sync? true}))
  ([kv-store branch config opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-get kv-store (head-key config branch) opts))))))

(defn save-head!
  "Write a branch's HEAD cell (LWW — a head is ONE branch's current working root; unlike the
   grow-set registry it moves, so it is a plain overwrite). `head` is the whole head value
   (fused root node(s) + :parents). (async+sync)"
  ([kv-store branch head] (save-head! kv-store branch head {} {:sync? true}))
  ([kv-store branch head config opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-assoc kv-store (head-key config branch) head opts))))))

(defn head-roots
  "The walkable PSS root value(s) carried by a head value — half-aware: a single-root head
   ({:root …}) yields one, a two-half head ({:adds :removals}) yields both; nils dropped.
   (`:parents` and friends are metadata, not walked.)"
  [head]
  (cond
    (nil? head)              []
    (contains? head :root)   (remove nil? [(:root head)])
    (or (contains? head :adds) (contains? head :removals))
    (remove nil? [(:adds head) (:removals head)])
    :else                    []))

(defn delete-head!
  "Remove branch `b`'s head cell and drop `b` from the registry (atomic grow-set disj).
   (async+sync)"
  ([kv-store b] (delete-head! kv-store b {} {:sync? true}))
  ([kv-store b config opts]
   (async+sync (:sync? opts)
               (async
                (await (kb/k-dissoc kv-store (head-key config b) opts))
                (await (kb/k-update kv-store (bk config)
                                    (fn [existing] (disj (or existing #{}) b)) opts))))))

(defn head-cell-keys
  "The pointer cells a branchable CRDT must SPARE from GC: the branches registry + every
   known branch's head cell. Pass as `:spare-keys` to `gc!`."
  [config branches]
  (into #{(bk config)} (map #(head-key config %)) branches))

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
                  ;; content-addressed snapshot handle → write-once → immutable.
                  (await (kb/k-assoc kv-store addr commit kb/immutable-meta opts))
                  addr)))))

(defn read-commit
  "Read a commit map by its address. (async+sync)"
  ([kv-store addr] (read-commit kv-store addr {:sync? true}))
  ([kv-store addr opts]
   (async+sync (:sync? opts)
               (async (await (kb/k-get kv-store addr opts))))))

;; ============================================================
;; Commit-DAG — a walkable lineage for the convergent CRDTs
;; ============================================================
;; A convergent CRDT (G-Set, OR-Set, …) gains a CDVCS-style commit-graph: a PSS grow-set of
;; `[commit-id {:root <handle> :parents}]` entries (the graph rides its own reserved-branch
;; head cell; parents are inlined, so they ship WITH the graph nodes). Each branch's head
;; cell carries a scalar `:commit` tip. Commits are ANONYMOUS + CANONICAL — no author/ts,
;; parents SORTED — so two peers reaching the same state with the same parents mint the SAME
;; id (the precondition for a convergent merge commit under `-join`, where history-independent
;; MST roots make both peers' union root, hence the merge commit, byte-identical). merge-base =
;; `dag/lowest-common-ancestors` over the graph's `parents-of`; NO historical root trees needed.

(defn make-commit
  "A commit VALUE + its content-addressed id. `root-handle` is the materialized root
   address (single-root) or `{:adds <addr> :removals <addr>}` (two-half); `parents` any coll of
   parent commit-ids. The commit is stored as a STANDALONE content-addressed key (`store-commit!`)
   — so the id IS its store key `(hasch/uuid value)`. Parents sorted → order-independent +
   author/ts-free, so equal (state, parents) across peers ⇒ equal id (one O(1) write per commit,
   no tree-path amplification)."
  [root-handle parents]
  (let [cval {:root root-handle :parents (vec (sort-by str parents))}]
    {:id (hasch/uuid cval) :value cval}))

(defn base-commit
  "The canonical base commit (no root, no parents) — a fixed shared sentinel every fresh convergent
   CRDT starts from, so same-origin CRDTs always share a common ancestor (merge-base never nil)."
  []
  (let [cval {:root nil :parents []}]
    {:id (hasch/uuid cval) :value cval}))

(defn append-commit!
  "Persist a commit (`{:id :value}` from `make-commit`) as a STANDALONE content-addressed,
   write-once key (`store-commit!` — key == id). Idempotent (content-addressed dedups a
   re-commit). Returns the id. (async+sync)"
  [kv-store commit opts]
  (store-commit! kv-store (:value commit) opts))

(defn commit-parents-of
  "An accessor `commit-id -> (async parents-or-nil)` over the STANDALONE commit keys, for
   `dag/lowest-common-ancestors`: reads the commit and yields its `:parents` (a canonical sorted
   vec), or nil when the commit is absent. (async+sync)"
  [kv-store opts]
  (fn [id]
    (async+sync (:sync? opts)
                (async (:parents (await (read-commit kv-store id opts)))))))

;; ---- shared Graphable / merge-base over the commit-graph (gset + two-half all delegate) ----

(defn commit-history
  "The commit-DAG history (DFS linearization) of a branch tip `commit`. (async+sync)"
  [kv-store _storage _config commit opts]
  (async+sync (:sync? opts)
              (async
               (await (dag/commit-history (commit-parents-of kv-store opts) commit opts)))))

(defn commit-ancestors
  "All ancestor commit-ids of commit `s` (excluding `s`). (async+sync)"
  [kv-store _storage _config s opts]
  (async+sync (:sync? opts)
              (async
               (remove #{s} (await (dag/commit-history (commit-parents-of kv-store opts) s opts))))))

(defn commit-ancestor?
  "True if commit `a` is an ancestor of commit `b`. (async+sync)"
  [kv-store storage config a b opts]
  (async+sync (:sync? opts)
              (async (boolean (some #{a} (await (commit-ancestors kv-store storage config b opts)))))))

(defn commit-common-ancestor
  "The merge-base of commits `a` and `b` (most recent common ancestor). (async+sync)"
  [kv-store _storage _config a b opts]
  (async+sync (:sync? opts)
              (async
               (let [pof (commit-parents-of kv-store opts)]
                 (first (:lcas (await (dag/lowest-common-ancestors pof #{a} pof #{b} opts))))))))

(defn commit-graph-map
  "The whole commit-DAG — every commit reachable from a branch tip — as
   `{commit-id {:parent-ids #{…}}}`. (async+sync)"
  [kv-store _storage config opts]
  (async+sync (:sync? opts)
              (async
               (let [branches (await (load-branches kv-store config opts))
                     tips     (loop [bs (seq branches) acc []]
                                (if bs
                                  (recur (next bs) (conj acc (:commit (await (load-head kv-store (first bs) config opts)))))
                                  acc))]
                 (loop [stack (vec (remove nil? tips)) graph {}]
                   (if-let [id (peek stack)]
                     (if (contains? graph id)
                       (recur (pop stack) graph)
                       (let [parents (:parents (await (read-commit kv-store id opts)))]
                         (recur (into (pop stack) parents)
                                (assoc graph id {:parent-ids (set parents)}))))
                     graph))))))

(defn commit-info
  "`{:parent-ids #{…} :root <handle>}` for commit `id`, or nil. (async+sync)"
  [kv-store _storage _config id opts]
  (async+sync (:sync? opts)
              (async
               (when-let [v (await (read-commit kv-store id opts))]
                 {:parent-ids (set (:parents v)) :root (:root v)}))))

(defn commit-merge-base
  "The merge-base commit of branches `a-branch` and `b-branch` — LCA over the standalone commit
   keys, seeded from each branch's tip. nil if no shared ancestor. (async+sync)"
  [kv-store _storage config a-branch b-branch opts]
  (async+sync (:sync? opts)
              (async
               (let [ha  (await (load-head kv-store a-branch config opts))
                     hb  (await (load-head kv-store b-branch config opts))
                     pof (commit-parents-of kv-store opts)]
                 (first (:lcas (await (dag/lowest-common-ancestors
                                       pof #{(:commit ha)} pof #{(:commit hb)} opts))))))))

(defn two-half-author-commit!
  "Materialize both halves of a two-half value, append a commit `{:root {:adds :removals}
   :parents}` to the shared DAG, advance the branch tip, persist the head. Returns the record
   with the new tip + halves. `parents` any coll of parent commit-ids (deduped; nils dropped).
   (async+sync)"
  [this adds removals parents opts]
  (async+sync (:sync? opts)
              (async
               (let [{:keys [kv-store storage branch config]} this
                     a-addr (str (await (materialize-root! adds storage opts)))
                     r-addr (str (await (materialize-root! removals storage opts)))
                     mc     (make-commit {:adds a-addr :removals r-addr} (disj (into #{} parents) nil))]
                 (await (append-commit! kv-store mc opts))     ; standalone content-addressed commit key
                 (await (save-head! kv-store branch
                                    {:adds (root-node-blob adds) :removals (root-node-blob removals) :commit (:id mc)}
                                    config opts))
                 (assoc this :adds adds :removals removals :dirty false :commit (:id mc))))))

(defn two-half-commit!
  "Commit the current two-half value; parents = the current branch tip. (async+sync)"
  ([this] (two-half-commit! this {:sync? true}))
  ([this opts]
   (two-half-author-commit! this (:adds this) (:removals this)
                            (when (:commit this) #{(:commit this)}) opts)))

(defn commit-root-addresses
  "Store key(s) of a commit's materialized root: a bare address string → `[uuid]`; a two-half
   `{:adds :removals}` handle → `[adds-uuid removals-uuid]`; nil (the base commit) → `[]`."
  [root-handle]
  (cond
    (nil? root-handle) []
    (map? root-handle) (into [] (keep #(some-> % str parse-uuid) [(:adds root-handle) (:removals root-handle)]))
    :else              (if-let [u (some-> root-handle str parse-uuid)] [u] [])))

(defn commit-reachable-roots
  "Root addresses to RETAIN under GC (pass as `:retain-roots` to `gc!`): from each branch tip
   in `tips`, walk the commit-graph's `:parents`, adding every reached commit's `:root`
   address(es), and RECURSE into a commit's parents ONLY when it is IN-WINDOW — its root's
   konserve `:last-write` is newer than `remove-before` (the datahike `reachable-in-branch`
   walk-truncation; commits are anonymous so the root's store time is the recency proxy).
   `remove-before` nil/epoch ⇒ every commit in-window ⇒ ALL historical roots retained (the
   safe default — reclaims nothing). (async+sync)"
  [kv-store _storage tips remove-before _config opts]
  (async+sync (:sync? opts)
              (async
               (let [cutoff (when remove-before (inst-ms remove-before))
                     all?   (or (nil? cutoff) (zero? cutoff))]
                 (loop [stack (vec (remove nil? tips)) seen #{} roots #{}]
                   (if-let [id (peek stack)]
                     (if (contains? seen id)
                       (recur (pop stack) seen roots)
                       (let [v (await (read-commit kv-store id opts))]
                         (if v
                           (let [addrs     (commit-root-addresses (:root v))
                                 lw        (when (and (not all?) (seq addrs))
                                             (let [m (await (kb/k-get-meta kv-store (first addrs) opts))]
                                               (when (:last-write m) (inst-ms (:last-write m)))))
                                 in-range? (or all? (and lw (> lw cutoff)))]
                             (recur (into (pop stack) (when in-range? (:parents v)))
                                    (conj seen id)
                                    (into roots addrs)))
                           (recur (pop stack) (conj seen id) roots))))
                     roots))))))

(defn commit-reachable-keys
  "The set of commit KEYS reachable from `tips` (the FULL parent chain — commit metadata is tiny,
   so it is retained in full; only historical ROOT TREES are window-pruned by
   `commit-reachable-roots`). Pass as `:retain-keys` to `gc!` so live commits survive and a
   deleted branch's orphan commits become reclaimable. (async+sync)"
  [kv-store tips opts]
  (async+sync (:sync? opts)
              (async
               (loop [stack (vec (remove nil? tips)) seen #{}]
                 (if-let [id (peek stack)]
                   (if (contains? seen id)
                     (recur (pop stack) seen)
                     (let [parents (:parents (await (read-commit kv-store id opts)))]
                       (recur (into (pop stack) parents) (conj seen id))))
                   seen)))))

;; ============================================================
;; Reachability walk — the ship-set (delta-first sync primitive)
;; ============================================================

(defn- collect-refs
  "Store keys (`hasch/ref->uuid`) of every hasch `HashRef` embedded in `x` — a node's
   elements or a broken-out value — recursing through maps/colls. A `HashRef` is caught
   BEFORE descent (it is a record, hence also a map), so we key off the ref itself and
   never walk its bytes. This is what makes GC / ship END-TO-END: a value broken out by
   `yggdrasil.cas` and referenced from a live element stays reachable (never swept)."
  ([x] (collect-refs x #{}))
  ([x acc]
   (cond
     (hasch/hash-ref? x) (conj acc (hasch/ref->uuid x))
     (map? x)            (reduce-kv (fn [a k v] (collect-refs v (collect-refs k a))) acc x)
     (coll? x)           (reduce (fn [a v] (collect-refs v a)) acc x)
     :else               acc)))

(defn- node->map-or-nil
  "`node->map obj` if `obj` is a PSS Leaf/Branch OBJECT, else nil — the projection casts to
   `ANode` and throws on anything else (a broken-out value is arbitrary EDN, never a node),
   which is the value-vs-node discriminator we need (`node?` only rules out uuids/strings)."
  [obj]
  (try (pss-fress/node->map obj) (catch #?(:clj Exception :cljs :default) _ nil)))

(defn- follow-addresses
  "Addresses to visit next from a k-get'd store object `obj`: structural PSS children
   (`:addresses`) PLUS the content-address keys of any `HashRef`s in its elements (`:keys`
   of a node) or its whole value (a broken-out payload — no structural children). Always
   walks elements: the nodes are already fetched + decoded here, so scanning them is
   in-memory-cheap, and a `:break-out?` gate would only risk sweeping live values."
  [obj]
  (if-let [m (node->map-or-nil obj)]
    (into (vec (:addresses m)) (collect-refs (:keys m)))
    (vec (collect-refs obj))))

(defn reachable-addresses
  "Every node + broken-out-value address reachable from `root` in `kv-store` (root +
   transitive `:addresses` + embedded `HashRef`s). This IS the ship-set: everything a peer
   must hold to restore the set rooted at `root`, INCLUDING content-addressed values broken
   out of elements. (async+sync — the recursive walk awaits each `k-get`, exactly like PSS's
   `walk-addresses`.)"
  ([kv-store root] (reachable-addresses kv-store root {:sync? true}))
  ([kv-store root opts]
   (async+sync (:sync? opts)
               (async
                ;; a stored node is a PSS Leaf/Branch OBJECT (not a map — `(:addresses obj)`
                ;; is nil), or a broken-out VALUE reached via a HashRef; `follow-addresses`
                ;; handles both (structural children + embedded refs). Without the node->map
                ;; read the walk would stop at the root and ship!/gc! see only ONE node.
                (loop [to-visit [root] seen #{}]
                  (if-let [addr (first to-visit)]
                    (if (contains? seen addr)
                      (recur (rest to-visit) seen)
                      (let [obj (await (kb/k-get kv-store addr opts))]
                        (recur (into (vec (rest to-visit))
                                     (when obj (follow-addresses obj)))
                               (conj seen addr))))
                    seen))))))

(defn reachable-from-node
  "Every node address reachable from the CHILDREN of an in-memory `root-node` — the root
   itself EXCLUDED (post-fusion it is inlined in the head cell, not a store object). The
   GC / internal-walk variant for a fused root: it seeds from the node's own child
   addresses instead of `k-get`-ing the root address (which would return nil for an
   inlined root and collapse the walk). For a single-leaf root this is empty. (async+sync)"
  ([kv-store root-node] (reachable-from-node kv-store root-node {:sync? true}))
  ([kv-store root-node opts]
   (async+sync (:sync? opts)
               (async
                ;; seed from the root node's structural children + its element refs (the
                ;; root itself is inlined in the head cell, not a store object).
                (loop [to-visit (follow-addresses root-node) seen #{}]
                  (if-let [addr (first to-visit)]
                    (if (contains? seen addr)
                      (recur (rest to-visit) seen)
                      (let [obj (await (kb/k-get kv-store addr opts))]
                        (recur (into (vec (rest to-visit))
                                     (when obj (follow-addresses obj)))
                               (conj seen addr))))
                    seen))))))

(defn reachable-of
  "Reachable node addresses for a head `root` that may be a fused NODE (walk from its
   children — the root is inlined in the head cell, not a store object) or a bare ADDRESS
   (walk from it). The GC entry point over a mixed set of head roots + snapshot roots.
   (async+sync)"
  [kv-store root opts]
  (if (node? root)
    (reachable-from-node kv-store root opts)
    (reachable-addresses kv-store root opts)))

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

(defn ship-commits!
  "Copy the commit-DAG reachable from `tip` — every standalone commit key (`read-commit` →
   `:parents`) and each commit's `:root` tree — from `src-store` into `dst-store`, skipping what
   dst already holds. The cross-store transport for lineage now that commits are standalone
   content-addressed keys (no reserved branch). (async+sync)"
  ([src-store dst-store tip] (ship-commits! src-store dst-store tip {:sync? true}))
  ([src-store dst-store tip opts]
   (async+sync (:sync? opts)
               (async
                (loop [stack (if tip [tip] []) seen #{}]
                  (if-let [id (peek stack)]
                    (if (contains? seen id)
                      (recur (pop stack) seen)
                      (let [v (await (read-commit src-store id opts))]
                        (when (and v (not (await (read-commit dst-store id opts))))
                          (await (store-commit! dst-store v opts)))
                        (loop [as (seq (commit-root-addresses (:root v)))]
                          (when as
                            (await (ship! src-store dst-store (first as) opts))
                            (recur (next as))))
                        (recur (into (pop stack) (:parents v)) (conj seen id))))
                    nil))))))

(defn ship-node!
  "Ship a FUSED root NODE's tree from `src-store` to `dst-store`: store the node in `src`
   under its content-address (so it's a store object `ship!` can copy), then copy the node
   + its children to `dst`. Returns the node's address (nil for a nil node). Used by the
   cross-store whole-tree reconcile, which reads head cells (nodes) directly — avoiding a
   `materialize-root!` on a LAZILY-restored set (whose `.-root` is null on cljs). (async+sync)"
  [src-store dst-store node opts]
  (async+sync (:sync? opts)
              (async
               (when node
                 (let [addr (store/node-address node)]
                   (await (kb/k-assoc src-store addr node kb/immutable-meta opts))
                   (await (ship! src-store dst-store addr opts))
                   addr)))))

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
  ([kv-store roots] (gc! kv-store roots {} {}))
  ([kv-store roots config opts]
   (let [opts (t/async-gc-opts "durable/gc!" opts)]
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
                                             (into acc (await (reachable-of kv-store (first rs) opts))))
                                      acc))]
                    (await (ca/chan->cps (kgc/sweep! kv-store reachable before)))))))))

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
               ;; EXPORT: materialize both half roots as durable store objects (the commit
               ;; references them by address) — the per-op flush may have inlined them.
               (let [a (await (materialize-root! adds storage opts))
                     r (await (materialize-root! removals storage opts))]
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

(declare two-half-flush!)

(defn two-half-join
  "Symmetric same-store join — union both grow-only halves with `other`. Returns the
   receiver UNCHANGED on a no-op (idempotence — a signal holding it won't re-fire),
   else `assoc`-s the new halves + `:dirty` (preserving the record's type/fields).
   AUTO-FLUSHes the changed result (`:flush?` default true) — durable-after-apply for
   orset/twopset/ormap's receive side. (async+sync)"
  [{:keys [adds removals comparator] :as this} other opts]
  (async+sync (:sync? opts)
              (async
               (let [a' (await (set-union adds (:adds other) comparator opts))
                     r' (await (set-union removals (:removals other) comparator opts))]
                 (if (and (= a' adds) (= r' removals))
                   this
                   (let [r (assoc this :adds a' :removals r' :dirty true)]
                     (if (:flush? opts true) (await (two-half-flush! r opts)) r)))))))

(defn two-half-flush!
  "Persist BOTH halves of the CURRENT branch into its head cell `:crdt.head/<branch>`
   `{:adds node :removals node :parents}`; clear `:dirty`. A no-op when not dirty.
   (async+sync)"
  [{:keys [adds removals branch storage kv-store dirty commit config] :as this} opts]
  (async+sync (:sync? opts)
              (async
               (if dirty
                 ;; ROOT FUSION: inline each half's root NODE into the branch head cell
                 ;; (children written, root skipped). A clean half's flush-set-fused! buffers
                 ;; nothing (pss/store no-ops) and just re-inlines its resident root — no write.
                 (let [{a-node :root-node} (await (flush-set-fused! adds storage opts))
                       {r-node :root-node} (await (flush-set-fused! removals storage opts))]
                   (await (save-head! kv-store (or branch :main)
                                      {:adds a-node :removals r-node :commit commit} config opts))
                   ;; NO save-freed! — GC sweeps via konserve :last-write (see gset flush!).
                   (assoc this :dirty false))
                 this))))

(defn- fold-peer-branch-head!
  "Fold a peer branch's already-restored halves `poa`/`por` into b's LOCAL head cell:
   union with b's existing local halves, flush both, then save + register the head.
   Used by merge-peer-branch! for NON-current peer branches; `o-commit` is the peer
   head's commit (the fallback tip). Split out to keep merge-peer-branch!'s CPS nesting
   shallow (see it). (async+sync)"
  [{:keys [comparator storage kv-store config]} b poa por o-commit opts]
  (async+sync (:sync? opts)
              (async
               (let [gh (await (load-head kv-store b config opts))
                     a' (await (set-union (restore-fused comparator (:adds gh) storage opts) poa comparator opts))
                     r' (await (set-union (restore-fused comparator (:removals gh) storage opts) por comparator opts))
                     {an :root-node} (await (flush-set-fused! a' storage opts))
                     {rn :root-node} (await (flush-set-fused! r' storage opts))]
                 (await (save-head! kv-store b {:adds an :removals rn :commit (or (:commit gh) o-commit)} config opts))
                 (await (register-branch! kv-store b config opts))))))

(defn- merge-peer-branch!
  "Reconcile ONE peer branch `b` into the local store during a whole-tree
   `two-half-merge-peer!`: ship its halves' nodes + lineage here, then either UNION
   into the caller's accumulators `[ca cr]` (when `b` is the local current `branch`)
   or fold into b's own local head cell. Returns `[adds removals]` — the (possibly
   updated) accumulators.

   Extracted from the merge loop on purpose: with ~8 awaits inlined in a loop, the
   partial-cps CPS expansion nested one continuation class per await deep enough that
   the generated `.class` name blew past the filesystem's 255-byte limit under AOT.
   Splitting the per-branch body into its own fn keeps BOTH fns' continuation nesting
   shallow. (async+sync)"
  [{:keys [comparator storage kv-store branch] :as this} o-store o-config b ca cr opts]
  (async+sync (:sync? opts)
              (async
               (let [oh  (await (load-head o-store b o-config opts))
                     _   (await (ship-node! o-store kv-store (:adds oh) opts))
                     _   (await (ship-node! o-store kv-store (:removals oh) opts))
                     ;; ship the peer branch's LINEAGE (standalone commit keys + root trees)
                     _   (await (ship-commits! o-store kv-store (:commit oh) opts))
                     oa  (restore-fused comparator (:adds oh) storage opts)
                     or* (restore-fused comparator (:removals oh) storage opts)]
                 (if (= b branch)
                   [(await (set-union ca oa comparator opts))
                    (await (set-union cr or* comparator opts))]
                   (do (await (fold-peer-branch-head! this b oa or* (:commit oh) opts))
                       [ca cr]))))))

(defn two-half-merge-peer!
  "Cross-STORE reconcile over the WHOLE tree (holds both stores): for every branch the
   peer has, ship both halves' nodes here and union into the same-named local branch's
   head cell. Returns a NEW record on its (possibly-merged) current branch. (async+sync)"
  [{:keys [adds removals] :as this} other opts]
  (async+sync (:sync? opts)
              (async
               (let [this      (await (two-half-flush! this opts))
                     o-store   (:kv-store other) o-config (:config other)
                     obranches (await (load-branches o-store o-config opts))]
                 ;; merge each peer branch; the CURRENT branch accumulates into this's
                 ;; in-memory (resident, non-lazy) halves, OTHER branches into head cells —
                 ;; so the returned value's halves are never a lazy restore (a cljs slice on
                 ;; a lazily-seeded root can miss). Per-branch work lives in merge-peer-branch!
                 ;; (one await here) to keep this fn's CPS nesting shallow (see that fn).
                 (loop [bs (seq obranches) ca adds cr removals]
                   (if bs
                     (let [[ca' cr'] (await (merge-peer-branch! this o-store o-config (first bs) ca cr opts))]
                       (recur (next bs) ca' cr'))
                     (let [merged (assoc this :adds ca :removals cr :dirty true)]
                       (if (:flush? opts true) (await (two-half-flush! merged opts)) merged))))))))

;; ---- two-half branch ops (parallel to the G-Set; store-backed) ----

(defn two-half-branches [{:keys [kv-store config]} opts] (load-branches kv-store config opts))

(defn two-half-branch!
  "Create branch `name` from `from` (a branch keyword) by copying its halves into a new head
   cell, INHERITING `from`'s tip commit (no commit authored — datahike parity). Stays on the
   current branch. (async+sync)"
  [{:keys [adds removals branch commit kv-store config] :as this} name from opts]
  (async+sync (:sync? opts)
              (async
               (let [head (if (= from branch)
                            {:adds (root-node-blob adds) :removals (root-node-blob removals) :commit commit}
                            (let [h (await (load-head kv-store from config opts))]
                              {:adds (:adds h) :removals (:removals h) :commit (:commit h)}))]
                 (await (save-head! kv-store name head config opts))
                 (await (register-branch! kv-store name config opts))
                 this))))

(defn two-half-checkout
  [{:keys [comparator storage kv-store config] :as this} name opts]
  (async+sync (:sync? opts)
              (async
               (let [flushed (await (two-half-flush! this opts))
                     h       (await (load-head kv-store name config opts))]
                 (assoc flushed :adds (restore-fused comparator (:adds h) storage opts)
                        :removals (restore-fused comparator (:removals h) storage opts)
                        :branch name :commit (:commit h) :dirty false)))))

(defn two-half-delete-branch!
  [{:keys [kv-store config] :as this} name opts]
  (async+sync (:sync? opts) (async (await (delete-head! kv-store name config opts)) this)))

(defn two-half-merge!
  "Branch-merge: union another branch's halves into the current branch + author a MERGE commit
   (parents = both branch tips). (async+sync)"
  [{:keys [adds removals commit comparator storage kv-store config] :as this} source opts]
  (async+sync (:sync? opts)
              (async
               (let [h      (await (load-head kv-store source config opts))
                     a'     (await (set-union adds (restore-fused comparator (:adds h) storage opts) comparator opts))
                     r'     (await (set-union removals (restore-fused comparator (:removals h) storage opts) comparator opts))
                     a-addr (str (await (materialize-root! a' storage opts)))
                     r-addr (str (await (materialize-root! r' storage opts)))
                     mc     (make-commit {:adds a-addr :removals r-addr} (disj (into #{} [commit (:commit h)]) nil))
                     _      (await (append-commit! kv-store mc opts))
                     merged (assoc this :adds a' :removals r' :dirty true :commit (:id mc))]
                 (if (:flush? opts true) (await (two-half-flush! merged opts)) merged)))))

(defn two-half-gc-sweep!
  "Flush, then mark-and-sweep nodes unreachable from the live roots — retaining the
   commit objects (and both halves' nodes) named by `snapshot-ids` so a frozen
   `as-of` survives. (async+sync)"
  ;; `opts` carries BOTH the runtime mode (`:sync?`) AND any GC window
  ;; (`:remove-before`/`:grace-period-ms`) — the record no longer stamps a mode, so
  ;; the caller's per-op opts is the single source.
  [{:keys [kv-store storage config] :as this} snapshot-ids opts]
  (let [opts (t/async-gc-opts "durable/two-half-gc-sweep!" opts)]
    (async+sync (:sync? opts)
                (async
                 (await (two-half-flush! this opts))
                 (let [commit-addrs (map #(parse-uuid (str %)) snapshot-ids)
                       retain-roots (loop [cs (seq commit-addrs) acc []]
                                      (if cs
                                        (let [c (await (read-commit kv-store (first cs) opts))]
                                          (recur (next cs) (conj acc (:adds c) (:removals c))))
                                        acc))
                       ;; enumerate EVERY branch's head (both halves + tip) — else GC sweeps a
                       ;; sibling branch's live nodes.
                       branches (await (load-branches kv-store config opts))
                       {:keys [roots tips]}
                       (loop [bs (seq branches) roots [] tips []]
                         (if bs
                           (let [h (await (load-head kv-store (first bs) config opts))]
                             (recur (next bs) (into roots (head-roots h)) (conj tips (:commit h))))
                           {:roots roots :tips tips}))
                       ;; datahike-style retention of historical commit root trees (both halves)
                       ;; within the window (epoch cutoff ⇒ keep all → reclaim nothing).
                       commit-roots (await (commit-reachable-roots kv-store storage tips
                                                                   (t/gc-cutoff opts) config opts))
                       ;; retain the live commit KEYS (full chain from tips); orphaned commits
                       ;; from a deleted branch fall out of the whitelist → reclaimable.
                       commit-keys  (await (commit-reachable-keys kv-store tips opts))]
                   (await (gc! kv-store roots config
                               (merge opts {:retain-roots (into (vec retain-roots) commit-roots)
                                            :retain-keys (into (vec commit-addrs) commit-keys)
                                            :spare-keys (head-cell-keys config branches)}))))))))

(defn two-half-open
  "Open a two-half CRDT's store and restore both halves from `:crdt/roots`. Returns
   `{:kv-store :storage :store-config :config :comparator :adds :removals}` — where
   `:config` is the persistent DOMAIN map (cell-keys) the record carries. `store-config`
   defaults to a fresh in-memory store. `config` (DOMAIN) may carry `:comparator`,
   `:kv-store`, `:roots-key`/`:freed-key`, and `:element-read-handlers`/
   `:element-write-handlers` (a fressian handler for the CRDT's element type — e.g.
   the registry's RegistryEntry; bare-element CRDTs need none). `opts` (RUNTIME)
   carries only `:sync?`. (async+sync)"
  [store-config {:keys [comparator kv-store cell-ns branches-key branch
                        element-read-handlers element-write-handlers]
                 :or {comparator compare branch :main}}
   opts]
  (let [store-config (or store-config (when-not kv-store (mem-store-config)))
        ;; the PERSISTENT domain the record carries: the cell-keys (constant per instance).
        cell-config (cond-> {}
                      branches-key (assoc :branches-key branches-key)
                      cell-ns (assoc :cell-ns cell-ns))
        ;; the OPEN-TIME domain: cell-keys + the store-opening params (consumed by
        ;; `open`, not persisted on the record).
        open-config (cond-> cell-config
                      kv-store               (assoc :kv-store kv-store)
                      element-read-handlers  (assoc :element-read-handlers element-read-handlers)
                      element-write-handlers (assoc :element-write-handlers element-write-handlers))]
    (async+sync (:sync? opts)
                (async
                 (let [{:keys [kv-store storage]} (await (open store-config open-config opts))
                       registry (await (load-branches kv-store cell-config opts))
                       cur (cond (registry branch) branch (registry :main) :main
                                 (seq registry) (first registry) :else branch)
                       h   (await (load-head kv-store cur cell-config opts))]
                   ;; a fresh store has no registry — register the current branch.
                   (when-not (seq registry)
                     (await (register-branch! kv-store cur cell-config opts)))
                   ;; seed the canonical base commit ONCE per store (shared common ancestor) —
                   ;; a standalone content-addressed key; store-once if absent.
                   (let [base (base-commit)]
                     (when-not (await (read-commit kv-store (:id base) opts))
                       (await (append-commit! kv-store base opts)))
                     ;; each half is a fused root NODE (inlined) — restore-fused cache-seeds it.
                     {:kv-store kv-store :storage storage :store-config store-config
                      :config cell-config :comparator comparator :branch cur
                      :commit (or (:commit h) (:id base))
                      :adds (restore-fused comparator (:adds h) storage opts)
                      :removals (restore-fused comparator (:removals h) storage opts)}))))))
