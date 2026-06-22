# Convergent CRDTs

Yggdrasil ships a catalog of **conflict-free replicated data types** as first-class
yggdrasil *systems*: each is a value with a commutative/associative/idempotent join
(`-join`), durable on the same PSS-over-konserve substrate as the snapshot registry,
and cross-platform (JVM + ClojureScript). A replica is just a copy of the value held
in a mutable cell (a spindel signal, a registry conn, an overlay's local-writes);
mutation `swap!`s a new value in, and convergence is `-join`.

This document is the catalog. For CDVCS specifically see
[cdvcs-convergent-system.md](cdvcs-convergent-system.md).

## The model

- **Value-semantic.** Every operation returns a *new* value; the record never
  mutates in place. The only mutable cell is the holder.
- **Durable, read on the fly.** The value lives as a [persistent-sorted-set](https://github.com/replikativ/persistent-sorted-set)
  B-tree over [konserve](https://github.com/replikativ/konserve), content-addressed
  (a node's key is the `hasch` UUID of its content → a Merkle tree → incremental
  sync + dedup). The value is **never fully materialized in memory**: a read pulls
  only the B-tree nodes it touches, through an in-memory node cache (datahike-style).
  A key-scoped read is an O(log n + matches) range *slice*, not a full scan.
- **Two-map constructor: `(ctor id config opts)`.** The DOMAIN map (`config`, 2nd arg)
  carries what to build — `:store-config`, `:comparator`, `:branch`, `:kv-store`,
  `:roots-key`/`:freed-key`, element handlers, `:author`/`:state-key` (cdvcs), …; the
  RUNTIME map (`opts`, 3rd arg) carries ONLY the execution mode `:sync?`. So `opts`
  never holds store/domain arguments. All three arities exist: `(ctor id)`,
  `(ctor id config)`, `(ctor id config opts)`.
- **Memory = a memory-backed store.** Constructing a CRDT without a `:store-config`
  defaults to a fresh in-memory konserve store. There is no separate "in-memory"
  implementation — the memory variant is the durable one over a memory store.
- **Cross-platform via `async+sync`.** Every storage-touching op is written once and
  the macro emits a synchronous body (returns values) or a partial-cps continuation you
  `await`, per the record's sync-mode in `opts` (`{:sync? true|false}`). `:sync? true`
  works on the JVM **and on ClojureScript over a synchronous konserve backend (memory or
  node filestore)** — reads stay on-the-fly even synchronously (a key read is an O(log n)
  range slice). Only a browser **IndexedDB** store has no synchronous konserve API, so
  there you must pass `{:sync? false}` as the constructor's 3rd arg and `await` the results.
- **Conflict-free, except where it deliberately isn't.** `-conflict-free?` is `true`
  for the join-semilattice CRDTs; CDVCS returns `false` (it *lifts* conflict into a
  multiple-head state), and a heterogeneous composite is conflict-free only if all
  its sub-systems are.

## The catalog

| CRDT | ns | value | reads like |
|---|---|---|---|
| **G-Set** (grow-only set) | `yggdrasil.convergent.gset` | a set | `conj` / `elements` / `contains?` |
| **OR-Set** (observed-remove, add-wins) | `yggdrasil.convergent.orset` | adds+removals of `[elem tag]` | `conj` / `disj` / `elements` / `contains?` |
| **2P-Set** (two-phase, permanent remove) | `yggdrasil.convergent.twopset` | adds+removals of elements | `conj` / `disj` / `into` / `elements` / `contains?` |
| **OR-Map** (observed-remove map) | `yggdrasil.convergent.ormap` | adds+removals of `[hk uid k v]` | `assoc` / `dissoc` / `get` / `keys` |
| **Merging-OR-Map** | `yggdrasil.convergent.ormap` (`merging-ormap`) | as OR-Map + a merge-fn | `assoc` / `dissoc` / `get` (folded) / `keys` |
| **LWW-Register** | `yggdrasil.convergent.lwwr` | a single timestamped value | `set-register` / `value` |
| **CDVCS** (versioned, multi-head) | `yggdrasil.convergent.cdvcs` | a commit-graph + heads | `commit` / `merge` / `pull` / `history` |
| **Composite** (product of systems) | `yggdrasil.composite` | a bundle of sub-systems | per-sub, componentwise |

> The collection verbs are bare Clojure collection names. Alias the namespace at the
> call site (`[yggdrasil.convergent.ormap :as om]` → `(om/assoc m k v)`, `(om/get m k)`)
> so they read like Clojure collection ops; only the defining ns excludes them from
> `clojure.core`. `2pset` is `twopset` because a namespace segment can't start with a
> digit.

### Sets — `gset` / `orset` / `twopset`

```clojure
(require '[yggdrasil.convergent.orset :as o]
         '[yggdrasil.convergent :as c])

;; JVM (sync): no :store-config ⇒ a fresh in-memory store; values flow directly
(let [s (o/conj (o/orset "tags") :urgent)
      s (o/conj s :wip)
      s (o/disj s :wip)]
  (o/elements s))            ;; => #{:urgent}   (add-wins: a concurrent re-add survives)

;; cljs (async): runtime opts (the 3rd arg) carry :sync? — domain config is the 2nd
;; (let [s (<? (o/orset "tags" {} {:sync? false})) ...] (<? (o/elements s …)))
```

- **G-Set**: union only; `-join` = set union. The simplest CRDT.
- **OR-Set**: add-wins observed remove — a concurrent add the remover didn't observe
  survives the merge; removal converges (unlike a G-Set's union, which resurrects).
- **2P-Set**: removal is *permanent* (re-adding a removed element keeps it removed).
  `into` bulk-adds.

`contains?` is an O(log n) PSS lookup; `elements` materializes the whole set (O(n) —
it lists everything). `-join`/`merge-peer!` union the two grow-only halves;
`ship!` copies the missing content-addressed nodes to a peer store.

### Maps — `ormap` / `merging-ormap`

```clojure
(require '[yggdrasil.convergent.ormap :as om])

;; plain OR-Map: concurrent writes to a key surface as a value-SET
(let [m (-> (om/ormap "kb") (om/assoc :k 1) (om/assoc :k 2))]
  (om/get m :k))             ;; => #{1 2}

;; Merging-OR-Map: concurrent values FOLD via a merge-fn to a single value
(let [m (-> (om/merging-ormap "kb" max) (om/assoc :k 1) (om/assoc :k 9))]
  (om/get m :k))             ;; => 9
```

The map flattens to a grow-only set of `[hk uid k v]` entries (`hk = (str (hasch/uuid
k))`, a string so ordering is lexical and identical on both platforms). Entries for a
key are contiguous, so `get`/`dissoc` are **range slices** — O(log n + matches), not a
full scan. `keys` (listing all keys) is O(n). The `merge-fn` of a Merging-OR-Map MUST
be commutative, associative and idempotent (a lattice lub — LWW-by-timestamp, `max`,
set-union, deep entity merge); it is wrapped to absorb nils. Construct every replica
with the same `merge-fn`.

### LWW-Register — `lwwr`

A single value tagged with a Hybrid Logical Clock; `-join` keeps the higher-HLC value
(content-hash tiebreak). It is the one **in-memory** CRDT (a register has no large
collection to read on the fly), so it stays synchronous on both platforms.

```clojure
(require '[yggdrasil.convergent.lwwr :as lwwr])
(-> (lwwr/lwwr "cfg") (lwwr/set-register {:theme :dark}) (lwwr/value))
```

### CDVCS — the versioned, multi-head datatype

The catalog's middle ground between a flat CRDT (no conflict) and datahike (single
head, structural 3-way merge): a grow-only commit DAG whose `-join` (≡ replikativ's
`downstream`) always converges but may leave **multiple heads** — that *is* the lifted
conflict. `merge` resolves heads into one via a merge commit. `-conflict-free?` is
`false`. Git-like verbs. See [cdvcs-convergent-system.md](cdvcs-convergent-system.md).

Like the other CRDTs it is **bounded-resident**: the commit-graph is a grow-only PSS
of `[id parents]` (read on the fly — a parent lookup is an O(log n) slice), and only
the derived `:heads` (frontier) + `:version` stay in the record. The graph's roots
cell is the convergent source of truth (grow-map merge ⇒ peers converge on one
content-hash root); `:heads` is a cache that `-join` recomputes. `commit`/`merge`
append an entry; `-join`/`pull` recompute heads via the store-backed LCA;
`history`/`commit-graph` drain the PSS (inherently O(graph)).

### Composite — the product

`yggdrasil.composite` bundles N sub-systems into one. It is the **categorical product**
in the systems category: `-join` is applied per sub (a convergent sub joins by its
semilattice; a versioned sub goes through the 3-way merge path), `capabilities` is the
intersection, and `-conflict-free?` is the conjunction — so a composite of CRDTs *is* a
CRDT, while a composite mixing in a versioned/datahike sub is convergent-ish but not
conflict-free. `commit!` is transactional (flush every sub, then write
`:composite/root` last). See the README's "Multi-System Coordination".

## Storage layout & sync

A durable CRDT store holds:

```
<uuid>          → a PSS B-tree node {:level :keys :addresses}   (content-addressed)
:crdt/roots     → {branch → root-address}  (the live heads; two-half CRDTs key :adds/:removals)
:crdt/freed     → {address → ts}           (GC bookkeeping)
```

- **Snapshot / as-of.** `snapshot-id` is a content hash (for two-half CRDTs, a small
  commit object `{:adds <root> :removals <root>}`); `as-of` re-opens the frozen value.
- **GC.** `gc-sweep!` is mark-and-sweep: reachable nodes from the live + retained
  snapshot roots are kept, the rest swept past a cutoff (`:remove-before` /
  `:grace-period-ms`; epoch default reclaims nothing — pass a window in production).
- **Two joins — same-store vs cross-store.** `-join` (the `PConvergent` method) is the
  *same-store* join: both replicas' values live in one konserve store, so it just
  unions the halves. `merge-peer!` is the *cross-store* counterpart: it first `ship!`s
  the peer's missing nodes into this store, then unions — use it when the two replicas
  are backed by separate stores. Both yield the same converged value; pick by store
  topology.
- **Sync.** Distribution is konserve-sync: because nodes are content-addressed, a peer
  ships only the nodes the destination is missing. `merge-peer!` / `ship!` are the
  store-to-store primitives; no kabel, no op-log — the PSS *is* the sync granularity.

## Choosing a CRDT

| You want… | Use |
|---|---|
| a set that only grows | `gset` |
| a set with convergent removal, re-add allowed | `orset` |
| a set where removal is final | `twopset` |
| a keyed map, concurrent writes surfaced | `ormap` |
| a keyed map, concurrent writes auto-merged | `merging-ormap` + a lattice merge-fn |
| last-writer-wins single value | `lwwr` |
| versioned history with explicit conflict resolution | `cdvcs` |
| N systems moving as one unit | `composite` |
