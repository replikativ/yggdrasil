# CDVCS as a durable convergent yggdrasil system

CDVCS (Confree Distributed Version Control System) is a versioned, multi-head
convergent datatype ported from replikativ into yggdrasil's convergent catalog.

## Why CDVCS belongs here

yggdrasil already has the two ends of the conflict spectrum:

- **datahike** — single-head, 3-way structural merge (a real merge function).
- **flat CRDTs** (G/OR/2P-Set, OR-Map, LWWR) — no conflict ever: the join is a
  semilattice least-upper-bound; concurrent writes always reconcile by algebra.

**CDVCS is the missing middle.** It lifts conflict *into its own representation*
— concurrent edits produce a value with **multiple heads** — so the metadata
graph still converges (network sync never blocks), while the *resolution* of the
divergence is deferred to an explicit `merge` (which records a merge commit). It
is git's model made convergent: a grow-only commit DAG plus a derived head set.

It is also our own invention worth keeping.

## The key insight: the commit-graph is a convergent value

A CDVCS value is *logically*

```clojure
{:commit-graph {commit-id [parent-id ...] ...}   ; a grow-only DAG (G-Set of commits+edges)
 :heads        #{commit-id ...}                   ; a DERIVED projection (frontier − ancestors)
 :version      n}
```

(In the **durable** record this is stored as a grow-only PSS of `[id value]` entries —
the whole commit value inlined beside its id, the value carrying `:parents` — plus a
small `{:heads :version}` cache cell. The `{commit-id [parent-id …]}` map above is the
logical projection produced by `(commit-graph cd)`, not a stored field.)

- The **commit-graph is monotonic** — commits and their parent edges are only
  ever added (content-addressed by hash, so additions are idempotent).
- The **heads are a pure projection** of the graph: the frontier minus everything
  reachable as an ancestor.
- replikativ's `downstream` (merge the two graphs, recompute heads via the
  lowest-common-ancestor cut) is **exactly a join**: commutative, associative,
  idempotent on the graph. So:

```
yggdrasil  -join   ≡   replikativ  downstream
yggdrasil  -conflict-free?  =  false   (heads can be > 1 — that IS the lifted conflict)
```

CDVCS therefore implements `PConvergent` cleanly. It is a **convergent system
whose value is "a versioned DAG that may have several heads"**, not a flat CRDT
(it is `-conflict-free? false`), which is precisely the catalog slot between
datahike and the flat CRDTs.

## What ports, and from where

replikativ source (all already `.cljc`, ~340 LOC of pure logic worth porting):

| replikativ file | what | yggdrasil home |
|---|---|---|
| `crdt/cdvcs/meta.cljc` | `lowest-common-ancestors`, `remove-ancestors`, `pairwise-lcas`, `intersection`, `consistent-graph?`, **`downstream`** | the in-memory algebra ported **verbatim** to `yggdrasil.convergent.cdvcs.graph`, and re-expressed store-backed (`async+sync`, parents via an injected `parents-of` accessor) in **`yggdrasil.convergent.cdvcs.graph-store`** (the production path) |
| `crdt/cdvcs/core.cljc` | `new-cdvcs`, `fork`, `commit`/`raw-commit`, `pull`, `merge`, `merge-heads`, `multiple-heads?` | the durable value verbs live directly in `yggdrasil.convergent.cdvcs` (`commit`/`merge`/`pull`/`history`), built on the tiny pure builders in **`yggdrasil.convergent.cdvcs.commit`** (`make-commit`/`new-base`) |
| `crdt/cdvcs/realize.cljc` | **`commit-history`** (pure DFS linearization) | `…cdvcs.graph-store/commit-history` (store-backed); realization (the eval/reduce over a head's history) stays **app-level**. |

> **Note on `cdvcs.graph` / `cdvcs.core`:** the pure in-memory ports (`cdvcs.graph` +
> the pure value verbs `cdvcs.core`) are NOT shipped — they live under
> `test/yggdrasil/convergent/cdvcs/{graph,core}.cljc` as a **correctness oracle** the
> property test checks the production store-backed algebra (`cdvcs.graph-store`) against.
> Production ships only `cdvcs` (durable) + `cdvcs.commit` (builders) + `cdvcs.graph-store`.

**Dependency swaps** (this is the whole "make it a yggdrasil system" delta):

| replikativ dep | yggdrasil replacement |
|---|---|
| `*id-fn*` (hasch content hash of the commit) | hasch directly (yggdrasil already content-addresses with it) |
| `*date-fn*` | `yggdrasil.types/*now-fn*` (HLC) — injectable, replay-safe |
| `kabel.platform-log` debug/info | drop (or telemere if wanted) |
| `PExternalValues` / `-missing-commits` / kabel / superv.async / core.async | **DROP entirely** — see sync below |
| `map->CDVCS` record + `:downstream` op for kabel | the `-join` value + record swap; no wire op needed |

So the port removes kabel, superv.async, and core.async — the heaviest part of
replikativ's stack — because yggdrasil distributes differently.

## Storage & sync — no kabel needed

- **Option B — commit values are INLINED, no separate blob layer.** The commit-graph is
  a grow-only PSS of `[id value]` entries (`:cdvcs/graph` roots cell): each entry stores
  the whole commit value (`{:transactions … :parents … :ts … :author …}`) beside its id.
  The id is still the content hash of `{:transactions :parents}`, but there is no separate
  content-addressed commit-blob map — the value rides inline in the PSS node. This trades
  per-commit structural sharing of values for ZERO round-trip: a synced graph node already
  carries its commit, so a peer reads it (parents and all) without a follow-up fetch. The
  graph is read on the fly (`parents-of` slices the `[id value]` entry and reads `:parents`,
  O(log n); the store-backed LCA/history live in `cdvcs.graph-store`). The grow-map roots
  cell is the convergent source of truth.
- **Only the graph is convergent; heads are a recomputed cache.** The derived `:heads` +
  `:version` sit in a small `:cdvcs/state` LWW CACHE cell (heads can't be convergent — a
  commit removes its parents from heads — so `-join` recomputes them; a peer's heads
  reflect its last `-join`).
- **Distribution is konserve-sync + signal-sync**, not kabel: the PSS nodes are themselves
  content-addressed + write-once, so konserve-sync's reachability node-fetch ships the
  missing **graph nodes** (commit values ride inline) for free; the metadata rides the same
  signal-sync path the other convergent systems use. replikativ's whole `-missing-commits`
  machinery is **subsumed** by content-addressing + konserve-sync.

## API — git-like verbs (NOT collection verbs)

The collection CRDTs got Clojure-collection verbs (`conj`/`assoc`/`get`). CDVCS
is a DVCS, so it keeps **git-like verbs**, which are idiomatic for it:

```clojure
(require '[yggdrasil.convergent.cdvcs :as cd]    ; the verbs live in this ns
         '[yggdrasil.convergent :as c])          ; the join law (-join)

(cd/cdvcs id {:author "alice"})            ; open/create a durable convergent system
                                           ;   (cdvcs id config opts): config {:author :store-config …}, opts {:sync?}
(cd/commit cd author transactions)         ; single-head commit (throws on multiple heads)
(cd/merge cd author remote)                ; reconcile heads → a merge commit
                                           ;   (optional 4th arg: correcting-transactions; trailing opts)
(cd/pull cd remote remote-tip)             ; fast-forward (throws if it would induce a conflict)
(cd/heads cd) / (cd/multiple-heads? cd)    ; the head set / is it in the lifted-conflict state?
(c/-join cd other)                         ; ≡ downstream — always succeeds, may yield >1 head
                                           ;   ([a b] / [a b opts]); -apply-delta is the OP-path counterpart
(cd/history cd)                            ; linearize the single head (DFS); realize the value app-level
(cd/full-delta cd) / (cd/apply-delta cd δ) ; full state as a δ / integrate a peer's δ (the sync hooks)
(cd/ship! cd dst-store)                    ; copy missing graph PSS nodes to another store
```

CDVCS does **not** expose branching: `Branchable` is a no-op stub (`branches` ⇒ `#{:main}`,
`branch!`/`checkout` return `this` unchanged, `capabilities` reports `:branchable false`).
Divergence lives in the multi-head set, resolved by `merge`, not by named branches.

`-join` (always converges, conflict lifted into heads) and `merge` (explicit,
author-driven, resolves heads) are the two distinct operations — this is the entire
point of the datatype. Like the rest of the catalog, every verb takes an optional
trailing `opts {:sync?}` (default `c/default-opts` — sync on JVM, async on cljs); the
record carries no stamped sync-mode, and `-join`/`-apply-delta` are 2-arity or 3-arity.

## Implementation

The PRODUCTION code (in `src`) is:

- **`yggdrasil.convergent.cdvcs.graph-store`** — the store-backed commit-graph algebra
  (`lowest-common-ancestors`, `remove-ancestors`, `commit-history`), `async+sync`, with a
  commit's parents read through an injected `parents-of` accessor (an O(log n) PSS slice)
  so the full graph is never resident.
- **`yggdrasil.convergent.cdvcs.commit`** — the tiny pure commit builders (`make-commit`,
  `new-base`) over hasch (commit ids) + `yggdrasil.types/now-ms` (HLC ts). Store-free.
- **`yggdrasil.convergent.cdvcs`** — the durable system: a record implementing
  `PConvergent` (`-join` ≡ `downstream`, `-conflict-free?` `false`), `PDeltaApply`
  (`-apply-delta` — the OP path for signal-sync), `Snapshotable` (snapshot-id = content
  hash of `{:graph root :heads :version}`; `as-of` restores the frozen state),
  `Committable`, and `GarbageCollectable`, all through `async+sync` (cross-platform). The
  GRAPH lives in a `:cdvcs/graph` roots cell written *convergently* (a shared-store peer
  joins the grow-map rather than LWW-clobbers); the derived `:heads`/`:version` live in a
  separate `:cdvcs/state` LWW cache cell that `-join` recomputes. Commit values ride
  INLINE in the graph PSS nodes, so `ship!` copies a peer's missing **graph PSS nodes**
  (addressed by node hash) — no separate blob layer.

The pure in-memory `cdvcs.graph` + value verbs `cdvcs.core` are the **test-only oracle**
(`test/yggdrasil/convergent/cdvcs/{graph,core}.cljc`) the property test checks the
store-backed algebra against — they are not shipped.

`-join` converges on the commit graph alone (strong eventual consistency); a head's value
is realized by linearizing via `(history cd)` + `read-commit` and reducing with an
app-supplied eval-fn (left to the consumer). After a join, `ship!` brings any missing
graph nodes across (the inlined commit values come with them).

**MergingORMap** — replikativ's pluggable per-key merge — is a small, independent
catalog member; see `yggdrasil.convergent.ormap`.
