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

A CDVCS value is

```clojure
{:commit-graph {commit-id [parent-id ...] ...}   ; a grow-only DAG (G-Set of commits+edges)
 :heads        #{commit-id ...}                   ; a DERIVED projection (frontier − ancestors)
 :version      n}
```

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
| `crdt/cdvcs/meta.cljc` | `lowest-common-ancestors`, `remove-ancestors`, `pairwise-lcas`, `intersection`, `consistent-graph?`, **`downstream`** | `yggdrasil.convergent.cdvcs.graph` — **verbatim** (drop the unused `*date-fn*` require) |
| `crdt/cdvcs/core.cljc` | `new-cdvcs`, `fork`, `commit`/`raw-commit`, `pull`, `merge`, `merge-heads`, `multiple-heads?` | `yggdrasil.convergent.cdvcs` value verbs — port, swap deps (below) |
| `crdt/cdvcs/realize.cljc` | **`commit-history`** (pure DFS linearization) | `…cdvcs.graph/commit-history` — verbatim. The konserve+core.async `commit-value`/`head-value` reduce become `async+sync` blob reads; the eval/reduce stays **app-level**. |

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

- **Commit blobs** (`{commit-id → {:transactions … :parents … :ts … :author …}}`,
  replikativ's `:new-values`) are **content-addressed** by hash → they map
  straight onto yggdrasil's existing durable content-addressed konserve
  substrate (the same `KonserveStorage`/PSS world the durable CRDTs use). A
  commit id IS its content hash.
- **The commit-graph + heads** (small convergent metadata) live in the system's
  root cell, exactly like the durable sets keep `:crdt/roots`.
- **Distribution is konserve-sync + signal-sync**, not kabel: because every
  commit is content-addressed, konserve-sync's reachability node-fetch ships the
  missing commit blobs for free; the metadata graph rides the same signal-sync
  path the other convergent systems use. replikativ's whole `-missing-commits`
  machinery is **subsumed** by content-addressing + konserve-sync.

## API — git-like verbs (NOT collection verbs)

The collection CRDTs got Clojure-collection verbs (`conj`/`assoc`/`get`). CDVCS
is a DVCS, so it keeps **git-like verbs**, which are idiomatic for it:

```clojure
(cdvcs id :author "alice")                 ; new-cdvcs → a durable convergent system
(c/commit cd author)                       ; single-head commit (throws on multiple heads)
(c/branch! cd :feature) / (c/checkout …)   ; native — it is already a DAG
(c/merge cd author remote)                 ; reconcile multiple heads → a merge commit
(c/pull cd remote remote-tip)              ; fast-forward (throws if it would induce a conflict)
(c/multiple-heads? cd)                     ; is it in the lifted-conflict state?
(p/-join cd other)                         ; ≡ downstream — always succeeds, may yield >1 head
(c/history cd) / (c/head-value cd eval-fn) ; linearize; realize value (eval app-level)
```

`-join` (sync, always converges, conflict lifted into heads) and `merge`
(explicit, author-driven, resolves heads) are the two distinct operations — this
is the entire point of the datatype.

## Implementation

The datatype is split into a pure core and a durable wrapping:

- **`yggdrasil.convergent.cdvcs.graph`** — the pure commit-graph algebra
  (`lowest-common-ancestors`, `remove-ancestors`, `downstream`, `commit-history`),
  ported from replikativ's `crdt.cdvcs.meta`. No store, no async, no clock.
- **`yggdrasil.convergent.cdvcs.core`** — the pure value verbs (`new-cdvcs`,
  `commit`, `merge`, `pull`, `fork`) over hasch (commit ids) + `yggdrasil.types`
  (HLC timestamps). No kabel, no core.async — replikativ's op-log/missing-commits
  machinery is replaced by content-addressing + konserve-sync.
- **`yggdrasil.convergent.cdvcs`** — the durable system: a record implementing
  `PConvergent` (`-join` ≡ `downstream`, `-conflict-free?` `false`), `Snapshotable`
  (snapshot-id = content hash of the state; `as-of` restores the frozen state),
  and `GarbageCollectable`, all through `async+sync` (cross-platform). The commit
  graph + heads live in a root cell written *convergently* via `downstream` (a
  shared-store peer joins rather than LWW-clobbers); commit blobs are content-
  addressed by id. `ship!` copies a peer's missing blobs (the commit ids are the
  addresses — konserve-sync transport).

`-join` converges on the metadata graph alone (strong eventual consistency); the
commit blobs are needed only to *realize* a head's value, and `ship!` brings them
across after a join. Realization (linearize via `commit-history`, reduce with an
app-supplied eval-fn) is left to the consumer.

**MergingORMap** — replikativ's pluggable per-key merge — is a small, independent
catalog member; see `yggdrasil.convergent.ormap`.
