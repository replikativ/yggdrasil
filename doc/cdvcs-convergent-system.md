# CDVCS as a durable convergent yggdrasil system

Status: design (task #132). Porting replikativ's CDVCS (Confree Distributed
Version Control System) into yggdrasil's convergent catalog.

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

## Status — DONE (steps 1–4), MergingORMap optional

All four steps landed and are green on JVM **and** cljs/node:
- `cdvcs.graph` + `cdvcs.core` (pure) — commit `9ae8018`.
- `yggdrasil.convergent.cdvcs` durable system + cross-store spike — commit `81f490f`.
- Suite: JVM convergent 129/451, cljs node 68/243, 0 failures.

Implemented exactly as designed below: content-addressed commit blobs, the
convergent state cell written via `downstream` (so a shared-store peer joins, never
LWW-clobbers), `-join`≡downstream / `-conflict-free? false`, content-hash
`snapshot-id`/`as-of`, key-spared GC, git-like verbs, and `ship!` (copy missing
blobs by id — konserve-sync transport, no kabel). The cross-store spike proves
`-join` converges on metadata ALONE (strong eventual consistency), with `ship!`
making full history readable on both peers afterward.

Remaining optional: **MergingORMap**; and L4/L5 (spindel signal + dvergr/simmis
adoption) when a consumer needs it.

## Difficulty & sequencing — MEDIUM (as built)

1. **`cdvcs.graph` (pure)** — port `meta.cljc` + `commit-history` verbatim to
   `.cljc`; portable test: `downstream` is commutative/associative/idempotent;
   LCA on hand-built graphs; head recomputation. *No store, no async — fast,
   decisive.* ← the proven heart.
2. **`cdvcs` value verbs** — port `core.cljc` (`new-cdvcs`/`commit`/`merge`/
   `pull`/`fork`) over hasch + `types/*now-fn*`; portable test: linear history,
   divergence → 2 heads, `merge` → single head with a merge commit, `pull`
   fast-forward + the conflict guard.
3. **Durable wrapping** — a record implementing `PConvergent` (`-join`=downstream),
   `Snapshotable` (snapshot-id = content hash of the graph; `as-of` restores a
   frozen graph), `Branchable`/`Overlayable` (native — it is a DAG), storing the
   commit-graph in the root cell and commit blobs content-addressed, all through
   `async+sync` (cross-platform like the durable sets). GC: retain commits
   reachable from retained snapshot heads (same retain-roots pattern as 2P-Set).
4. **Decisive cross-peer spike** — two peers commit concurrently into separate
   stores, `ship!`/konserve-sync the blobs, `-join` the graphs → both converge to
   the same 2-head value; `merge` on one → single head; verify on JVM **and**
   cljs/node (the portable-harness pattern that caught the to-array/adopt bugs).

Optional alongside: **MergingORMap** (replikativ's pluggable per-key merge) — a
small additive catalog member, independent of CDVCS.

The pure graph core (steps 1–2) is the proven, portable heart and ports almost
verbatim; the integration (step 3) follows the now-cross-platform durable-CRDT
pattern; step 4 is the same decisive-spike discipline as the rest of the catalog.
No kabel, no core.async — `async+sync` + konserve-sync throughout.
