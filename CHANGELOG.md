# Changelog

All notable changes to yggdrasil are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/); versions are `0.MINOR.<commit-count>`.

## [0.3] — convergent CRDTs + cross-platform

The headline of 0.3 is a **convergent CRDT catalog** that runs on the JVM *and*
ClojureScript, on the same persistent-sorted-set + konserve substrate as the snapshot
registry. This is a breaking release (API renames + one on-disk registry change); a
migration is provided.

### Added

- **Convergent CRDT catalog** (`yggdrasil.convergent.*`) — see [doc/crdts.md](doc/crdts.md):
  - `gset` (grow-only set), `orset` (observed-remove, add-wins), `twopset` (two-phase,
    permanent remove), `ormap` (observed-remove map), `merging-ormap` (per-key values
    folded by a lattice merge-fn), `lwwr` (LWW-register, HLC).
  - `cdvcs` — a versioned, multi-head datatype ported from replikativ: a grow-only
    commit DAG whose join converges but lifts conflict into multiple heads, resolved by
    an explicit `merge`. See [doc/cdvcs-convergent-system.md](doc/cdvcs-convergent-system.md).
  - All durable by construction (content-addressed PSS over konserve), **read on the
    fly** (per-key reads are O(log n) range slices through an in-memory node cache — no
    full in-memory materialization), and cross-platform via `async+sync` (sync on JVM,
    CPS on cljs).
- **Cross-platform core.** `storage`, `registry`, `workspace` (and the convergent
  catalog) are now `.cljc` — yggdrasil's durable systems run in the browser/Node.
- `yggdrasil.composite` `-join` — composites are the **product** in the systems
  category: conflict-free iff every sub is; mixed (versioned + convergent) composites
  route each sub through its own join/merge path.
- `yggdrasil.migrate/migrate-registry-0.2->0.3!` — registry store migration (below).
- Datahike-style LRU node cache in `storage` (bounded; no `core.cache` dependency —
  inlined cross-platform, like `datahike.lru`).

### Changed (breaking)

- **CRDT API uses bare Clojure collection verbs.** Alias the namespace at the call
  site; only the defining ns excludes them from `clojure.core`:
  - sets: `add → conj`, `remove-elem → disj`, `contains-elem? → contains?`,
    2P-Set `add-all → into`
  - OR-Map: `assoc-key → assoc`, `dissoc-key → dissoc`, `lookup → get`,
    `ormap-keys → keys`
  - LWW-Register unchanged (`set-register` / `value`)
- **Namespace renames** (the `durable-` prefix is dropped — there is one
  implementation per CRDT now): `convergent.durable-gset → convergent.gset`,
  `durable-orset → orset`, `durable-2pset → twopset`, `durable-ormap → ormap`
  (`durable-merging-ormap → merging-ormap`). The substrate ns
  `yggdrasil.convergent.durable` keeps its name.
- **No separate in-memory CRDTs.** The pure-map in-memory G-Set / OR-Map / Merging-
  OR-Map are removed; the (formerly `durable-`) versions are canonical and default to
  an in-memory konserve store when constructed without one. Consequence: collection
  CRDTs are **asynchronous on ClojureScript** (`:sync? false` + `await`). `lwwr`
  remains in-memory and synchronous on both platforms.
- The **snapshot registry is now a durable 2P-Set** (adds + removals halves), giving
  it convergent removal; queries are unchanged.

### Migration

**Source.** Update callers for the verb + namespace renames above. On ClojureScript,
collection-CRDT (and registry) operations now return continuations — construct with
`:sync? false` and `await`.

**On-disk (registry only).** The PSS *node* format is unchanged, but the registry's
root cell moved from `:registry/roots {:tsbs <root>}` (a single index) to
`:crdt/roots {:adds <root> :removals <root>}` (a 2P-Set). The durable CRDT catalog is
new in 0.3, so it has no prior data to migrate; adapter stores (datahike/git/…) are
untouched. To migrate a 0.2 registry store in place:

```clojure
(require '[yggdrasil.migrate :as migrate])
(migrate/migrate-registry-0.2->0.3! {:backend :file :path "/var/lib/app/registry" :id (random-uuid)})
```

It is idempotent (a store already on 0.3 is a no-op) and leaves the old cells in place;
a windowed `registry/gc!` reclaims them.

## [0.2] and earlier

Adapter framework (git, IPFS, ZFS, Btrfs, OverlayFS, Podman, datahike, LakeFS, Dolt,
Iceberg), the protocol layers (Snapshotable / Branchable / Graphable / Mergeable /
Watchable / Addressable / Committable / GarbageCollectable), the HLC-indexed snapshot
registry, workspaces, and composite systems. See the git history and `README.md`.
