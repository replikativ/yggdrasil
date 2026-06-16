# Changelog

All notable changes to yggdrasil are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/); versions are `0.MINOR.<commit-count>`.

## [0.3] — convergent CRDTs + cross-platform

The headline of 0.3 is a **convergent CRDT catalog** that runs on the JVM *and*
ClojureScript, on the same persistent-sorted-set + konserve substrate as the snapshot
registry. It is **additive** for existing 0.2 API (the protocol / adapter / workspace /
registry surfaces are preserved); the one breaking change is the snapshot registry's
on-disk format, for which an in-place migration is provided.

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
    full in-memory materialization), and cross-platform via `async+sync`.
  - The collection CRDTs read like Clojure collections — `conj`/`disj`/`contains?`/
    `into` (sets), `assoc`/`dissoc`/`get`/`keys` (maps) — aliased at the call site. They
    are **synchronous on the JVM and asynchronous on ClojureScript** (`:sync? false` +
    `await`); `lwwr` is in-memory and synchronous on both.
- **Cross-platform core.** `storage`, `registry`, `workspace` (and the convergent
  catalog) are now `.cljc` — yggdrasil's durable systems run in the browser/Node.
- `yggdrasil.composite` `-join` — composites are the **product** in the systems
  category: conflict-free iff every sub is; mixed (versioned + convergent) composites
  route each sub through its own join/merge path.
- `yggdrasil.migrate/migrate-registry-0.2->0.3!` — registry store migration (below).
- Datahike-style LRU node cache in `storage` (bounded; no `core.cache` dependency —
  inlined cross-platform, like `datahike.lru`).

### Changed (breaking)

- **The snapshot registry's on-disk format changed.** It is now a durable **2P-Set**
  (adds + removals halves) — giving it convergent removal — instead of a single index.
  The PSS *node* format is unchanged, but the root cell moved from
  `:registry/roots {:tsbs <root>}` to `:crdt/roots {:adds <root> :removals <root>}`. The
  public registry API (`create-registry` / `register!` / `deregister!` / queries) and
  all other 0.2 surfaces are unchanged; only the persisted bytes differ, so a 0.2
  registry store needs the one-time migration below. (Adapter stores — datahike/git/… —
  are untouched; the convergent CRDT catalog is new, so it has no prior data.)

### Migration

A 0.2 deployment upgrades with **no source changes** — the protocol, adapter,
workspace, and registry APIs are preserved. The only step is migrating an existing
**registry** store in place:

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
