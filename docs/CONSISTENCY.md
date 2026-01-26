# Consistency Model

Yggdrasil provides **Parallel Snapshot Isolation (PSI)** semantics across heterogeneous storage systems. This document explains the consistency guarantees and how they map to the protocol stack.

## Parallel Snapshot Isolation

PSI is a relaxation of classical Snapshot Isolation that permits greater concurrency while maintaining strong consistency guarantees. The key properties are:

1. **Snapshot reads**: All reads within a transaction see a consistent snapshot of the database
2. **Causal ordering**: If transaction T1 commits before T2 begins, T2 sees T1's writes
3. **Write-conflict detection**: Concurrent transactions that write to the same data conflict

### How Yggdrasil Implements PSI

| PSI Concept | Yggdrasil Protocol | Implementation |
|-------------|-------------------|----------------|
| Snapshot | `Snapshotable.snapshot-id` | Immutable point-in-time reference |
| Snapshot read | `Snapshotable.as-of` | Read-only view at specific commit |
| Causal ordering | `Graphable.ancestor?` | DAG ancestry relationships |
| Causal timestamps | `HLC` (Hybrid Logical Clock) | Lamport clock + physical time |
| Write-conflict | `Mergeable.conflicts` | Detects concurrent modifications |
| Commit | `Snapshotable` via fixture | Atomically persists changes |

### Snapshot Isolation Guarantees

When using yggdrasil protocols correctly:

```clojure
;; Transaction T1
(let [snap-before (p/snapshot-id sys)]
  (mutate! sys)
  (commit! sys "T1")
  ;; All reads during mutation saw snap-before state
  )

;; Transaction T2 (concurrent)
(let [snap (p/snapshot-id sys)]
  ;; T2 sees either pre-T1 or post-T1 state, never partial
  (read-data sys))
```

The `as-of` protocol method enables reading historical snapshots:

```clojure
;; Read exactly what existed at a prior commit
(let [old-view (p/as-of sys "abc123")]
  (read-data old-view))
```

### Conflict Detection

The `conflicts` method in `Mergeable` detects write-write conflicts:

```clojure
(let [conflicts (p/conflicts sys snap-a snap-b)]
  (if (empty? conflicts)
    (p/merge! sys :feature)  ; Safe to merge
    (handle-conflicts conflicts)))
```

Conflicts occur when:
- Both branches modified the same file/key
- Modifications are not identical
- Neither change is an ancestor of the other

### Causal Ordering with HLC

Hybrid Logical Clocks provide causal ordering without synchronized physical clocks:

```clojure
(require '[yggdrasil.types :as t])

;; Create timestamp for local event
(def ts1 (t/hlc-now))

;; Advance for subsequent local event
(def ts2 (t/hlc-tick ts1))

;; Update on receiving remote timestamp
(def ts3 (t/hlc-receive ts2 remote-ts))

;; Compare for ordering
(t/hlc-compare ts1 ts2)  ; => -1 (ts1 < ts2)
```

HLC guarantees:
- **Local monotonicity**: Successive local events have increasing timestamps
- **Causality**: If event A causally precedes B, then `hlc(A) < hlc(B)`
- **Bounded drift**: HLC stays close to physical time

## Adapter-Specific Semantics

Different backends provide different levels of isolation:

| Adapter | Isolation Level | Notes |
|---------|----------------|-------|
| Git | Full PSI | Atomic commits, DAG ancestry |
| Datahike | Full PSI | ACID transactions, immutable history |
| Scriptum | Full PSI | Append-only generations |
| Btrfs | PSI with file-level conflicts | COW snapshots, no built-in merge |
| ZFS | Snapshot isolation only | No merge support |
| OverlayFS | PSI with file-level conflicts | Archive-based snapshots |
| Podman | PSI with layer-level conflicts | Container image layers |

### Branch Isolation

Each branch operates independently:

```clojure
(p/branch! sys :feature)
(let [main-sys (p/checkout sys :main)
      feat-sys (p/checkout sys :feature)]
  ;; Writes to feat-sys are invisible to main-sys
  ;; Until merge! is called
  )
```

For mutable adapters (Git, Btrfs, OverlayFS, Podman), the system object itself is mutable, so explicit checkout is required between branch switches.

## Multi-System Composition

The `yggdrasil.compose` namespace enables atomic operations across multiple systems:

```clojure
(require '[yggdrasil.compose :as compose])

;; Prepare overlays on multiple systems
(let [overlays (compose/prepare-all [sys-a sys-b] {:mode :gated})]
  ;; Make changes to overlays...

  ;; Commit all atomically (in causal order)
  (compose/commit-seq! (vals overlays)))
```

This provides PSI guarantees across heterogeneous storage systems by:
1. Creating isolated overlays for each system
2. Applying changes to overlays (invisible to other readers)
3. Committing in sequence with causal ordering

## References

### Academic Foundations

1. **Parallel Snapshot Isolation**: Raad, A., Lahav, O., & Vafeiadis, V. (2018). "On Parallel Snapshot Isolation and Release/Acquire Consistency." *ESOP 2018*. Provides formal semantics for PSI and its relationship to memory models.

2. **Snapshot Isolation**: Berenson, H., et al. (1995). "A Critique of ANSI SQL Isolation Levels." *SIGMOD 1995*. Original definition of Snapshot Isolation.

3. **Hybrid Logical Clocks**: Kulkarni, S., et al. (2014). "Logical Physical Clocks and Consistent Snapshots in Globally Distributed Databases." *OPODIS 2014*. HLC design and implementation.

### Implementation References

- **Git**: Uses SHA-1/SHA-256 content addressing for immutable snapshots
- **ZFS/Btrfs**: Copy-on-write filesystems with atomic snapshot primitives
- **Datahike**: Persistent data structures with structural sharing

## Best Practices

### For Correct PSI Semantics

1. **Always read from a snapshot**: Use `as-of` or capture `snapshot-id` at transaction start
2. **Check conflicts before merge**: Call `conflicts` to detect write-write conflicts
3. **Use HLC for cross-system ordering**: Include HLC timestamps in `SnapshotRef` for causal ordering
4. **Commit atomically**: Use `commit-seq!` for multi-system transactions

### For Performance

1. **Minimize snapshot lifetime**: Long-running snapshots may block garbage collection
2. **Merge frequently**: Reduces conflict scope and snapshot divergence
3. **Use appropriate adapter**: Choose based on isolation requirements (see table above)
