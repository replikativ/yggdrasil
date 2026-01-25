# Yggdrasil - Unified Copy-on-Write Memory Model

**A cross-language protocol for snapshot isolation, branching, and overlay-based development across heterogeneous systems.**

Named after the Norse world tree connecting all realms - Yggdrasil connects databases, filesystems, containers, and processes under a unified memory model.

---

## Motivation

Modern development involves many systems that independently support copy-on-write semantics:

| System | Snapshot | Branch | Merge | Graph |
|--------|----------|--------|-------|-------|
| Git | commits | branches | 3-way merge | full DAG |
| Datahike | commit-id | branch! | merge! | full DAG |
| Proximum | commit-id | branch! | merge! | full DAG |
| ZFS | snapshots | clones | send/receive | linear |
| Docker | images | tags | layer stacking | layer DAG |
| Nix | derivations | — | — | derivation graph |
| REPL | state capture | — | — | linear |

Each has its own API, naming conventions, and semantics. Yggdrasil provides:

1. **Unified protocols** - same operations across all systems
2. **JSON wire format** - cross-language communication
3. **Reference implementations** - Clojure primary, Python/JS/others
4. **Adapters** - thin wrappers for existing systems (Git, ZFS, Docker)
5. **Overlay abstraction** - live forks with observation modes (for Spindel integration)

---

## Theoretical Foundation

### Parallel Snapshot Isolation (PSI)

Cross-system consistency follows PSI semantics (Raad, Lahav, Vafeiadis):

- **Each system provides internal SI** (Datahike, Proximum already do this)
- **Cross-system: PSI** - long fork anomaly allowed, causality preserved
- **Monotonicity** - splitting transactions never removes behaviors (unlike SI)
- **No global coordination required** - each system manages its own snapshots

### Key Guarantees

| Guarantee | Within System | Cross-System |
|-----------|---------------|--------------|
| Snapshot isolation | ✓ | Per-system only |
| Read-your-writes | ✓ | Within session |
| Monotonic reads | ✓ | Within session |
| Causal consistency | ✓ | Via HLC |
| Serializability | ✓ (if system supports) | NOT guaranteed |

### Hybrid Logical Clocks (HLC)

Causal ordering without synchronized clocks:

```
HLC = (physical_time, logical_counter)

Update rules:
  Local event:  physical = max(local_time, hlc.physical); logical = 0
  On receive:   physical = max(local_time, msg.physical, hlc.physical)
                logical = (same physical ? max(logicals) + 1 : 0)

Comparison: lexicographic on (physical, logical)
```

---

## Protocol Layers

### Layer 1: Snapshotable (fundamental)

Every CoW system can take point-in-time snapshots and read from them.

```
Operations:
  snapshot-id    : System → SnapshotID
  parent-ids     : System → Set<SnapshotID>
  as-of          : System × SnapshotID → ReadView
  snapshot-meta  : System × SnapshotID → Metadata
```

### Layer 2: Branchable (named references)

Named mutable pointers to snapshots (like Git branches, ZFS clones).

```
Operations:
  branches       : System → Set<BranchName>
  current-branch : System → BranchName
  branch!        : System × BranchName × From? → void
  delete-branch! : System × BranchName → void
  checkout       : System × BranchName → System
```

### Layer 3: Graphable (history/DAG)

DAG traversal and ancestry queries.

```
Operations:
  history        : System × Opts? → Seq<SnapshotID>
  ancestors      : System × SnapshotID → Seq<SnapshotID>
  ancestor?      : System × SnapshotID × SnapshotID → Boolean
  common-ancestor: System × SnapshotID × SnapshotID → SnapshotID?
  commit-graph   : System → Graph
```

### Layer 4: Mergeable (combine lineages)

Merge support (not all systems support this).

```
Operations:
  merge!         : System × Source × Strategy → SnapshotID
  conflicts      : System × A × B → Seq<Conflict>
  diff           : System × A × B → Delta
```

### Layer 5: Overlayable (live fork - Spindel-specific)

Live fork that can observe parent's evolution. Three modes:

```
Operations:
  overlay        : System × Opts → Overlay
  advance!       : Overlay → void          (sync to parent, gated mode)
  peek           : Overlay → ReadView      (read parent without advancing)
  base-ref       : Overlay → SnapshotID    (current observation point)
  overlay-writes : Overlay → Delta         (isolated writes)
  merge-down!    : Overlay → void          (push to parent)
  discard!       : Overlay → void          (abandon)

Modes:
  :frozen    - classic snapshot, never sees parent updates
  :following - always sees parent's latest, own writes shadow
  :gated     - sees parent at last advance!, explicit sync points
```

---

## Data Types

### SnapshotRef

Universal reference to a point-in-time snapshot in any system.

```
SnapshotRef:
  system-id    : String     ; identifies the system
  snapshot-id  : String     ; UUID or content-hash (system-native)
  parent-ids   : Set<String>; ancestry
  hlc          : HLC        ; causal timestamp
  content-hash : String?    ; optional, for verification/dedup
```

For content-addressed systems (Git, Docker, IPFS): `snapshot-id` IS the content-hash.
For UUID-based systems (Datahike, Proximum): `snapshot-id` is UUID, content-hash optional.
For name-based systems (ZFS): `snapshot-id` is name, content-hash from metadata.

### HLC (Hybrid Logical Clock)

```
HLC:
  physical : Int64   ; milliseconds since epoch
  logical  : Int32   ; counter for same-ms events
```

### Capabilities

Each system advertises what it supports:

```
Capabilities:
  snapshotable : Boolean
  branchable   : Boolean
  graphable    : Boolean
  mergeable    : Boolean
  overlayable  : Boolean
```

---

## JSON Wire Format

### Request/Response Envelope

```json
{"op": "snapshot-id"}
{"snapshot-id": "550e8400-e29b-41d4-a716-446655440000"}

{"op": "parent-ids", "snapshot-id": "550e8400..."}
{"parent-ids": ["660e8400..."]}

{"op": "branches"}
{"branches": ["main", "feature-x"]}

{"op": "branch!", "name": "feature-y", "from": "main"}
{"created": "feature-y", "snapshot-id": "550e8400..."}

{"op": "history", "limit": 10}
{"history": ["550e8400...", "440e8400..."]}

{"op": "ancestors", "snapshot-id": "550e8400..."}
{"ancestors": ["440e8400...", "330e8400..."]}

{"op": "common-ancestor", "a": "550e8400...", "b": "660e8400..."}
{"common-ancestor": "220e8400..."}

{"op": "capabilities"}
{
  "system-id": "datahike-prod-1",
  "system-type": "datahike",
  "protocols": ["snapshotable", "branchable", "graphable", "mergeable"],
  "version": "1.0"
}
```

### SnapshotRef JSON

```json
{
  "system-id": "datahike-prod-1",
  "snapshot-id": "550e8400-e29b-41d4-a716-446655440000",
  "hlc": {"physical": 1705500000000, "logical": 42},
  "parent-ids": ["660e8400-e29b-41d4-a716-446655440001"],
  "content-hash": null
}
```

---

## Naming Conventions

Aligned across Datahike, Proximum, and wire protocol:

| Yggdrasil | Datahike (current) | Datahike (aligned) | Proximum (current) | Proximum (aligned) |
|-----------|--------------------|--------------------|---------------------|---------------------|
| `snapshot-id` | `commit-id` | `snapshot-id` (alias) | `get-commit-id` | `snapshot-id` |
| `parent-ids` | `parent-commit-ids` | `parent-ids` (alias) | `parents` | `parent-ids` |
| `branches` | — | add | `branches` | keep |
| `current-branch` | via config | add | `get-branch` | `current-branch` |
| `history` | `branch-history` (chan) | `history` (seq) | `history` | keep |
| `ancestors` | — | add | `ancestors` | keep |
| `ancestor?` | — | add | `ancestor?` | keep |
| `common-ancestor` | — | add | `common-ancestor` | keep |
| `merge!` | `merge!` | keep | `merge!` | keep |
| `reset!` | — | add | `reset!` | keep |
| default branch | `:db` | `:main` | `:main` | keep |

---

## Implementation Strategy

### Adapter Pattern

Each system gets a thin adapter that:
1. Wraps the native API
2. Translates native IDs to/from SnapshotRef
3. Advertises capabilities
4. Does NOT duplicate metadata storage

```
┌─────────────────────────────────────────────────────┐
│                 Yggdrasil Protocol                    │
│  (snapshot-id, parent-ids, branches, history, ...)   │
├─────────────────────────────────────────────────────┤
│          Adapter Layer (thin translation)            │
├──────┬──────┬──────┬──────┬──────┬──────────────────┤
│ Git  │ ZFS  │Docker│Datah.│Prox. │ REPL/Process     │
│native│native│native│native│native│ (custom)         │
└──────┴──────┴──────┴──────┴──────┴──────────────────┘
```

### What Adapters Do

| System | What Adapter Wraps | Native ID Format |
|--------|--------------------|------------------|
| Git | `git` CLI or libgit2 | SHA-1/SHA-256 hash |
| ZFS | `zfs` CLI | `pool/dataset@name` |
| Docker | Docker Engine API | `sha256:...` image ID |
| Datahike | Konserve store | UUID |
| Proximum | Internal store | UUID |
| REPL | Var/namespace state | UUID (generated) |

### Integration Points

**Datahike** implements protocols directly (no adapter needed):
- Add `branches`, `current-branch`, `ancestors`, `ancestor?`, `common-ancestor` to versioning API
- Change default branch from `:db` to `:main`
- Add sync `history` function returning seq

**Proximum** implements protocols directly:
- Rename `get-commit-id` → `snapshot-id`
- Rename `get-branch` → `current-branch`
- Rename `parents` → `parent-ids`

**Git/ZFS/Docker** use adapters:
- Thin wrappers calling native CLIs
- Translate output to SnapshotRef format

---

## Overlay Semantics (for Spindel)

The overlay model maps to PSI/RPSI:

| PSI Concept | Overlay Equivalent |
|-------------|--------------------|
| Transaction | Overlay session |
| Snapshot read | `base-ref` (frozen) or `advance!` (gated) |
| Write | `overlay-writes` (isolated) |
| Commit | `merge-down!` |
| Abort | `discard!` |
| Long fork anomaly | Different overlays see different orderings |

### RPSI Compliance

When overlay reads from non-transactional parent:
- Use **sequence lock pattern** (from PSI paper)
- Read parent version → read state → validate version unchanged
- Ensures atomic observation (no partial reads)

### Consistency per Mode

| Mode | SI within overlay | Sees parent updates | Merge required |
|------|-------------------|--------------------|--------------------|
| `:frozen` | ✓ | Never | Always (may conflict) |
| `:following` | — | Continuously | On merge-down (conflicts detected) |
| `:gated` | ✓ (per gate) | At advance! | At advance! or merge-down |

---

## Cross-System Composition

### The Problem

When a single logical operation spans multiple systems (e.g., Datahike transaction + Docker container rebuild + ZFS snapshot), how do we maintain consistency?

### Two Cases

**Same-store composition** (Datahike + fulltext + Proximum):
All indices share konserve/LMDB storage. Since each index produces a new immutable value via structural sharing, a single `multi-assoc` commits them atomically. This is internal to the Datahike adapter — from Yggdrasil's perspective, it's one system.

**Cross-system composition** (Datahike + Docker + ZFS + Git):
These systems cannot share a single storage transaction. Yggdrasil provides **overlay-as-prepare** mechanics but leaves ordering policy to the orchestrating runtime.

### Overlay-as-Prepare Pattern

```
1. prepare-all  → Create overlays on participating systems
2. Execute      → Operations on overlays (isolated, no visible side effects)
3. commit-seq!  → Merge-down overlays in caller-defined order
4. On failure   → Discard remaining overlays
```

Properties:
- **Isolation**: overlays don't affect running systems until merge-down
- **Atomicity per-system**: each merge-down is a single CoW commit
- **Recovery**: discard is free (abandon the overlay, no writes occurred)
- **Idempotent replay**: since commits are CoW, re-pointing a branch reverses a commit

### What Yggdrasil Provides

```clojure
;; yggdrasil.compose namespace
(prepare-all [systems opts])     ; → {system-id → overlay}
(commit-seq! [overlays])         ; → {:committed [...] :failed ... :discarded [...]}
(discard-all! [overlays])        ; cleanup on abort
(snapshot-refs [systems])        ; cross-system checkpoint
```

### What Yggdrasil Does NOT Provide

- **Dependency ordering** — determined by the orchestrating runtime (Spindel)
- **Retry/compensation logic** — application-specific
- **Global serializability** — PSI explicitly trades this for availability
- **Distributed locking** — contradicts CoW philosophy

Spindel uses its signal graph (causal DAG of reactive computations) to determine which systems depend on which, and drives `commit-seq!` accordingly.

### Recovery Model

Since CoW systems are append-only (old snapshots remain accessible):
- If system A commits but system B fails: read A's committed state, retry B
- Or: point A's branch back to its previous snapshot (= undo)
- This is **saga with CoW checkpoints** — simpler than traditional sagas because undo = branch reset

---

## Use Cases

### 1. LLM Coding Agent

```
LLM gets overlay → writes code → tests in overlay → commit or discard
- Safe: can't break shared state until explicit commit
- Explorable: fork overlays for SMC particles
- Reviewable: diff shows code + data impact
```

### 2. Collaborative Development (Simmis)

```
Each user gets overlay → works independently → merge-down with conflict resolution
- GemStone-style session isolation
- Signals show real-time changes (delta perspective)
- 3-way merge UI for code + data + schema
```

### 3. Reproducibility Verification

```
Periodically: create fresh context → replay commits → compare state hash
- Ensures system hasn't become a "snowflake"
- Background process, non-blocking
- Alerts on divergence
```

### 4. Probabilistic Code Synthesis (SMC)

```
Fork N overlays → LLM proposes actions → compute rewards → resample
- Particles = parallel overlays
- Resampling = fork successful, discard failed
- Anglican's SMC directly applicable
```

---

## Related Projects

| Project | Relationship |
|---------|-------------|
| Datahike | Database implementing Snapshotable/Branchable/Graphable/Mergeable |
| Proximum | Vector index implementing same protocols |
| Spindel | FRP runtime consuming overlays, providing execution contexts |
| Simmis | Application built on Spindel+Datahike, demonstrates collaborative coding |
| Anglican | Probabilistic programming (SMC) for code synthesis |
| distributed-scope | Peer-to-peer code execution |
| hasch | Content-addressable hashing for verification |

---

## References

### Academic
- [Parallel Snapshot Isolation](https://plv.mpi-sws.org/transactions/psi-paper.pdf) - Raad, Lahav, Vafeiadis
- [CALM Theorem](https://arxiv.org/abs/1901.01930) - Hellerstein et al.
- [Hybrid Logical Clocks](https://cse.buffalo.edu/~demirbas/publications/hlc.pdf)
- [Schema Evolution in Interactive Programming Systems](https://programming-journal.org/2025/9/2/) - Edwards et al.

### Systems
- [GemStone/S](https://gemtalksystems.com/) - 30+ years of session isolation
- [Glamorous Toolkit](https://gtoolkit.com/) - Moldable development
- [Malleable Systems](https://malleable.systems/) - User-adaptable software
- [Dolt](https://github.com/dolthub/dolt) - Git for data
- [lakeFS](https://lakefs.io/) - Data version control
