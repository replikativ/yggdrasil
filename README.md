# Yggdrasil

[![CircleCI](https://circleci.com/gh/replikativ/yggdrasil.svg?style=shield)](https://circleci.com/gh/replikativ/yggdrasil)
[![Clojars](https://img.shields.io/clojars/v/org.replikativ/yggdrasil.svg)](https://clojars.org/org.replikativ/yggdrasil)
[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)

> *In Norse mythology, [Yggdrasil](https://en.wikipedia.org/wiki/Yggdrasil) is the immense World Tree whose branches extend into the heavens and whose roots reach into disparate realms, connecting diverse worlds into a unified cosmos.*

Unified copy-on-write memory model protocols for heterogeneous storage systems.

Yggdrasil defines a layered protocol stack that abstracts over Git, ZFS, Btrfs, OverlayFS, Podman, IPFS, Iceberg, Datahike, LakeFS, Dolt, [Proximum](https://github.com/replikativ/proximum), and [Scriptum](https://github.com/replikativ/scriptum), enabling cross-system composition with causal consistency guarantees.

## Installation

Add to your dependencies: [![Clojars](https://img.shields.io/clojars/v/org.replikativ/yggdrasil.svg)](https://clojars.org/org.replikativ/yggdrasil)

**Python:**
```bash
pip install yggdrasil-protocols
```

## Protocol Layers

| Layer | Protocol | Purpose |
|-------|----------|---------|
| 0 | `SystemIdentity` | System identification and capability advertisement |
| 1 | `Snapshotable` | Point-in-time immutable snapshots |
| 2 | `Branchable` | Named mutable references (branches) |
| 3 | `Graphable` | DAG traversal, history, ancestry |
| 4 | `Mergeable` | Three-way merge, conflict detection |
| 5 | `Overlayable` | Live forks with observation modes |
| 6 | `Watchable` | State change observation (polling) |
| 7 | `GarbageCollectable` | Coordinated cross-system GC (optional) |
| - | `Addressable` | Filesystem working path (optional) |
| - | `Committable` | Explicit commit operations (optional) |

### Snapshotable

```clojure
(require '[yggdrasil.protocols :as p])

(p/snapshot-id sys)          ; => "abc123" (current snapshot ID)
(p/parent-ids sys)           ; => #{"parent-id"}
(p/as-of sys "abc123")       ; => read-only view at that snapshot
(p/snapshot-meta sys "abc123") ; => {:timestamp ..., :message ..., ...}
```

### Branchable

```clojure
(p/branches sys)             ; => #{:main :feature}
(p/current-branch sys)       ; => :main
(p/branch! sys :feature)     ; create branch from current HEAD
(p/branch! sys :fix "abc123") ; create branch from specific snapshot
(p/checkout sys :feature)    ; switch to branch
(p/delete-branch! sys :feature)
```

### Graphable

```clojure
(p/history sys)              ; => ["newest" "older" ...]
(p/history sys {:limit 5})   ; => last 5 commits
(p/ancestors sys "abc123")   ; => all ancestor IDs
(p/ancestor? sys "a" "b")   ; => true if a is ancestor of b
(p/common-ancestor sys "a" "b") ; => merge-base ID
(p/commit-graph sys)         ; => {:nodes {...} :branches {...} :roots #{...}}
```

### Mergeable

```clojure
(p/merge! sys :feature)                    ; merge branch
(p/merge! sys :feature {:message "Merge"}) ; with message
(p/conflicts sys "a" "b")                  ; => [] or [{:path ...}]
(p/diff sys "a" "b")                       ; => delta between snapshots
```

### Watchable

```clojure
(let [watch-id (p/watch! sys (fn [event]
                               (println (:type event))) ; :commit, :branch-created, etc.
                             {:poll-interval-ms 500})]
  ;; ... later ...
  (p/unwatch! sys watch-id))
```

### Addressable

```clojure
(require '[yggdrasil.protocols :as p])

(p/working-path sys)  ; => "/path/to/repo-worktrees/feature"
```

### Committable

```clojure
(p/commit! sys)                              ; commit with no message
(p/commit! sys "Add feature X")              ; with message
(p/commit! sys "IPFS snapshot" {:root "Qm…"}) ; adapter-specific opts
```

## Adapters

### Capabilities Matrix

| Adapter | Snapshot | Branch | Graph | Merge | Overlay | Watch | GC | Addressable | Committable |
|---------|----------|--------|-------|-------|---------|-------|----|-------------|-------------|
| Git | commits | branches (worktrees) | full DAG | 3-way | - | poll | yes | yes | yes |
| ZFS | snapshots | clones | linear | - | - | poll | yes | yes | yes |
| Btrfs | ro snapshots | subvolumes | full DAG | file-level | - | poll | yes | yes | yes |
| IPFS | commit CIDs | IPNS names | full DAG | manual | - | poll | yes | - | yes |
| Iceberg | snapshots | branches | full DAG | manual | - | poll | yes | - | yes |
| Datahike | commit-id | branch! | full DAG | merge! | - | listen | yes | - | - |
| Scriptum | generations | COW dirs | full DAG | add-only | - | - | yes | yes | yes |
| OverlayFS | upper dir archives | overlay dirs | full DAG | file-level | - | poll | yes | yes | yes |
| Podman | image layers | containers | full DAG | diff+apply | - | poll | yes | - | yes |
| LakeFS | commits | branches | full DAG | 3-way | - | poll | yes | - | yes |
| Dolt | commits | branches | full DAG | 3-way | - | poll | yes | yes | yes |

### Git Adapter

Uses **git worktrees** for concurrent branch access. Each branch gets its own working directory and index, enabling safe parallel operations across branches.

```clojure
(require '[yggdrasil.adapters.git :as git])

;; Initialize new repo
(def sys (git/init! "/path/to/repo" {:system-name "my-repo"}))

;; Or open existing repo
(def sys (git/create "/path/to/existing" {:system-name "my-repo"}))
```

**Architecture:**
- Main branch lives at `repo-path`
- Other branches live at `<repo-path>-worktrees/<branch-name>`
- Per-branch `ReentrantLock` serializes concurrent writes to the same branch
- Observer mode: poll function detects external `git checkout`, commits, and branch operations

### IPFS Adapter

P2P version control with content-addressed commits on IPFS. Uses IPNS for branch management and decentralized propagation.

```clojure
(require '[yggdrasil.adapters.ipfs :as ipfs])

;; Initialize system
(def sys (ipfs/init! {:system-name "dbpedia"}))

;; User adds data to IPFS
(shell "ipfs add -r dbpedia-jan/")  ; => QmXxx...

;; Yggdrasil tracks version
(p/commit! sys "DBpedia January 2024" {:root "QmXxx..."})

;; Branch and merge
(p/branch! sys :experimental)
(p/checkout sys :experimental)
(p/commit! sys "Experimental data" {:root "QmYyy..."})
(p/checkout sys :main)
(p/merge! sys :experimental {:root "QmMerged..." :message "Merge experimental"})

;; Cleanup
(ipfs/destroy! sys {:delete-state? true :delete-keys? false})
```

**Architecture:**
- Commits stored as DAG-JSON in IPFS (content-addressed)
- Branches are IPNS names (mutable pointers to commit CIDs)
- State file tracks branch metadata and cached CIDs
- IPNS publish: ~60s with `--allow-offline` (2-5 min for full DHT propagation)
- User manages data (Yggdrasil tracks versions only)
- Manual merge: user provides merged data root CID

**Requirements:** Running IPFS daemon (`ipfs daemon`)

**Use cases:**
- Monthly dataset releases (DBpedia, Wikidata)
- Reproducible data science (content-addressed training data)
- Decentralized build artifacts

### Iceberg Adapter

Git-like version control for data lakes on object storage (S3, HDFS, etc.). Tracks table metadata and snapshots without moving data.

```clojure
(require '[yggdrasil.adapters.iceberg :as ice])

;; Initialize new table
(def sys (ice/init! "s3://my-bucket/warehouse"
                    "db.table"
                    {:spark-config {...}}))

;; Create Spark session and register table
(def spark (ice/create-spark-session sys))

;; Write data via Spark
(.write (.format df "iceberg") "db.table")

;; Yggdrasil tracks snapshots
(p/commit! sys "Initial data load")

;; Branch for experimental schema changes
(p/branch! sys :schema-v2)
(p/checkout sys :schema-v2)
;; ... modify table schema via Spark ...
(p/commit! sys "Add new columns")

;; Merge schema changes back
(p/checkout sys :main)
(p/merge! sys :schema-v2 {:message "Merge schema v2"})

;; Cleanup
(ice/destroy! sys)
```

**Architecture:**
- Metadata stored in Iceberg's JSON manifests (object storage)
- Branches tracked via Iceberg's branch management
- Manual merge: user resolves data/schema conflicts before merge
- Full integration with Spark, Flink, Trino, etc.

**Requirements:** Apache Spark with Iceberg runtime JAR

**Use cases:**
- Multi-tenant data lake branching
- Schema evolution with rollback
- Time-travel queries and compliance

### ZFS Adapter

Maps ZFS concepts to yggdrasil: datasets=branches, snapshots=commits, clones=branch creation. No merge support (ZFS lacks three-way merge).

```clojure
(require '[yggdrasil.adapters.zfs :as zfs])

;; Initialize workspace
(def sys (zfs/init! "rpool" "yggdrasil/my-project"))

;; Cleanup
(zfs/destroy! sys)
```

**Architecture:**
- Datasets mounted under `/tmp/yggdrasil/<prefix>/<branch>`
- Cross-branch ancestry via ZFS `origin` property tracking
- Per-branch locking for concurrent safety

#### ZFS Setup (Linux)

ZFS on Linux requires sudo for mount operations. Minimal sudoers configuration:

```
# /etc/sudoers.d/yggdrasil
username ALL=(ALL) NOPASSWD: /usr/sbin/zfs
username ALL=(ALL) NOPASSWD: /bin/chmod 777 /tmp/yggdrasil/*
```

Create the parent dataset with delegation:

```bash
sudo zfs create rpool/yggdrasil
sudo zfs allow -u $USER create,destroy,mount,snapshot,clone,promote,rollback rpool/yggdrasil
```

Override the test pool via environment variable:

```bash
ZFS_TEST_POOL=rpool/yggdrasil clj -M:test
```

**Dynamic vars:**
- `zfs/*use-sudo*` - prefix commands with sudo (default: `true`)
- `zfs/*mount-base*` - mount point root (default: `"/tmp/yggdrasil"`)

### Btrfs Adapter

True COW filesystem branching using Btrfs subvolumes. Lower memory overhead than ZFS (~512MB vs 8GB+), in-kernel (no DKMS module), writable snapshots natively. Supports merge via file-level copy.

```clojure
(require '[yggdrasil.adapters.btrfs :as btrfs])

;; Initialize workspace (path must be on a Btrfs filesystem)
(def sys (btrfs/init! "/path/on/btrfs" {:system-name "my-workspace"}))

;; Write/read files
(btrfs/write-file! sys "hello" "world")
(btrfs/read-file sys "hello")  ; => "world"
(p/commit! sys "First commit")

;; Cleanup
(btrfs/destroy! sys)
```

**Architecture:**
- Each branch is a writable Btrfs subvolume under `<base>/branches/<name>`
- Commits are read-only snapshots (`-r` flag) under `<base>/snapshots/<branch>/<uuid>`
- Commit metadata in `.yggdrasil/commits.edn` (EDN, since Btrfs has no user properties)
- Automatic snapshot pruning when count exceeds `*max-snapshots*` (default 50)
- Per-branch locking for concurrent safety

**Key constraints:**
- Keep active snapshot count under ~100 (Btrfs degrades above this)
- Never enable qgroups on the filesystem
- Best on a dedicated partition/loop device for agent workspaces

#### Btrfs Setup (Linux)

Btrfs subvolume operations work without sudo when the filesystem is mounted with `user_subvol_rm_allowed`. However, creating and mounting the test filesystem requires sudo:

```
# /etc/sudoers.d/yggdrasil-btrfs
username ALL=(ALL) NOPASSWD: /usr/sbin/mkfs.btrfs -f /tmp/btrfs-test.img
username ALL=(ALL) NOPASSWD: /usr/bin/mount
username ALL=(ALL) NOPASSWD: /usr/bin/umount /tmp/btrfs-test
username ALL=(ALL) NOPASSWD: /usr/bin/chmod 777 /tmp/btrfs-test
```

Create and mount the test filesystem:

```bash
truncate -s 256M /tmp/btrfs-test.img
sudo mkfs.btrfs -f /tmp/btrfs-test.img
mkdir -p /tmp/btrfs-test
sudo mount -o loop,user_subvol_rm_allowed /tmp/btrfs-test.img /tmp/btrfs-test
sudo chmod 777 /tmp/btrfs-test
```

Run tests (auto-detects btrfs mounts, or set path explicitly):

```bash
clj -M:test -n yggdrasil.adapters.btrfs-test
# Or explicitly:
BTRFS_TEST_PATH=/tmp/btrfs-test clj -M:test -n yggdrasil.adapters.btrfs-test
```

**Dynamic vars:**
- `btrfs/*use-sudo*` - prefix btrfs commands with sudo (default: `false`)
- `btrfs/*btrfs-binary*` - path to btrfs binary (default: `"btrfs"`)
- `btrfs/*max-snapshots*` - max snapshots per branch before pruning (default: `50`)

### OverlayFS + Bubblewrap Adapter

Lightweight COW filesystem branching with optional process isolation via bubblewrap. Each branch gets a separate "upper" directory overlaid on a shared read-only base.

```clojure
(require '[yggdrasil.adapters.overlayfs :as ofs])

;; Initialize workspace
(def sys (ofs/init! "/path/to/base" "/path/to/workspace"))

;; Write/read files (overlayfs semantics: upper overrides base)
(ofs/write-file! sys "entries/hello" "world")
(ofs/read-file sys "entries/hello")  ; => "world"
(p/commit! sys "First commit")

;; Execute commands in bubblewrap sandbox
(ofs/exec! sys ["ls" "/entries"])  ; network-isolated by default

;; Cleanup
(ofs/destroy! sys)
```

**Architecture:**
- Base directory is the shared read-only lower layer
- Each branch has `upper/` (writable), `work/` (overlayfs workdir), `snapshots/` (archived commits)
- `exec!` runs commands via bubblewrap with PID/network namespace isolation
- Sub-millisecond sandbox creation, zero memory overhead

### Podman Adapter

Container-based isolation using Podman (rootless, daemonless). Each branch is a running container with its own overlay filesystem layer.

```clojure
(require '[yggdrasil.adapters.podman :as pm])

;; Initialize (pulls base image, starts main container)
(def sys (pm/init! "/path/to/workspace" {:base-image "ubuntu:24.04"}))

;; Write/read files inside container
(pm/write-file! sys "entries/hello" "world")
(pm/read-file sys "entries/hello")  ; => "world"
(p/commit! sys "First commit")

;; Execute commands inside container
(pm/exec! sys "ls /entries")

;; Cleanup (stops containers, removes images)
(pm/destroy! sys)
```

**Architecture:**
- Each branch has a running container (named `ygg-<id>-<branch>`)
- Commits create new image layers via `podman commit`
- Branches fork from committed images
- Full process/network/filesystem isolation per branch
- Rootless operation (no daemon, no root required)

### Datahike Adapter

Wraps a Datahike connection, using its internal versioning system. Uses `d/listen` for native commit hooks (no polling needed).

```clojure
(require '[yggdrasil.adapters.datahike :as dh])

(def sys (dh/create conn {:system-name "my-db"}))
```

Requires Datahike on the classpath.

### LakeFS Adapter

Git-like versioning for object storage (S3, GCS, Azure). Each branch is a separate namespace in the object store with full merge support.

```clojure
(require '[yggdrasil.adapters.lakefs :as lfs])

;; Initialize new repository
(def sys (lfs/init! "my-repo" {:endpoint "http://localhost:8000"
                               :access-key "..."
                               :secret-key "..."
                               :storage-namespace "s3://bucket/prefix"}))

;; Write/read entries
(lfs/write-entry! sys "key" "value")
(lfs/read-entry sys "key")  ; => "value"
(p/commit! sys "First commit")

;; Cleanup
(lfs/destroy! sys)
```

**Requirements:** `lakectl` CLI and running LakeFS server.

**Dynamic vars:**
- `lakefs/*lakectl-binary*` - path to lakectl (default: `"lakectl"`)
- `lakefs/*lakectl-config*` - config file path (default: `nil`, uses `~/.lakectl.yaml`)

### Dolt Adapter

Git-like versioning for SQL databases. Each branch is a separate database state with full merge support via SQL semantics.

```clojure
(require '[yggdrasil.adapters.dolt :as dolt])

;; Initialize new repository (creates entries table)
(def sys (dolt/init! "/path/to/dolt-repo"))

;; Write/read entries via SQL
(dolt/write-entry! sys "key" "value")
(dolt/read-entry sys "key")  ; => "value"
(p/commit! sys "First commit")

;; Cleanup
(dolt/destroy! sys)
```

**Requirements:** `dolt` CLI installed.

**Dynamic vars:**
- `dolt/*dolt-binary*` - path to dolt (default: `"dolt"`)
- `dolt/*author*` - commit author (default: `"yggdrasil <yggdrasil@local>"`)

## Compliance Test Suite

The `yggdrasil.compliance` namespace provides protocol-agnostic tests for adapter implementations. Adapters provide a fixture map and the test suite validates correct behavior.

### Fixture Map

```clojure
{:create-system  (fn [] system)              ; create fresh system
 :mutate         (fn [sys] sys')             ; make observable change, return system
 :commit         (fn [sys msg] sys')         ; persist and return system
 :close!         (fn [sys] ...)              ; cleanup
 :write-entry    (fn [sys key value] sys')   ; write key-value, return system
 :read-entry     (fn [sys key] value-or-nil) ; read value
 :count-entries  (fn [sys] n)                ; count entries
 :delete-entry   (fn [sys key] sys')         ; delete key, return system (optional)
 :supports-concurrent? false}                ; optional, default true
```

### Running Tests

```clojure
(require '[yggdrasil.compliance :as compliance])

;; Run full suite
(compliance/run-compliance-tests fixture-map)

;; Or individual test groups
(compliance/test-system-identity fixture-map)
(compliance/test-snapshot-id-after-commit fixture-map)
(compliance/test-create-branch fixture-map)
(compliance/test-history fixture-map)
(compliance/test-merge fixture-map)
```

Tests automatically skip based on system capabilities (e.g., merge tests skip for ZFS).

## Types

### SnapshotRef

Universal reference to a point-in-time snapshot:

```clojure
(require '[yggdrasil.types :as t])

(t/->SnapshotRef "system-id" "snap-id" #{"parent"} (t/hlc-now) nil)
```

### HLC (Hybrid Logical Clock)

Each HLC has a `physical` (millis since epoch) and a `logical` counter. The physical component tracks `max(wall-clock, previous-physical)` — it never goes backward. When multiple events occur within the same millisecond, the logical counter increments to maintain strict ordering. This gives causal ordering without synchronized clocks across heterogeneous systems.

```clojure
(t/hlc-now)                        ; create from current time
(t/hlc-tick hlc)                   ; advance for local event
(t/hlc-receive local-hlc remote)   ; update on message receipt
(t/hlc-compare a b)                ; ordering comparison
(t/->HLC-ceil millis)              ; upper bound for wall-clock queries
```

### Capabilities

```clojure
(t/->Capabilities true true true false false true true true true)
;; snapshotable, branchable, graphable, mergeable, overlayable, watchable,
;; garbage-collectable, addressable, committable
```

## Composition

The `yggdrasil.compose` namespace provides multi-system coordination:

```clojure
(require '[yggdrasil.compose :as compose])

;; Create overlays on multiple systems
(def overlays (compose/prepare-all [sys-a sys-b] {:mode :gated}))

;; Commit all overlays in order
(compose/commit-seq! (vals overlays))

;; Or discard all
(compose/discard-all! (vals overlays))

;; Collect current snapshot refs
(compose/snapshot-refs [sys-a sys-b])
```

## Coordination Layer

The coordination layer provides cross-system snapshot tracking, temporal queries, and garbage collection. It sits above individual adapters and below the orchestrating runtime (e.g., Spindel).

### Workspace

The `yggdrasil.workspace` namespace is the primary coordination API. A workspace holds system refs, coordinates HLC timestamps, and manages the snapshot registry.

```clojure
(require '[yggdrasil.workspace :as ws])
(require '[yggdrasil.protocols :as p])

;; Create a workspace (in-memory registry)
(def w (ws/create-workspace))

;; Or with persistent registry (survives restarts)
(def w (ws/create-workspace {:store-path "/var/lib/yggdrasil/registry"}))

;; Add systems with auto-registration of commits
(def git-sys (git/create "/path/to/repo" {:system-name "my-repo"}))
(ws/manage! w git-sys)   ; installs commit hooks for auto-registration

;; Coordinated multi-system commit with shared HLC
(let [result (ws/coordinated-commit! w
               {"my-repo"  (fn [sys] (p/snapshot-id (p/commit! sys "sync point")))
                "my-db"    (fn [sys] (p/snapshot-id sys))})]
  ;; result: {:results {system-id -> RegistryEntry}
  ;;          :errors  {system-id -> Exception}
  ;;          :hlc     <pinned HLC>}
  (when (seq (:errors result))
    (println "Partial failure:" (keys (:errors result)))))

;; Temporal query: world state at time T (by HLC)
(let [world (ws/as-of-world w some-hlc)]
  ;; world: {["my-repo" "main"] -> RegistryEntry
  ;;         ["my-db" "main"]   -> RegistryEntry}
  (doseq [[[sys-id branch] entry] world]
    (println sys-id branch "was at" (:snapshot-id entry))))

;; Temporal query: world state at a wall-clock time
(let [world (ws/as-of-time w (.getTime some-date))]
  ;; Joint query: get Datahike db at the same point as a Git commit
  (let [db-entry (get world ["my-db" "main"])
        db       (p/as-of (ws/get-system w "my-db")
                           (:snapshot-id db-entry))]
    (d/q '[:find ?e ?v :where [?e :attr ?v]] db)))

;; Hold refs to multiple branches (prevents GC)
(ws/hold-ref! w "my-repo/:feature" feature-sys)
(ws/release-ref! w "my-repo/:feature")  ; allows GC

;; Run garbage collection
(ws/gc! w {:grace-period-ms (* 7 24 60 60 1000)  ; 7 days
           :dry-run? true})                        ; preview first

;; Clean up
(ws/unmanage! w "my-repo")
(ws/close! w)
```

### Snapshot Registry

The `yggdrasil.registry` namespace maintains a persistent sorted-set (PSS) index over `RegistryEntry` records, sorted by `[hlc system-id branch-name snapshot-id]`. Backed by konserve for durable, lazy-loading from disk.

```clojure
(require '[yggdrasil.registry :as reg])

;; Query: all entries for a system/branch
(reg/system-history registry "my-repo" "main" {:limit 10})

;; Query: who references a snapshot?
(reg/snapshot-refs registry "abc123")

;; Query: temporal range
(reg/entries-in-range registry from-hlc to-hlc)
```

### Garbage Collection

The `yggdrasil.gc` namespace implements mark-and-sweep with a grace period:

1. **Mark**: Walk all systems' branch heads to compute the reachable set
2. **Retain**: Keep snapshots within the retention window (default 7 days)
3. **Sweep**: Delete unreachable, expired snapshots via each adapter's native GC

Adapters implement the `GarbageCollectable` protocol:

```clojure
(p/gc-roots sys)               ; => #{"snap-3" "snap-5"} branch heads
(p/gc-sweep! sys #{"snap-1"})  ; delete unreachable snapshots
```

Cross-system safety: a snapshot referenced by *any* system is never collected, even if unreachable in another system.

### Hooks

The `yggdrasil.hooks` namespace provides an extension point for adapter-specific commit notification. Adapters extend the `install-commit-hook!` multimethod dispatched on `system-type`:

- **Datahike**: Uses `d/listen` for immediate notification
- **Default (Watchable)**: Falls back to `p/watch!` polling

### Multi-System Lifecycle

Runtimes like Spindel use these protocols for isolated agent execution:

1. **Fork** — `p/branch!` + `p/checkout` on each managed system
2. **Work** — read/write via `p/working-path` (filesystem adapters) or adapter API
3. **Commit** — `p/commit!` on all `Committable` systems
4. **Merge** — `p/merge!` back to parent branch
5. **GC** — `ws/gc!` reclaims old snapshots across all systems

```clojure
;; Fork
(def child-sys (-> sys (p/branch! :agent-123) (p/checkout :agent-123)))

;; Work (filesystem adapters)
(spit (str (p/working-path child-sys) "/result.edn") data)

;; Commit
(def child-sys (p/commit! child-sys "Agent work complete"))

;; Merge back
(def sys (-> sys (p/checkout :main) (p/merge! :agent-123)))

;; GC old branches
(p/delete-branch! sys :agent-123)
(ws/gc! workspace)
```

## Consistency Model

Yggdrasil provides **Parallel Snapshot Isolation (PSI)** semantics across all adapters:

- **Snapshot reads**: All reads within a transaction see a consistent snapshot
- **Causal ordering**: Commits form a DAG with HLC timestamps for cross-system ordering
- **Write-conflict detection**: `Mergeable.conflicts` detects concurrent modifications

See [CONSISTENCY.md](docs/CONSISTENCY.md) for detailed semantics, adapter-specific guarantees, and academic references.

## Categorical Semantics

For users interested in the formal mathematical foundations, Yggdrasil's protocols can be understood through category theory as a **snapshot-first model** where version control operations correspond to categorical constructions (pushouts, coinitial morphisms, etc.).

See [CATEGORICAL_SEMANTICS.md](docs/CATEGORICAL_SEMANTICS.md) for the formal treatment, comparison with patch-based systems (Darcs, Pijul), and theoretical justification of design decisions. This is optional reading and not required to use the library.

## Development

```bash
# Run all tests
clj -M:test

# Run only git adapter tests
clj -M:test -n yggdrasil.adapters.git-test

# Run IPFS tests (requires IPFS daemon running)
clj -M:test -n yggdrasil.adapters.ipfs-test

# Run Iceberg tests (requires Spark with Iceberg)
clj -M:test -n yggdrasil.adapters.iceberg-test

# Run ZFS tests (requires ZFS + sudo setup)
ZFS_TEST_POOL=rpool/yggdrasil clj -M:test -n yggdrasil.adapters.zfs-test

# Run Btrfs tests (requires Btrfs mount, see Btrfs Setup)
BTRFS_TEST_PATH=/tmp/btrfs-test clj -M:test -n yggdrasil.adapters.btrfs-test

# Start nREPL
clj -M:dev
```

## Project Structure

```
src/yggdrasil/
  protocols.cljc       # Protocol definitions (layers 0-7 + Addressable, Committable)
  types.cljc           # Core data types (SnapshotRef, HLC, Capabilities, RegistryEntry)
  watcher.clj          # Polling watcher infrastructure
  compose.cljc         # Multi-system overlay lifecycle
  compliance.clj       # Protocol compliance test suite
  workspace.clj        # Multi-system workspace with HLC coordination
  registry.clj         # Snapshot registry (PSS index in konserve)
  storage.clj          # IStorage for PSS B-tree nodes in konserve
  gc.clj               # Coordinated cross-system garbage collection
  hooks.clj            # Extension point for adapter-specific commit hooks
  adapters/
    git.clj            # Git adapter (worktree-based)
    ipfs.clj           # IPFS adapter (P2P content-addressed)
    iceberg.clj        # Iceberg adapter (data lake versioning)
    zfs.clj            # ZFS adapter
    btrfs.clj          # Btrfs adapter
    overlayfs.clj      # OverlayFS + Bubblewrap adapter
    podman.clj         # Podman container adapter
    datahike.clj       # Datahike adapter (with native d/listen hooks)
    lakefs.clj         # LakeFS adapter (object storage)
    dolt.clj           # Dolt adapter (SQL versioning)
test/yggdrasil/
  workspace_test.clj   # Workspace coordination tests
  registry_test.clj    # Registry index tests
  gc_test.clj          # GC integration tests
  adapters/
    git_test.clj       # Git compliance tests
    ipfs_test.clj      # IPFS compliance tests
    iceberg_test.clj   # Iceberg compliance tests
    zfs_test.clj       # ZFS compliance tests
    btrfs_test.clj     # Btrfs compliance tests
    overlayfs_test.clj # OverlayFS compliance tests
    podman_test.clj    # Podman compliance tests
    lakefs_test.clj    # LakeFS compliance tests
    dolt_test.clj      # Dolt compliance tests
```

## License

Copyright © 2026 Christian Weilbach

Licensed under the Eclipse Public License 2.0, see [LICENSE](LICENSE).
