# Yggdrasil

[![CircleCI](https://circleci.com/gh/replikativ/yggdrasil.svg?style=shield)](https://circleci.com/gh/replikativ/yggdrasil)
[![Clojars](https://img.shields.io/clojars/v/org.replikativ/yggdrasil.svg)](https://clojars.org/org.replikativ/yggdrasil)
[![Slack](https://img.shields.io/badge/slack-join_chat-brightgreen.svg)](https://clojurians.slack.com/archives/CB7GJAN0L)

> *In Norse mythology, [Yggdrasil](https://en.wikipedia.org/wiki/Yggdrasil) is the immense World Tree whose branches extend into the heavens and whose roots reach into disparate realms, connecting the nine worlds into a unified cosmos.*

Unified copy-on-write memory model protocols for heterogeneous storage systems.

Yggdrasil defines a layered protocol stack that abstracts over Git, ZFS, Btrfs, OverlayFS, Podman, Datahike, [Proximum](https://github.com/replikativ/proximum), and [Scriptum](https://github.com/replikativ/scriptum), enabling cross-system composition with causal consistency guarantees.

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

## Adapters

### Capabilities Matrix

| Adapter | Snapshot | Branch | Graph | Merge | Overlay | Watch |
|---------|----------|--------|-------|-------|---------|-------|
| Git | commits | branches (worktrees) | full DAG | 3-way | - | poll |
| ZFS | snapshots | clones | linear | - | - | poll |
| Btrfs | ro snapshots | subvolumes | full DAG | file-level | - | poll |
| Datahike | commit-id | branch! | full DAG | merge! | - | - |
| Scriptum | generations | COW dirs | full DAG | add-only | - | - |
| OverlayFS | upper dir archives | overlay dirs | full DAG | file-level | - | poll |
| Podman | image layers | containers | full DAG | diff+apply | - | poll |
| LakeFS | commits | branches | full DAG | 3-way | - | poll |
| Dolt | commits | branches | full DAG | 3-way | - | poll |

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
(btrfs/commit! sys "First commit")

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
(ofs/commit! sys "First commit")

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
(pm/commit! sys "First commit")

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

Wraps a Datahike connection, using its internal versioning system.

```clojure
(require '[yggdrasil.adapters.datahike :as dh])

(def sys (dh/create conn {:system-name "my-db"}))
```

Requires Datahike on the classpath (resolved dynamically via `requiring-resolve`).

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
(lfs/commit! sys "First commit")

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
(dolt/commit! sys "First commit")

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

Causal ordering without synchronized clocks:

```clojure
(t/hlc-now)                        ; create from current time
(t/hlc-tick hlc)                   ; advance for local event
(t/hlc-receive local-hlc remote)   ; update on message receipt
(t/hlc-compare a b)                ; ordering comparison
```

### Capabilities

```clojure
(t/->Capabilities true true true false false true)
;; snapshotable, branchable, graphable, mergeable, overlayable, watchable
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

## Consistency Model

Yggdrasil provides **Parallel Snapshot Isolation (PSI)** semantics across all adapters:

- **Snapshot reads**: All reads within a transaction see a consistent snapshot
- **Causal ordering**: Commits form a DAG with HLC timestamps for cross-system ordering
- **Write-conflict detection**: `Mergeable.conflicts` detects concurrent modifications

See [CONSISTENCY.md](CONSISTENCY.md) for detailed semantics, adapter-specific guarantees, and academic references.

## Development

```bash
# Run all tests
clj -M:test

# Run only git adapter tests
clj -M:test -n yggdrasil.adapters.git-test

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
  protocols.cljc       # Protocol definitions
  types.cljc           # Core data types (SnapshotRef, HLC, etc.)
  watcher.clj          # Polling watcher infrastructure
  compose.cljc         # Multi-system composition
  compliance.clj       # Protocol compliance test suite
  adapters/
    git.clj            # Git adapter (worktree-based)
    zfs.clj            # ZFS adapter
    btrfs.clj          # Btrfs adapter
    overlayfs.clj      # OverlayFS + Bubblewrap adapter
    podman.clj         # Podman container adapter
    datahike.clj       # Datahike adapter
    lakefs.clj         # LakeFS adapter (object storage)
    dolt.clj           # Dolt adapter (SQL versioning)
test/yggdrasil/adapters/
  git_test.clj         # Git compliance tests
  zfs_test.clj         # ZFS compliance tests
  btrfs_test.clj       # Btrfs compliance tests
  overlayfs_test.clj   # OverlayFS compliance tests
  podman_test.clj      # Podman compliance tests
  lakefs_test.clj      # LakeFS compliance tests
  dolt_test.clj        # Dolt compliance tests
```

## License

Copyright © 2026 Christian Weilbach

Licensed under the Eclipse Public License 2.0, see [LICENSE](LICENSE).
