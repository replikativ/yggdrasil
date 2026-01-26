# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Yggdrasil is a unified copy-on-write memory model protocol framework for heterogeneous storage systems. It abstracts over Git, ZFS, Btrfs, OverlayFS, Podman, Datahike, LakeFS, and Dolt with causal consistency guarantees (Parallel Snapshot Isolation semantics).

## Build & Development Commands

```bash
# Run all tests
clj -M:test

# Run specific adapter tests
clj -M:test -n yggdrasil.adapters.git-test
clj -M:test -n yggdrasil.adapters.zfs-test      # requires ZFS_TEST_POOL env var
clj -M:test -n yggdrasil.adapters.btrfs-test    # requires BTRFS_TEST_PATH env var

# Start nREPL for development
clj -M:dev

# Check code formatting
clj -M:format

# Auto-fix formatting
clj -M:ffix
```

## Architecture

### Protocol Layers (src/yggdrasil/protocols.cljc)

The protocol stack builds from basic to advanced capabilities:

| Layer | Protocol | Purpose |
|-------|----------|---------|
| 0 | `SystemIdentity` | System identification and capability advertisement |
| 1 | `Snapshotable` | Point-in-time immutable snapshots |
| 2 | `Branchable` | Named mutable references (branches) |
| 3 | `Graphable` | DAG traversal, history, ancestry |
| 4 | `Mergeable` | Three-way merge, conflict detection |
| 5 | `Overlayable` | Live forks with observation modes |
| 6 | `Watchable` | State change observation (polling) |

### Core Types (src/yggdrasil/types.cljc)

- `SnapshotRef` - Universal reference (system-id, snapshot-id, parent-ids, hlc, content-hash)
- `HLC` - Hybrid Logical Clock for causal ordering across systems
- `Capabilities` - Feature support indicators per adapter

### Key Design Principles

- **Value semantics**: All mutations return new system values (Clojure-style immutability)
- **Protocol composition**: Adapters implement only the protocols they support
- **Cross-platform**: Core files use `.cljc` for ClojureScript compatibility where possible

### Adapter Implementations (src/yggdrasil/adapters/)

Each adapter maps the underlying system's concepts to the protocol stack:

- **git.clj**: Uses worktrees for concurrent branch access, per-branch locking
- **zfs.clj**: Datasets=branches, snapshots=commits, clones=branch creation
- **btrfs.clj**: Subvolumes as branches, read-only snapshots for commits
- **overlayfs.clj**: Upper directories overlaid on shared base, bubblewrap sandboxing
- **podman.clj**: Containers as branches, image layers as commits
- **datahike.clj**: Wraps Datahike's internal versioning (requires dynamic resolution)
- **lakefs.clj**: Object storage versioning via lakectl CLI
- **dolt.clj**: SQL database versioning via dolt CLI

### Compliance Testing (src/yggdrasil/compliance.clj)

Protocol-agnostic test suite that validates adapter implementations. Adapters provide a fixture map with `create-system`, `mutate`, `commit`, `close!`, and optional data operations. Tests auto-skip based on system capabilities.

## Contributing Requirements

- All commits must include `Signed-off-by` line: `git commit -s -m "message"`
- First-time contributors must sign the CLA
- Use `clj-kondo` for linting, `cljfmt` for formatting
- Present tense commit messages, <72 chars first line
