"""Yggdrasil protocol definitions as Python Abstract Base Classes.

Six layers, each optional:
    1. Snapshotable - point-in-time immutable snapshots
    2. Branchable   - named mutable references
    3. Graphable    - history/DAG traversal
    4. Mergeable    - combine lineages
    5. Overlayable  - live fork with observation modes
    6. Watchable    - state change observation

Execution control:
    Methods that may perform IO accept an optional `opts` dict.
    Recognized key: "sync" (bool, default True).
    When sync=True (default), methods block and return direct values.
    When sync=False, methods return awaitables/futures.
"""

from abc import ABC, abstractmethod
from typing import Set, Optional, List, Dict, Any, Callable
from yggdrasil.types import SnapshotRef, Capabilities, Conflict, Overlay


# ============================================================
# System Identity
# ============================================================

class SystemIdentity(ABC):
    """System identification and capability advertisement."""

    @abstractmethod
    def system_id(self) -> str:
        """Unique identifier for this system instance."""
        ...

    @abstractmethod
    def system_type(self) -> str:
        """Type string: 'datahike', 'proximum', 'git', 'zfs', 'docker', 'repl'."""
        ...

    @abstractmethod
    def capabilities(self) -> Capabilities:
        """Map of supported protocols."""
        ...


# ============================================================
# Layer 1: Snapshotable (fundamental)
# ============================================================

class Snapshotable(ABC):
    """Point-in-time immutable snapshots. Every CoW system implements this."""

    @abstractmethod
    def snapshot_id(self) -> str:
        """Current snapshot ID. Returns UUID or content-hash string."""
        ...

    @abstractmethod
    def parent_ids(self, snap_id: Optional[str] = None) -> Set[str]:
        """Parent snapshot IDs. If snap_id is None, uses current state."""
        ...

    @abstractmethod
    def as_of(self, snap_id: str, opts: Optional[Dict[str, Any]] = None) -> Any:
        """Read-only view at given snapshot.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def snapshot_meta(self, snap_id: str, opts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Metadata for snapshot. Returns dict with timestamp, author, message, etc.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...


# ============================================================
# Layer 2: Branchable (named references)
# ============================================================

class Branchable(ABC):
    """Named references to snapshots.
    Mutating operations return self (mutated) for method chaining."""

    @abstractmethod
    def branches(self, opts: Optional[Dict[str, Any]] = None) -> Set[str]:
        """List all branch names.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def current_branch(self) -> str:
        """Current branch name."""
        ...

    @abstractmethod
    def branch(self, name: str, from_ref: Optional[str] = None,
               opts: Optional[Dict[str, Any]] = None) -> "Branchable":
        """Create branch from current state (or from_ref snapshot/branch).
        Returns self with branch created. Current branch unchanged.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def delete_branch(self, name: str, opts: Optional[Dict[str, Any]] = None) -> "Branchable":
        """Remove branch. Returns self without the branch.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def checkout(self, name: str, opts: Optional[Dict[str, Any]] = None) -> "Branchable":
        """Switch to branch. Returns self at branch head.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...


# ============================================================
# Layer 3: Graphable (history/DAG traversal)
# ============================================================

class Graphable(ABC):
    """DAG traversal and history."""

    @abstractmethod
    def history(self, limit: Optional[int] = None, since: Optional[str] = None,
                opts: Optional[Dict[str, Any]] = None) -> List[str]:
        """Commit history as list of snapshot-ids, newest first.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def ancestors(self, snap_id: str, opts: Optional[Dict[str, Any]] = None) -> List[str]:
        """All ancestor snapshot-ids of given snapshot.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def is_ancestor(self, a: str, b: str, opts: Optional[Dict[str, Any]] = None) -> bool:
        """True if snapshot a is an ancestor of snapshot b.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def common_ancestor(self, a: str, b: str,
                        opts: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Most recent common ancestor. Returns snapshot-id or None.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    def commit_graph(self, opts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Full DAG structure. Optional -- default raises NotImplementedError.
        opts: {"sync": True} -- when False, returns awaitable."""
        raise NotImplementedError("commit_graph not supported by this system")

    def commit_info(self, snap_id: str, opts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Metadata for specific commit. Falls back to snapshot_meta.
        opts: {"sync": True} -- when False, returns awaitable."""
        if isinstance(self, Snapshotable):
            return self.snapshot_meta(snap_id, opts)
        raise NotImplementedError("commit_info not supported by this system")


# ============================================================
# Layer 4: Mergeable (combine lineages)
# ============================================================

class Mergeable(ABC):
    """Merge support. Mutating operations return self."""

    @abstractmethod
    def merge(self, source: str, strategy: Optional[str] = None,
              message: Optional[str] = None,
              opts: Optional[Dict[str, Any]] = None) -> "Mergeable":
        """Merge source branch/snapshot into current. Returns self with merge applied.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def conflicts(self, a: str, b: str,
                  opts: Optional[Dict[str, Any]] = None) -> List[Conflict]:
        """Detect conflicts between two snapshots without merging.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def diff(self, a: str, b: str, opts: Optional[Dict[str, Any]] = None) -> Any:
        """Compute delta between two snapshots.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...


# ============================================================
# Layer 5: Overlayable (live fork - Spindel integration)
# ============================================================

class Overlayable(ABC):
    """Live fork that can observe parent's evolution.
    Three modes: frozen, following, gated."""

    @abstractmethod
    def overlay(self, mode: str = "gated",
                opts: Optional[Dict[str, Any]] = None) -> Overlay:
        """Create overlay. mode: 'frozen', 'following', 'gated'.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def advance(self, overlay: Overlay,
                opts: Optional[Dict[str, Any]] = None) -> None:
        """Sync overlay to parent's current state (gated mode).
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def peek_parent(self, overlay: Overlay,
                    opts: Optional[Dict[str, Any]] = None) -> Any:
        """Read parent's current state without advancing.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def base_ref(self, overlay: Overlay) -> SnapshotRef:
        """SnapshotRef of overlay's current observation point."""
        ...

    @abstractmethod
    def overlay_writes(self, overlay: Overlay) -> Any:
        """Delta of overlay's isolated writes."""
        ...

    @abstractmethod
    def merge_down(self, overlay: Overlay,
                   opts: Optional[Dict[str, Any]] = None) -> None:
        """Push overlay writes to parent.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...

    @abstractmethod
    def discard(self, overlay: Overlay,
                opts: Optional[Dict[str, Any]] = None) -> None:
        """Abandon overlay and all its isolated writes.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...


# ============================================================
# Layer 6: Watchable (state change observation)
# ============================================================

class Watchable(ABC):
    """Observe state changes via polling or event notification."""

    @abstractmethod
    def watch(self, callback: Callable[[Dict[str, Any]], None],
              opts: Optional[Dict[str, Any]] = None) -> str:
        """Register callback for state change events. Returns watch-id string.
        callback receives dicts with keys: type, snapshot_id, branch, timestamp.
        type is one of: 'commit', 'branch_created', 'branch_deleted', 'checkout'.
        opts: {"poll_interval_ms": 1000, "sync": True}"""
        ...

    @abstractmethod
    def unwatch(self, watch_id: str, opts: Optional[Dict[str, Any]] = None) -> None:
        """Stop watching. Removes callback and cleans up if last watcher.
        opts: {"sync": True} -- when False, returns awaitable."""
        ...
