"""Core data types for the Yggdrasil memory model.

Mirrors the Clojure types in src/yggdrasil/types.cljc.
All types are immutable dataclasses for safety and hashability.
"""

from dataclasses import dataclass, field
from typing import Set, Optional, Dict, Any
import time


# ============================================================
# HLC - Hybrid Logical Clock
# ============================================================

@dataclass(frozen=True, order=True)
class HLC:
    """Hybrid Logical Clock for causal ordering without synchronized clocks.

    Comparison is lexicographic: physical first, then logical.
    """
    physical: int  # milliseconds since epoch
    logical: int = 0  # counter for same-ms events

    @classmethod
    def now(cls) -> "HLC":
        """Create HLC from current time."""
        return cls(physical=int(time.time() * 1000), logical=0)

    def tick(self) -> "HLC":
        """Advance HLC for local event."""
        now_ms = int(time.time() * 1000)
        if now_ms > self.physical:
            return HLC(physical=now_ms, logical=0)
        return HLC(physical=self.physical, logical=self.logical + 1)

    def receive(self, remote: "HLC") -> "HLC":
        """Update HLC on receiving message with remote HLC."""
        now_ms = int(time.time() * 1000)
        max_physical = max(now_ms, self.physical, remote.physical)

        if max_physical == self.physical == remote.physical:
            return HLC(physical=max_physical,
                       logical=max(self.logical, remote.logical) + 1)
        elif max_physical == self.physical:
            return HLC(physical=max_physical, logical=self.logical + 1)
        elif max_physical == remote.physical:
            return HLC(physical=max_physical, logical=remote.logical + 1)
        else:
            return HLC(physical=max_physical, logical=0)

    def to_json(self) -> Dict[str, int]:
        return {"physical": self.physical, "logical": self.logical}

    @classmethod
    def from_json(cls, data: Dict[str, int]) -> "HLC":
        return cls(physical=data["physical"], logical=data["logical"])


# ============================================================
# SnapshotRef - universal reference to a point-in-time
# ============================================================

@dataclass(frozen=True)
class SnapshotRef:
    """Universal reference to a point-in-time snapshot.

    This is the core data structure exchanged between systems.
    """
    system_id: str  # which system instance
    snapshot_id: str  # UUID or content-hash (native format)
    parent_ids: frozenset = field(default_factory=frozenset)  # ancestry
    hlc: Optional[HLC] = None  # causal timestamp
    content_hash: Optional[str] = None  # for verification/dedup

    def to_json(self) -> Dict[str, Any]:
        return {
            "system-id": self.system_id,
            "snapshot-id": self.snapshot_id,
            "parent-ids": list(self.parent_ids),
            "hlc": self.hlc.to_json() if self.hlc else None,
            "content-hash": self.content_hash,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "SnapshotRef":
        return cls(
            system_id=data["system-id"],
            snapshot_id=data["snapshot-id"],
            parent_ids=frozenset(data.get("parent-ids", [])),
            hlc=HLC.from_json(data["hlc"]) if data.get("hlc") else None,
            content_hash=data.get("content-hash"),
        )


# ============================================================
# Capabilities
# ============================================================

@dataclass(frozen=True)
class Capabilities:
    """Advertises which protocol layers a system supports."""
    snapshotable: bool = False
    branchable: bool = False
    graphable: bool = False
    mergeable: bool = False
    overlayable: bool = False
    watchable: bool = False

    def to_json(self) -> Dict[str, bool]:
        return {
            "snapshotable": self.snapshotable,
            "branchable": self.branchable,
            "graphable": self.graphable,
            "mergeable": self.mergeable,
            "overlayable": self.overlayable,
            "watchable": self.watchable,
        }

    @classmethod
    def from_json(cls, data: Dict[str, bool]) -> "Capabilities":
        return cls(**data)


# ============================================================
# Overlay - live fork state
# ============================================================

@dataclass
class Overlay:
    """Live fork state for Overlayable systems.

    Three modes:
      - frozen: snapshot at creation time, never updates
      - following: always sees parent's latest state
      - gated: explicit advance() to sync with parent
    """
    overlay_id: str  # unique overlay identifier
    parent: Any  # the parent system (Overlayable instance)
    mode: str  # "frozen" | "following" | "gated"
    base_snapshot: Optional[SnapshotRef] = None  # current observation point
    local_writes: Optional[Any] = None  # isolated writes (system-specific)
    created_at: Optional[HLC] = None  # when overlay was created

    def to_json(self) -> Dict[str, Any]:
        return {
            "overlay-id": self.overlay_id,
            "mode": self.mode,
            "base-snapshot": self.base_snapshot.to_json() if self.base_snapshot else None,
            "created-at": self.created_at.to_json() if self.created_at else None,
        }


# ============================================================
# Conflict descriptor
# ============================================================

@dataclass(frozen=True)
class Conflict:
    """Describes a conflict between two snapshots during merge.

    path is system-specific (e.g., entity-attribute for Datahike,
    file path for Git, dataset path for ZFS).
    """
    path: tuple  # location of conflict (system-specific)
    base: Any = None  # value in common ancestor
    ours: Any = None  # value in local branch
    theirs: Any = None  # value in remote branch

    def to_json(self) -> Dict[str, Any]:
        return {
            "path": list(self.path),
            "base": self.base,
            "ours": self.ours,
            "theirs": self.theirs,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "Conflict":
        return cls(
            path=tuple(data["path"]),
            base=data.get("base"),
            ours=data.get("ours"),
            theirs=data.get("theirs"),
        )
