"""Yggdrasil - Unified Copy-on-Write Memory Model.

Cross-language protocol for snapshot isolation, branching, and
overlay-based development across heterogeneous systems.
"""

__version__ = "0.1.0"

from yggdrasil.protocols import (
    Snapshotable,
    Branchable,
    Graphable,
    Mergeable,
    Overlayable,
    Watchable,
    SystemIdentity,
)
from yggdrasil.types import SnapshotRef, HLC, Capabilities, Overlay, Conflict
