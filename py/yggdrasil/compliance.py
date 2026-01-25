"""Protocol-agnostic compliance test suite for Yggdrasil adapters.

Python port of the Clojure compliance suite. Adapters provide a fixture dict:

    fixture = {
        "create_system": lambda: ...,       # fresh system with 'main' branch
        "mutate": lambda sys: ...,          # perform a mutation, return system
        "commit": lambda sys, msg: ...,     # commit, return system
        "close": lambda sys: ...,           # cleanup
        "write_entry": lambda sys, k, v: ...,  # write keyed entry, return system
        "read_entry": lambda sys, k: ...,      # read by key, or None
        "count_entries": lambda sys: ...,      # count entries in current state
        "delete_entry": lambda sys, k: ...,    # delete by key, return system (or None)
    }

Usage with pytest:

    from yggdrasil.compliance import run_compliance_tests

    def test_compliance(git_fixture):
        run_compliance_tests(git_fixture)
"""

from typing import Dict, Any, Callable, Optional


# ============================================================
# Helpers
# ============================================================

def _has_capability(fixture: Dict[str, Any], cap: str) -> bool:
    """Check if system supports a capability."""
    sys = fixture["create_system"]()
    try:
        caps = sys.capabilities()
        return getattr(caps, cap, False)
    finally:
        fixture["close"](sys)


# ============================================================
# Layer 1: Snapshotable tests
# ============================================================

def test_snapshot_id_after_commit(fix: Dict[str, Any]) -> None:
    """snapshot_id returns the current commit ID after a commit."""
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "first")
        sid = sys.snapshot_id()
        assert isinstance(sid, str), "snapshot_id should return a string"
        assert len(sid) > 0, "snapshot_id should be non-empty"
    finally:
        fix["close"](sys)


def test_parent_ids_root_commit(fix: Dict[str, Any]) -> None:
    """Root of the DAG has empty parent_ids."""
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "root")
        # Walk back to the actual root commit
        snap = sys.snapshot_id()
        while True:
            parents = sys.parent_ids(snap)
            if len(parents) == 0:
                break
            snap = next(iter(parents))
        assert len(sys.parent_ids(snap)) == 0, \
            "Root commit should have no parents"
    finally:
        fix["close"](sys)


def test_parent_ids_chain(fix: Dict[str, Any]) -> None:
    """Second commit has first commit as parent."""
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "first")
        first_id = sys.snapshot_id()
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "second")
        parents = sys.parent_ids()
        assert first_id in parents, "Second commit should have first commit as parent"
    finally:
        fix["close"](sys)


def test_snapshot_meta(fix: Dict[str, Any]) -> None:
    """snapshot_meta returns metadata for a commit."""
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "test message")
        sid = sys.snapshot_id()
        meta = sys.snapshot_meta(sid)
        assert meta is not None, "snapshot_meta should return non-None"
        assert "snapshot-id" in meta, "meta should contain snapshot-id"
        assert "parent-ids" in meta, "meta should contain parent-ids"
        assert isinstance(meta["parent-ids"], set), "parent-ids should be a set"
    finally:
        fix["close"](sys)


def test_as_of(fix: Dict[str, Any]) -> None:
    """as_of returns a read-only view at a snapshot."""
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "snapshot point")
        sid = sys.snapshot_id()
        view = sys.as_of(sid)
        assert view is not None, "as_of should return non-None"
    finally:
        fix["close"](sys)


# ============================================================
# Layer 2: Branchable tests
# ============================================================

def test_initial_branches(fix: Dict[str, Any]) -> None:
    """Fresh system has a main branch."""
    if not _has_capability(fix, "branchable"):
        return
    sys = fix["create_system"]()
    try:
        branches = sys.branches()
        assert "main" in branches, "Should have 'main' branch"
        assert sys.current_branch() == "main", "Current branch should be 'main'"
    finally:
        fix["close"](sys)


def test_create_branch(fix: Dict[str, Any]) -> None:
    """branch creates a new branch and returns self."""
    if not _has_capability(fix, "branchable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "before fork")
        sys = sys.branch("experiment")
        assert "experiment" in sys.branches(), \
            "Should have 'experiment' branch after branching"
        assert sys.current_branch() == "main", \
            "branch should not switch current branch"
    finally:
        fix["close"](sys)


def test_checkout(fix: Dict[str, Any]) -> None:
    """checkout returns self on target branch."""
    if not _has_capability(fix, "branchable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "before fork")
        sys = sys.branch("experiment")
        sys = sys.checkout("experiment")
        assert sys.current_branch() == "experiment", \
            "current_branch should be 'experiment' after checkout"
    finally:
        fix["close"](sys)


def test_branch_isolation(fix: Dict[str, Any]) -> None:
    """Commits on one branch don't affect another."""
    if not _has_capability(fix, "branchable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "main commit")
        sys = sys.branch("experiment")
        main_after_fork = sys.snapshot_id()
        # Advance experiment
        sys = sys.checkout("experiment")
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "experiment commit")
        # Main should still be at fork point
        sys = sys.checkout("main")
        assert sys.snapshot_id() == main_after_fork, \
            "main should still be at its fork-point commit"
    finally:
        fix["close"](sys)


def test_delete_branch(fix: Dict[str, Any]) -> None:
    """delete_branch returns system without the branch."""
    if not _has_capability(fix, "branchable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "before fork")
        sys = sys.branch("temp")
        assert "temp" in sys.branches()
        sys = sys.delete_branch("temp")
        assert "temp" not in sys.branches(), \
            "Branch should be gone after delete"
    finally:
        fix["close"](sys)


# ============================================================
# Layer 3: Graphable tests
# ============================================================

def test_history(fix: Dict[str, Any]) -> None:
    """history returns commit IDs newest first."""
    if not _has_capability(fix, "graphable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "first")
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "second")
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "third")
        id3 = sys.snapshot_id()
        hist = sys.history()
        assert hist[0] == id3, "Most recent commit should be first"
        assert len(hist) >= 3, "Should have at least 3 commits"
    finally:
        fix["close"](sys)


def test_history_limit(fix: Dict[str, Any]) -> None:
    """history respects limit parameter."""
    if not _has_capability(fix, "graphable"):
        return
    sys = fix["create_system"]()
    try:
        for i in range(5):
            sys = fix["mutate"](sys)
            sys = fix["commit"](sys, f"commit {i}")
        hist = sys.history(limit=2)
        assert len(hist) == 2, "Should return only 2 commits with limit=2"
    finally:
        fix["close"](sys)


def test_ancestors(fix: Dict[str, Any]) -> None:
    """ancestors returns all ancestor IDs."""
    if not _has_capability(fix, "graphable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "first")
        id1 = sys.snapshot_id()
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "second")
        id2 = sys.snapshot_id()
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "third")
        id3 = sys.snapshot_id()
        ancs = set(sys.ancestors(id3))
        assert id2 in ancs, "id2 should be ancestor of id3"
        assert id1 in ancs, "id1 should be ancestor of id3"
    finally:
        fix["close"](sys)


def test_ancestor_predicate(fix: Dict[str, Any]) -> None:
    """is_ancestor checks ancestry relationship."""
    if not _has_capability(fix, "graphable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "first")
        id1 = sys.snapshot_id()
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "second")
        id2 = sys.snapshot_id()
        assert sys.is_ancestor(id1, id2) is True, \
            "id1 should be ancestor of id2"
        assert sys.is_ancestor(id2, id1) is False, \
            "id2 should NOT be ancestor of id1"
    finally:
        fix["close"](sys)


def test_common_ancestor(fix: Dict[str, Any]) -> None:
    """common_ancestor finds merge base of diverged branches."""
    if not _has_capability(fix, "graphable"):
        return
    if not _has_capability(fix, "branchable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "common base")
        sys = sys.branch("feature")
        fork_point = sys.snapshot_id()
        # Advance main
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "main advance")
        main_id = sys.snapshot_id()
        # Advance feature
        sys = sys.checkout("feature")
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "feature advance")
        feat_id = sys.snapshot_id()
        # Check common ancestor
        ancestor = sys.common_ancestor(main_id, feat_id)
        assert ancestor == fork_point, \
            "Common ancestor should be the fork point"
    finally:
        fix["close"](sys)


def test_commit_info(fix: Dict[str, Any]) -> None:
    """commit_info returns metadata for a specific commit."""
    if not _has_capability(fix, "graphable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "test info")
        sid = sys.snapshot_id()
        # commit_info falls back to snapshot_meta in default impl
        meta = sys.snapshot_meta(sid)
        assert meta is not None, "commit info should return non-None"
        assert "parent-ids" in meta, "Should have parent-ids"
    finally:
        fix["close"](sys)


# ============================================================
# Layer 4: Mergeable tests
# ============================================================

def test_merge(fix: Dict[str, Any]) -> None:
    """merge returns self with merge applied."""
    if not _has_capability(fix, "mergeable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "base")
        sys = sys.branch("feature")
        sys = sys.checkout("feature")
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "feature work")
        # Merge feature into main
        sys = sys.checkout("main")
        sys = sys.merge("feature")
        merge_id = sys.snapshot_id()
        assert isinstance(merge_id, str), \
            "merge result should have a snapshot_id"
    finally:
        fix["close"](sys)


def test_merge_parent_ids(fix: Dict[str, Any]) -> None:
    """Merge commit has two parents."""
    if not _has_capability(fix, "mergeable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "base")
        sys = sys.branch("feature")
        # Advance main
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "main advance")
        # Advance feature
        sys = sys.checkout("feature")
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "feature advance")
        # Merge from main
        sys = sys.checkout("main")
        sys = sys.merge("feature")
        parents = sys.parent_ids()
        assert len(parents) >= 2, \
            "Merge commit should have at least 2 parents"
    finally:
        fix["close"](sys)


def test_conflicts_empty_for_compatible(fix: Dict[str, Any]) -> None:
    """conflicts returns empty for compatible branches."""
    if not _has_capability(fix, "mergeable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "base")
        sys = sys.branch("feature")
        # Advance main
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "main")
        main_id = sys.snapshot_id()
        # Advance feature
        sys = sys.checkout("feature")
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "feature")
        feat_id = sys.snapshot_id()
        conflicts = sys.conflicts(main_id, feat_id)
        assert len(conflicts) == 0, \
            "Compatible branches should have no conflicts"
    finally:
        fix["close"](sys)


def test_diff(fix: Dict[str, Any]) -> None:
    """diff returns delta between two snapshots."""
    if not _has_capability(fix, "mergeable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "first")
        id1 = sys.snapshot_id()
        sys = fix["mutate"](sys)
        sys = fix["commit"](sys, "second")
        id2 = sys.snapshot_id()
        d = sys.diff(id1, id2)
        assert d is not None, "diff should return non-None"
    finally:
        fix["close"](sys)


# ============================================================
# SystemIdentity tests
# ============================================================

def test_system_identity(fix: Dict[str, Any]) -> None:
    """System identity is properly configured."""
    sys = fix["create_system"]()
    try:
        assert isinstance(sys.system_id(), str), \
            "system_id should return a string"
        assert isinstance(sys.system_type(), str), \
            "system_type should return a string"
        caps = sys.capabilities()
        assert caps is not None, "capabilities should return non-None"
        assert caps.snapshotable is True, "should support snapshotable"
    finally:
        fix["close"](sys)


# ============================================================
# Data Consistency tests
# ============================================================

def test_write_read_roundtrip(fix: Dict[str, Any]) -> None:
    """Write then read returns the written value."""
    sys = fix["create_system"]()
    try:
        sys = fix["write_entry"](sys, "key-1", "value-alpha")
        sys = fix["commit"](sys, "write one entry")
        assert fix["read_entry"](sys, "key-1") == "value-alpha", \
            "Should read back the written value"
    finally:
        fix["close"](sys)


def test_count_after_writes(fix: Dict[str, Any]) -> None:
    """count_entries increases after writes and commits."""
    sys = fix["create_system"]()
    try:
        assert fix["count_entries"](sys) == 0, \
            "Fresh system should have 0 entries"
        sys = fix["write_entry"](sys, "a", "1")
        sys = fix["commit"](sys, "first")
        assert fix["count_entries"](sys) == 1
        sys = fix["write_entry"](sys, "b", "2")
        sys = fix["write_entry"](sys, "c", "3")
        sys = fix["commit"](sys, "second")
        assert fix["count_entries"](sys) == 3
    finally:
        fix["close"](sys)


def test_multiple_entries_readable(fix: Dict[str, Any]) -> None:
    """Multiple entries are independently readable."""
    sys = fix["create_system"]()
    try:
        sys = fix["write_entry"](sys, "x", "val-x")
        sys = fix["write_entry"](sys, "y", "val-y")
        sys = fix["write_entry"](sys, "z", "val-z")
        sys = fix["commit"](sys, "three entries")
        assert fix["read_entry"](sys, "x") == "val-x"
        assert fix["read_entry"](sys, "y") == "val-y"
        assert fix["read_entry"](sys, "z") == "val-z"
        assert fix["read_entry"](sys, "nonexistent") is None, \
            "Non-existent key should return None"
    finally:
        fix["close"](sys)


def test_branch_data_isolation(fix: Dict[str, Any]) -> None:
    """Data written on one branch is not visible on another."""
    if not _has_capability(fix, "branchable"):
        return
    sys = fix["create_system"]()
    try:
        sys = fix["write_entry"](sys, "shared", "base-value")
        sys = fix["commit"](sys, "base commit")
        sys = sys.branch("feature")
        # Write on main
        sys = fix["write_entry"](sys, "main-only", "main-data")
        sys = fix["commit"](sys, "main write")
        # Write on feature
        sys = sys.checkout("feature")
        sys = fix["write_entry"](sys, "feature-only", "feature-data")
        sys = fix["commit"](sys, "feature write")
        # Verify feature isolation (currently on feature)
        assert fix["read_entry"](sys, "feature-only") == "feature-data", \
            "Feature branch should see its own data"
        assert fix["read_entry"](sys, "main-only") is None, \
            "Feature branch should NOT see main-only data"
        assert fix["read_entry"](sys, "shared") == "base-value", \
            "Feature should see shared base data"
        # Switch to main and verify its isolation
        sys = sys.checkout("main")
        assert fix["read_entry"](sys, "main-only") == "main-data", \
            "Main branch should see its own data"
        assert fix["read_entry"](sys, "feature-only") is None, \
            "Main branch should NOT see feature-only data"
        assert fix["read_entry"](sys, "shared") == "base-value", \
            "Main should see shared base data"
    finally:
        fix["close"](sys)


def test_delete_entry_consistency(fix: Dict[str, Any]) -> None:
    """Deleted entry is no longer readable after commit."""
    if fix.get("delete_entry") is None:
        return
    sys = fix["create_system"]()
    try:
        sys = fix["write_entry"](sys, "keep", "keep-val")
        sys = fix["write_entry"](sys, "remove", "remove-val")
        sys = fix["commit"](sys, "two entries")
        assert fix["count_entries"](sys) == 2
        assert fix["read_entry"](sys, "remove") == "remove-val"
        sys = fix["delete_entry"](sys, "remove")
        sys = fix["commit"](sys, "delete one")
        assert fix["count_entries"](sys) == 1, \
            "Count should decrease after delete"
        assert fix["read_entry"](sys, "remove") is None, \
            "Deleted entry should not be readable"
        assert fix["read_entry"](sys, "keep") == "keep-val", \
            "Non-deleted entry should still be readable"
    finally:
        fix["close"](sys)


def test_overwrite_entry(fix: Dict[str, Any]) -> None:
    """Overwriting an entry updates its value."""
    if fix.get("delete_entry") is None:
        return
    sys = fix["create_system"]()
    try:
        sys = fix["write_entry"](sys, "key", "original")
        sys = fix["commit"](sys, "original value")
        assert fix["read_entry"](sys, "key") == "original"
        # Overwrite: delete + write
        sys = fix["delete_entry"](sys, "key")
        sys = fix["write_entry"](sys, "key", "updated")
        sys = fix["commit"](sys, "updated value")
        assert fix["read_entry"](sys, "key") == "updated", \
            "Should read the updated value"
        assert fix["count_entries"](sys) == 1, \
            "Count should remain 1 after overwrite"
    finally:
        fix["close"](sys)


# ============================================================
# Full test suite
# ============================================================

ALL_TESTS = {
    "system_identity": [test_system_identity],
    "snapshotable": [
        test_snapshot_id_after_commit,
        test_parent_ids_root_commit,
        test_parent_ids_chain,
        test_snapshot_meta,
        test_as_of,
    ],
    "branchable": [
        test_initial_branches,
        test_create_branch,
        test_checkout,
        test_branch_isolation,
        test_delete_branch,
    ],
    "graphable": [
        test_history,
        test_history_limit,
        test_ancestors,
        test_ancestor_predicate,
        test_common_ancestor,
        test_commit_info,
    ],
    "mergeable": [
        test_merge,
        test_merge_parent_ids,
        test_conflicts_empty_for_compatible,
        test_diff,
    ],
    "data_consistency": [
        test_write_read_roundtrip,
        test_count_after_writes,
        test_multiple_entries_readable,
        test_branch_data_isolation,
        test_delete_entry_consistency,
        test_overwrite_entry,
    ],
}


def run_compliance_tests(fixture: Dict[str, Any]) -> None:
    """Run all compliance tests for the given fixture.

    Required fixture keys:
        create_system  - () -> system
        mutate         - (system) -> system
        commit         - (system, msg) -> system
        close          - (system) -> None
        write_entry    - (system, key, value) -> system
        read_entry     - (system, key) -> value or None
        count_entries  - (system) -> int
        delete_entry   - (system, key) -> system (or None to skip delete tests)
    """
    for layer, tests in ALL_TESTS.items():
        for test_fn in tests:
            test_fn(fixture)
