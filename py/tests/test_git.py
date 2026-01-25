"""Compliance tests for the Git adapter.

Tests the GitSystem adapter against the full Yggdrasil compliance suite.
Requires git to be installed.
"""

import os
import subprocess
import tempfile
import shutil
import uuid

import pytest

from yggdrasil.adapters.git import GitSystem, create
from yggdrasil.compliance import run_compliance_tests, ALL_TESTS


# ============================================================
# Fixture helpers
# ============================================================

DATA_DIR = "data"  # subdirectory for test entries (avoids counting .gitkeep etc.)


def _git(repo: str, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", repo] + list(args),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git error: {result.stderr.strip()}")
    return result.stdout.strip()


def _init_repo() -> str:
    """Create a temporary git repo with an initial empty commit on 'main'."""
    tmp = tempfile.mkdtemp(prefix="ygg-test-git-")
    _git(tmp, "init", "-b", "main")
    _git(tmp, "config", "user.email", "test@yggdrasil.dev")
    _git(tmp, "config", "user.name", "Yggdrasil Test")
    # Create data dir with .gitkeep so it's tracked
    os.makedirs(os.path.join(tmp, DATA_DIR))
    gitkeep = os.path.join(tmp, DATA_DIR, ".gitkeep")
    open(gitkeep, "w").close()
    _git(tmp, "add", "-A")
    _git(tmp, "commit", "-m", "init")
    return tmp


def _create_system() -> GitSystem:
    repo = _init_repo()
    return create(repo)


def _mutate(sys: GitSystem) -> GitSystem:
    """Create a unique file in the repo to make it dirty."""
    fname = f"_mut_{uuid.uuid4().hex[:8]}.txt"
    fpath = os.path.join(sys.repo_path, fname)
    with open(fpath, "w") as f:
        f.write(f"mutation {uuid.uuid4()}\n")
    _git(sys.repo_path, "add", fname)
    return sys


def _commit(sys: GitSystem, msg: str) -> GitSystem:
    """Stage all and commit."""
    # Stage any remaining unstaged changes
    _git(sys.repo_path, "add", "-A")
    try:
        _git(sys.repo_path, "commit", "--allow-empty-message", "-m", msg)
    except RuntimeError as e:
        if "nothing to commit" not in str(e):
            raise
    return sys


def _close(sys: GitSystem) -> None:
    """Remove the temporary repo."""
    if os.path.exists(sys.repo_path):
        shutil.rmtree(sys.repo_path)


def _write_entry(sys: GitSystem, key: str, value: str) -> GitSystem:
    """Write a file in data/ directory with key as filename."""
    data_path = os.path.join(sys.repo_path, DATA_DIR)
    os.makedirs(data_path, exist_ok=True)
    fpath = os.path.join(data_path, key)
    with open(fpath, "w") as f:
        f.write(value)
    _git(sys.repo_path, "add", os.path.join(DATA_DIR, key))
    return sys


def _read_entry(sys: GitSystem, key: str) -> str:
    """Read a file from data/ directory, or None if not exists."""
    fpath = os.path.join(sys.repo_path, DATA_DIR, key)
    if not os.path.exists(fpath):
        return None
    with open(fpath, "r") as f:
        return f.read()


def _count_entries(sys: GitSystem) -> int:
    """Count files in the data/ directory (excluding .gitkeep)."""
    data_path = os.path.join(sys.repo_path, DATA_DIR)
    if not os.path.exists(data_path):
        return 0
    return len([f for f in os.listdir(data_path)
                if os.path.isfile(os.path.join(data_path, f)) and f != ".gitkeep"])


def _delete_entry(sys: GitSystem, key: str) -> GitSystem:
    """Delete a file from data/ directory."""
    fpath = os.path.join(sys.repo_path, DATA_DIR, key)
    if os.path.exists(fpath):
        _git(sys.repo_path, "rm", "-f", os.path.join(DATA_DIR, key))
    return sys


GIT_FIXTURE = {
    "create_system": _create_system,
    "mutate": _mutate,
    "commit": _commit,
    "close": _close,
    "write_entry": _write_entry,
    "read_entry": _read_entry,
    "count_entries": _count_entries,
    "delete_entry": _delete_entry,
}


# ============================================================
# Individual test functions (pytest discovers these)
# ============================================================

class TestGitSystemIdentity:
    def test_system_identity(self):
        from yggdrasil.compliance import test_system_identity
        test_system_identity(GIT_FIXTURE)


class TestGitSnapshotable:
    def test_snapshot_id_after_commit(self):
        from yggdrasil.compliance import test_snapshot_id_after_commit
        test_snapshot_id_after_commit(GIT_FIXTURE)

    def test_parent_ids_root_commit(self):
        from yggdrasil.compliance import test_parent_ids_root_commit
        test_parent_ids_root_commit(GIT_FIXTURE)

    def test_parent_ids_chain(self):
        from yggdrasil.compliance import test_parent_ids_chain
        test_parent_ids_chain(GIT_FIXTURE)

    def test_snapshot_meta(self):
        from yggdrasil.compliance import test_snapshot_meta
        test_snapshot_meta(GIT_FIXTURE)

    def test_as_of(self):
        from yggdrasil.compliance import test_as_of
        test_as_of(GIT_FIXTURE)


class TestGitBranchable:
    def test_initial_branches(self):
        from yggdrasil.compliance import test_initial_branches
        test_initial_branches(GIT_FIXTURE)

    def test_create_branch(self):
        from yggdrasil.compliance import test_create_branch
        test_create_branch(GIT_FIXTURE)

    def test_checkout(self):
        from yggdrasil.compliance import test_checkout
        test_checkout(GIT_FIXTURE)

    def test_branch_isolation(self):
        from yggdrasil.compliance import test_branch_isolation
        test_branch_isolation(GIT_FIXTURE)

    def test_delete_branch(self):
        from yggdrasil.compliance import test_delete_branch
        test_delete_branch(GIT_FIXTURE)


class TestGitGraphable:
    def test_history(self):
        from yggdrasil.compliance import test_history
        test_history(GIT_FIXTURE)

    def test_history_limit(self):
        from yggdrasil.compliance import test_history_limit
        test_history_limit(GIT_FIXTURE)

    def test_ancestors(self):
        from yggdrasil.compliance import test_ancestors
        test_ancestors(GIT_FIXTURE)

    def test_ancestor_predicate(self):
        from yggdrasil.compliance import test_ancestor_predicate
        test_ancestor_predicate(GIT_FIXTURE)

    def test_common_ancestor(self):
        from yggdrasil.compliance import test_common_ancestor
        test_common_ancestor(GIT_FIXTURE)

    def test_commit_info(self):
        from yggdrasil.compliance import test_commit_info
        test_commit_info(GIT_FIXTURE)


class TestGitMergeable:
    def test_merge(self):
        from yggdrasil.compliance import test_merge
        test_merge(GIT_FIXTURE)

    def test_merge_parent_ids(self):
        from yggdrasil.compliance import test_merge_parent_ids
        test_merge_parent_ids(GIT_FIXTURE)

    def test_conflicts_empty_for_compatible(self):
        from yggdrasil.compliance import test_conflicts_empty_for_compatible
        test_conflicts_empty_for_compatible(GIT_FIXTURE)

    def test_diff(self):
        from yggdrasil.compliance import test_diff
        test_diff(GIT_FIXTURE)


class TestGitDataConsistency:
    def test_write_read_roundtrip(self):
        from yggdrasil.compliance import test_write_read_roundtrip
        test_write_read_roundtrip(GIT_FIXTURE)

    def test_count_after_writes(self):
        from yggdrasil.compliance import test_count_after_writes
        test_count_after_writes(GIT_FIXTURE)

    def test_multiple_entries_readable(self):
        from yggdrasil.compliance import test_multiple_entries_readable
        test_multiple_entries_readable(GIT_FIXTURE)

    def test_branch_data_isolation(self):
        from yggdrasil.compliance import test_branch_data_isolation
        test_branch_data_isolation(GIT_FIXTURE)

    def test_delete_entry_consistency(self):
        from yggdrasil.compliance import test_delete_entry_consistency
        test_delete_entry_consistency(GIT_FIXTURE)

    def test_overwrite_entry(self):
        from yggdrasil.compliance import test_overwrite_entry
        test_overwrite_entry(GIT_FIXTURE)


class TestGitFullSuite:
    """Run all compliance tests as a single mega-test."""

    def test_run_all(self):
        run_compliance_tests(GIT_FIXTURE)
