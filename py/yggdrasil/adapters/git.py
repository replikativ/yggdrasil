"""Git adapter for Yggdrasil protocols.

Thin wrapper around git CLI, mirrors the Clojure adapter.
"""

import subprocess
from typing import Set, Optional, List, Dict, Any

from yggdrasil.protocols import (
    SystemIdentity, Snapshotable, Branchable, Graphable, Mergeable,
)
from yggdrasil.types import SnapshotRef, Capabilities, Conflict


def _git(repo_path: str, *args: str) -> str:
    """Run a git command and return stdout."""
    result = subprocess.run(
        ["git", "-C", repo_path] + list(args),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"git error: {result.stderr.strip()}")
    return result.stdout.strip()


def _git_lines(repo_path: str, *args: str) -> List[str]:
    """Run a git command and return output lines."""
    out = _git(repo_path, *args)
    return out.split("\n") if out else []


class GitSystem(SystemIdentity, Snapshotable, Branchable, Graphable, Mergeable):
    """Git repository adapter implementing Yggdrasil protocols."""

    def __init__(self, repo_path: str, system_name: Optional[str] = None):
        self.repo_path = repo_path
        self._system_name = system_name

    # -- SystemIdentity --

    def system_id(self) -> str:
        return self._system_name or f"git:{self.repo_path}"

    def system_type(self) -> str:
        return "git"

    def capabilities(self) -> Capabilities:
        return Capabilities(
            snapshotable=True,
            branchable=True,
            graphable=True,
            mergeable=True,
            overlayable=False,
            watchable=False,
        )

    # -- Snapshotable --

    def snapshot_id(self) -> str:
        return _git(self.repo_path, "rev-parse", "HEAD")

    def parent_ids(self, snap_id: Optional[str] = None) -> Set[str]:
        ref = snap_id or "HEAD"
        try:
            out = _git(self.repo_path, "rev-parse", f"{ref}^@")
        except RuntimeError:
            return set()
        return set(out.split("\n")) if out else set()

    def as_of(self, snap_id: str, opts: Optional[Dict[str, Any]] = None) -> Any:
        return {"repo_path": self.repo_path, "commit": snap_id}

    def snapshot_meta(self, snap_id: str, opts: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        fmt = "%H%n%P%n%an <%ae>%n%at%n%s"
        lines = _git_lines(self.repo_path, "log", "-1", f"--format={fmt}", snap_id)
        if len(lines) < 5:
            return {}
        parent_str = lines[1]
        return {
            "snapshot-id": lines[0],
            "parent-ids": set(parent_str.split()) if parent_str else set(),
            "author": lines[2],
            "timestamp": int(lines[3]) * 1000,
            "message": lines[4],
        }

    # -- Branchable --

    def branches(self, opts: Optional[Dict[str, Any]] = None) -> Set[str]:
        lines = _git_lines(self.repo_path, "branch", "--list", "--format=%(refname:short)")
        return set(lines)

    def current_branch(self) -> str:
        return _git(self.repo_path, "rev-parse", "--abbrev-ref", "HEAD")

    def branch(self, name: str, from_ref: Optional[str] = None,
               opts: Optional[Dict[str, Any]] = None) -> "GitSystem":
        args = ["branch", name]
        if from_ref:
            args.append(from_ref)
        _git(self.repo_path, *args)
        return self

    def delete_branch(self, name: str, opts: Optional[Dict[str, Any]] = None) -> "GitSystem":
        _git(self.repo_path, "branch", "-d", name)
        return self

    def checkout(self, name: str, opts: Optional[Dict[str, Any]] = None) -> "GitSystem":
        _git(self.repo_path, "checkout", name)
        return self

    # -- Graphable --

    def history(self, limit: Optional[int] = None, since: Optional[str] = None,
                opts: Optional[Dict[str, Any]] = None) -> List[str]:
        args = ["log", "--format=%H"]
        if limit:
            args.append(f"-{limit}")
        if since:
            args.append(f"{since}..HEAD")
        return _git_lines(self.repo_path, *args)

    def ancestors(self, snap_id: str, opts: Optional[Dict[str, Any]] = None) -> List[str]:
        return _git_lines(self.repo_path, "rev-list", snap_id)

    def is_ancestor(self, a: str, b: str, opts: Optional[Dict[str, Any]] = None) -> bool:
        try:
            _git(self.repo_path, "merge-base", "--is-ancestor", a, b)
            return True
        except RuntimeError:
            return False

    def common_ancestor(self, a: str, b: str,
                        opts: Optional[Dict[str, Any]] = None) -> Optional[str]:
        try:
            return _git(self.repo_path, "merge-base", a, b)
        except RuntimeError:
            return None

    # -- Mergeable --

    def merge(self, source: str, strategy: Optional[str] = None,
              message: Optional[str] = None,
              opts: Optional[Dict[str, Any]] = None) -> "GitSystem":
        args = ["merge"]
        if strategy:
            args.append(f"--strategy-option={strategy}")
        if message:
            args.extend(["-m", message])
        args.append(source)
        _git(self.repo_path, *args)
        return self

    def conflicts(self, a: str, b: str,
                  opts: Optional[Dict[str, Any]] = None) -> List[Conflict]:
        try:
            base = _git(self.repo_path, "merge-base", a, b)
        except RuntimeError:
            return []
        result = subprocess.run(
            ["git", "-C", self.repo_path, "merge-tree", base, a, b],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            return [Conflict(path=("merge-tree",), base=base, ours=a, theirs=b)]
        return []

    def diff(self, a: str, b: str, opts: Optional[Dict[str, Any]] = None) -> Any:
        return _git(self.repo_path, "diff", a, b)


def create(repo_path: str, system_name: Optional[str] = None) -> GitSystem:
    """Create a Git adapter.

    Usage:
        git = create("/path/to/repo")
        git = create("/path/to/repo", system_name="my-repo")
    """
    return GitSystem(repo_path, system_name)
