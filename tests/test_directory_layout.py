"""Compare directory layout and file contents of git-fs vs git clone."""

from __future__ import annotations

import enum
import fnmatch
import shutil
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Self

import pytest

from tests.conftest import gitfs_container_factory

REPOS = [
    "kelseyhightower/nocode",
    "github-samples/planventure",
]

MOUNT_POINT = "/mnt/git-fs"


def shallow_clone(repo_slug: str) -> Path:
    """Shallow-clone a GitHub repo, returning the local path."""
    owner, repo = repo_slug.split("/", 1)
    dest = Path(f"/tmp/clone-{owner}-{repo}")
    shutil.rmtree(dest, ignore_errors=True)
    subprocess.run(
        [
            "git",
            "clone",
            "--depth",
            "1",
            f"https://github.com/{repo_slug}.git",
            str(dest),
        ],
        check=True,
        capture_output=True,
        timeout=120,
    )
    return dest


class EntryKind(enum.StrEnum):
    """Kind of filesystem entry."""

    FILE = "FILE"
    DIRECTORY = "DIRECTORY"
    SYMLINK = "SYMLINK"


@dataclass(frozen=True, slots=True)
class _EntryInfo:
    """Filesystem entry metadata: kind and executable bit."""

    kind: EntryKind
    executable: bool

    @classmethod
    def from_path(cls, p: Path) -> Self:
        """Classify a path and capture the executable bit for files."""
        if p.is_symlink():
            return cls(kind=EntryKind.SYMLINK, executable=False)
        if p.is_dir():
            return cls(kind=EntryKind.DIRECTORY, executable=False)
        mode = p.stat().st_mode
        return cls(kind=EntryKind.FILE, executable=bool(mode & stat.S_IXUSR))

    def __str__(self) -> str:
        if self.kind is EntryKind.FILE:
            return f"FILE({'x' if self.executable else '-'})"
        return self.kind.value


def dfs_compare(
    lhs: Path, rhs: Path, *, excluded_globs: frozenset[str] | set[str] = frozenset()
) -> None:
    """Recursively compare two directory trees, raising AssertionError on mismatch.

    Walks both trees simultaneously via sorted iterdir(), comparing child
    names, entry types, file contents, and symlink targets at each level,
    then recursing into subdirectories.
    """

    def _is_excluded(name: str) -> bool:
        return any(fnmatch.fnmatch(name, g) for g in excluded_globs)

    lhs_info = _EntryInfo.from_path(lhs)
    rhs_info = _EntryInfo.from_path(rhs)
    assert lhs_info == rhs_info, (
        f"Entry mismatch at {lhs.name}: {lhs} is {lhs_info}, {rhs} is {rhs_info}"
    )

    if lhs_info.kind is EntryKind.SYMLINK:
        lhs_target = lhs.readlink()
        rhs_target = rhs.readlink()
        assert lhs_target == rhs_target, (
            f"Symlink target mismatch at {lhs.name}: "
            f"{lhs} -> {lhs_target}, {rhs} -> {rhs_target}"
        )
        return

    if lhs_info.kind is EntryKind.FILE:
        lhs_bytes = lhs.read_bytes()
        rhs_bytes = rhs.read_bytes()
        assert lhs_bytes == rhs_bytes, (
            f"Content mismatch at {lhs.name}: "
            f"{lhs} ({len(lhs_bytes)} bytes) != {rhs} ({len(rhs_bytes)} bytes)"
        )
        return

    lhs_children = sorted(
        [c for c in lhs.iterdir() if not _is_excluded(c.name)], key=lambda p: p.name
    )
    rhs_children = sorted(
        [c for c in rhs.iterdir() if not _is_excluded(c.name)], key=lambda p: p.name
    )

    lhs_names = [c.name for c in lhs_children]
    rhs_names = [c.name for c in rhs_children]

    missing_in_rhs = set(lhs_names) - set(rhs_names)
    extra_in_rhs = set(rhs_names) - set(lhs_names)
    if missing_in_rhs or extra_in_rhs:
        parts = [f"Children mismatch in {lhs.name}:"]
        if missing_in_rhs:
            parts.append(f"  Missing in {rhs}: {sorted(missing_in_rhs)}")
        if extra_in_rhs:
            parts.append(f"  Extra in {rhs}: {sorted(extra_in_rhs)}")
        raise AssertionError("\n".join(parts))

    for lhs_child, rhs_child in zip(lhs_children, rhs_children, strict=True):
        dfs_compare(lhs_child, rhs_child, excluded_globs=excluded_globs)


def test_dfs_compare_detects_content_mismatch(tmp_path: Path) -> None:
    """dfs_compare should fail when file contents differ."""
    lhs = tmp_path / "lhs"
    rhs = tmp_path / "rhs"
    lhs.mkdir()
    rhs.mkdir()

    (lhs / "file.txt").write_text("hello")
    (rhs / "file.txt").write_text("world")

    with pytest.raises(AssertionError, match="Content mismatch"):
        dfs_compare(lhs, rhs)


def test_dfs_compare_detects_symlink_target_mismatch(tmp_path: Path) -> None:
    """dfs_compare should fail when symlink targets differ."""
    lhs = tmp_path / "lhs"
    rhs = tmp_path / "rhs"
    lhs.mkdir()
    rhs.mkdir()

    (lhs / "link").symlink_to("target_a")
    (rhs / "link").symlink_to("target_b")

    with pytest.raises(AssertionError, match="Symlink target mismatch"):
        dfs_compare(lhs, rhs)


def test_dfs_compare_passes_when_contents_match(tmp_path: Path) -> None:
    """dfs_compare should pass when structure and contents are identical."""
    lhs = tmp_path / "lhs"
    rhs = tmp_path / "rhs"
    for root in (lhs, rhs):
        root.mkdir()
        (root / "a.txt").write_text("same content")
        (root / "sub").mkdir()
        (root / "sub" / "b.txt").write_bytes(b"\x00\x01\x02")
        (root / "link").symlink_to("a.txt")

    dfs_compare(lhs, rhs)  # Should not raise


@pytest.mark.integration
@pytest.mark.timeout(180)
@pytest.mark.parametrize("repo_slug", REPOS, ids=REPOS)
@pytest.mark.in_container(factory=gitfs_container_factory)
def test_directory_layout_matches_clone(repo_slug: str) -> None:
    """Compare the directory tree visible through git-fs with a shallow clone.

    Entry names, types (file/directory/symlink), file contents, and symlink
    targets are all compared.
    """
    owner, repo = repo_slug.split("/", 1)
    clone_dest = shallow_clone(repo_slug)
    gitfs_root = Path(f"{MOUNT_POINT}/github/{owner}/{repo}")

    dfs_compare(clone_dest, gitfs_root, excluded_globs={".git"})
