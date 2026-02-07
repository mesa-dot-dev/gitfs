# Review: The manual tree walking feels incredibly janky. Can't we just run the `tree`
# command to confirm correctness?
"""Compare directory layout of git-fs mounted repos vs git clone --depth 1."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer

from tests.conftest import MOUNT_POINT, clone_repo

logger = logging.getLogger(__name__)


class EntryKind(Enum):
    FILE = auto()
    DIRECTORY = auto()
    SYMLINK = auto()


@dataclass(frozen=True)
class TreeEntry:
    relative_path: str
    kind: EntryKind


def gather_tree_from_clone(clone_path: Path) -> set[TreeEntry]:
    """Walk a cloned repo and collect entries, skipping .git/."""
    entries: set[TreeEntry] = set()
    clone_str = str(clone_path)

    for dirpath, dirnames, filenames in os.walk(clone_path):
        if ".git" in dirnames:
            dirnames.remove(".git")

        rel_dir = os.path.relpath(dirpath, clone_str)

        for d in dirnames:
            rel = os.path.join(rel_dir, d) if rel_dir != "." else d
            entries.add(TreeEntry(relative_path=rel, kind=EntryKind.DIRECTORY))

        for f in filenames:
            rel = os.path.join(rel_dir, f) if rel_dir != "." else f
            full = os.path.join(dirpath, f)
            if os.path.islink(full):
                entries.add(TreeEntry(relative_path=rel, kind=EntryKind.SYMLINK))
            else:
                entries.add(TreeEntry(relative_path=rel, kind=EntryKind.FILE))

    return entries


def _get_gitfs_logs(container: DockerContainer) -> str:
    """Fetch git-fs logs from the container for diagnostic output."""
    parts = []
    _, stderr_log = container.exec(["cat", "/tmp/git-fs-stderr.log"])
    parts.append(f"stderr:\n{stderr_log.decode('utf-8', errors='replace')}")
    _, stdout_log = container.exec(["cat", "/tmp/git-fs-stdout.log"])
    parts.append(f"stdout:\n{stdout_log.decode('utf-8', errors='replace')}")
    return "\n".join(parts)


def gather_tree_from_gitfs(
    container: DockerContainer,
    owner: str,
    repo: str,
) -> set[TreeEntry]:
    """Run `find` inside the container to list the git-fs mounted tree."""
    repo_root = f"{MOUNT_POINT}/github/{owner}/{repo}"

    exit_code, output = container.exec(["ls", repo_root])
    if exit_code != 0:
        raise RuntimeError(
            f"Repo root {repo_root} not accessible: "
            f"{output.decode('utf-8', errors='replace')}\n"
            f"git-fs logs:\n{_get_gitfs_logs(container)}",
        )

    exit_code, output = container.exec(
        [
            "find",
            repo_root,
            "-mindepth",
            "1",
            "-printf",
            r"%y %P\n",
        ],
    )
    if exit_code != 0:
        raise RuntimeError(f"find failed (exit={exit_code}): {output.decode('utf-8', errors='replace')}")

    entries: set[TreeEntry] = set()
    for line in output.decode("utf-8", errors="replace").strip().splitlines():
        if not line.strip():
            continue
        kind_char, _, rel_path = line.partition(" ")
        if kind_char == "f":
            kind = EntryKind.FILE
        elif kind_char == "d":
            kind = EntryKind.DIRECTORY
        elif kind_char == "l":
            kind = EntryKind.SYMLINK
        else:
            logger.warning("Unknown entry type %r for %r", kind_char, rel_path)
            kind = EntryKind.FILE
        entries.add(TreeEntry(relative_path=rel_path, kind=kind))

    return entries


REPOS = [
    "kelseyhightower/nocode",
    "mesa-dot-dev/git-fs",
    "stedolan/jq",
]


@pytest.mark.integration
@pytest.mark.timeout(180)
@pytest.mark.parametrize("repo_slug", REPOS, ids=REPOS)
def test_directory_layout_matches_clone(
    gitfs_container: DockerContainer,
    tmp_path: Path,
    repo_slug: str,
) -> None:
    """Compare the directory tree visible through git-fs with a shallow clone.
    Only entry names and types (file/directory/symlink) are compared, NOT contents.
    """
    owner, repo = repo_slug.split("/", 1)

    # Clone on host
    clone_dest = tmp_path / repo
    clone_repo(repo_slug, clone_dest)
    clone_tree = gather_tree_from_clone(clone_dest)
    logger.info("Clone tree for %s: %d entries", repo_slug, len(clone_tree))

    # Gather from git-fs
    gitfs_tree = gather_tree_from_gitfs(gitfs_container, owner, repo)
    logger.info("git-fs tree for %s: %d entries", repo_slug, len(gitfs_tree))

    # Compare paths
    clone_paths = {e.relative_path for e in clone_tree}
    gitfs_paths = {e.relative_path for e in gitfs_tree}

    missing_in_gitfs = clone_paths - gitfs_paths
    extra_in_gitfs = gitfs_paths - clone_paths

    if missing_in_gitfs:
        logger.warning("Missing in git-fs for %s:\n  %s", repo_slug, "\n  ".join(sorted(missing_in_gitfs)[:20]))
    if extra_in_gitfs:
        logger.warning("Extra in git-fs for %s:\n  %s", repo_slug, "\n  ".join(sorted(extra_in_gitfs)[:20]))

    assert clone_paths == gitfs_paths, (
        f"Directory layout mismatch for {repo_slug}. "
        f"Missing in git-fs: {len(missing_in_gitfs)}, "
        f"Extra in git-fs: {len(extra_in_gitfs)}"
    )

    # Compare entry types
    clone_map = {e.relative_path: e.kind for e in clone_tree}
    gitfs_map = {e.relative_path: e.kind for e in gitfs_tree}
    type_mismatches = [
        f"  {p}: clone={clone_map[p].name}, gitfs={gitfs_map[p].name}"
        for p in clone_paths & gitfs_paths
        if clone_map[p] != gitfs_map[p]
    ]

    assert not type_mismatches, f"Entry type mismatches for {repo_slug}:\n" + "\n".join(type_mismatches)
