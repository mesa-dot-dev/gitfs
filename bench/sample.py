"""Sample benchmark to validate the harness works end-to-end."""

from __future__ import annotations

from pathlib import Path

from bench import bench_many, git_bench, mesafs_bench


@bench_many([
    #git_bench("mesa-ci/planventure"),
    mesafs_bench("mesa-ci/planventure"),
])
def bench_list_root(repo_path: Path) -> None:
    """List the root directory of the cloned repo."""
    for path in Path(repo_path).rglob("*"):
        _ = path


@bench_many([
    mesafs_bench("mesa-ci/planventure"),
])
def bench_list_root_mesafs(mount_path: Path) -> None:
    """List root directory via mesafs FUSE mount."""
    for path in Path(mount_path).rglob("*"):
        _ = path
