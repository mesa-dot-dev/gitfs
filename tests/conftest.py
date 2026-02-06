"""Shared setup for git-fs integration tests."""

from __future__ import annotations

import contextlib
import subprocess
import time
from pathlib import Path
from typing import TYPE_CHECKING

from testcontainers.core.container import DockerContainer

if TYPE_CHECKING:
    from collections.abc import Iterator

IMAGE_TAG = "git-fs-test:latest"
REPO_ROOT = Path(__file__).resolve().parent.parent

_GITFS_READY_TIMEOUT = 60
_GITFS_READY_POLL_INTERVAL = 2


@contextlib.contextmanager
def gitfs_container_factory(port: int) -> Iterator[DockerContainer]:
    """Create a privileged container with git-fs mounted and ready."""
    subprocess.run(
        [
            "docker",
            "build",
            "-f",
            str(REPO_ROOT / "tests/docker/Dockerfile"),
            "-t",
            IMAGE_TAG,
            str(REPO_ROOT),
        ],
        check=True,
        timeout=600,
    )

    container = (
        DockerContainer(IMAGE_TAG).with_kwargs(privileged=True).with_exposed_ports(port)
    )
    with container:
        deadline = time.monotonic() + _GITFS_READY_TIMEOUT
        while time.monotonic() < deadline:
            exit_code, _ = container.exec(["test", "-f", "/tmp/git-fs-ready"])
            if exit_code == 0:
                break
            time.sleep(_GITFS_READY_POLL_INTERVAL)
        else:
            msg = f"git-fs mount did not become ready within {_GITFS_READY_TIMEOUT}s"
            raise TimeoutError(msg)

        yield container
