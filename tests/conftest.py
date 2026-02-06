"""Shared fixtures for git-fs integration tests."""

from __future__ import annotations

import logging
import subprocess
import textwrap
import time
from pathlib import Path
from typing import Generator

import pytest
from testcontainers.core.container import DockerContainer

logger = logging.getLogger(__name__)

MOUNT_POINT = "/mnt/git-fs"
API_KEY = "dp_live_uAgKRbhVNcDiUZXVyTQbBIaJEerhSwQh"
GITFS_READY_TIMEOUT = 60
GITFS_READY_POLL_INTERVAL = 2

IMAGE_TAG = "git-fs-integration-test:latest"

REPO_ROOT = Path(__file__).resolve().parent.parent
DOCKERFILE_PATH = REPO_ROOT / "Dockerfile.test"


@pytest.fixture(scope="session")
def gitfs_image() -> str:
    """Build the git-fs test Docker image once per test session via docker CLI."""
    logger.info("Building Docker image from %s", REPO_ROOT)
    subprocess.run(
        [
            "docker", "build",
            "-f", str(DOCKERFILE_PATH),
            "-t", IMAGE_TAG,
            str(REPO_ROOT),
        ],
        check=True,
        timeout=600,
    )
    return IMAGE_TAG


def _generate_config_toml() -> str:
    return textwrap.dedent(f"""\
        mount-point = "{MOUNT_POINT}"
        uid = 0
        gid = 0

        [organizations.github]
        api-key = "{API_KEY}"

        [cache]
        path = "/tmp/git-fs-cache"

        [daemon]
        pid-file = "/tmp/git-fs.pid"
    """)


def _wait_for_mount(container: DockerContainer, timeout: int = GITFS_READY_TIMEOUT) -> None:
    """Wait until the FUSE mount is up by checking that 'github' appears in ls /mnt/git-fs.

    Note: The filesystem is on-demand — you cannot ls /mnt/git-fs/github,
    but you CAN access specific repo paths like /mnt/git-fs/github/owner/repo.
    We just need to confirm the FUSE mount itself is serving.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        exit_code, output = container.exec(["ls", MOUNT_POINT])
        decoded = output.decode("utf-8", errors="replace").strip()
        if exit_code == 0 and "github" in decoded:
            logger.info("git-fs mount is ready (ls %s: %s)", MOUNT_POINT, decoded)
            return
        time.sleep(GITFS_READY_POLL_INTERVAL)
    exit_code, output = container.exec(["ls", "-la", MOUNT_POINT])
    raise TimeoutError(
        f"git-fs mount did not become ready within {timeout}s. "
        f"Mount point listing (exit={exit_code}): {output.decode('utf-8', errors='replace')}"
    )


@pytest.fixture(scope="session")
def gitfs_container(gitfs_image: str) -> Generator[DockerContainer, None, None]:
    """Start a privileged container with FUSE, write config, launch git-fs."""
    container = DockerContainer(gitfs_image).with_kwargs(privileged=True)

    with container:
        # Write config
        config_content = _generate_config_toml()
        container.exec(["mkdir", "-p", "/etc/git-fs"])
        container.exec([
            "sh", "-c",
            f"cat > /etc/git-fs/config.toml << 'HEREDOC'\n{config_content}\nHEREDOC",
        ])

        exit_code, output = container.exec(["cat", "/etc/git-fs/config.toml"])
        assert exit_code == 0, f"Failed to write config: {output}"
        logger.info("Config written:\n%s", output.decode())

        # Create mount point and start git-fs
        container.exec(["mkdir", "-p", MOUNT_POINT])
        container.exec([
            "sh", "-c",
            "nohup git-fs --config-path /etc/git-fs/config.toml run "
            "> /tmp/git-fs-stdout.log 2> /tmp/git-fs-stderr.log &",
        ])

        _wait_for_mount(container)
        yield container

        # Diagnostic logs on teardown
        _, stdout_log = container.exec(["cat", "/tmp/git-fs-stdout.log"])
        _, stderr_log = container.exec(["cat", "/tmp/git-fs-stderr.log"])
        logger.info("git-fs stdout:\n%s", stdout_log.decode("utf-8", errors="replace"))
        logger.info("git-fs stderr:\n%s", stderr_log.decode("utf-8", errors="replace"))


def clone_repo(repo_slug: str, dest: Path) -> Path:
    """Clone a GitHub repo at depth 1 into dest."""
    url = f"https://github.com/{repo_slug}.git"
    logger.info("Cloning %s into %s", url, dest)
    subprocess.run(
        ["git", "clone", "--depth", "1", url, str(dest)],
        check=True,
        capture_output=True,
        timeout=120,
    )
    return dest
