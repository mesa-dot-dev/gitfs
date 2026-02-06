"""Shared fixtures for git-fs integration tests."""

from __future__ import annotations

import logging
import os
import subprocess
import textwrap
import time
from collections.abc import Generator
from pathlib import Path

import pytest
from testcontainers.core.container import DockerContainer

# Disable Ryuk (testcontainers' crash-recovery sidecar). The context manager
# on DockerContainer already handles cleanup on normal/exception exit.
os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"

logger = logging.getLogger(__name__)

MOUNT_POINT = "/mnt/git-fs"
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
            "docker",
            "build",
            "-f",
            str(DOCKERFILE_PATH),
            "-t",
            IMAGE_TAG,
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

        [cache]
        path = "/tmp/git-fs-cache"

        [daemon]
        pid-file = "/tmp/git-fs.pid"
    """)


def _wait_for_mount(container: DockerContainer, timeout: int = GITFS_READY_TIMEOUT) -> None:
    """Wait until the FUSE mount is up."""
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
def gitfs_container(gitfs_image: str) -> Generator[DockerContainer]:
    """Start a privileged container with FUSE, write config, launch git-fs."""
    container = DockerContainer(gitfs_image).with_kwargs(privileged=True)

    with container:
        # Write config
        config_content = _generate_config_toml()
        container.exec(["mkdir", "-p", "/etc/git-fs"])
        # Review: Is there no create-file method on the container?
        container.exec(
            [
                "sh",
                "-c",
                f"cat > /etc/git-fs/config.toml << 'HEREDOC'\n{config_content}\nHEREDOC",
            ]
        )

        # Review: Is there a point to this?
        exit_code, output = container.exec(["cat", "/etc/git-fs/config.toml"])
        assert exit_code == 0, f"Failed to write config: {output}"
        logger.info("Config written:\n%s", output.decode())

        # Review: We shouldn't need to create the mount point. git-fs should handle it.
        container.exec(["mkdir", "-p", MOUNT_POINT])
        container.exec(
            [
                "sh",
                "-c",
                "GIT_FS_LOG=debug nohup git-fs --config-path /etc/git-fs/config.toml run "
                "> /tmp/git-fs-stdout.log 2> /tmp/git-fs-stderr.log &",
            ]
        )

        _wait_for_mount(container)
        yield container

        # Diagnostic logs on teardown — DEBUG level so they only appear
        # in pytest output when a test fails (pytest captures and replays).
        _, stdout_log = container.exec(["cat", "/tmp/git-fs-stdout.log"])
        _, stderr_log = container.exec(["cat", "/tmp/git-fs-stderr.log"])
        logger.debug("git-fs stdout:\n%s", stdout_log.decode("utf-8", errors="replace"))
        logger.debug("git-fs stderr:\n%s", stderr_log.decode("utf-8", errors="replace"))


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
