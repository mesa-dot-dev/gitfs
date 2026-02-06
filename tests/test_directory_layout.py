"""Smoke test: verify container starts and git-fs mounts."""

import pytest


@pytest.mark.integration
def test_gitfs_mount_is_alive(gitfs_container):
    """Verify git-fs is running and the mount point is accessible."""
    exit_code, output = gitfs_container.exec(["ls", "/mnt/git-fs"])
    decoded = output.decode("utf-8", errors="replace").strip()
    assert exit_code == 0
    assert "github" in decoded
