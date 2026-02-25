"""Tests for the flush_caches parameter of git_bench."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from bench.fixture import BenchConfig, _REGISTRY, git_bench


@pytest.fixture(autouse=True)
def _clean_registry():
    """Snapshot and restore the benchmark registry around each test."""
    snapshot = list(_REGISTRY)
    yield
    _REGISTRY.clear()
    _REGISTRY.extend(snapshot)


def test_git_bench_accepts_flush_caches_false():
    """git_bench() should accept flush_caches=False without error."""

    @git_bench("owner/repo", flush_caches=False)
    def my_bench(repo_path: Path) -> None:  # noqa: ARG001
        pass  # pragma: no cover


def test_git_bench_accepts_flush_caches_true():
    """git_bench() should accept flush_caches=True (explicit default)."""

    @git_bench("owner/repo2", flush_caches=True)
    def my_bench(repo_path: Path) -> None:  # noqa: ARG001
        pass  # pragma: no cover


def test_git_bench_default_flush_caches():
    """git_bench() should work without specifying flush_caches (default True)."""

    @git_bench("owner/repo3")
    def my_bench(repo_path: Path) -> None:  # noqa: ARG001
        pass  # pragma: no cover


def test_flush_caches_true_calls_flush(tmp_path: Path) -> None:
    """flush_disk_caches is called during prepare when flush_caches=True."""
    with (
        patch("bench.fixture.flush_disk_caches") as mock_flush,
        patch("bench.fixture.subprocess.run"),
        patch("bench.fixture.tempfile.TemporaryDirectory") as mock_tmpdir,
        patch("bench.fixture.run_with_live_progress") as mock_progress,
    ):
        mock_tmpdir.return_value.__enter__ = MagicMock(return_value=str(tmp_path))
        mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)

        def call_run_one(
            name: str,  # noqa: ARG001
            run_one: object,
            **kwargs: object,  # noqa: ARG001
        ) -> list[float]:
            # stdev requires >= 2 data points
            return [run_one(), run_one()]  # type: ignore[operator]

        mock_progress.side_effect = call_run_one

        @git_bench("owner/flush-true-behavioral", flush_caches=True)
        def my_bench(repo_path: Path) -> None:  # noqa: ARG001
            pass

        my_bench(BenchConfig(token="fake", mesafs_binary="/fake/bin"))
        mock_flush.assert_called()


def test_flush_caches_false_skips_flush(tmp_path: Path) -> None:
    """flush_disk_caches is NOT called during prepare when flush_caches=False."""
    with (
        patch("bench.fixture.flush_disk_caches") as mock_flush,
        patch("bench.fixture.subprocess.run"),
        patch("bench.fixture.tempfile.TemporaryDirectory") as mock_tmpdir,
        patch("bench.fixture.run_with_live_progress") as mock_progress,
    ):
        mock_tmpdir.return_value.__enter__ = MagicMock(return_value=str(tmp_path))
        mock_tmpdir.return_value.__exit__ = MagicMock(return_value=False)

        def call_run_one(
            name: str,  # noqa: ARG001
            run_one: object,
            **kwargs: object,  # noqa: ARG001
        ) -> list[float]:
            # stdev requires >= 2 data points
            return [run_one(), run_one()]  # type: ignore[operator]

        mock_progress.side_effect = call_run_one

        @git_bench("owner/flush-false-behavioral", flush_caches=False)
        def my_bench(repo_path: Path) -> None:  # noqa: ARG001
            pass

        my_bench(BenchConfig(token="fake", mesafs_binary="/fake/bin"))
        mock_flush.assert_not_called()
