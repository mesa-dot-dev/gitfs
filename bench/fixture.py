"""Benchmark decorator and result type."""

from __future__ import annotations

import contextlib
import functools
import os
import shutil
import statistics
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from bench.ui import run_with_live_progress
from bench.util import flush_disk_caches

if TYPE_CHECKING:
    from collections.abc import Callable, Generator


@dataclass(frozen=True)
class BenchConfig:
    """Configuration passed to every benchmark at execution time."""

    token: str
    mesafs_binary: str


@dataclass(frozen=True)
class BenchmarkResult:
    """Aggregated timing statistics from a benchmark run."""

    iterations: int
    min_s: float
    max_s: float
    mean_s: float
    median_s: float
    stdev_s: float


_REGISTRY: list[Callable[[BenchConfig], BenchmarkResult]] = []


def bench(
    prepare: Callable[..., Generator[Any]],
    *,
    setup: Callable[[BenchConfig], Generator[Any]] | None = None,
    label: str | None = None,
    min_rounds: int = 5,
    max_rounds: int = 100,
    cv_threshold: float = 0.05,
) -> Callable[..., Any]:
    """Register a benchmark whose setup is provided by *prepare*.

    ``prepare`` receives a :class:`BenchConfig` and returns a generator
    (context-manager style).  It is entered before each iteration;
    the yielded value is passed as the first positional argument.
    Cleanup happens after each iteration completes (or raises).

    When *setup* is provided it must also be a single-yield generator
    accepting a :class:`BenchConfig`.  It is entered **once** for the
    entire benchmark run (before the first iteration) and its yielded
    value is forwarded to *prepare* as a second argument.  Cleanup
    runs after all iterations complete (or on error).  Use *setup* for
    expensive one-time work (e.g. cloning a repository) and *prepare*
    for cheap per-iteration work (e.g. flushing disk caches).

    The decorated function's signature becomes ``fn(ctx) -> None``.
    Timing is measured by the harness.  Iterations continue until either
    the coefficient of variation (stdev / mean) drops below
    *cv_threshold*, or *max_rounds* is reached — whichever comes first.
    At least *min_rounds* iterations are always executed.
    """
    _min_for_stdev = 2
    if min_rounds < _min_for_stdev:
        msg = f"min_rounds must be >= {_min_for_stdev}, got {min_rounds}"
        raise ValueError(msg)
    if max_rounds < min_rounds:
        msg = f"max_rounds ({max_rounds}) must be >= min_rounds ({min_rounds})"
        raise ValueError(msg)

    resolved_label = label if label is not None else prepare.__qualname__

    def decorator(
        fn: Callable[..., None],
    ) -> Callable[[BenchConfig], BenchmarkResult]:
        @functools.wraps(fn)
        def wrapper(config: BenchConfig) -> BenchmarkResult:
            setup_gen = None
            setup_ctx = None
            if setup is not None:
                setup_gen = setup(config)
                setup_ctx = next(setup_gen)

            try:
                def run_one() -> float:
                    gen = (
                        prepare(config, setup_ctx)
                        if setup is not None
                        else prepare(config)
                    )
                    ctx = next(gen)
                    try:
                        t0 = time.perf_counter()
                        fn(ctx)
                        return time.perf_counter() - t0
                    finally:
                        with contextlib.suppress(StopIteration):
                            next(gen)

                timings = run_with_live_progress(
                    wrapper.__name__,
                    run_one,
                    min_rounds=min_rounds,
                    max_rounds=max_rounds,
                    cv_threshold=cv_threshold,
                )

                return BenchmarkResult(
                    iterations=len(timings),
                    min_s=min(timings),
                    max_s=max(timings),
                    mean_s=statistics.mean(timings),
                    median_s=statistics.median(timings),
                    stdev_s=statistics.stdev(timings),
                )
            finally:
                if setup_gen is not None:
                    with contextlib.suppress(StopIteration):
                        next(setup_gen)

        wrapper.__name__ = f"{fn.__name__}[{resolved_label}]"
        existing = {w.__name__ for w in _REGISTRY}
        if wrapper.__name__ in existing:
            msg = f"duplicate benchmark name: {wrapper.__name__}"
            raise ValueError(msg)
        _REGISTRY.append(wrapper)
        return wrapper

    return decorator


def git_bench(
    repo: str,
    *,
    flush_caches: bool = True,
    min_rounds: int = 5,
    max_rounds: int = 100,
    cv_threshold: float = 0.05,
) -> Callable[..., Any]:
    """Clone *repo* once and pass the checkout path to each iteration.

    The clone happens in ``setup`` (once per benchmark).  Each iteration's
    ``prepare`` only flushes disk caches (when enabled) so that every round
    benchmarks against cold storage without re-cloning.

    Usage::

        @git_bench("mesa-dot-dev/gitfs")
        def my_benchmark(repo_path: Path) -> None:
            ...

    When *flush_caches* is ``True`` (the default), kernel disk caches are
    dropped before each iteration so that the benchmark body measures
    cold-storage performance.  Set to ``False`` to benchmark with warm caches.
    """

    def setup(config: BenchConfig) -> Generator[Path]:
        url = f"https://{config.token}@depot.mesa.dev/{repo}.git"
        with tempfile.TemporaryDirectory(
            prefix=f"bench-{repo.replace('/', '-')}-",
            ignore_cleanup_errors=True,
        ) as tmp:
            dest = Path(tmp) / repo.rsplit("/", maxsplit=1)[-1]
            subprocess.run(
                ["git", "clone", url, str(dest)],
                check=True,
                capture_output=True,
            )
            yield dest

    def prepare(config: BenchConfig, setup_ctx: Path) -> Generator[Path]:
        if flush_caches:
            flush_disk_caches()
        yield setup_ctx

    return bench(
        prepare=prepare,
        setup=setup,
        label=f"gitbench:{repo}",
        min_rounds=min_rounds,
        max_rounds=max_rounds,
        cv_threshold=cv_threshold,
    )


_MESAFS_READY_POLL_INTERVAL = 0.5
_MESAFS_READY_TIMEOUT = 30


def mesafs_bench(
    repo: str,
    *,
    flush_caches: bool = True,
    min_rounds: int = 5,
    max_rounds: int = 100,
    cv_threshold: float = 0.05,
) -> Callable[..., Any]:
    """Mount *repo* via mesafs and pass the mount path to the body.

    Convenience wrapper around :func:`bench` that spawns the mesafs binary,
    waits for the FUSE mount to become ready, and yields the repository
    path inside the mount.

    When *flush_caches* is ``True`` (the default), kernel disk caches are
    dropped after the mount is ready so that the benchmark body measures
    cold-storage performance.  Set to ``False`` to benchmark with warm caches.

    Usage::

        @mesafs_bench("mesa-ci/planventure")
        def my_benchmark(mount_path: Path) -> None:
            ...
    """

    def prepare(config: BenchConfig) -> Generator[Path]:
        org_name, repo_name = repo.rsplit("/", maxsplit=1)

        base_dir = Path(tempfile.gettempdir()) / f"bench-mesafs-{repo.replace('/', '-')}"
        if base_dir.exists():
            shutil.rmtree(base_dir)
        base_dir.mkdir()
        mount_path = Path(base_dir) / "mnt"
        mount_path.mkdir()
        cache_path = Path(base_dir) / "cache"
        cache_path.mkdir()
        pid_path = Path(base_dir) / "git-fs.pid"

        config_content = (
            f'mount-point = "{mount_path}"\n'
            f"uid = {os.getuid()}\n"
            f"gid = {os.getgid()}\n"
            f"\n"
            f"[cache]\n"
            f'path = "{cache_path}"\n'
            f"\n"
            f"[daemon]\n"
            f'pid-file = "{pid_path}"\n'
            f"\n"
            f"[organizations.{org_name}]\n"
            f'api-key = "{config.token}"\n'
        )
        config_file = Path(base_dir) / "config.toml"
        config_file.write_text(config_content)

        log_path = Path(base_dir) / "mesafs.log"
        log_file = log_path.open("w")
        proc = subprocess.Popen(
            [config.mesafs_binary, "--config-path", str(config_file), "run"],
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )

        try:
            repo_path = mount_path / org_name / repo_name
            deadline = time.monotonic() + _MESAFS_READY_TIMEOUT
            while time.monotonic() < deadline:
                if repo_path.is_dir():
                    break
                if proc.poll() is not None:
                    log_file.close()
                    logs = log_path.read_text()
                    msg = (
                        f"mesafs exited early with code {proc.returncode}\n"
                        f"--- mesafs logs ---\n{logs}"
                    )
                    raise RuntimeError(msg)
                time.sleep(_MESAFS_READY_POLL_INTERVAL)
            else:
                log_file.close()
                logs = log_path.read_text()
                msg = (
                    f"mesafs mount not ready within {_MESAFS_READY_TIMEOUT}s\n"
                    f"--- mesafs logs ---\n{logs}"
                )
                raise TimeoutError(msg)

            if flush_caches:
                flush_disk_caches()
            yield repo_path
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            log_file.close()
            shutil.rmtree(base_dir, ignore_errors=True)

    return bench(
        prepare=prepare,
        label=f"mesafs:{repo}",
        min_rounds=min_rounds,
        max_rounds=max_rounds,
        cv_threshold=cv_threshold,
    )


def bench_many(
    decorators: list[Callable[..., Any]],
) -> Callable[..., Any]:
    """Apply multiple bench decorators to a single function body.

    Each decorator in *decorators* is applied independently, registering
    a separate benchmark with its own label.  The decorated function
    itself is returned unchanged (the last decorator's wrapper wins as
    the return value, but all are registered in the global registry).

    Usage::

        @bench_many([
            git_bench("mesa-ci/planventure"),
            git_bench("foo/bar"),
        ])
        def bench_list_root(repo_path: Path) -> None:
            list(repo_path.iterdir())
    """

    def outer(fn: Callable[..., Any]) -> Callable[..., Any]:
        result = fn
        for dec in decorators:
            result = dec(fn)
        return result

    return outer
