"""Benchmark harness for gitfs."""

from __future__ import annotations

import importlib
import pkgutil
from typing import TYPE_CHECKING

from bench.fixture import (
    _REGISTRY,  # pyright: ignore[reportPrivateUsage]
    BenchConfig,
    BenchmarkResult,
    bench,
    bench_many,
    git_bench,
    mesafs_bench,
)

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = [
    "BenchConfig",
    "BenchmarkResult",
    "bench",
    "bench_many",
    "collect_benchmarks",
    "git_bench",
    "mesafs_bench",
    "run_benchmarks",
]


def collect_benchmarks() -> list[Callable[[BenchConfig], BenchmarkResult]]:
    """Import all submodules of the bench package and return registered benchmarks.

    Walking the package triggers ``@bench`` / ``@git_bench`` decorators,
    which append to the internal registry.  Returns a snapshot of the
    registry at time of call.
    """
    import bench as _pkg

    for info in pkgutil.walk_packages(_pkg.__path__, prefix=f"{_pkg.__name__}."):
        importlib.import_module(info.name)

    return list(_REGISTRY)


def run_benchmarks(config: BenchConfig) -> list[tuple[str, BenchmarkResult]]:
    """Collect and sequentially run all benchmarks.

    Returns a list of ``(name, result)`` tuples in execution order.
    """
    benchmarks = collect_benchmarks()
    return [(fn.__name__, fn(config)) for fn in benchmarks]
