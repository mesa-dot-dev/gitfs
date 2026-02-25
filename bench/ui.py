"""Live progress display for benchmark runs."""

from __future__ import annotations

import statistics
from typing import TYPE_CHECKING

from rich.console import Console
from rich.progress import BarColumn, Progress, TextColumn

if TYPE_CHECKING:
    from collections.abc import Callable


def run_with_live_progress(
    name: str,
    run_one: Callable[[], float],
    *,
    min_rounds: int,
    max_rounds: int,
    cv_threshold: float,
) -> list[float]:
    """Execute benchmark rounds with a live progress bar on stderr.

    *run_one* is called once per round and must return the elapsed time
    in seconds.  The loop runs adaptively: at least *min_rounds*, then
    continues until the coefficient of variation drops to *cv_threshold*
    or *max_rounds* is reached.

    Returns the collected timings.
    """
    console = Console(stderr=True)
    timings: list[float] = []

    progress = Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        TextColumn("{task.fields[stats]}"),
        console=console,
        transient=True,
    )

    with progress:
        task = progress.add_task(name, total=max_rounds, stats="starting…")

        for i in range(max_rounds):
            elapsed = run_one()
            timings.append(elapsed)
            n = i + 1

            mean = statistics.mean(timings)
            cv = (
                statistics.stdev(timings) / mean
                if n > 1 and mean > 0
                else float("inf")
            )

            converged = n >= min_rounds and cv <= cv_threshold

            if converged:
                status = "converged"
            elif n >= max_rounds:
                status = "max rounds"
            else:
                status = "running"

            stats = (
                f"round={n}/{max_rounds}"
                f"  last={elapsed * 1000:.2f}ms"
                f"  mean={mean * 1000:.2f}ms"
                f"  cv={cv:.2%}"
                f"  status={status}"
            )
            progress.update(task, completed=n, stats=stats)

            if converged:
                break

    return timings
