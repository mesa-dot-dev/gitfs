"""CLI entry point for the benchmark harness."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

import click

from bench import BenchConfig, BenchmarkResult, run_benchmarks
from bench.util import warm_sudo


def _to_github_action_benchmark(
    results: list[tuple[str, BenchmarkResult]],
) -> list[dict[str, Any]]:
    """Transform benchmark results to ``github-action-benchmark`` JSON format.

    Produces entries compatible with the ``customSmallerIsBetter`` format
    expected by ``benchmark-action/github-action-benchmark``.
    """
    return [
        {
            "name": name,
            "unit": "seconds",
            "value": round(result.median_s, 6),
            "range": str(round(result.stdev_s, 6)),
            "extra": (
                f"iterations: {result.iterations}\n"
                f"min: {result.min_s:.6f}s\n"
                f"max: {result.max_s:.6f}s\n"
                f"mean: {result.mean_s:.6f}s"
            ),
        }
        for name, result in results
    ]


@click.command()
@click.option(
    "--token",
    envvar="MESA_TEST_API_KEY",
    required=True,
    help="API token for depot.mesa.dev",
)
@click.option(
    "--mesafs-binary",
    envvar="MESAFS_BINARY",
    required=True,
    help="Path to the mesafs (git-fs) binary.",
)
@click.option(
    "--output-format",
    type=click.Choice(["default", "github-action-benchmark"]),
    default="default",
    help="Output format for benchmark results.",
)
def main(token: str, mesafs_binary: str, output_format: str) -> None:
    """Run all registered gitfs benchmarks and print JSON results."""
    warm_sudo()
    config = BenchConfig(token=token, mesafs_binary=mesafs_binary)
    results = run_benchmarks(config)

    if output_format == "github-action-benchmark":
        output: list[dict[str, Any]] = _to_github_action_benchmark(results)
    else:
        output = [{"name": name, **asdict(result)} for name, result in results]

    click.echo(json.dumps(output, indent=2))


if __name__ == "__main__":
    main()
