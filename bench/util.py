"""Benchmark utilities."""

from __future__ import annotations

import os
import platform
import subprocess
import time


def warm_sudo() -> None:
    """Validate sudo credentials so later sudo calls are non-interactive.

    Runs ``sudo -v`` which prompts for the password (if needed) and
    refreshes the credential cache.  Call this once before entering
    a ``rich.live.Live`` context so the prompt is visible.
    """
    if os.geteuid() != 0:
        subprocess.run(["sudo", "-v"], check=True)


def flush_disk_caches() -> None:
    """Drop kernel disk caches so benchmarks hit cold storage.

    On macOS this runs ``sudo purge``.  On Linux this runs
    ``sync`` followed by ``echo 3 > /proc/sys/vm/drop_caches``.

    If the current process is not running as root, ``sudo`` is used
    and may prompt the user for their password.
    """
    system = platform.system()

    match system:
        case "Darwin":
            cmd = ["purge"]
            if os.geteuid() != 0:
                cmd = ["sudo", *cmd]
            subprocess.run(cmd, check=True)

        case "Linux":
            subprocess.run(["sync"], check=True)
            cmd = ["sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"]
            if os.geteuid() != 0:
                cmd = ["sudo", *cmd]
            subprocess.run(cmd, check=True)

        case _:
            msg = f"flush_disk_caches is not supported on {system}"
            raise NotImplementedError(msg)

    # Add a little bit of delay to let the system stabilize
    time.sleep(0.3)
