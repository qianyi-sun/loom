"""Shared subprocess helpers for rollout steps (#340).

Every concrete step that shells out (git, docker, kubectl, ssh, or the
existing loom cluster subcommands) uses these helpers so that:

* stdout / stderr always land in the step's evidence dir (partial or
  full — even a killed subprocess leaves visible logs).
* exit codes translate consistently to :class:`RunResult`.
* subprocess.run() is stubbed at one point for tests, not sprinkled
  across every step's implementation.

Kept separate from :mod:`steps.base` so steps can import selectively.
"""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class SubprocessResult:
    """Result of a captured subprocess run."""

    argv: list[str]
    returncode: int
    stdout: str
    stderr: str


def run_captured(
    argv: Sequence[str],
    *,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout_sec: float | None = None,
) -> SubprocessResult:
    """Run ``argv``, capture stdout/stderr, optionally tee to log files.

    Every step should call this rather than :func:`subprocess.run`
    directly so that (1) evidence log files are consistently populated
    and (2) tests can monkeypatch at one call site.
    """
    proc = subprocess.run(
        list(argv),
        capture_output=True,
        text=True,
        check=False,
        cwd=str(cwd) if cwd else None,
        env=env,
        timeout=timeout_sec,
    )
    if stdout_log is not None:
        stdout_log.write_text(proc.stdout)
    if stderr_log is not None:
        stderr_log.write_text(proc.stderr)
    return SubprocessResult(
        argv=list(argv),
        returncode=proc.returncode,
        stdout=proc.stdout,
        stderr=proc.stderr,
    )


def format_command(argv: Sequence[str]) -> str:
    """Human-readable rendering of a command; shell-quote each arg."""
    return " ".join(shlex.quote(a) for a in argv)
