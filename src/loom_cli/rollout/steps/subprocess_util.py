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

from loom_cli.rollout.operator.redaction import redact_rollout_text


@dataclass(frozen=True, slots=True)
class SubprocessResult:
    """Result of a captured subprocess run."""

    argv: list[str]
    returncode: int
    stdout: str
    stderr: str


class SubprocessExecutionError(RuntimeError):
    """A subprocess launch/timeout failure whose message is safe to persist."""


def _output_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


def _write_diagnostic(path: Path | None, text: str) -> None:
    if path is not None:
        path.write_text(redact_rollout_text(text), encoding="utf-8")


def run_captured(
    argv: Sequence[str],
    *,
    stdin_text: str | None = None,
    stdout_log: Path | None = None,
    stderr_log: Path | None = None,
    cwd: Path | None = None,
    env: dict[str, str] | None = None,
    timeout_sec: float | None = None,
    sanitize_return: bool = False,
) -> SubprocessResult:
    """Run ``argv``, capture output, and optionally provide UTF-8 text stdin.

    Every step should call this rather than :func:`subprocess.run`
    directly so that (1) evidence log files are consistently populated
    and (2) tests can monkeypatch at one call site.
    """
    command = list(argv)
    try:
        if stdin_text is None:
            proc = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=False,
                cwd=str(cwd) if cwd else None,
                env=env,
                timeout=timeout_sec,
            )
        else:
            proc = subprocess.run(
                command,
                input=stdin_text,
                capture_output=True,
                text=True,
                encoding="utf-8",
                check=False,
                cwd=str(cwd) if cwd else None,
                env=env,
                timeout=timeout_sec,
            )
    except subprocess.TimeoutExpired as exc:
        stdout = _output_text(exc.stdout)
        stderr = _output_text(exc.stderr)
        _write_diagnostic(stdout_log, stdout)
        _write_diagnostic(stderr_log, stderr)
        rendered_timeout = timeout_sec if timeout_sec is not None else exc.timeout
        raise SubprocessExecutionError(
            f"command timed out after {rendered_timeout}s: {format_command(command)}"
        ) from None
    except OSError as exc:
        safe_error = redact_rollout_text(str(exc), limit=1000)
        _write_diagnostic(stdout_log, "")
        _write_diagnostic(stderr_log, safe_error + "\n")
        raise SubprocessExecutionError(
            f"failed to launch {format_command(command)}: {safe_error}"
        ) from None

    raw_stdout = _output_text(proc.stdout)
    raw_stderr = _output_text(proc.stderr)
    _write_diagnostic(stdout_log, raw_stdout)
    _write_diagnostic(stderr_log, raw_stderr)
    stdout = redact_rollout_text(raw_stdout) if sanitize_return else raw_stdout
    stderr = redact_rollout_text(raw_stderr) if sanitize_return else raw_stderr
    return SubprocessResult(
        argv=[redact_rollout_text(value) for value in command],
        returncode=proc.returncode,
        stdout=stdout,
        stderr=stderr,
    )


def format_command(argv: Sequence[str]) -> str:
    """Human-readable rendering of a command; shell-quote each arg."""
    return redact_rollout_text(" ".join(shlex.quote(a) for a in argv))
