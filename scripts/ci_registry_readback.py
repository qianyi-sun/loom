#!/usr/bin/env python3
"""Fail-closed, bounded OCI registry readback for trusted image publication."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Sequence
from pathlib import Path

_DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}")
_REFERENCE_RE = re.compile(
    r"ghcr\.io/[a-z0-9](?:[a-z0-9._-]{0,127})/"
    r"[a-z0-9](?:[a-z0-9._/-]{0,254})"
    r"(?::[A-Za-z0-9_][A-Za-z0-9._-]{0,127}|@sha256:[0-9a-f]{64})"
)
_DIGEST_LINE_RE = re.compile(r"^Digest:\s+(sha256:[0-9a-f]{64})\s*$", re.MULTILINE)


class RegistryReadbackError(RuntimeError):
    """Raised when an immutable registry readback cannot be proven."""


Runner = Callable[[Sequence[str]], subprocess.CompletedProcess[bytes]]
Sleeper = Callable[[float], None]


def _run(command: Sequence[str]) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, check=False, capture_output=True)


def _validate(reference: str, expected_digest: str, attempts: int, delay_seconds: float) -> None:
    if _REFERENCE_RE.fullmatch(reference) is None:
        raise RegistryReadbackError("registry reference is outside the closed GHCR grammar")
    if _DIGEST_RE.fullmatch(expected_digest) is None:
        raise RegistryReadbackError("expected digest must be lowercase sha256")
    if type(attempts) is not int or not 1 <= attempts <= 10:
        raise RegistryReadbackError("attempts must be in 1..10")
    if not 0 <= delay_seconds <= 30:
        raise RegistryReadbackError("delay seconds must be in 0..30")


def _bounded_error(payload: bytes) -> str:
    text = payload.decode("utf-8", errors="replace").replace("\x00", "")
    return text[-2048:]


def read_digest(
    *,
    reference: str,
    expected_digest: str,
    attempts: int,
    delay_seconds: float,
    runner: Runner = _run,
    sleeper: Sleeper = time.sleep,
) -> str:
    """Return the exact expected digest after a bounded registry readback."""

    _validate(reference, expected_digest, attempts, delay_seconds)
    observations: list[str] = []
    for attempt in range(1, attempts + 1):
        completed = runner(("docker", "buildx", "imagetools", "inspect", reference))
        stdout = completed.stdout.decode("utf-8", errors="replace")
        matches = _DIGEST_LINE_RE.findall(stdout)
        observed = matches[0] if completed.returncode == 0 and len(matches) == 1 else ""
        if observed == expected_digest:
            return observed
        observations.append(
            f"attempt={attempt} rc={completed.returncode} observed={observed or 'none'} "
            f"stderr={_bounded_error(completed.stderr)!r}"
        )
        if attempt < attempts:
            sleeper(delay_seconds)
    raise RegistryReadbackError(
        "registry digest did not converge to the expected value; " + "; ".join(observations)
    )


def read_raw(
    *,
    reference: str,
    expected_digest: str,
    output: Path,
    attempts: int,
    delay_seconds: float,
    runner: Runner = _run,
    sleeper: Sleeper = time.sleep,
) -> Path:
    """Atomically persist raw manifest bytes only after their digest matches."""

    _validate(reference, expected_digest, attempts, delay_seconds)
    if not output.is_absolute() or output.name in {"", ".", ".."}:
        raise RegistryReadbackError("output must be an absolute file path")
    observations: list[str] = []
    for attempt in range(1, attempts + 1):
        completed = runner(
            ("docker", "buildx", "imagetools", "inspect", "--raw", reference)
        )
        observed = "sha256:" + hashlib.sha256(completed.stdout).hexdigest()
        if completed.returncode == 0 and completed.stdout and observed == expected_digest:
            output.parent.mkdir(parents=True, exist_ok=True)
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f".{output.name}.", suffix=".tmp", dir=output.parent
            )
            temporary = Path(temporary_name)
            try:
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(completed.stdout)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, output)
                directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                temporary.unlink(missing_ok=True)
            return output
        observations.append(
            f"attempt={attempt} rc={completed.returncode} observed={observed} "
            f"bytes={len(completed.stdout)} stderr={_bounded_error(completed.stderr)!r}"
        )
        if attempt < attempts:
            sleeper(delay_seconds)
    raise RegistryReadbackError(
        "raw registry manifest did not converge to the expected bytes; "
        + "; ".join(observations)
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("digest", "raw"):
        child = subparsers.add_parser(name)
        child.add_argument("--reference", required=True)
        child.add_argument("--expected-digest", required=True)
        child.add_argument("--attempts", type=int, default=6)
        child.add_argument("--delay-seconds", type=float, default=2.0)
        if name == "raw":
            child.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "digest":
            print(
                read_digest(
                    reference=args.reference,
                    expected_digest=args.expected_digest,
                    attempts=args.attempts,
                    delay_seconds=args.delay_seconds,
                )
            )
        else:
            read_raw(
                reference=args.reference,
                expected_digest=args.expected_digest,
                output=args.output,
                attempts=args.attempts,
                delay_seconds=args.delay_seconds,
            )
    except RegistryReadbackError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
