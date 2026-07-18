#!/usr/bin/env python3
"""Converge the fixed platform-dev data filesystem's ext4 root reserve."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

DATA_PATH = Path("/data")
DATA_DEVICE = Path("/dev/nvme0n1p2")
DATA_MOUNT = Path("/")
FILESYSTEM_TYPE = "ext4"
TARGET_RESERVED_PERCENT = 3.0
MAX_INITIAL_RESERVED_PERCENT = 5.01
LOCK_PATH = Path("/run/loom-staging-data-reserve.lock")
MIN_FILESYSTEM_BYTES = 4 * 1024**4
_ROOT_ENV = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
}


class ReserveError(RuntimeError):
    """A bounded, non-sensitive data-reserve contract failure."""


@dataclass(frozen=True, slots=True)
class CommandResult:
    returncode: int
    stdout: str
    stderr: str


class Runner(Protocol):
    def run(self, argv: Sequence[str]) -> CommandResult: ...


class SubprocessRunner:
    def run(self, argv: Sequence[str]) -> CommandResult:
        completed = subprocess.run(
            list(argv),
            check=False,
            capture_output=True,
            text=True,
            env=_ROOT_ENV,
        )
        return CommandResult(completed.returncode, completed.stdout, completed.stderr)


@dataclass(frozen=True, slots=True)
class Ext4Identity:
    block_count: int
    reserved_block_count: int
    block_size: int
    reserved_uid: int
    reserved_gid: int

    @property
    def size_bytes(self) -> int:
        return self.block_count * self.block_size

    @property
    def reserved_percent(self) -> float:
        return self.reserved_block_count * 100.0 / self.block_count

    @property
    def reserved_bytes(self) -> int:
        return self.reserved_block_count * self.block_size


def _run(runner: Runner, argv: Sequence[str]) -> str:
    result = runner.run(argv)
    if result.returncode != 0:
        raise ReserveError(f"{Path(argv[0]).name} failed safely")
    return result.stdout


def _parse_findmnt(payload: str) -> None:
    try:
        report = json.loads(payload)
        filesystems = report["filesystems"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ReserveError("data filesystem identity is invalid") from exc
    if not isinstance(filesystems, list) or len(filesystems) != 1:
        raise ReserveError("data filesystem identity is invalid")
    row = filesystems[0]
    if not isinstance(row, dict):
        raise ReserveError("data filesystem identity is invalid")
    options = row.get("options")
    option_set = set(options.split(",")) if isinstance(options, str) else set()
    if (
        row.get("target") != str(DATA_MOUNT)
        or row.get("source") != str(DATA_DEVICE)
        or row.get("fstype") != FILESYSTEM_TYPE
        or "rw" not in option_set
    ):
        raise ReserveError("data filesystem identity is invalid")


_TUNE_FIELD = re.compile(r"^([^:]+):\s*(.*?)\s*$")


def _parse_tune2fs(payload: str) -> Ext4Identity:
    fields: dict[str, str] = {}
    for line in payload.splitlines():
        match = _TUNE_FIELD.fullmatch(line)
        if match is not None:
            fields[match.group(1)] = match.group(2)
    try:
        identity = Ext4Identity(
            block_count=int(fields["Block count"]),
            reserved_block_count=int(fields["Reserved block count"]),
            block_size=int(fields["Block size"]),
            reserved_uid=int(fields["Reserved blocks uid"].split()[0]),
            reserved_gid=int(fields["Reserved blocks gid"].split()[0]),
        )
    except (KeyError, ValueError) as exc:
        raise ReserveError("ext4 reserve metadata is invalid") from exc
    if (
        identity.block_count <= 0
        or identity.block_size not in {1024, 2048, 4096}
        or identity.reserved_block_count < 0
        or identity.reserved_block_count > identity.block_count
        or identity.reserved_uid != 0
        or identity.reserved_gid != 0
        or identity.size_bytes < MIN_FILESYSTEM_BYTES
    ):
        raise ReserveError("ext4 reserve metadata is invalid")
    return identity


def _parse_device_stat(payload: str) -> None:
    fields = payload.strip().split("|")
    try:
        mode = int(fields[0], 16)
    except (IndexError, ValueError) as exc:
        raise ReserveError("data block device identity is invalid") from exc
    if len(fields) != 3 or not stat.S_ISBLK(mode) or fields[1:] != ["0", "0"]:
        raise ReserveError("data block device identity is invalid")


def inspect(runner: Runner) -> Ext4Identity:
    _parse_findmnt(
        _run(
            runner,
            (
                "/usr/bin/findmnt",
                "--json",
                "--target",
                str(DATA_PATH),
                "--output",
                "TARGET,SOURCE,FSTYPE,OPTIONS",
            ),
        )
    )
    _parse_device_stat(
        _run(
            runner,
            (
                "/usr/bin/stat",
                "--format=%f|%u|%g",
                str(DATA_DEVICE),
            ),
        )
    )
    return _parse_tune2fs(_run(runner, ("/usr/sbin/tune2fs", "-l", str(DATA_DEVICE))))


def _report(identity: Ext4Identity, *, changed: bool) -> dict[str, object]:
    return {
        "schema_version": 1,
        "ok": abs(identity.reserved_percent - TARGET_RESERVED_PERCENT) <= 0.01,
        "changed": changed,
        "data_path": str(DATA_PATH),
        "device": str(DATA_DEVICE),
        "filesystem_type": FILESYSTEM_TYPE,
        "filesystem_bytes": identity.size_bytes,
        "reserved_block_count": identity.reserved_block_count,
        "reserved_bytes": identity.reserved_bytes,
        "reserved_percent": round(identity.reserved_percent, 6),
        "target_reserved_percent": TARGET_RESERVED_PERCENT,
    }


def check(runner: Runner) -> dict[str, object]:
    identity = inspect(runner)
    report = _report(identity, changed=False)
    if not report["ok"]:
        raise ReserveError("data filesystem root reserve is not converged")
    return report


def _ensure_no_active_rollout(runner: Runner) -> None:
    active = _run(
        runner,
        (
            "/usr/bin/systemctl",
            "list-units",
            "--type=service",
            "--state=activating,active",
            "--no-legend",
            "--plain",
            "loom-staging-rollout-*.service",
        ),
    )
    if active.strip():
        raise ReserveError("data reserve install refuses an active rollout unit")


def install(runner: Runner, *, euid: int, lock_path: Path = LOCK_PATH) -> dict[str, object]:
    if euid != 0:
        raise ReserveError("data reserve install requires root")
    _ensure_no_active_rollout(runner)
    lock_path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_handle:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        before = inspect(runner)
        if abs(before.reserved_percent - TARGET_RESERVED_PERCENT) <= 0.01:
            return _report(before, changed=False)
        if not (TARGET_RESERVED_PERCENT < before.reserved_percent <= MAX_INITIAL_RESERVED_PERCENT):
            raise ReserveError("data filesystem root reserve is outside the supported transition")
        result = runner.run(
            (
                "/usr/sbin/tune2fs",
                "-m",
                f"{TARGET_RESERVED_PERCENT:g}",
                str(DATA_DEVICE),
            )
        )
        if result.returncode != 0:
            raise ReserveError("tune2fs failed safely")
        try:
            after = inspect(runner)
            report = _report(after, changed=True)
            if not report["ok"]:
                raise ReserveError("data filesystem root reserve readback failed")
            report.update(
                {
                    "previous_reserved_block_count": before.reserved_block_count,
                    "previous_reserved_bytes": before.reserved_bytes,
                    "previous_reserved_percent": round(before.reserved_percent, 6),
                    "released_bytes": before.reserved_bytes - after.reserved_bytes,
                }
            )
            return report
        except ReserveError as exc:
            rollback = runner.run(
                (
                    "/usr/sbin/tune2fs",
                    "-r",
                    str(before.reserved_block_count),
                    str(DATA_DEVICE),
                )
            )
            if rollback.returncode != 0:
                raise ReserveError("data reserve rollback failed") from exc
            restored = inspect(runner)
            if restored.reserved_block_count != before.reserved_block_count:
                raise ReserveError("data reserve rollback readback failed") from exc
            raise ReserveError("data filesystem root reserve change rolled back") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="staging_rollout_data_reserve.py")
    parser.add_argument("command", choices=("check", "install"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    runner = SubprocessRunner()
    try:
        result = check(runner) if args.command == "check" else install(runner, euid=os.geteuid())
    except ReserveError as exc:
        sys.stderr.write(json.dumps({"error": str(exc)}, sort_keys=True) + "\n")
        return 1
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
