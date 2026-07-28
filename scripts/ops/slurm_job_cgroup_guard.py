#!/usr/bin/env python3
"""Continuously apply the reviewed PID ceiling to opted-in Slurm job cgroups.

Slurm 23.11 runs the administrator Prolog before it creates the contained
extern step, so a Prolog cannot safely mutate the eventual job cgroup. This
root service instead observes only exact ``job_<id>`` cgroups, validates the
closed Loom job comment and account through Slurm, then lowers ``pids.max``.
The batch entry waits for the exact readback before it starts Docker.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import subprocess
import sys
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

_JOB_DIR_RE = re.compile(r"^job_([1-9][0-9]*)$")
_ACCOUNT_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_COMMENT_RE = re.compile(r"^loom-cgroup-v1:pids=([1-9][0-9]{0,8})$")
_REQUIRED_CONTROLLERS = frozenset({"cpu", "memory", "pids"})
_MAX_WALKED_DIRECTORIES = 100_000
_MAX_JOB_RECORD_CACHE = 10_000


class GuardError(RuntimeError):
    """The guard cannot safely apply one requested boundary."""


@dataclass(frozen=True, slots=True)
class GuardConfig:
    cluster: str
    pids_max: int
    allowed_accounts: frozenset[str]
    poll_interval_seconds: float


@dataclass(frozen=True, slots=True)
class JobRecord:
    job_id: str
    account: str
    comment: str


JobLookup = Callable[[str], JobRecord]


class BoundedJobLookup:
    """Cache immutable Slurm job identity without unbounded daemon growth."""

    def __init__(self, lookup: JobLookup | None = None) -> None:
        self._lookup = lookup or _job_record
        self._records: OrderedDict[str, JobRecord] = OrderedDict()

    def __call__(self, job_id: str) -> JobRecord:
        record = self._records.get(job_id)
        if record is not None:
            self._records.move_to_end(job_id)
            return record
        record = self._lookup(job_id)
        self._records[job_id] = record
        if len(self._records) > _MAX_JOB_RECORD_CACHE:
            self._records.popitem(last=False)
        return record

    def retain(self, job_ids: set[str]) -> None:
        for job_id in tuple(self._records):
            if job_id not in job_ids:
                del self._records[job_id]


def load_config(path: Path) -> GuardConfig:
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise GuardError("guard config is unavailable or invalid") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o022
        or not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "cluster",
            "pids_max",
            "allowed_accounts",
            "poll_interval_seconds",
        }
        or payload.get("schema_version") != 1
    ):
        raise GuardError("guard config does not match the closed schema")
    cluster = payload.get("cluster")
    pids_max = payload.get("pids_max")
    accounts = payload.get("allowed_accounts")
    interval = payload.get("poll_interval_seconds")
    if not isinstance(cluster, str) or not cluster or any(char.isspace() for char in cluster):
        raise GuardError("guard cluster is invalid")
    if type(pids_max) is not int or not 1 <= pids_max <= 100_000_000:
        raise GuardError("guard pids_max is invalid")
    if (
        not isinstance(accounts, list)
        or len(accounts) != 3
        or len(accounts) != len(set(accounts))
        or not all(isinstance(item, str) and _ACCOUNT_RE.fullmatch(item) for item in accounts)
    ):
        raise GuardError("guard accounts are invalid")
    if (
        isinstance(interval, bool)
        or not isinstance(interval, (int, float))
        or not 0.05 <= float(interval) <= 5.0
    ):
        raise GuardError("guard poll interval is invalid")
    return GuardConfig(
        cluster=cluster,
        pids_max=pids_max,
        allowed_accounts=frozenset(accounts),
        poll_interval_seconds=float(interval),
    )


def _slurm_marker_before(parts: tuple[str, ...], index: int) -> bool:
    return any(
        part == "slurm" or part == "slurmstepd.scope" or part.endswith("_slurmstepd.scope")
        for part in parts[:index]
    )


def discover_job_cgroups(cgroup_root: Path) -> tuple[tuple[str, Path], ...]:
    try:
        root = cgroup_root.resolve(strict=True)
    except OSError as exc:
        raise GuardError("cgroup v2 root is unavailable") from exc
    found: list[tuple[str, Path]] = []
    walked = 0
    for raw_root, directories, _files in os.walk(root, followlinks=False):
        walked += 1
        if walked > _MAX_WALKED_DIRECTORIES:
            raise GuardError("cgroup directory walk exceeded its bound")
        current = Path(raw_root)
        safe_directories: list[str] = []
        for name in directories:
            child = current / name
            try:
                metadata = child.lstat()
            except OSError:
                continue
            if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                safe_directories.append(name)
        directories[:] = safe_directories
        match = _JOB_DIR_RE.fullmatch(current.name)
        if match is None:
            continue
        relative_parts = current.relative_to(root).parts
        if not _slurm_marker_before(relative_parts, len(relative_parts) - 1):
            continue
        try:
            resolved = current.resolve(strict=True)
            resolved.relative_to(root)
        except (OSError, ValueError):
            continue
        if resolved != current:
            continue
        found.append((match.group(1), current))
        directories[:] = []
    found.sort(key=lambda item: (int(item[0]), str(item[1])))
    return tuple(found)


def _job_record(job_id: str) -> JobRecord:
    completed = subprocess.run(
        ("/usr/bin/scontrol", "show", "job", "--oneliner", job_id),
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if completed.returncode:
        raise GuardError("could not read the Slurm job record")
    values: dict[str, list[str]] = {"JobId": [], "Account": [], "Comment": []}
    for field in values:
        values[field] = re.findall(rf"(?:^|\s){field}=(\S+)", completed.stdout)
    if any(len(items) != 1 for items in values.values()):
        raise GuardError("Slurm job identity fields are ambiguous")
    if values["JobId"][0] != job_id:
        raise GuardError("Slurm job record does not match the cgroup")
    return JobRecord(
        job_id=job_id,
        account=values["Account"][0],
        comment=values["Comment"][0],
    )


def _cluster_name() -> str:
    completed = subprocess.run(
        ("/usr/bin/scontrol", "show", "config"),
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if completed.returncode:
        raise GuardError("could not read the local Slurm cluster")
    matches = re.findall(r"(?m)^\s*ClusterName\s*=\s*(\S+)\s*$", completed.stdout)
    if len(matches) != 1:
        raise GuardError("local Slurm cluster identity is ambiguous")
    return matches[0]


def apply_job_limit(
    job_path: Path,
    *,
    record: JobRecord,
    config: GuardConfig,
) -> bool:
    """Return False for unrelated jobs and True for a converged Loom job."""

    if record.comment in {"(null)", "None", ""} or not record.comment.startswith(
        "loom-cgroup-",
    ):
        return False
    match = _COMMENT_RE.fullmatch(record.comment)
    if match is None:
        raise GuardError("Loom cgroup job comment is invalid")
    if record.account not in config.allowed_accounts:
        raise GuardError("Loom cgroup job account is not allowed")
    if int(match.group(1)) != config.pids_max:
        raise GuardError("Loom cgroup job PID ceiling differs from host policy")
    try:
        controllers = set((job_path / "cgroup.controllers").read_text().split())
        subtree = set((job_path / "cgroup.subtree_control").read_text().split())
        resident = (job_path / "cgroup.procs").read_text().split()
    except OSError as exc:
        raise GuardError("could not read the Slurm job cgroup") from exc
    if resident:
        raise GuardError("Slurm job cgroup contains internal processes")
    if not _REQUIRED_CONTROLLERS.issubset(controllers):
        raise GuardError("Slurm job cgroup lacks cpu, memory, or pids")
    missing = _REQUIRED_CONTROLLERS - subtree
    if missing:
        try:
            (job_path / "cgroup.subtree_control").write_text(
                " ".join(f"+{item}" for item in sorted(missing)),
                encoding="utf-8",
            )
        except OSError as exc:
            raise GuardError("could not delegate Slurm job controllers") from exc
    try:
        (job_path / "pids.max").write_text(f"{config.pids_max}\n", encoding="utf-8")
        readback = (job_path / "pids.max").read_text().strip()
        delegated = subtree | {
            item.lstrip("+-") for item in (job_path / "cgroup.subtree_control").read_text().split()
        }
    except OSError as exc:
        raise GuardError("could not apply the Slurm job PID ceiling") from exc
    if readback != str(config.pids_max):
        raise GuardError("Slurm job PID ceiling readback drifted")
    if not _REQUIRED_CONTROLLERS.issubset(delegated):
        raise GuardError("Slurm job controller delegation readback drifted")
    return True


def scan_once(
    config: GuardConfig,
    *,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    job_lookup: JobLookup = _job_record,
) -> dict[str, int]:
    result = {"discovered": 0, "converged": 0, "unrelated": 0, "failed": 0}
    discovered = discover_job_cgroups(cgroup_root)
    if isinstance(job_lookup, BoundedJobLookup):
        job_lookup.retain({job_id for job_id, _path in discovered})
    for job_id, job_path in discovered:
        result["discovered"] += 1
        try:
            record = job_lookup(job_id)
            if apply_job_limit(job_path, record=record, config=config):
                result["converged"] += 1
            else:
                result["unrelated"] += 1
        except GuardError:
            result["failed"] += 1
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("command", choices=("once", "run"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        config = load_config(args.config)
        if _cluster_name() != config.cluster:
            raise GuardError("local Slurm cluster does not match guard config")
        if args.command == "once":
            result = scan_once(config)
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return int(result["failed"] > 0)
        lookup = BoundedJobLookup()
        while True:
            result = scan_once(config, job_lookup=lookup)
            if result["converged"] or result["failed"]:
                sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
                sys.stdout.flush()
            time.sleep(config.poll_interval_seconds)
    except (GuardError, OSError, subprocess.SubprocessError, ValueError):
        sys.stderr.write('{"error":"slurm-job-cgroup-guard-failed-safely"}\n')
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
