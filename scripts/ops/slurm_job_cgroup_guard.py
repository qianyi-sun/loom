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
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import time
from collections import OrderedDict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_JOB_DIR_RE = re.compile(r"^job_([1-9][0-9]*)$")
_ACCOUNT_RE = re.compile(r"^[a-z][a-z0-9-]{0,62}$")
_COMMENT_RE = re.compile(r"^loom-cgroup-v1:pids=([1-9][0-9]{0,8})$")
_REQUIRED_CONTROLLERS = frozenset({"cpu", "memory", "pids"})
_MAX_WALKED_DIRECTORIES = 100_000
_MAX_JOB_RECORD_CACHE = 10_000
_CANDIDATE_RE = re.compile(r"^[0-9a-f]{40}$")
DEFAULT_STATUS_PATH = Path(
    "/var/lib/loom-developer-sandbox-slurm-policy/guard-status.json",
)


class GuardError(RuntimeError):
    """The guard cannot safely apply one requested boundary."""


@dataclass(frozen=True, slots=True)
class GuardConfig:
    cluster: str
    controller: str
    submit_host: str
    allowed_nodes: frozenset[str]
    candidate_sha: str
    config_sha256: str
    pids_max: int
    allowed_accounts: frozenset[str]
    poll_interval_seconds: float
    require_gpu_probe: bool


@dataclass(frozen=True, slots=True)
class JobRecord:
    job_id: str
    account: str
    comment: str
    alloc_tres: str = ""
    job_name: str = ""
    batch_host: str = ""
    node_list: str = ""


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
        raw = path.read_bytes()
        payload = json.loads(raw)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise GuardError("guard config is unavailable or invalid") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "cluster",
            "controller",
            "submit_host",
            "allowed_nodes",
            "candidate_sha",
            "pids_max",
            "allowed_accounts",
            "poll_interval_seconds",
            "require_gpu_probe",
        }
        or payload.get("schema_version") != 1
    ):
        raise GuardError("guard config does not match the closed schema")
    cluster = payload.get("cluster")
    controller = payload.get("controller")
    submit_host = payload.get("submit_host")
    allowed_nodes = payload.get("allowed_nodes")
    candidate_sha = payload.get("candidate_sha")
    pids_max = payload.get("pids_max")
    accounts = payload.get("allowed_accounts")
    interval = payload.get("poll_interval_seconds")
    require_gpu_probe = payload.get("require_gpu_probe")
    if not isinstance(cluster, str) or not cluster or any(char.isspace() for char in cluster):
        raise GuardError("guard cluster is invalid")
    if (
        not isinstance(controller, str)
        or not controller
        or not isinstance(submit_host, str)
        or not submit_host
        or not isinstance(allowed_nodes, list)
        or not allowed_nodes
        or len(allowed_nodes) != len(set(allowed_nodes))
        or not all(isinstance(node, str) and node for node in allowed_nodes)
    ):
        raise GuardError("guard Slurm route is invalid")
    if type(pids_max) is not int or not 1 <= pids_max <= 100_000_000:
        raise GuardError("guard pids_max is invalid")
    if not isinstance(candidate_sha, str) or _CANDIDATE_RE.fullmatch(candidate_sha) is None:
        raise GuardError("guard candidate SHA is invalid")
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
    if type(require_gpu_probe) is not bool:
        raise GuardError("guard GPU probe policy is invalid")
    return GuardConfig(
        cluster=cluster,
        controller=controller,
        submit_host=submit_host,
        allowed_nodes=frozenset(node.lower() for node in allowed_nodes),
        candidate_sha=candidate_sha,
        config_sha256=hashlib.sha256(raw).hexdigest(),
        pids_max=pids_max,
        allowed_accounts=frozenset(accounts),
        poll_interval_seconds=float(interval),
        require_gpu_probe=require_gpu_probe,
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
    values: dict[str, list[str]] = {
        "JobId": [],
        "Account": [],
        "Comment": [],
        "AllocTRES": [],
        "JobName": [],
        "BatchHost": [],
        "NodeList": [],
    }
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
        alloc_tres=values["AllocTRES"][0],
        job_name=values["JobName"][0],
        batch_host=values["BatchHost"][0],
        node_list=values["NodeList"][0],
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
    return str(matches[0])


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
    candidate_label = config.candidate_sha[:12]
    if (
        not record.job_name.startswith("loom-")
        or (
            config.candidate_sha not in record.job_name
            and f"-{candidate_label}-" not in record.job_name
        )
    ):
        raise GuardError("Loom job name is not bound to the host candidate")
    if (
        record.batch_host.lower() not in config.allowed_nodes
        or record.node_list.lower() not in config.allowed_nodes
    ):
        raise GuardError("Loom job allocation is outside the reviewed Slurm route")
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


def _positive_int_or_quota(value: str) -> bool:
    fields = value.split()
    if not fields or fields[0] == "max":
        return False
    try:
        return int(fields[0]) > 0
    except ValueError:
        return False


def _gpu_tres_is_positive(value: str) -> bool:
    for item in value.split(","):
        key, separator, raw = item.partition("=")
        if separator and (key == "gres/gpu" or key.startswith("gres/gpu:")):
            try:
                return float(raw) > 0
            except ValueError:
                return False
    return False


def read_resource_probe(
    job_path: Path,
    *,
    record: JobRecord,
    config: GuardConfig,
) -> dict[str, Any]:
    """Read back finite controls from the real Slurm job cgroup."""

    try:
        cpu_max = (job_path / "cpu.max").read_text(encoding="utf-8").strip()
        memory_max = (job_path / "memory.max").read_text(encoding="utf-8").strip()
        pids_max = (job_path / "pids.max").read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise GuardError("job cgroup resource readback is unavailable") from exc
    if not _positive_int_or_quota(cpu_max):
        raise GuardError("job cgroup CPU ceiling is not finite and positive")
    if not _positive_int_or_quota(memory_max):
        raise GuardError("job cgroup memory ceiling is not finite and positive")
    if pids_max != str(config.pids_max):
        raise GuardError("job cgroup PID ceiling readback drifted")
    gpu_ok = not config.require_gpu_probe or _gpu_tres_is_positive(record.alloc_tres)
    if not gpu_ok:
        raise GuardError("job GPU TRES allocation readback is missing")
    return {
        "job_id": record.job_id,
        "cluster": config.cluster,
        "controller": config.controller,
        "submit_host": config.submit_host,
        "candidate_sha": config.candidate_sha,
        "job_name": record.job_name,
        "batch_host": record.batch_host,
        "node_list": record.node_list,
        "cpu_max": cpu_max,
        "memory_max": memory_max,
        "pids_max": pids_max,
        "gpu_tres": record.alloc_tres if config.require_gpu_probe else "not-required",
        "gpu_verified": gpu_ok,
    }


def scan_once(
    config: GuardConfig,
    *,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    job_lookup: JobLookup = _job_record,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "scanned": 0,
        "verified": 0,
        "unrelated": 0,
        "failed": 0,
        "failures": [],
        "resource_probe": None,
    }
    discovered = discover_job_cgroups(cgroup_root)
    if isinstance(job_lookup, BoundedJobLookup):
        job_lookup.retain({job_id for job_id, _path in discovered})
    for job_id, job_path in discovered:
        result["scanned"] += 1
        try:
            record = job_lookup(job_id)
            if apply_job_limit(job_path, record=record, config=config):
                probe = read_resource_probe(job_path, record=record, config=config)
                result["verified"] += 1
                probe["observed_at"] = datetime.now(UTC).isoformat()
                result["resource_probe"] = probe
            else:
                result["unrelated"] += 1
        except GuardError as exc:
            result["failed"] += 1
            result["failures"].append({"job_id": job_id, "reason": str(exc)})
    return result


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def write_status(
    path: Path,
    *,
    config: GuardConfig,
    result: dict[str, Any],
) -> None:
    payload = {
        "schema_version": 1,
        "timestamp": datetime.now(UTC).isoformat(),
        "candidate_sha": config.candidate_sha,
        "config_sha256": config.config_sha256,
        **result,
    }
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    path.parent.chmod(0o700)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(0o600)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def failed_status(reason: str) -> dict[str, Any]:
    return {
        "scanned": 0,
        "verified": 0,
        "unrelated": 0,
        "failed": 1,
        "failures": [{"job_id": None, "reason": reason}],
        "resource_probe": None,
    }


def daemon_iteration(
    config: GuardConfig,
    *,
    status_path: Path,
    job_lookup: JobLookup,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    last_resource_probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    try:
        if _cluster_name() != config.cluster:
            raise GuardError("local Slurm cluster does not match guard config")
        result = scan_once(config, cgroup_root=cgroup_root, job_lookup=job_lookup)
    except GuardError as exc:
        result = failed_status(str(exc))
    if result["resource_probe"] is None and last_resource_probe is not None:
        result["resource_probe"] = last_resource_probe
    write_status(status_path, config=config, result=result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("command", choices=("once", "run"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        config = load_config(args.config)
        if args.command == "once":
            try:
                if _cluster_name() != config.cluster:
                    raise GuardError("local Slurm cluster does not match guard config")
                result = scan_once(config)
            except GuardError as exc:
                result = failed_status(str(exc))
            write_status(args.status, config=config, result=result)
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return int(result["failed"] > 0)
        lookup = BoundedJobLookup()
        last_resource_probe: dict[str, Any] | None = None
        while True:
            result = daemon_iteration(
                config,
                status_path=args.status,
                job_lookup=lookup,
                last_resource_probe=last_resource_probe,
            )
            if isinstance(result["resource_probe"], dict):
                last_resource_probe = result["resource_probe"]
            if result["verified"] or result["failed"]:
                sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
                sys.stdout.flush()
            time.sleep(config.poll_interval_seconds)
    except (GuardError, OSError, subprocess.SubprocessError, ValueError):
        sys.stderr.write('{"error":"slurm-job-cgroup-guard-failed-safely"}\n')
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
