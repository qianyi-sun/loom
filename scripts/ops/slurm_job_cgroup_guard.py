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
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_JOB_DIR_RE = re.compile(r"^job_([1-9][0-9]*)$")
_SAFE_NAME_RE = re.compile(r"^[a-z][a-z0-9_-]{1,62}$")
_SANDBOX_RE = re.compile(r"^[a-z][a-z0-9-]{0,47}$")
_COMMENT_RE = re.compile(r"^loom-cgroup-v1:pids=([1-9][0-9]{0,8})$")
_REQUIRED_CONTROLLERS = frozenset({"cpu", "memory", "pids"})
_MAX_WALKED_DIRECTORIES = 100_000
_MAX_JOB_RECORD_CACHE = 10_000
_MAX_CONFIG_BYTES = 1 << 20
_CANDIDATE_RE = re.compile(r"^[0-9a-f]{40}$")
DEFAULT_STATUS_PATH = Path(
    "/var/lib/loom-developer-sandbox-slurm-policy/guard-status.json",
)


class GuardError(RuntimeError):
    """The guard cannot safely apply one requested boundary."""


@dataclass(frozen=True, slots=True)
class CandidateBinding:
    account: str
    sandbox: str
    service_user: str
    candidate_sha: str
    candidate_tree: str


@dataclass(frozen=True, slots=True)
class GuardConfig:
    cluster: str
    controller: str
    submit_host: str
    allowed_nodes: frozenset[str]
    candidate_bindings: Mapping[str, CandidateBinding]
    candidate_set_sha256: str
    config_sha256: str
    pids_max: int
    poll_interval_seconds: float
    require_gpu_probe: bool

    @property
    def allowed_accounts(self) -> frozenset[str]:
        return frozenset(self.candidate_bindings)


@dataclass(frozen=True, slots=True)
class JobRecord:
    job_id: str
    account: str
    comment: str
    alloc_tres: str = ""
    job_name: str = ""
    batch_host: str = ""
    node_list: str = ""
    user: str = ""


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


def _read_bound_config(path: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise GuardError("guard config is unavailable or invalid") from exc
    try:
        opened = os.fstat(descriptor)
        linked = path.lstat()
        expected_uid, expected_gid = os.geteuid(), os.getegid()
        if (
            stat.S_ISLNK(linked.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or stat.S_IMODE(linked.st_mode) != 0o600
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or linked.st_uid != expected_uid
            or linked.st_gid != expected_gid
            or opened.st_nlink != 1
            or linked.st_nlink != 1
            or (opened.st_dev, opened.st_ino) != (linked.st_dev, linked.st_ino)
            or not 0 <= opened.st_size <= _MAX_CONFIG_BYTES
        ):
            raise GuardError("guard config metadata is unsafe")
        content = bytearray()
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > _MAX_CONFIG_BYTES:
                raise GuardError("guard config is too large")
        after = os.fstat(descriptor)
        rebound = path.lstat()
        identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_uid,
            opened.st_gid,
            opened.st_nlink,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        )
        if identity != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) or identity != (
            rebound.st_dev,
            rebound.st_ino,
            rebound.st_mode,
            rebound.st_uid,
            rebound.st_gid,
            rebound.st_nlink,
            rebound.st_size,
            rebound.st_mtime_ns,
            rebound.st_ctime_ns,
        ):
            raise GuardError("guard config changed during read")
        if len(content) != opened.st_size:
            raise GuardError("guard config size changed during read")
        return bytes(content)
    except OSError as exc:
        raise GuardError("guard config is unavailable or invalid") from exc
    finally:
        os.close(descriptor)


def _unique_json_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise GuardError("guard config contains duplicate fields")
        result[key] = value
    return result


def load_config(path: Path) -> GuardConfig:
    try:
        raw = _read_bound_config(path)
        payload = json.loads(raw, object_pairs_hook=_unique_json_object)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise GuardError("guard config is unavailable or invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "cluster",
            "controller",
            "submit_host",
            "allowed_nodes",
            "candidate_bindings",
            "candidate_set_sha256",
            "pids_max",
            "poll_interval_seconds",
            "require_gpu_probe",
        }
        or payload.get("schema_version") != 2
    ):
        raise GuardError("guard config does not match the closed schema")
    cluster = payload.get("cluster")
    controller = payload.get("controller")
    submit_host = payload.get("submit_host")
    allowed_nodes = payload.get("allowed_nodes")
    raw_bindings = payload.get("candidate_bindings")
    candidate_set_sha256 = payload.get("candidate_set_sha256")
    pids_max = payload.get("pids_max")
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
    if (
        not isinstance(raw_bindings, dict)
        or not raw_bindings
        or any(
            not isinstance(account, str)
            or _SAFE_NAME_RE.fullmatch(account) is None
            or not isinstance(binding, dict)
            or set(binding)
            != {
                "sandbox",
                "service_user",
                "candidate_sha",
                "candidate_tree",
            }
            for account, binding in raw_bindings.items()
        )
    ):
        raise GuardError("guard candidate bindings are invalid")
    candidate_bindings: dict[str, CandidateBinding] = {}
    for account in sorted(raw_bindings):
        raw_binding = raw_bindings[account]
        sandbox = raw_binding.get("sandbox")
        service_user = raw_binding.get("service_user")
        candidate_sha = raw_binding.get("candidate_sha")
        candidate_tree = raw_binding.get("candidate_tree")
        if (
            not isinstance(sandbox, str)
            or _SANDBOX_RE.fullmatch(sandbox) is None
            or not isinstance(service_user, str)
            or _SAFE_NAME_RE.fullmatch(service_user) is None
            or service_user == "root"
            or not isinstance(candidate_sha, str)
            or _CANDIDATE_RE.fullmatch(candidate_sha) is None
            or not isinstance(candidate_tree, str)
            or _CANDIDATE_RE.fullmatch(candidate_tree) is None
        ):
            raise GuardError("guard candidate bindings are invalid")
        candidate_bindings[account] = CandidateBinding(
            account=account,
            sandbox=sandbox,
            service_user=service_user,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
    if len({binding.sandbox for binding in candidate_bindings.values()}) != len(
        candidate_bindings
    ) or len({binding.service_user for binding in candidate_bindings.values()}) != len(
        candidate_bindings
    ):
        raise GuardError("guard candidate binding identities must be globally unique")
    normalized_bindings = {
        account: {
            "sandbox": binding.sandbox,
            "service_user": binding.service_user,
            "candidate_sha": binding.candidate_sha,
            "candidate_tree": binding.candidate_tree,
        }
        for account, binding in candidate_bindings.items()
    }
    canonical_bindings = json.dumps(
        normalized_bindings,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    expected_set_sha256 = hashlib.sha256(canonical_bindings).hexdigest()
    if candidate_set_sha256 != expected_set_sha256:
        raise GuardError("guard candidate-set digest is invalid")
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
        candidate_bindings=candidate_bindings,
        candidate_set_sha256=expected_set_sha256,
        config_sha256=hashlib.sha256(raw).hexdigest(),
        pids_max=pids_max,
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
        "UserId": [],
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
        user=values["UserId"][0].partition("(")[0],
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
    binding = config.candidate_bindings[record.account]
    if record.account != binding.account:
        raise GuardError("Loom cgroup job account binding is inconsistent")
    if record.user != binding.service_user:
        raise GuardError("Loom cgroup job user does not match its sandbox account")
    if int(match.group(1)) != config.pids_max:
        raise GuardError("Loom cgroup job PID ceiling differs from host policy")
    candidate_label = binding.candidate_sha[:12]
    node = record.node_list.lower()
    regular_job_name = f"loom-sandbox-{binding.sandbox}-{candidate_label}-{node}"
    allocation_job_name = re.fullmatch(
        (
            rf"loom827-{re.escape(binding.sandbox)}-{candidate_label}-"
            rf"{re.escape(node)}-g[0-9a-f]{{64}}-a[1-9][0-9]*"
        ),
        record.job_name,
    )
    if record.job_name != regular_job_name and allocation_job_name is None:
        raise GuardError("Loom job name is not bound to its sandbox candidate")
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
        binding = config.candidate_bindings[record.account]
    except KeyError as exc:
        raise GuardError("job resource probe account is not allowed") from exc
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
        "account": record.account,
        "sandbox": binding.sandbox,
        "service_user": binding.service_user,
        "candidate_sha": binding.candidate_sha,
        "candidate_tree": binding.candidate_tree,
        "candidate_set_sha256": config.candidate_set_sha256,
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
        "resource_probes": {},
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
                result["resource_probes"][record.account] = probe
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
        "schema_version": 2,
        "timestamp": datetime.now(UTC).isoformat(),
        "candidate_set_sha256": config.candidate_set_sha256,
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
        "resource_probes": {},
    }


def daemon_iteration(
    config: GuardConfig,
    *,
    status_path: Path,
    job_lookup: JobLookup,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    last_resource_probes: Mapping[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    try:
        if _cluster_name() != config.cluster:
            raise GuardError("local Slurm cluster does not match guard config")
        result = scan_once(config, cgroup_root=cgroup_root, job_lookup=job_lookup)
    except GuardError as exc:
        result = failed_status(str(exc))
    if last_resource_probes is not None:
        result["resource_probes"] = {
            **last_resource_probes,
            **result["resource_probes"],
        }
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
        last_resource_probes: dict[str, dict[str, Any]] = {}
        while True:
            result = daemon_iteration(
                config,
                status_path=args.status,
                job_lookup=lookup,
                last_resource_probes=last_resource_probes,
            )
            last_resource_probes = dict(result["resource_probes"])
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
