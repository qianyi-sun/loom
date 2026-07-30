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
import signal
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
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_CANDIDATE_ID_RE = re.compile(r"^cand-[0-9a-f]{40}$")
_ENV_ID_RE = re.compile(r"^denv-[a-z0-9-]{8,64}$")
_SLICE_RE = re.compile(r"^loom-job-[1-9][0-9]*-[0-9a-f]{40}\.slice$")
_SYSTEMD_UNIT_ROOT = Path("/run/systemd/system")
_SLICE_RECEIPT_ROOT = Path(
    "/run/loom-developer-sandbox-slurm-policy/systemd-slices",
)
_SLICE_RECEIPT_FIELDS = {
    "schema_version",
    "kind",
    "systemd_slice",
    "slice_identity_sha256",
    "unit_sha256",
    "job_id",
    "job_start_time",
    "cluster",
    "node_list",
    "account",
    "env_id",
    "resource_generation",
    "runtime_id",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "cpu_max",
    "memory_max",
    "memory_swap_max_source",
    "memory_swap_max_effective",
    "pids_max",
    "cpuset_cpus",
    "cpuset_mems",
    "gpu_tres",
    "gpu_detail",
    "payload_sha256",
}
DEFAULT_STATUS_PATH = Path(
    "/var/lib/loom-developer-sandbox-slurm-policy/guard-status.json",
)


class GuardError(RuntimeError):
    """The guard cannot safely apply one requested boundary."""


def _fixed_staging_binding_is_exact(binding: CandidateBinding) -> bool:
    return (
        binding.account == "loom-staging"
        and binding.sandbox == "staging"
        and binding.service_user == "loom-staging-worker"
        and binding.slurm_qos == "loom-staging"
        and binding.env_id == f"denv-staging-{binding.candidate_sha}"
        and binding.candidate_id == f"cand-{binding.candidate_sha}"
        and binding.resource_generation >= 1
    )


@dataclass(frozen=True, slots=True)
class CandidateBinding:
    account: str
    env_id: str
    resource_generation: int
    sandbox: str
    service_user: str
    slurm_qos: str
    candidate_id: str
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
    docker_cgroup_driver: str

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
    start_time: str = ""
    gres_detail: str = ""


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
            "docker_cgroup_driver",
        }
        or payload.get("schema_version") != 3
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
    docker_cgroup_driver = payload.get("docker_cgroup_driver")
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
                "env_id",
                "resource_generation",
                "sandbox",
                "service_user",
                "slurm_qos",
                "candidate_id",
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
        env_id = raw_binding.get("env_id")
        resource_generation = raw_binding.get("resource_generation")
        sandbox = raw_binding.get("sandbox")
        service_user = raw_binding.get("service_user")
        slurm_qos = raw_binding.get("slurm_qos")
        candidate_id = raw_binding.get("candidate_id")
        candidate_sha = raw_binding.get("candidate_sha")
        candidate_tree = raw_binding.get("candidate_tree")
        if (
            not isinstance(env_id, str)
            or _ENV_ID_RE.fullmatch(env_id) is None
            or type(resource_generation) is not int
            or resource_generation < 1
            or not isinstance(sandbox, str)
            or _SANDBOX_RE.fullmatch(sandbox) is None
            or not isinstance(service_user, str)
            or _SAFE_NAME_RE.fullmatch(service_user) is None
            or service_user == "root"
            or not isinstance(slurm_qos, str)
            or _SAFE_NAME_RE.fullmatch(slurm_qos) is None
            or not isinstance(candidate_id, str)
            or _CANDIDATE_ID_RE.fullmatch(candidate_id) is None
            or not isinstance(candidate_sha, str)
            or _CANDIDATE_RE.fullmatch(candidate_sha) is None
            or not isinstance(candidate_tree, str)
            or _CANDIDATE_RE.fullmatch(candidate_tree) is None
        ):
            raise GuardError("guard candidate bindings are invalid")
        candidate_bindings[account] = CandidateBinding(
            account=account,
            env_id=env_id,
            resource_generation=resource_generation,
            sandbox=sandbox,
            service_user=service_user,
            slurm_qos=slurm_qos,
            candidate_id=candidate_id,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
    if len({binding.sandbox for binding in candidate_bindings.values()}) != len(
        candidate_bindings
    ) or len({binding.service_user for binding in candidate_bindings.values()}) != len(
        candidate_bindings
    ):
        raise GuardError("guard candidate binding identities must be globally unique")
    staging = candidate_bindings.get("loom-staging")
    if (staging is not None and not _fixed_staging_binding_is_exact(staging)) or any(
        account != "loom-staging"
        and (
            binding.sandbox == "staging"
            or binding.service_user == "loom-staging-worker"
            or binding.slurm_qos == "loom-staging"
            or binding.env_id.startswith("denv-staging-")
        )
        for account, binding in candidate_bindings.items()
    ):
        raise GuardError("guard fixed staging binding is invalid")
    normalized_bindings = {
        account: {
            "env_id": binding.env_id,
            "resource_generation": binding.resource_generation,
            "sandbox": binding.sandbox,
            "service_user": binding.service_user,
            "slurm_qos": binding.slurm_qos,
            "candidate_id": binding.candidate_id,
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
    if docker_cgroup_driver not in {"cgroupfs", "systemd"}:
        raise GuardError("guard Docker cgroup driver is invalid")
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
        docker_cgroup_driver=docker_cgroup_driver,
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
        "StartTime": [],
        "GresDetail": [],
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
        start_time=values["StartTime"][0],
        gres_detail=values["GresDetail"][0],
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
            rf"{re.escape(node)}-g(?P<convergence>[0-9a-f]{{64}})-"
            rf"a(?P<generation>[1-9][0-9]*)"
        ),
        record.job_name,
    )
    allocation_name_valid = (
        allocation_job_name is not None
        and int(allocation_job_name.group("generation")) == binding.resource_generation
        and (
            binding.account != "loom-staging"
            or allocation_job_name.group("convergence") == config.candidate_set_sha256
        )
    )
    if (binding.account == "loom-staging" and not allocation_name_valid) or (
        binding.account != "loom-staging"
        and record.job_name != regular_job_name
        and not allocation_name_valid
    ):
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


def _positive_limit(value: str, *, label: str) -> int:
    if not value.isascii() or not value.isdecimal():
        raise GuardError(f"job cgroup {label} ceiling is not finite")
    parsed = int(value)
    if parsed <= 0:
        raise GuardError(f"job cgroup {label} ceiling is not positive")
    return parsed


def _cpu_max(value: str) -> tuple[int, int]:
    fields = value.split()
    if len(fields) != 2:
        raise GuardError("job cgroup CPU ceiling is invalid")
    return (
        _positive_limit(fields[0], label="CPU"),
        _positive_limit(fields[1], label="CPU period"),
    )


def _slice_identity(
    *,
    config: GuardConfig,
    record: JobRecord,
    binding: CandidateBinding,
) -> tuple[str, str]:
    if (
        not record.start_time
        or any(character.isspace() for character in record.start_time)
        or record.node_list.lower() not in config.allowed_nodes
    ):
        raise GuardError("Loom job start identity is unavailable")
    identity = {
        "cluster": config.cluster,
        "node": record.node_list.lower(),
        "job_id": record.job_id,
        "job_start_time": record.start_time,
        "account": record.account,
        "env_id": binding.env_id,
        "resource_generation": binding.resource_generation,
        "runtime_id": binding.sandbox,
        "candidate_id": binding.candidate_id,
        "candidate_sha": binding.candidate_sha,
        "candidate_tree": binding.candidate_tree,
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii"),
    ).hexdigest()
    unit = f"loom-job-{record.job_id}-{digest[:40]}.slice"
    if _SLICE_RE.fullmatch(unit) is None:
        raise GuardError("allocation systemd slice identity is invalid")
    return unit, digest


def _systemd_slice_unit(probe: Mapping[str, Any]) -> bytes:
    quota, period = _cpu_max(str(probe["cpu_max"]))
    # Six fractional percentage digits are sufficient for cgroup v2's
    # microsecond CPU period. Flooring is deliberately equal-or-stricter.
    quota_millionths = quota * 100_000_000 // period
    quota_percent = f"{quota_millionths // 1_000_000}.{quota_millionths % 1_000_000:06d}%"
    return (
        "[Unit]\n"
        f"Description=Loom allocation mirror {probe['slice_identity_sha256']}\n"
        "\n"
        "[Slice]\n"
        "CPUAccounting=yes\n"
        "MemoryAccounting=yes\n"
        "TasksAccounting=yes\n"
        f"CPUQuota={quota_percent}\n"
        f"CPUQuotaPeriodSec={period}us\n"
        f"MemoryMax={probe['memory_max']}\n"
        f"MemorySwapMax={probe['memory_swap_max_effective']}\n"
        f"TasksMax={probe['pids_max']}\n"
        f"AllowedCPUs={probe['cpuset_cpus']}\n"
        f"AllowedMemoryNodes={probe['cpuset_mems']}\n"
    ).encode("ascii")


def _ensure_authority_directory(
    path: Path,
    *,
    mode: int,
    create: bool,
) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        if not create:
            raise GuardError("allocation authority directory is unavailable") from None
        try:
            path.mkdir(mode=mode)
        except OSError as exc:
            raise GuardError("allocation authority directory could not be created") from exc
    for ancestor in (path, *path.parents):
        metadata = ancestor.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid not in {0, os.geteuid()}
            or (ancestor != Path("/") and stat.S_IMODE(metadata.st_mode) & 0o022)
        ):
            raise GuardError("allocation authority directory ancestry is unsafe")
    metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != mode
        or metadata.st_nlink < 2
    ):
        raise GuardError("allocation authority directory is unsafe")


def _read_exact_existing(path: Path, *, mode: int) -> bytes:
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        content = bytearray()
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > _MAX_CONFIG_BYTES:
                raise GuardError("allocation authority file is too large")
        after = os.fstat(descriptor)
        rebound = path.lstat()
    except OSError as exc:
        raise GuardError("allocation authority file is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identities = {
        (
            item.st_dev,
            item.st_ino,
            item.st_mode,
            item.st_uid,
            item.st_gid,
            item.st_nlink,
            item.st_size,
            item.st_mtime_ns,
            item.st_ctime_ns,
        )
        for item in (lexical, opened, after, rebound)
    }
    if (
        len(identities) != 1
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_uid != os.geteuid()
        or opened.st_gid != os.getegid()
        or stat.S_IMODE(opened.st_mode) != mode
        or opened.st_nlink != 1
        or len(content) != opened.st_size
    ):
        raise GuardError("allocation authority file metadata is unsafe")
    return bytes(content)


def _write_exact_runtime_file(
    path: Path,
    content: bytes,
    *,
    mode: int,
    parent_mode: int,
    create_parent: bool,
) -> bool:
    _ensure_authority_directory(
        path.parent,
        mode=parent_mode,
        create=create_parent,
    )
    try:
        path.lstat()
    except FileNotFoundError:
        pass
    else:
        if _read_exact_existing(path, mode=mode) != content:
            raise GuardError("allocation systemd slice residue conflicts")
        return False
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    return True


SystemdRunner = Callable[[Sequence[str]], None]


def _run_systemd(command: Sequence[str]) -> None:
    try:
        completed = subprocess.run(
            tuple(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardError("allocation systemd slice validation failed safely") from exc
    if completed.returncode:
        raise GuardError("allocation systemd slice validation failed")


def converge_systemd_slice(
    probe: dict[str, Any],
    *,
    unit_root: Path = _SYSTEMD_UNIT_ROOT,
    receipt_root: Path = _SLICE_RECEIPT_ROOT,
    runner: SystemdRunner = _run_systemd,
) -> dict[str, Any]:
    """Publish an inactive, limit-bound slice for Docker's systemd driver.

    No process is moved and no unit is stopped or killed. Docker activates the
    slice when it creates the first container scope. A conflicting residue is
    preserved and fails closed for operator inspection.
    """

    unit_name = str(probe["systemd_slice"])
    unit_path = unit_root / unit_name
    unit = _systemd_slice_unit(probe)
    changed = _write_exact_runtime_file(
        unit_path,
        unit,
        mode=0o644,
        parent_mode=0o755,
        create_parent=False,
    )
    runner(("/usr/bin/systemd-analyze", "verify", str(unit_path)))
    if changed:
        runner(("/usr/bin/systemctl", "daemon-reload"))
    receipt_unsigned = {
        "schema_version": 1,
        "kind": "loom.slurm-systemd-slice-receipt",
        "systemd_slice": unit_name,
        "slice_identity_sha256": probe["slice_identity_sha256"],
        "unit_sha256": hashlib.sha256(unit).hexdigest(),
        "job_id": probe["job_id"],
        "job_start_time": probe["job_start_time"],
        "cluster": probe["cluster"],
        "node_list": probe["node_list"],
        "account": probe["account"],
        "env_id": probe["env_id"],
        "resource_generation": probe["resource_generation"],
        "runtime_id": probe["sandbox"],
        "candidate_id": probe["candidate_id"],
        "candidate_sha": probe["candidate_sha"],
        "candidate_tree": probe["candidate_tree"],
        "cpu_max": probe["cpu_max"],
        "memory_max": probe["memory_max"],
        "memory_swap_max_source": probe["memory_swap_max_source"],
        "memory_swap_max_effective": probe["memory_swap_max_effective"],
        "pids_max": probe["pids_max"],
        "cpuset_cpus": probe["cpuset_cpus"],
        "cpuset_mems": probe["cpuset_mems"],
        "gpu_tres": probe["gpu_tres"],
        "gpu_detail": probe["gpu_detail"],
    }
    receipt = {
        **receipt_unsigned,
        "payload_sha256": hashlib.sha256(
            json.dumps(
                receipt_unsigned,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("ascii"),
        ).hexdigest(),
    }
    receipt_bytes = (
        json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
    )
    _write_exact_runtime_file(
        receipt_root / f"{unit_name}.json",
        receipt_bytes,
        mode=0o444,
        parent_mode=0o755,
        create_parent=True,
    )
    return {**probe, "slice_unit_sha256": hashlib.sha256(unit).hexdigest()}


def _slice_cgroup_path(cgroup_root: Path, unit: str) -> Path:
    stem = unit.removesuffix(".slice")
    components = stem.split("-")
    prefixes = ["-".join(components[:index]) + ".slice" for index in range(1, len(components))]
    return cgroup_root.joinpath(*prefixes, unit)


def _cleanup_expired_slice(
    unit: str,
    *,
    cgroup_root: Path,
    unit_root: Path,
    receipt_root: Path,
) -> bool:
    if _SLICE_RE.fullmatch(unit) is None:
        raise GuardError("residual allocation slice name is foreign")
    unit_path = unit_root / unit
    receipt_path = receipt_root / f"{unit}.json"
    try:
        unit_bytes = _read_exact_existing(unit_path, mode=0o644)
        receipt_bytes = _read_exact_existing(receipt_path, mode=0o444)
        receipt = json.loads(receipt_bytes)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise GuardError("residual allocation slice authority is incomplete") from exc
    unsigned = (
        {key: value for key, value in receipt.items() if key != "payload_sha256"}
        if isinstance(receipt, dict)
        else {}
    )
    identity = (
        {
            "cluster": receipt.get("cluster"),
            "node": str(receipt.get("node_list", "")).lower(),
            "job_id": receipt.get("job_id"),
            "job_start_time": receipt.get("job_start_time"),
            "account": receipt.get("account"),
            "env_id": receipt.get("env_id"),
            "resource_generation": receipt.get("resource_generation"),
            "runtime_id": receipt.get("runtime_id"),
            "candidate_id": receipt.get("candidate_id"),
            "candidate_sha": receipt.get("candidate_sha"),
            "candidate_tree": receipt.get("candidate_tree"),
        }
        if isinstance(receipt, dict)
        else {}
    )
    identity_digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii"),
    ).hexdigest()
    if (
        not isinstance(receipt, dict)
        or set(receipt) != _SLICE_RECEIPT_FIELDS
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.slurm-systemd-slice-receipt"
        or receipt_bytes
        != json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        or receipt.get("systemd_slice") != unit
        or receipt.get("slice_identity_sha256") != identity_digest
        or unit != f"loom-job-{receipt.get('job_id')}-{identity_digest[:40]}.slice"
        or _DIGEST_RE.fullmatch(str(receipt.get("unit_sha256"))) is None
        or receipt.get("unit_sha256") != hashlib.sha256(unit_bytes).hexdigest()
        or _ENV_ID_RE.fullmatch(str(receipt.get("env_id"))) is None
        or _CANDIDATE_ID_RE.fullmatch(str(receipt.get("candidate_id"))) is None
        or _CANDIDATE_RE.fullmatch(str(receipt.get("candidate_sha"))) is None
        or _CANDIDATE_RE.fullmatch(str(receipt.get("candidate_tree"))) is None
        or type(receipt.get("resource_generation")) is not int
        or int(receipt["resource_generation"]) < 1
        or receipt.get("payload_sha256")
        != hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest()
    ):
        raise GuardError("residual allocation slice authority is invalid")
    control_group = _slice_cgroup_path(cgroup_root, unit)
    if control_group.exists():
        walked = 0
        for raw_root, directories, _files in os.walk(control_group, followlinks=False):
            walked += 1
            if walked > 10_000:
                raise GuardError("residual allocation slice cgroup walk exceeded its bound")
            current = Path(raw_root)
            if current.name.endswith(".scope") or any(
                name.endswith(".scope") for name in directories
            ):
                raise GuardError("residual allocation slice still contains a scope")
            try:
                if (current / "cgroup.procs").read_text(encoding="utf-8").split():
                    raise GuardError("residual allocation slice still contains processes")
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise GuardError("residual allocation slice cgroup is unreadable") from exc
    unit_path.unlink()
    receipt_path.unlink()
    _fsync_directory(unit_root)
    _fsync_directory(receipt_root)
    return True


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
        memory_swap_max_source = (
            (job_path / "memory.swap.max")
            .read_text(
                encoding="utf-8",
            )
            .strip()
        )
        pids_max = (job_path / "pids.max").read_text(encoding="utf-8").strip()
        cpuset_cpus = (
            (job_path / "cpuset.cpus.effective")
            .read_text(
                encoding="utf-8",
            )
            .strip()
        )
        cpuset_mems = (
            (job_path / "cpuset.mems.effective")
            .read_text(
                encoding="utf-8",
            )
            .strip()
        )
    except OSError as exc:
        raise GuardError("job cgroup resource readback is unavailable") from exc
    if not _positive_int_or_quota(cpu_max):
        raise GuardError("job cgroup CPU ceiling is not finite and positive")
    if not _positive_int_or_quota(memory_max):
        raise GuardError("job cgroup memory ceiling is not finite and positive")
    if pids_max != str(config.pids_max):
        raise GuardError("job cgroup PID ceiling readback drifted")
    _cpu_max(cpu_max)
    _positive_limit(memory_max, label="memory")
    if memory_swap_max_source != "max" and (
        not memory_swap_max_source.isascii() or not memory_swap_max_source.isdecimal()
    ):
        raise GuardError("job cgroup swap ceiling is not finite")
    # The mirror never grants anonymous swap, even when Slurm's source job
    # cgroup has a finite or unlimited swap ceiling. This is deliberately
    # stricter than the source allocation and avoids host-wide swap pressure
    # escaping the job's memory accounting boundary.
    memory_swap_max_effective = "0"
    if not cpuset_cpus or not cpuset_mems:
        raise GuardError("job cgroup cpuset binding is unavailable")
    gpu_ok = not config.require_gpu_probe or (
        _gpu_tres_is_positive(record.alloc_tres)
        and record.gres_detail not in {"", "(null)", "None"}
        and "gpu" in record.gres_detail.lower()
    )
    if not gpu_ok:
        raise GuardError("job GPU TRES allocation readback is missing")
    result = {
        "job_id": record.job_id,
        "job_start_time": record.start_time,
        "cluster": config.cluster,
        "controller": config.controller,
        "submit_host": config.submit_host,
        "account": record.account,
        "sandbox": binding.sandbox,
        "service_user": binding.service_user,
        "env_id": binding.env_id,
        "resource_generation": binding.resource_generation,
        "candidate_id": binding.candidate_id,
        "candidate_sha": binding.candidate_sha,
        "candidate_tree": binding.candidate_tree,
        "candidate_set_sha256": config.candidate_set_sha256,
        "job_name": record.job_name,
        "batch_host": record.batch_host,
        "node_list": record.node_list,
        "cpu_max": cpu_max,
        "memory_max": memory_max,
        "memory_swap_max_source": memory_swap_max_source,
        "memory_swap_max_effective": memory_swap_max_effective,
        "pids_max": pids_max,
        "cpuset_cpus": cpuset_cpus,
        "cpuset_mems": cpuset_mems,
        "gpu_tres": record.alloc_tres if config.require_gpu_probe else "not-required",
        "gpu_detail": record.gres_detail if config.require_gpu_probe else "not-required",
        "gpu_verified": gpu_ok,
        "docker_cgroup_driver": config.docker_cgroup_driver,
    }
    if config.docker_cgroup_driver == "systemd":
        systemd_slice, slice_identity_sha256 = _slice_identity(
            config=config,
            record=record,
            binding=binding,
        )
        result.update(
            {
                "systemd_slice": systemd_slice,
                "slice_identity_sha256": slice_identity_sha256,
            },
        )
    return result


def scan_once(
    config: GuardConfig,
    *,
    cgroup_root: Path = Path("/sys/fs/cgroup"),
    job_lookup: JobLookup = _job_record,
    unit_root: Path = _SYSTEMD_UNIT_ROOT,
    receipt_root: Path = _SLICE_RECEIPT_ROOT,
    systemd_runner: SystemdRunner = _run_systemd,
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
    active_slices: set[str] = set()
    if isinstance(job_lookup, BoundedJobLookup):
        job_lookup.retain({job_id for job_id, _path in discovered})
    for job_id, job_path in discovered:
        result["scanned"] += 1
        try:
            record = job_lookup(job_id)
            if apply_job_limit(job_path, record=record, config=config):
                probe = read_resource_probe(job_path, record=record, config=config)
                if config.docker_cgroup_driver == "systemd":
                    probe = converge_systemd_slice(
                        probe,
                        unit_root=unit_root,
                        receipt_root=receipt_root,
                        runner=systemd_runner,
                    )
                    active_slices.add(str(probe["systemd_slice"]))
                result["verified"] += 1
                probe["observed_at"] = datetime.now(UTC).isoformat()
                result["resource_probes"][record.account] = probe
            else:
                result["unrelated"] += 1
        except GuardError as exc:
            result["failed"] += 1
            result["failures"].append({"job_id": job_id, "reason": str(exc)})
    if config.docker_cgroup_driver == "systemd":
        try:
            residues = {
                path.name
                for path in unit_root.glob("loom-job-*.slice")
                if path.name not in active_slices
            }
        except OSError as exc:
            raise GuardError("allocation systemd slice inventory is unavailable") from exc
        cleaned = False
        for unit in sorted(residues):
            try:
                cleaned = (
                    _cleanup_expired_slice(
                        unit,
                        cgroup_root=cgroup_root,
                        unit_root=unit_root,
                        receipt_root=receipt_root,
                    )
                    or cleaned
                )
            except GuardError as exc:
                result["failed"] += 1
                result["failures"].append(
                    {
                        "job_id": None,
                        "reason": f"residual allocation systemd slice preserved: {exc}",
                    },
                )
        if cleaned:
            systemd_runner(("/usr/bin/systemctl", "daemon-reload"))
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
        signal.signal(signal.SIGHUP, lambda _signum, _frame: None)
        lookup = BoundedJobLookup()
        last_resource_probes: dict[str, dict[str, Any]] = {}
        previous_candidate_set: str | None = None
        while True:
            config = load_config(args.config)
            if (
                previous_candidate_set is not None
                and config.candidate_set_sha256 != previous_candidate_set
            ):
                last_resource_probes = {}
            previous_candidate_set = config.candidate_set_sha256
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
