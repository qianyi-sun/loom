#!/usr/bin/env python3
"""Plan, check, and converge developer-sandbox Slurm host policy.

Mutations are local-host only, require root, and are disabled unless
``--execute`` is present. The caller must drain one node at a time before
requesting service restarts.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import pwd
import re
import socket
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any


class PolicyError(ValueError):
    """The requested policy cannot be applied safely."""


_SLURM_KEYS = {
    "ProctrackType": "proctrack_type",
    "TaskPlugin": "task_plugin",
    "JobAcctGatherType": "jobacct_gather_type",
    "AccountingStorageEnforce": "accounting_storage_enforce",
    "PriorityType": "priority_type",
    "PriorityWeightFairshare": "priority_weight_fairshare",
    "PrologFlags": "prolog_flags",
}
_CGROUP_KEYS = {
    "CgroupPlugin": "plugin",
    "ConstrainCores": "constrain_cores",
    "ConstrainRAMSpace": "constrain_ram_space",
    "ConstrainSwapSpace": "constrain_swap_space",
    "ConstrainDevices": "constrain_devices",
    "EnableControllers": "enable_controllers",
}
_SAFE_NAME = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_CANDIDATE_RE = re.compile(r"^[0-9a-f]{40}$")
_SNAPSHOT_NAME_RE = re.compile(r"^[0-9]{8}T[0-9]{6}\.[0-9]{6}Z$")
_STATE_RELATIVE = Path("var/lib/loom-developer-sandbox-slurm-policy")
_GUARD_STATUS_RELATIVE = _STATE_RELATIVE / "guard-status.json"
_GUARD_STATUS_MAX_AGE = timedelta(seconds=30)
_ALLOCATION_PROBE_RELATIVE = _STATE_RELATIVE / "allocation-probes"
_ALLOCATION_PROBE_MAX_AGE = timedelta(minutes=15)
_ALLOCATION_POLL_SECONDS = 1.0
_ALLOCATION_TIMEOUT_SECONDS = 180.0
_TERMINAL_JOB_STATES = frozenset(
    {
        "BOOT_FAIL",
        "CANCELLED",
        "COMPLETED",
        "DEADLINE",
        "FAILED",
        "NODE_FAIL",
        "OUT_OF_MEMORY",
        "PREEMPTED",
        "TIMEOUT",
    },
)
_ENV_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True, slots=True)
class Profile:
    cluster: str
    controller: str
    submit_host: str
    allowed_nodes: tuple[str, ...]
    host_aliases: Mapping[str, str]
    slot_budget: int
    pending_slot_budget: int
    cpus_per_slot: int
    memory_mib_per_slot: int
    gpu_tres_per_slot: float
    job_pids_max: int
    slurm: Mapping[str, str | int]
    cgroup: Mapping[str, str | bool]
    docker_cgroup_driver: str
    parent_account: str
    child_accounts: tuple[str, ...]
    users: tuple[str, ...]
    fairshare: int
    qos: str
    qos_priority: int
    qos_max_wall: str
    qos_max_jobs_per_user: int
    qos_max_submit_jobs_per_user: int
    parent_group_tres: tuple[str, ...]


def _table(raw: Mapping[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise PolicyError(f"{key} must be a TOML table")
    return dict(value)


def _strings(value: Any, field: str) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or not value
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise PolicyError(f"{field} must be a non-empty string array")
    return tuple(value)


def load_profile(path: Path) -> Profile:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PolicyError(f"could not load policy profile: {path}") from exc
    if raw.get("schema_version") != 1:
        raise PolicyError("schema_version must be 1")
    allowed_top_level = {
        "schema_version",
        "cluster",
        "controller",
        "submit_host",
        "allowed_nodes",
        "host_aliases",
        "capacity",
        "slurm",
        "cgroup",
        "docker",
        "accounting",
    }
    if set(raw) != allowed_top_level:
        raise PolicyError("profile has missing or unknown top-level fields")
    capacity = _table(raw, "capacity")
    slurm = _table(raw, "slurm")
    cgroup = _table(raw, "cgroup")
    docker = _table(raw, "docker")
    accounting = _table(raw, "accounting")
    required_slurm = set(_SLURM_KEYS.values())
    required_cgroup = set(_CGROUP_KEYS.values())
    required_capacity = {
        "slot_budget",
        "pending_slot_budget",
        "cpus_per_slot",
        "memory_mib_per_slot",
        "gpu_tres_per_slot",
        "job_pids_max",
    }
    required_accounting = {
        "parent_account",
        "child_accounts",
        "users",
        "fairshare",
        "qos",
        "qos_priority",
        "qos_max_wall",
        "qos_max_jobs_per_user",
        "qos_max_submit_jobs_per_user",
        "parent_group_tres",
    }
    if set(capacity) != required_capacity:
        raise PolicyError("capacity table has missing or unknown fields")
    if set(docker) != {"cgroup_driver"}:
        raise PolicyError("docker table has missing or unknown fields")
    if set(accounting) != required_accounting:
        raise PolicyError("accounting table has missing or unknown fields")
    if set(slurm) != required_slurm:
        raise PolicyError("slurm table has missing or unknown fields")
    if set(cgroup) != required_cgroup:
        raise PolicyError("cgroup table has missing or unknown fields")
    allowed_nodes = _strings(raw.get("allowed_nodes"), "allowed_nodes")
    host_aliases_raw = _table(raw, "host_aliases")
    host_aliases = {
        str(key): str(value).lower()
        for key, value in host_aliases_raw.items()
        if isinstance(key, str) and isinstance(value, str)
    }
    if set(host_aliases) != set(allowed_nodes):
        raise PolicyError("host_aliases must map every allowed Slurm node")
    if len(set(host_aliases.values())) != len(host_aliases):
        raise PolicyError("host_aliases canonical hostnames must be distinct")
    users = _strings(accounting.get("users"), "accounting.users")
    child_accounts = _strings(
        accounting.get("child_accounts"),
        "accounting.child_accounts",
    )
    if len(users) != 3 or len(child_accounts) != 3:
        raise PolicyError("exactly three sandbox users and child accounts are required")
    cluster = raw.get("cluster")
    controller = raw.get("controller")
    submit_host = raw.get("submit_host")
    if (
        not isinstance(cluster, str)
        or not cluster
        or not isinstance(controller, str)
        or not controller
        or not isinstance(submit_host, str)
        or not submit_host
    ):
        raise PolicyError("cluster, controller, and submit_host are required")
    names = (
        str(accounting.get("parent_account", "")),
        *child_accounts,
        str(accounting.get("qos", "")),
    )
    if any(_SAFE_NAME.fullmatch(item) is None for item in names):
        raise PolicyError("account and QoS names must be lowercase safe identifiers")
    if len(set(child_accounts)) != len(child_accounts):
        raise PolicyError("child accounts must be distinct")
    if len(set(users)) != len(users):
        raise PolicyError("sandbox users must be distinct")
    driver = docker.get("cgroup_driver")
    if driver != "cgroupfs":
        raise PolicyError("Docker cgroup driver must be cgroupfs for Slurm job paths")
    for key in required_cgroup - {"plugin"}:
        if cgroup[key] is not True:
            raise PolicyError(f"cgroup.{key} must stay fail-closed true")
    if cgroup["plugin"] != "autodetect":
        raise PolicyError("cgroup.plugin must be autodetect")
    fairshare = accounting.get("fairshare")
    if type(fairshare) is not int or fairshare <= 0:
        raise PolicyError("accounting.fairshare must be a positive integer")
    parent_group_tres = _strings(
        accounting.get("parent_group_tres"),
        "accounting.parent_group_tres",
    )
    for field in (
        "slot_budget",
        "pending_slot_budget",
        "cpus_per_slot",
        "memory_mib_per_slot",
        "job_pids_max",
    ):
        value = capacity[field]
        if type(value) is not int or value <= 0:
            raise PolicyError(f"capacity.{field} must be a positive integer")
    gpu_tres_per_slot = capacity["gpu_tres_per_slot"]
    if (
        not isinstance(gpu_tres_per_slot, int | float)
        or isinstance(gpu_tres_per_slot, bool)
        or gpu_tres_per_slot < 0
    ):
        raise PolicyError("capacity.gpu_tres_per_slot must be non-negative")
    return Profile(
        cluster=cluster,
        controller=controller,
        submit_host=submit_host,
        allowed_nodes=allowed_nodes,
        host_aliases=host_aliases,
        slot_budget=capacity["slot_budget"],
        pending_slot_budget=capacity["pending_slot_budget"],
        cpus_per_slot=capacity["cpus_per_slot"],
        memory_mib_per_slot=capacity["memory_mib_per_slot"],
        gpu_tres_per_slot=float(gpu_tres_per_slot),
        job_pids_max=capacity["job_pids_max"],
        slurm=slurm,
        cgroup=cgroup,
        docker_cgroup_driver=driver,
        parent_account=names[0],
        child_accounts=child_accounts,
        users=users,
        fairshare=fairshare,
        qos=names[-1],
        qos_priority=int(accounting["qos_priority"]),
        qos_max_wall=str(accounting["qos_max_wall"]),
        qos_max_jobs_per_user=int(accounting["qos_max_jobs_per_user"]),
        qos_max_submit_jobs_per_user=int(
            accounting["qos_max_submit_jobs_per_user"],
        ),
        parent_group_tres=parent_group_tres,
    )


def _slurm_value(value: str | int) -> str:
    return str(value)


def render_key_value_config(
    current: str,
    *,
    desired: Mapping[str, str],
) -> str:
    remaining = dict(desired)
    output: list[str] = []
    seen: set[str] = set()
    for line in current.splitlines():
        match = re.match(r"^\s*([A-Za-z][A-Za-z0-9]*)\s*=", line)
        if match is None or match.group(1) not in desired:
            output.append(line)
            continue
        key = match.group(1)
        if key in seen:
            continue
        output.append(f"{key}={desired[key]}")
        seen.add(key)
        remaining.pop(key, None)
    if output and output[-1]:
        output.append("")
    output.extend(f"{key}={value}" for key, value in remaining.items())
    return "\n".join(output).rstrip() + "\n"


def render_slurm_conf(current: str, profile: Profile) -> str:
    desired = {key: _slurm_value(profile.slurm[field]) for key, field in _SLURM_KEYS.items()}
    return render_key_value_config(current, desired=desired)


def render_cgroup_conf(profile: Profile) -> str:
    desired: dict[str, str] = {}
    for key, field in _CGROUP_KEYS.items():
        value = profile.cgroup[field]
        desired[key] = "yes" if value is True else "no" if value is False else str(value)
    return "".join(f"{key}={value}\n" for key, value in desired.items())


def render_daemon_json(current: str, profile: Profile) -> str:
    try:
        payload = json.loads(current) if current.strip() else {}
    except json.JSONDecodeError as exc:
        raise PolicyError("Docker daemon.json is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise PolicyError("Docker daemon.json must contain an object")
    raw_opts = payload.get("exec-opts", [])
    if not isinstance(raw_opts, list) or any(not isinstance(item, str) for item in raw_opts):
        raise PolicyError("Docker exec-opts must be a string array")
    opts = [item for item in raw_opts if not item.startswith("native.cgroupdriver=")]
    opts.append(f"native.cgroupdriver={profile.docker_cgroup_driver}")
    payload["exec-opts"] = opts
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def source_candidate_sha() -> str:
    repository = Path(__file__).resolve().parents[2]
    completed = subprocess.run(
        ("git", "-C", str(repository), "rev-parse", "HEAD"),
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    candidate = completed.stdout.strip().lower()
    if completed.returncode or _CANDIDATE_RE.fullmatch(candidate) is None:
        raise PolicyError("could not bind the Slurm policy to an exact candidate SHA")
    return candidate


def verify_source_candidate(candidate_sha: str) -> None:
    repository = Path(__file__).resolve().parents[2]
    if source_candidate_sha() != candidate_sha:
        raise PolicyError("requested candidate SHA does not match the policy checkout")
    completed = subprocess.run(
        (
            "git",
            "-C",
            str(repository),
            "diff",
            "--quiet",
            "HEAD",
            "--",
            "scripts/ops/developer_sandbox_slurm_policy.py",
            "scripts/ops/slurm_job_cgroup_guard.py",
            "deploy/slurm",
        ),
        check=False,
        timeout=5,
    )
    if completed.returncode != 0:
        raise PolicyError("policy checkout differs from the requested candidate SHA")


def _safe_path_chain(path: Path, *, leaf_directory: bool) -> Path:
    if not path.is_absolute():
        raise PolicyError("trusted path must be absolute")
    current = Path("/")
    parts = path.parts[1:]
    for index, part in enumerate(parts):
        current /= part
        try:
            metadata = current.lstat()
        except OSError as exc:
            raise PolicyError("trusted path chain is unavailable") from exc
        is_leaf = index == len(parts) - 1
        if stat.S_ISLNK(metadata.st_mode):
            raise PolicyError("trusted path chain must not contain symlinks")
        if is_leaf and not leaf_directory:
            if not stat.S_ISREG(metadata.st_mode):
                raise PolicyError("trusted file must be regular")
        elif not stat.S_ISDIR(metadata.st_mode):
            raise PolicyError("trusted path parent must be a directory")
        if stat.S_IMODE(metadata.st_mode) & 0o022:
            raise PolicyError("trusted path chain must not be group/world writable")
    return path


def _read_private_env(
    path: Path,
    *,
    expected_uid: int | None = None,
    expected_gid: int | None = None,
) -> dict[str, Any]:
    _safe_path_chain(path, leaf_directory=False)
    before = path.lstat()
    if stat.S_IMODE(before.st_mode) != 0o600:
        raise PolicyError("worker env must have exact mode 0600")
    if (
        expected_uid is not None
        and expected_gid is not None
        and (before.st_uid, before.st_gid) != (expected_uid, expected_gid)
    ):
        raise PolicyError("worker env owner does not match the batch UID/GID")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or (before.st_dev, before.st_ino) != (
            opened.st_dev,
            opened.st_ino,
        ):
            raise PolicyError("worker env inode changed while it was opened")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise PolicyError("worker env changed while it was read")
    finally:
        os.close(descriptor)
    payload = b"".join(chunks)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PolicyError("worker env must be UTF-8") from exc
    keys: set[str] = set()
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise PolicyError("worker env contains an invalid assignment")
        key, _separator, value = line.partition("=")
        if _ENV_KEY_RE.fullmatch(key) is None or not value:
            raise PolicyError("worker env contains an invalid key or empty value")
        if key in keys:
            raise PolicyError("worker env contains a duplicate key")
        keys.add(key)
    if not keys:
        raise PolicyError("worker env contains no assignments")
    return {
        "path": str(path),
        "device": opened.st_dev,
        "inode": opened.st_ino,
        "uid": opened.st_uid,
        "gid": opened.st_gid,
        "sha256": hashlib.sha256(payload).hexdigest(),
        "keys": sorted(keys),
    }


def _git_read(repository: Path, *args: str) -> bytes:
    environment = {
        **{key: value for key, value in os.environ.items() if not key.startswith("GIT_")},
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_NO_REPLACE_OBJECTS": "1",
    }
    completed = subprocess.run(
        (
            "git",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "core.untrackedCache=false",
            "-c",
            "core.attributesFile=/dev/null",
            "-c",
            "core.autocrlf=false",
            "-C",
            str(repository),
            *args,
        ),
        check=False,
        capture_output=True,
        env=environment,
        timeout=30,
    )
    if completed.returncode:
        raise PolicyError("candidate repository verification command failed")
    return completed.stdout


def _git_blob_sha(payload: bytes) -> str:
    header = f"blob {len(payload)}\0".encode()
    return hashlib.sha1(header + payload).hexdigest()


def _repository_paths(repository: Path) -> set[str]:
    found: set[str] = set()
    stack = [repository]
    while stack:
        directory = stack.pop()
        for child in directory.iterdir():
            if child.parent == repository and child.name == ".git":
                continue
            metadata = child.lstat()
            relative = child.relative_to(repository).as_posix()
            if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                stack.append(child)
            else:
                found.add(relative)
    return found


def _verify_git_metadata_path(repository: Path) -> None:
    marker = repository / ".git"
    try:
        metadata = marker.lstat()
    except OSError as exc:
        raise PolicyError("candidate Git metadata is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise PolicyError("candidate Git metadata path is unsafe")
    if stat.S_ISDIR(metadata.st_mode):
        _safe_path_chain(marker, leaf_directory=True)
        return
    if not stat.S_ISREG(metadata.st_mode):
        raise PolicyError("candidate Git metadata marker is invalid")
    descriptor = os.open(marker, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise PolicyError("candidate Git metadata marker inode changed")
        payload = os.read(descriptor, 4097)
        if len(payload) > 4096:
            raise PolicyError("candidate Git metadata marker is too large")
    finally:
        os.close(descriptor)
    try:
        line = payload.decode("utf-8").strip()
    except UnicodeDecodeError as exc:
        raise PolicyError("candidate Git metadata marker is invalid") from exc
    prefix = "gitdir: "
    if not line.startswith(prefix) or "\n" in line:
        raise PolicyError("candidate Git metadata marker is invalid")
    raw_git_dir = Path(line[len(prefix) :])
    git_dir = raw_git_dir if raw_git_dir.is_absolute() else repository / raw_git_dir
    normalized = Path(os.path.abspath(git_dir))
    _safe_path_chain(normalized, leaf_directory=True)


def _reject_git_attribute_filters(repository: Path, paths: Sequence[str]) -> None:
    for offset in range(0, len(paths), 200):
        batch = paths[offset : offset + 200]
        output = _git_read(
            repository,
            "check-attr",
            "-z",
            "filter",
            "working-tree-encoding",
            "--",
            *batch,
        ).split(b"\0")
        values = [item.decode("utf-8") for item in output if item]
        if len(values) % 3:
            raise PolicyError("candidate Git attribute readback is malformed")
        for index in range(0, len(values), 3):
            _path, attribute, value = values[index : index + 3]
            if value not in {"unspecified", "unset"}:
                raise PolicyError(
                    f"candidate tracked file has interfering Git {attribute} attributes",
                )


def verify_candidate_repository(
    repository: Path,
    *,
    candidate_sha: str,
) -> dict[str, Any]:
    if _CANDIDATE_RE.fullmatch(candidate_sha) is None:
        raise PolicyError("candidate SHA must be an exact lowercase Git SHA")
    _safe_path_chain(repository, leaf_directory=True)
    _verify_git_metadata_path(repository)
    if _git_read(repository, "rev-parse", "--verify", "HEAD").decode().strip() != candidate_sha:
        raise PolicyError("candidate repository HEAD drifted")
    tree = (
        _git_read(
            repository,
            "rev-parse",
            "--verify",
            f"{candidate_sha}^{{tree}}",
        )
        .decode()
        .strip()
    )
    if _CANDIDATE_RE.fullmatch(tree) is None:
        raise PolicyError("candidate repository tree identity is invalid")

    tree_rows = _git_read(
        repository,
        "ls-tree",
        "-rz",
        "--full-tree",
        candidate_sha,
    ).split(b"\0")
    tracked: dict[str, tuple[str, str]] = {}
    for raw in tree_rows:
        if not raw:
            continue
        metadata, separator, raw_path = raw.partition(b"\t")
        fields = metadata.decode().split()
        if not separator or len(fields) != 3 or fields[1] != "blob":
            raise PolicyError("candidate tree contains an unsupported entry")
        path = raw_path.decode("utf-8")
        tracked[path] = (fields[0], fields[2])
    if not tracked:
        raise PolicyError("candidate tree contains no tracked files")

    index_rows = _git_read(repository, "ls-files", "--stage", "-z").split(b"\0")
    indexed: set[str] = set()
    for raw in index_rows:
        if not raw:
            continue
        metadata, separator, raw_path = raw.partition(b"\t")
        fields = metadata.decode().split()
        if not separator or len(fields) != 3 or fields[2] != "0":
            raise PolicyError("candidate index contains a non-zero or invalid stage")
        indexed.add(raw_path.decode("utf-8"))
    if indexed != set(tracked):
        raise PolicyError("candidate index differs from the commit tree")
    for raw in _git_read(repository, "ls-files", "-v", "-z").split(b"\0"):
        if raw and (len(raw) < 3 or raw[:2] != b"H "):
            raise PolicyError("candidate index has skip-worktree or assume-unchanged flags")
    if _repository_paths(repository) != set(tracked):
        raise PolicyError("candidate repository contains extra or missing files")
    _reject_git_attribute_filters(repository, tuple(sorted(tracked)))

    for relative, (mode, expected_blob) in tracked.items():
        source_path = repository / relative
        file_metadata = source_path.lstat()
        if stat.S_IMODE(file_metadata.st_mode) & 0o022:
            raise PolicyError("candidate tracked file is group/world writable")
        if mode not in {"100644", "100755", "120000"}:
            raise PolicyError("candidate tracked file mode is unsupported")
        if mode == "120000":
            if not stat.S_ISLNK(file_metadata.st_mode):
                raise PolicyError("candidate symlink type differs from the commit tree")
            payload = os.readlink(source_path).encode()
        else:
            if stat.S_ISLNK(file_metadata.st_mode) or not stat.S_ISREG(
                file_metadata.st_mode,
            ):
                raise PolicyError("candidate tracked file type differs from the commit tree")
            executable = bool(stat.S_IMODE(file_metadata.st_mode) & 0o111)
            if executable is not (mode == "100755"):
                raise PolicyError("candidate tracked executable mode differs from the tree")
            descriptor = os.open(
                source_path,
                os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            )
            try:
                opened = os.fstat(descriptor)
                if (opened.st_dev, opened.st_ino) != (
                    file_metadata.st_dev,
                    file_metadata.st_ino,
                ):
                    raise PolicyError("candidate tracked file inode changed")
                chunks: list[bytes] = []
                while True:
                    chunk = os.read(descriptor, 64 * 1024)
                    if not chunk:
                        break
                    chunks.append(chunk)
                after = os.fstat(descriptor)
                if (
                    opened.st_size,
                    opened.st_mtime_ns,
                    opened.st_ino,
                ) != (after.st_size, after.st_mtime_ns, after.st_ino):
                    raise PolicyError("candidate tracked file changed while it was read")
                payload = b"".join(chunks)
            finally:
                os.close(descriptor)
        if _git_blob_sha(payload) != expected_blob:
            raise PolicyError("candidate raw tracked bytes differ from the commit tree")
    return {
        "path": str(repository),
        "candidate_sha": candidate_sha,
        "candidate_tree": tree,
        "tracked_files": len(tracked),
    }


def strict_candidate_binding(
    repository: Path,
    worker_env: Path,
    *,
    candidate_sha: str,
    expected_batch_uid: int | None = None,
    expected_batch_gid: int | None = None,
) -> dict[str, Any]:
    return {
        "repository": verify_candidate_repository(
            repository,
            candidate_sha=candidate_sha,
        ),
        "worker_env": _read_private_env(
            worker_env,
            expected_uid=expected_batch_uid,
            expected_gid=expected_batch_gid,
        ),
    }


def desired_files(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str | None = None,
) -> dict[Path, str]:
    candidate = candidate_sha or source_candidate_sha()
    if _CANDIDATE_RE.fullmatch(candidate) is None:
        raise PolicyError("candidate SHA must be an exact lowercase Git SHA")
    slurm_path = root / "etc/slurm/slurm.conf"
    daemon_path = root / "etc/docker/daemon.json"
    try:
        slurm_current = slurm_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise PolicyError(f"could not read {slurm_path}") from exc
    daemon_current = daemon_path.read_text(encoding="utf-8") if daemon_path.exists() else "{}\n"
    return {
        slurm_path: render_slurm_conf(slurm_current, profile),
        root / "etc/slurm/cgroup.conf": render_cgroup_conf(profile),
        daemon_path: render_daemon_json(daemon_current, profile),
        root / "usr/libexec/loom-slurm-job-cgroup-guard": (
            Path(__file__)
            .with_name("slurm_job_cgroup_guard.py")
            .read_text(
                encoding="utf-8",
            )
        ),
        root / "etc/loom/slurm-job-cgroup-guard.json": (
            json.dumps(
                {
                    "schema_version": 1,
                    "cluster": profile.cluster,
                    "controller": profile.controller,
                    "submit_host": profile.submit_host,
                    "allowed_nodes": sorted(
                        {
                            *(node.lower() for node in profile.allowed_nodes),
                            *profile.host_aliases.values(),
                        },
                    ),
                    "candidate_sha": candidate,
                    "pids_max": profile.job_pids_max,
                    "allowed_accounts": sorted(profile.child_accounts),
                    "poll_interval_seconds": 0.2,
                    "require_gpu_probe": profile.gpu_tres_per_slot > 0,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        ),
        root / "etc/systemd/system/loom-slurm-job-cgroup-guard.service": (
            Path(__file__).resolve().parents[2] / "deploy/slurm/loom-slurm-job-cgroup-guard.service"
        ).read_text(encoding="utf-8"),
    }


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _desired_file_mode(root: Path, path: Path) -> int:
    if path == root / "usr/libexec/loom-slurm-job-cgroup-guard":
        return 0o755
    if path == root / "etc/loom/slurm-job-cgroup-guard.json":
        return 0o600
    if path.exists():
        hardened = stat.S_IMODE(path.stat().st_mode) & ~0o022
        if hardened:
            return hardened
    return 0o644


def plan(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str | None = None,
) -> dict[str, Any]:
    candidate = candidate_sha or source_candidate_sha()
    files = desired_files(root, profile, candidate_sha=candidate)
    rows = []
    for path, desired in files.items():
        live = path.read_bytes() if path.exists() else b""
        live_metadata = path.stat() if path.exists() else None
        live_mode = stat.S_IMODE(live_metadata.st_mode) if live_metadata else None
        live_uid = live_metadata.st_uid if live_metadata else None
        desired_mode = _desired_file_mode(root, path)
        rows.append(
            {
                "path": str(path),
                "live_sha256": _sha256(live),
                "desired_sha256": _sha256(desired.encode()),
                "live_mode": live_mode,
                "desired_mode": desired_mode,
                "live_uid": live_uid,
                "desired_uid": 0 if root == Path("/") else None,
                "converged": live == desired.encode()
                and live_mode == desired_mode
                and (root != Path("/") or live_uid == 0),
            },
        )
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-policy-plan",
        "cluster": profile.cluster,
        "candidate_sha": candidate,
        "capacity": {
            "slot_budget": profile.slot_budget,
            "pending_slot_budget": profile.pending_slot_budget,
            "cpus_per_slot": profile.cpus_per_slot,
            "memory_mib_per_slot": profile.memory_mib_per_slot,
            "gpu_tres_per_slot": profile.gpu_tres_per_slot,
            "job_pids_max": profile.job_pids_max,
        },
        "mutation_authorized": False,
        "file_plan": {"converged": all(row["converged"] for row in rows)},
        "files": rows,
        "accounting_commands": accounting_commands(profile),
    }


def accounting_commands(profile: Profile) -> list[list[str]]:
    commands = [
        [
            "sacctmgr",
            "-i",
            "add",
            "qos",
            profile.qos,
        ],
        [
            "sacctmgr",
            "-i",
            "modify",
            "qos",
            "where",
            f"name={profile.qos}",
            "set",
            f"Priority={profile.qos_priority}",
            f"MaxWall={profile.qos_max_wall}",
            f"MaxJobsPerUser={profile.qos_max_jobs_per_user}",
            f"MaxSubmitJobsPerUser={profile.qos_max_submit_jobs_per_user}",
        ],
        [
            "sacctmgr",
            "-i",
            "add",
            "account",
            profile.parent_account,
            "Description=Loom developer sandboxes",
            "Organization=loom",
        ],
        [
            "sacctmgr",
            "-i",
            "modify",
            "account",
            "where",
            f"account={profile.parent_account}",
            "set",
            f"Fairshare={profile.fairshare}",
            f"GrpTRES={','.join(profile.parent_group_tres)}",
        ],
    ]
    for user, account in zip(profile.users, profile.child_accounts, strict=True):
        commands.extend(
            [
                [
                    "sacctmgr",
                    "-i",
                    "add",
                    "account",
                    account,
                    f"Parent={profile.parent_account}",
                    f"Description=Loom sandbox {user}",
                    "Organization=loom",
                ],
                [
                    "sacctmgr",
                    "-i",
                    "add",
                    "user",
                    user,
                    f"Account={account}",
                ],
                [
                    "sacctmgr",
                    "-i",
                    "modify",
                    "user",
                    "where",
                    f"name={user}",
                    f"account={account}",
                    "set",
                    f"Fairshare={profile.fairshare}",
                    f"QOS={profile.qos}",
                    f"DefaultQOS={profile.qos}",
                ],
            ],
        )
    return commands


def _run(argv: Sequence[str], *, timeout: float = 60) -> str:
    try:
        completed = subprocess.run(
            list(argv),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError(f"{argv[0]} failed safely before completion") from exc
    if completed.returncode:
        raise PolicyError(f"{argv[0]} failed safely with exit code {completed.returncode}")
    return completed.stdout


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, content: str, *, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    effective_mode = (
        mode if mode is not None else stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    )
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(effective_mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _prepare_private_directory(
    path: Path,
    *,
    enforce_root_ownership: bool,
    create: bool,
) -> None:
    if not path.is_absolute():
        raise PolicyError("private state directory must be absolute")
    if not enforce_root_ownership:
        if create:
            path.mkdir(parents=True, mode=0o700, exist_ok=True)
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise PolicyError("private state directory is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
        ):
            raise PolicyError("private state directory ownership is unsafe")
        if create and stat.S_IMODE(metadata.st_mode) != 0o700:
            path.chmod(0o700)
            metadata = path.lstat()
        if stat.S_IMODE(metadata.st_mode) != 0o700:
            raise PolicyError("private state directory must have exact mode 0700")
        return

    current = Path("/")
    for index, part in enumerate(path.parts[1:]):
        current /= part
        try:
            metadata = current.lstat()
        except FileNotFoundError:
            if not create:
                raise PolicyError("private state directory chain is unavailable") from None
            try:
                current.mkdir(mode=0o700)
                metadata = current.lstat()
            except OSError as exc:
                raise PolicyError("private state directory could not be created") from exc
        except OSError as exc:
            raise PolicyError("private state directory chain is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
        ):
            raise PolicyError("private state chain must be root-owned directories")
        is_leaf = index == len(path.parts[1:]) - 1
        if is_leaf:
            if create and stat.S_IMODE(metadata.st_mode) != 0o700:
                current.chmod(0o700)
                metadata = current.lstat()
            if stat.S_IMODE(metadata.st_mode) != 0o700:
                raise PolicyError("private state directory must have exact mode 0700")
        elif stat.S_IMODE(metadata.st_mode) & 0o022:
            raise PolicyError("private state directory chain is writable")


@contextmanager
def _persistent_private_lock(
    path: Path,
    *,
    enforce_root_ownership: bool,
) -> Iterator[None]:
    _prepare_private_directory(
        path.parent,
        enforce_root_ownership=enforce_root_ownership,
        create=True,
    )
    expected_uid, expected_gid = (0, 0) if enforce_root_ownership else (os.geteuid(), os.getegid())
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    created = False
    try:
        descriptor = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
    except FileExistsError:
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise PolicyError("private state lock could not be opened safely") from exc
    except OSError as exc:
        raise PolicyError("private state lock could not be created safely") from exc
    locked = False
    try:
        if created:
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            _fsync_directory(path.parent)
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or opened.st_nlink != 1
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
        ):
            raise PolicyError("private state lock inode is unsafe")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        locked = True
        try:
            linked = path.lstat()
        except OSError as exc:
            raise PolicyError("private state lock path is unavailable") from exc
        if (
            stat.S_ISLNK(linked.st_mode)
            or not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(linked.st_mode) != 0o600
            or linked.st_nlink != 1
            or linked.st_uid != expected_uid
            or linked.st_gid != expected_gid
            or linked.st_dev != opened.st_dev
            or linked.st_ino != opened.st_ino
        ):
            raise PolicyError("private state lock path changed during acquisition")
        yield
    finally:
        if locked:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _snapshot(root: Path, files: Mapping[Path, str]) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    state = root / _STATE_RELATIVE
    enforce_root_ownership = root == Path("/")
    _prepare_private_directory(
        state,
        enforce_root_ownership=enforce_root_ownership,
        create=True,
    )
    snapshots = state / "snapshots"
    _prepare_private_directory(
        snapshots,
        enforce_root_ownership=enforce_root_ownership,
        create=True,
    )
    snapshot = snapshots / timestamp
    try:
        snapshot.mkdir(mode=0o700)
    except OSError as exc:
        raise PolicyError("Slurm policy snapshot directory could not be created") from exc
    _prepare_private_directory(
        snapshot,
        enforce_root_ownership=enforce_root_ownership,
        create=False,
    )
    manifest: dict[str, Any] = {"schema_version": 1, "files": []}
    for path in files:
        relative = path.relative_to(root)
        target = snapshot / relative
        _prepare_private_directory(
            target.parent,
            enforce_root_ownership=enforce_root_ownership,
            create=True,
        )
        if path.exists():
            content = path.read_bytes()
            mode = stat.S_IMODE(path.stat().st_mode)
            _atomic_write(target, content.decode("utf-8"), mode=mode)
            manifest["files"].append(
                {"path": str(relative), "present": True, "mode": mode},
            )
        else:
            manifest["files"].append(
                {"path": str(relative), "present": False, "mode": None},
            )
    _atomic_write(
        snapshot / "manifest.json",
        json.dumps(manifest, sort_keys=True) + "\n",
        mode=0o600,
    )
    _fsync_directory(snapshot)
    return snapshot


def _canonical_host() -> str:
    return socket.gethostname().split(".", 1)[0].rstrip(".").lower()


def _slurm_node_for_host(profile: Profile, host: str) -> str | None:
    for slurm_node, canonical_host in profile.host_aliases.items():
        if host == canonical_host:
            return slurm_node
    return None


def _state_root(root: Path) -> Path:
    return root / _STATE_RELATIVE


def _journal_path(root: Path, profile: Profile) -> Path:
    return _state_root(root) / "transactions" / f"{profile.cluster}.json"


def _state_path_enforces_root(path: Path) -> bool:
    try:
        path.relative_to(Path("/") / _STATE_RELATIVE)
    except ValueError:
        return False
    return True


@contextmanager
def _domain_lock(root: Path, profile: Profile) -> Iterator[None]:
    lock_path = _state_root(root) / "locks" / f"{profile.cluster}.lock"
    with _persistent_private_lock(
        lock_path,
        enforce_root_ownership=root == Path("/"),
    ):
        yield


def _write_journal(path: Path, payload: Mapping[str, Any]) -> None:
    enforce_root_ownership = _state_path_enforces_root(path)
    _prepare_private_directory(
        path.parent,
        enforce_root_ownership=enforce_root_ownership,
        create=True,
    )
    _atomic_write(
        path,
        json.dumps(dict(payload), sort_keys=True, separators=(",", ":")) + "\n",
        mode=0o600,
    )
    metadata = path.lstat()
    expected_uid, expected_gid = (0, 0) if enforce_root_ownership else (os.geteuid(), os.getegid())
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
        or metadata.st_uid != expected_uid
        or metadata.st_gid != expected_gid
    ):
        raise PolicyError("durable Slurm policy journal write is unsafe")


def _load_journal(path: Path) -> dict[str, Any] | None:
    enforce_root_ownership = _state_path_enforces_root(path)
    _prepare_private_directory(
        path.parent,
        enforce_root_ownership=enforce_root_ownership,
        create=True,
    )
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise PolicyError("durable Slurm policy journal is unreadable") from exc
    try:
        opened = os.fstat(descriptor)
        linked = path.lstat()
        expected_uid, expected_gid = (
            (0, 0) if enforce_root_ownership else (os.geteuid(), os.getegid())
        )
        if (
            stat.S_ISLNK(linked.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or stat.S_IMODE(linked.st_mode) != 0o600
            or opened.st_nlink != 1
            or linked.st_nlink != 1
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or linked.st_uid != expected_uid
            or linked.st_gid != expected_gid
            or opened.st_dev != linked.st_dev
            or opened.st_ino != linked.st_ino
        ):
            raise PolicyError("durable Slurm policy journal is unsafe")
        chunks: list[bytes] = []
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            chunks.append(chunk)
            if sum(len(item) for item in chunks) > 1024 * 1024:
                raise PolicyError("durable Slurm policy journal is too large")
        payload = json.loads(b"".join(chunks).decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("durable Slurm policy journal is unreadable") from exc
    finally:
        os.close(descriptor)
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise PolicyError("durable Slurm policy journal is unsafe")
    return payload


def _advance_journal(path: Path, payload: dict[str, Any], phase: str) -> None:
    payload["phase"] = phase
    payload["updated_at"] = datetime.now(UTC).isoformat()
    _write_journal(path, payload)


def _validate_snapshot_path(root: Path, snapshot: Path) -> Path:
    expected_parent = _state_root(root) / "snapshots"
    if (
        not snapshot.is_absolute()
        or snapshot.parent != expected_parent
        or _SNAPSHOT_NAME_RE.fullmatch(snapshot.name) is None
    ):
        raise PolicyError("Slurm policy snapshot path is outside the canonical root")
    enforce_root_ownership = root == Path("/")
    _prepare_private_directory(
        expected_parent,
        enforce_root_ownership=enforce_root_ownership,
        create=False,
    )
    _prepare_private_directory(
        snapshot,
        enforce_root_ownership=enforce_root_ownership,
        create=False,
    )
    return snapshot


def _validate_accounting_snapshot_path(
    root: Path,
    snapshot: Path,
    accounting: Path,
) -> Path:
    validated_snapshot = _validate_snapshot_path(root, snapshot)
    expected = validated_snapshot / "accounting-cas.json"
    if accounting != expected:
        raise PolicyError("Loom accounting snapshot path is not canonical")
    return accounting


def _read_private_json_file(
    path: Path,
    *,
    enforce_root_ownership: bool,
    description: str,
) -> Any:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PolicyError(f"{description} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        linked = path.lstat()
        expected_uid, expected_gid = (
            (0, 0) if enforce_root_ownership else (os.geteuid(), os.getegid())
        )
        if (
            stat.S_ISLNK(linked.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or stat.S_IMODE(linked.st_mode) != 0o600
            or opened.st_nlink != 1
            or linked.st_nlink != 1
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or linked.st_uid != expected_uid
            or linked.st_gid != expected_gid
            or opened.st_dev != linked.st_dev
            or opened.st_ino != linked.st_ino
        ):
            raise PolicyError(f"{description} is unsafe")
        content = bytearray()
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > 1024 * 1024:
                raise PolicyError(f"{description} is too large")
        return json.loads(bytes(content).decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError(f"{description} is unreadable") from exc
    finally:
        os.close(descriptor)


def _restore_snapshot(root: Path, snapshot: Path) -> None:
    snapshot = _validate_snapshot_path(root, snapshot)
    manifest = _read_private_json_file(
        snapshot / "manifest.json",
        enforce_root_ownership=root == Path("/"),
        description="Slurm policy snapshot manifest",
    )
    if not isinstance(manifest, dict) or manifest.get("schema_version") != 1:
        raise PolicyError("Slurm policy snapshot manifest is invalid")
    rows = manifest.get("files")
    if not isinstance(rows, list):
        raise PolicyError("Slurm policy snapshot file list is invalid")
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("path"), str):
            raise PolicyError("Slurm policy snapshot row is invalid")
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise PolicyError("Slurm policy snapshot path escapes the root")
        target = root / relative
        if row.get("present") is True:
            source = snapshot / relative
            try:
                content = source.read_text(encoding="utf-8")
            except OSError as exc:
                raise PolicyError("Slurm policy snapshot content is unavailable") from exc
            mode = row.get("mode")
            if type(mode) is not int:
                raise PolicyError("Slurm policy snapshot mode is invalid")
            _atomic_write(target, content, mode=mode)
        elif row.get("present") is False:
            target.unlink(missing_ok=True)
            if target.parent.exists():
                _fsync_directory(target.parent)
        else:
            raise PolicyError("Slurm policy snapshot presence is invalid")


def _accounting_desired_state(profile: Profile) -> dict[str, Any]:
    return {
        "qos": {
            profile.qos: {
                "Priority": str(profile.qos_priority),
                "MaxWall": profile.qos_max_wall,
                "MaxJobsPU": str(profile.qos_max_jobs_per_user),
                "MaxSubmitJobsPU": str(profile.qos_max_submit_jobs_per_user),
            },
        },
        "accounts": {
            profile.parent_account: {
                "ParentName": "",
                "Fairshare": str(profile.fairshare),
                "GrpTRES": ",".join(profile.parent_group_tres),
            },
            **{
                account: {
                    "ParentName": profile.parent_account,
                }
                for account in profile.child_accounts
            },
        },
        "associations": {
            f"{user}|{account}": {
                "User": user,
                "Account": account,
                "Fairshare": str(profile.fairshare),
                "QOS": profile.qos,
                "DefaultQOS": profile.qos,
            }
            for user, account in zip(profile.users, profile.child_accounts, strict=True)
        },
    }


def _accounting_state(profile: Profile) -> dict[str, Any]:
    qos_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "qos",
                "where",
                f"name={profile.qos}",
                "format=Name,Priority,MaxWall,MaxJobsPU,MaxSubmitJobsPU",
            ),
        ).splitlines()
        if line.strip()
    ]
    if len(qos_rows) > 1 or any(len(row) < 5 for row in qos_rows):
        raise PolicyError("Loom QoS accounting snapshot is ambiguous")
    qos = (
        {}
        if not qos_rows
        else {
            profile.qos: {
                "Priority": qos_rows[0][1],
                "MaxWall": qos_rows[0][2],
                "MaxJobsPU": qos_rows[0][3],
                "MaxSubmitJobsPU": qos_rows[0][4],
            },
        }
    )

    account_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "account",
                "where",
                f"cluster={profile.cluster}",
                "format=Account,ParentName,Fairshare,GrpTRES",
            ),
        ).splitlines()
        if line.strip()
    ]
    identities = {profile.parent_account, *profile.child_accounts}
    accounts: dict[str, dict[str, str]] = {}
    for row in account_rows:
        if len(row) < 4:
            raise PolicyError("Slurm account accounting snapshot is malformed")
        if row[0] in identities:
            if row[0] in accounts:
                raise PolicyError("Loom account accounting snapshot is ambiguous")
            fields = {"ParentName": row[1]}
            if row[0] == profile.parent_account:
                fields.update({"Fairshare": row[2], "GrpTRES": row[3]})
            accounts[row[0]] = fields

    association_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "association",
                "where",
                f"cluster={profile.cluster}",
                "format=User,Account,Fairshare,QOS,DefaultQOS",
            ),
        ).splitlines()
        if line.strip()
    ]
    exact = set(zip(profile.users, profile.child_accounts, strict=True))
    associations: dict[str, dict[str, str]] = {}
    for row in association_rows:
        if len(row) < 5:
            raise PolicyError("Slurm association accounting snapshot is malformed")
        if (row[0], row[1]) in exact:
            key = f"{row[0]}|{row[1]}"
            if key in associations:
                raise PolicyError("Loom association accounting snapshot is ambiguous")
            associations[key] = {
                "User": row[0],
                "Account": row[1],
                "Fairshare": row[2],
                "QOS": row[3],
                "DefaultQOS": row[4],
            }
    return {"qos": qos, "accounts": accounts, "associations": associations}


def _accounting_snapshot(root: Path, profile: Profile, snapshot: Path) -> Path:
    snapshot = _validate_snapshot_path(root, snapshot)
    payload = {
        "schema_version": 1,
        "cluster": profile.cluster,
        "before": _accounting_state(profile),
        "desired": _accounting_desired_state(profile),
    }
    target = snapshot / "accounting-cas.json"
    _atomic_write(
        target,
        json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n",
        mode=0o600,
    )
    _read_private_json_file(
        target,
        enforce_root_ownership=root == Path("/"),
        description="Loom accounting CAS snapshot",
    )
    return target


def _load_accounting_snapshot(path: Path) -> dict[str, Any]:
    payload = _read_private_json_file(
        path,
        enforce_root_ownership=_state_path_enforces_root(path),
        description="Loom accounting CAS snapshot",
    )
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or not isinstance(payload.get("before"), dict)
        or not isinstance(payload.get("desired"), dict)
    ):
        raise PolicyError("Loom accounting CAS snapshot is unsafe")
    return payload


def _validate_accounting_cas(
    current: Mapping[str, Any],
    before: Mapping[str, Any],
    desired: Mapping[str, Any],
) -> None:
    for category in ("qos", "accounts", "associations"):
        current_rows = current.get(category)
        before_rows = before.get(category)
        desired_rows = desired.get(category)
        if not all(isinstance(rows, Mapping) for rows in (current_rows, before_rows, desired_rows)):
            raise PolicyError("Loom accounting CAS state schema is invalid")
        assert isinstance(current_rows, Mapping)
        assert isinstance(before_rows, Mapping)
        assert isinstance(desired_rows, Mapping)
        for identity, desired_fields in desired_rows.items():
            prior = before_rows.get(identity)
            observed = current_rows.get(identity)
            if prior is None and observed is None:
                continue
            if not isinstance(observed, Mapping) or not isinstance(desired_fields, Mapping):
                raise PolicyError("Loom accounting identity drifted during rollback")
            if prior is not None and not isinstance(prior, Mapping):
                raise PolicyError("Loom accounting snapshot identity is invalid")
            for field, desired_value in desired_fields.items():
                allowed = {desired_value}
                if isinstance(prior, Mapping):
                    allowed.add(prior.get(field))
                if observed.get(field) not in allowed:
                    raise PolicyError("Loom accounting field changed concurrently")


def _accounting_external_references(profile: Profile) -> set[str]:
    account_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "account",
                "where",
                f"cluster={profile.cluster}",
                "format=Account,ParentName",
            ),
        ).splitlines()
        if line.strip()
    ]
    if any(len(row) < 2 for row in account_rows):
        raise PolicyError("Slurm account reference readback is malformed")
    exact_accounts = {profile.parent_account, *profile.child_accounts}
    references = {
        row[1]
        for row in account_rows
        if len(row) >= 2 and row[1] in exact_accounts and row[0] not in exact_accounts
    }
    exact_associations = set(zip(profile.users, profile.child_accounts, strict=True))
    association_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "association",
                "where",
                f"cluster={profile.cluster}",
                "format=User,Account,QOS,DefaultQOS",
            ),
        ).splitlines()
        if line.strip()
    ]
    if any(len(row) < 4 for row in association_rows):
        raise PolicyError("Slurm association reference readback is malformed")
    for row in association_rows:
        if (row[0], row[1]) in exact_associations:
            continue
        if row[1] in exact_accounts:
            references.add(row[1])
        if profile.qos in _split_csv(row[2]) or row[3] == profile.qos:
            references.add(profile.qos)
    return references


def _require_accounting_state(
    profile: Profile,
    expected: Mapping[str, Any],
    *,
    phase: str,
) -> None:
    if _accounting_state(profile) != expected:
        raise PolicyError(f"Loom accounting state drifted {phase}")


def _checked_accounting_transition(
    profile: Profile,
    command: Sequence[str],
    expected: dict[str, Any],
    next_expected: dict[str, Any],
) -> dict[str, Any]:
    _require_accounting_state(profile, expected, phase="before mutation")
    _run(command)
    _require_accounting_state(profile, next_expected, phase="after mutation")
    return next_expected


def _checked_accounting_add(
    profile: Profile,
    command: Sequence[str],
    expected: dict[str, Any],
    *,
    category: str,
    identity: str,
    required_fields: Mapping[str, str],
) -> dict[str, Any]:
    _require_accounting_state(profile, expected, phase="before add")
    _run(command)
    observed = _accounting_state(profile)
    if identity in expected[category]:
        if observed != expected:
            raise PolicyError("Loom accounting add changed an existing identity")
        return expected
    row = observed.get(category, {}).get(identity)
    if not isinstance(row, Mapping) or any(
        row.get(field) != value for field, value in required_fields.items()
    ):
        raise PolicyError("Loom accounting add readback is incomplete")
    next_expected = deepcopy(expected)
    next_expected[category][identity] = dict(row)
    if observed != next_expected:
        raise PolicyError("Loom accounting add changed an unrelated identity")
    return next_expected


def _apply_accounting(
    profile: Profile,
    snapshot: Mapping[str, Any],
) -> None:
    before = snapshot["before"]
    desired = snapshot["desired"]
    if not isinstance(before, dict) or not isinstance(desired, dict):
        raise PolicyError("Loom accounting apply snapshot schema is invalid")
    expected = deepcopy(before)
    commands = iter(accounting_commands(profile))

    expected = _checked_accounting_add(
        profile,
        next(commands),
        expected,
        category="qos",
        identity=profile.qos,
        required_fields={},
    )
    next_expected = deepcopy(expected)
    next_expected["qos"][profile.qos] = deepcopy(desired["qos"][profile.qos])
    expected = _checked_accounting_transition(
        profile,
        next(commands),
        expected,
        next_expected,
    )

    expected = _checked_accounting_add(
        profile,
        next(commands),
        expected,
        category="accounts",
        identity=profile.parent_account,
        required_fields={"ParentName": ""},
    )
    next_expected = deepcopy(expected)
    next_expected["accounts"][profile.parent_account] = deepcopy(
        desired["accounts"][profile.parent_account],
    )
    expected = _checked_accounting_transition(
        profile,
        next(commands),
        expected,
        next_expected,
    )

    for user, account in zip(profile.users, profile.child_accounts, strict=True):
        expected = _checked_accounting_add(
            profile,
            next(commands),
            expected,
            category="accounts",
            identity=account,
            required_fields={"ParentName": profile.parent_account},
        )
        association = f"{user}|{account}"
        expected = _checked_accounting_add(
            profile,
            next(commands),
            expected,
            category="associations",
            identity=association,
            required_fields={"User": user, "Account": account},
        )
        next_expected = deepcopy(expected)
        next_expected["associations"][association] = deepcopy(
            desired["associations"][association],
        )
        expected = _checked_accounting_transition(
            profile,
            next(commands),
            expected,
            next_expected,
        )
    try:
        next(commands)
    except StopIteration:
        pass
    else:
        raise PolicyError("Loom accounting command plan has unexpected mutations")
    if expected != desired:
        raise PolicyError("Loom accounting apply did not converge to the desired state")


def _restore_accounting(profile: Profile, path: Path) -> None:
    snapshot = _load_accounting_snapshot(path)
    if snapshot.get("cluster") != profile.cluster:
        raise PolicyError("Loom accounting CAS snapshot cluster drifted")
    before = snapshot["before"]
    desired = snapshot["desired"]
    current = _accounting_state(profile)
    _validate_accounting_cas(current, before, desired)

    before_accounts = before["accounts"]
    created_identities = {
        account
        for account in (*profile.child_accounts, profile.parent_account)
        if account not in before_accounts and account in current["accounts"]
    }
    if profile.qos not in before["qos"] and profile.qos in current["qos"]:
        created_identities.add(profile.qos)
    if created_identities and (_accounting_external_references(profile) & created_identities):
        raise PolicyError("new Loom accounting identities have external references")

    expected = deepcopy(current)
    before_associations = before["associations"]
    for key, desired_row in desired["associations"].items():
        if key in before_associations:
            row = before_associations[key]
            next_expected = deepcopy(expected)
            next_expected["associations"][key] = deepcopy(row)
            expected = _checked_accounting_transition(
                profile,
                (
                    "sacctmgr",
                    "-i",
                    "modify",
                    "user",
                    "where",
                    f"name={row['User']}",
                    f"account={row['Account']}",
                    f"cluster={profile.cluster}",
                    "set",
                    f"Fairshare={row['Fairshare']}",
                    f"QOS={row['QOS']}",
                    f"DefaultQOS={row['DefaultQOS']}",
                ),
                expected,
                next_expected,
            )
        elif key in expected["associations"]:
            next_expected = deepcopy(expected)
            next_expected["associations"].pop(key)
            expected = _checked_accounting_transition(
                profile,
                (
                    "sacctmgr",
                    "-i",
                    "delete",
                    "user",
                    "where",
                    f"name={desired_row['User']}",
                    f"account={desired_row['Account']}",
                    f"cluster={profile.cluster}",
                ),
                expected,
                next_expected,
            )

    for account in (*profile.child_accounts, profile.parent_account):
        if account in before_accounts:
            row = before_accounts[account]
            fields = [f"Parent={row['ParentName']}"]
            if account == profile.parent_account:
                fields.extend(
                    (
                        f"Fairshare={row['Fairshare']}",
                        f"GrpTRES={row['GrpTRES']}",
                    ),
                )
            next_expected = deepcopy(expected)
            next_expected["accounts"][account] = deepcopy(row)
            expected = _checked_accounting_transition(
                profile,
                (
                    "sacctmgr",
                    "-i",
                    "modify",
                    "account",
                    "where",
                    f"account={account}",
                    f"cluster={profile.cluster}",
                    "set",
                    *fields,
                ),
                expected,
                next_expected,
            )
        elif account in expected["accounts"]:
            _require_accounting_state(
                profile,
                expected,
                phase="before external-reference readback",
            )
            if account in _accounting_external_references(profile):
                raise PolicyError("new Loom account gained an external reference")
            next_expected = deepcopy(expected)
            next_expected["accounts"].pop(account)
            expected = _checked_accounting_transition(
                profile,
                (
                    "sacctmgr",
                    "-i",
                    "delete",
                    "account",
                    "where",
                    f"account={account}",
                    f"cluster={profile.cluster}",
                ),
                expected,
                next_expected,
            )
    before_qos = before["qos"]
    if profile.qos in before_qos:
        row = before_qos[profile.qos]
        next_expected = deepcopy(expected)
        next_expected["qos"][profile.qos] = deepcopy(row)
        expected = _checked_accounting_transition(
            profile,
            (
                "sacctmgr",
                "-i",
                "modify",
                "qos",
                "where",
                f"name={profile.qos}",
                "set",
                f"Priority={row['Priority']}",
                f"MaxWall={row['MaxWall']}",
                f"MaxJobsPerUser={row['MaxJobsPU']}",
                f"MaxSubmitJobsPerUser={row['MaxSubmitJobsPU']}",
            ),
            expected,
            next_expected,
        )
    elif profile.qos in expected["qos"]:
        _require_accounting_state(
            profile,
            expected,
            phase="before external-reference readback",
        )
        if profile.qos in _accounting_external_references(profile):
            raise PolicyError("new Loom QoS gained an external reference")
        next_expected = deepcopy(expected)
        next_expected["qos"].pop(profile.qos)
        expected = _checked_accounting_transition(
            profile,
            (
                "sacctmgr",
                "-i",
                "delete",
                "qos",
                "where",
                f"name={profile.qos}",
            ),
            expected,
            next_expected,
        )
    if expected != before:
        raise PolicyError("Loom accounting CAS restore readback drifted")


def _restart_services(profile: Profile, slurm_node: str) -> None:
    _run(("systemctl", "daemon-reload"))
    _run(("systemctl", "enable", "loom-slurm-job-cgroup-guard.service"))
    _run(("systemctl", "restart", "docker"))
    _run(("systemctl", "restart", "slurmd"))
    _run(("systemctl", "restart", "loom-slurm-job-cgroup-guard.service"))
    if slurm_node == profile.controller:
        _run(("systemctl", "restart", "slurmctld"))
    _run(("scontrol", "reconfigure"))


def _run_status(argv: Sequence[str]) -> tuple[int, str]:
    try:
        completed = subprocess.run(
            list(argv),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError(f"{argv[0]} status readback failed safely") from exc
    return completed.returncode, completed.stdout


def _restore_services(root: Path, profile: Profile, slurm_node: str) -> None:
    guard_unit = root / "etc/systemd/system/loom-slurm-job-cgroup-guard.service"
    _run(("systemctl", "daemon-reload"))
    if guard_unit.exists():
        _run(("systemctl", "enable", "loom-slurm-job-cgroup-guard.service"))
    else:
        _run_status(
            ("systemctl", "disable", "--now", "loom-slurm-job-cgroup-guard.service"),
        )
        active_code, _active = _run_status(
            ("systemctl", "is-active", "loom-slurm-job-cgroup-guard.service"),
        )
        enabled_code, _enabled = _run_status(
            ("systemctl", "is-enabled", "loom-slurm-job-cgroup-guard.service"),
        )
        if active_code == 0 or enabled_code == 0:
            raise PolicyError("restored cgroup guard should be inactive and disabled")
    _run(("systemctl", "restart", "docker"))
    _run(("systemctl", "restart", "slurmd"))
    if guard_unit.exists():
        _run(("systemctl", "restart", "loom-slurm-job-cgroup-guard.service"))
    if slurm_node == profile.controller:
        _run(("systemctl", "restart", "slurmctld"))
    _run(("scontrol", "reconfigure"))


def _snapshot_readback(root: Path, snapshot: Path) -> dict[str, Any]:
    snapshot = _validate_snapshot_path(root, snapshot)
    manifest = _read_private_json_file(
        snapshot / "manifest.json",
        enforce_root_ownership=root == Path("/"),
        description="Slurm policy snapshot manifest",
    )
    rows = manifest.get("files") if isinstance(manifest, dict) else None
    if not isinstance(rows, list):
        raise PolicyError("Slurm policy snapshot file list is invalid")
    checked: list[str] = []
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("path"), str):
            raise PolicyError("Slurm policy snapshot row is invalid")
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise PolicyError("Slurm policy snapshot path escapes the root")
        live = root / relative
        archived = snapshot / relative
        if row.get("present") is True:
            try:
                if live.read_bytes() != archived.read_bytes():
                    raise PolicyError("restored Slurm policy file readback drifted")
            except OSError as exc:
                raise PolicyError("restored Slurm policy file is unavailable") from exc
        elif row.get("present") is False:
            if live.exists():
                raise PolicyError("restored Slurm policy file should be absent")
        else:
            raise PolicyError("Slurm policy snapshot presence is invalid")
        checked.append(str(relative))
    return {"converged": True, "snapshot": str(snapshot), "files": checked}


def _accounting_snapshot_matches(profile: Profile, snapshot: Path) -> None:
    payload = _load_accounting_snapshot(snapshot)
    if payload.get("cluster") != profile.cluster:
        raise PolicyError("restored Loom accounting snapshot cluster drifted")
    if _accounting_state(profile) != payload["before"]:
        raise PolicyError("restored Loom accounting readback drifted")


def _parse_key_values(raw: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for line in raw.splitlines():
        match = re.match(r"^\s*([A-Za-z][A-Za-z0-9]*)\s*=\s*(.*?)\s*$", line)
        if match is not None:
            parsed[match.group(1)] = match.group(2)
    return parsed


def _split_csv(value: str) -> set[str]:
    return {item.strip() for item in value.split(",") if item.strip()}


def _accounting_readback(profile: Profile) -> dict[str, Any]:
    qos_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "qos",
                "where",
                f"name={profile.qos}",
                "format=Name,Priority,MaxWall,MaxJobsPU,MaxSubmitJobsPU",
            ),
        ).splitlines()
        if line.strip()
    ]
    if len(qos_rows) != 1 or len(qos_rows[0]) < 5:
        raise PolicyError("live Slurm QoS readback is missing or ambiguous")
    qos = qos_rows[0][:5]
    expected_qos = [
        profile.qos,
        str(profile.qos_priority),
        profile.qos_max_wall,
        str(profile.qos_max_jobs_per_user),
        str(profile.qos_max_submit_jobs_per_user),
    ]
    if qos != expected_qos:
        raise PolicyError("live Slurm QoS readback drifted")

    account_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "account",
                "where",
                f"cluster={profile.cluster}",
                "format=Account,ParentName,Fairshare,GrpTRES",
            ),
        ).splitlines()
        if line.strip()
    ]
    accounts = {row[0]: row for row in account_rows if len(row) >= 4}
    expected_accounts = {profile.parent_account, *profile.child_accounts}
    if not expected_accounts.issubset(accounts):
        raise PolicyError("live Slurm account hierarchy is incomplete")
    parent = accounts[profile.parent_account]
    if parent[2] != str(profile.fairshare) or _split_csv(parent[3]) != set(
        profile.parent_group_tres,
    ):
        raise PolicyError("live Slurm parent account fair-share or TRES drifted")
    for child in profile.child_accounts:
        row = accounts[child]
        if row[1] != profile.parent_account:
            raise PolicyError("live Slurm child account parent drifted")

    association_rows = [
        line.split("|")
        for line in _run(
            (
                "sacctmgr",
                "-nP",
                "show",
                "association",
                "where",
                f"cluster={profile.cluster}",
                "format=User,Account,Fairshare,QOS,DefaultQOS",
            ),
        ).splitlines()
        if line.strip()
    ]
    associations = {(row[0], row[1]): row for row in association_rows if len(row) >= 5 and row[0]}
    for user, account in zip(profile.users, profile.child_accounts, strict=True):
        association = associations.get((user, account))
        if (
            association is None
            or association[2] != str(profile.fairshare)
            or profile.qos not in _split_csv(association[3])
            or association[4] != profile.qos
        ):
            raise PolicyError("live Slurm user association or fair-share drifted")
    return {
        "qos": profile.qos,
        "accounts": sorted(expected_accounts),
        "associations": [
            {"user": user, "account": account}
            for user, account in zip(profile.users, profile.child_accounts, strict=True)
        ],
    }


def _guard_status_readback(
    root: Path,
    *,
    candidate_sha: str,
    expected_config_sha256: str,
    require_probe: bool,
) -> dict[str, Any]:
    path = root / _GUARD_STATUS_RELATIVE
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyError("cgroup guard status is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not isinstance(payload, dict)
        or payload.get("schema_version") != 1
    ):
        raise PolicyError("cgroup guard status is unsafe")
    try:
        observed = datetime.fromisoformat(str(payload["timestamp"]))
    except (KeyError, ValueError) as exc:
        raise PolicyError("cgroup guard status timestamp is invalid") from exc
    if observed.tzinfo is None or datetime.now(UTC) - observed.astimezone(UTC) > (
        _GUARD_STATUS_MAX_AGE
    ):
        raise PolicyError("cgroup guard status is stale")
    if (
        payload.get("candidate_sha") != candidate_sha
        or payload.get("config_sha256") != expected_config_sha256
        or payload.get("failed") != 0
        or payload.get("failures") != []
    ):
        raise PolicyError("cgroup guard status failed or drifted")
    if require_probe:
        probe = payload.get("resource_probe")
        if not isinstance(probe, dict) or probe.get("candidate_sha") != candidate_sha:
            raise PolicyError("cgroup guard lacks a candidate-bound live job probe")
        try:
            probe_observed = datetime.fromisoformat(str(probe["observed_at"]))
        except (KeyError, ValueError) as exc:
            raise PolicyError("cgroup guard job probe timestamp is invalid") from exc
        if (
            probe_observed.tzinfo is None
            or datetime.now(UTC) - probe_observed.astimezone(UTC) > _ALLOCATION_PROBE_MAX_AGE
        ):
            raise PolicyError("cgroup guard job resource probe is stale")
    return payload


def _wait_for_guard_status(
    root: Path,
    *,
    candidate_sha: str,
    expected_config_sha256: str,
    require_probe: bool,
) -> dict[str, Any]:
    deadline = time.monotonic() + 10
    last_error: PolicyError | None = None
    while time.monotonic() < deadline:
        try:
            return _guard_status_readback(
                root,
                candidate_sha=candidate_sha,
                expected_config_sha256=expected_config_sha256,
                require_probe=require_probe,
            )
        except PolicyError as exc:
            last_error = exc
            time.sleep(0.25)
    raise PolicyError("cgroup guard did not publish matching fresh status") from last_error


def _allocation_probe_path(root: Path, profile: Profile, candidate_sha: str) -> Path:
    return root / _ALLOCATION_PROBE_RELATIVE / profile.cluster / f"{candidate_sha}.json"


def _allocation_inflight_path(root: Path, profile: Profile, candidate_sha: str) -> Path:
    return root / _ALLOCATION_PROBE_RELATIVE / profile.cluster / f"{candidate_sha}.inflight.json"


def _allocation_lock_path(root: Path, profile: Profile, candidate_sha: str) -> Path:
    return root / _ALLOCATION_PROBE_RELATIVE / profile.cluster / f"{candidate_sha}.lock"


@contextmanager
def _allocation_probe_lock(
    root: Path,
    profile: Profile,
    candidate_sha: str,
    *,
    enforce_root_ownership: bool = True,
) -> Iterator[None]:
    if _CANDIDATE_RE.fullmatch(candidate_sha) is None:
        raise PolicyError("allocation probe lock candidate SHA is invalid")
    path = _allocation_lock_path(root, profile, candidate_sha)
    with _persistent_private_lock(
        path,
        enforce_root_ownership=enforce_root_ownership,
    ):
        yield


def _invalidate_allocation_artifact(root: Path, profile: Profile, candidate_sha: str) -> None:
    path = _allocation_probe_path(root, profile, candidate_sha)
    _require_root_private_directory(path.parent)
    path.unlink(missing_ok=True)
    _fsync_directory(path.parent)


def _require_root_private_directory(path: Path) -> None:
    _prepare_private_directory(
        path,
        enforce_root_ownership=True,
        create=True,
    )


def _write_allocation_state(
    path: Path,
    payload: Mapping[str, Any],
    *,
    enforce_root_ownership: bool = True,
) -> None:
    if enforce_root_ownership:
        _require_root_private_directory(path.parent)
    else:
        path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        path.parent.chmod(0o700)
    _atomic_write(
        path,
        json.dumps(dict(payload), sort_keys=True, separators=(",", ":")) + "\n",
        mode=0o600,
    )
    metadata = path.lstat()
    if enforce_root_ownership and (metadata.st_uid != 0 or metadata.st_gid != 0):
        raise PolicyError("allocation evidence must be root:root")


def _load_allocation_state(
    path: Path,
    *,
    enforce_root_ownership: bool = True,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    if enforce_root_ownership:
        _require_root_private_directory(path.parent)
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyError("allocation inflight journal is unreadable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (enforce_root_ownership and metadata.st_uid != 0)
        or (enforce_root_ownership and metadata.st_gid != 0)
        or not isinstance(payload, dict)
        or payload.get("schema_version") != 1
    ):
        raise PolicyError("allocation inflight journal is unsafe")
    return payload


def _parse_sacct_rows(raw: str) -> list[list[str]]:
    rows = [line.split("|") for line in raw.splitlines() if line.strip()]
    if any(len(row) < 6 for row in rows):
        raise PolicyError("Slurm allocation probe accounting output is malformed")
    return rows


def _probe_accounting_rows(job_id: str) -> list[list[str]]:
    output = _run(
        (
            "sacct",
            "-nP",
            "-j",
            job_id,
            "--format=JobIDRaw,JobName,State,NodeList,AllocTRES,Account",
        ),
        timeout=15,
    )
    if not output.strip():
        return []
    return _parse_sacct_rows(output)


def _base_job_state(rows: Sequence[Sequence[str]], job_id: str) -> str | None:
    base = next((row for row in rows if row[0] == job_id), None)
    if base is None:
        return None
    return base[2].split("+", 1)[0]


def _poll_probe_terminal(
    job_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float = _ALLOCATION_POLL_SECONDS,
) -> list[list[str]]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        rows = _probe_accounting_rows(job_id)
        state = _base_job_state(rows, job_id)
        if state in _TERMINAL_JOB_STATES:
            return rows
        time.sleep(poll_seconds)
    raise PolicyError("allocation probe did not reach a terminal state before timeout")


def _finish_allocation_inflight(
    path: Path,
    payload: dict[str, Any],
    phase: str,
    *,
    enforce_root_ownership: bool,
) -> None:
    payload["phase"] = phase
    payload["updated_at"] = datetime.now(UTC).isoformat()
    _write_allocation_state(
        path,
        payload,
        enforce_root_ownership=enforce_root_ownership,
    )
    history = path.with_name(
        f"{payload['candidate_sha']}.{payload['job_id']}.{phase}.json",
    )
    os.replace(path, history)
    _fsync_directory(path.parent)


def _cancel_allocation_job(
    path: Path,
    payload: dict[str, Any],
    profile: Profile,
    *,
    enforce_root_ownership: bool,
) -> None:
    job_id = str(payload.get("job_id", ""))
    if re.fullmatch(r"[1-9][0-9]*", job_id) is None:
        raise PolicyError("allocation inflight journal job ID is invalid")
    try:
        observed = _probe_accounting_rows(job_id)
    except PolicyError:
        observed = []
    observed_state = _base_job_state(observed, job_id)
    if observed_state in _TERMINAL_JOB_STATES:
        payload["terminal_state"] = observed_state
        _finish_allocation_inflight(
            path,
            payload,
            "terminal",
            enforce_root_ownership=enforce_root_ownership,
        )
        return
    payload["phase"] = "cancel_requested"
    _write_allocation_state(
        path,
        payload,
        enforce_root_ownership=enforce_root_ownership,
    )
    try:
        _run(("scancel", f"--clusters={profile.cluster}", job_id), timeout=30)
    except PolicyError:
        payload["phase"] = "cancel_failed"
        _write_allocation_state(
            path,
            payload,
            enforce_root_ownership=enforce_root_ownership,
        )
        raise
    try:
        rows = _poll_probe_terminal(job_id, timeout_seconds=60)
    except PolicyError:
        payload["phase"] = "cancel_readback_failed"
        _write_allocation_state(
            path,
            payload,
            enforce_root_ownership=enforce_root_ownership,
        )
        raise
    state = _base_job_state(rows, job_id)
    if state not in _TERMINAL_JOB_STATES:
        raise PolicyError("cancelled allocation probe lacks terminal readback")
    payload["terminal_state"] = state
    _finish_allocation_inflight(
        path,
        payload,
        "cancelled",
        enforce_root_ownership=enforce_root_ownership,
    )


def _poll_allocation_or_cancel(
    path: Path,
    payload: dict[str, Any],
    profile: Profile,
    *,
    timeout_seconds: float,
    enforce_root_ownership: bool,
) -> list[list[str]]:
    try:
        return _poll_probe_terminal(
            str(payload["job_id"]),
            timeout_seconds=timeout_seconds,
        )
    except Exception:
        _cancel_allocation_job(
            path,
            payload,
            profile,
            enforce_root_ownership=enforce_root_ownership,
        )
        raise


def _recover_allocation_probe(
    path: Path,
    profile: Profile,
    *,
    candidate_sha: str,
    job_name: str,
    enforce_root_ownership: bool,
) -> None:
    recovered_job_id: str | None = None
    payload = _load_allocation_state(
        path,
        enforce_root_ownership=enforce_root_ownership,
    )
    if payload is not None:
        if (
            payload.get("candidate_sha") != candidate_sha
            or payload.get("cluster") != profile.cluster
            or payload.get("controller") != profile.controller
            or payload.get("submit_host") != profile.submit_host
            or payload.get("job_name") != job_name
        ):
            raise PolicyError("allocation inflight journal binding drifted")
        recovered_job_id = str(payload.get("job_id", ""))
        _cancel_allocation_job(
            path,
            payload,
            profile,
            enforce_root_ownership=enforce_root_ownership,
        )
    orphan_rows = [
        line.split("|")
        for line in _run(
            ("squeue", "-h", "-n", job_name, "-o", "%A|%j|%T"),
            timeout=15,
        ).splitlines()
        if line.strip()
    ]
    if recovered_job_id is not None:
        orphan_rows = [row for row in orphan_rows if row[0] != recovered_job_id]
    if not orphan_rows:
        return
    if (
        len(orphan_rows) != 1
        or len(orphan_rows[0]) < 3
        or orphan_rows[0][1] != job_name
        or re.fullmatch(r"[1-9][0-9]*", orphan_rows[0][0]) is None
    ):
        raise PolicyError("unjournaled allocation probe jobs are ambiguous")
    recovered = {
        "schema_version": 1,
        "candidate_sha": candidate_sha,
        "cluster": profile.cluster,
        "controller": profile.controller,
        "submit_host": profile.submit_host,
        "job_id": orphan_rows[0][0],
        "job_name": job_name,
        "phase": "recovered_unjournaled",
        "created_at": datetime.now(UTC).isoformat(),
    }
    _write_allocation_state(
        path,
        recovered,
        enforce_root_ownership=enforce_root_ownership,
    )
    _cancel_allocation_job(
        path,
        recovered,
        profile,
        enforce_root_ownership=enforce_root_ownership,
    )


def _positive_gpu_tres(value: str) -> bool:
    for item in value.split(","):
        key, separator, raw = item.partition("=")
        if separator and (key == "gres/gpu" or key.startswith("gres/gpu:")):
            try:
                return float(raw) > 0
            except ValueError:
                return False
    return False


def _run_allocation_probe_transaction(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    batch_uid: int,
    batch_gid: int,
    timeout_seconds: float = _ALLOCATION_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    if root != Path("/") or os.geteuid() != 0:
        raise PolicyError("allocation-side Slurm probe requires the live root")
    if batch_uid < 0 or batch_gid < 0:
        raise PolicyError("allocation probe batch UID/GID must be non-negative")
    if not 1 <= timeout_seconds <= 600:
        raise PolicyError("allocation probe timeout must be between 1 and 600 seconds")
    verify_source_candidate(candidate_sha)
    try:
        batch_identity = pwd.getpwnam(profile.users[0])
    except KeyError as exc:
        raise PolicyError("allocation probe batch user is unavailable") from exc
    if (batch_identity.pw_uid, batch_identity.pw_gid) != (batch_uid, batch_gid):
        raise PolicyError("submit-host batch UID/GID differs from the expected identity")
    binding = strict_candidate_binding(
        candidate_root,
        worker_env,
        candidate_sha=candidate_sha,
        expected_batch_uid=batch_uid,
        expected_batch_gid=batch_gid,
    )
    host = _canonical_host()
    submit_node = _slurm_node_for_host(profile, host)
    if submit_node != profile.submit_host:
        raise PolicyError("allocation probe must run from the profile's exact submit host")
    live_config = _parse_key_values(_run(("scontrol", "show", "config")))
    if live_config.get("ClusterName") != profile.cluster:
        raise PolicyError("allocation probe reached the wrong Slurm cluster")
    controllers = _split_csv(live_config.get("SlurmctldHost", ""))
    if controllers and profile.controller.lower() not in {item.lower() for item in controllers}:
        raise PolicyError("allocation probe reached the wrong Slurm controller")

    job_name = f"loom-policy-{candidate_sha}-probe"
    inflight_path = _allocation_inflight_path(root, profile, candidate_sha)
    _invalidate_allocation_artifact(root, profile, candidate_sha)
    _recover_allocation_probe(
        inflight_path,
        profile,
        candidate_sha=candidate_sha,
        job_name=job_name,
        enforce_root_ownership=True,
    )
    arguments = [
        "sbatch",
        "--parsable",
        f"--job-name={job_name}",
        f"--uid={profile.users[0]}",
        f"--account={profile.child_accounts[0]}",
        f"--qos={profile.qos}",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=1",
        "--mem=256M",
        "--time=00:02:00",
        f"--comment=loom-cgroup-v1:pids={profile.job_pids_max}",
        f"--export=LOOM_POLICY_CANDIDATE_SHA={candidate_sha}",
    ]
    if profile.gpu_tres_per_slot > 0:
        arguments.append("--gres=gpu:1")
    arguments.append(
        (
            "--wrap=/usr/bin/srun --nodes=1 --ntasks=1 /bin/sh -c "
            f'\'test "$(/usr/bin/id -u)" -eq {batch_uid} && '
            f'test "$(/usr/bin/id -g)" -eq {batch_gid} && /bin/sleep 2\''
        ),
    )
    try:
        output = _run(tuple(arguments), timeout=30)
    except Exception:
        _recover_allocation_probe(
            inflight_path,
            profile,
            candidate_sha=candidate_sha,
            job_name=job_name,
            enforce_root_ownership=True,
        )
        raise
    job_ids = [
        match.group(1)
        for line in output.splitlines()
        if (match := re.fullmatch(r"([1-9][0-9]*)(?:;[A-Za-z0-9_.-]+)?", line.strip()))
    ]
    if len(job_ids) != 1:
        _recover_allocation_probe(
            inflight_path,
            profile,
            candidate_sha=candidate_sha,
            job_name=job_name,
            enforce_root_ownership=True,
        )
        raise PolicyError("allocation probe did not return one unambiguous job ID")
    job_id = job_ids[0]
    inflight: dict[str, Any] = {
        "schema_version": 1,
        "created_at": datetime.now(UTC).isoformat(),
        "candidate_sha": candidate_sha,
        "cluster": profile.cluster,
        "controller": profile.controller,
        "submit_host": profile.submit_host,
        "job_id": job_id,
        "job_name": job_name,
        "batch_uid": batch_uid,
        "batch_gid": batch_gid,
        "phase": "submitted",
    }
    try:
        _write_allocation_state(
            inflight_path,
            inflight,
            enforce_root_ownership=True,
        )
    except Exception as journal_exc:
        _run(("scancel", f"--clusters={profile.cluster}", job_id), timeout=30)
        terminal = _poll_probe_terminal(job_id, timeout_seconds=60)
        if _base_job_state(terminal, job_id) not in _TERMINAL_JOB_STATES:
            raise PolicyError(
                "unjournaled allocation probe lacks terminal readback",
            ) from journal_exc
        raise
    rows = _poll_allocation_or_cancel(
        inflight_path,
        inflight,
        profile,
        timeout_seconds=timeout_seconds,
        enforce_root_ownership=True,
    )
    try:
        base = next((row for row in rows if row[0] == job_id), None)
        srun = next((row for row in rows if row[0].startswith(f"{job_id}.0")), None)
        if (
            base is None
            or base[1] != job_name
            or not base[2].startswith("COMPLETED")
            or base[5] != profile.child_accounts[0]
            or srun is None
            or not srun[2].startswith("COMPLETED")
        ):
            raise PolicyError("allocation probe sbatch/srun steps did not complete exactly")
        allowed = {
            *(node.lower() for node in profile.allowed_nodes),
            *profile.host_aliases.values(),
        }
        node = base[3].lower()
        if node not in allowed:
            raise PolicyError("allocation probe ran outside the reviewed pool")
        gpu_verified = profile.gpu_tres_per_slot <= 0 or _positive_gpu_tres(base[4])
        if not gpu_verified:
            raise PolicyError("allocation probe did not read back a positive GPU TRES")

        guard_config = root / "etc/loom/slurm-job-cgroup-guard.json"
        guard = _wait_for_guard_status(
            root,
            candidate_sha=candidate_sha,
            expected_config_sha256=hashlib.sha256(guard_config.read_bytes()).hexdigest(),
            require_probe=True,
        )
        resource_probe = guard.get("resource_probe")
        if not isinstance(resource_probe, dict) or resource_probe.get("job_id") != job_id:
            raise PolicyError("cgroup guard did not attest the exact allocation probe job")
        payload = {
            "schema_version": 1,
            "created_at": datetime.now(UTC).isoformat(),
            "candidate_sha": candidate_sha,
            "candidate_tree": binding["repository"]["candidate_tree"],
            "cluster": profile.cluster,
            "controller": profile.controller,
            "submit_host": profile.submit_host,
            "submitting_host": host,
            "job_id": job_id,
            "job_name": job_name,
            "node": node,
            "state": "COMPLETED",
            "account": profile.child_accounts[0],
            "qos": profile.qos,
            "alloc_tres": base[4],
            "gpu_verified": gpu_verified,
            "sbatch_verified": True,
            "srun_verified": True,
            "batch_uid": batch_uid,
            "batch_gid": batch_gid,
            "guard_config_sha256": hashlib.sha256(
                guard_config.read_bytes(),
            ).hexdigest(),
            "guard_resource_probe": resource_probe,
            "candidate_binding": binding,
            "command_sha256": hashlib.sha256(
                "\0".join(arguments).encode(),
            ).hexdigest(),
        }
        _write_allocation_state(
            _allocation_probe_path(root, profile, candidate_sha),
            payload,
            enforce_root_ownership=True,
        )
        _finish_allocation_inflight(
            inflight_path,
            inflight,
            "completed",
            enforce_root_ownership=True,
        )
        return payload
    except Exception:
        _invalidate_allocation_artifact(root, profile, candidate_sha)
        _cancel_allocation_job(
            inflight_path,
            inflight,
            profile,
            enforce_root_ownership=True,
        )
        raise


def run_allocation_probe(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    batch_uid: int,
    batch_gid: int,
    timeout_seconds: float = _ALLOCATION_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    if root != Path("/") or os.geteuid() != 0:
        raise PolicyError("allocation-side Slurm probe requires the live root")
    with _allocation_probe_lock(root, profile, candidate_sha):
        return _run_allocation_probe_transaction(
            root,
            profile,
            candidate_sha=candidate_sha,
            candidate_root=candidate_root,
            worker_env=worker_env,
            batch_uid=batch_uid,
            batch_gid=batch_gid,
            timeout_seconds=timeout_seconds,
        )


def allocation_probe_readback(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str,
    candidate_binding: Mapping[str, Any],
) -> dict[str, Any]:
    path = _allocation_probe_path(root, profile, candidate_sha)
    if _allocation_inflight_path(root, profile, candidate_sha).exists():
        raise PolicyError("allocation-side Slurm probe still has an inflight job")
    if root == Path("/"):
        _require_root_private_directory(path.parent)
    try:
        metadata = path.lstat()
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PolicyError("allocation-side Slurm probe evidence is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (root == Path("/") and metadata.st_uid != 0)
        or (root == Path("/") and metadata.st_gid != 0)
        or not isinstance(payload, dict)
        or payload.get("schema_version") != 1
    ):
        raise PolicyError("allocation-side Slurm probe evidence is unsafe")
    try:
        created = datetime.fromisoformat(str(payload["created_at"]))
    except (KeyError, ValueError) as exc:
        raise PolicyError("allocation-side Slurm probe timestamp is invalid") from exc
    if created.tzinfo is None or datetime.now(UTC) - created.astimezone(UTC) > (
        _ALLOCATION_PROBE_MAX_AGE
    ):
        raise PolicyError("allocation-side Slurm probe evidence is stale")
    repository = candidate_binding.get("repository")
    worker_env_binding = candidate_binding.get("worker_env")
    evidence_binding = payload.get("candidate_binding")
    if (
        not isinstance(repository, Mapping)
        or not isinstance(worker_env_binding, Mapping)
        or not isinstance(evidence_binding, Mapping)
        or payload.get("candidate_sha") != candidate_sha
        or payload.get("candidate_tree") != repository.get("candidate_tree")
        or evidence_binding.get("repository") != repository
        or evidence_binding.get("worker_env") != worker_env_binding
        or payload.get("cluster") != profile.cluster
        or payload.get("controller") != profile.controller
        or payload.get("submit_host") != profile.submit_host
        or payload.get("state") != "COMPLETED"
        or payload.get("sbatch_verified") is not True
        or payload.get("srun_verified") is not True
        or payload.get("account") not in profile.child_accounts
        or payload.get("qos") != profile.qos
        or type(worker_env_binding.get("uid")) is not int
        or type(worker_env_binding.get("gid")) is not int
        or payload.get("batch_uid") != worker_env_binding.get("uid")
        or payload.get("batch_gid") != worker_env_binding.get("gid")
    ):
        raise PolicyError("allocation-side Slurm probe binding drifted")
    allowed = {
        *(node.lower() for node in profile.allowed_nodes),
        *profile.host_aliases.values(),
    }
    if str(payload.get("node", "")).lower() not in allowed:
        raise PolicyError("allocation-side Slurm probe node drifted")
    if profile.gpu_tres_per_slot > 0 and (
        payload.get("gpu_verified") is not True
        or not _positive_gpu_tres(str(payload.get("alloc_tres", "")))
    ):
        raise PolicyError("allocation-side Slurm GPU probe drifted")
    return payload


def live_readback(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str,
    require_probe: bool,
    check_accounting: bool = True,
    wait_for_guard: bool = False,
    candidate_binding: Mapping[str, Any] | None = None,
    require_allocation_probe: bool = False,
) -> dict[str, Any]:
    desired = desired_files(root, profile, candidate_sha=candidate_sha)
    slurm = _parse_key_values(_run(("scontrol", "show", "config")))
    expected_slurm = {key: _slurm_value(profile.slurm[field]) for key, field in _SLURM_KEYS.items()}
    for key, expected in expected_slurm.items():
        observed = slurm.get(key)
        if key in {"TaskPlugin", "AccountingStorageEnforce", "PrologFlags"}:
            if _split_csv(observed or "") != _split_csv(expected):
                raise PolicyError(f"live Slurm {key} readback drifted")
        elif observed != expected:
            raise PolicyError(f"live Slurm {key} readback drifted")
    cgroup_path = root / "etc/slurm/cgroup.conf"
    try:
        cgroup = _parse_key_values(cgroup_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise PolicyError("live cgroup.conf is unavailable") from exc
    expected_cgroup = _parse_key_values(render_cgroup_conf(profile))
    if any(cgroup.get(key) != value for key, value in expected_cgroup.items()):
        raise PolicyError("live cgroup.conf readback drifted")
    docker_driver = _run(("docker", "info", "--format", "{{.CgroupDriver}}")).strip()
    if docker_driver != profile.docker_cgroup_driver:
        raise PolicyError("live Docker cgroup driver readback drifted")
    if (
        _run(
            ("systemctl", "is-enabled", "loom-slurm-job-cgroup-guard.service"),
        ).strip()
        != "enabled"
    ):
        raise PolicyError("cgroup guard is not enabled")
    if (
        _run(
            ("systemctl", "is-active", "loom-slurm-job-cgroup-guard.service"),
        ).strip()
        != "active"
    ):
        raise PolicyError("cgroup guard is not active")
    guard_config_path = root / "etc/loom/slurm-job-cgroup-guard.json"
    expected_config_sha = _sha256(desired[guard_config_path].encode())
    guard_reader = _wait_for_guard_status if wait_for_guard else _guard_status_readback
    guard = guard_reader(
        root,
        candidate_sha=candidate_sha,
        expected_config_sha256=expected_config_sha,
        require_probe=require_probe,
    )
    accounting = _accounting_readback(profile) if check_accounting else None
    if require_allocation_probe:
        if candidate_binding is None:
            raise PolicyError("strict candidate binding is required for allocation readback")
        allocation = allocation_probe_readback(
            root,
            profile,
            candidate_sha=candidate_sha,
            candidate_binding=candidate_binding,
        )
    else:
        allocation = None
    return {
        "converged": True,
        "slurm": expected_slurm,
        "cgroup": expected_cgroup,
        "docker_cgroup_driver": docker_driver,
        "guard": guard,
        "accounting": accounting,
        "allocation_probe": allocation,
    }


def _recover_orphan(
    root: Path,
    profile: Profile,
    *,
    slurm_node: str | None,
) -> dict[str, Any] | None:
    path = _journal_path(root, profile)
    journal = _load_journal(path)
    if journal is None:
        return journal
    snapshot_raw = journal.get("snapshot")
    if not isinstance(snapshot_raw, str):
        raise PolicyError("orphan Slurm policy journal lacks a snapshot")
    snapshot = _validate_snapshot_path(root, Path(snapshot_raw))
    accounting_raw = journal.get("accounting_snapshot")
    if accounting_raw is None:
        accounting_snapshot = None
    elif isinstance(accounting_raw, str):
        accounting_snapshot = _validate_accounting_snapshot_path(
            root,
            snapshot,
            Path(accounting_raw),
        )
    else:
        raise PolicyError("orphan Slurm policy accounting snapshot path is invalid")
    rollback_target = journal.get("rollback_target")
    if rollback_target is not None:
        if not isinstance(rollback_target, str):
            raise PolicyError("orphan Slurm policy rollback target is invalid")
        _validate_snapshot_path(root, Path(rollback_target))
    if journal.get("phase") in {"committed", "rolled_back"}:
        return journal
    try:
        _restore_snapshot(root, snapshot)
        if accounting_snapshot is not None:
            _restore_accounting(profile, accounting_snapshot)
        if root == Path("/") and journal.get("restart") is True and slurm_node is not None:
            _restore_services(root, profile, slurm_node)
        _snapshot_readback(root, snapshot)
        if accounting_snapshot is not None:
            _accounting_snapshot_matches(profile, accounting_snapshot)
    except Exception:
        _advance_journal(path, journal, "recovery_failed")
        raise
    _advance_journal(path, journal, "rolled_back")
    return journal


def _validate_live_apply(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str,
    restart: bool,
    apply_accounting: bool,
) -> tuple[str, str | None]:
    host = _canonical_host()
    slurm_node = _slurm_node_for_host(profile, host)
    if root != Path("/") and (restart or apply_accounting):
        raise PolicyError("service restart and accounting apply require the live root")
    if root == Path("/"):
        verify_source_candidate(candidate_sha)
        if os.geteuid() != 0:
            raise PolicyError("live apply requires root")
        if slurm_node is None:
            raise PolicyError(f"host {host!r} is outside the reviewed node allowlist")
        if apply_accounting and slurm_node != profile.controller:
            raise PolicyError("accounting apply is controller-only")
        if apply_accounting:
            cluster = _run(("sacctmgr", "-nP", "show", "cluster", "format=Cluster"))
            if profile.cluster not in {line.strip("|") for line in cluster.splitlines()}:
                raise PolicyError("live Slurm cluster identity does not match the profile")
        if restart:
            if _run(("squeue", "-h", "-w", slurm_node)).strip():
                raise PolicyError("node still has Slurm jobs; drain and retry")
            if _run(("docker", "ps", "-q")).strip():
                raise PolicyError("node still has running Docker containers; drain and retry")
    return host, slurm_node


def apply(
    root: Path,
    profile: Profile,
    *,
    restart: bool,
    apply_accounting: bool,
    candidate_sha: str | None = None,
) -> dict[str, Any]:
    candidate = candidate_sha or source_candidate_sha()
    _host, slurm_node = _validate_live_apply(
        root,
        profile,
        candidate_sha=candidate,
        restart=False,
        apply_accounting=False,
    )
    with _domain_lock(root, profile):
        _recover_orphan(root, profile, slurm_node=slurm_node)
        _host, slurm_node = _validate_live_apply(
            root,
            profile,
            candidate_sha=candidate,
            restart=restart,
            apply_accounting=apply_accounting,
        )
        files = desired_files(root, profile, candidate_sha=candidate)
        if root == Path("/"):
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix="loom-dockerd-validate-",
            ) as handle:
                handle.write(files[root / "etc/docker/daemon.json"])
                handle.flush()
                _run(("dockerd", "--validate", "--config-file", handle.name))
        snapshot = _snapshot(root, files)
        accounting_snapshot = (
            _accounting_snapshot(root, profile, snapshot) if apply_accounting else None
        )
        journal_path = _journal_path(root, profile)
        journal: dict[str, Any] = {
            "schema_version": 1,
            "operation": "apply",
            "cluster": profile.cluster,
            "candidate_sha": candidate,
            "snapshot": str(snapshot),
            "accounting_snapshot": (
                str(accounting_snapshot) if accounting_snapshot is not None else None
            ),
            "restart": restart,
            "apply_accounting": apply_accounting,
            "phase": "prepared",
            "created_at": datetime.now(UTC).isoformat(),
        }
        _write_journal(journal_path, journal)
        try:
            for path, content in files.items():
                _atomic_write(
                    path,
                    content,
                    mode=_desired_file_mode(root, path),
                )
            _advance_journal(journal_path, journal, "files_written")
            if apply_accounting:
                if accounting_snapshot is None:
                    raise PolicyError("Loom accounting CAS snapshot is missing")
                accounting_payload = _load_accounting_snapshot(accounting_snapshot)
                _apply_accounting(profile, accounting_payload)
            _advance_journal(journal_path, journal, "accounting_applied")
            if restart:
                if slurm_node is None:
                    raise PolicyError("live restart lacks an allowed Slurm node")
                _restart_services(profile, slurm_node)
            _advance_journal(journal_path, journal, "services_reconfigured")
            if root == Path("/"):
                live = live_readback(
                    root,
                    profile,
                    candidate_sha=candidate,
                    require_probe=False,
                    check_accounting=apply_accounting,
                    wait_for_guard=True,
                )
            else:
                rendered = plan(root, profile, candidate_sha=candidate)
                if not rendered["file_plan"]["converged"]:
                    raise PolicyError("offline Slurm policy write readback drifted")
                live = {"converged": True, "offline": True}
            _advance_journal(journal_path, journal, "verified")
            _advance_journal(journal_path, journal, "committed")
        except Exception as exc:
            try:
                _restore_snapshot(root, snapshot)
                if accounting_snapshot is not None:
                    _restore_accounting(profile, accounting_snapshot)
                if restart and slurm_node is not None:
                    _restore_services(root, profile, slurm_node)
                _snapshot_readback(root, snapshot)
                if accounting_snapshot is not None:
                    _accounting_snapshot_matches(profile, accounting_snapshot)
                _advance_journal(journal_path, journal, "rolled_back")
            except Exception as rollback_exc:
                _advance_journal(journal_path, journal, "rollback_failed")
                raise PolicyError(
                    "Slurm policy apply failed and automatic rollback did not converge",
                ) from rollback_exc
            if isinstance(exc, PolicyError):
                raise
            raise PolicyError("Slurm policy apply failed and was rolled back") from exc
    return {
        **plan(root, profile, candidate_sha=candidate),
        "mutation_authorized": True,
        "snapshot": str(snapshot),
        "journal": str(journal_path),
        "phase": "committed",
        "restart_requested": restart,
        "accounting_requested": apply_accounting,
        "live_readback": live,
    }


def rollback(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str | None = None,
) -> dict[str, Any]:
    current_candidate = candidate_sha or source_candidate_sha()
    _host, slurm_node = _validate_live_apply(
        root,
        profile,
        candidate_sha=current_candidate,
        restart=False,
        apply_accounting=False,
    )
    with _domain_lock(root, profile):
        _recover_orphan(root, profile, slurm_node=slurm_node)
        _host, slurm_node = _validate_live_apply(
            root,
            profile,
            candidate_sha=current_candidate,
            restart=root == Path("/"),
            apply_accounting=False,
        )
        journal_path = _journal_path(root, profile)
        previous = _load_journal(journal_path)
        if previous is None or previous.get("phase") != "committed":
            raise PolicyError("no committed Slurm policy transaction is available to roll back")
        target_raw = previous.get("snapshot")
        if not isinstance(target_raw, str):
            raise PolicyError("committed Slurm policy transaction lacks a snapshot")
        target = _validate_snapshot_path(root, Path(target_raw))
        previous_accounting_raw = previous.get("accounting_snapshot")
        if previous_accounting_raw is None:
            previous_accounting = None
        elif isinstance(previous_accounting_raw, str):
            previous_accounting = _validate_accounting_snapshot_path(
                root,
                target,
                Path(previous_accounting_raw),
            )
        else:
            raise PolicyError("committed accounting snapshot path is invalid")
        current_files = desired_files(
            root,
            profile,
            candidate_sha=current_candidate,
        )
        current_snapshot = _snapshot(root, current_files)
        current_accounting = (
            _accounting_snapshot(root, profile, current_snapshot)
            if previous_accounting is not None
            else None
        )
        transaction: dict[str, Any] = {
            "schema_version": 1,
            "operation": "rollback",
            "cluster": profile.cluster,
            "candidate_sha": current_candidate,
            "snapshot": str(current_snapshot),
            "accounting_snapshot": (
                str(current_accounting) if current_accounting is not None else None
            ),
            "rollback_target": str(target),
            "restart": root == Path("/"),
            "apply_accounting": current_accounting is not None,
            "phase": "prepared",
            "created_at": datetime.now(UTC).isoformat(),
        }
        _write_journal(journal_path, transaction)
        try:
            _restore_snapshot(root, target)
            _advance_journal(journal_path, transaction, "files_written")
            if previous_accounting is not None:
                _restore_accounting(profile, previous_accounting)
            _advance_journal(journal_path, transaction, "accounting_applied")
            if root == Path("/"):
                if slurm_node is None:
                    raise PolicyError("live rollback lacks an allowed Slurm node")
                _restore_services(root, profile, slurm_node)
            _advance_journal(journal_path, transaction, "services_reconfigured")
            guard_config = root / "etc/loom/slurm-job-cgroup-guard.json"
            live = _snapshot_readback(root, target)
            if previous_accounting is not None:
                _accounting_snapshot_matches(profile, previous_accounting)
            if guard_config.exists() and root == Path("/"):
                try:
                    restored_candidate = json.loads(
                        guard_config.read_text(encoding="utf-8"),
                    )["candidate_sha"]
                except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
                    raise PolicyError("restored guard candidate binding is invalid") from exc
                if (
                    not isinstance(restored_candidate, str)
                    or _CANDIDATE_RE.fullmatch(restored_candidate) is None
                ):
                    raise PolicyError("restored guard candidate binding is invalid")
                live["guard"] = _wait_for_guard_status(
                    root,
                    candidate_sha=restored_candidate,
                    expected_config_sha256=_sha256(guard_config.read_bytes()),
                    require_probe=False,
                )
            _advance_journal(journal_path, transaction, "verified")
            _advance_journal(journal_path, transaction, "committed")
        except Exception as exc:
            try:
                _restore_snapshot(root, current_snapshot)
                if current_accounting is not None:
                    _restore_accounting(profile, current_accounting)
                if root == Path("/") and slurm_node is not None:
                    _restore_services(root, profile, slurm_node)
                _snapshot_readback(root, current_snapshot)
                if current_accounting is not None:
                    _accounting_snapshot_matches(profile, current_accounting)
                _advance_journal(journal_path, transaction, "rolled_back")
            except Exception as rollback_exc:
                _advance_journal(journal_path, transaction, "rollback_failed")
                raise PolicyError(
                    "Slurm policy rollback failed and prior state could not be restored",
                ) from rollback_exc
            if isinstance(exc, PolicyError):
                raise
            raise PolicyError("Slurm policy rollback failed safely") from exc
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-policy-rollback",
        "cluster": profile.cluster,
        "mutation_authorized": True,
        "restored_snapshot": str(target),
        "recovery_snapshot": str(current_snapshot),
        "journal": str(journal_path),
        "phase": "committed",
        "live_readback": live,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "command",
        choices=("plan", "check", "apply", "rollback", "allocation-probe"),
    )
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=Path("/"))
    parser.add_argument("--candidate-sha")
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--worker-env", type=Path)
    parser.add_argument("--batch-uid", type=int)
    parser.add_argument("--batch-gid", type=int)
    parser.add_argument(
        "--allocation-timeout-seconds",
        type=float,
        default=_ALLOCATION_TIMEOUT_SECONDS,
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--apply-accounting", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        profile = load_profile(args.profile)
        candidate = args.candidate_sha or source_candidate_sha()
        if args.command == "apply" and args.execute:
            result = apply(
                args.root,
                profile,
                restart=args.restart,
                apply_accounting=args.apply_accounting,
                candidate_sha=candidate,
            )
        elif args.command == "rollback" and args.execute:
            result = rollback(args.root, profile, candidate_sha=candidate)
        elif args.command == "allocation-probe" and args.execute:
            if (
                args.candidate_root is None
                or args.worker_env is None
                or args.batch_uid is None
                or args.batch_gid is None
            ):
                raise PolicyError(
                    "allocation-probe requires candidate root, worker env, and batch UID/GID",
                )
            result = run_allocation_probe(
                args.root,
                profile,
                candidate_sha=candidate,
                candidate_root=args.candidate_root,
                worker_env=args.worker_env,
                batch_uid=args.batch_uid,
                batch_gid=args.batch_gid,
                timeout_seconds=args.allocation_timeout_seconds,
            )
        else:
            result = plan(args.root, profile, candidate_sha=candidate)
            if args.command == "check":
                if not result["file_plan"]["converged"]:
                    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
                    return 1
                if args.root == Path("/"):
                    verify_source_candidate(candidate)
                    if (
                        args.candidate_root is None
                        or args.worker_env is None
                        or args.batch_uid is None
                        or args.batch_gid is None
                    ):
                        raise PolicyError(
                            "live check requires candidate root, worker env, and batch UID/GID",
                        )
                    binding = strict_candidate_binding(
                        args.candidate_root,
                        args.worker_env,
                        candidate_sha=candidate,
                        expected_batch_uid=args.batch_uid,
                        expected_batch_gid=args.batch_gid,
                    )
                    result["live_readback"] = live_readback(
                        args.root,
                        profile,
                        candidate_sha=candidate,
                        require_probe=True,
                        check_accounting=True,
                        candidate_binding=binding,
                        require_allocation_probe=True,
                    )
                else:
                    result["live_readback"] = {
                        "converged": None,
                        "performed": False,
                    }
        sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        return 0
    except PolicyError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
