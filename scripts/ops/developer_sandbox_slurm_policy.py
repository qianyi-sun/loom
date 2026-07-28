#!/usr/bin/env python3
"""Plan, check, and converge developer-sandbox Slurm host policy.

Mutations are local-host only, require root, and are disabled unless
``--execute`` is present. The caller must drain one node at a time before
requesting service restarts.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
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
    if not all(isinstance(item, str) and item for item in (cluster, controller, submit_host)):
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
        key = match.group(1) if match else None
        if key not in desired:
            output.append(line)
            continue
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


def desired_files(root: Path, profile: Profile) -> dict[Path, str]:
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
                    "pids_max": profile.job_pids_max,
                    "allowed_accounts": sorted(profile.child_accounts),
                    "poll_interval_seconds": 0.2,
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


def plan(root: Path, profile: Profile) -> dict[str, Any]:
    files = desired_files(root, profile)
    rows = []
    for path, desired in files.items():
        live = path.read_bytes() if path.exists() else b""
        rows.append(
            {
                "path": str(path),
                "live_sha256": _sha256(live),
                "desired_sha256": _sha256(desired.encode()),
                "converged": live == desired.encode(),
            },
        )
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-policy-plan",
        "cluster": profile.cluster,
        "capacity": {
            "slot_budget": profile.slot_budget,
            "pending_slot_budget": profile.pending_slot_budget,
            "cpus_per_slot": profile.cpus_per_slot,
            "memory_mib_per_slot": profile.memory_mib_per_slot,
            "gpu_tres_per_slot": profile.gpu_tres_per_slot,
            "job_pids_max": profile.job_pids_max,
        },
        "mutation_authorized": False,
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


def _run(argv: Sequence[str]) -> str:
    completed = subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        raise PolicyError(f"{argv[0]} failed safely with exit code {completed.returncode}")
    return completed.stdout


def _atomic_write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else 0o644
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.chmod(mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _snapshot(root: Path, files: Mapping[Path, str]) -> Path:
    timestamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    snapshot = root / "var/lib/loom-developer-sandbox-slurm-policy/snapshots" / timestamp
    snapshot.mkdir(parents=True, mode=0o700)
    manifest: dict[str, Any] = {"schema_version": 1, "files": []}
    for path in files:
        relative = path.relative_to(root)
        target = snapshot / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            shutil.copy2(path, target)
            manifest["files"].append({"path": str(relative), "present": True})
        else:
            manifest["files"].append({"path": str(relative), "present": False})
    _atomic_write(snapshot / "manifest.json", json.dumps(manifest, sort_keys=True) + "\n")
    (snapshot / "manifest.json").chmod(0o600)
    return snapshot


def _canonical_host() -> str:
    return socket.gethostname().split(".", 1)[0].rstrip(".").lower()


def _slurm_node_for_host(profile: Profile, host: str) -> str | None:
    for slurm_node, canonical_host in profile.host_aliases.items():
        if host == canonical_host:
            return slurm_node
    return None


def apply(
    root: Path,
    profile: Profile,
    *,
    restart: bool,
    apply_accounting: bool,
) -> dict[str, Any]:
    host = _canonical_host()
    slurm_node = _slurm_node_for_host(profile, host)
    if root != Path("/") and (restart or apply_accounting):
        raise PolicyError(
            "service restart and accounting apply require the live root",
        )
    if root == Path("/"):
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
                raise PolicyError(
                    "node still has running Docker containers; drain and retry",
                )
    files = desired_files(root, profile)
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
    for path, content in files.items():
        _atomic_write(path, content)
        if path == root / "usr/libexec/loom-slurm-job-cgroup-guard":
            path.chmod(0o755)
    if apply_accounting:
        for command in accounting_commands(profile):
            _run(command)
    if restart:
        _run(("systemctl", "daemon-reload"))
        _run(("systemctl", "enable", "loom-slurm-job-cgroup-guard.service"))
        _run(("systemctl", "restart", "docker"))
        _run(("systemctl", "restart", "slurmd"))
        _run(("systemctl", "restart", "loom-slurm-job-cgroup-guard.service"))
        if slurm_node == profile.controller:
            _run(("systemctl", "restart", "slurmctld"))
    return {
        **plan(root, profile),
        "mutation_authorized": True,
        "snapshot": str(snapshot),
        "restart_requested": restart,
        "accounting_requested": apply_accounting,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("command", choices=("plan", "check", "apply"))
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--root", type=Path, default=Path("/"))
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--restart", action="store_true")
    parser.add_argument("--apply-accounting", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        profile = load_profile(args.profile)
        if args.command == "apply" and args.execute:
            result = apply(
                args.root,
                profile,
                restart=args.restart,
                apply_accounting=args.apply_accounting,
            )
        else:
            result = plan(args.root, profile)
            if args.command == "check" and not all(row["converged"] for row in result["files"]):
                sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
                return 1
        sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        return 0
    except PolicyError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
