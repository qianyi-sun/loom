#!/usr/bin/env python3
"""Install the exact-candidate shared-capacity supervisor and adapters.

Public mutation commands are plan-only unless ``--execute`` is supplied.  The
live path is fixed to oldlab2 and installs the registry-bound sandbox adapter
instances plus the broker supervisor.  Capacity remains fail-closed until a
valid broker handoff and runtime receipt reach an adapter.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import grp
import hashlib
import json
import os
import pwd
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, cast

REPO_ROOT = Path(__file__).resolve().parents[2]
IMPORT_ROOT = (
    REPO_ROOT
    if (REPO_ROOT / "scripts/ops/developer_sandbox_capacity_contract.py").is_file()
    else Path(__file__).resolve().parent
)
if str(IMPORT_ROOT) not in sys.path:
    sys.path.insert(0, str(IMPORT_ROOT))

from scripts.ops.developer_sandbox_capacity_contract import (  # noqa: E402, I001
    CAPACITY_POLICY_SOURCES as PLATFORM_POLICY_SOURCES,
    CapacityContractError,
    load_capacity_policy,
    load_platform_health_contract,
)
from scripts.ops import developer_environment_registry as registry_contract  # noqa: E402

SOURCE_PROFILE = REPO_ROOT / "deploy/developer-sandboxes/shared-capacity-runtime-host.toml"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
EXPECTED_HOSTNAME = "trt-eai-oldlab-2"
PROGRAM_PATH = Path("/usr/local/libexec/loom-shared-capacity-runtime-host")
CAPACITY_CONTRACT_PATH = Path(
    "/usr/local/libexec/scripts/ops/developer_sandbox_capacity_contract.py",
)
REGISTRY_CONTRACT_PATH = Path(
    "/usr/local/libexec/scripts/ops/developer_environment_registry.py",
)
PROFILE_PATH = Path("/etc/loom/shared-capacity-runtime-host.toml")
CONFIG_ROOT = Path("/etc/loom")
ADAPTER_CONFIG_ROOT = CONFIG_ROOT / "shared-capacity-adapters"
SUPERVISOR_CONFIG_PATH = CONFIG_ROOT / "shared-capacity-supervisor.toml"
SUPERVISOR_BASE_CONFIG_PATH = CONFIG_ROOT / "shared-capacity-supervisor.base.toml"
LOOM_RUNTIME_PYTHON_ROOT = Path("/usr/local/libexec/loom-runtime-python")
STATE_ROOT = Path("/var/lib/loom-shared-capacity")
BROKER_STATE_DB_PATH = STATE_ROOT / "broker.sqlite3"
REGISTRY_SNAPSHOT_PATH = registry_contract.SYSTEM_SNAPSHOT
INSTALLER_ROOT = STATE_ROOT / "runtime-host-installer"
JOURNAL_ROOT = INSTALLER_ROOT / "transactions"
STATE_PATH = INSTALLER_ROOT / "state.json"
PLATFORM_HEALTH_ROOT = Path(
    "/var/lib/loom-developer-sandbox-platform-health-authority",
)
ACTIVE_JOURNAL_PATH = INSTALLER_ROOT / "active-transaction.json"
ACCEPTANCE_OPERATION_PATH = INSTALLER_ROOT / "acceptance-operation.json"
RECOVERY_PROGRAM_PATH = INSTALLER_ROOT / "runtime-host-recovery"
ENVIRONMENT_RECONCILE_ROOT = INSTALLER_ROOT / "environment-reconcile"
ENVIRONMENT_RETIRE_ROOT = INSTALLER_ROOT / "environment-retire"
EMPTY_COHORT_PATH = INSTALLER_ROOT / "empty-cohort.json"
DEVELOPER_ENVIRONMENT_RUNTIME_ROOT = Path(
    "/var/lib/loom-developer-environment-runtime",
)
ENVIRONMENT_ADMISSION_INTENT_ROOT = DEVELOPER_ENVIRONMENT_RUNTIME_ROOT / "lifecycle" / "admission"
ENVIRONMENT_ADMISSION_INTENT_KIND = "loom.developer-environment.admission-intent"
LOCK_PATH = Path("/run/loom-shared-capacity-runtime-host.lock")
CANDIDATE_PARENT = Path("/opt/loom-shared-capacity/candidates")
CURRENT_LINK = Path("/opt/loom-shared-capacity/current")
ADAPTER_SERVICE_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-adapter@.service",
)
ADAPTER_TIMER_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-adapter@.timer",
)
SUPERVISOR_SERVICE_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-supervisor.service",
)
SUPERVISOR_TIMER_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-supervisor.timer",
)
ADAPTER_SERVICE_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-adapter@.service"
)
ADAPTER_TIMER_SOURCE = REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-adapter@.timer"
SUPERVISOR_SERVICE_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-supervisor.service"
)
SUPERVISOR_TIMER_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-supervisor.timer"
)
SUPERVISOR_CONFIG_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/shared-capacity-supervisor/config.toml"
)
POOLS = ("gb10", "oldlab")
ACCEPTANCE_PHASES = (
    "multi_candidate_overlap",
    "large_batch_burst",
    "fairness_contention",
    "mixed_non_loom",
    "cancel_cleanup",
    "ttl_cleanup",
    "submit_host_restart",
    "worker_crash",
)
ACCEPTANCE_CONTRACT_TTL_SECONDS = 86400
ACCEPTANCE_DEFAULT_PHASE_TTL_SECONDS = 7200
ACCEPTANCE_MIXED_NON_LOOM_TTL_SECONDS = 21600
ACCEPTANCE_TTL_CLEANUP_SECONDS = 120
LIVE_PHASES = (
    "preflight",
    "baseline",
    "multi_candidate_overlap",
    "large_batch_burst",
    "fairness_contention",
    "mixed_non_loom",
    "cancel_cleanup",
    "ttl_cleanup",
    "submit_host_restart",
    "worker_crash",
    "final_drain",
)
PLATFORM_HEALTH_EVIDENCE_TTL = timedelta(minutes=15)
PLATFORM_HEALTH_ACTIVATION_MINIMUM_REMAINING = timedelta(minutes=5)
PLATFORM_HEALTH_MAX_CLOCK_SKEW = timedelta(seconds=5)
SUPERVISOR_SERVICE = "loom-shared-capacity-supervisor.service"
SUPERVISOR_TIMER = "loom-shared-capacity-supervisor.timer"
RETIREMENT_MAX_CYCLES = 60
RETIREMENT_POLL_SECONDS = 5.0
UNIT_PATHS = (
    ADAPTER_SERVICE_PATH,
    ADAPTER_TIMER_PATH,
    SUPERVISOR_SERVICE_PATH,
    SUPERVISOR_TIMER_PATH,
)
REQUIRED_UNIT_FILES = {
    ADAPTER_SERVICE_PATH.name,
    ADAPTER_TIMER_PATH.name,
    SUPERVISOR_SERVICE_PATH.name,
    SUPERVISOR_TIMER_PATH.name,
}


class RuntimeHostError(RuntimeError):
    """The requested host convergence could not be completed safely."""


@dataclass(frozen=True, slots=True)
class Candidate:
    sha: str
    tree: str
    source: Path

    @property
    def root(self) -> Path:
        return CANDIDATE_PARENT / self.sha

    @property
    def repo(self) -> Path:
        return self.root / "repo"

    @property
    def venv(self) -> Path:
        return self.root / "venv"


@dataclass(frozen=True, slots=True)
class RuntimeEnvironment:
    env_id: str
    runtime_id: str
    layout_version: str
    state: str
    resource_generation: int
    service_user: str
    service_group: str
    uid: int
    gid: int
    systemd_instance: str
    state_root: Path
    runtime_root: Path
    candidate_root: Path
    control_plane_port: int
    slurm_user: str
    slurm_account: str
    slurm_qos: str
    candidate_id: str
    candidate_sha: str
    candidate_tree: str
    deployment_phase: str


@dataclass(frozen=True, slots=True)
class RegistryCohort:
    generation: int
    payload_sha256: str
    environments: tuple[RuntimeEnvironment, ...]
    provisioning_environments: tuple[RuntimeEnvironment, ...] = ()

    @property
    def sandboxes(self) -> tuple[str, ...]:
        return tuple(item.runtime_id for item in self.environments)

    @property
    def instances(self) -> tuple[str, ...]:
        return tuple(f"{sandbox}-{pool}" for sandbox in self.sandboxes for pool in POOLS)

    @property
    def provisioning_instances(self) -> tuple[str, ...]:
        return tuple(
            f"{environment.runtime_id}-{pool}"
            for environment in self.provisioning_environments
            for pool in POOLS
        )


_COHORT_CACHE: RegistryCohort | None = None


def _run(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    expected: set[int] | frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        env=dict(env) if env is not None else None,
    )
    if completed.returncode not in expected:
        name = Path(argv[0]).name if argv else "command"
        raise RuntimeHostError(
            f"{name} failed safely with exit code {completed.returncode}",
        )
    return completed


def _git_environment() -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C",
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_PROTOCOL_FROM_USER": "0",
        "GIT_PAGER": "cat",
        "GIT_EXTERNAL_DIFF": "/usr/bin/false",
        "GIT_SSH_COMMAND": "/usr/bin/false",
        "HOME": "/nonexistent",
        "XDG_CONFIG_HOME": "/nonexistent",
    }


def _git_raw(repo: Path, *args: str) -> bytes:
    completed = subprocess.run(
        (
            "git",
            "--no-replace-objects",
            "-c",
            f"safe.directory={repo}",
            "-c",
            "credential.helper=",
            "-c",
            "core.sshCommand=/usr/bin/false",
            "-c",
            "fetch.recurseSubmodules=false",
            "-C",
            str(repo),
            *args,
        ),
        check=False,
        capture_output=True,
        text=False,
        env=_git_environment(),
    )
    if completed.returncode != 0:
        raise RuntimeHostError(
            f"git failed safely with exit code {completed.returncode}",
        )
    return completed.stdout


def _git(repo: Path, *args: str) -> str:
    try:
        return _git_raw(repo, *args).decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError as exc:
        raise RuntimeHostError("git output is invalid") from exc


def _repository_entries(
    raw: bytes,
    *,
    tree: bool,
) -> dict[str, tuple[str, str]]:
    entries: dict[str, tuple[str, str]] = {}
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        metadata, separator, encoded_path = encoded.partition(b"\t")
        fields = metadata.split()
        try:
            relative = encoded_path.decode("utf-8", errors="strict")
            ascii_fields = [field.decode("ascii", errors="strict") for field in fields]
        except UnicodeDecodeError as exc:
            raise RuntimeHostError("repository metadata is invalid") from exc
        path = PurePosixPath(relative)
        if tree:
            valid_metadata = (
                len(ascii_fields) == 3
                and ascii_fields[0] in {"100644", "100755", "120000"}
                and ascii_fields[1] == "blob"
                and SHA_RE.fullmatch(ascii_fields[2]) is not None
            )
            value = (
                (
                    ascii_fields[0],
                    ascii_fields[2],
                )
                if valid_metadata
                else ("", "")
            )
        else:
            valid_metadata = (
                len(ascii_fields) == 3
                and ascii_fields[0] in {"100644", "100755", "120000"}
                and SHA_RE.fullmatch(ascii_fields[1]) is not None
                and ascii_fields[2] == "0"
            )
            value = (
                (
                    ascii_fields[0],
                    ascii_fields[1],
                )
                if valid_metadata
                else ("", "")
            )
        if (
            separator != b"\t"
            or not valid_metadata
            or not relative
            or path.is_absolute()
            or ".." in path.parts
            or path.parts[0] == ".git"
            or relative in entries
        ):
            raise RuntimeHostError("repository metadata is invalid")
        entries[relative] = value
    if not entries:
        raise RuntimeHostError("repository metadata is empty")
    return entries


def _reject_git_indirection(repo: Path) -> None:
    if _git(repo, "rev-parse", "--is-shallow-repository") != "false":
        raise RuntimeHostError("repository history is not self-contained")
    if _git(repo, "replace", "-l"):
        raise RuntimeHostError("repository replacement objects are forbidden")
    for git_path in ("objects/info/alternates", "info/grafts", "shallow"):
        resolved = Path(_git(repo, "rev-parse", "--git-path", git_path))
        if resolved.exists() or resolved.is_symlink():
            raise RuntimeHostError("repository object indirection is forbidden")


def _validate_repository(repo: Path, sha: str) -> str:
    if _git(repo, "rev-parse", "--verify", "HEAD") != sha:
        raise RuntimeHostError("candidate source HEAD does not match requested SHA")
    if _git(repo, "rev-parse", "--verify", f"{sha}^{{commit}}") != sha:
        raise RuntimeHostError("candidate commit does not resolve exactly")
    _reject_git_indirection(repo)
    index = _repository_entries(
        _git_raw(repo, "ls-files", "--stage", "-z", "--"),
        tree=False,
    )
    commit_tree = _repository_entries(
        _git_raw(repo, "ls-tree", "-r", "-z", "--full-tree", sha),
        tree=True,
    )
    if index != commit_tree:
        raise RuntimeHostError("candidate index does not match the commit tree")
    flag_rows = [row for row in _git_raw(repo, "ls-files", "-v", "-z", "--").split(b"\0") if row]
    if len(flag_rows) != len(index) or any(not row.startswith(b"H ") for row in flag_rows):
        raise RuntimeHostError("candidate index flags are unsafe")
    _git(repo, "diff-files", "--quiet", "--")
    _git(repo, "diff-index", "--quiet", sha, "--")
    if _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise RuntimeHostError("candidate source is not clean")
    tree_sha = _git(repo, "rev-parse", "--verify", f"{sha}^{{tree}}")
    if SHA_RE.fullmatch(tree_sha) is None:
        raise RuntimeHostError("candidate tree is invalid")
    return tree_sha


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(dict(payload), sort_keys=True, separators=(",", ":")) + "\n").encode()


def _canonical_json_value(payload: Any) -> bytes:
    return (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _registry_record_fields(record_type: type[Any]) -> set[str]:
    return set(record_type.__dataclass_fields__)


def _registry_path(value: object, *, label: str) -> Path:
    if not isinstance(value, str):
        raise RuntimeHostError(f"runtime registry {label} is invalid")
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        raise RuntimeHostError(f"runtime registry {label} is invalid")
    return path


def _validate_registry_environment_resources(row: Mapping[str, Any]) -> None:
    runtime_id = str(row["runtime_id"])
    layout_version = row.get("layout_version")
    if layout_version == "dynamic-v1":
        expected = registry_contract.DeveloperEnvironmentRegistry._dynamic_resources(
            str(row["env_id"]),
            runtime_id,
        )
    elif layout_version == "legacy-v1":
        expected = {
            "service_user": f"loom-sandbox-{runtime_id}",
            "service_group": f"loom-sandbox-{runtime_id}",
            "compose_project": f"loom-sandbox-{runtime_id}",
            "systemd_instance": runtime_id,
            "candidate_root": f"/shared_work/loom/candidates/sandboxes/{runtime_id}",
            "runtime_root": f"/shared_work/loom/runtime/sandboxes/{runtime_id}",
            "state_root": f"/srv/loom/developer-sandboxes/{runtime_id}",
            "evidence_root": f"/srv/loom/developer-sandboxes/{runtime_id}/evidence",
            "database_name": f"loom_sandbox_{runtime_id}",
            "postgres_volume": f"loom-sandbox-{runtime_id}_postgres_data",
            "minio_volume": f"loom-sandbox-{runtime_id}_minio_data",
            "task_bucket": f"loom-sandbox-{runtime_id}-tasks",
            "trajectories_bucket": f"loom-sandbox-{runtime_id}-trajectories",
            "artifacts_bucket": f"loom-sandbox-{runtime_id}-artifacts",
            "provider_namespace": f"sandbox-{runtime_id}",
            "slurm_user": f"loom-sandbox-{runtime_id}",
            "slurm_account": f"loom-dev-{runtime_id}",
            "slurm_qos": f"loom-dev-{runtime_id}",
            "cgroup_slice": f"loom-dev-{runtime_id}.slice",
        }
    else:
        raise RuntimeHostError("runtime registry environment layout is invalid")
    if any(row.get(field) != value for field, value in expected.items()):
        raise RuntimeHostError("runtime registry environment resource binding is invalid")


def _validate_registry_environment_row(row: object) -> dict[str, Any]:
    fields = _registry_record_fields(registry_contract.EnvironmentRecord)
    if not isinstance(row, dict) or set(row) != fields:
        raise RuntimeHostError("runtime registry environment shape is invalid")
    ports = row.get("ports")
    display_name = row.get("display_name")
    if (
        registry_contract.ENV_ID_RE.fullmatch(str(row.get("env_id"))) is None
        or registry_contract.PRINCIPAL_RE.fullmatch(str(row.get("principal_id"))) is None
        or registry_contract.RUNTIME_ID_RE.fullmatch(str(row.get("runtime_id"))) is None
        or row.get("state") not in {"ready", "deploying", "active", "retired", "quarantined"}
        or type(row.get("resource_generation")) is not int
        or row["resource_generation"] < 1
        or type(row.get("uid")) is not int
        or type(row.get("gid")) is not int
        or row["uid"] < 1
        or row["uid"] != row["gid"]
        or not isinstance(display_name, str)
        or not display_name
        or len(display_name) > 128
        or any(ord(character) < 32 or ord(character) == 127 for character in display_name)
        or not isinstance(ports, dict)
        or set(ports) != set(registry_contract.PORT_NAMES)
        or any(type(port) is not int or not 1024 <= port <= 65_535 for port in ports.values())
        or len(set(ports.values())) != len(registry_contract.PORT_NAMES)
        or (
            row.get("current_candidate_id") is not None
            and registry_contract.CANDIDATE_ID_RE.fullmatch(
                str(row["current_candidate_id"]),
            )
            is None
        )
        or not isinstance(row.get("created_at"), str)
    ):
        raise RuntimeHostError("runtime registry environment binding is invalid")
    for field in (
        "service_user",
        "service_group",
        "compose_project",
        "systemd_instance",
        "database_name",
        "postgres_volume",
        "minio_volume",
        "provider_namespace",
        "slurm_user",
        "slurm_account",
        "slurm_qos",
    ):
        if registry_contract.SAFE_NAME_RE.fullmatch(str(row.get(field))) is None:
            raise RuntimeHostError("runtime registry environment name binding is invalid")
    for field in ("task_bucket", "trajectories_bucket", "artifacts_bucket"):
        if registry_contract.SAFE_BUCKET_RE.fullmatch(str(row.get(field))) is None:
            raise RuntimeHostError("runtime registry environment bucket binding is invalid")
    for field in ("candidate_root", "runtime_root", "state_root", "evidence_root"):
        _registry_path(row.get(field), label=f"environment {field}")
    _validate_registry_environment_resources(row)
    return row


def _validate_registry_candidate_row(row: object) -> dict[str, Any]:
    fields = _registry_record_fields(registry_contract.CandidateRecord)
    image_digests = row.get("image_digests") if isinstance(row, dict) else None
    if (
        not isinstance(row, dict)
        or set(row) != fields
        or registry_contract.CANDIDATE_ID_RE.fullmatch(str(row.get("candidate_id"))) is None
        or registry_contract.PRINCIPAL_RE.fullmatch(str(row.get("principal_id"))) is None
        or registry_contract.ENV_ID_RE.fullmatch(str(row.get("env_id"))) is None
        or row.get("repository_id") != "qianyi-sun/loom"
        or registry_contract.SHA_RE.fullmatch(str(row.get("candidate_sha"))) is None
        or registry_contract.SHA_RE.fullmatch(str(row.get("candidate_tree"))) is None
        or registry_contract.DIGEST_RE.fullmatch(str(row.get("bundle_sha256"))) is None
        or type(row.get("bundle_size")) is not int
        or row["bundle_size"] < 1
        or row.get("bundle_path")
        != str(
            registry_contract.SYSTEM_CANDIDATE_ROOT
            / str(row.get("candidate_id"))
            / "candidate.bundle",
        )
        or not isinstance(image_digests, dict)
        or set(image_digests) != {"amd64", "arm64"}
        or any(
            registry_contract.IMAGE_DIGEST_RE.fullmatch(str(digest)) is None
            for digest in image_digests.values()
        )
        or not isinstance(row.get("imported_at"), str)
    ):
        raise RuntimeHostError("runtime registry candidate binding is invalid")
    return row


def _validate_registry_deployment_row(row: object) -> dict[str, Any]:
    fields = _registry_record_fields(registry_contract.DeploymentRecord)
    if (
        not isinstance(row, dict)
        or set(row) != fields
        or registry_contract.DEPLOYMENT_ID_RE.fullmatch(
            str(row.get("deployment_id")),
        )
        is None
        or registry_contract.PRINCIPAL_RE.fullmatch(str(row.get("principal_id"))) is None
        or registry_contract.ENV_ID_RE.fullmatch(str(row.get("env_id"))) is None
        or registry_contract.CANDIDATE_ID_RE.fullmatch(str(row.get("candidate_id"))) is None
        or type(row.get("expected_resource_generation")) is not int
        or row["expected_resource_generation"] < 1
        or row.get("phase") not in {*registry_contract.DEPLOY_PHASES, "failed"}
        or (
            row.get("previous_candidate_id") is not None
            and registry_contract.CANDIDATE_ID_RE.fullmatch(
                str(row["previous_candidate_id"]),
            )
            is None
        )
        or registry_contract.DIGEST_RE.fullmatch(str(row.get("request_digest"))) is None
        or not isinstance(row.get("created_at"), str)
        or not isinstance(row.get("updated_at"), str)
    ):
        raise RuntimeHostError("runtime registry deployment binding is invalid")
    return row


def _load_registry_cohort(path: Path = REGISTRY_SNAPSHOT_PATH) -> RegistryCohort:
    try:
        raw = registry_contract._read_regular(path, limit=16 * 1024 * 1024)
        metadata = path.lstat()
    except (OSError, registry_contract.RegistryError) as exc:
        raise RuntimeHostError("runtime registry snapshot is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise RuntimeHostError("runtime registry snapshot metadata is unsafe")
    try:
        payload = registry_contract.DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except registry_contract.RegistryError as exc:
        raise RuntimeHostError("runtime registry snapshot is invalid") from exc

    environment_rows: dict[str, dict[str, Any]] = {}
    runtime_ids: set[str] = set()
    all_ports: set[int] = set()
    for raw_environment in payload["environments"]:
        row = _validate_registry_environment_row(raw_environment)
        env_id = str(row["env_id"])
        runtime_id = str(row["runtime_id"])
        ports = cast(dict[str, int], row["ports"])
        if (
            env_id in environment_rows
            or runtime_id in runtime_ids
            or all_ports.intersection(ports.values())
        ):
            raise RuntimeHostError("runtime registry environment identity is duplicated")
        environment_rows[env_id] = row
        runtime_ids.add(runtime_id)
        all_ports.update(ports.values())

    candidates: dict[str, dict[str, Any]] = {}
    for raw_candidate in payload["candidates"]:
        row = _validate_registry_candidate_row(raw_candidate)
        candidate_id = str(row["candidate_id"])
        environment = environment_rows.get(str(row["env_id"]))
        if (
            candidate_id in candidates
            or environment is None
            or row["principal_id"] != environment["principal_id"]
        ):
            raise RuntimeHostError("runtime registry candidate ownership is invalid")
        candidates[candidate_id] = row

    deployments: dict[str, dict[str, Any]] = {}
    deployments_by_environment: dict[str, list[dict[str, Any]]] = {
        env_id: [] for env_id in environment_rows
    }
    for raw_deployment in payload["deployments"]:
        row = _validate_registry_deployment_row(raw_deployment)
        deployment_id = str(row["deployment_id"])
        environment = environment_rows.get(str(row["env_id"]))
        candidate = candidates.get(str(row["candidate_id"]))
        previous = row.get("previous_candidate_id")
        previous_candidate = None if previous is None else candidates.get(str(previous))
        if (
            deployment_id in deployments
            or environment is None
            or candidate is None
            or candidate["env_id"] != environment["env_id"]
            or candidate["principal_id"] != environment["principal_id"]
            or row["principal_id"] != environment["principal_id"]
            or (
                previous is not None
                and (
                    previous_candidate is None
                    or previous_candidate["env_id"] != environment["env_id"]
                )
            )
        ):
            raise RuntimeHostError("runtime registry deployment ownership is invalid")
        deployments[deployment_id] = row
        deployments_by_environment[str(environment["env_id"])].append(row)

    active_environments: list[RuntimeEnvironment] = []
    provisioning_environments: list[RuntimeEnvironment] = []
    for env_id, row in environment_rows.items():
        state_value = str(row["state"])
        current_candidate_id = row.get("current_candidate_id")
        current_candidate = (
            None if current_candidate_id is None else candidates.get(str(current_candidate_id))
        )
        environment_deployments = deployments_by_environment[env_id]
        live_deployments = [
            item for item in environment_deployments if item["phase"] not in {"committed", "failed"}
        ]
        committed_current = [
            item
            for item in environment_deployments
            if item["phase"] == "committed"
            and item["candidate_id"] == current_candidate_id
            and item["expected_resource_generation"] + 1 == row["resource_generation"]
            and item["applied_resource_generation"] == row["resource_generation"]
        ]
        if state_value == "ready":
            consistent = current_candidate_id is None and not live_deployments
        elif state_value == "active":
            consistent = (
                current_candidate is not None
                and current_candidate["env_id"] == env_id
                and not live_deployments
                and bool(committed_current)
            )
        elif state_value == "deploying":
            consistent = (
                len(live_deployments) == 1
                and live_deployments[0]["expected_resource_generation"]
                == row["resource_generation"]
                and live_deployments[0]["previous_candidate_id"] == current_candidate_id
            )
        else:
            consistent = not live_deployments
        if not consistent:
            raise RuntimeHostError("runtime registry environment state is inconsistent")

        selected_candidate: Mapping[str, Any] | None = None
        deployment_phase = "committed"
        capacity_eligible = state_value == "active"
        provisioning_eligible = capacity_eligible
        if state_value == "deploying":
            deployment = live_deployments[0]
            deployment_phase = str(deployment["phase"])
            if registry_contract.DEPLOY_PHASES.index(deployment_phase) >= (
                registry_contract.DEPLOY_PHASES.index("services-prepared")
            ):
                selected_candidate = candidates[str(deployment["candidate_id"])]
                provisioning_eligible = True
        elif capacity_eligible:
            selected_candidate = current_candidate
        if not provisioning_eligible or selected_candidate is None:
            continue
        runtime_environment = RuntimeEnvironment(
            env_id=env_id,
            runtime_id=str(row["runtime_id"]),
            layout_version=str(row["layout_version"]),
            state=state_value,
            resource_generation=int(
                deployment["applied_resource_generation"]
                if deployment_phase == "verified"
                and deployment["applied_resource_generation"] is not None
                else row["resource_generation"]
            ),
            service_user=str(row["service_user"]),
            service_group=str(row["service_group"]),
            uid=int(row["uid"]),
            gid=int(row["gid"]),
            systemd_instance=str(row["systemd_instance"]),
            state_root=_registry_path(row["state_root"], label="environment state_root"),
            runtime_root=_registry_path(
                row["runtime_root"],
                label="environment runtime_root",
            ),
            candidate_root=_registry_path(
                row["candidate_root"],
                label="environment candidate_root",
            ),
            control_plane_port=cast(dict[str, int], row["ports"])["control_plane"],
            slurm_user=str(row["slurm_user"]),
            slurm_account=str(row["slurm_account"]),
            slurm_qos=str(row["slurm_qos"]),
            candidate_id=str(selected_candidate["candidate_id"]),
            candidate_sha=str(selected_candidate["candidate_sha"]),
            candidate_tree=str(selected_candidate["candidate_tree"]),
            deployment_phase=deployment_phase,
        )
        provisioning_environments.append(runtime_environment)
        if capacity_eligible:
            active_environments.append(runtime_environment)
    if not active_environments and not provisioning_environments:
        raise RuntimeHostError("runtime registry provisionable cohort is empty")
    active_environments.sort(key=lambda item: item.runtime_id)
    provisioning_environments.sort(key=lambda item: item.runtime_id)
    return RegistryCohort(
        generation=int(payload["generation"]),
        payload_sha256=str(payload["payload_sha256"]),
        environments=tuple(active_environments),
        provisioning_environments=tuple(provisioning_environments),
    )


def _cohort() -> RegistryCohort:
    global _COHORT_CACHE
    if _COHORT_CACHE is None:
        _COHORT_CACHE = _load_registry_cohort()
    return _COHORT_CACHE


def _require_active_cohort() -> RegistryCohort:
    cohort = _cohort()
    if not cohort.environments:
        raise RuntimeHostError("runtime registry active cohort is empty")
    return cohort


def _registry_environment(runtime_id: str, *, provisioning: bool) -> RuntimeEnvironment:
    if registry_contract.RUNTIME_ID_RE.fullmatch(runtime_id) is None:
        raise RuntimeHostError("runtime environment identity is invalid")
    source = _cohort().provisioning_environments if provisioning else _cohort().environments
    matches = [item for item in source if item.runtime_id == runtime_id]
    if len(matches) != 1:
        raise RuntimeHostError("runtime environment is not in the required registry cohort")
    return matches[0]


def _sandboxes() -> tuple[str, ...]:
    return _cohort().sandboxes


def _instances() -> tuple[str, ...]:
    return _cohort().instances


def _provisioning_instances() -> tuple[str, ...]:
    return _cohort().provisioning_instances or _cohort().instances


def _adapter_timers() -> tuple[str, ...]:
    return tuple(f"loom-shared-capacity-adapter@{item}.timer" for item in _instances())


def _adapter_services() -> tuple[str, ...]:
    return tuple(f"loom-shared-capacity-adapter@{item}.service" for item in _instances())


def _all_timers() -> tuple[str, ...]:
    return (SUPERVISOR_TIMER, *_adapter_timers())


def _all_services() -> tuple[str, ...]:
    return (SUPERVISOR_SERVICE, *_adapter_services())


def _all_units() -> tuple[str, ...]:
    return (*_all_timers(), *_all_services())


def _unit_fragment_paths() -> dict[str, Path]:
    return {
        SUPERVISOR_SERVICE: SUPERVISOR_SERVICE_PATH,
        SUPERVISOR_TIMER: SUPERVISOR_TIMER_PATH,
        **{service: ADAPTER_SERVICE_PATH for service in _adapter_services()},
        **{timer: ADAPTER_TIMER_PATH for timer in _adapter_timers()},
    }


def _registry_binding() -> dict[str, object]:
    cohort = _cohort()
    return {
        "registry_generation": cohort.generation,
        "registry_payload_sha256": cohort.payload_sha256,
    }


def _runtime_manifest(cohort: RegistryCohort | None = None) -> dict[str, Any]:
    selected = _cohort() if cohort is None else cohort
    provisioned = selected.provisioning_environments or selected.environments
    environments = [
        {
            "env_id": environment.env_id,
            "runtime_id": environment.runtime_id,
            "layout_version": environment.layout_version,
            "state": environment.state,
            "deployment_phase": environment.deployment_phase,
            "capacity_eligible": environment in selected.environments,
            "resource_generation": environment.resource_generation,
            "service_user": environment.service_user,
            "service_group": environment.service_group,
            "uid": environment.uid,
            "gid": environment.gid,
            "systemd_instance": environment.systemd_instance,
            "state_root": str(environment.state_root),
            "runtime_root": str(environment.runtime_root),
            "candidate_root": str(environment.candidate_root),
            "control_plane_port": environment.control_plane_port,
            "slurm_user": environment.slurm_user,
            "slurm_account": environment.slurm_account,
            "slurm_qos": environment.slurm_qos,
            "candidate_id": environment.candidate_id,
            "candidate_sha": environment.candidate_sha,
            "candidate_tree": environment.candidate_tree,
            "instances": [f"{environment.runtime_id}-{pool}" for pool in POOLS],
        }
        for environment in sorted(
            provisioned,
            key=lambda item: item.runtime_id,
        )
    ]
    unsigned = {
        "schema_version": 1,
        "registry_generation": selected.generation,
        "registry_payload_sha256": selected.payload_sha256,
        "environments": environments,
    }
    return {
        **unsigned,
        "manifest_sha256": _sha256(_canonical_json(unsigned)),
    }


def _cohort_from_runtime_manifest(value: object) -> RegistryCohort:
    fields = {
        "schema_version",
        "registry_generation",
        "registry_payload_sha256",
        "environments",
        "manifest_sha256",
    }
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("schema_version") != 1
        or type(value.get("registry_generation")) is not int
        or value["registry_generation"] < 0
        or DIGEST_RE.fullmatch(str(value.get("registry_payload_sha256"))) is None
        or DIGEST_RE.fullmatch(str(value.get("manifest_sha256"))) is None
        or not isinstance(value.get("environments"), list)
        or not value["environments"]
    ):
        raise RuntimeHostError("runtime generation manifest is invalid")
    unsigned = {key: item for key, item in value.items() if key != "manifest_sha256"}
    if value["manifest_sha256"] != _sha256(_canonical_json(unsigned)):
        raise RuntimeHostError("runtime generation manifest digest is invalid")
    environment_fields = {
        "env_id",
        "runtime_id",
        "layout_version",
        "state",
        "deployment_phase",
        "capacity_eligible",
        "resource_generation",
        "service_user",
        "service_group",
        "uid",
        "gid",
        "systemd_instance",
        "state_root",
        "runtime_root",
        "candidate_root",
        "control_plane_port",
        "slurm_user",
        "slurm_account",
        "slurm_qos",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
        "instances",
    }
    environments: list[RuntimeEnvironment] = []
    provisioning_environments: list[RuntimeEnvironment] = []
    seen_env_ids: set[str] = set()
    seen_runtime_ids: set[str] = set()
    seen_instances: set[str] = set()
    for raw in value["environments"]:
        if not isinstance(raw, dict) or set(raw) != environment_fields:
            raise RuntimeHostError("runtime generation manifest environment is invalid")
        runtime_id = raw.get("runtime_id")
        env_id = raw.get("env_id")
        expected_instances = (
            [f"{runtime_id}-{pool}" for pool in POOLS] if isinstance(runtime_id, str) else []
        )
        if (
            not isinstance(env_id, str)
            or registry_contract.ENV_ID_RE.fullmatch(env_id) is None
            or env_id in seen_env_ids
            or not isinstance(runtime_id, str)
            or registry_contract.RUNTIME_ID_RE.fullmatch(runtime_id) is None
            or runtime_id in seen_runtime_ids
            or raw.get("systemd_instance") != runtime_id
            or raw.get("layout_version") not in {"legacy-v1", "dynamic-v1"}
            or raw.get("state") not in {"active", "deploying"}
            or raw.get("deployment_phase") not in registry_contract.DEPLOY_PHASES
            or type(raw.get("capacity_eligible")) is not bool
            or (raw["capacity_eligible"] is True and raw.get("state") != "active")
            or (raw["capacity_eligible"] is False and raw.get("state") != "deploying")
            or (raw.get("state") == "active" and raw.get("deployment_phase") != "committed")
            or (
                raw.get("state") == "deploying"
                and registry_contract.DEPLOY_PHASES.index(
                    str(raw.get("deployment_phase")),
                )
                < registry_contract.DEPLOY_PHASES.index("services-prepared")
            )
            or type(raw.get("resource_generation")) is not int
            or raw["resource_generation"] < 1
            or registry_contract.SAFE_NAME_RE.fullmatch(
                str(raw.get("service_user")),
            )
            is None
            or registry_contract.SAFE_NAME_RE.fullmatch(
                str(raw.get("service_group")),
            )
            is None
            or type(raw.get("uid")) is not int
            or type(raw.get("gid")) is not int
            or raw["uid"] < 1
            or raw["uid"] != raw["gid"]
            or type(raw.get("control_plane_port")) is not int
            or not 1024 <= raw["control_plane_port"] <= 65_535
            or any(
                registry_contract.SAFE_NAME_RE.fullmatch(str(raw.get(field))) is None
                for field in ("slurm_user", "slurm_account", "slurm_qos")
            )
            or registry_contract.CANDIDATE_ID_RE.fullmatch(
                str(raw.get("candidate_id")),
            )
            is None
            or SHA_RE.fullmatch(str(raw.get("candidate_sha"))) is None
            or SHA_RE.fullmatch(str(raw.get("candidate_tree"))) is None
            or raw.get("instances") != expected_instances
            or seen_instances.intersection(expected_instances)
        ):
            raise RuntimeHostError("runtime generation manifest binding is invalid")
        state_root = _registry_path(
            raw.get("state_root"),
            label="generation manifest state_root",
        )
        runtime_root = _registry_path(
            raw.get("runtime_root"),
            label="generation manifest runtime_root",
        )
        candidate_root = _registry_path(
            raw.get("candidate_root"),
            label="generation manifest candidate_root",
        )
        root_pairs = {
            "legacy-v1": (
                Path("/shared_work/loom/runtime/sandboxes") / runtime_id,
                Path("/shared_work/loom/candidates/sandboxes") / runtime_id,
            ),
            "dynamic-v1": (
                Path("/shared_work/loom/runtime/environments") / env_id,
                Path("/shared_work/loom/candidates/environments") / env_id,
            ),
        }
        if (runtime_root, candidate_root) != root_pairs[str(raw["layout_version"])]:
            raise RuntimeHostError(
                "runtime generation manifest registry roots are invalid",
            )
        seen_env_ids.add(env_id)
        seen_runtime_ids.add(runtime_id)
        seen_instances.update(expected_instances)
        environment = RuntimeEnvironment(
            env_id=env_id,
            runtime_id=runtime_id,
            layout_version=str(raw["layout_version"]),
            state=str(raw["state"]),
            resource_generation=int(raw["resource_generation"]),
            service_user=str(raw["service_user"]),
            service_group=str(raw["service_group"]),
            uid=int(raw["uid"]),
            gid=int(raw["gid"]),
            systemd_instance=runtime_id,
            state_root=state_root,
            runtime_root=runtime_root,
            candidate_root=candidate_root,
            control_plane_port=int(raw["control_plane_port"]),
            slurm_user=str(raw["slurm_user"]),
            slurm_account=str(raw["slurm_account"]),
            slurm_qos=str(raw["slurm_qos"]),
            candidate_id=str(raw["candidate_id"]),
            candidate_sha=str(raw["candidate_sha"]),
            candidate_tree=str(raw["candidate_tree"]),
            deployment_phase=str(raw["deployment_phase"]),
        )
        provisioning_environments.append(environment)
        if raw["capacity_eligible"] is True:
            environments.append(environment)
    if [item.runtime_id for item in provisioning_environments] != sorted(seen_runtime_ids):
        raise RuntimeHostError("runtime generation manifest order is invalid")
    if not environments:
        raise RuntimeHostError("runtime generation manifest active cohort is empty")
    return RegistryCohort(
        generation=int(value["registry_generation"]),
        payload_sha256=str(value["registry_payload_sha256"]),
        environments=tuple(environments),
        provisioning_environments=tuple(provisioning_environments),
    )


def _require_current_registry_binding(payload: Mapping[str, Any]) -> None:
    if (
        payload.get("registry_generation") != _cohort().generation
        or payload.get("registry_payload_sha256") != _cohort().payload_sha256
    ):
        raise RuntimeHostError("runtime registry generation binding drifted")


def _identity_has_sharedwork_group(environment: RuntimeEnvironment) -> bool:
    try:
        sharedwork_gid = grp.getgrnam("sharedwork").gr_gid
        group_ids = os.getgrouplist(environment.service_user, environment.gid)
    except KeyError:
        return False
    except OSError as exc:
        raise RuntimeHostError("dynamic service group readback failed") from exc
    return sharedwork_gid in group_ids


def _identity_mode_bits(metadata: os.stat_result, *, uid: int, gid: int) -> int:
    mode = stat.S_IMODE(metadata.st_mode)
    if metadata.st_uid == uid:
        return (mode >> 6) & 0o7
    if metadata.st_gid == gid:
        return (mode >> 3) & 0o7
    return mode & 0o7


def _validate_dynamic_storage_access(
    environment: RuntimeEnvironment,
    *,
    traversal_floor: Path = Path("/shared_work"),
) -> dict[str, object]:
    if environment.layout_version != "dynamic-v1":
        raise RuntimeHostError("dynamic storage readback received a legacy environment")
    try:
        user_by_name = pwd.getpwnam(environment.service_user)
        user_by_id = pwd.getpwuid(environment.uid)
        group_by_name = grp.getgrnam(environment.service_group)
        group_by_id = grp.getgrgid(environment.gid)
    except (KeyError, OSError) as exc:
        raise RuntimeHostError("dynamic service identity is unavailable") from exc
    if (
        user_by_name.pw_uid != environment.uid
        or user_by_name.pw_gid != environment.gid
        or user_by_id.pw_name != environment.service_user
        or group_by_name.gr_gid != environment.gid
        or group_by_id.gr_name != environment.service_group
        or _identity_has_sharedwork_group(environment)
    ):
        raise RuntimeHostError("dynamic service identity binding drifted")

    roots = (
        ("candidate_root", environment.candidate_root, 0o750),
        ("runtime_root", environment.runtime_root, 0o700),
    )
    readback: dict[str, str] = {}
    for label, path, expected_mode in roots:
        if not path.is_relative_to(traversal_floor) or path == traversal_floor:
            raise RuntimeHostError("dynamic storage root escaped its traversal boundary")
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise RuntimeHostError(f"dynamic {label} is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != environment.uid
            or metadata.st_gid != environment.gid
            or stat.S_IMODE(metadata.st_mode) != expected_mode
            or _identity_mode_bits(
                metadata,
                uid=environment.uid,
                gid=environment.gid,
            )
            != 0o7
        ):
            raise RuntimeHostError(f"dynamic {label} ownership or access drifted")
        for parent in path.parents:
            if parent == traversal_floor.parent:
                break
            try:
                parent_metadata = parent.lstat()
            except OSError as exc:
                raise RuntimeHostError(
                    f"dynamic {label} traversal parent is unavailable",
                ) from exc
            if (
                stat.S_ISLNK(parent_metadata.st_mode)
                or not stat.S_ISDIR(parent_metadata.st_mode)
                or not (
                    _identity_mode_bits(
                        parent_metadata,
                        uid=environment.uid,
                        gid=environment.gid,
                    )
                    & 0o1
                )
                or (
                    parent.is_relative_to(traversal_floor)
                    and stat.S_IMODE(parent_metadata.st_mode) & 0o002
                )
            ):
                raise RuntimeHostError(
                    f"dynamic {label} traversal is not independently accessible",
                )
            if parent == traversal_floor:
                break
        else:
            raise RuntimeHostError(f"dynamic {label} traversal boundary is missing")
        readback[label] = f"{metadata.st_uid}:{metadata.st_gid}:{expected_mode:04o}"
    return {
        "env_id": environment.env_id,
        "runtime_id": environment.runtime_id,
        "service_user": environment.service_user,
        "service_group": environment.service_group,
        "uid": environment.uid,
        "gid": environment.gid,
        "supplementary_sharedwork": False,
        **readback,
    }


@contextmanager
def _bound_cohort(cohort: RegistryCohort) -> Iterator[None]:
    global _COHORT_CACHE
    previous = _COHORT_CACHE
    _COHORT_CACHE = cohort
    try:
        yield
    finally:
        _COHORT_CACHE = previous


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_directory(path: Path, *, mode: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeHostError(f"unsafe directory: {path}")
    os.chown(path, 0, 0)
    os.chmod(path, mode)


def _atomic_write(path: Path, content: bytes, *, mode: int) -> None:
    private_parent = path.parent == STATE_ROOT or STATE_ROOT in path.parent.parents
    _ensure_directory(path.parent, mode=0o700 if private_parent else 0o755)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chown(temporary, 0, 0)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_symlink(path: Path, target: str) -> None:
    _ensure_directory(path.parent, mode=0o755)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        temporary.symlink_to(target)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _remove_path(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        shutil.rmtree(path)
    else:
        path.unlink()
    _fsync_directory(path.parent)


def _validate_profile_bytes(content: bytes) -> dict[str, Any]:
    try:
        raw = tomllib.loads(content.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeHostError("runtime-host profile is unavailable") from exc
    expected = {
        "schema_version",
        "expected_hostname",
        "candidate_parent",
        "current_link",
        "state_root",
        "registry_snapshot_path",
        "bootstrap_requires_zero_capacity",
        "positive_admission_requires_platform_health",
        "acceptance_contract_ttl_seconds",
        "acceptance_default_phase_ttl_seconds",
        "acceptance_mixed_non_loom_ttl_seconds",
        "acceptance_ttl_cleanup_seconds",
        "acceptance_phases",
        "slurm_domains",
    }
    expected_domains = {
        "oldlab": {
            "submit_host": "trt-EAI-OLDLAB-2",
            "controller": "TRT-EAI-OLDLAB-1",
        },
        "gb10": {
            "submit_host": "trt-gb10-1",
            "controller": "trt-gb10-1",
        },
    }
    if (
        set(raw) != expected
        or raw.get("schema_version") != 2
        or raw.get("expected_hostname") != EXPECTED_HOSTNAME
        or raw.get("candidate_parent") != str(CANDIDATE_PARENT)
        or raw.get("current_link") != str(CURRENT_LINK)
        or raw.get("state_root") != str(STATE_ROOT)
        or raw.get("registry_snapshot_path") != str(REGISTRY_SNAPSHOT_PATH)
        or raw.get("bootstrap_requires_zero_capacity") is not True
        or raw.get("positive_admission_requires_platform_health") is not True
        or raw.get("acceptance_contract_ttl_seconds") != ACCEPTANCE_CONTRACT_TTL_SECONDS
        or raw.get("acceptance_default_phase_ttl_seconds") != ACCEPTANCE_DEFAULT_PHASE_TTL_SECONDS
        or raw.get("acceptance_mixed_non_loom_ttl_seconds") != ACCEPTANCE_MIXED_NON_LOOM_TTL_SECONDS
        or raw.get("acceptance_ttl_cleanup_seconds") != ACCEPTANCE_TTL_CLEANUP_SECONDS
        or raw.get("acceptance_phases") != list(ACCEPTANCE_PHASES)
        or raw.get("slurm_domains") != expected_domains
    ):
        raise RuntimeHostError("runtime-host profile drifted from the closed contract")
    return raw


def _load_profile(path: Path = SOURCE_PROFILE) -> dict[str, Any]:
    try:
        return _validate_profile_bytes(path.read_bytes())
    except OSError as exc:
        raise RuntimeHostError("runtime-host profile is unavailable") from exc


def _load_candidate_profile(candidate: Candidate) -> dict[str, Any]:
    return _validate_profile_bytes(
        _read_candidate_file(
            candidate,
            SOURCE_PROFILE.relative_to(REPO_ROOT),
        ),
    )


def _candidate_identity(source: Path, sha: str) -> Candidate:
    if SHA_RE.fullmatch(sha) is None or not source.is_absolute():
        raise RuntimeHostError("candidate source and full SHA are required")
    try:
        metadata = source.lstat()
    except OSError as exc:
        raise RuntimeHostError("candidate source is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeHostError("candidate source must be a non-symlink directory")
    tree = _validate_repository(source, sha)
    candidate = Candidate(sha=sha, tree=tree, source=source)
    _read_candidate_file(candidate, Path("uv.lock"))
    _load_candidate_profile(candidate)
    return candidate


def _read_candidate_file(candidate: Candidate, relative: Path) -> bytes:
    raw_entry = _git_raw(
        candidate.source,
        "ls-tree",
        "-z",
        candidate.sha,
        "--",
        relative.as_posix(),
    )
    entries = _repository_entries(raw_entry, tree=True)
    entry = entries.get(relative.as_posix())
    if len(entries) != 1 or entry is None or entry[0] not in {"100644", "100755"}:
        raise RuntimeHostError("candidate deployment asset is unsafe")
    return _git_raw(
        candidate.source,
        "cat-file",
        "blob",
        f"{candidate.sha}:{relative.as_posix()}",
    )


def _render_service(template: bytes, candidate: Candidate) -> bytes:
    token = b"${GIT_SHA}"
    if template.count(token) != 4:
        raise RuntimeHostError("service template placeholder count is invalid")
    rendered = template.replace(token, candidate.sha.encode())
    exact_root = str(candidate.root).encode()
    if token in rendered or exact_root not in rendered:
        raise RuntimeHostError("service did not render to the exact candidate")
    if b"/opt/loom-shared-capacity/current" in rendered:
        raise RuntimeHostError("service references a mutable candidate pointer")
    return rendered


def _render_supervisor_config(candidate: Candidate) -> bytes:
    raw = _read_candidate_file(
        candidate,
        SUPERVISOR_CONFIG_SOURCE.relative_to(REPO_ROOT),
    )
    try:
        payload = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeHostError("candidate supervisor config is invalid") from exc
    instances = "\n".join(f'  "{instance}",' for instance in _instances())
    return (
        "\n".join(
            (
                f"schema_version = {payload['schema_version']}",
                f'state_db = "{payload["state_db"]}"',
                f'handoff_dir = "{payload["handoff_dir"]}"',
                f'observation_dir = "{payload["observation_dir"]}"',
                f'supervisor_state_path = "{payload["supervisor_state_path"]}"',
                f'audit_path = "{payload["audit_path"]}"',
                f'evidence_path = "{payload["evidence_path"]}"',
                f"global_slot_budget = {payload['global_slot_budget']}",
                f"global_pending_slot_budget = {payload['global_pending_slot_budget']}",
                "instances = [",
                instances,
                "]",
                "",
                "[pool_slot_budgets]",
                f"gb10 = {payload['pool_slot_budgets']['gb10']}",
                f"oldlab = {payload['pool_slot_budgets']['oldlab']}",
                "",
                "[pool_pending_slot_budgets]",
                f"gb10 = {payload['pool_pending_slot_budgets']['gb10']}",
                f"oldlab = {payload['pool_pending_slot_budgets']['oldlab']}",
                "",
            ),
        )
    ).encode()


def _registry_supervisor_base() -> dict[str, Any]:
    try:
        metadata = SUPERVISOR_BASE_CONFIG_PATH.lstat()
        raw = SUPERVISOR_BASE_CONFIG_PATH.read_bytes()
        payload = tomllib.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeHostError("fixed supervisor base config is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o644
    ):
        raise RuntimeHostError("fixed supervisor base config metadata drifted")
    return payload


def _render_registry_supervisor_config_for_instances(
    instances: Sequence[str],
) -> bytes:
    payload = _registry_supervisor_base()
    normalized = tuple(instances)
    if (
        not normalized
        or len(normalized) != len(set(normalized))
        or tuple(sorted(normalized)) != normalized
        or any(
            registry_contract.RUNTIME_ID_RE.fullmatch(instance.rpartition("-")[0]) is None
            or instance.rpartition("-")[2] not in POOLS
            for instance in normalized
        )
    ):
        raise RuntimeHostError("fixed supervisor instance cohort is invalid")
    instance_rows = "\n".join(f'  "{instance}",' for instance in normalized)
    try:
        return (
            "\n".join(
                (
                    f"schema_version = {payload['schema_version']}",
                    f'state_db = "{payload["state_db"]}"',
                    f'handoff_dir = "{payload["handoff_dir"]}"',
                    f'observation_dir = "{payload["observation_dir"]}"',
                    f'supervisor_state_path = "{payload["supervisor_state_path"]}"',
                    f'audit_path = "{payload["audit_path"]}"',
                    f'evidence_path = "{payload["evidence_path"]}"',
                    f"global_slot_budget = {payload['global_slot_budget']}",
                    f"global_pending_slot_budget = {payload['global_pending_slot_budget']}",
                    "instances = [",
                    instance_rows,
                    "]",
                    "",
                    "[pool_slot_budgets]",
                    f"gb10 = {payload['pool_slot_budgets']['gb10']}",
                    f"oldlab = {payload['pool_slot_budgets']['oldlab']}",
                    "",
                    "[pool_pending_slot_budgets]",
                    f"gb10 = {payload['pool_pending_slot_budgets']['gb10']}",
                    f"oldlab = {payload['pool_pending_slot_budgets']['oldlab']}",
                    "",
                ),
            )
            + "\n"
        ).encode("ascii")
    except (KeyError, TypeError, UnicodeEncodeError) as exc:
        raise RuntimeHostError("fixed supervisor base config is invalid") from exc


def _render_registry_supervisor_config() -> bytes:
    return _render_registry_supervisor_config_for_instances(_instances())


def _render_adapter_config(environment: RuntimeEnvironment, pool: str) -> bytes:
    if pool not in POOLS:
        raise RuntimeHostError("adapter pool is invalid")
    instance = f"{environment.runtime_id}-{pool}"
    max_slots = 120 if pool == "gb10" else 20
    return (
        "\n".join(
            (
                "schema_version = 1",
                f'sandbox = "{environment.runtime_id}"',
                f'environment = "sandbox-{environment.runtime_id}"',
                f'pool_name = "{pool}"',
                f'slurm_account = "{environment.slurm_account}"',
                f'slurm_qos = "{environment.slurm_qos}"',
                f'runtime_root = "{environment.runtime_root}"',
                f'candidate_root = "{environment.candidate_root}"',
                f'control_plane_url = "http://127.0.0.1:{environment.control_plane_port}"',
                f'admin_secret_file = "{environment.state_root}/secrets/admin.toml"',
                f'handoff_path = "{STATE_ROOT}/handoffs/current/{instance}.json"',
                f'observation_path = "{STATE_ROOT}/observations/{instance}.json"',
                f'adapter_state_path = "{STATE_ROOT}/adapters/{instance}.json"',
                f'sandbox_state_path = "{environment.state_root}/sandbox-state.json"',
                f'runtime_attestation_root = "{STATE_ROOT}/runtime-attestations"',
                f"max_slots_bound = {max_slots}",
                "timeout_seconds = 10",
                "",
            ),
        )
    ).encode()


def _desired_files(candidate: Candidate) -> dict[Path, tuple[bytes, int]]:
    files: dict[Path, tuple[bytes, int]] = {
        PROGRAM_PATH: (
            _read_candidate_file(
                candidate,
                Path("scripts/ops/shared_capacity_runtime_host.py"),
            ),
            0o755,
        ),
        CAPACITY_CONTRACT_PATH: (
            _read_candidate_file(
                candidate,
                Path("scripts/ops/developer_sandbox_capacity_contract.py"),
            ),
            0o644,
        ),
        REGISTRY_CONTRACT_PATH: (
            _read_candidate_file(
                candidate,
                Path("scripts/ops/developer_environment_registry.py"),
            ),
            0o644,
        ),
        PROFILE_PATH: (
            _read_candidate_file(
                candidate,
                SOURCE_PROFILE.relative_to(REPO_ROOT),
            ),
            0o600,
        ),
        SUPERVISOR_CONFIG_PATH: (
            _render_supervisor_config(candidate),
            0o600,
        ),
        ADAPTER_SERVICE_PATH: (
            _render_service(
                _read_candidate_file(
                    candidate,
                    ADAPTER_SERVICE_SOURCE.relative_to(REPO_ROOT),
                ),
                candidate,
            ),
            0o644,
        ),
        ADAPTER_TIMER_PATH: (
            _read_candidate_file(
                candidate,
                ADAPTER_TIMER_SOURCE.relative_to(REPO_ROOT),
            ),
            0o644,
        ),
        SUPERVISOR_SERVICE_PATH: (
            _render_service(
                _read_candidate_file(
                    candidate,
                    SUPERVISOR_SERVICE_SOURCE.relative_to(REPO_ROOT),
                ),
                candidate,
            ),
            0o644,
        ),
        SUPERVISOR_TIMER_PATH: (
            _read_candidate_file(
                candidate,
                SUPERVISOR_TIMER_SOURCE.relative_to(REPO_ROOT),
            ),
            0o644,
        ),
    }
    by_runtime = {
        item.runtime_id: item
        for item in (_cohort().provisioning_environments or _cohort().environments)
    }
    for instance in _provisioning_instances():
        sandbox, pool = instance.rsplit("-", 1)
        files[ADAPTER_CONFIG_ROOT / f"{instance}.toml"] = (
            _render_adapter_config(by_runtime[sandbox], pool),
            0o600,
        )
    try:
        compile(files[PROGRAM_PATH][0], str(PROGRAM_PATH), "exec")
        compile(
            files[CAPACITY_CONTRACT_PATH][0],
            str(CAPACITY_CONTRACT_PATH),
            "exec",
        )
        compile(
            files[REGISTRY_CONTRACT_PATH][0],
            str(REGISTRY_CONTRACT_PATH),
            "exec",
        )
    except (SyntaxError, ValueError) as exc:
        raise RuntimeHostError("candidate runtime-host Python asset is invalid") from exc
    return files


def plan(candidate: Candidate, operation: str) -> dict[str, Any]:
    desired = _desired_files(candidate)
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-runtime-host-plan",
        "operation": operation,
        "mutation_authorized": False,
        "host": EXPECTED_HOSTNAME,
        "candidate_sha": candidate.sha,
        "candidate_tree": candidate.tree,
        "candidate_root": str(candidate.root),
        "lockfile_sha256": _sha256(
            _read_candidate_file(candidate, Path("uv.lock")),
        ),
        "instances": list(_instances()),
        "provisioning_instances": list(_provisioning_instances()),
        **_registry_binding(),
        "slurm_domains": _load_candidate_profile(candidate)["slurm_domains"],
        "files": [
            {
                "path": str(path),
                "mode": f"{mode:04o}",
                "sha256": _sha256(content),
            }
            for path, (content, mode) in sorted(
                desired.items(),
                key=lambda item: str(item[0]),
            )
        ],
        "activation_order": [
            "journal-before-any-opt-or-systemd-mutation",
            "stop-and-disable-existing-services-and-timers",
            "materialize-exact-repo-and-frozen-venv",
            "publish-configs-and-exact-units",
            "leave-supervisor-and-registry-adapters-disabled-and-inactive",
            "closed-world-readback",
        ],
        "capacity_enabled_by_installer": False,
    }


def _require_live_host() -> None:
    if os.geteuid() != 0:
        raise RuntimeHostError("live convergence requires root")
    hostname = socket.gethostname().split(".", 1)[0].rstrip(".").lower()
    if hostname != EXPECTED_HOSTNAME:
        raise RuntimeHostError("live convergence is restricted to oldlab2")


@contextmanager
def _lock() -> Iterator[None]:
    descriptor = os.open(
        LOCK_PATH,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0:
            raise RuntimeHostError("installer lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _systemctl_state(unit: str) -> dict[str, bool]:
    enabled_result = _run(
        ("systemctl", "is-enabled", unit),
        expected={0, 1, 3, 4},
    )
    enabled = enabled_result.stdout.strip() in {"enabled", "enabled-runtime"}
    active_result = _run(
        ("systemctl", "is-active", unit),
        expected={0, 3, 4},
    )
    active = active_result.stdout.strip() == "active"
    return {"enabled": enabled, "active": active}


_MANAGED_UNIT_RE = re.compile(
    r"^loom-shared-capacity-(?:"
    r"supervisor\.(?:service|timer)|"
    r"adapter@([a-z0-9-]*)\.(?:service|timer)"
    r")$",
)


def _validate_managed_unit_rows(rows: Sequence[str]) -> set[str]:
    names = [row.split(maxsplit=1)[0] for row in rows if row.strip()]
    if len(names) != len(set(names)):
        raise RuntimeHostError("duplicate managed systemd unit readback")
    managed: set[str] = set()
    for name in names:
        match = _MANAGED_UNIT_RE.fullmatch(name)
        if match is None:
            raise RuntimeHostError("orphan shared-capacity systemd unit exists")
        instance = match.group(1)
        if instance not in {None, "", *_instances()}:
            raise RuntimeHostError("orphan shared-capacity adapter unit is loaded")
        managed.add(name)
    return managed


def _loaded_managed_units() -> set[str]:
    rows = _run(
        (
            "systemctl",
            "list-units",
            "--all",
            "--plain",
            "--no-legend",
            "loom-shared-capacity-*",
        ),
    ).stdout.splitlines()
    return _validate_managed_unit_rows(rows)


def _installed_managed_unit_files() -> set[str]:
    rows = _run(
        (
            "systemctl",
            "list-unit-files",
            "--all",
            "--plain",
            "--no-legend",
            "loom-shared-capacity-*",
        ),
    ).stdout.splitlines()
    return _validate_managed_unit_rows(rows)


def _unit_fragment(unit: str) -> tuple[str, str]:
    load_state = _run(
        ("systemctl", "show", unit, "--property=LoadState", "--value"),
    ).stdout.strip()
    fragment_path = _run(
        ("systemctl", "show", unit, "--property=FragmentPath", "--value"),
    ).stdout.strip()
    return load_state, fragment_path


def _validate_unit_fragment(unit: str, expected_path: Path) -> None:
    load_state, fragment_path = _unit_fragment(unit)
    if load_state != "loaded" or fragment_path != str(expected_path):
        raise RuntimeHostError(f"managed systemd fragment drifted: {unit}")


def _reject_orphan_unit_files() -> None:
    installed = _installed_managed_unit_files()
    if not REQUIRED_UNIT_FILES <= installed:
        raise RuntimeHostError("required shared-capacity unit file is missing")
    allowed = {*REQUIRED_UNIT_FILES, *_all_units()}
    if installed - allowed:
        raise RuntimeHostError("orphan shared-capacity unit file is installed")


def _reject_orphan_configs() -> None:
    if not ADAPTER_CONFIG_ROOT.exists():
        return
    expected = {f"{instance}.toml" for instance in _provisioning_instances()}
    entries = list(ADAPTER_CONFIG_ROOT.iterdir())
    actual = {path.name for path in entries}
    if actual - expected or any(
        path.name not in expected or path.is_symlink() or not path.is_file() for path in entries
    ):
        raise RuntimeHostError("orphan shared-capacity adapter config is installed")


def _capture_files(paths: Sequence[Path]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in paths:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            result[str(path)] = {"present": False}
            continue
        if stat.S_ISLNK(metadata.st_mode):
            result[str(path)] = {
                "present": True,
                "kind": "symlink",
                "target": os.readlink(path),
            }
        elif stat.S_ISREG(metadata.st_mode):
            result[str(path)] = {
                "present": True,
                "kind": "file",
                "mode": stat.S_IMODE(metadata.st_mode),
                "content_b64": base64.b64encode(path.read_bytes()).decode(),
            }
        else:
            raise RuntimeHostError(f"cannot snapshot unsafe path: {path}")
    return result


def _snapshot_paths() -> tuple[Path, ...]:
    return (
        PROGRAM_PATH,
        CAPACITY_CONTRACT_PATH,
        REGISTRY_CONTRACT_PATH,
        PROFILE_PATH,
        SUPERVISOR_CONFIG_PATH,
        *(ADAPTER_CONFIG_ROOT / f"{instance}.toml" for instance in _provisioning_instances()),
        *UNIT_PATHS,
        CURRENT_LINK,
        STATE_PATH,
    )


def _write_journal(
    candidate: Candidate,
    *,
    operation: str,
    admission_token: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    if operation not in {"install", "activate", "admit"}:
        raise RuntimeHostError("runtime-host transaction operation is invalid")
    if operation == "install":
        if admission_token is not None:
            raise RuntimeHostError("install transaction cannot own an admission token")
    elif (
        not isinstance(admission_token, str)
        or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
    ):
        raise RuntimeHostError("runtime-host admission binding is invalid")
    transaction_id = uuid.uuid4().hex
    _ensure_directory(STATE_ROOT, mode=0o700)
    _ensure_directory(INSTALLER_ROOT, mode=0o700)
    _ensure_directory(JOURNAL_ROOT, mode=0o700)
    path = JOURNAL_ROOT / f"{transaction_id}.json"
    staging_path = CANDIDATE_PARENT / f".install-{candidate.sha}-{transaction_id}"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "transaction_id": transaction_id,
        "operation": operation,
        "phase": "prepared",
        "candidate_sha": candidate.sha,
        "candidate_tree": candidate.tree,
        "candidate_previously_existed": (candidate.root.exists() or candidate.root.is_symlink()),
        "staging_path": str(staging_path),
        "started_at": datetime.now(UTC).isoformat(),
        **_registry_binding(),
        "runtime_manifest": _runtime_manifest(),
        "files": _capture_files(_snapshot_paths()),
        "units": {unit: _systemctl_state(unit) for unit in _all_units()},
    }
    if admission_token is not None:
        payload["admission_token"] = admission_token
    _atomic_write(path, _canonical_json(payload), mode=0o600)
    _atomic_write(
        ACTIVE_JOURNAL_PATH,
        _canonical_json({"transaction_id": transaction_id}),
        mode=0o600,
    )
    return path, payload


def _update_journal(path: Path, payload: dict[str, Any], phase: str) -> None:
    payload["phase"] = phase
    _atomic_write(path, _canonical_json(payload), mode=0o600)


def _prepare_rollback_recovery(
    path: Path,
    payload: dict[str, Any],
    candidate: Candidate,
) -> None:
    content = _read_candidate_file(
        candidate,
        Path("scripts/ops/shared_capacity_runtime_host.py"),
    )
    try:
        compile(content, str(RECOVERY_PROGRAM_PATH), "exec")
    except (SyntaxError, ValueError) as exc:
        raise RuntimeHostError("rollback recovery program is invalid") from exc
    _atomic_write(RECOVERY_PROGRAM_PATH, content, mode=0o700)
    payload["rollback_recovery_path"] = str(RECOVERY_PROGRAM_PATH)
    payload["rollback_recovery_sha256"] = _sha256(content)
    _update_journal(path, payload, "rollback-recovery-ready")


def _validate_rollback_recovery(payload: Mapping[str, Any]) -> None:
    digest = payload.get("rollback_recovery_sha256")
    if (
        payload.get("rollback_recovery_path") != str(RECOVERY_PROGRAM_PATH)
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        raise RuntimeHostError("rollback recovery binding is invalid")
    try:
        metadata = RECOVERY_PROGRAM_PATH.lstat()
        content = RECOVERY_PROGRAM_PATH.read_bytes()
    except OSError as exc:
        raise RuntimeHostError("rollback recovery program is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or _sha256(content) != digest
    ):
        raise RuntimeHostError("rollback recovery program is unsafe or drifted")
    try:
        compile(content, str(RECOVERY_PROGRAM_PATH), "exec")
    except (SyntaxError, ValueError) as exc:
        raise RuntimeHostError("rollback recovery program is invalid") from exc


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeHostError(f"{label} is unavailable") from exc
    if not isinstance(payload, dict):
        raise RuntimeHostError(f"{label} is invalid")
    return payload


def _platform_health_json(path: Path, *, label: str) -> dict[str, Any]:
    try:
        relative = path.relative_to(PLATFORM_HEALTH_ROOT)
    except ValueError as exc:
        raise RuntimeHostError(f"{label} path escaped its authority root") from exc
    flags = os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0)
    try:
        directory = os.open(PLATFORM_HEALTH_ROOT, flags)
    except OSError as exc:
        raise RuntimeHostError(f"{label} authority root is unavailable") from exc
    try:
        for part in relative.parts[:-1]:
            metadata = os.fstat(directory)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_uid, metadata.st_gid) != (0, 0)
                or stat.S_IMODE(metadata.st_mode) & 0o022
            ):
                raise RuntimeHostError(f"{label} authority directory is unsafe")
            child = os.open(part, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        metadata = os.fstat(directory)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise RuntimeHostError(f"{label} authority directory is unsafe")
        descriptor = os.open(
            relative.parts[-1],
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory,
        )
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
                or (opened.st_uid, opened.st_gid) != (0, 0)
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_size > 16 * 1024 * 1024
            ):
                raise RuntimeHostError(f"{label} authority file is unsafe")
            raw = b""
            while len(raw) <= 16 * 1024 * 1024:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    break
                raw += chunk
            if len(raw) > 16 * 1024 * 1024:
                raise RuntimeHostError(f"{label} authority file is too large")
        finally:
            os.close(descriptor)
    except RuntimeHostError:
        raise
    except OSError as exc:
        raise RuntimeHostError(f"{label} authority file is unavailable") from exc
    finally:
        os.close(directory)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeHostError(f"{label} authority file is invalid") from exc
    if not isinstance(payload, dict) or raw != _canonical_json(payload):
        raise RuntimeHostError(f"{label} authority file is not canonical")
    return payload


def _platform_health_timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise RuntimeHostError(f"{label} timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise RuntimeHostError(f"{label} timestamp is invalid") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise RuntimeHostError(f"{label} timestamp is invalid")
    return parsed.astimezone(UTC)


def _platform_health_now(now: datetime | None) -> datetime:
    current = datetime.now(UTC) if now is None else now
    if current.tzinfo is None or current.utcoffset() is None:
        raise RuntimeHostError("platform-health trusted clock is invalid")
    return current.astimezone(UTC)


def _candidate_platform_policy(
    candidate: Candidate,
    pool: str,
) -> tuple[dict[str, Any], str]:
    try:
        health = load_platform_health_contract(candidate.source)
        contract = load_capacity_policy(
            candidate.source,
            pool,
            expected_nodes=(
                tuple(health.host_aliases[node] for node in health.oldlab_nodes)
                if pool == "oldlab"
                else health.capacity_gb10_nodes
            ),
        )
    except CapacityContractError as exc:
        raise RuntimeHostError(str(exc)) from exc
    return dict(contract.values), contract.source_sha256


def _validate_gate6_observations(
    value: object,
    candidates: Mapping[str, Mapping[str, str]],
) -> bool:
    if not isinstance(value, dict) or set(value) != {
        "soak",
        "device_isolation",
        "cleanup",
    }:
        return False
    soak = value.get("soak")
    devices = value.get("device_isolation")
    cleanup = value.get("cleanup")
    soak_fields = {
        "started_at",
        "completed_at",
        "duration_seconds",
        "sample_count",
        "required_duration_seconds",
        "required_sample_count",
        "workloads",
        "trial_success_numerator",
        "trial_success_denominator",
        "trial_success_ratio",
        "minimum_trial_success_ratio",
        "trial_outcomes",
        "resource_envelope_breaches",
        "kube_api_healthy",
        "minio_quorum_healthy",
        "longhorn_healthy",
        "non_loom_slurm_healthy",
        "pair_headroom",
    }
    if (
        not isinstance(soak, dict)
        or set(soak) != soak_fields
        or not isinstance(soak.get("duration_seconds"), int)
        or isinstance(soak.get("duration_seconds"), bool)
        or soak["duration_seconds"] < 14_400
        or not isinstance(soak.get("sample_count"), int)
        or isinstance(soak.get("sample_count"), bool)
        or soak["sample_count"] < 120
        or soak.get("required_duration_seconds") != 14_400
        or soak.get("required_sample_count") != 120
        or soak.get("workloads") != ["loom", "non_loom_slurm", "kubernetes", "minio", "longhorn"]
        or not isinstance(soak.get("trial_success_numerator"), int)
        or isinstance(soak.get("trial_success_numerator"), bool)
        or soak["trial_success_numerator"] < len(_instances())
        or not isinstance(soak.get("trial_success_denominator"), int)
        or isinstance(soak.get("trial_success_denominator"), bool)
        or soak["trial_success_denominator"] < len(_instances())
        or not isinstance(soak.get("trial_success_ratio"), (int, float))
        or isinstance(soak.get("trial_success_ratio"), bool)
        or not 0.95 <= soak["trial_success_ratio"] <= 1
        or soak.get("minimum_trial_success_ratio") != 0.95
        or not isinstance(soak.get("trial_outcomes"), list)
        or soak.get("resource_envelope_breaches") != 0
        or any(
            soak.get(field) is not True
            for field in (
                "kube_api_healthy",
                "minio_quorum_healthy",
                "longhorn_healthy",
                "non_loom_slurm_healthy",
            )
        )
        or not isinstance(soak.get("pair_headroom"), list)
    ):
        return False
    try:
        started = _platform_health_timestamp(
            soak["started_at"],
            label="gate6 soak started_at",
        )
        completed = _platform_health_timestamp(
            soak["completed_at"],
            label="gate6 soak completed_at",
        )
    except RuntimeHostError:
        return False
    if int((completed - started).total_seconds()) != soak["duration_seconds"]:
        return False
    pair_fields = {
        "sandbox",
        "pool",
        "min_free_cpu_cores",
        "min_free_memory_bytes",
        "max_pid_usage_ratio",
        "observed_peak_concurrency",
        "within_reviewed_envelope",
    }
    pairs = soak["pair_headroom"]
    expected_pairs = {(sandbox, pool) for sandbox in _sandboxes() for pool in POOLS}
    outcome_fields = {
        "sandbox",
        "pool",
        "candidate_sha",
        "candidate_tree",
        "terminal_trial_count",
        "succeeded_trial_count",
        "failed_trial_count",
        "cancelled_trial_count",
        "retried_trial_count",
        "retry_attempt_count",
        "success_ratio",
    }
    outcome_count_fields = {
        "terminal_trial_count",
        "succeeded_trial_count",
        "failed_trial_count",
        "cancelled_trial_count",
        "retried_trial_count",
        "retry_attempt_count",
    }
    outcomes = soak["trial_outcomes"]
    if (
        len(outcomes) != len(_instances())
        or {(row.get("sandbox"), row.get("pool")) for row in outcomes if isinstance(row, dict)}
        != expected_pairs
        or any(
            not isinstance(row, dict)
            or set(row) != outcome_fields
            or not isinstance(row.get("sandbox"), str)
            or row["sandbox"] not in candidates
            or row.get("candidate_sha") != candidates[row["sandbox"]]["sha"]
            or row.get("candidate_tree") != candidates[row["sandbox"]]["tree"]
            or any(
                not isinstance(row.get(field), int)
                or isinstance(row.get(field), bool)
                or row[field] < 0
                for field in outcome_count_fields
            )
            or row["terminal_trial_count"] <= 0
            or row["terminal_trial_count"]
            != row["succeeded_trial_count"]
            + row["failed_trial_count"]
            + row["cancelled_trial_count"]
            or row["retried_trial_count"] > row["terminal_trial_count"]
            or row["retry_attempt_count"] < row["retried_trial_count"]
            or not isinstance(row.get("success_ratio"), (int, float))
            or isinstance(row.get("success_ratio"), bool)
            or not 0.95 <= row["success_ratio"] <= 1
            or row["success_ratio"] != row["succeeded_trial_count"] / row["terminal_trial_count"]
            for row in outcomes
        )
        or soak["trial_success_numerator"] != sum(row["succeeded_trial_count"] for row in outcomes)
        or soak["trial_success_denominator"] != sum(row["terminal_trial_count"] for row in outcomes)
        or soak["trial_success_ratio"]
        != soak["trial_success_numerator"] / soak["trial_success_denominator"]
    ):
        return False
    if (
        len(pairs) != len(_instances())
        or {(row.get("sandbox"), row.get("pool")) for row in pairs if isinstance(row, dict)}
        != expected_pairs
        or any(
            not isinstance(row, dict)
            or set(row) != pair_fields
            or not isinstance(row.get("min_free_cpu_cores"), (int, float))
            or isinstance(row.get("min_free_cpu_cores"), bool)
            or row["min_free_cpu_cores"] < 0
            or not isinstance(row.get("min_free_memory_bytes"), int)
            or isinstance(row.get("min_free_memory_bytes"), bool)
            or row["min_free_memory_bytes"] < 0
            or not isinstance(row.get("max_pid_usage_ratio"), (int, float))
            or isinstance(row.get("max_pid_usage_ratio"), bool)
            or not 0 <= row["max_pid_usage_ratio"] <= 1
            or not isinstance(row.get("observed_peak_concurrency"), int)
            or isinstance(row.get("observed_peak_concurrency"), bool)
            or row["observed_peak_concurrency"] < 1
            or row.get("within_reviewed_envelope") is not True
            for row in pairs
        )
    ):
        return False
    device_fields = {
        "sandbox",
        "pool",
        "job_id",
        "node",
        "host",
        "allocated_ids",
        "all_allocated_usable",
        "unallocated_denied",
        "proof",
    }
    proof_fields = {
        "method",
        "allocated_probe_container_ids",
        "denial_probe_container_ids",
        "observed_at",
    }
    if (
        not isinstance(devices, list)
        or len(devices) != len(_instances())
        or {(row.get("sandbox"), row.get("pool")) for row in devices if isinstance(row, dict)}
        != expected_pairs
        or any(
            not isinstance(row, dict)
            or set(row) != device_fields
            or not isinstance(row.get("job_id"), str)
            or not row["job_id"]
            or not isinstance(row.get("node"), str)
            or not row["node"]
            or not isinstance(row.get("host"), str)
            or not row["host"]
            or not isinstance(row.get("allocated_ids"), list)
            or len(row["allocated_ids"]) != len(set(row["allocated_ids"]))
            or any(not isinstance(item, str) or not item for item in row["allocated_ids"])
            or row.get("all_allocated_usable") is not True
            or row.get("unallocated_denied") is not True
            or not isinstance(row.get("proof"), dict)
            or set(row["proof"]) != proof_fields
            or not isinstance(row["proof"].get("method"), str)
            or not row["proof"]["method"]
            or not isinstance(row["proof"].get("allocated_probe_container_ids"), list)
            or not isinstance(row["proof"].get("denial_probe_container_ids"), list)
            for row in devices
        )
    ):
        return False
    try:
        for row in devices:
            _platform_health_timestamp(
                row["proof"]["observed_at"],
                label="gate6 device observed_at",
            )
    except RuntimeHostError:
        return False
    cleanup_fields = {
        "event",
        "checkpoint",
        "job_ids",
        "terminal_states",
        "observed_within_seconds",
        "maximum_cleanup_seconds",
        "live_jobs",
        "live_containers",
        "durable_trial_state",
        "retryable_interrupted_trials",
        "observed_at",
    }
    event_checkpoints = {
        "cancellation": "cancel_cleanup",
        "ttl_expiry": "ttl_cleanup",
        "worker_crash": "worker_crash",
        "submit_host_restart": "submit_host_restart",
    }
    if (
        not isinstance(cleanup, list)
        or len(cleanup) != len(event_checkpoints)
        or {row.get("event") for row in cleanup if isinstance(row, dict)} != set(event_checkpoints)
        or any(
            not isinstance(row, dict)
            or set(row) != cleanup_fields
            or row.get("checkpoint") != event_checkpoints.get(cast(str, row.get("event")))
            or not isinstance(row.get("job_ids"), list)
            or not row["job_ids"]
            or not isinstance(row.get("terminal_states"), list)
            or not row["terminal_states"]
            or not isinstance(row.get("observed_within_seconds"), int)
            or isinstance(row.get("observed_within_seconds"), bool)
            or not 0 <= row["observed_within_seconds"] <= 300
            or row.get("maximum_cleanup_seconds") != 300
            or row.get("live_jobs") != 0
            or row.get("live_containers") != 0
            or row.get("durable_trial_state") is not True
            or row.get("retryable_interrupted_trials") is not True
            for row in cleanup
        )
    ):
        return False
    try:
        for row in cleanup:
            _platform_health_timestamp(
                row["observed_at"],
                label="gate6 cleanup observed_at",
            )
    except RuntimeHostError:
        return False
    return True


def _validate_platform_health_activation_gate(
    candidate: Candidate,
    adapter_candidates: Mapping[str, Mapping[str, str]],
    *,
    now: datetime | None = None,
) -> tuple[str, str]:
    current = _platform_health_json(
        PLATFORM_HEALTH_ROOT / "current.json",
        label="platform-health current pointer",
    )
    session_id = current.get("session_id")
    evidence_path = PLATFORM_HEALTH_ROOT / "sessions" / str(session_id) / "evidence.json"
    if (
        set(current) != {"schema_version", "session_id", "evidence_path", "payload_sha256"}
        or current.get("schema_version") != 1
        or re.fullmatch(r"[0-9a-f]{32}", str(session_id)) is None
        or current.get("evidence_path") != str(evidence_path)
        or re.fullmatch(r"[0-9a-f]{64}", str(current.get("payload_sha256"))) is None
    ):
        raise RuntimeHostError("platform-health current pointer is invalid")
    evidence = _platform_health_json(
        evidence_path,
        label="platform-health evidence",
    )
    unsigned = {key: value for key, value in evidence.items() if key != "payload_sha256"}
    candidates = evidence.get("candidates")
    capacity = evidence.get("policy_capacity")
    recommendation = evidence.get("oldlab_capacity_recommendation")
    gate6_observations = evidence.get("gate6_observations")
    expected_policy = {pool: _candidate_platform_policy(candidate, pool) for pool in POOLS}
    try:
        health_contract = load_platform_health_contract(candidate.source)
    except CapacityContractError as exc:
        raise RuntimeHostError(str(exc)) from exc
    oldlab_capacity = capacity.get("oldlab") if isinstance(capacity, dict) else None
    gb10_capacity = capacity.get("gb10") if isinstance(capacity, dict) else None
    extra_capacity_fields = {
        "minimum_node_cpu_cores",
        "minimum_node_memory_bytes",
        "reserved_cpu_cores_per_node",
        "reserved_memory_mib_per_node",
    }
    capacity_fields = set(expected_policy["oldlab"][0]) | extra_capacity_fields
    derivation = recommendation.get("derivation") if isinstance(recommendation, dict) else None
    gb10_minimum_cpu = (
        gb10_capacity.get("minimum_node_cpu_cores") if isinstance(gb10_capacity, dict) else None
    )
    gb10_minimum_memory = (
        gb10_capacity.get("minimum_node_memory_bytes") if isinstance(gb10_capacity, dict) else None
    )
    gb10_headroom_is_typed = (
        isinstance(gb10_minimum_cpu, int)
        and not isinstance(gb10_minimum_cpu, bool)
        and isinstance(gb10_minimum_memory, int)
        and not isinstance(gb10_minimum_memory, bool)
    )
    expected_gb10_reserved_cpu = (
        cast(int, gb10_minimum_cpu) - expected_policy["gb10"][0]["requested_cpus"]
        if gb10_headroom_is_typed
        else None
    )
    expected_gb10_reserved_memory_mib = (
        cast(int, gb10_minimum_memory) // 1024**2
        - expected_policy["gb10"][0]["requested_memory_mib"]
        if gb10_headroom_is_typed
        else None
    )
    current_time = _platform_health_now(now)
    completed_at = _platform_health_timestamp(
        evidence.get("completed_at"),
        label="platform-health completed_at",
    )
    expires_at = _platform_health_timestamp(
        evidence.get("expires_at"),
        label="platform-health expires_at",
    )
    if (
        evidence.get("schema_version") != 1
        or evidence.get("kind") != "loom.developer-sandbox.platform-health-evidence"
        or set(evidence)
        != {
            "schema_version",
            "kind",
            "session_id",
            "candidates",
            "collector_host",
            "checkpoints",
            "mixed_jobs",
            "cancelled_jobs",
            "crashed_jobs",
            "node_intervals",
            "policy_capacity",
            "oldlab_capacity_recommendation",
            "gate6_observations",
            "zero_orphans",
            "completed_at",
            "expires_at",
            "payload_sha256",
        }
        or evidence.get("session_id") != session_id
        or evidence.get("payload_sha256") != current["payload_sha256"]
        or evidence.get("payload_sha256") != _sha256(_canonical_json(unsigned))
        or not isinstance(candidates, dict)
        or set(candidates) != set(_sandboxes())
        or any(
            not isinstance(candidates[sandbox], dict)
            or set(candidates[sandbox]) != {"sha", "tree"}
            or SHA_RE.fullmatch(str(candidates[sandbox].get("sha"))) is None
            or SHA_RE.fullmatch(str(candidates[sandbox].get("tree"))) is None
            for sandbox in _sandboxes()
        )
        or len({candidates[sandbox]["sha"] for sandbox in _sandboxes()}) != len(_sandboxes())
        or candidates != adapter_candidates
        or {"sha": candidate.sha, "tree": candidate.tree} not in candidates.values()
        or expires_at - completed_at != PLATFORM_HEALTH_EVIDENCE_TTL
        or completed_at > current_time + PLATFORM_HEALTH_MAX_CLOCK_SKEW
        or expires_at <= current_time + PLATFORM_HEALTH_ACTIVATION_MINIMUM_REMAINING
        or evidence.get("zero_orphans") is not True
        or not _validate_gate6_observations(gate6_observations, adapter_candidates)
        or not isinstance(capacity, dict)
        or set(capacity) != set(POOLS)
        or not isinstance(oldlab_capacity, dict)
        or not isinstance(gb10_capacity, dict)
        or set(oldlab_capacity) != capacity_fields
        or set(gb10_capacity) != capacity_fields
        or any(
            pool_capacity.get(key) != expected_policy[pool][0][key]
            for pool, pool_capacity in (
                ("oldlab", oldlab_capacity),
                ("gb10", gb10_capacity),
            )
            for key in expected_policy[pool][0]
        )
        or oldlab_capacity.get("reserved_cpu_cores_per_node")
        != health_contract.minimum_oldlab_free_cpu_cores
        or oldlab_capacity.get("reserved_memory_mib_per_node")
        != health_contract.minimum_oldlab_free_memory_bytes // 1024**2
        or not gb10_headroom_is_typed
        or gb10_capacity.get("reserved_cpu_cores_per_node") != expected_gb10_reserved_cpu
        or gb10_capacity.get("reserved_memory_mib_per_node") != expected_gb10_reserved_memory_mib
        or not isinstance(recommendation, dict)
        or set(recommendation)
        != {"schema_version", "pool", "source", "source_sha256", "values", "derivation"}
        or recommendation.get("schema_version") != 1
        or recommendation.get("pool") != "oldlab"
        or recommendation.get("source") != PLATFORM_POLICY_SOURCES["oldlab"]
        or recommendation.get("source_sha256") != expected_policy["oldlab"][1]
        or recommendation.get("values") != oldlab_capacity
        or not isinstance(derivation, dict)
        or set(derivation)
        != {
            "method",
            "measured_node_count",
            "minimum_observed_node_cpu_cores",
            "minimum_observed_node_memory_bytes",
            "minimum_observed_free_cpu_cores",
            "minimum_observed_free_memory_bytes",
            "minimum_required_free_cpu_cores",
            "minimum_required_free_memory_bytes",
            "maximum_allowed_cpu_busy_ratio",
            "all_nodes_passed",
        }
        or derivation.get("method") != "installed-shared-capacity-policy-v1"
        or derivation.get("measured_node_count") != len(health_contract.oldlab_nodes)
        or derivation.get("minimum_observed_node_cpu_cores")
        != oldlab_capacity.get("minimum_node_cpu_cores")
        or derivation.get("minimum_observed_node_memory_bytes")
        != oldlab_capacity.get("minimum_node_memory_bytes")
        or derivation.get("minimum_required_free_cpu_cores")
        != health_contract.minimum_oldlab_free_cpu_cores
        or derivation.get("minimum_required_free_memory_bytes")
        != health_contract.minimum_oldlab_free_memory_bytes
        or derivation.get("maximum_allowed_cpu_busy_ratio")
        != health_contract.maximum_cpu_busy_ratio
        or derivation.get("all_nodes_passed") is not True
        or not isinstance(derivation.get("minimum_observed_free_cpu_cores"), (int, float))
        or isinstance(derivation.get("minimum_observed_free_cpu_cores"), bool)
        or derivation["minimum_observed_free_cpu_cores"]
        < health_contract.minimum_oldlab_free_cpu_cores
        or not isinstance(derivation.get("minimum_observed_free_memory_bytes"), int)
        or isinstance(derivation.get("minimum_observed_free_memory_bytes"), bool)
        or derivation["minimum_observed_free_memory_bytes"]
        < health_contract.minimum_oldlab_free_memory_bytes
    ):
        raise RuntimeHostError("platform-health activation gate is not satisfied")
    rebuilt_sha256, gate6_sha256 = _verify_platform_health_authority_evidence(
        candidate,
        evidence_path,
    )
    if rebuilt_sha256 != evidence["payload_sha256"]:
        raise RuntimeHostError("platform-health authority evidence drifted")
    return str(evidence["payload_sha256"]), gate6_sha256


def _active_journal() -> tuple[Path, dict[str, Any]] | None:
    if not ACTIVE_JOURNAL_PATH.exists():
        return None
    pointer = _load_json(ACTIVE_JOURNAL_PATH, "active transaction pointer")
    transaction_id = pointer.get("transaction_id")
    if not isinstance(transaction_id, str) or not re.fullmatch(r"[0-9a-f]{32}", transaction_id):
        raise RuntimeHostError("active transaction pointer is invalid")
    path = JOURNAL_ROOT / f"{transaction_id}.json"
    return path, _load_json(path, "runtime-host transaction")


def _stop_units() -> None:
    for unit in _all_timers():
        _run(("systemctl", "stop", unit), expected={0, 5})
    for unit in _all_services():
        _run(("systemctl", "stop", unit), expected={0, 5})


def _restore_files(snapshot: Mapping[str, Any]) -> None:
    for raw_path, raw in snapshot.items():
        path = Path(raw_path)
        if not isinstance(raw, dict) or raw.get("present") is not True:
            _remove_path(path)
            continue
        kind = raw.get("kind")
        if kind == "symlink":
            target = raw.get("target")
            if not isinstance(target, str):
                raise RuntimeHostError("transaction symlink snapshot is invalid")
            _remove_path(path)
            _atomic_symlink(path, target)
        elif kind == "file":
            content = raw.get("content_b64")
            mode = raw.get("mode")
            if not isinstance(content, str) or type(mode) is not int:
                raise RuntimeHostError("transaction file snapshot is invalid")
            _atomic_write(path, base64.b64decode(content, validate=True), mode=mode)
        else:
            raise RuntimeHostError("transaction snapshot kind is invalid")


def _restore_units(states: Mapping[str, Any]) -> None:
    _run(("systemctl", "daemon-reload"))
    for unit in _all_units():
        state = states.get(unit)
        if not isinstance(state, dict):
            raise RuntimeHostError("transaction unit snapshot is invalid")
        if state.get("enabled") is True:
            _run(("systemctl", "enable", unit), expected={0, 1})
        else:
            _run(("systemctl", "disable", unit), expected={0, 1, 5})
        if state.get("active") is True:
            _run(("systemctl", "start", unit))
        else:
            _run(("systemctl", "stop", unit), expected={0, 5})


def _validate_transaction(
    path: Path,
    payload: dict[str, Any],
) -> tuple[str, str, str, str, Mapping[str, Any], Mapping[str, Any]]:
    manifest_cohort = _cohort_from_runtime_manifest(payload.get("runtime_manifest"))
    transaction_id = payload.get("transaction_id")
    operation = payload.get("operation")
    sha = payload.get("candidate_sha")
    tree = payload.get("candidate_tree")
    staging_path = payload.get("staging_path")
    if (
        not isinstance(transaction_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", transaction_id) is None
        or path != JOURNAL_ROOT / f"{transaction_id}.json"
        or operation not in {"install", "activate", "admit"}
        or not isinstance(sha, str)
        or SHA_RE.fullmatch(sha) is None
        or not isinstance(tree, str)
        or SHA_RE.fullmatch(tree) is None
        or type(payload.get("candidate_previously_existed")) is not bool
        or staging_path != str(CANDIDATE_PARENT / f".install-{sha}-{transaction_id}")
        or payload.get("registry_generation") != manifest_cohort.generation
        or payload.get("registry_payload_sha256") != manifest_cohort.payload_sha256
    ):
        raise RuntimeHostError("runtime-host transaction ownership is invalid")
    files = payload.get("files")
    units = payload.get("units")
    with _bound_cohort(manifest_cohort):
        if (
            not isinstance(files, dict)
            or set(files) != {str(item) for item in _snapshot_paths()}
            or not isinstance(units, dict)
            or set(units) != set(_all_units())
        ):
            raise RuntimeHostError("runtime-host transaction snapshot is invalid")
    for state in units.values():
        if (
            not isinstance(state, dict)
            or set(state) != {"enabled", "active"}
            or type(state.get("enabled")) is not bool
            or type(state.get("active")) is not bool
        ):
            raise RuntimeHostError("runtime-host transaction unit state is invalid")
    return transaction_id, operation, sha, tree, files, units


def _restore_local_transaction(
    path: Path,
    payload: dict[str, Any],
    *,
    remove_candidate: bool,
) -> tuple[str, str, str, str]:
    manifest_cohort = _cohort_from_runtime_manifest(payload.get("runtime_manifest"))
    with _bound_cohort(manifest_cohort):
        transaction_id, operation, sha, tree, files, units = _validate_transaction(
            path,
            payload,
        )
        _stop_units()
        _remove_path(Path(str(payload["staging_path"])))
        _restore_files(files)
        _restore_units(units)
        if remove_candidate and payload.get("candidate_previously_existed") is False:
            _remove_path(CANDIDATE_PARENT / sha)
    return transaction_id, operation, sha, tree


def _restore_transaction(
    path: Path,
    payload: dict[str, Any],
) -> None:
    manifest_cohort = _cohort_from_runtime_manifest(payload.get("runtime_manifest"))
    with _bound_cohort(manifest_cohort):
        _restore_transaction_bound(path, payload)


def _restore_transaction_bound(
    path: Path,
    payload: dict[str, Any],
) -> None:
    operation = payload.get("operation")
    admission_token = payload.get("admission_token")
    if operation in {"activate", "admit"}:
        if (
            not isinstance(admission_token, str)
            or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
        ):
            raise RuntimeHostError("runtime-host admission binding is invalid")
        if operation == "admit":
            candidate = Candidate(
                sha=str(payload.get("candidate_sha")),
                tree=str(payload.get("candidate_tree")),
                source=CANDIDATE_PARENT / str(payload.get("candidate_sha")) / "repo",
            )
            _close_activation_admission(candidate, admission_token)
            _drain_activated_capacity(candidate, admission_token)
            _verify_activated_capacity_drained(candidate, admission_token)
    _transaction_id, operation, sha, tree = _restore_local_transaction(
        path,
        payload,
        remove_candidate=True,
    )
    _update_journal(path, payload, "rolled-back")
    if operation == "activate":
        _open_activation_admission(
            Candidate(
                sha=sha,
                tree=tree,
                source=CANDIDATE_PARENT / sha / "repo",
            ),
            str(admission_token),
        )
    ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
    _fsync_directory(INSTALLER_ROOT)


def _recover_orphan() -> None:
    active = _active_journal()
    if active is not None:
        path, payload = active
        if payload.get("operation") == "install" and str(
            payload.get("phase", ""),
        ).startswith("rollback-"):
            _resume_activated_rollback(path, payload)
        elif payload.get("operation") == "admit" and payload.get("phase") in {
            "admission-authorized",
            "admission-open",
            "state-activated",
        }:
            try:
                _resume_admission(path, payload)
            except Exception:
                _restore_transaction(path, payload)
                raise
        elif payload.get("phase") == "committed":
            ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
            _fsync_directory(INSTALLER_ROOT)
        else:
            _restore_transaction(path, payload)
    _recover_acceptance_operation()


def _reject_orphan_stages() -> None:
    if not CANDIDATE_PARENT.exists():
        return
    for path in CANDIDATE_PARENT.iterdir():
        if path.name.startswith(".install-"):
            raise RuntimeHostError("unjournaled candidate staging path exists")


def _make_read_only(root: Path) -> None:
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        os.chown(current_path, 0, 0)
        os.chmod(current_path, stat.S_IMODE(current_path.stat().st_mode) & ~0o222)
        for name in (*directories, *files):
            path = current_path / name
            metadata = path.lstat()
            os.lchown(path, 0, 0)
            if not stat.S_ISLNK(metadata.st_mode):
                os.chmod(path, stat.S_IMODE(metadata.st_mode) & ~0o222)


def _verify_installed_candidate(candidate: Candidate) -> None:
    try:
        root_metadata = candidate.root.lstat()
    except OSError as exc:
        raise RuntimeHostError("installed candidate root is unavailable") from exc
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise RuntimeHostError("installed candidate root is unsafe")
    if not candidate.repo.is_dir() or candidate.repo.is_symlink():
        raise RuntimeHostError("installed candidate repo is unavailable")
    if _validate_repository(candidate.repo, candidate.sha) != candidate.tree:
        raise RuntimeHostError("installed candidate tree drifted")
    python = candidate.venv / "bin/python"
    if not python.is_file():
        raise RuntimeHostError("installed frozen venv is unavailable")
    for current, directories, files in os.walk(candidate.root, followlinks=False):
        for path in (
            Path(current),
            *(Path(current) / name for name in (*directories, *files)),
        ):
            metadata = path.lstat()
            if (metadata.st_uid, metadata.st_gid) != (0, 0):
                raise RuntimeHostError("installed candidate ownership drifted")
            if not stat.S_ISLNK(metadata.st_mode) and metadata.st_mode & 0o222:
                raise RuntimeHostError("installed candidate is writable")


def _materialize_candidate(candidate: Candidate, staging_path: Path) -> bool:
    expected_prefix = f".install-{candidate.sha}-"
    if (
        staging_path.parent != CANDIDATE_PARENT
        or not staging_path.name.startswith(expected_prefix)
        or staging_path.exists()
        or staging_path.is_symlink()
    ):
        raise RuntimeHostError("candidate staging path is invalid")
    _ensure_directory(STATE_ROOT, mode=0o700)
    _ensure_directory(INSTALLER_ROOT, mode=0o700)
    _ensure_directory(INSTALLER_ROOT / "uv-cache", mode=0o700)
    _ensure_directory(CANDIDATE_PARENT, mode=0o755)
    if candidate.root.exists() or candidate.root.is_symlink():
        _verify_installed_candidate(candidate)
        return False
    try:
        _run(
            (
                "git",
                "--no-replace-objects",
                "-c",
                "protocol.file.allow=always",
                "-c",
                "credential.helper=",
                "-c",
                "core.sshCommand=/usr/bin/false",
                "clone",
                "--no-hardlinks",
                "--no-checkout",
                str(candidate.source),
                str(staging_path / "repo"),
            ),
            env=_git_environment(),
        )
        _run(
            (
                "git",
                "--no-replace-objects",
                "-c",
                "credential.helper=",
                "-C",
                str(staging_path / "repo"),
                "checkout",
                "--detach",
                candidate.sha,
            ),
            env=_git_environment(),
        )
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "HOME": "/nonexistent",
            "XDG_CONFIG_HOME": "/nonexistent",
            "UV_PROJECT_ENVIRONMENT": str(staging_path / "venv"),
            "UV_CACHE_DIR": str(INSTALLER_ROOT / "uv-cache"),
            "UV_NO_PROGRESS": "1",
        }
        _run(
            (
                "uv",
                "sync",
                "--frozen",
                "--no-dev",
                "--project",
                str(staging_path / "repo"),
            ),
            env=env,
        )
        installed = Candidate(
            sha=candidate.sha,
            tree=candidate.tree,
            source=staging_path / "repo",
        )
        if _validate_repository(installed.source, installed.sha) != installed.tree:
            raise RuntimeHostError("materialized candidate tree drifted")
        _make_read_only(staging_path)
        os.rename(staging_path, candidate.root)
        _fsync_directory(CANDIDATE_PARENT)
    finally:
        if staging_path.exists() or staging_path.is_symlink():
            _remove_path(staging_path)
    _verify_installed_candidate(candidate)
    return True


def _publish_files(candidate: Candidate) -> None:
    _ensure_directory(CAPACITY_CONTRACT_PATH.parent.parent, mode=0o755)
    _ensure_directory(CAPACITY_CONTRACT_PATH.parent, mode=0o755)
    for path, (content, mode) in _desired_files(candidate).items():
        _atomic_write(path, content, mode=mode)
    _atomic_symlink(CURRENT_LINK, f"candidates/{candidate.sha}")


def _publish_unit_state() -> None:
    _run(("systemctl", "daemon-reload"))
    for unit in _all_units():
        _run(("systemctl", "disable", "--now", unit), expected={0, 1, 5})
    _stop_units()


def _service_result(unit: str) -> tuple[str, str]:
    result = _run(
        ("systemctl", "show", unit, "--property=Result", "--value"),
    ).stdout.strip()
    status = _run(
        ("systemctl", "show", unit, "--property=ExecMainStatus", "--value"),
    ).stdout.strip()
    return result, status


def _run_candidate_python(
    candidate: Candidate,
    code: str,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    expected_arguments = _EMBEDDED_PROGRAM_ARGUMENT_COUNTS.get(code)
    if expected_arguments is None or len(args) != expected_arguments:
        raise RuntimeHostError("embedded candidate program argument contract is invalid")
    try:
        compile(code, "<shared-capacity-runtime-host-embedded>", "exec")
    except SyntaxError as exc:
        raise RuntimeHostError("embedded candidate program is invalid") from exc
    return _run(
        (
            str(candidate.venv / "bin/python"),
            "-I",
            "-B",
            "-c",
            code,
            str(candidate.repo),
            *args,
        ),
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "HOME": "/nonexistent",
            "XDG_CONFIG_HOME": "/nonexistent",
        },
    )


def _candidate_broker_state_db(candidate: Candidate) -> Path:
    try:
        payload = tomllib.loads(
            _read_candidate_file(
                candidate,
                SUPERVISOR_CONFIG_SOURCE.relative_to(REPO_ROOT),
            ).decode("utf-8", errors="strict"),
        )
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeHostError("candidate supervisor config is invalid") from exc
    raw = payload.get("state_db")
    if not isinstance(raw, str):
        raise RuntimeHostError("candidate broker authority path is invalid")
    path = Path(raw)
    if not path.is_absolute() or ".." in path.parts:
        raise RuntimeHostError("candidate broker authority path is invalid")
    return path


def _validate_candidate_sha_set(
    candidate_shas: Mapping[str, Any],
) -> dict[str, str]:
    if (
        set(candidate_shas) != set(_sandboxes())
        or any(
            not isinstance(candidate_shas.get(sandbox), str)
            or SHA_RE.fullmatch(str(candidate_shas[sandbox])) is None
            for sandbox in _sandboxes()
        )
        or len({str(candidate_shas[sandbox]) for sandbox in _sandboxes()}) != len(_sandboxes())
    ):
        raise RuntimeHostError("sandbox candidate SHA set is invalid")
    return {sandbox: str(candidate_shas[sandbox]) for sandbox in _sandboxes()}


def _retirement_request_ids(
    report: Mapping[str, Any],
    candidate_shas: Mapping[str, Any],
) -> tuple[str, ...]:
    exact_shas = _validate_candidate_sha_set(candidate_shas)
    records = report.get("requests")
    if not isinstance(records, list):
        raise RuntimeHostError("broker retirement report is invalid")
    by_instance: dict[str, list[tuple[Mapping[str, Any], Mapping[str, Any]]]] = {
        instance: [] for instance in _instances()
    }
    request_ids: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        request = record.get("request")
        lease = record.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        request_id = request.get("id")
        instance = f"{request.get('sandbox')}-{request.get('pool')}"
        if (
            not isinstance(request_id, str)
            or not request_id
            or request_id in request_ids
            or instance not in by_instance
        ):
            raise RuntimeHostError("broker retirement request binding is invalid")
        request_ids.add(request_id)
        by_instance[instance].append((request, lease))

    retire: list[str] = []
    for instance, lane_records in by_instance.items():
        sandbox, _pool = instance.rsplit("-", 1)
        expected_sha = exact_shas[sandbox]
        exact = [
            (request, lease)
            for request, lease in lane_records
            if request.get("candidate_sha") == expected_sha
        ]
        if not exact:
            raise RuntimeHostError(f"broker retirement lane is missing: {instance}")
        nonterminal = [
            (request, lease)
            for request, lease in lane_records
            if request.get("state") != "terminal"
        ]
        if len(nonterminal) > 1:
            raise RuntimeHostError(f"broker retirement lane is ambiguous: {instance}")
        if nonterminal:
            request, _lease = nonterminal[0]
            if request.get("candidate_sha") != expected_sha:
                raise RuntimeHostError(
                    f"broker retirement lane belongs to another candidate: {instance}",
                )
            retire.append(str(request["id"]))
    return tuple(retire)


def _retirement_is_drained(
    report: Mapping[str, Any],
    candidate_shas: Mapping[str, Any],
) -> bool:
    exact_shas = _validate_candidate_sha_set(candidate_shas)
    outstanding = _retirement_request_ids(report, exact_shas)
    records = report.get("requests")
    aggregate = report.get("aggregate")
    if not isinstance(records, list) or not isinstance(aggregate, dict):
        raise RuntimeHostError("broker retirement aggregate is invalid")
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        request = record.get("request")
        lease = record.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        sandbox = request.get("sandbox")
        pool = request.get("pool")
        if sandbox not in exact_shas or f"{sandbox}-{pool}" not in _instances():
            continue
        if request.get("candidate_sha") != exact_shas[str(sandbox)]:
            continue
        if request.get("state") != "terminal" or any(
            lease.get(field) != 0
            for field in (
                "granted_slots",
                "pending_slots",
                "active_slots",
                "draining_slots",
                "committed_slots",
            )
        ):
            return False
    if outstanding:
        return False
    return not any(
        aggregate.get(field) != 0
        for field in (
            "granted_slots",
            "pending_slots",
            "active_slots",
            "draining_slots",
            "committed_slots",
        )
    )


def _validate_zero_broker_handoffs(
    report: Mapping[str, Any],
    selected: Mapping[str, Any],
    candidate_shas: Mapping[str, Any],
) -> None:
    exact_shas = _validate_candidate_sha_set(candidate_shas)
    if set(selected) != set(_instances()):
        raise RuntimeHostError("broker activation preflight is not closed-world")
    records = report.get("requests")
    aggregate = report.get("aggregate")
    if not isinstance(records, list) or not isinstance(aggregate, dict):
        raise RuntimeHostError("broker activation report is invalid")
    requests: dict[str, Mapping[str, Any]] = {}
    leases: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeHostError("broker activation request is invalid")
        request = record.get("request")
        lease = record.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise RuntimeHostError("broker activation request is invalid")
        request_id = request.get("id")
        if not isinstance(request_id, str) or request_id in leases:
            raise RuntimeHostError("broker activation request binding is invalid")
        requests[request_id] = request
        leases[request_id] = lease
    for instance in _instances():
        sandbox, _pool = instance.rsplit("-", 1)
        expected_sha = exact_shas[sandbox]
        handoff = selected.get(instance)
        if (
            not isinstance(handoff, dict)
            or handoff.get("enabled") is not False
            or handoff.get("min_slots") != 0
            or handoff.get("max_slots") != 0
            or handoff.get("candidate_sha") != expected_sha
        ):
            raise RuntimeHostError(
                "activation requires registry-closed zero-capacity handoffs",
            )
        request_id = str(handoff.get("request_id"))
        request = requests.get(request_id)
        lease = leases.get(request_id)
        if (
            request is None
            or request.get("state") != "terminal"
            or request.get("candidate_sha") != expected_sha
        ):
            raise RuntimeHostError("activation requires terminal zero-capacity requests")
        if lease is None or any(
            lease.get(field) != 0 for field in ("pending_slots", "active_slots", "draining_slots")
        ):
            raise RuntimeHostError("broker activation lease is not fully drained")
    if any(
        aggregate.get(field) != 0
        for field in (
            "committed_slots",
            "pending_slots",
            "active_slots",
            "draining_slots",
        )
    ):
        raise RuntimeHostError("broker activation aggregate is not fully drained")


def _validate_zero_cp_policy(
    policy: Mapping[str, Any],
    *,
    candidate_sha: str,
) -> None:
    actuator = policy.get("actuator_config")
    if (
        policy.get("enabled") is not False
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != 0
        or not isinstance(actuator, dict)
        or actuator.get("candidate_sha") != candidate_sha
    ):
        raise RuntimeHostError("control-plane autoscaler policy is not disabled")
    for field in (
        "last_pending_slots",
        "last_actual_slots",
        "last_draining_slots",
        "last_occupied_slots",
        "last_queued_slots",
    ):
        if policy.get(field) != 0:
            raise RuntimeHostError("control-plane autoscaler still has live capacity")


def _validate_zero_worker_status(
    payload: Mapping[str, Any],
    *,
    environment: str,
    pool_name: str,
) -> None:
    summary = payload.get("summary")
    jobs = payload.get("jobs")
    if not isinstance(summary, list) or not isinstance(jobs, list):
        raise RuntimeHostError("control-plane worker status is invalid")
    matching_summary = [
        item
        for item in summary
        if isinstance(item, dict)
        and item.get("environment") == environment
        and item.get("pool_name") == pool_name
    ]
    if len(matching_summary) > 1:
        raise RuntimeHostError("control-plane worker summary is duplicated")
    for item in matching_summary:
        if any(
            item.get(field) != 0
            for field in (
                "desired_slots",
                "active_slots",
                "pending_slots",
                "stale_slots",
                "running_jobs",
                "pending_jobs",
                "stale_jobs",
            )
        ):
            raise RuntimeHostError("control-plane worker summary is not drained")
    for item in jobs:
        if not isinstance(item, dict):
            raise RuntimeHostError("control-plane worker job status is invalid")
        if (
            item.get("environment") == environment
            and item.get("pool_name") == pool_name
            and item.get("state") in {"pending", "running"}
        ):
            raise RuntimeHostError("control-plane still has a live Slurm worker job")


def _validate_zero_adapter_state(
    handoff: Any,
    state: Mapping[str, Any],
) -> None:
    if (
        getattr(handoff, "enabled", None) is not False
        or getattr(handoff, "min_slots", None) != 0
        or getattr(handoff, "max_slots", None) != 0
    ):
        raise RuntimeHostError("bootstrap adapter handoff is not disabled at zero")
    if any(state.get(field) != 0 for field in ("pending_slots", "active_slots", "draining_slots")):
        raise RuntimeHostError("bootstrap adapter state still has live capacity")


_BROKER_PREFLIGHT = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
from scripts.ops.shared_capacity_runtime_host import _validate_zero_broker_handoffs
from scripts.ops.shared_capacity_supervisor import (
    _publication_handoffs,
    _validate_report_budgets,
    load_config,
)
config = load_config(Path(sys.argv[2]))
candidate_shas = json.loads(sys.argv[3])
broker = SharedCapacityBroker(config.state_db)
broker.close_admission(sys.argv[4])
report = broker.status()
_validate_report_budgets(report, config)
selected = _publication_handoffs(report, config)
_validate_zero_broker_handoffs(report, selected, candidate_shas)
"""

_BROKER_OPEN = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
SharedCapacityBroker(Path(sys.argv[2])).open_admission(sys.argv[3])
"""

_BROKER_CLOSE = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
SharedCapacityBroker(Path(sys.argv[2])).close_admission(sys.argv[3])
"""

_BROKER_ADMISSION_READBACK = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
broker = SharedCapacityBroker(Path(sys.argv[2]))
broker.initialize()
connection = broker._connect()
try:
    fence = broker._admission_fence(connection)
finally:
    connection.close()
print("open" if fence is None else fence)
"""

_BROKER_ENVIRONMENT_CLOSE = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
broker = SharedCapacityBroker(Path(sys.argv[2]))
broker.close_environment_admission(sys.argv[3], sys.argv[4])
print(broker.environment_admission_fence(sys.argv[3]) or "open")
"""

_BROKER_ENVIRONMENT_OPEN = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
broker = SharedCapacityBroker(Path(sys.argv[2]))
broker.open_environment_admission(sys.argv[3], sys.argv[4])
print(broker.environment_admission_fence(sys.argv[3]) or "open")
"""

_BROKER_ENVIRONMENT_ROTATE = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
broker = SharedCapacityBroker(Path(sys.argv[2]))
broker.rotate_environment_admission(sys.argv[3], sys.argv[4], sys.argv[5])
print(broker.environment_admission_fence(sys.argv[3]) or "open")
"""

_BROKER_ENVIRONMENT_READBACK = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
print(
    SharedCapacityBroker(Path(sys.argv[2])).environment_admission_fence(sys.argv[3])
    or "open"
)
"""

_PLATFORM_HEALTH_AUTHORITY_REBUILD = """
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts.ops.developer_sandbox_platform_health_authority import (
    _load_receipts,
    _load_samples,
    _secure_json,
    _verify_checkpoints,
    load_config,
)
from scripts.ops.developer_sandbox_live_acceptance import (
    NONEXCLUSIVE_SCHEMA,
    POOLS,
    SLURM_POLICY_STATE_ROOT,
    _canonical_bytes,
    _checkpoint_payload,
    _gate6_matrix_path,
    _gate6_runtime_domain_bindings,
    _load_schema,
    _phase_checkpoints,
    _registry_sandboxes,
    _secure_json_load,
    _session_dir,
    _session_state_unlocked,
    _trusted_authority_json,
    gate6_verifier,
    verify_evidence,
)
repo = Path(sys.argv[1])
evidence_path = Path(sys.argv[2])
config = load_config(
    repo / "deploy/developer-sandboxes/platform-health-authority.toml",
)
receipts = _load_receipts(evidence_path.parent)
if not receipts:
    raise RuntimeError("platform-health receipt set is empty")
samples = _load_samples(
    config,
    evidence_path.parent,
    session_id=receipts[0]["session_id"],
    candidates=receipts[0]["candidates"],
)
rebuilt = _verify_checkpoints(
    config,
    receipts,
    require_complete=True,
    samples=samples,
)
existing, _raw = _secure_json(
    evidence_path,
    label="platform-health final evidence",
)
if rebuilt is None or existing != rebuilt:
    raise RuntimeError("platform-health final evidence does not rebuild exactly")
session_id = existing["session_id"]
state = _session_state_unlocked(session_id)
sandboxes = _registry_sandboxes(state["registry_snapshot"])
phase_checkpoints = _phase_checkpoints(sandboxes)
if (
    state["status"] != "complete"
    or state["next_phase_index"] != len(phase_checkpoints)
    or state["completed_phases"]
    != [f"{sandbox}:{phase}" for phase, sandbox in phase_checkpoints]
    or state["candidates"] != existing["candidates"]
):
    raise RuntimeError("live acceptance session is not complete")
live_evidence_path = _session_dir(session_id) / "evidence.json"
live_evidence = _secure_json_load(live_evidence_path)
if verify_evidence(live_evidence, _load_schema()):
    raise RuntimeError("live acceptance final evidence is invalid")
if (
    live_evidence["session"]["id"] != session_id
    or {
        sandbox: {
            "sha": live_evidence["candidates"][sandbox]["sha"],
            "tree": live_evidence["candidates"][sandbox]["tree"],
        }
        for sandbox in sandboxes
    }
    != state["candidates"]
):
    raise RuntimeError("live acceptance candidate binding drifted")
checkpoint_root = _session_dir(session_id) / "checkpoints"
expected_names = {
    f"{index:02d}-{sandbox}-{phase}.json"
    for index, (phase, sandbox) in enumerate(phase_checkpoints)
}
if {path.name for path in checkpoint_root.iterdir()} != expected_names:
    raise RuntimeError("live acceptance checkpoint journal is incomplete")
for index, (phase, sandbox) in enumerate(phase_checkpoints):
    checkpoint = _secure_json_load(
        checkpoint_root / f"{index:02d}-{sandbox}-{phase}.json",
    )
    phase_evidence = live_evidence["state_machine"][index]
    canonical_phase = {
        key: value for key, value in phase_evidence.items() if key != "checkpoint_sha256"
    }
    actual_digest = hashlib.sha256(_canonical_bytes(canonical_phase)).hexdigest()
    if (
        checkpoint != _checkpoint_payload(session_id, canonical_phase, actual_digest)
        or phase_evidence["checkpoint_sha256"] != actual_digest
    ):
        raise RuntimeError("live acceptance checkpoint journal drifted")
live_digest = hashlib.sha256(_canonical_bytes(live_evidence)).hexdigest()
if state["evidence_sha256"] != live_digest:
    raise RuntimeError("live acceptance final digest drifted")
if "gate6_sha256" not in state:
    raise RuntimeError("live acceptance gate 6 is not sealed")
matrices = {}
for sandbox in sandboxes:
    candidate_sha = state["candidates"][sandbox]["sha"]
    for pool in POOLS:
        matrix_path = _gate6_matrix_path(sandbox, pool, candidate_sha)
        matrices[(sandbox, pool)] = _trusted_authority_json(
            matrix_path,
            SLURM_POLICY_STATE_ROOT,
            label="allocation-matrix",
        )
runtime_bindings = _gate6_runtime_domain_bindings(live_evidence)
for pair, matrix in matrices.items():
    runtime = matrix.get("runtime_attestation")
    if not isinstance(runtime, dict) or (
        runtime.get("receipt_sha256"),
        runtime.get("domain_payload_sha256"),
        runtime.get("domain_signature_sha256"),
        runtime.get("domain_generation"),
    ) not in runtime_bindings[pair]:
        raise RuntimeError("gate-6 allocation matrix runtime binding drifted")
rebuilt_bundle, rebuilt_pairs = gate6_verifier.build_gate6_bundle(
    live_evidence,
    existing,
    matrices,
    gate6_verifier._load_schema(NONEXCLUSIVE_SCHEMA),
)
gate_root = _session_dir(session_id) / "gate6"
stored_bundle = _secure_json_load(gate_root / "acceptance.json")
if (
    stored_bundle != rebuilt_bundle
    or state["gate6_sha256"] != rebuilt_bundle["payload_sha256"]
):
    raise RuntimeError("live acceptance gate-6 bundle drifted")
for pair, artifact in rebuilt_pairs.items():
    sandbox, pool = pair
    if _secure_json_load(
        gate_root / f"{sandbox}-{pool}.nonexclusive.json",
    ) != artifact:
        raise RuntimeError("live acceptance gate-6 pair artifact drifted")
print(json.dumps(
    {
        "gate6_sha256": rebuilt_bundle["payload_sha256"],
        "platform_health_sha256": existing["payload_sha256"],
        "registry_generation": state["registry_snapshot"]["source_registry"]["generation"],
        "registry_payload_sha256": (
            state["registry_snapshot"]["source_registry"]["payload_sha256"]
        ),
    },
    sort_keys=True,
    separators=(",", ":"),
))
"""

_ACCEPTANCE_SESSION_READBACK = """
import json
import sys
sys.path.insert(0, sys.argv[1])
from scripts.ops.developer_sandbox_live_acceptance import (
    PHASES,
    _phase_checkpoints,
    _registry_sandboxes,
    _session_state_unlocked,
)
state = _session_state_unlocked(sys.argv[2])
sandboxes = _registry_sandboxes(state["registry_snapshot"])
print(json.dumps(
    {
        "session_id": state["session_id"],
        "status": state["status"],
        "candidates": state["candidates"],
        "next_phase_index": state["next_phase_index"],
        "phase_checkpoints": len(_phase_checkpoints(sandboxes)),
        "phases": list(PHASES),
    },
    sort_keys=True,
    separators=(",", ":"),
))
"""

_ACCEPTANCE_OPEN = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
broker = SharedCapacityBroker(Path(sys.argv[2]))
contract = broker.open_acceptance_admission(
    token=sys.argv[3],
    contract=json.loads(sys.argv[4]),
)
print(json.dumps(contract, sort_keys=True, separators=(",", ":")))
"""

_ACCEPTANCE_COHORT = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
rows = SharedCapacityBroker(Path(sys.argv[2])).request_acceptance_cohort(
    token=sys.argv[3],
    session_id=sys.argv[4],
    phase=sys.argv[5],
)
print(json.dumps(rows, sort_keys=True, separators=(",", ":")))
"""

_ACCEPTANCE_COHORT_STATUS = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
rows = SharedCapacityBroker(Path(sys.argv[2])).acceptance_cohort_status(
    token=sys.argv[3],
    session_id=sys.argv[4],
    phase=sys.argv[5],
)
print(json.dumps(rows, sort_keys=True, separators=(",", ":")))
"""

_ACCEPTANCE_CANCEL = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
row = SharedCapacityBroker(Path(sys.argv[2])).cancel_acceptance_request(
    token=sys.argv[3],
    session_id=sys.argv[4],
    phase=sys.argv[5],
    sandbox=sys.argv[6],
    pool=sys.argv[7],
)
print(json.dumps(row, sort_keys=True, separators=(",", ":")))
"""

_ACCEPTANCE_CLOSE = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
SharedCapacityBroker(Path(sys.argv[2])).close_acceptance_admission(
    token=sys.argv[3],
    session_id=sys.argv[4],
)
"""

_ACCEPTANCE_CONTRACT_READBACK = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
contract = SharedCapacityBroker(Path(sys.argv[2])).acceptance_contract()
print(json.dumps(contract, sort_keys=True, separators=(",", ":")))
"""

_BROKER_RETIRE = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
from scripts.ops.shared_capacity_runtime_host import (
    _retirement_is_drained,
    _retirement_request_ids,
)
from scripts.ops.shared_capacity_supervisor import (
    _validate_report_budgets,
    load_config,
)
config = load_config(Path(sys.argv[2]))
candidate_shas = json.loads(sys.argv[3])
if config.state_db != Path(sys.argv[5]):
    raise RuntimeError("broker retirement authority binding mismatch")
broker = SharedCapacityBroker(config.state_db)
broker.close_admission(sys.argv[4])
report = broker.status()
_validate_report_budgets(report, config)
for request_id in _retirement_request_ids(report, candidate_shas):
    broker.cancel(request_id, reason="runtime_host_rollback")
report = broker.status()
_validate_report_budgets(report, config)
print("drained" if _retirement_is_drained(report, candidate_shas) else "pending")
"""

_ACCEPTANCE_RETIRE = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
from scripts.ops.shared_capacity_runtime_host import _retirement_is_drained
from scripts.ops.shared_capacity_supervisor import (
    _validate_report_budgets,
    load_config,
)
config = load_config(Path(sys.argv[2]))
candidate_shas = json.loads(sys.argv[3])
if config.state_db != Path(sys.argv[6]):
    raise RuntimeError("acceptance retirement authority binding mismatch")
broker = SharedCapacityBroker(config.state_db)
broker.close_admission(sys.argv[4])
contract = broker.acceptance_contract()
if (
    contract is None
    or contract["admission_token"] != sys.argv[4]
    or contract["session_id"] != sys.argv[5]
    or contract["candidate_shas"] != candidate_shas
):
    raise RuntimeError("acceptance retirement contract binding mismatch")
broker.retire_acceptance_requests(
    token=sys.argv[4],
    session_id=sys.argv[5],
)
report = broker.status()
_validate_report_budgets(report, config)
print("drained" if _retirement_is_drained(report, candidate_shas) else "pending")
"""

_ADAPTER_PREFLIGHT = """
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts.ops.shared_capacity_adapter import (
    _bootstrap_policy_body,
    _get_policy,
    _http_json,
    _load_admin_token,
    _load_sandbox_binding,
    _policy_path,
    _validate_bootstrap_policy,
    _validate_policy,
    _validate_runtime_attestation,
    load_config,
)
from scripts.ops.shared_capacity_runtime_host import (
    _validate_zero_cp_policy,
    _validate_zero_worker_status,
)
config = load_config(Path(sys.argv[2]))
binding = _load_sandbox_binding(config)
if binding.sha != sys.argv[3] or binding.tree != sys.argv[4]:
    raise RuntimeError("sandbox candidate binding mismatch")
_token = _load_admin_token(config.admin_secret_file)
_validate_runtime_attestation(
    config,
    candidate=binding,
    now=datetime.now(UTC),
    minimum_remaining=timedelta(seconds=max(30.0, config.timeout_seconds * 3)),
)
policy, missing = _get_policy(
    config,
    token=_token,
    path=_policy_path(config),
    http_json=_http_json,
)
if missing or policy is None:
    raise RuntimeError("control-plane autoscaler policy is missing")
expected = _bootstrap_policy_body(config, candidate_sha=binding.sha)
_validate_policy(policy, config=config, candidate_sha=binding.sha)
_validate_bootstrap_policy(
    policy,
    config=config,
    candidate_sha=binding.sha,
    expected_body=expected,
)
_validate_zero_cp_policy(policy, candidate_sha=binding.sha)
worker_status = _http_json(
    method="GET",
    base_url=config.control_plane_url,
    token=_token,
    path="/admin/slurm-worker-jobs/status",
    timeout=config.timeout_seconds,
)
_validate_zero_worker_status(
    worker_status,
    environment=config.environment,
    pool_name=config.pool_name,
)
"""

_ADAPTER_BINDING_READBACK = """
import json
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts.ops.shared_capacity_adapter import _load_sandbox_binding, load_config
config = load_config(Path(sys.argv[2]))
binding = _load_sandbox_binding(config)
print(json.dumps(
    {
        "pool": config.pool_name,
        "sandbox": config.sandbox,
        "sha": binding.sha,
        "tree": binding.tree,
    },
    sort_keys=True,
    separators=(",", ":"),
))
"""

_GENERATION_READBACK = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
from scripts.ops.shared_capacity_supervisor import (
    _current_generation,
    _load_supervisor_state,
    _publication_handoffs,
    _read_json,
    _validate_generation_contents,
    load_config,
)
config = load_config(Path(sys.argv[2]))
state = _load_supervisor_state(config.supervisor_state_path)
generation = _current_generation(config.handoff_dir)
if state is None or generation != state.get("generation"):
    raise RuntimeError("supervisor generation state mismatch")
published = state.get("published")
if not isinstance(published, dict) or set(published) != set(config.instances):
    raise RuntimeError("supervisor generation is not closed-world")
manifest = _read_json(
    config.handoff_dir / generation / "manifest.json",
    label="activation generation manifest",
)
if not isinstance(manifest, dict) or manifest.get("instances") != published:
    raise RuntimeError("supervisor generation manifest mismatch")
report = SharedCapacityBroker(config.state_db).status()
selected = _publication_handoffs(report, config)
_validate_generation_contents(
    config.handoff_dir / generation,
    manifest=manifest,
    selected=selected,
    config=config,
)
"""

_ACTIVATED_ADAPTER_READBACK = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts.ops.shared_capacity_adapter import (
    _bootstrap_policy_body,
    _get_policy,
    _handoff_binding,
    _http_json,
    _load_adapter_state,
    _load_admin_token,
    _load_sandbox_binding,
    _policy_path,
    _validate_policy_update_readback,
    load_config,
    load_handoff,
)
from scripts.ops.shared_capacity_runtime_host import (
    _validate_zero_adapter_state,
    _validate_zero_cp_policy,
    _validate_zero_worker_status,
)
config = load_config(Path(sys.argv[2]))
candidate = _load_sandbox_binding(config)
if candidate.sha != sys.argv[3] or candidate.tree != sys.argv[4]:
    raise RuntimeError("activated sandbox candidate binding mismatch")
if sys.argv[5] not in {"allow-positive", "require-zero"}:
    raise RuntimeError("activated adapter readback mode is invalid")
require_zero = sys.argv[5] == "require-zero"
handoff = load_handoff(config)
state = _load_adapter_state(config.adapter_state_path)
if state is None:
    raise RuntimeError("activated adapter state is missing")
token = _load_admin_token(config.admin_secret_file)
policy, missing = _get_policy(
    config,
    token=token,
    path=_policy_path(config),
    http_json=_http_json,
)
if missing or policy is None:
    raise RuntimeError("activated control-plane policy is missing")
binding = _handoff_binding(handoff)
_validate_policy_update_readback(
    policy,
    config=config,
    candidate_sha=candidate.sha,
    expected_policy=_bootstrap_policy_body(config, candidate_sha=candidate.sha),
    binding=binding,
    enabled=handoff.enabled,
    max_slots=handoff.max_slots,
)
if (
    state.get("request_id") != handoff.request_id
    or state.get("lease_epoch") != handoff.lease_epoch
    or state.get("candidate_sha") != handoff.candidate_sha
    or state.get("applied_enabled") != handoff.enabled
    or state.get("applied_max_slots") != handoff.max_slots
):
    raise RuntimeError("activated adapter state is not current")
if require_zero:
    _validate_zero_adapter_state(handoff, state)
if require_zero or not handoff.enabled:
    _validate_zero_cp_policy(policy, candidate_sha=candidate.sha)
    worker_status = _http_json(
        method="GET",
        base_url=config.control_plane_url,
        token=token,
        path="/admin/slurm-worker-jobs/status",
        timeout=config.timeout_seconds,
    )
    _validate_zero_worker_status(
        worker_status,
        environment=config.environment,
        pool_name=config.pool_name,
    )
    if not require_zero and any(
        state.get(field) != 0
        for field in ("pending_slots", "active_slots", "draining_slots")
    ):
        raise RuntimeError("disabled activated adapter still has live capacity")
"""

_EMBEDDED_PROGRAM_ARGUMENT_COUNTS = {
    _BROKER_PREFLIGHT: 3,
    _BROKER_OPEN: 2,
    _BROKER_CLOSE: 2,
    _BROKER_ADMISSION_READBACK: 1,
    _PLATFORM_HEALTH_AUTHORITY_REBUILD: 1,
    _ACCEPTANCE_SESSION_READBACK: 1,
    _ACCEPTANCE_OPEN: 3,
    _ACCEPTANCE_COHORT: 4,
    _ACCEPTANCE_COHORT_STATUS: 4,
    _ACCEPTANCE_CANCEL: 6,
    _ACCEPTANCE_CLOSE: 3,
    _ACCEPTANCE_CONTRACT_READBACK: 1,
    _BROKER_RETIRE: 4,
    _ACCEPTANCE_RETIRE: 5,
    _ADAPTER_PREFLIGHT: 3,
    _ADAPTER_BINDING_READBACK: 1,
    _GENERATION_READBACK: 1,
    _ACTIVATED_ADAPTER_READBACK: 4,
}


def _verify_platform_health_authority_evidence(
    candidate: Candidate,
    evidence_path: Path,
) -> tuple[str, str]:
    completed = _run_candidate_python(
        candidate,
        _PLATFORM_HEALTH_AUTHORITY_REBUILD,
        str(evidence_path),
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeHostError("platform-health authority rebuild is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "gate6_sha256",
            "platform_health_sha256",
            "registry_generation",
            "registry_payload_sha256",
        }
        or completed.stdout.encode() != _canonical_json(payload)
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("gate6_sha256"))) is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("platform_health_sha256")),
        )
        is None
        or payload.get("registry_generation") != _cohort().generation
        or payload.get("registry_payload_sha256") != _cohort().payload_sha256
    ):
        raise RuntimeHostError("platform-health authority rebuild is invalid")
    return str(payload["platform_health_sha256"]), str(payload["gate6_sha256"])


def _acceptance_session_readback(
    candidate: Candidate,
    session_id: str,
) -> dict[str, Any]:
    if re.fullmatch(r"[0-9a-f]{32}", session_id) is None:
        raise RuntimeHostError("acceptance session identity is invalid")
    completed = _run_candidate_python(
        candidate,
        _ACCEPTANCE_SESSION_READBACK,
        session_id,
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeHostError("acceptance session readback is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "session_id",
            "status",
            "candidates",
            "next_phase_index",
            "phase_checkpoints",
            "phases",
        }
        or completed.stdout.encode() != _canonical_json(payload)
        or payload.get("session_id") != session_id
        or payload.get("status") not in {"running", "complete"}
        or payload.get("phases") != list(LIVE_PHASES)
        or payload.get("phase_checkpoints") != len(LIVE_PHASES) * len(_sandboxes())
        or not isinstance(payload.get("next_phase_index"), int)
        or isinstance(payload.get("next_phase_index"), bool)
        or not isinstance(payload.get("candidates"), dict)
    ):
        raise RuntimeHostError("acceptance session readback is invalid")
    return payload


def _acceptance_candidate_shas(
    candidate: Candidate,
    session: Mapping[str, Any],
) -> dict[str, str]:
    adapter_candidates = _adapter_candidate_bindings(candidate)
    if session.get("candidates") != adapter_candidates:
        raise RuntimeHostError("acceptance session candidate set drifted")
    return {sandbox: adapter_candidates[sandbox]["sha"] for sandbox in _sandboxes()}


def _acceptance_contract(
    candidate: Candidate,
    *,
    admission_token: str,
    session: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    candidate_shas = _acceptance_candidate_shas(candidate, session)
    policies = {pool: _candidate_platform_policy(candidate, pool)[0] for pool in POOLS}
    current = _platform_health_now(now)
    return {
        "schema_version": 1,
        "admission_token": admission_token,
        "session_id": session["session_id"],
        "candidate_shas": candidate_shas,
        "phases": list(ACCEPTANCE_PHASES),
        "target_slots": {pool: policies[pool]["requested_concurrency"] for pool in POOLS},
        "ttl_seconds": {
            phase: (
                ACCEPTANCE_TTL_CLEANUP_SECONDS
                if phase == "ttl_cleanup"
                else (
                    ACCEPTANCE_MIXED_NON_LOOM_TTL_SECONDS
                    if phase == "mixed_non_loom"
                    else ACCEPTANCE_DEFAULT_PHASE_TTL_SECONDS
                )
            )
            for phase in ACCEPTANCE_PHASES
        },
        "pool_slot_budgets": {pool: policies[pool]["slot_budget"] for pool in POOLS},
        "pool_pending_slot_budgets": {
            pool: policies[pool]["pending_slot_budget"] for pool in POOLS
        },
        "expires_at": (current + timedelta(seconds=ACCEPTANCE_CONTRACT_TTL_SECONDS)).isoformat(),
    }


def _acceptance_contract_readback(candidate: Candidate) -> dict[str, Any] | None:
    completed = _run_candidate_python(
        candidate,
        _ACCEPTANCE_CONTRACT_READBACK,
        str(_candidate_broker_state_db(candidate)),
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeHostError("acceptance contract readback is invalid") from exc
    if completed.stdout.encode() != _canonical_json_value(payload):
        raise RuntimeHostError("acceptance contract readback is not canonical")
    if payload is not None and not isinstance(payload, dict):
        raise RuntimeHostError("acceptance contract readback is invalid")
    return payload


def _require_acceptance_phase_prefix(
    session: Mapping[str, Any],
    phase: str,
) -> int:
    if phase not in LIVE_PHASES:
        raise RuntimeHostError("acceptance phase is invalid")
    start = LIVE_PHASES.index(phase) * len(_sandboxes())
    next_index = session.get("next_phase_index")
    if (
        session.get("status") != "running"
        or not isinstance(next_index, int)
        or isinstance(next_index, bool)
        or next_index not in range(start, start + len(_sandboxes()))
    ):
        raise RuntimeHostError("acceptance phase prefix is not current")
    return next_index - start


def _run_acceptance_program(
    candidate: Candidate,
    code: str,
    *args: str,
) -> Any:
    completed = _run_candidate_python(
        candidate,
        code,
        str(_candidate_broker_state_db(candidate)),
        *args,
    )
    if not completed.stdout:
        return None
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeHostError("acceptance broker result is invalid") from exc
    if completed.stdout.encode() != _canonical_json_value(payload):
        raise RuntimeHostError("acceptance broker result is not canonical")
    return payload


def _adapter_candidate_bindings(candidate: Candidate) -> dict[str, dict[str, str]]:
    by_sandbox: dict[str, dict[str, str]] = {}
    observed_instances: set[str] = set()
    registry_environments = {
        environment.runtime_id: environment for environment in _cohort().environments
    }
    for instance in _instances():
        completed = _run_candidate_python(
            candidate,
            _ADAPTER_BINDING_READBACK,
            str(ADAPTER_CONFIG_ROOT / f"{instance}.toml"),
        )
        try:
            payload = json.loads(completed.stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeHostError("adapter candidate binding readback is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"pool", "sandbox", "sha", "tree"}
            or completed.stdout.encode() != _canonical_json(payload)
            or payload.get("sandbox") not in _sandboxes()
            or payload.get("pool") not in POOLS
            or instance != f"{payload.get('sandbox')}-{payload.get('pool')}"
            or SHA_RE.fullmatch(str(payload.get("sha"))) is None
            or SHA_RE.fullmatch(str(payload.get("tree"))) is None
            or instance in observed_instances
        ):
            raise RuntimeHostError("adapter candidate binding readback is invalid")
        observed_instances.add(instance)
        binding = {"sha": str(payload["sha"]), "tree": str(payload["tree"])}
        sandbox = str(payload["sandbox"])
        registry_environment = registry_environments.get(sandbox)
        if registry_environment is None or binding != {
            "sha": registry_environment.candidate_sha,
            "tree": registry_environment.candidate_tree,
        }:
            raise RuntimeHostError(
                "adapter candidate binding drifted from the registry snapshot",
            )
        existing = by_sandbox.get(sandbox)
        if existing is not None and existing != binding:
            raise RuntimeHostError("sandbox adapter candidate bindings drifted across pools")
        by_sandbox[sandbox] = binding
    if observed_instances != set(_instances()) or set(by_sandbox) != set(_sandboxes()):
        raise RuntimeHostError("adapter candidate binding set is not closed")
    if len({binding["sha"] for binding in by_sandbox.values()}) != len(_sandboxes()):
        raise RuntimeHostError("sandbox adapter candidate bindings are not unique")
    return by_sandbox


def _activation_preflight(
    candidate: Candidate,
    *,
    transaction_id: str,
) -> None:
    adapter_candidates = _adapter_candidate_bindings(candidate)
    candidate_shas = {sandbox: adapter_candidates[sandbox]["sha"] for sandbox in _sandboxes()}
    _run_candidate_python(
        candidate,
        _BROKER_PREFLIGHT,
        str(SUPERVISOR_CONFIG_PATH),
        json.dumps(candidate_shas, sort_keys=True, separators=(",", ":")),
        transaction_id,
    )
    for instance in _instances():
        sandbox, _pool = instance.rsplit("-", 1)
        binding = adapter_candidates[sandbox]
        _run_candidate_python(
            candidate,
            _ADAPTER_PREFLIGHT,
            str(ADAPTER_CONFIG_ROOT / f"{instance}.toml"),
            binding["sha"],
            binding["tree"],
        )


def _positive_capacity_admission_gate(
    candidate: Candidate,
) -> tuple[dict[str, dict[str, str]], str, str]:
    adapter_candidates = _adapter_candidate_bindings(candidate)
    payload_sha256, gate6_sha256 = _validate_platform_health_activation_gate(
        candidate,
        adapter_candidates,
    )
    return adapter_candidates, payload_sha256, gate6_sha256


def _open_activation_admission(
    candidate: Candidate,
    transaction_id: str,
) -> None:
    _run_candidate_python(
        candidate,
        _BROKER_OPEN,
        str(_candidate_broker_state_db(candidate)),
        transaction_id,
    )


def _close_activation_admission(
    candidate: Candidate,
    transaction_id: str,
) -> None:
    _run_candidate_python(
        candidate,
        _BROKER_CLOSE,
        str(_candidate_broker_state_db(candidate)),
        transaction_id,
    )


def _admission_fence(candidate: Candidate) -> str | None:
    completed = _run_candidate_python(
        candidate,
        _BROKER_ADMISSION_READBACK,
        str(_candidate_broker_state_db(candidate)),
    )
    value = completed.stdout.strip()
    if value == "open":
        return None
    if re.fullmatch(r"[0-9a-f]{32}", value) is None:
        raise RuntimeHostError("broker admission fence readback is invalid")
    return value


def _request_capacity_retirement(
    candidate: Candidate,
    transaction_id: str,
) -> str:
    adapter_candidates = _adapter_candidate_bindings(candidate)
    candidate_shas = {sandbox: adapter_candidates[sandbox]["sha"] for sandbox in _sandboxes()}
    completed = _run_candidate_python(
        candidate,
        _BROKER_RETIRE,
        str(SUPERVISOR_CONFIG_PATH),
        json.dumps(candidate_shas, sort_keys=True, separators=(",", ":")),
        transaction_id,
        str(_candidate_broker_state_db(candidate)),
    )
    status = completed.stdout.strip()
    if status not in {"pending", "drained"}:
        raise RuntimeHostError("broker retirement status is invalid")
    return status


def _request_acceptance_retirement(
    candidate: Candidate,
    transaction_id: str,
    session_id: str,
) -> str:
    adapter_candidates = _adapter_candidate_bindings(candidate)
    candidate_shas = {sandbox: adapter_candidates[sandbox]["sha"] for sandbox in _sandboxes()}
    completed = _run_candidate_python(
        candidate,
        _ACCEPTANCE_RETIRE,
        str(SUPERVISOR_CONFIG_PATH),
        json.dumps(candidate_shas, sort_keys=True, separators=(",", ":")),
        transaction_id,
        session_id,
        str(_candidate_broker_state_db(candidate)),
    )
    status = completed.stdout.strip()
    if status not in {"pending", "drained"}:
        raise RuntimeHostError("acceptance retirement status is invalid")
    return status


def _run_retirement_cycle() -> None:
    for unit in (SUPERVISOR_SERVICE, *_adapter_services(), SUPERVISOR_SERVICE):
        _run(("systemctl", "start", unit))
        if _service_result(unit) != ("success", "0"):
            raise RuntimeHostError("shared-capacity retirement cycle failed")


def _drain_activated_capacity(
    candidate: Candidate,
    transaction_id: str,
) -> None:
    for cycle in range(RETIREMENT_MAX_CYCLES):
        _request_capacity_retirement(candidate, transaction_id)
        _run_retirement_cycle()
        if _request_capacity_retirement(candidate, transaction_id) == "drained":
            _activated_adapter_readback(candidate, require_zero=True)
            return
        if cycle + 1 < RETIREMENT_MAX_CYCLES:
            time.sleep(RETIREMENT_POLL_SECONDS)
    raise RuntimeHostError("shared-capacity retirement did not drain before timeout")


def _drain_acceptance_capacity(
    candidate: Candidate,
    transaction_id: str,
    session_id: str,
) -> None:
    for cycle in range(RETIREMENT_MAX_CYCLES):
        _request_acceptance_retirement(candidate, transaction_id, session_id)
        _run_retirement_cycle()
        if (
            _request_acceptance_retirement(
                candidate,
                transaction_id,
                session_id,
            )
            == "drained"
        ):
            _activated_adapter_readback(candidate, require_zero=True)
            return
        if cycle + 1 < RETIREMENT_MAX_CYCLES:
            time.sleep(RETIREMENT_POLL_SECONDS)
    raise RuntimeHostError("acceptance retirement did not drain before timeout")


def _verify_acceptance_capacity_drained(
    candidate: Candidate,
    transaction_id: str,
    session_id: str,
) -> None:
    if (
        _request_acceptance_retirement(
            candidate,
            transaction_id,
            session_id,
        )
        != "drained"
    ):
        raise RuntimeHostError("acceptance retirement regressed before phase transition")
    _activated_adapter_readback(candidate, require_zero=True)


def _verify_activated_capacity_drained(
    candidate: Candidate,
    transaction_id: str,
) -> None:
    if _request_capacity_retirement(candidate, transaction_id) != "drained":
        raise RuntimeHostError("shared-capacity retirement regressed before local restore")
    _activated_adapter_readback(candidate, require_zero=True)


def _rollback_candidate(payload: Mapping[str, Any]) -> Candidate:
    sha = payload.get("candidate_sha")
    tree = payload.get("candidate_tree")
    if (
        not isinstance(sha, str)
        or SHA_RE.fullmatch(sha) is None
        or not isinstance(tree, str)
        or SHA_RE.fullmatch(tree) is None
    ):
        raise RuntimeHostError("activated rollback candidate binding is invalid")
    return Candidate(
        sha=sha,
        tree=tree,
        source=CANDIDATE_PARENT / sha / "repo",
    )


def _complete_activated_rollback(
    path: Path,
    payload: dict[str, Any],
) -> None:
    manifest_cohort = _cohort_from_runtime_manifest(payload.get("runtime_manifest"))
    with _bound_cohort(manifest_cohort):
        _complete_activated_rollback_bound(path, payload)


def _complete_activated_rollback_bound(
    path: Path,
    payload: dict[str, Any],
) -> None:
    transaction_id, operation, _sha, _tree, _files, _units = _validate_transaction(
        path,
        payload,
    )
    if operation != "install":
        raise RuntimeHostError("activated rollback transaction is invalid")
    _validate_rollback_recovery(payload)
    candidate = _rollback_candidate(payload)
    phase = payload.get("phase")
    if phase == "rollback-recovery-ready":
        _update_journal(path, payload, "rollback-closing-admission")
        phase = "rollback-closing-admission"
    if phase == "rollback-closing-admission":
        _request_capacity_retirement(candidate, transaction_id)
        _update_journal(path, payload, "rollback-draining")
        phase = "rollback-draining"
    if phase == "rollback-draining":
        _drain_activated_capacity(candidate, transaction_id)
        _update_journal(path, payload, "rollback-drained")
        phase = "rollback-drained"
    if phase == "rollback-drained":
        _verify_activated_capacity_drained(candidate, transaction_id)
        _update_journal(path, payload, "rollback-restoring")
        phase = "rollback-restoring"
    if phase == "rollback-restoring":
        _restore_local_transaction(
            path,
            payload,
            remove_candidate=False,
        )
        _update_journal(path, payload, "rollback-restored-fenced")
        phase = "rollback-restored-fenced"
    if phase == "rollback-restored-fenced":
        _open_activation_admission(candidate, transaction_id)
        _update_journal(path, payload, "rollback-admission-open")
        phase = "rollback-admission-open"
    if phase == "rollback-admission-open":
        if payload.get("candidate_previously_existed") is False:
            _remove_path(candidate.root)
        _update_journal(path, payload, "rolled-back")
        ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
        _fsync_directory(INSTALLER_ROOT)
        return
    raise RuntimeHostError("activated rollback journal phase is invalid")


def _resume_activated_rollback(
    path: Path,
    payload: dict[str, Any],
) -> None:
    if payload.get("phase") not in {
        "rollback-recovery-ready",
        "rollback-closing-admission",
        "rollback-draining",
        "rollback-drained",
        "rollback-restoring",
        "rollback-restored-fenced",
        "rollback-admission-open",
    }:
        raise RuntimeHostError("activated rollback journal phase is invalid")
    _complete_activated_rollback(path, payload)


def _activate_units(candidate: Candidate) -> None:
    _run(("systemctl", "start", SUPERVISOR_SERVICE))
    if _service_result(SUPERVISOR_SERVICE) != ("success", "0"):
        raise RuntimeHostError("supervisor activation cycle failed")
    _run_candidate_python(
        candidate,
        _GENERATION_READBACK,
        str(SUPERVISOR_CONFIG_PATH),
    )
    _run(("systemctl", "enable", "--now", SUPERVISOR_TIMER))
    for service, timer in zip(_adapter_services(), _adapter_timers(), strict=True):
        _run(("systemctl", "start", service))
        if _service_result(service) != ("success", "0"):
            raise RuntimeHostError("adapter activation cycle failed")
        _run(("systemctl", "enable", "--now", timer))


def _activated_adapter_readback(
    candidate: Candidate,
    *,
    require_zero: bool,
) -> None:
    adapter_candidates = _adapter_candidate_bindings(candidate)
    _run_candidate_python(
        candidate,
        _GENERATION_READBACK,
        str(SUPERVISOR_CONFIG_PATH),
    )
    for instance in _instances():
        sandbox, _pool = instance.rsplit("-", 1)
        binding = adapter_candidates[sandbox]
        _run_candidate_python(
            candidate,
            _ACTIVATED_ADAPTER_READBACK,
            str(ADAPTER_CONFIG_ROOT / f"{instance}.toml"),
            binding["sha"],
            binding["tree"],
            "require-zero" if require_zero else "allow-positive",
        )


def _acceptance_operation_candidate(
    payload: Mapping[str, Any],
) -> tuple[Candidate, dict[str, Any], str]:
    state = _load_json(STATE_PATH, "runtime-host state")
    sha = state.get("candidate_sha")
    tree = state.get("candidate_tree")
    token = state.get("transaction_id")
    if (
        not isinstance(sha, str)
        or SHA_RE.fullmatch(sha) is None
        or not isinstance(tree, str)
        or SHA_RE.fullmatch(tree) is None
        or not isinstance(token, str)
        or re.fullmatch(r"[0-9a-f]{32}", token) is None
        or payload.get("candidate_sha") != sha
        or payload.get("candidate_tree") != tree
        or payload.get("admission_token") != token
        or state.get("registry_generation") != payload.get("registry_generation")
        or state.get("registry_payload_sha256") != payload.get("registry_payload_sha256")
        or state.get("runtime_manifest") != payload.get("runtime_manifest")
    ):
        raise RuntimeHostError("acceptance operation candidate binding is invalid")
    return (
        Candidate(sha=sha, tree=tree, source=CANDIDATE_PARENT / sha / "repo"),
        state,
        token,
    )


def _write_acceptance_operation(payload: Mapping[str, Any]) -> None:
    if ACCEPTANCE_OPERATION_PATH.exists() or ACCEPTANCE_OPERATION_PATH.is_symlink():
        raise RuntimeHostError("another acceptance operation is active")
    _atomic_write(
        ACCEPTANCE_OPERATION_PATH,
        _canonical_json(payload),
        mode=0o600,
    )


def _update_acceptance_operation(
    payload: dict[str, Any],
    step: str,
) -> None:
    updated = dict(payload)
    updated["step"] = step
    _validate_acceptance_operation(updated)
    _atomic_write(
        ACCEPTANCE_OPERATION_PATH,
        _canonical_json(updated),
        mode=0o600,
    )
    payload.clear()
    payload.update(updated)


def _clear_acceptance_operation() -> None:
    ACCEPTANCE_OPERATION_PATH.unlink(missing_ok=True)
    _fsync_directory(INSTALLER_ROOT)


def _load_acceptance_operation() -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(ACCEPTANCE_OPERATION_PATH, flags)
    except OSError as exc:
        raise RuntimeHostError("acceptance operation is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
            or metadata.st_gid != os.getegid()
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise RuntimeHostError("acceptance operation metadata is unsafe")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            content = handle.read(1024 * 1024 + 1)
        if len(content) > 1024 * 1024:
            raise RuntimeHostError("acceptance operation is too large")
        try:
            payload = json.loads(content)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeHostError("acceptance operation is invalid") from exc
        if not isinstance(payload, dict) or content != _canonical_json(payload):
            raise RuntimeHostError("acceptance operation is not canonical")
        return payload
    finally:
        os.close(descriptor)


def _validate_acceptance_operation(payload: Mapping[str, Any]) -> None:
    operation = payload.get("operation")
    common = {
        "schema_version",
        "operation",
        "candidate_sha",
        "candidate_tree",
        "admission_token",
        "session_id",
        "registry_generation",
        "registry_payload_sha256",
        "runtime_manifest",
    }
    expected = {
        "open": common | {"contract", "step"},
        "cohort": common | {"phase", "mode", "step", "checkpoint_offset"},
        "cancel": common | {"phase", "sandbox", "pool"},
        "close": common,
    }
    if (
        operation not in expected
        or set(payload) != expected[cast(str, operation)]
        or payload.get("schema_version") != 1
        or SHA_RE.fullmatch(str(payload.get("candidate_sha"))) is None
        or SHA_RE.fullmatch(str(payload.get("candidate_tree"))) is None
        or re.fullmatch(r"[0-9a-f]{32}", str(payload.get("admission_token"))) is None
        or re.fullmatch(r"[0-9a-f]{32}", str(payload.get("session_id"))) is None
    ):
        raise RuntimeHostError("acceptance operation is invalid")
    manifest_cohort = _cohort_from_runtime_manifest(payload.get("runtime_manifest"))
    if (
        payload.get("registry_generation") != manifest_cohort.generation
        or payload.get("registry_payload_sha256") != manifest_cohort.payload_sha256
    ):
        raise RuntimeHostError("acceptance registry generation binding is invalid")
    if operation == "open" and not isinstance(payload.get("contract"), dict):
        raise RuntimeHostError("acceptance open contract is invalid")
    if operation == "open" and payload.get("step") not in {
        "prepared",
        "contract-open",
        "state-active",
        "rolling-back",
    }:
        raise RuntimeHostError("acceptance open operation is invalid")
    if operation == "cohort" and (
        payload.get("phase") not in ACCEPTANCE_PHASES
        or payload.get("mode") not in {"replay", "rotate"}
        or payload.get("step")
        not in {
            "prepared",
            "rotation-drained",
            "cohort-created",
            "cohort-ready",
        }
        or payload.get("checkpoint_offset") not in {0, 1, 2}
        or (payload.get("mode") == "rotate" and payload.get("checkpoint_offset") != 0)
        or (payload.get("mode") == "replay" and payload.get("step") != "cohort-ready")
    ):
        raise RuntimeHostError("acceptance cohort operation is invalid")
    if operation == "cancel" and (
        payload.get("phase") != "cancel_cleanup"
        or payload.get("sandbox") not in _sandboxes()
        or payload.get("pool") not in POOLS
    ):
        raise RuntimeHostError("acceptance cancel operation is invalid")


def _resume_acceptance_operation() -> dict[str, Any] | None:
    if not ACCEPTANCE_OPERATION_PATH.exists():
        return None
    payload = _load_acceptance_operation()
    _validate_acceptance_operation(payload)
    manifest_cohort = _cohort_from_runtime_manifest(payload.get("runtime_manifest"))
    with _bound_cohort(manifest_cohort):
        return _resume_acceptance_operation_bound(payload)


def _resume_acceptance_operation_bound(
    payload: dict[str, Any],
) -> dict[str, Any]:
    operation = payload.get("operation")
    session_id = payload.get("session_id")
    candidate, state, token = _acceptance_operation_candidate(payload)
    if operation == "open":
        contract = payload.get("contract")
        if not isinstance(contract, dict):
            raise RuntimeHostError("acceptance open contract is invalid")
        step = payload.get("step")
        if step == "prepared":
            observed = _run_acceptance_program(
                candidate,
                _ACCEPTANCE_OPEN,
                token,
                json.dumps(contract, sort_keys=True, separators=(",", ":")),
            )
            if observed != contract:
                raise RuntimeHostError("acceptance open contract readback drifted")
            _update_acceptance_operation(payload, "contract-open")
            step = "contract-open"
        if step == "contract-open":
            updated = dict(state)
            updated["activation_status"] = "acceptance-active"
            updated["acceptance_session_id"] = session_id
            updated["acceptance_contract_sha256"] = _sha256(_canonical_json(contract))
            _atomic_write(STATE_PATH, _canonical_json(updated), mode=0o600)
            state = updated
            _update_acceptance_operation(payload, "state-active")
            step = "state-active"
        if step == "state-active":
            try:
                report = check(candidate, activation_mode="acceptance-active")
            except Exception:
                _update_acceptance_operation(payload, "rolling-back")
                step = "rolling-back"
        if step == "rolling-back":
            _close_activation_admission(candidate, token)
            _drain_acceptance_capacity(candidate, token, str(session_id))
            _verify_acceptance_capacity_drained(
                candidate,
                token,
                str(session_id),
            )
            _run_acceptance_program(
                candidate,
                _ACCEPTANCE_CLOSE,
                token,
                str(session_id),
            )
            updated = dict(state)
            updated["activation_status"] = "bootstrap-active"
            updated.pop("acceptance_session_id", None)
            updated.pop("acceptance_contract_sha256", None)
            _atomic_write(STATE_PATH, _canonical_json(updated), mode=0o600)
            report = check(candidate, activation_mode="bootstrap-active")
            report = {
                **report,
                "acceptance_open_rolled_back": True,
            }
    elif operation == "cohort":
        phase = payload.get("phase")
        if phase not in ACCEPTANCE_PHASES:
            raise RuntimeHostError("acceptance cohort operation is invalid")
        step = payload.get("step")
        if payload.get("mode") == "rotate" and step == "prepared":
            _drain_acceptance_capacity(candidate, token, str(session_id))
            _verify_acceptance_capacity_drained(
                candidate,
                token,
                str(session_id),
            )
            _update_acceptance_operation(payload, "rotation-drained")
            step = "rotation-drained"
        if payload.get("mode") == "rotate" and step == "rotation-drained":
            try:
                _run_acceptance_program(
                    candidate,
                    _ACCEPTANCE_COHORT,
                    token,
                    str(session_id),
                    str(phase),
                )
            except Exception:
                _clear_acceptance_operation()
                raise
            _update_acceptance_operation(payload, "cohort-created")
        rows = _run_acceptance_program(
            candidate,
            _ACCEPTANCE_COHORT_STATUS,
            token,
            str(session_id),
            str(phase),
        )
        if not isinstance(rows, list) or len(rows) != len(_instances()):
            raise RuntimeHostError("acceptance cohort is not closed-world")
        report = {
            "schema_version": 1,
            "status": "pass",
            "operation": "acceptance-cohort",
            **_registry_binding(),
            "session_id": session_id,
            "phase": phase,
            "requests": rows,
        }
    elif operation == "cancel":
        phase = payload.get("phase")
        sandbox = payload.get("sandbox")
        pool = payload.get("pool")
        if phase != "cancel_cleanup" or sandbox not in _sandboxes() or pool not in POOLS:
            raise RuntimeHostError("acceptance cancel operation is invalid")
        row = _run_acceptance_program(
            candidate,
            _ACCEPTANCE_CANCEL,
            token,
            str(session_id),
            str(phase),
            str(sandbox),
            str(pool),
        )
        if not isinstance(row, dict):
            raise RuntimeHostError("acceptance cancel result is invalid")
        report = {
            "schema_version": 1,
            "status": "pass",
            "operation": "acceptance-cancel",
            **_registry_binding(),
            "session_id": session_id,
            "phase": phase,
            "sandbox": sandbox,
            "pool": pool,
            "request": row,
        }
    else:
        _close_activation_admission(candidate, token)
        _drain_acceptance_capacity(candidate, token, str(session_id))
        _verify_acceptance_capacity_drained(
            candidate,
            token,
            str(session_id),
        )
        _run_acceptance_program(
            candidate,
            _ACCEPTANCE_CLOSE,
            token,
            str(session_id),
        )
        updated = dict(state)
        updated["activation_status"] = "bootstrap-active"
        updated.pop("acceptance_session_id", None)
        updated.pop("acceptance_contract_sha256", None)
        _atomic_write(STATE_PATH, _canonical_json(updated), mode=0o600)
        report = check(candidate, activation_mode="bootstrap-active")
    _clear_acceptance_operation()
    return report


def _recover_acceptance_operation() -> None:
    _resume_acceptance_operation()


def check(
    candidate: Candidate,
    *,
    activation_mode: str = "installed",
) -> dict[str, Any]:
    if activation_mode not in {
        "installed",
        "bootstrap-active",
        "acceptance-active",
        "activated",
    }:
        raise RuntimeHostError("runtime-host check mode is invalid")
    _verify_installed_candidate(candidate)
    desired = _desired_files(candidate)
    for path, (content, mode) in desired.items():
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise RuntimeHostError(f"installed file is unavailable: {path}") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or path.read_bytes() != content
        ):
            raise RuntimeHostError(f"installed file drifted: {path}")
    if not CURRENT_LINK.is_symlink() or os.readlink(CURRENT_LINK) != f"candidates/{candidate.sha}":
        raise RuntimeHostError("current candidate pointer drifted")
    for directory, mode in (
        (STATE_ROOT, 0o700),
        (INSTALLER_ROOT, 0o700),
        (JOURNAL_ROOT, 0o700),
        (INSTALLER_ROOT / "uv-cache", 0o700),
        (ADAPTER_CONFIG_ROOT, 0o755),
        (CAPACITY_CONTRACT_PATH.parent.parent, 0o755),
        (CAPACITY_CONTRACT_PATH.parent, 0o755),
    ):
        metadata = directory.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise RuntimeHostError(f"installed directory drifted: {directory}")
    _reject_orphan_configs()
    loaded_units = _loaded_managed_units()
    unexpected = loaded_units - set(_all_units())
    if unexpected:
        raise RuntimeHostError("orphan shared-capacity unit is loaded")
    _reject_orphan_unit_files()
    for unit, fragment_path in _unit_fragment_paths().items():
        _validate_unit_fragment(unit, fragment_path)
    runtime_active = activation_mode in {
        "bootstrap-active",
        "acceptance-active",
        "activated",
    }
    if not runtime_active:
        for unit in _all_units():
            if _systemctl_state(unit) != {"enabled": False, "active": False}:
                raise RuntimeHostError(f"managed unit is not fail-closed: {unit}")
    else:
        for timer in _all_timers():
            if _systemctl_state(timer) != {"enabled": True, "active": True}:
                raise RuntimeHostError(f"managed timer is not active: {timer}")
        for service in _all_services():
            if _service_result(service) != ("success", "0"):
                raise RuntimeHostError(f"managed service result failed: {service}")
        _activated_adapter_readback(
            candidate,
            require_zero=activation_mode == "bootstrap-active",
        )
    installed_state = _load_json(STATE_PATH, "runtime-host state")
    _require_current_registry_binding(installed_state)
    installed_manifest = _cohort_from_runtime_manifest(
        installed_state.get("runtime_manifest"),
    )
    if (
        installed_state.get("candidate_sha") != candidate.sha
        or installed_state.get("candidate_tree") != candidate.tree
        or installed_state.get("activation_status") != activation_mode
        or installed_manifest.environments != _cohort().environments
        or installed_manifest.provisioning_environments != _cohort().provisioning_environments
    ):
        raise RuntimeHostError("runtime-host state candidate drifted")
    dynamic_storage_access = [
        _validate_dynamic_storage_access(environment)
        for environment in _cohort().provisioning_environments
        if environment.layout_version == "dynamic-v1"
    ]
    admission_token = installed_state.get("transaction_id")
    if runtime_active:
        if (
            not isinstance(admission_token, str)
            or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
        ):
            raise RuntimeHostError("runtime-host admission binding is invalid")
        observed_fence = _admission_fence(candidate)
        if (
            activation_mode in {"bootstrap-active", "acceptance-active"}
            and observed_fence != admission_token
        ):
            raise RuntimeHostError("closed general admission fence drifted")
        if activation_mode == "activated" and observed_fence is not None:
            raise RuntimeHostError("positive-capacity admission remains fenced")
        acceptance_contract = _acceptance_contract_readback(candidate)
        if activation_mode == "acceptance-active":
            if (
                not isinstance(acceptance_contract, dict)
                or acceptance_contract.get("session_id")
                != installed_state.get("acceptance_session_id")
                or acceptance_contract.get("admission_token") != admission_token
                or installed_state.get("acceptance_contract_sha256")
                != _sha256(_canonical_json(acceptance_contract))
            ):
                raise RuntimeHostError("acceptance-active contract drifted")
        elif acceptance_contract is not None:
            raise RuntimeHostError("acceptance contract exists outside acceptance-active state")
    return {
        "schema_version": 1,
        "status": "pass",
        "candidate_sha": candidate.sha,
        "candidate_tree": candidate.tree,
        "candidate_root": str(candidate.root),
        "instances": list(_instances()),
        "provisioning_instances": list(_provisioning_instances()),
        **_registry_binding(),
        "dynamic_storage_access": dynamic_storage_access,
        "activation_status": activation_mode,
        "timers_active": len(_all_timers()) if runtime_active else 0,
        "managed_units_disabled_and_inactive": (
            len(_all_units()) if activation_mode == "installed" else 0
        ),
        "capacity_enabled_by_install_command": False,
        "adapter_activation_authorized": runtime_active,
        "positive_capacity_admission_authorized": activation_mode == "activated",
        "acceptance_only_admission_authorized": activation_mode == "acceptance-active",
        "capacity_enabled_by_installer": False,
    }


def _environment_units(runtime_id: str) -> tuple[tuple[str, str], ...]:
    return tuple(
        (
            f"loom-shared-capacity-adapter@{runtime_id}-{pool}.service",
            f"loom-shared-capacity-adapter@{runtime_id}-{pool}.timer",
        )
        for pool in POOLS
    )


def _environment_configs(runtime_id: str) -> tuple[Path, ...]:
    return tuple(ADAPTER_CONFIG_ROOT / f"{runtime_id}-{pool}.toml" for pool in POOLS)


def _registry_admission_intent(
    runtime_id: str,
    *,
    states: frozenset[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        raw = registry_contract._read_regular(
            REGISTRY_SNAPSHOT_PATH,
            limit=16 * 1024 * 1024,
        )
        snapshot = registry_contract.DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except (OSError, registry_contract.RegistryError) as exc:
        raise RuntimeHostError("runtime registry admission intent is unavailable") from exc
    matches = [
        row
        for row in cast(list[dict[str, Any]], snapshot["environments"])
        if row["runtime_id"] == runtime_id
    ]
    if len(matches) != 1 or matches[0]["state"] not in states:
        raise RuntimeHostError("runtime registry admission intent is invalid")
    return snapshot, matches[0]


def _registry_broker_state_db() -> Path:
    try:
        raw = registry_contract._read_regular(
            SUPERVISOR_BASE_CONFIG_PATH,
            limit=64 * 1024,
        )
        metadata = SUPERVISOR_BASE_CONFIG_PATH.lstat()
        payload = tomllib.loads(raw.decode("ascii"))
        state_db = Path(str(payload["state_db"]))
    except (
        OSError,
        registry_contract.RegistryError,
        UnicodeDecodeError,
        tomllib.TOMLDecodeError,
        KeyError,
        TypeError,
    ) as exc:
        raise RuntimeHostError("fixed registry broker state is invalid") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o644
        or state_db != BROKER_STATE_DB_PATH
    ):
        raise RuntimeHostError("fixed registry broker state is invalid")
    return state_db


def _environment_admission_intent(
    runtime_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if registry_contract.RUNTIME_ID_RE.fullmatch(runtime_id) is None:
        raise RuntimeHostError("runtime environment identity is invalid")
    payload = _load_json(
        ENVIRONMENT_ADMISSION_INTENT_ROOT / f"{runtime_id}.json",
        "environment admission intent",
    )
    unsigned = dict(payload)
    payload_sha256 = unsigned.pop("payload_sha256", None)
    immutable_fields = (
        "env_id",
        "principal_id",
        "runtime_id",
        "operation",
        "target_candidate_id",
        "current_candidate_id",
        "resource_generation",
        "expected_resource_generation",
        "applied_resource_generation",
        "idempotency_key",
        "registry_generation",
        "registry_payload_sha256",
        "prior_admission_token",
    )
    immutable = {field: payload.get(field) for field in immutable_fields}
    if (
        payload.get("schema_version") != 1
        or payload.get("kind") != ENVIRONMENT_ADMISSION_INTENT_KIND
        or payload.get("runtime_id") != runtime_id
        or payload.get("operation") not in {"create", "update", "retire"}
        or payload.get("phase")
        not in {"recorded", "fenced", "registry-transitioned", "activated", "retired"}
        or registry_contract.ENV_ID_RE.fullmatch(str(payload.get("env_id"))) is None
        or registry_contract.RUNTIME_ID_RE.fullmatch(str(payload.get("runtime_id"))) is None
        or type(payload.get("resource_generation")) is not int
        or cast(int, payload["resource_generation"]) < 1
        or payload.get("expected_resource_generation") != payload["resource_generation"]
        or payload.get("applied_resource_generation")
        != cast(int, payload["expected_resource_generation"]) + 1
        or type(payload.get("registry_generation")) is not int
        or cast(int, payload["registry_generation"]) < 1
        or DIGEST_RE.fullmatch(str(payload.get("registry_payload_sha256"))) is None
        or DIGEST_RE.fullmatch(str(payload.get("intent_sha256"))) is None
        or DIGEST_RE.fullmatch(str(payload_sha256)) is None
        or _sha256(_canonical_json(unsigned)) != payload_sha256
        or _sha256(_canonical_json(immutable)) != payload["intent_sha256"]
        or (
            payload.get("prior_admission_token") is not None
            and re.fullmatch(
                r"[0-9a-f]{32}",
                str(payload.get("prior_admission_token")),
            )
            is None
        )
    ):
        raise RuntimeHostError("environment admission intent is invalid")
    snapshot, environment = _registry_admission_intent(
        runtime_id,
        states=frozenset({"ready", "active", "deploying", "quarantined"}),
    )
    exact_environment = {
        "env_id": environment["env_id"],
        "principal_id": environment["principal_id"],
        "runtime_id": environment["runtime_id"],
    }
    applied_active = (
        payload["operation"] in {"create", "update"}
        and environment["state"] == "active"
        and environment["current_candidate_id"] == payload["target_candidate_id"]
    )
    expected_generation = (
        payload["applied_resource_generation"]
        if applied_active
        else payload["expected_resource_generation"]
    )
    if (
        any(payload.get(field) != value for field, value in exact_environment.items())
        or environment["resource_generation"] != expected_generation
    ):
        raise RuntimeHostError("environment admission intent registry binding drifted")
    return snapshot, payload


def _environment_admission_token(intent: Mapping[str, Any]) -> str:
    return hashlib.sha256(
        _canonical_json(
            {
                "env_id": intent["env_id"],
                "runtime_id": intent["runtime_id"],
                "expected_resource_generation": intent["expected_resource_generation"],
                "applied_resource_generation": intent["applied_resource_generation"],
                "current_candidate_id": intent["current_candidate_id"],
                "target_candidate_id": intent["target_candidate_id"],
                "operation": intent["operation"],
                "intent_sha256": intent["intent_sha256"],
            }
        )
    ).hexdigest()[:32]


def _environment_broker_action(
    source: str,
    runtime_id: str,
    *values: str,
) -> str:
    completed = _run(
        (
            sys.executable,
            "-I",
            "-B",
            "-c",
            source,
            str(LOOM_RUNTIME_PYTHON_ROOT),
            str(_registry_broker_state_db()),
            runtime_id,
            *values,
        )
    )
    observed = completed.stdout.strip()
    if observed != "open" and re.fullmatch(r"[0-9a-f]{32}", observed) is None:
        raise RuntimeHostError("environment admission fence readback is invalid")
    return observed


def fence_registry_environment(runtime_id: str) -> dict[str, Any]:
    """Persist and read back one exact-env fence after registry intent exists."""

    _require_live_host()
    snapshot, environment = _registry_admission_intent(
        runtime_id,
        states=frozenset({"deploying", "quarantined"}),
    )
    unsigned = {
        "env_id": environment["env_id"],
        "runtime_id": runtime_id,
        "resource_generation": environment["resource_generation"],
        "registry_generation": snapshot["generation"],
        "registry_payload_sha256": snapshot["payload_sha256"],
        "state": environment["state"],
    }
    token = hashlib.sha256(_canonical_json(unsigned)).hexdigest()[:32]
    observed = _environment_broker_action(
        _BROKER_ENVIRONMENT_CLOSE,
        runtime_id,
        token,
    )
    if observed != token:
        raise RuntimeHostError("environment admission fence did not close exactly")
    return {
        "schema_version": 1,
        "status": "ready",
        "runtime_id": runtime_id,
        "instances": [f"{runtime_id}-{pool}" for pool in POOLS],
    }


def fence_registry_environment_intent(
    runtime_id: str,
    intent_sha256: str,
) -> dict[str, Any]:
    """Close one exact environment while its registry state is still active/ready."""

    _require_live_host()
    snapshot, intent = _environment_admission_intent(runtime_id)
    if intent["intent_sha256"] != intent_sha256 or intent["phase"] not in {"recorded", "fenced"}:
        raise RuntimeHostError("environment admission intent digest is invalid")
    environment = next(
        row
        for row in cast(list[dict[str, Any]], snapshot["environments"])
        if row["runtime_id"] == runtime_id
    )
    if environment["state"] not in {"ready", "active"}:
        raise RuntimeHostError("environment must be active or ready before fencing")
    if (
        environment["current_candidate_id"] != intent["current_candidate_id"]
        or (intent["operation"] == "create" and environment["current_candidate_id"] is not None)
        or (intent["operation"] == "update" and environment["current_candidate_id"] is None)
        or (
            intent["operation"] != "retire"
            and not any(
                candidate["candidate_id"] == intent["target_candidate_id"]
                and candidate["env_id"] == intent["env_id"]
                for candidate in cast(
                    list[dict[str, Any]],
                    snapshot["candidates"],
                )
            )
        )
    ):
        raise RuntimeHostError("environment admission candidate binding drifted")
    token = _environment_admission_token(intent)
    current = environment_admission_readback(runtime_id)
    if current is None:
        if (
            snapshot["generation"] != intent["registry_generation"]
            or snapshot["payload_sha256"] != intent["registry_payload_sha256"]
        ):
            raise RuntimeHostError("environment admission registry snapshot drifted")
        current = _environment_broker_action(
            _BROKER_ENVIRONMENT_CLOSE,
            runtime_id,
            token,
        )
    elif current == intent.get("prior_admission_token") and current != token:
        current = _environment_broker_action(
            _BROKER_ENVIRONMENT_ROTATE,
            runtime_id,
            current,
            token,
        )
    if current != token or environment_admission_readback(runtime_id) != token:
        raise RuntimeHostError("environment admission fence did not close exactly")
    unsigned = {
        "schema_version": 1,
        "status": "ready",
        "runtime_id": runtime_id,
        "intent_sha256": intent_sha256,
        "admission_token": token,
        "registry_generation": intent["registry_generation"],
        "registry_payload_sha256": intent["registry_payload_sha256"],
        "instances": [f"{runtime_id}-{pool}" for pool in POOLS],
    }
    return {**unsigned, "payload_sha256": _sha256(_canonical_json(unsigned))}


def environment_admission_readback(runtime_id: str) -> str | None:
    observed = _environment_broker_action(
        _BROKER_ENVIRONMENT_READBACK,
        runtime_id,
    )
    return None if observed == "open" else observed


def reopen_registry_environment_admission(runtime_id: str) -> dict[str, Any]:
    """Open only the exact persisted fence after the registry reports active."""

    _require_live_host()
    snapshot, intent = _environment_admission_intent(runtime_id)
    environment = next(
        row
        for row in cast(list[dict[str, Any]], snapshot["environments"])
        if row["runtime_id"] == runtime_id
    )
    if environment["state"] not in {"ready", "active"}:
        raise RuntimeHostError("environment cannot reopen admission")
    if intent["operation"] not in {"create", "update"}:
        raise RuntimeHostError("retirement admission cannot be reopened")
    expected_candidate = (
        intent["target_candidate_id"]
        if environment["state"] == "active"
        and environment["current_candidate_id"] == intent["target_candidate_id"]
        else intent["current_candidate_id"]
    )
    if environment["current_candidate_id"] != expected_candidate:
        raise RuntimeHostError("environment admission reopen binding drifted")
    deployment_id = intent.get("deployment_id")
    if not isinstance(deployment_id, str) or not deployment_id.startswith("dep-"):
        raise RuntimeHostError("environment admission deployment binding is invalid")
    deployments = [
        row
        for row in cast(list[dict[str, Any]], snapshot["deployments"])
        if row["deployment_id"] == deployment_id
        and row["env_id"] == intent["env_id"]
        and row["candidate_id"] == intent["target_candidate_id"]
        and row["expected_resource_generation"] == intent["expected_resource_generation"]
    ]
    if len(deployments) != 1:
        raise RuntimeHostError("environment admission deployment binding is invalid")
    deployment = deployments[0]
    committed = (
        environment["state"] == "active"
        and environment["current_candidate_id"] == intent["target_candidate_id"]
    )
    if committed:
        if (
            deployment["phase"] != "committed"
            or deployment["applied_resource_generation"] != intent["applied_resource_generation"]
            or deployment["finalization_payload_sha256"]
            != intent.get("finalization_payload_sha256")
            or DIGEST_RE.fullmatch(
                str(intent.get("finalization_payload_sha256")),
            )
            is None
            or not any(
                row["deployment_id"] == deployment_id
                and row["payload_sha256"] == intent["finalization_payload_sha256"]
                and row["applied_resource_generation"] == intent["applied_resource_generation"]
                and row["candidate_id"] == intent["target_candidate_id"]
                for row in cast(
                    list[dict[str, Any]],
                    snapshot["deployment_finalizations"],
                )
            )
        ):
            raise RuntimeHostError(
                "environment admission finalization binding is invalid",
            )
    elif deployment["phase"] != "failed":
        raise RuntimeHostError("environment admission rollback binding is invalid")
    token = environment_admission_readback(runtime_id)
    if token is None:
        return {
            "schema_version": 1,
            "status": "ready",
            "runtime_id": runtime_id,
            "instances": [f"{runtime_id}-{pool}" for pool in POOLS],
        }
    if token != _environment_admission_token(intent):
        raise RuntimeHostError("environment admission token drifted")
    observed = _environment_broker_action(
        _BROKER_ENVIRONMENT_OPEN,
        runtime_id,
        token,
    )
    if observed != "open":
        raise RuntimeHostError("environment admission fence remained closed")
    return {
        "schema_version": 1,
        "status": "ready",
        "runtime_id": runtime_id,
        "instances": [f"{runtime_id}-{pool}" for pool in POOLS],
    }


def _restore_exact_units(states: Mapping[str, Mapping[str, bool]]) -> None:
    _run(("systemctl", "daemon-reload"))
    for unit, state in states.items():
        if state["enabled"]:
            _run(("systemctl", "enable", unit), expected={0, 1})
        else:
            _run(("systemctl", "disable", unit), expected={0, 1, 5})
        if state["active"]:
            _run(("systemctl", "start", unit))
        else:
            _run(("systemctl", "stop", unit), expected={0, 5})


def _recover_environment_reconcile(runtime_id: str) -> None:
    path = ENVIRONMENT_RECONCILE_ROOT / f"{runtime_id}.json"
    if not path.exists():
        return
    payload = _load_json(path, "environment reconcile journal")
    if (
        payload.get("schema_version") != 1
        or payload.get("runtime_id") != runtime_id
        or not isinstance(payload.get("files"), dict)
        or not isinstance(payload.get("units"), dict)
        or (payload.get("state_b64") is not None and not isinstance(payload.get("state_b64"), str))
    ):
        raise RuntimeHostError("environment reconcile journal is invalid")
    _restore_files(cast(dict[str, dict[str, Any]], payload["files"]))
    encoded_state = payload.get("state_b64")
    if encoded_state is None:
        _remove_path(STATE_PATH)
    else:
        try:
            previous_state = base64.b64decode(encoded_state, validate=True)
        except (ValueError, TypeError) as exc:
            raise RuntimeHostError("environment reconcile journal state is invalid") from exc
        _atomic_write(STATE_PATH, previous_state, mode=0o600)
    _restore_exact_units(cast(dict[str, dict[str, bool]], payload["units"]))
    path.unlink()
    _fsync_directory(ENVIRONMENT_RECONCILE_ROOT)


def reconcile_registry_environment(runtime_id: str) -> dict[str, Any]:
    """Incrementally publish one registry environment without touching peers."""

    _require_live_host()
    environment = _registry_environment(runtime_id, provisioning=True)
    with _lock():
        if environment_admission_readback(runtime_id) is None:
            raise RuntimeHostError("environment admission is not fail-closed")
        _recover_orphan()
        _recover_environment_reconcile(runtime_id)
        state = (
            _load_json(STATE_PATH, "runtime-host state")
            if STATE_PATH.exists()
            else {
                "schema_version": 1,
                "activation_status": "installed",
                "installation_mode": "fixed-registry-runtime",
                "admission_state": "closed",
            }
        )
        sha = state.get("candidate_sha")
        tree = state.get("candidate_tree")
        activation_status = state.get("activation_status")
        legacy_candidate = isinstance(sha, str) and SHA_RE.fullmatch(sha) is not None
        if activation_status not in {
            "installed",
            "bootstrap-active",
            "acceptance-active",
            "activated",
        }:
            raise RuntimeHostError("runtime-host state is unavailable for environment reconcile")
        if legacy_candidate:
            if not isinstance(tree, str) or SHA_RE.fullmatch(tree) is None:
                raise RuntimeHostError("runtime-host state candidate binding is invalid")
            _verify_installed_candidate(
                Candidate(
                    sha=cast(str, sha),
                    tree=tree,
                    source=CANDIDATE_PARENT / cast(str, sha) / "repo",
                )
            )
        configs = (
            *_environment_configs(runtime_id),
            SUPERVISOR_CONFIG_PATH,
            EMPTY_COHORT_PATH,
        )
        units = (
            SUPERVISOR_SERVICE,
            SUPERVISOR_TIMER,
            *(unit for pair in _environment_units(runtime_id) for unit in pair),
        )
        previous_files = _capture_files(configs)
        previous_units = {unit: _systemctl_state(unit) for unit in units}
        previous_state = STATE_PATH.read_bytes() if STATE_PATH.exists() else None
        transaction = {
            "schema_version": 1,
            "runtime_id": runtime_id,
            "registry_generation": _cohort().generation,
            "registry_payload_sha256": _cohort().payload_sha256,
            "candidate_sha": environment.candidate_sha,
            "candidate_tree": environment.candidate_tree,
            "files": previous_files,
            "units": previous_units,
            "state_b64": (
                None if previous_state is None else base64.b64encode(previous_state).decode("ascii")
            ),
        }
        _ensure_directory(ENVIRONMENT_RECONCILE_ROOT, mode=0o700)
        journal_path = ENVIRONMENT_RECONCILE_ROOT / f"{runtime_id}.json"
        _atomic_write(journal_path, _canonical_json(transaction), mode=0o600)
        try:
            for service, timer in _environment_units(runtime_id):
                _run(("systemctl", "stop", timer), expected={0, 5})
                _run(("systemctl", "stop", service), expected={0, 5})
            for pool, path in zip(POOLS, _environment_configs(runtime_id), strict=True):
                _atomic_write(
                    path,
                    _render_adapter_config(environment, pool),
                    mode=0o600,
                )
            _atomic_write(
                SUPERVISOR_CONFIG_PATH,
                _render_registry_supervisor_config_for_instances(
                    _provisioning_instances(),
                ),
                mode=0o600,
            )
            _run(("systemctl", "daemon-reload"))
            if activation_status != "installed":
                _run(("systemctl", "start", SUPERVISOR_SERVICE))
                if _service_result(SUPERVISOR_SERVICE) != ("success", "0"):
                    raise RuntimeHostError("registry supervisor activation cycle failed")
                _run(("systemctl", "enable", "--now", SUPERVISOR_TIMER))
                for service, timer in _environment_units(runtime_id):
                    _run(("systemctl", "start", service))
                    if _service_result(service) != ("success", "0"):
                        raise RuntimeHostError("environment adapter activation cycle failed")
                    _run(("systemctl", "enable", "--now", timer))
            updated = dict(state)
            updated.update(_registry_binding())
            updated["runtime_manifest"] = _runtime_manifest()
            _atomic_write(STATE_PATH, _canonical_json(updated), mode=0o600)
            report = check_registry_environment(
                runtime_id,
                require_admission_open=False,
            )
            _remove_path(EMPTY_COHORT_PATH)
        except Exception:
            _restore_files(previous_files)
            if previous_state is None:
                _remove_path(STATE_PATH)
            else:
                _atomic_write(STATE_PATH, previous_state, mode=0o600)
            _restore_exact_units(previous_units)
            raise
        journal_path.unlink(missing_ok=True)
        _fsync_directory(ENVIRONMENT_RECONCILE_ROOT)
        return report


def check_registry_environment(
    runtime_id: str,
    *,
    require_admission_open: bool = True,
) -> dict[str, Any]:
    environment = _registry_environment(runtime_id, provisioning=True)
    state = _load_json(STATE_PATH, "runtime-host state")
    activation_status = state.get("activation_status")
    admission_fence = environment_admission_readback(runtime_id)
    if environment.state == "deploying" and admission_fence is None:
        raise RuntimeHostError("deploying environment admission is open")
    if environment.state == "active" and require_admission_open and admission_fence is not None:
        raise RuntimeHostError("active environment admission remains fenced")
    if environment.state == "active" and not require_admission_open and admission_fence is None:
        raise RuntimeHostError("active reconcile lost its admission fence")
    expected_supervisor_config = (
        _render_registry_supervisor_config_for_instances(
            _provisioning_instances(),
        )
        if environment.state == "deploying"
        else _render_registry_supervisor_config()
    )
    if SUPERVISOR_CONFIG_PATH.read_bytes() != expected_supervisor_config:
        raise RuntimeHostError("registry supervisor config drifted")
    for pool, path in zip(POOLS, _environment_configs(runtime_id), strict=True):
        metadata = path.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or path.read_bytes() != _render_adapter_config(environment, pool)
        ):
            raise RuntimeHostError("environment adapter config drifted")
    for service, timer in _environment_units(runtime_id):
        if activation_status == "installed":
            expected = {"enabled": False, "active": False}
            if _systemctl_state(service) != expected or _systemctl_state(timer) != expected:
                raise RuntimeHostError("environment adapter is not fail-closed")
        else:
            if _service_result(service) != ("success", "0") or _systemctl_state(timer) != {
                "enabled": True,
                "active": True,
            }:
                raise RuntimeHostError("environment adapter activation readback failed")
    return {
        "schema_version": 1,
        "status": "prepared" if activation_status == "installed" else "ready",
        "runtime_id": runtime_id,
        "candidate_id": environment.candidate_id,
        "candidate_sha": environment.candidate_sha,
        "candidate_tree": environment.candidate_tree,
        "resource_generation": environment.resource_generation,
        **_registry_binding(),
        "instances": [f"{runtime_id}-{pool}" for pool in POOLS],
        "activation_status": activation_status,
    }


def _update_environment_retire(
    path: Path,
    payload: dict[str, Any],
    phase: str,
) -> None:
    payload["phase"] = phase
    _atomic_write(path, _canonical_json(payload), mode=0o600)


def _require_file_snapshot(snapshot: Mapping[str, Any]) -> None:
    paths = tuple(Path(raw_path) for raw_path in snapshot)
    if _capture_files(paths) != snapshot:
        raise RuntimeHostError("peer runtime evidence changed during retirement")


def _rollback_environment_retire(
    journal_path: Path,
    payload: Mapping[str, Any],
) -> None:
    files = payload.get("files")
    units = payload.get("units")
    peer_files = payload.get("peer_files")
    if (
        not isinstance(files, dict)
        or not isinstance(units, dict)
        or not isinstance(peer_files, dict)
    ):
        raise RuntimeHostError("environment retire journal is invalid")
    _run(("systemctl", "stop", SUPERVISOR_TIMER), expected={0, 5})
    _run(("systemctl", "stop", SUPERVISOR_SERVICE), expected={0, 5})
    _restore_files(files)
    _run(("systemctl", "daemon-reload"))
    supervisor_was_operational = any(
        cast(dict[str, Any], units).get(unit, {}).get(field) is True
        for unit in (SUPERVISOR_SERVICE, SUPERVISOR_TIMER)
        for field in ("enabled", "active")
    )
    supervisor_config = cast(dict[str, Any], files).get(
        str(SUPERVISOR_CONFIG_PATH),
        {},
    )
    if supervisor_was_operational and supervisor_config.get("present") is True:
        _run(("systemctl", "start", SUPERVISOR_SERVICE))
        if _service_result(SUPERVISOR_SERVICE) != ("success", "0"):
            raise RuntimeHostError("environment retire rollback supervisor failed")
    _restore_exact_units(cast(dict[str, dict[str, bool]], units))
    _require_file_snapshot(cast(dict[str, dict[str, Any]], peer_files))
    journal_path.unlink(missing_ok=True)
    _fsync_directory(ENVIRONMENT_RETIRE_ROOT)


def _recover_environment_retire(runtime_id: str) -> None:
    journal_path = ENVIRONMENT_RETIRE_ROOT / f"{runtime_id}.json"
    if not journal_path.exists():
        return
    payload = _load_json(journal_path, "environment retire journal")
    if (
        payload.get("schema_version") != 1
        or payload.get("operation") != "retire-environment"
        or payload.get("runtime_id") != runtime_id
        or payload.get("phase")
        not in {
            "prepared",
            "supervisor-stopped",
            "target-stopped",
            "observations-removed",
            "reduced-config-published",
            "supervisor-migrated",
            "empty-cohort-published",
            "read-back",
            "committed",
        }
        or not isinstance(payload.get("files"), dict)
        or not isinstance(payload.get("peer_files"), dict)
        or not isinstance(payload.get("units"), dict)
    ):
        raise RuntimeHostError("environment retire journal is invalid")
    _rollback_environment_retire(journal_path, payload)


def retire_registry_environment(runtime_id: str) -> dict[str, Any]:
    """Transactionally subtract one drained environment from the fixed runtime."""

    _require_live_host()
    if registry_contract.RUNTIME_ID_RE.fullmatch(runtime_id) is None:
        raise RuntimeHostError("runtime environment identity is invalid")
    with _lock():
        if environment_admission_readback(runtime_id) is None:
            raise RuntimeHostError("retirement requires a closed environment fence")
        _recover_environment_retire(runtime_id)
        snapshot, target = _registry_admission_intent(
            runtime_id,
            states=frozenset({"quarantined"}),
        )
        peer_runtime_ids = tuple(
            sorted(
                str(environment["runtime_id"])
                for environment in cast(
                    list[dict[str, Any]],
                    snapshot["environments"],
                )
                if environment["state"] == "active" and environment["runtime_id"] != runtime_id
            )
        )
        peer_instances = tuple(
            f"{peer_runtime_id}-{pool}" for peer_runtime_id in peer_runtime_ids for pool in POOLS
        )
        target_instances = tuple(f"{runtime_id}-{pool}" for pool in POOLS)
        target_observations = tuple(
            STATE_ROOT / "observations" / f"{instance}.json" for instance in target_instances
        )
        peer_observations = tuple(
            STATE_ROOT / "observations" / f"{instance}.json" for instance in peer_instances
        )
        peer_handoffs = tuple(
            STATE_ROOT / "handoffs" / "current" / f"{instance}.json" for instance in peer_instances
        )
        base = _registry_supervisor_base()
        try:
            supervisor_state_path = Path(str(base["supervisor_state_path"]))
            handoff_root = Path(str(base["handoff_dir"]))
        except (KeyError, TypeError) as exc:
            raise RuntimeHostError("fixed supervisor runtime paths are invalid") from exc
        if (
            not supervisor_state_path.is_absolute()
            or not handoff_root.is_absolute()
            or STATE_ROOT not in supervisor_state_path.parents
            or STATE_ROOT not in handoff_root.parents
            or ".." in supervisor_state_path.parts
            or ".." in handoff_root.parts
        ):
            raise RuntimeHostError("fixed supervisor runtime paths are invalid")
        zero_cohort = not peer_instances
        rollback_paths = [
            *_environment_configs(runtime_id),
            *target_observations,
            SUPERVISOR_CONFIG_PATH,
            EMPTY_COHORT_PATH,
        ]
        if zero_cohort:
            rollback_paths.extend(
                (
                    supervisor_state_path,
                    handoff_root / "current",
                )
            )
        units = (
            SUPERVISOR_SERVICE,
            SUPERVISOR_TIMER,
            *(unit for pair in _environment_units(runtime_id) for unit in pair),
        )
        transaction = {
            "schema_version": 1,
            "operation": "retire-environment",
            "phase": "prepared",
            "runtime_id": runtime_id,
            "env_id": target["env_id"],
            "resource_generation": target["resource_generation"],
            "registry_generation": snapshot["generation"],
            "registry_payload_sha256": snapshot["payload_sha256"],
            "zero_cohort": zero_cohort,
            "peer_instances": list(peer_instances),
            "files": _capture_files(tuple(rollback_paths)),
            "peer_files": _capture_files((*peer_observations, *peer_handoffs)),
            "units": {unit: _systemctl_state(unit) for unit in units},
        }
        _ensure_directory(ENVIRONMENT_RETIRE_ROOT, mode=0o700)
        journal_path = ENVIRONMENT_RETIRE_ROOT / f"{runtime_id}.json"
        _atomic_write(journal_path, _canonical_json(transaction), mode=0o600)
        try:
            _run(("systemctl", "stop", SUPERVISOR_TIMER), expected={0, 5})
            _run(("systemctl", "stop", SUPERVISOR_SERVICE), expected={0, 5})
            _update_environment_retire(journal_path, transaction, "supervisor-stopped")
            for service, timer in _environment_units(runtime_id):
                _run(("systemctl", "disable", "--now", timer), expected={0, 1, 5})
                _run(("systemctl", "stop", service), expected={0, 5})
            _update_environment_retire(journal_path, transaction, "target-stopped")
            for path in target_observations:
                _remove_path(path)
            _update_environment_retire(
                journal_path,
                transaction,
                "observations-removed",
            )
            if zero_cohort:
                _run(
                    ("systemctl", "disable", SUPERVISOR_TIMER),
                    expected={0, 1, 5},
                )
                _run(
                    ("systemctl", "disable", SUPERVISOR_SERVICE),
                    expected={0, 1, 5},
                )
                _remove_path(SUPERVISOR_CONFIG_PATH)
                _remove_path(supervisor_state_path)
                _remove_path(handoff_root / "current")
                empty = {
                    "schema_version": 1,
                    "status": "empty-fail-closed",
                    "retired_runtime_id": runtime_id,
                    "registry_generation": snapshot["generation"],
                    "registry_payload_sha256": snapshot["payload_sha256"],
                }
                _atomic_write(
                    EMPTY_COHORT_PATH,
                    _canonical_json(empty),
                    mode=0o600,
                )
                _update_environment_retire(
                    journal_path,
                    transaction,
                    "empty-cohort-published",
                )
            else:
                reduced_config = _render_registry_supervisor_config_for_instances(
                    peer_instances,
                )
                _atomic_write(
                    SUPERVISOR_CONFIG_PATH,
                    reduced_config,
                    mode=0o600,
                )
                _update_environment_retire(
                    journal_path,
                    transaction,
                    "reduced-config-published",
                )
                _run(("systemctl", "start", SUPERVISOR_SERVICE))
                if _service_result(SUPERVISOR_SERVICE) != ("success", "0"):
                    raise RuntimeHostError(
                        "registry supervisor subtractive migration failed",
                    )
                _update_environment_retire(
                    journal_path,
                    transaction,
                    "supervisor-migrated",
                )
                supervisor_units = {
                    unit: cast(dict[str, dict[str, bool]], transaction["units"])[unit]
                    for unit in (SUPERVISOR_SERVICE, SUPERVISOR_TIMER)
                }
                _restore_exact_units(supervisor_units)
                if SUPERVISOR_CONFIG_PATH.read_bytes() != reduced_config:
                    raise RuntimeHostError(
                        "registry supervisor reduced config drifted",
                    )
                _require_file_snapshot(
                    cast(dict[str, dict[str, Any]], transaction["peer_files"]),
                )
            for path in target_observations:
                if path.exists() or path.is_symlink():
                    raise RuntimeHostError("retired adapter observation remains installed")
            for service, timer in _environment_units(runtime_id):
                if _systemctl_state(service) != {
                    "enabled": False,
                    "active": False,
                } or _systemctl_state(timer) != {
                    "enabled": False,
                    "active": False,
                }:
                    raise RuntimeHostError("retired adapter unit remains enabled")
            if zero_cohort:
                if (
                    SUPERVISOR_CONFIG_PATH.exists()
                    or SUPERVISOR_CONFIG_PATH.is_symlink()
                    or _systemctl_state(SUPERVISOR_SERVICE) != {"enabled": False, "active": False}
                    or _systemctl_state(SUPERVISOR_TIMER) != {"enabled": False, "active": False}
                ):
                    raise RuntimeHostError("empty supervisor cohort is not fail-closed")
            _update_environment_retire(journal_path, transaction, "read-back")
            for path in _environment_configs(runtime_id):
                _remove_path(path)
            _run(("systemctl", "daemon-reload"))
            if any(path.exists() or path.is_symlink() for path in _environment_configs(runtime_id)):
                raise RuntimeHostError("retired adapter config remains installed")
            _update_environment_retire(journal_path, transaction, "committed")
        except Exception:
            _rollback_environment_retire(journal_path, transaction)
            raise
        journal_path.unlink()
        _fsync_directory(ENVIRONMENT_RETIRE_ROOT)
    return {
        "schema_version": 1,
        "status": "ready",
        "runtime_id": runtime_id,
        "instances": list(target_instances),
        "peer_instances": list(peer_instances),
        "zero_cohort": zero_cohort,
        "registry_generation": snapshot["generation"],
        "registry_payload_sha256": snapshot["payload_sha256"],
    }


def install(candidate: Candidate) -> dict[str, Any]:
    _require_live_host()
    _load_candidate_profile(candidate)
    with _lock():
        _recover_orphan()
        if STATE_PATH.exists() or STATE_PATH.is_symlink():
            current_state = _load_json(STATE_PATH, "runtime-host state")
            if current_state.get("activation_status") in {
                "bootstrap-active",
                "acceptance-active",
                "activated",
            }:
                raise RuntimeHostError(
                    "activated runtime must be retired through rollback before install",
                )
        _reject_orphan_stages()
        _reject_orphan_configs()
        _loaded_managed_units()
        _installed_managed_unit_files()
        journal_path, journal = _write_journal(candidate, operation="install")
        try:
            _stop_units()
            _update_journal(journal_path, journal, "stopped")
            staging_path = Path(str(journal["staging_path"]))
            _materialize_candidate(candidate, staging_path)
            _update_journal(journal_path, journal, "materialized")
            installed_candidate = Candidate(
                sha=candidate.sha,
                tree=candidate.tree,
                source=candidate.repo,
            )
            _publish_files(installed_candidate)
            _update_journal(journal_path, journal, "published")
            state = {
                "schema_version": 1,
                "candidate_sha": candidate.sha,
                "candidate_tree": candidate.tree,
                "activation_status": "installed",
                "installed_at": datetime.now(UTC).isoformat(),
                "transaction_id": journal["transaction_id"],
                **_registry_binding(),
                "runtime_manifest": _runtime_manifest(),
            }
            _atomic_write(STATE_PATH, _canonical_json(state), mode=0o600)
            _publish_unit_state()
            _update_journal(journal_path, journal, "fail-closed")
            report = check(installed_candidate)
            _update_journal(journal_path, journal, "committed")
            ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
            _fsync_directory(INSTALLER_ROOT)
            return report
        except Exception:
            _restore_transaction(journal_path, journal)
            raise


def activation_plan(sha: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("activation requires the installed full candidate SHA")
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-runtime-host-activation-plan",
        "mutation_authorized": False,
        "candidate_sha": sha,
        **_registry_binding(),
        "instances": list(_instances()),
        "steps": [
            "verify-installed-inactive-exact-candidate",
            "journal-and-fence-new-broker-requests-by-installed-transaction",
            "require-registry-terminal-disabled-zero-handoffs-and-zero-broker-counters",
            "validate-registry-config-secret-combined-receipt-and-zero-cp-policies",
            "run-supervisor-once-and-read-back-complete-generation",
            "enable-supervisor-timer",
            "run-and-enable-registry-adapter-timers",
            "commit-bootstrap-active-state-with-exact-admission-fence-closed",
            "rollback-to-all-disabled-on-any-failure",
        ],
        "positive_capacity_admission_authorized": False,
    }


def activate(sha: str) -> dict[str, Any]:
    _require_live_host()
    _require_active_cohort()
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("activation requires the installed full candidate SHA")
    with _lock():
        _recover_orphan()
        _reject_orphan_stages()
        state = _load_json(STATE_PATH, "runtime-host state")
        tree = state.get("candidate_tree")
        if (
            state.get("candidate_sha") != sha
            or not isinstance(tree, str)
            or SHA_RE.fullmatch(tree) is None
        ):
            raise RuntimeHostError("activation SHA does not match the installed candidate")
        candidate = Candidate(
            sha=sha,
            tree=tree,
            source=CANDIDATE_PARENT / sha / "repo",
        )
        if state.get("activation_status") in {
            "bootstrap-active",
            "acceptance-active",
            "activated",
        }:
            return check(candidate, activation_mode=str(state["activation_status"]))
        if state.get("activation_status") != "installed":
            raise RuntimeHostError("runtime-host activation state is invalid")
        check(candidate, activation_mode="installed")
        admission_token = state.get("transaction_id")
        if (
            not isinstance(admission_token, str)
            or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
        ):
            raise RuntimeHostError("runtime-host admission binding is invalid")
        journal_path, journal = _write_journal(
            candidate,
            operation="activate",
            admission_token=admission_token,
        )
        try:
            _update_journal(journal_path, journal, "activation-closing-admission")
            _activation_preflight(
                candidate,
                transaction_id=admission_token,
            )
            _update_journal(journal_path, journal, "activation-preflight-passed")
            _activate_units(candidate)
            _update_journal(journal_path, journal, "units-activated")
            activated_state = dict(state)
            activated_state["activation_status"] = "bootstrap-active"
            activated_state["bootstrap_activated_at"] = datetime.now(UTC).isoformat()
            activated_state["activation_transaction_id"] = journal["transaction_id"]
            _atomic_write(
                STATE_PATH,
                _canonical_json(activated_state),
                mode=0o600,
            )
            report = check(candidate, activation_mode="bootstrap-active")
            _update_journal(journal_path, journal, "committed")
            ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
            _fsync_directory(INSTALLER_ROOT)
            return report
        except Exception:
            _restore_transaction(journal_path, journal)
            raise


def admission_plan(sha: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("admission requires the installed full candidate SHA")
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-runtime-host-admission-plan",
        "mutation_authorized": False,
        "candidate_sha": sha,
        **_registry_binding(),
        "instances": list(_instances()),
        "steps": [
            "verify-bootstrap-active-runtime-and-exact-closed-admission-fence",
            "require-fresh-complete-candidate-bound-platform-health-live-evidence",
            "re-read-registry-zero-handoffs-policies-workers-and-adapter-state",
            "persist-evidence-digest-before-opening-admission",
            "release-only-the-installed-transaction-admission-fence",
            "commit-positive-capacity-admitted-state",
        ],
        "positive_capacity_admission_authorized": False,
    }


def _resume_admission(
    path: Path,
    payload: dict[str, Any],
) -> dict[str, Any]:
    (
        _transaction_id,
        operation,
        sha,
        tree,
        _files,
        _units,
    ) = _validate_transaction(path, payload)
    admission_token = payload.get("admission_token")
    evidence_sha256 = payload.get("platform_health_payload_sha256")
    gate6_sha256 = payload.get("gate6_payload_sha256")
    if (
        operation != "admit"
        or not isinstance(admission_token, str)
        or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
        or not isinstance(evidence_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", evidence_sha256) is None
        or not isinstance(gate6_sha256, str)
        or re.fullmatch(r"[0-9a-f]{64}", gate6_sha256) is None
    ):
        raise RuntimeHostError("positive-capacity admission journal is invalid")
    candidate = Candidate(
        sha=sha,
        tree=tree,
        source=CANDIDATE_PARENT / sha / "repo",
    )
    phase = payload.get("phase")
    if phase in {"admission-authorized", "admission-open", "state-activated"}:
        _close_activation_admission(candidate, admission_token)
        (
            _adapter_candidates,
            current_evidence_sha256,
            current_gate6_sha256,
        ) = _positive_capacity_admission_gate(candidate)
        if current_evidence_sha256 != evidence_sha256 or current_gate6_sha256 != gate6_sha256:
            raise RuntimeHostError("positive-capacity admission evidence digest drifted")
        _activated_adapter_readback(candidate, require_zero=True)
        if _admission_fence(candidate) != admission_token:
            raise RuntimeHostError("positive-capacity admission fence drifted")
        _open_activation_admission(candidate, admission_token)
        if phase == "admission-authorized":
            _update_journal(path, payload, "admission-open")
        phase = "admission-open"
    if phase == "admission-open":
        state = _load_json(STATE_PATH, "runtime-host state")
        if (
            state.get("candidate_sha") != sha
            or state.get("candidate_tree") != tree
            or state.get("transaction_id") != admission_token
            or state.get("activation_status") not in {"bootstrap-active", "activated"}
        ):
            raise RuntimeHostError("positive-capacity admission state drifted")
        activated_state = dict(state)
        activated_state["activation_status"] = "activated"
        activated_state["admitted_at"] = datetime.now(UTC).isoformat()
        activated_state["platform_health_payload_sha256"] = evidence_sha256
        activated_state["gate6_payload_sha256"] = gate6_sha256
        activated_state["admission_transaction_id"] = payload["transaction_id"]
        _atomic_write(STATE_PATH, _canonical_json(activated_state), mode=0o600)
        _update_journal(path, payload, "state-activated")
        phase = "state-activated"
    if phase == "state-activated":
        report = check(candidate, activation_mode="activated")
        _update_journal(path, payload, "committed")
        ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
        _fsync_directory(INSTALLER_ROOT)
        return report
    raise RuntimeHostError("positive-capacity admission journal phase is invalid")


def admit(sha: str) -> dict[str, Any]:
    _require_live_host()
    _require_active_cohort()
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("admission requires the installed full candidate SHA")
    with _lock():
        _recover_orphan()
        _reject_orphan_stages()
        state = _load_json(STATE_PATH, "runtime-host state")
        tree = state.get("candidate_tree")
        admission_token = state.get("transaction_id")
        if (
            state.get("candidate_sha") != sha
            or not isinstance(tree, str)
            or SHA_RE.fullmatch(tree) is None
            or not isinstance(admission_token, str)
            or re.fullmatch(r"[0-9a-f]{32}", admission_token) is None
        ):
            raise RuntimeHostError("admission SHA does not match the installed candidate")
        candidate = Candidate(
            sha=sha,
            tree=tree,
            source=CANDIDATE_PARENT / sha / "repo",
        )
        if state.get("activation_status") == "activated":
            return check(candidate, activation_mode="activated")
        if state.get("activation_status") != "bootstrap-active":
            raise RuntimeHostError("positive-capacity admission requires bootstrap-active state")
        check(candidate, activation_mode="bootstrap-active")
        (
            _adapter_candidates,
            evidence_sha256,
            gate6_sha256,
        ) = _positive_capacity_admission_gate(candidate)
        journal_path, journal = _write_journal(
            candidate,
            operation="admit",
            admission_token=admission_token,
        )
        try:
            _update_journal(journal_path, journal, "admission-validating")
            _activated_adapter_readback(candidate, require_zero=True)
            if _admission_fence(candidate) != admission_token:
                raise RuntimeHostError("positive-capacity admission fence drifted")
            journal["platform_health_payload_sha256"] = evidence_sha256
            journal["gate6_payload_sha256"] = gate6_sha256
            _update_journal(journal_path, journal, "admission-authorized")
            return _resume_admission(journal_path, journal)
        except Exception:
            _restore_transaction(journal_path, journal)
            raise


def acceptance_plan(command: str, *, session_id: str) -> dict[str, Any]:
    if command not in {
        "acceptance-open",
        "acceptance-cohort",
        "acceptance-cancel",
        "acceptance-close",
    }:
        raise RuntimeHostError("acceptance operation is invalid")
    if re.fullmatch(r"[0-9a-f]{32}", session_id) is None:
        raise RuntimeHostError("acceptance session identity is invalid")
    return {
        "schema_version": 1,
        "artifact_type": f"shared-capacity-runtime-host-{command}-plan",
        "mutation_authorized": False,
        "session_id": session_id,
        "general_positive_capacity_admission_authorized": False,
        "sandboxes": list(_sandboxes()),
        **_registry_binding(),
        "fixed_pools": list(POOLS),
        "fixed_phases": list(ACCEPTANCE_PHASES),
        "fixed_phase_ttl_seconds": {
            phase: (
                ACCEPTANCE_TTL_CLEANUP_SECONDS
                if phase == "ttl_cleanup"
                else (
                    ACCEPTANCE_MIXED_NON_LOOM_TTL_SECONDS
                    if phase == "mixed_non_loom"
                    else ACCEPTANCE_DEFAULT_PHASE_TTL_SECONDS
                )
            )
            for phase in ACCEPTANCE_PHASES
        },
    }


def _acceptance_state_candidate(
    *,
    session_id: str,
    activation_status: str,
) -> tuple[Candidate, dict[str, Any], str]:
    if re.fullmatch(r"[0-9a-f]{32}", session_id) is None:
        raise RuntimeHostError("acceptance session identity is invalid")
    state = _load_json(STATE_PATH, "runtime-host state")
    _require_current_registry_binding(state)
    if (
        _cohort_from_runtime_manifest(state.get("runtime_manifest")).environments
        != _cohort().environments
    ):
        raise RuntimeHostError("acceptance runtime manifest drifted")
    sha = state.get("candidate_sha")
    tree = state.get("candidate_tree")
    token = state.get("transaction_id")
    if (
        state.get("activation_status") != activation_status
        or not isinstance(sha, str)
        or SHA_RE.fullmatch(sha) is None
        or not isinstance(tree, str)
        or SHA_RE.fullmatch(tree) is None
        or not isinstance(token, str)
        or re.fullmatch(r"[0-9a-f]{32}", token) is None
    ):
        raise RuntimeHostError("acceptance runtime state is invalid")
    if (
        activation_status == "acceptance-active"
        and state.get("acceptance_session_id") != session_id
    ):
        raise RuntimeHostError("acceptance session binding drifted")
    return (
        Candidate(sha=sha, tree=tree, source=CANDIDATE_PARENT / sha / "repo"),
        state,
        token,
    )


def _acceptance_operation_payload(
    *,
    operation: str,
    candidate: Candidate,
    admission_token: str,
    session_id: str,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": 1,
        "operation": operation,
        "candidate_sha": candidate.sha,
        "candidate_tree": candidate.tree,
        "admission_token": admission_token,
        "session_id": session_id,
        **_registry_binding(),
        "runtime_manifest": _runtime_manifest(),
    }
    if extra is not None:
        payload.update(extra)
    _validate_acceptance_operation(payload)
    return payload


def acceptance_open(session_id: str) -> dict[str, Any]:
    _require_live_host()
    _require_active_cohort()
    with _lock():
        _recover_orphan()
        candidate, _state, token = _acceptance_state_candidate(
            session_id=session_id,
            activation_status="bootstrap-active",
        )
        check(candidate, activation_mode="bootstrap-active")
        session = _acceptance_session_readback(candidate, session_id)
        required_open_index = LIVE_PHASES.index("multi_candidate_overlap") * len(_sandboxes())
        if (
            session.get("status") != "running"
            or not isinstance(session.get("next_phase_index"), int)
            or isinstance(session.get("next_phase_index"), bool)
            or session["next_phase_index"] != required_open_index
        ):
            raise RuntimeHostError("acceptance opening prefix is invalid")
        contract = _acceptance_contract(
            candidate,
            admission_token=token,
            session=session,
        )
        payload = _acceptance_operation_payload(
            operation="open",
            candidate=candidate,
            admission_token=token,
            session_id=session_id,
            extra={"contract": contract, "step": "prepared"},
        )
        _write_acceptance_operation(payload)
        report = _resume_acceptance_operation()
        if report is None:
            raise RuntimeHostError("acceptance open did not converge")
        if report.get("acceptance_open_rolled_back") is True:
            raise RuntimeHostError("acceptance open failed and rolled back safely")
        return report


def acceptance_cohort(session_id: str, phase: str) -> dict[str, Any]:
    _require_live_host()
    _require_active_cohort()
    with _lock():
        _recover_orphan()
        candidate, _state, token = _acceptance_state_candidate(
            session_id=session_id,
            activation_status="acceptance-active",
        )
        check(candidate, activation_mode="acceptance-active")
        session = _acceptance_session_readback(candidate, session_id)
        checkpoint_offset = _require_acceptance_phase_prefix(session, phase)
        existing = _run_acceptance_program(
            candidate,
            _ACCEPTANCE_COHORT_STATUS,
            token,
            session_id,
            phase,
        )
        if existing is not None and (
            not isinstance(existing, list) or len(existing) != len(_instances())
        ):
            raise RuntimeHostError("acceptance cohort readback is invalid")
        if checkpoint_offset > 0 and existing is None:
            raise RuntimeHostError(
                "acceptance cohort must exist before the first phase checkpoint",
            )
        mode = "replay" if existing is not None else "rotate"
        payload = _acceptance_operation_payload(
            operation="cohort",
            candidate=candidate,
            admission_token=token,
            session_id=session_id,
            extra={
                "phase": phase,
                "mode": mode,
                "step": "cohort-ready" if mode == "replay" else "prepared",
                "checkpoint_offset": checkpoint_offset,
            },
        )
        _write_acceptance_operation(payload)
        report = _resume_acceptance_operation()
        if report is None:
            raise RuntimeHostError("acceptance cohort did not converge")
        return report


def acceptance_cancel(
    session_id: str,
    *,
    phase: str,
    sandbox: str,
    pool: str,
) -> dict[str, Any]:
    _require_live_host()
    _require_active_cohort()
    with _lock():
        _recover_orphan()
        candidate, _state, token = _acceptance_state_candidate(
            session_id=session_id,
            activation_status="acceptance-active",
        )
        check(candidate, activation_mode="acceptance-active")
        session = _acceptance_session_readback(candidate, session_id)
        _require_acceptance_phase_prefix(session, phase)
        payload = _acceptance_operation_payload(
            operation="cancel",
            candidate=candidate,
            admission_token=token,
            session_id=session_id,
            extra={"phase": phase, "sandbox": sandbox, "pool": pool},
        )
        _write_acceptance_operation(payload)
        report = _resume_acceptance_operation()
        if report is None:
            raise RuntimeHostError("acceptance cancel did not converge")
        return report


def acceptance_close(session_id: str) -> dict[str, Any]:
    _require_live_host()
    _require_active_cohort()
    with _lock():
        _recover_orphan()
        candidate, _state, token = _acceptance_state_candidate(
            session_id=session_id,
            activation_status="acceptance-active",
        )
        check(candidate, activation_mode="acceptance-active")
        session = _acceptance_session_readback(candidate, session_id)
        _require_acceptance_phase_prefix(session, "final_drain")
        payload = _acceptance_operation_payload(
            operation="close",
            candidate=candidate,
            admission_token=token,
            session_id=session_id,
        )
        _write_acceptance_operation(payload)
        report = _resume_acceptance_operation()
        if report is None:
            raise RuntimeHostError("acceptance close did not converge")
        return report


def rollback_plan(sha: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("rollback requires the current full candidate SHA")
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-runtime-host-rollback-plan",
        "mutation_authorized": False,
        "candidate_sha": sha,
        **_registry_binding(),
        "instances": list(_instances()),
        "steps": [
            "persist-and-journal-root-owned-recovery-entrypoint",
            "journal-and-persistently-fence-new-broker-requests",
            "cancel-only-the-registry-exact-candidate-sandbox-pool-requests",
            "reconcile-and-read-back-registry-terminal-zero-handoffs",
            "read-back-registry-disabled-zero-control-plane-policies-and-worker-jobs",
            "stop-current-services-and-timers-after-external-capacity-is-zero",
            "restore-previous-configs-and-exact-units",
            "restore-previous-enabled-and-active-state",
            "reopen-the-exact-admission-fence-after-local-restore",
            "remove-journal-owned-stage-and-candidate-last",
        ],
    }


def rollback(sha: str) -> dict[str, Any]:
    _require_live_host()
    with _lock():
        _recover_orphan()
        state = _load_json(STATE_PATH, "runtime-host state")
        if state.get("candidate_sha") != sha:
            raise RuntimeHostError("rollback SHA does not match the active candidate")
        transaction_id = state.get("transaction_id")
        if not isinstance(transaction_id, str) or not re.fullmatch(
            r"[0-9a-f]{32}",
            transaction_id,
        ):
            raise RuntimeHostError("runtime-host rollback binding is invalid")
        path = JOURNAL_ROOT / f"{transaction_id}.json"
        payload = _load_json(path, "runtime-host rollback transaction")
        if payload.get("phase") != "committed" or payload.get("candidate_sha") != sha:
            raise RuntimeHostError("runtime-host rollback transaction is invalid")
        activation_status = state.get("activation_status")
        if activation_status == "acceptance-active":
            raise RuntimeHostError(
                "runtime-host acceptance must be closed before rollback",
            )
        if activation_status not in {"bootstrap-active", "activated", "installed"}:
            raise RuntimeHostError("runtime-host rollback activation state is invalid")
        _atomic_write(
            ACTIVE_JOURNAL_PATH,
            _canonical_json({"transaction_id": transaction_id}),
            mode=0o600,
        )
        if activation_status in {"bootstrap-active", "activated"}:
            _prepare_rollback_recovery(
                path,
                payload,
                _rollback_candidate(payload),
            )
            _complete_activated_rollback(path, payload)
        else:
            _restore_transaction(path, payload)
        return {
            "schema_version": 1,
            "status": "rolled-back",
            "candidate_sha": sha,
            "capacity_enabled_by_installer": False,
        }


def recover() -> dict[str, Any]:
    _require_live_host()
    with _lock():
        active = _active_journal()
        acceptance_active = (
            ACCEPTANCE_OPERATION_PATH.exists() or ACCEPTANCE_OPERATION_PATH.is_symlink()
        )
        if active is None and not acceptance_active:
            return {
                "schema_version": 1,
                "status": "no-active-transaction",
            }
        transaction_id = (
            active[1].get("transaction_id") if active is not None else "acceptance-operation"
        )
        _recover_orphan()
        return {
            "schema_version": 1,
            "status": "recovered",
            "transaction_id": transaction_id,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "install"):
        child = subparsers.add_parser(command)
        child.add_argument("--source-repo", type=Path, required=True)
        child.add_argument("--candidate-sha", required=True)
        if command == "install":
            child.add_argument("--execute", action="store_true")
    activate_parser = subparsers.add_parser("activate")
    activate_parser.add_argument("--candidate-sha", required=True)
    activate_parser.add_argument("--execute", action="store_true")
    admit_parser = subparsers.add_parser("admit")
    admit_parser.add_argument("--candidate-sha", required=True)
    admit_parser.add_argument("--execute", action="store_true")
    for command in ("acceptance-open", "acceptance-close"):
        acceptance_parser = subparsers.add_parser(command)
        acceptance_parser.add_argument("--session-id", required=True)
        acceptance_parser.add_argument("--execute", action="store_true")
    cohort_parser = subparsers.add_parser("acceptance-cohort")
    cohort_parser.add_argument("--session-id", required=True)
    cohort_parser.add_argument("--phase", choices=ACCEPTANCE_PHASES, required=True)
    cohort_parser.add_argument("--execute", action="store_true")
    cancel_parser = subparsers.add_parser("acceptance-cancel")
    cancel_parser.add_argument("--session-id", required=True)
    cancel_parser.add_argument("--phase", choices=("cancel_cleanup",), required=True)
    cancel_parser.add_argument("--sandbox", required=True)
    cancel_parser.add_argument("--pool", choices=POOLS, required=True)
    cancel_parser.add_argument("--execute", action="store_true")
    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--candidate-sha", required=True)
    check_parser.add_argument(
        "--mode",
        choices=("installed", "bootstrap-active", "acceptance-active", "activated"),
        default="installed",
    )
    check_parser.add_argument("--execute", action="store_true")
    rollback_parser = subparsers.add_parser("rollback")
    rollback_parser.add_argument("--candidate-sha", required=True)
    rollback_parser.add_argument("--execute", action="store_true")
    recover_parser = subparsers.add_parser("recover")
    recover_parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command in {"plan", "install"}:
            candidate = _candidate_identity(args.source_repo, args.candidate_sha)
            if args.command == "install" and args.execute:
                result = install(candidate)
            else:
                result = plan(candidate, args.command)
        elif args.command == "activate":
            result = (
                activate(args.candidate_sha)
                if args.execute
                else activation_plan(args.candidate_sha)
            )
        elif args.command == "admit":
            result = (
                admit(args.candidate_sha) if args.execute else admission_plan(args.candidate_sha)
            )
        elif args.command == "acceptance-open":
            result = (
                acceptance_open(args.session_id)
                if args.execute
                else acceptance_plan(args.command, session_id=args.session_id)
            )
        elif args.command == "acceptance-cohort":
            result = (
                acceptance_cohort(args.session_id, args.phase)
                if args.execute
                else acceptance_plan(args.command, session_id=args.session_id)
            )
        elif args.command == "acceptance-cancel":
            result = (
                acceptance_cancel(
                    args.session_id,
                    phase=args.phase,
                    sandbox=args.sandbox,
                    pool=args.pool,
                )
                if args.execute
                else acceptance_plan(args.command, session_id=args.session_id)
            )
        elif args.command == "acceptance-close":
            result = (
                acceptance_close(args.session_id)
                if args.execute
                else acceptance_plan(args.command, session_id=args.session_id)
            )
        elif args.command == "check":
            if not args.execute:
                result = {
                    "schema_version": 1,
                    "artifact_type": "shared-capacity-runtime-host-check-plan",
                    "mutation_authorized": False,
                    "candidate_sha": args.candidate_sha,
                }
            else:
                state = _load_json(STATE_PATH, "runtime-host state")
                candidate = Candidate(
                    sha=args.candidate_sha,
                    tree=str(state.get("candidate_tree", "")),
                    source=CANDIDATE_PARENT / args.candidate_sha / "repo",
                )
                result = check(candidate, activation_mode=args.mode)
        elif args.command == "rollback" and args.execute:
            result = rollback(args.candidate_sha)
        elif args.command == "recover" and args.execute:
            result = recover()
        elif args.command == "recover":
            result = {
                "schema_version": 1,
                "artifact_type": "shared-capacity-runtime-host-recovery-plan",
                "mutation_authorized": False,
                "entrypoint": str(RECOVERY_PROGRAM_PATH),
            }
        else:
            result = rollback_plan(args.candidate_sha)
        sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        return 0
    except (OSError, RuntimeHostError, ValueError):
        sys.stderr.write("error: shared-capacity runtime host failed safely\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
