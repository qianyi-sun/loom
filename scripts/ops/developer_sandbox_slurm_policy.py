#!/usr/bin/env python3
"""Plan, check, and converge developer-sandbox Slurm host policy.

Mutations are local-host only, require root, and are disabled unless
``--execute`` is present. Live restart transactions own a candidate-bound
Slurm drain until service readback or rollback finishes.
"""

from __future__ import annotations

import argparse
import base64
import ctypes
import errno
import fcntl
import hashlib
import importlib.util
import json
import os
import pwd
import re
import resource
import shlex
import socket
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
import types
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager, nullcontext
from copy import deepcopy
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast


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
_SNAPSHOT_RELATIVE_PATHS = (
    "etc/slurm/slurm.conf",
    "etc/slurm/cgroup.conf",
    "etc/docker/daemon.json",
    "usr/libexec/loom-slurm-job-cgroup-guard",
    "etc/loom/slurm-job-cgroup-guard.json",
    "etc/systemd/system/loom-slurm-job-cgroup-guard.service",
)
_SNAPSHOT_ROW_FIELDS = {
    "path",
    "present",
    "mode",
    "uid",
    "gid",
    "nlink",
    "size",
    "sha256",
}
_POLICY_JOURNAL_COMMON_FIELDS = {
    "schema_version",
    "operation",
    "cluster",
    "host",
    "slurm_node",
    "candidate_sha",
    "candidate_set_sha256",
    "candidate_bindings",
    "transaction_id",
    "candidate_set_generation",
    "candidate_set_convergence_id",
    "candidate_set_payload_sha256",
    "snapshot",
    "accounting_snapshot",
    "restart",
    "apply_accounting",
    "phase",
    "created_at",
    "updated_at",
}
_POLICY_JOURNAL_PHASES = {
    "prepared",
    "files_written",
    "accounting_applied",
    "services_reconfigured",
    "verified",
    "committed",
    "rolled_back",
    "rollback_failed",
    "recovery_failed",
}
_DRAIN_JOURNAL_FIELDS = {
    "schema_version",
    "kind",
    "cluster",
    "host",
    "slurm_node",
    "candidate_sha",
    "candidate_set_sha256",
    "candidate_bindings",
    "transaction_id",
    "candidate_set_generation",
    "candidate_set_convergence_id",
    "candidate_set_payload_sha256",
    "candidate_tree",
    "candidate_root",
    "profile_relative",
    "operation",
    "apply_accounting",
    "ownership_token",
    "ownership_reason",
    "owned",
    "prior_state",
    "prior_reason",
    "phase",
    "created_at",
    "updated_at",
}
_LEGACY_POLICY_JOURNAL_FIELDS = _POLICY_JOURNAL_COMMON_FIELDS - {
    "candidate_set_sha256",
    "candidate_bindings",
    "transaction_id",
    "candidate_set_generation",
    "candidate_set_convergence_id",
    "candidate_set_payload_sha256",
}
_LEGACY_DRAIN_JOURNAL_FIELDS = _DRAIN_JOURNAL_FIELDS - {
    "candidate_set_sha256",
    "candidate_bindings",
    "transaction_id",
    "candidate_set_generation",
    "candidate_set_convergence_id",
    "candidate_set_payload_sha256",
}
_DRAIN_JOURNAL_PHASES = {
    "prepared",
    "drained",
    "quiesced",
    "transacting",
    "released",
    "release_failed",
}
_SLURM_NODE_STATE_RE = re.compile(r"^[A-Z][A-Z0-9_+*~#@-]{0,127}$")
_DRAIN_TOKEN_RE = re.compile(r"^[0-9a-f]{64}$")
_DRAIN_REASON_RE = re.compile(
    r"^loom-sandbox-policy:[0-9a-f]{12}:[0-9a-f]{16}$",
)
_RECOVERY_CANDIDATE_ROOT_RE = re.compile(
    r"^/shared_work/loom/candidates/(?:"
    r"sandboxes/[a-z][a-z0-9-]{1,31}|"
    r"environments/denv-[a-z0-9-]{8,64}"
    r")/([0-9a-f]{40})$",
)
_RECOVERY_PROFILE_RELATIVE = {
    "trt-oldlab": "deploy/slurm/developer-sandboxes/oldlab.toml",
    "trt-gb10": "deploy/slurm/developer-sandboxes/gb10.toml",
}
_RECOVERY_POLICY_RELATIVE = "scripts/ops/developer_sandbox_slurm_policy.py"
_RECOVERY_CLUSTERS = tuple(_RECOVERY_PROFILE_RELATIVE)
_RESTART_QUIESCE_TIMEOUT_SECONDS = 30.0
_RESTART_QUIESCE_POLL_SECONDS = 1.0
_STATE_RELATIVE = Path("var/lib/loom-developer-sandbox-slurm-policy")
_GUARD_STATUS_RELATIVE = _STATE_RELATIVE / "guard-status.json"
_GUARD_STATUS_MAX_AGE = timedelta(seconds=30)
_GUARD_MAX_CLOCK_SKEW = timedelta(seconds=5)
_ALLOCATION_PROBE_RELATIVE = _STATE_RELATIVE / "allocation-probes"
_RUNTIME_PROOF_RELATIVE = _STATE_RELATIVE / "runtime-proofs"
_RUNTIME_PROOF_TRANSACTION_RELATIVE = _STATE_RELATIVE / "runtime-proof-transactions"
_RUNTIME_PROOF_HIGH_WATER_RELATIVE = _STATE_RELATIVE / "runtime-proof-high-water"
_ALLOCATION_PROBE_MAX_AGE = timedelta(minutes=15)
_ALLOCATION_POLL_SECONDS = 1.0
_ALLOCATION_TIMEOUT_SECONDS = 180.0
_ALLOCATION_PROOF_EXPIRY_MARGIN = timedelta(seconds=30)
_ALLOCATION_GENERATION_RE = re.compile(r"^[0-9a-f]{64}$")
_LEGACY_ALLOCATION_GENERATION_RE = re.compile(r"^[0-9a-f]{12}$")
_COMBINED_RUNTIME_ATTESTATION_ROOT = Path(
    "/var/lib/loom-shared-capacity/runtime-attestations",
)
_DOMAIN_RUNTIME_ATTESTATION_ROOT = Path("/var/lib/loom-developer-domain-attestations")
_FLEET_ATTESTATION_ROOT = Path("/var/lib/loom-developer-sandbox-links/attestations")
_NODE_TRANSPORT = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
_RUNTIME_PROOF_SOURCES = {
    "collector": ("oldlab-2", "trt-eai-oldlab-2"),
    "oldlab": ("oldlab-1", "trt-eai-oldlab-1"),
    "gb10": ("trt-gb10-1", "gx10-01c7"),
}
_RUNTIME_DOMAIN_HOSTS = {
    "oldlab": (
        "trt-eai-oldlab-1",
        "trt-eai-oldlab-2",
        "trt-eai-oldlab-3",
        "trt-eai-oldlab-4",
        "trt-eai-oldlab-5",
    ),
    "gb10": (
        "gx10-01c7",
        "gx10-0fca",
        "gx10-0f0d",
        "gx10-0d93",
        "gx10-1036",
        "gx10-1000",
        "gx10-0faf",
        "gx10-db22",
        "gx10-16f6",
        "gx10-0f82",
        "gx10-c38b",
        "gx10-e45f",
        "gx10-fc5d",
        "gx10-0a49",
        "gx10-0152",
    ),
}
_RUNTIME_FLEET_NODES = (
    "oldlab-1",
    "oldlab-2",
    "oldlab-3",
    "oldlab-4",
    "oldlab-5",
    "trt-gb10-1",
    "trt-gb10-2",
    "trt-gb10-3",
    "trt-gb10-4",
    "trt-gb10-5",
    "trt-gb10-6",
    "trt-gb10-7",
    "trt-gb10-8",
    "trt-gb10-9",
    "trt-gb10-10",
    "trt-gb10-11",
    "trt-gb10-12",
    "trt-gb10-13",
    "trt-gb10-14",
    "trt-gb10-15",
)
_RUNTIME_PROOF_FILE_NAMES = frozenset(
    {
        "combined.json",
        "fleet.json",
        "oldlab.json",
        "oldlab.sig",
        "oldlab.pub",
        "gb10.json",
        "gb10.sig",
        "gb10.pub",
        "manifest.json",
    },
)
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100
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
_BINDING_FIELDS = {
    "env_id",
    "resource_generation",
    "sandbox",
    "service_user",
    "slurm_qos",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
}
_REGISTRY_SET_FIELDS = {
    "schema_version",
    "kind",
    "candidate_set_sha256",
    "candidate_bindings",
    "generation",
    "convergence_id",
    "registry_generation",
    "registry_payload_sha256",
}
_REGISTRY_SET_KIND = "loom.developer-environment.slurm-candidate-set"


def _registry_contract() -> types.ModuleType:
    """Load the registry verifier from the candidate that owns this policy."""

    try:
        from scripts.ops import developer_environment_registry

        return developer_environment_registry
    except ModuleNotFoundError:
        path = Path(__file__).with_name("developer_environment_registry.py")
        spec = importlib.util.spec_from_file_location(
            "_loom_developer_environment_registry_for_slurm",
            path,
        )
        if spec is None or spec.loader is None:
            raise PolicyError(
                "developer environment registry verifier is unavailable",
            ) from None
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        try:
            spec.loader.exec_module(module)
        except (ImportError, OSError) as exc:
            raise PolicyError("developer environment registry verifier is unavailable") from exc
        return module


_REGISTRY = _registry_contract()
REGISTRY_SNAPSHOT_PATH = Path(_REGISTRY.SYSTEM_SNAPSHOT)
_CAPACITY_ROOT = Path("/var/lib/loom-developer-environment-capacity")
_CAPACITY_TRANSPORT_PROGRAM = Path(
    "/usr/local/libexec/loom-developer-sandbox-node-transport",
)
_CAPACITY_DOMAINS = {
    "oldlab": {
        "cluster": "trt-oldlab",
        "controller": "TRT-EAI-OLDLAB-1",
        "submit_host": "trt-EAI-OLDLAB-2",
        "authority_node": "oldlab-1",
        "infrastructure_nodes": tuple(f"oldlab-{index}" for index in range(1, 6)),
    },
    "gb10": {
        "cluster": "trt-gb10",
        "controller": "trt-gb10-1",
        "submit_host": "trt-gb10-1",
        "authority_node": "trt-gb10-1",
        "infrastructure_nodes": tuple(f"trt-gb10-{index}" for index in range(1, 16)),
    },
}
_INCREMENTAL_IDENTITY_FIELDS = {
    "schema_version",
    "kind",
    "env_id",
    "principal_id",
    "resource_generation",
    "service_user",
    "service_group",
    "uid",
    "gid",
    "slurm_account",
    "slurm_qos",
    "registry_generation",
    "registry_payload_sha256",
    "candidate_set_sha256",
    "revive_journal_sha256",
}
_INCREMENTAL_IDENTITY_V1_FIELDS = _INCREMENTAL_IDENTITY_FIELDS - {
    "principal_id",
    "revive_journal_sha256",
}
_REVIVE_ROOT = Path("/var/lib/loom-developer-environment-runtime/revive")
_ACCEPTANCE_PROBE_RELATIVE = _STATE_RELATIVE / "acceptance-probes"
_ACCEPTANCE_PROBE_ACTION = "developer-environment-acceptance-probe"
_ACCEPTANCE_PROBE_KIND = "loom.developer-environment.acceptance-probe-domain-request"
_ACCEPTANCE_PROBE_RECEIPT_KIND = "loom.developer-environment.acceptance-probe-domain-receipt"
_ACCEPTANCE_PROBE_CONTAINER_RESULT_KIND = (
    "loom.developer-environment.acceptance-probe-container-result"
)
_ACCEPTANCE_PROBE_REQUEST_FIELDS = {
    "schema_version",
    "kind",
    "action",
    "domain",
    "cluster",
    "submit_host",
    "controller",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "applied_resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "service_user",
    "slurm_account",
    "slurm_qos",
    "job_name",
    "time_limit_seconds",
    "health_services",
    "general_admission_authorized",
    "foreign_job_action",
    "idempotency_key",
    "payload_sha256",
}
_ACCEPTANCE_PROBE_SERVICES = ("control-plane", "gateway", "minio")
_ACCEPTANCE_PROBE_LABELS = frozenset(
    {
        "loom.sandbox",
        "loom.candidate_sha",
        "loom.slurm_job_id",
        "loom.compose_project",
        "loom.env_id",
        "loom.resource_generation",
        "loom.candidate_id",
        "loom.candidate_tree",
        "loom.registry_generation",
        "loom.registry_payload_sha256",
    },
)
_ACCEPTANCE_PROBE_COMPOSE_FILES = (
    Path("deploy/docker-compose.remote-worker.yml"),
    Path("deploy/docker-compose.remote-worker.sandbox-link.yml"),
    Path("deploy/docker-compose.remote-worker.cgroup-parent.yml"),
    Path("deploy/docker-compose.remote-worker.acceptance-probe.yml"),
)
_ACCEPTANCE_PROBE_PROGRAM = Path(
    "scripts/ops/developer_environment_acceptance_probe_container.py",
)
_ACCEPTANCE_CGROUP_PROGRAM = Path("src/loom_control_plane/slurm_job_cgroup.py")


@dataclass(frozen=True, slots=True)
class Profile:
    cluster: str
    controller: str
    submit_host: str
    infrastructure_nodes: tuple[str, ...]
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
    environment_bindings: Mapping[str, Mapping[str, Any]]


def _sandbox_account(profile: Profile, sandbox: str) -> str:
    matches = [
        account
        for account, binding in profile.environment_bindings.items()
        if binding.get("sandbox") == sandbox
    ]
    if len(matches) == 1:
        return matches[0]
    if profile.environment_bindings:
        raise PolicyError("sandbox is absent or ambiguous in the registry cohort")
    suffix = f"-{sandbox}"
    fallback = [
        account
        for user, account in zip(profile.users, profile.child_accounts, strict=True)
        if user.endswith(suffix)
    ]
    if len(fallback) != 1:
        raise PolicyError("sandbox is absent or ambiguous in the offline profile")
    return fallback[0]


def _sandbox_service_user(profile: Profile, sandbox: str) -> str:
    account = _sandbox_account(profile, sandbox)
    if profile.environment_bindings:
        return str(profile.environment_bindings[account]["service_user"])
    return profile.users[profile.child_accounts.index(account)]


def _sandbox_qos(profile: Profile, sandbox: str) -> str:
    account = _sandbox_account(profile, sandbox)
    if profile.environment_bindings:
        return str(profile.environment_bindings[account]["slurm_qos"])
    return profile.qos


def _account_qos(profile: Profile, account: str) -> str:
    if profile.environment_bindings:
        try:
            return str(profile.environment_bindings[account]["slurm_qos"])
        except KeyError as exc:
            raise PolicyError("Slurm account is absent from the registry cohort") from exc
    return profile.qos


def _profile_qoses(profile: Profile) -> tuple[str, ...]:
    if profile.environment_bindings:
        return tuple(
            sorted(
                {str(binding["slurm_qos"]) for binding in profile.environment_bindings.values()},
            ),
        )
    return (profile.qos,)


def _profile_with_bindings(
    profile: Profile,
    bindings: Mapping[str, Mapping[str, Any]],
) -> Profile:
    normalized = {account: dict(binding) for account, binding in bindings.items()}
    ordered_accounts = tuple(sorted(normalized))
    return replace(
        profile,
        child_accounts=ordered_accounts,
        users=tuple(str(normalized[account]["service_user"]) for account in ordered_accounts),
        environment_bindings=normalized,
    )


def _candidate_bindings(
    _profile: Profile | None,
    raw: Mapping[str, Any],
) -> dict[str, dict[str, Any]]:
    if not raw:
        raise PolicyError("candidate bindings must contain at least one active environment")
    normalized: dict[str, dict[str, Any]] = {}
    for account in sorted(raw):
        binding = raw.get(account)
        if (
            not isinstance(account, str)
            or _SAFE_NAME.fullmatch(account) is None
            or not isinstance(binding, Mapping)
            or set(binding) != _BINDING_FIELDS
            or _REGISTRY.ENV_ID_RE.fullmatch(str(binding.get("env_id"))) is None
            or type(binding.get("resource_generation")) is not int
            or int(binding["resource_generation"]) < 1
            or _REGISTRY.RUNTIME_ID_RE.fullmatch(str(binding.get("sandbox"))) is None
            or _REGISTRY.SAFE_NAME_RE.fullmatch(str(binding.get("service_user"))) is None
            or _REGISTRY.SAFE_NAME_RE.fullmatch(str(binding.get("slurm_qos"))) is None
            or _REGISTRY.CANDIDATE_ID_RE.fullmatch(str(binding.get("candidate_id"))) is None
            or _CANDIDATE_RE.fullmatch(str(binding.get("candidate_sha"))) is None
            or _CANDIDATE_RE.fullmatch(str(binding.get("candidate_tree"))) is None
        ):
            raise PolicyError("candidate account binding is invalid")
        normalized[account] = {
            "env_id": str(binding["env_id"]),
            "resource_generation": int(binding["resource_generation"]),
            "sandbox": str(binding["sandbox"]),
            "service_user": str(binding["service_user"]),
            "slurm_qos": str(binding["slurm_qos"]),
            "candidate_id": str(binding["candidate_id"]),
            "candidate_sha": str(binding["candidate_sha"]),
            "candidate_tree": str(binding["candidate_tree"]),
        }
    unique_fields = (
        "env_id",
        "sandbox",
        "service_user",
        "candidate_id",
    )
    if any(
        len({str(row[field]) for row in normalized.values()}) != len(normalized)
        for field in unique_fields
    ):
        raise PolicyError("candidate account bindings must be pairwise unique")
    return normalized


def _candidate_set_sha256(bindings: Mapping[str, Mapping[str, Any]]) -> str:
    return hashlib.sha256(
        json.dumps(bindings, sort_keys=True, separators=(",", ":")).encode("ascii"),
    ).hexdigest()


def _read_registry_snapshot(
    path: Path = REGISTRY_SNAPSHOT_PATH,
    *,
    require_root_ownership: bool,
) -> dict[str, Any]:
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        raw = os.pread(descriptor, 8 * 1024 * 1024 + 1, 0)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except OSError as exc:
        raise PolicyError("developer environment registry snapshot is unavailable") from exc
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
            item.st_size,
            item.st_mtime_ns,
        )
        for item in (lexical, opened, rebound, current)
    }
    expected_uid = 0 if require_root_ownership else os.geteuid()
    if (
        len(raw) > 8 * 1024 * 1024
        or len(identities) != 1
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != expected_uid
        or opened.st_gid != (0 if require_root_ownership else os.getegid())
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise PolicyError("developer environment registry snapshot is unsafe")
    try:
        payload = _REGISTRY.DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except Exception as exc:
        raise PolicyError("developer environment registry snapshot is invalid") from exc
    if not isinstance(payload, dict):
        raise PolicyError("developer environment registry snapshot is invalid")
    return payload


def _registry_candidate_bindings(
    snapshot: Mapping[str, Any],
    *,
    include_provisioning: bool,
    deployment_id: str | None = None,
    target_resource_generation: int | None = None,
    include_retiring: bool = False,
) -> tuple[dict[str, dict[str, Any]], tuple[dict[str, Any], ...]]:
    candidates = {str(candidate["candidate_id"]): candidate for candidate in snapshot["candidates"]}
    deployments = tuple(snapshot["deployments"])
    bindings: dict[str, dict[str, Any]] = {}
    provisioning: list[dict[str, Any]] = []
    target_env_id: str | None = None
    if deployment_id is not None:
        targets = [
            deployment for deployment in deployments if deployment["deployment_id"] == deployment_id
        ]
        if len(targets) != 1:
            raise PolicyError("registry deployment selector is invalid")
        target_env_id = str(targets[0]["env_id"])
    for environment in snapshot["environments"]:
        candidate_id: str | None = None
        phase: str | None = None
        resource_generation = int(environment["resource_generation"])
        state = environment["state"]
        deployment_matches = environment["env_id"] == target_env_id
        if state == "active":
            candidate_id = str(environment["current_candidate_id"])
            committed = [
                deployment
                for deployment in deployments
                if deployment["env_id"] == environment["env_id"]
                and deployment["principal_id"] == environment["principal_id"]
                and deployment["candidate_id"] == candidate_id
                and (not deployment_matches or deployment["deployment_id"] == deployment_id)
                and deployment["phase"] == "committed"
                and deployment.get("applied_resource_generation")
                == environment["resource_generation"]
                and deployment.get("expected_resource_generation", 0) + 1
                == deployment["applied_resource_generation"]
                and type(deployment.get("applied_registry_generation")) is int
                and 1 <= deployment["applied_registry_generation"] < snapshot["generation"]
                and _REGISTRY.DIGEST_RE.fullmatch(
                    str(deployment.get("applied_registry_payload_sha256")),
                )
                is not None
            ]
            if len(committed) != 1:
                raise PolicyError(
                    "registry active environment lacks one applied committed deployment",
                )
            latest = committed[0]
            if not _registry_finalization_exact(
                snapshot,
                environment,
                candidates.get(candidate_id),
                latest,
            ):
                raise PolicyError(
                    "registry committed deployment finalization binding is invalid",
                )
            phase = str(latest["phase"])
        elif state == "deploying" and include_provisioning and deployment_matches:
            in_flight = [
                deployment
                for deployment in deployments
                if deployment["env_id"] == environment["env_id"]
                and deployment["principal_id"] == environment["principal_id"]
                and (not deployment_matches or deployment["deployment_id"] == deployment_id)
                and deployment["expected_resource_generation"] == environment["resource_generation"]
                and deployment["phase"] not in {"committed", "failed"}
            ]
            if len(in_flight) != 1:
                raise PolicyError("registry provisioning environment is ambiguous")
            candidate_id = str(in_flight[0]["candidate_id"])
            phase = str(in_flight[0]["phase"])
            selected_generation = (
                int(environment["resource_generation"])
                if target_resource_generation is None
                else target_resource_generation
            )
            allowed_generations = {int(environment["resource_generation"])}
            if phase == "verified":
                allowed_generations.add(int(environment["resource_generation"]) + 1)
            if selected_generation not in allowed_generations:
                raise PolicyError("registry provisioning generation is invalid")
            if selected_generation != environment["resource_generation"] and (
                in_flight[0].get("applied_resource_generation") != selected_generation
                or type(in_flight[0].get("applied_registry_generation")) is not int
                or not 1 <= in_flight[0]["applied_registry_generation"] < snapshot["generation"]
                or _REGISTRY.DIGEST_RE.fullmatch(
                    str(in_flight[0].get("applied_registry_payload_sha256")),
                )
                is None
            ):
                raise PolicyError("registry provisioning applied binding is invalid")
            resource_generation = selected_generation
            provisioning.append(
                {
                    "env_id": environment["env_id"],
                    "deployment_id": in_flight[0]["deployment_id"],
                    "phase": phase,
                },
            )
        elif state == "quarantined" and include_retiring and deployment_matches:
            committed = [
                deployment
                for deployment in deployments
                if deployment["deployment_id"] == deployment_id
                and deployment["env_id"] == environment["env_id"]
                and deployment["principal_id"] == environment["principal_id"]
                and deployment["candidate_id"] == environment["current_candidate_id"]
                and deployment["phase"] == "committed"
                and deployment.get("applied_resource_generation")
                == environment["resource_generation"]
                and deployment.get("expected_resource_generation", 0) + 1
                == deployment["applied_resource_generation"]
            ]
            if len(committed) != 1:
                raise PolicyError("registry retiring environment binding is invalid")
            candidate_id = str(committed[0]["candidate_id"])
            phase = "committed"
        else:
            continue
        candidate = candidates.get(candidate_id)
        if (
            candidate is None
            or candidate["env_id"] != environment["env_id"]
            or candidate["principal_id"] != environment["principal_id"]
            or phase is None
        ):
            raise PolicyError("registry candidate ownership is invalid")
        selected_deployments = [
            deployment
            for deployment in deployments
            if deployment["env_id"] == environment["env_id"]
            and deployment["principal_id"] == environment["principal_id"]
            and deployment["candidate_id"] == candidate_id
            and deployment["phase"] == phase
            and (
                phase != "committed"
                or deployment.get("applied_resource_generation") == resource_generation
            )
            and (not deployment_matches or deployment["deployment_id"] == deployment_id)
        ]
        if phase == "committed" and (
            len(selected_deployments) != 1
            or not _registry_finalization_exact(
                snapshot,
                environment,
                candidate,
                selected_deployments[0],
            )
        ):
            raise PolicyError(
                "registry committed deployment finalization binding is invalid",
            )
        account = str(environment["slurm_account"])
        if account in bindings:
            raise PolicyError("registry Slurm account is duplicated")
        bindings[account] = {
            "env_id": str(environment["env_id"]),
            "resource_generation": resource_generation,
            "sandbox": str(environment["runtime_id"]),
            # The Slurm batch identity, not the host service identity, owns jobs.
            "service_user": str(environment["slurm_user"]),
            "slurm_qos": str(environment["slurm_qos"]),
            "candidate_id": str(candidate["candidate_id"]),
            "candidate_sha": str(candidate["candidate_sha"]),
            "candidate_tree": str(candidate["candidate_tree"]),
        }
    return _candidate_bindings(None, bindings), tuple(
        sorted(provisioning, key=lambda row: str(row["env_id"])),
    )


def _registry_finalization_exact(
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
    candidate: object,
    deployment: Mapping[str, Any],
) -> bool:
    if not isinstance(candidate, Mapping):
        return False
    digest = deployment.get("finalization_payload_sha256")
    records = snapshot.get("deployment_finalizations")
    if _REGISTRY.DIGEST_RE.fullmatch(str(digest)) is None or not isinstance(records, list):
        return False
    matched = [
        record
        for record in records
        if isinstance(record, Mapping)
        and record.get("deployment_id") == deployment["deployment_id"]
        and record.get("payload_sha256") == digest
    ]
    if len(matched) != 1:
        return False
    record = matched[0]
    fields = {
        "deployment_id",
        "env_id",
        "principal_id",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
        "applied_resource_generation",
        "applied_registry_generation",
        "applied_registry_payload_sha256",
        "capacity_finalize_receipt_sha256",
        "capacity_finalize_check_receipt_sha256",
        "runtime_reconcile_receipt_sha256",
        "runtime_prepare_check_receipt_sha256",
        "acceptance_probe_receipt_sha256",
        "created_at",
        "payload_sha256",
    }
    unsigned = {field: value for field, value in record.items() if field != "payload_sha256"}
    return (
        set(record) == fields
        and record.get("payload_sha256")
        == hashlib.sha256(_canonical_json_bytes(unsigned) + b"\n").hexdigest()
        and record.get("env_id") == environment["env_id"]
        and record.get("principal_id") == environment["principal_id"]
        and record.get("candidate_id") == candidate["candidate_id"]
        and record.get("candidate_sha") == candidate["candidate_sha"]
        and record.get("candidate_tree") == candidate["candidate_tree"]
        and record.get("applied_resource_generation")
        == deployment.get("applied_resource_generation")
        and record.get("applied_registry_generation")
        == deployment.get("applied_registry_generation")
        and record.get("applied_registry_payload_sha256")
        == deployment.get("applied_registry_payload_sha256")
        and all(
            _REGISTRY.DIGEST_RE.fullmatch(str(record.get(field))) is not None
            for field in (
                "capacity_finalize_receipt_sha256",
                "capacity_finalize_check_receipt_sha256",
                "runtime_reconcile_receipt_sha256",
                "runtime_prepare_check_receipt_sha256",
                "acceptance_probe_receipt_sha256",
            )
        )
        and isinstance(record.get("created_at"), str)
    )


def slurm_candidate_set_from_snapshot(
    snapshot: Mapping[str, Any],
    *,
    generation: int | None = None,
    convergence_id: str | None = None,
    include_provisioning: bool = True,
    deployment_id: str | None = None,
    target_resource_generation: int | None = None,
    include_retiring: bool = False,
) -> dict[str, Any]:
    """Produce the exact registry-bound payload consumed by node authorities."""

    bindings, _provisioning = _registry_candidate_bindings(
        snapshot,
        include_provisioning=include_provisioning,
        deployment_id=deployment_id,
        target_resource_generation=target_resource_generation,
        include_retiring=include_retiring,
    )
    registry_generation = snapshot.get("generation")
    registry_digest = snapshot.get("payload_sha256")
    if (
        type(registry_generation) is not int
        or registry_generation < 1
        or _REGISTRY.DIGEST_RE.fullmatch(str(registry_digest)) is None
    ):
        raise PolicyError("registry snapshot identity is invalid")
    policy_generation = registry_generation if generation is None else generation
    if type(policy_generation) is not int or policy_generation < 1:
        raise PolicyError("Slurm candidate-set generation is invalid")
    binding_digest = _candidate_set_sha256(bindings)
    convergence = (
        convergence_id
        or hashlib.sha256(
            (
                f"{registry_generation}:{registry_digest}:{policy_generation}:{binding_digest}"
            ).encode("ascii"),
        ).hexdigest()
    )
    if _REGISTRY.DIGEST_RE.fullmatch(convergence) is None:
        raise PolicyError("Slurm candidate-set convergence identity is invalid")
    return {
        "schema_version": 2,
        "kind": "loom.developer-sandbox.slurm-candidate-set",
        "candidate_set_sha256": binding_digest,
        "candidate_bindings": bindings,
        "generation": policy_generation,
        "convergence_id": convergence,
        "registry_generation": registry_generation,
        "registry_payload_sha256": str(registry_digest),
    }


def load_slurm_candidate_set(
    path: Path = REGISTRY_SNAPSHOT_PATH,
    *,
    require_root_ownership: bool = True,
    generation: int | None = None,
    convergence_id: str | None = None,
) -> dict[str, Any]:
    snapshot = _read_registry_snapshot(
        path,
        require_root_ownership=require_root_ownership,
    )
    return slurm_candidate_set_from_snapshot(
        snapshot,
        generation=generation,
        convergence_id=convergence_id,
    )


def _require_current_registry_bindings(
    bindings: Mapping[str, Mapping[str, Any]],
    *,
    path: Path = REGISTRY_SNAPSHOT_PATH,
) -> dict[str, Any]:
    snapshot = _read_registry_snapshot(path, require_root_ownership=True)
    expected, _provisioning = _registry_candidate_bindings(
        snapshot,
        include_provisioning=True,
    )
    if dict(bindings) != expected:
        raise PolicyError("Slurm candidate bindings are stale against the current registry")
    return {
        "registry_generation": snapshot["generation"],
        "registry_payload_sha256": snapshot["payload_sha256"],
    }


def _transaction_identity(
    *,
    transaction_id: str | None,
    generation: int | None,
    convergence_id: str | None,
    payload_sha256: str | None,
    required: bool,
) -> dict[str, str | int]:
    values = (transaction_id, generation, convergence_id, payload_sha256)
    if not required and all(value is None for value in values):
        return {
            "transaction_id": "0" * 64,
            "candidate_set_generation": 1,
            "candidate_set_convergence_id": "0" * 64,
            "candidate_set_payload_sha256": "0" * 64,
        }
    if (
        re.fullmatch(r"[0-9a-f]{64}", str(transaction_id)) is None
        or type(generation) is not int
        or generation < 1
        or re.fullmatch(r"[0-9a-f]{64}", str(convergence_id)) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(payload_sha256)) is None
    ):
        raise PolicyError("Slurm transaction identity is invalid")
    return {
        "transaction_id": str(transaction_id),
        "candidate_set_generation": generation,
        "candidate_set_convergence_id": str(convergence_id),
        "candidate_set_payload_sha256": str(payload_sha256),
    }


def _offline_candidate_bindings(
    profile: Profile,
    candidate_sha: str,
) -> dict[str, dict[str, Any]]:
    """Provide closed schema-v2 fixtures only for non-live planning roots."""

    rows: dict[str, dict[str, Any]] = {}
    for index, (service_user, account) in enumerate(
        zip(profile.users, profile.child_accounts, strict=True),
    ):
        sandbox = service_user.removeprefix("loom-sandbox-")
        sha = (
            candidate_sha
            if index == 0
            else hashlib.sha256(f"{candidate_sha}:{sandbox}".encode("ascii")).hexdigest()[:40]
        )
        rows[account] = {
            "env_id": f"denv-offline-{index:08d}",
            "resource_generation": 1,
            "sandbox": sandbox,
            "service_user": service_user,
            "slurm_qos": profile.qos,
            "candidate_id": f"cand-{sha}",
            "candidate_sha": sha,
            "candidate_tree": candidate_sha,
        }
    return _candidate_bindings(profile, rows)


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
    required_top_level = {
        "schema_version",
        "cluster",
        "controller",
        "submit_host",
        "infrastructure_nodes",
        "allowed_nodes",
        "host_aliases",
        "capacity",
        "slurm",
        "cgroup",
        "docker",
        "accounting",
    }
    if set(raw) != required_top_level:
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
    infrastructure_nodes = _strings(raw.get("infrastructure_nodes"), "infrastructure_nodes")
    if len(set(infrastructure_nodes)) != len(infrastructure_nodes):
        raise PolicyError("infrastructure_nodes must be distinct")
    if len(set(allowed_nodes)) != len(allowed_nodes):
        raise PolicyError("allowed_nodes must be distinct")
    if not set(allowed_nodes).issubset(infrastructure_nodes):
        raise PolicyError("allowed_nodes must be a subset of infrastructure_nodes")
    host_aliases_raw = _table(raw, "host_aliases")
    host_aliases = {
        str(key): str(value).lower()
        for key, value in host_aliases_raw.items()
        if isinstance(key, str) and isinstance(value, str)
    }
    if set(host_aliases) != set(infrastructure_nodes):
        raise PolicyError("host_aliases must map every infrastructure Slurm node")
    if len(set(host_aliases.values())) != len(host_aliases):
        raise PolicyError("host_aliases canonical hostnames must be distinct")
    users = _strings(accounting.get("users"), "accounting.users")
    child_accounts = _strings(
        accounting.get("child_accounts"),
        "accounting.child_accounts",
    )
    if len(users) != len(child_accounts):
        raise PolicyError("accounting users and child accounts must have equal length")
    if any(not user.startswith("loom-") for user in users):
        raise PolicyError("accounting users must be non-login Loom service users")
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
        infrastructure_nodes=infrastructure_nodes,
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
        environment_bindings={},
    )


def _worker_capacity_contract(
    profile: Profile,
    candidate_root: Path,
) -> tuple[str, int]:
    """Load the pool/concurrency binding from the exact candidate policy."""
    domain_by_cluster = {"trt-oldlab": "oldlab", "trt-gb10": "gb10"}
    domain = domain_by_cluster.get(profile.cluster)
    if domain is None:
        raise PolicyError("worker capacity contract cluster is invalid")
    runtime_path = candidate_root / "deploy/developer-sandboxes/runtime-domains.toml"
    expected_source = f"deploy/developer-sandboxes/shared-capacity-policies/{domain}.toml"
    capacity_path = candidate_root / expected_source
    payloads: list[dict[str, Any]] = []
    for path, label in (
        (runtime_path, "runtime-domain contract"),
        (capacity_path, "capacity policy contract"),
    ):
        try:
            metadata = path.lstat()
            if (
                stat.S_ISLNK(metadata.st_mode)
                or not stat.S_ISREG(metadata.st_mode)
                or metadata.st_size > 1024 * 1024
            ):
                raise PolicyError(f"{label} metadata is invalid")
            payload = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, tomllib.TOMLDecodeError) as exc:
            raise PolicyError(f"{label} is unavailable or invalid") from exc
        payloads.append(payload)
    runtime, capacity = payloads
    domains = runtime.get("domains")
    domain_config = domains.get(domain) if isinstance(domains, dict) else None
    policy = capacity.get("policy")
    actuator = policy.get("actuator_config") if isinstance(policy, dict) else None
    if not isinstance(domain_config, dict) or not isinstance(actuator, dict):
        raise PolicyError("worker capacity contract tables are invalid")
    concurrency = actuator.get("requested_concurrency")
    allowed_nodes = actuator.get("allowed_nodes")
    expected_env = f"${{RUNTIME_ROOT}}/${{CANDIDATE_SHA}}/worker-{domain}.env"
    if (
        runtime.get("schema_version") != 1
        or domain_config.get("worker_env_name") != f"worker-{domain}.env"
        or domain_config.get("worker_pool_name") != domain
        or domain_config.get("capacity_policy_source") != expected_source
        or domain_config.get("worker_max_concurrent") != concurrency
        or capacity.get("schema_version") != 1
        or capacity.get("pool_name") != domain
        or type(concurrency) is not int
        or concurrency < 1
        or not isinstance(allowed_nodes, list)
        or not all(isinstance(node, str) for node in allowed_nodes)
        or tuple(node.lower() for node in allowed_nodes)
        != tuple(node.lower() for node in profile.allowed_nodes)
        or actuator.get("env_file") != expected_env
        or actuator.get("repo_dir") != "${CANDIDATE_ROOT}/${CANDIDATE_SHA}"
        or actuator.get("candidate_sha") != "${CANDIDATE_SHA}"
        or actuator.get("slurm_account") != "${SLURM_ACCOUNT}"
        or actuator.get("qos_normal") != "${SLURM_QOS}"
        or capacity.get("slot_budget") != profile.slot_budget
        or capacity.get("job_pids_max") != profile.job_pids_max
    ):
        raise PolicyError("worker capacity contract binding drifted")
    return domain, concurrency


def _require_worker_capacity_assertion(
    profile: Profile,
    candidate_root: Path,
    *,
    expected_pool: str,
    expected_concurrency: int,
) -> tuple[str, int]:
    pool, concurrency = _worker_capacity_contract(profile, candidate_root)
    if expected_pool != pool or expected_concurrency != concurrency:
        raise PolicyError(
            "operator pool/concurrency assertion differs from the checked-in capacity policy",
        )
    return pool, concurrency


def _slurm_value(value: str | int) -> str:
    return str(value)


def _allowed_host_aliases(profile: Profile) -> dict[str, str]:
    return {node: profile.host_aliases[node] for node in profile.allowed_nodes}


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


def _read_exact_env_values(
    path: Path,
    *,
    expected_inode: int,
    expected_sha256: str,
) -> dict[str, str]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PolicyError("allocation-side worker env is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode) or opened.st_ino != expected_inode:
            raise PolicyError("allocation-side worker env inode drifted")
        content = bytearray()
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > 1024 * 1024:
                raise PolicyError("allocation-side worker env is too large")
        after = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise PolicyError("allocation-side worker env changed while it was read")
    finally:
        os.close(descriptor)
    payload = bytes(content)
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise PolicyError("allocation-side worker env content drifted")
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise PolicyError("allocation-side worker env must be UTF-8") from exc
    values: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or _ENV_KEY_RE.fullmatch(key) is None or not value or key in values:
            raise PolicyError("allocation-side worker env schema drifted")
        values[key] = value
    return values


def allocation_node_check(
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    expected_tree: str,
    expected_env_inode: int,
    expected_env_sha256: str,
    batch_uid: int,
    batch_gid: int,
    expected_host: str,
    expected_pool: str,
    expected_concurrency: int,
    result_path: Path,
) -> dict[str, Any]:
    """Run the secret-safe compute-side portion of the #827 matrix."""
    account = _sandbox_account(profile, sandbox)
    service_user = _sandbox_service_user(profile, sandbox)
    try:
        sandbox_identity = pwd.getpwnam(service_user)
    except KeyError as exc:
        raise PolicyError("allocation-side sandbox user is unavailable") from exc
    if (sandbox_identity.pw_uid, sandbox_identity.pw_gid) != (batch_uid, batch_gid):
        raise PolicyError("allocation-side sandbox UID/GID binding drifted")
    if os.geteuid() != batch_uid or os.getegid() != batch_gid:
        raise PolicyError("allocation-side numeric batch identity drifted")
    host = _canonical_host()
    if host != expected_host.lower():
        raise PolicyError("allocation-side node identity drifted")
    binding = strict_candidate_binding(
        candidate_root,
        worker_env,
        candidate_sha=candidate_sha,
        expected_batch_uid=batch_uid,
        expected_batch_gid=batch_gid,
    )
    if binding["repository"]["candidate_tree"] != expected_tree:
        raise PolicyError("allocation-side candidate tree drifted")
    pool, concurrency = _require_worker_capacity_assertion(
        profile,
        candidate_root,
        expected_pool=expected_pool,
        expected_concurrency=expected_concurrency,
    )
    env_binding = binding["worker_env"]
    if env_binding["inode"] != expected_env_inode or env_binding["sha256"] != expected_env_sha256:
        raise PolicyError("allocation-side worker env binding drifted")
    values = _read_exact_env_values(
        worker_env,
        expected_inode=expected_env_inode,
        expected_sha256=expected_env_sha256,
    )
    if (
        values.get("LOOM_WORKER_CANDIDATE_SHA") != candidate_sha
        or values.get("LOOM_WORKER_POOL_NAME") != pool
        or values.get("LOOM_WORKER_MAX_CONCURRENT") != str(concurrency)
    ):
        raise PolicyError("allocation-side worker env effective values drifted")
    docker_driver = _run(("docker", "info", "--format", "{{.CgroupDriver}}"), timeout=30).strip()
    if docker_driver != profile.docker_cgroup_driver:
        raise PolicyError("allocation-side Docker cgroup driver drifted")
    job_id = os.environ.get("SLURM_JOB_ID", "")
    if re.fullmatch(r"[1-9][0-9]*", job_id) is None:
        raise PolicyError("allocation-side Slurm job identity is unavailable")
    cgroup_program = candidate_root / "src/loom_control_plane/slurm_job_cgroup.py"
    try:
        cgroup_completed = subprocess.run(
            (
                "/usr/bin/python3",
                "-I",
                "-B",
                str(cgroup_program),
                "--job-id",
                job_id,
                "--pids-max",
                str(profile.job_pids_max),
                "--wait-seconds",
                "30",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=45,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError("allocation-side cgroup guard validation failed safely") from exc
    cgroup_parent = cgroup_completed.stdout.strip()
    if cgroup_completed.returncode or not cgroup_parent.startswith("/"):
        raise PolicyError("allocation-side cgroup guard validation failed")
    base_compose = candidate_root / "deploy/docker-compose.remote-worker.yml"
    sandbox_compose = candidate_root / "deploy/docker-compose.remote-worker.sandbox-link.yml"
    cgroup_compose = candidate_root / "deploy/docker-compose.remote-worker.cgroup-parent.yml"
    compose_environment = {
        **os.environ,
        "LOOM_WORKER_CGROUP_PARENT": cgroup_parent,
        "LOOM_WORKER_SLURM_JOB_ID": job_id,
        "LOOM_WORKER_COMPOSE_PROJECT": "loom-acceptance",
        "LOOM_WORKER_RESTART_POLICY": "no",
    }
    try:
        completed = subprocess.run(
            (
                "docker",
                "compose",
                "--env-file",
                str(worker_env),
                "-f",
                str(base_compose),
                "-f",
                str(sandbox_compose),
                "-f",
                str(cgroup_compose),
                "config",
                "--format",
                "json",
            ),
            cwd=candidate_root,
            env=compose_environment,
            check=False,
            capture_output=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError("allocation-side Docker Compose validation failed safely") from exc
    if completed.returncode:
        raise PolicyError("allocation-side Docker Compose validation failed")
    try:
        rendered = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("allocation-side Docker Compose output is invalid") from exc
    services = rendered.get("services") if isinstance(rendered, dict) else None
    if (
        not isinstance(services, dict)
        or not isinstance(services.get("worker"), dict)
        or not isinstance(services.get("sandbox-link"), dict)
        or services["worker"].get("cgroup_parent") != cgroup_parent
        or services["sandbox-link"].get("cgroup_parent") != cgroup_parent
    ):
        raise PolicyError("allocation-side Docker Compose cgroup binding drifted")
    result = {
        "schema_version": 2,
        "sandbox": sandbox,
        "account": account,
        "candidate_sha": candidate_sha,
        "candidate_tree": expected_tree,
        "host": host,
        "env_device": env_binding["device"],
        "env_inode": expected_env_inode,
        "env_sha256": expected_env_sha256,
        "pool": expected_pool,
        "concurrency": expected_concurrency,
        "docker_cgroup_driver": docker_driver,
        "job_id": job_id,
        "cgroup_parent": cgroup_parent,
        "cgroup_guard_verified": True,
        "compose_verified": True,
    }
    _prepare_private_directory(
        result_path.parent,
        enforce_root_ownership=False,
        create=False,
    )
    _write_allocation_state(
        result_path,
        result,
        enforce_root_ownership=False,
    )
    return result


def desired_files(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str | None = None,
    candidate_bindings: Mapping[str, Any] | None = None,
) -> dict[Path, str]:
    candidate = candidate_sha or source_candidate_sha()
    if _CANDIDATE_RE.fullmatch(candidate) is None:
        raise PolicyError("candidate SHA must be an exact lowercase Git SHA")
    if candidate_bindings is None:
        if root == Path("/"):
            raise PolicyError("live Slurm policy requires the complete candidate set")
        bindings = _offline_candidate_bindings(profile, candidate)
    else:
        bindings = _candidate_bindings(profile, candidate_bindings)
    profile = _profile_with_bindings(profile, bindings)
    candidate_set_sha256 = _candidate_set_sha256(bindings)
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
                    "schema_version": 2,
                    "cluster": profile.cluster,
                    "controller": profile.controller,
                    "submit_host": profile.submit_host,
                    "allowed_nodes": sorted(
                        {
                            *(node.lower() for node in profile.allowed_nodes),
                            *(profile.host_aliases[node] for node in profile.allowed_nodes),
                        },
                    ),
                    "candidate_bindings": bindings,
                    "candidate_set_sha256": candidate_set_sha256,
                    "pids_max": profile.job_pids_max,
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
    candidate_bindings: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = candidate_sha or source_candidate_sha()
    files = desired_files(
        root,
        profile,
        candidate_sha=candidate,
        candidate_bindings=candidate_bindings,
    )
    bindings = (
        _offline_candidate_bindings(profile, candidate)
        if candidate_bindings is None
        else _candidate_bindings(profile, candidate_bindings)
    )
    profile = _profile_with_bindings(profile, bindings)
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
        "schema_version": 2,
        "artifact_type": "developer-sandbox-slurm-policy-plan",
        "cluster": profile.cluster,
        "candidate_sha": candidate,
        "candidate_set_sha256": _candidate_set_sha256(bindings),
        "candidate_bindings": bindings,
        "infrastructure_nodes": list(profile.infrastructure_nodes),
        "allowed_nodes": list(profile.allowed_nodes),
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
    commands: list[list[str]] = []
    for qos in _profile_qoses(profile):
        commands.extend(
            [
                ["sacctmgr", "-i", "add", "qos", qos],
                [
                    "sacctmgr",
                    "-i",
                    "modify",
                    "qos",
                    "where",
                    f"name={qos}",
                    "set",
                    f"Priority={profile.qos_priority}",
                    f"MaxWall={profile.qos_max_wall}",
                    f"MaxJobsPerUser={profile.qos_max_jobs_per_user}",
                    f"MaxSubmitJobsPerUser={profile.qos_max_submit_jobs_per_user}",
                ],
            ],
        )
    commands.extend(
        [
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
        ],
    )
    for user, account in zip(profile.users, profile.child_accounts, strict=True):
        qos = _account_qos(profile, account)
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
                    f"QOS={qos}",
                    f"DefaultQOS={qos}",
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


def _run_bounded_stdout(
    argv: Sequence[str],
    *,
    input_bytes: bytes | None = None,
    timeout: float,
    max_bytes: int,
) -> bytes:
    def limit_output() -> None:
        resource.setrlimit(resource.RLIMIT_FSIZE, (max_bytes, max_bytes))

    try:
        with tempfile.TemporaryFile() as output:
            completed = subprocess.run(
                list(argv),
                check=False,
                input=input_bytes,
                stdout=output,
                stderr=subprocess.DEVNULL,
                timeout=timeout,
                preexec_fn=limit_output,
                env={
                    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                    "LANG": "C.UTF-8",
                    "LC_ALL": "C.UTF-8",
                },
            )
            if completed.returncode:
                raise PolicyError(
                    f"{argv[0]} failed safely with exit code {completed.returncode}",
                )
            size = os.fstat(output.fileno()).st_size
            if size > max_bytes:
                raise PolicyError(f"{argv[0]} returned oversized output")
            output.seek(0)
            return output.read(max_bytes + 1)
    except PolicyError:
        raise
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError(f"{argv[0]} failed safely before completion") from exc


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
    relative_paths = tuple(path.relative_to(root).as_posix() for path in files)
    if relative_paths != _SNAPSHOT_RELATIVE_PATHS:
        raise PolicyError("Slurm policy snapshot input set is not closed")
    manifest: dict[str, Any] = {"schema_version": 1, "files": []}
    for path in files:
        relative = path.relative_to(root)
        target = snapshot / relative
        snapshot_parent = snapshot
        for component in relative.parent.parts:
            snapshot_parent /= component
            _prepare_private_directory(
                snapshot_parent,
                enforce_root_ownership=enforce_root_ownership,
                create=True,
            )
        if path.exists() or path.is_symlink():
            content, metadata = _read_bound_regular_file(
                path,
                expected_uid=0 if enforce_root_ownership else os.geteuid(),
                expected_gid=0 if enforce_root_ownership else os.getegid(),
                expected_mode=None,
                description="live Slurm policy snapshot input",
            )
            mode = stat.S_IMODE(metadata.st_mode)
            _atomic_write(target, content.decode("utf-8"), mode=0o600)
            manifest["files"].append(
                {
                    "path": relative.as_posix(),
                    "present": True,
                    "mode": mode,
                    "uid": metadata.st_uid,
                    "gid": metadata.st_gid,
                    "nlink": metadata.st_nlink,
                    "size": len(content),
                    "sha256": _sha256(content),
                },
            )
        else:
            manifest["files"].append(
                {
                    "path": relative.as_posix(),
                    "present": False,
                    "mode": None,
                    "uid": None,
                    "gid": None,
                    "nlink": None,
                    "size": None,
                    "sha256": None,
                },
            )
    _atomic_write(
        snapshot / "manifest.json",
        json.dumps(manifest, sort_keys=True) + "\n",
        mode=0o600,
    )
    _fsync_directory(snapshot)
    _snapshot_manifest_rows(root, snapshot)
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


def _load_journal(
    path: Path,
    *,
    allowed_schema_versions: frozenset[int] = frozenset({1, 2}),
) -> dict[str, Any] | None:
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
        raw = b"".join(chunks)
        payload = json.loads(raw.decode("utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("durable Slurm policy journal is unreadable") from exc
    finally:
        os.close(descriptor)
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") not in allowed_schema_versions
        or raw
        != (
            json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
        ).encode("ascii")
    ):
        raise PolicyError("durable Slurm policy journal is unsafe")
    return payload


def _load_policy_journal(
    path: Path,
    *,
    root: Path,
    profile: Profile,
    slurm_node: str | None,
) -> dict[str, Any] | None:
    payload = _load_journal(path, allowed_schema_versions=frozenset({1, 2}))
    if payload is None:
        return None
    operation = payload.get("operation")
    legacy_fields = set(_LEGACY_POLICY_JOURNAL_FIELDS)
    if operation == "rollback":
        legacy_fields.add("rollback_target")
    if set(payload) == legacy_fields:
        if (
            operation not in {"apply", "rollback"}
            or payload.get("phase") not in {"committed", "rolled_back"}
            or payload.get("cluster") != profile.cluster
            or payload.get("host") != _canonical_host()
            or payload.get("slurm_node") != slurm_node
            or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_sha", ""))) is None
            or type(payload.get("restart")) is not bool
            or type(payload.get("apply_accounting")) is not bool
            or not isinstance(payload.get("snapshot"), str)
            or (
                payload.get("accounting_snapshot") is not None
                and not isinstance(payload.get("accounting_snapshot"), str)
            )
            or not isinstance(payload.get("created_at"), str)
            or not isinstance(payload.get("updated_at"), str)
        ):
            raise PolicyError("nonterminal legacy Slurm policy journal requires exact recovery")
        try:
            created_at = datetime.fromisoformat(payload["created_at"])
            updated_at = datetime.fromisoformat(payload["updated_at"])
        except ValueError as exc:
            raise PolicyError("legacy Slurm policy journal timestamp is invalid") from exc
        if created_at.tzinfo is None or updated_at.tzinfo is None or updated_at < created_at:
            raise PolicyError("legacy Slurm policy journal timestamp is invalid")
        snapshot = _validate_snapshot_path(root, Path(payload["snapshot"]))
        accounting = payload["accounting_snapshot"]
        if accounting is not None:
            _validate_accounting_snapshot_path(root, snapshot, Path(accounting))
        if payload["apply_accounting"] is not (accounting is not None):
            raise PolicyError("legacy Slurm policy accounting binding is invalid")
        rollback_target = payload.get("rollback_target")
        if operation == "rollback":
            if not isinstance(rollback_target, str):
                raise PolicyError("legacy Slurm rollback target is invalid")
            _validate_snapshot_path(root, Path(rollback_target))
        elif rollback_target is not None:
            raise PolicyError("legacy Slurm apply rollback binding is invalid")
        archive = (
            path.parent
            / "legacy"
            / f"{profile.cluster}-{hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()}.json"
        )
        existing = _load_journal(archive)
        if existing is None:
            _write_journal(archive, payload)
        elif existing != payload:
            raise PolicyError("legacy Slurm policy archive identity collided")
        path.unlink()
        _fsync_directory(path.parent)
        return None
    try:
        bindings = _candidate_bindings(profile, payload.get("candidate_bindings", {}))
    except PolicyError as exc:
        raise PolicyError("durable Slurm policy candidate-set binding is invalid") from exc
    expected_fields = set(_POLICY_JOURNAL_COMMON_FIELDS)
    if operation == "rollback":
        expected_fields.add("rollback_target")
    if (
        set(payload) != expected_fields
        or operation not in {"apply", "rollback"}
        or payload.get("cluster") != profile.cluster
        or payload.get("host") != _canonical_host()
        or payload.get("slurm_node") != slurm_node
        or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_sha", ""))) is None
        or payload.get("candidate_set_sha256") != _candidate_set_sha256(bindings)
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("transaction_id"))) is None
        or type(payload.get("candidate_set_generation")) is not int
        or payload["candidate_set_generation"] < 1
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("candidate_set_convergence_id")),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("candidate_set_payload_sha256")),
        )
        is None
        or type(payload.get("restart")) is not bool
        or type(payload.get("apply_accounting")) is not bool
        or payload.get("phase") not in _POLICY_JOURNAL_PHASES
        or not isinstance(payload.get("snapshot"), str)
        or (
            payload.get("accounting_snapshot") is not None
            and not isinstance(payload.get("accounting_snapshot"), str)
        )
        or not isinstance(payload.get("created_at"), str)
        or not isinstance(payload.get("updated_at"), str)
    ):
        raise PolicyError("durable Slurm policy journal binding is invalid")
    if root == Path("/"):
        if slurm_node is None or profile.host_aliases.get(slurm_node) != payload["host"]:
            raise PolicyError("durable Slurm policy journal host binding is invalid")
    elif slurm_node is not None:
        raise PolicyError("offline Slurm policy journal has a live node binding")
    if payload["apply_accounting"] is True and slurm_node != profile.controller:
        raise PolicyError("durable Slurm policy accounting binding is invalid")
    if payload["operation"] == "apply" and "rollback_target" in payload:
        raise PolicyError("durable Slurm apply journal contains a rollback target")
    if payload["operation"] == "rollback" and not isinstance(payload.get("rollback_target"), str):
        raise PolicyError("durable Slurm rollback journal lacks its exact target")
    try:
        created_at = datetime.fromisoformat(payload["created_at"])
        updated_at = datetime.fromisoformat(payload["updated_at"])
    except ValueError as exc:
        raise PolicyError("durable Slurm policy journal timestamp is invalid") from exc
    if created_at.tzinfo is None or updated_at.tzinfo is None or updated_at < created_at:
        raise PolicyError("durable Slurm policy journal timestamp is invalid")
    snapshot = _validate_snapshot_path(root, Path(payload["snapshot"]))
    accounting = payload["accounting_snapshot"]
    if accounting is not None:
        _validate_accounting_snapshot_path(root, snapshot, Path(accounting))
    if payload["apply_accounting"] is not (accounting is not None):
        raise PolicyError("durable Slurm policy accounting path binding is invalid")
    rollback_target = payload.get("rollback_target")
    if rollback_target is not None:
        _validate_snapshot_path(root, Path(rollback_target))
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


def _read_bound_regular_file(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int | None,
    description: str,
    max_bytes: int = 8 * 1024 * 1024,
) -> tuple[bytes, os.stat_result]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        lexical = path.lstat()
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PolicyError(f"{description} is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or lexical.st_uid != expected_uid
            or lexical.st_gid != expected_gid
            or opened.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or lexical.st_nlink != 1
            or opened.st_nlink != 1
            or (expected_mode is not None and stat.S_IMODE(opened.st_mode) != expected_mode)
            or (lexical.st_dev, lexical.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise PolicyError(f"{description} metadata is unsafe")
        content = bytearray()
        while True:
            chunk = os.read(descriptor, 65536)
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > max_bytes:
                raise PolicyError(f"{description} is too large")
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
            raise PolicyError(f"{description} changed during read")
        if len(content) != opened.st_size:
            raise PolicyError(f"{description} size changed during read")
        return bytes(content), opened
    finally:
        os.close(descriptor)


def _snapshot_manifest_rows(root: Path, snapshot: Path) -> list[dict[str, Any]]:
    snapshot = _validate_snapshot_path(root, snapshot)
    enforce_root_ownership = root == Path("/")
    expected_uid, expected_gid = (0, 0) if enforce_root_ownership else (os.geteuid(), os.getegid())
    manifest = _read_private_json_file(
        snapshot / "manifest.json",
        enforce_root_ownership=enforce_root_ownership,
        description="Slurm policy snapshot manifest",
    )
    rows = manifest.get("files") if isinstance(manifest, dict) else None
    if (
        not isinstance(manifest, dict)
        or set(manifest) != {"schema_version", "files"}
        or manifest.get("schema_version") != 1
    ):
        raise PolicyError("Slurm policy snapshot manifest is invalid")
    if not isinstance(rows, list) or len(rows) != len(_SNAPSHOT_RELATIVE_PATHS):
        raise PolicyError("Slurm policy snapshot file list is invalid")
    actual_paths: list[str] = []
    expected_archive_files = {"manifest.json"}
    checked: list[dict[str, Any]] = []
    for row in rows:
        if (
            not isinstance(row, dict)
            or set(row) != _SNAPSHOT_ROW_FIELDS
            or not isinstance(row.get("path"), str)
        ):
            raise PolicyError("Slurm policy snapshot row is invalid")
        relative = Path(row["path"])
        if relative.is_absolute() or ".." in relative.parts or relative.as_posix() != row["path"]:
            raise PolicyError("Slurm policy snapshot path escapes the root")
        actual_paths.append(row["path"])
        archived = snapshot / relative
        _prepare_private_directory(
            archived.parent,
            enforce_root_ownership=enforce_root_ownership,
            create=False,
        )
        if row.get("present") is True:
            if (
                type(row.get("mode")) is not int
                or not 0 <= row["mode"] <= 0o7777
                or row["mode"] & 0o022
                or row.get("uid") != expected_uid
                or row.get("gid") != expected_gid
                or row.get("nlink") != 1
                or type(row.get("size")) is not int
                or row["size"] < 0
                or _ALLOCATION_GENERATION_RE.fullmatch(str(row.get("sha256", ""))) is None
            ):
                raise PolicyError("Slurm policy snapshot metadata is invalid")
            content, _metadata = _read_bound_regular_file(
                archived,
                expected_uid=expected_uid,
                expected_gid=expected_gid,
                expected_mode=0o600,
                description="Slurm policy snapshot content",
            )
            if len(content) != row["size"] or _sha256(content) != row["sha256"]:
                raise PolicyError("Slurm policy snapshot content identity drifted")
            expected_archive_files.add(row["path"])
        elif row.get("present") is False:
            if any(
                row.get(field) is not None for field in _SNAPSHOT_ROW_FIELDS - {"path", "present"}
            ):
                raise PolicyError("absent Slurm policy snapshot row contains metadata")
            if archived.exists() or archived.is_symlink():
                raise PolicyError("absent Slurm policy snapshot has foreign content")
        else:
            raise PolicyError("Slurm policy snapshot presence is invalid")
        checked.append(dict(row))
    if tuple(actual_paths) != _SNAPSHOT_RELATIVE_PATHS:
        raise PolicyError("Slurm policy snapshot file set is not closed")
    accounting_snapshot = snapshot / "accounting-cas.json"
    if accounting_snapshot.exists() or accounting_snapshot.is_symlink():
        _read_private_json_file(
            accounting_snapshot,
            enforce_root_ownership=enforce_root_ownership,
            description="Loom accounting CAS snapshot",
        )
        expected_archive_files.add("accounting-cas.json")
    actual_archive_files: set[str] = set()
    for directory, directory_names, file_names in os.walk(snapshot, followlinks=False):
        directory_path = Path(directory)
        _prepare_private_directory(
            directory_path,
            enforce_root_ownership=enforce_root_ownership,
            create=False,
        )
        for name in directory_names:
            child = directory_path / name
            metadata = child.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise PolicyError("Slurm policy snapshot contains an unsafe directory")
        for name in file_names:
            actual_archive_files.add((directory_path / name).relative_to(snapshot).as_posix())
    if actual_archive_files != expected_archive_files:
        raise PolicyError("Slurm policy snapshot archive set is not closed")
    return checked


def _restore_snapshot(root: Path, snapshot: Path) -> None:
    rows = _snapshot_manifest_rows(root, snapshot)
    for row in rows:
        relative = Path(row["path"])
        target = root / relative
        if row.get("present") is True:
            source = snapshot / relative
            content, _metadata = _read_bound_regular_file(
                source,
                expected_uid=0 if root == Path("/") else os.geteuid(),
                expected_gid=0 if root == Path("/") else os.getegid(),
                expected_mode=0o600,
                description="Slurm policy snapshot content",
            )
            _atomic_write(target, content.decode("utf-8"), mode=row["mode"])
            restored, metadata = _read_bound_regular_file(
                target,
                expected_uid=row["uid"],
                expected_gid=row["gid"],
                expected_mode=row["mode"],
                description="restored Slurm policy file",
            )
            if (
                metadata.st_nlink != row["nlink"]
                or len(restored) != row["size"]
                or _sha256(restored) != row["sha256"]
            ):
                raise PolicyError("restored Slurm policy file identity drifted")
        elif row.get("present") is False:
            target.unlink(missing_ok=True)
            if target.parent.exists():
                _fsync_directory(target.parent)
            if target.exists() or target.is_symlink():
                raise PolicyError("restored Slurm policy file should be absent")


def _accounting_desired_state(profile: Profile) -> dict[str, Any]:
    return {
        "qos": {
            qos: {
                "Priority": str(profile.qos_priority),
                "MaxWall": profile.qos_max_wall,
                "MaxJobsPU": str(profile.qos_max_jobs_per_user),
                "MaxSubmitJobsPU": str(profile.qos_max_submit_jobs_per_user),
            }
            for qos in _profile_qoses(profile)
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
                "QOS": _account_qos(profile, account),
                "DefaultQOS": _account_qos(profile, account),
            }
            for user, account in zip(profile.users, profile.child_accounts, strict=True)
        },
    }


def _accounting_state(profile: Profile) -> dict[str, Any]:
    qos: dict[str, dict[str, str]] = {}
    for qos_name in _profile_qoses(profile):
        qos_rows = [
            line.split("|")
            for line in _run(
                (
                    "sacctmgr",
                    "-nP",
                    "show",
                    "qos",
                    "where",
                    f"name={qos_name}",
                    "format=Name,Priority,MaxWall,MaxJobsPU,MaxSubmitJobsPU",
                ),
            ).splitlines()
            if line.strip()
        ]
        if len(qos_rows) > 1 or any(len(row) < 5 for row in qos_rows):
            raise PolicyError("Loom QoS accounting snapshot is ambiguous")
        if qos_rows:
            qos[qos_name] = {
                "Priority": qos_rows[0][1],
                "MaxWall": qos_rows[0][2],
                "MaxJobsPU": qos_rows[0][3],
                "MaxSubmitJobsPU": qos_rows[0][4],
            }

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
        or set(payload) != {"schema_version", "cluster", "before", "desired"}
        or payload.get("schema_version") != 1
        or not isinstance(payload.get("cluster"), str)
        or not isinstance(payload.get("before"), dict)
        or not isinstance(payload.get("desired"), dict)
    ):
        raise PolicyError("Loom accounting CAS snapshot is unsafe")
    return payload


def _validated_accounting_snapshot(
    profile: Profile,
    path: Path,
) -> dict[str, Any]:
    payload = _load_accounting_snapshot(path)
    desired = _accounting_desired_state(profile)
    before = payload["before"]
    if (
        payload["cluster"] != profile.cluster
        or payload["desired"] != desired
        or set(before) != {"qos", "accounts", "associations"}
    ):
        raise PolicyError("Loom accounting CAS snapshot binding drifted")
    for category, desired_rows in desired.items():
        before_rows = before.get(category)
        if not isinstance(before_rows, dict) or not set(before_rows).issubset(desired_rows):
            raise PolicyError("Loom accounting CAS snapshot schema is invalid")
        for identity, row in before_rows.items():
            expected_row = desired_rows[identity]
            if (
                not isinstance(row, dict)
                or set(row) != set(expected_row)
                or any(not isinstance(value, str) for value in row.values())
            ):
                raise PolicyError("Loom accounting CAS snapshot row is invalid")
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
        for qos in _profile_qoses(profile):
            if qos in _split_csv(row[2]) or row[3] == qos:
                references.add(qos)
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

    for qos in _profile_qoses(profile):
        expected = _checked_accounting_add(
            profile,
            next(commands),
            expected,
            category="qos",
            identity=qos,
            required_fields={},
        )
        next_expected = deepcopy(expected)
        next_expected["qos"][qos] = deepcopy(desired["qos"][qos])
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
    snapshot = _validated_accounting_snapshot(profile, path)
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
    for qos in _profile_qoses(profile):
        if qos not in before["qos"] and qos in current["qos"]:
            created_identities.add(qos)
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
    for qos in reversed(_profile_qoses(profile)):
        if qos in before_qos:
            row = before_qos[qos]
            next_expected = deepcopy(expected)
            next_expected["qos"][qos] = deepcopy(row)
            expected = _checked_accounting_transition(
                profile,
                (
                    "sacctmgr",
                    "-i",
                    "modify",
                    "qos",
                    "where",
                    f"name={qos}",
                    "set",
                    f"Priority={row['Priority']}",
                    f"MaxWall={row['MaxWall']}",
                    f"MaxJobsPerUser={row['MaxJobsPU']}",
                    f"MaxSubmitJobsPerUser={row['MaxSubmitJobsPU']}",
                ),
                expected,
                next_expected,
            )
        elif qos in expected["qos"]:
            _require_accounting_state(
                profile,
                expected,
                phase="before external-reference readback",
            )
            if qos in _accounting_external_references(profile):
                raise PolicyError("new Loom QoS gained an external reference")
            next_expected = deepcopy(expected)
            next_expected["qos"].pop(qos)
            expected = _checked_accounting_transition(
                profile,
                (
                    "sacctmgr",
                    "-i",
                    "delete",
                    "qos",
                    "where",
                    f"name={qos}",
                ),
                expected,
                next_expected,
            )
    if expected != before:
        raise PolicyError("Loom accounting CAS restore readback drifted")


def _stop_guard_before_status_invalidation() -> None:
    _run_status(("systemctl", "stop", "loom-slurm-job-cgroup-guard.service"))
    active_code, _active = _run_status(
        ("systemctl", "is-active", "loom-slurm-job-cgroup-guard.service"),
    )
    if active_code == 0:
        raise PolicyError("cgroup guard remained active before status invalidation")


def _restart_services(profile: Profile, slurm_node: str) -> datetime:
    _stop_guard_before_status_invalidation()
    _invalidate_guard_status(Path("/"))
    guard_restart_boundary = datetime.now(UTC)
    _run(("systemctl", "daemon-reload"))
    _run(("systemctl", "enable", "loom-slurm-job-cgroup-guard.service"))
    _run(("systemctl", "restart", "docker"))
    _run(("systemctl", "restart", "slurmd"))
    _run(("systemctl", "start", "loom-slurm-job-cgroup-guard.service"))
    if slurm_node == profile.controller:
        _run(("systemctl", "restart", "slurmctld"))
    _run(("scontrol", "reconfigure"))
    return guard_restart_boundary


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


def _drain_journal_path(root: Path, profile: Profile) -> Path:
    return _state_root(root) / "drains" / f"{profile.cluster}.json"


def _drain_recovery_binding(
    root: Path,
    profile: Profile,
    *,
    candidate_sha: str,
    operation: str,
    apply_accounting: bool,
) -> dict[str, Any]:
    if operation not in {"apply", "rollback"}:
        raise PolicyError("Slurm restart recovery operation is invalid")
    if operation == "rollback" and apply_accounting:
        raise PolicyError("Slurm rollback recovery cannot request accounting apply")
    profile_relative = _RECOVERY_PROFILE_RELATIVE.get(profile.cluster)
    if profile_relative is None:
        raise PolicyError("Slurm restart recovery cluster is invalid")
    repository = Path(__file__).resolve().parents[2]
    profile_path = repository / profile_relative
    if root == Path("/"):
        match = _RECOVERY_CANDIDATE_ROOT_RE.fullmatch(str(repository))
        if match is None or match.group(1) != candidate_sha:
            raise PolicyError("live Slurm recovery candidate root is not exact")
        _safe_path_chain(repository, leaf_directory=True)
        _safe_path_chain(repository / _RECOVERY_POLICY_RELATIVE, leaf_directory=False)
        _safe_path_chain(profile_path, leaf_directory=False)
        if load_profile(profile_path) != profile:
            raise PolicyError("live Slurm recovery profile is not the exact candidate profile")
        candidate_tree = (
            _git_read(
                repository,
                "rev-parse",
                "--verify",
                f"{candidate_sha}^{{tree}}",
            )
            .decode("ascii")
            .strip()
        )
        if _CANDIDATE_RE.fullmatch(candidate_tree) is None:
            raise PolicyError("live Slurm recovery candidate tree is invalid")
    else:
        candidate_tree = "0" * 40
    return {
        "candidate_tree": candidate_tree,
        "candidate_root": str(repository),
        "profile_relative": profile_relative,
        "operation": operation,
        "apply_accounting": apply_accounting,
    }


def _slurm_node_admission(slurm_node: str) -> tuple[str, str]:
    raw = _run(("scontrol", "show", "node", slurm_node, "-o"))
    lines = [line.strip() for line in raw.splitlines() if line.strip()]
    if len(lines) != 1:
        raise PolicyError("Slurm node admission readback is invalid")
    line = lines[0]
    name_match = re.search(r"(?:^|\s)NodeName=(\S+)", line)
    state_match = re.search(r"(?:^|\s)State=(\S+)", line)
    reason_match = re.search(r"(?:^|\s)Reason=(.*)$", line)
    if (
        name_match is None
        or name_match.group(1).lower() != slurm_node.lower()
        or state_match is None
        or _SLURM_NODE_STATE_RE.fullmatch(state_match.group(1)) is None
    ):
        raise PolicyError("Slurm node admission readback is invalid")
    reason = reason_match.group(1).strip() if reason_match is not None else ""
    if len(reason) > 4096 or any(
        not character.isprintable() and character not in "\t" for character in reason
    ):
        raise PolicyError("Slurm node admission reason is invalid")
    return state_match.group(1), reason


def _node_is_drained_or_down(state: str) -> bool:
    return "DRAIN" in state or "DOWN" in state


def _owned_drain_reason_matches(observed: str, expected: str) -> bool:
    return observed == expected or observed.startswith(f"{expected} ")


def _load_drain_journal(
    root: Path,
    profile: Profile,
    *,
    slurm_node: str,
) -> dict[str, Any] | None:
    path = _drain_journal_path(root, profile)
    payload = _load_journal(path)
    if payload is None:
        return None
    if set(payload) == _LEGACY_DRAIN_JOURNAL_FIELDS:
        if (
            payload.get("schema_version") != 1
            or payload.get("kind") != "loom.developer-sandbox.slurm-restart-drain"
            or payload.get("cluster") != profile.cluster
            or payload.get("host") != _canonical_host()
            or payload.get("slurm_node") != slurm_node
            or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_sha", ""))) is None
            or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_tree", ""))) is None
            or payload.get("operation") not in {"apply", "rollback"}
            or type(payload.get("apply_accounting")) is not bool
            or payload.get("phase") != "released"
            or type(payload.get("owned")) is not bool
            or not isinstance(payload.get("created_at"), str)
            or not isinstance(payload.get("updated_at"), str)
        ):
            raise PolicyError("nonterminal legacy Slurm drain requires exact recovery")
        state, _reason = _slurm_node_admission(slurm_node)
        if _node_is_drained_or_down(state):
            raise PolicyError("legacy released Slurm drain still owns scheduler state")
        archive = (
            path.parent
            / "legacy"
            / f"{profile.cluster}-{hashlib.sha256(_canonical_json_bytes(payload)).hexdigest()}.json"
        )
        existing = _load_journal(archive)
        if existing is None:
            _write_journal(archive, payload)
        elif existing != payload:
            raise PolicyError("legacy Slurm drain archive identity collided")
        path.unlink()
        _fsync_directory(path.parent)
        return None
    try:
        bindings = _candidate_bindings(profile, payload.get("candidate_bindings", {}))
    except PolicyError as exc:
        raise PolicyError("durable Slurm restart candidate-set binding is invalid") from exc
    if (
        set(payload) != _DRAIN_JOURNAL_FIELDS
        or payload.get("schema_version") != 1
        or payload.get("kind") != "loom.developer-sandbox.slurm-restart-drain"
        or payload.get("cluster") != profile.cluster
        or payload.get("host") != _canonical_host()
        or payload.get("slurm_node") != slurm_node
        or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_sha", ""))) is None
        or payload.get("candidate_set_sha256") != _candidate_set_sha256(bindings)
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("transaction_id"))) is None
        or type(payload.get("candidate_set_generation")) is not int
        or payload["candidate_set_generation"] < 1
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("candidate_set_convergence_id")),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("candidate_set_payload_sha256")),
        )
        is None
        or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_tree", ""))) is None
        or not isinstance(payload.get("candidate_root"), str)
        or not Path(payload["candidate_root"]).is_absolute()
        or len(payload["candidate_root"]) > 4096
        or payload.get("profile_relative") != _RECOVERY_PROFILE_RELATIVE.get(profile.cluster)
        or payload.get("operation") not in {"apply", "rollback"}
        or type(payload.get("apply_accounting")) is not bool
        or (payload.get("operation") == "rollback" and payload.get("apply_accounting") is True)
        or _DRAIN_TOKEN_RE.fullmatch(str(payload.get("ownership_token", ""))) is None
        or _DRAIN_REASON_RE.fullmatch(str(payload.get("ownership_reason", ""))) is None
        or type(payload.get("owned")) is not bool
        or _SLURM_NODE_STATE_RE.fullmatch(str(payload.get("prior_state", ""))) is None
        or not isinstance(payload.get("prior_reason"), str)
        or len(payload["prior_reason"]) > 4096
        or payload.get("phase") not in _DRAIN_JOURNAL_PHASES
        or not isinstance(payload.get("created_at"), str)
        or not isinstance(payload.get("updated_at"), str)
    ):
        raise PolicyError("durable Slurm restart drain binding is invalid")
    try:
        created_at = datetime.fromisoformat(payload["created_at"])
        updated_at = datetime.fromisoformat(payload["updated_at"])
    except ValueError as exc:
        raise PolicyError("durable Slurm restart drain timestamp is invalid") from exc
    if (
        created_at.tzinfo is None
        or updated_at.tzinfo is None
        or updated_at < created_at
        or any(
            not character.isprintable() and character not in "\t"
            for character in payload["prior_reason"]
        )
    ):
        raise PolicyError("durable Slurm restart drain binding is invalid")
    return payload


def _advance_drain_journal(
    root: Path,
    profile: Profile,
    payload: dict[str, Any],
    phase: str,
) -> None:
    if phase not in _DRAIN_JOURNAL_PHASES:
        raise PolicyError("Slurm restart drain phase is invalid")
    payload["phase"] = phase
    payload["updated_at"] = datetime.now(UTC).isoformat()
    _write_journal(_drain_journal_path(root, profile), payload)


def _release_restart_drain(
    root: Path,
    profile: Profile,
    payload: dict[str, Any],
) -> None:
    if payload.get("phase") == "released":
        return
    if payload.get("owned") is not True:
        _advance_drain_journal(root, profile, payload, "released")
        return
    slurm_node = str(payload["slurm_node"])
    state, reason = _slurm_node_admission(slurm_node)
    if "DOWN" in state:
        _advance_drain_journal(root, profile, payload, "release_failed")
        raise PolicyError("owned Slurm drain became DOWN; preserving scheduler state")
    if "DRAIN" not in state:
        _advance_drain_journal(root, profile, payload, "released")
        return
    ownership_reason = str(payload["ownership_reason"])
    if not _owned_drain_reason_matches(reason, ownership_reason):
        _advance_drain_journal(root, profile, payload, "release_failed")
        raise PolicyError("owned Slurm drain reason changed; preserving scheduler state")
    try:
        _run(("scontrol", "update", f"NodeName={slurm_node}", "State=RESUME"))
        released_state, _released_reason = _slurm_node_admission(slurm_node)
    except Exception:
        _advance_drain_journal(root, profile, payload, "release_failed")
        raise
    if _node_is_drained_or_down(released_state):
        _advance_drain_journal(root, profile, payload, "release_failed")
        raise PolicyError("owned Slurm drain release readback drifted")
    _advance_drain_journal(root, profile, payload, "released")


def _acquire_restart_drain(
    root: Path,
    profile: Profile,
    *,
    slurm_node: str,
    candidate_sha: str,
    candidate_bindings: Mapping[str, Any] | None = None,
    transaction_id: str | None = None,
    generation: int | None = None,
    convergence_id: str | None = None,
    payload_sha256: str | None = None,
    operation: str = "apply",
    apply_accounting: bool = False,
) -> dict[str, Any]:
    if candidate_bindings is None and root == Path("/"):
        raise PolicyError("live Slurm drain requires the complete candidate set")
    bindings = (
        _offline_candidate_bindings(profile, candidate_sha)
        if candidate_bindings is None
        else _candidate_bindings(profile, candidate_bindings)
    )
    candidate_set_sha256 = _candidate_set_sha256(bindings)
    transaction = _transaction_identity(
        transaction_id=transaction_id,
        generation=generation,
        convergence_id=convergence_id,
        payload_sha256=payload_sha256,
        required=root == Path("/"),
    )
    recovery_binding = _drain_recovery_binding(
        root,
        profile,
        candidate_sha=candidate_sha,
        operation=operation,
        apply_accounting=apply_accounting,
    )
    existing = _load_drain_journal(
        root,
        profile,
        slurm_node=slurm_node,
    )
    if existing is not None and existing["phase"] != "released":
        state, reason = _slurm_node_admission(slurm_node)
        if (
            existing["candidate_sha"] != candidate_sha
            or existing["candidate_set_sha256"] != candidate_set_sha256
        ):
            _release_restart_drain(root, profile, existing)
        elif any(existing[field] != value for field, value in transaction.items()):
            raise PolicyError("candidate-owned Slurm drain transaction identity drifted")
        elif any(existing[field] != value for field, value in recovery_binding.items()):
            raise PolicyError("candidate-owned Slurm drain recovery binding drifted")
        elif existing["owned"] is True:
            if "DOWN" in state:
                raise PolicyError("owned Slurm drain became DOWN; preserving scheduler state")
            if "DRAIN" in state and _owned_drain_reason_matches(
                reason,
                str(existing["ownership_reason"]),
            ):
                return existing
            if "DRAIN" in state:
                raise PolicyError("owned Slurm drain reason changed; preserving scheduler state")
            _advance_drain_journal(root, profile, existing, "released")
        elif _node_is_drained_or_down(state):
            raise PolicyError(
                "Slurm node has a foreign DRAIN/DOWN; deferring destructive convergence",
            )
        else:
            _advance_drain_journal(root, profile, existing, "released")

    prior_state, prior_reason = _slurm_node_admission(slurm_node)
    if _node_is_drained_or_down(prior_state):
        raise PolicyError(
            "Slurm node has a foreign DRAIN/DOWN; deferring destructive convergence",
        )
    token = hashlib.sha256(os.urandom(32)).hexdigest()
    ownership_reason = f"loom-sandbox-policy:{candidate_sha[:12]}:{token[:16]}"
    created_at = datetime.now(UTC).isoformat()
    payload: dict[str, Any] = {
        "schema_version": 1,
        "kind": "loom.developer-sandbox.slurm-restart-drain",
        "cluster": profile.cluster,
        "host": _canonical_host(),
        "slurm_node": slurm_node,
        "candidate_sha": candidate_sha,
        "candidate_set_sha256": candidate_set_sha256,
        "candidate_bindings": bindings,
        **transaction,
        **recovery_binding,
        "ownership_token": token,
        "ownership_reason": ownership_reason,
        "owned": True,
        "prior_state": prior_state,
        "prior_reason": prior_reason,
        "phase": "prepared",
        "created_at": created_at,
        "updated_at": created_at,
    }
    _write_journal(_drain_journal_path(root, profile), payload)
    _run(
        (
            "scontrol",
            "update",
            f"NodeName={slurm_node}",
            "State=DRAIN",
            f"Reason={ownership_reason}",
        ),
    )
    drained_state, drained_reason = _slurm_node_admission(slurm_node)
    if "DRAIN" not in drained_state or not _owned_drain_reason_matches(
        drained_reason,
        ownership_reason,
    ):
        raise PolicyError("candidate-owned Slurm drain readback drifted")
    _advance_drain_journal(root, profile, payload, "drained")
    return payload


def _restart_activity(profile: Profile, slurm_node: str) -> dict[str, bool]:
    slurm_jobs = bool(_run(("squeue", "-h", "-w", slurm_node)).strip())
    docker_containers = bool(_run(("docker", "ps", "-q")).strip())
    gpu_processes = False
    if profile.gpu_tres_per_slot > 0:
        gpu_processes = bool(
            _run(
                (
                    "nvidia-smi",
                    "--query-compute-apps=pid",
                    "--format=csv,noheader,nounits",
                ),
            ).strip(),
        )
    return {
        "slurm_jobs": slurm_jobs,
        "docker_containers": docker_containers,
        "gpu_processes": gpu_processes,
    }


def _wait_for_restart_quiescence(
    root: Path,
    profile: Profile,
    payload: dict[str, Any],
) -> None:
    deadline = time.monotonic() + _RESTART_QUIESCE_TIMEOUT_SECONDS
    while True:
        activity = _restart_activity(profile, str(payload["slurm_node"]))
        active = [name for name, present in activity.items() if present]
        if not active:
            _advance_drain_journal(root, profile, payload, "quiesced")
            return
        if time.monotonic() >= deadline:
            raise PolicyError(
                "node remains busy after candidate-owned drain: " + ", ".join(active),
            )
        time.sleep(_RESTART_QUIESCE_POLL_SECONDS)


def _mark_restart_drain_transacting(
    root: Path,
    profile: Profile,
    payload: dict[str, Any],
) -> None:
    _advance_drain_journal(root, profile, payload, "transacting")


def _recovery_candidate(
    root: Path,
    payload: Mapping[str, Any],
) -> tuple[Profile, Path, Path]:
    cluster = str(payload.get("cluster", ""))
    candidate_sha = str(payload.get("candidate_sha", ""))
    candidate_tree = str(payload.get("candidate_tree", ""))
    candidate_root = Path(str(payload.get("candidate_root", "")))
    profile_relative = str(payload.get("profile_relative", ""))
    expected_profile_relative = _RECOVERY_PROFILE_RELATIVE.get(cluster)
    match = _RECOVERY_CANDIDATE_ROOT_RE.fullmatch(str(candidate_root))
    if (
        root != Path("/")
        or os.geteuid() != 0
        or expected_profile_relative is None
        or profile_relative != expected_profile_relative
        or match is None
        or match.group(1) != candidate_sha
        or _CANDIDATE_RE.fullmatch(candidate_tree) is None
    ):
        raise PolicyError("durable Slurm recovery candidate binding is invalid")
    policy_path = candidate_root / _RECOVERY_POLICY_RELATIVE
    profile_path = candidate_root / profile_relative
    _safe_path_chain(candidate_root, leaf_directory=True)
    _safe_path_chain(policy_path, leaf_directory=False)
    _safe_path_chain(profile_path, leaf_directory=False)
    if (
        _git_read(candidate_root, "rev-parse", "--verify", "HEAD").decode("ascii").strip()
        != candidate_sha
        or _git_read(
            candidate_root,
            "rev-parse",
            "--verify",
            f"{candidate_sha}^{{tree}}",
        )
        .decode("ascii")
        .strip()
        != candidate_tree
        or _git_read(
            candidate_root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        )
    ):
        raise PolicyError("durable Slurm recovery candidate checkout drifted")
    profile = load_profile(profile_path)
    if profile.cluster != cluster:
        raise PolicyError("durable Slurm recovery profile cluster drifted")
    return profile, policy_path, profile_path


def _run_recovery_candidate(
    policy_path: Path,
    profile_path: Path,
    payload: Mapping[str, Any],
) -> None:
    operation = str(payload["operation"])
    argv = [
        "/usr/bin/python3",
        "-I",
        "-B",
        str(policy_path),
        operation,
        "--profile",
        str(profile_path),
        "--candidate-sha",
        str(payload["candidate_sha"]),
        "--candidate-bindings-json",
        json.dumps(
            payload["candidate_bindings"],
            sort_keys=True,
            separators=(",", ":"),
        ),
        "--transaction-id",
        str(payload["transaction_id"]),
        "--candidate-set-generation",
        str(payload["candidate_set_generation"]),
        "--candidate-set-convergence-id",
        str(payload["candidate_set_convergence_id"]),
        "--candidate-set-payload-sha256",
        str(payload["candidate_set_payload_sha256"]),
        "--execute",
        "--restart",
    ]
    if operation == "apply" and payload["apply_accounting"] is True:
        argv.append("--apply-accounting")
    environment = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    }
    try:
        completed = subprocess.run(
            argv,
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=environment,
            timeout=240,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError("candidate-bound Slurm recovery failed safely") from exc
    if completed.returncode != 0:
        raise PolicyError(
            f"candidate-bound Slurm recovery deferred with exit code {completed.returncode}",
        )


def recover_pending_drains(root: Path = Path("/")) -> dict[str, Any]:
    if root != Path("/") or os.geteuid() != 0:
        raise PolicyError("Slurm drain recovery requires the persistent live root")
    recovered: list[dict[str, str]] = []
    for cluster in _RECOVERY_CLUSTERS:
        path = _state_root(root) / "drains" / f"{cluster}.json"
        raw = _load_journal(path)
        if raw is None or raw.get("phase") == "released":
            continue
        profile, policy_path, profile_path = _recovery_candidate(root, raw)
        slurm_node = _slurm_node_for_host(profile, _canonical_host())
        if slurm_node is None:
            raise PolicyError("durable Slurm recovery host is outside the candidate profile")
        payload = _load_drain_journal(
            root,
            profile,
            slurm_node=slurm_node,
        )
        if payload is None or payload != raw:
            raise PolicyError("durable Slurm recovery journal changed during validation")
        if payload["owned"] is not True:
            raise PolicyError("foreign Slurm admission state cannot be recovered by Loom")
        _run_recovery_candidate(policy_path, profile_path, payload)
        final = _load_drain_journal(
            root,
            profile,
            slurm_node=slurm_node,
        )
        if final is None or final["phase"] != "released":
            raise PolicyError("candidate-bound Slurm recovery did not release its owned drain")
        recovered.append(
            {
                "cluster": cluster,
                "candidate_sha": str(payload["candidate_sha"]),
                "operation": str(payload["operation"]),
                "phase": "released",
            },
        )
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-recovery",
        "recovered": recovered,
        "status": "succeeded",
    }


def _restore_services(root: Path, profile: Profile, slurm_node: str) -> datetime:
    guard_unit = root / "etc/systemd/system/loom-slurm-job-cgroup-guard.service"
    _stop_guard_before_status_invalidation()
    _invalidate_guard_status(root)
    guard_restart_boundary = datetime.now(UTC)
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
        _run(("systemctl", "start", "loom-slurm-job-cgroup-guard.service"))
    if slurm_node == profile.controller:
        _run(("systemctl", "restart", "slurmctld"))
    _run(("scontrol", "reconfigure"))
    return guard_restart_boundary


def _snapshot_readback(root: Path, snapshot: Path) -> dict[str, Any]:
    rows = _snapshot_manifest_rows(root, snapshot)
    checked: list[str] = []
    for row in rows:
        relative = Path(row["path"])
        live = root / relative
        if row.get("present") is True:
            content, metadata = _read_bound_regular_file(
                live,
                expected_uid=row["uid"],
                expected_gid=row["gid"],
                expected_mode=row["mode"],
                description="restored Slurm policy file",
            )
            if (
                metadata.st_nlink != row["nlink"]
                or len(content) != row["size"]
                or _sha256(content) != row["sha256"]
            ):
                raise PolicyError("restored Slurm policy file readback drifted")
        elif row.get("present") is False:
            if live.exists() or live.is_symlink():
                raise PolicyError("restored Slurm policy file should be absent")
        checked.append(str(relative))
    return {"converged": True, "snapshot": str(snapshot), "files": checked}


def _accounting_snapshot_matches(profile: Profile, snapshot: Path) -> None:
    payload = _validated_accounting_snapshot(profile, snapshot)
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
    for qos_name in _profile_qoses(profile):
        qos_rows = [
            line.split("|")
            for line in _run(
                (
                    "sacctmgr",
                    "-nP",
                    "show",
                    "qos",
                    "where",
                    f"name={qos_name}",
                    "format=Name,Priority,MaxWall,MaxJobsPU,MaxSubmitJobsPU",
                ),
            ).splitlines()
            if line.strip()
        ]
        expected_qos = [
            qos_name,
            str(profile.qos_priority),
            profile.qos_max_wall,
            str(profile.qos_max_jobs_per_user),
            str(profile.qos_max_submit_jobs_per_user),
        ]
        if len(qos_rows) != 1 or qos_rows[0][:5] != expected_qos:
            raise PolicyError("live Slurm QoS readback is missing or drifted")

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
        qos = _account_qos(profile, account)
        if (
            association is None
            or association[2] != str(profile.fairshare)
            or qos not in _split_csv(association[3])
            or association[4] != qos
        ):
            raise PolicyError("live Slurm user association or fair-share drifted")
    return {
        "qos": list(_profile_qoses(profile)),
        "accounts": sorted(expected_accounts),
        "associations": [
            {"user": user, "account": account}
            for user, account in zip(profile.users, profile.child_accounts, strict=True)
        ],
    }


def _guard_status_readback(
    root: Path,
    *,
    candidate_bindings: Mapping[str, Any],
    expected_config_sha256: str,
    require_probe: bool,
    sandbox: str | None = None,
    not_before: datetime | None = None,
) -> dict[str, Any]:
    bindings = dict(candidate_bindings)
    candidate_set_sha256 = _candidate_set_sha256(bindings)
    path = root / _GUARD_STATUS_RELATIVE
    try:
        expected_uid, expected_gid = (0, 0) if root == Path("/") else (os.geteuid(), os.getegid())
        raw, _metadata = _read_bound_regular_file(
            path,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=0o600,
            description="cgroup guard status",
            max_bytes=1 << 20,
        )
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise PolicyError("cgroup guard status is unavailable") from exc
    if not isinstance(payload, dict) or payload.get("schema_version") != 2:
        raise PolicyError("cgroup guard status is unsafe")
    try:
        observed = datetime.fromisoformat(str(payload["timestamp"]))
    except (KeyError, ValueError) as exc:
        raise PolicyError("cgroup guard status timestamp is invalid") from exc
    now = datetime.now(UTC)
    normalized_observed = observed.astimezone(UTC) if observed.tzinfo is not None else None
    normalized_not_before = (
        not_before.astimezone(UTC)
        if not_before is not None and not_before.tzinfo is not None
        else None
    )
    if (
        normalized_observed is None
        or (not_before is not None and normalized_not_before is None)
        or (normalized_not_before is not None and normalized_observed < normalized_not_before)
        or normalized_observed > now + _GUARD_MAX_CLOCK_SKEW
        or now - normalized_observed > _GUARD_STATUS_MAX_AGE
    ):
        raise PolicyError("cgroup guard status is stale")
    if (
        payload.get("candidate_set_sha256") != candidate_set_sha256
        or payload.get("config_sha256") != expected_config_sha256
        or payload.get("failed") != 0
        or payload.get("failures") != []
    ):
        raise PolicyError("cgroup guard status failed or drifted")
    if require_probe:
        if sandbox is None:
            raise PolicyError("cgroup guard probe readback requires a sandbox")
        matching = [
            (account, binding)
            for account, binding in bindings.items()
            if isinstance(binding, Mapping) and binding.get("sandbox") == sandbox
        ]
        if len(matching) != 1:
            raise PolicyError("cgroup guard sandbox binding is absent or ambiguous")
        account, binding = matching[0]
        probes = payload.get("resource_probes")
        probe = probes.get(account) if isinstance(probes, dict) else None
        if (
            not isinstance(binding, Mapping)
            or not isinstance(probe, dict)
            or probe.get("account") != account
            or probe.get("sandbox") != sandbox
            or probe.get("service_user") != binding.get("service_user")
            or probe.get("candidate_sha") != binding.get("candidate_sha")
            or probe.get("candidate_tree") != binding.get("candidate_tree")
            or probe.get("candidate_set_sha256") != candidate_set_sha256
        ):
            raise PolicyError("cgroup guard lacks an account-bound live job probe")
        try:
            probe_observed = datetime.fromisoformat(str(probe["observed_at"]))
        except (KeyError, ValueError) as exc:
            raise PolicyError("cgroup guard job probe timestamp is invalid") from exc
        normalized_probe = (
            probe_observed.astimezone(UTC) if probe_observed.tzinfo is not None else None
        )
        if (
            normalized_probe is None
            or normalized_probe > now + _GUARD_MAX_CLOCK_SKEW
            or now - normalized_probe > _ALLOCATION_PROBE_MAX_AGE
        ):
            raise PolicyError("cgroup guard job resource probe is stale")
    return payload


def _wait_for_guard_status(
    root: Path,
    *,
    candidate_bindings: Mapping[str, Any],
    expected_config_sha256: str,
    require_probe: bool,
    sandbox: str | None = None,
    not_before: datetime | None = None,
) -> dict[str, Any]:
    deadline = time.monotonic() + 10
    last_error: PolicyError | None = None
    while time.monotonic() < deadline:
        try:
            return _guard_status_readback(
                root,
                candidate_bindings=candidate_bindings,
                expected_config_sha256=expected_config_sha256,
                require_probe=require_probe,
                sandbox=sandbox,
                not_before=not_before,
            )
        except PolicyError as exc:
            last_error = exc
            time.sleep(0.25)
    raise PolicyError("cgroup guard did not publish matching fresh status") from last_error


def _invalidate_guard_status(root: Path) -> None:
    path = root / _GUARD_STATUS_RELATIVE
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or (root == Path("/") and (metadata.st_uid, metadata.st_gid) != (0, 0))
        or metadata.st_nlink != 1
    ):
        raise PolicyError("cgroup guard status cannot be invalidated safely")
    path.unlink()
    _fsync_directory(path.parent)


def _legacy_guard_status_readback(
    root: Path,
    profile: Profile,
    *,
    config: Mapping[str, Any],
    expected_config_sha256: str,
    not_before: datetime | None = None,
) -> dict[str, Any]:
    if (
        set(config)
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
        or config.get("schema_version") != 1
        or config.get("cluster") != profile.cluster
        or config.get("controller") != profile.controller
        or config.get("submit_host") != profile.submit_host
        or _CANDIDATE_RE.fullmatch(str(config.get("candidate_sha"))) is None
        or config.get("allowed_accounts") != sorted(profile.child_accounts)
    ):
        raise PolicyError("restored legacy guard config is invalid")
    path = root / _GUARD_STATUS_RELATIVE
    try:
        expected_uid, expected_gid = (0, 0) if root == Path("/") else (os.geteuid(), os.getegid())
        raw, _metadata = _read_bound_regular_file(
            path,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=0o600,
            description="restored legacy guard status",
            max_bytes=1 << 20,
        )
        payload = json.loads(raw)
        observed = datetime.fromisoformat(str(payload["timestamp"]))
    except (KeyError, ValueError, json.JSONDecodeError) as exc:
        raise PolicyError("restored legacy guard status is unavailable") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or payload.get("candidate_sha") != config["candidate_sha"]
        or payload.get("config_sha256") != expected_config_sha256
        or payload.get("failed") != 0
        or payload.get("failures") != []
        or observed.tzinfo is None
        or (
            not_before is not None
            and (not_before.tzinfo is None or observed.astimezone(UTC) < not_before.astimezone(UTC))
        )
        or datetime.now(UTC) - observed.astimezone(UTC) > _GUARD_STATUS_MAX_AGE
    ):
        raise PolicyError("restored legacy guard status failed or drifted")
    return payload


def _wait_for_restored_guard_status(
    root: Path,
    profile: Profile,
    *,
    guard_config: Path,
    not_before: datetime | None = None,
) -> dict[str, Any]:
    expected_uid, expected_gid = (0, 0) if root == Path("/") else (os.geteuid(), os.getegid())
    raw, _metadata = _read_bound_regular_file(
        guard_config,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=0o600,
        description="restored guard config",
        max_bytes=1 << 20,
    )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("restored guard config is invalid") from exc
    deadline = time.monotonic() + 10
    last_error: PolicyError | None = None
    while time.monotonic() < deadline:
        try:
            if payload.get("schema_version") == 2:
                bindings = _candidate_bindings(profile, payload["candidate_bindings"])
                return _guard_status_readback(
                    root,
                    candidate_bindings=bindings,
                    expected_config_sha256=_sha256(raw),
                    require_probe=False,
                    not_before=not_before,
                )
            if payload.get("schema_version") == 1:
                return _legacy_guard_status_readback(
                    root,
                    profile,
                    config=payload,
                    expected_config_sha256=_sha256(raw),
                    not_before=not_before,
                )
            raise PolicyError("restored guard config version is unsupported")
        except (KeyError, TypeError, PolicyError) as exc:
            last_error = (
                exc
                if isinstance(exc, PolicyError)
                else PolicyError("restored guard config invalid")
            )
            time.sleep(0.25)
    raise PolicyError("restored guard did not publish matching fresh status") from last_error


def _allocation_state_base(root: Path, profile: Profile, sandbox: str) -> Path:
    _sandbox_account(profile, sandbox)
    return root / _ALLOCATION_PROBE_RELATIVE / profile.cluster / sandbox


def _allocation_probe_path(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
) -> Path:
    return _allocation_state_base(root, profile, sandbox) / f"{candidate_sha}.json"


def _allocation_inflight_path(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
) -> Path:
    return _allocation_state_base(root, profile, sandbox) / f"{candidate_sha}.inflight.json"


def _allocation_matrix_path(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
) -> Path:
    return _allocation_state_base(root, profile, sandbox) / f"{candidate_sha}.matrix.json"


def _allocation_generation_id(runtime_attestation: Mapping[str, Any]) -> str:
    bundle_id = runtime_attestation.get("bundle_id")
    if _ALLOCATION_GENERATION_RE.fullmatch(str(bundle_id)) is None:
        raise PolicyError("allocation matrix runtime proof bundle ID is invalid")
    return str(bundle_id)


def _legacy_allocation_generation_is_bound(
    matrix: Mapping[str, Any],
    *,
    sandbox: str,
) -> bool:
    generation_id = str(matrix.get("generation_id", ""))
    runtime_attestation = matrix.get("runtime_attestation")
    if (
        _LEGACY_ALLOCATION_GENERATION_RE.fullmatch(generation_id) is None
        or not isinstance(runtime_attestation, Mapping)
        or _ALLOCATION_GENERATION_RE.fullmatch(
            str(runtime_attestation.get("bundle_id", "")),
        )
        is None
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(runtime_attestation.get("receipt_sha256", "")),
        )
        is None
        or runtime_attestation.get("candidate_sha") != matrix.get("candidate_sha")
        or runtime_attestation.get("candidate_tree") != matrix.get("candidate_tree")
        or runtime_attestation.get("sandbox") != sandbox
        or matrix.get("sandbox") != sandbox
    ):
        return False
    return generation_id == str(runtime_attestation["receipt_sha256"])[:12]


def _allocation_generation_is_bundle_bound(
    matrix: Mapping[str, Any],
    *,
    sandbox: str,
) -> bool:
    runtime_attestation = matrix.get("runtime_attestation")
    return (
        isinstance(runtime_attestation, Mapping)
        and _ALLOCATION_GENERATION_RE.fullmatch(str(matrix.get("generation_id", ""))) is not None
        and matrix.get("generation_id") == runtime_attestation.get("bundle_id")
        and runtime_attestation.get("candidate_sha") == matrix.get("candidate_sha")
        and runtime_attestation.get("candidate_tree") == matrix.get("candidate_tree")
        and runtime_attestation.get("sandbox") == sandbox
        and matrix.get("sandbox") == sandbox
    )


def _allocation_archive_path(
    path: Path,
    *,
    generation_id: str,
    archived_at: datetime,
) -> Path:
    if (
        _ALLOCATION_GENERATION_RE.fullmatch(generation_id) is None
        and _LEGACY_ALLOCATION_GENERATION_RE.fullmatch(generation_id) is None
    ):
        raise PolicyError("allocation matrix generation ID is invalid")
    timestamp = archived_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
    return path.with_name(f"{path.name}.{generation_id}.{timestamp}.archived")


def _archive_allocation_generation(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
    matrix: Mapping[str, Any],
) -> None:
    generation_id = str(matrix.get("generation_id", ""))
    if not (
        _allocation_generation_is_bundle_bound(matrix, sandbox=sandbox)
        or _legacy_allocation_generation_is_bound(matrix, sandbox=sandbox)
    ):
        raise PolicyError("allocation matrix generation cannot be archived safely")
    archived_at = datetime.now(UTC)
    matrix_path = _allocation_matrix_path(root, profile, sandbox, candidate_sha)
    final_path = _allocation_probe_path(root, profile, sandbox, candidate_sha)
    _require_root_private_directory(matrix_path.parent)
    for source in (matrix_path, final_path):
        if not source.exists():
            continue
        destination = _allocation_archive_path(
            source,
            generation_id=generation_id,
            archived_at=archived_at,
        )
        if destination.exists():
            raise PolicyError("allocation matrix archive destination already exists")
        os.replace(source, destination)
    _fsync_directory(matrix_path.parent)


def _allocation_node_inflight_path(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
    node: str,
) -> Path:
    if node not in profile.allowed_nodes:
        raise PolicyError("allocation matrix node is outside the profile")
    safe_node = node.lower()
    if _SAFE_NAME.fullmatch(safe_node) is None:
        raise PolicyError("allocation matrix node name is unsafe")
    return (
        root
        / _ALLOCATION_PROBE_RELATIVE
        / profile.cluster
        / sandbox
        / f"{candidate_sha}.{safe_node}.inflight.json"
    )


def _allocation_result_path(
    worker_env: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
    node: str,
) -> Path:
    if node not in profile.allowed_nodes:
        raise PolicyError("allocation result node is outside the profile")
    return (
        worker_env.parent
        / ".loom-allocation-probes"
        / profile.cluster
        / sandbox
        / candidate_sha
        / f"{node.lower()}.json"
    )


def _prepare_allocation_result_path(
    path: Path,
    *,
    worker_env: Path,
    batch_uid: int,
    batch_gid: int,
) -> None:
    expected_root = worker_env.parent / ".loom-allocation-probes"
    try:
        relative = path.relative_to(expected_root)
    except ValueError as exc:
        raise PolicyError("allocation result path escaped its private root") from exc
    if len(relative.parts) != 4:
        raise PolicyError("allocation result path escaped its private root")
    cluster_directory = expected_root / relative.parts[0]
    sandbox_directory = cluster_directory / relative.parts[1]
    candidate_directory = sandbox_directory / relative.parts[2]
    for directory in (
        expected_root,
        cluster_directory,
        sandbox_directory,
        candidate_directory,
        path.parent,
    ):
        try:
            directory.mkdir(mode=0o700)
            os.chown(directory, batch_uid, batch_gid)
        except FileExistsError:
            pass
        except OSError as exc:
            raise PolicyError("allocation result directory could not be prepared") from exc
        metadata = directory.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != batch_uid
            or metadata.st_gid != batch_gid
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise PolicyError("allocation result directory is unsafe")
    path.unlink(missing_ok=True)
    _fsync_directory(path.parent)


def _discard_allocation_result(path: Path) -> None:
    path.unlink(missing_ok=True)
    if path.parent.exists():
        _fsync_directory(path.parent)


def _load_allocation_result(
    path: Path,
    *,
    batch_uid: int,
    batch_gid: int,
) -> dict[str, Any]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PolicyError("allocation node result is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        linked = path.lstat()
        if (
            stat.S_ISLNK(linked.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or stat.S_IMODE(linked.st_mode) != 0o600
            or opened.st_nlink != 1
            or linked.st_nlink != 1
            or opened.st_uid != batch_uid
            or opened.st_gid != batch_gid
            or linked.st_uid != batch_uid
            or linked.st_gid != batch_gid
            or opened.st_dev != linked.st_dev
            or opened.st_ino != linked.st_ino
        ):
            raise PolicyError("allocation node result is unsafe")
        content = bytearray()
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > 1024 * 1024:
                raise PolicyError("allocation node result is too large")
    finally:
        os.close(descriptor)
    try:
        payload = json.loads(bytes(content))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("allocation node result is invalid") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != 1
        or bytes(content) != _canonical_json_bytes(payload) + b"\n"
    ):
        raise PolicyError("allocation node result is not canonical")
    return payload


def _allocation_job_name(
    sandbox: str,
    candidate_sha: str,
    node: str,
    attempt: int,
    *,
    generation_id: str,
    allow_legacy: bool = False,
) -> str:
    if _SAFE_NAME.fullmatch(sandbox) is None:
        raise PolicyError("allocation matrix sandbox name is unsafe")
    if _CANDIDATE_RE.fullmatch(candidate_sha) is None:
        raise PolicyError("allocation matrix candidate SHA is invalid")
    safe_node = node.lower()
    if _SAFE_NAME.fullmatch(safe_node) is None:
        raise PolicyError("allocation matrix node name is unsafe")
    if attempt < 1:
        raise PolicyError("allocation matrix attempt is invalid")
    if _ALLOCATION_GENERATION_RE.fullmatch(generation_id) is not None:
        job_name = f"loom827-{sandbox}-{candidate_sha[:12]}-{safe_node}-g{generation_id}-a{attempt}"
    elif allow_legacy and _LEGACY_ALLOCATION_GENERATION_RE.fullmatch(generation_id):
        job_name = f"loom827-{sandbox}-{candidate_sha}-{safe_node}-g{generation_id}-a{attempt}"
    else:
        raise PolicyError("allocation matrix generation ID is invalid")
    if len(job_name) > 128:
        raise PolicyError("allocation matrix job name exceeds Slurm's safe limit")
    return job_name


def _allocation_job_generation(job_name: str) -> str:
    match = re.search(r"-g([0-9a-f]{64}|[0-9a-f]{12})-a[1-9][0-9]*$", job_name)
    if match is None:
        raise PolicyError("allocation matrix job generation is invalid")
    return match.group(1)


def _allocation_lock_path(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
) -> Path:
    return _allocation_state_base(root, profile, sandbox) / f"{candidate_sha}.lock"


@contextmanager
def _allocation_probe_lock(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
    *,
    enforce_root_ownership: bool = True,
) -> Iterator[None]:
    if _CANDIDATE_RE.fullmatch(candidate_sha) is None:
        raise PolicyError("allocation probe lock candidate SHA is invalid")
    path = _allocation_lock_path(root, profile, sandbox, candidate_sha)
    with _persistent_private_lock(
        path,
        enforce_root_ownership=enforce_root_ownership,
    ):
        yield


def _invalidate_allocation_artifact(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
) -> None:
    path = _allocation_probe_path(root, profile, sandbox, candidate_sha)
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
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
        or (enforce_root_ownership and (metadata.st_uid != 0 or metadata.st_gid != 0))
    ):
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
    payload = _read_private_json_file(
        path,
        enforce_root_ownership=enforce_root_ownership,
        description="allocation state",
    )
    if not isinstance(payload, dict) or payload.get("schema_version") != 1:
        raise PolicyError("allocation inflight journal is unsafe")
    return payload


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode()


def _read_attestation_file(
    path: Path,
    *,
    expected_mode: int,
    enforce_root_ownership: bool,
) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise PolicyError("runtime attestation input is unavailable") from exc
    try:
        opened = os.fstat(descriptor)
        linked = path.lstat()
        expected_uid = 0 if enforce_root_ownership else os.geteuid()
        expected_gid = 0 if enforce_root_ownership else os.getegid()
        if (
            stat.S_ISLNK(linked.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or not stat.S_ISREG(linked.st_mode)
            or stat.S_IMODE(opened.st_mode) != expected_mode
            or stat.S_IMODE(linked.st_mode) != expected_mode
            or opened.st_nlink != 1
            or linked.st_nlink != 1
            or opened.st_uid != expected_uid
            or linked.st_uid != expected_uid
            or opened.st_gid != expected_gid
            or linked.st_gid != expected_gid
            or opened.st_dev != linked.st_dev
            or opened.st_ino != linked.st_ino
        ):
            raise PolicyError("runtime attestation input is unsafe")
        content = bytearray()
        while True:
            chunk = os.read(descriptor, 64 * 1024)
            if not chunk:
                break
            content.extend(chunk)
            if len(content) > 1024 * 1024:
                raise PolicyError("runtime attestation input is too large")
        return bytes(content)
    finally:
        os.close(descriptor)


def _load_canonical_attestation_json(
    path: Path,
    *,
    expected_mode: int,
    enforce_root_ownership: bool,
) -> dict[str, Any]:
    raw = _read_attestation_file(
        path,
        expected_mode=expected_mode,
        enforce_root_ownership=enforce_root_ownership,
    )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("runtime attestation input is invalid JSON") from exc
    if not isinstance(payload, dict) or raw != _canonical_json_bytes(payload) + b"\n":
        raise PolicyError("runtime attestation input is not canonical")
    return payload


def _runtime_proof_base(
    root: Path,
    profile: Profile,
    sandbox: str,
    candidate_sha: str,
) -> Path:
    _sandbox_account(profile, sandbox)
    return root / _RUNTIME_PROOF_RELATIVE / profile.cluster / sandbox / candidate_sha


def _runtime_proof_high_water_path(
    root: Path,
    profile: Profile,
    sandbox: str,
) -> Path:
    _sandbox_account(profile, sandbox)
    return root / _RUNTIME_PROOF_HIGH_WATER_RELATIVE / profile.cluster / f"{sandbox}.json"


def _runtime_proof_transaction_path(
    root: Path,
    profile: Profile,
    sandbox: str,
) -> Path:
    _sandbox_account(profile, sandbox)
    return root / _RUNTIME_PROOF_TRANSACTION_RELATIVE / profile.cluster / f"{sandbox}.json"


@contextmanager
def _runtime_proof_lock(
    root: Path,
    profile: Profile,
    sandbox: str,
    *,
    enforce_root_ownership: bool,
) -> Iterator[None]:
    transaction = _runtime_proof_transaction_path(root, profile, sandbox)
    lock_path = transaction.with_suffix(".lock")
    with _persistent_private_lock(
        lock_path,
        enforce_root_ownership=enforce_root_ownership,
    ):
        yield


def _path_exists_without_following(path: Path) -> bool:
    try:
        path.lstat()
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise PolicyError("runtime proof path is unavailable") from exc
    return True


def _rename_noreplace(source: Path, target: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise PolicyError("atomic no-replace runtime proof publication is unavailable")
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(target),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error == errno.EEXIST:
            raise FileExistsError(target)
        raise PolicyError("atomic no-replace runtime proof publication failed")


def _fetch_runtime_proof_source(
    node: str,
    expected_hostname: str,
    artifact_id: str,
    *,
    expected_mode: int,
) -> bytes:
    fields = artifact_id.split("/")
    if (
        len(fields) != 7
        or fields[:2] != ["runtime-proof", "v1"]
        or _REGISTRY.RUNTIME_ID_RE.fullmatch(fields[2]) is None
        or _CANDIDATE_RE.fullmatch(fields[3]) is None
        or _CANDIDATE_RE.fullmatch(fields[4]) is None
        or fields[5] != "artifact"
        or fields[6] not in _RUNTIME_PROOF_FILE_NAMES - {"manifest.json"}
        or expected_mode not in {0o600, 0o644}
        or expected_mode != (0o600 if fields[6] in {"combined.json", "fleet.json"} else 0o644)
    ):
        raise PolicyError("runtime proof artifact identity is invalid")
    artifact_name = fields[6]
    domain = "gb10" if artifact_name.startswith("gb10.") else "oldlab"
    expected_source = (
        _RUNTIME_PROOF_SOURCES["collector"]
        if artifact_name in {"combined.json", "fleet.json"}
        else _RUNTIME_PROOF_SOURCES[domain]
    )
    if (node, expected_hostname) != expected_source:
        raise PolicyError("runtime proof artifact source is invalid")
    body: dict[str, Any] = {
        "schema_version": 1,
        "action": "export-runtime-proof-artifact",
        "node": node,
        "domain": domain,
        "sandbox": fields[2],
        "candidate_sha": fields[3],
        "candidate_tree": fields[4],
        "payload_kind": "runtime-proof-artifact-id",
        "payload_sha256": hashlib.sha256(artifact_id.encode("ascii")).hexdigest(),
        "payload_base64": base64.b64encode(artifact_id.encode("ascii")).decode("ascii"),
        "prior_request_id": None,
    }
    body["request_id"] = hashlib.sha256(_canonical_json_bytes(body) + b"\n").hexdigest()
    envelope = _canonical_json_bytes(body) + b"\n"
    output = _run_bounded_stdout(
        (
            "/usr/bin/python3",
            "-I",
            str(_NODE_TRANSPORT),
            "invoke",
            "--node",
            node,
            "--verb",
            "check",
        ),
        input_bytes=envelope,
        timeout=120,
        max_bytes=1536 * 1024,
    )
    try:
        outer = json.loads(output)
        payload = outer["result"]
        encoded = payload["content_base64"]
        content = base64.b64decode(encoded, validate=True)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise PolicyError("runtime proof source returned invalid metadata") from exc
    if (
        not isinstance(outer, dict)
        or set(outer) != {"schema_version", "request_id", "status", "result"}
        or outer["schema_version"] != 1
        or output != _canonical_json_bytes(outer) + b"\n"
        or outer["request_id"] != body["request_id"]
        or outer["status"] != "succeeded"
        or not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "operation",
            "artifact_id",
            "artifact_name",
            "node",
            "hostname",
            "domain",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "content_size",
            "content_sha256",
            "content_base64",
        }
        or payload["schema_version"] != 1
        or payload["operation"] != "export-runtime-proof-artifact"
        or payload["artifact_id"] != artifact_id
        or payload["artifact_name"] != artifact_name
        or payload["node"] != node
        or payload["hostname"] != expected_hostname
        or payload["domain"] != domain
        or payload["sandbox"] != fields[2]
        or payload["candidate_sha"] != fields[3]
        or payload["candidate_tree"] != fields[4]
        or not isinstance(encoded, str)
        or len(content) > 1024 * 1024
        or payload["content_size"] != len(content)
        or payload["content_sha256"] != hashlib.sha256(content).hexdigest()
    ):
        raise PolicyError("runtime proof source binding drifted")
    return content


def _verify_ed25519_signature(content: bytes, encoded_signature: bytes, key: bytes) -> bytes:
    try:
        signature = base64.b64decode(encoded_signature.strip(), validate=True)
    except ValueError as exc:
        raise PolicyError("runtime domain attestation signature is invalid") from exc
    if encoded_signature != base64.b64encode(signature) + b"\n":
        raise PolicyError("runtime domain attestation signature is not canonical")
    with tempfile.TemporaryDirectory() as temporary:
        temporary_root = Path(temporary)
        payload_path = temporary_root / "payload.json"
        signature_path = temporary_root / "payload.sig"
        key_path = temporary_root / "public.pem"
        payload_path.write_bytes(content)
        signature_path.write_bytes(signature)
        key_path.write_bytes(key)
        description = _run(
            ("openssl", "pkey", "-pubin", "-in", str(key_path), "-text_pub", "-noout"),
        )
        if "ED25519" not in description.upper():
            raise PolicyError("runtime domain attestation key is not Ed25519")
        _run(
            (
                "openssl",
                "pkeyutl",
                "-verify",
                "-rawin",
                "-pubin",
                "-inkey",
                str(key_path),
                "-in",
                str(payload_path),
                "-sigfile",
                str(signature_path),
            ),
        )
    return signature


def _runtime_proof_source_specs(
    sandbox: str,
    candidate_sha: str,
    candidate_tree: str,
) -> dict[str, tuple[str, str, str, int]]:
    collector_target, collector_host = _RUNTIME_PROOF_SOURCES["collector"]
    oldlab_target, oldlab_host = _RUNTIME_PROOF_SOURCES["oldlab"]
    gb10_target, gb10_host = _RUNTIME_PROOF_SOURCES["gb10"]
    prefix = f"runtime-proof/v1/{sandbox}/{candidate_sha}/{candidate_tree}/artifact"
    return {
        "combined.json": (
            collector_target,
            collector_host,
            f"{prefix}/combined.json",
            0o600,
        ),
        "fleet.json": (
            collector_target,
            collector_host,
            f"{prefix}/fleet.json",
            0o600,
        ),
        "oldlab.json": (oldlab_target, oldlab_host, f"{prefix}/oldlab.json", 0o644),
        "oldlab.sig": (oldlab_target, oldlab_host, f"{prefix}/oldlab.sig", 0o644),
        "oldlab.pub": (
            oldlab_target,
            oldlab_host,
            f"{prefix}/oldlab.pub",
            0o644,
        ),
        "gb10.json": (gb10_target, gb10_host, f"{prefix}/gb10.json", 0o644),
        "gb10.sig": (gb10_target, gb10_host, f"{prefix}/gb10.sig", 0o644),
        "gb10.pub": (
            gb10_target,
            gb10_host,
            f"{prefix}/gb10.pub",
            0o644,
        ),
    }


def _prevalidate_runtime_proof_sources(
    content: Mapping[str, bytes],
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_tree: str,
    require_fresh: bool = True,
) -> dict[str, Any]:
    parsed: dict[str, dict[str, Any]] = {}
    for name in ("combined.json", "fleet.json", "oldlab.json", "gb10.json"):
        try:
            payload = json.loads(content[name])
        except (KeyError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PolicyError("runtime proof source JSON is invalid") from exc
        if not isinstance(payload, dict) or content[name] != _canonical_json_bytes(payload) + b"\n":
            raise PolicyError("runtime proof source JSON is not canonical")
        parsed[name] = payload
    receipt = parsed["combined.json"]
    receipt_unsigned = dict(receipt)
    receipt_digest = receipt_unsigned.pop("payload_sha256", None)
    domains = receipt.get("domains")
    fleet_reference = receipt.get("fleet_attestation")
    collector = receipt.get("collector")
    now = datetime.now(UTC)
    if not isinstance(collector, dict):
        raise PolicyError("runtime proof combined collector is invalid")
    try:
        collected_at = datetime.fromisoformat(str(collector["collected_at"]))
        receipt_expires_at = datetime.fromisoformat(str(collector["expires_at"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise PolicyError("runtime proof combined timestamps are invalid") from exc
    if (
        set(receipt)
        != {
            "schema_version",
            "kind",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "collector",
            "fleet_attestation",
            "domains",
            "payload_sha256",
        }
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.developer-runtime-combined-activation"
        or receipt.get("sandbox") != sandbox
        or receipt.get("candidate_sha") != candidate_sha
        or receipt.get("candidate_tree") != candidate_tree
        or receipt_digest != hashlib.sha256(_canonical_json_bytes(receipt_unsigned)).hexdigest()
        or not isinstance(domains, dict)
        or set(domains) != {"oldlab", "gb10"}
        or not isinstance(fleet_reference, dict)
        or set(collector) != {"hostname", "collected_at", "expires_at"}
        or collector.get("hostname") != _RUNTIME_PROOF_SOURCES["collector"][1]
        or collected_at.tzinfo is None
        or receipt_expires_at.tzinfo is None
        or not timedelta(0)
        < receipt_expires_at.astimezone(UTC) - collected_at.astimezone(UTC)
        <= timedelta(minutes=15)
        or (
            require_fresh
            and (
                collected_at.astimezone(UTC) > now + timedelta(seconds=30)
                or receipt_expires_at.astimezone(UTC) <= now
            )
        )
    ):
        raise PolicyError("runtime proof combined source binding drifted")
    expected_fleet_path = _FLEET_ATTESTATION_ROOT / sandbox / candidate_sha / "fleet.json"
    fleet = parsed["fleet.json"]
    fleet_unsigned = dict(fleet)
    fleet_digest = fleet_unsigned.pop("payload_sha256", None)
    try:
        fleet_generated_at = datetime.fromisoformat(
            str(fleet.get("generated_at")).replace("Z", "+00:00")
        )
        fleet_expires_at = datetime.fromisoformat(
            str(fleet.get("expires_at")).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise PolicyError("runtime proof fleet timestamps are invalid") from exc
    if (
        fleet_reference
        != {
            "path": str(expected_fleet_path),
            "payload_sha256": fleet_digest,
            "generated_at": fleet.get("generated_at"),
            "expires_at": fleet.get("expires_at"),
        }
        or fleet_digest
        != "sha256:" + hashlib.sha256(_canonical_json_bytes(fleet_unsigned)).hexdigest()
        or fleet.get("candidate_sha") != candidate_sha
        or fleet.get("eligible_nodes") != list(_RUNTIME_FLEET_NODES)
        or not isinstance(fleet.get("nodes"), dict)
        or set(fleet["nodes"]) != set(_RUNTIME_FLEET_NODES)
        or fleet_generated_at.tzinfo is None
        or fleet_expires_at.tzinfo is None
        or (require_fresh and fleet_expires_at.astimezone(UTC) <= now)
    ):
        raise PolicyError("runtime proof fleet source binding drifted")
    domain_identity: dict[str, dict[str, Any]] = {}
    for domain_name in ("oldlab", "gb10"):
        row = domains.get(domain_name)
        manifest = parsed[f"{domain_name}.json"]
        manifest_raw = content[f"{domain_name}.json"]
        signature = _verify_ed25519_signature(
            manifest_raw,
            content[f"{domain_name}.sig"],
            content[f"{domain_name}.pub"],
        )
        unsigned = dict(manifest)
        payload_digest = unsigned.pop("payload_sha256", None)
        publisher = manifest.get("publisher")
        candidate = manifest.get("candidate")
        runtime_env = manifest.get("runtime_env")
        eligible_peers = manifest.get("eligible_peers")
        expected_parent = _DOMAIN_RUNTIME_ATTESTATION_ROOT / sandbox / candidate_sha
        if (
            not isinstance(row, dict)
            or set(row)
            != {
                "manifest_path",
                "signature_path",
                "payload_sha256",
                "signature_sha256",
                "key_id",
                "generation",
                "published_at",
                "expires_at",
            }
            or type(row.get("generation")) is not int
            or row["generation"] < 1
            or any(
                re.fullmatch(r"[0-9a-f]{64}", str(row.get(field))) is None
                for field in ("payload_sha256", "signature_sha256", "key_id")
            )
            or row.get("manifest_path") != str(expected_parent / f"{domain_name}.json")
            or row.get("signature_path") != str(expected_parent / f"{domain_name}.sig")
            or payload_digest != row.get("payload_sha256")
            or payload_digest != hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()
            or hashlib.sha256(signature).hexdigest() != row.get("signature_sha256")
            or not isinstance(publisher, dict)
            or publisher.get("hostname") != _RUNTIME_PROOF_SOURCES[domain_name][1]
            or publisher.get("generation") != row.get("generation")
            or publisher.get("published_at") != row.get("published_at")
            or publisher.get("expires_at") != row.get("expires_at")
            or publisher.get("signature_algorithm") != "ed25519"
            or publisher.get("key_id") != row.get("key_id")
            or publisher.get("key_id") != hashlib.sha256(content[f"{domain_name}.pub"]).hexdigest()
            or manifest.get("schema_version") != 1
            or manifest.get("kind") != "loom.developer-runtime-domain-attestation"
            or manifest.get("domain") != domain_name
            or manifest.get("sandbox") != sandbox
            or not isinstance(candidate, dict)
            or candidate.get("sha") != candidate_sha
            or candidate.get("tree") != candidate_tree
            or not isinstance(candidate.get("path"), str)
            or not isinstance(runtime_env, dict)
            or not isinstance(runtime_env.get("path"), str)
            or manifest.get("fleet_attestation") != fleet_reference
            or not isinstance(eligible_peers, list)
            or [item.get("hostname") for item in eligible_peers if isinstance(item, dict)]
            != list(_RUNTIME_DOMAIN_HOSTS[domain_name])
            or len(eligible_peers) != len(_RUNTIME_DOMAIN_HOSTS[domain_name])
            or any(
                not isinstance(item, dict)
                or set(item) != {"hostname", "candidate_inode", "env_inode", "result"}
                or type(item.get("candidate_inode")) is not int
                or type(item.get("env_inode")) is not int
                or item.get("result") != "verified"
                for item in eligible_peers
            )
            or len({item["candidate_inode"] for item in eligible_peers}) != 1
            or len({item["env_inode"] for item in eligible_peers}) != 1
        ):
            raise PolicyError("runtime proof domain source binding drifted")
        domain_identity[domain_name] = {
            "generation": row["generation"],
            "payload_sha256": row["payload_sha256"],
            "signature_sha256": row["signature_sha256"],
            "key_id": row["key_id"],
        }
    return {
        "receipt_sha256": receipt_digest,
        "collected_at": collector.get("collected_at"),
        "domains": domain_identity,
    }


def _validate_runtime_proof_bundle_directory(
    directory: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_tree: str,
    bundle_id: str,
    enforce_root_ownership: bool,
    require_fresh: bool,
) -> dict[str, Any]:
    _prepare_private_directory(
        directory,
        enforce_root_ownership=enforce_root_ownership,
        create=False,
    )
    try:
        names = {item.name for item in directory.iterdir()}
    except OSError as exc:
        raise PolicyError("runtime proof bundle is unreadable") from exc
    if names != _RUNTIME_PROOF_FILE_NAMES:
        raise PolicyError("runtime proof bundle is not closed-world")
    local_bytes = {
        name: _read_attestation_file(
            directory / name,
            expected_mode=0o600,
            enforce_root_ownership=enforce_root_ownership,
        )
        for name in _RUNTIME_PROOF_FILE_NAMES
    }
    sources = {name: local_bytes[name] for name in local_bytes if name != "manifest.json"}
    validated = _prevalidate_runtime_proof_sources(
        sources,
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        require_fresh=require_fresh,
    )
    specs = _runtime_proof_source_specs(
        sandbox,
        candidate_sha,
        candidate_tree,
    )
    file_rows = {
        name: {
            "source_node": target,
            "source_hostname": hostname,
            "transport_artifact_id": path,
            "source_mode": f"{mode:04o}",
            "sha256": hashlib.sha256(local_bytes[name]).hexdigest(),
        }
        for name, (target, hostname, path, mode) in specs.items()
    }
    try:
        manifest = json.loads(local_bytes["manifest.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("runtime proof bundle manifest is invalid") from exc
    if (
        not isinstance(manifest, dict)
        or local_bytes["manifest.json"] != _canonical_json_bytes(manifest) + b"\n"
    ):
        raise PolicyError("runtime proof bundle manifest is not canonical")
    unsigned = dict(manifest)
    manifest_digest = unsigned.pop("payload_sha256", None)
    if (
        set(manifest)
        != {
            "schema_version",
            "kind",
            "cluster",
            "submit_node",
            "submit_hostname",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "receipt_sha256",
            "created_at",
            "files",
            "payload_sha256",
        }
        or manifest.get("schema_version") != 1
        or manifest.get("kind") != "loom.developer-runtime-proof-bundle"
        or manifest.get("cluster") != profile.cluster
        or manifest.get("submit_node") != profile.submit_host
        or manifest.get("submit_hostname") != profile.host_aliases[profile.submit_host]
        or manifest.get("sandbox") != sandbox
        or manifest.get("candidate_sha") != candidate_sha
        or manifest.get("candidate_tree") != candidate_tree
        or manifest.get("receipt_sha256") != validated["receipt_sha256"]
        or manifest.get("created_at") != validated["collected_at"]
        or manifest.get("files") != file_rows
        or manifest_digest != bundle_id
        or manifest_digest != hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()
        or directory.name not in {bundle_id, f".stage-{bundle_id}"}
    ):
        raise PolicyError("runtime proof bundle manifest binding drifted")
    return validated


def _validate_runtime_proof_high_water(
    payload: Mapping[str, Any],
    profile: Profile,
    *,
    sandbox: str,
) -> dict[str, dict[str, Any]]:
    domains = payload.get("domains")
    if (
        set(payload)
        != {
            "schema_version",
            "kind",
            "cluster",
            "submit_node",
            "submit_hostname",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "bundle_id",
            "receipt_sha256",
            "domains",
            "updated_at",
        }
        or payload.get("schema_version") != 1
        or payload.get("kind") != "loom.developer-runtime-proof-high-water"
        or payload.get("cluster") != profile.cluster
        or payload.get("submit_node") != profile.submit_host
        or payload.get("submit_hostname") != profile.host_aliases[profile.submit_host]
        or payload.get("sandbox") != sandbox
        or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_sha"))) is None
        or _CANDIDATE_RE.fullmatch(str(payload.get("candidate_tree"))) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("bundle_id"))) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("receipt_sha256"))) is None
        or not isinstance(payload.get("updated_at"), str)
        or not isinstance(domains, dict)
        or set(domains) != {"oldlab", "gb10"}
    ):
        raise PolicyError("runtime proof high-water is invalid")
    for row in domains.values():
        if (
            not isinstance(row, dict)
            or set(row) != {"generation", "payload_sha256", "signature_sha256", "key_id"}
            or type(row.get("generation")) is not int
            or row["generation"] < 1
            or any(
                re.fullmatch(r"[0-9a-f]{64}", str(row.get(field))) is None
                for field in ("payload_sha256", "signature_sha256", "key_id")
            )
        ):
            raise PolicyError("runtime proof high-water is invalid")
    return domains


def _advance_runtime_proof_high_water(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_tree: str,
    bundle_id: str,
    validated: Mapping[str, Any],
    enforce_root_ownership: bool,
) -> None:
    path = _runtime_proof_high_water_path(root, profile, sandbox)
    _prepare_private_directory(
        path.parent,
        enforce_root_ownership=enforce_root_ownership,
        create=True,
    )
    previous = (
        _load_canonical_attestation_json(
            path,
            expected_mode=0o600,
            enforce_root_ownership=enforce_root_ownership,
        )
        if _path_exists_without_following(path)
        else None
    )
    current_domains = validated.get("domains")
    if not isinstance(current_domains, dict) or set(current_domains) != {"oldlab", "gb10"}:
        raise PolicyError("runtime proof verified domains are invalid")
    if previous is not None:
        previous_domains = _validate_runtime_proof_high_water(
            previous,
            profile,
            sandbox=sandbox,
        )
        for domain_name, current in current_domains.items():
            prior = previous_domains[domain_name]
            if current["generation"] < prior["generation"]:
                raise PolicyError("runtime proof domain generation regressed")
            if current["generation"] == prior["generation"] and current != prior:
                raise PolicyError("runtime proof domain generation identity changed")
            if current["key_id"] != prior["key_id"]:
                raise PolicyError("runtime proof domain key rotation is not authorized")
    high_water = {
        "schema_version": 1,
        "kind": "loom.developer-runtime-proof-high-water",
        "cluster": profile.cluster,
        "submit_node": profile.submit_host,
        "submit_hostname": profile.host_aliases[profile.submit_host],
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "bundle_id": bundle_id,
        "receipt_sha256": validated["receipt_sha256"],
        "domains": current_domains,
        "updated_at": validated["collected_at"],
    }
    _atomic_write(
        path,
        _canonical_json_bytes(high_water).decode("utf-8") + "\n",
        mode=0o600,
    )


def _validate_runtime_proof_transaction(
    transaction: Mapping[str, Any],
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
) -> tuple[Path, Path]:
    candidate_sha = transaction.get("candidate_sha")
    candidate_tree = transaction.get("candidate_tree")
    bundle_id = transaction.get("bundle_id")
    receipt_sha256 = transaction.get("receipt_sha256")
    if (
        set(transaction)
        != {
            "schema_version",
            "kind",
            "cluster",
            "submit_node",
            "submit_hostname",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "bundle_id",
            "receipt_sha256",
            "stage",
            "final",
            "phase",
        }
        or transaction.get("schema_version") != 1
        or transaction.get("kind") != "loom.developer-runtime-proof-transaction"
        or transaction.get("cluster") != profile.cluster
        or transaction.get("submit_node") != profile.submit_host
        or transaction.get("submit_hostname") != profile.host_aliases[profile.submit_host]
        or transaction.get("sandbox") != sandbox
        or _CANDIDATE_RE.fullmatch(str(candidate_sha)) is None
        or _CANDIDATE_RE.fullmatch(str(candidate_tree)) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(bundle_id)) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(receipt_sha256)) is None
        or transaction.get("phase") != "prepared"
    ):
        raise PolicyError("foreign runtime proof transaction exists")
    base = _runtime_proof_base(root, profile, sandbox, str(candidate_sha))
    stage = base / f".stage-{bundle_id}"
    final = base / str(bundle_id)
    if transaction.get("stage") != str(stage) or transaction.get("final") != str(final):
        raise PolicyError("foreign runtime proof transaction exists")
    return stage, final


def _finish_runtime_proof_transaction(path: Path, transaction: Mapping[str, Any]) -> None:
    if _load_journal(path) != transaction:
        raise PolicyError("runtime proof transaction changed during recovery")
    path.unlink()
    _fsync_directory(path.parent)


def _clean_owned_runtime_proof_stage(
    stage: Path,
    *,
    enforce_root_ownership: bool,
) -> None:
    _prepare_private_directory(
        stage,
        enforce_root_ownership=enforce_root_ownership,
        create=False,
    )
    items = list(stage.iterdir())
    if any(item.name not in _RUNTIME_PROOF_FILE_NAMES for item in items):
        raise PolicyError("foreign runtime proof stage exists")
    for item in items:
        _read_attestation_file(
            item,
            expected_mode=0o600,
            enforce_root_ownership=enforce_root_ownership,
        )
    for item in items:
        item.unlink()
    _fsync_directory(stage)
    stage.rmdir()
    _fsync_directory(stage.parent)


def _recover_runtime_proof_transaction(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    enforce_root_ownership: bool,
) -> None:
    transaction_path = _runtime_proof_transaction_path(root, profile, sandbox)
    transaction = _load_journal(transaction_path)
    if transaction is None:
        return
    stage, final = _validate_runtime_proof_transaction(
        transaction,
        root,
        profile,
        sandbox=sandbox,
    )
    high_water_path = _runtime_proof_high_water_path(root, profile, sandbox)
    high_water: dict[str, Any] | None = None
    high_water_matches_transaction = False
    if _path_exists_without_following(high_water_path):
        high_water = _load_canonical_attestation_json(
            high_water_path,
            expected_mode=0o600,
            enforce_root_ownership=enforce_root_ownership,
        )
        _validate_runtime_proof_high_water(high_water, profile, sandbox=sandbox)
        high_water_matches_transaction = high_water["bundle_id"] == transaction["bundle_id"]
        if high_water_matches_transaction:
            if (
                high_water["candidate_sha"] != transaction["candidate_sha"]
                or high_water["candidate_tree"] != transaction["candidate_tree"]
                or high_water["receipt_sha256"] != transaction["receipt_sha256"]
            ):
                raise PolicyError("runtime proof transaction high-water binding drifted")
        else:
            current = _runtime_proof_base(
                root,
                profile,
                sandbox,
                str(high_water["candidate_sha"]),
            ) / str(high_water["bundle_id"])
            _validate_runtime_proof_bundle_directory(
                current,
                profile,
                sandbox=sandbox,
                candidate_sha=str(high_water["candidate_sha"]),
                candidate_tree=str(high_water["candidate_tree"]),
                bundle_id=str(high_water["bundle_id"]),
                enforce_root_ownership=enforce_root_ownership,
                require_fresh=False,
            )
    stage_exists = _path_exists_without_following(stage)
    final_exists = _path_exists_without_following(final)
    if stage_exists and final_exists:
        raise PolicyError("runtime proof transaction collides with stage and final")
    validated: dict[str, Any] | None = None
    if final_exists:
        validated = _validate_runtime_proof_bundle_directory(
            final,
            profile,
            sandbox=sandbox,
            candidate_sha=str(transaction["candidate_sha"]),
            candidate_tree=str(transaction["candidate_tree"]),
            bundle_id=str(transaction["bundle_id"]),
            enforce_root_ownership=enforce_root_ownership,
            require_fresh=False,
        )
    elif stage_exists:
        _prepare_private_directory(
            stage,
            enforce_root_ownership=enforce_root_ownership,
            create=False,
        )
        names = {item.name for item in stage.iterdir()}
        if any(name not in _RUNTIME_PROOF_FILE_NAMES for name in names):
            raise PolicyError("foreign runtime proof stage exists")
        if names == _RUNTIME_PROOF_FILE_NAMES:
            validated = _validate_runtime_proof_bundle_directory(
                stage,
                profile,
                sandbox=sandbox,
                candidate_sha=str(transaction["candidate_sha"]),
                candidate_tree=str(transaction["candidate_tree"]),
                bundle_id=str(transaction["bundle_id"]),
                enforce_root_ownership=enforce_root_ownership,
                require_fresh=False,
            )
            try:
                _rename_noreplace(stage, final)
            except FileExistsError as exc:
                raise PolicyError("foreign runtime proof final exists") from exc
            _fsync_directory(final.parent)
            validated = _validate_runtime_proof_bundle_directory(
                final,
                profile,
                sandbox=sandbox,
                candidate_sha=str(transaction["candidate_sha"]),
                candidate_tree=str(transaction["candidate_tree"]),
                bundle_id=str(transaction["bundle_id"]),
                enforce_root_ownership=enforce_root_ownership,
                require_fresh=False,
            )
        else:
            if high_water_matches_transaction:
                raise PolicyError("runtime proof current transaction stage is incomplete")
            _clean_owned_runtime_proof_stage(
                stage,
                enforce_root_ownership=enforce_root_ownership,
            )
    elif high_water_matches_transaction:
        raise PolicyError("runtime proof current transaction has no recoverable bundle")
    if validated is not None:
        _advance_runtime_proof_high_water(
            root,
            profile,
            sandbox=sandbox,
            candidate_sha=str(transaction["candidate_sha"]),
            candidate_tree=str(transaction["candidate_tree"]),
            bundle_id=str(transaction["bundle_id"]),
            validated=validated,
            enforce_root_ownership=enforce_root_ownership,
        )
    _finish_runtime_proof_transaction(transaction_path, transaction)


def materialize_runtime_proof(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_tree: str,
    fetcher: Any = _fetch_runtime_proof_source,
) -> dict[str, Any]:
    _sandbox_account(profile, sandbox)
    enforce_root_ownership = root == Path("/")
    if enforce_root_ownership:
        if os.geteuid() != 0:
            raise PolicyError("runtime proof materialization requires root")
        host = _canonical_host()
        if _slurm_node_for_host(profile, host) != profile.submit_host:
            raise PolicyError("runtime proof must be materialized on the exact submit host")
    if (
        _CANDIDATE_RE.fullmatch(candidate_sha) is None
        or _CANDIDATE_RE.fullmatch(candidate_tree) is None
    ):
        raise PolicyError("runtime proof candidate binding is invalid")
    with _runtime_proof_lock(
        root,
        profile,
        sandbox,
        enforce_root_ownership=enforce_root_ownership,
    ):
        _recover_runtime_proof_transaction(
            root,
            profile,
            sandbox=sandbox,
            enforce_root_ownership=enforce_root_ownership,
        )
        specs = _runtime_proof_source_specs(
            sandbox,
            candidate_sha,
            candidate_tree,
        )
        fetched = {
            name: fetcher(target, hostname, path, expected_mode=mode)
            for name, (target, hostname, path, mode) in specs.items()
        }
        validated = _prevalidate_runtime_proof_sources(
            fetched,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
        )
        file_rows = {
            name: {
                "source_node": target,
                "source_hostname": hostname,
                "transport_artifact_id": path,
                "source_mode": f"{mode:04o}",
                "sha256": hashlib.sha256(fetched[name]).hexdigest(),
            }
            for name, (target, hostname, path, mode) in specs.items()
        }
        manifest: dict[str, Any] = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-proof-bundle",
            "cluster": profile.cluster,
            "submit_node": profile.submit_host,
            "submit_hostname": profile.host_aliases[profile.submit_host],
            "sandbox": sandbox,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "receipt_sha256": validated["receipt_sha256"],
            "created_at": validated["collected_at"],
            "files": file_rows,
        }
        bundle_id = hashlib.sha256(_canonical_json_bytes(manifest)).hexdigest()
        manifest["payload_sha256"] = bundle_id
        manifest_bytes = _canonical_json_bytes(manifest) + b"\n"
        fetched["manifest.json"] = manifest_bytes
        base = _runtime_proof_base(root, profile, sandbox, candidate_sha)
        _prepare_private_directory(
            base,
            enforce_root_ownership=enforce_root_ownership,
            create=True,
        )
        stage = base / f".stage-{bundle_id}"
        final = base / bundle_id
        transaction_path = _runtime_proof_transaction_path(root, profile, sandbox)
        transaction = {
            "schema_version": 1,
            "kind": "loom.developer-runtime-proof-transaction",
            "cluster": profile.cluster,
            "submit_node": profile.submit_host,
            "submit_hostname": profile.host_aliases[profile.submit_host],
            "sandbox": sandbox,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "bundle_id": bundle_id,
            "receipt_sha256": validated["receipt_sha256"],
            "stage": str(stage),
            "final": str(final),
            "phase": "prepared",
        }
        existing_transaction = _load_journal(transaction_path)
        if existing_transaction is not None:
            raise PolicyError("runtime proof transaction recovery did not converge")
        _write_journal(transaction_path, transaction)
        if _path_exists_without_following(final):
            _prepare_private_directory(
                final,
                enforce_root_ownership=enforce_root_ownership,
                create=False,
            )
            if _path_exists_without_following(stage):
                raise PolicyError("runtime proof final and stage both exist")
            existing_manifest = _read_attestation_file(
                final / "manifest.json",
                expected_mode=0o600,
                enforce_root_ownership=enforce_root_ownership,
            )
            if existing_manifest != manifest_bytes:
                raise PolicyError("foreign runtime proof final exists")
        else:
            if _path_exists_without_following(stage):
                _prepare_private_directory(
                    stage,
                    enforce_root_ownership=enforce_root_ownership,
                    create=False,
                )
                if any(item.name not in _RUNTIME_PROOF_FILE_NAMES for item in stage.iterdir()):
                    raise PolicyError("foreign runtime proof stage exists")
            else:
                stage.mkdir(mode=0o700)
                _prepare_private_directory(
                    stage,
                    enforce_root_ownership=enforce_root_ownership,
                    create=False,
                )
            for name, content in fetched.items():
                target = stage / name
                if _path_exists_without_following(target):
                    if (
                        _read_attestation_file(
                            target,
                            expected_mode=0o600,
                            enforce_root_ownership=enforce_root_ownership,
                        )
                        != content
                    ):
                        raise PolicyError("foreign runtime proof stage content exists")
                else:
                    try:
                        text_content = content.decode("utf-8")
                    except UnicodeDecodeError as exc:
                        raise PolicyError("runtime proof source is not textual") from exc
                    _atomic_write(target, text_content, mode=0o600)
            _fsync_directory(stage)
        if not _path_exists_without_following(final):
            _validate_runtime_proof_bundle_directory(
                stage,
                profile,
                sandbox=sandbox,
                candidate_sha=candidate_sha,
                candidate_tree=candidate_tree,
                bundle_id=bundle_id,
                enforce_root_ownership=enforce_root_ownership,
                require_fresh=True,
            )
            try:
                _rename_noreplace(stage, final)
            except FileExistsError as exc:
                raise PolicyError("foreign runtime proof final exists") from exc
            _fsync_directory(base)
        published = _validate_runtime_proof_bundle_directory(
            final,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            bundle_id=bundle_id,
            enforce_root_ownership=enforce_root_ownership,
            require_fresh=True,
        )
        _advance_runtime_proof_high_water(
            root,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            bundle_id=bundle_id,
            validated=published,
            enforce_root_ownership=enforce_root_ownership,
        )
        binding = _runtime_attestation_binding(
            root,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            candidate_root=Path("/unbound-at-materialization"),
            worker_env=Path("/unbound-at-materialization"),
            enforce_root_ownership=enforce_root_ownership,
        )
        _finish_runtime_proof_transaction(transaction_path, transaction)
        return {
            "schema_version": 1,
            "operation": "materialize-runtime-proof",
            "cluster": profile.cluster,
            "submit_node": profile.submit_host,
            "sandbox": sandbox,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "bundle_id": bundle_id,
            "proof_path": binding["proof_path"],
            "receipt_sha256": binding["receipt_sha256"],
        }


def _runtime_attestation_binding(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_tree: str,
    candidate_root: Path,
    worker_env: Path,
    enforce_root_ownership: bool,
) -> dict[str, Any]:
    _sandbox_account(profile, sandbox)
    high_water_path = _runtime_proof_high_water_path(root, profile, sandbox)
    high_water = _load_canonical_attestation_json(
        high_water_path,
        expected_mode=0o600,
        enforce_root_ownership=enforce_root_ownership,
    )
    if (
        set(high_water)
        != {
            "schema_version",
            "kind",
            "cluster",
            "submit_node",
            "submit_hostname",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "bundle_id",
            "receipt_sha256",
            "domains",
            "updated_at",
        }
        or high_water.get("schema_version") != 1
        or high_water.get("kind") != "loom.developer-runtime-proof-high-water"
        or high_water.get("cluster") != profile.cluster
        or high_water.get("submit_node") != profile.submit_host
        or high_water.get("submit_hostname") != profile.host_aliases[profile.submit_host]
        or high_water.get("sandbox") != sandbox
        or high_water.get("candidate_sha") != candidate_sha
        or high_water.get("candidate_tree") != candidate_tree
        or re.fullmatch(r"[0-9a-f]{64}", str(high_water.get("bundle_id"))) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(high_water.get("receipt_sha256"))) is None
    ):
        raise PolicyError("runtime proof high-water binding drifted")
    proof_path = (
        _runtime_proof_base(root, profile, sandbox, candidate_sha)
        / str(high_water["bundle_id"])
        / "manifest.json"
    )
    proof_directory = proof_path.parent
    _prepare_private_directory(
        proof_directory,
        enforce_root_ownership=enforce_root_ownership,
        create=False,
    )
    try:
        actual_names = {item.name for item in proof_directory.iterdir()}
    except OSError as exc:
        raise PolicyError("runtime proof bundle is unreadable") from exc
    if actual_names != _RUNTIME_PROOF_FILE_NAMES:
        raise PolicyError("runtime proof bundle is not closed-world")
    proof = _load_canonical_attestation_json(
        proof_path,
        expected_mode=0o600,
        enforce_root_ownership=enforce_root_ownership,
    )
    proof_unsigned = dict(proof)
    bundle_id = proof_unsigned.pop("payload_sha256", None)
    if (
        set(proof)
        != {
            "schema_version",
            "kind",
            "cluster",
            "submit_node",
            "submit_hostname",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "receipt_sha256",
            "created_at",
            "files",
            "payload_sha256",
        }
        or proof.get("schema_version") != 1
        or proof.get("kind") != "loom.developer-runtime-proof-bundle"
        or proof.get("cluster") != profile.cluster
        or proof.get("submit_node") != profile.submit_host
        or proof.get("submit_hostname") != profile.host_aliases[profile.submit_host]
        or proof.get("sandbox") != sandbox
        or proof.get("candidate_sha") != candidate_sha
        or proof.get("candidate_tree") != candidate_tree
        or proof.get("receipt_sha256") != high_water["receipt_sha256"]
        or bundle_id != high_water["bundle_id"]
        or bundle_id != hashlib.sha256(_canonical_json_bytes(proof_unsigned)).hexdigest()
    ):
        raise PolicyError("runtime proof manifest binding drifted")
    file_rows = proof.get("files")
    source_specs = _runtime_proof_source_specs(
        sandbox,
        candidate_sha,
        candidate_tree,
    )
    if not isinstance(file_rows, dict) or set(file_rows) != set(source_specs):
        raise PolicyError("runtime proof file manifest is not closed-world")
    local_bytes: dict[str, bytes] = {}
    for name, (
        source_node,
        source_hostname,
        transport_artifact_id,
        source_mode,
    ) in source_specs.items():
        row = file_rows.get(name)
        if (
            not isinstance(row, dict)
            or set(row)
            != {
                "source_node",
                "source_hostname",
                "transport_artifact_id",
                "source_mode",
                "sha256",
            }
            or row.get("source_node") != source_node
            or row.get("source_hostname") != source_hostname
            or row.get("transport_artifact_id") != transport_artifact_id
            or row.get("source_mode") != f"{source_mode:04o}"
            or re.fullmatch(r"[0-9a-f]{64}", str(row.get("sha256"))) is None
        ):
            raise PolicyError("runtime proof source binding drifted")
        content = _read_attestation_file(
            proof_directory / name,
            expected_mode=0o600,
            enforce_root_ownership=enforce_root_ownership,
        )
        if hashlib.sha256(content).hexdigest() != row["sha256"]:
            raise PolicyError("runtime proof local digest drifted")
        local_bytes[name] = content
    try:
        receipt = json.loads(local_bytes["combined.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("runtime attestation receipt is invalid JSON") from exc
    if (
        not isinstance(receipt, dict)
        or local_bytes["combined.json"] != _canonical_json_bytes(receipt) + b"\n"
    ):
        raise PolicyError("runtime attestation receipt is not canonical")
    expected_top = {
        "schema_version",
        "kind",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "collector",
        "fleet_attestation",
        "domains",
        "payload_sha256",
    }
    unsigned = dict(receipt)
    digest = unsigned.pop("payload_sha256", None)
    collector = receipt.get("collector")
    now = datetime.now(UTC)
    if not isinstance(collector, dict):
        raise PolicyError("runtime attestation collector is invalid")
    try:
        collected_at = datetime.fromisoformat(str(collector["collected_at"]))
        receipt_expires_at = datetime.fromisoformat(str(collector["expires_at"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise PolicyError("runtime attestation collector timestamps are invalid") from exc
    if (
        set(receipt) != expected_top
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.developer-runtime-combined-activation"
        or receipt.get("candidate_sha") != candidate_sha
        or receipt.get("candidate_tree") != candidate_tree
        or receipt.get("sandbox") != sandbox
        or set(collector) != {"hostname", "collected_at", "expires_at"}
        or collector.get("hostname") != "trt-eai-oldlab-2"
        or collected_at.tzinfo is None
        or receipt_expires_at.tzinfo is None
        or not timedelta(0)
        < receipt_expires_at.astimezone(UTC) - collected_at.astimezone(UTC)
        <= timedelta(minutes=15)
        or collected_at.astimezone(UTC) > now + timedelta(seconds=30)
        or receipt_expires_at.astimezone(UTC) <= now
        or digest != hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()
        or digest != proof.get("receipt_sha256")
    ):
        raise PolicyError("runtime attestation receipt binding drifted")
    domains = receipt.get("domains")
    fleet_reference = receipt.get("fleet_attestation")
    if (
        not isinstance(domains, dict)
        or set(domains) != {"oldlab", "gb10"}
        or not isinstance(fleet_reference, dict)
        or set(fleet_reference) != {"path", "payload_sha256", "generated_at", "expires_at"}
    ):
        raise PolicyError("runtime attestation receipt is not closed-world")
    expected_domain_fields = {
        "manifest_path",
        "signature_path",
        "payload_sha256",
        "signature_sha256",
        "key_id",
        "generation",
        "published_at",
        "expires_at",
    }
    if any(
        not isinstance(value, dict)
        or set(value) != expected_domain_fields
        or type(value.get("generation")) is not int
        or value["generation"] < 1
        or re.fullmatch(r"[0-9a-f]{64}", str(value.get("payload_sha256"))) is None
        or re.fullmatch(r"[0-9a-f]{64}", str(value.get("signature_sha256"))) is None
        for value in domains.values()
    ):
        raise PolicyError("runtime domain attestation references are invalid")
    expected_manifest_parent = _DOMAIN_RUNTIME_ATTESTATION_ROOT / sandbox / candidate_sha
    verified_domains: dict[str, dict[str, Any]] = {}
    domain_expiries: list[datetime] = []
    for domain_name in ("oldlab", "gb10"):
        domain_row = domains[domain_name]
        manifest_path = Path(str(domain_row["manifest_path"]))
        signature_path = Path(str(domain_row["signature_path"]))
        if (
            manifest_path != expected_manifest_parent / f"{domain_name}.json"
            or signature_path != expected_manifest_parent / f"{domain_name}.sig"
        ):
            raise PolicyError("runtime domain attestation path drifted")
        manifest_raw = local_bytes[f"{domain_name}.json"]
        signature_raw = local_bytes[f"{domain_name}.sig"]
        key_raw = local_bytes[f"{domain_name}.pub"]
        try:
            manifest = json.loads(manifest_raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PolicyError("runtime domain attestation is invalid JSON") from exc
        if (
            not isinstance(manifest, dict)
            or manifest_raw != _canonical_json_bytes(manifest) + b"\n"
        ):
            raise PolicyError("runtime domain attestation is not canonical")
        manifest_unsigned = dict(manifest)
        manifest_digest = manifest_unsigned.pop("payload_sha256", None)
        signature = _verify_ed25519_signature(manifest_raw, signature_raw, key_raw)
        publisher = manifest.get("publisher")
        candidate = manifest.get("candidate")
        runtime_env = manifest.get("runtime_env")
        fleet_binding = manifest.get("fleet_attestation")
        eligible_peers = manifest.get("eligible_peers")
        expected_hosts = list(_RUNTIME_DOMAIN_HOSTS[domain_name])
        expected_publisher = _RUNTIME_PROOF_SOURCES[domain_name][1]
        if not isinstance(publisher, dict):
            raise PolicyError("runtime domain attestation binding drifted")
        try:
            domain_published_at = datetime.fromisoformat(str(publisher["published_at"]))
            domain_expires_at = datetime.fromisoformat(str(publisher["expires_at"]))
        except (KeyError, ValueError) as exc:
            raise PolicyError("runtime domain attestation timestamps are invalid") from exc
        if (
            manifest_digest != domain_row.get("payload_sha256")
            or manifest_digest
            != hashlib.sha256(_canonical_json_bytes(manifest_unsigned)).hexdigest()
            or manifest.get("schema_version") != 1
            or manifest.get("kind") != "loom.developer-runtime-domain-attestation"
            or manifest.get("domain") != domain_name
            or manifest.get("sandbox") != sandbox
            or publisher.get("hostname") != expected_publisher
            or publisher.get("generation") != domain_row.get("generation")
            or publisher.get("published_at") != domain_row.get("published_at")
            or publisher.get("expires_at") != domain_row.get("expires_at")
            or publisher.get("signature_algorithm") != "ed25519"
            or publisher.get("key_id") != domain_row.get("key_id")
            or publisher.get("key_id") != hashlib.sha256(key_raw).hexdigest()
            or domain_published_at.tzinfo is None
            or domain_expires_at.tzinfo is None
            or domain_published_at.astimezone(UTC) > now + timedelta(seconds=30)
            or domain_expires_at.astimezone(UTC) <= now
            or domain_published_at.astimezone(UTC) >= domain_expires_at.astimezone(UTC)
            or not isinstance(candidate, dict)
            or candidate.get("sha") != candidate_sha
            or candidate.get("tree") != candidate_tree
            or not isinstance(candidate.get("path"), str)
            or not isinstance(runtime_env, dict)
            or not isinstance(runtime_env.get("path"), str)
            or fleet_binding != fleet_reference
            or hashlib.sha256(signature).hexdigest() != domain_row.get("signature_sha256")
            or not isinstance(eligible_peers, list)
            or [item.get("hostname") for item in eligible_peers if isinstance(item, dict)]
            != expected_hosts
            or len(eligible_peers) != len(expected_hosts)
            or any(
                not isinstance(item, dict)
                or set(item) != {"hostname", "candidate_inode", "env_inode", "result"}
                or type(item.get("candidate_inode")) is not int
                or type(item.get("env_inode")) is not int
                or item.get("result") != "verified"
                for item in eligible_peers
            )
            or len({item["candidate_inode"] for item in eligible_peers}) != 1
            or len({item["env_inode"] for item in eligible_peers}) != 1
        ):
            raise PolicyError("runtime domain attestation binding drifted")
        domain_expiries.append(domain_expires_at.astimezone(UTC))
        verified_domains[domain_name] = {
            "manifest_digest": manifest_digest,
            "signature_digest": domain_row["signature_sha256"],
            "generation": domain_row["generation"],
            "hosts": expected_hosts,
            "candidate_path": candidate["path"],
            "runtime_env_path": runtime_env["path"],
        }
    fleet_path = Path(str(fleet_reference["path"]))
    expected_fleet_path = _FLEET_ATTESTATION_ROOT / sandbox / candidate_sha / "fleet.json"
    if fleet_path != expected_fleet_path:
        raise PolicyError("runtime fleet attestation path drifted")
    try:
        fleet = json.loads(local_bytes["fleet.json"])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("runtime fleet attestation is invalid JSON") from exc
    if (
        not isinstance(fleet, dict)
        or local_bytes["fleet.json"] != _canonical_json_bytes(fleet) + b"\n"
    ):
        raise PolicyError("runtime fleet attestation is not canonical")
    fleet_unsigned = dict(fleet)
    fleet_digest = fleet_unsigned.pop("payload_sha256", None)
    expected_fleet_digest = (
        "sha256:"
        + hashlib.sha256(
            _canonical_json_bytes(fleet_unsigned),
        ).hexdigest()
    )
    try:
        fleet_generated_at = datetime.fromisoformat(
            str(fleet["generated_at"]).replace("Z", "+00:00")
        )
        fleet_expires_at = datetime.fromisoformat(str(fleet["expires_at"]).replace("Z", "+00:00"))
    except (KeyError, ValueError) as exc:
        raise PolicyError("runtime fleet attestation timestamps are invalid") from exc
    expected_fleet_nodes = list(_RUNTIME_FLEET_NODES)
    account = _sandbox_account(profile, sandbox)
    environment_binding = profile.environment_bindings.get(account)
    dynamic_fleet_fields = {
        "env_id",
        "resource_generation",
        "registry_generation",
        "registry_payload_sha256",
        "candidate_tree",
    }
    expected_fleet_fields = {
        "schema_version",
        "sandbox",
        "candidate_sha",
        "generated_at",
        "expires_at",
        "eligible_nodes",
        "bundle_generation",
        "server",
        "nodes",
        "payload_sha256",
    }
    if environment_binding is not None:
        expected_fleet_fields |= dynamic_fleet_fields
    if (
        (environment_binding is not None and set(fleet) != expected_fleet_fields)
        or fleet_digest != fleet_reference.get("payload_sha256")
        or fleet_digest != expected_fleet_digest
        or fleet.get("generated_at") != fleet_reference.get("generated_at")
        or fleet.get("expires_at") != fleet_reference.get("expires_at")
        or fleet_generated_at.tzinfo is None
        or fleet_expires_at.tzinfo is None
        or fleet_expires_at.astimezone(UTC) <= now
        or fleet.get("candidate_sha") != candidate_sha
        or (
            environment_binding is not None
            and (
                fleet.get("env_id") != environment_binding["env_id"]
                or fleet.get("resource_generation") != environment_binding["resource_generation"]
                or type(fleet.get("registry_generation")) is not int
                or int(fleet["registry_generation"]) < 1
                or _REGISTRY.DIGEST_RE.fullmatch(
                    str(fleet.get("registry_payload_sha256")),
                )
                is None
                or fleet.get("candidate_tree") != candidate_tree
            )
        )
        or fleet.get("eligible_nodes") != expected_fleet_nodes
        or not isinstance(fleet.get("nodes"), dict)
        or set(fleet["nodes"]) != set(expected_fleet_nodes)
    ):
        raise PolicyError("runtime fleet attestation coverage drifted")
    selected_domain = {
        "trt-oldlab": "oldlab",
        "trt-gb10": "gb10",
    }.get(profile.cluster)
    if selected_domain is None:
        raise PolicyError("runtime attestation does not support this Slurm cluster")
    current_domain = verified_domains[selected_domain]
    proof_expires_at = min(
        receipt_expires_at.astimezone(UTC),
        fleet_expires_at.astimezone(UTC),
        *domain_expiries,
    )
    worker_registry_binding: dict[str, str] | None = None
    if environment_binding is not None and worker_env != Path("/unbound-at-materialization"):
        worker_env_binding = _read_private_env(worker_env)
        worker_values = _read_exact_env_values(
            worker_env,
            expected_inode=int(worker_env_binding["inode"]),
            expected_sha256=str(worker_env_binding["sha256"]),
        )
        worker_registry_binding = {
            "LOOM_WORKER_ENV_ID": str(environment_binding["env_id"]),
            "LOOM_WORKER_RESOURCE_GENERATION": str(
                environment_binding["resource_generation"],
            ),
            "LOOM_WORKER_CANDIDATE_ID": str(environment_binding["candidate_id"]),
            "LOOM_WORKER_CANDIDATE_TREE": candidate_tree,
            "LOOM_WORKER_REGISTRY_GENERATION": str(fleet["registry_generation"]),
            "LOOM_WORKER_REGISTRY_PAYLOAD_SHA256": str(
                fleet["registry_payload_sha256"],
            ),
        }
        if any(worker_values.get(key) != value for key, value in worker_registry_binding.items()):
            raise PolicyError("runtime worker env registry binding drifted")
    return {
        "proof_path": str(proof_path),
        "bundle_id": bundle_id,
        "receipt_path": str(proof_directory / "combined.json"),
        "receipt_sha256": digest,
        "receipt_collected_at": collected_at.astimezone(UTC).isoformat(),
        "receipt_expires_at": receipt_expires_at.astimezone(UTC).isoformat(),
        "proof_expires_at": proof_expires_at.isoformat(),
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "domain": selected_domain,
        "domain_payload_sha256": current_domain["manifest_digest"],
        "domain_signature_sha256": current_domain["signature_digest"],
        "domain_generation": current_domain["generation"],
        "domain_hosts": current_domain["hosts"],
        "domain_candidate_path": current_domain["candidate_path"],
        "domain_runtime_env_path": current_domain["runtime_env_path"],
        "domains": verified_domains,
        "allocation_candidate_path": str(candidate_root),
        "allocation_worker_env_path": str(worker_env),
        "fleet_payload_sha256": fleet_digest,
        "fleet_nodes": expected_fleet_nodes,
        **(
            {
                "env_id": fleet["env_id"],
                "resource_generation": fleet["resource_generation"],
                "registry_generation": fleet["registry_generation"],
                "registry_payload_sha256": fleet["registry_payload_sha256"],
            }
            if environment_binding is not None
            else {}
        ),
        **(
            {"worker_registry_binding": worker_registry_binding}
            if worker_registry_binding is not None
            else {}
        ),
    }


def _parse_sacct_rows(raw: str) -> list[list[str]]:
    rows = [line.split("|") for line in raw.splitlines() if line.strip()]
    if any(len(row) < 9 for row in rows):
        raise PolicyError("Slurm allocation probe accounting output is malformed")
    return rows


def _probe_accounting_rows(job_id: str, profile: Profile) -> list[list[str]]:
    output = _run(
        (
            "sacct",
            "-nP",
            f"--clusters={profile.cluster}",
            "-j",
            job_id,
            "--format=JobIDRaw,JobName,State,NodeList,AllocTRES,Account,User,Cluster,QOS",
        ),
        timeout=15,
    )
    if not output.strip():
        return []
    return _parse_sacct_rows(output)


def _probe_named_accounting_rows(job_name: str, profile: Profile) -> list[list[str]]:
    output = _run(
        (
            "sacct",
            "-nP",
            f"--clusters={profile.cluster}",
            f"--name={job_name}",
            "--starttime=now-1day",
            "--format=JobIDRaw,JobName,State,NodeList,AllocTRES,Account,User,Cluster,QOS",
        ),
        timeout=15,
    )
    if not output.strip():
        return []
    return _parse_sacct_rows(output)


def _normalize_probe_job_state(raw: str) -> str:
    tokens = raw.strip().upper().split(maxsplit=1)
    if not tokens:
        return ""
    token = tokens[0]
    return token.split("+", 1)[0]


def _base_job_state(rows: Sequence[Sequence[str]], job_id: str) -> str | None:
    base = next((row for row in rows if row[0] == job_id), None)
    if base is None:
        return None
    return _normalize_probe_job_state(base[2])


def _poll_probe_terminal(
    job_id: str,
    profile: Profile,
    *,
    timeout_seconds: float,
    poll_seconds: float = _ALLOCATION_POLL_SECONDS,
) -> list[list[str]]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        rows = _probe_accounting_rows(job_id, profile)
        state = _base_job_state(rows, job_id)
        if state in _TERMINAL_JOB_STATES:
            return rows
        time.sleep(poll_seconds)
    raise PolicyError("allocation probe did not reach a terminal state before timeout")


def _validate_probe_base_row(
    row: Sequence[str],
    payload: Mapping[str, Any],
    profile: Profile,
    *,
    sandbox: str,
) -> None:
    account = _sandbox_account(profile, sandbox)
    if (
        len(row) < 9
        or row[0] != str(payload.get("job_id", ""))
        or row[1] != payload.get("job_name")
        or row[3].lower() != str(payload.get("node", "")).lower()
        or payload.get("sandbox") != sandbox
        or row[5] != account
        or row[6] != _sandbox_service_user(profile, sandbox)
        or row[7] != profile.cluster
        or row[8] != _sandbox_qos(profile, sandbox)
    ):
        raise PolicyError("allocation probe job identity drifted")


def _queue_probe_rows(
    profile: Profile,
    *,
    job_name: str | None = None,
    job_id: str | None = None,
) -> list[list[str]]:
    selector: tuple[str, str]
    if job_id is not None:
        selector = ("-j", job_id)
    elif job_name is not None:
        selector = ("-n", job_name)
    else:
        raise PolicyError("allocation queue probe requires an exact selector")
    output = _run(
        (
            "squeue",
            f"--clusters={profile.cluster}",
            "-h",
            *selector,
            "-o",
            "%A|%j|%T|%u|%a|%N",
        ),
        timeout=15,
    )
    rows = [line.split("|") for line in output.splitlines() if line.strip()]
    if any(len(row) < 6 for row in rows):
        raise PolicyError("Slurm allocation probe queue output is malformed")
    return rows


def _validate_probe_queue_row(
    row: Sequence[str],
    payload: Mapping[str, Any],
    profile: Profile,
    *,
    sandbox: str,
) -> None:
    if len(row) < 6:
        raise PolicyError("allocation probe queued job identity drifted")
    state = _normalize_probe_job_state(str(row[2]))
    if (
        row[0] != str(payload.get("job_id", ""))
        or row[1] != payload.get("job_name")
        or payload.get("sandbox") != sandbox
        or row[3] != _sandbox_service_user(profile, sandbox)
        or row[4] != _sandbox_account(profile, sandbox)
        or (
            state not in {"PENDING", "PD"}
            and row[5].lower() != str(payload.get("node", "")).lower()
        )
    ):
        raise PolicyError("allocation probe queued job identity drifted")


def _validate_probe_job_identity(
    payload: Mapping[str, Any],
    profile: Profile,
    *,
    sandbox: str,
) -> tuple[list[list[str]], str | None]:
    job_id = str(payload.get("job_id", ""))
    rows = _probe_accounting_rows(job_id, profile)
    base_rows = [row for row in rows if row[0] == job_id]
    if base_rows:
        if len(base_rows) != 1:
            raise PolicyError("allocation probe accounting identity is ambiguous")
        _validate_probe_base_row(base_rows[0], payload, profile, sandbox=sandbox)
        return rows, _base_job_state(rows, job_id)
    queued = _queue_probe_rows(profile, job_id=job_id)
    if len(queued) != 1:
        raise PolicyError("allocation probe job identity is unavailable or ambiguous")
    _validate_probe_queue_row(queued[0], payload, profile, sandbox=sandbox)
    return rows, None


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
    sandbox: str,
    enforce_root_ownership: bool,
) -> list[list[str]]:
    job_id = str(payload.get("job_id", ""))
    if re.fullmatch(r"[1-9][0-9]*", job_id) is None:
        raise PolicyError("allocation inflight journal job ID is invalid")
    _observed, observed_state = _validate_probe_job_identity(
        payload,
        profile,
        sandbox=sandbox,
    )
    if observed_state in _TERMINAL_JOB_STATES:
        payload["terminal_state"] = observed_state
        _finish_allocation_inflight(
            path,
            payload,
            "terminal",
            enforce_root_ownership=enforce_root_ownership,
        )
        return _observed
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
        rows = _poll_probe_terminal(job_id, profile, timeout_seconds=60)
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
    return rows


def _poll_allocation_or_cancel(
    path: Path,
    payload: dict[str, Any],
    profile: Profile,
    *,
    sandbox: str,
    timeout_seconds: float,
    enforce_root_ownership: bool,
) -> list[list[str]]:
    try:
        return _poll_probe_terminal(
            str(payload["job_id"]),
            profile,
            timeout_seconds=timeout_seconds,
        )
    except Exception:
        _cancel_allocation_job(
            path,
            payload,
            profile,
            sandbox=sandbox,
            enforce_root_ownership=enforce_root_ownership,
        )
        raise


def _recover_allocation_probe(
    path: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    node: str,
    job_name: str,
    enforce_root_ownership: bool,
) -> tuple[str, list[list[str]]] | None:
    recovered_job_id: str | None = None
    completed_recovery: tuple[str, list[list[str]]] | None = None
    payload = _load_allocation_state(
        path,
        enforce_root_ownership=enforce_root_ownership,
    )
    expected_generation = _allocation_job_generation(job_name)
    if payload is not None:
        if (
            payload.get("candidate_sha") != candidate_sha
            or payload.get("cluster") != profile.cluster
            or payload.get("controller") != profile.controller
            or payload.get("submit_host") != profile.submit_host
            or payload.get("node") != node
            or payload.get("host") != profile.host_aliases[node]
            or payload.get("job_name") != job_name
            or payload.get("sandbox") != sandbox
            or payload.get("user") != _sandbox_service_user(profile, sandbox)
            or payload.get("account") != _sandbox_account(profile, sandbox)
            or (
                payload.get("generation_id") != expected_generation
                and not (
                    _LEGACY_ALLOCATION_GENERATION_RE.fullmatch(expected_generation) is not None
                    and payload.get("generation_id") is None
                )
            )
        ):
            raise PolicyError("allocation inflight journal binding drifted")
        recovered_job_id = str(payload.get("job_id", ""))
        recovered_rows = _cancel_allocation_job(
            path,
            payload,
            profile,
            sandbox=sandbox,
            enforce_root_ownership=enforce_root_ownership,
        )
        if _base_job_state(recovered_rows, recovered_job_id) == "COMPLETED":
            completed_recovery = (recovered_job_id, recovered_rows)
    orphan_rows = _queue_probe_rows(profile, job_name=job_name)
    if recovered_job_id is not None:
        orphan_rows = [row for row in orphan_rows if row[0] != recovered_job_id]
    if len(orphan_rows) > 1:
        raise PolicyError("unjournaled allocation probe jobs are ambiguous")
    if not orphan_rows:
        accounting_rows = _probe_named_accounting_rows(job_name, profile)
        base_rows = [
            row for row in accounting_rows if "." not in row[0] and row[0] != recovered_job_id
        ]
        if not base_rows:
            return completed_recovery
        if len(base_rows) != 1:
            raise PolicyError("unjournaled allocation probe history is ambiguous")
        recovered_job_id = base_rows[0][0]
        orphan_payload = {
            "sandbox": sandbox,
            "job_id": recovered_job_id,
            "job_name": job_name,
            "generation_id": expected_generation,
            "node": node,
        }
        _validate_probe_base_row(
            base_rows[0],
            orphan_payload,
            profile,
            sandbox=sandbox,
        )
        state = _base_job_state(accounting_rows, recovered_job_id)
        if state not in _TERMINAL_JOB_STATES:
            raise PolicyError("unjournaled allocation probe history is not terminal")
        recovered_rows = _probe_accounting_rows(recovered_job_id, profile)
        recovered_base_rows = [row for row in recovered_rows if row[0] == recovered_job_id]
        if len(recovered_base_rows) != 1:
            raise PolicyError("unjournaled allocation probe accounting is incomplete")
        _validate_probe_base_row(
            recovered_base_rows[0],
            orphan_payload,
            profile,
            sandbox=sandbox,
        )
        recovered_state = _base_job_state(recovered_rows, recovered_job_id)
        if recovered_state != state:
            raise PolicyError("unjournaled allocation probe terminal state drifted")
        recovered = {
            "schema_version": 1,
            "sandbox": sandbox,
            "candidate_sha": candidate_sha,
            "cluster": profile.cluster,
            "controller": profile.controller,
            "submit_host": profile.submit_host,
            "node": node,
            "host": profile.host_aliases[node],
            "user": _sandbox_service_user(profile, sandbox),
            "account": _sandbox_account(profile, sandbox),
            "job_id": recovered_job_id,
            "job_name": job_name,
            "generation_id": expected_generation,
            "phase": "recovered_terminal",
            "created_at": datetime.now(UTC).isoformat(),
            "terminal_state": state,
        }
        _write_allocation_state(
            path,
            recovered,
            enforce_root_ownership=enforce_root_ownership,
        )
        _finish_allocation_inflight(
            path,
            recovered,
            "terminal",
            enforce_root_ownership=enforce_root_ownership,
        )
        if state == "COMPLETED":
            return (recovered_job_id, recovered_rows)
        return completed_recovery
    recovered_job_id = orphan_rows[0][0]
    orphan_payload = {
        "sandbox": sandbox,
        "job_id": recovered_job_id,
        "job_name": job_name,
        "generation_id": expected_generation,
        "node": node,
    }
    _validate_probe_queue_row(
        orphan_rows[0],
        orphan_payload,
        profile,
        sandbox=sandbox,
    )
    if re.fullmatch(r"[1-9][0-9]*", recovered_job_id) is None:
        raise PolicyError("unjournaled allocation probe job ID is invalid")
    recovered = {
        "schema_version": 1,
        "sandbox": sandbox,
        "candidate_sha": candidate_sha,
        "cluster": profile.cluster,
        "controller": profile.controller,
        "submit_host": profile.submit_host,
        "node": node,
        "host": profile.host_aliases[node],
        "user": _sandbox_service_user(profile, sandbox),
        "account": _sandbox_account(profile, sandbox),
        "job_id": recovered_job_id,
        "job_name": job_name,
        "generation_id": expected_generation,
        "phase": "recovered_unjournaled",
        "created_at": datetime.now(UTC).isoformat(),
    }
    _write_allocation_state(
        path,
        recovered,
        enforce_root_ownership=enforce_root_ownership,
    )
    recovered_rows = _cancel_allocation_job(
        path,
        recovered,
        profile,
        sandbox=sandbox,
        enforce_root_ownership=enforce_root_ownership,
    )
    if _base_job_state(recovered_rows, recovered_job_id) == "COMPLETED":
        return (recovered_job_id, recovered_rows)
    return completed_recovery


def _positive_gpu_tres(value: str) -> bool:
    for item in value.split(","):
        key, separator, raw = item.partition("=")
        if separator and (key == "gres/gpu" or key.startswith("gres/gpu:")):
            try:
                return float(raw) > 0
            except ValueError:
                return False
    return False


def _allocation_generation_window(
    runtime_attestation: Mapping[str, Any],
) -> tuple[str, datetime, datetime]:
    generation_id = _allocation_generation_id(runtime_attestation)
    try:
        started_at = datetime.fromisoformat(
            str(runtime_attestation["receipt_collected_at"]),
        ).astimezone(UTC)
        expires_at = datetime.fromisoformat(
            str(runtime_attestation["proof_expires_at"]),
        ).astimezone(UTC)
    except (KeyError, ValueError) as exc:
        raise PolicyError("allocation matrix generation window is invalid") from exc
    if not started_at < expires_at:
        raise PolicyError("allocation matrix generation window is empty")
    return generation_id, started_at, expires_at


def _require_allocation_proof_freshness(
    runtime_attestation: Mapping[str, Any],
    *,
    timeout_seconds: float,
    now: datetime | None = None,
) -> None:
    if not 1 <= timeout_seconds <= 600:
        raise PolicyError("allocation probe timeout must be between 1 and 600 seconds")
    _generation_id, _started_at, expires_at = _allocation_generation_window(
        runtime_attestation,
    )
    observed_at = datetime.now(UTC) if now is None else now.astimezone(UTC)
    required_until = observed_at + timedelta(seconds=timeout_seconds)
    required_until += _ALLOCATION_PROOF_EXPIRY_MARGIN
    if expires_at < required_until:
        raise PolicyError(
            "runtime proof expires before the allocation timeout safety window",
        )


def _parse_allocation_timestamp(value: Any, description: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise PolicyError(f"{description} timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise PolicyError(f"{description} timestamp is invalid")
    return parsed.astimezone(UTC)


def _allocation_matrix_generation_is_stale(
    matrix: Mapping[str, Any],
    profile: Profile,
    *,
    now: datetime,
) -> bool:
    started_at = _parse_allocation_timestamp(
        matrix.get("generation_started_at"),
        "allocation matrix generation start",
    )
    expires_at = _parse_allocation_timestamp(
        matrix.get("generation_expires_at"),
        "allocation matrix generation expiry",
    )
    if not started_at < expires_at or now >= expires_at:
        return True
    nodes = matrix.get("nodes")
    if not isinstance(nodes, Mapping):
        raise PolicyError("allocation matrix journal nodes are invalid")
    for node in profile.allowed_nodes:
        row = nodes.get(node)
        if not isinstance(row, Mapping) or row.get("status") != "completed":
            continue
        evidence = row.get("evidence")
        if not isinstance(evidence, Mapping):
            raise PolicyError("completed allocation matrix node lacks evidence")
        completed_at = _parse_allocation_timestamp(
            evidence.get("completed_at"),
            "allocation matrix node completion",
        )
        if (
            completed_at < started_at
            or completed_at >= expires_at
            or now - completed_at > _ALLOCATION_PROBE_MAX_AGE
        ):
            return True
    return False


def _allocation_matrix_requires_reset(
    matrix: Mapping[str, Any],
    profile: Profile,
    *,
    runtime_attestation: Mapping[str, Any],
    now: datetime,
) -> bool:
    return (
        matrix.get("runtime_attestation") != runtime_attestation
        or matrix.get("generation_id") != _allocation_generation_id(runtime_attestation)
        or _allocation_matrix_generation_is_stale(matrix, profile, now=now)
    )


def _new_allocation_matrix(
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    binding: Mapping[str, Any],
    runtime_attestation: Mapping[str, Any],
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
) -> dict[str, Any]:
    _sandbox_account(profile, sandbox)
    if runtime_attestation.get("sandbox") != sandbox:
        raise PolicyError("allocation matrix runtime proof sandbox binding drifted")
    now = datetime.now(UTC).isoformat()
    generation_id, generation_started_at, generation_expires_at = _allocation_generation_window(
        runtime_attestation
    )
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-allocation-matrix-journal",
        "created_at": now,
        "updated_at": now,
        "candidate_sha": candidate_sha,
        "candidate_tree": binding["repository"]["candidate_tree"],
        "sandbox": sandbox,
        "cluster": profile.cluster,
        "controller": profile.controller,
        "submit_host": profile.submit_host,
        "allowed_nodes": list(profile.allowed_nodes),
        "host_aliases": _allowed_host_aliases(profile),
        "batch_uid": batch_uid,
        "batch_gid": batch_gid,
        "account": _sandbox_account(profile, sandbox),
        "qos": _sandbox_qos(profile, sandbox),
        "expected_pool": expected_pool,
        "expected_concurrency": expected_concurrency,
        "candidate_binding": dict(binding),
        "runtime_attestation": dict(runtime_attestation),
        "generation_id": generation_id,
        "generation_started_at": generation_started_at.isoformat(),
        "generation_expires_at": generation_expires_at.isoformat(),
        "nodes": {
            node: {
                "node": node,
                "host": profile.host_aliases[node],
                "status": "pending",
                "attempts": 0,
                "evidence": None,
            }
            for node in profile.allowed_nodes
        },
        "phase": "running",
    }


def _validate_allocation_matrix(
    matrix: Mapping[str, Any],
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    binding: Mapping[str, Any],
    runtime_attestation: Mapping[str, Any],
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
) -> None:
    nodes = matrix.get("nodes")
    if (
        matrix.get("schema_version") != 1
        or matrix.get("artifact_type") != "developer-sandbox-slurm-allocation-matrix-journal"
        or matrix.get("candidate_sha") != candidate_sha
        or matrix.get("candidate_tree") != binding["repository"]["candidate_tree"]
        or matrix.get("sandbox") != sandbox
        or matrix.get("cluster") != profile.cluster
        or matrix.get("controller") != profile.controller
        or matrix.get("submit_host") != profile.submit_host
        or matrix.get("allowed_nodes") != list(profile.allowed_nodes)
        or matrix.get("host_aliases") != _allowed_host_aliases(profile)
        or matrix.get("batch_uid") != batch_uid
        or matrix.get("batch_gid") != batch_gid
        or matrix.get("account") != _sandbox_account(profile, sandbox)
        or matrix.get("qos") != _sandbox_qos(profile, sandbox)
        or matrix.get("expected_pool") != expected_pool
        or matrix.get("expected_concurrency") != expected_concurrency
        or matrix.get("candidate_binding") != binding
        or matrix.get("runtime_attestation") != runtime_attestation
        or runtime_attestation.get("sandbox") != sandbox
        or matrix.get("generation_id") != _allocation_generation_id(runtime_attestation)
        or matrix.get("generation_started_at") != runtime_attestation.get("receipt_collected_at")
        or matrix.get("generation_expires_at") != runtime_attestation.get("proof_expires_at")
        or not isinstance(nodes, dict)
        or list(nodes) != list(profile.allowed_nodes)
    ):
        raise PolicyError("allocation matrix journal binding drifted")
    for node in profile.allowed_nodes:
        row = nodes[node]
        if (
            not isinstance(row, dict)
            or row.get("node") != node
            or row.get("host") != profile.host_aliases[node]
            or row.get("status") not in {"pending", "inflight", "completed", "failed"}
            or type(row.get("attempts")) is not int
            or row["attempts"] < 0
        ):
            raise PolicyError("allocation matrix journal node row drifted")


def _validate_allocation_matrix_recovery_binding(
    matrix: Mapping[str, Any],
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
) -> None:
    nodes = matrix.get("nodes")
    if (
        matrix.get("schema_version") != 1
        or matrix.get("artifact_type") != "developer-sandbox-slurm-allocation-matrix-journal"
        or matrix.get("candidate_sha") != candidate_sha
        or matrix.get("sandbox") != sandbox
        or matrix.get("cluster") != profile.cluster
        or matrix.get("controller") != profile.controller
        or matrix.get("submit_host") != profile.submit_host
        or matrix.get("allowed_nodes") != list(profile.allowed_nodes)
        or matrix.get("host_aliases") != _allowed_host_aliases(profile)
        or not (
            _allocation_generation_is_bundle_bound(matrix, sandbox=sandbox)
            or _legacy_allocation_generation_is_bound(matrix, sandbox=sandbox)
        )
        or not isinstance(matrix.get("generation_started_at"), str)
        or not isinstance(matrix.get("generation_expires_at"), str)
        or not isinstance(nodes, dict)
        or list(nodes) != list(profile.allowed_nodes)
        or any(
            not isinstance(nodes[node], dict)
            or nodes[node].get("node") != node
            or nodes[node].get("host") != profile.host_aliases[node]
            or nodes[node].get("status") not in {"pending", "inflight", "completed", "failed"}
            or type(nodes[node].get("attempts")) is not int
            or nodes[node]["attempts"] < 0
            for node in profile.allowed_nodes
        )
    ):
        raise PolicyError("allocation matrix recovery binding drifted")


def _persist_allocation_matrix(
    path: Path,
    matrix: dict[str, Any],
    *,
    enforce_root_ownership: bool,
) -> None:
    matrix["updated_at"] = datetime.now(UTC).isoformat()
    _write_allocation_state(
        path,
        matrix,
        enforce_root_ownership=enforce_root_ownership,
    )


def _unfinished_allocation_nodes(
    matrix: Mapping[str, Any],
    profile: Profile,
) -> tuple[str, ...]:
    nodes = matrix.get("nodes")
    if not isinstance(nodes, Mapping):
        raise PolicyError("allocation matrix journal nodes are invalid")
    return tuple(
        node
        for node in profile.allowed_nodes
        if not isinstance(nodes.get(node), Mapping) or nodes[node].get("status") != "completed"
    )


def _allocation_probe_arguments(
    profile: Profile,
    *,
    sandbox: str,
    node: str,
    attempt: int,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    binding: Mapping[str, Any],
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
    result_path: Path,
    generation_id: str,
) -> tuple[str, ...]:
    host = profile.host_aliases[node]
    repository = binding["repository"]
    env_binding = binding["worker_env"]
    policy_program = candidate_root / "scripts/ops/developer_sandbox_slurm_policy.py"
    profile_name = "gb10.toml" if profile.cluster == "trt-gb10" else "oldlab.toml"
    profile_path = candidate_root / "deploy/slurm/developer-sandboxes" / profile_name
    node_check = (
        "/usr/bin/python3",
        "-I",
        "-B",
        str(policy_program),
        "allocation-node-check",
        "--profile",
        str(profile_path),
        "--sandbox",
        sandbox,
        "--candidate-sha",
        candidate_sha,
        "--candidate-root",
        str(candidate_root),
        "--worker-env",
        str(worker_env),
        "--expected-tree",
        str(repository["candidate_tree"]),
        "--expected-env-inode",
        str(env_binding["inode"]),
        "--expected-env-sha256",
        str(env_binding["sha256"]),
        "--batch-uid",
        str(batch_uid),
        "--batch-gid",
        str(batch_gid),
        "--expected-host",
        host,
        "--expected-pool",
        expected_pool,
        "--expected-concurrency",
        str(expected_concurrency),
        "--result-path",
        str(result_path),
    )
    wrapped = " ".join(
        shlex.quote(item)
        for item in (
            "/usr/bin/srun",
            "--nodes=1",
            "--ntasks=1",
            f"--nodelist={node}",
            *node_check,
        )
    )
    arguments = [
        "sbatch",
        "--parsable",
        f"--job-name={_allocation_job_name(sandbox, candidate_sha, node, attempt, generation_id=generation_id)}",
        f"--uid={_sandbox_service_user(profile, sandbox)}",
        f"--account={_sandbox_account(profile, sandbox)}",
        f"--qos={_sandbox_qos(profile, sandbox)}",
        f"--clusters={profile.cluster}",
        f"--nodelist={node}",
        "--oversubscribe",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=1",
        "--mem=256M",
        "--time=00:02:00",
        "--output=/dev/null",
        "--error=/dev/null",
        f"--comment=loom-cgroup-v1:pids={profile.job_pids_max}",
        "--export=NONE",
    ]
    if profile.gpu_tres_per_slot > 0:
        arguments.append("--gres=gpu:1")
    arguments.append(f"--wrap={wrapped}")
    return tuple(arguments)


def _allocation_node_evidence(
    profile: Profile,
    *,
    sandbox: str,
    node: str,
    candidate_sha: str,
    binding: Mapping[str, Any],
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
    job_id: str,
    job_name: str,
    rows: Sequence[Sequence[str]],
    arguments: Sequence[str],
    node_result: Mapping[str, Any],
) -> dict[str, Any]:
    base_rows = [row for row in rows if row[0] == job_id]
    srun_rows = [row for row in rows if row[0] == f"{job_id}.0"]
    expected_host = profile.host_aliases[node]
    if (
        len(base_rows) != 1
        or len(srun_rows) != 1
        or base_rows[0][1] != job_name
        or _normalize_probe_job_state(base_rows[0][2]) != "COMPLETED"
        or base_rows[0][3].lower() != node.lower()
        or base_rows[0][5] != _sandbox_account(profile, sandbox)
        or base_rows[0][6] != _sandbox_service_user(profile, sandbox)
        or base_rows[0][7] != profile.cluster
        or base_rows[0][8] != _sandbox_qos(profile, sandbox)
        or _normalize_probe_job_state(srun_rows[0][2]) != "COMPLETED"
        or srun_rows[0][3].lower() != node.lower()
        or srun_rows[0][5] != _sandbox_account(profile, sandbox)
        or srun_rows[0][6] != _sandbox_service_user(profile, sandbox)
        or srun_rows[0][7] != profile.cluster
    ):
        raise PolicyError("allocation matrix sbatch/srun readback drifted")
    alloc_tres = base_rows[0][4]
    gpu_verified = profile.gpu_tres_per_slot <= 0 or _positive_gpu_tres(alloc_tres)
    if not gpu_verified:
        raise PolicyError("allocation matrix GPU TRES readback drifted")
    expected_compute = {
        "schema_version": 1,
        "sandbox": sandbox,
        "account": _sandbox_account(profile, sandbox),
        "candidate_sha": candidate_sha,
        "candidate_tree": binding["repository"]["candidate_tree"],
        "host": expected_host,
        "env_device": node_result.get("env_device"),
        "env_inode": binding["worker_env"]["inode"],
        "env_sha256": binding["worker_env"]["sha256"],
        "pool": expected_pool,
        "concurrency": expected_concurrency,
        "docker_cgroup_driver": profile.docker_cgroup_driver,
        "job_id": job_id,
        "cgroup_parent": node_result.get("cgroup_parent"),
        "cgroup_guard_verified": True,
        "compose_verified": True,
    }
    if (
        node_result != expected_compute
        or not isinstance(node_result.get("env_device"), int)
        or not isinstance(node_result.get("cgroup_parent"), str)
        or f"job_{job_id}" not in str(node_result["cgroup_parent"]).split("/")
    ):
        raise PolicyError("allocation-side compute result binding drifted")
    return {
        "node": node,
        "host": expected_host,
        "job_id": job_id,
        "job_name": job_name,
        "state": "COMPLETED",
        "sandbox": sandbox,
        "account": _sandbox_account(profile, sandbox),
        "qos": base_rows[0][8],
        "alloc_tres": alloc_tres,
        "gpu_verified": gpu_verified,
        "sbatch_verified": True,
        "srun_verified": True,
        "nonexclusive": True,
        "explicit_nodelist": node,
        "compute_check": dict(node_result),
        "batch_uid": batch_uid,
        "batch_gid": batch_gid,
        "command_sha256": hashlib.sha256("\0".join(arguments).encode()).hexdigest(),
        "completed_at": datetime.now(UTC).isoformat(),
    }


def _replay_completed_allocation_probe(
    matrix_path: Path,
    matrix: dict[str, Any],
    profile: Profile,
    *,
    sandbox: str,
    node: str,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    binding: Mapping[str, Any],
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
    job_id: str,
    recovered_rows: Sequence[Sequence[str]],
    enforce_root_ownership: bool,
) -> None:
    row = matrix["nodes"][node]
    attempt = row["attempts"]
    generation_id = str(matrix["generation_id"])
    job_name = _allocation_job_name(
        sandbox,
        candidate_sha,
        node,
        attempt,
        generation_id=generation_id,
    )
    result_path = _allocation_result_path(
        worker_env,
        profile,
        sandbox,
        candidate_sha,
        node,
    )
    arguments = _allocation_probe_arguments(
        profile,
        sandbox=sandbox,
        node=node,
        attempt=attempt,
        candidate_sha=candidate_sha,
        candidate_root=candidate_root,
        worker_env=worker_env,
        binding=binding,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
        expected_pool=expected_pool,
        expected_concurrency=expected_concurrency,
        result_path=result_path,
        generation_id=generation_id,
    )
    node_result = _load_allocation_result(
        result_path,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
    )
    evidence = _allocation_node_evidence(
        profile,
        sandbox=sandbox,
        node=node,
        candidate_sha=candidate_sha,
        binding=binding,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
        expected_pool=expected_pool,
        expected_concurrency=expected_concurrency,
        job_id=job_id,
        job_name=job_name,
        rows=recovered_rows,
        arguments=arguments,
        node_result=node_result,
    )
    row["status"] = "completed"
    row["job_id"] = job_id
    row["job_name"] = job_name
    row["evidence"] = evidence
    _persist_allocation_matrix(
        matrix_path,
        matrix,
        enforce_root_ownership=enforce_root_ownership,
    )
    _discard_allocation_result(result_path)


def _replay_completed_or_mark_retry(
    matrix_path: Path,
    matrix: dict[str, Any],
    profile: Profile,
    *,
    sandbox: str,
    node: str,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    binding: Mapping[str, Any],
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
    job_id: str,
    recovered_rows: Sequence[Sequence[str]],
    enforce_root_ownership: bool,
) -> bool:
    try:
        _replay_completed_allocation_probe(
            matrix_path,
            matrix,
            profile,
            sandbox=sandbox,
            node=node,
            candidate_sha=candidate_sha,
            candidate_root=candidate_root,
            worker_env=worker_env,
            binding=binding,
            batch_uid=batch_uid,
            batch_gid=batch_gid,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
            job_id=job_id,
            recovered_rows=recovered_rows,
            enforce_root_ownership=enforce_root_ownership,
        )
    except PolicyError as replay_exc:
        row = matrix["nodes"][node]
        row["status"] = "pending"
        row["evidence"] = None
        row["last_replay_failure"] = {
            "job_id": job_id,
            "failure": str(replay_exc),
            "failed_at": datetime.now(UTC).isoformat(),
        }
        matrix["phase"] = "running"
        _persist_allocation_matrix(
            matrix_path,
            matrix,
            enforce_root_ownership=enforce_root_ownership,
        )
        _discard_allocation_result(
            _allocation_result_path(
                worker_env,
                profile,
                sandbox,
                candidate_sha,
                node,
            ),
        )
        return False
    return True


def _run_allocation_probe_transaction(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
    timeout_seconds: float = _ALLOCATION_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    account = _sandbox_account(profile, sandbox)
    if root != Path("/") or os.geteuid() != 0:
        raise PolicyError("allocation-side Slurm probe requires the live root")
    if batch_uid < 0 or batch_gid < 0:
        raise PolicyError("allocation probe batch UID/GID must be non-negative")
    if not 1 <= timeout_seconds <= 600:
        raise PolicyError("allocation probe timeout must be between 1 and 600 seconds")
    _require_worker_capacity_assertion(
        profile,
        candidate_root,
        expected_pool=expected_pool,
        expected_concurrency=expected_concurrency,
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
    matrix_path = _allocation_matrix_path(root, profile, sandbox, candidate_sha)
    matrix = _load_allocation_state(matrix_path, enforce_root_ownership=True)
    if matrix is not None:
        _validate_allocation_matrix_recovery_binding(
            matrix,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
        )
        nodes = matrix["nodes"]
    else:
        nodes = {}
    recovered_completed: dict[str, tuple[str, list[list[str]]]] = {}
    for node in profile.allowed_nodes:
        inflight_path = _allocation_node_inflight_path(
            root,
            profile,
            sandbox,
            candidate_sha,
            node,
        )
        if matrix is None:
            if inflight_path.exists():
                raise PolicyError("allocation inflight exists without its matrix journal")
            continue
        row = nodes[node]
        attempt = row["attempts"]
        if attempt > 0 and (row["status"] != "completed" or inflight_path.exists()):
            generation_id = str(matrix["generation_id"])
            recovered = _recover_allocation_probe(
                inflight_path,
                profile,
                sandbox=sandbox,
                candidate_sha=candidate_sha,
                node=node,
                job_name=_allocation_job_name(
                    sandbox,
                    candidate_sha,
                    node,
                    attempt,
                    generation_id=generation_id,
                    allow_legacy=True,
                ),
                enforce_root_ownership=True,
            )
            if recovered is not None:
                recovered_completed[node] = recovered
            else:
                _discard_allocation_result(
                    _allocation_result_path(
                        worker_env,
                        profile,
                        sandbox,
                        candidate_sha,
                        node,
                    ),
                )
        else:
            _discard_allocation_result(
                _allocation_result_path(
                    worker_env,
                    profile,
                    sandbox,
                    candidate_sha,
                    node,
                ),
            )
        if row["status"] == "inflight" and node not in recovered_completed:
            nodes[node]["status"] = "pending"
            nodes[node]["evidence"] = None
            _persist_allocation_matrix(
                matrix_path,
                matrix,
                enforce_root_ownership=True,
            )

    verify_source_candidate(candidate_sha)
    try:
        batch_identity = pwd.getpwnam(_sandbox_service_user(profile, sandbox))
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
    if _SAFE_NAME.fullmatch(expected_pool) is None or expected_concurrency < 1:
        raise PolicyError("allocation matrix pool/concurrency binding is invalid")
    runtime_attestation = _runtime_attestation_binding(
        root,
        profile,
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        candidate_tree=binding["repository"]["candidate_tree"],
        candidate_root=candidate_root,
        worker_env=worker_env,
        enforce_root_ownership=True,
    )
    _require_allocation_proof_freshness(
        runtime_attestation,
        timeout_seconds=timeout_seconds,
    )
    if matrix is not None and _allocation_matrix_requires_reset(
        matrix,
        profile,
        runtime_attestation=runtime_attestation,
        now=datetime.now(UTC),
    ):
        _archive_allocation_generation(
            root,
            profile,
            sandbox,
            candidate_sha,
            matrix,
        )
        for node in profile.allowed_nodes:
            _discard_allocation_result(
                _allocation_result_path(
                    worker_env,
                    profile,
                    sandbox,
                    candidate_sha,
                    node,
                ),
            )
        matrix = None
        recovered_completed.clear()
    if matrix is None:
        matrix = _new_allocation_matrix(
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            binding=binding,
            runtime_attestation=runtime_attestation,
            batch_uid=batch_uid,
            batch_gid=batch_gid,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
        )
        _persist_allocation_matrix(
            matrix_path,
            matrix,
            enforce_root_ownership=True,
        )
    _validate_allocation_matrix(
        matrix,
        profile,
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        binding=binding,
        runtime_attestation=runtime_attestation,
        batch_uid=batch_uid,
        batch_gid=batch_gid,
        expected_pool=expected_pool,
        expected_concurrency=expected_concurrency,
    )
    nodes = matrix["nodes"]
    if matrix.get("phase") == "completed":
        final_path = _allocation_probe_path(root, profile, sandbox, candidate_sha)
        if final_path.exists():
            return _allocation_probe_readback_unlocked(
                root,
                profile,
                sandbox=sandbox,
                candidate_sha=candidate_sha,
                candidate_binding=binding,
                runtime_attestation=runtime_attestation,
                expected_pool=expected_pool,
                expected_concurrency=expected_concurrency,
            )
    else:
        _invalidate_allocation_artifact(root, profile, sandbox, candidate_sha)
    for node, (job_id, recovered_rows) in recovered_completed.items():
        _replay_completed_or_mark_retry(
            matrix_path,
            matrix,
            profile,
            sandbox=sandbox,
            node=node,
            candidate_sha=candidate_sha,
            candidate_root=candidate_root,
            worker_env=worker_env,
            binding=binding,
            batch_uid=batch_uid,
            batch_gid=batch_gid,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
            job_id=job_id,
            recovered_rows=recovered_rows,
            enforce_root_ownership=True,
        )
    generation_id = str(matrix["generation_id"])
    generation_expires_at = _parse_allocation_timestamp(
        matrix["generation_expires_at"],
        "allocation matrix generation expiry",
    )
    for node in _unfinished_allocation_nodes(matrix, profile):
        _require_allocation_proof_freshness(
            runtime_attestation,
            timeout_seconds=timeout_seconds,
        )
        if datetime.now(UTC) >= generation_expires_at:
            matrix["phase"] = "generation_expired"
            _persist_allocation_matrix(
                matrix_path,
                matrix,
                enforce_root_ownership=True,
            )
            raise PolicyError("allocation matrix generation expired before completion")
        row = nodes[node]
        inflight_path = _allocation_node_inflight_path(
            root,
            profile,
            sandbox,
            candidate_sha,
            node,
        )
        result_path = _allocation_result_path(
            worker_env,
            profile,
            sandbox,
            candidate_sha,
            node,
        )
        row["attempts"] += 1
        attempt = row["attempts"]
        job_name = _allocation_job_name(
            sandbox,
            candidate_sha,
            node,
            attempt,
            generation_id=generation_id,
        )
        row["status"] = "pending"
        row["evidence"] = None
        _persist_allocation_matrix(
            matrix_path,
            matrix,
            enforce_root_ownership=True,
        )
        _prepare_allocation_result_path(
            result_path,
            worker_env=worker_env,
            batch_uid=batch_uid,
            batch_gid=batch_gid,
        )
        arguments = _allocation_probe_arguments(
            profile,
            sandbox=sandbox,
            node=node,
            attempt=attempt,
            candidate_sha=candidate_sha,
            candidate_root=candidate_root,
            worker_env=worker_env,
            binding=binding,
            batch_uid=batch_uid,
            batch_gid=batch_gid,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
            result_path=result_path,
            generation_id=generation_id,
        )
        try:
            output = _run(arguments, timeout=30)
            job_ids = [
                match.group(1)
                for line in output.splitlines()
                if (
                    match := re.fullmatch(
                        r"([1-9][0-9]*)(?:;[A-Za-z0-9_.-]+)?",
                        line.strip(),
                    )
                )
            ]
            if len(job_ids) != 1:
                _recover_allocation_probe(
                    inflight_path,
                    profile,
                    sandbox=sandbox,
                    candidate_sha=candidate_sha,
                    node=node,
                    job_name=job_name,
                    enforce_root_ownership=True,
                )
                raise PolicyError("allocation matrix did not return one exact job ID")
            job_id = job_ids[0]
            inflight = {
                "schema_version": 1,
                "sandbox": sandbox,
                "created_at": datetime.now(UTC).isoformat(),
                "candidate_sha": candidate_sha,
                "cluster": profile.cluster,
                "controller": profile.controller,
                "submit_host": profile.submit_host,
                "node": node,
                "host": profile.host_aliases[node],
                "user": _sandbox_service_user(profile, sandbox),
                "account": account,
                "job_id": job_id,
                "job_name": job_name,
                "generation_id": generation_id,
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
                _accounting, state = _validate_probe_job_identity(
                    inflight,
                    profile,
                    sandbox=sandbox,
                )
                if state not in _TERMINAL_JOB_STATES:
                    _run(("scancel", f"--clusters={profile.cluster}", job_id), timeout=30)
                terminal = _poll_probe_terminal(job_id, profile, timeout_seconds=60)
                if _base_job_state(terminal, job_id) not in _TERMINAL_JOB_STATES:
                    raise PolicyError(
                        "unjournaled allocation matrix job lacks terminal readback",
                    ) from journal_exc
                raise
            row["status"] = "inflight"
            row["job_id"] = job_id
            row["job_name"] = job_name
            _persist_allocation_matrix(
                matrix_path,
                matrix,
                enforce_root_ownership=True,
            )
            rows = _poll_allocation_or_cancel(
                inflight_path,
                inflight,
                profile,
                sandbox=sandbox,
                timeout_seconds=timeout_seconds,
                enforce_root_ownership=True,
            )
            node_result = _load_allocation_result(
                result_path,
                batch_uid=batch_uid,
                batch_gid=batch_gid,
            )
            evidence = _allocation_node_evidence(
                profile,
                sandbox=sandbox,
                node=node,
                candidate_sha=candidate_sha,
                binding=binding,
                batch_uid=batch_uid,
                batch_gid=batch_gid,
                expected_pool=expected_pool,
                expected_concurrency=expected_concurrency,
                job_id=job_id,
                job_name=job_name,
                rows=rows,
                arguments=arguments,
                node_result=node_result,
            )
            row["status"] = "completed"
            row["evidence"] = evidence
            _persist_allocation_matrix(
                matrix_path,
                matrix,
                enforce_root_ownership=True,
            )
            _finish_allocation_inflight(
                inflight_path,
                inflight,
                "completed",
                enforce_root_ownership=True,
            )
            _discard_allocation_result(result_path)
        except Exception as exc:
            _invalidate_allocation_artifact(root, profile, sandbox, candidate_sha)
            cleanup_completed = False
            try:
                if inflight_path.exists():
                    inflight_payload = _load_allocation_state(
                        inflight_path,
                        enforce_root_ownership=True,
                    )
                    if inflight_payload is None:
                        raise PolicyError("allocation matrix inflight state disappeared")
                    cleanup_rows = _cancel_allocation_job(
                        inflight_path,
                        inflight_payload,
                        profile,
                        sandbox=sandbox,
                        enforce_root_ownership=True,
                    )
                    cleanup_completed = (
                        _base_job_state(cleanup_rows, str(inflight_payload["job_id"]))
                        == "COMPLETED"
                    )
                else:
                    recovered = _recover_allocation_probe(
                        inflight_path,
                        profile,
                        sandbox=sandbox,
                        candidate_sha=candidate_sha,
                        node=node,
                        job_name=job_name,
                        enforce_root_ownership=True,
                    )
                    cleanup_completed = recovered is not None
                if not cleanup_completed:
                    _discard_allocation_result(result_path)
            except Exception as cleanup_exc:
                row["status"] = "failed"
                row["evidence"] = {
                    "node": node,
                    "host": profile.host_aliases[node],
                    "terminal": False,
                    "failure": str(exc),
                    "cleanup_failure": str(cleanup_exc),
                    "failed_at": datetime.now(UTC).isoformat(),
                }
                matrix["phase"] = "failed"
                _persist_allocation_matrix(
                    matrix_path,
                    matrix,
                    enforce_root_ownership=True,
                )
                raise PolicyError(
                    "allocation matrix failed and exact job cleanup is unconfirmed",
                ) from cleanup_exc
            row["status"] = "failed"
            row["evidence"] = {
                "node": node,
                "host": profile.host_aliases[node],
                "terminal": True,
                "failure": str(exc),
                "failed_at": datetime.now(UTC).isoformat(),
            }
            matrix["phase"] = "failed"
            _persist_allocation_matrix(
                matrix_path,
                matrix,
                enforce_root_ownership=True,
            )
            raise

    completed = [nodes[node]["evidence"] for node in profile.allowed_nodes]
    if (
        len(completed) != len(profile.allowed_nodes)
        or any(nodes[node]["status"] != "completed" for node in profile.allowed_nodes)
        or [item["node"] for item in completed] != list(profile.allowed_nodes)
        or len({item["node"] for item in completed}) != len(profile.allowed_nodes)
    ):
        raise PolicyError("allocation matrix is not an exact closed-world pass")
    completed_at = matrix.get("completed_at")
    if completed_at is None:
        completed_at = datetime.now(UTC).isoformat()
        matrix["completed_at"] = completed_at
    completed_timestamp = _parse_allocation_timestamp(
        completed_at,
        "allocation matrix completion",
    )
    generation_started_at = _parse_allocation_timestamp(
        matrix["generation_started_at"],
        "allocation matrix generation start",
    )
    if not generation_started_at <= completed_timestamp < generation_expires_at:
        matrix["phase"] = "generation_expired"
        _persist_allocation_matrix(
            matrix_path,
            matrix,
            enforce_root_ownership=True,
        )
        raise PolicyError("allocation matrix completed outside its receipt generation window")
    payload = {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-allocation-matrix",
        "created_at": completed_timestamp.isoformat(),
        "generation_id": generation_id,
        "generation_started_at": generation_started_at.isoformat(),
        "generation_expires_at": generation_expires_at.isoformat(),
        "candidate_sha": candidate_sha,
        "candidate_tree": binding["repository"]["candidate_tree"],
        "sandbox": sandbox,
        "cluster": profile.cluster,
        "controller": profile.controller,
        "submit_host": profile.submit_host,
        "submitting_host": host,
        "allowed_nodes": list(profile.allowed_nodes),
        "host_aliases": _allowed_host_aliases(profile),
        "batch_uid": batch_uid,
        "batch_gid": batch_gid,
        "account": account,
        "qos": _sandbox_qos(profile, sandbox),
        "expected_pool": expected_pool,
        "expected_concurrency": expected_concurrency,
        "candidate_binding": binding,
        "runtime_attestation": runtime_attestation,
        "nodes": completed,
        "closed_world_verified": True,
    }
    _write_allocation_state(
        _allocation_probe_path(root, profile, sandbox, candidate_sha),
        payload,
        enforce_root_ownership=True,
    )
    matrix["phase"] = "completed"
    _persist_allocation_matrix(
        matrix_path,
        matrix,
        enforce_root_ownership=True,
    )
    return payload


def run_allocation_probe(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_root: Path,
    worker_env: Path,
    batch_uid: int,
    batch_gid: int,
    expected_pool: str,
    expected_concurrency: int,
    timeout_seconds: float = _ALLOCATION_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    _sandbox_account(profile, sandbox)
    if root != Path("/") or os.geteuid() != 0:
        raise PolicyError("allocation-side Slurm probe requires the live root")
    with _allocation_probe_lock(root, profile, sandbox, candidate_sha):
        return _run_allocation_probe_transaction(
            root,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_root=candidate_root,
            worker_env=worker_env,
            batch_uid=batch_uid,
            batch_gid=batch_gid,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
            timeout_seconds=timeout_seconds,
        )


def _allocation_probe_readback_unlocked(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_binding: Mapping[str, Any],
    runtime_attestation: Mapping[str, Any],
    expected_pool: str,
    expected_concurrency: int,
) -> dict[str, Any]:
    account = _sandbox_account(profile, sandbox)
    path = _allocation_probe_path(root, profile, sandbox, candidate_sha)
    inflight_paths = [
        _allocation_inflight_path(root, profile, sandbox, candidate_sha),
        *(
            _allocation_node_inflight_path(root, profile, sandbox, candidate_sha, node)
            for node in profile.allowed_nodes
        ),
    ]
    if any(item.exists() for item in inflight_paths):
        raise PolicyError("allocation-side Slurm probe still has an inflight job")
    matrix = _load_allocation_state(
        _allocation_matrix_path(root, profile, sandbox, candidate_sha),
        enforce_root_ownership=root == Path("/"),
    )
    if matrix is None:
        raise PolicyError("allocation-side Slurm matrix journal is unavailable")
    worker_env_binding = candidate_binding.get("worker_env")
    if (
        not isinstance(worker_env_binding, Mapping)
        or type(worker_env_binding.get("uid")) is not int
        or type(worker_env_binding.get("gid")) is not int
    ):
        raise PolicyError("allocation-side Slurm candidate binding is invalid")
    _validate_allocation_matrix(
        matrix,
        profile,
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        binding=candidate_binding,
        runtime_attestation=runtime_attestation,
        batch_uid=worker_env_binding["uid"],
        batch_gid=worker_env_binding["gid"],
        expected_pool=expected_pool,
        expected_concurrency=expected_concurrency,
    )
    if matrix.get("phase") != "completed":
        raise PolicyError("allocation-side Slurm matrix journal is not complete")
    if root == Path("/"):
        _require_root_private_directory(path.parent)
    payload = _load_allocation_state(
        path,
        enforce_root_ownership=root == Path("/"),
    )
    if payload is None:
        raise PolicyError("allocation-side Slurm probe evidence is unavailable")
    try:
        created = datetime.fromisoformat(str(payload["created_at"]))
    except (KeyError, ValueError) as exc:
        raise PolicyError("allocation-side Slurm probe timestamp is invalid") from exc
    if created.tzinfo is None or datetime.now(UTC) - created.astimezone(UTC) > (
        _ALLOCATION_PROBE_MAX_AGE
    ):
        raise PolicyError("allocation-side Slurm probe evidence is stale")
    generation_id, generation_started_at, generation_expires_at = _allocation_generation_window(
        runtime_attestation
    )
    if not generation_started_at <= created.astimezone(UTC) < generation_expires_at:
        raise PolicyError("allocation-side Slurm probe generation window drifted")
    repository = candidate_binding.get("repository")
    evidence_binding = payload.get("candidate_binding")
    evidence_nodes = payload.get("nodes")
    expected_top = {
        "schema_version",
        "artifact_type",
        "created_at",
        "generation_id",
        "generation_started_at",
        "generation_expires_at",
        "candidate_sha",
        "candidate_tree",
        "sandbox",
        "cluster",
        "controller",
        "submit_host",
        "submitting_host",
        "allowed_nodes",
        "host_aliases",
        "batch_uid",
        "batch_gid",
        "account",
        "qos",
        "expected_pool",
        "expected_concurrency",
        "candidate_binding",
        "runtime_attestation",
        "nodes",
        "closed_world_verified",
    }
    if (
        set(payload) != expected_top
        or payload.get("artifact_type") != "developer-sandbox-slurm-allocation-matrix"
        or not isinstance(repository, Mapping)
        or not isinstance(worker_env_binding, Mapping)
        or not isinstance(evidence_binding, Mapping)
        or payload.get("candidate_sha") != candidate_sha
        or payload.get("sandbox") != sandbox
        or payload.get("generation_id") != generation_id
        or payload.get("generation_started_at") != generation_started_at.isoformat()
        or payload.get("generation_expires_at") != generation_expires_at.isoformat()
        or payload.get("candidate_tree") != repository.get("candidate_tree")
        or evidence_binding.get("repository") != repository
        or evidence_binding.get("worker_env") != worker_env_binding
        or payload.get("cluster") != profile.cluster
        or payload.get("controller") != profile.controller
        or payload.get("submit_host") != profile.submit_host
        or payload.get("allowed_nodes") != list(profile.allowed_nodes)
        or payload.get("host_aliases") != _allowed_host_aliases(profile)
        or payload.get("account") != account
        or payload.get("qos") != _sandbox_qos(profile, sandbox)
        or payload.get("expected_pool") != expected_pool
        or payload.get("expected_concurrency") != expected_concurrency
        or payload.get("runtime_attestation") != runtime_attestation
        or payload.get("closed_world_verified") is not True
        or type(worker_env_binding.get("uid")) is not int
        or type(worker_env_binding.get("gid")) is not int
        or payload.get("batch_uid") != worker_env_binding.get("uid")
        or payload.get("batch_gid") != worker_env_binding.get("gid")
        or not isinstance(evidence_nodes, list)
        or len(evidence_nodes) != len(profile.allowed_nodes)
        or [item.get("node") for item in evidence_nodes if isinstance(item, dict)]
        != list(profile.allowed_nodes)
        or len(
            {item.get("node") for item in evidence_nodes if isinstance(item, dict)},
        )
        != len(profile.allowed_nodes)
        or [matrix["nodes"][node]["evidence"] for node in profile.allowed_nodes] != evidence_nodes
    ):
        raise PolicyError("allocation-side Slurm probe binding drifted")
    for node, evidence in zip(profile.allowed_nodes, evidence_nodes, strict=True):
        matrix_row = matrix["nodes"][node]
        attempt = matrix_row.get("attempts")
        compute_check = evidence.get("compute_check") if isinstance(evidence, dict) else None
        if (
            not isinstance(evidence, dict)
            or type(attempt) is not int
            or attempt < 1
            or evidence.get("node") != node
            or evidence.get("host") != profile.host_aliases[node]
            or evidence.get("job_name")
            != _allocation_job_name(
                sandbox,
                candidate_sha,
                node,
                attempt,
                generation_id=generation_id,
            )
            or re.fullmatch(r"[1-9][0-9]*", str(evidence.get("job_id", ""))) is None
            or evidence.get("state") != "COMPLETED"
            or evidence.get("sandbox") != sandbox
            or evidence.get("account") != account
            or evidence.get("qos") != _sandbox_qos(profile, sandbox)
            or evidence.get("batch_uid") != worker_env_binding["uid"]
            or evidence.get("batch_gid") != worker_env_binding["gid"]
            or evidence.get("sbatch_verified") is not True
            or evidence.get("srun_verified") is not True
            or evidence.get("nonexclusive") is not True
            or evidence.get("explicit_nodelist") != node
            or not isinstance(compute_check, dict)
            or type(compute_check.get("env_device")) is not int
            or not isinstance(compute_check.get("cgroup_parent"), str)
            or f"job_{evidence.get('job_id')}"
            not in str(compute_check.get("cgroup_parent")).split("/")
            or compute_check
            != {
                "schema_version": 1,
                "sandbox": sandbox,
                "account": account,
                "candidate_sha": candidate_sha,
                "candidate_tree": repository["candidate_tree"],
                "host": profile.host_aliases[node],
                "env_device": compute_check.get("env_device"),
                "env_inode": worker_env_binding["inode"],
                "env_sha256": worker_env_binding["sha256"],
                "pool": expected_pool,
                "concurrency": expected_concurrency,
                "docker_cgroup_driver": profile.docker_cgroup_driver,
                "job_id": evidence.get("job_id"),
                "cgroup_parent": compute_check.get("cgroup_parent"),
                "cgroup_guard_verified": True,
                "compose_verified": True,
            }
        ):
            raise PolicyError("allocation-side Slurm matrix node drifted")
        completed_at = _parse_allocation_timestamp(
            evidence.get("completed_at"),
            "allocation-side Slurm matrix node completion",
        )
        if (
            completed_at < generation_started_at
            or completed_at >= generation_expires_at
            or datetime.now(UTC) - completed_at > _ALLOCATION_PROBE_MAX_AGE
        ):
            raise PolicyError("allocation-side Slurm matrix node evidence is stale")
        if profile.gpu_tres_per_slot > 0 and (
            evidence.get("gpu_verified") is not True
            or not _positive_gpu_tres(str(evidence.get("alloc_tres", "")))
        ):
            raise PolicyError("allocation-side Slurm GPU probe drifted")
    return payload


def _allocation_probe_readback_candidate_locked(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_binding: Mapping[str, Any],
    runtime_attestation: Mapping[str, Any],
    expected_pool: str,
    expected_concurrency: int,
) -> dict[str, Any]:
    with _allocation_probe_lock(
        root,
        profile,
        sandbox,
        candidate_sha,
        enforce_root_ownership=root == Path("/"),
    ):
        return _allocation_probe_readback_unlocked(
            root,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_binding=candidate_binding,
            runtime_attestation=runtime_attestation,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
        )


def allocation_probe_readback(
    root: Path,
    profile: Profile,
    *,
    sandbox: str,
    candidate_sha: str,
    candidate_binding: Mapping[str, Any],
    runtime_attestation: Mapping[str, Any],
    expected_pool: str,
    expected_concurrency: int,
) -> dict[str, Any]:
    return _allocation_probe_readback_candidate_locked(
        root,
        profile,
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        candidate_binding=candidate_binding,
        runtime_attestation=runtime_attestation,
        expected_pool=expected_pool,
        expected_concurrency=expected_concurrency,
    )


def _live_readback_unlocked(
    root: Path,
    profile: Profile,
    *,
    sandbox: str | None,
    candidate_sha: str,
    candidate_bindings: Mapping[str, Any],
    require_probe: bool,
    check_accounting: bool = True,
    wait_for_guard: bool = False,
    candidate_binding: Mapping[str, Any] | None = None,
    runtime_attestation: Mapping[str, Any] | None = None,
    expected_pool: str | None = None,
    expected_concurrency: int | None = None,
    require_allocation_probe: bool = False,
    guard_not_before: datetime | None = None,
) -> dict[str, Any]:
    bindings = _candidate_bindings(profile, candidate_bindings)
    profile = _profile_with_bindings(profile, bindings)
    desired = desired_files(
        root,
        profile,
        candidate_sha=candidate_sha,
        candidate_bindings=bindings,
    )
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
        candidate_bindings=bindings,
        expected_config_sha256=expected_config_sha,
        require_probe=require_probe,
        sandbox=sandbox,
        not_before=guard_not_before,
    )
    accounting = _accounting_readback(profile) if check_accounting else None
    if require_allocation_probe:
        if (
            sandbox is None
            or candidate_binding is None
            or runtime_attestation is None
            or expected_pool is None
            or expected_concurrency is None
        ):
            raise PolicyError("strict matrix binding is required for allocation readback")
        allocation = _allocation_probe_readback_candidate_locked(
            root,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_binding=candidate_binding,
            runtime_attestation=runtime_attestation,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
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


def live_readback(
    root: Path,
    profile: Profile,
    *,
    sandbox: str | None,
    candidate_sha: str,
    candidate_bindings: Mapping[str, Any],
    require_probe: bool,
    check_accounting: bool = True,
    wait_for_guard: bool = False,
    candidate_binding: Mapping[str, Any] | None = None,
    runtime_attestation: Mapping[str, Any] | None = None,
    expected_pool: str | None = None,
    expected_concurrency: int | None = None,
    require_allocation_probe: bool = False,
) -> dict[str, Any]:
    with _domain_lock(root, profile):
        return _live_readback_unlocked(
            root,
            profile,
            sandbox=sandbox,
            candidate_sha=candidate_sha,
            candidate_bindings=candidate_bindings,
            require_probe=require_probe,
            check_accounting=check_accounting,
            wait_for_guard=wait_for_guard,
            candidate_binding=candidate_binding,
            runtime_attestation=runtime_attestation,
            expected_pool=expected_pool,
            expected_concurrency=expected_concurrency,
            require_allocation_probe=require_allocation_probe,
        )


def _recover_orphan(
    root: Path,
    profile: Profile,
    *,
    slurm_node: str | None,
) -> dict[str, Any] | None:
    path = _journal_path(root, profile)
    journal = _load_policy_journal(
        path,
        root=root,
        profile=profile,
        slurm_node=slurm_node,
    )
    if journal is None:
        return journal
    snapshot = _validate_snapshot_path(root, Path(journal["snapshot"]))
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
    _snapshot_manifest_rows(root, snapshot)
    archive_accounting = snapshot / "accounting-cas.json"
    if accounting_snapshot is None:
        if archive_accounting.exists() or archive_accounting.is_symlink():
            raise PolicyError("orphan Slurm policy snapshot has unexpected accounting state")
    else:
        _validated_accounting_snapshot(profile, accounting_snapshot)
    if journal.get("phase") in {"committed", "rolled_back"}:
        return journal
    drain: dict[str, Any] | None = None
    if root == Path("/") and journal.get("restart") is True and slurm_node is not None:
        drain = _acquire_restart_drain(
            root,
            profile,
            slurm_node=slurm_node,
            candidate_sha=str(journal["candidate_sha"]),
            candidate_bindings=journal["candidate_bindings"],
            transaction_id=str(journal["transaction_id"]),
            generation=int(journal["candidate_set_generation"]),
            convergence_id=str(journal["candidate_set_convergence_id"]),
            payload_sha256=str(journal["candidate_set_payload_sha256"]),
            operation=str(journal["operation"]),
            apply_accounting=(
                bool(journal["apply_accounting"]) if journal["operation"] == "apply" else False
            ),
        )
        _wait_for_restart_quiescence(root, profile, drain)
        _mark_restart_drain_transacting(root, profile, drain)
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
    if drain is not None:
        _release_restart_drain(root, profile, drain)
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
            raise PolicyError(f"host {host!r} is outside the infrastructure inventory")
        live_config = _parse_key_values(_run(("scontrol", "show", "config")))
        if live_config.get("ClusterName") != profile.cluster:
            raise PolicyError("live Slurm cluster identity does not match the profile")
        controllers = _split_csv(live_config.get("SlurmctldHost", ""))
        if controllers and profile.controller.lower() not in {item.lower() for item in controllers}:
            raise PolicyError("live Slurm controller identity does not match the profile")
        if apply_accounting and slurm_node != profile.controller:
            raise PolicyError("accounting apply is controller-only")
        if apply_accounting:
            cluster = _run(("sacctmgr", "-nP", "show", "cluster", "format=Cluster"))
            if profile.cluster not in {line.strip("|") for line in cluster.splitlines()}:
                raise PolicyError("live Slurm cluster identity does not match the profile")
    return host, slurm_node


def _committed_transaction_matches(
    journal: Mapping[str, Any] | None,
    *,
    operation: str,
    candidate_sha: str,
    candidate_bindings: Mapping[str, Mapping[str, str]],
    transaction: Mapping[str, str | int],
) -> bool:
    return bool(
        journal is not None
        and journal.get("phase") == "committed"
        and journal.get("operation") == operation
        and journal.get("candidate_sha") == candidate_sha
        and journal.get("candidate_set_sha256") == _candidate_set_sha256(candidate_bindings)
        and journal.get("candidate_bindings") == candidate_bindings
        and all(journal.get(field) == value for field, value in transaction.items())
    )


def _transaction_matches(
    journal: Mapping[str, Any] | None,
    *,
    phase: str,
    operation: str,
    candidate_sha: str,
    candidate_bindings: Mapping[str, Mapping[str, str]],
    transaction: Mapping[str, str | int],
) -> bool:
    return bool(
        journal is not None
        and journal.get("phase") == phase
        and journal.get("operation") == operation
        and journal.get("candidate_sha") == candidate_sha
        and journal.get("candidate_set_sha256") == _candidate_set_sha256(candidate_bindings)
        and journal.get("candidate_bindings") == candidate_bindings
        and all(journal.get(field) == value for field, value in transaction.items())
    )


def _release_committed_transaction_drain(
    root: Path,
    profile: Profile,
    *,
    slurm_node: str | None,
    journal: Mapping[str, Any],
) -> None:
    if root != Path("/") or journal.get("restart") is not True:
        return
    if slurm_node is None:
        raise PolicyError("committed Slurm transaction lacks an infrastructure node")
    drain = _load_drain_journal(
        root,
        profile,
        slurm_node=slurm_node,
    )
    if drain is None or drain.get("phase") == "released":
        return
    exact_fields = (
        "candidate_sha",
        "candidate_set_sha256",
        "candidate_bindings",
        "transaction_id",
        "candidate_set_generation",
        "candidate_set_convergence_id",
        "candidate_set_payload_sha256",
        "operation",
    )
    expected_drain_apply_accounting = (
        journal.get("apply_accounting") if journal.get("operation") == "apply" else False
    )
    if (
        any(drain.get(field) != journal.get(field) for field in exact_fields)
        or drain.get("apply_accounting") is not expected_drain_apply_accounting
    ):
        raise PolicyError("committed Slurm transaction drain identity drifted")
    _release_restart_drain(root, profile, drain)
    final = _load_drain_journal(
        root,
        profile,
        slurm_node=slurm_node,
    )
    if final is None or final.get("phase") != "released":
        raise PolicyError("committed Slurm transaction retained its owned drain")


def apply(
    root: Path,
    profile: Profile,
    *,
    restart: bool,
    apply_accounting: bool,
    candidate_sha: str | None = None,
    candidate_bindings: Mapping[str, Any] | None = None,
    transaction_id: str | None = None,
    generation: int | None = None,
    convergence_id: str | None = None,
    payload_sha256: str | None = None,
) -> dict[str, Any]:
    candidate = candidate_sha or source_candidate_sha()
    if candidate_bindings is None and root == Path("/"):
        raise PolicyError("live Slurm apply requires the complete candidate set")
    bindings = (
        _offline_candidate_bindings(profile, candidate)
        if candidate_bindings is None
        else _candidate_bindings(profile, candidate_bindings)
    )
    profile = _profile_with_bindings(profile, bindings)
    transaction = _transaction_identity(
        transaction_id=transaction_id,
        generation=generation,
        convergence_id=convergence_id,
        payload_sha256=payload_sha256,
        required=root == Path("/"),
    )
    _host, slurm_node = _validate_live_apply(
        root,
        profile,
        candidate_sha=candidate,
        restart=False,
        apply_accounting=False,
    )
    with _domain_lock(root, profile):
        recovered = _recover_orphan(root, profile, slurm_node=slurm_node)
        host, slurm_node = _validate_live_apply(
            root,
            profile,
            candidate_sha=candidate,
            restart=restart,
            apply_accounting=apply_accounting,
        )
        files = desired_files(
            root,
            profile,
            candidate_sha=candidate,
            candidate_bindings=bindings,
        )
        if root == Path("/") and _committed_transaction_matches(
            recovered,
            operation="apply",
            candidate_sha=candidate,
            candidate_bindings=bindings,
            transaction=transaction,
        ):
            assert recovered is not None
            snapshot = _validate_snapshot_path(root, Path(str(recovered["snapshot"])))
            live = _live_readback_unlocked(
                root,
                profile,
                sandbox=None,
                candidate_sha=candidate,
                candidate_bindings=bindings,
                require_probe=False,
                check_accounting=apply_accounting,
                wait_for_guard=True,
            )
            _release_committed_transaction_drain(
                root,
                profile,
                slurm_node=slurm_node,
                journal=recovered,
            )
            return {
                **plan(
                    root,
                    profile,
                    candidate_sha=candidate,
                    candidate_bindings=bindings,
                ),
                "mutation_authorized": True,
                "snapshot": str(snapshot),
                "journal": str(_journal_path(root, profile)),
                "phase": "committed",
                "restart_requested": restart,
                "accounting_requested": apply_accounting,
                "live_readback": live,
                "replayed": True,
            }
        if root == Path("/") and recovered is not None and recovered.get("phase") == "committed":
            previous_generation = int(recovered["candidate_set_generation"])
            requested_generation = int(transaction["candidate_set_generation"])
            if requested_generation != previous_generation + 1:
                raise PolicyError("Slurm candidate-set generation regressed or skipped")
        if root == Path("/"):
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                prefix="loom-dockerd-validate-",
            ) as handle:
                handle.write(files[root / "etc/docker/daemon.json"])
                handle.flush()
                _run(("dockerd", "--validate", "--config-file", handle.name))
        drain: dict[str, Any] | None = None
        if root == Path("/") and restart:
            if slurm_node is None:
                raise PolicyError("live restart lacks an infrastructure Slurm node")
            drain = _acquire_restart_drain(
                root,
                profile,
                slurm_node=slurm_node,
                candidate_sha=candidate,
                candidate_bindings=bindings,
                transaction_id=str(transaction["transaction_id"]),
                generation=int(transaction["candidate_set_generation"]),
                convergence_id=str(transaction["candidate_set_convergence_id"]),
                payload_sha256=str(transaction["candidate_set_payload_sha256"]),
                operation="apply",
                apply_accounting=apply_accounting,
            )
            _wait_for_restart_quiescence(root, profile, drain)
        try:
            snapshot = _snapshot(root, files)
            accounting_snapshot = (
                _accounting_snapshot(root, profile, snapshot) if apply_accounting else None
            )
        except Exception as exc:
            if drain is not None:
                try:
                    _release_restart_drain(root, profile, drain)
                except Exception as release_exc:
                    raise PolicyError(
                        "Slurm apply pre-transaction failure retained its owned drain",
                    ) from release_exc
            if isinstance(exc, PolicyError):
                raise
            raise PolicyError("Slurm apply pre-transaction validation failed safely") from exc
        if drain is not None:
            _mark_restart_drain_transacting(root, profile, drain)
        journal_path = _journal_path(root, profile)
        created_at = datetime.now(UTC).isoformat()
        journal: dict[str, Any] = {
            "schema_version": 1,
            "operation": "apply",
            "cluster": profile.cluster,
            "host": host,
            "slurm_node": slurm_node,
            "candidate_sha": candidate,
            "candidate_set_sha256": _candidate_set_sha256(bindings),
            "candidate_bindings": bindings,
            **transaction,
            "snapshot": str(snapshot),
            "accounting_snapshot": (
                str(accounting_snapshot) if accounting_snapshot is not None else None
            ),
            "restart": restart,
            "apply_accounting": apply_accounting,
            "phase": "prepared",
            "created_at": created_at,
            "updated_at": created_at,
        }
        _write_journal(journal_path, journal)
        guard_restart_boundary: datetime | None = None
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
                accounting_payload = _validated_accounting_snapshot(
                    profile,
                    accounting_snapshot,
                )
                _apply_accounting(profile, accounting_payload)
            _advance_journal(journal_path, journal, "accounting_applied")
            if restart:
                if slurm_node is None:
                    raise PolicyError("live restart lacks an allowed Slurm node")
                guard_restart_boundary = _restart_services(profile, slurm_node)
            _advance_journal(journal_path, journal, "services_reconfigured")
            if root == Path("/"):
                live = _live_readback_unlocked(
                    root,
                    profile,
                    sandbox=None,
                    candidate_sha=candidate,
                    candidate_bindings=bindings,
                    require_probe=False,
                    check_accounting=apply_accounting,
                    wait_for_guard=True,
                    guard_not_before=guard_restart_boundary,
                )
            else:
                rendered = plan(
                    root,
                    profile,
                    candidate_sha=candidate,
                    candidate_bindings=bindings,
                )
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
            if drain is not None:
                _release_restart_drain(root, profile, drain)
            if isinstance(exc, PolicyError):
                raise
            raise PolicyError("Slurm policy apply failed and was rolled back") from exc
        if drain is not None:
            _release_restart_drain(root, profile, drain)
    return {
        **plan(
            root,
            profile,
            candidate_sha=candidate,
            candidate_bindings=bindings,
        ),
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
    candidate_bindings: Mapping[str, Any] | None = None,
    transaction_id: str | None = None,
    generation: int | None = None,
    convergence_id: str | None = None,
    payload_sha256: str | None = None,
) -> dict[str, Any]:
    current_candidate = candidate_sha or source_candidate_sha()
    if candidate_bindings is None and root == Path("/"):
        raise PolicyError("live Slurm rollback requires the complete candidate set")
    bindings = (
        _offline_candidate_bindings(profile, current_candidate)
        if candidate_bindings is None
        else _candidate_bindings(profile, candidate_bindings)
    )
    profile = _profile_with_bindings(profile, bindings)
    transaction_identity = _transaction_identity(
        transaction_id=transaction_id,
        generation=generation,
        convergence_id=convergence_id,
        payload_sha256=payload_sha256,
        required=root == Path("/"),
    )
    _host, slurm_node = _validate_live_apply(
        root,
        profile,
        candidate_sha=current_candidate,
        restart=False,
        apply_accounting=False,
    )
    with _domain_lock(root, profile):
        recovered = _recover_orphan(root, profile, slurm_node=slurm_node)
        host, slurm_node = _validate_live_apply(
            root,
            profile,
            candidate_sha=current_candidate,
            restart=root == Path("/"),
            apply_accounting=False,
        )
        journal_path = _journal_path(root, profile)
        if root == Path("/") and _committed_transaction_matches(
            recovered,
            operation="rollback",
            candidate_sha=current_candidate,
            candidate_bindings=bindings,
            transaction=transaction_identity,
        ):
            assert recovered is not None
            restored = _validate_snapshot_path(
                root,
                Path(str(recovered["rollback_target"])),
            )
            recovery = _validate_snapshot_path(root, Path(str(recovered["snapshot"])))
            live = _snapshot_readback(root, restored)
            if recovered.get("apply_accounting") is True:
                restored_accounting = _validate_accounting_snapshot_path(
                    root,
                    restored,
                    restored / "accounting-cas.json",
                )
                _accounting_snapshot_matches(profile, restored_accounting)
            guard_config = root / "etc/loom/slurm-job-cgroup-guard.json"
            if guard_config.exists():
                live["guard"] = _wait_for_restored_guard_status(
                    root,
                    profile,
                    guard_config=guard_config,
                )
            _release_committed_transaction_drain(
                root,
                profile,
                slurm_node=slurm_node,
                journal=recovered,
            )
            return {
                "schema_version": 1,
                "artifact_type": "developer-sandbox-slurm-policy-rollback",
                "cluster": profile.cluster,
                "mutation_authorized": True,
                "restored_snapshot": str(restored),
                "recovery_snapshot": str(recovery),
                "journal": str(journal_path),
                "phase": "committed",
                "live_readback": live,
                "replayed": True,
            }
        retry_after_recovery = root == Path("/") and _transaction_matches(
            recovered,
            phase="rolled_back",
            operation="rollback",
            candidate_sha=current_candidate,
            candidate_bindings=bindings,
            transaction=transaction_identity,
        )
        previous: dict[str, Any] | None
        if retry_after_recovery:
            assert recovered is not None
            previous = recovered
            target_raw = previous.get("rollback_target")
        else:
            previous = _load_policy_journal(
                journal_path,
                root=root,
                profile=profile,
                slurm_node=slurm_node,
            )
            if (
                previous is None
                or previous.get("phase") != "committed"
                or previous.get("operation") != "apply"
            ):
                raise PolicyError("no committed Slurm policy transaction is available to roll back")
            if any(
                previous.get(field) != transaction_identity[field]
                for field in (
                    "candidate_set_generation",
                    "candidate_set_convergence_id",
                    "candidate_set_payload_sha256",
                )
            ):
                raise PolicyError("Slurm rollback candidate-set transaction identity drifted")
            target_raw = previous.get("snapshot")
        if not isinstance(target_raw, str):
            raise PolicyError("committed Slurm policy transaction lacks a snapshot")
        target = _validate_snapshot_path(root, Path(target_raw))
        previous_accounting_raw = (
            str(target / "accounting-cas.json")
            if retry_after_recovery and previous.get("apply_accounting") is True
            else None
            if retry_after_recovery
            else previous.get("accounting_snapshot")
        )
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
            candidate_bindings=bindings,
        )
        drain: dict[str, Any] | None = None
        if root == Path("/"):
            if slurm_node is None:
                raise PolicyError("live rollback lacks an infrastructure Slurm node")
            drain = _acquire_restart_drain(
                root,
                profile,
                slurm_node=slurm_node,
                candidate_sha=current_candidate,
                candidate_bindings=bindings,
                transaction_id=str(transaction_identity["transaction_id"]),
                generation=int(transaction_identity["candidate_set_generation"]),
                convergence_id=str(transaction_identity["candidate_set_convergence_id"]),
                payload_sha256=str(transaction_identity["candidate_set_payload_sha256"]),
                operation="rollback",
                apply_accounting=False,
            )
            _wait_for_restart_quiescence(root, profile, drain)
        try:
            current_snapshot = _snapshot(root, current_files)
            current_accounting = (
                _accounting_snapshot(root, profile, current_snapshot)
                if previous_accounting is not None
                else None
            )
        except Exception as exc:
            if drain is not None:
                try:
                    _release_restart_drain(root, profile, drain)
                except Exception as release_exc:
                    raise PolicyError(
                        "Slurm rollback pre-transaction failure retained its owned drain",
                    ) from release_exc
            if isinstance(exc, PolicyError):
                raise
            raise PolicyError("Slurm rollback pre-transaction validation failed safely") from exc
        if drain is not None:
            _mark_restart_drain_transacting(root, profile, drain)
        created_at = datetime.now(UTC).isoformat()
        transaction: dict[str, Any] = {
            "schema_version": 1,
            "operation": "rollback",
            "cluster": profile.cluster,
            "host": host,
            "slurm_node": slurm_node,
            "candidate_sha": current_candidate,
            "candidate_set_sha256": _candidate_set_sha256(bindings),
            "candidate_bindings": bindings,
            **transaction_identity,
            "snapshot": str(current_snapshot),
            "accounting_snapshot": (
                str(current_accounting) if current_accounting is not None else None
            ),
            "rollback_target": str(target),
            "restart": root == Path("/"),
            "apply_accounting": current_accounting is not None,
            "phase": "prepared",
            "created_at": created_at,
            "updated_at": created_at,
        }
        _write_journal(journal_path, transaction)
        guard_restart_boundary: datetime | None = None
        try:
            _restore_snapshot(root, target)
            _advance_journal(journal_path, transaction, "files_written")
            if previous_accounting is not None:
                _restore_accounting(profile, previous_accounting)
            _advance_journal(journal_path, transaction, "accounting_applied")
            if root == Path("/"):
                if slurm_node is None:
                    raise PolicyError("live rollback lacks an allowed Slurm node")
                guard_restart_boundary = _restore_services(root, profile, slurm_node)
            _advance_journal(journal_path, transaction, "services_reconfigured")
            guard_config = root / "etc/loom/slurm-job-cgroup-guard.json"
            live = _snapshot_readback(root, target)
            if previous_accounting is not None:
                _accounting_snapshot_matches(profile, previous_accounting)
            if guard_config.exists() and root == Path("/"):
                live["guard"] = _wait_for_restored_guard_status(
                    root,
                    profile,
                    guard_config=guard_config,
                    not_before=guard_restart_boundary,
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
            if drain is not None:
                _release_restart_drain(root, profile, drain)
            if isinstance(exc, PolicyError):
                raise
            raise PolicyError("Slurm policy rollback failed safely") from exc
        if drain is not None:
            _release_restart_drain(root, profile, drain)
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


def _incremental_identity_payload(raw: bytes, profile: Profile) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("incremental Slurm identity payload is invalid") from exc
    if (
        not isinstance(payload, dict)
        or (
            not (
                payload.get("schema_version") == 1
                and set(payload) == _INCREMENTAL_IDENTITY_V1_FIELDS
            )
            and not (
                payload.get("schema_version") == 2 and set(payload) == _INCREMENTAL_IDENTITY_FIELDS
            )
        )
        or raw != _canonical_json_bytes(payload) + b"\n"
        or payload.get("kind") != "loom.developer-environment.identity-preflight"
        or _REGISTRY.ENV_ID_RE.fullmatch(str(payload.get("env_id"))) is None
        or (
            payload.get("schema_version") == 2
            and _REGISTRY.PRINCIPAL_RE.fullmatch(
                str(payload.get("principal_id")),
            )
            is None
        )
        or type(payload.get("resource_generation")) is not int
        or int(payload["resource_generation"]) < 1
        or _REGISTRY.SAFE_NAME_RE.fullmatch(str(payload.get("service_user"))) is None
        or _REGISTRY.SAFE_NAME_RE.fullmatch(str(payload.get("service_group"))) is None
        or type(payload.get("uid")) is not int
        or int(payload["uid"]) < 1
        or payload.get("gid") != payload["uid"]
        or _SAFE_NAME.fullmatch(str(payload.get("slurm_account"))) is None
        or _SAFE_NAME.fullmatch(str(payload.get("slurm_qos"))) is None
        or payload.get("slurm_account") == profile.parent_account
        or type(payload.get("registry_generation")) is not int
        or int(payload["registry_generation"]) < 1
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(payload.get("registry_payload_sha256")),
        )
        is None
        or _REGISTRY.DIGEST_RE.fullmatch(str(payload.get("candidate_set_sha256"))) is None
        or (
            payload.get("schema_version") == 2
            and payload.get("revive_journal_sha256") is not None
            and _REGISTRY.DIGEST_RE.fullmatch(
                str(payload.get("revive_journal_sha256")),
            )
            is None
        )
    ):
        raise PolicyError("incremental Slurm identity payload is invalid")
    return payload


def _single_accounting_row(
    argv: Sequence[str],
    *,
    width: int,
    label: str,
) -> list[str] | None:
    rows = [line.split("|") for line in _run(argv).splitlines() if line.strip()]
    if len(rows) > 1 or any(len(row) < width for row in rows):
        raise PolicyError(f"incremental Slurm {label} readback is ambiguous")
    return rows[0] if rows else None


def _incremental_accounting_state(
    profile: Profile,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    qos = str(identity["slurm_qos"])
    account = str(identity["slurm_account"])
    user = str(identity["service_user"])
    qos_row = _single_accounting_row(
        (
            "sacctmgr",
            "-nP",
            "show",
            "qos",
            "where",
            f"name={qos}",
            "format=Name,Priority,MaxWall,MaxJobsPU,MaxSubmitJobsPU,Flags",
        ),
        width=6,
        label="QoS",
    )
    account_row = _single_accounting_row(
        (
            "sacctmgr",
            "-nP",
            "show",
            "account",
            "where",
            f"cluster={profile.cluster}",
            f"account={account}",
            "format=Account,ParentName,Fairshare",
        ),
        width=3,
        label="account",
    )
    association_row = _single_accounting_row(
        (
            "sacctmgr",
            "-nP",
            "show",
            "association",
            "where",
            f"cluster={profile.cluster}",
            f"account={account}",
            f"user={user}",
            "format=User,Account,Fairshare,QOS,DefaultQOS",
        ),
        width=5,
        label="association",
    )
    return {
        "qos": None
        if qos_row is None
        else {
            "Name": qos_row[0],
            "Priority": qos_row[1],
            "MaxWall": qos_row[2],
            "MaxJobsPU": qos_row[3],
            "MaxSubmitJobsPU": qos_row[4],
            "Flags": qos_row[5],
        },
        "account": None
        if account_row is None
        else {
            "Account": account_row[0],
            "ParentName": account_row[1],
            "Fairshare": account_row[2],
        },
        "association": None
        if association_row is None
        else {
            "User": association_row[0],
            "Account": association_row[1],
            "Fairshare": association_row[2],
            "QOS": association_row[3],
            "DefaultQOS": association_row[4],
        },
    }


def _incremental_desired_state(
    profile: Profile,
    identity: Mapping[str, Any],
    *,
    retired: bool,
) -> dict[str, Any]:
    qos = str(identity["slurm_qos"])
    account = str(identity["slurm_account"])
    user = str(identity["service_user"])
    return {
        "qos": {
            "Name": qos,
            "Priority": "0" if retired else str(profile.qos_priority),
            "MaxWall": profile.qos_max_wall,
            "MaxJobsPU": "0" if retired else str(profile.qos_max_jobs_per_user),
            "MaxSubmitJobsPU": ("0" if retired else str(profile.qos_max_submit_jobs_per_user)),
            "Flags": "DenyOnLimit",
        },
        "account": {
            "Account": account,
            "ParentName": profile.parent_account,
            "Fairshare": "0" if retired else str(profile.fairshare),
        },
        "association": {
            "User": user,
            "Account": account,
            "Fairshare": "0" if retired else str(profile.fairshare),
            "QOS": qos,
            "DefaultQOS": qos,
        },
    }


def _incremental_accounting_status(
    profile: Profile,
    identity: Mapping[str, Any],
) -> tuple[str, dict[str, Any]]:
    state = _incremental_accounting_state(profile, identity)
    if all(value is None for value in state.values()):
        return "available", state
    if state == _incremental_desired_state(profile, identity, retired=False):
        return "exact-existing", state
    if state == _incremental_desired_state(profile, identity, retired=True):
        return "retired", state
    raise PolicyError("incremental Slurm identity collides with existing accounting state")


def _incremental_transaction_path(
    root: Path,
    profile: Profile,
    transaction_id: str,
) -> Path:
    return (
        root
        / "var/lib/loom-developer-sandbox-slurm-policy/incremental-transactions"
        / profile.cluster
        / f"{transaction_id}.json"
    )


def _incremental_transaction(
    root: Path,
    profile: Profile,
    identity: Mapping[str, Any],
    *,
    transaction_id: str,
    operation: str,
    phase: str | None = None,
) -> dict[str, Any] | None:
    if (
        _REGISTRY.DIGEST_RE.fullmatch(transaction_id) is None
        or operation not in {"reconcile", "retire"}
        or phase not in {None, "prepared", "committed"}
    ):
        raise PolicyError("incremental Slurm transaction binding is invalid")
    path = _incremental_transaction_path(root, profile, transaction_id)
    existing = _load_journal(path)
    payload_sha256 = hashlib.sha256(_canonical_json_bytes(identity)).hexdigest()
    if existing is not None and (
        set(existing)
        != {
            "schema_version",
            "kind",
            "transaction_id",
            "operation",
            "cluster",
            "env_id",
            "resource_generation",
            "payload_sha256",
            "phase",
            "created_at",
            "updated_at",
        }
        or existing.get("schema_version") != 1
        or existing.get("kind") != "loom.developer-environment.slurm-identity-transaction"
        or existing.get("transaction_id") != transaction_id
        or existing.get("operation") != operation
        or existing.get("cluster") != profile.cluster
        or existing.get("env_id") != identity["env_id"]
        or existing.get("resource_generation") != identity["resource_generation"]
        or existing.get("payload_sha256") != payload_sha256
        or existing.get("phase") not in {"prepared", "committed"}
    ):
        raise PolicyError("incremental Slurm transaction binding drifted")
    if phase is None:
        return existing
    now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
    updated = {
        "schema_version": 1,
        "kind": "loom.developer-environment.slurm-identity-transaction",
        "transaction_id": transaction_id,
        "operation": operation,
        "cluster": profile.cluster,
        "env_id": identity["env_id"],
        "resource_generation": identity["resource_generation"],
        "payload_sha256": payload_sha256,
        "phase": phase,
        "created_at": existing["created_at"] if existing is not None else now,
        "updated_at": now,
    }
    _prepare_private_directory(
        path.parent,
        enforce_root_ownership=root == Path("/"),
        create=True,
    )
    _atomic_write(
        path,
        (_canonical_json_bytes(updated) + b"\n").decode("ascii"),
        mode=0o600,
    )
    rebound = _load_journal(path)
    if rebound != updated:
        raise PolicyError("incremental Slurm transaction publication drifted")
    return updated


def _incremental_jobs(profile: Profile, identity: Mapping[str, Any]) -> list[dict[str, str]]:
    account = str(identity["slurm_account"])
    user = str(identity["service_user"])
    rows = [
        line.split("|")
        for line in _run(
            (
                "squeue",
                "--noheader",
                f"--clusters={profile.cluster}",
                f"--accounts={account}",
                f"--user={user}",
                "--format=%i|%T|%a|%u",
            ),
        ).splitlines()
        if line.strip()
    ]
    if any(
        len(row) != 4
        or re.fullmatch(r"[1-9][0-9]*(?:_[0-9]+)?", row[0]) is None
        or re.fullmatch(r"[A-Z][A-Z0-9_+*~-]{1,63}", row[1]) is None
        or row[2] != account
        or row[3] != user
        for row in rows
    ):
        raise PolicyError("incremental Slurm job ownership readback is invalid")
    return [{"job_id": row[0], "state": row[1], "account": row[2], "user": row[3]} for row in rows]


def incremental_identity_check(
    profile: Profile,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    status, state = _incremental_accounting_status(profile, identity)
    jobs = _incremental_jobs(profile, identity)
    return {
        "schema_version": 1,
        "kind": "loom.developer-environment.slurm-identity-result",
        "operation": "check",
        "cluster": profile.cluster,
        "env_id": identity["env_id"],
        "resource_generation": identity["resource_generation"],
        "service_user": identity["service_user"],
        "slurm_account": identity["slurm_account"],
        "slurm_qos": identity["slurm_qos"],
        "status": status,
        "jobs": jobs,
        "state_sha256": hashlib.sha256(_canonical_json_bytes(state)).hexdigest(),
        "mutations": [],
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }


def _incremental_revive_binding(
    root: Path,
    profile: Profile,
    identity: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    expected_journal_sha = identity.get("revive_journal_sha256")
    if _REGISTRY.DIGEST_RE.fullmatch(str(expected_journal_sha)) is None:
        raise PolicyError("retired incremental Slurm identity lacks a revive journal")
    journal_path = (
        root / "var/lib/loom-developer-environment-runtime/revive" / f"{identity['env_id']}.json"
    )
    journal = _load_journal(journal_path)
    tombstone_fields = {
        "schema_version",
        "kind",
        "cluster",
        "env_id",
        "principal_id",
        "resource_generation",
        "service_user",
        "service_group",
        "uid",
        "gid",
        "slurm_account",
        "slurm_qos",
        "registry_generation",
        "registry_payload_sha256",
        "state_sha256",
        "retired_at",
        "payload_sha256",
    }
    journal_fields = {
        "schema_version",
        "kind",
        "phase",
        "env_id",
        "principal_id",
        "runtime_id",
        "uid",
        "gid",
        "service_user",
        "service_group",
        "slurm_user",
        "slurm_account",
        "slurm_qos",
        "previous_resource_generation",
        "new_resource_generation",
        "registry_generation",
        "registry_payload_sha256",
        "retire_tombstone_sha256",
        "idempotency_key",
        "created_at",
        "updated_at",
        "payload_sha256",
    }
    if not isinstance(journal, dict):
        raise PolicyError("incremental Slurm revive journal is unavailable")
    previous_generation = journal.get("previous_resource_generation")
    if type(previous_generation) is not int or previous_generation < 2:
        raise PolicyError("incremental Slurm revive generation is invalid")
    tombstone_path = (
        root
        / "var/lib/loom-developer-sandbox-slurm-policy/identity-tombstones"
        / profile.cluster
        / str(identity["env_id"])
        / f"{previous_generation - 1}.json"
    )
    tombstone = _load_journal(tombstone_path)
    tombstone_unsigned = (
        {field: value for field, value in tombstone.items() if field != "payload_sha256"}
        if isinstance(tombstone, dict)
        else {}
    )
    journal_unsigned = (
        {field: value for field, value in journal.items() if field != "payload_sha256"}
        if isinstance(journal, dict)
        else {}
    )
    if (
        not isinstance(tombstone, dict)
        or set(tombstone) != tombstone_fields
        or tombstone.get("schema_version") != 1
        or tombstone.get("kind") != "loom.developer-environment.slurm-identity-tombstone"
        or tombstone.get("cluster") != profile.cluster
        or tombstone.get("payload_sha256")
        != hashlib.sha256(_canonical_json_bytes(tombstone_unsigned)).hexdigest()
        or not isinstance(journal, dict)
        or set(journal) != journal_fields
        or journal.get("schema_version") != 1
        or journal.get("kind") != "loom.developer-environment.revive-journal"
        or journal.get("phase") not in {"registered", "capacity-restored"}
        or _REGISTRY.RUNTIME_ID_RE.fullmatch(str(journal.get("runtime_id"))) is None
        or _REGISTRY.IDEMPOTENCY_RE.fullmatch(
            str(journal.get("idempotency_key")),
        )
        is None
        or not isinstance(journal.get("created_at"), str)
        or not isinstance(journal.get("updated_at"), str)
        or journal.get("payload_sha256") != expected_journal_sha
        or journal.get("payload_sha256")
        != hashlib.sha256(_canonical_json_bytes(journal_unsigned)).hexdigest()
        or journal.get("previous_resource_generation") != tombstone["resource_generation"] + 1
        or journal.get("new_resource_generation") != tombstone["resource_generation"] + 2
        or identity["resource_generation"] != journal["new_resource_generation"]
        or journal.get("registry_generation") != identity["registry_generation"]
        or journal.get("registry_payload_sha256") != identity["registry_payload_sha256"]
        or any(
            tombstone.get(field) != identity[field]
            for field in (
                "env_id",
                "principal_id",
                "service_user",
                "service_group",
                "uid",
                "gid",
                "slurm_account",
                "slurm_qos",
            )
        )
        or any(
            journal.get(field) != identity[field]
            for field in (
                "env_id",
                "principal_id",
                "service_user",
                "service_group",
                "uid",
                "gid",
                "slurm_account",
                "slurm_qos",
            )
        )
        or journal.get("slurm_user") != identity["service_user"]
    ):
        raise PolicyError("incremental Slurm revive binding is invalid")
    _parse_allocation_timestamp(
        str(tombstone["retired_at"]),
        "incremental Slurm retirement",
    )
    _parse_allocation_timestamp(
        str(journal["created_at"]),
        "incremental Slurm revive journal",
    )
    _parse_allocation_timestamp(
        str(journal["updated_at"]),
        "incremental Slurm revive journal",
    )
    return tombstone, journal


def incremental_identity_reconcile(
    root: Path,
    profile: Profile,
    identity: Mapping[str, Any],
    *,
    transaction_id: str,
) -> dict[str, Any]:
    transaction = _incremental_transaction(
        root,
        profile,
        identity,
        transaction_id=transaction_id,
        operation="reconcile",
    )
    if transaction is None:
        status, _state = _incremental_accounting_status(profile, identity)
        transaction = _incremental_transaction(
            root,
            profile,
            identity,
            transaction_id=transaction_id,
            operation="reconcile",
            phase="prepared",
        )
    elif transaction["phase"] == "committed":
        status, _state = _incremental_accounting_status(profile, identity)
        if status != "exact-existing":
            raise PolicyError("committed incremental Slurm identity drifted")
    else:
        status = "recovering"
    mutations: list[str] = []
    revive: tuple[dict[str, Any], dict[str, Any]] | None = None
    if status == "retired" or (
        status == "recovering" and identity.get("revive_journal_sha256") is not None
    ):
        revive = _incremental_revive_binding(root, profile, identity)
    if status in {"available", "recovering", "retired"}:
        qos = str(identity["slurm_qos"])
        account = str(identity["slurm_account"])
        user = str(identity["service_user"])
        commands = (
            ("sacctmgr", "-i", "add", "qos", qos),
            (
                "sacctmgr",
                "-i",
                "modify",
                "qos",
                "where",
                f"name={qos}",
                "set",
                f"Priority={profile.qos_priority}",
                f"MaxWall={profile.qos_max_wall}",
                f"MaxJobsPerUser={profile.qos_max_jobs_per_user}",
                f"MaxSubmitJobsPerUser={profile.qos_max_submit_jobs_per_user}",
                "Flags=DenyOnLimit",
            ),
            (
                "sacctmgr",
                "-i",
                "add",
                "account",
                account,
                f"Parent={profile.parent_account}",
                f"Description=Loom environment {identity['env_id']}",
                "Organization=loom",
            ),
            (
                "sacctmgr",
                "-i",
                "modify",
                "account",
                "where",
                f"cluster={profile.cluster}",
                f"account={account}",
                "set",
                f"Fairshare={profile.fairshare}",
            ),
            ("sacctmgr", "-i", "add", "user", user, f"Account={account}"),
            (
                "sacctmgr",
                "-i",
                "modify",
                "user",
                "where",
                f"name={user}",
                f"account={account}",
                f"cluster={profile.cluster}",
                "set",
                f"Fairshare={profile.fairshare}",
                f"QOS={qos}",
                f"DefaultQOS={qos}",
            ),
        )
        for command in commands:
            _run(command)
            mutations.append(" ".join(command[:4]))
    rebound_status, rebound = _incremental_accounting_status(profile, identity)
    if rebound_status != "exact-existing":
        raise PolicyError("incremental Slurm identity did not converge exactly")
    result = incremental_identity_check(profile, identity)
    if result["status"] != "exact-existing":
        raise PolicyError("incremental Slurm identity readback drifted")
    if revive is not None:
        tombstone, journal = revive
        revival_unsigned = {
            "schema_version": 1,
            "kind": "loom.developer-environment.slurm-identity-revival",
            "cluster": profile.cluster,
            "env_id": identity["env_id"],
            "principal_id": identity["principal_id"],
            "previous_resource_generation": tombstone["resource_generation"],
            "resource_generation": identity["resource_generation"],
            "registry_generation": identity["registry_generation"],
            "registry_payload_sha256": identity["registry_payload_sha256"],
            "retire_tombstone_sha256": tombstone["payload_sha256"],
            "revive_journal_sha256": journal["payload_sha256"],
            "state_sha256": hashlib.sha256(
                _canonical_json_bytes(rebound),
            ).hexdigest(),
            "restored_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
        revival = {
            **revival_unsigned,
            "payload_sha256": hashlib.sha256(
                _canonical_json_bytes(revival_unsigned),
            ).hexdigest(),
        }
        revival_path = (
            root
            / "var/lib/loom-developer-sandbox-slurm-policy/identity-revivals"
            / profile.cluster
            / str(identity["env_id"])
            / f"{identity['resource_generation']}.json"
        )
        _prepare_private_directory(
            revival_path.parent,
            enforce_root_ownership=root == Path("/"),
            create=True,
        )
        existing_revival = _load_journal(revival_path)
        if existing_revival is None:
            _atomic_write(
                revival_path,
                (_canonical_json_bytes(revival) + b"\n").decode("ascii"),
                mode=0o600,
            )
        elif existing_revival != revival:
            raise PolicyError("incremental Slurm revival receipt replay drifted")
    _incremental_transaction(
        root,
        profile,
        identity,
        transaction_id=transaction_id,
        operation="reconcile",
        phase="committed",
    )
    result.update(
        {
            "operation": "reconcile",
            "mutations": mutations,
            "state_sha256": hashlib.sha256(
                _canonical_json_bytes(rebound),
            ).hexdigest(),
        },
    )
    return result


def incremental_identity_retire(
    root: Path,
    profile: Profile,
    identity: Mapping[str, Any],
    *,
    transaction_id: str,
) -> dict[str, Any]:
    jobs = _incremental_jobs(profile, identity)
    if jobs:
        raise PolicyError("incremental Slurm identity still owns jobs")
    transaction = _incremental_transaction(
        root,
        profile,
        identity,
        transaction_id=transaction_id,
        operation="retire",
    )
    if transaction is None:
        status, _state = _incremental_accounting_status(profile, identity)
        transaction = _incremental_transaction(
            root,
            profile,
            identity,
            transaction_id=transaction_id,
            operation="retire",
            phase="prepared",
        )
    elif transaction["phase"] == "committed":
        status, _state = _incremental_accounting_status(profile, identity)
        if status != "retired":
            raise PolicyError("committed incremental Slurm retirement drifted")
    else:
        status = "recovering"
    if status == "available":
        raise PolicyError("incremental Slurm identity is unavailable for retirement")
    mutations: list[str] = []
    if status != "retired":
        qos = str(identity["slurm_qos"])
        account = str(identity["slurm_account"])
        user = str(identity["service_user"])
        commands = (
            (
                "sacctmgr",
                "-i",
                "modify",
                "qos",
                "where",
                f"name={qos}",
                "set",
                "Priority=0",
                "MaxJobsPerUser=0",
                "MaxSubmitJobsPerUser=0",
                "Flags=DenyOnLimit",
            ),
            (
                "sacctmgr",
                "-i",
                "modify",
                "account",
                "where",
                f"cluster={profile.cluster}",
                f"account={account}",
                "set",
                "Fairshare=0",
            ),
            (
                "sacctmgr",
                "-i",
                "modify",
                "user",
                "where",
                f"name={user}",
                f"account={account}",
                f"cluster={profile.cluster}",
                "set",
                "Fairshare=0",
                f"QOS={qos}",
                f"DefaultQOS={qos}",
            ),
        )
        for command in commands:
            _run(command)
            mutations.append(" ".join(command[:4]))
    rebound_status, rebound = _incremental_accounting_status(profile, identity)
    if rebound_status != "retired" or _incremental_jobs(profile, identity):
        raise PolicyError("incremental Slurm identity retirement did not read back exactly")
    tombstone_unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.slurm-identity-tombstone",
        "cluster": profile.cluster,
        "env_id": identity["env_id"],
        "principal_id": identity["principal_id"],
        "resource_generation": identity["resource_generation"],
        "service_user": identity["service_user"],
        "service_group": identity["service_group"],
        "uid": identity["uid"],
        "gid": identity["gid"],
        "slurm_account": identity["slurm_account"],
        "slurm_qos": identity["slurm_qos"],
        "registry_generation": identity["registry_generation"],
        "registry_payload_sha256": identity["registry_payload_sha256"],
        "state_sha256": hashlib.sha256(_canonical_json_bytes(rebound)).hexdigest(),
        "retired_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    tombstone = {
        **tombstone_unsigned,
        "payload_sha256": hashlib.sha256(
            _canonical_json_bytes(tombstone_unsigned),
        ).hexdigest(),
    }
    tombstone_root = (
        root
        / "var/lib/loom-developer-sandbox-slurm-policy/identity-tombstones"
        / profile.cluster
        / str(identity["env_id"])
    )
    _prepare_private_directory(
        tombstone_root,
        enforce_root_ownership=root == Path("/"),
        create=True,
    )
    tombstone_path = tombstone_root / f"{identity['resource_generation']}.json"
    existing = _load_journal(tombstone_path)
    if existing is None:
        _atomic_write(
            tombstone_path,
            (_canonical_json_bytes(tombstone) + b"\n").decode("ascii"),
            mode=0o600,
        )
    else:
        existing_unsigned = {
            field: value for field, value in existing.items() if field != "payload_sha256"
        }
        if (
            set(existing) != set(tombstone)
            or any(
                existing.get(field) != tombstone[field]
                for field in tombstone
                if field not in {"retired_at", "payload_sha256"}
            )
            or existing.get("payload_sha256")
            != hashlib.sha256(_canonical_json_bytes(existing_unsigned)).hexdigest()
        ):
            raise PolicyError("incremental Slurm identity tombstone binding drifted")
        tombstone = existing
    _incremental_transaction(
        root,
        profile,
        identity,
        transaction_id=transaction_id,
        operation="retire",
        phase="committed",
    )
    return {
        "schema_version": 1,
        "kind": "loom.developer-environment.slurm-identity-result",
        "operation": "retire",
        "cluster": profile.cluster,
        "env_id": identity["env_id"],
        "resource_generation": identity["resource_generation"],
        "service_user": identity["service_user"],
        "slurm_account": identity["slurm_account"],
        "slurm_qos": identity["slurm_qos"],
        "status": "retired",
        "jobs": [],
        "state_sha256": tombstone["state_sha256"],
        "mutations": mutations,
        "tombstone": str(tombstone_path),
        "completed_at": tombstone["retired_at"],
    }


def _capacity_request(
    deployment_id: str,
    *,
    root: Path,
    require_root_ownership: bool,
    suffix: str = "",
    kind: str = "loom.developer-environment.capacity-request",
) -> tuple[dict[str, Any], bytes]:
    if _REGISTRY.DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None:
        raise PolicyError("capacity deployment identity is invalid")
    path = root / "requests" / f"{deployment_id}{suffix}.json"
    raw, _metadata = _read_bound_regular_file(
        path,
        expected_uid=0 if require_root_ownership else os.geteuid(),
        expected_gid=0 if require_root_ownership else os.getegid(),
        expected_mode=0o600,
        description="capacity reconciliation request",
        max_bytes=1 << 20,
    )
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("capacity reconciliation request is invalid") from exc
    fields = {
        "schema_version",
        "kind",
        "env_id",
        "principal_id",
        "deployment_id",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
        "resource_generation",
        "registry_generation",
        "registry_snapshot_sha256",
        "slurm_user",
        "service_group",
        "slurm_account",
        "slurm_qos",
        "uid",
        "gid",
        "identity_preflight_nodes",
        "payload_sha256",
    }
    unsigned = (
        {key: value for key, value in request.items() if key != "payload_sha256"}
        if isinstance(request, dict)
        else {}
    )
    if (
        not isinstance(request, dict)
        or set(request) != fields
        or request.get("schema_version") != 1
        or request.get("kind") != kind
        or request.get("deployment_id") != deployment_id
        or _REGISTRY.ENV_ID_RE.fullmatch(str(request.get("env_id"))) is None
        or _REGISTRY.PRINCIPAL_RE.fullmatch(str(request.get("principal_id"))) is None
        or _REGISTRY.CANDIDATE_ID_RE.fullmatch(str(request.get("candidate_id"))) is None
        or _CANDIDATE_RE.fullmatch(str(request.get("candidate_sha"))) is None
        or _CANDIDATE_RE.fullmatch(str(request.get("candidate_tree"))) is None
        or type(request.get("resource_generation")) is not int
        or int(request["resource_generation"]) < 1
        or type(request.get("registry_generation")) is not int
        or int(request["registry_generation"]) < 1
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(request.get("registry_snapshot_sha256")),
        )
        is None
        or _REGISTRY.SAFE_NAME_RE.fullmatch(str(request.get("slurm_user"))) is None
        or _REGISTRY.SAFE_NAME_RE.fullmatch(str(request.get("service_group"))) is None
        or _SAFE_NAME.fullmatch(str(request.get("slurm_account"))) is None
        or _REGISTRY.SAFE_NAME_RE.fullmatch(str(request.get("slurm_qos"))) is None
        or type(request.get("uid")) is not int
        or int(request["uid"]) < 1
        or request.get("gid") != request["uid"]
        or request.get("identity_preflight_nodes")
        != {
            domain: [str(_CAPACITY_DOMAINS[domain]["authority_node"])]
            for domain in ("oldlab", "gb10")
        }
        or request.get("payload_sha256")
        != hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()
        or raw != _canonical_json_bytes(request) + b"\n"
    ):
        raise PolicyError("capacity reconciliation request binding is invalid")
    return request, raw


def _capacity_registry_binding(
    request: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    committed: bool,
) -> dict[str, Any]:
    environments = [
        environment
        for environment in snapshot["environments"]
        if environment["env_id"] == request["env_id"]
        and environment["principal_id"] == request["principal_id"]
    ]
    deployments = [
        deployment
        for deployment in snapshot["deployments"]
        if deployment["deployment_id"] == request["deployment_id"]
        and deployment["env_id"] == request["env_id"]
        and deployment["candidate_id"] == request["candidate_id"]
        and deployment["principal_id"] == request["principal_id"]
        and (
            (
                deployment["expected_resource_generation"] + 1 == request["resource_generation"]
                and deployment["phase"] == "verified"
                and deployment.get("applied_resource_generation") == request["resource_generation"]
                and type(deployment.get("applied_registry_generation")) is int
                and 1 <= deployment["applied_registry_generation"] < request["registry_generation"]
                and _REGISTRY.DIGEST_RE.fullmatch(
                    str(deployment.get("applied_registry_payload_sha256")),
                )
                is not None
            )
            if committed
            else (
                deployment["phase"] not in {"committed", "failed"}
                and (
                    deployment["expected_resource_generation"] == request["resource_generation"]
                    or (
                        deployment["phase"] == "verified"
                        and deployment.get("applied_resource_generation")
                        == request["resource_generation"]
                    )
                )
            )
        )
    ]
    candidates = [
        candidate
        for candidate in snapshot["candidates"]
        if candidate["candidate_id"] == request["candidate_id"]
        and candidate["env_id"] == request["env_id"]
        and candidate["principal_id"] == request["principal_id"]
        and candidate["candidate_sha"] == request["candidate_sha"]
        and candidate["candidate_tree"] == request["candidate_tree"]
    ]
    if (
        snapshot.get("generation") != request["registry_generation"]
        or snapshot.get("payload_sha256") != request["registry_snapshot_sha256"]
        or len(environments) != 1
        or len(deployments) != 1
        or len(candidates) != 1
    ):
        raise PolicyError("capacity request is stale against the registry snapshot")
    environment = cast(dict[str, Any], environments[0])
    expected_environment_generation = (
        request["resource_generation"] - 1
        if committed
        else deployments[0]["expected_resource_generation"]
    )
    if (
        environment["state"] != "deploying"
        or environment["resource_generation"] != expected_environment_generation
        or environment["slurm_user"] != request["slurm_user"]
        or environment["service_user"] != request["slurm_user"]
        or environment["service_group"] != request["service_group"]
        or environment["slurm_account"] != request["slurm_account"]
        or environment["slurm_qos"] != request["slurm_qos"]
        or environment["uid"] != request["uid"]
        or environment["gid"] != request["gid"]
    ):
        raise PolicyError("capacity request resource binding drifted")
    return environment


def _capacity_node_converge(
    domain: str,
    node: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    program: Path,
) -> dict[str, Any]:
    route = _CAPACITY_DOMAINS[domain]
    infrastructure_nodes = route["infrastructure_nodes"]
    if node not in infrastructure_nodes:
        raise PolicyError("capacity node is outside the closed domain inventory")
    bindings = candidate_set["candidate_bindings"]
    account = str(request["slurm_account"])
    binding = bindings.get(account)
    if not isinstance(binding, Mapping):
        raise PolicyError("capacity domain candidate binding is unavailable")
    action = (
        "slurm-controller-converge" if node == route["authority_node"] else "slurm-node-converge"
    )
    candidate_set_bytes = _canonical_json_bytes(candidate_set) + b"\n"
    body: dict[str, Any] = {
        "schema_version": 1,
        "action": action,
        "node": node,
        "domain": domain,
        "sandbox": binding["sandbox"],
        "candidate_sha": binding["candidate_sha"],
        "candidate_tree": binding["candidate_tree"],
        "payload_kind": "slurm-candidate-set-json",
        "payload_sha256": hashlib.sha256(candidate_set_bytes).hexdigest(),
        "payload_base64": base64.b64encode(candidate_set_bytes).decode("ascii"),
        "prior_request_id": None,
    }
    envelope = {
        **body,
        "request_id": hashlib.sha256(
            _canonical_json_bytes(body) + b"\n",
        ).hexdigest(),
    }
    envelope_bytes = _canonical_json_bytes(envelope) + b"\n"
    try:
        completed = subprocess.run(
            (
                str(program),
                "invoke",
                "--node",
                node,
                "--verb",
                "transact",
            ),
            input=envelope_bytes,
            check=False,
            capture_output=True,
            timeout=1800,
            env={
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C.UTF-8",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError("capacity domain authority is unavailable") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > 2 * 1024 * 1024:
        raise PolicyError("capacity domain reconciliation failed safely")
    try:
        state = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("capacity domain authority response is invalid") from exc
    inner_receipt = state.get("inner_receipt") if isinstance(state, dict) else None
    expected_inner = re.fullmatch(
        rf"slurm-policy-v1:{re.escape(str(route['cluster']))}:"
        r"([0-9a-f]{64}):([0-9a-f]{64})",
        str(inner_receipt),
    )
    if (
        not isinstance(state, dict)
        or completed.stdout != _canonical_json_bytes(state) + b"\n"
        or set(state)
        != {
            "schema_version",
            "request_id",
            "action",
            "node",
            "domain",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "payload_sha256",
            "result_sha256",
            "inner_receipt",
            "completed_at",
            "status",
        }
        or state.get("schema_version") != 1
        or state.get("request_id") != envelope["request_id"]
        or state.get("action") != action
        or state.get("node") != node
        or state.get("domain") != domain
        or state.get("sandbox") != binding["sandbox"]
        or state.get("candidate_sha") != binding["candidate_sha"]
        or state.get("candidate_tree") != binding["candidate_tree"]
        or state.get("payload_sha256") != body["payload_sha256"]
        or _REGISTRY.DIGEST_RE.fullmatch(str(state.get("result_sha256"))) is None
        or expected_inner is None
        or not isinstance(state.get("completed_at"), str)
        or state.get("status") != "succeeded"
        or bindings.get(account, {}).get("candidate_id") != request["candidate_id"]
    ):
        raise PolicyError("capacity domain authority readback drifted")
    try:
        completed_at = datetime.fromisoformat(str(state["completed_at"]))
    except ValueError as exc:
        raise PolicyError("capacity domain authority timestamp is invalid") from exc
    if completed_at.tzinfo is None:
        raise PolicyError("capacity domain authority timestamp is invalid")
    return {
        "action": action,
        "request_id": state["request_id"],
        "result_sha256": state["result_sha256"],
        "authority_receipt_sha256": hashlib.sha256(
            _canonical_json_bytes(state),
        ).hexdigest(),
        "completed_at": completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }


def _capacity_identity_preflight(
    domain: str,
    node: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    program: Path,
) -> dict[str, Any]:
    route = _CAPACITY_DOMAINS[domain]
    if node not in route["infrastructure_nodes"]:
        raise PolicyError("capacity identity node is outside the closed inventory")
    binding = candidate_set["candidate_bindings"].get(str(request["slurm_account"]))
    if not isinstance(binding, Mapping):
        raise PolicyError("capacity identity candidate binding is unavailable")
    preflight = {
        "schema_version": 2,
        "kind": "loom.developer-environment.identity-preflight",
        "env_id": request["env_id"],
        "principal_id": request["principal_id"],
        "resource_generation": request["resource_generation"],
        "service_user": request["slurm_user"],
        "service_group": request["service_group"],
        "uid": request["uid"],
        "gid": request["gid"],
        "slurm_account": request["slurm_account"],
        "slurm_qos": request["slurm_qos"],
        "registry_generation": request["registry_generation"],
        "registry_payload_sha256": request["registry_snapshot_sha256"],
        "candidate_set_sha256": candidate_set["candidate_set_sha256"],
        "revive_journal_sha256": request.get("revive_journal_sha256"),
    }
    payload_bytes = _canonical_json_bytes(preflight) + b"\n"
    body: dict[str, Any] = {
        "schema_version": 1,
        "action": "slurm-identity-preflight",
        "node": node,
        "domain": domain,
        "sandbox": binding["sandbox"],
        "candidate_sha": binding["candidate_sha"],
        "candidate_tree": binding["candidate_tree"],
        "env_id": request["env_id"],
        "deployment_id": request["deployment_id"],
        "resource_generation": request["resource_generation"],
        "candidate_id": request["candidate_id"],
        "registry_generation": request["registry_generation"],
        "registry_payload_sha256": request["registry_snapshot_sha256"],
        "payload_kind": "developer-environment-identity-preflight-json",
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload_base64": base64.b64encode(payload_bytes).decode("ascii"),
        "prior_request_id": None,
    }
    envelope = {
        **body,
        "request_id": hashlib.sha256(
            _canonical_json_bytes(body) + b"\n",
        ).hexdigest(),
    }
    try:
        completed = subprocess.run(
            (
                str(program),
                "invoke",
                "--node",
                node,
                "--verb",
                "check",
            ),
            input=_canonical_json_bytes(envelope) + b"\n",
            check=False,
            capture_output=True,
            timeout=120,
            env={
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C.UTF-8",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError("capacity identity preflight is unavailable") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > 2 * 1024 * 1024:
        raise PolicyError("capacity identity preflight failed safely")
    try:
        response = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("capacity identity preflight response is invalid") from exc
    result = response.get("result") if isinstance(response, Mapping) else None
    status = result.get("status") if isinstance(result, Mapping) else None
    local_status = result.get("local_identity_status") if isinstance(result, Mapping) else None
    expected_name = request["slurm_user"] if local_status == "exact-existing" else None
    expected_group = request["service_group"] if local_status == "exact-existing" else None
    if (
        not isinstance(response, dict)
        or completed.stdout != _canonical_json_bytes(response) + b"\n"
        or set(response) != {"schema_version", "request_id", "status", "result"}
        or response.get("schema_version") != 1
        or response.get("request_id") != envelope["request_id"]
        or response.get("status") != "succeeded"
        or not isinstance(result, Mapping)
        or set(result)
        != {
            "schema_version",
            "kind",
            "node",
            "domain",
            "env_id",
            "service_user",
            "service_group",
            "uid",
            "gid",
            "status",
            "passwd_name",
            "group_name",
            "identity_inventory_sha256",
            "local_identity_status",
            "slurm_accounting_status",
            "slurm_accounting_receipt_sha256",
            "owned_jobs",
            "checked_at",
        }
        or result.get("schema_version") != 1
        or result.get("kind") != "loom.developer-environment.identity-preflight-result"
        or result.get("node") != node
        or result.get("domain") != domain
        or result.get("env_id") != request["env_id"]
        or result.get("service_user") != request["slurm_user"]
        or result.get("service_group") != request["service_group"]
        or result.get("uid") != request["uid"]
        or result.get("gid") != request["gid"]
        or local_status not in {"available", "exact-existing"}
        or result.get("slurm_accounting_status") not in {"available", "exact-existing"}
        or status
        != (
            "exact-existing"
            if local_status == result.get("slurm_accounting_status") == "exact-existing"
            else "available"
        )
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(result.get("slurm_accounting_receipt_sha256")),
        )
        is None
        or not isinstance(result.get("owned_jobs"), list)
        or status not in {"available", "exact-existing"}
        or result.get("passwd_name") != expected_name
        or result.get("group_name") != expected_group
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(result.get("identity_inventory_sha256")),
        )
        is None
        or not isinstance(result.get("checked_at"), str)
    ):
        raise PolicyError("capacity identity preflight readback drifted")
    try:
        checked_at = datetime.fromisoformat(str(result["checked_at"]))
    except ValueError as exc:
        raise PolicyError("capacity identity preflight timestamp is invalid") from exc
    if checked_at.tzinfo is None:
        raise PolicyError("capacity identity preflight timestamp is invalid")
    return {
        "status": status,
        "receipt_sha256": hashlib.sha256(
            _canonical_json_bytes(response),
        ).hexdigest(),
    }


def _capacity_identity_converge(
    domain: str,
    node: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    program: Path,
    action: str = "slurm-identity-converge",
) -> dict[str, Any]:
    if action not in {"slurm-identity-converge", "slurm-identity-retire"}:
        raise PolicyError("capacity identity action is invalid")
    route = _CAPACITY_DOMAINS[domain]
    if node not in route["infrastructure_nodes"]:
        raise PolicyError("capacity identity node is outside the closed inventory")
    binding = candidate_set["candidate_bindings"].get(str(request["slurm_account"]))
    if not isinstance(binding, Mapping):
        raise PolicyError("capacity identity candidate binding is unavailable")
    identity = {
        "schema_version": 2,
        "kind": "loom.developer-environment.identity-preflight",
        "env_id": request["env_id"],
        "principal_id": request["principal_id"],
        "resource_generation": request["resource_generation"],
        "service_user": request["slurm_user"],
        "service_group": request["service_group"],
        "uid": request["uid"],
        "gid": request["gid"],
        "slurm_account": request["slurm_account"],
        "slurm_qos": request["slurm_qos"],
        "registry_generation": request["registry_generation"],
        "registry_payload_sha256": request["registry_snapshot_sha256"],
        "candidate_set_sha256": candidate_set["candidate_set_sha256"],
        "revive_journal_sha256": request.get("revive_journal_sha256"),
    }
    payload_bytes = _canonical_json_bytes(identity) + b"\n"
    body: dict[str, Any] = {
        "schema_version": 1,
        "action": action,
        "node": node,
        "domain": domain,
        "sandbox": binding["sandbox"],
        "candidate_sha": binding["candidate_sha"],
        "candidate_tree": binding["candidate_tree"],
        "env_id": request["env_id"],
        "deployment_id": request["deployment_id"],
        "resource_generation": request["resource_generation"],
        "candidate_id": request["candidate_id"],
        "registry_generation": request["registry_generation"],
        "registry_payload_sha256": request["registry_snapshot_sha256"],
        "payload_kind": "developer-environment-identity-preflight-json",
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload_base64": base64.b64encode(payload_bytes).decode("ascii"),
        "prior_request_id": None,
    }
    envelope = {
        **body,
        "request_id": hashlib.sha256(
            _canonical_json_bytes(body) + b"\n",
        ).hexdigest(),
    }
    try:
        completed = subprocess.run(
            (
                str(program),
                "invoke",
                "--node",
                node,
                "--verb",
                "transact",
            ),
            input=_canonical_json_bytes(envelope) + b"\n",
            check=False,
            capture_output=True,
            timeout=120,
            env={
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C.UTF-8",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError("capacity identity convergence is unavailable") from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > 2 * 1024 * 1024:
        raise PolicyError("capacity identity convergence failed safely")
    try:
        receipt = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("capacity identity convergence response is invalid") from exc
    if (
        not isinstance(receipt, dict)
        or completed.stdout != _canonical_json_bytes(receipt) + b"\n"
        or set(receipt)
        != {
            "schema_version",
            "request_id",
            "action",
            "node",
            "domain",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "env_id",
            "deployment_id",
            "resource_generation",
            "candidate_id",
            "registry_generation",
            "registry_payload_sha256",
            "payload_sha256",
            "result_sha256",
            "inner_receipt",
            "completed_at",
            "status",
        }
        or receipt.get("schema_version") != 1
        or receipt.get("request_id") != envelope["request_id"]
        or receipt.get("action") != action
        or receipt.get("node") != node
        or receipt.get("domain") != domain
        or receipt.get("sandbox") != binding["sandbox"]
        or receipt.get("candidate_sha") != binding["candidate_sha"]
        or receipt.get("candidate_tree") != binding["candidate_tree"]
        or receipt.get("env_id") != request["env_id"]
        or receipt.get("deployment_id") != request["deployment_id"]
        or receipt.get("resource_generation") != request["resource_generation"]
        or receipt.get("candidate_id") != request["candidate_id"]
        or receipt.get("registry_generation") != request["registry_generation"]
        or receipt.get("registry_payload_sha256") != request["registry_snapshot_sha256"]
        or receipt.get("payload_sha256") != body["payload_sha256"]
        or _REGISTRY.DIGEST_RE.fullmatch(str(receipt.get("result_sha256"))) is None
        or (
            receipt.get("inner_receipt")
            != (
                f"/var/lib/loom-developer-sandbox-slurm-policy/identity-tombstones/"
                f"{route['cluster']}/{request['env_id']}/"
                f"{request['resource_generation']}.json"
            )
            if action == "slurm-identity-retire"
            else receipt.get("inner_receipt") is not None
        )
        or not isinstance(receipt.get("completed_at"), str)
        or receipt.get("status") != "succeeded"
    ):
        raise PolicyError("capacity identity convergence readback drifted")
    try:
        completed_at = datetime.fromisoformat(str(receipt["completed_at"]))
    except ValueError as exc:
        raise PolicyError("capacity identity convergence timestamp is invalid") from exc
    if completed_at.tzinfo is None:
        raise PolicyError("capacity identity convergence timestamp is invalid")
    result = {
        "request_id": receipt["request_id"],
        "result_sha256": receipt["result_sha256"],
        "authority_receipt_sha256": hashlib.sha256(
            _canonical_json_bytes(receipt),
        ).hexdigest(),
        "completed_at": completed_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
    }
    if action == "slurm-identity-retire":
        result["action"] = action
        result["tombstone"] = receipt["inner_receipt"]
    return result


def _capacity_domain_converge(
    domain: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    identity_preflight: Mapping[str, Mapping[str, Any]],
    identity_convergence: Mapping[str, Mapping[str, Any]],
    *,
    program: Path,
) -> dict[str, Any]:
    route = _CAPACITY_DOMAINS[domain]
    account = str(request["slurm_account"])
    controller = str(route["authority_node"])
    if not route["infrastructure_nodes"] or route["infrastructure_nodes"][0] != controller:
        raise PolicyError("capacity domain inventory is invalid")
    if (
        set(identity_preflight) != {controller}
        or set(identity_convergence) != {controller}
        or any(proof.get("status") != "exact-existing" for proof in identity_convergence.values())
    ):
        raise PolicyError("capacity domain identity convergence is incomplete")
    convergence = identity_convergence[controller]
    node_receipts = {
        controller: {
            "action": "slurm-identity-converge",
            "request_id": convergence["request_id"],
            "result_sha256": convergence["result_sha256"],
            "authority_receipt_sha256": convergence["authority_receipt_sha256"],
            "completed_at": convergence["completed_at"],
        },
    }
    completed_at = max(str(receipt["completed_at"]) for receipt in node_receipts.values())
    receipts_sha256 = hashlib.sha256(
        _canonical_json_bytes(
            {
                "identity_convergence": identity_convergence,
                "slurm_convergence": node_receipts,
            },
        ),
    ).hexdigest()
    return {
        "status": "ready",
        "cluster": route["cluster"],
        "controller": route["controller"],
        "submit_host": route["submit_host"],
        "env_id": request["env_id"],
        "slurm_user": request["slurm_user"],
        "service_group": request["service_group"],
        "uid": request["uid"],
        "gid": request["gid"],
        "slurm_account": account,
        "slurm_qos": request["slurm_qos"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "registry_snapshot_sha256": request["registry_snapshot_sha256"],
        "policy_generation": candidate_set["generation"],
        "policy_sha256": hashlib.sha256(
            _canonical_json_bytes(
                {controller: node_receipts[controller]["result_sha256"]},
            ),
        ).hexdigest(),
        "authority_receipt_sha256": receipts_sha256,
        "slurm_convergence": node_receipts,
        "slurm_convergence_sha256": hashlib.sha256(
            _canonical_json_bytes(node_receipts),
        ).hexdigest(),
        "completed_at": completed_at,
        "identity_preflight": identity_preflight,
        "identity_preflight_sha256": hashlib.sha256(
            _canonical_json_bytes(identity_preflight),
        ).hexdigest(),
        "identity_convergence": identity_convergence,
        "identity_convergence_sha256": hashlib.sha256(
            _canonical_json_bytes(identity_convergence),
        ).hexdigest(),
    }


def _capacity_domain_preflight(
    domain: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    program: Path,
) -> dict[str, dict[str, Any]]:
    return {
        str(_CAPACITY_DOMAINS[domain]["authority_node"]): _capacity_identity_preflight(
            domain,
            str(_CAPACITY_DOMAINS[domain]["authority_node"]),
            request,
            candidate_set,
            program=program,
        )
    }


def _capacity_domain_identity_converge(
    domain: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    program: Path,
) -> dict[str, dict[str, Any]]:
    return {
        str(_CAPACITY_DOMAINS[domain]["authority_node"]): _capacity_identity_converge(
            domain,
            str(_CAPACITY_DOMAINS[domain]["authority_node"]),
            request,
            candidate_set,
            program=program,
        )
    }


def _capacity_domain_identity_retire(
    domain: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    *,
    program: Path,
) -> dict[str, dict[str, Any]]:
    controller = str(_CAPACITY_DOMAINS[domain]["authority_node"])
    return {
        controller: _capacity_identity_converge(
            domain,
            controller,
            request,
            candidate_set,
            program=program,
            action="slurm-identity-retire",
        ),
    }


def _capacity_domain_identity_readback(
    domain: str,
    request: Mapping[str, Any],
    candidate_set: Mapping[str, Any],
    transactions: Mapping[str, Mapping[str, Any]],
    *,
    program: Path,
) -> dict[str, dict[str, Any]]:
    controller = str(_CAPACITY_DOMAINS[domain]["authority_node"])
    if set(transactions) != {controller}:
        raise PolicyError("capacity identity transaction set is incomplete")
    readback: dict[str, dict[str, Any]] = {}
    proof = _capacity_identity_preflight(
        domain,
        controller,
        request,
        candidate_set,
        program=program,
    )
    if proof["status"] != "exact-existing":
        raise PolicyError("capacity identity convergence did not read back exactly")
    readback[controller] = {
        **transactions[controller],
        "status": "exact-existing",
        "readback_receipt_sha256": proof["receipt_sha256"],
    }
    return readback


def _capacity_revive_journal_sha256(
    request: Mapping[str, Any],
    *,
    revive_root: Path = _REVIVE_ROOT,
) -> str:
    path = revive_root / f"{request['env_id']}.json"
    journal = _load_journal(path)
    fields = {
        "schema_version",
        "kind",
        "phase",
        "env_id",
        "principal_id",
        "runtime_id",
        "uid",
        "gid",
        "service_user",
        "service_group",
        "slurm_user",
        "slurm_account",
        "slurm_qos",
        "previous_resource_generation",
        "new_resource_generation",
        "registry_generation",
        "registry_payload_sha256",
        "retire_tombstone_sha256",
        "idempotency_key",
        "created_at",
        "updated_at",
        "payload_sha256",
    }
    unsigned = (
        {field: value for field, value in journal.items() if field != "payload_sha256"}
        if isinstance(journal, dict)
        else {}
    )
    if (
        not isinstance(journal, dict)
        or set(journal) != fields
        or journal.get("schema_version") != 1
        or journal.get("kind") != "loom.developer-environment.revive-journal"
        or journal.get("phase") not in {"registered", "capacity-restored"}
        or journal.get("env_id") != request["env_id"]
        or journal.get("principal_id") != request["principal_id"]
        or journal.get("uid") != request["uid"]
        or journal.get("gid") != request["gid"]
        or journal.get("service_user") != request["slurm_user"]
        or journal.get("service_group") != request["service_group"]
        or journal.get("slurm_user") != request["slurm_user"]
        or journal.get("slurm_account") != request["slurm_account"]
        or journal.get("slurm_qos") != request["slurm_qos"]
        or type(journal.get("previous_resource_generation")) is not int
        or type(journal.get("new_resource_generation")) is not int
        or journal.get("new_resource_generation") != request["resource_generation"]
        or journal.get("previous_resource_generation") + 1 != journal.get("new_resource_generation")
        or journal.get("registry_generation") != request["registry_generation"]
        or journal.get("registry_payload_sha256") != request["registry_snapshot_sha256"]
        or _REGISTRY.RUNTIME_ID_RE.fullmatch(str(journal.get("runtime_id"))) is None
        or _REGISTRY.IDEMPOTENCY_RE.fullmatch(
            str(journal.get("idempotency_key")),
        )
        is None
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(journal.get("retire_tombstone_sha256")),
        )
        is None
        or journal.get("payload_sha256")
        != hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()
    ):
        raise PolicyError("capacity revive journal binding is invalid")
    _parse_allocation_timestamp(
        str(journal["created_at"]),
        "capacity revive journal",
    )
    _parse_allocation_timestamp(
        str(journal["updated_at"]),
        "capacity revive journal",
    )
    return str(journal["payload_sha256"])


def reconcile_capacity(
    deployment_id: str,
    *,
    root: Path = _CAPACITY_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
    program: Path = _CAPACITY_TRANSPORT_PROGRAM,
    require_root_ownership: bool = True,
    committed: bool = False,
    revive_journal_sha256: str | None = None,
) -> dict[str, Any]:
    """Converge both independent Slurm domains and publish one durable receipt."""

    if require_root_ownership and os.geteuid() != 0:
        raise PolicyError("capacity reconciliation requires root")
    request, _raw = _capacity_request(
        deployment_id,
        root=root,
        require_root_ownership=require_root_ownership,
    )
    if revive_journal_sha256 is not None:
        if _REGISTRY.DIGEST_RE.fullmatch(revive_journal_sha256) is None:
            raise PolicyError("capacity revive journal identity is invalid")
        request = {**request, "revive_journal_sha256": revive_journal_sha256}
    snapshot = _read_registry_snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    _capacity_registry_binding(request, snapshot, committed=committed)
    candidate_set = slurm_candidate_set_from_snapshot(
        snapshot,
        deployment_id=deployment_id,
        target_resource_generation=int(request["resource_generation"]),
    )
    account = str(request["slurm_account"])
    binding = candidate_set["candidate_bindings"].get(account)
    if (
        not isinstance(binding, Mapping)
        or binding.get("env_id") != request["env_id"]
        or binding.get("resource_generation") != request["resource_generation"]
        or binding.get("service_user") != request["slurm_user"]
        or binding.get("slurm_qos") != request["slurm_qos"]
        or binding.get("candidate_id") != request["candidate_id"]
        or binding.get("candidate_sha") != request["candidate_sha"]
        or binding.get("candidate_tree") != request["candidate_tree"]
    ):
        raise PolicyError("capacity candidate set does not contain the deployment")
    preflight = {
        domain: _capacity_domain_preflight(
            domain,
            request,
            candidate_set,
            program=program,
        )
        for domain in ("oldlab", "gb10")
    }
    identity_transactions = {
        domain: _capacity_domain_identity_converge(
            domain,
            request,
            candidate_set,
            program=program,
        )
        for domain in ("oldlab", "gb10")
    }
    identity_convergence = {
        domain: _capacity_domain_identity_readback(
            domain,
            request,
            candidate_set,
            identity_transactions[domain],
            program=program,
        )
        for domain in ("oldlab", "gb10")
    }
    domains = {
        domain: _capacity_domain_converge(
            domain,
            request,
            candidate_set,
            preflight[domain],
            identity_convergence[domain],
            program=program,
        )
        for domain in ("oldlab", "gb10")
    }
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.capacity-receipt",
        "status": (
            "revive-prepared"
            if revive_journal_sha256 is not None
            else "acceptance-prepared"
            if committed
            else "prepared"
        ),
        "request_sha256": request["payload_sha256"],
        **{
            field: request[field]
            for field in (
                "env_id",
                "principal_id",
                "deployment_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
                "slurm_user",
                "service_group",
                "slurm_account",
                "slurm_qos",
                "uid",
                "gid",
            )
        },
        "domains": domains,
    }
    receipt = {
        **unsigned,
        "payload_sha256": hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest(),
    }
    receipt_root = root / "receipts"
    _prepare_private_directory(
        receipt_root,
        enforce_root_ownership=require_root_ownership,
        create=True,
    )
    receipt_path = receipt_root / f"{deployment_id}.json"
    existing = _load_journal(receipt_path)
    if existing is not None:
        if existing == receipt:
            return existing
        if not committed:
            raise PolicyError("capacity receipt replay drifted")
        precommit = receipt_root / f"{deployment_id}-precommit.json"
        archived = _load_journal(precommit)
        if archived is None:
            _atomic_write(
                precommit,
                (_canonical_json_bytes(existing) + b"\n").decode("ascii"),
                mode=0o600,
            )
        elif archived != existing:
            raise PolicyError("capacity precommit receipt archive drifted")
    _atomic_write(
        receipt_path,
        (_canonical_json_bytes(receipt) + b"\n").decode("ascii"),
        mode=0o600,
    )
    rebound = _load_journal(receipt_path)
    if rebound != receipt:
        raise PolicyError("capacity receipt publication drifted")
    return receipt


def abort_capacity(
    deployment_id: str,
    *,
    root: Path = _CAPACITY_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
    program: Path = _CAPACITY_TRANSPORT_PROGRAM,
    require_root_ownership: bool = True,
) -> dict[str, Any]:
    """Retire a first-create identity while its deployment is still in flight."""

    if require_root_ownership and os.geteuid() != 0:
        raise PolicyError("capacity abort requires root")
    request, _raw = _capacity_request(
        deployment_id,
        root=root,
        require_root_ownership=require_root_ownership,
    )
    snapshot = _read_registry_snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    _capacity_registry_binding(request, snapshot, committed=False)
    candidate_set = slurm_candidate_set_from_snapshot(
        snapshot,
        include_provisioning=True,
        deployment_id=deployment_id,
        target_resource_generation=int(request["resource_generation"]),
        include_retiring=True,
    )
    binding = candidate_set["candidate_bindings"].get(str(request["slurm_account"]))
    if (
        not isinstance(binding, Mapping)
        or binding.get("env_id") != request["env_id"]
        or binding.get("resource_generation") != request["resource_generation"]
        or binding.get("service_user") != request["slurm_user"]
        or binding.get("slurm_qos") != request["slurm_qos"]
        or binding.get("candidate_id") != request["candidate_id"]
        or binding.get("candidate_sha") != request["candidate_sha"]
        or binding.get("candidate_tree") != request["candidate_tree"]
    ):
        raise PolicyError("capacity abort candidate set does not contain the deployment")
    domains = {
        domain: _capacity_domain_identity_retire(
            domain,
            request,
            candidate_set,
            program=program,
        )
        for domain in ("oldlab", "gb10")
    }
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.capacity-abort-receipt",
        "status": "retired",
        "request_sha256": request["payload_sha256"],
        **{
            field: request[field]
            for field in (
                "env_id",
                "principal_id",
                "deployment_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
                "slurm_user",
                "service_group",
                "slurm_account",
                "slurm_qos",
                "uid",
                "gid",
            )
        },
        "domains": domains,
    }
    receipt = {
        **unsigned,
        "payload_sha256": hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest(),
    }
    receipt_root = root / "receipts"
    _prepare_private_directory(
        receipt_root,
        enforce_root_ownership=require_root_ownership,
        create=True,
    )
    receipt_path = receipt_root / f"{deployment_id}-abort.json"
    existing = _load_journal(receipt_path)
    if existing is None:
        _atomic_write(
            receipt_path,
            (_canonical_json_bytes(receipt) + b"\n").decode("ascii"),
            mode=0o600,
        )
    elif existing != receipt:
        raise PolicyError("capacity abort receipt replay drifted")
    return receipt


def retire_capacity(
    deployment_id: str,
    *,
    root: Path = _CAPACITY_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
    program: Path = _CAPACITY_TRANSPORT_PROGRAM,
    require_root_ownership: bool = True,
) -> dict[str, Any]:
    """Retire one quarantined committed identity without touching peer accounts."""

    if require_root_ownership and os.geteuid() != 0:
        raise PolicyError("capacity retirement requires root")
    request, _raw = _capacity_request(
        deployment_id,
        root=root,
        require_root_ownership=require_root_ownership,
        suffix="-retire-request",
        kind="loom.developer-environment.capacity-retire-request",
    )
    snapshot = _read_registry_snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    environments = [
        environment
        for environment in snapshot["environments"]
        if environment["env_id"] == request["env_id"]
        and environment["principal_id"] == request["principal_id"]
        and environment["state"] == "quarantined"
        and environment["current_candidate_id"] == request["candidate_id"]
        and environment["resource_generation"] == request["resource_generation"]
        and environment["slurm_user"] == request["slurm_user"]
        and environment["service_user"] == request["slurm_user"]
        and environment["service_group"] == request["service_group"]
        and environment["slurm_account"] == request["slurm_account"]
        and environment["slurm_qos"] == request["slurm_qos"]
        and environment["uid"] == request["uid"]
        and environment["gid"] == request["gid"]
    ]
    deployments = [
        deployment
        for deployment in snapshot["deployments"]
        if deployment["deployment_id"] == deployment_id
        and deployment["env_id"] == request["env_id"]
        and deployment["principal_id"] == request["principal_id"]
        and deployment["candidate_id"] == request["candidate_id"]
        and deployment["phase"] == "committed"
        and deployment.get("applied_resource_generation") == request["resource_generation"]
        and deployment.get("expected_resource_generation", 0) + 1
        == deployment["applied_resource_generation"]
    ]
    candidates = [
        candidate
        for candidate in snapshot["candidates"]
        if candidate["candidate_id"] == request["candidate_id"]
        and candidate["env_id"] == request["env_id"]
        and candidate["principal_id"] == request["principal_id"]
        and candidate["candidate_sha"] == request["candidate_sha"]
        and candidate["candidate_tree"] == request["candidate_tree"]
    ]
    if (
        snapshot.get("generation") != request["registry_generation"]
        or snapshot.get("payload_sha256") != request["registry_snapshot_sha256"]
        or len(environments) != 1
        or len(deployments) != 1
        or len(candidates) != 1
    ):
        raise PolicyError("capacity retirement registry binding is invalid")
    candidate_set = slurm_candidate_set_from_snapshot(
        snapshot,
        deployment_id=deployment_id,
        target_resource_generation=int(request["resource_generation"]),
        include_retiring=True,
    )
    binding = candidate_set["candidate_bindings"].get(str(request["slurm_account"]))
    if (
        not isinstance(binding, Mapping)
        or binding.get("env_id") != request["env_id"]
        or binding.get("resource_generation") != request["resource_generation"]
        or binding.get("candidate_id") != request["candidate_id"]
        or binding.get("candidate_sha") != request["candidate_sha"]
        or binding.get("candidate_tree") != request["candidate_tree"]
    ):
        raise PolicyError("capacity retirement candidate set binding is invalid")
    domains = {
        domain: _capacity_domain_identity_retire(
            domain,
            request,
            candidate_set,
            program=program,
        )
        for domain in ("oldlab", "gb10")
    }
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.capacity-retire-receipt",
        "status": "retired",
        "request_sha256": request["payload_sha256"],
        **{
            field: request[field]
            for field in (
                "env_id",
                "principal_id",
                "deployment_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
                "slurm_user",
                "service_group",
                "slurm_account",
                "slurm_qos",
                "uid",
                "gid",
            )
        },
        "domains": domains,
    }
    receipt = {
        **unsigned,
        "payload_sha256": hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest(),
    }
    receipt_root = root / "receipts"
    _prepare_private_directory(
        receipt_root,
        enforce_root_ownership=require_root_ownership,
        create=True,
    )
    receipt_path = receipt_root / f"{deployment_id}-retire.json"
    existing = _load_journal(receipt_path)
    if existing is None:
        _atomic_write(
            receipt_path,
            (_canonical_json_bytes(receipt) + b"\n").decode("ascii"),
            mode=0o600,
        )
    elif existing != receipt:
        raise PolicyError("capacity retirement receipt replay drifted")
    return receipt


def finalize_capacity(
    deployment_id: str,
    *,
    root: Path = _CAPACITY_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
    program: Path = _CAPACITY_TRANSPORT_PROGRAM,
    require_root_ownership: bool = True,
) -> dict[str, Any]:
    """Publish an active+committed final-generation receipt without fleet restart."""

    return reconcile_capacity(
        deployment_id,
        root=root,
        registry_snapshot=registry_snapshot,
        program=program,
        require_root_ownership=require_root_ownership,
        committed=True,
    )


def reactivate_capacity(
    deployment_id: str,
    *,
    root: Path = _CAPACITY_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
    program: Path = _CAPACITY_TRANSPORT_PROGRAM,
    require_root_ownership: bool = True,
    revive_root: Path = _REVIVE_ROOT,
) -> dict[str, Any]:
    """Restore only the exact same retired identity under an independent root journal."""

    if require_root_ownership and os.geteuid() != 0:
        raise PolicyError("capacity reactivation requires root")
    snapshot = _read_registry_snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    deployments = [
        deployment
        for deployment in snapshot["deployments"]
        if deployment["deployment_id"] == deployment_id
        and deployment["phase"] not in {"committed", "failed"}
    ]
    if len(deployments) != 1:
        raise PolicyError("capacity reactivation deployment binding is invalid")
    deployment = deployments[0]
    environments = [
        environment
        for environment in snapshot["environments"]
        if environment["env_id"] == deployment["env_id"]
        and environment["principal_id"] == deployment["principal_id"]
        and environment["state"] == "deploying"
        and environment["resource_generation"] == deployment["expected_resource_generation"]
    ]
    candidates = [
        candidate
        for candidate in snapshot["candidates"]
        if candidate["candidate_id"] == deployment["candidate_id"]
        and candidate["env_id"] == deployment["env_id"]
        and candidate["principal_id"] == deployment["principal_id"]
    ]
    if len(environments) != 1 or len(candidates) != 1:
        raise PolicyError("capacity reactivation registry binding is invalid")
    environment = environments[0]
    candidate = candidates[0]
    unsigned_request = {
        "schema_version": 1,
        "kind": "loom.developer-environment.capacity-request",
        "env_id": environment["env_id"],
        "principal_id": environment["principal_id"],
        "deployment_id": deployment_id,
        "candidate_id": candidate["candidate_id"],
        "candidate_sha": candidate["candidate_sha"],
        "candidate_tree": candidate["candidate_tree"],
        "resource_generation": environment["resource_generation"],
        "registry_generation": snapshot["generation"],
        "registry_snapshot_sha256": snapshot["payload_sha256"],
        "slurm_user": environment["slurm_user"],
        "service_group": environment["service_group"],
        "slurm_account": environment["slurm_account"],
        "slurm_qos": environment["slurm_qos"],
        "uid": environment["uid"],
        "gid": environment["gid"],
        "identity_preflight_nodes": {
            domain: [str(_CAPACITY_DOMAINS[domain]["authority_node"])]
            for domain in ("oldlab", "gb10")
        },
    }
    request = {
        **unsigned_request,
        "payload_sha256": hashlib.sha256(
            _canonical_json_bytes(unsigned_request),
        ).hexdigest(),
    }
    revive_journal_sha256 = _capacity_revive_journal_sha256(
        request,
        revive_root=revive_root,
    )
    request_root = root / "requests"
    _prepare_private_directory(
        request_root,
        enforce_root_ownership=require_root_ownership,
        create=True,
    )
    request_path = request_root / f"{deployment_id}.json"
    existing = _load_journal(request_path)
    if existing is None:
        _atomic_write(
            request_path,
            (_canonical_json_bytes(request) + b"\n").decode("ascii"),
            mode=0o600,
        )
    elif existing != request:
        raise PolicyError("capacity reactivation request replay drifted")
    rebound, _raw = _capacity_request(
        deployment_id,
        root=root,
        require_root_ownership=require_root_ownership,
    )
    if rebound != request:
        raise PolicyError("capacity reactivation request publication drifted")
    return reconcile_capacity(
        deployment_id,
        root=root,
        registry_snapshot=registry_snapshot,
        program=program,
        require_root_ownership=require_root_ownership,
        revive_journal_sha256=revive_journal_sha256,
    )


def rollback_capacity(
    deployment_id: str,
    *,
    root: Path = _CAPACITY_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
    program: Path = _CAPACITY_TRANSPORT_PROGRAM,
    require_root_ownership: bool = True,
) -> dict[str, Any]:
    """Rebind accounting to the current active candidate after a failed deployment."""

    if require_root_ownership and os.geteuid() != 0:
        raise PolicyError("capacity rollback requires root")
    failed, _raw = _capacity_request(
        deployment_id,
        root=root,
        require_root_ownership=require_root_ownership,
    )
    snapshot = _read_registry_snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    environments = [
        environment
        for environment in snapshot["environments"]
        if environment["env_id"] == failed["env_id"]
        and environment["principal_id"] == failed["principal_id"]
    ]
    failed_deployments = [
        deployment
        for deployment in snapshot["deployments"]
        if deployment["deployment_id"] == deployment_id
        and deployment["env_id"] == failed["env_id"]
        and deployment["candidate_id"] == failed["candidate_id"]
        and deployment["principal_id"] == failed["principal_id"]
        and deployment["phase"] == "failed"
    ]
    if len(environments) != 1 or len(failed_deployments) != 1:
        raise PolicyError("capacity rollback failed deployment binding is invalid")
    environment = environments[0]
    current_candidate_id = environment.get("current_candidate_id")
    if environment.get("state") != "active" or not isinstance(current_candidate_id, str):
        raise PolicyError("capacity rollback requires a preserved active environment")
    candidates = [
        candidate
        for candidate in snapshot["candidates"]
        if candidate["candidate_id"] == current_candidate_id
        and candidate["env_id"] == failed["env_id"]
        and candidate["principal_id"] == failed["principal_id"]
    ]
    committed = [
        deployment
        for deployment in snapshot["deployments"]
        if deployment["env_id"] == failed["env_id"]
        and deployment["candidate_id"] == current_candidate_id
        and deployment["principal_id"] == failed["principal_id"]
        and deployment["phase"] == "committed"
        and deployment.get("applied_resource_generation") == environment["resource_generation"]
        and deployment.get("expected_resource_generation", 0) + 1
        == deployment["applied_resource_generation"]
        and type(deployment.get("applied_registry_generation")) is int
        and 1 <= deployment["applied_registry_generation"] < snapshot["generation"]
        and _REGISTRY.DIGEST_RE.fullmatch(
            str(deployment.get("applied_registry_payload_sha256")),
        )
        is not None
    ]
    if len(candidates) != 1 or not committed:
        raise PolicyError("capacity rollback active candidate binding is invalid")
    current_candidate = candidates[0]
    restored_request = {
        **failed,
        "candidate_id": current_candidate["candidate_id"],
        "candidate_sha": current_candidate["candidate_sha"],
        "candidate_tree": current_candidate["candidate_tree"],
        "resource_generation": environment["resource_generation"],
        "registry_generation": snapshot["generation"],
        "registry_snapshot_sha256": snapshot["payload_sha256"],
        "slurm_user": environment["slurm_user"],
        "service_group": environment["service_group"],
        "slurm_account": environment["slurm_account"],
        "slurm_qos": environment["slurm_qos"],
        "uid": environment["uid"],
        "gid": environment["gid"],
        "identity_preflight_nodes": {
            domain: [str(_CAPACITY_DOMAINS[domain]["authority_node"])]
            for domain in ("oldlab", "gb10")
        },
    }
    restored_unsigned = {
        key: value for key, value in restored_request.items() if key != "payload_sha256"
    }
    restored_request["payload_sha256"] = hashlib.sha256(
        _canonical_json_bytes(restored_unsigned),
    ).hexdigest()
    candidate_set = slurm_candidate_set_from_snapshot(snapshot)
    binding = candidate_set["candidate_bindings"].get(str(restored_request["slurm_account"]))
    if (
        not isinstance(binding, Mapping)
        or binding.get("candidate_id") != restored_request["candidate_id"]
        or binding.get("candidate_sha") != restored_request["candidate_sha"]
        or binding.get("candidate_tree") != restored_request["candidate_tree"]
        or failed["candidate_id"] == restored_request["candidate_id"]
    ):
        raise PolicyError("capacity rollback current candidate set binding is invalid")
    domains: dict[str, Any] = {}
    for domain in ("oldlab", "gb10"):
        preflight = _capacity_domain_preflight(
            domain,
            restored_request,
            candidate_set,
            program=program,
        )
        transactions = _capacity_domain_identity_converge(
            domain,
            restored_request,
            candidate_set,
            program=program,
        )
        convergence = _capacity_domain_identity_readback(
            domain,
            restored_request,
            candidate_set,
            transactions,
            program=program,
        )
        domains[domain] = _capacity_domain_converge(
            domain,
            restored_request,
            candidate_set,
            preflight,
            convergence,
            program=program,
        )
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.capacity-rollback-receipt",
        "status": "ready",
        "deployment_id": deployment_id,
        "env_id": failed["env_id"],
        "failed_candidate_id": failed["candidate_id"],
        "failed_candidate_sha": failed["candidate_sha"],
        "failed_candidate_tree": failed["candidate_tree"],
        "restored_candidate_id": restored_request["candidate_id"],
        "restored_candidate_sha": restored_request["candidate_sha"],
        "restored_candidate_tree": restored_request["candidate_tree"],
        "resource_generation": restored_request["resource_generation"],
        "registry_generation": snapshot["generation"],
        "registry_payload_sha256": snapshot["payload_sha256"],
        "failed_candidate_projection_present": False,
        "association_preserved": True,
        "domains": domains,
    }
    receipt = {
        **unsigned,
        "payload_sha256": hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest(),
    }
    receipt_root = root / "receipts"
    _prepare_private_directory(
        receipt_root,
        enforce_root_ownership=require_root_ownership,
        create=True,
    )
    receipt_path = receipt_root / f"{deployment_id}-rollback.json"
    existing = _load_journal(receipt_path)
    if existing is None:
        _atomic_write(
            receipt_path,
            (_canonical_json_bytes(receipt) + b"\n").decode("ascii"),
            mode=0o600,
        )
    elif existing != receipt:
        raise PolicyError("capacity rollback receipt replay drifted")
    return receipt


def check_capacity(
    deployment_id: str,
    *,
    root: Path = _CAPACITY_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
    require_root_ownership: bool = True,
    finalized: bool = False,
) -> dict[str, Any]:
    """Validate one committed capacity receipt against the current registry."""

    if require_root_ownership and os.geteuid() != 0:
        raise PolicyError("capacity check requires root")
    request, _raw = _capacity_request(
        deployment_id,
        root=root,
        require_root_ownership=require_root_ownership,
    )
    snapshot = _read_registry_snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    if finalized:
        _capacity_registry_binding(request, snapshot, committed=True)
        environments: list[Mapping[str, Any]] = []
        deployments: list[Mapping[str, Any]] = []
        candidates: list[Mapping[str, Any]] = []
    else:
        environments = [
            environment
            for environment in snapshot["environments"]
            if environment["env_id"] == request["env_id"]
            and environment["principal_id"] == request["principal_id"]
        ]
        deployments = [
            deployment
            for deployment in snapshot["deployments"]
            if deployment["deployment_id"] == deployment_id
            and deployment["env_id"] == request["env_id"]
            and deployment["principal_id"] == request["principal_id"]
            and deployment["candidate_id"] == request["candidate_id"]
            and deployment["expected_resource_generation"] + 1 == request["resource_generation"]
            and deployment.get("applied_resource_generation") == request["resource_generation"]
            and type(deployment.get("applied_registry_generation")) is int
            and 1 <= deployment["applied_registry_generation"] < snapshot["generation"]
            and _REGISTRY.DIGEST_RE.fullmatch(
                str(deployment.get("applied_registry_payload_sha256")),
            )
            is not None
        ]
        candidates = [
            candidate
            for candidate in snapshot["candidates"]
            if candidate["candidate_id"] == request["candidate_id"]
            and candidate["env_id"] == request["env_id"]
            and candidate["principal_id"] == request["principal_id"]
            and candidate["candidate_sha"] == request["candidate_sha"]
            and candidate["candidate_tree"] == request["candidate_tree"]
        ]
    if not finalized and (
        len(environments) != 1
        or len(deployments) != 1
        or len(candidates) != 1
        or environments[0]["state"] != "active"
        or environments[0]["resource_generation"] != request["resource_generation"]
        or environments[0]["current_candidate_id"] != request["candidate_id"]
        or environments[0]["service_user"] != request["slurm_user"]
        or environments[0]["service_group"] != request["service_group"]
        or environments[0]["uid"] != request["uid"]
        or environments[0]["gid"] != request["gid"]
        or environments[0]["slurm_account"] != request["slurm_account"]
        or environments[0]["slurm_qos"] != request["slurm_qos"]
        or deployments[0]["phase"] != "committed"
        or snapshot["generation"] < request["registry_generation"]
    ):
        raise PolicyError("capacity receipt is stale against the current registry")
    candidate_set = slurm_candidate_set_from_snapshot(
        snapshot,
        deployment_id=deployment_id if finalized else None,
        target_resource_generation=(int(request["resource_generation"]) if finalized else None),
    )
    binding = candidate_set["candidate_bindings"].get(str(request["slurm_account"]))
    if (
        not isinstance(binding, Mapping)
        or binding.get("env_id") != request["env_id"]
        or binding.get("resource_generation") != request["resource_generation"]
        or binding.get("service_user") != request["slurm_user"]
        or binding.get("slurm_qos") != request["slurm_qos"]
        or binding.get("candidate_id") != request["candidate_id"]
        or binding.get("candidate_sha") != request["candidate_sha"]
        or binding.get("candidate_tree") != request["candidate_tree"]
    ):
        raise PolicyError("capacity receipt candidate is not current")
    receipt_path = root / "receipts" / f"{deployment_id}.json"
    receipt_raw, _metadata = _read_bound_regular_file(
        receipt_path,
        expected_uid=0 if require_root_ownership else os.geteuid(),
        expected_gid=0 if require_root_ownership else os.getegid(),
        expected_mode=0o600,
        description="capacity receipt",
        max_bytes=2 << 20,
    )
    try:
        receipt = json.loads(receipt_raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("capacity receipt is invalid") from exc
    receipt_fields = {
        "schema_version",
        "kind",
        "status",
        "request_sha256",
        "env_id",
        "principal_id",
        "deployment_id",
        "candidate_id",
        "candidate_sha",
        "candidate_tree",
        "resource_generation",
        "registry_generation",
        "registry_snapshot_sha256",
        "slurm_user",
        "service_group",
        "slurm_account",
        "slurm_qos",
        "uid",
        "gid",
        "domains",
        "payload_sha256",
    }
    unsigned = (
        {key: value for key, value in receipt.items() if key != "payload_sha256"}
        if isinstance(receipt, dict)
        else {}
    )
    exact_request = {
        field: request[field]
        for field in (
            "env_id",
            "principal_id",
            "deployment_id",
            "candidate_id",
            "candidate_sha",
            "candidate_tree",
            "resource_generation",
            "registry_generation",
            "registry_snapshot_sha256",
            "slurm_user",
            "service_group",
            "slurm_account",
            "slurm_qos",
            "uid",
            "gid",
        )
    }
    domains = receipt.get("domains") if isinstance(receipt, Mapping) else None
    if (
        not isinstance(receipt, dict)
        or set(receipt) != receipt_fields
        or receipt_raw != _canonical_json_bytes(receipt) + b"\n"
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.developer-environment.capacity-receipt"
        or receipt.get("status") != "acceptance-prepared"
        or receipt.get("request_sha256") != request["payload_sha256"]
        or receipt.get("payload_sha256")
        != hashlib.sha256(_canonical_json_bytes(unsigned)).hexdigest()
        or any(receipt.get(field) != value for field, value in exact_request.items())
        or not isinstance(domains, dict)
        or set(domains) != {"oldlab", "gb10"}
    ):
        raise PolicyError("capacity receipt binding is invalid")
    domain_fields = {
        "status",
        "cluster",
        "controller",
        "submit_host",
        "env_id",
        "slurm_user",
        "service_group",
        "uid",
        "gid",
        "slurm_account",
        "slurm_qos",
        "candidate_sha",
        "candidate_tree",
        "registry_snapshot_sha256",
        "policy_generation",
        "policy_sha256",
        "authority_receipt_sha256",
        "completed_at",
        "identity_preflight",
        "identity_preflight_sha256",
        "identity_convergence",
        "identity_convergence_sha256",
        "slurm_convergence",
        "slurm_convergence_sha256",
    }
    for domain, route in _CAPACITY_DOMAINS.items():
        value = domains[domain]
        nodes = {str(route["authority_node"])}
        preflight = value.get("identity_preflight") if isinstance(value, Mapping) else None
        convergence = value.get("identity_convergence") if isinstance(value, Mapping) else None
        slurm = value.get("slurm_convergence") if isinstance(value, Mapping) else None
        if (
            not isinstance(value, dict)
            or set(value) != domain_fields
            or value.get("status") != "ready"
            or value.get("cluster") != route["cluster"]
            or value.get("controller") != route["controller"]
            or value.get("submit_host") != route["submit_host"]
            or any(
                value.get(field) != request[field]
                for field in (
                    "env_id",
                    "slurm_user",
                    "service_group",
                    "uid",
                    "gid",
                    "slurm_account",
                    "slurm_qos",
                    "candidate_sha",
                    "candidate_tree",
                    "registry_snapshot_sha256",
                )
            )
            or value.get("policy_generation") != request["registry_generation"]
            or not isinstance(value.get("completed_at"), str)
            or not isinstance(preflight, dict)
            or set(preflight) != nodes
            or any(
                not isinstance(proof, dict)
                or set(proof) != {"status", "receipt_sha256"}
                or proof.get("status") not in {"available", "exact-existing"}
                or _REGISTRY.DIGEST_RE.fullmatch(str(proof.get("receipt_sha256"))) is None
                for proof in preflight.values()
            )
            or value.get("identity_preflight_sha256")
            != hashlib.sha256(_canonical_json_bytes(preflight)).hexdigest()
            or not isinstance(convergence, dict)
            or set(convergence) != nodes
            or any(
                not isinstance(proof, dict)
                or set(proof)
                != {
                    "request_id",
                    "result_sha256",
                    "authority_receipt_sha256",
                    "completed_at",
                    "status",
                    "readback_receipt_sha256",
                }
                or proof.get("status") != "exact-existing"
                or any(
                    _REGISTRY.DIGEST_RE.fullmatch(str(proof.get(field))) is None
                    for field in (
                        "request_id",
                        "result_sha256",
                        "authority_receipt_sha256",
                        "readback_receipt_sha256",
                    )
                )
                or not isinstance(proof.get("completed_at"), str)
                for proof in convergence.values()
            )
            or value.get("identity_convergence_sha256")
            != hashlib.sha256(_canonical_json_bytes(convergence)).hexdigest()
            or not isinstance(slurm, dict)
            or set(slurm) != nodes
            or any(
                not isinstance(proof, dict)
                or set(proof)
                != {
                    "action",
                    "request_id",
                    "result_sha256",
                    "authority_receipt_sha256",
                    "completed_at",
                }
                or proof.get("action") != "slurm-identity-converge"
                or any(
                    _REGISTRY.DIGEST_RE.fullmatch(str(proof.get(field))) is None
                    for field in (
                        "request_id",
                        "result_sha256",
                        "authority_receipt_sha256",
                    )
                )
                or not isinstance(proof.get("completed_at"), str)
                for node, proof in slurm.items()
            )
            or value.get("slurm_convergence_sha256")
            != hashlib.sha256(_canonical_json_bytes(slurm)).hexdigest()
            or value.get("policy_sha256")
            != hashlib.sha256(
                _canonical_json_bytes(
                    {node: proof["result_sha256"] for node, proof in slurm.items()},
                ),
            ).hexdigest()
            or value.get("authority_receipt_sha256")
            != hashlib.sha256(
                _canonical_json_bytes(
                    {
                        "identity_convergence": convergence,
                        "slurm_convergence": slurm,
                    },
                ),
            ).hexdigest()
        ):
            raise PolicyError("capacity domain receipt binding is invalid")
        _parse_allocation_timestamp(
            value["completed_at"],
            "capacity domain receipt",
        )
        for proof in convergence.values():
            _parse_allocation_timestamp(
                proof["completed_at"],
                "capacity identity receipt",
            )
        for proof in slurm.values():
            _parse_allocation_timestamp(
                proof["completed_at"],
                "capacity Slurm receipt",
            )
    unsigned_check = {
        "schema_version": 1,
        "kind": (
            "loom.developer-environment.capacity-finalize-check"
            if finalized
            else "loom.developer-environment.capacity-check"
        ),
        "status": "acceptance-prepared" if finalized else "activated",
        "deployment_id": deployment_id,
        "env_id": request["env_id"],
        "candidate_id": request["candidate_id"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "resource_generation": request["resource_generation"],
        "registry_generation": snapshot["generation"],
        "registry_payload_sha256": snapshot["payload_sha256"],
        "capacity_receipt_sha256": receipt["payload_sha256"],
        "identity_node_count": sum(
            len(domains[domain]["identity_convergence"]) for domain in ("oldlab", "gb10")
        ),
        "domains": ["oldlab", "gb10"],
        "checked_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    return {
        **unsigned_check,
        "payload_sha256": hashlib.sha256(
            _canonical_json_bytes(unsigned_check),
        ).hexdigest(),
    }


def _acceptance_probe_request(
    raw: bytes,
    profile: Profile,
    *,
    registry_snapshot: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("acceptance probe domain request is invalid") from exc
    unsigned = (
        {key: value for key, value in request.items() if key != "payload_sha256"}
        if isinstance(request, dict)
        else {}
    )
    domain = "oldlab" if profile.cluster == "trt-oldlab" else "gb10"
    snapshot = _read_registry_snapshot(
        registry_snapshot,
        require_root_ownership=registry_snapshot == REGISTRY_SNAPSHOT_PATH,
    )
    environments = (
        [
            row
            for row in snapshot["environments"]
            if row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
            and row["runtime_id"] == request.get("runtime_id")
        ]
        if isinstance(request, dict)
        else []
    )
    deployments = (
        [
            row
            for row in snapshot["deployments"]
            if row["deployment_id"] == request.get("deployment_id")
            and row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
            and row["candidate_id"] == request.get("candidate_id")
        ]
        if isinstance(request, dict)
        else []
    )
    candidates = (
        [
            row
            for row in snapshot["candidates"]
            if row["candidate_id"] == request.get("candidate_id")
            and row["env_id"] == request.get("env_id")
            and row["principal_id"] == request.get("principal_id")
            and row["candidate_sha"] == request.get("candidate_sha")
            and row["candidate_tree"] == request.get("candidate_tree")
        ]
        if isinstance(request, dict)
        else []
    )
    environment = environments[0] if len(environments) == 1 else {}
    deployment = deployments[0] if len(deployments) == 1 else {}
    if (
        not isinstance(request, dict)
        or set(request) != _ACCEPTANCE_PROBE_REQUEST_FIELDS
        or raw != _canonical_json_bytes(request) + b"\n"
        or request.get("schema_version") != 1
        or request.get("kind") != _ACCEPTANCE_PROBE_KIND
        or request.get("action") != _ACCEPTANCE_PROBE_ACTION
        or request.get("domain") != domain
        or request.get("cluster") != profile.cluster
        or request.get("submit_host") != profile.submit_host
        or request.get("controller") != profile.controller
        or _REGISTRY.DEPLOYMENT_ID_RE.fullmatch(
            str(request.get("deployment_id")),
        )
        is None
        or _REGISTRY.ENV_ID_RE.fullmatch(str(request.get("env_id"))) is None
        or _REGISTRY.PRINCIPAL_RE.fullmatch(str(request.get("principal_id"))) is None
        or _REGISTRY.RUNTIME_ID_RE.fullmatch(str(request.get("runtime_id"))) is None
        or _REGISTRY.CANDIDATE_ID_RE.fullmatch(
            str(request.get("candidate_id")),
        )
        is None
        or _CANDIDATE_RE.fullmatch(str(request.get("candidate_sha"))) is None
        or _CANDIDATE_RE.fullmatch(str(request.get("candidate_tree"))) is None
        or type(request.get("applied_resource_generation")) is not int
        or request["applied_resource_generation"] < 2
        or type(request.get("registry_generation")) is not int
        or request["registry_generation"] < 1
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(request.get("registry_snapshot_sha256")),
        )
        is None
        or _REGISTRY.SAFE_NAME_RE.fullmatch(
            str(request.get("service_user")),
        )
        is None
        or _SAFE_NAME.fullmatch(str(request.get("slurm_account"))) is None
        or _REGISTRY.SAFE_NAME_RE.fullmatch(str(request.get("slurm_qos"))) is None
        or re.fullmatch(
            r"loom-env-[a-z0-9][a-z0-9-]{0,62}-finalize-[0-9a-f]{12}",
            str(request.get("job_name")),
        )
        is None
        or request.get("time_limit_seconds") != 300
        or request.get("health_services") != list(_ACCEPTANCE_PROBE_SERVICES)
        or request.get("general_admission_authorized") is not False
        or request.get("foreign_job_action") != "observe-only"
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(request.get("idempotency_key")),
        )
        is None
        or request.get("payload_sha256")
        != hashlib.sha256(_canonical_json_bytes(unsigned) + b"\n").hexdigest()
        or snapshot.get("generation") != request.get("registry_generation")
        or snapshot.get("payload_sha256") != request.get("registry_snapshot_sha256")
        or len(environments) != 1
        or len(deployments) != 1
        or len(candidates) != 1
        or environment.get("state") != "deploying"
        or environment.get("resource_generation") != deployment.get("expected_resource_generation")
        or environment.get("slurm_user") != request.get("service_user")
        or environment.get("service_user") != request.get("service_user")
        or environment.get("slurm_account") != request.get("slurm_account")
        or environment.get("slurm_qos") != request.get("slurm_qos")
        or deployment.get("phase") != "verified"
        or deployment.get("applied_resource_generation")
        != deployment.get("expected_resource_generation", 0) + 1
        or deployment.get("applied_resource_generation")
        != request.get("applied_resource_generation")
        or type(deployment.get("applied_registry_generation")) is not int
        or deployment["applied_registry_generation"] < 1
        or _REGISTRY.DIGEST_RE.fullmatch(
            str(deployment.get("applied_registry_payload_sha256")),
        )
        is None
        or deployment.get("finalization_payload_sha256") is not None
    ):
        raise PolicyError("acceptance probe domain request binding is invalid")
    return request, environment


def _acceptance_probe_output(
    path: Path,
    *,
    uid: int,
    gid: int,
) -> tuple[dict[str, Any], bytes]:
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        raw = os.read(descriptor, (1 << 20) + 1)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except OSError as exc:
        raise PolicyError("acceptance probe job output is unavailable") from exc
    finally:
        if "descriptor" in locals():
            os.close(descriptor)
    if (
        len(raw) > (1 << 20)
        or not stat.S_ISREG(opened.st_mode)
        or stat.S_ISLNK(lexical.st_mode)
        or opened.st_nlink != 1
        or current.st_nlink != 1
        or (opened.st_uid, opened.st_gid) != (uid, gid)
        or stat.S_IMODE(opened.st_mode) & 0o022
        or (opened.st_dev, opened.st_ino, opened.st_size, opened.st_mtime_ns)
        != (rebound.st_dev, rebound.st_ino, rebound.st_size, rebound.st_mtime_ns)
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        raise PolicyError("acceptance probe job output is unsafe")
    try:
        output = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("acceptance probe job output is invalid") from exc
    if not isinstance(output, dict) or raw != _canonical_json_bytes(output) + b"\n":
        raise PolicyError("acceptance probe job output is invalid")
    return output, raw


def _acceptance_probe_accounting(
    job_id: str,
    profile: Profile,
) -> list[list[str]]:
    output = _run(
        (
            "sacct",
            "-nP",
            f"--clusters={profile.cluster}",
            "-j",
            job_id,
            "--format=JobIDRaw,JobName,State,NodeList,Account,User,Cluster,QOS,ExitCode",
        ),
        timeout=15,
    )
    rows = [line.split("|") for line in output.splitlines() if line.strip()]
    if any(len(row) != 9 for row in rows):
        raise PolicyError("acceptance probe accounting output is malformed")
    return rows


def _acceptance_probe_wrapped_script(
    *,
    profile: Profile,
    request_path: Path,
    result_path: Path,
) -> str:
    source_root = Path(__file__).resolve().parents[2]
    return " ".join(
        shlex.quote(item)
        for item in (
            "/usr/bin/python3",
            "-I",
            "-B",
            str(source_root / "scripts/ops/developer_sandbox_slurm_policy.py"),
            "acceptance-probe-job",
            "--profile",
            str(
                source_root
                / "deploy/slurm/developer-sandboxes"
                / ("oldlab.toml" if profile.cluster == "trt-oldlab" else "gb10.toml")
            ),
            "--probe-request",
            str(request_path),
            "--result-path",
            str(result_path),
            "--execute",
        )
    )


def _acceptance_probe_job_request(path: Path) -> dict[str, Any]:
    try:
        metadata = path.lstat()
        raw = path.read_bytes()
    except OSError as exc:
        raise PolicyError("allocation acceptance probe request is unavailable") from exc
    try:
        request = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PolicyError("allocation acceptance probe request is invalid") from exc
    unsigned = (
        {key: value for key, value in request.items() if key != "payload_sha256"}
        if isinstance(request, dict)
        else {}
    )
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) & 0o077
        or len(raw) > (1 << 20)
        or not isinstance(request, dict)
        or set(request) != _ACCEPTANCE_PROBE_REQUEST_FIELDS
        or raw != _canonical_json_bytes(request) + b"\n"
        or request.get("schema_version") != 1
        or request.get("kind") != _ACCEPTANCE_PROBE_KIND
        or request.get("action") != _ACCEPTANCE_PROBE_ACTION
        or request.get("health_services") != list(_ACCEPTANCE_PROBE_SERVICES)
        or request.get("general_admission_authorized") is not False
        or request.get("foreign_job_action") != "observe-only"
        or request.get("payload_sha256")
        != hashlib.sha256(_canonical_json_bytes(unsigned) + b"\n").hexdigest()
    ):
        raise PolicyError("allocation acceptance probe request is invalid")
    return request


def _acceptance_probe_compose_environment(
    request: Mapping[str, Any],
    *,
    job_id: str,
    cgroup_parent: str,
    worker_env: Path,
    result_path: Path,
) -> tuple[dict[str, str], str]:
    source_root = Path(__file__).resolve().parents[2]
    project = (
        f"loom-accept-{request['runtime_id']}-{request['idempotency_key'][:12]}-{request['domain']}"
    )
    try:
        identity = pwd.getpwnam(str(request["service_user"]))
    except KeyError as exc:
        raise PolicyError("allocation acceptance probe identity is unavailable") from exc
    environment = {
        **os.environ,
        "COMPOSE_PROJECT_NAME": project,
        "LOOM_IMAGE_TAG": str(request["candidate_sha"])[:12],
        "LOOM_WORKER_SANDBOX_IDENTITY": str(request["runtime_id"]),
        "LOOM_WORKER_CANDIDATE_SHA": str(request["candidate_sha"]),
        "LOOM_WORKER_SLURM_JOB_ID": job_id,
        "LOOM_WORKER_COMPOSE_PROJECT": project,
        "LOOM_WORKER_ENV_ID": str(request["env_id"]),
        "LOOM_WORKER_RESOURCE_GENERATION": str(
            request["applied_resource_generation"],
        ),
        "LOOM_WORKER_CANDIDATE_ID": str(request["candidate_id"]),
        "LOOM_WORKER_CANDIDATE_TREE": str(request["candidate_tree"]),
        "LOOM_WORKER_REGISTRY_GENERATION": str(request["registry_generation"]),
        "LOOM_WORKER_REGISTRY_PAYLOAD_SHA256": str(
            request["registry_snapshot_sha256"],
        ),
        "LOOM_WORKER_CGROUP_PARENT": cgroup_parent,
        "LOOM_WORKER_RESTART_POLICY": "no",
        "LOOM_ACCEPTANCE_PROBE_UID": str(identity.pw_uid),
        "LOOM_ACCEPTANCE_PROBE_GID": str(identity.pw_gid),
        "LOOM_ACCEPTANCE_PROBE_PROGRAM_HOST": str(
            source_root / _ACCEPTANCE_PROBE_PROGRAM,
        ),
        "LOOM_ACCEPTANCE_PROBE_REQUEST_HOST": str(worker_env.parent / "request.json"),
        "LOOM_ACCEPTANCE_PROBE_OUTPUT_HOST": str(result_path.parent),
    }
    return environment, project


def _validate_acceptance_probe_compose(
    rendered: Mapping[str, Any],
    request: Mapping[str, Any],
    *,
    project: str,
    job_id: str,
    cgroup_parent: str,
) -> None:
    services = rendered.get("services")
    if not isinstance(services, Mapping):
        raise PolicyError("allocation acceptance probe Compose services are invalid")
    expected_labels = {
        "loom.sandbox": str(request["runtime_id"]),
        "loom.candidate_sha": str(request["candidate_sha"]),
        "loom.slurm_job_id": job_id,
        "loom.compose_project": project,
        "loom.env_id": str(request["env_id"]),
        "loom.resource_generation": str(request["applied_resource_generation"]),
        "loom.candidate_id": str(request["candidate_id"]),
        "loom.candidate_tree": str(request["candidate_tree"]),
        "loom.registry_generation": str(request["registry_generation"]),
        "loom.registry_payload_sha256": str(request["registry_snapshot_sha256"]),
    }
    expected_image = f"loom-worker:{str(request['candidate_sha'])[:12]}"
    for service_name in ("sandbox-link", "acceptance-probe"):
        service = services.get(service_name)
        labels = service.get("labels") if isinstance(service, Mapping) else None
        if (
            not isinstance(service, Mapping)
            or service.get("image") != expected_image
            or service.get("cgroup_parent") != cgroup_parent
            or service.get("ports") not in (None, [])
            or service.get("network_mode") == "host"
            or service.get("restart") not in (None, "no")
            or not isinstance(labels, Mapping)
            or set(labels) != _ACCEPTANCE_PROBE_LABELS
            or dict(labels) != expected_labels
            or float(service.get("cpus", 0)) <= 0
            or int(service.get("pids_limit", 0)) <= 0
            or int(service.get("mem_limit", 0)) <= 0
        ):
            raise PolicyError("allocation acceptance probe Compose binding drifted")


def run_acceptance_probe_job(
    profile: Profile,
    *,
    probe_request: Path,
    result_path: Path,
) -> dict[str, Any]:
    """Run the fixed candidate-bound link probe inside one Slurm allocation."""
    request = _acceptance_probe_job_request(probe_request)
    job_id = os.environ.get("SLURM_JOB_ID", "")
    try:
        identity = pwd.getpwnam(str(request["service_user"]))
    except KeyError as exc:
        raise PolicyError("allocation acceptance probe identity is unavailable") from exc
    expected_result_parent = (
        Path("/srv/loom/developer-environments")
        / str(request["env_id"])
        / "evidence"
        / "acceptance-probes"
        / f"{request['cluster']}-{request['idempotency_key']}"
    )
    worker_env = (
        Path("/shared_work/loom/runtime/environments")
        / str(request["env_id"])
        / str(request["candidate_sha"])
        / f"worker-{request['domain']}.env"
    )
    if (
        re.fullmatch(r"[1-9][0-9]*", job_id) is None
        or os.geteuid() != identity.pw_uid
        or os.getegid() != identity.pw_gid
        or request["cluster"] != profile.cluster
        or request["submit_host"] != profile.submit_host
        or (request["domain"] != ("oldlab" if profile.cluster == "trt-oldlab" else "gb10"))
        or probe_request != expected_result_parent / "request.json"
        or result_path != expected_result_parent / "result.json"
    ):
        raise PolicyError("allocation acceptance probe execution binding drifted")
    source_root = Path(__file__).resolve().parents[2]
    try:
        cgroup_completed = subprocess.run(
            (
                "/usr/bin/python3",
                "-I",
                "-B",
                str(source_root / _ACCEPTANCE_CGROUP_PROGRAM),
                "--job-id",
                job_id,
                "--pids-max",
                str(profile.job_pids_max),
                "--wait-seconds",
                "30",
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=45,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError("allocation acceptance probe cgroup is unavailable") from exc
    cgroup_parent = cgroup_completed.stdout.strip()
    if cgroup_completed.returncode or not cgroup_parent.startswith("/"):
        raise PolicyError("allocation acceptance probe cgroup is unavailable")
    compose_environment, project = _acceptance_probe_compose_environment(
        request,
        job_id=job_id,
        cgroup_parent=cgroup_parent,
        worker_env=worker_env,
        result_path=result_path,
    )
    compose_environment["LOOM_ACCEPTANCE_PROBE_REQUEST_HOST"] = str(probe_request)
    compose = [
        "docker",
        "compose",
        "--env-file",
        str(worker_env),
    ]
    for relative in _ACCEPTANCE_PROBE_COMPOSE_FILES:
        compose.extend(("-f", str(source_root / relative)))
    try:
        config = subprocess.run(
            (*compose, "config", "--format", "json"),
            cwd=source_root,
            env=compose_environment,
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PolicyError(
            "allocation acceptance probe Compose validation failed safely",
        ) from exc
    try:
        rendered = json.loads(config.stdout)
    except json.JSONDecodeError as exc:
        raise PolicyError("allocation acceptance probe Compose output is invalid") from exc
    if config.returncode or not isinstance(rendered, dict):
        raise PolicyError("allocation acceptance probe Compose validation failed")
    _validate_acceptance_probe_compose(
        rendered,
        request,
        project=project,
        job_id=job_id,
        cgroup_parent=cgroup_parent,
    )
    failure: PolicyError | None = None
    try:
        for suffix in (
            ("up", "--detach", "--no-build", "--wait", "sandbox-link"),
            ("run", "--rm", "--no-deps", "acceptance-probe"),
        ):
            completed = subprocess.run(
                (*compose, *suffix),
                cwd=source_root,
                env=compose_environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=180,
            )
            if completed.returncode:
                raise PolicyError("allocation acceptance probe container failed")
    except (OSError, subprocess.SubprocessError, PolicyError) as exc:
        failure = (
            exc
            if isinstance(exc, PolicyError)
            else PolicyError("allocation acceptance probe container failed safely")
        )
    finally:
        try:
            down = subprocess.run(
                (*compose, "down", "--remove-orphans"),
                cwd=source_root,
                env=compose_environment,
                check=False,
                capture_output=True,
                text=True,
                timeout=120,
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise PolicyError("allocation acceptance probe cleanup failed safely") from exc
        if down.returncode:
            raise PolicyError("allocation acceptance probe cleanup failed")
    if failure is not None:
        raise failure
    output, _raw = _acceptance_probe_output(
        result_path,
        uid=identity.pw_uid,
        gid=identity.pw_gid,
    )
    if (
        output.get("kind") != _ACCEPTANCE_PROBE_CONTAINER_RESULT_KIND
        or output.get("request_payload_sha256") != request["payload_sha256"]
        or output.get("slurm_job_id") != job_id
    ):
        raise PolicyError("allocation acceptance probe result binding drifted")
    return output


def run_acceptance_probe_domain(
    root: Path,
    profile: Profile,
    raw: bytes,
    *,
    transport_request_id: str,
    registry_snapshot: Path = REGISTRY_SNAPSHOT_PATH,
) -> dict[str, Any]:
    if (
        root != Path("/")
        or os.geteuid() != 0
        or _REGISTRY.DIGEST_RE.fullmatch(transport_request_id) is None
    ):
        raise PolicyError("acceptance probe requires fixed live root authority")
    request, environment = _acceptance_probe_request(
        raw,
        profile,
        registry_snapshot=registry_snapshot,
    )
    if _slurm_node_for_host(profile, _canonical_host()) != profile.submit_host:
        raise PolicyError("acceptance probe reached the wrong submit host")
    state_root = root / _ACCEPTANCE_PROBE_RELATIVE / profile.cluster
    _prepare_private_directory(
        state_root,
        enforce_root_ownership=True,
        create=True,
    )
    state_path = state_root / f"{transport_request_id}.json"
    receipt_path = state_root / f"{transport_request_id}.receipt.json"
    existing_receipt = _load_journal(receipt_path)
    if existing_receipt is not None:
        if (
            existing_receipt.get("transport_request_id") != transport_request_id
            or existing_receipt.get("probe_request_sha256") != request["payload_sha256"]
        ):
            raise PolicyError("acceptance probe receipt replay drifted")
        return existing_receipt
    output_root = (
        Path(str(environment["evidence_root"]))
        / "acceptance-probes"
        / f"{profile.cluster}-{request['idempotency_key']}"
    )
    request_path = output_root / "request.json"
    output_path = output_root / "result.json"
    stdout_path = output_root / "slurm.stdout.log"
    state = _load_journal(state_path)
    created_transaction = state is None
    if state is not None and (
        set(state)
        != {
            "schema_version",
            "kind",
            "transport_request_id",
            "idempotency_key",
            "probe_request_sha256",
            "job_name",
            "job_id",
            "output_path",
            "phase",
            "created_at",
            "updated_at",
        }
        or state.get("schema_version") != 1
        or state.get("kind") != "loom.developer-environment.acceptance-probe-domain-transaction"
        or state.get("transport_request_id") != transport_request_id
        or state.get("idempotency_key") != request["idempotency_key"]
        or state.get("probe_request_sha256") != request["payload_sha256"]
        or state.get("job_name") != request["job_name"]
        or state.get("output_path") != str(output_path)
        or state.get("phase") not in {"prepared", "submitted"}
    ):
        raise PolicyError("acceptance probe durable transaction drifted")
    if state is None:
        now = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        state = {
            "schema_version": 1,
            "kind": "loom.developer-environment.acceptance-probe-domain-transaction",
            "transport_request_id": transport_request_id,
            "idempotency_key": request["idempotency_key"],
            "probe_request_sha256": request["payload_sha256"],
            "job_name": request["job_name"],
            "job_id": None,
            "output_path": str(output_path),
            "phase": "prepared",
            "created_at": now,
            "updated_at": now,
        }
        _atomic_write(
            state_path,
            (_canonical_json_bytes(state) + b"\n").decode("ascii"),
            mode=0o600,
        )
    job_id = state.get("job_id")
    if job_id is None:
        historical = [
            row
            for row in _probe_named_accounting_rows(str(request["job_name"]), profile)
            if "." not in row[0]
        ]
        if len(historical) > 1:
            raise PolicyError("acceptance probe idempotency history is ambiguous")
        if historical:
            job_id = historical[0][0]
        else:
            if not created_transaction:
                raise PolicyError(
                    "acceptance probe prepared transaction has no recoverable job; "
                    "duplicate submission refused",
                )
            try:
                service = pwd.getpwnam(str(request["service_user"]))
                output_root.mkdir(parents=True, mode=0o700, exist_ok=True)
                output_metadata = output_root.lstat()
                if (
                    not stat.S_ISDIR(output_metadata.st_mode)
                    or stat.S_ISLNK(output_metadata.st_mode)
                    or output_metadata.st_nlink < 2
                    or output_metadata.st_uid not in {0, service.pw_uid}
                    or output_metadata.st_gid not in {0, service.pw_gid}
                ):
                    raise PolicyError("acceptance probe output root is unsafe")
                os.chown(output_root, service.pw_uid, service.pw_gid)
                os.chmod(output_root, 0o700)
                _atomic_write(
                    request_path,
                    (_canonical_json_bytes(request) + b"\n").decode("ascii"),
                    mode=0o600,
                )
                os.chown(request_path, service.pw_uid, service.pw_gid)
            except KeyError as exc:
                raise PolicyError("acceptance probe service identity is unavailable") from exc
            except OSError as exc:
                raise PolicyError("acceptance probe output root is unavailable") from exc
            arguments = [
                "sbatch",
                "--parsable",
                f"--job-name={request['job_name']}",
                f"--uid={request['service_user']}",
                f"--account={request['slurm_account']}",
                f"--qos={request['slurm_qos']}",
                f"--clusters={profile.cluster}",
                f"--nodelist={profile.submit_host}",
                "--oversubscribe",
                "--nodes=1",
                "--ntasks=1",
                "--cpus-per-task=1",
                "--mem=256M",
                "--time=00:05:00",
                f"--output={stdout_path}",
                f"--error={stdout_path}.error",
                "--open-mode=truncate",
                "--export=NONE",
                f"--comment=loom-cgroup-v1:pids={profile.job_pids_max}",
            ]
            if profile.gpu_tres_per_slot > 0:
                arguments.append("--gres=gpu:1")
            arguments.append(
                f"--wrap={_acceptance_probe_wrapped_script(profile=profile, request_path=request_path, result_path=output_path)}",
            )
            submitted = _run(tuple(arguments), timeout=30).strip()
            job_id = submitted.split(";", 1)[0]
            if re.fullmatch(r"[1-9][0-9]*", job_id) is None:
                raise PolicyError("acceptance probe Slurm submission is invalid")
        state["job_id"] = job_id
        state["phase"] = "submitted"
        state["updated_at"] = datetime.now(UTC).isoformat().replace("+00:00", "Z")
        _atomic_write(
            state_path,
            (_canonical_json_bytes(state) + b"\n").decode("ascii"),
            mode=0o600,
        )
    if re.fullmatch(r"[1-9][0-9]*", str(job_id)) is None:
        raise PolicyError("acceptance probe durable job identity is invalid")
    _poll_probe_terminal(str(job_id), profile, timeout_seconds=420)
    rows = _acceptance_probe_accounting(str(job_id), profile)
    base = [row for row in rows if row[0] == str(job_id)]
    if (
        len(base) != 1
        or base[0][1] != request["job_name"]
        or _normalize_probe_job_state(base[0][2]) != "COMPLETED"
        or base[0][3].lower() != profile.submit_host.lower()
        or base[0][4] != request["slurm_account"]
        or base[0][5] != request["service_user"]
        or base[0][6] != profile.cluster
        or base[0][7] != request["slurm_qos"]
        or base[0][8] != "0:0"
    ):
        raise PolicyError("acceptance probe terminal accounting drifted")
    try:
        service = pwd.getpwnam(str(request["service_user"]))
    except KeyError as exc:
        raise PolicyError("acceptance probe service identity is unavailable") from exc
    output, output_raw = _acceptance_probe_output(
        output_path,
        uid=service.pw_uid,
        gid=service.pw_gid,
    )
    health = output.get("health")
    if (
        set(output)
        != {
            "schema_version",
            "kind",
            "request_payload_sha256",
            "slurm_job_id",
            "health",
            "completed_at",
        }
        or output.get("schema_version") != 1
        or output.get("kind") != _ACCEPTANCE_PROBE_CONTAINER_RESULT_KIND
        or output.get("request_payload_sha256") != request["payload_sha256"]
        or output.get("slurm_job_id") != str(job_id)
        or not isinstance(health, dict)
        or set(health) != set(_ACCEPTANCE_PROBE_SERVICES)
        or any(
            not isinstance(health[name], dict)
            or set(health[name])
            != {
                "service",
                "status",
                "http_status",
                "candidate_binding_sha256",
                "response_sha256",
            }
            or health[name].get("service") != name
            or health[name].get("status") != "healthy"
            or health[name].get("http_status") != 200
            or _REGISTRY.DIGEST_RE.fullmatch(
                str(health[name].get("candidate_binding_sha256")),
            )
            is None
            or _REGISTRY.DIGEST_RE.fullmatch(
                str(health[name].get("response_sha256")),
            )
            is None
            for name in _ACCEPTANCE_PROBE_SERVICES
        )
        or not isinstance(output.get("completed_at"), str)
    ):
        raise PolicyError("acceptance probe health output drifted")
    completed_at = _parse_allocation_timestamp(
        output["completed_at"],
        "acceptance probe completion",
    )
    if completed_at > datetime.now(UTC) + timedelta(minutes=5):
        raise PolicyError("acceptance probe completion timestamp is in the future")
    output_sha = hashlib.sha256(output_raw).hexdigest()
    authority_receipt_sha = hashlib.sha256(
        _canonical_json_bytes(
            {
                "transport_request_id": transport_request_id,
                "idempotency_key": request["idempotency_key"],
                "probe_request_sha256": request["payload_sha256"],
                "job_id": job_id,
                "job_output_sha256": output_sha,
            },
        )
        + b"\n",
    ).hexdigest()
    unsigned_receipt = {
        "schema_version": 1,
        "kind": _ACCEPTANCE_PROBE_RECEIPT_KIND,
        "status": "passed",
        "action": "acceptance-probe",
        "domain": request["domain"],
        "cluster": request["cluster"],
        "submit_host": request["submit_host"],
        "controller": request["controller"],
        **{
            field: request[field]
            for field in (
                "deployment_id",
                "env_id",
                "principal_id",
                "runtime_id",
                "candidate_id",
                "candidate_sha",
                "candidate_tree",
                "applied_resource_generation",
                "registry_generation",
                "registry_snapshot_sha256",
            )
        },
        "probe_request_sha256": request["payload_sha256"],
        "transport_request_id": transport_request_id,
        "submission_count": 1,
        "job": {
            "job_id": str(job_id),
            "job_name": request["job_name"],
            "user": request["service_user"],
            "account": request["slurm_account"],
            "qos": request["slurm_qos"],
            "submit_host": request["submit_host"],
            "controller": request["controller"],
            "allocation_nodes": [
                "oldlab-2" if request["domain"] == "oldlab" else profile.submit_host
            ],
            "time_limit_seconds": 300,
        },
        "health": health,
        "terminal": {
            "state": "COMPLETED",
            "exit_code": "0:0",
            "natural_exit": True,
            "cancel_requested": False,
            "timed_out": False,
        },
        "job_output_sha256": output_sha,
        "authority_receipt_sha256": authority_receipt_sha,
        "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
    }
    receipt = {
        **unsigned_receipt,
        "payload_sha256": hashlib.sha256(
            _canonical_json_bytes(unsigned_receipt) + b"\n",
        ).hexdigest(),
    }
    _atomic_write(
        receipt_path,
        (_canonical_json_bytes(receipt) + b"\n").decode("ascii"),
        mode=0o600,
    )
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "command",
        choices=(
            "candidate-set",
            "capacity-abort",
            "capacity-check",
            "capacity-finalize",
            "capacity-finalize-check",
            "capacity-reconcile",
            "capacity-reactivate",
            "capacity-retire",
            "capacity-rollback",
            "identity-check",
            "identity-reconcile",
            "identity-retire",
            "acceptance-probe-domain",
            "acceptance-probe-job",
            "plan",
            "check",
            "node-check",
            "apply",
            "rollback",
            "recover-drain",
            "materialize-runtime-proof",
            "allocation-probe",
            "allocation-node-check",
        ),
    )
    parser.add_argument("--profile", type=Path)
    parser.add_argument("--registry-snapshot", type=Path, default=REGISTRY_SNAPSHOT_PATH)
    parser.add_argument("--sandbox")
    parser.add_argument("--root", type=Path, default=Path("/"))
    parser.add_argument("--candidate-sha")
    parser.add_argument("--candidate-bindings-json")
    parser.add_argument("--transaction-id")
    parser.add_argument("--candidate-set-generation", type=int)
    parser.add_argument("--candidate-set-convergence-id")
    parser.add_argument("--candidate-set-payload-sha256")
    parser.add_argument("--candidate-root", type=Path)
    parser.add_argument("--worker-env", type=Path)
    parser.add_argument("--batch-uid", type=int)
    parser.add_argument("--batch-gid", type=int)
    parser.add_argument("--expected-tree")
    parser.add_argument("--expected-env-inode", type=int)
    parser.add_argument("--expected-env-sha256")
    parser.add_argument("--expected-host")
    parser.add_argument("--expected-pool")
    parser.add_argument("--expected-concurrency", type=int)
    parser.add_argument("--result-path", type=Path)
    parser.add_argument("--deployment-id")
    parser.add_argument("--transport-request-id")
    parser.add_argument("--probe-request", type=Path)
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
        if args.command == "capacity-abort":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-abort requires only --deployment-id and --execute",
                )
            if not args.execute:
                raise PolicyError("capacity-abort requires --execute")
            result = abort_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.command == "candidate-set":
            if (
                args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.execute
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError("candidate-set accepts only registry and generation bindings")
            result = load_slurm_candidate_set(
                args.registry_snapshot,
                require_root_ownership=args.registry_snapshot == REGISTRY_SNAPSHOT_PATH,
                generation=args.candidate_set_generation,
                convergence_id=args.candidate_set_convergence_id,
            )
            sys.stdout.write(
                json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
            )
            return 0
        if args.command == "capacity-reconcile":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-reconcile requires only --deployment-id and --execute",
                )
            if not args.execute:
                raise PolicyError("capacity-reconcile requires --execute")
            result = reconcile_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.command == "capacity-reactivate":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-reactivate requires only --deployment-id and --execute",
                )
            if not args.execute:
                raise PolicyError("capacity-reactivate requires --execute")
            result = reactivate_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.command == "capacity-retire":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-retire requires only --deployment-id and --execute",
                )
            if not args.execute:
                raise PolicyError("capacity-retire requires --execute")
            result = retire_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.command == "capacity-finalize":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-finalize requires only --deployment-id and --execute",
                )
            if not args.execute:
                raise PolicyError("capacity-finalize requires --execute")
            result = finalize_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.command == "capacity-finalize-check":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.execute
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-finalize-check requires only --deployment-id",
                )
            result = check_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
                finalized=True,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.command == "capacity-rollback":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-rollback requires only --deployment-id and --execute",
                )
            if not args.execute:
                raise PolicyError("capacity-rollback requires --execute")
            result = rollback_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.command == "capacity-check":
            if (
                args.deployment_id is None
                or args.profile is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.execute
                or args.restart
                or args.apply_accounting
                or args.root != Path("/")
            ):
                raise PolicyError(
                    "capacity-check requires only --deployment-id",
                )
            result = check_capacity(
                args.deployment_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(
                json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
            )
            return 0
        if args.command == "recover-drain":
            if not args.execute:
                raise PolicyError("recover-drain requires --execute")
            if any(
                value is not None
                for value in (
                    args.profile,
                    args.candidate_sha,
                    args.candidate_root,
                    args.worker_env,
                    args.sandbox,
                    args.transaction_id,
                    args.candidate_set_generation,
                    args.candidate_set_convergence_id,
                    args.candidate_set_payload_sha256,
                )
            ) or args.root != Path("/"):
                raise PolicyError("recover-drain accepts no caller-selected binding")
            result = recover_pending_drains()
            sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
            return 0
        if args.profile is None:
            raise PolicyError(f"{args.command} requires --profile")
        profile = load_profile(args.profile)
        if args.command == "acceptance-probe-job":
            if (
                args.probe_request is None
                or args.result_path is None
                or args.transport_request_id is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.deployment_id is not None
                or args.restart
                or args.apply_accounting
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.root != Path("/")
                or not args.execute
            ):
                raise PolicyError("acceptance probe job arguments are invalid")
            result = run_acceptance_probe_job(
                profile,
                probe_request=args.probe_request,
                result_path=args.result_path,
            )
            sys.stdout.write(
                json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
            )
            return 0
        if args.command == "acceptance-probe-domain":
            if (
                args.transport_request_id is None
                or args.probe_request is not None
                or args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.deployment_id is not None
                or args.restart
                or args.apply_accounting
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or args.root != Path("/")
                or not args.execute
            ):
                raise PolicyError("acceptance probe domain arguments are invalid")
            raw = sys.stdin.buffer.read((1 << 20) + 1)
            if len(raw) > (1 << 20):
                raise PolicyError("acceptance probe domain request is too large")
            result = run_acceptance_probe_domain(
                args.root,
                profile,
                raw,
                transport_request_id=args.transport_request_id,
                registry_snapshot=args.registry_snapshot,
            )
            sys.stdout.write(
                json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
            )
            return 0
        if args.command in {"identity-check", "identity-reconcile", "identity-retire"}:
            if (
                args.sandbox is not None
                or args.candidate_sha is not None
                or args.candidate_bindings_json is not None
                or args.deployment_id is not None
                or args.restart
                or args.apply_accounting
                or args.registry_snapshot != REGISTRY_SNAPSHOT_PATH
                or _REGISTRY.DIGEST_RE.fullmatch(str(args.transaction_id)) is None
                or (args.command == "identity-check" and args.execute)
                or (args.command != "identity-check" and not args.execute)
            ):
                raise PolicyError("incremental Slurm identity arguments are invalid")
            raw = sys.stdin.buffer.read(64 * 1024 + 1)
            if len(raw) > 64 * 1024:
                raise PolicyError("incremental Slurm identity payload is too large")
            identity = _incremental_identity_payload(raw, profile)
            if args.command == "identity-check":
                result = incremental_identity_check(profile, identity)
            elif args.command == "identity-reconcile":
                result = incremental_identity_reconcile(
                    args.root,
                    profile,
                    identity,
                    transaction_id=str(args.transaction_id),
                )
            else:
                if identity["schema_version"] != 2:
                    raise PolicyError(
                        "incremental Slurm retirement requires identity schema v2",
                    )
                result = incremental_identity_retire(
                    args.root,
                    profile,
                    identity,
                    transaction_id=str(args.transaction_id),
                )
            sys.stdout.write(
                json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n",
            )
            return 0
        candidate = args.candidate_sha or source_candidate_sha()
        if (
            args.command
            in {
                "check",
                "node-check",
                "materialize-runtime-proof",
                "allocation-probe",
                "allocation-node-check",
            }
            and args.sandbox is None
        ):
            raise PolicyError(f"{args.command} requires --sandbox")
        candidate_bindings: dict[str, dict[str, Any]] | None = None
        if args.candidate_bindings_json is not None:
            try:
                raw_candidate_bindings = json.loads(args.candidate_bindings_json)
            except json.JSONDecodeError as exc:
                raise PolicyError("candidate bindings JSON is invalid") from exc
            if not isinstance(
                raw_candidate_bindings, dict
            ) or args.candidate_bindings_json != json.dumps(
                raw_candidate_bindings,
                sort_keys=True,
                separators=(",", ":"),
            ):
                raise PolicyError("candidate bindings JSON is not canonical")
            candidate_bindings = _candidate_bindings(profile, raw_candidate_bindings)
            if args.root == Path("/"):
                _require_current_registry_bindings(
                    candidate_bindings,
                    path=args.registry_snapshot,
                )
            profile = _profile_with_bindings(profile, candidate_bindings)
        elif args.root == Path("/") and args.command not in {"recover-drain"}:
            candidate_set = load_slurm_candidate_set(args.registry_snapshot)
            candidate_bindings = _candidate_bindings(
                profile,
                candidate_set["candidate_bindings"],
            )
            profile = _profile_with_bindings(profile, candidate_bindings)
        if args.command in {
            "check",
            "node-check",
            "materialize-runtime-proof",
            "allocation-probe",
            "allocation-node-check",
        }:
            if args.sandbox is None:
                raise PolicyError(f"{args.command} requires --sandbox")
            _sandbox_account(profile, args.sandbox)
        if args.command == "node-check":
            if args.root != Path("/"):
                raise PolicyError("node check requires the live root")
            _sandbox_account(profile, args.sandbox)
            _host, slurm_node = _validate_live_apply(
                args.root,
                profile,
                candidate_sha=candidate,
                restart=False,
                apply_accounting=False,
            )
            with _domain_lock(args.root, profile):
                if candidate_bindings is None:
                    raise PolicyError("node check requires the complete candidate set")
                result = plan(
                    args.root,
                    profile,
                    candidate_sha=candidate,
                    candidate_bindings=candidate_bindings,
                )
                if not result["file_plan"]["converged"]:
                    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
                    return 1
                result["live_readback"] = _live_readback_unlocked(
                    args.root,
                    profile,
                    sandbox=None,
                    candidate_sha=candidate,
                    candidate_bindings=candidate_bindings,
                    require_probe=False,
                    check_accounting=slurm_node == profile.controller,
                )
        elif args.command == "allocation-node-check":
            required = {
                "candidate_root": args.candidate_root,
                "worker_env": args.worker_env,
                "expected_tree": args.expected_tree,
                "expected_env_inode": args.expected_env_inode,
                "expected_env_sha256": args.expected_env_sha256,
                "batch_uid": args.batch_uid,
                "batch_gid": args.batch_gid,
                "expected_host": args.expected_host,
                "expected_pool": args.expected_pool,
                "expected_concurrency": args.expected_concurrency,
                "result_path": args.result_path,
            }
            if any(value is None for value in required.values()):
                raise PolicyError("allocation-node-check requires every exact binding")
            result = allocation_node_check(
                profile,
                sandbox=args.sandbox,
                candidate_sha=candidate,
                candidate_root=args.candidate_root,
                worker_env=args.worker_env,
                expected_tree=args.expected_tree,
                expected_env_inode=args.expected_env_inode,
                expected_env_sha256=args.expected_env_sha256,
                batch_uid=args.batch_uid,
                batch_gid=args.batch_gid,
                expected_host=args.expected_host,
                expected_pool=args.expected_pool,
                expected_concurrency=args.expected_concurrency,
                result_path=args.result_path,
            )
        elif args.command == "materialize-runtime-proof" and args.execute:
            if args.root != Path("/"):
                raise PolicyError("runtime proof materialization requires the live root")
            verify_source_candidate(candidate)
            repository = Path(__file__).resolve().parents[2]
            candidate_tree = (
                _git_read(
                    repository,
                    "rev-parse",
                    "--verify",
                    f"{candidate}^{{tree}}",
                )
                .decode("ascii")
                .strip()
            )
            result = materialize_runtime_proof(
                args.root,
                profile,
                sandbox=args.sandbox,
                candidate_sha=candidate,
                candidate_tree=candidate_tree,
            )
        elif args.command == "apply" and args.execute:
            result = apply(
                args.root,
                profile,
                restart=args.restart,
                apply_accounting=args.apply_accounting,
                candidate_sha=candidate,
                candidate_bindings=candidate_bindings,
                transaction_id=args.transaction_id,
                generation=args.candidate_set_generation,
                convergence_id=args.candidate_set_convergence_id,
                payload_sha256=args.candidate_set_payload_sha256,
            )
        elif args.command == "rollback" and args.execute:
            result = rollback(
                args.root,
                profile,
                candidate_sha=candidate,
                candidate_bindings=candidate_bindings,
                transaction_id=args.transaction_id,
                generation=args.candidate_set_generation,
                convergence_id=args.candidate_set_convergence_id,
                payload_sha256=args.candidate_set_payload_sha256,
            )
        elif args.command == "materialize-runtime-proof":
            raise PolicyError("materialize-runtime-proof requires --execute")
        elif args.command == "allocation-probe" and args.execute:
            if (
                args.sandbox is None
                or args.candidate_root is None
                or args.worker_env is None
                or args.batch_uid is None
                or args.batch_gid is None
                or args.expected_pool is None
                or args.expected_concurrency is None
            ):
                raise PolicyError(
                    "allocation-probe requires candidate, identity, pool, and concurrency",
                )
            result = run_allocation_probe(
                args.root,
                profile,
                sandbox=args.sandbox,
                candidate_sha=candidate,
                candidate_root=args.candidate_root,
                worker_env=args.worker_env,
                batch_uid=args.batch_uid,
                batch_gid=args.batch_gid,
                expected_pool=args.expected_pool,
                expected_concurrency=args.expected_concurrency,
                timeout_seconds=args.allocation_timeout_seconds,
            )
        elif args.command == "allocation-probe":
            raise PolicyError("allocation-probe requires --execute")
        else:
            check_lock = (
                _domain_lock(args.root, profile)
                if args.command == "check" and args.root == Path("/")
                else nullcontext()
            )
            with check_lock:
                result = plan(
                    args.root,
                    profile,
                    candidate_sha=candidate,
                    candidate_bindings=candidate_bindings,
                )
                if args.command != "check":
                    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
                    return 0
                if not result["file_plan"]["converged"]:
                    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
                    return 1
                if args.root == Path("/"):
                    verify_source_candidate(candidate)
                    if (
                        args.sandbox is None
                        or args.candidate_root is None
                        or args.worker_env is None
                        or args.batch_uid is None
                        or args.batch_gid is None
                        or args.expected_pool is None
                        or args.expected_concurrency is None
                    ):
                        raise PolicyError(
                            "live check requires candidate, identity, pool, and concurrency",
                        )
                    binding = strict_candidate_binding(
                        args.candidate_root,
                        args.worker_env,
                        candidate_sha=candidate,
                        expected_batch_uid=args.batch_uid,
                        expected_batch_gid=args.batch_gid,
                    )
                    runtime_attestation = _runtime_attestation_binding(
                        args.root,
                        profile,
                        sandbox=args.sandbox,
                        candidate_sha=candidate,
                        candidate_tree=binding["repository"]["candidate_tree"],
                        candidate_root=args.candidate_root,
                        worker_env=args.worker_env,
                        enforce_root_ownership=True,
                    )
                    result["live_readback"] = _live_readback_unlocked(
                        args.root,
                        profile,
                        sandbox=args.sandbox,
                        candidate_sha=candidate,
                        candidate_bindings=(
                            candidate_bindings
                            if candidate_bindings is not None
                            else _offline_candidate_bindings(profile, candidate)
                        ),
                        require_probe=True,
                        check_accounting=True,
                        candidate_binding=binding,
                        runtime_attestation=runtime_attestation,
                        expected_pool=args.expected_pool,
                        expected_concurrency=args.expected_concurrency,
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
