#!/usr/bin/env python3
"""Converge registry-owned developer environments on oldlab-2.

All public mutation commands are plan-only unless ``--execute`` is supplied.
The installed systemd entry point has no repository, path, host, or secret
overrides.  The ``environment-*`` commands delegate to the dynamic registry
orchestrator.  The older sandbox commands remain only for migration convergence
of environments whose registry layout is ``legacy-v1``.
"""

from __future__ import annotations

import argparse
import base64
import errno
import fcntl
import grp
import hashlib
import io
import json
import os
import pwd
import re
import secrets
import shlex
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from scripts.ops import developer_environment_registry as environment_registry
from scripts.ops import developer_sandbox_slurm_policy as slurm_policy

from loom_cli import external_slurm_acceptance as external_slurm

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PROFILES = REPO_ROOT / "deploy/developer-sandboxes"
SOURCE_UNIT = SOURCE_PROFILES / "loom-developer-sandbox@.service"

# Migration input only. Normal deployment and renewal enumerate the verified
# registry snapshot and never use this seed cohort as an allowlist.
LEGACY_SEED_RUNTIME_IDS = ("qianyi", "hongjian", "devansh")
LEGACY_SEED_BATCH_IDENTITIES = {
    "qianyi": ("loom-sandbox-qianyi", 31021),
    "hongjian": ("loom-sandbox-hongjian", 31022),
    "devansh": ("loom-sandbox-devansh", 31023),
}
EXPECTED_HOSTNAME = "trt-eai-oldlab-2"
SHARED_GROUP = "sharedwork"
NFS_ROOT = Path("/shared_work/loom/candidates/sandboxes")
NFS_RUNTIME_ROOT = Path("/shared_work/loom/runtime/sandboxes")
STATE_PARENT = Path("/srv/loom/developer-sandboxes")
CONFIG_ROOT = Path("/etc/loom/developer-sandboxes")
DESIRED_ROOT = CONFIG_ROOT / "desired"
PROFILE_CONFIG_ROOT = CONFIG_ROOT / "profiles"
TRANSACTION_ROOT = Path("/var/lib/loom-developer-sandbox-installer/transactions")
SLURM_MAINTENANCE_ROOT = Path(
    "/var/lib/loom-developer-sandbox-installer/slurm-maintenance",
)
TRANSACTION_LOCK_ROOT = Path("/run/loom-developer-sandbox-installer")
SOURCE_STAGING_ROOT = Path("/var/lib/loom-developer-sandbox-installer/source")
RENEWAL_STATE_ROOT = Path("/var/lib/loom-developer-sandbox-installer/renewals")
COMBINED_RECEIPT_ROOT = Path("/var/lib/loom-shared-capacity/runtime-attestations")
FLEET_ATTESTATION_ROOT = Path("/var/lib/loom-developer-sandbox-links/attestations")
REMOTE_LINK_ISSUANCE_ROOT = Path("/var/lib/loom/developer-sandbox-links/issuance")
REMOTE_LINK_SERVER_ROOT = Path("/etc/loom/developer-sandbox-links/server")
NODE_AUTHORITY_PROGRAM = Path(
    "/usr/local/libexec/loom-developer-sandbox-node-authority",
)
NODE_TRANSPORT_PROGRAM = Path(
    "/usr/local/libexec/loom-developer-sandbox-node-transport",
)
INSTALLED_REMOTE_LINK_HOST = Path(
    "/usr/local/libexec/loom-developer-sandbox-remote-link-host",
)
INSTALLED_DOMAIN_RUNTIME = Path("/usr/local/libexec/loom-developer-domain-runtime")
INSTALLED_DOMAIN_RUNTIME_CONFIG = Path("/etc/loom/developer-runtime-domains.toml")
INSTALLED_CAPACITY_POLICY_ROOT = Path(
    "/etc/loom/developer-shared-capacity-policies",
)
UNIT_PATH = Path("/etc/systemd/system/loom-developer-sandbox@.service")
RENEWAL_SERVICE_PATH = Path(
    "/etc/systemd/system/loom-developer-sandbox-attestation-renewal.service",
)
RENEWAL_TIMER_PATH = Path(
    "/etc/systemd/system/loom-developer-sandbox-attestation-renewal.timer",
)
INSTALLER_TMPFILES_PATH = Path(
    "/etc/tmpfiles.d/loom-developer-sandbox-installer.conf",
)
INSTALLED_PROGRAM = Path("/usr/local/libexec/loom-developer-sandbox-host")
STAGING_ALLOCATION_CONFIG = Path(
    "/etc/loom/staging-external-slurm-authority/authority.toml",
)
STAGING_SUBMISSION_WAL_ROOT = Path(
    "/var/lib/loom-staging-external-slurm-authority/submissions",
)
UNIT_NAME = "loom-developer-sandbox@{sandbox}.service"
RENEWAL_TIMER = "loom-developer-sandbox-attestation-renewal.timer"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
SYSTEMD_SLICE_RE = re.compile(r"^loom-job-[1-9][0-9]*-[0-9a-f]{40}\.slice$")
SYSTEMD_SLICE_RECEIPT_ROOT = Path(
    "/run/loom-developer-sandbox-slurm-policy/systemd-slices",
)
SYSTEMD_UNIT_ROOT = Path("/run/systemd/system")
SLURM_AUTHORITY_BINDING_RE = re.compile(
    r"^slurm-policy-v1:(trt-oldlab|trt-gb10):[0-9a-f]{64}:[0-9a-f]{64}$",
)
SLURM_AUTHORITY_RECEIPT_FIELDS = {
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
FINGERPRINT_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
RENEWAL_HISTORY_RE = re.compile(r"^([0-9]{20})-([0-9a-f]{64})\.json$")
TRANSACTION_TTL = timedelta(minutes=30)
RECEIPT_FRESHNESS = timedelta(seconds=60)
ATTESTATION_TTL = timedelta(minutes=15)
DOMAIN_PEERS = {
    "oldlab": ("oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4", "oldlab-5"),
    "gb10": tuple(f"trt-gb10-{index}" for index in range(1, 16)),
}
DOMAIN_PUBLISHERS = {"oldlab": "oldlab-1", "gb10": "trt-gb10-1"}
DOMAIN_CAPACITY_NODES = {
    "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(1, 6)),
    "gb10": tuple(f"trt-gb10-{index}" for index in range(1, 16)),
}
ELIGIBLE_LINK_NODES = DOMAIN_PEERS["oldlab"] + DOMAIN_PEERS["gb10"]
REMOTE_LINK_SERVER_ADDRESS = "192.168.50.14"
REMOTE_LINK_SERVICE_NAMES = ("control-plane", "gateway", "minio")
LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS = {
    "qianyi": {
        "control-plane": (26080, 20080),
        "gateway": (26100, 20100),
        "minio": (26900, 20900),
    },
    "hongjian": {
        "control-plane": (27080, 21080),
        "gateway": (27100, 21100),
        "minio": (27900, 21900),
    },
    "devansh": {
        "control-plane": (28080, 22080),
        "gateway": (28100, 22100),
        "minio": (28900, 22900),
    },
}
REMOTE_LINK_HEALTH_PATHS = {
    "control-plane": "/healthz",
    "gateway": "/healthz",
    "minio": "/minio/health/live",
}

SECRET_KEYS = (
    "LOOM_DEV_POSTGRES_USER",
    "LOOM_DEV_POSTGRES_PASSWORD",
    "LOOM_DEV_MINIO_ROOT_USER",
    "LOOM_DEV_MINIO_ROOT_PASSWORD",
    "LOOM_CP_STEP_JWT_SIGNING_KEY",
    "LOOM_SECRET_STORE_MASTER_KEY",
    "LOOM_WORKER_TOKEN",
)


class HostConvergeError(RuntimeError):
    """A secret-safe host convergence failure."""


@dataclass(frozen=True, slots=True)
class Profile:
    sandbox: str
    compose_project: str
    canonical_hostname: str
    candidate_root: Path
    state_root: Path
    cache_root: Path
    evidence_root: Path
    runtime_root: Path
    ports: dict[str, int]
    env_id: str | None = None
    resource_generation: int | None = None
    registry_generation: int | None = None
    registry_payload_sha256: str | None = None
    candidate_id: str | None = None
    candidate_tree: str | None = None
    service_user: str | None = None
    worker_image_ids: dict[str, str] | None = None

    @property
    def secrets_root(self) -> Path:
        return self.state_root / "secrets"

    @property
    def secrets_env(self) -> Path:
        return self.secrets_root / "sandbox.env"

    @property
    def admin_secret(self) -> Path:
        return self.secrets_root / "admin.toml"

    @property
    def state_file(self) -> Path:
        return self.state_root / "sandbox-state.json"

    @property
    def desired_file(self) -> Path:
        return DESIRED_ROOT / f"{self.sandbox}.json"

    @property
    def private_runtime_root(self) -> Path:
        return self.state_root / "runtime"

    def worker_runtime_env(self, sha: str, domain: str = "oldlab") -> Path:
        if SHA_RE.fullmatch(sha) is None or domain not in {"oldlab", "gb10"}:
            raise HostConvergeError("worker runtime identity is invalid")
        return self.runtime_root / sha / f"worker-{domain}.env"

    def worker_image_id(self, domain: str) -> str:
        image_ids = self.worker_image_ids
        image_id = image_ids.get(domain) if isinstance(image_ids, dict) else None
        if (
            domain not in DOMAIN_PEERS
            or set(image_ids or {}) != set(DOMAIN_PEERS)
            or FINGERPRINT_RE.fullmatch(str(image_id)) is None
            or image_ids["oldlab"] == image_ids["gb10"]
        ):
            raise HostConvergeError("worker image config ID binding is incomplete")
        return str(image_id)


@dataclass(frozen=True, slots=True)
class Identity:
    user: str
    group: str
    uid: int
    gid: int


@dataclass(frozen=True, slots=True)
class ActivationReceipt:
    path: Path
    payload_sha256: str
    fleet_payload_sha256: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class StagingAllocationConfig:
    environment: str
    pool: str
    source_host: str
    submit_host: str
    controller: str
    cluster: str
    partition: str
    producer_user: str
    producer_group: str
    producer_uid: int
    producer_gid: int
    producer_home: Path
    producer_shell: Path
    batch_user: str
    batch_group: str
    batch_uid: int
    batch_gid: int
    batch_home: Path
    batch_shell: Path
    batch_supplementary_groups: tuple[str, ...]
    shared_mount_source: str
    shared_mount_target: Path
    shared_mount_filesystem_type: str
    shared_mount_unit: str
    repository_root: Path
    worker_env_root: Path
    result_root: Path
    slurm_account: str
    qos: str
    infrastructure_nodes: tuple[str, ...]
    allowed_nodes: tuple[str, ...]
    excluded_nodes: tuple[str, ...]
    host_aliases: dict[str, str]
    repository_template: str
    worker_env_template: str
    probe_result_root: Path
    job_timeout_seconds: int
    heartbeat_interval_seconds: int

    def candidate_paths(self, candidate_sha: str) -> tuple[Path, Path]:
        image_tag = f"staging-{candidate_sha[:7]}"
        return (
            Path(self.repository_template.format(image_tag=image_tag)),
            Path(self.worker_env_template.format(image_tag=image_tag)),
        )


def _load_profile(path: Path) -> Profile:
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError(f"could not load profile {path}") from exc
    sandbox = raw.get("sandbox")
    if sandbox not in LEGACY_SEED_RUNTIME_IDS or path.stem != sandbox:
        raise HostConvergeError(f"invalid sandbox profile identity: {path}")
    ports = raw.get("ports")
    if not isinstance(ports, dict) or not ports:
        raise HostConvergeError(f"profile ports are invalid: {path}")
    parsed_ports: dict[str, int] = {}
    for name, value in ports.items():
        if not isinstance(name, str) or type(value) is not int or not 1 <= value <= 65535:
            raise HostConvergeError(f"profile ports are invalid: {path}")
        parsed_ports[name] = value
    private_runtime_root = Path(str(raw.get("runtime_root", "")))
    profile = Profile(
        sandbox=sandbox,
        compose_project=str(raw.get("compose_project", "")),
        canonical_hostname=str(raw.get("canonical_hostname", "")),
        candidate_root=Path(str(raw.get("candidate_root", ""))),
        state_root=Path(str(raw.get("state_root", ""))),
        cache_root=Path(str(raw.get("cache_root", ""))),
        evidence_root=Path(str(raw.get("evidence_root", ""))),
        runtime_root=NFS_RUNTIME_ROOT / sandbox,
        ports=parsed_ports,
    )
    if profile.compose_project != f"loom-sandbox-{sandbox}":
        raise HostConvergeError(f"invalid Compose project in {path}")
    if profile.canonical_hostname != EXPECTED_HOSTNAME:
        raise HostConvergeError(f"invalid host binding in {path}")
    if profile.candidate_root != NFS_ROOT / sandbox:
        raise HostConvergeError(f"invalid candidate root in {path}")
    expected_state = STATE_PARENT / sandbox
    if profile.state_root != expected_state:
        raise HostConvergeError(f"invalid state root in {path}")
    expected_children = {
        profile.cache_root: expected_state / "cache",
        profile.evidence_root: expected_state / "evidence",
        private_runtime_root: expected_state / "runtime",
    }
    if any(actual != expected for actual, expected in expected_children.items()):
        raise HostConvergeError(f"invalid private roots in {path}")
    return profile


def load_profiles(root: Path = SOURCE_PROFILES) -> tuple[Profile, ...]:
    profiles = tuple(_load_profile(root / f"{sandbox}.toml") for sandbox in LEGACY_SEED_RUNTIME_IDS)
    all_ports = [port for profile in profiles for port in profile.ports.values()]
    if len(all_ports) != len(set(all_ports)):
        raise HostConvergeError("sandbox host ports collide")
    for field in ("compose_project", "candidate_root", "state_root"):
        values = [getattr(profile, field) for profile in profiles]
        if len(values) != len(set(values)):
            raise HostConvergeError(f"sandbox {field} values collide")
    return profiles


def load_staging_allocation_config(
    path: Path = STAGING_ALLOCATION_CONFIG,
) -> StagingAllocationConfig:
    try:
        raw_bytes = path.read_bytes()
        raw = tomllib.loads(raw_bytes.decode("utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("fixed staging allocation config is unavailable") from exc
    if len(raw_bytes) > 128 * 1024 or raw.get("schema_version") != 1:
        raise HostConvergeError("fixed staging allocation config is invalid")
    candidate_paths = raw.get("candidate_paths")
    probe = raw.get("probe")
    host_aliases = raw.get("host_aliases")
    shared_mount = raw.get("shared_mount")
    if (
        not isinstance(candidate_paths, dict)
        or not isinstance(probe, dict)
        or not isinstance(host_aliases, dict)
        or not isinstance(shared_mount, dict)
    ):
        raise HostConvergeError("fixed staging allocation config is incomplete")

    def text(source: Mapping[str, Any], field: str) -> str:
        value = source.get(field)
        if not isinstance(value, str) or not value or value != value.strip():
            raise HostConvergeError(f"fixed staging allocation {field} is invalid")
        return value

    def integer(source: Mapping[str, Any], field: str, minimum: int) -> int:
        value = source.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise HostConvergeError(f"fixed staging allocation {field} is invalid")
        return value

    def text_list(source: Mapping[str, Any], field: str) -> tuple[str, ...]:
        value = source.get(field)
        if (
            not isinstance(value, list)
            or not value
            or any(not isinstance(item, str) or not item for item in value)
        ):
            raise HostConvergeError(f"fixed staging allocation {field} is invalid")
        items = tuple(value)
        if len(items) != len(set(items)):
            raise HostConvergeError(f"fixed staging allocation {field} has duplicates")
        return items

    infrastructure_nodes = text_list(raw, "infrastructure_nodes")
    allowed_nodes = text_list(raw, "allowed_nodes")
    excluded_nodes_raw = raw.get("excluded_nodes")
    if not isinstance(excluded_nodes_raw, list) or excluded_nodes_raw:
        raise HostConvergeError("fixed staging allocation excluded_nodes must be empty")
    aliases = {
        str(node): str(host)
        for node, host in host_aliases.items()
        if isinstance(node, str) and isinstance(host, str) and host
    }
    config = StagingAllocationConfig(
        environment=text(raw, "environment"),
        pool=text(raw, "pool"),
        source_host=text(raw, "source_host"),
        submit_host=text(raw, "submit_host"),
        controller=text(raw, "controller"),
        cluster=text(raw, "cluster"),
        partition=text(raw, "partition"),
        producer_user=text(raw, "producer_user"),
        producer_group=text(raw, "producer_group"),
        producer_uid=integer(raw, "producer_uid", 1),
        producer_gid=integer(raw, "producer_gid", 1),
        producer_home=Path(text(raw, "producer_home")),
        producer_shell=Path(text(raw, "producer_shell")),
        batch_user=text(raw, "batch_user"),
        batch_group=text(raw, "batch_group"),
        batch_uid=integer(raw, "batch_uid", 1),
        batch_gid=integer(raw, "batch_gid", 1),
        batch_home=Path(text(raw, "batch_home")),
        batch_shell=Path(text(raw, "batch_shell")),
        batch_supplementary_groups=text_list(raw, "batch_supplementary_groups"),
        shared_mount_source=text(shared_mount, "source"),
        shared_mount_target=Path(text(shared_mount, "target")),
        shared_mount_filesystem_type=text(shared_mount, "filesystem_type"),
        shared_mount_unit=text(shared_mount, "unit"),
        repository_root=Path(text(shared_mount, "repository_root")),
        worker_env_root=Path(text(shared_mount, "worker_env_root")),
        result_root=Path(text(shared_mount, "result_root")),
        slurm_account=text(raw, "slurm_account"),
        qos=text(raw, "qos"),
        infrastructure_nodes=infrastructure_nodes,
        allowed_nodes=allowed_nodes,
        excluded_nodes=(),
        host_aliases=aliases,
        repository_template=text(candidate_paths, "repository"),
        worker_env_template=text(candidate_paths, "worker_env"),
        probe_result_root=Path(text(probe, "result_root")),
        job_timeout_seconds=integer(probe, "job_timeout_seconds", 30),
        heartbeat_interval_seconds=integer(
            probe,
            "heartbeat_interval_seconds",
            1,
        ),
    )
    expected_infrastructure_nodes = tuple(f"trt-gb10-{index}" for index in range(1, 16))
    expected_allowed_nodes = expected_infrastructure_nodes
    paths = (
        config.producer_home,
        config.producer_shell,
        config.batch_home,
        config.batch_shell,
        config.shared_mount_target,
        config.repository_root,
        config.worker_env_root,
        config.result_root,
        config.probe_result_root,
    )
    if (
        config.environment != "staging"
        or config.pool != "gb10"
        or config.source_host != "trt-eai-oldlab-1"
        or config.submit_host != "trt-gb10-1"
        or config.controller != "trt-gb10-1"
        or config.cluster != "trt-gb10"
        or config.partition != "gb10"
        or config.producer_user != "loom-rollout"
        or config.producer_group != "loom-rollout"
        or config.producer_uid != 995
        or config.producer_gid != 982
        or config.producer_home != Path("/var/lib/loom-staging-rollout")
        or config.producer_shell != Path("/bin/sh")
        or config.batch_user != "loom-staging-worker"
        or config.batch_group != "loom-staging-worker"
        or config.batch_uid != 31024
        or config.batch_gid != 31024
        or config.batch_home != Path("/nonexistent")
        or config.batch_shell != Path("/usr/sbin/nologin")
        or config.batch_supplementary_groups != ("docker",)
        or config.shared_mount_source != "192.168.20.12:/shared_work2/loom/staging"
        or config.shared_mount_target != Path("/srv/loom/staging-shared")
        or config.shared_mount_filesystem_type != "nfs4"
        or config.shared_mount_unit != r"srv-loom-staging\x2dshared.mount"
        or config.repository_root != config.shared_mount_target / "candidates"
        or config.worker_env_root != config.shared_mount_target / "generated"
        or config.result_root != config.shared_mount_target / "results"
        or config.probe_result_root != config.result_root
        or config.slurm_account != "loom-staging"
        or config.qos != "loom-staging"
        or config.infrastructure_nodes != expected_infrastructure_nodes
        or config.allowed_nodes != expected_allowed_nodes
        or config.excluded_nodes
        or not set(config.allowed_nodes).issubset(config.infrastructure_nodes)
        or set(config.host_aliases) != set(expected_infrastructure_nodes)
        or len(set(config.host_aliases.values())) != len(expected_infrastructure_nodes)
        or any(not item.is_absolute() or ".." in item.parts for item in paths)
        or config.job_timeout_seconds > 600
        or config.heartbeat_interval_seconds > 30
    ):
        raise HostConvergeError("fixed staging allocation config drifted")
    return config


def _identity(user: str, group: str) -> Identity:
    try:
        account = pwd.getpwnam(user)
        group_row = grp.getgrnam(group)
    except KeyError as exc:
        raise HostConvergeError(f"required host identity is absent: {exc}") from exc
    return Identity(user=user, group=group, uid=account.pw_uid, gid=group_row.gr_gid)


def _sandbox_batch_identity(sandbox: str) -> Identity:
    try:
        user, expected_id = LEGACY_SEED_BATCH_IDENTITIES[sandbox]
    except KeyError as exc:
        raise HostConvergeError("sandbox batch identity is outside the fixed contract") from exc
    identity = _identity(user, user)
    account = pwd.getpwnam(user)
    if (
        identity.uid != expected_id
        or identity.gid != expected_id
        or account.pw_gid != expected_id
        or account.pw_dir != "/nonexistent"
        or account.pw_shell != "/usr/sbin/nologin"
    ):
        raise HostConvergeError("sandbox batch identity metadata drifted")
    return identity


def _staging_allocation_node(config: StagingAllocationConfig) -> str:
    hostname = socket.gethostname().rstrip(".").lower()
    if hostname == config.source_host.lower():
        return config.source_host
    matches = [
        node
        for node, alias in config.host_aliases.items()
        if hostname in {node.lower(), alias.lower()}
    ]
    if len(matches) != 1:
        raise HostConvergeError(
            "staging allocation action requires its fixed source or GB10 node",
        )
    return matches[0]


def _staging_identity_snapshot(
    config: StagingAllocationConfig,
) -> dict[str, Any]:
    try:
        account = pwd.getpwnam(config.batch_user)
        primary = grp.getgrnam(config.batch_group)
    except KeyError as exc:
        raise HostConvergeError("staging allocation service identity is absent") from exc
    supplementary = sorted(
        row.gr_name
        for row in grp.getgrall()
        if row.gr_gid != account.pw_gid and config.batch_user in row.gr_mem
    )
    if (
        account.pw_uid != config.batch_uid
        or account.pw_gid != config.batch_gid
        or primary.gr_gid != config.batch_gid
        or account.pw_dir != str(config.batch_home)
        or account.pw_shell != str(config.batch_shell)
        or supplementary != sorted(config.batch_supplementary_groups)
    ):
        raise HostConvergeError("staging allocation service identity drifted")
    return {
        "username": config.batch_user,
        "group": config.batch_group,
        "uid": config.batch_uid,
        "gid": config.batch_gid,
        "home": str(config.batch_home),
        "shell": str(config.batch_shell),
        "supplementary_groups": list(config.batch_supplementary_groups),
    }


def _converge_staging_identity(
    config: StagingAllocationConfig,
) -> dict[str, Any]:
    if os.geteuid() != 0:
        raise HostConvergeError("staging allocation identity bootstrap requires root")
    try:
        existing_group = grp.getgrnam(config.batch_group)
    except KeyError:
        try:
            occupied = grp.getgrgid(config.batch_gid)
        except KeyError:
            _run(
                (
                    "groupadd",
                    "--system",
                    "--gid",
                    str(config.batch_gid),
                    config.batch_group,
                )
            )
        else:
            raise HostConvergeError(
                f"staging allocation GID is occupied by {occupied.gr_name}",
            ) from None
    else:
        if existing_group.gr_gid != config.batch_gid:
            raise HostConvergeError("staging allocation service GID is occupied")
    try:
        existing_user = pwd.getpwnam(config.batch_user)
    except KeyError:
        try:
            occupied_user = pwd.getpwuid(config.batch_uid)
        except KeyError:
            _run(
                (
                    "useradd",
                    "--system",
                    "--uid",
                    str(config.batch_uid),
                    "--gid",
                    config.batch_group,
                    "--home-dir",
                    str(config.batch_home),
                    "--shell",
                    str(config.batch_shell),
                    "--no-create-home",
                    config.batch_user,
                )
            )
        else:
            raise HostConvergeError(
                f"staging allocation UID is occupied by {occupied_user.pw_name}",
            ) from None
    else:
        if existing_user.pw_uid != config.batch_uid or existing_user.pw_gid != config.batch_gid:
            raise HostConvergeError("staging allocation service UID/GID is occupied")
        if existing_user.pw_dir != str(config.batch_home) or existing_user.pw_shell != str(
            config.batch_shell
        ):
            _run(
                (
                    "usermod",
                    "--home",
                    str(config.batch_home),
                    "--shell",
                    str(config.batch_shell),
                    config.batch_user,
                )
            )
    for group_name in config.batch_supplementary_groups:
        try:
            grp.getgrnam(group_name)
        except KeyError as exc:
            raise HostConvergeError(
                f"staging allocation required group is absent: {group_name}",
            ) from exc
    _run(
        (
            "usermod",
            "--groups",
            ",".join(config.batch_supplementary_groups),
            config.batch_user,
        )
    )
    return _staging_identity_snapshot(config)


def _converge_staging_shared_namespace(
    config: StagingAllocationConfig,
) -> dict[str, Any]:
    if os.geteuid() != 0:
        raise HostConvergeError("staging shared namespace bootstrap requires root")
    staging_root = config.shared_mount_target
    repository, worker_env = config.candidate_paths("a" * 40)
    if (
        repository.parent != staging_root / "candidates"
        or worker_env.parent != staging_root / "generated"
        or config.probe_result_root != staging_root / "results"
    ):
        raise HostConvergeError("staging shared namespace config drifted")
    mount = _run(
        (
            "findmnt",
            "-n",
            "-o",
            "SOURCE,FSTYPE,TARGET",
            "-T",
            str(staging_root),
        )
    ).stdout.split()
    if mount != [
        config.shared_mount_source,
        config.shared_mount_filesystem_type,
        str(staging_root),
    ]:
        raise HostConvergeError("staging shared mount identity drifted")
    path_contracts = (
        (staging_root, 0, config.batch_gid, 0o750),
        (repository.parent, 0, config.batch_gid, 0o750),
        (worker_env.parent, 0, config.batch_gid, 0o750),
        (config.probe_result_root, config.batch_uid, config.batch_gid, 0o2770),
    )
    for path, owner, group, mode in path_contracts:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            path.mkdir(mode=mode)
            os.chown(path, owner, group)
            os.chmod(path, mode)
            metadata = path.lstat()
        except OSError as exc:
            raise HostConvergeError("staging shared namespace is unavailable") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (owner, group)
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise HostConvergeError(
                f"staging shared namespace metadata drifted at {path}",
            )
    return {
        "root": str(staging_root),
        "mount_source": mount[0],
        "mount_fstype": mount[1],
        "mount_device": staging_root.lstat().st_dev,
        "mount_inode": staging_root.lstat().st_ino,
        "repository_root": str(repository.parent),
        "worker_env_root": str(worker_env.parent),
        "result_root": str(config.probe_result_root),
        "service_uid": config.batch_uid,
        "service_gid": config.batch_gid,
        "root_mode": "0o750",
        "repository_root_mode": "0o750",
        "worker_env_root_mode": "0o750",
        "result_root_mode": "0o2770",
    }


def staging_allocation_identity_converge(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    authority_generation: int,
    authority_convergence_id: str,
    authority_request_id: str,
    authority_requested_at: str,
) -> dict[str, Any]:
    node = _staging_allocation_node(config)
    identity = _converge_staging_identity(config)
    namespace = _converge_staging_shared_namespace(config)
    profile = _staging_slurm_profile(config, REPO_ROOT)
    try:
        guard = slurm_policy.reconcile_staging_guard_binding(
            Path("/"),
            profile,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            authority_generation=authority_generation,
            authority_convergence_id=authority_convergence_id,
            authority_request_id=authority_request_id,
            authority_requested_at=authority_requested_at,
        )
    except slurm_policy.PolicyError as exc:
        raise HostConvergeError("staging allocation guard convergence failed safely") from exc
    return {
        "schema_version": 1,
        "kind": "staging_external_slurm_identity_bootstrap",
        "node": node,
        "canonical_host": socket.gethostname().rstrip(".").lower(),
        "service_identity": identity,
        "namespace": namespace,
        "guard_binding": guard,
        "result": "pass",
    }


def _staging_timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _staging_candidate_binding(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> dict[str, Any]:
    repository, worker_env = config.candidate_paths(candidate_sha)
    expected_namespace = config.shared_mount_target
    if (
        repository.parent != expected_namespace / "candidates"
        or worker_env.parent != expected_namespace / "generated"
    ):
        raise HostConvergeError("staging allocation candidate paths drifted")
    _converge_staging_shared_namespace(config) if os.geteuid() == 0 else None
    identity = _staging_identity_snapshot(config)
    try:
        repository_metadata = repository.lstat()
        env_metadata = worker_env.lstat()
    except OSError as exc:
        raise HostConvergeError("staging allocation candidate inputs are unavailable") from exc
    if (
        not stat.S_ISDIR(repository_metadata.st_mode)
        or stat.S_ISLNK(repository_metadata.st_mode)
        or (repository_metadata.st_uid, repository_metadata.st_gid) != (0, config.batch_gid)
        or stat.S_IMODE(repository_metadata.st_mode) != 0o550
    ):
        raise HostConvergeError("staging allocation repository metadata drifted")
    if (
        not stat.S_ISREG(env_metadata.st_mode)
        or stat.S_ISLNK(env_metadata.st_mode)
        or env_metadata.st_nlink != 1
        or (env_metadata.st_uid, env_metadata.st_gid) != (config.batch_uid, config.batch_gid)
        or stat.S_IMODE(env_metadata.st_mode) != 0o600
        or not 0 < env_metadata.st_size <= 1024 * 1024
    ):
        raise HostConvergeError("staging allocation worker env metadata drifted")
    try:
        verified_repository = slurm_policy.verify_candidate_repository(
            repository,
            candidate_sha=candidate_sha,
        )
    except slurm_policy.PolicyError as exc:
        raise HostConvergeError("staging allocation candidate Git binding drifted") from exc
    if verified_repository["candidate_tree"] != candidate_tree:
        raise HostConvergeError("staging allocation candidate Git tree drifted")
    entry_count = 0
    for root, directories, files in os.walk(repository, followlinks=False):
        for entry in (
            Path(root),
            *(Path(root) / name for name in (*directories, *files)),
        ):
            entry_count += 1
            if entry_count > 250_000:
                raise HostConvergeError("staging allocation repository is too large")
            metadata = entry.lstat()
            if (metadata.st_uid, metadata.st_gid) != (0, config.batch_gid) or (
                not stat.S_ISLNK(metadata.st_mode) and metadata.st_mode & 0o022
            ):
                raise HostConvergeError(
                    "staging allocation repository ownership or mode drifted",
                )
    descriptor = os.open(
        worker_env,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (
            env_metadata.st_dev,
            env_metadata.st_ino,
        ):
            raise HostConvergeError("staging allocation worker env inode changed")
        chunks: list[bytes] = []
        while chunk := os.read(descriptor, 64 * 1024):
            chunks.append(chunk)
        after = os.fstat(descriptor)
        if (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise HostConvergeError("staging allocation worker env changed while read")
        raw_env = b"".join(chunks)
    finally:
        os.close(descriptor)
    try:
        values = external_slurm.parse_staging_worker_env(
            raw_env,
            candidate_sha=candidate_sha,
            pool=config.pool,
            concurrency=10,
        )
    except external_slurm.ExternalSlurmAcceptanceError as exc:
        raise HostConvergeError("staging allocation worker env binding drifted") from exc
    return {
        "service_identity": identity,
        "repository": {
            "path": str(repository),
            "device": repository_metadata.st_dev,
            "inode": repository_metadata.st_ino,
            "candidate_sha": candidate_sha,
            "candidate_tree": candidate_tree,
            "entry_count": entry_count,
        },
        "worker_env": {
            "path": str(worker_env),
            "device": env_metadata.st_dev,
            "inode": env_metadata.st_ino,
            "sha256": hashlib.sha256(raw_env).hexdigest(),
        },
        "env_values": values,
    }


def _staging_result_path(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    request_id: str,
    node: str,
) -> Path:
    if (
        SHA_RE.fullmatch(candidate_sha) is None
        or DIGEST_RE.fullmatch(request_id) is None
        or node not in config.allowed_nodes
    ):
        raise HostConvergeError("staging allocation result identity is invalid")
    return config.probe_result_root / request_id / f"{node}.json"


def _staging_worker_token(values: Mapping[str, str]) -> str:
    inline = values.get("LOOM_WORKER_TOKEN", "")
    token_file = values.get("LOOM_WORKER_TOKEN_FILE_HOST", "")
    if bool(inline) == bool(token_file):
        raise HostConvergeError("staging worker token source is ambiguous")
    if inline:
        return inline
    path = Path(token_file)
    try:
        metadata = path.lstat()
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise HostConvergeError("staging worker token file is unavailable") from exc
    if (
        not path.is_absolute()
        or stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or not token
    ):
        raise HostConvergeError("staging worker token file is unsafe")
    return token


def _staging_worker_heartbeat(
    *,
    control_plane_url: str,
    worker_id: str,
    token: str,
) -> None:
    if (
        re.fullmatch(
            r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}",
            worker_id,
        )
        is None
    ):
        raise HostConvergeError("staging worker registration ID is invalid")
    request = urllib.request.Request(
        f"{control_plane_url.rstrip('/')}/workers/{worker_id}/heartbeat",
        data=b"{}",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            body = response.read(4096)
            if response.status != 200 or json.loads(body) != {"status": "ok"}:
                raise HostConvergeError("staging worker heartbeat was rejected")
    except (OSError, urllib.error.HTTPError, json.JSONDecodeError) as exc:
        raise HostConvergeError("staging worker heartbeat failed safely") from exc


def _staging_guard_binding(
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> dict[str, Any]:
    try:
        binding = slurm_policy.load_staging_guard_binding(required=True)
    except slurm_policy.PolicyError as exc:
        raise HostConvergeError("staging allocation guard binding is unavailable") from exc
    assert binding is not None
    if binding["candidate_sha"] != candidate_sha or binding["candidate_tree"] != candidate_tree:
        raise HostConvergeError("staging allocation guard candidate binding drifted")
    return binding


def _staging_candidate_set_identity(
    *,
    candidate_sha: str,
    candidate_tree: str,
) -> tuple[dict[str, Any], str]:
    binding = _staging_guard_binding(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    try:
        candidate_set = slurm_policy.load_slurm_candidate_set()
    except slurm_policy.PolicyError as exc:
        raise HostConvergeError("staging allocation candidate set is unavailable") from exc
    guard_binding = candidate_set["candidate_bindings"].get("loom-staging")
    if (
        not isinstance(guard_binding, dict)
        or guard_binding
        != {
            "env_id": binding["env_id"],
            "resource_generation": binding["resource_generation"],
            "sandbox": binding["runtime_id"],
            "service_user": binding["service_user"],
            "slurm_qos": binding["slurm_qos"],
            "candidate_id": binding["candidate_id"],
            "candidate_sha": binding["candidate_sha"],
            "candidate_tree": binding["candidate_tree"],
        }
        or DIGEST_RE.fullmatch(str(candidate_set.get("candidate_set_sha256"))) is None
    ):
        raise HostConvergeError("staging allocation candidate-set binding drifted")
    return binding, str(candidate_set["candidate_set_sha256"])


def _staging_allocation_job_name(
    *,
    candidate_sha: str,
    node: str,
    candidate_set_sha256: str,
    resource_generation: int,
    request_id: str,
) -> str:
    if (
        SHA_RE.fullmatch(candidate_sha) is None
        or re.fullmatch(r"trt-gb10-(?:[1-9]|1[0-5])", node) is None
        or DIGEST_RE.fullmatch(candidate_set_sha256) is None
        or type(resource_generation) is not int
        or resource_generation < 1
        or DIGEST_RE.fullmatch(request_id) is None
    ):
        raise HostConvergeError("staging allocation job identity is invalid")
    identity = hashlib.sha256(
        (
            f"{candidate_sha}|{node}|{candidate_set_sha256}|{resource_generation}|{request_id}"
        ).encode("ascii"),
    ).hexdigest()
    return f"loom827-staging-{candidate_sha[:12]}-{node}-x{identity}"


def _staging_allocation_recover_job_id(
    config: StagingAllocationConfig,
    *,
    job_name: str,
    requested_node: str,
    request_id: str,
    prepared_at: str,
) -> str | None:
    expected_comment = f"loom-cgroup-v1:pids=65536:r={request_id}"
    observed: set[str] = set()
    queued = _run(
        (
            "squeue",
            f"--clusters={config.cluster}",
            "-h",
            f"--name={job_name}",
            f"--user={config.batch_user}",
            "-o",
            "%i|%j|%N|%a|%u|%q|%k",
        ),
    ).stdout
    for line in queued.splitlines():
        if not line.strip():
            continue
        row = line.split("|")
        if len(row) != 7:
            raise HostConvergeError("staging allocation recovery queue readback is invalid")
        job_id, name, nodelist, account, user, qos, comment = row
        if (
            re.fullmatch(r"[1-9][0-9]*", job_id) is None
            or name != job_name
            or nodelist not in {"", "(null)", "None", "N/A", requested_node}
            or account != config.slurm_account
            or user != config.batch_user
            or qos != config.qos
            or comment != expected_comment
        ):
            raise HostConvergeError("staging allocation recovery queue identity drifted")
        observed.add(job_id)
    accounting = _run(
        (
            "sacct",
            "-nP",
            f"--clusters={config.cluster}",
            f"--starttime={prepared_at}",
            f"--name={job_name}",
            "--format=JobIDRaw,JobName,NodeList,Account,User,Cluster,QOS,Comment",
        ),
    ).stdout
    for line in accounting.splitlines():
        if not line.strip():
            continue
        row = line.split("|")
        if len(row) != 8:
            raise HostConvergeError("staging allocation recovery accounting readback is invalid")
        job_id, name, nodelist, account, user, cluster, qos, comment = row
        if "." in job_id:
            continue
        if (
            re.fullmatch(r"[1-9][0-9]*", job_id) is None
            or name != job_name
            or nodelist not in {"", "(null)", "None", "N/A", requested_node}
            or account != config.slurm_account
            or user != config.batch_user
            or cluster != config.cluster
            or qos != config.qos
            or comment != expected_comment
        ):
            raise HostConvergeError("staging allocation recovery accounting identity drifted")
        observed.add(job_id)
    if len(observed) > 1:
        raise HostConvergeError("staging allocation recovery found duplicate exact jobs")
    return next(iter(observed), None)


def _staging_submission_wal_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _staging_submission_wal(
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
    requested_node: str,
    candidate_set_sha256: str,
    resource_generation: int,
    job_name: str,
    config: StagingAllocationConfig,
    wrapped: str,
) -> tuple[Path, dict[str, Any], bool]:
    _ensure_root_private_directory(STAGING_SUBMISSION_WAL_ROOT)
    path = STAGING_SUBMISSION_WAL_ROOT / f"{request_id}.json"
    existed = path.exists() or path.is_symlink()
    if existed:
        raw = _read_combined_receipt_bytes(path)
        try:
            wal = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HostConvergeError("staging submission WAL is invalid") from exc
        unsigned = (
            {key: value for key, value in wal.items() if key != "payload_sha256"}
            if isinstance(wal, dict)
            else {}
        )
        if (
            not isinstance(wal, dict)
            or raw != _staging_submission_wal_bytes(wal)
            or wal.get("schema_version") != 1
            or wal.get("kind") != "staging_external_slurm_submission_wal"
            or wal.get("candidate_sha") != candidate_sha
            or wal.get("candidate_tree") != candidate_tree
            or wal.get("request_id") != request_id
            or wal.get("requested_node") != requested_node
            or wal.get("candidate_set_sha256") != candidate_set_sha256
            or wal.get("resource_generation") != resource_generation
            or wal.get("job_name") != job_name
            or wal.get("cluster") != config.cluster
            or wal.get("partition") != config.partition
            or wal.get("account") != config.slurm_account
            or wal.get("qos") != config.qos
            or wal.get("user") != config.batch_user
            or wal.get("wrapped") != wrapped
            or wal.get("phase") not in {"prepared", "submitted"}
            or not isinstance(wal.get("prepared_at"), str)
            or hashlib.sha256(_staging_submission_wal_bytes(unsigned)).hexdigest()
            != wal.get("payload_sha256")
            or (
                wal["phase"] == "prepared"
                and (wal.get("result") is not None or wal.get("result_sha256") is not None)
            )
            or (
                wal["phase"] == "submitted"
                and (
                    not isinstance(wal.get("result"), dict)
                    or DIGEST_RE.fullmatch(str(wal.get("result_sha256"))) is None
                    or hashlib.sha256(_staging_submission_wal_bytes(wal["result"])).hexdigest()
                    != wal["result_sha256"]
                )
            )
        ):
            raise HostConvergeError("staging submission WAL identity drifted")
        return path, wal, True
    prepared_at = _staging_timestamp()
    wal = {
        "schema_version": 1,
        "kind": "staging_external_slurm_submission_wal",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "request_id": request_id,
        "requested_node": requested_node,
        "candidate_set_sha256": candidate_set_sha256,
        "resource_generation": resource_generation,
        "job_name": job_name,
        "cluster": config.cluster,
        "partition": config.partition,
        "account": config.slurm_account,
        "qos": config.qos,
        "user": config.batch_user,
        "wrapped": wrapped,
        "phase": "prepared",
        "prepared_at": prepared_at,
        "result": None,
        "result_sha256": None,
    }
    wal["payload_sha256"] = hashlib.sha256(_staging_submission_wal_bytes(wal)).hexdigest()
    _atomic_write(path, _staging_submission_wal_bytes(wal), mode=0o600)
    return path, wal, False


@contextmanager
def _staging_submission_lock(request_id: str) -> Iterator[None]:
    if DIGEST_RE.fullmatch(request_id) is None:
        raise HostConvergeError("staging submission lock identity is invalid")
    _ensure_root_private_directory(STAGING_SUBMISSION_WAL_ROOT)
    descriptor = os.open(
        STAGING_SUBMISSION_WAL_ROOT / f".{request_id}.lock",
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or (metadata.st_uid, metadata.st_gid) != (0, 0):
            raise HostConvergeError("staging submission lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _staging_slurm_profile(
    config: StagingAllocationConfig,
    repository: Path,
) -> slurm_policy.Profile:
    try:
        profile = slurm_policy.load_profile(
            repository / "deploy/slurm/developer-sandboxes/gb10.toml",
        )
    except slurm_policy.PolicyError as exc:
        raise HostConvergeError("staging allocation Slurm profile is invalid") from exc
    if (
        profile.cluster != config.cluster
        or profile.controller != config.controller
        or profile.submit_host != config.submit_host
        or profile.allowed_nodes != config.allowed_nodes
        or profile.infrastructure_nodes != config.infrastructure_nodes
        or profile.host_aliases
        != {node: host.lower() for node, host in config.host_aliases.items()}
        or profile.job_pids_max != 65536
        or profile.docker_cgroup_driver not in {"cgroupfs", "systemd"}
    ):
        raise HostConvergeError("staging allocation Slurm profile binding drifted")
    return profile


def _staging_allocation_job_start_time(
    config: StagingAllocationConfig,
    *,
    job_id: str,
    node: str,
) -> str:
    output = _run(("scontrol", "show", "job", "--oneliner", job_id)).stdout
    fields = {
        name: re.findall(rf"(?:^|\s){name}=(\S+)", output)
        for name in ("JobId", "Account", "UserId", "NodeList", "StartTime")
    }
    expected_user_prefix = f"{config.batch_user}("
    if (
        any(len(values) != 1 for values in fields.values())
        or fields["JobId"][0] != job_id
        or fields["Account"][0] != config.slurm_account
        or not fields["UserId"][0].startswith(expected_user_prefix)
        or fields["NodeList"][0].lower() != node.lower()
        or fields["StartTime"][0] in {"", "Unknown", "None", "(null)"}
        or any(character.isspace() for character in fields["StartTime"][0])
    ):
        raise HostConvergeError("staging allocation Slurm start identity drifted")
    return str(fields["StartTime"][0])


def _staging_cgroup_command(
    config: StagingAllocationConfig,
    profile: slurm_policy.Profile,
    binding: Mapping[str, Any],
    *,
    cgroup_program: Path,
    job_id: str,
    node: str,
    job_start_time: str,
) -> tuple[str, ...]:
    return (
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
        "--docker-driver",
        profile.docker_cgroup_driver,
        "--cluster",
        config.cluster,
        "--node",
        node,
        "--job-start-time",
        job_start_time,
        "--account",
        config.slurm_account,
        "--env-id",
        str(binding["env_id"]),
        "--resource-generation",
        str(binding["resource_generation"]),
        "--runtime-id",
        str(binding["runtime_id"]),
        "--candidate-id",
        str(binding["candidate_id"]),
        "--candidate-sha",
        str(binding["candidate_sha"]),
        "--candidate-tree",
        str(binding["candidate_tree"]),
    )


def _staging_slice_identity(
    config: StagingAllocationConfig,
    binding: Mapping[str, Any],
    *,
    job_id: str,
    node: str,
    job_start_time: str,
) -> tuple[str, str]:
    identity = {
        "cluster": config.cluster,
        "node": node.lower(),
        "job_id": job_id,
        "job_start_time": job_start_time,
        "account": config.slurm_account,
        "env_id": binding["env_id"],
        "resource_generation": binding["resource_generation"],
        "runtime_id": binding["runtime_id"],
        "candidate_id": binding["candidate_id"],
        "candidate_sha": binding["candidate_sha"],
        "candidate_tree": binding["candidate_tree"],
    }
    digest = hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("ascii"),
    ).hexdigest()
    return f"loom-job-{job_id}-{digest[:40]}.slice", digest


def _staging_systemd_slice_receipt(
    config: StagingAllocationConfig,
    binding: Mapping[str, Any],
    *,
    job_id: str,
    node: str,
    job_start_time: str,
    cgroup_parent: str,
    receipt_root: Path = SYSTEMD_SLICE_RECEIPT_ROOT,
    unit_root: Path = SYSTEMD_UNIT_ROOT,
    expected_authority_uid: int = 0,
    expected_authority_gid: int = 0,
) -> dict[str, Any]:
    expected_unit, identity_sha256 = _staging_slice_identity(
        config,
        binding,
        job_id=job_id,
        node=node,
        job_start_time=job_start_time,
    )
    if cgroup_parent != expected_unit:
        raise HostConvergeError("staging allocation systemd slice identity drifted")
    receipt_path = receipt_root / f"{cgroup_parent}.json"
    unit_path = unit_root / cgroup_parent
    try:
        receipt_metadata = receipt_path.lstat()
        unit_metadata = unit_path.lstat()
        raw = receipt_path.read_bytes()
        unit = unit_path.read_bytes()
        receipt = json.loads(raw)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HostConvergeError("staging allocation systemd receipt is unavailable") from exc
    fields = {
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
    unsigned = (
        {key: value for key, value in receipt.items() if key != "payload_sha256"}
        if isinstance(receipt, dict)
        else {}
    )
    if (
        not isinstance(receipt, dict)
        or set(receipt) != fields
        or raw != json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        or not stat.S_ISREG(receipt_metadata.st_mode)
        or stat.S_ISLNK(receipt_metadata.st_mode)
        or receipt_metadata.st_nlink != 1
        or (receipt_metadata.st_uid, receipt_metadata.st_gid)
        != (expected_authority_uid, expected_authority_gid)
        or stat.S_IMODE(receipt_metadata.st_mode) != 0o444
        or not stat.S_ISREG(unit_metadata.st_mode)
        or stat.S_ISLNK(unit_metadata.st_mode)
        or unit_metadata.st_nlink != 1
        or (unit_metadata.st_uid, unit_metadata.st_gid)
        != (expected_authority_uid, expected_authority_gid)
        or stat.S_IMODE(unit_metadata.st_mode) != 0o644
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != "loom.slurm-systemd-slice-receipt"
        or receipt.get("systemd_slice") != cgroup_parent
        or receipt.get("slice_identity_sha256") != identity_sha256
        or receipt.get("unit_sha256") != hashlib.sha256(unit).hexdigest()
        or receipt.get("job_id") != job_id
        or receipt.get("job_start_time") != job_start_time
        or receipt.get("cluster") != config.cluster
        or receipt.get("node_list") != node.lower()
        or receipt.get("account") != config.slurm_account
        or receipt.get("env_id") != binding["env_id"]
        or receipt.get("resource_generation") != binding["resource_generation"]
        or receipt.get("runtime_id") != binding["runtime_id"]
        or receipt.get("candidate_id") != binding["candidate_id"]
        or receipt.get("candidate_sha") != binding["candidate_sha"]
        or receipt.get("candidate_tree") != binding["candidate_tree"]
        or receipt.get("pids_max") != "65536"
        or receipt.get("payload_sha256")
        != hashlib.sha256(
            json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii"),
        ).hexdigest()
    ):
        raise HostConvergeError("staging allocation systemd receipt binding drifted")
    return receipt


def _cpuset_values(value: str) -> set[int]:
    result: set[int] = set()
    for item in value.split(","):
        start, separator, end = item.partition("-")
        if not start.isdecimal() or (separator and not end.isdecimal()):
            raise HostConvergeError("staging allocation systemd cpuset is invalid")
        lower = int(start)
        upper = int(end) if separator else lower
        if lower > upper or upper > 1_000_000:
            raise HostConvergeError("staging allocation systemd cpuset is invalid")
        result.update(range(lower, upper + 1))
    if not result:
        raise HostConvergeError("staging allocation systemd cpuset is empty")
    return result


def _staging_systemd_container_containment(
    compose: Sequence[str],
    *,
    environment: Mapping[str, str],
    repository: Path,
    service: str,
    systemd_slice: str,
    receipt: Mapping[str, Any],
    proc_root: Path = Path("/proc"),
    cgroup_root: Path = Path("/sys/fs/cgroup"),
) -> dict[str, str]:
    identity = subprocess.run(
        (*compose, "ps", "--quiet", service),
        cwd=repository,
        env=dict(environment),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    container_id = identity.stdout.strip()
    inspected = subprocess.run(
        ("docker", "inspect", "--format", "{{.State.Pid}}", container_id),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    control_group = subprocess.run(
        (
            "systemctl",
            "show",
            "--property=ControlGroup",
            "--value",
            systemd_slice,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    pid = inspected.stdout.strip()
    live_slice = control_group.stdout.strip()
    slice_match = re.fullmatch(
        r"loom-job-([1-9][0-9]*)-[0-9a-f]{40}\.slice",
        systemd_slice,
    )
    expected_live_slice = (
        f"/loom.slice/loom-job.slice/loom-job-{slice_match.group(1)}.slice/{systemd_slice}"
        if slice_match is not None
        else ""
    )
    if (
        identity.returncode
        or re.fullmatch(r"[0-9a-f]{12,64}", container_id) is None
        or inspected.returncode
        or not pid.isdecimal()
        or int(pid) <= 1
        or control_group.returncode
        or not live_slice.startswith("/")
        or live_slice == "/"
        or "\x00" in live_slice
        or ".." in live_slice.split("/")
        or live_slice != expected_live_slice
    ):
        raise HostConvergeError("staging allocation container identity readback failed")
    try:
        proc_cgroup = (proc_root / pid / "cgroup").read_text(encoding="utf-8")
    except OSError as exc:
        raise HostConvergeError("staging allocation container cgroup is unavailable") from exc
    unified = [row.partition("::")[2] for row in proc_cgroup.splitlines() if row.startswith("0::")]
    if len(unified) != 1:
        raise HostConvergeError("staging allocation container cgroup is ambiguous")
    observed = Path(unified[0])
    live_slice_path = Path(live_slice)
    if (
        not observed.is_absolute()
        or observed == live_slice_path
        or live_slice_path not in observed.parents
    ):
        raise HostConvergeError("staging allocation container is not a strict slice descendant")
    slice_path = cgroup_root / live_slice_path.relative_to("/")
    try:
        actual_cpu = (slice_path / "cpu.max").read_text(encoding="utf-8").strip().split()
        source_cpu = str(receipt["cpu_max"]).split()
        actual_memory = (slice_path / "memory.max").read_text(encoding="utf-8").strip()
        actual_swap = (slice_path / "memory.swap.max").read_text(encoding="utf-8").strip()
        actual_pids = (slice_path / "pids.max").read_text(encoding="utf-8").strip()
        actual_cpus = (slice_path / "cpuset.cpus.effective").read_text(encoding="utf-8").strip()
        actual_mems = (slice_path / "cpuset.mems.effective").read_text(encoding="utf-8").strip()
    except (OSError, KeyError) as exc:
        raise HostConvergeError("staging allocation systemd live limits are unavailable") from exc
    if (
        len(actual_cpu) != 2
        or len(source_cpu) != 2
        or not all(value.isdecimal() for value in (*actual_cpu, *source_cpu))
        or int(actual_cpu[0]) * int(source_cpu[1]) > int(source_cpu[0]) * int(actual_cpu[1])
        or not actual_memory.isdecimal()
        or not str(receipt["memory_max"]).isdecimal()
        or int(actual_memory) > int(receipt["memory_max"])
        or not actual_swap.isdecimal()
        or not str(receipt["memory_swap_max_effective"]).isdecimal()
        or int(actual_swap) > int(receipt["memory_swap_max_effective"])
        or not actual_pids.isdecimal()
        or not str(receipt["pids_max"]).isdecimal()
        or int(actual_pids) > int(receipt["pids_max"])
        or not _cpuset_values(actual_cpus).issubset(
            _cpuset_values(str(receipt["cpuset_cpus"])),
        )
        or not _cpuset_values(actual_mems).issubset(
            _cpuset_values(str(receipt["cpuset_mems"])),
        )
    ):
        raise HostConvergeError("staging allocation systemd live limits are weaker than Slurm")
    return {
        "container_id": container_id,
        "observed_cgroup": unified[0],
        "cpu_max": " ".join(actual_cpu),
        "memory_max": actual_memory,
        "memory_swap_max": actual_swap,
        "pids_max": actual_pids,
        "cpuset_cpus": actual_cpus,
        "cpuset_mems": actual_mems,
    }


def staging_allocation_node_check(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
    steady: bool = False,
) -> dict[str, Any]:
    if (
        SHA_RE.fullmatch(candidate_sha) is None
        or SHA_RE.fullmatch(candidate_tree) is None
        or DIGEST_RE.fullmatch(request_id) is None
    ):
        raise HostConvergeError("staging allocation node request is invalid")
    node = _staging_allocation_node(config)
    if node not in config.allowed_nodes:
        raise HostConvergeError("staging allocation node check requires a GB10 node")
    if (
        os.geteuid() != config.batch_uid
        or os.getegid() != config.batch_gid
        or os.environ.get("SLURM_JOB_ID", "").isdigit() is False
        or os.environ.get("SLURM_JOB_NODELIST", "").lower() != node.lower()
    ):
        raise HostConvergeError("staging allocation numeric/Slurm identity drifted")
    binding = _staging_candidate_binding(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    repository = Path(binding["repository"]["path"])
    profile = _staging_slurm_profile(config, repository)
    guard_binding, candidate_set_sha256 = _staging_candidate_set_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    job_id = os.environ["SLURM_JOB_ID"]
    worker_env = Path(binding["worker_env"]["path"])
    worker_image = slurm_policy._inspect_worker_image(
        str(binding["env_values"]["LOOM_WORKER_IMAGE_ID"]),
        candidate_sha=candidate_sha,
        domain="gb10",
    )
    job_start_time = _staging_allocation_job_start_time(
        config,
        job_id=job_id,
        node=node,
    )
    compose_project = f"loom-staging-{request_id[:12]}-{node.replace('trt-', '')}"
    base_compose = repository / "deploy/docker-compose.remote-worker.yml"
    cgroup_compose = repository / "deploy/docker-compose.remote-worker.cgroup-parent.yml"
    if not base_compose.is_file() or not cgroup_compose.is_file():
        raise HostConvergeError("staging worker Compose assets are unavailable")
    cgroup_program = repository / "src/loom_control_plane/slurm_job_cgroup.py"
    docker_driver = _run(
        ("docker", "info", "--format", "{{.CgroupDriver}}"),
    ).stdout.strip()
    if docker_driver != profile.docker_cgroup_driver:
        raise HostConvergeError("staging allocation Docker cgroup driver drifted")
    cgroup = subprocess.run(
        _staging_cgroup_command(
            config,
            profile,
            guard_binding,
            cgroup_program=cgroup_program,
            job_id=job_id,
            node=node,
            job_start_time=job_start_time,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=45,
    )
    cgroup_parent = cgroup.stdout.strip()
    if (
        cgroup.returncode != 0
        or (profile.docker_cgroup_driver == "cgroupfs" and not cgroup_parent.startswith("/"))
        or (
            profile.docker_cgroup_driver == "systemd"
            and SYSTEMD_SLICE_RE.fullmatch(cgroup_parent) is None
        )
    ):
        raise HostConvergeError("staging allocation cgroup guard failed")
    slice_receipt = (
        _staging_systemd_slice_receipt(
            config,
            guard_binding,
            job_id=job_id,
            node=node,
            job_start_time=job_start_time,
            cgroup_parent=cgroup_parent,
        )
        if profile.docker_cgroup_driver == "systemd"
        else None
    )
    compose_environment = {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "LOOM_WORKER_CGROUP_PARENT": cgroup_parent,
        "LOOM_WORKER_REQUIRE_CGROUP_PARENT": "true",
        "LOOM_WORKER_SLURM_JOB_ID": job_id,
        "LOOM_WORKER_COMPOSE_PROJECT": compose_project,
        "LOOM_WORKER_RESTART_POLICY": "no",
        "LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS": "0" if steady else "60",
        "LOOM_WORKER_HOSTNAME": node,
        "LOOM_WORKER_CANDIDATE_SHA": candidate_sha,
        "LOOM_WORKER_CONTAINER_CPUS": "2",
        "LOOM_WORKER_CONTAINER_MEMORY_MIB": "11500",
        "LOOM_WORKER_CONTAINER_PIDS": "4096",
        "COMPOSE_PROJECT_NAME": compose_project,
    }
    compose = (
        "docker",
        "compose",
        "--project-name",
        compose_project,
        "--env-file",
        str(worker_env),
        "-f",
        str(base_compose),
        "-f",
        str(cgroup_compose),
    )
    docker_version = _run(
        ("docker", "info", "--format", "{{.ServerVersion}}"),
        env=compose_environment,
    ).stdout.strip()
    config_result = subprocess.run(
        (*compose, "config", "--format", "json"),
        cwd=repository,
        env=compose_environment,
        check=False,
        capture_output=True,
        timeout=60,
    )
    if config_result.returncode != 0 or len(config_result.stdout) > 4 * 1024 * 1024:
        raise HostConvergeError("staging worker Compose config failed safely")
    try:
        rendered = json.loads(config_result.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HostConvergeError("staging worker Compose config is invalid") from exc
    services = rendered.get("services") if isinstance(rendered, dict) else None
    if (
        not isinstance(services, dict)
        or not isinstance(services.get("worker"), dict)
        or services["worker"].get("cgroup_parent") != cgroup_parent
        or services["worker"].get("image") != binding["env_values"]["LOOM_WORKER_IMAGE_ID"]
    ):
        raise HostConvergeError("staging worker Compose cgroup binding drifted")
    compose_config_sha256 = hashlib.sha256(config_result.stdout).hexdigest()
    registered_at = ""
    first_heartbeat_at = ""
    last_heartbeat_at = ""
    cancel_requested_at = ""
    worker_id = ""
    stopped_at = ""
    cleanup_verified = False
    container_cgroup: dict[str, str] | None = None
    try:
        up = subprocess.run(
            (*compose, "up", "--detach", "--no-build", "worker"),
            cwd=repository,
            env=compose_environment,
            check=False,
            capture_output=True,
            timeout=90,
        )
        if up.returncode != 0:
            raise HostConvergeError("staging worker failed to start")
        try:
            slurm_policy._verify_compose_service_image(
                compose,
                environment=compose_environment,
                source_root=repository,
                service="worker",
                worker_image_id=str(binding["env_values"]["LOOM_WORKER_IMAGE_ID"]),
            )
        except slurm_policy.PolicyError as exc:
            raise HostConvergeError("staging worker image readback drifted") from exc
        if profile.docker_cgroup_driver == "systemd":
            assert slice_receipt is not None
            container_cgroup = _staging_systemd_container_containment(
                compose,
                environment=compose_environment,
                repository=repository,
                service="worker",
                systemd_slice=cgroup_parent,
                receipt=slice_receipt,
            )
        deadline = time.monotonic() + 90
        registration = re.compile(
            rb"worker_registered worker_id=([0-9a-f-]{36})",
        )
        while time.monotonic() < deadline:
            logs = subprocess.run(
                (*compose, "logs", "--no-color", "--tail", "200", "worker"),
                cwd=repository,
                env=compose_environment,
                check=False,
                capture_output=True,
                timeout=15,
            )
            if logs.returncode == 0 and (match := registration.search(logs.stdout)):
                worker_id = match.group(1).decode("ascii")
                registered_at = _staging_timestamp()
                break
            time.sleep(1)
        if not worker_id:
            raise HostConvergeError("staging worker did not register within the bound")
        values = binding["env_values"]
        token = _staging_worker_token(values)
        control_plane_url = values.get("LOOM_WORKER_CONTROL_PLANE_URL", "")
        if not control_plane_url:
            raise HostConvergeError("staging worker control-plane URL is unavailable")
        _staging_worker_heartbeat(
            control_plane_url=control_plane_url,
            worker_id=worker_id,
            token=token,
        )
        first_heartbeat_at = _staging_timestamp()
        time.sleep(config.heartbeat_interval_seconds)
        _staging_worker_heartbeat(
            control_plane_url=control_plane_url,
            worker_id=worker_id,
            token=token,
        )
        last_heartbeat_at = _staging_timestamp()
        if steady:
            stop_requested = False

            def request_stop(_signum: int, _frame: object) -> None:
                nonlocal stop_requested
                stop_requested = True

            previous_term = signal.signal(signal.SIGTERM, request_stop)
            previous_int = signal.signal(signal.SIGINT, request_stop)
            try:
                while not stop_requested:
                    time.sleep(1)
            finally:
                signal.signal(signal.SIGTERM, previous_term)
                signal.signal(signal.SIGINT, previous_int)
        cancel_requested_at = _staging_timestamp()
    finally:
        down = subprocess.run(
            (*compose, "down", "--remove-orphans", "--volumes", "--timeout", "15"),
            cwd=repository,
            env=compose_environment,
            check=False,
            capture_output=True,
            timeout=60,
        )
        stopped_at = _staging_timestamp()
        containers = _run(
            (
                "docker",
                "ps",
                "-aq",
                "--filter",
                f"label=com.docker.compose.project={compose_project}",
            ),
            env=compose_environment,
        ).stdout.splitlines()
        networks = _run(
            (
                "docker",
                "network",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={compose_project}",
            ),
            env=compose_environment,
        ).stdout.splitlines()
        volumes = _run(
            (
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={compose_project}",
            ),
            env=compose_environment,
        ).stdout.splitlines()
        cleanup_verified = down.returncode == 0 and not containers and not networks and not volumes
    if (
        not all(
            (
                registered_at,
                first_heartbeat_at,
                last_heartbeat_at,
                cancel_requested_at,
                stopped_at,
            )
        )
        or not cleanup_verified
    ):
        raise HostConvergeError("staging worker lifecycle did not close cleanly")
    result = {
        "schema_version": 1,
        "kind": (
            "staging_external_slurm_worker_terminal"
            if steady
            else "staging_external_slurm_node_probe"
        ),
        "request_id": request_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "node": node,
        "canonical_host": socket.gethostname().rstrip(".").lower(),
        "service_identity": binding["service_identity"],
        "repository": binding["repository"],
        "worker_env": {key: value for key, value in binding["worker_env"].items()},
        "job_id": job_id,
        "compose_project": compose_project,
        "worker_id": worker_id,
        "registered_at": registered_at,
        "first_heartbeat_at": first_heartbeat_at,
        "last_heartbeat_at": last_heartbeat_at,
        "heartbeat_count": 2,
        "cancel_requested_at": cancel_requested_at,
        "stopped_at": stopped_at,
        "docker_server_version": docker_version,
        "worker_image": worker_image,
        "docker_cgroup_driver": docker_driver,
        "job_start_time": job_start_time,
        "cgroup_parent": cgroup_parent,
        "cgroup_mode": (
            "direct-slurm-cgroup"
            if profile.docker_cgroup_driver == "cgroupfs"
            else "allocation-mirrored-systemd-slice"
        ),
        "candidate_set_sha256": candidate_set_sha256,
        "guard_binding_payload_sha256": guard_binding["payload_sha256"],
        "systemd_slice_receipt": slice_receipt,
        "container_cgroup": container_cgroup,
        "compose_config_sha256": compose_config_sha256,
        "orphan_containers": 0,
        "orphan_networks": 0,
        "orphan_volumes": 0,
        "cleanup_verified": True,
        "result": "pass",
    }
    result_path = _staging_result_path(
        config,
        candidate_sha=candidate_sha,
        request_id=request_id,
        node=node,
    )
    try:
        parent_metadata = result_path.parent.lstat()
    except OSError as exc:
        raise HostConvergeError("staging result request directory is unavailable") from exc
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or stat.S_ISLNK(parent_metadata.st_mode)
        or (parent_metadata.st_uid, parent_metadata.st_gid) != (config.batch_uid, config.batch_gid)
        or stat.S_IMODE(parent_metadata.st_mode) != 0o700
    ):
        raise HostConvergeError("staging result request directory is unsafe")
    _atomic_write(
        result_path,
        (
            json.dumps(
                result,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("ascii"),
        mode=0o600,
        identity=Identity(
            config.batch_user,
            config.batch_group,
            config.batch_uid,
            config.batch_gid,
        ),
    )
    return result


def _staging_prepare_result_directory(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    request_id: str,
    allow_existing: bool = False,
) -> Path:
    if SHA_RE.fullmatch(candidate_sha) is None or DIGEST_RE.fullmatch(request_id) is None:
        raise HostConvergeError("staging allocation result identity is invalid")
    request_root = config.probe_result_root / request_id
    for path, must_create in ((request_root, True),):
        try:
            path.mkdir(mode=0o700)
            os.chown(path, config.batch_uid, config.batch_gid)
            os.chmod(path, 0o700)
        except FileExistsError:
            if must_create and not allow_existing:
                raise HostConvergeError(
                    "staging allocation request ID was already used",
                ) from None
        except OSError as exc:
            raise HostConvergeError(
                "staging allocation result directory cannot be prepared",
            ) from exc
        metadata = path.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (config.batch_uid, config.batch_gid)
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise HostConvergeError("staging allocation result directory is unsafe")
    return request_root


def _staging_slurm_live_config(
    config: StagingAllocationConfig,
) -> dict[str, str]:
    raw = _run(("scontrol", "show", "config")).stdout
    values: dict[str, str] = {}
    for line in raw.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            values[key.strip()] = value.strip()
    controllers = {
        item.strip().split("(", 1)[0].lower()
        for item in values.get("SlurmctldHost", "").split(",")
        if item.strip()
    }
    if values.get("ClusterName") != config.cluster or config.controller.lower() not in controllers:
        raise HostConvergeError("staging allocation reached the wrong Slurm authority")
    assoc = _run(
        (
            "sacctmgr",
            "-nP",
            "show",
            "assoc",
            f"where cluster={config.cluster}",
            f"user={config.batch_user}",
            f"account={config.slurm_account}",
            "format=Cluster,Account,User,QOS",
        )
    ).stdout
    rows = [line.split("|") for line in assoc.splitlines() if line.strip()]
    if len(rows) != 1 or len(rows[0]) != 4:
        raise HostConvergeError("staging Slurm account association is unavailable")
    cluster, account, user, qos_values = rows[0]
    if (
        cluster != config.cluster
        or account != config.slurm_account
        or user != config.batch_user
        or config.qos not in {item for item in qos_values.split(",") if item}
    ):
        raise HostConvergeError("staging Slurm account/QoS association drifted")
    return {
        "cluster": cluster,
        "controller": config.controller,
        "account": account,
        "user": user,
        "qos": config.qos,
    }


def _staging_sacct_rows(
    config: StagingAllocationConfig,
    job_id: str,
) -> list[list[str]]:
    output = _run(
        (
            "sacct",
            "-nP",
            f"--clusters={config.cluster}",
            "--starttime=now-1hour",
            f"--jobs={job_id}",
            "--format=JobIDRaw,JobName,State,NodeList,Account,User,Cluster,QOS,Start,End",
        )
    ).stdout
    return [line.split("|") for line in output.splitlines() if line.strip()]


def _staging_terminal_state(value: str) -> str:
    return value.split("+", 1)[0].split(" ", 1)[0].strip().upper()


def staging_allocation_query(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
    job_ids: tuple[str, ...],
    nodes: tuple[str, ...],
) -> dict[str, Any]:
    if (
        os.geteuid() != 0
        or SHA_RE.fullmatch(candidate_sha) is None
        or SHA_RE.fullmatch(candidate_tree) is None
        or DIGEST_RE.fullmatch(request_id) is None
        or (not job_ids and not nodes)
        or len(job_ids) > 64
        or len(nodes) > len(config.allowed_nodes)
        or len(job_ids) != len(set(job_ids))
        or len(nodes) != len(set(nodes))
        or any(re.fullmatch(r"[1-9][0-9]*", job_id) is None for job_id in job_ids)
        or any(node not in config.allowed_nodes for node in nodes)
        or _staging_allocation_node(config) != config.submit_host
        or config.cluster != "trt-gb10"
        or config.submit_host != "trt-gb10-1"
        or config.controller != "trt-gb10-1"
    ):
        raise HostConvergeError("staging allocation query request is invalid")
    _guard_binding, _candidate_set_sha256 = _staging_candidate_set_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    expected_name = re.compile(
        rf"loom827-staging-{candidate_sha[:12]}-"
        r"trt-gb10-(?:[1-9]|1[0-5])-x[0-9a-f]{64}\Z",
    )
    observed_at = _staging_timestamp()
    jobs: dict[str, dict[str, Any]] = {}
    if job_ids:
        queued = _run(
            (
                "squeue",
                f"--clusters={config.cluster}",
                "-h",
                "-j",
                ",".join(job_ids),
                "-o",
                "%i|%j|%T|%N|%a|%u|%R|%q",
            ),
        ).stdout
        for line in queued.splitlines():
            if not line.strip():
                continue
            row = line.split("|")
            if len(row) != 8 or row[0] not in job_ids or row[0] in jobs:
                raise HostConvergeError("staging allocation query queue readback is invalid")
            job_id, name, state, nodelist, account, user, reason, qos = row
            if (
                expected_name.fullmatch(name) is None
                or account != config.slurm_account
                or user != config.batch_user
                or qos != config.qos
                or re.fullmatch(r"[A-Z_]+", _staging_terminal_state(state)) is None
                or (
                    nodelist not in {"", "(null)", "None", "N/A"}
                    and nodelist not in config.allowed_nodes
                )
            ):
                raise HostConvergeError("staging allocation query queue identity drifted")
            jobs[job_id] = {
                "job_id": job_id,
                "job_name": name,
                "state": _staging_terminal_state(state),
                "nodelist": ("" if nodelist in {"", "(null)", "None", "N/A"} else nodelist),
                "account": account,
                "user": user,
                "cluster": config.cluster,
                "qos": qos,
                "pending_reason": (None if reason in {"", "(null)", "None", "N/A"} else reason),
            }
        missing = tuple(job_id for job_id in job_ids if job_id not in jobs)
        if missing:
            rows = _staging_sacct_rows(config, ",".join(missing))
            for row in rows:
                if len(row) != 10 or row[0] not in missing or row[0] in jobs:
                    if len(row) == 10 and "." in row[0]:
                        continue
                    raise HostConvergeError(
                        "staging allocation query accounting readback is invalid",
                    )
                (
                    job_id,
                    name,
                    state,
                    nodelist,
                    account,
                    user,
                    cluster,
                    qos,
                    _start,
                    _end,
                ) = row
                if (
                    expected_name.fullmatch(name) is None
                    or account != config.slurm_account
                    or user != config.batch_user
                    or cluster != config.cluster
                    or qos != config.qos
                    or re.fullmatch(r"[A-Z_]+", _staging_terminal_state(state)) is None
                    or (
                        nodelist not in {"", "(null)", "None", "N/A"}
                        and nodelist not in config.allowed_nodes
                    )
                ):
                    raise HostConvergeError(
                        "staging allocation query accounting identity drifted",
                    )
                jobs[job_id] = {
                    "job_id": job_id,
                    "job_name": name,
                    "state": _staging_terminal_state(state),
                    "nodelist": ("" if nodelist in {"", "(null)", "None", "N/A"} else nodelist),
                    "account": account,
                    "user": user,
                    "cluster": cluster,
                    "qos": qos,
                    "pending_reason": None,
                }
    resources: dict[str, dict[str, Any]] = {}
    if nodes:
        output = _run(
            (
                "sinfo",
                f"--clusters={config.cluster}",
                "-h",
                "-N",
                "-n",
                ",".join(nodes),
                "-o",
                "%N|%T|%c|%m|%e|%O|%C",
            ),
        ).stdout
        for line in output.splitlines():
            if not line.strip():
                continue
            row = line.split("|")
            if len(row) != 7 or row[0] not in nodes or row[0] in resources:
                raise HostConvergeError("staging allocation query node readback is invalid")
            node, state, cpus, _memory, free_memory, cpu_load, cpu_counts = row
            try:
                cpus_total = int(cpus)
                free_memory_mib = 0 if free_memory in {"", "N/A", "(null)"} else int(free_memory)
                parsed_load = None if cpu_load in {"", "N/A", "(null)"} else float(cpu_load)
                counts = tuple(int(value) for value in cpu_counts.split("/"))
            except ValueError as exc:
                raise HostConvergeError(
                    "staging allocation query node resources are invalid",
                ) from exc
            if (
                cpus_total < 1
                or free_memory_mib < 0
                or (parsed_load is not None and parsed_load < 0)
                or len(counts) != 4
                or any(value < 0 for value in counts)
                or counts[3] != cpus_total
                or re.fullmatch(r"[A-Za-z0-9_+*~#$@%-]+", state) is None
            ):
                raise HostConvergeError(
                    "staging allocation query node resources are invalid",
                )
            resources[node] = {
                "hostname": node,
                "state": state,
                "cpus_total": cpus_total,
                "free_memory_mib": free_memory_mib,
                "cpu_load": parsed_load,
                "idle_cpus": counts[1],
            }
        if set(resources) != set(nodes):
            raise HostConvergeError("staging allocation query node set is incomplete")
    return {
        "schema_version": 1,
        "kind": "staging_external_slurm_allocation_query",
        "request_id": request_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "cluster": config.cluster,
        "controller": config.controller,
        "submit_host": config.submit_host,
        "jobs": [jobs[job_id] for job_id in job_ids if job_id in jobs],
        "nodes": [resources[node] for node in nodes],
        "observed_at": observed_at,
        "status": "observed",
    }


def staging_allocation_submit(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
    requested_node: str,
) -> dict[str, Any]:
    with _staging_submission_lock(request_id):
        return _staging_allocation_submit_locked(
            config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            request_id=request_id,
            requested_node=requested_node,
        )


def _staging_allocation_submit_locked(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
    requested_node: str,
) -> dict[str, Any]:
    if (
        os.geteuid() != 0
        or SHA_RE.fullmatch(candidate_sha) is None
        or SHA_RE.fullmatch(candidate_tree) is None
        or DIGEST_RE.fullmatch(request_id) is None
        or requested_node not in config.allowed_nodes
        or _staging_allocation_node(config) != config.submit_host
    ):
        raise HostConvergeError("staging allocation submit request is invalid")
    identity = _staging_identity_snapshot(config)
    namespace = _converge_staging_shared_namespace(config)
    candidate_binding = _staging_candidate_binding(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    repository = Path(candidate_binding["repository"]["path"])
    profile = _staging_slurm_profile(config, repository)
    guard_binding, candidate_set_sha256 = _staging_candidate_set_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    driver = _run(("docker", "info", "--format", "{{.CgroupDriver}}")).stdout.strip()
    if driver != profile.docker_cgroup_driver:
        raise HostConvergeError("staging allocation submit Docker driver drifted")
    slurm = _staging_slurm_live_config(config)
    job_name = _staging_allocation_job_name(
        candidate_sha=candidate_sha,
        node=requested_node,
        candidate_set_sha256=candidate_set_sha256,
        resource_generation=int(guard_binding["resource_generation"]),
        request_id=request_id,
    )
    worker = (
        str(INSTALLED_PROGRAM),
        "staging-allocation-worker",
        "--candidate-sha",
        candidate_sha,
        "--candidate-tree",
        candidate_tree,
        "--request-id",
        request_id,
    )
    wrapped = " ".join(
        shlex.quote(item)
        for item in (
            "/usr/bin/srun",
            "--nodes=1",
            "--ntasks=1",
            f"--nodelist={requested_node}",
            *worker,
        )
    )
    wal_path, wal, wal_existed = _staging_submission_wal(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        request_id=request_id,
        requested_node=requested_node,
        candidate_set_sha256=candidate_set_sha256,
        resource_generation=int(guard_binding["resource_generation"]),
        job_name=job_name,
        config=config,
        wrapped=wrapped,
    )
    _staging_prepare_result_directory(
        config,
        candidate_sha=candidate_sha,
        request_id=request_id,
        allow_existing=wal_existed,
    )
    if wal["phase"] == "submitted":
        result = wal["result"]
        if not isinstance(result, dict):
            raise HostConvergeError("staging submission WAL result is invalid")
        return result
    recovered_job_id = _staging_allocation_recover_job_id(
        config,
        job_name=job_name,
        requested_node=requested_node,
        request_id=request_id,
        prepared_at=str(wal["prepared_at"]),
    )
    if recovered_job_id is None:
        submitted = _run(
            (
                "sbatch",
                "--parsable",
                f"--job-name={job_name}",
                f"--uid={config.batch_user}",
                f"--account={config.slurm_account}",
                f"--qos={config.qos}",
                f"--clusters={config.cluster}",
                f"--partition={config.partition}",
                f"--nodelist={requested_node}",
                "--oversubscribe",
                "--nodes=1",
                "--ntasks=1",
                "--cpus-per-task=2",
                "--mem=11500M",
                "--time=12:00:00",
                "--output=/dev/null",
                "--error=/dev/null",
                f"--comment=loom-cgroup-v1:pids=65536:r={request_id}",
                "--export=NONE",
                f"--wrap={wrapped}",
            ),
        ).stdout.strip()
        match = re.fullmatch(r"([1-9][0-9]*)(?:;[A-Za-z0-9_.-]+)?", submitted)
        if match is None:
            raise HostConvergeError("staging allocation submit returned no exact job ID")
        job_id = match.group(1)
    else:
        job_id = recovered_job_id
    result = {
        "schema_version": 1,
        "kind": "staging_external_slurm_allocation_submission",
        "request_id": request_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "job_id": job_id,
        "job_name": job_name,
        "candidate_set_sha256": candidate_set_sha256,
        "resource_generation": guard_binding["resource_generation"],
        "docker_cgroup_driver": driver,
        "node": requested_node,
        "cluster": slurm["cluster"],
        "account": slurm["account"],
        "qos": slurm["qos"],
        "user": config.batch_user,
        "uid": config.batch_uid,
        "gid": config.batch_gid,
        "service_identity": identity,
        "mount": namespace,
        "submitted_at": _staging_timestamp(),
        "status": "submitted",
    }
    submitted_wal = {
        **wal,
        "phase": "submitted",
        "result": result,
        "result_sha256": hashlib.sha256(_staging_submission_wal_bytes(result)).hexdigest(),
    }
    unsigned_wal = {key: value for key, value in submitted_wal.items() if key != "payload_sha256"}
    submitted_wal["payload_sha256"] = hashlib.sha256(
        _staging_submission_wal_bytes(unsigned_wal)
    ).hexdigest()
    _atomic_write(
        wal_path,
        _staging_submission_wal_bytes(submitted_wal),
        mode=0o600,
    )
    return result


def staging_allocation_cancel(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
    submit_request_id: str,
    job_id: str,
    requested_node: str,
) -> dict[str, Any]:
    if (
        os.geteuid() != 0
        or SHA_RE.fullmatch(candidate_sha) is None
        or SHA_RE.fullmatch(candidate_tree) is None
        or DIGEST_RE.fullmatch(request_id) is None
        or DIGEST_RE.fullmatch(submit_request_id) is None
        or re.fullmatch(r"[1-9][0-9]*", job_id) is None
        or requested_node not in config.allowed_nodes
        or _staging_allocation_node(config) != config.submit_host
    ):
        raise HostConvergeError("staging allocation cancel request is invalid")
    guard_binding, candidate_set_sha256 = _staging_candidate_set_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    rows = _staging_sacct_rows(config, job_id)
    base = [row for row in rows if len(row) == 10 and row[0] == job_id]
    expected_name = _staging_allocation_job_name(
        candidate_sha=candidate_sha,
        node=requested_node,
        candidate_set_sha256=candidate_set_sha256,
        resource_generation=int(guard_binding["resource_generation"]),
        request_id=submit_request_id,
    )
    if (
        len(base) != 1
        or base[0][1] != expected_name
        or base[0][3].lower() != requested_node.lower()
        or base[0][4] != config.slurm_account
        or base[0][5] != config.batch_user
        or base[0][6] != config.cluster
        or base[0][7] != config.qos
    ):
        raise HostConvergeError("staging allocation cancel readback drifted")
    _run(("scancel", f"--clusters={config.cluster}", job_id), expected={0, 1})
    deadline = time.monotonic() + config.job_timeout_seconds
    terminal: list[str] | None = None
    while time.monotonic() < deadline:
        rows = _staging_sacct_rows(config, job_id)
        matches = [row for row in rows if len(row) == 10 and row[0] == job_id]
        if len(matches) == 1 and _staging_terminal_state(matches[0][2]) in {
            "CANCELLED",
            "COMPLETED",
            "FAILED",
            "TIMEOUT",
            "NODE_FAIL",
        }:
            terminal = matches[0]
            break
        time.sleep(1)
    if terminal is None:
        raise HostConvergeError("staging allocation cancel did not become terminal")
    cleanup_path = _staging_result_path(
        config,
        candidate_sha=candidate_sha,
        request_id=submit_request_id,
        node=requested_node,
    )
    try:
        cleanup = json.loads(cleanup_path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise HostConvergeError("staging allocation cleanup result is unavailable") from exc
    if (
        not isinstance(cleanup, dict)
        or cleanup.get("kind") != "staging_external_slurm_worker_terminal"
        or cleanup.get("request_id") != submit_request_id
        or cleanup.get("candidate_sha") != candidate_sha
        or cleanup.get("candidate_tree") != candidate_tree
        or cleanup.get("node") != requested_node
        or cleanup.get("job_id") != job_id
        or cleanup.get("cleanup_verified") is not True
        or any(
            cleanup.get(field) != 0
            for field in (
                "orphan_containers",
                "orphan_networks",
                "orphan_volumes",
            )
        )
    ):
        raise HostConvergeError("staging allocation cleanup result drifted")
    return {
        "schema_version": 1,
        "kind": "staging_external_slurm_allocation_cancellation",
        "request_id": request_id,
        "submit_request_id": submit_request_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "job_id": job_id,
        "node": requested_node,
        "state": _staging_terminal_state(terminal[2]),
        "cleanup_sha256": hashlib.sha256(
            (
                json.dumps(cleanup, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
            ).encode("ascii"),
        ).hexdigest(),
        "orphan_containers": 0,
        "orphan_networks": 0,
        "orphan_volumes": 0,
        "cancelled_at": _staging_timestamp(),
        "status": "cancelled",
    }


def _staging_load_node_result(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
    node: str,
) -> dict[str, Any]:
    guard_binding, candidate_set_sha256 = _staging_candidate_set_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    path = _staging_result_path(
        config,
        candidate_sha=candidate_sha,
        request_id=request_id,
        node=node,
    )
    try:
        metadata = path.lstat()
        raw = path.read_bytes()
    except OSError as exc:
        raise HostConvergeError("staging allocation node result is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or (metadata.st_uid, metadata.st_gid) != (config.batch_uid, config.batch_gid)
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or len(raw) > 256 * 1024
    ):
        raise HostConvergeError("staging allocation node result metadata drifted")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("staging allocation node result is invalid") from exc
    driver = payload.get("docker_cgroup_driver") if isinstance(payload, dict) else None
    cgroup_parent = payload.get("cgroup_parent") if isinstance(payload, dict) else None
    systemd_receipt = payload.get("systemd_slice_receipt") if isinstance(payload, dict) else None
    container_cgroup = payload.get("container_cgroup") if isinstance(payload, dict) else None
    cgroup_binding_valid = (
        driver == "cgroupfs"
        and isinstance(cgroup_parent, str)
        and cgroup_parent.startswith("/")
        and systemd_receipt is None
        and container_cgroup is None
    ) or (
        driver == "systemd"
        and isinstance(cgroup_parent, str)
        and SYSTEMD_SLICE_RE.fullmatch(cgroup_parent) is not None
        and isinstance(systemd_receipt, dict)
        and systemd_receipt.get("systemd_slice") == cgroup_parent
        and systemd_receipt.get("candidate_sha") == candidate_sha
        and systemd_receipt.get("candidate_tree") == candidate_tree
        and systemd_receipt.get("resource_generation") == guard_binding["resource_generation"]
        and isinstance(container_cgroup, dict)
        and isinstance(container_cgroup.get("observed_cgroup"), str)
        and f"/{cgroup_parent}/" in container_cgroup["observed_cgroup"]
    )
    if (
        not isinstance(payload, dict)
        or raw
        != (
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("ascii")
        or payload.get("schema_version") != 1
        or payload.get("kind") != "staging_external_slurm_node_probe"
        or payload.get("request_id") != request_id
        or payload.get("candidate_sha") != candidate_sha
        or payload.get("candidate_tree") != candidate_tree
        or payload.get("candidate_set_sha256") != candidate_set_sha256
        or payload.get("guard_binding_payload_sha256") != guard_binding["payload_sha256"]
        or not isinstance(payload.get("job_start_time"), str)
        or not payload["job_start_time"]
        or not cgroup_binding_valid
        or payload.get("node") != node
        or payload.get("canonical_host") != config.host_aliases[node]
        or payload.get("service_identity") != _staging_identity_snapshot(config)
        or payload.get("heartbeat_count") != 2
        or payload.get("orphan_containers") != 0
        or payload.get("orphan_networks") != 0
        or payload.get("orphan_volumes") != 0
        or payload.get("cleanup_verified") is not True
        or payload.get("result") != "pass"
    ):
        raise HostConvergeError("staging allocation node result binding drifted")
    return payload


def staging_allocation_probe(
    config: StagingAllocationConfig,
    *,
    candidate_sha: str,
    candidate_tree: str,
    request_id: str,
) -> dict[str, Any]:
    if (
        os.geteuid() != 0
        or SHA_RE.fullmatch(candidate_sha) is None
        or SHA_RE.fullmatch(candidate_tree) is None
        or DIGEST_RE.fullmatch(request_id) is None
    ):
        raise HostConvergeError("staging allocation probe request is invalid")
    current_node = _staging_allocation_node(config)
    if current_node != config.submit_host:
        raise HostConvergeError("staging allocation probe requires the exact submit host")
    identity = _staging_identity_snapshot(config)
    namespace = _converge_staging_shared_namespace(config)
    binding = _staging_candidate_binding(
        config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    repository = Path(binding["repository"]["path"])
    profile = _staging_slurm_profile(config, repository)
    guard_binding, candidate_set_sha256 = _staging_candidate_set_identity(
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
    )
    driver = _run(("docker", "info", "--format", "{{.CgroupDriver}}")).stdout.strip()
    if driver != profile.docker_cgroup_driver:
        raise HostConvergeError("staging allocation probe Docker driver drifted")
    slurm = _staging_slurm_live_config(config)
    request_root = _staging_prepare_result_directory(
        config,
        candidate_sha=candidate_sha,
        request_id=request_id,
    )
    jobs: dict[str, tuple[str, str]] = {}
    try:
        for node in config.allowed_nodes:
            job_name = _staging_allocation_job_name(
                candidate_sha=candidate_sha,
                node=node,
                candidate_set_sha256=candidate_set_sha256,
                resource_generation=int(guard_binding["resource_generation"]),
                request_id=request_id,
            )
            node_check = (
                str(INSTALLED_PROGRAM),
                "staging-allocation-node-check",
                "--candidate-sha",
                candidate_sha,
                "--candidate-tree",
                candidate_tree,
                "--request-id",
                request_id,
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
            submitted = _run(
                (
                    "sbatch",
                    "--parsable",
                    f"--job-name={job_name}",
                    f"--uid={config.batch_user}",
                    f"--account={config.slurm_account}",
                    f"--qos={config.qos}",
                    f"--clusters={config.cluster}",
                    f"--partition={config.partition}",
                    f"--nodelist={node}",
                    "--oversubscribe",
                    "--nodes=1",
                    "--ntasks=1",
                    "--cpus-per-task=2",
                    "--mem=11500M",
                    "--time=00:04:00",
                    "--output=/dev/null",
                    "--error=/dev/null",
                    f"--comment=loom-cgroup-v1:pids=65536:r={request_id}",
                    "--export=NONE",
                    f"--wrap={wrapped}",
                )
            ).stdout.strip()
            match = re.fullmatch(r"([1-9][0-9]*)(?:;[A-Za-z0-9_.-]+)?", submitted)
            if match is None:
                raise HostConvergeError("staging allocation submit returned no exact job ID")
            jobs[node] = (match.group(1), job_name)
    except Exception:
        for job_id, _job_name in jobs.values():
            _run(
                ("scancel", f"--clusters={config.cluster}", job_id),
                expected={0, 1},
            )
        raise
    terminal: dict[str, tuple[list[str], list[str]]] = {}
    deadline = time.monotonic() + config.job_timeout_seconds
    while time.monotonic() < deadline and len(terminal) < len(jobs):
        for node, (job_id, job_name) in jobs.items():
            if node in terminal:
                continue
            rows = _staging_sacct_rows(config, job_id)
            base_rows = [row for row in rows if len(row) == 10 and row[0] == job_id]
            step_rows = [row for row in rows if len(row) == 10 and row[0] == f"{job_id}.0"]
            if (
                len(base_rows) == 1
                and len(step_rows) == 1
                and _staging_terminal_state(base_rows[0][2])
                in {
                    "COMPLETED",
                    "FAILED",
                    "CANCELLED",
                    "TIMEOUT",
                    "OUT_OF_MEMORY",
                    "NODE_FAIL",
                }
                and _staging_terminal_state(step_rows[0][2])
                in {
                    "COMPLETED",
                    "FAILED",
                    "CANCELLED",
                    "TIMEOUT",
                    "OUT_OF_MEMORY",
                    "NODE_FAIL",
                }
            ):
                if base_rows[0][1] != job_name:
                    raise HostConvergeError("staging allocation job name drifted")
                terminal[node] = (base_rows[0], step_rows[0])
        if len(terminal) < len(jobs):
            time.sleep(1)
    if len(terminal) != len(jobs):
        for node, (job_id, _job_name) in jobs.items():
            if node not in terminal:
                _run(
                    ("scancel", f"--clusters={config.cluster}", job_id),
                    expected={0, 1},
                )
        raise HostConvergeError("staging allocation jobs exceeded the bounded timeout")
    final_nodes: list[dict[str, Any]] = []
    for node in config.allowed_nodes:
        job_id, job_name = jobs[node]
        base, step = terminal[node]
        if (
            _staging_terminal_state(base[2]) != "COMPLETED"
            or _staging_terminal_state(step[2]) != "COMPLETED"
            or base[3].lower() != node.lower()
            or step[3].lower() != node.lower()
            or base[4] != config.slurm_account
            or step[4] != config.slurm_account
            or base[5] != config.batch_user
            or step[5] != config.batch_user
            or base[6] != config.cluster
            or step[6] != config.cluster
            or base[7] != config.qos
        ):
            raise HostConvergeError("staging allocation sbatch/srun readback drifted")
        node_result = _staging_load_node_result(
            config,
            candidate_sha=candidate_sha,
            candidate_tree=candidate_tree,
            request_id=request_id,
            node=node,
        )
        repository_binding = node_result["repository"]
        env_binding = node_result["worker_env"]
        final_nodes.append(
            {
                "node": node,
                "job_id": job_id,
                "job_name": job_name,
                "account": config.slurm_account,
                "qos": config.qos,
                "user": config.batch_user,
                "uid": config.batch_uid,
                "gid": config.batch_gid,
                "sbatch_verified": True,
                "srun_verified": True,
                "candidate_sha": candidate_sha,
                "candidate_tree": candidate_tree,
                "repository": repository_binding["path"],
                "repository_device": repository_binding["device"],
                "repository_inode": repository_binding["inode"],
                "worker_env": env_binding["path"],
                "worker_env_device": env_binding["device"],
                "worker_env_inode": env_binding["inode"],
                "worker_env_sha256": env_binding["sha256"],
                "compose_project": node_result["compose_project"],
                "compose_config_sha256": node_result["compose_config_sha256"],
                "docker_server_version": node_result["docker_server_version"],
                "worker_id": node_result["worker_id"],
                "registered_at": node_result["registered_at"],
                "first_heartbeat_at": node_result["first_heartbeat_at"],
                "last_heartbeat_at": node_result["last_heartbeat_at"],
                "heartbeat_count": node_result["heartbeat_count"],
                "cancel_requested_at": node_result["cancel_requested_at"],
                "stopped_at": node_result["stopped_at"],
                "job_terminal_at": base[9],
                "job_state": "COMPLETED",
                "orphan_containers": 0,
                "orphan_networks": 0,
                "orphan_volumes": 0,
                "cleanup_verified": True,
            }
        )
    repository = str(binding["repository"]["path"])
    worker_env = str(binding["worker_env"]["path"])
    if any(path.parent != request_root for path in request_root.iterdir() if path.is_file()):
        raise HostConvergeError("staging allocation result set escaped its request root")
    result = {
        "schema_version": 1,
        "kind": "staging_external_slurm_allocation_probe",
        "request_id": request_id,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "cluster": config.cluster,
        "pool": config.pool,
        "submit_host": config.submit_host,
        "controller": config.controller,
        "service_identity": identity,
        "namespace": namespace,
        "slurm_account": slurm["account"],
        "qos": slurm["qos"],
        "allowed_nodes": list(config.allowed_nodes),
        "excluded_nodes": list(config.excluded_nodes),
        "repository": repository,
        "worker_env": worker_env,
        "nodes": final_nodes,
        "result": "pass",
    }
    _atomic_write(
        request_root / "probe.json",
        (
            json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
        ).encode("ascii"),
        mode=0o600,
        identity=Identity(
            config.batch_user,
            config.batch_group,
            config.batch_uid,
            config.batch_gid,
        ),
    )
    return result


def _run(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    identity: Identity | None = None,
    init_groups: bool = False,
    expected: set[int] | frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    command = list(argv)
    if identity is not None and os.geteuid() != identity.uid:
        prefix = ["runuser", "--user", identity.user]
        if not init_groups:
            prefix.extend(("--group", identity.group))
        child_environment = env or {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
        }
        command = [
            *prefix,
            "--",
            "env",
            "-i",
            *(f"{key}={value}" for key, value in sorted(child_environment.items())),
            *command,
        ]
        env = None
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=dict(env) if env is not None else None,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode not in expected:
        purpose = Path(argv[0]).name if argv else "command"
        raise HostConvergeError(
            f"{purpose} failed safely with exit code {completed.returncode}",
        )
    return completed


def _path_exists_as(path: Path, identity: Identity) -> bool:
    return (
        _run(
            ("test", "-e", str(path)),
            identity=identity,
            expected={0, 1},
        ).returncode
        == 0
    )


def _clean_git_environment() -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
    }


def _atomic_write(
    path: Path,
    content: bytes,
    *,
    mode: int,
    identity: Identity | None = None,
) -> None:
    if not path.is_absolute() or path.name in {"", ".", ".."}:
        raise HostConvergeError("atomic write target path is invalid")
    descriptor = -1
    temporary_name = ""
    try:
        with _seized_directory(path.parent, create=True) as parent_fd:
            try:
                for _attempt in range(16):
                    temporary_name = f".{path.name}.{secrets.token_hex(16)}"
                    try:
                        descriptor = os.open(
                            temporary_name,
                            os.O_WRONLY
                            | os.O_CREAT
                            | os.O_EXCL
                            | getattr(os, "O_CLOEXEC", 0)
                            | os.O_NOFOLLOW,
                            0o600,
                            dir_fd=parent_fd,
                        )
                        break
                    except FileExistsError:
                        continue
                else:
                    raise HostConvergeError(
                        "could not reserve atomic write temporary file",
                    )

                view = memoryview(content)
                while view:
                    written = os.write(descriptor, view)
                    if written <= 0:
                        raise HostConvergeError("atomic write made no progress")
                    view = view[written:]
                if identity is not None:
                    os.fchown(descriptor, identity.uid, identity.gid)
                os.fchmod(descriptor, mode)
                os.fsync(descriptor)

                opened = os.fstat(descriptor)
                temporary = os.stat(
                    temporary_name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                if not stat.S_ISREG(temporary.st_mode) or (temporary.st_dev, temporary.st_ino) != (
                    opened.st_dev,
                    opened.st_ino,
                ):
                    raise HostConvergeError("atomic write temporary binding changed")

                _replace_file_at(parent_fd, temporary_name, path.name)
                temporary_name = ""
                rebound = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
                if not stat.S_ISREG(rebound.st_mode) or (rebound.st_dev, rebound.st_ino) != (
                    opened.st_dev,
                    opened.st_ino,
                ):
                    raise HostConvergeError("atomic write target binding changed")
                os.fsync(parent_fd)
            finally:
                if temporary_name:
                    try:
                        os.unlink(temporary_name, dir_fd=parent_fd)
                        os.fsync(parent_fd)
                    except FileNotFoundError:
                        pass
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError(f"could not atomically write {path.name}") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_root_private_directory(path: Path) -> None:
    descriptor = _open_absolute_directory(path, create=True)
    try:
        os.fchown(descriptor, 0, 0)
        os.fchmod(descriptor, 0o700)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
        ):
            raise HostConvergeError(f"root-private directory did not converge: {path}")
    except OSError as exc:
        raise HostConvergeError(f"root-private directory did not converge: {path}") from exc
    finally:
        os.close(descriptor)


def _assert_secure_file(path: Path, identity: Identity, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise HostConvergeError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise HostConvergeError(f"{label} must be a regular non-symlink file")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise HostConvergeError(f"{label} must have mode 0600")
    if (metadata.st_uid, metadata.st_gid) != (identity.uid, identity.gid):
        raise HostConvergeError(f"{label} owner is invalid")


def _directory_open_flags() -> int:
    return os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW


def _open_absolute_directory(path: Path, *, create: bool) -> int:
    if not path.is_absolute() or ".." in path.parts:
        raise HostConvergeError("private sandbox parent path is invalid")
    descriptor = os.open("/", _directory_open_flags())
    try:
        for component in path.parts[1:]:
            if create:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                except FileExistsError:
                    pass
            child = os.open(component, _directory_open_flags(), dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        return descriptor
    except Exception:
        os.close(descriptor)
        raise


@contextmanager
def _seized_directory(path: Path, *, create: bool) -> Iterator[int]:
    descriptor = -1
    metadata: os.stat_result | None = None
    seized = False
    try:
        descriptor = _open_absolute_directory(path, create=create)
        metadata = os.fstat(descriptor)
        seized = True
        os.fchown(descriptor, os.geteuid(), os.getegid())
        os.fchmod(descriptor, 0o700)
        yield descriptor
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError(f"could not seize directory: {path}") from exc
    finally:
        if descriptor >= 0:
            try:
                if seized and metadata is not None:
                    os.fchown(descriptor, metadata.st_uid, metadata.st_gid)
                    os.fchmod(descriptor, stat.S_IMODE(metadata.st_mode))
            except OSError as exc:
                raise HostConvergeError(
                    f"could not restore directory: {path}",
                ) from exc
            finally:
                os.close(descriptor)


def _replace_file_at(parent_fd: int, source: str, target: str) -> None:
    os.replace(
        source,
        target,
        src_dir_fd=parent_fd,
        dst_dir_fd=parent_fd,
    )


def _mkdir_private_dir_at(parent_fd: int, name: str) -> None:
    try:
        os.mkdir(name, mode=0o700, dir_fd=parent_fd)
    except FileExistsError:
        return


def _private_child_names(profile: Profile) -> tuple[str, ...]:
    children = (
        profile.cache_root,
        profile.evidence_root,
        profile.private_runtime_root,
        profile.secrets_root,
    )
    if any(
        path.parent != profile.state_root or path.name in {"", ".", ".."} for path in children
    ) or len({path.name for path in children}) != len(children):
        raise HostConvergeError("private sandbox child paths are invalid")
    return tuple(path.name for path in children)


def _validate_private_directory_fd(
    descriptor: int,
    identity: Identity,
    path: Path,
) -> os.stat_result:
    metadata = os.fstat(descriptor)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or (metadata.st_uid, metadata.st_gid) != (identity.uid, identity.gid)
    ):
        raise HostConvergeError(f"private sandbox root is invalid: {path}")
    return metadata


@contextmanager
def _private_state_directory(
    profile: Profile,
    identity: Identity,
    *,
    create: bool,
    seize: bool,
) -> Iterator[tuple[int, int, os.stat_result]]:
    parent_fd = -1
    state_fd = -1
    state_metadata: os.stat_result | None = None
    seized = False
    try:
        parent_fd = _open_absolute_directory(profile.state_root.parent, create=create)
        if create:
            _mkdir_private_dir_at(parent_fd, profile.state_root.name)
        state_fd = os.open(
            profile.state_root.name,
            _directory_open_flags(),
            dir_fd=parent_fd,
        )
        state_metadata = os.fstat(state_fd)
        if not stat.S_ISDIR(state_metadata.st_mode):
            raise HostConvergeError(
                f"private sandbox root is unsafe: {profile.state_root}",
            )
        if seize:
            # Temporarily remove the sandbox user's authority over child names.
            # All following mutations use the already-bound descriptor.
            seized = True
            os.fchown(state_fd, os.geteuid(), os.getegid())
            os.fchmod(state_fd, 0o700)
        yield parent_fd, state_fd, state_metadata
        rebound = os.stat(
            profile.state_root.name,
            dir_fd=parent_fd,
            follow_symlinks=False,
        )
        current = os.fstat(state_fd)
        if not stat.S_ISDIR(rebound.st_mode) or (rebound.st_dev, rebound.st_ino) != (
            current.st_dev,
            current.st_ino,
        ):
            raise HostConvergeError(
                f"private sandbox root binding changed: {profile.state_root}",
            )
    except HostConvergeError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise HostConvergeError(
                f"private sandbox root is unsafe: {profile.state_root}",
            ) from exc
        raise HostConvergeError(
            f"could not access private sandbox root: {profile.state_root}",
        ) from exc
    finally:
        if state_fd >= 0:
            try:
                if seized:
                    os.fchmod(state_fd, 0o700)
                    os.fchown(state_fd, identity.uid, identity.gid)
            except OSError as exc:
                raise HostConvergeError(
                    f"could not restore private sandbox root: {profile.state_root}",
                ) from exc
            finally:
                os.close(state_fd)
        if parent_fd >= 0:
            os.close(parent_fd)


def verify_private_roots(profile: Profile, identity: Identity) -> None:
    child_names = _private_child_names(profile)
    try:
        with _private_state_directory(
            profile,
            identity,
            create=False,
            seize=False,
        ) as (_parent_fd, state_fd, _state_metadata):
            _validate_private_directory_fd(state_fd, identity, profile.state_root)
            for name in child_names:
                descriptor = os.open(name, _directory_open_flags(), dir_fd=state_fd)
                try:
                    _validate_private_directory_fd(
                        descriptor,
                        identity,
                        profile.state_root / name,
                    )
                finally:
                    os.close(descriptor)
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError(
            f"private sandbox root is unavailable: {profile.state_root}",
        ) from exc


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or key in values:
            raise HostConvergeError("sandbox secret env file is malformed")
        values[key] = value
    return values


def _render_env(values: Mapping[str, str]) -> bytes:
    return "".join(f"{key}={values[key]}\n" for key in sorted(values)).encode()


def _new_secret_values(sandbox: str) -> dict[str, str]:
    return {
        "LOOM_DEV_POSTGRES_USER": f"loom_{sandbox}",
        "LOOM_DEV_POSTGRES_PASSWORD": secrets.token_urlsafe(32),
        "LOOM_DEV_MINIO_ROOT_USER": f"loom-{sandbox}",
        "LOOM_DEV_MINIO_ROOT_PASSWORD": secrets.token_urlsafe(32),
        "LOOM_CP_STEP_JWT_SIGNING_KEY": secrets.token_urlsafe(48),
        "LOOM_SECRET_STORE_MASTER_KEY": base64.b64encode(os.urandom(32)).decode(),
        # This bootstrap value is intentionally not authoritative until the
        # local Control Plane mints and persists its hash after first boot.
        "LOOM_WORKER_TOKEN": f"loom_w_{secrets.token_hex(32)}",
        "LOOM_SVC_BATCH_RUNNER_CP_TOKEN": "",
    }


def ensure_private_roots(profile: Profile, identity: Identity) -> None:
    child_names = _private_child_names(profile)
    try:
        with _private_state_directory(
            profile,
            identity,
            create=True,
            seize=True,
        ) as (_parent_fd, state_fd, _state_metadata):
            for name in child_names:
                _mkdir_private_dir_at(state_fd, name)
                descriptor = os.open(name, _directory_open_flags(), dir_fd=state_fd)
                try:
                    metadata = os.fstat(descriptor)
                    if not stat.S_ISDIR(metadata.st_mode):
                        raise HostConvergeError(
                            f"private sandbox root is unsafe: {profile.state_root / name}",
                        )
                    os.fchown(descriptor, identity.uid, identity.gid)
                    os.fchmod(descriptor, 0o700)
                    current = os.fstat(descriptor)
                    rebound = os.stat(name, dir_fd=state_fd, follow_symlinks=False)
                    if not stat.S_ISDIR(rebound.st_mode) or (current.st_dev, current.st_ino) != (
                        rebound.st_dev,
                        rebound.st_ino,
                    ):
                        raise HostConvergeError(
                            f"private sandbox root binding changed: {profile.state_root / name}",
                        )
                finally:
                    os.close(descriptor)
    except HostConvergeError:
        raise
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
            raise HostConvergeError(
                f"private sandbox root is unsafe: {profile.state_root}",
            ) from exc
        raise HostConvergeError(
            f"could not converge private sandbox root: {profile.state_root}",
        ) from exc
    verify_private_roots(profile, identity)


def verify_secret_files(profile: Profile, identity: Identity) -> None:
    verify_private_roots(profile, identity)
    _assert_secure_file(profile.secrets_env, identity, "sandbox secret env file")
    values = _parse_env_file(profile.secrets_env)
    missing = [key for key in SECRET_KEYS if not values.get(key)]
    if missing:
        raise HostConvergeError(
            "sandbox secret env file is incomplete: " + ", ".join(missing),
        )
    _assert_secure_file(profile.admin_secret, identity, "sandbox admin secret file")
    try:
        payload = tomllib.loads(profile.admin_secret.read_text(encoding="utf-8"))
        token = payload["admin"]["token"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("sandbox admin secret file is invalid") from exc
    if not isinstance(token, str) or not token.startswith("loom_admin_") or len(token) < 43:
        raise HostConvergeError("sandbox admin secret file is invalid")


def ensure_secret_files(profile: Profile, identity: Identity) -> None:
    ensure_private_roots(profile, identity)
    if not profile.secrets_env.exists():
        _atomic_write(
            profile.secrets_env,
            _render_env(_new_secret_values(profile.sandbox)),
            mode=0o600,
            identity=identity,
        )
    if not profile.admin_secret.exists():
        token = f"loom_admin_{secrets.token_urlsafe(32)}"
        content = (f'[admin]\ntoken = "{token}"\nversion = 1\n').encode()
        _atomic_write(
            profile.admin_secret,
            content,
            mode=0o600,
            identity=identity,
        )
    verify_secret_files(profile, identity)


def _git(
    candidate: Path,
    *args: str,
    identity: Identity | None = None,
) -> str:
    result = _run(
        ("git", "-c", f"safe.directory={candidate}", "-C", str(candidate), *args),
        env=_clean_git_environment(),
        identity=identity,
    )
    return result.stdout.strip()


def verify_candidate(
    profile: Profile,
    path: Path,
    sha: str,
    authority: Identity,
) -> str:
    if path != profile.candidate_root / sha or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate path is not exact-SHA bound")
    directory = _run(
        ("test", "-d", str(path)),
        identity=authority,
        expected={0, 1},
    )
    symlink = _run(
        ("test", "-L", str(path)),
        identity=authority,
        expected={0, 1},
    )
    if directory.returncode != 0 or symlink.returncode == 0:
        raise HostConvergeError("candidate directory is unavailable")
    if _git(path, "rev-parse", "--verify", "HEAD", identity=authority) != sha:
        raise HostConvergeError("candidate HEAD does not match requested SHA")
    if _git(path, "rev-parse", "--verify", f"{sha}^{{commit}}", identity=authority) != sha:
        raise HostConvergeError("candidate commit does not resolve exactly")
    if _git(
        path,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        identity=authority,
    ):
        raise HostConvergeError("candidate checkout is not clean")
    tree = _git(path, "rev-parse", "--verify", "HEAD^{tree}", identity=authority)
    if SHA_RE.fullmatch(tree) is None:
        raise HostConvergeError("candidate tree is invalid")
    root_metadata = path.lstat()
    if (
        root_metadata.st_uid != authority.uid
        or root_metadata.st_gid != authority.gid
        or stat.S_IMODE(root_metadata.st_mode) != 0o2750
    ):
        raise HostConvergeError("candidate root metadata is invalid")
    for root, directories, files in os.walk(path, followlinks=False):
        for entry in (
            Path(root),
            *(Path(root) / name for name in (*directories, *files)),
        ):
            metadata = entry.lstat()
            if (metadata.st_uid, metadata.st_gid) != (authority.uid, authority.gid):
                raise HostConvergeError("candidate ownership is invalid")
            if not stat.S_ISLNK(metadata.st_mode) and metadata.st_mode & 0o022:
                raise HostConvergeError("candidate contains a group/world-writable entry")
    return tree


def verify_candidate_root(profile: Profile, authority: Identity) -> None:
    directory = _run(
        ("test", "-d", str(profile.candidate_root)),
        identity=authority,
        expected={0, 1},
    )
    symlink = _run(
        ("test", "-L", str(profile.candidate_root)),
        identity=authority,
        expected={0, 1},
    )
    metadata = _run(
        ("stat", "-Lc", "%u:%g:%a", str(profile.candidate_root)),
        identity=authority,
    ).stdout.strip()
    if (
        directory.returncode != 0
        or symlink.returncode == 0
        or metadata != f"0:{authority.gid}:2750"
    ):
        raise HostConvergeError("candidate root owner or mode is invalid")


def _exact_keys(payload: Mapping[str, Any], expected: set[str], label: str) -> None:
    if set(payload) != expected:
        raise HostConvergeError(f"{label} does not match the closed schema")


def _parse_attestation_time(value: object, label: str) -> datetime:
    if not isinstance(value, str):
        raise HostConvergeError(f"{label} is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HostConvergeError(f"{label} is invalid") from exc
    if parsed.tzinfo is None:
        raise HostConvergeError(f"{label} must include timezone")
    return parsed.astimezone(UTC)


def combined_receipt_path(profile: Profile, sha: str) -> Path:
    return COMBINED_RECEIPT_ROOT / profile.sandbox / sha / "combined.json"


def verify_worker_runtime_env(
    profile: Profile,
    sha: str,
    sandbox_group: Identity,
) -> None:
    path = profile.worker_runtime_env(sha)
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise HostConvergeError("OLDLAB worker runtime env is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (sandbox_group.uid, sandbox_group.gid)
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise HostConvergeError("OLDLAB worker runtime env metadata is invalid")
    values = _parse_env_file(path)
    bundle_root = f"/etc/loom/developer-sandbox-links/clients/{profile.sandbox}/{sha}"
    expected = {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://sandbox-link:8080",
        "LOOM_WORKER_GATEWAY_URL": "http://sandbox-link:9100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://sandbox-link:9000",
        "LOOM_WORKER_SANDBOX_IDENTITY": profile.sandbox,
        "LOOM_WORKER_CANDIDATE_SHA": sha,
        "LOOM_WORKER_TOKEN_FILE_HOST": f"{bundle_root}/worker-token",
        "LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST": f"{bundle_root}/minio-access-key",
        "LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST": f"{bundle_root}/minio-secret-key",
        "LOOM_WORKER_CP_TLS_CA_FILE_HOST": f"{bundle_root}/ca.pem",
        "LOOM_WORKER_CP_TLS_CERT_FILE_HOST": f"{bundle_root}/client.pem",
        "LOOM_WORKER_CP_TLS_KEY_FILE_HOST": f"{bundle_root}/client-key.pem",
    }
    if values != expected:
        raise HostConvergeError("OLDLAB worker runtime env binding is invalid")


def _read_combined_receipt_bytes(path: Path) -> bytes:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        opened = os.fstat(descriptor)
        rebound = path.lstat()
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_uid, opened.st_gid) != (0, 0)
            or stat.S_IMODE(opened.st_mode) != 0o600
            or (opened.st_dev, opened.st_ino) != (rebound.st_dev, rebound.st_ino)
        ):
            raise HostConvergeError("combined activation receipt metadata is invalid")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > 8 * 1024 * 1024:
                raise HostConvergeError("combined activation receipt is too large")
        after = path.lstat()
        if (opened.st_dev, opened.st_ino) != (after.st_dev, after.st_ino):
            raise HostConvergeError("combined activation receipt changed during read")
        return b"".join(chunks)
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError("combined activation receipt is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def verify_combined_receipt(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    now: datetime | None = None,
) -> ActivationReceipt:
    now = (now or datetime.now(UTC)).astimezone(UTC)
    path = combined_receipt_path(profile, sha)
    raw = _read_combined_receipt_bytes(path)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("combined activation receipt is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("combined activation receipt is invalid")
    canonical = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        + b"\n"
    )
    if raw != canonical:
        raise HostConvergeError("combined activation receipt is not canonical")
    _exact_keys(
        payload,
        {
            "schema_version",
            "kind",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "collector",
            "fleet_attestation",
            "domains",
            "payload_sha256",
        },
        "combined activation receipt",
    )
    digest = payload["payload_sha256"]
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    expected_digest = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    if (
        payload["schema_version"] != 1
        or payload["kind"] != "loom.developer-runtime-combined-activation"
        or payload["sandbox"] != profile.sandbox
        or payload["candidate_sha"] != sha
        or payload["candidate_tree"] != tree
        or not isinstance(digest, str)
        or digest != expected_digest
    ):
        raise HostConvergeError("combined activation receipt identity or digest is invalid")
    collector = payload["collector"]
    fleet = payload["fleet_attestation"]
    domains = payload["domains"]
    if not all(isinstance(item, dict) for item in (collector, fleet, domains)):
        raise HostConvergeError("combined activation receipt sections are invalid")
    _exact_keys(collector, {"hostname", "collected_at", "expires_at"}, "collector")
    collected_at = _parse_attestation_time(collector["collected_at"], "collected_at")
    expires_at = _parse_attestation_time(collector["expires_at"], "expires_at")
    if (
        collector["hostname"] != EXPECTED_HOSTNAME
        or collected_at > now + timedelta(seconds=30)
        or now - collected_at > RECEIPT_FRESHNESS
        or expires_at <= now
        or expires_at <= collected_at
        or expires_at - collected_at > ATTESTATION_TTL
    ):
        raise HostConvergeError("combined activation receipt is stale or expired")
    _exact_keys(
        fleet,
        {"path", "payload_sha256", "generated_at", "expires_at"},
        "fleet receipt reference",
    )
    fleet_generated = _parse_attestation_time(fleet["generated_at"], "fleet generated_at")
    fleet_expires = _parse_attestation_time(fleet["expires_at"], "fleet expires_at")
    if (
        fleet["path"] != str(FLEET_ATTESTATION_ROOT / profile.sandbox / sha / "fleet.json")
        or not isinstance(fleet["payload_sha256"], str)
        or FINGERPRINT_RE.fullmatch(fleet["payload_sha256"]) is None
        or fleet_generated > now + timedelta(seconds=30)
        or now - fleet_generated > RECEIPT_FRESHNESS
        or fleet_generated > collected_at + timedelta(seconds=30)
        or collected_at - fleet_generated > RECEIPT_FRESHNESS
        or fleet_expires <= now
        or fleet_expires - fleet_generated != ATTESTATION_TTL
        or fleet_expires < expires_at
    ):
        raise HostConvergeError("combined fleet receipt binding is invalid")
    if set(domains) != {"oldlab", "gb10"}:
        raise HostConvergeError("combined receipt domain set is incomplete")
    domain_keys = {
        "manifest_path",
        "signature_path",
        "payload_sha256",
        "signature_sha256",
        "key_id",
        "generation",
        "published_at",
        "expires_at",
    }
    for domain in ("oldlab", "gb10"):
        row = domains[domain]
        if not isinstance(row, dict):
            raise HostConvergeError("combined receipt domain input is invalid")
        _exact_keys(row, domain_keys, "combined receipt domain input")
        base = f"/var/lib/loom-developer-domain-attestations/{profile.sandbox}/{sha}"
        published = _parse_attestation_time(row["published_at"], "domain published_at")
        domain_expires = _parse_attestation_time(row["expires_at"], "domain expires_at")
        if (
            row["manifest_path"] != f"{base}/{domain}.json"
            or row["signature_path"] != f"{base}/{domain}.sig"
            or not isinstance(row["payload_sha256"], str)
            or DIGEST_RE.fullmatch(row["payload_sha256"]) is None
            or not isinstance(row["signature_sha256"], str)
            or DIGEST_RE.fullmatch(row["signature_sha256"]) is None
            or not isinstance(row["key_id"], str)
            or DIGEST_RE.fullmatch(row["key_id"]) is None
            or type(row["generation"]) is not int
            or row["generation"] < 1
            or published > now + timedelta(seconds=30)
            or now - published > RECEIPT_FRESHNESS
            or published > collected_at + timedelta(seconds=30)
            or collected_at - published > ATTESTATION_TTL
            or domain_expires <= now
            or domain_expires - published != ATTESTATION_TTL
            or domain_expires < expires_at
        ):
            raise HostConvergeError("combined receipt domain binding is invalid")
    return ActivationReceipt(
        path=path,
        payload_sha256=digest,
        fleet_payload_sha256=fleet["payload_sha256"],
        expires_at=expires_at,
    )


def _renewal_state_file(profile: Profile) -> Path:
    return RENEWAL_STATE_ROOT / f"{profile.sandbox}.json"


def _write_root_exclusive(path: Path, content: bytes) -> None:
    descriptor = -1
    try:
        with _seized_directory(path.parent, create=True) as parent_fd:
            try:
                descriptor = os.open(
                    path.name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=parent_fd,
                )
            except FileExistsError as exc:
                if _read_combined_receipt_bytes(path) != content:
                    raise HostConvergeError(
                        "renewal history generation already exists with different bytes",
                    ) from exc
                return
            view = memoryview(content)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise HostConvergeError("renewal history write made no progress")
                view = view[written:]
            os.fchown(descriptor, 0, 0)
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            opened = os.fstat(descriptor)
            rebound = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if not stat.S_ISREG(rebound.st_mode) or (opened.st_dev, opened.st_ino) != (
                rebound.st_dev,
                rebound.st_ino,
            ):
                raise HostConvergeError("renewal history binding changed")
            os.fsync(parent_fd)
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError("could not persist renewal history") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _archive_runtime_attestation(
    profile: Profile,
    sha: str,
    tree: str,
    receipt: ActivationReceipt,
) -> dict[str, Any]:
    try:
        raw = _read_combined_receipt_bytes(receipt.path)
        combined = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise HostConvergeError("combined activation receipt is unavailable") from exc
    if not isinstance(combined, dict):
        raise HostConvergeError("combined activation receipt is invalid")
    canonical = (
        json.dumps(
            combined,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        + b"\n"
    )
    unsigned_combined = {key: value for key, value in combined.items() if key != "payload_sha256"}
    combined_digest = hashlib.sha256(
        json.dumps(
            unsigned_combined,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    if (
        raw != canonical
        or combined.get("sandbox") != profile.sandbox
        or combined.get("candidate_sha") != sha
        or combined.get("candidate_tree") != tree
        or combined.get("payload_sha256") != combined_digest
        or receipt.payload_sha256 != combined_digest
    ):
        raise HostConvergeError("combined activation receipt binding is invalid")
    collector = combined.get("collector")
    domains = combined.get("domains")
    fleet_reference = combined.get("fleet_attestation")
    if (
        not isinstance(collector, dict)
        or not isinstance(domains, dict)
        or not isinstance(fleet_reference, dict)
    ):
        raise HostConvergeError("combined activation receipt sections are invalid")
    collected_at = _parse_attestation_time(collector.get("collected_at"), "collected_at")
    combined_expires = _parse_attestation_time(collector.get("expires_at"), "expires_at")
    if combined_expires != receipt.expires_at:
        raise HostConvergeError("combined activation receipt expiry binding is invalid")
    fleet_path = fleet_reference.get("path")
    if not isinstance(fleet_path, str):
        raise HostConvergeError("combined fleet receipt path is invalid")
    try:
        fleet_payload = json.loads(_read_combined_receipt_bytes(Path(fleet_path)))
    except json.JSONDecodeError as exc:
        raise HostConvergeError("fleet attestation is invalid") from exc
    if not isinstance(fleet_payload, dict):
        raise HostConvergeError("fleet attestation is invalid")
    fleet_unsigned = {key: value for key, value in fleet_payload.items() if key != "payload_sha256"}
    fleet_digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                fleet_unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
    )
    fleet_nodes = fleet_payload.get("nodes")
    fleet_bundle = fleet_payload.get("bundle_generation")
    fleet_server = fleet_payload.get("server")
    if (
        fleet_payload.get("sandbox") != profile.sandbox
        or fleet_payload.get("candidate_sha") != sha
        or fleet_payload.get("payload_sha256") != fleet_digest
        or fleet_digest != receipt.fleet_payload_sha256
        or fleet_reference.get("payload_sha256") != fleet_digest
        or fleet_payload.get("generated_at") != fleet_reference.get("generated_at")
        or fleet_payload.get("expires_at") != fleet_reference.get("expires_at")
        or fleet_payload.get("eligible_nodes") != list(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_nodes, dict)
        or set(fleet_nodes) != set(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_bundle, dict)
        or fleet_bundle.get("candidate_sha") != sha
        or not isinstance(fleet_server, dict)
        or fleet_server.get("active_candidate_sha") != sha
        or fleet_server.get("node") != "oldlab-2"
        or fleet_server.get("unit_active") is not True
        or any(
            not isinstance(node, dict) or node.get("candidate_sha") != sha
            for node in fleet_nodes.values()
        )
    ):
        raise HostConvergeError("fleet attestation host coverage is invalid")
    try:
        domain_generations = {
            domain: int(domains[domain]["generation"]) for domain in ("oldlab", "gb10")
        }
    except (KeyError, TypeError, ValueError) as exc:
        raise HostConvergeError("combined activation receipt generations are invalid") from exc
    previous = _load_json(_renewal_state_file(profile), "attestation renewal state")
    generation = 1
    previous_digest: str | None = None
    if previous is not None:
        _exact_keys(
            previous,
            {
                "schema_version",
                "sandbox",
                "candidate_sha",
                "candidate_tree",
                "renewal_generation",
                "renewal_payload_sha256",
                "combined_payload_sha256",
                "collected_at",
                "expires_at",
                "domain_generations",
            },
            "attestation renewal state",
        )
        prior_generation = previous.get("renewal_generation")
        previous_digest = previous.get("renewal_payload_sha256")
        previous_combined_digest = previous.get("combined_payload_sha256")
        previous_sha = previous.get("candidate_sha")
        previous_tree = previous.get("candidate_tree")
        previous_domains = previous.get("domain_generations")
        if (
            previous.get("schema_version") != 1
            or previous.get("sandbox") != profile.sandbox
            or not isinstance(previous_sha, str)
            or SHA_RE.fullmatch(previous_sha) is None
            or not isinstance(previous_tree, str)
            or SHA_RE.fullmatch(previous_tree) is None
            or type(prior_generation) is not int
            or prior_generation < 1
            or not isinstance(previous_digest, str)
            or DIGEST_RE.fullmatch(previous_digest) is None
            or not isinstance(previous_combined_digest, str)
            or DIGEST_RE.fullmatch(previous_combined_digest) is None
            or not isinstance(previous_domains, dict)
            or set(previous_domains) != {"oldlab", "gb10"}
            or any(
                type(previous_domains[domain]) is not int or previous_domains[domain] < 1
                for domain in ("oldlab", "gb10")
            )
        ):
            raise HostConvergeError("attestation renewal state is invalid")
        if previous_combined_digest == receipt.payload_sha256:
            raise HostConvergeError("attestation renewal replay did not produce fresh proof")
        previous_collected = _parse_attestation_time(
            previous.get("collected_at"),
            "previous renewal collected_at",
        )
        if collected_at <= previous_collected:
            raise HostConvergeError("attestation renewal time did not advance")
        if previous_sha == sha:
            if previous_tree != tree or any(
                domain_generations[domain] <= previous_domains[domain]
                for domain in ("oldlab", "gb10")
            ):
                raise HostConvergeError("domain attestation generation did not advance")
        generation = prior_generation + 1

    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-runtime-attestation-renewal",
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "renewal_generation": generation,
        "previous_payload_sha256": previous_digest,
        "collected_at": collected_at.isoformat(),
        "expires_at": receipt.expires_at.isoformat(),
        "domain_generations": domain_generations,
        "fleet_attestation": fleet_payload,
        "combined_receipt": combined,
    }
    payload = dict(unsigned)
    payload["payload_sha256"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(),
    ).hexdigest()
    content = (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode()
    history = (
        COMBINED_RECEIPT_ROOT
        / profile.sandbox
        / sha
        / "renewals"
        / f"{generation:020d}-{payload['payload_sha256']}.json"
    )
    _write_root_exclusive(history, content)
    state = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "renewal_generation": generation,
        "renewal_payload_sha256": payload["payload_sha256"],
        "combined_payload_sha256": receipt.payload_sha256,
        "collected_at": collected_at.isoformat(),
        "expires_at": receipt.expires_at.isoformat(),
        "domain_generations": domain_generations,
    }
    _ensure_root_private_directory(RENEWAL_STATE_ROOT)
    _atomic_write(
        _renewal_state_file(profile),
        (json.dumps(state, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )
    return {
        "path": str(history),
        "payload_sha256": payload["payload_sha256"],
        "previous_payload_sha256": previous_digest,
        "renewal_generation": generation,
        "collected_at": collected_at.isoformat(),
        "expires_at": receipt.expires_at.isoformat(),
        "domain_generations": domain_generations,
    }


def _archived_activation_from_path(
    profile: Profile,
    sha: str,
    tree: str,
    path: Path,
) -> tuple[int, ActivationReceipt]:
    raw = _read_combined_receipt_bytes(path)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("archived activation receipt is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("archived activation receipt is invalid")
    canonical = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        + b"\n"
    )
    _exact_keys(
        payload,
        {
            "schema_version",
            "kind",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "renewal_generation",
            "previous_payload_sha256",
            "collected_at",
            "expires_at",
            "domain_generations",
            "fleet_attestation",
            "combined_receipt",
            "payload_sha256",
        },
        "archived activation receipt",
    )
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    expected_digest = hashlib.sha256(
        json.dumps(
            unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    generation = payload["renewal_generation"]
    filename = RENEWAL_HISTORY_RE.fullmatch(path.name)
    previous_digest = payload["previous_payload_sha256"]
    if (
        raw != canonical
        or payload["schema_version"] != 1
        or payload["kind"] != "loom.developer-runtime-attestation-renewal"
        or payload["sandbox"] != profile.sandbox
        or payload["candidate_sha"] != sha
        or payload["candidate_tree"] != tree
        or type(generation) is not int
        or generation < 1
        or not isinstance(payload["payload_sha256"], str)
        or payload["payload_sha256"] != expected_digest
        or filename is None
        or int(filename.group(1)) != generation
        or filename.group(2) != expected_digest
        or (
            previous_digest is not None
            and (
                not isinstance(previous_digest, str) or DIGEST_RE.fullmatch(previous_digest) is None
            )
        )
    ):
        raise HostConvergeError("archived activation receipt binding is invalid")

    collected_at = _parse_attestation_time(payload["collected_at"], "collected_at")
    expires_at = _parse_attestation_time(payload["expires_at"], "expires_at")
    if expires_at <= collected_at or expires_at - collected_at > ATTESTATION_TTL:
        raise HostConvergeError("archived activation receipt lifetime is invalid")

    domain_generations = payload["domain_generations"]
    combined = payload["combined_receipt"]
    fleet = payload["fleet_attestation"]
    if (
        not isinstance(domain_generations, dict)
        or set(domain_generations) != {"oldlab", "gb10"}
        or any(
            type(domain_generations[domain]) is not int or domain_generations[domain] < 1
            for domain in ("oldlab", "gb10")
        )
        or not isinstance(combined, dict)
        or not isinstance(fleet, dict)
    ):
        raise HostConvergeError("archived activation receipt sections are invalid")

    combined_digest = combined.get("payload_sha256")
    combined_unsigned = {key: value for key, value in combined.items() if key != "payload_sha256"}
    expected_combined_digest = hashlib.sha256(
        json.dumps(
            combined_unsigned,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode(),
    ).hexdigest()
    collector = combined.get("collector")
    combined_domains = combined.get("domains")
    fleet_reference = combined.get("fleet_attestation")
    if (
        combined.get("schema_version") != 1
        or combined.get("kind") != "loom.developer-runtime-combined-activation"
        or combined.get("sandbox") != profile.sandbox
        or combined.get("candidate_sha") != sha
        or combined.get("candidate_tree") != tree
        or not isinstance(combined_digest, str)
        or combined_digest != expected_combined_digest
        or not isinstance(collector, dict)
        or collector.get("hostname") != EXPECTED_HOSTNAME
        or collector.get("collected_at") != payload["collected_at"]
        or collector.get("expires_at") != payload["expires_at"]
        or not isinstance(combined_domains, dict)
        or set(combined_domains) != {"oldlab", "gb10"}
        or any(
            not isinstance(combined_domains[domain], dict)
            or combined_domains[domain].get("generation") != domain_generations[domain]
            for domain in ("oldlab", "gb10")
        )
        or not isinstance(fleet_reference, dict)
    ):
        raise HostConvergeError("archived combined receipt binding is invalid")

    fleet_unsigned = {key: value for key, value in fleet.items() if key != "payload_sha256"}
    expected_fleet_digest = (
        "sha256:"
        + hashlib.sha256(
            json.dumps(
                fleet_unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
    )
    fleet_nodes = fleet.get("nodes")
    fleet_bundle = fleet.get("bundle_generation")
    fleet_server = fleet.get("server")
    if (
        fleet.get("sandbox") != profile.sandbox
        or fleet.get("candidate_sha") != sha
        or fleet.get("payload_sha256") != expected_fleet_digest
        or fleet_reference.get("path")
        != str(FLEET_ATTESTATION_ROOT / profile.sandbox / sha / "fleet.json")
        or fleet_reference.get("payload_sha256") != expected_fleet_digest
        or fleet_reference.get("generated_at") != fleet.get("generated_at")
        or fleet_reference.get("expires_at") != fleet.get("expires_at")
        or fleet.get("eligible_nodes") != list(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_nodes, dict)
        or set(fleet_nodes) != set(ELIGIBLE_LINK_NODES)
        or not isinstance(fleet_bundle, dict)
        or fleet_bundle.get("candidate_sha") != sha
        or not isinstance(fleet_server, dict)
        or fleet_server.get("active_candidate_sha") != sha
        or fleet_server.get("node") != "oldlab-2"
        or fleet_server.get("unit_active") is not True
        or any(
            not isinstance(node, dict) or node.get("candidate_sha") != sha
            for node in fleet_nodes.values()
        )
    ):
        raise HostConvergeError("archived fleet attestation binding is invalid")
    fleet_generated = _parse_attestation_time(fleet.get("generated_at"), "fleet generated_at")
    fleet_expires = _parse_attestation_time(fleet.get("expires_at"), "fleet expires_at")
    if (
        fleet_generated > collected_at + timedelta(seconds=30)
        or collected_at - fleet_generated > RECEIPT_FRESHNESS
        or fleet_expires - fleet_generated != ATTESTATION_TTL
        or fleet_expires < expires_at
    ):
        raise HostConvergeError("archived fleet attestation lifetime is invalid")
    return generation, ActivationReceipt(
        path=combined_receipt_path(profile, sha),
        payload_sha256=combined_digest,
        fleet_payload_sha256=expected_fleet_digest,
        expires_at=expires_at,
    )


def _verify_archived_activation(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    desired: Mapping[str, Any] | None = None,
) -> ActivationReceipt:
    history_root = COMBINED_RECEIPT_ROOT / profile.sandbox / sha / "renewals"
    descriptor = -1
    try:
        descriptor = _open_absolute_directory(history_root, create=False)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise HostConvergeError("archived activation directory is unsafe")
        names = sorted(os.listdir(descriptor))
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError("archived activation history is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not names or any(RENEWAL_HISTORY_RE.fullmatch(name) is None for name in names):
        raise HostConvergeError("archived activation history is invalid")
    receipts = [
        _archived_activation_from_path(profile, sha, tree, history_root / name) for name in names
    ]
    receipts.sort(key=lambda item: item[0], reverse=True)
    if desired is None:
        return receipts[0][1]
    for _generation, receipt in receipts:
        try:
            _validate_desired_binding(
                profile,
                desired,
                sha=sha,
                tree=tree,
                receipt=receipt,
            )
        except HostConvergeError:
            continue
        return receipt
    raise HostConvergeError("desired state has no matching archived activation")


def _desired_payload(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    previous_sha: str | None,
    receipt: ActivationReceipt,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_path": str(profile.candidate_root / sha),
        "previous_sha": previous_sha,
        "worker_runtime_env": str(profile.worker_runtime_env(sha)),
        "combined_receipt": str(receipt.path),
        "combined_receipt_sha256": receipt.payload_sha256,
        "fleet_attestation_sha256": receipt.fleet_payload_sha256,
        "receipt_expires_at": receipt.expires_at.isoformat(),
        "secrets_env": str(profile.secrets_env),
        "admin_secret_file": str(profile.admin_secret),
    }


def _load_json(path: Path, label: str) -> dict[str, Any] | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HostConvergeError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise HostConvergeError(f"{label} is unsafe")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HostConvergeError(f"{label} is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError(f"{label} is invalid")
    return payload


def write_desired(
    profile: Profile,
    sha: str,
    tree: str,
    receipt: ActivationReceipt,
) -> dict[str, Any] | None:
    previous = _load_json(profile.desired_file, "sandbox desired state")
    previous_sha = None
    if previous is not None:
        current = previous.get("candidate_sha")
        if isinstance(current, str) and current != sha:
            previous_sha = current
        elif isinstance(previous.get("previous_sha"), str):
            previous_sha = previous["previous_sha"]
    payload = _desired_payload(
        profile,
        sha,
        tree,
        previous_sha=previous_sha,
        receipt=receipt,
    )
    _atomic_write(
        profile.desired_file,
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )
    return previous


def _validate_installer_runtime_directory() -> None:
    try:
        metadata = TRANSACTION_LOCK_ROOT.lstat()
    except OSError as exc:
        raise HostConvergeError("sandbox installer runtime directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise HostConvergeError("sandbox installer runtime directory is unsafe")


@dataclass(frozen=True)
class _InstallerRuntimeSnapshot:
    present: bool
    device: int | None
    inode: int | None
    uid: int | None
    gid: int | None
    mode: int | None


@dataclass(frozen=True)
class _InstallerTmpfilesSnapshot:
    config: bytes | None
    runtime: _InstallerRuntimeSnapshot


def _snapshot_installer_runtime_directory() -> _InstallerRuntimeSnapshot:
    try:
        metadata = TRANSACTION_LOCK_ROOT.lstat()
    except FileNotFoundError:
        return _InstallerRuntimeSnapshot(False, None, None, None, None, None)
    except OSError as exc:
        raise HostConvergeError("sandbox installer runtime directory is unavailable") from exc
    _validate_installer_runtime_directory()
    return _InstallerRuntimeSnapshot(
        True,
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    )


def _validate_restored_installer_runtime(snapshot: _InstallerRuntimeSnapshot) -> None:
    try:
        metadata = TRANSACTION_LOCK_ROOT.lstat()
    except FileNotFoundError:
        if snapshot.present:
            raise HostConvergeError(
                "sandbox installer prior runtime directory was not restored",
            ) from None
        return
    except OSError as exc:
        raise HostConvergeError("sandbox installer runtime directory is unavailable") from exc
    _validate_installer_runtime_directory()
    if snapshot.present and (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
    ) != (
        snapshot.device,
        snapshot.inode,
        snapshot.uid,
        snapshot.gid,
        snapshot.mode,
    ):
        raise HostConvergeError("sandbox installer prior runtime directory drifted")


def _read_optional_tmpfiles_asset(path: Path) -> bytes | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HostConvergeError("sandbox installer tmpfiles asset is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
        or stat.S_IMODE(metadata.st_mode) != 0o644
        or metadata.st_nlink != 1
    ):
        raise HostConvergeError("sandbox installer tmpfiles asset is unsafe")
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise HostConvergeError("sandbox installer tmpfiles asset is unavailable") from exc
    if len(payload) > 64 * 1024:
        raise HostConvergeError("sandbox installer tmpfiles asset is unsafe")
    return payload


def _restore_tmpfiles_asset(snapshot: _InstallerTmpfilesSnapshot) -> None:
    if snapshot.config is None:
        INSTALLER_TMPFILES_PATH.unlink(missing_ok=True)
        _fsync_directory(INSTALLER_TMPFILES_PATH.parent)
    else:
        _atomic_write(INSTALLER_TMPFILES_PATH, snapshot.config, mode=0o644)
        _run(("systemd-tmpfiles", "--create", str(INSTALLER_TMPFILES_PATH)))
    if _read_optional_tmpfiles_asset(INSTALLER_TMPFILES_PATH) != snapshot.config:
        raise HostConvergeError("sandbox installer prior tmpfiles asset was not restored")
    # A runtime root created while no prior policy existed may already contain
    # live lock files. Preserve it, but require the exact safe metadata. Never
    # delete a shared volatile lock root during rollback.
    _validate_restored_installer_runtime(snapshot.runtime)


def _install_tmpfiles_asset(source: Path) -> _InstallerTmpfilesSnapshot:
    expected = b"d /run/loom-developer-sandbox-installer 0700 root root -\n"
    try:
        payload = source.read_bytes()
    except OSError as exc:
        raise HostConvergeError("sandbox installer tmpfiles source is unavailable") from exc
    if payload != expected:
        raise HostConvergeError("sandbox installer tmpfiles source is invalid")
    snapshot = _InstallerTmpfilesSnapshot(
        config=_read_optional_tmpfiles_asset(INSTALLER_TMPFILES_PATH),
        runtime=_snapshot_installer_runtime_directory(),
    )
    try:
        _atomic_write(INSTALLER_TMPFILES_PATH, payload, mode=0o644)
        _run(("systemd-tmpfiles", "--create", str(INSTALLER_TMPFILES_PATH)))
        installed = _read_optional_tmpfiles_asset(INSTALLER_TMPFILES_PATH)
        if installed != payload:
            raise HostConvergeError("sandbox installer tmpfiles install drifted")
        _validate_installer_runtime_directory()
        return snapshot
    except Exception as install_exc:
        try:
            _restore_tmpfiles_asset(snapshot)
        except Exception as rollback_exc:
            raise HostConvergeError(
                "sandbox installer tmpfiles install and rollback both failed safely",
            ) from rollback_exc
        raise HostConvergeError(
            "sandbox installer tmpfiles install failed and was rolled back",
        ) from install_exc


def _install_assets(source_root: Path) -> None:
    _ensure_root_private_directory(CONFIG_ROOT)
    _ensure_root_private_directory(DESIRED_ROOT)
    _ensure_root_private_directory(PROFILE_CONFIG_ROOT)
    profiles_root = source_root / "deploy/developer-sandboxes"
    _atomic_write(
        INSTALLED_PROGRAM,
        (source_root / "scripts/ops/developer_sandbox_host.py").read_bytes(),
        mode=0o755,
    )
    _atomic_write(
        UNIT_PATH,
        (profiles_root / "loom-developer-sandbox@.service").read_bytes(),
        mode=0o644,
    )
    _atomic_write(
        RENEWAL_SERVICE_PATH,
        (profiles_root / "loom-developer-sandbox-attestation-renewal.service").read_bytes(),
        mode=0o644,
    )
    _atomic_write(
        RENEWAL_TIMER_PATH,
        (profiles_root / "loom-developer-sandbox-attestation-renewal.timer").read_bytes(),
        mode=0o644,
    )
    for sandbox in LEGACY_SEED_RUNTIME_IDS:
        _atomic_write(
            PROFILE_CONFIG_ROOT / f"{sandbox}.toml",
            (profiles_root / f"{sandbox}.toml").read_bytes(),
            mode=0o600,
        )
    tmpfiles_snapshot = _install_tmpfiles_asset(
        profiles_root / "loom-developer-sandbox-installer.tmpfiles.conf",
    )
    try:
        _run(("systemctl", "daemon-reload"))
    except Exception as reload_exc:
        try:
            _restore_tmpfiles_asset(tmpfiles_snapshot)
        except Exception as rollback_exc:
            raise HostConvergeError(
                "sandbox installer asset reload and tmpfiles rollback both failed safely",
            ) from rollback_exc
        raise HostConvergeError(
            "sandbox installer asset reload failed and tmpfiles was rolled back",
        ) from reload_exc


def _read_admin_token(path: Path) -> str:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
        token = payload["admin"]["token"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("sandbox admin secret is invalid") from exc
    if not isinstance(token, str):
        raise HostConvergeError("sandbox admin secret is invalid")
    return token


def _request_json(
    url: str,
    *,
    token: str | None,
    expected: set[int],
) -> tuple[int, dict[str, Any]]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=b"{}",
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read()
    except urllib.error.URLError as exc:
        raise HostConvergeError("sandbox Control Plane is unavailable") from exc
    if status not in expected:
        raise HostConvergeError(f"sandbox Control Plane returned unexpected status {status}")
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError as exc:
        raise HostConvergeError("sandbox Control Plane returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("sandbox Control Plane returned invalid JSON")
    return status, payload


def _wait_for_control_plane(profile: Profile) -> None:
    url = f"http://127.0.0.1:{profile.ports['control_plane']}/healthz"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(1)
    raise HostConvergeError("sandbox Control Plane did not become healthy")


def _update_secret_tokens(
    profile: Profile,
    identity: Identity,
    updates: Mapping[str, str],
) -> None:
    _assert_secure_file(profile.secrets_env, identity, "sandbox secret env file")
    values = _parse_env_file(profile.secrets_env)
    values.update(updates)
    _atomic_write(
        profile.secrets_env,
        _render_env(values),
        mode=0o600,
        identity=identity,
    )


def bootstrap_runtime_tokens(profile: Profile, identity: Identity) -> bool:
    _wait_for_control_plane(profile)
    values = _parse_env_file(profile.secrets_env)
    worker_token = values.get("LOOM_WORKER_TOKEN", "")
    register_url = f"http://127.0.0.1:{profile.ports['control_plane']}/workers/register"
    worker_status, _ = _request_json(
        register_url,
        token=worker_token,
        expected={400, 401},
    )
    batch_token = values.get("LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "")
    if worker_status == 400 and batch_token:
        return False

    admin_token = _read_admin_token(profile.admin_secret)
    base = f"http://127.0.0.1:{profile.ports['control_plane']}/admin"
    updates: dict[str, str] = {}
    if worker_status == 401:
        _, worker_payload = _request_json(
            f"{base}/worker-tokens",
            token=admin_token,
            expected={201},
        )
        raw_worker = worker_payload.get("token")
        if not isinstance(raw_worker, str) or not raw_worker.startswith("loom_w_"):
            raise HostConvergeError("Control Plane returned an invalid worker token")
        updates["LOOM_WORKER_TOKEN"] = raw_worker
    if not batch_token or worker_status == 401:
        _, batch_payload = _request_json(
            f"{base}/batch-runner-tokens",
            token=admin_token,
            expected={201},
        )
        raw_batch = batch_payload.get("token")
        if not isinstance(raw_batch, str) or not raw_batch.startswith("loom_br_"):
            raise HostConvergeError("Control Plane returned an invalid batch token")
        updates["LOOM_SVC_BATCH_RUNNER_CP_TOKEN"] = raw_batch
    _update_secret_tokens(profile, identity, updates)
    return bool(updates)


def _candidate_environment(profile: Profile, candidate: Path) -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "HOME": str(profile.private_runtime_root),
        "PYTHONPATH": str(candidate / "src"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": str(candidate),
        "GIT_NO_REPLACE_OBJECTS": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _invoke_lifecycle(profile: Profile, sha: str, operation: str) -> None:
    candidate = profile.candidate_root / sha
    program = candidate / "scripts/ops/developer_sandbox.py"
    profile_path = candidate / f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    owner = _identity(profile.sandbox, SHARED_GROUP)
    if (
        _run(
            ("test", "-f", str(program)),
            identity=owner,
            init_groups=True,
            expected={0, 1},
        ).returncode
        != 0
        or _run(
            ("test", "-f", str(profile_path)),
            identity=owner,
            init_groups=True,
            expected={0, 1},
        ).returncode
        != 0
    ):
        raise HostConvergeError("candidate sandbox lifecycle assets are unavailable")
    _run(
        (
            sys.executable,
            str(program),
            operation,
            "--profile",
            str(profile_path),
            "--source-repo",
            str(candidate),
            "--candidate-sha",
            sha,
            "--secrets-env",
            str(profile.secrets_env),
            "--admin-secret-file",
            str(profile.admin_secret),
            "--execute",
        ),
        env=_candidate_environment(profile, candidate),
        identity=owner,
        init_groups=True,
    )


def _desired_for_service(sandbox: str) -> tuple[Profile, dict[str, Any]]:
    profile = _load_profile(PROFILE_CONFIG_ROOT / f"{sandbox}.toml")
    desired = _load_json(profile.desired_file, "sandbox desired state")
    if desired is None or desired.get("sandbox") != sandbox:
        raise HostConvergeError("sandbox desired state is absent or invalid")
    return profile, desired


def _sandbox_state_sha(profile: Profile) -> str | None:
    state = _load_json(profile.state_file, "sandbox lifecycle state")
    if state is None:
        return None
    sha = state.get("candidate_sha")
    if not isinstance(sha, str) or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("sandbox lifecycle state SHA is invalid")
    return sha


def _validate_desired_binding(
    profile: Profile,
    desired: Mapping[str, Any],
    *,
    sha: str,
    tree: str,
    receipt: ActivationReceipt,
) -> None:
    expected = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_path": str(profile.candidate_root / sha),
        "worker_runtime_env": str(profile.worker_runtime_env(sha)),
        "combined_receipt": str(receipt.path),
        "combined_receipt_sha256": receipt.payload_sha256,
        "fleet_attestation_sha256": receipt.fleet_payload_sha256,
        "receipt_expires_at": receipt.expires_at.isoformat(),
        "secrets_env": str(profile.secrets_env),
        "admin_secret_file": str(profile.admin_secret),
    }
    if any(desired.get(key) != value for key, value in expected.items()):
        raise HostConvergeError("sandbox desired state binding is invalid")
    previous = desired.get("previous_sha")
    if previous is not None and (
        not isinstance(previous, str) or SHA_RE.fullmatch(previous) is None or previous == sha
    ):
        raise HostConvergeError("sandbox desired rollback binding is invalid")


def _renew_attestation_locked(
    profile: Profile,
    *,
    sha: str,
    tree: str,
) -> ActivationReceipt:
    _collect_and_persist_remote_link_fleet(profile, sha, tree)
    _publish_domain_attestations(profile, sha, tree)
    receipt = verify_combined_receipt(profile, sha, tree)
    _archive_runtime_attestation(profile, sha, tree, receipt)
    write_desired(profile, sha, tree, receipt)
    return receipt


def _service_converge_locked(
    profile: Profile,
    desired: Mapping[str, Any],
) -> None:
    sandbox = profile.sandbox
    sha = str(desired["candidate_sha"])
    authority = _identity("root", SHARED_GROUP)
    owner = _identity(sandbox, SHARED_GROUP)
    runtime_group = _sandbox_batch_identity(sandbox)
    verify_candidate_root(profile, authority)
    tree = verify_candidate(profile, profile.candidate_root / sha, sha, authority)
    verify_worker_runtime_env(profile, sha, runtime_group)
    try:
        receipt = verify_combined_receipt(profile, sha, tree)
        _validate_desired_binding(
            profile,
            desired,
            sha=sha,
            tree=tree,
            receipt=receipt,
        )
    except HostConvergeError:
        receipt = _verify_archived_activation(
            profile,
            sha,
            tree,
            desired=desired,
        )
        _validate_desired_binding(
            profile,
            desired,
            sha=sha,
            tree=tree,
            receipt=receipt,
        )
    ensure_secret_files(profile, owner)
    current = _sandbox_state_sha(profile)
    if current is None:
        _invoke_lifecycle(profile, sha, "create")
    else:
        _invoke_lifecycle(profile, sha, "update")
    if bootstrap_runtime_tokens(profile, owner):
        _invoke_lifecycle(profile, sha, "update")
    _invoke_lifecycle(profile, sha, "check")
    verify_listening_ports(profile)


def service_converge(sandbox: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    profile, _desired = _desired_for_service(sandbox)
    with _activation_lock(profile):
        transaction = _transaction_payload(profile)
        if transaction is not None and transaction["phase"] != "desired-written":
            _recover_transaction(profile, transaction)
            transaction = None
        locked_profile, desired = _desired_for_service(sandbox)
        if transaction is not None and desired.get("candidate_sha") != transaction.get(
            "candidate_sha",
        ):
            raise HostConvergeError(
                "sandbox desired state does not match pending activation transaction",
            )
        _service_converge_locked(locked_profile, desired)
        if transaction is not None and transaction["operation"] != "rollback":
            _write_transaction(
                profile,
                operation=str(transaction["operation"]),
                sha=str(transaction["candidate_sha"]),
                tree=str(transaction["candidate_tree"]),
                phase="committed",
                previous_desired=transaction["previous_desired"],
                previous_relay_sha=transaction["previous_relay_sha"],
            )
            _remove_transaction(profile)


def service_check(sandbox: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    profile, desired = _desired_for_service(sandbox)
    sha = str(desired["candidate_sha"])
    authority = _identity("root", SHARED_GROUP)
    verify_candidate_root(profile, authority)
    tree = verify_candidate(
        profile,
        profile.candidate_root / sha,
        sha,
        authority,
    )
    verify_worker_runtime_env(profile, sha, _sandbox_batch_identity(sandbox))
    receipt = verify_combined_receipt(profile, sha, tree)
    _validate_desired_binding(
        profile,
        desired,
        sha=sha,
        tree=tree,
        receipt=receipt,
    )
    verify_secret_files(profile, _identity(sandbox, SHARED_GROUP))
    _invoke_lifecycle(profile, sha, "check")
    verify_listening_ports(profile)


def renew_attestations(profiles: Sequence[Profile], *, execute: bool) -> None:
    if not execute:
        raise HostConvergeError("attestation renewal requires --execute")
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    renewed: list[str] = []
    with _install_lock():
        for profile in profiles:
            with _activation_lock(profile):
                desired = _load_json(profile.desired_file, "sandbox desired state")
                if desired is None:
                    continue
                sha = desired.get("candidate_sha")
                if not isinstance(sha, str) or SHA_RE.fullmatch(sha) is None:
                    raise HostConvergeError(
                        f"{profile.sandbox} desired candidate SHA is invalid",
                    )
                authority = _identity("root", SHARED_GROUP)
                verify_candidate_root(profile, authority)
                tree = verify_candidate(
                    profile,
                    profile.candidate_root / sha,
                    sha,
                    authority,
                )
                verify_worker_runtime_env(
                    profile,
                    sha,
                    _sandbox_batch_identity(profile.sandbox),
                )
                _renew_attestation_locked(profile, sha=sha, tree=tree)
                renewed.append(profile.sandbox)
    if not renewed:
        raise HostConvergeError("no installed sandbox desired state was found")


def verify_listening_ports(profile: Profile) -> None:
    result = _run(("ss", "-H", "-ltn"))
    listeners: set[tuple[str, int]] = set()
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        local = fields[3]
        host, separator, raw_port = local.rpartition(":")
        if not separator:
            continue
        try:
            port = int(raw_port)
        except ValueError:
            continue
        listeners.add((host.strip("[]"), port))
    missing = sorted(
        port for port in profile.ports.values() if ("127.0.0.1", port) not in listeners
    )
    if missing:
        raise HostConvergeError(
            "sandbox loopback ports are not listening: " + ", ".join(str(port) for port in missing),
        )


def _candidate_program(profile: Profile, sha: str, relative: str) -> Path:
    path = profile.candidate_root / sha / relative
    if not path.is_file() or path.is_symlink():
        raise HostConvergeError("exact candidate operation asset is unavailable")
    return path


def _run_candidate_program(
    profile: Profile,
    sha: str,
    relative: str,
    *arguments: str,
) -> dict[str, Any]:
    completed = _run(
        (
            sys.executable,
            str(_candidate_program(profile, sha, relative)),
            *arguments,
        ),
        env=_candidate_environment(profile, profile.candidate_root / sha),
    )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("exact candidate helper returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("exact candidate helper returned invalid JSON")
    return payload


def _node_authority_envelope(
    *,
    action: str,
    node: str,
    domain: str,
    sandbox: str,
    sha: str,
    tree: str,
    payload_kind: str = "none",
    payload_bytes: bytes = b"",
    prior_request_id: str | None = None,
    profile: Profile | None = None,
) -> bytes:
    dynamic_actions = {
        "host-converge",
        "materialize",
        "install-client",
        "attest",
        "rollback",
        "persist-fleet-attestation",
        "inspect-candidate",
        "inspect-local",
        "inspect-link-client",
        "inspect-link-server",
        "export-domain-attestation",
        "export-runtime-proof-artifact",
        "collect-live-overlap",
        "observe-live-overlap-job",
        "observe-platform-health-node",
    }
    if (
        action
        not in {
            "host-converge",
            "materialize",
            "install-client",
            "attest",
            "rollback",
            "persist-fleet-attestation",
            "inspect-candidate",
            "inspect-local",
            "inspect-link-client",
            "inspect-link-server",
            "export-domain-attestation",
            "export-runtime-proof-artifact",
            "slurm-node-converge",
            "slurm-controller-converge",
            "slurm-rollback",
            "slurm-check",
            "collect-live-overlap",
            "observe-live-overlap-job",
            "observe-platform-health-node",
            "staging-allocation-bootstrap",
            "staging-allocation-probe",
            "staging-allocation-query",
            "staging-allocation-submit",
            "staging-allocation-cancel",
            "staging-shared-source-bootstrap",
            "staging-slurm-accounting-converge",
        }
        or node not in ELIGIBLE_LINK_NODES
        or domain not in DOMAIN_PEERS
        or (sandbox != "staging" if action.startswith("staging-") else False)
        or SHA_RE.fullmatch(sha) is None
        or SHA_RE.fullmatch(tree) is None
        or (prior_request_id is not None and DIGEST_RE.fullmatch(prior_request_id) is None)
    ):
        raise HostConvergeError("node authority request identity is invalid")
    body: dict[str, Any] = {
        "schema_version": 1,
        "action": action,
        "node": node,
        "domain": domain,
        "sandbox": sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "payload_kind": payload_kind,
        "payload_sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "payload_base64": base64.b64encode(payload_bytes).decode("ascii"),
        "prior_request_id": prior_request_id,
    }
    if action in dynamic_actions:
        if profile is not None and profile.env_id is None:
            profile = _registry_bound_profile(profile, sha=sha, tree=tree)
        if (
            profile is None
            or profile.sandbox != sandbox
            or profile.env_id is None
            or profile.resource_generation is None
            or profile.candidate_id is None
            or profile.registry_generation is None
            or profile.registry_payload_sha256 is None
            or DIGEST_RE.fullmatch(profile.registry_payload_sha256) is None
        ):
            raise HostConvergeError("node authority registry binding is incomplete")
        body.update(
            {
                "env_id": profile.env_id,
                "resource_generation": profile.resource_generation,
                "candidate_id": profile.candidate_id,
                "registry_generation": profile.registry_generation,
                "registry_payload_sha256": profile.registry_payload_sha256,
                "worker_image_id": profile.worker_image_id(domain),
            }
        )
    digest = hashlib.sha256(
        (
            json.dumps(
                body,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("ascii"),
    ).hexdigest()
    body["request_id"] = digest
    return (
        json.dumps(
            body,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )


def _registry_bound_profile(profile: Profile, *, sha: str, tree: str) -> Profile:
    path = environment_registry.SYSTEM_SNAPSHOT
    try:
        before = path.lstat()
        raw = path.read_bytes()
        after = path.lstat()
        snapshot = environment_registry.DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except (OSError, environment_registry.RegistryError) as exc:
        raise HostConvergeError("developer environment registry snapshot is invalid") from exc

    def identity(item: os.stat_result) -> tuple[int, int, int, int, int, int, int, int]:
        return (
            item.st_dev,
            item.st_ino,
            item.st_mode,
            item.st_uid,
            item.st_gid,
            item.st_nlink,
            item.st_size,
            item.st_mtime_ns,
        )

    if (
        identity(before) != identity(after)
        or stat.S_ISLNK(before.st_mode)
        or not stat.S_ISREG(before.st_mode)
        or (before.st_uid, before.st_gid) != (0, 0)
        or before.st_nlink != 1
        or stat.S_IMODE(before.st_mode) != 0o600
    ):
        raise HostConvergeError("developer environment registry snapshot metadata is unsafe")
    environments = [
        row
        for row in snapshot["environments"]
        if row["runtime_id"] == profile.sandbox and row["state"] in {"deploying", "active"}
    ]
    if len(environments) != 1:
        raise HostConvergeError("developer environment registry profile is unavailable")
    environment = environments[0]
    candidate_ids = {environment["current_candidate_id"]}
    candidate_ids.update(
        row["candidate_id"]
        for row in snapshot["deployments"]
        if row["env_id"] == environment["env_id"] and row["phase"] not in {"committed", "failed"}
    )
    candidates = [
        row
        for row in snapshot["candidates"]
        if row["candidate_id"] in candidate_ids
        and row["candidate_sha"] == sha
        and row["candidate_tree"] == tree
    ]
    if len(candidates) != 1:
        raise HostConvergeError("developer environment candidate profile is unavailable")
    bound_deployments = [
        row
        for row in snapshot["deployments"]
        if row["env_id"] == environment["env_id"]
        and row["candidate_id"] == candidates[0]["candidate_id"]
        and (
            (
                environment["state"] == "deploying"
                and row["phase"] not in {"committed", "failed"}
                and row["expected_resource_generation"] == environment["resource_generation"]
            )
            or (
                environment["state"] == "active"
                and row["phase"] == "committed"
                and row["applied_resource_generation"] == environment["resource_generation"]
            )
        )
        and isinstance(row.get("worker_runtime_bindings"), dict)
    ]
    deployment = bound_deployments[0] if len(bound_deployments) == 1 else None
    runtime_bindings = (
        deployment.get("worker_runtime_bindings") if isinstance(deployment, dict) else None
    )
    domains = runtime_bindings.get("domains") if isinstance(runtime_bindings, dict) else None
    worker_image_ids = (
        {domain: domains[domain].get("runtime_image_id") for domain in ("oldlab", "gb10")}
        if isinstance(domains, dict)
        and all(isinstance(domains.get(domain), dict) for domain in ("oldlab", "gb10"))
        else {}
    )
    if (
        set(worker_image_ids) != {"oldlab", "gb10"}
        or any(
            FINGERPRINT_RE.fullmatch(str(worker_image_ids[domain])) is None
            for domain in ("oldlab", "gb10")
        )
        or worker_image_ids["oldlab"] == worker_image_ids["gb10"]
    ):
        raise HostConvergeError("developer environment worker image binding is invalid")
    resource_generation = environment["resource_generation"]
    if environment["state"] == "deploying":
        prepared = [
            row
            for row in snapshot["deployments"]
            if row["env_id"] == environment["env_id"]
            and row["candidate_id"] == candidates[0]["candidate_id"]
            and row["phase"] == "verified"
            and row["applied_resource_generation"] is not None
        ]
        if prepared and (
            len(prepared) != 1
            or prepared[0]["applied_resource_generation"] != environment["resource_generation"] + 1
        ):
            raise HostConvergeError("developer environment prepared profile is invalid")
        if prepared:
            resource_generation = prepared[0]["applied_resource_generation"]
    return replace(
        profile,
        env_id=environment["env_id"],
        resource_generation=resource_generation,
        registry_generation=snapshot["generation"],
        registry_payload_sha256=snapshot["payload_sha256"],
        candidate_id=candidates[0]["candidate_id"],
        candidate_tree=tree,
        service_user=environment["service_user"],
        worker_image_ids={
            "oldlab": str(worker_image_ids["oldlab"]),
            "gb10": str(worker_image_ids["gb10"]),
        },
    )


def _node_authority(
    node: str,
    verb: str,
    envelope: bytes,
) -> dict[str, Any]:
    if verb not in {"transact", "check"}:
        raise HostConvergeError("node authority verb is invalid")
    result = subprocess.run(
        (
            str(NODE_TRANSPORT_PROGRAM),
            "invoke",
            "--node",
            node,
            "--verb",
            verb,
        ),
        input=envelope,
        env={
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "LANG": "C.UTF-8",
        },
        check=False,
        capture_output=True,
    )
    if result.returncode != 0 or result.stderr:
        raise HostConvergeError(f"node authority transport failed safely on {node}")
    try:
        report = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("node authority returned invalid JSON") from exc
    if not isinstance(report, dict) or report.get("status") != "succeeded":
        raise HostConvergeError("node authority returned an invalid receipt")
    return report


def _slurm_maintenance_file(domain: str, sandbox: str, sha: str) -> Path:
    if (
        domain not in DOMAIN_PEERS
        or sandbox not in LEGACY_SEED_RUNTIME_IDS
        or SHA_RE.fullmatch(sha) is None
    ):
        raise HostConvergeError("Slurm maintenance identity is invalid")
    return SLURM_MAINTENANCE_ROOT / f"{domain}-candidate-set.json"


def _ensure_slurm_maintenance_root(*, create: bool) -> bool:
    expected_uid, expected_gid = os.geteuid(), os.getegid()
    for directory in (SLURM_MAINTENANCE_ROOT.parent, SLURM_MAINTENANCE_ROOT):
        try:
            metadata = directory.lstat()
        except FileNotFoundError:
            if not create:
                return False
            try:
                directory.mkdir(mode=0o700)
                os.chown(directory, expected_uid, expected_gid)
                os.chmod(directory, 0o700)
                _fsync_directory(directory.parent)
                metadata = directory.lstat()
            except OSError as exc:
                raise HostConvergeError(
                    "Slurm maintenance state root could not be created safely",
                ) from exc
        except OSError as exc:
            raise HostConvergeError("Slurm maintenance state root is unavailable") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISDIR(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (expected_uid, expected_gid)
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise HostConvergeError("Slurm maintenance state root is unsafe")
    return True


def _load_slurm_maintenance_file(path: Path) -> dict[str, Any] | None:
    if path.parent != SLURM_MAINTENANCE_ROOT or not _ensure_slurm_maintenance_root(create=False):
        return None
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    try:
        lexical = path.lstat()
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HostConvergeError("Slurm maintenance journal is unavailable") from exc
    expected_uid, expected_gid = os.geteuid(), os.getegid()
    try:
        opened = os.fstat(descriptor)
        if (
            stat.S_ISLNK(lexical.st_mode)
            or not stat.S_ISREG(lexical.st_mode)
            or not stat.S_ISREG(opened.st_mode)
            or (lexical.st_uid, lexical.st_gid) != (expected_uid, expected_gid)
            or (opened.st_uid, opened.st_gid) != (expected_uid, expected_gid)
            or stat.S_IMODE(lexical.st_mode) != 0o600
            or stat.S_IMODE(opened.st_mode) != 0o600
            or lexical.st_nlink != 1
            or opened.st_nlink != 1
            or (lexical.st_dev, lexical.st_ino) != (opened.st_dev, opened.st_ino)
        ):
            raise HostConvergeError("Slurm maintenance journal metadata is unsafe")
        payloads: list[bytes] = []
        identities: list[tuple[int, ...]] = []
        for _attempt in range(2):
            os.lseek(descriptor, 0, os.SEEK_SET)
            content = bytearray()
            while True:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    break
                content.extend(chunk)
                if len(content) > 1024 * 1024:
                    raise HostConvergeError("Slurm maintenance journal is too large")
            current = os.fstat(descriptor)
            payloads.append(bytes(content))
            identities.append(
                (
                    current.st_dev,
                    current.st_ino,
                    current.st_mode,
                    current.st_uid,
                    current.st_gid,
                    current.st_nlink,
                    current.st_size,
                    current.st_mtime_ns,
                    current.st_ctime_ns,
                ),
            )
        rebound = path.lstat()
        rebound_identity = (
            rebound.st_dev,
            rebound.st_ino,
            rebound.st_mode,
            rebound.st_uid,
            rebound.st_gid,
            rebound.st_nlink,
            rebound.st_size,
            rebound.st_mtime_ns,
            rebound.st_ctime_ns,
        )
        if (
            payloads[0] != payloads[1]
            or hashlib.sha256(payloads[0]).digest() != hashlib.sha256(payloads[1]).digest()
            or identities[0] != identities[1]
            or identities[1] != rebound_identity
        ):
            raise HostConvergeError("Slurm maintenance journal changed during read")
    finally:
        os.close(descriptor)
    try:
        payload = json.loads(payloads[0])
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HostConvergeError("Slurm maintenance journal is invalid") from exc
    if not isinstance(payload, dict) or payloads[0] != (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii"):
        raise HostConvergeError("Slurm maintenance journal is invalid")
    return payload


@contextmanager
def _slurm_maintenance_lock(domain: str) -> Iterator[None]:
    if domain not in DOMAIN_PEERS:
        raise HostConvergeError("Slurm maintenance domain is invalid")
    _ensure_root_private_directory(TRANSACTION_LOCK_ROOT)
    path = TRANSACTION_LOCK_ROOT / f"slurm-{domain}.lock"
    descriptor = os.open(
        path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or (metadata.st_uid, metadata.st_gid) != (0, 0):
            raise HostConvergeError("Slurm maintenance lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _slurm_node_order(domain: str) -> tuple[str, ...]:
    controller = DOMAIN_PUBLISHERS[domain]
    return (
        *(node for node in DOMAIN_PEERS[domain] if node != controller),
        controller,
    )


def _slurm_maintenance_tree(profile: Profile, sha: str) -> str:
    authority = _identity("root", SHARED_GROUP)
    verify_candidate_root(profile, authority)
    return verify_candidate(profile, profile.candidate_root / sha, sha, authority)


def _slurm_candidate_set(
    *,
    generation: int = 1,
    convergence_id: str | None = None,
) -> tuple[dict[str, Any], bytes]:
    if generation < 1:
        raise HostConvergeError("Slurm candidate-set generation is invalid")
    bindings: dict[str, dict[str, str]] = {}
    for sandbox in LEGACY_SEED_RUNTIME_IDS:
        profile, desired = _desired_for_service(sandbox)
        sha = desired.get("candidate_sha")
        tree = desired.get("candidate_tree")
        if (
            not isinstance(sha, str)
            or SHA_RE.fullmatch(sha) is None
            or not isinstance(tree, str)
            or SHA_RE.fullmatch(tree) is None
        ):
            raise HostConvergeError("sandbox desired candidate set is invalid")
        authority = _identity("root", SHARED_GROUP)
        verify_candidate_root(profile, authority)
        if verify_candidate(profile, profile.candidate_root / sha, sha, authority) != tree:
            raise HostConvergeError("sandbox desired candidate tree drifted")
        receipt = verify_combined_receipt(profile, sha, tree)
        _validate_desired_binding(
            profile,
            desired,
            sha=sha,
            tree=tree,
            receipt=receipt,
        )
        bindings[f"loom-dev-{sandbox}"] = {
            "sandbox": sandbox,
            "service_user": f"loom-sandbox-{sandbox}",
            "candidate_sha": sha,
            "candidate_tree": tree,
        }
    if len({row["candidate_sha"] for row in bindings.values()}) != len(LEGACY_SEED_RUNTIME_IDS):
        raise HostConvergeError("sandbox desired candidate SHAs must be pairwise distinct")
    bindings_bytes = json.dumps(
        bindings,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("ascii")
    payload = {
        "schema_version": 2,
        "kind": "loom.developer-sandbox.slurm-candidate-set",
        "candidate_set_sha256": hashlib.sha256(bindings_bytes).hexdigest(),
        "candidate_bindings": bindings,
        "generation": generation,
        "convergence_id": convergence_id or hashlib.sha256(bindings_bytes).hexdigest(),
    }
    return (
        payload,
        (
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("ascii"),
    )


def _new_slurm_maintenance_state(
    profile: Profile,
    *,
    domain: str,
    sha: str,
    tree: str,
    candidate_set: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if candidate_set is None:
        bindings = {
            f"loom-dev-{sandbox}": {
                "sandbox": sandbox,
                "service_user": f"loom-sandbox-{sandbox}",
                "candidate_sha": candidate,
                "candidate_tree": tree,
            }
            for sandbox, candidate in zip(
                LEGACY_SEED_RUNTIME_IDS,
                (sha, "c" * 40, "d" * 40),
                strict=True,
            )
        }
        candidate_set = {
            "candidate_set_sha256": hashlib.sha256(
                json.dumps(bindings, sort_keys=True, separators=(",", ":")).encode("ascii"),
            ).hexdigest(),
            "candidate_bindings": bindings,
            "generation": 1,
            "convergence_id": "e" * 64,
        }
    order = _slurm_node_order(domain)
    controller = DOMAIN_PUBLISHERS[domain]
    return {
        "schema_version": 2,
        "artifact_type": "developer-sandbox-slurm-maintenance-journal",
        "domain": domain,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_set_sha256": candidate_set["candidate_set_sha256"],
        "candidate_bindings": candidate_set["candidate_bindings"],
        "generation": candidate_set["generation"],
        "convergence_id": candidate_set["convergence_id"],
        "controller": controller,
        "node_order": list(order),
        "phase": "running",
        "nodes": {
            node: {
                "converge_action": (
                    "slurm-controller-converge" if node == controller else "slurm-node-converge"
                ),
                "converge_receipt": None,
                "check_request_id": None,
                "rollback_receipt": None,
            }
            for node in order
        },
        "last_failure": None,
        "updated_at": datetime.now(UTC).isoformat(),
    }


def _validate_slurm_maintenance_state(
    payload: object,
    profile: Profile,
    *,
    domain: str,
    sha: str,
    tree: str,
    candidate_set: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if candidate_set is None:
        candidate_set = _new_slurm_maintenance_state(
            profile,
            domain=domain,
            sha=sha,
            tree=tree,
        )
    order = _slurm_node_order(domain)
    controller = DOMAIN_PUBLISHERS[domain]
    if (
        not isinstance(payload, dict)
        or set(payload)
        != {
            "schema_version",
            "artifact_type",
            "domain",
            "sandbox",
            "candidate_sha",
            "candidate_tree",
            "candidate_set_sha256",
            "candidate_bindings",
            "generation",
            "convergence_id",
            "controller",
            "node_order",
            "phase",
            "nodes",
            "last_failure",
            "updated_at",
        }
        or payload.get("schema_version") != 2
        or payload.get("artifact_type") != "developer-sandbox-slurm-maintenance-journal"
        or payload.get("domain") != domain
        or payload.get("sandbox") != profile.sandbox
        or payload.get("candidate_sha") != sha
        or payload.get("candidate_tree") != tree
        or payload.get("candidate_set_sha256") != candidate_set.get("candidate_set_sha256")
        or payload.get("candidate_bindings") != candidate_set.get("candidate_bindings")
        or payload.get("generation") != candidate_set.get("generation")
        or payload.get("convergence_id") != candidate_set.get("convergence_id")
        or payload.get("controller") != controller
        or payload.get("node_order") != list(order)
        or payload.get("phase")
        not in {"running", "blocked", "completed", "rolling-back", "rolled-back"}
        or not isinstance(payload.get("nodes"), dict)
        or set(payload["nodes"]) != set(order)
        or not isinstance(payload.get("updated_at"), str)
    ):
        raise HostConvergeError("Slurm maintenance journal binding drifted")
    for node in order:
        row = payload["nodes"][node]
        expected_action = (
            "slurm-controller-converge" if node == controller else "slurm-node-converge"
        )
        if (
            not isinstance(row, dict)
            or set(row)
            != {
                "converge_action",
                "converge_receipt",
                "check_request_id",
                "rollback_receipt",
            }
            or row.get("converge_action") != expected_action
            or (
                row.get("check_request_id") is not None
                and DIGEST_RE.fullmatch(str(row["check_request_id"])) is None
            )
            or (
                row.get("converge_receipt") is not None
                and not isinstance(row["converge_receipt"], dict)
            )
            or (
                row.get("rollback_receipt") is not None
                and not isinstance(row["rollback_receipt"], dict)
            )
        ):
            raise HostConvergeError("Slurm maintenance node journal binding drifted")
    failure = payload["last_failure"]
    if failure is not None and (
        not isinstance(failure, dict)
        or set(failure) != {"node", "action", "failed_at"}
        or failure.get("node") not in order
        or not isinstance(failure.get("action"), str)
        or not isinstance(failure.get("failed_at"), str)
    ):
        raise HostConvergeError("Slurm maintenance failure journal is invalid")
    return payload


def _write_slurm_maintenance_state(
    path: Path,
    state: dict[str, Any],
) -> None:
    state["updated_at"] = datetime.now(UTC).isoformat()
    _ensure_slurm_maintenance_root(create=True)
    _atomic_write(
        path,
        (json.dumps(state, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n").encode(
            "ascii"
        ),
        mode=0o600,
    )
    if _load_slurm_maintenance_file(path) != state:
        raise HostConvergeError("Slurm maintenance journal write readback drifted")


def _slurm_authority_request(
    profile: Profile,
    *,
    domain: str,
    node: str,
    action: str,
    sha: str,
    tree: str,
    candidate_set_bytes: bytes | None = None,
    prior_request_id: str | None = None,
) -> tuple[dict[str, Any], str]:
    if candidate_set_bytes is None:
        candidate_set, candidate_set_bytes = _slurm_candidate_set()
    else:
        try:
            candidate_set = json.loads(candidate_set_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HostConvergeError("Slurm candidate-set payload is invalid") from exc
    qianyi = candidate_set["candidate_bindings"]["loom-dev-qianyi"]
    if sha != qianyi["candidate_sha"] or tree != qianyi["candidate_tree"]:
        raise HostConvergeError("Slurm maintenance source is not the candidate-set authority")
    envelope = _node_authority_envelope(
        action=action,
        node=node,
        domain=domain,
        sandbox=profile.sandbox,
        sha=sha,
        tree=tree,
        payload_kind="slurm-candidate-set-json",
        payload_bytes=candidate_set_bytes,
        prior_request_id=prior_request_id,
    )
    request_id = str(json.loads(envelope)["request_id"])
    verb = "check" if action == "slurm-check" else "transact"
    response = _node_authority(node, verb, envelope)
    binding = SLURM_AUTHORITY_BINDING_RE.fullmatch(
        str(response.get("inner_receipt")),
    )
    expected_cluster = "trt-oldlab" if domain == "oldlab" else "trt-gb10"
    if response.get("request_id") != request_id or (
        verb == "transact"
        and (
            set(response) != SLURM_AUTHORITY_RECEIPT_FIELDS
            or response.get("schema_version") != 1
            or response.get("action") != action
            or response.get("node") != node
            or response.get("domain") != domain
            or response.get("sandbox") != profile.sandbox
            or response.get("candidate_sha") != sha
            or response.get("candidate_tree") != tree
            or response.get("payload_sha256") != hashlib.sha256(candidate_set_bytes).hexdigest()
            or DIGEST_RE.fullmatch(str(response.get("result_sha256"))) is None
            or binding is None
            or binding.group(1) != expected_cluster
            or not isinstance(response.get("completed_at"), str)
        )
    ):
        raise HostConvergeError("Slurm node authority receipt binding is invalid")
    return response, request_id


def _slurm_check_node(
    profile: Profile,
    *,
    domain: str,
    node: str,
    sha: str,
    tree: str,
    candidate_set_bytes: bytes,
) -> str:
    response, request_id = _slurm_authority_request(
        profile,
        domain=domain,
        node=node,
        action="slurm-check",
        sha=sha,
        tree=tree,
        candidate_set_bytes=candidate_set_bytes,
    )
    result = response.get("result")
    expected_cluster = "trt-oldlab" if domain == "oldlab" else "trt-gb10"
    if (
        not isinstance(result, dict)
        or result.get("cluster") != expected_cluster
        or result.get("candidate_sha") != sha
        or result.get("file_plan", {}).get("converged") is not True
    ):
        raise HostConvergeError("Slurm node check readback is invalid")
    return request_id


def slurm_maintenance_converge(profile: Profile, sha: str, domain: str) -> dict[str, Any]:
    _require_live_host()
    with _install_lock():
        return _slurm_maintenance_converge_locked(profile, sha, domain)


def _slurm_maintenance_converge_locked(
    profile: Profile,
    sha: str,
    domain: str,
) -> dict[str, Any]:
    tree = _slurm_maintenance_tree(profile, sha)
    path = _slurm_maintenance_file(domain, profile.sandbox, sha)
    with _slurm_maintenance_lock(domain):
        raw = _load_slurm_maintenance_file(path)
        if raw is not None and raw.get("phase") in {"running", "blocked"}:
            candidate_set, candidate_set_bytes = _slurm_candidate_set(
                generation=int(raw.get("generation", 0)),
                convergence_id=str(raw.get("convergence_id", "")),
            )
            if candidate_set["candidate_set_sha256"] != raw.get(
                "candidate_set_sha256"
            ) or candidate_set["candidate_bindings"] != raw.get("candidate_bindings"):
                raise HostConvergeError(
                    "blocked Slurm candidate-set transition differs from desired state",
                )
            state = _validate_slurm_maintenance_state(
                raw,
                profile,
                domain=domain,
                sha=sha,
                tree=tree,
                candidate_set=candidate_set,
            )
        else:
            generation = int(raw.get("generation", 0)) + 1 if raw is not None else 1
            candidate_set, candidate_set_bytes = _slurm_candidate_set(
                generation=generation,
                convergence_id=secrets.token_hex(32),
            )
            state = _new_slurm_maintenance_state(
                profile,
                domain=domain,
                sha=sha,
                tree=tree,
                candidate_set=candidate_set,
            )
        if state["phase"] in {"rolling-back", "rolled-back"}:
            raise HostConvergeError("Slurm maintenance candidate is rolling back")
        state["phase"] = "running"
        state["last_failure"] = None
        _write_slurm_maintenance_state(path, state)
        current_node = _slurm_node_order(domain)[0]
        current_action = "slurm-node-converge"
        try:
            for node in _slurm_node_order(domain):
                current_node = node
                row = state["nodes"][node]
                action = str(row["converge_action"])
                current_action = action
                response, _request_id = _slurm_authority_request(
                    profile,
                    domain=domain,
                    node=node,
                    action=action,
                    sha=sha,
                    tree=tree,
                    candidate_set_bytes=candidate_set_bytes,
                )
                if row["converge_receipt"] is not None:
                    if response != row["converge_receipt"]:
                        raise HostConvergeError("Slurm converge receipt replay drifted")
                else:
                    row["converge_receipt"] = response
                    _write_slurm_maintenance_state(path, state)
                current_action = "slurm-check"
                row["check_request_id"] = _slurm_check_node(
                    profile,
                    domain=domain,
                    node=node,
                    sha=sha,
                    tree=tree,
                    candidate_set_bytes=candidate_set_bytes,
                )
                _write_slurm_maintenance_state(path, state)
            for node in _slurm_node_order(domain):
                current_node = node
                current_action = "slurm-check"
                state["nodes"][node]["check_request_id"] = _slurm_check_node(
                    profile,
                    domain=domain,
                    node=node,
                    sha=sha,
                    tree=tree,
                    candidate_set_bytes=candidate_set_bytes,
                )
                _write_slurm_maintenance_state(path, state)
            state["phase"] = "completed"
            _write_slurm_maintenance_state(path, state)
            return state
        except Exception:
            state["phase"] = "blocked"
            state["last_failure"] = {
                "node": current_node,
                "action": current_action,
                "failed_at": datetime.now(UTC).isoformat(),
            }
            _write_slurm_maintenance_state(path, state)
            raise


def slurm_maintenance_check(profile: Profile, sha: str, domain: str) -> dict[str, Any]:
    _require_live_host()
    with _install_lock():
        return _slurm_maintenance_check_locked(profile, sha, domain)


def _slurm_maintenance_check_locked(
    profile: Profile,
    sha: str,
    domain: str,
) -> dict[str, Any]:
    tree = _slurm_maintenance_tree(profile, sha)
    path = _slurm_maintenance_file(domain, profile.sandbox, sha)
    with _slurm_maintenance_lock(domain):
        raw = _load_slurm_maintenance_file(path)
        if not isinstance(raw, dict) or raw.get("phase") != "completed":
            raise HostConvergeError(
                "Slurm check requires a completed candidate-set convergence",
            )
        candidate_set, candidate_set_bytes = _slurm_candidate_set(
            generation=int(raw.get("generation", 0)),
            convergence_id=str(raw.get("convergence_id", "")),
        )
        state = _validate_slurm_maintenance_state(
            raw,
            profile,
            domain=domain,
            sha=sha,
            tree=tree,
            candidate_set=candidate_set,
        )
        checked = [
            {
                "node": node,
                "request_id": _slurm_check_node(
                    profile,
                    domain=domain,
                    node=node,
                    sha=sha,
                    tree=tree,
                    candidate_set_bytes=candidate_set_bytes,
                ),
            }
            for node in _slurm_node_order(domain)
        ]
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-slurm-maintenance-check",
        "domain": domain,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_set_sha256": candidate_set["candidate_set_sha256"],
        "generation": state["generation"],
        "convergence_id": state["convergence_id"],
        "nodes": checked,
        "status": "succeeded",
    }


def slurm_maintenance_rollback(profile: Profile, sha: str, domain: str) -> dict[str, Any]:
    _require_live_host()
    with _install_lock():
        return _slurm_maintenance_rollback_locked(profile, sha, domain)


def _slurm_maintenance_rollback_locked(
    profile: Profile,
    sha: str,
    domain: str,
) -> dict[str, Any]:
    tree = _slurm_maintenance_tree(profile, sha)
    path = _slurm_maintenance_file(domain, profile.sandbox, sha)
    with _slurm_maintenance_lock(domain):
        raw = _load_slurm_maintenance_file(path)
        if not isinstance(raw, dict):
            raise HostConvergeError("Slurm maintenance journal is unavailable")
        candidate_set = {
            "schema_version": 2,
            "kind": "loom.developer-sandbox.slurm-candidate-set",
            "candidate_set_sha256": raw.get("candidate_set_sha256"),
            "candidate_bindings": raw.get("candidate_bindings"),
            "generation": raw.get("generation"),
            "convergence_id": raw.get("convergence_id"),
        }
        candidate_set_bytes = (
            json.dumps(
                candidate_set,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            )
            + "\n"
        ).encode("ascii")
        state = _validate_slurm_maintenance_state(
            raw,
            profile,
            domain=domain,
            sha=sha,
            tree=tree,
            candidate_set=candidate_set,
        )
        rollback_resume = state["phase"] == "rolling-back" or (
            state["phase"] == "blocked"
            and isinstance(state["last_failure"], dict)
            and state["last_failure"].get("action") == "slurm-rollback"
        )
        if state["phase"] not in {"completed", "rolled-back"} and not rollback_resume:
            raise HostConvergeError(
                "Slurm rollback requires a completed candidate-set convergence",
            )
        prior_request_ids: dict[str, str] = {}
        for node in _slurm_node_order(domain):
            prior = state["nodes"][node]["converge_receipt"]
            if not isinstance(prior, dict):
                raise HostConvergeError(
                    "Slurm rollback requires every converge authority receipt",
                )
            prior_request_id = prior.get("request_id")
            if (
                not isinstance(prior_request_id, str)
                or DIGEST_RE.fullmatch(prior_request_id) is None
            ):
                raise HostConvergeError("Slurm rollback owned receipt is invalid")
            prior_request_ids[node] = prior_request_id
        if state["phase"] == "rolled-back":
            return state
        state["phase"] = "rolling-back"
        state["last_failure"] = None
        _write_slurm_maintenance_state(path, state)
        current_node = DOMAIN_PUBLISHERS[domain]
        try:
            for node in _slurm_node_order(domain):
                current_node = node
                row = state["nodes"][node]
                response, _request_id = _slurm_authority_request(
                    profile,
                    domain=domain,
                    node=node,
                    action="slurm-rollback",
                    sha=sha,
                    tree=tree,
                    candidate_set_bytes=candidate_set_bytes,
                    prior_request_id=prior_request_ids[node],
                )
                if row["rollback_receipt"] is not None:
                    if response != row["rollback_receipt"]:
                        raise HostConvergeError("Slurm rollback receipt replay drifted")
                else:
                    row["rollback_receipt"] = response
                    _write_slurm_maintenance_state(path, state)
            state["phase"] = "rolled-back"
            _write_slurm_maintenance_state(path, state)
            return state
        except Exception:
            state["phase"] = "blocked"
            state["last_failure"] = {
                "node": current_node,
                "action": "slurm-rollback",
                "failed_at": datetime.now(UTC).isoformat(),
            }
            _write_slurm_maintenance_state(path, state)
            raise


def _verify_remote_candidate(
    profile: Profile,
    node: str,
    sha: str,
    tree: str,
    shared_gid: int,
) -> None:
    if profile.env_id is None:
        profile = _registry_bound_profile(profile, sha=sha, tree=tree)
    domain = next(
        (name for name, nodes in DOMAIN_PEERS.items() if node in nodes),
        None,
    )
    if domain is None:
        raise HostConvergeError("remote candidate node is outside the closed inventory")
    authority_report = _node_authority(
        node,
        "check",
        _node_authority_envelope(
            action="inspect-candidate",
            node=node,
            domain=domain,
            sandbox=profile.sandbox,
            sha=sha,
            tree=tree,
            profile=profile,
        ),
    )
    report = authority_report.get("result")
    if (
        not isinstance(report, dict)
        or report.get("operation") != "inspect-candidate"
        or report.get("domain") != domain
        or report.get("sandbox") != profile.sandbox
        or report.get("candidate_sha") != sha
        or report.get("candidate_tree") != tree
        or report.get("candidate_uid") != 0
        or report.get("candidate_gid") != shared_gid
        or report.get("candidate_mode") != "2750"
        or report.get("candidate_clean") is not True
    ):
        raise HostConvergeError(f"{node} candidate identity or metadata is invalid")


def _archive_credentials(
    source: Path,
    *,
    worker_token: str,
    minio_access_key: str,
    minio_secret_key: str,
) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w") as archive:
        for name in ("ca.pem", "client.pem", "client-key.pem"):
            path = source / name
            content = path.read_bytes()
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o600 if name == "client-key.pem" else 0o644
            info.uid = 0
            info.gid = 0
            archive.addfile(info, io.BytesIO(content))
        for name, value in (
            ("worker-token", worker_token),
            ("minio-access-key", minio_access_key),
            ("minio-secret-key", minio_secret_key),
        ):
            content = (value + "\n").encode()
            info = tarfile.TarInfo(name)
            info.size = len(content)
            info.mode = 0o600
            info.uid = 0
            info.gid = 0
            archive.addfile(info, io.BytesIO(content))
    return output.getvalue()


def _remove_source_stage(path: Path, sha: str) -> None:
    if path != SOURCE_STAGING_ROOT / sha:
        raise HostConvergeError("candidate source staging path is invalid")
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise HostConvergeError("candidate source staging metadata is invalid")
    shutil.rmtree(path)
    _fsync_directory(path.parent)


@contextmanager
def _candidate_source_stage(sha: str) -> Iterator[tuple[Path, str]]:
    if SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate SHA must be full lowercase 40-hex")
    authority = _identity("root", SHARED_GROUP)
    head = _git(REPO_ROOT, "rev-parse", "--verify", "HEAD", identity=authority)
    tree = _git(REPO_ROOT, "rev-parse", "--verify", "HEAD^{tree}", identity=authority)
    status = _git(
        REPO_ROOT,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        identity=authority,
    )
    if head != sha or SHA_RE.fullmatch(tree) is None or status:
        raise HostConvergeError("installer source is not the clean exact candidate")
    _ensure_root_private_directory(SOURCE_STAGING_ROOT)
    stage = SOURCE_STAGING_ROOT / sha
    # A prior process may have died after staging. The root-private, exact-SHA
    # namespace is disposable and is always rebuilt from the verified checkout.
    _remove_source_stage(stage, sha)
    _ensure_root_private_directory(stage)
    bundle = stage / "candidate.bundle"
    temporary = stage / ".candidate.bundle.tmp"
    manifest = stage / "manifest.json"
    try:
        _run(
            (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
                "-C",
                str(REPO_ROOT),
                "bundle",
                "create",
                str(temporary),
                "HEAD",
            ),
            env=_clean_git_environment(),
        )
        os.chown(temporary, 0, 0)
        os.chmod(temporary, 0o600)
        descriptor = os.open(temporary, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, bundle)
        _fsync_directory(stage)
        heads = _run(
            (
                "git",
                "-c",
                "core.hooksPath=/dev/null",
                "-c",
                "core.attributesFile=/dev/null",
                "bundle",
                "list-heads",
                str(bundle),
            ),
            env=_clean_git_environment(),
        ).stdout.splitlines()
        if heads != [f"{sha} HEAD"]:
            raise HostConvergeError("candidate source bundle is not exact-HEAD bounded")
        payload = {
            "schema_version": 1,
            "status": "staged",
            "candidate_sha": sha,
            "candidate_tree": tree,
            "bundle_sha256": hashlib.sha256(bundle.read_bytes()).hexdigest(),
            "created_at": datetime.now(UTC).isoformat(),
        }
        _atomic_write(
            manifest,
            (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            mode=0o600,
        )
        yield bundle, tree
    finally:
        temporary.unlink(missing_ok=True)
        _remove_source_stage(stage, sha)


def _bootstrap_domain_runtime_hosts(profile: Profile, sha: str, tree: str) -> None:
    if profile.env_id is None:
        profile = _registry_bound_profile(profile, sha=sha, tree=tree)
    for domain, nodes in DOMAIN_PEERS.items():
        for node in nodes:
            _node_authority(
                node,
                "transact",
                _node_authority_envelope(
                    action="host-converge",
                    node=node,
                    domain=domain,
                    sandbox=profile.sandbox,
                    sha=sha,
                    tree=tree,
                    profile=profile,
                ),
            )


def _materialize_domain_candidates(
    profile: Profile,
    sha: str,
    tree: str,
    bundle: Path,
) -> None:
    if profile.env_id is None:
        profile = _registry_bound_profile(profile, sha=sha, tree=tree)
    payload = bundle.read_bytes()
    for domain, publisher in DOMAIN_PUBLISHERS.items():
        _node_authority(
            publisher,
            "transact",
            _node_authority_envelope(
                action="materialize",
                node=publisher,
                domain=domain,
                sandbox=profile.sandbox,
                sha=sha,
                tree=tree,
                payload_kind="git-bundle",
                payload_bytes=payload,
                profile=profile,
            ),
        )


def _link_domain(node: str) -> str:
    for domain, nodes in DOMAIN_PEERS.items():
        if node in nodes:
            return domain
    raise HostConvergeError("remote-link node is outside the closed inventory")


def _link_client_uri(profile: Profile, sha: str) -> str:
    return f"spiffe://loom/developer-sandbox/{profile.sandbox}/candidate/{sha}/worker"


def _validate_link_client_inspection(
    profile: Profile,
    sha: str,
    node: str,
    report: object,
) -> dict[str, Any]:
    expected_keys = {
        "schema_version",
        "sandbox",
        "candidate_sha",
        "node",
        "route",
        "tls_version",
        "services",
        "client_uri_san",
        "secret_files",
        "ca_fingerprint",
        "client_cert_fingerprint",
    }
    expected_secret = {"uid": 0, "gid": 0, "mode": "0600", "present": True}
    expected_secret_names = {
        "worker-token",
        "minio-access-key",
        "minio-secret-key",
        "client-key.pem",
    }
    expected_services = {
        name: {
            "listener_port": profile.ports.get(
                f"relay_{name.replace('-', '_')}",
                LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS.get(profile.sandbox, {}).get(name, (0, 0))[0],
            ),
            "target_port": profile.ports.get(
                {
                    "control-plane": "control_plane",
                    "gateway": "llm_gateway",
                    "minio": "minio",
                }[name],
                LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS.get(profile.sandbox, {}).get(name, (0, 0))[1],
            ),
            "health": "ok",
        }
        for name in REMOTE_LINK_SERVICE_NAMES
    }
    if (
        not isinstance(report, dict)
        or set(report) != expected_keys
        or report.get("schema_version") != 1
        or report.get("sandbox") != profile.sandbox
        or report.get("candidate_sha") != sha
        or report.get("node") != node
        or report.get("route") != "ok"
        or report.get("tls_version") != "TLSv1.3"
        or report.get("client_uri_san") != _link_client_uri(profile, sha)
        or not isinstance(report.get("services"), dict)
        or report.get("services") != expected_services
        or not isinstance(report.get("secret_files"), dict)
        or set(report["secret_files"]) != expected_secret_names
        or any(state != expected_secret for state in report["secret_files"].values())
        or FINGERPRINT_RE.fullmatch(str(report.get("ca_fingerprint"))) is None
        or FINGERPRINT_RE.fullmatch(str(report.get("client_cert_fingerprint"))) is None
    ):
        raise HostConvergeError(f"{node} remote-link client inspection is invalid")
    return {
        "node": node,
        "candidate_sha": sha,
        "route": {
            "destination": REMOTE_LINK_SERVER_ADDRESS,
            "status": "ok",
        },
        "tls_version": "TLSv1.3",
        "client_uri_san": _link_client_uri(profile, sha),
        "ca_fingerprint": report["ca_fingerprint"],
        "client_cert_fingerprint": report["client_cert_fingerprint"],
        "secret_files": report["secret_files"],
        "services": {
            name: {
                "listener_port": expected_services[name]["listener_port"],
                "health": "ok",
            }
            for name in REMOTE_LINK_SERVICE_NAMES
        },
    }


def _validate_link_server_inspection(
    profile: Profile,
    sha: str,
    report: object,
) -> dict[str, Any]:
    expected_keys = {
        "node",
        "address",
        "unit",
        "unit_active",
        "active_candidate_sha",
        "ca_fingerprint",
        "server_cert_fingerprint",
        "client_uri_san",
        "services",
    }
    expected_services = {
        name: {
            "listener_port": profile.ports.get(
                f"relay_{name.replace('-', '_')}",
                LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS.get(profile.sandbox, {}).get(name, (0, 0))[0],
            ),
            "target_host": "127.0.0.1",
            "target_port": profile.ports.get(
                {
                    "control-plane": "control_plane",
                    "gateway": "llm_gateway",
                    "minio": "minio",
                }[name],
                LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS.get(profile.sandbox, {}).get(name, (0, 0))[1],
            ),
            "health_path": REMOTE_LINK_HEALTH_PATHS[name],
            "tls_version": "TLSv1.3",
            "status": "active",
        }
        for name in REMOTE_LINK_SERVICE_NAMES
    }
    if (
        not isinstance(report, dict)
        or set(report) != expected_keys
        or report.get("node") != "oldlab-2"
        or report.get("address") != REMOTE_LINK_SERVER_ADDRESS
        or report.get("unit") != f"loom-developer-sandbox-link@{profile.sandbox}.service"
        or report.get("unit_active") is not True
        or report.get("active_candidate_sha") != sha
        or report.get("client_uri_san") != _link_client_uri(profile, sha)
        or FINGERPRINT_RE.fullmatch(str(report.get("ca_fingerprint"))) is None
        or FINGERPRINT_RE.fullmatch(str(report.get("server_cert_fingerprint"))) is None
        or report.get("services") != expected_services
    ):
        raise HostConvergeError("oldlab-2 remote-link server inspection is invalid")
    return dict(report)


def _fleet_attestation_digest(payload: Mapping[str, Any]) -> str:
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    canonical = json.dumps(
        unsigned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("ascii")
    return "sha256:" + hashlib.sha256(canonical).hexdigest()


def _collect_and_persist_remote_link_fleet(
    profile: Profile,
    sha: str,
    tree: str,
) -> dict[str, Any]:
    if profile.env_id is None:
        profile = _registry_bound_profile(profile, sha=sha, tree=tree)
    node_payloads: dict[str, dict[str, Any]] = {}
    failures: list[str] = []

    def inspect_client(node: str) -> tuple[str, dict[str, Any]]:
        envelope = _node_authority_envelope(
            action="inspect-link-client",
            node=node,
            domain=_link_domain(node),
            sandbox=profile.sandbox,
            sha=sha,
            tree=tree,
            profile=profile,
        )
        request_id = str(json.loads(envelope)["request_id"])
        response = _node_authority(
            node,
            "check",
            envelope,
        )
        if (
            set(response) != {"schema_version", "request_id", "status", "result"}
            or response.get("schema_version") != 1
            or response.get("request_id") != request_id
            or response.get("status") != "succeeded"
        ):
            raise HostConvergeError(f"{node} link authority response is invalid")
        return node, _validate_link_client_inspection(
            profile,
            sha,
            node,
            response.get("result"),
        )

    with ThreadPoolExecutor(max_workers=8, thread_name_prefix="loom-link-authority") as pool:
        futures = {pool.submit(inspect_client, node): node for node in ELIGIBLE_LINK_NODES}
        for future in as_completed(futures):
            node = futures[future]
            try:
                checked_node, payload = future.result()
            except Exception:
                failures.append(node)
                continue
            if checked_node != node:
                failures.append(node)
                continue
            node_payloads[node] = payload
    if failures or set(node_payloads) != set(ELIGIBLE_LINK_NODES):
        failed = sorted(set(failures) | (set(ELIGIBLE_LINK_NODES) - set(node_payloads)))
        raise HostConvergeError(
            "remote-link client inspection failed for: " + ",".join(failed),
        )

    server_envelope = _node_authority_envelope(
        action="inspect-link-server",
        node="oldlab-2",
        domain="oldlab",
        sandbox=profile.sandbox,
        sha=sha,
        tree=tree,
        profile=profile,
    )
    server_request_id = str(json.loads(server_envelope)["request_id"])
    server_response = _node_authority("oldlab-2", "check", server_envelope)
    if (
        set(server_response) != {"schema_version", "request_id", "status", "result"}
        or server_response.get("schema_version") != 1
        or server_response.get("request_id") != server_request_id
        or server_response.get("status") != "succeeded"
    ):
        raise HostConvergeError("oldlab-2 link authority response is invalid")
    server = _validate_link_server_inspection(
        profile,
        sha,
        server_response.get("result"),
    )
    if {str(payload["ca_fingerprint"]) for payload in node_payloads.values()} != {
        server["ca_fingerprint"]
    }:
        raise HostConvergeError("remote-link fleet CA generation is inconsistent")
    generated = datetime.now(UTC).replace(microsecond=0)
    fleet: dict[str, Any] = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "generated_at": generated.isoformat().replace("+00:00", "Z"),
        "expires_at": (generated + ATTESTATION_TTL).isoformat().replace("+00:00", "Z"),
        "eligible_nodes": list(ELIGIBLE_LINK_NODES),
        "bundle_generation": {
            "candidate_sha": sha,
            "ca_fingerprint": server["ca_fingerprint"],
            "client_uri_san": _link_client_uri(profile, sha),
        },
        "server": server,
        "nodes": {node: node_payloads[node] for node in ELIGIBLE_LINK_NODES},
    }
    dynamic_bindings = (
        profile.env_id,
        profile.resource_generation,
        profile.registry_generation,
        profile.registry_payload_sha256,
    )
    if any(value is not None for value in dynamic_bindings):
        if (
            profile.env_id is None
            or profile.resource_generation is None
            or profile.registry_generation is None
            or profile.registry_payload_sha256 is None
            or DIGEST_RE.fullmatch(profile.registry_payload_sha256) is None
        ):
            raise HostConvergeError("dynamic fleet registry binding is incomplete")
        fleet.update(
            {
                "env_id": profile.env_id,
                "resource_generation": profile.resource_generation,
                "registry_generation": profile.registry_generation,
                "registry_payload_sha256": profile.registry_payload_sha256,
                "candidate_tree": tree,
            }
        )
    fleet["payload_sha256"] = _fleet_attestation_digest(fleet)
    serialized = (
        json.dumps(
            fleet,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )
    envelope = _node_authority_envelope(
        action="persist-fleet-attestation",
        node="oldlab-2",
        domain="oldlab",
        sandbox=profile.sandbox,
        sha=sha,
        tree=tree,
        payload_kind="fleet-attestation-json",
        payload_bytes=serialized,
        profile=profile,
    )
    request_id = str(json.loads(envelope)["request_id"])
    persisted = _node_authority("oldlab-2", "transact", envelope)
    if (
        set(persisted)
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
        or persisted.get("schema_version") != 1
        or persisted.get("request_id") != request_id
        or persisted.get("action") != "persist-fleet-attestation"
        or persisted.get("node") != "oldlab-2"
        or persisted.get("domain") != "oldlab"
        or persisted.get("sandbox") != profile.sandbox
        or persisted.get("candidate_sha") != sha
        or persisted.get("candidate_tree") != tree
        or persisted.get("env_id") != profile.env_id
        or persisted.get("resource_generation") != profile.resource_generation
        or persisted.get("candidate_id") != profile.candidate_id
        or persisted.get("registry_generation") != profile.registry_generation
        or persisted.get("registry_payload_sha256") != profile.registry_payload_sha256
        or persisted.get("payload_sha256") != hashlib.sha256(serialized).hexdigest()
        or DIGEST_RE.fullmatch(str(persisted.get("result_sha256"))) is None
        or persisted.get("inner_receipt") is not None
        or not isinstance(persisted.get("completed_at"), str)
        or persisted.get("status") != "succeeded"
    ):
        raise HostConvergeError("fleet attestation persistence receipt is invalid")
    return fleet


def _install_remote_link_fleet(
    profile: Profile,
    sha: str,
    tree: str,
    authority: Identity,
) -> None:
    if profile.env_id is None:
        profile = _registry_bound_profile(profile, sha=sha, tree=tree)
    values = _parse_env_file(profile.secrets_env)
    program = "scripts/ops/developer_sandbox_remote_link_host.py"
    prepare_args = (
        "prepare-rotation",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--execute",
    )
    if profile.env_id is None:
        _run_candidate_program(profile, sha, program, *prepare_args)
    else:
        _run((str(INSTALLED_REMOTE_LINK_HOST), *prepare_args))
    issuance = REMOTE_LINK_ISSUANCE_ROOT / profile.sandbox / sha
    server_args = (
        "install-server",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--credential-source",
        str(issuance / "server"),
        "--execute",
    )
    if profile.env_id is None:
        _run_candidate_program(profile, sha, program, *server_args)
    else:
        _run((str(INSTALLED_REMOTE_LINK_HOST), *server_args))
    for node in ELIGIBLE_LINK_NODES:
        _verify_remote_candidate(profile, node, sha, tree, authority.gid)
        archive = _archive_credentials(
            issuance / "clients" / node,
            worker_token=values["LOOM_WORKER_TOKEN"],
            minio_access_key=(
                values["LOOM_DEV_MINIO_ROOT_USER"]
                if profile.env_id is None
                else (
                    profile.service_user
                    if profile.service_user is not None
                    else _raise_missing_service_user()
                )
            ),
            minio_secret_key=values["LOOM_DEV_MINIO_ROOT_PASSWORD"],
        )
        domain = next(name for name, nodes in DOMAIN_PEERS.items() if node in nodes)
        _node_authority(
            node,
            "transact",
            _node_authority_envelope(
                action="install-client",
                node=node,
                domain=domain,
                sandbox=profile.sandbox,
                sha=sha,
                tree=tree,
                payload_kind="client-credentials",
                payload_bytes=archive,
                profile=profile,
            ),
        )
    activate_args = (
        "activate-server",
        "--sandbox",
        profile.sandbox,
        "--candidate-sha",
        sha,
        "--execute",
    )
    if profile.env_id is None:
        _run_candidate_program(profile, sha, program, *activate_args)
    else:
        _run((str(INSTALLED_REMOTE_LINK_HOST), *activate_args))
    _collect_and_persist_remote_link_fleet(profile, sha, tree)


def _raise_missing_service_user() -> str:
    raise HostConvergeError("dynamic service identity binding is incomplete")


def _worker_capacity_contract(
    domain: str,
    *,
    installed: bool = False,
) -> tuple[str, int]:
    """Read the domain worker binding from the exact checked-in capacity policy."""
    if domain not in DOMAIN_PEERS:
        raise HostConvergeError("worker capacity domain is invalid")
    runtime_config = (
        INSTALLED_DOMAIN_RUNTIME_CONFIG if installed else SOURCE_PROFILES / "runtime-domains.toml"
    )
    try:
        runtime = tomllib.loads(runtime_config.read_text(encoding="utf-8"))
        domain_config = runtime["domains"][domain]
        source = domain_config["capacity_policy_source"]
        expected_source = f"deploy/developer-sandboxes/shared-capacity-policies/{domain}.toml"
        if source != expected_source:
            raise HostConvergeError("worker capacity policy source drifted")
        capacity_path = (
            REPO_ROOT / source
            if not installed
            else INSTALLED_CAPACITY_POLICY_ROOT / f"{domain}.toml"
        )
        capacity = tomllib.loads(capacity_path.read_text(encoding="utf-8"))
        actuator = capacity["policy"]["actuator_config"]
    except (KeyError, OSError, TypeError, UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("worker capacity contract is unavailable or invalid") from exc
    expected_env = f"${{RUNTIME_ROOT}}/${{CANDIDATE_SHA}}/worker-{domain}.env"
    concurrency = actuator.get("requested_concurrency")
    allowed_nodes = actuator.get("allowed_nodes")
    if (
        runtime.get("schema_version") != 1
        or not isinstance(domain_config, dict)
        or capacity.get("schema_version") != 1
        or capacity.get("pool_name") != domain
        or domain_config.get("worker_pool_name") != domain
        or domain_config.get("worker_max_concurrent") != concurrency
        or type(concurrency) is not int
        or concurrency < 1
        or not isinstance(allowed_nodes, list)
        or not all(isinstance(node, str) for node in allowed_nodes)
        or tuple(node.lower() for node in allowed_nodes) != DOMAIN_CAPACITY_NODES[domain]
        or actuator.get("env_file") != expected_env
        or actuator.get("candidate_sha") != "${CANDIDATE_SHA}"
        or actuator.get("slurm_account") != "${SLURM_ACCOUNT}"
    ):
        raise HostConvergeError("worker capacity contract binding drifted")
    return domain, concurrency


def _worker_env_seed(profile: Profile, sha: str, domain: str) -> bytes:
    pool, concurrency = _worker_capacity_contract(
        domain,
        installed=profile.env_id is not None,
    )
    bundle = f"/etc/loom/developer-sandbox-links/clients/{profile.sandbox}/{sha}"
    legacy_ports = LEGACY_SEED_REMOTE_LINK_SERVICE_PORTS.get(profile.sandbox, {})
    control_plane_port = profile.ports.get(
        "relay_control_plane",
        legacy_ports.get("control-plane", (0, 0))[0],
    )
    gateway_port = profile.ports.get(
        "relay_gateway",
        legacy_ports.get("gateway", (0, 0))[0],
    )
    minio_port = profile.ports.get(
        "relay_minio",
        legacy_ports.get("minio", (0, 0))[0],
    )
    if (
        any(
            type(port) is not int or not 1024 <= port <= 65535
            for port in (control_plane_port, gateway_port, minio_port)
        )
        or len({control_plane_port, gateway_port, minio_port}) != 3
    ):
        raise HostConvergeError("worker remote-link ports are unavailable or invalid")
    values = {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://sandbox-link:8080",
        "LOOM_WORKER_GATEWAY_URL": "http://sandbox-link:9100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://sandbox-link:9000",
        "LOOM_WORKER_SANDBOX_IDENTITY": profile.sandbox,
        "LOOM_WORKER_CANDIDATE_SHA": sha,
        "LOOM_WORKER_IMAGE_ID": profile.worker_image_id(domain),
        "LOOM_SANDBOX_LINK_CP_UPSTREAM": (
            f"https://{REMOTE_LINK_SERVER_ADDRESS}:{control_plane_port}"
        ),
        "LOOM_SANDBOX_LINK_CP_EXPECTED_PORT": str(control_plane_port),
        "LOOM_SANDBOX_LINK_GATEWAY_UPSTREAM": (
            f"https://{REMOTE_LINK_SERVER_ADDRESS}:{gateway_port}"
        ),
        "LOOM_SANDBOX_LINK_GATEWAY_EXPECTED_PORT": str(gateway_port),
        "LOOM_SANDBOX_LINK_MINIO_UPSTREAM": (f"https://{REMOTE_LINK_SERVER_ADDRESS}:{minio_port}"),
        "LOOM_SANDBOX_LINK_MINIO_EXPECTED_PORT": str(minio_port),
        "LOOM_WORKER_POOL_NAME": pool,
        "LOOM_WORKER_MAX_CONCURRENT": str(concurrency),
        "LOOM_WORKER_TOKEN_FILE_HOST": f"{bundle}/worker-token",
        "LOOM_WORKER_MINIO_ACCESS_KEY_FILE_HOST": f"{bundle}/minio-access-key",
        "LOOM_WORKER_MINIO_SECRET_KEY_FILE_HOST": f"{bundle}/minio-secret-key",
        "LOOM_WORKER_CP_TLS_CA_FILE_HOST": f"{bundle}/ca.pem",
        "LOOM_WORKER_CP_TLS_CERT_FILE_HOST": f"{bundle}/client.pem",
        "LOOM_WORKER_CP_TLS_KEY_FILE_HOST": f"{bundle}/client-key.pem",
    }
    dynamic = (
        profile.env_id,
        profile.resource_generation,
        profile.registry_generation,
        profile.registry_payload_sha256,
        profile.candidate_id,
        profile.candidate_tree,
        profile.worker_image_ids,
    )
    if any(value is not None for value in dynamic):
        if (
            profile.env_id is None
            or profile.resource_generation is None
            or profile.registry_generation is None
            or profile.registry_payload_sha256 is None
            or profile.candidate_id is None
            or profile.candidate_tree is None
            or DIGEST_RE.fullmatch(profile.registry_payload_sha256) is None
            or SHA_RE.fullmatch(profile.candidate_tree) is None
        ):
            raise HostConvergeError("dynamic worker env registry binding is incomplete")
        values.update(
            {
                "LOOM_WORKER_COMPOSE_PROJECT": profile.compose_project,
                "LOOM_WORKER_ENV_ID": profile.env_id,
                "LOOM_WORKER_RESOURCE_GENERATION": str(profile.resource_generation),
                "LOOM_WORKER_CANDIDATE_ID": profile.candidate_id,
                "LOOM_WORKER_CANDIDATE_TREE": profile.candidate_tree,
                "LOOM_WORKER_REGISTRY_GENERATION": str(profile.registry_generation),
                "LOOM_WORKER_REGISTRY_PAYLOAD_SHA256": profile.registry_payload_sha256,
            }
        )
    return _render_env(values)


def _converge_domain_runtime_hosts(
    profile: Profile,
    sha: str,
    tree: str,
    authority: Identity,
) -> None:
    if profile.env_id is None:
        profile = _registry_bound_profile(profile, sha=sha, tree=tree)
    for domain, nodes in DOMAIN_PEERS.items():
        for node in nodes:
            _verify_remote_candidate(profile, node, sha, tree, authority.gid)
            _node_authority(
                node,
                "transact",
                _node_authority_envelope(
                    action="host-converge",
                    node=node,
                    domain=domain,
                    sandbox=profile.sandbox,
                    sha=sha,
                    tree=tree,
                    profile=profile,
                ),
            )


def _publish_domain_attestations(
    profile: Profile,
    sha: str,
    tree: str,
) -> None:
    if profile.env_id is None:
        profile = _registry_bound_profile(profile, sha=sha, tree=tree)
    relative_program = "scripts/ops/developer_sandbox_domain_runtime.py"
    config_relative = "deploy/developer-sandboxes/runtime-domains.toml"
    fleet_path = FLEET_ATTESTATION_ROOT / profile.sandbox / sha / "fleet.json"
    try:
        fleet_metadata = fleet_path.lstat()
        fleet = fleet_path.read_bytes()
    except OSError as exc:
        raise HostConvergeError("fleet attestation seed is unavailable") from exc
    if (
        stat.S_ISLNK(fleet_metadata.st_mode)
        or not stat.S_ISREG(fleet_metadata.st_mode)
        or fleet_metadata.st_uid != 0
        or fleet_metadata.st_gid != 0
        or stat.S_IMODE(fleet_metadata.st_mode) != 0o600
        or fleet_metadata.st_nlink != 1
        or not fleet
        or len(fleet) > (1 << 20)
    ):
        raise HostConvergeError("fleet attestation seed metadata is invalid")
    for domain in DOMAIN_PEERS:
        seed = _worker_env_seed(profile, sha, domain)
        archive_buffer = io.BytesIO()
        with tarfile.open(fileobj=archive_buffer, mode="w") as archive:
            for name, content in (("worker.env", seed), ("fleet.json", fleet)):
                info = tarfile.TarInfo(name)
                info.size = len(content)
                info.mode = 0o600
                info.uid = 0
                info.gid = 0
                archive.addfile(info, io.BytesIO(content))
        attestation_seed = archive_buffer.getvalue()
        publisher = DOMAIN_PUBLISHERS[domain]
        _node_authority(
            publisher,
            "transact",
            _node_authority_envelope(
                action="attest",
                node=publisher,
                domain=domain,
                sandbox=profile.sandbox,
                sha=sha,
                tree=tree,
                payload_kind="attestation-seed",
                payload_bytes=attestation_seed,
                profile=profile,
            ),
        )
    if profile.env_id is None:
        _run_candidate_program(
            profile,
            sha,
            relative_program,
            "collect",
            "--config",
            str(profile.candidate_root / sha / config_relative),
            "--sandbox",
            profile.sandbox,
            "--candidate-sha",
            sha,
            "--candidate-tree",
            tree,
            "--execute",
        )
    else:
        _run(
            (
                str(INSTALLED_DOMAIN_RUNTIME),
                "collect",
                "--config",
                str(INSTALLED_DOMAIN_RUNTIME_CONFIG),
                "--sandbox",
                profile.sandbox,
                "--candidate-sha",
                sha,
                "--candidate-tree",
                tree,
                "--execute",
            )
        )


def _read_policy(profile: Profile, pool: str) -> dict[str, Any] | None:
    token = _read_admin_token(profile.admin_secret)
    environment = f"sandbox-{profile.sandbox}"
    url = (
        f"http://127.0.0.1:{profile.ports['control_plane']}"
        f"/admin/worker-pool-autoscaler-policies/{environment}/{pool}"
    )
    request = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return None
        raise HostConvergeError("capacity policy readback failed safely") from exc
    except urllib.error.URLError as exc:
        raise HostConvergeError("capacity policy readback is unavailable") from exc
    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise HostConvergeError("capacity policy readback is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("capacity policy readback is invalid")
    return payload


def _assert_capacity_units_stopped(profile: Profile) -> None:
    for instance in (f"{profile.sandbox}-gb10", f"{profile.sandbox}-oldlab"):
        for suffix in ("timer", "service"):
            unit = f"loom-shared-capacity-adapter@{instance}.{suffix}"
            active = _run(("systemctl", "is-active", unit), expected={0, 3, 4})
            if active.returncode == 0:
                raise HostConvergeError("shared capacity adapter is active during prepare")
        enabled = _run(
            ("systemctl", "is-enabled", f"loom-shared-capacity-adapter@{instance}.timer"),
            expected={0, 1, 3, 4},
        )
        if enabled.returncode == 0:
            raise HostConvergeError("shared capacity adapter timer is enabled during prepare")


def assert_capacity_quiescent(profile: Profile) -> None:
    _assert_capacity_units_stopped(profile)
    for pool in ("gb10", "oldlab"):
        policy = _read_policy(profile, pool)
        if policy is None:
            continue
        lease = policy.get("capacity_lease_state")
        lease_state = lease.get("state") if isinstance(lease, dict) else None
        if policy.get("enabled") is not False or policy.get("max_slots") != 0:
            raise HostConvergeError("shared capacity policy is not drained")
        if lease_state not in {None, "retired"}:
            raise HostConvergeError("shared capacity lease is still nonterminal")


def verify_nfs_mount() -> None:
    result = _run(("findmnt", "-n", "-o", "FSTYPE,TARGET", "-T", str(NFS_ROOT)))
    fields = result.stdout.split()
    if len(fields) != 2 or fields[0] not in {"nfs", "nfs4"} or fields[1] != "/shared_work":
        raise HostConvergeError("candidate namespace is not on the expected /shared_work NFS mount")


def verify_state_parent() -> None:
    shared = _identity("root", SHARED_GROUP)
    try:
        metadata = STATE_PARENT.lstat()
    except OSError as exc:
        raise HostConvergeError("sandbox state parent is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o2750
        or (metadata.st_uid, metadata.st_gid) != (0, shared.gid)
    ):
        raise HostConvergeError("sandbox state parent owner or mode is invalid")


def _require_live_host() -> None:
    if os.geteuid() != 0:
        raise HostConvergeError("host convergence must run as root")
    hostname = socket.gethostname().rstrip(".").lower()
    if hostname != EXPECTED_HOSTNAME:
        raise HostConvergeError(
            f"host convergence requires {EXPECTED_HOSTNAME}, got {hostname}",
        )


def _migration_tree(candidate: Path, publisher: Identity) -> str:
    result = _run(
        (
            "git",
            "-c",
            f"safe.directory={candidate}",
            "-C",
            str(candidate),
            "rev-parse",
            "--verify",
            "HEAD:migrations",
        ),
        env=_clean_git_environment(),
        identity=publisher,
    )
    tree = result.stdout.strip()
    if SHA_RE.fullmatch(tree) is None:
        raise HostConvergeError("candidate migration tree is invalid")
    return tree


def verify_developer_docker_access(identity: Identity) -> None:
    _run(
        ("docker", "info", "--format", "{{.ServerVersion}}"),
        env={"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"},
        identity=identity,
        init_groups=True,
    )


def verify_candidate_consumer(profile: Profile, sha: str, identity: Identity) -> None:
    candidate = profile.candidate_root / sha
    for relative in (
        "scripts/ops/developer_sandbox.py",
        f"deploy/developer-sandboxes/{profile.sandbox}.toml",
        "deploy/docker-compose.dev.yml",
    ):
        result = _run(
            ("test", "-r", str(candidate / relative)),
            identity=identity,
            init_groups=True,
            expected={0, 1},
        )
        if result.returncode != 0:
            raise HostConvergeError(
                f"{profile.sandbox} cannot read the immutable candidate through sharedwork",
            )


def verify_candidate_profile_bytes(profile: Profile, sha: str, publisher: Identity) -> None:
    relative = f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    candidate = profile.candidate_root / sha / f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    source = _run(
        (
            "git",
            "-c",
            "core.hooksPath=/dev/null",
            "-c",
            "core.attributesFile=/dev/null",
            "-C",
            str(REPO_ROOT),
            "show",
            f"{sha}:{relative}",
        ),
        env=_clean_git_environment(),
        identity=publisher,
    )
    expected = hashlib.sha256(source.stdout.encode()).hexdigest()
    result = _run(("sha256sum", str(candidate)), identity=publisher)
    actual = result.stdout.split(maxsplit=1)[0] if result.stdout else ""
    if actual != expected:
        raise HostConvergeError(
            f"candidate changed the fixed host profile for {profile.sandbox}",
        )


def require_migration_compatible_update(
    profile: Profile,
    target_sha: str,
    publisher: Identity,
) -> None:
    desired = _load_json(profile.desired_file, "sandbox desired state")
    if desired is None:
        return
    current_sha = desired.get("candidate_sha")
    if not isinstance(current_sha, str) or SHA_RE.fullmatch(current_sha) is None:
        raise HostConvergeError("sandbox desired SHA is invalid")
    if current_sha == target_sha:
        return
    current = profile.candidate_root / current_sha
    verify_candidate(profile, current, current_sha, publisher)
    if _migration_tree(current, publisher) != _migration_tree(
        profile.candidate_root / target_sha,
        publisher,
    ):
        raise HostConvergeError(
            "candidate update crosses a migration-tree change; "
            "use a reviewed backup and restore workflow",
        )


def rollback(profile: Profile, target_sha: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    with _install_lock():
        desired: dict[str, Any] | None = None
        target_tree = ""
        previous_relay: str | None = None
        try:
            with _activation_lock(profile):
                orphan = _transaction_payload(profile)
                if orphan is not None:
                    _recover_transaction(profile, orphan)
                desired = _load_json(profile.desired_file, "sandbox desired state")
                if desired is None:
                    raise HostConvergeError("sandbox desired state is absent")
                current_sha = desired.get("candidate_sha")
                if target_sha != desired.get("previous_sha") or not isinstance(
                    current_sha,
                    str,
                ):
                    raise HostConvergeError(
                        "rollback target must equal the recorded previous SHA",
                    )
                authority = _identity("root", SHARED_GROUP)
                current = profile.candidate_root / current_sha
                target = profile.candidate_root / target_sha
                verify_candidate(profile, current, current_sha, authority)
                target_tree = verify_candidate(profile, target, target_sha, authority)
                if _migration_tree(current, authority) != _migration_tree(
                    target,
                    authority,
                ):
                    raise HostConvergeError(
                        "rollback crosses a migration-tree change; "
                        "restore a reviewed data backup instead",
                    )
                verify_worker_runtime_env(
                    profile,
                    target_sha,
                    _identity(
                        profile.sandbox,
                        f"loom-sandbox-{profile.sandbox}",
                    ),
                )
                receipt = _verify_archived_activation(
                    profile,
                    target_sha,
                    target_tree,
                )
                replacement = _desired_payload(
                    profile,
                    target_sha,
                    target_tree,
                    previous_sha=current_sha,
                    receipt=receipt,
                )
                previous_relay = _current_relay_sha(profile)
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="preparing",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
                _atomic_write(
                    profile.desired_file,
                    (
                        json.dumps(
                            replacement,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode(),
                    mode=0o600,
                )
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="desired-written",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
            _run(("systemctl", "start", UNIT_NAME.format(sandbox=profile.sandbox)))
            with _activation_lock(profile):
                transaction = _transaction_payload(profile)
                if (
                    transaction is None
                    or transaction["operation"] != "rollback"
                    or transaction["candidate_sha"] != target_sha
                    or transaction["phase"] != "desired-written"
                ):
                    raise HostConvergeError(
                        "rollback transaction changed during sandbox convergence",
                    )
                _restore_relay(profile, target_sha, current_sha)
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="link-installed",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                )
                target_receipt = _renew_attestation_locked(
                    profile,
                    sha=target_sha,
                    tree=target_tree,
                )
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="domains-proved",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                    target_receipt_sha256=target_receipt.payload_sha256,
                )
                service_check(profile.sandbox)
                _write_transaction(
                    profile,
                    operation="rollback",
                    sha=target_sha,
                    tree=target_tree,
                    phase="committed",
                    previous_desired=desired,
                    previous_relay_sha=previous_relay,
                    target_receipt_sha256=target_receipt.payload_sha256,
                )
                _remove_transaction(profile)
        except Exception:
            with _activation_lock(profile):
                transaction = _transaction_payload(profile)
                if transaction is not None:
                    try:
                        _recover_transaction(profile, transaction)
                    except Exception as recovery_exc:
                        raise HostConvergeError(
                            f"{profile.sandbox} rollback and previous-candidate "
                            "recovery both failed",
                        ) from recovery_exc
            raise


def _nfs_readback_commands(_profile: Profile, _sha: str) -> list[list[str]]:
    return [
        [
            str(NODE_TRANSPORT_PROGRAM),
            "invoke",
            "--node",
            host,
            "--verb",
            "check",
        ]
        for host in ("oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4", "oldlab-5")
    ]


def plan_document(profiles: Sequence[Profile], sha: str, operation: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate SHA must be full lowercase 40-hex")
    rows = []
    for profile in profiles:
        rows.append(
            {
                "sandbox": profile.sandbox,
                "compose_project": profile.compose_project,
                "candidate": str(profile.candidate_root / sha),
                "candidate_owner": f"root:{SHARED_GROUP}",
                "candidate_group_world_writable": False,
                "worker_runtime_env": str(profile.worker_runtime_env(sha)),
                "combined_receipt": str(combined_receipt_path(profile, sha)),
                "state_root": str(profile.state_root),
                "private_owner": f"{profile.sandbox}:{SHARED_GROUP}",
                "private_mode": "0700",
                "secrets_env": str(profile.secrets_env),
                "admin_secret_file": str(profile.admin_secret),
                "secret_mode": "0600",
                "ports": profile.ports,
                "unit": UNIT_NAME.format(sandbox=profile.sandbox),
                "nfs_readback_commands": _nfs_readback_commands(profile, sha),
            },
        )
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-host-plan",
        "operation": operation,
        "mutation_authorized": False,
        "host": EXPECTED_HOSTNAME,
        "candidate_sha": sha,
        "sandboxes": rows,
        "node_authority": {
            "program": str(NODE_AUTHORITY_PROGRAM),
            "runtime_verbs": ["transact", "check"],
            "external_root_bootstrap_required": True,
            "candidate_tree_pinned": True,
            "nodes": list(ELIGIBLE_LINK_NODES),
            "raw_remote_sudo_allowed": False,
        },
        "rollback": {
            "preserves_compose_volumes": True,
            "requires_recorded_previous_sha": True,
            "requires_equal_migration_tree": True,
        },
    }


def _transaction_file(profile: Profile) -> Path:
    return TRANSACTION_ROOT / f"{profile.sandbox}.json"


@contextmanager
def _activation_lock(profile: Profile) -> Iterator[None]:
    _ensure_root_private_directory(TRANSACTION_LOCK_ROOT)
    lock_path = TRANSACTION_LOCK_ROOT / f"{profile.sandbox}.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or (metadata.st_uid, metadata.st_gid) != (0, 0):
            raise HostConvergeError("sandbox activation lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


@contextmanager
def _install_lock() -> Iterator[None]:
    _ensure_root_private_directory(TRANSACTION_LOCK_ROOT)
    lock_path = TRANSACTION_LOCK_ROOT / "install.lock"
    descriptor = os.open(
        lock_path,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or (metadata.st_uid, metadata.st_gid) != (0, 0):
            raise HostConvergeError("global install lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _write_transaction(
    profile: Profile,
    *,
    operation: str = "install",
    sha: str,
    tree: str,
    phase: str,
    previous_desired: Mapping[str, Any] | None,
    previous_relay_sha: str | None,
    target_receipt_sha256: str | None = None,
) -> None:
    now = datetime.now(UTC)
    existing = _load_json(_transaction_file(profile), "sandbox activation transaction")
    started_at = now
    expires_at = now + TRANSACTION_TTL
    if (
        isinstance(existing, dict)
        and existing.get("sandbox") == profile.sandbox
        and existing.get("operation", "install") == operation
        and existing.get("candidate_sha") == sha
        and existing.get("candidate_tree") == tree
    ):
        started_at = _parse_attestation_time(
            existing.get("started_at"),
            "transaction started_at",
        )
        expires_at = _parse_attestation_time(
            existing.get("expires_at"),
            "transaction expires_at",
        )
        if target_receipt_sha256 is None:
            existing_receipt = existing.get("target_receipt_sha256")
            if isinstance(existing_receipt, str):
                target_receipt_sha256 = existing_receipt
    payload = {
        "schema_version": 3,
        "sandbox": profile.sandbox,
        "operation": operation,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "phase": phase,
        "started_at": started_at.isoformat(),
        "expires_at": expires_at.isoformat(),
        "previous_desired": dict(previous_desired) if previous_desired is not None else None,
        "previous_relay_sha": previous_relay_sha,
        "target_receipt_sha256": target_receipt_sha256,
    }
    _ensure_root_private_directory(TRANSACTION_ROOT)
    _atomic_write(
        _transaction_file(profile),
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )


def _transaction_payload(profile: Profile) -> dict[str, Any] | None:
    payload = _load_json(_transaction_file(profile), "sandbox activation transaction")
    if payload is None:
        return None
    schema_version = payload.get("schema_version")
    expected_keys = {
        "schema_version",
        "sandbox",
        "candidate_sha",
        "candidate_tree",
        "phase",
        "started_at",
        "expires_at",
        "previous_desired",
        "previous_relay_sha",
    }
    if schema_version in {2, 3}:
        expected_keys.add("operation")
    if schema_version == 3:
        expected_keys.add("target_receipt_sha256")
    _exact_keys(
        payload,
        expected_keys,
        "sandbox activation transaction",
    )
    operation = payload.get("operation", "install")
    if (
        schema_version not in {1, 2, 3}
        or payload["sandbox"] != profile.sandbox
        or operation not in {"install", "rollback"}
        or SHA_RE.fullmatch(str(payload["candidate_sha"])) is None
        or SHA_RE.fullmatch(str(payload["candidate_tree"])) is None
        or payload["phase"]
        not in {
            "prepared",
            "preparing",
            "link-installed",
            "fleet-proved",
            "domains-proved",
            "desired-written",
            "committed",
        }
        or (
            payload["previous_desired"] is not None
            and not isinstance(payload["previous_desired"], dict)
        )
        or (
            payload["previous_relay_sha"] is not None
            and SHA_RE.fullmatch(str(payload["previous_relay_sha"])) is None
        )
        or (
            schema_version == 3
            and payload["target_receipt_sha256"] is not None
            and (
                not isinstance(payload["target_receipt_sha256"], str)
                or DIGEST_RE.fullmatch(payload["target_receipt_sha256"]) is None
            )
        )
        or (
            operation != "rollback"
            and schema_version == 3
            and payload["target_receipt_sha256"] is not None
        )
        or (
            operation == "rollback"
            and schema_version == 3
            and payload["phase"] in {"domains-proved", "committed"}
            and payload["target_receipt_sha256"] is None
        )
    ):
        raise HostConvergeError("sandbox activation transaction binding is invalid")
    _parse_attestation_time(payload["started_at"], "transaction started_at")
    _parse_attestation_time(payload["expires_at"], "transaction expires_at")
    payload["operation"] = operation
    payload["target_receipt_sha256"] = payload.get("target_receipt_sha256")
    return payload


def _remove_transaction(profile: Profile) -> None:
    path = _transaction_file(profile)
    if path.exists():
        path.unlink()
        _fsync_directory(path.parent)


def _current_relay_sha(profile: Profile) -> str | None:
    current = REMOTE_LINK_SERVER_ROOT / profile.sandbox / "current"
    try:
        metadata = current.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HostConvergeError("sandbox relay current pointer is unavailable") from exc
    if not stat.S_ISLNK(metadata.st_mode):
        raise HostConvergeError("sandbox relay current pointer is invalid")
    target = os.readlink(current)
    prefix = "candidates/"
    sha = target.removeprefix(prefix)
    if target != prefix + sha or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("sandbox relay current pointer is invalid")
    return sha


def _restore_relay(profile: Profile, target_sha: str | None, transaction_sha: str) -> None:
    program = "scripts/ops/developer_sandbox_remote_link_host.py"
    if target_sha is not None:
        _run_candidate_program(
            profile,
            target_sha,
            program,
            "rollback-server",
            "--sandbox",
            profile.sandbox,
            "--candidate-sha",
            target_sha,
            "--execute",
        )
        return
    unit = f"loom-developer-sandbox-link@{profile.sandbox}.service"
    _run(("systemctl", "disable", "--now", unit), expected={0, 1, 5})
    current = REMOTE_LINK_SERVER_ROOT / profile.sandbox / "current"
    if current.is_symlink():
        current.unlink()
        _fsync_directory(current.parent)


def _invalidate_receipt(profile: Profile, sha: str) -> None:
    path = combined_receipt_path(profile, sha)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(metadata.st_mode):
        raise HostConvergeError("combined activation receipt path is a directory")
    path.unlink()
    _fsync_directory(path.parent)


def _invalidate_exact_live_receipt(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    journal_digest: str | None,
) -> None:
    path = combined_receipt_path(profile, sha)
    parent_fd = -1
    descriptor = -1
    try:
        try:
            parent_fd = _open_absolute_directory(path.parent, create=False)
        except FileNotFoundError:
            return
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            return
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or (opened.st_uid, opened.st_gid) != (os.geteuid(), os.getegid())
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise HostConvergeError("live activation receipt metadata is invalid")
        chunks: list[bytes] = []
        size = 0
        while True:
            chunk = os.read(descriptor, 1024 * 1024)
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > 8 * 1024 * 1024:
                raise HostConvergeError("live activation receipt is too large")
        raw = b"".join(chunks)
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise HostConvergeError("live activation receipt is invalid") from exc
        if not isinstance(payload, dict):
            raise HostConvergeError("live activation receipt is invalid")
        canonical = (
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode()
            + b"\n"
        )
        digest = payload.get("payload_sha256")
        unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
        expected_digest = hashlib.sha256(
            json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode(),
        ).hexdigest()
        if (
            raw != canonical
            or payload.get("schema_version") != 1
            or payload.get("kind") != "loom.developer-runtime-combined-activation"
            or payload.get("sandbox") != profile.sandbox
            or payload.get("candidate_sha") != sha
            or payload.get("candidate_tree") != tree
            or not isinstance(digest, str)
            or digest != expected_digest
        ):
            raise HostConvergeError(
                "live activation receipt does not match the rollback transaction",
            )
        if journal_digest is not None and DIGEST_RE.fullmatch(journal_digest) is None:
            raise HostConvergeError("rollback target receipt journal binding is invalid")
        if journal_digest is not None and digest != journal_digest:
            desired = _load_json(profile.desired_file, "sandbox desired state")
            if (
                desired is None
                or desired.get("candidate_sha") != sha
                or desired.get("candidate_tree") != tree
                or desired.get("combined_receipt_sha256") != digest
            ):
                raise HostConvergeError(
                    "live activation receipt advanced outside the rollback transaction",
                )
        rebound = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
        if not stat.S_ISREG(rebound.st_mode) or (rebound.st_dev, rebound.st_ino) != (
            opened.st_dev,
            opened.st_ino,
        ):
            raise HostConvergeError("live activation receipt changed during invalidation")
        os.unlink(path.name, dir_fd=parent_fd)
        os.fsync(parent_fd)
    except HostConvergeError:
        raise
    except OSError as exc:
        raise HostConvergeError("could not invalidate live activation receipt") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if parent_fd >= 0:
            os.close(parent_fd)


def _recover_transaction(profile: Profile, transaction: Mapping[str, Any]) -> None:
    if transaction.get("phase") == "committed":
        _remove_transaction(profile)
        return
    sha = str(transaction["candidate_sha"])
    operation = str(transaction.get("operation", "install"))
    previous = transaction["previous_desired"]
    previous_relay = transaction["previous_relay_sha"]
    if operation == "rollback":
        _invalidate_exact_live_receipt(
            profile,
            sha,
            str(transaction["candidate_tree"]),
            journal_digest=transaction.get("target_receipt_sha256"),
        )
    if previous is None:
        if profile.desired_file.exists():
            profile.desired_file.unlink()
            _fsync_directory(profile.desired_file.parent)
        current_state = _sandbox_state_sha(profile)
        lifecycle_operation = "destroy" if current_state == sha else "prepare-stop"
        try:
            _invoke_lifecycle(profile, sha, lifecycle_operation)
        except HostConvergeError:
            if lifecycle_operation != "prepare-stop":
                raise
    else:
        previous_sha = previous.get("candidate_sha")
        previous_tree = previous.get("candidate_tree")
        if (
            not isinstance(previous_sha, str)
            or SHA_RE.fullmatch(previous_sha) is None
            or (
                operation == "rollback"
                and (not isinstance(previous_tree, str) or SHA_RE.fullmatch(previous_tree) is None)
            )
        ):
            raise HostConvergeError("previous desired state in transaction is invalid")
        _atomic_write(
            profile.desired_file,
            (json.dumps(previous, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            mode=0o600,
        )
        _invoke_lifecycle(profile, previous_sha, "update")
    _restore_relay(profile, previous_relay, sha)
    if operation == "install":
        _invalidate_receipt(profile, sha)
    elif previous is not None:
        assert isinstance(previous_tree, str)
        try:
            _renew_attestation_locked(
                profile,
                sha=previous_sha,
                tree=previous_tree,
            )
        except Exception:
            _invalidate_exact_live_receipt(
                profile,
                previous_sha,
                previous_tree,
                journal_digest=None,
            )
            raise
    _remove_transaction(profile)


def _install_materialized(
    profiles: Sequence[Profile],
    sha: str,
    source_bundle: Path,
    source_tree: str,
) -> None:
    authority = _identity("root", SHARED_GROUP)
    assets_installed = False
    fingerprints: dict[tuple[str, str], str] = {}
    candidates: list[tuple[Profile, str, Identity]] = []
    if profiles:
        _bootstrap_domain_runtime_hosts(profiles[0], sha, source_tree)
    for profile in profiles:
        owner = _identity(profile.sandbox, SHARED_GROUP)
        runtime_group = _sandbox_batch_identity(profile.sandbox)
        verify_developer_docker_access(owner)
        ensure_secret_files(profile, owner)
        _materialize_domain_candidates(profile, sha, source_tree, source_bundle)
        verify_candidate_root(profile, authority)
        tree = verify_candidate(profile, profile.candidate_root / sha, sha, authority)
        if tree != source_tree:
            raise HostConvergeError("materialized candidate tree differs from source bundle")
        if not assets_installed:
            _install_assets(profile.candidate_root / sha)
            assets_installed = True
        verify_candidate_profile_bytes(profile, sha, authority)
        verify_candidate_consumer(profile, sha, owner)
        require_migration_compatible_update(profile, sha, authority)
        _converge_domain_runtime_hosts(profile, sha, tree, authority)
        values = _parse_env_file(profile.secrets_env)
        admin = _read_admin_token(profile.admin_secret)
        for key in (
            "LOOM_DEV_POSTGRES_PASSWORD",
            "LOOM_DEV_MINIO_ROOT_PASSWORD",
            "LOOM_CP_STEP_JWT_SIGNING_KEY",
            "LOOM_SECRET_STORE_MASTER_KEY",
            "LOOM_WORKER_TOKEN",
        ):
            fingerprints[(profile.sandbox, key)] = hashlib.sha256(
                values[key].encode(),
            ).hexdigest()
        fingerprints[(profile.sandbox, "admin")] = hashlib.sha256(
            admin.encode(),
        ).hexdigest()
        candidates.append((profile, tree, runtime_group))
    for key in {key for _, key in fingerprints}:
        matching_fingerprints = [
            fingerprint
            for (sandbox, candidate_key), fingerprint in fingerprints.items()
            if candidate_key == key
        ]
        if len(matching_fingerprints) != len(set(matching_fingerprints)):
            raise HostConvergeError(f"cross-sandbox secret collision detected for {key}")
    for profile, tree, runtime_group in candidates:
        previous: dict[str, Any] | None = None
        previous_relay: str | None = None
        try:
            with _activation_lock(profile):
                orphan = _transaction_payload(profile)
                if orphan is not None:
                    _recover_transaction(profile, orphan)
                previous = _load_json(profile.desired_file, "sandbox desired state")
                previous_relay = _current_relay_sha(profile)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="preparing",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _assert_capacity_units_stopped(profile)
                _invoke_lifecycle(profile, sha, "prepare")
                verify_listening_ports(profile)
                assert_capacity_quiescent(profile)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="prepared",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _install_remote_link_fleet(profile, sha, tree, authority)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="fleet-proved",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _publish_domain_attestations(profile, sha, tree)
                verify_worker_runtime_env(profile, sha, runtime_group)
                receipt = verify_combined_receipt(profile, sha, tree)
                _archive_runtime_attestation(profile, sha, tree, receipt)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="domains-proved",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                write_desired(profile, sha, tree, receipt)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="desired-written",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
            unit = UNIT_NAME.format(sandbox=profile.sandbox)
            _run(("systemctl", "enable", unit))
            _run(("systemctl", "restart", unit))
            with _activation_lock(profile):
                service_check(profile.sandbox)
                _write_transaction(
                    profile,
                    sha=sha,
                    tree=tree,
                    phase="committed",
                    previous_desired=previous,
                    previous_relay_sha=previous_relay,
                )
                _remove_transaction(profile)
        except Exception:
            with _activation_lock(profile):
                transaction = _transaction_payload(profile)
                if transaction is not None:
                    try:
                        _recover_transaction(profile, transaction)
                    except Exception as recovery_exc:
                        raise HostConvergeError(
                            f"{profile.sandbox} activation and previous-candidate "
                            "recovery both failed",
                        ) from recovery_exc
            raise
    if candidates:
        _run(("systemctl", "enable", "--now", RENEWAL_TIMER))


def install(profiles: Sequence[Profile], sha: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    with _install_lock():
        with _candidate_source_stage(sha) as (source_bundle, source_tree):
            _install_materialized(
                profiles,
                sha,
                source_bundle,
                source_tree,
            )


def _select_profiles(all_profiles: Sequence[Profile], sandbox: str) -> tuple[Profile, ...]:
    if sandbox == "all":
        return tuple(all_profiles)
    return tuple(profile for profile in all_profiles if profile.sandbox == sandbox)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)

    def legacy_seed_gate(child: argparse.ArgumentParser) -> None:
        child.add_argument(
            "--legacy-v1-seed-migrate",
            action="store_true",
            required=True,
            help="authorize only migration convergence of the three legacy-v1 seed rows",
        )

    for command in (
        "environment-create",
        "environment-update",
        "environment-check",
        "environment-resume",
        "environment-rollback",
        "environment-destroy",
        "environment-retire",
    ):
        environment = subparsers.add_parser(command, allow_abbrev=False)
        if command == "environment-resume":
            environment.add_argument("--runtime-id", required=True)
        else:
            environment.add_argument("--env-id", required=True)
        if command in {
            "environment-create",
            "environment-update",
            "environment-rollback",
        }:
            environment.add_argument("--idempotency-key", required=True)
        if command in {"environment-create", "environment-update"}:
            environment.add_argument("--candidate-id", required=True)
        if command != "environment-check":
            environment.add_argument("--execute", action="store_true")
    for command in ("plan", "install", "check"):
        child = subparsers.add_parser(command)
        legacy_seed_gate(child)
        child.add_argument("--candidate-sha", required=True)
        child.add_argument(
            "--sandbox",
            choices=(*LEGACY_SEED_RUNTIME_IDS, "all"),
            default="all",
        )
        if command != "plan":
            child.add_argument("--execute", action="store_true")
    rollback_parser = subparsers.add_parser("rollback")
    legacy_seed_gate(rollback_parser)
    rollback_parser.add_argument(
        "--sandbox",
        choices=LEGACY_SEED_RUNTIME_IDS,
        required=True,
    )
    rollback_parser.add_argument("--candidate-sha", required=True)
    rollback_parser.add_argument("--execute", action="store_true")
    for command in ("slurm-converge", "slurm-check", "slurm-rollback"):
        child = subparsers.add_parser(command)
        legacy_seed_gate(child)
        child.add_argument("--domain", choices=tuple(DOMAIN_PEERS), required=True)
        child.add_argument("--sandbox", choices=("all",), required=True)
        child.add_argument("--candidate-sha", required=True)
        child.add_argument("--execute", action="store_true")
    for command in ("service-converge", "service-check"):
        child = subparsers.add_parser(command)
        legacy_seed_gate(child)
        child.add_argument(
            "--sandbox",
            choices=LEGACY_SEED_RUNTIME_IDS,
            required=True,
        )
    renew = subparsers.add_parser("renew-attestations")
    legacy_seed_gate(renew)
    renew.add_argument(
        "--sandbox",
        choices=(*LEGACY_SEED_RUNTIME_IDS, "all"),
        default="all",
    )
    renew.add_argument("--execute", action="store_true")
    staging_identity = subparsers.add_parser(
        "staging-allocation-identity-converge",
        allow_abbrev=False,
    )
    staging_identity.add_argument("--candidate-sha", required=True)
    staging_identity.add_argument("--candidate-tree", required=True)
    staging_identity.add_argument("--authority-generation", required=True, type=int)
    staging_identity.add_argument("--authority-convergence-id", required=True)
    staging_identity.add_argument("--authority-request-id", required=True)
    staging_identity.add_argument("--authority-requested-at", required=True)
    staging_identity.add_argument("--execute", action="store_true")
    for command in (
        "staging-allocation-node-check",
        "staging-allocation-worker",
        "staging-allocation-probe",
        "staging-allocation-query",
        "staging-allocation-submit",
        "staging-allocation-cancel",
    ):
        staging = subparsers.add_parser(command, allow_abbrev=False)
        staging.add_argument("--candidate-sha", required=True)
        staging.add_argument("--candidate-tree", required=True)
        staging.add_argument("--request-id", required=True)
        if command == "staging-allocation-query":
            staging.add_argument("--job-id", action="append", default=[])
            staging.add_argument("--node", action="append", default=[])
        if command in {"staging-allocation-probe", "staging-allocation-submit"}:
            if command == "staging-allocation-submit":
                staging.add_argument("--requested-node", required=True)
            staging.add_argument("--execute", action="store_true")
        if command == "staging-allocation-cancel":
            staging.add_argument("--submit-request-id", required=True)
            staging.add_argument("--job-id", required=True)
            staging.add_argument("--requested-node", required=True)
            staging.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    if args.command.startswith("environment-"):
        from scripts.ops.developer_environment_deploy import main as environment_main

        delegated = [
            args.command.removeprefix("environment-"),
        ]
        if getattr(args, "env_id", None) is not None:
            delegated.extend(("--env-id", args.env_id))
        if getattr(args, "runtime_id", None) is not None:
            delegated.extend(("--runtime-id", args.runtime_id))
        if getattr(args, "candidate_id", None) is not None:
            delegated.extend(("--candidate-id", args.candidate_id))
        if getattr(args, "idempotency_key", None) is not None:
            delegated.extend(("--idempotency-key", args.idempotency_key))
        if bool(getattr(args, "execute", False)):
            delegated.append("--execute")
        return environment_main(delegated)
    try:
        if args.command == "staging-allocation-identity-converge":
            if not args.execute:
                raise HostConvergeError("staging identity convergence requires --execute")
            result = staging_allocation_identity_converge(
                load_staging_allocation_config(),
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                authority_generation=args.authority_generation,
                authority_convergence_id=args.authority_convergence_id,
                authority_request_id=args.authority_request_id,
                authority_requested_at=args.authority_requested_at,
            )
        elif args.command == "staging-allocation-node-check":
            result = staging_allocation_node_check(
                load_staging_allocation_config(),
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                request_id=args.request_id,
            )
        elif args.command == "staging-allocation-worker":
            result = staging_allocation_node_check(
                load_staging_allocation_config(),
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                request_id=args.request_id,
                steady=True,
            )
        elif args.command == "staging-allocation-probe":
            if not args.execute:
                raise HostConvergeError("staging allocation probe requires --execute")
            result = staging_allocation_probe(
                load_staging_allocation_config(),
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                request_id=args.request_id,
            )
        elif args.command == "staging-allocation-query":
            result = staging_allocation_query(
                load_staging_allocation_config(),
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                request_id=args.request_id,
                job_ids=tuple(args.job_id),
                nodes=tuple(args.node),
            )
        elif args.command == "staging-allocation-submit":
            if not args.execute:
                raise HostConvergeError("staging allocation submit requires --execute")
            result = staging_allocation_submit(
                load_staging_allocation_config(),
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                request_id=args.request_id,
                requested_node=args.requested_node,
            )
        elif args.command == "staging-allocation-cancel":
            if not args.execute:
                raise HostConvergeError("staging allocation cancel requires --execute")
            result = staging_allocation_cancel(
                load_staging_allocation_config(),
                candidate_sha=args.candidate_sha,
                candidate_tree=args.candidate_tree,
                request_id=args.request_id,
                submit_request_id=args.submit_request_id,
                job_id=args.job_id,
                requested_node=args.requested_node,
            )
        elif args.command == "service-converge":
            service_converge(args.sandbox)
            result = {"status": "succeeded", "sandbox": args.sandbox}
        elif args.command == "service-check":
            service_check(args.sandbox)
            result = {"status": "succeeded", "sandbox": args.sandbox}
        elif args.command == "renew-attestations":
            profiles = load_profiles()
            selected = _select_profiles(profiles, args.sandbox)
            renew_attestations(selected, execute=args.execute)
            result = {
                "status": "succeeded",
                "sandboxes": [profile.sandbox for profile in selected],
            }
        elif args.command in {"slurm-converge", "slurm-check", "slurm-rollback"}:
            if SHA_RE.fullmatch(args.candidate_sha) is None:
                raise HostConvergeError("candidate SHA must be full lowercase 40-hex")
            profiles = load_profiles()
            selected = _select_profiles(profiles, args.sandbox)
            qianyi = next(profile for profile in selected if profile.sandbox == "qianyi")
            result = {
                "schema_version": 1,
                "artifact_type": "developer-sandbox-slurm-maintenance-plan",
                "operation": args.command,
                "domain": args.domain,
                "sandbox": args.sandbox,
                "candidate_sha": args.candidate_sha,
                "controller": DOMAIN_PUBLISHERS[args.domain],
                "node_order": list(_slurm_node_order(args.domain)),
                "controller_last": True,
                "mutation_authorized": False,
            }
            if args.execute:
                operation = {
                    "slurm-converge": slurm_maintenance_converge,
                    "slurm-check": slurm_maintenance_check,
                    "slurm-rollback": slurm_maintenance_rollback,
                }[args.command]
                result = operation(qianyi, args.candidate_sha, args.domain)
        else:
            profiles = load_profiles()
            selected = _select_profiles(profiles, args.sandbox)
            result = plan_document(selected, args.candidate_sha, args.command)
            execute = bool(getattr(args, "execute", False))
            if execute and args.command == "install":
                install(selected, args.candidate_sha)
                result = {**result, "mutation_authorized": True, "status": "succeeded"}
            elif execute and args.command == "check":
                for profile in selected:
                    service_check(profile.sandbox)
                result = {
                    **result,
                    "mutation_authorized": False,
                    "verified": True,
                    "status": "succeeded",
                }
            elif execute and args.command == "rollback":
                rollback(selected[0], args.candidate_sha)
                result = {**result, "mutation_authorized": True, "status": "succeeded"}
        sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except HostConvergeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
