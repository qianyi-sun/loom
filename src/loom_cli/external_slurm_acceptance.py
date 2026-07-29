"""Independent staging external-Slurm acceptance authority.

The checked-in environment profile may require this authority, but it cannot
name an artifact or claim that acceptance passed.  Production callers derive
the immutable artifact path from the fixed root-installed configuration and
verify a detached Ed25519 signature before consuming the receipt.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import stat
import subprocess
import tomllib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

DEFAULT_CONFIG_PATH = Path("/etc/loom/staging-external-slurm-authority/authority.toml")
DEFAULT_PROGRAM = Path("/usr/local/libexec/loom-staging-external-slurm-authority")
_OBJECT_ID_RE = re.compile(r"[0-9a-f]{40}\Z")
_GENERATION_ID_RE = re.compile(r"[0-9a-f]{64}\Z")
_MAX_CONFIG_BYTES = 128 * 1024
_MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
_MAX_SIGNATURE_BYTES = 512
_AUTHORITY_CONFIG_FIELDS = frozenset(
    {
        "schema_version",
        "environment",
        "pool",
        "source_host",
        "submit_host",
        "controller",
        "cluster",
        "partition",
        "producer_user",
        "producer_group",
        "producer_uid",
        "producer_gid",
        "producer_home",
        "producer_shell",
        "batch_user",
        "batch_group",
        "batch_uid",
        "batch_gid",
        "batch_home",
        "batch_shell",
        "batch_supplementary_groups",
        "slurm_account",
        "qos",
        "artifact_root",
        "public_key",
        "private_key",
        "supervisor_service",
        "supervisor_timer",
        "max_age_seconds",
        "infrastructure_nodes",
        "allowed_nodes",
        "installation",
        "infrastructure",
        "host_aliases",
        "shared_mount",
        "submission_broker",
        "candidate_paths",
        "producer_paths",
        "probe",
    }
)
_FIXED_INSTALLATION = {
    "source_root": "/opt/loom-developer-sandbox-node-authority/source",
    "candidate_runtime_template": (
        "/opt/loom-staging-runner/candidates/{candidate_sha}/venv/bin/python"
    ),
    "wrapper": "/usr/local/libexec/loom-staging-external-slurm-authority",
    "isolated_python": True,
    "required_modules": ["loom_cli", "cryptography"],
}
_FIXED_INFRASTRUCTURE = {
    "receipt_root": ("/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure"),
    "source_controller": "oldlab-2",
    "source_controller_host": "trt-eai-oldlab-2",
    "max_age_seconds": 3600,
}


class ExternalSlurmAcceptanceError(RuntimeError):
    """Raised when the external authority is unavailable or invalid."""


@dataclass(frozen=True)
class ExternalSlurmAuthorityConfig:
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
    slurm_account: str
    qos: str
    artifact_root: Path
    public_key: Path
    private_key: Path
    supervisor_service: str
    supervisor_timer: str
    max_age_seconds: int
    shared_mount_source: str
    shared_mount_target: Path
    shared_mount_filesystem_type: str
    shared_mount_unit: str
    repository_root: Path
    worker_env_root: Path
    result_root: Path
    broker_transport: Path
    broker_node: str
    broker_domain: str
    broker_sandbox: str
    broker_submit_action: str
    broker_cancel_action: str
    infrastructure_nodes: tuple[str, ...]
    allowed_nodes: tuple[str, ...]
    host_aliases: dict[str, str]
    repository_template: str
    worker_env_template: str
    producer_repository_template: str
    producer_worker_env_template: str
    environment_state_profile: Path
    probe_action: str
    probe_result_root: Path
    probe_job_timeout_seconds: int
    probe_heartbeat_interval_seconds: int


@dataclass(frozen=True)
class VerifiedExternalSlurmAuthority:
    payload: dict[str, Any]
    artifact_path: str
    artifact_sha256: str
    signature_sha256: str
    key_id: str


CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]


def _clean_string(raw: Mapping[str, Any], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value or value != value.strip():
        raise ExternalSlurmAcceptanceError(f"{field} must be a non-empty string")
    return value


def _clean_int(raw: Mapping[str, Any], field: str, *, minimum: int = 0) -> int:
    value = raw.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ExternalSlurmAcceptanceError(f"{field} must be an integer >= {minimum}")
    return value


def _clean_string_array(raw: Mapping[str, Any], field: str) -> tuple[str, ...]:
    value = raw.get(field)
    if not isinstance(value, list) or not value:
        raise ExternalSlurmAcceptanceError(f"{field} must be a non-empty array")
    cleaned: list[str] = []
    for index, item in enumerate(value):
        if not isinstance(item, str) or not item or item != item.strip():
            raise ExternalSlurmAcceptanceError(f"{field}[{index}] must be a non-empty string")
        cleaned.append(item)
    if len(set(cleaned)) != len(cleaned):
        raise ExternalSlurmAcceptanceError(f"{field} must not contain duplicates")
    return tuple(cleaned)


def _bounded_read(path: Path, *, maximum: int, label: str) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size > maximum
        ):
            raise ExternalSlurmAcceptanceError(f"{label} must be a bounded regular file")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > maximum:
                raise ExternalSlurmAcceptanceError(f"{label} exceeds its size limit")
            chunks.append(chunk)
        final = os.fstat(descriptor)
        if (
            final.st_dev != metadata.st_dev
            or final.st_ino != metadata.st_ino
            or final.st_size != metadata.st_size
            or total != metadata.st_size
        ):
            raise ExternalSlurmAcceptanceError(f"{label} changed while being read")
        return b"".join(chunks)
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} cannot be read") from exc
    finally:
        os.close(descriptor)


def load_authority_config(
    path: Path = DEFAULT_CONFIG_PATH,
) -> ExternalSlurmAuthorityConfig:
    if path == DEFAULT_CONFIG_PATH:
        _require_root_owned_file(
            path,
            label="authority config",
            allowed_modes={0o600, 0o640, 0o644},
        )
        _require_root_owned_parents(path, label="authority config")
    try:
        raw = tomllib.loads(
            _bounded_read(path, maximum=_MAX_CONFIG_BYTES, label="authority config").decode("utf-8")
        )
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ExternalSlurmAcceptanceError("authority config is invalid TOML") from exc
    if frozenset(raw) != _AUTHORITY_CONFIG_FIELDS:
        raise ExternalSlurmAcceptanceError(
            "authority config must contain the exact closed set of top-level fields"
        )
    if raw.get("schema_version") != 1:
        raise ExternalSlurmAcceptanceError("authority config schema_version must be 1")
    candidate_paths = raw.get("candidate_paths")
    producer_paths = raw.get("producer_paths")
    probe = raw.get("probe")
    host_aliases = raw.get("host_aliases")
    shared_mount = raw.get("shared_mount")
    submission_broker = raw.get("submission_broker")
    installation = raw.get("installation")
    infrastructure = raw.get("infrastructure")
    if (
        not isinstance(candidate_paths, dict)
        or not isinstance(producer_paths, dict)
        or not isinstance(probe, dict)
        or not isinstance(host_aliases, dict)
        or not isinstance(shared_mount, dict)
        or not isinstance(submission_broker, dict)
        or not isinstance(installation, dict)
        or not isinstance(infrastructure, dict)
    ):
        raise ExternalSlurmAcceptanceError(
            "authority config requires installation, infrastructure, host_aliases, "
            "shared_mount, submission_broker, candidate_paths, producer_paths, and "
            "probe tables"
        )
    if installation != _FIXED_INSTALLATION:
        raise ExternalSlurmAcceptanceError(
            "authority config installation table must match the fixed system installation"
        )
    if infrastructure != _FIXED_INFRASTRUCTURE:
        raise ExternalSlurmAcceptanceError(
            "authority config infrastructure table must match the fixed convergence authority"
        )
    allowed_nodes = _clean_string_array(raw, "allowed_nodes")
    infrastructure_nodes = _clean_string_array(raw, "infrastructure_nodes")
    config = ExternalSlurmAuthorityConfig(
        environment=_clean_string(raw, "environment"),
        pool=_clean_string(raw, "pool"),
        source_host=_clean_string(raw, "source_host"),
        submit_host=_clean_string(raw, "submit_host"),
        controller=_clean_string(raw, "controller"),
        cluster=_clean_string(raw, "cluster"),
        partition=_clean_string(raw, "partition"),
        producer_user=_clean_string(raw, "producer_user"),
        producer_group=_clean_string(raw, "producer_group"),
        producer_uid=_clean_int(raw, "producer_uid"),
        producer_gid=_clean_int(raw, "producer_gid"),
        producer_home=Path(_clean_string(raw, "producer_home")),
        producer_shell=Path(_clean_string(raw, "producer_shell")),
        batch_user=_clean_string(raw, "batch_user"),
        batch_group=_clean_string(raw, "batch_group"),
        batch_uid=_clean_int(raw, "batch_uid"),
        batch_gid=_clean_int(raw, "batch_gid"),
        batch_home=Path(_clean_string(raw, "batch_home")),
        batch_shell=Path(_clean_string(raw, "batch_shell")),
        batch_supplementary_groups=_clean_string_array(
            raw,
            "batch_supplementary_groups",
        ),
        slurm_account=_clean_string(raw, "slurm_account"),
        qos=_clean_string(raw, "qos"),
        artifact_root=Path(_clean_string(raw, "artifact_root")),
        public_key=Path(_clean_string(raw, "public_key")),
        private_key=Path(_clean_string(raw, "private_key")),
        supervisor_service=_clean_string(raw, "supervisor_service"),
        supervisor_timer=_clean_string(raw, "supervisor_timer"),
        max_age_seconds=_clean_int(raw, "max_age_seconds", minimum=1),
        shared_mount_source=_clean_string(shared_mount, "source"),
        shared_mount_target=Path(_clean_string(shared_mount, "target")),
        shared_mount_filesystem_type=_clean_string(
            shared_mount,
            "filesystem_type",
        ),
        shared_mount_unit=_clean_string(shared_mount, "unit"),
        repository_root=Path(_clean_string(shared_mount, "repository_root")),
        worker_env_root=Path(_clean_string(shared_mount, "worker_env_root")),
        result_root=Path(_clean_string(shared_mount, "result_root")),
        broker_transport=Path(_clean_string(submission_broker, "transport")),
        broker_node=_clean_string(submission_broker, "node"),
        broker_domain=_clean_string(submission_broker, "domain"),
        broker_sandbox=_clean_string(submission_broker, "sandbox"),
        broker_submit_action=_clean_string(submission_broker, "submit_action"),
        broker_cancel_action=_clean_string(submission_broker, "cancel_action"),
        infrastructure_nodes=infrastructure_nodes,
        allowed_nodes=allowed_nodes,
        host_aliases={
            str(key): str(value)
            for key, value in host_aliases.items()
            if isinstance(key, str) and isinstance(value, str)
        },
        repository_template=_clean_string(candidate_paths, "repository"),
        worker_env_template=_clean_string(candidate_paths, "worker_env"),
        producer_repository_template=_clean_string(producer_paths, "repository"),
        producer_worker_env_template=_clean_string(producer_paths, "worker_env"),
        environment_state_profile=Path(_clean_string(candidate_paths, "environment_state_profile")),
        probe_action=_clean_string(probe, "action"),
        probe_result_root=Path(_clean_string(probe, "result_root")),
        probe_job_timeout_seconds=_clean_int(
            probe,
            "job_timeout_seconds",
            minimum=30,
        ),
        probe_heartbeat_interval_seconds=_clean_int(
            probe,
            "heartbeat_interval_seconds",
            minimum=1,
        ),
    )
    absolute_paths = (
        config.artifact_root,
        config.public_key,
        config.private_key,
        config.environment_state_profile,
        config.producer_home,
        config.producer_shell,
        config.batch_home,
        config.batch_shell,
        config.probe_result_root,
        config.shared_mount_target,
        config.repository_root,
        config.worker_env_root,
        config.result_root,
        config.broker_transport,
    )
    if any(not path.is_absolute() or ".." in path.parts for path in absolute_paths):
        raise ExternalSlurmAcceptanceError("authority paths must be absolute and normalized")
    if config.environment != "staging" or config.pool != "gb10":
        raise ExternalSlurmAcceptanceError("authority config is restricted to staging/gb10")
    if (
        config.producer_user != "loom-rollout"
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
    ):
        raise ExternalSlurmAcceptanceError(
            "authority config must use the fixed producer and independent batch identities"
        )
    if (
        config.artifact_root != Path("/var/lib/loom-staging-external-slurm-authority")
        or config.public_key
        != Path("/etc/loom/staging-external-slurm-authority/authority-public.pem")
        or config.private_key
        != Path("/etc/loom/staging-external-slurm-authority/authority-private.pem")
        or config.public_key == config.private_key
    ):
        raise ExternalSlurmAcceptanceError(
            "authority config must use the fixed distinct key and artifact paths"
        )
    if (
        config.shared_mount_source != "192.168.20.12:/shared_work2/loom/staging"
        or config.shared_mount_target != Path("/srv/loom/staging-shared")
        or config.shared_mount_filesystem_type != "nfs4"
        or config.shared_mount_unit != r"srv-loom-staging\x2dshared.mount"
        or config.repository_root != config.shared_mount_target / "candidates"
        or config.worker_env_root != config.shared_mount_target / "generated"
        or config.result_root != config.shared_mount_target / "results"
        or config.probe_result_root != config.result_root
    ):
        raise ExternalSlurmAcceptanceError(
            "authority config must use the fixed system staging mount"
        )
    if config.producer_repository_template != (
        "/var/lib/loom-staging-rollout/prepared/candidates/loom-remote-worker-{image_tag}"
    ) or config.producer_worker_env_template != (
        "/var/lib/loom-staging-rollout/prepared/generated/staging-gb10-worker-{image_tag}.env"
    ):
        raise ExternalSlurmAcceptanceError(
            "authority config must use the fixed private producer namespace"
        )
    if (
        config.broker_transport != Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
        or config.broker_node != config.submit_host
        or config.broker_domain != "gb10"
        or config.broker_sandbox != "staging"
        or config.broker_submit_action != "staging-allocation-submit"
        or config.broker_cancel_action != "staging-allocation-cancel"
        or config.probe_action != "staging-allocation-probe"
    ):
        raise ExternalSlurmAcceptanceError("authority config submission broker binding is invalid")
    expected_infrastructure_nodes = tuple(f"trt-gb10-{index}" for index in range(1, 16))
    if config.allowed_nodes != expected_infrastructure_nodes:
        raise ExternalSlurmAcceptanceError(
            "authority config must contain the exact 15-node GB10 acceptance set"
        )
    if config.infrastructure_nodes != expected_infrastructure_nodes:
        raise ExternalSlurmAcceptanceError(
            "authority config must contain the exact 15-node GB10 infrastructure set"
        )
    if (
        not set(config.allowed_nodes).issubset(config.infrastructure_nodes)
        or set(config.host_aliases) != set(config.infrastructure_nodes)
        or len(set(config.host_aliases.values())) != len(config.infrastructure_nodes)
        or any(not value for value in config.host_aliases.values())
    ):
        raise ExternalSlurmAcceptanceError(
            "authority config host_aliases must match the infrastructure node set"
        )
    return config


def canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


INFRASTRUCTURE_VERIFICATION_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "candidate_sha",
        "candidate_tree",
        "generation",
        "convergence_id",
        "requested_at",
        "request_sha256",
        "receipt_path",
        "payload_sha256",
        "source_controller",
        "source_controller_host",
        "created_at",
        "expires_at",
        "source_bootstrap",
        "accounting",
        "infrastructure_nodes",
        "node_bootstraps",
        "mount_contract",
        "mount_digests",
        "mount_digest",
        "source_digest",
        "boot_ids",
        "node_count",
        "result",
    }
)
INFRASTRUCTURE_OPERATION_FIELDS = frozenset(
    {
        "action",
        "node",
        "request_id",
        "payload_sha256",
        "result_sha256",
        "inner_receipt",
        "completed_at",
        "status",
    }
)
_BOOT_ID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z"
)


def infrastructure_mount_digest(
    mount_contract: Mapping[str, Any],
) -> str:
    """Return the stable mount binding shared by producer and consumers."""

    return hashlib.sha256(
        canonical_json_bytes({"mount_contract": dict(mount_contract)})
    ).hexdigest()


def infrastructure_candidate_source_digest(
    *,
    candidate_sha: str,
    candidate_tree: str,
    unit_set_digest: str,
) -> str:
    """Return the stable candidate/unit-source binding for external-v2."""

    return hashlib.sha256(
        canonical_json_bytes(
            {
                "candidate_sha": candidate_sha,
                "candidate_tree": candidate_tree,
                "unit_set_digest": unit_set_digest,
            }
        )
    ).hexdigest()


def validate_infrastructure_verification_summary(
    payload: Mapping[str, Any],
    *,
    candidate_sha: str,
    candidate_tree: str,
    receipt_path: str,
    expected_hosts: Sequence[str],
    expected_mount_contract: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Validate the exact root-producer summary consumed by external-v2."""

    hosts = tuple(expected_hosts)
    boot_ids = payload.get("boot_ids")
    mount_digests = payload.get("mount_digests")
    node_bootstraps = payload.get("node_bootstraps")
    infrastructure_nodes = payload.get("infrastructure_nodes")
    source = payload.get("source_bootstrap")
    accounting = payload.get("accounting")
    if (
        set(payload) != INFRASTRUCTURE_VERIFICATION_FIELDS
        or payload.get("schema_version") != 1
        or payload.get("kind") != "staging_external_slurm_infrastructure_verification"
        or payload.get("candidate_sha") != candidate_sha
        or payload.get("candidate_tree") != candidate_tree
        or isinstance(payload.get("generation"), bool)
        or not isinstance(payload.get("generation"), int)
        or payload["generation"] < 1
        or _GENERATION_ID_RE.fullmatch(str(payload.get("convergence_id"))) is None
        or _GENERATION_ID_RE.fullmatch(str(payload.get("request_sha256"))) is None
        or payload.get("receipt_path") != receipt_path
        or _GENERATION_ID_RE.fullmatch(str(payload.get("payload_sha256"))) is None
        or payload.get("source_controller") != "oldlab-2"
        or payload.get("source_controller_host") != "trt-eai-oldlab-2"
        or infrastructure_nodes != list(hosts)
        or payload.get("node_count") != len(hosts)
        or payload.get("mount_contract") != dict(expected_mount_contract)
        or not isinstance(mount_digests, dict)
        or set(mount_digests) != set(hosts)
        or any(
            not isinstance(value, str) or _GENERATION_ID_RE.fullmatch(value) is None
            for value in mount_digests.values()
        )
        or payload.get("mount_digest")
        != infrastructure_mount_digest(expected_mount_contract)
        or _GENERATION_ID_RE.fullmatch(str(payload.get("source_digest"))) is None
        or not isinstance(boot_ids, dict)
        or set(boot_ids) != set(hosts)
        or any(
            not isinstance(value, str) or _BOOT_ID_RE.fullmatch(value) is None
            for value in boot_ids.values()
        )
        or len(set(boot_ids.values())) != len(hosts)
        or payload.get("result") != "pass"
        or not isinstance(source, dict)
        or set(source) != INFRASTRUCTURE_OPERATION_FIELDS
        or not isinstance(accounting, dict)
        or set(accounting) != INFRASTRUCTURE_OPERATION_FIELDS
        or not isinstance(node_bootstraps, list)
        or len(node_bootstraps) != len(hosts)
        or any(
            not isinstance(item, dict) or set(item) != INFRASTRUCTURE_OPERATION_FIELDS
            for item in node_bootstraps
        )
    ):
        raise ExternalSlurmAcceptanceError(
            "infrastructure verification summary binding is invalid"
        )
    expected_request = {
        "schema_version": 1,
        "kind": "loom.staging-external-slurm.infrastructure-converge-request",
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "convergence_id": payload["convergence_id"],
        "requested_at": payload.get("requested_at"),
    }
    if payload["request_sha256"] != hashlib.sha256(
        canonical_json_bytes(expected_request)
    ).hexdigest():
        raise ExternalSlurmAcceptanceError(
            "infrastructure verification request binding is invalid"
        )
    operations = (
        (source, "staging-shared-source-bootstrap", "trt-gb10-2"),
        (accounting, "staging-slurm-accounting-converge", "trt-gb10-1"),
        *tuple(
            (item, "staging-allocation-bootstrap", host)
            for item, host in zip(node_bootstraps, hosts, strict=True)
        ),
    )
    completed: list[datetime] = []
    for item, action, host in operations:
        inner = item.get("inner_receipt")
        valid_inner = (
            re.fullmatch(r"staging-shared-source-bootstrap/v1/[0-9a-f]{64}", str(inner))
            is not None
            if action == "staging-shared-source-bootstrap"
            else (
                re.fullmatch(r"staging-accounting/v1/[0-9a-f]{64}", str(inner))
                is not None
                if action == "staging-slurm-accounting-converge"
                else re.fullmatch(
                    (
                        r"staging-allocation-bootstrap/v1/"
                        r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                        r"[0-9a-f]{4}-[0-9a-f]{12}/[0-9a-f]{64}"
                    ),
                    str(inner),
                )
                is not None
            )
        )
        if (
            item.get("action") != action
            or item.get("node") != host
            or item.get("status") != "succeeded"
            or _GENERATION_ID_RE.fullmatch(str(item.get("request_id"))) is None
            or _GENERATION_ID_RE.fullmatch(str(item.get("payload_sha256"))) is None
            or _GENERATION_ID_RE.fullmatch(str(item.get("result_sha256"))) is None
            or not valid_inner
        ):
            raise ExternalSlurmAcceptanceError(
                "infrastructure verification operation binding is invalid"
            )
        try:
            completed.append(_parse_timestamp(item.get("completed_at"), "completed_at"))
        except ExternalSlurmAcceptanceError as exc:
            raise ExternalSlurmAcceptanceError(
                "infrastructure verification operation timestamp is invalid"
            ) from exc
    source_inner = str(source["inner_receipt"]).rsplit("/", 1)[-1]
    if payload["source_digest"] != source_inner:
        raise ExternalSlurmAcceptanceError(
            "infrastructure verification source digest is invalid"
        )
    for item, host in zip(node_bootstraps, hosts, strict=True):
        _prefix, boot_id, mount_digest = str(item["inner_receipt"]).rsplit("/", 2)
        if boot_ids[host] != boot_id or mount_digests[host] != mount_digest:
            raise ExternalSlurmAcceptanceError(
                "infrastructure verification node evidence is invalid"
            )
    if now is not None and (now.tzinfo is None or now.utcoffset() is None):
        raise ExternalSlurmAcceptanceError(
            "infrastructure verification trusted clock is invalid"
        )
    requested = _parse_timestamp(payload.get("requested_at"), "requested_at")
    created = _parse_timestamp(payload.get("created_at"), "created_at")
    expires = _parse_timestamp(payload.get("expires_at"), "expires_at")
    observed = None if now is None else now.astimezone(UTC)
    if (
        requested > completed[0]
        or completed != sorted(completed)
        or completed[-1] > created
        or not created < expires
        or expires - created > timedelta(seconds=3600)
        or (
            observed is not None
            and (
                created > observed + timedelta(seconds=30)
                or expires <= observed
                or observed - created > timedelta(seconds=3600)
            )
        )
    ):
        raise ExternalSlurmAcceptanceError(
            "infrastructure verification summary lifetime is invalid"
        )
    return dict(payload)


def authority_paths(
    config: ExternalSlurmAuthorityConfig,
    candidate_sha: str,
    generation_id: str,
) -> tuple[Path, Path]:
    if _OBJECT_ID_RE.fullmatch(candidate_sha) is None:
        raise ExternalSlurmAcceptanceError("candidate_sha must be a full lowercase git SHA")
    if _GENERATION_ID_RE.fullmatch(generation_id) is None:
        raise ExternalSlurmAcceptanceError("generation_id must be a 64-character digest")
    root = config.artifact_root / "authorities" / candidate_sha / "generations" / generation_id
    return root / "acceptance.json", root / "acceptance.sig"


def _parse_timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ExternalSlurmAcceptanceError(f"{field} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ExternalSlurmAcceptanceError(f"{field} must be a UTC timestamp") from exc
    return parsed.astimezone(UTC)


_PAYLOAD_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "generation",
        "generation_id",
        "environment",
        "pool",
        "candidate_sha",
        "candidate_tree",
        "profile_sha256",
        "source_host",
        "created_at",
        "expires_at",
        "service_identity",
        "cluster",
        "controller",
        "submit_host",
        "partition",
        "slurm_account",
        "qos",
        "allowed_nodes",
        "repository",
        "worker_env",
        "supervisor",
        "nodes",
        "result",
    }
)
_NODE_FIELDS = frozenset(
    {
        "node",
        "job_id",
        "job_name",
        "account",
        "qos",
        "user",
        "uid",
        "gid",
        "sbatch_verified",
        "srun_verified",
        "candidate_sha",
        "candidate_tree",
        "repository",
        "repository_device",
        "repository_inode",
        "worker_env",
        "worker_env_device",
        "worker_env_inode",
        "worker_env_sha256",
        "compose_project",
        "compose_config_sha256",
        "docker_server_version",
        "worker_id",
        "registered_at",
        "first_heartbeat_at",
        "last_heartbeat_at",
        "heartbeat_count",
        "cancel_requested_at",
        "stopped_at",
        "job_terminal_at",
        "job_state",
        "orphan_containers",
        "orphan_networks",
        "orphan_volumes",
        "cleanup_verified",
    }
)


def validate_authority_payload(
    payload: Mapping[str, Any],
    *,
    config: ExternalSlurmAuthorityConfig,
    candidate_sha: str,
    candidate_tree: str | None = None,
    profile_sha256: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    if set(payload) != _PAYLOAD_FIELDS:
        raise ExternalSlurmAcceptanceError("authority payload has an invalid field set")
    if payload.get("schema_version") != 1:
        raise ExternalSlurmAcceptanceError("authority payload schema_version must be 1")
    if payload.get("kind") != "staging_external_slurm_acceptance":
        raise ExternalSlurmAcceptanceError("authority payload kind is invalid")
    if (
        isinstance(payload.get("generation"), bool)
        or not isinstance(payload.get("generation"), int)
        or payload["generation"] < 1
        or not isinstance(payload.get("generation_id"), str)
        or _GENERATION_ID_RE.fullmatch(payload["generation_id"]) is None
    ):
        raise ExternalSlurmAcceptanceError("authority generation binding is invalid")
    exact_scalars = {
        "environment": config.environment,
        "pool": config.pool,
        "candidate_sha": candidate_sha,
        "source_host": config.source_host,
        "cluster": config.cluster,
        "controller": config.controller,
        "submit_host": config.submit_host,
        "partition": config.partition,
        "slurm_account": config.slurm_account,
        "qos": config.qos,
    }
    if candidate_tree is not None:
        exact_scalars["candidate_tree"] = candidate_tree
    if profile_sha256 is not None:
        exact_scalars["profile_sha256"] = profile_sha256
    for field, expected in exact_scalars.items():
        if payload.get(field) != expected:
            raise ExternalSlurmAcceptanceError(f"authority payload {field} mismatch")
    for field in ("candidate_tree", "profile_sha256"):
        value = payload.get(field)
        required_length = 40 if field == "candidate_tree" else 64
        if (
            not isinstance(value, str)
            or len(value) != required_length
            or re.fullmatch(r"[0-9a-f]+", value) is None
        ):
            raise ExternalSlurmAcceptanceError(f"authority payload {field} is invalid")
    expected_repository = config.repository_template.format(
        image_tag=f"staging-{candidate_sha[:7]}"
    )
    expected_worker_env = config.worker_env_template.format(
        image_tag=f"staging-{candidate_sha[:7]}"
    )
    if payload.get("repository") != expected_repository:
        raise ExternalSlurmAcceptanceError("authority repository mismatch")
    if payload.get("worker_env") != expected_worker_env:
        raise ExternalSlurmAcceptanceError("authority worker_env mismatch")
    identity = payload.get("service_identity")
    expected_identity = {
        "username": config.batch_user,
        "group": config.batch_group,
        "uid": config.batch_uid,
        "gid": config.batch_gid,
        "home": str(config.batch_home),
        "shell": str(config.batch_shell),
        "supplementary_groups": list(config.batch_supplementary_groups),
    }
    if identity != expected_identity:
        raise ExternalSlurmAcceptanceError("authority service_identity mismatch")
    supervisor = payload.get("supervisor")
    if supervisor != {
        "service": config.supervisor_service,
        "timer": config.supervisor_timer,
        "enabled": False,
        "active": False,
    }:
        raise ExternalSlurmAcceptanceError(
            "authority prepare state did not keep the supervisor stopped"
        )
    if payload.get("allowed_nodes") != list(config.allowed_nodes):
        raise ExternalSlurmAcceptanceError("authority allowed_nodes mismatch")
    created_at = _parse_timestamp(payload.get("created_at"), "created_at")
    expires_at = _parse_timestamp(payload.get("expires_at"), "expires_at")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if created_at > current or expires_at <= current:
        raise ExternalSlurmAcceptanceError("authority receipt is not currently valid")
    if (expires_at - created_at).total_seconds() > config.max_age_seconds:
        raise ExternalSlurmAcceptanceError("authority receipt lifetime exceeds policy")
    rows = payload.get("nodes")
    if not isinstance(rows, list) or len(rows) != len(config.allowed_nodes):
        raise ExternalSlurmAcceptanceError("authority node matrix must cover 15 nodes")
    expected_nodes = list(config.allowed_nodes)
    actual_nodes: list[str] = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or set(row) != _NODE_FIELDS:
            raise ExternalSlurmAcceptanceError(f"authority nodes[{index}] has an invalid field set")
        node = row.get("node")
        if not isinstance(node, str):
            raise ExternalSlurmAcceptanceError(f"authority nodes[{index}].node is invalid")
        actual_nodes.append(node)
        exact_row = {
            "account": config.slurm_account,
            "qos": config.qos,
            "user": config.batch_user,
            "uid": config.batch_uid,
            "gid": config.batch_gid,
            "candidate_sha": candidate_sha,
            "candidate_tree": payload["candidate_tree"],
            "repository": expected_repository,
            "worker_env": expected_worker_env,
            "sbatch_verified": True,
            "srun_verified": True,
            "job_state": "COMPLETED",
            "orphan_containers": 0,
            "orphan_networks": 0,
            "orphan_volumes": 0,
            "cleanup_verified": True,
        }
        for field, expected in exact_row.items():
            if row.get(field) != expected:
                raise ExternalSlurmAcceptanceError(f"authority nodes[{index}].{field} mismatch")
        for field in (
            "job_id",
            "job_name",
            "compose_project",
            "docker_server_version",
            "worker_id",
        ):
            if not isinstance(row.get(field), str) or not row[field]:
                raise ExternalSlurmAcceptanceError(f"authority nodes[{index}].{field} is invalid")
        for field in (
            "repository_device",
            "repository_inode",
            "worker_env_device",
            "worker_env_inode",
        ):
            if (
                isinstance(row.get(field), bool)
                or not isinstance(row.get(field), int)
                or row[field] <= 0
            ):
                raise ExternalSlurmAcceptanceError(f"authority nodes[{index}].{field} is invalid")
        for field in ("worker_env_sha256", "compose_config_sha256"):
            if (
                not isinstance(row.get(field), str)
                or re.fullmatch(r"[0-9a-f]{64}", row[field]) is None
            ):
                raise ExternalSlurmAcceptanceError(f"authority nodes[{index}].{field} is invalid")
        if (
            isinstance(row.get("heartbeat_count"), bool)
            or not isinstance(row.get("heartbeat_count"), int)
            or row["heartbeat_count"] < 2
        ):
            raise ExternalSlurmAcceptanceError(
                f"authority nodes[{index}].heartbeat_count is invalid"
            )
        timestamps = [
            _parse_timestamp(row.get(field), f"nodes[{index}].{field}")
            for field in (
                "registered_at",
                "first_heartbeat_at",
                "last_heartbeat_at",
                "cancel_requested_at",
                "stopped_at",
                "job_terminal_at",
            )
        ]
        if timestamps != sorted(timestamps) or timestamps[-1] > created_at:
            raise ExternalSlurmAcceptanceError(
                f"authority nodes[{index}] lifecycle is not closed and ordered"
            )
    if actual_nodes != expected_nodes:
        raise ExternalSlurmAcceptanceError(
            "authority node matrix must match the ordered 15-node set"
        )
    if payload.get("result") != "pass":
        raise ExternalSlurmAcceptanceError("authority result is not pass")
    return dict(payload)


def _load_ed25519_public_key(payload: bytes) -> tuple[Ed25519PublicKey, str]:
    try:
        key = serialization.load_pem_public_key(payload)
    except ValueError as exc:
        raise ExternalSlurmAcceptanceError("authority public key is invalid") from exc
    if not isinstance(key, Ed25519PublicKey):
        raise ExternalSlurmAcceptanceError("authority public key must be Ed25519")
    return key, hashlib.sha256(payload).hexdigest()


def _current_pointer(
    config: ExternalSlurmAuthorityConfig,
    *,
    candidate_sha: str,
) -> dict[str, Any]:
    path = config.artifact_root / "current.json"
    raw = _bounded_read(
        path,
        maximum=_MAX_CONFIG_BYTES,
        label="authority current pointer",
    )
    try:
        pointer = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalSlurmAcceptanceError("authority current pointer is invalid JSON") from exc
    expected_fields = {
        "schema_version",
        "candidate_sha",
        "candidate_tree",
        "generation",
        "generation_id",
        "artifact_sha256",
        "signature_sha256",
        "key_id",
        "created_at",
        "expires_at",
    }
    if (
        not isinstance(pointer, dict)
        or set(pointer) != expected_fields
        or canonical_json_bytes(pointer) != raw
        or pointer.get("schema_version") != 1
        or pointer.get("candidate_sha") != candidate_sha
        or _OBJECT_ID_RE.fullmatch(str(pointer.get("candidate_tree") or "")) is None
        or isinstance(pointer.get("generation"), bool)
        or not isinstance(pointer.get("generation"), int)
        or pointer["generation"] < 1
        or _GENERATION_ID_RE.fullmatch(str(pointer.get("generation_id") or "")) is None
        or any(
            re.fullmatch(r"[0-9a-f]{64}", str(pointer.get(field) or "")) is None
            for field in ("artifact_sha256", "signature_sha256", "key_id")
        )
    ):
        raise ExternalSlurmAcceptanceError("authority current pointer binding is invalid")
    created_at = _parse_timestamp(pointer.get("created_at"), "current.created_at")
    expires_at = _parse_timestamp(pointer.get("expires_at"), "current.expires_at")
    if expires_at <= created_at:
        raise ExternalSlurmAcceptanceError("authority current pointer lifetime is invalid")
    return pointer


def _require_root_owned_file(path: Path, *, label: str, allowed_modes: set[int]) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ExternalSlurmAcceptanceError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) not in allowed_modes
    ):
        raise ExternalSlurmAcceptanceError(f"{label} must be a root-owned single-link regular file")


def _require_root_owned_parents(path: Path, *, label: str) -> None:
    for parent in path.parents:
        if parent == Path("/"):
            break
        try:
            metadata = parent.lstat()
        except OSError as exc:
            raise ExternalSlurmAcceptanceError(f"{label} parent is unavailable") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or metadata.st_mode & 0o022
        ):
            raise ExternalSlurmAcceptanceError(
                f"{label} parents must be root-owned and non-writable"
            )


def verify_authority(
    *,
    config: ExternalSlurmAuthorityConfig,
    candidate_sha: str,
    candidate_tree: str | None = None,
    profile_sha256: str | None = None,
    now: datetime | None = None,
    enforce_root_security: bool = True,
) -> VerifiedExternalSlurmAuthority:
    current_path = config.artifact_root / "current.json"
    if enforce_root_security:
        for path, label in (
            (current_path, "authority current pointer"),
            (config.public_key, "authority public key"),
        ):
            _require_root_owned_parents(path, label=label)
        _require_root_owned_file(
            current_path,
            label="authority current pointer",
            allowed_modes={0o600},
        )
    pointer = _current_pointer(config, candidate_sha=candidate_sha)
    artifact_path, signature_path = authority_paths(
        config,
        candidate_sha,
        str(pointer["generation_id"]),
    )
    if enforce_root_security:
        for path, label, allowed_modes in (
            (artifact_path, "authority artifact", {0o600}),
            (signature_path, "authority signature", {0o600}),
            (config.public_key, "authority public key", {0o600, 0o644}),
        ):
            _require_root_owned_parents(path, label=label)
            _require_root_owned_file(path, label=label, allowed_modes=allowed_modes)
    artifact = _bounded_read(
        artifact_path,
        maximum=_MAX_ARTIFACT_BYTES,
        label="authority artifact",
    )
    signature_encoded = _bounded_read(
        signature_path,
        maximum=_MAX_SIGNATURE_BYTES,
        label="authority signature",
    )
    public_key_bytes = _bounded_read(
        config.public_key,
        maximum=_MAX_CONFIG_BYTES,
        label="authority public key",
    )
    if not signature_encoded.endswith(b"\n") or signature_encoded.count(b"\n") != 1:
        raise ExternalSlurmAcceptanceError("authority signature must be canonical base64")
    try:
        signature = base64.b64decode(signature_encoded[:-1], validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ExternalSlurmAcceptanceError("authority signature is invalid base64") from exc
    public_key, key_id = _load_ed25519_public_key(public_key_bytes)
    if key_id != pointer["key_id"]:
        raise ExternalSlurmAcceptanceError("authority current pointer key mismatch")
    try:
        public_key.verify(signature, artifact)
    except InvalidSignature as exc:
        raise ExternalSlurmAcceptanceError(
            "authority artifact signature verification failed"
        ) from exc
    try:
        payload = json.loads(artifact)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExternalSlurmAcceptanceError("authority artifact is invalid JSON") from exc
    if not isinstance(payload, dict) or canonical_json_bytes(payload) != artifact:
        raise ExternalSlurmAcceptanceError("authority artifact must be canonical JSON")
    validated = validate_authority_payload(
        payload,
        config=config,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        profile_sha256=profile_sha256,
        now=now,
    )
    artifact_sha256 = hashlib.sha256(artifact).hexdigest()
    signature_sha256 = hashlib.sha256(signature).hexdigest()
    if (
        validated["candidate_tree"] != pointer["candidate_tree"]
        or validated["generation"] != pointer["generation"]
        or validated["generation_id"] != pointer["generation_id"]
        or validated["created_at"] != pointer["created_at"]
        or validated["expires_at"] != pointer["expires_at"]
        or artifact_sha256 != pointer["artifact_sha256"]
        or signature_sha256 != pointer["signature_sha256"]
    ):
        raise ExternalSlurmAcceptanceError(
            "authority current pointer does not match its immutable generation"
        )
    return VerifiedExternalSlurmAuthority(
        payload=validated,
        artifact_path=str(artifact_path),
        artifact_sha256=artifact_sha256,
        signature_sha256=signature_sha256,
        key_id=key_id,
    )


def activation_requested(profile: object) -> bool:
    if getattr(profile, "environment", None) != "staging":
        return False
    policies = getattr(profile, "autoscaler_policies", ())
    supervisors = getattr(profile, "external_slurm_autoscaler_supervisors", ())
    prerequisites = getattr(profile, "external_slurm_runner_prerequisites", {})
    gb10_enabled = any(
        isinstance(policy, dict)
        and policy.get("pool_name") == "gb10"
        and policy.get("actuator") == "slurm"
        and policy.get("enabled") is True
        and isinstance(policy.get("actuator_config"), dict)
        and policy["actuator_config"].get("external_runner") is True
        for policy in policies
    )
    supervisor_active = any(
        isinstance(row, dict)
        and row.get("pool_name") == "gb10"
        and (row.get("enabled") is True or row.get("active") is True)
        for row in supervisors
    )
    return (
        gb10_enabled
        or supervisor_active
        or (
            isinstance(prerequisites, dict)
            and prerequisites.get("materialize") is True
            and prerequisites.get("require_external_allocation_authority") is True
        )
    )


def candidate_sha_from_profile(profile: object) -> str:
    for policy in getattr(profile, "autoscaler_policies", ()):
        if not isinstance(policy, dict) or policy.get("pool_name") != "gb10":
            continue
        actuator_config = policy.get("actuator_config")
        if isinstance(actuator_config, dict):
            candidate_sha = actuator_config.get("candidate_sha")
            if isinstance(candidate_sha, str) and _OBJECT_ID_RE.fullmatch(candidate_sha):
                return candidate_sha
    raise ExternalSlurmAcceptanceError(
        "staging GB10 activation requires actuator_config.candidate_sha"
    )


def require_node_agent_authority_retired(profile: object) -> None:
    if not activation_requested(profile):
        return
    desired_states = getattr(profile, "gb10_desired_states", ())
    expected_intents = {f"trt-gb10-{index}": "stopped" for index in range(1, 16)}
    if (
        not isinstance(desired_states, list)
        or len(desired_states) != 1
        or not isinstance(desired_states[0], dict)
        or desired_states[0].get("environment") != "staging"
        or desired_states[0].get("pool_name") != "gb10"
        or desired_states[0].get("target_slots") != 0
        or desired_states[0].get("host_intents") != expected_intents
    ):
        raise ExternalSlurmAcceptanceError(
            "staging GB10 external Slurm activation requires the node-agent "
            "desired state to stop all 15 hosts with target_slots=0"
        )


def run_fixed_activation_verifier(
    profile: object,
    *,
    runner: CommandRunner | None = None,
) -> dict[str, Any] | None:
    if not activation_requested(profile):
        return None
    require_node_agent_authority_retired(profile)
    candidate_sha = candidate_sha_from_profile(profile)
    argv = [
        "sudo",
        "-n",
        str(DEFAULT_PROGRAM),
        "activate",
        "--candidate-sha",
        candidate_sha,
    ]
    completed = (runner or _default_runner)(argv)
    if completed.returncode != 0:
        detail = (completed.stderr or "").strip()[:300]
        raise ExternalSlurmAcceptanceError(
            "external Slurm acceptance authority rejected activation"
            + (f": {detail}" if detail else "")
        )
    try:
        payload = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise ExternalSlurmAcceptanceError(
            "external Slurm acceptance verifier returned invalid JSON"
        ) from exc
    if not isinstance(payload, dict) or payload.get("result") != "pass":
        raise ExternalSlurmAcceptanceError("external Slurm acceptance verifier did not return pass")
    if payload.get("candidate_sha") != candidate_sha:
        raise ExternalSlurmAcceptanceError("external Slurm acceptance verifier candidate mismatch")
    return payload


def _default_runner(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
        env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
    )
