#!/usr/bin/env python3
"""Secret-safe dynamic cross-environment credential probe (A3).

The cohort and every endpoint, secret path, bucket, candidate, and service
identity come from the fixed root registry snapshot.  Callers can request only
``--execute`` and an optional evidence destination; they cannot select an
environment, path, endpoint, credential, bucket, or candidate.  Execute mode
probes every ordered foreign pair plus same-environment positive controls and
re-reads both registry and runtime authorities before emitting evidence.

Never prints or stores worker tokens, admin tokens, or MinIO credentials.
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
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from loom.admin_secret import AdminSecretConfigError, load_admin_secret_file
from loom.worker_token import (
    DEFAULT_WORKER_TOKEN_ENV_KEY,
    worker_token_fingerprint,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.ops import developer_environment_registry as environment_registry  # noqa: E402

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_HEX_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_FLEET_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
REGISTRY_SNAPSHOT = environment_registry.SYSTEM_SNAPSHOT
DEFAULT_RUNTIME_ATTESTATION_ROOT = Path(
    "/var/lib/loom-shared-capacity/runtime-attestations",
)
EXPECTED_FLEET_ATTESTATION_ROOT = Path(
    "/var/lib/loom-developer-sandbox-links/attestations",
)
SECRET_NEEDLES = (
    "Bearer ",
    "loom_w_",
    "loom_admin_",
    "sk-",
    "AKIA",
    "BEGIN PRIVATE KEY",
    "password=",
    "secret=",
)
_TOKEN_SHAPE = re.compile(r"loom_(?:w|admin|worker|team)_[A-Za-z0-9+/=_-]{8,}")

# Authenticated application responses (auth succeeded).
WORKER_CLAIM_SAME_STATUSES = frozenset({200, 204})
WORKER_CLAIM_FOREIGN_STATUSES = frozenset({401})
ADMIN_MINT_SAME_STATUSES = frozenset({201})
ADMIN_MINT_FOREIGN_STATUSES = frozenset({403})
MINIO_FOREIGN_CREDENTIAL_ERROR_CODES = frozenset(
    {
        "AccessDenied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
    },
)
MINIO_FOREIGN_BUCKET_ERROR_CODES = frozenset({"AccessDenied", "NoSuchBucket", "404"})


@dataclass(frozen=True)
class CandidateIdentity:
    sandbox: str
    env_id: str
    candidate_id: str
    candidate_sha: str
    candidate_tree: str
    compose_project: str
    source_repo: str
    state_path: str
    state_payload_sha256: str
    updated_at: str | None


@dataclass(frozen=True)
class RuntimeActivationIdentity:
    sandbox: str
    candidate_sha: str
    candidate_tree: str
    receipt_path: str
    payload_sha256: str
    fleet_payload_sha256: str
    collected_at: str
    expires_at: str
    domain_generations: dict[str, int]


@dataclass(frozen=True)
class SandboxTarget:
    sandbox: str
    env_id: str
    owner_uid: int
    control_plane_url: str
    worker_token_file: Path
    admin_secret_file: Path
    minio_endpoint: str | None
    minio_access_key_file: Path | None
    minio_secret_key_file: Path | None
    own_bucket: str | None
    candidate: CandidateIdentity | None = None
    runtime_activation: RuntimeActivationIdentity | None = None


@dataclass(frozen=True)
class ProbeResult:
    source: str
    target: str
    surface: str
    status: int | str
    passed: bool
    detail: str
    source_worker_fingerprint: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "target": self.target,
            "surface": self.surface,
            "status": self.status,
            "pass": self.passed,
            "detail": self.detail,
            "source_worker_fingerprint": self.source_worker_fingerprint,
        }


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode()


def _registry_projection(value: object) -> dict[str, Any]:
    """Verify a full registry snapshot and project its active committed cohort."""

    if not isinstance(value, Mapping):
        raise ValueError("developer environment registry snapshot is invalid")
    try:
        verified = environment_registry.DeveloperEnvironmentRegistry.verify_snapshot(
            _canonical(dict(value)),
        )
    except environment_registry.RegistryError as exc:
        raise ValueError("developer environment registry snapshot is invalid") from exc
    candidates = {
        str(row["candidate_id"]): row for row in verified["candidates"] if isinstance(row, Mapping)
    }
    deployments = [row for row in verified["deployments"] if isinstance(row, Mapping)]
    finalizations = [
        row for row in verified.get("deployment_finalizations", []) if isinstance(row, Mapping)
    ]
    projected: list[dict[str, Any]] = []
    for raw_environment in verified["environments"]:
        if not isinstance(raw_environment, Mapping) or raw_environment.get("state") != "active":
            continue
        environment = dict(raw_environment)
        candidate_id = environment.get("current_candidate_id")
        candidate = candidates.get(str(candidate_id))
        committed = [
            row
            for row in deployments
            if row.get("phase") == "committed"
            and row.get("env_id") == environment.get("env_id")
            and row.get("principal_id") == environment.get("principal_id")
            and row.get("candidate_id") == candidate_id
            and row.get("applied_resource_generation") == environment.get("resource_generation")
            and row.get("expected_resource_generation", 0) + 1
            == row.get("applied_resource_generation")
            and type(row.get("applied_registry_generation")) is int
            and 1 <= row["applied_registry_generation"] < verified["generation"]
            and environment_registry.DIGEST_RE.fullmatch(
                str(row.get("applied_registry_payload_sha256")),
            )
            is not None
        ]
        if not isinstance(candidate_id, str) or candidate is None or len(committed) != 1:
            raise ValueError("active developer environment is not committed")
        latest = committed[0]
        records = [
            row
            for row in finalizations
            if row.get("deployment_id") == latest.get("deployment_id")
            and row.get("env_id") == environment.get("env_id")
            and row.get("principal_id") == environment.get("principal_id")
            and row.get("candidate_id") == candidate_id
            and row.get("candidate_sha") == candidate.get("candidate_sha")
            and row.get("candidate_tree") == candidate.get("candidate_tree")
            and row.get("applied_resource_generation") == latest.get("applied_resource_generation")
            and row.get("applied_registry_generation") == latest.get("applied_registry_generation")
            and row.get("applied_registry_payload_sha256")
            == latest.get("applied_registry_payload_sha256")
        ]
        if len(records) != 1:
            raise ValueError("active developer environment lacks exact finalization evidence")
        finalization = dict(records[0])
        finalization_sha = finalization.pop("payload_sha256", None)
        if (
            latest.get("finalization_payload_sha256") != finalization_sha
            or _HEX_SHA256_RE.fullmatch(str(finalization_sha)) is None
            or finalization_sha != hashlib.sha256(_canonical(finalization)).hexdigest()
            or any(
                _HEX_SHA256_RE.fullmatch(str(records[0].get(field))) is None
                for field in (
                    "capacity_finalize_receipt_sha256",
                    "capacity_finalize_check_receipt_sha256",
                    "runtime_reconcile_receipt_sha256",
                    "runtime_prepare_check_receipt_sha256",
                    "acceptance_probe_receipt_sha256",
                )
            )
        ):
            raise ValueError("active developer environment finalization evidence drifted")
        ports = environment.get("ports")
        if not isinstance(ports, Mapping):
            raise ValueError("developer environment port registry is invalid")
        projected.append(
            {
                "env_id": environment["env_id"],
                "principal_id": environment["principal_id"],
                "runtime_id": environment["runtime_id"],
                "resource_generation": environment["resource_generation"],
                "service_user": environment["service_user"],
                "service_group": environment["service_group"],
                "uid": environment["uid"],
                "gid": environment["gid"],
                "ports": dict(ports),
                "compose_project": environment["compose_project"],
                "systemd_instance": environment["systemd_instance"],
                "candidate_root": environment["candidate_root"],
                "runtime_root": environment["runtime_root"],
                "state_root": environment["state_root"],
                "evidence_root": environment["evidence_root"],
                "database_name": environment["database_name"],
                "task_bucket": environment["task_bucket"],
                "trajectories_bucket": environment["trajectories_bucket"],
                "artifacts_bucket": environment["artifacts_bucket"],
                "provider_namespace": environment["provider_namespace"],
                "slurm_user": environment["slurm_user"],
                "slurm_account": environment["slurm_account"],
                "slurm_qos": environment["slurm_qos"],
                "cgroup_slice": environment["cgroup_slice"],
                "candidate_id": candidate_id,
                "candidate_sha": candidate["candidate_sha"],
                "candidate_tree": candidate["candidate_tree"],
                "deployment_id": latest["deployment_id"],
                "deployment_generation": latest["applied_resource_generation"],
            },
        )
    projected.sort(key=lambda row: (str(row["env_id"]), str(row["runtime_id"])))
    if len(projected) < 2:
        raise ValueError("registry has fewer than two active committed environments")
    unsigned = {
        "schema_version": 1,
        "kind": "loom.developer-environment.crossover-registry-projection",
        "generation": verified["generation"],
        "source_registry_payload_sha256": verified["payload_sha256"],
        "source_registry": verified,
        "environments": projected,
    }
    return {
        **unsigned,
        "payload_sha256": hashlib.sha256(_canonical(unsigned)).hexdigest(),
    }


def _read_registry_projection(path: Path = REGISTRY_SNAPSHOT) -> dict[str, Any]:
    try:
        metadata = path.lstat()
        before = (metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mtime_ns)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_nlink != 1
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size > 16 * 1024 * 1024
        ):
            raise ValueError("developer environment registry snapshot is unsafe")
        raw = path.read_bytes()
        after_metadata = path.lstat()
    except OSError as exc:
        raise ValueError("developer environment registry snapshot is unavailable") from exc
    after = (
        after_metadata.st_dev,
        after_metadata.st_ino,
        after_metadata.st_size,
        after_metadata.st_mtime_ns,
    )
    if before != after or len(raw) != metadata.st_size:
        raise ValueError("developer environment registry snapshot changed during read")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("developer environment registry snapshot is invalid") from exc
    if raw != _canonical(value):
        raise ValueError("developer environment registry snapshot is not canonical")
    return _registry_projection(value)


def secure_secret_file(
    path: Path,
    *,
    label: str,
    allowed_uids: frozenset[int] | None = None,
) -> Path:
    """Require a mode-0600 single-link file owned by an approved identity."""
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    if metadata.st_nlink != 1:
        raise ValueError(f"{label} must have exactly one hard link: {path}")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode != 0o600:
        raise ValueError(f"{label} must be mode 0600 (got {mode:o}): {path}")
    accepted_owners = allowed_uids or frozenset({os.geteuid()})
    if metadata.st_uid not in accepted_owners:
        raise ValueError(
            f"{label} has unsafe owner uid={metadata.st_uid}: {path}",
        )
    resolved = path.resolve(strict=True)
    resolved_metadata = resolved.stat()
    if resolved_metadata.st_dev != metadata.st_dev or resolved_metadata.st_ino != metadata.st_ino:
        raise ValueError(f"{label} changed during validation: {path}")
    return resolved


def _secure_read_text(
    path: Path,
    *,
    label: str,
    allowed_uids: frozenset[int] | None = None,
) -> tuple[Path, str]:
    secure = secure_secret_file(path, label=label, allowed_uids=allowed_uids)
    before = secure.stat()
    try:
        value = secure.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise ValueError(f"{label} is unreadable: {path}") from exc
    after = secure.stat()
    if before.st_dev != after.st_dev or before.st_ino != after.st_ino or after.st_nlink != 1:
        raise ValueError(f"{label} changed during read: {path}")
    return secure, value


def _read_environment_secret(path: Path, *, key: str, owner_uid: int) -> str:
    _, raw = _secure_read_text(
        path,
        label="environment secret file",
        allowed_uids=frozenset({0, owner_uid}),
    )
    values: dict[str, str] = {}
    for line in raw.splitlines():
        name, separator, value = line.partition("=")
        if not separator or not name or not value or name in values:
            raise ValueError("environment secret file is invalid")
        values[name] = value
    secret = values.get(key)
    if not secret:
        raise ValueError("environment secret file is incomplete")
    return secret


def _load_worker_token(path: Path, *, owner_uid: int | None = None) -> str:
    _, raw = _secure_read_text(
        path,
        label="worker-token secret file",
        allowed_uids=(None if owner_uid is None else frozenset({0, owner_uid})),
    )
    token: str | None = None
    for raw_line in raw.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        name, separator, value = line.partition("=")
        if separator != "=" or name.strip() != DEFAULT_WORKER_TOKEN_ENV_KEY:
            continue
        if token is not None:
            raise ValueError(f"{path}: duplicate {DEFAULT_WORKER_TOKEN_ENV_KEY} entry")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        token = value
    if token is None:
        raw = raw.strip()
        if not raw:
            raise ValueError(f"{path}: empty worker token")
        if "=" in raw.splitlines()[0]:
            raise ValueError(
                f"{path}: missing {DEFAULT_WORKER_TOKEN_ENV_KEY}= entry",
            )
        token = raw.splitlines()[0].strip()
    if not token:
        raise ValueError(f"{path}: empty worker token")
    return token


def _load_admin_token(path: Path, *, owner_uid: int) -> str:
    secure, content = _secure_read_text(
        path,
        label="admin secret file",
        allowed_uids=frozenset({0, owner_uid}),
    )
    try:
        verifier = load_admin_secret_file(
            secure,
            require_safe_permissions=True,
        )
    except AdminSecretConfigError as exc:
        raise ValueError(str(exc)) from exc
    raw = tomllib.loads(content)
    admin = raw.get("admin")
    if not isinstance(admin, dict):
        raise ValueError(f"{path}: missing [admin]")
    token = admin.get("token")
    if not isinstance(token, str) or not token.strip():
        raise ValueError(f"{path}: [admin].token must be a non-empty string")
    if not verifier.verify(token.strip()):
        raise ValueError(f"{path}: admin token failed local verify")
    return token.strip()


def _normalize_http_url(url: str) -> str:
    parsed = urlparse(url.rstrip("/"))
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError(
            f"endpoint must be an http://host:port URL without path: {url}",
        ) from exc
    if (
        parsed.scheme != "http"
        or parsed.hostname is None
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"endpoint must be an http://host:port URL without path: {url}")
    return f"{parsed.scheme}://{parsed.hostname}:{port}"


def assert_endpoint_matches_reviewed(provided: str, reviewed: str, *, label: str) -> str:
    normalized = _normalize_http_url(provided)
    expected = _normalize_http_url(reviewed)
    if normalized != expected:
        raise ValueError(
            f"{label} must equal reviewed endpoint {expected!r} (got {normalized!r})",
        )
    return expected


def _parse_utc_timestamp(value: object, *, label: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{label} must be a UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be a UTC timestamp")
    return parsed.astimezone(UTC)


def _run_git_readback(repository: Path, *args: str) -> str:
    completed = subprocess.run(
        (
            "git",
            "-c",
            f"safe.directory={repository}",
            "-C",
            str(repository),
            *args,
        ),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise ValueError(
            f"candidate repository readback failed safely: git {args[0]}",
        )
    return completed.stdout.strip()


def _verify_candidate_repository(
    *,
    sandbox: str,
    candidate_root: Path,
    expected_sha: str,
    expected_tree: str,
) -> Path:
    expected_path = candidate_root / expected_sha
    try:
        expected_resolved = expected_path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(
            f"{sandbox}: exact candidate repository is unavailable",
        ) from exc
    source_metadata = expected_path.lstat()
    if stat.S_ISLNK(source_metadata.st_mode) or not stat.S_ISDIR(source_metadata.st_mode):
        raise ValueError(
            f"{sandbox}: candidate repository must be a non-symlink directory",
        )
    head = _run_git_readback(expected_resolved, "rev-parse", "--verify", "HEAD")
    resolved = _run_git_readback(
        expected_resolved,
        "rev-parse",
        "--verify",
        f"{expected_sha}^{{commit}}",
    )
    tree = _run_git_readback(
        expected_resolved,
        "rev-parse",
        "--verify",
        "HEAD^{tree}",
    )
    status = _run_git_readback(
        expected_resolved,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if head != expected_sha or resolved != expected_sha:
        raise ValueError(f"{sandbox}: candidate HEAD readback drifted")
    if tree != expected_tree:
        raise ValueError(f"{sandbox}: candidate tree readback drifted")
    if status:
        raise ValueError(f"{sandbox}: candidate repository is not clean")
    return expected_resolved


def load_candidate_identity(
    environment: Mapping[str, Any],
    *,
    verify_checkout: bool,
    registry_payload_sha256: str,
) -> CandidateIdentity:
    sandbox = str(environment["runtime_id"])
    candidate_sha = str(environment["candidate_sha"])
    candidate_tree = str(environment["candidate_tree"])
    if _SHA_RE.fullmatch(candidate_sha) is None or _SHA_RE.fullmatch(candidate_tree) is None:
        raise ValueError(f"{sandbox}: registry candidate binding is invalid")
    source_repo = Path(str(environment["candidate_root"])) / candidate_sha
    if verify_checkout:
        source_repo = _verify_candidate_repository(
            sandbox=sandbox,
            candidate_root=Path(str(environment["candidate_root"])),
            expected_sha=candidate_sha,
            expected_tree=candidate_tree,
        )
    return CandidateIdentity(
        sandbox=sandbox,
        env_id=str(environment["env_id"]),
        candidate_id=str(environment["candidate_id"]),
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        compose_project=str(environment["compose_project"]),
        source_repo=str(source_repo),
        state_path=str(REGISTRY_SNAPSHOT),
        state_payload_sha256=registry_payload_sha256,
        updated_at=None,
    )


def _canonical_json_without_digest(payload: dict[str, Any]) -> bytes:
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    return json.dumps(
        unsigned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")


def load_runtime_activation(
    runtime_root: Path,
    candidate: CandidateIdentity,
    *,
    now: datetime | None = None,
) -> RuntimeActivationIdentity:
    sandbox = candidate.sandbox
    path = runtime_root / sandbox / candidate.candidate_sha / "combined.json"
    secure, content = _secure_read_text(
        path,
        label=f"{sandbox} combined runtime activation",
    )
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{sandbox}: combined runtime activation is invalid") from exc
    expected_keys = {
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
    if not isinstance(payload, dict) or set(payload) != expected_keys:
        raise ValueError(f"{sandbox}: combined runtime activation schema is invalid")
    digest = payload.get("payload_sha256")
    expected_digest = hashlib.sha256(_canonical_json_without_digest(payload)).hexdigest()
    if (
        payload.get("schema_version") != 1
        or payload.get("kind") != "loom.developer-runtime-combined-activation"
        or payload.get("sandbox") != sandbox
        or payload.get("candidate_sha") != candidate.candidate_sha
        or payload.get("candidate_tree") != candidate.candidate_tree
        or not isinstance(digest, str)
        or _HEX_SHA256_RE.fullmatch(digest) is None
        or digest != expected_digest
    ):
        raise ValueError(f"{sandbox}: combined runtime activation binding is invalid")

    current = now or datetime.now(UTC)
    collector = payload.get("collector")
    if not isinstance(collector, dict) or set(collector) != {
        "hostname",
        "collected_at",
        "expires_at",
    }:
        raise ValueError(f"{sandbox}: runtime activation collector is invalid")
    collected_at = _parse_utc_timestamp(
        collector.get("collected_at"),
        label=f"{sandbox} activation collected_at",
    )
    expires_at = _parse_utc_timestamp(
        collector.get("expires_at"),
        label=f"{sandbox} activation expires_at",
    )
    if (
        collector.get("hostname") != "trt-eai-oldlab-2"
        or collected_at > current + timedelta(seconds=30)
        or expires_at <= current
        or expires_at <= collected_at
        or expires_at - collected_at > timedelta(minutes=15)
    ):
        raise ValueError(f"{sandbox}: runtime activation is stale or untrusted")

    fleet = payload.get("fleet_attestation")
    expected_fleet_path = (
        EXPECTED_FLEET_ATTESTATION_ROOT / sandbox / candidate.candidate_sha / "fleet.json"
    )
    if not isinstance(fleet, dict) or set(fleet) != {
        "path",
        "payload_sha256",
        "generated_at",
        "expires_at",
    }:
        raise ValueError(f"{sandbox}: runtime fleet binding is invalid")
    fleet_generated = _parse_utc_timestamp(
        fleet.get("generated_at"),
        label=f"{sandbox} fleet generated_at",
    )
    fleet_expires = _parse_utc_timestamp(
        fleet.get("expires_at"),
        label=f"{sandbox} fleet expires_at",
    )
    if (
        fleet.get("path") != str(expected_fleet_path)
        or _FLEET_SHA256_RE.fullmatch(str(fleet.get("payload_sha256"))) is None
        or fleet_generated > current + timedelta(seconds=30)
        or fleet_expires <= current
        or expires_at > fleet_expires
    ):
        raise ValueError(f"{sandbox}: runtime fleet binding is stale or invalid")

    domains = payload.get("domains")
    if not isinstance(domains, dict) or set(domains) != {"oldlab", "gb10"}:
        raise ValueError(f"{sandbox}: runtime domain binding is incomplete")
    generations: dict[str, int] = {}
    expected_domain_keys = {
        "manifest_path",
        "signature_path",
        "payload_sha256",
        "signature_sha256",
        "key_id",
        "generation",
        "published_at",
        "expires_at",
    }
    for name in ("oldlab", "gb10"):
        row = domains.get(name)
        if not isinstance(row, dict) or set(row) != expected_domain_keys:
            raise ValueError(f"{sandbox}: {name} runtime domain binding is invalid")
        generation = row.get("generation")
        published = _parse_utc_timestamp(
            row.get("published_at"),
            label=f"{sandbox} {name} published_at",
        )
        domain_expires = _parse_utc_timestamp(
            row.get("expires_at"),
            label=f"{sandbox} {name} expires_at",
        )
        if (
            type(generation) is not int
            or generation <= 0
            or _HEX_SHA256_RE.fullmatch(str(row.get("payload_sha256"))) is None
            or _HEX_SHA256_RE.fullmatch(str(row.get("signature_sha256"))) is None
            or published > current + timedelta(seconds=30)
            or domain_expires < expires_at
        ):
            raise ValueError(f"{sandbox}: {name} runtime domain binding is invalid")
        generations[name] = generation

    _, final_content = _secure_read_text(
        path,
        label=f"{sandbox} combined runtime activation",
    )
    if final_content != content:
        raise ValueError(f"{sandbox}: runtime activation changed during readback")
    return RuntimeActivationIdentity(
        sandbox=sandbox,
        candidate_sha=candidate.candidate_sha,
        candidate_tree=candidate.candidate_tree,
        receipt_path=str(secure),
        payload_sha256=digest,
        fleet_payload_sha256=str(fleet["payload_sha256"]),
        collected_at=str(collector["collected_at"]),
        expires_at=str(collector["expires_at"]),
        domain_generations=generations,
    )


def _http_status(
    method: str,
    url: str,
    *,
    token: str | None = None,
    body: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> tuple[int | str, str]:
    headers = {"Accept": "application/json"}
    data = None
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=timeout) as response:
            return int(response.status), "ok"
    except HTTPError as exc:
        return int(exc.code), exc.reason or "http-error"
    except URLError as exc:
        return "connection-error", str(exc.reason)


def assert_evidence_secret_free(payload: dict[str, Any]) -> list[str]:
    blob = json.dumps(payload, sort_keys=True)
    errors = [
        f"evidence must not contain {needle!r}"
        for needle in SECRET_NEEDLES
        if needle.lower() in blob.lower()
    ]
    if _TOKEN_SHAPE.search(blob):
        errors.append("evidence must not contain token-shaped substrings")
    return errors


def _write_evidence(path: Path, payload: dict[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        metadata = path.lstat()
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != os.geteuid()
        ):
            raise ValueError(f"evidence target is unsafe: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    encoded = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    try:
        descriptor = os.open(temporary, flags, 0o600)
        try:
            remaining = memoryview(encoded)
            while remaining:
                written = os.write(descriptor, remaining)
                if written <= 0:
                    raise ValueError(f"evidence write failed safely: {path}")
                remaining = remaining[written:]
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        os.replace(temporary, path)
        os.chmod(path, 0o600)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _require_minio_config(target: SandboxTarget, *, role: str) -> None:
    missing: list[str] = []
    if target.minio_endpoint is None:
        missing.append("minio_endpoint")
    if target.minio_access_key_file is None:
        missing.append("minio_access_key_file")
    if target.minio_secret_key_file is None:
        missing.append("minio_secret_key_file")
    if target.own_bucket is None:
        missing.append("own_bucket")
    if missing:
        raise ValueError(
            f"execute mode requires complete MinIO config for {role} "
            f"{target.sandbox}: missing {', '.join(missing)}",
        )


def probe_worker_claim_crossover(
    source: SandboxTarget,
    target: SandboxTarget,
    *,
    execute: bool,
) -> ProbeResult:
    surface = "worker_claim"
    if not execute:
        return ProbeResult(
            source=source.sandbox,
            target=target.sandbox,
            surface=surface,
            status="dry-run",
            passed=True,
            detail="would POST /trials/claim with source worker token on target CP",
            source_worker_fingerprint=None,
        )
    token = _load_worker_token(source.worker_token_file, owner_uid=source.owner_uid)
    fingerprint = worker_token_fingerprint(token)
    status, detail = _http_status(
        "POST",
        f"{target.control_plane_url.rstrip('/')}/trials/claim",
        token=token,
        body={
            "worker_id": "00000000-0000-0000-0000-000000000000",
            "caps": [
                {
                    "os": "linux",
                    "gpu_vendor": "none",
                    "network_policies": ["public"],
                    "dynamic_network_policy": True,
                    "mounted_fs": True,
                    "resource_modes": ["auto"],
                }
            ],
        },
    )
    if source.sandbox == target.sandbox:
        passed = status in WORKER_CLAIM_SAME_STATUSES
        expect = f"own-token claim status in {sorted(WORKER_CLAIM_SAME_STATUSES)}"
    else:
        passed = status in WORKER_CLAIM_FOREIGN_STATUSES
        expect = f"foreign worker token status in {sorted(WORKER_CLAIM_FOREIGN_STATUSES)}"
    return ProbeResult(
        source=source.sandbox,
        target=target.sandbox,
        surface=surface,
        status=status,
        passed=passed,
        detail=f"{expect}; got {status} ({detail})",
        source_worker_fingerprint=fingerprint,
    )


def probe_admin_mint_crossover(
    source: SandboxTarget,
    target: SandboxTarget,
    *,
    execute: bool,
) -> ProbeResult:
    surface = "admin_worker_token_mint"
    if not execute:
        return ProbeResult(
            source=source.sandbox,
            target=target.sandbox,
            surface=surface,
            status="dry-run",
            passed=True,
            detail="would POST /admin/worker-tokens with source admin on target CP",
        )
    token = _load_admin_token(
        source.admin_secret_file,
        owner_uid=source.owner_uid,
    )
    status, detail = _http_status(
        "POST",
        f"{target.control_plane_url.rstrip('/')}/admin/worker-tokens",
        token=token,
        body={"expires_in_days": 1},
    )
    if source.sandbox == target.sandbox:
        passed = status in ADMIN_MINT_SAME_STATUSES
        expect = f"own admin mint status in {sorted(ADMIN_MINT_SAME_STATUSES)}"
    else:
        passed = status in ADMIN_MINT_FOREIGN_STATUSES
        expect = f"foreign admin status in {sorted(ADMIN_MINT_FOREIGN_STATUSES)}"
    return ProbeResult(
        source=source.sandbox,
        target=target.sandbox,
        surface=surface,
        status=status,
        passed=passed,
        detail=f"{expect}; got {status} ({detail})",
    )


def probe_minio_foreign_creds(
    source: SandboxTarget,
    target: SandboxTarget,
    *,
    execute: bool,
) -> ProbeResult:
    surface = "minio_foreign_credentials"
    incomplete = (
        target.minio_endpoint is None
        or source.minio_access_key_file is None
        or source.minio_secret_key_file is None
        or target.own_bucket is None
    )
    if incomplete:
        if execute:
            raise ValueError(
                f"execute mode missing MinIO foreign-creds inputs for "
                f"{source.sandbox}->{target.sandbox}",
            )
        return ProbeResult(
            source=source.sandbox,
            target=target.sandbox,
            surface=surface,
            status="skipped",
            passed=True,
            detail="minio endpoints/creds not configured (dry-run informational)",
        )
    if not execute:
        return ProbeResult(
            source=source.sandbox,
            target=target.sandbox,
            surface=surface,
            status="dry-run",
            passed=True,
            detail="would ListObjects with source MinIO creds against target endpoint",
        )
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as exc:  # pragma: no cover
        raise ValueError("boto3 required for --execute MinIO probes") from exc

    assert source.minio_access_key_file is not None
    assert source.minio_secret_key_file is not None
    assert target.minio_endpoint is not None
    assert target.own_bucket is not None
    access = _read_environment_secret(
        source.minio_access_key_file,
        key="LOOM_DEV_MINIO_ROOT_USER",
        owner_uid=source.owner_uid,
    )
    secret = _read_environment_secret(
        source.minio_secret_key_file,
        key="LOOM_DEV_MINIO_ROOT_PASSWORD",
        owner_uid=source.owner_uid,
    )
    client = boto3.client(
        "s3",
        endpoint_url=target.minio_endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    try:
        client.list_objects_v2(Bucket=target.own_bucket, MaxKeys=1)
        code: int | str = "Success"
        passed = False
        detail = "foreign MinIO creds unexpectedly succeeded"
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "ClientError"))
        passed = code in MINIO_FOREIGN_CREDENTIAL_ERROR_CODES
        detail = f"foreign creds rejected: {code}"
    except BotoCoreError:
        code = "ProbeError"
        passed = False
        detail = "foreign-credentials probe could not complete"
    return ProbeResult(
        source=source.sandbox,
        target=target.sandbox,
        surface=surface,
        status=code,
        passed=passed,
        detail=detail,
    )


def probe_minio_own_positive(
    target: SandboxTarget,
    *,
    execute: bool,
) -> ProbeResult:
    """Same-sandbox positive: own MinIO creds can list the own bucket."""
    surface = "minio_own_credentials"
    incomplete = (
        target.minio_endpoint is None
        or target.minio_access_key_file is None
        or target.minio_secret_key_file is None
        or target.own_bucket is None
    )
    if incomplete:
        if execute:
            raise ValueError(
                f"execute mode missing MinIO same-sandbox positive inputs for {target.sandbox}",
            )
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="skipped",
            passed=True,
            detail="minio own-bucket positive not configured (dry-run informational)",
        )
    if not execute:
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="dry-run",
            passed=True,
            detail="would ListObjects with own MinIO creds on own bucket",
        )
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as exc:  # pragma: no cover
        raise ValueError("boto3 required for --execute MinIO probes") from exc

    assert target.minio_access_key_file is not None
    assert target.minio_secret_key_file is not None
    assert target.minio_endpoint is not None
    assert target.own_bucket is not None
    access = _read_environment_secret(
        target.minio_access_key_file,
        key="LOOM_DEV_MINIO_ROOT_USER",
        owner_uid=target.owner_uid,
    )
    secret = _read_environment_secret(
        target.minio_secret_key_file,
        key="LOOM_DEV_MINIO_ROOT_PASSWORD",
        owner_uid=target.owner_uid,
    )
    client = boto3.client(
        "s3",
        endpoint_url=target.minio_endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    try:
        client.list_objects_v2(Bucket=target.own_bucket, MaxKeys=1)
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="Success",
            passed=True,
            detail="own MinIO creds listed own bucket",
        )
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "ClientError"))
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status=code,
            passed=False,
            detail=f"own MinIO positive failed: {code}",
        )
    except BotoCoreError:
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="ProbeError",
            passed=False,
            detail="own MinIO positive could not complete",
        )


def probe_minio_foreign_bucket(
    source: SandboxTarget,
    foreign: SandboxTarget,
    *,
    execute: bool,
) -> ProbeResult:
    surface = "minio_foreign_bucket"
    incomplete = (
        source.minio_endpoint is None
        or source.minio_access_key_file is None
        or source.minio_secret_key_file is None
        or foreign.own_bucket is None
    )
    if incomplete:
        if execute:
            raise ValueError(
                "execute mode missing MinIO foreign-bucket inputs for "
                f"{source.sandbox}->{foreign.sandbox}",
            )
        return ProbeResult(
            source=source.sandbox,
            target=foreign.sandbox,
            surface=surface,
            status="skipped",
            passed=True,
            detail="minio bucket probe not configured (dry-run informational)",
        )
    if not execute:
        return ProbeResult(
            source=source.sandbox,
            target=foreign.sandbox,
            surface=surface,
            status="dry-run",
            passed=True,
            detail="would ListObjects on foreign bucket name with own creds",
        )
    try:
        import boto3
        from botocore.exceptions import BotoCoreError, ClientError
    except ImportError as exc:  # pragma: no cover
        raise ValueError("boto3 required for --execute MinIO probes") from exc

    assert source.minio_access_key_file is not None
    assert source.minio_secret_key_file is not None
    assert source.minio_endpoint is not None
    assert foreign.own_bucket is not None
    access = _read_environment_secret(
        source.minio_access_key_file,
        key="LOOM_DEV_MINIO_ROOT_USER",
        owner_uid=source.owner_uid,
    )
    secret = _read_environment_secret(
        source.minio_secret_key_file,
        key="LOOM_DEV_MINIO_ROOT_PASSWORD",
        owner_uid=source.owner_uid,
    )
    client = boto3.client(
        "s3",
        endpoint_url=source.minio_endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    try:
        client.list_objects_v2(Bucket=foreign.own_bucket, MaxKeys=1)
        return ProbeResult(
            source=source.sandbox,
            target=foreign.sandbox,
            surface=surface,
            status="Success",
            passed=False,
            detail="foreign bucket unexpectedly readable",
        )
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "ClientError"))
        passed = code in MINIO_FOREIGN_BUCKET_ERROR_CODES
        return ProbeResult(
            source=source.sandbox,
            target=foreign.sandbox,
            surface=surface,
            status=code,
            passed=passed,
            detail=f"foreign bucket rejected: {code}",
        )
    except BotoCoreError:
        return ProbeResult(
            source=source.sandbox,
            target=foreign.sandbox,
            surface=surface,
            status="ProbeError",
            passed=False,
            detail="foreign-bucket probe could not complete",
        )


def directed_pairs(sandboxes: list[str]) -> list[tuple[str, str]]:
    return [(a, b) for a in sandboxes for b in sandboxes if a != b]


def run_probe_matrix(
    targets: dict[str, SandboxTarget],
    *,
    execute: bool,
    include_same_sandbox: bool = False,
) -> list[ProbeResult]:
    results: list[ProbeResult] = []
    names = list(targets)
    if execute:
        include_same_sandbox = True
        for target in targets.values():
            _require_minio_config(target, role="target")
    pairs = directed_pairs(names)
    if include_same_sandbox:
        pairs = [(n, n) for n in names] + pairs
    for source_name, target_name in pairs:
        source = targets[source_name]
        target = targets[target_name]
        results.append(probe_worker_claim_crossover(source, target, execute=execute))
        results.append(probe_admin_mint_crossover(source, target, execute=execute))
        if source_name != target_name:
            results.append(probe_minio_foreign_creds(source, target, execute=execute))
            results.append(
                probe_minio_foreign_bucket(source, target, execute=execute),
            )
    for name in names:
        target = targets[name]
        if include_same_sandbox or execute:
            results.append(probe_minio_own_positive(target, execute=execute))
    return results


def build_evidence(
    results: list[ProbeResult],
    *,
    execute: bool,
    registry_snapshot: Mapping[str, Any],
    candidates: list[CandidateIdentity],
    runtime_activations: list[RuntimeActivationIdentity],
) -> dict[str, Any]:
    payload = {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-crossover-probe",
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "mode": "execute" if execute else "dry-run",
        "registry_snapshot": dict(registry_snapshot),
        "candidates": [
            {
                "sandbox": row.sandbox,
                "env_id": row.env_id,
                "candidate_id": row.candidate_id,
                "candidate_sha": row.candidate_sha,
                "candidate_tree": row.candidate_tree,
                "compose_project": row.compose_project,
                "source_repo": row.source_repo,
                "state_path": row.state_path,
                "state_payload_sha256": row.state_payload_sha256,
                "updated_at": row.updated_at,
            }
            for row in candidates
        ],
        "runtime_activations": [
            {
                "sandbox": row.sandbox,
                "candidate_sha": row.candidate_sha,
                "candidate_tree": row.candidate_tree,
                "receipt_path": row.receipt_path,
                "payload_sha256": row.payload_sha256,
                "fleet_payload_sha256": row.fleet_payload_sha256,
                "collected_at": row.collected_at,
                "expires_at": row.expires_at,
                "domain_generations": row.domain_generations,
            }
            for row in runtime_activations
        ],
        "notes": [
            "CI-safe dry-run / dual-stack negatives are not live A3 host evidence",
            "A3 crossover evidence is not #896 soak evidence",
            "secret values are never recorded; worker tokens appear only as fingerprints",
            "live targets bind exact clean candidate readback plus the remote-link "
            "fleet and dual-domain combined activation receipt",
        ],
        "results": [row.as_dict() for row in results],
        "summary": {
            "total": len(results),
            "passed": sum(1 for row in results if row.passed),
            "failed": sum(1 for row in results if not row.passed),
        },
    }
    payload["payload_sha256"] = hashlib.sha256(_canonical(payload)).hexdigest()
    return payload


def build_targets(
    registry_snapshot: Mapping[str, Any],
    *,
    execute: bool,
) -> tuple[dict[str, SandboxTarget], list[CandidateIdentity]]:
    projection = _registry_projection(registry_snapshot)
    candidates: list[CandidateIdentity] = []
    runtime_activations: list[RuntimeActivationIdentity] = []
    targets: dict[str, SandboxTarget] = {}
    for environment in projection["environments"]:
        sandbox = str(environment["runtime_id"])
        owner_uid = int(environment["uid"])
        state_root = Path(str(environment["state_root"]))
        secrets_file = state_root / "secrets" / "environment.env"
        admin_file = state_root / "secrets" / "admin.toml"
        candidate = load_candidate_identity(
            environment,
            verify_checkout=execute,
            registry_payload_sha256=str(
                projection["source_registry_payload_sha256"],
            ),
        )
        candidates.append(candidate)
        runtime_activation = (
            load_runtime_activation(DEFAULT_RUNTIME_ATTESTATION_ROOT, candidate)
            if execute
            else None
        )
        if runtime_activation is not None:
            runtime_activations.append(runtime_activation)
        if execute:
            allowed = frozenset({0, owner_uid})
            worker_path = secure_secret_file(
                secrets_file,
                label=f"{sandbox} environment secret file",
                allowed_uids=allowed,
            )
            admin_path = secure_secret_file(
                admin_file,
                label=f"{sandbox} admin secret file",
                allowed_uids=allowed,
            )
        else:
            worker_path = secrets_file
            admin_path = admin_file
        ports = environment["ports"]
        targets[sandbox] = SandboxTarget(
            sandbox=sandbox,
            env_id=str(environment["env_id"]),
            owner_uid=owner_uid,
            control_plane_url=f"http://127.0.0.1:{int(ports['control_plane'])}",
            worker_token_file=worker_path,
            admin_secret_file=admin_path,
            minio_endpoint=f"http://127.0.0.1:{int(ports['minio'])}",
            minio_access_key_file=worker_path,
            minio_secret_key_file=worker_path,
            own_bucket=str(environment["artifacts_bucket"]),
            candidate=candidate,
            runtime_activation=runtime_activation,
        )
    if len(targets) < 2 or len(targets) != len(projection["environments"]):
        raise ValueError("dynamic crossover cohort is incomplete")
    return targets, candidates


def refresh_runtime_activations(
    targets: dict[str, SandboxTarget],
    *,
    runtime_root: Path,
) -> list[RuntimeActivationIdentity]:
    refreshed: list[RuntimeActivationIdentity] = []
    for sandbox in sorted(targets):
        target = targets[sandbox]
        if target.candidate is None or target.runtime_activation is None:
            raise ValueError(f"{sandbox}: live candidate binding is incomplete")
        current = load_runtime_activation(runtime_root, target.candidate)
        if current != target.runtime_activation:
            raise ValueError(f"{sandbox}: runtime activation changed during live probes")
        refreshed.append(current)
    return refreshed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="developer_sandbox_crossover_probe",
        description=__doc__,
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform live HTTP/MinIO probes (fail-closed; default: dry-run)",
    )
    parser.add_argument(
        "--write-evidence",
        type=Path,
        default=None,
        help="Write secret-safe evidence JSON to this path",
    )
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    # Reject accidental literal tokens on the CLI before argparse.
    for raw in sys.argv[1:]:
        if raw.startswith("loom_w_") or raw.startswith("loom_admin_"):
            print(
                "refusing literal token on CLI; pass --*-file paths only",
                file=sys.stderr,
            )
            return 2
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        registry_projection = _read_registry_projection()
        targets, candidates = build_targets(
            registry_projection["source_registry"],
            execute=args.execute,
        )
        results = run_probe_matrix(
            targets,
            execute=args.execute,
            include_same_sandbox=True,
        )
        runtime_activations = (
            refresh_runtime_activations(
                targets,
                runtime_root=DEFAULT_RUNTIME_ATTESTATION_ROOT,
            )
            if args.execute
            else []
        )
        if _read_registry_projection() != registry_projection:
            raise ValueError("developer environment registry changed during crossover probes")
        evidence = build_evidence(
            results,
            execute=args.execute,
            registry_snapshot=registry_projection,
            candidates=candidates,
            runtime_activations=runtime_activations,
        )
        secret_errors = assert_evidence_secret_free(evidence)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if secret_errors:
        for err in secret_errors:
            print(f"error: {err}", file=sys.stderr)
        return 1

    if args.write_evidence is not None:
        try:
            _write_evidence(args.write_evidence, evidence)
        except (OSError, ValueError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1

    failed = evidence["summary"]["failed"]
    if args.json:
        print(json.dumps(evidence, indent=2, sort_keys=True))
    else:
        mode = evidence["mode"]
        print(
            f"developer-sandbox crossover probe ({mode}): "
            f"{evidence['summary']['passed']}/{evidence['summary']['total']} passed",
        )
        for row in results:
            mark = "PASS" if row.passed else "FAIL"
            print(
                f"  [{mark}] {row.source}->{row.target} {row.surface} status={row.status}",
            )

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
