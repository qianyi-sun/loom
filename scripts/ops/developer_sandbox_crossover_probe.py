#!/usr/bin/env python3
"""Secret-safe cross-sandbox negative credential probe (A3).

Default mode is dry-run / CI: plan pairwise edges against injected base URLs
and emit fingerprint-only evidence JSON without sending credentials.

``--execute`` is the live oldlab-2 pairwise matrix. It is fail-closed:

- binds CP/MinIO targets to exact reviewed profile loopback endpoints
- requires non-symlink secret files with owner/mode checks
- requires complete MinIO negatives and same-sandbox positive controls
- requires a single candidate SHA matching every sandbox state readback

Never accepts raw tokens on the CLI. Never prints or stores worker tokens,
admin tokens, or MinIO passwords.

CI-safe dry-run / dual-stack integration negatives are not live A3 host
evidence and are not #896 soak evidence.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from loom.admin_secret import AdminSecretConfigError, load_admin_secret_file
from loom.worker_token import (
    DEFAULT_WORKER_TOKEN_ENV_KEY,
    read_env_file_value,
    worker_token_fingerprint,
)

ALLOWED_SANDBOXES = ("qianyi", "hongjian", "devansh")
STATE_SCHEMA_VERSION = 1
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
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
ADMIN_MINT_SAME_STATUSES = frozenset({200, 201})
ADMIN_MINT_FOREIGN_STATUSES = frozenset({401, 403})
MINIO_FOREIGN_ERROR_CODES = frozenset(
    {
        "AccessDenied",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "InvalidArgument",
        "NoSuchBucket",
        "404",
    },
)


@dataclass(frozen=True)
class SandboxProfileView:
    sandbox: str
    bind_address: str
    compose_project: str
    state_root: Path
    control_plane_port: int
    minio_port: int
    artifacts_bucket: str
    trajectories_bucket: str
    task_bucket: str

    @property
    def control_plane_url(self) -> str:
        return f"http://{self.bind_address}:{self.control_plane_port}"

    @property
    def minio_endpoint(self) -> str:
        return f"http://{self.bind_address}:{self.minio_port}"


@dataclass(frozen=True)
class CandidateIdentity:
    sandbox: str
    candidate_sha: str
    candidate_tree: str
    compose_project: str
    source_repo: str
    state_path: str
    updated_at: str | None


@dataclass(frozen=True)
class SandboxTarget:
    sandbox: str
    control_plane_url: str
    worker_token_file: Path
    admin_secret_file: Path
    minio_endpoint: str | None
    minio_access_key_file: Path | None
    minio_secret_key_file: Path | None
    own_bucket: str | None
    foreign_bucket: str | None
    candidate: CandidateIdentity | None = None


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


def _require_str(raw: dict[str, Any], path: Path, key: str) -> str:
    value = raw.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path}: {key} must be a non-empty string")
    return value.strip()


def load_profile(path: Path) -> SandboxProfileView:
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    ports = raw.get("ports")
    if not isinstance(ports, dict):
        raise ValueError(f"{path}: missing [ports]")
    object_store = raw.get("object_store")
    if not isinstance(object_store, dict):
        raise ValueError(f"{path}: missing [object_store]")
    for field in ("control_plane", "minio"):
        value = ports.get(field)
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{path}: ports.{field} must be an int")
    bind_address = _require_str(raw, path, "bind_address")
    if bind_address != "127.0.0.1":
        raise ValueError(f"{path}: bind_address must remain 127.0.0.1")
    return SandboxProfileView(
        sandbox=_require_str(raw, path, "sandbox"),
        bind_address=bind_address,
        compose_project=_require_str(raw, path, "compose_project"),
        state_root=Path(_require_str(raw, path, "state_root")),
        control_plane_port=int(ports["control_plane"]),
        minio_port=int(ports["minio"]),
        artifacts_bucket=_require_str(object_store, path, "artifacts_bucket"),
        trajectories_bucket=_require_str(object_store, path, "trajectories_bucket"),
        task_bucket=_require_str(object_store, path, "task_bucket"),
    )


def load_profiles(profiles_dir: Path) -> dict[str, SandboxProfileView]:
    profiles: dict[str, SandboxProfileView] = {}
    for sandbox in ALLOWED_SANDBOXES:
        path = profiles_dir / f"{sandbox}.toml"
        if not path.is_file():
            raise ValueError(f"missing profile {path}")
        profile = load_profile(path)
        if profile.sandbox != sandbox:
            raise ValueError(f"{path}: sandbox={profile.sandbox!r} != {sandbox!r}")
        profiles[sandbox] = profile
    return profiles


def secure_secret_file(path: Path, *, label: str) -> Path:
    """Require a non-symlink regular file owned by the caller with mode 0600."""
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValueError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise ValueError(f"{label} must be a regular non-symlink file: {path}")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode != 0o600:
        raise ValueError(f"{label} must be mode 0600 (got {mode:o}): {path}")
    if metadata.st_uid != os.geteuid():
        raise ValueError(
            f"{label} must be owned by the invoking user "
            f"(uid={os.geteuid()}, got uid={metadata.st_uid}): {path}",
        )
    return path.resolve(strict=True)


def _read_secret_file(path: Path, *, kind: str) -> str:
    secure = secure_secret_file(path, label=f"{kind} secret file")
    value = secure.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"{kind} secret file is empty: {path}")
    return value


def _load_worker_token(path: Path) -> str:
    secure = secure_secret_file(path, label="worker-token secret file")
    token = read_env_file_value(secure, DEFAULT_WORKER_TOKEN_ENV_KEY)
    if token is None:
        raw = secure.read_text(encoding="utf-8").strip()
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


def _load_admin_token(path: Path) -> str:
    secure = secure_secret_file(path, label="admin secret file")
    try:
        verifier = load_admin_secret_file(
            secure,
            require_safe_permissions=True,
        )
    except AdminSecretConfigError as exc:
        raise ValueError(str(exc)) from exc
    raw = tomllib.loads(secure.read_text(encoding="utf-8"))
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
    if parsed.scheme != "http" or not parsed.netloc or parsed.path not in {"", "/"}:
        raise ValueError(f"endpoint must be an http://host:port URL without path: {url}")
    return f"{parsed.scheme}://{parsed.netloc}"


def assert_endpoint_matches_reviewed(provided: str, reviewed: str, *, label: str) -> str:
    normalized = _normalize_http_url(provided)
    expected = _normalize_http_url(reviewed)
    if normalized != expected:
        raise ValueError(
            f"{label} must equal reviewed endpoint {expected!r} (got {normalized!r})",
        )
    return expected


def load_candidate_identity(
    profile: SandboxProfileView,
    *,
    expected_sha: str,
) -> CandidateIdentity:
    if _SHA_RE.fullmatch(expected_sha) is None:
        raise ValueError("candidate-sha must be a full lowercase 40-character hex digest")
    state_path = profile.state_root / "sandbox-state.json"
    secure = secure_secret_file(state_path, label=f"{profile.sandbox} sandbox state")
    try:
        payload = json.loads(secure.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"{profile.sandbox}: sandbox state is invalid") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"{profile.sandbox}: sandbox state is invalid")
    if payload.get("schema_version") != STATE_SCHEMA_VERSION:
        raise ValueError(f"{profile.sandbox}: sandbox state schema_version mismatch")
    if payload.get("sandbox") != profile.sandbox:
        raise ValueError(f"{profile.sandbox}: sandbox state sandbox mismatch")
    if payload.get("compose_project") != profile.compose_project:
        raise ValueError(f"{profile.sandbox}: sandbox state compose_project mismatch")
    candidate_sha = str(payload.get("candidate_sha", ""))
    candidate_tree = str(payload.get("candidate_tree", ""))
    if _SHA_RE.fullmatch(candidate_sha) is None or _SHA_RE.fullmatch(candidate_tree) is None:
        raise ValueError(f"{profile.sandbox}: sandbox state candidate binding is invalid")
    if candidate_sha != expected_sha:
        raise ValueError(
            f"{profile.sandbox}: candidate_sha mismatch "
            f"(state={candidate_sha}, expected={expected_sha})",
        )
    source_repo = payload.get("source_repo")
    if not isinstance(source_repo, str) or not source_repo.strip():
        raise ValueError(f"{profile.sandbox}: sandbox state missing source_repo")
    updated_at = payload.get("updated_at")
    if updated_at is not None and not isinstance(updated_at, str):
        raise ValueError(f"{profile.sandbox}: sandbox state updated_at is invalid")
    return CandidateIdentity(
        sandbox=profile.sandbox,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        compose_project=profile.compose_project,
        source_repo=source_repo.strip(),
        state_path=str(secure),
        updated_at=updated_at,
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
    if target.foreign_bucket is None:
        missing.append("foreign_bucket")
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
    fingerprint = worker_token_fingerprint(_load_worker_token(source.worker_token_file))
    surface = "worker_claim"
    if not execute:
        return ProbeResult(
            source=source.sandbox,
            target=target.sandbox,
            surface=surface,
            status="dry-run",
            passed=True,
            detail="would POST /trials/claim with source worker token on target CP",
            source_worker_fingerprint=fingerprint,
        )
    token = _load_worker_token(source.worker_token_file)
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
    token = _load_admin_token(source.admin_secret_file)
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
        from botocore.exceptions import ClientError
    except ImportError as exc:  # pragma: no cover
        raise ValueError("boto3 required for --execute MinIO probes") from exc

    assert source.minio_access_key_file is not None
    assert source.minio_secret_key_file is not None
    assert target.minio_endpoint is not None
    assert target.own_bucket is not None
    access = _read_secret_file(source.minio_access_key_file, kind="minio-access")
    secret = _read_secret_file(source.minio_secret_key_file, kind="minio-secret")
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
        passed = code in MINIO_FOREIGN_ERROR_CODES
        detail = f"foreign creds rejected: {code}"
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
        from botocore.exceptions import ClientError
    except ImportError as exc:  # pragma: no cover
        raise ValueError("boto3 required for --execute MinIO probes") from exc

    assert target.minio_access_key_file is not None
    assert target.minio_secret_key_file is not None
    assert target.minio_endpoint is not None
    assert target.own_bucket is not None
    access = _read_secret_file(target.minio_access_key_file, kind="minio-access")
    secret = _read_secret_file(target.minio_secret_key_file, kind="minio-secret")
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


def probe_minio_foreign_bucket(
    target: SandboxTarget,
    *,
    execute: bool,
) -> ProbeResult:
    surface = "minio_foreign_bucket"
    incomplete = (
        target.minio_endpoint is None
        or target.minio_access_key_file is None
        or target.minio_secret_key_file is None
        or target.foreign_bucket is None
    )
    if incomplete:
        if execute:
            raise ValueError(
                f"execute mode missing MinIO foreign-bucket inputs for {target.sandbox}",
            )
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="skipped",
            passed=True,
            detail="minio bucket probe not configured (dry-run informational)",
        )
    if not execute:
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="dry-run",
            passed=True,
            detail="would ListObjects on foreign bucket name with own creds",
        )
    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:  # pragma: no cover
        raise ValueError("boto3 required for --execute MinIO probes") from exc

    assert target.minio_access_key_file is not None
    assert target.minio_secret_key_file is not None
    assert target.minio_endpoint is not None
    assert target.foreign_bucket is not None
    access = _read_secret_file(target.minio_access_key_file, kind="minio-access")
    secret = _read_secret_file(target.minio_secret_key_file, kind="minio-secret")
    client = boto3.client(
        "s3",
        endpoint_url=target.minio_endpoint,
        aws_access_key_id=access,
        aws_secret_access_key=secret,
        region_name="us-east-1",
    )
    try:
        client.list_objects_v2(Bucket=target.foreign_bucket, MaxKeys=1)
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="Success",
            passed=False,
            detail="foreign bucket unexpectedly readable",
        )
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "ClientError"))
        passed = code in MINIO_FOREIGN_ERROR_CODES
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status=code,
            passed=passed,
            detail=f"foreign bucket rejected: {code}",
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
    for name in names:
        target = targets[name]
        if include_same_sandbox or execute:
            results.append(probe_minio_own_positive(target, execute=execute))
        results.append(probe_minio_foreign_bucket(target, execute=execute))
    return results


def build_evidence(
    results: list[ProbeResult],
    *,
    execute: bool,
    candidate_sha: str | None,
    candidates: list[CandidateIdentity],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-crossover-probe",
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "mode": "execute" if execute else "dry-run",
        "candidate_sha": candidate_sha,
        "candidates": [
            {
                "sandbox": row.sandbox,
                "candidate_sha": row.candidate_sha,
                "candidate_tree": row.candidate_tree,
                "compose_project": row.compose_project,
                "source_repo": row.source_repo,
                "state_path": row.state_path,
                "updated_at": row.updated_at,
            }
            for row in candidates
        ],
        "notes": [
            "CI-safe dry-run / dual-stack negatives are not live A3 host evidence",
            "A3 crossover evidence is not #896 soak evidence",
            "secret values are never recorded; worker tokens appear only as fingerprints",
        ],
        "results": [row.as_dict() for row in results],
        "summary": {
            "total": len(results),
            "passed": sum(1 for row in results if row.passed),
            "failed": sum(1 for row in results if not row.passed),
        },
    }


def build_targets(
    args: argparse.Namespace,
    *,
    execute: bool,
) -> tuple[dict[str, SandboxTarget], list[CandidateIdentity], str | None]:
    profiles: dict[str, SandboxProfileView] | None = None
    if args.profiles_dir is not None or execute:
        if args.profiles_dir is None:
            raise ValueError("--profiles-dir is required for --execute")
        profiles = load_profiles(Path(args.profiles_dir))

    candidate_sha: str | None = args.candidate_sha
    candidates: list[CandidateIdentity] = []
    if execute:
        if not candidate_sha:
            raise ValueError("--candidate-sha is required for --execute")
        if profiles is None:
            raise ValueError("--profiles-dir is required for --execute")
        for sandbox in ALLOWED_SANDBOXES:
            candidates.append(
                load_candidate_identity(profiles[sandbox], expected_sha=candidate_sha),
            )
        shas = {row.candidate_sha for row in candidates}
        if len(shas) != 1:
            raise ValueError(f"mixed candidate SHAs across sandboxes: {sorted(shas)}")

    targets: dict[str, SandboxTarget] = {}
    for sandbox in ALLOWED_SANDBOXES:
        worker_file = getattr(args, f"{sandbox}_worker_token_file", None)
        admin_file = getattr(args, f"{sandbox}_admin_secret_file", None)
        cp_override = getattr(args, f"{sandbox}_cp_url", None)
        minio_override = getattr(args, f"{sandbox}_minio_endpoint", None)
        access_file = getattr(args, f"{sandbox}_minio_access_key_file", None)
        secret_file = getattr(args, f"{sandbox}_minio_secret_key_file", None)
        own_bucket_override = getattr(args, f"{sandbox}_own_bucket", None)
        foreign_bucket_override = getattr(args, f"{sandbox}_foreign_bucket", None)

        if execute:
            if profiles is None:
                raise ValueError("--profiles-dir is required for --execute")
            if not worker_file or not admin_file or not access_file or not secret_file:
                raise ValueError(
                    f"execute mode requires --{sandbox}-worker-token-file, "
                    f"--{sandbox}-admin-secret-file, "
                    f"--{sandbox}-minio-access-key-file, and "
                    f"--{sandbox}-minio-secret-key-file",
                )
            profile = profiles[sandbox]
            cp_url = profile.control_plane_url
            if cp_override:
                cp_url = assert_endpoint_matches_reviewed(
                    cp_override,
                    profile.control_plane_url,
                    label=f"--{sandbox}-cp-url",
                )
            minio_endpoint = profile.minio_endpoint
            if minio_override:
                minio_endpoint = assert_endpoint_matches_reviewed(
                    minio_override,
                    profile.minio_endpoint,
                    label=f"--{sandbox}-minio-endpoint",
                )
            own_bucket = own_bucket_override or profile.artifacts_bucket
            if own_bucket != profile.artifacts_bucket:
                raise ValueError(
                    f"--{sandbox}-own-bucket must equal reviewed artifacts "
                    f"bucket {profile.artifacts_bucket!r}",
                )
            # Foreign bucket is a peer sandbox artifacts bucket.
            peer = next(name for name in ALLOWED_SANDBOXES if name != sandbox)
            expected_foreign = profiles[peer].artifacts_bucket
            foreign_bucket = foreign_bucket_override or expected_foreign
            if foreign_bucket not in {
                profiles[name].artifacts_bucket for name in ALLOWED_SANDBOXES if name != sandbox
            }:
                raise ValueError(
                    f"--{sandbox}-foreign-bucket must be a peer sandbox artifacts bucket",
                )
            candidate = next(row for row in candidates if row.sandbox == sandbox)
            targets[sandbox] = SandboxTarget(
                sandbox=sandbox,
                control_plane_url=cp_url,
                worker_token_file=secure_secret_file(
                    Path(worker_file),
                    label=f"{sandbox} worker-token secret file",
                ),
                admin_secret_file=secure_secret_file(
                    Path(admin_file),
                    label=f"{sandbox} admin secret file",
                ),
                minio_endpoint=minio_endpoint,
                minio_access_key_file=secure_secret_file(
                    Path(access_file),
                    label=f"{sandbox} minio-access secret file",
                ),
                minio_secret_key_file=secure_secret_file(
                    Path(secret_file),
                    label=f"{sandbox} minio-secret secret file",
                ),
                own_bucket=own_bucket,
                foreign_bucket=foreign_bucket,
                candidate=candidate,
            )
            continue

        if not cp_override or not worker_file or not admin_file:
            continue
        targets[sandbox] = SandboxTarget(
            sandbox=sandbox,
            control_plane_url=_normalize_http_url(cp_override),
            worker_token_file=secure_secret_file(
                Path(worker_file),
                label=f"{sandbox} worker-token secret file",
            ),
            admin_secret_file=secure_secret_file(
                Path(admin_file),
                label=f"{sandbox} admin secret file",
            ),
            minio_endpoint=(_normalize_http_url(minio_override) if minio_override else None),
            minio_access_key_file=(
                secure_secret_file(
                    Path(access_file),
                    label=f"{sandbox} minio-access secret file",
                )
                if access_file
                else None
            ),
            minio_secret_key_file=(
                secure_secret_file(
                    Path(secret_file),
                    label=f"{sandbox} minio-secret secret file",
                )
                if secret_file
                else None
            ),
            own_bucket=own_bucket_override,
            foreign_bucket=foreign_bucket_override,
        )

    minimum = 3 if execute else 2
    if len(targets) < minimum:
        raise ValueError(
            f"configure at least {minimum} sandboxes "
            f"(got {sorted(targets)}); execute requires all three",
        )
    if execute and set(targets) != set(ALLOWED_SANDBOXES):
        raise ValueError(
            f"execute mode requires all sandboxes {list(ALLOWED_SANDBOXES)}",
        )
    return targets, candidates, candidate_sha


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
        "--profiles-dir",
        type=Path,
        default=None,
        help="Developer-sandbox profiles dir (required for --execute)",
    )
    parser.add_argument(
        "--candidate-sha",
        default=None,
        help="Exact full lowercase candidate SHA bound in every sandbox-state.json",
    )
    parser.add_argument(
        "--include-same-sandbox",
        action="store_true",
        help="Also probe each sandbox against itself (always on for --execute)",
    )
    parser.add_argument(
        "--write-evidence",
        type=Path,
        default=None,
        help="Write secret-safe evidence JSON to this path",
    )
    parser.add_argument("--json", action="store_true")
    for sandbox in ALLOWED_SANDBOXES:
        parser.add_argument(f"--{sandbox}-cp-url", default=None)
        parser.add_argument(f"--{sandbox}-worker-token-file", default=None)
        parser.add_argument(f"--{sandbox}-admin-secret-file", default=None)
        parser.add_argument(f"--{sandbox}-minio-endpoint", default=None)
        parser.add_argument(f"--{sandbox}-minio-access-key-file", default=None)
        parser.add_argument(f"--{sandbox}-minio-secret-key-file", default=None)
        parser.add_argument(f"--{sandbox}-own-bucket", default=None)
        parser.add_argument(f"--{sandbox}-foreign-bucket", default=None)
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
        targets, candidates, candidate_sha = build_targets(args, execute=args.execute)
        results = run_probe_matrix(
            targets,
            execute=args.execute,
            include_same_sandbox=args.include_same_sandbox,
        )
        evidence = build_evidence(
            results,
            execute=args.execute,
            candidate_sha=candidate_sha,
            candidates=candidates,
        )
        secret_errors = assert_evidence_secret_free(evidence)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if secret_errors:
        for err in secret_errors:
            print(f"error: {err}", file=sys.stderr)
        return 1

    if args.write_evidence is not None:
        args.write_evidence.write_text(
            json.dumps(evidence, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

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
