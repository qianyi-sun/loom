#!/usr/bin/env python3
"""Secret-safe cross-sandbox negative credential probe (A3).

Default mode is dry-run / CI: load secret *file paths*, exercise injected
base URLs, and emit fingerprint-only evidence JSON. Pass ``--execute`` for
the live oldlab-2 pairwise matrix after three sandboxes are installed.

Never accepts raw tokens on the CLI. Never prints or stores worker tokens,
admin tokens, or MinIO passwords.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from loom.admin_secret import AdminSecretConfigError, load_admin_secret_file
from loom.worker_token import (
    DEFAULT_WORKER_TOKEN_ENV_KEY,
    read_env_file_value,
    worker_token_fingerprint,
)

ALLOWED_SANDBOXES = ("qianyi", "hongjian", "devansh")
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


def _read_secret_file(path: Path, *, kind: str) -> str:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"{kind} secret path must be a regular file: {path}")
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError(f"{kind} secret file is empty: {path}")
    return value


def _load_worker_token(path: Path) -> str:
    token = read_env_file_value(path, DEFAULT_WORKER_TOKEN_ENV_KEY)
    if token is None:
        # Allow a one-line token file (ops convenience) without KEY=value.
        raw = _read_secret_file(path, kind="worker-token")
        if "=" in raw.splitlines()[0]:
            raise ValueError(
                f"{path}: missing {DEFAULT_WORKER_TOKEN_ENV_KEY}= entry",
            )
        token = raw.splitlines()[0].strip()
    if not token:
        raise ValueError(f"{path}: empty worker token")
    return token


def _load_admin_token(path: Path) -> str:
    try:
        verifier = load_admin_secret_file(
            path,
            require_safe_permissions=False,
        )
    except AdminSecretConfigError as exc:
        raise ValueError(str(exc)) from exc
    # AdminSecretVerifier stores only a hash; re-read the configured token
    # for the HTTP probe without logging it.
    raw = tomllib.loads(path.read_text(encoding="utf-8"))
    admin = raw.get("admin")
    if not isinstance(admin, dict):
        raise ValueError(f"{path}: missing [admin]")
    token = admin.get("token")
    if not isinstance(token, str) or not token.strip():
        raise ValueError(f"{path}: [admin].token must be a non-empty string")
    if not verifier.verify(token.strip()):
        raise ValueError(f"{path}: admin token failed local verify")
    return token.strip()


def _http_status(
    method: str,
    url: str,
    *,
    token: str | None = None,
    body: dict[str, Any] | None = None,
    timeout: float = 10.0,
) -> tuple[int, str]:
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
    # Same-sandbox: auth should succeed (may 404/409 on missing worker).
    # Cross-sandbox: must be 401.
    if source.sandbox == target.sandbox:
        passed = isinstance(status, int) and status != 401
        expect = "own-token accepted (non-401)"
    else:
        passed = status == 401
        expect = "foreign worker token rejected with 401"
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
        passed = isinstance(status, int) and status in {200, 201}
        expect = "own admin accepted"
    else:
        # Admin routes collapse unauthenticated + missing-scope into 403.
        passed = status in {401, 403}
        expect = "foreign admin rejected with 401/403"
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
    if (
        target.minio_endpoint is None
        or source.minio_access_key_file is None
        or source.minio_secret_key_file is None
        or target.own_bucket is None
    ):
        return ProbeResult(
            source=source.sandbox,
            target=target.sandbox,
            surface=surface,
            status="skipped",
            passed=True,
            detail="minio endpoints/creds not configured for this target",
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
        code = "Success"
        passed = source.sandbox == target.sandbox
        detail = "list succeeded"
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", "ClientError"))
        if source.sandbox == target.sandbox:
            passed = False
            detail = f"own creds unexpectedly rejected: {code}"
        else:
            passed = code in {
                "AccessDenied",
                "InvalidAccessKeyId",
                "SignatureDoesNotMatch",
                "InvalidArgument",
            }
            detail = f"foreign creds rejected: {code}"
    return ProbeResult(
        source=source.sandbox,
        target=target.sandbox,
        surface=surface,
        status=code,
        passed=passed,
        detail=detail,
    )


def probe_minio_foreign_bucket(
    target: SandboxTarget,
    *,
    execute: bool,
) -> ProbeResult:
    surface = "minio_foreign_bucket"
    if (
        target.minio_endpoint is None
        or target.minio_access_key_file is None
        or target.minio_secret_key_file is None
        or target.foreign_bucket is None
    ):
        return ProbeResult(
            source=target.sandbox,
            target=target.sandbox,
            surface=surface,
            status="skipped",
            passed=True,
            detail="minio bucket probe not configured",
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
        passed = code in {"NoSuchBucket", "AccessDenied", "404"}
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
    pairs = directed_pairs(names)
    if include_same_sandbox:
        pairs = [(n, n) for n in names] + pairs
    for source_name, target_name in pairs:
        source = targets[source_name]
        target = targets[target_name]
        results.append(
            probe_worker_claim_crossover(source, target, execute=execute),
        )
        results.append(
            probe_admin_mint_crossover(source, target, execute=execute),
        )
        if source_name != target_name:
            results.append(
                probe_minio_foreign_creds(source, target, execute=execute),
            )
    for name in names:
        results.append(probe_minio_foreign_bucket(targets[name], execute=execute))
    return results


def build_evidence(
    results: list[ProbeResult],
    *,
    execute: bool,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-crossover-probe",
        "generated_at": datetime.now(UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace(
            "+00:00",
            "Z",
        ),
        "mode": "execute" if execute else "dry-run",
        "notes": [
            "A3 crossover negatives; not #896 soak evidence",
            "secret values are never recorded; worker tokens appear only as fingerprints",
        ],
        "results": [row.as_dict() for row in results],
        "summary": {
            "total": len(results),
            "passed": sum(1 for row in results if row.passed),
            "failed": sum(1 for row in results if not row.passed),
        },
    }


def _parse_target_args(args: argparse.Namespace) -> dict[str, SandboxTarget]:
    targets: dict[str, SandboxTarget] = {}
    for sandbox in ALLOWED_SANDBOXES:
        cp = getattr(args, f"{sandbox}_cp_url", None)
        worker_file = getattr(args, f"{sandbox}_worker_token_file", None)
        admin_file = getattr(args, f"{sandbox}_admin_secret_file", None)
        if not cp or not worker_file or not admin_file:
            continue
        targets[sandbox] = SandboxTarget(
            sandbox=sandbox,
            control_plane_url=cp,
            worker_token_file=Path(worker_file),
            admin_secret_file=Path(admin_file),
            minio_endpoint=getattr(args, f"{sandbox}_minio_endpoint", None),
            minio_access_key_file=(
                Path(p) if (p := getattr(args, f"{sandbox}_minio_access_key_file", None)) else None
            ),
            minio_secret_key_file=(
                Path(p) if (p := getattr(args, f"{sandbox}_minio_secret_key_file", None)) else None
            ),
            own_bucket=getattr(args, f"{sandbox}_own_bucket", None),
            foreign_bucket=getattr(args, f"{sandbox}_foreign_bucket", None),
        )
    if len(targets) < 2:
        raise ValueError(
            "configure at least two sandboxes via --<sandbox>-cp-url and "
            "matching --<sandbox>-worker-token-file / --<sandbox>-admin-secret-file",
        )
    return targets


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="developer_sandbox_crossover_probe",
        description=__doc__,
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Perform live HTTP/MinIO probes (default: dry-run)",
    )
    parser.add_argument(
        "--include-same-sandbox",
        action="store_true",
        help="Also probe each sandbox against itself as a positive control",
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
        targets = _parse_target_args(args)
        results = run_probe_matrix(
            targets,
            execute=args.execute,
            include_same_sandbox=args.include_same_sandbox,
        )
        evidence = build_evidence(results, execute=args.execute)
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
