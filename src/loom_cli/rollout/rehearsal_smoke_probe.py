"""Fixed, secret-safe Tier-3 admin admission probe inside the candidate service image."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from pathlib import Path

from loom.admin_secret import AdminSecretConfigError, AdminSecretVerifier
from loom_cli.rollout.admin_smoke_contract import (
    AdminSmokeAuthority,
    AdminSmokeContract,
    decode_json_object,
)

_SERVICE_BASE = "http://loom-service:8090"
_ADMIN_SECRET_PATH = Path("/var/run/loom/rehearsal-admin/secrets.toml")
_MAX_SECRET_BYTES = 16 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024
_STABLE_STAT_FIELDS = (
    "st_dev",
    "st_ino",
    "st_mode",
    "st_uid",
    "st_gid",
    "st_nlink",
    "st_size",
    "st_mtime_ns",
    "st_ctime_ns",
)


class RehearsalSmokeProbeError(RuntimeError):
    """A bounded, non-secret Tier-3 admission failure."""


def load_rehearsal_admin_token(
    path: Path,
    *,
    expected_owner_uid: int = 0,
    allowed_group_gid: int | None = None,
) -> str:
    """Read one exact regular secret file without following any symlink."""
    if not path.is_absolute() or ".." in path.parts or path == Path("/"):
        raise RehearsalSmokeProbeError("admin secret path authority is invalid")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise RehearsalSmokeProbeError("admin secret cannot be opened safely") from exc
    try:
        before = os.fstat(fd)
        effective_group = os.getegid() if allowed_group_gid is None else allowed_group_gid
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != expected_owner_uid
            or before.st_gid != effective_group
            or stat.S_IMODE(before.st_mode) != 0o440
            or before.st_nlink != 1
            or not 1 <= before.st_size <= _MAX_SECRET_BYTES
        ):
            raise RehearsalSmokeProbeError("admin secret metadata authority is invalid")
        chunks: list[bytes] = []
        remaining = _MAX_SECRET_BYTES + 1
        while remaining > 0:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(fd)
        if len(payload) > _MAX_SECRET_BYTES or any(
            getattr(before, field) != getattr(after, field) for field in _STABLE_STAT_FIELDS
        ):
            raise RehearsalSmokeProbeError("admin secret changed while it was read")
    finally:
        os.close(fd)
    try:
        record = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise RehearsalSmokeProbeError("admin secret payload is invalid") from exc
    admin = record.get("admin")
    token = admin.get("token") if isinstance(admin, Mapping) else None
    if not isinstance(token, str):
        raise RehearsalSmokeProbeError("admin secret payload is incomplete")
    try:
        AdminSecretVerifier.from_token(token)
    except AdminSecretConfigError as exc:
        raise RehearsalSmokeProbeError("admin secret token contract is invalid") from exc
    return token


def _http(
    method: str,
    path: str,
    *,
    token: str,
    payload: Mapping[str, object] | None = None,
    headers: Mapping[str, str] | None = None,
) -> tuple[int, bytes]:
    if method not in {"GET", "POST"} or not path.startswith("/api/v1/"):
        raise RehearsalSmokeProbeError("service request authority is invalid")
    body = None if payload is None else json.dumps(payload, sort_keys=True).encode()
    request = urllib.request.Request(_SERVICE_BASE + path, data=body, method=method)
    request.add_header("Authorization", f"Bearer {token}")
    if body is not None:
        request.add_header("Content-Type", "application/json")
    for key, value in (headers or {}).items():
        request.add_header(key, value)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            status = response.status
            response_body = response.read(_MAX_RESPONSE_BYTES + 1)
    except urllib.error.HTTPError as exc:
        status = exc.code
        response_body = exc.read(_MAX_RESPONSE_BYTES + 1)
    except (OSError, urllib.error.URLError) as exc:
        raise RehearsalSmokeProbeError("service request failed") from exc
    if len(response_body) > _MAX_RESPONSE_BYTES:
        raise RehearsalSmokeProbeError("service response exceeded evidence bound")
    return status, response_body


def run_probe(
    *,
    plan_sha256: str,
    batch_name: str,
    authority: AdminSmokeAuthority,
    admin_secret_path: Path = _ADMIN_SECRET_PATH,
    expected_owner_uid: int = 0,
    allowed_group_gid: int | None = None,
) -> dict[str, object]:
    """Prove exact-candidate admission and cloned-DB persistence, not completion."""
    if len(plan_sha256) != 64 or any(item not in "0123456789abcdef" for item in plan_sha256):
        raise RehearsalSmokeProbeError("rehearsal plan identity is invalid")
    contract = AdminSmokeContract(authority)
    token = load_rehearsal_admin_token(
        admin_secret_path,
        expected_owner_uid=expected_owner_uid,
        allowed_group_gid=allowed_group_gid,
    )
    evidence: dict[str, str] = {}

    def request(
        method: str,
        path: str,
        *,
        payload: Mapping[str, object] | None = None,
        headers: Mapping[str, str] | None = None,
        accepted: frozenset[int] = frozenset({200}),
    ) -> Mapping[str, object]:
        status, body = _http(method, path, token=token, payload=payload, headers=headers)
        evidence[f"{method.lower()}:{path.split('?', 1)[0]}"] = hashlib.sha256(
            str(status).encode() + b"\0" + body
        ).hexdigest()
        if status not in accepted:
            raise RehearsalSmokeProbeError(f"service request returned HTTP {status}")
        decoded = decode_json_object(body)
        if decoded is None:
            raise RehearsalSmokeProbeError("service response is not a JSON object")
        return decoded

    health = request("GET", "/api/v1/health")
    if health.get("status") not in {"ok", "healthy"}:
        raise RehearsalSmokeProbeError("service health contract failed")
    whoami = request("GET", "/api/v1/auth/whoami")
    identity_error = contract.validate_admin_identity(whoami)
    if identity_error is not None:
        raise RehearsalSmokeProbeError(identity_error)
    catalog = request("GET", "/api/v1/benchmarks")
    catalog_error = contract.validate_benchmark_catalog(catalog)
    if catalog_error is not None:
        raise RehearsalSmokeProbeError(catalog_error)
    quoted_task = urllib.parse.quote(authority.task_id, safe="/")
    task = request("GET", f"/api/v1/tasks/{quoted_task}")
    if task.get("id") not in {None, authority.task_id} and task.get("task_id") != authority.task_id:
        raise RehearsalSmokeProbeError("service task identity drifted")

    query = urllib.parse.urlencode({"team_id": authority.team_id, "q": batch_name, "limit": "20"})
    existing = request("GET", f"/api/v1/batches?{query}")
    batch_id = contract.existing_batch_id(existing, batch_name=batch_name)
    recovered = batch_id is not None
    if batch_id is None:
        submitted = request(
            "POST",
            "/api/v1/admin/batches/on-behalf",
            payload=contract.submission_payload(batch_name=batch_name),
            headers={"X-Loom-Admin-Actor": authority.admin_actor},
            accepted=frozenset({200, 201}),
        )
        raw_batch_id = submitted.get("id") or submitted.get("batch_id")
        if not isinstance(raw_batch_id, str) or not raw_batch_id:
            raise RehearsalSmokeProbeError("service submission returned no batch id")
        batch_id = raw_batch_id
    persisted = request("GET", f"/api/v1/batches/{batch_id}")
    persisted_error = contract.validate_admitted_batch(
        persisted,
        batch_id=batch_id,
        batch_name=batch_name,
    )
    if persisted_error is not None:
        raise RehearsalSmokeProbeError(persisted_error)
    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "evidence": dict(sorted(evidence.items())),
        "persisted": True,
        "plan_sha256": plan_sha256,
        "recovered": recovered,
        "schema_version": 1,
        "status": "ready",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="loom-rehearsal-smoke-probe")
    parser.add_argument("--plan-sha256", required=True)
    parser.add_argument("--batch-name", required=True)
    parser.add_argument("--represented-username", required=True)
    parser.add_argument("--team-id", required=True)
    parser.add_argument("--admin-actor", required=True)
    parser.add_argument("--task-id", required=True)
    parser.add_argument("--required-worker-pool", required=True)
    parser.add_argument("--agent", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_probe(
            plan_sha256=args.plan_sha256,
            batch_name=args.batch_name,
            authority=AdminSmokeAuthority(
                represented_username=args.represented_username,
                team_id=args.team_id,
                admin_actor=args.admin_actor,
                task_id=args.task_id,
                required_worker_pool=args.required_worker_pool,
                agent=args.agent,
            ),
        )
    except (RehearsalSmokeProbeError, ValueError) as exc:
        print(
            json.dumps(
                {"failure_code": "rehearsal-api-smoke-failed", "status": "blocked"},
                sort_keys=True,
            )
        )
        print(str(exc), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
