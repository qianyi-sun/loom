#!/usr/bin/env python3
"""Fixed allocation-local health probe for one candidate-bound sandbox link."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import tempfile
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import Request, urlopen

SERVICES = {
    "control-plane": "http://sandbox-link:8080/healthz",
    "gateway": "http://sandbox-link:9100/healthz",
    "minio": "http://sandbox-link:9000/minio/health/live",
}
REQUIRED_ENV = {
    "LOOM_WORKER_SANDBOX_IDENTITY": "runtime_id",
    "LOOM_WORKER_CANDIDATE_SHA": "candidate_sha",
    "LOOM_WORKER_ENV_ID": "env_id",
    "LOOM_WORKER_RESOURCE_GENERATION": "applied_resource_generation",
    "LOOM_WORKER_CANDIDATE_ID": "candidate_id",
    "LOOM_WORKER_CANDIDATE_TREE": "candidate_tree",
    "LOOM_WORKER_REGISTRY_GENERATION": "registry_generation",
    "LOOM_WORKER_REGISTRY_PAYLOAD_SHA256": "registry_snapshot_sha256",
}


class ProbeError(RuntimeError):
    """The allocation-local probe failed closed."""


def _canonical(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(dict(value), sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
            "ascii"
        )
        + b"\n"
    )


def _read(path: Path) -> tuple[dict[str, object], bytes]:
    try:
        metadata = path.lstat()
        raw = path.read_bytes()
    except OSError as exc:
        raise ProbeError("probe request is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or len(raw) > (1 << 20)
    ):
        raise ProbeError("probe request is unsafe")
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProbeError("probe request is invalid") from exc
    if not isinstance(payload, dict) or raw != _canonical(payload):
        raise ProbeError("probe request is invalid")
    return payload, raw


def _write(path: Path, payload: Mapping[str, object]) -> None:
    if path.parent != Path("/run/loom-acceptance-output"):
        raise ProbeError("probe output path is invalid")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".result.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        content = _canonical(payload)
        offset = 0
        while offset < len(content):
            written = os.write(descriptor, content[offset:])
            if written <= 0:
                raise OSError("short probe result write")
            offset += written
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, path)
    except OSError as exc:
        raise ProbeError("probe result publication failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)


def execute(request_path: Path, result_path: Path) -> dict[str, object]:
    if request_path != Path("/run/loom-acceptance/request.json"):
        raise ProbeError("probe request path is invalid")
    request, _raw = _read(request_path)
    unsigned = {key: value for key, value in request.items() if key != "payload_sha256"}
    if (
        request.get("kind") != "loom.developer-environment.acceptance-probe-domain-request"
        or request.get("action") != "developer-environment-acceptance-probe"
        or request.get("health_services") != list(SERVICES)
        or request.get("general_admission_authorized") is not False
        or request.get("foreign_job_action") != "observe-only"
        or request.get("payload_sha256") != hashlib.sha256(_canonical(unsigned)).hexdigest()
        or os.environ.get("LOOM_WORKER_SLURM_JOB_ID") != os.environ.get("SLURM_JOB_ID")
        or not str(os.environ.get("SLURM_JOB_ID", "")).isdigit()
        or any(
            os.environ.get(variable) != str(request.get(field))
            for variable, field in REQUIRED_ENV.items()
        )
    ):
        raise ProbeError("probe environment binding is invalid")
    health: dict[str, object] = {}
    for service, endpoint in SERVICES.items():
        try:
            with urlopen(Request(endpoint, method="GET"), timeout=30) as response:
                body = response.read((1 << 20) + 1)
                status = response.status
        except OSError as exc:
            raise ProbeError(f"{service} health endpoint is unavailable") from exc
        if status != 200 or len(body) > (1 << 20):
            raise ProbeError(f"{service} health endpoint is unhealthy")
        binding = {
            "deployment_id": request["deployment_id"],
            "env_id": request["env_id"],
            "candidate_sha": request["candidate_sha"],
            "candidate_tree": request["candidate_tree"],
            "domain": request["domain"],
            "service": service,
            "endpoint": endpoint,
        }
        health[service] = {
            "service": service,
            "status": "healthy",
            "http_status": 200,
            "candidate_binding_sha256": hashlib.sha256(_canonical(binding)).hexdigest(),
            "response_sha256": hashlib.sha256(body).hexdigest(),
        }
    result = {
        "schema_version": 1,
        "kind": "loom.developer-environment.acceptance-probe-container-result",
        "request_payload_sha256": request["payload_sha256"],
        "slurm_job_id": os.environ["SLURM_JOB_ID"],
        "health": health,
        "completed_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
    }
    _write(result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if not args.execute:
            raise ProbeError("probe execution was not authorized")
        execute(args.request, args.result)
        return 0
    except ProbeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
