#!/usr/bin/env python3
"""Run one fixed, registry-bound finalization probe in each Slurm domain.

This module is installed as root-owned infrastructure code. It never imports
or executes code from a developer candidate. The fixed node authority action
implemented by the node/Slurm layer is:

``developer-environment-acceptance-probe``

Its payload kind is
``loom.developer-environment.acceptance-probe-domain-request``. The node
authority must use the deterministic ``request_id`` as its durable idempotency
key, submit at most one job under the exact registry service user/account/QoS,
and return the closed domain-receipt schema validated below. The job has a
fixed 300-second Slurm time limit, performs only candidate-bound Control Plane,
Gateway, and MinIO reachability checks, is never cancelled by this workflow,
and reaches a natural terminal state. A retry of the same request must return
the original job and receipt instead of submitting another allocation.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Final, cast

from scripts.ops.developer_environment_registry import (
    CANDIDATE_ID_RE,
    DEPLOYMENT_ID_RE,
    DIGEST_RE,
    ENV_ID_RE,
    PRINCIPAL_RE,
    RUNTIME_ID_RE,
    SHA_RE,
    DeveloperEnvironmentRegistry,
    RegistryError,
)

RUNTIME_ROOT: Final = Path("/var/lib/loom-developer-environment-runtime")
REGISTRY_SNAPSHOT: Final = Path(
    "/var/lib/loom-developer-environment-registry/current-snapshot.json"
)
NODE_TRANSPORT: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
REQUEST_KIND: Final = "loom.developer-environment.runtime-request"
DOMAIN_REQUEST_KIND: Final = "loom.developer-environment.acceptance-probe-domain-request"
DOMAIN_RECEIPT_KIND: Final = "loom.developer-environment.acceptance-probe-domain-receipt"
COMBINED_RECEIPT_KIND: Final = "loom.developer-environment.acceptance-probe-receipt"
TRANSPORT_ACTION: Final = "developer-environment-acceptance-probe"
ACTION: Final = "acceptance-probe"
MAX_FILE_BYTES: Final = 8 * 1024 * 1024
PROBE_TIME_LIMIT_SECONDS: Final = 300
TERMINAL_STATE: Final = "COMPLETED"
EXIT_CODE: Final = "0:0"
SERVICE_NAMES: Final = ("control-plane", "gateway", "minio")
JOB_ID_RE: Final = re.compile(r"[1-9][0-9]*(?:_[0-9]+)?")
JOB_NAME_RE: Final = re.compile(r"loom-env-[a-z0-9][a-z0-9-]{0,62}-finalize-[0-9a-f]{12}")


class AcceptanceProbeError(RuntimeError):
    """A bounded, secret-safe finalization-probe error."""


@dataclass(frozen=True, slots=True)
class DomainRoute:
    domain: str
    cluster: str
    transport_node: str
    submit_host: str
    controller: str
    allowed_nodes: tuple[str, ...]


ROUTES: Final = {
    "oldlab": DomainRoute(
        domain="oldlab",
        cluster="trt-oldlab",
        transport_node="oldlab-2",
        submit_host="trt-EAI-OLDLAB-2",
        controller="TRT-EAI-OLDLAB-1",
        allowed_nodes=tuple(f"oldlab-{index}" for index in range(1, 6)),
    ),
    "gb10": DomainRoute(
        domain="gb10",
        cluster="trt-gb10",
        transport_node="trt-gb10-1",
        submit_host="trt-gb10-1",
        controller="trt-gb10-1",
        allowed_nodes=tuple(f"trt-gb10-{index}" for index in range(1, 16)),
    ),
}

RUNTIME_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "action",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "payload_sha256",
}

DOMAIN_REQUEST_FIELDS: Final = {
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

DOMAIN_RECEIPT_FIELDS: Final = {
    "schema_version",
    "kind",
    "status",
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
    "probe_request_sha256",
    "transport_request_id",
    "submission_count",
    "job",
    "health",
    "terminal",
    "job_output_sha256",
    "authority_receipt_sha256",
    "completed_at",
    "payload_sha256",
}

JOB_FIELDS: Final = {
    "job_id",
    "job_name",
    "user",
    "account",
    "qos",
    "submit_host",
    "controller",
    "allocation_nodes",
    "time_limit_seconds",
}

TERMINAL_FIELDS: Final = {
    "state",
    "exit_code",
    "natural_exit",
    "cancel_requested",
    "timed_out",
}

HEALTH_FIELDS: Final = {
    "service",
    "status",
    "http_status",
    "candidate_binding_sha256",
    "response_sha256",
}

TRANSPORT_RESPONSE_FIELDS: Final = {
    "schema_version",
    "request_id",
    "status",
    "action",
    "node",
    "domain",
    "sandbox",
    "candidate_sha",
    "candidate_tree",
    "payload_sha256",
    "result",
    "result_sha256",
    "completed_at",
}

COMBINED_FIELDS: Final = {
    "schema_version",
    "kind",
    "status",
    "action",
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
    "runtime_request_sha256",
    "domains",
    "completed_at",
    "payload_sha256",
}


def _canonical(value: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(value),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, UnicodeEncodeError) as exc:
        raise AcceptanceProbeError("acceptance probe payload is not canonical") from exc


def _digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _expected_identity(require_root_ownership: bool) -> tuple[int, int]:
    return (0, 0) if require_root_ownership else (os.geteuid(), os.getegid())


def _read_stable_regular(
    path: Path,
    *,
    description: str,
    require_root_ownership: bool,
    limit: int = MAX_FILE_BYTES,
) -> bytes:
    descriptor = -1
    try:
        lexical = path.lstat()
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
        )
        opened = os.fstat(descriptor)
        raw = os.pread(descriptor, limit + 1, 0)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except OSError as exc:
        raise AcceptanceProbeError(f"{description} is unavailable") from exc
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
    expected_uid, expected_gid = _expected_identity(require_root_ownership)
    if (
        len(raw) > limit
        or len(identities) != 1
        or not stat.S_ISREG(opened.st_mode)
        or stat.S_ISLNK(current.st_mode)
        or opened.st_nlink != 1
        or current.st_nlink != 1
        or (opened.st_uid, opened.st_gid) != (expected_uid, expected_gid)
        or stat.S_IMODE(opened.st_mode) != 0o600
    ):
        raise AcceptanceProbeError(f"{description} is unsafe")
    return raw


def _ensure_private_directory(
    path: Path,
    *,
    require_root_ownership: bool,
) -> None:
    expected_uid, expected_gid = _expected_identity(require_root_ownership)
    try:
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        metadata = path.lstat()
    except OSError as exc:
        raise AcceptanceProbeError("acceptance probe state root is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_uid, metadata.st_gid) != (expected_uid, expected_gid)
    ):
        raise AcceptanceProbeError("acceptance probe state root is unsafe")
    try:
        os.chmod(path, 0o700)
    except OSError as exc:
        raise AcceptanceProbeError("acceptance probe state root is unavailable") from exc


def _atomic_write(
    path: Path,
    payload: Mapping[str, Any],
    *,
    require_root_ownership: bool,
) -> None:
    _ensure_private_directory(
        path.parent,
        require_root_ownership=require_root_ownership,
    )
    if path.exists() or path.is_symlink():
        raise AcceptanceProbeError("acceptance probe receipt already exists")
    descriptor = -1
    temporary: Path | None = None
    directory_descriptor = -1
    try:
        descriptor, name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=path.parent,
        )
        temporary = Path(name)
        os.fchmod(descriptor, 0o600)
        raw = _canonical(payload)
        view = memoryview(raw)
        while view:
            written = os.write(descriptor, view)
            if written < 1:
                raise AcceptanceProbeError("acceptance probe receipt write failed")
            view = view[written:]
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        # The deploy authority holds the per-environment operation lock.  The
        # same-filesystem replace keeps publication crash-atomic, including a
        # crash between the remote authority response and local convergence.
        os.replace(temporary, path)
        temporary = None
        directory_descriptor = os.open(
            path.parent,
            os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
        )
        os.fsync(directory_descriptor)
    except AcceptanceProbeError:
        raise
    except OSError as exc:
        raise AcceptanceProbeError("acceptance probe receipt publication failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if directory_descriptor >= 0:
            os.close(directory_descriptor)
        if temporary is not None:
            try:
                temporary.unlink(missing_ok=True)
            except OSError:
                pass


def _load_json(
    path: Path,
    *,
    description: str,
    require_root_ownership: bool,
) -> dict[str, Any]:
    raw = _read_stable_regular(
        path,
        description=description,
        require_root_ownership=require_root_ownership,
    )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcceptanceProbeError(f"{description} is invalid") from exc
    if not isinstance(payload, dict) or raw != _canonical(payload):
        raise AcceptanceProbeError(f"{description} is invalid")
    return payload


def _runtime_request(
    deployment_id: str,
    *,
    runtime_root: Path,
    require_root_ownership: bool,
) -> dict[str, Any]:
    payload = _load_json(
        runtime_root / "requests" / f"{deployment_id}-{ACTION}.json",
        description="acceptance probe request",
        require_root_ownership=require_root_ownership,
    )
    unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
    if (
        set(payload) != RUNTIME_REQUEST_FIELDS
        or payload.get("schema_version") != 1
        or payload.get("kind") != REQUEST_KIND
        or payload.get("action") != ACTION
        or payload.get("deployment_id") != deployment_id
        or DEPLOYMENT_ID_RE.fullmatch(str(payload.get("deployment_id"))) is None
        or ENV_ID_RE.fullmatch(str(payload.get("env_id"))) is None
        or PRINCIPAL_RE.fullmatch(str(payload.get("principal_id"))) is None
        or RUNTIME_ID_RE.fullmatch(str(payload.get("runtime_id"))) is None
        or CANDIDATE_ID_RE.fullmatch(str(payload.get("candidate_id"))) is None
        or SHA_RE.fullmatch(str(payload.get("candidate_sha"))) is None
        or SHA_RE.fullmatch(str(payload.get("candidate_tree"))) is None
        or type(payload.get("resource_generation")) is not int
        or int(payload["resource_generation"]) < 2
        or type(payload.get("registry_generation")) is not int
        or int(payload["registry_generation"]) < 1
        or DIGEST_RE.fullmatch(str(payload.get("registry_snapshot_sha256"))) is None
        or payload.get("payload_sha256") != _digest(unsigned)
    ):
        raise AcceptanceProbeError("acceptance probe request binding is invalid")
    return payload


def _snapshot(
    path: Path,
    *,
    require_root_ownership: bool,
) -> dict[str, Any]:
    raw = _read_stable_regular(
        path,
        description="developer environment registry snapshot",
        require_root_ownership=require_root_ownership,
    )
    try:
        return DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except RegistryError as exc:
        raise AcceptanceProbeError("developer environment registry snapshot is invalid") from exc


def _binding(
    snapshot: Mapping[str, Any],
    request: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    environments = [
        row
        for row in cast(list[dict[str, Any]], snapshot["environments"])
        if row["env_id"] == request["env_id"]
        and row["principal_id"] == request["principal_id"]
        and row["runtime_id"] == request["runtime_id"]
    ]
    deployments = [
        row
        for row in cast(list[dict[str, Any]], snapshot["deployments"])
        if row["deployment_id"] == request["deployment_id"]
        and row["env_id"] == request["env_id"]
        and row["principal_id"] == request["principal_id"]
        and row["candidate_id"] == request["candidate_id"]
    ]
    candidates = [
        row
        for row in cast(list[dict[str, Any]], snapshot["candidates"])
        if row["candidate_id"] == request["candidate_id"]
        and row["env_id"] == request["env_id"]
        and row["principal_id"] == request["principal_id"]
        and row["candidate_sha"] == request["candidate_sha"]
        and row["candidate_tree"] == request["candidate_tree"]
    ]
    if (
        snapshot.get("generation") != request["registry_generation"]
        or snapshot.get("payload_sha256") != request["registry_snapshot_sha256"]
        or len(environments) != 1
        or len(deployments) != 1
        or len(candidates) != 1
    ):
        raise AcceptanceProbeError("acceptance probe registry binding is stale")
    environment = environments[0]
    deployment = deployments[0]
    candidate = candidates[0]
    if (
        environment["state"] != "deploying"
        or environment["resource_generation"] != deployment["expected_resource_generation"]
        or deployment["phase"] != "verified"
        or deployment.get("applied_resource_generation")
        != deployment["expected_resource_generation"] + 1
        or request["resource_generation"] != deployment["applied_resource_generation"]
        or type(deployment.get("applied_registry_generation")) is not int
        or deployment["applied_registry_generation"] < 1
        or DIGEST_RE.fullmatch(str(deployment.get("applied_registry_payload_sha256"))) is None
        or deployment.get("finalization_payload_sha256") is not None
    ):
        raise AcceptanceProbeError("acceptance probe finalization intent is invalid")
    return environment, deployment, candidate


def _job_name(request: Mapping[str, Any]) -> str:
    suffix = hashlib.sha256(f"{request['deployment_id']}:acceptance".encode("ascii")).hexdigest()[
        :12
    ]
    return f"loom-env-{request['runtime_id']}-finalize-{suffix}"


def _domain_request(
    route: DomainRoute,
    request: Mapping[str, Any],
    environment: Mapping[str, Any],
) -> dict[str, Any]:
    unsigned = {
        "schema_version": 1,
        "kind": DOMAIN_REQUEST_KIND,
        "action": TRANSPORT_ACTION,
        "domain": route.domain,
        "cluster": route.cluster,
        "submit_host": route.submit_host,
        "controller": route.controller,
        "deployment_id": request["deployment_id"],
        "env_id": request["env_id"],
        "principal_id": request["principal_id"],
        "runtime_id": request["runtime_id"],
        "candidate_id": request["candidate_id"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "applied_resource_generation": request["resource_generation"],
        "registry_generation": request["registry_generation"],
        "registry_snapshot_sha256": request["registry_snapshot_sha256"],
        "service_user": environment["slurm_user"],
        "slurm_account": environment["slurm_account"],
        "slurm_qos": environment["slurm_qos"],
        "job_name": _job_name(request),
        "time_limit_seconds": PROBE_TIME_LIMIT_SECONDS,
        "health_services": list(SERVICE_NAMES),
        "general_admission_authorized": False,
        "foreign_job_action": "observe-only",
        "idempotency_key": hashlib.sha256(
            (f"{request['deployment_id']}:{route.domain}:{request['resource_generation']}").encode(
                "ascii"
            )
        ).hexdigest(),
    }
    return {**unsigned, "payload_sha256": _digest(unsigned)}


def _transport_envelope(
    route: DomainRoute,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    payload = _canonical(request)
    body = {
        "schema_version": 1,
        "action": TRANSPORT_ACTION,
        "node": route.transport_node,
        "domain": route.domain,
        "sandbox": request["runtime_id"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "payload_kind": "developer-environment-acceptance-probe-json",
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "prior_request_id": None,
    }
    return {
        **body,
        "request_id": hashlib.sha256(_canonical(body)).hexdigest(),
    }


def _invoke_transport(
    route: DomainRoute,
    envelope: Mapping[str, Any],
    *,
    transport_program: Path,
) -> dict[str, Any]:
    raw_request = _canonical(envelope)
    try:
        completed = subprocess.run(
            (
                str(transport_program),
                "invoke",
                "--node",
                route.transport_node,
                "--verb",
                "transact",
            ),
            input=raw_request,
            check=False,
            capture_output=True,
            timeout=PROBE_TIME_LIMIT_SECONDS + 120,
            env={
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C.UTF-8",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise AcceptanceProbeError(
            f"{route.domain} acceptance probe authority is unavailable"
        ) from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > MAX_FILE_BYTES:
        raise AcceptanceProbeError(f"{route.domain} acceptance probe failed safely")
    try:
        response = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AcceptanceProbeError(f"{route.domain} acceptance probe response is invalid") from exc
    if (
        not isinstance(response, dict)
        or completed.stdout != _canonical(response)
        or set(response) != TRANSPORT_RESPONSE_FIELDS
        or response.get("schema_version") != 1
        or response.get("request_id") != envelope["request_id"]
        or response.get("status") != "succeeded"
        or response.get("action") != TRANSPORT_ACTION
        or response.get("node") != route.transport_node
        or response.get("domain") != route.domain
        or response.get("sandbox") != envelope["sandbox"]
        or response.get("candidate_sha") != envelope["candidate_sha"]
        or response.get("candidate_tree") != envelope["candidate_tree"]
        or response.get("payload_sha256") != envelope["payload_sha256"]
        or not isinstance(response.get("result"), dict)
        or response.get("result_sha256") != _digest(cast(dict[str, Any], response["result"]))
        or not _valid_timestamp(response.get("completed_at"))
    ):
        raise AcceptanceProbeError(f"{route.domain} acceptance probe response binding is invalid")
    return cast(dict[str, Any], response["result"])


def _validate_domain_receipt(
    receipt: Mapping[str, Any],
    *,
    route: DomainRoute,
    request: Mapping[str, Any],
    environment: Mapping[str, Any],
    domain_request: Mapping[str, Any],
    transport_request_id: str,
) -> dict[str, Any]:
    unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
    exact = {
        "action": ACTION,
        "domain": route.domain,
        "cluster": route.cluster,
        "submit_host": route.submit_host,
        "controller": route.controller,
        "deployment_id": request["deployment_id"],
        "env_id": request["env_id"],
        "principal_id": request["principal_id"],
        "runtime_id": request["runtime_id"],
        "candidate_id": request["candidate_id"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "applied_resource_generation": request["resource_generation"],
        "registry_generation": request["registry_generation"],
        "registry_snapshot_sha256": request["registry_snapshot_sha256"],
        "probe_request_sha256": domain_request["payload_sha256"],
        "transport_request_id": transport_request_id,
    }
    job = receipt.get("job")
    terminal = receipt.get("terminal")
    health = receipt.get("health")
    if (
        set(receipt) != DOMAIN_RECEIPT_FIELDS
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != DOMAIN_RECEIPT_KIND
        or receipt.get("status") != "passed"
        or any(receipt.get(field) != value for field, value in exact.items())
        or receipt.get("submission_count") != 1
        or not isinstance(job, dict)
        or set(job) != JOB_FIELDS
        or JOB_ID_RE.fullmatch(str(job.get("job_id"))) is None
        or JOB_NAME_RE.fullmatch(str(job.get("job_name"))) is None
        or job.get("job_name") != domain_request["job_name"]
        or job.get("user") != environment["slurm_user"]
        or job.get("account") != environment["slurm_account"]
        or job.get("qos") != environment["slurm_qos"]
        or job.get("submit_host") != route.submit_host
        or job.get("controller") != route.controller
        or not isinstance(job.get("allocation_nodes"), list)
        or not job["allocation_nodes"]
        or any(node not in route.allowed_nodes for node in job["allocation_nodes"])
        or len(job["allocation_nodes"]) != len(set(job["allocation_nodes"]))
        or job.get("time_limit_seconds") != PROBE_TIME_LIMIT_SECONDS
        or not isinstance(health, dict)
        or set(health) != set(SERVICE_NAMES)
        or not isinstance(terminal, dict)
        or set(terminal) != TERMINAL_FIELDS
        or terminal
        != {
            "state": TERMINAL_STATE,
            "exit_code": EXIT_CODE,
            "natural_exit": True,
            "cancel_requested": False,
            "timed_out": False,
        }
        or any(
            not isinstance(health[name], dict)
            or set(health[name]) != HEALTH_FIELDS
            or health[name].get("service") != name
            or health[name].get("status") != "healthy"
            or health[name].get("http_status") != 200
            or DIGEST_RE.fullmatch(str(health[name].get("candidate_binding_sha256"))) is None
            or DIGEST_RE.fullmatch(str(health[name].get("response_sha256"))) is None
            for name in SERVICE_NAMES
        )
        or DIGEST_RE.fullmatch(str(receipt.get("job_output_sha256"))) is None
        or DIGEST_RE.fullmatch(str(receipt.get("authority_receipt_sha256"))) is None
        or not _valid_timestamp(receipt.get("completed_at"))
        or receipt.get("payload_sha256") != _digest(unsigned)
    ):
        raise AcceptanceProbeError(f"{route.domain} acceptance probe receipt is invalid")
    return dict(receipt)


def _domain_receipt(
    route: DomainRoute,
    request: Mapping[str, Any],
    environment: Mapping[str, Any],
    *,
    receipt_root: Path,
    transport_program: Path,
    require_root_ownership: bool,
) -> dict[str, Any]:
    domain_request = _domain_request(route, request, environment)
    envelope = _transport_envelope(route, domain_request)
    path = receipt_root / request["deployment_id"] / f"{route.domain}.json"
    if path.exists() or path.is_symlink():
        loaded = _load_json(
            path,
            description=f"{route.domain} acceptance probe receipt",
            require_root_ownership=require_root_ownership,
        )
        return _validate_domain_receipt(
            loaded,
            route=route,
            request=request,
            environment=environment,
            domain_request=domain_request,
            transport_request_id=cast(str, envelope["request_id"]),
        )
    result = _invoke_transport(
        route,
        envelope,
        transport_program=transport_program,
    )
    receipt = _validate_domain_receipt(
        result,
        route=route,
        request=request,
        environment=environment,
        domain_request=domain_request,
        transport_request_id=cast(str, envelope["request_id"]),
    )
    _atomic_write(
        path,
        receipt,
        require_root_ownership=require_root_ownership,
    )
    rebound = _load_json(
        path,
        description=f"{route.domain} acceptance probe receipt",
        require_root_ownership=require_root_ownership,
    )
    return _validate_domain_receipt(
        rebound,
        route=route,
        request=request,
        environment=environment,
        domain_request=domain_request,
        transport_request_id=cast(str, envelope["request_id"]),
    )


def _combined_receipt(
    request: Mapping[str, Any],
    domains: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    completed_at = max(str(receipt["completed_at"]) for receipt in domains.values())
    unsigned = {
        "schema_version": 1,
        "kind": COMBINED_RECEIPT_KIND,
        "status": "passed",
        "action": ACTION,
        "deployment_id": request["deployment_id"],
        "env_id": request["env_id"],
        "principal_id": request["principal_id"],
        "runtime_id": request["runtime_id"],
        "candidate_id": request["candidate_id"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "applied_resource_generation": request["resource_generation"],
        "registry_generation": request["registry_generation"],
        "registry_snapshot_sha256": request["registry_snapshot_sha256"],
        "runtime_request_sha256": request["payload_sha256"],
        "domains": {domain: dict(receipt) for domain, receipt in sorted(domains.items())},
        "completed_at": completed_at,
    }
    return {**unsigned, "payload_sha256": _digest(unsigned)}


def _validate_combined_receipt(
    receipt: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    environment: Mapping[str, Any],
) -> dict[str, Any]:
    domains = receipt.get("domains")
    unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
    exact = {
        "deployment_id": request["deployment_id"],
        "env_id": request["env_id"],
        "principal_id": request["principal_id"],
        "runtime_id": request["runtime_id"],
        "candidate_id": request["candidate_id"],
        "candidate_sha": request["candidate_sha"],
        "candidate_tree": request["candidate_tree"],
        "applied_resource_generation": request["resource_generation"],
        "registry_generation": request["registry_generation"],
        "registry_snapshot_sha256": request["registry_snapshot_sha256"],
        "runtime_request_sha256": request["payload_sha256"],
    }
    if (
        set(receipt) != COMBINED_FIELDS
        or receipt.get("schema_version") != 1
        or receipt.get("kind") != COMBINED_RECEIPT_KIND
        or receipt.get("status") != "passed"
        or receipt.get("action") != ACTION
        or any(receipt.get(field) != value for field, value in exact.items())
        or not isinstance(domains, dict)
        or set(domains) != set(ROUTES)
        or not _valid_timestamp(receipt.get("completed_at"))
        or receipt.get("payload_sha256") != _digest(unsigned)
    ):
        raise AcceptanceProbeError("combined acceptance probe receipt is invalid")
    if any(
        not isinstance(domains[domain], dict)
        or not _valid_timestamp(domains[domain].get("completed_at"))
        for domain in ROUTES
    ):
        raise AcceptanceProbeError("combined acceptance probe receipt is invalid")
    if receipt.get("completed_at") != max(
        str(domains[domain]["completed_at"]) for domain in ROUTES
    ):
        raise AcceptanceProbeError("combined acceptance probe receipt is invalid")
    validated = {
        domain: _validate_domain_receipt(
            cast(dict[str, Any], domains[domain]),
            route=ROUTES[domain],
            request=request,
            environment=environment,
            domain_request=_domain_request(ROUTES[domain], request, environment),
            transport_request_id=cast(
                str,
                _transport_envelope(
                    ROUTES[domain],
                    _domain_request(ROUTES[domain], request, environment),
                )["request_id"],
            ),
        )
        for domain in ROUTES
    }
    if validated != domains:
        raise AcceptanceProbeError("combined acceptance probe receipt drifted")
    return dict(receipt)


def execute(
    deployment_id: str,
    *,
    runtime_root: Path = RUNTIME_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT,
    transport_program: Path = NODE_TRANSPORT,
    require_root_ownership: bool = True,
) -> dict[str, Any]:
    """Converge exactly one receipt per fixed domain and one combined receipt."""

    if require_root_ownership and os.geteuid() != 0:
        raise AcceptanceProbeError("developer environment acceptance probe requires root")
    if DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None:
        raise AcceptanceProbeError("acceptance probe deployment identity is invalid")
    request = _runtime_request(
        deployment_id,
        runtime_root=runtime_root,
        require_root_ownership=require_root_ownership,
    )
    snapshot = _snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    environment, _deployment, _candidate = _binding(snapshot, request)
    receipt_root = runtime_root / "acceptance-probes"
    combined_path = receipt_root / deployment_id / "combined.json"
    if combined_path.exists() or combined_path.is_symlink():
        combined = _load_json(
            combined_path,
            description="combined acceptance probe receipt",
            require_root_ownership=require_root_ownership,
        )
        return _validate_combined_receipt(
            combined,
            request=request,
            environment=environment,
        )
    domains = {
        domain: _domain_receipt(
            route,
            request,
            environment,
            receipt_root=receipt_root,
            transport_program=transport_program,
            require_root_ownership=require_root_ownership,
        )
        for domain, route in ROUTES.items()
    }
    combined = _combined_receipt(request, domains)
    _atomic_write(
        combined_path,
        combined,
        require_root_ownership=require_root_ownership,
    )
    rebound = _load_json(
        combined_path,
        description="combined acceptance probe receipt",
        require_root_ownership=require_root_ownership,
    )
    return _validate_combined_receipt(
        rebound,
        request=request,
        environment=environment,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("action", choices=(ACTION,))
    parser.add_argument("--deployment-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    receipt = execute(cast(str, args.deployment_id))
    sys.stdout.buffer.write(_canonical(receipt))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except AcceptanceProbeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        raise SystemExit(1) from None
