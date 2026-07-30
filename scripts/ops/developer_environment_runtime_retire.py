#!/usr/bin/env python3
"""Remove one quarantined developer environment from every runtime node.

This is fixed, root-installed infrastructure code.  Its public interface accepts
only registry identifiers and the digest of the global retirement WAL.  Paths,
candidate bytes, credentials, and deletion targets are derived by the remote
node authority from the verified registry snapshot and the closed request below.
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
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, Final, cast

_INSTALLED_ROOT: Final = Path("/usr/local/libexec")
_INSTALLED_REGISTRY: Final = _INSTALLED_ROOT / "scripts/ops/developer_environment_registry.py"
if __package__ in {None, ""} and _INSTALLED_REGISTRY.is_file():
    roots = (
        _INSTALLED_ROOT,
        _INSTALLED_ROOT / "scripts",
        _INSTALLED_ROOT / "scripts/ops",
    )
    try:
        metadata = tuple(path.lstat() for path in roots)
        module_metadata = _INSTALLED_REGISTRY.lstat()
    except OSError as exc:
        raise RuntimeError("installed retirement module tree is unavailable") from exc
    if (
        any(
            not stat.S_ISDIR(item.st_mode) or item.st_uid != 0 or item.st_mode & 0o022
            for item in metadata
        )
        or not stat.S_ISREG(module_metadata.st_mode)
        or stat.S_ISLNK(module_metadata.st_mode)
        or module_metadata.st_uid != 0
        or module_metadata.st_nlink != 1
        or module_metadata.st_mode & 0o022
    ):
        raise RuntimeError("installed retirement module tree is unsafe")
    sys.path.insert(0, str(_INSTALLED_ROOT))

from scripts.ops.developer_environment_registry import (  # noqa: E402
    CANDIDATE_ID_RE,
    DEPLOYMENT_ID_RE,
    DIGEST_RE,
    ENV_ID_RE,
    SHA_RE,
    DeveloperEnvironmentRegistry,
    RegistryError,
)

SCHEMA_VERSION: Final = 1
ACTION: Final = "developer-environment-runtime-retire"
PAYLOAD_KIND: Final = "developer-environment-runtime-retire-json"
REQUEST_KIND: Final = "loom.developer-environment.runtime-retire-node-request"
NODE_RECEIPT_KIND: Final = "loom.developer-environment.runtime-retire-node-receipt"
COMBINED_RECEIPT_KIND: Final = "loom.developer-environment.runtime-retire-receipt"
RETIRE_WAL_KIND: Final = "loom.developer-environment.retire-journal"

RUNTIME_ROOT: Final = Path("/var/lib/loom-developer-environment-runtime")
REGISTRY_SNAPSHOT: Final = Path(
    "/var/lib/loom-developer-environment-registry/current-snapshot.json"
)
NODE_TRANSPORT: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
MAX_FILE_BYTES: Final = 16 * 1024 * 1024
SAFE_OPERATION_RE: Final = re.compile(r"^[0-9a-f]{64}$")
SAFE_TOMBSTONE_PATH_RE: Final = re.compile(
    r"^/var/lib/loom-developer-environment-runtime-retire/tombstones/"
    r"(?:oldlab-[1-5]|trt-gb10-(?:[1-9]|1[0-5]))/"
    r"[a-z][a-z0-9-]{1,31}/[0-9a-f]{64}[.]json$"
)

NODES: Final = (
    *(f"oldlab-{index}" for index in range(1, 6)),
    *(f"trt-gb10-{index}" for index in range(1, 16)),
)
ABSENCE_FIELDS: Final = {
    "link_client_credentials",
    "tls_private_keys",
    "token_files",
    "domain_environment",
    "domain_config",
    "candidate_material",
    "active_attestation_pointers",
}
REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "action",
    "node",
    "domain",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "retire_operation_sha256",
    "current_candidate_id",
    "candidate_bindings",
    "foreign_path_action",
    "audit_action",
    "payload_sha256",
}
NODE_RECEIPT_FIELDS: Final = {
    "schema_version",
    "kind",
    "status",
    "action",
    "node",
    "domain",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "retire_operation_sha256",
    "request_sha256",
    "transport_request_id",
    "candidate_bindings",
    "absent",
    "tombstone",
    "peer_digest_before",
    "peer_digest_after",
    "foreign_path_action",
    "audit_action",
    "completed_at",
    "payload_sha256",
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
COMBINED_RECEIPT_FIELDS: Final = {
    "schema_version",
    "kind",
    "status",
    "action",
    "deployment_id",
    "env_id",
    "principal_id",
    "runtime_id",
    "resource_generation",
    "registry_generation",
    "registry_snapshot_sha256",
    "retire_operation_sha256",
    "candidate_bindings",
    "nodes",
    "completed_at",
    "payload_sha256",
}
RETIRE_WAL_FIELDS: Final = {
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
    "expected_resource_generation",
    "current_candidate_id",
    "idempotency_key",
    "evidence",
    "object_checkpoints",
    "created_at",
    "updated_at",
    "payload_sha256",
}


class RuntimeRetireError(RuntimeError):
    """A bounded error that never contains credential material."""


Transport = Callable[..., subprocess.CompletedProcess[bytes]]


def _canonical(payload: Mapping[str, Any]) -> bytes:
    try:
        return (
            json.dumps(
                dict(payload),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ).encode("ascii")
            + b"\n"
        )
    except (TypeError, UnicodeEncodeError) as exc:
        raise RuntimeRetireError("runtime retirement payload is not canonical") from exc


def _digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


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
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65_536, limit + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > limit:
                raise RuntimeRetireError(f"{description} exceeds its size bound")
            chunks.append(chunk)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except RuntimeRetireError:
        raise
    except OSError as exc:
        raise RuntimeRetireError(f"{description} is unavailable") from exc
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
            item.st_nlink,
            item.st_size,
            item.st_mtime_ns,
            item.st_ctime_ns,
        )
        for item in (lexical, opened, rebound, current)
    }
    expected_uid = 0 if require_root_ownership else os.geteuid()
    if (
        stat.S_ISLNK(lexical.st_mode)
        or not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or opened.st_uid != expected_uid
        or stat.S_IMODE(opened.st_mode) != 0o600
        or len(identities) != 1
    ):
        raise RuntimeRetireError(f"{description} metadata is unsafe")
    return b"".join(chunks)


def _load_bound_json(
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
        raise RuntimeRetireError(f"{description} is invalid") from exc
    if (
        not isinstance(payload, dict)
        or raw != _canonical(payload)
        or DIGEST_RE.fullmatch(str(payload.get("payload_sha256"))) is None
        or payload.get("payload_sha256")
        != _digest({key: value for key, value in payload.items() if key != "payload_sha256"})
    ):
        raise RuntimeRetireError(f"{description} binding is invalid")
    return payload


def _atomic_write(
    path: Path,
    payload: Mapping[str, Any],
    *,
    require_root_ownership: bool,
) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    expected_uid = 0 if require_root_ownership else os.geteuid()
    metadata = path.parent.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != expected_uid
        or metadata.st_mode & 0o077
    ):
        raise RuntimeRetireError("runtime retirement receipt directory is unsafe")
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        if require_root_ownership:
            os.fchown(descriptor, 0, 0)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical(payload))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)


def _domain(node: str) -> str:
    return "oldlab" if node.startswith("oldlab-") else "gb10"


def _timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return value.endswith("Z") and parsed.tzinfo is not None


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
        raise RuntimeRetireError("developer environment registry snapshot is invalid") from exc


def _retire_wal(
    path: Path,
    *,
    expected_digest: str,
    require_root_ownership: bool,
) -> dict[str, Any]:
    wal = _load_bound_json(
        path,
        description="developer environment retirement WAL",
        require_root_ownership=require_root_ownership,
    )
    evidence = wal.get("evidence")
    object_checkpoints = wal.get("object_checkpoints")
    if (
        set(wal) != RETIRE_WAL_FIELDS
        or wal.get("schema_version") != SCHEMA_VERSION
        or wal.get("kind") != RETIRE_WAL_KIND
        or wal.get("phase") != "capacity-retired"
        or wal.get("payload_sha256") != expected_digest
        or not isinstance(evidence, dict)
        or set(evidence) != {"admission_fence", "capacity_retire"}
        or DIGEST_RE.fullmatch(str(evidence.get("admission_fence"))) is None
        or DIGEST_RE.fullmatch(str(evidence.get("capacity_retire"))) is None
        or object_checkpoints != {}
    ):
        raise RuntimeRetireError("developer environment retirement WAL binding is invalid")
    return wal


def _candidate_binding(candidate: Mapping[str, Any]) -> dict[str, str]:
    binding = {
        "candidate_id": str(candidate.get("candidate_id")),
        "candidate_sha": str(candidate.get("candidate_sha")),
        "candidate_tree": str(candidate.get("candidate_tree")),
    }
    if (
        CANDIDATE_ID_RE.fullmatch(binding["candidate_id"]) is None
        or SHA_RE.fullmatch(binding["candidate_sha"]) is None
        or SHA_RE.fullmatch(binding["candidate_tree"]) is None
    ):
        raise RuntimeRetireError("runtime retirement candidate binding is invalid")
    return binding


def _binding(
    snapshot: Mapping[str, Any],
    *,
    env_id: str,
    deployment_id: str,
    wal: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    environments = [
        row
        for row in cast(list[dict[str, Any]], snapshot["environments"])
        if row["env_id"] == env_id
    ]
    deployments = [
        row
        for row in cast(list[dict[str, Any]], snapshot["deployments"])
        if row["env_id"] == env_id
    ]
    selected = [row for row in deployments if row["deployment_id"] == deployment_id]
    if len(environments) != 1 or len(selected) != 1:
        raise RuntimeRetireError("runtime retirement registry binding is unavailable")
    environment = environments[0]
    deployment = selected[0]
    current_candidate_id = environment["current_candidate_id"]
    if (
        environment["state"] != "quarantined"
        or current_candidate_id is None
        or deployment["phase"] != "committed"
        or deployment["candidate_id"] != current_candidate_id
        or deployment["applied_resource_generation"] != environment["resource_generation"]
        or wal.get("env_id") != env_id
        or wal.get("principal_id") != environment["principal_id"]
        or wal.get("runtime_id") != environment["runtime_id"]
        or wal.get("uid") != environment["uid"]
        or wal.get("gid") != environment["gid"]
        or wal.get("service_user") != environment["service_user"]
        or wal.get("service_group") != environment["service_group"]
        or wal.get("slurm_user") != environment["slurm_user"]
        or wal.get("slurm_account") != environment["slurm_account"]
        or wal.get("slurm_qos") != environment["slurm_qos"]
        or wal.get("expected_resource_generation") != environment["resource_generation"]
        or wal.get("current_candidate_id") != current_candidate_id
    ):
        raise RuntimeRetireError("runtime retirement registry intent is invalid")
    candidate_ids = {
        str(current_candidate_id),
        *(str(row["candidate_id"]) for row in deployments if row["phase"] == "failed"),
    }
    candidates_by_id = {
        str(row["candidate_id"]): row
        for row in cast(list[dict[str, Any]], snapshot["candidates"])
        if row["env_id"] == env_id
        and row["principal_id"] == environment["principal_id"]
        and str(row["candidate_id"]) in candidate_ids
    }
    if set(candidates_by_id) != candidate_ids:
        raise RuntimeRetireError("runtime retirement candidate set is incomplete")
    bindings = sorted(
        (_candidate_binding(candidates_by_id[candidate_id]) for candidate_id in candidate_ids),
        key=lambda item: item["candidate_id"],
    )
    return environment, bindings


def _node_request(
    node: str,
    *,
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
    deployment_id: str,
    retire_operation_sha256: str,
    candidates: list[dict[str, str]],
) -> dict[str, Any]:
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "kind": REQUEST_KIND,
        "action": ACTION,
        "node": node,
        "domain": _domain(node),
        "deployment_id": deployment_id,
        "env_id": environment["env_id"],
        "principal_id": environment["principal_id"],
        "runtime_id": environment["runtime_id"],
        "resource_generation": environment["resource_generation"],
        "registry_generation": snapshot["generation"],
        "registry_snapshot_sha256": snapshot["payload_sha256"],
        "retire_operation_sha256": retire_operation_sha256,
        "current_candidate_id": environment["current_candidate_id"],
        "candidate_bindings": candidates,
        "foreign_path_action": "preserve",
        "audit_action": "append-only-preserve",
    }
    return {**unsigned, "payload_sha256": _digest(unsigned)}


def _envelope(request: Mapping[str, Any]) -> dict[str, Any]:
    payload = _canonical(request)
    current = next(
        item
        for item in cast(list[dict[str, str]], request["candidate_bindings"])
        if item["candidate_id"] == request["current_candidate_id"]
    )
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "action": ACTION,
        "node": request["node"],
        "domain": request["domain"],
        "sandbox": request["runtime_id"],
        "candidate_sha": current["candidate_sha"],
        "candidate_tree": current["candidate_tree"],
        "payload_kind": PAYLOAD_KIND,
        "payload_sha256": hashlib.sha256(payload).hexdigest(),
        "payload_base64": base64.b64encode(payload).decode("ascii"),
        "prior_request_id": None,
    }
    return {**unsigned, "request_id": hashlib.sha256(_canonical(unsigned)).hexdigest()}


def _validate_node_receipt(
    receipt: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    transport_request_id: str,
) -> dict[str, Any]:
    unsigned = {key: value for key, value in receipt.items() if key != "payload_sha256"}
    exact = {
        field: request[field]
        for field in (
            "node",
            "domain",
            "deployment_id",
            "env_id",
            "principal_id",
            "runtime_id",
            "resource_generation",
            "registry_generation",
            "registry_snapshot_sha256",
            "retire_operation_sha256",
            "candidate_bindings",
            "foreign_path_action",
            "audit_action",
        )
    }
    absent = receipt.get("absent")
    tombstone = receipt.get("tombstone")
    if (
        set(receipt) != NODE_RECEIPT_FIELDS
        or receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("kind") != NODE_RECEIPT_KIND
        or receipt.get("status") != "cleaned"
        or receipt.get("action") != ACTION
        or any(receipt.get(field) != value for field, value in exact.items())
        or receipt.get("request_sha256") != request["payload_sha256"]
        or receipt.get("transport_request_id") != transport_request_id
        or not isinstance(absent, dict)
        or set(absent) != ABSENCE_FIELDS
        or any(value is not True for value in absent.values())
        or not isinstance(tombstone, dict)
        or set(tombstone) != {"path", "payload_sha256", "persisted"}
        or SAFE_TOMBSTONE_PATH_RE.fullmatch(str(tombstone.get("path"))) is None
        or f"/{request['node']}/{request['runtime_id']}/" not in str(tombstone.get("path"))
        or not str(tombstone.get("path")).endswith(f"/{request['retire_operation_sha256']}.json")
        or DIGEST_RE.fullmatch(str(tombstone.get("payload_sha256"))) is None
        or tombstone.get("persisted") is not True
        or DIGEST_RE.fullmatch(str(receipt.get("peer_digest_before"))) is None
        or receipt.get("peer_digest_after") != receipt.get("peer_digest_before")
        or not _timestamp(receipt.get("completed_at"))
        or receipt.get("payload_sha256") != _digest(unsigned)
    ):
        raise RuntimeRetireError(f"{request['node']} runtime retirement receipt binding is invalid")
    return dict(receipt)


def _invoke(
    request: Mapping[str, Any],
    envelope: Mapping[str, Any],
    *,
    transport_program: Path,
    transport: Transport,
) -> dict[str, Any]:
    try:
        completed = transport(
            (
                str(transport_program),
                "invoke",
                "--node",
                str(request["node"]),
                "--verb",
                "transact",
            ),
            input=_canonical(envelope),
            check=False,
            capture_output=True,
            timeout=180,
            env={
                "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
                "LANG": "C.UTF-8",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeRetireError(
            f"{request['node']} runtime retirement authority is unavailable"
        ) from exc
    if completed.returncode != 0 or completed.stderr or len(completed.stdout) > MAX_FILE_BYTES:
        raise RuntimeRetireError(f"{request['node']} runtime retirement failed safely")
    try:
        response = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeRetireError(
            f"{request['node']} runtime retirement response is invalid"
        ) from exc
    current = next(
        item
        for item in cast(list[dict[str, str]], request["candidate_bindings"])
        if item["candidate_id"] == request["current_candidate_id"]
    )
    if (
        not isinstance(response, dict)
        or completed.stdout != _canonical(response)
        or set(response) != TRANSPORT_RESPONSE_FIELDS
        or response.get("schema_version") != SCHEMA_VERSION
        or response.get("request_id") != envelope["request_id"]
        or response.get("status") != "succeeded"
        or response.get("action") != ACTION
        or response.get("node") != request["node"]
        or response.get("domain") != request["domain"]
        or response.get("sandbox") != request["runtime_id"]
        or response.get("candidate_sha") != current["candidate_sha"]
        or response.get("candidate_tree") != current["candidate_tree"]
        or response.get("payload_sha256") != envelope["payload_sha256"]
        or not isinstance(response.get("result"), dict)
        or response.get("result_sha256") != _digest(cast(dict[str, Any], response["result"]))
        or not _timestamp(response.get("completed_at"))
    ):
        raise RuntimeRetireError(
            f"{request['node']} runtime retirement response binding is invalid"
        )
    return _validate_node_receipt(
        cast(dict[str, Any], response["result"]),
        request=request,
        transport_request_id=cast(str, envelope["request_id"]),
    )


def _combined(
    *,
    snapshot: Mapping[str, Any],
    environment: Mapping[str, Any],
    deployment_id: str,
    retire_operation_sha256: str,
    candidates: list[dict[str, str]],
    nodes: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any]:
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "kind": COMBINED_RECEIPT_KIND,
        "status": "cleaned",
        "action": ACTION,
        "deployment_id": deployment_id,
        "env_id": environment["env_id"],
        "principal_id": environment["principal_id"],
        "runtime_id": environment["runtime_id"],
        "resource_generation": environment["resource_generation"],
        "registry_generation": snapshot["generation"],
        "registry_snapshot_sha256": snapshot["payload_sha256"],
        "retire_operation_sha256": retire_operation_sha256,
        "candidate_bindings": candidates,
        "nodes": {node: cast(str, nodes[node]["payload_sha256"]) for node in NODES},
        "completed_at": max(str(nodes[node]["completed_at"]) for node in NODES),
    }
    return {**unsigned, "payload_sha256": _digest(unsigned)}


def execute(
    deployment_id: str,
    env_id: str,
    retire_operation_sha256: str,
    *,
    runtime_root: Path = RUNTIME_ROOT,
    registry_snapshot: Path = REGISTRY_SNAPSHOT,
    transport_program: Path = NODE_TRANSPORT,
    transport: Transport = subprocess.run,
    require_root_ownership: bool = True,
) -> dict[str, Any]:
    """Converge one exact node receipt per fleet member and a combined receipt."""

    if require_root_ownership and os.geteuid() != 0:
        raise RuntimeRetireError("developer environment runtime retirement requires root")
    if (
        DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None
        or ENV_ID_RE.fullmatch(env_id) is None
        or SAFE_OPERATION_RE.fullmatch(retire_operation_sha256) is None
    ):
        raise RuntimeRetireError("runtime retirement operation identity is invalid")
    snapshot = _snapshot(
        registry_snapshot,
        require_root_ownership=require_root_ownership,
    )
    wal = _retire_wal(
        runtime_root / "lifecycle/retire" / f"{env_id}.json",
        expected_digest=retire_operation_sha256,
        require_root_ownership=require_root_ownership,
    )
    environment, candidates = _binding(
        snapshot,
        env_id=env_id,
        deployment_id=deployment_id,
        wal=wal,
    )
    receipt_root = runtime_root / "runtime-retire" / env_id / retire_operation_sha256
    combined_path = receipt_root / "combined.json"
    node_receipts: dict[str, dict[str, Any]] = {}
    for node in NODES:
        request = _node_request(
            node,
            snapshot=snapshot,
            environment=environment,
            deployment_id=deployment_id,
            retire_operation_sha256=retire_operation_sha256,
            candidates=candidates,
        )
        envelope = _envelope(request)
        path = receipt_root / f"{node}.json"
        if path.exists() or path.is_symlink():
            existing = _load_bound_json(
                path,
                description=f"{node} runtime retirement receipt",
                require_root_ownership=require_root_ownership,
            )
            node_receipts[node] = _validate_node_receipt(
                existing,
                request=request,
                transport_request_id=cast(str, envelope["request_id"]),
            )
            continue
        receipt = _invoke(
            request,
            envelope,
            transport_program=transport_program,
            transport=transport,
        )
        _atomic_write(
            path,
            receipt,
            require_root_ownership=require_root_ownership,
        )
        node_receipts[node] = _validate_node_receipt(
            _load_bound_json(
                path,
                description=f"{node} runtime retirement receipt",
                require_root_ownership=require_root_ownership,
            ),
            request=request,
            transport_request_id=cast(str, envelope["request_id"]),
        )
    if set(node_receipts) != set(NODES):
        raise RuntimeRetireError("runtime retirement node receipt set is incomplete")
    combined = _combined(
        snapshot=snapshot,
        environment=environment,
        deployment_id=deployment_id,
        retire_operation_sha256=retire_operation_sha256,
        candidates=candidates,
        nodes=node_receipts,
    )
    if combined_path.exists() or combined_path.is_symlink():
        existing = _load_bound_json(
            combined_path,
            description="combined runtime retirement receipt",
            require_root_ownership=require_root_ownership,
        )
        if existing != combined:
            raise RuntimeRetireError("combined runtime retirement receipt drifted")
        return existing
    _atomic_write(
        combined_path,
        combined,
        require_root_ownership=require_root_ownership,
    )
    rebound = _load_bound_json(
        combined_path,
        description="combined runtime retirement receipt",
        require_root_ownership=require_root_ownership,
    )
    if set(rebound) != COMBINED_RECEIPT_FIELDS or rebound != combined:
        raise RuntimeRetireError("combined runtime retirement receipt binding is invalid")
    return rebound


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("action", choices=("cleanup",))
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--env-id", required=True)
    parser.add_argument("--retire-operation-sha256", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    receipt = execute(
        cast(str, args.deployment_id),
        cast(str, args.env_id),
        cast(str, args.retire_operation_sha256),
    )
    sys.stdout.buffer.write(_canonical(receipt))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeRetireError as exc:
        sys.stderr.write(f"error: {exc}\n")
        raise SystemExit(1) from None
