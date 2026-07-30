#!/usr/bin/python3 -I
"""Root-authoritative registry for dynamically allocated developer environments.

The public request schemas contain identity and content bindings only.  Runtime
paths, numeric identities, ports, service names, storage namespaces, Slurm
identities, and cgroups are allocated by this module and cannot be supplied by
the caller.  A separately trusted seed importer preserves the three legacy
environments during migration.
"""

from __future__ import annotations

import argparse
import fcntl
import grp
import gzip
import hashlib
import json
import os
import pwd
import re
import secrets
import socket
import sqlite3
import stat
import struct
import sys
import tarfile
import tempfile
import threading
import tomllib
from collections.abc import Callable, Iterator, Mapping, Sequence, Set
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Final, cast

SCHEMA_VERSION: Final = 1
DATABASE_SCHEMA_VERSION: Final = 3
WORKER_IMAGE_BINDING_KIND: Final = "docker-archive-identities"
WORKER_IMAGE_BINDING_VERSION: Final = 2
SYSTEM_ROOT: Final = Path("/var/lib/loom-developer-environment-registry")
SYSTEM_DATABASE: Final = SYSTEM_ROOT / "registry.sqlite3"
SYSTEM_CANDIDATE_ROOT: Final = SYSTEM_ROOT / "candidates"
SYSTEM_SNAPSHOT: Final = SYSTEM_ROOT / "current-snapshot.json"
CURRENT_SNAPSHOT_PATH: Final = SYSTEM_SNAPSHOT
SYSTEM_FLEET_IDENTITY_INVENTORY: Final = SYSTEM_ROOT / "fleet-identity-inventory.json"
SYSTEM_SEED: Final = Path("/usr/local/share/loom/developer-environment-registry-seed.toml")
EXPECTED_ALLOCATOR_HOST: Final = "trt-eai-oldlab-2"
PROC_NET_TCP: Final = (Path("/proc/net/tcp"), Path("/proc/net/tcp6"))
DOCKER_SOCKET: Final = Path("/var/run/docker.sock")
DOCKER_API_MAX_BYTES: Final = 4 << 20

REGISTER_KIND: Final = "loom.developer-environment.register"
CANDIDATE_KIND: Final = "loom.developer-environment.candidate-import"
DEPLOY_KIND: Final = "loom.developer-environment.deploy"
SNAPSHOT_KIND: Final = "loom.developer-environment.registry-snapshot"
SEED_KIND: Final = "loom.developer-environment.legacy-seed"
FLEET_IDENTITY_INVENTORY_KIND: Final = "loom.developer-environment.fleet-identity-inventory"
NODE_IDENTITY_INVENTORY_KIND: Final = "loom.developer-environment.identity-inventory-result"
FLEET_IDENTITY_MAX_AGE_SECONDS: Final = 300
FLEET_IDENTITY_MAX_NODE_SKEW_SECONDS: Final = 120
FLEET_NODES: Final = (
    *(f"oldlab-{index}" for index in range(1, 6)),
    *(f"trt-gb10-{index}" for index in range(1, 16)),
)

PORT_NAMES: Final = (
    "postgres",
    "minio",
    "minio_console",
    "control_plane",
    "loom_service",
    "llm_gateway",
    "egress_xds",
    "egress_proxy",
    "egress_admin",
    "web",
    "relay_control_plane",
    "relay_gateway",
    "relay_minio",
)
PORT_OFFSETS: Final = {name: index for index, name in enumerate(PORT_NAMES)}
DEPLOY_PHASES: Final = (
    "requested",
    "resources-verified",
    "candidate-materialized",
    "services-prepared",
    "capacity-ready",
    "verified",
    "committed",
)

REGISTER_FIELDS: Final = {
    "schema_version",
    "kind",
    "principal_id",
    "idempotency_key",
    "display_name",
}
CANDIDATE_FIELDS: Final = {
    "schema_version",
    "kind",
    "principal_id",
    "idempotency_key",
    "env_id",
    "candidate_sha",
    "candidate_tree",
    "bundle_sha256",
    "bundle_size",
    "image_digests",
    "image_archives",
}
DEPLOY_FIELDS: Final = {
    "schema_version",
    "kind",
    "principal_id",
    "idempotency_key",
    "env_id",
    "candidate_id",
    "expected_resource_generation",
}

PRINCIPAL_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9:._@/+%-]{1,254}$")
IDEMPOTENCY_RE: Final = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{15,127}$")
ENV_ID_RE: Final = re.compile(r"^denv-[a-z0-9-]{8,64}$")
RUNTIME_ID_RE: Final = re.compile(r"^(?:[a-z][a-z0-9-]{1,31})$")
CANDIDATE_ID_RE: Final = re.compile(r"^cand-[0-9a-f]{40}$")
DEPLOYMENT_ID_RE: Final = re.compile(r"^dep-[0-9a-f]{32}$")
SHA_RE: Final = re.compile(r"^[0-9a-f]{40}$")
DIGEST_RE: Final = re.compile(r"^[0-9a-f]{64}$")
IMAGE_DIGEST_RE: Final = re.compile(r"^sha256:[0-9a-f]{64}$")
WORKER_IMAGE_ARCHITECTURES: Final = {"amd64": "amd64", "arm64": "arm64"}
WORKER_IMAGE_ARCHIVE_BINDING_FIELDS: Final = {
    "sha256",
    "size",
    "config_digest",
    "index_digest",
    "manifest_digest",
    "manifest_media_type",
    "load_descriptor_digest",
    "load_descriptor_media_type",
}
WORKER_RUNTIME_BINDING_DOMAINS: Final = {
    "oldlab": "amd64",
    "gb10": "arm64",
}
WORKER_RUNTIME_BACKENDS: Final = {
    "classic-overlay2": "overlay2",
    "containerd-snapshotter-v1": "overlayfs",
}
WORKER_IMAGE_COMMAND: Final = ["python", "-m", "loom_worker"]
WORKER_IMAGE_REVISION_LABEL: Final = "org.opencontainers.image.revision"
MAX_WORKER_IMAGE_ARCHIVE_BYTES: Final = 16 * 1024 * 1024 * 1024
MAX_WORKER_IMAGE_CONFIG_BYTES: Final = 16 * 1024 * 1024
MAX_WORKER_IMAGE_LAYER_UNCOMPRESSED_BYTES: Final = 32 * 1024 * 1024 * 1024
MAX_WORKER_IMAGE_TOTAL_UNCOMPRESSED_BYTES: Final = 64 * 1024 * 1024 * 1024
MAX_WORKER_IMAGE_ARCHIVE_MEMBERS: Final = 100_000
OCI_MANIFEST_MEDIA_TYPES: Final = frozenset(
    {
        "application/vnd.oci.image.manifest.v1+json",
        "application/vnd.docker.distribution.manifest.v2+json",
    }
)
OCI_INDEX_MEDIA_TYPES: Final = frozenset(
    {
        "application/vnd.oci.image.index.v1+json",
        "application/vnd.docker.distribution.manifest.list.v2+json",
    }
)
OCI_CONFIG_MEDIA_TYPES: Final = frozenset(
    {
        "application/vnd.oci.image.config.v1+json",
        "application/vnd.docker.container.image.v1+json",
    }
)
OCI_LAYER_MEDIA_TYPES: Final = frozenset(
    {
        "application/vnd.oci.image.layer.v1.tar+gzip",
        "application/vnd.docker.image.rootfs.diff.tar.gzip",
    }
)
SAFE_NAME_RE: Final = re.compile(r"^[a-z][a-z0-9_-]{1,62}$")
SAFE_BUCKET_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
SNAPSHOT_PROCESS_LOCK: Final = threading.Lock()

ENVIRONMENT_SNAPSHOT_FIELDS: Final = {
    "env_id",
    "principal_id",
    "display_name",
    "layout_version",
    "runtime_id",
    "state",
    "resource_generation",
    "lifecycle_epoch",
    "service_user",
    "service_group",
    "uid",
    "gid",
    "ports",
    "compose_project",
    "systemd_instance",
    "candidate_root",
    "runtime_root",
    "state_root",
    "evidence_root",
    "database_name",
    "postgres_volume",
    "minio_volume",
    "task_bucket",
    "trajectories_bucket",
    "artifacts_bucket",
    "provider_namespace",
    "slurm_user",
    "slurm_account",
    "slurm_qos",
    "cgroup_slice",
    "current_candidate_id",
    "created_at",
}
CANDIDATE_SNAPSHOT_FIELDS: Final = {
    "candidate_id",
    "principal_id",
    "env_id",
    "lifecycle_epoch",
    "repository_id",
    "image_binding_kind",
    "image_binding_version",
    "candidate_sha",
    "candidate_tree",
    "bundle_sha256",
    "bundle_size",
    "bundle_path",
    "image_digests",
    "image_archives",
    "imported_at",
}
DEPLOYMENT_SNAPSHOT_FIELDS: Final = {
    "deployment_id",
    "principal_id",
    "env_id",
    "candidate_id",
    "expected_resource_generation",
    "applied_resource_generation",
    "applied_registry_generation",
    "applied_registry_payload_sha256",
    "finalization_payload_sha256",
    "worker_runtime_bindings",
    "phase",
    "previous_candidate_id",
    "request_digest",
    "created_at",
    "updated_at",
}
FINALIZATION_SNAPSHOT_FIELDS: Final = {
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
UNIQUE_ENVIRONMENT_STRING_FIELDS: Final = (
    "runtime_id",
    "service_user",
    "service_group",
    "compose_project",
    "systemd_instance",
    "candidate_root",
    "runtime_root",
    "state_root",
    "evidence_root",
    "database_name",
    "postgres_volume",
    "minio_volume",
    "task_bucket",
    "trajectories_bucket",
    "artifacts_bucket",
    "provider_namespace",
    "slurm_user",
    "slurm_account",
    "slurm_qos",
    "cgroup_slice",
)
FLEET_IDENTITY_INVENTORY_FIELDS: Final = {
    "schema_version",
    "kind",
    "registry_generation",
    "registry_payload_sha256",
    "uid_start",
    "uid_end",
    "collected_at",
    "expires_at",
    "node_set_sha256",
    "nodes",
    "payload_sha256",
}
NODE_IDENTITY_INVENTORY_FIELDS: Final = {
    "schema_version",
    "kind",
    "node",
    "domain",
    "uid_start",
    "uid_end",
    "occupied_ids",
    "identity_inventory_sha256",
    "checked_at",
}


class RegistryError(RuntimeError):
    """A bounded, secret-safe registry failure."""


def _valid_worker_image_ids(value: object) -> bool:
    """Validate the closed per-architecture loom-worker config-ID contract."""

    return bool(
        isinstance(value, dict)
        and set(value) == {"amd64", "arm64"}
        and all(
            isinstance(image_id, str) and IMAGE_DIGEST_RE.fullmatch(image_id) is not None
            for image_id in value.values()
        )
        # A Docker image config records its target architecture, so one config
        # object cannot truthfully represent both supported platforms.
        and value["amd64"] != value["arm64"]
    )


def _valid_worker_image_archives(
    value: object,
    *,
    candidate_root: Path | None = None,
    candidate_id: str | None = None,
) -> bool:
    if not isinstance(value, dict) or set(value) != {"amd64", "arm64"}:
        return False
    for architecture, item in value.items():
        if (
            architecture not in WORKER_IMAGE_ARCHITECTURES
            or not isinstance(item, dict)
            or set(item)
            != (
                WORKER_IMAGE_ARCHIVE_BINDING_FIELDS
                if candidate_root is None
                else WORKER_IMAGE_ARCHIVE_BINDING_FIELDS | {"path"}
            )
            or DIGEST_RE.fullmatch(str(item.get("sha256"))) is None
            or not isinstance(item.get("size"), int)
            or isinstance(item.get("size"), bool)
            or not 0 < int(item["size"]) <= MAX_WORKER_IMAGE_ARCHIVE_BYTES
            or IMAGE_DIGEST_RE.fullmatch(str(item.get("config_digest"))) is None
            or IMAGE_DIGEST_RE.fullmatch(str(item.get("index_digest"))) is None
            or IMAGE_DIGEST_RE.fullmatch(str(item.get("manifest_digest"))) is None
            or item.get("manifest_media_type") not in OCI_MANIFEST_MEDIA_TYPES
            or IMAGE_DIGEST_RE.fullmatch(str(item.get("load_descriptor_digest"))) is None
            or item.get("load_descriptor_media_type")
            not in OCI_MANIFEST_MEDIA_TYPES | OCI_INDEX_MEDIA_TYPES
        ):
            return False
        if candidate_root is not None and (
            candidate_id is None
            or item.get("path")
            != str(worker_image_archive_path(candidate_root, candidate_id, architecture))
        ):
            return False
    return True


def _archive_identity(value: Mapping[str, Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        architecture: {
            field: value[architecture][field]
            for field in sorted(WORKER_IMAGE_ARCHIVE_BINDING_FIELDS)
        }
        for architecture in ("amd64", "arm64")
    }


def _worker_archive_configs_match(
    image_digests: Mapping[str, str],
    image_archives: Mapping[str, Mapping[str, Any]],
) -> bool:
    return all(
        image_archives[architecture]["config_digest"] == image_digests[architecture]
        for architecture in ("amd64", "arm64")
    )


def _valid_worker_runtime_bindings(
    value: object,
    *,
    candidate: CandidateRecord | None = None,
) -> bool:
    if not isinstance(value, dict) or set(value) != {"nodes", "domains"}:
        return False
    nodes = value.get("nodes")
    domains = value.get("domains")
    if (
        not isinstance(nodes, dict)
        or set(nodes) != set(FLEET_NODES)
        or not isinstance(domains, dict)
        or set(domains) != set(WORKER_RUNTIME_BINDING_DOMAINS)
    ):
        return False
    node_fields = {
        "domain",
        "architecture",
        "docker_driver",
        "docker_backend",
        "config_digest",
        "load_descriptor_digest",
        "load_descriptor_media_type",
        "runtime_image_id",
        "docker_descriptor_digest",
        "docker_descriptor_media_type",
        "receipt_sha256",
    }
    domain_fields = node_fields - {
        "domain",
        "docker_descriptor_digest",
        "docker_descriptor_media_type",
        "receipt_sha256",
    }
    for node, item in nodes.items():
        expected_domain = "oldlab" if node.startswith("oldlab-") else "gb10"
        if not isinstance(item, dict) or set(item) != node_fields:
            return False
        domain = item.get("domain")
        architecture = item.get("architecture")
        backend = item.get("docker_backend")
        driver = item.get("docker_driver")
        config_digest = item.get("config_digest")
        load_digest = item.get("load_descriptor_digest")
        load_media_type = item.get("load_descriptor_media_type")
        runtime_image_id = item.get("runtime_image_id")
        descriptor_digest = item.get("docker_descriptor_digest")
        descriptor_media_type = item.get("docker_descriptor_media_type")
        if (
            domain != expected_domain
            or architecture != WORKER_RUNTIME_BINDING_DOMAINS[expected_domain]
            or backend not in WORKER_RUNTIME_BACKENDS
            or driver != WORKER_RUNTIME_BACKENDS[backend]
            or IMAGE_DIGEST_RE.fullmatch(str(config_digest)) is None
            or IMAGE_DIGEST_RE.fullmatch(str(load_digest)) is None
            or load_media_type not in OCI_MANIFEST_MEDIA_TYPES | OCI_INDEX_MEDIA_TYPES
            or IMAGE_DIGEST_RE.fullmatch(str(runtime_image_id)) is None
            or DIGEST_RE.fullmatch(str(item.get("receipt_sha256"))) is None
        ):
            return False
        if backend == "classic-overlay2":
            if (
                runtime_image_id != config_digest
                or descriptor_digest is not None
                or descriptor_media_type is not None
            ):
                return False
        elif (
            runtime_image_id != load_digest
            or descriptor_digest != load_digest
            or descriptor_media_type != load_media_type
        ):
            return False
        if candidate is not None:
            archive = candidate.image_archives[architecture]
            if (
                config_digest != archive["config_digest"]
                or load_digest != archive["load_descriptor_digest"]
                or load_media_type != archive["load_descriptor_media_type"]
            ):
                return False
    for domain, item in domains.items():
        architecture = WORKER_RUNTIME_BINDING_DOMAINS[domain]
        if not isinstance(item, dict) or set(item) != domain_fields:
            return False
        domain_nodes = [
            node_item
            for node, node_item in nodes.items()
            if ("oldlab" if node.startswith("oldlab-") else "gb10") == domain
        ]
        if not domain_nodes or any(
            item[field] != node_item[field] for node_item in domain_nodes for field in domain_fields
        ):
            return False
        if item["architecture"] != architecture:
            return False
    return True


def _proc_listener_ports(path: Path) -> set[int]:
    """Read one bounded procfs TCP table and reject malformed inventory."""

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
            chunk = os.read(descriptor, min(65_536, (4 << 20) + 1 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > 4 << 20:
                raise RegistryError("host port inventory exceeds its size bound")
            chunks.append(chunk)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except RegistryError:
        raise
    except OSError as exc:
        raise RegistryError("host port inventory is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identities = {(item.st_dev, item.st_ino) for item in (lexical, opened, rebound, current)}
    if len(identities) != 1 or not stat.S_ISREG(opened.st_mode) or stat.S_ISLNK(lexical.st_mode):
        raise RegistryError("host port inventory metadata is unsafe")
    try:
        lines = b"".join(chunks).decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise RegistryError("host port inventory is malformed") from exc
    if not lines or "local_address" not in lines[0].split():
        raise RegistryError("host port inventory is malformed")
    listeners: set[int] = set()
    for line in lines[1:]:
        fields = line.split()
        if (
            len(fields) < 10
            or ":" not in fields[1]
            or re.fullmatch(r"[0-9A-Fa-f]{2}", fields[3]) is None
        ):
            raise RegistryError("host port inventory is malformed")
        _address, raw_port = fields[1].rsplit(":", 1)
        if re.fullmatch(r"[0-9A-Fa-f]{4}", raw_port) is None:
            raise RegistryError("host port inventory is malformed")
        port = int(raw_port, 16)
        if port < 1 or port > 65_535:
            raise RegistryError("host port inventory is malformed")
        if fields[3].upper() == "0A":
            listeners.add(port)
    return listeners


def collect_host_listener_ports(
    paths: Sequence[Path] = PROC_NET_TCP,
) -> frozenset[int]:
    """Collect the canonical allocator host's current TCP listeners as root."""

    hostname = socket.gethostname().split(".", 1)[0].rstrip(".").lower()
    if os.getuid() != 0 or os.geteuid() != 0 or hostname != EXPECTED_ALLOCATOR_HOST:
        raise RegistryError("host port inventory authority is unavailable")
    if tuple(paths) != PROC_NET_TCP:
        raise RegistryError("host port inventory paths are not fixed")
    listeners: set[int] = set()
    for path in paths:
        listeners.update(_proc_listener_ports(path))
    return frozenset(listeners)


def _decode_bounded_chunked_body(encoded: bytes) -> bytes:
    """Decode Docker's HTTP/1.1 chunked body without accepting extensions/trailers."""

    decoded = bytearray()
    offset = 0
    while True:
        line_end = encoded.find(b"\r\n", offset)
        if line_end < 0:
            raise RegistryError("Docker port inventory chunk framing is malformed")
        raw_size = encoded[offset:line_end]
        if re.fullmatch(rb"[0-9A-Fa-f]{1,8}", raw_size) is None:
            raise RegistryError("Docker port inventory chunk size is malformed")
        size = int(raw_size, 16)
        offset = line_end + 2
        if size == 0:
            if encoded[offset:] != b"\r\n":
                raise RegistryError("Docker port inventory chunk terminator is malformed")
            return bytes(decoded)
        if size > DOCKER_API_MAX_BYTES or len(decoded) + size > DOCKER_API_MAX_BYTES:
            raise RegistryError("Docker port inventory body exceeds its size bound")
        chunk_end = offset + size
        if chunk_end + 2 > len(encoded) or encoded[chunk_end : chunk_end + 2] != b"\r\n":
            raise RegistryError("Docker port inventory chunk framing is malformed")
        decoded.extend(encoded[offset:chunk_end])
        offset = chunk_end + 2


def _docker_published_ports(
    path: Path,
    *,
    expected_uid: int,
) -> set[int]:
    """Read Docker's published-port reservations over one fixed Unix socket."""

    client: socket.socket | None = None
    try:
        before = path.lstat()
        if (
            not stat.S_ISSOCK(before.st_mode)
            or stat.S_ISLNK(before.st_mode)
            or before.st_uid != expected_uid
            or before.st_mode & 0o002
        ):
            raise RegistryError("Docker port inventory socket metadata is unsafe")
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(5)
        client.connect(str(path))
        connected = path.lstat()
        if (before.st_dev, before.st_ino, before.st_mode, before.st_uid, before.st_gid) != (
            connected.st_dev,
            connected.st_ino,
            connected.st_mode,
            connected.st_uid,
            connected.st_gid,
        ):
            raise RegistryError("Docker port inventory socket identity drifted")
        if hasattr(socket, "SO_PEERCRED"):
            credentials = client.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            _pid, peer_uid, _gid = struct.unpack("3i", credentials)
            if peer_uid != expected_uid:
                raise RegistryError("Docker port inventory peer identity is invalid")
        client.sendall(
            b"GET /containers/json?all=1 HTTP/1.1\r\nHost: docker\r\nConnection: close\r\n\r\n",
        )
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = client.recv(min(65_536, DOCKER_API_MAX_BYTES + 65_537 - total))
            if not chunk:
                break
            total += len(chunk)
            if total > DOCKER_API_MAX_BYTES + 65_536:
                raise RegistryError("Docker port inventory response exceeds its size bound")
            chunks.append(chunk)
        after = path.lstat()
    except RegistryError:
        raise
    except (OSError, TimeoutError) as exc:
        raise RegistryError("Docker port inventory is unavailable") from exc
    finally:
        if client is not None:
            client.close()
    if (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_uid,
        before.st_gid,
    ) != (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_uid,
        after.st_gid,
    ):
        raise RegistryError("Docker port inventory socket identity drifted")
    response = b"".join(chunks)
    if b"\r\n\r\n" not in response:
        raise RegistryError("Docker port inventory HTTP response is malformed")
    header, body = response.split(b"\r\n\r\n", 1)
    if len(header) > 65_536:
        raise RegistryError("Docker port inventory HTTP headers exceed their size bound")
    try:
        lines = header.decode("ascii").split("\r\n")
    except UnicodeDecodeError as exc:
        raise RegistryError("Docker port inventory HTTP response is malformed") from exc
    if not lines or re.fullmatch(r"HTTP/1\.[01] 200(?: [ -~]*)?", lines[0]) is None:
        raise RegistryError("Docker port inventory HTTP status is invalid")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if ":" not in line:
            raise RegistryError("Docker port inventory HTTP response is malformed")
        name, value = line.split(":", 1)
        normalized = name.strip().lower()
        if (
            re.fullmatch(r"[a-z0-9-]+", normalized) is None
            or normalized in headers
            or "\r" in value
            or "\n" in value
        ):
            raise RegistryError("Docker port inventory HTTP response is malformed")
        headers[normalized] = value.strip()
    allowed_headers = {
        "api-version",
        "connection",
        "content-length",
        "content-type",
        "date",
        "docker-experimental",
        "ostype",
        "server",
        "transfer-encoding",
    }
    if set(headers) - allowed_headers:
        raise RegistryError("Docker port inventory HTTP binding is invalid")
    content_length = headers.get("content-length")
    transfer_encoding = headers.get("transfer-encoding")
    if transfer_encoding is not None:
        if transfer_encoding.lower() != "chunked" or content_length is not None:
            raise RegistryError("Docker port inventory HTTP binding is invalid")
        body = _decode_bounded_chunked_body(body)
    elif (
        content_length is None
        or re.fullmatch(r"[0-9]+", content_length) is None
        or int(content_length) != len(body)
        or len(body) > DOCKER_API_MAX_BYTES
    ):
        raise RegistryError("Docker port inventory HTTP binding is invalid")
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RegistryError("Docker port inventory payload is malformed") from exc
    if not isinstance(payload, list):
        raise RegistryError("Docker port inventory payload is malformed")
    published: set[int] = set()
    for container in payload:
        if (
            not isinstance(container, dict)
            or DIGEST_RE.fullmatch(str(container.get("Id"))) is None
            or not isinstance(container.get("Ports"), list)
        ):
            raise RegistryError("Docker port inventory container binding is invalid")
        for entry in container["Ports"]:
            if not isinstance(entry, dict) or set(entry) not in (
                {"PrivatePort", "Type"},
                {"IP", "PrivatePort", "PublicPort", "Type"},
            ):
                raise RegistryError("Docker port inventory port binding is invalid")
            if (
                not _plain_integer(entry.get("PrivatePort"), minimum=1)
                or entry["PrivatePort"] > 65_535
                or entry.get("Type") not in {"tcp", "udp", "sctp"}
                or (
                    "PublicPort" in entry
                    and (
                        not _plain_integer(entry["PublicPort"], minimum=1)
                        or entry["PublicPort"] > 65_535
                        or not isinstance(entry.get("IP"), str)
                        or len(entry["IP"]) > 64
                    )
                )
            ):
                raise RegistryError("Docker port inventory port binding is invalid")
            if "PublicPort" in entry:
                published.add(int(entry["PublicPort"]))
    return published


def collect_host_reserved_ports() -> frozenset[int]:
    """Collect all fixed-host TCP listeners and Docker published reservations."""

    listeners = collect_host_listener_ports()
    published = _docker_published_ports(DOCKER_SOCKET, expected_uid=0)
    return frozenset(set(listeners) | published)


@dataclass(frozen=True, slots=True)
class AllocationPolicy:
    """Trusted allocator bounds; never populated from a developer request."""

    uid_start: int = 32_000
    uid_end: int = 60_000
    port_start: int = 23_000
    port_end: int = 29_999
    port_block_size: int = 16
    max_bundle_bytes: int = 256 * 1024 * 1024

    def validate(self) -> None:
        if (
            self.uid_start < 1
            or self.uid_end < self.uid_start
            or self.port_start < 1024
            or self.port_end > 65_535
            or self.port_end < self.port_start
            or self.port_block_size <= max(PORT_OFFSETS.values())
            or self.max_bundle_bytes < 1
        ):
            raise RegistryError("registry allocation policy is invalid")


@dataclass(frozen=True, slots=True)
class EnvironmentRecord:
    env_id: str
    principal_id: str
    display_name: str
    layout_version: str
    runtime_id: str
    state: str
    resource_generation: int
    lifecycle_epoch: int
    service_user: str
    service_group: str
    uid: int
    gid: int
    ports: dict[str, int]
    compose_project: str
    systemd_instance: str
    candidate_root: str
    runtime_root: str
    state_root: str
    evidence_root: str
    database_name: str
    postgres_volume: str
    minio_volume: str
    task_bucket: str
    trajectories_bucket: str
    artifacts_bucket: str
    provider_namespace: str
    slurm_user: str
    slurm_account: str
    slurm_qos: str
    cgroup_slice: str
    current_candidate_id: str | None
    created_at: str


@dataclass(frozen=True, slots=True)
class CandidateRecord:
    candidate_id: str
    principal_id: str
    env_id: str
    lifecycle_epoch: int
    repository_id: str
    image_binding_kind: str
    image_binding_version: int
    candidate_sha: str
    candidate_tree: str
    bundle_sha256: str
    bundle_size: int
    bundle_path: str
    image_digests: dict[str, str]
    image_archives: dict[str, dict[str, Any]]
    imported_at: str


@dataclass(frozen=True, slots=True)
class DeploymentRecord:
    deployment_id: str
    principal_id: str
    env_id: str
    candidate_id: str
    expected_resource_generation: int
    applied_resource_generation: int | None
    applied_registry_generation: int | None
    applied_registry_payload_sha256: str | None
    finalization_payload_sha256: str | None
    worker_runtime_bindings: dict[str, Any] | None
    phase: str
    previous_candidate_id: str | None
    request_digest: str
    created_at: str
    updated_at: str


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
        raise RegistryError("registry payload is not canonical JSON") from exc


def _digest(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical(payload)).hexdigest()


def worker_image_archive_path(
    candidate_root: Path,
    candidate_id: str,
    architecture: str,
) -> Path:
    if (
        CANDIDATE_ID_RE.fullmatch(candidate_id) is None
        or architecture not in WORKER_IMAGE_ARCHITECTURES
    ):
        raise RegistryError("worker image archive identity is invalid")
    return candidate_root / candidate_id / f"loom-worker-linux-{architecture}.tar"


def _safe_tar_name(value: object) -> str:
    if not isinstance(value, str):
        raise RegistryError("worker image archive member is invalid")
    path = PurePosixPath(value)
    if not value or path.is_absolute() or ".." in path.parts:
        raise RegistryError("worker image archive member is unsafe")
    return value


def verify_worker_image_archive(
    path: Path,
    *,
    architecture: str,
    candidate_sha: str,
    image_id: str,
    expected_archive_sha256: str | None = None,
    expected_archive_size: int | None = None,
) -> dict[str, Any]:
    """Verify one uncompressed buildx ``type=docker`` archive without loading it."""

    if (
        architecture not in WORKER_IMAGE_ARCHITECTURES
        or SHA_RE.fullmatch(candidate_sha) is None
        or IMAGE_DIGEST_RE.fullmatch(image_id) is None
    ):
        raise RegistryError("worker image archive binding is invalid")
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise RegistryError("worker image archive is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_size < 1
        or metadata.st_size > MAX_WORKER_IMAGE_ARCHIVE_BYTES
    ):
        raise RegistryError("worker image archive metadata is unsafe")
    digest = hashlib.sha256()
    size = 0
    try:
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                size += len(chunk)
                if size > MAX_WORKER_IMAGE_ARCHIVE_BYTES:
                    raise RegistryError("worker image archive is too large")
                digest.update(chunk)
    except OSError as exc:
        raise RegistryError("worker image archive is unavailable") from exc
    archive_sha256 = digest.hexdigest()
    if (expected_archive_sha256 is not None and archive_sha256 != expected_archive_sha256) or (
        expected_archive_size is not None and size != expected_archive_size
    ):
        raise RegistryError("worker image archive content binding is invalid")

    def _member_bytes(
        archive: tarfile.TarFile,
        member: tarfile.TarInfo,
        *,
        maximum: int,
    ) -> bytes:
        if not member.isfile() or member.size < 1 or member.size > maximum:
            raise RegistryError("worker image archive member size is invalid")
        stream = archive.extractfile(member)
        if stream is None:
            raise RegistryError("worker image archive member is unavailable")
        raw = stream.read(maximum + 1)
        if len(raw) != member.size or len(raw) > maximum:
            raise RegistryError("worker image archive member size is invalid")
        return raw

    def _descriptor(
        value: object,
        *,
        media_types: frozenset[str],
    ) -> tuple[str, int, str]:
        if not isinstance(value, dict) or not {
            "digest",
            "size",
            "mediaType",
        }.issubset(value):
            raise RegistryError("worker image OCI descriptor is invalid")
        digest_value = value.get("digest")
        size_value = value.get("size")
        media_type = value.get("mediaType")
        if (
            IMAGE_DIGEST_RE.fullmatch(str(digest_value)) is None
            or not _plain_integer(size_value, minimum=1)
            or media_type not in media_types
        ):
            raise RegistryError("worker image OCI descriptor is invalid")
        return str(digest_value), int(size_value), str(media_type)

    def _blob_name(digest_value: str) -> str:
        return f"blobs/sha256/{digest_value.removeprefix('sha256:')}"

    try:
        with tarfile.open(path, mode="r:") as archive:
            members: dict[str, tarfile.TarInfo] = {}
            for member in archive:
                if len(members) >= MAX_WORKER_IMAGE_ARCHIVE_MEMBERS:
                    raise RegistryError("worker image archive has too many members")
                name = _safe_tar_name(member.name)
                if name in members:
                    raise RegistryError("worker image archive has duplicate members")
                if not (member.isdir() or member.isfile()):
                    raise RegistryError("worker image archive has unsafe member types")
                members[name] = member
            manifest_member = members.get("manifest.json")
            if (
                manifest_member is None
                or not manifest_member.isfile()
                or not 0 < manifest_member.size <= 1024 * 1024
            ):
                raise RegistryError("worker image archive manifest is invalid")
            manifest = json.loads(
                _member_bytes(
                    archive,
                    manifest_member,
                    maximum=1024 * 1024,
                )
            )
            if (
                not isinstance(manifest, list)
                or len(manifest) != 1
                or not isinstance(manifest[0], dict)
            ):
                raise RegistryError("worker image archive manifest is invalid")
            descriptor = manifest[0]
            config_name = _safe_tar_name(descriptor.get("Config"))
            layer_names = descriptor.get("Layers")
            if (
                not isinstance(layer_names, list)
                or not layer_names
                or any(not isinstance(item, str) for item in layer_names)
                or len(set(layer_names)) != len(layer_names)
            ):
                raise RegistryError("worker image archive layer inventory is invalid")
            index_member = members.get("index.json")
            if index_member is None:
                raise RegistryError("worker image OCI index is unavailable")
            index_raw = _member_bytes(archive, index_member, maximum=1024 * 1024)
            index_digest = f"sha256:{hashlib.sha256(index_raw).hexdigest()}"
            index = json.loads(index_raw)
            if (
                not isinstance(index, dict)
                or index.get("schemaVersion") != 2
                or not isinstance(index.get("manifests"), list)
                or len(index["manifests"]) != 1
            ):
                raise RegistryError("worker image OCI index is invalid")
            load_descriptor_digest, load_descriptor_size, load_descriptor_media_type = _descriptor(
                index["manifests"][0],
                media_types=OCI_MANIFEST_MEDIA_TYPES | OCI_INDEX_MEDIA_TYPES,
            )
            load_descriptor_member = members.get(_blob_name(load_descriptor_digest))
            if load_descriptor_member is None:
                raise RegistryError("worker image OCI load descriptor is unavailable")
            load_descriptor_raw = _member_bytes(
                archive,
                load_descriptor_member,
                maximum=16 * 1024 * 1024,
            )
            if len(load_descriptor_raw) != load_descriptor_size or hashlib.sha256(
                load_descriptor_raw
            ).hexdigest() != load_descriptor_digest.removeprefix("sha256:"):
                raise RegistryError("worker image OCI load descriptor digest is invalid")
            if load_descriptor_media_type in OCI_MANIFEST_MEDIA_TYPES:
                manifest_digest = load_descriptor_digest
                manifest_size = load_descriptor_size
                manifest_media_type = load_descriptor_media_type
            else:
                load_index = json.loads(load_descriptor_raw)
                if (
                    not isinstance(load_index, dict)
                    or load_index.get("schemaVersion") != 2
                    or not isinstance(load_index.get("manifests"), list)
                    or not load_index["manifests"]
                ):
                    raise RegistryError("worker image OCI load index is invalid")
                runtime_descriptors: list[tuple[str, int, str]] = []
                attestation_references: list[str] = []
                for child in load_index["manifests"]:
                    child_digest, child_size, child_media_type = _descriptor(
                        child,
                        media_types=OCI_MANIFEST_MEDIA_TYPES,
                    )
                    child_member = members.get(_blob_name(child_digest))
                    if child_member is None:
                        raise RegistryError("worker image OCI child descriptor is unavailable")
                    child_raw = _member_bytes(
                        archive,
                        child_member,
                        maximum=16 * 1024 * 1024,
                    )
                    if len(child_raw) != child_size or hashlib.sha256(
                        child_raw
                    ).hexdigest() != child_digest.removeprefix("sha256:"):
                        raise RegistryError("worker image OCI child descriptor digest is invalid")
                    platform_binding = child.get("platform") if isinstance(child, dict) else None
                    annotations = child.get("annotations") if isinstance(child, dict) else None
                    if (
                        isinstance(platform_binding, dict)
                        and platform_binding.get("os") == "linux"
                        and platform_binding.get("architecture")
                        == WORKER_IMAGE_ARCHITECTURES[architecture]
                    ):
                        runtime_descriptors.append((child_digest, child_size, child_media_type))
                    elif not (
                        isinstance(platform_binding, dict)
                        and platform_binding.get("os") == "unknown"
                        and platform_binding.get("architecture") == "unknown"
                        and isinstance(annotations, dict)
                        and annotations.get("vnd.docker.reference.type") == "attestation-manifest"
                        and IMAGE_DIGEST_RE.fullmatch(
                            str(annotations.get("vnd.docker.reference.digest"))
                        )
                        is not None
                    ):
                        raise RegistryError("worker image OCI child platform is invalid")
                    else:
                        attestation_references.append(
                            str(annotations["vnd.docker.reference.digest"])
                        )
                if len(runtime_descriptors) != 1:
                    raise RegistryError("worker image OCI runtime manifest is ambiguous")
                manifest_digest, manifest_size, manifest_media_type = runtime_descriptors[0]
                if any(reference != manifest_digest for reference in attestation_references):
                    raise RegistryError("worker image OCI attestation reference is invalid")
            oci_manifest_member = members.get(_blob_name(manifest_digest))
            if oci_manifest_member is None:
                raise RegistryError("worker image OCI manifest is unavailable")
            oci_manifest_raw = _member_bytes(
                archive,
                oci_manifest_member,
                maximum=16 * 1024 * 1024,
            )
            if len(oci_manifest_raw) != manifest_size or hashlib.sha256(
                oci_manifest_raw
            ).hexdigest() != manifest_digest.removeprefix("sha256:"):
                raise RegistryError("worker image OCI manifest digest is invalid")
            oci_manifest = json.loads(oci_manifest_raw)
            if (
                not isinstance(oci_manifest, dict)
                or oci_manifest.get("schemaVersion") != 2
                or not isinstance(oci_manifest.get("layers"), list)
                or not oci_manifest["layers"]
            ):
                raise RegistryError("worker image OCI manifest is invalid")
            config_digest, config_size, _config_media_type = _descriptor(
                oci_manifest.get("config"),
                media_types=OCI_CONFIG_MEDIA_TYPES,
            )
            layer_descriptors = [
                _descriptor(item, media_types=OCI_LAYER_MEDIA_TYPES)
                for item in oci_manifest["layers"]
            ]
            if (
                config_name != _blob_name(config_digest)
                or layer_names != [_blob_name(item[0]) for item in layer_descriptors]
                or len({item[0] for item in layer_descriptors}) != len(layer_descriptors)
            ):
                raise RegistryError("worker image archive OCI inventory is invalid")
            config_member = members.get(config_name)
            if (
                config_member is None
                or not config_member.isfile()
                or not 0 < config_member.size <= MAX_WORKER_IMAGE_CONFIG_BYTES
            ):
                raise RegistryError("worker image archive config is invalid")
            config_raw = _member_bytes(
                archive,
                config_member,
                maximum=MAX_WORKER_IMAGE_CONFIG_BYTES,
            )
            config_digest = hashlib.sha256(config_raw).hexdigest()
            if (
                config_size != len(config_raw)
                or config_name != f"blobs/sha256/{config_digest}"
                or image_id != f"sha256:{config_digest}"
            ):
                raise RegistryError("worker image archive config ID is invalid")
            expected_tag = f"loom-worker:{candidate_sha[:12]}-{architecture}"
            if descriptor.get("RepoTags") != [expected_tag]:
                raise RegistryError("worker image archive tag contract is invalid")
            config = json.loads(config_raw)
            rootfs = config.get("rootfs") if isinstance(config, dict) else None
            diff_ids = rootfs.get("diff_ids") if isinstance(rootfs, dict) else None
            if (
                not isinstance(diff_ids, list)
                or len(diff_ids) != len(layer_names)
                or any(IMAGE_DIGEST_RE.fullmatch(str(item)) is None for item in diff_ids)
            ):
                raise RegistryError("worker image archive rootfs binding is invalid")
            aggregate_diff_size = 0
            for layer_name, diff_id, layer_descriptor in zip(
                layer_names,
                diff_ids,
                layer_descriptors,
                strict=True,
            ):
                safe_name = _safe_tar_name(layer_name)
                layer = members.get(safe_name)
                if layer is None or not layer.isfile() or layer.size <= 0:
                    raise RegistryError("worker image archive layer is invalid")
                layer_stream = archive.extractfile(layer)
                if layer_stream is None:
                    raise RegistryError("worker image archive layer is unavailable")
                raw_digest = hashlib.sha256()
                raw_size = 0
                compressed = tempfile.SpooledTemporaryFile(max_size=64 * 1024 * 1024)
                while chunk := layer_stream.read(1024 * 1024):
                    raw_size += len(chunk)
                    if raw_size > MAX_WORKER_IMAGE_ARCHIVE_BYTES:
                        raise RegistryError("worker image archive layer is too large")
                    raw_digest.update(chunk)
                    compressed.write(chunk)
                descriptor_digest, descriptor_size, _media_type = layer_descriptor
                if (
                    raw_size != descriptor_size
                    or raw_size != layer.size
                    or raw_digest.hexdigest() != descriptor_digest.removeprefix("sha256:")
                ):
                    raise RegistryError("worker image archive layer blob is invalid")
                compressed.seek(0)
                diff_digest = hashlib.sha256()
                diff_size = 0
                try:
                    with gzip.GzipFile(fileobj=compressed, mode="rb") as expanded:
                        for chunk in iter(lambda: expanded.read(1024 * 1024), b""):
                            diff_size += len(chunk)
                            if diff_size > MAX_WORKER_IMAGE_LAYER_UNCOMPRESSED_BYTES:
                                raise RegistryError(
                                    "worker image archive expanded layer is too large"
                                )
                            aggregate_diff_size += len(chunk)
                            if aggregate_diff_size > MAX_WORKER_IMAGE_TOTAL_UNCOMPRESSED_BYTES:
                                raise RegistryError(
                                    "worker image archive expanded layers are too large"
                                )
                            diff_digest.update(chunk)
                except (OSError, EOFError) as exc:
                    raise RegistryError(
                        "worker image archive layer compression is invalid"
                    ) from exc
                finally:
                    compressed.close()
                if diff_id != f"sha256:{diff_digest.hexdigest()}":
                    raise RegistryError("worker image archive layer digest is invalid")
    except RegistryError:
        raise
    except (OSError, tarfile.TarError, UnicodeError, json.JSONDecodeError) as exc:
        raise RegistryError("worker image archive is invalid") from exc

    runtime = config.get("config") if isinstance(config, dict) else None
    labels = runtime.get("Labels") if isinstance(runtime, dict) else None
    if (
        config.get("os") != "linux"
        or config.get("architecture") != WORKER_IMAGE_ARCHITECTURES[architecture]
        or not isinstance(labels, dict)
        or labels.get(WORKER_IMAGE_REVISION_LABEL) != candidate_sha
        or runtime.get("Cmd") != WORKER_IMAGE_COMMAND
        or runtime.get("Entrypoint") not in (None, [])
    ):
        raise RegistryError("worker image archive runtime contract is invalid")
    return {
        "architecture": architecture,
        "archive_sha256": archive_sha256,
        "archive_size": size,
        "config_digest": image_id,
        "index_digest": index_digest,
        "manifest_digest": manifest_digest,
        "manifest_media_type": manifest_media_type,
        "load_descriptor_digest": load_descriptor_digest,
        "load_descriptor_media_type": load_descriptor_media_type,
        "tag": expected_tag,
    }


def _timestamp() -> str:
    # SQLite owns transaction ordering; this value is evidence metadata only.
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _read_regular(path: Path, *, limit: int) -> bytes:
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
                raise RegistryError("registry input exceeds its size bound")
            chunks.append(chunk)
        rebound = os.fstat(descriptor)
        current = path.lstat()
    except RegistryError:
        raise
    except OSError as exc:
        raise RegistryError("registry input is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    identities = (
        (
            lexical.st_dev,
            lexical.st_ino,
            lexical.st_mode,
            lexical.st_size,
            lexical.st_mtime_ns,
            lexical.st_ctime_ns,
        ),
        (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        ),
        (
            rebound.st_dev,
            rebound.st_ino,
            rebound.st_mode,
            rebound.st_size,
            rebound.st_mtime_ns,
            rebound.st_ctime_ns,
        ),
        (
            current.st_dev,
            current.st_ino,
            current.st_mode,
            current.st_size,
            current.st_mtime_ns,
            current.st_ctime_ns,
        ),
    )
    if (
        not stat.S_ISREG(opened.st_mode)
        or stat.S_ISLNK(lexical.st_mode)
        or opened.st_nlink != 1
        or len(set(identities)) != 1
        or total != opened.st_size
    ):
        raise RegistryError("registry input metadata is unsafe")
    return b"".join(chunks)


def _closed_request(
    payload: Mapping[str, Any],
    *,
    fields: set[str],
    kind: str,
) -> dict[str, Any]:
    if not isinstance(payload, Mapping):
        raise RegistryError("registry request is invalid")
    request = dict(payload)
    if (
        set(request) != fields
        or request.get("schema_version") != SCHEMA_VERSION
        or request.get("kind") != kind
        or PRINCIPAL_RE.fullmatch(str(request.get("principal_id"))) is None
        or IDEMPOTENCY_RE.fullmatch(str(request.get("idempotency_key"))) is None
        or _canonical(request) != _canonical(payload)
    ):
        raise RegistryError("registry request binding is invalid")
    return request


def _validate_display_name(value: object) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise RegistryError("registry display name is invalid")
    return value


def _row_mapping(row: sqlite3.Row) -> dict[str, Any]:
    return {key: row[key] for key in row.keys()}


def _plain_integer(value: object, *, minimum: int = 0) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= minimum


def _valid_timestamp(value: object) -> bool:
    if not isinstance(value, str) or not value.endswith("Z"):
        return False
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError:
        return False
    return parsed.tzinfo is not None


def build_fleet_identity_inventory(
    node_results: Sequence[Mapping[str, Any]],
    *,
    registry_generation: int,
    registry_payload_sha256: str,
    policy: AllocationPolicy,
) -> bytes:
    """Build the one closed aggregate accepted by the production allocator."""

    if (
        not _plain_integer(registry_generation)
        or DIGEST_RE.fullmatch(registry_payload_sha256) is None
        or len(node_results) != len(FLEET_NODES)
    ):
        raise RegistryError("fleet identity inventory source binding is invalid")
    nodes = [dict(item) for item in node_results]
    try:
        checked = [
            datetime.fromisoformat(str(item["checked_at"]).removesuffix("Z") + "+00:00")
            for item in nodes
        ]
    except (KeyError, ValueError) as exc:
        raise RegistryError("fleet identity inventory source freshness is invalid") from exc
    collected = max(checked)
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "kind": FLEET_IDENTITY_INVENTORY_KIND,
        "registry_generation": registry_generation,
        "registry_payload_sha256": registry_payload_sha256,
        "uid_start": policy.uid_start,
        "uid_end": policy.uid_end,
        "collected_at": collected.isoformat().replace("+00:00", "Z"),
        "expires_at": (collected + timedelta(seconds=FLEET_IDENTITY_MAX_AGE_SECONDS))
        .isoformat()
        .replace("+00:00", "Z"),
        "node_set_sha256": _digest({"nodes": list(FLEET_NODES)}),
        "nodes": nodes,
    }
    raw = _canonical({**unsigned, "payload_sha256": _digest(unsigned)})
    DeveloperEnvironmentRegistry.verify_fleet_identity_inventory(
        raw,
        policy=policy,
        now=collected,
    )
    return raw


def _valid_fixed_path(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("/"):
        return False
    path = Path(value)
    return (
        path.is_absolute()
        and "." not in path.parts
        and ".." not in path.parts
        and str(path) == value
    )


def _validate_host_user_binding(name: str, identity: int) -> None:
    try:
        by_identity = pwd.getpwuid(identity)
    except KeyError:
        by_identity = None
    except OSError as exc:
        raise RegistryError("host user inventory is unavailable") from exc
    try:
        by_name = pwd.getpwnam(name)
    except KeyError:
        by_name = None
    except OSError as exc:
        raise RegistryError("host user inventory is unavailable") from exc
    if (by_identity is None) != (by_name is None) or (
        by_identity is not None
        and (
            by_identity.pw_name != name
            or by_identity.pw_uid != identity
            or by_name is None
            or by_name.pw_name != name
            or by_name.pw_uid != identity
        )
    ):
        raise RegistryError("legacy service user identity conflicts with host")


def _validate_host_group_binding(name: str, identity: int) -> None:
    try:
        by_identity = grp.getgrgid(identity)
    except KeyError:
        by_identity = None
    except OSError as exc:
        raise RegistryError("host group inventory is unavailable") from exc
    try:
        by_name = grp.getgrnam(name)
    except KeyError:
        by_name = None
    except OSError as exc:
        raise RegistryError("host group inventory is unavailable") from exc
    if (by_identity is None) != (by_name is None) or (
        by_identity is not None
        and (
            by_identity.gr_name != name
            or by_identity.gr_gid != identity
            or by_name is None
            or by_name.gr_name != name
            or by_name.gr_gid != identity
        )
    ):
        raise RegistryError("legacy service group identity conflicts with host")


class DeveloperEnvironmentRegistry:
    """SQLite-backed authority with per-operation short-lived connections."""

    def __init__(
        self,
        database: Path,
        *,
        policy: AllocationPolicy | None = None,
        candidate_root: Path | None = None,
        snapshot_path: Path | None = None,
        fleet_identity_inventory_path: Path | None = None,
        system_mode: bool = False,
        port_inventory_collector: Callable[[], Set[int]] | None = None,
    ) -> None:
        policy = policy or AllocationPolicy()
        policy.validate()
        self.database = database
        self.candidate_root = candidate_root or database.parent / "candidates"
        self.snapshot_path = snapshot_path or database.parent / "current-snapshot.json"
        if self.snapshot_path != database.parent / "current-snapshot.json":
            raise RegistryError("registry snapshot path is not fixed")
        self.fleet_identity_inventory_path = (
            fleet_identity_inventory_path or database.parent / "fleet-identity-inventory.json"
        )
        if system_mode and self.fleet_identity_inventory_path != SYSTEM_FLEET_IDENTITY_INVENTORY:
            raise RegistryError("fleet identity inventory path is not fixed")
        self.require_fleet_identity_inventory = (
            system_mode or fleet_identity_inventory_path is not None
        )
        self.snapshot_lock_path = database.parent / ".current-snapshot.lock"
        self.snapshot_dirty = False
        self.system_mode = system_mode
        self.port_inventory_collector = (
            collect_host_reserved_ports
            if system_mode and port_inventory_collector is None
            else port_inventory_collector
        )
        self.policy = policy
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @classmethod
    def open_system(cls) -> DeveloperEnvironmentRegistry:
        """Open the fixed production registry after root metadata validation."""

        if os.getuid() != 0 or os.geteuid() != 0:
            raise RegistryError("system registry requires root")
        try:
            SYSTEM_ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
            root_descriptor = os.open(
                SYSTEM_ROOT,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            try:
                metadata = os.fstat(root_descriptor)
                if not stat.S_ISDIR(metadata.st_mode) or (metadata.st_uid, metadata.st_gid) != (
                    0,
                    0,
                ):
                    raise RegistryError("system registry root is unsafe")
                os.fchmod(root_descriptor, 0o700)
                if stat.S_IMODE(os.fstat(root_descriptor).st_mode) != 0o700:
                    raise RegistryError("system registry root is unsafe")
            finally:
                os.close(root_descriptor)
        except RegistryError:
            raise
        except OSError as exc:
            raise RegistryError("system registry root is unsafe") from exc
        registry = cls(
            SYSTEM_DATABASE,
            candidate_root=SYSTEM_CANDIDATE_ROOT,
            snapshot_path=SYSTEM_SNAPSHOT,
            fleet_identity_inventory_path=SYSTEM_FLEET_IDENTITY_INVENTORY,
            system_mode=True,
        )
        registry._validate_database_metadata(require_root=True)
        return registry

    def _connect(self) -> sqlite3.Connection:
        self._validate_storage_boundary()
        connection = sqlite3.connect(
            self.database,
            timeout=30,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute("PRAGMA busy_timeout = 30000")
            journal_mode = connection.execute("PRAGMA journal_mode = WAL").fetchone()
            connection.execute("PRAGMA synchronous = FULL")
            synchronous = connection.execute("PRAGMA synchronous").fetchone()
            foreign_keys = connection.execute("PRAGMA foreign_keys").fetchone()
            if (
                journal_mode is None
                or str(journal_mode[0]).lower() != "wal"
                or synchronous is None
                or int(synchronous[0]) != 2
                or foreign_keys is None
                or int(foreign_keys[0]) != 1
            ):
                raise RegistryError("registry durability settings are unavailable")
            return connection
        except Exception:
            connection.close()
            raise

    def _validate_storage_boundary(self) -> None:
        try:
            parent = self.database.parent.lstat()
        except OSError as exc:
            raise RegistryError("registry storage root is unavailable") from exc
        if (
            not stat.S_ISDIR(parent.st_mode)
            or stat.S_ISLNK(parent.st_mode)
            or (
                self.system_mode
                and (
                    (parent.st_uid, parent.st_gid) != (0, 0)
                    or stat.S_IMODE(parent.st_mode) != 0o700
                )
            )
        ):
            raise RegistryError("registry storage root is unsafe")
        try:
            database = self.database.lstat()
        except OSError as exc:
            raise RegistryError("registry storage metadata is unavailable") from exc
        if (
            not stat.S_ISREG(database.st_mode)
            or stat.S_ISLNK(database.st_mode)
            or database.st_nlink != 1
            or (
                self.system_mode
                and (
                    (database.st_uid, database.st_gid) != (0, 0)
                    or stat.S_IMODE(database.st_mode) != 0o600
                )
            )
        ):
            raise RegistryError("registry storage metadata is unsafe")
        for path in (Path(f"{self.database}-wal"), Path(f"{self.database}-shm")):
            self._validate_sqlite_sidecar(path)

    def _validate_sqlite_sidecar(self, path: Path) -> None:
        for _attempt in range(8):
            descriptor = -1
            try:
                lexical = path.lstat()
            except FileNotFoundError:
                return
            except OSError as exc:
                raise RegistryError("registry storage metadata is unavailable") from exc
            if not stat.S_ISREG(lexical.st_mode) or stat.S_ISLNK(lexical.st_mode):
                raise RegistryError("registry storage metadata is unsafe")
            try:
                descriptor = os.open(
                    path,
                    os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
                )
                opened = os.fstat(descriptor)
                current = path.lstat()
                rebound = os.fstat(descriptor)
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise RegistryError("registry storage metadata is unavailable") from exc
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
            identities = {
                (item.st_dev, item.st_ino) for item in (lexical, opened, current, rebound)
            }
            if len(identities) != 1:
                continue
            if opened.st_nlink == 0 or current.st_nlink == 0:
                continue
            if (
                not stat.S_ISREG(opened.st_mode)
                or stat.S_ISLNK(current.st_mode)
                or opened.st_nlink != 1
                or current.st_nlink != 1
                or (
                    self.system_mode
                    and (
                        (opened.st_uid, opened.st_gid) != (0, 0)
                        or (current.st_uid, current.st_gid) != (0, 0)
                        or stat.S_IMODE(opened.st_mode) != 0o600
                        or stat.S_IMODE(current.st_mode) != 0o600
                    )
                )
            ):
                raise RegistryError("registry storage metadata is unsafe")
            return
        raise RegistryError("registry storage metadata changed during verification")

    def _initialize(self) -> None:
        try:
            descriptor = os.open(
                self.database,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
        except FileExistsError:
            pass
        except OSError as exc:
            raise RegistryError("registry database cannot be created safely") from exc
        else:
            os.close(descriptor)
        connection = self._connect()
        try:
            # Inspect an existing deployment table before any DDL.  In
            # particular, do not create the finalization table (or partially
            # add its reference column) around legacy committed rows: those
            # rows need an explicit, evidence-backed rebuild.
            deployments_exist = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'deployments'
                """
            ).fetchone()
            if deployments_exist is not None:
                existing_columns = {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(deployments)").fetchall()
                }
                finalizations_exist = connection.execute(
                    """
                    SELECT 1 FROM sqlite_master
                    WHERE type = 'table' AND name = 'deployment_finalizations'
                    """
                ).fetchone()
                required_binding_columns = {
                    "applied_resource_generation",
                    "applied_registry_generation",
                    "applied_registry_payload_sha256",
                    "finalization_payload_sha256",
                }
                committed = connection.execute(
                    "SELECT 1 FROM deployments WHERE phase = 'committed' LIMIT 1"
                ).fetchone()
                if committed is not None and (
                    not required_binding_columns.issubset(existing_columns)
                    or finalizations_exist is None
                ):
                    raise RegistryError(
                        "legacy committed deployment binding requires explicit migration"
                    )
            environments_exist = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'environments'
                """
            ).fetchone()
            candidates_exist = connection.execute(
                """
                SELECT 1 FROM sqlite_master
                WHERE type = 'table' AND name = 'candidates'
                """
            ).fetchone()
            missing_lifecycle_binding = False
            if environments_exist is not None:
                environment_columns = {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(environments)").fetchall()
                }
                missing_lifecycle_binding = "lifecycle_epoch" not in environment_columns
            if candidates_exist is not None:
                candidate_columns = {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(candidates)").fetchall()
                }
                missing_lifecycle_binding = (
                    missing_lifecycle_binding or "lifecycle_epoch" not in candidate_columns
                )
            if missing_lifecycle_binding:
                populated = (
                    connection.execute("SELECT 1 FROM environments LIMIT 1").fetchone()
                    if environments_exist is not None
                    else None
                )
                populated_candidate = (
                    connection.execute("SELECT 1 FROM candidates LIMIT 1").fetchone()
                    if candidates_exist is not None
                    else None
                )
                if populated is not None or populated_candidate is not None:
                    raise RegistryError(
                        "legacy lifecycle candidate binding requires explicit migration"
                    )
            self._prepare_worker_image_binding_schema(connection)
            connection.executescript(
                """
                BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS registry_meta (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    schema_version INTEGER NOT NULL,
                    database_schema_version INTEGER NOT NULL,
                    generation INTEGER NOT NULL CHECK (generation >= 0)
                );
                INSERT OR IGNORE INTO registry_meta(
                    singleton, schema_version, database_schema_version, generation
                ) VALUES (1, 1, 3, 0);

                CREATE TABLE IF NOT EXISTS environments (
                    env_id TEXT PRIMARY KEY,
                    principal_id TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    layout_version TEXT NOT NULL
                        CHECK (layout_version IN ('dynamic-v1', 'legacy-v1')),
                    runtime_id TEXT NOT NULL UNIQUE,
                    state TEXT NOT NULL
                        CHECK (state IN ('ready', 'deploying', 'active', 'retired', 'quarantined')),
                    resource_generation INTEGER NOT NULL CHECK (resource_generation >= 1),
                    lifecycle_epoch INTEGER NOT NULL CHECK (lifecycle_epoch >= 1),
                    service_user TEXT NOT NULL UNIQUE,
                    service_group TEXT NOT NULL UNIQUE,
                    uid INTEGER NOT NULL UNIQUE,
                    gid INTEGER NOT NULL UNIQUE CHECK (gid = uid),
                    compose_project TEXT NOT NULL UNIQUE,
                    systemd_instance TEXT NOT NULL UNIQUE,
                    candidate_root TEXT NOT NULL UNIQUE,
                    runtime_root TEXT NOT NULL UNIQUE,
                    state_root TEXT NOT NULL UNIQUE,
                    evidence_root TEXT NOT NULL UNIQUE,
                    database_name TEXT NOT NULL UNIQUE,
                    postgres_volume TEXT NOT NULL UNIQUE,
                    minio_volume TEXT NOT NULL UNIQUE,
                    task_bucket TEXT NOT NULL UNIQUE,
                    trajectories_bucket TEXT NOT NULL UNIQUE,
                    artifacts_bucket TEXT NOT NULL UNIQUE,
                    provider_namespace TEXT NOT NULL UNIQUE,
                    slurm_user TEXT NOT NULL UNIQUE,
                    slurm_account TEXT NOT NULL UNIQUE,
                    slurm_qos TEXT NOT NULL UNIQUE,
                    cgroup_slice TEXT NOT NULL UNIQUE,
                    current_candidate_id TEXT,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS environment_ports (
                    env_id TEXT NOT NULL REFERENCES environments(env_id),
                    name TEXT NOT NULL,
                    port INTEGER NOT NULL UNIQUE,
                    PRIMARY KEY (env_id, name)
                );

                CREATE TABLE IF NOT EXISTS registration_requests (
                    principal_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    env_id TEXT NOT NULL REFERENCES environments(env_id),
                    PRIMARY KEY (principal_id, idempotency_key)
                );

                CREATE TABLE IF NOT EXISTS candidates (
                    candidate_id TEXT PRIMARY KEY,
                    principal_id TEXT NOT NULL,
                    env_id TEXT NOT NULL REFERENCES environments(env_id),
                    lifecycle_epoch INTEGER NOT NULL CHECK (lifecycle_epoch >= 1),
                    repository_id TEXT NOT NULL,
                    image_binding_kind TEXT NOT NULL,
                    image_binding_version INTEGER NOT NULL,
                    candidate_sha TEXT NOT NULL,
                    candidate_tree TEXT NOT NULL,
                    bundle_sha256 TEXT NOT NULL,
                    bundle_size INTEGER NOT NULL,
                    image_digests_json TEXT NOT NULL,
                    image_archives_json TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    UNIQUE (
                        env_id, candidate_sha, candidate_tree, bundle_sha256,
                        image_binding_kind, image_binding_version,
                        image_digests_json, image_archives_json
                    )
                );

                CREATE TABLE IF NOT EXISTS candidate_requests (
                    principal_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
                    PRIMARY KEY (principal_id, idempotency_key)
                );

                CREATE TABLE IF NOT EXISTS deployments (
                    deployment_id TEXT PRIMARY KEY,
                    principal_id TEXT NOT NULL,
                    env_id TEXT NOT NULL REFERENCES environments(env_id),
                    candidate_id TEXT NOT NULL REFERENCES candidates(candidate_id),
                    expected_resource_generation INTEGER NOT NULL,
                    applied_resource_generation INTEGER,
                    applied_registry_generation INTEGER,
                    applied_registry_payload_sha256 TEXT,
                    finalization_payload_sha256 TEXT,
                    worker_runtime_bindings_json TEXT,
                    failed_from_phase TEXT,
                    phase TEXT NOT NULL
                        CHECK (
                            phase IN (
                                'requested', 'resources-verified', 'candidate-materialized',
                                'services-prepared', 'capacity-ready', 'verified',
                                'committed', 'failed'
                            )
                        ),
                    previous_candidate_id TEXT,
                    request_digest TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS deployment_requests (
                    principal_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    request_digest TEXT NOT NULL,
                    deployment_id TEXT NOT NULL REFERENCES deployments(deployment_id),
                    PRIMARY KEY (principal_id, idempotency_key)
                );

                CREATE TABLE IF NOT EXISTS deployment_finalizations (
                    deployment_id TEXT PRIMARY KEY REFERENCES deployments(deployment_id),
                    payload_json TEXT NOT NULL,
                    payload_sha256 TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS port_allocation_journal (
                    journal_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    env_id TEXT NOT NULL REFERENCES environments(env_id),
                    action TEXT NOT NULL CHECK (action IN ('initial', 'pre-deployment-reallocation')),
                    expected_resource_generation INTEGER NOT NULL,
                    applied_resource_generation INTEGER NOT NULL,
                    registry_generation INTEGER NOT NULL,
                    prior_ports_json TEXT NOT NULL,
                    allocated_ports_json TEXT NOT NULL,
                    reserved_ports_json TEXT NOT NULL,
                    reservation_inventory_sha256 TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE (env_id, applied_resource_generation)
                );
                COMMIT;
                """
            )
            environment_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(environments)").fetchall()
            }
            candidate_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(candidates)").fetchall()
            }
            if "lifecycle_epoch" not in environment_columns:
                connection.execute("ALTER TABLE environments ADD COLUMN lifecycle_epoch INTEGER")
            if "lifecycle_epoch" not in candidate_columns:
                connection.execute("ALTER TABLE candidates ADD COLUMN lifecycle_epoch INTEGER")
            deployment_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(deployments)").fetchall()
            }
            if "worker_runtime_bindings_json" not in deployment_columns:
                connection.execute(
                    "ALTER TABLE deployments ADD COLUMN worker_runtime_bindings_json TEXT",
                )
            applied_columns = {
                "applied_resource_generation",
                "applied_registry_generation",
                "applied_registry_payload_sha256",
                "finalization_payload_sha256",
            }
            missing_applied_columns = applied_columns - deployment_columns
            if missing_applied_columns:
                committed = connection.execute(
                    "SELECT 1 FROM deployments WHERE phase = 'committed' LIMIT 1"
                ).fetchone()
                if committed is not None:
                    raise RegistryError(
                        "legacy committed deployment binding requires explicit migration"
                    )
                connection.execute("BEGIN IMMEDIATE")
                for column, declaration in (
                    ("applied_resource_generation", "INTEGER"),
                    ("applied_registry_generation", "INTEGER"),
                    ("applied_registry_payload_sha256", "TEXT"),
                    ("finalization_payload_sha256", "TEXT"),
                ):
                    if column in missing_applied_columns:
                        connection.execute(
                            f"ALTER TABLE deployments ADD COLUMN {column} {declaration}"
                        )
                connection.execute("COMMIT")
            if "failed_from_phase" not in deployment_columns:
                connection.execute(
                    "ALTER TABLE deployments ADD COLUMN failed_from_phase TEXT",
                )
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()
        os.chmod(self.database, 0o600)
        self._validate_database_metadata(require_root=False)
        with self._transaction(immediate=False) as verified:
            metadata = verified.execute(
                """
                SELECT schema_version, database_schema_version
                FROM registry_meta WHERE singleton = 1
                """
            ).fetchone()
            if (
                metadata is None
                or metadata["schema_version"] != SCHEMA_VERSION
                or metadata["database_schema_version"] != DATABASE_SCHEMA_VERSION
            ):
                raise RegistryError("registry schema version is unsupported")
        self._reconcile_current_snapshot()

    @staticmethod
    def _prepare_worker_image_binding_schema(connection: sqlite3.Connection) -> None:
        """Atomically mark an unbound legacy registry or reject it unchanged."""

        table_names = {
            str(row["name"])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
        registry_meta_exists = "registry_meta" in table_names
        candidates_exist = "candidates" in table_names
        if not registry_meta_exists and not candidates_exist:
            return
        if registry_meta_exists != candidates_exist:
            raise RegistryError("worker image binding schema is incomplete")
        connection.execute("BEGIN IMMEDIATE")

        meta_columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(registry_meta)").fetchall()
        }
        candidate_columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(candidates)").fetchall()
        }
        marker_columns = {
            "database_schema_version" in meta_columns,
            "image_binding_kind" in candidate_columns,
            "image_binding_version" in candidate_columns,
            "image_archives_json" in candidate_columns,
        }
        if marker_columns == {True}:
            metadata = connection.execute(
                """
                SELECT schema_version, database_schema_version
                FROM registry_meta WHERE singleton = 1
                """
            ).fetchone()
            if metadata is None or metadata["schema_version"] != SCHEMA_VERSION:
                raise RegistryError("worker image binding schema is unsupported")
            database_schema_version = metadata["database_schema_version"]
            if database_schema_version == DATABASE_SCHEMA_VERSION:
                connection.execute("COMMIT")
                return
            if database_schema_version != 2:
                raise RegistryError("worker image binding schema is unsupported")
            for table in (
                "candidates",
                "candidate_requests",
                "deployments",
                "deployment_requests",
                "deployment_finalizations",
            ):
                if (
                    table in table_names
                    and connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None
                ):
                    raise RegistryError("worker image binding v2-to-v3 requires explicit migration")
            unsafe_environment = connection.execute(
                """
                SELECT 1 FROM environments
                WHERE state NOT IN ('ready', 'retired') OR current_candidate_id IS NOT NULL
                LIMIT 1
                """
            ).fetchone()
            if unsafe_environment is not None:
                raise RegistryError(
                    "worker image binding v3 requires ready or retired environments"
                )
            try:
                deployment_columns = (
                    {
                        str(row["name"])
                        for row in connection.execute("PRAGMA table_info(deployments)").fetchall()
                    }
                    if "deployments" in table_names
                    else set()
                )
                if deployment_columns and "worker_runtime_bindings_json" not in deployment_columns:
                    connection.execute(
                        "ALTER TABLE deployments ADD COLUMN worker_runtime_bindings_json TEXT"
                    )
                changed = connection.execute(
                    """
                    UPDATE registry_meta SET database_schema_version = ?
                    WHERE singleton = 1 AND schema_version = ?
                        AND database_schema_version = 2
                    """,
                    (DATABASE_SCHEMA_VERSION, SCHEMA_VERSION),
                )
                if changed.rowcount != 1:
                    raise RegistryError("worker image binding v3 migration is incomplete")
                connection.execute("COMMIT")
            except Exception:
                try:
                    connection.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
            return
        if marker_columns != {False}:
            raise RegistryError("worker image binding schema is incomplete")

        metadata = connection.execute(
            "SELECT schema_version FROM registry_meta WHERE singleton = 1"
        ).fetchone()
        metadata_count = connection.execute("SELECT COUNT(*) FROM registry_meta").fetchone()
        if (
            metadata is None
            or metadata_count is None
            or int(metadata_count[0]) != 1
            or metadata["schema_version"] != SCHEMA_VERSION
            or "environments" not in table_names
        ):
            raise RegistryError("legacy worker image binding schema is invalid")
        environment_columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(environments)").fetchall()
        }
        if not {"state", "current_candidate_id"}.issubset(environment_columns):
            raise RegistryError("legacy worker image binding schema is invalid")
        unsafe_environment = connection.execute(
            """
            SELECT 1 FROM environments
            WHERE state NOT IN ('ready', 'retired') OR current_candidate_id IS NOT NULL
            LIMIT 1
            """
        ).fetchone()
        if unsafe_environment is not None:
            raise RegistryError(
                "legacy worker image binding requires ready or retired environments"
            )
        for table in (
            "candidates",
            "registration_requests",
            "candidate_requests",
            "deployments",
            "deployment_requests",
            "deployment_finalizations",
        ):
            if (
                table in table_names
                and connection.execute(f"SELECT 1 FROM {table} LIMIT 1").fetchone() is not None
            ):
                raise RegistryError("legacy worker image binding requires explicit migration")

        try:
            connection.execute("DROP TABLE candidates")
            connection.execute(
                """
                CREATE TABLE candidates (
                    candidate_id TEXT PRIMARY KEY,
                    principal_id TEXT NOT NULL,
                    env_id TEXT NOT NULL REFERENCES environments(env_id),
                    lifecycle_epoch INTEGER NOT NULL CHECK (lifecycle_epoch >= 1),
                    repository_id TEXT NOT NULL,
                    image_binding_kind TEXT NOT NULL,
                    image_binding_version INTEGER NOT NULL,
                    candidate_sha TEXT NOT NULL,
                    candidate_tree TEXT NOT NULL,
                    bundle_sha256 TEXT NOT NULL,
                    bundle_size INTEGER NOT NULL,
                    image_digests_json TEXT NOT NULL,
                    image_archives_json TEXT NOT NULL,
                    imported_at TEXT NOT NULL,
                    UNIQUE (
                        env_id, candidate_sha, candidate_tree, bundle_sha256,
                        image_binding_kind, image_binding_version,
                        image_digests_json, image_archives_json
                    )
                )
                """
            )
            connection.execute(
                """
                ALTER TABLE registry_meta
                ADD COLUMN database_schema_version INTEGER NOT NULL DEFAULT 3
                """
            )
            if "deployments" in table_names:
                deployment_columns = {
                    str(row["name"])
                    for row in connection.execute("PRAGMA table_info(deployments)").fetchall()
                }
                if "worker_runtime_bindings_json" not in deployment_columns:
                    connection.execute(
                        "ALTER TABLE deployments ADD COLUMN worker_runtime_bindings_json TEXT"
                    )
            changed = connection.execute(
                """
                UPDATE registry_meta SET database_schema_version = ?
                WHERE singleton = 1 AND schema_version = ?
                """,
                (DATABASE_SCHEMA_VERSION, SCHEMA_VERSION),
            )
            if changed.rowcount != 1:
                raise RegistryError("legacy worker image binding migration is incomplete")
            migrated_metadata = connection.execute(
                """
                SELECT schema_version, database_schema_version
                FROM registry_meta WHERE singleton = 1
                """
            ).fetchone()
            migrated_candidate_columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(candidates)").fetchall()
            }
            if (
                migrated_metadata is None
                or migrated_metadata["schema_version"] != SCHEMA_VERSION
                or migrated_metadata["database_schema_version"] != DATABASE_SCHEMA_VERSION
                or not {
                    "image_binding_kind",
                    "image_binding_version",
                    "image_archives_json",
                }.issubset(migrated_candidate_columns)
                or connection.execute("PRAGMA foreign_key_check").fetchone() is not None
            ):
                raise RegistryError("legacy worker image binding migration is incomplete")
            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise

    def _validate_database_metadata(self, *, require_root: bool) -> None:
        metadata = self.database.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or (require_root and (metadata.st_uid, metadata.st_gid) != (0, 0))
        ):
            raise RegistryError("registry database metadata is unsafe")

    @contextmanager
    def _transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
            yield connection
            connection.execute("COMMIT")
        except Exception:
            try:
                connection.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            connection.close()
        if immediate:
            self._publish_current_snapshot()

    @staticmethod
    def _bump_generation(connection: sqlite3.Connection) -> int:
        connection.execute(
            "UPDATE registry_meta SET generation = generation + 1 WHERE singleton = 1"
        )
        row = connection.execute(
            "SELECT generation FROM registry_meta WHERE singleton = 1"
        ).fetchone()
        if row is None:
            raise RegistryError("registry generation is unavailable")
        return int(row["generation"])

    @staticmethod
    def _new_identifier(
        connection: sqlite3.Connection,
        *,
        table: str,
        column: str,
        prefix: str,
        token_bytes: int,
    ) -> str:
        if (table, column) not in {
            ("environments", "env_id"),
            ("environments", "runtime_id"),
            ("deployments", "deployment_id"),
        }:
            raise RegistryError("registry identifier allocation is invalid")
        for _attempt in range(128):
            value = prefix + secrets.token_hex(token_bytes)
            if (
                connection.execute(
                    f"SELECT 1 FROM {table} WHERE {column} = ?",
                    (value,),
                ).fetchone()
                is None
            ):
                return value
        raise RegistryError("registry identifier allocation is exhausted")

    @staticmethod
    def verify_fleet_identity_inventory(
        raw: bytes,
        *,
        policy: AllocationPolicy,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RegistryError("fleet identity inventory is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != FLEET_IDENTITY_INVENTORY_FIELDS
            or payload.get("schema_version") != SCHEMA_VERSION
            or payload.get("kind") != FLEET_IDENTITY_INVENTORY_KIND
            or not _plain_integer(payload.get("registry_generation"))
            or DIGEST_RE.fullmatch(str(payload.get("registry_payload_sha256"))) is None
            or payload.get("uid_start") != policy.uid_start
            or payload.get("uid_end") != policy.uid_end
            or not isinstance(payload.get("nodes"), list)
            or DIGEST_RE.fullmatch(str(payload.get("node_set_sha256"))) is None
            or DIGEST_RE.fullmatch(str(payload.get("payload_sha256"))) is None
            or raw != _canonical(payload)
        ):
            raise RegistryError("fleet identity inventory binding is invalid")
        unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
        if payload["payload_sha256"] != _digest(unsigned):
            raise RegistryError("fleet identity inventory digest is invalid")
        expected_node_set_sha256 = _digest({"nodes": list(FLEET_NODES)})
        if payload["node_set_sha256"] != expected_node_set_sha256:
            raise RegistryError("fleet identity node set binding is invalid")
        try:
            collected_at = datetime.fromisoformat(
                str(payload["collected_at"]).removesuffix("Z") + "+00:00",
            )
            expires_at = datetime.fromisoformat(
                str(payload["expires_at"]).removesuffix("Z") + "+00:00",
            )
        except ValueError as exc:
            raise RegistryError("fleet identity inventory freshness is invalid") from exc
        observed_now = now or datetime.now(UTC)
        if (
            not _valid_timestamp(payload["collected_at"])
            or not _valid_timestamp(payload["expires_at"])
            or collected_at > observed_now + timedelta(seconds=30)
            or expires_at != collected_at + timedelta(seconds=FLEET_IDENTITY_MAX_AGE_SECONDS)
            or observed_now > expires_at
        ):
            raise RegistryError("fleet identity inventory freshness is invalid")
        nodes = payload["nodes"]
        if len(nodes) != len(FLEET_NODES) or [
            item.get("node") if isinstance(item, dict) else None for item in nodes
        ] != list(FLEET_NODES):
            raise RegistryError("fleet identity inventory is incomplete")
        for item in nodes:
            if not isinstance(item, dict):
                raise RegistryError("fleet identity inventory node binding is invalid")
            occupied_ids = item.get("occupied_ids")
            expected_domain = "oldlab" if str(item.get("node")).startswith("oldlab-") else "gb10"
            try:
                checked_at = datetime.fromisoformat(
                    str(item["checked_at"]).removesuffix("Z") + "+00:00",
                )
            except (KeyError, ValueError) as exc:
                raise RegistryError("fleet identity inventory node freshness is invalid") from exc
            if (
                set(item) != NODE_IDENTITY_INVENTORY_FIELDS
                or item.get("schema_version") != SCHEMA_VERSION
                or item.get("kind") != NODE_IDENTITY_INVENTORY_KIND
                or item.get("domain") != expected_domain
                or item.get("uid_start") != policy.uid_start
                or item.get("uid_end") != policy.uid_end
                or not isinstance(occupied_ids, list)
                or any(
                    not _plain_integer(identity, minimum=policy.uid_start)
                    or identity > policy.uid_end
                    for identity in occupied_ids
                )
                or occupied_ids != sorted(set(occupied_ids))
                or DIGEST_RE.fullmatch(str(item.get("identity_inventory_sha256"))) is None
                or not _valid_timestamp(item.get("checked_at"))
                or checked_at > collected_at
                or collected_at - checked_at
                > timedelta(seconds=FLEET_IDENTITY_MAX_NODE_SKEW_SECONDS)
            ):
                raise RegistryError("fleet identity inventory node binding is invalid")
        return payload

    def _fleet_occupied_identities(self) -> set[int]:
        try:
            metadata = self.fleet_identity_inventory_path.lstat()
            if (
                not stat.S_ISREG(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_nlink != 1
                or (
                    self.system_mode
                    and (
                        (metadata.st_uid, metadata.st_gid) != (0, 0)
                        or stat.S_IMODE(metadata.st_mode) != 0o600
                    )
                )
            ):
                raise RegistryError("fleet identity inventory metadata is unsafe")
            raw = _read_regular(self.fleet_identity_inventory_path, limit=8 << 20)
            payload = self.verify_fleet_identity_inventory(raw, policy=self.policy)
        except FileNotFoundError as exc:
            raise RegistryError("fleet identity inventory is unavailable") from exc
        return {int(identity) for node in payload["nodes"] for identity in node["occupied_ids"]}

    def _allocate_uid(self, connection: sqlite3.Connection) -> int:
        used = {
            int(row["identity"])
            for row in connection.execute(
                "SELECT uid AS identity FROM environments "
                "UNION SELECT gid AS identity FROM environments"
            )
        }
        if self.require_fleet_identity_inventory:
            used.update(self._fleet_occupied_identities())
        else:
            try:
                used.update(
                    account.pw_uid
                    for account in pwd.getpwall()
                    if isinstance(account.pw_uid, int) and account.pw_uid >= 0
                )
                used.update(
                    group.gr_gid
                    for group in grp.getgrall()
                    if isinstance(group.gr_gid, int) and group.gr_gid >= 0
                )
            except (KeyError, OSError) as exc:
                raise RegistryError("host identity inventory is unavailable") from exc
        for identity in range(self.policy.uid_start, self.policy.uid_end + 1):
            if identity not in used:
                return identity
        raise RegistryError("registry UID allocation is exhausted")

    def _host_port_inventory(self) -> tuple[set[int], str]:
        collector = self.port_inventory_collector
        if collector is None:
            return set(), _digest({"reserved_ports": []})
        try:
            collected = collector()
        except RegistryError:
            raise
        except Exception as exc:
            raise RegistryError("host port inventory collection failed safely") from exc
        if not isinstance(collected, Set):
            raise RegistryError("host port inventory is malformed")
        ports = set(collected)
        if any(
            not isinstance(port, int) or isinstance(port, bool) or port < 1 or port > 65_535
            for port in ports
        ):
            raise RegistryError("host port inventory is malformed")
        ordered = sorted(ports)
        return ports, _digest({"reserved_ports": ordered})

    @staticmethod
    def _record_port_allocation(
        connection: sqlite3.Connection,
        *,
        env_id: str,
        action: str,
        expected_resource_generation: int,
        applied_resource_generation: int,
        registry_generation: int,
        prior_ports: Mapping[str, int],
        allocated_ports: Mapping[str, int],
        reserved_ports: set[int],
        reservation_inventory_sha256: str,
    ) -> None:
        if (
            action not in {"initial", "pre-deployment-reallocation"}
            or set(allocated_ports) != set(PORT_NAMES)
            or (prior_ports and set(prior_ports) != set(PORT_NAMES))
            or DIGEST_RE.fullmatch(reservation_inventory_sha256) is None
        ):
            raise RegistryError("port allocation journal binding is invalid")
        connection.execute(
            """
            INSERT INTO port_allocation_journal(
                env_id, action, expected_resource_generation,
                applied_resource_generation, registry_generation,
                prior_ports_json, allocated_ports_json, reserved_ports_json,
                reservation_inventory_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                env_id,
                action,
                expected_resource_generation,
                applied_resource_generation,
                registry_generation,
                json.dumps(dict(prior_ports), sort_keys=True, separators=(",", ":")),
                json.dumps(dict(allocated_ports), sort_keys=True, separators=(",", ":")),
                json.dumps(sorted(reserved_ports), separators=(",", ":")),
                reservation_inventory_sha256,
                _timestamp(),
            ),
        )

    def _allocate_ports(
        self,
        connection: sqlite3.Connection,
        *,
        reserved_ports: set[int],
        excluding_env_id: str | None = None,
    ) -> dict[str, int]:
        used = {
            int(row["port"])
            for row in connection.execute(
                "SELECT port FROM environment_ports"
                + (" WHERE env_id != ?" if excluding_env_id is not None else ""),
                (() if excluding_env_id is None else (excluding_env_id,)),
            )
        }
        used.update(reserved_ports)
        base = self.policy.port_start
        while base + max(PORT_OFFSETS.values()) <= self.policy.port_end:
            ports = {name: base + offset for name, offset in PORT_OFFSETS.items()}
            if not (set(ports.values()) & used):
                return ports
            base += self.policy.port_block_size
        raise RegistryError("registry port allocation is exhausted")

    def reconcile_predeployment_ports(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> EnvironmentRecord:
        """Repair one pristine dynamic allocation after a listener TOCTOU race."""

        if (
            ENV_ID_RE.fullmatch(env_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or not _plain_integer(expected_resource_generation, minimum=1)
        ):
            raise RegistryError("pre-deployment port recovery binding is invalid")
        if self.port_inventory_collector is None:
            return self.lookup(env_id, principal_id=principal_id)
        reserved_ports, inventory_sha256 = self._host_port_inventory()
        with self._transaction() as connection:
            environment = self._environment(connection, env_id)
            if environment.principal_id != principal_id:
                raise RegistryError("pre-deployment port recovery ownership is invalid")
            if environment.resource_generation != expected_resource_generation:
                raise RegistryError("pre-deployment port recovery generation is stale")
            deployments = connection.execute(
                """
                SELECT phase, failed_from_phase, applied_resource_generation,
                    applied_registry_generation, applied_registry_payload_sha256,
                    finalization_payload_sha256
                FROM deployments WHERE env_id = ?
                """,
                (env_id,),
            ).fetchall()
            safe_failed_history = all(
                row["phase"] == "failed"
                and row["failed_from_phase"] == "requested"
                and row["applied_resource_generation"] is None
                and row["applied_registry_generation"] is None
                and row["applied_registry_payload_sha256"] is None
                and row["finalization_payload_sha256"] is None
                for row in deployments
            )
            # Legacy seeds and every environment with deployment history keep
            # their published immutable identity. Only failures fenced while
            # still in the initial requested phase prove that no immutable
            # resource publication was accepted.
            if (
                environment.layout_version != "dynamic-v1"
                or environment.state != "ready"
                or environment.current_candidate_id is not None
                or not safe_failed_history
            ):
                return environment
            if not (set(environment.ports.values()) & reserved_ports):
                return environment
            ports = self._allocate_ports(
                connection,
                reserved_ports=reserved_ports,
                excluding_env_id=env_id,
            )
            connection.execute("DELETE FROM environment_ports WHERE env_id = ?", (env_id,))
            connection.executemany(
                "INSERT INTO environment_ports(env_id, name, port) VALUES (?, ?, ?)",
                ((env_id, name, port) for name, port in sorted(ports.items())),
            )
            applied_generation = environment.resource_generation + 1
            connection.execute(
                "UPDATE environments SET resource_generation = ? WHERE env_id = ?",
                (applied_generation, env_id),
            )
            registry_generation = self._bump_generation(connection)
            self._record_port_allocation(
                connection,
                env_id=env_id,
                action="pre-deployment-reallocation",
                expected_resource_generation=environment.resource_generation,
                applied_resource_generation=applied_generation,
                registry_generation=registry_generation,
                prior_ports=environment.ports,
                allocated_ports=ports,
                reserved_ports=reserved_ports,
                reservation_inventory_sha256=inventory_sha256,
            )
            return self._environment(connection, env_id)

    @staticmethod
    def _dynamic_resources(env_id: str, runtime_id: str) -> dict[str, str]:
        token = runtime_id.removeprefix("e-")
        service = f"loom-e-{token}"
        compose = f"loom-env-{token}"
        bucket = f"loom-e-{token}"
        return {
            "service_user": service,
            "service_group": service,
            "compose_project": compose,
            "systemd_instance": runtime_id,
            "candidate_root": f"/shared_work/loom/candidates/environments/{env_id}",
            "runtime_root": f"/shared_work/loom/runtime/environments/{env_id}",
            "state_root": f"/srv/loom/developer-environments/{env_id}",
            "evidence_root": f"/srv/loom/developer-environments/{env_id}/evidence",
            "database_name": f"loom_env_{token}",
            "postgres_volume": f"{compose}_postgres",
            "minio_volume": f"{compose}_minio",
            "task_bucket": f"{bucket}-tasks",
            "trajectories_bucket": f"{bucket}-trajectories",
            "artifacts_bucket": f"{bucket}-artifacts",
            "provider_namespace": f"environment-{env_id}",
            "slurm_user": service,
            "slurm_account": f"lda-{token}",
            "slurm_qos": f"ldq-{token}",
            "cgroup_slice": f"loom-dev-{token}.slice",
        }

    def register(self, payload: Mapping[str, Any]) -> EnvironmentRecord:
        request = _closed_request(payload, fields=REGISTER_FIELDS, kind=REGISTER_KIND)
        principal_id = str(request["principal_id"])
        key = str(request["idempotency_key"])
        display_name = _validate_display_name(request["display_name"])
        request_digest = _digest(request)
        with self._transaction() as connection:
            replay = connection.execute(
                "SELECT request_digest, env_id FROM registration_requests "
                "WHERE principal_id = ? AND idempotency_key = ?",
                (principal_id, key),
            ).fetchone()
            if replay is not None:
                if replay["request_digest"] != request_digest:
                    raise RegistryError("registration idempotency key conflicts")
                return self._environment(connection, str(replay["env_id"]))

            existing = connection.execute(
                "SELECT env_id, display_name FROM environments WHERE principal_id = ?",
                (principal_id,),
            ).fetchone()
            if existing is not None:
                env_id = str(existing["env_id"])
                if existing["display_name"] != display_name:
                    connection.execute(
                        "UPDATE environments SET display_name = ? WHERE env_id = ?",
                        (display_name, env_id),
                    )
                connection.execute(
                    "INSERT INTO registration_requests VALUES (?, ?, ?, ?)",
                    (principal_id, key, request_digest, env_id),
                )
                self._bump_generation(connection)
                return self._environment(connection, env_id)

            env_id = self._new_identifier(
                connection,
                table="environments",
                column="env_id",
                prefix="denv-",
                token_bytes=16,
            )
            runtime_id = self._new_identifier(
                connection,
                table="environments",
                column="runtime_id",
                prefix="e-",
                token_bytes=8,
            )
            uid = self._allocate_uid(connection)
            reserved_ports, reservation_inventory_sha256 = self._host_port_inventory()
            ports = self._allocate_ports(
                connection,
                reserved_ports=reserved_ports,
            )
            resources = self._dynamic_resources(env_id, runtime_id)
            created_at = _timestamp()
            connection.execute(
                """
                INSERT INTO environments (
                    env_id, principal_id, display_name, layout_version, runtime_id,
                    state, resource_generation, lifecycle_epoch,
                    service_user, service_group, uid, gid,
                    compose_project, systemd_instance, candidate_root, runtime_root,
                    state_root, evidence_root, database_name, postgres_volume,
                    minio_volume, task_bucket, trajectories_bucket, artifacts_bucket,
                    provider_namespace, slurm_user, slurm_account, slurm_qos,
                    cgroup_slice, current_candidate_id, created_at
                ) VALUES (
                    ?, ?, ?, 'dynamic-v1', ?, 'ready', 1, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?
                )
                """,
                (
                    env_id,
                    principal_id,
                    display_name,
                    runtime_id,
                    resources["service_user"],
                    resources["service_group"],
                    uid,
                    uid,
                    resources["compose_project"],
                    resources["systemd_instance"],
                    resources["candidate_root"],
                    resources["runtime_root"],
                    resources["state_root"],
                    resources["evidence_root"],
                    resources["database_name"],
                    resources["postgres_volume"],
                    resources["minio_volume"],
                    resources["task_bucket"],
                    resources["trajectories_bucket"],
                    resources["artifacts_bucket"],
                    resources["provider_namespace"],
                    resources["slurm_user"],
                    resources["slurm_account"],
                    resources["slurm_qos"],
                    resources["cgroup_slice"],
                    created_at,
                ),
            )
            connection.executemany(
                "INSERT INTO environment_ports(env_id, name, port) VALUES (?, ?, ?)",
                ((env_id, name, port) for name, port in sorted(ports.items())),
            )
            connection.execute(
                "INSERT INTO registration_requests VALUES (?, ?, ?, ?)",
                (principal_id, key, request_digest, env_id),
            )
            registry_generation = self._bump_generation(connection)
            self._record_port_allocation(
                connection,
                env_id=env_id,
                action="initial",
                expected_resource_generation=1,
                applied_resource_generation=1,
                registry_generation=registry_generation,
                prior_ports={},
                allocated_ports=ports,
                reserved_ports=reserved_ports,
                reservation_inventory_sha256=reservation_inventory_sha256,
            )
            return self._environment(connection, env_id)

    def registration_idempotency_replay(
        self,
        *,
        principal_id: str,
        idempotency_key: str,
    ) -> bool:
        """Report whether an exact registration key was already consumed."""

        if (
            PRINCIPAL_RE.fullmatch(principal_id) is None
            or IDEMPOTENCY_RE.fullmatch(idempotency_key) is None
        ):
            raise RegistryError("registration replay lookup is invalid")
        with self._transaction(immediate=False) as connection:
            return (
                connection.execute(
                    """
                    SELECT 1 FROM registration_requests
                    WHERE principal_id = ? AND idempotency_key = ?
                    """,
                    (principal_id, idempotency_key),
                ).fetchone()
                is not None
            )

    @staticmethod
    def _environment(connection: sqlite3.Connection, env_id: str) -> EnvironmentRecord:
        row = connection.execute(
            "SELECT * FROM environments WHERE env_id = ?",
            (env_id,),
        ).fetchone()
        if row is None:
            raise RegistryError("developer environment is unavailable")
        ports = {
            str(port["name"]): int(port["port"])
            for port in connection.execute(
                "SELECT name, port FROM environment_ports WHERE env_id = ? ORDER BY name",
                (env_id,),
            )
        }
        if set(ports) != set(PORT_NAMES):
            raise RegistryError("developer environment port inventory is invalid")
        values = _row_mapping(row)
        environment = EnvironmentRecord(
            env_id=str(values["env_id"]),
            principal_id=str(values["principal_id"]),
            display_name=str(values["display_name"]),
            layout_version=str(values["layout_version"]),
            runtime_id=str(values["runtime_id"]),
            state=str(values["state"]),
            resource_generation=int(values["resource_generation"]),
            lifecycle_epoch=int(values["lifecycle_epoch"]),
            service_user=str(values["service_user"]),
            service_group=str(values["service_group"]),
            uid=int(values["uid"]),
            gid=int(values["gid"]),
            ports=ports,
            compose_project=str(values["compose_project"]),
            systemd_instance=str(values["systemd_instance"]),
            candidate_root=str(values["candidate_root"]),
            runtime_root=str(values["runtime_root"]),
            state_root=str(values["state_root"]),
            evidence_root=str(values["evidence_root"]),
            database_name=str(values["database_name"]),
            postgres_volume=str(values["postgres_volume"]),
            minio_volume=str(values["minio_volume"]),
            task_bucket=str(values["task_bucket"]),
            trajectories_bucket=str(values["trajectories_bucket"]),
            artifacts_bucket=str(values["artifacts_bucket"]),
            provider_namespace=str(values["provider_namespace"]),
            slurm_user=str(values["slurm_user"]),
            slurm_account=str(values["slurm_account"]),
            slurm_qos=str(values["slurm_qos"]),
            cgroup_slice=str(values["cgroup_slice"]),
            current_candidate_id=(
                None
                if values["current_candidate_id"] is None
                else str(values["current_candidate_id"])
            ),
            created_at=str(values["created_at"]),
        )
        if (
            ENV_ID_RE.fullmatch(environment.env_id) is None
            or PRINCIPAL_RE.fullmatch(environment.principal_id) is None
            or RUNTIME_ID_RE.fullmatch(environment.runtime_id) is None
            or environment.layout_version not in {"dynamic-v1", "legacy-v1"}
            or environment.state not in {"ready", "deploying", "active", "retired", "quarantined"}
            or environment.resource_generation < 1
            or environment.lifecycle_epoch < 1
            or environment.uid < 1
            or environment.uid != environment.gid
            or len(set(environment.ports.values())) != len(PORT_NAMES)
            or any(not 1024 <= port <= 65_535 for port in environment.ports.values())
            or (
                environment.current_candidate_id is not None
                and CANDIDATE_ID_RE.fullmatch(environment.current_candidate_id) is None
            )
        ):
            raise RegistryError("developer environment binding is invalid")
        if environment.layout_version == "dynamic-v1":
            expected = DeveloperEnvironmentRegistry._dynamic_resources(
                environment.env_id,
                environment.runtime_id,
            )
            if any(getattr(environment, field) != value for field, value in expected.items()):
                raise RegistryError("developer environment resource binding is invalid")
        return environment

    def lookup(
        self,
        env_id: str,
        *,
        principal_id: str | None = None,
    ) -> EnvironmentRecord:
        if ENV_ID_RE.fullmatch(env_id) is None:
            raise RegistryError("developer environment identifier is invalid")
        with self._transaction(immediate=False) as connection:
            environment = self._environment(connection, env_id)
            if principal_id is not None and environment.principal_id != principal_id:
                raise RegistryError("developer environment ownership is invalid")
            return environment

    def list_environments(
        self, *, principal_id: str | None = None
    ) -> tuple[EnvironmentRecord, ...]:
        if principal_id is not None and PRINCIPAL_RE.fullmatch(principal_id) is None:
            raise RegistryError("developer principal is invalid")
        with self._transaction(immediate=False) as connection:
            if principal_id is None:
                rows = connection.execute("SELECT env_id FROM environments ORDER BY env_id")
            else:
                rows = connection.execute(
                    "SELECT env_id FROM environments WHERE principal_id = ? ORDER BY env_id",
                    (principal_id,),
                )
            return tuple(self._environment(connection, str(row["env_id"])) for row in rows)

    def import_legacy_seed(self, path: Path = SYSTEM_SEED) -> tuple[EnvironmentRecord, ...]:
        """Import trusted migration data; this method is not a developer API."""

        try:
            raw = _read_regular(path, limit=1024 * 1024)
            payload = tomllib.loads(raw.decode("utf-8"))
        except (RegistryError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
            raise RegistryError("legacy registry seed is unavailable or invalid") from exc
        if (
            set(payload) != {"schema_version", "kind", "environment"}
            or payload.get("schema_version") != SCHEMA_VERSION
            or payload.get("kind") != SEED_KIND
            or not isinstance(payload.get("environment"), list)
            or len(payload["environment"]) != 3
            or {item.get("runtime_id") for item in payload["environment"] if isinstance(item, dict)}
            != {"qianyi", "hongjian", "devansh"}
        ):
            raise RegistryError("legacy registry seed shape is invalid")
        try:
            owner_bindings = {
                str(item["owner_username"]): pwd.getpwnam(str(item["owner_username"])).pw_uid
                for item in payload["environment"]
                if isinstance(item, dict) and "owner_username" in item
            }
        except (KeyError, OSError) as exc:
            raise RegistryError("legacy registry owner is unavailable") from exc
        if (
            set(owner_bindings) != {"qianyi", "hongjian", "devansh"}
            or any(
                not isinstance(identity, int) or identity < 0
                for identity in owner_bindings.values()
            )
            or len(set(owner_bindings.values())) != 3
        ):
            raise RegistryError("legacy registry owner binding is invalid")
        for item in payload["environment"]:
            if not isinstance(item, dict):
                raise RegistryError("legacy registry environment shape is invalid")
            service_user = item.get("service_user")
            service_group = item.get("service_group")
            uid = item.get("uid")
            gid = item.get("gid")
            if (
                not isinstance(service_user, str)
                or not isinstance(service_group, str)
                or not isinstance(uid, int)
                or isinstance(uid, bool)
                or not isinstance(gid, int)
                or isinstance(gid, bool)
            ):
                raise RegistryError("legacy registry service identity is invalid")
            _validate_host_user_binding(service_user, uid)
            _validate_host_group_binding(service_group, gid)
        inserted = False
        env_ids: list[str] = []
        with self._transaction() as connection:
            for raw_environment in payload["environment"]:
                if not isinstance(raw_environment, dict):
                    raise RegistryError("legacy registry environment shape is invalid")
                owner_username = str(raw_environment.get("owner_username"))
                normalized = self._validate_legacy_environment(
                    raw_environment,
                    principal_id=f"unix-uid:{owner_bindings[owner_username]}",
                )
                env_id = str(normalized["env_id"])
                env_ids.append(env_id)
                existing = connection.execute(
                    "SELECT env_id FROM environments WHERE env_id = ? OR principal_id = ?",
                    (env_id, normalized["principal_id"]),
                ).fetchone()
                if existing is not None:
                    current = self._environment(connection, str(existing["env_id"]))
                    if asdict(current) != normalized:
                        raise RegistryError("legacy registry seed conflicts with existing state")
                    continue
                ports = normalized.pop("ports")
                connection.execute(
                    """
                    INSERT INTO environments (
                        env_id, principal_id, display_name, layout_version, runtime_id,
                        state, resource_generation, lifecycle_epoch,
                        service_user, service_group, uid, gid,
                        compose_project, systemd_instance, candidate_root, runtime_root,
                        state_root, evidence_root, database_name, postgres_volume,
                        minio_volume, task_bucket, trajectories_bucket, artifacts_bucket,
                        provider_namespace, slurm_user, slurm_account, slurm_qos,
                        cgroup_slice, current_candidate_id, created_at
                    ) VALUES (
                        :env_id, :principal_id, :display_name, :layout_version, :runtime_id,
                        :state, :resource_generation, :lifecycle_epoch,
                        :service_user, :service_group, :uid, :gid,
                        :compose_project, :systemd_instance, :candidate_root, :runtime_root,
                        :state_root, :evidence_root, :database_name, :postgres_volume,
                        :minio_volume, :task_bucket, :trajectories_bucket, :artifacts_bucket,
                        :provider_namespace, :slurm_user, :slurm_account, :slurm_qos,
                        :cgroup_slice, :current_candidate_id, :created_at
                    )
                    """,
                    normalized,
                )
                connection.executemany(
                    "INSERT INTO environment_ports(env_id, name, port) VALUES (?, ?, ?)",
                    ((env_id, name, port) for name, port in sorted(ports.items())),
                )
                inserted = True
            if inserted:
                self._bump_generation(connection)
            return tuple(self._environment(connection, env_id) for env_id in env_ids)

    @staticmethod
    def _validate_legacy_environment(
        value: object,
        *,
        principal_id: str,
    ) -> dict[str, Any]:
        fields = {
            "env_id",
            "owner_username",
            "display_name",
            "runtime_id",
            "service_user",
            "service_group",
            "uid",
            "gid",
            "ports",
            "compose_project",
            "systemd_instance",
            "candidate_root",
            "runtime_root",
            "state_root",
            "evidence_root",
            "database_name",
            "postgres_volume",
            "minio_volume",
            "task_bucket",
            "trajectories_bucket",
            "artifacts_bucket",
            "provider_namespace",
            "slurm_user",
            "slurm_account",
            "slurm_qos",
            "cgroup_slice",
            "created_at",
        }
        if not isinstance(value, dict) or set(value) != fields:
            raise RegistryError("legacy registry environment shape is invalid")
        payload = dict(value)
        owner_username = payload.pop("owner_username", None)
        ports = payload.get("ports")
        if (
            ENV_ID_RE.fullmatch(str(payload.get("env_id"))) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or RUNTIME_ID_RE.fullmatch(str(payload.get("runtime_id"))) is None
            or owner_username != payload.get("runtime_id")
            or not isinstance(payload.get("uid"), int)
            or isinstance(payload.get("uid"), bool)
            or not isinstance(payload.get("gid"), int)
            or isinstance(payload.get("gid"), bool)
            or int(payload["uid"]) < 1
            or int(payload["gid"]) < 1
            or payload["uid"] != payload["gid"]
            or not isinstance(ports, dict)
            or set(ports) != set(PORT_NAMES)
            or any(
                not isinstance(port, int) or isinstance(port, bool) or not 1024 <= port <= 65_535
                for port in ports.values()
            )
            or len(set(ports.values())) != len(ports)
        ):
            raise RegistryError("legacy registry environment binding is invalid")
        _validate_display_name(payload["display_name"])
        for field in (
            "service_user",
            "service_group",
            "compose_project",
            "systemd_instance",
            "database_name",
            "postgres_volume",
            "minio_volume",
            "provider_namespace",
            "slurm_user",
            "slurm_account",
            "slurm_qos",
        ):
            if SAFE_NAME_RE.fullmatch(str(payload[field])) is None:
                raise RegistryError("legacy registry resource name is invalid")
        for field in ("task_bucket", "trajectories_bucket", "artifacts_bucket"):
            if SAFE_BUCKET_RE.fullmatch(str(payload[field])) is None:
                raise RegistryError("legacy registry bucket name is invalid")
        runtime_id = str(payload["runtime_id"])
        expected_paths = {
            "candidate_root": f"/shared_work/loom/candidates/sandboxes/{runtime_id}",
            "runtime_root": f"/shared_work/loom/runtime/sandboxes/{runtime_id}",
            "state_root": f"/srv/loom/developer-sandboxes/{runtime_id}",
            "evidence_root": f"/srv/loom/developer-sandboxes/{runtime_id}/evidence",
        }
        if any(payload[field] != expected for field, expected in expected_paths.items()):
            raise RegistryError("legacy registry fixed root binding is invalid")
        expected_names = {
            "service_user": f"loom-sandbox-{runtime_id}",
            "service_group": f"loom-sandbox-{runtime_id}",
            "compose_project": f"loom-sandbox-{runtime_id}",
            "systemd_instance": runtime_id,
            "database_name": f"loom_sandbox_{runtime_id}",
            "postgres_volume": f"loom-sandbox-{runtime_id}_postgres_data",
            "minio_volume": f"loom-sandbox-{runtime_id}_minio_data",
            "task_bucket": f"loom-sandbox-{runtime_id}-tasks",
            "trajectories_bucket": f"loom-sandbox-{runtime_id}-trajectories",
            "artifacts_bucket": f"loom-sandbox-{runtime_id}-artifacts",
            "provider_namespace": f"sandbox-{runtime_id}",
            "slurm_user": f"loom-sandbox-{runtime_id}",
            "slurm_account": f"loom-dev-{runtime_id}",
            "slurm_qos": f"loom-dev-{runtime_id}",
            "cgroup_slice": f"loom-dev-{runtime_id}.slice",
        }
        if any(payload[field] != expected for field, expected in expected_names.items()):
            raise RegistryError("legacy registry resource binding is invalid")
        return {
            **payload,
            "principal_id": principal_id,
            "layout_version": "legacy-v1",
            "state": "ready",
            "resource_generation": 1,
            "lifecycle_epoch": 1,
            "current_candidate_id": None,
        }

    def import_candidate(self, payload: Mapping[str, Any]) -> CandidateRecord:
        request = _closed_request(payload, fields=CANDIDATE_FIELDS, kind=CANDIDATE_KIND)
        principal_id = str(request["principal_id"])
        key = str(request["idempotency_key"])
        env_id = str(request["env_id"])
        image_digests = request.get("image_digests")
        image_archives = request.get("image_archives")
        if (
            ENV_ID_RE.fullmatch(env_id) is None
            or SHA_RE.fullmatch(str(request.get("candidate_sha"))) is None
            or SHA_RE.fullmatch(str(request.get("candidate_tree"))) is None
            or DIGEST_RE.fullmatch(str(request.get("bundle_sha256"))) is None
            or not isinstance(request.get("bundle_size"), int)
            or isinstance(request.get("bundle_size"), bool)
            or not 0 < int(request["bundle_size"]) <= self.policy.max_bundle_bytes
            or not _valid_worker_image_ids(image_digests)
            or not _valid_worker_image_archives(image_archives)
            or not _worker_archive_configs_match(image_digests, image_archives)
        ):
            raise RegistryError("candidate import binding is invalid")
        request_digest = _digest(request)
        with self._transaction() as connection:
            replay = connection.execute(
                "SELECT request_digest, candidate_id FROM candidate_requests "
                "WHERE principal_id = ? AND idempotency_key = ?",
                (principal_id, key),
            ).fetchone()
            if replay is not None:
                if replay["request_digest"] != request_digest:
                    raise RegistryError("candidate idempotency key conflicts")
                replayed = self._candidate(connection, str(replay["candidate_id"]))
                environment = self._environment(connection, replayed.env_id)
                if (
                    environment.state not in {"ready", "active"}
                    or replayed.lifecycle_epoch != environment.lifecycle_epoch
                ):
                    raise RegistryError("candidate replay belongs to a retired lifecycle")
                return replayed
            environment = self._environment(connection, env_id)
            if environment.principal_id != principal_id:
                raise RegistryError("candidate ownership is invalid")
            if environment.state not in {"ready", "active"}:
                raise RegistryError("candidate environment state is invalid")
            same_content = connection.execute(
                """
                SELECT candidate_id, lifecycle_epoch
                FROM candidates
                WHERE env_id = ? AND (
                    candidate_sha = ? OR candidate_tree = ? OR bundle_sha256 = ?
                )
                """,
                (
                    env_id,
                    request["candidate_sha"],
                    request["candidate_tree"],
                    request["bundle_sha256"],
                ),
            ).fetchone()
            if (
                same_content is not None
                and int(same_content["lifecycle_epoch"]) != environment.lifecycle_epoch
            ):
                raise RegistryError("candidate content belongs to a retired lifecycle")
            identity = {
                "principal_id": principal_id,
                "env_id": env_id,
                "lifecycle_epoch": environment.lifecycle_epoch,
                "repository_id": "qianyi-sun/loom",
                "image_binding_kind": WORKER_IMAGE_BINDING_KIND,
                "image_binding_version": WORKER_IMAGE_BINDING_VERSION,
                "candidate_sha": request["candidate_sha"],
                "candidate_tree": request["candidate_tree"],
                "bundle_sha256": request["bundle_sha256"],
                "image_digests": image_digests,
                "image_archives": image_archives,
            }
            candidate_id = "cand-" + _digest(identity)[:40]
            existing = connection.execute(
                "SELECT candidate_id FROM candidates WHERE candidate_id = ?",
                (candidate_id,),
            ).fetchone()
            if existing is None:
                connection.execute(
                    """
                    INSERT INTO candidates (
                        candidate_id, principal_id, env_id, lifecycle_epoch,
                        repository_id, image_binding_kind, image_binding_version,
                        candidate_sha, candidate_tree, bundle_sha256, bundle_size,
                        image_digests_json, image_archives_json, imported_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        candidate_id,
                        principal_id,
                        env_id,
                        environment.lifecycle_epoch,
                        "qianyi-sun/loom",
                        WORKER_IMAGE_BINDING_KIND,
                        WORKER_IMAGE_BINDING_VERSION,
                        request["candidate_sha"],
                        request["candidate_tree"],
                        request["bundle_sha256"],
                        request["bundle_size"],
                        json.dumps(image_digests, sort_keys=True, separators=(",", ":")),
                        json.dumps(image_archives, sort_keys=True, separators=(",", ":")),
                        _timestamp(),
                    ),
                )
            else:
                bound = self._candidate(connection, candidate_id)
                if (
                    bound.bundle_size != request["bundle_size"]
                    or bound.image_digests != image_digests
                    or _archive_identity(bound.image_archives) != image_archives
                ):
                    raise RegistryError("candidate metadata conflicts with existing identity")
            connection.execute(
                "INSERT INTO candidate_requests VALUES (?, ?, ?, ?)",
                (principal_id, key, request_digest, candidate_id),
            )
            self._bump_generation(connection)
            return self._candidate(connection, candidate_id)

    def validate_revival_candidate_content(
        self,
        env_id: str,
        *,
        principal_id: str,
        candidate_sha: str,
        candidate_tree: str,
        bundle_sha256: str,
    ) -> None:
        """Validate new content without changing a retired lifecycle."""

        if (
            ENV_ID_RE.fullmatch(env_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or SHA_RE.fullmatch(candidate_sha) is None
            or SHA_RE.fullmatch(candidate_tree) is None
            or DIGEST_RE.fullmatch(bundle_sha256) is None
        ):
            raise RegistryError("revival candidate binding is invalid")
        with self._transaction(immediate=False) as connection:
            environment = self._environment(connection, env_id)
            if environment.principal_id != principal_id:
                raise RegistryError("revival candidate ownership is invalid")
            if environment.state != "retired":
                raise RegistryError("revival candidate environment is not retired")
            prior = connection.execute(
                """
                SELECT 1 FROM candidates
                WHERE env_id = ? AND (
                    candidate_sha = ? OR candidate_tree = ? OR bundle_sha256 = ?
                )
                LIMIT 1
                """,
                (env_id, candidate_sha, candidate_tree, bundle_sha256),
            ).fetchone()
            if prior is not None:
                raise RegistryError("revival candidate content is not new")

    def _candidate(
        self,
        connection: sqlite3.Connection,
        candidate_id: str,
    ) -> CandidateRecord:
        row = connection.execute(
            "SELECT * FROM candidates WHERE candidate_id = ?",
            (candidate_id,),
        ).fetchone()
        if row is None:
            raise RegistryError("developer candidate is unavailable")
        try:
            image_digests = json.loads(str(row["image_digests_json"]))
            stored_archives = json.loads(str(row["image_archives_json"]))
        except json.JSONDecodeError as exc:
            raise RegistryError("developer candidate image binding is invalid") from exc
        if not isinstance(image_digests, dict) or not _valid_worker_image_archives(stored_archives):
            raise RegistryError("developer candidate image binding is invalid")
        image_archives = {
            architecture: {
                **{
                    field: stored_archives[architecture][field]
                    for field in WORKER_IMAGE_ARCHIVE_BINDING_FIELDS
                },
                "path": str(
                    worker_image_archive_path(
                        self.candidate_root,
                        candidate_id,
                        architecture,
                    )
                ),
            }
            for architecture in ("amd64", "arm64")
        }
        candidate = CandidateRecord(
            candidate_id=str(row["candidate_id"]),
            principal_id=str(row["principal_id"]),
            env_id=str(row["env_id"]),
            lifecycle_epoch=int(row["lifecycle_epoch"]),
            repository_id=str(row["repository_id"]),
            image_binding_kind=str(row["image_binding_kind"]),
            image_binding_version=int(row["image_binding_version"]),
            candidate_sha=str(row["candidate_sha"]),
            candidate_tree=str(row["candidate_tree"]),
            bundle_sha256=str(row["bundle_sha256"]),
            bundle_size=int(row["bundle_size"]),
            bundle_path=str(self.candidate_root / candidate_id / "candidate.bundle"),
            image_digests={str(key): str(value) for key, value in image_digests.items()},
            image_archives=image_archives,
            imported_at=str(row["imported_at"]),
        )
        if (
            CANDIDATE_ID_RE.fullmatch(candidate.candidate_id) is None
            or PRINCIPAL_RE.fullmatch(candidate.principal_id) is None
            or ENV_ID_RE.fullmatch(candidate.env_id) is None
            or candidate.lifecycle_epoch < 1
            or candidate.repository_id != "qianyi-sun/loom"
            or candidate.image_binding_kind != WORKER_IMAGE_BINDING_KIND
            or candidate.image_binding_version != WORKER_IMAGE_BINDING_VERSION
            or SHA_RE.fullmatch(candidate.candidate_sha) is None
            or SHA_RE.fullmatch(candidate.candidate_tree) is None
            or DIGEST_RE.fullmatch(candidate.bundle_sha256) is None
            or candidate.bundle_size < 1
            or candidate.bundle_path
            != str(self.candidate_root / candidate.candidate_id / "candidate.bundle")
            or not _valid_worker_image_ids(candidate.image_digests)
            or not _valid_worker_image_archives(
                candidate.image_archives,
                candidate_root=self.candidate_root,
                candidate_id=candidate.candidate_id,
            )
            or not _worker_archive_configs_match(
                candidate.image_digests,
                candidate.image_archives,
            )
        ):
            raise RegistryError("developer candidate binding is invalid")
        return candidate

    def lookup_candidate(
        self,
        candidate_id: str,
        *,
        principal_id: str | None = None,
        env_id: str | None = None,
    ) -> CandidateRecord:
        if (
            CANDIDATE_ID_RE.fullmatch(candidate_id) is None
            or (principal_id is not None and PRINCIPAL_RE.fullmatch(principal_id) is None)
            or (env_id is not None and ENV_ID_RE.fullmatch(env_id) is None)
        ):
            raise RegistryError("developer candidate lookup is invalid")
        with self._transaction(immediate=False) as connection:
            candidate = self._candidate(connection, candidate_id)
            if (principal_id is not None and candidate.principal_id != principal_id) or (
                env_id is not None and candidate.env_id != env_id
            ):
                raise RegistryError("developer candidate ownership is invalid")
            return candidate

    def begin_deployment(self, payload: Mapping[str, Any]) -> DeploymentRecord:
        request = _closed_request(payload, fields=DEPLOY_FIELDS, kind=DEPLOY_KIND)
        principal_id = str(request["principal_id"])
        key = str(request["idempotency_key"])
        env_id = str(request["env_id"])
        candidate_id = str(request["candidate_id"])
        expected_generation = request.get("expected_resource_generation")
        if (
            ENV_ID_RE.fullmatch(env_id) is None
            or CANDIDATE_ID_RE.fullmatch(candidate_id) is None
            or not isinstance(expected_generation, int)
            or isinstance(expected_generation, bool)
            or expected_generation < 1
        ):
            raise RegistryError("deployment request binding is invalid")
        request_digest = _digest(request)
        with self._transaction() as connection:
            replay = connection.execute(
                "SELECT request_digest, deployment_id FROM deployment_requests "
                "WHERE principal_id = ? AND idempotency_key = ?",
                (principal_id, key),
            ).fetchone()
            if replay is not None:
                if replay["request_digest"] != request_digest:
                    raise RegistryError("deployment idempotency key conflicts")
                return self._deployment(connection, str(replay["deployment_id"]))
            environment = self._environment(connection, env_id)
            candidate = self._candidate(connection, candidate_id)
            if (
                environment.principal_id != principal_id
                or candidate.principal_id != principal_id
                or candidate.env_id != env_id
            ):
                raise RegistryError("deployment ownership is invalid")
            if candidate.lifecycle_epoch != environment.lifecycle_epoch:
                raise RegistryError("deployment candidate lifecycle is stale")
            if environment.state not in {"ready", "active"}:
                raise RegistryError("deployment environment state is invalid")
            if environment.resource_generation != expected_generation:
                raise RegistryError("deployment resource generation is stale")
            active = connection.execute(
                "SELECT 1 FROM deployments WHERE env_id = ? "
                "AND phase NOT IN ('committed', 'failed')",
                (env_id,),
            ).fetchone()
            if active is not None:
                raise RegistryError("developer environment already has an active deployment")
            deployment_id = self._new_identifier(
                connection,
                table="deployments",
                column="deployment_id",
                prefix="dep-",
                token_bytes=16,
            )
            now = _timestamp()
            connection.execute(
                """
                INSERT INTO deployments(
                    deployment_id, principal_id, env_id, candidate_id,
                    expected_resource_generation, applied_resource_generation,
                    applied_registry_generation, applied_registry_payload_sha256,
                    finalization_payload_sha256, worker_runtime_bindings_json,
                    phase, previous_candidate_id,
                    request_digest, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL,
                    'requested', ?, ?, ?, ?)
                """,
                (
                    deployment_id,
                    principal_id,
                    env_id,
                    candidate_id,
                    expected_generation,
                    environment.current_candidate_id,
                    request_digest,
                    now,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO deployment_requests VALUES (?, ?, ?, ?)",
                (principal_id, key, request_digest, deployment_id),
            )
            connection.execute(
                "UPDATE environments SET state = 'deploying' WHERE env_id = ?",
                (env_id,),
            )
            self._bump_generation(connection)
            return self._deployment(connection, deployment_id)

    @staticmethod
    def _deployment(connection: sqlite3.Connection, deployment_id: str) -> DeploymentRecord:
        row = connection.execute(
            "SELECT * FROM deployments WHERE deployment_id = ?",
            (deployment_id,),
        ).fetchone()
        if row is None:
            raise RegistryError("developer deployment is unavailable")
        try:
            worker_runtime_bindings = (
                None
                if row["worker_runtime_bindings_json"] is None
                else json.loads(str(row["worker_runtime_bindings_json"]))
            )
        except json.JSONDecodeError as exc:
            raise RegistryError("developer deployment runtime binding is invalid") from exc
        deployment = DeploymentRecord(
            deployment_id=str(row["deployment_id"]),
            principal_id=str(row["principal_id"]),
            env_id=str(row["env_id"]),
            candidate_id=str(row["candidate_id"]),
            expected_resource_generation=int(row["expected_resource_generation"]),
            applied_resource_generation=(
                None
                if row["applied_resource_generation"] is None
                else int(row["applied_resource_generation"])
            ),
            applied_registry_generation=(
                None
                if row["applied_registry_generation"] is None
                else int(row["applied_registry_generation"])
            ),
            applied_registry_payload_sha256=(
                None
                if row["applied_registry_payload_sha256"] is None
                else str(row["applied_registry_payload_sha256"])
            ),
            finalization_payload_sha256=(
                None
                if row["finalization_payload_sha256"] is None
                else str(row["finalization_payload_sha256"])
            ),
            worker_runtime_bindings=worker_runtime_bindings,
            phase=str(row["phase"]),
            previous_candidate_id=(
                None if row["previous_candidate_id"] is None else str(row["previous_candidate_id"])
            ),
            request_digest=str(row["request_digest"]),
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )
        if (
            DEPLOYMENT_ID_RE.fullmatch(deployment.deployment_id) is None
            or PRINCIPAL_RE.fullmatch(deployment.principal_id) is None
            or ENV_ID_RE.fullmatch(deployment.env_id) is None
            or CANDIDATE_ID_RE.fullmatch(deployment.candidate_id) is None
            or deployment.expected_resource_generation < 1
            or (
                deployment.worker_runtime_bindings is not None
                and not _valid_worker_runtime_bindings(deployment.worker_runtime_bindings)
            )
            or (
                deployment.phase not in {"requested", "failed"}
                and deployment.worker_runtime_bindings is None
            )
            or deployment.phase not in {*DEPLOY_PHASES, "failed"}
            or (
                deployment.phase == "committed"
                and (
                    deployment.applied_resource_generation
                    != deployment.expected_resource_generation + 1
                    or deployment.applied_registry_generation is None
                    or deployment.applied_registry_generation < 1
                    or deployment.applied_registry_payload_sha256 is None
                    or DIGEST_RE.fullmatch(deployment.applied_registry_payload_sha256) is None
                    or deployment.finalization_payload_sha256 is None
                    or DIGEST_RE.fullmatch(deployment.finalization_payload_sha256) is None
                )
            )
            or (
                deployment.phase not in {"verified", "committed"}
                and (
                    deployment.applied_resource_generation is not None
                    or deployment.applied_registry_generation is not None
                    or deployment.applied_registry_payload_sha256 is not None
                    or deployment.finalization_payload_sha256 is not None
                )
            )
            or (
                deployment.phase == "verified"
                and deployment.finalization_payload_sha256 is not None
                and DIGEST_RE.fullmatch(deployment.finalization_payload_sha256) is None
            )
            or (
                deployment.phase == "verified"
                and (
                    (
                        deployment.applied_resource_generation is None
                        and deployment.applied_registry_generation is None
                        and deployment.applied_registry_payload_sha256 is None
                    )
                    is False
                    and (
                        deployment.applied_resource_generation
                        != deployment.expected_resource_generation + 1
                        or deployment.applied_registry_generation is None
                        or deployment.applied_registry_generation < 1
                        or deployment.applied_registry_payload_sha256 is None
                        or DIGEST_RE.fullmatch(deployment.applied_registry_payload_sha256) is None
                    )
                )
            )
            or (
                deployment.previous_candidate_id is not None
                and CANDIDATE_ID_RE.fullmatch(deployment.previous_candidate_id) is None
            )
            or DIGEST_RE.fullmatch(deployment.request_digest) is None
        ):
            raise RegistryError("developer deployment binding is invalid")
        return deployment

    def record_worker_runtime_bindings(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
        bindings: Mapping[str, Any],
    ) -> DeploymentRecord:
        """Bind all fleet image-load receipts before any host resource mutation."""

        normalized = json.loads(_canonical(bindings))
        if (
            DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or not _plain_integer(expected_resource_generation, minimum=1)
            or not isinstance(normalized, dict)
        ):
            raise RegistryError("worker runtime bindings are invalid")
        with self._transaction() as connection:
            deployment = self._deployment(connection, deployment_id)
            candidate = self._candidate(connection, deployment.candidate_id)
            environment = self._environment(connection, deployment.env_id)
            if (
                deployment.principal_id != principal_id
                or deployment.phase != "requested"
                or deployment.expected_resource_generation != expected_resource_generation
                or environment.state != "deploying"
                or environment.resource_generation != expected_resource_generation
                or not _valid_worker_runtime_bindings(normalized, candidate=candidate)
            ):
                raise RegistryError("worker runtime bindings are stale or invalid")
            if deployment.worker_runtime_bindings is not None:
                if deployment.worker_runtime_bindings != normalized:
                    raise RegistryError("worker runtime bindings conflict")
                return deployment
            connection.execute(
                """
                UPDATE deployments
                SET worker_runtime_bindings_json = ?, updated_at = ?
                WHERE deployment_id = ?
                """,
                (
                    json.dumps(normalized, sort_keys=True, separators=(",", ":")),
                    _timestamp(),
                    deployment_id,
                ),
            )
            self._bump_generation(connection)
            return self._deployment(connection, deployment_id)

    def advance_deployment(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_phase: str,
        next_phase: str,
        expected_resource_generation: int,
    ) -> DeploymentRecord:
        if (
            DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or expected_phase not in DEPLOY_PHASES
            or next_phase not in DEPLOY_PHASES
            or DEPLOY_PHASES.index(next_phase) != DEPLOY_PHASES.index(expected_phase) + 1
            or not isinstance(expected_resource_generation, int)
            or isinstance(expected_resource_generation, bool)
            or expected_resource_generation < 1
        ):
            raise RegistryError("deployment phase transition is invalid")
        with self._transaction() as connection:
            deployment = self._deployment(connection, deployment_id)
            if deployment.principal_id != principal_id:
                raise RegistryError("deployment ownership is invalid")
            environment = self._environment(connection, deployment.env_id)
            if deployment.expected_resource_generation != expected_resource_generation:
                raise RegistryError("deployment resource generation is stale")
            if deployment.phase == next_phase:
                return deployment
            if deployment.phase != expected_phase:
                raise RegistryError("deployment phase transition is stale")
            if expected_phase == "requested" and deployment.worker_runtime_bindings is None:
                raise RegistryError("deployment worker runtime binding is unavailable")
            if environment.resource_generation != expected_resource_generation:
                raise RegistryError("deployment resource generation is stale")
            now = _timestamp()
            connection.execute(
                "UPDATE deployments SET phase = ?, updated_at = ? WHERE deployment_id = ?",
                (next_phase, now, deployment_id),
            )
            if next_phase == "committed":
                if (
                    deployment.phase != "verified"
                    or deployment.applied_resource_generation != expected_resource_generation + 1
                    or deployment.finalization_payload_sha256 is None
                ):
                    raise RegistryError("deployment applied binding is invalid")
                finalization = connection.execute(
                    """
                    SELECT payload_sha256 FROM deployment_finalizations
                    WHERE deployment_id = ?
                    """,
                    (deployment_id,),
                ).fetchone()
                if (
                    finalization is None
                    or finalization["payload_sha256"] != deployment.finalization_payload_sha256
                ):
                    raise RegistryError("deployment finalization evidence is invalid")
                connection.execute(
                    """
                    UPDATE environments
                    SET state = 'active', current_candidate_id = ?,
                        resource_generation = ?
                    WHERE env_id = ?
                    """,
                    (
                        deployment.candidate_id,
                        deployment.applied_resource_generation,
                        deployment.env_id,
                    ),
                )
            self._bump_generation(connection)
            return self._deployment(connection, deployment_id)

    def prepare_deployment_finalization(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> DeploymentRecord:
        """Persist the pending applied binding while admission remains closed."""

        if (
            DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or not isinstance(expected_resource_generation, int)
            or isinstance(expected_resource_generation, bool)
            or expected_resource_generation < 1
        ):
            raise RegistryError("deployment finalization binding is invalid")
        with self._transaction() as connection:
            deployment = self._deployment(connection, deployment_id)
            environment = self._environment(connection, deployment.env_id)
            if (
                deployment.principal_id != principal_id
                or deployment.phase != "verified"
                or deployment.expected_resource_generation != expected_resource_generation
                or environment.state != "deploying"
                or environment.resource_generation != expected_resource_generation
            ):
                raise RegistryError("deployment finalization binding is stale")
            if deployment.applied_resource_generation is not None:
                return deployment
            precommit = self._snapshot_from_connection(connection)
            connection.execute(
                """
                UPDATE deployments
                SET applied_resource_generation = ?,
                    applied_registry_generation = ?,
                    applied_registry_payload_sha256 = ?,
                    updated_at = ?
                WHERE deployment_id = ?
                """,
                (
                    expected_resource_generation + 1,
                    cast(int, precommit["generation"]),
                    cast(str, precommit["payload_sha256"]),
                    _timestamp(),
                    deployment_id,
                ),
            )
            self._bump_generation(connection)
            return self._deployment(connection, deployment_id)

    def record_deployment_finalization(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
        evidence: Mapping[str, str],
    ) -> DeploymentRecord:
        """Persist the authoritative precommit proof for one exact deployment."""

        evidence_fields = {
            "capacity_finalize_receipt_sha256",
            "capacity_finalize_check_receipt_sha256",
            "runtime_reconcile_receipt_sha256",
            "runtime_prepare_check_receipt_sha256",
            "acceptance_probe_receipt_sha256",
        }
        if (
            DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or not isinstance(expected_resource_generation, int)
            or isinstance(expected_resource_generation, bool)
            or expected_resource_generation < 1
            or set(evidence) != evidence_fields
            or any(DIGEST_RE.fullmatch(value) is None for value in evidence.values())
        ):
            raise RegistryError("deployment finalization evidence is invalid")
        with self._transaction() as connection:
            deployment = self._deployment(connection, deployment_id)
            environment = self._environment(connection, deployment.env_id)
            candidate = self._candidate(connection, deployment.candidate_id)
            if (
                deployment.principal_id != principal_id
                or deployment.phase != "verified"
                or deployment.expected_resource_generation != expected_resource_generation
                or deployment.applied_resource_generation != expected_resource_generation + 1
                or deployment.applied_registry_generation is None
                or deployment.applied_registry_payload_sha256 is None
                or environment.state != "deploying"
                or environment.resource_generation != expected_resource_generation
            ):
                raise RegistryError("deployment finalization evidence is stale")
            unsigned = {
                "deployment_id": deployment_id,
                "env_id": deployment.env_id,
                "principal_id": principal_id,
                "candidate_id": candidate.candidate_id,
                "candidate_sha": candidate.candidate_sha,
                "candidate_tree": candidate.candidate_tree,
                "applied_resource_generation": deployment.applied_resource_generation,
                "applied_registry_generation": deployment.applied_registry_generation,
                "applied_registry_payload_sha256": (deployment.applied_registry_payload_sha256),
                **dict(evidence),
                "created_at": _timestamp(),
            }
            payload = {**unsigned, "payload_sha256": _digest(unsigned)}
            existing = connection.execute(
                "SELECT payload_json FROM deployment_finalizations WHERE deployment_id = ?",
                (deployment_id,),
            ).fetchone()
            if existing is not None:
                previous = json.loads(str(existing["payload_json"]))
                comparable = {
                    key: value
                    for key, value in previous.items()
                    if key not in {"created_at", "payload_sha256"}
                }
                current = {
                    key: value
                    for key, value in payload.items()
                    if key not in {"created_at", "payload_sha256"}
                }
                if comparable != current:
                    raise RegistryError("deployment finalization evidence conflicts")
                return deployment
            connection.execute(
                "INSERT INTO deployment_finalizations VALUES (?, ?, ?, ?)",
                (
                    deployment_id,
                    _canonical(payload).decode("ascii").rstrip("\n"),
                    payload["payload_sha256"],
                    payload["created_at"],
                ),
            )
            connection.execute(
                """
                UPDATE deployments
                SET finalization_payload_sha256 = ?, updated_at = ?
                WHERE deployment_id = ?
                """,
                (payload["payload_sha256"], _timestamp(), deployment_id),
            )
            self._bump_generation(connection)
            return self._deployment(connection, deployment_id)

    def fail_deployment(
        self,
        deployment_id: str,
        *,
        principal_id: str,
        expected_phase: str,
        expected_resource_generation: int,
    ) -> DeploymentRecord:
        """Persist a failed terminal phase without changing allocated resources."""

        if (
            DEPLOYMENT_ID_RE.fullmatch(deployment_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or expected_phase not in DEPLOY_PHASES[:-1]
            or not isinstance(expected_resource_generation, int)
            or isinstance(expected_resource_generation, bool)
            or expected_resource_generation < 1
        ):
            raise RegistryError("deployment failure transition is invalid")
        with self._transaction() as connection:
            deployment = self._deployment(connection, deployment_id)
            if deployment.principal_id != principal_id:
                raise RegistryError("deployment ownership is invalid")
            if deployment.phase == "failed":
                return deployment
            environment = self._environment(connection, deployment.env_id)
            if (
                deployment.phase != expected_phase
                or deployment.expected_resource_generation != expected_resource_generation
                or environment.resource_generation != expected_resource_generation
            ):
                raise RegistryError("deployment failure transition is stale")
            connection.execute(
                """
                UPDATE deployments
                SET phase = 'failed', updated_at = ?,
                    applied_resource_generation = NULL,
                    applied_registry_generation = NULL,
                    applied_registry_payload_sha256 = NULL,
                    finalization_payload_sha256 = NULL,
                    failed_from_phase = ?
                WHERE deployment_id = ?
                """,
                (_timestamp(), expected_phase, deployment_id),
            )
            connection.execute(
                "DELETE FROM deployment_finalizations WHERE deployment_id = ?",
                (deployment_id,),
            )
            connection.execute(
                "UPDATE environments SET state = ? WHERE env_id = ?",
                (
                    "active" if environment.current_candidate_id is not None else "ready",
                    deployment.env_id,
                ),
            )
            self._bump_generation(connection)
            return self._deployment(connection, deployment_id)

    def begin_retirement(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> EnvironmentRecord:
        """Quarantine one environment before any external retirement mutation."""

        if (
            ENV_ID_RE.fullmatch(env_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or not isinstance(expected_resource_generation, int)
            or isinstance(expected_resource_generation, bool)
            or expected_resource_generation < 1
        ):
            raise RegistryError("environment retirement binding is invalid")
        with self._transaction() as connection:
            environment = self._environment(connection, env_id)
            if environment.principal_id != principal_id:
                raise RegistryError("environment retirement ownership is invalid")
            if environment.state == "quarantined":
                if environment.resource_generation != expected_resource_generation:
                    raise RegistryError("environment retirement generation is stale")
                return environment
            if environment.resource_generation != expected_resource_generation:
                raise RegistryError("environment retirement generation is stale")
            if environment.state not in {"ready", "active"}:
                raise RegistryError("environment retirement state is invalid")
            active = connection.execute(
                "SELECT 1 FROM deployments WHERE env_id = ? "
                "AND phase NOT IN ('committed', 'failed')",
                (env_id,),
            ).fetchone()
            if active is not None:
                raise RegistryError("environment retirement has an active deployment")
            connection.execute(
                "UPDATE environments SET state = 'quarantined' WHERE env_id = ?",
                (env_id,),
            )
            self._bump_generation(connection)
            return self._environment(connection, env_id)

    def retire_environment(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> EnvironmentRecord:
        """Complete a previously quarantined retirement."""

        if (
            ENV_ID_RE.fullmatch(env_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or not isinstance(expected_resource_generation, int)
            or isinstance(expected_resource_generation, bool)
            or expected_resource_generation < 1
        ):
            raise RegistryError("environment retirement binding is invalid")
        with self._transaction() as connection:
            environment = self._environment(connection, env_id)
            if environment.principal_id != principal_id:
                raise RegistryError("environment retirement ownership is invalid")
            if environment.state == "retired":
                if environment.resource_generation != expected_resource_generation + 1:
                    raise RegistryError("environment retirement generation is stale")
                return environment
            if (
                environment.state != "quarantined"
                or environment.resource_generation != expected_resource_generation
            ):
                raise RegistryError("environment retirement state is invalid")
            connection.execute(
                """
                UPDATE environments
                SET state = 'retired', resource_generation = resource_generation + 1
                WHERE env_id = ?
                """,
                (env_id,),
            )
            self._bump_generation(connection)
            return self._environment(connection, env_id)

    def revive_environment(
        self,
        env_id: str,
        *,
        principal_id: str,
        expected_resource_generation: int,
    ) -> EnvironmentRecord:
        """Revive one fully retired environment without reallocating its identity."""

        if (
            ENV_ID_RE.fullmatch(env_id) is None
            or PRINCIPAL_RE.fullmatch(principal_id) is None
            or not isinstance(expected_resource_generation, int)
            or isinstance(expected_resource_generation, bool)
            or expected_resource_generation < 1
        ):
            raise RegistryError("environment revival binding is invalid")
        with self._transaction() as connection:
            environment = self._environment(connection, env_id)
            if environment.principal_id != principal_id:
                raise RegistryError("environment revival ownership is invalid")
            if environment.state == "ready":
                if environment.resource_generation != expected_resource_generation + 1:
                    raise RegistryError("environment revival generation is stale")
                return environment
            if (
                environment.state != "retired"
                or environment.resource_generation != expected_resource_generation
            ):
                raise RegistryError("environment revival state is invalid")
            connection.execute(
                """
                UPDATE environments
                SET state = 'ready', current_candidate_id = NULL,
                    resource_generation = resource_generation + 1,
                    lifecycle_epoch = lifecycle_epoch + 1
                WHERE env_id = ?
                """,
                (env_id,),
            )
            self._bump_generation(connection)
            return self._environment(connection, env_id)

    @contextmanager
    def _snapshot_publication_lock(self) -> Iterator[None]:
        descriptor = -1
        with SNAPSHOT_PROCESS_LOCK:
            try:
                descriptor = os.open(
                    self.snapshot_lock_path,
                    os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
                    0o600,
                )
                metadata = os.fstat(descriptor)
                expected_owner = 0 if self.system_mode else os.geteuid()
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_uid != expected_owner
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                ):
                    raise RegistryError("registry snapshot lock is unsafe")
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            except RegistryError:
                raise
            except OSError as exc:
                raise RegistryError("registry snapshot lock is unavailable") from exc
            finally:
                if descriptor >= 0:
                    try:
                        fcntl.flock(descriptor, fcntl.LOCK_UN)
                    except OSError:
                        pass
                    os.close(descriptor)

    def _validate_current_snapshot(self, expected: bytes) -> None:
        descriptor = -1
        try:
            lexical = self.snapshot_path.lstat()
            descriptor = os.open(
                self.snapshot_path,
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            opened = os.fstat(descriptor)
            raw = os.pread(descriptor, len(expected) + 1, 0)
            rebound = os.fstat(descriptor)
            current = self.snapshot_path.lstat()
        except OSError as exc:
            raise RegistryError("registry current snapshot is unavailable") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        expected_owner = 0 if self.system_mode else os.geteuid()
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
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid != expected_owner
            or stat.S_IMODE(opened.st_mode) != 0o600
            or len(identities) != 1
            or raw != expected
        ):
            raise RegistryError("registry current snapshot binding is invalid")
        self.verify_snapshot(raw)

    def _publish_snapshot_bytes(self, raw: bytes) -> None:
        temporary: Path | None = None
        descriptor = -1
        directory_descriptor = -1
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".current-snapshot-",
                suffix=".tmp",
                dir=self.snapshot_path.parent,
            )
            temporary = Path(temporary_name)
            os.fchmod(descriptor, 0o600)
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written < 1:
                    raise RegistryError("registry current snapshot publication failed")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, self.snapshot_path)
            temporary = None
            directory_descriptor = os.open(
                self.snapshot_path.parent,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            os.fsync(directory_descriptor)
            self._validate_current_snapshot(raw)
        except RegistryError:
            raise
        except OSError as exc:
            raise RegistryError("registry current snapshot publication failed") from exc
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

    def publish_fleet_identity_inventory(
        self,
        node_results: Sequence[Mapping[str, Any]],
        *,
        registry_generation: int,
        registry_payload_sha256: str,
    ) -> dict[str, Any]:
        """Atomically publish one root-owned, current-registry fleet inventory."""

        if not self.system_mode or os.getuid() != 0 or os.geteuid() != 0:
            raise RegistryError("fleet identity inventory publication requires root")
        current = self.snapshot()
        if (
            current["generation"] != registry_generation
            or current["payload_sha256"] != registry_payload_sha256
        ):
            raise RegistryError("fleet identity inventory registry binding is stale")
        raw = build_fleet_identity_inventory(
            node_results,
            registry_generation=registry_generation,
            registry_payload_sha256=registry_payload_sha256,
            policy=self.policy,
        )
        temporary: Path | None = None
        descriptor = -1
        directory_descriptor = -1
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=".fleet-identity-inventory-",
                suffix=".tmp",
                dir=self.fleet_identity_inventory_path.parent,
            )
            temporary = Path(temporary_name)
            os.fchmod(descriptor, 0o600)
            view = memoryview(raw)
            while view:
                written = os.write(descriptor, view)
                if written < 1:
                    raise RegistryError("fleet identity inventory publication failed")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary, self.fleet_identity_inventory_path)
            temporary = None
            directory_descriptor = os.open(
                self.fleet_identity_inventory_path.parent,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            os.fsync(directory_descriptor)
            return self.verify_fleet_identity_inventory(raw, policy=self.policy)
        except RegistryError:
            raise
        except OSError as exc:
            raise RegistryError("fleet identity inventory publication failed") from exc
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

    def _publish_current_snapshot(self) -> None:
        try:
            with self._snapshot_publication_lock():
                raw = self.snapshot_bytes()
                self._publish_snapshot_bytes(raw)
        except RegistryError:
            self.snapshot_dirty = True
            raise
        self.snapshot_dirty = False

    def _reconcile_current_snapshot(self) -> None:
        try:
            with self._snapshot_publication_lock():
                raw = self.snapshot_bytes()
                try:
                    self._validate_current_snapshot(raw)
                except RegistryError:
                    self._publish_snapshot_bytes(raw)
        except RegistryError:
            self.snapshot_dirty = True
            raise
        self.snapshot_dirty = False

    def _snapshot_from_connection(self, connection: sqlite3.Connection) -> dict[str, Any]:
        meta = connection.execute(
            """
            SELECT schema_version, database_schema_version, generation
            FROM registry_meta WHERE singleton = 1
            """
        ).fetchone()
        if (
            meta is None
            or meta["schema_version"] != SCHEMA_VERSION
            or meta["database_schema_version"] != DATABASE_SCHEMA_VERSION
        ):
            raise RegistryError("registry metadata is invalid")
        environments = [
            asdict(self._environment(connection, str(row["env_id"])))
            for row in connection.execute("SELECT env_id FROM environments ORDER BY env_id")
        ]
        candidates = [
            asdict(self._candidate(connection, str(row["candidate_id"])))
            for row in connection.execute(
                "SELECT candidate_id FROM candidates ORDER BY candidate_id"
            )
        ]
        deployments = [
            asdict(self._deployment(connection, str(row["deployment_id"])))
            for row in connection.execute(
                "SELECT deployment_id FROM deployments ORDER BY deployment_id"
            )
        ]
        finalizations: list[dict[str, Any]] = []
        for row in connection.execute(
            "SELECT payload_json FROM deployment_finalizations ORDER BY deployment_id"
        ):
            payload = json.loads(str(row["payload_json"]))
            if not isinstance(payload, dict):
                raise RegistryError("deployment finalization evidence is invalid")
            finalizations.append(payload)
        unsigned = {
            "schema_version": SCHEMA_VERSION,
            "kind": SNAPSHOT_KIND,
            "generation": int(meta["generation"]),
            "environments": environments,
            "candidates": candidates,
            "deployments": deployments,
            "deployment_finalizations": finalizations,
        }
        return {**unsigned, "payload_sha256": _digest(unsigned)}

    def snapshot(self) -> dict[str, Any]:
        with self._transaction(immediate=False) as connection:
            return self._snapshot_from_connection(connection)

    def snapshot_bytes(self) -> bytes:
        return _canonical(self.snapshot())

    def reconcile_snapshot(self) -> bytes:
        """Republish and verify the exact SQLite projection after a prior I/O fault."""

        self._reconcile_current_snapshot()
        return self.snapshot_bytes()

    @staticmethod
    def verify_snapshot(raw: bytes) -> dict[str, Any]:
        try:
            payload = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RegistryError("registry snapshot is invalid") from exc
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {
                "schema_version",
                "kind",
                "generation",
                "environments",
                "candidates",
                "deployments",
                "deployment_finalizations",
                "payload_sha256",
            }
            or payload.get("schema_version") != SCHEMA_VERSION
            or payload.get("kind") != SNAPSHOT_KIND
            or not _plain_integer(payload.get("generation"))
            or not isinstance(payload.get("environments"), list)
            or not isinstance(payload.get("candidates"), list)
            or not isinstance(payload.get("deployments"), list)
            or not isinstance(payload.get("deployment_finalizations"), list)
            or DIGEST_RE.fullmatch(str(payload.get("payload_sha256"))) is None
            or raw != _canonical(payload)
        ):
            raise RegistryError("registry snapshot binding is invalid")
        unsigned = {key: value for key, value in payload.items() if key != "payload_sha256"}
        if payload["payload_sha256"] != _digest(unsigned):
            raise RegistryError("registry snapshot digest is invalid")

        environments = payload["environments"]
        candidates = payload["candidates"]
        deployments = payload["deployments"]
        finalizations = payload["deployment_finalizations"]
        if (
            any(
                not isinstance(item, dict) or set(item) != ENVIRONMENT_SNAPSHOT_FIELDS
                for item in environments
            )
            or any(
                not isinstance(item, dict) or set(item) != CANDIDATE_SNAPSHOT_FIELDS
                for item in candidates
            )
            or any(
                not isinstance(item, dict) or set(item) != DEPLOYMENT_SNAPSHOT_FIELDS
                for item in deployments
            )
            or any(
                not isinstance(item, dict) or set(item) != FINALIZATION_SNAPSHOT_FIELDS
                for item in finalizations
            )
        ):
            raise RegistryError("registry snapshot row shape is invalid")

        environment_ids = [item["env_id"] for item in environments]
        candidate_ids = [item["candidate_id"] for item in candidates]
        deployment_ids = [item["deployment_id"] for item in deployments]
        if (
            environment_ids != sorted(environment_ids)
            or candidate_ids != sorted(candidate_ids)
            or deployment_ids != sorted(deployment_ids)
            or len(set(environment_ids)) != len(environment_ids)
            or len(set(candidate_ids)) != len(candidate_ids)
            or len(set(deployment_ids)) != len(deployment_ids)
        ):
            raise RegistryError("registry snapshot row ordering is invalid")

        unique_values: dict[str, set[object]] = {
            "principal_id": set(),
            "uid": set(),
            **{field: set() for field in UNIQUE_ENVIRONMENT_STRING_FIELDS},
        }
        all_ports: set[int] = set()
        environment_by_id: dict[str, dict[str, Any]] = {}
        for item in environments:
            ports = item["ports"]
            if (
                not isinstance(item["env_id"], str)
                or ENV_ID_RE.fullmatch(item["env_id"]) is None
                or not isinstance(item["principal_id"], str)
                or PRINCIPAL_RE.fullmatch(item["principal_id"]) is None
                or not isinstance(item["display_name"], str)
                or _validate_display_name(item["display_name"]) != item["display_name"]
                or item["layout_version"] not in {"dynamic-v1", "legacy-v1"}
                or not isinstance(item["runtime_id"], str)
                or RUNTIME_ID_RE.fullmatch(item["runtime_id"]) is None
                or item["state"] not in {"ready", "deploying", "active", "retired", "quarantined"}
                or not _plain_integer(item["resource_generation"], minimum=1)
                or not _plain_integer(item["lifecycle_epoch"], minimum=1)
                or not _plain_integer(item["uid"], minimum=1)
                or not _plain_integer(item["gid"], minimum=1)
                or item["uid"] != item["gid"]
                or not isinstance(ports, dict)
                or set(ports) != set(PORT_NAMES)
                or any(
                    not _plain_integer(port, minimum=1024) or port > 65_535
                    for port in ports.values()
                )
                or len(set(ports.values())) != len(PORT_NAMES)
                or any(
                    not isinstance(item[field], str) or not item[field]
                    for field in UNIQUE_ENVIRONMENT_STRING_FIELDS
                )
                or not _valid_fixed_path(item["candidate_root"])
                or not _valid_fixed_path(item["runtime_root"])
                or not _valid_fixed_path(item["state_root"])
                or not _valid_fixed_path(item["evidence_root"])
                or (
                    item["current_candidate_id"] is not None
                    and (
                        not isinstance(item["current_candidate_id"], str)
                        or CANDIDATE_ID_RE.fullmatch(item["current_candidate_id"]) is None
                    )
                )
                or not _valid_timestamp(item["created_at"])
            ):
                raise RegistryError("registry snapshot environment binding is invalid")

            if item["layout_version"] == "dynamic-v1":
                expected_resources = DeveloperEnvironmentRegistry._dynamic_resources(
                    item["env_id"],
                    item["runtime_id"],
                )
            else:
                runtime_id = item["runtime_id"]
                if runtime_id not in {"qianyi", "hongjian", "devansh"}:
                    raise RegistryError("registry snapshot legacy runtime is invalid")
                expected_resources = {
                    "service_user": f"loom-sandbox-{runtime_id}",
                    "service_group": f"loom-sandbox-{runtime_id}",
                    "compose_project": f"loom-sandbox-{runtime_id}",
                    "systemd_instance": runtime_id,
                    "candidate_root": (f"/shared_work/loom/candidates/sandboxes/{runtime_id}"),
                    "runtime_root": f"/shared_work/loom/runtime/sandboxes/{runtime_id}",
                    "state_root": f"/srv/loom/developer-sandboxes/{runtime_id}",
                    "evidence_root": (f"/srv/loom/developer-sandboxes/{runtime_id}/evidence"),
                    "database_name": f"loom_sandbox_{runtime_id}",
                    "postgres_volume": f"loom-sandbox-{runtime_id}_postgres_data",
                    "minio_volume": f"loom-sandbox-{runtime_id}_minio_data",
                    "task_bucket": f"loom-sandbox-{runtime_id}-tasks",
                    "trajectories_bucket": (f"loom-sandbox-{runtime_id}-trajectories"),
                    "artifacts_bucket": f"loom-sandbox-{runtime_id}-artifacts",
                    "provider_namespace": f"sandbox-{runtime_id}",
                    "slurm_user": f"loom-sandbox-{runtime_id}",
                    "slurm_account": f"loom-dev-{runtime_id}",
                    "slurm_qos": f"loom-dev-{runtime_id}",
                    "cgroup_slice": f"loom-dev-{runtime_id}.slice",
                }
            if any(item[field] != value for field, value in expected_resources.items()):
                raise RegistryError("registry snapshot environment resources are invalid")

            for field, seen in unique_values.items():
                value = item[field]
                if value in seen:
                    raise RegistryError("registry snapshot environment uniqueness is invalid")
                seen.add(value)
            if all_ports.intersection(ports.values()):
                raise RegistryError("registry snapshot port uniqueness is invalid")
            all_ports.update(ports.values())
            environment_by_id[item["env_id"]] = item

        candidate_by_id: dict[str, dict[str, Any]] = {}
        candidate_identities: set[tuple[object, ...]] = set()
        candidate_paths: set[str] = set()
        candidate_storage_roots: set[Path] = set()
        for item in candidates:
            image_digests = item["image_digests"]
            image_archives = item["image_archives"]
            bundle_path = item["bundle_path"]
            if (
                not isinstance(item["candidate_id"], str)
                or CANDIDATE_ID_RE.fullmatch(item["candidate_id"]) is None
                or not isinstance(item["principal_id"], str)
                or PRINCIPAL_RE.fullmatch(item["principal_id"]) is None
                or not isinstance(item["env_id"], str)
                or ENV_ID_RE.fullmatch(item["env_id"]) is None
                or not _plain_integer(item["lifecycle_epoch"], minimum=1)
                or item["repository_id"] != "qianyi-sun/loom"
                or item["image_binding_kind"] != WORKER_IMAGE_BINDING_KIND
                or not _plain_integer(item["image_binding_version"], minimum=1)
                or item["image_binding_version"] != WORKER_IMAGE_BINDING_VERSION
                or not isinstance(item["candidate_sha"], str)
                or SHA_RE.fullmatch(item["candidate_sha"]) is None
                or not isinstance(item["candidate_tree"], str)
                or SHA_RE.fullmatch(item["candidate_tree"]) is None
                or not isinstance(item["bundle_sha256"], str)
                or DIGEST_RE.fullmatch(item["bundle_sha256"]) is None
                or not _plain_integer(item["bundle_size"], minimum=1)
                or item["bundle_size"] > AllocationPolicy().max_bundle_bytes
                or not isinstance(bundle_path, str)
                or not _valid_fixed_path(bundle_path)
                or Path(bundle_path).parts[-3:]
                != ("candidates", item["candidate_id"], "candidate.bundle")
                or not _valid_worker_image_ids(image_digests)
                or not _valid_worker_image_archives(
                    image_archives,
                    candidate_root=Path(bundle_path).parent.parent,
                    candidate_id=item["candidate_id"],
                )
                or not _worker_archive_configs_match(image_digests, image_archives)
                or not _valid_timestamp(item["imported_at"])
            ):
                raise RegistryError("registry snapshot candidate binding is invalid")
            environment = environment_by_id.get(item["env_id"])
            if environment is None or environment["principal_id"] != item["principal_id"]:
                raise RegistryError("registry snapshot candidate ownership is invalid")
            identity = {
                "principal_id": item["principal_id"],
                "env_id": item["env_id"],
                "lifecycle_epoch": item["lifecycle_epoch"],
                "repository_id": item["repository_id"],
                "image_binding_kind": item["image_binding_kind"],
                "image_binding_version": item["image_binding_version"],
                "candidate_sha": item["candidate_sha"],
                "candidate_tree": item["candidate_tree"],
                "bundle_sha256": item["bundle_sha256"],
                "image_digests": image_digests,
                "image_archives": _archive_identity(image_archives),
            }
            if item["candidate_id"] != "cand-" + _digest(identity)[:40]:
                raise RegistryError("registry snapshot candidate identity is invalid")
            candidate_identity = (
                item["env_id"],
                item["candidate_sha"],
                item["candidate_tree"],
                item["bundle_sha256"],
                item["image_binding_kind"],
                item["image_binding_version"],
                tuple(sorted(image_digests.items())),
                tuple(
                    (
                        architecture,
                        tuple(
                            (
                                field,
                                image_archives[architecture][field],
                            )
                            for field in sorted(WORKER_IMAGE_ARCHIVE_BINDING_FIELDS)
                        ),
                    )
                    for architecture in ("amd64", "arm64")
                ),
            )
            if candidate_identity in candidate_identities or bundle_path in candidate_paths:
                raise RegistryError("registry snapshot candidate uniqueness is invalid")
            candidate_identities.add(candidate_identity)
            candidate_paths.add(bundle_path)
            candidate_storage_roots.add(Path(bundle_path).parent.parent)
            candidate_by_id[item["candidate_id"]] = item
        if len(candidate_storage_roots) > 1:
            raise RegistryError("registry snapshot candidate store is not fixed")

        finalization_by_deployment: dict[str, dict[str, Any]] = {}
        for item in finalizations:
            unsigned_finalization = {
                key: value for key, value in item.items() if key != "payload_sha256"
            }
            if (
                DEPLOYMENT_ID_RE.fullmatch(str(item["deployment_id"])) is None
                or ENV_ID_RE.fullmatch(str(item["env_id"])) is None
                or PRINCIPAL_RE.fullmatch(str(item["principal_id"])) is None
                or CANDIDATE_ID_RE.fullmatch(str(item["candidate_id"])) is None
                or SHA_RE.fullmatch(str(item["candidate_sha"])) is None
                or SHA_RE.fullmatch(str(item["candidate_tree"])) is None
                or not _plain_integer(item["applied_resource_generation"], minimum=2)
                or not _plain_integer(item["applied_registry_generation"], minimum=1)
                or any(
                    DIGEST_RE.fullmatch(str(item[field])) is None
                    for field in (
                        "applied_registry_payload_sha256",
                        "capacity_finalize_receipt_sha256",
                        "capacity_finalize_check_receipt_sha256",
                        "runtime_reconcile_receipt_sha256",
                        "runtime_prepare_check_receipt_sha256",
                        "acceptance_probe_receipt_sha256",
                        "payload_sha256",
                    )
                )
                or not _valid_timestamp(item["created_at"])
                or item["payload_sha256"] != _digest(unsigned_finalization)
                or item["deployment_id"] in finalization_by_deployment
            ):
                raise RegistryError("registry snapshot finalization binding is invalid")
            finalization_by_deployment[item["deployment_id"]] = item

        active_by_environment: dict[str, int] = {}
        committed_by_environment: dict[str, list[dict[str, Any]]] = {}
        for item in deployments:
            if (
                not isinstance(item["deployment_id"], str)
                or DEPLOYMENT_ID_RE.fullmatch(item["deployment_id"]) is None
                or not isinstance(item["principal_id"], str)
                or PRINCIPAL_RE.fullmatch(item["principal_id"]) is None
                or not isinstance(item["env_id"], str)
                or ENV_ID_RE.fullmatch(item["env_id"]) is None
                or not isinstance(item["candidate_id"], str)
                or CANDIDATE_ID_RE.fullmatch(item["candidate_id"]) is None
                or not _plain_integer(
                    item["expected_resource_generation"],
                    minimum=1,
                )
                or (
                    item["worker_runtime_bindings"] is not None
                    and not _valid_worker_runtime_bindings(item["worker_runtime_bindings"])
                )
                or (
                    item["phase"] not in {"requested", "failed"}
                    and item["worker_runtime_bindings"] is None
                )
                or (
                    item["phase"] == "committed"
                    and (
                        item["applied_resource_generation"]
                        != item["expected_resource_generation"] + 1
                        or not _plain_integer(
                            item["applied_registry_generation"],
                            minimum=1,
                        )
                        or not isinstance(
                            item["applied_registry_payload_sha256"],
                            str,
                        )
                        or DIGEST_RE.fullmatch(
                            item["applied_registry_payload_sha256"],
                        )
                        is None
                    )
                )
                or (
                    item["phase"] not in {"verified", "committed"}
                    and (
                        item["applied_resource_generation"] is not None
                        or item["applied_registry_generation"] is not None
                        or item["applied_registry_payload_sha256"] is not None
                    )
                )
                or (
                    item["phase"] == "verified"
                    and (
                        (
                            item["applied_resource_generation"] is None
                            and item["applied_registry_generation"] is None
                            and item["applied_registry_payload_sha256"] is None
                        )
                        is False
                        and (
                            item["applied_resource_generation"]
                            != item["expected_resource_generation"] + 1
                            or not _plain_integer(
                                item["applied_registry_generation"],
                                minimum=1,
                            )
                            or not isinstance(
                                item["applied_registry_payload_sha256"],
                                str,
                            )
                            or DIGEST_RE.fullmatch(
                                item["applied_registry_payload_sha256"],
                            )
                            is None
                        )
                    )
                )
                or item["phase"] not in {*DEPLOY_PHASES, "failed"}
                or (
                    item["previous_candidate_id"] is not None
                    and (
                        not isinstance(item["previous_candidate_id"], str)
                        or CANDIDATE_ID_RE.fullmatch(item["previous_candidate_id"]) is None
                    )
                )
                or not isinstance(item["request_digest"], str)
                or DIGEST_RE.fullmatch(item["request_digest"]) is None
                or not _valid_timestamp(item["created_at"])
                or not _valid_timestamp(item["updated_at"])
                or item["updated_at"] < item["created_at"]
            ):
                raise RegistryError("registry snapshot deployment binding is invalid")
            environment = environment_by_id.get(item["env_id"])
            candidate = candidate_by_id.get(item["candidate_id"])
            finalization = finalization_by_deployment.get(item["deployment_id"])
            previous = (
                None
                if item["previous_candidate_id"] is None
                else candidate_by_id.get(item["previous_candidate_id"])
            )
            if (
                environment is None
                or candidate is None
                or environment["principal_id"] != item["principal_id"]
                or candidate["principal_id"] != item["principal_id"]
                or candidate["env_id"] != item["env_id"]
                or candidate["lifecycle_epoch"] > environment["lifecycle_epoch"]
                or (
                    item["worker_runtime_bindings"] is not None
                    and not _valid_worker_runtime_bindings(
                        item["worker_runtime_bindings"],
                        candidate=CandidateRecord(
                            candidate_id=candidate["candidate_id"],
                            principal_id=candidate["principal_id"],
                            env_id=candidate["env_id"],
                            lifecycle_epoch=candidate["lifecycle_epoch"],
                            repository_id=candidate["repository_id"],
                            image_binding_kind=candidate["image_binding_kind"],
                            image_binding_version=candidate["image_binding_version"],
                            candidate_sha=candidate["candidate_sha"],
                            candidate_tree=candidate["candidate_tree"],
                            bundle_sha256=candidate["bundle_sha256"],
                            bundle_size=candidate["bundle_size"],
                            bundle_path=candidate["bundle_path"],
                            image_digests=candidate["image_digests"],
                            image_archives=candidate["image_archives"],
                            imported_at=candidate["imported_at"],
                        ),
                    )
                )
                or (
                    item["previous_candidate_id"] is not None
                    and (
                        previous is None
                        or previous["principal_id"] != item["principal_id"]
                        or previous["env_id"] != item["env_id"]
                    )
                )
                or item["expected_resource_generation"] > environment["resource_generation"]
                or (
                    item["phase"] == "committed"
                    and item["applied_resource_generation"] is not None
                    and item["applied_resource_generation"] > environment["resource_generation"]
                )
                or (item["phase"] == "committed" and finalization is None)
                or (
                    finalization is not None
                    and (
                        item["finalization_payload_sha256"] != finalization["payload_sha256"]
                        or any(
                            finalization[field] != item[field]
                            for field in (
                                "deployment_id",
                                "env_id",
                                "principal_id",
                                "candidate_id",
                                "applied_resource_generation",
                                "applied_registry_generation",
                                "applied_registry_payload_sha256",
                            )
                        )
                        or finalization["candidate_sha"] != candidate["candidate_sha"]
                        or finalization["candidate_tree"] != candidate["candidate_tree"]
                    )
                )
                or (
                    item["phase"] not in {"verified", "committed"}
                    and item["finalization_payload_sha256"] is not None
                )
            ):
                raise RegistryError("registry snapshot deployment relationship is invalid")
            if item["phase"] in DEPLOY_PHASES[:-1]:
                active_by_environment[item["env_id"]] = (
                    active_by_environment.get(item["env_id"], 0) + 1
                )
                if (
                    environment["state"] != "deploying"
                    or item["expected_resource_generation"] != environment["resource_generation"]
                    or candidate["lifecycle_epoch"] != environment["lifecycle_epoch"]
                ):
                    raise RegistryError("registry snapshot active deployment is invalid")
            elif item["phase"] == "committed":
                if item["expected_resource_generation"] + 1 > environment["resource_generation"]:
                    raise RegistryError("registry snapshot committed generation is invalid")
                committed_by_environment.setdefault(item["env_id"], []).append(item)

        if set(finalization_by_deployment) != {
            item["deployment_id"]
            for item in deployments
            if item["finalization_payload_sha256"] is not None
        }:
            raise RegistryError("registry snapshot finalization set is invalid")

        if any(count != 1 for count in active_by_environment.values()):
            raise RegistryError("registry snapshot active deployment uniqueness is invalid")
        for environment in environments:
            env_id = environment["env_id"]
            current_candidate_id = environment["current_candidate_id"]
            active_count = active_by_environment.get(env_id, 0)
            committed = committed_by_environment.get(env_id, [])
            if (
                (environment["state"] == "deploying") != (active_count == 1)
                or (environment["state"] == "ready" and current_candidate_id is not None)
                or (environment["state"] == "active" and current_candidate_id is None)
            ):
                raise RegistryError("registry snapshot environment state is invalid")
            if current_candidate_id is not None:
                current = candidate_by_id.get(current_candidate_id)
                if (
                    current is None
                    or current["env_id"] != env_id
                    or current["principal_id"] != environment["principal_id"]
                    or current["lifecycle_epoch"] != environment["lifecycle_epoch"]
                    or not committed
                ):
                    raise RegistryError("registry snapshot current candidate is invalid")
                latest = max(
                    committed,
                    key=lambda deployment: (
                        deployment["expected_resource_generation"],
                        deployment["updated_at"],
                        deployment["deployment_id"],
                    ),
                )
                if latest["candidate_id"] != current_candidate_id:
                    raise RegistryError("registry snapshot current candidate is stale")
            elif (
                environment["state"] not in {"ready", "deploying", "retired", "quarantined"}
                and committed
            ):
                raise RegistryError("registry snapshot committed candidate is missing")
        return payload


def _admin_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument(
        "command",
        choices=("init", "import-seed", "export-snapshot", "status"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _admin_parser().parse_args(list(argv) if argv is not None else None)
    if os.getuid() != 0 or os.geteuid() != 0:
        sys.stderr.write("error: developer environment registry admin requires root\n")
        return 1
    try:
        authority = DeveloperEnvironmentRegistry.open_system()
        if args.command == "import-seed":
            environments = authority.import_legacy_seed()
            result: dict[str, Any] = {
                "schema_version": SCHEMA_VERSION,
                "action": "import-seed",
                "environment_count": len(environments),
                "status": "succeeded",
            }
        elif args.command == "export-snapshot":
            sys.stdout.buffer.write(authority.snapshot_bytes())
            return 0
        else:
            snapshot = authority.snapshot()
            result = {
                "schema_version": SCHEMA_VERSION,
                "action": args.command,
                "generation": snapshot["generation"],
                "environment_count": len(snapshot["environments"]),
                "candidate_count": len(snapshot["candidates"]),
                "deployment_count": len(snapshot["deployments"]),
                "snapshot_sha256": snapshot["payload_sha256"],
                "status": "succeeded",
            }
    except RegistryError:
        sys.stderr.write("error: developer environment registry admin failed safely\n")
        return 1
    sys.stdout.buffer.write(_canonical(result))
    return 0


__all__ = [
    "CURRENT_SNAPSHOT_PATH",
    "DEPLOY_PHASES",
    "MAX_WORKER_IMAGE_ARCHIVE_BYTES",
    "AllocationPolicy",
    "CandidateRecord",
    "DeploymentRecord",
    "DeveloperEnvironmentRegistry",
    "EnvironmentRecord",
    "RegistryError",
    "verify_worker_image_archive",
    "worker_image_archive_path",
]


if __name__ == "__main__":
    raise SystemExit(main())
