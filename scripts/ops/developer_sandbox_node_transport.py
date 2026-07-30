#!/usr/bin/python3 -I
"""Fixed, key-isolated transport for developer-sandbox node authority.

This program deliberately has no general SSH surface.  A persistent host-root
bootstrap channel supplies pre-existing root-owned identities, matching public
keys, and a pinned known_hosts file.  That channel may be a direct root session
or the repository's one-shot Docker/chroot bootstrap.  Bootstrap copies those
assets to fixed root-owned paths and records their digests.  Runtime callers
may select only a closed node and an explicitly authorized node-authority verb.

The same installed program is the forced command for the dedicated public
keys.  Authority roles map exactly to ``transact``, ``check``, or the
oldlab2-controller-only ``load-image`` stream.  The GB10
jump role maps only a closed target name to a fixed address on TCP port 22; it
does not enable SSH forwarding, a shell, an agent, a password, or a PTY.
"""

from __future__ import annotations

import argparse
import base64
import binascii
import contextlib
import fcntl
import hashlib
import importlib.util
import ipaddress
import json
import os
import pwd
import re
import shutil
import socket
import stat
import subprocess
import sys
import threading
import time
import tomllib
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

REPO_ROOT: Final = Path(__file__).resolve().parents[2]
CHECKED_IN_CONFIG: Final = REPO_ROOT / "deploy/developer-sandboxes/node-authority-transport.toml"
INSTALL_ROOT: Final = Path("/etc/loom/developer-sandbox-node-transport")
LIBEXEC: Final = Path("/usr/local/libexec/loom-developer-sandbox-node-transport")
AUTHORITY_PROGRAM: Final = Path(
    "/usr/local/libexec/loom-developer-sandbox-node-authority",
)
SCHEMA_VERSION: Final = 1
OPERATOR: Final = "qianyi"
MAX_ASSET_BYTES: Final = 1 << 20
MAX_IDENTITY_BYTES: Final = 64 * 1024
# One runtime-proof artifact may contain up to 1 MiB before canonical
# JSON/base64 framing.  Keep the transport output closed and large enough for
# that exact protocol bound.
MAX_STDOUT_BYTES: Final = 1536 * 1024
MAX_STDERR_BYTES: Final = 1024 * 1024
DEFAULT_INVOKE_TIMEOUT_SECONDS: Final = 120
INFRASTRUCTURE_CONVERGE_TIMEOUT_SECONDS: Final = 3600
WORKER_IMAGE_LOAD_TIMEOUT_SECONDS: Final = 3600
MAX_WORKER_IMAGE_HEADER_BYTES: Final = 64 * 1024
NAME_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
ROLE_RE: Final = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")
ALLOWED_VERBS: Final = frozenset({"transact", "check", "load-image"})
SSH: Final = "/usr/bin/ssh"
SUDO: Final = "/usr/bin/sudo"
RUNUSER: Final = "/usr/sbin/runuser"
SSH_KEYGEN: Final = "/usr/bin/ssh-keygen"
ROOT_UID: Final = 0
ROOT_GID: Final = 0
INSTALL_ROOT_MODE: Final = 0o755
ROUTE_CONFIG_MODE: Final = 0o644
SERVER_POLICY_MODE: Final = 0o644
REQUEST_FIELDS: Final = {
    "schema_version",
    "request_id",
    "action",
    "node",
    "domain",
    "sandbox",
    "candidate_sha",
    "candidate_tree",
    "payload_kind",
    "payload_sha256",
    "payload_base64",
    "prior_request_id",
}
REGISTRY_SNAPSHOT: Final = Path(
    "/var/lib/loom-developer-environment-registry/current-snapshot.json",
)
REGISTRY_MODULE_RELATIVE: Final = Path(
    "scripts/ops/developer_environment_registry.py",
)
REGISTRY_VERIFIER_SOURCE: Final = (
    Path("/opt/loom-developer-sandbox-node-authority/source") / REGISTRY_MODULE_RELATIVE
)
REGISTRY_SNAPSHOT_SYNC_ACTION: Final = "registry-snapshot-sync"
REGISTRY_SNAPSHOT_SYNC_KIND: Final = "developer-environment-registry-snapshot-json"
REGISTRY_SNAPSHOT_SYNC_FIELDS: Final = REQUEST_FIELDS | {
    "registry_generation",
    "registry_payload_sha256",
}
REGISTRY_DEPENDENT_ACTIONS: Final = frozenset(
    {
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
        "slurm-node-converge",
        "slurm-controller-converge",
        "slurm-check",
        "slurm-rollback",
        "slurm-identity-preflight",
        "slurm-identity-converge",
        "slurm-identity-retire",
        "slurm-identity-inventory",
        "developer-environment-acceptance-probe",
        "developer-environment-runtime-retire",
    },
)
WORKER_IMAGE_BOUND_ACTIONS: Final = frozenset(
    {
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
        "slurm-identity-preflight",
        "slurm-identity-converge",
        "slurm-identity-retire",
        "developer-environment-acceptance-probe",
    },
)
IDENTITY_PREFLIGHT_ACTION: Final = "slurm-identity-preflight"
IDENTITY_PREFLIGHT_PAYLOAD_KIND: Final = "developer-environment-identity-preflight-json"
INFRASTRUCTURE_CONVERGE_FIELDS: Final = {
    "schema_version",
    "kind",
    "candidate_sha",
    "candidate_tree",
    "convergence_id",
    "requested_at",
}
WORKER_IMAGE_LOAD_REQUEST_KIND: Final = "loom.developer-sandbox.worker-image-load-request"
WORKER_IMAGE_LOAD_RECEIPT_KIND: Final = "loom.developer-sandbox.worker-image-load-receipt"
WORKER_IMAGE_LOAD_REQUEST_FIELDS: Final = {
    "schema_version",
    "kind",
    "node",
    "domain",
    "architecture",
    "env_id",
    "resource_generation",
    "candidate_id",
    "candidate_sha",
    "candidate_tree",
    "config_digest",
    "index_digest",
    "load_descriptor_digest",
    "load_descriptor_media_type",
    "archive_sha256",
    "archive_size",
    "registry_generation",
    "registry_payload_sha256",
    "payload_sha256",
}
WORKER_IMAGE_LOAD_RECEIPT_FIELDS: Final = {
    "schema_version",
    "kind",
    "status",
    "node",
    "domain",
    "architecture",
    "candidate_id",
    "candidate_sha",
    "config_digest",
    "index_digest",
    "load_descriptor_digest",
    "load_descriptor_media_type",
    "runtime_image_id",
    "archive_sha256",
    "archive_size",
    "registry_generation",
    "registry_payload_sha256",
    "docker_storage_driver",
    "docker_backend",
    "docker_descriptor_digest",
    "docker_descriptor_media_type",
    "payload_sha256",
}


class TransportError(RuntimeError):
    """A bounded, secret-safe transport failure."""


@dataclass(frozen=True, slots=True)
class Node:
    name: str
    hostname: str


@dataclass(frozen=True, slots=True)
class Role:
    name: str
    kind: str
    initiator: str
    verbs: frozenset[str]
    targets: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class Route:
    initiator: str
    node: str
    address: str
    port: int
    jump: str | None

    @property
    def known_hosts_token(self) -> str:
        return self.address if self.port == 22 else f"[{self.address}]:{self.port}"


@dataclass(frozen=True, slots=True)
class TransportConfig:
    operator: str
    authority_program: Path
    nodes: Mapping[str, Node]
    roles: Mapping[str, Role]
    routes: Mapping[tuple[str, str], Route]

    def node_for_hostname(self, hostname: str) -> str:
        matches = [
            node.name
            for node in self.nodes.values()
            if node.hostname == hostname.rstrip(".").lower()
        ]
        if len(matches) != 1:
            raise TransportError("transport initiator is outside the closed inventory")
        return matches[0]

    def authority_role(self, initiator: str, node: str, verb: str) -> Role:
        matches = [
            role
            for role in self.roles.values()
            if role.kind == "authority"
            and role.initiator == initiator
            and node in role.targets
            and verb in role.verbs
        ]
        if len(matches) != 1:
            raise TransportError("transport request is outside the closed authority")
        return matches[0]

    def route(self, initiator: str, node: str) -> Route:
        try:
            return self.routes[(initiator, node)]
        except KeyError as exc:
            raise TransportError("transport route is outside the closed inventory") from exc

    def proxy_role(self, initiator: str, node: str) -> Role:
        matches = [
            role
            for role in self.roles.values()
            if role.kind == "proxy" and role.initiator == initiator and node in role.targets
        ]
        if len(matches) != 1:
            raise TransportError("transport proxy request is outside the closed authority")
        return matches[0]


@dataclass(frozen=True, slots=True)
class Layout:
    root: Path
    libexec: Path
    authorized_keys: Path
    operator_uid: int
    operator_gid: int

    @property
    def config(self) -> Path:
        return self.root / "routes.toml"

    @property
    def known_hosts(self) -> Path:
        return self.root / "known_hosts"

    @property
    def identities(self) -> Path:
        return self.root / "identities"

    @property
    def public_keys(self) -> Path:
        return self.root / "public-keys"

    @property
    def client_policy(self) -> Path:
        return self.root / "client-policy.json"

    @property
    def server_policy(self) -> Path:
        return self.root / "server-policy.json"

    @property
    def upgrade_root(self) -> Path:
        return self.root / "upgrades"

    @property
    def upgrade_active(self) -> Path:
        return self.root / "upgrade-active.json"

    @property
    def upgrade_journal(self) -> Path:
        return self.root / "upgrade-journal.jsonl"

    @property
    def upgrade_lock(self) -> Path:
        return self.root / "upgrade.lock"

    def identity(self, role: str) -> Path:
        return self.identities / role

    def public_key(self, role: str) -> Path:
        return self.public_keys / f"{role}.pub"


@dataclass(frozen=True, slots=True)
class UpgradeSnapshot:
    upgrade_id: str
    root: Path
    entries: tuple[Mapping[str, Any], ...]
    old_config_sha256: str
    new_config_sha256: str
    old_install_root_mode: int
    roles: tuple[str, ...]
    new_identity_roles: tuple[str, ...]
    client_installed: bool
    server_installed: bool


def default_layout() -> Layout:
    try:
        account = pwd.getpwnam(OPERATOR)
    except KeyError as exc:
        raise TransportError("transport operator identity is unavailable") from exc
    return Layout(
        root=INSTALL_ROOT,
        libexec=LIBEXEC,
        authorized_keys=Path(account.pw_dir) / ".ssh/authorized_keys",
        operator_uid=account.pw_uid,
        operator_gid=account.pw_gid,
    )


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("ascii")
        + b"\n"
    )


def _hostname() -> str:
    return socket.gethostname().rstrip(".").lower()


def _clean_env() -> dict[str, str]:
    return {
        "HOME": "/var/empty",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    }


def _exact_dict(
    value: object,
    fields: set[str],
    label: str,
) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise TransportError(f"{label} schema is invalid")
    return value


def _safe_name(value: object, label: str, pattern: re.Pattern[str] = NAME_RE) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise TransportError(f"{label} is invalid")
    return value


def load_config(path: Path = CHECKED_IN_CONFIG) -> TransportConfig:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise TransportError("transport route configuration is unavailable") from exc
    return _load_config_payload(raw)


def _load_config_payload(raw: bytes) -> TransportConfig:
    try:
        payload = tomllib.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise TransportError("transport route configuration is unavailable") from exc
    if len(raw) > MAX_ASSET_BYTES:
        raise TransportError("transport route configuration exceeds its size bound")
    root = _exact_dict(
        payload,
        {
            "schema_version",
            "operator",
            "authority_program",
            "nodes",
            "roles",
            "routes",
        },
        "transport route configuration",
    )
    if (
        root["schema_version"] != SCHEMA_VERSION
        or root["operator"] != OPERATOR
        or root["authority_program"] != str(AUTHORITY_PROGRAM)
    ):
        raise TransportError("transport route configuration identity is invalid")

    nodes: dict[str, Node] = {}
    hostnames: set[str] = set()
    if not isinstance(root["nodes"], list) or not root["nodes"]:
        raise TransportError("transport node inventory is invalid")
    for item in root["nodes"]:
        row = _exact_dict(item, {"name", "hostname"}, "transport node")
        name = _safe_name(row["name"], "transport node name")
        hostname = _safe_name(row["hostname"], "transport node hostname")
        if name in nodes or hostname in hostnames:
            raise TransportError("transport node inventory is ambiguous")
        nodes[name] = Node(name=name, hostname=hostname)
        hostnames.add(hostname)

    roles: dict[str, Role] = {}
    if not isinstance(root["roles"], list) or not root["roles"]:
        raise TransportError("transport role inventory is invalid")
    for item in root["roles"]:
        row = _exact_dict(
            item,
            {"name", "kind", "initiator", "verbs", "targets"},
            "transport role",
        )
        name = _safe_name(row["name"], "transport role name", ROLE_RE)
        kind = row["kind"]
        initiator = _safe_name(row["initiator"], "transport role initiator")
        verbs_raw = row["verbs"]
        targets_raw = row["targets"]
        if (
            kind not in {"authority", "proxy"}
            or initiator not in nodes
            or not isinstance(verbs_raw, list)
            or not isinstance(targets_raw, list)
            or not targets_raw
        ):
            raise TransportError("transport role is invalid")
        verbs = tuple(verbs_raw)
        targets = tuple(targets_raw)
        if (
            any(not isinstance(verb, str) for verb in verbs)
            or len(set(verbs)) != len(verbs)
            or any(not isinstance(target, str) or target not in nodes for target in targets)
            or len(set(targets)) != len(targets)
            or (kind == "authority" and (not verbs or not set(verbs) <= ALLOWED_VERBS))
            or (kind == "proxy" and verbs)
            or name in roles
        ):
            raise TransportError("transport role authority is invalid")
        roles[name] = Role(
            name=name,
            kind=kind,
            initiator=initiator,
            verbs=frozenset(verbs),
            targets=targets,
        )

    routes: dict[tuple[str, str], Route] = {}
    if not isinstance(root["routes"], list) or not root["routes"]:
        raise TransportError("transport route inventory is invalid")
    for item in root["routes"]:
        row = _exact_dict(
            item,
            {"initiator", "node", "address", "port", "jump"},
            "transport route",
        )
        initiator = _safe_name(row["initiator"], "transport route initiator")
        node = _safe_name(row["node"], "transport route node")
        jump_raw = row["jump"]
        try:
            address = str(ipaddress.ip_address(row["address"]))
        except (TypeError, ValueError) as exc:
            raise TransportError("transport route address is invalid") from exc
        port = row["port"]
        if (
            initiator not in nodes
            or node not in nodes
            or type(port) is not int
            or not 1 <= port <= 65535
            or not isinstance(jump_raw, str)
        ):
            raise TransportError("transport route is invalid")
        jump = jump_raw or None
        if jump is not None and jump not in nodes:
            raise TransportError("transport jump is outside the closed inventory")
        key = (initiator, node)
        if key in routes:
            raise TransportError("transport route inventory is ambiguous")
        routes[key] = Route(
            initiator=initiator,
            node=node,
            address=address,
            port=port,
            jump=jump,
        )

    for role in roles.values():
        for target in role.targets:
            if role.kind == "authority" and (role.initiator, target) not in routes:
                raise TransportError("transport authority route is incomplete")
            if role.kind == "proxy":
                route = routes.get((role.initiator, target))
                if route is None or route.jump is None:
                    raise TransportError("transport proxy route is incomplete")
    for route in routes.values():
        if route.jump is not None:
            if (route.initiator, route.jump) not in routes:
                raise TransportError("transport jump route is incomplete")
            proxy_matches = [
                role
                for role in roles.values()
                if role.kind == "proxy"
                and role.initiator == route.initiator
                and route.node in role.targets
            ]
            if len(proxy_matches) != 1:
                raise TransportError("transport jump authority is ambiguous")

    return TransportConfig(
        operator=OPERATOR,
        authority_program=AUTHORITY_PROGRAM,
        nodes=nodes,
        roles=roles,
        routes=routes,
    )


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_external_fd_twice(descriptor: int, *, limit: int) -> bytes:
    payloads: list[bytes] = []
    identities: list[tuple[int, ...]] = []
    for _ in range(2):
        os.lseek(descriptor, 0, os.SEEK_SET)
        content = bytearray()
        while len(content) <= limit:
            chunk = os.read(descriptor, min(65_536, limit + 1 - len(content)))
            if not chunk:
                break
            content.extend(chunk)
        if len(content) > limit:
            raise TransportError("external-root transport asset metadata is unsafe")
        payloads.append(bytes(content))
        identities.append(_metadata_identity(os.fstat(descriptor)))
    if (
        identities[0] != identities[1]
        or payloads[0] != payloads[1]
        or hashlib.sha256(payloads[0]).digest() != hashlib.sha256(payloads[1]).digest()
    ):
        raise TransportError("external-root transport asset changed during verification")
    return payloads[0]


def _safe_external_directory(
    metadata: os.stat_result,
    *,
    expected_uid: int,
) -> bool:
    return (
        stat.S_ISDIR(metadata.st_mode)
        and not stat.S_ISLNK(metadata.st_mode)
        and metadata.st_uid in {0, expected_uid}
        and not stat.S_IMODE(metadata.st_mode) & 0o022
    )


def _open_external_parent_chain(
    path: Path,
    *,
    expected_uid: int,
) -> tuple[list[int], list[tuple[int, str, int, tuple[int, ...]]], str]:
    try:
        absolute = path.absolute()
    except OSError as exc:
        raise TransportError("external-root transport asset path is unsafe") from exc
    if not absolute.is_absolute() or not absolute.name:
        raise TransportError("external-root transport asset path is unsafe")
    descriptors: list[int] = []
    records: list[tuple[int, str, int, tuple[int, ...]]] = []
    try:
        root = os.open(
            "/",
            os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        descriptors.append(root)
        if not _safe_external_directory(os.fstat(root), expected_uid=expected_uid):
            raise TransportError("external-root transport asset path is unsafe")
        parent = root
        for component in absolute.parts[1:-1]:
            lexical = os.stat(component, dir_fd=parent, follow_symlinks=False)
            child = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
                dir_fd=parent,
            )
            descriptors.append(child)
            metadata = os.fstat(child)
            if (
                not _safe_external_directory(lexical, expected_uid=expected_uid)
                or not _safe_external_directory(metadata, expected_uid=expected_uid)
                or _metadata_identity(lexical) != _metadata_identity(metadata)
            ):
                raise TransportError("external-root transport asset path is unsafe")
            records.append((parent, component, child, _metadata_identity(metadata)))
            parent = child
        return descriptors, records, absolute.name
    except OSError as exc:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise TransportError("external-root transport asset path is unsafe") from exc
    except Exception:
        for descriptor in reversed(descriptors):
            os.close(descriptor)
        raise


def _revalidate_external_parent_chain(
    records: Sequence[tuple[int, str, int, tuple[int, ...]]],
    *,
    expected_uid: int,
) -> None:
    for parent, component, child, identity in records:
        try:
            lexical = os.stat(component, dir_fd=parent, follow_symlinks=False)
            metadata = os.fstat(child)
        except OSError as exc:
            raise TransportError("external-root transport asset path is unsafe") from exc
        if (
            _metadata_identity(lexical) != identity
            or _metadata_identity(metadata) != identity
            or not _safe_external_directory(metadata, expected_uid=expected_uid)
        ):
            raise TransportError(
                "external-root transport asset parent changed during verification",
            )


def _safe_external_file(
    path: Path,
    *,
    modes: frozenset[int],
    limit: int,
    expected_uid: int = 0,
    validate_parents: bool = False,
) -> bytes:
    descriptors: list[int] = []
    records: list[tuple[int, str, int, tuple[int, ...]]] = []
    try:
        if validate_parents:
            descriptors, records, name = _open_external_parent_chain(
                path,
                expected_uid=expected_uid,
            )
            parent = descriptors[-1]
            lexical = os.stat(name, dir_fd=parent, follow_symlinks=False)
            descriptor = os.open(
                name,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
                dir_fd=parent,
            )
        else:
            lexical = path.lstat()
            descriptor = os.open(
                path,
                os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
            )
    except OSError as exc:
        for opened in reversed(descriptors):
            os.close(opened)
        raise TransportError("external-root transport asset is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        content = _read_external_fd_twice(descriptor, limit=limit)
        after = os.fstat(descriptor)
        if validate_parents:
            current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        else:
            current = path.lstat()
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or before.st_uid != expected_uid
            or stat.S_IMODE(before.st_mode) not in modes
            or before.st_nlink != 1
            or _metadata_identity(lexical) != _metadata_identity(before)
            or _metadata_identity(before) != _metadata_identity(after)
            or _metadata_identity(after) != _metadata_identity(current)
        ):
            raise TransportError("external-root transport asset metadata is unsafe")
        if validate_parents:
            _revalidate_external_parent_chain(
                records,
                expected_uid=expected_uid,
            )
        return content
    finally:
        os.close(descriptor)
        for opened in reversed(descriptors):
            os.close(opened)


@contextlib.contextmanager
def _safe_external_stream(
    path: Path,
    *,
    expected_size: int,
    expected_uid: int = ROOT_UID,
) -> Any:
    """Keep a root-owned, non-link regular file and its parent chain pinned."""

    descriptors: list[int] = []
    records: list[tuple[int, str, int, tuple[int, ...]]] = []
    descriptor = -1
    try:
        descriptors, records, name = _open_external_parent_chain(
            path,
            expected_uid=expected_uid,
        )
        parent = descriptors[-1]
        lexical = os.stat(name, dir_fd=parent, follow_symlinks=False)
        descriptor = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
            dir_fd=parent,
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or before.st_uid != expected_uid
            or before.st_gid != ROOT_GID
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size != expected_size
            or _metadata_identity(lexical) != _metadata_identity(before)
        ):
            raise TransportError("worker image archive metadata is unsafe")
        yield descriptor
        after = os.fstat(descriptor)
        current = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if _metadata_identity(before) != _metadata_identity(after) or _metadata_identity(
            after
        ) != _metadata_identity(current):
            raise TransportError("worker image archive changed during transport")
        _revalidate_external_parent_chain(records, expected_uid=expected_uid)
    except OSError as exc:
        raise TransportError("worker image archive is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        for opened in reversed(descriptors):
            os.close(opened)


def _validate_external_parent_chain(path: Path, *, expected_uid: int) -> None:
    descriptors, records, _name = _open_external_parent_chain(
        path,
        expected_uid=expected_uid,
    )
    try:
        _revalidate_external_parent_chain(records, expected_uid=expected_uid)
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _require_persistent_install_root(
    *,
    root_path: Path = Path("/"),
    pid1_root_path: Path = Path("/proc/1/root"),
    pid1_comm_path: Path = Path("/proc/1/comm"),
    uid: int | None = None,
    euid: int | None = None,
) -> None:
    real_uid = os.getuid() if uid is None else uid
    effective_uid = os.geteuid() if euid is None else euid
    if real_uid != 0 or effective_uid != 0:
        raise TransportError(
            "transport operation requires persistent host-root authority",
        )
    try:
        root = root_path.stat()
        pid1_root = pid1_root_path.stat()
        pid1_comm = pid1_comm_path.read_text(encoding="ascii").strip()
    except OSError as exc:
        raise TransportError(
            "persistent host-root systemd view is unavailable",
        ) from exc
    if (root.st_dev, root.st_ino) != (pid1_root.st_dev, pid1_root.st_ino) or pid1_comm != "systemd":
        raise TransportError("persistent host-root systemd view is invalid")


def _decode_public_key(payload: bytes) -> tuple[bytes, str]:
    try:
        fields = payload.decode("ascii").strip().split()
    except UnicodeDecodeError as exc:
        raise TransportError("transport public key is invalid") from exc
    if len(fields) not in {2, 3} or fields[0] != "ssh-ed25519":
        raise TransportError("transport public key is invalid")
    try:
        blob = base64.b64decode(fields[1], validate=True)
    except (binascii.Error, ValueError) as exc:
        raise TransportError("transport public key is invalid") from exc
    algorithm = b"ssh-ed25519"
    prefix = len(algorithm).to_bytes(4, "big") + algorithm
    if not blob.startswith(prefix):
        raise TransportError("transport public key is invalid")
    offset = len(prefix)
    if len(blob) != offset + 4 + 32 or int.from_bytes(blob[offset : offset + 4]) != 32:
        raise TransportError("transport public key is invalid")
    fingerprint = "SHA256:" + base64.b64encode(hashlib.sha256(blob).digest()).decode(
        "ascii"
    ).rstrip("=")
    return f"ssh-ed25519 {fields[1]}\n".encode("ascii"), fingerprint


def _known_hosts_endpoints(payload: bytes) -> set[str]:
    endpoints: set[str] = set()
    try:
        lines = payload.decode("ascii").splitlines()
    except UnicodeDecodeError as exc:
        raise TransportError("transport known_hosts is invalid") from exc
    for line in lines:
        if not line or line.startswith("#"):
            continue
        fields = line.split()
        if (
            len(fields) != 3
            or "," in fields[0]
            or fields[0].startswith("|")
            or fields[1] != "ssh-ed25519"
        ):
            raise TransportError("transport known_hosts is invalid")
        _decode_public_key(f"{fields[1]} {fields[2]}\n".encode("ascii"))
        if fields[0] in endpoints:
            raise TransportError("transport known_hosts contains duplicate authority")
        endpoints.add(fields[0])
    if not endpoints:
        raise TransportError("transport known_hosts is empty")
    return endpoints


def _required_known_hosts(config: TransportConfig, initiator: str) -> set[str]:
    local = config.nodes[initiator].hostname
    return {
        route.known_hosts_token
        for route in config.routes.values()
        if route.initiator == initiator and config.nodes[route.node].hostname != local
    }


def _derive_public_key(
    identity: Path,
    *,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
) -> bytes:
    try:
        completed = run(
            (SSH_KEYGEN, "-y", "-f", str(identity)),
            env=_clean_env(),
            check=False,
            capture_output=True,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise TransportError("transport identity verification failed safely") from exc
    if completed.returncode != 0 or completed.stderr:
        raise TransportError("transport identity verification failed safely")
    return bytes(completed.stdout)


def _ensure_directory(path: Path, *, mode: int, uid: int = 0, gid: int = 0) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        path.mkdir(mode=mode)
        os.chown(path, uid, gid)
        os.chmod(path, mode)
        metadata = path.lstat()
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise TransportError("transport install directory metadata is unsafe")


def _install_once(
    path: Path,
    payload: bytes,
    *,
    mode: int,
    uid: int = 0,
    gid: int = 0,
) -> bool:
    try:
        existing = _safe_external_file(
            path,
            modes=frozenset({mode}),
            limit=max(len(payload), 1),
            expected_uid=uid,
        )
    except TransportError:
        if path.exists() or path.is_symlink():
            raise
    else:
        if existing != payload:
            raise TransportError("installed transport asset drifted")
        return False
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    try:
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise TransportError("transport asset write failed safely")
            view = view[written:]
        os.fchown(descriptor, uid, gid)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
    except Exception:
        os.close(descriptor)
        path.unlink(missing_ok=True)
        raise
    else:
        os.close(descriptor)
    return True


def _fsync_directory(path: Path, *, mode: int, uid: int = 0, gid: int = 0) -> None:
    _ensure_directory(path, mode=mode, uid=uid, gid=gid)
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace_installed(
    path: Path,
    payload: bytes,
    *,
    mode: int,
    uid: int = 0,
    gid: int = 0,
    parent_mode: int | None = None,
) -> None:
    parent_mode = (
        parent_mode
        if parent_mode is not None
        else (0o700 if path.parent.name in {"identities", "upgrades"} else 0o755)
    )
    _ensure_directory(
        path.parent,
        mode=parent_mode,
        uid=uid if parent_mode == 0o700 and uid else 0,
        gid=gid if parent_mode == 0o700 and gid else 0,
    )
    temporary = path.with_name(f".{path.name}.new-{os.getpid()}-{uuid.uuid4().hex}")
    _install_once(temporary, payload, mode=mode, uid=uid, gid=gid)
    try:
        os.replace(temporary, path)
        _fsync_directory(
            path.parent,
            mode=parent_mode,
            uid=uid if path.parent.name == ".ssh" else 0,
            gid=gid if path.parent.name == ".ssh" else 0,
        )
    finally:
        temporary.unlink(missing_ok=True)


def _remove_installed(
    path: Path,
    *,
    mode: int,
    uid: int = 0,
    parent_mode: int,
    parent_uid: int = 0,
    parent_gid: int = 0,
) -> None:
    if not path.exists() and not path.is_symlink():
        return
    _installed_asset(path, mode=mode, uid=uid)
    path.unlink()
    _fsync_directory(
        path.parent,
        mode=parent_mode,
        uid=parent_uid,
        gid=parent_gid,
    )


def _parse_role_paths(values: Sequence[str], label: str) -> dict[str, Path]:
    parsed: dict[str, Path] = {}
    for value in values:
        role, separator, path = value.partition("=")
        if not separator or ROLE_RE.fullmatch(role) is None or not path or role in parsed:
            raise TransportError(f"{label} mapping is invalid")
        parsed[role] = Path(path)
    return parsed


def _asset_digest(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _client_roles(config: TransportConfig, initiator: str) -> set[str]:
    return {role.name for role in config.roles.values() if role.initiator == initiator}


def bootstrap_client(
    *,
    identity_sources: Mapping[str, Path],
    public_key_sources: Mapping[str, Path],
    known_hosts_source: Path,
    execute: bool,
    layout: Layout | None = None,
    expected_root_uid: int = 0,
    public_resolver: Callable[[Path], bytes] = _derive_public_key,
) -> dict[str, Any]:
    _require_persistent_install_root()
    layout = default_layout() if layout is None else layout
    route_payload = _safe_external_file(
        CHECKED_IN_CONFIG,
        modes=frozenset({0o644, 0o600}),
        limit=MAX_ASSET_BYTES,
        expected_uid=expected_root_uid,
        validate_parents=True,
    )
    config = _load_config_payload(route_payload)
    initiator = config.node_for_hostname(_hostname())
    required_roles = _client_roles(config, initiator)
    if set(identity_sources) != required_roles or set(public_key_sources) != required_roles:
        raise TransportError("transport client bootstrap role set is incomplete")
    program_payload = _safe_external_file(
        Path(__file__),
        modes=frozenset({0o755, 0o700}),
        limit=MAX_ASSET_BYTES,
        expected_uid=expected_root_uid,
        validate_parents=True,
    )
    known_hosts = _safe_external_file(
        known_hosts_source,
        modes=frozenset({0o644, 0o600}),
        limit=MAX_ASSET_BYTES,
        expected_uid=expected_root_uid,
        validate_parents=True,
    )
    if _known_hosts_endpoints(known_hosts) != _required_known_hosts(config, initiator):
        raise TransportError("transport known_hosts does not match the closed route set")
    identities: dict[str, bytes] = {}
    public_keys: dict[str, bytes] = {}
    identity_rows: dict[str, dict[str, str]] = {}
    for role in sorted(required_roles):
        identity = _safe_external_file(
            identity_sources[role],
            modes=frozenset({0o600}),
            limit=MAX_IDENTITY_BYTES,
            expected_uid=expected_root_uid,
            validate_parents=True,
        )
        supplied_public = _safe_external_file(
            public_key_sources[role],
            modes=frozenset({0o644, 0o600}),
            limit=16 * 1024,
            expected_uid=expected_root_uid,
            validate_parents=True,
        )
        canonical_public, fingerprint = _decode_public_key(supplied_public)
        derived_public, derived_fingerprint = _decode_public_key(
            public_resolver(identity_sources[role]),
        )
        if (
            _safe_external_file(
                identity_sources[role],
                modes=frozenset({0o600}),
                limit=MAX_IDENTITY_BYTES,
                expected_uid=expected_root_uid,
                validate_parents=True,
            )
            != identity
        ):
            raise TransportError("transport identity changed during verification")
        if derived_public != canonical_public or derived_fingerprint != fingerprint:
            raise TransportError("transport identity and public key do not match")
        identities[role] = identity
        public_keys[role] = canonical_public
        identity_rows[role] = {
            "identity_sha256": _asset_digest(identity),
            "public_key_fingerprint": fingerprint,
        }
    policy = {
        "schema_version": SCHEMA_VERSION,
        "initiator": initiator,
        "route_config_sha256": _asset_digest(route_payload),
        "program_sha256": _asset_digest(program_payload),
        "known_hosts_sha256": _asset_digest(known_hosts),
        "identities": identity_rows,
    }
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "action": "bootstrap-client",
        "initiator": initiator,
        "roles": sorted(required_roles),
        "known_hosts_sha256": policy["known_hosts_sha256"],
        "identity_fingerprints": {
            role: row["public_key_fingerprint"] for role, row in sorted(identity_rows.items())
        },
        "mutation_authorized": execute,
    }
    if not execute:
        return result
    _ensure_directory(layout.root, mode=INSTALL_ROOT_MODE)
    _ensure_directory(layout.identities, mode=0o700)
    _ensure_directory(layout.public_keys, mode=0o755)
    _ensure_directory(layout.libexec.parent, mode=0o755)
    _install_once(layout.config, route_payload, mode=ROUTE_CONFIG_MODE)
    _install_once(layout.libexec, program_payload, mode=0o755)
    _install_once(layout.known_hosts, known_hosts, mode=0o600)
    for role in sorted(required_roles):
        _install_once(layout.identity(role), identities[role], mode=0o600)
        _install_once(layout.public_key(role), public_keys[role], mode=0o644)
    _install_once(layout.client_policy, _canonical_json(policy), mode=0o600)
    validate_client_install(layout)
    result["status"] = "succeeded"
    return result


def _server_roles(config: TransportConfig, node: str) -> set[str]:
    roles = {
        role.name
        for role in config.roles.values()
        if role.kind == "authority" and node in role.targets
    }
    for route in config.routes.values():
        if route.jump == node:
            roles.add(config.proxy_role(route.initiator, route.node).name)
    return roles


def bootstrap_inventory() -> dict[str, Any]:
    """Render the secret-free, non-authoritative external bootstrap checklist."""
    config = load_config()
    initiators = sorted({role.initiator for role in config.roles.values()})
    return {
        "schema_version": SCHEMA_VERSION,
        "kind": "loom.developer-sandbox.node-transport-bootstrap-inventory",
        "source": "deploy/developer-sandboxes/node-authority-transport.toml",
        "mutation_authorized": False,
        "informational_only": True,
        "node_count": len(config.nodes),
        "initiator_count": len(initiators),
        "excluded_nodes": [],
        "nodes": [
            {
                "node": node,
                "canonical_hostname": config.nodes[node].hostname,
                "server_roles": sorted(_server_roles(config, node)),
            }
            for node in config.nodes
        ],
        "initiators": [
            {
                "node": initiator,
                "canonical_hostname": config.nodes[initiator].hostname,
                "client_roles": sorted(_client_roles(config, initiator)),
                "required_known_hosts_endpoints": sorted(
                    _required_known_hosts(config, initiator),
                ),
            }
            for initiator in initiators
        ],
    }


def _authorized_key_line(role: Role, public_key: bytes) -> str:
    key = public_key.decode("ascii").strip()
    command = f"{LIBEXEC} forced {role.name}"
    return f'restrict,command="{command}" {key} loom-developer-sandbox-transport:{role.name}'


def _converge_authorized_keys(
    layout: Layout,
    expected_lines: Mapping[str, str],
) -> bytes:
    ssh_dir = layout.authorized_keys.parent
    try:
        metadata = ssh_dir.lstat()
    except FileNotFoundError:
        metadata = None
    if metadata is not None and (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != layout.operator_uid
        or metadata.st_gid != layout.operator_gid
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise TransportError("transport SSH directory metadata is unsafe")
    if metadata is None:
        lines: list[str] = []
    elif layout.authorized_keys.exists() or layout.authorized_keys.is_symlink():
        current = _safe_external_file(
            layout.authorized_keys,
            modes=frozenset({0o600}),
            limit=MAX_ASSET_BYTES,
            expected_uid=layout.operator_uid,
        ).decode("utf-8")
        lines = current.splitlines()
    else:
        lines = []
    marker = "loom-developer-sandbox-transport:"
    managed: dict[str, str] = {}
    unrelated: list[str] = []
    for line in lines:
        if marker not in line:
            unrelated.append(line)
            continue
        role = line.rsplit(marker, 1)[-1]
        if role in managed:
            raise TransportError("transport authorized_keys marker is ambiguous")
        managed[role] = line
    for role, line in expected_lines.items():
        existing = managed.pop(role, None)
        if existing is not None and existing != line:
            raise TransportError("transport authorized_keys entry drifted")
    if managed:
        raise TransportError("transport authorized_keys contains an unknown managed role")
    rendered = "\n".join([*unrelated, *[expected_lines[key] for key in sorted(expected_lines)]])
    if rendered:
        rendered += "\n"
    return rendered.encode("utf-8")


def bootstrap_server(
    *,
    public_key_sources: Mapping[str, Path],
    execute: bool,
    layout: Layout | None = None,
    expected_root_uid: int = 0,
) -> dict[str, Any]:
    _require_persistent_install_root()
    layout = default_layout() if layout is None else layout
    route_payload = _safe_external_file(
        CHECKED_IN_CONFIG,
        modes=frozenset({0o644, 0o600}),
        limit=MAX_ASSET_BYTES,
        expected_uid=expected_root_uid,
        validate_parents=True,
    )
    config = _load_config_payload(route_payload)
    node = config.node_for_hostname(_hostname())
    required_roles = _server_roles(config, node)
    if set(public_key_sources) != required_roles:
        raise TransportError("transport server bootstrap role set is incomplete")
    program_payload = _safe_external_file(
        Path(__file__),
        modes=frozenset({0o755, 0o700}),
        limit=MAX_ASSET_BYTES,
        expected_uid=expected_root_uid,
        validate_parents=True,
    )
    public_keys: dict[str, bytes] = {}
    fingerprints: dict[str, str] = {}
    lines: dict[str, str] = {}
    for role_name in sorted(required_roles):
        raw = _safe_external_file(
            public_key_sources[role_name],
            modes=frozenset({0o644, 0o600}),
            limit=16 * 1024,
            expected_uid=expected_root_uid,
            validate_parents=True,
        )
        canonical, fingerprint = _decode_public_key(raw)
        public_keys[role_name] = canonical
        fingerprints[role_name] = fingerprint
        lines[role_name] = _authorized_key_line(config.roles[role_name], canonical)
    rendered = _converge_authorized_keys(layout, lines)
    policy = {
        "schema_version": SCHEMA_VERSION,
        "node": node,
        "route_config_sha256": _asset_digest(route_payload),
        "program_sha256": _asset_digest(program_payload),
        "public_key_fingerprints": fingerprints,
        "authorized_keys_sha256": _asset_digest(rendered),
    }
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "action": "bootstrap-server",
        "node": node,
        "roles": sorted(required_roles),
        "public_key_fingerprints": dict(sorted(fingerprints.items())),
        "mutation_authorized": execute,
    }
    if not execute:
        return result
    _ensure_directory(layout.root, mode=INSTALL_ROOT_MODE)
    _ensure_directory(layout.public_keys, mode=0o755)
    _ensure_directory(layout.libexec.parent, mode=0o755)
    _ensure_directory(
        layout.authorized_keys.parent,
        mode=0o700,
        uid=layout.operator_uid,
        gid=layout.operator_gid,
    )
    _install_once(layout.config, route_payload, mode=ROUTE_CONFIG_MODE)
    _install_once(layout.libexec, program_payload, mode=0o755)
    for role_name in sorted(required_roles):
        _install_once(layout.public_key(role_name), public_keys[role_name], mode=0o644)
    if layout.authorized_keys.exists():
        current = layout.authorized_keys.read_bytes()
        if current != rendered:
            temporary = layout.authorized_keys.with_name(
                f".{layout.authorized_keys.name}.new-{os.getpid()}",
            )
            _install_once(
                temporary,
                rendered,
                mode=0o600,
                uid=layout.operator_uid,
                gid=layout.operator_gid,
            )
            os.replace(temporary, layout.authorized_keys)
    else:
        _install_once(
            layout.authorized_keys,
            rendered,
            mode=0o600,
            uid=layout.operator_uid,
            gid=layout.operator_gid,
        )
    _install_once(
        layout.server_policy,
        _canonical_json(policy),
        mode=SERVER_POLICY_MODE,
    )
    validate_server_install(layout)
    result["status"] = "succeeded"
    return result


def _read_policy(
    path: Path,
    fields: set[str],
    *,
    mode: int = 0o600,
) -> dict[str, Any]:
    raw = _safe_external_file(
        path,
        modes=frozenset({mode}),
        limit=MAX_ASSET_BYTES,
    )
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransportError("installed transport policy is invalid") from exc
    policy = _exact_dict(payload, fields, "installed transport policy")
    if raw != _canonical_json(policy) or policy.get("schema_version") != SCHEMA_VERSION:
        raise TransportError("installed transport policy is invalid")
    return policy


def _installed_asset(
    path: Path,
    *,
    mode: int,
    limit: int = MAX_ASSET_BYTES,
    uid: int = 0,
) -> bytes:
    return _safe_external_file(
        path,
        modes=frozenset({mode}),
        limit=limit,
        expected_uid=uid,
    )


def _install_root_mode(
    layout: Layout,
    *,
    allowed: frozenset[int] = frozenset({INSTALL_ROOT_MODE}),
) -> int:
    try:
        metadata = layout.root.lstat()
    except OSError as exc:
        raise TransportError("transport install root is unavailable") from exc
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != ROOT_UID
        or metadata.st_gid != ROOT_GID
        or mode not in allowed
    ):
        raise TransportError("transport install directory metadata is unsafe")
    return mode


def _set_install_root_mode(
    layout: Layout,
    *,
    expected: frozenset[int],
    target: int,
) -> None:
    current = _install_root_mode(layout, allowed=expected)
    if current == target:
        return
    descriptor = os.open(
        layout.root,
        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != ROOT_UID
            or metadata.st_gid != ROOT_GID
            or stat.S_IMODE(metadata.st_mode) != current
        ):
            raise TransportError("transport install directory changed during mode migration")
        os.fchmod(descriptor, target)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    _install_root_mode(layout, allowed=frozenset({target}))


def _reject_active_upgrade(layout: Layout) -> None:
    if layout.upgrade_active.exists() or layout.upgrade_active.is_symlink():
        raise TransportError("transport runtime admission is disabled during upgrade")


def validate_client_install(
    layout: Layout | None = None,
    *,
    _allow_upgrade: bool = False,
    _allow_legacy_modes: bool = False,
) -> dict[str, Any]:
    layout = default_layout() if layout is None else layout
    _install_root_mode(
        layout,
        allowed=frozenset({0o700 if _allow_legacy_modes else INSTALL_ROOT_MODE}),
    )
    if not _allow_upgrade:
        _reject_active_upgrade(layout)
    config_payload = _installed_asset(
        layout.config,
        mode=0o600 if _allow_legacy_modes else ROUTE_CONFIG_MODE,
    )
    program_payload = _installed_asset(layout.libexec, mode=0o755)
    known_hosts = _installed_asset(layout.known_hosts, mode=0o600)
    config = load_config(layout.config)
    initiator = config.node_for_hostname(_hostname())
    roles = _client_roles(config, initiator)
    policy = _read_policy(
        layout.client_policy,
        {
            "schema_version",
            "initiator",
            "route_config_sha256",
            "program_sha256",
            "known_hosts_sha256",
            "identities",
        },
    )
    if (
        policy["initiator"] != initiator
        or policy["route_config_sha256"] != _asset_digest(config_payload)
        or policy["program_sha256"] != _asset_digest(program_payload)
        or policy["known_hosts_sha256"] != _asset_digest(known_hosts)
        or _known_hosts_endpoints(known_hosts) != _required_known_hosts(config, initiator)
        or not isinstance(policy["identities"], dict)
        or set(policy["identities"]) != roles
    ):
        raise TransportError("installed transport client policy drifted")
    fingerprints: dict[str, str] = {}
    for role in sorted(roles):
        row = _exact_dict(
            policy["identities"][role],
            {"identity_sha256", "public_key_fingerprint"},
            "installed transport identity policy",
        )
        identity = _installed_asset(
            layout.identity(role),
            mode=0o600,
            limit=MAX_IDENTITY_BYTES,
        )
        public_key = _installed_asset(layout.public_key(role), mode=0o644, limit=16 * 1024)
        _, fingerprint = _decode_public_key(public_key)
        if (
            row["identity_sha256"] != _asset_digest(identity)
            or row["public_key_fingerprint"] != fingerprint
        ):
            raise TransportError("installed transport identity drifted")
        fingerprints[role] = fingerprint
    return {
        "schema_version": SCHEMA_VERSION,
        "action": "check-client",
        "initiator": initiator,
        "roles": sorted(roles),
        "public_key_fingerprints": fingerprints,
        "status": "succeeded",
    }


def validate_server_install(
    layout: Layout | None = None,
    *,
    _allow_upgrade: bool = False,
    _allow_legacy_modes: bool = False,
) -> dict[str, Any]:
    layout = default_layout() if layout is None else layout
    _install_root_mode(
        layout,
        allowed=frozenset({0o700 if _allow_legacy_modes else INSTALL_ROOT_MODE}),
    )
    if not _allow_upgrade:
        _reject_active_upgrade(layout)
    config_payload = _installed_asset(
        layout.config,
        mode=0o600 if _allow_legacy_modes else ROUTE_CONFIG_MODE,
    )
    program_payload = _installed_asset(layout.libexec, mode=0o755)
    config = load_config(layout.config)
    node = config.node_for_hostname(_hostname())
    roles = _server_roles(config, node)
    policy = _read_policy(
        layout.server_policy,
        {
            "schema_version",
            "node",
            "route_config_sha256",
            "program_sha256",
            "public_key_fingerprints",
            "authorized_keys_sha256",
        },
        mode=0o600 if _allow_legacy_modes else SERVER_POLICY_MODE,
    )
    if (
        policy["node"] != node
        or policy["route_config_sha256"] != _asset_digest(config_payload)
        or policy["program_sha256"] != _asset_digest(program_payload)
        or not isinstance(policy["public_key_fingerprints"], dict)
        or set(policy["public_key_fingerprints"]) != roles
    ):
        raise TransportError("installed transport server policy drifted")
    expected_lines: dict[str, str] = {}
    for role in sorted(roles):
        public_key = _installed_asset(layout.public_key(role), mode=0o644, limit=16 * 1024)
        canonical, fingerprint = _decode_public_key(public_key)
        if policy["public_key_fingerprints"][role] != fingerprint:
            raise TransportError("installed transport public key drifted")
        expected_lines[role] = _authorized_key_line(config.roles[role], canonical)
    authorized = _converge_authorized_keys(layout, expected_lines)
    if policy["authorized_keys_sha256"] != _asset_digest(authorized):
        raise TransportError("installed transport authorized_keys drifted")
    return {
        "schema_version": SCHEMA_VERSION,
        "action": "check-server",
        "node": node,
        "roles": sorted(roles),
        "status": "succeeded",
    }


def _exists(path: Path) -> bool:
    return path.exists() or path.is_symlink()


def _installed_roles(
    layout: Layout,
    config: TransportConfig,
    node: str,
    *,
    client_installed: bool,
    server_installed: bool,
) -> tuple[set[str], set[str]]:
    client_roles = _client_roles(config, node) if client_installed else set()
    server_roles = _server_roles(config, node) if server_installed else set()
    if _exists(layout.identities):
        _ensure_directory(layout.identities, mode=0o700)
        identity_paths = list(layout.identities.iterdir())
    else:
        identity_paths = []
    if _exists(layout.public_keys):
        _ensure_directory(layout.public_keys, mode=0o755)
        public_paths = list(layout.public_keys.iterdir())
    else:
        public_paths = []
    installed_identities = {path.name for path in identity_paths}
    expected_public = {path.name.removesuffix(".pub") for path in public_paths}
    if (
        installed_identities != client_roles
        or expected_public != client_roles | server_roles
        or any(not path.is_file() or path.is_symlink() for path in identity_paths)
    ):
        raise TransportError("installed transport role inventory drifted")
    if any(
        not path.is_file() or path.is_symlink() or not path.name.endswith(".pub")
        for path in public_paths
    ):
        raise TransportError("installed transport public-key inventory drifted")
    return client_roles, server_roles


def _validate_install_root_inventory(
    layout: Layout,
    *,
    root_mode: int = INSTALL_ROOT_MODE,
) -> None:
    _install_root_mode(layout, allowed=frozenset({root_mode}))
    allowed = {
        layout.config.name,
        layout.known_hosts.name,
        layout.identities.name,
        layout.public_keys.name,
        layout.client_policy.name,
        layout.server_policy.name,
        layout.upgrade_root.name,
        layout.upgrade_active.name,
        layout.upgrade_journal.name,
        layout.upgrade_lock.name,
    }
    foreign = {path.name for path in layout.root.iterdir()} - allowed
    if foreign:
        raise TransportError("installed transport contains foreign state")


def _render_authorized_keys_upgrade(
    layout: Layout,
    old_lines: Mapping[str, str],
    new_lines: Mapping[str, str],
) -> bytes:
    current = _converge_authorized_keys(layout, old_lines).decode("utf-8").splitlines()
    marker = "loom-developer-sandbox-transport:"
    unrelated = [line for line in current if marker not in line]
    rendered = "\n".join([*unrelated, *[new_lines[key] for key in sorted(new_lines)]])
    return (rendered + "\n").encode("utf-8") if rendered else b""


def _upgrade_paths(
    layout: Layout,
    roles: set[str],
    new_identity_roles: set[str],
    *,
    client_installed: bool,
    server_installed: bool,
    install_root_mode: int = INSTALL_ROOT_MODE,
) -> list[tuple[Path, int, int, int, int]]:
    legacy_modes = install_root_mode == 0o700
    paths = [
        (
            layout.config,
            0o600 if legacy_modes else ROUTE_CONFIG_MODE,
            0,
            0,
            install_root_mode,
        ),
        (layout.libexec, 0o755, 0, 0, 0o755),
    ]
    if client_installed:
        paths.extend(
            [
                (layout.known_hosts, 0o600, 0, 0, install_root_mode),
                (layout.client_policy, 0o600, 0, 0, install_root_mode),
            ],
        )
    if server_installed:
        paths.extend(
            [
                (
                    layout.server_policy,
                    0o600 if legacy_modes else SERVER_POLICY_MODE,
                    0,
                    0,
                    install_root_mode,
                ),
                (
                    layout.authorized_keys,
                    0o600,
                    layout.operator_uid,
                    layout.operator_gid,
                    0o700,
                ),
            ],
        )
    for role in sorted(roles):
        paths.append((layout.public_key(role), 0o644, 0, 0, 0o755))
    for role in sorted(new_identity_roles):
        paths.append((layout.identity(role), 0o600, 0, 0, 0o700))
    return paths


def _snapshot_manifest(snapshot: UpgradeSnapshot) -> bytes:
    return _canonical_json(
        {
            "schema_version": SCHEMA_VERSION,
            "upgrade_id": snapshot.upgrade_id,
            "old_config_sha256": snapshot.old_config_sha256,
            "new_config_sha256": snapshot.new_config_sha256,
            "old_install_root_mode": f"{snapshot.old_install_root_mode:04o}",
            "roles": list(snapshot.roles),
            "new_identity_roles": list(snapshot.new_identity_roles),
            "client_installed": snapshot.client_installed,
            "server_installed": snapshot.server_installed,
            "entries": list(snapshot.entries),
        },
    )


def _prepare_upgrade_snapshot(
    layout: Layout,
    *,
    paths: Sequence[tuple[Path, int, int, int, int]],
    roles: set[str],
    new_identity_roles: set[str],
    old_config_sha256: str,
    new_config_sha256: str,
    old_install_root_mode: int,
    client_installed: bool,
    server_installed: bool,
) -> UpgradeSnapshot:
    _ensure_directory(layout.upgrade_root, mode=0o700)
    upgrade_id = (
        f"{time.time_ns()}-{old_config_sha256[:12]}-"
        f"{new_config_sha256[:12]}-{uuid.uuid4().hex[:12]}"
    )
    root = layout.upgrade_root / upgrade_id
    _ensure_directory(root, mode=0o700)
    entries: list[Mapping[str, Any]] = []
    for index, (path, mode, uid, gid, parent_mode) in enumerate(paths):
        exists = _exists(path)
        entry: dict[str, Any] = {
            "path": str(path),
            "mode": f"{mode:04o}",
            "uid": uid,
            "gid": gid,
            "parent_mode": f"{parent_mode:04o}",
            "existed": exists,
            "snapshot": None,
            "sha256": None,
        }
        if exists:
            payload = _installed_asset(path, mode=mode, uid=uid)
            snapshot_name = f"{index:04d}.bin"
            _install_once(root / snapshot_name, payload, mode=0o600)
            entry["snapshot"] = snapshot_name
            entry["sha256"] = _asset_digest(payload)
        entries.append(entry)
    snapshot = UpgradeSnapshot(
        upgrade_id=upgrade_id,
        root=root,
        entries=tuple(entries),
        old_config_sha256=old_config_sha256,
        new_config_sha256=new_config_sha256,
        old_install_root_mode=old_install_root_mode,
        roles=tuple(sorted(roles)),
        new_identity_roles=tuple(sorted(new_identity_roles)),
        client_installed=client_installed,
        server_installed=server_installed,
    )
    _install_once(root / "manifest.json", _snapshot_manifest(snapshot), mode=0o600)
    _fsync_directory(root, mode=0o700)
    _fsync_directory(layout.upgrade_root, mode=0o700)
    return snapshot


def _load_upgrade_snapshot(layout: Layout, root: Path) -> UpgradeSnapshot:
    try:
        resolved = root.resolve(strict=True)
        expected_parent = layout.upgrade_root.resolve(strict=True)
    except OSError as exc:
        raise TransportError("transport upgrade snapshot is unavailable") from exc
    if resolved.parent != expected_parent:
        raise TransportError("transport upgrade snapshot path is invalid")
    _ensure_directory(resolved, mode=0o700)
    raw = _installed_asset(resolved / "manifest.json", mode=0o600)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransportError("transport upgrade snapshot is invalid") from exc
    fields = {
        "schema_version",
        "upgrade_id",
        "old_config_sha256",
        "new_config_sha256",
        "old_install_root_mode",
        "roles",
        "new_identity_roles",
        "client_installed",
        "server_installed",
        "entries",
    }
    if (
        not isinstance(payload, dict)
        or set(payload) != fields
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("upgrade_id") != resolved.name
        or not isinstance(payload.get("roles"), list)
        or any(ROLE_RE.fullmatch(role) is None for role in payload["roles"])
        or payload["roles"] != sorted(set(payload["roles"]))
        or not isinstance(payload.get("new_identity_roles"), list)
        or any(ROLE_RE.fullmatch(role) is None for role in payload["new_identity_roles"])
        or payload["new_identity_roles"] != sorted(set(payload["new_identity_roles"]))
        or not set(payload["new_identity_roles"]).issubset(payload["roles"])
        or not isinstance(payload.get("client_installed"), bool)
        or not isinstance(payload.get("server_installed"), bool)
        or not isinstance(payload.get("entries"), list)
        or any(
            not isinstance(payload.get(field), str)
            or re.fullmatch(r"[0-9a-f]{64}", payload[field]) is None
            for field in ("old_config_sha256", "new_config_sha256")
        )
        or payload.get("old_install_root_mode") not in {"0700", "0755"}
        or raw != _canonical_json(payload)
    ):
        raise TransportError("transport upgrade snapshot is invalid")
    expected_paths = _upgrade_paths(
        layout,
        set(payload["roles"]),
        set(payload["new_identity_roles"]),
        client_installed=payload["client_installed"],
        server_installed=payload["server_installed"],
        install_root_mode=int(payload["old_install_root_mode"], 8),
    )
    if len(payload["entries"]) != len(expected_paths):
        raise TransportError("transport upgrade snapshot inventory is invalid")
    for index, (entry, expected) in enumerate(zip(payload["entries"], expected_paths, strict=True)):
        path, mode, uid, gid, parent_mode = expected
        if (
            not isinstance(entry, dict)
            or set(entry)
            != {
                "path",
                "mode",
                "uid",
                "gid",
                "parent_mode",
                "existed",
                "snapshot",
                "sha256",
            }
            or entry["path"] != str(path)
            or entry["mode"] != f"{mode:04o}"
            or entry["uid"] != uid
            or entry["gid"] != gid
            or entry["parent_mode"] != f"{parent_mode:04o}"
            or not isinstance(entry["existed"], bool)
        ):
            raise TransportError("transport upgrade snapshot inventory is invalid")
        if entry["existed"]:
            if (
                entry["snapshot"] != f"{index:04d}.bin"
                or not isinstance(entry["sha256"], str)
                or re.fullmatch(r"[0-9a-f]{64}", entry["sha256"]) is None
            ):
                raise TransportError("transport upgrade snapshot inventory is invalid")
            saved = _installed_asset(resolved / entry["snapshot"], mode=0o600)
            if _asset_digest(saved) != entry["sha256"]:
                raise TransportError("transport upgrade snapshot digest drifted")
        elif entry["snapshot"] is not None or entry["sha256"] is not None:
            raise TransportError("transport upgrade snapshot inventory is invalid")
    return UpgradeSnapshot(
        upgrade_id=payload["upgrade_id"],
        root=resolved,
        entries=tuple(payload["entries"]),
        old_config_sha256=payload["old_config_sha256"],
        new_config_sha256=payload["new_config_sha256"],
        old_install_root_mode=int(payload["old_install_root_mode"], 8),
        roles=tuple(payload["roles"]),
        new_identity_roles=tuple(payload["new_identity_roles"]),
        client_installed=payload["client_installed"],
        server_installed=payload["server_installed"],
    )


def _write_upgrade_active(layout: Layout, snapshot: UpgradeSnapshot, phase: str) -> None:
    root_mode = _install_root_mode(
        layout,
        allowed=frozenset({0o700, INSTALL_ROOT_MODE}),
    )
    payload = _canonical_json(
        {
            "schema_version": SCHEMA_VERSION,
            "upgrade_id": snapshot.upgrade_id,
            "snapshot": str(snapshot.root),
            "phase": phase,
        },
    )
    if _exists(layout.upgrade_active):
        _replace_installed(
            layout.upgrade_active,
            payload,
            mode=0o600,
            parent_mode=root_mode,
        )
    else:
        _install_once(layout.upgrade_active, payload, mode=0o600)
        _fsync_directory(layout.root, mode=root_mode)


def _read_upgrade_active(layout: Layout) -> tuple[UpgradeSnapshot, str] | None:
    if not _exists(layout.upgrade_active):
        return None
    raw = _installed_asset(layout.upgrade_active, mode=0o600)
    try:
        payload = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransportError("transport active upgrade is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != {"schema_version", "upgrade_id", "snapshot", "phase"}
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("phase") not in {"prepared", "assets-replaced", "committed"}
        or not isinstance(payload.get("snapshot"), str)
        or raw != _canonical_json(payload)
    ):
        raise TransportError("transport active upgrade is invalid")
    snapshot = _load_upgrade_snapshot(layout, Path(payload["snapshot"]))
    if payload["upgrade_id"] != snapshot.upgrade_id:
        raise TransportError("transport active upgrade binding is invalid")
    return snapshot, payload["phase"]


def _append_upgrade_journal(
    layout: Layout,
    snapshot: UpgradeSnapshot,
    phase: str,
) -> None:
    record = _canonical_json(
        {
            "schema_version": SCHEMA_VERSION,
            "upgrade_id": snapshot.upgrade_id,
            "old_config_sha256": snapshot.old_config_sha256,
            "new_config_sha256": snapshot.new_config_sha256,
            "phase": phase,
        },
    )
    descriptor = os.open(
        layout.upgrade_journal,
        os.O_WRONLY | os.O_APPEND | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != ROOT_UID
            or metadata.st_gid != ROOT_GID
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise TransportError("transport upgrade journal metadata is unsafe")
        view = memoryview(record)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise TransportError("transport upgrade journal write failed safely")
            view = view[written:]
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_upgrade_state(layout: Layout) -> None:
    _install_root_mode(
        layout,
        allowed=frozenset({0o700, INSTALL_ROOT_MODE}),
    )
    _ensure_directory(layout.upgrade_root, mode=0o700)
    for path in (layout.upgrade_lock, layout.upgrade_journal):
        if not _exists(path):
            _install_once(path, b"", mode=0o600)
        _installed_asset(path, mode=0o600)
    raw = _installed_asset(layout.upgrade_journal, mode=0o600)
    for line in raw.splitlines(keepends=True):
        try:
            record = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransportError("transport upgrade journal is invalid") from exc
        if (
            not isinstance(record, dict)
            or set(record)
            != {
                "schema_version",
                "upgrade_id",
                "old_config_sha256",
                "new_config_sha256",
                "phase",
            }
            or record.get("schema_version") != SCHEMA_VERSION
            or record.get("phase")
            not in {
                "prepared",
                "assets-replaced",
                "committed",
                "recovered-committed",
                "rolled-back",
                "recovered-rolled-back",
            }
            or line != _canonical_json(record)
        ):
            raise TransportError("transport upgrade journal is invalid")


def _remove_active(layout: Layout) -> None:
    root_mode = _install_root_mode(
        layout,
        allowed=frozenset({0o700, INSTALL_ROOT_MODE}),
    )
    _remove_installed(
        layout.upgrade_active,
        mode=0o600,
        parent_mode=root_mode,
    )


def _restore_upgrade_snapshot(layout: Layout, snapshot: UpgradeSnapshot) -> None:
    _set_install_root_mode(
        layout,
        expected=frozenset({0o700, INSTALL_ROOT_MODE}),
        target=snapshot.old_install_root_mode,
    )
    for entry in snapshot.entries:
        path = Path(str(entry["path"]))
        mode = int(str(entry["mode"]), 8)
        uid = int(entry["uid"])
        gid = int(entry["gid"])
        parent_mode = int(str(entry["parent_mode"]), 8)
        if entry["existed"]:
            payload = _installed_asset(snapshot.root / str(entry["snapshot"]), mode=0o600)
            _replace_installed(
                path,
                payload,
                mode=mode,
                uid=uid,
                gid=gid,
                parent_mode=parent_mode,
            )
        else:
            _remove_installed(
                path,
                mode=mode,
                uid=uid,
                parent_mode=parent_mode,
                parent_uid=layout.operator_uid if path.parent.name == ".ssh" else 0,
                parent_gid=layout.operator_gid if path.parent.name == ".ssh" else 0,
            )
    if snapshot.client_installed:
        validate_client_install(
            layout,
            _allow_upgrade=True,
            _allow_legacy_modes=snapshot.old_install_root_mode == 0o700,
        )
    if snapshot.server_installed:
        validate_server_install(
            layout,
            _allow_upgrade=True,
            _allow_legacy_modes=snapshot.old_install_root_mode == 0o700,
        )


def _recover_upgrade(layout: Layout) -> str | None:
    active = _read_upgrade_active(layout)
    if active is None:
        return None
    snapshot, phase = active
    if phase == "committed":
        if snapshot.client_installed:
            validate_client_install(layout, _allow_upgrade=True)
        if snapshot.server_installed:
            validate_server_install(layout, _allow_upgrade=True)
        _append_upgrade_journal(layout, snapshot, "recovered-committed")
        _remove_active(layout)
        return "recovered-committed"
    _restore_upgrade_snapshot(layout, snapshot)
    _append_upgrade_journal(layout, snapshot, "recovered-rolled-back")
    _remove_active(layout)
    return "recovered-rolled-back"


def upgrade(
    *,
    identity_sources: Mapping[str, Path],
    public_key_sources: Mapping[str, Path],
    known_hosts_source: Path | None,
    execute: bool,
    layout: Layout | None = None,
    expected_root_uid: int = 0,
    public_resolver: Callable[[Path], bytes] = _derive_public_key,
) -> dict[str, Any]:
    _require_persistent_install_root()
    layout = default_layout() if layout is None else layout
    if not _exists(layout.config) or not _exists(layout.libexec):
        raise TransportError("transport upgrade requires an installed transport")
    lock: int | None = None
    if execute:
        _ensure_upgrade_state(layout)
        lock = os.open(
            layout.upgrade_lock,
            os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
        )
        fcntl.flock(lock, fcntl.LOCK_EX)
    else:
        _reject_active_upgrade(layout)
    try:
        recovered = _recover_upgrade(layout) if execute else None
        old_install_root_mode = _install_root_mode(
            layout,
            allowed=frozenset({0o700, INSTALL_ROOT_MODE}),
        )
        legacy_modes = old_install_root_mode == 0o700
        _validate_install_root_inventory(
            layout,
            root_mode=old_install_root_mode,
        )
        old_config_payload = _installed_asset(
            layout.config,
            mode=0o600 if legacy_modes else ROUTE_CONFIG_MODE,
        )
        old_program_payload = _installed_asset(layout.libexec, mode=0o755)
        old_config = load_config(layout.config)
        old_node = old_config.node_for_hostname(_hostname())
        client_installed = _exists(layout.client_policy)
        server_installed = _exists(layout.server_policy)
        if not client_installed and not server_installed:
            raise TransportError("transport installed role is unavailable")
        if client_installed:
            validate_client_install(
                layout,
                _allow_upgrade=True,
                _allow_legacy_modes=legacy_modes,
            )
        if server_installed:
            validate_server_install(
                layout,
                _allow_upgrade=True,
                _allow_legacy_modes=legacy_modes,
            )
        old_client_roles, old_server_roles = _installed_roles(
            layout,
            old_config,
            old_node,
            client_installed=client_installed,
            server_installed=server_installed,
        )
        route_payload = _safe_external_file(
            CHECKED_IN_CONFIG,
            modes=frozenset({0o644, 0o600}),
            limit=MAX_ASSET_BYTES,
            expected_uid=expected_root_uid,
            validate_parents=True,
        )
        program_payload = _safe_external_file(
            Path(__file__),
            modes=frozenset({0o755, 0o700}),
            limit=MAX_ASSET_BYTES,
            expected_uid=expected_root_uid,
            validate_parents=True,
        )
        new_config = _load_config_payload(route_payload)
        new_node = new_config.node_for_hostname(_hostname())
        if new_node != old_node:
            raise TransportError("transport upgrade host role binding drifted")
        new_client_roles = _client_roles(new_config, new_node) if client_installed else set()
        new_server_roles = _server_roles(new_config, new_node) if server_installed else set()
        if old_client_roles - new_client_roles:
            raise TransportError(
                "transport client identity retirement requires a separate operation",
            )
        new_identity_roles = new_client_roles - old_client_roles
        new_public_roles = (new_client_roles | new_server_roles) - (
            old_client_roles | old_server_roles
        )
        if set(identity_sources) != new_identity_roles:
            raise TransportError("transport upgrade identity input set is not closed")
        if set(public_key_sources) != new_public_roles:
            raise TransportError("transport upgrade public-key input set is not closed")

        identities = {
            role: _installed_asset(
                layout.identity(role),
                mode=0o600,
                limit=MAX_IDENTITY_BYTES,
            )
            for role in old_client_roles & new_client_roles
        }
        public_keys = {
            role: _installed_asset(layout.public_key(role), mode=0o644, limit=16 * 1024)
            for role in (old_client_roles | old_server_roles)
            & (new_client_roles | new_server_roles)
        }
        for role in sorted(new_identity_roles):
            identities[role] = _safe_external_file(
                identity_sources[role],
                modes=frozenset({0o600}),
                limit=MAX_IDENTITY_BYTES,
                expected_uid=expected_root_uid,
                validate_parents=True,
            )
        for role in sorted(new_public_roles):
            public_keys[role] = _safe_external_file(
                public_key_sources[role],
                modes=frozenset({0o644, 0o600}),
                limit=16 * 1024,
                expected_uid=expected_root_uid,
                validate_parents=True,
            )
        canonical_public: dict[str, bytes] = {}
        fingerprints: dict[str, str] = {}
        for role, raw in public_keys.items():
            canonical_public[role], fingerprints[role] = _decode_public_key(raw)
        for role in sorted(new_client_roles):
            identity_path = (
                identity_sources[role] if role in new_identity_roles else layout.identity(role)
            )
            derived, derived_fingerprint = _decode_public_key(
                public_resolver(identity_path),
            )
            if role in new_identity_roles and (
                _safe_external_file(
                    identity_path,
                    modes=frozenset({0o600}),
                    limit=MAX_IDENTITY_BYTES,
                    expected_uid=expected_root_uid,
                    validate_parents=True,
                )
                != identities[role]
            ):
                raise TransportError("transport identity changed during verification")
            if derived != canonical_public[role] or derived_fingerprint != fingerprints[role]:
                raise TransportError("transport identity and public key do not match")

        known_hosts: bytes | None = None
        if client_installed:
            old_known_hosts = _installed_asset(layout.known_hosts, mode=0o600)
            required_endpoints = _required_known_hosts(new_config, new_node)
            if _known_hosts_endpoints(old_known_hosts) == required_endpoints:
                if known_hosts_source is not None:
                    raise TransportError("transport upgrade known_hosts input is unexpected")
                known_hosts = old_known_hosts
            else:
                if known_hosts_source is None:
                    raise TransportError("transport upgrade requires explicit known_hosts")
                known_hosts = _safe_external_file(
                    known_hosts_source,
                    modes=frozenset({0o644, 0o600}),
                    limit=MAX_ASSET_BYTES,
                    expected_uid=expected_root_uid,
                    validate_parents=True,
                )
                if _known_hosts_endpoints(known_hosts) != required_endpoints:
                    raise TransportError(
                        "transport known_hosts does not match the closed route set",
                    )
        elif known_hosts_source is not None:
            raise TransportError("transport upgrade known_hosts input is unexpected")

        client_policy: bytes | None = None
        if client_installed:
            assert known_hosts is not None
            client_policy = _canonical_json(
                {
                    "schema_version": SCHEMA_VERSION,
                    "initiator": new_node,
                    "route_config_sha256": _asset_digest(route_payload),
                    "program_sha256": _asset_digest(program_payload),
                    "known_hosts_sha256": _asset_digest(known_hosts),
                    "identities": {
                        role: {
                            "identity_sha256": _asset_digest(identities[role]),
                            "public_key_fingerprint": fingerprints[role],
                        }
                        for role in sorted(new_client_roles)
                    },
                },
            )
        authorized_keys: bytes | None = None
        server_policy: bytes | None = None
        if server_installed:
            old_lines = {
                role: _authorized_key_line(
                    old_config.roles[role],
                    _installed_asset(layout.public_key(role), mode=0o644),
                )
                for role in old_server_roles
            }
            new_lines = {
                role: _authorized_key_line(new_config.roles[role], canonical_public[role])
                for role in new_server_roles
            }
            authorized_keys = _render_authorized_keys_upgrade(
                layout,
                old_lines,
                new_lines,
            )
            server_policy = _canonical_json(
                {
                    "schema_version": SCHEMA_VERSION,
                    "node": new_node,
                    "route_config_sha256": _asset_digest(route_payload),
                    "program_sha256": _asset_digest(program_payload),
                    "public_key_fingerprints": {
                        role: fingerprints[role] for role in sorted(new_server_roles)
                    },
                    "authorized_keys_sha256": _asset_digest(authorized_keys),
                },
            )
        changed = (
            old_config_payload != route_payload
            or old_program_payload != program_payload
            or (
                client_policy is not None
                and _installed_asset(layout.client_policy, mode=0o600) != client_policy
            )
            or (
                server_policy is not None
                and _installed_asset(
                    layout.server_policy,
                    mode=0o600 if legacy_modes else SERVER_POLICY_MODE,
                )
                != server_policy
            )
            or legacy_modes
        )
        result: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "action": "upgrade",
            "node": new_node,
            "client_installed": client_installed,
            "server_installed": server_installed,
            "client_roles": sorted(new_client_roles),
            "server_roles": sorted(new_server_roles),
            "new_public_key_fingerprints": {
                role: fingerprints[role] for role in sorted(new_public_roles)
            },
            "changed": changed,
            "mutation_authorized": execute,
            "recovered": recovered,
        }
        if not execute or not changed:
            result["status"] = "succeeded"
            return result
        all_roles = old_client_roles | old_server_roles | new_client_roles | new_server_roles
        snapshot = _prepare_upgrade_snapshot(
            layout,
            paths=_upgrade_paths(
                layout,
                all_roles,
                new_identity_roles,
                client_installed=client_installed,
                server_installed=server_installed,
                install_root_mode=old_install_root_mode,
            ),
            roles=all_roles,
            new_identity_roles=new_identity_roles,
            old_config_sha256=_asset_digest(old_config_payload),
            new_config_sha256=_asset_digest(route_payload),
            old_install_root_mode=old_install_root_mode,
            client_installed=client_installed,
            server_installed=server_installed,
        )
        try:
            _write_upgrade_active(layout, snapshot, "prepared")
            _append_upgrade_journal(layout, snapshot, "prepared")
            _replace_installed(
                layout.config,
                route_payload,
                mode=ROUTE_CONFIG_MODE,
                parent_mode=old_install_root_mode,
            )
            for role in sorted(new_identity_roles):
                _replace_installed(
                    layout.identity(role),
                    identities[role],
                    mode=0o600,
                    parent_mode=0o700,
                )
            for role in sorted(all_roles - (new_client_roles | new_server_roles)):
                _remove_installed(
                    layout.public_key(role),
                    mode=0o644,
                    parent_mode=0o755,
                )
            for role in sorted(new_client_roles | new_server_roles):
                _replace_installed(
                    layout.public_key(role),
                    canonical_public[role],
                    mode=0o644,
                    parent_mode=0o755,
                )
            if client_policy is not None and known_hosts is not None:
                _replace_installed(
                    layout.known_hosts,
                    known_hosts,
                    mode=0o600,
                    parent_mode=old_install_root_mode,
                )
                _replace_installed(
                    layout.client_policy,
                    client_policy,
                    mode=0o600,
                    parent_mode=old_install_root_mode,
                )
            if server_policy is not None and authorized_keys is not None:
                _replace_installed(
                    layout.authorized_keys,
                    authorized_keys,
                    mode=0o600,
                    uid=layout.operator_uid,
                    gid=layout.operator_gid,
                    parent_mode=0o700,
                )
                _replace_installed(
                    layout.server_policy,
                    server_policy,
                    mode=SERVER_POLICY_MODE,
                    parent_mode=old_install_root_mode,
                )
            # Replace the dispatcher last. The active marker keeps both the old
            # and new program fail closed while the transaction is incomplete.
            _replace_installed(layout.libexec, program_payload, mode=0o755, parent_mode=0o755)
            _set_install_root_mode(
                layout,
                expected=frozenset({old_install_root_mode}),
                target=INSTALL_ROOT_MODE,
            )
            _write_upgrade_active(layout, snapshot, "assets-replaced")
            _append_upgrade_journal(layout, snapshot, "assets-replaced")
            if client_installed:
                validate_client_install(layout, _allow_upgrade=True)
            if server_installed:
                validate_server_install(layout, _allow_upgrade=True)
            _write_upgrade_active(layout, snapshot, "committed")
            _append_upgrade_journal(layout, snapshot, "committed")
            _remove_active(layout)
            result["snapshot"] = str(snapshot.root)
            result["status"] = "succeeded"
            return result
        except Exception as upgrade_exc:
            try:
                _restore_upgrade_snapshot(layout, snapshot)
                _append_upgrade_journal(layout, snapshot, "rolled-back")
                _remove_active(layout)
            except Exception as rollback_exc:
                raise TransportError(
                    "transport upgrade and rollback both failed safely",
                ) from rollback_exc
            raise TransportError(
                "transport upgrade failed and was rolled back",
            ) from upgrade_exc
    finally:
        if lock is not None:
            os.close(lock)


def _base_ssh_argv(
    *,
    identity: Path,
    known_hosts: Path,
    route: Route,
) -> list[str]:
    return [
        SSH,
        "-F",
        "/dev/null",
        "-o",
        "BatchMode=yes",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "IdentityAgent=none",
        "-o",
        "PreferredAuthentications=publickey",
        "-o",
        "PasswordAuthentication=no",
        "-o",
        "KbdInteractiveAuthentication=no",
        "-o",
        "GSSAPIAuthentication=no",
        "-o",
        "HostbasedAuthentication=no",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GlobalKnownHostsFile=/dev/null",
        "-o",
        "UpdateHostKeys=no",
        "-o",
        "ControlMaster=no",
        "-o",
        "ControlPath=none",
        "-o",
        "ControlPersist=no",
        "-o",
        "ForwardAgent=no",
        "-o",
        "ClearAllForwardings=yes",
        "-o",
        "RequestTTY=no",
        "-o",
        "PermitLocalCommand=no",
        "-o",
        "ConnectTimeout=10",
        "-i",
        str(identity),
        "-l",
        OPERATOR,
        "-p",
        str(route.port),
        route.address,
    ]


def _remote_ssh_argv(
    config: TransportConfig,
    layout: Layout,
    *,
    initiator: str,
    node: str,
    verb: str,
    role: Role | None = None,
) -> list[str]:
    role = role or config.authority_role(initiator, node, verb)
    route = config.route(initiator, node)
    argv = _base_ssh_argv(
        identity=layout.identity(role.name),
        known_hosts=layout.known_hosts,
        route=route,
    )
    if route.jump is not None:
        argv[1:1] = [
            "-o",
            f"ProxyCommand={layout.libexec} proxy-client --node {node}",
        ]
    return [*argv, SUDO, "-n", str(config.authority_program), verb]


def _invoke_timeout(node: str, verb: str, envelope: bytes) -> int:
    if node != "oldlab-2" or verb != "transact":
        return DEFAULT_INVOKE_TIMEOUT_SECONDS
    try:
        outer = json.loads(envelope)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return DEFAULT_INVOKE_TIMEOUT_SECONDS
    if not isinstance(outer, dict) or set(outer) != REQUEST_FIELDS:
        return DEFAULT_INVOKE_TIMEOUT_SECONDS
    unsigned = dict(outer)
    unsigned.pop("request_id", None)
    canonical_unsigned = (
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
    )
    try:
        payload = base64.b64decode(str(outer["payload_base64"]), validate=True)
        inner = json.loads(payload)
        canonical_outer = (
            json.dumps(outer, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        )
        canonical_inner = (
            json.dumps(inner, sort_keys=True, separators=(",", ":")).encode("ascii") + b"\n"
        )
    except (UnicodeEncodeError, UnicodeDecodeError, ValueError, json.JSONDecodeError):
        return DEFAULT_INVOKE_TIMEOUT_SECONDS
    sha_pattern = re.compile(r"[0-9a-f]{40}\Z")
    digest_pattern = re.compile(r"[0-9a-f]{64}\Z")
    requested_at = inner.get("requested_at") if isinstance(inner, dict) else None
    try:
        requested_time = (
            datetime.fromisoformat(requested_at[:-1] + "+00:00").astimezone(UTC)
            if isinstance(requested_at, str) and requested_at.endswith("Z")
            else None
        )
    except ValueError:
        requested_time = None
    if (
        envelope != canonical_outer
        or not isinstance(inner, dict)
        or set(inner) != INFRASTRUCTURE_CONVERGE_FIELDS
        or payload != canonical_inner
        or outer.get("schema_version") != SCHEMA_VERSION
        or outer.get("action") != "staging-infrastructure-converge"
        or outer.get("node") != "oldlab-2"
        or outer.get("domain") != "oldlab"
        or outer.get("sandbox") != "staging"
        or outer.get("payload_kind") != "staging-infrastructure-converge-request"
        or outer.get("prior_request_id") is not None
        or sha_pattern.fullmatch(str(outer.get("candidate_sha"))) is None
        or sha_pattern.fullmatch(str(outer.get("candidate_tree"))) is None
        or digest_pattern.fullmatch(str(outer.get("payload_sha256"))) is None
        or outer["payload_sha256"] != hashlib.sha256(payload).hexdigest()
        or digest_pattern.fullmatch(str(outer.get("request_id"))) is None
        or outer["request_id"] != hashlib.sha256(canonical_unsigned).hexdigest()
        or inner.get("schema_version") != SCHEMA_VERSION
        or inner.get("kind") != "loom.staging-external-slurm.infrastructure-converge-request"
        or inner.get("candidate_sha") != outer["candidate_sha"]
        or inner.get("candidate_tree") != outer["candidate_tree"]
        or digest_pattern.fullmatch(str(inner.get("convergence_id"))) is None
        or requested_time is None
        or requested_time.isoformat().replace("+00:00", "Z") != requested_at
    ):
        return DEFAULT_INVOKE_TIMEOUT_SECONDS
    return INFRASTRUCTURE_CONVERGE_TIMEOUT_SECONDS


def _validate_identity_preflight_route(verb: str, envelope: bytes) -> None:
    try:
        outer = json.loads(envelope)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return
    if not isinstance(outer, dict) or outer.get("action") != IDENTITY_PREFLIGHT_ACTION:
        return
    if (
        verb != "check"
        or not REQUEST_FIELDS.issubset(outer)
        or outer.get("payload_kind") != IDENTITY_PREFLIGHT_PAYLOAD_KIND
    ):
        raise TransportError("identity preflight is outside the read-only route")


def _registry_module() -> Any:
    source = (
        REGISTRY_VERIFIER_SOURCE
        if REGISTRY_VERIFIER_SOURCE.exists()
        else REPO_ROOT / REGISTRY_MODULE_RELATIVE
        if Path(__file__).resolve().is_relative_to(REPO_ROOT)
        else REGISTRY_VERIFIER_SOURCE
    )
    spec = importlib.util.spec_from_file_location(
        "_loom_transport_developer_environment_registry",
        source,
    )
    if spec is None or spec.loader is None:
        raise TransportError("registry snapshot verifier is unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except (OSError, ImportError) as exc:
        raise TransportError("registry snapshot verifier is unavailable") from exc
    return module


def _verified_registry_snapshot(raw: bytes) -> dict[str, Any]:
    try:
        snapshot = _registry_module().DeveloperEnvironmentRegistry.verify_snapshot(raw)
    except Exception as exc:
        raise TransportError("registry snapshot is invalid") from exc
    if not isinstance(snapshot, dict):
        raise TransportError("registry snapshot is invalid")
    return snapshot


def _registry_binding_from_envelope(
    node: str,
    envelope: bytes,
) -> tuple[dict[str, Any], tuple[int, str]] | None:
    try:
        outer = json.loads(envelope)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransportError("registry-bound transport request is invalid") from exc
    if not isinstance(outer, dict):
        raise TransportError("registry-bound transport request is invalid")
    action = outer.get("action")
    if action == REGISTRY_SNAPSHOT_SYNC_ACTION:
        return None
    if action not in REGISTRY_DEPENDENT_ACTIONS:
        return None
    unsigned = dict(outer)
    unsigned.pop("request_id", None)
    if (
        envelope != _canonical_json(outer)
        or outer.get("schema_version") != SCHEMA_VERSION
        or outer.get("node") != node
        or outer.get("request_id") != hashlib.sha256(_canonical_json(unsigned)).hexdigest()
        or not isinstance(outer.get("payload_base64"), str)
        or not isinstance(outer.get("payload_sha256"), str)
    ):
        raise TransportError("registry-bound transport request is invalid")
    try:
        inner_raw = base64.b64decode(outer["payload_base64"], validate=True)
    except (binascii.Error, ValueError) as exc:
        raise TransportError("registry-bound transport request payload is invalid") from exc
    if hashlib.sha256(inner_raw).hexdigest() != outer["payload_sha256"]:
        raise TransportError("registry-bound transport request payload is invalid")
    outer_binding = (
        outer.get("registry_generation"),
        outer.get("registry_payload_sha256"),
    )
    inner_binding: tuple[object, object] = (None, None)
    if inner_raw:
        try:
            inner = json.loads(inner_raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            inner = None
        if isinstance(inner, dict):
            inner_binding = (
                inner.get("registry_generation"),
                inner.get(
                    "registry_payload_sha256",
                    inner.get("registry_snapshot_sha256"),
                ),
            )
    bindings = [binding for binding in (outer_binding, inner_binding) if binding != (None, None)]
    if (
        not bindings
        or any(
            type(generation) is not int
            or int(generation) < 1
            or re.fullmatch(r"[0-9a-f]{64}", str(digest)) is None
            for generation, digest in bindings
        )
        or len(set((int(generation), str(digest)) for generation, digest in bindings)) != 1
    ):
        raise TransportError("registry-bound transport request has no exact snapshot binding")
    generation, digest = bindings[0]
    return outer, (int(generation), str(digest))


def _registry_sync_envelope(
    original: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    snapshot_raw: bytes,
) -> bytes:
    unsigned = {
        "schema_version": SCHEMA_VERSION,
        "action": REGISTRY_SNAPSHOT_SYNC_ACTION,
        "node": original["node"],
        "domain": original["domain"],
        "sandbox": original["sandbox"],
        "candidate_sha": original["candidate_sha"],
        "candidate_tree": original["candidate_tree"],
        "registry_generation": snapshot["generation"],
        "registry_payload_sha256": snapshot["payload_sha256"],
        "payload_kind": REGISTRY_SNAPSHOT_SYNC_KIND,
        "payload_sha256": hashlib.sha256(snapshot_raw).hexdigest(),
        "payload_base64": base64.b64encode(snapshot_raw).decode("ascii"),
        "prior_request_id": None,
    }
    return _canonical_json(
        {
            **unsigned,
            "request_id": hashlib.sha256(_canonical_json(unsigned)).hexdigest(),
        },
    )


def _validate_worker_image_binding(
    outer: Mapping[str, Any],
    snapshot: Mapping[str, Any],
) -> None:
    """Fail before transport when a domain image ID is absent or mismatched."""
    try:
        decoded = base64.b64decode(str(outer["payload_base64"]), validate=True)
        inner = json.loads(decoded) if decoded else {}
    except (binascii.Error, UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise TransportError("registry worker image binding is invalid") from exc
    if not isinstance(inner, dict):
        raise TransportError("registry worker image binding is invalid")
    worker_image_id = outer.get("worker_image_id", inner.get("worker_image_id"))
    if worker_image_id is None:
        if outer.get("action") in WORKER_IMAGE_BOUND_ACTIONS:
            raise TransportError("registry worker image binding is invalid")
        return
    domain = outer.get("domain")
    architecture = {"oldlab": "amd64", "gb10": "arm64"}.get(str(domain))
    candidate_id = outer.get("candidate_id", inner.get("candidate_id"))
    deployment_id = outer.get("deployment_id", inner.get("deployment_id"))
    env_id = outer.get("env_id", inner.get("env_id"))
    sandbox = outer.get("sandbox", inner.get("sandbox"))
    resource_generation = outer.get(
        "resource_generation",
        inner.get("resource_generation"),
    )
    candidates = [
        row
        for row in snapshot.get("candidates", [])
        if isinstance(row, dict)
        and row.get("candidate_id") == candidate_id
        and (env_id is None or row.get("env_id") == env_id)
        and row.get("candidate_sha") == outer.get("candidate_sha")
        and row.get("candidate_tree") == outer.get("candidate_tree")
    ]
    environments = [
        row
        for row in snapshot.get("environments", [])
        if isinstance(row, dict)
        and (env_id is None or row.get("env_id") == env_id)
        and (sandbox is None or row.get("runtime_id") == sandbox)
        and row.get("state") in {"deploying", "active"}
    ]
    if len(environments) != 1:
        raise TransportError("registry worker image binding is invalid")
    environment = environments[0]
    if resource_generation is not None and resource_generation != environment.get(
        "resource_generation"
    ):
        raise TransportError("registry worker image binding is invalid")
    matching_deployments = [
        row
        for row in snapshot.get("deployments", [])
        if isinstance(row, dict)
        and row.get("env_id") == environment.get("env_id")
        and row.get("candidate_id") == candidate_id
        and isinstance(row.get("worker_runtime_bindings"), dict)
        and (deployment_id is None or row.get("deployment_id") == deployment_id)
        and (
            (
                environment.get("state") == "deploying"
                and row.get("phase") not in {"committed", "failed"}
                and row.get("expected_resource_generation")
                == environment.get("resource_generation")
            )
            or (
                environment.get("state") == "active"
                and row.get("phase") == "committed"
                and row.get("applied_resource_generation") == environment.get("resource_generation")
            )
        )
    ]
    deployment = matching_deployments[0] if len(matching_deployments) == 1 else {}
    bindings = deployment.get("worker_runtime_bindings")
    domains = bindings.get("domains") if isinstance(bindings, dict) else None
    domain_binding = domains.get(domain) if isinstance(domains, dict) else None
    expected_image_id = (
        domain_binding.get("runtime_image_id") if isinstance(domain_binding, dict) else None
    )
    if (
        architecture is None
        or re.fullmatch(r"sha256:[0-9a-f]{64}", str(worker_image_id)) is None
        or len(candidates) != 1
        or expected_image_id != worker_image_id
    ):
        raise TransportError("registry worker image binding is invalid")


def _validate_registry_sync_receipt(
    completed: subprocess.CompletedProcess[bytes],
    *,
    sync_envelope: bytes,
    snapshot: Mapping[str, Any],
) -> None:
    try:
        request = json.loads(sync_envelope)
        receipt = json.loads(completed.stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransportError("registry snapshot sync receipt is invalid") from exc
    expected_fields = {
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
        "registry_generation",
        "registry_payload_sha256",
        "source_sha",
        "source_tree",
    }
    if (
        not isinstance(request, dict)
        or not isinstance(receipt, dict)
        or set(receipt) != expected_fields
        or completed.stdout != _canonical_json(receipt)
        or receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("request_id") != request.get("request_id")
        or receipt.get("action") != REGISTRY_SNAPSHOT_SYNC_ACTION
        or receipt.get("node") != request.get("node")
        or receipt.get("domain") != request.get("domain")
        or receipt.get("sandbox") != request.get("sandbox")
        or receipt.get("candidate_sha") != request.get("candidate_sha")
        or receipt.get("candidate_tree") != request.get("candidate_tree")
        or receipt.get("payload_sha256") != request.get("payload_sha256")
        or receipt.get("registry_generation") != snapshot["generation"]
        or receipt.get("registry_payload_sha256") != snapshot["payload_sha256"]
        or re.fullmatch(r"[0-9a-f]{40}", str(receipt.get("source_sha"))) is None
        or re.fullmatch(r"[0-9a-f]{40}", str(receipt.get("source_tree"))) is None
        or receipt.get("inner_receipt") is not None
        or receipt.get("status") != "succeeded"
        or re.fullmatch(r"[0-9a-f]{64}", str(receipt.get("result_sha256"))) is None
    ):
        raise TransportError("registry snapshot sync receipt is invalid")


def _worker_image_request(
    metadata_json: str,
    *,
    node: str,
    archive: Path,
    snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        payload = json.loads(metadata_json)
        canonical_matches = metadata_json.encode("ascii") + b"\n" == _canonical_json(payload)
    except (UnicodeEncodeError, UnicodeDecodeError, json.JSONDecodeError, TypeError) as exc:
        raise TransportError("worker image load metadata is invalid") from exc
    unsigned = (
        {key: value for key, value in payload.items() if key != "payload_sha256"}
        if isinstance(payload, dict)
        else {}
    )
    domain = "oldlab" if node.startswith("oldlab-") else "gb10"
    architecture = "amd64" if domain == "oldlab" else "arm64"
    candidates = (
        [
            row
            for row in snapshot.get("candidates", [])
            if isinstance(row, dict)
            and row.get("candidate_id") == payload.get("candidate_id")
            and row.get("env_id") == payload.get("env_id")
            and row.get("candidate_sha") == payload.get("candidate_sha")
            and row.get("candidate_tree") == payload.get("candidate_tree")
        ]
        if isinstance(payload, dict)
        else []
    )
    environments = (
        [
            row
            for row in snapshot.get("environments", [])
            if isinstance(row, dict)
            and row.get("env_id") == payload.get("env_id")
            and row.get("resource_generation") == payload.get("resource_generation")
        ]
        if isinstance(payload, dict)
        else []
    )
    candidate = candidates[0] if len(candidates) == 1 else {}
    image_digests = candidate.get("image_digests")
    image_archives = candidate.get("image_archives")
    archive_binding = image_archives.get(architecture) if isinstance(image_archives, dict) else None
    registry_module = _registry_module()
    maximum_archive_size = getattr(registry_module, "MAX_WORKER_IMAGE_ARCHIVE_BYTES", 0)
    try:
        archive_matches = (
            isinstance(archive_binding, dict)
            and isinstance(archive_binding.get("path"), str)
            and archive.absolute() == Path(str(archive_binding["path"])).absolute()
        )
    except OSError:
        archive_matches = False
    if (
        not isinstance(payload, dict)
        or set(payload) != WORKER_IMAGE_LOAD_REQUEST_FIELDS
        or not canonical_matches
        or payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("kind") != WORKER_IMAGE_LOAD_REQUEST_KIND
        or payload.get("node") != node
        or payload.get("domain") != domain
        or payload.get("architecture") != architecture
        or re.fullmatch(r"denv-[a-z0-9-]{8,64}", str(payload.get("env_id"))) is None
        or type(payload.get("resource_generation")) is not int
        or int(payload["resource_generation"]) < 1
        or re.fullmatch(r"cand-[0-9a-f]{40}", str(payload.get("candidate_id"))) is None
        or re.fullmatch(r"[0-9a-f]{40}", str(payload.get("candidate_sha"))) is None
        or re.fullmatch(r"[0-9a-f]{40}", str(payload.get("candidate_tree"))) is None
        or any(
            re.fullmatch(r"sha256:[0-9a-f]{64}", str(payload.get(field))) is None
            for field in (
                "config_digest",
                "index_digest",
                "load_descriptor_digest",
            )
        )
        or payload.get("load_descriptor_media_type")
        not in {
            "application/vnd.oci.image.manifest.v1+json",
            "application/vnd.docker.distribution.manifest.v2+json",
            "application/vnd.oci.image.index.v1+json",
            "application/vnd.docker.distribution.manifest.list.v2+json",
        }
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("archive_sha256"))) is None
        or type(payload.get("archive_size")) is not int
        or type(maximum_archive_size) is not int
        or not 0 < int(payload["archive_size"]) <= maximum_archive_size
        or type(payload.get("registry_generation")) is not int
        or int(payload["registry_generation"]) < 1
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(payload.get("registry_payload_sha256")),
        )
        is None
        or re.fullmatch(r"[0-9a-f]{64}", str(payload.get("payload_sha256"))) is None
        or payload.get("payload_sha256") != hashlib.sha256(_canonical_json(unsigned)).hexdigest()
        or snapshot.get("generation") != payload.get("registry_generation")
        or snapshot.get("payload_sha256") != payload.get("registry_payload_sha256")
        or len(candidates) != 1
        or len(environments) != 1
        or not isinstance(image_digests, dict)
        or image_digests.get(architecture) != payload.get("config_digest")
        or not isinstance(archive_binding, dict)
        or set(archive_binding)
        != {
            "path",
            "sha256",
            "size",
            "config_digest",
            "index_digest",
            "manifest_digest",
            "manifest_media_type",
            "load_descriptor_digest",
            "load_descriptor_media_type",
        }
        or archive_binding.get("sha256") != payload.get("archive_sha256")
        or archive_binding.get("size") != payload.get("archive_size")
        or any(
            archive_binding.get(field) != payload.get(field)
            for field in (
                "config_digest",
                "index_digest",
                "load_descriptor_digest",
                "load_descriptor_media_type",
            )
        )
        or not archive_matches
    ):
        raise TransportError("worker image load metadata binding is invalid")
    try:
        verified = registry_module.verify_worker_image_archive(
            archive,
            architecture=architecture,
            candidate_sha=str(payload["candidate_sha"]),
            image_id=str(payload["config_digest"]),
            expected_archive_sha256=str(payload["archive_sha256"]),
            expected_archive_size=int(payload["archive_size"]),
        )
    except Exception as exc:
        raise TransportError("worker image archive verification failed") from exc
    if any(
        verified.get(field) != payload.get(field)
        for field in (
            "config_digest",
            "index_digest",
            "load_descriptor_digest",
            "load_descriptor_media_type",
        )
    ):
        raise TransportError("worker image archive descriptor binding is invalid")
    return payload


def _validate_worker_image_load_receipt(
    raw: bytes,
    *,
    request: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        receipt = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransportError("worker image load receipt is invalid") from exc
    unsigned = (
        {key: value for key, value in receipt.items() if key != "payload_sha256"}
        if isinstance(receipt, dict)
        else {}
    )
    if (
        not isinstance(receipt, dict)
        or set(receipt) != WORKER_IMAGE_LOAD_RECEIPT_FIELDS
        or raw != _canonical_json(receipt)
        or receipt.get("schema_version") != SCHEMA_VERSION
        or receipt.get("kind") != WORKER_IMAGE_LOAD_RECEIPT_KIND
        or receipt.get("status") not in {"loaded", "reused"}
        or any(
            receipt.get(field) != request.get(field)
            for field in (
                "node",
                "domain",
                "architecture",
                "candidate_id",
                "candidate_sha",
                "config_digest",
                "index_digest",
                "load_descriptor_digest",
                "load_descriptor_media_type",
                "archive_sha256",
                "archive_size",
                "registry_generation",
                "registry_payload_sha256",
            )
        )
        or re.fullmatch(
            r"sha256:[0-9a-f]{64}",
            str(receipt.get("runtime_image_id")),
        )
        is None
        or (
            receipt.get("docker_backend") == "classic-overlay2"
            and (
                receipt.get("docker_storage_driver") != "overlay2"
                or receipt.get("runtime_image_id") != request.get("config_digest")
                or receipt.get("docker_descriptor_digest") is not None
                or receipt.get("docker_descriptor_media_type") is not None
            )
        )
        or (
            receipt.get("docker_backend") == "containerd-snapshotter-v1"
            and (
                receipt.get("docker_storage_driver") != "overlayfs"
                or receipt.get("runtime_image_id") != request.get("load_descriptor_digest")
                or receipt.get("docker_descriptor_digest") != request.get("load_descriptor_digest")
                or receipt.get("docker_descriptor_media_type")
                != request.get("load_descriptor_media_type")
            )
        )
        or receipt.get("docker_backend") not in {"classic-overlay2", "containerd-snapshotter-v1"}
        or receipt.get("payload_sha256") != hashlib.sha256(_canonical_json(unsigned)).hexdigest()
    ):
        raise TransportError("worker image load receipt binding is invalid")
    return receipt


def _stream_worker_image(
    argv: Sequence[str],
    *,
    header: bytes,
    archive_descriptor: int,
    archive_size: int,
) -> subprocess.CompletedProcess[bytes]:
    read_descriptor, write_descriptor = os.pipe()
    try:
        process = subprocess.Popen(
            tuple(argv),
            stdin=read_descriptor,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_clean_env(),
        )
    except OSError as exc:
        os.close(read_descriptor)
        os.close(write_descriptor)
        raise TransportError("node authority image transport failed safely") from exc
    os.close(read_descriptor)
    writer_error: list[BaseException] = []

    def upload() -> None:
        total = 0
        try:
            with os.fdopen(write_descriptor, "wb", closefd=True) as output:
                output.write(header)
                os.lseek(archive_descriptor, 0, os.SEEK_SET)
                while total < archive_size:
                    chunk = os.read(
                        archive_descriptor,
                        min(1024 * 1024, archive_size - total),
                    )
                    if not chunk:
                        raise TransportError("worker image archive ended during transport")
                    total += len(chunk)
                    output.write(chunk)
                if os.read(archive_descriptor, 1):
                    raise TransportError("worker image archive exceeds its declared size")
        except BaseException as exc:  # delivered to the controlling thread
            writer_error.append(exc)
            try:
                os.close(write_descriptor)
            except OSError:
                pass

    writer = threading.Thread(target=upload, daemon=True)
    writer.start()
    stdout_buffer = bytearray()
    stderr_buffer = bytearray()
    reader_errors: list[BaseException] = []
    output_exceeded = threading.Event()

    def drain(pipe: Any, output: bytearray, limit: int) -> None:
        try:
            while True:
                chunk = pipe.read(65_536)
                if not chunk:
                    break
                if len(output) + len(chunk) <= limit:
                    output.extend(chunk)
                else:
                    output_exceeded.set()
                    try:
                        process.kill()
                    except OSError:
                        pass
        except BaseException as exc:
            reader_errors.append(exc)
            try:
                process.kill()
            except OSError:
                pass

    assert process.stdout is not None
    assert process.stderr is not None
    stdout_reader = threading.Thread(
        target=drain,
        args=(process.stdout, stdout_buffer, MAX_STDOUT_BYTES),
        daemon=True,
    )
    stderr_reader = threading.Thread(
        target=drain,
        args=(process.stderr, stderr_buffer, MAX_STDERR_BYTES),
        daemon=True,
    )
    stdout_reader.start()
    stderr_reader.start()
    try:
        process.wait(timeout=WORKER_IMAGE_LOAD_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        process.wait()
        writer.join(timeout=1)
        stdout_reader.join(timeout=1)
        stderr_reader.join(timeout=1)
        raise TransportError("node authority image transport timed out safely") from exc
    writer.join(timeout=5)
    stdout_reader.join(timeout=5)
    stderr_reader.join(timeout=5)
    if writer.is_alive() or stdout_reader.is_alive() or stderr_reader.is_alive():
        process.kill()
        process.wait()
        raise TransportError("node authority image transport did not close safely")
    stdout = bytes(stdout_buffer)
    stderr = bytes(stderr_buffer)
    if (
        writer_error
        or reader_errors
        or output_exceeded.is_set()
        or process.returncode != 0
        or stderr
    ):
        raise TransportError("node authority image transport failed safely")
    return subprocess.CompletedProcess(tuple(argv), process.returncode, stdout, stderr)


def load_image(
    node: str,
    archive: Path,
    metadata_json: str,
    *,
    layout: Layout | None = None,
) -> dict[str, Any]:
    """Sync the exact registry snapshot, then stream one verified archive."""

    if os.geteuid() != ROOT_UID:
        raise TransportError("transport client requires root")
    if len(metadata_json.encode("utf-8")) > MAX_WORKER_IMAGE_HEADER_BYTES:
        raise TransportError("worker image load metadata exceeds its size bound")
    layout = default_layout() if layout is None else layout
    validate_client_install(layout)
    config = load_config(layout.config)
    initiator = config.node_for_hostname(_hostname())
    if initiator != "oldlab-2":
        raise TransportError("worker image loading is restricted to oldlab-2")
    role = config.authority_role(initiator, node, "load-image")
    snapshot_raw = _safe_external_file(
        REGISTRY_SNAPSHOT,
        modes=frozenset({0o600}),
        limit=8 << 20,
        expected_uid=ROOT_UID,
        validate_parents=True,
    )
    snapshot = _verified_registry_snapshot(snapshot_raw)
    request = _worker_image_request(
        metadata_json,
        node=node,
        archive=archive,
        snapshot=snapshot,
    )
    sync_original = {**request, "sandbox": "image-load"}
    sync_envelope = _registry_sync_envelope(sync_original, snapshot, snapshot_raw)
    sync_completed = invoke(
        node,
        "transact",
        sync_envelope,
        layout=layout,
    )
    _validate_registry_sync_receipt(
        sync_completed,
        sync_envelope=sync_envelope,
        snapshot=snapshot,
    )
    local = config.nodes[node].hostname == _hostname()
    argv = (
        [
            RUNUSER,
            "-u",
            OPERATOR,
            "--",
            SUDO,
            "-n",
            str(config.authority_program),
            "load-image",
        ]
        if local
        else _remote_ssh_argv(
            config,
            layout,
            initiator=initiator,
            node=node,
            verb="load-image",
            role=role,
        )
    )
    with _safe_external_stream(
        archive,
        expected_size=int(request["archive_size"]),
    ) as descriptor:
        completed = _stream_worker_image(
            argv,
            header=_canonical_json(request),
            archive_descriptor=descriptor,
            archive_size=int(request["archive_size"]),
        )
    return _validate_worker_image_load_receipt(completed.stdout, request=request)


def invoke(
    node: str,
    verb: str,
    envelope: bytes,
    *,
    layout: Layout | None = None,
    run: Callable[..., subprocess.CompletedProcess[bytes]] = subprocess.run,
) -> subprocess.CompletedProcess[bytes]:
    if os.geteuid() != 0:
        raise TransportError("transport client requires root")
    if len(envelope) > 96 * 1024 * 1024:
        raise TransportError("transport request exceeds its size bound")
    _validate_identity_preflight_route(verb, envelope)
    layout = default_layout() if layout is None else layout
    validate_client_install(layout)
    config = load_config(layout.config)
    initiator = config.node_for_hostname(_hostname())
    local = config.nodes[node].hostname == _hostname()

    def execute(
        request_verb: str,
        request_envelope: bytes,
    ) -> subprocess.CompletedProcess[bytes]:
        role = config.authority_role(initiator, node, request_verb)
        if local:
            argv = [
                RUNUSER,
                "-u",
                OPERATOR,
                "--",
                SUDO,
                "-n",
                str(config.authority_program),
                request_verb,
            ]
        else:
            argv = _remote_ssh_argv(
                config,
                layout,
                initiator=initiator,
                node=node,
                verb=request_verb,
                role=role,
            )
        try:
            completed = run(
                argv,
                input=request_envelope,
                env=_clean_env(),
                check=False,
                capture_output=True,
                timeout=_invoke_timeout(node, request_verb, request_envelope),
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise TransportError("node authority transport failed safely") from exc
        if (
            completed.returncode != 0
            or completed.stderr
            or len(completed.stdout) > MAX_STDOUT_BYTES
        ):
            raise TransportError("node authority transport failed safely")
        if role.kind != "authority":  # pragma: no cover - config parser invariant
            raise TransportError("transport role is invalid")
        return completed

    registry_binding = _registry_binding_from_envelope(node, envelope)
    if registry_binding is not None:
        original, expected = registry_binding
        snapshot_raw = _safe_external_file(
            REGISTRY_SNAPSHOT,
            modes=frozenset({0o600}),
            limit=8 << 20,
            expected_uid=ROOT_UID,
            validate_parents=True,
        )
        snapshot = _verified_registry_snapshot(snapshot_raw)
        current = (int(snapshot["generation"]), str(snapshot["payload_sha256"]))
        if current != expected:
            raise TransportError(
                "registry-bound transport request is stale before snapshot sync",
            )
        _validate_worker_image_binding(original, snapshot)
        sync_envelope = _registry_sync_envelope(original, snapshot, snapshot_raw)
        sync_completed = execute("transact", sync_envelope)
        _validate_registry_sync_receipt(
            sync_completed,
            sync_envelope=sync_envelope,
            snapshot=snapshot,
        )
    return execute(verb, envelope)


def _forced_action(
    config: TransportConfig,
    role_name: str,
    original_command: str,
) -> tuple[str, str]:
    try:
        role = config.roles[role_name]
    except KeyError as exc:
        raise TransportError("forced transport role is invalid") from exc
    if role.kind == "authority":
        expected = {f"{SUDO} -n {config.authority_program} {verb}": verb for verb in role.verbs}
        try:
            return "authority", expected[original_command]
        except KeyError as exc:
            raise TransportError("forced authority command is not approved") from exc
    prefix = "proxy "
    if not original_command.startswith(prefix) or original_command.count(" ") != 1:
        raise TransportError("forced proxy command is not approved")
    target = original_command[len(prefix) :]
    if target not in role.targets:
        raise TransportError("forced proxy target is not approved")
    return "proxy", target


def _proxy_endpoint(
    config: TransportConfig,
    role_name: str,
    target: str,
) -> tuple[str, int]:
    role = config.roles[role_name]
    route = config.route(role.initiator, target)
    if role.kind != "proxy" or route.jump is None:
        raise TransportError("forced proxy route is invalid")
    return route.address, route.port


def _proxy_stream(address: str, port: int) -> None:
    try:
        connection = socket.create_connection((address, port), timeout=10)
    except OSError as exc:
        raise TransportError("forced proxy connection failed safely") from exc

    def upload() -> None:
        try:
            shutil.copyfileobj(sys.stdin.buffer, connection.makefile("wb", buffering=0))
            connection.shutdown(socket.SHUT_WR)
        except OSError:
            pass

    thread = threading.Thread(target=upload, daemon=True)
    thread.start()
    try:
        shutil.copyfileobj(connection.makefile("rb", buffering=0), sys.stdout.buffer)
        sys.stdout.buffer.flush()
    except OSError as exc:
        raise TransportError("forced proxy stream failed safely") from exc
    finally:
        connection.close()
        thread.join(timeout=1)


def forced(role_name: str, *, layout: Layout | None = None) -> None:
    layout = default_layout() if layout is None else layout
    if os.geteuid() != layout.operator_uid:
        raise TransportError("forced transport caller is not the operator")
    validate_server_install(layout)
    config = load_config(layout.config)
    original = os.environ.get("SSH_ORIGINAL_COMMAND", "")
    action, value = _forced_action(config, role_name, original)
    if action == "authority":
        os.execve(
            SUDO,
            [SUDO, "-n", str(config.authority_program), value],
            _clean_env(),
        )
    address, port = _proxy_endpoint(config, role_name, value)
    _proxy_stream(address, port)


def proxy_client(node: str, *, layout: Layout | None = None) -> None:
    if os.geteuid() != 0:
        raise TransportError("transport proxy client requires root")
    layout = default_layout() if layout is None else layout
    validate_client_install(layout)
    config = load_config(layout.config)
    initiator = config.node_for_hostname(_hostname())
    role = config.proxy_role(initiator, node)
    route = config.route(initiator, node)
    if route.jump is None:
        raise TransportError("transport proxy route is invalid")
    jump_route = config.route(initiator, route.jump)
    argv = _base_ssh_argv(
        identity=layout.identity(role.name),
        known_hosts=layout.known_hosts,
        route=jump_route,
    )
    os.execve(SSH, [*argv, "proxy", node], _clean_env())


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap-inventory", allow_abbrev=False)

    client = subparsers.add_parser("bootstrap-client", allow_abbrev=False)
    client.add_argument("--identity", action="append", default=[], metavar="ROLE=PATH")
    client.add_argument("--public-key", action="append", default=[], metavar="ROLE=PATH")
    client.add_argument("--known-hosts", type=Path, required=True)
    client.add_argument("--execute", action="store_true")

    server = subparsers.add_parser("bootstrap-server", allow_abbrev=False)
    server.add_argument("--public-key", action="append", default=[], metavar="ROLE=PATH")
    server.add_argument("--execute", action="store_true")

    upgrade_parser = subparsers.add_parser("upgrade", allow_abbrev=False)
    upgrade_parser.add_argument(
        "--identity",
        action="append",
        default=[],
        metavar="ROLE=PATH",
    )
    upgrade_parser.add_argument(
        "--public-key",
        action="append",
        default=[],
        metavar="ROLE=PATH",
    )
    upgrade_parser.add_argument("--known-hosts", type=Path)
    upgrade_parser.add_argument("--execute", action="store_true")

    subparsers.add_parser("check-client", allow_abbrev=False)
    subparsers.add_parser("check-server", allow_abbrev=False)

    invoke_parser = subparsers.add_parser("invoke", allow_abbrev=False)
    invoke_parser.add_argument("--node", required=True)
    invoke_parser.add_argument("--verb", choices=sorted(ALLOWED_VERBS), required=True)

    load_image_parser = subparsers.add_parser("load-image", allow_abbrev=False)
    load_image_parser.add_argument("--node", required=True)
    load_image_parser.add_argument("--archive", type=Path, required=True)
    load_image_parser.add_argument("--metadata-json", required=True)

    forced_parser = subparsers.add_parser("forced", allow_abbrev=False)
    forced_parser.add_argument("role")

    proxy_parser = subparsers.add_parser("proxy-client", allow_abbrev=False)
    proxy_parser.add_argument("--node", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "bootstrap-inventory":
            sys.stdout.buffer.write(_canonical_json(bootstrap_inventory()))
        elif args.command == "bootstrap-client":
            report = bootstrap_client(
                identity_sources=_parse_role_paths(args.identity, "identity"),
                public_key_sources=_parse_role_paths(args.public_key, "public key"),
                known_hosts_source=args.known_hosts,
                execute=args.execute,
            )
            sys.stdout.write(
                json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
            )
        elif args.command == "bootstrap-server":
            report = bootstrap_server(
                public_key_sources=_parse_role_paths(args.public_key, "public key"),
                execute=args.execute,
            )
            sys.stdout.write(
                json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
            )
        elif args.command == "upgrade":
            report = upgrade(
                identity_sources=_parse_role_paths(args.identity, "identity"),
                public_key_sources=_parse_role_paths(args.public_key, "public key"),
                known_hosts_source=args.known_hosts,
                execute=args.execute,
            )
            sys.stdout.write(
                json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
            )
        elif args.command == "check-client":
            sys.stdout.write(
                json.dumps(
                    validate_client_install(),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
            )
        elif args.command == "check-server":
            sys.stdout.write(
                json.dumps(
                    validate_server_install(),
                    sort_keys=True,
                    separators=(",", ":"),
                )
                + "\n",
            )
        elif args.command == "invoke":
            completed = invoke(args.node, args.verb, sys.stdin.buffer.read())
            sys.stdout.buffer.write(completed.stdout)
        elif args.command == "load-image":
            sys.stdout.buffer.write(
                _canonical_json(
                    load_image(
                        args.node,
                        args.archive,
                        args.metadata_json,
                    ),
                ),
            )
        elif args.command == "forced":
            forced(args.role)
        else:
            proxy_client(args.node)
        return 0
    except TransportError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1
    except OSError:
        sys.stderr.write("error: node authority transport failed safely\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
