"""Fail-closed eBPF loading, map programming, and pinned cgroup links."""

from __future__ import annotations

import ctypes
import errno
import hashlib
import ipaddress
import json
import os
import platform
import re
import stat
import struct
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol, cast
from uuid import UUID, uuid4

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import CommandIdentity
from loom_task_image_builder_guard.safeio import read_stable_file
from loom_task_image_builder_guard.slurm import CommandResult, CommandRunner

BPF_MAP_LOOKUP_ELEM = 1
BPF_MAP_UPDATE_ELEM = 2
BPF_MAP_GET_NEXT_KEY = 4
BPF_OBJ_PIN = 6
BPF_OBJ_GET = 7
BPF_OBJ_GET_INFO_BY_FD = 15
BPF_LINK_CREATE = 28

_BPF_ATTR_SIZE = 144
_BPF_INFO_SIZE = 256
_BPF_LINK_TYPE_CGROUP = 3
_BPF_ANY = 0
_MAX_OBJECT_BYTES = 128 * 1024 * 1024
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_STAGING_SUFFIX = re.compile(r"^[a-z0-9]{1,32}$")

# name, enum bpf_attach_type, enum bpf_prog_type
ATTACHMENTS: tuple[tuple[str, int, int], ...] = (
    ("guard_connect4", 10, 18),
    ("guard_connect6", 11, 18),
    ("guard_sendmsg4", 14, 18),
    ("guard_sendmsg6", 15, 18),
    ("guard_sock_create", 2, 9),
    ("guard_sock_release", 34, 9),
    ("guard_ingress", 0, 8),
    ("guard_egress", 1, 8),
)

# name -> (enum bpf_map_type, key bytes, value bytes, maximum entries)
_MAP_LAYOUTS: dict[str, tuple[int, int, int, int]] = {
    "scope_subject": (2, 4, 4, 1),
    "allow_v4": (1, 12, 1, 4096),
    "allow_v6": (1, 24, 1, 4096),
    "subject_limits": (1, 4, 120, 16),
    "flow_sockets": (1, 8, 8, 4096),
    "drop_counters": (6, 4, 8, 16),
}


@dataclass(frozen=True, slots=True)
class BpfObjectInfo:
    kind: Literal["program", "map", "link"]
    object_id: int
    object_type: int
    name: str
    key_size: int | None = None
    value_size: int | None = None
    max_entries: int | None = None
    program_id: int | None = None
    cgroup_id: int | None = None
    attach_type: int | None = None


class _RawSyscall(Protocol):
    def __call__(self, number: int, command: int, address: int, size: int) -> int: ...


class BpfOperations(Protocol):
    def obj_get(self, path: Path) -> int: ...

    def map_update(self, descriptor: int, key: bytes, value: bytes, *, flags: int = 0) -> None: ...

    def map_items(
        self,
        descriptor: int,
        *,
        key_size: int,
        value_size: int,
        max_entries: int,
    ) -> tuple[tuple[bytes, bytes], ...]: ...

    def object_info(
        self, descriptor: int, kind: Literal["program", "map", "link"]
    ) -> BpfObjectInfo: ...

    def link_create(self, *, program_fd: int, target_fd: int, attach_type: int) -> int: ...

    def obj_pin(self, descriptor: int, path: Path) -> None: ...


def _native_syscall(number: int, command: int, address: int, size: int) -> int:
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    result = int(
        libc.syscall(
            ctypes.c_long(number),
            ctypes.c_uint(command),
            ctypes.c_void_p(address),
            ctypes.c_uint(size),
        )
    )
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    return result


def _path_buffer(path: Path) -> ctypes.Array[ctypes.c_char]:
    if not isinstance(path, Path) or not path.is_absolute() or "\x00" in os.fspath(path):
        raise GuardError("bpf_path_invalid")
    return ctypes.create_string_buffer(os.fsencode(path) + b"\0")


def _name(payload: bytes) -> str:
    value = payload.split(b"\0", 1)[0]
    try:
        decoded = value.decode("ascii")
    except UnicodeDecodeError:
        raise GuardError("bpf_object_readback_invalid") from None
    if not decoded:
        raise GuardError("bpf_object_readback_invalid")
    return decoded


class BpfSyscall:
    """Minimal checked wrapper around the architecture-specific ``bpf(2)`` call."""

    def __init__(
        self,
        *,
        machine: str | None = None,
        syscall: _RawSyscall = _native_syscall,
    ) -> None:
        selected = platform.machine() if machine is None else machine
        try:
            self.number = {"x86_64": 321, "aarch64": 280}[selected]
        except KeyError:
            raise GuardError("bpf_architecture_unsupported") from None
        self._syscall = syscall

    def _invoke(self, command: int, attr: ctypes.Array[ctypes.c_char]) -> int:
        try:
            return self._syscall(self.number, command, ctypes.addressof(attr), len(attr))
        except OSError as exc:
            raise GuardError("bpf_syscall_failed") from exc

    def obj_get(self, path: Path) -> int:
        encoded = _path_buffer(path)
        attr = ctypes.create_string_buffer(_BPF_ATTR_SIZE)
        struct.pack_into("<QII", attr, 0, ctypes.addressof(encoded), 0, 0)
        descriptor = self._invoke(BPF_OBJ_GET, attr)
        if descriptor < 0:
            raise GuardError("bpf_descriptor_invalid")
        return descriptor

    def map_update(
        self,
        descriptor: int,
        key: bytes,
        value: bytes,
        *,
        flags: int = _BPF_ANY,
    ) -> None:
        if (
            type(descriptor) is not int
            or descriptor < 0
            or not isinstance(key, bytes)
            or not key
            or len(key) > 4096
            or not isinstance(value, bytes)
            or not value
            or len(value) > 4096
            or type(flags) is not int
            or not 0 <= flags <= (1 << 64) - 1
        ):
            raise GuardError("bpf_map_update_invalid")
        key_buffer = ctypes.create_string_buffer(key, len(key))
        value_buffer = ctypes.create_string_buffer(value, len(value))
        attr = ctypes.create_string_buffer(_BPF_ATTR_SIZE)
        struct.pack_into(
            "<I4xQQQ",
            attr,
            0,
            descriptor,
            ctypes.addressof(key_buffer),
            ctypes.addressof(value_buffer),
            flags,
        )
        if self._invoke(BPF_MAP_UPDATE_ELEM, attr) != 0:
            raise GuardError("bpf_map_update_invalid")

    def map_items(
        self,
        descriptor: int,
        *,
        key_size: int,
        value_size: int,
        max_entries: int,
    ) -> tuple[tuple[bytes, bytes], ...]:
        if (
            type(descriptor) is not int
            or descriptor < 0
            or type(key_size) is not int
            or not 1 <= key_size <= 4096
            or type(value_size) is not int
            or not 1 <= value_size <= 4096
            or type(max_entries) is not int
            or not 1 <= max_entries <= 4096
        ):
            raise GuardError("bpf_map_readback_invalid")
        previous: ctypes.Array[ctypes.c_char] | None = None
        observed: list[tuple[bytes, bytes]] = []
        seen: set[bytes] = set()
        for _index in range(max_entries + 1):
            next_key = ctypes.create_string_buffer(key_size)
            next_attr = ctypes.create_string_buffer(_BPF_ATTR_SIZE)
            struct.pack_into(
                "<I4xQQ",
                next_attr,
                0,
                descriptor,
                0 if previous is None else ctypes.addressof(previous),
                ctypes.addressof(next_key),
            )
            try:
                result = self._syscall(
                    self.number,
                    BPF_MAP_GET_NEXT_KEY,
                    ctypes.addressof(next_attr),
                    len(next_attr),
                )
            except OSError as exc:
                if exc.errno == errno.ENOENT:
                    return tuple(sorted(observed))
                raise GuardError("bpf_map_readback_invalid") from exc
            if result != 0:
                raise GuardError("bpf_map_readback_invalid")
            key = bytes(next_key.raw)
            if key in seen:
                raise GuardError("bpf_map_readback_invalid")
            seen.add(key)
            value = ctypes.create_string_buffer(value_size)
            lookup_attr = ctypes.create_string_buffer(_BPF_ATTR_SIZE)
            struct.pack_into(
                "<I4xQQ",
                lookup_attr,
                0,
                descriptor,
                ctypes.addressof(next_key),
                ctypes.addressof(value),
            )
            try:
                result = self._syscall(
                    self.number,
                    BPF_MAP_LOOKUP_ELEM,
                    ctypes.addressof(lookup_attr),
                    len(lookup_attr),
                )
            except OSError as exc:
                raise GuardError("bpf_map_readback_invalid") from exc
            if result != 0:
                raise GuardError("bpf_map_readback_invalid")
            observed.append((key, bytes(value.raw)))
            previous = ctypes.create_string_buffer(key, key_size)
        raise GuardError("bpf_map_readback_invalid")

    def object_info(
        self,
        descriptor: int,
        kind: Literal["program", "map", "link"],
    ) -> BpfObjectInfo:
        if type(descriptor) is not int or descriptor < 0 or kind not in {
            "program",
            "map",
            "link",
        }:
            raise GuardError("bpf_object_readback_invalid")
        info = ctypes.create_string_buffer(_BPF_INFO_SIZE)
        attr = ctypes.create_string_buffer(_BPF_ATTR_SIZE)
        struct.pack_into("<IIQ", attr, 0, descriptor, len(info), ctypes.addressof(info))
        if self._invoke(BPF_OBJ_GET_INFO_BY_FD, attr) != 0:
            raise GuardError("bpf_object_readback_invalid")
        payload = bytes(info)
        object_type, object_id = struct.unpack_from("<II", payload)
        if object_id <= 0:
            raise GuardError("bpf_object_readback_invalid")
        if kind == "program":
            return BpfObjectInfo(kind, object_id, object_type, _name(payload[64:80]))
        if kind == "map":
            key_size, value_size, max_entries = struct.unpack_from("<III", payload, 8)
            return BpfObjectInfo(
                kind,
                object_id,
                object_type,
                _name(payload[24:40]),
                key_size=key_size,
                value_size=value_size,
                max_entries=max_entries,
            )
        program_id = struct.unpack_from("<I", payload, 8)[0]
        cgroup_id, attach_type = struct.unpack_from("<QI", payload, 16)
        return BpfObjectInfo(
            kind,
            object_id,
            object_type,
            "",
            program_id=program_id,
            cgroup_id=cgroup_id,
            attach_type=attach_type,
        )

    def link_create(self, *, program_fd: int, target_fd: int, attach_type: int) -> int:
        if any(type(item) is not int or item < 0 for item in (program_fd, target_fd, attach_type)):
            raise GuardError("bpf_link_arguments_invalid")
        attr = ctypes.create_string_buffer(_BPF_ATTR_SIZE)
        struct.pack_into("<IIII", attr, 0, program_fd, target_fd, attach_type, 0)
        descriptor = self._invoke(BPF_LINK_CREATE, attr)
        if descriptor < 0:
            raise GuardError("bpf_link_create_failed")
        return descriptor

    def obj_pin(self, descriptor: int, path: Path) -> None:
        if type(descriptor) is not int or descriptor < 0:
            raise GuardError("bpf_descriptor_invalid")
        encoded = _path_buffer(path)
        attr = ctypes.create_string_buffer(_BPF_ATTR_SIZE)
        struct.pack_into("<QII", attr, 0, ctypes.addressof(encoded), descriptor, 0)
        if self._invoke(BPF_OBJ_PIN, attr) != 0:
            raise GuardError("bpf_pin_failed")


@dataclass(frozen=True, slots=True)
class Endpoint:
    address: str
    port: int
    protocol: Literal["tcp", "udp"]

    def __post_init__(self) -> None:
        try:
            parsed = ipaddress.ip_address(self.address)
        except ValueError:
            raise GuardError("bpf_policy_endpoint_invalid") from None
        if (
            str(parsed) != self.address
            or type(self.port) is not int
            or not 1 <= self.port <= 65535
            or self.protocol not in {"tcp", "udp"}
        ):
            raise GuardError("bpf_policy_endpoint_invalid")

    @property
    def ip(self) -> ipaddress.IPv4Address | ipaddress.IPv6Address:
        return ipaddress.ip_address(self.address)

    @property
    def protocol_number(self) -> int:
        return 6 if self.protocol == "tcp" else 17


@dataclass(frozen=True, slots=True)
class TrafficLimits:
    ingress_bytes_per_second: int
    egress_bytes_per_second: int
    ingress_packets_per_second: int
    egress_packets_per_second: int
    new_flows_per_second: int
    dns_queries_per_second: int
    max_concurrent_flows: int

    def __post_init__(self) -> None:
        values = (
            self.ingress_bytes_per_second,
            self.egress_bytes_per_second,
            self.ingress_packets_per_second,
            self.egress_packets_per_second,
            self.new_flows_per_second,
            self.dns_queries_per_second,
        )
        if any(type(item) is not int or not 1 <= item <= (1 << 63) - 1 for item in values):
            raise GuardError("bpf_policy_limit_invalid")
        if (
            type(self.max_concurrent_flows) is not int
            or not 1 <= self.max_concurrent_flows <= (1 << 32) - 1
        ):
            raise GuardError("bpf_policy_limit_invalid")


def _endpoint_key(endpoint: Endpoint) -> tuple[int, bytes, int, int]:
    return (
        endpoint.ip.version,
        endpoint.ip.packed,
        endpoint.port,
        endpoint.protocol_number,
    )


@dataclass(frozen=True, slots=True)
class ScopeNetworkPolicy:
    name: Literal["root", "trusted-service", "build-egress"]
    ipv4: tuple[Endpoint, ...]
    ipv6: tuple[Endpoint, ...]
    limits: TrafficLimits

    def __post_init__(self) -> None:
        if self.name not in {"root", "trusted-service", "build-egress"}:
            raise GuardError("bpf_policy_scope_invalid")
        for version, endpoints in ((4, self.ipv4), (6, self.ipv6)):
            if (
                not isinstance(endpoints, tuple)
                or len(endpoints) > 4096
                or any(not isinstance(item, Endpoint) or item.ip.version != version for item in endpoints)
                or tuple(sorted(endpoints, key=_endpoint_key)) != endpoints
                or len(set(endpoints)) != len(endpoints)
            ):
                raise GuardError("bpf_policy_endpoint_invalid")
        if not self.ipv4 and not self.ipv6:
            raise GuardError("bpf_policy_endpoint_invalid")


def _limit_values(limits: TrafficLimits) -> tuple[int, ...]:
    return (
        limits.ingress_bytes_per_second,
        limits.egress_bytes_per_second,
        limits.ingress_packets_per_second,
        limits.egress_packets_per_second,
        limits.new_flows_per_second,
        limits.dns_queries_per_second,
        limits.max_concurrent_flows,
    )


@dataclass(frozen=True, slots=True)
class NetworkPolicy:
    containment_policy_sha256: str
    resource_profile_sha256: str
    bpf_program_sha256: str
    bpf_map_schema_sha256: str
    scopes: tuple[ScopeNetworkPolicy, ...]

    @classmethod
    def from_file(
        cls,
        path: Path,
        *,
        uid: int,
        gid: int,
        containment_policy_sha256: str,
        resource_profile_sha256: str,
        bpf_program_sha256: str,
        bpf_map_schema_sha256: str,
    ) -> NetworkPolicy:
        payload = read_stable_file(path, uid=uid, gid=gid, mode=0o444, maximum=1024 * 1024)
        if hashlib.sha256(payload).hexdigest() != containment_policy_sha256:
            raise GuardError("bpf_policy_digest_invalid")
        try:
            document = json.loads(payload, object_pairs_hook=_policy_pairs)
            canonical = (
                json.dumps(
                    document,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("ascii")
                + b"\n"
            )
        except (
            UnicodeDecodeError,
            UnicodeEncodeError,
            ValueError,
            json.JSONDecodeError,
            RecursionError,
        ):
            raise GuardError("bpf_policy_json_invalid") from None
        if canonical != payload:
            raise GuardError("bpf_policy_json_invalid")
        raw = _policy_object(
            document,
            {
                "schema",
                "resource_profile_sha256",
                "bpf_program_sha256",
                "bpf_map_schema_sha256",
                "scopes",
            },
        )
        if raw["schema"] != "loom.task-image-builder-guard-network-policy/v1":
            raise GuardError("bpf_policy_json_invalid")
        if (
            raw["resource_profile_sha256"] != resource_profile_sha256
            or raw["bpf_program_sha256"] != bpf_program_sha256
            or raw["bpf_map_schema_sha256"] != bpf_map_schema_sha256
        ):
            raise GuardError("bpf_policy_digest_invalid")
        scopes = _policy_object(
            raw["scopes"], {"root", "trusted-service", "build-egress"}
        )
        return cls(
            containment_policy_sha256=containment_policy_sha256,
            resource_profile_sha256=resource_profile_sha256,
            bpf_program_sha256=bpf_program_sha256,
            bpf_map_schema_sha256=bpf_map_schema_sha256,
            scopes=tuple(
                _parse_scope(name, scopes[name])
                for name in ("root", "trusted-service", "build-egress")
            ),
        )

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, str)
            or _DIGEST.fullmatch(value) is None
            or value == "0" * 64
            for value in (
                self.containment_policy_sha256,
                self.resource_profile_sha256,
                self.bpf_program_sha256,
                self.bpf_map_schema_sha256,
            )
        ):
            raise GuardError("bpf_policy_digest_invalid")
        if tuple(scope.name for scope in self.scopes) != (
            "root",
            "trusted-service",
            "build-egress",
        ):
            raise GuardError("bpf_policy_scope_invalid")
        root, trusted, build = self.scopes
        if root.ipv4 != tuple(sorted(set(trusted.ipv4) | set(build.ipv4), key=_endpoint_key)) or (
            root.ipv6
            != tuple(sorted(set(trusted.ipv6) | set(build.ipv6), key=_endpoint_key))
        ):
            raise GuardError("bpf_policy_root_invalid")
        for root_value, trusted_value, build_value in zip(
            _limit_values(root.limits),
            _limit_values(trusted.limits),
            _limit_values(build.limits),
            strict=True,
        ):
            if not max(trusted_value, build_value) <= root_value <= trusted_value + build_value:
                raise GuardError("bpf_policy_root_invalid")


def _policy_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _policy_object(value: object, keys: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys or not all(
        isinstance(key, str) for key in value
    ):
        raise GuardError("bpf_policy_json_invalid")
    return cast(dict[str, object], value)


def _parse_endpoint(value: object, *, version: int) -> Endpoint:
    raw = _policy_object(value, {"address", "port", "protocol"})
    address = raw["address"]
    port = raw["port"]
    protocol = raw["protocol"]
    if not isinstance(address, str) or type(port) is not int or protocol not in {"tcp", "udp"}:
        raise GuardError("bpf_policy_endpoint_invalid")
    endpoint = Endpoint(address, port, cast(Literal["tcp", "udp"], protocol))
    if endpoint.ip.version != version:
        raise GuardError("bpf_policy_endpoint_invalid")
    return endpoint


def _parse_scope(
    name: Literal["root", "trusted-service", "build-egress"] | str,
    value: object,
) -> ScopeNetworkPolicy:
    if name not in {"root", "trusted-service", "build-egress"}:
        raise GuardError("bpf_policy_scope_invalid")
    raw = _policy_object(value, {"ipv4", "ipv6", "limits"})
    ipv4 = raw["ipv4"]
    ipv6 = raw["ipv6"]
    if (
        not isinstance(ipv4, list)
        or not isinstance(ipv6, list)
        or len(ipv4) > 4096
        or len(ipv6) > 4096
    ):
        raise GuardError("bpf_policy_endpoint_invalid")
    limit_raw = _policy_object(
        raw["limits"],
        {
            "ingress_bytes_per_second",
            "egress_bytes_per_second",
            "ingress_packets_per_second",
            "egress_packets_per_second",
            "new_flows_per_second",
            "dns_queries_per_second",
            "max_concurrent_flows",
        },
    )
    if not all(type(item) is int for item in limit_raw.values()):
        raise GuardError("bpf_policy_limit_invalid")
    limits = TrafficLimits(
        ingress_bytes_per_second=cast(int, limit_raw["ingress_bytes_per_second"]),
        egress_bytes_per_second=cast(int, limit_raw["egress_bytes_per_second"]),
        ingress_packets_per_second=cast(int, limit_raw["ingress_packets_per_second"]),
        egress_packets_per_second=cast(int, limit_raw["egress_packets_per_second"]),
        new_flows_per_second=cast(int, limit_raw["new_flows_per_second"]),
        dns_queries_per_second=cast(int, limit_raw["dns_queries_per_second"]),
        max_concurrent_flows=cast(int, limit_raw["max_concurrent_flows"]),
    )
    return ScopeNetworkPolicy(
        cast(Literal["root", "trusted-service", "build-egress"], name),
        tuple(_parse_endpoint(item, version=4) for item in ipv4),
        tuple(_parse_endpoint(item, version=6) for item in ipv6),
        limits,
    )


@dataclass(frozen=True, slots=True)
class BpfScopeTarget:
    name: Literal["root", "trusted-service", "build-egress"]
    descriptor: int
    cgroup_id: int

    def __post_init__(self) -> None:
        if (
            self.name not in {"root", "trusted-service", "build-egress"}
            or type(self.descriptor) is not int
            or self.descriptor < 0
            or type(self.cgroup_id) is not int
            or self.cgroup_id <= 0
        ):
            raise GuardError("bpf_target_invalid")


class BpfTree(Protocol):
    def bpf_scope_targets(self) -> tuple[BpfScopeTarget, ...]: ...


@dataclass(frozen=True, slots=True)
class BpfAttachment:
    pin_path: Path
    link_ids: tuple[int, ...]
    program_ids: tuple[int, ...]
    map_ids: tuple[int, ...]


@dataclass(slots=True)
class _LoadedScope:
    target: BpfScopeTarget
    programs: dict[str, tuple[int, BpfObjectInfo]]
    maps: dict[str, tuple[int, BpfObjectInfo]]
    link_fds: list[int]
    link_ids: list[int]


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_nlink,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _stable_object_digest(path: Path, trusted_uid: int) -> str:
    descriptor: int | None = None
    try:
        lexical = os.lstat(path)
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0))
        opened = os.fstat(descriptor)
        if (
            _file_identity(lexical) != _file_identity(opened)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_nlink != 1
            or opened.st_uid != trusted_uid
            or stat.S_IMODE(opened.st_mode) & 0o222
            or not 0 < opened.st_size <= _MAX_OBJECT_BYTES
        ):
            raise GuardError("bpf_object_identity_invalid")
        digest = hashlib.sha256()
        offset = 0
        while offset < opened.st_size:
            chunk = os.pread(descriptor, min(1024 * 1024, opened.st_size - offset), offset)
            if not chunk:
                raise GuardError("bpf_object_identity_invalid")
            digest.update(chunk)
            offset += len(chunk)
        if (
            _file_identity(os.fstat(descriptor)) != _file_identity(opened)
            or _file_identity(os.lstat(path)) != _file_identity(opened)
        ):
            raise GuardError("bpf_object_identity_invalid")
        return digest.hexdigest()
    except GuardError:
        raise
    except OSError as exc:
        raise GuardError("bpf_object_identity_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = libc.renameat2
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    if renameat2(-100, os.fsencode(source), -100, os.fsencode(destination), 1) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))


def _limits_payload(limits: TrafficLimits) -> bytes:
    return struct.pack(
        "<II13QII",
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        limits.ingress_bytes_per_second,
        limits.egress_bytes_per_second,
        limits.ingress_packets_per_second,
        limits.egress_packets_per_second,
        limits.new_flows_per_second,
        limits.dns_queries_per_second,
        limits.max_concurrent_flows,
        0,
    )


def _endpoint_payload(subject: int, endpoint: Endpoint) -> bytes:
    prefix = struct.pack("<I", subject) + endpoint.ip.packed
    return prefix + struct.pack("!H", endpoint.port) + bytes((endpoint.protocol_number, 0))


def static_map_items(
    policy: ScopeNetworkPolicy,
) -> dict[str, tuple[tuple[bytes, bytes], ...]]:
    if not isinstance(policy, ScopeNetworkPolicy):
        raise GuardError("bpf_policy_scope_invalid")
    subject = 1
    return {
        "scope_subject": ((struct.pack("<I", 0), struct.pack("<I", subject)),),
        "subject_limits": (
            (struct.pack("<I", subject), _limits_payload(policy.limits)),
        ),
        "allow_v4": tuple(
            sorted(
                (
                    (_endpoint_payload(subject, endpoint), b"\x01")
                    for endpoint in policy.ipv4
                ),
            )
        ),
        "allow_v6": tuple(
            sorted(
                (
                    (_endpoint_payload(subject, endpoint), b"\x01")
                    for endpoint in policy.ipv6
                ),
            )
        ),
    }


class BpfLoader:
    """Load three independent policy instances, then pin exact cgroup links."""

    def __init__(
        self,
        *,
        kernel: BpfOperations,
        runner: CommandRunner,
        bpftool: CommandIdentity,
        bpf_object_path: Path,
        bpffs_root: Path,
        containment_policy_sha256: str,
        resource_profile_sha256: str,
        bpf_map_schema_sha256: str,
        trusted_uid: int = 0,
        close_fd: Callable[[int], None] = os.close,
        staging_suffix: Callable[[], str] = lambda: uuid4().hex,
    ) -> None:
        self.kernel = kernel
        self.runner = runner
        self.bpftool = bpftool
        self.bpf_object_path = bpf_object_path
        self.bpffs_root = bpffs_root
        self.containment_policy_sha256 = containment_policy_sha256
        self.resource_profile_sha256 = resource_profile_sha256
        self.bpf_map_schema_sha256 = bpf_map_schema_sha256
        self.trusted_uid = trusted_uid
        self._close_fd = close_fd
        self._staging_suffix = staging_suffix

    @staticmethod
    def _successful(result: CommandResult) -> None:
        if result.returncode != 0 or result.stdout or result.stderr:
            raise GuardError("bpftool_load_failed")

    @staticmethod
    def _entries(path: Path, expected: set[str]) -> None:
        try:
            actual = {item.name for item in path.iterdir()}
        except OSError as exc:
            raise GuardError("bpf_pin_inventory_invalid") from exc
        if actual != expected:
            raise GuardError("bpf_pin_inventory_invalid")

    def _load_scope(self, root: Path, target: BpfScopeTarget) -> _LoadedScope:
        scope_root = root / target.name
        program_root = scope_root / "progs"
        map_root = scope_root / "maps"
        link_root = scope_root / "links"
        for path in (scope_root, program_root, map_root, link_root):
            path.mkdir(mode=0o700)
        self._successful(
            self.runner.run(
                self.bpftool,
                (
                    "prog",
                    "loadall",
                    str(self.bpf_object_path),
                    str(program_root),
                    "pinmaps",
                    str(map_root),
                ),
            )
        )
        self._entries(program_root, {item[0] for item in ATTACHMENTS})
        self._entries(map_root, set(_MAP_LAYOUTS))
        programs: dict[str, tuple[int, BpfObjectInfo]] = {}
        maps: dict[str, tuple[int, BpfObjectInfo]] = {}
        owned: list[int] = []
        complete = False
        try:
            for name, _attach_type, expected_type in ATTACHMENTS:
                descriptor = self.kernel.obj_get(program_root / name)
                owned.append(descriptor)
                info = self.kernel.object_info(descriptor, "program")
                if (
                    info.kind != "program"
                    or info.object_type != expected_type
                    or info.name != name[:15]
                ):
                    raise GuardError("bpf_object_readback_invalid")
                programs[name] = (descriptor, info)
            for name, layout in _MAP_LAYOUTS.items():
                descriptor = self.kernel.obj_get(map_root / name)
                owned.append(descriptor)
                info = self.kernel.object_info(descriptor, "map")
                if (
                    info.kind != "map"
                    or info.name != name[:15]
                    or (
                        info.object_type,
                        info.key_size,
                        info.value_size,
                        info.max_entries,
                    )
                    != layout
                ):
                    raise GuardError("bpf_object_readback_invalid")
                maps[name] = (descriptor, info)
            complete = True
            return _LoadedScope(target, programs, maps, [], [])
        finally:
            if not complete:
                for descriptor in reversed(owned):
                    try:
                        self._close_fd(descriptor)
                    except OSError:
                        pass

    def _program_maps(self, loaded: _LoadedScope, policy: ScopeNetworkPolicy) -> None:
        for name, entries in static_map_items(policy).items():
            for key, value in entries:
                self.kernel.map_update(loaded.maps[name][0], key, value)

    def _attach_scope(self, loaded: _LoadedScope, root: Path) -> None:
        link_root = root / loaded.target.name / "links"
        for name, attach_type, _program_type in ATTACHMENTS:
            program_fd, program_info = loaded.programs[name]
            link_fd = self.kernel.link_create(
                program_fd=program_fd,
                target_fd=loaded.target.descriptor,
                attach_type=attach_type,
            )
            loaded.link_fds.append(link_fd)
            self.kernel.obj_pin(link_fd, link_root / name)
            info = self.kernel.object_info(link_fd, "link")
            if (
                info.kind != "link"
                or info.object_type != _BPF_LINK_TYPE_CGROUP
                or info.program_id != program_info.object_id
                or info.cgroup_id != loaded.target.cgroup_id
                or info.attach_type != attach_type
            ):
                raise GuardError("bpf_link_readback_invalid")
            loaded.link_ids.append(info.object_id)

    def attach(self, tree: BpfTree, policy: NetworkPolicy, grant_id: UUID) -> BpfAttachment:
        if not isinstance(grant_id, UUID) or grant_id.int == 0:
            raise GuardError("bpf_grant_invalid")
        if (
            policy.containment_policy_sha256 != self.containment_policy_sha256
            or policy.resource_profile_sha256 != self.resource_profile_sha256
            or policy.bpf_map_schema_sha256 != self.bpf_map_schema_sha256
        ):
            raise GuardError("bpf_policy_identity_invalid")
        if _stable_object_digest(self.bpf_object_path, self.trusted_uid) != (
            policy.bpf_program_sha256
        ):
            raise GuardError("bpf_object_identity_invalid")
        targets = tree.bpf_scope_targets()
        if tuple(target.name for target in targets) != (
            "root",
            "trusted-service",
            "build-egress",
        ) or len({target.cgroup_id for target in targets}) != 3:
            raise GuardError("bpf_target_invalid")
        try:
            root_metadata = self.bpffs_root.stat()
        except OSError as exc:
            raise GuardError("bpf_root_invalid") from exc
        if (
            not self.bpffs_root.is_dir()
            or self.bpffs_root.resolve(strict=True) != self.bpffs_root
            or root_metadata.st_uid != self.trusted_uid
            or stat.S_IMODE(root_metadata.st_mode) & 0o022
        ):
            raise GuardError("bpf_root_invalid")
        suffix = self._staging_suffix()
        if not isinstance(suffix, str) or _STAGING_SUFFIX.fullmatch(suffix) is None:
            raise GuardError("bpf_staging_invalid")
        staging = self.bpffs_root / f"staging-{grant_id}-{suffix}"
        published = self.bpffs_root / str(grant_id)
        try:
            staging.mkdir(mode=0o700)
        except FileExistsError:
            raise GuardError("bpf_pin_collision") from None
        except OSError as exc:
            raise GuardError("bpf_staging_invalid") from exc
        loaded_scopes: list[_LoadedScope] = []
        descriptors: list[int] = []
        try:
            for target in targets:
                loaded = self._load_scope(staging, target)
                loaded_scopes.append(loaded)
                descriptors.extend(item[0] for item in loaded.programs.values())
                descriptors.extend(item[0] for item in loaded.maps.values())
            program_ids = [
                item[1].object_id
                for loaded in loaded_scopes
                for item in loaded.programs.values()
            ]
            map_ids = [
                item[1].object_id for loaded in loaded_scopes for item in loaded.maps.values()
            ]
            if (
                any(item <= 0 for item in (*program_ids, *map_ids))
                or len(set(program_ids)) != len(program_ids)
                or len(set(map_ids)) != len(map_ids)
            ):
                raise GuardError("bpf_object_readback_invalid")
            for loaded, scope_policy in zip(loaded_scopes, policy.scopes, strict=True):
                if loaded.target.name != scope_policy.name:
                    raise GuardError("bpf_policy_scope_invalid")
                self._program_maps(loaded, scope_policy)
            for loaded in loaded_scopes:
                self._attach_scope(loaded, staging)
            link_ids = [item for loaded in loaded_scopes for item in loaded.link_ids]
            if (
                len(link_ids) != len(ATTACHMENTS) * 3
                or any(item <= 0 for item in link_ids)
                or len(set(link_ids)) != len(link_ids)
            ):
                raise GuardError("bpf_link_readback_invalid")
            _rename_noreplace(staging, published)
            return BpfAttachment(
                pin_path=published,
                link_ids=tuple(sorted(link_ids)),
                program_ids=tuple(sorted(program_ids)),
                map_ids=tuple(sorted(map_ids)),
            )
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("bpf_publish_failed") from exc
        finally:
            link_descriptors = [
                descriptor for loaded in loaded_scopes for descriptor in loaded.link_fds
            ]
            for descriptor in reversed([*descriptors, *link_descriptors]):
                try:
                    self._close_fd(descriptor)
                except OSError:
                    pass


__all__ = [
    "ATTACHMENTS",
    "BPF_LINK_CREATE",
    "BPF_MAP_GET_NEXT_KEY",
    "BPF_MAP_LOOKUP_ELEM",
    "BPF_MAP_UPDATE_ELEM",
    "BPF_OBJ_GET",
    "BPF_OBJ_GET_INFO_BY_FD",
    "BPF_OBJ_PIN",
    "BpfAttachment",
    "BpfLoader",
    "BpfObjectInfo",
    "BpfScopeTarget",
    "BpfSyscall",
    "Endpoint",
    "NetworkPolicy",
    "ScopeNetworkPolicy",
    "TrafficLimits",
    "static_map_items",
]
