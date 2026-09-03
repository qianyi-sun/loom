from __future__ import annotations

import ctypes
import hashlib
import json
import os
import struct
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import pytest

from loom_task_image_builder_guard.bpf import (
    ATTACHMENTS,
    BPF_LINK_CREATE,
    BPF_MAP_UPDATE_ELEM,
    BPF_OBJ_GET,
    BPF_OBJ_GET_INFO_BY_FD,
    BPF_OBJ_PIN,
    BpfLoader,
    BpfObjectInfo,
    BpfScopeTarget,
    BpfSyscall,
    Endpoint,
    NetworkPolicy,
    ScopeNetworkPolicy,
    TrafficLimits,
)
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import CommandIdentity
from loom_task_image_builder_guard.slurm import CommandResult

GRANT = UUID("11111111-1111-4111-8111-111111111111")
DIGEST = "a" * 64
PROGRAM_TYPES = {
    "guard_connect4": 18,
    "guard_connect6": 18,
    "guard_sendmsg4": 18,
    "guard_sendmsg6": 18,
    "guard_sock_create": 9,
    "guard_sock_release": 9,
    "guard_ingress": 8,
    "guard_egress": 8,
}
MAP_LAYOUTS = {
    "scope_subject": (2, 4, 4, 1),
    "allow_v4": (1, 12, 1, 4096),
    "allow_v6": (1, 24, 1, 4096),
    "subject_limits": (1, 4, 120, 16),
    "flow_sockets": (1, 8, 8, 4096),
    "drop_counters": (6, 4, 8, 16),
}


class _RawSyscall:
    def __init__(self) -> None:
        self.calls: list[tuple[int, int, bytes]] = []
        self.paths: list[bytes] = []
        self.map_payloads: list[tuple[bytes, bytes]] = []
        self.result_fd = 40

    def __call__(self, number: int, command: int, address: int, size: int) -> int:
        payload = ctypes.string_at(address, size)
        self.calls.append((number, command, payload))
        if command in {BPF_OBJ_GET, BPF_OBJ_PIN}:
            self.paths.append(ctypes.string_at(struct.unpack_from("<Q", payload)[0]))
        if command == BPF_MAP_UPDATE_ELEM:
            _fd, key_address, value_address, _flags = struct.unpack_from(
                "<I4xQQQ", payload
            )
            self.map_payloads.append(
                (ctypes.string_at(key_address, 4), ctypes.string_at(value_address, 5))
            )
        if command == BPF_OBJ_GET_INFO_BY_FD:
            descriptor, info_size, info_address = struct.unpack_from("<IIQ", payload)
            assert info_size >= 80
            info = bytearray(info_size)
            if descriptor == 41:
                struct.pack_into("<II", info, 0, 18, 701)
                info[64:79] = b"guard_connect4"
            else:
                assert descriptor == 42
                struct.pack_into("<III4xQI", info, 0, 3, 901, 701, 501, 10)
            ctypes.memmove(info_address, bytes(info), len(info))
            return 0
        if command in {BPF_MAP_UPDATE_ELEM, BPF_OBJ_PIN}:
            return 0
        self.result_fd += 1
        return self.result_fd


def test_raw_bpf_syscall_uses_arch_number_zeroed_attrs_and_exact_binary_fields(
    tmp_path: Path,
) -> None:
    raw = _RawSyscall()
    kernel = BpfSyscall(machine="x86_64", syscall=raw)
    pin = tmp_path / "program"

    opened = kernel.obj_get(pin)
    kernel.map_update(opened, b"\x01\x00\x00\x00", b"value", flags=0)
    info = kernel.object_info(41, "program")
    link = kernel.link_create(program_fd=opened, target_fd=17, attach_type=10)
    kernel.obj_pin(link, tmp_path / "link")
    link_info = kernel.object_info(link, "link")

    assert [item[0] for item in raw.calls] == [321] * 6
    assert [item[1] for item in raw.calls] == [
        BPF_OBJ_GET,
        BPF_MAP_UPDATE_ELEM,
        BPF_OBJ_GET_INFO_BY_FD,
        BPF_LINK_CREATE,
        BPF_OBJ_PIN,
        BPF_OBJ_GET_INFO_BY_FD,
    ]
    assert opened == 41
    assert link == 42
    assert info == BpfObjectInfo(kind="program", object_id=701, object_type=18, name="guard_connect4")
    assert link_info == BpfObjectInfo(
        kind="link",
        object_id=901,
        object_type=3,
        name="",
        program_id=701,
        cgroup_id=501,
        attach_type=10,
    )

    get_attr = raw.calls[0][2]
    assert raw.paths == [os.fsencode(pin), os.fsencode(tmp_path / "link")]
    assert not any(get_attr[16:])

    update_attr = raw.calls[1][2]
    map_fd, key_address, value_address, flags = struct.unpack_from("<I4xQQQ", update_attr)
    assert map_fd == opened
    assert (key_address, value_address) != (0, 0)
    assert raw.map_payloads == [(b"\x01\x00\x00\x00", b"value")]
    assert flags == 0
    assert not any(update_attr[32:])

    link_attr = raw.calls[3][2]
    assert struct.unpack_from("<IIII", link_attr) == (opened, 17, 10, 0)
    assert not any(link_attr[16:])


def _limits(value: int) -> TrafficLimits:
    return TrafficLimits(
        ingress_bytes_per_second=value,
        egress_bytes_per_second=value,
        ingress_packets_per_second=value,
        egress_packets_per_second=value,
        new_flows_per_second=value,
        dns_queries_per_second=value,
        max_concurrent_flows=value,
    )


def _policy(object_digest: str) -> NetworkPolicy:
    trusted_v4 = Endpoint("203.0.113.10", 443, "tcp")
    trusted_v6 = Endpoint("2001:db8::10", 443, "tcp")
    build_v4 = Endpoint("198.51.100.20", 443, "tcp")
    build_dns = Endpoint("198.51.100.53", 53, "udp")
    return NetworkPolicy(
        containment_policy_sha256="c" * 64,
        resource_profile_sha256="d" * 64,
        bpf_program_sha256=object_digest,
        bpf_map_schema_sha256="e" * 64,
        scopes=(
            ScopeNetworkPolicy(
                "root",
                (build_v4, build_dns, trusted_v4),
                (trusted_v6,),
                _limits(300),
            ),
            ScopeNetworkPolicy("trusted-service", (trusted_v4,), (trusted_v6,), _limits(100)),
            ScopeNetworkPolicy("build-egress", (build_v4, build_dns), (), _limits(200)),
        ),
    )


def _policy_document() -> dict[str, object]:
    def scope(
        ipv4: list[dict[str, object]],
        *,
        value: int,
    ) -> dict[str, object]:
        return {
            "ipv4": ipv4,
            "ipv6": [],
            "limits": {
                "ingress_bytes_per_second": value,
                "egress_bytes_per_second": value,
                "ingress_packets_per_second": value,
                "egress_packets_per_second": value,
                "new_flows_per_second": value,
                "dns_queries_per_second": value,
                "max_concurrent_flows": value,
            },
        }

    trusted = {"address": "203.0.113.10", "port": 443, "protocol": "tcp"}
    build = {"address": "198.51.100.20", "port": 443, "protocol": "tcp"}
    return {
        "schema": "loom.task-image-builder-guard-network-policy/v1",
        "resource_profile_sha256": "d" * 64,
        "bpf_program_sha256": "e" * 64,
        "bpf_map_schema_sha256": "f" * 64,
        "scopes": {
            "root": scope([build, trusted], value=300),
            "trusted-service": scope([trusted], value=100),
            "build-egress": scope([build], value=200),
        },
    }


def _canonical_policy(document: dict[str, object]) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def test_network_policy_file_is_canonical_root_owned_and_digest_bound(tmp_path: Path) -> None:
    payload = _canonical_policy(_policy_document())
    path = tmp_path / "network-policy.json"
    path.write_bytes(payload)
    path.chmod(0o444)

    policy = NetworkPolicy.from_file(
        path,
        uid=os.geteuid(),
        gid=os.getegid(),
        containment_policy_sha256=hashlib.sha256(payload).hexdigest(),
        resource_profile_sha256="d" * 64,
        bpf_program_sha256="e" * 64,
        bpf_map_schema_sha256="f" * 64,
    )

    assert tuple(scope.name for scope in policy.scopes) == (
        "root",
        "trusted-service",
        "build-egress",
    )
    assert tuple(endpoint.address for endpoint in policy.scopes[0].ipv4) == (
        "198.51.100.20",
        "203.0.113.10",
    )


def test_network_policy_rejects_noncanonical_or_semantically_broadened_file(
    tmp_path: Path,
) -> None:
    document = _policy_document()
    payloads = [json.dumps(document, indent=2).encode("ascii")]
    scopes = document["scopes"]
    assert isinstance(scopes, dict)
    root = scopes["root"]
    assert isinstance(root, dict)
    root["ipv4"] = []
    payloads.append(_canonical_policy(document))

    for index, payload in enumerate(payloads):
        path = tmp_path / f"network-policy-{index}.json"
        path.write_bytes(payload)
        path.chmod(0o444)
        with pytest.raises(GuardError) as caught:
            NetworkPolicy.from_file(
                path,
                uid=os.geteuid(),
                gid=os.getegid(),
                containment_policy_sha256=hashlib.sha256(payload).hexdigest(),
                resource_profile_sha256="d" * 64,
                bpf_program_sha256="e" * 64,
                bpf_map_schema_sha256="f" * 64,
            )
        assert caught.value.code in {
            "bpf_policy_json_invalid",
            "bpf_policy_endpoint_invalid",
            "bpf_policy_root_invalid",
        }


@dataclass(frozen=True)
class _Tree:
    targets: tuple[BpfScopeTarget, ...]

    def bpf_scope_targets(self) -> tuple[BpfScopeTarget, ...]:
        return self.targets


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[Path, tuple[str, ...]]] = []

    def run(self, command: CommandIdentity, argv: tuple[str, ...]) -> CommandResult:
        self.calls.append((command.path, argv))
        assert argv[:2] == ("prog", "loadall")
        program_root = Path(argv[3])
        map_root = Path(argv[5])
        program_root.mkdir(parents=True, exist_ok=True)
        map_root.mkdir(parents=True, exist_ok=True)
        for name in PROGRAM_TYPES:
            (program_root / name).touch()
        for name in MAP_LAYOUTS:
            (map_root / name).touch()
        return CommandResult(0, "", "")


class _Kernel:
    def __init__(
        self,
        *,
        bad_link: bool = False,
        bad_map: bool = False,
        duplicate_id: bool = False,
        overlap_kinds: bool = False,
    ) -> None:
        self.bad_link = bad_link
        self.bad_map = bad_map
        self.duplicate_id = duplicate_id
        self.overlap_kinds = overlap_kinds
        self.events: list[tuple[object, ...]] = []
        self.objects: dict[int, BpfObjectInfo] = {}
        self.next_fd = 100
        self.next_id = 1000
        self.kind_ids = {"program": 1000, "map": 1000, "link": 1000}
        self.closed: list[int] = []

    def _allocate(self, info: BpfObjectInfo) -> int:
        self.next_fd += 1
        self.objects[self.next_fd] = info
        return self.next_fd

    def obj_get(self, path: Path) -> int:
        self.events.append(("obj_get", path))
        kind = "program" if path.parent.name == "progs" else "map"
        if self.overlap_kinds:
            self.kind_ids[kind] += 1
            object_id = self.kind_ids[kind]
        else:
            self.next_id += 0 if self.duplicate_id else 1
            object_id = self.next_id
        if path.parent.name == "progs":
            info = BpfObjectInfo(
                "program", object_id, PROGRAM_TYPES[path.name], path.name[:15]
            )
        else:
            map_type, key_size, value_size, maximum = MAP_LAYOUTS[path.name]
            info = BpfObjectInfo(
                "map",
                object_id,
                map_type,
                "wrong_map" if self.bad_map else path.name,
                key_size=key_size,
                value_size=value_size,
                max_entries=maximum,
            )
        return self._allocate(info)

    def map_update(self, descriptor: int, key: bytes, value: bytes, *, flags: int = 0) -> None:
        self.events.append(("map_update", descriptor, key, value, flags))

    def object_info(self, descriptor: int, kind: str) -> BpfObjectInfo:
        self.events.append(("object_info", descriptor, kind))
        info = self.objects[descriptor]
        if kind == "link" and self.bad_link:
            return BpfObjectInfo(
                "link",
                info.object_id,
                info.object_type,
                "",
                program_id=info.program_id,
                cgroup_id=info.cgroup_id,
                attach_type=(info.attach_type or 0) + 1,
            )
        return info

    def link_create(self, *, program_fd: int, target_fd: int, attach_type: int) -> int:
        self.events.append(("link_create", program_fd, target_fd, attach_type))
        if self.overlap_kinds:
            self.kind_ids["link"] += 1
            object_id = self.kind_ids["link"]
        else:
            self.next_id += 1
            object_id = self.next_id
        program = self.objects[program_fd]
        return self._allocate(
            BpfObjectInfo(
                "link",
                object_id,
                3,
                "",
                program_id=program.object_id,
                cgroup_id={21: 501, 22: 502, 23: 503}[target_fd],
                attach_type=attach_type,
            )
        )

    def obj_pin(self, descriptor: int, path: Path) -> None:
        self.events.append(("obj_pin", descriptor, path))
        path.touch()

    def close(self, descriptor: int) -> None:
        self.closed.append(descriptor)


def _loader(tmp_path: Path, kernel: _Kernel, runner: _Runner, *, object_payload: bytes) -> BpfLoader:
    object_path = tmp_path / "guard.bpf.o"
    object_path.write_bytes(object_payload)
    object_path.chmod(0o444)
    bpffs_root = tmp_path / "bpffs"
    bpffs_root.mkdir()
    bpffs_root.chmod(0o700)
    return BpfLoader(
        kernel=kernel,
        runner=runner,
        bpftool=CommandIdentity(Path("/opt/release/bpftool"), DIGEST),
        bpf_object_path=object_path,
        bpffs_root=bpffs_root,
        containment_policy_sha256="c" * 64,
        resource_profile_sha256="d" * 64,
        bpf_map_schema_sha256="e" * 64,
        trusted_uid=os.geteuid(),
        close_fd=kernel.close,
        staging_suffix=lambda: "fixed",
    )


def _tree() -> _Tree:
    return _Tree(
        (
            BpfScopeTarget("root", 21, 501),
            BpfScopeTarget("trusted-service", 22, 502),
            BpfScopeTarget("build-egress", 23, 503),
        )
    )


def test_loader_populates_all_independent_maps_before_pinning_exact_24_links(
    tmp_path: Path,
) -> None:
    payload = b"reviewed-bpf-object"
    kernel = _Kernel()
    runner = _Runner()
    loader = _loader(tmp_path, kernel, runner, object_payload=payload)

    attachment = loader.attach(_tree(), _policy(hashlib.sha256(payload).hexdigest()), GRANT)

    assert len(runner.calls) == 3
    assert [call[1][2] for call in runner.calls] == [str(loader.bpf_object_path)] * 3
    first_link = next(index for index, event in enumerate(kernel.events) if event[0] == "link_create")
    assert all(event[0] != "map_update" for event in kernel.events[first_link:])
    link_calls = [event for event in kernel.events if event[0] == "link_create"]
    assert len(link_calls) == 24
    assert {(event[2], event[3]) for event in link_calls} == {
        (target.descriptor, attach_type)
        for target in _tree().targets
        for _name, attach_type, _program_type in ATTACHMENTS
    }
    assert attachment.pin_path == loader.bpffs_root / str(GRANT)
    assert attachment.pin_path.is_dir()
    assert len(attachment.link_ids) == 24
    assert len(attachment.program_ids) == 24
    assert len(attachment.map_ids) == 18
    assert attachment.link_ids == tuple(sorted(attachment.link_ids))
    assert len(set(kernel.closed)) == 66


def test_loader_preserves_deny_pins_and_refuses_publish_on_link_readback_mismatch(
    tmp_path: Path,
) -> None:
    payload = b"reviewed-bpf-object"
    kernel = _Kernel(bad_link=True)
    runner = _Runner()
    loader = _loader(tmp_path, kernel, runner, object_payload=payload)

    with pytest.raises(GuardError) as caught:
        loader.attach(_tree(), _policy(hashlib.sha256(payload).hexdigest()), GRANT)

    assert caught.value.code == "bpf_link_readback_invalid"
    assert not (loader.bpffs_root / str(GRANT)).exists()
    staging = loader.bpffs_root / f"staging-{GRANT}-fixed"
    assert staging.is_dir()
    assert any(path.parent.name == "links" for path in staging.rglob("*"))
    assert not any(event[0] == "unlink" for event in kernel.events)
    opened_count = sum(event[0] in {"obj_get", "link_create"} for event in kernel.events)
    assert len(kernel.closed) == opened_count


def test_loader_rejects_policy_or_object_identity_before_running_bpftool(tmp_path: Path) -> None:
    payload = b"reviewed-bpf-object"
    kernel = _Kernel()
    runner = _Runner()
    loader = _loader(tmp_path, kernel, runner, object_payload=payload)

    with pytest.raises(GuardError) as caught:
        loader.attach(_tree(), _policy("f" * 64), GRANT)

    assert caught.value.code == "bpf_object_identity_invalid"
    assert runner.calls == []
    assert kernel.events == []


def test_loader_rejects_duplicate_kernel_ids(tmp_path: Path) -> None:
    payload = b"reviewed-bpf-object"
    kernel = _Kernel(duplicate_id=True)
    runner = _Runner()
    loader = _loader(tmp_path, kernel, runner, object_payload=payload)

    with pytest.raises(GuardError) as caught:
        loader.attach(_tree(), _policy(hashlib.sha256(payload).hexdigest()), GRANT)

    assert caught.value.code == "bpf_object_readback_invalid"
    assert not any(event[0] == "link_create" for event in kernel.events)


def test_partial_scope_readback_failure_closes_every_open_descriptor(tmp_path: Path) -> None:
    payload = b"reviewed-bpf-object"
    kernel = _Kernel(bad_map=True)
    runner = _Runner()
    loader = _loader(tmp_path, kernel, runner, object_payload=payload)

    with pytest.raises(GuardError) as caught:
        loader.attach(_tree(), _policy(hashlib.sha256(payload).hexdigest()), GRANT)

    assert caught.value.code == "bpf_object_readback_invalid"
    opened = [event for event in kernel.events if event[0] == "obj_get"]
    assert len(kernel.closed) == len(opened)
    assert len(kernel.closed) == len(set(kernel.closed))


def test_loader_accepts_ids_that_overlap_only_across_kernel_object_namespaces(
    tmp_path: Path,
) -> None:
    payload = b"reviewed-bpf-object"
    kernel = _Kernel(overlap_kinds=True)
    runner = _Runner()
    loader = _loader(tmp_path, kernel, runner, object_payload=payload)

    attachment = loader.attach(_tree(), _policy(hashlib.sha256(payload).hexdigest()), GRANT)

    assert set(attachment.program_ids).intersection(attachment.map_ids)
    assert set(attachment.program_ids).intersection(attachment.link_ids)
    assert len(set(attachment.program_ids)) == 24
    assert len(set(attachment.map_ids)) == 18
    assert len(set(attachment.link_ids)) == 24
