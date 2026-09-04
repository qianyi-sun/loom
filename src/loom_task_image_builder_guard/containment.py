"""Exact cgroup-v2 containment preparation for one Slurm builder allocation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from itertools import pairwise
from pathlib import Path
from typing import Protocol
from uuid import UUID

from loom_task_image_builder_guard.bpf import (
    BpfAttachment,
    BpfScopeTarget,
    NetworkPolicy,
)
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.identity import BatchCgroup
from loom_task_image_builder_guard.models import CommandIdentity, IoLimit
from loom_task_image_builder_guard.slurm import CommandRunner

_MAX_CONTROL_BYTES = 64 * 1024
_CONTROL_NAME = re.compile(r"^[a-z][a-z0-9.]{0,63}$")
_CPU_COMPONENT = re.compile(r"^(0|[1-9][0-9]*)(?:-(0|[1-9][0-9]*))?$")
_BPF_TAG = re.compile(r"^[0-9a-f]{16}$")


@dataclass(frozen=True, slots=True)
class CgroupNode:
    path: Path
    authority_path: str
    descriptor: int
    device: int
    inode: int


class CgroupOperations(Protocol):
    def open_batch(self, batch: BatchCgroup) -> CgroupNode: ...

    def create_child(self, parent: CgroupNode, name: str) -> CgroupNode: ...

    def assert_stable(self, node: CgroupNode) -> None: ...

    def read(self, node: CgroupNode, name: str) -> str: ...

    def write(self, node: CgroupNode, name: str, value: str) -> None: ...

    def delegate_process_migration(
        self,
        common_ancestor: CgroupNode,
        destination: CgroupNode,
        *,
        uid: int,
        gid: int,
    ) -> dict[str, object]: ...

    def close(self, descriptor: int) -> None: ...


def _directory_identity(value: os.stat_result) -> tuple[int, ...]:
    return (value.st_dev, value.st_ino, value.st_mode, value.st_uid, value.st_gid)


class CgroupFilesystem:
    """Directory-FD-confined access to the exact Loom cgroup subtree."""

    def __init__(self, *, trusted_uid: int = 0) -> None:
        self.trusted_uid = trusted_uid

    @staticmethod
    def _open_directory(path: Path) -> tuple[int, os.stat_result]:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_CLOEXEC
            | os.O_DIRECTORY
            | getattr(os, "O_NOFOLLOW", 0),
        )
        return descriptor, os.fstat(descriptor)

    def open_batch(self, batch: BatchCgroup) -> CgroupNode:
        descriptor: int | None = None
        try:
            lexical = os.lstat(batch.path)
            if batch.path.resolve(strict=True) != batch.path:
                raise GuardError("containment_batch_invalid")
            descriptor, opened = self._open_directory(batch.path)
            if (
                _directory_identity(lexical) != _directory_identity(opened)
                or not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != self.trusted_uid
                or stat.S_IMODE(opened.st_mode) & 0o022
                or opened.st_ino != batch.inode
            ):
                raise GuardError("containment_batch_invalid")
            return CgroupNode(
                batch.path,
                batch.authority_path,
                descriptor,
                opened.st_dev,
                opened.st_ino,
            )
        except GuardError:
            if descriptor is not None:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise GuardError("containment_batch_invalid") from exc

    def create_child(self, parent: CgroupNode, name: str) -> CgroupNode:
        allowed = {
            "loom-builder": not parent.authority_path.endswith("/loom-builder"),
            "trusted-service": parent.authority_path.endswith("/loom-builder"),
            "build-egress": parent.authority_path.endswith("/loom-builder"),
        }
        if name not in allowed or not allowed[name]:
            raise GuardError("containment_path_invalid")
        self.assert_stable(parent)
        descriptor: int | None = None
        try:
            os.mkdir(name, mode=0o755, dir_fd=parent.descriptor)
            descriptor = os.open(
                name,
                os.O_RDONLY
                | os.O_CLOEXEC
                | os.O_DIRECTORY
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent.descriptor,
            )
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISDIR(opened.st_mode)
                or opened.st_uid != self.trusted_uid
                or opened.st_dev != parent.device
                or opened.st_ino <= 0
                or opened.st_ino == parent.inode
            ):
                raise GuardError("containment_path_invalid")
            self.assert_stable(parent)
            return CgroupNode(
                parent.path / name,
                f"{parent.authority_path}/{name}",
                descriptor,
                opened.st_dev,
                opened.st_ino,
            )
        except GuardError:
            if descriptor is not None:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor is not None:
                os.close(descriptor)
            raise GuardError("containment_path_invalid") from exc

    def assert_stable(self, node: CgroupNode) -> None:
        try:
            current = os.fstat(node.descriptor)
        except OSError as exc:
            raise GuardError("containment_cgroup_changed") from exc
        if (
            not stat.S_ISDIR(current.st_mode)
            or (current.st_dev, current.st_ino) != (node.device, node.inode)
        ):
            raise GuardError("containment_cgroup_changed")

    @staticmethod
    def _control_name(name: str) -> None:
        if not isinstance(name, str) or _CONTROL_NAME.fullmatch(name) is None:
            raise GuardError("containment_control_invalid")

    def read(self, node: CgroupNode, name: str) -> str:
        self._control_name(name)
        self.assert_stable(node)
        descriptor: int | None = None
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=node.descriptor,
            )
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise GuardError("containment_control_invalid")
            chunks: list[bytes] = []
            total = 0
            while total <= _MAX_CONTROL_BYTES:
                chunk = os.read(descriptor, min(4096, _MAX_CONTROL_BYTES + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
            if total > _MAX_CONTROL_BYTES:
                raise GuardError("containment_control_invalid")
            payload = b"".join(chunks)
            value = payload.decode("ascii")
            if "\x00" in value or "\r" in value:
                raise GuardError("containment_control_invalid")
            self.assert_stable(node)
            return value
        except (UnicodeDecodeError, OSError) as exc:
            raise GuardError("containment_control_invalid") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def write(self, node: CgroupNode, name: str, value: str) -> None:
        self._control_name(name)
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 4096
            or "\x00" in value
            or "\n" in value
            or "\r" in value
        ):
            raise GuardError("containment_write_invalid")
        try:
            payload = value.encode("ascii")
        except UnicodeEncodeError:
            raise GuardError("containment_write_invalid") from None
        self.assert_stable(node)
        descriptor: int | None = None
        try:
            descriptor = os.open(
                name,
                os.O_WRONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=node.descriptor,
            )
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise GuardError("containment_control_invalid")
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise GuardError("containment_write_invalid")
                view = view[written:]
            self.assert_stable(node)
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("containment_write_invalid") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def _delegate_process_file(
        self,
        node: CgroupNode,
        *,
        uid: int,
        gid: int,
    ) -> dict[str, object]:
        self.assert_stable(node)
        descriptor: int | None = None
        try:
            descriptor = os.open(
                "cgroup.procs",
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=node.descriptor,
            )
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_dev != node.device
                or before.st_uid != self.trusted_uid
                or stat.S_IMODE(before.st_mode) != 0o644
            ):
                raise GuardError("containment_process_delegation_invalid")
            os.fchown(descriptor, uid, gid)
            after = os.fstat(descriptor)
            if (
                (before.st_dev, before.st_ino, before.st_mode)
                != (after.st_dev, after.st_ino, after.st_mode)
                or after.st_uid != uid
                or after.st_gid != gid
            ):
                raise GuardError("containment_process_delegation_invalid")
            self.assert_stable(node)
            return {
                "path": f"{node.authority_path}/cgroup.procs",
                "uid": after.st_uid,
                "gid": after.st_gid,
                "mode": stat.S_IMODE(after.st_mode),
            }
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("containment_process_delegation_invalid") from exc
        finally:
            if descriptor is not None:
                os.close(descriptor)

    def delegate_process_migration(
        self,
        common_ancestor: CgroupNode,
        destination: CgroupNode,
        *,
        uid: int,
        gid: int,
    ) -> dict[str, object]:
        if (
            type(uid) is not int
            or type(gid) is not int
            or uid <= 0
            or gid <= 0
            or destination.authority_path
            != f"{common_ancestor.authority_path}/build-egress"
        ):
            raise GuardError("containment_process_delegation_invalid")
        common = self._delegate_process_file(common_ancestor, uid=uid, gid=gid)
        destination_file = self._delegate_process_file(destination, uid=uid, gid=gid)
        return {
            "common_ancestor": common,
            "destination": destination_file,
        }

    @staticmethod
    def close(descriptor: int) -> None:
        os.close(descriptor)


@dataclass(frozen=True, slots=True)
class DeviceProgram:
    program_id: int
    tag: str
    attach_type: str
    attach_flags: str
    name: str

    def __post_init__(self) -> None:
        if (
            type(self.program_id) is not int
            or not 1 <= self.program_id <= (1 << 32) - 1
            or not isinstance(self.tag, str)
            or _BPF_TAG.fullmatch(self.tag) is None
            or self.tag == "0" * 16
            or self.attach_type != "cgroup_device"
            or not all(
                isinstance(item, str)
                and len(item) <= 64
                and "\n" not in item
                and "\r" not in item
                for item in (self.attach_flags, self.name)
            )
            or not self.name
        ):
            raise GuardError("containment_device_authority_invalid")


class DeviceProbe(Protocol):
    def inspect(self, batch: CgroupNode) -> tuple[DeviceProgram, ...]: ...


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


class BpftoolDeviceProbe:
    def __init__(self, runner: CommandRunner, command: CommandIdentity) -> None:
        self.runner = runner
        self.command = command

    def inspect_path(self, path: Path) -> tuple[DeviceProgram, ...]:
        result = self.runner.run(
            self.command,
            ("-j", "cgroup", "show", str(path), "effective"),
        )
        if (
            result.returncode != 0
            or result.stderr
            or not result.stdout
            or len(result.stdout.encode("utf-8")) > _MAX_CONTROL_BYTES
        ):
            raise GuardError("containment_device_authority_invalid")
        try:
            raw = json.loads(result.stdout, object_pairs_hook=_pairs)
            if not isinstance(raw, list) or not raw or len(raw) > 64:
                raise ValueError("invalid program list")
            programs: list[DeviceProgram] = []
            for item in raw:
                required = {
                    "id",
                    "attach_type",
                    "name",
                    "attach_btf_obj_id",
                    "attach_btf_id",
                }
                if (
                    not isinstance(item, dict)
                    or set(item)
                    not in {
                        frozenset(required),
                        frozenset((*required, "attach_btf_name")),
                    }
                    or type(item["id"]) is not int
                    or not 1 <= item["id"] <= (1 << 32) - 1
                    or type(item["attach_btf_obj_id"]) is not int
                    or not 0 <= item["attach_btf_obj_id"] <= (1 << 32) - 1
                    or type(item["attach_btf_id"]) is not int
                    or not 0 <= item["attach_btf_id"] <= (1 << 32) - 1
                    or not all(
                        isinstance(item[name], str)
                        and item[name]
                        and len(item[name]) <= 64
                        and "\n" not in item[name]
                        and "\r" not in item[name]
                        for name in ("attach_type", "name")
                    )
                    or (
                        "attach_btf_name" in item
                        and (
                            not isinstance(item["attach_btf_name"], str)
                            or not item["attach_btf_name"]
                            or len(item["attach_btf_name"]) > 128
                        )
                    )
                ):
                    raise ValueError("invalid program")
                if item["attach_type"] != "cgroup_device":
                    continue
                if (
                    item["attach_btf_obj_id"] != 0
                    or item["attach_btf_id"] != 0
                    or "attach_btf_name" in item
                ):
                    raise ValueError("unexpected BTF attachment")
                identity = self.runner.run(
                    self.command,
                    ("-j", "prog", "show", "id", str(item["id"])),
                )
                if (
                    identity.returncode != 0
                    or identity.stderr
                    or not identity.stdout
                    or len(identity.stdout.encode("utf-8")) > _MAX_CONTROL_BYTES
                ):
                    raise ValueError("invalid program identity")
                program = json.loads(identity.stdout, object_pairs_hook=_pairs)
                if (
                    not isinstance(program, dict)
                    or program.get("id") != item["id"]
                    or program.get("type") != "cgroup_device"
                    or program.get("name") != item["name"]
                    or not isinstance(program.get("tag"), str)
                    or _BPF_TAG.fullmatch(program["tag"]) is None
                    or program["tag"] == "0" * 16
                    or program.get("uid") != 0
                    or program.get("orphaned") is not False
                    or type(program.get("bytes_xlated")) is not int
                    or not 1 <= program["bytes_xlated"] <= _MAX_CONTROL_BYTES
                    or type(program.get("gpl_compatible")) is not bool
                    or not isinstance(program.get("map_ids", []), list)
                    or bool(program.get("map_ids", []))
                ):
                    raise ValueError("invalid program identity")
                programs.append(
                    DeviceProgram(
                        item["id"],
                        program["tag"],
                        item["attach_type"],
                        "",
                        item["name"],
                    )
                )
        except (
            json.JSONDecodeError,
            RecursionError,
            TypeError,
            ValueError,
            GuardError,
        ):
            raise GuardError("containment_device_authority_invalid") from None
        canonical = tuple(sorted(programs, key=lambda item: (item.tag, item.program_id)))
        if (
            len({item.program_id for item in canonical}) != len(canonical)
            or len({item.tag for item in canonical}) != len(canonical)
        ):
            raise GuardError("containment_device_authority_invalid")
        return canonical

    def inspect(self, batch: CgroupNode) -> tuple[DeviceProgram, ...]:
        return self.inspect_path(batch.path)


@dataclass(frozen=True, slots=True)
class GuardPolicy:
    cpus: int
    memory_mib: int
    device_program_tags: tuple[str, ...]
    pids_max: int
    io_limits: tuple[IoLimit, ...]
    network: NetworkPolicy

    def __post_init__(self) -> None:
        if (
            type(self.cpus) is not int
            or not 1 <= self.cpus <= 65536
            or type(self.memory_mib) is not int
            or self.memory_mib <= 0
            or not isinstance(self.device_program_tags, tuple)
            or not self.device_program_tags
            or self.device_program_tags
            != tuple(sorted(set(self.device_program_tags)))
            or any(
                not isinstance(item, str)
                or _BPF_TAG.fullmatch(item) is None
                or item == "0" * 16
                for item in self.device_program_tags
            )
            or type(self.pids_max) is not int
            or self.pids_max <= 0
            or not isinstance(self.io_limits, tuple)
            or not self.io_limits
            or tuple(item.device for item in self.io_limits)
            != tuple(sorted({item.device for item in self.io_limits}))
            or not isinstance(self.network, NetworkPolicy)
        ):
            raise GuardError("containment_policy_invalid")


class BpfAttacher(Protocol):
    def attach(
        self,
        tree: ContainmentTree,
        policy: NetworkPolicy,
        grant_id: UUID,
    ) -> BpfAttachment: ...


@dataclass(slots=True)
class ContainmentTree:
    filesystem: CgroupOperations = field(repr=False)
    batch: CgroupNode
    root: CgroupNode
    trusted_service: CgroupNode
    build_egress: CgroupNode
    _closed: bool = field(default=False, init=False, repr=False)

    def bpf_scope_targets(self) -> tuple[BpfScopeTarget, ...]:
        if self._closed:
            raise GuardError("containment_tree_closed")
        for node in (self.root, self.trusted_service, self.build_egress):
            self.filesystem.assert_stable(node)
        return (
            BpfScopeTarget("root", self.root.descriptor, self.root.inode),
            BpfScopeTarget(
                "trusted-service", self.trusted_service.descriptor, self.trusted_service.inode
            ),
            BpfScopeTarget("build-egress", self.build_egress.descriptor, self.build_egress.inode),
        )

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for node in (self.build_egress, self.trusted_service, self.root, self.batch):
            try:
                self.filesystem.close(node.descriptor)
            except OSError:
                pass


@dataclass(slots=True)
class ContainmentAttachment:
    tree: ContainmentTree = field(repr=False)
    cgroup_inode: int
    containment_root: str
    trusted_service_cgroup: str
    build_egress_cgroup: str
    bpf_program_sha256: str
    bpf_map_schema_sha256: str
    containment_policy_sha256: str
    resource_limits_sha256: str
    probe_sha256: str
    link_ids: tuple[int, ...]
    program_ids: tuple[int, ...]
    map_ids: tuple[int, ...]
    probe: bytes

    def close(self) -> None:
        self.tree.close()


class PeerForContainment(Protocol):
    pid: int
    uid: int
    gid: int

    def assert_unchanged(self) -> None: ...

    def adopt_trusted_service_cgroup(self) -> None: ...

    def containment_hold(self) -> AbstractContextManager[None]: ...


def _tokens(value: str) -> tuple[str, ...]:
    tokens = tuple(value.split())
    if not tokens or len(tokens) != len(set(tokens)):
        raise GuardError("containment_inherited_resources_invalid")
    return tuple(sorted(tokens))


def _processes(value: str) -> tuple[int, ...]:
    try:
        rows = tuple(int(item) for item in value.split())
    except ValueError:
        raise GuardError("containment_processes_invalid") from None
    if any(item <= 0 for item in rows) or len(rows) != len(set(rows)):
        raise GuardError("containment_processes_invalid")
    return rows


def _cgroup_stat(value: str) -> tuple[int, int]:
    rows: dict[str, int] = {}
    try:
        for line in value.splitlines():
            fields = line.split()
            if (
                len(fields) != 2
                or not fields[0].startswith("nr_")
                or fields[0] in rows
            ):
                raise ValueError
            parsed = int(fields[1])
            if not 0 <= parsed <= (1 << 63) - 1:
                raise ValueError
            rows[fields[0]] = parsed
    except ValueError:
        raise GuardError("containment_descendants_invalid") from None
    if "nr_descendants" not in rows or "nr_dying_descendants" not in rows:
        raise GuardError("containment_descendants_invalid")
    return rows["nr_descendants"], rows["nr_dying_descendants"]


def _cpu_count(value: str) -> int:
    ranges: list[tuple[int, int]] = []
    for component in value.strip().split(","):
        matched = _CPU_COMPONENT.fullmatch(component)
        if matched is None:
            raise GuardError("containment_inherited_resources_invalid")
        start = int(matched.group(1))
        end = start if matched.group(2) is None else int(matched.group(2))
        if start > end or end > (1 << 31) - 1:
            raise GuardError("containment_inherited_resources_invalid")
        ranges.append((start, end))
    ranges.sort()
    if any(current[0] <= previous[1] for previous, current in pairwise(ranges)):
        raise GuardError("containment_inherited_resources_invalid")
    return sum(end - start + 1 for start, end in ranges)


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


class ContainmentManager:
    def __init__(
        self,
        *,
        filesystem: CgroupOperations,
        bpf_loader: BpfAttacher,
        device_probe: DeviceProbe,
    ) -> None:
        self.filesystem = filesystem
        self.bpf_loader = bpf_loader
        self.device_probe = device_probe

    def _read(self, node: CgroupNode, name: str) -> str:
        self.filesystem.assert_stable(node)
        value = self.filesystem.read(node, name)
        self.filesystem.assert_stable(node)
        return value

    def _write(self, node: CgroupNode, name: str, value: str) -> None:
        self.filesystem.assert_stable(node)
        self.filesystem.write(node, name, value)
        self.filesystem.assert_stable(node)

    def _verify_inheritance(
        self,
        batch: CgroupNode,
        peer: PeerForContainment,
        policy: GuardPolicy,
    ) -> dict[str, object]:
        if self._read(batch, "cgroup.type").strip() != "domain" or _processes(
            self._read(batch, "cgroup.procs")
        ) != (peer.pid,):
            raise GuardError("containment_processes_invalid")
        self._verify_descendants(batch, 0)
        controllers = _tokens(self._read(batch, "cgroup.controllers"))
        delegated = tuple(self._read(batch, "cgroup.subtree_control").split())
        if not {"io", "pids"}.issubset(controllers) or delegated:
            raise GuardError("containment_controller_invalid")
        cpu_count = _cpu_count(self._read(batch, "cpuset.cpus.effective"))
        cpu_max = self._read(batch, "cpu.max").split()
        try:
            quota, period = (int(item) for item in cpu_max)
            memory_max = int(self._read(batch, "memory.max").strip())
            swap_max = int(self._read(batch, "memory.swap.max").strip())
        except (ValueError, TypeError):
            raise GuardError("containment_inherited_resources_invalid") from None
        if (
            len(cpu_max) != 2
            or quota <= 0
            or period <= 0
            or quota != policy.cpus * period
            or cpu_count != policy.cpus
            or memory_max != policy.memory_mib * 1024 * 1024
            or swap_max != 0
        ):
            raise GuardError("containment_inherited_resources_invalid")
        device_programs = self.device_probe.inspect(batch)
        if tuple(item.tag for item in device_programs) != policy.device_program_tags:
            raise GuardError("containment_device_authority_invalid")
        return {
            "controllers": list(controllers),
            "delegated": list(delegated),
            "cpu_count": cpu_count,
            "cpu_max": [quota, period],
            "memory_max": memory_max,
            "memory_swap_max": swap_max,
            "device_programs": [
                {
                    "id": item.program_id,
                    "tag": item.tag,
                    "attach_type": item.attach_type,
                    "attach_flags": item.attach_flags,
                    "name": item.name,
                }
                for item in device_programs
            ],
        }

    def _verify_descendants(self, node: CgroupNode, expected: int) -> None:
        descendants, dying = _cgroup_stat(self._read(node, "cgroup.stat"))
        if descendants != expected or dying != 0:
            raise GuardError("containment_descendants_invalid")

    def _verify_empty(self, node: CgroupNode) -> None:
        if self._read(node, "cgroup.type").strip() != "domain" or _processes(
            self._read(node, "cgroup.procs")
        ):
            raise GuardError("containment_cgroup_not_empty")

    def _delegate(self, node: CgroupNode) -> None:
        self._write(node, "cgroup.subtree_control", "+io +pids")
        if _tokens(self._read(node, "cgroup.subtree_control")) != ("io", "pids"):
            raise GuardError("containment_limit_readback_invalid")

    def _apply_limits(self, root: CgroupNode, policy: GuardPolicy) -> dict[str, object]:
        self._write(root, "pids.max", str(policy.pids_max))
        if self._read(root, "pids.max").strip() != str(policy.pids_max):
            raise GuardError("containment_limit_readback_invalid")
        expected_io: list[str] = []
        for item in policy.io_limits:
            row = (
                f"{item.device} rbps={item.rbps} wbps={item.wbps} "
                f"riops={item.riops} wiops={item.wiops}"
            )
            self._write(root, "io.max", row)
            expected_io.append(row)
        observed_io = tuple(sorted(line.strip() for line in self._read(root, "io.max").splitlines() if line))
        if observed_io != tuple(sorted(expected_io)):
            raise GuardError("containment_limit_readback_invalid")
        return {"pids_max": policy.pids_max, "io_max": expected_io}

    def prepare(
        self,
        batch: BatchCgroup,
        peer: PeerForContainment,
        policy: GuardPolicy,
        grant_id: UUID,
    ) -> ContainmentAttachment:
        if batch.peer_pid != peer.pid or not isinstance(grant_id, UUID) or grant_id.int == 0:
            raise GuardError("containment_identity_invalid")
        peer.assert_unchanged()
        batch_node = self.filesystem.open_batch(batch)
        opened_nodes = [batch_node]
        tree: ContainmentTree | None = None
        success = False
        try:
            inherited = self._verify_inheritance(batch_node, peer, policy)
            root = self.filesystem.create_child(batch_node, "loom-builder")
            opened_nodes.append(root)
            trusted = self.filesystem.create_child(root, "trusted-service")
            opened_nodes.append(trusted)
            build = self.filesystem.create_child(root, "build-egress")
            opened_nodes.append(build)
            tree = ContainmentTree(self.filesystem, batch_node, root, trusted, build)
            for node in (root, trusted, build):
                self._verify_empty(node)
            with peer.containment_hold():
                peer.assert_unchanged()
                bpf = self.bpf_loader.attach(tree, policy.network, grant_id)
                peer.assert_unchanged()
                self._write(trusted, "cgroup.procs", str(peer.pid))
                peer.adopt_trusted_service_cgroup()
                peer.assert_unchanged()
                if _processes(self._read(batch_node, "cgroup.procs")) or _processes(
                    self._read(root, "cgroup.procs")
                ):
                    raise GuardError("containment_processes_invalid")
                if _processes(
                    self._read(trusted, "cgroup.procs")
                ) != (peer.pid,) or _processes(self._read(build, "cgroup.procs")):
                    raise GuardError("containment_processes_invalid")
                for node, expected_descendants in (
                    (batch_node, 3),
                    (root, 2),
                    (trusted, 0),
                    (build, 0),
                ):
                    self._verify_descendants(node, expected_descendants)
                self._delegate(batch_node)
                self._delegate(root)
                applied = self._apply_limits(root, policy)
                if self._read(build, "cgroup.subtree_control").split():
                    raise GuardError("containment_controller_invalid")
                process_migration = self.filesystem.delegate_process_migration(
                    root,
                    build,
                    uid=peer.uid,
                    gid=peer.gid,
                )
                probe: dict[str, object] = {
                    "schema": "loom.task-image-builder-guard-containment-probe/v1",
                    "cgroups": {
                        "batch": {
                            "path": batch_node.authority_path,
                            "inode": batch_node.inode,
                        },
                        "root": {"path": root.authority_path, "inode": root.inode},
                        "trusted_service": {
                            "path": trusted.authority_path,
                            "inode": trusted.inode,
                        },
                        "build_egress": {
                            "path": build.authority_path,
                            "inode": build.inode,
                        },
                    },
                    "descendants": {
                        "batch": 3,
                        "root": 2,
                        "trusted_service": 0,
                        "build_egress": 0,
                    },
                    "inherited": inherited,
                    "applied": applied,
                    "process_migration": process_migration,
                    "bpf": {
                        "pin_path": str(bpf.pin_path),
                        "link_ids": list(bpf.link_ids),
                        "program_ids": list(bpf.program_ids),
                        "map_ids": list(bpf.map_ids),
                    },
                }
                probe_payload = _canonical(probe)
                probe_sha256 = hashlib.sha256(probe_payload).hexdigest()
                peer.assert_unchanged()
            success = True
            return ContainmentAttachment(
                tree=tree,
                cgroup_inode=batch.inode,
                containment_root=root.authority_path,
                trusted_service_cgroup=trusted.authority_path,
                build_egress_cgroup=build.authority_path,
                bpf_program_sha256=policy.network.bpf_program_sha256,
                bpf_map_schema_sha256=policy.network.bpf_map_schema_sha256,
                containment_policy_sha256=policy.network.containment_policy_sha256,
                resource_limits_sha256=policy.network.resource_profile_sha256,
                probe_sha256=probe_sha256,
                link_ids=bpf.link_ids,
                program_ids=bpf.program_ids,
                map_ids=bpf.map_ids,
                probe=probe_payload,
            )
        finally:
            if not success:
                if tree is not None:
                    tree.close()
                else:
                    for node in reversed(opened_nodes):
                        try:
                            self.filesystem.close(node.descriptor)
                        except OSError:
                            pass


__all__ = [
    "BpftoolDeviceProbe",
    "CgroupFilesystem",
    "CgroupNode",
    "ContainmentAttachment",
    "ContainmentManager",
    "ContainmentTree",
    "DeviceProgram",
    "GuardPolicy",
]
