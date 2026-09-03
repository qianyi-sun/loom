#!/usr/bin/env python3
# ruff: noqa: E402
"""Verify a staged node-guard release while preserving production inertness."""

from __future__ import annotations

# The direct operator entry point establishes trusted checkout imports first.
import argparse
import ctypes
import errno
import fcntl
import hashlib
import json
import os
import platform
import posixpath
import re
import stat
import struct
import subprocess
import sys
import tomllib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from uuid import UUID, uuid4

_DIRECT_SCRIPT = Path(__file__).resolve(strict=True)
_DIRECT_REPOSITORY = _DIRECT_SCRIPT.parents[2]
if __package__ in {None, ""}:
    if _DIRECT_SCRIPT != (
        _DIRECT_REPOSITORY / "scripts/ops/task_image_builder_guard_conformance.py"
    ):
        raise RuntimeError("conformance script path is invalid")
    _DIRECT_IMPORT_ROOTS = (_DIRECT_REPOSITORY, _DIRECT_REPOSITORY / "src")
    if any(not path.is_dir() for path in _DIRECT_IMPORT_ROOTS):
        raise RuntimeError("conformance import roots are unavailable")
    sys.path[:0] = [str(path) for path in _DIRECT_IMPORT_ROOTS]

from scripts.ops.task_image_builder_guard_release import (
    Architecture,
    GuardReleaseError,
    VerifiedGuardRelease,
    verify_release_directory,
)
from scripts.ops.task_image_builder_guard_release import (
    validate_unit as validate_release_unit,
)

from loom_task_image_builder_guard.bpf import (
    ATTACHMENTS,
    BpfAttachment,
    BpfLoader,
    BpfScopeTarget,
    BpfSyscall,
    Endpoint,
    NetworkPolicy,
    ScopeNetworkPolicy,
    TrafficLimits,
)
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import CommandIdentity
from loom_task_image_builder_guard.slurm import PinnedCommandRunner

ROOT = Path(__file__).resolve().parents[2]
_SCHEMA = "loom.task-image-builder-guard-conformance/v1"
_BLOCKER = "phase2_guard_provider_release_missing"
_RELEASE_PREFIX = Path("opt/loom-task-image-builder-guard/releases")
_SPEC = Path("deploy/task-image-builder/guard-release-v1.json")
_PREREQUISITES = Path("deploy/task-image-builder/prerequisites-v1.toml")
_PROVIDERS = Path("deploy/task-image-builder/rootless-provider-v1.toml")
_AUTHORITY = Path("deploy/task-image-builder/authority-service-v1.yaml")
_PREREQUISITES_SHA256 = "c661d1ee54b392f05f1d4eb2381ee10bc6579381ebdd817cdd440d9bb2ae5046"
_PROVIDERS_SHA256 = "a5286f925d1e78aa08673c36c922425f8f417d6b674fe99dfbef042542d25186"
_AUTHORITY_SHA256 = "b72c82f110c3b8b013fb14782f1dd7cb6faedccb4ae644fcdb783a236dc173fe"
_SELF_CHECK = (
    b'{"schema":"loom.task-image-builder-node-guard-self-check/v1",'
    b'"status":"ok"}\n'
)
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_UNIT_NAME = "loom-task-image-builder-node-guard.service"
_SYSTEMD_ROOTS = (
    Path("etc/systemd/system.control"),
    Path("run/systemd/system.control"),
    Path("run/systemd/transient"),
    Path("run/systemd/generator.early"),
    Path("etc/systemd/system"),
    Path("etc/systemd/system.attached"),
    Path("run/systemd/system"),
    Path("run/systemd/system.attached"),
    Path("run/systemd/generator"),
    Path("usr/local/lib/systemd/system"),
    Path("usr/lib/systemd/system"),
    Path("run/systemd/generator.late"),
)
_MAX_SYSTEMD_ENTRIES = 16 * 1024
_MAX_PROCESSES = 1 << 20
_MAX_CMDLINE_BYTES = 64 * 1024
_GUARD_ARCHIVE_ARGUMENT = re.compile(
    rb"^/opt/loom-task-image-builder-guard/releases/[0-9a-f]{64}/"
    rb"loom-task-image-builder-guard\.pyz$"
)
_REQUIRED_PROGRAM_TYPES = (
    "have_cgroup_skb_prog_type",
    "have_cgroup_sock_prog_type",
    "have_cgroup_sock_addr_prog_type",
)
_REQUIRED_MAP_TYPES = (
    "have_hash_map_type",
    "have_array_map_type",
    "have_percpu_array_map_type",
)
_REQUIRED_HELPERS = {
    "cgroup_skb_available_helpers": frozenset(
        {
            "bpf_map_lookup_elem",
            "bpf_ktime_get_ns",
            "bpf_spin_lock",
            "bpf_spin_unlock",
        }
    ),
    "cgroup_sock_addr_available_helpers": frozenset(
        {
            "bpf_map_lookup_elem",
            "bpf_ktime_get_ns",
            "bpf_spin_lock",
            "bpf_spin_unlock",
        }
    ),
    "cgroup_sock_available_helpers": frozenset(
        {
            "bpf_map_lookup_elem",
            "bpf_map_update_elem",
            "bpf_map_delete_elem",
            "bpf_ktime_get_ns",
            "bpf_get_socket_cookie",
            "bpf_spin_lock",
            "bpf_spin_unlock",
        }
    ),
}
_BPF_LINK_CREATE = 28
_PROBE_GRANT = UUID("00000000-0000-4000-8000-000000000001")


def _is_guard_archive_argument(argument: bytes) -> bool:
    if not argument.startswith(b"/"):
        return False
    normalized = b"/" + posixpath.normpath(argument).lstrip(b"/")
    return _GUARD_ARCHIVE_ARGUMENT.fullmatch(normalized) is not None


class GuardConformanceError(ValueError):
    """A staged release or an inertness prerequisite is not exact."""


@dataclass(frozen=True, slots=True)
class ConformanceCheck:
    id: str
    status: Literal["pass"]
    evidence_sha256: str

    def as_dict(self) -> dict[str, object]:
        return {
            "evidence_sha256": self.evidence_sha256,
            "id": self.id,
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class ConformanceReport:
    release_sha256: str
    architecture: Architecture
    live: bool
    checks: tuple[ConformanceCheck, ...]
    production_ready: bool = False
    blockers: tuple[str, ...] = (_BLOCKER,)

    def as_dict(self) -> dict[str, object]:
        return {
            "architecture": self.architecture,
            "blockers": list(self.blockers),
            "checks": [item.as_dict() for item in self.checks],
            "live": self.live,
            "production_ready": self.production_ready,
            "release_sha256": self.release_sha256,
            "schema": _SCHEMA,
        }


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True) + "\n"
    ).encode("ascii")


def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _validate_bpftool_features(payload: bytes) -> None:
    def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    def reject_constant(_value: str) -> object:
        raise ValueError("non-finite number")

    try:
        raw = json.loads(
            payload,
            object_pairs_hook=pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError) as exc:
        raise GuardConformanceError("staged bpftool feature probe is invalid") from exc
    if not isinstance(raw, dict):
        raise GuardConformanceError("staged bpftool feature probe is invalid")
    syscall = raw.get("syscall_config")
    programs = raw.get("program_types")
    maps = raw.get("map_types")
    helpers = raw.get("helpers")
    if (
        not isinstance(syscall, dict)
        or syscall.get("have_bpf_syscall") is not True
        or not isinstance(programs, dict)
        or any(programs.get(name) is not True for name in _REQUIRED_PROGRAM_TYPES)
        or not isinstance(maps, dict)
        or any(maps.get(name) is not True for name in _REQUIRED_MAP_TYPES)
        or not isinstance(helpers, dict)
    ):
        raise GuardConformanceError("staged bpftool feature probe is insufficient")
    for name, required in _REQUIRED_HELPERS.items():
        available = helpers.get(name)
        if (
            not isinstance(available, list)
            or any(not isinstance(item, str) for item in available)
            or len(available) != len(set(available))
            or not required.issubset(available)
        ):
            raise GuardConformanceError("staged bpftool feature probe is insufficient")


def _read_virtual_path(path: Path, *, maximum: int, label: str) -> bytes:
    descriptor = -1
    try:
        initial = path.lstat()
        if stat.S_ISLNK(initial.st_mode) or not stat.S_ISREG(initial.st_mode):
            raise GuardConformanceError(f"{label} is unsafe")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if (initial.st_dev, initial.st_ino, initial.st_mode) != (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
        ):
            raise GuardConformanceError(f"{label} changed while opening")
        chunks: list[bytes] = []
        total = 0
        while total <= maximum:
            chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
        if total > maximum:
            raise GuardConformanceError(f"{label} is too large")
        final = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino, opened.st_mode) != (
            final.st_dev,
            final.st_ino,
            final.st_mode,
        ):
            raise GuardConformanceError(f"{label} changed while reading")
        return b"".join(chunks)
    except GuardConformanceError:
        raise
    except OSError as exc:
        raise GuardConformanceError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _validate_live_mounts(mountinfo: bytes, controllers_payload: bytes) -> bytes:
    try:
        controllers_text = controllers_payload.decode("ascii")
    except UnicodeDecodeError as exc:
        raise GuardConformanceError("live cgroup or bpffs prerequisite is invalid") from exc
    controllers = controllers_text.split()
    if (
        not controllers
        or len(controllers) != len(set(controllers))
        or any(re.fullmatch(r"[a-z][a-z0-9_]{0,63}", item) is None for item in controllers)
        or not {"cpu", "cpuset", "io", "memory", "pids"}.issubset(controllers)
    ):
        raise GuardConformanceError("live cgroup or bpffs prerequisite is unavailable")
    observed: dict[bytes, bytes] = {}
    lines = mountinfo.splitlines()
    if not lines or len(lines) > 1 << 16:
        raise GuardConformanceError("live cgroup or bpffs prerequisite is invalid")
    for line in lines:
        left, separator, right = line.partition(b" - ")
        left_fields = left.split()
        right_fields = right.split()
        if not separator or len(left_fields) < 6 or len(right_fields) < 3:
            raise GuardConformanceError("live cgroup or bpffs prerequisite is invalid")
        mountpoint = left_fields[4]
        if mountpoint in {b"/sys/fs/cgroup", b"/sys/fs/bpf"}:
            if mountpoint in observed:
                raise GuardConformanceError("live cgroup or bpffs prerequisite is invalid")
            observed[mountpoint] = right_fields[0]
    if observed != {
        b"/sys/fs/cgroup": b"cgroup2",
        b"/sys/fs/bpf": b"bpf",
    }:
        raise GuardConformanceError("live cgroup or bpffs prerequisite is unavailable")
    return _canonical(
        {
            "controllers": sorted(controllers),
            "mounts": {
                "/sys/fs/bpf": "bpf",
                "/sys/fs/cgroup": "cgroup2",
            },
        }
    )


def _validate_bpf_link_probe(error_number: int) -> None:
    if error_number != errno.EBADF:
        raise GuardConformanceError("live BPF link command is unavailable")


def _probe_bpf_link_create() -> bytes:
    syscall_number = {"x86_64": 321, "aarch64": 280}.get(platform.machine())
    if syscall_number is None:
        raise GuardConformanceError("live BPF link command is unavailable")
    attributes = ctypes.create_string_buffer(48)
    struct.pack_into("=IIII", attributes, 0, 0xFFFFFFFF, 0xFFFFFFFF, 0, 0)
    libc = ctypes.CDLL(None, use_errno=True)
    libc.syscall.restype = ctypes.c_long
    result = int(
        libc.syscall(
            ctypes.c_long(syscall_number),
            ctypes.c_uint(_BPF_LINK_CREATE),
            ctypes.c_void_p(ctypes.addressof(attributes)),
            ctypes.c_uint(len(attributes)),
        )
    )
    if result >= 0:
        os.close(result)
        raise GuardConformanceError("live BPF link probe unexpectedly succeeded")
    observed_errno = ctypes.get_errno()
    _validate_bpf_link_probe(observed_errno)
    return _canonical(
        {
            "bpf_command": _BPF_LINK_CREATE,
            "errno": observed_errno,
            "machine": platform.machine(),
        }
    )


def _validate_pinned_link_probe(attachment: BpfAttachment) -> None:
    if (
        len(attachment.link_ids) != len(ATTACHMENTS) * 3
        or len(attachment.program_ids) != len(ATTACHMENTS) * 3
        or len(attachment.map_ids) != 6 * 3
        or any(item <= 0 for item in attachment.link_ids)
        or any(item <= 0 for item in attachment.program_ids)
        or any(item <= 0 for item in attachment.map_ids)
        or len(set(attachment.link_ids)) != len(attachment.link_ids)
        or len(set(attachment.program_ids)) != len(attachment.program_ids)
        or len(set(attachment.map_ids)) != len(attachment.map_ids)
    ):
        raise GuardConformanceError("live pinned BPF link probe is incomplete")


def _remove_probe_tree(path: Path) -> None:
    try:
        path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise GuardConformanceError("live pinned BPF link cleanup failed") from exc
    visited = 0

    def remove(current: Path) -> None:
        nonlocal visited
        try:
            metadata = current.lstat()
            if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
                raise GuardConformanceError("live pinned BPF link cleanup is ambiguous")
            entries = tuple(sorted(current.iterdir(), key=lambda item: item.name))
        except GuardConformanceError:
            raise
        except OSError as exc:
            raise GuardConformanceError("live pinned BPF link cleanup failed") from exc
        visited += len(entries)
        if visited > 1024:
            raise GuardConformanceError("live pinned BPF link cleanup is ambiguous")
        for entry in entries:
            try:
                opened = entry.lstat()
                if stat.S_ISLNK(opened.st_mode):
                    raise GuardConformanceError(
                        "live pinned BPF link cleanup is ambiguous"
                    )
                if stat.S_ISDIR(opened.st_mode):
                    remove(entry)
                    entry.rmdir()
                else:
                    entry.unlink()
            except GuardConformanceError:
                raise
            except OSError as exc:
                raise GuardConformanceError(
                    "live pinned BPF link cleanup failed"
                ) from exc

    remove(path)


def _probe_network_policy(release: VerifiedGuardRelease) -> NetworkPolicy:
    members = {name: payload for name, _mode, payload in release.members}
    trusted_endpoint = Endpoint("192.0.2.1", 443, "tcp")
    build_endpoint = Endpoint("198.51.100.1", 443, "tcp")
    limits = TrafficLimits(1, 1, 1, 1, 1, 1, 1)
    return NetworkPolicy(
        containment_policy_sha256=_sha(b"live-bpf-probe-policy-v1"),
        resource_profile_sha256=_sha(b"live-bpf-probe-resources-v1"),
        bpf_program_sha256=_sha(members["guard-network-v1.bpf.o"]),
        bpf_map_schema_sha256=_sha(members["guard-network-map-schema-v1.json"]),
        scopes=(
            ScopeNetworkPolicy(
                "root",
                (trusted_endpoint, build_endpoint),
                (),
                limits,
            ),
            ScopeNetworkPolicy("trusted-service", (trusted_endpoint,), (), limits),
            ScopeNetworkPolicy("build-egress", (build_endpoint,), (), limits),
        ),
    )


def _probe_pinned_bpf_links(release: VerifiedGuardRelease) -> bytes:
    suffix = uuid4().hex
    cgroup_root = Path("/sys/fs/cgroup") / f".loom-guard-conformance-{suffix}"
    bpffs_root = Path("/sys/fs/bpf") / f".loom-guard-conformance-{suffix}"
    trusted = cgroup_root / "trusted-service"
    build = cgroup_root / "build-egress"
    cgroup_descriptors: list[int] = []
    created_cgroups: list[Path] = []
    created_bpffs = False
    error: BaseException | None = None
    try:
        cgroup_root.mkdir(mode=0o700)
        created_cgroups.append(cgroup_root)
        trusted.mkdir(mode=0o700)
        created_cgroups.append(trusted)
        build.mkdir(mode=0o700)
        created_cgroups.append(build)
        bpffs_root.mkdir(mode=0o700)
        created_bpffs = True
        targets: list[BpfScopeTarget] = []
        for name, path in (
            ("root", cgroup_root),
            ("trusted-service", trusted),
            ("build-egress", build),
        ):
            descriptor = os.open(
                path,
                os.O_RDONLY
                | os.O_CLOEXEC
                | os.O_DIRECTORY
                | getattr(os, "O_NOFOLLOW", 0),
            )
            cgroup_descriptors.append(descriptor)
            metadata = os.fstat(descriptor)
            if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) != 0o700:
                raise GuardConformanceError("live scratch cgroup identity is invalid")
            targets.append(
                BpfScopeTarget(
                    cast(Literal["root", "trusted-service", "build-egress"], name),
                    descriptor,
                    metadata.st_ino,
                )
            )

        class ProbeTree:
            def bpf_scope_targets(self) -> tuple[BpfScopeTarget, ...]:
                return tuple(targets)

        members = {name: payload for name, _mode, payload in release.members}
        bpftool = release.directory / "bpftool"
        policy = _probe_network_policy(release)
        loader = BpfLoader(
            kernel=BpfSyscall(),
            runner=PinnedCommandRunner(trusted_uid=0, timeout_seconds=30),
            bpftool=CommandIdentity(bpftool, _sha(members["bpftool"])),
            bpf_object_path=release.directory / "guard-network-v1.bpf.o",
            bpffs_root=bpffs_root,
            containment_policy_sha256=policy.containment_policy_sha256,
            resource_profile_sha256=policy.resource_profile_sha256,
            bpf_map_schema_sha256=policy.bpf_map_schema_sha256,
            trusted_uid=0,
            staging_suffix=lambda: "probe",
        )
        attachment = loader.attach(ProbeTree(), policy, _PROBE_GRANT)
        _validate_pinned_link_probe(attachment)
        return _canonical(
            {
                "attach_types": [item[1] for item in ATTACHMENTS],
                "links": len(attachment.link_ids),
                "maps": len(attachment.map_ids),
                "programs": len(attachment.program_ids),
                "scopes": ["root", "trusted-service", "build-egress"],
            }
        )
    except (GuardError, OSError) as exc:
        error = exc
        raise GuardConformanceError("live pinned BPF link probe failed") from exc
    except GuardConformanceError as exc:
        error = exc
        raise
    finally:
        cleanup_errors: list[BaseException] = []
        if created_bpffs:
            try:
                _remove_probe_tree(bpffs_root)
            except (GuardConformanceError, OSError) as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        for descriptor in reversed(cgroup_descriptors):
            try:
                os.close(descriptor)
            except OSError as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        for path in reversed(created_cgroups):
            try:
                processes = path / "cgroup.procs"
                if processes.exists() and _read_virtual_path(
                    processes,
                    maximum=64 * 1024,
                    label="live scratch cgroup process inventory",
                ).strip():
                    raise GuardConformanceError("live scratch cgroup is not empty")
                path.rmdir()
            except (GuardConformanceError, OSError) as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if created_bpffs:
            try:
                bpffs_root.rmdir()
            except OSError as cleanup_exc:
                cleanup_errors.append(cleanup_exc)
        if cleanup_errors:
            if error is not None:
                raise GuardConformanceError(
                    "live pinned BPF link probe and cleanup failed"
                ) from BaseExceptionGroup(
                    "live pinned BPF link probe and cleanup failures",
                    [error, *cleanup_errors],
                )
            raise GuardConformanceError(
                "live pinned BPF link cleanup failed"
            ) from BaseExceptionGroup(
                "live pinned BPF link cleanup failures",
                cleanup_errors,
            )


def _read_regular_path(path: Path, *, maximum: int, label: str) -> bytes:
    descriptor = -1
    try:
        initial = path.lstat()
        if (
            stat.S_ISLNK(initial.st_mode)
            or not stat.S_ISREG(initial.st_mode)
            or initial.st_nlink != 1
            or initial.st_size <= 0
            or initial.st_size > maximum
            or initial.st_mode & 0o002
        ):
            raise GuardConformanceError(f"{label} is unsafe")
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        identity = (
            initial.st_dev,
            initial.st_ino,
            initial.st_size,
            initial.st_mtime_ns,
            initial.st_ctime_ns,
        )
        if identity != (
            opened.st_dev,
            opened.st_ino,
            opened.st_size,
            opened.st_mtime_ns,
            opened.st_ctime_ns,
        ):
            raise GuardConformanceError(f"{label} changed while opening")
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        final = os.fstat(descriptor)
        final_path = path.lstat()
        if len(payload) != initial.st_size or len(payload) > maximum or identity != (
            final.st_dev,
            final.st_ino,
            final.st_size,
            final.st_mtime_ns,
            final.st_ctime_ns,
        ) or identity != (
            final_path.st_dev,
            final_path.st_ino,
            final_path.st_size,
            final_path.st_mtime_ns,
            final_path.st_ctime_ns,
        ):
            raise GuardConformanceError(f"{label} changed while reading")
        return payload
    except GuardConformanceError:
        raise
    except OSError as exc:
        raise GuardConformanceError(f"{label} is unavailable") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_source(root: Path, relative: Path, *, maximum: int = 1024 * 1024) -> bytes:
    if not root.is_absolute() or not relative.parts or ".." in relative.parts:
        raise GuardConformanceError("reviewed source path is invalid")
    return _read_regular_path(
        root / relative,
        maximum=maximum,
        label="reviewed source artifact",
    )


def validate_unit(payload: bytes) -> str:
    """Reject any unit template that could activate or broaden the guard."""

    try:
        return validate_release_unit(payload)
    except GuardReleaseError as exc:
        raise GuardConformanceError(str(exc)) from exc


def _exists(path: Path) -> bool:
    try:
        path.lstat()
        return True
    except FileNotFoundError:
        return False
    except OSError as exc:
        raise GuardConformanceError("inert path could not be inspected") from exc


def _verify_systemd_inert(root: Path) -> None:
    inspected = 0

    def walk_error(error: OSError) -> None:
        raise GuardConformanceError("systemd unit inventory is ambiguous") from error

    for relative in _SYSTEMD_ROOTS:
        directory = root / relative
        try:
            metadata = directory.lstat()
        except FileNotFoundError:
            continue
        except OSError as exc:
            raise GuardConformanceError("systemd unit inventory is ambiguous") from exc
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise GuardConformanceError("systemd unit inventory is ambiguous")
        for current, directories, files in os.walk(
            directory,
            topdown=True,
            onerror=walk_error,
            followlinks=False,
        ):
            entries = tuple(sorted((*directories, *files)))
            inspected += len(entries)
            if inspected > _MAX_SYSTEMD_ENTRIES:
                raise GuardConformanceError("systemd unit inventory is ambiguous")
            for name in entries:
                path = Path(current) / name
                if name == _UNIT_NAME:
                    raise GuardConformanceError("guard is not inert")
                if path.is_symlink():
                    try:
                        target = os.readlink(path)
                    except OSError as exc:
                        raise GuardConformanceError(
                            "systemd unit inventory is ambiguous"
                        ) from exc
                    if name in directories:
                        raise GuardConformanceError("systemd unit inventory is ambiguous")
                    if Path(target).name == _UNIT_NAME:
                        raise GuardConformanceError("guard is not inert")


def _verify_processes_inert(root: Path) -> None:
    proc = root / "proc"
    try:
        metadata = proc.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise GuardConformanceError("process inventory is ambiguous") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise GuardConformanceError("process inventory is ambiguous")
    inspected = 0
    try:
        candidates = sorted(proc.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise GuardConformanceError("process inventory is ambiguous") from exc
    for process in candidates:
        if not process.name.isascii() or not process.name.isdigit():
            continue
        inspected += 1
        if inspected > _MAX_PROCESSES:
            raise GuardConformanceError("process inventory is ambiguous")
        cmdline = process / "cmdline"
        descriptor = -1
        try:
            opened = cmdline.lstat()
            if stat.S_ISLNK(opened.st_mode) or not stat.S_ISREG(opened.st_mode):
                raise GuardConformanceError("process inventory is ambiguous")
            descriptor = os.open(
                cmdline,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
            )
            payload = os.read(descriptor, _MAX_CMDLINE_BYTES + 1)
            if len(payload) > _MAX_CMDLINE_BYTES:
                raise GuardConformanceError("process inventory is ambiguous")
        except FileNotFoundError:
            continue
        except GuardConformanceError:
            raise
        except OSError as exc:
            raise GuardConformanceError("process inventory is ambiguous") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        arguments = tuple(item for item in payload.split(b"\0") if item)
        if any(_is_guard_archive_argument(item) for item in arguments):
            raise GuardConformanceError("guard is not inert")


def _verify_inert_paths(root: Path) -> str:
    forbidden = (
        Path("etc/loom/task-image-builder-guard/config-v1.json"),
        Path("etc/loom/task-image-builder-guard/activation-v1.json"),
        Path("etc/systemd/system/loom-task-image-builder-node-guard.service"),
        Path("opt/loom-task-image-builder-guard/current"),
        Path("run/loom-task-image-builder-guard/guard.sock"),
        Path("sys/fs/bpf/loom-task-image-builder"),
    )
    if any(_exists(root / relative) for relative in forbidden):
        raise GuardConformanceError("guard is not inert")
    _verify_systemd_inert(root)
    _verify_processes_inert(root)
    releases = root / _RELEASE_PREFIX
    if releases.is_dir():
        entries = list(releases.iterdir())
        if len(entries) > 1024 or any(
            path.name.startswith(".stage-") or ".conflict." in path.name
            for path in entries
        ):
            raise GuardConformanceError("guard release staging is not inert")
    return _sha(
        _canonical(
            [
                *[path.as_posix() for path in forbidden],
                *[path.as_posix() for path in _SYSTEMD_ROOTS],
                "proc/<pid>/cmdline",
                _UNIT_NAME,
            ]
        )
    )


def _verify_provider(source_root: Path) -> tuple[str, str]:
    try:
        prerequisite_payload = _read_source(source_root, _PREREQUISITES)
        provider_payload = _read_source(source_root, _PROVIDERS)
        prerequisites = tomllib.loads(prerequisite_payload.decode("utf-8"))
        providers = tomllib.loads(provider_payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise GuardConformanceError("provider policy is invalid") from exc
    policies = providers.get("policies")
    if (
        _sha(prerequisite_payload) != _PREREQUISITES_SHA256
        or _sha(provider_payload) != _PROVIDERS_SHA256
        or providers.get("schema") != "loom.task-image-rootless-provider-policies/v1"
        or not isinstance(policies, list)
        or len(policies) != 2
        or not all(isinstance(item, dict) for item in policies)
        or {(item.get("slurm_cluster_id"), item.get("cpu_arch")) for item in policies}
        != {("oldlab", "x86_64"), ("gb10", "arm64")}
        or any(item.get("enabled") is not False for item in policies)
        or prerequisites.get("production_certification_allowed") is not False
        or prerequisites.get("certified_nodes") != []
        or prerequisites.get("unconditional_blockers") != [_BLOCKER]
    ):
        raise GuardConformanceError("provider policy inertness is invalid")
    legacy = prerequisites.get("legacy_guard")
    if legacy != {
        "qos": "loom-task-image-builder",
        "reservation": "loom-task-image-builder",
        "account": "loom-staging",
        "user": "loom-rollout",
        "max_jobs_per_user": 1,
        "max_submit_jobs_per_user": 1,
        "max_wall": "04:00:00",
    }:
        raise GuardConformanceError("Phase 1 rollback authority changed")
    return _sha(provider_payload + prerequisite_payload), _sha(_canonical(legacy))


def _verify_authority(source_root: Path) -> str:
    payload = _read_source(source_root, _AUTHORITY)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        raise GuardConformanceError("authority inertness is invalid") from None
    if (
        _sha(payload) != _AUTHORITY_SHA256
        or text.count("  replicas: 0") != 1
        or re.search(r"^  replicas: (?!0$)", text, flags=re.MULTILINE) is not None
        or text.count("  ingress: []") != 1
        or text.count("  egress: []") != 1
        or "loom.qianyi.dev/activation: disabled-phase2b1" not in text
    ):
        raise GuardConformanceError("authority inertness is invalid")
    return _sha(payload)


def _release_from_path(path: Path, *, live: bool) -> VerifiedGuardRelease:
    manifest_path = path / "release-manifest.json"
    try:
        raw = json.loads(
            _read_regular_path(
                manifest_path,
                maximum=1024 * 1024,
                label="guard release manifest",
            )
        )
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GuardConformanceError("guard release manifest is unavailable") from exc
    architecture = raw.get("architecture") if isinstance(raw, dict) else None
    if architecture not in {"x86_64", "aarch64"}:
        raise GuardConformanceError("guard release architecture is invalid")
    try:
        return verify_release_directory(
            path,
            expected_release_sha256=path.name,
            expected_architecture=cast(Architecture, architecture),
            expected_uid=0 if live else os.geteuid(),
        )
    except GuardReleaseError as exc:
        raise GuardConformanceError("guard release verification failed") from exc


def _verify_receipt(root: Path, release: VerifiedGuardRelease) -> str:
    path = (
        root
        / "var/lib/loom-task-image-builder-guard/staged"
        / f"{release.release_sha256}.json"
    )
    try:
        metadata = path.lstat()
        payload = _read_regular_path(path, maximum=64 * 1024, label="stage receipt")
        value = json.loads(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GuardConformanceError("stage receipt is unavailable") from exc
    expected = {
        "activated": False,
        "architecture": release.architecture,
        "installed_path": (
            f"/opt/loom-task-image-builder-guard/releases/{release.release_sha256}"
        ),
        "manifest_sha256": _sha(release.manifest_payload),
        "production_ready": False,
        "release_sha256": release.release_sha256,
        "schema": "loom.task-image-builder-guard-stage-receipt/v1",
    }
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_uid != (0 if root == Path("/") else os.geteuid())
        or payload != _canonical(value)
        or value != expected
    ):
        raise GuardConformanceError("stage receipt is invalid")
    return _sha(payload)


def _self_check(release: VerifiedGuardRelease) -> str:
    archive = release.directory / "loom-task-image-builder-guard.pyz"
    try:
        completed = subprocess.run(
            ("/usr/bin/python3", "-I", "-B", str(archive), "--self-check"),
            cwd="/",
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardConformanceError("guard zipapp self-check failed") from exc
    if completed.returncode != 0 or completed.stdout != _SELF_CHECK or completed.stderr:
        raise GuardConformanceError("guard zipapp self-check failed")
    return _sha(completed.stdout)


def _live_kernel_checks(release: VerifiedGuardRelease) -> tuple[ConformanceCheck, ...]:
    if os.geteuid() != 0:
        raise GuardConformanceError("live conformance requires root authority")
    controllers = Path("/sys/fs/cgroup/cgroup.controllers")
    bpffs = Path("/sys/fs/bpf")
    cgroup = Path("/sys/fs/cgroup")
    if not controllers.is_file() or not bpffs.is_dir() or not cgroup.is_dir():
        raise GuardConformanceError("live cgroup or bpffs prerequisite is unavailable")
    mount_evidence = _validate_live_mounts(
        _read_virtual_path(
            Path("/proc/self/mountinfo"),
            maximum=2 * 1024 * 1024,
            label="live mount inventory",
        ),
        _read_virtual_path(
            controllers,
            maximum=64 * 1024,
            label="live cgroup controllers",
        ),
    )
    link_evidence = _probe_bpf_link_create()
    descriptors: list[int] = []
    try:
        descriptors.append(os.pidfd_open(os.getpid(), 0))
        memfd = os.memfd_create("loom-guard-conformance", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
        descriptors.append(memfd)
        os.write(memfd, b"probe")
        seals = fcntl.F_SEAL_SEAL | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_GROW | fcntl.F_SEAL_WRITE
        fcntl.fcntl(memfd, fcntl.F_ADD_SEALS, seals)
        if fcntl.fcntl(memfd, fcntl.F_GET_SEALS) != seals:
            raise GuardConformanceError("live sealed memfd prerequisite failed")
    except OSError as exc:
        raise GuardConformanceError("live kernel primitive prerequisite failed") from exc
    finally:
        for descriptor in descriptors:
            os.close(descriptor)
    bpftool = release.directory / "bpftool"
    try:
        completed = subprocess.run(
            (str(bpftool), "-j", "feature", "probe", "kernel"),
            cwd="/",
            env={"LANG": "C", "LC_ALL": "C", "PATH": "/usr/bin:/bin"},
            stdin=subprocess.DEVNULL,
            capture_output=True,
            check=False,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise GuardConformanceError("staged bpftool feature probe failed") from exc
    if (
        completed.returncode != 0
        or not completed.stdout
        or len(completed.stdout) > 1024 * 1024
        or completed.stderr
    ):
        raise GuardConformanceError("staged bpftool feature probe failed")
    _validate_bpftool_features(completed.stdout)
    pinned_link_evidence = _probe_pinned_bpf_links(release)
    primitive_evidence = (
        mount_evidence
        + b"\0"
        + link_evidence
        + b"\0"
        + pinned_link_evidence
        + b"\0"
        + str(bpffs.stat().st_dev).encode("ascii")
    )
    return (
        ConformanceCheck("live_kernel_primitives", "pass", _sha(primitive_evidence)),
        ConformanceCheck("staged_bpftool_features", "pass", _sha(completed.stdout)),
    )


def conform(
    staged_release: Path,
    *,
    live: bool = False,
    root: Path = Path("/"),
    source_root: Path = ROOT,
) -> ConformanceReport:
    """Return bounded public evidence only after every inert guard check passes."""

    if not root.is_absolute() or not source_root.is_absolute():
        raise GuardConformanceError("conformance roots must be absolute")
    expected_release = root / _RELEASE_PREFIX / staged_release.name
    if staged_release != expected_release:
        raise GuardConformanceError("staged release path is invalid")
    release = _release_from_path(staged_release, live=live)
    spec_payload = _read_source(source_root, _SPEC)
    if release.manifest.get("release_spec_sha256") != _sha(spec_payload):
        raise GuardConformanceError("guard release differs from reviewed specification")
    member_map = {name: payload for name, _mode, payload in release.members}
    unit_payload = member_map["loom-task-image-builder-node-guard.service"]
    provider_digest, phase1_digest = _verify_provider(source_root)
    checks = [
        ConformanceCheck("authority_inert", "pass", _verify_authority(source_root)),
        ConformanceCheck(
            "bpf_artifacts",
            "pass",
            _sha(
                b"".join(
                    payload
                    for name, _mode, payload in release.members
                    if name.startswith("guard-network-")
                )
            ),
        ),
        ConformanceCheck("guard_release", "pass", _sha(release.manifest_payload)),
        ConformanceCheck("inert_runtime", "pass", _verify_inert_paths(root)),
        ConformanceCheck("phase1_rollback", "pass", phase1_digest),
        ConformanceCheck("provider_policy", "pass", provider_digest),
        ConformanceCheck("stage_receipt", "pass", _verify_receipt(root, release)),
        ConformanceCheck("systemd_unit", "pass", validate_unit(unit_payload)),
        ConformanceCheck("zipapp_self_check", "pass", _self_check(release)),
    ]
    if live:
        if root != Path("/"):
            raise GuardConformanceError("live conformance must inspect the real root")
        checks.extend(_live_kernel_checks(release))
    checks.sort(key=lambda item: item.id)
    return ConformanceReport(
        release_sha256=release.release_sha256,
        architecture=release.architecture,
        live=live,
        checks=tuple(checks),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--staged-release", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, default=ROOT)
    parser.add_argument("--root", type=Path, default=Path("/"))
    parser.add_argument("--live", action="store_true")
    arguments = parser.parse_args(argv)
    try:
        report = conform(
            arguments.staged_release,
            live=arguments.live,
            root=arguments.root,
            source_root=arguments.source_root,
        )
    except GuardConformanceError as exc:
        parser.error(str(exc))
    print(_canonical(report.as_dict()).decode("ascii"), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "ConformanceCheck",
    "ConformanceReport",
    "GuardConformanceError",
    "conform",
    "main",
    "validate_unit",
]
