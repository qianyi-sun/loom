"""Local projection mediation and monotonic guard attestation service."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import socket
import stat
import threading
import time
from collections import deque
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import pairwise
from pathlib import Path, PurePosixPath
from typing import ClassVar, Literal, Protocol, cast
from uuid import UUID, uuid4

from loom_task_image_builder_guard.authority import (
    AcceptedAttestation,
    BuildSession,
    ProjectionChallenge,
    ProjectionReceipt,
)
from loom_task_image_builder_guard.bpf import (
    ATTACHMENTS,
    BpfOperations,
    NetworkPolicy,
    static_map_items,
)
from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.identity import (
    BatchCgroup,
    PeerHandle,
    PeerInspector,
    derive_batch_cgroup,
    projection_request,
)
from loom_task_image_builder_guard.ledger import GuardLedger, LedgerEntry
from loom_task_image_builder_guard.models import GuardConfigValue
from loom_task_image_builder_guard.protocol import (
    LOCAL_SCHEMA,
    PeerCredentials,
    create_sealed_memfd,
    parse_local_request,
    read_sealed_memfd,
    receive_authenticated_packet,
    require_ack,
    send_packet,
)

_BOOTSTRAP = re.compile(r"^loom_tibp_[A-Za-z0-9_-]{64,128}$")
_CPU_COMPONENT = re.compile(r"^([0-9]+)(?:-([0-9]+))?$")
_MAX_SECRET_BYTES = 64 * 1024
_MAX_CGROUP_CONTROL_BYTES = 64 * 1024


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _json(value: object) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=True,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    except (TypeError, ValueError, UnicodeEncodeError):
        raise GuardError("service_document_invalid") from None


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise GuardError("service_clock_invalid")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _uuid(value: object, *, code: str) -> UUID:
    try:
        if not isinstance(value, str):
            raise ValueError
        parsed = UUID(value)
    except (ValueError, AttributeError):
        raise GuardError(code) from None
    if parsed.int == 0 or str(parsed) != value:
        raise GuardError(code)
    return parsed


class Peer(Protocol):
    pid: int
    uid: int
    gid: int
    job_id: str
    executable_sha256: str
    batch_cgroup_relative: object
    cgroup_relative: object

    def assert_unchanged(self) -> None: ...

    def containment_hold(self) -> AbstractContextManager[None]: ...

    def close(self) -> None: ...


class PeerSource(Protocol):
    def capture(
        self,
        connection: socket.socket,
        message_credentials: PeerCredentials,
    ) -> Peer: ...


class SlurmSource(Protocol):
    def observe(self, *, job_id: str, grant_id: UUID) -> object: ...

    def quarantine_capability(self) -> None: ...


class Batch(Protocol):
    authority_path: str
    inode: int


class Attachment(Protocol):
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

    def close(self) -> None: ...


class Containment(Protocol):
    def prepare(
        self,
        batch: Batch,
        peer: Peer,
        policy: object,
        grant_id: UUID,
    ) -> Attachment: ...


class Authority(Protocol):
    @staticmethod
    def request_sha256(value: object) -> str: ...

    def challenge(
        self,
        grant_id: UUID,
        request: dict[str, object],
        *,
        containment_policy_sha256: str,
        resource_profile_sha256: str,
    ) -> ProjectionChallenge: ...

    def attach(self, grant_id: UUID, proof: dict[str, object]) -> ProjectionReceipt: ...

    def exchange(self, grant_id: UUID, request: dict[str, object]) -> BuildSession: ...

    def attest(
        self,
        grant_id: UUID,
        generation: int,
        attestation: dict[str, object],
    ) -> AcceptedAttestation: ...

    def revoke(self, grant_id: UUID, request: dict[str, object]) -> None: ...


class Reconciler(Protocol):
    def reconcile(self, ledger: GuardLedger) -> None: ...

    def assert_live(self, entry: LedgerEntry) -> None: ...

    def recover_live(self) -> tuple[tuple[LedgerEntry, Peer], ...]: ...


class ReconciliationProbe(Protocol):
    def classify(self, entry: LedgerEntry) -> str: ...

    def assert_live(self, entry: LedgerEntry) -> None: ...

    def open_live(self, entry: LedgerEntry) -> Peer: ...

    def cleanup_terminal(self, entry: LedgerEntry) -> None: ...


class DeviceProgramEvidence(Protocol):
    @property
    def program_id(self) -> int: ...

    @property
    def tag(self) -> str: ...

    @property
    def attach_type(self) -> str: ...

    @property
    def attach_flags(self) -> str: ...

    @property
    def name(self) -> str: ...


class DeviceProbeSource(Protocol):
    def inspect_path(self, path: Path) -> tuple[DeviceProgramEvidence, ...]: ...


class NodeReconciler:
    """Correlate durable entries and pins before the socket becomes reachable."""

    def __init__(
        self,
        pin_root: Path,
        *,
        probe: ReconciliationProbe,
        slurm: SlurmSource,
        trusted_uid: int = 0,
        trusted_gid: int = 0,
        progress: Callable[[], None] = lambda: None,
    ) -> None:
        self.pin_root = pin_root
        self.probe = probe
        self.slurm = slurm
        self.trusted_uid = trusted_uid
        self.trusted_gid = trusted_gid
        self._progress = progress
        self._recoverable: tuple[LedgerEntry, ...] = ()

    def _pin_inventory(self) -> tuple[set[UUID], set[UUID], bool]:
        try:
            metadata = os.lstat(self.pin_root)
            if (
                self.pin_root.resolve(strict=True) != self.pin_root
                or not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != self.trusted_uid
                or metadata.st_gid != self.trusted_gid
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise GuardError("reconciliation_pin_root_invalid")
            published: set[UUID] = set()
            staging: set[UUID] = set()
            unknown = False
            for path in sorted(self.pin_root.iterdir(), key=lambda value: value.name):
                self._progress()
                opened = os.lstat(path)
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or stat.S_ISLNK(opened.st_mode)
                    or opened.st_uid != self.trusted_uid
                    or opened.st_gid != self.trusted_gid
                    or stat.S_IMODE(opened.st_mode) != 0o700
                ):
                    unknown = True
                    continue
                try:
                    grant_id = UUID(path.name)
                except ValueError:
                    matched = re.fullmatch(
                        r"staging-([0-9a-f-]{36})-[a-z0-9]{1,32}", path.name
                    )
                    if matched is None:
                        unknown = True
                        continue
                    try:
                        grant_id = UUID(matched.group(1))
                    except ValueError:
                        unknown = True
                        continue
                    if str(grant_id) not in path.name or grant_id.int == 0:
                        unknown = True
                        continue
                    staging.add(grant_id)
                else:
                    if grant_id.int == 0 or str(grant_id) != path.name:
                        unknown = True
                    else:
                        published.add(grant_id)
            return published, staging, unknown
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("reconciliation_pin_root_invalid") from exc

    def reconcile(self, ledger: GuardLedger) -> None:
        recoverable: list[LedgerEntry] = []
        withdrawal_attempted = False
        try:
            entries = ledger.load_all()
            self._progress()
            published, staging, ambiguous = self._pin_inventory()
            self._progress()
            by_grant = {entry.grant_id: entry for entry in entries}
            expected: set[UUID] = set()
            for entry in entries:
                self._progress()
                document = entry.document()
                if entry.state == "quarantined":
                    if document["pin_path"] is not None:
                        expected.add(entry.grant_id)
                    ambiguous = True
                    continue
                if entry.state == "containment_pending":
                    ambiguous = True
                    ledger.quarantine(
                        entry.grant_id,
                        reason="containment_state_ambiguous",
                    )
                    continue
                pin_path = document["pin_path"]
                if pin_path is not None:
                    expected.add(entry.grant_id)
                    if pin_path != str(self.pin_root / str(entry.grant_id)):
                        ambiguous = True
                        ledger.quarantine(
                            entry.grant_id, reason="pin_identity_ambiguous"
                        )
                        continue
                    if entry.grant_id not in published:
                        ambiguous = True
                        ledger.quarantine(
                            entry.grant_id, reason="pin_inventory_ambiguous"
                        )
                        continue
                elif entry.grant_id in published:
                    ambiguous = True
                    ledger.quarantine(
                        entry.grant_id,
                        reason="pin_inventory_ambiguous",
                    )
                    continue
                if entry.grant_id in staging:
                    ambiguous = True
                    ledger.quarantine(entry.grant_id, reason="pin_staging_ambiguous")
                    continue
                try:
                    classification = self.probe.classify(entry)
                except GuardError:
                    classification = "ambiguous"
                self._progress()
                if classification == "pre_containment_clean" and entry.state in {
                    "intent",
                    "challenge_pending",
                    "challenged",
                }:
                    continue
                if classification == "live_exact":
                    if entry.state in {"projected", "exchanged"}:
                        recoverable.append(entry)
                    elif entry.state != "attached":
                        ambiguous = True
                        ledger.quarantine(
                            entry.grant_id, reason="runtime_identity_ambiguous"
                        )
                    continue
                if classification == "terminal_empty" and entry.state == "terminal":
                    self.probe.cleanup_terminal(entry)
                    if entry.grant_id in published and (
                        self.pin_root / str(entry.grant_id)
                    ).exists():
                        raise GuardError("reconciliation_cleanup_ambiguous")
                    ledger.remove_terminal(entry.grant_id, allocation_empty=True)
                    continue
                ambiguous = True
                ledger.quarantine(entry.grant_id, reason="runtime_identity_ambiguous")
            if published - expected or staging - set(by_grant):
                ambiguous = True
            if ambiguous:
                withdrawal_attempted = True
                self.slurm.quarantine_capability()
        except GuardError:
            if not withdrawal_attempted:
                self.slurm.quarantine_capability()
            raise
        self._recoverable = tuple(recoverable)

    def assert_live(self, entry: LedgerEntry) -> None:
        self.probe.assert_live(entry)

    def recover_live(self) -> tuple[tuple[LedgerEntry, Peer], ...]:
        recovered: list[tuple[LedgerEntry, Peer]] = []
        try:
            for entry in self._recoverable:
                recovered.append((entry, self.probe.open_live(entry)))
                self._progress()
            return tuple(recovered)
        except GuardError:
            for _entry, peer in recovered:
                peer.close()
            self.slurm.quarantine_capability()
            raise


class SystemReconciliationProbe:
    """Re-establish exact process, Slurm, cgroup, and pinned-BPF identity."""

    _SCOPES: ClassVar[tuple[str, ...]] = ("root", "trusted-service", "build-egress")
    _MAP_LAYOUTS: ClassVar[dict[str, tuple[int, int, int, int]]] = {
        "scope_subject": (2, 4, 4, 1),
        "allow_v4": (1, 12, 1, 4096),
        "allow_v6": (1, 24, 1, 4096),
        "subject_limits": (1, 4, 120, 16),
        "flow_sockets": (1, 8, 8, 4096),
        "drop_counters": (6, 4, 8, 16),
    }
    _MAPS: ClassVar[frozenset[str]] = frozenset(_MAP_LAYOUTS)
    _SUBJECT_LIMIT_MUTABLE_PREFIX_BYTES: ClassVar[int] = 64

    def __init__(
        self,
        config: GuardConfigValue,
        *,
        peers: PeerInspector,
        slurm: SlurmSource,
        kernel: BpfOperations,
        device_probe: DeviceProbeSource,
        network_policy: NetworkPolicy,
        trusted_uid: int = 0,
        trusted_gid: int = 0,
    ) -> None:
        self.config = config
        self.peers = peers
        self.slurm = slurm
        self.kernel = kernel
        self.device_probe = device_probe
        self.network_policy = network_policy
        self.trusted_uid = trusted_uid
        self.trusted_gid = trusted_gid

    @staticmethod
    def _expected_ids(document: dict[str, object], name: str) -> tuple[int, ...]:
        value = document[name]
        if not isinstance(value, list) or any(type(item) is not int for item in value):
            raise GuardError("reconciliation_pin_identity_invalid")
        return cast(tuple[int, ...], tuple(value))

    def _attachment_cgroup_ids(self, document: dict[str, object]) -> dict[str, int]:
        proof = document.get("proof")
        if not isinstance(proof, dict):
            raise GuardError("reconciliation_pin_identity_invalid")
        attachment = proof.get("attachment")
        if not isinstance(attachment, dict):
            raise GuardError("reconciliation_pin_identity_invalid")
        expected_attachment_fields = {
            "schema_version",
            "cgroup_inode",
            "containment_root",
            "trusted_service_cgroup",
            "build_egress_cgroup",
            "bpf_program_sha256",
            "bpf_map_schema_sha256",
            "containment_policy_sha256",
            "resource_limits_sha256",
            "probe_sha256",
            "link_ids",
            "program_ids",
            "map_ids",
        }
        request = document.get("projection_request")
        if (
            set(attachment) != expected_attachment_fields
            or attachment.get("schema_version") != 1
            or not isinstance(request, dict)
            or type(request.get("cgroup_inode")) is not int
            or proof.get("cgroup_inode") != request.get("cgroup_inode")
            or attachment.get("cgroup_inode") != request.get("cgroup_inode")
            or any(
                attachment.get(name) != document.get(name)
                for name in ("link_ids", "program_ids", "map_ids")
            )
        ):
            raise GuardError("reconciliation_pin_identity_invalid")
        expected_digests = {
            "bpf_program_sha256": self.config.containment.bpf_program_sha256,
            "bpf_map_schema_sha256": self.config.containment.bpf_map_schema_sha256,
            "containment_policy_sha256": (
                self.config.containment.containment_policy_sha256
            ),
            "resource_limits_sha256": (
                self.config.containment.resource_profile_sha256
            ),
        }
        if any(attachment.get(name) != value for name, value in expected_digests.items()):
            raise GuardError("reconciliation_pin_identity_invalid")
        values = {
            "root": attachment.get("containment_root"),
            "trusted-service": attachment.get("trusted_service_cgroup"),
            "build-egress": attachment.get("build_egress_cgroup"),
        }
        if any(not isinstance(value, str) for value in values.values()):
            raise GuardError("reconciliation_pin_identity_invalid")
        paths = {name: Path(cast(str, value)) for name, value in values.items()}
        root = paths["root"]
        if (
            root.name != "loom-builder"
            or paths["trusted-service"] != root / "trusted-service"
            or paths["build-egress"] != root / "build-egress"
        ):
            raise GuardError("reconciliation_pin_identity_invalid")
        cgroup_root = self.config.containment.cgroup_root.resolve(strict=True)
        result: dict[str, int] = {}
        for name, path in paths.items():
            metadata = os.lstat(path)
            path.relative_to(cgroup_root)
            if path.resolve(strict=True) != path or not stat.S_ISDIR(metadata.st_mode):
                raise GuardError("reconciliation_pin_identity_invalid")
            result[name] = metadata.st_ino
        return result

    @staticmethod
    def _control_identity(metadata: os.stat_result) -> tuple[int, ...]:
        return (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_mode,
            metadata.st_uid,
            metadata.st_gid,
        )

    def _read_control(self, root: Path, name: str, *, allow_empty: bool = False) -> str:
        root_fd: int | None = None
        control_fd: int | None = None
        try:
            root_before = os.lstat(root)
            root_fd = os.open(
                root,
                os.O_RDONLY
                | os.O_CLOEXEC
                | os.O_DIRECTORY
                | getattr(os, "O_NOFOLLOW", 0),
            )
            if (
                not stat.S_ISDIR(root_before.st_mode)
                or self._control_identity(os.fstat(root_fd))
                != self._control_identity(root_before)
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            control_path = root / name
            control_before = os.lstat(control_path)
            control_fd = os.open(
                name,
                os.O_RDONLY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=root_fd,
            )
            if (
                not stat.S_ISREG(control_before.st_mode)
                or self._control_identity(os.fstat(control_fd))
                != self._control_identity(control_before)
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            chunks: list[bytes] = []
            remaining = _MAX_CGROUP_CONTROL_BYTES + 1
            while remaining:
                chunk = os.read(control_fd, min(16 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            if (
                (not payload and not allow_empty)
                or len(payload) > _MAX_CGROUP_CONTROL_BYTES
                or self._control_identity(os.fstat(control_fd))
                != self._control_identity(control_before)
                or self._control_identity(os.fstat(root_fd))
                != self._control_identity(root_before)
                or self._control_identity(os.lstat(root))
                != self._control_identity(root_before)
                or self._control_identity(os.lstat(control_path))
                != self._control_identity(control_before)
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            value = payload.decode("ascii")
            if "\x00" in value or "\r" in value:
                raise GuardError("reconciliation_resource_identity_invalid")
            return value
        except GuardError:
            raise
        except (OSError, UnicodeError) as exc:
            raise GuardError("reconciliation_resource_identity_invalid") from exc
        finally:
            if control_fd is not None:
                os.close(control_fd)
            if root_fd is not None:
                os.close(root_fd)

    def _control_metadata(self, root: Path, name: str) -> dict[str, object]:
        root_fd: int | None = None
        control_fd: int | None = None
        try:
            root_before = os.lstat(root)
            root_fd = os.open(
                root,
                os.O_RDONLY
                | os.O_CLOEXEC
                | os.O_DIRECTORY
                | getattr(os, "O_NOFOLLOW", 0),
            )
            if (
                not stat.S_ISDIR(root_before.st_mode)
                or self._control_identity(os.fstat(root_fd))
                != self._control_identity(root_before)
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            control_path = root / name
            control_before = os.lstat(control_path)
            control_fd = os.open(
                name,
                os.O_RDONLY
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=root_fd,
            )
            control_after = os.fstat(control_fd)
            if (
                not stat.S_ISREG(control_before.st_mode)
                or self._control_identity(control_after)
                != self._control_identity(control_before)
                or self._control_identity(os.fstat(root_fd))
                != self._control_identity(root_before)
                or self._control_identity(os.lstat(root))
                != self._control_identity(root_before)
                or self._control_identity(os.lstat(control_path))
                != self._control_identity(control_before)
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            return {
                "path": str(control_path),
                "uid": control_after.st_uid,
                "gid": control_after.st_gid,
                "mode": stat.S_IMODE(control_after.st_mode),
            }
        except GuardError:
            raise
        except OSError as exc:
            raise GuardError("reconciliation_resource_identity_invalid") from exc
        finally:
            if control_fd is not None:
                os.close(control_fd)
            if root_fd is not None:
                os.close(root_fd)

    @staticmethod
    def _cpu_count(value: str) -> int:
        ranges: list[tuple[int, int]] = []
        for component in value.strip().split(","):
            matched = _CPU_COMPONENT.fullmatch(component)
            if matched is None:
                raise GuardError("reconciliation_resource_identity_invalid")
            start = int(matched.group(1))
            end = start if matched.group(2) is None else int(matched.group(2))
            if start > end or end > (1 << 31) - 1:
                raise GuardError("reconciliation_resource_identity_invalid")
            ranges.append((start, end))
        ranges.sort()
        if any(
            current[0] <= previous[1]
            for previous, current in pairwise(ranges)
        ):
            raise GuardError("reconciliation_resource_identity_invalid")
        return sum(end - start + 1 for start, end in ranges)

    def _verify_runtime_controls(self, document: dict[str, object]) -> None:
        try:
            cgroup_ids = self._attachment_cgroup_ids(document)
            proof = cast(dict[str, object], document["proof"])
            attachment = cast(dict[str, object], proof["attachment"])
            root = Path(cast(str, attachment["containment_root"]))
            paths = {
                "batch": root.parent,
                "root": root,
                "trusted-service": Path(
                    cast(str, attachment["trusted_service_cgroup"])
                ),
                "build-egress": Path(cast(str, attachment["build_egress_cgroup"])),
            }
            request = document.get("projection_request")
            batch_metadata = os.lstat(paths["batch"])
            if (
                not isinstance(request, dict)
                or request.get("cgroup_path") != str(paths["batch"])
                or request.get("cgroup_inode") != batch_metadata.st_ino
                or cgroup_ids
                != {
                    name: os.lstat(paths[name]).st_ino
                    for name in self._SCOPES
                }
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            for name in self._SCOPES:
                if self._read_control(paths[name], "cgroup.type").strip() != "domain":
                    raise GuardError("reconciliation_resource_identity_invalid")
            available_controllers = tuple(
                sorted(
                    self._read_control(
                        paths["batch"], "cgroup.controllers"
                    ).split()
                )
            )
            if (
                len(available_controllers) != len(set(available_controllers))
                or not {"io", "pids"}.issubset(available_controllers)
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            for name, expected in (
                ("batch", ("io", "pids")),
                ("root", ("io", "pids")),
                ("trusted-service", ()),
                ("build-egress", ()),
            ):
                controllers = tuple(
                    sorted(
                        self._read_control(
                            paths[name],
                            "cgroup.subtree_control",
                            allow_empty=True,
                        ).split()
                    )
                )
                if controllers != expected:
                    raise GuardError("reconciliation_resource_identity_invalid")
            descendants: dict[str, int] = {}
            for name, probe_name, expected_descendants in (
                ("batch", "batch", 3),
                ("root", "root", 2),
                ("trusted-service", "trusted_service", 0),
                ("build-egress", "build_egress", 0),
            ):
                first = self._read_control(paths[name], "cgroup.stat")
                second = self._read_control(paths[name], "cgroup.stat")
                if first != second:
                    raise GuardError("reconciliation_resource_identity_invalid")
                values: dict[str, int] = {}
                for row in first.splitlines():
                    fields = row.split()
                    if len(fields) != 2 or fields[0] in values:
                        raise GuardError("reconciliation_resource_identity_invalid")
                    values[fields[0]] = int(fields[1])
                if (
                    values.get("nr_descendants") != expected_descendants
                    or values.get("nr_dying_descendants") != 0
                ):
                    raise GuardError("reconciliation_resource_identity_invalid")
                descendants[probe_name] = expected_descendants
            locked_process_files = (
                self._control_metadata(paths["batch"], "cgroup.procs"),
                self._control_metadata(paths["trusted-service"], "cgroup.procs"),
            )
            if any(
                item["uid"] != self.trusted_uid
                or item["gid"] != self.trusted_gid
                or item["mode"] != 0o644
                for item in locked_process_files
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            process_migration = {
                "common_ancestor": self._control_metadata(
                    paths["root"], "cgroup.procs"
                ),
                "destination": self._control_metadata(
                    paths["build-egress"], "cgroup.procs"
                ),
            }
            if any(
                item["uid"] != self.config.identity.uid
                or item["gid"] != self.config.identity.gid
                or item["mode"] != 0o644
                for item in process_migration.values()
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            cpu_max = self._read_control(paths["batch"], "cpu.max").split()
            if len(cpu_max) != 2:
                raise GuardError("reconciliation_resource_identity_invalid")
            quota, period = (int(value) for value in cpu_max)
            memory_max = int(
                self._read_control(paths["batch"], "memory.max").strip()
            )
            swap_max = int(
                self._read_control(paths["batch"], "memory.swap.max").strip()
            )
            cpu_count = self._cpu_count(
                self._read_control(paths["batch"], "cpuset.cpus.effective")
            )
            if (
                quota <= 0
                or period <= 0
                or quota != self.config.slurm.cpus * period
                or cpu_count != self.config.slurm.cpus
                or memory_max != self.config.slurm.memory_mib * 1024 * 1024
                or swap_max != 0
                or self._read_control(paths["root"], "pids.max").strip()
                != str(self.config.containment.pids_max)
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            expected_io = tuple(
                sorted(
                    f"{item.device} rbps={item.rbps} wbps={item.wbps} "
                    f"riops={item.riops} wiops={item.wiops}"
                    for item in self.config.containment.io_limits
                )
            )
            observed_io = tuple(
                sorted(
                    line.strip()
                    for line in self._read_control(paths["root"], "io.max").splitlines()
                    if line.strip()
                )
            )
            if observed_io != expected_io or len(observed_io) != len(set(observed_io)):
                raise GuardError("reconciliation_resource_identity_invalid")
            device_programs = self.device_probe.inspect_path(paths["batch"])
            if tuple(item.tag for item in device_programs) != (
                self.config.containment.device_program_tags
            ):
                raise GuardError("reconciliation_resource_identity_invalid")
            probe = {
                "schema": "loom.task-image-builder-guard-containment-probe/v1",
                "cgroups": {
                    "batch": {
                        "path": str(paths["batch"]),
                        "inode": batch_metadata.st_ino,
                    },
                    "root": {
                        "path": str(paths["root"]),
                        "inode": cgroup_ids["root"],
                    },
                    "trusted_service": {
                        "path": str(paths["trusted-service"]),
                        "inode": cgroup_ids["trusted-service"],
                    },
                    "build_egress": {
                        "path": str(paths["build-egress"]),
                        "inode": cgroup_ids["build-egress"],
                    },
                },
                "descendants": descendants,
                "inherited": {
                    "controllers": list(available_controllers),
                    "delegated": [],
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
                },
                "applied": {
                    "pids_max": self.config.containment.pids_max,
                    "io_max": list(expected_io),
                },
                "process_migration": process_migration,
                "bpf": {
                    "pin_path": document["pin_path"],
                    "link_ids": document["link_ids"],
                    "program_ids": document["program_ids"],
                    "map_ids": document["map_ids"],
                },
            }
            if attachment.get("probe_sha256") != hashlib.sha256(
                _json(probe)
            ).hexdigest():
                raise GuardError("reconciliation_resource_identity_invalid")
        except GuardError as exc:
            if exc.code == "reconciliation_resource_identity_invalid":
                raise
            raise GuardError("reconciliation_resource_identity_invalid") from None
        except (KeyError, OSError, TypeError, ValueError):
            raise GuardError("reconciliation_resource_identity_invalid") from None

    def _assert_pin_directory(self, path: Path) -> None:
        metadata = os.lstat(path)
        if (
            path.resolve(strict=True) != path
            or not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != self.trusted_uid
            or metadata.st_gid != self.trusted_gid
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise GuardError("reconciliation_pin_identity_invalid")

    @classmethod
    def _static_map_contents_match(
        cls,
        name: str,
        observed: tuple[tuple[bytes, bytes], ...],
        expected: tuple[tuple[bytes, bytes], ...],
        *,
        value_size: int,
    ) -> bool:
        if name != "subject_limits":
            return observed == expected
        if tuple(key for key, _value in observed) != tuple(
            key for key, _value in expected
        ):
            return False
        mutable = cls._SUBJECT_LIMIT_MUTABLE_PREFIX_BYTES
        return all(
            len(observed_value) == value_size
            and len(expected_value) == value_size
            and observed_value[mutable:] == expected_value[mutable:]
            for (_observed_key, observed_value), (_expected_key, expected_value) in zip(
                observed, expected, strict=True
            )
        )

    def _verify_pin_tree(self, entry: LedgerEntry) -> None:
        document = entry.document()
        raw_path = document["pin_path"]
        if raw_path is None:
            if any(document[name] for name in ("link_ids", "program_ids", "map_ids")):
                raise GuardError("reconciliation_pin_identity_invalid")
            return
        expected = self.config.containment.bpffs_root / str(entry.grant_id)
        if raw_path != str(expected):
            raise GuardError("reconciliation_pin_identity_invalid")
        try:
            self._assert_pin_directory(expected)
            if {item.name for item in expected.iterdir()} != set(self._SCOPES):
                raise GuardError("reconciliation_pin_identity_invalid")
            cgroup_ids = self._attachment_cgroup_ids(document)
            observed: dict[str, list[int]] = {"link": [], "program": [], "map": []}
            for scope in self._SCOPES:
                scope_policy = next(
                    item for item in self.network_policy.scopes if item.name == scope
                )
                expected_static_maps = static_map_items(scope_policy)
                scope_root = expected / scope
                self._assert_pin_directory(scope_root)
                roots = {
                    "program": scope_root / "progs",
                    "map": scope_root / "maps",
                    "link": scope_root / "links",
                }
                if {item.name for item in scope_root.iterdir()} != {
                    "progs",
                    "maps",
                    "links",
                }:
                    raise GuardError("reconciliation_pin_identity_invalid")
                program_ids: dict[str, int] = {}
                for kind, root in roots.items():
                    self._assert_pin_directory(root)
                    expected_names = (
                        {item[0] for item in ATTACHMENTS}
                        if kind != "map"
                        else self._MAPS
                    )
                    if {item.name for item in root.iterdir()} != expected_names:
                        raise GuardError("reconciliation_pin_identity_invalid")
                    for name in sorted(expected_names):
                        descriptor = self.kernel.obj_get(root / name)
                        try:
                            info = self.kernel.object_info(
                                descriptor,
                                cast(Literal["program", "map", "link"], kind),
                            )
                            attachment = next(
                                (
                                    item
                                    for item in ATTACHMENTS
                                    if item[0] == name
                                ),
                                None,
                            )
                            if info.kind != kind or info.object_id <= 0:
                                raise GuardError("reconciliation_pin_identity_invalid")
                            if kind == "program":
                                if attachment is None or (
                                    info.object_type,
                                    info.name,
                                ) != (attachment[2], name[:15]):
                                    raise GuardError(
                                        "reconciliation_pin_identity_invalid"
                                    )
                                program_ids[name] = info.object_id
                            elif kind == "map":
                                if (
                                    info.name != name[:15]
                                    or (
                                        info.object_type,
                                        info.key_size,
                                        info.value_size,
                                        info.max_entries,
                                    )
                                    != self._MAP_LAYOUTS[name]
                                ):
                                    raise GuardError(
                                        "reconciliation_pin_identity_invalid"
                                    )
                                if name in expected_static_maps:
                                    map_type, key_size, value_size, maximum = (
                                        self._MAP_LAYOUTS[name]
                                    )
                                    del map_type
                                    observed_items = self.kernel.map_items(
                                        descriptor,
                                        key_size=key_size,
                                        value_size=value_size,
                                        max_entries=maximum,
                                    )
                                    if not self._static_map_contents_match(
                                        name,
                                        observed_items,
                                        expected_static_maps[name],
                                        value_size=value_size,
                                    ):
                                        raise GuardError(
                                            "reconciliation_pin_identity_invalid"
                                        )
                            elif attachment is None or (
                                info.object_type,
                                info.program_id,
                                info.cgroup_id,
                                info.attach_type,
                            ) != (
                                3,
                                program_ids.get(name),
                                cgroup_ids[scope],
                                attachment[1],
                            ):
                                raise GuardError("reconciliation_pin_identity_invalid")
                            observed[kind].append(info.object_id)
                        finally:
                            os.close(descriptor)
            if (
                tuple(sorted(observed["link"])) != self._expected_ids(document, "link_ids")
                or tuple(sorted(observed["program"]))
                != self._expected_ids(document, "program_ids")
                or tuple(sorted(observed["map"])) != self._expected_ids(document, "map_ids")
            ):
                raise GuardError("reconciliation_pin_identity_invalid")
        except GuardError:
            raise
        except (OSError, ValueError) as exc:
            raise GuardError("reconciliation_pin_identity_invalid") from exc

    def _live_peer(self, entry: LedgerEntry) -> PeerHandle:
        document = entry.document()
        peer = self.peers.capture_pid(
            entry.peer_pid,
            expected_uid=self.config.identity.uid,
            expected_gid=self.config.identity.gid,
        )
        complete = False
        try:
            expected_relative = document["batch_cgroup_relative"]
            if (
                peer.job_id != entry.job_id
                or peer.executable_sha256 != document["peer_executable_sha256"]
                or str(peer.batch_cgroup_relative) != expected_relative
            ):
                raise GuardError("reconciliation_peer_identity_invalid")
            trusted = peer.batch_cgroup_relative / "loom-builder" / "trusted-service"
            if entry.state in {"attached", "projected", "exchanged"}:
                if peer.cgroup_relative != trusted:
                    raise GuardError("reconciliation_peer_identity_invalid")
            elif peer.cgroup_relative != peer.batch_cgroup_relative:
                raise GuardError("reconciliation_peer_identity_invalid")
            peer.assert_unchanged()
            self.slurm.observe(job_id=entry.job_id, grant_id=entry.grant_id)
            batch = derive_batch_cgroup(
                peer,
                job_id=entry.job_id,
                cgroup_root=self.config.containment.cgroup_root,
            )
            request = document["projection_request"]
            if isinstance(request, dict) and (
                request.get("cgroup_path") != batch.authority_path
                or request.get("cgroup_inode") != batch.inode
            ):
                raise GuardError("reconciliation_cgroup_identity_invalid")
            if entry.state in {"intent", "challenge_pending", "challenged"}:
                self._verify_pre_containment_batch(batch.path)
            self._verify_pin_tree(entry)
            if entry.state in {"attached", "projected", "exchanged"}:
                self._verify_runtime_controls(document)
            peer.assert_unchanged()
            complete = True
            return peer
        finally:
            if not complete:
                peer.close()

    def _verify_pre_containment_batch(self, batch: Path) -> None:
        try:
            first = self._read_control(batch, "cgroup.stat")
            second = self._read_control(batch, "cgroup.stat")
            if first != second:
                raise GuardError("reconciliation_cgroup_identity_invalid")
            values: dict[str, int] = {}
            for row in first.splitlines():
                fields = row.split()
                if len(fields) != 2 or fields[0] in values:
                    raise GuardError("reconciliation_cgroup_identity_invalid")
                values[fields[0]] = int(fields[1])
            if (
                values.get("nr_descendants") != 0
                or values.get("nr_dying_descendants") != 0
            ):
                raise GuardError("reconciliation_cgroup_identity_invalid")
        except GuardError:
            raise
        except (OSError, ValueError):
            raise GuardError("reconciliation_cgroup_identity_invalid") from None

    def _terminal_empty(self, entry: LedgerEntry) -> None:
        document = entry.document()
        request = document["projection_request"]
        if not isinstance(request, dict):
            raise GuardError("reconciliation_terminal_identity_invalid")
        cgroup_path = request.get("cgroup_path")
        cgroup_inode = request.get("cgroup_inode")
        if not isinstance(cgroup_path, str) or type(cgroup_inode) is not int:
            raise GuardError("reconciliation_terminal_identity_invalid")
        batch = Path(cgroup_path)
        try:
            batch.relative_to(self.config.containment.cgroup_root)
            metadata = os.lstat(batch)
            if (
                batch.resolve(strict=True) != batch
                or not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_ino != cgroup_inode
            ):
                raise GuardError("reconciliation_terminal_identity_invalid")
            root = batch / "loom-builder"
            trusted = root / "trusted-service"
            build = root / "build-egress"
            directories = [batch, root, trusted, build]
            descendants: list[Path] = []
            pending = [trusted, build]
            while pending:
                directory = pending.pop()
                for child in sorted(directory.iterdir(), key=lambda item: item.name):
                    child_metadata = os.lstat(child)
                    if stat.S_ISLNK(child_metadata.st_mode):
                        raise GuardError("reconciliation_terminal_identity_invalid")
                    if stat.S_ISDIR(child_metadata.st_mode):
                        if child.resolve(strict=True) != child:
                            raise GuardError("reconciliation_terminal_identity_invalid")
                        descendants.append(child)
                        pending.append(child)
            directories.extend(descendants)
            for directory in directories:
                if self._read_control(
                    directory, "cgroup.procs", allow_empty=True
                ).split():
                    raise GuardError("reconciliation_allocation_not_empty")
            child_names = {
                item.name for item in root.iterdir() if item.is_dir() and not item.is_symlink()
            }
            if child_names != {"trusted-service", "build-egress"} or descendants:
                raise GuardError("reconciliation_terminal_identity_invalid")
            self._verify_pin_tree(entry)
        except GuardError:
            raise
        except (OSError, UnicodeError, ValueError) as exc:
            raise GuardError("reconciliation_terminal_identity_invalid") from exc

    def classify(self, entry: LedgerEntry) -> str:
        if entry.state == "terminal":
            self._terminal_empty(entry)
            return "terminal_empty"
        peer = self._live_peer(entry)
        peer.close()
        if entry.state in {"intent", "challenge_pending", "challenged"}:
            return "pre_containment_clean"
        return "live_exact"

    def assert_live(self, entry: LedgerEntry) -> None:
        peer = self._live_peer(entry)
        peer.close()

    def open_live(self, entry: LedgerEntry) -> PeerHandle:
        return self._live_peer(entry)

    @staticmethod
    def _remove_tree(path: Path) -> None:
        if path.is_symlink() or not path.is_dir():
            raise GuardError("reconciliation_cleanup_ambiguous")
        for child in sorted(path.iterdir(), key=lambda value: value.name, reverse=True):
            if child.is_symlink():
                raise GuardError("reconciliation_cleanup_ambiguous")
            if child.is_dir():
                SystemReconciliationProbe._remove_tree(child)
            else:
                child.unlink()
        path.rmdir()

    def cleanup_terminal(self, entry: LedgerEntry) -> None:
        self._terminal_empty(entry)
        document = entry.document()
        pin_path = document["pin_path"]
        request = cast(dict[str, object], document["projection_request"])
        root = Path(cast(str, request["cgroup_path"])) / "loom-builder"
        try:
            (root / "build-egress").rmdir()
            (root / "trusted-service").rmdir()
            root.rmdir()
        except OSError as exc:
            raise GuardError("reconciliation_cleanup_ambiguous") from exc
        if pin_path is not None:
            self._remove_tree(Path(cast(str, pin_path)))


@dataclass(slots=True)
class _LiveAllocation:
    grant_id: UUID
    peer: Peer = field(repr=False)
    attachment: Attachment = field(repr=False)
    next_attestation: float

    def close(self) -> None:
        self.attachment.close()
        self.peer.close()


@dataclass(frozen=True, slots=True)
class _RecoveredAttachment:
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

    @classmethod
    def from_proof(cls, proof: dict[str, object]) -> _RecoveredAttachment:
        value = proof.get("attachment")
        if not isinstance(value, dict):
            raise GuardError("service_replay_attachment_invalid")
        attachment = cast(dict[str, object], value)
        expected = {
            "schema_version",
            "cgroup_inode",
            "containment_root",
            "trusted_service_cgroup",
            "build_egress_cgroup",
            "bpf_program_sha256",
            "bpf_map_schema_sha256",
            "containment_policy_sha256",
            "resource_limits_sha256",
            "probe_sha256",
            "link_ids",
            "program_ids",
            "map_ids",
        }
        if set(attachment) != expected:
            raise GuardError("service_replay_attachment_invalid")
        scalar_names = (
            "containment_root",
            "trusted_service_cgroup",
            "build_egress_cgroup",
            "bpf_program_sha256",
            "bpf_map_schema_sha256",
            "containment_policy_sha256",
            "resource_limits_sha256",
            "probe_sha256",
        )
        if any(not isinstance(attachment[name], str) for name in scalar_names):
            raise GuardError("service_replay_attachment_invalid")

        def ids(name: str) -> tuple[int, ...]:
            items = attachment[name]
            if not isinstance(items, list) or any(type(item) is not int for item in items):
                raise GuardError("service_replay_attachment_invalid")
            return cast(tuple[int, ...], tuple(items))

        inode = attachment["cgroup_inode"]
        if attachment["schema_version"] != 1 or type(inode) is not int or inode <= 0:
            raise GuardError("service_replay_attachment_invalid")
        return cls(
            inode,
            cast(str, attachment["containment_root"]),
            cast(str, attachment["trusted_service_cgroup"]),
            cast(str, attachment["build_egress_cgroup"]),
            cast(str, attachment["bpf_program_sha256"]),
            cast(str, attachment["bpf_map_schema_sha256"]),
            cast(str, attachment["containment_policy_sha256"]),
            cast(str, attachment["resource_limits_sha256"]),
            cast(str, attachment["probe_sha256"]),
            ids("link_ids"),
            ids("program_ids"),
            ids("map_ids"),
        )

    def close(self) -> None:
        return


class MainLoopProgress:
    """Share main-thread progress with bounded operations and the notifier."""

    def __init__(self, monotonic: Callable[[], float] = time.monotonic) -> None:
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._started_at: float | None = None
        self._progress_at: float | None = None

    def start(self) -> None:
        now = self._monotonic()
        with self._lock:
            self._started_at = now
            self._progress_at = now

    def mark(self) -> None:
        now = self._monotonic()
        with self._lock:
            if self._progress_at is None or now < self._progress_at:
                raise GuardError("service_monotonic_clock_invalid")
            self._progress_at = now

    def snapshot(self) -> tuple[float, float]:
        with self._lock:
            started_at = self._started_at
            progress_at = self._progress_at
        if started_at is None or progress_at is None:
            raise GuardError("service_keepalive_failed")
        return started_at, progress_at


class _ServiceKeepalive:
    def __init__(
        self,
        *,
        startup: Callable[[], None],
        watchdog: Callable[[], None],
        interval_seconds: float,
        monotonic: Callable[[], float],
        progress_timeout_seconds: float,
        startup_extension_limit_seconds: float,
        progress: MainLoopProgress | None = None,
    ) -> None:
        if (
            type(interval_seconds) not in {int, float}
            or not 0 < interval_seconds <= 10
            or type(progress_timeout_seconds) not in {int, float}
            or progress_timeout_seconds <= interval_seconds
            or type(startup_extension_limit_seconds) not in {int, float}
            or startup_extension_limit_seconds <= progress_timeout_seconds
        ):
            raise GuardError("service_keepalive_interval_invalid")
        self._startup = startup
        self._watchdog = watchdog
        self._interval_seconds = interval_seconds
        self._monotonic = monotonic
        self._progress = progress or MainLoopProgress(monotonic)
        self._progress_timeout_seconds = progress_timeout_seconds
        self._startup_extension_limit_seconds = startup_extension_limit_seconds
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._failure_lock = threading.Lock()
        self._failure: GuardError | None = None
        self._thread = threading.Thread(
            target=self._run,
            name="loom-guard-keepalive",
            daemon=True,
        )

    def _record_failure(self, error: GuardError) -> None:
        with self._failure_lock:
            if self._failure is None:
                self._failure = error
        self._stop.set()

    def _run(self) -> None:
        while not self._stop.wait(self._interval_seconds):
            try:
                now = self._monotonic()
                started_at, progress_at = self._progress.snapshot()
                if now < progress_at or now - progress_at > self._progress_timeout_seconds:
                    raise GuardError("service_main_loop_stalled")
                ready = self._ready.is_set()
                if (
                    not ready
                    and now - started_at > self._startup_extension_limit_seconds
                ):
                    raise GuardError("service_startup_deadline_exceeded")
                callback = self._watchdog if ready else self._startup
                callback()
            except GuardError as exc:
                self._record_failure(exc)
                return
            except Exception:
                self._record_failure(GuardError("service_keepalive_failed"))
                return

    def start(self) -> None:
        try:
            self._progress.start()
            self._startup()
            self._thread.start()
        except GuardError:
            raise
        except Exception:
            raise GuardError("service_keepalive_failed") from None

    def progress(self) -> None:
        self._progress.mark()

    def mark_ready(self) -> None:
        self._ready.set()

    def check(self) -> None:
        with self._failure_lock:
            failure = self._failure
        if failure is not None:
            raise GuardError(failure.code)

    def close(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join()


def _challenge_document(value: ProjectionChallenge) -> dict[str, object]:
    return {
        "schema_version": 1,
        "request_id": str(value.request_id),
        "grant_id": str(value.grant_id),
        "request_sha256": value.request_sha256,
        "challenge_nonce": str(value.challenge_nonce),
        "containment_policy_sha256": value.containment_policy_sha256,
        "resource_profile_sha256": value.resource_profile_sha256,
        "issued_at": _timestamp(value.issued_at),
        "expires_at": _timestamp(value.expires_at),
    }


def _attachment_document(value: Attachment) -> dict[str, object]:
    return {
        "schema_version": 1,
        "cgroup_inode": value.cgroup_inode,
        "containment_root": value.containment_root,
        "trusted_service_cgroup": value.trusted_service_cgroup,
        "build_egress_cgroup": value.build_egress_cgroup,
        "bpf_program_sha256": value.bpf_program_sha256,
        "bpf_map_schema_sha256": value.bpf_map_schema_sha256,
        "containment_policy_sha256": value.containment_policy_sha256,
        "resource_limits_sha256": value.resource_limits_sha256,
        "probe_sha256": value.probe_sha256,
        "link_ids": list(value.link_ids),
        "program_ids": list(value.program_ids),
        "map_ids": list(value.map_ids),
    }


class GuardService:
    """Bounded seqpacket service; all authority comes from local observation."""

    def __init__(
        self,
        config: GuardConfigValue,
        *,
        ledger: GuardLedger,
        peers: PeerSource,
        slurm: SlurmSource,
        derive_batch: Callable[[Peer, str], Batch],
        containment: Containment,
        policy: object,
        authority: Authority,
        reconciler: Reconciler,
        node_boot_id: UUID,
        uuid_factory: Callable[[], UUID] = uuid4,
        now_factory: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
        trusted_uid: int = 0,
        trusted_gid: int = 0,
        startup: Callable[[], None] = lambda: None,
        ready: Callable[[], None] = lambda: None,
        watchdog: Callable[[], None] = lambda: None,
        keepalive_interval_seconds: float = 10.0,
        progress_timeout_seconds: float = 75.0,
        startup_extension_limit_seconds: float = 900.0,
        progress: MainLoopProgress | None = None,
    ) -> None:
        if not isinstance(node_boot_id, UUID) or node_boot_id.int == 0:
            raise GuardError("service_boot_identity_invalid")
        self.config = config
        self.ledger = ledger
        self.peers = peers
        self.slurm = slurm
        self.derive_batch = derive_batch
        self.containment = containment
        self.policy = policy
        self.authority = authority
        self.reconciler = reconciler
        self.node_boot_id = node_boot_id
        self._uuid = uuid_factory
        self._now = now_factory or (lambda: datetime.now(UTC))
        self._monotonic = monotonic
        self._trusted_uid = trusted_uid
        self._trusted_gid = trusted_gid
        self._startup = startup
        self._ready = ready
        self._watchdog = watchdog
        self._keepalive_interval_seconds = keepalive_interval_seconds
        self._progress_timeout_seconds = progress_timeout_seconds
        self._startup_extension_limit_seconds = startup_extension_limit_seconds
        self._progress = progress or MainLoopProgress(monotonic)
        self._stop = threading.Event()
        self._listener: socket.socket | None = None
        self._runtime_fd: int | None = None
        self._live: dict[UUID, _LiveAllocation] = {}
        self._request_times: deque[float] = deque()
        self._mark_progress: Callable[[], None] = lambda: None

    def stop(self) -> None:
        self._stop.set()

    def close(self) -> None:
        self.stop()
        for grant_id in sorted(self._live, key=str):
            self._live.pop(grant_id).close()

    def _new_uuid(self) -> UUID:
        value = self._uuid()
        if not isinstance(value, UUID) or value.int == 0:
            raise GuardError("service_uuid_invalid")
        return value

    def _now_utc(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise GuardError("service_clock_invalid")
        return value.astimezone(UTC)

    def _quarantine_unrecorded_mutation(self, grant_id: UUID) -> None:
        try:
            observed_at = _timestamp(self._now_utc())
        except GuardError:
            observed_at = None
        if observed_at is not None:
            try:
                self.authority.revoke(
                    grant_id,
                    {
                        "schema_version": 1,
                        "grant_id": str(grant_id),
                        "reason": "containment_state_ambiguous",
                        "observed_at": observed_at,
                    },
                )
            except GuardError:
                pass
        try:
            self.ledger.quarantine(
                grant_id,
                reason="containment_state_ambiguous",
            )
        except GuardError:
            self.stop()
        try:
            self.slurm.quarantine_capability()
        except GuardError:
            self.stop()

    def _acquire_runtime_lock(self) -> None:
        path = self.config.protocol.socket_path
        parent = path.parent
        runtime_fd: int | None = None
        try:
            metadata = os.lstat(parent)
            if (
                parent.resolve(strict=True) != parent
                or not stat.S_ISDIR(metadata.st_mode)
                or metadata.st_uid != self._trusted_uid
                or metadata.st_gid != self._trusted_gid
                or stat.S_IMODE(metadata.st_mode) != 0o711
            ):
                raise GuardError("service_socket_root_invalid")
            runtime_fd = os.open(
                parent,
                os.O_RDONLY
                | os.O_CLOEXEC
                | os.O_DIRECTORY
                | getattr(os, "O_NOFOLLOW", 0),
            )
            opened = os.fstat(runtime_fd)
            if (
                opened.st_dev != metadata.st_dev
                or opened.st_ino != metadata.st_ino
                or opened.st_mode != metadata.st_mode
                or opened.st_uid != metadata.st_uid
                or opened.st_gid != metadata.st_gid
            ):
                raise GuardError("service_socket_root_invalid")
            try:
                fcntl.flock(runtime_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as exc:
                raise GuardError("service_already_running") from exc
            self._runtime_fd = runtime_fd
            runtime_fd = None
        except GuardError:
            if runtime_fd is not None:
                os.close(runtime_fd)
            raise
        except OSError as exc:
            if runtime_fd is not None:
                os.close(runtime_fd)
            raise GuardError("service_socket_root_invalid") from exc

    def _bind_listener(self) -> socket.socket:
        path = self.config.protocol.socket_path
        listener: socket.socket | None = None
        try:
            if self._runtime_fd is None:
                raise GuardError("service_runtime_lock_missing")
            try:
                existing = os.lstat(path)
            except FileNotFoundError:
                pass
            else:
                if (
                    not stat.S_ISSOCK(existing.st_mode)
                    or existing.st_uid != self._trusted_uid
                    or existing.st_gid != self.config.protocol.socket_gid
                    or stat.S_IMODE(existing.st_mode) != self.config.protocol.socket_mode
                ):
                    raise GuardError("service_socket_root_invalid")
                path.unlink()
            listener = socket.socket(socket.AF_UNIX, socket.SOCK_SEQPACKET | socket.SOCK_CLOEXEC)
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
            listener.bind(str(path))
            os.chown(path, self._trusted_uid, self.config.protocol.socket_gid)
            os.chmod(path, self.config.protocol.socket_mode)
            opened = os.lstat(path)
            if (
                not stat.S_ISSOCK(opened.st_mode)
                or opened.st_uid != self._trusted_uid
                or opened.st_gid != self.config.protocol.socket_gid
                or stat.S_IMODE(opened.st_mode) != self.config.protocol.socket_mode
            ):
                raise GuardError("service_socket_invalid")
            listener.listen(self.config.protocol.max_pending_peers)
            listener.settimeout(0.5)
            return listener
        except GuardError:
            if listener is not None:
                listener.close()
            raise
        except OSError as exc:
            if listener is not None:
                listener.close()
            raise GuardError("service_socket_invalid") from exc

    def start(self) -> None:
        """Take singleton authority, then reconcile before exposing a socket."""

        keepalive = _ServiceKeepalive(
            startup=self._startup,
            watchdog=self._watchdog,
            interval_seconds=self._keepalive_interval_seconds,
            monotonic=self._monotonic,
            progress_timeout_seconds=self._progress_timeout_seconds,
            startup_extension_limit_seconds=self._startup_extension_limit_seconds,
            progress=self._progress,
        )
        try:
            keepalive.start()
            self._mark_progress = keepalive.progress
            self._acquire_runtime_lock()
            self._mark_progress()
            self.reconciler.reconcile(self.ledger)
            self._mark_progress()
            keepalive.check()
            recovered = self.reconciler.recover_live()
            try:
                for entry, peer in recovered:
                    document = entry.document()
                    proof = document["proof"]
                    if not isinstance(proof, dict) or entry.grant_id in self._live:
                        raise GuardError("reconciliation_live_state_invalid")
                    attachment = cast(
                        Attachment,
                        _RecoveredAttachment.from_proof(cast(dict[str, object], proof)),
                    )
                    self._live[entry.grant_id] = _LiveAllocation(
                        entry.grant_id,
                        peer,
                        attachment,
                        self._monotonic(),
                    )
                    self._mark_progress()
            except GuardError:
                recovered_ids = {entry.grant_id for entry, _peer in recovered}
                for entry, peer in recovered:
                    live = self._live.pop(entry.grant_id, None)
                    if live is not None:
                        live.close()
                    elif entry.grant_id in recovered_ids:
                        peer.close()
                self.slurm.quarantine_capability()
                raise
            self.run_attestations_once()
            self._mark_progress()
            keepalive.check()
            listener = self._bind_listener()
            self._mark_progress()
            self._listener = listener
            try:
                keepalive.check()
                self._ready()
                keepalive.mark_ready()
                while not self._stop.is_set():
                    self._mark_progress()
                    keepalive.check()
                    self.run_attestations_once()
                    keepalive.check()
                    try:
                        connection, _address = listener.accept()
                    except TimeoutError:
                        continue
                    with connection:
                        connection.settimeout(self.config.protocol.ack_timeout_seconds)
                        connection.setsockopt(socket.SOL_SOCKET, socket.SO_PASSCRED, 1)
                        self.serve_connection(connection)
                    self._mark_progress()
                    keepalive.check()
            finally:
                self._listener = None
                listener.close()
                try:
                    self.config.protocol.socket_path.unlink()
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    raise GuardError("service_socket_cleanup_failed") from exc
        finally:
            self._mark_progress = lambda: None
            if self._runtime_fd is not None:
                os.close(self._runtime_fd)
                self._runtime_fd = None
            keepalive.close()

    def _admit(self) -> None:
        now = self._monotonic()
        while self._request_times and now - self._request_times[0] >= 1.0:
            self._request_times.popleft()
        if len(self._request_times) >= self.config.protocol.requests_per_second:
            raise GuardError("local_rate_limited")
        self._request_times.append(now)

    def serve_connection(self, connection: socket.socket) -> None:
        descriptor: int | None = None
        peer: Peer | None = None
        packet = None
        try:
            self._admit()
            packet = receive_authenticated_packet(
                connection,
                maximum=self.config.protocol.max_packet_bytes,
            )
            message_credentials = packet.credentials
            peer = self.peers.capture(connection, message_credentials)
            self._mark_progress()
            payload, descriptor = packet.finish()
            request = parse_local_request(payload)
            if request.operation == "project":
                if descriptor is not None:
                    raise GuardError("local_project_descriptor_forbidden")
                if request.grant_id is None:
                    raise GuardError("local_request_invalid")
                owned_peer = peer
                peer = None
                self._project(
                    connection,
                    request.grant_id,
                    owned_peer,
                    message_credentials,
                )
            elif request.operation == "exchange":
                if descriptor is None:
                    raise GuardError("local_exchange_descriptor_required")
                if (
                    request.grant_id is None
                    or request.exchange_id is None
                    or request.proof_sha256 is None
                ):
                    raise GuardError("local_request_invalid")
                owned_peer = peer
                peer = None
                self._exchange(
                    connection,
                    request.grant_id,
                    request.exchange_id,
                    request.proof_sha256,
                    descriptor,
                    owned_peer,
                    message_credentials,
                )
            else:
                raise GuardError("local_operation_invalid")
        except GuardError as exc:
            try:
                send_packet(
                    connection,
                    _json(
                        {
                            "schema": LOCAL_SCHEMA,
                            "operation": "error",
                            "code": exc.code,
                        }
                    ),
                )
            except GuardError:
                pass
        finally:
            if packet is not None:
                packet.close()
            if peer is not None:
                peer.close()
            if descriptor is not None:
                try:
                    os.close(descriptor)
                except OSError:
                    pass

    def _project(
        self,
        connection: socket.socket,
        grant_id: UUID,
        peer: Peer,
        message_credentials: PeerCredentials,
    ) -> None:
        attachment: Attachment | None = None
        retained = False
        try:
            existing = self.ledger.get(grant_id)
            request_id = self._new_uuid() if existing is None else existing.request_id
            entry = self.ledger.create_intent(
                grant_id=grant_id,
                request_id=request_id,
                peer_pid=peer.pid,
                job_id=peer.job_id,
                peer_executable_sha256=peer.executable_sha256,
                batch_cgroup_relative=str(peer.batch_cgroup_relative),
            )
            self._mark_progress()
            peer.assert_unchanged()
            self.slurm.observe(job_id=peer.job_id, grant_id=grant_id)
            self._mark_progress()
            peer.assert_unchanged()
            batch = self.derive_batch(peer, peer.job_id)
            self._mark_progress()
            peer.assert_unchanged()
            if entry.state == "intent":
                request = projection_request(
                    grant_id=grant_id,
                    request_id=request_id,
                    observed_at=self._now_utc(),
                    node_name=self.config.node_name,
                    node_boot_id=self.node_boot_id,
                    cluster_id=self.config.cluster_id,
                    cpu_arch=self.config.cpu_arch,
                    slurm_request_sha256=self.config.slurm.request_sha256,
                    slurm_qos=self.config.slurm.qos,
                    peer=cast(PeerHandle, peer),
                    batch=cast(BatchCgroup, batch),
                )
                entry = self.ledger.record_projection_request(
                    grant_id,
                    projection_request=request,
                    projection_request_sha256=self.authority.request_sha256(request),
                )
            if entry.state in {"challenge_pending", "challenged"}:
                replay = entry.document()
                request_value = replay["projection_request"]
                if not isinstance(request_value, dict):
                    raise GuardError("service_replay_identity_invalid")
                request = cast(dict[str, object], request_value)
                self._validate_replay_request(entry, request, peer, batch)
                peer.assert_unchanged()
                challenge = self.authority.challenge(
                    grant_id,
                    request,
                    containment_policy_sha256=(
                        self.config.containment.containment_policy_sha256
                    ),
                    resource_profile_sha256=(
                        self.config.containment.resource_profile_sha256
                    ),
                )
                self._mark_progress()
                challenge_document = _challenge_document(challenge)
                if entry.state == "challenge_pending":
                    entry = self.ledger.record_challenge(
                        grant_id,
                        projection_request=request,
                        projection_request_sha256=self.authority.request_sha256(request),
                        challenge=challenge_document,
                        challenge_sha256=self.authority.request_sha256(challenge_document),
                    )
                    self._mark_progress()
                elif challenge_document != replay["challenge"]:
                    raise GuardError("service_replay_identity_invalid")
            else:
                replay = entry.document()
                request_value = replay["projection_request"]
                challenge_value = replay["challenge"]
                if not isinstance(request_value, dict) or not isinstance(
                    challenge_value, dict
                ):
                    raise GuardError("service_replay_identity_invalid")
                request = cast(dict[str, object], request_value)
                challenge_document = cast(dict[str, object], challenge_value)
                self._validate_replay_request(entry, request, peer, batch)
            if entry.state == "challenged":
                if peer.cgroup_relative != peer.batch_cgroup_relative:
                    raise GuardError("service_replay_identity_ambiguous")
                entry = self.ledger.record_containment_pending(grant_id)
                try:
                    attachment = self.containment.prepare(
                        batch, peer, self.policy, grant_id
                    )
                    self._mark_progress()
                    peer.assert_unchanged()
                    proof_id = self._new_uuid()
                    proof_observed = self._now_utc()
                    attachment_document = _attachment_document(attachment)
                    proof = {
                        "schema_version": 1,
                        "proof_id": str(proof_id),
                        "grant_id": str(grant_id),
                        "request_id": str(request_id),
                        "request_sha256": self.authority.request_sha256(request),
                        "challenge_nonce": challenge_document["challenge_nonce"],
                        "observed_at": _timestamp(proof_observed),
                        "node_name": self.config.node_name,
                        "node_boot_id": str(self.node_boot_id),
                        "slurm_cluster_id": self.config.cluster_id,
                        "slurm_job_id": peer.job_id,
                        "cgroup_path": batch.authority_path,
                        "cgroup_inode": batch.inode,
                        "attachment": attachment_document,
                        "attestation_generation": 1,
                        "attestation_expires_at": _timestamp(
                            proof_observed
                            + timedelta(
                                seconds=(
                                    self.config.service.attestation_lifetime_seconds
                                )
                            )
                        ),
                    }
                    initial_attestation = self._attestation_from_proof(proof)
                    proof_sha256 = self.authority.request_sha256(proof)
                    entry = self.ledger.record_attachment(
                        grant_id,
                        proof=proof,
                        proof_sha256=proof_sha256,
                        pin_path=str(
                            Path(self.config.containment.bpffs_root) / str(grant_id)
                        ),
                        link_ids=attachment.link_ids,
                        program_ids=attachment.program_ids,
                        map_ids=attachment.map_ids,
                        attestation_generation=1,
                        attestation_sha256=self.authority.request_sha256(
                            initial_attestation
                        ),
                        attestation_expires_at=cast(
                            str, proof["attestation_expires_at"]
                        ),
                    )
                    self._mark_progress()
                except GuardError:
                    self._quarantine_unrecorded_mutation(grant_id)
                    raise
            elif entry.state in {"attached", "projected"}:
                trusted = (
                    PurePosixPath(str(peer.batch_cgroup_relative))
                    / "loom-builder"
                    / "trusted-service"
                )
                if PurePosixPath(str(peer.cgroup_relative)) != trusted:
                    raise GuardError("service_replay_identity_ambiguous")
                replay = entry.document()
                proof_value = replay["proof"]
                if not isinstance(proof_value, dict):
                    raise GuardError("service_replay_identity_invalid")
                proof = cast(dict[str, object], proof_value)
                proof_sha256 = cast(str, replay["proof_sha256"])
                attachment = cast(Attachment, _RecoveredAttachment.from_proof(proof))
            else:
                raise GuardError("service_projection_not_replayable")
            peer.assert_unchanged()
            if attachment is None:
                raise GuardError("service_attachment_missing")
            self.reconciler.assert_live(entry)
            self._mark_progress()
            receipt = self.authority.attach(grant_id, proof)
            self._mark_progress()
            projected_entry = self.ledger.record_projection(
                grant_id,
                receipt_public_binding_sha256=receipt.public_binding_sha256,
                bootstrap_token_sha256=hashlib.sha256(
                    receipt.bootstrap_token.encode("ascii")
                ).hexdigest(),
            )
            self._mark_progress()
            peer.assert_unchanged()
            if grant_id not in self._live:
                self._live[grant_id] = _LiveAllocation(
                    grant_id,
                    peer,
                    attachment,
                    self._monotonic() + self.config.service.attestation_interval_seconds,
                )
                retained = True
            elif projected_entry.request_id != entry.request_id:
                raise GuardError("service_replay_identity_invalid")
            response_id = self._new_uuid()
            response = _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "projected",
                    "response_id": str(response_id),
                    "grant_id": str(grant_id),
                    "proof_sha256": proof_sha256,
                    "receipt_public_binding_sha256": receipt.public_binding_sha256,
                }
            )
            secret_fd = create_sealed_memfd(
                "task-image-bootstrap",
                receipt.wire_payload,
                maximum=_MAX_SECRET_BYTES,
            )
            try:
                with peer.containment_hold():
                    peer.assert_unchanged()
                    send_packet(connection, response, descriptor=secret_fd)
                self._mark_progress()
                require_ack(
                    connection,
                    response_id=response_id,
                    timeout_seconds=self.config.protocol.ack_timeout_seconds,
                    maximum=self.config.protocol.max_packet_bytes,
                    expected_credentials=message_credentials,
                )
            finally:
                os.close(secret_fd)
        finally:
            if not retained:
                if attachment is not None:
                    attachment.close()
                peer.close()

    def _validate_replay_request(
        self,
        entry: LedgerEntry,
        request: dict[str, object],
        peer: Peer,
        batch: Batch,
    ) -> None:
        expected = {
            "schema_version": 1,
            "request_id": str(entry.request_id),
            "grant_id": str(entry.grant_id),
            "node_name": self.config.node_name,
            "node_boot_id": str(self.node_boot_id),
            "slurm_cluster_id": self.config.cluster_id,
            "slurm_job_id": peer.job_id,
            "supervisor_pid": peer.pid,
            "supervisor_uid": peer.uid,
            "supervisor_gid": peer.gid,
            "supervisor_executable_sha256": peer.executable_sha256,
            "cgroup_path": batch.authority_path,
            "cgroup_inode": batch.inode,
            "submitting_identity": "loom-builder",
            "slurm_account": "loom-task-builder",
            "slurm_partition": "loom-task-builder",
            "slurm_qos": self.config.slurm.qos,
            "cpu_arch": self.config.cpu_arch,
            "slurm_request_sha256": self.config.slurm.request_sha256,
        }
        if any(request.get(name) != value for name, value in expected.items()) or set(
            request
        ) != {*expected, "observed_at"}:
            raise GuardError("service_replay_identity_invalid")

    @staticmethod
    def _attestation_from_proof(proof: dict[str, object]) -> dict[str, object]:
        return {
            "schema_version": 1,
            "attestation_id": proof["proof_id"],
            "grant_id": proof["grant_id"],
            "generation": proof["attestation_generation"],
            "node_name": proof["node_name"],
            "node_boot_id": proof["node_boot_id"],
            "slurm_cluster_id": proof["slurm_cluster_id"],
            "slurm_job_id": proof["slurm_job_id"],
            "cgroup_path": proof["cgroup_path"],
            "cgroup_inode": proof["cgroup_inode"],
            "attachment": proof["attachment"],
            "issued_at": proof["observed_at"],
            "expires_at": proof["attestation_expires_at"],
        }

    def _exchange_document(self, payload: bytes) -> dict[str, object]:
        try:
            value = json.loads(payload, object_pairs_hook=_pairs)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, ValueError):
            raise GuardError("local_exchange_invalid") from None
        keys = {
            "schema_version",
            "exchange_id",
            "grant_id",
            "proof_sha256",
            "bootstrap_token",
            "observed_at",
        }
        if not isinstance(value, dict) or set(value) != keys:
            raise GuardError("local_exchange_invalid")
        result = cast(dict[str, object], value)
        token = result["bootstrap_token"]
        if result["schema_version"] != 1 or not isinstance(token, str) or (
            _BOOTSTRAP.fullmatch(token) is None
        ):
            raise GuardError("local_exchange_invalid")
        return result

    def _exchange(
        self,
        connection: socket.socket,
        grant_id: UUID,
        exchange_id: UUID,
        proof_sha256: str,
        descriptor: int,
        peer: Peer,
        message_credentials: PeerCredentials,
    ) -> None:
        entry = self.ledger.get(grant_id)
        self._mark_progress()
        if entry is None or entry.state not in {"projected", "exchanged"}:
            raise GuardError("local_exchange_unavailable")
        document = entry.document()
        if document["proof_sha256"] != proof_sha256:
            raise GuardError("local_exchange_binding_invalid")
        exchange = self._exchange_document(
            read_sealed_memfd(descriptor, maximum=_MAX_SECRET_BYTES)
        )
        self._mark_progress()
        if (
            _uuid(exchange["grant_id"], code="local_exchange_invalid") != grant_id
            or _uuid(exchange["exchange_id"], code="local_exchange_invalid") != exchange_id
            or exchange["proof_sha256"] != proof_sha256
        ):
            raise GuardError("local_exchange_binding_invalid")
        token = cast(str, exchange["bootstrap_token"])
        if hashlib.sha256(token.encode("ascii")).hexdigest() != document[
            "bootstrap_token_sha256"
        ]:
            raise GuardError("local_exchange_binding_invalid")
        try:
            if (
                peer.pid != entry.peer_pid
                or peer.job_id != entry.job_id
                or peer.executable_sha256 != document["peer_executable_sha256"]
            ):
                raise GuardError("local_exchange_peer_invalid")
            peer.assert_unchanged()
            self.slurm.observe(job_id=peer.job_id, grant_id=grant_id)
            self._mark_progress()
            peer.assert_unchanged()
            self.reconciler.assert_live(entry)
            self._mark_progress()
            session = self.authority.exchange(grant_id, exchange)
            self._mark_progress()
            if (
                session.attestation_generation != document["attestation_generation"]
                or session.attestation_sha256 != document["attestation_sha256"]
                or session.cpu_arch != self.config.cpu_arch
            ):
                raise GuardError("authority_session_binding_invalid")
            public_exchange = dict(exchange)
            public_exchange.pop("bootstrap_token")
            public_exchange["bootstrap_token_sha256"] = hashlib.sha256(
                token.encode("ascii")
            ).hexdigest()
            self.ledger.record_exchange(
                grant_id,
                exchange_id=exchange_id,
                exchange_public_binding_sha256=self.authority.request_sha256(
                    public_exchange
                ),
                session_id=session.session_id,
                session_public_binding_sha256=session.public_binding_sha256,
                session_token_sha256=hashlib.sha256(
                    session.session_token.encode("ascii")
                ).hexdigest(),
                session_expires_at=_timestamp(session.expires_at),
            )
            self._mark_progress()
            response_id = self._new_uuid()
            response = _json(
                {
                    "schema": LOCAL_SCHEMA,
                    "operation": "session",
                    "response_id": str(response_id),
                    "grant_id": str(grant_id),
                    "session_id": str(session.session_id),
                    "session_public_binding_sha256": session.public_binding_sha256,
                }
            )
            secret_fd = create_sealed_memfd(
                "task-image-session", session.wire_payload, maximum=_MAX_SECRET_BYTES
            )
            try:
                with peer.containment_hold():
                    peer.assert_unchanged()
                    send_packet(connection, response, descriptor=secret_fd)
                self._mark_progress()
                require_ack(
                    connection,
                    response_id=response_id,
                    timeout_seconds=self.config.protocol.ack_timeout_seconds,
                    maximum=self.config.protocol.max_packet_bytes,
                    expected_credentials=message_credentials,
                )
            finally:
                os.close(secret_fd)
        finally:
            peer.close()

    def run_attestations_once(self) -> None:
        now_monotonic = self._monotonic()
        for grant_id in sorted(tuple(self._live), key=str):
            self._mark_progress()
            live = self._live.get(grant_id)
            if live is None or now_monotonic < live.next_attestation:
                continue
            try:
                entry = self.ledger.get(grant_id)
                if entry is None or entry.state not in {"projected", "exchanged"}:
                    raise GuardError("attestation_ledger_invalid")
                document = entry.document()
                live.peer.assert_unchanged()
                self.slurm.observe(job_id=entry.job_id, grant_id=grant_id)
                self._mark_progress()
                live.peer.assert_unchanged()
                self.reconciler.assert_live(entry)
                self._mark_progress()
                generation_value = document["attestation_generation"]
                if type(generation_value) is not int:
                    raise GuardError("attestation_generation_invalid")
                pending = document["pending_attestation"]
                if pending is None:
                    generation = generation_value + 1
                    issued = self._now_utc()
                    proof = document["proof"]
                    if not isinstance(proof, dict):
                        raise GuardError("attestation_proof_invalid")
                    attestation = self._attestation_from_proof(
                        cast(dict[str, object], proof)
                    )
                    attestation["attestation_id"] = str(self._new_uuid())
                    attestation["generation"] = generation
                    attestation["issued_at"] = _timestamp(issued)
                    attestation["expires_at"] = _timestamp(
                        issued
                        + timedelta(seconds=self.config.service.attestation_lifetime_seconds)
                    )
                    self.ledger.record_pending_attestation(
                        grant_id,
                        generation=generation,
                        attestation=attestation,
                        attestation_sha256=self.authority.request_sha256(attestation),
                    )
                elif isinstance(pending, dict):
                    attestation = cast(dict[str, object], pending)
                    pending_generation = attestation.get("generation")
                    if type(pending_generation) is not int:
                        raise GuardError("attestation_generation_invalid")
                    generation = pending_generation
                else:
                    raise GuardError("attestation_pending_invalid")
                accepted = self.authority.attest(grant_id, generation, attestation)
                self._mark_progress()
                self.ledger.record_attestation(
                    grant_id,
                    generation=generation,
                    attestation_sha256=accepted.sha256,
                    expires_at=_timestamp(accepted.expires_at),
                )
                self._mark_progress()
                live.next_attestation = (
                    now_monotonic + self.config.service.attestation_interval_seconds
                )
            except GuardError as exc:
                self._fail_live(grant_id, reason=exc.code)

    def _fail_live(self, grant_id: UUID, *, reason: str) -> None:
        del reason
        quarantine_failure: GuardError | None = None
        try:
            observed_at = _timestamp(self._now_utc())
        except GuardError:
            observed_at = None
        if observed_at is not None:
            try:
                self.authority.revoke(
                    grant_id,
                    {
                        "schema_version": 1,
                        "grant_id": str(grant_id),
                        "reason": "attestation_failed",
                        "observed_at": observed_at,
                    },
                )
            except GuardError:
                pass
        try:
            self.ledger.quarantine(grant_id, reason="attestation_ambiguous")
        except GuardError as exc:
            quarantine_failure = exc
        try:
            self.slurm.quarantine_capability()
        except GuardError as exc:
            quarantine_failure = exc
        live = self._live.pop(grant_id, None)
        if live is not None:
            live.close()
        if quarantine_failure is not None:
            raise quarantine_failure


__all__ = ["GuardService"]
