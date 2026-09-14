"""Fixed management sender, not a worker-facing command or cleanup authority.

The installed management process owns the inventory and its private SSH key.
Only its exact store read is sent; callers supply a claim selector, never facts,
paths, credentials or commands. The remote dedicated principal must be restricted
to the pinned no-argument helper by protected SSH/sudo policy. That installation
and the helper's local terminal/quiescence/journal checks are prerequisites: this
module alone must never enable deletion or release capacity.
"""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import os
import stat
from contextlib import ExitStack
from pathlib import Path
from typing import Annotated, Literal, Self
from uuid import UUID, uuid4

from pydantic import Field, field_validator, model_validator
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.native_recovery import (
    NativeInstalledAttemptV2,
    NativeRecoveryPreparationV1,
    _path,
)
from loom_capacity_build_guard.installation_store import RetainedBuildInstallation
from loom_capacity_build_guard.native_terminal_recovery import (
    NativeTerminalRecoveryStore,
    NativeTerminalRecoveryV1,
)
from loom_capacity_executor.native_installed_release import _Observation
from loom_capacity_executor.native_outer_build import _join, _stop_process
from loom_capacity_manager.contracts import (
    Digest,
    Identifier,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)

_MAX_REQUEST = 128 * 1024
_MAX_REPLY = 4096


class NativeRecoveryTargetV1(StrictV1Model):
    """Protected installation inventory; never populated from feature inputs."""

    installation_id: UUID
    pool_id: Identifier
    node_id: Identifier
    address: str
    port: int = Field(ge=1, le=65535)
    profile_sha256: Digest
    host_sha256: Digest
    identity: str
    identity_sha256: Digest
    known_hosts: str
    known_hosts_sha256: Digest

    @field_validator("address")
    @classmethod
    def _address(cls, value: str) -> str:
        parsed = ipaddress.ip_address(value)
        if str(parsed) != value or "%" in value:
            raise ValueError("recovery target requires a canonical pinned IP address")
        return value

    @field_validator("identity", "known_hosts")
    @classmethod
    def _material_path(cls, value: str) -> str:
        _path(value)
        if any(character.isspace() or character in "%$~\"'\\" for character in value):
            raise ValueError("recovery SSH material path cannot contain option expansions")
        return value


class NativeNodeRecoveryRequestV1(StrictV1Model):
    operation: Literal["reconcile"] = "reconcile"
    invocation_id: UUID
    history: NativeTerminalRecoveryV1


class NativeRecoveryInventoryV1(StrictV1Model):
    """Retained root-owned transport inventory, independent of live membership."""

    targets: Annotated[tuple[NativeRecoveryTargetV1, ...], Field(min_length=1, max_length=4096)]

    @model_validator(mode="after")
    def _unique(self) -> Self:
        keys = [(item.installation_id, item.pool_id, item.node_id, item.host_sha256) for item in self.targets]
        if len(set(keys)) != len(keys):
            raise ValueError("recovery inventory requires unique protected targets")
        return self


def read_native_recovery_inventory(path: Path, *, expected_sha256: str) -> NativeRecoveryInventoryV1:
    """Read exact protected bootstrap bytes, never worker-selected inventory.

    Old installation entries must remain while their attempts need recovery.
    Credentials are still separately snapshotted and checked by each exchange.
    The installed management entrypoint owns this path/digest, not request JSON.
    """
    _path(str(path))
    observation = _Observation()
    wire = observation.read(path, digest=expected_sha256, size=None, mode=0o444,
        bound=4 * 1024**2, collect=True)
    inventory = NativeRecoveryInventoryV1.model_validate_json(wire)
    if canonical_bytes(inventory) != wire:
        raise ValueError("recovery inventory must be canonical")
    observation.finish()
    return inventory


class NativeNodeRecoveryResultV1(StrictV1Model):
    request_sha256: Digest
    state: Literal["retained", "completed"]
    reason: Literal["complete", "identity", "quiescence", "mapping", "journal", "unsupported"]
    executable: Literal[False] = False


def _protected_parents(path: Path, stack: ExitStack) -> int:
    descriptor = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    stack.callback(os.close, descriptor)
    for part in (None, *path.parts[1:]):
        if part is not None:
            descriptor = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptor)
            stack.callback(os.close, descriptor)
        metadata = os.fstat(descriptor)
        if metadata.st_uid not in (0, os.getuid()) or metadata.st_mode & 0o7022:
            raise ValueError("recovery transport material has an unprotected parent")
    return descriptor


def _read_transport_material(path: Path, *, expected_sha256: str) -> bytes:
    _path(str(path))
    with ExitStack() as stack:
        parent = _protected_parents(path.parent, stack)
        descriptor = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK,
            dir_fd=parent)
        stack.callback(os.close, descriptor)
        before = os.fstat(descriptor)
        if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
            or stat.S_IMODE(before.st_mode) != 0o600 or before.st_nlink != 1 or not 1 <= before.st_size <= 65536):
            raise ValueError("recovery transport material must be bounded, private and single-link")
        wire = bytearray()
        while part := os.read(descriptor, 65537 - len(wire)):
            wire.extend(part)
            if len(wire) > 65536:
                raise ValueError("recovery transport material exceeds bound")
        after = os.fstat(descriptor)
        if (len(wire) != before.st_size or hashlib.sha256(wire).hexdigest() != expected_sha256
            or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
            != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns)):
            raise ValueError("recovery transport material identity changed")
        return bytes(wire)


def _snapshot_target(target: NativeRecoveryTargetV1, stack: ExitStack) -> NativeRecoveryTargetV1:
    """Consume exact verified bytes despite atomic management-side rotation.

    OpenSSH may close inherited descriptors. Same-UID parent /proc FD paths
    remain available through the entire exchange, with immutable memfd bytes.
    No credential copy is written to disk or exposed to a workload identity.
    """
    from loom_capacity_executor.trusted_launcher import (
        _create_candidate_snapshot_descriptor,
        _seal_candidate_snapshot,
        _write_all,
    )

    updates = {}
    for field, digest in (("identity", target.identity_sha256), ("known_hosts", target.known_hosts_sha256)):
        wire = _read_transport_material(Path(getattr(target, field)), expected_sha256=digest)
        descriptor = _create_candidate_snapshot_descriptor()
        stack.callback(os.close, descriptor)
        os.fchmod(descriptor, 0o600)
        _write_all(descriptor, wire)
        _seal_candidate_snapshot(descriptor)
        updates[field] = f"/proc/{os.getpid()}/fd/{descriptor}"
    return target.model_copy(update=updates)


def _ssh_argv(target: NativeRecoveryTargetV1) -> tuple[str, ...]:
    target = NativeRecoveryTargetV1.model_validate_json(canonical_bytes(target))
    options = ("BatchMode=yes", "StrictHostKeyChecking=yes", "IdentitiesOnly=yes", "IdentityAgent=none",
        "ForwardAgent=no", "ForwardX11=no", "ClearAllForwardings=yes", "PermitLocalCommand=no",
        "ControlMaster=no", "ControlPath=none", "ProxyCommand=none", "ProxyJump=none",
        "PasswordAuthentication=no", "KbdInteractiveAuthentication=no", "PreferredAuthentications=publickey",
        "CertificateFile=none", "KnownHostsCommand=none", "HostbasedAuthentication=no", "GSSAPIAuthentication=no",
        "UpdateHostKeys=no", "GlobalKnownHostsFile=/dev/null", f"UserKnownHostsFile={target.known_hosts}",
        "ConnectTimeout=10", "ConnectionAttempts=1", "ServerAliveInterval=5", "ServerAliveCountMax=2",
        "RequestTTY=no", "EscapeChar=none", "LogLevel=ERROR")
    return ("/usr/bin/ssh", "-F", "/dev/null", "-T", "-i", target.identity, "-p", str(target.port),
        *(part for option in options for part in ("-o", option)), f"loom-native-recovery@{target.address}")


async def _spawn(target: NativeRecoveryTargetV1) -> asyncio.subprocess.Process:
    starting = asyncio.create_task(asyncio.create_subprocess_exec(*_ssh_argv(target),
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
        limit=_MAX_REPLY + 1, env={"PATH": "/usr/bin:/bin", "LANG": "C"}))
    try:
        return await asyncio.shield(starting)
    except asyncio.CancelledError:
        async def settle() -> None:
            try:
                process = await starting
            except Exception:
                return
            await _stop_process(process)
        await _join(asyncio.create_task(settle()))
        raise


async def _exchange(target: NativeRecoveryTargetV1, wire: bytes, *, timeout_seconds: float = 90) -> bytes:
    """One bounded attempt; a lost response never implies successful cleanup."""
    if not isinstance(wire, bytes) or not 1 <= len(wire) <= _MAX_REQUEST:
        raise ValueError("recovery request exceeds byte bound")
    if (isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float))
        or not 0.05 <= timeout_seconds <= 120):
        raise ValueError("recovery transport timeout exceeds bound")
    process = None
    material = ExitStack()
    try:
        async with asyncio.timeout(timeout_seconds):
            process = await _spawn(_snapshot_target(target, material))
            if process.stdin is None or process.stdout is None:
                raise ValueError("recovery transport pipes absent")
            process.stdin.write(wire)
            await process.stdin.drain()
            process.stdin.close()
            reply = bytearray()
            while part := await process.stdout.read(_MAX_REPLY + 1 - len(reply)):
                reply.extend(part)
                if len(reply) > _MAX_REPLY:
                    raise ValueError("recovery response exceeds byte bound")
            if await process.wait() != 0:
                raise ValueError("recovery transport failed; node outcome is unknown")
            return bytes(reply)
    finally:
        try:
            if process is not None:
                await _join(asyncio.create_task(_stop_process(process)))
        finally:
            material.close()


class NativeRecoverySender:
    """Installed management scope owns lookup; no caller-parsed evidence API."""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], installation: RetainedBuildInstallation,
        targets: tuple[NativeRecoveryTargetV1, ...],
    ) -> None:
        self._sessions = session_factory
        self._installation = installation
        self._targets = NativeRecoveryInventoryV1.model_validate_json(
            canonical_bytes(NativeRecoveryInventoryV1(targets=targets))).targets

    @classmethod
    def from_installed_inventory(cls, *, session_factory: async_sessionmaker[AsyncSession],
        installation: RetainedBuildInstallation, inventory_path: Path, inventory_sha256: str,
    ) -> Self:
        inventory = read_native_recovery_inventory(inventory_path, expected_sha256=inventory_sha256)
        return cls(session_factory=session_factory, installation=installation, targets=inventory.targets)

    async def reconcile(self, claim_operation_id: UUID) -> NativeNodeRecoveryResultV1 | None:
        async with asyncio.timeout(10):
            async with self._sessions.begin() as session:
                history = await NativeTerminalRecoveryStore(session, installation=self._installation).read(claim_operation_id)
        if history is None:
            return None
        prepared = history.preparation.request.record
        assert isinstance(prepared, NativeRecoveryPreparationV1)
        final = history.finalization.request.record if history.finalization is not None else None
        management_uid = os.getuid()
        if prepared.original_uid == management_uid or (isinstance(final, NativeInstalledAttemptV2)
            and any(item.outside <= management_uid < item.outside + item.count for item in final.uid_map)):
            raise ValueError("recovery management principal must differ from the workload identity")
        matches = [item for item in self._targets if item.installation_id == self._installation.id
            and item.pool_id == history.profile.pool_id and item.node_id == prepared.node_id
            and item.profile_sha256 == canonical_digest(history.profile)
            and item.host_sha256 == canonical_digest(history.host)]
        if len(matches) != 1:
            raise ValueError("recovery history has no unique protected target")
        request = NativeNodeRecoveryRequestV1(invocation_id=uuid4(), history=history)
        reply = await _exchange(matches[0], canonical_bytes(request))
        result = NativeNodeRecoveryResultV1.model_validate_json(reply)
        if (canonical_bytes(result) + b"\n" != reply or result.request_sha256 != canonical_digest(request)
            or (result.state == "completed") != (result.reason == "complete")):
            raise ValueError("recovery response binding changed")
        return result
