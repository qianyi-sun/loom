"""Digest-approved retirement of exact installed preflight artifact bundles."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from loom_cli.rollout.preflight_artifact_retention import (
    ARTIFACT_FILE_NAMES,
    ArtifactDirectoryIdentity,
    ArtifactFileIdentity,
    OpaqueArtifactEvidence,
    PreflightArtifactInventoryRecord,
    PreflightArtifactProtection,
    PreflightArtifactRetentionPlan,
    build_preflight_artifact_retention_plan,
)
from loom_cli.rollout.preflight_artifact_store import (
    PreflightArtifactStore,
    PreflightArtifactStoreError,
)

from .config import OperatorConfig
from .store import RequestStore, RequestStoreError

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_ARTIFACT_FILE_BYTES = 16 * 1024 * 1024
_MAX_EVIDENCE_BYTES = 8 * 1024 * 1024
_POLICY_PROTECTION_REASONS = frozenset({"batch-deferred", "grace-period", "opaque-store"})


class InstalledPreflightArtifactRetentionError(RuntimeError):
    """Raised when installed artifact retirement cannot proceed exactly."""


def _metadata_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _directory_identity(metadata: os.stat_result) -> ArtifactDirectoryIdentity:
    return ArtifactDirectoryIdentity(
        device=metadata.st_dev,
        inode=metadata.st_ino,
        owner_uid=metadata.st_uid,
        owner_gid=metadata.st_gid,
        mode=stat.S_IMODE(metadata.st_mode),
        link_count=metadata.st_nlink,
        size_bytes=metadata.st_size,
        modified_ns=metadata.st_mtime_ns,
        changed_ns=metadata.st_ctime_ns,
    )


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _require_private_directory_metadata(
    metadata: os.stat_result,
    *,
    service_uid: int,
    label: str,
) -> None:
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise InstalledPreflightArtifactRetentionError(f"{label} is unsafe")


def _require_private_file_metadata(
    metadata: os.stat_result,
    *,
    service_uid: int,
    label: str,
) -> None:
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
        or metadata.st_nlink != 1
        or not 0 < metadata.st_size <= _MAX_ARTIFACT_FILE_BYTES
    ):
        raise InstalledPreflightArtifactRetentionError(f"{label} is unsafe")


def _read_bounded_descriptor(descriptor: int, *, max_bytes: int, label: str) -> bytes:
    chunks: list[bytes] = []
    remaining = max_bytes + 1
    while remaining > 0:
        chunk = os.read(descriptor, min(64 * 1024, remaining))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    payload = b"".join(chunks)
    if not payload or len(payload) > max_bytes:
        raise InstalledPreflightArtifactRetentionError(f"{label} is unbounded")
    return payload


def _read_inventory_file(
    directory_fd: int,
    *,
    name: str,
    service_uid: int,
) -> ArtifactFileIdentity:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(name, flags, dir_fd=directory_fd)
    except OSError as exc:
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact inventory file is unavailable"
        ) from exc
    try:
        before = os.fstat(descriptor)
        _require_private_file_metadata(
            before,
            service_uid=service_uid,
            label="preflight artifact inventory file",
        )
        payload = _read_bounded_descriptor(
            descriptor,
            max_bytes=_MAX_ARTIFACT_FILE_BYTES,
            label="preflight artifact inventory file",
        )
        after = os.fstat(descriptor)
        if _metadata_identity(before) != _metadata_identity(after):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact inventory file changed during read"
            )
    finally:
        os.close(descriptor)
    return ArtifactFileIdentity(
        name=name,
        device=before.st_dev,
        inode=before.st_ino,
        owner_uid=before.st_uid,
        owner_gid=before.st_gid,
        mode=stat.S_IMODE(before.st_mode),
        link_count=before.st_nlink,
        size_bytes=before.st_size,
        modified_ns=before.st_mtime_ns,
        changed_ns=before.st_ctime_ns,
        sha256=hashlib.sha256(payload).hexdigest(),
    )


def _require_inventory_file_unchanged(
    directory_fd: int,
    identity: ArtifactFileIdentity,
    *,
    service_uid: int,
) -> None:
    try:
        observed = os.stat(identity.name, dir_fd=directory_fd, follow_symlinks=False)
    except OSError as exc:
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact inventory file changed after read"
        ) from exc
    _require_private_file_metadata(
        observed,
        service_uid=service_uid,
        label="preflight artifact inventory file",
    )
    expected = (
        identity.device,
        identity.inode,
        stat.S_IFREG | identity.mode,
        identity.link_count,
        identity.owner_uid,
        identity.owner_gid,
        identity.size_bytes,
        identity.modified_ns,
        identity.changed_ns,
    )
    if _metadata_identity(observed) != expected:
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact inventory file changed after read"
        )


def _opaque_evidence(
    root_fd: int,
    *,
    name: str,
    metadata: os.stat_result,
    service_uid: int,
) -> OpaqueArtifactEvidence:
    if stat.S_ISREG(metadata.st_mode):
        _require_private_file_metadata(
            metadata,
            service_uid=service_uid,
            label="unknown preflight artifact evidence",
        )
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(name, flags, dir_fd=root_fd)
        except OSError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "unknown preflight artifact evidence is unavailable"
            ) from exc
        try:
            opened = os.fstat(descriptor)
            if _metadata_identity(opened) != _metadata_identity(metadata):
                raise InstalledPreflightArtifactRetentionError(
                    "unknown preflight artifact evidence changed during open"
                )
            payload = _read_bounded_descriptor(
                descriptor,
                max_bytes=_MAX_ARTIFACT_FILE_BYTES,
                label="unknown preflight artifact evidence",
            )
            after = os.fstat(descriptor)
            if _metadata_identity(after) != _metadata_identity(opened):
                raise InstalledPreflightArtifactRetentionError(
                    "unknown preflight artifact evidence changed during read"
                )
        finally:
            os.close(descriptor)
        kind = "file"
        digest: str | None = hashlib.sha256(payload).hexdigest()
    elif stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        _require_private_directory_metadata(
            metadata,
            service_uid=service_uid,
            label="unknown preflight artifact directory",
        )
        kind = "directory"
        digest = None
    else:
        raise InstalledPreflightArtifactRetentionError(
            "unknown preflight artifact evidence is unsafe"
        )
    return OpaqueArtifactEvidence(
        name=name,
        kind=kind,
        device=metadata.st_dev,
        inode=metadata.st_ino,
        owner_uid=metadata.st_uid,
        owner_gid=metadata.st_gid,
        mode=stat.S_IMODE(metadata.st_mode),
        link_count=metadata.st_nlink,
        size_bytes=metadata.st_size,
        modified_ns=metadata.st_mtime_ns,
        changed_ns=metadata.st_ctime_ns,
        reason="unknown-entry",
        sha256=digest,
    )


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate preflight artifact retention evidence field")
        result[key] = value
    return result


def _ensure_private_directory(path: Path, *, service_uid: int) -> None:
    created = False
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        created = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact retention evidence directory is unavailable"
        ) from exc
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact retention evidence directory is unavailable"
        ) from exc
    _require_private_directory_metadata(
        metadata,
        service_uid=service_uid,
        label="preflight artifact retention evidence directory",
    )
    if created:
        _fsync_directory(path.parent)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, _directory_flags())
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _fsync_descriptor(descriptor: int) -> None:
    os.fsync(descriptor)


def _read_exact_evidence(path: Path, *, service_uid: int) -> tuple[dict[str, object], bytes]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact retention evidence is unavailable"
        ) from exc
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != service_uid
            or stat.S_IMODE(before.st_mode) != _PRIVATE_FILE_MODE
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_EVIDENCE_BYTES
        ):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention evidence is unsafe"
            )
        payload = _read_bounded_descriptor(
            descriptor,
            max_bytes=_MAX_EVIDENCE_BYTES,
            label="preflight artifact retention evidence",
        )
        after = os.fstat(descriptor)
        if _metadata_identity(before) != _metadata_identity(after):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention evidence changed during read"
            )
    finally:
        os.close(descriptor)
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact retention evidence is invalid"
        ) from exc
    if not isinstance(value, dict):
        raise InstalledPreflightArtifactRetentionError(
            "preflight artifact retention evidence is invalid"
        )
    return cast(dict[str, object], value), payload


def _recover_exact_evidence_link_residue(
    directory_fd: int,
    *,
    final_name: str,
    expected_payload: bytes,
    service_uid: int,
) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(final_name, flags, dir_fd=directory_fd)
    try:
        before = os.fstat(descriptor)
        if before.st_nlink == 1:
            return
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != service_uid
            or stat.S_IMODE(before.st_mode) != _PRIVATE_FILE_MODE
            or before.st_nlink != 2
            or not 0 < before.st_size <= _MAX_EVIDENCE_BYTES
        ):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention evidence link residue is unsafe"
            )
        payload = _read_bounded_descriptor(
            descriptor,
            max_bytes=_MAX_EVIDENCE_BYTES,
            label="preflight artifact retention evidence link residue",
        )
        after_read = os.fstat(descriptor)
        if _metadata_identity(before) != _metadata_identity(after_read):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention evidence link residue changed during read"
            )
        if payload != expected_payload:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention evidence link residue collided"
            )
        temporary_pattern = re.compile(rf"^\.{re.escape(final_name)}\.[0-9a-f]{{32}}\.tmp$")
        aliases: list[str] = []
        with os.scandir(directory_fd) as entries:
            for entry in entries:
                if temporary_pattern.fullmatch(entry.name) is None:
                    continue
                metadata = entry.stat(follow_symlinks=False)
                if (metadata.st_dev, metadata.st_ino) == (before.st_dev, before.st_ino):
                    aliases.append(entry.name)
        if len(aliases) != 1:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention evidence link residue is ambiguous"
            )
        alias_descriptor = os.open(aliases[0], flags, dir_fd=directory_fd)
        try:
            if _metadata_identity(os.fstat(alias_descriptor)) != _metadata_identity(before):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact retention evidence link residue drifted"
                )
        finally:
            os.close(alias_descriptor)
        os.unlink(aliases[0], dir_fd=directory_fd)
        os.fsync(directory_fd)
        after_unlink = os.fstat(descriptor)
        stable_before = (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_uid,
            before.st_gid,
            before.st_size,
            before.st_mtime_ns,
        )
        stable_after = (
            after_unlink.st_dev,
            after_unlink.st_ino,
            after_unlink.st_mode,
            after_unlink.st_uid,
            after_unlink.st_gid,
            after_unlink.st_size,
            after_unlink.st_mtime_ns,
        )
        if stable_after != stable_before or after_unlink.st_nlink != 1:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention evidence link residue did not converge"
            )
    finally:
        os.close(descriptor)


def _publish_exact_evidence(
    path: Path,
    value: Mapping[str, object],
    *,
    service_uid: int,
) -> None:
    _ensure_private_directory(path.parent, service_uid=service_uid)
    payload = _json_bytes(value)
    directory_fd = os.open(path.parent, _directory_flags())
    temporary = f".{path.name}.{uuid4().hex}.tmp"
    temporary_exists = False
    try:
        flags = (
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        descriptor = os.open(temporary, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
        temporary_exists = True
        try:
            os.fchmod(descriptor, _PRIVATE_FILE_MODE)
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != service_uid
                or metadata.st_nlink != 1
            ):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact retention temporary evidence is unsafe"
                )
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        try:
            os.link(
                temporary,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            _recover_exact_evidence_link_residue(
                directory_fd,
                final_name=path.name,
                expected_payload=payload,
                service_uid=service_uid,
            )
            existing, existing_payload = _read_exact_evidence(path, service_uid=service_uid)
            if existing != dict(value) or existing_payload != payload:
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact retention evidence collision"
                ) from None
        os.unlink(temporary, dir_fd=directory_fd)
        temporary_exists = False
        os.fsync(directory_fd)
    finally:
        if temporary_exists:
            with contextlib.suppress(OSError):
                os.unlink(temporary, dir_fd=directory_fd)
                os.fsync(directory_fd)
        os.close(directory_fd)


@dataclass(slots=True)
class InstalledPreflightArtifactRetentionService:
    """Inventory and converge one operator-approved bounded artifact plan."""

    config: OperatorConfig
    service_uid: int
    store: RequestStore
    artifact_store: PreflightArtifactStore
    collect_references: Callable[[datetime], tuple[PreflightArtifactProtection, ...]]
    now: Callable[[], datetime]

    def __post_init__(self) -> None:
        if (
            self.service_uid < 1
            or self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or self.store.root != self.config.state_root
            or self.artifact_store.state_root != self.config.state_root
            or self.artifact_store.service_uid != self.service_uid
        ):
            raise ValueError("installed preflight artifact retention authority is invalid")

    @property
    def evidence_root(self) -> Path:
        return self.config.state_root / "preflight-artifact-retention"

    @property
    def quarantine_root(self) -> Path:
        return self.config.state_root / "preflight-artifact-quarantine"

    def inventory(self) -> PreflightArtifactRetentionPlan:
        existing_claim = self.store.read_preflight_artifact_retention_claim()
        if existing_claim is not None:
            return self.load_claim(existing_claim[0])
        inventory_at = self.now()
        if not isinstance(inventory_at, datetime):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention clock is invalid"
            )
        try:
            references = self.collect_references(inventory_at)
            with self.artifact_store.shared_lifecycle_lock():
                root, records, opaque = self._snapshot()
            plan = build_preflight_artifact_retention_plan(
                root=root,
                records=records,
                references=references,
                opaque_evidence=opaque,
                inventory_at=inventory_at,
                environment=self.config.environment,
                namespace=self.config.namespace,
            )
            _publish_exact_evidence(
                self._plan_path(plan.plan_digest),
                plan.to_dict(),
                service_uid=self.service_uid,
            )
        except InstalledPreflightArtifactRetentionError:
            raise
        except (OSError, PreflightArtifactStoreError, RequestStoreError, ValueError) as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact inventory could not be collected safely"
            ) from exc
        return plan

    def load_claim(self, approved_plan_digest: str) -> PreflightArtifactRetentionPlan:
        if _SHA256_RE.fullmatch(approved_plan_digest) is None:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention approval is invalid"
            )
        try:
            value, payload = _read_exact_evidence(
                self._plan_path(approved_plan_digest),
                service_uid=self.service_uid,
            )
            plan = PreflightArtifactRetentionPlan.from_dict(value)
        except FileNotFoundError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention approval is unavailable"
            ) from exc
        except (TypeError, ValueError) as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention approval is invalid"
            ) from exc
        if payload != _json_bytes(plan.to_dict()) or plan.plan_digest != approved_plan_digest:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention approval drifted"
            )
        existing_claim = self.store.read_preflight_artifact_retention_claim()
        expected_claim = (
            plan.plan_digest,
            tuple(sorted(item.bundle_digest for item in plan.candidates)),
        )
        if existing_claim is not None and existing_claim != expected_claim:
            raise InstalledPreflightArtifactRetentionError(
                "another preflight artifact retention claim is active"
            )
        return plan

    def claim(self, plan: PreflightArtifactRetentionPlan) -> None:
        """Persist the exact bounded start/resume fence before destructive work."""
        approved = self.load_claim(plan.plan_digest)
        if approved != plan:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention approval identity drifted"
            )
        if self.store.read_active() is not None:
            raise InstalledPreflightArtifactRetentionError(
                "active rollout blocks preflight artifact retirement"
            )
        try:
            self.store.claim_preflight_artifact_retention(
                plan.plan_digest,
                tuple(item.bundle_digest for item in plan.candidates),
            )
        except RequestStoreError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention claim could not be acquired"
            ) from exc

    def apply(self, plan: PreflightArtifactRetentionPlan) -> dict[str, object]:
        """Converge only the exact durable claim through quarantine and receipts."""
        try:
            with self._execution_guard():
                approved = self.load_claim(plan.plan_digest)
                if approved != plan:
                    raise InstalledPreflightArtifactRetentionError(
                        "preflight artifact retention approval identity drifted"
                    )
                self._require_execution_claim(plan)
                with self.artifact_store.exclusive_lifecycle_lock():
                    self._require_execution_claim(plan)
                    root, records, opaque = self._snapshot()
                    quarantine_names = self._quarantine_names(plan)
                    states = self._candidate_states(
                        plan,
                        records=records,
                        quarantine_names=quarantine_names,
                    )
                    self._validate_apply_snapshot(
                        plan,
                        root=root,
                        records=records,
                        opaque=opaque,
                        states=states,
                    )
                    _ensure_private_directory(
                        self.quarantine_root,
                        service_uid=self.service_uid,
                    )
                    artifact_root_fd = os.open(self.artifact_store.root, _directory_flags())
                    quarantine_root_fd = os.open(self.quarantine_root, _directory_flags())
                    try:
                        for record in plan.candidates:
                            self._require_execution_claim(plan)
                            self._require_reference_snapshot(plan)
                            self._converge_candidate(
                                plan,
                                record,
                                artifact_root_fd=artifact_root_fd,
                                quarantine_root_fd=quarantine_root_fd,
                            )
                        self._require_execution_claim(plan)
                        result = self._applied_document(plan)
                        _publish_exact_evidence(
                            self._applied_path(plan.plan_digest),
                            result,
                            service_uid=self.service_uid,
                        )
                        self._validate_final_state(plan)
                        self.store.clear_preflight_artifact_retention_claim(plan.plan_digest)
                    finally:
                        os.close(quarantine_root_fd)
                        os.close(artifact_root_fd)
                return result
        except InstalledPreflightArtifactRetentionError:
            raise
        except (OSError, PreflightArtifactStoreError, RequestStoreError, ValueError) as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention convergence failed closed"
            ) from exc

    def _require_execution_claim(self, plan: PreflightArtifactRetentionPlan) -> None:
        expected = (
            plan.plan_digest,
            tuple(sorted(item.bundle_digest for item in plan.candidates)),
        )
        if self.store.read_preflight_artifact_retention_claim() != expected:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention execution claim drifted"
            )
        if self.store.read_active() is not None:
            raise InstalledPreflightArtifactRetentionError(
                "active rollout blocks preflight artifact retirement"
            )

    @contextmanager
    def _execution_guard(self) -> Iterator[None]:
        _ensure_private_directory(self.evidence_root, service_uid=self.service_uid)
        path = self.evidence_root / ".apply.lock"
        create_flags = (
            os.O_RDWR
            | os.O_CREAT
            | os.O_EXCL
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        existing_flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            try:
                descriptor = os.open(path, create_flags, _PRIVATE_FILE_MODE)
                os.fchmod(descriptor, _PRIVATE_FILE_MODE)
                os.fsync(descriptor)
                _fsync_directory(path.parent)
            except FileExistsError:
                descriptor = os.open(path, existing_flags)
        except OSError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention execution lock is unavailable"
            ) from exc
        locked = False
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
                or metadata.st_nlink != 1
            ):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact retention execution lock is unsafe"
                )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
                locked = True
            except BlockingIOError:
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact retention execution is already running"
                ) from None
            yield
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    def _planned_references(
        self,
        plan: PreflightArtifactRetentionPlan,
    ) -> tuple[PreflightArtifactProtection, ...]:
        references: list[PreflightArtifactProtection] = []
        for protection in plan.protections:
            reasons = tuple(
                reason for reason in protection.reasons if reason not in _POLICY_PROTECTION_REASONS
            )
            if reasons:
                references.append(PreflightArtifactProtection(protection.bundle_digest, reasons))
        return tuple(references)

    def _require_reference_snapshot(self, plan: PreflightArtifactRetentionPlan) -> None:
        observed = tuple(
            sorted(self.collect_references(self.now()), key=lambda item: item.bundle_digest)
        )
        if observed != self._planned_references(plan):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact reference inventory drifted before retirement"
            )

    def _candidate_states(
        self,
        plan: PreflightArtifactRetentionPlan,
        *,
        records: tuple[PreflightArtifactInventoryRecord, ...],
        quarantine_names: frozenset[str],
    ) -> dict[str, tuple[bool, bool, bool]]:
        sources = {item.bundle_digest for item in records}
        states: dict[str, tuple[bool, bool, bool]] = {}
        for record in plan.candidates:
            quarantine = self._quarantine_name(plan, record) in quarantine_names
            receipt = self.store.read_preflight_artifact_retirement_receipt(
                record.bundle_digest,
                plan_sha256=plan.plan_digest,
                inventory_record_sha256=record.record_digest,
            )
            source = record.bundle_digest in sources
            if (source and quarantine) or (source and receipt):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact source was recreated or duplicated after retirement"
                )
            if not source and not quarantine and not receipt:
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact disappeared without quarantine or receipt"
                )
            states[record.bundle_digest] = (source, quarantine, receipt)
        return states

    def _validate_apply_snapshot(
        self,
        plan: PreflightArtifactRetentionPlan,
        *,
        root: ArtifactDirectoryIdentity,
        records: tuple[PreflightArtifactInventoryRecord, ...],
        opaque: tuple[OpaqueArtifactEvidence, ...],
        states: Mapping[str, tuple[bool, bool, bool]],
    ) -> None:
        self._require_reference_snapshot(plan)
        expected = {item.bundle_digest: item for item in (*plan.candidates, *plan.protected)}
        observed = {item.bundle_digest: item for item in records}
        if (
            opaque != plan.opaque_evidence
            or not set(observed).issubset(expected)
            or any(expected[digest] != record for digest, record in observed.items())
            or any(item.bundle_digest not in observed for item in plan.protected)
            or any(
                state[0] != (candidate.bundle_digest in observed)
                for candidate in plan.candidates
                for state in (states[candidate.bundle_digest],)
            )
        ):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact inventory drifted before retirement"
            )
        missing = sum(1 for source, _quarantine, _receipt in states.values() if not source)
        same_core = (
            root.device,
            root.inode,
            root.owner_uid,
            root.owner_gid,
            root.mode,
        ) == (
            plan.root.device,
            plan.root.inode,
            plan.root.owner_uid,
            plan.root.owner_gid,
            plan.root.mode,
        )
        if not same_core or root.link_count != plan.root.link_count - missing:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact root identity drifted before retirement"
            )
        if missing == 0 and root != plan.root:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact root metadata drifted before retirement"
            )

    def _quarantine_names(
        self,
        plan: PreflightArtifactRetentionPlan,
    ) -> frozenset[str]:
        try:
            metadata = self.quarantine_root.lstat()
        except FileNotFoundError:
            return frozenset()
        except OSError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact quarantine is unavailable"
            ) from exc
        _require_private_directory_metadata(
            metadata,
            service_uid=self.service_uid,
            label="preflight artifact quarantine",
        )
        descriptor = os.open(self.quarantine_root, _directory_flags())
        try:
            opened = os.fstat(descriptor)
            if _metadata_identity(metadata) != _metadata_identity(opened):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact quarantine changed during open"
                )
            with os.scandir(descriptor) as entries:
                names = frozenset(entry.name for entry in entries)
            after = os.fstat(descriptor)
            if _metadata_identity(opened) != _metadata_identity(after):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact quarantine changed during inventory"
                )
        finally:
            os.close(descriptor)
        expected = {self._quarantine_name(plan, item) for item in plan.candidates}
        if not names <= expected:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact quarantine contains unknown authority"
            )
        return names

    def _converge_candidate(
        self,
        plan: PreflightArtifactRetentionPlan,
        record: PreflightArtifactInventoryRecord,
        *,
        artifact_root_fd: int,
        quarantine_root_fd: int,
    ) -> None:
        quarantine_name = self._quarantine_name(plan, record)
        receipt = self.store.read_preflight_artifact_retirement_receipt(
            record.bundle_digest,
            plan_sha256=plan.plan_digest,
            inventory_record_sha256=record.record_digest,
        )
        source = self._entry_exists(artifact_root_fd, record.bundle_digest)
        quarantine = self._entry_exists(quarantine_root_fd, quarantine_name)
        if (source and quarantine) or (source and receipt):
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact source and retirement authority conflict"
            )
        if not source and not quarantine and not receipt:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact disappeared without an exact receipt"
            )
        if source:
            discovered = os.stat(
                record.bundle_digest,
                dir_fd=artifact_root_fd,
                follow_symlinks=False,
            )
            observed = self._inventory_bundle(
                artifact_root_fd,
                bundle_digest=record.bundle_digest,
                discovered=discovered,
            )
            if observed != record:
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact candidate drifted before quarantine"
                )
            os.rename(
                record.bundle_digest,
                quarantine_name,
                src_dir_fd=artifact_root_fd,
                dst_dir_fd=quarantine_root_fd,
            )
            _fsync_descriptor(artifact_root_fd)
            _fsync_descriptor(quarantine_root_fd)
            quarantine = True
        if quarantine:
            self._converge_quarantine(
                plan,
                record,
                quarantine_name=quarantine_name,
                quarantine_root_fd=quarantine_root_fd,
                receipt=receipt,
            )

    def _converge_quarantine(
        self,
        plan: PreflightArtifactRetentionPlan,
        record: PreflightArtifactInventoryRecord,
        *,
        quarantine_name: str,
        quarantine_root_fd: int,
        receipt: bool,
    ) -> None:
        descriptor = os.open(
            quarantine_name,
            _directory_flags(),
            dir_fd=quarantine_root_fd,
        )
        try:
            before = os.fstat(descriptor)
            expected_directory = record.directory
            if (
                not stat.S_ISDIR(before.st_mode)
                or before.st_dev != expected_directory.device
                or before.st_ino != expected_directory.inode
                or before.st_uid != expected_directory.owner_uid
                or before.st_gid != expected_directory.owner_gid
                or stat.S_IMODE(before.st_mode) != expected_directory.mode
                or before.st_nlink != expected_directory.link_count
            ):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact quarantine identity drifted"
                )
            with os.scandir(descriptor) as entries:
                names = tuple(sorted(entry.name for entry in entries))
            suffixes = tuple(ARTIFACT_FILE_NAMES[index:] for index in range(5))
            if names not in suffixes or (receipt and names):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact quarantine contents drifted"
                )
            by_name = {item.name: item for item in record.files}
            for name in names:
                observed = _read_inventory_file(
                    descriptor,
                    name=name,
                    service_uid=self.service_uid,
                )
                if observed != by_name[name]:
                    raise InstalledPreflightArtifactRetentionError(
                        "preflight artifact quarantine file drifted"
                    )
            for name in names:
                _require_inventory_file_unchanged(
                    descriptor,
                    by_name[name],
                    service_uid=self.service_uid,
                )
                os.unlink(name, dir_fd=descriptor)
                _fsync_descriptor(descriptor)
            after = os.fstat(descriptor)
            if before.st_ino != after.st_ino or after.st_nlink != before.st_nlink:
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact quarantine directory drifted"
                )
        finally:
            os.close(descriptor)
        if not receipt:
            self.store.publish_preflight_artifact_retirement_receipt(
                record.bundle_digest,
                plan_sha256=plan.plan_digest,
                inventory_record_sha256=record.record_digest,
            )
        os.rmdir(quarantine_name, dir_fd=quarantine_root_fd)
        _fsync_descriptor(quarantine_root_fd)

    def _validate_final_state(self, plan: PreflightArtifactRetentionPlan) -> None:
        self._require_execution_claim(plan)
        self._require_reference_snapshot(plan)
        quarantine_names = self._quarantine_names(plan)
        if quarantine_names:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact quarantine remains after convergence"
            )
        for record in plan.candidates:
            if (self.artifact_store.root / record.bundle_digest).exists():
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact source remains after retirement"
                )
            if not self.store.read_preflight_artifact_retirement_receipt(
                record.bundle_digest,
                plan_sha256=plan.plan_digest,
                inventory_record_sha256=record.record_digest,
            ):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact retirement receipt is missing"
                )
        root, records, opaque = self._snapshot()
        states = {item.bundle_digest: (False, False, True) for item in plan.candidates}
        self._validate_apply_snapshot(
            plan,
            root=root,
            records=records,
            opaque=opaque,
            states=states,
        )

    def _applied_document(
        self,
        plan: PreflightArtifactRetentionPlan,
    ) -> dict[str, object]:
        return {
            "approved_plan_sha256": plan.plan_digest,
            "environment": self.config.environment,
            "namespace": self.config.namespace,
            "retirements": [
                {
                    "bundle_digest": item.bundle_digest,
                    "inventory_record_sha256": item.record_digest,
                }
                for item in plan.candidates
            ],
            "schema_version": 1,
        }

    @staticmethod
    def _entry_exists(directory_fd: int, name: str) -> bool:
        try:
            os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact retention entry is unavailable"
            ) from exc
        return True

    @staticmethod
    def _quarantine_name(
        plan: PreflightArtifactRetentionPlan,
        record: PreflightArtifactInventoryRecord,
    ) -> str:
        return f"{plan.plan_digest}.{record.bundle_digest}"

    def _applied_path(self, digest: str) -> Path:
        return self.evidence_root / f"{digest}.applied.json"

    def _snapshot(
        self,
    ) -> tuple[
        ArtifactDirectoryIdentity,
        tuple[PreflightArtifactInventoryRecord, ...],
        tuple[OpaqueArtifactEvidence, ...],
    ]:
        try:
            root_fd = os.open(self.artifact_store.root, _directory_flags())
        except OSError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact root is unavailable"
            ) from exc
        records: list[PreflightArtifactInventoryRecord] = []
        opaque: list[OpaqueArtifactEvidence] = []
        try:
            root_before = os.fstat(root_fd)
            _require_private_directory_metadata(
                root_before,
                service_uid=self.service_uid,
                label="preflight artifact root",
            )
            with os.scandir(root_fd) as entries:
                for entry in entries:
                    try:
                        metadata = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise InstalledPreflightArtifactRetentionError(
                            "preflight artifact root entry is unavailable"
                        ) from exc
                    if _SHA256_RE.fullmatch(entry.name) is None:
                        opaque.append(
                            _opaque_evidence(
                                root_fd,
                                name=entry.name,
                                metadata=metadata,
                                service_uid=self.service_uid,
                            )
                        )
                        continue
                    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                        raise InstalledPreflightArtifactRetentionError(
                            "preflight artifact bundle entry is unsafe"
                        )
                    records.append(
                        self._inventory_bundle(
                            root_fd,
                            bundle_digest=entry.name,
                            discovered=metadata,
                        )
                    )
            root_after = os.fstat(root_fd)
            if _metadata_identity(root_before) != _metadata_identity(root_after):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact root changed during inventory"
                )
        finally:
            os.close(root_fd)
        return (
            _directory_identity(root_before),
            tuple(sorted(records, key=lambda item: item.bundle_digest)),
            tuple(sorted(opaque, key=lambda item: item.name)),
        )

    def _inventory_bundle(
        self,
        root_fd: int,
        *,
        bundle_digest: str,
        discovered: os.stat_result,
    ) -> PreflightArtifactInventoryRecord:
        try:
            directory_fd = os.open(bundle_digest, _directory_flags(), dir_fd=root_fd)
        except OSError as exc:
            raise InstalledPreflightArtifactRetentionError(
                "preflight artifact bundle is unavailable"
            ) from exc
        try:
            before = os.fstat(directory_fd)
            _require_private_directory_metadata(
                before,
                service_uid=self.service_uid,
                label="preflight artifact bundle",
            )
            if _metadata_identity(before) != _metadata_identity(discovered):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact bundle changed during open"
                )
            with os.scandir(directory_fd) as entries:
                names = tuple(sorted(entry.name for entry in entries))
            if names != ARTIFACT_FILE_NAMES:
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact bundle file set is inexact"
                )
            files = tuple(
                _read_inventory_file(
                    directory_fd,
                    name=name,
                    service_uid=self.service_uid,
                )
                for name in ARTIFACT_FILE_NAMES
            )
            self.artifact_store.read(bundle_digest)
            for identity in files:
                _require_inventory_file_unchanged(
                    directory_fd,
                    identity,
                    service_uid=self.service_uid,
                )
            after = os.fstat(directory_fd)
            if _metadata_identity(before) != _metadata_identity(after):
                raise InstalledPreflightArtifactRetentionError(
                    "preflight artifact bundle changed during inventory"
                )
        finally:
            os.close(directory_fd)
        return PreflightArtifactInventoryRecord(
            bundle_digest=bundle_digest,
            directory=_directory_identity(before),
            files=files,
        )

    def _plan_path(self, digest: str) -> Path:
        return self.evidence_root / f"{digest}.plan.json"


__all__ = [
    "InstalledPreflightArtifactRetentionError",
    "InstalledPreflightArtifactRetentionService",
]
