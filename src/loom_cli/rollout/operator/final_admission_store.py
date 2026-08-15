"""Immutable admission evidence needed to resume after protected apply."""

from __future__ import annotations

import json
import os
import stat
from collections.abc import Mapping
from pathlib import Path
from uuid import uuid4

from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmission
from loom_cli.rollout.preflight_contract import CheckExecution, PreflightAttestation

from .model import validate_safe_identifier

_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_EVIDENCE_BYTES = 512 * 1024


class FinalAdmissionStoreError(RuntimeError):
    pass


class FinalAdmissionStore:
    """Publish the exact pre-apply admission once for one attributed attempt."""

    def __init__(
        self,
        state_root: Path,
        *,
        request_id: str,
        attempt_number: int,
        service_uid: int | None = None,
    ) -> None:
        self.service_uid = os.geteuid() if service_uid is None else service_uid
        self.request_id = validate_safe_identifier(request_id, "request_id")
        self.attempt_number = attempt_number
        if (
            not state_root.is_absolute()
            or ".." in state_root.parts
            or type(attempt_number) is not int
            or attempt_number < 1
            or self.service_uid < 0
        ):
            raise FinalAdmissionStoreError("final admission store authority is invalid")
        self.attempt_root = (
            state_root / "requests" / self.request_id / "attempts" / str(attempt_number)
        )
        self.path = self.attempt_root / "final-admission.json"

    def publish(self, admission: FinalAttestationAdmission) -> Path:
        _require_directory(self.attempt_root, uid=self.service_uid)
        payload = _encode(admission)
        try:
            existing = self.read(admission.attestation)
        except FileNotFoundError:
            pass
        else:
            if not _same_evidence(existing, admission):
                raise FinalAdmissionStoreError("final admission evidence cannot be replaced")
            return self.path

        directory_fd = _open_directory(self.attempt_root)
        temporary = f".{self.path.name}.{uuid4().hex}.tmp"
        created = False
        try:
            fd = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                _PRIVATE_FILE_MODE,
                dir_fd=directory_fd,
            )
            created = True
            try:
                os.fchmod(fd, _PRIVATE_FILE_MODE)
                _write_all(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.link(
                temporary,
                self.path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(temporary, dir_fd=directory_fd)
            created = False
            os.fsync(directory_fd)
        except FileExistsError:
            if not _same_evidence(self.read(admission.attestation), admission):
                raise FinalAdmissionStoreError(
                    "final admission evidence cannot be replaced"
                ) from None
        except OSError as exc:
            raise FinalAdmissionStoreError("could not publish final admission evidence") from exc
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)
        return self.path

    def read(self, attestation: PreflightAttestation) -> FinalAttestationAdmission:
        fd = os.open(
            self.path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
                or metadata.st_nlink != 1
                or metadata.st_size > _MAX_EVIDENCE_BYTES
            ):
                raise FinalAdmissionStoreError("final admission evidence authority is unsafe")
            payload = os.read(fd, _MAX_EVIDENCE_BYTES + 1)
        finally:
            os.close(fd)
        if len(payload) > _MAX_EVIDENCE_BYTES:
            raise FinalAdmissionStoreError("final admission evidence is too large")
        try:
            raw = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
            if not isinstance(raw, Mapping) or set(raw) != {
                "schema_version",
                "attestation_digest",
                "tier0_executions",
                "tier2_executions",
            }:
                raise ValueError("evidence must be an exact object")
            if raw["schema_version"] != 1:
                raise ValueError("evidence schema is unsupported")
            if raw["attestation_digest"] != attestation.attestation_digest:
                raise ValueError("attestation identity drifted")
            tier0 = _executions(raw["tier0_executions"])
            tier2 = _executions(raw["tier2_executions"])
            admission = FinalAttestationAdmission(attestation, tier0, tier2)
            if any(
                attestation.check_implementation_digests.get(execution.check_id)
                != execution.implementation_digest
                for execution in (*tier0, *tier2)
            ):
                raise ValueError("admission implementation identity drifted")
            return admission
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            raise FinalAdmissionStoreError("final admission evidence is invalid") from exc


def _encode(admission: FinalAttestationAdmission) -> bytes:
    payload = {
        "schema_version": 1,
        "attestation_digest": admission.attestation.attestation_digest,
        "tier0_executions": [execution.to_dict() for execution in admission.tier0_executions],
        "tier2_executions": [execution.to_dict() for execution in admission.tier2_executions],
    }
    encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(encoded) > _MAX_EVIDENCE_BYTES:
        raise FinalAdmissionStoreError("final admission evidence is too large")
    return encoded


def _same_evidence(
    left: FinalAttestationAdmission,
    right: FinalAttestationAdmission,
) -> bool:
    return (
        left.attestation == right.attestation
        and left.tier0_executions == right.tier0_executions
        and left.tier2_executions == right.tier2_executions
    )


def _executions(value: object) -> tuple[CheckExecution, ...]:
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(item, Mapping) for item in value)
    ):
        raise ValueError("admission execution list is invalid")
    return tuple(CheckExecution.from_dict(item) for item in value)


def _require_directory(path: Path, *, uid: int) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise FinalAdmissionStoreError("final admission directory authority is unsafe")


def _open_directory(path: Path) -> int:
    return os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )


def _write_all(fd: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(fd, view)
        if written <= 0:
            raise OSError("short write")
        view = view[written:]


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


__all__ = ["FinalAdmissionStore", "FinalAdmissionStoreError"]
