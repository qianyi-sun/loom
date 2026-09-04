"""Strict durable records for protected capacity-manager configuration rollback."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID, uuid4

from .model import validate_safe_identifier

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_UUID_RE = r"([0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})"
_RECORD_PREFIX = ".capacity-manager-configuration-compensation-"
_INTENT_FILE_RE = re.compile(rf"^{re.escape(_RECORD_PREFIX)}{_UUID_RE}-intent\.json$")
_TERMINAL_FILE_RE = re.compile(rf"^{re.escape(_RECORD_PREFIX)}{_UUID_RE}-terminal\.json$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_RECORD_BYTES = 64 * 1024
_MAX_DIRECTORY_ENTRIES = 4096


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()


def _canonical_json_bytes(value: dict[str, object]) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate key: {key}")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class CapacityManagerConfigurationCompensationIntentRecord:
    schema_version: int
    request_id: str
    attempt_number: int
    plan_digest: str
    activation_idempotency_key: UUID
    activation_request_digest: str
    target_configuration_epoch: int
    target_configuration_digest: str
    target_configuration_evidence_digest: str
    predecessor_configuration_epoch: int
    predecessor_configuration_digest: str
    predecessor_configuration_evidence_digest: str
    backup_lease_digest: str
    rollback_idempotency_key: UUID
    rollback_request_digest: str
    rollback_evidence_sha256: str
    record_digest: str

    def __post_init__(self) -> None:
        _validate_common_record(
            schema_version=self.schema_version,
            request_id=self.request_id,
            attempt_number=self.attempt_number,
            plan_digest=self.plan_digest,
            activation_idempotency_key=self.activation_idempotency_key,
            activation_request_digest=self.activation_request_digest,
            target_configuration_epoch=self.target_configuration_epoch,
            target_configuration_digest=self.target_configuration_digest,
            target_configuration_evidence_digest=self.target_configuration_evidence_digest,
            predecessor_configuration_epoch=self.predecessor_configuration_epoch,
            predecessor_configuration_digest=self.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=self.predecessor_configuration_evidence_digest,
            backup_lease_digest=self.backup_lease_digest,
            rollback_idempotency_key=self.rollback_idempotency_key,
            rollback_request_digest=self.rollback_request_digest,
            rollback_evidence_sha256=self.rollback_evidence_sha256,
            record_digest=self.record_digest,
        )

    @classmethod
    def build(
        cls,
        *,
        request_id: str,
        attempt_number: int,
        plan_digest: str,
        activation_idempotency_key: UUID,
        activation_request_digest: str,
        target_configuration_epoch: int,
        target_configuration_digest: str,
        target_configuration_evidence_digest: str,
        predecessor_configuration_epoch: int,
        predecessor_configuration_digest: str,
        predecessor_configuration_evidence_digest: str,
        backup_lease_digest: str,
        rollback_idempotency_key: UUID,
        rollback_request_digest: str,
        rollback_evidence_sha256: str,
    ) -> CapacityManagerConfigurationCompensationIntentRecord:
        payload = {
            "schema_version": 1,
            "request_id": request_id,
            "attempt_number": attempt_number,
            "plan_digest": plan_digest,
            "activation_idempotency_key": str(activation_idempotency_key),
            "activation_request_digest": activation_request_digest,
            "target_configuration_epoch": target_configuration_epoch,
            "target_configuration_digest": target_configuration_digest,
            "target_configuration_evidence_digest": target_configuration_evidence_digest,
            "predecessor_configuration_epoch": predecessor_configuration_epoch,
            "predecessor_configuration_digest": predecessor_configuration_digest,
            "predecessor_configuration_evidence_digest": (
                predecessor_configuration_evidence_digest
            ),
            "backup_lease_digest": backup_lease_digest,
            "rollback_idempotency_key": str(rollback_idempotency_key),
            "rollback_request_digest": rollback_request_digest,
            "rollback_evidence_sha256": rollback_evidence_sha256,
        }
        return cls.from_dict({**payload, "record_digest": _hash_json(payload)})

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "attempt_number": self.attempt_number,
            "plan_digest": self.plan_digest,
            "activation_idempotency_key": str(self.activation_idempotency_key),
            "activation_request_digest": self.activation_request_digest,
            "target_configuration_epoch": self.target_configuration_epoch,
            "target_configuration_digest": self.target_configuration_digest,
            "target_configuration_evidence_digest": self.target_configuration_evidence_digest,
            "predecessor_configuration_epoch": self.predecessor_configuration_epoch,
            "predecessor_configuration_digest": self.predecessor_configuration_digest,
            "predecessor_configuration_evidence_digest": (
                self.predecessor_configuration_evidence_digest
            ),
            "backup_lease_digest": self.backup_lease_digest,
            "rollback_idempotency_key": str(self.rollback_idempotency_key),
            "rollback_request_digest": self.rollback_request_digest,
            "rollback_evidence_sha256": self.rollback_evidence_sha256,
        }

    def binding_payload(self) -> dict[str, object]:
        return self.payload()

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "record_digest": self.record_digest}

    @classmethod
    def from_dict(
        cls, value: dict[str, object] | os.PathLike[str] | object
    ) -> CapacityManagerConfigurationCompensationIntentRecord:
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected capacity manager compensation record fields are invalid")
        record = cls(
            schema_version=_integer(value, "schema_version"),
            request_id=_string(value, "request_id"),
            attempt_number=_integer(value, "attempt_number"),
            plan_digest=_string(value, "plan_digest"),
            activation_idempotency_key=_uuid(value, "activation_idempotency_key"),
            activation_request_digest=_string(value, "activation_request_digest"),
            target_configuration_epoch=_integer(value, "target_configuration_epoch"),
            target_configuration_digest=_string(value, "target_configuration_digest"),
            target_configuration_evidence_digest=_string(
                value, "target_configuration_evidence_digest"
            ),
            predecessor_configuration_epoch=_integer(value, "predecessor_configuration_epoch"),
            predecessor_configuration_digest=_string(value, "predecessor_configuration_digest"),
            predecessor_configuration_evidence_digest=_string(
                value, "predecessor_configuration_evidence_digest"
            ),
            backup_lease_digest=_string(value, "backup_lease_digest"),
            rollback_idempotency_key=_uuid(value, "rollback_idempotency_key"),
            rollback_request_digest=_string(value, "rollback_request_digest"),
            rollback_evidence_sha256=_string(value, "rollback_evidence_sha256"),
            record_digest=_string(value, "record_digest"),
        )
        if _hash_json(record.payload()) != record.record_digest:
            raise ValueError("protected capacity manager compensation record content drifted")
        return record

    def to_bytes(self) -> bytes:
        payload = _canonical_json_bytes(self.to_dict())
        _validate_record_size(payload)
        return payload

    @classmethod
    def from_bytes(cls, payload: bytes) -> CapacityManagerConfigurationCompensationIntentRecord:
        value = _strict_json_object(payload)
        record = cls.from_dict(value)
        if payload != record.to_bytes():
            raise ValueError("protected capacity manager compensation encoding is not canonical")
        return record


@dataclass(frozen=True, slots=True)
class CapacityManagerConfigurationCompensationRecord:
    schema_version: int
    request_id: str
    attempt_number: int
    plan_digest: str
    activation_idempotency_key: UUID
    activation_request_digest: str
    target_configuration_epoch: int
    target_configuration_digest: str
    target_configuration_evidence_digest: str
    predecessor_configuration_epoch: int
    predecessor_configuration_digest: str
    predecessor_configuration_evidence_digest: str
    backup_lease_digest: str
    rollback_idempotency_key: UUID
    rollback_request_digest: str
    rollback_evidence_sha256: str
    resulting_configuration_epoch: int
    resulting_configuration_digest: str
    resulting_configuration_evidence_digest: str
    record_digest: str

    def __post_init__(self) -> None:
        _validate_common_record(
            schema_version=self.schema_version,
            request_id=self.request_id,
            attempt_number=self.attempt_number,
            plan_digest=self.plan_digest,
            activation_idempotency_key=self.activation_idempotency_key,
            activation_request_digest=self.activation_request_digest,
            target_configuration_epoch=self.target_configuration_epoch,
            target_configuration_digest=self.target_configuration_digest,
            target_configuration_evidence_digest=self.target_configuration_evidence_digest,
            predecessor_configuration_epoch=self.predecessor_configuration_epoch,
            predecessor_configuration_digest=self.predecessor_configuration_digest,
            predecessor_configuration_evidence_digest=self.predecessor_configuration_evidence_digest,
            backup_lease_digest=self.backup_lease_digest,
            rollback_idempotency_key=self.rollback_idempotency_key,
            rollback_request_digest=self.rollback_request_digest,
            rollback_evidence_sha256=self.rollback_evidence_sha256,
            record_digest=self.record_digest,
        )
        if (
            any(
                type(value) is not int or value < 1
                for value in (self.resulting_configuration_epoch,)
            )
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.resulting_configuration_digest,
                    self.resulting_configuration_evidence_digest,
                )
            )
            or self.resulting_configuration_epoch != self.target_configuration_epoch + 1
        ):
            raise ValueError("protected capacity manager compensation record is invalid")

    @classmethod
    def build(
        cls,
        *,
        request_id: str,
        attempt_number: int,
        plan_digest: str,
        activation_idempotency_key: UUID,
        activation_request_digest: str,
        target_configuration_epoch: int,
        target_configuration_digest: str,
        target_configuration_evidence_digest: str,
        predecessor_configuration_epoch: int,
        predecessor_configuration_digest: str,
        predecessor_configuration_evidence_digest: str,
        backup_lease_digest: str,
        rollback_idempotency_key: UUID,
        rollback_request_digest: str,
        rollback_evidence_sha256: str,
        resulting_configuration_epoch: int,
        resulting_configuration_digest: str,
        resulting_configuration_evidence_digest: str,
    ) -> CapacityManagerConfigurationCompensationRecord:
        payload = {
            "schema_version": 1,
            "request_id": request_id,
            "attempt_number": attempt_number,
            "plan_digest": plan_digest,
            "activation_idempotency_key": str(activation_idempotency_key),
            "activation_request_digest": activation_request_digest,
            "target_configuration_epoch": target_configuration_epoch,
            "target_configuration_digest": target_configuration_digest,
            "target_configuration_evidence_digest": target_configuration_evidence_digest,
            "predecessor_configuration_epoch": predecessor_configuration_epoch,
            "predecessor_configuration_digest": predecessor_configuration_digest,
            "predecessor_configuration_evidence_digest": (
                predecessor_configuration_evidence_digest
            ),
            "backup_lease_digest": backup_lease_digest,
            "rollback_idempotency_key": str(rollback_idempotency_key),
            "rollback_request_digest": rollback_request_digest,
            "rollback_evidence_sha256": rollback_evidence_sha256,
            "resulting_configuration_epoch": resulting_configuration_epoch,
            "resulting_configuration_digest": resulting_configuration_digest,
            "resulting_configuration_evidence_digest": resulting_configuration_evidence_digest,
        }
        return cls.from_dict({**payload, "record_digest": _hash_json(payload)})

    def payload(self) -> dict[str, object]:
        return {
            **CapacityManagerConfigurationCompensationIntentRecord(
                schema_version=self.schema_version,
                request_id=self.request_id,
                attempt_number=self.attempt_number,
                plan_digest=self.plan_digest,
                activation_idempotency_key=self.activation_idempotency_key,
                activation_request_digest=self.activation_request_digest,
                target_configuration_epoch=self.target_configuration_epoch,
                target_configuration_digest=self.target_configuration_digest,
                target_configuration_evidence_digest=self.target_configuration_evidence_digest,
                predecessor_configuration_epoch=self.predecessor_configuration_epoch,
                predecessor_configuration_digest=self.predecessor_configuration_digest,
                predecessor_configuration_evidence_digest=self.predecessor_configuration_evidence_digest,
                backup_lease_digest=self.backup_lease_digest,
                rollback_idempotency_key=self.rollback_idempotency_key,
                rollback_request_digest=self.rollback_request_digest,
                rollback_evidence_sha256=self.rollback_evidence_sha256,
                record_digest=self.record_digest,
            ).payload(),
            "resulting_configuration_epoch": self.resulting_configuration_epoch,
            "resulting_configuration_digest": self.resulting_configuration_digest,
            "resulting_configuration_evidence_digest": self.resulting_configuration_evidence_digest,
        }

    def binding_payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "request_id": self.request_id,
            "attempt_number": self.attempt_number,
            "plan_digest": self.plan_digest,
            "activation_idempotency_key": str(self.activation_idempotency_key),
            "activation_request_digest": self.activation_request_digest,
            "target_configuration_epoch": self.target_configuration_epoch,
            "target_configuration_digest": self.target_configuration_digest,
            "target_configuration_evidence_digest": self.target_configuration_evidence_digest,
            "predecessor_configuration_epoch": self.predecessor_configuration_epoch,
            "predecessor_configuration_digest": self.predecessor_configuration_digest,
            "predecessor_configuration_evidence_digest": (
                self.predecessor_configuration_evidence_digest
            ),
            "backup_lease_digest": self.backup_lease_digest,
            "rollback_idempotency_key": str(self.rollback_idempotency_key),
            "rollback_request_digest": self.rollback_request_digest,
            "rollback_evidence_sha256": self.rollback_evidence_sha256,
        }

    def to_dict(self) -> dict[str, object]:
        return {**self.payload(), "record_digest": self.record_digest}

    @classmethod
    def from_dict(
        cls, value: dict[str, object] | os.PathLike[str] | object
    ) -> CapacityManagerConfigurationCompensationRecord:
        if not isinstance(value, dict) or set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected capacity manager compensation record fields are invalid")
        record = cls(
            schema_version=_integer(value, "schema_version"),
            request_id=_string(value, "request_id"),
            attempt_number=_integer(value, "attempt_number"),
            plan_digest=_string(value, "plan_digest"),
            activation_idempotency_key=_uuid(value, "activation_idempotency_key"),
            activation_request_digest=_string(value, "activation_request_digest"),
            target_configuration_epoch=_integer(value, "target_configuration_epoch"),
            target_configuration_digest=_string(value, "target_configuration_digest"),
            target_configuration_evidence_digest=_string(
                value, "target_configuration_evidence_digest"
            ),
            predecessor_configuration_epoch=_integer(value, "predecessor_configuration_epoch"),
            predecessor_configuration_digest=_string(value, "predecessor_configuration_digest"),
            predecessor_configuration_evidence_digest=_string(
                value, "predecessor_configuration_evidence_digest"
            ),
            backup_lease_digest=_string(value, "backup_lease_digest"),
            rollback_idempotency_key=_uuid(value, "rollback_idempotency_key"),
            rollback_request_digest=_string(value, "rollback_request_digest"),
            rollback_evidence_sha256=_string(value, "rollback_evidence_sha256"),
            resulting_configuration_epoch=_integer(value, "resulting_configuration_epoch"),
            resulting_configuration_digest=_string(value, "resulting_configuration_digest"),
            resulting_configuration_evidence_digest=_string(
                value, "resulting_configuration_evidence_digest"
            ),
            record_digest=_string(value, "record_digest"),
        )
        if _hash_json(record.payload()) != record.record_digest:
            raise ValueError("protected capacity manager compensation record content drifted")
        return record

    def to_bytes(self) -> bytes:
        payload = _canonical_json_bytes(self.to_dict())
        _validate_record_size(payload)
        return payload

    @classmethod
    def from_bytes(cls, payload: bytes) -> CapacityManagerConfigurationCompensationRecord:
        value = _strict_json_object(payload)
        record = cls.from_dict(value)
        if payload != record.to_bytes():
            raise ValueError("protected capacity manager compensation encoding is not canonical")
        return record


@dataclass(frozen=True, slots=True)
class CapacityManagerConfigurationCompensationStore:
    root: Path
    service_uid: int

    def __post_init__(self) -> None:
        if (
            not self.root.is_absolute()
            or ".." in self.root.parts
            or type(self.service_uid) is not int
            or self.service_uid < 0
        ):
            raise ValueError("protected capacity manager compensation root is invalid")

    def intent_path_for(self, activation_idempotency_key: UUID) -> Path:
        return self.root / self._intent_name(activation_idempotency_key)

    def terminal_path_for(self, activation_idempotency_key: UUID) -> Path:
        return self.root / self._terminal_name(activation_idempotency_key)

    def path_for(self, activation_idempotency_key: UUID) -> Path:
        return self.terminal_path_for(activation_idempotency_key)

    def record_intent(self, record: CapacityManagerConfigurationCompensationIntentRecord) -> None:
        canonical = CapacityManagerConfigurationCompensationIntentRecord.from_bytes(
            record.to_bytes()
        )
        if canonical != record:
            raise ValueError("protected capacity manager compensation record drifted")
        self._record_payload(
            self._intent_name(record.activation_idempotency_key),
            record.to_bytes(),
        )

    def record(self, record: CapacityManagerConfigurationCompensationRecord) -> None:
        canonical = CapacityManagerConfigurationCompensationRecord.from_bytes(record.to_bytes())
        if canonical != record:
            raise ValueError("protected capacity manager compensation record drifted")
        directory = self._open_directory(create=False)
        if directory is None:
            raise RuntimeError("protected capacity manager compensation intent is missing")
        try:
            self._require_matching_intent_at(directory, record)
            self._record_payload_at(
                directory,
                self._terminal_name(record.activation_idempotency_key),
                record.to_bytes(),
            )
        finally:
            os.close(directory)

    def read_intent(
        self, activation_idempotency_key: UUID
    ) -> CapacityManagerConfigurationCompensationIntentRecord:
        directory = self._open_directory(create=False)
        if directory is None:
            raise FileNotFoundError(self.root / self._intent_name(activation_idempotency_key))
        try:
            return self._read_intent_at(directory, activation_idempotency_key)
        finally:
            os.close(directory)

    def read(
        self, activation_idempotency_key: UUID
    ) -> CapacityManagerConfigurationCompensationRecord:
        directory = self._open_directory(create=False)
        if directory is None:
            raise FileNotFoundError(self.root / self._terminal_name(activation_idempotency_key))
        try:
            record = self._read_terminal_at(directory, activation_idempotency_key)
            self._require_matching_intent_at(directory, record)
            return record
        finally:
            os.close(directory)

    def find_record_for_plan(
        self,
        *,
        request_id: str,
        attempt_number: int,
        plan_digest: str,
        predecessor_configuration_epoch: int,
        predecessor_configuration_digest: str,
        backup_lease_digest: str,
    ) -> CapacityManagerConfigurationCompensationRecord | None:
        validate_safe_identifier(request_id, "request_id")
        if (
            type(attempt_number) is not int
            or attempt_number < 1
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    plan_digest,
                    predecessor_configuration_digest,
                    backup_lease_digest,
                )
            )
            or type(predecessor_configuration_epoch) is not int
            or predecessor_configuration_epoch < 1
        ):
            raise ValueError("protected capacity manager compensation binding is invalid")
        directory = self._open_directory(create=False)
        if directory is None:
            return None
        records: dict[str, dict[str, object]] = {}

        def matches_record(
            record: (
                CapacityManagerConfigurationCompensationIntentRecord
                | CapacityManagerConfigurationCompensationRecord
            ),
        ) -> bool:
            return record.request_id == request_id and record.attempt_number == attempt_number

        try:
            entry_count = 0
            with os.scandir(directory) as entries:
                for entry in entries:
                    entry_count += 1
                    if entry_count > _MAX_DIRECTORY_ENTRIES:
                        raise RuntimeError(
                            "protected capacity manager compensation directory has too many entries"
                        )
                    name = entry.name
                    intent_match = _INTENT_FILE_RE.fullmatch(name)
                    terminal_match = _TERMINAL_FILE_RE.fullmatch(name)
                    if intent_match is None and terminal_match is None:
                        if name.startswith(_RECORD_PREFIX):
                            raise RuntimeError(
                                "protected capacity manager compensation filename is invalid"
                            )
                        continue
                    payload = self._read_at(directory, name)
                    if payload is None:
                        raise RuntimeError(
                            "protected capacity manager compensation record disappeared"
                        )
                    if intent_match is not None:
                        record: (
                            CapacityManagerConfigurationCompensationIntentRecord
                            | CapacityManagerConfigurationCompensationRecord
                        ) = CapacityManagerConfigurationCompensationIntentRecord.from_bytes(payload)
                        phase = "intent"
                        activation_id = intent_match.group(1)
                    else:
                        record = CapacityManagerConfigurationCompensationRecord.from_bytes(payload)
                        phase = "terminal"
                        assert terminal_match is not None
                        activation_id = terminal_match.group(1)
                    if str(record.activation_idempotency_key) != activation_id:
                        raise RuntimeError(
                            "protected capacity manager compensation filename drifted"
                        )
                    records.setdefault(activation_id, {})[phase] = record
        finally:
            os.close(directory)
        matches: list[CapacityManagerConfigurationCompensationRecord] = []
        for phases in records.values():
            intent = phases.get("intent")
            terminal = phases.get("terminal")
            relevant_records = [
                record
                for record in phases.values()
                if isinstance(
                    record,
                    (
                        CapacityManagerConfigurationCompensationIntentRecord,
                        CapacityManagerConfigurationCompensationRecord,
                    ),
                )
                and matches_record(record)
            ]
            if not relevant_records:
                continue
            for record in relevant_records:
                if (
                    record.plan_digest != plan_digest
                    or record.predecessor_configuration_epoch != predecessor_configuration_epoch
                    or record.predecessor_configuration_digest != predecessor_configuration_digest
                    or record.backup_lease_digest != backup_lease_digest
                ):
                    raise RuntimeError(
                        "protected capacity manager compensation plan binding drifted"
                    )
            if not isinstance(
                intent, CapacityManagerConfigurationCompensationIntentRecord
            ) or not isinstance(terminal, CapacityManagerConfigurationCompensationRecord):
                raise RuntimeError("protected capacity manager compensation record is incomplete")
            if terminal.binding_payload() != intent.binding_payload():
                raise RuntimeError("protected capacity manager compensation intent drifted")
            if (
                terminal.request_id != request_id
                or terminal.attempt_number != attempt_number
                or terminal.plan_digest != plan_digest
                or intent.request_id != request_id
                or intent.attempt_number != attempt_number
                or intent.plan_digest != plan_digest
            ):
                raise RuntimeError("protected capacity manager compensation binding drifted")
            if (
                intent.predecessor_configuration_epoch != predecessor_configuration_epoch
                or intent.predecessor_configuration_digest != predecessor_configuration_digest
                or intent.backup_lease_digest != backup_lease_digest
            ):
                raise RuntimeError("protected capacity manager compensation plan binding drifted")
            matches.append(terminal)
        if len(matches) > 1:
            raise RuntimeError("protected capacity manager compensation match is ambiguous")
        return matches[0] if matches else None

    def _record_payload(self, name: str, payload: bytes) -> None:
        _validate_record_size(payload)
        directory = self._open_directory(create=True)
        assert directory is not None
        try:
            self._record_payload_at(directory, name, payload)
        finally:
            os.close(directory)

    def _record_payload_at(self, directory: int, name: str, payload: bytes) -> None:
        temporary = f"..{name}.loom-{uuid4().hex}.tmp"
        descriptor: int | None = None
        try:
            current = self._read_at(directory, name)
            if current is not None:
                if current != payload:
                    raise RuntimeError(
                        "protected capacity manager compensation record already drifted"
                    )
                return
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            descriptor = os.open(temporary, flags, _PRIVATE_FILE_MODE, dir_fd=directory)
            _write_all(descriptor, payload)
            os.fchmod(descriptor, _PRIVATE_FILE_MODE)
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            try:
                os.link(
                    temporary,
                    name,
                    src_dir_fd=directory,
                    dst_dir_fd=directory,
                    follow_symlinks=False,
                )
            except FileExistsError:
                current = self._read_at(directory, name)
                if current != payload:
                    raise RuntimeError(
                        "protected capacity manager compensation record already drifted"
                    ) from None
            else:
                os.unlink(temporary, dir_fd=directory)
                os.fsync(directory)
            if self._read_at(directory, name) != payload:
                raise RuntimeError("protected capacity manager compensation publication drifted")
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass

    def _read_payload(self, name: str) -> bytes:
        directory = self._open_directory(create=False)
        if directory is None:
            raise FileNotFoundError(self.root / name)
        try:
            return self._read_payload_at(directory, name)
        finally:
            os.close(directory)

    def _read_payload_at(self, directory: int, name: str) -> bytes:
        payload = self._read_at(directory, name)
        if payload is None:
            raise FileNotFoundError(self.root / name)
        return payload

    def _open_directory(self, *, create: bool) -> int | None:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        parts = self.root.parts
        descriptor = os.open(parts[0], flags)
        try:
            self._validate_path_component(os.fstat(descriptor), "path component")
            for index, part in enumerate(parts[1:], start=1):
                is_root = index == len(parts) - 1
                try:
                    child = os.open(part, flags, dir_fd=descriptor)
                except FileNotFoundError:
                    if not create:
                        os.close(descriptor)
                        return None
                    os.mkdir(part, _PRIVATE_DIRECTORY_MODE, dir_fd=descriptor)
                    os.fsync(descriptor)
                    child = os.open(part, flags, dir_fd=descriptor)
                except OSError as exc:
                    raise RuntimeError(
                        "protected capacity manager compensation root path is unsafe"
                    ) from exc
                try:
                    metadata = os.fstat(child)
                    if is_root:
                        self._validate_directory(metadata, "root")
                    else:
                        self._validate_path_component(metadata, "path component")
                except BaseException:
                    os.close(child)
                    raise
                os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _require_matching_intent(
        self,
        record: CapacityManagerConfigurationCompensationRecord,
    ) -> CapacityManagerConfigurationCompensationIntentRecord:
        directory = self._open_directory(create=False)
        if directory is None:
            raise RuntimeError("protected capacity manager compensation intent is missing")
        try:
            return self._require_matching_intent_at(directory, record)
        finally:
            os.close(directory)

    def _require_matching_intent_at(
        self,
        directory: int,
        record: CapacityManagerConfigurationCompensationRecord,
    ) -> CapacityManagerConfigurationCompensationIntentRecord:
        try:
            intent = self._read_intent_at(directory, record.activation_idempotency_key)
        except FileNotFoundError as exc:
            raise RuntimeError("protected capacity manager compensation intent is missing") from exc
        if record.binding_payload() != intent.binding_payload():
            raise RuntimeError("protected capacity manager compensation intent drifted")
        return intent

    def _read_intent_at(
        self,
        directory: int,
        activation_idempotency_key: UUID,
    ) -> CapacityManagerConfigurationCompensationIntentRecord:
        payload = self._read_payload_at(directory, self._intent_name(activation_idempotency_key))
        record = CapacityManagerConfigurationCompensationIntentRecord.from_bytes(payload)
        matched = _INTENT_FILE_RE.fullmatch(self._intent_name(activation_idempotency_key))
        assert matched is not None
        if str(record.activation_idempotency_key) != matched.group(1):
            raise RuntimeError("protected capacity manager compensation filename drifted")
        return record

    def _read_terminal_at(
        self,
        directory: int,
        activation_idempotency_key: UUID,
    ) -> CapacityManagerConfigurationCompensationRecord:
        payload = self._read_payload_at(directory, self._terminal_name(activation_idempotency_key))
        record = CapacityManagerConfigurationCompensationRecord.from_bytes(payload)
        matched = _TERMINAL_FILE_RE.fullmatch(self._terminal_name(activation_idempotency_key))
        assert matched is not None
        if str(record.activation_idempotency_key) != matched.group(1):
            raise RuntimeError("protected capacity manager compensation filename drifted")
        return record

    def _validate_path_component(self, metadata: os.stat_result, label: str) -> None:
        if not stat.S_ISDIR(metadata.st_mode):
            raise RuntimeError(f"protected capacity manager compensation {label} is unsafe")

    def _validate_directory(self, metadata: os.stat_result, label: str) -> None:
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
        ):
            raise RuntimeError(f"protected capacity manager compensation {label} is unsafe")

    def _read_at(self, directory: int, name: str) -> bytes | None:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            descriptor = os.open(name, flags, dir_fd=directory)
        except FileNotFoundError:
            return None
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != self.service_uid
                or before.st_nlink != 1
                or stat.S_IMODE(before.st_mode) != _PRIVATE_FILE_MODE
                or not 1 <= before.st_size <= _MAX_RECORD_BYTES
            ):
                raise RuntimeError(
                    "protected capacity manager compensation record metadata is unsafe"
                )
            chunks: list[bytes] = []
            remaining = _MAX_RECORD_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
            if (
                len(payload) > _MAX_RECORD_BYTES
                or before.st_size != len(payload)
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            ):
                raise RuntimeError("protected capacity manager compensation read is unstable")
            return payload
        finally:
            os.close(descriptor)

    @staticmethod
    def _intent_name(activation_idempotency_key: UUID) -> str:
        return f"{_RECORD_PREFIX}{_validate_activation_identity(activation_idempotency_key)}-intent.json"

    @staticmethod
    def _terminal_name(activation_idempotency_key: UUID) -> str:
        return (
            f"{_RECORD_PREFIX}{_validate_activation_identity(activation_idempotency_key)}"
            "-terminal.json"
        )


def _validate_common_record(
    *,
    schema_version: int,
    request_id: str,
    attempt_number: int,
    plan_digest: str,
    activation_idempotency_key: UUID,
    activation_request_digest: str,
    target_configuration_epoch: int,
    target_configuration_digest: str,
    target_configuration_evidence_digest: str,
    predecessor_configuration_epoch: int,
    predecessor_configuration_digest: str,
    predecessor_configuration_evidence_digest: str,
    backup_lease_digest: str,
    rollback_idempotency_key: UUID,
    rollback_request_digest: str,
    rollback_evidence_sha256: str,
    record_digest: str,
) -> None:
    validate_safe_identifier(request_id, "request_id")
    digests = (
        plan_digest,
        activation_request_digest,
        target_configuration_digest,
        target_configuration_evidence_digest,
        predecessor_configuration_digest,
        predecessor_configuration_evidence_digest,
        backup_lease_digest,
        rollback_request_digest,
        rollback_evidence_sha256,
        record_digest,
    )
    if (
        schema_version != 1
        or type(attempt_number) is not int
        or attempt_number < 1
        or any(
            type(value) is not int or value < 1
            for value in (target_configuration_epoch, predecessor_configuration_epoch)
        )
        or predecessor_configuration_epoch + 1 != target_configuration_epoch
        or any(_SHA256_RE.fullmatch(value) is None for value in digests)
        or activation_idempotency_key.int == 0
        or rollback_idempotency_key.int == 0
        or activation_idempotency_key == rollback_idempotency_key
    ):
        raise ValueError("protected capacity manager compensation record is invalid")


def _validate_record_size(payload: bytes) -> None:
    if type(payload) is not bytes or not 1 <= len(payload) <= _MAX_RECORD_BYTES:
        raise ValueError("protected capacity manager compensation record bytes are invalid")


def _strict_json_object(payload: bytes) -> dict[str, object]:
    _validate_record_size(payload)
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("protected capacity manager compensation record is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError("protected capacity manager compensation record is invalid")
    return value


def _write_all(descriptor: int, payload: bytes) -> None:
    written = 0
    while written < len(payload):
        amount = os.write(descriptor, payload[written:])
        if amount <= 0:
            raise OSError("protected capacity manager compensation write made no progress")
        written += amount


def _validate_activation_identity(activation_idempotency_key: UUID) -> str:
    if not isinstance(activation_idempotency_key, UUID) or activation_idempotency_key.int == 0:
        raise ValueError("protected capacity manager compensation identity is invalid")
    return str(activation_idempotency_key)


def _string(value: dict[str, object], field: str) -> str:
    observed = value.get(field)
    if not isinstance(observed, str):
        raise ValueError("protected capacity manager compensation record is invalid")
    return observed


def _integer(value: dict[str, object], field: str) -> int:
    observed = value.get(field)
    if type(observed) is not int:
        raise ValueError("protected capacity manager compensation record is invalid")
    return observed


def _uuid(value: dict[str, object], field: str) -> UUID:
    observed = _string(value, field)
    parsed = UUID(observed)
    if parsed.int == 0 or str(parsed) != observed:
        raise ValueError("protected capacity manager compensation record is invalid")
    return parsed


__all__ = [
    "CapacityManagerConfigurationCompensationIntentRecord",
    "CapacityManagerConfigurationCompensationRecord",
    "CapacityManagerConfigurationCompensationStore",
]
