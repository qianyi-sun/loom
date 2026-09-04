"""Write-once recovery authority for protected execution preparation operations."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Literal
from uuid import uuid4

from .final_gate_plan import FinalGatePlan
from .model import validate_safe_identifier

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OPERATION_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_TEMPORARY_RE = re.compile(
    r"^\.\.(?P<final>[a-z][a-z0-9-]{2,63}\.(?:intent|terminal)\.json)\.loom-"
    r"[0-9a-f]{32}\.tmp$"
)
_OPERATIONS = frozenset(
    {
        "manager-preparation",
        "controller-files-gb10",
        "controller-files-oldlab",
        "prepared-timer-gb10",
        "prepared-timer-oldlab",
        "prepared-tick-gb10",
        "prepared-tick-oldlab",
        "manager-abort",
    }
)
_FORWARD_OPERATIONS = _OPERATIONS - {"manager-abort"}
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_RECORD_BYTES = 256 * 1024
_MAX_DIRECTORY_ENTRIES = 32


class ExecutionPreparationRecoveryState(StrEnum):
    """Durable local state of one plan-bound preparation attempt."""

    NO_MUTATION = "no-mutation"
    UNRESOLVED = "unresolved"
    PREPARED = "prepared"
    FORWARD_COMPLETE = "forward-complete"
    COMPENSATED = "compensated"


def _digest(value: object, *, label: str, allow_none: bool = False) -> str | None:
    if value is None and allow_none:
        return None
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError(f"execution preparation {label} is invalid")
    return value


def _canonical_json(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            dict(value),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def _hash(value: Mapping[str, object]) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class ExecutionPreparationOperationIntent:
    schema_version: Literal[1]
    request_id: str
    attempt_number: int
    plan_digest: str
    artifact_sha256: str
    operation: str
    request_sha256: str
    prepared_execution_epoch: int | None
    prepared_execution_manifest_sha256: str | None
    intent_sha256: str

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        _digest(self.plan_digest, label="intent plan digest")
        _digest(self.artifact_sha256, label="intent artifact digest")
        _digest(self.request_sha256, label="intent request digest")
        _digest(self.intent_sha256, label="intent digest")
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or type(self.attempt_number) is not int
            or self.attempt_number < 1
            or self.operation not in _OPERATIONS
            or _OPERATION_RE.fullmatch(self.operation) is None
            or (
                self.operation == "manager-preparation"
                and (
                    self.prepared_execution_epoch is not None
                    or self.prepared_execution_manifest_sha256 is not None
                )
            )
            or (
                self.operation != "manager-preparation"
                and (
                    type(self.prepared_execution_epoch) is not int
                    or self.prepared_execution_epoch < 1
                    or _digest(
                        self.prepared_execution_manifest_sha256,
                        label="intent prepared manifest",
                    )
                    is None
                )
            )
        ):
            raise ValueError("execution preparation intent is invalid")

    @classmethod
    def build(
        cls,
        *,
        plan: FinalGatePlan,
        artifact_sha256: str,
        operation: str,
        request_sha256: str,
        prepared_execution_epoch: int | None,
        prepared_execution_manifest_sha256: str | None,
    ) -> ExecutionPreparationOperationIntent:
        if not isinstance(plan, FinalGatePlan):
            raise TypeError("execution preparation intent plan is invalid")
        payload: dict[str, object] = {
            "schema_version": 1,
            "request_id": plan.request_id,
            "attempt_number": plan.attempt_number,
            "plan_digest": plan.plan_digest,
            "artifact_sha256": artifact_sha256,
            "operation": operation,
            "request_sha256": request_sha256,
            "prepared_execution_epoch": prepared_execution_epoch,
            "prepared_execution_manifest_sha256": prepared_execution_manifest_sha256,
        }
        return cls.from_dict({**payload, "intent_sha256": _hash(payload)})

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    def to_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ExecutionPreparationOperationIntent:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("execution preparation intent fields are invalid")
        intent = cls(
            schema_version=_integer(value, "schema_version"),  # type: ignore[arg-type]
            request_id=_string(value, "request_id"),
            attempt_number=_integer(value, "attempt_number"),
            plan_digest=_string(value, "plan_digest"),
            artifact_sha256=_string(value, "artifact_sha256"),
            operation=_string(value, "operation"),
            request_sha256=_string(value, "request_sha256"),
            prepared_execution_epoch=_optional_integer(value, "prepared_execution_epoch"),
            prepared_execution_manifest_sha256=_optional_string(
                value, "prepared_execution_manifest_sha256"
            ),
            intent_sha256=_string(value, "intent_sha256"),
        )
        payload = {key: item for key, item in intent.to_dict().items() if key != "intent_sha256"}
        if _hash(payload) != intent.intent_sha256:
            raise ValueError("execution preparation intent content drifted")
        return intent

    @classmethod
    def from_bytes(cls, payload: bytes) -> ExecutionPreparationOperationIntent:
        value = _decode(payload, label="intent")
        intent = cls.from_dict(value)
        if intent.to_bytes() != payload:
            raise ValueError("execution preparation intent is not canonical")
        return intent


@dataclass(frozen=True, slots=True)
class ExecutionPreparationOperationTerminal:
    schema_version: Literal[1]
    intent_sha256: str
    operation: str
    evidence_sha256: str
    prepared_execution_epoch: int
    prepared_execution_manifest_sha256: str
    result_state: Literal["prepared", "shadow"]
    terminal_sha256: str

    def __post_init__(self) -> None:
        for value, label in (
            (self.intent_sha256, "terminal intent digest"),
            (self.evidence_sha256, "terminal evidence digest"),
            (self.prepared_execution_manifest_sha256, "terminal prepared manifest"),
            (self.terminal_sha256, "terminal digest"),
        ):
            _digest(value, label=label)
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or self.operation not in _OPERATIONS
            or type(self.prepared_execution_epoch) is not int
            or self.prepared_execution_epoch < 1
            or self.result_state not in {"prepared", "shadow"}
            or (self.operation == "manager-abort") != (self.result_state == "shadow")
        ):
            raise ValueError("execution preparation terminal is invalid")

    @classmethod
    def build(
        cls,
        *,
        intent: ExecutionPreparationOperationIntent,
        evidence_sha256: str,
        prepared_execution_epoch: int,
        prepared_execution_manifest_sha256: str,
        result_state: Literal["prepared", "shadow"],
    ) -> ExecutionPreparationOperationTerminal:
        if not isinstance(intent, ExecutionPreparationOperationIntent):
            raise TypeError("execution preparation terminal intent is invalid")
        payload: dict[str, object] = {
            "schema_version": 1,
            "intent_sha256": intent.intent_sha256,
            "operation": intent.operation,
            "evidence_sha256": evidence_sha256,
            "prepared_execution_epoch": prepared_execution_epoch,
            "prepared_execution_manifest_sha256": prepared_execution_manifest_sha256,
            "result_state": result_state,
        }
        return cls.from_dict({**payload, "terminal_sha256": _hash(payload)})

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    def to_bytes(self) -> bytes:
        return _canonical_json(self.to_dict())

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ExecutionPreparationOperationTerminal:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("execution preparation terminal fields are invalid")
        result_state = _string(value, "result_state")
        if result_state not in {"prepared", "shadow"}:
            raise ValueError("execution preparation terminal state is invalid")
        terminal = cls(
            schema_version=_integer(value, "schema_version"),  # type: ignore[arg-type]
            intent_sha256=_string(value, "intent_sha256"),
            operation=_string(value, "operation"),
            evidence_sha256=_string(value, "evidence_sha256"),
            prepared_execution_epoch=_integer(value, "prepared_execution_epoch"),
            prepared_execution_manifest_sha256=_string(value, "prepared_execution_manifest_sha256"),
            result_state=result_state,  # type: ignore[arg-type]
            terminal_sha256=_string(value, "terminal_sha256"),
        )
        payload = {
            key: item for key, item in terminal.to_dict().items() if key != "terminal_sha256"
        }
        if _hash(payload) != terminal.terminal_sha256:
            raise ValueError("execution preparation terminal content drifted")
        return terminal

    @classmethod
    def from_bytes(cls, payload: bytes) -> ExecutionPreparationOperationTerminal:
        value = _decode(payload, label="terminal")
        terminal = cls.from_dict(value)
        if terminal.to_bytes() != payload:
            raise ValueError("execution preparation terminal is not canonical")
        return terminal


@dataclass(frozen=True, slots=True)
class ExecutionPreparationOperationJournal:
    """Publish and classify one plan's immutable preparation operation records."""

    state_root: Path
    request_id: str
    attempt_number: int
    service_uid: int

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        if (
            not isinstance(self.state_root, Path)
            or not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or type(self.attempt_number) is not int
            or self.attempt_number < 1
            or type(self.service_uid) is not int
            or self.service_uid < 0
        ):
            raise ValueError("execution preparation journal authority is invalid")

    @property
    def root(self) -> Path:
        return (
            self.state_root
            / "protected-capacity"
            / "execution-preparation-journals"
            / self.request_id
            / str(self.attempt_number)
        )

    def record_intent(self, intent: ExecutionPreparationOperationIntent) -> None:
        canonical = ExecutionPreparationOperationIntent.from_bytes(intent.to_bytes())
        if canonical != intent or not self._identity_matches(intent):
            raise ValueError("execution preparation intent binding is invalid")
        self._publish(f"{intent.operation}.intent.json", intent.to_bytes())

    def record_terminal(self, terminal: ExecutionPreparationOperationTerminal) -> None:
        canonical = ExecutionPreparationOperationTerminal.from_bytes(terminal.to_bytes())
        if canonical != terminal:
            raise ValueError("execution preparation terminal binding is invalid")
        intent = self.read_intent(terminal.operation)
        if terminal.intent_sha256 != intent.intent_sha256:
            raise RuntimeError("execution preparation terminal intent drifted")
        self._publish(f"{terminal.operation}.terminal.json", terminal.to_bytes())

    def read_intent(self, operation: str) -> ExecutionPreparationOperationIntent:
        self._operation(operation)
        return ExecutionPreparationOperationIntent.from_bytes(
            self._read(f"{operation}.intent.json")
        )

    def read_terminal(self, operation: str) -> ExecutionPreparationOperationTerminal:
        self._operation(operation)
        terminal = ExecutionPreparationOperationTerminal.from_bytes(
            self._read(f"{operation}.terminal.json")
        )
        intent = self.read_intent(operation)
        if terminal.intent_sha256 != intent.intent_sha256:
            raise RuntimeError("execution preparation terminal intent drifted")
        return terminal

    def records(
        self,
        plan: FinalGatePlan,
        *,
        artifact_sha256: str,
    ) -> Mapping[
        str,
        tuple[
            ExecutionPreparationOperationIntent,
            ExecutionPreparationOperationTerminal | None,
        ],
    ]:
        _digest(artifact_sha256, label="journal artifact digest")
        if (
            not isinstance(plan, FinalGatePlan)
            or plan.request_id != self.request_id
            or plan.attempt_number != self.attempt_number
        ):
            raise RuntimeError("execution preparation journal binding drifted")
        try:
            entries = self._entry_names()
        except FileNotFoundError:
            return MappingProxyType({})
        phases: dict[str, set[str]] = {}
        for name in entries:
            temporary = _TEMPORARY_RE.fullmatch(name)
            if temporary is not None:
                continue
            matched = re.fullmatch(
                r"(?P<operation>[a-z][a-z0-9-]{2,63})\.(?P<phase>intent|terminal)\.json",
                name,
            )
            if matched is None or matched.group("operation") not in _OPERATIONS:
                raise RuntimeError("execution preparation journal inventory is invalid")
            phases.setdefault(matched.group("operation"), set()).add(matched.group("phase"))
        records: dict[
            str,
            tuple[
                ExecutionPreparationOperationIntent,
                ExecutionPreparationOperationTerminal | None,
            ],
        ] = {}
        for operation, found in phases.items():
            if "intent" not in found:
                raise RuntimeError("execution preparation journal terminal has no intent")
            intent = self.read_intent(operation)
            if (
                not self._identity_matches(intent)
                or intent.plan_digest != plan.plan_digest
                or intent.artifact_sha256 != artifact_sha256
            ):
                raise RuntimeError("execution preparation journal binding drifted")
            terminal = self.read_terminal(operation) if "terminal" in found else None
            records[operation] = (intent, terminal)
        if set(records) - {"manager-preparation"} and "manager-preparation" not in records:
            raise RuntimeError("execution preparation journal sequence is invalid")
        return MappingProxyType(dict(sorted(records.items())))

    def recovery_state(
        self,
        plan: FinalGatePlan,
        *,
        artifact_sha256: str,
    ) -> ExecutionPreparationRecoveryState:
        records = self.records(plan, artifact_sha256=artifact_sha256)
        if not records:
            return ExecutionPreparationRecoveryState.NO_MUTATION
        abort = records.get("manager-abort")
        if abort is not None and abort[1] is not None:
            return ExecutionPreparationRecoveryState.COMPENSATED
        if any(terminal is None for _intent, terminal in records.values()):
            return ExecutionPreparationRecoveryState.UNRESOLVED
        completed = set(records)
        if completed == set(_FORWARD_OPERATIONS):
            return ExecutionPreparationRecoveryState.FORWARD_COMPLETE
        return ExecutionPreparationRecoveryState.PREPARED

    def _identity_matches(self, intent: ExecutionPreparationOperationIntent) -> bool:
        return intent.request_id == self.request_id and intent.attempt_number == self.attempt_number

    @staticmethod
    def _operation(operation: str) -> str:
        if operation not in _OPERATIONS:
            raise ValueError("execution preparation journal operation is invalid")
        return operation

    def _entry_names(self) -> tuple[str, ...]:
        self._validate_directories()
        entries = tuple(os.listdir(self.root))
        if len(entries) > _MAX_DIRECTORY_ENTRIES:
            raise RuntimeError("execution preparation journal inventory is too large")
        return entries

    def _publish(self, name: str, payload: bytes) -> None:
        if not 0 < len(payload) <= _MAX_RECORD_BYTES or "/" in name or "\\" in name:
            raise ValueError("execution preparation journal publication is invalid")
        self._ensure_directories()
        directory = self._open_root()
        temporary = f"..{name}.loom-{uuid4().hex}.tmp"
        descriptor: int | None = None
        try:
            current = self._read_at(directory, name)
            if current is not None:
                if current != payload:
                    raise RuntimeError("execution preparation journal record already drifted")
                return
            descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                _PRIVATE_FILE_MODE,
                dir_fd=directory,
            )
            os.fchmod(descriptor, _PRIVATE_FILE_MODE)
            _write_all(descriptor, payload)
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
                if self._read_at(directory, name) != payload:
                    raise RuntimeError(
                        "execution preparation journal record already drifted"
                    ) from None
            os.unlink(temporary, dir_fd=directory)
            os.fsync(directory)
            if self._read_at(directory, name) != payload:
                raise RuntimeError("execution preparation journal publication drifted")
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass
            os.close(directory)

    def _read(self, name: str) -> bytes:
        self._validate_directories()
        directory = self._open_root()
        try:
            payload = self._read_at(directory, name)
        finally:
            os.close(directory)
        if payload is None:
            raise FileNotFoundError(self.root / name)
        return payload

    def _read_at(self, directory: int, name: str) -> bytes | None:
        try:
            descriptor = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=directory,
            )
        except FileNotFoundError:
            return None
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or stat.S_IMODE(before.st_mode) != _PRIVATE_FILE_MODE
                or before.st_uid != self.service_uid
                or before.st_nlink not in {1, 2}
                or not 0 < before.st_size <= _MAX_RECORD_BYTES
            ):
                raise RuntimeError("execution preparation journal record is unsafe")
            payload = os.read(descriptor, _MAX_RECORD_BYTES + 1)
            after = os.fstat(descriptor)
            if len(payload) != before.st_size or _metadata(before) != _metadata(after):
                raise RuntimeError("execution preparation journal record changed while reading")
            if before.st_nlink == 2:
                self._recover_linked_temporary(
                    directory,
                    name=name,
                    descriptor=descriptor,
                    metadata=before,
                )
        finally:
            os.close(descriptor)
        return payload

    def _recover_linked_temporary(
        self,
        directory: int,
        *,
        name: str,
        descriptor: int,
        metadata: os.stat_result,
    ) -> None:
        aliases: list[str] = []
        with os.scandir(directory) as entries:
            for entry in entries:
                matched = _TEMPORARY_RE.fullmatch(entry.name)
                if matched is None or matched.group("final") != name:
                    continue
                alias_metadata = entry.stat(follow_symlinks=False)
                if (alias_metadata.st_dev, alias_metadata.st_ino) == (
                    metadata.st_dev,
                    metadata.st_ino,
                ):
                    aliases.append(entry.name)
        if len(aliases) != 1:
            raise RuntimeError("execution preparation journal temporary link residue is ambiguous")
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        alias_descriptor = os.open(aliases[0], flags, dir_fd=directory)
        try:
            if _metadata(os.fstat(alias_descriptor)) != _metadata(metadata):
                raise RuntimeError("execution preparation journal temporary link residue changed")
        finally:
            os.close(alias_descriptor)
        os.unlink(aliases[0], dir_fd=directory)
        os.fsync(directory)
        after_unlink = os.fstat(descriptor)
        if (
            _stable_metadata(after_unlink) != _stable_metadata(metadata)
            or after_unlink.st_nlink != 1
        ):
            raise RuntimeError(
                "execution preparation journal temporary link residue did not converge"
            )

    def _open_root(self) -> int:
        return os.open(
            self.root,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )

    def _ensure_directories(self) -> None:
        for path in (
            self.state_root,
            self.state_root / "protected-capacity",
            self.state_root / "protected-capacity" / "execution-preparation-journals",
            self.state_root
            / "protected-capacity"
            / "execution-preparation-journals"
            / self.request_id,
            self.root,
        ):
            try:
                path.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
            except FileExistsError:
                pass
            _require_directory(path, service_uid=self.service_uid)

    def _validate_directories(self) -> None:
        for path in (
            self.state_root,
            self.state_root / "protected-capacity",
            self.state_root / "protected-capacity" / "execution-preparation-journals",
            self.state_root
            / "protected-capacity"
            / "execution-preparation-journals"
            / self.request_id,
            self.root,
        ):
            _require_directory(path, service_uid=self.service_uid)


def _decode(payload: bytes, *, label: str) -> Mapping[str, object]:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_RECORD_BYTES:
        raise ValueError(f"execution preparation {label} bytes are invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"execution preparation {label} bytes are invalid") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"execution preparation {label} fields are invalid")
    return value


def _require_directory(path: Path, *, service_uid: int) -> None:
    metadata = path.stat(follow_symlinks=False)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
        or metadata.st_uid != service_uid
    ):
        raise RuntimeError("execution preparation journal directory is unsafe")


def _write_all(descriptor: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(descriptor, payload[offset:])
        if written <= 0:  # pragma: no cover - os.write contract
            raise OSError("execution preparation journal write made no progress")
        offset += written


def _metadata(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _stable_metadata(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_size,
        value.st_mtime_ns,
    )


def _string(value: Mapping[str, object], key: str) -> str:
    found = value.get(key)
    if not isinstance(found, str):
        raise ValueError(f"execution preparation {key} must be a string")
    return found


def _optional_string(value: Mapping[str, object], key: str) -> str | None:
    found = value.get(key)
    if found is not None and not isinstance(found, str):
        raise ValueError(f"execution preparation {key} must be a string or null")
    return found


def _integer(value: Mapping[str, object], key: str) -> int:
    found = value.get(key)
    if type(found) is not int:
        raise ValueError(f"execution preparation {key} must be an integer")
    return found


def _optional_integer(value: Mapping[str, object], key: str) -> int | None:
    found = value.get(key)
    if found is not None and type(found) is not int:
        raise ValueError(f"execution preparation {key} must be an integer or null")
    return found


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("execution preparation journal contains duplicate fields")
        value[key] = item
    return value


__all__ = [
    "ExecutionPreparationOperationIntent",
    "ExecutionPreparationOperationJournal",
    "ExecutionPreparationOperationTerminal",
    "ExecutionPreparationRecoveryState",
]
