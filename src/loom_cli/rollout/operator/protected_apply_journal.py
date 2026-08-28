"""Crash-safe component journal for one protected staging apply.

The outer final-gate journal cannot distinguish a crash immediately before a
mutation from a crash immediately after it.  This journal publishes an
immutable intent before each component, classifies live state, applies only a
component whose attested precondition is ready, and publishes terminal evidence only after live
state is exact.  A restart therefore verifies an in-flight intent instead of
blindly repeating protected work.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from uuid import uuid4

from .failure_diagnostics import unclassified_failure_diagnostic
from .final_gate_plan import FinalGatePlan
from .model import validate_safe_identifier

_COMPONENT_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_GB10_HOST_RE = re.compile(r"^trt-gb10-(?:[1-9]|1[0-5])$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_RECORD_BYTES = 256 * 1024


class ProtectedApplyJournalError(RuntimeError):
    """Raised when protected component state is unsafe or ambiguous."""


class ComponentState(StrEnum):
    READY = "ready"
    EXACT = "exact"
    DRIFTED = "drifted"


@dataclass(frozen=True, slots=True)
class ComponentObservation:
    state: ComponentState
    evidence_digest: str
    observed_epoch: int

    def __post_init__(self) -> None:
        if (
            _SHA256_RE.fullmatch(self.evidence_digest) is None
            or type(self.observed_epoch) is not int
            or self.observed_epoch < 0
        ):
            raise ValueError("protected component observation is invalid")


@dataclass(frozen=True, slots=True)
class ProtectedApplyComponent:
    component_id: str
    implementation_digest: str
    input_fingerprint: str
    classify: Callable[[FinalGatePlan], ComponentObservation]
    apply: Callable[[FinalGatePlan], None]

    def __post_init__(self) -> None:
        if (
            _COMPONENT_RE.fullmatch(self.component_id) is None
            or _SHA256_RE.fullmatch(self.implementation_digest) is None
            or _SHA256_RE.fullmatch(self.input_fingerprint) is None
        ):
            raise ValueError("protected apply component authority is invalid")


@dataclass(frozen=True, slots=True)
class ComponentIntent:
    schema_version: int
    request_id: str
    attempt_number: int
    plan_digest: str
    component_id: str
    ordinal: int
    implementation_digest: str
    input_fingerprint: str
    intent_digest: str

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        if (
            self.schema_version != 1
            or type(self.attempt_number) is not int
            or self.attempt_number < 1
            or type(self.ordinal) is not int
            or self.ordinal < 0
            or _COMPONENT_RE.fullmatch(self.component_id) is None
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.plan_digest,
                    self.implementation_digest,
                    self.input_fingerprint,
                    self.intent_digest,
                )
            )
        ):
            raise ValueError("protected component intent is invalid")

    @classmethod
    def build(
        cls,
        plan: FinalGatePlan,
        component: ProtectedApplyComponent,
        ordinal: int,
    ) -> ComponentIntent:
        payload = {
            "schema_version": 1,
            "request_id": plan.request_id,
            "attempt_number": plan.attempt_number,
            "plan_digest": plan.plan_digest,
            "component_id": component.component_id,
            "ordinal": ordinal,
            "implementation_digest": component.implementation_digest,
            "input_fingerprint": component.input_fingerprint,
        }
        return cls.from_dict({**payload, "intent_digest": _hash_json(payload)})

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ComponentIntent:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected component intent fields are invalid")
        intent = cls(
            schema_version=_integer(value, "schema_version"),
            request_id=_string(value, "request_id"),
            attempt_number=_integer(value, "attempt_number"),
            plan_digest=_string(value, "plan_digest"),
            component_id=_string(value, "component_id"),
            ordinal=_integer(value, "ordinal"),
            implementation_digest=_string(value, "implementation_digest"),
            input_fingerprint=_string(value, "input_fingerprint"),
            intent_digest=_string(value, "intent_digest"),
        )
        payload = {key: item for key, item in intent.to_dict().items() if key != "intent_digest"}
        if _hash_json(payload) != intent.intent_digest:
            raise ValueError("protected component intent content drifted")
        return intent


@dataclass(frozen=True, slots=True)
class ComponentFailure:
    schema_version: int
    component_id: str
    failure_code: str
    failed_hosts: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or self.component_id != "gb10-candidate"
            or self.failure_code != "gb10-convergence-failed"
            or not self.failed_hosts
            or tuple(sorted(self.failed_hosts)) != self.failed_hosts
            or len(set(self.failed_hosts)) != len(self.failed_hosts)
            or any(_GB10_HOST_RE.fullmatch(host) is None for host in self.failed_hosts)
        ):
            raise ValueError("protected component failure metadata is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "component_id": self.component_id,
            "failure_code": self.failure_code,
            "failed_hosts": list(self.failed_hosts),
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ComponentFailure:
        if set(value) != {
            "schema_version",
            "component_id",
            "failure_code",
            "failed_hosts",
        }:
            raise ValueError("protected component failure fields are invalid")
        hosts = value["failed_hosts"]
        if not isinstance(hosts, list) or any(not isinstance(host, str) for host in hosts):
            raise ValueError("protected component failed-host metadata is invalid")
        return cls(
            schema_version=_integer(value, "schema_version"),
            component_id=_string(value, "component_id"),
            failure_code=_string(value, "failure_code"),
            failed_hosts=tuple(hosts),
        )


@dataclass(frozen=True, slots=True)
class ComponentTerminal:
    schema_version: int
    intent_digest: str
    component_id: str
    evidence_digest: str
    observed_epoch: int
    applied: bool
    terminal_digest: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or _COMPONENT_RE.fullmatch(self.component_id) is None
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.intent_digest,
                    self.evidence_digest,
                    self.terminal_digest,
                )
            )
            or type(self.observed_epoch) is not int
            or self.observed_epoch < 0
            or type(self.applied) is not bool
        ):
            raise ValueError("protected component terminal evidence is invalid")

    @classmethod
    def build(
        cls,
        intent: ComponentIntent,
        observation: ComponentObservation,
        *,
        applied: bool,
    ) -> ComponentTerminal:
        payload = {
            "schema_version": 1,
            "intent_digest": intent.intent_digest,
            "component_id": intent.component_id,
            "evidence_digest": observation.evidence_digest,
            "observed_epoch": observation.observed_epoch,
            "applied": applied,
        }
        return cls.from_dict({**payload, "terminal_digest": _hash_json(payload)})

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ComponentTerminal:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected component terminal fields are invalid")
        terminal = cls(
            schema_version=_integer(value, "schema_version"),
            intent_digest=_string(value, "intent_digest"),
            component_id=_string(value, "component_id"),
            evidence_digest=_string(value, "evidence_digest"),
            observed_epoch=_integer(value, "observed_epoch"),
            applied=_boolean(value, "applied"),
            terminal_digest=_string(value, "terminal_digest"),
        )
        payload = {
            key: item for key, item in terminal.to_dict().items() if key != "terminal_digest"
        }
        if _hash_json(payload) != terminal.terminal_digest:
            raise ValueError("protected component terminal content drifted")
        return terminal


class ProtectedApplyJournal:
    """Serialize and recover one exact ordered protected component chain."""

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
            raise ProtectedApplyJournalError("protected apply journal authority is invalid")
        self.attempt_root = (
            state_root / "requests" / self.request_id / "attempts" / str(attempt_number)
        )
        self.root = self.attempt_root / "protected-apply"
        self.lock_path = self.root / "execution.lock"

    def execute(
        self,
        plan: FinalGatePlan,
        components: Sequence[ProtectedApplyComponent],
    ) -> Mapping[str, ComponentTerminal]:
        if (
            plan.request_id != self.request_id
            or plan.attempt_number != self.attempt_number
            or not components
            or len(components) > 32
            or len({component.component_id for component in components}) != len(components)
        ):
            raise ProtectedApplyJournalError("protected apply chain identity is invalid")
        self._ensure()
        lock_fd = os.open(
            self.lock_path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            _PRIVATE_FILE_MODE,
        )
        try:
            _require_regular(lock_fd, uid=self.service_uid)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            results: dict[str, ComponentTerminal] = {}
            for ordinal, component in enumerate(components):
                results[component.component_id] = self._execute_one(plan, component, ordinal)
            return MappingProxyType(results)
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

    def has_advanced_epoch_terminal(self, plan: FinalGatePlan) -> bool:
        """Return whether this exact plan durably advanced its mutation epoch.

        The protected apply check is journaled twice: once per component and
        once by the outer final-gate DAG.  A process can terminate after the
        component terminal is durable but before the outer check is
        published.  Recovery may trust that narrow window only when the
        service-owned plan, epoch intent, and epoch terminal all bind to the
        same request, attempt, plan digest, and expected next epoch.
        """
        if plan.request_id != self.request_id or plan.attempt_number != self.attempt_number:
            raise ProtectedApplyJournalError("protected apply recovery identity is invalid")
        try:
            _require_directory(self.root, uid=self.service_uid)
        except FileNotFoundError:
            return False

        matches: list[tuple[int, Path]] = []
        for ordinal in (0, 1):
            component_root = self.root / f"{ordinal:02d}-mutation-epoch-claim"
            try:
                _require_directory(component_root, uid=self.service_uid)
            except FileNotFoundError:
                continue
            matches.append((ordinal, component_root))
        if not matches:
            return False
        if len(matches) != 1:
            raise ProtectedApplyJournalError("protected epoch journal identity is ambiguous")

        ordinal, component_root = matches[0]
        try:
            intent = ComponentIntent.from_dict(self._read(component_root / "intent.json"))
        except FileNotFoundError:
            return False
        except ValueError as exc:
            raise ProtectedApplyJournalError("protected epoch intent is invalid") from exc
        if (
            intent.request_id != plan.request_id
            or intent.attempt_number != plan.attempt_number
            or intent.plan_digest != plan.plan_digest
            or intent.component_id != "mutation-epoch-claim"
            or intent.ordinal != ordinal
        ):
            raise ProtectedApplyJournalError("protected epoch intent identity drifted")

        try:
            terminal = ComponentTerminal.from_dict(self._read(component_root / "terminal.json"))
        except FileNotFoundError:
            return False
        except ValueError as exc:
            raise ProtectedApplyJournalError("protected epoch terminal is invalid") from exc
        if (
            terminal.intent_digest != intent.intent_digest
            or terminal.component_id != intent.component_id
            or terminal.observed_epoch != plan.starting_mutation_epoch + 1
        ):
            raise ProtectedApplyJournalError("protected epoch terminal identity drifted")
        return True

    def _execute_one(
        self,
        plan: FinalGatePlan,
        component: ProtectedApplyComponent,
        ordinal: int,
    ) -> ComponentTerminal:
        component_root = self.root / f"{ordinal:02d}-{component.component_id}"
        try:
            component_root.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError:
            pass
        except OSError as exc:
            raise ProtectedApplyJournalError(
                "could not create protected component journal"
            ) from exc
        _require_directory(component_root, uid=self.service_uid)
        intent = ComponentIntent.build(plan, component, ordinal)
        intent_path = component_root / "intent.json"
        terminal_path = component_root / "terminal.json"
        self._publish_or_match(intent_path, intent.to_dict())
        try:
            terminal = ComponentTerminal.from_dict(self._read(terminal_path))
        except FileNotFoundError:
            pass
        else:
            if terminal.intent_digest != intent.intent_digest:
                raise ProtectedApplyJournalError("protected component terminal identity drifted")
            observed = self._classify_with_diagnostic(
                component_root,
                component,
                ordinal,
                plan,
                failure_code="terminal-classify-failed",
            )
            if (
                observed.state is not ComponentState.EXACT
                or observed.evidence_digest != terminal.evidence_digest
                or observed.observed_epoch != terminal.observed_epoch
            ):
                raise ProtectedApplyJournalError(
                    f"protected component {component.component_id} terminal state drifted"
                )
            return terminal

        before = self._classify_with_diagnostic(
            component_root,
            component,
            ordinal,
            plan,
            failure_code="pre-classify-failed",
        )
        if before.state is ComponentState.DRIFTED:
            raise ProtectedApplyJournalError(
                f"protected component {component.component_id} live state drifted"
            )
        applied = False
        if before.state is ComponentState.READY:
            try:
                component.apply(plan)
            except BaseException as exc:
                from .protected_gb10_transport import GB10FleetApplyError

                if component.component_id == "gb10-candidate" and isinstance(
                    exc, GB10FleetApplyError
                ):
                    failure = ComponentFailure(
                        schema_version=1,
                        component_id=component.component_id,
                        failure_code="gb10-convergence-failed",
                        failed_hosts=exc.failed_hosts,
                    )
                    self._publish_or_match(component_root / "failure.json", failure.to_dict())
                else:
                    # Every other component previously published no failure
                    # record at all, leaving its cause a masked dead-end
                    # (#1081). Record a coded, secret-safe reason (#1085 p1).
                    self._publish_failure_diagnostic(
                        component_root,
                        component,
                        ordinal,
                        failure_code="apply-failed",
                        diagnostic=unclassified_failure_diagnostic(
                            exc, activity=component.component_id
                        ),
                    )
                raise
            applied = True
        after = self._classify_with_diagnostic(
            component_root,
            component,
            ordinal,
            plan,
            failure_code="post-classify-failed",
        )
        if after.state is not ComponentState.EXACT:
            self._publish_failure_diagnostic(
                component_root,
                component,
                ordinal,
                failure_code="did-not-converge",
                diagnostic=f"component classified {after.state.value} after apply",
            )
            raise ProtectedApplyJournalError(
                f"protected component {component.component_id} did not converge exactly"
            )
        terminal = ComponentTerminal.build(intent, after, applied=applied)
        self._publish_or_match(terminal_path, terminal.to_dict())
        return terminal

    def _classify_with_diagnostic(
        self,
        component_root: Path,
        component: ProtectedApplyComponent,
        ordinal: int,
        plan: FinalGatePlan,
        *,
        failure_code: str,
    ) -> ComponentObservation:
        try:
            return component.classify(plan)
        except BaseException as exc:
            self._publish_failure_diagnostic(
                component_root,
                component,
                ordinal,
                failure_code=failure_code,
                diagnostic=unclassified_failure_diagnostic(
                    exc,
                    activity=component.component_id,
                ),
            )
            raise

    def _publish_failure_diagnostic(
        self,
        component_root: Path,
        component: ProtectedApplyComponent,
        ordinal: int,
        *,
        failure_code: str,
        diagnostic: str,
    ) -> None:
        """Record *why* a component failed — durably, coded, and secret-safe.

        A failing component otherwise publishes no terminal (terminals are
        written only after exact convergence), so its cause was previously
        unrecoverable — a masked dead-end (#1081, #1085 phase 1). This writes a
        coded reason plus a secret-safe diagnostic (exception type + raise-site
        only; never the message — the #1077 lesson) beside the intent.

        Strictly best-effort: it must never mask the real failure. Any error
        writing it — including a write-once mismatch on a differing retry — is
        swallowed so the original exception still propagates unchanged.
        """
        record = {
            "schema_version": 1,
            "component_id": component.component_id,
            "ordinal": ordinal,
            "failure_code": failure_code,
            "diagnostic": diagnostic,
        }
        try:
            self._publish_or_match(component_root / "failure-diagnostic.json", record)
        except Exception:
            pass

    def _ensure(self) -> None:
        _require_directory(self.attempt_root, uid=self.service_uid)
        try:
            self.root.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError:
            pass
        except OSError as exc:
            raise ProtectedApplyJournalError("could not create protected apply journal") from exc
        _require_directory(self.root, uid=self.service_uid)

    def _publish_or_match(self, path: Path, value: Mapping[str, object]) -> None:
        payload = _json_bytes(value)
        if len(payload) > _MAX_RECORD_BYTES:
            raise ProtectedApplyJournalError("protected component record is too large")
        try:
            existing = self._read(path)
        except FileNotFoundError:
            pass
        else:
            if existing != dict(value):
                raise ProtectedApplyJournalError("protected component record cannot be replaced")
            return
        directory_fd = _open_directory(path.parent)
        temporary = f".{path.name}.{uuid4().hex}.tmp"
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
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(temporary, dir_fd=directory_fd)
            created = False
            os.fsync(directory_fd)
        except FileExistsError:
            if self._read(path) != dict(value):
                raise ProtectedApplyJournalError(
                    "protected component record cannot be replaced"
                ) from None
        except OSError as exc:
            raise ProtectedApplyJournalError(
                "could not publish protected component record"
            ) from exc
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)

    def _read(self, path: Path) -> dict[str, object]:
        fd = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        try:
            _require_regular(fd, uid=self.service_uid)
            metadata = os.fstat(fd)
            if metadata.st_size > _MAX_RECORD_BYTES:
                raise ProtectedApplyJournalError("protected component record is too large")
            payload = os.read(fd, _MAX_RECORD_BYTES + 1)
        finally:
            os.close(fd)
        if len(payload) > _MAX_RECORD_BYTES:
            raise ProtectedApplyJournalError("protected component record is too large")
        try:
            value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise ProtectedApplyJournalError("protected component record is invalid") from exc
        if not isinstance(value, dict):
            raise ProtectedApplyJournalError("protected component record is invalid")
        return value


def _require_directory(path: Path, *, uid: int) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise ProtectedApplyJournalError("protected apply directory authority is unsafe")


def _require_regular(fd: int, *, uid: int) -> None:
    metadata = os.fstat(fd)
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != uid
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
        or metadata.st_nlink != 1
    ):
        raise ProtectedApplyJournalError("protected component file authority is unsafe")


def _open_directory(path: Path) -> int:
    return os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )


def _hash_json(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(dict(value), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _json_bytes(value: Mapping[str, object]) -> bytes:
    return (json.dumps(dict(value), sort_keys=True, separators=(",", ":")) + "\n").encode()


def _string(value: Mapping[str, object], key: str) -> str:
    item = value[key]
    if not isinstance(item, str):
        raise ValueError(f"protected component {key} must be a string")
    return item


def _integer(value: Mapping[str, object], key: str) -> int:
    item = value[key]
    if type(item) is not int:
        raise ValueError(f"protected component {key} must be an integer")
    return item


def _boolean(value: Mapping[str, object], key: str) -> bool:
    item = value[key]
    if type(item) is not bool:
        raise ValueError(f"protected component {key} must be a boolean")
    return item


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:  # pragma: no cover - os.write contract
            raise OSError("protected component write made no progress")
        offset += written


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("protected component record contains duplicate fields")
        value[key] = item
    return value


def read_component_failure(path: Path, *, service_uid: int) -> ComponentFailure:
    """Read one bounded service-owned failure record for the public broker."""
    if (
        not path.is_absolute()
        or ".." in path.parts
        or path.name != "failure.json"
        or service_uid < 0
    ):
        raise ProtectedApplyJournalError("protected component failure path is invalid")
    fd = os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0),
    )
    try:
        _require_regular(fd, uid=service_uid)
        metadata = os.fstat(fd)
        if metadata.st_size > _MAX_RECORD_BYTES:
            raise ProtectedApplyJournalError("protected component failure record is too large")
        payload = os.read(fd, _MAX_RECORD_BYTES + 1)
    finally:
        os.close(fd)
    if len(payload) > _MAX_RECORD_BYTES:
        raise ProtectedApplyJournalError("protected component failure record is too large")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        if not isinstance(value, dict):
            raise ValueError("failure record must be an object")
        return ComponentFailure.from_dict(value)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ProtectedApplyJournalError("protected component failure record is invalid") from exc


__all__ = [
    "ComponentFailure",
    "ComponentIntent",
    "ComponentObservation",
    "ComponentState",
    "ComponentTerminal",
    "ProtectedApplyComponent",
    "ProtectedApplyJournal",
    "ProtectedApplyJournalError",
    "read_component_failure",
]
