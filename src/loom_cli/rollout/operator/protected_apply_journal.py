"""Crash-safe component journal for one protected staging apply.

The outer final-gate journal cannot distinguish a crash immediately before a
mutation from a crash immediately after it.  This journal publishes an
immutable intent before each component, classifies live state, applies only a
component whose attested precondition is ready, and publishes terminal evidence only after live
state is exact.  A restart therefore verifies an in-flight intent instead of
blindly repeating protected work.
"""

from __future__ import annotations

import ctypes
import errno
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
from .protected_external_supervisor_transport import (
    COMPENSATION_RECONCILIATION_FAILURE_CODES,
    EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES,
    ExternalSupervisorApplyError,
    ExternalSupervisorCompensationError,
)

_COMPONENT_RE = re.compile(r"^[a-z][a-z0-9-]{2,63}$")
_GB10_HOST_RE = re.compile(r"^trt-gb10-(?:[1-9]|1[0-5])$")
_RECONCILIATION_COMPONENT_DIRECTORY_RE = re.compile(r"^\d{2}-external-supervisor-reconciliation$")
_RECONCILIATION_OUTCOME_FILE_RE = re.compile(r"^(?P<sequence>\d{8})\.json$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_RENAME_NOREPLACE = 1
_MAX_RECORD_BYTES = 256 * 1024
_MAX_FAILURE_DIAGNOSTIC_CHARS = 512
_MAX_RECONCILIATION_OUTCOMES = 1024
_MAX_RECONCILIATION_OUTCOME_BYTES = 4096
_LEGACY_FAILURE_DIAGNOSTIC_CODES = frozenset(
    {
        "apply-failed",
        "did-not-converge",
        "post-classify-failed",
        "pre-classify-failed",
        "terminal-classify-failed",
    }
)
_FAILURE_DIAGNOSTIC_CODES = _LEGACY_FAILURE_DIAGNOSTIC_CODES | {
    "compensation-reconciliation-failed"
}
_EXTERNAL_SUPERVISOR_COMPONENT_IDS = frozenset(
    {
        "external-supervisors",
        "external-supervisors-gb10",
        "external-supervisors-oldlab",
    }
)
_TYPED_APPLY_DIAGNOSTIC = "classified external-supervisor apply failure"
_TYPED_COMPENSATION_DIAGNOSTIC = (
    "classified external-supervisor compensation reconciliation failure"
)


class ProtectedApplyJournalError(RuntimeError):
    """Raised when protected component state is unsafe or ambiguous."""


class ComponentState(StrEnum):
    READY = "ready"
    EXACT = "exact"
    DRIFTED = "drifted"


class ReconciliationOutcomeStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"


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
    preapply_group: str | None = None
    reconcile_before_apply: bool = False

    def __post_init__(self) -> None:
        if (
            _COMPONENT_RE.fullmatch(self.component_id) is None
            or _SHA256_RE.fullmatch(self.implementation_digest) is None
            or _SHA256_RE.fullmatch(self.input_fingerprint) is None
            or (
                self.preapply_group is not None
                and _COMPONENT_RE.fullmatch(self.preapply_group) is None
            )
            or self.reconcile_before_apply
            != (self.component_id == "external-supervisor-reconciliation")
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
class ComponentFailureDiagnostic:
    schema_version: int
    component_id: str
    ordinal: int
    failure_code: str
    diagnostic: str
    primary_failure_code: str | None = None
    compensation_failure_code: str | None = None

    def __post_init__(self) -> None:
        if (
            self.schema_version not in {1, 2}
            or _COMPONENT_RE.fullmatch(self.component_id) is None
            or type(self.ordinal) is not int
            or not 0 <= self.ordinal < 32
            or self.failure_code not in _FAILURE_DIAGNOSTIC_CODES
            or not self.diagnostic
            or len(self.diagnostic) > _MAX_FAILURE_DIAGNOSTIC_CHARS
            or any(ord(char) < 32 or ord(char) == 127 for char in self.diagnostic)
        ):
            raise ValueError("protected component failure diagnostic is invalid")
        if self.schema_version == 1:
            if (
                self.failure_code not in _LEGACY_FAILURE_DIAGNOSTIC_CODES
                or self.primary_failure_code is not None
                or self.compensation_failure_code is not None
            ):
                raise ValueError("protected component failure diagnostic is invalid")
        elif self.failure_code == "apply-failed":
            if (
                self.component_id not in _EXTERNAL_SUPERVISOR_COMPONENT_IDS
                or self.primary_failure_code not in EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES
                or self.diagnostic != _TYPED_APPLY_DIAGNOSTIC
            ) or (
                self.compensation_failure_code is not None
                and self.compensation_failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES
            ):
                raise ValueError("protected component failure diagnostic is invalid")
        elif (
            self.failure_code != "compensation-reconciliation-failed"
            or self.component_id != "external-supervisor-reconciliation"
            or self.diagnostic != _TYPED_COMPENSATION_DIAGNOSTIC
            or self.primary_failure_code is not None
            or self.compensation_failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES
        ):
            raise ValueError("protected component failure diagnostic is invalid")

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema_version": self.schema_version,
            "component_id": self.component_id,
            "ordinal": self.ordinal,
            "failure_code": self.failure_code,
            "diagnostic": self.diagnostic,
        }
        if self.schema_version == 2:
            payload["primary_failure_code"] = self.primary_failure_code
            payload["compensation_failure_code"] = self.compensation_failure_code
        return payload

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ComponentFailureDiagnostic:
        base_fields = {
            "schema_version",
            "component_id",
            "ordinal",
            "failure_code",
            "diagnostic",
        }
        schema_version = value.get("schema_version")
        if type(schema_version) is not int:
            raise ValueError("protected component failure diagnostic schema is invalid")
        if set(value) != (
            base_fields
            if schema_version == 1
            else base_fields | {"primary_failure_code", "compensation_failure_code"}
        ):
            raise ValueError("protected component failure diagnostic fields are invalid")
        primary_failure_code = value.get("primary_failure_code")
        compensation_failure_code = value.get("compensation_failure_code")
        if (primary_failure_code is not None and not isinstance(primary_failure_code, str)) or (
            compensation_failure_code is not None and not isinstance(compensation_failure_code, str)
        ):
            raise ValueError("protected component typed failure diagnostic is invalid")
        return cls(
            schema_version=schema_version,
            component_id=_string(value, "component_id"),
            ordinal=_integer(value, "ordinal"),
            failure_code=_string(value, "failure_code"),
            diagnostic=_string(value, "diagnostic"),
            primary_failure_code=primary_failure_code,
            compensation_failure_code=compensation_failure_code,
        )


@dataclass(frozen=True, slots=True)
class ReconciliationOutcome:
    schema_version: int
    component_id: str
    sequence: int
    status: ReconciliationOutcomeStatus
    failure_code: str | None
    diagnostic: str | None
    compensation_failure_code: str | None

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or self.component_id != "external-supervisor-reconciliation"
            or type(self.sequence) is not int
            or not 0 <= self.sequence < _MAX_RECONCILIATION_OUTCOMES
            or not isinstance(self.status, ReconciliationOutcomeStatus)
        ):
            raise ValueError("protected reconciliation outcome is invalid")
        if self.status is ReconciliationOutcomeStatus.SUCCEEDED:
            if (
                self.failure_code is not None
                or self.diagnostic is not None
                or self.compensation_failure_code is not None
            ):
                raise ValueError("protected reconciliation outcome is invalid")
            return
        if self.failure_code == "compensation-reconciliation-failed":
            if (
                self.diagnostic != _TYPED_COMPENSATION_DIAGNOSTIC
                or self.compensation_failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES
            ):
                raise ValueError("protected reconciliation outcome is invalid")
        elif (
            self.failure_code not in _LEGACY_FAILURE_DIAGNOSTIC_CODES
            or self.diagnostic is None
            or not self.diagnostic
            or len(self.diagnostic) > _MAX_FAILURE_DIAGNOSTIC_CHARS
            or any(ord(char) < 32 or ord(char) == 127 for char in self.diagnostic)
            or self.compensation_failure_code is not None
        ):
            raise ValueError("protected reconciliation outcome is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "component_id": self.component_id,
            "sequence": self.sequence,
            "status": self.status.value,
            "failure_code": self.failure_code,
            "diagnostic": self.diagnostic,
            "compensation_failure_code": self.compensation_failure_code,
        }

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ReconciliationOutcome:
        if set(value) != {
            "schema_version",
            "component_id",
            "sequence",
            "status",
            "failure_code",
            "diagnostic",
            "compensation_failure_code",
        }:
            raise ValueError("protected reconciliation outcome fields are invalid")
        return cls(
            schema_version=_integer(value, "schema_version"),
            component_id=_string(value, "component_id"),
            sequence=_integer(value, "sequence"),
            status=ReconciliationOutcomeStatus(_string(value, "status")),
            failure_code=_optional_string(value, "failure_code"),
            diagnostic=_optional_string(value, "diagnostic"),
            compensation_failure_code=_optional_string(
                value,
                "compensation_failure_code",
            ),
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
        groups = self._validated_preapply_groups(components)
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
            self._validate_chain_layout(components)
            results: dict[str, ComponentTerminal] = {}
            preclassified_groups: set[str] = set()
            for ordinal, component in enumerate(components):
                if (
                    component.preapply_group is not None
                    and component.preapply_group not in preclassified_groups
                ):
                    self._preclassify_group(
                        plan,
                        groups[component.preapply_group],
                    )
                    preclassified_groups.add(component.preapply_group)
                results[component.component_id] = self._execute_one(plan, component, ordinal)
            for ordinal, component in enumerate(components):
                if component.reconcile_before_apply:
                    self._publish_or_match(
                        self.root / f"{ordinal:02d}-{component.component_id}" / "terminal.json",
                        results[component.component_id].to_dict(),
                    )
            return MappingProxyType(results)
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

    def _validate_chain_layout(
        self,
        components: Sequence[ProtectedApplyComponent],
    ) -> None:
        expected = {
            f"{ordinal:02d}-{component.component_id}"
            for ordinal, component in enumerate(components)
        }
        try:
            entries = tuple(self.root.iterdir())
        except OSError as exc:
            raise ProtectedApplyJournalError(
                "protected apply chain identity is unreadable"
            ) from exc
        for entry in entries:
            if entry.name == self.lock_path.name:
                continue
            if entry.name not in expected:
                raise ProtectedApplyJournalError("protected apply chain identity drifted")
            _require_directory(entry, uid=self.service_uid)

    def _validated_preapply_groups(
        self,
        components: Sequence[ProtectedApplyComponent],
    ) -> Mapping[str, tuple[ProtectedApplyComponent, ...]]:
        members: dict[str, list[tuple[int, ProtectedApplyComponent]]] = {}
        for ordinal, component in enumerate(components):
            if component.preapply_group is not None:
                members.setdefault(component.preapply_group, []).append((ordinal, component))
        validated: dict[str, tuple[ProtectedApplyComponent, ...]] = {}
        for group, grouped in members.items():
            ordinals = tuple(ordinal for ordinal, _component in grouped)
            if ordinals != tuple(range(ordinals[0], ordinals[-1] + 1)):
                raise ProtectedApplyJournalError("protected preapply group identity is invalid")
            validated[group] = tuple(component for _ordinal, component in grouped)
        return MappingProxyType(validated)

    def _preclassify_group(
        self,
        plan: FinalGatePlan,
        components: Sequence[ProtectedApplyComponent],
    ) -> None:
        observations = tuple(component.classify(plan) for component in components)
        drifted = tuple(
            component.component_id
            for component, observation in zip(components, observations, strict=True)
            if observation.state is ComponentState.DRIFTED
        )
        if drifted:
            raise ProtectedApplyJournalError("protected preapply group live state drifted")

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
        for ordinal in (0, 1, 2):
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
            if component.reconcile_before_apply:
                self._apply_with_diagnostic(
                    component_root,
                    component,
                    ordinal,
                    plan,
                )
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
            if component.reconcile_before_apply:
                self._append_reconciliation_outcome(
                    component_root,
                    status=ReconciliationOutcomeStatus.SUCCEEDED,
                    failure_code=None,
                    diagnostic=None,
                    compensation_failure_code=None,
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
        if before.state is ComponentState.READY or component.reconcile_before_apply:
            self._apply_with_diagnostic(
                component_root,
                component,
                ordinal,
                plan,
            )
            applied = True
        after = self._classify_with_diagnostic(
            component_root,
            component,
            ordinal,
            plan,
            failure_code="post-classify-failed",
        )
        if after.state is not ComponentState.EXACT:
            diagnostic = f"component classified {after.state.value} after apply"
            self._publish_failure_diagnostic(
                component_root,
                component,
                ordinal,
                failure_code="did-not-converge",
                diagnostic=diagnostic,
            )
            self._publish_reconciliation_outcome_best_effort(
                component_root,
                component,
                status=ReconciliationOutcomeStatus.FAILED,
                failure_code="did-not-converge",
                diagnostic=diagnostic,
                compensation_failure_code=None,
            )
            raise ProtectedApplyJournalError(
                f"protected component {component.component_id} did not converge exactly"
            )
        terminal = ComponentTerminal.build(intent, after, applied=applied)
        if component.reconcile_before_apply:
            self._append_reconciliation_outcome(
                component_root,
                status=ReconciliationOutcomeStatus.SUCCEEDED,
                failure_code=None,
                diagnostic=None,
                compensation_failure_code=None,
            )
        else:
            self._publish_or_match(terminal_path, terminal.to_dict())
        return terminal

    def _apply_with_diagnostic(
        self,
        component_root: Path,
        component: ProtectedApplyComponent,
        ordinal: int,
        plan: FinalGatePlan,
    ) -> None:
        try:
            component.apply(plan)
        except BaseException as exc:
            from .protected_gb10_transport import GB10FleetApplyError

            if component.component_id == "gb10-candidate" and isinstance(exc, GB10FleetApplyError):
                failure = ComponentFailure(
                    schema_version=1,
                    component_id=component.component_id,
                    failure_code="gb10-convergence-failed",
                    failed_hosts=exc.failed_hosts,
                )
                self._publish_or_match(component_root / "failure.json", failure.to_dict())
            elif isinstance(exc, ExternalSupervisorApplyError):
                self._publish_failure_diagnostic(
                    component_root,
                    component,
                    ordinal,
                    failure_code="apply-failed",
                    diagnostic=_TYPED_APPLY_DIAGNOSTIC,
                    primary_failure_code=exc.failure_code,
                    compensation_failure_code=exc.compensation_failure_code,
                )
            elif isinstance(exc, ExternalSupervisorCompensationError):
                self._publish_failure_diagnostic(
                    component_root,
                    component,
                    ordinal,
                    failure_code="compensation-reconciliation-failed",
                    diagnostic=_TYPED_COMPENSATION_DIAGNOSTIC,
                    compensation_failure_code=exc.failure_code,
                )
                self._publish_reconciliation_outcome_best_effort(
                    component_root,
                    component,
                    status=ReconciliationOutcomeStatus.FAILED,
                    failure_code="compensation-reconciliation-failed",
                    diagnostic=_TYPED_COMPENSATION_DIAGNOSTIC,
                    compensation_failure_code=exc.failure_code,
                )
            else:
                # Every other component previously published no failure record
                # at all, leaving its cause a masked dead-end (#1081). Record a
                # coded, secret-safe reason (#1085 p1).
                diagnostic = unclassified_failure_diagnostic(
                    exc,
                    activity=component.component_id,
                )
                self._publish_failure_diagnostic(
                    component_root,
                    component,
                    ordinal,
                    failure_code="apply-failed",
                    diagnostic=diagnostic,
                )
                self._publish_reconciliation_outcome_best_effort(
                    component_root,
                    component,
                    status=ReconciliationOutcomeStatus.FAILED,
                    failure_code="apply-failed",
                    diagnostic=diagnostic,
                    compensation_failure_code=None,
                )
            raise

    def _publish_reconciliation_outcome_best_effort(
        self,
        component_root: Path,
        component: ProtectedApplyComponent,
        *,
        status: ReconciliationOutcomeStatus,
        failure_code: str,
        diagnostic: str,
        compensation_failure_code: str | None,
    ) -> None:
        if not component.reconcile_before_apply:
            return
        try:
            self._append_reconciliation_outcome(
                component_root,
                status=status,
                failure_code=failure_code,
                diagnostic=diagnostic,
                compensation_failure_code=compensation_failure_code,
            )
        except Exception:
            pass

    def _append_reconciliation_outcome(
        self,
        component_root: Path,
        *,
        status: ReconciliationOutcomeStatus,
        failure_code: str | None,
        diagnostic: str | None,
        compensation_failure_code: str | None,
    ) -> None:
        outcomes_root = component_root / "reconciliation-outcomes"
        try:
            outcomes_root.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError:
            pass
        except OSError as exc:
            raise ProtectedApplyJournalError(
                "could not create protected reconciliation outcome journal"
            ) from exc
        _require_directory(outcomes_root, uid=self.service_uid)
        existing = _read_reconciliation_outcomes(
            component_root,
            service_uid=self.service_uid,
        )
        if len(existing) >= _MAX_RECONCILIATION_OUTCOMES:
            raise ProtectedApplyJournalError(
                "protected reconciliation outcome journal is too large"
            )
        sequence = len(existing)
        outcome = ReconciliationOutcome(
            schema_version=1,
            component_id="external-supervisor-reconciliation",
            sequence=sequence,
            status=status,
            failure_code=failure_code,
            diagnostic=diagnostic,
            compensation_failure_code=compensation_failure_code,
        )
        self._publish_reconciliation_outcome(
            outcomes_root / f"{sequence:08d}.json",
            outcome.to_dict(),
        )

    def _publish_reconciliation_outcome(
        self,
        path: Path,
        value: Mapping[str, object],
    ) -> None:
        payload = _json_bytes(value)
        if (
            len(payload) > _MAX_RECONCILIATION_OUTCOME_BYTES
            or path.parent.name != "reconciliation-outcomes"
            or _RECONCILIATION_COMPONENT_DIRECTORY_RE.fullmatch(path.parent.parent.name) is None
        ):
            raise ProtectedApplyJournalError(
                "protected reconciliation outcome publication is invalid"
            )
        source_directory_fd = _open_directory(path.parent.parent)
        try:
            destination_directory_fd = _open_directory(path.parent)
        except BaseException:
            os.close(source_directory_fd)
            raise
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
                dir_fd=source_directory_fd,
            )
            created = True
            try:
                os.fchmod(fd, _PRIVATE_FILE_MODE)
                _write_all(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            _rename_noreplace(
                source_directory_fd,
                temporary,
                destination_directory_fd,
                path.name,
            )
            created = False
            os.fsync(destination_directory_fd)
            os.fsync(source_directory_fd)
        except FileExistsError:
            if self._read(path) != dict(value):
                raise ProtectedApplyJournalError(
                    "protected reconciliation outcome cannot be replaced"
                ) from None
        except OSError as exc:
            raise ProtectedApplyJournalError(
                "could not publish protected reconciliation outcome"
            ) from exc
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=source_directory_fd)
                except OSError:
                    pass
            os.close(destination_directory_fd)
            os.close(source_directory_fd)

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
            diagnostic = unclassified_failure_diagnostic(
                exc,
                activity=component.component_id,
            )
            self._publish_failure_diagnostic(
                component_root,
                component,
                ordinal,
                failure_code=failure_code,
                diagnostic=diagnostic,
            )
            self._publish_reconciliation_outcome_best_effort(
                component_root,
                component,
                status=ReconciliationOutcomeStatus.FAILED,
                failure_code=failure_code,
                diagnostic=diagnostic,
                compensation_failure_code=None,
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
        primary_failure_code: str | None = None,
        compensation_failure_code: str | None = None,
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
        record = ComponentFailureDiagnostic(
            schema_version=(
                2
                if primary_failure_code is not None or compensation_failure_code is not None
                else 1
            ),
            component_id=component.component_id,
            ordinal=ordinal,
            failure_code=failure_code,
            diagnostic=diagnostic,
            primary_failure_code=primary_failure_code,
            compensation_failure_code=compensation_failure_code,
        ).to_dict()
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


def _rename_noreplace(
    source_directory_fd: int,
    source: str,
    destination_directory_fd: int,
    destination: str,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise ProtectedApplyJournalError(
            "atomic protected reconciliation outcome publication is unavailable"
        )
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        source_directory_fd,
        os.fsencode(source),
        destination_directory_fd,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error in {errno.EEXIST, errno.ENOTEMPTY}:
            raise FileExistsError(error, os.strerror(error), destination)
        raise OSError(error, os.strerror(error), destination)


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


def _optional_string(value: Mapping[str, object], key: str) -> str | None:
    item = value[key]
    if item is not None and not isinstance(item, str):
        raise ValueError(f"protected component {key} must be a string or null")
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


def _read_service_component_record(
    path: Path,
    *,
    service_uid: int,
    filename: str,
    max_bytes: int = _MAX_RECORD_BYTES,
) -> dict[str, object]:
    if (
        not path.is_absolute()
        or ".." in path.parts
        or path.name != filename
        or service_uid < 0
        or not 0 < max_bytes <= _MAX_RECORD_BYTES
    ):
        raise ProtectedApplyJournalError("protected component record path is invalid")
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
        if metadata.st_size > max_bytes:
            raise ProtectedApplyJournalError("protected component record is too large")
        payload = os.read(fd, max_bytes + 1)
    finally:
        os.close(fd)
    if len(payload) > max_bytes:
        raise ProtectedApplyJournalError("protected component record is too large")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
        if not isinstance(value, dict):
            raise ValueError("component record must be an object")
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ProtectedApplyJournalError("protected component record is invalid") from exc
    return value


def read_component_failure(path: Path, *, service_uid: int) -> ComponentFailure:
    """Read one bounded service-owned failure record for the public broker."""
    try:
        return ComponentFailure.from_dict(
            _read_service_component_record(
                path,
                service_uid=service_uid,
                filename="failure.json",
            )
        )
    except ValueError as exc:
        raise ProtectedApplyJournalError("protected component failure record is invalid") from exc


def read_component_failure_diagnostic(
    path: Path,
    *,
    service_uid: int,
) -> ComponentFailureDiagnostic:
    """Read one bounded service-owned secret-safe failure diagnostic."""
    try:
        return ComponentFailureDiagnostic.from_dict(
            _read_service_component_record(
                path,
                service_uid=service_uid,
                filename="failure-diagnostic.json",
            )
        )
    except ValueError as exc:
        raise ProtectedApplyJournalError(
            "protected component failure diagnostic is invalid"
        ) from exc


def _read_reconciliation_outcomes(
    component_root: Path,
    *,
    service_uid: int,
) -> tuple[ReconciliationOutcome, ...]:
    if (
        not component_root.is_absolute()
        or ".." in component_root.parts
        or _RECONCILIATION_COMPONENT_DIRECTORY_RE.fullmatch(component_root.name) is None
        or service_uid < 0
    ):
        raise ProtectedApplyJournalError("protected reconciliation outcome path is invalid")
    _require_directory(component_root, uid=service_uid)
    outcomes_root = component_root / "reconciliation-outcomes"
    try:
        _require_directory(outcomes_root, uid=service_uid)
    except FileNotFoundError:
        return ()
    try:
        entries = tuple(os.scandir(outcomes_root))
    except OSError as exc:
        raise ProtectedApplyJournalError(
            "protected reconciliation outcome journal is unavailable"
        ) from exc
    if len(entries) > _MAX_RECONCILIATION_OUTCOMES:
        raise ProtectedApplyJournalError("protected reconciliation outcome journal is too large")
    paths: list[tuple[int, Path]] = []
    for entry in entries:
        match = _RECONCILIATION_OUTCOME_FILE_RE.fullmatch(entry.name)
        if match is None or entry.is_symlink() or not entry.is_file(follow_symlinks=False):
            raise ProtectedApplyJournalError("protected reconciliation outcome journal is unsafe")
        paths.append((int(match.group("sequence")), Path(entry.path)))
    paths.sort()
    if [sequence for sequence, _path in paths] != list(range(len(paths))):
        raise ProtectedApplyJournalError("protected reconciliation outcome sequence is invalid")
    outcomes: list[ReconciliationOutcome] = []
    for sequence, path in paths:
        try:
            outcome = ReconciliationOutcome.from_dict(
                _read_service_component_record(
                    path,
                    service_uid=service_uid,
                    filename=path.name,
                    max_bytes=_MAX_RECONCILIATION_OUTCOME_BYTES,
                )
            )
        except ValueError as exc:
            raise ProtectedApplyJournalError(
                "protected reconciliation outcome record is invalid"
            ) from exc
        if outcome.sequence != sequence:
            raise ProtectedApplyJournalError("protected reconciliation outcome identity drifted")
        outcomes.append(outcome)
    return tuple(outcomes)


def read_latest_reconciliation_outcome(
    component_root: Path,
    *,
    service_uid: int,
) -> ReconciliationOutcome | None:
    """Read the newest certified append-only reconciliation outcome."""
    outcomes = _read_reconciliation_outcomes(component_root, service_uid=service_uid)
    return outcomes[-1] if outcomes else None


__all__ = [
    "ComponentFailure",
    "ComponentFailureDiagnostic",
    "ComponentIntent",
    "ComponentObservation",
    "ComponentState",
    "ComponentTerminal",
    "ProtectedApplyComponent",
    "ProtectedApplyJournal",
    "ProtectedApplyJournalError",
    "ReconciliationOutcome",
    "ReconciliationOutcomeStatus",
    "read_component_failure",
    "read_component_failure_diagnostic",
    "read_latest_reconciliation_outcome",
]
