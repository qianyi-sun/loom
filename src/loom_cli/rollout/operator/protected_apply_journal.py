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
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from .protected_application_restoration import (
        ApplicationRestorationEvidence,
        ApplicationRestorationRunner,
    )
    from .staging_mutation_guard import MutationGuardEvidence

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
)

from .failure_diagnostics import unclassified_failure_diagnostic
from .final_gate_plan import FinalGatePlan
from .model import validate_safe_identifier
from .protected_application_admission_recovery import (
    MAX_HANDOFF_RECOVERIES,
    ApplicationAdmissionRecoveryRecord,
    ApplicationHandoffRecoveryIntent,
    ApplicationHandoffReplacementReceipt,
    admission_record_digest,
    require_replacement_identity,
)
from .protected_application_credential_recovery import ApplicationCredentialRecoveryBinding
from .protected_application_owner_preparation import (
    MAX_OWNER_CREATIONS,
    ApplicationOwnerCreationIntent,
)
from .protected_application_workloads import ApplicationWorkload, validate_workload_inventory
from .protected_cnpg_fence_recovery import (
    CNPGFenceCreateIntent,
    CNPGFenceObjectReceipt,
    CNPGFenceRequest,
)
from .protected_cnpg_manager_replacement import (
    CNPGManagerIdentity,
    CNPGManagerReplacementIntent,
    CNPGManagerReplacementReceipt,
)
from .protected_cnpg_runtime_admission import CNPGPrimaryRuntime
from .protected_cnpg_writer_configuration import CNPGWriterConfigurationBinding
from .protected_external_supervisor_transport import (
    COMPENSATION_RECONCILIATION_FAILURE_CODES,
    EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES,
    ExternalSupervisorApplyError,
    ExternalSupervisorCompensationError,
)

_COMPONENT_PATTERN = r"[a-z][a-z0-9-]{2,63}"
_COMPONENT_RE = re.compile(rf"^{_COMPONENT_PATTERN}$")
_COMPONENT_DIRECTORY_RE = re.compile(
    rf"^(?P<ordinal>\d{{2}})-(?P<component_id>{_COMPONENT_PATTERN})$"
)
_GB10_HOST_RE = re.compile(r"^trt-gb10-(?:[1-9]|1[0-5])$")
_RECONCILIATION_COMPONENT_DIRECTORY_RE = re.compile(r"^\d{2}-external-supervisor-reconciliation$")
_RECONCILIATION_OUTCOME_FILE_RE = re.compile(r"^(?P<sequence>\d{8})\.json$")
_FAILURE_DIAGNOSTIC_FILE_RE = re.compile(r"^(?P<sequence>\d{8})\.json$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_GIT_SHA_RE = re.compile(r"^(?:[0-9a-f]{40}|[0-9a-f]{64})$")
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_RENAME_NOREPLACE = 1
_MAX_RECORD_BYTES = 256 * 1024
_MAX_FAILURE_DIAGNOSTIC_CHARS = 512
_MAX_FAILURE_DIAGNOSTICS = 1024
_MAX_FAILURE_DIAGNOSTIC_BYTES = 4096
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
_CLASSIFICATION_DRIFT_DIAGNOSTICS = {
    "pre-classify-failed": "component classified drifted before apply",
    "post-classify-failed": "component classified drifted after apply",
    "terminal-classify-failed": "terminal component classified drifted",
}


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
class ComponentTerminalRecoveryAuthority:
    schema_version: int
    component_id: str
    source_authority_incarnation: str
    target_authority_incarnation: str
    authority_digest: str

    def __post_init__(self) -> None:
        payload = {
            "schema_version": self.schema_version,
            "component_id": self.component_id,
            "source_authority_incarnation": self.source_authority_incarnation,
            "target_authority_incarnation": self.target_authority_incarnation,
        }
        if (
            self.schema_version != 1
            or _COMPONENT_RE.fullmatch(self.component_id) is None
            or not _canonical_nonzero_uuid(self.source_authority_incarnation)
            or not _canonical_nonzero_uuid(self.target_authority_incarnation)
            or self.source_authority_incarnation == self.target_authority_incarnation
            or _SHA256_RE.fullmatch(self.authority_digest) is None
            or _hash_json(payload) != self.authority_digest
        ):
            raise ValueError("protected component terminal recovery authority is invalid")

    @classmethod
    def build(
        cls,
        *,
        component_id: str,
        source_authority_incarnation: str,
        target_authority_incarnation: str,
    ) -> ComponentTerminalRecoveryAuthority:
        payload = {
            "schema_version": 1,
            "component_id": component_id,
            "source_authority_incarnation": source_authority_incarnation,
            "target_authority_incarnation": target_authority_incarnation,
        }
        return cls(
            schema_version=1,
            component_id=component_id,
            source_authority_incarnation=source_authority_incarnation,
            target_authority_incarnation=target_authority_incarnation,
            authority_digest=_hash_json(payload),
        )


@dataclass(frozen=True, slots=True)
class ProtectedApplyComponent:
    component_id: str
    implementation_digest: str
    input_fingerprint: str
    classify: Callable[[FinalGatePlan], ComponentObservation]
    apply: Callable[[FinalGatePlan], None]
    preapply_group: str | None = None
    reconcile_before_apply: bool = False
    terminal_recovery_authority: (
        Callable[
            [FinalGatePlan, ComponentTerminal, ComponentObservation],
            ComponentTerminalRecoveryAuthority | None,
        ]
        | None
    ) = None

    def __post_init__(self) -> None:
        if (
            _COMPONENT_RE.fullmatch(self.component_id) is None
            or _SHA256_RE.fullmatch(self.implementation_digest) is None
            or _SHA256_RE.fullmatch(self.input_fingerprint) is None
            or (
                self.preapply_group is not None
                and _COMPONENT_RE.fullmatch(self.preapply_group) is None
            )
            or (
                self.terminal_recovery_authority is not None
                and not callable(self.terminal_recovery_authority)
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
class _ComponentFailureDiagnosticEntry:
    schema_version: int
    component_id: str
    ordinal: int
    sequence: int
    failure_code: str
    diagnostic: str
    primary_failure_code: str | None
    compensation_failure_code: str | None

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or type(self.sequence) is not int
            or not 0 <= self.sequence < _MAX_FAILURE_DIAGNOSTICS
        ):
            raise ValueError("protected component failure diagnostic entry is invalid")
        self.as_diagnostic()

    def as_diagnostic(self) -> ComponentFailureDiagnostic:
        return ComponentFailureDiagnostic(
            schema_version=(
                2
                if self.primary_failure_code is not None
                or self.compensation_failure_code is not None
                else 1
            ),
            component_id=self.component_id,
            ordinal=self.ordinal,
            failure_code=self.failure_code,
            diagnostic=self.diagnostic,
            primary_failure_code=self.primary_failure_code,
            compensation_failure_code=self.compensation_failure_code,
        )

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> _ComponentFailureDiagnosticEntry:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected component failure diagnostic entry fields are invalid")
        return cls(
            schema_version=_integer(value, "schema_version"),
            component_id=_string(value, "component_id"),
            ordinal=_integer(value, "ordinal"),
            sequence=_integer(value, "sequence"),
            failure_code=_string(value, "failure_code"),
            diagnostic=_string(value, "diagnostic"),
            primary_failure_code=_optional_string(value, "primary_failure_code"),
            compensation_failure_code=_optional_string(
                value,
                "compensation_failure_code",
            ),
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


@dataclass(frozen=True, slots=True)
class ComponentTerminalRecoveryIntent:
    schema_version: int
    request_id: str
    attempt_number: int
    plan_digest: str
    candidate_sha: str
    candidate_tree: str
    component_id: str
    ordinal: int
    component_intent_digest: str
    prior_terminal_digest: str
    prior_evidence_digest: str
    source_authority_incarnation: str
    target_authority_incarnation: str
    observed_epoch: int
    authority_digest: str
    recovery_intent_digest: str

    def __post_init__(self) -> None:
        validate_safe_identifier(self.request_id, "request_id")
        if (
            self.schema_version != 1
            or type(self.attempt_number) is not int
            or self.attempt_number < 1
            or _SHA256_RE.fullmatch(self.plan_digest) is None
            or _GIT_SHA_RE.fullmatch(self.candidate_sha) is None
            or _GIT_SHA_RE.fullmatch(self.candidate_tree) is None
            or _COMPONENT_RE.fullmatch(self.component_id) is None
            or type(self.ordinal) is not int
            or not 0 <= self.ordinal < 32
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.component_intent_digest,
                    self.prior_terminal_digest,
                    self.prior_evidence_digest,
                    self.authority_digest,
                    self.recovery_intent_digest,
                )
            )
            or not _canonical_nonzero_uuid(self.source_authority_incarnation)
            or not _canonical_nonzero_uuid(self.target_authority_incarnation)
            or self.source_authority_incarnation == self.target_authority_incarnation
            or type(self.observed_epoch) is not int
            or self.observed_epoch < 0
        ):
            raise ValueError("protected component terminal recovery intent is invalid")
        try:
            authority = ComponentTerminalRecoveryAuthority.build(
                component_id=self.component_id,
                source_authority_incarnation=self.source_authority_incarnation,
                target_authority_incarnation=self.target_authority_incarnation,
            )
        except ValueError as exc:
            raise ValueError("protected component terminal recovery intent is invalid") from exc
        if authority.authority_digest != self.authority_digest:
            raise ValueError("protected component terminal recovery intent is invalid")

    @classmethod
    def build(
        cls,
        *,
        plan: FinalGatePlan,
        intent: ComponentIntent,
        terminal: ComponentTerminal,
        authority: ComponentTerminalRecoveryAuthority,
    ) -> ComponentTerminalRecoveryIntent:
        if (
            intent.request_id != plan.request_id
            or intent.attempt_number != plan.attempt_number
            or intent.plan_digest != plan.plan_digest
            or terminal.intent_digest != intent.intent_digest
            or terminal.component_id != intent.component_id
            or terminal.observed_epoch != plan.starting_mutation_epoch + 1
            or authority.component_id != intent.component_id
            or authority.target_authority_incarnation != plan.manager_authority_incarnation
        ):
            raise ValueError("protected component terminal recovery identity is invalid")
        payload = {
            "schema_version": 1,
            "request_id": plan.request_id,
            "attempt_number": plan.attempt_number,
            "plan_digest": plan.plan_digest,
            "candidate_sha": plan.candidate_sha,
            "candidate_tree": plan.candidate_tree,
            "component_id": intent.component_id,
            "ordinal": intent.ordinal,
            "component_intent_digest": intent.intent_digest,
            "prior_terminal_digest": terminal.terminal_digest,
            "prior_evidence_digest": terminal.evidence_digest,
            "source_authority_incarnation": authority.source_authority_incarnation,
            "target_authority_incarnation": authority.target_authority_incarnation,
            "observed_epoch": terminal.observed_epoch,
            "authority_digest": authority.authority_digest,
        }
        return cls.from_dict({**payload, "recovery_intent_digest": _hash_json(payload)})

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ComponentTerminalRecoveryIntent:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected component terminal recovery intent fields are invalid")
        recovery_intent = cls(
            schema_version=_integer(value, "schema_version"),
            request_id=_string(value, "request_id"),
            attempt_number=_integer(value, "attempt_number"),
            plan_digest=_string(value, "plan_digest"),
            candidate_sha=_string(value, "candidate_sha"),
            candidate_tree=_string(value, "candidate_tree"),
            component_id=_string(value, "component_id"),
            ordinal=_integer(value, "ordinal"),
            component_intent_digest=_string(value, "component_intent_digest"),
            prior_terminal_digest=_string(value, "prior_terminal_digest"),
            prior_evidence_digest=_string(value, "prior_evidence_digest"),
            source_authority_incarnation=_string(value, "source_authority_incarnation"),
            target_authority_incarnation=_string(value, "target_authority_incarnation"),
            observed_epoch=_integer(value, "observed_epoch"),
            authority_digest=_string(value, "authority_digest"),
            recovery_intent_digest=_string(value, "recovery_intent_digest"),
        )
        payload = {
            key: item
            for key, item in recovery_intent.to_dict().items()
            if key != "recovery_intent_digest"
        }
        if _hash_json(payload) != recovery_intent.recovery_intent_digest:
            raise ValueError("protected component terminal recovery intent content drifted")
        return recovery_intent


@dataclass(frozen=True, slots=True)
class ComponentTerminalRecovery:
    schema_version: int
    recovery_intent_digest: str
    component_id: str
    evidence_digest: str
    observed_epoch: int
    applied: bool
    effective_terminal_digest: str
    recovery_digest: str

    def __post_init__(self) -> None:
        if (
            self.schema_version != 1
            or _COMPONENT_RE.fullmatch(self.component_id) is None
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.recovery_intent_digest,
                    self.evidence_digest,
                    self.effective_terminal_digest,
                    self.recovery_digest,
                )
            )
            or type(self.observed_epoch) is not int
            or self.observed_epoch < 0
            or type(self.applied) is not bool
        ):
            raise ValueError("protected component terminal recovery is invalid")

    @classmethod
    def build(
        cls,
        recovery_intent: ComponentTerminalRecoveryIntent,
        terminal: ComponentTerminal,
    ) -> ComponentTerminalRecovery:
        payload = {
            "schema_version": 1,
            "recovery_intent_digest": recovery_intent.recovery_intent_digest,
            "component_id": recovery_intent.component_id,
            "evidence_digest": terminal.evidence_digest,
            "observed_epoch": terminal.observed_epoch,
            "applied": terminal.applied,
            "effective_terminal_digest": terminal.terminal_digest,
        }
        return cls.from_dict({**payload, "recovery_digest": _hash_json(payload)})

    def to_dict(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in self.__dataclass_fields__}

    @classmethod
    def from_dict(cls, value: Mapping[str, object]) -> ComponentTerminalRecovery:
        if set(value) != set(cls.__dataclass_fields__):
            raise ValueError("protected component terminal recovery fields are invalid")
        recovery = cls(
            schema_version=_integer(value, "schema_version"),
            recovery_intent_digest=_string(value, "recovery_intent_digest"),
            component_id=_string(value, "component_id"),
            evidence_digest=_string(value, "evidence_digest"),
            observed_epoch=_integer(value, "observed_epoch"),
            applied=_boolean(value, "applied"),
            effective_terminal_digest=_string(value, "effective_terminal_digest"),
            recovery_digest=_string(value, "recovery_digest"),
        )
        payload = {
            key: item for key, item in recovery.to_dict().items() if key != "recovery_digest"
        }
        if _hash_json(payload) != recovery.recovery_digest:
            raise ValueError("protected component terminal recovery content drifted")
        return recovery

    def effective_terminal(self, intent: ComponentIntent) -> ComponentTerminal:
        terminal = ComponentTerminal.build(
            intent,
            ComponentObservation(
                state=ComponentState.EXACT,
                evidence_digest=self.evidence_digest,
                observed_epoch=self.observed_epoch,
            ),
            applied=self.applied,
        )
        if terminal.terminal_digest != self.effective_terminal_digest:
            raise ValueError("protected component effective terminal content drifted")
        return terminal


@dataclass(frozen=True, slots=True)
class ApplicationRecoveryView:
    """Saved process recovery only; not live admission or a safe release outcome.

    Classification must reconcile these exact identities with current authority.
    An absent receipt preserves uncertainty, never permission to repeat a PUT.
    Credential, policy, SQL and workload observations are separately required.
    """

    intent: ComponentIntent
    admission: ApplicationAdmissionRecoveryRecord | None
    handoff_recoveries: tuple[
        tuple[ApplicationHandoffRecoveryIntent, ApplicationHandoffReplacementReceipt | None], ...
    ]
    manager_replacement: tuple[
        CNPGManagerReplacementIntent, bool, CNPGManagerReplacementReceipt | None
    ] | None
    workloads: tuple[ApplicationWorkload, ...] = ()
    workloads_restoring: bool = False
    owner_creations: tuple[tuple[ApplicationOwnerCreationIntent, int | None], ...] = ()
    cnpg_runtime: CNPGPrimaryRuntime | None = None
    credential_binding: ApplicationCredentialRecoveryBinding | None = None
    cnpg_configuration: CNPGWriterConfigurationBinding | None = None
    restoration: ApplicationRestorationEvidence | None = None
    fences_retiring: bool = False


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
        self._active_apply: tuple[Path, ComponentIntent] | None = None
        self._active_apply_owner: tuple[int, int] | None = None

    def _application_admission_context(self) -> tuple[Path, ComponentIntent]:
        if self._active_apply is None or self._active_apply_owner != (
            os.getpid(),
            threading.get_ident(),
        ):
            raise ProtectedApplyJournalError(
                "application admission requires active component apply"
            )
        root, intent = self._active_apply
        _require_directory(root, uid=self.service_uid)
        if ComponentIntent.from_dict(self._read(root / "intent.json")) != intent:
            raise ProtectedApplyJournalError("application admission component intent changed")
        return root, intent

    def read_application_admission_recovery(self) -> ApplicationAdmissionRecoveryRecord | None:
        """Read saved identity inside active apply; never recapture closed state on retry."""
        root, intent = self._application_admission_context()
        return self._read_application_admission(root, intent, durable=True)

    def _read_application_admission(
        self, root: Path, intent: ComponentIntent, *, durable: bool,
    ) -> ApplicationAdmissionRecoveryRecord | None:
        try:
            payload = self._read(root / "application-admission.json")
        except FileNotFoundError:
            return None
        try:
            record = ApplicationAdmissionRecoveryRecord.from_dict(payload)
        except ValueError:
            raise ProtectedApplyJournalError(
                "application admission recovery record is invalid"
            ) from None
        if record.intent_digest != intent.intent_digest:
            raise ProtectedApplyJournalError("application admission recovery intent changed")
        if durable:
            self._sync_application_recovery(root, "application-admission.json")
        return record

    def read_application_recovery_view(
        self, plan: FinalGatePlan, component: ProtectedApplyComponent, *, ordinal: int,
    ) -> ApplicationRecoveryView | None:
        """Read an exact handoff intent without creating or entering active apply.

        Immutable records may form a partial prefix. They are NOT dispatch or
        release authority. Active apply must re-read and flush them before any
        mutation. Concurrent publication detected during this read fails closed.
        """
        if (plan.request_id != self.request_id or plan.attempt_number != self.attempt_number
                or component.component_id != "application-ownership-handoff"
                or type(ordinal) is not int or not 0 <= ordinal < 32):
            raise ProtectedApplyJournalError("application recovery view binding is invalid")
        try:
            FinalGatePlan.from_dict(plan.to_dict())
        except ValueError:
            raise ProtectedApplyJournalError("application recovery view plan changed") from None
        root = self.root / f"{ordinal:02d}-{component.component_id}"
        for directory in (self.attempt_root, self.root):
            try:
                _require_directory(directory, uid=self.service_uid)
            except FileNotFoundError:
                return None
        if any(
            path.name.endswith("-application-ownership-handoff") and path != root
            for path in self.root.iterdir()
        ):
            raise ProtectedApplyJournalError("application recovery view ordinal changed")
        try:
            _require_directory(root, uid=self.service_uid)
        except FileNotFoundError:
            return None
        expected = ComponentIntent.build(plan, component, ordinal)
        try:
            observed = ComponentIntent.from_dict(self._read(root / "intent.json"))
        except (FileNotFoundError, ValueError):
            raise ProtectedApplyJournalError("application recovery view intent is invalid") from None
        if observed != expected:
            raise ProtectedApplyJournalError("application recovery view intent changed")
        return self._read_application_recovery_view(plan, root, expected, durable=False)

    def read_active_application_recovery_view(self, plan: FinalGatePlan) -> ApplicationRecoveryView:
        """Read and flush original phase records inside the owning component apply."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        if intent.component_id != "application-ownership-handoff":
            raise ProtectedApplyJournalError("application recovery requires the original handoff component")
        return self._read_application_recovery_view(plan, root, intent, durable=True)

    def _read_application_recovery_view(
        self, plan: FinalGatePlan, root: Path, expected: ComponentIntent, *, durable: bool,
    ) -> ApplicationRecoveryView:
        names = {path.name for path in root.iterdir() if path.name.startswith("application-")}
        admission = self._read_application_admission(root, expected, durable=False)
        if admission is None and any(
            name.startswith(("application-handoff-", "application-manager-")) for name in names
        ):
            raise ProtectedApplyJournalError("application recovery view lacks original admission")
        recoveries = (
            self._read_application_handoff_recoveries(root, admission, durable=False)
            if admission is not None else ()
        )
        manager = self._read_application_manager_replacement(
            root, expected, admission, durable=False,
        )
        if names != {path.name for path in root.iterdir() if path.name.startswith("application-")}:
            raise ProtectedApplyJournalError("application recovery view changed during read")
        workloads = self._read_application_workloads(root, expected, durable=False)
        if names != {path.name for path in root.iterdir() if path.name.startswith("application-")}:
            raise ProtectedApplyJournalError("application recovery view changed during workload read")
        restoring = self._read_workload_restoration(root, expected, workloads, durable=False)
        if names != {path.name for path in root.iterdir() if path.name.startswith("application-")}:
            raise ProtectedApplyJournalError("application recovery view changed during restoration read")
        owners = self._read_application_owner_creations(root, expected, durable=False)
        if names != {path.name for path in root.iterdir() if path.name.startswith("application-")}:
            raise ProtectedApplyJournalError("application recovery view changed during owner read")
        runtime = self._read_application_cnpg_runtime(root, expected, durable=False)
        if names != {path.name for path in root.iterdir() if path.name.startswith("application-")}:
            raise ProtectedApplyJournalError("application recovery view changed during runtime read")
        credentials = self._read_application_source_binding(root, expected, "application-credentials.json")
        configuration = self._read_application_source_binding(root, expected, "application-cnpg-configuration.json")
        binding = ApplicationCredentialRecoveryBinding.from_dict(credentials) if credentials is not None else None
        cnpg = CNPGWriterConfigurationBinding.from_dict(configuration) if configuration is not None else None
        if binding is not None and (
            binding.manifest_sha256 != plan.backup_manifest_sha256 or plan.checkpoint_component_sha256 is None
            or binding.component_sha256 != plan.checkpoint_component_sha256["k8s_secrets"]
        ):
            raise ProtectedApplyJournalError("application recovery credential source changed")
        if runtime is not None and cnpg is not None and runtime.cluster_uid != cnpg.cluster_uid:
            raise ProtectedApplyJournalError("application recovery CNPG Cluster changed")
        if names != {path.name for path in root.iterdir() if path.name.startswith("application-")}:
            raise ProtectedApplyJournalError("application recovery view changed during source read")
        view = ApplicationRecoveryView(expected, admission, recoveries, manager, workloads, restoring, owners, runtime, binding, cnpg)
        try:
            record = self._read(root / "application-restoration.json")
        except FileNotFoundError:
            record = None
        if record is not None:
            from .protected_application_restoration import (
                ApplicationRestorationEvidence,
                _bound_evidence,
            )

            try:
                restoration = ApplicationRestorationEvidence.from_dict(record)
                if restoration != _bound_evidence(view):
                    raise ValueError("restoration binding changed")
            except (ValueError, RuntimeError):
                raise ProtectedApplyJournalError("application restoration record binding changed") from None
            view = replace(view, restoration=restoration)
        try:
            retirement = self._read(root / "application-cnpg-fence-retirement.json")
        except FileNotFoundError:
            retirement = None
        if retirement is not None:
            if (type(retirement.get("schema_version")) is not int
                    or retirement != self._application_fence_retirement_record(root, view)):
                raise ProtectedApplyJournalError("application fence retirement binding changed")
            view = replace(view, fences_retiring=True)
        if names != {path.name for path in root.iterdir() if path.name.startswith("application-")}:
            raise ProtectedApplyJournalError("application recovery view changed during outcome read")
        if durable:
            for name in sorted(names):
                self._sync_application_recovery(root, name)
        return view

    def observe_and_record_application_restoration(
        self, plan: FinalGatePlan, *, runner: ApplicationRestorationRunner, guard: MutationGuardEvidence,
    ) -> ApplicationRestorationEvidence:
        """Publish only actual combined restoration under the original retained guard.

        Every retry repeats the fixed observer, including after a lost fsync reply.
        This is historical evidence, not a terminal or standalone release permit.
        The enclosing admitted operation preserves continuous writer exclusion.
        """
        from .protected_application_restoration import observe_application_restoration

        self.require_application_guard_retained(plan, guard=guard)
        view = self.read_active_application_recovery_view(plan)
        evidence = observe_application_restoration(plan, view=view, runner=runner, guard=guard,
                                                   service_uid=self.service_uid)
        self.require_application_guard_retained(plan, guard=guard)
        if self.read_active_application_recovery_view(plan) != view:
            raise ProtectedApplyJournalError("application restoration records changed during observation")
        root, _ = self._application_admission_context()
        self._publish_or_match(root / "application-restoration.json", evidence.to_dict())
        observed = self.read_active_application_recovery_view(plan).restoration
        if observed != evidence:
            raise ProtectedApplyJournalError("application restoration durable readback changed")
        return evidence

    def _application_fence_retirement_record(
        self, root: Path, view: ApplicationRecoveryView,
    ) -> dict[str, object]:
        if view.restoration is None:
            raise ProtectedApplyJournalError("application fence retirement lacks observed restoration")
        try:
            request = CNPGFenceRequest.from_dict(self._read(root / "application-cnpg-fence-request.json"))
            if request.intent_digest != view.intent.intent_digest:
                raise ValueError("fence intent changed")
            objects = []
            for ordinal, _ in enumerate(request.documents()):
                pending = CNPGFenceCreateIntent.from_dict(self._read(root / f"application-cnpg-fence-{ordinal:02d}-create.json"))
                receipt = CNPGFenceObjectReceipt.from_dict(self._read(root / f"application-cnpg-fence-{ordinal:02d}-object.json"))
                pending.document(request)
                if (pending.ordinal != ordinal or receipt.ordinal != ordinal
                        or receipt.intent_digest != request.intent_digest
                        or receipt.document_sha256 != request.document_sha256(ordinal)):
                    raise ValueError("fence object binding changed")
                objects.append({"create": pending.to_dict(), "object": receipt.to_dict()})
        except (FileNotFoundError, ValueError):
            raise ProtectedApplyJournalError("application fence retirement original inventory changed") from None
        return {"schema_version": 1, "intent_digest": view.intent.intent_digest,
                "restoration_sha256": view.restoration.digest,
                "request_sha256": admission_record_digest(request.to_dict()),
                "inventory_sha256": _hash_json({"objects": objects})}

    def begin_application_cnpg_fence_retirement(
        self, plan: FinalGatePlan, *, guard: MutationGuardEvidence,
    ) -> None:
        """Flush monotonic retirement direction after live inventory/restoration checks.

        The fixed retirement executor performs those checks immediately before
        this marker. The record binds all original CREATE nonces and object UIDs;
        it never accepts a caller-supplied success boolean or replacement evidence.
        """
        self.require_application_guard_retained(plan, guard=guard)
        view = self.read_active_application_recovery_view(plan)
        root, _ = self._application_admission_context()
        record = self._application_fence_retirement_record(root, view)
        self._publish_or_match(root / "application-cnpg-fence-retirement.json", record)
        if not self.read_active_application_recovery_view(plan).fences_retiring:
            raise ProtectedApplyJournalError("application fence retirement durable readback changed")


    def _read_application_source_binding(
        self, root: Path, intent: ComponentIntent, filename: str,
    ) -> dict[str, object] | None:
        if filename not in {"application-credentials.json", "application-cnpg-configuration.json"}:
            raise ProtectedApplyJournalError("application recovery source is invalid")
        try:
            record = self._read(root / filename)
        except FileNotFoundError:
            return None
        if (set(record) != {"schema_version", "intent_digest", "binding"}
                or type(record["schema_version"]) is not int or record["schema_version"] != 1
                or record["intent_digest"] != intent.intent_digest or not isinstance(record["binding"], dict)):
            raise ProtectedApplyJournalError("application recovery source binding changed")
        return record["binding"]

    def _sync_application_recovery(self, root: Path, filename: str) -> None:
        # A prior publisher can exit after making its link visible but BEFORE
        # fsync. A matching file is not proof of durable publication on retry.
        for path in (root / "intent.json", root / filename):
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                _require_regular(fd, uid=self.service_uid)
                os.fsync(fd)
            finally:
                os.close(fd)
        # Persist both file entries and newly-created component/journal directories.
        # The admitted attempt directory itself belongs to the outer plan store.
        for path in (root, self.root, self.attempt_root):
            _require_directory(path, uid=self.service_uid)
            fd = _open_directory(path)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)

    def read_application_handoff_recoveries(
        self,
    ) -> tuple[tuple[ApplicationHandoffRecoveryIntent, ApplicationHandoffReplacementReceipt | None], ...]:
        """Validate and flush the entire append-only chain before recovery decisions."""
        root, _active = self._application_admission_context()
        original = self.read_application_admission_recovery()
        if original is None:
            raise ProtectedApplyJournalError("application handoff recovery requires original admission")
        return self._read_application_handoff_recoveries(root, original, durable=True)

    def _read_application_handoff_recoveries(
        self, root: Path, original: ApplicationAdmissionRecoveryRecord, *, durable: bool,
    ) -> tuple[tuple[ApplicationHandoffRecoveryIntent, ApplicationHandoffReplacementReceipt | None], ...]:
        names = {path.name for path in root.iterdir() if path.name.startswith("application-handoff-")}
        allowed = {
            f"application-handoff-{ordinal:02d}-{kind}.json"
            for ordinal in range(1, MAX_HANDOFF_RECOVERIES + 1) for kind in ("intent", "peer")
        }
        if not names <= allowed:
            raise ProtectedApplyJournalError("application handoff recovery layout changed")
        previous_digest = admission_record_digest(original.to_dict())
        prior_backends = [original.handoff_backend]
        records: list[tuple[ApplicationHandoffRecoveryIntent, ApplicationHandoffReplacementReceipt | None]] = []
        consumed: set[str] = set()
        try:
            for ordinal in range(1, MAX_HANDOFF_RECOVERIES + 1):
                intent_name = f"application-handoff-{ordinal:02d}-intent.json"
                peer_name = f"application-handoff-{ordinal:02d}-peer.json"
                if intent_name not in names:
                    break
                intent = ApplicationHandoffRecoveryIntent.from_dict(self._read(root / intent_name))
                if intent != ApplicationHandoffRecoveryIntent(ordinal, previous_digest):
                    raise ValueError("chain binding changed")
                if durable:
                    self._sync_application_recovery(root, intent_name)
                consumed.add(intent_name)
                receipt = None
                if peer_name in names:
                    receipt = ApplicationHandoffReplacementReceipt.from_dict(self._read(root / peer_name))
                    if receipt.recovery_intent_digest != intent.digest:
                        raise ValueError("receipt binding changed")
                    require_replacement_identity(original, prior_backends, receipt.handoff_backend)
                    if durable:
                        self._sync_application_recovery(root, peer_name)
                    consumed.add(peer_name)
                    previous_digest = receipt.digest
                    prior_backends.append(receipt.handoff_backend)
                records.append((intent, receipt))
                if receipt is None:
                    break
        except ValueError:
            raise ProtectedApplyJournalError("application handoff recovery chain identity is invalid") from None
        if names != consumed:
            raise ProtectedApplyJournalError("application handoff recovery chain has gaps or pending successor")
        return tuple(records)

    def prepare_application_handoff_recovery(self, *, ordinal: int) -> ApplicationHandoffRecoveryIntent:
        """Persist intent BEFORE reopening; retry an explicit ordinal, never silently advance."""
        records = self.read_application_handoff_recoveries()
        if type(ordinal) is not int or not 1 <= ordinal <= MAX_HANDOFF_RECOVERIES or ordinal > len(records) + 1:
            raise ProtectedApplyJournalError("application handoff recovery ordinal is invalid")
        if ordinal <= len(records):
            return records[ordinal - 1][0]
        if records and records[-1][1] is None:
            raise ProtectedApplyJournalError("application handoff recovery is pending")
        root, _active = self._application_admission_context()
        original = self.read_application_admission_recovery()
        assert original is not None
        previous = records[-1][1] if records else None
        intent = ApplicationHandoffRecoveryIntent(
            ordinal, previous.digest if previous is not None else admission_record_digest(original.to_dict()),
        )
        self._publish_or_match(root / f"application-handoff-{ordinal:02d}-intent.json", intent.to_dict())
        if self.read_application_handoff_recoveries()[-1] != (intent, None):
            raise ProtectedApplyJournalError("application handoff recovery intent readback changed")
        return intent

    def record_application_handoff_replacement(
        self, *, ordinal: int, handoff_backend: ApplicationDatabaseHandoffBackend,
    ) -> ApplicationHandoffReplacementReceipt:
        """Save an independently admitted peer; caller must still reclose and exact-peer drain."""
        records = self.read_application_handoff_recoveries()
        if type(ordinal) is not int or not 1 <= ordinal <= len(records):
            raise ProtectedApplyJournalError("application handoff recovery intent must precede peer receipt")
        root, _active = self._application_admission_context()
        original = self.read_application_admission_recovery()
        assert original is not None
        intent, existing = records[ordinal - 1]
        receipt = ApplicationHandoffReplacementReceipt(intent.digest, handoff_backend)
        if existing is not None and existing != receipt:
            raise ProtectedApplyJournalError("application handoff recovery peer cannot be replaced")
        prior = [original.handoff_backend] + [
            peer.handoff_backend for _, peer in records[:ordinal - 1] if peer is not None
        ]
        try:
            require_replacement_identity(original, prior, handoff_backend)
        except ValueError:
            raise ProtectedApplyJournalError("application handoff replacement identity is invalid") from None
        self._publish_or_match(root / f"application-handoff-{ordinal:02d}-peer.json", receipt.to_dict())
        if self.read_application_handoff_recoveries()[ordinal - 1][1] != receipt:
            raise ProtectedApplyJournalError("application handoff recovery peer readback changed")
        return receipt

    def require_application_credential_context(self, plan: FinalGatePlan) -> None:
        """Require the current apply intent before any sensitive backup/live read."""
        _root, intent = self._application_admission_context()
        if (
            FinalGatePlan.from_dict(plan.to_dict()) != plan
            or plan.plan_digest != intent.plan_digest
            or plan.namespace != "loom-staging"
            or plan.checkpoint_schema_version != 3
            or plan.checkpoint_component_sha256 is None
        ):
            raise ProtectedApplyJournalError("application credential plan binding changed")

    def _read_application_workloads(
        self, root: Path, intent: ComponentIntent, *, durable: bool,
    ) -> tuple[ApplicationWorkload, ...]:
        late = sorted(root.glob("application-workload-job-*.json"))
        if len(late) > 120:
            raise ProtectedApplyJournalError("application workload inventory is unbounded")
        try:
            original = self._read(root / "application-workloads.json")
        except FileNotFoundError:
            if late:
                raise ProtectedApplyJournalError("application workloads lack their original inventory") from None
            return ()
        try:
            if (set(original) != {"schema_version", "intent_digest", "workloads"}
                    or type(original.get("schema_version")) is not int or original["schema_version"] != 1
                    or original["intent_digest"] != intent.intent_digest
                    or not isinstance(original["workloads"], list)):
                raise ValueError("invalid original workload inventory")
            values = original["workloads"]
            if any(not isinstance(value, dict) for value in values):
                raise ValueError("invalid workload record")
            workloads = [ApplicationWorkload.from_dict(value) for value in values]
            validate_workload_inventory(tuple(workloads))
            if durable:
                self._sync_application_recovery(root, "application-workloads.json")
            for path in late:
                record = self._read(path)
                if (set(record) != {"schema_version", "intent_digest", "workload"}
                        or type(record.get("schema_version")) is not int or record["schema_version"] != 1
                        or record["intent_digest"] != intent.intent_digest
                        or not isinstance(record["workload"], dict)):
                    raise ValueError("invalid late workload record")
                workload = ApplicationWorkload.from_dict(record["workload"])
                if workload.kind != "Job" or path.name != f"application-workload-job-{workload.uid}.json":
                    raise ValueError("late workload identity changed")
                workloads.append(workload)
                if durable:
                    self._sync_application_recovery(root, path.name)
            return validate_workload_inventory(tuple(workloads))
        except (ValueError, TypeError, KeyError):
            raise ProtectedApplyJournalError("application workload inventory changed or is invalid") from None

    def read_application_workloads(self, plan: FinalGatePlan) -> tuple[ApplicationWorkload, ...]:
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        return self._read_application_workloads(root, intent, durable=True)

    def record_application_workloads(
        self, plan: FinalGatePlan, *, workloads: tuple[ApplicationWorkload, ...],
    ) -> None:
        """Persist every original fixed writer before any workload is paused."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        if intent.component_id != "application-ownership-handoff":
            raise ProtectedApplyJournalError("application workloads require the original handoff component")
        values = validate_workload_inventory(workloads)
        record = {"schema_version": 1, "intent_digest": intent.intent_digest,
                  "workloads": [value.to_dict() for value in values]}
        self._publish_or_match(root / "application-workloads.json", record)
        self._read_application_workloads(root, intent, durable=True)

    def record_application_workload_job(self, plan: FinalGatePlan, *, workload: ApplicationWorkload) -> None:
        """Append a late owned Job; never recapture a paused parent's replicas."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        original = self._read_application_workloads(root, intent, durable=True)
        if not original:
            raise ProtectedApplyJournalError("application workload Job requires the original inventory")
        if intent.component_id != "application-ownership-handoff" or workload.kind != "Job":
            raise ProtectedApplyJournalError("application workload late extension must be an owned Job")
        for known in original:
            if (known.kind, known.name) == (workload.kind, workload.name) or known.uid == workload.uid:
                if known != workload:
                    raise ProtectedApplyJournalError("application workload saved Job cannot be replaced")
                return
        validate_workload_inventory((*original, workload))
        record = {"schema_version": 1, "intent_digest": intent.intent_digest, "workload": workload.to_dict()}
        self._publish_or_match(root / f"application-workload-job-{workload.uid}.json", record)
        self._read_application_workloads(root, intent, durable=True)

    def _workload_restoration_record(
        self, root: Path, intent: ComponentIntent, workloads: tuple[ApplicationWorkload, ...],
    ) -> dict[str, object]:
        admission = self._read_application_admission(root, intent, durable=False)
        if not workloads or admission is None or admission.coordination_guard is None:
            raise ProtectedApplyJournalError("application workload restoration lacks original authority")
        return {"schema_version": 1, "intent_digest": intent.intent_digest,
                "inventory_digest": _hash_json({"workloads": [value.to_dict() for value in workloads]}),
                "target": asdict(admission.target), "coordination_guard": asdict(admission.coordination_guard)}

    def _read_workload_restoration(
        self, root: Path, intent: ComponentIntent, workloads: tuple[ApplicationWorkload, ...], *, durable: bool,
    ) -> bool:
        filename = "application-workload-restoration.json"
        try:
            observed = self._read(root / filename)
        except FileNotFoundError:
            return False
        if (type(observed.get("schema_version")) is not int
                or observed != self._workload_restoration_record(root, intent, workloads)):
            raise ProtectedApplyJournalError("application workload restoration binding changed")
        if durable:
            self._sync_application_recovery(root, filename)
        return True

    def application_workloads_restoring(self, plan: FinalGatePlan) -> bool:
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        workloads = self._read_application_workloads(root, intent, durable=True)
        return self._read_workload_restoration(root, intent, workloads, durable=True)

    def begin_application_workload_restoration(self, plan: FinalGatePlan) -> None:
        """Publish recovery direction before restoring the first owned workload.

        The installed caller first repeats real database completion. This record
        prevents re-pausing on retry; it is not a cached database safe-outcome.
        """
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        workloads = self._read_application_workloads(root, intent, durable=True)
        record = self._workload_restoration_record(root, intent, workloads)
        self._publish_or_match(root / "application-workload-restoration.json", record)
        self._read_workload_restoration(root, intent, workloads, durable=True)

    def retain_application_guard(self, plan: FinalGatePlan, *, guard: MutationGuardEvidence) -> None:
        """Publish retention before sealing; acknowledgement is separately required."""
        from .protected_application_guard_retention import _REQUEST, _journal_context, _sync
        from .staging_mutation_guard import MutationGuardEvidence

        self.require_application_credential_context(plan)
        _, intent = self._application_admission_context()
        if (type(guard) is not MutationGuardEvidence or guard.candidate_sha != plan.candidate_sha
                or guard.candidate_tree != plan.candidate_tree):
            raise ProtectedApplyJournalError("application guard candidate binding changed")
        root, record = _journal_context(self, intent=intent, guard=guard,
                                        starting_epoch=plan.starting_mutation_epoch)
        _require_directory(root, uid=self.service_uid)
        self._publish_or_match(root / _REQUEST, record)
        _sync(root / _REQUEST)

    def require_application_guard_retained(self, plan: FinalGatePlan, *, guard: MutationGuardEvidence) -> None:
        """Refuse SQL mutation until the same supervised guard durably promises retention."""
        from .protected_application_guard_retention import (
            _ACK,
            _journal_context,
            _sync,
            application_guard_is_retained,
        )
        from .staging_mutation_guard import MutationGuardEvidence

        self.require_application_credential_context(plan)
        _, intent = self._application_admission_context()
        if (type(guard) is not MutationGuardEvidence or guard.candidate_sha != plan.candidate_sha
                or guard.candidate_tree != plan.candidate_tree):
            raise ProtectedApplyJournalError("application guard candidate binding changed")
        root, _ = _journal_context(self, intent=intent, guard=guard,
                                   starting_epoch=plan.starting_mutation_epoch)
        state_root = self.attempt_root.parents[3]
        if not application_guard_is_retained(state_root, request_id=self.request_id,
                                             service_uid=self.service_uid, guard=guard):
            raise ProtectedApplyJournalError("application guard retention is not pending")
        expected = {"schema_version": 1, "intent_digest": intent.intent_digest,
                    "guard_evidence_digest": guard.evidence_digest}
        try:
            ack = self._read(root / _ACK)
        except FileNotFoundError:
            raise ProtectedApplyJournalError("application guard acknowledgement is absent") from None
        if ack != expected:
            raise ProtectedApplyJournalError("application guard acknowledgement changed")
        _sync(root / _ACK)

    def record_application_cnpg_configuration(
        self, plan: FinalGatePlan, *, binding: CNPGWriterConfigurationBinding
    ) -> None:
        """Persist declared-writer inputs, not controller quiescence authority."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        if type(binding) is not CNPGWriterConfigurationBinding:
            raise ProtectedApplyJournalError("CNPG writer configuration binding is invalid")
        record = {"schema_version": 1, "intent_digest": intent.intent_digest, "binding": asdict(binding)}
        path = root / "application-cnpg-configuration.json"
        self._publish_or_match(path, record)
        observed = self._read(path)
        if json.dumps(observed, sort_keys=True) != json.dumps(record, sort_keys=True):
            raise ProtectedApplyJournalError("CNPG writer configuration readback changed")
        self._sync_application_recovery(root, path.name)

    def record_application_cnpg_runtime(self, plan: FinalGatePlan, *, runtime: CNPGPrimaryRuntime) -> None:
        """Bind original observations before any SQL mutation or manager dispatch."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        if type(runtime) is not CNPGPrimaryRuntime:
            raise ProtectedApplyJournalError("CNPG runtime binding is invalid")
        previous = self._read_application_cnpg_runtime(root, intent, durable=True)
        if previous is None and (
            self.read_application_admission_recovery() is not None or self.read_application_owner_creations(plan)
            or self.read_application_manager_replacement() is not None
        ):
            raise ProtectedApplyJournalError("CNPG runtime binding must precede application mutation")
        path = root / "application-cnpg-runtime.json"
        self._publish_or_match(path, {"schema_version": 1, "intent_digest": intent.intent_digest,
                                     "runtime": runtime.to_dict()})
        if self._read_application_cnpg_runtime(root, intent, durable=True) != runtime:
            raise ProtectedApplyJournalError("CNPG runtime binding readback changed")

    def read_application_cnpg_runtime(self, plan: FinalGatePlan) -> CNPGPrimaryRuntime | None:
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        return self._read_application_cnpg_runtime(root, intent, durable=True)

    def _read_application_cnpg_runtime(
        self, root: Path, intent: ComponentIntent, *, durable: bool,
    ) -> CNPGPrimaryRuntime | None:
        path = root / "application-cnpg-runtime.json"
        try:
            record = self._read(path)
        except FileNotFoundError:
            return None
        if (set(record) != {"schema_version", "intent_digest", "runtime"}
                or type(record["schema_version"]) is not int or record["schema_version"] != 1
                or record["intent_digest"] != intent.intent_digest or not isinstance(record["runtime"], dict)):
            raise ProtectedApplyJournalError("CNPG runtime binding changed")
        runtime = CNPGPrimaryRuntime.from_dict(record["runtime"])
        if durable:
            self._sync_application_recovery(root, path.name)
        return runtime

    def read_application_owner_creations(
        self, plan: FinalGatePlan,
    ) -> tuple[tuple[ApplicationOwnerCreationIntent, int | None], ...]:
        self.require_application_credential_context(plan)
        root, component = self._application_admission_context()
        return self._read_application_owner_creations(root, component, durable=True)

    def _read_application_owner_creations(
        self, root: Path, component: ComponentIntent, *, durable: bool,
    ) -> tuple[tuple[ApplicationOwnerCreationIntent, int | None], ...]:
        names = {path.name for path in root.iterdir() if path.name.startswith("application-owner-")}
        allowed = {f"application-owner-{i:02d}-{suffix}.json" for i in range(1, MAX_OWNER_CREATIONS + 1)
                   for suffix in ("intent", "oid")}
        if not names <= allowed:
            raise ProtectedApplyJournalError("application owner creation layout changed")
        previous, consumed = component.intent_digest, set()
        records: list[tuple[ApplicationOwnerCreationIntent, int | None]] = []
        for i in range(1, MAX_OWNER_CREATIONS + 1):
            name, receipt = f"application-owner-{i:02d}-intent.json", f"application-owner-{i:02d}-oid.json"
            if name not in names:
                break
            intent = ApplicationOwnerCreationIntent.from_dict(self._read(root / name))
            if intent.ordinal != i or intent.previous_record_digest != previous:
                raise ProtectedApplyJournalError("application owner creation chain changed")
            consumed.add(name)
            if durable:
                self._sync_application_recovery(root, name)
            oid = None
            if receipt in names:
                value = self._read(root / receipt)
                candidate = value.get("role_oid")
                if (set(value) != {"schema_version", "intent_digest", "role_oid"}
                        or type(value["schema_version"]) is not int or value["schema_version"] != 1
                        or value["intent_digest"] != intent.digest or type(candidate) is not int
                        or not 0 < candidate < 2**32 or candidate == intent.coordination_guard.role_oid):
                    raise ProtectedApplyJournalError("application owner creation OID receipt changed")
                oid = candidate
                consumed.add(receipt)
                if durable:
                    self._sync_application_recovery(root, receipt)
            records.append((intent, oid))
            previous = admission_record_digest({"intent": intent.to_dict(), "role_oid": oid})
        if names != consumed:
            raise ProtectedApplyJournalError("application owner creation chain is incomplete")
        return tuple(records)

    def prepare_application_owner_creation(
        self, plan: FinalGatePlan, *, backend: ApplicationDatabaseHandoffBackend,
        coordination_guard: ApplicationDatabaseCoordinationGuard,
    ) -> ApplicationOwnerCreationIntent:
        records = self.read_application_owner_creations(plan)
        root, component = self._application_admission_context()
        if records and any(item.coordination_guard != coordination_guard for item, _ in records):
            raise ProtectedApplyJournalError("application owner creation original guard changed")
        if records and records[-1][1] is None and records[-1][0].backend == backend:
            return records[-1][0]
        previous = (admission_record_digest({"intent": records[-1][0].to_dict(), "role_oid": records[-1][1]})
                    if records else component.intent_digest)
        intent = ApplicationOwnerCreationIntent(len(records) + 1, previous, backend, coordination_guard)
        name = f"application-owner-{intent.ordinal:02d}-intent.json"
        self._publish_or_match(root / name, intent.to_dict())
        if self.read_application_owner_creations(plan)[-1] != (intent, None):
            raise ProtectedApplyJournalError("application owner creation intent readback changed")
        return intent

    def record_application_owner_oid(self, plan: FinalGatePlan, *, ordinal: int, role_oid: int) -> None:
        records = self.read_application_owner_creations(plan)
        if not records or records[-1][0].ordinal != ordinal or type(role_oid) is not int or not 0 < role_oid < 2**32:
            raise ProtectedApplyJournalError("application owner creation requires its pending intent")
        root, _ = self._application_admission_context()
        name = f"application-owner-{ordinal:02d}-oid.json"
        self._publish_or_match(root / name, {"schema_version": 1, "intent_digest": records[-1][0].digest, "role_oid": role_oid})
        if self.read_application_owner_creations(plan)[-1][1] != role_oid:
            raise ProtectedApplyJournalError("application owner creation OID readback changed")

    def prepare_application_cnpg_fence(
        self, plan: FinalGatePlan, *,
        target_pooler_names: tuple[str, ...],
    ) -> CNPGFenceRequest:
        """Bind renderer inputs durably before any attempted policy installation."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        request = CNPGFenceRequest(intent.intent_digest, target_pooler_names)
        self._publish_or_match(root / "application-cnpg-fence-request.json", request.to_dict())
        observed = self.read_application_cnpg_fence(plan)
        if observed != request:
            raise ProtectedApplyJournalError("CNPG fence request readback changed")
        return request

    def read_application_cnpg_fence(self, plan: FinalGatePlan) -> CNPGFenceRequest | None:
        """Recover the exact guarded request, never adopt legacy restart authority."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        path = root / "application-cnpg-fence-request.json"
        try:
            value = self._read(path)
        except FileNotFoundError:
            return None
        request = CNPGFenceRequest.from_dict(value)
        if request.intent_digest != intent.intent_digest:
            raise ProtectedApplyJournalError("CNPG fence request intent changed")
        self._sync_application_recovery(root, path.name)
        return request

    def record_application_cnpg_fence_object(
        self, plan: FinalGatePlan, *, ordinal: int, uid: str,
    ) -> CNPGFenceObjectReceipt:
        """Persist caller-verified API identity; never infer ownership from a name."""
        request = self.read_application_cnpg_fence(plan)
        if request is None:
            raise ProtectedApplyJournalError("CNPG fence request must precede object identity")
        receipt = CNPGFenceObjectReceipt(request.intent_digest, ordinal, uid,
                                         request.document_sha256(ordinal))
        root, _intent = self._application_admission_context()
        path = root / f"application-cnpg-fence-{ordinal:02d}-object.json"
        self._publish_or_match(path, receipt.to_dict())
        observed = self.read_application_cnpg_fence_object(plan, ordinal=ordinal)
        if observed != receipt:
            raise ProtectedApplyJournalError("CNPG fence object readback changed")
        return receipt

    def prepare_application_cnpg_fence_create(
        self, plan: FinalGatePlan, *, ordinal: int,
    ) -> CNPGFenceCreateIntent:
        """Write ahead only after the caller observes authoritative absence.

        This records the caller's observation; it does not itself query the API
        or establish exclusive policy authority. Reuse the original nonce on retry.
        """
        request = self.read_application_cnpg_fence(plan)
        if request is None:
            raise ProtectedApplyJournalError("CNPG fence request must precede create intent")
        existing = self.read_application_cnpg_fence_create(plan, ordinal=ordinal)
        if existing is not None:
            return existing
        if self.read_application_cnpg_fence_object(plan, ordinal=ordinal) is not None:
            raise ProtectedApplyJournalError("CNPG fence known object cannot be recreated")
        intent = CNPGFenceCreateIntent.prepare(request, ordinal=ordinal, nonce=uuid4().hex)
        root, _active = self._application_admission_context()
        self._publish_or_match(root / f"application-cnpg-fence-{ordinal:02d}-create.json", intent.to_dict())
        observed = self.read_application_cnpg_fence_create(plan, ordinal=ordinal)
        if observed != intent:
            raise ProtectedApplyJournalError("CNPG fence create intent readback changed")
        return intent

    def read_application_cnpg_fence_create(
        self, plan: FinalGatePlan, *, ordinal: int,
    ) -> CNPGFenceCreateIntent | None:
        request = self.read_application_cnpg_fence(plan)
        if request is None:
            raise ProtectedApplyJournalError("CNPG fence request must precede create intent")
        request.document_sha256(ordinal)
        root, _active = self._application_admission_context()
        path = root / f"application-cnpg-fence-{ordinal:02d}-create.json"
        try:
            value = self._read(path)
        except FileNotFoundError:
            return None
        intent = CNPGFenceCreateIntent.from_dict(value)
        if intent.ordinal != ordinal:
            raise ProtectedApplyJournalError("CNPG fence create ordinal changed")
        intent.document(request)
        self._sync_application_recovery(root, path.name)
        return intent

    def read_application_cnpg_fence_object(
        self, plan: FinalGatePlan, *, ordinal: int,
    ) -> CNPGFenceObjectReceipt | None:
        request = self.read_application_cnpg_fence(plan)
        if request is None:
            raise ProtectedApplyJournalError("CNPG fence request must precede object identity")
        digest = request.document_sha256(ordinal)
        root, _intent = self._application_admission_context()
        path = root / f"application-cnpg-fence-{ordinal:02d}-object.json"
        try:
            value = self._read(path)
        except FileNotFoundError:
            return None
        receipt = CNPGFenceObjectReceipt.from_dict(value)
        if (receipt.intent_digest != request.intent_digest or receipt.ordinal != ordinal
                or receipt.document_sha256 != digest):
            raise ProtectedApplyJournalError("CNPG fence object identity binding changed")
        self._sync_application_recovery(root, path.name)
        return receipt

    def record_application_credential_recovery(
        self, plan: FinalGatePlan, *, binding: ApplicationCredentialRecoveryBinding
    ) -> None:
        """Bind original sources and live identity without persisting passwords."""
        self.require_application_credential_context(plan)
        root, intent = self._application_admission_context()
        assert plan.checkpoint_component_sha256 is not None
        if (
            type(binding) is not ApplicationCredentialRecoveryBinding
            or binding.manifest_sha256 != plan.backup_manifest_sha256
            or binding.component_sha256 != plan.checkpoint_component_sha256["k8s_secrets"]
        ):
            raise ProtectedApplyJournalError("application credential backup binding changed")
        record = {
            "schema_version": 1,
            "intent_digest": intent.intent_digest,
            "binding": asdict(binding),
        }
        path = root / "application-credentials.json"
        self._publish_or_match(path, record)
        observed = self._read(path)
        if type(observed.get("schema_version")) is not int or observed != record:
            raise ProtectedApplyJournalError("application credential recovery readback changed")
        self._sync_application_recovery(root, path.name)

    def read_application_manager_replacement(
        self,
    ) -> tuple[CNPGManagerReplacementIntent, bool, CNPGManagerReplacementReceipt | None] | None:
        root, component = self._application_admission_context()
        admission = self.read_application_admission_recovery()
        return self._read_application_manager_replacement(root, component, admission, durable=True)

    def _read_application_manager_replacement(
        self, root: Path, component: ComponentIntent,
        admission: ApplicationAdmissionRecoveryRecord | None, *, durable: bool,
    ) -> tuple[CNPGManagerReplacementIntent, bool, CNPGManagerReplacementReceipt | None] | None:
        names = (
            "application-manager-intent.json", "application-manager-dispatch.json",
            "application-manager-receipt.json",
        )
        if not (root / names[0]).exists():
            if any((root / name).exists() for name in names[1:]):
                raise ProtectedApplyJournalError("CNPG manager record lacks its original intent")
            return None
        intent = CNPGManagerReplacementIntent.from_dict(self._read(root / names[0]))
        if (admission is None or admission.coordination_guard is None
                or intent.component_intent_digest != component.intent_digest
                or intent.admission_digest != admission_record_digest(admission.to_dict())):
            raise ProtectedApplyJournalError("CNPG manager replacement requires original admission guard")
        if durable:
            self._sync_application_recovery(root, names[0])
        dispatched = (root / names[1]).exists()
        if dispatched:
            marker = self._read(root / names[1])
            if (type(marker.get("schema_version")) is not int
                    or marker != {"schema_version": 1, "intent_digest": intent.digest}):
                raise ProtectedApplyJournalError("CNPG manager dispatch binding changed")
            if durable:
                self._sync_application_recovery(root, names[1])
        receipt = None
        if (root / names[2]).exists():
            if not dispatched:
                raise ProtectedApplyJournalError("CNPG manager receipt precedes dispatch")
            receipt = CNPGManagerReplacementReceipt.from_dict(
                self._read(root / names[2]), intent=intent,
            )
            if durable:
                self._sync_application_recovery(root, names[2])
        return intent, dispatched, receipt

    def prepare_application_manager_replacement(
        self, *, identity: CNPGManagerIdentity,
    ) -> CNPGManagerReplacementIntent:
        root, component = self._application_admission_context()
        admission = self.read_application_admission_recovery()
        if admission is None or admission.coordination_guard is None:
            raise ProtectedApplyJournalError("CNPG manager replacement requires original admission guard")
        intent = CNPGManagerReplacementIntent(
            component.intent_digest, admission_record_digest(admission.to_dict()), identity,
        )
        self._publish_or_match(root / "application-manager-intent.json", intent.to_dict())
        record = self.read_application_manager_replacement()
        if record is None or record[0] != intent:
            raise ProtectedApplyJournalError("CNPG manager replacement intent readback changed")
        return intent

    def begin_application_manager_replacement(self) -> bool:
        """Authorize one local dispatch only after durable intent and issuance.

        False means already issued, even if the preceding caller never reached
        PUT. Recovery observes the exact transition; it must never resend.
        """
        root, _component = self._application_admission_context()
        record = self.read_application_manager_replacement()
        if record is None:
            raise ProtectedApplyJournalError("CNPG manager dispatch requires original intent")
        intent, dispatched, _receipt = record
        if dispatched:
            return False
        self._publish_or_match(
            root / "application-manager-dispatch.json",
            {"schema_version": 1, "intent_digest": intent.digest},
        )
        after = self.read_application_manager_replacement()
        if after is None or after[:2] != (intent, True):
            raise ProtectedApplyJournalError("CNPG manager dispatch readback changed")
        return True

    def record_application_manager_replacement(
        self, *, identity: CNPGManagerIdentity,
    ) -> CNPGManagerReplacementReceipt:
        root, _component = self._application_admission_context()
        record = self.read_application_manager_replacement()
        if record is None or not record[1]:
            raise ProtectedApplyJournalError("CNPG manager receipt requires durable dispatch")
        intent, _dispatched, _previous = record
        receipt = CNPGManagerReplacementReceipt.validate(intent, identity)
        self._publish_or_match(root / "application-manager-receipt.json", receipt.to_dict())
        after = self.read_application_manager_replacement()
        if after is None or after[2] != receipt:
            raise ProtectedApplyJournalError("CNPG manager receipt readback changed")
        return receipt

    def record_application_admission_recovery(
        self,
        *,
        target: ApplicationDatabaseAdmissionTarget,
        handoff_backend: ApplicationDatabaseHandoffBackend,
        coordination_guard: ApplicationDatabaseCoordinationGuard | None = None,
    ) -> ApplicationAdmissionRecoveryRecord:
        """Durably bind the full target BEFORE closure, without passwords or new authority.

        Caller must independently admit maintenance and credential/workload recovery.
        Publication is immutable and reuses this journal's private file contract.
        """
        root, intent = self._application_admission_context()
        record = ApplicationAdmissionRecoveryRecord(intent.intent_digest, target, handoff_backend, coordination_guard)
        self._publish_or_match(root / "application-admission.json", record.to_dict())
        if self.read_application_admission_recovery() != record:
            raise ProtectedApplyJournalError("application admission recovery readback changed")
        return record

    def recover_pending_application_handoff(
        self, plan: FinalGatePlan, components: Sequence[ProtectedApplyComponent], *, guard: MutationGuardEvidence,
    ) -> ComponentTerminal | None:
        """Resume only the saved handoff, before ordinary database preflight reads.

        The installed caller must first verify the original supervised guard's
        liveness, fresh +1 epoch and enclosing writer/process authority. This
        entrypoint supplies journal ordering only. It never creates a new intent,
        moves the handoff to ordinal zero, or runs other component callbacks.
        Normal preflight and the full component chain must still run afterward.
        """
        from .protected_application_guard_retention import retained_application_guard_for_resume

        if self._active_apply is not None:
            raise ProtectedApplyJournalError("application early recovery cannot nest active apply")
        if plan.request_id != self.request_id or plan.attempt_number != self.attempt_number:
            raise ProtectedApplyJournalError("application early recovery plan changed")

        def require_retention() -> bool:
            original = retained_application_guard_for_resume(
                self.attempt_root.parents[3], request_id=self.request_id, service_uid=self.service_uid,
                recovery_attempt=self.attempt_number, candidate_sha=plan.candidate_sha,
                candidate_tree=plan.candidate_tree, attestation_digest=plan.attestation_digest,
                starting_mutation_epoch=plan.starting_mutation_epoch,
            )
            if original is None:
                return False
            if original != guard:
                raise ProtectedApplyJournalError("application early recovery original guard changed")
            return True

        if not require_retention():
            return None
        if (not components or len(components) > 32
                or len({component.component_id for component in components}) != len(components)):
            raise ProtectedApplyJournalError("application early recovery chain is invalid")
        selected = [(ordinal, component) for ordinal, component in enumerate(components)
                    if component.component_id == "application-ownership-handoff"]
        epochs = [ordinal for ordinal, component in enumerate(components)
                  if component.component_id == "mutation-epoch-claim"]
        if (len(selected) != 1 or len(epochs) != 1 or epochs[0] >= selected[0][0]
                or selected[0][1].preapply_group is not None
                or selected[0][1].terminal_recovery_authority is not None):
            raise ProtectedApplyJournalError("application early recovery original ordering changed")
        ordinal, component = selected[0]
        # Deliberately no _ensure, O_CREAT, directory creation or new lock name.
        lock_fd = os.open(self.lock_path, os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW)
        try:
            _require_regular(lock_fd, uid=self.service_uid)
            fcntl.flock(lock_fd, fcntl.LOCK_EX)
            if not require_retention():
                return None
            self._validate_chain_layout(components)
            for index, item in enumerate(components):
                root = self.root / f"{index:02d}-{item.component_id}"
                try:
                    _require_directory(root, uid=self.service_uid)
                except FileNotFoundError:
                    continue
                if ComponentIntent.from_dict(self._read(root / "intent.json")) != ComponentIntent.build(plan, item, index):
                    raise ProtectedApplyJournalError("application early recovery chain intent changed")
            if self.read_application_recovery_view(plan, component, ordinal=ordinal) is None:
                raise ProtectedApplyJournalError("application early recovery original intent disappeared")

            def classify(bound: FinalGatePlan) -> ComponentObservation:
                observed = component.classify(bound)
                if observed.observed_epoch != plan.starting_mutation_epoch + 1:
                    raise ProtectedApplyJournalError("application early recovery observed epoch changed")
                return observed

            return self._execute_one(plan, replace(component, classify=classify), ordinal)
        finally:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)

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
                return self._recover_terminal_authority_forward(
                    component_root=component_root,
                    component=component,
                    ordinal=ordinal,
                    plan=plan,
                    intent=intent,
                    prior_terminal=terminal,
                    observed=observed,
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

    def _recover_terminal_authority_forward(
        self,
        *,
        component_root: Path,
        component: ProtectedApplyComponent,
        ordinal: int,
        plan: FinalGatePlan,
        intent: ComponentIntent,
        prior_terminal: ComponentTerminal,
        observed: ComponentObservation,
    ) -> ComponentTerminal:
        authority_resolver = component.terminal_recovery_authority
        recovery_intent_path = component_root / "terminal-recovery-intent.json"
        recovery_path = component_root / "terminal-recovery.json"
        authority = (
            None
            if authority_resolver is None
            else authority_resolver(plan, prior_terminal, observed)
        )
        if (
            authority is None
            or not isinstance(authority, ComponentTerminalRecoveryAuthority)
            or authority.component_id != component.component_id
            or prior_terminal.observed_epoch != plan.starting_mutation_epoch + 1
        ):
            raise ProtectedApplyJournalError(
                f"protected component {component.component_id} terminal state drifted"
            )
        recovery_intent = ComponentTerminalRecoveryIntent.build(
            plan=plan,
            intent=intent,
            terminal=prior_terminal,
            authority=authority,
        )
        try:
            self._read(recovery_intent_path)
        except FileNotFoundError:
            recovery_intent_preexisted = False
        else:
            recovery_intent_preexisted = True

        if not recovery_intent_preexisted:
            try:
                self._read(recovery_path)
            except FileNotFoundError:
                pass
            else:
                raise ProtectedApplyJournalError(
                    "protected component terminal recovery intent is missing"
                )
        if not recovery_intent_preexisted and observed.state is not ComponentState.READY:
            raise ProtectedApplyJournalError(
                f"protected component {component.component_id} terminal state drifted"
            )
        self._publish_or_match(recovery_intent_path, recovery_intent.to_dict())

        try:
            recovery = ComponentTerminalRecovery.from_dict(self._read(recovery_path))
        except FileNotFoundError:
            recovery = None
        except ValueError as exc:
            raise ProtectedApplyJournalError(
                "protected component terminal recovery is invalid"
            ) from exc
        if recovery is not None:
            if (
                not recovery_intent_preexisted
                or recovery.recovery_intent_digest != recovery_intent.recovery_intent_digest
                or recovery.component_id != component.component_id
            ):
                raise ProtectedApplyJournalError(
                    "protected component terminal recovery identity drifted"
                )
            try:
                terminal = recovery.effective_terminal(intent)
            except ValueError as exc:
                raise ProtectedApplyJournalError(
                    "protected component terminal recovery is invalid"
                ) from exc
            if (
                observed.state is not ComponentState.EXACT
                or observed.evidence_digest != terminal.evidence_digest
                or observed.observed_epoch != terminal.observed_epoch
            ):
                raise ProtectedApplyJournalError(
                    f"protected component {component.component_id} recovered terminal state drifted"
                )
            return terminal

        applied = False
        if observed.state is ComponentState.READY:
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
        elif recovery_intent_preexisted and observed.state is ComponentState.EXACT:
            after = observed
        else:
            raise ProtectedApplyJournalError(
                f"protected component {component.component_id} terminal recovery state drifted"
            )
        if after.state is not ComponentState.EXACT:
            diagnostic = f"component classified {after.state.value} after terminal recovery"
            self._publish_failure_diagnostic(
                component_root,
                component,
                ordinal,
                failure_code="did-not-converge",
                diagnostic=diagnostic,
            )
            raise ProtectedApplyJournalError(
                f"protected component {component.component_id} terminal recovery did not converge"
            )
        terminal = ComponentTerminal.build(intent, after, applied=applied)
        recovery = ComponentTerminalRecovery.build(recovery_intent, terminal)
        self._publish_or_match(recovery_path, recovery.to_dict())
        return terminal

    def _apply_with_diagnostic(
        self,
        component_root: Path,
        component: ProtectedApplyComponent,
        ordinal: int,
        plan: FinalGatePlan,
    ) -> None:
        if self._active_apply is not None:
            raise ProtectedApplyJournalError("protected component apply is already active")
        self._active_apply = (component_root, ComponentIntent.build(plan, component, ordinal))
        self._active_apply_owner = (os.getpid(), threading.get_ident())
        try:
            try:
                component.apply(plan)
            finally:
                self._active_apply = None
                self._active_apply_owner = None
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
            observation = component.classify(plan)
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
        if observation.state is ComponentState.DRIFTED:
            diagnostic = _CLASSIFICATION_DRIFT_DIAGNOSTICS[failure_code]
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
        return observation

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
        only; never the message — the #1077 lesson) beside the intent. The
        legacy write-once record remains intact while a bounded append-only
        stream makes later retry failures observable.

        Strictly best-effort: it must never mask the real failure. Any error
        writing either form is swallowed so the original exception still
        propagates unchanged.
        """
        diagnostic_record = ComponentFailureDiagnostic(
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
        )
        try:
            self._publish_or_match(
                component_root / "failure-diagnostic.json",
                diagnostic_record.to_dict(),
            )
        except Exception:
            pass
        try:
            self._append_failure_diagnostic(component_root, diagnostic_record)
        except Exception:
            pass

    def _append_failure_diagnostic(
        self,
        component_root: Path,
        diagnostic: ComponentFailureDiagnostic,
    ) -> None:
        diagnostics_root = component_root / "failure-diagnostics"
        try:
            diagnostics_root.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError:
            pass
        except OSError as exc:
            raise ProtectedApplyJournalError(
                "could not create protected failure diagnostic journal"
            ) from exc
        _require_directory(diagnostics_root, uid=self.service_uid)
        existing = _read_failure_diagnostics(
            component_root,
            service_uid=self.service_uid,
        )
        if len(existing) >= _MAX_FAILURE_DIAGNOSTICS:
            raise ProtectedApplyJournalError("protected failure diagnostic journal is too large")
        sequence = len(existing)
        entry = _ComponentFailureDiagnosticEntry(
            schema_version=1,
            component_id=diagnostic.component_id,
            ordinal=diagnostic.ordinal,
            sequence=sequence,
            failure_code=diagnostic.failure_code,
            diagnostic=diagnostic.diagnostic,
            primary_failure_code=diagnostic.primary_failure_code,
            compensation_failure_code=diagnostic.compensation_failure_code,
        )
        self._publish_failure_diagnostic_entry(
            diagnostics_root / f"{sequence:08d}.json",
            entry.to_dict(),
        )

    def _publish_failure_diagnostic_entry(
        self,
        path: Path,
        value: Mapping[str, object],
    ) -> None:
        payload = _json_bytes(value)
        if (
            len(payload) > _MAX_FAILURE_DIAGNOSTIC_BYTES
            or path.parent.name != "failure-diagnostics"
            or _COMPONENT_DIRECTORY_RE.fullmatch(path.parent.parent.name) is None
        ):
            raise ProtectedApplyJournalError("protected failure diagnostic publication is invalid")
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
                    "protected failure diagnostic cannot be replaced"
                ) from None
        except OSError as exc:
            raise ProtectedApplyJournalError(
                "could not publish protected failure diagnostic"
            ) from exc
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=source_directory_fd)
                except OSError:
                    pass
            os.close(destination_directory_fd)
            os.close(source_directory_fd)

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


def _canonical_nonzero_uuid(value: str) -> bool:
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError):
        return False
    return parsed.int != 0 and str(parsed) == value


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


def _read_failure_diagnostics(
    component_root: Path,
    *,
    service_uid: int,
) -> tuple[_ComponentFailureDiagnosticEntry, ...]:
    match = _COMPONENT_DIRECTORY_RE.fullmatch(component_root.name)
    if (
        not component_root.is_absolute()
        or ".." in component_root.parts
        or match is None
        or service_uid < 0
    ):
        raise ProtectedApplyJournalError("protected failure diagnostic path is invalid")
    _require_directory(component_root, uid=service_uid)
    diagnostics_root = component_root / "failure-diagnostics"
    try:
        _require_directory(diagnostics_root, uid=service_uid)
    except FileNotFoundError:
        return ()
    try:
        entries = tuple(os.scandir(diagnostics_root))
    except OSError as exc:
        raise ProtectedApplyJournalError(
            "protected failure diagnostic journal is unavailable"
        ) from exc
    if len(entries) > _MAX_FAILURE_DIAGNOSTICS:
        raise ProtectedApplyJournalError("protected failure diagnostic journal is too large")
    paths: list[tuple[int, Path]] = []
    for entry in entries:
        entry_match = _FAILURE_DIAGNOSTIC_FILE_RE.fullmatch(entry.name)
        if entry_match is None or entry.is_symlink() or not entry.is_file(follow_symlinks=False):
            raise ProtectedApplyJournalError("protected failure diagnostic journal is unsafe")
        paths.append((int(entry_match.group("sequence")), Path(entry.path)))
    paths.sort()
    if [sequence for sequence, _path in paths] != list(range(len(paths))):
        raise ProtectedApplyJournalError("protected failure diagnostic sequence is invalid")
    expected_component_id = match.group("component_id")
    expected_ordinal = int(match.group("ordinal"))
    diagnostics: list[_ComponentFailureDiagnosticEntry] = []
    for sequence, path in paths:
        try:
            diagnostic = _ComponentFailureDiagnosticEntry.from_dict(
                _read_service_component_record(
                    path,
                    service_uid=service_uid,
                    filename=path.name,
                    max_bytes=_MAX_FAILURE_DIAGNOSTIC_BYTES,
                )
            )
        except ValueError as exc:
            raise ProtectedApplyJournalError(
                "protected failure diagnostic record is invalid"
            ) from exc
        if (
            diagnostic.component_id != expected_component_id
            or diagnostic.ordinal != expected_ordinal
            or diagnostic.sequence != sequence
        ):
            raise ProtectedApplyJournalError("protected failure diagnostic identity drifted")
        diagnostics.append(diagnostic)
    return tuple(diagnostics)


def read_latest_component_failure_diagnostic(
    component_root: Path,
    *,
    service_uid: int,
) -> ComponentFailureDiagnostic | None:
    """Read the newest certified diagnostic, falling back to legacy evidence."""
    diagnostics = _read_failure_diagnostics(component_root, service_uid=service_uid)
    if diagnostics:
        return diagnostics[-1].as_diagnostic()
    match = _COMPONENT_DIRECTORY_RE.fullmatch(component_root.name)
    if match is None:  # guarded by _read_failure_diagnostics
        raise ProtectedApplyJournalError("protected failure diagnostic path is invalid")
    try:
        diagnostic = read_component_failure_diagnostic(
            component_root / "failure-diagnostic.json",
            service_uid=service_uid,
        )
    except FileNotFoundError:
        return None
    if diagnostic.component_id != match.group("component_id") or diagnostic.ordinal != int(
        match.group("ordinal")
    ):
        raise ProtectedApplyJournalError("protected failure diagnostic identity drifted")
    return diagnostic


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
    "ComponentTerminalRecovery",
    "ComponentTerminalRecoveryAuthority",
    "ComponentTerminalRecoveryIntent",
    "ProtectedApplyComponent",
    "ProtectedApplyJournal",
    "ProtectedApplyJournalError",
    "ReconciliationOutcome",
    "ReconciliationOutcomeStatus",
    "read_component_failure",
    "read_component_failure_diagnostic",
    "read_latest_component_failure_diagnostic",
    "read_latest_reconciliation_outcome",
]
