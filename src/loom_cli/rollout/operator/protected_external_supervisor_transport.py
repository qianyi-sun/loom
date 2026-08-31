"""Fixed user-systemd transport for protected external autoscaler convergence."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import stat
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Protocol
from uuid import uuid4

from loom_cli.rollout.external_supervisor_predecessor import (
    ABSENT_PREDECESSOR_DIGEST,
    NO_TRANSITION_GROUP_ID,
    ExternalSupervisorCanonicalIdentity,
    ExternalSupervisorCanonicalPointer,
    ExternalSupervisorPredecessorAuthority,
    ExternalSupervisorPredecessorManifest,
    load_predecessor_manifest,
)
from loom_cli.rollout.external_supervisor_readiness import (
    PROTECTED_EXTERNAL_SUPERVISOR_UNIT_PREFIXES,
    PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE,
    ExternalSupervisorArtifact,
    ExternalSupervisorIdentity,
)

PROTECTED_USER_UNIT_DIR = Path("/var/lib/loom-staging-rollout/.config/systemd/user")
PROTECTED_USER_UNIT_ANCHOR = Path("/var/lib/loom-staging-rollout")

_COMPENSATION_ID_RE = re.compile(r"^[0-9a-f]{32}$")
_COMPENSATION_PREFIX = ".loom-external-supervisor-compensation-"
_COMPENSATION_FILE_RE = re.compile(
    rf"^{re.escape(_COMPENSATION_PREFIX)}"
    r"([0-9a-f]{32})-(intent|activated|canonical|recovered|verified|failed)\.json$"
)
_CANONICAL_POINTER = ".loom-external-supervisor-canonical.json"
_ACTIVATION_PREFIX = ".loom-external-supervisor-activation-"
_ACTIVATION_FILE_RE = re.compile(rf"^{re.escape(_ACTIVATION_PREFIX)}([0-9a-f]{{64}})\.json$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_UNIT_BYTES = 128 * 1024
_MAX_PROTECTED_BYTES = 10 * 1024 * 1024
_MAX_SYSTEMCTL_OUTPUT = 64 * 1024
_SYSTEMCTL_TIMEOUT_SECONDS = 30.0
_RECONCILIATION_SERVICE_TIMEOUT_SECONDS = 7215.0
COMPENSATION_RECONCILIATION_FAILURE_CODES = frozenset(
    {
        "blocker-inventory-failed",
        "journal-inventory-failed",
        "terminal-publication-failed",
        "transition-conflict",
        "transition-operation-failed",
        "transition-reconciliation-failed",
        "transition-validation-failed",
        "transition-verification-failed",
        "unresolved-blockers",
    }
)
EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES = frozenset(
    {
        "activated-terminal-publication-failed",
        "activation-verification-failed",
        "canonical-promotion-failed",
        "canonical-terminal-publication-failed",
        "canonical-verification-failed",
        "daemon-reload-failed",
        "disabled-service-convergence-failed",
        "loaded-definition-verification-failed",
        "preapply-observation-failed",
        "preapply-state-drifted",
        "service-activation-failed",
        "timer-enable-failed",
        "timer-start-failed",
        "transition-publication-failed",
        "unit-byte-verification-failed",
    }
)


class ExternalSupervisorCompensationError(RuntimeError):
    """Secret-safe stage classification for typed compensation recovery."""

    def __init__(self, failure_code: str) -> None:
        if failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES:
            raise ValueError("protected external supervisor compensation failure code is invalid")
        self.failure_code = failure_code
        super().__init__("protected external supervisor compensation reconciliation failed safely")


class ExternalSupervisorApplyError(RuntimeError):
    """Secret-safe apply stage and compensation outcome."""

    def __init__(
        self,
        failure_code: str,
        *,
        compensation_failure_code: str | None,
    ) -> None:
        if failure_code not in EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES or (
            compensation_failure_code is not None
            and compensation_failure_code not in COMPENSATION_RECONCILIATION_FAILURE_CODES
        ):
            raise ValueError("protected external supervisor apply failure code is invalid")
        self.failure_code = failure_code
        self.compensation_failure_code = compensation_failure_code
        if failure_code == "preapply-state-drifted":
            message = "protected external supervisor state changed before apply"
        elif failure_code == "preapply-observation-failed":
            message = "protected external supervisor pre-apply observation failed safely"
        elif compensation_failure_code is None:
            message = "protected external supervisor transition was safely compensated"
        else:
            message = "protected external supervisor transition compensation failed safely"
        super().__init__(message)


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _unit_name(value: str, suffix: str | None = None) -> str:
    if (
        not isinstance(value, str)
        or PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE.fullmatch(value) is None
        or (suffix is not None and not value.endswith(suffix))
    ):
        raise ValueError("protected external supervisor unit name is invalid")
    return value


def _canonical_record_text(value: str, *, label: str) -> ExternalSupervisorCanonicalIdentity:
    if type(value) is not str or not value.endswith("\n") or len(value.encode()) > 512 * 1024:
        raise ValueError(f"protected external supervisor {label} is invalid")
    try:
        return ExternalSupervisorCanonicalIdentity.from_bytes(value.encode())
    except ValueError as exc:
        raise ValueError(f"protected external supervisor {label} is invalid") from exc


def _strict_json_object(payload: bytes, *, label: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"{label} contains duplicate keys")
            result[key] = value
        return result

    try:
        value = json.loads(payload.decode("utf-8"), object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{label} is invalid") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} is invalid")
    return value


@dataclass(frozen=True, slots=True)
class TimerRuntimeStatus:
    load_state: str
    unit_file_state: str
    active_state: str
    fragment_path: str
    need_daemon_reload: str

    def __post_init__(self) -> None:
        if (
            type(self.load_state) is not str
            or self.load_state not in {"loaded", "not-found"}
            or type(self.unit_file_state) is not str
            or self.unit_file_state not in {"", "disabled", "enabled", "not-found"}
            or type(self.active_state) is not str
            or self.active_state not in {"active", "inactive"}
            or type(self.fragment_path) is not str
            or len(self.fragment_path) > 512
            or any(ord(character) < 32 or ord(character) == 127 for character in self.fragment_path)
            or type(self.need_daemon_reload) is not str
            or self.need_daemon_reload not in {"no", "yes"}
        ):
            raise ValueError("protected external supervisor timer status is invalid")

    def to_dict(self) -> dict[str, str]:
        return {
            "active_state": self.active_state,
            "load_state": self.load_state,
            "unit_file_state": self.unit_file_state,
            "fragment_path": self.fragment_path,
            "need_daemon_reload": self.need_daemon_reload,
        }


@dataclass(frozen=True, slots=True)
class ServiceRuntimeStatus:
    load_state: str
    result: str
    exec_main_status: int | None
    fragment_path: str
    need_daemon_reload: str

    def __post_init__(self) -> None:
        if (
            type(self.load_state) is not str
            or self.load_state not in {"loaded", "not-found"}
            or type(self.result) is not str
            or len(self.load_state) > 64
            or len(self.result) > 64
            or any(
                ord(character) < 32 or ord(character) == 127
                for value in (self.load_state, self.result)
                for character in value
            )
            or (
                self.exec_main_status is not None
                and (
                    type(self.exec_main_status) is not int or not 0 <= self.exec_main_status <= 255
                )
            )
            or type(self.fragment_path) is not str
            or len(self.fragment_path) > 512
            or any(ord(character) < 32 or ord(character) == 127 for character in self.fragment_path)
            or type(self.need_daemon_reload) is not str
            or self.need_daemon_reload not in {"no", "yes"}
        ):
            raise ValueError("protected external supervisor service status is invalid")

    def to_dict(self) -> dict[str, object]:
        return {
            "exec_main_status": self.exec_main_status,
            "load_state": self.load_state,
            "result": self.result,
            "fragment_path": self.fragment_path,
            "need_daemon_reload": self.need_daemon_reload,
        }


@dataclass(frozen=True, slots=True)
class TimerCompensationEvidence:
    """One immutable append-only timer compensation journal record."""

    schema_version: int
    compensation_id: str
    artifact_digest: str
    service_name: str
    timer_name: str
    service_unit_sha256: str
    timer_unit_sha256: str
    predecessor_kind: str
    predecessor_authority_digest: str
    predecessor_pointer_digest: str
    predecessor_canonical_json: str
    target_canonical_json: str
    transition_digest: str
    transition_group_id: str
    phase: str
    reason: str
    evidence_digest: str

    def __post_init__(self) -> None:
        valid_reason = {
            "intent": {"supervisor-mutation", "supervisor-repair"},
            "activated": {"timer-active", "timer-disabled"},
            "canonical": {"canonical-promoted"},
            "recovered": {"target-reactivated"},
            "verified": {"inactive-disabled", "predecessor-reactivated"},
            "failed": {"identity-drift", "operation-failed", "verification-failed"},
        }
        target = _canonical_record_text(
            self.target_canonical_json,
            label="target canonical snapshot",
        )
        predecessor: ExternalSupervisorCanonicalIdentity | None = None
        if self.predecessor_canonical_json:
            predecessor = _canonical_record_text(
                self.predecessor_canonical_json,
                label="predecessor canonical snapshot",
            )
        if (
            self.schema_version != 3
            or any(
                type(value) is not str
                for value in (
                    self.compensation_id,
                    self.artifact_digest,
                    self.service_name,
                    self.timer_name,
                    self.service_unit_sha256,
                    self.timer_unit_sha256,
                    self.predecessor_kind,
                    self.predecessor_authority_digest,
                    self.predecessor_pointer_digest,
                    self.predecessor_canonical_json,
                    self.target_canonical_json,
                    self.transition_digest,
                    self.transition_group_id,
                    self.phase,
                    self.reason,
                    self.evidence_digest,
                )
            )
            or _COMPENSATION_ID_RE.fullmatch(self.compensation_id) is None
            or _COMPENSATION_ID_RE.fullmatch(self.transition_group_id) is None
            or any(
                _SHA256_RE.fullmatch(value) is None
                for value in (
                    self.artifact_digest,
                    self.service_unit_sha256,
                    self.timer_unit_sha256,
                    self.transition_digest,
                    self.evidence_digest,
                )
            )
            or _unit_name(self.service_name, ".service") != self.service_name
            or _unit_name(self.timer_name, ".timer") != self.timer_name
            or self.predecessor_kind not in {"absent", "legacy-manifest", "canonical"}
            or _SHA256_RE.fullmatch(self.predecessor_authority_digest) is None
            or _SHA256_RE.fullmatch(self.predecessor_pointer_digest) is None
            or target.record_kind != "activation"
            or target.transition_group_id != self.transition_group_id
            or target.artifact_digest != self.artifact_digest
            or target.unit_sha256.get(self.service_name) != self.service_unit_sha256
            or target.unit_sha256.get(self.timer_name) != self.timer_unit_sha256
            or (
                self.predecessor_kind == "absent"
                and (
                    predecessor is not None
                    or self.predecessor_authority_digest != ABSENT_PREDECESSOR_DIGEST
                    or self.predecessor_pointer_digest != ABSENT_PREDECESSOR_DIGEST
                )
            )
            or (
                self.predecessor_kind == "legacy-manifest"
                and (
                    predecessor is None
                    or predecessor.record_kind != "legacy-snapshot"
                    or self.predecessor_authority_digest != predecessor.artifact_digest
                    or self.predecessor_pointer_digest != ABSENT_PREDECESSOR_DIGEST
                )
            )
            or (
                self.predecessor_kind == "canonical"
                and (
                    predecessor is None
                    or predecessor.record_kind != "activation"
                    or self.predecessor_authority_digest != predecessor.evidence_digest
                    or self.predecessor_pointer_digest
                    != ExternalSupervisorCanonicalPointer.build(predecessor).pointer_digest
                )
            )
            or (
                predecessor is not None
                and (
                    not set(predecessor.unit_payloads).issubset(target.unit_payloads)
                    or (
                        (self.service_name in predecessor.unit_payloads)
                        != (self.timer_name in predecessor.unit_payloads)
                    )
                )
            )
            or (
                self.phase == "intent"
                and self.reason == "supervisor-repair"
                and (
                    self.predecessor_kind != "canonical"
                    or predecessor is None
                    or predecessor.record_kind != "activation"
                    or (predecessor.candidate_sha, predecessor.candidate_tree)
                    == (target.candidate_sha, target.candidate_tree)
                    or self.service_name not in predecessor.unit_payloads
                    or self.timer_name not in predecessor.unit_payloads
                    or not _identity_pair_desired_active(
                        predecessor,
                        self.service_name,
                        self.timer_name,
                    )
                )
            )
            or self.phase not in valid_reason
            or self.reason not in valid_reason[self.phase]
            or _hash_json(self.payload()) != self.evidence_digest
        ):
            raise ValueError("protected external supervisor compensation evidence is invalid")

    @classmethod
    def build(
        cls,
        *,
        compensation_id: str,
        artifact_digest: str,
        service_name: str,
        timer_name: str,
        service_unit_sha256: str,
        timer_unit_sha256: str,
        predecessor_kind: str,
        predecessor_authority_digest: str,
        predecessor_pointer_digest: str,
        predecessor_canonical_json: str,
        target_canonical_json: str,
        transition_digest: str,
        transition_group_id: str,
        phase: str,
        reason: str,
    ) -> TimerCompensationEvidence:
        payload = {
            "schema_version": 3,
            "compensation_id": compensation_id,
            "artifact_digest": artifact_digest,
            "service_name": service_name,
            "timer_name": timer_name,
            "service_unit_sha256": service_unit_sha256,
            "timer_unit_sha256": timer_unit_sha256,
            "predecessor_kind": predecessor_kind,
            "predecessor_authority_digest": predecessor_authority_digest,
            "predecessor_pointer_digest": predecessor_pointer_digest,
            "predecessor_canonical_json": predecessor_canonical_json,
            "target_canonical_json": target_canonical_json,
            "transition_digest": transition_digest,
            "transition_group_id": transition_group_id,
            "phase": phase,
            "reason": reason,
        }
        return cls(
            schema_version=3,
            compensation_id=compensation_id,
            artifact_digest=artifact_digest,
            service_name=service_name,
            timer_name=timer_name,
            service_unit_sha256=service_unit_sha256,
            timer_unit_sha256=timer_unit_sha256,
            predecessor_kind=predecessor_kind,
            predecessor_authority_digest=predecessor_authority_digest,
            predecessor_pointer_digest=predecessor_pointer_digest,
            predecessor_canonical_json=predecessor_canonical_json,
            target_canonical_json=target_canonical_json,
            transition_digest=transition_digest,
            transition_group_id=transition_group_id,
            phase=phase,
            reason=reason,
            evidence_digest=_hash_json(payload),
        )

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "compensation_id": self.compensation_id,
            "artifact_digest": self.artifact_digest,
            "service_name": self.service_name,
            "timer_name": self.timer_name,
            "service_unit_sha256": self.service_unit_sha256,
            "timer_unit_sha256": self.timer_unit_sha256,
            "predecessor_kind": self.predecessor_kind,
            "predecessor_authority_digest": self.predecessor_authority_digest,
            "predecessor_pointer_digest": self.predecessor_pointer_digest,
            "predecessor_canonical_json": self.predecessor_canonical_json,
            "target_canonical_json": self.target_canonical_json,
            "transition_digest": self.transition_digest,
            "transition_group_id": self.transition_group_id,
            "phase": self.phase,
            "reason": self.reason,
        }

    def to_bytes(self) -> bytes:
        return (
            json.dumps(
                {**self.payload(), "evidence_digest": self.evidence_digest},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> TimerCompensationEvidence:
        if not 1 <= len(payload) <= _MAX_PROTECTED_BYTES:
            raise ValueError("protected external supervisor compensation record bytes are invalid")
        raw = _strict_json_object(
            payload,
            label="protected external supervisor compensation record",
        )
        expected = set(cls.__dataclass_fields__)
        if (
            set(raw) != expected
            or any(not isinstance(raw[name], str) for name in expected - {"schema_version"})
            or type(raw["schema_version"]) is not int
        ):
            raise ValueError("protected external supervisor compensation fields are invalid")
        record = cls(**raw)  # type: ignore[arg-type]
        if payload != record.to_bytes():
            raise ValueError("protected external supervisor compensation encoding is not canonical")
        return record


def _transition_group_key(record: TimerCompensationEvidence) -> tuple[str, ...]:
    return (
        record.transition_group_id,
        record.transition_digest,
        record.artifact_digest,
        record.predecessor_kind,
        record.predecessor_authority_digest,
        record.predecessor_pointer_digest,
        record.predecessor_canonical_json,
        record.target_canonical_json,
    )


def _active_transition_group_intents(
    active: ExternalSupervisorCanonicalIdentity,
    records: Mapping[str, Mapping[str, TimerCompensationEvidence]],
) -> tuple[TimerCompensationEvidence, ...] | None:
    """Prove the active activation's durable group has one intent per unit pair."""

    if active.record_kind != "activation" or active.transition_group_id == NO_TRANSITION_GROUP_ID:
        return None
    intents: list[TimerCompensationEvidence] = []
    for phases in records.values():
        matching = [
            record
            for record in phases.values()
            if record.transition_group_id == active.transition_group_id
        ]
        if not matching:
            continue
        if len(matching) != len(phases) or (intent := phases.get("intent")) is None:
            return None
        intents.append(intent)
    if (
        not intents
        or len({_transition_group_key(intent) for intent in intents}) != 1
        or any(intent.target_canonical_json != active.to_bytes().decode() for intent in intents)
    ):
        return None
    covered_units = {
        name for intent in intents for name in (intent.service_name, intent.timer_name)
    }
    if not (
        len(covered_units) == 2 * len(intents)
        and covered_units == set(active.unit_payloads)
        and all(
            intent.service_name.removesuffix(".service") == intent.timer_name.removesuffix(".timer")
            for intent in intents
        )
    ):
        return None
    return tuple(sorted(intents, key=lambda intent: intent.compensation_id))


def _active_transition_group_complete(
    active: ExternalSupervisorCanonicalIdentity,
    records: Mapping[str, Mapping[str, TimerCompensationEvidence]],
) -> bool:
    return _active_transition_group_intents(active, records) is not None


@dataclass(frozen=True, slots=True)
class ExternalSupervisorLiveObservation:
    """Secret-free exact unit bytes plus bounded user-systemd state."""

    unit_payloads: Mapping[str, bytes | None]
    timer_statuses: Mapping[str, TimerRuntimeStatus]
    service_statuses: Mapping[str, ServiceRuntimeStatus]
    canonical_identity: ExternalSupervisorCanonicalIdentity | None = None
    predecessor_authority: ExternalSupervisorPredecessorAuthority | None = None
    predecessor_manifest: ExternalSupervisorPredecessorManifest | None = field(
        default=None,
        repr=False,
        compare=False,
    )
    compensation_blockers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        units = dict(self.unit_payloads)
        timers = dict(self.timer_statuses)
        services = dict(self.service_statuses)
        blockers = dict(self.compensation_blockers)
        canonical = self.canonical_identity
        authority = self.predecessor_authority
        if (
            not units
            or any(
                PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE.fullmatch(name) is None
                or (payload is not None and not isinstance(payload, bytes))
                or (payload is not None and len(payload) > _MAX_UNIT_BYTES)
                for name, payload in units.items()
            )
            or any(_unit_name(name, ".timer") != name for name in timers)
            or any(_unit_name(name, ".service") != name for name in services)
            or any(
                _COMPENSATION_ID_RE.fullmatch(name) is None or _SHA256_RE.fullmatch(digest) is None
                for name, digest in blockers.items()
            )
        ):
            raise ValueError("protected external supervisor observation is invalid")
        if authority is None:
            if canonical is not None:
                authority = ExternalSupervisorPredecessorAuthority(
                    kind="canonical",
                    authority_digest=canonical.evidence_digest,
                    unit_sha256=canonical.unit_sha256,
                )
            else:
                legacy = self.predecessor_manifest or load_predecessor_manifest()
                live_digests = {
                    name: hashlib.sha256(payload).hexdigest()
                    for name, payload in units.items()
                    if payload is not None
                }
                if live_digests == dict(legacy.unit_sha256):
                    authority = ExternalSupervisorPredecessorAuthority(
                        kind="legacy-manifest",
                        authority_digest=legacy.manifest_digest,
                        unit_sha256=legacy.unit_sha256,
                    )
                elif not live_digests:
                    # First introduction of the external supervisor: no canonical
                    # predecessor was ever recorded and no supervisor units are
                    # live, so there is genuinely no predecessor to validate. An
                    # absent predecessor is a valid bootstrap (nothing to clobber).
                    # A live unit set that merely *differs* from the manifest is
                    # still rejected below as drift.
                    authority = ExternalSupervisorPredecessorAuthority(
                        kind="absent",
                        authority_digest=ABSENT_PREDECESSOR_DIGEST,
                        unit_sha256={},
                    )
                else:
                    raise ValueError(
                        "protected external supervisor predecessor is not authoritative"
                    )
        if canonical is not None and (
            authority.kind != "canonical"
            or authority.authority_digest != canonical.evidence_digest
            or dict(authority.unit_sha256) != dict(canonical.unit_sha256)
        ):
            raise ValueError("protected external supervisor canonical authority drifted")
        if canonical is None and authority.kind == "canonical":
            raise ValueError("protected external supervisor canonical authority is missing")
        object.__setattr__(self, "unit_payloads", MappingProxyType(dict(sorted(units.items()))))
        object.__setattr__(self, "timer_statuses", MappingProxyType(dict(sorted(timers.items()))))
        object.__setattr__(
            self,
            "service_statuses",
            MappingProxyType(dict(sorted(services.items()))),
        )
        object.__setattr__(
            self,
            "compensation_blockers",
            MappingProxyType(dict(sorted(blockers.items()))),
        )
        object.__setattr__(self, "predecessor_authority", authority)

    @property
    def transition_digest(self) -> str:
        return _hash_json(dict(self.compensation_blockers))

    @property
    def pending_transition_digest(self) -> str:
        return self.transition_digest

    @property
    def evidence_digest(self) -> str:
        authority = self.predecessor_authority
        assert authority is not None
        return _hash_json(
            {
                "services": {
                    name: status.to_dict() for name, status in self.service_statuses.items()
                },
                "compensation_blockers": dict(self.compensation_blockers),
                "canonical_digest": (
                    None
                    if self.canonical_identity is None
                    else self.canonical_identity.evidence_digest
                ),
                "predecessor_authority": {
                    "authority_digest": authority.authority_digest,
                    "kind": authority.kind,
                    "unit_sha256": dict(authority.unit_sha256),
                },
                "transition_digest": self.transition_digest,
                "timers": {name: status.to_dict() for name, status in self.timer_statuses.items()},
                "units": {
                    name: None if payload is None else hashlib.sha256(payload).hexdigest()
                    for name, payload in self.unit_payloads.items()
                },
            }
        )


class UserUnitStore(Protocol):
    def list_units(self) -> tuple[str, ...]: ...

    def read_unit(self, unit_name: str) -> bytes | None: ...

    def read_canonical(self) -> ExternalSupervisorCanonicalIdentity | None: ...

    def protected_activation_references(self) -> tuple[str, ...]: ...

    def record_compensation(self, evidence: TimerCompensationEvidence) -> None: ...

    def compensation_blockers(self) -> Mapping[str, str]: ...

    def pending_compensations(self) -> tuple[TimerCompensationEvidence, ...]: ...

    def publish_unit(
        self,
        unit_name: str,
        payload: bytes,
        *,
        expected_current: bytes | None,
    ) -> None: ...

    def publish_transition(
        self,
        intents: Sequence[TimerCompensationEvidence],
        units: Mapping[str, tuple[bytes | None, bytes]],
    ) -> None: ...

    def restore_unit(
        self,
        unit_name: str,
        payload: bytes | None,
        *,
        expected_current: bytes | None,
    ) -> None: ...

    def restore_transition(
        self,
        units: Mapping[str, tuple[bytes | None, bytes | None]],
    ) -> None: ...

    def promote_canonical(
        self,
        identity: ExternalSupervisorCanonicalIdentity,
        *,
        expected_current: ExternalSupervisorCanonicalIdentity | None,
    ) -> None: ...


class UserSystemdControl(Protocol):
    def timer_status(self, timer_name: str) -> TimerRuntimeStatus: ...

    def service_status(self, service_name: str) -> ServiceRuntimeStatus: ...

    def daemon_reload(self) -> None: ...

    def enable_timer(self, timer_name: str) -> None: ...

    def start_timer(self, timer_name: str) -> None: ...

    def stop_timer(self, timer_name: str) -> None: ...

    def disable_timer(self, timer_name: str) -> None: ...

    def stop_service(self, service_name: str) -> None: ...

    def reset_service_failure(self, service_name: str) -> None: ...

    def start_service(self, service_name: str, *, timeout_seconds: float) -> None: ...


class ProtectedExternalSupervisorTransport(Protocol):
    """Artifact-bound transport with no arbitrary command surface."""

    def observe(
        self,
        artifact: ExternalSupervisorArtifact,
        predecessor_authority: ExternalSupervisorPredecessorAuthority | None = None,
    ) -> ExternalSupervisorLiveObservation: ...

    def apply(
        self,
        artifact: ExternalSupervisorArtifact,
        expected: ExternalSupervisorLiveObservation,
        *,
        plan_digest: str,
        attestation_digest: str,
        transition_digest: str,
    ) -> None: ...

    def reconcile_compensations(self) -> None: ...


@dataclass(frozen=True, slots=True)
class AtomicUserUnitStore:
    """Publish exact unit bytes without following links or replacing a race."""

    unit_dir: Path
    service_uid: int
    creation_anchor: Path | None = None

    def __post_init__(self) -> None:
        anchor = self.creation_anchor
        if anchor is None:
            anchor = (
                PROTECTED_USER_UNIT_ANCHOR
                if self.unit_dir == PROTECTED_USER_UNIT_DIR
                else self.unit_dir.parent
            )
            object.__setattr__(self, "creation_anchor", anchor)
        if (
            not self.unit_dir.is_absolute()
            or ".." in self.unit_dir.parts
            or not anchor.is_absolute()
            or ".." in anchor.parts
            or self.unit_dir == anchor
            or not self.unit_dir.is_relative_to(anchor)
            or self.service_uid < 0
        ):
            raise ValueError("protected external supervisor unit store is invalid")

    def read_unit(self, unit_name: str) -> bytes | None:
        name = _unit_name(unit_name)
        directory = self._open_directory(create=False)
        if directory is None:
            return None
        try:
            payload = self._read_at(directory, name)
            if payload is not None and len(payload) > _MAX_UNIT_BYTES:
                raise RuntimeError("protected external supervisor unit payload is oversized")
            return payload
        finally:
            os.close(directory)

    def read_canonical(self) -> ExternalSupervisorCanonicalIdentity | None:
        payload = self._read_protected_payload(_CANONICAL_POINTER)
        activations = self._activation_records()
        if payload is None:
            return None
        try:
            pointer = ExternalSupervisorCanonicalPointer.from_bytes(payload)
            activation = activations.get(pointer.activation_digest)
            if activation is None:
                raise ValueError("canonical activation is missing")
            return activation
        except ValueError as exc:
            raise RuntimeError("protected external supervisor canonical pointer drifted") from exc

    def protected_activation_references(self) -> tuple[str, ...]:
        references: set[str] = set()
        active = self.read_canonical()
        if active is not None:
            references.add(active.evidence_digest)
        for intent in self.pending_compensations():
            target = _canonical_record_text(
                intent.target_canonical_json,
                label="referenced target activation",
            )
            if target.record_kind == "activation":
                references.add(target.evidence_digest)
            if intent.predecessor_canonical_json:
                predecessor = _canonical_record_text(
                    intent.predecessor_canonical_json,
                    label="referenced predecessor activation",
                )
                if predecessor.record_kind == "activation":
                    references.add(predecessor.evidence_digest)
        return tuple(sorted(references))

    def list_units(self) -> tuple[str, ...]:
        directory = self._open_directory(create=False)
        if directory is None:
            return ()
        try:
            managed: list[str] = []
            for entry in os.listdir(directory):
                if entry.startswith("."):
                    continue
                if entry.startswith(PROTECTED_EXTERNAL_SUPERVISOR_UNIT_PREFIXES) and entry.endswith(
                    (".service", ".timer")
                ):
                    if PROTECTED_EXTERNAL_SUPERVISOR_UNIT_RE.fullmatch(entry) is None:
                        raise RuntimeError("protected external supervisor unit inventory drifted")
                    managed.append(entry)
            return tuple(sorted(managed))
        finally:
            os.close(directory)

    def publish_unit(
        self,
        unit_name: str,
        payload: bytes,
        *,
        expected_current: bytes | None,
    ) -> None:
        name = _unit_name(unit_name)
        if (
            not isinstance(payload, bytes)
            or not 1 <= len(payload) <= _MAX_UNIT_BYTES
            or not payload.endswith(b"\n")
            or b"\x00" in payload
        ):
            raise ValueError("protected external supervisor unit payload is invalid")
        self._publish_exact(name, payload, expected_current=expected_current)

    def publish_transition(
        self,
        intents: Sequence[TimerCompensationEvidence],
        units: Mapping[str, tuple[bytes | None, bytes]],
    ) -> None:
        canonical_intents = tuple(
            TimerCompensationEvidence.from_bytes(item.to_bytes()) for item in intents
        )
        publications = dict(units)
        if (
            not canonical_intents
            or any(item.phase != "intent" for item in canonical_intents)
            or len({item.compensation_id for item in canonical_intents}) != len(canonical_intents)
            or {name for item in canonical_intents for name in (item.service_name, item.timer_name)}
            != set(publications)
            or len({item.target_canonical_json for item in canonical_intents}) != 1
            or len({item.predecessor_pointer_digest for item in canonical_intents}) != 1
            or len({item.transition_digest for item in canonical_intents}) != 1
        ):
            raise ValueError("protected external supervisor transition identity is invalid")
        for name, (expected, payload) in publications.items():
            _unit_name(name)
            if (
                expected is not None
                and (type(expected) is not bytes or len(expected) > _MAX_UNIT_BYTES)
            ) or (
                type(payload) is not bytes
                or not 1 <= len(payload) <= _MAX_UNIT_BYTES
                or not payload.endswith(b"\n")
                or b"\x00" in payload
            ):
                raise ValueError("protected external supervisor transition bytes are invalid")

        directory = self._open_directory(create=True)
        assert directory is not None
        lock_descriptor: int | None = None
        try:
            lock_descriptor = self._lock_directory(directory)
            for name, (expected, _payload) in publications.items():
                if self._read_at(directory, name) != expected:
                    raise RuntimeError(
                        "protected external supervisor unit changed before transition"
                    )
            # Every self-contained snapshot is durable before the first unit
            # byte can move.  The same global lock covers every subsequent CAS.
            for evidence in canonical_intents:
                name = f"{_COMPENSATION_PREFIX}{evidence.compensation_id}-intent.json"
                payload = evidence.to_bytes()
                current = self._read_at(directory, name)
                if current is not None and current != payload:
                    raise RuntimeError("protected external supervisor transition intent drifted")
                self._publish_exact_locked(
                    directory,
                    name,
                    payload,
                    expected_current=current,
                )
            for name, (expected, payload) in sorted(publications.items()):
                self._publish_exact_locked(
                    directory,
                    name,
                    payload,
                    expected_current=expected,
                )
        finally:
            if lock_descriptor is not None:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)
            os.close(directory)

    def restore_unit(
        self,
        unit_name: str,
        payload: bytes | None,
        *,
        expected_current: bytes | None,
    ) -> None:
        name = _unit_name(unit_name)
        if payload is not None:
            self.publish_unit(name, payload, expected_current=expected_current)
            return
        directory = self._open_directory(create=True)
        assert directory is not None
        lock_descriptor: int | None = None
        try:
            lock_descriptor = self._lock_directory(directory)
            if self._read_at(directory, name) != expected_current:
                raise RuntimeError("protected external supervisor unit changed before restore")
            if expected_current is not None:
                os.unlink(name, dir_fd=directory)
                os.fsync(directory)
            if self._read_at(directory, name) is not None:
                raise RuntimeError("protected external supervisor unit restore drifted")
        finally:
            if lock_descriptor is not None:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)
            os.close(directory)

    def restore_transition(
        self,
        units: Mapping[str, tuple[bytes | None, bytes | None]],
    ) -> None:
        restorations = dict(units)
        if not restorations:
            raise ValueError("protected external supervisor restore set is empty")
        for name, (expected, payload) in restorations.items():
            _unit_name(name)
            for value in (expected, payload):
                if value is not None and (type(value) is not bytes or len(value) > _MAX_UNIT_BYTES):
                    raise ValueError("protected external supervisor restore bytes are invalid")
        directory = self._open_directory(create=True)
        assert directory is not None
        lock_descriptor: int | None = None
        try:
            lock_descriptor = self._lock_directory(directory)
            for name, (expected, _payload) in restorations.items():
                if self._read_at(directory, name) != expected:
                    raise RuntimeError("protected external supervisor unit changed before restore")
            for name, (expected, payload) in sorted(restorations.items()):
                if payload is None:
                    if expected is not None:
                        os.unlink(name, dir_fd=directory)
                        os.fsync(directory)
                else:
                    self._publish_exact_locked(
                        directory,
                        name,
                        payload,
                        expected_current=expected,
                    )
            if any(
                self._read_at(directory, name) != payload
                for name, (_expected, payload) in restorations.items()
            ):
                raise RuntimeError("protected external supervisor restore drifted")
        finally:
            if lock_descriptor is not None:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)
            os.close(directory)

    def promote_canonical(
        self,
        identity: ExternalSupervisorCanonicalIdentity,
        *,
        expected_current: ExternalSupervisorCanonicalIdentity | None,
    ) -> None:
        canonical = ExternalSupervisorCanonicalIdentity.from_bytes(identity.to_bytes())
        if (
            canonical != identity
            or canonical.record_kind != "activation"
            or canonical.transition_group_id == NO_TRANSITION_GROUP_ID
        ):
            raise ValueError("protected external supervisor canonical identity drifted")
        intents = _active_transition_group_intents(
            canonical,
            self._compensation_records(),
        )
        if intents is None:
            raise RuntimeError("protected external supervisor activation journal is incomplete")
        predecessor = intents[0]
        if expected_current is None:
            predecessor_matches = predecessor.predecessor_kind in {
                "absent",
                "legacy-manifest",
            }
        else:
            predecessor_matches = (
                predecessor.predecessor_kind == "canonical"
                and predecessor.predecessor_authority_digest == expected_current.evidence_digest
                and predecessor.predecessor_pointer_digest
                == ExternalSupervisorCanonicalPointer.build(expected_current).pointer_digest
                and predecessor.predecessor_canonical_json == expected_current.to_bytes().decode()
            )
        if not predecessor_matches:
            raise RuntimeError(
                "protected external supervisor activation predecessor journal drifted"
            )
        activation_name = f"{_ACTIVATION_PREFIX}{canonical.evidence_digest}.json"
        activation_current = self._read_protected_payload(activation_name)
        if activation_current is not None and activation_current != canonical.to_bytes():
            raise RuntimeError("protected external supervisor activation record drifted")
        self._publish_exact(
            activation_name,
            canonical.to_bytes(),
            expected_current=activation_current,
        )
        pointer = ExternalSupervisorCanonicalPointer.build(canonical)
        expected_pointer = (
            None
            if expected_current is None
            else ExternalSupervisorCanonicalPointer.build(expected_current).to_bytes()
        )
        self._publish_exact(
            _CANONICAL_POINTER,
            pointer.to_bytes(),
            expected_current=expected_pointer,
        )

    def record_compensation(self, evidence: TimerCompensationEvidence) -> None:
        canonical = TimerCompensationEvidence.from_bytes(evidence.to_bytes())
        if canonical != evidence:
            raise ValueError("protected external supervisor compensation evidence drifted")
        name = (
            ".loom-external-supervisor-compensation-"
            f"{evidence.compensation_id}-{evidence.phase}.json"
        )
        payload = evidence.to_bytes()
        current = self._read_protected_payload(name)
        if current is not None and current != payload:
            raise RuntimeError("protected external supervisor compensation record already drifted")
        self._publish_exact(name, payload, expected_current=current)

    def compensation_blockers(self) -> Mapping[str, str]:
        records = self._compensation_records()
        blockers: dict[str, str] = {}
        for compensation_id, phases in records.items():
            if not self._compensation_resolved(phases):
                blockers[compensation_id] = _hash_json(
                    {name: record.evidence_digest for name, record in sorted(phases.items())}
                )
        active = self.read_canonical()
        if active is not None and not _active_transition_group_complete(active, records):
            group_records = {
                compensation_id: {
                    phase: record.evidence_digest
                    for phase, record in sorted(phases.items())
                    if record.transition_group_id == active.transition_group_id
                }
                for compensation_id, phases in sorted(records.items())
                if any(
                    record.transition_group_id == active.transition_group_id
                    for record in phases.values()
                )
            }
            blockers[active.transition_group_id] = _hash_json(
                {
                    "activation_digest": active.evidence_digest,
                    "group_records": group_records,
                    "reason": "active-transition-group-incomplete",
                    "transition_group_id": active.transition_group_id,
                }
            )
        return MappingProxyType(dict(sorted(blockers.items())))

    def pending_compensations(self) -> tuple[TimerCompensationEvidence, ...]:
        records = self._compensation_records()
        intents: list[TimerCompensationEvidence] = []
        open_groups: set[tuple[str, ...]] = set()
        for phases in records.values():
            intent = phases.get("intent")
            if intent is not None:
                intents.append(intent)
                if not self._compensation_resolved(phases):
                    open_groups.add(_transition_group_key(intent))
        # A crash can land between sibling terminal writes.  Include every
        # intent in an open group so recovery always converges the complete
        # unit set, including siblings whose terminal reached disk first.
        return tuple(
            sorted(
                (intent for intent in intents if _transition_group_key(intent) in open_groups),
                key=lambda record: record.compensation_id,
            )
        )

    def _compensation_records(
        self,
    ) -> Mapping[str, Mapping[str, TimerCompensationEvidence]]:
        directory = self._open_directory(create=False)
        if directory is None:
            return MappingProxyType({})
        records: dict[str, dict[str, TimerCompensationEvidence]] = {}
        try:
            for entry in os.listdir(directory):
                matched = _COMPENSATION_FILE_RE.fullmatch(entry)
                if matched is None:
                    # Temporary publications begin with two dots and the lock
                    # has a different fixed name.  Anything claiming the
                    # durable journal prefix must parse exactly or it is
                    # evidence drift, never an ignorable side file.
                    if entry.startswith(_COMPENSATION_PREFIX):
                        raise RuntimeError(
                            "protected external supervisor compensation filename is invalid"
                        )
                    continue
                payload = self._read_at(directory, entry)
                if payload is None:
                    raise RuntimeError(
                        "protected external supervisor compensation record disappeared"
                    )
                record = TimerCompensationEvidence.from_bytes(payload)
                if record.compensation_id != matched.group(1) or record.phase != matched.group(2):
                    raise RuntimeError(
                        "protected external supervisor compensation filename drifted"
                    )
                records.setdefault(record.compensation_id, {})[record.phase] = record
        finally:
            os.close(directory)
        return MappingProxyType(
            {
                compensation_id: MappingProxyType(dict(sorted(phases.items())))
                for compensation_id, phases in sorted(records.items())
            }
        )

    def _activation_records(self) -> Mapping[str, ExternalSupervisorCanonicalIdentity]:
        directory = self._open_directory(create=False)
        if directory is None:
            return MappingProxyType({})
        records: dict[str, ExternalSupervisorCanonicalIdentity] = {}
        try:
            for entry in os.listdir(directory):
                matched = _ACTIVATION_FILE_RE.fullmatch(entry)
                if matched is None:
                    if entry.startswith(_ACTIVATION_PREFIX):
                        raise RuntimeError(
                            "protected external supervisor activation filename is invalid"
                        )
                    continue
                payload = self._read_at(directory, entry)
                if payload is None:
                    raise RuntimeError(
                        "protected external supervisor activation record disappeared"
                    )
                record = ExternalSupervisorCanonicalIdentity.from_bytes(payload)
                digest = matched.group(1)
                if record.record_kind != "activation" or record.evidence_digest != digest:
                    raise RuntimeError("protected external supervisor activation record drifted")
                records[digest] = record
        finally:
            os.close(directory)
        return MappingProxyType(dict(sorted(records.items())))

    @staticmethod
    def _compensation_resolved(
        phases: Mapping[str, TimerCompensationEvidence],
    ) -> bool:
        intent = phases.get("intent")
        if intent is None:
            return False
        identity = (
            intent.artifact_digest,
            intent.service_name,
            intent.timer_name,
            intent.service_unit_sha256,
            intent.timer_unit_sha256,
            intent.predecessor_kind,
            intent.predecessor_authority_digest,
            intent.predecessor_pointer_digest,
            intent.predecessor_canonical_json,
            intent.target_canonical_json,
            intent.transition_digest,
            intent.transition_group_id,
        )
        terminal_records = {phase: record for phase, record in phases.items() if phase != "intent"}
        if any(
            identity
            != (
                record.artifact_digest,
                record.service_name,
                record.timer_name,
                record.service_unit_sha256,
                record.timer_unit_sha256,
                record.predecessor_kind,
                record.predecessor_authority_digest,
                record.predecessor_pointer_digest,
                record.predecessor_canonical_json,
                record.target_canonical_json,
                record.transition_digest,
                record.transition_group_id,
            )
            for record in terminal_records.values()
        ):
            return False
        # Activation is deliberately non-terminal.  A canonical terminal from
        # the initial apply closes only a failure-free transition.  A later
        # immutable recovered/verified terminal proves full reconciliation and
        # therefore supersedes an earlier immutable failure without deleting it.
        return (
            "recovered" in terminal_records
            or "verified" in terminal_records
            or ("canonical" in terminal_records and "failed" not in phases)
        )

    def _read_protected_payload(self, name: str) -> bytes | None:
        directory = self._open_directory(create=False)
        if directory is None:
            return None
        try:
            return self._read_at(directory, name)
        finally:
            os.close(directory)

    def _publish_exact(
        self,
        name: str,
        payload: bytes,
        *,
        expected_current: bytes | None,
    ) -> None:
        if (
            not name
            or "/" in name
            or "\\" in name
            or len(name) > 220
            or not isinstance(payload, bytes)
            or not 1 <= len(payload) <= _MAX_PROTECTED_BYTES
        ):
            raise ValueError("protected external supervisor protected payload is invalid")
        directory = self._open_directory(create=True)
        assert directory is not None
        lock_descriptor: int | None = None
        try:
            lock_descriptor = self._lock_directory(directory)
            self._publish_exact_locked(
                directory,
                name,
                payload,
                expected_current=expected_current,
            )
        finally:
            if lock_descriptor is not None:
                fcntl.flock(lock_descriptor, fcntl.LOCK_UN)
                os.close(lock_descriptor)
            os.close(directory)

    def _publish_exact_locked(
        self,
        directory: int,
        name: str,
        payload: bytes,
        *,
        expected_current: bytes | None,
    ) -> None:
        temporary = f".{name}.loom-{uuid4().hex}.tmp"
        descriptor: int | None = None
        try:
            current = self._read_at(directory, name)
            if current != expected_current:
                raise RuntimeError("protected external supervisor unit changed before publish")
            if current == payload:
                return
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            descriptor = os.open(temporary, flags, 0o600, dir_fd=directory)
            written = 0
            while written < len(payload):
                amount = os.write(descriptor, payload[written:])
                if amount <= 0:
                    raise OSError("protected external supervisor unit write made no progress")
                written += amount
            os.fsync(descriptor)
            os.fchmod(descriptor, 0o600)
            os.close(descriptor)
            descriptor = None
            if self._read_at(directory, name) != expected_current:
                raise RuntimeError("protected external supervisor unit changed during publish")
            if expected_current is None:
                os.link(
                    temporary,
                    name,
                    src_dir_fd=directory,
                    dst_dir_fd=directory,
                    follow_symlinks=False,
                )
                os.unlink(temporary, dir_fd=directory)
            else:
                os.replace(temporary, name, src_dir_fd=directory, dst_dir_fd=directory)
            os.fsync(directory)
            if self._read_at(directory, name) != payload:
                raise RuntimeError("protected external supervisor unit publication drifted")
        finally:
            if descriptor is not None:
                os.close(descriptor)
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass

    def _open_directory(self, *, create: bool) -> int | None:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            descriptor = os.open(self.unit_dir, flags)
        except FileNotFoundError:
            if not create:
                return None
            descriptor = self._create_directory_chain(flags)
        self._validate_directory(os.fstat(descriptor), "unit directory")
        return descriptor

    def _create_directory_chain(self, flags: int) -> int:
        anchor = self.creation_anchor
        assert anchor is not None
        descriptor = os.open(anchor, flags)
        self._validate_directory(os.fstat(descriptor), "creation anchor")
        try:
            relative = self.unit_dir.relative_to(anchor)
            for part in relative.parts:
                try:
                    child = os.open(part, flags, dir_fd=descriptor)
                except FileNotFoundError:
                    try:
                        os.mkdir(part, 0o700, dir_fd=descriptor)
                    except FileExistsError:
                        pass
                    os.fsync(descriptor)
                    child = os.open(part, flags, dir_fd=descriptor)
                try:
                    self._validate_directory(os.fstat(child), "created unit directory")
                except BaseException:
                    os.close(child)
                    raise
                os.close(descriptor)
                descriptor = child
            return descriptor
        except BaseException:
            os.close(descriptor)
            raise

    def _lock_directory(self, directory: int) -> int:
        name = ".loom-external-supervisor.lock"
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(name, flags, 0o600, dir_fd=directory)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            os.close(descriptor)
            raise RuntimeError("protected external supervisor unit lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX)
        except BaseException:
            os.close(descriptor)
            raise
        os.fsync(directory)
        return descriptor

    def _validate_directory(self, metadata: os.stat_result, label: str) -> None:
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise RuntimeError(f"protected external supervisor {label} is unsafe")

    def _read_at(self, directory: int, unit_name: str) -> bytes | None:
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        try:
            descriptor = os.open(unit_name, flags, dir_fd=directory)
        except FileNotFoundError:
            return None
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid != self.service_uid
                or before.st_nlink != 1
                or stat.S_IMODE(before.st_mode) & 0o022
                or not 0 <= before.st_size <= _MAX_PROTECTED_BYTES
            ):
                raise RuntimeError("protected external supervisor unit metadata is unsafe")
            chunks: list[bytes] = []
            remaining = _MAX_PROTECTED_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            after = os.fstat(descriptor)
            if (
                len(payload) > _MAX_PROTECTED_BYTES
                or before.st_size != len(payload)
                or (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
                != (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
            ):
                raise RuntimeError("protected external supervisor unit read is unstable")
            return payload
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class FixedUserSystemdControl:
    """Execute a closed set of fixed ``systemctl --user`` operations."""

    service_uid: int
    service_home: Path = PROTECTED_USER_UNIT_ANCHOR

    def __post_init__(self) -> None:
        if (
            self.service_uid < 0
            or self.service_uid != os.geteuid()
            or not self.service_home.is_absolute()
            or ".." in self.service_home.parts
            or self.service_home not in {PROTECTED_USER_UNIT_ANCHOR, Path("/var/lib/loom-rollout")}
        ):
            raise ValueError("protected external supervisor systemd identity is invalid")

    @property
    def environment(self) -> Mapping[str, str]:
        return {
            "HOME": str(self.service_home),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "XDG_CONFIG_HOME": str(self.service_home / ".config"),
            "XDG_RUNTIME_DIR": f"/run/user/{self.service_uid}",
        }

    def timer_status(self, timer_name: str) -> TimerRuntimeStatus:
        name = _unit_name(timer_name, ".timer")
        values = self._show(
            name,
            (
                "LoadState",
                "UnitFileState",
                "ActiveState",
                "FragmentPath",
                "NeedDaemonReload",
            ),
        )
        return TimerRuntimeStatus(
            load_state=values["LoadState"],
            unit_file_state=values["UnitFileState"],
            active_state=values["ActiveState"],
            fragment_path=values["FragmentPath"],
            need_daemon_reload=values["NeedDaemonReload"],
        )

    def service_status(self, service_name: str) -> ServiceRuntimeStatus:
        name = _unit_name(service_name, ".service")
        values = self._show(
            name,
            (
                "LoadState",
                "Result",
                "ExecMainStatus",
                "FragmentPath",
                "NeedDaemonReload",
            ),
        )
        raw_status = values["ExecMainStatus"]
        if raw_status == "":
            status = None
        elif raw_status.isascii() and raw_status.isdecimal() and int(raw_status) <= 255:
            status = int(raw_status)
        else:
            raise RuntimeError("protected external supervisor service status drifted")
        if values["LoadState"] == "not-found":
            return ServiceRuntimeStatus(
                load_state="not-found",
                result="",
                exec_main_status=None,
                fragment_path=values["FragmentPath"],
                need_daemon_reload=values["NeedDaemonReload"],
            )
        return ServiceRuntimeStatus(
            load_state=values["LoadState"],
            result=values["Result"],
            exec_main_status=status,
            fragment_path=values["FragmentPath"],
            need_daemon_reload=values["NeedDaemonReload"],
        )

    def daemon_reload(self) -> None:
        self._run_checked(("systemctl", "--user", "daemon-reload"), timeout_seconds=30)

    def enable_timer(self, timer_name: str) -> None:
        self._run_checked(
            ("systemctl", "--user", "enable", _unit_name(timer_name, ".timer")),
            timeout_seconds=30,
        )

    def start_timer(self, timer_name: str) -> None:
        self._run_checked(
            ("systemctl", "--user", "start", _unit_name(timer_name, ".timer")),
            timeout_seconds=30,
        )

    def stop_timer(self, timer_name: str) -> None:
        self._run_checked(
            ("systemctl", "--user", "stop", _unit_name(timer_name, ".timer")),
            timeout_seconds=30,
        )

    def disable_timer(self, timer_name: str) -> None:
        self._run_checked(
            ("systemctl", "--user", "disable", _unit_name(timer_name, ".timer")),
            timeout_seconds=30,
        )

    def stop_service(self, service_name: str) -> None:
        self._run_checked(
            ("systemctl", "--user", "stop", _unit_name(service_name, ".service")),
            timeout_seconds=30,
        )

    def reset_service_failure(self, service_name: str) -> None:
        # ResetFailedUnit deliberately does not load an unloaded unit. A clean,
        # disabled oneshot may be garbage-collected after stop, so only reset a
        # failure result that systemd must keep loaded.
        before = self.service_status(service_name)
        try:
            _normalized_disabled_service_status(before)
        except ValueError:
            self._run_checked(
                ("systemctl", "--user", "reset-failed", _unit_name(service_name, ".service")),
                timeout_seconds=30,
            )
        try:
            _normalized_disabled_service_status(self.service_status(service_name))
        except ValueError as exc:
            raise RuntimeError(
                "protected external supervisor failure state did not clear safely"
            ) from exc

    def start_service(self, service_name: str, *, timeout_seconds: float) -> None:
        if not 0 < timeout_seconds <= 7215:
            raise ValueError("protected external supervisor service timeout is invalid")
        self._run_checked(
            ("systemctl", "--user", "start", _unit_name(service_name, ".service")),
            timeout_seconds=timeout_seconds,
        )

    def _show(self, unit_name: str, properties: Sequence[str]) -> dict[str, str]:
        allowed = {
            "ActiveState",
            "ExecMainStatus",
            "LoadState",
            "FragmentPath",
            "NeedDaemonReload",
            "Result",
            "UnitFileState",
        }
        if not properties or any(value not in allowed for value in properties):
            raise ValueError("protected external supervisor status property is invalid")
        command = (
            "systemctl",
            "--user",
            "show",
            unit_name,
            "--no-pager",
            *(f"--property={value}" for value in properties),
        )
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            timeout=_SYSTEMCTL_TIMEOUT_SECONDS,
            env=dict(self.environment),
        )
        if (
            result.returncode not in {0, 1, 3, 4}
            or len(result.stdout) > _MAX_SYSTEMCTL_OUTPUT
            or len(result.stderr) > _MAX_SYSTEMCTL_OUTPUT
        ):
            raise RuntimeError("protected external supervisor status command failed safely")
        if result.returncode != 0 and not result.stdout:
            defaults = {
                "LoadState": "not-found",
                "UnitFileState": "not-found",
                "ActiveState": "inactive",
                "Result": "",
                "ExecMainStatus": "",
                "FragmentPath": "",
                "NeedDaemonReload": "no",
            }
            return {name: defaults[name] for name in properties}
        values: dict[str, str] = {}
        try:
            text = result.stdout.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError("protected external supervisor status output is invalid") from exc
        for line in text.splitlines():
            key, separator, value = line.partition("=")
            if not separator or key not in properties or key in values or len(value) > 512:
                raise RuntimeError("protected external supervisor status output drifted")
            values[key] = value
        if set(values) != set(properties):
            raise RuntimeError("protected external supervisor status output is incomplete")
        return values

    def _run_checked(self, command: tuple[str, ...], *, timeout_seconds: float) -> None:
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout_seconds,
            env=dict(self.environment),
        )
        if result.returncode != 0:
            raise RuntimeError("protected external supervisor systemctl operation failed safely")


@dataclass(frozen=True, slots=True)
class FixedExternalSupervisorTransport:
    store: UserUnitStore
    control: UserSystemdControl
    unit_dir: Path = PROTECTED_USER_UNIT_DIR

    def __post_init__(self) -> None:
        if not self.unit_dir.is_absolute() or ".." in self.unit_dir.parts:
            raise ValueError("protected external supervisor unit directory is invalid")

    def observe(
        self,
        artifact: ExternalSupervisorArtifact,
        predecessor_authority: ExternalSupervisorPredecessorAuthority | None = None,
    ) -> ExternalSupervisorLiveObservation:
        execution_hosts = {supervisor.execution_host for supervisor in artifact.supervisors}
        if len(execution_hosts) != 1:
            raise ValueError("protected external supervisor artifact spans controllers")
        predecessor_manifest = load_predecessor_manifest(
            execution_host=next(iter(execution_hosts)),
        )
        expected_names = {
            name
            for supervisor in artifact.supervisors
            for name in (supervisor.service_name, supervisor.timer_name)
        }
        unit_names = expected_names | set(self.store.list_units())
        units: dict[str, bytes | None] = {}
        timers: dict[str, TimerRuntimeStatus] = {}
        services: dict[str, ServiceRuntimeStatus] = {}
        for name in sorted(unit_names):
            units[name] = self.store.read_unit(name)
            if name.endswith(".timer"):
                timers[name] = self.control.timer_status(name)
            else:
                services[name] = self.control.service_status(name)
        canonical = self.store.read_canonical()
        authority = self._resolve_authority(
            units,
            canonical,
            predecessor_authority,
            predecessor_manifest,
        )
        return ExternalSupervisorLiveObservation(
            unit_payloads=units,
            timer_statuses=timers,
            service_statuses=services,
            canonical_identity=canonical,
            predecessor_authority=authority,
            predecessor_manifest=predecessor_manifest,
            compensation_blockers=self.store.compensation_blockers(),
        )

    def reconcile_compensations(self) -> None:
        """Converge crash prefixes to the identity selected by canonical authority."""

        try:
            pending = self.store.pending_compensations()
        except Exception as exc:
            raise ExternalSupervisorCompensationError("journal-inventory-failed") from exc
        groups: dict[tuple[str, ...], list[TimerCompensationEvidence]] = {}
        for intent in pending:
            groups.setdefault(_transition_group_key(intent), []).append(intent)
        if len(groups) > 1:
            raise ExternalSupervisorCompensationError("transition-conflict")
        reconciliation_failure: str | None = None
        for intents in groups.values():
            try:
                self._reconcile_persisted_transition(tuple(intents))
            except ExternalSupervisorCompensationError as exc:
                reconciliation_failure = exc.failure_code
            except Exception:
                reconciliation_failure = "transition-reconciliation-failed"
        try:
            blockers = self.store.compensation_blockers()
        except Exception as exc:
            raise ExternalSupervisorCompensationError("blocker-inventory-failed") from exc
        if reconciliation_failure is not None:
            raise ExternalSupervisorCompensationError(reconciliation_failure)
        if blockers:
            raise ExternalSupervisorCompensationError("unresolved-blockers")

    def apply(
        self,
        artifact: ExternalSupervisorArtifact,
        expected: ExternalSupervisorLiveObservation,
        *,
        plan_digest: str,
        attestation_digest: str,
        transition_digest: str,
    ) -> None:
        try:
            current = self.observe(artifact, expected.predecessor_authority)
            live_state = classify_external_supervisor_live_state(
                artifact,
                current,
                unit_dir=self.unit_dir,
            )
        except Exception as exc:
            raise ExternalSupervisorApplyError(
                "preapply-observation-failed",
                compensation_failure_code=None,
            ) from exc
        # A verified caller may re-attest an already exact canonical
        # predecessor to a new request-bound plan without changing unit bytes.
        if current != expected or live_state not in {"exact", "ready", "repairable"}:
            raise ExternalSupervisorApplyError(
                "preapply-state-drifted",
                compensation_failure_code=None,
            )
        transition_group_id = uuid4().hex
        target = ExternalSupervisorCanonicalIdentity.build(
            artifact,
            plan_digest=plan_digest,
            attestation_digest=attestation_digest,
            transition_group_id=transition_group_id,
            runtime_evidence_digest=_expected_activation_runtime_digest(
                artifact,
                unit_dir=self.unit_dir,
            ),
            unit_dir=str(self.unit_dir),
        )
        predecessor = self._predecessor_snapshot(
            current,
            unit_dir=self.unit_dir,
        )
        authority = current.predecessor_authority
        assert authority is not None
        intents: list[TimerCompensationEvidence] = []
        identities: list[Mapping[str, str]] = []
        for supervisor in artifact.supervisors:
            identity = self._activation_identity(
                artifact,
                supervisor,
                target=target,
                predecessor=predecessor,
                authority=authority,
                transition_digest=transition_digest,
            )
            identities.append(identity)
            intents.append(
                TimerCompensationEvidence.build(
                    **identity,
                    phase="intent",
                    reason=(
                        "supervisor-repair"
                        if live_state == "repairable"
                        and _service_runtime_is_repairable_failure(
                            current.service_statuses[supervisor.service_name]
                        )
                        else "supervisor-mutation"
                    ),
                )
            )
        publications = {
            name: (current.unit_payloads[name], payload)
            for supervisor in artifact.supervisors
            for name, payload in (
                (supervisor.service_name, supervisor.service_unit.encode("utf-8")),
                (supervisor.timer_name, supervisor.timer_unit.encode("utf-8")),
            )
        }
        failure_code = "transition-publication-failed"
        try:
            # One global transition lock writes and fsyncs every self-contained
            # intent before it performs the first byte CAS.
            self.store.publish_transition(intents, publications)
            failure_code = "unit-byte-verification-failed"
            self._verify_unit_bytes(target)
            failure_code = "daemon-reload-failed"
            self.control.daemon_reload()
            failure_code = "loaded-definition-verification-failed"
            self._verify_loaded_definitions(artifact)
            for supervisor in artifact.supervisors:
                if _supervisor_desired_active(supervisor):
                    failure_code = "service-activation-failed"
                    self.control.start_service(
                        supervisor.service_name,
                        timeout_seconds=float(supervisor.service_timeout_sec) + 15.0,
                    )
                else:
                    failure_code = "disabled-service-convergence-failed"
                    self.control.stop_timer(supervisor.timer_name)
                    self.control.disable_timer(supervisor.timer_name)
                    self.control.stop_service(supervisor.service_name)
                    self.control.reset_service_failure(supervisor.service_name)
            for supervisor in artifact.supervisors:
                if _supervisor_desired_active(supervisor):
                    failure_code = "timer-enable-failed"
                    self.control.enable_timer(supervisor.timer_name)
            for supervisor in artifact.supervisors:
                if _supervisor_desired_active(supervisor):
                    failure_code = "timer-start-failed"
                    self.control.start_timer(supervisor.timer_name)
            failure_code = "activation-verification-failed"
            self._verify_activated(artifact, target)
            failure_code = "activated-terminal-publication-failed"
            for identity in identities:
                self._record_compensation_terminal(
                    identity,
                    phase="activated",
                    reason=(
                        "timer-active"
                        if _identity_pair_desired_active(
                            target,
                            identity["service_name"],
                            identity["timer_name"],
                        )
                        else "timer-disabled"
                    ),
                )
            failure_code = "canonical-promotion-failed"
            self.store.promote_canonical(
                target,
                expected_current=(predecessor if authority.kind == "canonical" else None),
            )
            failure_code = "canonical-verification-failed"
            if self.store.read_canonical() != target:
                raise RuntimeError("protected external supervisor canonical promotion drifted")
            # Promotion is only authoritative while the exact runtime proof is
            # still current.  The immutable canonical terminals close intents.
            self._verify_activated(artifact, target)
            failure_code = "canonical-terminal-publication-failed"
            for identity in identities:
                self._record_compensation_terminal(
                    identity,
                    phase="canonical",
                    reason="canonical-promoted",
                )
        except Exception as exc:
            compensation_failure_code: str | None = None
            try:
                self.reconcile_compensations()
            except ExternalSupervisorCompensationError as compensation_exc:
                compensation_failure_code = compensation_exc.failure_code
            except Exception:
                compensation_failure_code = "transition-reconciliation-failed"
            raise ExternalSupervisorApplyError(
                failure_code,
                compensation_failure_code=compensation_failure_code,
            ) from exc

    def _reconcile_persisted_transition(
        self,
        intents: tuple[TimerCompensationEvidence, ...],
    ) -> None:
        identities = tuple(self._evidence_identity(intent) for intent in intents)
        try:
            target, predecessor, covered_units = self._validate_transition_group(intents)
            repairable_predecessor_services = frozenset(
                intent.service_name for intent in intents if intent.reason == "supervisor-repair"
            )
            pointer = self.store.read_canonical()
            target_is_active = pointer == target
            predecessor_is_active = (
                intents[0].predecessor_pointer_digest == ABSENT_PREDECESSOR_DIGEST
                and pointer is None
            ) or (
                predecessor is not None
                and predecessor.record_kind == "activation"
                and pointer == predecessor
                and ExternalSupervisorCanonicalPointer.build(pointer).pointer_digest
                == intents[0].predecessor_pointer_digest
            )
            if not target_is_active and not predecessor_is_active:
                raise RuntimeError(
                    "protected external supervisor persisted canonical pointer drifted"
                )
            # publish_transition durably writes every sibling intent before its
            # first unit CAS.  A target pointer is reached even later, so target
            # authority plus partial coverage can only mean journal loss/tamper.
            # Partial coverage remains recoverable while predecessor/absent is
            # authoritative because that is the valid pre-CAS crash boundary.
            if target_is_active and covered_units != frozenset(target.unit_payloads):
                raise RuntimeError(
                    "protected external supervisor target transition coverage drifted"
                )
            desired = target if target_is_active else predecessor
            allowed_failed_services = (
                repairable_predecessor_services if predecessor_is_active else frozenset()
            )
            current = {name: self.store.read_unit(name) for name in target.unit_payloads}
            desired_payloads = {
                name: (
                    None
                    if desired is None or name not in desired.unit_payloads
                    else desired.unit_payloads[name].encode()
                )
                for name in target.unit_payloads
            }
            target_payloads = {
                name: target.unit_payloads[name].encode() for name in target.unit_payloads
            }
            if any(
                current[name] not in (target_payloads[name], desired_payloads[name])
                for name in target.unit_payloads
            ):
                raise RuntimeError(
                    "protected external supervisor persisted transition bytes drifted"
                )
        except Exception as exc:
            self._record_group_terminal(
                identities,
                phase="failed",
                reason="identity-drift",
            )
            raise ExternalSupervisorCompensationError("transition-validation-failed") from exc

        try:
            if desired is not None:
                absent_services = sorted(
                    name
                    for name in target.unit_payloads
                    if name.endswith(".service") and name not in desired.unit_payloads
                )
                for service_name in absent_services:
                    timer_name = f"{service_name.removesuffix('.service')}.timer"
                    self.control.stop_timer(timer_name)
                    self.control.disable_timer(timer_name)
                    self.control.stop_service(service_name)
                    self.control.reset_service_failure(service_name)
                self.store.restore_transition(
                    {name: (current[name], desired_payloads[name]) for name in target.unit_payloads}
                )
                self.control.daemon_reload()
                for service_name in sorted(
                    name for name in desired.unit_payloads if name.endswith(".service")
                ):
                    timer_name = f"{service_name.removesuffix('.service')}.timer"
                    if _identity_pair_desired_active(desired, service_name, timer_name):
                        try:
                            self.control.start_service(
                                service_name,
                                timeout_seconds=_RECONCILIATION_SERVICE_TIMEOUT_SECONDS,
                            )
                        except Exception:
                            if service_name not in allowed_failed_services:
                                raise
                        self.control.enable_timer(timer_name)
                        self.control.start_timer(timer_name)
                    else:
                        self.control.stop_timer(timer_name)
                        self.control.disable_timer(timer_name)
                        self.control.stop_service(service_name)
                        self.control.reset_service_failure(service_name)
            else:
                operation_failed = False
                for timer_name in sorted(
                    name for name in target.unit_payloads if name.endswith(".timer")
                ):
                    try:
                        self.control.stop_timer(timer_name)
                    except Exception:
                        operation_failed = True
                    try:
                        self.control.disable_timer(timer_name)
                    except Exception:
                        operation_failed = True
                if operation_failed:
                    raise RuntimeError(
                        "protected external supervisor persisted compensation operation failed"
                    )
                self.store.restore_transition(
                    {name: (current[name], desired_payloads[name]) for name in target.unit_payloads}
                )
                self.control.daemon_reload()
        except Exception as exc:
            self._record_group_terminal(
                identities,
                phase="failed",
                reason="operation-failed",
            )
            raise ExternalSupervisorCompensationError("transition-operation-failed") from exc

        try:
            if desired is not None:
                verified = self._verify_reactivated_identity(
                    desired,
                    require_bound_runtime=(desired.record_kind == "activation"),
                    allowed_failed_services=allowed_failed_services,
                ) and all(
                    self._verify_reconciled_runtime(
                        service_name,
                        f"{service_name.removesuffix('.service')}.timer",
                        None,
                        None,
                    )
                    for service_name in target.unit_payloads
                    if service_name.endswith(".service")
                    and service_name not in desired.unit_payloads
                )
            else:
                verified = all(
                    self._verify_reconciled_runtime(
                        service_name,
                        f"{service_name.removesuffix('.service')}.timer",
                        None,
                        None,
                    )
                    for service_name in target.unit_payloads
                    if service_name.endswith(".service")
                )
        except Exception as exc:
            raise ExternalSupervisorCompensationError("transition-verification-failed") from exc
        if target_is_active:
            phase = "recovered"
            reason = "target-reactivated"
        elif desired is not None:
            phase = "verified"
            reason = "predecessor-reactivated"
        else:
            phase = "verified"
            reason = "inactive-disabled"
        if not verified:
            self._record_group_terminal(
                identities,
                phase="failed",
                reason="verification-failed",
            )
            raise ExternalSupervisorCompensationError("transition-verification-failed")
        self._record_group_terminal(identities, phase=phase, reason=reason)

    @staticmethod
    def _validate_transition_group(
        intents: tuple[TimerCompensationEvidence, ...],
    ) -> tuple[
        ExternalSupervisorCanonicalIdentity,
        ExternalSupervisorCanonicalIdentity | None,
        frozenset[str],
    ]:
        if not intents or len({_transition_group_key(intent) for intent in intents}) != 1:
            raise RuntimeError("protected external supervisor transition group drifted")
        target = _canonical_record_text(
            intents[0].target_canonical_json,
            label="persisted target snapshot",
        )
        predecessor = (
            None
            if not intents[0].predecessor_canonical_json
            else _canonical_record_text(
                intents[0].predecessor_canonical_json,
                label="persisted predecessor snapshot",
            )
        )
        covered_units = {
            name for intent in intents for name in (intent.service_name, intent.timer_name)
        }
        target_units = set(target.unit_payloads)
        if (
            len(covered_units) != 2 * len(intents)
            or not covered_units.issubset(target_units)
            or any(
                intent.service_name.removesuffix(".service")
                != intent.timer_name.removesuffix(".timer")
                for intent in intents
            )
            or (
                predecessor is not None
                and not set(predecessor.unit_payloads).issubset(target_units)
            )
        ):
            raise RuntimeError("protected external supervisor transition coverage drifted")
        return target, predecessor, frozenset(covered_units)

    def _record_group_terminal(
        self,
        identities: tuple[Mapping[str, str], ...],
        *,
        phase: str,
        reason: str,
    ) -> None:
        failed = False
        for identity in identities:
            try:
                self._record_compensation_terminal(
                    identity,
                    phase=phase,
                    reason=reason,
                )
            except Exception:
                failed = True
        if failed:
            raise ExternalSupervisorCompensationError("terminal-publication-failed")

    @staticmethod
    def _activation_identity(
        artifact: ExternalSupervisorArtifact,
        supervisor: ExternalSupervisorIdentity,
        *,
        target: ExternalSupervisorCanonicalIdentity,
        predecessor: ExternalSupervisorCanonicalIdentity | None,
        authority: ExternalSupervisorPredecessorAuthority,
        transition_digest: str,
    ) -> Mapping[str, str]:
        return MappingProxyType(
            {
                "compensation_id": uuid4().hex,
                "artifact_digest": artifact.artifact_digest,
                "service_name": supervisor.service_name,
                "timer_name": supervisor.timer_name,
                "service_unit_sha256": hashlib.sha256(
                    supervisor.service_unit.encode("utf-8")
                ).hexdigest(),
                "timer_unit_sha256": hashlib.sha256(
                    supervisor.timer_unit.encode("utf-8")
                ).hexdigest(),
                "predecessor_kind": authority.kind,
                "predecessor_authority_digest": authority.authority_digest,
                "predecessor_pointer_digest": (
                    ExternalSupervisorCanonicalPointer.build(predecessor).pointer_digest
                    if authority.kind == "canonical" and predecessor is not None
                    else ABSENT_PREDECESSOR_DIGEST
                ),
                "predecessor_canonical_json": (
                    "" if predecessor is None else predecessor.to_bytes().decode()
                ),
                "target_canonical_json": target.to_bytes().decode(),
                "transition_digest": transition_digest,
                "transition_group_id": target.transition_group_id,
            }
        )

    @staticmethod
    def _evidence_identity(evidence: TimerCompensationEvidence) -> Mapping[str, str]:
        return MappingProxyType(
            {
                "compensation_id": evidence.compensation_id,
                "artifact_digest": evidence.artifact_digest,
                "service_name": evidence.service_name,
                "timer_name": evidence.timer_name,
                "service_unit_sha256": evidence.service_unit_sha256,
                "timer_unit_sha256": evidence.timer_unit_sha256,
                "predecessor_kind": evidence.predecessor_kind,
                "predecessor_authority_digest": evidence.predecessor_authority_digest,
                "predecessor_pointer_digest": evidence.predecessor_pointer_digest,
                "predecessor_canonical_json": evidence.predecessor_canonical_json,
                "target_canonical_json": evidence.target_canonical_json,
                "transition_digest": evidence.transition_digest,
                "transition_group_id": evidence.transition_group_id,
            }
        )

    def _record_compensation_terminal(
        self,
        identity: Mapping[str, str],
        *,
        phase: str,
        reason: str,
    ) -> None:
        self.store.record_compensation(
            TimerCompensationEvidence.build(
                compensation_id=identity["compensation_id"],
                artifact_digest=identity["artifact_digest"],
                service_name=identity["service_name"],
                timer_name=identity["timer_name"],
                service_unit_sha256=identity["service_unit_sha256"],
                timer_unit_sha256=identity["timer_unit_sha256"],
                predecessor_kind=identity["predecessor_kind"],
                predecessor_authority_digest=identity["predecessor_authority_digest"],
                predecessor_pointer_digest=identity["predecessor_pointer_digest"],
                predecessor_canonical_json=identity["predecessor_canonical_json"],
                target_canonical_json=identity["target_canonical_json"],
                transition_digest=identity["transition_digest"],
                transition_group_id=identity["transition_group_id"],
                phase=phase,
                reason=reason,
            )
        )

    def _resolve_authority(
        self,
        units: Mapping[str, bytes | None],
        canonical: ExternalSupervisorCanonicalIdentity | None,
        requested: ExternalSupervisorPredecessorAuthority | None,
        legacy: ExternalSupervisorPredecessorManifest,
    ) -> ExternalSupervisorPredecessorAuthority:
        if requested is None:
            if canonical is not None:
                requested = ExternalSupervisorPredecessorAuthority(
                    kind="canonical",
                    authority_digest=canonical.evidence_digest,
                    unit_sha256=canonical.unit_sha256,
                )
            else:
                live = {
                    name: hashlib.sha256(payload).hexdigest()
                    for name, payload in units.items()
                    if payload is not None
                }
                if live == dict(legacy.unit_sha256):
                    requested = ExternalSupervisorPredecessorAuthority(
                        kind="legacy-manifest",
                        authority_digest=legacy.manifest_digest,
                        unit_sha256=legacy.unit_sha256,
                    )
                else:
                    raise RuntimeError(
                        "protected external supervisor predecessor is not authoritative"
                    )
        if requested.kind == "canonical":
            if (
                canonical is None
                or canonical.evidence_digest != requested.authority_digest
                or dict(canonical.unit_sha256) != dict(requested.unit_sha256)
            ):
                raise RuntimeError("protected external supervisor canonical predecessor drifted")
        elif canonical is not None:
            raise RuntimeError("protected external supervisor unexpected canonical predecessor")
        elif requested.kind == "legacy-manifest":
            if requested.authority_digest != legacy.manifest_digest or dict(
                requested.unit_sha256
            ) != dict(legacy.unit_sha256):
                raise RuntimeError("protected external supervisor legacy predecessor drifted")
        elif requested.authority_digest != ABSENT_PREDECESSOR_DIGEST or requested.unit_sha256:
            raise RuntimeError("protected external supervisor absent predecessor drifted")
        return requested

    @staticmethod
    def _predecessor_snapshot(
        observation: ExternalSupervisorLiveObservation,
        *,
        unit_dir: Path = PROTECTED_USER_UNIT_DIR,
    ) -> ExternalSupervisorCanonicalIdentity | None:
        authority = observation.predecessor_authority
        assert authority is not None
        if authority.kind == "canonical":
            predecessor = observation.canonical_identity
            if predecessor is None or predecessor.evidence_digest != authority.authority_digest:
                raise RuntimeError("protected external supervisor canonical snapshot drifted")
            return predecessor
        if authority.kind == "legacy-manifest":
            legacy = observation.predecessor_manifest or load_predecessor_manifest()
            if legacy.manifest_digest != authority.authority_digest:
                raise RuntimeError("protected external supervisor legacy snapshot drifted")
            return ExternalSupervisorCanonicalIdentity.from_manifest(
                legacy,
                unit_dir=str(unit_dir),
            )
        return None

    def _verify_unit_bytes(self, identity: ExternalSupervisorCanonicalIdentity) -> None:
        if any(
            self.store.read_unit(name) != payload.encode()
            for name, payload in identity.unit_payloads.items()
        ):
            raise RuntimeError("protected external supervisor unit changed after publication")

    def _verify_loaded_definitions(self, artifact: ExternalSupervisorArtifact) -> None:
        for supervisor in artifact.supervisors:
            timer = self.control.timer_status(supervisor.timer_name)
            service = self.control.service_status(supervisor.service_name)
            if not _definition_is_fresh(
                supervisor.timer_name,
                timer,
                unit_dir=self.unit_dir,
            ) or not _definition_is_fresh(
                supervisor.service_name,
                service,
                unit_dir=self.unit_dir,
            ):
                raise RuntimeError("protected external supervisor loaded definition is stale")

    def _verify_activated(
        self,
        artifact: ExternalSupervisorArtifact,
        target: ExternalSupervisorCanonicalIdentity,
    ) -> None:
        self._verify_unit_bytes(target)
        for supervisor in artifact.supervisors:
            timer = self.control.timer_status(supervisor.timer_name)
            service = self.control.service_status(supervisor.service_name)
            active = _supervisor_desired_active(supervisor)
            if active and (
                not _definition_is_fresh(
                    supervisor.timer_name,
                    timer,
                    unit_dir=self.unit_dir,
                )
                or not _definition_is_fresh(
                    supervisor.service_name,
                    service,
                    unit_dir=self.unit_dir,
                )
                or timer.unit_file_state != "enabled"
                or timer.active_state != "active"
                or service.result != "success"
                or service.exec_main_status != 0
            ):
                raise RuntimeError("protected external supervisor activation verification failed")
            if not active and not self._verify_reconciled_runtime(
                supervisor.service_name,
                supervisor.timer_name,
                supervisor.service_unit.encode(),
                supervisor.timer_unit.encode(),
            ):
                raise RuntimeError("protected external supervisor disable verification failed")
        if (
            _observed_activation_runtime_digest(
                artifact,
                self.control,
                unit_dir=self.unit_dir,
            )
            != target.runtime_evidence_digest
        ):
            raise RuntimeError("protected external supervisor runtime evidence drifted")

    def _verify_reactivated_identity(
        self,
        target: ExternalSupervisorCanonicalIdentity,
        *,
        require_bound_runtime: bool,
        allowed_failed_services: frozenset[str] = frozenset(),
    ) -> bool:
        expected_runtime = _expected_identity_runtime_digest(target)
        if (require_bound_runtime and target.runtime_evidence_digest != expected_runtime) or any(
            self.store.read_unit(name) != payload.encode()
            for name, payload in target.unit_payloads.items()
        ):
            return False
        services = {
            name: self.control.service_status(name)
            for name in target.unit_payloads
            if name.endswith(".service")
        }
        timers = {
            name: self.control.timer_status(name)
            for name in target.unit_payloads
            if name.endswith(".timer")
        }
        failed_services: set[str] = set()
        for service_name, service in services.items():
            timer_name = f"{service_name.removesuffix('.service')}.timer"
            timer = timers.get(timer_name)
            if timer is None:
                return False
            if _identity_pair_desired_active(target, service_name, timer_name):
                successful = service.result == "success" and service.exec_main_status == 0
                repairable_failure = (
                    service_name in allowed_failed_services
                    and _service_runtime_is_repairable_failure(service)
                )
                if (
                    not _definition_is_fresh(service_name, service, unit_dir=self.unit_dir)
                    or not (successful or repairable_failure)
                    or not _definition_is_fresh(timer_name, timer, unit_dir=self.unit_dir)
                    or timer.unit_file_state != "enabled"
                    or timer.active_state != "active"
                ):
                    return False
                if repairable_failure:
                    failed_services.add(service_name)
            elif not self._verify_reconciled_runtime(
                service_name,
                timer_name,
                target.unit_payloads[service_name].encode(),
                target.unit_payloads[timer_name].encode(),
            ):
                return False
        return bool(failed_services) or (
            _normalized_identity_runtime_digest(target, services=services, timers=timers)
            == expected_runtime
        )

    def _verify_reconciled_runtime(
        self,
        service_name: str,
        timer_name: str,
        service_payload: bytes | None,
        timer_payload: bytes | None,
    ) -> bool:
        if (
            self.store.read_unit(service_name) != service_payload
            or self.store.read_unit(timer_name) != timer_payload
        ):
            return False
        timer = self.control.timer_status(timer_name)
        service = self.control.service_status(service_name)
        if service_payload is None or timer_payload is None:
            return (
                service_payload is None
                and timer_payload is None
                and timer.load_state == "not-found"
                and timer.unit_file_state in {"", "disabled", "not-found"}
                and timer.active_state == "inactive"
                and timer.fragment_path == ""
                and timer.need_daemon_reload == "no"
                and service.load_state == "not-found"
                and service.fragment_path == ""
                and service.need_daemon_reload == "no"
            )
        no_result = service.result == "" and service.exec_main_status is None
        success = service.result == "success" and service.exec_main_status == 0
        return (
            _definition_is_fresh(timer_name, timer, unit_dir=self.unit_dir)
            and _definition_is_fresh(service_name, service, unit_dir=self.unit_dir)
            and timer.unit_file_state == "disabled"
            and timer.active_state == "inactive"
            and (no_result or success)
        )


def _expected_fragment_path(
    unit_name: str,
    *,
    unit_dir: Path = PROTECTED_USER_UNIT_DIR,
) -> str:
    return str(unit_dir / _unit_name(unit_name))


_DESIRED_ACTIVE_MARKER = "# LoomDesiredState=active"
_DESIRED_DISABLED_MARKER = "# LoomDesiredState=disabled"


def _supervisor_desired_active(supervisor: ExternalSupervisorIdentity) -> bool:
    if supervisor.enabled is not supervisor.active:
        raise ValueError("protected external supervisor desired runtime is split")
    return supervisor.enabled


def _timer_payload_desired_active(payload: str) -> bool:
    lines = payload.splitlines()
    markers = [line for line in lines if line in {_DESIRED_ACTIVE_MARKER, _DESIRED_DISABLED_MARKER}]
    if not markers:
        # Canonical records created before the desired-state marker always
        # represented an enabled and active supervisor.
        return True
    if len(markers) != 1:
        raise ValueError("protected external supervisor desired-state marker drifted")
    return markers[0] == _DESIRED_ACTIVE_MARKER


def _identity_pair_desired_active(
    identity: ExternalSupervisorCanonicalIdentity,
    service_name: str,
    timer_name: str,
) -> bool:
    if (
        service_name.removesuffix(".service") != timer_name.removesuffix(".timer")
        or service_name not in identity.unit_payloads
        or timer_name not in identity.unit_payloads
    ):
        raise ValueError("protected external supervisor canonical unit pair drifted")
    return _timer_payload_desired_active(identity.unit_payloads[timer_name])


def _normalized_disabled_service_status(
    status: ServiceRuntimeStatus,
) -> ServiceRuntimeStatus:
    no_result = status.result == "" and status.exec_main_status is None
    success = status.result == "success" and status.exec_main_status == 0
    if not (no_result or success):
        raise ValueError("protected external supervisor disabled service result drifted")
    return ServiceRuntimeStatus(
        load_state=status.load_state,
        result="",
        exec_main_status=None,
        fragment_path=status.fragment_path,
        need_daemon_reload=status.need_daemon_reload,
    )


def _expected_activation_runtime_digest(
    artifact: ExternalSupervisorArtifact,
    *,
    unit_dir: Path = PROTECTED_USER_UNIT_DIR,
) -> str:
    target = ExternalSupervisorCanonicalIdentity.build(
        artifact,
        plan_digest="0" * 64,
        attestation_digest="0" * 64,
        transition_group_id="0" * 32,
        runtime_evidence_digest="0" * 64,
        unit_dir=str(unit_dir),
    )
    return _expected_identity_runtime_digest(target)


def _expected_identity_runtime_digest(
    identity: ExternalSupervisorCanonicalIdentity,
) -> str:
    services: dict[str, ServiceRuntimeStatus] = {}
    timers: dict[str, TimerRuntimeStatus] = {}
    for service_name in sorted(
        name for name in identity.unit_payloads if name.endswith(".service")
    ):
        timer_name = f"{service_name.removesuffix('.service')}.timer"
        active = _identity_pair_desired_active(identity, service_name, timer_name)
        services[service_name] = ServiceRuntimeStatus(
            load_state="loaded",
            result="success" if active else "",
            exec_main_status=0 if active else None,
            fragment_path=_expected_fragment_path(
                service_name,
                unit_dir=Path(identity.unit_dir),
            ),
            need_daemon_reload="no",
        )
        timers[timer_name] = TimerRuntimeStatus(
            load_state="loaded",
            unit_file_state="enabled" if active else "disabled",
            active_state="active" if active else "inactive",
            fragment_path=_expected_fragment_path(
                timer_name,
                unit_dir=Path(identity.unit_dir),
            ),
            need_daemon_reload="no",
        )
    return _identity_runtime_digest(identity, services=services, timers=timers)


def _normalized_identity_runtime_digest(
    identity: ExternalSupervisorCanonicalIdentity,
    *,
    services: Mapping[str, ServiceRuntimeStatus],
    timers: Mapping[str, TimerRuntimeStatus],
) -> str:
    normalized = dict(services)
    for service_name, status in services.items():
        timer_name = f"{service_name.removesuffix('.service')}.timer"
        if not _identity_pair_desired_active(identity, service_name, timer_name):
            normalized[service_name] = _normalized_disabled_service_status(status)
    return _identity_runtime_digest(identity, services=normalized, timers=timers)


def _identity_runtime_digest(
    identity: ExternalSupervisorCanonicalIdentity,
    *,
    services: Mapping[str, ServiceRuntimeStatus],
    timers: Mapping[str, TimerRuntimeStatus],
) -> str:
    return _hash_json(
        {
            "unit_dir": identity.unit_dir,
            "services": {name: status.to_dict() for name, status in services.items()},
            "timers": {name: status.to_dict() for name, status in timers.items()},
            "unit_sha256": dict(identity.unit_sha256),
        }
    )


def _observed_activation_runtime_digest(
    artifact: ExternalSupervisorArtifact,
    control: UserSystemdControl,
    *,
    unit_dir: Path = PROTECTED_USER_UNIT_DIR,
) -> str:
    services: dict[str, ServiceRuntimeStatus] = {}
    timers: dict[str, TimerRuntimeStatus] = {}
    for supervisor in artifact.supervisors:
        service = control.service_status(supervisor.service_name)
        if not _supervisor_desired_active(supervisor):
            service = _normalized_disabled_service_status(service)
        services[supervisor.service_name] = service
        timers[supervisor.timer_name] = control.timer_status(supervisor.timer_name)
    return _hash_json(
        {
            "unit_dir": str(unit_dir),
            "services": {name: status.to_dict() for name, status in services.items()},
            "timers": {name: status.to_dict() for name, status in timers.items()},
            "unit_sha256": dict(artifact.unit_sha256),
        }
    )


def _canonical_external_supervisor_runtime_state(
    identity: ExternalSupervisorCanonicalIdentity,
    *,
    unit_payloads: Mapping[str, bytes | None],
    timer_statuses: Mapping[str, TimerRuntimeStatus],
    service_statuses: Mapping[str, ServiceRuntimeStatus],
    allowed_failed_services: frozenset[str] = frozenset(),
) -> str:
    expected_payloads = {name: payload.encode() for name, payload in identity.unit_payloads.items()}
    if (
        dict(unit_payloads) != expected_payloads
        or set(timer_statuses)
        != {name for name in identity.unit_payloads if name.endswith(".timer")}
        or set(service_statuses)
        != {name for name in identity.unit_payloads if name.endswith(".service")}
        or identity.runtime_evidence_digest != _expected_identity_runtime_digest(identity)
    ):
        return "drifted"
    failed_services: set[str] = set()
    for service_name, service in service_statuses.items():
        timer_name = f"{service_name.removesuffix('.service')}.timer"
        timer = timer_statuses[timer_name]
        if _identity_pair_desired_active(identity, service_name, timer_name):
            successful = service.result == "success" and service.exec_main_status == 0
            repairable_failure = (
                service_name in allowed_failed_services
                and _service_runtime_is_repairable_failure(service)
            )
            if (
                not _definition_is_fresh(
                    service_name,
                    service,
                    unit_dir=Path(identity.unit_dir),
                )
                or not (successful or repairable_failure)
                or not _definition_is_fresh(
                    timer_name,
                    timer,
                    unit_dir=Path(identity.unit_dir),
                )
                or timer.unit_file_state != "enabled"
                or timer.active_state != "active"
            ):
                return "drifted"
            if repairable_failure:
                failed_services.add(service_name)
        else:
            no_result = service.result == "" and service.exec_main_status is None
            success = service.result == "success" and service.exec_main_status == 0
            if (
                not _definition_is_fresh(
                    service_name,
                    service,
                    unit_dir=Path(identity.unit_dir),
                )
                or not (no_result or success)
                or not _definition_is_fresh(
                    timer_name,
                    timer,
                    unit_dir=Path(identity.unit_dir),
                )
                or timer.unit_file_state != "disabled"
                or timer.active_state != "inactive"
            ):
                return "drifted"
    return "repairable" if failed_services else "ready"


def canonical_external_supervisor_runtime_ready(
    identity: ExternalSupervisorCanonicalIdentity,
    *,
    unit_payloads: Mapping[str, bytes | None],
    timer_statuses: Mapping[str, TimerRuntimeStatus],
    service_statuses: Mapping[str, ServiceRuntimeStatus],
) -> bool:
    return (
        _canonical_external_supervisor_runtime_state(
            identity,
            unit_payloads=unit_payloads,
            timer_statuses=timer_statuses,
            service_statuses=service_statuses,
        )
        == "ready"
    )


def canonical_external_supervisor_runtime_repairable(
    identity: ExternalSupervisorCanonicalIdentity,
    *,
    unit_payloads: Mapping[str, bytes | None],
    timer_statuses: Mapping[str, TimerRuntimeStatus],
    service_statuses: Mapping[str, ServiceRuntimeStatus],
) -> bool:
    active_services = frozenset(
        service_name
        for service_name in service_statuses
        if _identity_pair_desired_active(
            identity,
            service_name,
            f"{service_name.removesuffix('.service')}.timer",
        )
    )
    return (
        _canonical_external_supervisor_runtime_state(
            identity,
            unit_payloads=unit_payloads,
            timer_statuses=timer_statuses,
            service_statuses=service_statuses,
            allowed_failed_services=active_services,
        )
        == "repairable"
    )


def _service_runtime_is_repairable_failure(status: ServiceRuntimeStatus) -> bool:
    return (
        status.result == "exit-code"
        and status.exec_main_status is not None
        and 1 <= status.exec_main_status <= 255
    )


def _definition_is_fresh(
    unit_name: str,
    status: TimerRuntimeStatus | ServiceRuntimeStatus,
    *,
    unit_dir: Path = PROTECTED_USER_UNIT_DIR,
) -> bool:
    return (
        status.load_state == "loaded"
        and status.fragment_path == _expected_fragment_path(unit_name, unit_dir=unit_dir)
        and status.need_daemon_reload == "no"
    )


def _definition_state_is_reachable(
    unit_name: str,
    status: TimerRuntimeStatus | ServiceRuntimeStatus,
    *,
    unit_dir: Path = PROTECTED_USER_UNIT_DIR,
) -> bool:
    if status.load_state == "not-found":
        return status.fragment_path == "" and status.need_daemon_reload == "no"
    return status.fragment_path == _expected_fragment_path(unit_name, unit_dir=unit_dir)


def classify_external_supervisor_live_state(
    artifact: ExternalSupervisorArtifact,
    observation: ExternalSupervisorLiveObservation,
    *,
    unit_dir: Path = PROTECTED_USER_UNIT_DIR,
) -> str:
    """Classify only reachable apply prefixes; stale bytes/state fail closed."""

    expected_units: dict[str, bytes] = {}
    execution_hosts = {supervisor.execution_host for supervisor in artifact.supervisors}
    if len(execution_hosts) != 1:
        return "drifted"
    for supervisor in artifact.supervisors:
        expected_units[supervisor.service_name] = supervisor.service_unit.encode("utf-8")
        expected_units[supervisor.timer_name] = supervisor.timer_unit.encode("utf-8")
    if (
        observation.compensation_blockers
        or set(observation.unit_payloads) != set(expected_units)
        or set(observation.timer_statuses) != {item.timer_name for item in artifact.supervisors}
        or set(observation.service_statuses) != {item.service_name for item in artifact.supervisors}
    ):
        return "drifted"

    try:
        predecessor = FixedExternalSupervisorTransport._predecessor_snapshot(
            observation,
            unit_dir=unit_dir,
        )
    except (RuntimeError, ValueError):
        return "drifted"
    if predecessor is not None and not set(predecessor.unit_payloads).issubset(expected_units):
        return "drifted"

    if (
        predecessor is not None
        and predecessor.record_kind == "activation"
        and (predecessor.candidate_sha, predecessor.candidate_tree)
        != (artifact.candidate_sha, artifact.candidate_tree)
    ):
        predecessor_units = set(predecessor.unit_payloads)
        expected_predecessor_payloads = {
            name: (predecessor.unit_payloads[name].encode() if name in predecessor_units else None)
            for name in expected_units
        }
        candidate_only_units_absent = all(
            (
                observation.timer_statuses[name].load_state == "not-found"
                and observation.timer_statuses[name].unit_file_state
                in {"", "disabled", "not-found"}
                and observation.timer_statuses[name].active_state == "inactive"
                and observation.timer_statuses[name].fragment_path == ""
                and observation.timer_statuses[name].need_daemon_reload == "no"
            )
            if name.endswith(".timer")
            else (
                observation.service_statuses[name].load_state == "not-found"
                and observation.service_statuses[name].result == ""
                and observation.service_statuses[name].exec_main_status is None
                and observation.service_statuses[name].fragment_path == ""
                and observation.service_statuses[name].need_daemon_reload == "no"
            )
            for name in set(expected_units) - predecessor_units
        )
        if (
            dict(observation.unit_payloads) == expected_predecessor_payloads
            and candidate_only_units_absent
            and canonical_external_supervisor_runtime_repairable(
                predecessor,
                unit_payloads={name: observation.unit_payloads[name] for name in predecessor_units},
                timer_statuses={
                    name: status
                    for name, status in observation.timer_statuses.items()
                    if name in predecessor_units
                },
                service_statuses={
                    name: status
                    for name, status in observation.service_statuses.items()
                    if name in predecessor_units
                },
            )
        ):
            return "repairable"

    all_target = True
    for supervisor in artifact.supervisors:
        service_bytes = observation.unit_payloads[supervisor.service_name]
        timer_bytes = observation.unit_payloads[supervisor.timer_name]
        expected_service = expected_units[supervisor.service_name]
        expected_timer = expected_units[supervisor.timer_name]
        predecessor_service = (
            None
            if predecessor is None or supervisor.service_name not in predecessor.unit_payloads
            else predecessor.unit_payloads[supervisor.service_name].encode()
        )
        predecessor_timer = (
            None
            if predecessor is None or supervisor.timer_name not in predecessor.unit_payloads
            else predecessor.unit_payloads[supervisor.timer_name].encode()
        )
        if service_bytes not in (predecessor_service, expected_service) or timer_bytes not in (
            predecessor_timer,
            expected_timer,
        ):
            return "drifted"
        complete = service_bytes == expected_service and timer_bytes == expected_timer
        all_target = all_target and complete
        timer = observation.timer_statuses[supervisor.timer_name]
        service = observation.service_statuses[supervisor.service_name]
        if (
            not _definition_state_is_reachable(
                supervisor.timer_name,
                timer,
                unit_dir=unit_dir,
            )
            or not _definition_state_is_reachable(
                supervisor.service_name,
                service,
                unit_dir=unit_dir,
            )
            or (timer.load_state == "loaded") != (service.load_state == "loaded")
            or (timer.active_state == "active" and timer.unit_file_state != "enabled")
            or (
                timer.load_state == "not-found"
                and (
                    timer.unit_file_state not in {"", "disabled", "not-found"}
                    or timer.active_state != "inactive"
                    or service.result != ""
                    or service.exec_main_status is not None
                )
            )
        ):
            return "drifted"
        no_result = service.result == "" and service.exec_main_status is None
        success = service.result == "success" and service.exec_main_status == 0
        if not (no_result or success):
            return "drifted"
    pointer = observation.canonical_identity
    pointer_is_target = (
        pointer is not None
        and pointer.record_kind == "activation"
        and pointer.artifact_digest == artifact.artifact_digest
        and pointer.candidate_sha == artifact.candidate_sha
        and pointer.candidate_tree == artifact.candidate_tree
        and pointer.environment == artifact.environment
        and pointer.unit_dir == str(unit_dir)
        and dict(pointer.unit_sha256) == dict(artifact.unit_sha256)
        and pointer.runtime_evidence_digest
        == _expected_activation_runtime_digest(artifact, unit_dir=unit_dir)
    )
    if (
        all_target
        and pointer_is_target
        and all(
            _definition_is_fresh(
                supervisor.timer_name,
                observation.timer_statuses[supervisor.timer_name],
                unit_dir=unit_dir,
            )
            and _definition_is_fresh(
                supervisor.service_name,
                observation.service_statuses[supervisor.service_name],
                unit_dir=unit_dir,
            )
            and (
                (
                    observation.timer_statuses[supervisor.timer_name].unit_file_state == "enabled"
                    and observation.timer_statuses[supervisor.timer_name].active_state == "active"
                    and observation.service_statuses[supervisor.service_name].result == "success"
                    and observation.service_statuses[supervisor.service_name].exec_main_status == 0
                )
                if _supervisor_desired_active(supervisor)
                else (
                    observation.timer_statuses[supervisor.timer_name].unit_file_state == "disabled"
                    and observation.timer_statuses[supervisor.timer_name].active_state == "inactive"
                    and (
                        (
                            observation.service_statuses[supervisor.service_name].result == ""
                            and observation.service_statuses[
                                supervisor.service_name
                            ].exec_main_status
                            is None
                        )
                        or (
                            observation.service_statuses[supervisor.service_name].result
                            == "success"
                            and observation.service_statuses[
                                supervisor.service_name
                            ].exec_main_status
                            == 0
                        )
                    )
                )
            )
            for supervisor in artifact.supervisors
        )
    ):
        return "exact"
    return "ready"


def build_fixed_external_supervisor_transport(
    *,
    service_uid: int,
) -> FixedExternalSupervisorTransport:
    """Bind the production transport to the one protected user-unit directory."""

    return FixedExternalSupervisorTransport(
        store=AtomicUserUnitStore(
            unit_dir=PROTECTED_USER_UNIT_DIR,
            service_uid=service_uid,
            creation_anchor=PROTECTED_USER_UNIT_ANCHOR,
        ),
        control=FixedUserSystemdControl(service_uid=service_uid),
        unit_dir=PROTECTED_USER_UNIT_DIR,
    )


__all__ = [
    "COMPENSATION_RECONCILIATION_FAILURE_CODES",
    "EXTERNAL_SUPERVISOR_APPLY_FAILURE_CODES",
    "PROTECTED_USER_UNIT_ANCHOR",
    "PROTECTED_USER_UNIT_DIR",
    "AtomicUserUnitStore",
    "ExternalSupervisorApplyError",
    "ExternalSupervisorCompensationError",
    "ExternalSupervisorLiveObservation",
    "FixedExternalSupervisorTransport",
    "FixedUserSystemdControl",
    "ProtectedExternalSupervisorTransport",
    "ServiceRuntimeStatus",
    "TimerCompensationEvidence",
    "TimerRuntimeStatus",
    "build_fixed_external_supervisor_transport",
    "canonical_external_supervisor_runtime_ready",
    "canonical_external_supervisor_runtime_repairable",
    "classify_external_supervisor_live_state",
]
