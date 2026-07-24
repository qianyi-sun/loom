"""Issue or reuse immutable rollout preflight attestations.

This module is deliberately orchestration-only.  Predicate implementations live
in the registered checks, so the same code remains usable by preflight and by
the final rollout verifier.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

from loom_cli.rollout.preflight_attestation_store import (
    PreflightAttestationStore,
    PreflightAttestationStoreError,
)
from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckExecution,
    CheckOperation,
    EvidenceValue,
    PreflightAttestation,
)
from loom_cli.rollout.preflight_registry import PreflightRegistry

_CHECKPOINT_TRANSITION_CHECK_IDS = frozenset(
    {"backup.lease-eligibility", "backup.rotation-capacity"}
)


@dataclass(frozen=True, slots=True)
class PreflightBlocker:
    check_id: str
    failure_code: str
    outcome: str
    blocked_by: tuple[str, ...]
    remediation: str
    evidence: Mapping[str, EvidenceValue]
    evidence_hash: str

    @classmethod
    def from_execution(cls, execution: CheckExecution) -> PreflightBlocker:
        if execution.passed:
            raise ValueError("passing check cannot be a preflight blocker")
        return cls(
            check_id=execution.check_id,
            failure_code=execution.failure_code,
            outcome=execution.outcome.value,
            blocked_by=execution.blocked_by,
            remediation=execution.remediation or "restore the declared preflight invariant",
            evidence=dict(execution.evidence),
            evidence_hash=execution.evidence_hash,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "blocked_by": list(self.blocked_by),
            "check_id": self.check_id,
            "evidence": dict(self.evidence),
            "evidence_hash": self.evidence_hash,
            "failure_code": self.failure_code,
            "outcome": self.outcome,
            "remediation": self.remediation,
        }


@dataclass(frozen=True, slots=True)
class PreflightAssessment:
    """Immutable Tier 0-2 result published before request-specific backup I/O."""

    through_tier: int
    registry_digest: str
    coverage_digest: str
    executions: tuple[CheckExecution, ...]
    blockers: tuple[PreflightBlocker, ...]
    assessment_digest: str

    @property
    def passed(self) -> bool:
        return bool(self.executions) and not self.blockers

    def to_dict(self) -> dict[str, object]:
        return {
            "assessment_digest": self.assessment_digest,
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "check_count": len(self.executions),
            "coverage_digest": self.coverage_digest,
            "passed": self.passed,
            "registry_digest": self.registry_digest,
            "through_tier": self.through_tier,
        }

    def to_record(self) -> dict[str, object]:
        """Serialize complete evidence for immutable cross-process continuation."""
        return {
            "assessment_digest": self.assessment_digest,
            "coverage_digest": self.coverage_digest,
            "executions": [execution.to_dict() for execution in self.executions],
            "registry_digest": self.registry_digest,
            "schema_version": 1,
            "through_tier": self.through_tier,
        }

    @classmethod
    def from_record(cls, data: Mapping[str, object]) -> PreflightAssessment:
        expected = {
            "assessment_digest",
            "coverage_digest",
            "executions",
            "registry_digest",
            "schema_version",
            "through_tier",
        }
        if set(data) != expected:
            raise ValueError("preflight assessment fields are invalid")
        raw_executions = data["executions"]
        if (
            data["schema_version"] != 1
            or data["through_tier"] != 2
            or not isinstance(data["registry_digest"], str)
            or not isinstance(data["coverage_digest"], str)
            or not isinstance(data["assessment_digest"], str)
            or not isinstance(raw_executions, list)
            or not raw_executions
            or not all(isinstance(item, Mapping) for item in raw_executions)
        ):
            raise ValueError("preflight assessment identity is invalid")
        executions = tuple(
            CheckExecution.from_dict(cast(Mapping[str, object], item)) for item in raw_executions
        )
        if len({execution.check_id for execution in executions}) != len(executions) or any(
            execution.tier > 2 for execution in executions
        ):
            raise ValueError("preflight assessment coverage is invalid")
        digest = _assessment_digest(
            through_tier=2,
            registry_digest=data["registry_digest"],
            coverage_digest=data["coverage_digest"],
            executions=executions,
        )
        if digest != data["assessment_digest"]:
            raise ValueError("preflight assessment digest is invalid")
        blockers = tuple(
            PreflightBlocker.from_execution(execution)
            for execution in executions
            if not execution.passed
        )
        return cls(
            through_tier=2,
            registry_digest=data["registry_digest"],
            coverage_digest=data["coverage_digest"],
            executions=executions,
            blockers=blockers,
            assessment_digest=digest,
        )


@dataclass(frozen=True, slots=True)
class PreflightPipelineResult:
    registry_digest: str
    coverage_digest: str
    executions: tuple[CheckExecution, ...]
    blockers: tuple[PreflightBlocker, ...]
    attestation: PreflightAttestation | None
    reused: bool

    @property
    def passed(self) -> bool:
        return self.attestation is not None and not self.blockers

    def to_dict(self) -> dict[str, object]:
        return {
            "attestation_digest": (
                None if self.attestation is None else self.attestation.attestation_digest
            ),
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "check_count": len(self.executions),
            "coverage_digest": self.coverage_digest,
            "passed": self.passed,
            "registry_digest": self.registry_digest,
            "reused": self.reused,
        }


@dataclass(frozen=True, slots=True)
class PreflightRehearsal:
    """Complete Tier 0-3 evidence awaiting restore lease publication."""

    registry_digest: str
    coverage_digest: str
    executions: tuple[CheckExecution, ...]
    blockers: tuple[PreflightBlocker, ...]
    rehearsal_digest: str

    @property
    def passed(self) -> bool:
        return bool(self.executions) and not self.blockers

    def to_dict(self) -> dict[str, object]:
        return {
            "blockers": [blocker.to_dict() for blocker in self.blockers],
            "check_count": len(self.executions),
            "coverage_digest": self.coverage_digest,
            "passed": self.passed,
            "registry_digest": self.registry_digest,
            "rehearsal_digest": self.rehearsal_digest,
        }

    def to_record(self) -> dict[str, object]:
        return {
            "coverage_digest": self.coverage_digest,
            "executions": [execution.to_dict() for execution in self.executions],
            "registry_digest": self.registry_digest,
            "rehearsal_digest": self.rehearsal_digest,
            "schema_version": 1,
        }

    def require_integrity(self) -> None:
        try:
            executions_round_trip = all(
                CheckExecution.from_dict(execution.to_dict()) == execution
                for execution in self.executions
            )
        except ValueError as exc:
            raise ValueError("preflight rehearsal authority is incomplete or drifted") from exc
        if (
            not self.executions
            or any(
                len(value) != 64 or any(character not in "0123456789abcdef" for character in value)
                for value in (
                    self.registry_digest,
                    self.coverage_digest,
                    self.rehearsal_digest,
                )
            )
            or len({execution.check_id for execution in self.executions}) != len(self.executions)
            or not executions_round_trip
            or self.rehearsal_digest
            != _rehearsal_digest(
                registry_digest=self.registry_digest,
                coverage_digest=self.coverage_digest,
                executions=self.executions,
            )
        ):
            raise ValueError("preflight rehearsal authority is incomplete or drifted")

    @classmethod
    def from_executions(
        cls,
        *,
        registry_digest: str,
        coverage_digest: str,
        executions: Sequence[CheckExecution],
    ) -> PreflightRehearsal:
        stable = tuple(executions)
        blockers = tuple(
            PreflightBlocker.from_execution(execution)
            for execution in stable
            if not execution.passed
        )
        rehearsal = cls(
            registry_digest=registry_digest,
            coverage_digest=coverage_digest,
            executions=stable,
            blockers=blockers,
            rehearsal_digest=_rehearsal_digest(
                registry_digest=registry_digest,
                coverage_digest=coverage_digest,
                executions=stable,
            ),
        )
        rehearsal.require_integrity()
        return rehearsal

    @classmethod
    def from_record(cls, data: Mapping[str, object]) -> PreflightRehearsal:
        expected = {
            "coverage_digest",
            "executions",
            "registry_digest",
            "rehearsal_digest",
            "schema_version",
        }
        raw = data.get("executions")
        if (
            set(data) != expected
            or data.get("schema_version") != 1
            or not isinstance(data.get("registry_digest"), str)
            or not isinstance(data.get("coverage_digest"), str)
            or not isinstance(data.get("rehearsal_digest"), str)
            or not isinstance(raw, list)
            or not raw
            or not all(isinstance(item, Mapping) for item in raw)
        ):
            raise ValueError("preflight rehearsal record is invalid")
        rehearsal = cls.from_executions(
            registry_digest=cast(str, data["registry_digest"]),
            coverage_digest=cast(str, data["coverage_digest"]),
            executions=tuple(
                CheckExecution.from_dict(cast(Mapping[str, object], item)) for item in raw
            ),
        )
        if rehearsal.rehearsal_digest != data["rehearsal_digest"]:
            raise ValueError("preflight rehearsal record digest is invalid")
        return rehearsal


class PreflightPipeline:
    """Execute the exact registered DAG and publish a digest-addressed result."""

    def __init__(
        self,
        *,
        registry: PreflightRegistry,
        store: PreflightAttestationStore,
        max_concurrency: int = 8,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        if registry.through_tier != 3:
            raise ValueError("rollout preflight registry must cover exactly tiers 0 through 3")
        self._registry = registry
        self._store = store
        self._max_concurrency = max_concurrency
        self._now = now or (lambda: datetime.now(UTC))

    def authorize(
        self,
        *,
        context: CheckContext,
        bindings: AttestationBindings | None = None,
        binding_factory: Callable[[Sequence[CheckExecution]], AttestationBindings] | None = None,
        reusable_attestation_digest: str | None = None,
        assessment: PreflightAssessment | None = None,
    ) -> PreflightPipelineResult:
        """Reuse an exact valid attestation or execute every independent check."""
        now = self._clock()
        if reusable_attestation_digest is not None and bindings is not None:
            reusable = self._read_reusable(
                reusable_attestation_digest,
                bindings=bindings,
                now=now,
            )
            if reusable is not None:
                if assessment is not None:
                    self._require_reusable_assessment(
                        assessment,
                        attestation=reusable,
                        now=now,
                    )
                return PreflightPipelineResult(
                    registry_digest=self._registry.registry_digest,
                    coverage_digest=self._registry.coverage_digest,
                    executions=(),
                    blockers=(),
                    attestation=reusable,
                    reused=True,
                )

        rehearsal = self.rehearse(context=context, assessment=assessment)
        if rehearsal.blockers:
            return PreflightPipelineResult(
                registry_digest=rehearsal.registry_digest,
                coverage_digest=rehearsal.coverage_digest,
                executions=rehearsal.executions,
                blockers=rehearsal.blockers,
                attestation=None,
                reused=False,
            )
        if bindings is None:
            if binding_factory is None:
                raise ValueError("fresh preflight requires an attestation binding factory")
            bindings = binding_factory(rehearsal.executions)
        attestation = self.attest(rehearsal=rehearsal, bindings=bindings)
        return PreflightPipelineResult(
            registry_digest=rehearsal.registry_digest,
            coverage_digest=rehearsal.coverage_digest,
            executions=rehearsal.executions,
            blockers=(),
            attestation=attestation,
            reused=False,
        )

    def rehearse(
        self,
        *,
        context: CheckContext,
        assessment: PreflightAssessment | None = None,
    ) -> PreflightRehearsal:
        """Execute Tier 0-3 without claiming a restore-verified lease yet."""
        now = self._clock()
        executions = self._registry.dag(max_concurrency=self._max_concurrency).run(
            context,
            operation=CheckOperation.PROBE,
            through_tier=3,
            now=self._clock,
        )
        if assessment is not None:
            self._require_matching_assessment(assessment, executions=executions, now=now)
        return PreflightRehearsal.from_executions(
            registry_digest=self._registry.registry_digest,
            coverage_digest=self._registry.coverage_digest,
            executions=executions,
        )

    def attest(
        self,
        *,
        rehearsal: PreflightRehearsal,
        bindings: AttestationBindings,
    ) -> PreflightAttestation:
        """Issue only after the caller has published restore-verified lease authority."""
        rehearsal.require_integrity()
        expected = dict(self._registry.implementation_digests)
        executions = {execution.check_id: execution for execution in rehearsal.executions}
        if (
            not rehearsal.passed
            or rehearsal.registry_digest != self._registry.registry_digest
            or rehearsal.coverage_digest != self._registry.coverage_digest
            or set(executions) != set(expected)
            or any(
                not execution.passed or execution.implementation_digest != expected[check_id]
                for check_id, execution in executions.items()
            )
        ):
            raise ValueError("preflight rehearsal authority is incomplete or drifted")
        attestation = PreflightAttestation.issue(
            bindings=bindings,
            executions=rehearsal.executions,
            issued_at=self._clock(),
            registry_digest=self._registry.registry_digest,
            coverage_digest=self._registry.coverage_digest,
        )
        self._store.publish(attestation)
        return attestation

    def assess(
        self,
        *,
        context: CheckContext,
        through_tier: int = 2,
    ) -> PreflightAssessment:
        """Run all pre-backup tiers and bind their complete blocker report."""
        if through_tier != 2:
            raise ValueError("pre-backup assessment must cover exactly tiers 0 through 2")
        executions = self._registry.dag(max_concurrency=self._max_concurrency).run(
            context,
            operation=CheckOperation.PROBE,
            through_tier=through_tier,
            now=self._clock,
        )
        blockers = tuple(
            PreflightBlocker.from_execution(execution)
            for execution in executions
            if not execution.passed
        )
        digest = _assessment_digest(
            through_tier=through_tier,
            registry_digest=self._registry.registry_digest,
            coverage_digest=self._registry.coverage_digest,
            executions=executions,
        )
        return PreflightAssessment(
            through_tier=through_tier,
            registry_digest=self._registry.registry_digest,
            coverage_digest=self._registry.coverage_digest,
            executions=executions,
            blockers=blockers,
            assessment_digest=digest,
        )

    def _require_matching_assessment(
        self,
        assessment: PreflightAssessment,
        *,
        executions: tuple[CheckExecution, ...],
        now: datetime,
    ) -> None:
        if (
            assessment.through_tier != 2
            or not assessment.passed
            or assessment.registry_digest != self._registry.registry_digest
            or assessment.coverage_digest != self._registry.coverage_digest
            or assessment.assessment_digest
            != _assessment_digest(
                through_tier=assessment.through_tier,
                registry_digest=assessment.registry_digest,
                coverage_digest=assessment.coverage_digest,
                executions=assessment.executions,
            )
        ):
            raise ValueError("pre-backup assessment identity is invalid")
        prior = {execution.check_id: execution for execution in assessment.executions}
        current = {
            execution.check_id: execution
            for execution in executions
            if execution.tier <= assessment.through_tier
        }
        if set(prior) != set(current):
            raise ValueError("pre-backup assessment coverage drifted")
        for check_id, earlier in prior.items():
            later = current[check_id]
            if (
                later.expires_at <= now
                or not later.passed
                or earlier.implementation_digest != later.implementation_digest
                or (
                    check_id not in _CHECKPOINT_TRANSITION_CHECK_IDS
                    and earlier.input_fingerprint != later.input_fingerprint
                )
            ):
                raise ValueError("pre-backup assessment evidence drifted")

    def _require_reusable_assessment(
        self,
        assessment: PreflightAssessment,
        *,
        attestation: PreflightAttestation,
        now: datetime,
    ) -> None:
        if (
            assessment.through_tier != 2
            or not assessment.passed
            or assessment.registry_digest != self._registry.registry_digest
            or assessment.coverage_digest != self._registry.coverage_digest
            or assessment.assessment_digest
            != _assessment_digest(
                through_tier=assessment.through_tier,
                registry_digest=assessment.registry_digest,
                coverage_digest=assessment.coverage_digest,
                executions=assessment.executions,
            )
        ):
            raise ValueError("pre-backup assessment identity is invalid")
        for execution in assessment.executions:
            if (
                execution.expires_at <= now
                or not execution.passed
                or attestation.evidence_hashes.get(execution.check_id) != execution.evidence_hash
                or attestation.check_implementation_digests.get(execution.check_id)
                != execution.implementation_digest
            ):
                raise ValueError("pre-backup assessment evidence drifted")

    def _read_reusable(
        self,
        digest: str,
        *,
        bindings: AttestationBindings,
        now: datetime,
    ) -> PreflightAttestation | None:
        try:
            attestation = self._store.read(digest)
        except PreflightAttestationStoreError:
            return None
        expected = dict(self._registry.implementation_digests)
        if (
            not attestation.valid_for(bindings, now=now)
            or attestation.registry_digest != self._registry.registry_digest
            or attestation.coverage_digest != self._registry.coverage_digest
            or dict(attestation.check_implementation_digests) != expected
            or set(attestation.evidence_hashes) != set(expected)
        ):
            return None
        return attestation

    def _clock(self) -> datetime:
        value = self._now()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("preflight pipeline clock must be timezone-aware")
        return value.astimezone(UTC)


def evidence_by_check(
    executions: tuple[CheckExecution, ...],
) -> Mapping[str, Mapping[str, object]]:
    """Return immutable-safe evidence indexing for binding construction."""
    return {execution.check_id: dict(execution.evidence) for execution in executions}


def _assessment_digest(
    *,
    through_tier: int,
    registry_digest: str,
    coverage_digest: str,
    executions: Sequence[CheckExecution],
) -> str:
    payload = {
        "coverage_digest": coverage_digest,
        "executions": [
            {
                "check_id": execution.check_id,
                "evidence_hash": execution.evidence_hash,
                "expires_at": execution.expires_at.isoformat(),
                "implementation_digest": execution.implementation_digest,
                "input_fingerprint": execution.input_fingerprint,
                "outcome": execution.outcome.value,
                "tier": execution.tier,
            }
            for execution in executions
        ],
        "registry_digest": registry_digest,
        "schema_version": 1,
        "through_tier": through_tier,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _rehearsal_digest(
    *,
    registry_digest: str,
    coverage_digest: str,
    executions: Sequence[CheckExecution],
) -> str:
    payload = {
        "coverage_digest": coverage_digest,
        "executions": [execution.to_dict() for execution in executions],
        "registry_digest": registry_digest,
        "schema_version": 1,
        "through_tier": 3,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "PreflightAssessment",
    "PreflightBlocker",
    "PreflightPipeline",
    "PreflightPipelineResult",
    "PreflightRehearsal",
    "evidence_by_check",
]
