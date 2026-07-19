"""Issue or reuse immutable rollout preflight attestations.

This module is deliberately orchestration-only.  Predicate implementations live
in the registered checks, so the same code remains usable by preflight and by
the final rollout verifier.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from loom_cli.rollout.preflight_attestation_store import (
    PreflightAttestationStore,
    PreflightAttestationStoreError,
)
from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckExecution,
    CheckOperation,
    PreflightAttestation,
)
from loom_cli.rollout.preflight_registry import PreflightRegistry


@dataclass(frozen=True, slots=True)
class PreflightBlocker:
    check_id: str
    failure_code: str
    outcome: str
    blocked_by: tuple[str, ...]
    remediation: str
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
            evidence_hash=execution.evidence_hash,
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "blocked_by": list(self.blocked_by),
            "check_id": self.check_id,
            "evidence_hash": self.evidence_hash,
            "failure_code": self.failure_code,
            "outcome": self.outcome,
            "remediation": self.remediation,
        }


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
        bindings: AttestationBindings,
        reusable_attestation_digest: str | None = None,
    ) -> PreflightPipelineResult:
        """Reuse an exact valid attestation or execute every independent check."""
        now = self._clock()
        if reusable_attestation_digest is not None:
            reusable = self._read_reusable(
                reusable_attestation_digest,
                bindings=bindings,
                now=now,
            )
            if reusable is not None:
                return PreflightPipelineResult(
                    registry_digest=self._registry.registry_digest,
                    coverage_digest=self._registry.coverage_digest,
                    executions=(),
                    blockers=(),
                    attestation=reusable,
                    reused=True,
                )

        executions = self._registry.dag(max_concurrency=self._max_concurrency).run(
            context,
            operation=CheckOperation.PROBE,
            through_tier=3,
            now=self._clock,
        )
        blockers = tuple(
            PreflightBlocker.from_execution(execution)
            for execution in executions
            if not execution.passed
        )
        if blockers:
            return PreflightPipelineResult(
                registry_digest=self._registry.registry_digest,
                coverage_digest=self._registry.coverage_digest,
                executions=executions,
                blockers=blockers,
                attestation=None,
                reused=False,
            )
        attestation = PreflightAttestation.issue(
            bindings=bindings,
            executions=executions,
            issued_at=self._clock(),
            registry_digest=self._registry.registry_digest,
            coverage_digest=self._registry.coverage_digest,
        )
        self._store.publish(attestation)
        return PreflightPipelineResult(
            registry_digest=self._registry.registry_digest,
            coverage_digest=self._registry.coverage_digest,
            executions=executions,
            blockers=(),
            attestation=attestation,
            reused=False,
        )

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


__all__ = [
    "PreflightBlocker",
    "PreflightPipeline",
    "PreflightPipelineResult",
    "evidence_by_check",
]
