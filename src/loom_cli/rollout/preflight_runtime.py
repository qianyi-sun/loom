"""Assemble one restart-safe candidate preflight plan from shared checks."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType

from loom_cli.rollout.operator.checkpoint_lease import CriticalCheckpointEvidence
from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
from loom_cli.rollout.preflight_contract import CheckContext, RegisteredCheck, SafeValue
from loom_cli.rollout.preflight_registered_checks import build_rehearsal_checks
from loom_cli.rollout.preflight_registry import PreflightRegistry
from loom_cli.rollout.rehearsal_readiness import (
    REHEARSAL_CHECK_IDS,
    RehearsalAction,
    RehearsalResult,
)

_ZERO_SHA256 = "0" * 64

RehearsalActionFactory = Callable[
    [CandidateBinding, CriticalCheckpointEvidence, str],
    Mapping[str, RehearsalAction],
]
RehearsalIdentityFactory = Callable[
    [CandidateBinding, CriticalCheckpointEvidence],
    tuple[str, str],
]
StaticCheckFactory = Callable[
    [],
    tuple[
        tuple[RegisteredCheck, ...],
        tuple[RegisteredCheck, ...],
        tuple[RegisteredCheck, ...],
    ],
]


def _candidate_bindings(candidate: CandidateBinding) -> dict[str, SafeValue]:
    return {
        "candidate.base.sha": candidate.approved_base_sha or "none",
        "candidate.sha": candidate.resolved_sha,
        "candidate.source-mode": candidate.source_mode,
    }


def _unavailable_rehearsal_actions(
    *, candidate_sha: str, mutation_epoch: int, isolation_id: str
) -> Mapping[str, RehearsalAction]:
    """Create fail-closed placeholders that are never run by Tier 0-2 assessment."""

    def action(check_id: str) -> RehearsalAction:
        def unavailable() -> RehearsalResult:
            return RehearsalResult(
                check_id=check_id,
                isolation_id=isolation_id,
                candidate_sha=candidate_sha,
                mutation_epoch=mutation_epoch,
                evidence_digest=_ZERO_SHA256,
                journal_digest=_ZERO_SHA256,
                protected_mutation=False,
                cleanup_verified=False,
                blockers={"checkpoint": "not-yet-published"},
            )

        return unavailable

    return MappingProxyType({check_id: action(check_id) for check_id in REHEARSAL_CHECK_IDS})


@dataclass(frozen=True, slots=True)
class CandidatePreflightRuntime:
    """Bind static/baseline checks to one exact candidate and checkpoint planner.

    Tier 0-2 checks are constructed once from the shared registered-check
    implementations.  The pre-backup plan includes fail-closed Tier 3
    placeholders only so the complete registry contract is fixed before any
    request exists.  After the checkpoint is immutable, the same contract is
    rebuilt with exact isolated rehearsal actions and bindings.
    """

    candidate: CandidateBinding
    tier0: tuple[RegisteredCheck, ...]
    tier1: tuple[RegisteredCheck, ...]
    tier2: tuple[RegisteredCheck, ...]
    bindings: Mapping[str, SafeValue]
    rehearsal_actions: RehearsalActionFactory
    rehearsal_identity: RehearsalIdentityFactory
    refresh_static_checks: StaticCheckFactory | None = None

    def __post_init__(self) -> None:
        groups = {0: self.tier0, 1: self.tier1, 2: self.tier2}
        if any(not checks for checks in groups.values()) or any(
            check.spec.tier != tier for tier, checks in groups.items() for check in checks
        ):
            raise ValueError("preflight runtime static check groups are incomplete")
        all_checks = self.tier0 + self.tier1 + self.tier2
        if len({check.spec.check_id for check in all_checks}) != len(all_checks):
            raise ValueError("preflight runtime contains duplicate static checks")
        normalized = dict(self.bindings)
        expected = _candidate_bindings(self.candidate)
        if any(normalized.get(key) != value for key, value in expected.items()):
            raise ValueError("preflight runtime candidate binding drifted")
        reserved = {"checkpoint.evidence.sha256", "rehearsal.plan.sha256"}
        if reserved & normalized.keys():
            raise ValueError("checkpoint-only bindings cannot be declared before backup")
        required = {key for check in all_checks for key in check.spec.input_keys} | {
            "staging.mutation-epoch"
        }
        missing = required - normalized.keys()
        if missing:
            raise ValueError(f"preflight runtime bindings are incomplete: {sorted(missing)}")
        if (
            type(normalized["staging.mutation-epoch"]) is not int
            or normalized["staging.mutation-epoch"] < 0
        ):
            raise ValueError("preflight runtime mutation epoch is invalid")
        object.__setattr__(self, "bindings", MappingProxyType(normalized))
        self._static_checks()

    def prebackup_plan(self, candidate: CandidateBinding) -> CandidatePreflightPlan:
        """Return the complete registry used for Tier 0-2 admission."""
        self._require_candidate(candidate)
        epoch = self._mutation_epoch
        isolation_id = f"rehearsal-prebackup-{candidate.resolved_sha[:16]}"
        return self._plan(
            checkpoint_digest=_ZERO_SHA256,
            rehearsal_plan_digest=_ZERO_SHA256,
            isolation_id=isolation_id,
            actions=_unavailable_rehearsal_actions(
                candidate_sha=candidate.resolved_sha,
                mutation_epoch=epoch,
                isolation_id=isolation_id,
            ),
        )

    def checkpoint_plan(
        self,
        candidate: CandidateBinding,
        checkpoint: CriticalCheckpointEvidence,
    ) -> CandidatePreflightPlan:
        """Rebuild the same registry with exact checkpoint-bound rehearsal actions."""
        self._require_candidate(candidate)
        if (
            checkpoint.environment != self.bindings.get("environment")
            or checkpoint.namespace != self.bindings.get("namespace")
            or checkpoint.mutation_epoch != self._mutation_epoch
        ):
            raise ValueError("checkpoint identity drifts from preflight runtime")
        isolation_id, plan_digest = self.rehearsal_identity(candidate, checkpoint)
        if (
            not isolation_id.startswith("rehearsal-")
            or len(plan_digest) != 64
            or any(character not in "0123456789abcdef" for character in plan_digest)
        ):
            raise ValueError("rehearsal identity is invalid")
        actions = self.rehearsal_actions(candidate, checkpoint, isolation_id)
        return self._plan(
            checkpoint_digest=checkpoint.evidence_digest,
            rehearsal_plan_digest=plan_digest,
            isolation_id=isolation_id,
            actions=actions,
        )

    @property
    def _mutation_epoch(self) -> int:
        value = self.bindings["staging.mutation-epoch"]
        assert type(value) is int
        return value

    def _require_candidate(self, candidate: CandidateBinding) -> None:
        if candidate != self.candidate:
            raise ValueError("preflight runtime candidate changed")

    def _plan(
        self,
        *,
        checkpoint_digest: str,
        rehearsal_plan_digest: str,
        isolation_id: str,
        actions: Mapping[str, RehearsalAction],
    ) -> CandidatePreflightPlan:
        tier0, tier1, tier2 = self._static_checks()
        tier3 = build_rehearsal_checks(
            actions,
            isolation_id=isolation_id,
            candidate_sha=self.candidate.resolved_sha,
            mutation_epoch=self._mutation_epoch,
            checkpoint_evidence_digest=checkpoint_digest,
            rehearsal_plan_digest=rehearsal_plan_digest,
        )
        registry = PreflightRegistry.build(
            tier0 + tier1 + tier2 + tier3,
            through_tier=3,
        )
        context = CheckContext(
            {
                **dict(self.bindings),
                "checkpoint.evidence.sha256": checkpoint_digest,
                "rehearsal.plan.sha256": rehearsal_plan_digest,
            }
        )
        return CandidatePreflightPlan(
            candidate=self.candidate,
            registry=registry,
            context=context,
        )

    def _static_checks(
        self,
    ) -> tuple[
        tuple[RegisteredCheck, ...],
        tuple[RegisteredCheck, ...],
        tuple[RegisteredCheck, ...],
    ]:
        groups = (
            (self.tier0, self.tier1, self.tier2)
            if self.refresh_static_checks is None
            else self.refresh_static_checks()
        )
        if len(groups) != 3:
            raise ValueError("preflight runtime check factory is invalid")
        declared = self.tier0 + self.tier1 + self.tier2
        refreshed = groups[0] + groups[1] + groups[2]
        declared_identity = {
            check.spec.check_id: (check.spec.contract_digest, check.implementation_digest)
            for check in declared
        }
        refreshed_identity = {
            check.spec.check_id: (check.spec.contract_digest, check.implementation_digest)
            for check in refreshed
        }
        if (
            len(refreshed_identity) != len(refreshed)
            or refreshed_identity != declared_identity
            or any(
                check.spec.tier != tier for tier, checks in enumerate(groups) for check in checks
            )
        ):
            raise ValueError("preflight runtime check factory changed implementation identity")
        return groups


__all__ = [
    "CandidatePreflightRuntime",
    "RehearsalActionFactory",
    "RehearsalIdentityFactory",
    "StaticCheckFactory",
]
