"""Fail-closed drift admission immediately before protected final gates."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime

from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
from loom_cli.rollout.preflight_contract import (
    AdmissionPhase,
    CheckExecution,
    CheckOperation,
    PreflightAttestation,
    PreflightDag,
    RegisteredCheck,
    identity_evidence_hash,
)

_BASELINE_CHECKS = frozenset(
    {
        "staging.health",
        "staging.auth",
        "staging.catalog-task",
        "staging.storage-db",
        "staging.network",
        "staging.release-baseline",
    }
)
_RUNTIME_UPGRADE_ATTESTED_DEPENDENCIES = frozenset({"runner.install"})


class FinalAttestationAdmissionError(ValueError):
    """Secret-safe, bounded failure identity for pre-apply admission."""

    _FAILURE_CODES = frozenset(
        {
            "check-contract-invalid",
            "check-failed",
            "clock-invalid",
            "context-drift",
            "evidence-drift",
            "identity-drift",
        }
    )

    def __init__(self, failure_code: str, message: str) -> None:
        if failure_code not in self._FAILURE_CODES:
            raise ValueError("final admission failure code is invalid")
        self.failure_code = failure_code
        super().__init__(message)


class PostApplyDriftTransientError(ValueError):
    """A post-apply observation was temporarily incomplete but may converge."""


def _checks_for_admission_phase(
    plan: CandidatePreflightPlan,
    phase: AdmissionPhase,
) -> tuple[tuple[RegisteredCheck, ...], frozenset[str]]:
    available = {check.spec.check_id: check for check in plan.registry.checks}
    selected = {
        check_id for check_id, check in available.items() if phase in check.spec.admission_phases
    }
    if not selected:
        raise ValueError("admission phase has no registered checks")
    required = set(selected)
    pending = list(selected)
    while pending:
        check_id = pending.pop()
        try:
            dependencies = available[check_id].spec.dependencies
        except KeyError as exc:
            raise ValueError("admission check implementation is missing") from exc
        for dependency in dependencies:
            if dependency not in available:
                raise ValueError("admission check dependency is missing")
            if dependency not in required:
                required.add(dependency)
                pending.append(dependency)
    checks = tuple(check for check in plan.registry.checks if check.spec.check_id in required)
    return checks, frozenset(selected)


def _identity_evidence_matches(
    *,
    checks: tuple[RegisteredCheck, ...],
    executions: tuple[CheckExecution, ...],
    attestation: PreflightAttestation,
) -> bool:
    specs = {check.spec.check_id: check.spec for check in checks}
    by_id = {execution.check_id: execution for execution in executions}
    return specs.keys() == by_id.keys() and all(
        identity_evidence_hash(specs[check_id], by_id[check_id].evidence)
        == attestation.identity_evidence_hashes.get(check_id)
        for check_id in specs
    )


def _partition_post_apply_resume_checks(
    *,
    checks: tuple[RegisteredCheck, ...],
    admission: FinalAttestationAdmission,
    plan: CandidatePreflightPlan,
    attested_dependencies: frozenset[str],
) -> tuple[tuple[RegisteredCheck, ...], tuple[CheckExecution, ...]]:
    """Reuse only an explicitly admitted historical runner installation."""
    if (
        not isinstance(attested_dependencies, frozenset)
        or not attested_dependencies <= _RUNTIME_UPGRADE_ATTESTED_DEPENDENCIES
    ):
        raise ValueError("post-apply resume attested dependencies are invalid")
    if not attested_dependencies:
        return checks, ()
    if not admission.post_apply_resume:
        raise ValueError("post-apply resume dependency requires resumed admission")

    available = {check.spec.check_id: check for check in checks}
    prior = {execution.check_id: execution for execution in admission.tier0_executions}
    admitted: list[CheckExecution] = []
    for check_id in sorted(attested_dependencies):
        check = available.get(check_id)
        execution = prior.get(check_id)
        if (
            check is None
            or execution is None
            or not execution.passed
            or execution.failure_code != check.spec.failure_code
            or execution.tier != check.spec.tier
            or execution.stage is not check.spec.stage
            or execution.operation is not CheckOperation.PROBE
            or execution.input_fingerprint != check.input_fingerprint(plan.context)
            or execution.implementation_digest != check.implementation_digest
            or admission.attestation.check_implementation_digests.get(check_id)
            != execution.implementation_digest
            or identity_evidence_hash(check.spec, execution.evidence)
            != admission.attestation.identity_evidence_hashes.get(check_id)
        ):
            raise ValueError("post-apply resume attested dependency drifted")
        admitted.append(execution)
    runnable = tuple(check for check in checks if check.spec.check_id not in attested_dependencies)
    return runnable, tuple(admitted)


@dataclass(frozen=True, slots=True)
class FinalAttestationAdmission:
    attestation: PreflightAttestation
    tier0_executions: tuple[CheckExecution, ...]
    tier2_executions: tuple[CheckExecution, ...]
    preflight_plan: CandidatePreflightPlan | None = None
    post_apply_resume: bool = False

    def __post_init__(self) -> None:
        tier2_by_id = {execution.check_id: execution for execution in self.tier2_executions}
        if (
            not self.tier0_executions
            or any(result.tier != 0 or not result.passed for result in self.tier0_executions)
            or len(self.tier2_executions) != len(_BASELINE_CHECKS)
            or set(tier2_by_id) != _BASELINE_CHECKS
            or any(result.tier != 2 or not result.passed for result in self.tier2_executions)
            or type(self.post_apply_resume) is not bool
        ):
            raise ValueError("final attestation admission is incomplete")


@dataclass(frozen=True, slots=True)
class PostApplyDriftEvidence:
    """Exact read-only drift evidence after the rollout epoch is claimed."""

    observed_mutation_epoch: int
    executions: tuple[CheckExecution, ...]
    evidence_digest: str

    def __post_init__(self) -> None:
        if (
            self.observed_mutation_epoch < 1
            or not self.executions
            or any(not execution.passed or execution.tier != 0 for execution in self.executions)
            or len(self.evidence_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.evidence_digest)
        ):
            raise ValueError("post-apply drift evidence is invalid")


def validate_final_attestation(
    *,
    attestation: PreflightAttestation,
    candidate: CandidateBinding,
    plan: CandidatePreflightPlan,
    current_mutation_epoch: int,
    now: datetime,
    max_concurrency: int = 8,
) -> FinalAttestationAdmission:
    """Recheck only drift-sensitive Tier 0 authority before any live apply."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise FinalAttestationAdmissionError(
            "clock-invalid",
            "final admission clock must be timezone-aware",
        )
    bindings = attestation.bindings
    if (
        attestation.schema_version != 3
        or now >= attestation.expires_at
        or plan.candidate != candidate
        or bindings.candidate_sha != candidate.resolved_sha
        or bindings.candidate_tree != candidate.resolved_tree
        or bindings.staging_mutation_epoch != current_mutation_epoch
        or plan.context.bindings.get("staging.mutation-epoch") != current_mutation_epoch
        or plan.registry.registry_digest != attestation.registry_digest
        or plan.registry.coverage_digest != attestation.coverage_digest
        or plan.registry.implementation_digests != attestation.check_implementation_digests
    ):
        raise FinalAttestationAdmissionError(
            "identity-drift",
            "final admission attestation identity drifted",
        )
    context_expected = {
        "candidate.sha": bindings.candidate_sha,
        "candidate.tree": bindings.candidate_tree,
        "runner.config.sha256": bindings.runner_config_hash,
        "environment": bindings.environment,
        "namespace": bindings.namespace,
        "route": bindings.route,
    }
    if any(plan.context.bindings.get(key) != value for key, value in context_expected.items()):
        raise FinalAttestationAdmissionError(
            "context-drift",
            "final admission context binding drifted",
        )

    try:
        drift_checks, _ = _checks_for_admission_phase(
            plan,
            AdmissionPhase.PRE_APPLY,
        )
    except ValueError as exc:
        raise FinalAttestationAdmissionError(
            "check-contract-invalid",
            "final admission check contract is invalid",
        ) from exc
    executions = PreflightDag(drift_checks, max_concurrency=max_concurrency).run(
        plan.context,
        through_tier=2,
        now=lambda: now,
    )
    tier0 = tuple(execution for execution in executions if execution.tier == 0)
    tier2 = tuple(execution for execution in executions if execution.tier == 2)
    if (
        set(execution.check_id for execution in tier2) != _BASELINE_CHECKS
        or any(not execution.passed for execution in executions)
        or any(
            execution.implementation_digest
            != attestation.check_implementation_digests[execution.check_id]
            for execution in executions
        )
    ):
        raise FinalAttestationAdmissionError(
            "check-failed",
            "final admission Tier 0 drift check failed",
        )
    if not _identity_evidence_matches(
        checks=drift_checks,
        executions=executions,
        attestation=attestation,
    ):
        raise FinalAttestationAdmissionError(
            "evidence-drift",
            "final admission drift-sensitive evidence changed",
        )
    return FinalAttestationAdmission(attestation, tier0, tier2, plan)


def validate_post_apply_attestation_drift(
    *,
    admission: FinalAttestationAdmission,
    plan: CandidatePreflightPlan,
    current_mutation_epoch: int,
    now: datetime,
    max_concurrency: int = 8,
    attested_dependencies: frozenset[str] = frozenset(),
) -> PostApplyDriftEvidence:
    """Rerun only exact inputs from a fresh post-apply runtime plan."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("post-apply drift clock must be timezone-aware")
    attestation = admission.attestation
    bindings = attestation.bindings
    if (
        (now >= attestation.expires_at and not admission.post_apply_resume)
        or current_mutation_epoch != bindings.staging_mutation_epoch + 1
        or plan.candidate.resolved_sha != bindings.candidate_sha
        or plan.candidate.resolved_tree != bindings.candidate_tree
        or plan.registry.registry_digest != attestation.registry_digest
        or plan.registry.coverage_digest != attestation.coverage_digest
    ):
        raise ValueError("post-apply mutation epoch or attestation drifted")
    context_expected = {
        "candidate.sha": bindings.candidate_sha,
        "candidate.tree": bindings.candidate_tree,
        "runner.config.sha256": bindings.runner_config_hash,
        "environment": bindings.environment,
        "namespace": bindings.namespace,
        "route": bindings.route,
        "staging.mutation-epoch": current_mutation_epoch,
    }
    if any(plan.context.bindings.get(key) != value for key, value in context_expected.items()):
        raise ValueError("post-apply context binding drifted")
    checks, selected_ids = _checks_for_admission_phase(
        plan,
        AdmissionPhase.POST_APPLY,
    )
    runnable, admitted = _partition_post_apply_resume_checks(
        checks=checks,
        admission=admission,
        plan=plan,
        attested_dependencies=attested_dependencies,
    )
    fresh_executions = PreflightDag(
        runnable,
        max_concurrency=max_concurrency,
        attested_dependencies=attested_dependencies,
    ).run(
        plan.context,
        through_tier=0,
        now=lambda: now,
    )
    executions = fresh_executions + admitted
    by_id = {execution.check_id: execution for execution in executions}
    if not selected_ids <= by_id.keys() or any(not execution.passed for execution in executions):
        raise PostApplyDriftTransientError("post-apply drift evidence is incomplete")
    selected = tuple(
        sorted(
            (by_id[check_id] for check_id in selected_ids),
            key=lambda execution: execution.check_id,
        )
    )
    if any(
        execution.implementation_digest
        != attestation.check_implementation_digests.get(execution.check_id)
        for execution in executions
    ):
        raise ValueError("post-apply drift evidence implementation changed")
    if any(execution.expires_at <= now for execution in fresh_executions):
        raise PostApplyDriftTransientError("post-apply drift evidence expired")
    if not _identity_evidence_matches(
        checks=checks,
        executions=executions,
        attestation=attestation,
    ):
        # Protected apply can leave a short read-after-write window across the
        # independently managed OLDLAB and GB10 hosts.  Re-observe without
        # mutating; the action source still fails closed after its bounded wait.
        raise PostApplyDriftTransientError("post-apply drift-sensitive evidence changed")
    payload = {
        "checks": {
            execution.check_id: {
                "evidence_hash": execution.evidence_hash,
                "implementation_digest": execution.implementation_digest,
                "input_fingerprint": execution.input_fingerprint,
            }
            for execution in selected
        },
        "observed_mutation_epoch": current_mutation_epoch,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return PostApplyDriftEvidence(current_mutation_epoch, executions, digest)


def validate_post_apply_resume_attestation(
    *,
    prior_admission: FinalAttestationAdmission,
    candidate: CandidateBinding,
    plan: CandidatePreflightPlan,
    current_mutation_epoch: int,
    now: datetime,
    max_concurrency: int = 8,
    attested_dependencies: frozenset[str] = frozenset(),
) -> FinalAttestationAdmission:
    """Re-admit an interrupted final chain after its exact protected apply."""
    attestation = prior_admission.attestation
    resumed = FinalAttestationAdmission(
        attestation,
        prior_admission.tier0_executions,
        prior_admission.tier2_executions,
        plan,
        post_apply_resume=True,
    )
    if plan.candidate != candidate:
        raise ValueError("post-apply resume candidate drifted")
    validate_post_apply_attestation_drift(
        admission=resumed,
        plan=plan,
        current_mutation_epoch=current_mutation_epoch,
        now=now,
        max_concurrency=max_concurrency,
        attested_dependencies=attested_dependencies,
    )
    checks = tuple(check for check in plan.registry.checks if check.spec.tier in {0, 2})
    runnable, admitted = _partition_post_apply_resume_checks(
        checks=checks,
        admission=resumed,
        plan=plan,
        attested_dependencies=attested_dependencies,
    )
    executions = (
        PreflightDag(
            runnable,
            max_concurrency=max_concurrency,
            attested_dependencies=attested_dependencies,
        ).run(
            plan.context,
            through_tier=2,
            now=lambda: now,
        )
        + admitted
    )
    tier2 = {execution.check_id: execution for execution in executions if execution.tier == 2}
    if (
        set(tier2) != _BASELINE_CHECKS
        or any(not execution.passed for execution in executions)
        or any(
            execution.implementation_digest
            != attestation.check_implementation_digests.get(execution.check_id)
            for execution in executions
        )
    ):
        raise ValueError("post-apply resume baseline recheck failed")
    for execution in tier2.values():
        evidence = execution.evidence
        if (
            evidence.get("ready") is not True
            or evidence.get("observed-epoch") != current_mutation_epoch
            or evidence.get("readonly-principal") in {None, ""}
            or evidence.get("resource-digest") in {None, ""}
            or evidence.get("blockers") != {}
        ):
            raise ValueError("post-apply resume baseline changed")
    return resumed


__all__ = [
    "FinalAttestationAdmission",
    "FinalAttestationAdmissionError",
    "PostApplyDriftEvidence",
    "PostApplyDriftTransientError",
    "validate_final_attestation",
    "validate_post_apply_attestation_drift",
    "validate_post_apply_resume_attestation",
]
