"""Execute the protected final gate chain from one immutable attestation."""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime

from loom_cli.rollout.final_gate_readiness import FinalGateAction
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckExecution,
    CheckOperation,
    MutationClass,
    PreflightAttestation,
    PreflightDag,
)
from loom_cli.rollout.preflight_coverage import (
    DEFAULT_COVERAGE_MANIFEST,
    load_coverage_manifest,
)
from loom_cli.rollout.preflight_registered_checks import build_final_gate_checks


@dataclass(frozen=True, slots=True)
class AttestedFinalGateReport:
    """Complete normalized Tier 4 result set for one protected attempt."""

    attestation_digest: str
    executions: tuple[CheckExecution, ...]

    def __post_init__(self) -> None:
        if (
            len(self.attestation_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.attestation_digest)
            or not self.executions
            or any(result.tier != 4 for result in self.executions)
            or len({result.check_id for result in self.executions}) != len(self.executions)
        ):
            raise ValueError("attested final gate report is invalid")

    @property
    def passed(self) -> bool:
        return all(result.passed for result in self.executions)


class AttestedFinalGateAuthority:
    """Bind the final registered checks to exact, unexpired Tier 0-3 proof."""

    def __init__(
        self,
        *,
        attestation: PreflightAttestation,
        actions: Mapping[str, FinalGateAction],
        candidate_sha: str,
        mutation_epoch: int,
        now: datetime,
        post_apply_resume: bool = False,
        protected_apply_recovery: bool = False,
        max_concurrency: int = 4,
    ) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("final gate clock must be timezone-aware")
        if (
            attestation.attestation_digest != attestation.attestation_digest.strip()
            or attestation.bindings.candidate_sha != candidate_sha
            or attestation.bindings.staging_mutation_epoch != mutation_epoch
            or type(post_apply_resume) is not bool
            or type(protected_apply_recovery) is not bool
            or (protected_apply_recovery and not post_apply_resume)
            or (now >= attestation.expires_at and not post_apply_resume)
        ):
            raise ValueError("final gate attestation is expired or drifted")
        coverage = load_coverage_manifest()
        coverage_digest = hashlib.sha256(DEFAULT_COVERAGE_MANIFEST.read_bytes()).hexdigest()
        prefinal_ids = frozenset(entry.check_id for entry in coverage.checks if entry.tier < 4)
        if (
            attestation.coverage_digest != coverage_digest
            or set(attestation.check_implementation_digests) != prefinal_ids
            or set(attestation.evidence_hashes) != prefinal_ids
        ):
            raise ValueError("final gate attested dependency coverage drifted")
        checks = build_final_gate_checks(
            actions,
            candidate_sha=candidate_sha,
            attestation_digest=attestation.attestation_digest,
            mutation_epoch=mutation_epoch,
        )
        coverage.require_exact_tier(checks, tier=4)
        self._attestation = attestation
        self._post_apply_resume = post_apply_resume
        self._protected_apply_recovery = protected_apply_recovery
        self._checks = checks
        self._checks_by_id = {check.spec.check_id: check for check in checks}
        self._dag = PreflightDag(
            checks,
            max_concurrency=max_concurrency,
            attested_dependencies=prefinal_ids,
        )
        self._context = CheckContext(
            {
                "candidate.sha": candidate_sha,
                "preflight.attestation.sha256": attestation.attestation_digest,
                "staging.mutation-epoch": mutation_epoch,
            }
        )

    def select_resume_evidence(
        self,
        candidates: Mapping[str, CheckExecution],
        *,
        now: datetime,
    ) -> dict[str, CheckExecution]:
        """Carry the newest compatible pass, treating protected apply as durable."""
        if (
            now.tzinfo is None
            or now.utcoffset() is None
            or (now >= self._attestation.expires_at and not self._post_apply_resume)
            or not set(candidates) <= set(self._checks_by_id)
        ):
            raise ValueError("final gate resume evidence authority is invalid")
        operations = self._operations()
        selected: dict[str, CheckExecution] = {}
        for check_id, execution in candidates.items():
            check = self._checks_by_id[check_id]
            compatible = (
                execution.passed
                and execution.check_id == check_id
                and execution.failure_code == check.spec.failure_code
                and execution.tier == check.spec.tier
                and execution.stage is check.spec.stage
                and execution.operation is operations[check_id]
                and execution.input_fingerprint == check.input_fingerprint(self._context)
                and execution.implementation_digest == check.implementation_digest
            )
            if check_id == "final.protected-apply":
                evidence = execution.evidence
                if not compatible or (
                    evidence.get("ready") is not True
                    or evidence.get("candidate-sha") != self._attestation.bindings.candidate_sha
                    or evidence.get("attestation-digest") != self._attestation.attestation_digest
                    or evidence.get("observed-epoch")
                    != self._attestation.bindings.staging_mutation_epoch + 1
                    or evidence.get("protected-mutation") is not True
                    or evidence.get("blockers") != {}
                ):
                    raise ValueError("durable protected apply evidence drifted")
                selected[check_id] = execution
            elif compatible and execution.expires_at > now:
                selected[check_id] = execution
        return selected

    def _operations(self) -> dict[str, CheckOperation]:
        return {
            check.spec.check_id: (
                CheckOperation.APPLY
                if check.spec.mutation_class is MutationClass.PROTECTED_STAGING
                else CheckOperation.VERIFY
            )
            for check in self._checks
        }

    def execute(
        self,
        *,
        now: datetime,
        clock: Callable[[], datetime] | None = None,
        prior_executions: Mapping[str, CheckExecution] | None = None,
        durable_prior_executions: frozenset[str] = frozenset(),
        on_execution: Callable[[CheckExecution], None] | None = None,
    ) -> AttestedFinalGateReport:
        """Apply once, then verify every non-mutating protected invariant."""
        live_clock = clock or (lambda: now)
        execution_time = live_clock()
        if (
            execution_time.tzinfo is None
            or execution_time.utcoffset() is None
            or (execution_time >= self._attestation.expires_at and not self._post_apply_resume)
        ):
            raise ValueError("final gate attestation expired before execution")
        prior = dict(prior_executions or {})
        has_durable_apply = durable_prior_executions == frozenset({"final.protected-apply"})
        if self._post_apply_resume != (has_durable_apply or self._protected_apply_recovery):
            raise ValueError("post-apply final gate authority is incomplete")
        if self._protected_apply_recovery and (
            durable_prior_executions or "final.protected-apply" in prior
        ):
            raise ValueError("protected apply recovery evidence is ambiguous")
        if durable_prior_executions:
            if (
                durable_prior_executions != frozenset({"final.protected-apply"})
                or not durable_prior_executions <= prior.keys()
            ):
                raise ValueError("durable final gate evidence identity is invalid")
            selected = self.select_resume_evidence(
                {check_id: prior[check_id] for check_id in durable_prior_executions},
                now=execution_time,
            )
            if set(selected) != set(durable_prior_executions):
                raise ValueError("durable final gate evidence drifted")
        executions = self._dag.run(
            self._context,
            operation=self._operations(),
            through_tier=4,
            now=live_clock,
            prior_executions=prior,
            freshness_exempt_prior_executions=durable_prior_executions,
            on_execution=on_execution,
        )
        return AttestedFinalGateReport(
            attestation_digest=self._attestation.attestation_digest,
            executions=executions,
        )


__all__ = ["AttestedFinalGateAuthority", "AttestedFinalGateReport"]
