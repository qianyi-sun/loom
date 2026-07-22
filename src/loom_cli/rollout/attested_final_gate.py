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
        max_concurrency: int = 4,
    ) -> None:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("final gate clock must be timezone-aware")
        if (
            attestation.attestation_digest != attestation.attestation_digest.strip()
            or attestation.bindings.candidate_sha != candidate_sha
            or attestation.bindings.staging_mutation_epoch != mutation_epoch
            or now >= attestation.expires_at
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
        self._checks = checks
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

    def execute(
        self,
        *,
        now: datetime,
        prior_executions: Mapping[str, CheckExecution] | None = None,
        on_execution: Callable[[CheckExecution], None] | None = None,
    ) -> AttestedFinalGateReport:
        """Apply once, then verify every non-mutating protected invariant."""
        if now.tzinfo is None or now.utcoffset() is None or now >= self._attestation.expires_at:
            raise ValueError("final gate attestation expired before execution")
        operations = {
            check.spec.check_id: (
                CheckOperation.APPLY
                if check.spec.mutation_class is MutationClass.PROTECTED_STAGING
                else CheckOperation.VERIFY
            )
            for check in self._checks
        }
        executions = self._dag.run(
            self._context,
            operation=operations,
            through_tier=4,
            now=lambda: now,
            prior_executions=prior_executions,
            on_execution=on_execution,
        )
        return AttestedFinalGateReport(
            attestation_digest=self._attestation.attestation_digest,
            executions=executions,
        )


__all__ = ["AttestedFinalGateAuthority", "AttestedFinalGateReport"]
