"""Fail-closed drift admission immediately before protected final gates."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from loom_cli.rollout.operator.model import CandidateBinding
from loom_cli.rollout.preflight_authority import CandidatePreflightPlan
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    PreflightAttestation,
    PreflightDag,
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

_DRIFT_EVIDENCE_CHECKS = frozenset(
    {
        "candidate.identity",
        "runner.install",
        "credentials.metadata",
        "gb10.shared-mount",
        "gb10.host-readiness",
    }
)


@dataclass(frozen=True, slots=True)
class FinalAttestationAdmission:
    attestation: PreflightAttestation
    tier0_executions: tuple[CheckExecution, ...]
    tier2_executions: tuple[CheckExecution, ...]

    def __post_init__(self) -> None:
        tier2_by_id = {execution.check_id: execution for execution in self.tier2_executions}
        if (
            not self.tier0_executions
            or any(result.tier != 0 or not result.passed for result in self.tier0_executions)
            or len(self.tier2_executions) != len(_BASELINE_CHECKS)
            or set(tier2_by_id) != _BASELINE_CHECKS
            or any(result.tier != 2 or not result.passed for result in self.tier2_executions)
        ):
            raise ValueError("final attestation admission is incomplete")


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
        raise ValueError("final admission clock must be timezone-aware")
    bindings = attestation.bindings
    if (
        now >= attestation.expires_at
        or plan.candidate != candidate
        or bindings.candidate_sha != candidate.resolved_sha
        or bindings.candidate_tree != candidate.resolved_tree
        or bindings.staging_mutation_epoch != current_mutation_epoch
        or plan.context.bindings.get("staging.mutation-epoch") != current_mutation_epoch
        or plan.registry.registry_digest != attestation.registry_digest
        or plan.registry.coverage_digest != attestation.coverage_digest
        or plan.registry.implementation_digests != attestation.check_implementation_digests
    ):
        raise ValueError("final admission attestation identity drifted")
    context_expected = {
        "candidate.sha": bindings.candidate_sha,
        "candidate.tree": bindings.candidate_tree,
        "runner.config.sha256": bindings.runner_config_hash,
        "environment": bindings.environment,
        "namespace": bindings.namespace,
        "route": bindings.route,
    }
    if any(plan.context.bindings.get(key) != value for key, value in context_expected.items()):
        raise ValueError("final admission context binding drifted")

    drift_checks = tuple(check for check in plan.registry.checks if check.spec.tier in {0, 2})
    executions = PreflightDag(drift_checks, max_concurrency=max_concurrency).run(
        plan.context,
        through_tier=2,
        now=lambda: now,
    )
    by_id = {execution.check_id: execution for execution in executions}
    tier0 = tuple(execution for execution in executions if execution.tier == 0)
    tier2 = tuple(execution for execution in executions if execution.tier == 2)
    if (
        not _DRIFT_EVIDENCE_CHECKS <= by_id.keys()
        or set(execution.check_id for execution in tier2) != _BASELINE_CHECKS
        or any(not execution.passed for execution in executions)
        or any(
            execution.implementation_digest
            != attestation.check_implementation_digests[execution.check_id]
            for execution in executions
        )
    ):
        raise ValueError("final admission Tier 0 drift check failed")

    def evidence(check_id: str, field: str) -> object:
        try:
            return by_id[check_id].evidence[field]
        except KeyError as exc:
            raise ValueError("final admission drift evidence is incomplete") from exc

    def string_map(check_id: str, field: str) -> dict[str, str]:
        value = evidence(check_id, field)
        if not isinstance(value, Mapping) or any(
            not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
        ):
            raise ValueError("final admission drift evidence is incomplete")
        return dict(value)

    current_secret_metadata = string_map("credentials.metadata", "metadata-fingerprints")
    normalized_secret_metadata = {
        str(key): (str(value) if str(value).startswith("sha256:") else f"sha256:{value}")
        for key, value in current_secret_metadata.items()
    }
    exact_evidence = (
        evidence("candidate.identity", "resolved-sha") == bindings.candidate_sha,
        evidence("candidate.identity", "resolved-tree") == bindings.candidate_tree,
        evidence("runner.install", "attestation-digest") == bindings.runner_install_hash,
        normalized_secret_metadata == dict(bindings.secret_metadata_fingerprints),
        evidence("gb10.shared-mount", "mount-digest") == bindings.gb10_mount_digest,
        evidence("gb10.host-readiness", "inventory-digest") == bindings.gb10_inventory_digest,
        string_map("gb10.host-readiness", "boot-ids") == dict(bindings.gb10_boot_ids),
    )
    if not all(exact_evidence):
        raise ValueError("final admission drift-sensitive evidence changed")
    for execution in tier2:
        current_evidence = execution.evidence
        if (
            current_evidence.get("ready") is not True
            or current_evidence.get("observed-epoch") != current_mutation_epoch
            or current_evidence.get("readonly-principal") in {None, ""}
            or current_evidence.get("resource-digest") in {None, ""}
            or current_evidence.get("blockers") != {}
            or execution.evidence_hash != attestation.evidence_hashes.get(execution.check_id)
        ):
            raise ValueError("final admission Tier 2 baseline changed")
    return FinalAttestationAdmission(attestation, tier0, tier2)


__all__ = ["FinalAttestationAdmission", "validate_final_attestation"]
