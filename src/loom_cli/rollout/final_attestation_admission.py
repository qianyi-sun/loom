"""Fail-closed drift admission immediately before protected final gates."""

from __future__ import annotations

import hashlib
import json
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

_POST_APPLY_DRIFT_EVIDENCE_CHECKS = frozenset(
    {
        "candidate.identity",
        "runner.install",
        "credentials.metadata",
        "gb10.shared-mount",
        "gb10.candidate-source",
        "gb10.host-readiness",
    }
)
_PRE_APPLY_DRIFT_EVIDENCE_CHECKS = _POST_APPLY_DRIFT_EVIDENCE_CHECKS | {
    "external-supervisor.predecessor"
}


@dataclass(frozen=True, slots=True)
class FinalAttestationAdmission:
    attestation: PreflightAttestation
    tier0_executions: tuple[CheckExecution, ...]
    tier2_executions: tuple[CheckExecution, ...]
    preflight_plan: CandidatePreflightPlan | None = None

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
        not _PRE_APPLY_DRIFT_EVIDENCE_CHECKS <= by_id.keys()
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
        evidence("gb10.candidate-source", "source-digest") == bindings.gb10_unit_digest,
        # gb10.host-readiness inventory-digest is intentionally NOT byte-matched:
        # it folds in each host's node-agent service/timer runtime state
        # (ActiveState/SubState/Result), which cycles as the readiness timer
        # fires and the oneshot agent runs between the restore rehearsal and this
        # re-check, so a fixed attestation snapshot can never re-match a running
        # fleet. Meaningful gb10 drift is still gated -- the check must PASS
        # (fleet reachable + ready) and boot-ids must match (no host reboot or
        # host-set change).
        string_map("gb10.host-readiness", "boot-ids") == dict(bindings.gb10_boot_ids),
        evidence("external-supervisor.predecessor", "authority-kind")
        == bindings.supervisor_predecessor_kind,
        evidence("external-supervisor.predecessor", "authority-digest")
        == bindings.supervisor_predecessor_digest,
        evidence("external-supervisor.predecessor", "pointer-digest")
        == bindings.supervisor_predecessor_pointer_digest,
        string_map("external-supervisor.predecessor", "unit-digests")
        == dict(bindings.supervisor_predecessor_unit_sha256),
        evidence("external-supervisor.predecessor", "unit-set-digest")
        == bindings.supervisor_predecessor_unit_set_digest,
        evidence("external-supervisor.predecessor", "live-evidence-digest")
        == bindings.supervisor_predecessor_live_evidence_digest,
        evidence("external-supervisor.predecessor", "pending-transition-digest")
        == bindings.supervisor_predecessor_pending_transition_digest,
        evidence("external-supervisor.predecessor", "transition-clear") is True,
        evidence("external-supervisor.predecessor", "runtime-ready") is True,
    )
    # The drift-sensitive external-supervisor.predecessor *authority* and
    # transition fields are re-checked individually above. We deliberately do
    # NOT additionally require the check's whole evidence_hash to byte-match the
    # attestation, because that evidence also folds in ``pool-identity-digest`` --
    # a live count of external-supervisor worker rows per pool (legacy `gb10-arm64`
    # vs target `gb10`). Ordinary worker registration between the restore
    # rehearsal (which mints the attestation) and this final admission shifts that
    # count, so a fixed attestation snapshot of live operational data can never
    # re-match in a running system; gating on it fails every deploy whose window
    # sees any worker activity while adding no authority-drift safety the fields
    # above miss. (Concurrent legacy->target migration remains coordinated by the
    # rollout itself and by the transition-clear/pending-transition digests.)
    if not all(exact_evidence):
        raise ValueError("final admission drift-sensitive evidence changed")
    for execution in tier2:
        current_evidence = execution.evidence
        # Each Tier 2 baseline is re-verified HEALTHY at final admission: ready,
        # observed at the current mutation epoch, bound to the read-only
        # principal, carrying a resource-digest, and unblocked. We intentionally
        # do NOT additionally require the check's whole evidence_hash to byte-match
        # the attestation: the evidence carries ``resource-digest`` -- a live hash
        # of the probed staging resource (auth/release-baseline/storage-db) that
        # shifts with ordinary staging traffic between the restore rehearsal and
        # this re-check, so a fixed attestation snapshot can never re-match a
        # serving system. The health checks above (not the frozen resource
        # digest) are the meaningful pre-apply baseline gate.
        if (
            current_evidence.get("ready") is not True
            or current_evidence.get("observed-epoch") != current_mutation_epoch
            or current_evidence.get("readonly-principal") in {None, ""}
            or current_evidence.get("resource-digest") in {None, ""}
            or current_evidence.get("blockers") != {}
        ):
            raise ValueError("final admission Tier 2 baseline changed")
    return FinalAttestationAdmission(attestation, tier0, tier2, plan)


def validate_post_apply_attestation_drift(
    *,
    admission: FinalAttestationAdmission,
    plan: CandidatePreflightPlan,
    current_mutation_epoch: int,
    now: datetime,
    max_concurrency: int = 8,
) -> PostApplyDriftEvidence:
    """Rerun only epoch-independent exact inputs after protected apply."""
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("post-apply drift clock must be timezone-aware")
    attestation = admission.attestation
    bindings = attestation.bindings
    if (
        now >= attestation.expires_at
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
        "staging.mutation-epoch": bindings.staging_mutation_epoch,
    }
    if any(plan.context.bindings.get(key) != value for key, value in context_expected.items()):
        raise ValueError("post-apply context binding drifted")
    available = {
        check.spec.check_id: check for check in plan.registry.checks if check.spec.tier == 0
    }
    selected_ids = set(_POST_APPLY_DRIFT_EVIDENCE_CHECKS)
    pending = list(selected_ids)
    while pending:
        check_id = pending.pop()
        try:
            dependencies = available[check_id].spec.dependencies
        except KeyError as exc:
            raise ValueError("post-apply drift check implementation is missing") from exc
        for dependency in dependencies:
            if dependency not in selected_ids:
                selected_ids.add(dependency)
                pending.append(dependency)
    checks = tuple(available[check_id] for check_id in sorted(selected_ids))
    executions = PreflightDag(checks, max_concurrency=max_concurrency).run(
        plan.context,
        through_tier=0,
        now=lambda: now,
    )
    by_id = {execution.check_id: execution for execution in executions}
    if not _POST_APPLY_DRIFT_EVIDENCE_CHECKS <= by_id.keys() or any(
        not execution.passed for execution in executions
    ):
        raise ValueError("post-apply drift evidence is incomplete")
    selected = tuple(
        sorted(
            (by_id[check_id] for check_id in _POST_APPLY_DRIFT_EVIDENCE_CHECKS),
            key=lambda execution: execution.check_id,
        )
    )
    if any(
        execution.expires_at <= now
        or execution.implementation_digest
        != attestation.check_implementation_digests.get(execution.check_id)
        for execution in executions
    ):
        raise ValueError("post-apply drift evidence expired or changed")

    def evidence(check_id: str, field: str) -> object:
        try:
            return by_id[check_id].evidence[field]
        except KeyError as exc:
            raise ValueError("post-apply drift evidence is incomplete") from exc

    def string_map(check_id: str, field: str) -> dict[str, str]:
        value = evidence(check_id, field)
        if not isinstance(value, Mapping) or any(
            not isinstance(key, str) or not isinstance(item, str) for key, item in value.items()
        ):
            raise ValueError("post-apply drift evidence is incomplete")
        return dict(value)

    current_secret_metadata = string_map("credentials.metadata", "metadata-fingerprints")
    normalized_secret_metadata = {
        str(key): (str(value) if str(value).startswith("sha256:") else f"sha256:{value}")
        for key, value in current_secret_metadata.items()
    }
    exact = (
        evidence("candidate.identity", "resolved-sha") == bindings.candidate_sha,
        evidence("candidate.identity", "resolved-tree") == bindings.candidate_tree,
        evidence("runner.install", "attestation-digest") == bindings.runner_install_hash,
        normalized_secret_metadata == dict(bindings.secret_metadata_fingerprints),
        evidence("gb10.shared-mount", "mount-digest") == bindings.gb10_mount_digest,
        evidence("gb10.candidate-source", "source-digest") == bindings.gb10_unit_digest,
        # gb10.host-readiness inventory-digest is intentionally NOT byte-matched:
        # it folds in each host's node-agent service/timer runtime state
        # (ActiveState/SubState/Result), which cycles as the readiness timer
        # fires and the oneshot agent runs between the restore rehearsal and this
        # re-check, so a fixed attestation snapshot can never re-match a running
        # fleet. Meaningful gb10 drift is still gated -- the check must PASS
        # (fleet reachable + ready) and boot-ids must match (no host reboot or
        # host-set change).
        string_map("gb10.host-readiness", "boot-ids") == dict(bindings.gb10_boot_ids),
    )
    if not all(exact):
        raise ValueError("post-apply drift-sensitive evidence changed")
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


__all__ = [
    "FinalAttestationAdmission",
    "PostApplyDriftEvidence",
    "validate_final_attestation",
    "validate_post_apply_attestation_drift",
]
