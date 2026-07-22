"""Derive restore proof only from a complete exact-candidate rehearsal."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime

from loom_cli.rollout.operator.checkpoint_lease import (
    CriticalCheckpointEvidence,
    RestoreVerificationEvidence,
)
from loom_cli.rollout.preflight_contract import CheckContext
from loom_cli.rollout.preflight_pipeline import PreflightRehearsal
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS


def build_restore_verification_evidence(
    checkpoint: CriticalCheckpointEvidence,
    rehearsal: PreflightRehearsal,
    *,
    context: CheckContext,
    verified_at: datetime,
) -> RestoreVerificationEvidence:
    """Bind DB-clone success and cleanup to the exact critical checkpoint."""
    rehearsal.require_integrity()
    if not rehearsal.passed:
        raise ValueError("restore verification requires a passing rehearsal")
    if (
        context.bindings.get("checkpoint.evidence.sha256") != checkpoint.evidence_digest
        or context.bindings.get("staging.mutation-epoch") != checkpoint.mutation_epoch
        or context.bindings.get("environment") != checkpoint.environment
        or context.bindings.get("namespace") != checkpoint.namespace
    ):
        raise ValueError("restore rehearsal context does not match checkpoint authority")
    tier_three = {
        execution.check_id: execution for execution in rehearsal.executions if execution.tier == 3
    }
    if set(tier_three) != set(REHEARSAL_CHECK_IDS):
        raise ValueError("restore rehearsal coverage is incomplete")
    isolation_ids: set[str] = set()
    candidate_shas: set[str] = set()
    report_entries: list[dict[str, object]] = []
    for check_id in REHEARSAL_CHECK_IDS:
        execution = tier_three[check_id]
        evidence = execution.evidence
        blockers = evidence.get("blockers")
        isolation_id = evidence.get("isolation-id")
        candidate_sha = evidence.get("candidate-sha")
        evidence_digest = evidence.get("evidence-digest")
        journal_digest = evidence.get("journal-digest")
        if (
            not execution.passed
            or evidence.get("ready") is not True
            or evidence.get("observed-epoch") != checkpoint.mutation_epoch
            or evidence.get("protected-mutation") is not False
            or not isinstance(blockers, dict)
            or blockers
            or not isinstance(isolation_id, str)
            or not isinstance(candidate_sha, str)
            or not isinstance(evidence_digest, str)
            or not isinstance(journal_digest, str)
        ):
            raise ValueError("restore rehearsal evidence is incomplete")
        if check_id == "rehearsal.cleanup":
            if evidence.get("cleanup-verified") is not True:
                raise ValueError("restore rehearsal cleanup is incomplete")
        elif evidence.get("cleanup-verified") is not False:
            raise ValueError("restore rehearsal cleanup evidence is misplaced")
        isolation_ids.add(isolation_id)
        candidate_shas.add(candidate_sha)
        report_entries.append(
            {
                "check_id": check_id,
                "evidence_hash": execution.evidence_hash,
                "implementation_digest": execution.implementation_digest,
                "input_fingerprint": execution.input_fingerprint,
                "journal_digest": journal_digest,
            }
        )
    if len(isolation_ids) != 1 or len(candidate_shas) != 1:
        raise ValueError("restore rehearsal identity is inconsistent")
    report = {
        "candidate_sha": next(iter(candidate_shas)),
        "checkpoint_evidence_sha256": checkpoint.evidence_digest,
        "checks": report_entries,
        "isolation_id": next(iter(isolation_ids)),
        "rehearsal_digest": rehearsal.rehearsal_digest,
        "schema_version": 1,
    }
    report_sha256 = hashlib.sha256(
        json.dumps(report, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return RestoreVerificationEvidence(
        verification_id=f"restore-{report_sha256[:24]}",
        request_id=checkpoint.request_id,
        checkpoint_evidence_sha256=checkpoint.evidence_digest,
        manifest_sha256=checkpoint.manifest_sha256,
        db_snapshot_identity=checkpoint.db_snapshot_identity,
        object_inventory_root=checkpoint.object_inventory_root,
        mutation_epoch=checkpoint.mutation_epoch,
        schema_revision=checkpoint.schema_revision,
        environment=checkpoint.environment,
        namespace=checkpoint.namespace,
        report_sha256=report_sha256,
        verified_at=verified_at,
    )


__all__ = ["build_restore_verification_evidence"]
