from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType

import pytest

from loom_cli.rollout.operator.protected_apply_baseline import ProtectedApplyBaseline
from loom_cli.rollout.preflight_contract import (
    CheckExecution,
    CheckOperation,
    CheckOutcome,
    StageCapability,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import _attestation

NOW = datetime(2026, 7, 19, 21, tzinfo=UTC)
CHECK_IDS = (
    "staging.health",
    "staging.auth",
    "staging.catalog-task",
    "staging.storage-db",
    "staging.network",
    "staging.release-baseline",
)


def _baseline_executions(
    *, epoch: int = 7, resource_offset: int = 0
) -> tuple[CheckExecution, ...]:
    results = []
    for ordinal, check_id in enumerate(CHECK_IDS):
        evidence = {
            "ready": True,
            "readonly-principal": "loom-staging-preflight-readonly",
            "observed-epoch": epoch,
            "resource-digest": f"{ordinal + 1 + resource_offset:064x}",
            "blockers": {},
        }
        evidence_hash = hashlib.sha256(
            json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        results.append(
            CheckExecution(
                check_id=check_id,
                failure_code=f"{check_id}.failed",
                tier=2,
                stage=StageCapability.BASELINE_LIVE_READONLY,
                operation=CheckOperation.PROBE,
                outcome=CheckOutcome.PASS,
                input_fingerprint=f"{ordinal + 11:064x}",
                implementation_digest=f"{ordinal + 21:064x}",
                evidence=MappingProxyType(evidence),
                evidence_hash=evidence_hash,
                started_at=NOW,
                finished_at=NOW,
                expires_at=NOW + timedelta(minutes=10),
                remediation=None,
            )
        )
    return tuple(results)


def _baseline(attestation=None) -> ProtectedApplyBaseline:
    original = attestation or _attestation()
    executions = _baseline_executions()
    enriched = type(original).issue(
        bindings=original.bindings,
        executions=executions,
        issued_at=NOW,
        registry_digest=original.registry_digest,
        coverage_digest=original.coverage_digest,
    )
    return ProtectedApplyBaseline.from_executions(enriched, executions)


def test_protected_apply_baseline_binds_all_readonly_tier2_evidence() -> None:
    baseline = _baseline()

    assert ProtectedApplyBaseline.from_dict(baseline.to_dict()) == baseline
    assert set(baseline.resource_digests) == set(CHECK_IDS)
    assert baseline.mutation_epoch == 7


def test_protected_apply_baseline_ignores_named_tier_zero_live_readonly_checks() -> None:
    original = _attestation()
    executions = _baseline_executions()
    tier0_predecessor = replace(
        executions[0],
        check_id="external-supervisor.predecessor",
        failure_code="external-supervisor.predecessor.drift",
        tier=0,
    )
    all_executions = (*executions, tier0_predecessor)
    enriched = type(original).issue(
        bindings=original.bindings,
        executions=all_executions,
        issued_at=NOW,
        registry_digest=original.registry_digest,
        coverage_digest=original.coverage_digest,
    )

    baseline = ProtectedApplyBaseline.from_executions(enriched, all_executions)

    assert set(baseline.resource_digests) == set(CHECK_IDS)


def test_protected_apply_baseline_rejects_missing_or_epoch_drifted_evidence() -> None:
    original = _attestation()
    executions = _baseline_executions()
    attestation = type(original).issue(
        bindings=original.bindings,
        executions=executions,
        issued_at=NOW,
        registry_digest=original.registry_digest,
        coverage_digest=original.coverage_digest,
    )

    with pytest.raises(ValueError, match="coverage is incomplete"):
        ProtectedApplyBaseline.from_executions(attestation, executions[:-1])
    with pytest.raises(ValueError, match="coverage is incomplete"):
        ProtectedApplyBaseline.from_executions(attestation, (*executions, executions[0]))
    with pytest.raises(ValueError, match="evidence drifted"):
        ProtectedApplyBaseline.from_executions(attestation, _baseline_executions(epoch=8))


def test_protected_apply_baseline_tolerates_live_resource_digest_drift() -> None:
    # The Tier 2 baseline ``resource-digest`` is a live hash of the probed
    # staging resource; it legitimately shifts between the restore rehearsal
    # (which froze the attestation) and this protected apply as ordinary traffic
    # mutates the serving system. A drifted resource-digest (and its derived
    # evidence_hash) must NOT be treated as tampering so long as the baseline is
    # still healthy at the current epoch (see #986/#988/#990).
    original = _attestation()
    attestation = type(original).issue(
        bindings=original.bindings,
        executions=_baseline_executions(),
        issued_at=NOW,
        registry_digest=original.registry_digest,
        coverage_digest=original.coverage_digest,
    )

    drifted = _baseline_executions(resource_offset=1000)
    baseline = ProtectedApplyBaseline.from_executions(attestation, drifted)

    assert baseline.mutation_epoch == attestation.bindings.staging_mutation_epoch
    # The recorded resource-digests are the FRESH probe values, not the frozen
    # attestation snapshot.
    assert baseline.resource_digests == {
        execution.check_id: execution.evidence["resource-digest"] for execution in drifted
    }
