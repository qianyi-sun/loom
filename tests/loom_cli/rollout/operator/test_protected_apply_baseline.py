from __future__ import annotations

import hashlib
import json
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


def _baseline_executions(*, epoch: int = 7) -> tuple[CheckExecution, ...]:
    results = []
    for ordinal, check_id in enumerate(CHECK_IDS):
        evidence = {
            "ready": True,
            "readonly-principal": "loom-staging-preflight-readonly",
            "observed-epoch": epoch,
            "resource-digest": f"{ordinal + 1:064x}",
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
