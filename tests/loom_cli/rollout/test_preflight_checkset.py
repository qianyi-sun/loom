from __future__ import annotations

from dataclasses import replace

import pytest

from loom_cli.rollout.preflight_checkset import RolloutCheckSet
from loom_cli.rollout.preflight_contract import (
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    RegisteredCheck,
    SecretRedactionPolicy,
)
from loom_cli.rollout.preflight_coverage import CoverageEntry, load_coverage_manifest


def _check(entry: CoverageEntry) -> RegisteredCheck:
    return RegisteredCheck(
        spec=CheckSpec(
            check_id=entry.check_id,
            failure_code=entry.failure_code,
            tier=entry.tier,
            stage=entry.stage,
            dependencies=entry.dependencies,
            mutation_class=entry.mutation_class,
            input_keys=("runner.config.sha256",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=60,
            remediation=f"restore {entry.check_id}",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
            final_only_justification=entry.final_only_justification,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"ready": True},
            )
        },
    )


def _groups() -> dict[int, tuple[RegisteredCheck, ...]]:
    checks = tuple(_check(entry) for entry in load_coverage_manifest().checks)
    return {tier: tuple(check for check in checks if check.spec.tier == tier) for tier in range(5)}


def test_assembles_exact_tier0_through_final_coverage() -> None:
    groups = _groups()
    result = RolloutCheckSet.assemble(
        tier0=groups[0],
        tier1=groups[1],
        tier2=groups[2],
        tier3=groups[3],
        final=groups[4],
    )
    assert result.preflight_registry.through_tier == 3
    assert len(result.preflight_registry.checks) + len(result.final_checks) == sum(
        len(group) for group in groups.values()
    )


def test_rejects_missing_or_misclassified_check() -> None:
    groups = _groups()
    with pytest.raises(ValueError, match=r"missing=.*candidate.identity"):
        RolloutCheckSet.assemble(
            tier0=groups[0][1:],
            tier1=groups[1],
            tier2=groups[2],
            tier3=groups[3],
            final=groups[4],
        )
    with pytest.raises(ValueError, match="misclassified"):
        RolloutCheckSet.assemble(
            tier0=(replace(groups[0][0], spec=replace(groups[0][0].spec, tier=1)),),
            tier1=groups[1],
            tier2=groups[2],
            tier3=groups[3],
            final=groups[4],
        )
