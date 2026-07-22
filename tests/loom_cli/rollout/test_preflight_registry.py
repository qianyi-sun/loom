from __future__ import annotations

from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_coverage import load_coverage_manifest
from loom_cli.rollout.preflight_registry import PreflightRegistry


def _check(entry) -> RegisteredCheck:
    spec = CheckSpec(
        check_id=entry.check_id,
        failure_code=entry.failure_code,
        tier=entry.tier,
        stage=entry.stage,
        dependencies=entry.dependencies,
        mutation_class=entry.mutation_class,
        input_keys=(f"{entry.check_id}.input",),
        evidence_schema=(EvidenceField("ready", "boolean"),),
        timeout_seconds=5,
        freshness_ttl_seconds=60,
        remediation=f"restore {entry.check_id}",
        secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        final_only_justification=entry.final_only_justification,
    )
    return RegisteredCheck(
        spec=spec,
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"ready": True},
            )
        },
    )


def test_tier_zero_registry_is_exact_order_independent_and_digest_bound() -> None:
    entries = tuple(entry for entry in load_coverage_manifest().checks if entry.tier == 0)

    first = PreflightRegistry.build(
        tuple(_check(entry) for entry in reversed(entries)), through_tier=0
    )
    second = PreflightRegistry.build(tuple(_check(entry) for entry in entries), through_tier=0)

    assert first.registry_digest == second.registry_digest
    assert first.coverage_digest == second.coverage_digest
    assert tuple(check.spec.check_id for check in first.checks) == tuple(
        sorted(entry.check_id for entry in entries)
    )
    context = CheckContext({f"{entry.check_id}.input": "bound" for entry in entries})
    assert all(result.passed for result in first.dag().run(context, through_tier=0))


def test_contract_digest_changes_for_timeout_or_evidence_drift() -> None:
    entry = load_coverage_manifest().checks[0]
    original = _check(entry)
    changed = RegisteredCheck(
        spec=CheckSpec(
            check_id=original.spec.check_id,
            failure_code=original.spec.failure_code,
            tier=original.spec.tier,
            stage=StageCapability.STATIC,
            dependencies=original.spec.dependencies,
            mutation_class=MutationClass.NONE,
            input_keys=original.spec.input_keys,
            evidence_schema=(EvidenceField("bound", "string"),),
            timeout_seconds=6,
            freshness_ttl_seconds=original.spec.freshness_ttl_seconds,
            remediation=original.spec.remediation,
            secret_redaction_policy=original.spec.secret_redaction_policy,
        ),
        implementation_version="test-v1",
        operations=original.operations,
    )

    assert original.spec.contract_digest != changed.spec.contract_digest
    assert original.implementation_digest != changed.implementation_digest
