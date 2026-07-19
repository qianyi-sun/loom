from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import (
    AttestationBindings,
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    RegisteredCheck,
    SecretRedactionPolicy,
)
from loom_cli.rollout.preflight_coverage import load_coverage_manifest
from loom_cli.rollout.preflight_pipeline import PreflightAssessment, PreflightPipeline
from loom_cli.rollout.preflight_registry import PreflightRegistry


def _bindings(*, epoch: int = 7) -> AttestationBindings:
    digest = "1" * 64
    return AttestationBindings(
        candidate_sha="2" * 40,
        candidate_tree="3" * 40,
        image_digests={"api": f"sha256:{'4' * 64}"},
        runner_source_sha="2" * 40,
        runner_source_tree="3" * 40,
        runner_install_hash=digest,
        runner_config_hash="5" * 64,
        staging_mutation_epoch=epoch,
        backup_lease_id="lease-7",
        backup_lease_digest="c" * 64,
        backup_manifest_sha256="d" * 64,
        backup_component_set_digest="e" * 64,
        db_snapshot_identity="lsn-0-16b6a80",
        schema_revision="0066",
        object_inventory_root="f" * 64,
        migration_plan_digest="6" * 64,
        environment="staging",
        namespace="loom-staging",
        route="staging.loom.internal",
        secret_metadata_fingerprints={"admin": f"sha256:{'7' * 64}"},
        gb10_inventory_digest="8" * 64,
        gb10_boot_ids={"gb10-1": "boot-1"},
        gb10_mount_digest="9" * 64,
        gb10_unit_digest="a" * 64,
        browser_image_digest=f"sha256:{'b' * 64}",
        browser_report_schema="v3",
    )


def _registry(*, failed_check: str | None = None) -> PreflightRegistry:
    manifest = load_coverage_manifest()
    checks: list[RegisteredCheck] = []
    for entry in manifest.checks:
        if entry.tier > 3:
            continue

        def probe(_context: CheckContext, *, check_id: str = entry.check_id) -> CheckProbe:
            evidence: dict[str, object] = {}
            for field in (EvidenceField("result", "string"),):
                evidence[field.name] = {
                    "string": "ok",
                    "integer": 1,
                    "number": 1,
                    "boolean": True,
                    "sha256": "c" * 64,
                    "string-map": {"value": "ok"},
                }[field.value_type]
            return CheckProbe(passed=check_id != failed_check, evidence=evidence)  # type: ignore[arg-type]

        checks.append(
            RegisteredCheck(
                spec=CheckSpec(
                    check_id=entry.check_id,
                    failure_code=entry.failure_code,
                    tier=entry.tier,
                    stage=entry.stage,
                    dependencies=entry.dependencies,
                    mutation_class=entry.mutation_class,
                    input_keys=(f"input.{entry.check_id}",),
                    evidence_schema=(EvidenceField("result", "string"),),
                    timeout_seconds=10,
                    freshness_ttl_seconds=300,
                    remediation="restore the exact declared test preflight invariant",
                    secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
                    final_only_justification=entry.final_only_justification,
                ),
                implementation_version="v1",
                operations={CheckOperation.PROBE: probe},
            )
        )
    return PreflightRegistry.build(checks, through_tier=3)


def _context(registry: PreflightRegistry) -> CheckContext:
    bindings: dict[str, object] = {}
    for check in registry.checks:
        for key in check.spec.input_keys:
            bindings.setdefault(key, f"value-{key}")
    for key in tuple(bindings):
        if "secret" in key:
            bindings[key] = {"admin": f"sha256:{'d' * 64}"}
    return CheckContext(bindings)  # type: ignore[arg-type]


def test_pipeline_reports_every_independent_blocker(tmp_path: Path) -> None:
    registry = _registry(failed_check="candidate.identity")
    pipeline = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "state"),
        now=lambda: datetime(2026, 7, 19, 10, tzinfo=UTC),
    )
    result = pipeline.authorize(context=_context(registry), bindings=_bindings())
    assert not result.passed
    assert result.attestation is None
    assert "candidate.identity" in {blocker.check_id for blocker in result.blockers}
    assert any(blocker.outcome == "blocked" for blocker in result.blockers)


def test_pipeline_publishes_and_reuses_exact_attestation(tmp_path: Path) -> None:
    registry = _registry()
    pipeline = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "state"),
        now=lambda: datetime(2026, 7, 19, 10, tzinfo=UTC),
    )
    first = pipeline.authorize(
        context=_context(registry),
        binding_factory=lambda _executions: _bindings(),
    )
    assert first.passed and first.attestation is not None and not first.reused
    assessment = pipeline.assess(context=_context(registry))
    second = pipeline.authorize(
        context=_context(registry),
        bindings=_bindings(),
        reusable_attestation_digest=first.attestation.attestation_digest,
        assessment=assessment,
    )
    assert second.passed and second.reused
    assert second.executions == ()


def test_pipeline_invalidates_epoch_drift_and_reruns(tmp_path: Path) -> None:
    registry = _registry()
    pipeline = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "state"),
        now=lambda: datetime(2026, 7, 19, 10, tzinfo=UTC),
    )
    first = pipeline.authorize(context=_context(registry), bindings=_bindings())
    assert first.attestation is not None
    drifted = pipeline.authorize(
        context=_context(registry),
        bindings=_bindings(epoch=8),
        reusable_attestation_digest=first.attestation.attestation_digest,
    )
    assert drifted.passed and not drifted.reused
    assert drifted.executions


def test_tier_two_assessment_is_rechecked_before_tier_three_attestation(
    tmp_path: Path,
) -> None:
    registry = _registry()
    pipeline = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "state"),
        now=lambda: datetime(2026, 7, 19, 10, tzinfo=UTC),
    )
    context = _context(registry)

    assessment = pipeline.assess(context=context)
    result = pipeline.authorize(
        context=context,
        bindings=_bindings(),
        assessment=assessment,
    )

    assert assessment.passed
    assert assessment.through_tier == 2
    assert {item.tier for item in assessment.executions} == {0, 1, 2}
    assert result.passed and result.attestation is not None


def test_rehearsal_and_attestation_are_separate_restore_authority_steps(
    tmp_path: Path,
) -> None:
    registry = _registry()
    store = PreflightAttestationStore(tmp_path / "state")
    pipeline = PreflightPipeline(
        registry=registry,
        store=store,
        now=lambda: datetime(2026, 7, 19, 10, tzinfo=UTC),
    )
    context = _context(registry)
    assessment = pipeline.assess(context=context)

    rehearsal = pipeline.rehearse(context=context, assessment=assessment)

    assert rehearsal.passed
    assert type(rehearsal).from_record(rehearsal.to_record()) == rehearsal
    assert {execution.tier for execution in rehearsal.executions} == {0, 1, 2, 3}
    assert len(rehearsal.rehearsal_digest) == 64
    assert not store.root.exists()

    attestation = pipeline.attest(rehearsal=rehearsal, bindings=_bindings())

    assert store.read(attestation.attestation_digest) == attestation

    with pytest.raises(ValueError, match="rehearsal authority"):
        pipeline.attest(
            rehearsal=replace(rehearsal, rehearsal_digest="0" * 64),
            bindings=_bindings(),
        )

    record = rehearsal.to_record()
    record["rehearsal_digest"] = "0" * 64
    with pytest.raises(ValueError, match="record digest"):
        type(rehearsal).from_record(record)


def test_tier_two_assessment_rejects_input_drift_before_attestation(tmp_path: Path) -> None:
    registry = _registry()
    pipeline = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "state"),
        now=lambda: datetime(2026, 7, 19, 10, tzinfo=UTC),
    )
    context = _context(registry)
    assessment = pipeline.assess(context=context)
    drifted = dict(context.bindings)
    drifted[registry.checks[0].spec.input_keys[0]] = "drifted"

    with pytest.raises(ValueError, match="assessment evidence drifted"):
        pipeline.authorize(
            context=CheckContext(drifted),
            bindings=_bindings(),
            assessment=assessment,
        )


def test_assessment_record_round_trips_and_rejects_evidence_tampering(tmp_path: Path) -> None:
    registry = _registry()
    assessment = PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "state"),
        now=lambda: datetime(2026, 7, 19, 10, tzinfo=UTC),
    ).assess(context=_context(registry))

    record = assessment.to_record()
    assert PreflightAssessment.from_record(record) == assessment

    executions = record["executions"]
    assert isinstance(executions, list)
    first = executions[0]
    assert isinstance(first, dict)
    evidence = first["evidence"]
    assert isinstance(evidence, dict)
    evidence["result"] = "tampered"
    with pytest.raises(ValueError, match="evidence hash"):
        PreflightAssessment.from_record(record)
