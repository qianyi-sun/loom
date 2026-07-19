from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

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
from loom_cli.rollout.preflight_pipeline import PreflightPipeline
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
        db_snapshot_identity="lsn-0-16b6a80",
        schema_revision="0066",
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
    first = pipeline.authorize(context=_context(registry), bindings=_bindings())
    assert first.passed and first.attestation is not None and not first.reused
    second = pipeline.authorize(
        context=_context(registry),
        bindings=_bindings(),
        reusable_attestation_digest=first.attestation.attestation_digest,
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
