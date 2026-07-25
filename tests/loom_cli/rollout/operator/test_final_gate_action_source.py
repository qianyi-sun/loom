from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

import loom_cli.rollout.operator.final_gate_action_source as action_source_module
from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    PROTECTED_MUTATION_CHECK_IDS,
)
from loom_cli.rollout.operator.backup_lease import BackupLease
from loom_cli.rollout.operator.final_gate_action_source import FinalGateActionSource
from loom_cli.rollout.operator.final_gate_plan import FinalGatePlanStore
from loom_cli.rollout.preflight_artifact_store import PreflightArtifactStore
from loom_cli.rollout.preflight_contract import (
    CheckContext,
    CheckOperation,
    CheckProbe,
    CheckSpec,
    EvidenceField,
    MutationClass,
    PreflightAttestation,
    PreflightDag,
    RegisteredCheck,
    SecretRedactionPolicy,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import PreflightRehearsal
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _attestation as binding_attestation,
)
from tests.loom_cli.rollout.operator.test_final_gate_plan import (
    _envelope,
    _lease,
    _predecessor_evidence,
    _systemd_evidence,
)
from tests.loom_cli.rollout.operator.test_final_gate_runner import _admission
from tests.loom_cli.rollout.operator.test_protected_apply_baseline import (
    _baseline_executions,
)
from tests.loom_cli.rollout.test_preflight_artifact_store import (
    _images,
    _manifests,
    _migration,
    _production_defaults,
)

NOW = datetime(2026, 7, 19, 21, tzinfo=UTC)


@dataclass
class _Store:
    rehearsal: PreflightRehearsal

    def read_preflight_rehearsal(self, _request_id: str) -> PreflightRehearsal:
        return self.rehearsal

    def read_backup_lease(self, _digest: str) -> BackupLease:
        return _lease()


def _execution(publication, *, image_digest: str | None = None):
    check = RegisteredCheck(
        spec=CheckSpec(
            check_id="artifacts.publish",
            failure_code="artifacts.publish.failed",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(
                EvidenceField("bundle-digest", "sha256"),
                EvidenceField("image-artifact-digest", "sha256"),
                EvidenceField("manifest-artifact-digest", "sha256"),
                EvidenceField("rendered-manifest-digest", "sha256"),
                EvidenceField("migration-manifest-digest", "sha256"),
                EvidenceField("migration-artifact-digest", "sha256"),
                EvidenceField("production-defaults-digest", "sha256"),
            ),
            timeout_seconds=5,
            freshness_ttl_seconds=600,
            remediation="restore the immutable preflight artifact publication",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={
                    "bundle-digest": publication.bundle_digest,
                    "image-artifact-digest": image_digest or publication.image_artifact_sha256,
                    "manifest-artifact-digest": publication.manifest_artifact_sha256,
                    "rendered-manifest-digest": publication.rendered_manifest_sha256,
                    "migration-manifest-digest": publication.migration_manifest_sha256,
                    "migration-artifact-digest": publication.migration_manifest_artifact_sha256,
                    "production-defaults-digest": publication.production_defaults_sha256,
                },
            )
        },
    )
    return PreflightDag((check,)).run(
        CheckContext({"candidate.sha": "a" * 40}),
        now=lambda: NOW,
    )[0]


def _systemd_execution():
    evidence = _systemd_evidence()
    check = RegisteredCheck(
        spec=CheckSpec(
            check_id="systemd.render",
            failure_code="systemd.render.invalid",
            tier=1,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(
                EvidenceField("failed-units", "string-map"),
                EvidenceField("supervisor-artifact-digest", "sha256"),
                EvidenceField("supervisor-profile-sha256", "sha256"),
                EvidenceField("supervisor-script-digests", "string-map"),
                EvidenceField("supervisor-unit-digests", "string-map"),
                EvidenceField("supervisor-unit-set-digest", "sha256"),
                EvidenceField("unit-count", "integer"),
                EvidenceField("unit-digests", "string-map"),
                EvidenceField("unit-set-digest", "sha256"),
            ),
            timeout_seconds=5,
            freshness_ttl_seconds=600,
            remediation="restore exact candidate systemd unit rendering",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence=evidence,  # type: ignore[arg-type]
            )
        },
    )
    return PreflightDag((check,)).run(
        CheckContext({"candidate.sha": "a" * 40}),
        now=lambda: NOW,
    )[0]


def _predecessor_execution():
    evidence = _predecessor_evidence()
    check = RegisteredCheck(
        spec=CheckSpec(
            check_id="external-supervisor.predecessor",
            failure_code="external-supervisor.predecessor.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(
                EvidenceField("authority-kind", "string"),
                EvidenceField("authority-digest", "sha256"),
                EvidenceField("pointer-digest", "sha256"),
                EvidenceField("unit-digests", "string-map"),
                EvidenceField("unit-set-digest", "sha256"),
                EvidenceField("live-evidence-digest", "sha256"),
                EvidenceField("pending-transition-digest", "sha256"),
                EvidenceField("transition-clear", "boolean"),
                EvidenceField("runtime-ready", "boolean"),
                EvidenceField("pool-identity-digest", "sha256"),
            ),
            timeout_seconds=5,
            freshness_ttl_seconds=600,
            remediation="restore exact external supervisor predecessor authority",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence=evidence,  # type: ignore[arg-type]
            )
        },
    )
    return PreflightDag((check,)).run(
        CheckContext({"candidate.sha": "a" * 40}),
        now=lambda: NOW,
    )[0]


def _authority(tmp_path: Path, *, tamper: bool = False):
    state = tmp_path / "state"
    artifact_store = PreflightArtifactStore(state)
    images = _images()
    publication = artifact_store.publish(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        mutation_epoch=7,
        images=images,
        manifests=_manifests(images),
        migration=_migration(
            images,
            candidate_tree="b" * 40,
            migration_plan_sha256="4" * 64,
        ),
        production_defaults=_production_defaults(candidate_tree="b" * 40),
        migration_plan_sha256="4" * 64,
        migration_target_revision="0067",
        browser_report_schema_sha256="8" * 64,
    )
    execution = _execution(publication, image_digest="f" * 64 if tamper else None)
    systemd_execution = _systemd_execution()
    predecessor_execution = _predecessor_execution()
    baseline_executions = _baseline_executions()
    base = binding_attestation()
    attestation = PreflightAttestation.issue(
        bindings=base.bindings,
        executions=(execution, systemd_execution, predecessor_execution, *baseline_executions),
        issued_at=NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )
    rehearsal = PreflightRehearsal.from_executions(
        registry_digest=attestation.registry_digest,
        coverage_digest=attestation.coverage_digest,
        executions=(execution, systemd_execution, predecessor_execution, *baseline_executions),
    )
    attempt = state / "requests" / "req-alpha" / "attempts" / "1"
    attempt.mkdir(parents=True, mode=0o700)
    executable = tmp_path / "final-helper"
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o755)
    calls: list[tuple[str, ...]] = []

    def run(argv, _environment, _timeout):
        command = tuple(argv)
        calls.append(command)
        check_id = command[command.index("--check-id") + 1]
        operation = command[command.index("--operation") + 1]
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "attestation_digest": attestation.attestation_digest,
                    "blockers": {},
                    "candidate_sha": "a" * 40,
                    "check_id": check_id,
                    "evidence_digest": "e" * 64,
                    "observed_epoch": 7,
                    "operation": operation,
                    "protected_mutation": check_id in PROTECTED_MUTATION_CHECK_IDS,
                    "schema_version": 1,
                }
            ),
            "",
        )

    source = FinalGateActionSource(
        request_store=_Store(rehearsal),
        artifact_store=artifact_store,
        state_root=state,
        service_uid=os.geteuid(),
        run=run,
        read_mutation_epoch=lambda: 8,
        now=lambda: NOW,
        executable=executable,
        executable_owner_uid=os.geteuid(),
    )
    return source, attestation, calls


def test_final_gate_action_source_uses_attested_bundle_and_fixed_helper(tmp_path: Path) -> None:
    source, attestation, calls = _authority(tmp_path)
    envelope = _envelope(attestation)

    actions = source(envelope, attestation, 7, _admission(attestation))
    result = actions["final.browser"](CheckOperation.APPLY)

    assert set(actions) == set(FINAL_CHECK_IDS)
    assert result.ready and result.protected_mutation
    assert calls[0][0].endswith("final-helper")
    plan = FinalGatePlanStore(tmp_path / "state", request_id="req-alpha", attempt_number=1).read()
    assert plan.artifact_bundle_digest == Path(plan.artifact_descriptor_path).parent.name
    assert plan.supervisor_artifact_digest == "4" * 64
    assert calls[0][calls[0].index("--plan-sha256") + 1] == plan.plan_digest


def test_final_gate_action_source_rejects_attested_publication_mismatch(tmp_path: Path) -> None:
    source, attestation, calls = _authority(tmp_path, tamper=True)

    with pytest.raises(ValueError, match="publication drifted"):
        source(_envelope(attestation), attestation, 7, _admission(attestation))

    assert calls == []


def test_final_gate_action_source_runs_post_apply_drift_in_process(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source, attestation, calls = _authority(tmp_path)
    preflight_plan = SimpleNamespace(candidate="exact-plan")
    admission = replace(
        _admission(attestation),
        preflight_plan=preflight_plan,  # type: ignore[arg-type]
    )
    captured: dict[str, object] = {}

    def validate(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(observed_mutation_epoch=8, evidence_digest="d" * 64)

    monkeypatch.setattr(action_source_module, "validate_post_apply_attestation_drift", validate)
    actions = source(_envelope(attestation), attestation, 7, admission)

    result = actions["final.drift"](CheckOperation.VERIFY)

    assert result.ready
    assert result.observed_epoch == 8
    assert not result.protected_mutation
    assert captured["admission"] is admission
    assert captured["plan"] is preflight_plan
    assert captured["current_mutation_epoch"] == 8
    assert calls == []


def test_final_gate_action_source_rejects_unbound_post_apply_plan(tmp_path: Path) -> None:
    source, attestation, calls = _authority(tmp_path)
    actions = source(_envelope(attestation), attestation, 7, _admission(attestation))

    with pytest.raises(ValueError, match="unavailable"):
        actions["final.drift"](CheckOperation.VERIFY)

    assert calls == []
