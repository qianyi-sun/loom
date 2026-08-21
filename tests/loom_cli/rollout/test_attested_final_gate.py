from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.attested_final_gate import AttestedFinalGateAuthority
from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    PROTECTED_MUTATION_CHECK_IDS,
    FinalGateResult,
)
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    AttestationBindings,
    CheckOperation,
    PreflightAttestation,
    external_supervisor_unit_set_digest,
)
from loom_cli.rollout.preflight_coverage import (
    DEFAULT_COVERAGE_MANIFEST,
    load_coverage_manifest,
)

NOW = datetime(2026, 7, 19, 20, tzinfo=UTC)
CANDIDATE = "a" * 40
ATTESTATION = "f" * 64


def _bindings() -> AttestationBindings:
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "d" * 64,
        "loom-autoscaler-gb10-staging.timer": "e" * 64,
    }
    oldlab_predecessor_units = {
        "loom-autoscaler-oldlab-staging.service": "4" * 64,
        "loom-autoscaler-oldlab-staging.timer": "5" * 64,
    }
    return AttestationBindings(
        candidate_sha=CANDIDATE,
        candidate_tree="b" * 40,
        image_digests={"control-plane": "sha256:" + "1" * 64},
        runner_source_sha=CANDIDATE,
        runner_source_tree="b" * 40,
        runner_install_hash="2" * 64,
        runner_config_hash="3" * 64,
        staging_mutation_epoch=7,
        backup_lease_id="lease-1",
        backup_lease_digest="4" * 64,
        backup_manifest_sha256="5" * 64,
        backup_component_set_digest="6" * 64,
        db_snapshot_identity="snapshot-1",
        schema_revision="0067",
        object_inventory_root="7" * 64,
        migration_plan_digest="8" * 64,
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        secret_metadata_fingerprints={"browser": "sha256:abcdef len=32"},
        gb10_inventory_digest="9" * 64,
        gb10_boot_ids={"trt-gb10-1": "boot-1"},
        gb10_mount_digest="a" * 64,
        gb10_unit_digest="b" * 64,
        browser_image_digest="sha256:" + "c" * 64,
        browser_report_schema="v3",
        supervisor_predecessor_kind="legacy-manifest",
        supervisor_predecessor_digest="f" * 64,
        supervisor_predecessor_pointer_digest=EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
        supervisor_predecessor_unit_sha256=predecessor_units,
        supervisor_predecessor_unit_set_digest=external_supervisor_unit_set_digest(
            predecessor_units
        ),
        supervisor_predecessor_live_evidence_digest="1" * 64,
        supervisor_predecessor_pending_transition_digest="2" * 64,
        supervisor_transition_digest="3" * 64,
        supervisor_controller_bindings={
            "gx10-01c7/authority-kind": "legacy-manifest",
            "gx10-01c7/authority-digest": "f" * 64,
            "gx10-01c7/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            "gx10-01c7/unit-set-digest": external_supervisor_unit_set_digest(predecessor_units),
            "gx10-01c7/live-evidence-digest": "1" * 64,
            "gx10-01c7/pending-transition-digest": "2" * 64,
            "gx10-01c7/unit-directory": "/var/lib/loom-rollout/.config/systemd/user",
            "gx10-01c7/transition-digest": "3" * 64,
            **{f"gx10-01c7/unit/{name}": digest for name, digest in predecessor_units.items()},
            "TRT-EAI-OLDLAB-1/authority-kind": "legacy-manifest",
            "TRT-EAI-OLDLAB-1/authority-digest": "6" * 64,
            "TRT-EAI-OLDLAB-1/pointer-digest": EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
            "TRT-EAI-OLDLAB-1/unit-set-digest": external_supervisor_unit_set_digest(
                oldlab_predecessor_units
            ),
            "TRT-EAI-OLDLAB-1/live-evidence-digest": "7" * 64,
            "TRT-EAI-OLDLAB-1/pending-transition-digest": "8" * 64,
            "TRT-EAI-OLDLAB-1/unit-directory": (
                "/var/lib/loom-staging-rollout/.config/systemd/user"
            ),
            "TRT-EAI-OLDLAB-1/transition-digest": "9" * 64,
            **{
                f"TRT-EAI-OLDLAB-1/unit/{name}": digest
                for name, digest in oldlab_predecessor_units.items()
            },
        },
    )


def _attestation(*, expires_at: datetime | None = None) -> PreflightAttestation:
    prefinal = {entry.check_id for entry in load_coverage_manifest().checks if entry.tier < 4}
    return PreflightAttestation(
        schema_version=2,
        bindings=_bindings(),
        registry_digest="d" * 64,
        coverage_digest=hashlib.sha256(DEFAULT_COVERAGE_MANIFEST.read_bytes()).hexdigest(),
        check_implementation_digests={check_id: "e" * 64 for check_id in prefinal},
        evidence_hashes={check_id: "1" * 64 for check_id in prefinal},
        issued_at=NOW - timedelta(minutes=5),
        expires_at=expires_at or NOW + timedelta(minutes=30),
        attestation_digest=ATTESTATION,
    )


def _actions(calls: list[tuple[str, CheckOperation]]):
    def action(check_id: str):
        def execute(operation: CheckOperation) -> FinalGateResult:
            calls.append((check_id, operation))
            return FinalGateResult(
                check_id=check_id,
                operation=operation,
                candidate_sha=CANDIDATE,
                attestation_digest=ATTESTATION,
                observed_epoch=7,
                evidence_digest=hashlib.sha256(check_id.encode()).hexdigest(),
                protected_mutation=(
                    check_id in PROTECTED_MUTATION_CHECK_IDS and operation is CheckOperation.APPLY
                ),
                blockers={},
            )

        return execute

    return {check_id: action(check_id) for check_id in FINAL_CHECK_IDS}


def test_attested_final_gate_applies_once_then_verifies_shared_checks() -> None:
    calls: list[tuple[str, CheckOperation]] = []
    authority = AttestedFinalGateAuthority(
        attestation=_attestation(),
        actions=_actions(calls),
        candidate_sha=CANDIDATE,
        mutation_epoch=7,
        now=NOW,
    )

    report = authority.execute(now=NOW)

    assert report.passed
    assert calls[0] == ("final.protected-apply", CheckOperation.APPLY)
    assert calls[1:] == [
        (
            check_id,
            CheckOperation.APPLY
            if check_id in PROTECTED_MUTATION_CHECK_IDS
            else CheckOperation.VERIFY,
        )
        for check_id in FINAL_CHECK_IDS[1:]
    ]


def test_attested_final_gate_replays_component_journal_after_outer_apply_crash() -> None:
    calls: list[tuple[str, CheckOperation]] = []
    authority = AttestedFinalGateAuthority(
        attestation=_attestation(),
        actions=_actions(calls),
        candidate_sha=CANDIDATE,
        mutation_epoch=7,
        now=NOW,
        post_apply_resume=True,
        protected_apply_recovery=True,
    )

    report = authority.execute(now=NOW)

    assert report.passed
    assert calls[0] == ("final.protected-apply", CheckOperation.APPLY)


def test_attested_final_gate_rejects_ambiguous_recovery_authority() -> None:
    with pytest.raises(ValueError, match="expired or drifted"):
        AttestedFinalGateAuthority(
            attestation=_attestation(),
            actions=_actions([]),
            candidate_sha=CANDIDATE,
            mutation_epoch=7,
            now=NOW,
            protected_apply_recovery=True,
        )


def test_attested_final_gate_refuses_expired_or_incomplete_proof() -> None:
    with pytest.raises(ValueError, match="expired or drifted"):
        AttestedFinalGateAuthority(
            attestation=_attestation(expires_at=NOW),
            actions=_actions([]),
            candidate_sha=CANDIDATE,
            mutation_epoch=7,
            now=NOW,
        )

    incomplete = _attestation()
    incomplete = PreflightAttestation(
        schema_version=incomplete.schema_version,
        bindings=incomplete.bindings,
        registry_digest=incomplete.registry_digest,
        coverage_digest=incomplete.coverage_digest,
        check_implementation_digests={"candidate.identity": "e" * 64},
        evidence_hashes={"candidate.identity": "1" * 64},
        issued_at=incomplete.issued_at,
        expires_at=incomplete.expires_at,
        attestation_digest=incomplete.attestation_digest,
    )
    with pytest.raises(ValueError, match="dependency coverage drifted"):
        AttestedFinalGateAuthority(
            attestation=incomplete,
            actions=_actions([]),
            candidate_sha=CANDIDATE,
            mutation_epoch=7,
            now=NOW,
        )
