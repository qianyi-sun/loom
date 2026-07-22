from __future__ import annotations

import json
import stat
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.preflight_attestation_store import (
    PreflightAttestationStore,
    PreflightAttestationStoreError,
)
from loom_cli.rollout.preflight_contract import (
    EXTERNAL_SUPERVISOR_ABSENT_DIGEST,
    AttestationBindings,
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
    external_supervisor_unit_set_digest,
)

NOW = datetime(2026, 7, 19, 16, tzinfo=UTC)


def _attestation() -> PreflightAttestation:
    check = RegisteredCheck(
        spec=CheckSpec(
            check_id="candidate.identity",
            failure_code="candidate.identity.drift",
            tier=0,
            stage=StageCapability.STATIC,
            dependencies=(),
            mutation_class=MutationClass.NONE,
            input_keys=("candidate.sha",),
            evidence_schema=(EvidenceField("ready", "boolean"),),
            timeout_seconds=5,
            freshness_ttl_seconds=600,
            remediation="restore the exact candidate identity",
            secret_redaction_policy=SecretRedactionPolicy.NO_SECRET_INPUTS,
        ),
        implementation_version="test-v1",
        operations={
            CheckOperation.PROBE: lambda _context: CheckProbe(
                passed=True,
                evidence={"ready": True},
            )
        },
    )
    executions = PreflightDag((check,)).run(
        CheckContext({"candidate.sha": "a" * 40}),
        now=lambda: NOW,
    )
    predecessor_units = {
        "loom-autoscaler-gb10-staging.service": "d" * 64,
        "loom-autoscaler-gb10-staging.timer": "e" * 64,
    }
    bindings = AttestationBindings(
        candidate_sha="a" * 40,
        candidate_tree="b" * 40,
        image_digests={"api": "sha256:" + "1" * 64},
        runner_source_sha="c" * 40,
        runner_source_tree="d" * 40,
        runner_install_hash="2" * 64,
        runner_config_hash="3" * 64,
        staging_mutation_epoch=7,
        backup_lease_id="lease-1",
        backup_lease_digest="9" * 64,
        backup_manifest_sha256="a" * 64,
        backup_component_set_digest="b" * 64,
        db_snapshot_identity="snapshot-1",
        schema_revision="0066",
        object_inventory_root="c" * 64,
        migration_plan_digest="4" * 64,
        environment="staging",
        namespace="loom-staging",
        route="https://yylx.world/dev",
        secret_metadata_fingerprints={"admin": "sha256:abc len=32"},
        gb10_inventory_digest="5" * 64,
        gb10_boot_ids={"trt-gb10-1": "boot-1"},
        gb10_mount_digest="6" * 64,
        gb10_unit_digest="7" * 64,
        browser_image_digest="sha256:" + "8" * 64,
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
    )
    return PreflightAttestation.issue(
        bindings=bindings,
        executions=executions,
        issued_at=NOW,
        registry_digest="9" * 64,
        coverage_digest="a" * 64,
    )


def test_store_is_private_immutable_and_reuses_exact_attestation(tmp_path: Path) -> None:
    store = PreflightAttestationStore(tmp_path)
    attestation = _attestation()

    path = store.publish(attestation)

    assert path == tmp_path / "preflight-attestations" / (f"{attestation.attestation_digest}.json")
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert path.stat().st_nlink == 1
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert store.read(attestation.attestation_digest) == attestation
    assert store.publish(attestation) == path


def test_store_rejects_payload_tamper_and_untrusted_digest(tmp_path: Path) -> None:
    store = PreflightAttestationStore(tmp_path)
    attestation = _attestation()
    path = store.publish(attestation)
    payload = json.loads(path.read_text(encoding="utf-8"))
    bindings = payload["bindings"]
    assert isinstance(bindings, dict)
    bindings["staging_mutation_epoch"] = 8
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(PreflightAttestationStoreError, match="invalid"):
        store.read(attestation.attestation_digest)
    with pytest.raises(PreflightAttestationStoreError, match="digest is invalid"):
        store.read("../escape")

    with pytest.raises(PreflightAttestationStoreError, match="invalid"):
        store.publish(replace(attestation, attestation_digest="f" * 64))


def test_store_rejects_symlinked_attestation_directory(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    root = tmp_path / "state"
    root.mkdir(mode=0o700)
    (root / "preflight-attestations").symlink_to(outside, target_is_directory=True)

    with pytest.raises(PreflightAttestationStoreError, match="authority is unsafe"):
        PreflightAttestationStore(root).publish(_attestation())

    assert list(outside.iterdir()) == []
