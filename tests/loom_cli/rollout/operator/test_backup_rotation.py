from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from loom_cli.rollout.operator.backup_lease import BackupLease
from loom_cli.rollout.operator.backup_rotation import (
    BackupPayloadPhase,
    BackupPayloadRecord,
    BackupRetirementRecord,
    BackupRotationError,
    BackupRotationState,
    acknowledge_retirement,
    begin_candidate,
    collect_failed_candidate,
    fail_candidate,
    promote_candidate,
    record_manifest_verified,
    record_restore_verified,
    recover_failed_retirement,
)

NOW = datetime(2026, 7, 19, 20, tzinfo=UTC)


def _lease(request_id: str, *, suffix: str) -> BackupLease:
    return BackupLease(
        lease_id=f"lease-{suffix}0000000",
        source_request_id=request_id,
        manifest_sha256=suffix * 64,
        component_sha256={"postgres": "b" * 64, "authority": "c" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="lsn:0/16B6C50",
        schema_revision="0066",
        object_inventory_root="d" * 64,
        created_at=NOW - timedelta(minutes=20),
        restore_verified_at=NOW - timedelta(minutes=10),
        expires_at=NOW + timedelta(hours=2),
    )


def _verified_candidate(
    state: BackupRotationState,
    *,
    request_id: str,
    payload_id: str,
    suffix: str,
) -> BackupRotationState:
    state = begin_candidate(
        state,
        payload_id=payload_id,
        request_id=request_id,
        bundle_name=f"20260719T200000Z-{request_id}",
        created_at=NOW,
    ).state
    state = record_manifest_verified(
        state,
        payload_id=payload_id,
        manifest_sha256=suffix * 64,
    ).state
    return record_restore_verified(
        state,
        payload_id=payload_id,
        lease=_lease(request_id, suffix=suffix),
    ).state


def test_candidate_failure_before_manifest_preserves_active_until_compacted() -> None:
    active_state = _verified_candidate(
        BackupRotationState(),
        request_id="req-active000",
        payload_id="payload-active00",
        suffix="a",
    )
    active_state = promote_candidate(active_state, payload_id="payload-active00").state
    state = begin_candidate(
        active_state,
        payload_id="payload-failed00",
        request_id="req-failed000",
        bundle_name="20260719T200000Z-req-failed000",
        created_at=NOW,
    ).state

    result = fail_candidate(
        state,
        payload_id="payload-failed00",
        failure_code="backup_cancelled_before_manifest",
    )

    assert result.state.active == active_state.active
    assert result.state.candidate is None
    assert tuple(record.payload_id for record in result.state.retirements) == ("payload-failed00",)
    assert result.delete_payload_ids == ("payload-failed00",)


def test_validation_and_restore_failure_never_promote_candidate() -> None:
    state = begin_candidate(
        BackupRotationState(),
        payload_id="payload-candidate0",
        request_id="req-candidate0",
        bundle_name="20260719T200000Z-req-candidate0",
        created_at=NOW,
    ).state
    with pytest.raises(ValueError, match="digest"):
        record_manifest_verified(
            state,
            payload_id="payload-candidate0",
            manifest_sha256="invalid",
        )
    with pytest.raises(BackupRotationError, match="wrong phase"):
        promote_candidate(state, payload_id="payload-candidate0")

    state = record_manifest_verified(
        state,
        payload_id="payload-candidate0",
        manifest_sha256="a" * 64,
    ).state
    with pytest.raises(BackupRotationError, match="another request"):
        record_restore_verified(
            state,
            payload_id="payload-candidate0",
            lease=_lease("req-other0000", suffix="a"),
        )
    failed = fail_candidate(
        state,
        payload_id="payload-candidate0",
        failure_code="restore_verification_failed",
        referenced_payload_ids=frozenset({"payload-candidate0"}),
    ).state
    assert failed.active is None
    assert failed.candidate is not None
    assert failed.candidate.phase is BackupPayloadPhase.FAILED


def test_active_request_reference_defers_payload_deletion_until_retry() -> None:
    state = begin_candidate(
        BackupRotationState(),
        payload_id="payload-failed00",
        request_id="req-failed000",
        bundle_name="20260719T200000Z-req-failed000",
        created_at=NOW,
    ).state
    state = fail_candidate(
        state,
        payload_id="payload-failed00",
        failure_code="manifest_failed",
        referenced_payload_ids=frozenset({"payload-failed00"}),
    ).state

    deferred = collect_failed_candidate(
        state,
        referenced_payload_ids=frozenset({"payload-failed00"}),
    )
    collected = collect_failed_candidate(state)

    assert deferred.state == state
    assert deferred.delete_payload_ids == ()
    assert collected.state.candidate is None
    assert tuple(record.payload_id for record in collected.state.retirements) == (
        "payload-failed00",
    )
    assert collected.delete_payload_ids == ("payload-failed00",)


def test_promotion_state_is_crash_safe_before_old_payload_deletion() -> None:
    state = _verified_candidate(
        BackupRotationState(),
        request_id="req-old000000",
        payload_id="payload-old00000",
        suffix="a",
    )
    state = promote_candidate(state, payload_id="payload-old00000").state
    state = _verified_candidate(
        state,
        request_id="req-new000000",
        payload_id="payload-new00000",
        suffix="e",
    )

    promoted = promote_candidate(state, payload_id="payload-new00000")

    assert promoted.state.active is not None
    assert promoted.state.active.payload_id == "payload-new00000"
    assert promoted.state.candidate is None
    assert promoted.delete_payload_ids == ("payload-old00000",)
    # A crash after atomic state publication but before deletion leaves the new
    # verified lease active and the old payload safe for idempotent GC retry.
    assert promoted.state.payload_count == 2
    assert tuple(record.payload_id for record in promoted.state.retirements) == (
        "payload-old00000",
    )

    with pytest.raises(BackupRotationError, match="transient limit"):
        begin_candidate(
            promoted.state,
            payload_id="payload-third000",
            request_id="req-third0000",
            bundle_name="20260719T200000Z-req-third0000",
            created_at=NOW,
        )

    acknowledged = acknowledge_retirement(
        promoted.state,
        payload_id="payload-old00000",
    ).state
    assert acknowledged.payload_count == 1
    assert acknowledged.retirements == ()


def test_exact_lease_backed_failed_retirement_can_be_recovered_as_active() -> None:
    lease = _lease("req-latest0000", suffix="a")
    active = BackupPayloadRecord(
        payload_id="payload-latest00",
        request_id="req-latest0000",
        bundle_name="20260719T200000Z-req-latest0000",
        phase=BackupPayloadPhase.ACTIVE,
        created_at=lease.created_at,
        manifest_sha256=lease.manifest_sha256,
        lease=lease,
    )
    latest = BackupRetirementRecord(
        payload_id=active.payload_id,
        request_id=active.request_id,
        bundle_name=active.bundle_name,
        reason="failed",
        manifest_sha256=active.manifest_sha256,
    )
    incomplete = BackupRetirementRecord(
        payload_id="payload-partial00",
        request_id="req-partial000",
        bundle_name="20260719T210000Z-req-partial000",
        reason="failed",
    )
    state = BackupRotationState(generation=7, retirements=(latest, incomplete))

    recovered = recover_failed_retirement(state, active=active).state

    assert recovered.generation == 8
    assert recovered.active == active
    assert recovered.retirements == (incomplete,)
    assert recovered.payload_count == 2

    with pytest.raises(BackupRotationError, match="empty active slot"):
        recover_failed_retirement(recovered, active=active)


def test_one_failed_retirement_can_share_the_transient_window_with_candidate() -> None:
    state = begin_candidate(
        BackupRotationState(),
        payload_id="payload-failed00",
        request_id="req-failed000",
        bundle_name="20260719T200000Z-req-failed000",
        created_at=NOW,
    ).state
    state = fail_candidate(
        state,
        payload_id="payload-failed00",
        failure_code="restore_failed",
    ).state

    result = begin_candidate(
        state,
        payload_id="payload-replace0",
        request_id="req-replace000",
        bundle_name="20260719T210000Z-req-replace000",
        created_at=NOW + timedelta(hours=1),
    )

    assert result.state.payload_count == 2
    assert result.state.candidate is not None
    assert result.state.candidate.payload_id == "payload-replace0"
    assert tuple(record.payload_id for record in result.state.retirements) == ("payload-failed00",)


def test_three_replacements_stay_at_one_steady_and_two_transient_payloads() -> None:
    state = BackupRotationState()
    deleted: list[str] = []
    for index, suffix in enumerate(("a", "e", "f", "9")):
        payload_id = f"payload-rotate0{index}"
        request_id = f"req-rotate000{index}"
        state = _verified_candidate(
            state,
            request_id=request_id,
            payload_id=payload_id,
            suffix=suffix,
        )
        assert state.payload_count <= 2
        result = promote_candidate(state, payload_id=payload_id)
        state = result.state
        deleted.extend(result.delete_payload_ids)
        if result.delete_payload_ids:
            assert state.payload_count == 2
            state = acknowledge_retirement(
                state,
                payload_id=result.delete_payload_ids[0],
            ).state
        assert state.payload_count == 1

    assert deleted == ["payload-rotate00", "payload-rotate01", "payload-rotate02"]
    assert state.active is not None
    assert state.active.payload_id == "payload-rotate03"


def test_second_candidate_is_rejected_and_state_digest_is_deterministic() -> None:
    state = begin_candidate(
        BackupRotationState(),
        payload_id="payload-first000",
        request_id="req-first0000",
        bundle_name="20260719T200000Z-req-first0000",
        created_at=NOW,
    ).state

    with pytest.raises(BackupRotationError, match="already reserved"):
        begin_candidate(
            state,
            payload_id="payload-second00",
            request_id="req-second000",
            bundle_name="20260719T200000Z-req-second000",
            created_at=NOW,
        )

    assert state.evidence_digest == state.evidence_digest
    assert len(state.evidence_digest) == 64
    assert BackupRotationState.from_dict(state.to_dict()) == state


def test_schema_one_rotation_state_upgrades_without_retirements() -> None:
    legacy = {
        "active": None,
        "candidate": None,
        "generation": 3,
        "schema_version": 1,
    }

    state = BackupRotationState.from_dict(legacy)

    assert state.generation == 3
    assert state.retirements == ()
    assert state.to_dict()["schema_version"] == 3


def test_schema_two_rotation_derives_bundle_and_defers_retirement_resolution() -> None:
    state = _verified_candidate(
        BackupRotationState(),
        request_id="req-legacy0000",
        payload_id="payload-legacy000",
        suffix="a",
    )
    state = promote_candidate(state, payload_id="payload-legacy000").state
    payload = state.to_dict()
    payload["schema_version"] = 2
    active = payload["active"]
    assert isinstance(active, dict)
    active.pop("bundle_name")
    payload["retirements"] = [
        {
            "payload_id": "payload-retired00",
            "reason": "superseded",
            "request_id": "req-retired000",
        }
    ]

    upgraded = BackupRotationState.from_dict(payload)

    assert upgraded.active is not None
    assert upgraded.active.bundle_name == "20260719T200000Z-req-legacy0000"
    assert upgraded.retirements[0].bundle_name is None
    assert upgraded.to_dict()["schema_version"] == 3
