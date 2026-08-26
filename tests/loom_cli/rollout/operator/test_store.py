from __future__ import annotations

import fcntl
import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pytest

from loom_cli.rollout.lifecycle_protocol import LifecycleAction
from loom_cli.rollout.operator import store as store_module
from loom_cli.rollout.operator.backup_job import (
    BackupJobEnvelope,
    BackupJobState,
    PreflightBackupJobEnvelope,
    transition_backup_job,
)
from loom_cli.rollout.operator.backup_lease import BackupLease
from loom_cli.rollout.operator.backup_rotation import (
    BackupRotationState,
    begin_candidate,
    record_manifest_verified,
    record_restore_verified,
)
from loom_cli.rollout.operator.config import APPROVED_REMOTE_URL
from loom_cli.rollout.operator.model import (
    ActivePointer,
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    PreflightRequest,
    RequestEvent,
    RolloutRequest,
)
from loom_cli.rollout.operator.store import RequestStore, RequestStoreError
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_pipeline import PreflightAssessment, PreflightPipeline
from tests.loom_cli.rollout.test_preflight_pipeline import _context, _registry

REQUEST_ID = "stg-20260713-abcdef12"
RESOLVED_SHA = "abcdef1234567890abcdef1234567890abcdef12"


def _set_active_concurrently(
    root: str,
    pointer_payload: dict[str, object],
    ready_queue: Any,
    start_event: Any,
    result_queue: Any,
) -> None:
    request_id = str(pointer_payload["request_id"])
    ready_queue.put(request_id)
    if not start_event.wait(timeout=5):
        result_queue.put(("timeout", request_id, "start event was not released"))
        return
    try:
        RequestStore(root).set_active(ActivePointer.from_dict(pointer_payload))
    except Exception as exc:
        result_queue.put(("error", request_id, str(exc)))
    else:
        result_queue.put(("ok", request_id, ""))


def _append_event_in_two_locked_writes(
    events_path: str,
    payload: bytes,
    first_half_written: Any,
    release_writer: Any,
) -> None:
    fd = os.open(events_path, os.O_WRONLY | os.O_APPEND)
    locked = False
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        locked = True
        midpoint = len(payload) // 2
        os.write(fd, payload[:midpoint])
        first_half_written.set()
        if not release_writer.wait(timeout=5):
            raise RuntimeError("reader did not release split event writer")
        os.write(fd, payload[midpoint:])
        os.fsync(fd)
    finally:
        if locked:
            fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def make_request(
    *,
    request_id: str = REQUEST_ID,
    status: str = "pending",
) -> RolloutRequest:
    return RolloutRequest(
        request_id=request_id,
        rollout_id="staging-abcdef1",
        caller=CallerIdentity("hongjian", 2002),
        candidate=CandidateBinding(
            remote_url=APPROVED_REMOTE_URL,
            target_ref="origin/dev",
            resolved_sha=RESOLVED_SHA,
            image_tag="staging-abcdef1",
            fetched_at="2026-07-13T20:00:00Z",
        ),
        requested_at="2026-07-13T20:00:01Z",
        runner_config_sha256="2" * 64,
        preflight_attestation_sha256="3" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
        command="start",
        status=status,  # type: ignore[arg-type]
    )


def make_envelope(
    *,
    request_id: str = REQUEST_ID,
    attempt_number: int = 1,
    attempt_operator: str = "hongjian",
    attempt_uid: int = 2002,
    resume: bool = False,
) -> DriverEnvelope:
    return DriverEnvelope(
        schema_version=1,
        request_id=request_id,
        rollout_id="staging-abcdef1",
        initiating_operator="hongjian",
        initiating_uid=2002,
        attempt_number=attempt_number,
        attempt_operator=attempt_operator,
        attempt_uid=attempt_uid,
        remote_url=APPROVED_REMOTE_URL,
        target_ref="origin/dev",
        resolved_sha=RESOLVED_SHA,
        image_tag="staging-abcdef1",
        fetched_at="2026-07-13T20:00:00Z",
        backup_manifest_path=(
            "/data/loom-staging/backups/20260713T200000Z-stg-20260713-abcdef12/backup-manifest.json"
        ),
        backup_manifest_sha256="1" * 64,
        runner_config_sha256="2" * 64,
        preflight_attestation_sha256="3" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        cluster_config_path=(
            "/opt/loom-staging-runner/repo/deploy/environments/staging.cluster.toml"
        ),
        rollout_root="/data/loom-staging",
        admin_token_source="file:/var/lib/loom-staging-rollout/credentials/admin-token",
        worker_token_source="file:/var/lib/loom-staging-rollout/credentials/worker-token",
        service_token_source="file:/var/lib/loom-staging-rollout/credentials/service-token",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        resume=resume,
    )


def make_event(
    *,
    event: str = "requested",
    occurred_at: str = "2026-07-13T20:00:01Z",
    attempt_number: int | None = None,
    status: str | None = "pending",
) -> RequestEvent:
    return RequestEvent(
        request_id=REQUEST_ID,
        event=event,  # type: ignore[arg-type]
        occurred_at=occurred_at,
        operator="hongjian",
        operator_uid=2002,
        attempt_number=attempt_number,
        status=status,  # type: ignore[arg-type]
    )


def make_backup_job_request() -> RolloutRequest:
    return replace(
        make_request(),
        candidate=CandidateBinding(
            remote_url=APPROVED_REMOTE_URL,
            target_ref="origin/dev",
            resolved_sha=RESOLVED_SHA,
            image_tag="staging-abcdef1",
            fetched_at="2026-07-13T20:00:00Z",
            source_mode="sealed-cumulative",
            resolved_tree="b" * 40,
            approved_base_sha="c" * 40,
        ),
    )


def make_backup_job() -> BackupJobEnvelope:
    return BackupJobEnvelope(
        job_id="job-20260713-abcdef12",
        request_id=REQUEST_ID,
        payload_id="payload-20260713-abcdef12",
        candidate_sha=RESOLVED_SHA,
        candidate_tree="b" * 40,
        preflight_attestation_sha256="3" * 64,
        mutation_epoch=4,
        environment="staging",
        namespace="loom-staging",
        bundle_name="20260713T200000Z-stg-20260713-abcdef12",
        created_at=datetime(2026, 7, 13, 20, tzinfo=UTC),
    )


def make_preflight_request() -> PreflightRequest:
    return PreflightRequest(
        request_id=REQUEST_ID,
        rollout_id="staging-abcdef1",
        caller=CallerIdentity("hongjian", 2002),
        candidate=CandidateBinding(
            remote_url=APPROVED_REMOTE_URL,
            target_ref="origin/dev",
            resolved_sha=RESOLVED_SHA,
            image_tag="staging-abcdef1",
            fetched_at="2026-07-13T20:00:00Z",
            source_mode="sealed-cumulative",
            resolved_tree="b" * 40,
            approved_base_sha="c" * 40,
        ),
        candidate_tree="b" * 40,
        requested_at="2026-07-13T20:00:01Z",
        runner_config_sha256="2" * 64,
        preflight_assessment_sha256="6" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
        mutation_epoch=4,
        environment="staging",
        namespace="loom-staging",
    )


def make_preflight_backup_job() -> PreflightBackupJobEnvelope:
    return PreflightBackupJobEnvelope(
        job_id="job-20260713-abcdef12",
        request_id=REQUEST_ID,
        payload_id="payload-20260713-abcdef12",
        candidate_sha=RESOLVED_SHA,
        candidate_tree="b" * 40,
        preflight_assessment_sha256="6" * 64,
        preflight_registry_sha256="4" * 64,
        preflight_coverage_sha256="5" * 64,
        mutation_epoch=4,
        environment="staging",
        namespace="loom-staging",
        bundle_name="20260713T200000Z-stg-20260713-abcdef12",
        created_at=datetime(2026, 7, 13, 20, tzinfo=UTC),
    )


def make_assessment(tmp_path: Path) -> PreflightAssessment:
    registry = _registry()
    return PreflightPipeline(
        registry=registry,
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: datetime(2026, 7, 13, 20, tzinfo=UTC),
    ).assess(context=_context(registry))


def test_create_request_is_private_and_no_replace(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    request = make_request()

    path = store.create_request(request)

    assert path == tmp_path / "requests" / REQUEST_ID / "request.json"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.parent.parent.stat().st_mode) == 0o700
    assert list(path.parent.glob(".*.tmp")) == []
    with pytest.raises(RequestStoreError, match="already exists"):
        store.create_request(request)
    assert store.read_request(REQUEST_ID) == request


def test_backup_lease_and_rotation_are_digest_bound_and_compare_and_swap(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    lease = BackupLease(
        lease_id="lease-alpha000",
        source_request_id="req-alpha0000",
        manifest_sha256="a" * 64,
        component_sha256={"postgres": "b" * 64, "authority": "c" * 64},
        environment="staging",
        namespace="loom-staging",
        mutation_epoch=7,
        db_snapshot_identity="lsn:0/16B6C50",
        schema_revision="0067",
        object_inventory_root="d" * 64,
        created_at=datetime(2026, 7, 19, 20, tzinfo=UTC),
        restore_verified_at=datetime(2026, 7, 19, 20, 5, tzinfo=UTC),
        expires_at=datetime(2026, 7, 19, 20, tzinfo=UTC) + timedelta(hours=2),
    )
    lease_path = store.publish_backup_lease(lease)
    state = begin_candidate(
        BackupRotationState(),
        payload_id="payload-alpha000",
        request_id="req-alpha0000",
        bundle_name="20260719T200000Z-req-alpha0000",
        created_at=datetime(2026, 7, 19, 20, tzinfo=UTC),
    ).state
    store.replace_backup_rotation(state, expected_generation=0)
    next_state = record_manifest_verified(
        state,
        payload_id="payload-alpha000",
        manifest_sha256=lease.manifest_sha256,
    ).state
    store.replace_backup_rotation(next_state, expected_generation=1)
    restored_state = record_restore_verified(
        next_state,
        payload_id="payload-alpha000",
        lease=lease,
    ).state
    store.replace_backup_rotation(restored_state, expected_generation=2)

    assert stat.S_IMODE(lease_path.stat().st_mode) == 0o600
    assert store.publish_backup_lease(lease) == lease_path
    assert store.read_backup_lease(lease.evidence_digest) == lease
    assert store.read_backup_rotation() == restored_state
    with pytest.raises(RequestStoreError, match="changed concurrently"):
        store.replace_backup_rotation(restored_state, expected_generation=2)


def test_preflight_request_backup_and_promotion_are_separate_immutable_authorities(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    assessment = make_assessment(tmp_path)
    preliminary = replace(
        make_preflight_request(),
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    path = store.create_preflight_request(preliminary)
    store.publish_preflight_assessment(REQUEST_ID, assessment)
    job = replace(
        make_preflight_backup_job(),
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )

    job_path = store.publish_preflight_backup_job(job)
    rehearsal = PreflightPipeline(
        registry=_registry(),
        store=PreflightAttestationStore(tmp_path / "attestations"),
        now=lambda: datetime(2026, 7, 13, 20, tzinfo=UTC),
    ).rehearse(context=_context(_registry()), assessment=assessment)
    rehearsal_path = store.publish_preflight_rehearsal(REQUEST_ID, rehearsal)
    state = store.read_preflight_backup_job_state(REQUEST_ID)
    running = transition_backup_job(
        state,
        LifecycleAction.START_BACKUP,
        updated_at=datetime(2026, 7, 13, 20, 1, tzinfo=UTC),
    )
    store.replace_preflight_backup_job_state(running, expected_sequence=0)

    assert path.name == "preflight.json"
    assert store.read_preflight_request(REQUEST_ID) == preliminary
    assert job_path.parent.name == "preflight-backup"
    assert store.read_preflight_backup_job(REQUEST_ID) == job
    assert rehearsal_path.name == "rehearsal.json"
    assert store.read_preflight_rehearsal(REQUEST_ID) == rehearsal
    assert store.read_preflight_backup_job_state(REQUEST_ID) == running
    store.append_event(make_event(event="backup_started"))
    assert store.read_events(REQUEST_ID)[-1].event == "backup_started"
    with pytest.raises(RequestStoreError, match="not promoted"):
        store.read_request(REQUEST_ID)

    promoted = replace(
        make_backup_job_request(),
        preflight_attestation_sha256="7" * 64,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    request_path = store.promote_preflight_request(promoted)

    assert request_path.name == "request.json"
    assert store.read_request(REQUEST_ID) == promoted
    assert store.read_preflight_request(REQUEST_ID) == preliminary
    with pytest.raises(RequestStoreError, match="already exists"):
        store.promote_preflight_request(promoted)


def test_active_attempt_resolves_exact_backup_payload_reference(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    assessment = make_assessment(tmp_path)
    request = replace(
        make_preflight_request(),
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    job = replace(
        make_preflight_backup_job(),
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    store.create_preflight_request(request)
    store.publish_preflight_assessment(REQUEST_ID, assessment)
    store.publish_preflight_backup_job(job)

    assert store.referenced_backup_payload_ids() == frozenset()
    store.set_active(
        ActivePointer(
            request_id=REQUEST_ID,
            attempt_number=1,
            unit_name="loom-staging-rollout-test.service",
            status="running",
        )
    )

    assert store.referenced_backup_payload_ids() == frozenset({job.payload_id})


def test_preflight_promotion_and_backup_reject_identity_drift(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    assessment = make_assessment(tmp_path)
    preliminary = replace(
        make_preflight_request(),
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
    )
    store.create_preflight_request(preliminary)
    store.publish_preflight_assessment(REQUEST_ID, assessment)

    with pytest.raises(RequestStoreError, match="preflight request binding"):
        store.publish_preflight_backup_job(
            replace(
                make_preflight_backup_job(),
                preflight_assessment_sha256=assessment.assessment_digest,
                preflight_registry_sha256=assessment.registry_digest,
                preflight_coverage_sha256=assessment.coverage_digest,
                mutation_epoch=5,
            )
        )
    with pytest.raises(RequestStoreError, match="preflight authority"):
        store.promote_preflight_request(
            replace(
                make_backup_job_request(),
                runner_config_sha256="8" * 64,
                preflight_registry_sha256=assessment.registry_digest,
                preflight_coverage_sha256=assessment.coverage_digest,
            )
        )


def test_backup_job_is_immutable_private_and_state_uses_compare_and_swap(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_backup_job_request())
    job = make_backup_job()

    path = store.publish_backup_job(job)
    state = store.read_backup_job_state(REQUEST_ID)
    running = transition_backup_job(
        state,
        LifecycleAction.START_BACKUP,
        updated_at=datetime(2026, 7, 13, 20, 1, tzinfo=UTC),
    )
    store.replace_backup_job_state(running, expected_sequence=0)

    assert path == tmp_path / "requests" / REQUEST_ID / "backup" / "job.json"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert store.read_backup_job(REQUEST_ID) == job
    assert store.read_backup_job_state(REQUEST_ID) == running
    with pytest.raises(RequestStoreError, match="already exists"):
        store.publish_backup_job(job)
    with pytest.raises(RequestStoreError, match="changed concurrently"):
        store.replace_backup_job_state(running, expected_sequence=0)


def test_backup_job_rejects_request_binding_and_cross_job_state(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_backup_job_request())

    with pytest.raises(RequestStoreError, match="immutable request"):
        store.publish_backup_job(replace(make_backup_job(), candidate_tree="d" * 40))

    store.publish_backup_job(make_backup_job())
    wrong = BackupJobState(
        job_id="job-other0000",
        request_id=REQUEST_ID,
        sequence=1,
        updated_at=datetime(2026, 7, 13, 20, 1, tzinfo=UTC),
    )
    with pytest.raises(RequestStoreError, match="immutable envelope"):
        store.replace_backup_job_state(wrong, expected_sequence=0)


@pytest.mark.parametrize("request_id", ["../escape", "short", "UPPERCASE-ID"])
def test_request_store_rejects_untrusted_path_components(
    tmp_path: Path,
    request_id: str,
) -> None:
    store = RequestStore(tmp_path)

    with pytest.raises(RequestStoreError, match="request_id"):
        store.read_request(request_id)
    with pytest.raises(RequestStoreError, match="request_id"):
        store.next_attempt_number(request_id)


def test_read_request_rejects_unknown_or_malformed_schema(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    path = store.create_request(make_request())
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["unknown"] = "unsafe"
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(RequestStoreError, match=r"unknown keys.*unknown"):
        store.read_request(REQUEST_ID)

    payload.pop("unknown")
    payload["schema_version"] = "1"
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    with pytest.raises(RequestStoreError, match="schema_version must be 1"):
        store.read_request(REQUEST_ID)


def test_publish_attempt_envelope_is_private_and_no_replace(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    envelope = make_envelope()

    path = store.publish_attempt_envelope(envelope)

    assert path == tmp_path / "requests" / REQUEST_ID / "attempts" / "1" / "envelope.json"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert list(path.parent.glob(".*.tmp")) == []
    assert store.read_attempt_envelope(REQUEST_ID, 1) == envelope
    with pytest.raises(RequestStoreError, match="already exists"):
        store.publish_attempt_envelope(envelope)


@pytest.mark.parametrize(
    "mutations",
    [
        {"rollout_id": "staging-fedcba9"},
        {"initiating_operator": "devansh"},
        {"initiating_uid": 2003},
        {
            "resolved_sha": "fedcba9876543210fedcba9876543210fedcba98",
            "image_tag": "staging-fedcba9",
        },
        {"fetched_at": "2026-07-13T20:00:02Z"},
        {"runner_config_sha256": "3" * 64},
        {"preflight_attestation_sha256": "6" * 64},
        {"preflight_registry_sha256": "7" * 64},
        {"preflight_coverage_sha256": "8" * 64},
    ],
)
def test_attempt_envelope_must_match_immutable_request_binding(
    tmp_path: Path,
    mutations: dict[str, object],
) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())

    with pytest.raises(RequestStoreError, match="immutable request binding"):
        store.publish_attempt_envelope(replace(make_envelope(), **mutations))

    assert not (tmp_path / "requests" / REQUEST_ID / "attempts").exists()


def test_preview_request_cannot_publish_driver_envelope(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request(status="preview"))

    with pytest.raises(RequestStoreError, match="preview request"):
        store.publish_attempt_envelope(make_envelope())


def test_resume_envelope_reuses_first_attempt_stable_binding(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    request_path = store.create_request(make_request())
    request_before = (request_path.read_bytes(), request_path.stat().st_ino)
    first = make_envelope()
    store.publish_attempt_envelope(first)
    second = replace(
        first,
        attempt_number=2,
        attempt_operator="devansh",
        attempt_uid=2003,
        resume=True,
    )

    store.publish_attempt_envelope(second)

    assert first.rollout_inputs() == second.rollout_inputs()
    assert (request_path.read_bytes(), request_path.stat().st_ino) == request_before

    drifted = replace(
        second,
        attempt_number=3,
        backup_manifest_sha256="3" * 64,
    )
    with pytest.raises(RequestStoreError, match="first attempt binding"):
        store.publish_attempt_envelope(drifted)
    assert not (request_path.parent / "attempts" / "3").exists()


def test_later_attempt_requires_first_attempt_envelope(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())

    with pytest.raises(RequestStoreError, match="first attempt envelope"):
        store.publish_attempt_envelope(
            make_envelope(
                attempt_number=2,
                attempt_operator="devansh",
                attempt_uid=2003,
                resume=True,
            )
        )


def test_attempt_envelope_requires_existing_request(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)

    with pytest.raises(RequestStoreError, match="request does not exist"):
        store.publish_attempt_envelope(make_envelope())
    with pytest.raises(RequestStoreError, match="attempt_number"):
        store.read_attempt_envelope(REQUEST_ID, 0)


def test_next_attempt_number_never_reuses_published_or_reserved_directory(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    assert store.next_attempt_number(REQUEST_ID) == 1

    store.publish_attempt_envelope(make_envelope(attempt_number=1))
    assert store.next_attempt_number(REQUEST_ID) == 2

    reserved = tmp_path / "requests" / REQUEST_ID / "attempts" / "4"
    reserved.mkdir(mode=0o700)
    assert store.next_attempt_number(REQUEST_ID) == 5


def test_read_attempt_envelope_rejects_unknown_schema_data(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    path = store.publish_attempt_envelope(make_envelope())
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["attempt_number"] = "1"
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(RequestStoreError, match="attempt_number must be a positive integer"):
        store.read_attempt_envelope(REQUEST_ID, 1)


def test_clear_active_is_compare_and_delete(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    first = ActivePointer("req-first", 1, "unit-first", "pending")
    second = ActivePointer("req-second", 1, "unit-second", "pending")

    store.set_active(first)

    assert store.clear_active_if_matches(second) is False
    assert store.read_active() == first
    assert store.clear_active_if_matches(first) is True
    assert store.read_active() is None
    assert store.clear_active_if_matches(first) is False


def test_set_active_only_replaces_status_for_the_same_attempt(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    first = ActivePointer("req-first", 1, "unit-first", "pending")
    running = replace(first, status="running")
    different = ActivePointer("req-second", 2, "unit-second", "running")

    first_path = store.set_active(first)
    running_path = store.set_active(running)

    assert first_path == running_path == tmp_path / "active.json"
    assert stat.S_IMODE(running_path.stat().st_mode) == 0o600
    assert store.read_active() == running
    with pytest.raises(RequestStoreError, match="already belongs to another attempt"):
        store.set_active(different)
    assert store.read_active() == running
    assert list(tmp_path.glob(".active.json.*.tmp")) == []


def test_backup_retention_claim_is_exact_idempotent_and_blocks_active_publication(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    digest = "a" * 64
    payload_ids = ("payload-retire02", "payload-retire01")

    path = store.claim_backup_retention(digest, payload_ids)
    repeated = store.claim_backup_retention(
        digest,
        ("payload-retire01", "payload-retire02"),
    )

    assert path == repeated == tmp_path / "backup-retention-claim.json"
    assert store.read_backup_retention_claim() == (
        digest,
        ("payload-retire01", "payload-retire02"),
    )
    with pytest.raises(RequestStoreError, match="another backup retention claim"):
        store.claim_backup_retention("b" * 64, payload_ids)
    with pytest.raises(RequestStoreError, match="retention maintenance"):
        store.set_active(ActivePointer("req-blocked", 1, "unit-blocked", "pending"))
    with pytest.raises(RequestStoreError, match="identity does not match"):
        store.clear_backup_retention_claim("b" * 64)

    assert store.clear_backup_retention_claim(digest) is True
    assert store.clear_backup_retention_claim(digest) is False
    assert store.read_backup_retention_claim() is None
    store.set_active(ActivePointer("req-allowed", 1, "unit-allowed", "pending"))


def test_backup_retention_claim_rejects_existing_active_pointer(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    pointer = ActivePointer("req-active", 1, "unit-active", "pending")
    store.set_active(pointer)

    with pytest.raises(RequestStoreError, match="active rollout blocks"):
        store.claim_backup_retention("a" * 64, ())

    assert store.read_active() == pointer
    assert store.read_backup_retention_claim() is None


def test_preflight_artifact_retention_claim_is_exact_idempotent_and_blocks_active(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    plan_digest = "a" * 64
    bundle_digests = ("c" * 64, "b" * 64)

    path = store.claim_preflight_artifact_retention(plan_digest, bundle_digests)
    repeated = store.claim_preflight_artifact_retention(
        plan_digest,
        tuple(reversed(bundle_digests)),
    )

    assert path == repeated == tmp_path / "preflight-artifact-retention-claim.json"
    assert store.read_preflight_artifact_retention_claim() == (
        plan_digest,
        ("b" * 64, "c" * 64),
    )
    with pytest.raises(RequestStoreError, match="another preflight artifact retention claim"):
        store.claim_preflight_artifact_retention("d" * 64, bundle_digests)
    with pytest.raises(RequestStoreError, match="artifact retention maintenance"):
        store.set_active(ActivePointer("req-blocked", 1, "unit-blocked", "pending"))
    with pytest.raises(RequestStoreError, match="identity does not match"):
        store.clear_preflight_artifact_retention_claim("d" * 64)

    assert store.clear_preflight_artifact_retention_claim(plan_digest) is True
    assert store.clear_preflight_artifact_retention_claim(plan_digest) is False
    assert store.read_preflight_artifact_retention_claim() is None
    store.set_active(ActivePointer("req-allowed", 1, "unit-allowed", "pending"))


def test_preflight_artifact_retention_claim_rejects_active_duplicate_and_unbounded_set(
    tmp_path: Path,
) -> None:
    active_store = RequestStore(tmp_path / "active")
    pointer = ActivePointer("req-active", 1, "unit-active", "pending")
    active_store.set_active(pointer)
    with pytest.raises(RequestStoreError, match="active rollout blocks"):
        active_store.claim_preflight_artifact_retention("a" * 64, ())
    assert active_store.read_active() == pointer
    assert active_store.read_preflight_artifact_retention_claim() is None

    store = RequestStore(tmp_path / "bounds")
    maximum = tuple(f"{index:064x}" for index in range(32))
    store.claim_preflight_artifact_retention("a" * 64, maximum)
    assert store.read_preflight_artifact_retention_claim() == ("a" * 64, maximum)
    assert store.clear_preflight_artifact_retention_claim("a" * 64)
    with pytest.raises(RequestStoreError, match="bundle digests are invalid"):
        store.claim_preflight_artifact_retention("a" * 64, (maximum[0], maximum[0]))
    with pytest.raises(RequestStoreError, match="at most 32"):
        store.claim_preflight_artifact_retention(
            "a" * 64,
            (*maximum, f"{32:064x}"),
        )


def test_preflight_artifact_retirement_receipt_is_no_replace_and_exact(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    bundle_digest = "a" * 64
    plan_digest = "b" * 64
    record_digest = "c" * 64

    assert not store.read_preflight_artifact_retirement_receipt(
        bundle_digest,
        plan_sha256=plan_digest,
        inventory_record_sha256=record_digest,
    )
    path = store.publish_preflight_artifact_retirement_receipt(
        bundle_digest,
        plan_sha256=plan_digest,
        inventory_record_sha256=record_digest,
    )
    repeated = store.publish_preflight_artifact_retirement_receipt(
        bundle_digest,
        plan_sha256=plan_digest,
        inventory_record_sha256=record_digest,
    )

    assert path == repeated == tmp_path / "preflight-artifact-retirements" / f"{bundle_digest}.json"
    assert store.read_preflight_artifact_retirement_receipt(
        bundle_digest,
        plan_sha256=plan_digest,
        inventory_record_sha256=record_digest,
    )
    with pytest.raises(RequestStoreError, match="receipt identity drifted"):
        store.publish_preflight_artifact_retirement_receipt(
            bundle_digest,
            plan_sha256="d" * 64,
            inventory_record_sha256=record_digest,
        )


def test_preflight_artifact_retirement_receipt_recovers_exact_temp_link_residue(
    tmp_path: Path,
) -> None:
    store = RequestStore(tmp_path)
    bundle_digest = "a" * 64
    plan_digest = "b" * 64
    record_digest = "c" * 64
    receipt = store.publish_preflight_artifact_retirement_receipt(
        bundle_digest,
        plan_sha256=plan_digest,
        inventory_record_sha256=record_digest,
    )
    residue = receipt.with_name(f".{receipt.name}.{'f' * 32}.tmp")
    os.link(receipt, residue)

    repeated = store.publish_preflight_artifact_retirement_receipt(
        bundle_digest,
        plan_sha256=plan_digest,
        inventory_record_sha256=record_digest,
    )

    assert repeated == receipt
    assert receipt.stat().st_nlink == 1
    assert not residue.exists()


@pytest.mark.parametrize(
    ("expected_plan", "expected_record"),
    (("d" * 64, "c" * 64), ("b" * 64, "d" * 64)),
)
def test_preflight_artifact_retirement_receipt_rejects_other_authority(
    tmp_path: Path,
    expected_plan: str,
    expected_record: str,
) -> None:
    store = RequestStore(tmp_path)
    bundle_digest = "a" * 64
    store.publish_preflight_artifact_retirement_receipt(
        bundle_digest,
        plan_sha256="b" * 64,
        inventory_record_sha256="c" * 64,
    )

    with pytest.raises(RequestStoreError, match="receipt identity drifted"):
        store.read_preflight_artifact_retirement_receipt(
            bundle_digest,
            plan_sha256=expected_plan,
            inventory_record_sha256=expected_record,
        )


def test_preflight_artifact_retention_authority_rejects_hard_link_aliases(
    tmp_path: Path,
) -> None:
    claim_store = RequestStore(tmp_path / "claim")
    claim = claim_store.claim_preflight_artifact_retention("a" * 64, ("b" * 64,))
    os.link(claim, claim.with_name("claim-alias.json"))
    with pytest.raises(RequestStoreError, match="single-link"):
        claim_store.read_preflight_artifact_retention_claim()

    receipt_store = RequestStore(tmp_path / "receipt")
    receipt = receipt_store.publish_preflight_artifact_retirement_receipt(
        "a" * 64,
        plan_sha256="b" * 64,
        inventory_record_sha256="c" * 64,
    )
    os.link(receipt, receipt.with_name("receipt-alias.json"))
    with pytest.raises(RequestStoreError, match="single-link"):
        receipt_store.read_preflight_artifact_retirement_receipt(
            "a" * 64,
            plan_sha256="b" * 64,
            inventory_record_sha256="c" * 64,
        )


def test_request_and_attempt_inventory_is_exact_sorted_and_typed(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    second_request_id = "stg-20260713-bcdef234"
    store.create_request(make_request(request_id=second_request_id))
    store.create_request(make_request())
    store.publish_attempt_envelope(make_envelope())
    store.publish_attempt_envelope(
        make_envelope(
            attempt_number=2,
            attempt_operator="qianyi",
            attempt_uid=2003,
            resume=True,
        )
    )

    assert store.request_ids() == (REQUEST_ID, second_request_id)
    assert store.attempt_numbers(REQUEST_ID) == (1, 2)
    assert store.attempt_numbers(second_request_id) == ()


@pytest.mark.parametrize("unsafe_kind", ["unknown-file", "symlink"])
def test_request_inventory_rejects_unknown_or_aliased_entry(
    tmp_path: Path,
    unsafe_kind: str,
) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    unsafe = tmp_path / "requests" / "unknown-entry"
    if unsafe_kind == "unknown-file":
        unsafe.write_text("unknown\n")
    else:
        unsafe.symlink_to(tmp_path / "outside")

    with pytest.raises(RequestStoreError, match="unsafe entry"):
        store.request_ids()


def test_request_inventory_rejects_malformed_typed_identity(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    path = store.create_request(make_request())
    path.write_text('{"schema_version":1}\n', encoding="utf-8")

    with pytest.raises(RequestStoreError, match="missing keys"):
        store.request_ids()


def test_request_inventory_binds_scan_to_validated_directory_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RequestStore(tmp_path / "store")
    second_request_id = "stg-20260713-bcdef234"
    store.create_request(make_request())
    store.create_request(make_request(request_id=second_request_id))
    redirected = RequestStore(tmp_path / "redirected")
    redirected.create_request(make_request())
    original_scandir = store_module.os.scandir

    def redirect_path_scan(path):  # type: ignore[no-untyped-def]
        return original_scandir(redirected.requests_root if path == store.requests_root else path)

    monkeypatch.setattr(store_module.os, "scandir", redirect_path_scan)

    assert store.request_ids() == (REQUEST_ID, second_request_id)


def test_attempt_inventory_binds_scan_to_validated_directory_descriptor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = RequestStore(tmp_path / "store")
    store.create_request(make_request())
    store.publish_attempt_envelope(make_envelope())
    store.publish_attempt_envelope(
        make_envelope(
            attempt_number=2,
            attempt_operator="qianyi",
            attempt_uid=2003,
            resume=True,
        )
    )
    redirected = RequestStore(tmp_path / "redirected")
    redirected.create_request(make_request())
    redirected.publish_attempt_envelope(make_envelope())
    attempts = store.requests_root / REQUEST_ID / "attempts"
    redirected_attempts = redirected.requests_root / REQUEST_ID / "attempts"
    original_scandir = store_module.os.scandir

    def redirect_path_scan(path):  # type: ignore[no-untyped-def]
        return original_scandir(redirected_attempts if path == attempts else path)

    monkeypatch.setattr(store_module.os, "scandir", redirect_path_scan)

    assert store.attempt_numbers(REQUEST_ID) == (1, 2)


def test_attempt_inventory_rejects_gap_and_missing_envelope(tmp_path: Path) -> None:
    gap_store = RequestStore(tmp_path / "gap")
    gap_store.create_request(make_request())
    gap_store.publish_attempt_envelope(make_envelope())
    attempt_three = gap_store.requests_root / REQUEST_ID / "attempts" / "3"
    attempt_three.mkdir(mode=0o700)
    with pytest.raises(RequestStoreError, match="consecutive"):
        gap_store.attempt_numbers(REQUEST_ID)

    missing_store = RequestStore(tmp_path / "missing")
    missing_store.create_request(make_request())
    missing = missing_store.requests_root / REQUEST_ID / "attempts" / "1"
    missing.mkdir(parents=True, mode=0o700)
    missing.parent.chmod(0o700)
    with pytest.raises(RequestStoreError, match="envelope"):
        missing_store.attempt_numbers(REQUEST_ID)


def test_attempt_inventory_rejects_unknown_entry(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    store.publish_attempt_envelope(make_envelope())
    (store.requests_root / REQUEST_ID / "attempts" / "notes").write_text("unsafe\n")

    with pytest.raises(RequestStoreError, match="unsafe entry"):
        store.attempt_numbers(REQUEST_ID)


def test_concurrent_active_reservation_has_exactly_one_winner(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    seed = ActivePointer("req-seed", 1, "unit-seed", "pending")
    store.set_active(seed)
    assert store.clear_active_if_matches(seed) is True

    context = get_context("spawn")
    ready_queue = context.Queue()
    result_queue = context.Queue()
    start_event = context.Event()
    pointers = [
        ActivePointer("req-first", 1, "unit-first", "pending"),
        ActivePointer("req-second", 1, "unit-second", "pending"),
    ]
    processes = [
        context.Process(
            target=_set_active_concurrently,
            args=(
                str(tmp_path),
                pointer.to_dict(),
                ready_queue,
                start_event,
                result_queue,
            ),
        )
        for pointer in pointers
    ]

    try:
        for process in processes:
            process.start()
        assert {ready_queue.get(timeout=5) for _ in processes} == {
            pointer.request_id for pointer in pointers
        }
        start_event.set()
        results = [result_queue.get(timeout=5) for _ in processes]
    finally:
        start_event.set()
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

    assert all(process.exitcode == 0 for process in processes)
    successes = [result for result in results if result[0] == "ok"]
    failures = [result for result in results if result[0] == "error"]
    assert len(successes) == len(failures) == 1
    assert "already belongs to another attempt" in failures[0][2]
    active = store.read_active()
    assert active is not None
    assert active.request_id == successes[0][1]


def test_read_active_rejects_unknown_literal(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    path = store.set_active(ActivePointer("req-first", 1, "unit-first", "pending"))
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["status"] = "done"
    path.write_text(json.dumps(payload) + "\n", encoding="utf-8")

    with pytest.raises(RequestStoreError, match="unknown active status"):
        store.read_active()


def test_append_event_is_private_fsynced_jsonl_and_roundtrips(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    requested = make_event()
    running = make_event(
        event="attempt_running",
        occurred_at="2026-07-13T20:02:00Z",
        attempt_number=1,
        status="running",
    )

    path = store.append_event(requested)
    assert store.append_event(running) == path

    assert path == tmp_path / "requests" / REQUEST_ID / "events.jsonl"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["event"] == "requested"
    assert json.loads(lines[0])["status"] == "pending"
    assert json.loads(lines[0])["schema_version"] == 1
    assert store.read_events(REQUEST_ID) == [requested, running]


def test_append_event_requires_immutable_request_record(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)

    with pytest.raises(RequestStoreError, match="request does not exist"):
        store.append_event(make_event())


def test_concurrent_event_appends_produce_only_complete_json_lines(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    events = [
        make_event(
            event="attempt_running",
            occurred_at=f"2026-07-13T20:02:{index:02d}Z",
            attempt_number=index + 1,
            status="running",
        )
        for index in range(20)
    ]

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(store.append_event, events))

    loaded = store.read_events(REQUEST_ID)
    assert len(loaded) == len(events)
    assert {event.occurred_at for event in loaded} == {event.occurred_at for event in events}


def test_read_events_waits_for_exclusive_append_lock(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    store.append_event(make_event())
    second = make_event(
        event="attempt_running",
        occurred_at="2026-07-13T20:02:00Z",
        attempt_number=1,
        status="running",
    )
    payload = (json.dumps(second.to_dict(), separators=(",", ":")) + "\n").encode()

    context = get_context("spawn")
    first_half_written = context.Event()
    release_writer = context.Event()
    events_path = tmp_path / "requests" / REQUEST_ID / "events.jsonl"
    process = context.Process(
        target=_append_event_in_two_locked_writes,
        args=(str(events_path), payload, first_half_written, release_writer),
    )
    process.start()
    try:
        assert first_half_written.wait(timeout=5)
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(store.read_events, REQUEST_ID)
            try:
                with pytest.raises(FutureTimeoutError):
                    future.result(timeout=0.2)
            finally:
                release_writer.set()
            assert future.result(timeout=5) == [make_event(), second]
    finally:
        release_writer.set()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)

    assert process.exitcode == 0


def test_read_events_rejects_unknown_or_non_object_lines(tmp_path: Path) -> None:
    store = RequestStore(tmp_path)
    store.create_request(make_request())
    path = store.append_event(make_event())
    path.write_text("[]\n", encoding="utf-8")

    with pytest.raises(RequestStoreError, match="line 1 must be a JSON object"):
        store.read_events(REQUEST_ID)

    valid = json.dumps(make_event().to_dict(), separators=(",", ":"))
    path.write_text(f"{valid}\nnot-json\n", encoding="utf-8")
    with pytest.raises(RequestStoreError, match=r"events\.jsonl line 2"):
        store.read_events(REQUEST_ID)


def test_read_paths_reject_symlinked_store_root(tmp_path: Path) -> None:
    real_root = tmp_path / "real-store"
    real = RequestStore(real_root)
    real.create_request(make_request())
    real.set_active(ActivePointer(REQUEST_ID, 1, "unit-first", "pending"))
    alias = tmp_path / "store-alias"
    alias.symlink_to(real_root, target_is_directory=True)
    through_alias = RequestStore(alias)

    with pytest.raises(RequestStoreError, match=r"request store root.*symlink"):
        through_alias.read_request(REQUEST_ID)
    with pytest.raises(RequestStoreError, match=r"request store root.*symlink"):
        through_alias.read_active()


def test_read_paths_reject_replaced_requests_directory(tmp_path: Path) -> None:
    store = RequestStore(tmp_path / "store")
    store.create_request(make_request())
    requests_root = store.requests_root
    moved = store.root / "moved-requests"
    requests_root.rename(moved)
    requests_root.symlink_to(moved, target_is_directory=True)

    with pytest.raises(RequestStoreError, match=r"requests directory.*symlink"):
        store.read_request(REQUEST_ID)


def test_optional_store_entries_reject_broken_symlinks(tmp_path: Path) -> None:
    store = RequestStore(tmp_path / "store")
    store.create_request(make_request())
    request_directory = store.requests_root / REQUEST_ID

    events_path = request_directory / "events.jsonl"
    events_path.symlink_to(request_directory / "missing-events.jsonl")
    with pytest.raises(RequestStoreError, match=r"events\.jsonl"):
        store.read_events(REQUEST_ID)

    attempts_path = request_directory / "attempts"
    attempts_path.symlink_to(request_directory / "missing-attempts", target_is_directory=True)
    with pytest.raises(RequestStoreError, match=r"attempts directory.*symlink"):
        store.next_attempt_number(REQUEST_ID)
