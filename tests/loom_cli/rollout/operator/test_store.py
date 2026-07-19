from __future__ import annotations

import fcntl
import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import replace
from multiprocessing import get_context
from pathlib import Path
from typing import Any

import pytest

from loom_cli.rollout.operator.config import APPROVED_REMOTE_URL
from loom_cli.rollout.operator.model import (
    ActivePointer,
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    RequestEvent,
    RolloutRequest,
)
from loom_cli.rollout.operator.store import RequestStore, RequestStoreError

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
