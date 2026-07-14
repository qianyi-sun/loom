from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_cli.rollout_lock import (
    RolloutAttribution,
    RolloutLeaseError,
    RolloutLeaseManager,
)

NOW = datetime(2026, 7, 2, 16, 0, 0, tzinfo=UTC)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


ATTRIBUTION = RolloutAttribution(
    request_id="request-20260713-hongjian",
    initiating_operator="hongjian",
    initiating_uid=2011,
    attempt_number=2,
    attempt_operator="devansh",
    attempt_uid=2501,
)


def test_attributed_acquire_and_release_persist_all_six_fields(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)

    lease = manager.acquire(
        environment="staging",
        owner_id="rollout-staging-d46a16c",
        ttl_seconds=3600,
        command=[
            "loom",
            "cluster",
            "up",
            "--rollout-request-envelope",
            "/run/loom-rollout/private/attempt-2.json",
        ],
        evidence_path=evidence_path,
        attribution=ATTRIBUTION,
    )

    expected = ATTRIBUTION.to_dict()
    assert lease.attribution == ATTRIBUTION
    active = _read_json(tmp_path / "staging.lock")
    assert {field: active[field] for field in expected} == expected
    assert active["command"] == ["broker-attributed-rollout-mutation"]
    acquired = _read_json(evidence_path)["events"][0]
    assert {field: acquired[field] for field in expected} == expected

    lease.release(status="released")

    released = _read_json(tmp_path / "staging.lock")
    assert {field: released[field] for field in expected} == expected
    release_event = _read_json(evidence_path)["events"][1]
    assert {field: release_event[field] for field in expected} == expected
    serialized = json.dumps({"active": released, "event": release_event})
    assert "/run/loom-rollout/private/attempt-2.json" not in serialized


@pytest.mark.parametrize(
    ("field", "invalid"),
    [
        ("request_id", "short"),
        ("request_id", None),
        ("initiating_operator", "Hongjian Invalid"),
        ("initiating_operator", None),
        ("initiating_uid", True),
        ("initiating_uid", -1),
        ("initiating_uid", "2011"),
        ("attempt_number", True),
        ("attempt_number", 0),
        ("attempt_number", "2"),
        ("attempt_operator", "Devansh Invalid"),
        ("attempt_operator", None),
        ("attempt_uid", True),
        ("attempt_uid", -1),
        ("attempt_uid", "2501"),
    ],
)
def test_rollout_attribution_rejects_invalid_field_types_and_values(
    field: str,
    invalid: object,
) -> None:
    with pytest.raises(ValueError, match=field):
        replace(ATTRIBUTION, **{field: invalid})


def test_acquire_writes_active_lease_and_release_evidence(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)

    lease = manager.acquire(
        environment="staging",
        owner_id="rollout-staging-d46a16c",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
        evidence_path=evidence_path,
    )

    active = _read_json(tmp_path / "staging.lock")
    assert active["environment"] == "staging"
    assert active["owner_id"] == "rollout-staging-d46a16c"
    assert active["expires_at"] == "2026-07-02T17:00:00+00:00"
    assert active["command"] == ["loom", "cluster", "up"]
    assert set(ATTRIBUTION.to_dict()).isdisjoint(active)
    assert _read_json(evidence_path)["events"][0]["event"] == "acquired"

    lease.release(status="released")

    released = _read_json(tmp_path / "staging.lock")
    assert released["release_status"] == "released"
    assert released["released_at"]
    evidence = _read_json(evidence_path)
    assert [event["event"] for event in evidence["events"]] == [
        "acquired",
        "released",
    ]
    assert evidence["events"][1]["status"] == "released"
    assert all(set(ATTRIBUTION.to_dict()).isdisjoint(event) for event in evidence["events"])


def test_same_environment_contention_reports_active_owner(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)
    manager.acquire(
        environment="staging",
        owner_id="owner-a",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    with pytest.raises(RolloutLeaseError) as exc_info:
        manager.acquire(
            environment="staging",
            owner_id="owner-b",
            ttl_seconds=3600,
            command=["loom", "admin", "environment-state", "apply"],
        )

    assert exc_info.value.diagnostic["environment"] == "staging"
    assert exc_info.value.diagnostic["active_owner_id"] == "owner-a"
    assert exc_info.value.diagnostic["reason"] == "active_rollout_lease"
    assert "owner-a" in str(exc_info.value)


def test_contention_diagnostic_does_not_echo_legacy_command_values(
    tmp_path: Path,
) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)
    sensitive_source = "file:/shared_work/private/staging-admin-token"
    lease = manager.acquire(
        environment="staging",
        owner_id="legacy-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up", "--admin-token", sensitive_source],
    )

    with pytest.raises(RolloutLeaseError) as exc_info:
        manager.acquire(
            environment="staging",
            owner_id="broker-owner",
            ttl_seconds=3600,
            command=["loom", "cluster", "up"],
            attribution=ATTRIBUTION,
        )

    serialized = json.dumps(exc_info.value.diagnostic, sort_keys=True)
    assert sensitive_source not in serialized
    assert "active_command" not in exc_info.value.diagnostic
    lease.release()


def test_different_environments_acquire_independent_leases(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)

    staging = manager.acquire(
        environment="staging",
        owner_id="staging-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )
    production = manager.acquire(
        environment="production",
        owner_id="production-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    assert staging.lock_path == tmp_path / "staging.lock"
    assert production.lock_path == tmp_path / "production.lock"
    assert staging.lock_path.exists()
    assert production.lock_path.exists()


def test_expired_active_lease_still_blocks_until_owner_releases(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)
    lease = manager.acquire(
        environment="staging",
        owner_id="slow-owner",
        ttl_seconds=60,
        command=["loom", "cluster", "up"],
    )
    later = RolloutLeaseManager(
        tmp_path,
        now=lambda: NOW + timedelta(minutes=2),
    )

    with pytest.raises(RolloutLeaseError) as exc_info:
        later.acquire(
            environment="staging",
            owner_id="fresh-owner",
            ttl_seconds=3600,
            command=["loom", "cluster", "up"],
        )

    assert exc_info.value.diagnostic["active_owner_id"] == "slow-owner"
    assert exc_info.value.diagnostic["reason"] == "active_rollout_lease"
    lease.release()


def test_abandoned_stale_record_can_be_replaced_with_diagnostic(tmp_path: Path) -> None:
    lock_file = tmp_path / "staging.lock"
    lock_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "environment": "staging",
                "owner_id": "stale-owner",
                "acquired_at": (NOW - timedelta(hours=2)).isoformat(),
                "expires_at": (NOW - timedelta(hours=1)).isoformat(),
                "command": ["loom", "cluster", "up"],
            }
        ),
        encoding="utf-8",
    )

    replacement = RolloutLeaseManager(tmp_path, now=lambda: NOW).acquire(
        environment="staging",
        owner_id="fresh-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    assert replacement.stale_owner_id == "stale-owner"
    active = _read_json(tmp_path / "staging.lock")
    assert active["owner_id"] == "fresh-owner"
    assert active["replaced_stale_owner_id"] == "stale-owner"
    replacement.release()


def test_force_does_not_bypass_active_process_lock(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)
    lease = manager.acquire(
        environment="staging",
        owner_id="owner-a",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    with pytest.raises(RolloutLeaseError) as exc_info:
        manager.acquire(
            environment="staging",
            owner_id="owner-b",
            ttl_seconds=3600,
            command=["loom", "cluster", "up"],
            force=True,
        )

    assert exc_info.value.diagnostic["active_owner_id"] == "owner-a"
    lease.release()


def test_evidence_write_failure_releases_process_lock(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence-is-a-directory"
    evidence_path.mkdir()
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)

    with pytest.raises(ValueError, match="could not write rollout lock evidence"):
        manager.acquire(
            environment="staging",
            owner_id="owner-a",
            ttl_seconds=3600,
            command=["loom", "cluster", "up"],
            evidence_path=evidence_path,
        )

    lease = manager.acquire(
        environment="staging",
        owner_id="owner-b",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )
    assert lease.owner_id == "owner-b"
    lease.release()
