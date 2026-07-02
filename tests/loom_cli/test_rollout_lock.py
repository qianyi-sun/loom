from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from loom_cli.rollout_lock import RolloutLeaseError, RolloutLeaseManager

NOW = datetime(2026, 7, 2, 16, 0, 0, tzinfo=UTC)


def _read_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_acquire_writes_active_lease_and_release_evidence(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)

    lease = manager.acquire(
        environment="public-beta",
        owner_id="rollout-public-beta-d46a16c",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
        evidence_path=evidence_path,
    )

    active = _read_json(tmp_path / "public-beta.lock")
    assert active["environment"] == "public-beta"
    assert active["owner_id"] == "rollout-public-beta-d46a16c"
    assert active["expires_at"] == "2026-07-02T17:00:00+00:00"
    assert active["command"] == ["loom", "cluster", "up"]
    assert _read_json(evidence_path)["events"][0]["event"] == "acquired"

    lease.release(status="released")

    released = _read_json(tmp_path / "public-beta.lock")
    assert released["release_status"] == "released"
    assert released["released_at"]
    evidence = _read_json(evidence_path)
    assert [event["event"] for event in evidence["events"]] == [
        "acquired",
        "released",
    ]
    assert evidence["events"][1]["status"] == "released"


def test_same_environment_contention_reports_active_owner(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)
    manager.acquire(
        environment="public-beta",
        owner_id="owner-a",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    with pytest.raises(RolloutLeaseError) as exc_info:
        manager.acquire(
            environment="public-beta",
            owner_id="owner-b",
            ttl_seconds=3600,
            command=["loom", "admin", "environment-state", "apply"],
        )

    assert exc_info.value.diagnostic["environment"] == "public-beta"
    assert exc_info.value.diagnostic["active_owner_id"] == "owner-a"
    assert exc_info.value.diagnostic["reason"] == "active_rollout_lease"
    assert "owner-a" in str(exc_info.value)


def test_different_environments_acquire_independent_leases(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)

    public_beta = manager.acquire(
        environment="public-beta",
        owner_id="public-beta-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )
    staging = manager.acquire(
        environment="staging",
        owner_id="staging-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    assert public_beta.lock_path == tmp_path / "public-beta.lock"
    assert staging.lock_path == tmp_path / "staging.lock"
    assert public_beta.lock_path.exists()
    assert staging.lock_path.exists()


def test_expired_active_lease_still_blocks_until_owner_releases(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)
    lease = manager.acquire(
        environment="public-beta",
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
            environment="public-beta",
            owner_id="fresh-owner",
            ttl_seconds=3600,
            command=["loom", "cluster", "up"],
        )

    assert exc_info.value.diagnostic["active_owner_id"] == "slow-owner"
    assert exc_info.value.diagnostic["reason"] == "active_rollout_lease"
    lease.release()


def test_abandoned_stale_record_can_be_replaced_with_diagnostic(tmp_path: Path) -> None:
    lock_file = tmp_path / "public-beta.lock"
    lock_file.write_text(
        json.dumps({
            "schema_version": 1,
            "environment": "public-beta",
            "owner_id": "stale-owner",
            "acquired_at": (NOW - timedelta(hours=2)).isoformat(),
            "expires_at": (NOW - timedelta(hours=1)).isoformat(),
            "command": ["loom", "cluster", "up"],
        }),
        encoding="utf-8",
    )

    replacement = RolloutLeaseManager(tmp_path, now=lambda: NOW).acquire(
        environment="public-beta",
        owner_id="fresh-owner",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    assert replacement.stale_owner_id == "stale-owner"
    active = _read_json(tmp_path / "public-beta.lock")
    assert active["owner_id"] == "fresh-owner"
    assert active["replaced_stale_owner_id"] == "stale-owner"
    replacement.release()


def test_force_does_not_bypass_active_process_lock(tmp_path: Path) -> None:
    manager = RolloutLeaseManager(tmp_path, now=lambda: NOW)
    lease = manager.acquire(
        environment="public-beta",
        owner_id="owner-a",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )

    with pytest.raises(RolloutLeaseError) as exc_info:
        manager.acquire(
            environment="public-beta",
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
            environment="public-beta",
            owner_id="owner-a",
            ttl_seconds=3600,
            command=["loom", "cluster", "up"],
            evidence_path=evidence_path,
        )

    lease = manager.acquire(
        environment="public-beta",
        owner_id="owner-b",
        ttl_seconds=3600,
        command=["loom", "cluster", "up"],
    )
    assert lease.owner_id == "owner-b"
    lease.release()
