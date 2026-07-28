from __future__ import annotations

import json
import os
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path

import jsonschema
import pytest

from loom_control_plane.shared_capacity_broker import (
    BrokerBudgets,
    BrokerError,
    LeaseObservation,
    SandboxId,
    SharedCapacityBroker,
    main,
)

SHA_A = "a" * 40
SHA_B = "b" * 40
SHA_C = "c" * 40


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 7, 27, 20, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **kwargs: int) -> None:
        self.now += timedelta(**kwargs)


def _broker(tmp_path: Path, clock: Clock) -> SharedCapacityBroker:
    return SharedCapacityBroker(tmp_path / "broker.sqlite3", clock=clock)


def _request(
    broker: SharedCapacityBroker,
    sandbox: SandboxId,
    *,
    candidate_sha: str = SHA_A,
    pool: str = "gb10",
    min_slots: int = 0,
    target_slots: int = 10,
    ttl_seconds: int = 3600,
) -> str:
    request, _ = broker.request_capacity(
        sandbox=sandbox,
        candidate_sha=candidate_sha,
        pool=pool,
        min_slots=min_slots,
        target_slots=target_slots,
        ttl_seconds=ttl_seconds,
        purpose="large-batch-runtime-validation",
        preemptible=True,
        idempotency_key=f"{sandbox.value}:{pool}:{candidate_sha}",
    )
    return request.id


def _budgets(
    *,
    global_slots: int,
    pools: dict[str, int],
    global_pending_slots: int | None = None,
    pool_pending_slots: dict[str, int] | None = None,
) -> BrokerBudgets:
    return BrokerBudgets(
        global_slots=global_slots,
        pool_slots=pools,
        global_pending_slots=(
            global_slots if global_pending_slots is None else global_pending_slots
        ),
        pool_pending_slots=pool_pending_slots or pools,
    )


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("sandbox", "unknown", "allowlist"),
        ("candidate_sha", "abc1234", "40-hex"),
        ("pool", "../gb10", "identifier"),
        ("target_slots", 10_001, "10000"),
        ("ttl_seconds", 0, "60..86400"),
        ("purpose", "token=do-not-store", "secret-free"),
        ("idempotency_key", "token-do-not-store", "secret-like"),
    ],
)
def test_request_schema_fails_closed(
    tmp_path: Path,
    field: str,
    value: object,
    message: str,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    kwargs: dict[str, object] = {
        "sandbox": SandboxId.QIANYI,
        "candidate_sha": SHA_A,
        "pool": "gb10",
        "min_slots": 2,
        "target_slots": 10,
        "ttl_seconds": 3600,
        "purpose": "large-batch-runtime-validation",
        "preemptible": True,
        "idempotency_key": "request-1",
    }
    kwargs[field] = value
    with pytest.raises(BrokerError, match=message):
        broker.request_capacity(**kwargs)  # type: ignore[arg-type]


def test_state_authority_is_owner_only_and_sidecars_are_not_world_readable(
    tmp_path: Path,
) -> None:
    clock = Clock()
    authority = tmp_path / "authority"
    broker = SharedCapacityBroker(authority / "broker.sqlite3", clock=clock)
    broker.initialize()
    assert stat.S_IMODE(authority.stat().st_mode) == 0o700
    assert stat.S_IMODE(broker.state_db.stat().st_mode) == 0o600

    _request(broker, SandboxId.QIANYI)
    broker.reconcile(_budgets(global_slots=2, pools={"gb10": 2}))
    for path in broker._sqlite_sidecar_paths():
        if path.exists():
            assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_transient_sqlite_sidecar_can_disappear_during_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    transient = broker.state_db.with_name("transient-sidecar")
    transient.write_bytes(b"")
    transient.chmod(0o600)
    original_sidecars = broker._sqlite_sidecar_paths()
    monkeypatch.setattr(
        broker,
        "_sqlite_sidecar_paths",
        lambda: (*original_sidecars, transient),
    )
    real_lstat = os.lstat
    disappeared = False

    def disappearing_lstat(path: os.PathLike[str] | str) -> os.stat_result:
        nonlocal disappeared
        if Path(path) == transient and not disappeared:
            disappeared = True
            transient.unlink()
            raise FileNotFoundError(path)
        return real_lstat(path)

    monkeypatch.setattr(os, "lstat", disappearing_lstat)

    assert broker.status()["aggregate"]["granted_slots"] == 0
    assert disappeared is True


def test_state_authority_rejects_unsafe_parent_and_symlink_db(
    tmp_path: Path,
) -> None:
    clock = Clock()
    unsafe_parent = tmp_path / "unsafe"
    unsafe_parent.mkdir(mode=0o755)
    unsafe_parent.chmod(0o755)
    with pytest.raises(BrokerError, match="0700"):
        SharedCapacityBroker(unsafe_parent / "broker.sqlite3", clock=clock).initialize()

    real_authority = tmp_path / "real-authority"
    real_authority.mkdir(mode=0o700)
    real_authority.chmod(0o700)
    linked_authority = tmp_path / "linked-authority"
    os.symlink(real_authority, linked_authority)
    with pytest.raises(BrokerError, match="0700"):
        SharedCapacityBroker(linked_authority / "broker.sqlite3", clock=clock).initialize()

    authority = tmp_path / "authority"
    authority.mkdir(mode=0o700)
    authority.chmod(0o700)
    target = authority / "target.sqlite3"
    target.write_bytes(b"")
    target.chmod(0o600)
    symlink = authority / "broker.sqlite3"
    os.symlink(target, symlink)
    with pytest.raises(BrokerError, match="unsafe"):
        SharedCapacityBroker(symlink, clock=clock).initialize()


def test_request_is_idempotent_but_key_cannot_be_rebound(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    first_id = _request(broker, SandboxId.QIANYI)
    second_id = _request(broker, SandboxId.QIANYI)
    assert second_id == first_id

    with pytest.raises(BrokerError, match="already bound"):
        broker.request_capacity(
            sandbox=SandboxId.QIANYI,
            candidate_sha=SHA_B,
            pool="gb10",
            min_slots=0,
            target_slots=10,
            ttl_seconds=3600,
            purpose="large-batch-runtime-validation",
            preemptible=True,
            idempotency_key=f"qianyi:gb10:{SHA_A}",
        )


def test_three_sandboxes_share_minimum_and_burst_without_overshoot(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    _request(broker, SandboxId.QIANYI, candidate_sha=SHA_A, min_slots=2)
    _request(broker, SandboxId.HONGJIAN, candidate_sha=SHA_B, min_slots=2)
    _request(broker, SandboxId.DEVANSH, candidate_sha=SHA_C, min_slots=2)

    report = broker.reconcile(_budgets(global_slots=12, pools={"gb10": 12}))

    grants = {
        item["request"]["sandbox"]: item["lease"]["granted_slots"]
        for item in report["requests"]  # type: ignore[union-attr]
    }
    assert grants == {"qianyi": 4, "hongjian": 4, "devansh": 4}
    assert report["aggregate"] == {
        "requested_slots": 30,
        "granted_slots": 12,
        "active_slots": 0,
        "pending_slots": 12,
        "draining_slots": 0,
        "terminal_slots": 0,
        "committed_slots": 12,
    }
    assert sum(grants.values()) <= 12
    assert all(
        handoff["max_slots"] == 4
        and handoff["min_slots"] == 0
        and handoff["environment"].startswith("sandbox-")
        and handoff["candidate_sha"] in {SHA_A, SHA_B, SHA_C}
        for handoff in report["handoffs"]  # type: ignore[union-attr]
    )


def test_global_pool_and_pending_slot_budgets_all_clamp_grants(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    _request(broker, SandboxId.QIANYI, pool="gb10", candidate_sha=SHA_A)
    _request(broker, SandboxId.HONGJIAN, pool="oldlab", candidate_sha=SHA_B)

    report = broker.reconcile(
        _budgets(
            global_slots=7,
            pools={"gb10": 6, "oldlab": 6},
            global_pending_slots=5,
            pool_pending_slots={"gb10": 4, "oldlab": 3},
        ),
    )

    assert report["aggregate"]["granted_slots"] == 5  # type: ignore[index]
    assert report["aggregate"]["pending_slots"] == 5  # type: ignore[index]
    grants = {
        item["request"]["pool"]: item["lease"]["granted_slots"]
        for item in report["requests"]  # type: ignore[union-attr]
    }
    assert grants["gb10"] <= 4
    assert grants["oldlab"] <= 3
    assert sum(grants.values()) <= 7


def test_one_active_sandbox_can_burst_to_the_available_target(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    _request(broker, SandboxId.QIANYI, target_slots=140)
    report = broker.reconcile(
        _budgets(
            global_slots=140,
            pools={"gb10": 140},
            global_pending_slots=140,
        ),
    )
    assert report["aggregate"]["granted_slots"] == 140  # type: ignore[index]
    assert report["requests"][0]["lease"]["granted_slots"] == 140  # type: ignore[index]


def test_fair_turn_ages_all_three_sandboxes_through_one_slot(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    ids = {
        SandboxId.QIANYI: _request(broker, SandboxId.QIANYI, candidate_sha=SHA_A),
        SandboxId.HONGJIAN: _request(broker, SandboxId.HONGJIAN, candidate_sha=SHA_B),
        SandboxId.DEVANSH: _request(broker, SandboxId.DEVANSH, candidate_sha=SHA_C),
    }
    budget = _budgets(global_slots=1, pools={"gb10": 1})
    served: list[str] = []
    report = broker.reconcile(budget)

    for turn in range(3):
        holder = next(
            item
            for item in report["requests"]  # type: ignore[union-attr]
            if item["lease"]["granted_slots"] == 1
        )
        served.append(holder["request"]["sandbox"])
        holder_id = holder["request"]["id"]

        # The next fair turn first reduces this grant. Capacity is not reused
        # until the sandbox reports that the old epoch has fully drained.
        if turn == 2:
            break
        reduced = broker.reconcile(budget)
        reduced_holder = next(
            item
            for item in reduced["requests"]  # type: ignore[union-attr]
            if item["request"]["id"] == holder_id
        )
        assert reduced_holder["lease"]["granted_slots"] == 0
        assert reduced["aggregate"]["committed_slots"] == 1  # type: ignore[index]
        assert reduced["aggregate"]["granted_slots"] == 0  # type: ignore[index]
        epoch = reduced_holder["lease"]["lease_epoch"]
        report = broker.reconcile(
            budget,
            observations=[
                LeaseObservation(
                    request_id=holder_id,
                    lease_epoch=epoch,
                    pending_slots=0,
                    active_slots=0,
                    draining_slots=0,
                    terminal_slots=1,
                ),
            ],
        )

    assert served == [
        SandboxId.DEVANSH.value,
        SandboxId.HONGJIAN.value,
        SandboxId.QIANYI.value,
    ]
    assert set(served) == {sandbox.value for sandbox in ids}


def test_cancel_is_drain_first_and_terminal_only_after_observation(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    request_id = _request(broker, SandboxId.QIANYI, target_slots=4)
    budget = _budgets(global_slots=4, pools={"gb10": 4})
    broker.reconcile(budget)

    cancelled = broker.cancel(request_id)
    assert cancelled["request"]["state"] == "draining"  # type: ignore[index]
    assert cancelled["lease"]["granted_slots"] == 0  # type: ignore[index]
    assert cancelled["lease"]["pending_slots"] == 4  # type: ignore[index]
    epoch = cancelled["lease"]["lease_epoch"]  # type: ignore[index]

    report = broker.reconcile(
        budget,
        observations=[
            LeaseObservation(
                request_id=request_id,
                lease_epoch=epoch,
                pending_slots=0,
                active_slots=0,
                draining_slots=0,
                terminal_slots=4,
            ),
        ],
    )
    record = report["requests"][0]  # type: ignore[index]
    assert record["request"]["state"] == "terminal"
    assert record["lease"]["terminal_slots"] == 4
    assert report["aggregate"]["committed_slots"] == 0  # type: ignore[index]
    handoff = report["handoffs"][0]  # type: ignore[index]
    assert handoff["enabled"] is False
    assert handoff["max_slots"] == 0


def test_ttl_expiry_emits_zero_grant_handoff_and_audit(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    _request(broker, SandboxId.QIANYI, ttl_seconds=60)
    budget = _budgets(global_slots=2, pools={"gb10": 2})
    broker.reconcile(budget)
    clock.advance(seconds=61)

    report = broker.reconcile(budget)
    record = report["requests"][0]  # type: ignore[index]
    assert record["request"]["terminal_reason"] == "ttl_expired"
    assert record["request"]["state"] == "draining"
    assert record["lease"]["granted_slots"] == 0
    assert any(event["event_type"] == "ttl_expired" for event in report["audit"])  # type: ignore[union-attr]


def test_observation_is_epoch_fenced_and_cannot_expand_commitment(
    tmp_path: Path,
) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    request_id = _request(broker, SandboxId.QIANYI, target_slots=2)
    report = broker.reconcile(_budgets(global_slots=2, pools={"gb10": 2}))
    epoch = report["requests"][0]["lease"]["lease_epoch"]  # type: ignore[index]

    with pytest.raises(BrokerError, match="stale"):
        broker.reconcile(
            _budgets(global_slots=2, pools={"gb10": 2}),
            observations=[
                LeaseObservation(request_id, epoch - 1, 2, 0, 0, 0),
            ],
        )
    with pytest.raises(BrokerError, match="exceeds"):
        broker.reconcile(
            _budgets(global_slots=2, pools={"gb10": 2}),
            observations=[
                LeaseObservation(request_id, epoch, 0, 3, 0, 0),
            ],
        )


def test_two_broker_instances_share_one_persistent_authority(
    tmp_path: Path,
) -> None:
    clock = Clock()
    first = _broker(tmp_path, clock)
    request_id = _request(first, SandboxId.QIANYI)
    second = _broker(tmp_path, clock)
    report = second.reconcile(_budgets(global_slots=3, pools={"gb10": 3}))
    assert report["requests"][0]["request"]["id"] == request_id  # type: ignore[index]
    assert first.status()["aggregate"]["granted_slots"] == 3  # type: ignore[index]


def test_cli_surface_emits_secret_free_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_db = tmp_path / "broker.sqlite3"
    rc = main(
        [
            "--state-db",
            str(state_db),
            "request",
            "--sandbox",
            "qianyi",
            "--candidate-sha",
            SHA_A,
            "--pool",
            "gb10",
            "--min-slots",
            "2",
            "--target-slots",
            "10",
            "--ttl-minutes",
            "120",
            "--purpose",
            "large-batch-runtime-validation",
            "--idempotency-key",
            "cli-request",
            "--preemptible",
        ],
    )
    assert rc == 0
    document = json.loads(capsys.readouterr().out)
    assert document["request"]["candidate_sha"] == SHA_A
    assert "token" not in json.dumps(document).lower()


def test_status_matches_published_evidence_schema(tmp_path: Path) -> None:
    clock = Clock()
    broker = _broker(tmp_path, clock)
    _request(broker, SandboxId.QIANYI)
    report = broker.reconcile(_budgets(global_slots=3, pools={"gb10": 3}))
    schema = json.loads(
        (
            Path(__file__).parents[2]
            / "docs/evidence/shared-sandbox-capacity-evidence.schema.json"
        ).read_text(encoding="utf-8"),
    )
    jsonschema.Draft202012Validator(schema).validate(report)
