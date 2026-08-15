from __future__ import annotations

import base64
import hashlib
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_control_plane.global_dev_fleet_autoscaler import (
    DevCapacityDemand,
    GlobalDevAutoscalerError,
    GlobalDevFleetAutoscaler,
    capacity_grants_from_report,
)
from loom_control_plane.global_execution_fence import (
    GlobalExecutionWitness,
    canonical_global_execution_witness_bytes,
)
from loom_control_plane.shared_capacity_broker import (
    BrokerBudgets,
    LeaseObservation,
    SharedCapacityBroker,
)

SHA_A = "a" * 40
SHA_B = "b" * 40


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 8, 5, 12, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def _coordinator(tmp_path: Path, clock: Clock) -> GlobalDevFleetAutoscaler:
    broker = SharedCapacityBroker(tmp_path / "authority" / "capacity.sqlite3", clock=clock)
    return GlobalDevFleetAutoscaler(broker, clock=clock)


def _demand(
    clock: Clock,
    environment: str,
    *,
    generation: int = 1,
    candidate_sha: str = SHA_A,
    requested_slots: int = 8,
    pool_name: str = "gb10",
) -> DevCapacityDemand:
    return DevCapacityDemand(
        environment=environment,
        deployment_generation=generation,
        candidate_sha=candidate_sha,
        pool_name=pool_name,
        min_slots=0,
        requested_slots=requested_slots,
        observed_at=clock.now,
    )


def _budgets(slots: int = 8) -> BrokerBudgets:
    return BrokerBudgets(
        global_slots=slots,
        pool_slots={"gb10": slots},
        global_pending_slots=slots,
        pool_pending_slots={"gb10": slots},
    )


def _witness(clock: Clock, *, pool_id: str = "gb10") -> GlobalExecutionWitness:
    private_key = Ed25519PrivateKey.from_private_bytes(bytes([5]) * 32)
    public_key = private_key.public_key()
    payload: dict[str, object] = {
        "authority": "global-capacity-manager",
        "pool_id": pool_id,
        "execution_epoch": 0,
        "execution_state": "shadow",
        "executable_new_capacity_ceiling": 0,
        "expires_at": (clock.now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
        "signing_key_id": "manager-2026",
    }
    payload["canonical_digest"] = hashlib.sha256(
        canonical_global_execution_witness_bytes(payload)
    ).hexdigest()
    payload["signature_base64"] = base64.b64encode(
        private_key.sign(canonical_global_execution_witness_bytes(payload))
    ).decode("ascii")
    return GlobalExecutionWitness.from_mapping(
        payload,
        public_key=public_key,
        expected_public_key_sha256=hashlib.sha256(
            public_key.public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)
        ).hexdigest(),
    )


def _grants(report: dict[str, object]) -> dict[str, int]:
    return {
        str(item["environment"]): int(item["max_slots"])
        for item in report["grants"]  # type: ignore[union-attr]
    }


def test_four_dynamic_environments_share_one_global_budget_fairly(tmp_path: Path) -> None:
    clock = Clock()
    coordinator = _coordinator(tmp_path, clock)

    report = coordinator.reconcile(
        tuple(
            _demand(clock, environment)
            for environment in ("development", "dev-alice", "dev-bob", "dev-carol")
        ),
        _budgets(),
        execution_witness=_witness(clock),
    )

    assert report["authority"] == "global-dev-fleet-autoscaler"
    assert _grants(report) == {
        "development": 2,
        "dev-alice": 2,
        "dev-bob": 2,
        "dev-carol": 2,
    }
    assert report["aggregate"]["committed_slots"] == 8  # type: ignore[index]


def test_invalid_or_stale_snapshot_set_fails_before_state_mutation(tmp_path: Path) -> None:
    clock = Clock()
    coordinator = _coordinator(tmp_path, clock)
    stale = _demand(clock, "dev-alice")
    stale = replace(stale, observed_at=clock.now - timedelta(minutes=10))

    with pytest.raises(GlobalDevAutoscalerError, match="stale"):
        coordinator.reconcile((stale,), _budgets(), execution_witness=_witness(clock))

    assert coordinator.broker.status()["requests"] == []


def test_generation_supersession_is_drain_first_and_never_double_allocates(
    tmp_path: Path,
) -> None:
    clock = Clock()
    coordinator = _coordinator(tmp_path, clock)
    first = coordinator.reconcile(
        (_demand(clock, "dev-alice", requested_slots=4),),
        _budgets(4),
        execution_witness=_witness(clock),
    )
    first_grant = first["grants"][0]  # type: ignore[index]

    clock.now += timedelta(seconds=30)
    second = coordinator.reconcile(
        (
            _demand(
                clock,
                "dev-alice",
                generation=2,
                candidate_sha=SHA_B,
                requested_slots=4,
            ),
        ),
        _budgets(4),
        observations=(
            LeaseObservation(
                request_id=str(first_grant["request_id"]),
                lease_epoch=int(first_grant["lease_epoch"]),
                pending_slots=0,
                active_slots=4,
                draining_slots=0,
                terminal_slots=0,
            ),
        ),
        execution_witness=_witness(clock),
    )

    assert _grants(second) == {"dev-alice": 0}
    assert second["aggregate"]["committed_slots"] == 4  # type: ignore[index]
    old = next(
        item
        for item in second["ledger"]["requests"]  # type: ignore[index]
        if item["request"]["deployment_generation"] == 1
    )
    assert old["request"]["state"] == "draining"
    assert old["lease"]["granted_slots"] == 0
    assert old["lease"]["committed_slots"] == 4

    clock.now += timedelta(seconds=30)
    third = coordinator.reconcile(
        (
            _demand(
                clock,
                "dev-alice",
                generation=2,
                candidate_sha=SHA_B,
                requested_slots=4,
            ),
        ),
        _budgets(4),
        observations=(
            LeaseObservation(
                request_id=str(old["request"]["id"]),
                lease_epoch=int(old["lease"]["lease_epoch"]),
                pending_slots=0,
                active_slots=0,
                draining_slots=0,
                terminal_slots=4,
            ),
        ),
        execution_witness=_witness(clock),
    )
    assert _grants(third) == {"dev-alice": 4}
    assert third["aggregate"]["committed_slots"] == 4  # type: ignore[index]


def test_removed_registry_environment_is_cancelled(tmp_path: Path) -> None:
    clock = Clock()
    coordinator = _coordinator(tmp_path, clock)
    coordinator.reconcile(
        (_demand(clock, "dev-alice"),), _budgets(), execution_witness=_witness(clock)
    )

    report = coordinator.reconcile((), _budgets(), execution_witness=_witness(clock))

    assert report["grants"] == []
    assert report["status"] == "ok"
    requests = coordinator.broker.status()["requests"]
    assert len(requests) == 1
    assert requests[0]["request"]["pool"] == "gb10"
    assert requests[0]["request"]["state"] == "draining"
    assert requests[0]["request"]["cancel_requested"] is True


def test_fenced_report_has_a_safe_aggregate_and_cannot_be_parsed_as_grants(
    tmp_path: Path,
) -> None:
    clock = Clock()
    report = _coordinator(tmp_path, clock).reconcile(
        (_demand(clock, "dev-alice"),),
        _budgets(),
    )

    assert report["status"] == "fenced"
    assert report["aggregate"] == {"legacy_scale_up_fenced": True}
    with pytest.raises(GlobalDevAutoscalerError, match="authority is invalid"):
        capacity_grants_from_report(report)
