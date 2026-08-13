from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_control_plane.global_dev_fleet_autoscaler import GlobalDevFleetAutoscaler
from loom_control_plane.global_execution_fence import (
    GlobalExecutionFenceError,
    GlobalExecutionWitness,
    assert_legacy_scale_up_allowed,
    load_global_execution_witness,
)
from loom_control_plane.shared_capacity_broker import BrokerBudgets
from loom_control_plane.worker_pool_autoscaler import (
    AutoscalerPolicyConfig,
    apply_global_execution_fence,
)

NOW = datetime(2026, 8, 13, 12, tzinfo=UTC)


def _payload(**changes: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "authority": "global-capacity-manager",
        "pool_id": "oldlab",
        "execution_epoch": 0,
        "execution_state": "shadow",
        "executable_new_capacity_ceiling": 0,
        "expires_at": "2026-08-13T12:05:00Z",
        "authenticated": True,
    }
    payload.update(changes)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    payload["canonical_digest"] = hashlib.sha256(canonical.encode("ascii")).hexdigest()
    return payload


def witness_fixture(**changes: object) -> GlobalExecutionWitness:
    return GlobalExecutionWitness.from_mapping(_payload(**changes))


@pytest.mark.parametrize("state", ["prepared", "active", "drain-only"])
def test_legacy_scale_up_refuses_global_state(state: str) -> None:
    with pytest.raises(GlobalExecutionFenceError, match="state"):
        assert_legacy_scale_up_allowed(
            witness_fixture(execution_state=state, execution_epoch=1),
            expected_authority="global-capacity-manager",
            expected_pool_id="oldlab",
            now=NOW,
            required=True,
        )


def test_required_missing_witness_fails_closed() -> None:
    with pytest.raises(GlobalExecutionFenceError, match="unavailable"):
        assert_legacy_scale_up_allowed(
            None,
            expected_authority="global-capacity-manager",
            expected_pool_id="oldlab",
            now=NOW,
            required=True,
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"expires_at": "2026-08-13T12:00:00Z"}, "stale"),
        ({"authenticated": False}, "authenticated"),
        ({"authority": "foreign-capacity-manager"}, "authority"),
        ({"pool_id": "gb10"}, "pool"),
        ({"execution_epoch": 1}, "epoch"),
        ({"executable_new_capacity_ceiling": 1}, "ceiling"),
    ],
)
def test_legacy_scale_up_refuses_equivocal_or_nonshadow_witness(
    changes: dict[str, object],
    message: str,
) -> None:
    with pytest.raises(GlobalExecutionFenceError, match=message):
        assert_legacy_scale_up_allowed(
            witness_fixture(**changes),
            expected_authority="global-capacity-manager",
            expected_pool_id="oldlab",
            now=NOW,
            required=True,
        )


def test_load_witness_rejects_a_noncanonical_digest(tmp_path: Path) -> None:
    path = tmp_path / "witness.json"
    payload = _payload()
    payload["canonical_digest"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(GlobalExecutionFenceError, match="digest"):
        load_global_execution_witness(path)


def test_fresh_authenticated_shadow_witness_allows_legacy_scale_up() -> None:
    witness = witness_fixture()

    assert_legacy_scale_up_allowed(
        witness,
        expected_authority="global-capacity-manager",
        expected_pool_id="oldlab",
        now=NOW,
        required=True,
    )


def test_nonshadow_witness_clamps_legacy_policy_before_scale_up() -> None:
    policy = AutoscalerPolicyConfig(
        environment="staging",
        pool_name="oldlab",
        actuator="slurm",
        enabled=True,
        min_slots=2,
        max_slots=8,
        scale_up_threshold_slots=1,
        scale_down_idle_seconds=30,
        scale_up_cooldown_seconds=0,
        scale_down_cooldown_seconds=0,
        drain_timeout_seconds=60,
    )

    fenced = apply_global_execution_fence(
        policy,
        witness_fixture(execution_state="prepared", execution_epoch=1),
        expected_authority="global-capacity-manager",
        expected_pool_id="oldlab",
        now=NOW,
        required=True,
    )

    assert fenced.enabled is True
    assert fenced.min_slots == 0
    assert fenced.max_slots == 0
    assert fenced.disabled_reason == "global_execution_fence_state"


def test_global_development_grants_are_fenced_before_ledger_mutation() -> None:
    class _Broker:
        def status(self) -> object:
            raise AssertionError("legacy fence must run before development grant calculation")

    with pytest.raises(GlobalExecutionFenceError, match="state"):
        GlobalDevFleetAutoscaler(_Broker(), clock=lambda: NOW).reconcile(
            (),
            BrokerBudgets(
                global_slots=0,
                pool_slots={},
                global_pending_slots=0,
                pool_pending_slots={},
            ),
            execution_witness=witness_fixture(execution_state="active", execution_epoch=1),
            execution_witness_required=True,
        )
