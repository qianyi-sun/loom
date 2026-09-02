from __future__ import annotations

import hashlib
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from loom.db.schema import (
    AdminAuditEvent,
    ExecutionAdmissionPolicy,
    ExecutionBudgetPolicy,
    ExecutionCapacityObservation,
    ExecutionCapacityPolicy,
    ExecutionCostReservation,
    ExecutionCostReservationDebit,
    ExecutionNodeCostAllocation,
    ExecutionNodeCostRecord,
    ExecutionPriceSnapshot,
    ExecutionProvisioningAuthorization,
    ExecutionResourceCalibration,
    ExecutionTargetPriceBinding,
    ServiceExecutionClass,
    ServiceExecutionTarget,
    Token,
    WorkerPoolAutoscalerPolicy,
)
from loom.execution_contract import NEBIUS_CPU_EXECUTION_CLASS_V1
from loom_control_plane.app import create_app
from loom_control_plane.config import ControlPlaneSettings

RAW_ADMIN_TOKEN = "loom_admin_" + "A" * 43


def _write_admin_secret(path: Path) -> None:
    path.write_text(
        f'[admin]\ntoken = "{RAW_ADMIN_TOKEN}"\ncreated_at = "2026-06-27T00:00:00Z"\nversion = 1\n',
        encoding="utf-8",
    )
    path.chmod(0o600)


def _set_cp_env(monkeypatch: pytest.MonkeyPatch, postgres_url: str) -> None:
    for k, v in {
        "LOOM_CP_DB_URL": postgres_url,
        "LOOM_CP_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_CP_MINIO_ACCESS_KEY": "x",
        "LOOM_CP_MINIO_SECRET_KEY": "y",
        "LOOM_CP_LLM_GATEWAY_URL": "http://gw:9100/",
    }.items():
        monkeypatch.setenv(k, v)


@pytest.fixture
def app(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    postgres_url: str,
):
    secret_file = tmp_path / "secrets.toml"
    _write_admin_secret(secret_file)
    _set_cp_env(monkeypatch, postgres_url)
    monkeypatch.setenv("LOOM_CP_ADMIN_SECRET_FILE", str(secret_file))
    return create_app(ControlPlaneSettings(_env_file=None))


@pytest.fixture(autouse=True)
def clean_autoscaler_policies(postgres_url: str) -> Iterator[None]:
    engine = create_engine(postgres_url)
    session_factory = sessionmaker(engine)
    with session_factory() as s:
        s.execute(text("TRUNCATE TABLE execution_resource_calibrations CASCADE"))
        s.execute(delete(ExecutionProvisioningAuthorization))
        s.execute(delete(ExecutionCapacityObservation))
        s.execute(delete(ExecutionCapacityPolicy))
        s.execute(delete(ExecutionNodeCostAllocation))
        s.execute(delete(ExecutionNodeCostRecord))
        s.execute(delete(ExecutionCostReservationDebit))
        s.execute(delete(ExecutionCostReservation))
        s.execute(delete(ExecutionTargetPriceBinding))
        s.execute(delete(ExecutionBudgetPolicy))
        s.execute(delete(ExecutionPriceSnapshot))
        s.execute(
            delete(ServiceExecutionTarget).where(ServiceExecutionTarget.id == "nebius-finance-api")
        )
        s.execute(
            delete(ServiceExecutionClass).where(
                ServiceExecutionClass.id == "nebius-finance-api-class"
            )
        )
        s.execute(delete(WorkerPoolAutoscalerPolicy))
        s.execute(delete(ExecutionAdmissionPolicy))
        s.execute(delete(AdminAuditEvent).where(AdminAuditEvent.action.like("execution.%")))
        s.execute(delete(Token))
        s.commit()
    try:
        yield
    finally:
        with session_factory() as s:
            s.execute(text("TRUNCATE TABLE execution_resource_calibrations CASCADE"))
            s.execute(delete(ExecutionProvisioningAuthorization))
            s.execute(delete(ExecutionCapacityObservation))
            s.execute(delete(ExecutionCapacityPolicy))
            s.execute(delete(ExecutionNodeCostAllocation))
            s.execute(delete(ExecutionNodeCostRecord))
            s.execute(delete(ExecutionCostReservationDebit))
            s.execute(delete(ExecutionCostReservation))
            s.execute(delete(ExecutionTargetPriceBinding))
            s.execute(delete(ExecutionBudgetPolicy))
            s.execute(delete(ExecutionPriceSnapshot))
            s.execute(
                delete(ServiceExecutionTarget).where(
                    ServiceExecutionTarget.id == "nebius-finance-api"
                )
            )
            s.execute(
                delete(ServiceExecutionClass).where(
                    ServiceExecutionClass.id == "nebius-finance-api-class"
                )
            )
            s.execute(delete(WorkerPoolAutoscalerPolicy))
            s.execute(delete(ExecutionAdmissionPolicy))
            s.execute(delete(AdminAuditEvent).where(AdminAuditEvent.action.like("execution.%")))
            s.execute(delete(Token))
            s.commit()
        engine.dispose()


def _policy_payload() -> dict[str, object]:
    return {
        "actuator": "slurm",
        "enabled": True,
        "min_slots": 6,
        "max_slots": 30,
        "scale_up_threshold_slots": 1,
        "scale_down_idle_seconds": 600,
        "scale_up_cooldown_seconds": 60,
        "scale_down_cooldown_seconds": 300,
        "drain_timeout_seconds": 600,
        "force": False,
        "actuator_config": {
            "allowed_nodes": ["oldlab-1", "oldlab-2"],
            "requested_concurrency": 6,
            "cpu_arch": "x86_64",
        },
    }


def test_policy_put_get_and_status_round_trip(app) -> None:
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        upserted = client.put(
            "/admin/worker-pool-autoscaler-policies/production/oldlab",
            headers=headers,
            json=_policy_payload(),
        )
        assert upserted.status_code == 200, upserted.text
        body = upserted.json()
        assert body["environment"] == "production"
        assert body["pool_name"] == "oldlab"
        assert body["actuator"] == "slurm"
        assert body["enabled"] is True
        assert body["min_slots"] == 6
        assert body["max_slots"] == 30
        assert body["last_decision"] is None

        fetched = client.get(
            "/admin/worker-pool-autoscaler-policies/production/oldlab",
            headers=headers,
        )
        assert fetched.status_code == 200, fetched.text
        assert fetched.json()["actuator_config"]["allowed_nodes"] == [
            "oldlab-1",
            "oldlab-2",
        ]

        status = client.get(
            "/admin/worker-pool-autoscalers/status",
            headers=headers,
        )
        assert status.status_code == 200, status.text
        status_body = status.json()
        assert status_body["policies"][0]["pool_name"] == "oldlab"
        capacity = status_body["policies"][0]["routing_capacity"]
        assert capacity["schema_version"] == "loom.pool-capacity.v1"
        assert capacity["capacity_is_fresh"] is False
        assert capacity["executable_free_slots"] == 0
        assert capacity["configured_ceiling_slots"] == 30
        assert capacity["configured_scale_headroom_slots"] == 30
        assert capacity["aggregate_executable_eligible"] is False


def test_policy_rejects_max_slots_below_min_slots(app) -> None:
    payload = _policy_payload()
    payload["min_slots"] = 10
    payload["max_slots"] = 5

    with TestClient(app) as client:
        response = client.put(
            "/admin/worker-pool-autoscaler-policies/production/oldlab",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=payload,
        )

    assert response.status_code == 400, response.text
    assert "max_slots" in response.json()["detail"]


def test_policy_rejects_invalid_routing_controls(app) -> None:
    payload = _policy_payload()
    payload["actuator_config"] = {
        **payload["actuator_config"],
        "routing_budget_eligible": "yes",
    }

    with TestClient(app) as client:
        response = client.put(
            "/admin/worker-pool-autoscaler-policies/production/oldlab",
            headers={"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"},
            json=payload,
        )

    assert response.status_code == 400, response.text
    assert "routing_budget_eligible" in response.json()["detail"]


def test_policy_delete_requires_drained_shape_and_is_idempotent(app) -> None:
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    payload = _policy_payload()
    payload.update({"enabled": False, "min_slots": 0, "max_slots": 0})
    with TestClient(app) as client:
        created = client.put(
            "/admin/worker-pool-autoscaler-policies/dev-alice/dev-alice",
            headers=headers,
            json=payload,
        )
        assert created.status_code == 200, created.text

        deleted = client.delete(
            "/admin/worker-pool-autoscaler-policies/dev-alice/dev-alice",
            headers=headers,
        )
        assert deleted.status_code == 204, deleted.text

        missing = client.delete(
            "/admin/worker-pool-autoscaler-policies/dev-alice/dev-alice",
            headers=headers,
        )
        assert missing.status_code == 404, missing.text


def test_execution_admission_policy_round_trip_and_status(app, postgres_url: str) -> None:
    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    with TestClient(app) as client:
        created = client.put(
            "/admin/execution-admission-policies/pool/nebius-cpu",
            headers=headers,
            json={
                "max_concurrent": 40,
                "enabled": True,
                "reason": "bounded Nebius canary",
            },
        )
        assert created.status_code == 200, created.text
        assert created.json()["version"] == 1

        status = client.get(
            "/admin/execution-admission/status",
            headers=headers,
        )
        assert status.status_code == 200, status.text
        policy = status.json()["policies"][0]
        assert policy["scope_kind"] == "pool"
        assert policy["scope_key"] == "nebius-cpu"
        assert policy["active_count"] == 0
        assert policy["ledger_active_count"] == 0
        assert policy["counter_in_sync"] is True
        assert policy["available"] == 40

        updated = client.put(
            "/admin/execution-admission-policies/pool/nebius-cpu",
            headers=headers,
            json={"max_concurrent": 12, "enabled": False},
        )
        assert updated.status_code == 200, updated.text
        assert updated.json()["version"] == 2
        assert updated.json()["enabled"] is False

    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        events = session.scalars(
            select(AdminAuditEvent).where(
                AdminAuditEvent.action == "execution.admission_policy.upserted"
            )
        ).all()
        assert len(events) == 2
        assert {event.target_id for event in events} == {"pool/nebius-cpu"}
        assert {event.event_metadata["version"] for event in events} == {1, 2}
    engine.dispose()


def test_execution_finance_admin_round_trip_keeps_bill_overhead_explicit(
    app,
    postgres_url: str,
) -> None:
    now = datetime.now(UTC)
    billing_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    engine = create_engine(postgres_url)
    with sessionmaker(engine)() as session:
        session.add(
            ServiceExecutionClass(
                id="nebius-finance-api-class",
                schema_version="loom.execution-class.v1",
                spec_json=NEBIUS_CPU_EXECUTION_CLASS_V1.model_dump(mode="json"),
                spec_sha256="sha256:" + "a" * 64,
                enabled=True,
            )
        )
        session.add(
            ServiceExecutionTarget(
                id="nebius-finance-api",
                logical_pool_id="nebius-finance-api-pool",
                execution_class_id="nebius-finance-api-class",
                schema_version="loom.execution-target.v1",
                spec_json={},
                spec_sha256="sha256:" + "b" * 64,
                environment="staging",
                provider="nebius",
                region="eu-north1",
                failure_domain="eu-north1-a",
                data_residency="eu",
                desired_state="active",
                observed_state="ready",
                health_status="healthy",
                health_observed_at=now,
            )
        )
        session.commit()

    headers = {"Authorization": f"Bearer {RAW_ADMIN_TOKEN}"}
    price_payload = {
        "provider": "nebius",
        "region": "eu-north1",
        "sku": "cpu-d3",
        "source": "nebius-public-rate-card",
        "source_version": "2026-08-26",
        "source_uri": "https://example.test/nebius/rate-card",
        "effective_at": "2026-08-26T00:00:00Z",
        "observed_at": "2026-08-26T01:00:00Z",
        "base_microusd_per_hour": 1_000_000,
        "vcpu_microusd_per_hour": 100_000,
        "memory_gib_microusd_per_hour": 10_000,
        "ephemeral_storage_gib_microusd_per_hour": 1_000,
    }
    with TestClient(app) as client:
        created = client.post(
            "/admin/execution-price-snapshots",
            headers=headers,
            json=price_payload,
        )
        assert created.status_code == 200, created.text
        assert created.json()["created"] is True
        price_id = created.json()["id"]

        replay = client.post(
            "/admin/execution-price-snapshots",
            headers=headers,
            json=price_payload,
        )
        assert replay.status_code == 200, replay.text
        assert replay.json() == {**created.json(), "created": False}

        conflict = client.post(
            "/admin/execution-price-snapshots",
            headers=headers,
            json={**price_payload, "base_microusd_per_hour": 2_000_000},
        )
        assert conflict.status_code == 400
        assert "different contents" in conflict.json()["detail"]

        binding = client.put(
            "/admin/execution-target-price-bindings/nebius-finance-api",
            headers=headers,
            json={
                "price_snapshot_id": price_id,
                "enabled": True,
                "reason": "accepted immutable price evidence",
            },
        )
        assert binding.status_code == 200, binding.text
        assert binding.json()["version"] == 1

        budget_payload = {
            "daily_limit_microusd": 10_000_000,
            "monthly_limit_microusd": 100_000_000,
            "per_attempt_limit_microusd": 5_000_000,
            "max_estimate_duration_seconds": 7_200,
            "emergency_stop": False,
            "enabled": True,
            "reason": "bounded paid execution",
        }
        for kind, key in (
            ("pool", "nebius-finance-api-pool"),
            ("target", "nebius-finance-api"),
        ):
            budget = client.put(
                f"/admin/execution-budget-policies/{kind}/{key}",
                headers=headers,
                json=budget_payload,
            )
            assert budget.status_code == 200, budget.text
            assert budget.json()["version"] == 1

        capacity_policy = client.put(
            "/admin/execution-capacity-policies/nebius-finance-api",
            headers=headers,
            json={
                "enabled": True,
                "max_nodes": 20,
                "max_vcpu_millis": 1_280_000,
                "max_memory_mib": 5_242_880,
                "max_storage_mib": 20_971_520,
                "node_cpu_millis": 64_000,
                "node_memory_mib": 262_144,
                "node_storage_mib": 1_048_576,
                "max_pending_jobs": 20,
                "max_unschedulable_jobs": 2,
                "max_image_pull_backoff_jobs": 2,
                "max_create_per_minute": 10,
                "observation_max_age_seconds": 300,
                "reason": "bounded Nebius provisioning",
            },
        )
        assert capacity_policy.status_code == 200, capacity_policy.text
        assert capacity_policy.json()["version"] == 1

        collector_token = client.post(
            "/admin/execution-capacity-collector-tokens",
            headers=headers,
            json={},
        )
        assert collector_token.status_code == 201, collector_token.text
        collector_token_hash = hashlib.sha256(
            collector_token.json()["token"].encode()
        ).digest()
        with sessionmaker(engine)() as session:
            persisted_collector_token = session.get(Token, collector_token_hash)
            assert persisted_collector_token is not None
            assert persisted_collector_token.expires_at is None
        collector_headers = {"Authorization": f"Bearer {collector_token.json()['token']}"}
        collector_policy = client.get(
            "/admin/execution-capacity-collector-policy/nebius-finance-api",
            headers=collector_headers,
            params={"pool_id": "nebius-finance-api-pool"},
        )
        assert collector_policy.status_code == 200, collector_policy.text
        assert collector_policy.json() == {
            "target_id": "nebius-finance-api",
            "pool_id": "nebius-finance-api-pool",
            "enabled": True,
            "max_nodes": 20,
            "node_cpu_millis": 64_000,
            "node_memory_mib": 262_144,
            "node_storage_mib": 1_048_576,
            "version": 1,
        }
        assert (
            client.get(
                "/admin/execution-capacity/status",
                headers=collector_headers,
                params={"pool_id": "nebius-finance-api-pool"},
            ).status_code
            == 403
        )
        forbidden_policy_write = client.put(
            "/admin/execution-capacity-policies/nebius-finance-api",
            headers=collector_headers,
            json={
                "enabled": False,
                "max_nodes": 20,
                "max_vcpu_millis": 1_280_000,
                "max_memory_mib": 5_242_880,
                "max_storage_mib": 20_971_520,
                "node_cpu_millis": 64_000,
                "node_memory_mib": 262_144,
                "node_storage_mib": 1_048_576,
                "max_pending_jobs": 10,
                "max_unschedulable_jobs": 1,
                "max_image_pull_backoff_jobs": 1,
                "max_create_per_minute": 10,
                "observation_max_age_seconds": 120,
            },
        )
        assert forbidden_policy_write.status_code == 403

        observation_payload = {
            "target_id": "nebius-finance-api",
            "source": "nebius-capacity-export",
            "source_version": "snapshot-1",
            "observed_at": now.isoformat(),
            "provider_capacity_state": "available",
            "provider_capacity_reason": None,
            "autoscaler_state": "ready",
            "autoscaler_reason": None,
            "provider_quota_nodes": 20,
            "provider_quota_vcpu_millis": 1_280_000,
            "provider_quota_memory_mib": 5_242_880,
            "provider_quota_storage_mib": 20_971_520,
            "provider_used_nodes": 1,
            "provider_used_vcpu_millis": 64_000,
            "provider_used_memory_mib": 262_144,
            "provider_used_storage_mib": 1_048_576,
            "active_nodes": 1,
            "provisioned_vcpu_millis": 64_000,
            "provisioned_memory_mib": 262_144,
            "provisioned_storage_mib": 1_048_576,
            "allocatable_cpu_millis": 62_000,
            "allocatable_memory_mib": 250_000,
            "allocatable_storage_mib": 1_000_000,
            "requested_cpu_millis": 20_000,
            "requested_memory_mib": 100_000,
            "requested_storage_mib": 200_000,
            "pending_jobs": 1,
            "unschedulable_jobs": 0,
            "image_pull_backoff_jobs": 0,
            "pending_reasons": {"autoscaler_delay": 1},
        }
        capacity_observation = client.post(
            "/admin/execution-capacity-observations",
            headers=collector_headers,
            json=observation_payload,
        )
        assert capacity_observation.status_code == 200, capacity_observation.text
        assert capacity_observation.json()["created"] is True
        capacity_replay = client.post(
            "/admin/execution-capacity-observations",
            headers=collector_headers,
            json=observation_payload,
        )
        assert capacity_replay.status_code == 200, capacity_replay.text
        assert capacity_replay.json()["created"] is False
        capacity_conflict = client.post(
            "/admin/execution-capacity-observations",
            headers=collector_headers,
            json={**observation_payload, "pending_jobs": 2},
        )
        assert capacity_conflict.status_code == 400, capacity_conflict.text
        assert "different contents" in capacity_conflict.json()["detail"]

        capacity_status = client.get(
            "/admin/execution-capacity/status?pool_id=nebius-finance-api-pool",
            headers=headers,
        )
        assert capacity_status.status_code == 200, capacity_status.text
        capacity_target = capacity_status.json()["targets"][0]
        assert capacity_target["observation"]["is_fresh"] is True
        assert capacity_target["observation"]["pending_reasons"] == {"autoscaler_delay": 1}
        assert capacity_target["observation"]["provider_quota_nodes_headroom"] == 19
        assert capacity_target["observation"]["allocatable_cpu_millis_free"] == 42_000
        assert capacity_target["command_backlog"] == 0
        assert capacity_target["recent_authorizations"] == []
        assert capacity_target["blockers"] == []

        over_quota = client.post(
            "/admin/execution-capacity-observations",
            headers=collector_headers,
            json={
                **observation_payload,
                "source_version": "snapshot-over-quota",
                "observed_at": (now + timedelta(seconds=1)).isoformat(),
                "provider_used_nodes": 21,
            },
        )
        assert over_quota.status_code == 200, over_quota.text
        over_quota_status = client.get(
            "/admin/execution-capacity/status?pool_id=nebius-finance-api-pool",
            headers=headers,
        )
        assert over_quota_status.status_code == 200, over_quota_status.text
        over_quota_target = over_quota_status.json()["targets"][0]
        assert over_quota_target["observation"]["provider_quota_nodes_headroom"] == 0
        assert "execution_capacity_provider_quota_nodes_exceeded" in over_quota_target["blockers"]

        calibration_payload = {
            "target_id": "nebius-finance-api",
            "source_pool_id": "oldlab",
            "source_architecture": "x86_64",
            "resource_profile": "legacy-cpu-profile@1",
            "candidate_sha": "c" * 40,
            "source_version": "calibration-empty-1",
            "window_started_at": (now - timedelta(days=15)).isoformat(),
            "window_stopped_at": (now - timedelta(days=1)).isoformat(),
        }
        calibration = client.post(
            "/admin/execution-resource-calibrations",
            headers=headers,
            json=calibration_payload,
        )
        assert calibration.status_code == 200, calibration.text
        assert calibration.json()["created"] is True
        assert calibration.json()["eligible"] is False
        assert calibration.json()["trial_attempts"] == 0
        assert calibration.json()["blockers"] == [
            "resource_calibration_duration_insufficient",
            "resource_calibration_high_concurrency_batch_missing",
            "resource_calibration_trial_attempts_insufficient",
        ]
        calibration_replay = client.post(
            "/admin/execution-resource-calibrations",
            headers=headers,
            json=calibration_payload,
        )
        assert calibration_replay.status_code == 200, calibration_replay.text
        assert calibration_replay.json() == {**calibration.json(), "created": False}

        rejected_binding = client.put(
            "/admin/execution-resource-profile-bindings/nebius-finance-api",
            headers=headers,
            json={
                "calibration_id": calibration.json()["id"],
                "enabled": True,
                "reason": "must reject insufficient evidence",
            },
        )
        assert rejected_binding.status_code == 400, rejected_binding.text
        assert "ineligible" in rejected_binding.json()["detail"]
        with engine.connect() as connection:
            transaction = connection.begin()
            with pytest.raises(DBAPIError, match="requires eligible calibration"):
                connection.execute(
                    text(
                        """
                        INSERT INTO execution_resource_profile_bindings
                          (target_id, calibration_id, enabled, reason, version)
                        VALUES (
                          :target_id, :calibration_id, true,
                          'direct database bypass attempt', 1
                        )
                        """
                    ),
                    {
                        "target_id": "nebius-finance-api",
                        "calibration_id": calibration.json()["id"],
                    },
                )
            transaction.rollback()
        disabled_binding = client.put(
            "/admin/execution-resource-profile-bindings/nebius-finance-api",
            headers=headers,
            json={
                "calibration_id": calibration.json()["id"],
                "enabled": False,
                "reason": "retain diagnostic snapshot only",
            },
        )
        assert disabled_binding.status_code == 200, disabled_binding.text
        assert disabled_binding.json()["enabled"] is False

        profile_status = client.get(
            "/admin/execution-resource-profile/status",
            headers=headers,
            params={"pool_id": "nebius-finance-api-pool"},
        )
        assert profile_status.status_code == 200, profile_status.text
        profile_target = profile_status.json()["targets"][0]
        assert profile_target["forecast_is_fresh"] is False
        assert profile_target["observed_fit_slots"] == 0
        assert profile_target["immediate_executable_slots"] == 0
        assert profile_target["configured_scale_headroom_slots"] == 0
        assert profile_target["configured_total_fit_slots"] == 0
        assert "resource_profile_binding_unavailable" in profile_target["blockers"]
        assert "resource_forecast_provider_quota_nodes_exhausted" in profile_target["blockers"]
        assert "resource_calibration_trial_attempts_insufficient" in profile_target["blockers"]
        assert profile_target["recent_calibrations"][0]["id"] == calibration.json()["id"]

        eligible_calibration_id = uuid4()
        with sessionmaker(engine)() as session:
            session.add(
                ExecutionResourceCalibration(
                    id=eligible_calibration_id,
                    target_id="nebius-finance-api",
                    source_pool_id="oldlab",
                    source_architecture="x86_64",
                    resource_profile="measured-cpu-profile@2",
                    candidate_sha="d" * 40,
                    source_version="eligible-fixture-1",
                    window_started_at=now - timedelta(days=15),
                    window_stopped_at=now - timedelta(days=1),
                    trial_attempts=1_000,
                    distinct_tasks=100,
                    usage_records=1_000,
                    incomplete_attempts=0,
                    evidence_duration_seconds=14 * 24 * 60 * 60,
                    peak_batch_concurrency=100,
                    throttled_attempts=0,
                    oom_attempts=0,
                    memory_limit_attempts=0,
                    eligible=True,
                    blockers_json=[],
                    percentiles_json={},
                    recommended_cpu_millis=2_000,
                    recommended_memory_mib=4_096,
                    recommended_ephemeral_storage_mib=8_192,
                    recommended_pids=256,
                    evidence_json={"schema_version": "test-only"},
                    evidence_sha256="sha256:" + "d" * 64,
                )
            )
            session.commit()
        recovered_observation = client.post(
            "/admin/execution-capacity-observations",
            headers=collector_headers,
            json={
                **observation_payload,
                "source_version": "snapshot-recovered",
                "observed_at": (now + timedelta(seconds=2)).isoformat(),
            },
        )
        assert recovered_observation.status_code == 200, recovered_observation.text
        missing_acceptance_reason = client.put(
            "/admin/execution-resource-profile-bindings/nebius-finance-api",
            headers=headers,
            json={
                "calibration_id": str(eligible_calibration_id),
                "enabled": True,
                "reason": None,
            },
        )
        assert missing_acceptance_reason.status_code == 400
        assert "acceptance reason" in missing_acceptance_reason.json()["detail"]
        enabled_binding = client.put(
            "/admin/execution-resource-profile-bindings/nebius-finance-api",
            headers=headers,
            json={
                "calibration_id": str(eligible_calibration_id),
                "enabled": True,
                "reason": "eligible fixed-candidate fixture",
            },
        )
        assert enabled_binding.status_code == 200, enabled_binding.text
        assert enabled_binding.json()["version"] == 2
        fresh_profile_status = client.get(
            "/admin/execution-resource-profile/status",
            headers=headers,
            params={"pool_id": "nebius-finance-api-pool"},
        )
        assert fresh_profile_status.status_code == 200, fresh_profile_status.text
        fresh_forecast = fresh_profile_status.json()["targets"][0]
        assert fresh_forecast["forecast_is_fresh"] is True
        assert fresh_forecast["immediate_executable_slots"] == 21
        assert fresh_forecast["configured_additional_nodes"] == 19
        assert fresh_forecast["configured_slots_per_node"] == 32
        assert fresh_forecast["configured_scale_headroom_slots"] == 608
        assert fresh_forecast["configured_total_fit_slots"] == 629
        assert fresh_forecast["blockers"] == []

        node_cost = client.post(
            "/admin/execution-node-cost-records",
            headers=headers,
            json={
                "target_id": "nebius-finance-api",
                "price_snapshot_id": price_id,
                "provider_record_id": "invoice-line-1",
                "node_name": "provider-node-private-name",
                "interval_started_at": (billing_day + timedelta(hours=2)).isoformat(),
                "interval_stopped_at": (billing_day + timedelta(hours=3)).isoformat(),
                "node_cpu_millis": 64_000,
                "node_memory_mib": 262_144,
                "node_ephemeral_storage_mib": 1_048_576,
                "provider_billed_microusd": 1_000_000,
                "billing_source": "nebius-invoice-export",
                "billing_source_version": "invoice-2026-08",
                "observed_at": (billing_day + timedelta(hours=4)).isoformat(),
            },
        )
        assert node_cost.status_code == 200, node_cost.text
        assert node_cost.json()["allocated_microusd"] == 0
        assert node_cost.json()["idle_system_fragmentation_microusd"] == 1_000_000
        assert "provider-node-private-name" not in str(node_cost.json())
        assert node_cost.json()["node_identity_sha256"].startswith("sha256:")

        cross_day = client.post(
            "/admin/execution-node-cost-records",
            headers=headers,
            json={
                "target_id": "nebius-finance-api",
                "price_snapshot_id": price_id,
                "provider_record_id": "invoice-line-cross-day",
                "node_name": "provider-node-private-name",
                "interval_started_at": (billing_day + timedelta(hours=23, minutes=30)).isoformat(),
                "interval_stopped_at": (billing_day + timedelta(days=1, minutes=30)).isoformat(),
                "node_cpu_millis": 64_000,
                "node_memory_mib": 262_144,
                "node_ephemeral_storage_mib": 1_048_576,
                "provider_billed_microusd": 1_000_000,
                "billing_source": "nebius-invoice-export",
                "billing_source_version": "invoice-2026-08",
                "observed_at": (billing_day + timedelta(days=1, hours=1)).isoformat(),
            },
        )
        assert cross_day.status_code == 400, cross_day.text
        assert "split at UTC day boundaries" in cross_day.json()["detail"]

        status = client.get(
            "/admin/execution-finance/status?pool_id=nebius-finance-api-pool",
            headers=headers,
        )
        assert status.status_code == 200, status.text
        body = status.json()
        assert len(body["price_snapshots"]) == 1
        assert len(body["target_bindings"]) == 1
        assert len(body["budget_policies"]) == 2
        assert all(policy["counter_in_sync"] is True for policy in body["budget_policies"])
        assert body["node_cost_records"][0]["provider_billed_microusd"] == 1_000_000
        assert body["node_cost_records"][0]["idle_system_fragmentation_microusd"] == 1_000_000

    with sessionmaker(engine)() as session:
        actions = set(
            session.scalars(
                select(AdminAuditEvent.action).where(AdminAuditEvent.action.like("execution.%"))
            ).all()
        )
        assert {
            "execution.price_snapshot.recorded",
            "execution.target_price_binding.upserted",
            "execution.budget_policy.upserted",
            "execution.capacity_policy.upserted",
            "execution.capacity_observation.recorded",
            "execution.node_cost.recorded",
        }.issubset(actions)
    engine.dispose()
