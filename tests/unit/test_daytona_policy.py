from datetime import UTC, datetime
from decimal import Decimal

import pytest

from loom.daytona_policy import (
    BackendPolicyRequest,
    BackendPolicySnapshot,
    DaytonaPriceSnapshot,
    DaytonaResources,
    build_policy_snapshot,
    daytona_incompatibilities,
    policy_digest,
)
from loom.models.task import TaskConfig, normalize_steps


def _request(*, budget: str = "5.00") -> BackendPolicyRequest:
    return BackendPolicyRequest(
        mode="overflow",
        allowed_backends=("docker", "daytona"),
        spillover_after_queue_seconds=120,
        daytona_resources=DaytonaResources(cpu=2, memory_gib=4, disk_gib=10),
        daytona_price_snapshot=DaytonaPriceSnapshot(
            source="operator-rate-card",
            version="2026-08-25",
            effective_at=datetime(2026, 8, 25, tzinfo=UTC),
            cpu_usd_per_hour=Decimal("0.10"),
            memory_gib_usd_per_hour=Decimal("0.01"),
            disk_gib_usd_per_hour=Decimal("0.001"),
        ),
        max_cloud_cost_usd=Decimal(budget),
        max_runtime_seconds=600,
    )


def test_policy_snapshot_cost_is_retry_and_batch_bounded() -> None:
    snapshot = build_policy_snapshot(
        request=_request(),
        expected_trial_count=4,
        max_attempts=3,
        authority={"kind": "platform_admin", "actor": "operator"},
        accepted_at=datetime(2026, 8, 25, tzinfo=UTC),
    )
    assert snapshot.worst_case_cloud_cost_usd == Decimal("0.500000")
    assert snapshot.worst_case_cloud_cost_usd <= snapshot.max_cloud_cost_usd
    assert policy_digest(snapshot) == policy_digest(snapshot.model_dump(mode="json"))


def test_policy_snapshot_rejects_worst_case_over_hard_budget() -> None:
    with pytest.raises(ValueError, match="worst-case cost exceeds"):
        build_policy_snapshot(
            request=_request(budget="0.49"),
            expected_trial_count=4,
            max_attempts=3,
            authority={"kind": "platform_admin"},
        )


def test_policy_snapshot_rejects_tampered_backend_order() -> None:
    snapshot = build_policy_snapshot(
        request=_request(),
        expected_trial_count=1,
        max_attempts=1,
        authority={"kind": "platform_admin"},
    )
    payload = snapshot.model_dump(mode="json")
    payload["allowed_backends"] = ["daytona", "docker"]

    with pytest.raises(ValueError, match="requires allowed_backends"):
        BackendPolicySnapshot.model_validate(payload)


def test_policy_snapshot_requires_nonempty_authority() -> None:
    snapshot = build_policy_snapshot(
        request=None,
        expected_trial_count=1,
        max_attempts=1,
        authority={"kind": "team_user"},
    )
    payload = snapshot.model_dump(mode="json")
    payload["authority"] = {}

    with pytest.raises(ValueError, match="at least 1 item"):
        BackendPolicySnapshot.model_validate(payload)


def test_daytona_compatibility_matrix_returns_structured_reasons() -> None:
    task = normalize_steps(
        TaskConfig.model_validate(
            {
                "schema_version": "1",
                "task": {"id": "incompatible", "name": "incompatible"},
                "environment": {
                    "os": "linux",
                    "cpu_arch": "arm64",
                    "gpu_vendor": "nvidia",
                    "docker_image": "python:latest",
                    "extra_hosts": {"private.service": "10.0.0.1"},
                    "skills_dir": "/host/skills",
                    "sidecars": [{"name": "db", "docker_image": "postgres:latest"}],
                },
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
                "steps": [
                    {
                        "name": "main",
                        "verifier": {"env_mode": "separate"},
                    }
                ],
            }
        )
    )
    codes = {item["code"] for item in daytona_incompatibilities(task)}
    assert codes == {
        "cpu_arch_unsupported",
        "gpu_unsupported",
        "sidecars_unsupported",
        "custom_network_unsupported",
        "local_resource_unsupported",
        "mutable_image_unsupported",
        "private_verifier_unsupported",
    }


def test_digest_pinned_cpu_task_is_daytona_compatible() -> None:
    task = normalize_steps(
        TaskConfig.model_validate(
            {
                "schema_version": "1",
                "task": {"id": "compatible", "name": "compatible"},
                "environment": {
                    "os": "linux",
                    "docker_image": "registry.example/task@sha256:" + "a" * 64,
                },
                "agent": {"name": "oracle"},
                "verifier": {"name": "pytest"},
            }
        )
    )
    assert daytona_incompatibilities(task) == []
