from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from loom.db.schema import ExecutionPriceSnapshot
from loom.execution_runtime_contract import (
    ContainerResourcesV1,
    ExecutionRuntimePlanV1,
    ProcessPhaseV1,
)
from loom_control_plane.execution_finance import (
    _normalized_allocations,
    estimate_execution_cost,
)
from tests.support.execution_image_admission import signed_image_admission_bundle


def _plan(now: datetime) -> ExecutionRuntimePlanV1:
    task_image = "registry.example/task@sha256:" + "a" * 64
    runtime_image = "registry.example/runtime@sha256:" + "b" * 64
    return ExecutionRuntimePlanV1(
        candidate_sha="1" * 40,
        task_revision_sha256="sha256:" + "2" * 64,
        command_identity_sha256="sha256:" + "3" * 64,
        execution_class_id="nebius-cpu-v1",
        composition="init_payload",
        task_image_ref=task_image,
        runtime_image_ref=runtime_image,
        runtime_binary_sha256="sha256:" + "4" * 64,
        image_admission=signed_image_admission_bundle(
            (task_image, runtime_image),
            now=now,
        ),
        task_resources=ContainerResourcesV1(
            cpu_millis=1_000,
            memory_mib=1_024,
            ephemeral_storage_mib=1_024,
        ),
        workspace_mib=1_024,
        runtime_volume_mib=32,
        main=ProcessPhaseV1(
            role="agent",
            argv=("/bin/true",),
            working_directory="/workspace",
            timeout_seconds=60,
        ),
        verifier_execution="skipped",
    )


def test_execution_cost_estimate_prices_complete_worst_case_pod_envelope() -> None:
    now = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)
    price = ExecutionPriceSnapshot(
        id=uuid4(),
        provider="nebius",
        region="eu-north1",
        sku="cpu-d3",
        currency="USD",
        source="test",
        source_version="v1",
        source_uri="https://example.test/rates",
        effective_at=now,
        observed_at=now,
        base_microusd_per_hour=3_600_000,
        vcpu_microusd_per_hour=3_600_000,
        memory_gib_microusd_per_hour=3_600_000,
        ephemeral_storage_gib_microusd_per_hour=3_600_000,
        rate_card_json={},
        rate_card_sha256="sha256:" + "5" * 64,
    )

    estimate = estimate_execution_cost(
        _plan(now),
        price,
        acquired_at=now,
        deadline_at=now + timedelta(hours=1),
    )

    assert estimate.duration_seconds == 3_600
    assert estimate.requested_cpu_millis == 1_050
    assert estimate.requested_memory_mib == 1_088
    assert estimate.requested_ephemeral_storage_mib == 3_156
    assert estimate.estimated_cost_microusd == 22_300_313
    assert estimate.daily_costs == ((now.date(), 22_300_313),)
    assert estimate.estimate_sha256.startswith("sha256:")


def test_execution_cost_estimate_splits_reservation_at_utc_day_boundary() -> None:
    now = datetime(2026, 8, 26, 23, 30, tzinfo=UTC)
    price = ExecutionPriceSnapshot(
        id=uuid4(),
        provider="nebius",
        region="eu-north1",
        sku="cpu-d3",
        currency="USD",
        source="test",
        source_version="v1",
        source_uri="https://example.test/rates",
        effective_at=now,
        observed_at=now,
        base_microusd_per_hour=3_600_000,
        vcpu_microusd_per_hour=0,
        memory_gib_microusd_per_hour=0,
        ephemeral_storage_gib_microusd_per_hour=0,
        rate_card_json={},
        rate_card_sha256="sha256:" + "5" * 64,
    )

    estimate = estimate_execution_cost(
        _plan(now),
        price,
        acquired_at=now,
        deadline_at=now + timedelta(hours=1),
    )

    assert estimate.daily_costs == (
        (datetime(2026, 8, 26, tzinfo=UTC).date(), 1_800_000),
        (datetime(2026, 8, 27, tzinfo=UTC).date(), 1_800_000),
    )
    assert estimate.estimated_cost_microusd == 3_600_000


def test_execution_cost_estimate_rounds_cross_day_duration_only_once() -> None:
    now = datetime(2026, 8, 26, 23, 30, 0, 1, tzinfo=UTC)
    price = ExecutionPriceSnapshot(
        id=uuid4(),
        provider="nebius",
        region="eu-north1",
        sku="cpu-d3",
        currency="USD",
        source="test",
        source_version="v1",
        source_uri="https://example.test/rates",
        effective_at=now,
        observed_at=now,
        base_microusd_per_hour=3_600_000,
        vcpu_microusd_per_hour=0,
        memory_gib_microusd_per_hour=0,
        ephemeral_storage_gib_microusd_per_hour=0,
        rate_card_json={},
        rate_card_sha256="sha256:" + "5" * 64,
    )

    estimate = estimate_execution_cost(
        _plan(now),
        price,
        acquired_at=now,
        deadline_at=now + timedelta(hours=1),
    )

    assert estimate.duration_seconds == 3_600
    assert estimate.daily_costs == (
        (datetime(2026, 8, 26, tzinfo=UTC).date(), 1_800_000),
        (datetime(2026, 8, 27, tzinfo=UTC).date(), 1_800_000),
    )
    assert estimate.estimated_cost_microusd == 3_600_000


def test_execution_cost_estimate_does_not_round_each_subsecond_day_segment() -> None:
    now = datetime(2026, 8, 26, 23, 59, 59, 999_999, tzinfo=UTC)
    price = ExecutionPriceSnapshot(
        id=uuid4(),
        provider="nebius",
        region="eu-north1",
        sku="cpu-d3",
        currency="USD",
        source="test",
        source_version="v1",
        source_uri="https://example.test/rates",
        effective_at=now,
        observed_at=now,
        base_microusd_per_hour=3_600_000,
        vcpu_microusd_per_hour=0,
        memory_gib_microusd_per_hour=0,
        ephemeral_storage_gib_microusd_per_hour=0,
        rate_card_json={},
        rate_card_sha256="sha256:" + "5" * 64,
    )

    estimate = estimate_execution_cost(
        _plan(now),
        price,
        acquired_at=now,
        deadline_at=now + timedelta(microseconds=2),
    )

    assert estimate.duration_seconds == 1
    assert estimate.daily_costs == (
        (datetime(2026, 8, 26, tzinfo=UTC).date(), 1_000),
        (datetime(2026, 8, 27, tzinfo=UTC).date(), 1),
    )
    assert estimate.estimated_cost_microusd == 1_001


def test_node_bill_normalization_never_allocates_above_provider_bill() -> None:
    assert _normalized_allocations([800, 800], 1_000) == [500, 500]
    assert _normalized_allocations([400, 200], 1_000) == [400, 200]
    assert sum(_normalized_allocations([5, 3, 2], 7)) == 7
