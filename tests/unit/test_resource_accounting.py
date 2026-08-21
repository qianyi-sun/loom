from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from loom.driver.base import DriverResourceSnapshot, StartOptions
from loom.driver.docker import snapshot_from_docker_stats
from loom.driver.fake import FakeDriver
from loom.models.resource_usage import (
    ResourceCounters,
    TrialResourceUsageReport,
    aggregate_resource_usage,
)
from loom_worker.resource_accounting import (
    ResourceAccountingDriver,
    ResourceUsageAccumulator,
    execution_key,
)
from loom_worker.resource_usage_outbox import ResourceUsageOutbox


def _snapshot(*, observed_at: datetime, cpu: int, memory: int) -> DriverResourceSnapshot:
    return DriverResourceSnapshot(
        observed_at=observed_at,
        source="docker_stats",
        runtime_id="runtime-1",
        image_digest="a" * 64,
        cpu_usage_usec=cpu,
        cpu_throttled_usec=cpu // 10,
        memory_current_bytes=memory,
        pids_current=3,
        io_read_bytes=cpu * 2,
    )


def test_docker_stats_normalization_preserves_unknowns_and_units() -> None:
    now = datetime.now(UTC)
    snapshot = snapshot_from_docker_stats(
        {
            "cpu_stats": {
                "cpu_usage": {
                    "total_usage": 12_345_000,
                    "usage_in_usermode": 10_000_000,
                    "usage_in_kernelmode": 2_345_000,
                },
                "throttling_data": {
                    "periods": 20,
                    "throttled_periods": 4,
                    "throttled_time": 500_000,
                },
            },
            "memory_stats": {"usage": 4096},
            "pids_stats": {"current": 7},
            "blkio_stats": {
                "io_service_bytes_recursive": [
                    {"op": "Read", "value": 100},
                    {"op": "Write", "value": 50},
                ]
            },
        },
        attrs={"Id": "abc", "Image": "b" * 64, "State": {}},
        observed_at=now,
        cgroup_files={
            "cpu.stat": (
                "usage_usec 12346\nuser_usec 10001\nsystem_usec 2345\n"
                "nr_periods 21\nnr_throttled 5\nthrottled_usec 501"
            ),
            "memory.current": "4097",
            "memory.peak": "8192",
            "memory.events": "low 1\nhigh 2\nmax 3\noom 4\noom_kill 1",
            "pids.current": "8",
            "pids.peak": "12",
            "io.stat": "8:0 rbytes=101 wbytes=51 rios=3 wios=4",
        },
    )
    assert snapshot.cpu_usage_usec == 12_346
    assert snapshot.cpu_throttled_usec == 501
    assert snapshot.memory_current_bytes == 4097
    assert snapshot.memory_peak_bytes == 8192
    assert snapshot.memory_events_oom == 4
    assert snapshot.memory_events_oom_kill == 1
    assert snapshot.pids_peak == 12
    assert snapshot.io_read_bytes == 101
    assert snapshot.io_write_bytes == 51


def test_docker_stats_preserves_inspect_oom_kill_when_cgroup_exec_is_unavailable() -> None:
    snapshot = snapshot_from_docker_stats(
        {"memory_stats": {"usage": 4096, "max_usage": 8192}},
        attrs={"State": {"OOMKilled": True}},
        observed_at=datetime.now(UTC),
    )
    assert snapshot.memory_peak_bytes == 8192
    assert snapshot.memory_events_oom == 1
    assert snapshot.memory_events_oom_kill == 1


def test_accumulator_never_regresses_counters_and_observes_sampled_peaks() -> None:
    now = datetime.now(UTC)
    accumulator = ResourceUsageAccumulator(now, now)
    accumulator.observe(_snapshot(observed_at=now, cpu=100, memory=200))
    accumulator.observe(
        _snapshot(observed_at=now + timedelta(seconds=1), cpu=90, memory=150),
    )
    counters = accumulator.counters()
    assert counters.cpu_usage_usec == 100
    assert counters.memory_current_bytes == 150
    assert counters.memory_peak_bytes == 200
    assert counters.pids_peak == 3


@pytest.mark.asyncio
async def test_driver_finalizes_before_inner_container_stop() -> None:
    now = datetime.now(UTC)
    snapshots = [_snapshot(observed_at=now, cpu=10, memory=20)]
    fake = FakeDriver(resource_snapshot_stub=lambda: snapshots[-1])
    emitted: list[tuple[TrialResourceUsageReport, bool, str]] = []

    async def sink(report: TrialResourceUsageReport, final: bool) -> None:
        emitted.append((report, final, fake.state))

    wrapper = ResourceAccountingDriver(
        fake,
        trial_id=uuid4(),
        attempt_count=1,
        worker_id=uuid4(),
        execution_key=execution_key("trial", "attempt", "agent"),
        container_role="agent",
        role_name="primary",
        architecture="arm64",
        candidate_sha="c" * 40,
        sink=sink,
        sample_interval_sec=60,
    )
    await wrapper.start(
        options=StartOptions(container_cpus=2, container_memory_mib=8192, container_pids=512),
    )
    snapshots.append(_snapshot(observed_at=now + timedelta(seconds=1), cpu=20, memory=30))
    await wrapper.stop()
    final, is_final, inner_state_at_sink = emitted[-1]
    assert is_final is True
    assert inner_state_at_sink == "running"
    assert fake.state == "stopped"
    assert final.completeness == "complete"
    assert final.counters.cpu_usage_usec == 20
    assert final.counters.memory_peak_bytes == 30
    assert final.limits.memory_bytes == 8192 * 1024 * 1024


@pytest.mark.asyncio
async def test_driver_marks_empty_backend_snapshot_unavailable() -> None:
    fake = FakeDriver(
        resource_snapshot_stub=lambda: DriverResourceSnapshot(
            observed_at=datetime.now(UTC),
            source="docker_stats",
        )
    )
    emitted: list[TrialResourceUsageReport] = []

    async def sink(report: TrialResourceUsageReport, final: bool) -> None:
        if final:
            emitted.append(report)

    wrapper = ResourceAccountingDriver(
        fake,
        trial_id=uuid4(),
        attempt_count=1,
        worker_id=uuid4(),
        execution_key=execution_key("empty"),
        container_role="agent",
        role_name="primary",
        architecture="arm64",
        candidate_sha=None,
        sink=sink,
        sample_interval_sec=60,
    )
    await wrapper.start()
    await wrapper.stop()
    assert emitted[0].completeness == "unavailable"
    assert emitted[0].diagnostic_code == "backend_telemetry_unavailable"


@pytest.mark.asyncio
async def test_outbox_replays_unfinished_checkpoint_as_partial(tmp_path: Path) -> None:
    root = tmp_path / "outbox"
    outbox = ResourceUsageOutbox(root)
    now = datetime.now(UTC)
    report = TrialResourceUsageReport(
        trial_id=uuid4(),
        attempt_count=1,
        worker_id=uuid4(),
        execution_key="d" * 64,
        container_role="agent",
        role_name="primary",
        backend="docker",
        source="docker_stats",
        observation_seq=1,
        first_observed_at=now,
        last_observed_at=now,
        completeness="partial",
        counters=ResourceCounters(cpu_usage_usec=10),
    )
    await outbox.stage(report)
    delivered: list[TrialResourceUsageReport] = []

    async def deliver(item: TrialResourceUsageReport) -> bool:
        delivered.append(item)
        return True

    assert await outbox.replay(deliver) == (1, 0)
    assert delivered[0].finalized_at is not None
    assert delivered[0].completeness == "partial"
    assert delivered[0].diagnostic_code == "worker_restart_before_finalize"
    assert list(root.glob("*.json")) == []


@pytest.mark.asyncio
async def test_outbox_replay_rejects_symlink_without_following_it(tmp_path: Path) -> None:
    root = tmp_path / "outbox"
    root.mkdir(mode=0o700)
    outside = tmp_path / "outside.json"
    outside.write_text("not an accounting report", encoding="utf-8")
    (root / ("e" * 64 + ".json")).symlink_to(outside)
    delivered: list[TrialResourceUsageReport] = []

    async def deliver(item: TrialResourceUsageReport) -> bool:
        delivered.append(item)
        return True

    assert await ResourceUsageOutbox(root).replay(deliver) == (0, 1)
    assert delivered == []
    assert outside.read_text(encoding="utf-8") == "not an accounting report"


def test_aggregate_names_peak_sum_as_upper_bound() -> None:
    now = datetime.now(UTC)
    reports = [
        TrialResourceUsageReport(
            trial_id=uuid4(),
            attempt_count=1,
            worker_id=uuid4(),
            execution_key=key * 64,
            container_role="sidecar",
            role_name=f"sidecar-{key}",
            backend="docker",
            source="docker_stats",
            observation_seq=1,
            first_observed_at=now,
            last_observed_at=now,
            finalized_at=now,
            completeness="complete",
            counters=ResourceCounters(memory_peak_bytes=peak, cpu_usage_usec=10),
        )
        for key, peak in (("a", 100), ("b", 200))
    ]
    aggregate = aggregate_resource_usage(reports)
    assert aggregate["memory_peak_upper_bound_bytes"] == 300
    assert "memory_peak_bytes" not in aggregate
    assert aggregate["cpu_usage_usec"] == 20
