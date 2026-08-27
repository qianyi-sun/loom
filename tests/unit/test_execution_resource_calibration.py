from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

from loom_control_plane.execution_resource_calibration import (
    CalibrationAttemptSample,
    derive_resource_calibration,
)


def _sample(
    index: int,
    *,
    started_at: datetime,
    stopped_at: datetime,
    throttled: bool = False,
    oom: bool = False,
    memory_limit_hit: bool = False,
) -> CalibrationAttemptSample:
    return CalibrationAttemptSample(
        trial_id=UUID(int=index + 1),
        attempt=1,
        task_id=f"task-{index % 100:03d}",
        batch_id=UUID(int=10_000) if index < 100 else UUID(int=20_000 + index // 100),
        started_at=started_at,
        stopped_at=stopped_at,
        cpu_average_millis=800,
        memory_peak_upper_bound_mib=1024,
        pids_peak_upper_bound=100,
        io_write_upper_bound_mib=2048,
        configured_cpu_millis=2_000,
        configured_ephemeral_storage_mib=4_096,
        throttled=throttled,
        oom=oom,
        memory_limit_hit=memory_limit_hit,
    )


def test_complete_representative_evidence_produces_eligible_profile() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    samples = []
    for index in range(1_000):
        if index < 100:
            started = base
            stopped = base + timedelta(hours=1)
        elif index == 999:
            started = base + timedelta(days=14)
            stopped = base + timedelta(days=14, hours=1)
        else:
            started = base + timedelta(minutes=index)
            stopped = started + timedelta(minutes=30)
        samples.append(_sample(index, started_at=started, stopped_at=stopped))

    result = derive_resource_calibration(samples)

    assert result.eligible is True
    assert result.blockers == ()
    assert result.trial_attempts == 1_000
    assert result.distinct_tasks == 100
    assert result.peak_batch_concurrency == 100
    assert result.evidence_duration_seconds == 14 * 24 * 60 * 60 + 60 * 60
    assert result.percentiles["cpu_average_millis"]["p995"] == 800
    assert result.recommended_cpu_millis == 2_000
    assert result.recommended_memory_mib == 1_280
    assert result.recommended_ephemeral_storage_mib == 4_096
    assert result.recommended_pids == 120


def test_incomplete_or_adverse_evidence_cannot_be_selected() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    samples = [
        _sample(
            index,
            started_at=base,
            stopped_at=base + timedelta(days=14, hours=1),
            throttled=index == 0,
            oom=index == 1,
            memory_limit_hit=index == 2,
        )
        for index in range(1_000)
    ]

    result = derive_resource_calibration(samples, incomplete_attempts=1)

    assert result.eligible is False
    assert result.blockers == (
        "resource_calibration_cpu_throttling_observed",
        "resource_calibration_memory_limit_observed",
        "resource_calibration_oom_observed",
        "resource_calibration_telemetry_incomplete",
    )


def test_sparse_evidence_preserves_every_acceptance_blocker() -> None:
    base = datetime(2026, 1, 1, tzinfo=UTC)
    sample = _sample(
        0,
        started_at=base,
        stopped_at=base + timedelta(hours=1),
    )

    result = derive_resource_calibration([sample])

    assert result.eligible is False
    assert result.blockers == (
        "resource_calibration_duration_insufficient",
        "resource_calibration_high_concurrency_batch_missing",
        "resource_calibration_trial_attempts_insufficient",
    )
