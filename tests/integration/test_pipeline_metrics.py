from __future__ import annotations

from prometheus_client import CollectorRegistry

from loom_control_plane.metrics import (
    EXECUTION_ATTEMPTS,
    PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS,
    PIPELINE_LIVE_PREVIEW_ACTIVE_GENERATIONS,
    PIPELINE_LIVE_PREVIEW_LAST_FRAME_AGE_SECONDS,
    PIPELINE_RUNS,
    PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS,
    PIPELINE_STAGE_QUEUE_AGE_SECONDS,
    PIPELINE_STAGE_RUNS,
)
from loom_control_plane.metrics_refresher import refresh_pipeline_gauges


class _Rows:
    def __init__(self, rows: list[tuple[object, ...]]) -> None:
        self._rows = rows

    def all(self) -> list[tuple[object, ...]]:
        return self._rows

    def one(self) -> tuple[object, ...]:
        return self._rows[0]


class _Session:
    def __init__(self, *pages: list[tuple[object, ...]]) -> None:
        self._pages = list(pages)

    async def execute(self, _statement: object) -> _Rows:
        return _Rows(self._pages.pop(0))


async def test_pipeline_gauges_publish_and_clear_disappeared_label_tuples() -> None:
    await refresh_pipeline_gauges(
        _Session(
            [("running", "none", 2)],
            [("queued", "gpu", 3)],
            [("running", "gpu", 1)],
            [("queued", "gpu", 42.0)],
            [("gpu", 7.0)],
            [("gpu", 11.0)],
            [(2, 3.5)],
        )
    )
    assert PIPELINE_RUNS.labels(state="running", result_status="none")._value.get() == 2
    assert PIPELINE_STAGE_RUNS.labels(state="queued", resource_class="gpu")._value.get() == 3
    assert EXECUTION_ATTEMPTS.labels(state="running", resource_class="gpu")._value.get() == 1
    assert (
        PIPELINE_STAGE_QUEUE_AGE_SECONDS.labels(state="queued", resource_class="gpu")._value.get()
        == 42
    )
    assert PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS.labels(resource_class="gpu")._value.get() == 7
    assert PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS.labels(resource_class="gpu")._value.get() == 11
    assert PIPELINE_LIVE_PREVIEW_ACTIVE_GENERATIONS._value.get() == 2
    assert PIPELINE_LIVE_PREVIEW_LAST_FRAME_AGE_SECONDS._value.get() == 3.5

    await refresh_pipeline_gauges(_Session([], [], [], [], [], [], [(0, 0.0)]))
    for gauge in (
        PIPELINE_RUNS,
        PIPELINE_STAGE_RUNS,
        EXECUTION_ATTEMPTS,
        PIPELINE_STAGE_QUEUE_AGE_SECONDS,
        PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS,
        PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS,
    ):
        assert not gauge._metrics


def test_pipeline_metric_names_register_without_collision() -> None:
    registry = CollectorRegistry()
    names = [
        "loom_pipeline_runs",
        "loom_pipeline_stage_runs",
        "loom_execution_attempts",
    ]
    assert len(names) == len(set(names))
    assert registry is not None
