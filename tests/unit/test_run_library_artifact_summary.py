from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from loom_service.routes import run_library as run_library_routes


class _FakeScalarResult:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def all(self) -> list[object]:
        return list(self._rows)


class _FakeExecuteResult:
    def __init__(self, rows: list[object]) -> None:
        self._rows = rows

    def scalars(self) -> _FakeScalarResult:
        return _FakeScalarResult(self._rows)


class _FakeSession:
    def __init__(self, rows_by_call: list[list[object]]) -> None:
        self.rows_by_call = rows_by_call
        self.calls = 0

    async def execute(self, _stmt: object) -> _FakeExecuteResult:
        rows = self.rows_by_call[self.calls]
        self.calls += 1
        return _FakeExecuteResult(rows)


@pytest.mark.asyncio
async def test_batch_list_artifact_summaries_cap_rows_per_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_library_routes,
        "_BATCH_LIST_ARTIFACT_SUMMARY_PER_BATCH_LIMIT",
        3,
        raising=False,
    )
    first_batch = uuid4()
    second_batch = uuid4()
    session = _FakeSession(
        [
            [
                "metric_table",
                "debug_bundle",
                "trajectory",
                "training_data_export",
            ],
            ["metric_table"],
        ]
    )

    summaries, truncated = await run_library_routes._batch_list_artifact_summaries(
        session,
        [first_batch, second_batch],
    )

    assert session.calls == 2
    assert sum(summaries[first_batch].values()) == 3
    assert summaries[first_batch]["reports"] == 1
    assert summaries[first_batch]["raw_diagnostics"] == 1
    assert summaries[first_batch]["trajectories"] == 1
    assert first_batch in truncated
    assert summaries[second_batch]["reports"] == 1
    assert second_batch not in truncated


@pytest.mark.asyncio
async def test_batch_detail_artifact_preview_caps_rows_per_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        run_library_routes,
        "_BATCH_DETAIL_ARTIFACT_PREVIEW_LIMIT",
        2,
        raising=False,
    )
    batch_id = uuid4()
    trial_id = uuid4()
    session = _FakeSession(
        [
            [
                SimpleNamespace(
                    id=uuid4(),
                    trial_id=trial_id,
                    artifact_type="metric_table",
                ),
                SimpleNamespace(
                    id=uuid4(),
                    trial_id=trial_id,
                    artifact_type="debug_bundle",
                ),
                SimpleNamespace(
                    id=uuid4(),
                    trial_id=trial_id,
                    artifact_type="trajectory",
                ),
            ]
        ]
    )

    typed_by_trial, summary, truncated = await (
        run_library_routes._batch_detail_artifact_preview(
            session,
            batch_id,
            [SimpleNamespace(id=trial_id)],
        )
    )

    assert session.calls == 1
    assert len(typed_by_trial[trial_id]) == 2
    assert sum(summary.values()) == 2
    assert summary["reports"] == 1
    assert summary["raw_diagnostics"] == 1
    assert truncated is True
