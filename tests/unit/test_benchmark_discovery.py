from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from loom_service.dependencies import authed_session
from loom_service.routes import benchmarks


@pytest.mark.parametrize("count", [1, 100])
def test_discovery_batches_reads_and_deduplicates_aliases(monkeypatch, count):
    ids = [f"benchmark-{n}" for n in range(count)]
    rows = [SimpleNamespace(id=id_) for id_ in ids]
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=SimpleNamespace(all=lambda: rows)),
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: [("year", ["2023", "2024"])])),
    )
    resolve = AsyncMock(return_value=SimpleNamespace(physical_ids=tuple(ids)))
    readiness = AsyncMock(return_value=[])
    monkeypatch.setattr(benchmarks, "resolve_benchmark_selectors", resolve)
    monkeypatch.setattr(benchmarks, "_benchmark_rows_with_readiness", readiness)
    app = FastAPI()
    app.include_router(benchmarks.router)
    app.dependency_overrides[authed_session] = lambda: (session, None)
    response = TestClient(app).post("/benchmarks/discover", json={"benchmark_ids": [*ids, ids[0]]})
    assert response.status_code == 200
    assert response.json() == {"items": [{"key": "year", "values": ["2023", "2024"]}]}
    assert session.scalars.await_count == session.execute.await_count == 1
    resolve.assert_awaited_once_with(session, sorted(ids), require_runnable=False)
    readiness.assert_not_awaited()


def test_empty_selection_is_rejected():
    app = FastAPI()
    app.include_router(benchmarks.router)
    app.dependency_overrides[authed_session] = lambda: (None, None)
    assert (
        TestClient(app).post("/benchmarks/discover", json={"benchmark_ids": []}).status_code == 422
    )
