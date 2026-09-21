from __future__ import annotations

import pytest
from fastapi import HTTPException

from loom_control_plane.routes.trials import _resolve_required_worker_pool_for_backend


def test_docker_ignores_nebius_task_binding(monkeypatch) -> None:
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    assert (
        _resolve_required_worker_pool_for_backend(
            batch_backend="docker",
            requested_pool=None,
            task_service_pool="nebius-cpu",
        )
        is None
    )


def test_docker_rejects_operator_pin_to_nebius_pool(monkeypatch) -> None:
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    with pytest.raises(HTTPException, match="requires backend 'nebius'"):
        _resolve_required_worker_pool_for_backend(
            batch_backend="docker",
            requested_pool="nebius-cpu",
            task_service_pool="nebius-cpu",
        )


def test_nebius_maps_only_to_nebius_cpu() -> None:
    assert (
        _resolve_required_worker_pool_for_backend(
            batch_backend="nebius",
            requested_pool=None,
            task_service_pool="nebius-cpu",
        )
        == "nebius-cpu"
    )


@pytest.mark.parametrize("task_service_pool", [None, "oldlab", "gb10"])
def test_nebius_rejects_tasks_without_nebius_binding(
    task_service_pool: str | None,
) -> None:
    with pytest.raises(HTTPException, match="requires a task revision") as exc_info:
        _resolve_required_worker_pool_for_backend(
            batch_backend="nebius",
            requested_pool=None,
            task_service_pool=task_service_pool,
        )
    assert exc_info.value.status_code == 400


def test_nebius_rejects_operator_pool_conflict() -> None:
    with pytest.raises(HTTPException, match="conflicts"):
        _resolve_required_worker_pool_for_backend(
            batch_backend="nebius",
            requested_pool="oldlab",
            task_service_pool="nebius-cpu",
        )
