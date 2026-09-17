"""Hosted admission never falls back to retired worker capacity."""

from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from loom.service_execution_backend import local_execution_enabled
from loom_service.routes.backends import list_backends
from loom_service.routes.batches import _reject_if_backend_cannot_execute_or_cold_start


@pytest.mark.parametrize("environment", [None, "", "development", "staging", "production"])
def test_local_execution_requires_explicit_opt_in(monkeypatch, environment):
    monkeypatch.delenv("LOOM_LOCAL_EXECUTION", raising=False)
    if environment is None:
        monkeypatch.delenv("LOOM_ENV", raising=False)
    else:
        monkeypatch.setenv("LOOM_ENV", environment)
    assert not local_execution_enabled()


def test_disposable_local_execution_is_explicit(monkeypatch):
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    assert local_execution_enabled()


@pytest.mark.parametrize("backend", ["docker", "slurm", "modal", "fake", "unknown"])
async def test_hosted_admission_rejects_non_nebius_before_capacity_lookup(monkeypatch, backend):
    monkeypatch.setenv("LOOM_ENV", "production")
    session = AsyncMock()
    with pytest.raises(HTTPException) as error:
        await _reject_if_backend_cannot_execute_or_cold_start(
            session, backend=backend, task_ids=[], trial_config={}, combinations=[],
            runtime_profile_json="",
        )
    assert error.value.status_code == 400
    assert error.value.detail["reason"] == "unsupported_hosted_backend"
    session.execute.assert_not_awaited()


async def test_hosted_catalog_does_not_consult_legacy_workers(monkeypatch):
    monkeypatch.setenv("LOOM_ENV", "production")
    monkeypatch.setattr("loom_service.routes.backends.get_service_execution_backend_pools", AsyncMock(return_value=()))
    session = AsyncMock()
    result = await list_backends((session, None))
    assert [item["name"] for item in result["items"]] == ["nebius"]
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("backend", ["docker", "fake", "modal"])
def test_control_plane_cannot_route_hosted_work_to_workers(monkeypatch, backend):
    from loom_control_plane.routes.trials import _resolve_required_worker_pool_for_backend

    monkeypatch.setenv("LOOM_ENV", "production")
    with pytest.raises(HTTPException, match="Hosted execution supports Nebius only"):
        _resolve_required_worker_pool_for_backend(
            batch_backend=backend, requested_pool=None, task_service_pool=None,
        )


@pytest.mark.parametrize("environment", ["staging", "production"])
def test_hosted_environment_cannot_enable_local_workers(monkeypatch, environment):
    monkeypatch.setenv("LOOM_ENV", environment)
    monkeypatch.setenv("LOOM_LOCAL_EXECUTION", "1")
    assert not local_execution_enabled()
