"""Personal APIs must not become additional shared background-service owners."""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack

import httpx
import pytest
from pydantic import ValidationError
from sqlalchemy import event
from sqlalchemy.ext.asyncio import create_async_engine

from loom_service import app as service_app
from loom_service.config import LoomServiceSettings

_WORKERS = {
    "loom-svc-batch-runner", "loom-svc-taskset-materializer", "loom-svc-taskset-gc",
    "loom-svc-provider-secret-gc", "loom-svc-price-catalogs",
}


def _settings(**changes: object) -> LoomServiceSettings:
    return LoomServiceSettings(**{
        "_env_file": None,
        "db_url": "postgresql+psycopg://loom:loom@offline.invalid/loom",
        "control_plane_url": "https://cp.example.com",
        "gateway_url": "https://gateway.example.com",
        "minio_endpoint": "https://objects.example.com",
        "minio_access_key": "test-access",
        "minio_secret_key": "test-secret",
        **changes,
    })


@pytest.fixture
def process_dependencies(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Replace external I/O; leave app lifecycle and asyncio scheduling real."""
    from loom_service import price_catalogs

    events: list[str] = []

    def engine(*args, **kwargs):
        # SQLAlchemy sessions require a real async engine even for an
        # unauthenticated request that never checks out a connection.
        result = create_async_engine(*args, **kwargs)
        event.listen(result.sync_engine, "engine_disposed", lambda _engine: events.append("engine-closed"))
        return result

    async def schema(_engine: object) -> int:
        events.append("schema-checked")
        return 0

    async def secrets(_session_factory: object) -> int:
        events.append("secrets-checked")
        return 0

    async def worker(**_kwargs: object) -> None:
        await asyncio.Event().wait()

    create_storage = service_app.create_minio_client

    def storage(*args, **kwargs):
        client = create_storage(*args, **kwargs)
        close = client.close

        def recorded_close():
            close()
            events.append("storage-closed")

        monkeypatch.setattr(client, "close", recorded_close)
        return client

    monkeypatch.setattr(service_app, "create_async_engine", engine)
    monkeypatch.setattr(service_app, "create_minio_client", storage)
    monkeypatch.setattr(service_app, "_assert_schema_startup", schema)
    monkeypatch.setattr(service_app, "_assert_secret_store_startup", secrets)
    for name in ("batch_run_loop", "taskset_materializer_run_loop", "taskset_gc_run_loop",
                 "provider_secret_gc_run_loop"):
        monkeypatch.setattr(service_app, name, worker)
    monkeypatch.setattr(price_catalogs, "run_loop", worker)
    monkeypatch.setenv("LOOM_ENV", "development")
    monkeypatch.delenv("LOOM_LOCAL_EXECUTION", raising=False)
    return events


@pytest.mark.parametrize("mode", ["application", "api_only"])
async def test_api_process_owns_workers_only_in_application_mode(mode, process_dependencies) -> None:
    app = service_app.create_app(_settings(service_mode=mode))
    async with app.router.lifespan_context(app):
        await asyncio.sleep(0)
        workers = {task for task in asyncio.all_tasks() if task.get_name() in _WORKERS}
        assert {task.get_name() for task in workers} == (_WORKERS if mode == "application" else set())
        assert process_dependencies == ["schema-checked", "secrets-checked"]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="https://alice.example.com",
        ) as client:
            assert (await client.get("/api/v1/health")).status_code == 200
            assert (await client.get("/api/v1/tasks")).status_code == 401
            assert (await client.get("/api/v1/environments")).status_code == 404
        assert hasattr(app.state, "session_factory")
        clients = (app.state.http_client, app.state.gateway_client)
        assert all(not client.is_closed for client in clients)
    assert process_dependencies[-2:] == ["storage-closed", "engine-closed"]
    assert all(task.done() and task.cancelled() for task in workers)
    assert all(client.is_closed for client in clients)
    assert not hasattr(app.state, "session_factory")


async def test_four_and_fifth_api_instances_do_not_duplicate_shared_workers(process_dependencies) -> None:
    async with AsyncExitStack() as shared:
        owner = service_app.create_app(_settings())
        await shared.enter_async_context(owner.router.lifespan_context(owner))
        await asyncio.sleep(0)
        workers = {task for task in asyncio.all_tasks() if task.get_name() in _WORKERS}
        assert {task.get_name() for task in workers} == _WORKERS
        for index in range(5):
            app = service_app.create_app(_settings(
                service_mode="api_only", public_base_url=f"https://dev-{index}.example.com",
            ))
            context = app.router.lifespan_context(app)
            if index == 4:
                # Replacing one short-lived API must not stop shared workers.
                async with context:
                    await asyncio.sleep(0)
            else:
                await shared.enter_async_context(context)
            assert {task for task in asyncio.all_tasks() if task.get_name() in _WORKERS} == workers
        assert all(not task.done() for task in workers)
    assert all(task.cancelled() for task in workers)
    assert process_dependencies.count("engine-closed") == 6
    assert process_dependencies.count("storage-closed") == 6


@pytest.mark.parametrize("missing", ["minio_access_key", "minio_secret_key"])
def test_api_only_requires_workload_storage_credentials(missing: str) -> None:
    with pytest.raises(ValidationError, match="storage credentials"):
        _settings(service_mode="api_only", **{missing: None})


def test_api_only_cannot_own_environment_provisioning() -> None:
    with pytest.raises(ValidationError, match="environment management configuration"):
        _settings(service_mode="api_only", environment_management_config_file="/protected/install.json",
                  environment_management_github_token="not-a-real-token")


def test_api_only_cannot_reinterpret_an_isolated_child_binding() -> None:
    with pytest.raises(ValidationError, match="child environment configuration"):
        _settings(service_mode="api_only", managed_environment_config_file="/protected/old-child.json")


async def test_api_only_schema_failure_closes_engine_without_serving(
    process_dependencies, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def reject(_engine: object) -> int:
        raise RuntimeError("incompatible shared schema")

    monkeypatch.setattr(service_app, "_assert_schema_startup", reject)
    app = service_app.create_app(_settings(service_mode="api_only"))
    with pytest.raises(RuntimeError, match="incompatible shared schema"):
        async with app.router.lifespan_context(app):
            pytest.fail("an incompatible API started serving")
    assert process_dependencies == ["engine-closed"]
    assert not hasattr(app.state, "session_factory")
    assert not hasattr(app.state, "http_client")


def test_api_only_retains_execution_profile_validation() -> None:
    settings = _settings(service_mode="api_only", service_execution_runtime_profile_json="not-json")
    with pytest.raises(ValueError):
        service_app.create_app(settings)
