"""Authenticated pool admission commits before a reply can leave management."""

from importlib import import_module
from uuid import uuid4

import httpx
import pytest
from fastapi import FastAPI
from sqlalchemy import text

from loom_capacity_manager.auth import CapacityPrincipalVerifier
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from tests.integration.test_personal_dev_build_guard_execution import admitted, physical
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions
from tests.unit.test_capacity_auth import _pool_executor, _write_registry


def application(prepared_input, tmp_path):
    factory, _engine, _installation, plan, _source, _request = prepared_input
    binding = plan.shapes[0].binding
    document = _pool_executor()
    document.update(pool_id=binding.pool_id,executor_id=binding.executor_id,
        executor_incarnation=str(binding.executor_incarnation),executor_pool_generation=binding.pool_generation)
    verifier = CapacityPrincipalVerifier.from_pool_executor_file(
        _write_registry(tmp_path / "admission-principals.json", [document]))
    app = FastAPI()
    app.state.personal_dev_build_admission_sessions = factory
    app.state.personal_dev_build_admission_verifier = verifier
    app.include_router(import_module("loom_service.routes.personal_dev_build_admission").router,
        prefix="/api/v1/internal")
    return app


def route(registration, operation):
    return f"/api/v1/internal/capacity-build/pools/{registration.binding.pool_id}/intents/{registration.binding.intent_id}/{operation}"


def preparation(registration, digest):
    return {"schema_version":1,"registration":registration.model_dump(mode="json"),"bootstrap_sha256":digest}


async def test_http_prepare_bind_and_lost_reply_replay_are_committed(prepared_input, tmp_path):
    _factory, engine, _installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    app = application(prepared_input, tmp_path)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test",
        headers={"Authorization":"Bearer executor-secret"}) as client:
        first = await client.post(route(registration,"prepare"),json=preparation(registration,digest))
        assert first.status_code == 200, first.text
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 1
        assert "executor-secret" not in first.text
        assert "postgresql" not in first.text
        replay = await client.post(route(registration,"prepare"),json=preparation(registration,digest))
        assert replay.content == first.content
        binding = physical(registration)
        bound = await client.post(route(registration,"bind"),content=canonical_executable_bytes(binding))
        assert bound.status_code == 200, bound.text
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 2
        replay = await client.post(route(registration,"bind"),content=canonical_executable_bytes(binding))
        assert replay.content == bound.content


@pytest.mark.parametrize("boundary", ["credential", "path-pool", "path-intent", "pool-generation", "executor", "incarnation", "subject", "body", "oversized", "http"])
async def test_http_rejects_untrusted_admission_without_writes(prepared_input, tmp_path, boundary):
    _factory, engine, _installation, _plan, _source, _request = prepared_input
    registration, digest = await admitted(prepared_input)
    app = application(prepared_input,tmp_path)
    url = route(registration,"prepare")
    token = "wrong" if boundary=="credential" else "executor-secret"
    if boundary=="path-pool":
        url = url.replace(f"/pools/{registration.binding.pool_id}/", "/pools/foreign/")
    elif boundary=="path-intent":
        url = url.replace(str(registration.binding.intent_id),str(uuid4()))
    elif boundary in {"pool-generation", "executor", "incarnation", "subject"}:
        changes = {"pool-generation":{"pool_generation":999},"executor":{"executor_id":"foreign"},
            "incarnation":{"executor_incarnation":uuid4()},"subject":{"subject_id":uuid4()}}[boundary]
        registration = registration.model_copy(update={"binding":registration.binding.model_copy(update=changes)})
    base = "http://management.test" if boundary=="http" else "https://management.test"
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url=base,
        headers={"Authorization":f"Bearer {token}"}) as client:
        if boundary in {"body", "oversized"}:
            reply = await client.post(url,content=b"{" if boundary=="body" else b"x"*(1024*1024+1))
        else:
            reply = await client.post(url,json=preparation(registration,digest))
    assert reply.status_code in {400,401,403,409,413}, reply.text
    assert "postgresql" not in reply.text
    assert "executor-secret" not in reply.text
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 0


async def test_http_defaults_closed_without_private_configuration(tmp_path):
    app = FastAPI()
    app.include_router(import_module("loom_service.routes.personal_dev_build_admission").router,prefix="/api/v1/internal")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test") as client:
        result = await client.post(f"/api/v1/internal/capacity-build/pools/gb10/intents/{uuid4()}/prepare",content=b"{}")
    assert result.status_code == 503


async def test_real_service_mounts_admission_closed_by_default(monkeypatch):
    from loom_service.app import create_app
    from loom_service.config import LoomServiceSettings
    from tests.unit.test_service_root_landing import _base_env

    for name,value in _base_env().items():
        monkeypatch.setenv(name,value)
    app = create_app(LoomServiceSettings(_env_file=None))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test") as client:
        result = await client.post(f"/api/v1/internal/capacity-build/pools/gb10/intents/{uuid4()}/prepare",content=b"{}")
    assert result.status_code == 503


async def test_http_commit_failure_cannot_emit_preparation_receipt(prepared_input,tmp_path):
    from sqlalchemy import event

    factory, engine, _installation, _plan, _source, _request = prepared_input
    registration,digest = await admitted(prepared_input)
    app = application(prepared_input,tmp_path)

    def fail_commit(session):
        session.execute(text("SELECT 1/0"))

    target = factory.class_.sync_session_class
    event.listen(target,"before_commit",fail_commit)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test",
            headers={"Authorization":"Bearer executor-secret"}) as client:
            result = await client.post(route(registration,"prepare"),json=preparation(registration,digest))
        assert result.status_code == 409
        assert "admission_digest" not in result.text
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.execution_events")) == 0
    finally:
        event.remove(target,"before_commit",fail_commit)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="https://management.test",
        headers={"Authorization":"Bearer executor-secret"}) as client:
        assert (await client.post(route(registration,"prepare"),json=preparation(registration,digest))).status_code == 200
