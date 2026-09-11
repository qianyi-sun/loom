"""Release transport cannot substitute pool or terminal recovery authority."""

import httpx
import pytest
from sqlalchemy import text

from tests.integration.test_personal_dev_build_guard_http import application, route
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registered_release import (
    registered_release_input,
)
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["disabled", "credential", "terminal", "pool", "missing-bearer"])
async def test_release_http_rejects_changed_authentication(prepared_input, tmp_path, monkeypatch, boundary):
    _factory, engine, _installation, *_ = prepared_input
    request, _claim, _terminal, _drain = await registered_release_input(prepared_input, monkeypatch)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "prepare-bind-only" if boundary == "disabled" else "native-claims"
    payload = {"schema_version": 1, "release": request.model_dump(mode="json"),
        "worker_credential": "x" * 43 if boundary == "credential" else CREDENTIAL}
    headers = {} if boundary == "missing-bearer" else {"Authorization": "Bearer executor-secret"}
    target = route(request, "release")
    if boundary == "terminal":
        payload["terminal_inventory_sha256"] = "a" * 64
    elif boundary == "pool":
        pool = request.binding.pool_id
        target = target.replace(f"/pools/{pool}/", f"/pools/{'oldlab' if pool == 'gb10' else 'gb10'}/")
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="https://management.test") as client:
        reply = await client.post(target, json=payload, headers=headers)
    assert reply.status_code == {"disabled": 503, "credential": 409, "terminal": 400,
        "pool": 403, "missing-bearer": 401}[boundary]
    assert CREDENTIAL not in reply.text
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == 0
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
