"""Installed startup reads current boot admission through the existing claim."""

from datetime import UTC, datetime, timedelta
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_native_recovery_publication import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_native_recovery_publication import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_native_recovery_publication import (
    prepared_input as prepared_input,
)
from tests.integration.test_native_recovery_publication import (
    recovery_input,
)
from tests.integration.test_native_recovery_publication import (
    sessions as sessions,
)
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL


@pytest.mark.parametrize("boundary", ["exact", "credential", "boot", "no-profile", "expired", "claim"])
async def test_recovery_admission_requires_live_exact_claim_and_committed_boot(prepared_input, owner_sessions, monkeypatch, boundary):
    contracts = import_module("loom_capacity_agent.native_recovery_publication")
    _contracts, claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch,
        admit=boundary != "no-profile")
    factory, engine, installation, _proposal, source, *_ = prepared_input
    request = contracts.NativeRecoveryAdmissionRequestV1(claim=claim, node_id=prepared.node_id,
        boot_id=uuid4() if boundary == "boot" else prepared.boot_id)
    if boundary == "claim":
        request = request.model_copy(update={"claim": claim.model_copy(update={"operation_id": uuid4()})})
    if boundary == "expired":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=:deadline WHERE id=:id"),
                {"id": source.build_attempt.id, "deadline": datetime.now(UTC) - timedelta(seconds=1)})
    async with factory.begin() as session:
        if boundary == "exact":
            result = await store(session, installation).read_recovery_admission(request, worker_credential=CREDENTIAL)
            assert result.request == request and result.profile == profile
            assert result.host.boot_id == prepared.boot_id and result.host.node_id == prepared.node_id
        else:
            with pytest.raises((ValueError, DBAPIError)):
                await store(session, installation).read_recovery_admission(request,
                    worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_records")) == 0


@pytest.mark.parametrize("boundary", ["exact", "disabled", "credential", "http"])
async def test_installed_admission_client_uses_authenticated_no_store_readback(prepared_input, owner_sessions, tmp_path, monkeypatch, boundary):
    import httpx

    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for

    contracts, claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "disabled" if boundary == "disabled" else "native-execution"
    request = contracts.NativeRecoveryAdmissionRequestV1(claim=claim, node_id=prepared.node_id, boot_id=prepared.boot_id)
    responses = []

    async def observe(response):
        responses.append(response)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), event_hooks={"response": [observe]}) as http:
        client = client_for(http, claim)
        client._token = "executor-secret"
        if boundary == "http":
            client._origin = "http://management.test"
        if boundary == "exact":
            result = await client.read_recovery_admission(request, worker_credential=CREDENTIAL)
            assert result.request == request and result.profile == profile
            assert responses[-1].headers["cache-control"] == "no-store"
        else:
            from loom_capacity_executor.build_admission_client import BuildAdmissionTransportError

            with pytest.raises(BuildAdmissionTransportError):
                await client.read_recovery_admission(request, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
