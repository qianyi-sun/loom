"""Only a committed final observation may bind fresh recovery-capable execution."""

from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy.exc import DBAPIError

from tests.integration.test_native_recovery_publication import recovery_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["exact", "missing", "uncommitted", "digest", "credential", "source", "closed"])
async def test_recovery_permission_binds_committed_finalization_and_live_claim(prepared_input, owner_sessions, monkeypatch, boundary):
    from loom_capacity_agent.build_admission import BuildOutcomeRequestV1

    contracts, claim, _profile, preparation, final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    execution = import_module("loom_capacity_agent.native_recovery_execution")
    factory, _engine, installation, _plan, _source, platform = prepared_input
    async with factory.begin() as session:
        await store(session, installation).publish_recovery(contracts.NativeRecoveryPublicationV1(claim=claim, record=preparation), worker_credential=CREDENTIAL)
    final_request = contracts.NativeRecoveryPublicationV1(claim=claim, record=final)
    from loom_capacity_manager.contracts import canonical_digest

    if boundary not in {"missing", "uncommitted"}:
        async with factory.begin() as session:
            await store(session, installation).publish_recovery(final_request, worker_credential=CREDENTIAL)
    if boundary == "closed":
        async with factory.begin() as session:
            await store(session, installation).record_outcome(BuildOutcomeRequestV1(claim=claim, operation_id=uuid4(), result="failed"), worker_credential=CREDENTIAL)
    request = execution.BuildExecutionRequestV2(claim=claim, challenge=uuid4(),
        source_binding_sha256="f" * 64 if boundary == "source" else platform.source_binding_sha256,
        recovery_finalization_sha256="f" * 64 if boundary == "digest" else canonical_digest(final_request))
    async with factory.begin() as session:
        guard = store(session, installation)
        if boundary == "uncommitted":
            await guard.publish_recovery(final_request, worker_credential=CREDENTIAL)
        if boundary != "exact":
            with pytest.raises((ValueError, DBAPIError)):
                await guard.authorize_recovery_execution(request, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
        else:
            permit = await guard.authorize_recovery_execution(request, worker_credential=CREDENTIAL)
            assert permit.request == request
            assert 0 < (permit.not_after - permit.issued_at).total_seconds() <= 10


async def test_recovery_permission_round_trips_authenticated_http(prepared_input, owner_sessions, tmp_path, monkeypatch):
    import httpx

    from loom_capacity_manager.contracts import canonical_digest
    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for

    contracts, claim, _profile, preparation, final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    execution = import_module("loom_capacity_agent.native_recovery_execution")
    factory, _engine, installation, _plan, _source, platform = prepared_input
    final_request = contracts.NativeRecoveryPublicationV1(claim=claim, record=final)
    for record in (preparation, final):
        async with factory.begin() as session:
            await store(session, installation).publish_recovery(contracts.NativeRecoveryPublicationV1(claim=claim, record=record), worker_credential=CREDENTIAL)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "native-execution"
    request = execution.BuildExecutionRequestV2(claim=claim, challenge=uuid4(),
        source_binding_sha256=platform.source_binding_sha256, recovery_finalization_sha256=canonical_digest(final_request))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app)) as http:
        client = client_for(http, claim)
        client._token = "executor-secret"
        permit = await client.authorize_recovery_execution(request, worker_credential=CREDENTIAL)
        assert permit.request == request
