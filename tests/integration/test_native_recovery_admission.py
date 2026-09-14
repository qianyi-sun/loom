"""Installed startup reads current boot admission through the existing claim."""

from datetime import UTC, datetime, timedelta
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_native_recovery_publication import (
    build_guard_database as build_guard_database,
    owner_sessions as owner_sessions,
    prepared_input as prepared_input,
    recovery_input,
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
