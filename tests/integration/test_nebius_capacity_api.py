"""Real CP HTTP quota ingestion and reservation rollback against PostgreSQL."""

from __future__ import annotations

import base64
import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import httpx
from cryptography.hazmat.primitives import serialization
from fastapi import FastAPI
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.admin_secret import AdminSecretVerifier
from loom.db.schema import (
    AdminAuditEvent,
    ExecutionAdmissionReservation,
    ExecutionCapacityObservation,
    ExecutionCostReservation,
    ServiceExecutionLease,
    Token,
    Trial,
)
from loom_control_plane.routes import admin, service_executions
from tests.execution_placement_fixtures import placement_fixture
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401 -- shared disposable fixture
    _requirements,
    _runtime_contract,
    _seed_ready_trial,
)
from tests.support.execution_image_admission import _PRIVATE_KEY


async def test_collector_zero_quota_blocks_http_reservation_then_growth_recovers(
    postgres_url: str,
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    app = FastAPI()
    app.state.session_factory = sessions
    admin_token = "loom_admin_" + "e" * 64
    app.state.admin_secret_verifier = AdminSecretVerifier.from_token(admin_token)
    app.state.settings = SimpleNamespace(
        execution_image_admission_public_keys_json=json.dumps(
            {
                "schema_version": 1,
                "keys": [
                    {
                        "signing_key_id": "test-builder",
                        "public_key_base64": base64.b64encode(
                            _PRIVATE_KEY.public_key().public_bytes(
                                serialization.Encoding.Raw, serialization.PublicFormat.Raw
                            )
                        ).decode(),
                    }
                ],
            }
        )
    )
    app.include_router(admin.router)
    app.include_router(service_executions.router)
    now = datetime.now(UTC)
    collector_hash = None
    target_id = None
    try:
        async with sessions() as session:
            trial_id, target = await _seed_ready_trial(session, now=now)
            target_id = target.target_id
            row = await session.scalar(
                select(ExecutionCapacityObservation).where(
                    ExecutionCapacityObservation.target_id == target_id
                )
            )
            assert row is not None
            original = deepcopy(row.observation_json)
            original.pop("schema_version")
            original.pop("provider")
            await session.commit()
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, raise_app_exceptions=False),
            base_url="http://capacity.test",
            headers={"Authorization": "Bearer " + admin_token},
        ) as client:
            issued = await client.post("/admin/execution-capacity-collector-tokens", json={})
            assert issued.status_code == 201, issued.text
            collector_token = issued.json()["token"]
            collector_hash = hashlib.sha256(collector_token.encode()).digest()
            collector_headers = {"Authorization": "Bearer " + collector_token}
            zero = deepcopy(original)
            zero.update(
                source_version="zero",
                observed_at=(now + timedelta(seconds=1)).isoformat(),
                active_nodes=0,
                node_states=None,
            )
            for key in zero:
                if key.startswith(
                    ("provider_quota_", "provider_used_", "provisioned_", "allocatable_")
                ):
                    zero[key] = 0
            zero["placement"] = placement_fixture(
                target_id=target_id, nodes=0, used_nodes=0, quota_nodes=0
            )
            response = await client.post(
                "/admin/execution-capacity-observations", json=zero, headers=collector_headers
            )
            assert response.status_code == 200, response.text
            status = await client.get("/admin/execution-capacity/status")
            assert status.status_code == 200, status.text
            projected = next(
                row for row in status.json()["targets"] if row["target_id"] == target_id
            )
            assert "execution_capacity_provider_quota_nodes_exceeded" in projected["blockers"]
            body = {
                "request_id": str(uuid4()),
                "trial_id": str(trial_id),
                "execution_class_id": target.execution_class_id,
                "target_id": target_id,
                "requirements": _requirements().model_dump(mode="json"),
                "runtime_contract": _runtime_contract(now=now).model_dump(mode="json"),
                "deadline_at": (now + timedelta(hours=1)).isoformat(),
            }
            # The collector may observe capacity but cannot acquire execution authority.
            denied = await client.post(
                "/admin/service-execution/reservations", json=body, headers=collector_headers
            )
            assert denied.status_code == 403
            blocked = await client.post("/admin/service-execution/reservations", json=body)
            assert blocked.status_code == 409, blocked.text
            assert blocked.headers["Retry-After"] == "15"
            assert blocked.json()["detail"] == "execution_capacity_provider_quota_nodes_exceeded"
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                assert trial is not None and trial.state == "queued" and trial.attempt_count == 0
                assert trial.claimed_at is None
                for model in (
                    ServiceExecutionLease,
                    ExecutionCostReservation,
                    ExecutionAdmissionReservation,
                ):
                    assert (
                        await session.scalar(
                            select(func.count())
                            .select_from(model)
                            .where(model.trial_id == trial_id)
                        )
                        == 0
                    )
            restored = deepcopy(original)
            restored.update(
                source_version="grown", observed_at=(now + timedelta(seconds=2)).isoformat()
            )
            observed = await client.post(
                "/admin/execution-capacity-observations", json=restored, headers=collector_headers
            )
            assert observed.status_code == 200, observed.text
            admitted = await client.post("/admin/service-execution/reservations", json=body)
            assert admitted.status_code == 201, admitted.text
            async with sessions() as session:
                trial = await session.get(Trial, trial_id)
                assert trial is not None and trial.state == "claimed" and trial.attempt_count == 1
                assert (
                    await session.scalar(
                        select(func.count())
                        .select_from(ServiceExecutionLease)
                        .where(ServiceExecutionLease.trial_id == trial_id)
                    )
                    == 1
                )
    finally:
        async with sessions() as session, session.begin():
            if collector_hash is not None:
                await session.execute(delete(Token).where(Token.token_hash == collector_hash))
            if target_id is not None:
                await session.execute(
                    delete(AdminAuditEvent).where(
                        AdminAuditEvent.action == "execution.capacity_observation.recorded",
                        AdminAuditEvent.event_metadata["target_id"].astext == target_id,
                    )
                )
        await engine.dispose()
