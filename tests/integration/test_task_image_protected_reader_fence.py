"""Real protected ARM claims distinguish native readiness from legacy images.

All state and credentials are disposable SQL fixtures, not native host evidence.
"""

import asyncio
import hashlib
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, insert, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Task, TaskImageMaterialization, Team, TeamQuota, Token, User
from tests.integration.test_capacity_agent_executable_admission import _assign_protected_attempt
from tests.integration.test_capacity_protected_worker_session import (
    _WORKER_CREDENTIAL,
    _project_worker,
    _protected_cp_app,
    _public_registration_payload,
    _seed_protected_worker,
    _seed_worker_bearer,
)
from tests.integration.test_task_image_authority_materializations import _queued_materialization
from tests.integration.test_task_image_publication_completion import _complete, _signed_job
from tests.integration.test_task_image_registry_credentials import (
    registry_issuer as registry_issuer,
)


async def _ready_task(database, issuer, *, native):
    engine = create_async_engine(database["admin_url"])
    try:
        async with async_sessionmaker(engine, expire_on_commit=False)() as session:
            if native:
                values = await _signed_job(session, issuer)
                await _complete(session, values)
                image = (await session.scalars(select(TaskImageMaterialization))).one()
                assert image.ready_publication_operation_id is not None
            else:
                image = await _queued_materialization(session)
                image.state = "ready"
                image.registry_images = {"task": "registry.example/task@sha256:" + "d" * 64}
                assert image.ready_publication_operation_id is None
            assert image.cpu_arch == "arm64"
            session.add(
                Task(
                    id=image.task_id,
                    checksum=image.task_checksum,
                    config=image.task_config,
                    source=image.task_source,
                    source_provenance=image.task_source_provenance,
                )
            )
            await session.commit()
            return image.task_id
    finally:
        await engine.dispose()


def _state(connection, trial_id, intent_id):
    return dict(
        connection.execute(
            text(
                "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                "(SELECT claim_high_water FROM loom_capacity_guard.executable_claim_state "
                " WHERE intent_id = :intent) AS claim_high_water, "
                "(SELECT count(*) FROM public.execution_admission_reservations "
                " WHERE trial_id = :trial) AS reservations, "
                "(SELECT count(*) FROM loom_capacity_guard.executable_claim_leases "
                " WHERE intent_id = :intent) AS claim_leases "
                "FROM public.trials AS trial WHERE trial.id = :trial"
            ),
            {"trial": trial_id, "intent": intent_id},
        )
        .mappings()
        .one()
    )


@pytest.mark.parametrize("protocol", ["trial", "work"])
@pytest.mark.parametrize("native", [False, True])
def test_protected_arm_reader_excludes_native_but_claims_phase1(
    capacity_guard_database,
    registry_issuer,
    monkeypatch,
    tmp_path,
    protocol,
    native,
):
    database = capacity_guard_database
    hostname = "gb10-1"
    seeded = asyncio.run(
        _seed_protected_worker(
            database,
            pool_id="gb10",
            hostname=hostname,
            cpu_arch="arm64",
        )
    )
    projection = _public_registration_payload()
    projection.update(
        hostname=hostname, pool_name="gb10", supported_work_kinds=["trial", "execution_attempt"]
    )
    projection["capabilities"][0]["cpu_arch"] = "arm64"
    projected = asyncio.run(_project_worker(database, payload=projection))
    task_id = asyncio.run(_ready_task(database, registry_issuer, native=native))
    worker_bearer = _seed_worker_bearer(database)
    team_id, user_id = uuid4(), uuid4()
    submit_token = "loom_team_" + uuid4().hex
    engine = create_engine(database["admin_url"])
    try:
        with engine.begin() as connection:
            connection.execute(insert(Team).values(id=team_id, name="reader-" + team_id.hex))
            connection.execute(
                insert(User).values(
                    id=user_id,
                    username="reader-" + user_id.hex,
                    username_normalized="reader-" + user_id.hex,
                    status="active",
                    is_platform_admin=False,
                )
            )
            connection.execute(insert(TeamQuota).values(team_id=team_id))
            connection.execute(
                insert(Token).values(
                    token_hash=hashlib.sha256(submit_token.encode()).digest(),
                    type="team",
                    scopes=["submit"],
                    team_id=team_id,
                    created_by_user_id=user_id,
                    issued_at=datetime.now(UTC),
                    expires_at=None,
                )
            )
        app = _protected_cp_app(database, monkeypatch, tmp_path)
        with TestClient(app) as client:
            submitted = client.post(
                "/trials",
                headers={"Authorization": "Bearer " + submit_token},
                json={
                    "task_id": task_id,
                    "required_worker_pool": "gb10",
                    "config": {"agent_name": "oracle", "agent_model": None},
                },
            )
        assert submitted.status_code == 201, submitted.text
        trial_id = UUID(submitted.json()["trial_id"])
        with engine.connect() as connection:
            attempt = (
                connection.execute(
                    text(
                        "SELECT protected_attempt_id, execution_generation, requirements_digest "
                        "FROM loom_capacity_guard.trial_attempts WHERE trial_id = :trial"
                    ),
                    {"trial": trial_id},
                )
                .mappings()
                .one()
            )
        asyncio.run(
            _assign_protected_attempt(
                database,
                registration=seeded.registration,
                request=seeded.bootstrap,
                protected_attempt_id=attempt["protected_attempt_id"],
                execution_generation=attempt["execution_generation"],
                requirements_digest=attempt["requirements_digest"],
                cpu_arch="arm64",
            )
        )
        with engine.connect() as connection:
            before = _state(connection, trial_id, seeded.worker.binding.intent_id)
        if protocol == "trial":
            route = "/trials/claim"
            payload = {
                "worker_id": str(seeded.worker.worker_id),
                "caps": projection["capabilities"],
            }
        else:
            route = "/work/claim"
            payload = {
                "schema_version": "loom.work-claim-request.v1",
                "worker_id": str(seeded.worker.worker_id),
                "capability_snapshot_digest": projected["capability_snapshot_digest"],
                "supported_work_kinds": ["trial", "execution_attempt"],
                "free_slots": 1,
            }
        with TestClient(app) as client:
            claimed = client.post(
                route,
                json=payload,
                headers={
                    "Authorization": "Bearer " + worker_bearer,
                    "X-Loom-Executor-Worker-Credential": _WORKER_CREDENTIAL,
                },
            )
        if native:
            assert claimed.status_code == 204, claimed.text
            with engine.connect() as connection:
                assert _state(connection, trial_id, seeded.worker.binding.intent_id) == before
        else:
            assert claimed.status_code == 200, claimed.text
            result = claimed.json()["payload"] if protocol == "work" else claimed.json()
            assert result["trial_id"] == str(trial_id)
            assert (
                result["task_image_materialization"]["schema_version"]
                == "loom.task-image-execution-grant.v1"
            )
    finally:
        engine.dispose()
