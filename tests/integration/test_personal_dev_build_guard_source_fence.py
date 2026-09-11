"""The protected build guard rechecks source and lease under physical row locks."""

import json
from datetime import UTC, datetime
from hashlib import sha256

import pytest
from alembic import command
from sqlalchemy import text

from loom.personal_dev_build_platform_requests import stage_platform_requests
from loom_capacity_build_guard.installation_store import BuildGuardInstallationStore
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_platform_requests import build_service
from tests.integration.test_personal_dev_native_builder_store import _seed_running_attempt
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("boundary", ["current", "source", "lease", "expired", "installation", "cancelled", "wire"])
async def test_protected_source_fence_rechecks_exact_durable_request(
    build_guard_database, owner_sessions, sessions, tmp_path, boundary,
):
    from loom.personal_dev_build_platform_requests import canonical_build_source

    _config, engine, owner, _agent, _url = build_guard_database
    now = datetime.now(UTC)
    registration = await _seed_running_attempt(sessions, now=now)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        requests = await stage_platform_requests(session, registration,
            member=member, runtime=runtime, platforms=("linux/arm64",), now=now)
    owner_factory, _owner = owner_sessions
    with engine.begin() as connection:
        quote = engine.dialect.identifier_preparer.quote
        for table in ("personal_dev_candidates", "personal_dev_candidate_build_attempts", "personal_dev_build_platform_requests"):
            connection.exec_driver_sql(f"GRANT SELECT, UPDATE (id) ON public.{table} TO {quote(owner)}")
        if boundary == "source":
            connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"), {"id": registration.candidate.id})
        elif boundary == "lease":
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_epoch=lease_epoch+1 WHERE id=:id"), {"id": registration.build_attempt.id})
        elif boundary == "expired":
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=now() - interval '1 second' WHERE id=:id"), {"id": registration.build_attempt.id})
        elif boundary == "cancelled":
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": requests[0].id})
    wire = canonical_build_source(registration)
    payload = json.loads(wire)
    if boundary == "wire":
        wire = b" " + wire
    if boundary == "installation":
        from dataclasses import replace

        runtime = replace(runtime, release_evidence_sha256="f" * 64)
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner}"))
        retained = await BuildGuardInstallationStore(session, expected_owner_role=owner).retain(member=member, runtime=runtime)
        statement = text("""SELECT loom_capacity_build_guard.assert_current_source(
            :installation, :request, CAST(:payload AS jsonb), :wire, :digest)
        """)
        parameters = {"installation": retained.id, "request": requests[0].id,
            "payload": json.dumps(payload), "wire": wire, "digest": sha256(wire).hexdigest()}
        if boundary == "current":
            assert await session.scalar(statement, parameters) == registration.build_attempt.lease_expires_at
            # The successful check retains source locks through the caller's
            # transaction, so publication cannot race a source mutation.
            from sqlalchemy.exc import DBAPIError

            with engine.begin() as connection:
                connection.execute(text("SET LOCAL lock_timeout='100ms'"))
                with pytest.raises(DBAPIError, match="lock timeout"):
                    connection.execute(text("UPDATE personal_dev_candidates SET archive_size_bytes=archive_size_bytes+1 WHERE id=:id"), {"id": registration.candidate.id})
        else:
            from sqlalchemy.exc import DBAPIError

            with pytest.raises(DBAPIError, match=r"source|lease|request"):
                await session.execute(statement, parameters)


def test_source_fence_is_not_agent_callable(build_guard_database):
    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    with engine.connect() as connection:
        assert connection.scalar(text("""SELECT NOT has_function_privilege(:agent,
            'loom_capacity_build_guard.assert_current_source(uuid,uuid,jsonb,bytea,text)', 'EXECUTE')
        """), {"agent": agent})
