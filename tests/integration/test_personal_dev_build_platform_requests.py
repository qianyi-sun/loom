"""Cold native requests are durable management work, never allocation grants."""

import asyncio
from dataclasses import replace
from datetime import timedelta
from importlib import import_module
from uuid import uuid4

import pytest
from alembic import command
from sqlalchemy import create_engine, inspect, select, text, update
from sqlalchemy.exc import DBAPIError

from loom.personal_dev_build_runtime_installation import resolve_personal_build_runtime_installation
from tests.integration.test_personal_dev_native_builder_migration import _config
from tests.integration.test_personal_dev_native_builder_store import (
    _NOW,
    _seed_running_attempt,
)
from tests.integration.test_personal_dev_native_builder_store import (
    sessions as sessions,
)
from tests.unit.test_capacity_build_membership import build_membership_input
from tests.unit.test_personal_dev_build_runtime_installation import installation_input


def build_service(tmp_path, registration):
    publication, preparation, configs = installation_input(tmp_path)
    runtime = resolve_personal_build_runtime_installation(publication, preparation=preparation, pool_profiles=configs)
    member = build_membership_input().membership.members[-1]
    owner = registration.candidate.owner_user_id
    config = member.configuration.model_copy(update={"account_id": f"dev-owner-{owner.hex}"})
    member = member.model_copy(update={"owner_id": owner, "configuration": config,
        "acknowledgement": member.acknowledgement.model_copy(update={"candidate": publication.candidate})})
    return member, runtime


@pytest.mark.asyncio
async def test_cold_requests_persist_replay_and_emit_both_native_demands(sessions, tmp_path):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        first = await module.stage_platform_requests(session, registration,
            member=member, runtime=runtime, platforms=("linux/arm64", "linux/amd64"), now=_NOW)
    async with sessions.begin() as session:
        replay = await module.stage_platform_requests(session, registration,
            member=member, runtime=runtime, platforms=("linux/amd64", "linux/arm64"), now=_NOW + timedelta(seconds=1))
        assert tuple(row.id for row in first) == tuple(row.id for row in replay)
        demands = await module.pending_platform_demand(session, member=member, runtime=runtime, now=_NOW)
    assert {bucket.eligible_pool_ids for bucket in demands} == {("gb10",), ("oldlab",)}
    assert all(bucket.requested_slots == 1 for bucket in demands)
    async with sessions() as session:
        from loom.db.schema import PersonalDevNativeBuilderAgent, PersonalDevNativeBuildGrant

        assert list((await session.scalars(select(PersonalDevNativeBuildGrant))).all()) == []
        assert list((await session.scalars(select(PersonalDevNativeBuilderAgent))).all()) == []


@pytest.mark.asyncio
async def test_cancelled_or_expired_platforms_do_not_emit_new_demand(sessions, tmp_path):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64", "linux/amd64"), now=_NOW)
        assert await module.cancel_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW + timedelta(seconds=1)) == 1
        remaining = await module.pending_platform_demand(session, member=member, runtime=runtime, now=_NOW + timedelta(seconds=2))
        assert len(remaining) == 1 and remaining[0].eligible_pool_ids == ("oldlab",)
        assert await module.pending_platform_demand(session, member=member, runtime=runtime,
            now=registration.build_attempt.lease_expires_at) == ()
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="cancelled"):
            await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
                platforms=("linux/arm64",), now=_NOW + timedelta(seconds=3))


@pytest.mark.asyncio
async def test_concurrent_replay_and_capacity_only_update_reuse_installation(sessions, tmp_path):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)

    async def stage(service):
        async with sessions.begin() as session:
            rows = await module.stage_platform_requests(session, registration, member=service, runtime=runtime,
                platforms=("linux/arm64", "linux/amd64"), now=_NOW)
            return tuple(row.id for row in rows)

    first, second = await asyncio.gather(stage(member), stage(member))
    assert first == second
    generation = member.configuration.configuration_generation + 1
    capacity = member.model_copy(update={"revision": member.revision + 4,
        "configuration": member.configuration.model_copy(update={"max_slots": 1, "configuration_generation": generation}),
        "acknowledgement": member.acknowledgement.model_copy(update={"configuration_generation": generation})})
    assert await stage(capacity) == first
    replacement = capacity.model_copy(update={
        "configuration": capacity.configuration.model_copy(update={"deployment_generation": 2}),
        "acknowledgement": capacity.acknowledgement.model_copy(update={"deployment_generation": 2})})
    with pytest.raises(ValueError, match="installation or identity changed"):
        await stage(replacement)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("owner", "source", "lease"))
async def test_re_read_rejects_foreign_or_stale_source_without_staging(sessions, tmp_path, boundary):
    module = import_module("loom.personal_dev_build_platform_requests")
    from loom.db.schema import PersonalDevBuildPlatformRequest, PersonalDevCandidateBuildAttempt

    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    if boundary == "owner":
        owner = uuid4()
        member = member.model_copy(update={"owner_id": owner,
            "configuration": member.configuration.model_copy(update={"account_id": f"dev-owner-{owner.hex}"})})
    elif boundary == "source":
        registration = replace(registration, candidate=replace(registration.candidate,
            archive_size_bytes=registration.candidate.archive_size_bytes + 1))
    else:
        async with sessions.begin() as session:
            await session.execute(update(PersonalDevCandidateBuildAttempt).where(
                PersonalDevCandidateBuildAttempt.id == registration.build_attempt.id).values(lease_epoch=2))
    async with sessions.begin() as session:
        with pytest.raises(ValueError):
            await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
                platforms=("linux/arm64", "linux/amd64"), now=_NOW)
        assert list((await session.scalars(select(PersonalDevBuildPlatformRequest))).all()) == []


@pytest.mark.asyncio
async def test_disabled_service_can_cancel_but_not_requeue(sessions, tmp_path):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW)
        disabled = member.model_copy(update={"configuration": member.configuration.model_copy(update={
            "lifecycle_state": "disabled", "max_slots": 0})})
        assert await module.cancel_platform_requests(session, registration, member=disabled, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW + timedelta(seconds=1)) == 1
        assert await module.cancel_platform_requests(session, registration, member=disabled, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW + timedelta(seconds=2)) == 0
        with pytest.raises(ValueError, match="disabled"):
            await module.stage_platform_requests(session, registration, member=disabled, runtime=runtime,
                platforms=("linux/arm64",), now=_NOW + timedelta(seconds=3))


@pytest.mark.asyncio
async def test_cancellation_before_staging_permanently_closes_that_platform_lease(sessions, tmp_path):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        await module.cancel_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW)
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="cancelled"):
            await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
                platforms=("linux/arm64",), now=_NOW + timedelta(seconds=1))


@pytest.mark.asyncio
async def test_concurrent_cancel_and_stage_leave_no_runnable_request(sessions, tmp_path):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)

    async def stage():
        async with sessions.begin() as session:
            try:
                await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
                    platforms=("linux/arm64",), now=_NOW)
            except ValueError as exc:
                assert "cancelled" in str(exc)

    async def cancel():
        async with sessions.begin() as session:
            assert await module.cancel_platform_requests(session, registration, member=member, runtime=runtime,
                platforms=("linux/arm64",), now=_NOW) == 1

    await asyncio.gather(stage(), cancel())
    async with sessions() as session:
        assert await module.pending_platform_demand(session, member=member, runtime=runtime, now=_NOW) == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ("future_epoch", "claimant"))
async def test_cancellation_rejects_unissued_lease_identity(sessions, tmp_path, boundary):
    module = import_module("loom.personal_dev_build_platform_requests")
    from loom.db.schema import PersonalDevBuildPlatformRequest

    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    attempt = registration.build_attempt
    forged = replace(registration, build_attempt=replace(attempt,
        **({"lease_epoch": attempt.lease_epoch + 1} if boundary == "future_epoch"
           else {"claimed_by": "other-builder"})))
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="lease identity changed"):
            await module.cancel_platform_requests(session, forged, member=member, runtime=runtime,
                platforms=("linux/arm64",), now=_NOW)
        assert list((await session.scalars(select(PersonalDevBuildPlatformRequest))).all()) == []
        rows = await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW)
        assert rows[0].cancelled_at is None


@pytest.mark.asyncio
async def test_expired_lease_tombstone_does_not_cancel_next_lease(sessions, tmp_path):
    module = import_module("loom.personal_dev_build_platform_requests")
    from loom.db.schema import PersonalDevCandidateBuildAttempt

    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    expired = registration.build_attempt.lease_expires_at
    next_epoch = registration.build_attempt.lease_epoch + 1
    async with sessions.begin() as session:
        assert await module.cancel_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=expired) == 1
        await session.execute(update(PersonalDevCandidateBuildAttempt).where(
            PersonalDevCandidateBuildAttempt.id == registration.build_attempt.id).values(
                lease_epoch=next_epoch, lease_expires_at=expired + timedelta(seconds=60)))
        renewed = replace(registration, build_attempt=replace(registration.build_attempt,
            lease_epoch=next_epoch, lease_expires_at=expired + timedelta(seconds=60)))
        rows = await module.stage_platform_requests(session, renewed, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=expired)
        assert rows[0].cancelled_at is None
        # Delayed cleanup of an issued historical lease must remain possible.
        assert await module.cancel_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/amd64",), now=expired) == 1
        assert len(await module.pending_platform_demand(session, member=member, runtime=runtime, now=expired)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("statement", (
    "UPDATE personal_dev_build_platform_requests SET source_binding_sha256 = repeat('c',64)",
    "DELETE FROM personal_dev_build_platform_requests",
    "TRUNCATE personal_dev_build_platform_requests",
))
async def test_sql_preserves_request_identity_and_recovery_history(sessions, tmp_path, statement):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW)
        with pytest.raises(DBAPIError, match=r"immutable|cannot be removed"):
            async with session.begin_nested():
                await session.execute(text(statement))
        assert len(await module.pending_platform_demand(session, member=member, runtime=runtime, now=_NOW)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("cancelled", (False, True))
async def test_0142_rollback_refuses_any_retained_request(sessions, tmp_path, isolated_migration_postgres_url, cancelled):
    module = import_module("loom.personal_dev_build_platform_requests")
    registration = await _seed_running_attempt(sessions, now=_NOW)
    member, runtime = build_service(tmp_path, registration)
    async with sessions.begin() as session:
        await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
            platforms=("linux/arm64",), now=_NOW)
        if cancelled:
            await module.cancel_platform_requests(session, registration, member=member, runtime=runtime,
                platforms=("linux/arm64",), now=_NOW + timedelta(seconds=1))
    with pytest.raises(DBAPIError, match="cannot downgrade 0142"):
        await asyncio.to_thread(command.downgrade, _config(isolated_migration_postgres_url), "0141")
    async with sessions() as session:
        assert await session.scalar(text("SELECT version_num FROM alembic_version")) == "0142"
        assert await session.scalar(text("SELECT count(*) FROM personal_dev_build_platform_requests")) == 1


def test_0142_empty_rollback_and_model_parity(isolated_migration_postgres_url):
    from loom.db.schema import PersonalDevBuildPlatformRequest

    config = _config(isolated_migration_postgres_url)
    engine = create_engine(isolated_migration_postgres_url)
    try:
        command.downgrade(config, "0141")
        assert not inspect(engine).has_table("personal_dev_build_platform_requests")
        command.upgrade(config, "0142")
        inspector = inspect(engine)
        columns = inspector.get_columns("personal_dev_build_platform_requests")
        expected = PersonalDevBuildPlatformRequest.__table__.columns
        assert {column["name"] for column in columns} == set(expected.keys())
        assert {column["name"] for column in columns if column["nullable"]} == {"cancelled_at"}
        assert {item["name"] for item in inspector.get_check_constraints("personal_dev_build_platform_requests")} == {
            "personal_build_request_identity_check", "personal_build_request_digest_check", "personal_build_request_time_check"}
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT version_num FROM alembic_version")) == "0142"
            assert connection.scalar(text("SELECT count(*) FROM pg_trigger WHERE tgrelid = 'personal_dev_build_platform_requests'::regclass AND NOT tgisinternal")) == 2
    finally:
        engine.dispose()
