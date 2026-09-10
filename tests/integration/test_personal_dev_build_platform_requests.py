"""Cold native requests are durable management work, never allocation grants."""

from datetime import timedelta
from importlib import import_module

import pytest
from sqlalchemy import select

from loom.personal_dev_build_runtime_installation import resolve_personal_build_runtime_installation
from tests.integration.test_personal_dev_native_builder_store import (
    _NOW,
    _seed_running_attempt,
    sessions,  # noqa: F401 - shared isolated database fixture
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
        from loom.db.schema import PersonalDevNativeBuildGrant, PersonalDevNativeBuilderAgent

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
        assert await module.cancel_platform_requests(session, registration, member=member,
            platforms=("linux/arm64",), now=_NOW + timedelta(seconds=1)) == 1
        remaining = await module.pending_platform_demand(session, member=member, runtime=runtime, now=_NOW + timedelta(seconds=2))
        assert len(remaining) == 1 and remaining[0].eligible_pool_ids == ("oldlab",)
        assert await module.pending_platform_demand(session, member=member, runtime=runtime,
            now=registration.build_attempt.lease_expires_at) == ()
    async with sessions.begin() as session:
        with pytest.raises(ValueError, match="cancelled"):
            await module.stage_platform_requests(session, registration, member=member, runtime=runtime,
                platforms=("linux/arm64",), now=_NOW + timedelta(seconds=3))
