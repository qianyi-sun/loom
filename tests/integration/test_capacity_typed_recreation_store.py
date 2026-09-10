"""Recreation authenticates actual predecessor release, not caller certificates."""

from uuid import UUID

import pytest
from sqlalchemy import select

from loom_capacity_manager.membership_release import predecessor_release_sha256
from loom_capacity_manager.models import CapacityCandidate, CapacityDemandReporter
from loom_capacity_manager.store import ConfigurationConflictError, WriterFence
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import application_request, build_request, typed_sql_execution
from tests.integration.test_capacity_mixed_membership_store import apply, transition


def recreate(request, *, revision):
    result = transition(request, "create", revision=revision)
    projection = result.command.projection.model_copy(update={
        "subject_incarnation": UUID(int=910000 + revision), "candidate_generation": 1, "deployment_generation": 1,
        "demand_reporter_incarnation": UUID(int=920000 + revision), "demand_reporter_token_sha256": f"{930000 + revision:064x}",
    })
    acknowledgement = result.command.acknowledgement.model_copy(update={
        "subject_incarnation": projection.subject_incarnation, "deployment_generation": 1,
        "reporter_incarnation": projection.demand_reporter_incarnation,
    })
    return result.model_copy(update={"command": result.command.model_copy(update={"projection": projection, "acknowledgement": acknowledgement})})


@pytest.mark.parametrize("build", (False, True))
async def test_typed_recreation_generates_release_evidence_and_retains_old_installations(capacity_session, build):
    management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = (build_request if build else application_request)(preparation, execution)
    created = await apply(capacity_session, request)
    disabled_request = transition(request, "destroy", revision=1)
    disabled = await apply(capacity_session, disabled_request, key=940001)
    old_snapshot = await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)
    recreated_request = recreate(disabled_request, revision=2)
    recreated = await apply(capacity_session, recreated_request, key=940002)
    proof = recreated.member.reincarnation
    assert proof is not None
    assert proof.predecessor == disabled.member.configuration
    assert proof.predecessor_revision == disabled.revision and proof.predecessor_head_sha256 == disabled.head_sha256
    assert proof.origin.subject_incarnation == created.member.configuration.subject_incarnation
    assert proof.release_set_sha256 == await predecessor_release_sha256(capacity_session, disabled.member.configuration)
    resized = await apply(capacity_session, transition(recreated_request, "capacity", revision=3), key=940003)
    assert resized.member.reincarnation == proof
    assert (await apply(capacity_session, recreated_request, key=940002)).replayed
    assert await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch, through_revision=2) == old_snapshot
    value = await management.load_allocation_input(capacity_session, WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.membership.members == (resized.member,)
    candidates = (await capacity_session.scalars(select(CapacityCandidate).where(CapacityCandidate.subject_id == created.member.configuration.subject_id))).all()
    assert {(row.subject_incarnation, row.candidate_generation) for row in candidates} == {
        (created.member.configuration.subject_incarnation, 1), (recreated.member.configuration.subject_incarnation, 1)}
    reporters = (await capacity_session.scalars(select(CapacityDemandReporter).where(CapacityDemandReporter.subject_id == created.member.configuration.subject_id))).all()
    assert {row.state for row in reporters} == {"current", "fenced"}


@pytest.mark.parametrize("build", (False, True))
async def test_typed_recreation_cannot_replace_an_active_predecessor(capacity_session, build):
    _management, preparation, _fleet, execution = await typed_sql_execution(capacity_session)
    request = (build_request if build else application_request)(preparation, execution)
    await apply(capacity_session, request)
    with pytest.raises(ConfigurationConflictError):
        await apply(capacity_session, recreate(request, revision=1), key=940001)
    assert (await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch)).revision == 1
