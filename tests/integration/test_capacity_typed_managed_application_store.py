"""Managed applications adopt pinned origins through the same typed SQL boundary."""

from uuid import UUID

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

from loom_capacity_manager.models import CapacityCandidate, CapacityDemandReporter
from loom_capacity_manager.store import WriterFence
from loom_capacity_manager.typed_membership_store import CapacityTypedMembershipStore
from tests.capacity_build_membership_fixtures import managed_application_request, staged_build_event
from tests.integration.test_capacity_mixed_membership_store import apply, transition
from tests.integration.test_capacity_typed_managed_base_history import prepared


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
async def test_typed_store_adopts_managed_base_without_recreating_it(capacity_session, operation):
    management, preparation, _fleet, execution = await prepared(capacity_session)
    request = managed_application_request(preparation, execution, operation=operation)
    receipt = await apply(capacity_session, request)
    assert receipt.revision == 1 and receipt.member.configuration.configuration_generation == 2
    assert receipt.member.configuration.subject_id == preparation.managed_application_origins[0].configuration.subject_id
    assert await apply(capacity_session, request) == receipt.model_copy(update={"replayed": True})
    value = await management.load_allocation_input(capacity_session,
        WriterFence(authority_incarnation=execution.authority_incarnation, writer_epoch=execution.writer_epoch))
    assert value.membership.members == (receipt.member,)
    assert sum(subject.configuration.subject_id == receipt.member.configuration.subject_id for subject in value.subjects) == 1


async def test_typed_managed_capacity_update_destroy_preserves_original_installation_and_reporter_history(capacity_session):
    _management, preparation, _fleet, execution = await prepared(capacity_session)
    origin = preparation.managed_application_origins[0]
    request = managed_application_request(preparation, execution)
    first = await apply(capacity_session, request)
    updated_request = transition(request, "update", revision=1)
    await apply(capacity_session, updated_request, key=100002)
    await apply(capacity_session, transition(updated_request, "destroy", revision=2), key=100003)
    assert (await CapacityTypedMembershipStore().snapshot(capacity_session, execution.execution_epoch, through_revision=1)).members == (first.member,)
    assert (await apply(capacity_session, request)).replayed
    candidate = (await capacity_session.scalars(select(CapacityCandidate).where(
        CapacityCandidate.subject_id == origin.configuration.subject_id,
        CapacityCandidate.candidate_generation == origin.configuration.candidate_generation))).one()
    assert candidate.attestation_payload["operation_id"] == str(origin.installation_projection.operation_id)
    reporter = (await capacity_session.scalars(select(CapacityDemandReporter).where(
        CapacityDemandReporter.reporter_incarnation == origin.configuration.demand_reporter_incarnation))).one()
    assert reporter.state == "fenced" and reporter.configuration_generation == 2
    assert reporter.token_sha256 == origin.base_projection.demand_reporter_token_sha256


@pytest.mark.parametrize("operation", ("capacity", "update", "destroy"))
async def test_sql_admits_exact_first_managed_lifecycle(capacity_session, operation):
    management, preparation, fleet, execution = await prepared(capacity_session)
    row = await staged_build_event(capacity_session, management, preparation, fleet,
        managed_application_request(preparation, execution, operation=operation), idempotency_key=UUID(int=88901))
    capacity_session.add(row)
    await capacity_session.flush()


@pytest.mark.parametrize("changed", ("configuration_generation=99", "token_sha256=repeat('1',64)", "state='current'"))
async def test_sql_first_managed_update_requires_exact_fenced_base_reporter(capacity_session, changed):
    management, preparation, fleet, execution = await prepared(capacity_session)
    row = await staged_build_event(capacity_session, management, preparation, fleet,
        managed_application_request(preparation, execution, operation="update"), idempotency_key=UUID(int=88901))
    reporter = preparation.managed_application_origins[0].configuration.demand_reporter_incarnation
    await capacity_session.execute(text(f"UPDATE capacity_demand_reporters SET {changed} WHERE reporter_incarnation=:reporter"), {"reporter": reporter})
    with pytest.raises(DBAPIError) as error:
        async with capacity_session.begin_nested():
            capacity_session.add(row)
            await capacity_session.flush()
    assert error.value.orig.sqlstate == "23514"
