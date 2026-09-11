"""Exact cleanup selection cannot act on another owner's older pool work."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from sqlalchemy import select, update

from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapProposalV2,
    ExecutableIntentCloseV2,
    ExecutablePermitConsumptionV2,
    ExecutableReservationAcceptanceV2,
)
from loom_capacity_manager.execution_store import CapacityExecutionStore
from loom_capacity_manager.models import CapacityAuthorityState, CapacityExecutableIntent
from tests.capacity_execution_fixtures import executor_binding
from tests.integration.test_capacity_manager_api import _v2_executor_headers
from tests.integration.test_capacity_manager_api import (
    execution_preparation_api_context as execution_preparation_api_context,
)
from tests.integration.test_capacity_manager_execution_store import (
    _admission_acknowledgement,
    _heartbeat,
)
from tests.integration.test_capacity_membership_admission import _bootstrap_ack, _personal_plan


async def test_exact_cleanup_selects_second_subject_without_touching_older_intent(capacity_session):
    _fixture,active,request = await _personal_plan(capacity_session,base_pending=True,ceiling=2)
    store = CapacityExecutionStore()
    executor = executor_binding("gb10")
    await _heartbeat(store,capacity_session,active,pool_id="gb10")
    for sequence in (1,4):
        proposal = await store.next_pool_work(capacity_session,executor)
        await store.accept_reservation(capacity_session,ExecutableReservationAcceptanceV2(
            execution=proposal.execution,tranche_id=proposal.tranche_id,proposal_digest=store.contract_digest(proposal),
            pool_id=executor.pool_id,pool_generation=executor.pool_generation,executor_id=executor.executor_id,
            executor_incarnation=executor.executor_incarnation,command_sequence=sequence))
        if sequence == 1:
            # Finish first-owner admission/permit consumption so the global
            # rank-order protocol can create the second owner's reservation.
            binding = await store.next_pool_work(capacity_session,executor)
            bootstrap = ExecutableBootstrapProposalV2(binding=binding,command_sequence=2,
                proposal_epoch=1,bootstrap_sha256="7"*64,expires_at=datetime.now(UTC)+timedelta(minutes=1))
            await store.propose_bootstrap(capacity_session,bootstrap)
            await store.acknowledge_bootstrap(capacity_session,_bootstrap_ack(request,bootstrap),
                actor="bob-capacity-agent",idempotency_key=uuid4())
            ack = request.acknowledgement
            plan = await store.next_subject_admission_plan(capacity_session,subject_id=ack.subject_id,
                subject_incarnation=ack.subject_incarnation,reporter_incarnation=ack.reporter_incarnation)
            await store.acknowledge_admission_plan(capacity_session,_admission_acknowledgement(plan),
                actor="bob-capacity-agent",idempotency_key=uuid4())
            permit = await store.next_pool_work(capacity_session,executor)
            await store.consume_launch_permit(capacity_session,ExecutablePermitConsumptionV2(
                binding=permit.binding,permit_id=permit.permit_id,permit_digest=store.contract_digest(permit),command_sequence=3))
    rows = (await capacity_session.scalars(select(CapacityExecutableIntent).order_by(CapacityExecutableIntent.launch_rank))).all()
    assert len(rows) == 2
    assert rows[0].subject_id != rows[1].subject_id
    await capacity_session.execute(update(CapacityAuthorityState).values(increase_freeze=True))
    target = rows[1]
    # The unfiltered queue now exposes the second subject's close. Polling the
    # first (still ambiguous) intent must return None, not borrow that authority.
    unfiltered = await store.next_pool_work(capacity_session,executor,cleanup_only=True)
    assert isinstance(unfiltered,ExecutableIntentCloseV2)
    assert unfiltered.binding.intent_id == target.intent_id
    assert await store.next_pool_work(capacity_session,executor,cleanup_only=True,cleanup_intent_id=rows[0].intent_id) is None
    selected = await store.next_pool_work(capacity_session,executor,cleanup_only=True,cleanup_intent_id=target.intent_id)
    assert isinstance(selected,ExecutableIntentCloseV2)
    assert selected.binding.intent_id == target.intent_id
    assert selected.command_sequence == 5
    assert await store.next_pool_work(capacity_session,executor,cleanup_only=True,cleanup_intent_id=uuid4()) is None
    await _heartbeat(store,capacity_session,active,pool_id="oldlab")
    assert await store.next_pool_work(capacity_session,executor_binding("oldlab"),
        cleanup_only=True,cleanup_intent_id=target.intent_id) is None
    assert await capacity_session.scalar(select(CapacityExecutableIntent.state).where(
        CapacityExecutableIntent.intent_id == rows[0].intent_id)) == "submitting-unknown"
    with pytest.raises(ValueError):
        await store.next_pool_work(capacity_session,executor,cleanup_intent_id=target.intent_id)


def test_exact_cleanup_api_preserves_pool_authority(execution_preparation_api_context,monkeypatch):
    client,app,*_ = execution_preparation_api_context
    choose = AsyncMock(return_value=None)
    monkeypatch.setattr(app.state.execution_store,"next_pool_work",choose)
    target = uuid4()
    path = f"/v2/executors/oldlab/work?cleanup_only=true&cleanup_intent_id={target}"
    assert client.get(path).status_code == 401
    assert client.get(path,headers=_v2_executor_headers("gb10")).status_code == 403
    choose.assert_not_awaited()
    actual = client.get(path,headers=_v2_executor_headers("oldlab"))
    assert actual.status_code == 200,actual.text
    assert choose.await_args.kwargs == {"cleanup_only":True,"cleanup_intent_id":target}
    choose.reset_mock()
    invalid = client.get(f"/v2/executors/oldlab/work?cleanup_intent_id={target}",headers=_v2_executor_headers("oldlab"))
    assert invalid.status_code == 422
    choose.assert_not_awaited()
