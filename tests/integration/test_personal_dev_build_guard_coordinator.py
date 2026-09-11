"""Real SQL transactions span exact manager receipts without losing prepared work."""

from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.client import (
    ExecutableAdmissionAcknowledgementReceiptV2,
    ExecutableAdmissionPlanClosureAcknowledgementReceiptV2,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionPlanClosureV2,
    canonical_executable_digest,
)
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


@pytest.mark.parametrize("first_reply", ["lost", "wrong-digest", "success"])
async def test_coordinator_keeps_preparation_durable_and_publication_locked(prepared_input, first_reply):
    coordinator_type = import_module("loom_capacity_build_guard.coordinator").BuildPlanCoordinator
    sessions, engine, retained, proposal, registration, request = prepared_input
    calls = []

    class Publisher:
        async def publish_executable_admission_acknowledgement(self, ack, *, idempotency_key):
            calls.append((ack, idempotency_key))
            with engine.connect() as connection:
                assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.assignments")) == 1
                assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
            with engine.begin() as connection:
                connection.execute(text("SET LOCAL lock_timeout='50ms'"))
                with pytest.raises(DBAPIError, match="lock timeout"):
                    connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
            if len(calls) == 1 and first_reply == "lost":
                raise ConnectionError("lost manager response")
            return ExecutableAdmissionAcknowledgementReceiptV2(proposal_id=ack.proposal_id,
                prepared_plan_digest=ack.prepared_plan_digest, replayed=len(calls)>1, executable=True,
                receipt_digest=("f"*64 if len(calls)==1 and first_reply=="wrong-digest" else canonical_executable_digest(ack)))

    coordinator = coordinator_type(sessions, installation=retained, publisher=Publisher())
    prepared = await coordinator.prepare(proposal, sources={request.id: registration})
    if first_reply != "success":
        with pytest.raises((ConnectionError, ValueError)):
            await coordinator.publish(proposal.plan_id)
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0
    result = await coordinator.publish(proposal.plan_id)
    assert result.prepared_plan_digest == prepared.digest
    assert await coordinator.publish(proposal.plan_id) == result.model_copy(update={"replayed": True})
    assert all(call == calls[0] for call in calls)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 1


async def test_coordinator_commits_closure_before_cleanup_publication(prepared_input):
    coordinator_type = import_module("loom_capacity_build_guard.coordinator").BuildPlanCoordinator
    sessions, engine, retained, proposal, _, _ = prepared_input
    calls = []

    class Publisher:
        async def publish_executable_admission_closure_acknowledgement(self, ack, *, idempotency_key):
            with engine.connect() as connection:
                assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions WHERE kind='closure'")) == 1
                assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 0
            calls.append((ack, idempotency_key))
            if len(calls) == 1:
                raise ConnectionError("lost cleanup response")
            return ExecutableAdmissionPlanClosureAcknowledgementReceiptV2(closure_id=ack.closure_id,
                disposition_kind=ack.disposition_kind, disposition_digest=ack.disposition_digest,
                receipt_digest=canonical_executable_digest(ack), replayed=True, executable=False)

    coordinator = coordinator_type(sessions, installation=retained, publisher=Publisher())
    closure = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    retained_closure = await coordinator.close(closure)
    with pytest.raises(ConnectionError):
        await coordinator.publish_closure(proposal.plan_id)
    receipt = await coordinator.publish_closure(proposal.plan_id)
    assert receipt.disposition_digest == retained_closure.digest
    assert calls[0] == calls[1]


@pytest.mark.parametrize("already_closed", [False, True])
async def test_restart_replays_retained_closure_before_new_manager_reason(prepared_input, already_closed):
    coordinator_type = import_module("loom_capacity_build_guard.coordinator").BuildPlanCoordinator
    sessions, _engine, retained, proposal, _, _ = prepared_input
    calls = []

    class Publisher:
        async def publish_executable_admission_closure_acknowledgement(self, ack, *, idempotency_key):
            calls.append((ack, idempotency_key))
            return ExecutableAdmissionPlanClosureAcknowledgementReceiptV2(closure_id=ack.closure_id,
                disposition_kind=ack.disposition_kind, disposition_digest=ack.disposition_digest,
                receipt_digest=canonical_executable_digest(ack), replayed=False, executable=False)

    first = ExecutableAdmissionPlanClosureV2(closure_id=uuid4(), proposal=proposal, close_reason="manager-closed")
    if already_closed:
        await coordinator_type(sessions, installation=retained, publisher=Publisher()).close(first)
    changed = first.model_copy(update={"closure_id": uuid4(), "close_reason": "expired"})
    restarted = coordinator_type(sessions, installation=retained, publisher=Publisher())
    receipt = await restarted.reconcile_closure(changed)
    assert receipt.closure_id == (first.closure_id if already_closed else changed.closure_id)
    assert calls[0][0].close_reason == ("manager-closed" if already_closed else "expired")


async def test_timeout_releases_publication_locks_without_losing_preparation(prepared_input):
    import asyncio

    coordinator_type = import_module("loom_capacity_build_guard.coordinator").BuildPlanCoordinator
    sessions, engine, retained, proposal, registration, request = prepared_input

    class Publisher:
        async def publish_executable_admission_acknowledgement(self, ack, *, idempotency_key):
            await asyncio.Event().wait()

    await coordinator_type(sessions, installation=retained, publisher=Publisher()).prepare(proposal, sources={request.id: registration})
    coordinator = coordinator_type(sessions, installation=retained, publisher=Publisher(), operation_timeout_seconds=0.2)
    with pytest.raises(TimeoutError):
        await coordinator.publish(proposal.plan_id)
    with engine.begin() as connection:
        connection.execute(text("SET LOCAL lock_timeout='100ms'"))
        connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.dispositions")) == 0


@pytest.mark.parametrize("boundary", ["fresh", "prepared", "lost-reply", "changed-proposal", "cancelled"])
async def test_convergence_loads_private_sources_and_replays_exact_plan(prepared_input, boundary):
    coordinator_type = import_module("loom_capacity_build_guard.coordinator").BuildPlanCoordinator
    factory, engine, installation, proposal, registration, request = prepared_input
    calls = []

    class Publisher:
        async def publish_executable_admission_acknowledgement(self, ack, *, idempotency_key):
            calls.append((ack, idempotency_key))
            if boundary == "lost-reply" and len(calls) == 1:
                raise ConnectionError("lost response")
            return ExecutableAdmissionAcknowledgementReceiptV2(proposal_id=ack.proposal_id,
                prepared_plan_digest=ack.prepared_plan_digest, receipt_digest=canonical_executable_digest(ack),
                replayed=len(calls)>1, executable=True)

    coordinator = coordinator_type(factory, installation=installation, publisher=Publisher())
    if boundary in {"prepared", "changed-proposal", "cancelled"}:
        await coordinator.prepare(proposal, sources={request.id: registration})
    if boundary == "cancelled":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_build_platform_requests SET cancelled_at=now() WHERE id=:id"), {"id": request.id})
    if boundary == "changed-proposal":
        proposal = proposal.model_copy(update={"proposal_id": uuid4()})
    if boundary in {"changed-proposal", "cancelled", "lost-reply"}:
        with pytest.raises((ValueError, DBAPIError, ConnectionError)):
            await coordinator.converge(proposal)
        if boundary != "lost-reply":
            assert calls == []
    if boundary not in {"changed-proposal", "cancelled"}:
        result = await coordinator_type(factory, installation=installation, publisher=Publisher()).converge(proposal)
        assert result.proposal_id == proposal.proposal_id
        assert all(call == calls[0] for call in calls)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.assignments")) == 1
