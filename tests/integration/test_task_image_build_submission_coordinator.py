from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta

import pytest
from sqlalchemy import event, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import Session

from loom.db.schema import TaskImageBuildGrant, TaskImageBuildGrantEvent
from loom_control_plane.elastic_slurm_worker_controller import SbatchRequest
from loom_control_plane.task_image_build_environment import (
    BuildEnvironmentDisabledError,
    SlurmBuildEnvironmentProvider,
    SlurmBuildGrantV2,
    SlurmBuildInventoryV1,
)
from loom_control_plane.task_image_build_grants import (
    TaskImageBuildGrantConflictError,
    begin_task_image_build_submission,
    issue_task_image_build_grant,
)
from loom_control_plane.task_image_build_submission import (
    TaskImageBuildSubmissionCoordinator,
    TaskImageBuildSubmissionUncertain,
)
from tests.integration.test_task_image_build_grant_store import (
    _NOW,
    _grant,
    _policy,
    grant_session,  # noqa: F401 -- owning disposable PostgreSQL fixture
)


class _Runner:
    def __init__(self, on_submit: Callable[[], Awaitable[str]]) -> None:
        self.on_submit = on_submit
        self.requests: list[SbatchRequest] = []

    async def submit(self, request: SbatchRequest) -> str:
        self.requests.append(request)
        return await self.on_submit()

    async def inventory(self, grant: SlurmBuildGrantV2) -> SlurmBuildInventoryV1:
        raise AssertionError("submission is not inventory authority")

    async def cancel(self, job_id: str) -> None:
        raise AssertionError("submission must not cancel")

    async def release(self, job_id: str) -> None:
        raise AssertionError("submission must not release")


async def _issue(factory: async_sessionmaker[AsyncSession]) -> SlurmBuildGrantV2:
    grant = _grant()
    async with factory.begin() as session:
        await issue_task_image_build_grant(
            session, environment="staging", grant=grant, ambiguity_settle_seconds=30, now=_NOW
        )
    return grant


def _coordinator(
    factory: async_sessionmaker[AsyncSession],
    runner: _Runner,
    *,
    enabled: bool = True,
    environment: str = "staging",
) -> TaskImageBuildSubmissionCoordinator:
    policy = _policy().model_copy(
        update={"enabled": enabled, "activation_blockers": () if enabled else ("guard_missing",)}
    )
    return TaskImageBuildSubmissionCoordinator(
        session_factory=factory,
        environment=environment,
        provider=SlurmBuildEnvironmentProvider(policy=policy, runner=runner),
        clock=lambda: _NOW,
    )


async def test_submission_commits_before_dispatch_and_receipt_never_binds(
    grant_session: async_sessionmaker[AsyncSession],  # noqa: F811
) -> None:
    grant = await _issue(grant_session)

    async def observe_committed_invocation() -> str:
        async with grant_session.begin() as observer:
            row = await observer.scalar(
                select(TaskImageBuildGrant)
                .where(TaskImageBuildGrant.id == grant.grant_id)
                .with_for_update(nowait=True)
            )
            assert row is not None and row.state == "submitting"
            assert row.invocation_started_at == _NOW
            events = list(await observer.scalars(
                select(TaskImageBuildGrantEvent.event_type)
                .where(TaskImageBuildGrantEvent.grant_id == grant.grant_id)
                .order_by(TaskImageBuildGrantEvent.sequence)
            ))
            assert events == ["issued", "submission_started"]
        return "123"

    runner = _Runner(observe_committed_invocation)
    result = await _coordinator(grant_session, runner).submit_once(grant.grant_id)
    assert result.grant_id == grant.grant_id
    assert result.reported_job_id == "123"
    assert len(runner.requests) == 1
    assert "--hold" in runner.requests[0].args
    assert f"--comment={grant.comment}" in runner.requests[0].args
    async with grant_session() as observer:
        row = await observer.get(TaskImageBuildGrant, grant.grant_id)
        assert row is not None and row.state == "submitting"
        assert row.slurm_job_id is None and row.bound_at is None and row.released_at is None


async def test_concurrent_controllers_cannot_repeat_inflight_submission(
    grant_session: async_sessionmaker[AsyncSession],  # noqa: F811
) -> None:
    grant = await _issue(grant_session)
    entered, finish = asyncio.Event(), asyncio.Event()

    async def slow_submit() -> str:
        entered.set()
        await finish.wait()
        return "123"

    runner = _Runner(slow_submit)
    first = asyncio.create_task(_coordinator(grant_session, runner).submit_once(grant.grant_id))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        results = await asyncio.gather(*(
            _coordinator(grant_session, runner).submit_once(grant.grant_id) for _ in range(4)
        ), return_exceptions=True)
        assert all(isinstance(item, TaskImageBuildGrantConflictError) for item in results)
        assert len(runner.requests) == 1
    finally:
        finish.set()
        await first


@pytest.mark.parametrize("receipt", ["", "0", "01", "123;gb10", "123\n", "-1", "１２３", "4294967295"])
async def test_invalid_receipt_stays_uncertain_without_retry(
    grant_session: async_sessionmaker[AsyncSession], receipt: str,  # noqa: F811
) -> None:
    grant = await _issue(grant_session)

    async def submit() -> str:
        return receipt

    runner = _Runner(submit)
    with pytest.raises(TaskImageBuildSubmissionUncertain):
        await _coordinator(grant_session, runner).submit_once(grant.grant_id)
    with pytest.raises(TaskImageBuildGrantConflictError):
        await _coordinator(grant_session, runner).submit_once(grant.grant_id)
    assert len(runner.requests) == 1


async def test_timeout_preserves_consumption_and_does_not_disclose_command_output(
    grant_session: async_sessionmaker[AsyncSession],  # noqa: F811
) -> None:
    grant = await _issue(grant_session)

    async def timeout() -> str:
        raise TimeoutError("untrusted remote output")

    runner = _Runner(timeout)
    with pytest.raises(TaskImageBuildSubmissionUncertain) as error:
        await _coordinator(grant_session, runner).submit_once(grant.grant_id)
    assert "untrusted remote output" not in str(error.value)
    assert error.value.__suppress_context__
    with pytest.raises(TaskImageBuildGrantConflictError):
        await _coordinator(grant_session, runner).submit_once(grant.grant_id)
    assert len(runner.requests) == 1


async def test_cancellation_does_not_restore_submission_authority(
    grant_session: async_sessionmaker[AsyncSession],  # noqa: F811
) -> None:
    grant = await _issue(grant_session)
    entered = asyncio.Event()

    async def interrupted() -> str:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    runner = _Runner(interrupted)
    task = asyncio.create_task(_coordinator(grant_session, runner).submit_once(grant.grant_id))
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    with pytest.raises(TaskImageBuildGrantConflictError):
        await _coordinator(grant_session, runner).submit_once(grant.grant_id)
    assert len(runner.requests) == 1


@pytest.mark.parametrize("boundary", ["disabled", "environment", "expired", "policy"])
async def test_admission_failure_does_not_consume_or_dispatch(
    grant_session: async_sessionmaker[AsyncSession], boundary: str,  # noqa: F811
) -> None:
    grant = await _issue(grant_session)

    async def forbidden() -> str:
        raise AssertionError("admission failure must not dispatch")

    runner = _Runner(forbidden)
    coordinator = _coordinator(
        grant_session, runner,
        enabled=boundary != "disabled",
        environment="production" if boundary == "environment" else "staging",
    )
    if boundary == "expired":
        coordinator.clock = lambda: _NOW + timedelta(hours=3)
    if boundary == "policy":
        coordinator.provider.policy = coordinator.provider.policy.model_copy(update={
            "resources": coordinator.provider.policy.resources.model_copy(update={"cpus": 9})
        })
    with pytest.raises((BuildEnvironmentDisabledError, TaskImageBuildGrantConflictError, ValueError)):
        await coordinator.submit_once(grant.grant_id)
    assert runner.requests == []
    async with grant_session() as observer:
        row = await observer.get(TaskImageBuildGrant, grant.grant_id)
        assert row is not None and row.state == "issued" and row.journal_sequence == 1


async def test_commit_failure_never_reaches_provider(
    grant_session: async_sessionmaker[AsyncSession],  # noqa: F811
) -> None:
    grant = await _issue(grant_session)

    class RefuseCommit(Session):
        pass

    @event.listens_for(RefuseCommit, "before_commit")
    def reject_commit(session: Session) -> None:
        raise RuntimeError("injected commit failure")

    failing_factory = async_sessionmaker(
        grant_session.kw["bind"], sync_session_class=RefuseCommit, expire_on_commit=False
    )

    async def forbidden() -> str:
        raise AssertionError("uncommitted invocation cannot dispatch")

    runner = _Runner(forbidden)
    with pytest.raises(RuntimeError, match="injected commit failure"):
        await _coordinator(failing_factory, runner).submit_once(grant.grant_id)
    assert runner.requests == []
    async with grant_session() as observer:
        row = await observer.get(TaskImageBuildGrant, grant.grant_id)
        assert row is not None and row.state == "issued" and row.journal_sequence == 1


async def test_restart_after_commit_without_send_cannot_dispatch_again(
    grant_session: async_sessionmaker[AsyncSession],  # noqa: F811
) -> None:
    grant = await _issue(grant_session)
    async with grant_session.begin() as session:
        await begin_task_image_build_submission(session, grant_id=grant.grant_id, now=_NOW)

    async def forbidden() -> str:
        raise AssertionError("restart must discover inventory instead of resubmitting")

    runner = _Runner(forbidden)
    with pytest.raises(TaskImageBuildGrantConflictError):
        await _coordinator(grant_session, runner).submit_once(grant.grant_id)
    assert runner.requests == []
