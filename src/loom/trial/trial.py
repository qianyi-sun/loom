"""Trial composition + run() body (spec §2.5 + §3.3)."""

from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from loom.agent.base import AgentRuntime, InBoxAgentRuntime
from loom.driver.base import Driver, StartOptions
from loom.errors import classify_failure
from loom.models.networking import NetworkPolicy
from loom.models.result import (
    AgentInfo,
    FailureReason,
    TrialResult,
    TrialState,
)
from loom.models.task import TaskConfig
from loom.models.trajectory import (
    TrialCancelledEvent,
    TrialEndEvent,
    TrialErrorEvent,
    TrialStartEvent,
)
from loom.models.trial import TrialConfig
from loom.trajectory.storage import ObjectStore
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.finalize import finalize_trajectory
from loom.trial.step_runner import run_step
from loom.verifier.base import Verifier

logger = logging.getLogger(__name__)

_FINALIZE_TIMEOUT_SEC = 60.0
_STATE_PATCH_TIMEOUT_SEC = 15.0

StatePatchCallback = Callable[[str, str | None], Awaitable[None]]


@dataclass
class TrialContext:
    """Everything a Trial needs to run. Constructed by the worker; the Trial
    itself just executes against this bundle."""

    trial_id: UUID
    team_id: UUID
    task_config: TaskConfig
    task_checksum: str
    task_dir: Path
    trial_config: TrialConfig
    driver: Driver
    agent: AgentRuntime
    verifier: Verifier
    object_store: ObjectStore
    local_trajectory_path: Path
    trajectory_bucket: str = "trajectories"
    artifacts_bucket: str = "artifacts"

    @property
    def task_id(self) -> str:
        return self.task_config.task.id

    @property
    def trajectory_key(self) -> str:
        return f"{self.team_id}/{self.trial_id}/events.jsonl"

    @property
    def trajectory_uri(self) -> str:
        return f"s3://{self.trajectory_bucket}/{self.trajectory_key}"


@dataclass
class Trial:
    """One trial's lifecycle. Wraps a TrialContext and executes run().

    The state PATCH callback is optional; in Plan 6 the worker provides one
    that hits the Control Plane. In tests / in-process runs it's None, and
    the resulting state lives only on TrialResult.
    """

    ctx: TrialContext
    state_patch: StatePatchCallback | None = None

    async def run(self) -> TrialResult:
        result = TrialResult(
            id=self.ctx.trial_id,
            task_id=self.ctx.task_id,
            task_checksum=self.ctx.task_checksum,
            team_id=self.ctx.team_id,
            agent=AgentInfo(
                name=self.ctx.agent.name,
                version=self.ctx.agent.version,
                mode=self.ctx.agent.mode,
                model=self.ctx.agent.model,
            ),
            config=self.ctx.trial_config,
            state=TrialState.RUNNING,
            started_at=datetime.now(UTC),
        )
        if self.state_patch is not None:
            await self.state_patch("running", None)

        baseline: NetworkPolicy = (
            self.ctx.trial_config.baseline_network_policy_override
            or self.ctx.task_config.environment.baseline_network_policy
        )

        writer = TrajectoryWriter(
            local_path=self.ctx.local_trajectory_path,
            store=self.ctx.object_store,
            bucket=self.ctx.trajectory_bucket,
            key=self.ctx.trajectory_key,
        )

        cancelled = False
        result.trajectory_uri = self.ctx.trajectory_uri
        seq = _SeqCounter()

        try:
            async with writer:
                await self.ctx.driver.start(options=StartOptions(
                    force_build=self.ctx.trial_config.force_build,
                ))
                await writer.append(TrialStartEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self.ctx.trial_id, step_id="__trial__",
                    seq=seq.next(),
                    task_id=self.ctx.task_id,
                    agent_name=self.ctx.agent.name,
                    agent_mode=self.ctx.agent.mode,
                ))
                try:
                    if isinstance(self.ctx.agent, InBoxAgentRuntime):
                        await self.ctx.agent.setup(env=self.ctx.driver)

                    for step in self.ctx.task_config.steps:
                        sr = await run_step(
                            ctx=self.ctx, step=step,
                            trajectory=writer, baseline_policy=baseline,
                        )
                        result.steps.append(sr)
                    result.reward = _aggregate(self.ctx, result)
                    result.state = TrialState.SUCCEEDED
                except asyncio.CancelledError:
                    cancelled = True
                    result.state = TrialState.CANCELLED
                    await writer.append(TrialCancelledEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.ctx.trial_id, step_id="__trial__",
                        seq=seq.next(),
                        cancellation_requested_at=datetime.now(UTC),
                        observed_at=datetime.now(UTC),
                    ))
                except Exception as exc:
                    result.state = TrialState.FAILED
                    result.failure_reason = classify_failure(exc)
                    await writer.append(TrialErrorEvent(
                        emitted_at=datetime.now(UTC),
                        trial_id=self.ctx.trial_id, step_id="__trial__",
                        seq=seq.next(),
                        error_type=type(exc).__name__,
                        message=str(exc),
                        traceback=_format_tb(exc),
                    ))
                finally:
                    await asyncio.shield(
                        self.ctx.driver.stop(delete=self.ctx.trial_config.delete_env),
                    )
                await writer.append(TrialEndEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self.ctx.trial_id, step_id="__trial__",
                    seq=seq.next(),
                    final_state=result.state.value,
                    reward=result.reward,
                    failure_reason=(
                        result.failure_reason.value if result.failure_reason else None
                    ),
                ))
        finally:
            try:
                atif_uri = await asyncio.wait_for(
                    asyncio.shield(finalize_trajectory(
                        local_path=self.ctx.local_trajectory_path,
                        store=self.ctx.object_store,
                        team_id=str(self.ctx.team_id),
                        trial_id=str(self.ctx.trial_id),
                        task_id=self.ctx.task_id,
                        agent_name=self.ctx.agent.name,
                        agent_version=self.ctx.agent.version,
                        bucket=self.ctx.trajectory_bucket,
                    )),
                    timeout=_FINALIZE_TIMEOUT_SEC,
                )
                result.atif_uri = atif_uri
                result.atif_schema_version = "1.7"
            except (Exception, TimeoutError):
                if result.state == TrialState.SUCCEEDED:
                    result.state = TrialState.FAILED
                    result.failure_reason = (
                        result.failure_reason or FailureReason.TRAJECTORY_FLUSH_FAILED
                    )

            result.finished_at = datetime.now(UTC)

            if self.state_patch is not None:
                try:
                    await asyncio.wait_for(
                        asyncio.shield(self.state_patch(
                            result.state.value,
                            result.failure_reason.value if result.failure_reason else None,
                        )),
                        timeout=_STATE_PATCH_TIMEOUT_SEC,
                    )
                except TimeoutError:
                    logger.warning(
                        "state PATCH timed out; crash detector will reclaim",
                    )

            if cancelled:
                raise asyncio.CancelledError

        return result


def _aggregate(ctx: TrialContext, result: TrialResult) -> dict[str, float] | None:
    strategy = (
        ctx.task_config.multi_step.reward_strategy
        if ctx.task_config.multi_step else "mean"
    )
    rewards = [
        s.verifier_result.rewards
        for s in result.steps
        if s.verifier_result is not None
    ]
    if not rewards:
        return None
    keys: set[str] = set()
    for r in rewards:
        keys.update(r.keys())
    if not keys:
        return None
    if strategy == "final":
        return dict(rewards[-1])
    if strategy == "min":
        return {k: min(r.get(k, 0.0) for r in rewards) for k in keys}
    return {k: sum(r.get(k, 0.0) for r in rewards) / len(rewards) for k in keys}


def _format_tb(exc: BaseException) -> str:
    return "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))


class _SeqCounter:
    """Monotonic per-trial sequence counter used by Trial.run()."""

    def __init__(self) -> None:
        self._n = 0

    def next(self) -> int:
        v = self._n
        self._n += 1
        return v
