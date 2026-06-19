"""Trial composition + run() body (spec §2.5 + §3.3)."""

from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

from loom.agent.base import AgentRuntime, InBoxAgentRuntime
from loom.driver.base import Driver, StartOptions
from loom.errors import classify_failure
from loom.models.networking import NetworkPolicy
from loom.models.result import (
    AgentInfo,
    FailureReason,
    StepError,
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
from loom.trial.workspace import materialize_workspace
from loom.verifier.base import Verifier

logger = logging.getLogger(__name__)

_FINALIZE_TIMEOUT_SEC = 60.0
_STATE_PATCH_TIMEOUT_SEC = 15.0

StatePatchCallback = Callable[[str, str | None, str | None], Awaitable[None]]


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
    # Plan 9/11 amendment A11.1: optional callback the worker uses to
    # fetch llm_calls rows from the Control Plane at finalize time and
    # project each into an LLMCallEvent before ATIF runs. If None, no
    # rows are injected (legacy v0.7 behavior — agents that bypassed
    # the Gateway have no llm_calls).
    llm_calls_fetcher: Callable[[UUID], Awaitable[list[dict[str, Any]]]] | None = None
    # #188 / Phase B: sandbox-isolation hooks. When the worker has
    # `LOOM_WORKER_SANDBOX_ISOLATION=1`, these are populated:
    # - `sandbox_network` is the per-trial bridge name passed to
    #   `StartOptions.network` so the container attaches there at
    #   create time.
    # - `on_driver_started` runs RIGHT AFTER `driver.start()`
    #   succeeds. The worker uses it to attach the
    #   `loom-llm-gateway-sandbox` singleton (PR-B2 #221) to the
    #   bridge — without that hop, the sandbox has no reachable
    #   outbound endpoint.
    # Both default None → pre-Phase-B behavior unchanged.
    sandbox_network: str | None = None
    on_driver_started: Callable[[], Awaitable[None]] | None = None
    # Phase D PR-D1: bind-mounts the driver applies at create time.
    # When sandbox isolation is on, the worker passes the rotator's
    # jwt_dir here mounted at `/run/loom/`. Empty tuple → no mounts
    # (unchanged behavior).
    sandbox_volumes: tuple[tuple[str, str, str], ...] = ()

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
    # Bug 4 fix: stash the TrialResult here so callers can recover it after
    # a CancelledError propagates out of run(). Populated by run() once the
    # in-flight result skeleton has been built.
    result: TrialResult | None = None

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
        self.result = result
        if self.state_patch is not None:
            # Bug 2 fix: PATCH errors must not propagate out of Trial.run.
            # Plan 6's worker has its own reclaim/retry — losing one heartbeat
            # is recoverable; raising here orphans the local state machine.
            try:
                await self.state_patch("running", None, None)
            except Exception:
                logger.exception("state PATCH (running) failed; continuing")

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
                driver_started = False
                try:
                    # Bug 1 fix: driver.start() + TrialStartEvent must land
                    # inside the broad-Exception catch so ENV_START_FAILURE
                    # (DriverError) becomes a classified TrialResult instead
                    # of propagating raw out of Trial.run.
                    await self.ctx.driver.start(
                        options=StartOptions(
                            force_build=self.ctx.trial_config.force_build,
                            network=self.ctx.sandbox_network,
                            volumes=self.ctx.sandbox_volumes,
                        )
                    )
                    driver_started = True
                    # #188 / Phase B: attach the per-node singleton
                    # to this trial's bridge AFTER container creation.
                    # Failure to attach is a hard error — the sandbox
                    # has no other reachable endpoint, so trying to
                    # run the agent would just hang.
                    if self.ctx.on_driver_started is not None:
                        await self.ctx.on_driver_started()
                    await writer.append(
                        TrialStartEvent(
                            emitted_at=datetime.now(UTC),
                            trial_id=self.ctx.trial_id,
                            step_id="__trial__",
                            seq=seq.next(),
                            task_id=self.ctx.task_id,
                            agent_name=self.ctx.agent.name,
                            agent_mode=self.ctx.agent.mode,
                        )
                    )
                    # #186: upload the materialized task bundle into the
                    # sandbox so the agent sees instructions / scaffolding
                    # AND the verifier sees grading tests at /workspace/
                    # tests. Mirrors what oracle_runner does for the
                    # verify CLI. Skips `task.toml` (host-side metadata).
                    await materialize_workspace(
                        driver=self.ctx.driver,
                        task_dir=self.ctx.task_dir,
                        dst=PurePosixPath("/workspace"),
                    )
                    if isinstance(self.ctx.agent, InBoxAgentRuntime):
                        await self.ctx.agent.setup(env=self.ctx.driver)

                    for step in self.ctx.task_config.steps:
                        sr = await run_step(
                            ctx=self.ctx,
                            step=step,
                            trajectory=writer,
                            baseline_policy=baseline,
                        )
                        result.steps.append(sr)
                    result.reward = _aggregate(self.ctx, result)
                    artifact_error = _first_artifact_error(result)
                    if artifact_error is not None:
                        result.state = TrialState.FAILED
                        result.failure_reason = FailureReason.ARTIFACT_UPLOAD_FAILED
                    else:
                        result.state = TrialState.SUCCEEDED
                except asyncio.CancelledError:
                    cancelled = True
                    result.state = TrialState.CANCELLED
                    try:
                        await writer.append(
                            TrialCancelledEvent(
                                emitted_at=datetime.now(UTC),
                                trial_id=self.ctx.trial_id,
                                step_id="__trial__",
                                seq=seq.next(),
                                cancellation_requested_at=datetime.now(UTC),
                                observed_at=datetime.now(UTC),
                            )
                        )
                    except Exception:
                        logger.exception("failed to append TrialCancelledEvent")
                except Exception as exc:
                    result.state = TrialState.FAILED
                    reason, message = classify_failure(exc)
                    result.failure_reason = reason
                    result.failure_message = message
                    try:
                        await writer.append(
                            TrialErrorEvent(
                                emitted_at=datetime.now(UTC),
                                trial_id=self.ctx.trial_id,
                                step_id="__trial__",
                                seq=seq.next(),
                                error_type=type(exc).__name__,
                                message=str(exc),
                                traceback=_format_tb(exc),
                            )
                        )
                    except Exception:
                        # Early-start failure leaves the writer with no
                        # TrialStartEvent; appending an error event can
                        # itself fail. Don't mask the real failure.
                        logger.exception("failed to append TrialErrorEvent")
                finally:
                    if driver_started:
                        await asyncio.shield(
                            self.ctx.driver.stop(delete=self.ctx.trial_config.delete_env),
                        )
                try:
                    await writer.append(
                        TrialEndEvent(
                            emitted_at=datetime.now(UTC),
                            trial_id=self.ctx.trial_id,
                            step_id="__trial__",
                            seq=seq.next(),
                            final_state=result.state.value,
                            reward=result.reward,
                            failure_reason=(
                                result.failure_reason.value if result.failure_reason else None
                            ),
                        )
                    )
                except Exception:
                    logger.exception("failed to append TrialEndEvent")

                # Plan 9/11 amendment A11.1: BEFORE the writer closes,
                # query CP for llm_calls rows and append each as an
                # LLMCallEvent-shaped dict so finalize_trajectory's ATIF
                # projection sees them. The writer is still open here
                # (inside `async with writer:`); skip silently if the
                # context didn't supply a fetcher (v0.7 behavior).
                if (
                    self.ctx.llm_calls_fetcher is not None
                    and not _agent_emits_gateway_llm_call_events(self.ctx.agent)
                ):
                    try:
                        await self._append_llm_call_events(writer)
                    except Exception:
                        logger.exception(
                            "failed to fetch llm_calls; trajectory will be "
                            "finalized without LLMCallEvents",
                        )
        finally:
            try:
                atif_uri = await asyncio.wait_for(
                    asyncio.shield(
                        finalize_trajectory(
                            local_path=self.ctx.local_trajectory_path,
                            store=self.ctx.object_store,
                            team_id=str(self.ctx.team_id),
                            trial_id=str(self.ctx.trial_id),
                            task_id=self.ctx.task_id,
                            agent_name=self.ctx.agent.name,
                            agent_version=self.ctx.agent.version,
                            bucket=self.ctx.trajectory_bucket,
                        )
                    ),
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
                # Bug 2 fix: catch general exceptions too — TimeoutError was
                # the only original except clause, leaving HTTP errors,
                # connection drops, etc. to propagate out of Trial.run.
                try:
                    await asyncio.wait_for(
                        asyncio.shield(
                            self.state_patch(
                                result.state.value,
                                result.failure_reason.value if result.failure_reason else None,
                                result.failure_message,
                            )
                        ),
                        timeout=_STATE_PATCH_TIMEOUT_SEC,
                    )
                except TimeoutError:
                    logger.warning(
                        "state PATCH timed out; crash detector will reclaim",
                    )
                except Exception:
                    logger.exception(
                        "state PATCH (terminal) failed; crash detector will reclaim",
                    )

            if cancelled:
                raise asyncio.CancelledError

        return result

    async def _append_llm_call_events(self, writer: TrajectoryWriter) -> None:
        """Plan 9/11 A11.1: query CP for llm_calls rows + project each
        into a valid LLMCallEvent on the local JSONL.

        Cost-attribution facts (tokens, model, rate_card_hash) come
        straight from the row. Conversation content (system_prompt,
        messages, response, finish_reason) is set to placeholders
        because the Gateway doesn't persist message bodies — the
        adapter's `capture_events()` is the source of truth for
        message-level semantics, this projection is the source of
        truth for billing.
        """
        from loom.models.trajectory import ChatMessage, LLMCallEvent
        from loom.models.types import ModelSpec

        assert self.ctx.llm_calls_fetcher is not None
        rows = await self.ctx.llm_calls_fetcher(self.ctx.trial_id)
        _provider_by_dialect = {
            "openai_chat": "openai",
            "openai_responses": "openai",
            # Facade routes (`/openai/v1/...`, `/anthropic/v1/...`,
            # `/google/v1beta/...` — cluster-deploy.md provider-
            # connection plumbing) write the *_facade dialect strings.
            # Map each to the matching provider so the projection
            # produces ModelSpec(provider=<provider>, ...) rather
            # than the "unknown" fallback.
            "openai_facade": "openai",
            "anthropic_facade": "anthropic",
            "gemini_facade": "google",
            "anthropic": "anthropic",
            "gemini": "google",
        }
        for row in rows:
            captured_at_raw = row.get("captured_at")
            emitted_at = (
                datetime.fromisoformat(
                    str(captured_at_raw).replace("Z", "+00:00"),
                )
                if captured_at_raw
                else datetime.now(UTC)
            )
            extras = row.get("provider_extras") or {}
            event = LLMCallEvent(
                emitted_at=emitted_at,
                trial_id=self.ctx.trial_id,
                step_id=str(row.get("step_id") or "__trial__"),
                seq=0,  # appended out-of-band; ATIF sorts by emitted_at
                model=ModelSpec(
                    provider=_provider_by_dialect.get(
                        str(row.get("dialect") or ""),
                        "unknown",
                    ),
                    name=str(row.get("model") or "unknown"),
                ),
                rate_card_hash=str(row.get("rate_card_hash") or ""),
                system_prompt=None,
                messages=[],
                response=ChatMessage(role="assistant", content=""),
                finish_reason="synthetic",
                input_tokens=int(row.get("input_tokens") or 0),
                cached_input_tokens=int(
                    extras.get("cache_read_input_tokens", 0)
                    + extras.get("cachedContentTokenCount", 0),
                ),
                cache_write_tokens=int(
                    extras.get("cache_creation_input_tokens", 0),
                ),
                output_tokens=int(row.get("output_tokens") or 0),
                thinking_tokens=int(
                    extras.get("reasoning_tokens", 0) + extras.get("thoughtsTokenCount", 0),
                ),
                provider_extras={
                    k: int(v) for k, v in extras.items() if isinstance(v, int | float)
                },
                cost_usd_snapshot=float(row.get("cost_usd") or 0.0),
                duration_sec=0.0,
                streamed=False,
                time_to_first_token_sec=None,
                gateway_request_id=str(row.get("id") or ""),
            )
            await writer.append(event)


def _agent_emits_gateway_llm_call_events(agent: AgentRuntime) -> bool:
    return bool(getattr(agent, "emits_gateway_llm_call_events", False))


def _aggregate(ctx: TrialContext, result: TrialResult) -> dict[str, float] | None:
    strategy = ctx.task_config.multi_step.reward_strategy if ctx.task_config.multi_step else "mean"
    rewards = [s.verifier_result.rewards for s in result.steps if s.verifier_result is not None]
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


def _first_artifact_error(result: TrialResult) -> StepError | None:
    for step in result.steps:
        if step.error is not None and step.error.phase == "artifacts":
            return step.error
    return None


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
