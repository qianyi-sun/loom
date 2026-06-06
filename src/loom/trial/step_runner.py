"""_run_step — per-step body (spec §3.4).

Phase order: prepare (skipped in v1; setup.sh lives in Plan 7) → agent →
artifact collection → verifier. Errors are recorded as StepError and the
loop continues to next phase.

NOTE on verifier_env_mode (spec §3.8): v1 ignores the setting and always
runs the verifier in the agent's Driver (shared mode). Separate mode (fresh
Driver from tests/Dockerfile, upload artifacts in, run verifier there) is
a v1.5 concern — it requires a "second driver" lifecycle the v1 runner does
not orchestrate. Tasks whose verifier_env_mode = "separate" will silently
run in shared mode.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

from loom.errors import AgentError
from loom.models.networking import NetworkPolicy
from loom.models.result import StepError, StepResult
from loom.models.task import StepConfig
from loom.models.trajectory import StepEndEvent, StepStartEvent
from loom.trajectory.reader import TrajectoryReader
from loom.trajectory.writer import TrajectoryWriter
from loom.trial.artifacts import ArtifactCollector
from loom.trial.phase_network import phase_network

if TYPE_CHECKING:
    from loom.trial.trial import TrialContext


async def run_step(
    *,
    ctx: TrialContext,
    step: StepConfig,
    trajectory: TrajectoryWriter,
    baseline_policy: NetworkPolicy,
) -> StepResult:
    sr_started = datetime.now(UTC)
    sr_error: StepError | None = None

    instruction = _resolve_instruction(ctx, step)
    seq = _SeqCounter()

    await trajectory.append(StepStartEvent(
        emitted_at=datetime.now(UTC),
        trial_id=ctx.trial_id,
        step_id=step.name,
        seq=seq.next(),
        instruction_excerpt=instruction[:200],
    ))

    # Agent phase ──────────────────────────────────────────────────────────
    agent_timeout = _resolve_agent_timeout(ctx, step)
    agent_phase: NetworkPolicy = (
        step.network.agent_phase if step.network and step.network.agent_phase
        else baseline_policy
    )
    try:
        async with phase_network(
            ctx.driver, baseline=baseline_policy, phase=agent_phase,
        ):
            await asyncio.wait_for(
                ctx.agent.run(
                    instruction=instruction, env=ctx.driver,
                    trajectory=trajectory, mcp=[], skills_dir=None,
                    step_id=step.name,
                ),
                timeout=agent_timeout,
            )
    except TimeoutError:
        sr_error = StepError(
            phase="agent", reason="timeout",
            message=f"agent run exceeded {agent_timeout}s",
            occurred_at=datetime.now(UTC),
        )
    except AgentError as exc:
        sr_error = StepError(
            phase="agent", reason="exception", message=str(exc),
            occurred_at=datetime.now(UTC),
        )

    # Artifact collection ──────────────────────────────────────────────────
    collector = ArtifactCollector(
        store=ctx.object_store, bucket=ctx.artifacts_bucket,
        team_id=str(ctx.team_id), trial_id=str(ctx.trial_id),
        step_name=step.name,
        local_root=ctx.local_trajectory_path.parent / "artifacts" / step.name,
    )
    artifacts_uri: str | None = None
    try:
        artifacts_uri = await collector.collect(
            env=ctx.driver, patterns=list(step.artifacts),
        )
    except Exception as exc:
        if sr_error is None:
            sr_error = StepError(
                phase="artifacts", reason="exception",
                message=str(exc), occurred_at=datetime.now(UTC),
            )

    # Verifier phase ───────────────────────────────────────────────────────
    verifier_result = None
    if not ctx.trial_config.skip_verifier:
        verifier_timeout = _resolve_verifier_timeout(ctx, step)
        verifier_phase: NetworkPolicy = (
            step.network.verifier_phase if step.network and step.network.verifier_phase
            else baseline_policy
        )
        try:
            async with phase_network(
                ctx.driver, baseline=baseline_policy, phase=verifier_phase,
            ):
                reader = TrajectoryReader(ctx.local_trajectory_path)
                verifier_result = await asyncio.wait_for(
                    ctx.verifier.verify(
                        task=ctx.task_config, env=ctx.driver,
                        artifacts_dir=PurePosixPath("/workspace"),
                        trajectory=reader,
                    ),
                    timeout=verifier_timeout,
                )
        except TimeoutError:
            if sr_error is None:
                sr_error = StepError(
                    phase="verifier", reason="timeout",
                    message=f"verifier exceeded {verifier_timeout}s",
                    occurred_at=datetime.now(UTC),
                )

    sr_finished = datetime.now(UTC)
    step_result = StepResult(
        step_name=step.name, started_at=sr_started, finished_at=sr_finished,
        verifier_result=verifier_result, error=sr_error,
        artifacts_uri=artifacts_uri,
    )

    await trajectory.append(StepEndEvent(
        emitted_at=datetime.now(UTC),
        trial_id=ctx.trial_id, step_id=step.name,
        seq=seq.next(),
        summary=verifier_result.rewards if verifier_result else None,
        error_phase=sr_error.phase if sr_error else None,
    ))
    return step_result


class _SeqCounter:
    def __init__(self) -> None:
        self._n = 0

    def next(self) -> int:
        v = self._n
        self._n += 1
        return v


def _resolve_instruction(ctx: TrialContext, step: StepConfig) -> str:
    step_dir = ctx.task_dir / "steps" / step.name
    candidate = step_dir / step.instruction_file
    if not candidate.is_file():
        candidate = ctx.task_dir / step.instruction_file
    if candidate.is_file():
        return candidate.read_text(encoding="utf-8")
    return ""


def _resolve_agent_timeout(ctx: TrialContext, step: StepConfig) -> float:
    base = ctx.task_config.agent.timeout_sec
    if step.agent and step.agent.timeout_sec is not None:
        base = step.agent.timeout_sec
    if ctx.trial_config.override_agent_timeout_sec is not None:
        base = ctx.trial_config.override_agent_timeout_sec
    return base * ctx.trial_config.agent_timeout_multiplier


def _resolve_verifier_timeout(ctx: TrialContext, step: StepConfig) -> float:
    base = ctx.task_config.verifier.timeout_sec
    if step.verifier and step.verifier.timeout_sec is not None:
        base = step.verifier.timeout_sec
    if ctx.trial_config.override_verifier_timeout_sec is not None:
        base = ctx.trial_config.override_verifier_timeout_sec
    return base * ctx.trial_config.verifier_timeout_multiplier
