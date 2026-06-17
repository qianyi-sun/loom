"""Per-trial runner — builds the TrialContext and invokes Trial.run().

Plan 3 owns Trial.run() itself; this is the worker-side wrapper that wires
it to a real Driver/Agent/Verifier and a state PATCH callback that hits
the Control Plane.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from loom.agent.base import AgentRuntime
from loom.agent.gateway_client import LLMGatewayClient
from loom.agent.local_vllm_client import LocalVLLMGatewayClient
from loom.driver.base import Driver
from loom.models.result import FailureReason, TrialResult, TrialState
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
from loom.models.types import ModelSpec
from loom.trajectory.storage import ObjectStore
from loom.trial.trial import Trial, TrialContext
from loom.verifier.base import Verifier
from loom_worker.vllm_registry import WorkerVLLMRegistry

logger = logging.getLogger(__name__)


# (state, failure_reason) → bool: True if the Control Plane accepted the
# transition, False if the worker has lost its claim (fenced).
StatePatchCallback = Callable[[str, str | None], Awaitable[bool]]
OutputProjectionCallback = Callable[
    [dict[str, object], dict[str, object]], Awaitable[bool]
]

# Factory signature: (task_dir, gateway, model, agent_name) → AgentRuntime.
# agent_name is read from task_config.agent.name; the factory routes:
#   "oracle"             → OracleAgent
#   "litellm" (or model) → LiteLLMAgent
#   "claude-code-inbox"  → ClaudeCodeAgent (v0.7 in-box; renamed per spec)
#   <launcher adapter>   → SubprocessAgent wrapping the adapter
AgentFactory = Callable[
    [Path, LLMGatewayClient, "ModelSpec | None", str], AgentRuntime,
]


@dataclass
class LocalTrialRunner:
    trial_id: UUID
    team_id: UUID
    task_config: TaskConfig
    task_checksum: str
    task_dir: Path
    trial_config: TrialConfig

    driver_factory: Callable[[], Driver]
    agent_factory: AgentFactory
    verifier_factory: Callable[[], Verifier]

    object_store: ObjectStore
    gateway_client: LLMGatewayClient

    local_trajectory_root: Path
    state_patch_callback: StatePatchCallback
    output_projection_callback: OutputProjectionCallback | None = None
    # Plan 9/11 amendment A11.1: optional fetcher the worker plumbs
    # through to Trial via TrialContext.llm_calls_fetcher. None means
    # no llm_calls injection at finalize (legacy v0.7 behavior).
    llm_calls_fetcher: Callable[[UUID], Awaitable[list[dict[str, object]]]] | None = None
    # PR-E: worker-spawned vLLM registry. Optional — when None, any
    # trial requesting `ModelSpec.source=hf, hf_execution=local-vllm`
    # surfaces an AgentError instead of silently routing elsewhere.
    vllm_registry: WorkerVLLMRegistry | None = None

    async def run(self) -> TrialResult:
        driver = self.driver_factory()
        # Plan 23: agent + model live on TrialConfig and are required.
        # TaskConfig.agent.* is no longer consulted for service-mode
        # trials — every submission specifies which agent + model run.
        # PR-E: when the model targets a worker-spawned vLLM, swap the
        # gateway for a direct LocalVLLMGatewayClient before handing
        # it to the agent factory. The vLLM registry caches across
        # trials so successive trials of the same model reuse one
        # subprocess instead of paying the 1-3 min startup each time.
        effective_gateway = await self._resolve_gateway()
        agent = self.agent_factory(
            self.task_dir,
            effective_gateway,
            self.trial_config.agent_model,
            self.trial_config.agent_name,
        )
        verifier = self.verifier_factory()

        ctx = TrialContext(
            trial_id=self.trial_id,
            team_id=self.team_id,
            task_config=self.task_config,
            task_checksum=self.task_checksum,
            task_dir=self.task_dir,
            trial_config=self.trial_config,
            driver=driver,
            agent=agent,
            verifier=verifier,
            object_store=self.object_store,
            local_trajectory_path=(
                self.local_trajectory_root / f"{self.trial_id}.jsonl"
            ),
            llm_calls_fetcher=self.llm_calls_fetcher,
        )

        async def _patch(state: str, fr: str | None) -> None:
            # Trial expects an `Awaitable[None]` callback. We adapt our
            # bool-returning callback by logging when the Control Plane
            # rejects with a fence (False) and swallowing other errors so a
            # transient PATCH failure doesn't crash the trial body.
            try:
                ok = await self.state_patch_callback(state, fr)
                if not ok:
                    logger.warning(
                        "state_patch_fenced trial=%s state=%s — "
                        "worker lost claim",
                        self.trial_id, state,
                    )
            except Exception as exc:
                logger.warning(
                    "state_patch_error trial=%s state=%s err=%s",
                    self.trial_id, state, exc,
                )

        trial = Trial(ctx=ctx, state_patch=_patch)
        try:
            result = await trial.run()
            await self._patch_output_projection(result)
            return result
        except Exception:
            logger.exception("trial_runner_uncaught_exception trial=%s", self.trial_id)
            if trial.result is None:
                raise
            result = trial.result
            if result.state != TrialState.CANCELLED:
                result.state = TrialState.FAILED
                result.failure_reason = (
                    result.failure_reason
                    or FailureReason.TRAJECTORY_FLUSH_FAILED
                )
                result.finished_at = datetime.now(UTC)
                await _patch(
                    result.state.value,
                    result.failure_reason.value if result.failure_reason else None,
                )
                return result
            raise

    async def _patch_output_projection(self, result: TrialResult) -> None:
        if self.output_projection_callback is None:
            return
        if result.state != TrialState.SUCCEEDED:
            return
        result_payload = _build_result_payload(result)
        trajectory_index = _build_trajectory_index(result)
        try:
            ok = await self.output_projection_callback(
                result_payload,
                trajectory_index,
            )
            if not ok:
                logger.warning(
                    "output_projection_patch_fenced trial=%s — worker lost claim",
                    self.trial_id,
                )
        except Exception as exc:
            logger.warning(
                "output_projection_patch_error trial=%s err=%s",
                self.trial_id, exc,
            )

    async def _resolve_gateway(self) -> LLMGatewayClient:
        """Pick the gateway client for this trial.

        Default: the worker's `gateway_client` (HTTP to the LLM Gateway
        service). When the trial's model selects worker-spawned vLLM,
        substitute a `LocalVLLMGatewayClient` pointed at the registry's
        cached subprocess URL — bypasses the gateway since the vLLM
        runs on this same worker host.
        """
        model = self.trial_config.agent_model
        if model is None:
            return self.gateway_client
        if model.source != "hf" or model.hf_execution != "local-vllm":
            return self.gateway_client
        if self.vllm_registry is None:
            from loom.errors import AgentError
            raise AgentError(
                "trial requests source=hf, hf_execution=local-vllm but "
                "this worker has no vllm_registry configured. Set up a "
                "worker with `pip install loom[vllm]` or pick a different "
                "model source.",
            )
        handle = await self.vllm_registry.get_or_launch(model.name)
        return LocalVLLMGatewayClient(base_url=handle.base_url)


def _build_result_payload(result: TrialResult) -> dict[str, object]:
    payload: dict[str, object] = result.model_dump(mode="json")
    payload["aggregate_reward"] = _aggregate_reward_scalar(result.reward)
    payload.setdefault("cost_usd", 0.0)
    return payload


def _aggregate_reward_scalar(reward: dict[str, float] | None) -> float | None:
    if not reward:
        return None
    if len(reward) == 1:
        return float(next(iter(reward.values())))
    return sum(float(v) for v in reward.values()) / len(reward)


def _build_trajectory_index(result: TrialResult) -> dict[str, object]:
    artifacts = [
        artifact.model_dump(mode="json")
        for step in result.steps
        for artifact in step.artifacts
    ]
    return {
        "schema_version": "1",
        "trial_id": str(result.id),
        "team_id": str(result.team_id),
        "task_id": result.task_id,
        "trajectory_uri": result.trajectory_uri,
        "atif_uri": result.atif_uri,
        "atif_schema_version": result.atif_schema_version,
        "artifacts": artifacts,
    }
