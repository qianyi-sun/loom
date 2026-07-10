"""Bridge Harbor trajectory checkpoints into Loom typed events (#744)."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID, uuid4

from loom.agent.terminus2.provenance import (
    HARBOR_COMPAT_SHA,
    LOOM_BRIDGE_REVISION,
    harbor_template_hashes,
)
from loom.models.trajectory import (
    LLMCallEvent,
    Terminus2ArtifactRefEvent,
    Terminus2CommandEvent,
    Terminus2RuntimeProvenanceEvent,
    Terminus2TerminalObservationEvent,
    Terminus2TurnEvent,
)
from loom.models.types import ModelSpec
from loom.security.redaction import redact_text
from loom.trajectory.writer import TrajectoryWriter


class HarborCheckpointBridge:
    """Incrementally sync ``trajectory.json`` episodes to Loom events."""

    def __init__(
        self,
        *,
        trajectory: TrajectoryWriter,
        trial_id: UUID,
        step_id: str,
        model: ModelSpec,
    ) -> None:
        self._trajectory = trajectory
        self._trial_id = trial_id
        self._step_id = step_id
        self._model = model
        self._seq = 0
        self._seen_step_ids: set[int] = set()
        self._provenance_emitted = False

    def _next_seq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq

    async def emit_provenance(self) -> None:
        if self._provenance_emitted:
            return
        self._provenance_emitted = True
        await self._trajectory.append(
            Terminus2RuntimeProvenanceEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self._trial_id,
                step_id=self._step_id,
                seq=self._next_seq(),
                loom_runtime_revision=LOOM_BRIDGE_REVISION,
                harbor_compat_sha=HARBOR_COMPAT_SHA,
                parser_name="json",
                prompt_hash=harbor_template_hashes().get(
                    "terminus-json-plain.txt", "",
                ),
                template_hashes=harbor_template_hashes(),
            ),
        )

    async def sync_trajectory_file(
        self,
        path: Path,
        *,
        completeness: str = "full",
    ) -> int:
        if not path.is_file():
            return 0
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return 0
        steps = data.get("steps") or []
        synced = 0
        for step in steps:
            if not isinstance(step, dict):
                continue
            step_id = step.get("step_id")
            if not isinstance(step_id, int) or step_id in self._seen_step_ids:
                continue
            if step.get("source") != "agent":
                self._seen_step_ids.add(step_id)
                continue
            await self._bridge_agent_step(step, completeness=completeness)
            self._seen_step_ids.add(step_id)
            synced += 1
        return synced

    async def _bridge_agent_step(
        self,
        step: dict[str, Any],
        *,
        completeness: str,
    ) -> None:
        turn_id = str(uuid4())
        batch_id = str(uuid4())
        gateway_request_id = f"harbor-step-{step.get('step_id')}"
        metrics = step.get("metrics") or {}
        tool_calls = step.get("tool_calls") or []
        observation = step.get("observation") or {}
        obs_results = observation.get("results") or []
        obs_text = ""
        if obs_results and isinstance(obs_results[0], dict):
            obs_text = str(obs_results[0].get("content") or "")

        commands = [
            tc for tc in tool_calls
            if isinstance(tc, dict) and tc.get("function_name") == "bash_command"
        ]
        is_complete = any(
            isinstance(tc, dict) and tc.get("function_name") == "mark_task_complete"
            for tc in tool_calls
        )
        completion_state = "complete" if is_complete else "continue"

        if metrics:
            from loom.models.trajectory import ChatMessage

            await self._trajectory.append(
                LLMCallEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self._trial_id,
                    step_id=self._step_id,
                    seq=self._next_seq(),
                    model=self._model,
                    rate_card_hash="harbor-bridge",
                    system_prompt=None,
                    messages=[ChatMessage(role="user", content="")],
                    tools=None,
                    tool_choice=None,
                    response=ChatMessage(
                        role="assistant",
                        content=str(step.get("message") or ""),
                    ),
                    finish_reason="stop",
                    input_tokens=int(metrics.get("input_tokens") or 0),
                    cached_input_tokens=int(metrics.get("cached_input_tokens") or 0),
                    cache_write_tokens=0,
                    output_tokens=int(metrics.get("output_tokens") or 0),
                    thinking_tokens=0,
                    provider_extras={},
                    cost_usd_snapshot=float(metrics.get("cost_usd") or 0.0),
                    duration_sec=0.0,
                    streamed=False,
                    time_to_first_token_sec=None,
                    gateway_request_id=gateway_request_id,
                ),
            )

        await self._trajectory.append(
            Terminus2TurnEvent(
                emitted_at=datetime.now(UTC),
                trial_id=self._trial_id,
                step_id=self._step_id,
                seq=self._next_seq(),
                turn_id=turn_id,
                turn_index=max(0, int(step.get("step_id", 1)) - 2),
                gateway_request_id=gateway_request_id,
                parse_state="ok",
                completion_state=completion_state,  # type: ignore[arg-type]
                analysis="",
                plan="",
                raw_response_excerpt=str(step.get("message") or "")[:2000],
            ),
        )

        for idx, tc in enumerate(commands):
            args = tc.get("arguments") or {}
            await self._trajectory.append(
                Terminus2CommandEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self._trial_id,
                    step_id=self._step_id,
                    seq=self._next_seq(),
                    turn_id=turn_id,
                    command_batch_id=batch_id,
                    command_id=str(tc.get("tool_call_id") or uuid4()),
                    index=idx,
                    keystrokes=str(args.get("keystrokes") or ""),
                    duration_sec=float(args.get("duration") or 1.0),
                ),
            )

        if obs_text:
            redacted = redact_text(obs_text)
            await self._trajectory.append(
                Terminus2TerminalObservationEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self._trial_id,
                    step_id=self._step_id,
                    seq=self._next_seq(),
                    turn_id=turn_id,
                    command_batch_id=batch_id,
                    observation_id=str(uuid4()),
                    text=redacted,
                    capture_source="incremental",
                    byte_len=len(redacted.encode("utf-8")),
                    truncated=False,
                    completeness=completeness,  # type: ignore[arg-type]
                    content_hash=hashlib.sha256(redacted.encode()).hexdigest(),
                    redaction_applied=redacted != obs_text,
                    is_aggregate=len(commands) > 1,
                ),
            )

    async def emit_artifact_refs(
        self,
        logs_dir: Path,
        *,
        sandbox_paths: dict[str, PurePosixPath] | None = None,
    ) -> None:
        sandbox_paths = sandbox_paths or {}
        for name, kind in (
            ("trajectory.json", "terminus_2.pane"),
            ("recording.cast", "recording.cast"),
        ):
            path = logs_dir / name
            if not path.is_file():
                continue
            body = path.read_bytes()
            sandbox_path = sandbox_paths.get(name)
            await self._trajectory.append(
                Terminus2ArtifactRefEvent(
                    emitted_at=datetime.now(UTC),
                    trial_id=self._trial_id,
                    step_id=self._step_id,
                    seq=self._next_seq(),
                    artifact_kind=kind,  # type: ignore[arg-type]
                    sandbox_path=(
                        sandbox_path.as_posix()
                        if sandbox_path is not None
                        else str(path)
                    ),
                    content_hash=hashlib.sha256(body).hexdigest(),
                    size_bytes=len(body),
                    share_policy="restricted",
                ),
            )
