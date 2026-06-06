"""LLMJudgeVerifier — submits trajectory excerpt + rubric to the Gateway.

Stubbed against the LLMGatewayClient Protocol; Plan 4 wires the real HTTP
backend. The verifier prompts the model for a JSON object:
`{rewards, confidence, rationale}`.

Excerpt strategy resolution (spec §3.6): defaults to TailExcerpt(n=50);
constructor accepts an override.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Any

from loom.agent.gateway_client import GatewayCallRequest, LLMGatewayClient
from loom.driver.base import Driver
from loom.models.trajectory import ChatMessage
from loom.models.types import ModelSpec
from loom.models.verifier import VerifierError, VerifierResult
from loom.trajectory.excerpt import ExcerptStrategy, TailExcerpt
from loom.trajectory.reader import TrajectoryReader

if TYPE_CHECKING:
    from loom.models.task import TaskConfig


@dataclass
class LLMJudgeVerifier:
    model: ModelSpec
    gateway: LLMGatewayClient
    team_id: str
    trial_id: str
    rubric_prompt: str
    excerpt_strategy: ExcerptStrategy = field(default_factory=lambda: TailExcerpt(n=50))
    excerpt_max_tokens: int = 32_000
    name: str = "llm-judge"

    async def verify(
        self,
        *,
        task: TaskConfig,
        env: Driver,
        artifacts_dir: PurePosixPath,
        trajectory: TrajectoryReader,
    ) -> VerifierResult:
        events = trajectory.excerpt(
            self.excerpt_strategy, max_tokens=self.excerpt_max_tokens,
        )
        excerpt_text = "\n".join(e.model_dump_json() for e in events)

        prompt = (
            f"{self.rubric_prompt}\n\n"
            f"Trajectory excerpt (most recent events):\n{excerpt_text}\n\n"
            "Respond with a JSON object: "
            '{"rewards": {<key>: <0..1>, ...}, '
            '"confidence": <0..1>, "rationale": <string>}.'
        )
        request = GatewayCallRequest(
            model=self.model,
            messages=[ChatMessage(role="user", content=prompt)],
            system_prompt="You are a strict and structured grading judge.",
            tools=None, tool_choice=None,
            team_id=self.team_id, trial_id=self.trial_id, step_id="__verifier__",
        )
        response = await self.gateway.call(request)
        content = response.response.content

        if not isinstance(content, str):
            return VerifierResult(
                rewards={},
                error=VerifierError(
                    kind="parse_failure",
                    message="LLM judge returned non-string content",
                ),
            )

        try:
            parsed: dict[str, Any] = json.loads(content)
        except json.JSONDecodeError as exc:
            return VerifierResult(
                rewards={},
                error=VerifierError(
                    kind="parse_failure",
                    message=f"LLM judge response is not valid JSON: {exc}",
                    detail={"raw": content[:512]},
                ),
            )

        rewards = parsed.get("rewards", {})
        if not isinstance(rewards, dict):
            return VerifierResult(
                rewards={},
                error=VerifierError(
                    kind="parse_failure",
                    message="`rewards` is not an object",
                ),
            )

        rationale = parsed.get("rationale")
        return VerifierResult(
            rewards={str(k): float(v) for k, v in rewards.items()},
            confidence=parsed.get("confidence"),
            structured={"rationale": rationale} if rationale is not None else None,
        )
