"""HelloAdapter — internal no-model canary for exercising the contract.

Not a real agent. It is intentionally hidden from user-facing service
catalogs and model compatibility matrices. Its `build_invocation` returns an
`echo` command so launcher contract tests can run it against a FakeDriver /
scripted ExecHandle without any external setup or model call.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import PurePosixPath
from uuid import UUID

from loom_launcher.adapter import ExecHandle, ModelSpec, TrajectoryEventLike
from loom_launcher.capture import stream_stdout_jsonl
from loom_launcher.registry import register_adapter


@dataclass(frozen=True)
class HelloAdapter:
    name: str = "hello"
    needs_model: bool = False
    catalog_visibility: str = "internal"
    supports_os: frozenset[str] = frozenset({"linux"})
    endpoint_dialect: str = "openai_chat"
    api_key_env: str = "OPENAI_API_KEY"
    base_url_env: str = "OPENAI_BASE_URL"
    model_name_template: str = "{model_id}"
    supports_multi_turn: bool = False
    additional_egress: frozenset[str] = frozenset()

    def build_invocation(
        self,
        *,
        instruction: str,
        workdir: PurePosixPath,
        model: ModelSpec,
        env: dict[str, str],
    ) -> list[str]:
        # Echoes a fixed JSONL line for capture tests to consume.
        return ["echo", f'{{"line": "hello from {instruction!s}"}}']

    async def capture_events(
        self,
        *,
        exec_handle: ExecHandle,
        step_id: str,
        trial_id: UUID,
    ) -> AsyncIterator[TrajectoryEventLike]:
        async for event in stream_stdout_jsonl(exec_handle):
            yield event


# Self-register at import time. cast tells mypy the dataclass
# structurally satisfies the AgentAdapter Protocol (runtime_checkable
# + Generic[T] don't infer cleanly together; the structural check still
# fires at runtime in the registry).
from typing import cast as _cast  # noqa: E402

from loom_launcher.adapter import AgentAdapter as _AgentAdapter  # noqa: E402

register_adapter(_cast(_AgentAdapter, HelloAdapter()))
