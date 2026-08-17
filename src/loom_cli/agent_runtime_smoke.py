"""Agent runtime smoke helpers.

Runtime audit checks whether dependencies exist in an image. Smoke checks go
one level deeper: run a minimal trial path for each displayed agent and report
whether it reaches a terminal platform result.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
import threading
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path, PurePosixPath
from typing import Any, Literal
from uuid import UUID, uuid4

from loom_service.agent_catalog import AgentEntry, list_agents, resolve_agents

SmokeState = Literal["passed", "failed"]
AgentSmokeRunner = Callable[..., Awaitable["AgentRuntimeSmokeItem"]]


@dataclass(frozen=True)
class AgentRuntimeSmokeItem:
    image: str
    name: str
    kind: str
    smoke_state: SmokeState
    trial_state: str | None
    failure_reason: str | None
    failure_message: str | None
    duration_sec: float
    trajectory_uri: str | None
    atif_uri: str | None


def run_agent_smoke_matrix(
    *,
    image: str,
    agents: Sequence[str] | None = None,
    timeout_sec: float = 60.0,
    model_name: str = "smoke-model",
) -> list[AgentRuntimeSmokeItem]:
    selected = _select_agents(agents)
    with _FakeProviderServer() as gateway_url:
        return asyncio.run(
            _run_smoke_matrix(
                image=image,
                agents=selected,
                timeout_sec=timeout_sec,
                model_name=model_name,
                gateway_url=gateway_url,
                smoke_runner=_run_one_agent_smoke,
            )
        )


def render_agent_smoke_json(items: list[AgentRuntimeSmokeItem]) -> str:
    image = items[0].image if items else None
    return json.dumps(
        {
            "image": image,
            "count": len(items),
            "items": [asdict(item) for item in items],
        },
        indent=2,
        sort_keys=True,
    )


def render_agent_smoke_table(items: list[AgentRuntimeSmokeItem]) -> str:
    name_w = max(7, max((len(item.name) for item in items), default=0))
    state_w = max(5, max((len(item.smoke_state) for item in items), default=0))
    trial_w = max(5, max((len(item.trial_state or "-") for item in items), default=0))
    reason_w = max(
        7,
        max((len(item.failure_reason or "-") for item in items), default=0),
    )
    label_agent = "AGENT"
    label_smoke = "SMOKE"
    label_trial = "TRIAL"
    label_duration = "DURATION"
    label_reason = "REASON"
    rows = [
        f"{label_agent:<{name_w}} {label_smoke:<{state_w}} "
        f"{label_trial:<{trial_w}} {label_duration:>8} {label_reason:<{reason_w}}"
    ]
    for item in items:
        trial_state = item.trial_state or "-"
        failure_reason = item.failure_reason or "-"
        rows.append(
            f"{item.name:<{name_w}} {item.smoke_state:<{state_w}} "
            f"{trial_state:<{trial_w}} {item.duration_sec:>8.2f} "
            f"{failure_reason:<{reason_w}}"
        )
    return "\n".join(rows)


def _select_agents(agents: Sequence[str] | None) -> list[AgentEntry]:
    if not agents:
        return list_agents()
    return resolve_agents(agents)


async def _run_smoke_matrix(
    *,
    image: str,
    agents: Sequence[AgentEntry],
    timeout_sec: float,
    model_name: str,
    gateway_url: str,
    smoke_runner: AgentSmokeRunner,
) -> list[AgentRuntimeSmokeItem]:
    items: list[AgentRuntimeSmokeItem] = []
    for agent in agents:
        started = time.monotonic()
        try:
            item = await asyncio.wait_for(
                smoke_runner(
                    image=image,
                    agent=agent,
                    timeout_sec=timeout_sec,
                    model_name=model_name,
                    gateway_url=gateway_url,
                ),
                timeout=timeout_sec,
            )
        except TimeoutError as exc:
            item = _failure_item(
                image=image,
                agent=agent,
                started=started,
                reason="timeout",
                message=str(exc) or f"agent smoke exceeded {timeout_sec}s",
            )
        except Exception as exc:
            item = _failure_item(
                image=image,
                agent=agent,
                started=started,
                reason=_exception_reason(exc),
                message=str(exc),
            )
        items.append(item)
    return items


async def _run_one_agent_smoke(
    *,
    image: str,
    agent: AgentEntry,
    timeout_sec: float,
    model_name: str,
    gateway_url: str,
) -> AgentRuntimeSmokeItem:
    from loom.agent.gateway_client import FakeLLMGatewayClient
    from loom.driver.docker import DockerDriver
    from loom.models.task import (
        AgentDefaults,
        EnvironmentConfig,
        StepConfig,
        TaskConfig,
        TaskMetadata,
        VerifierDefaults,
    )
    from loom.models.trajectory import ChatMessage
    from loom.models.trial import TrialConfig
    from loom.models.types import ModelSpec
    from loom.models.verifier import VerifierResult
    from loom.trajectory.storage import FakeObjectStore
    from loom_worker.main_loop import _default_agent_factory
    from loom_worker.trial_runner import LocalTrialRunner

    class _PassVerifier:
        name = "smoke-pass"

        async def verify(
            self,
            *,
            task: TaskConfig,
            env: object,
            artifacts_dir: PurePosixPath,
            trajectory: object,
        ) -> VerifierResult:
            return VerifierResult(rewards={"smoke": 1.0})

    trial_id = uuid4()
    team_id = uuid4()
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix=f"loom-agent-smoke-{agent.name}-") as td:
        task_dir = _write_smoke_task(Path(td), agent=agent)
        trajectory_root = Path(td) / "trajectories"
        task = TaskConfig(
            schema_version="1",
            task=TaskMetadata(id=f"smoke-{agent.name}", name=f"smoke-{agent.name}"),
            environment=EnvironmentConfig(
                os="linux",
                docker_image=image,
                workdir=PurePosixPath("/workspace"),
            ),
            agent=AgentDefaults(
                name=agent.name,
                timeout_sec=max(1.0, timeout_sec),
            ),
            verifier=VerifierDefaults(name="smoke-pass", timeout_sec=5.0),
            steps=[StepConfig(name="main")],
        )
        model = None
        if agent.needs_model:
            model = ModelSpec(provider=_model_provider(agent), name=model_name)
        trial_config = TrialConfig(
            agent_name=agent.name,
            agent_model=model,
            override_agent_timeout_sec=max(1.0, timeout_sec),
            override_verifier_timeout_sec=5.0,
        )
        runner = LocalTrialRunner(
            trial_id=trial_id,
            team_id=team_id,
            task_config=task,
            task_checksum="0" * 64,
            task_dir=task_dir,
            trial_config=trial_config,
            driver_factory=lambda: DockerDriver(
                image=image,
                workspace=PurePosixPath("/workspace"),
            ),
            agent_factory=_default_agent_factory(
                team_id,
                trial_id,
                cp_client=_StepTokenClient(),
                worker_gateway_url=gateway_url,
            ),
            verifier_factory=lambda: _PassVerifier(),
            object_store=FakeObjectStore(),
            gateway_client=FakeLLMGatewayClient(
                scripted=[
                    _fake_gateway_chat_response(
                        ChatMessage(role="assistant", content="smoke complete")
                    )
                ],
            ),
            local_trajectory_root=trajectory_root,
            state_patch_callback=_noop_state_patch,
        )
        result = await runner.run()
        trial_state = _value(result.state)
        smoke_state: SmokeState = "passed" if trial_state == "succeeded" else "failed"
        return AgentRuntimeSmokeItem(
            image=image,
            name=agent.name,
            kind=agent.kind,
            smoke_state=smoke_state,
            trial_state=trial_state,
            failure_reason=_value(result.failure_reason),
            failure_message=result.failure_message or _first_step_error_message(result),
            duration_sec=time.monotonic() - started,
            trajectory_uri=result.trajectory_uri,
            atif_uri=result.atif_uri,
        )


def _write_smoke_task(root: Path, *, agent: AgentEntry) -> Path:
    task_dir = root / "task"
    task_dir.mkdir(parents=True)
    (task_dir / "task.toml").write_text('schema_version = "1"\n', encoding="utf-8")
    (task_dir / "instruction.md").write_text(
        "Run a minimal Loom smoke check and exit successfully.",
        encoding="utf-8",
    )
    solution_dir = task_dir / "solution"
    solution_dir.mkdir()
    solve = solution_dir / "solve.sh"
    smoke_line = f'printf "%s\\n" "smoke {agent.name}" > /workspace/loom-smoke.txt\n'
    solve.write_text(
        "#!/bin/sh\nset -eu\n" + smoke_line,
        encoding="utf-8",
    )
    solve.chmod(0o755)
    return task_dir


def _model_provider(agent: AgentEntry) -> str:
    providers = list(agent.supported_providers)
    if not providers or providers[0] == "*":
        return "openai"
    return providers[0]


def _fake_gateway_chat_response(message: Any) -> Any:
    from loom.agent.gateway_client import GatewayCallResponse

    return GatewayCallResponse(
        response=message,
        input_tokens=1,
        cached_input_tokens=0,
        cache_write_tokens=0,
        output_tokens=1,
        thinking_tokens=0,
        provider_extras={},
        cost_usd=0.0,
        finish_reason="stop",
        duration_sec=0.01,
        streamed=False,
        time_to_first_token_sec=None,
        rate_card_hash="smoke",
        gateway_request_id="smoke",
    )


async def _noop_state_patch(
    state: str,
    failure_reason: str | None,
    failure_message: str | None = None,
) -> bool:
    return True


class _StepTokenClient:
    async def mint_step_token(
        self,
        *,
        team_id: UUID,
        trial_id: UUID,
        step_id: str,
        ttl_sec: int,
    ) -> str:
        return "loom_step_smoke-token"


def _failure_item(
    *,
    image: str,
    agent: AgentEntry,
    started: float,
    reason: str,
    message: str,
) -> AgentRuntimeSmokeItem:
    return AgentRuntimeSmokeItem(
        image=image,
        name=agent.name,
        kind=agent.kind,
        smoke_state="failed",
        trial_state=None,
        failure_reason=reason,
        failure_message=message,
        duration_sec=time.monotonic() - started,
        trajectory_uri=None,
        atif_uri=None,
    )


def _exception_reason(exc: Exception) -> str:
    name = type(exc).__name__
    out = []
    for idx, char in enumerate(name):
        if char.isupper() and idx > 0:
            out.append("_")
        out.append(char.lower())
    return "".join(out)


def _value(value: object | None) -> str | None:
    if value is None:
        return None
    raw = getattr(value, "value", value)
    return str(raw)


def _first_step_error_message(result: Any) -> str | None:
    for step in getattr(result, "steps", []):
        error = getattr(step, "error", None)
        if error is not None and getattr(error, "message", None):
            return str(error.message)
    return None


class _FakeProviderServer:
    def __enter__(self) -> str:
        self._server = ThreadingHTTPServer(("0.0.0.0", 0), _FakeProviderHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="loom-agent-smoke-fake-provider",
            daemon=True,
        )
        self._thread.start()
        host = os.environ.get("LOOM_AGENT_SMOKE_GATEWAY_HOST", "172.17.0.1")
        return f"http://{host}:{self._server.server_port}"

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5.0)


class _FakeProviderHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.endswith("/models") or self.path == "/models":
            self._send_json(
                200,
                {
                    "object": "list",
                    "data": [{"id": "smoke-model", "object": "model"}],
                },
            )
            return
        self._send_json(200, {"ok": True})

    def do_POST(self) -> None:
        body = self._read_json_body()
        path = self.path.split("?", 1)[0]
        if path.endswith("/messages"):
            self._send_json(200, _anthropic_message_response(body))
            return
        if ":generateContent" in path or path.endswith("/generateContent"):
            self._send_json(200, _gemini_response())
            return
        if ":streamGenerateContent" in path:
            self._send_sse(200, [_gemini_response()])
            return
        if path.endswith("/responses"):
            if "text/event-stream" in self.headers.get("Accept", ""):
                self._send_sse(200, _openai_response_stream_events(body))
                return
            self._send_json(200, _openai_response_response(body))
            return
        if body.get("stream") is True:
            self._send_sse(200, _openai_chat_stream_chunks(body))
            return
        self._send_json(200, _openai_chat_response(body))

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            return {}
        return body if isinstance(body, dict) else {}

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_sse(self, status: int, payloads: Sequence[dict[str, Any]]) -> None:
        data = "".join(f"data: {json.dumps(payload)}\n\n" for payload in payloads).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def _openai_chat_response(body: dict[str, Any]) -> dict[str, Any]:
    tool_call = _openai_tool_call_response(body)
    if tool_call is not None:
        return tool_call
    return {
        "id": "chatcmpl-smoke",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": str(body.get("model") or "smoke-model"),
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "smoke complete"},
                "finish_reason": "stop",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _openai_tool_call_response(body: dict[str, Any]) -> dict[str, Any] | None:
    tools = body.get("tools")
    if not isinstance(tools, list) or not tools:
        return None
    first = tools[0]
    if not isinstance(first, dict):
        return None
    function = first.get("function")
    if not isinstance(function, dict):
        return None
    name = function.get("name")
    if not isinstance(name, str) or not name:
        return None
    return {
        "id": "chatcmpl-smoke",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": str(body.get("model") or "smoke-model"),
        "choices": [
            {
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call_smoke",
                            "type": "function",
                            "function": {
                                "name": name,
                                "arguments": json.dumps(
                                    {"command": ("echo COMPLETE_TASK_AND_SUBMIT_FINAL_OUTPUT")}
                                ),
                            },
                        }
                    ],
                },
                "finish_reason": "tool_calls",
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _openai_chat_stream_chunks(body: dict[str, Any]) -> list[dict[str, Any]]:
    model = str(body.get("model") or "smoke-model")
    created = int(time.time())
    return [
        {
            "id": "chatcmpl-smoke",
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant"}, "finish_reason": None}],
        },
        {
            "id": "chatcmpl-smoke",
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "delta": {"content": "smoke complete"},
                    "finish_reason": None,
                }
            ],
        },
        {
            "id": "chatcmpl-smoke",
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
            "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        },
    ]


def _openai_response_response(body: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": "resp_smoke",
        "object": "response",
        "created_at": int(time.time()),
        "status": "completed",
        "model": str(body.get("model") or "smoke-model"),
        "output": [
            {
                "type": "message",
                "id": "msg_smoke",
                "status": "completed",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "smoke complete"}],
            }
        ],
        "output_text": "smoke complete",
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
    }


def _openai_response_stream_events(body: dict[str, Any]) -> list[dict[str, Any]]:
    response = _openai_response_response(body)
    return [
        {"type": "response.created", "response": {**response, "status": "in_progress"}},
        {
            "type": "response.output_item.added",
            "output_index": 0,
            "item": response["output"][0],
        },
        {
            "type": "response.output_text.delta",
            "item_id": "msg_smoke",
            "output_index": 0,
            "content_index": 0,
            "delta": "smoke complete",
        },
        {
            "type": "response.output_text.done",
            "item_id": "msg_smoke",
            "output_index": 0,
            "content_index": 0,
            "text": "smoke complete",
        },
        {"type": "response.completed", "response": response},
    ]


def _anthropic_message_response(body: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": "msg_smoke",
        "type": "message",
        "role": "assistant",
        "model": str(body.get("model") or "smoke-model"),
        "content": [{"type": "text", "text": "smoke complete"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 1, "output_tokens": 1},
    }


def _gemini_response() -> dict[str, Any]:
    return {
        "candidates": [
            {
                "content": {
                    "role": "model",
                    "parts": [{"text": "smoke complete"}],
                },
                "finishReason": "STOP",
            }
        ],
        "usageMetadata": {
            "promptTokenCount": 1,
            "candidatesTokenCount": 1,
            "totalTokenCount": 2,
        },
    }
