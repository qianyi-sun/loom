from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import httpx

from agentic_data_platform.domain.run_records import ModelConfig, ModelMode, TerminalTurn
from agentic_data_platform.providers.errors import (
    ProviderBoundaryError,
    ProviderErrorCode,
    normalize_provider_error,
)


@dataclass(frozen=True)
class ModelCommand:
    command: str
    cwd: str | None = None
    model_call_id: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty("command", self.command)


@dataclass(frozen=True)
class ModelProviderContext:
    run_id: str
    task_instruction: str
    turns: list[TerminalTurn]

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("task_instruction", self.task_instruction)


@runtime_checkable
class ModelProvider(Protocol):
    model: ModelConfig

    def next_command(self, context: ModelProviderContext) -> ModelCommand | None:
        ...


class ScriptedModelProvider:
    def __init__(self, *, model: ModelConfig, commands: list[ModelCommand]) -> None:
        if model.mode is not ModelMode.API:
            raise ValueError("v0 supports API-based model access only")

        self.model = model
        self._commands = list(commands)
        self._index = 0

    def next_command(self, context: ModelProviderContext) -> ModelCommand | None:
        if self._index >= len(self._commands):
            return None

        command = self._commands[self._index]
        self._index += 1
        return command


class OpenAICompatibleModelProvider:
    def __init__(
        self,
        *,
        model: ModelConfig,
        base_url: str,
        api_key: str,
        http_client: httpx.Client | None = None,
        temperature: float = 0.0,
    ) -> None:
        if model.mode is not ModelMode.API:
            raise ValueError("v0 supports API-based model access only")
        _require_non_empty("base_url", base_url)
        _require_non_empty("api_key", api_key)

        self.model = model
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.temperature = temperature
        self._http_client = http_client or httpx.Client(timeout=httpx.Timeout(30.0))

    def next_command(self, context: ModelProviderContext) -> ModelCommand | None:
        try:
            response = self._http_client.post(
                f"{self.base_url}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={
                    "model": self.model.model_name,
                    "temperature": self.temperature,
                    "messages": _messages_for_context(context),
                },
            )
            response.raise_for_status()
            payload = response.json()
        except Exception as exc:
            raise normalize_provider_error(exc) from exc

        response_id = payload.get("id") if isinstance(payload, dict) else None
        content = _message_content(payload)
        action_payload = _terminal_action_payload(content)
        action = action_payload.get("action")
        if action == "finish":
            return None
        if action == "run":
            command = action_payload.get("command")
            if not isinstance(command, str) or not command.strip():
                raise _invalid_action("run action requires a non-empty command")
            cwd = action_payload.get("cwd")
            if cwd is not None and not isinstance(cwd, str):
                raise _invalid_action("cwd must be a string when provided")
            return ModelCommand(
                command=command,
                cwd=cwd,
                model_call_id=(
                    response_id
                    if isinstance(response_id, str) and response_id.strip()
                    else None
                ),
            )
        raise _invalid_action("action must be either run or finish")


def _messages_for_context(context: ModelProviderContext) -> list[dict[str, str]]:
    return [
        {
            "role": "system",
            "content": (
                "You are a terminal-agent controller. Return only JSON. "
                "Use {\"action\":\"run\",\"command\":\"...\","
                "\"cwd\":\"/workspace\"} to execute a command, "
                "or {\"action\":\"finish\"} when the task is complete."
            ),
        },
        {
            "role": "user",
            "content": "\n".join(
                [
                    f"Run id: {context.run_id}",
                    "",
                    "Task instruction:",
                    context.task_instruction,
                    "",
                    "Terminal trajectory so far:",
                    _trajectory_context(context.turns),
                    "",
                    "Choose the next terminal action.",
                ]
            ),
        },
    ]


def _trajectory_context(turns: list[TerminalTurn]) -> str:
    if not turns:
        return "(no commands have been executed yet)"
    lines: list[str] = []
    for turn in turns[-8:]:
        lines.extend(
            [
                f"Turn {turn.turn_index}",
                f"cwd: {turn.cwd}",
                f"command: {turn.command}",
                f"exit_code: {turn.exit_code}",
                f"stdout: {_clip(turn.stdout)}",
                f"stderr: {_clip(turn.stderr)}",
            ]
        )
    return "\n".join(lines)


def _message_content(payload: object) -> str:
    if not isinstance(payload, dict):
        raise _invalid_action("provider response must be a JSON object")
    choices = payload.get("choices")
    if not isinstance(choices, list) or not choices:
        raise _invalid_action("provider response did not include choices")
    first = choices[0]
    if not isinstance(first, dict):
        raise _invalid_action("provider response choice must be an object")
    message = first.get("message")
    if not isinstance(message, dict):
        raise _invalid_action("provider response choice did not include message")
    content = message.get("content")
    if not isinstance(content, str) or not content.strip():
        raise _invalid_action("provider response message did not include content")
    return content


def _terminal_action_payload(content: str) -> dict[str, object]:
    try:
        payload = json.loads(content)
    except json.JSONDecodeError as exc:
        raise _invalid_action("provider response was not valid terminal action JSON") from exc
    if not isinstance(payload, dict):
        raise _invalid_action("terminal action JSON must be an object")
    return payload


def _invalid_action(message: str) -> ProviderBoundaryError:
    return ProviderBoundaryError(
        code=ProviderErrorCode.INVALID_REQUEST,
        message=f"invalid terminal action response: {message}",
        retryable=False,
    )


def _clip(value: str, *, limit: int = 4000) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "\n...[truncated]"


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
