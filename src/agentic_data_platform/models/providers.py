from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from agentic_data_platform.domain.run_records import ModelConfig, ModelMode, TerminalTurn


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


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")
