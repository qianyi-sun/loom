from __future__ import annotations

from typing import Any, Protocol

from terminalgen.catalog import DomainSpec
from terminalgen.constants import DEFAULT_BASE_IMAGE
from terminalgen.models import GenerationRequest, TaskPlan
from terminalgen.prompts import build_plan_prompt, build_plan_system_prompt


class JsonGenerator(Protocol):
    def generate_json(self, system_prompt: str, user_prompt: str) -> dict: ...

    def stats_snapshot(self) -> dict[str, Any]: ...


class TaskPlanner(Protocol):
    def generate_plan(
        self,
        request: GenerationRequest,
        domains: list[DomainSpec],
        *,
        base_image: str = DEFAULT_BASE_IMAGE,
    ) -> TaskPlan: ...

    def stats_snapshot(self) -> dict[str, Any]: ...


class OpenAITaskPlanner:
    def __init__(self, generator: JsonGenerator) -> None:
        self.generator = generator

    def generate_plan(
        self,
        request: GenerationRequest,
        domains: list[DomainSpec],
        *,
        base_image: str = DEFAULT_BASE_IMAGE,
    ) -> TaskPlan:
        payload = self.generator.generate_json(
            build_plan_system_prompt(),
            build_plan_prompt(request, domains, base_image=base_image),
        )
        return TaskPlan.model_validate(payload)

    def stats_snapshot(self) -> dict[str, Any]:
        return self.generator.stats_snapshot()
