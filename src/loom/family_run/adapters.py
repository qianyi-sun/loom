"""Family adapter plugins (#672).

``NoopAdapter`` is the framework-shipped identity adapter; the reference
``skill_patcher_llm`` adapter ships in PR-2 alongside the orchestrator
service.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loom.family_run.protocols import FamilyStateLike, StateBackend, TrialLike
from loom.family_run.spec import ResolvedFamilyRunSpec


@dataclass
class NoopAdapter:
    default_params: dict[str, Any] = field(default_factory=dict)

    async def initialize_state(
        self,
        *,
        family_key: str,
        spec: ResolvedFamilyRunSpec,
        backend: StateBackend,
        state_uri: str,
        params: dict[str, Any],
    ) -> str:
        return state_uri

    async def evolve(
        self,
        *,
        trial: TrialLike,
        family: FamilyStateLike,
        state_uri: str,
        backend: StateBackend,
        params: dict[str, Any],
    ) -> str:
        return state_uri
