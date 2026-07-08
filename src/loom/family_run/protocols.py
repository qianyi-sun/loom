"""Protocol classes for family-run plugins (#672)."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from loom.family_run.spec import (
    AdvanceDecision,
    FailureAction,
    ResolvedFamilyRunSpec,
)


class TaskLike(Protocol):
    """Minimum surface the family-run plugins consume from a task row."""

    id: str
    tags: dict[str, str] | list[str] | None


class TrialLike(Protocol):
    """Minimum surface consumed at finalize."""

    id: UUID
    task_id: str
    state: str
    reward: float | None
    attempt_count: int


class FamilyStateLike(Protocol):
    """Minimum surface consumed by advance / failure plugins."""

    batch_id: UUID
    family_key: str
    task_sequence: list[str]
    current_index: int
    attempt_count: int


@runtime_checkable
class FamilyKeyExtractor(Protocol):
    def key_for(self, task: TaskLike) -> str: ...


@runtime_checkable
class Sequencer(Protocol):
    def sequence(
        self,
        family_key: str,
        tasks: list[TaskLike],
        params: dict[str, Any],
    ) -> list[str]: ...


@runtime_checkable
class AdvancePredicate(Protocol):
    def decide(
        self,
        *,
        trial: TrialLike,
        family: FamilyStateLike,
        spec: ResolvedFamilyRunSpec,
        params: dict[str, Any],
    ) -> AdvanceDecision: ...


@runtime_checkable
class StateBackend(Protocol):
    async def initialize(
        self,
        *,
        batch_id: UUID,
        family_key: str,
        params: dict[str, Any],
    ) -> str: ...

    async def download(
        self,
        state_uri: str,
        dst: Path,
        params: dict[str, Any],
    ) -> None: ...

    async def upload(
        self,
        state_uri: str,
        src: Path,
        params: dict[str, Any],
    ) -> str: ...


@runtime_checkable
class Adapter(Protocol):
    async def initialize_state(
        self,
        *,
        family_key: str,
        spec: ResolvedFamilyRunSpec,
        backend: StateBackend,
        state_uri: str,
        params: dict[str, Any],
    ) -> str: ...

    async def evolve(
        self,
        *,
        trial: TrialLike,
        family: FamilyStateLike,
        state_uri: str,
        backend: StateBackend,
        params: dict[str, Any],
    ) -> str: ...


@runtime_checkable
class FailurePolicy(Protocol):
    def on_adapter_failure(
        self,
        *,
        family: FamilyStateLike,
        exception: BaseException,
        params: dict[str, Any],
    ) -> FailureAction: ...
