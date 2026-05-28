from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Protocol, runtime_checkable

from agentic_data_platform.domain.run_records import (
    ArtifactRef,
    BenchmarkTaskInstance,
    EvaluatorResult,
    JudgeConfig,
    RunRecord,
)


@dataclass(frozen=True)
class EvaluatorInput:
    run_id: str
    project_id: str
    owner_team: str
    task: BenchmarkTaskInstance
    trajectory_ref: ArtifactRef
    workspace_ref: ArtifactRef
    artifact_refs: list[ArtifactRef]
    rubric_id: str
    judge: JudgeConfig
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_run(
        cls,
        run: RunRecord,
        *,
        trajectory_ref: ArtifactRef,
        workspace_ref: ArtifactRef,
        artifact_refs: list[ArtifactRef],
        rubric_id: str,
        judge: JudgeConfig,
        metadata: dict[str, Any] | None = None,
    ) -> EvaluatorInput:
        return cls(
            run_id=run.run_id,
            project_id=run.project_id,
            owner_team=run.owner_team,
            task=run.task,
            trajectory_ref=trajectory_ref,
            workspace_ref=workspace_ref,
            artifact_refs=list(artifact_refs),
            rubric_id=rubric_id,
            judge=judge,
            metadata=metadata or {},
        )

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("project_id", self.project_id)
        _require_non_empty("owner_team", self.owner_team)
        _require_non_empty("rubric_id", self.rubric_id)

    def all_artifacts(self) -> list[ArtifactRef]:
        return [self.trajectory_ref, self.workspace_ref, *self.artifact_refs]

    def to_dict(self) -> dict[str, Any]:
        return _to_jsonable(self)


@runtime_checkable
class EvaluatorAdapter(Protocol):
    def evaluate(self, evaluator_input: EvaluatorInput) -> EvaluatorResult:
        ...


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    if is_dataclass(value):
        return {
            item.name: _to_jsonable(getattr(value, item.name))
            for item in fields(value)
            if getattr(value, item.name) is not None
        }

    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]

    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}

    return value
