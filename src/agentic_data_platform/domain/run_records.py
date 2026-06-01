from __future__ import annotations

from dataclasses import dataclass, field, is_dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Iterable


class ModelMode(str, Enum):
    API = "api"


class SandboxBackend(str, Enum):
    DOCKER_TERMINAL = "docker_terminal"


class RunnerKind(str, Enum):
    ORIGINAL_BENCHMARK = "original_benchmark"
    CUSTOM_PIPELINE = "custom_pipeline"


class RunStatus(str, Enum):
    QUEUED = "queued"
    DISPATCHED = "dispatched"
    PROVISIONING = "provisioning"
    RUNNING = "running"
    EVALUATING = "evaluating"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"


class ArtifactKind(str, Enum):
    INPUT_BUNDLE = "input_bundle"
    TRAJECTORY = "trajectory"
    WORKSPACE_SNAPSHOT = "workspace_snapshot"
    EVALUATOR_REPORT = "evaluator_report"
    GENERATED_FILE = "generated_file"
    SKILL_OBJECT = "skill_object"
    LOG = "log"


class SkillRepresentation(str, Enum):
    TEXT = "text"
    FILE_BUNDLE = "file_bundle"
    EMBEDDING = "embedding"
    CHECKPOINT = "checkpoint"
    OPAQUE = "opaque"


@dataclass(frozen=True)
class BenchmarkTaskInstance:
    benchmark_suite: str
    benchmark_version: str
    task_family: str
    instance_id: str
    source_uri: str
    input_artifact_refs: list[str]
    required_artifacts: list[str]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("benchmark_suite", self.benchmark_suite)
        _require_non_empty("benchmark_version", self.benchmark_version)
        _require_non_empty("task_family", self.task_family)
        _require_non_empty("instance_id", self.instance_id)
        _require_non_empty("source_uri", self.source_uri)
        _require_strings("input_artifact_refs", self.input_artifact_refs)
        _require_strings("required_artifacts", self.required_artifacts)

        missing = {"trajectory", "workspace_snapshot", "evaluator_report"} - set(self.required_artifacts)
        if missing:
            raise ValueError(f"required_artifacts must include MVP records: {sorted(missing)}")


@dataclass(frozen=True)
class ModelConfig:
    provider: str
    model_name: str
    mode: ModelMode | str
    prompt_template_version: str
    model_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("provider", self.provider)
        _require_non_empty("model_name", self.model_name)
        _require_non_empty("prompt_template_version", self.prompt_template_version)

        if self.mode != ModelMode.API and self.mode != ModelMode.API.value:
            raise ValueError("v0 supports API-based model access only")

        object.__setattr__(self, "mode", ModelMode.API)


@dataclass(frozen=True)
class RunnerConfig:
    kind: RunnerKind | str
    sandbox_backend: SandboxBackend | str
    image: str
    entrypoint: list[str]
    internet_access: bool
    resource_limits: dict[str, int | float]
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        kind = _coerce_enum(RunnerKind, self.kind, "kind")
        backend = _coerce_enum(SandboxBackend, self.sandbox_backend, "sandbox_backend")

        _require_non_empty("image", self.image)
        _require_strings("entrypoint", self.entrypoint)

        if backend is not SandboxBackend.DOCKER_TERMINAL:
            raise ValueError("v0 supports Docker terminal sandbox execution only")

        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "sandbox_backend", backend)


@dataclass(frozen=True)
class TerminalTurn:
    turn_index: int
    command: str
    cwd: str
    started_at: datetime
    completed_at: datetime
    exit_code: int
    stdout: str
    stderr: str
    changed_paths: list[str]
    model_call_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.turn_index < 0:
            raise ValueError("turn_index must be non-negative")

        _require_non_empty("command", self.command)
        _require_non_empty("cwd", self.cwd)
        _require_strings("changed_paths", self.changed_paths)

        if self.started_at.tzinfo is None or self.completed_at.tzinfo is None:
            raise ValueError("started_at and completed_at must be timezone-aware")

        if self.completed_at < self.started_at:
            raise ValueError("completed_at must be after started_at")


@dataclass(frozen=True)
class ArtifactRef:
    artifact_id: str
    kind: ArtifactKind | str
    uri: str
    media_type: str
    sha256: str | None = None
    size_bytes: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        kind = _coerce_enum(ArtifactKind, self.kind, "kind")

        _require_non_empty("artifact_id", self.artifact_id)
        _require_non_empty("uri", self.uri)
        _require_non_empty("media_type", self.media_type)

        if self.sha256 is not None and len(self.sha256) != 64:
            raise ValueError("sha256 must be a 64-character hex digest")

        if self.size_bytes is not None and self.size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")

        object.__setattr__(self, "kind", kind)


@dataclass(frozen=True)
class JudgeConfig:
    provider: str
    model_name: str
    rubric_version: str
    model_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("provider", self.provider)
        _require_non_empty("model_name", self.model_name)
        _require_non_empty("rubric_version", self.rubric_version)


@dataclass(frozen=True)
class EvaluatorConfig:
    evaluator_id: str
    mode: str
    judge: JudgeConfig | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("evaluator_id", self.evaluator_id)
        _require_non_empty("mode", self.mode)
        if self.mode in {"llm_judge", "hybrid"} and self.judge is None:
            raise ValueError(f"{self.mode} evaluator config requires a judge")


@dataclass(frozen=True)
class EvaluatorResult:
    evaluator_id: str
    status: str
    score: float | None
    metrics: dict[str, Any]
    verbal_feedback: str
    judge: JudgeConfig | None
    artifact_refs: list[str]
    mode: str = "llm_judge"
    failure_reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        _require_non_empty("evaluator_id", self.evaluator_id)
        _require_non_empty("status", self.status)
        _require_non_empty("mode", self.mode)
        _require_strings("artifact_refs", self.artifact_refs)

        if self.mode not in _EVALUATOR_MODES:
            allowed = ", ".join(sorted(_EVALUATOR_MODES))
            raise ValueError(f"mode must be one of: {allowed}")

        if self.mode in {"llm_judge", "hybrid"} and self.judge is None:
            raise ValueError(f"{self.mode} evaluator result requires a judge")

        if self.score is not None and not 0 <= self.score <= 1:
            raise ValueError("score must be between 0 and 1")

        if self.status == "completed" and self.mode in {"llm_judge", "hybrid", "manual_review"}:
            _require_non_empty("verbal_feedback", self.verbal_feedback)

        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")


@dataclass(frozen=True)
class SkillObjectRef:
    skill_id: str
    producing_run_id: str
    representation: SkillRepresentation | str
    artifact_refs: list[str]
    benchmark_suite: str
    task_family: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        representation = _coerce_enum(SkillRepresentation, self.representation, "representation")

        _require_non_empty("skill_id", self.skill_id)
        _require_non_empty("producing_run_id", self.producing_run_id)
        _require_strings("artifact_refs", self.artifact_refs)
        _require_non_empty("benchmark_suite", self.benchmark_suite)
        _require_non_empty("task_family", self.task_family)

        object.__setattr__(self, "representation", representation)

    def to_dict(self) -> dict[str, Any]:
        return _to_jsonable(self)


@dataclass
class RunRecord:
    run_id: str
    project_id: str
    owner_team: str
    task: BenchmarkTaskInstance
    model: ModelConfig
    runner: RunnerConfig
    status: RunStatus
    created_at: datetime
    updated_at: datetime
    trajectory: list[TerminalTurn] = field(default_factory=list)
    artifacts: list[ArtifactRef] = field(default_factory=list)
    evaluator_configs: list[EvaluatorConfig] = field(default_factory=list)
    evaluator_results: list[EvaluatorResult] = field(default_factory=list)
    evaluator_result: EvaluatorResult | None = None
    failure_reason: str | None = None
    created_by_user_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        project_id: str,
        owner_team: str,
        task: BenchmarkTaskInstance,
        model: ModelConfig,
        runner: RunnerConfig,
        evaluator_configs: list[EvaluatorConfig] | None = None,
        created_by_user_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> RunRecord:
        now = datetime.now(timezone.utc)
        return cls(
            run_id=run_id,
            project_id=project_id,
            owner_team=owner_team,
            task=task,
            model=model,
            runner=runner,
            status=RunStatus.QUEUED,
            created_at=now,
            updated_at=now,
            created_by_user_id=created_by_user_id,
            evaluator_configs=evaluator_configs or [],
            metadata=metadata or {},
        )

    def __post_init__(self) -> None:
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("project_id", self.project_id)
        _require_non_empty("owner_team", self.owner_team)

        self.status = _coerce_enum(RunStatus, self.status, "status")

        if self.created_at.tzinfo is None or self.updated_at.tzinfo is None:
            raise ValueError("created_at and updated_at must be timezone-aware")

        if self.created_by_user_id is not None:
            _require_non_empty("created_by_user_id", self.created_by_user_id)
        for evaluator_config in self.evaluator_configs:
            if not isinstance(evaluator_config, EvaluatorConfig):
                raise ValueError("evaluator_configs must contain EvaluatorConfig values")
        for evaluator_result in self.evaluator_results:
            if not isinstance(evaluator_result, EvaluatorResult):
                raise ValueError("evaluator_results must contain EvaluatorResult values")
        if self.evaluator_results and self.evaluator_result is None:
            self.evaluator_result = self.evaluator_results[-1]
        elif self.evaluator_result is not None and self.evaluator_result not in self.evaluator_results:
            self.evaluator_results.append(self.evaluator_result)

    def transition_to(self, next_status: RunStatus | str) -> None:
        next_status = _coerce_enum(RunStatus, next_status, "next_status")
        valid_next = _VALID_TRANSITIONS[self.status]

        if next_status not in valid_next:
            raise ValueError(
                f"Invalid run status transition from {self.status.value} to {next_status.value}"
            )

        self.status = next_status
        self.updated_at = datetime.now(timezone.utc)

    def add_turn(self, turn: TerminalTurn) -> None:
        if self.status is not RunStatus.RUNNING:
            raise ValueError("terminal turns can only be added while the run is running")

        if turn.turn_index != len(self.trajectory):
            raise ValueError("turn_index must match the next trajectory position")

        self.trajectory.append(turn)
        self.updated_at = datetime.now(timezone.utc)

    def attach_artifact(self, artifact: ArtifactRef) -> None:
        self.artifacts.append(artifact)
        self.updated_at = datetime.now(timezone.utc)

    def attach_evaluator_result(self, result: EvaluatorResult) -> None:
        if self.status is not RunStatus.EVALUATING:
            raise ValueError("evaluator results can only be attached while the run is evaluating")

        self.evaluator_results.append(result)
        self.evaluator_result = result
        self.updated_at = datetime.now(timezone.utc)

    def all_evaluator_results(self) -> list[EvaluatorResult]:
        results = list(self.evaluator_results)
        if self.evaluator_result is not None and self.evaluator_result not in results:
            results.append(self.evaluator_result)
        return results

    def to_dict(self) -> dict[str, Any]:
        return _to_jsonable(self)


@dataclass(frozen=True)
class RunStatusEvent:
    event_id: str
    seq: int
    run_id: str
    attempt_id: str | None
    event_type: str
    from_status: RunStatus | str | None
    to_status: RunStatus | str
    created_at: datetime
    reason: str | None = None
    actor_user_id: str | None = None
    request_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        _require_non_empty("event_id", self.event_id)
        _require_non_empty("run_id", self.run_id)
        _require_non_empty("event_type", self.event_type)
        if not isinstance(self.seq, int) or self.seq <= 0:
            raise ValueError("seq must be a positive integer")

        if self.attempt_id is not None:
            _require_non_empty("attempt_id", self.attempt_id)
        if self.actor_user_id is not None:
            _require_non_empty("actor_user_id", self.actor_user_id)
        if self.request_id is not None:
            _require_non_empty("request_id", self.request_id)
        if self.reason is not None:
            _require_non_empty("reason", self.reason)
        if self.created_at.tzinfo is None:
            raise ValueError("created_at must be timezone-aware")

        if self.from_status is not None:
            object.__setattr__(self, "from_status", _coerce_enum(RunStatus, self.from_status, "from_status"))
        object.__setattr__(self, "to_status", _coerce_enum(RunStatus, self.to_status, "to_status"))


_VALID_TRANSITIONS: dict[RunStatus, set[RunStatus]] = {
    RunStatus.QUEUED: {RunStatus.DISPATCHED, RunStatus.PROVISIONING, RunStatus.FAILED, RunStatus.CANCELED},
    RunStatus.DISPATCHED: {RunStatus.PROVISIONING, RunStatus.FAILED, RunStatus.CANCELED},
    RunStatus.PROVISIONING: {RunStatus.RUNNING, RunStatus.FAILED, RunStatus.CANCELED},
    RunStatus.RUNNING: {RunStatus.EVALUATING, RunStatus.FAILED, RunStatus.CANCELED},
    RunStatus.EVALUATING: {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED},
    RunStatus.SUCCEEDED: set(),
    RunStatus.FAILED: set(),
    RunStatus.CANCELED: set(),
}

_EVALUATOR_MODES = {"harbor_verifier", "llm_judge", "hybrid", "manual_review"}


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _require_strings(name: str, values: Iterable[str]) -> None:
    if isinstance(values, str):
        raise ValueError(f"{name} must be a list of strings")

    for value in values:
        _require_non_empty(name, value)


def _coerce_enum(enum_type: type[Enum], value: Enum | str, field_name: str) -> Any:
    if isinstance(value, enum_type):
        return value

    try:
        return enum_type(value)
    except ValueError as exc:
        allowed = ", ".join(item.value for item in enum_type)
        raise ValueError(f"{field_name} must be one of: {allowed}") from exc


def _to_jsonable(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value

    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    if is_dataclass(value):
        return {
            field_name: _to_jsonable(field_value)
            for field_name, field_value in value.__dict__.items()
            if field_value is not None
        }

    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]

    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}

    return value
