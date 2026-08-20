from __future__ import annotations

import ast
import re
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class GenerationMode(str, Enum):
    SKILL_BASED = "skill-based"
    AGENT_SKILL_BASED = "agent-skill-based"
    SEED_BASED = "seed-based"
    ATOMIC_TARGET = "atomic-target"


class SynthesizerMode(str, Enum):
    OPENCODE_AGENT = "opencode-agent"
    OPENAI_JSON = "openai-json"


class Difficulty(str, Enum):
    MEDIUM = "medium"
    HARD = "hard"
    EXPERT = "expert"
    MIXED = "mixed"


class AtomicVariantBucket(str, Enum):
    PARAMETRIC = "same-domain-parametric"
    STRUCTURAL = "same-domain-structural"
    CROSS_DOMAIN = "cross-domain-isomorph"
    DIAGNOSE_REPAIR = "diagnose-and-repair"
    ADVERSARIAL_ROLLBACK = "adversarial-rollback"


class AtomicWeaknessCard(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_task: str
    capability_id: str
    primary_domain: str
    allowed_domains: list[str] = Field(min_length=1)
    summary: str
    atomic_chain: list[str] = Field(min_length=2)
    failure_signatures: list[str] = Field(min_length=1)
    required_gates: list[str] = Field(min_length=1)
    forbidden_shortcuts: list[str] = Field(min_length=1)
    variation_axes: list[str] = Field(min_length=3)
    oracle_requirements: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def normalize_and_validate(self) -> "AtomicWeaknessCard":
        for field_name in (
            "source_task",
            "capability_id",
            "primary_domain",
            "summary",
        ):
            value = getattr(self, field_name).strip()
            if not value:
                raise ValueError(f"{field_name} cannot be empty")
            setattr(self, field_name, value)
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", self.source_task):
            raise ValueError("source_task must be a kebab-case identifier")
        if not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", self.capability_id):
            raise ValueError("capability_id must be a kebab-case identifier")

        self.allowed_domains = _normalize_nonempty_strings(
            self.allowed_domains,
            field_name="allowed_domains",
        )
        if self.primary_domain not in self.allowed_domains:
            raise ValueError("primary_domain must appear in allowed_domains")
        for field_name in (
            "atomic_chain",
            "failure_signatures",
            "required_gates",
            "forbidden_shortcuts",
            "variation_axes",
            "oracle_requirements",
        ):
            setattr(
                self,
                field_name,
                _normalize_nonempty_strings(getattr(self, field_name), field_name=field_name),
            )
        return self


class TaskFile(BaseModel):
    name: str
    context: str

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        cleaned = _normalize_workspace_relative_path(value)
        if not cleaned:
            raise ValueError("file name cannot be empty")
        return cleaned


class DatasetTask(BaseModel):
    id: str | None = None
    task_id: str | None = None
    prompt: str
    tests: str
    info: dict[str, Any] | None = None
    files: list[TaskFile] = Field(default_factory=list)
    workspace_dir: Path | None = Field(default=None, exclude=True)
    dockerfile: str | None = None
    solution: str | None = None
    test_requirements: list[str] = Field(default_factory=list)
    extra: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def normalize_and_validate(self) -> "DatasetTask":
        self.prompt = self.prompt.strip()
        self.tests = self.tests.strip()
        if self.dockerfile is not None:
            self.dockerfile = self.dockerfile.strip() or None
        if self.solution is not None:
            self.solution = self.solution.strip() or None
        self.test_requirements = [item.strip() for item in self.test_requirements if item.strip()]
        if not self.prompt:
            raise ValueError("prompt cannot be empty")
        if not self.tests:
            raise ValueError("tests cannot be empty")
        ast.parse(self.tests)
        _validate_task_files(self.files)
        task_id = self.stable_id
        if not task_id:
            raise ValueError("task_id or id is required")
        if "pytest" not in self.tests and "pytest" not in self.test_requirements:
            self.test_requirements = ["pytest", *self.test_requirements]
        return self

    @property
    def stable_id(self) -> str | None:
        return self.task_id or self.id

    def directory_placeholder_names(self) -> set[str]:
        return _directory_placeholder_names(self.files)

    def to_jsonable(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


class SeedRecord(BaseModel):
    model_config = ConfigDict(extra="forbid")

    seed_id: str
    content: str

    @model_validator(mode="before")
    @classmethod
    def accept_legacy_context(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        payload = dict(value)
        content = payload.get("content")
        context = payload.get("context")
        if content is not None and context is not None and content != context:
            raise ValueError("content and context must match when both are provided")
        if content is None and context is not None:
            payload["content"] = context
        payload.pop("context", None)
        return payload

    @model_validator(mode="after")
    def normalize_and_validate(self) -> "SeedRecord":
        self.seed_id = self.seed_id.strip()
        if not self.seed_id:
            raise ValueError("seed_id cannot be empty")
        if not self.content.strip():
            raise ValueError("content cannot be empty")
        return self

    @property
    def context(self) -> str:
        return self.content


class TaskPlan(BaseModel):
    model_config = ConfigDict(extra="forbid")

    domain: str = Field(max_length=64)
    difficulty: str = Field(max_length=16)
    objective: str = Field(max_length=400)
    initial_state: str = Field(max_length=2_000)
    workflow: list[str] = Field(min_length=2, max_length=8)
    deliverables: list[str] = Field(min_length=1, max_length=4)
    verification: list[str] = Field(min_length=1, max_length=10)
    constraints: list[str] = Field(default_factory=list, max_length=8)

    @model_validator(mode="after")
    def normalize_and_validate(self) -> "TaskPlan":
        self.domain = self.domain.strip()
        self.difficulty = self.difficulty.strip()
        self.objective = self.objective.strip()
        self.initial_state = self.initial_state.strip()
        if not self.domain or not self.difficulty:
            raise ValueError("plan domain and difficulty cannot be empty")
        if not self.objective:
            raise ValueError("plan objective cannot be empty")
        if not self.initial_state:
            raise ValueError("plan initial_state cannot be empty")
        for field_name in ("workflow", "deliverables", "verification", "constraints"):
            values = [value.strip() for value in getattr(self, field_name)]
            if any(not value for value in values):
                raise ValueError(f"plan {field_name} cannot contain empty items")
            if any(len(value) > 500 for value in values):
                raise ValueError(f"plan {field_name} items must be at most 500 characters")
            setattr(self, field_name, values)
        text_size = sum(
            len(value)
            for value in (
                self.objective,
                self.initial_state,
                *self.workflow,
                *self.deliverables,
                *self.verification,
                *self.constraints,
            )
        )
        if text_size > 5_000:
            raise ValueError("plan text must be at most 5000 characters")
        return self


class GenerationRequest(BaseModel):
    sample_index: int
    generation_mode: GenerationMode
    domain: str
    difficulty: str
    skills: list[str] = Field(default_factory=list)
    seed_record: SeedRecord | None = None
    seed_content: str | None = None
    domain_candidates: list[str] = Field(default_factory=list)
    plan: TaskPlan | None = None
    atomic_card: AtomicWeaknessCard | None = None
    variant_bucket: AtomicVariantBucket | None = None
    variant_index: int | None = Field(default=None, ge=1)
    template_family_id: str | None = None
    attempt_index: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def validate_atomic_target(self) -> "GenerationRequest":
        is_atomic = self.generation_mode == GenerationMode.ATOMIC_TARGET
        atomic_fields = (
            self.atomic_card,
            self.variant_bucket,
            self.variant_index,
            self.template_family_id,
        )
        if is_atomic and any(value is None for value in atomic_fields):
            raise ValueError("atomic-target requests require card, bucket, index, and family id")
        if not is_atomic and any(value is not None for value in atomic_fields):
            raise ValueError("atomic target fields are only valid for atomic-target requests")
        return self


_PROMPT_LEAK_PATTERNS = [
    ("pytest", re.compile(r"\bpytest\b", re.IGNORECASE)),
    ("unit test", re.compile(r"unit test", re.IGNORECASE)),
]


def prompt_has_test_leakage(prompt: str) -> bool:
    return prompt_test_leakage_matches(prompt) != []


def prompt_test_leakage_matches(prompt: str) -> list[str]:
    matches: list[str] = []
    for label, pattern in _PROMPT_LEAK_PATTERNS:
        if pattern.search(prompt):
            matches.append(label)
    return matches


def _normalize_nonempty_strings(values: list[str], *, field_name: str) -> list[str]:
    normalized = [value.strip() for value in values]
    if any(not value for value in normalized):
        raise ValueError(f"{field_name} cannot contain empty items")
    if len(set(normalized)) != len(normalized):
        raise ValueError(f"{field_name} cannot contain duplicate items")
    return normalized


def _normalize_workspace_relative_path(value: str) -> str:
    cleaned = value.strip().replace("\\", "/")
    while "//" in cleaned:
        cleaned = cleaned.replace("//", "/")
    while cleaned.startswith("./"):
        cleaned = cleaned[2:]

    for prefix in ("app/", "workspace/"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :]
            break

    for prefix in ("/app/", "/workspace/", "/root/project/", "/root/workspace/"):
        if cleaned.startswith(prefix):
            cleaned = cleaned[len(prefix) :]
            break
    else:
        if cleaned.startswith("/"):
            cleaned = cleaned.lstrip("/")

    normalized = PurePosixPath(cleaned).as_posix()
    if normalized in {".", ""}:
        raise ValueError("file name cannot be empty")
    if normalized.startswith("../") or normalized == "..":
        raise ValueError("file name must be workspace-relative")
    if any(part == ".." for part in PurePosixPath(normalized).parts):
        raise ValueError("file name must be workspace-relative")
    return normalized


def _directory_placeholder_names(files: list[TaskFile]) -> set[str]:
    parent_paths: set[str] = set()
    for task_file in files:
        parts = PurePosixPath(task_file.name).parts
        for index in range(1, len(parts)):
            parent_paths.add(PurePosixPath(*parts[:index]).as_posix())
    return {
        task_file.name
        for task_file in files
        if task_file.name in parent_paths and task_file.context == ""
    }


def _validate_task_files(files: list[TaskFile]) -> None:
    seen_paths: set[str] = set()
    parent_paths: set[str] = set()

    for task_file in files:
        if task_file.name in seen_paths:
            raise ValueError(f"duplicate task file path: {task_file.name}")
        seen_paths.add(task_file.name)

        parts = PurePosixPath(task_file.name).parts
        for index in range(1, len(parts)):
            parent_paths.add(PurePosixPath(*parts[:index]).as_posix())

    for task_file in files:
        if task_file.name in parent_paths and task_file.context != "":
            raise ValueError(
                "task file path cannot be both a file and a directory: "
                f"{task_file.name}"
            )
