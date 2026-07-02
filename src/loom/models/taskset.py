"""User TaskSet manifest schema (`loom.taskset/v1`, #242 sub-plan 2)."""

from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_API_VERSION = "loom.taskset/v1"
_KIND = "UserTaskSet"
_SOURCE_TYPES = frozenset({"hf", "git", "https", "jsonl-inline"})
_INTENTS = frozenset({"trajectory_generation", "evaluation"})
_VERIFIER_TYPES = frozenset({"pytest", "script", "exact-match", "regex", "llm-judge"})
_SLUG_RE = re.compile(r"^[a-z0-9](?:[a-z0-9_-]*[a-z0-9])?$")


class TaskSetMetadata(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=128)
    display_name: str = Field(min_length=1, max_length=256)


class TaskSetSource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["hf", "git", "https", "jsonl-inline"]
    locator: str = Field(min_length=1)
    revision: str | None = None
    subset: str | None = None
    split: str | None = None


class TaskSetVerifier(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    type: Literal["pytest", "script", "exact-match", "regex", "llm-judge"]
    file: str = Field(min_length=1)


class TaskSetTransform(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    file: str = Field(min_length=1)


class TaskSetLimits(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    max_instances: int = Field(default=500, ge=1)
    timeout_per_task_s: int = Field(default=300, ge=1)


class UserTaskSetManifest(BaseModel):
    """Validated manifest for team-submitted TaskSets."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    api_version: Literal["loom.taskset/v1"] = Field(
        validation_alias="apiVersion",
        serialization_alias="apiVersion",
    )
    kind: Literal["UserTaskSet"]
    metadata: TaskSetMetadata
    intents: list[Literal["trajectory_generation", "evaluation"]] | None = None
    source: TaskSetSource
    instance_mapping: dict[str, str]
    task_template: dict[str, Any]
    verifier: TaskSetVerifier | None = None
    transform: TaskSetTransform | None = None
    limits: TaskSetLimits | None = None

    @model_validator(mode="after")
    def _validate_slug_and_intents(self) -> UserTaskSetManifest:
        slug = self.metadata.name.strip()
        if slug != self.metadata.name:
            raise ValueError("metadata.name must not have leading or trailing whitespace")
        if ".." in slug or "/" in slug or "\\" in slug or "." in slug:
            raise ValueError("metadata.name must not contain path traversal characters")
        if not _SLUG_RE.match(slug):
            raise ValueError(
                "metadata.name must be a lowercase slug (alphanumeric, hyphens, underscores)",
            )

        intents = self.intents or ["trajectory_generation"]
        unknown = set(intents) - _INTENTS
        if unknown:
            raise ValueError(f"unsupported intents: {sorted(unknown)}")
        if len(intents) != len(set(intents)):
            raise ValueError("duplicate intents are not allowed")
        if "evaluation" in intents and self.verifier is None:
            raise ValueError("verifier_required_for_evaluation")
        return self

    @property
    def slug(self) -> str:
        return self.metadata.name


def task_set_id_for(*, team_id: str, slug: str) -> str:
    return f"ts/{team_id}/{slug}"


__all__ = [
    "_API_VERSION",
    "_INTENTS",
    "_SOURCE_TYPES",
    "_VERIFIER_TYPES",
    "UserTaskSetManifest",
    "task_set_id_for",
]
