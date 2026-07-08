"""Family-key extractor plugins (#672)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from loom.family_run.protocols import TaskLike


@dataclass
class InstanceIdPrefixExtractor:
    """Family key = first ``depth`` path segments of ``task.id``."""

    default_params: dict[str, Any] = field(default_factory=dict)

    def key_for(self, task: TaskLike) -> str:
        depth = int(self.default_params.get("depth", 1))
        parts = task.id.split("/")
        return "/".join(parts[:depth]) if len(parts) > depth else task.id
