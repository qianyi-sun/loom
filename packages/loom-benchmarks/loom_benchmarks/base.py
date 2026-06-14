"""Adapter contract + value types (benchmark integrations spec §3).

Each benchmark adapter declares its upstream source + license up-front
(class attributes) and implements two pure operations:

- `list_instances` walks a fetched source dir and yields
  `BenchmarkInstance` records.
- `convert_instance` writes the canonical Loom task layout (task.toml +
  instruction.md + solution/ + tests/ [+ environment/]) into `out_dir`
  and returns a `ConvertedTask` carrying the new task_id, the
  content-hash checksum, the license SPDX tag, and any warnings the
  conversion emitted.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol, runtime_checkable


@dataclass(frozen=True)
class UpstreamSource:
    kind: Literal["huggingface", "git", "https-tarball"]
    locator: str
    revision: str | None = None
    subset: str | None = None


@dataclass(frozen=True)
class BenchmarkInstance:
    instance_id: str
    split: str
    raw: dict[str, Any]
    # Open-ended key→value metadata surfaced to the SPA's tag filter
    # and the per-task `tags` column. Adapter convention: lowercase
    # string keys, stringified values (year="2024", exam="I",
    # verified="true"). Default empty so legacy adapters keep working.
    tags: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ConvertedTask:
    task_id: str
    checksum: str
    license_spdx: str
    warnings: tuple[str, ...]


@runtime_checkable
class BenchmarkAdapter(Protocol):
    name: str
    display_name: str
    upstream_source: UpstreamSource
    license_spdx: str
    license_url: str
    splits: tuple[str, ...]

    def list_instances(
        self, *, source_dir: Path, split: str,
    ) -> Iterator[BenchmarkInstance]:
        """Yield BenchmarkInstance records. Pure: same source_dir +
        split → same order."""
        ...

    def convert_instance(
        self, instance: BenchmarkInstance, *, out_dir: Path,
    ) -> ConvertedTask:
        """Write task.toml + instruction.md (+ solution/, tests/,
        environment/) into out_dir."""
        ...
