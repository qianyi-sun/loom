"""Validation helpers for user-owned local benchmark folders (#275)."""

from __future__ import annotations

import json
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import tomli_w
from pydantic import BaseModel, Field, ValidationError, field_validator

from loom.config.benchmarks import (
    BENCHMARK_ID_PATTERN,
    LocalBenchmarkEntry,
    normalize_source_subdir,
)
from loom.models.task import TaskConfig
from loom_cli.benchmarks_sync import walk_task_tomls


class LocalBenchmarkValidationError(Exception):
    def __init__(self, message: str, *, exit_code: int = 1) -> None:
        super().__init__(message)
        self.exit_code = exit_code


class BenchmarkToml(BaseModel):
    """Metadata file accepted at `<benchmark-root>/benchmark.toml`."""

    model_config = {"extra": "forbid"}
    schema_version: Literal[1] = 1
    id: str = Field(min_length=1, pattern=BENCHMARK_ID_PATTERN)
    display_name: str = Field(min_length=1)
    series: str = Field(min_length=1)
    license_spdx: str = Field(min_length=1)
    source_subdir: str | None = "tasks"

    @field_validator("source_subdir")
    @classmethod
    def _validate_source_subdir(cls, value: str | None) -> str | None:
        return normalize_source_subdir(value)

    def as_entry(self) -> LocalBenchmarkEntry:
        return LocalBenchmarkEntry(
            id=self.id,
            display_name=self.display_name,
            series=self.series,
            license_spdx=self.license_spdx,
            source_subdir=self.source_subdir,
        )


@dataclass(frozen=True)
class LocalBenchmarkValidationResult:
    root: Path
    task_root: Path
    entry: LocalBenchmarkEntry
    task_tomls: tuple[Path, ...]

    @property
    def task_count(self) -> int:
        return len(self.task_tomls)


def validate_local_benchmark(
    root: Path,
    *,
    benchmark_id: str | None = None,
    display_name: str | None = None,
    series: str | None = None,
    license_spdx: str | None = None,
    source_subdir: str | None = None,
) -> LocalBenchmarkValidationResult:
    root = root.resolve()
    if not root.is_dir():
        raise LocalBenchmarkValidationError(
            f"benchmark folder not found: {root}", exit_code=2,
        )

    metadata_path = root / "benchmark.toml"
    if metadata_path.exists():
        entry = _load_benchmark_toml(metadata_path).as_entry()
    else:
        missing = [
            name for name, value in (
                ("--id", benchmark_id),
                ("--display-name", display_name),
                ("--series", series),
                ("--license-spdx", license_spdx),
            ) if not value
        ]
        if missing:
            raise LocalBenchmarkValidationError(
                "benchmark.toml not found; pass metadata flags instead: "
                + ", ".join(missing),
                exit_code=2,
            )
        try:
            entry = LocalBenchmarkEntry(
                id=str(benchmark_id),
                display_name=str(display_name),
                series=str(series),
                license_spdx=str(license_spdx),
                source_subdir=source_subdir,
            )
        except ValidationError as exc:
            raise LocalBenchmarkValidationError(
                f"invalid local benchmark metadata: {exc}", exit_code=2,
            ) from exc

    task_root = root / entry.source_subdir if entry.source_subdir else root
    if not task_root.is_dir():
        raise LocalBenchmarkValidationError(
            f"task source directory not found: {task_root}", exit_code=1,
        )

    task_tomls = tuple(walk_task_tomls(task_root))
    if not task_tomls:
        raise LocalBenchmarkValidationError(
            f"no task.toml files found under {task_root}", exit_code=1,
        )
    for task_toml in task_tomls:
        _validate_task_toml(task_toml)

    return LocalBenchmarkValidationResult(
        root=root,
        task_root=task_root,
        entry=entry,
        task_tomls=task_tomls,
    )


def render_config_snippet(entry: LocalBenchmarkEntry) -> str:
    data = {
        "schema_version": 1,
        "local": [entry.model_dump(exclude_none=True)],
    }
    return tomli_w.dumps(data).strip()


def render_validation_json(result: LocalBenchmarkValidationResult) -> str:
    return json.dumps(
        {
            "benchmark_id": result.entry.id,
            "display_name": result.entry.display_name,
            "series": result.entry.series,
            "license_spdx": result.entry.license_spdx,
            "source_subdir": result.entry.source_subdir,
            "task_count": result.task_count,
            "root": str(result.root),
            "task_root": str(result.task_root),
            "config_snippet": render_config_snippet(result.entry),
        },
        indent=2,
        sort_keys=True,
    )


def _load_benchmark_toml(path: Path) -> BenchmarkToml:
    try:
        with path.open("rb") as f:
            raw = tomllib.load(f)
        return BenchmarkToml.model_validate(raw)
    except (OSError, tomllib.TOMLDecodeError, ValidationError) as exc:
        raise LocalBenchmarkValidationError(
            f"invalid benchmark.toml at {path}: {exc}", exit_code=1,
        ) from exc


def _validate_task_toml(path: Path) -> None:
    try:
        with path.open("rb") as f:
            raw = tomllib.load(f)
        TaskConfig.model_validate(raw)
    except Exception as exc:
        raise LocalBenchmarkValidationError(
            f"invalid task.toml at {path}: {exc}", exit_code=1,
        ) from exc
