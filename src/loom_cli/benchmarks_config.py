"""Operator-facing benchmark registry — `config/benchmarks.toml`.

Loads a TOML file declaring `[[local]]` (folders of task.toml bundles)
and `[[remap]]` (existing adapter against an alternate upstream)
entries. See issue #234 for the full design.

This module is the *parse* layer only. UPSERT into the `benchmarks` and
`tasks` tables lives in `benchmarks_sync.py`; the CLI subcommand lives
in `datasets_cmd.py`.

The TOML file is missing → `load_benchmarks_config` returns None
(legacy no-op behavior). The file is malformed → raises so callers can
exit with a clear error.
"""
from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field, model_validator

_KEBAB = r"^[a-z0-9][a-z0-9-]*$"


class LocalBenchmarkEntry(BaseModel):
    """A folder of `task.toml` bundles to register as a benchmark.

    The source path is *derived*: `<worker.fixtures_root>/<id>/`.
    Operators provision that directory out-of-band (host bind-mount
    in dev compose; PV / hostPath in k8s).
    """

    model_config = {"extra": "forbid"}
    id: str = Field(min_length=1, pattern=_KEBAB)
    display_name: str = Field(min_length=1)
    series: str = Field(min_length=1)
    license_spdx: str = Field(min_length=1)


class RemapBenchmarkEntry(BaseModel):
    """An existing adapter aimed at a different upstream locator.

    `inherit` MUST resolve to a name in `loom_benchmarks.REGISTRY` at
    sync time (the loader can't verify this — pre-flight does).
    """

    model_config = {"extra": "forbid"}
    id: str = Field(min_length=1, pattern=_KEBAB)
    inherit: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    upstream_kind: Literal["huggingface", "git", "https-tarball"]
    upstream_locator: str = Field(min_length=1)
    license_spdx: str = Field(min_length=1)
    license_url: str = Field(min_length=1)
    series: str | None = None
    splits: list[str] | None = None


class BenchmarksConfig(BaseModel):
    model_config = {"extra": "forbid"}
    schema_version: Literal[1] = 1
    local: list[LocalBenchmarkEntry] = []
    remap: list[RemapBenchmarkEntry] = []

    @model_validator(mode="after")
    def _unique_ids(self) -> BenchmarksConfig:
        ids = [e.id for e in self.local] + [e.id for e in self.remap]
        if len(ids) != len(set(ids)):
            seen: set[str] = set()
            dupes = sorted({i for i in ids if i in seen or seen.add(i)})  # type: ignore[func-returns-value]
            raise ValueError(
                f"duplicate benchmark id in benchmarks.toml: {dupes}",
            )
        return self


def load_benchmarks_config(path: Path | None) -> BenchmarksConfig | None:
    """Return the parsed config, or None if `path` is None / missing.

    Raises `pydantic.ValidationError` on malformed content. Callers
    are expected to wrap into a CLI-friendly error message.
    """
    if path is None or not path.exists():
        return None
    with path.open("rb") as f:
        raw = tomllib.load(f)
    return BenchmarksConfig.model_validate(raw)
