"""Declarative catalog of first-party benchmarks.

Single source of truth for benchmark metadata — series, display name,
upstream source, license, splits, and per-benchmark params. Loaded
from `benchmarks.json` shipped alongside this module. Adapter classes
that inherit `CatalogBackedAdapter` pick up everything except their
conversion logic from here.

**Why:** the previous shape (7 class attrs per adapter file ×
16 adapters) made structural changes — renaming a series, moving
osworld from `agents` to `ui-agent`, splitting aime-aimo-validation
into per-year siblings — touch 4 surfaces every time (adapter file +
seed script + migration + tests). Promoting metadata to a JSON
manifest centralizes the taxonomy and makes the diff legible.

**Schema evolution.** `schema_version` is a positive int. Readers must
tolerate unknown fields (forward-compat) and document required-fields
bumps explicitly. Pydantic `extra="ignore"` enforces tolerance.

**Plugin extensibility.** Third-party adapter packages keep declaring
metadata as class attrs (the legacy path); the JSON catalog only
ships first-party benchmarks under `loom_benchmarks/`. The base class
falls back to existing class attrs when the catalog has no entry for
`cls.name`, so a third-party plugin works without changes here."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from loom_benchmarks.base import UpstreamSource


class _CatalogUpstream(BaseModel):
    """Catalog projection of `UpstreamSource`. Converted to the
    runtime dataclass via `.to_dataclass()` so the rest of the
    package keeps using the frozen-dataclass type it already knows."""

    model_config = ConfigDict(extra="ignore")

    kind: Literal["huggingface", "git", "https-tarball"]
    locator: str
    revision: str | None = None
    subset: str | None = None
    trust_remote_code: bool = False

    def to_dataclass(self) -> UpstreamSource:
        return UpstreamSource(
            kind=self.kind,
            locator=self.locator,
            revision=self.revision,
            subset=self.subset,
            trust_remote_code=self.trust_remote_code,
        )


class _CatalogLicense(BaseModel):
    model_config = ConfigDict(extra="ignore")

    spdx: str
    url: str = ""
    execution_policy: Literal["allowlist", "notice"] = "allowlist"


class CatalogEntry(BaseModel):
    """One benchmark's declarative metadata. Mirrors the attribute
    block the adapter class would otherwise repeat."""

    model_config = ConfigDict(extra="ignore")

    name: str = Field(min_length=1)
    display_name: str = Field(min_length=1)
    series: str | None = None
    upstream: _CatalogUpstream
    license: _CatalogLicense
    splits: tuple[str, ...]
    # Per-benchmark knobs the adapter reads via `self._params`.
    # AIME's per-year adapters use this to share a single base class.
    params: dict[str, str] = Field(default_factory=dict)
    # #672 family-runs: benchmark-level defaults for the family-run
    # resolver. Any subset of :class:`FamilyRunSpec` roles is legal;
    # the batches route validates the shape at accept-time so a typo
    # here surfaces without waiting for the first trial to claim.
    family_run_defaults: dict[str, Any] | None = None


class Catalog(BaseModel):
    model_config = ConfigDict(extra="ignore")

    schema_version: int = Field(ge=1)
    benchmarks: list[CatalogEntry]


_CATALOG_PATH = Path(__file__).parent / "benchmarks.json"


def _load_catalog() -> dict[str, CatalogEntry]:
    """Parse `benchmarks.json` into a name → entry mapping. Module-level
    side effect; raises at import time if the catalog doesn't parse
    so typos surface fast instead of leaking into runtime fetches."""
    raw = json.loads(_CATALOG_PATH.read_text())
    parsed = Catalog.model_validate(raw)
    by_name: dict[str, CatalogEntry] = {}
    for entry in parsed.benchmarks:
        if entry.name in by_name:
            raise ValueError(
                f"benchmarks.json: duplicate benchmark name {entry.name!r}",
            )
        by_name[entry.name] = entry
    return by_name


CATALOG: dict[str, CatalogEntry] = _load_catalog()
