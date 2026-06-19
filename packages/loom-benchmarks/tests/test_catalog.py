"""`loom_benchmarks/benchmarks.json` schema + adapter consistency.

Pins three contracts the catalog redesign relies on:

1. benchmarks.json parses against the Pydantic schema. Typos in the JSON
   surface at import time as ValidationError, not deep inside a fetch.
2. Every entry-point adapter has a catalog entry. New adapters that
   forget to add JSON metadata will fail this test loud.
3. Every catalog entry has a corresponding entry-point adapter. JSON
   entries with no adapter would mislead the dropdown into showing
   a benchmark nobody can submit.
"""

from __future__ import annotations

import pytest
from loom_benchmarks.catalog import _CATALOG_PATH, CATALOG, Catalog
from pydantic import ValidationError


def test_catalog_json_parses() -> None:
    """If benchmarks.json has a typo (`licence` vs `license`, missing
    field, wrong type) Pydantic raises at module load — this test
    confirms the import path doesn't hide a ValidationError."""
    import json

    raw = json.loads(_CATALOG_PATH.read_text())
    parsed = Catalog.model_validate(raw)
    assert parsed.schema_version == 1
    assert len(parsed.benchmarks) >= 14


def test_every_registry_adapter_has_a_catalog_entry() -> None:
    """The catalog is the source of truth for first-party metadata.
    A new entry-point adapter that ships without a catalog entry
    would silently lose `series`, `display_name`, etc."""
    from loom_benchmarks.registry import REGISTRY

    missing = sorted(s for s in REGISTRY if s not in CATALOG)
    # terminal-bench-2 ships from a sibling package and isn't covered
    # by loom_benchmarks/benchmarks.json — that's fine; only first-party
    # benchmarks (loom_benchmarks/adapters/*) need a catalog entry.
    missing = [s for s in missing if s != "terminal-bench-2"]
    assert missing == [], f"Registry adapters without a benchmarks.json entry: {missing}"


def test_every_catalog_entry_has_a_registry_adapter() -> None:
    """Catalog entries with no adapter would render in the SPA
    dropdown as ghost benchmarks nobody can submit against."""
    from loom_benchmarks.registry import REGISTRY

    extra = sorted(name for name in CATALOG if name not in REGISTRY)
    assert extra == [], f"Catalog entries with no adapter: {extra}"


def test_adapter_metadata_matches_catalog() -> None:
    """For each first-party adapter, the runtime class attrs the
    base-class installed must equal the catalog entry. This is a
    backstop against a third party shipping an adapter that silently
    overrides metadata after CatalogBackedAdapter set it."""
    from loom_benchmarks.registry import REGISTRY

    mismatches: list[str] = []
    for slug, adapter in REGISTRY.items():
        entry = CATALOG.get(slug)
        if entry is None:
            continue
        if adapter.display_name != entry.display_name:
            mismatches.append(
                f"{slug}.display_name: {adapter.display_name!r} != {entry.display_name!r}",
            )
        if entry.series is not None and getattr(adapter, "series", None) != entry.series:
            mismatches.append(
                f"{slug}.series: {getattr(adapter, 'series', None)!r} != {entry.series!r}",
            )
        if adapter.upstream_source.locator != entry.upstream.locator:
            mismatches.append(
                f"{slug}.upstream.locator: {adapter.upstream_source.locator!r} != "
                f"{entry.upstream.locator!r}",
            )
        if adapter.license_spdx != entry.license.spdx:
            mismatches.append(
                f"{slug}.license_spdx: {adapter.license_spdx!r} != {entry.license.spdx!r}",
            )
        if getattr(adapter, "license_execution_policy", "allowlist") != (
            entry.license.execution_policy
        ):
            mismatches.append(
                f"{slug}.license_execution_policy: "
                f"{getattr(adapter, 'license_execution_policy', 'allowlist')!r} != "
                f"{entry.license.execution_policy!r}",
            )
    assert not mismatches, "\n".join(mismatches)


def test_aime_series_is_license_notice_not_hard_blocked() -> None:
    """AIME tasks keep their source/license metadata, but owner decision
    for #274 treats the public benchmark mirror as launchable by default.
    The execution policy is catalog-level so future public-mirror benchmarks
    can reuse the same path without AIME-specific submit bypasses."""
    for slug in ("aime-22", "aime-23", "aime-24", "aime-25"):
        entry = CATALOG[slug]
        assert entry.license.spdx == "proprietary-MAA"
        assert entry.license.execution_policy == "notice"


def test_catalog_pydantic_rejects_extra_required_fields() -> None:
    """Forward-compat: unknown fields are tolerated (extra='ignore').
    Required fields are enforced — missing display_name 422s."""
    from loom_benchmarks.catalog import CatalogEntry

    # Missing display_name should fail
    with pytest.raises(ValidationError):
        CatalogEntry.model_validate(
            {
                "name": "fake",
                "upstream": {"kind": "huggingface", "locator": "x/y"},
                "license": {"spdx": "MIT"},
                "splits": ["test"],
            }
        )
    # Unknown field is silently dropped
    e = CatalogEntry.model_validate(
        {
            "name": "fake",
            "display_name": "Fake",
            "upstream": {"kind": "huggingface", "locator": "x/y"},
            "license": {"spdx": "MIT"},
            "splits": ["test"],
            "future_field_unknown_today": 42,
        }
    )
    assert e.name == "fake"


def test_catalog_aime_year_params_match_subclass_year_filter() -> None:
    """Per-year AIME adapters read `cls._params['year']` instead of
    duplicating it. The catalog entries must carry the right year so
    the adapter filters correctly."""
    assert CATALOG["aime-22"].params == {"year": "2022"}
    assert CATALOG["aime-23"].params == {"year": "2023"}
    assert CATALOG["aime-24"].params == {"year": "2024"}
