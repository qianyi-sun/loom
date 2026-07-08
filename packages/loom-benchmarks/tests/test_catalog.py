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

import re

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


def test_skilllearnbench_catalog_declares_dual_arch_portability() -> None:
    """#49: certified portable benchmarks must declare compatibility
    explicitly in catalog metadata before adapters emit `cpu_arch=any`."""
    params = CATALOG["skilllearnbench"].params

    assert params["skill_method"] == "human_authored"
    assert params["cpu_arch"] == "any"


def test_livecodebench_upstream_is_pinned_to_hf_revision() -> None:
    """#307: LiveCodeBench must publish a stable official task set;
    floating HF HEAD would make future publish/register runs drift."""
    revision = CATALOG["livecodebench"].upstream.revision
    assert revision is not None
    assert re.fullmatch(r"[0-9a-f]{40}", revision)


def test_bfcl_upstream_is_pinned_to_git_revision() -> None:
    """#307: BFCL v4 must publish a stable official task set; floating
    `main` would make future publish/register runs drift."""
    revision = CATALOG["bfcl"].upstream.revision
    assert revision is not None
    assert re.fullmatch(r"[0-9a-f]{40}", revision)


def test_swe_bench_verified_upstream_is_pinned_to_hf_revision() -> None:
    """#307: SWE-Bench Verified must publish the stable official 500-task set;
    floating HF HEAD would make future publish/register runs drift."""
    revision = CATALOG["swe-bench-verified"].upstream.revision
    assert revision is not None
    assert re.fullmatch(r"[0-9a-f]{40}", revision)


def test_gpqa_math500_and_hendrycks_math_upstreams_are_pinned_sets() -> None:
    """#307: reasoning benchmarks use selected full official sets, not
    floating or sample subsets."""
    gpqa = CATALOG["gpqa"]
    assert gpqa.upstream.revision == "56686c06f5e19865c153de0fdb11be3890014df7"
    assert gpqa.params == {"subset": "extended", "rows": "546"}

    math500 = CATALOG["math-500"]
    assert math500.upstream.locator == "HuggingFaceH4/MATH-500"
    assert math500.upstream.subset is None
    assert math500.upstream.revision == "6e4ed1a2a79af7d8630a6b768ec859cb5af4d3be"
    assert math500.params == {"rows": "500"}

    math = CATALOG["hendrycks-math"]
    assert math.upstream.locator == "HuggingFaceTB/MATH"
    assert math.upstream.subset == "all"
    assert math.upstream.trust_remote_code is True
    assert math.upstream.revision == "140a673f1f7182daf7923fdc7108e8cdbf97df46"
    assert math.params == {"subset": "all", "rows": "5000"}


def test_mmlu_pro_upstream_is_pinned_full_test_set() -> None:
    """#307: MMLU-Pro publishes the full official test split."""
    mmlu_pro = CATALOG["mmlu-pro"]
    assert mmlu_pro.upstream.locator == "TIGER-Lab/MMLU-Pro"
    assert mmlu_pro.upstream.revision == "b189ec765aa7ed75c8acfea42df31fdae71f97be"
    assert mmlu_pro.params == {"rows": "12032"}


def test_tau2_bench_upstream_is_pinned_default_leaderboard_set() -> None:
    """#307: tau2-bench uses the complete default leaderboard domains,
    not mock, telecom_small, or a task-id-filtered run."""
    tau2 = CATALOG["tau2-bench"]
    assert tau2.upstream.locator == "https://huggingface.co/datasets/HuggingFaceH4/tau2-bench-data"
    assert tau2.upstream.revision == "60e37c7a19672769a6034c45a5c8b36e7cd3768b"
    assert tau2.params == {"domains": "airline,retail,telecom", "rows": "278"}


def test_browsecomp_upstream_is_pinned_full_csv_set() -> None:
    """#307: BrowseComp publishes the complete official 1,266-question
    simple-evals release and records the blob ETag because the data lives
    outside the git tree."""
    browsecomp = CATALOG["browsecomp"]
    assert browsecomp.upstream.locator == "https://github.com/openai/simple-evals.git"
    assert browsecomp.upstream.revision == "652c89d0ca9df547706735883097e9537d40dc47"
    assert browsecomp.params == {
        "rows": "1266",
        "csv_url": "https://openaipublic.blob.core.windows.net/simple-evals/browse_comp_test_set.csv",
        "csv_etag": "0x8DD785A972BF8A0",
    }


def test_skillflow_iterative_catalog_declares_family_run_defaults() -> None:
    """#672 PR-3: the iterative variant opts into family-run mode via
    the catalog's ``family_run_defaults`` block. The batches route
    reads this and seeds ``batch_family_state`` at accept time."""
    entry = CATALOG["skillflow-iterative"]
    assert entry.family_run_defaults is not None
    defaults = entry.family_run_defaults
    assert defaults["enabled"] is True
    assert defaults["adapter"]["name"] == "skill_patcher_llm"
    assert defaults["family_key_extractor"]["name"] == "instance_id_prefix"
    assert defaults["sequencer"]["name"] == "ranking_file"
    assert defaults["failure_policy"]["name"] == "stall_family"
    assert defaults["state_backend"]["name"] == "s3_artifacts"
    assert defaults["mount_path"] == "/root/.skills"


def test_default_skillflow_has_no_family_run_defaults() -> None:
    """Backward compatibility: the single-shot ``skillflow`` catalog
    entry stays classic. Operators pick ``skillflow-iterative``
    explicitly to opt into shared-skill mode."""
    assert CATALOG["skillflow"].family_run_defaults is None
