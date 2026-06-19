"""Loader unit tests for `config/benchmarks.toml` (issue #234)."""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from loom_cli.benchmarks_config import (
    BenchmarksConfig,
    LocalBenchmarkEntry,
    RemapBenchmarkEntry,
    load_benchmarks_config,
)


def _write(tmp_path: Path, body: str) -> Path:
    p = tmp_path / "benchmarks.toml"
    p.write_text(body)
    return p


def test_missing_file_returns_none(tmp_path: Path) -> None:
    assert load_benchmarks_config(tmp_path / "nope.toml") is None
    assert load_benchmarks_config(None) is None


def test_empty_file_loads_with_no_entries(tmp_path: Path) -> None:
    cfg = load_benchmarks_config(_write(tmp_path, "schema_version = 1\n"))
    assert cfg is not None
    assert cfg.local == []
    assert cfg.remap == []


def test_valid_local_and_remap_load(tmp_path: Path) -> None:
    body = """
schema_version = 1

[[local]]
id = "team-evals"
display_name = "Internal team evaluations"
series = "internal"
license_spdx = "proprietary"

[[remap]]
id = "humaneval-fork"
inherit = "humaneval"
display_name = "HumanEval (fork)"
upstream_kind = "huggingface"
upstream_locator = "myorg/humaneval-fork"
license_spdx = "Apache-2.0"
license_url = "https://example.org/LICENSE"
"""
    cfg = load_benchmarks_config(_write(tmp_path, body))
    assert cfg is not None
    assert isinstance(cfg.local[0], LocalBenchmarkEntry)
    assert cfg.local[0].id == "team-evals"
    assert isinstance(cfg.remap[0], RemapBenchmarkEntry)
    assert cfg.remap[0].inherit == "humaneval"
    assert cfg.remap[0].upstream_kind == "huggingface"
    # Optional fields default to None on RemapBenchmarkEntry
    assert cfg.remap[0].series is None
    assert cfg.remap[0].splits is None


def test_missing_required_field_raises(tmp_path: Path) -> None:
    body = """
schema_version = 1

[[local]]
id = "team-evals"
series = "internal"
license_spdx = "proprietary"
"""
    with pytest.raises(ValidationError) as exc:
        load_benchmarks_config(_write(tmp_path, body))
    assert "display_name" in str(exc.value)


def test_duplicate_id_across_local_and_remap_fails(tmp_path: Path) -> None:
    body = """
schema_version = 1

[[local]]
id = "shared"
display_name = "Local shared"
series = "x"
license_spdx = "MIT"

[[remap]]
id = "shared"
inherit = "humaneval"
display_name = "Remap shared"
upstream_kind = "huggingface"
upstream_locator = "x/y"
license_spdx = "MIT"
license_url = "https://example.org"
"""
    with pytest.raises(ValidationError) as exc:
        load_benchmarks_config(_write(tmp_path, body))
    assert "duplicate benchmark id" in str(exc.value)


@pytest.mark.parametrize(
    "bad_id",
    [
        "Team_Evals",   # underscore + capitals
        "team evals",   # space
        "-foo",         # leading dash
        "foo!",         # punctuation
        "",             # empty
    ],
)
def test_invalid_kebab_id_fails(tmp_path: Path, bad_id: str) -> None:
    body = f"""
schema_version = 1

[[local]]
id = "{bad_id}"
display_name = "x"
series = "x"
license_spdx = "MIT"
"""
    with pytest.raises(ValidationError):
        load_benchmarks_config(_write(tmp_path, body))


def test_extra_unknown_field_fails(tmp_path: Path) -> None:
    body = """
schema_version = 1

[[local]]
id = "team-evals"
display_name = "x"
series = "x"
license_spdx = "MIT"
unknown_field = "boom"
"""
    with pytest.raises(ValidationError) as exc:
        load_benchmarks_config(_write(tmp_path, body))
    assert "unknown_field" in str(exc.value) or "Extra" in str(exc.value)


def test_bad_schema_version_fails(tmp_path: Path) -> None:
    body = """
schema_version = 99
"""
    with pytest.raises(ValidationError):
        load_benchmarks_config(_write(tmp_path, body))


def test_top_level_extra_field_fails(tmp_path: Path) -> None:
    body = """
schema_version = 1
unexpected = true
"""
    with pytest.raises(ValidationError):
        load_benchmarks_config(_write(tmp_path, body))


def test_remap_invalid_upstream_kind_fails(tmp_path: Path) -> None:
    body = """
schema_version = 1

[[remap]]
id = "x"
inherit = "humaneval"
display_name = "X"
upstream_kind = "s3"
upstream_locator = "bucket/prefix"
license_spdx = "MIT"
license_url = "https://example.org"
"""
    with pytest.raises(ValidationError):
        load_benchmarks_config(_write(tmp_path, body))


def test_remap_optional_splits_accepted(tmp_path: Path) -> None:
    body = """
schema_version = 1

[[remap]]
id = "x"
inherit = "humaneval"
display_name = "X"
upstream_kind = "huggingface"
upstream_locator = "x/y"
license_spdx = "MIT"
license_url = "https://example.org"
series = "code"
splits = ["test", "validation"]
"""
    cfg = load_benchmarks_config(_write(tmp_path, body))
    assert cfg is not None
    assert cfg.remap[0].series == "code"
    assert cfg.remap[0].splits == ["test", "validation"]


def test_default_empty_config_is_valid() -> None:
    cfg = BenchmarksConfig()
    assert cfg.schema_version == 1
    assert cfg.local == []
    assert cfg.remap == []
