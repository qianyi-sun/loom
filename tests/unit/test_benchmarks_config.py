"""Loader unit tests for `config/benchmarks.toml` (issue #234)."""
from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from loom.config.benchmarks import (
    LOOM_BENCHMARKS_CONFIG_PATH,
    BenchmarksConfig,
    LocalBenchmarkEntry,
    RemapBenchmarkEntry,
    load_benchmarks_config,
    resolve_config_path,
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


def test_local_source_subdir_loads(tmp_path: Path) -> None:
    body = """
schema_version = 1

[[local]]
id = "team-evals"
display_name = "Internal team evaluations"
series = "internal"
license_spdx = "MIT"
source_subdir = "tasks"
"""
    cfg = load_benchmarks_config(_write(tmp_path, body))
    assert cfg is not None
    assert cfg.local[0].source_subdir == "tasks"


@pytest.mark.parametrize(
    "bad",
    [
        "/abs",
        "../tasks",
        "tasks/../x",
        "tasks//x",
        "tasks/",
        "tasks/./x",
        ".",
        "",
    ],
)
def test_local_source_subdir_rejects_unsafe_paths(
    tmp_path: Path, bad: str,
) -> None:
    body = f"""
schema_version = 1

[[local]]
id = "team-evals"
display_name = "Internal team evaluations"
series = "internal"
license_spdx = "MIT"
source_subdir = "{bad}"
"""
    with pytest.raises(ValidationError):
        load_benchmarks_config(_write(tmp_path, body))


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


# ─── resolve_config_path ─────────────────────────────────────────────


def test_resolve_explicit_existing_returns_path(tmp_path: Path) -> None:
    p = tmp_path / "explicit.toml"
    p.write_text("schema_version = 1\n")
    assert resolve_config_path(p) == p


def test_resolve_explicit_missing_returns_none(tmp_path: Path) -> None:
    """Explicit-but-missing path returns None — the CLI converts this
    to a `--config <path>; nothing to sync` warning."""
    assert resolve_config_path(tmp_path / "nope.toml") is None


def test_resolve_env_override(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    p = tmp_path / "env.toml"
    p.write_text("schema_version = 1\n")
    monkeypatch.setenv(LOOM_BENCHMARKS_CONFIG_PATH, str(p))
    monkeypatch.chdir(tmp_path)  # isolate from any real cwd config
    assert resolve_config_path() == p


def test_resolve_env_missing_returns_none_not_cwd_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """If env var is set but the path doesn't exist, return None — do
    NOT silently fall through to the cwd / /etc lookup. Operator who
    sets the env var explicitly is telling us *this* file or nothing."""
    cwd_cfg = tmp_path / "config" / "benchmarks.toml"
    cwd_cfg.parent.mkdir()
    cwd_cfg.write_text("schema_version = 1\n")
    monkeypatch.setenv(LOOM_BENCHMARKS_CONFIG_PATH, str(tmp_path / "nope.toml"))
    monkeypatch.chdir(tmp_path)
    assert resolve_config_path() is None


def test_resolve_cwd_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    cwd_cfg = tmp_path / "config" / "benchmarks.toml"
    cwd_cfg.parent.mkdir()
    cwd_cfg.write_text("schema_version = 1\n")
    monkeypatch.delenv(LOOM_BENCHMARKS_CONFIG_PATH, raising=False)
    monkeypatch.chdir(tmp_path)
    assert resolve_config_path() == cwd_cfg


def test_resolve_none_when_nothing_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty cwd + no env + no /etc/loom/ → None (auto-sync no-op)."""
    monkeypatch.delenv(LOOM_BENCHMARKS_CONFIG_PATH, raising=False)
    monkeypatch.chdir(tmp_path)  # tmp_path has no config/ subdir
    # We can't easily blank /etc/loom/, but on dev / CI it shouldn't
    # exist. If it does, this test will spuriously pass — accept that.
    assert resolve_config_path() is None
