"""The shipped default-catalog.json must parse + satisfy the
DatasetEntry-compatible shape the loader expects."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src" / "loom_cli" / "catalog_data" / "default-catalog.json"
)

REQUIRED_FIELDS = {
    "slug", "display_name", "license_spdx", "license_url",
    "task_count", "available",
}


def test_catalog_file_exists() -> None:
    assert REG_PATH.is_file(), REG_PATH


def test_catalog_is_a_versioned_object_with_entries_list() -> None:
    data = json.loads(REG_PATH.read_text())
    assert data["catalog_version"] == 1
    assert isinstance(data["entries"], list)
    # Registry holds non-builtin entries only — currently just terminal-
    # bench-2 ahead of Plan 25. Bump the floor as the slate grows.
    assert len(data["entries"]) >= 1


def test_every_entry_has_required_fields() -> None:
    data = json.loads(REG_PATH.read_text())
    for entry in data["entries"]:
        missing = REQUIRED_FIELDS - entry.keys()
        assert not missing, f"{entry.get('slug')!r} missing {missing}"


def test_terminal_bench_2_entry_present_with_pip_spec() -> None:
    data = json.loads(REG_PATH.read_text())
    tb2 = next(e for e in data["entries"] if e["slug"] == "terminal-bench-2")
    assert tb2["available"] == "loom-benchmark-terminal-bench-2"
    assert tb2["license_spdx"] == "Apache-2.0"


def test_cli_list_includes_terminal_bench_2_from_default_catalog(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end: with no remote service + default catalog, `list`
    surfaces both installed builtins and the catalog-only TB-2 entry."""
    monkeypatch.delenv("LOOM_SERVER_URL", raising=False)
    monkeypatch.delenv("LOOM_CATALOG_URL", raising=False)

    from loom_cli.datasets_cmd import dispatch

    rc = dispatch(["list"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "humaneval" in out
    assert "builtin" in out
    assert "installed" in out
    assert "terminal-bench-2" in out
    assert "available" in out
