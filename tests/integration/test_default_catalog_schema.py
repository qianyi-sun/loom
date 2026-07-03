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
    # Registry holds non-builtin entries only. Terminal-Bench-2 now ships as
    # an installed sibling package and must not be duplicated here.
    assert data["entries"] == []


def test_every_entry_has_required_fields() -> None:
    data = json.loads(REG_PATH.read_text())
    for entry in data["entries"]:
        missing = REQUIRED_FIELDS - entry.keys()
        assert not missing, f"{entry.get('slug')!r} missing {missing}"


def test_terminal_bench_2_not_duplicated_in_default_catalog() -> None:
    data = json.loads(REG_PATH.read_text())
    assert all(entry["slug"] != "terminal-bench-2" for entry in data["entries"])


def test_cli_list_includes_terminal_bench_2_from_default_catalog(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str],
) -> None:
    """End-to-end: with no remote service + default catalog, `list`
    surfaces both installed builtins and the catalog entry for TB-2.

    Note: TB-2 now ships as a sibling package (`packages/
    loom-benchmark-terminal-bench-2`) and registers via the
    `loom.benchmarks` entry-point, so when installed it shows up as
    `builtin` + `installed`. The catalog entry is masked by the
    builtin per `default-catalog.json`'s `_note`. The "available"
    column appears in the table header even when no entry is in
    that state.
    """
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
