"""Text table + machine-readable JSON for `loom datasets list`."""

from __future__ import annotations

import json

from loom_cli.discovery import DatasetEntry
from loom_cli.output import render_datasets_json, render_datasets_table


def _entries() -> list[DatasetEntry]:
    return [
        DatasetEntry(
            slug="humaneval", source="builtin",
            display_name="HumanEval", license_spdx="MIT",
            license_url="", task_count=164, status="installed",
            available_pip_spec=None,
            entry_point="loom_benchmarks.adapters.humaneval:HumanEvalAdapter",
        ),
        DatasetEntry(
            slug="terminal-bench-2", source="registry",
            display_name="Terminal-Bench 2.0", license_spdx="Apache-2.0",
            license_url="", task_count=None, status="available",
            available_pip_spec="loom-benchmark-terminal-bench-2",
            entry_point=None,
        ),
    ]


def test_table_renders_header_and_rows() -> None:
    out = render_datasets_table(_entries())
    lines = out.splitlines()
    assert lines[0].startswith("SLUG")
    assert "SOURCE" in lines[0]
    assert "LICENSE" in lines[0]
    assert "TASKS" in lines[0]
    assert "STATUS" in lines[0]
    assert any("humaneval" in line and "164" in line and "installed" in line
               for line in lines[1:])
    assert any("terminal-bench-2" in line and "-" in line and "available" in line
               for line in lines[1:])


def test_table_handles_empty_entries() -> None:
    out = render_datasets_table([])
    assert out.startswith("SLUG")
    assert out.splitlines()[1:] == []


def test_json_is_a_dict_with_items() -> None:
    raw = render_datasets_json(_entries())
    parsed = json.loads(raw)
    assert parsed["count"] == 2
    assert parsed["items"][0]["slug"] in {"humaneval", "terminal-bench-2"}
    assert parsed["items"][0]["source"] in {"builtin", "registry"}
