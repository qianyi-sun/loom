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
            upstream_kind="huggingface",
        ),
        DatasetEntry(
            slug="terminal-bench-2", source="catalog",
            display_name="Terminal-Bench 2.0", license_spdx="Apache-2.0",
            license_url="", task_count=None, status="available",
            available_pip_spec="loom-benchmark-terminal-bench-2",
            entry_point=None,
            upstream_kind=None,
        ),
        DatasetEntry(
            slug="team-evals", source="remote",
            display_name="Team Evals", license_spdx="proprietary",
            license_url="", task_count=12, status="remote-only",
            available_pip_spec=None, entry_point=None,
            upstream_kind="local-folder",
        ),
    ]


def test_table_renders_header_and_rows() -> None:
    out = render_datasets_table(_entries())
    lines = out.splitlines()
    assert lines[0].startswith("SLUG")
    assert "SOURCE" in lines[0]
    assert "UPSTREAM" in lines[0]
    assert "LICENSE" in lines[0]
    assert "TASKS" in lines[0]
    assert "STATUS" in lines[0]
    assert any(
        "humaneval" in line and "huggingface" in line
        and "164" in line and "installed" in line
        for line in lines[1:]
    )
    # Catalog entries have no upstream info → rendered as "-"
    assert any(
        "terminal-bench-2" in line and " - " in line and "available" in line
        for line in lines[1:]
    )
    # #234 local-folder benchmarks surface as "local-folder" UPSTREAM
    assert any(
        "team-evals" in line and "local-folder" in line
        and "remote-only" in line
        for line in lines[1:]
    )


def test_table_handles_empty_entries() -> None:
    out = render_datasets_table([])
    assert out.startswith("SLUG")
    assert "UPSTREAM" in out
    assert out.splitlines()[1:] == []


def test_json_is_a_dict_with_items() -> None:
    raw = render_datasets_json(_entries())
    parsed = json.loads(raw)
    assert parsed["count"] == 3
    slugs = {item["slug"] for item in parsed["items"]}
    assert slugs == {"humaneval", "terminal-bench-2", "team-evals"}
    # New field is in the JSON output
    by_slug = {item["slug"]: item for item in parsed["items"]}
    assert by_slug["humaneval"]["upstream_kind"] == "huggingface"
    assert by_slug["terminal-bench-2"]["upstream_kind"] is None
    assert by_slug["team-evals"]["upstream_kind"] == "local-folder"
