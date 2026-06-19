from __future__ import annotations

import json

from loom_cli.benchmark_readiness import (
    BenchmarkAuditSource,
    TaskAuditSource,
    build_readiness_item,
    render_readiness_json,
    render_readiness_table,
)


def _valid_task_config(task_id: str = "fake-bench/task-001") -> dict[str, object]:
    return {
        "schema_version": "1",
        "task": {"id": task_id, "name": "Fake task"},
        "environment": {"os": "linux", "docker_image": "python:3.12-slim"},
        "agent": {"name": "oracle"},
        "verifier": {"name": "pytest"},
        "steps": [{"name": "main"}],
    }


def _benchmark(benchmark_id: str = "fake-bench") -> BenchmarkAuditSource:
    return BenchmarkAuditSource(
        id=benchmark_id,
        display_name="Fake Bench",
        series="fake",
        upstream_kind="huggingface",
        upstream_locator="fake/source",
        upstream_revision="main",
    )


def test_readiness_marks_legacy_manifest_rows_as_blocked() -> None:
    item = build_readiness_item(
        _benchmark("swe-bench-verified"),
        tasks=[
            TaskAuditSource(
                id="swe-bench-verified/task-001",
                config={},
                source="hf://PRHW/loom-benchmark-swe-bench-verified@main/task-001/",
            ),
        ],
        registry_names={"swe-bench-verified"},
    )

    assert item.readiness_state == "blocked"
    assert item.blocker_reason == "manifest_legacy_missing_task_config"
    assert item.raw_task_count == 1
    assert item.valid_task_config_count == 0
    assert item.source_schemes == ["hf"]
    assert item.materializer_status == "available"


def test_readiness_marks_valid_hf_tasks_as_runnable() -> None:
    item = build_readiness_item(
        _benchmark(),
        tasks=[
            TaskAuditSource(
                id="fake-bench/task-001",
                config=_valid_task_config(),
                source="hf://PRHW/loom-benchmark-fake-bench@main/task-001/",
            ),
        ],
        registry_names={"fake-bench"},
    )

    assert item.readiness_state == "runnable"
    assert item.blocker_reason is None
    assert item.raw_task_count == 1
    assert item.valid_task_config_count == 1


def test_readiness_blocks_unknown_source_scheme() -> None:
    item = build_readiness_item(
        _benchmark(),
        tasks=[
            TaskAuditSource(
                id="fake-bench/task-001",
                config=_valid_task_config(),
                source="unknown://benchmark/task-001",
            ),
        ],
        registry_names={"fake-bench"},
    )

    assert item.readiness_state == "blocked"
    assert item.blocker_reason == "materializer_missing"
    assert item.materializer_status == "missing"


def test_readiness_renderers_are_stable() -> None:
    item = build_readiness_item(
        _benchmark(),
        tasks=[
            TaskAuditSource(
                id="fake-bench/task-001",
                config=_valid_task_config(),
                source="hf://PRHW/loom-benchmark-fake-bench@main/task-001/",
            ),
        ],
        registry_names={"fake-bench"},
    )

    payload = json.loads(render_readiness_json([item]))
    assert payload["count"] == 1
    assert payload["items"][0]["id"] == "fake-bench"
    assert "READINESS" in render_readiness_table([item])
    assert "fake-bench" in render_readiness_table([item])
