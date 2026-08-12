from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom.pipeline.public_api import PipelineStageRunSummaryV1
from loom_control_plane import metrics
from loom_worker.metrics import PIPELINE_GPU_ALLOCATED_IDLE_SECONDS

ROOT = Path(__file__).resolve().parents[2]


def test_pipeline_stage_summary_is_closed_to_unknown_enums_and_fields() -> None:
    value = {
        "id": "00000000-0000-4000-8000-000000000001",
        "node_key": "node",
        "shard_key": "singleton",
        "node_kind": "container",
        "topological_level": 0,
        "upstream_node_keys": [],
        "state": "running",
        "domain_outcome": None,
        "reason_code": None,
        "attempt_count": 1,
        "resource_profile_name": "cpu@1",
        "resource_class": "cpu",
        "retry_allowed": False,
        "retry_ineligible_reason": "stage_not_failed",
    }
    PipelineStageRunSummaryV1.model_validate(value)
    with pytest.raises(ValueError):
        PipelineStageRunSummaryV1.model_validate({**value, "state": "future_state"})
    with pytest.raises(ValueError):
        PipelineStageRunSummaryV1.model_validate({**value, "raw_graph": {}})


def test_pipeline_collectors_have_closed_labels_and_exact_buckets() -> None:
    expected = {
        metrics.PIPELINE_RUNS: ("state", "result_status"),
        metrics.PIPELINE_STAGE_RUNS: ("state", "resource_class"),
        metrics.PIPELINE_STAGE_DURATION_SECONDS: ("resource_class", "result"),
        metrics.EXECUTION_ATTEMPTS: ("state", "resource_class"),
        metrics.PIPELINE_GPU_SECONDS_TOTAL: ("slurm_cluster", "gpu_count_class"),
        metrics.PIPELINE_ARTIFACT_BYTES_TOTAL: ("artifact_class",),
        metrics.PIPELINE_CANCEL_LATENCY_SECONDS: ("outcome",),
        metrics.PIPELINE_CONTROLLER_RECONCILE_ERRORS_TOTAL: ("reason",),
        metrics.PIPELINE_STAGE_QUEUE_AGE_SECONDS: ("state", "resource_class"),
        metrics.PIPELINE_STAGE_DEADLINE_OVERRUN_SECONDS: ("resource_class",),
        metrics.PIPELINE_CHECKPOINT_OLDEST_AGE_SECONDS: ("resource_class",),
        metrics.PIPELINE_ARTIFACT_COMMIT_FAILURES_TOTAL: ("commit_kind", "reason"),
        PIPELINE_GPU_ALLOCATED_IDLE_SECONDS: ("slurm_cluster", "reason"),
    }
    forbidden = {
        "pipeline_run_id",
        "pipeline_stage_run_id",
        "execution_attempt_id",
        "node_key",
        "shard_key",
        "team_id",
        "recipe",
        "resource_profile",
    }
    for collector, labels in expected.items():
        assert collector._labelnames == labels
        assert forbidden.isdisjoint(labels)
    assert tuple(metrics.PIPELINE_STAGE_DURATION_SECONDS._upper_bounds[:-1]) == (
        1,
        5,
        15,
        30,
        60,
        120,
        300,
        600,
        1800,
        3600,
        7200,
        14400,
        28800,
        86400,
        172800,
        345600,
        864000,
    )
    assert tuple(metrics.PIPELINE_CANCEL_LATENCY_SECONDS._upper_bounds[:-1]) == (
        1,
        2,
        5,
        10,
        30,
        60,
        120,
        300,
        600,
    )


def test_pipeline_dashboard_contract_and_packaged_copy() -> None:
    canonical = ROOT / "deploy/grafana/dashboards/control-plane.json"
    packaged = ROOT / "src/loom_cli/data/grafana/control-plane.json"
    assert canonical.read_bytes() == packaged.read_bytes()
    dashboard = json.loads(canonical.read_text())
    assert dashboard["uid"] == "loom-control-plane"
    assert dashboard["schemaVersion"] == 39
    assert dashboard["templating"]["list"] == []
    assert dashboard["refresh"] == "30s"
    assert [panel["id"] for panel in dashboard["panels"]] == list(range(1, 20))
    expected_titles = [
        "PipelineRuns by state/result",
        "StageRuns by state/resource",
        "Attempts by state/resource",
        "Stage duration p50/p95",
        "Settled GPU seconds rate",
        "Pipeline Artifact byte rate",
        "Cancellation p95 / forced",
        "Reconcile and commit failures",
        "Stage queue/deadline age",
        "Checkpoint age",
        "Allocated GPU without process",
    ]
    assert [panel["title"] for panel in dashboard["panels"][8:]] == expected_titles
