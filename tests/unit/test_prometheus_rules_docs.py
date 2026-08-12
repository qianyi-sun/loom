from __future__ import annotations

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _prometheus_alert_names() -> set[str]:
    rules = (ROOT / "deploy/k8s/prometheus-rules.yaml").read_text()
    return set(re.findall(r"^\s*- alert: ([A-Za-z0-9_]+)\s*$", rules, re.MULTILINE))


def test_operator_runbook_documents_every_prometheus_alert() -> None:
    alerts = _prometheus_alert_names()
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text()

    missing = sorted(alert for alert in alerts if f"`{alert}`" not in runbook)

    assert not missing, (
        "docs/runbooks/operator-runbook.md production-alerts table must document every "
        f"alert in deploy/k8s/prometheus-rules.yaml; missing: {missing}"
    )


def test_operator_runbook_does_not_claim_gateway_service_worker_alerts_are_deferred() -> None:
    runbook = (ROOT / "docs/runbooks/operator-runbook.md").read_text()

    assert "Gateway / service / worker instrumentation is a follow-up slice" not in runbook


def test_pipeline_alert_contract_is_exact() -> None:
    document = yaml.safe_load((ROOT / "deploy/k8s/prometheus-rules.yaml").read_text())
    rules = {rule["alert"]: rule for group in document["spec"]["groups"] for rule in group["rules"]}
    expected = {
        "LoomPipelineStageQueueStuck": (
            '(max by (state,resource_class) (loom_pipeline_stage_queue_age_seconds{state=~"ready|queued|retry_wait"}) > 900) or (max by (state,resource_class) (loom_pipeline_stage_queue_age_seconds{state="claimed"}) > 300)',
            "10m",
            "warning",
            "control-plane",
            "#pipeline-stage-queue-stuck",
        ),
        "LoomPipelineStageDeadlineOverrun": (
            "max by (resource_class) (loom_pipeline_stage_deadline_overrun_seconds) > 0",
            "5m",
            "critical",
            "worker",
            "#pipeline-stage-deadline-overrun",
        ),
        "LoomPipelineControllerReconcileErrors": (
            "sum(increase(loom_pipeline_controller_reconcile_errors_total[5m])) >= 3",
            "5m",
            "warning",
            "control-plane",
            "#pipeline-controller-reconcile-errors",
        ),
        "LoomPipelineForcedCancellation": (
            'sum(increase(loom_pipeline_cancel_latency_seconds_count{outcome="forced"}[15m])) > 0',
            "0m",
            "warning",
            "worker",
            "#pipeline-forced-cancellation",
        ),
        "LoomPipelineCheckpointStale": (
            "max by (resource_class) (loom_pipeline_checkpoint_oldest_age_seconds) > 300",
            "10m",
            "warning",
            "worker",
            "#pipeline-checkpoint-stale",
        ),
        "LoomPipelineArtifactCommitFailures": (
            "sum by (commit_kind) (increase(loom_pipeline_artifact_commit_failures_total[10m])) > 0",
            "5m",
            "warning",
            "control-plane",
            "#pipeline-artifact-commit-failures",
        ),
        "LoomPipelineGpuAllocatedIdle": (
            'max by (slurm_cluster,reason) (loom_pipeline_gpu_allocated_idle_seconds{reason=~"process_absent|cleanup_pending"}) > 300',
            "10m",
            "critical",
            "worker",
            "#pipeline-gpu-allocated-idle",
        ),
    }
    for name, (expr, duration, severity, component, anchor) in expected.items():
        rule = rules[name]
        assert rule["expr"] == expr
        assert rule["for"] == duration
        assert rule["labels"] == {"severity": severity, "component": component}
        assert rule["annotations"]["runbook_url"].endswith(anchor)
