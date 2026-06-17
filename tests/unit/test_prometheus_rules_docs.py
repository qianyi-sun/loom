from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _prometheus_alert_names() -> set[str]:
    rules = (ROOT / "deploy/k8s/prometheus-rules.yaml").read_text()
    return set(re.findall(r"^\s*- alert: ([A-Za-z0-9_]+)\s*$", rules, re.MULTILINE))


def test_operator_runbook_documents_every_prometheus_alert() -> None:
    alerts = _prometheus_alert_names()
    runbook = (ROOT / "docs/operator-runbook.md").read_text()

    missing = sorted(alert for alert in alerts if f"`{alert}`" not in runbook)

    assert not missing, (
        "docs/operator-runbook.md production-alerts table must document every "
        f"alert in deploy/k8s/prometheus-rules.yaml; missing: {missing}"
    )


def test_operator_runbook_does_not_claim_gateway_service_worker_alerts_are_deferred() -> None:
    runbook = (ROOT / "docs/operator-runbook.md").read_text()

    assert "Gateway / service / worker instrumentation is a follow-up slice" not in runbook
