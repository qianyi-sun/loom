from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs" / "evidence" / "service-workload-compatibility-v2.json"


def test_execution_contract_artifacts_are_current() -> None:
    subprocess.run(
        ["python", "scripts/ops/generate_execution_contract_artifacts.py", "--check"],
        cwd=ROOT,
        check=True,
    )


def test_compatibility_report_is_complete_and_has_named_owners() -> None:
    report = json.loads(REPORT.read_text(encoding="utf-8"))
    rows = report["workloads"]
    assert report["schema_version"] == "loom.service-workload-compatibility-report.v2"
    assert report["accepted_pool_ids"] == ["gb10", "nebius-cpu", "oldlab"]
    assert report["workload_policy_pool_id"] == "nebius-cpu"
    assert report["summary"]["total_workloads"] == len(rows) == 69
    assert report["summary"]["pools"]["nebius-cpu"]["supported"] == 0
    assert report["summary"]["pools"]["nebius-cpu"]["conversion_required"] == 66
    assert report["summary"]["pools"]["nebius-cpu"]["unsupported"] == 3
    for pool_id in ("oldlab", "gb10"):
        assert report["summary"]["pools"][pool_id]["runtime_admission_required"] == 69
    assert len({row["workload_id"] for row in rows}) == len(rows)
    assert all(
        {decision["logical_pool_id"] for decision in row["pool_dispositions"]}
        == {"nebius-cpu", "oldlab", "gb10"}
        for row in rows
    )
    assert all(
        decision["owner"] and decision["reason"] and decision["required_actions"]
        for row in rows
        for decision in row["pool_dispositions"]
    )
    assert {
        row["workload_id"]
        for row in rows
        if next(
            decision
            for decision in row["pool_dispositions"]
            if decision["logical_pool_id"] == "nebius-cpu"
        )["disposition"]
        == "unsupported"
    } == {
        "osworld",
        "pipeline:behavior-sim-local-gateway@1",
        "pipeline:behavior-sim-local-none@1",
    }
