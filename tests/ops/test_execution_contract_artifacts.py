from __future__ import annotations

import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
REPORT = ROOT / "docs" / "evidence" / "service-workload-compatibility-v1.json"


def test_execution_contract_artifacts_are_current() -> None:
    subprocess.run(
        ["python", "scripts/ops/generate_execution_contract_artifacts.py", "--check"],
        cwd=ROOT,
        check=True,
    )


def test_compatibility_report_is_complete_and_has_named_owners() -> None:
    report = json.loads(REPORT.read_text(encoding="utf-8"))
    rows = report["workloads"]
    assert report["schema_version"] == "loom.service-workload-compatibility-report.v1"
    assert report["logical_pool_id"] == "nebius-cpu"
    assert report["summary"]["total"] == len(rows) == 69
    assert report["summary"]["supported"] == 0
    assert report["summary"]["conversion_required"] == 66
    assert sum(
        report["summary"][key]
        for key in (
            "supported",
            "conversion_required",
            "unsupported",
        )
    ) == len(rows)
    assert len({row["workload_id"] for row in rows}) == len(rows)
    assert all(row["owner"] and row["reason"] and row["required_changes"] for row in rows)
    assert {row["workload_id"] for row in rows if row["disposition"] == "unsupported"} == {
        "osworld",
        "pipeline:behavior-sim-local-gateway@1",
        "pipeline:behavior-sim-local-none@1",
    }
