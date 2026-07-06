from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_PATH = REPO_ROOT / ".github/workflows/publish-benchmarks.yml"


def _workflow() -> dict[str, Any]:
    return yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))


def _publish_job() -> dict[str, Any]:
    return _workflow()["jobs"]["publish"]


def _publish_step() -> dict[str, Any]:
    return next(
        step for step in _publish_job()["steps"]
        if step.get("name") == "Publish ${{ matrix.benchmark }}"
    )


def test_publish_step_failure_propagates_to_matrix_job() -> None:
    step = _publish_step()
    run_script = step["run"]

    assert step.get("continue-on-error") in (None, False)
    assert "set -euo pipefail" in run_script
    assert "exit \"$rc\"" in run_script


def test_publish_step_records_non_secret_success_and_failure_summary() -> None:
    run_script = _publish_step()["run"]

    assert "$GITHUB_STEP_SUMMARY" in run_script
    assert "Status: success" in run_script
    assert "Status: failed" in run_script
    assert "PRHW-authorized write token" in run_script


def test_publish_gate_checks_token_presence_without_printing_secret() -> None:
    workflow_text = WORKFLOW_PATH.read_text(encoding="utf-8")
    step_runs = "\n".join(
        str(step.get("run", "")) for step in _publish_job()["steps"]
    )

    assert "--hf-token" not in workflow_text
    assert "::add-mask::$HF_TOKEN" in step_runs
    assert "HF_TOKEN: configured (secret value not printed)" in step_runs
    assert "HF_TOKEN: missing (secret value not printed)" in step_runs
    assert "Missing HF_TOKEN" in step_runs

    forbidden_token_prints = (
        "echo \"$HF_TOKEN\"",
        "echo ${HF_TOKEN}",
        "printf '%s' \"$HF_TOKEN\"",
        "printf \"%s\" \"$HF_TOKEN\"",
        "set -x",
    )
    for forbidden in forbidden_token_prints:
        assert forbidden not in step_runs
