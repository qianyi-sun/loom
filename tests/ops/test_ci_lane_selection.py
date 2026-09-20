"""Small PRs avoid unrelated runners; shared inputs retain full validation."""
import json

import pytest
from scripts.plan_ci_validations import plan_validations


def plan(path, labels=()):
    return plan_validations(changed_paths=[path], labels=set(labels), event_name="pull_request")


def test_frontend_change_does_not_start_backend_baseline():
    outputs = plan("web/src/App.tsx").github_outputs()
    for lane in ("tests_root", "tests_packages", "go_checks", "runtime_payload", "nebius_iac", "locked_environments"):
        assert outputs[lane] == "false"
    assert outputs["web_checks"] == "true"


def test_unrelated_label_preserves_test_selection():
    p = "tests/ops/test_ci_hosted_execution.py"
    assert json.loads(plan(p, ["bug"]).github_outputs()["test_changes"]) == [p]


@pytest.mark.parametrize("path", ["uv.lock", "config/component-ownership.toml", "unknown/runtime.bin"])
def test_shared_or_unknown_change_retains_baseline(path):
    outputs = plan(path).github_outputs()
    for lane in ("tests_root", "tests_packages", "go_checks", "runtime_payload", "nebius_iac", "locked_environments"):
        assert outputs[lane] == "true"


def test_explicit_coverage_restores_python_lanes_for_frontend_change():
    outputs = plan("web/src/App.tsx", ["ci:coverage-summary"]).github_outputs()
    assert outputs["tests_root"] == outputs["tests_packages"] == "true"


def test_owned_go_flow_test_does_not_select_docker_or_generic_integration():
    outputs = plan("tests/integration/test_task_image_builder_guard_local_flow.py").github_outputs()
    assert outputs["go_checks"] == "true"
    assert outputs["integration_docker"] == outputs["integration"] == "false"


@pytest.mark.parametrize("selected,result,accepted", [
    ("false", "skipped", True), ("true", "success", True),
    ("true", "skipped", False), ("true", "failure", False),
    ("false", "cancelled", False), ("invalid", "success", False),
])
def test_baseline_aggregator_checks_selected_results(selected, result, accepted):
    import re
    import subprocess
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    step = next(s for s in workflow["jobs"]["fast-checks"]["steps"]
                if s.get("name") == "Validate parallel check results")
    def value(match):
        key = match[1].strip()
        if key.endswith("docs_only"):
            return "false"
        if key == "needs.tests-root.result":
            return result
        if key == "needs.workflow-plan.outputs.tests_root":
            return selected
        return "success" if key.endswith(".result") else "true"
    script = re.sub(r"\$\{\{(.*?)\}\}", value, step["run"])
    run = subprocess.run(["bash", "-c", script], capture_output=True, text=True)
    assert (run.returncode == 0) is accepted, run.stderr
