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


def test_scheduled_regression_requests_every_ci_test_lane_and_coverage():
    p = plan_validations(changed_paths=["docs/example.md"], labels=set(), event_name="schedule")
    outputs = p.github_outputs()
    for lane in ("tests_root", "tests_packages", "go_checks", "integration", "integration_docker", "coverage_summary", "web_checks"):
        assert outputs[lane] == "true"
    assert outputs["docs_only"] == "false"
    assert outputs["test_changes"] == "[]"


@pytest.mark.parametrize("changes,retained", [
    (("src/loom_service/api/routes/batches.py",), False),
    (("src/loom_llm_gateway/config.py",), False),
    (("migrations/versions/new.py",), True),
    (("src/loom/db/schema.py",), True),
    (("src/loom_cli/rollout/readonly_database_bootstrap.py",), True),
    (("unknown/input.bin",), True),
    (("src/loom_service/app.py", "uv.lock"), True),
    (("tests/integration/test_application_schema_reference.py",), True),
    ((), True),
])
def test_schema_component_scope_retains_database_and_unknown_inputs(changes, retained):
    from pathlib import Path

    from scripts.component_ownership import load_manifest, select_affected_test_suites

    root = Path(__file__).resolve().parents[2]
    manifest = load_manifest(root / "config/component-ownership.toml")
    heavy = "tests/integration/test_application_schema_reference.py"
    regular = "tests/integration/test_application_runtime_login.py"
    selected = select_affected_test_suites(manifest, (heavy, regular), changed_paths=changes)
    assert (heavy in selected) is retained
    assert regular in selected


def test_scheduled_workflow_shell_emits_full_plan(tmp_path):
    import os
    import subprocess
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    workflow = yaml.safe_load((root / ".github/workflows/ci.yml").read_text())
    step = next(s for s in workflow["jobs"]["workflow-plan"]["steps"] if s.get("id") == "plan")
    output = tmp_path / "output"
    env = {**os.environ, **dict.fromkeys(step["env"], ""),
           "EVENT_NAME": "schedule", "HEAD_SHA": "HEAD", "PR_DRAFT": "false",
           "PR_BASE_CHANGED": "false", "GITHUB_OUTPUT": str(output)}
    run = subprocess.run(["bash", "-c", step["run"].replace("/tmp/loom-changed-files.txt", str(tmp_path / "changes"))],
                         cwd=root, env=env, capture_output=True, text=True)
    assert run.returncode == 0, run.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["coverage_summary"] == values["integration_docker"] == values["web_checks"] == "true"
    assert values["test_changes"] == "[]"


def test_cli_component_selection_preserves_full_and_affected_modes():
    import subprocess
    import sys
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    heavy = "tests/integration/test_application_schema_reference.py"
    for changes, expected in [([], True), (["migrations/versions/new.py"], True),
                              (["src/loom_service/api/routes/batches.py"], False)]:
        run = subprocess.run([sys.executable, "scripts/component_ownership.py", "test-paths", "--lane", "integration",
                              "--changed-paths-json", json.dumps(changes)], cwd=root, capture_output=True, text=True)
        assert run.returncode == 0, run.stderr
        assert (heavy in run.stdout.splitlines()) is expected
        assert "tests/integration/test_application_runtime_login.py" in run.stdout.splitlines()
