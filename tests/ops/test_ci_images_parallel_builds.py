from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]


def _condition(expression: str, values: dict[str, str], *, cancelled: bool = False) -> bool:
    """Evaluate the Boolean-only job guards, including GitHub's implicit success guard."""
    status_function = "cancelled()" in expression or "always()" in expression
    if not status_function and any(
        value != "success" for key, value in values.items() if key.endswith(".result")
    ):
        return False
    expression = re.sub(
        r"(?:github|needs)\.[A-Za-z0-9_.-]+", lambda match: repr(values[match[0]]), expression
    )
    expression = expression.replace("cancelled()", str(cancelled)).replace("always()", "True")
    expression = expression.replace("&&", " and ").replace("||", " or ")
    expression = re.sub(r"!(?!=)", " not ", expression).strip()
    return bool(eval(f"({expression})", {"__builtins__": {}, "contains": lambda a, b: b in a}))


def _jobs() -> dict[str, Any]:
    return yaml.safe_load((ROOT / ".github/workflows/images.yml").read_text())["jobs"]


def test_native_builds_use_the_complete_supported_matrix() -> None:
    jobs = _jobs()
    assert jobs["build"]["needs"] == ["plan", "trivy-binary"]
    assert jobs["build"]["strategy"]["matrix"]["include"] == "${{ fromJSON(needs.plan.outputs.ordinary_builds) }}"


@pytest.mark.parametrize("event", ["pull_request", "merge_group", "push", "workflow_dispatch"])
def test_docs_plan_emits_empty_native_matrix(tmp_path: Path, event: str) -> None:
    """Exercise the real checked-out image planner when it selects no image work."""
    steps = _jobs()["plan"]["steps"]
    select = next(step for step in steps if step.get("id") == "plan")
    changed = tmp_path / "changed"
    changed.write_text("docs/architecture/overview.md\n")
    output = tmp_path / "output"
    env = {
        **os.environ,
        "EVENT_NAME": event,
        "REQUIRED": "false",
        "UNOWNED_RUNTIME": "false",
        "TRUSTED_PUBLISH": "true" if event == "workflow_dispatch" else "false",
        "CHANGED_FILES": str(changed),
        "GITHUB_OUTPUT": str(output),
    }
    planned = subprocess.run(
        ["bash"],
        input=select["run"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    assert planned.returncode == 0, planned.stderr
    values = dict(line.split("=", 1) for line in output.read_text().splitlines())
    assert values["required"] == "false"
    assert values["images"] == values["native_builds"] == "[]"


@pytest.mark.parametrize(
    "images",
    [[], ["service"], ["web"], ["service", "web"]],
)
@pytest.mark.parametrize("event", ["pull_request", "merge_group", "workflow_dispatch", "push"])
@pytest.mark.parametrize("fault", [None, "ordinary", "publish"])
def test_gate_enforces_selected_build_and_publish_results(
    images: list[str], event: str, fault: str | None
) -> None:
    ordinary_selected = bool(images)
    publish = event == "push"
    ordinary = "success" if ordinary_selected and not publish else "skipped"
    publish_result = "success" if images and publish else "skipped"
    env = {
        **os.environ,
        "EVENT_NAME": event,
        "TRUSTED_PUBLISH": "false",
        "PLAN_RESULT": "success",
        "GATE_MODE": "full",
        "REQUIRED": "true" if images else "false",
        "STANDARD_IMAGES": json.dumps([{"image": image} for image in images]),
        "BUILD_RESULT": "failure" if fault == "ordinary" else ordinary,
        "HARBOR_REQUIRED": "true" if ordinary_selected else "false",
        "HARNESS_BUILD_RESULT": "failure" if fault == "ordinary" else ordinary,
        "PUBLISH_RESULT": "failure" if fault == "publish" else publish_result,
        "MANIFEST_RESULT": publish_result,
    }
    step = _jobs()["images-gate"]["steps"][0]
    result = subprocess.run(
        ["bash"], input=step["run"], text=True, capture_output=True, env=env, check=False
    )
    assert (result.returncode == 0) == (fault is None), result.stderr


@pytest.mark.parametrize("job", ["build"])
def test_parallel_builds_keep_native_scan_and_untrusted_permissions(job: str) -> None:
    jobs = _jobs()
    build = jobs[job]
    assert build["permissions"] == {"contents": "read"}
    assert build["strategy"]["fail-fast"] is False
    assert "github.event_name != 'push'" in build["if"]
    assert "needs.plan.outputs.trusted_publish != 'true'" in build["if"]
    assert build["steps"] == jobs["build"]["steps"]
    scripts = "\n".join(step.get("run", "") for step in build["steps"])
    assert "scripts/validate_trivy_release_report.py" in scripts
    assert "--cache-to" not in scripts
    assert "--push" not in scripts


@pytest.mark.parametrize("result_name", ["BUILD_RESULT", "HARNESS_BUILD_RESULT"])
@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped"])
def test_gate_rejects_missing_selected_matrix_after_dependency_failure(
    result_name: str, result: str
) -> None:
    env = {
        **os.environ,
        "EVENT_NAME": "pull_request",
        "TRUSTED_PUBLISH": "false",
        "PLAN_RESULT": "success",
        "GATE_MODE": "full",
        "REQUIRED": "true",
        "STANDARD_IMAGES": '[{"image":"service"},{"image":"web"}]',
        "BUILD_RESULT": "success",
        "HARBOR_REQUIRED": "true",
        "HARNESS_BUILD_RESULT": "success",
        "PUBLISH_RESULT": "skipped",
        "MANIFEST_RESULT": "skipped",
        result_name: result,
    }
    completed = subprocess.run(
        ["bash"],
        input=_jobs()["images-gate"]["steps"][0]["run"],
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )
    assert completed.returncode != 0


@pytest.mark.parametrize("prerequisite", [None, "plan", "trivy-binary"])
def test_trusted_publish_requires_successful_selected_dependencies(
    prerequisite: str | None
) -> None:
    images = ["service"]
    values = {
        "github.event_name": "push",
        "github.ref": "refs/heads/dev",
        "needs.plan.result": "success",
        "needs.trivy-binary.result": "success",
        "needs.plan.outputs.trusted_publish": "false",
        "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true",
        "needs.plan.outputs.images": json.dumps(
            [{"image": image} for image in images], separators=(",", ":")
        ),
    }
    expression = _jobs()["publish"]["if"]
    for result in ("failure", "skipped", "cancelled"):
        if prerequisite:
            values[f"needs.{prerequisite}.result"] = result
        assert _condition(expression, values) == (prerequisite is None)
        assert not _condition(expression, values, cancelled=True)


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("github.event_name", "pull_request"),
        ("github.event_name", "merge_group"),
        ("github.event_name", "workflow_dispatch"),
        ("github.ref", "refs/heads/feature"),
        ("needs.plan.outputs.gate_mode", "filtered"),
        ("needs.plan.outputs.required", "false"),
        ("needs.plan.outputs.images", "[]"),
    ],
)
def test_publish_requires_trusted_event_branch_and_selection(key: str, value: str) -> None:
    values = {
        "github.event_name": "push",
        "github.ref": "refs/heads/dev",
        "needs.plan.result": "success",
        "needs.trivy-binary.result": "success",
        "needs.plan.outputs.trusted_publish": "false",
        "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true",
        "needs.plan.outputs.images": '[{"image":"service"}]',
        key: value,
    }
    assert not _condition(_jobs()["publish"]["if"], values)


@pytest.mark.parametrize("job", ["build"])
@pytest.mark.parametrize("failed_dependency", ["plan", "trivy-binary"])
def test_untrusted_builds_do_not_run_after_required_dependency_failure(
    job: str, failed_dependency: str
) -> None:
    selected = _jobs()[job]
    values = {f"needs.{dependency}.result": "success" for dependency in selected["needs"]}
    values.update(
        {
            "github.event_name": "pull_request",
            "needs.plan.outputs.trusted_publish": "false",
            "needs.plan.outputs.gate_mode": "full",
            "needs.plan.outputs.required": "true",
            "needs.plan.outputs.ordinary_builds": '[{"image":"service"}]',
        }
    )
    dependency = failed_dependency
    assert _condition(selected["if"], values)
    for result in ("failure", "skipped", "cancelled"):
        values[f"needs.{dependency}.result"] = result
        assert not _condition(selected["if"], values)


@pytest.mark.parametrize("publish_result", ["success", "failure", "cancelled", "skipped"])
def test_manifest_requires_publish_success(
    publish_result: str,
) -> None:
    values = {
        "github.event_name": "workflow_dispatch",
        "github.ref": "refs/heads/main",
        "needs.plan.result": "success",
        "needs.publish.result": publish_result,
        "needs.plan.outputs.trusted_publish": "true",
        "needs.plan.outputs.gate_mode": "full",
        "needs.plan.outputs.required": "true",
        "needs.plan.outputs.images": '[{"image":"service"}]',
    }
    condition = _jobs()["publish-manifest"]["if"]
    assert _condition(condition, values) == (publish_result == "success")
    assert not _condition(condition, values, cancelled=True)
