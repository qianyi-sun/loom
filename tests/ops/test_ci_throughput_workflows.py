from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


def _workflow_on(workflow: dict[str, Any]) -> dict[str, Any]:
    # PyYAML treats unquoted GitHub Actions key `on` as YAML 1.1 bool.
    return workflow.get("on", workflow.get(True))


def test_images_pr_builds_are_label_gated() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    on_config = _workflow_on(workflow)

    assert "labeled" in on_config["pull_request"]["types"]
    assert "ci:images" in workflow["jobs"]["plan"]["if"]
    assert "ci:images" in workflow["jobs"]["build"]["if"]


def test_images_workflow_uses_path_aware_matrix_plan() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert "plan" in jobs
    assert "images" in jobs["plan"]["outputs"]
    build = jobs["build"]
    assert build["needs"] == "plan"
    assert build["strategy"]["matrix"]["include"] == "${{ fromJSON(needs.plan.outputs.images) }}"
    plan_script = "\n".join(
        step.get("run", "") for step in jobs["plan"]["steps"] if "run" in step
    )
    assert "web/index.html" in plan_script
    assert "deploy/Dockerfile.worker" in plan_script
    assert "migrations/" in plan_script


def test_repository_checks_context_is_parallel_aggregator() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]

    assert jobs["repository-checks"]["name"] == "repository-checks"
    assert set(jobs["repository-checks"]["needs"]) == {
        "workflow-plan",
        "lint-and-static",
        "tests-root",
        "tests-packages",
    }
    assert jobs["lint-and-static"]["needs"] == "workflow-plan"
    assert jobs["tests-root"]["needs"] == "workflow-plan"
    assert jobs["tests-packages"]["needs"] == "workflow-plan"
    assert jobs["integration"]["needs"] == "repository-checks"


def test_ci_supports_merge_queue_merge_group_event() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    on_config = _workflow_on(workflow)

    assert "merge_group" in on_config
    assert "checks_requested" in on_config["merge_group"]["types"]
