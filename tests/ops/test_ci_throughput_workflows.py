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

    assert jobs["fast-checks"]["name"] == "fast-checks"
    assert set(jobs["fast-checks"]["needs"]) == {
        "workflow-plan",
        "lint-and-static",
        "tests-root",
        "tests-packages",
    }
    assert jobs["integration"]["needs"] == "fast-checks"
    assert jobs["integration-docker"]["needs"] == "fast-checks"
    assert jobs["repository-checks"]["name"] == "repository-checks"
    assert set(jobs["repository-checks"]["needs"]) == {
        "workflow-plan",
        "fast-checks",
        "go-checks",
        "integration",
        "integration-docker",
        "coverage-summary",
    }
    assert "always()" in jobs["repository-checks"]["if"]
    assert jobs["lint-and-static"]["needs"] == "workflow-plan"
    assert jobs["tests-root"]["needs"] == "workflow-plan"
    assert jobs["tests-packages"]["needs"] == "workflow-plan"

    assert {
        "docs_only",
        "integration",
        "integration_docker",
        "images",
        "cluster_smoke",
        "staging_smoke",
        "coverage_summary",
    } <= set(jobs["workflow-plan"]["outputs"])

    aggregate_step = next(
        step
        for step in jobs["repository-checks"]["steps"]
        if step.get("name") == "Enforce selected validation results"
    )
    assert aggregate_step["env"] == {
        "PLAN_RESULT": "${{ needs.workflow-plan.result }}",
        "FAST_RESULT": "${{ needs.fast-checks.result }}",
        "GO_RESULT": "${{ needs.go-checks.result }}",
        "INTEGRATION_SELECTED": "${{ needs.workflow-plan.outputs.integration }}",
        "INTEGRATION_RESULT": "${{ needs.integration.result }}",
        "DOCKER_SELECTED": "${{ needs.workflow-plan.outputs.integration_docker }}",
        "DOCKER_RESULT": "${{ needs.integration-docker.result }}",
        "COVERAGE_SELECTED": "${{ needs.workflow-plan.outputs.coverage_summary }}",
        "COVERAGE_RESULT": "${{ needs.coverage-summary.result }}",
    }
    aggregate_script = aggregate_step["run"]
    for result_name in (
        "PLAN_RESULT",
        "FAST_RESULT",
        "GO_RESULT",
        "INTEGRATION_RESULT",
        "DOCKER_RESULT",
        "COVERAGE_RESULT",
    ):
        assert f'"${result_name}"' in aggregate_script


def test_ci_supports_merge_queue_merge_group_event() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    on_config = _workflow_on(workflow)

    assert "merge_group" in on_config
    assert "checks_requested" in on_config["merge_group"]["types"]


def test_opt_in_pr_smokes_cancel_superseded_pr_runs() -> None:
    for workflow_path in (
        ".github/workflows/cluster-smoke.yml",
        ".github/workflows/cluster-deploy-spikes.yml",
        ".github/workflows/staging-smoke.yml",
    ):
        workflow = _workflow(workflow_path)

        assert workflow["concurrency"]["cancel-in-progress"] == (
            "${{ github.event_name == 'pull_request' }}"
        )
        assert "github.event.pull_request.number || github.ref" in workflow[
            "concurrency"
        ]["group"]


def test_real_aws_s3_storage_smoke_skips_without_environment_secrets() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    job = workflow["jobs"]["smoke-storage-aws-s3"]
    steps = job["steps"]

    guard = steps[0]
    assert guard["name"] == "Check AWS S3 smoke inputs"
    assert guard["id"] == "aws_s3_inputs"

    guard_script = guard["run"]
    for required_env in (
        "LOOM_SVC_MINIO_ACCESS_KEY",
        "LOOM_SVC_MINIO_SECRET_KEY",
        "LOOM_SVC_MINIO_REGION",
        "LOOM_CI_S3_BUCKET",
    ):
        assert required_env in guard_script
    assert "Real AWS S3 storage smoke skipped; missing required env vars:" in guard_script
    assert 'echo "enabled=false" >> "$GITHUB_OUTPUT"' in guard_script
    assert 'echo "enabled=true" >> "$GITHUB_OUTPUT"' in guard_script

    for step in steps[1:]:
        assert "steps.aws_s3_inputs.outputs.enabled == 'true'" in step["if"]

    cleanup = next(
        step for step in steps if step.get("name") == "Reset bucket lifecycle on exit (always)"
    )
    assert cleanup["if"] == "always() && steps.aws_s3_inputs.outputs.enabled == 'true'"


def test_repository_checks_writes_default_fast_coverage_summary() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]
    coverage_step = next(
        step
        for step in jobs["fast-checks"]["steps"]
        if step.get("name") == "Coverage gate + summary (fast tier)"
    )

    assert "GITHUB_STEP_SUMMARY" in coverage_step["run"]
    assert "coverage report --fail-under=70" in coverage_step["run"]


def test_combined_coverage_summary_is_opt_in() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    coverage_summary_if = workflow["jobs"]["coverage-summary"]["if"]

    assert "needs.workflow-plan.outputs.coverage_summary == 'true'" in coverage_summary_if


def test_repository_checks_uses_lightweight_coverage_tooling() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    fast_steps = workflow["jobs"]["fast-checks"]["steps"]
    step_names = {step.get("name") for step in fast_steps}
    run_blocks = "\n".join(step.get("run", "") for step in fast_steps)

    assert "Install uv" not in step_names
    assert "Set up Python 3.11" not in step_names
    assert "python3 -m coverage combine" in run_blocks
    assert "python3 -m coverage xml" in run_blocks


def test_lint_and_static_caches_mypy() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    steps = workflow["jobs"]["lint-and-static"]["steps"]
    step_names = [step.get("name") for step in steps]
    cache_step = steps[step_names.index("Cache mypy")]

    assert step_names.index("Cache mypy") < step_names.index("Mypy (strict)")
    assert cache_step["uses"] == "actions/cache@v4"
    assert cache_step["with"]["path"] == ".mypy_cache"
