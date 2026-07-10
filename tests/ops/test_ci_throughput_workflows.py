from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


def _workflow_on(workflow: dict[str, Any]) -> dict[str, Any]:
    # PyYAML treats unquoted GitHub Actions key `on` as YAML 1.1 bool.
    return workflow.get("on", workflow.get(True))


def _image_matrix_step() -> dict[str, Any]:
    workflow = _workflow(".github/workflows/images.yml")
    return next(
        step
        for step in workflow["jobs"]["plan"]["steps"]
        if step.get("name") == "Select affected images"
    )


def _image_matrix_python() -> str:
    script = _image_matrix_step()["run"]
    _, python_and_marker = script.split("python - <<'PY'\n", maxsplit=1)
    python_body, trailing = python_and_marker.rsplit("\nPY", maxsplit=1)
    assert not trailing.strip()
    return python_body


def _git(repo: Path, *args: str) -> str:
    return subprocess.check_output(
        ["git", *args],
        cwd=repo,
        text=True,
    ).strip()


def _run_image_matrix_plan(
    tmp_path: Path,
    *,
    required: str,
) -> tuple[subprocess.CompletedProcess[str], str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    _git(repo, "config", "user.email", "ci@example.invalid")
    _git(repo, "config", "user.name", "CI Test")

    (repo / "seed.txt").write_text("seed\n", encoding="utf-8")
    _git(repo, "add", "seed.txt")
    _git(repo, "commit", "--quiet", "-m", "base")
    base_sha = _git(repo, "rev-parse", "HEAD")

    unowned_path = repo / "unowned-runtime" / "new-input.bin"
    unowned_path.parent.mkdir()
    unowned_path.write_bytes(b"runtime input\n")
    _git(repo, "add", "unowned-runtime/new-input.bin")
    _git(repo, "commit", "--quiet", "-m", "head")
    head_sha = _git(repo, "rev-parse", "HEAD")

    github_output = tmp_path / "github-output.txt"
    result = subprocess.run(
        [sys.executable, "-"],
        input=_image_matrix_python(),
        cwd=repo,
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "EVENT_NAME": "pull_request",
            "BASE_SHA": base_sha,
            "HEAD_SHA": head_sha,
            "PR_LABELS_JSON": "[]",
            "REQUIRED": required,
            "GITHUB_OUTPUT": str(github_output),
        },
        check=False,
    )
    output = github_output.read_text(encoding="utf-8") if github_output.exists() else ""
    return result, output


GATE_CONTRACTS = {
    ".github/workflows/ci.yml": ("repository-checks", "repository-checks"),
    ".github/workflows/images.yml": ("images-gate", "images-gate"),
    ".github/workflows/cluster-smoke.yml": (
        "cluster-smoke-gate",
        "cluster-smoke-gate",
    ),
    ".github/workflows/staging-smoke.yml": (
        "staging-smoke-gate",
        "staging-smoke-gate",
    ),
}


def _gate_script(workflow_path: str, gate_id: str) -> str:
    workflow = _workflow(workflow_path)
    gate_step = next(
        step
        for step in workflow["jobs"][gate_id]["steps"]
        if step.get("name", "").startswith("Enforce selected")
    )
    return gate_step["run"]


def test_images_builds_use_planner_selection() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert "required" in jobs["plan"]["outputs"]
    assert jobs["build"]["needs"] == "plan"
    assert "needs.plan.outputs.required == 'true'" in jobs["build"]["if"]


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


def test_images_matrix_plan_receives_shared_required_decision() -> None:
    assert _image_matrix_step()["env"]["REQUIRED"] == (
        "${{ steps.required.outputs.images }}"
    )


def test_images_required_unowned_runtime_path_selects_all_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(tmp_path, required="true")

    assert result.returncode == 0, result.stderr
    matrix = json.loads(output.removeprefix("images=").strip())
    assert matrix == [
        {"image": "worker", "dockerfile": "deploy/Dockerfile.worker"},
        {"image": "service", "dockerfile": "deploy/Dockerfile.service"},
        {
            "image": "control-plane",
            "dockerfile": "deploy/Dockerfile.control-plane",
        },
        {"image": "llm-gateway", "dockerfile": "deploy/Dockerfile.gateway"},
        {"image": "web", "dockerfile": "deploy/Dockerfile.web"},
        {
            "image": "llm-gateway-sandbox",
            "dockerfile": "deploy/Dockerfile.gateway-sandbox",
        },
    ]


@pytest.mark.parametrize("required", ["", "invalid"])
def test_images_matrix_plan_rejects_malformed_required(
    tmp_path: Path,
    required: str,
) -> None:
    result, output = _run_image_matrix_plan(tmp_path, required=required)

    assert result.returncode != 0
    assert "FAIL: invalid planner boolean required=" in result.stderr
    assert output == ""


def test_images_merge_groups_select_a_nonempty_matrix() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    plan_script = "\n".join(
        step.get("run", "")
        for step in workflow["jobs"]["plan"]["steps"]
        if "run" in step
    )

    assert 'if event in {"workflow_dispatch", "merge_group"}:' in plan_script
    assert 'if event not in {"workflow_dispatch", "merge_group"}:' in plan_script


def test_images_merge_groups_do_not_publish_or_use_queue_ref_tags() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    build_steps = workflow["jobs"]["build"]["steps"]
    login_step = next(
        step for step in build_steps if step.get("name") == "Log in to GHCR"
    )
    ref_step = next(
        step for step in build_steps if step.get("id") == "ref"
    )

    assert login_step["if"] == (
        "github.event_name != 'pull_request' && github.event_name != 'merge_group'"
    )
    assert (
        'if [[ "${{ github.event_name }}" == "pull_request" ]]; then\n'
        '  echo "tag_args=--tag ${image}:pr-${{ github.event.number }}" >> "$GITHUB_OUTPUT"\n'
        '  echo "push_flag=" >> "$GITHUB_OUTPUT"\n'
        'elif [[ "${{ github.event_name }}" == "merge_group" ]]; then\n'
        '  echo "tag_args=--tag ${image}:merge-group-${sha_short}" >> "$GITHUB_OUTPUT"\n'
        '  echo "push_flag=" >> "$GITHUB_OUTPUT"\n'
        "else\n"
        '  branch="${{ github.ref_name }}"\n'
        '  echo "tag_args=--tag ${image}:${sha_short} --tag ${image}:${branch}" >> "$GITHUB_OUTPUT"\n'
        '  echo "push_flag=--push" >> "$GITHUB_OUTPUT"\n'
        "fi"
    ) in ref_step["run"]


def test_stable_gate_scripts_have_valid_bash_syntax() -> None:
    invalid_gates: dict[str, str] = {}
    for workflow_path, (gate_id, _) in GATE_CONTRACTS.items():
        result = subprocess.run(
            ["bash", "-n"],
            input=_gate_script(workflow_path, gate_id),
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode:
            invalid_gates[gate_id] = result.stderr

    assert not invalid_gates, invalid_gates


def test_protected_workflows_rerun_when_pull_request_base_is_edited() -> None:
    for workflow_path in GATE_CONTRACTS:
        workflow = _workflow(workflow_path)
        pull_request = _workflow_on(workflow)["pull_request"]

        assert "edited" in pull_request["types"], workflow_path


def test_manual_aggregate_contexts_have_distinct_event_specific_names() -> None:
    for workflow_path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        gate_name = workflow["jobs"][gate_id]["name"]

        assert gate_name == (
            "${{ github.event_name == 'workflow_dispatch' "
            f"&& '{protected_name}-manual' || '{protected_name}' "
            "}}"
        )


@pytest.mark.parametrize("required", ["", "invalid"])
@pytest.mark.parametrize(
    ("workflow_path", "gate_id", "result_env"),
    [
        (
            ".github/workflows/images.yml",
            "images-gate",
            {"BUILD_RESULT": "skipped"},
        ),
        (
            ".github/workflows/cluster-smoke.yml",
            "cluster-smoke-gate",
            {"SMOKE_RESULT": "skipped"},
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            {"SMOKE_RESULT": "skipped", "AWS_S3_RESULT": "skipped"},
        ),
    ],
)
def test_optional_gate_scripts_fail_closed_for_invalid_required(
    workflow_path: str,
    gate_id: str,
    result_env: dict[str, str],
    required: str,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(workflow_path, gate_id),
        text=True,
        capture_output=True,
        env={"PLAN_RESULT": "success", "REQUIRED": required, **result_env},
        check=False,
    )

    assert result.returncode != 0, (workflow_path, required, result.stdout)
    assert "FAIL: invalid planner boolean required=" in result.stderr


@pytest.mark.parametrize(
    ("required", "heavy_result"),
    [("true", "success"), ("false", "skipped"), ("false", "success")],
)
@pytest.mark.parametrize(
    ("workflow_path", "gate_id", "result_names"),
    [
        (".github/workflows/images.yml", "images-gate", ["BUILD_RESULT"]),
        (
            ".github/workflows/cluster-smoke.yml",
            "cluster-smoke-gate",
            ["SMOKE_RESULT"],
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            ["SMOKE_RESULT", "AWS_S3_RESULT"],
        ),
    ],
)
def test_optional_gate_scripts_preserve_result_semantics(
    workflow_path: str,
    gate_id: str,
    result_names: list[str],
    required: str,
    heavy_result: str,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(workflow_path, gate_id),
        text=True,
        capture_output=True,
        env={
            "PLAN_RESULT": "success",
            "REQUIRED": required,
            **dict.fromkeys(result_names, heavy_result),
        },
        check=False,
    )

    assert result.returncode == 0, (workflow_path, required, result.stderr)


@pytest.mark.parametrize("planner_value", ["", "invalid"])
@pytest.mark.parametrize(
    "planner_name",
    [
        "DOCS_ONLY",
        "INTEGRATION_SELECTED",
        "DOCKER_SELECTED",
        "COVERAGE_SELECTED",
    ],
)
def test_repository_checks_fails_closed_for_invalid_planner_booleans(
    planner_name: str,
    planner_value: str,
) -> None:
    env = {
        "PLAN_RESULT": "success",
        "FAST_RESULT": "success",
        "GO_RESULT": "success",
        "DOCS_ONLY": "false",
        "INTEGRATION_SELECTED": "false",
        "INTEGRATION_RESULT": "skipped",
        "DOCKER_SELECTED": "false",
        "DOCKER_RESULT": "skipped",
        "COVERAGE_SELECTED": "false",
        "COVERAGE_RESULT": "skipped",
    }
    env[planner_name] = planner_value

    result = subprocess.run(
        ["bash"],
        input=_gate_script(".github/workflows/ci.yml", "repository-checks"),
        text=True,
        capture_output=True,
        env=env,
        check=False,
    )

    assert result.returncode != 0, (planner_name, planner_value, result.stdout)
    assert "FAIL: invalid planner boolean" in result.stderr


@pytest.mark.parametrize(
    ("selected", "validation_result"),
    [("true", "success"), ("false", "skipped"), ("false", "success")],
)
def test_repository_checks_preserves_result_semantics(
    selected: str,
    validation_result: str,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(".github/workflows/ci.yml", "repository-checks"),
        text=True,
        capture_output=True,
        env={
            "PLAN_RESULT": "success",
            "FAST_RESULT": "success",
            "GO_RESULT": "success",
            "DOCS_ONLY": "false",
            "INTEGRATION_SELECTED": selected,
            "INTEGRATION_RESULT": validation_result,
            "DOCKER_SELECTED": selected,
            "DOCKER_RESULT": validation_result,
            "COVERAGE_SELECTED": selected,
            "COVERAGE_RESULT": validation_result,
        },
        check=False,
    )

    assert result.returncode == 0, (selected, validation_result, result.stderr)


def test_optional_validation_workflows_have_stable_gate_contexts() -> None:
    contracts = {
        ".github/workflows/images.yml": (
            "images-gate",
            "images-gate",
            {"build": "BUILD_RESULT"},
        ),
        ".github/workflows/cluster-smoke.yml": (
            "cluster-smoke-gate",
            "cluster-smoke-gate",
            {"smoke": "SMOKE_RESULT"},
        ),
        ".github/workflows/staging-smoke.yml": (
            "staging-smoke-gate",
            "staging-smoke-gate",
            {
                "smoke": "SMOKE_RESULT",
                "smoke-storage-aws-s3": "AWS_S3_RESULT",
            },
        ),
    }

    for workflow_path, (gate_id, gate_name, heavy_jobs) in contracts.items():
        workflow = _workflow(workflow_path)
        on_config = _workflow_on(workflow)
        jobs = workflow["jobs"]

        assert "merge_group" in on_config
        assert "paths" not in on_config["pull_request"]
        assert "plan" in jobs
        assert "always()" in jobs[gate_id]["if"]
        assert gate_name in jobs[gate_id]["name"]
        assert set(jobs[gate_id]["needs"]) == {"plan", *heavy_jobs}

        plan_script = "\n".join(
            step.get("run", "") for step in jobs["plan"]["steps"] if "run" in step
        )
        assert "scripts/plan_ci_validations.py" in plan_script

        gate_step = next(
            step
            for step in jobs[gate_id]["steps"]
            if step.get("name", "").startswith("Enforce selected")
        )
        gate_env = gate_step["env"]
        gate_script = gate_step["run"]
        assert gate_env["PLAN_RESULT"] == "${{ needs.plan.result }}"
        assert gate_env["REQUIRED"] == "${{ needs.plan.outputs.required }}"
        assert '"$PLAN_RESULT"' in gate_script
        assert '"$REQUIRED"' in gate_script

        for job_id, result_env in heavy_jobs.items():
            assert "plan" in jobs[job_id]["needs"]
            assert "needs.plan.outputs.required == 'true'" in jobs[job_id]["if"]
            assert gate_env[result_env] == f"${{{{ needs.{job_id}.result }}}}"
            assert f'"${result_env}"' in gate_script


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
    assert "repository-checks" in jobs["repository-checks"]["name"]
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
        "DOCS_ONLY": "${{ needs.workflow-plan.outputs.docs_only }}",
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


def test_ci_planner_uses_merge_base_for_pr_changed_paths_only() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    plan_script = next(
        step["run"]
        for step in workflow["jobs"]["workflow-plan"]["steps"]
        if step.get("id") == "plan"
    )

    assert (
        'pull_request)\n    git diff --name-only "$BASE_SHA...$HEAD_SHA"'
        in plan_script
    )
    assert (
        'merge_group)\n    git diff --name-only "$BASE_SHA" "$HEAD_SHA"'
        in plan_script
    )
    assert "pull_request|merge_group)" not in plan_script


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
