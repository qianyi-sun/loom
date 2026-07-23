from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import scripts.component_ownership as component_ownership
import yaml
from scripts.ops.authoritative_gate import GATE_SPECS

REPO_ROOT = Path(__file__).resolve().parents[2]


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


def _locked_action_sha(action: str) -> str:
    lock = json.loads(
        (REPO_ROOT / "config/ci-actions-lock.json").read_text(encoding="utf-8"),
    )
    return str(lock["actions"][action]["sha"])


def _yaml_documents(path: str) -> list[dict[str, Any]]:
    return [
        document
        for document in yaml.safe_load_all(
            (REPO_ROOT / path).read_text(encoding="utf-8"),
        )
        if document is not None
    ]


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


def _run_image_matrix_plan(
    tmp_path: Path,
    *,
    required: str,
    unowned_runtime: str,
    changed_paths: tuple[str, ...] = ("unowned-runtime/new-input.bin",),
) -> tuple[subprocess.CompletedProcess[str], str]:
    changed_files = tmp_path / "changed-files.txt"
    changed_files.write_text("\n".join(changed_paths) + "\n", encoding="utf-8")
    github_output = tmp_path / "github-output.txt"
    result = subprocess.run(
        ["bash"],
        input=_image_matrix_step()["run"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "EVENT_NAME": "pull_request",
            "REQUIRED": required,
            "UNOWNED_RUNTIME": unowned_runtime,
            "CHANGED_FILES": str(changed_files),
            "GITHUB_OUTPUT": str(github_output),
        },
        check=False,
    )
    output = github_output.read_text(encoding="utf-8") if github_output.exists() else ""
    return result, output


def _github_output_value(output: str, key: str) -> str:
    values = dict(line.split("=", maxsplit=1) for line in output.splitlines())
    return values[key]


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

SOURCE_PLAN_CONTRACTS = {
    ".github/workflows/ci.yml": ("workflow-plan", "plan"),
    ".github/workflows/images.yml": ("plan", "required"),
    ".github/workflows/cluster-smoke.yml": ("plan", "plan"),
    ".github/workflows/staging-smoke.yml": ("plan", "plan"),
}


def _normalized_expression(value: str) -> str:
    return " ".join(value.split())


def test_source_workflows_share_authoritative_generation_marker() -> None:
    workflows = {path: _workflow(path) for path in GATE_CONTRACTS}
    run_names = {workflow["run-name"] for workflow in workflows.values()}

    assert len(run_names) == 1
    run_name = run_names.pop()
    assert "github.event.pull_request.base.sha == " in run_name
    assert "28aa5257927a3468ebc35ec7f245fecaf3226dbf" in run_name
    assert "' ' ||" in run_name
    for mode in ("manual", "filtered", "full"):
        assert f"gate={mode} / head={{0}} / base={{1}}" in run_name
    for field in ("updated={2}", "action={3}", "label={4}", "labels={5}", "pull={6}"):
        assert field in run_name
    assert run_name.count("format('{0}{1}{2}{3}{4}{5}'") == 3

    assert "github.event.pull_request.draft" in run_name
    assert "github.event.action == 'converted_to_draft'" in run_name
    assert """fromJSON('["labeled","unlabeled"]')""" in run_name
    assert "github.event.action == 'edited'" in run_name
    assert "github.event.changes.base == null" in run_name
    for label in (
        "ci:integration",
        "ci:integration-docker",
        "ci:images",
        "cluster-smoke",
        "staging-smoke",
        "ci:coverage-summary",
    ):
        assert label in run_name
        assert run_name.count(f"contains(github.event.pull_request.labels.*.name, '{label}')") == 3

    expected_pr_types = {
        "opened",
        "synchronize",
        "reopened",
        "ready_for_review",
        "converted_to_draft",
        "labeled",
        "unlabeled",
        "edited",
    }
    for workflow in workflows.values():
        assert set(_workflow_on(workflow)["pull_request"]["types"]) == expected_pr_types


def test_source_workflows_detect_publisher_from_base_or_trusted_promotion() -> None:
    bootstrap_fragments: set[str] = set()

    for workflow_path, (plan_job_id, plan_step_id) in SOURCE_PLAN_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        plan_job = workflow["jobs"][plan_job_id]
        plan_step = next(step for step in plan_job["steps"] if step.get("id") == plan_step_id)
        expected_output = f"${{{{ steps.{plan_step_id}.outputs.publisher_active }}}}"

        assert plan_job["outputs"]["publisher_active"] == expected_output
        assert plan_step["env"]["BASE_SHA"].startswith("${{ ")
        assert plan_step["env"]["TRUSTED_PROMOTION"] == (
            "${{ github.event_name == 'pull_request' && "
            "github.event.pull_request.head.repo.full_name == github.repository && "
            "github.event.pull_request.head.ref == 'dev' && "
            "github.event.pull_request.base.ref == 'main' }}"
        )
        assert "publisher_active=false" in plan_step["run"]
        fragment = plan_step["run"].split("publisher_active=false", maxsplit=1)[1]
        bootstrap_fragments.add(fragment)
        assert 'git cat-file -e "${BASE_SHA}^{tree}"' in fragment
        assert '[[ "$TRUSTED_PROMOTION" == "true" ]]' in fragment
        assert (
            '[[ "$BASE_SHA" == "28aa5257927a3468ebc35ec7f245fecaf3226dbf" ]]'
            in fragment
        )
        assert '"${BASE_SHA}:.github/workflows/authoritative-gates.yml"' in fragment
        assert "publisher-contract: dynamic-run-name-v[12]" in fragment
        assert "HEAD_SHA" not in fragment

    assert len(bootstrap_fragments) == 1


def test_source_gate_names_switch_only_after_base_publisher_is_active() -> None:
    for workflow_path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        plan_job_id, _ = SOURCE_PLAN_CONTRACTS[workflow_path]
        plan_ref = f"needs.{plan_job_id}.outputs"
        expression = _normalized_expression(workflow["jobs"][gate_id]["name"])

        assert expression == _normalized_expression(
            "${{ "
            f"github.event_name == 'workflow_dispatch' && '{protected_name}-manual' || "
            f"github.event_name == 'push' && '{protected_name}-push' || "
            f"{plan_ref}.gate_mode == 'full' && "
            f"{plan_ref}.publisher_active == 'false' && "
            f"'{protected_name}' || "
            f"{plan_ref}.gate_mode == 'full' && '{protected_name}-attempt' || "
            f"'{protected_name}-filtered' "
            "}}"
        )


def test_v1_bootstrap_jobs_are_bound_to_the_exact_repair_pull() -> None:
    for workflow_path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        bootstrap = workflow["jobs"]["bootstrap-authoritative-v2"]
        condition = _normalized_expression(bootstrap["if"])

        assert bootstrap["needs"] == [gate_id]
        assert "github.event_name == 'pull_request'" in condition
        assert "!github.event.pull_request.draft" in condition
        assert "github.event.pull_request.number == 932" in condition
        assert "cfe71eddd9a8e768aa84d003bbf6a0bd0110f9ca" in condition
        assert "codex/833-authoritative-gates-acceptance" in condition
        assert "github.event.pull_request.head.repo.full_name == github.repository" in condition
        assert bootstrap["permissions"] == {
            "actions": "read",
            "checks": "write",
            "contents": "read",
            "issues": "read",
            "pull-requests": "read",
        }
        checkout = bootstrap["steps"][0]
        assert checkout["with"]["ref"] == "${{ github.event.pull_request.head.sha }}"
        publish = bootstrap["steps"][1]
        assert publish["env"]["AUTHORITATIVE_CONTEXT"] == protected_name
        assert publish["env"]["AUTHORITATIVE_BOOTSTRAP_GATE_RESULT"] == (
            f"${{{{ needs.{gate_id}.result }}}}"
        )
        assert publish["env"]["AUTHORITATIVE_BOOTSTRAP_WORKFLOW_SHA"] == (
            "${{ github.event.pull_request.head.sha }}"
        )


def test_push_aggregates_cannot_duplicate_protected_names_on_promotion_heads() -> None:
    for workflow_path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        expression = _normalized_expression(_workflow(workflow_path)["jobs"][gate_id]["name"])

        assert f"github.event_name == 'push' && '{protected_name}-push'" in expression


def test_trusted_dev_to_main_promotion_activates_the_publisher() -> None:
    for workflow_path, (gate_id, _protected_name) in GATE_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        plan_job_id, plan_step_id = SOURCE_PLAN_CONTRACTS[workflow_path]
        plan_step = next(
            step
            for step in workflow["jobs"][plan_job_id]["steps"]
            if step.get("id") == plan_step_id
        )
        assert (
            "github.event.pull_request.head.ref == 'dev'" in plan_step["env"]["TRUSTED_PROMOTION"]
        )
        assert (
            "github.event.pull_request.base.ref == 'main'" in plan_step["env"]["TRUSTED_PROMOTION"]
        )
        assert "publisher_active=true" in plan_step["run"]
        assert "-attempt" in workflow["jobs"][gate_id]["name"]


def test_authoritative_gate_workflow_uses_only_trusted_code() -> None:
    workflow = _workflow(".github/workflows/authoritative-gates.yml")
    on_config = _workflow_on(workflow)

    assert set(on_config) == {"pull_request_target", "workflow_run"}
    assert set(on_config["pull_request_target"]["types"]) == {
        "opened",
        "synchronize",
        "reopened",
        "ready_for_review",
        "converted_to_draft",
        "labeled",
        "unlabeled",
        "edited",
    }
    assert on_config["workflow_run"] == {
        "workflows": ["CI", "images", "cluster-smoke", "staging-smoke"],
        "types": ["requested", "in_progress", "completed"],
    }
    assert workflow["permissions"] == {
        "actions": "read",
        "checks": "write",
        "contents": "read",
        "issues": "read",
        "pull-requests": "read",
    }

    assert "concurrency" not in workflow

    assert set(workflow["jobs"]) == {"publish"}
    publish = workflow["jobs"]["publish"]
    job_filter = publish["if"]
    assert "github.event_name == 'workflow_run'" in job_filter
    assert (
        'contains(fromJSON(\'["pull_request","merge_group"]\'), github.event.workflow_run.event)'
    ) in _normalized_expression(job_filter)
    assert "!github.event.pull_request.draft" in job_filter
    assert "ready_for_review" in job_filter
    assert "converted_to_draft" in job_filter
    assert "synchronize" in job_filter
    assert "github.event.changes.base != null" in job_filter
    for label in (
        "ci:integration",
        "ci:integration-docker",
        "ci:images",
        "cluster-smoke",
        "staging-smoke",
        "ci:coverage-summary",
    ):
        assert label in job_filter
    matrix = publish["strategy"]["matrix"]["context"]
    for context in (
        "repository-checks",
        "images-gate",
        "cluster-smoke-gate",
        "staging-smoke-gate",
    ):
        assert context in matrix
    for spec in GATE_SPECS:
        source_identity = f"github.event.workflow_run.workflow_id == {spec.workflow_id}"
        assert str(spec.workflow_id) in _normalized_expression(job_filter)
        assert source_identity in _normalized_expression(matrix)
    assert "github.event.workflow_run.name" not in job_filter
    assert "github.event.workflow_run.name" not in matrix

    concurrency = publish["concurrency"]
    assert concurrency["cancel-in-progress"] is False
    assert "github.event.workflow_run.head_sha" in concurrency["group"]
    assert "github.event.pull_request.head.sha" in concurrency["group"]
    assert "matrix.context" in concurrency["group"]

    assert publish["timeout-minutes"] == 5
    checkout = next(
        step
        for step in publish["steps"]
        if str(step.get("uses", "")).startswith("actions/checkout@")
    )
    assert checkout["uses"] == (f"actions/checkout@{_locked_action_sha('actions/checkout')}")
    assert checkout["with"] == {
        "ref": "${{ github.workflow_sha }}",
        "fetch-depth": 1,
        "persist-credentials": False,
    }
    assert "pull_request" not in checkout["with"]["ref"]
    assert "workflow_run" not in checkout["with"]["ref"]

    publisher = next(
        step for step in publish["steps"] if step.get("name") == "Publish authoritative gate state"
    )
    assert publisher["env"] == {
        "AUTHORITATIVE_CONTEXT": "${{ matrix.context }}",
        "GITHUB_TOKEN": "${{ secrets.GITHUB_TOKEN }}",
    }
    assert _normalized_expression(publisher["run"]) == _normalized_expression(
        "python scripts/ops/authoritative_gate.py "
        '--event-path "$GITHUB_EVENT_PATH" '
        '--context "$AUTHORITATIVE_CONTEXT"'
    )


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

    assert jobs["plan"]["outputs"]["required"] == "${{ steps.plan.outputs.required }}"
    assert jobs["build"]["needs"] == "plan"
    assert "needs.plan.outputs.required == 'true'" in jobs["build"]["if"]


def test_images_multi_arch_jobs_have_bounded_build_budgets() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert jobs["build"]["timeout-minutes"] == 45
    assert jobs["publish"]["timeout-minutes"] == 45


@pytest.mark.parametrize(
    ("job_name", "lane"),
    [
        ("tests-root", "tests-root"),
        ("tests-packages", "tests-packages"),
        ("integration", "integration"),
        ("integration-docker", "integration-docker"),
    ],
)
def test_pytest_jobs_consume_manifest_owned_lane_paths(job_name: str, lane: str) -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    scripts = "\n".join(step.get("run", "") for step in workflow["jobs"][job_name]["steps"])

    assert (
        f"uv run --no-sync python scripts/component_ownership.py test-paths --lane {lane}"
        in scripts
    )
    assert 'uv run --no-sync pytest "${test_paths[@]}"' in scripts


def test_cluster_smoke_consumes_manifest_owned_lane_paths() -> None:
    workflow = _workflow(".github/workflows/cluster-smoke.yml")
    smoke = workflow["jobs"]["smoke"]
    scripts = "\n".join(step.get("run", "") for step in smoke["steps"])

    assert (
        "uv run --no-sync python scripts/component_ownership.py test-paths --lane cluster-smoke"
        in scripts
    )
    assert "uv sync --locked --all-packages --extra dev --extra cluster --extra rollout" in scripts
    assert "uv pip check --python .venv/bin/python" in scripts
    assert 'uv run --no-sync pytest "${test_paths[@]}"' in scripts
    assert smoke["timeout-minutes"] >= 25


def test_images_workflow_uses_path_aware_matrix_plan() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]
    push_trigger = _workflow_on(workflow)["push"]

    assert "plan" in jobs
    assert "images" in jobs["plan"]["outputs"]
    build = jobs["build"]
    assert build["needs"] == "plan"
    assert build["strategy"]["matrix"]["include"] == "${{ fromJSON(needs.plan.outputs.images) }}"
    plan_script = "\n".join(step.get("run", "") for step in jobs["plan"]["steps"] if "run" in step)
    assert "scripts/component_ownership.py" in plan_script
    assert "plan-images" in plan_script
    assert push_trigger == {"branches": ["dev", "main"]}


def test_images_matrix_plan_receives_shared_required_decision() -> None:
    env = _image_matrix_step()["env"]
    assert env["REQUIRED"] == ("${{ steps.required.outputs.images }}")
    assert env["UNOWNED_RUNTIME"] == ("${{ steps.required.outputs.unowned_runtime }}")


def test_images_required_unowned_runtime_path_selects_all_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime="true",
    )

    assert result.returncode == 0, result.stderr
    matrix = json.loads(_github_output_value(output, "images"))
    assert _github_output_value(output, "required") == "true"
    assert len(matrix) == 11
    assert {entry["image"] for entry in matrix} == {
        "agent-sandbox",
        "control-plane",
        "egress-xds",
        "family-orchestrator",
        "llm-gateway",
        "llm-gateway-sandbox",
        "rehearsal-postgres",
        "service",
        "staging-admin-browser-smoke",
        "web",
        "worker",
    }
    assert all(set(entry) == {"image", "image_name", "dockerfile", "context"} for entry in matrix)


def test_images_mixed_known_and_unowned_paths_select_all_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime="true",
        changed_paths=("web/src/App.tsx", "unowned-runtime/new-input.bin"),
    )

    assert result.returncode == 0, result.stderr
    matrix = json.loads(_github_output_value(output, "images"))
    assert {entry["image"] for entry in matrix} == {
        "agent-sandbox",
        "worker",
        "service",
        "control-plane",
        "egress-xds",
        "family-orchestrator",
        "llm-gateway",
        "staging-admin-browser-smoke",
        "web",
        "llm-gateway-sandbox",
        "rehearsal-postgres",
    }


def test_frontend_security_policy_change_selects_only_web_image(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime="false",
        changed_paths=("deploy/nginx-spa-security-headers.conf",),
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(_github_output_value(output, "images")) == [
        {
            "image": "web",
            "image_name": "loom-web",
            "dockerfile": "deploy/Dockerfile.web",
            "context": ".",
        },
    ]


def test_manifest_owned_markdown_build_input_requires_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="false",
        unowned_runtime="false",
        changed_paths=("README.md",),
    )

    assert result.returncode == 0, result.stderr
    assert _github_output_value(output, "required") == "true"
    matrix = json.loads(_github_output_value(output, "images"))
    assert {entry["image"] for entry in matrix} == {
        "control-plane",
        "family-orchestrator",
        "llm-gateway",
        "service",
        "worker",
    }


def test_unowned_static_documentation_does_not_require_images(tmp_path: Path) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="false",
        unowned_runtime="false",
        changed_paths=("docs/user-guide.md",),
    )

    assert result.returncode == 0, result.stderr
    assert _github_output_value(output, "required") == "false"
    assert json.loads(_github_output_value(output, "images")) == []


@pytest.mark.parametrize("required", ["", "invalid"])
def test_images_matrix_plan_rejects_malformed_required(
    tmp_path: Path,
    required: str,
) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required=required,
        unowned_runtime="false",
    )

    assert result.returncode != 0
    assert "FAIL: invalid planner boolean:" in result.stderr
    assert output == ""


@pytest.mark.parametrize("unowned_runtime", ["", "invalid"])
def test_images_matrix_plan_rejects_malformed_unowned_runtime(
    tmp_path: Path,
    unowned_runtime: str,
) -> None:
    result, output = _run_image_matrix_plan(
        tmp_path,
        required="true",
        unowned_runtime=unowned_runtime,
    )

    assert result.returncode != 0
    assert "FAIL: invalid planner boolean:" in result.stderr
    assert output == ""


def test_images_merge_groups_select_a_nonempty_matrix() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    plan_script = "\n".join(
        step.get("run", "") for step in workflow["jobs"]["plan"]["steps"] if "run" in step
    )

    assert '"$EVENT_NAME" == "workflow_dispatch"' in plan_script
    assert '"$EVENT_NAME" == "merge_group"' in plan_script
    assert "--force-all" in plan_script


def test_images_merge_groups_do_not_publish_or_write_cache() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    build_steps = workflow["jobs"]["build"]["steps"]
    build_script = "\n".join(str(step["run"]) for step in build_steps if "run" in step)
    publish = workflow["jobs"]["publish"]

    assert "docker login" not in build_script
    assert "--push" not in build_script
    assert "--cache-to" not in build_script
    assert 'merge_group) image_tag="merge-group-${sha_short}"' in build_script
    assert "github.event_name == 'push'" in publish["if"]
    assert any(step.get("name") == "Log in to GHCR" for step in publish["steps"])


def test_manifest_image_build_and_publish_pass_exact_full_head_sha() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    expected_steps = {
        "build": "Build without registry or cache write authority",
        "publish": "Build and publish trusted image",
    }

    for job_name, step_name in expected_steps.items():
        step = next(
            step for step in workflow["jobs"][job_name]["steps"] if step.get("name") == step_name
        )
        script = step["run"]
        assert step["env"]["HEAD_SHA"] == "${{ github.sha }}"
        assert step["env"]["BUILD_CONTEXT"] == "${{ matrix.context }}"
        assert '--build-arg "LOOM_BUILD_SHA=${HEAD_SHA}"' in script
        assert 'build_args+=("$BUILD_CONTEXT")' in script
        assert script.index("LOOM_BUILD_SHA=${HEAD_SHA}") < script.index(
            'build_args+=("$BUILD_CONTEXT")'
        )
        assert 'if [[ "$IMAGE_NAME" == "service" ]]' not in script
        assert "build_args+=(.)" not in script


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


def test_protected_workflows_cover_gate_authority_pr_transitions() -> None:
    for workflow_path in GATE_CONTRACTS:
        workflow = _workflow(workflow_path)
        pull_request = _workflow_on(workflow)["pull_request"]

        assert {
            "opened",
            "synchronize",
            "reopened",
            "ready_for_review",
            "converted_to_draft",
            "labeled",
            "unlabeled",
            "edited",
        } <= set(pull_request["types"]), workflow_path


def test_manual_and_filtered_contexts_have_distinct_event_specific_names() -> None:
    for workflow_path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        gate_name = workflow["jobs"][gate_id]["name"]

        assert f"'{protected_name}-manual'" in gate_name
        assert f"'{protected_name}'" in gate_name
        assert f"'{protected_name}-filtered'" in gate_name
        assert "preflight" not in gate_name
        assert "invalidate" not in gate_name


@pytest.mark.parametrize("required", ["", "invalid"])
@pytest.mark.parametrize(
    ("workflow_path", "gate_id", "result_env"),
    [
        (
            ".github/workflows/images.yml",
            "images-gate",
            {
                "EVENT_NAME": "pull_request",
                "BUILD_RESULT": "skipped",
                "PUBLISH_RESULT": "skipped",
            },
        ),
        (
            ".github/workflows/cluster-smoke.yml",
            "cluster-smoke-gate",
            {"SMOKE_RESULT": "skipped"},
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            {"SMOKE_RESULT": "skipped", "SYSTEM_SMOKE_RESULT": "skipped"},
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
        env={
            "PLAN_RESULT": "success",
            "GATE_MODE": "full",
            "REQUIRED": required,
            **result_env,
        },
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
        (
            ".github/workflows/cluster-smoke.yml",
            "cluster-smoke-gate",
            ["SMOKE_RESULT"],
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            ["SMOKE_RESULT", "SYSTEM_SMOKE_RESULT"],
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
            "GATE_MODE": "full",
            "REQUIRED": required,
            **dict.fromkeys(result_names, heavy_result),
        },
        check=False,
    )

    assert result.returncode == 0, (workflow_path, required, result.stderr)


@pytest.mark.parametrize(
    ("event_name", "required", "build_result", "publish_result"),
    [
        ("pull_request", "true", "success", "skipped"),
        ("merge_group", "true", "success", "skipped"),
        ("workflow_dispatch", "true", "success", "skipped"),
        ("push", "true", "skipped", "success"),
        ("pull_request", "false", "skipped", "skipped"),
    ],
)
def test_images_gate_separates_untrusted_build_from_trusted_publish(
    event_name: str,
    required: str,
    build_result: str,
    publish_result: str,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(".github/workflows/images.yml", "images-gate"),
        text=True,
        capture_output=True,
        env={
            "EVENT_NAME": event_name,
            "PLAN_RESULT": "success",
            "GATE_MODE": "full",
            "REQUIRED": required,
            "BUILD_RESULT": build_result,
            "PUBLISH_RESULT": publish_result,
        },
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("event_name", "required", "build_result", "publish_result"),
    [
        ("pull_request", "true", "success", "success"),
        ("workflow_dispatch", "true", "skipped", "success"),
        ("push", "true", "success", "skipped"),
        ("pull_request", "false", "success", "skipped"),
        ("invalid", "true", "success", "skipped"),
    ],
)
def test_images_gate_rejects_cross_lane_or_ambiguous_results(
    event_name: str,
    required: str,
    build_result: str,
    publish_result: str,
) -> None:
    result = subprocess.run(
        ["bash"],
        input=_gate_script(".github/workflows/images.yml", "images-gate"),
        text=True,
        capture_output=True,
        env={
            "EVENT_NAME": event_name,
            "PLAN_RESULT": "success",
            "GATE_MODE": "full",
            "REQUIRED": required,
            "BUILD_RESULT": build_result,
            "PUBLISH_RESULT": publish_result,
        },
        check=False,
    )

    assert result.returncode != 0


@pytest.mark.parametrize("planner_value", ["", "invalid"])
@pytest.mark.parametrize(
    "planner_name",
    [
        "DOCS_ONLY",
        "INTEGRATION_SELECTED",
        "DOCKER_SELECTED",
        "COVERAGE_SELECTED",
        "WEB_SELECTED",
    ],
)
def test_repository_checks_fails_closed_for_invalid_planner_booleans(
    planner_name: str,
    planner_value: str,
) -> None:
    env = {
        "PLAN_RESULT": "success",
        "GATE_MODE": "full",
        "FAST_RESULT": "success",
        "GO_RESULT": "success",
        "DOCS_ONLY": "false",
        "INTEGRATION_SELECTED": "false",
        "INTEGRATION_RESULT": "skipped",
        "DOCKER_SELECTED": "false",
        "DOCKER_RESULT": "skipped",
        "COVERAGE_SELECTED": "false",
        "COVERAGE_RESULT": "skipped",
        "WEB_SELECTED": "false",
        "WEB_RESULT": "skipped",
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
            "GATE_MODE": "full",
            "FAST_RESULT": "success",
            "GO_RESULT": "success",
            "DOCS_ONLY": "false",
            "INTEGRATION_SELECTED": selected,
            "INTEGRATION_RESULT": validation_result,
            "DOCKER_SELECTED": selected,
            "DOCKER_RESULT": validation_result,
            "COVERAGE_SELECTED": selected,
            "COVERAGE_RESULT": validation_result,
            "WEB_SELECTED": selected,
            "WEB_RESULT": validation_result,
        },
        check=False,
    )

    assert result.returncode == 0, (selected, validation_result, result.stderr)


def test_optional_validation_workflows_have_stable_gate_contexts() -> None:
    contracts = {
        ".github/workflows/images.yml": (
            "images-gate",
            "images-gate",
            {"build": "BUILD_RESULT", "publish": "PUBLISH_RESULT"},
        ),
        ".github/workflows/cluster-smoke.yml": (
            "cluster-smoke-gate",
            "cluster-smoke-gate",
            {"smoke": "SMOKE_RESULT"},
        ),
        ".github/workflows/staging-smoke.yml": (
            "staging-smoke-gate",
            "staging-smoke-gate",
            {"smoke": "SMOKE_RESULT", "system-smoke": "SYSTEM_SMOKE_RESULT"},
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
            assert "needs.plan.outputs.gate_mode == 'full'" in jobs[job_id]["if"]
            assert "needs.plan.outputs.required == 'true'" in jobs[job_id]["if"]
            assert gate_env[result_env] == f"${{{{ needs.{job_id}.result }}}}"
            assert f'"${result_env}"' in gate_script


def test_repository_checks_context_is_parallel_aggregator() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]

    assert jobs["fast-checks"]["name"] == "fast-checks"
    assert set(jobs["fast-checks"]["needs"]) == {
        "workflow-plan",
        "locked-environments",
        "lint-and-static",
        "runtime-payload",
        "tests-root",
        "tests-packages",
    }
    assert "gate_mode == 'full'" in jobs["fast-checks"]["if"]
    assert "gate_mode == 'preflight'" not in jobs["fast-checks"]["if"]
    assert jobs["integration"]["needs"] == "workflow-plan"
    assert jobs["integration-docker"]["needs"] == "workflow-plan"
    assert "gate_mode == 'full'" in jobs["integration"]["if"]
    assert "gate_mode == 'full'" in jobs["integration-docker"]["if"]
    assert "repository-checks" in jobs["repository-checks"]["name"]
    assert set(jobs["repository-checks"]["needs"]) == {
        "workflow-plan",
        "fast-checks",
        "go-checks",
        "integration",
        "integration-docker",
        "coverage-summary",
        "web-checks",
    }
    assert "always()" in jobs["repository-checks"]["if"]
    assert jobs["lint-and-static"]["needs"] == "workflow-plan"
    assert jobs["tests-root"]["needs"] == "workflow-plan"
    assert jobs["tests-packages"]["needs"] == "workflow-plan"
    assert jobs["runtime-payload"]["needs"] == "workflow-plan"
    assert "gate_mode == 'full'" in jobs["runtime-payload"]["if"]
    assert "gate_mode == 'preflight'" in jobs["runtime-payload"]["if"]

    runtime_payload_scripts = "\n".join(
        step.get("run", "") for step in jobs["runtime-payload"]["steps"]
    ).strip()
    assert runtime_payload_scripts == "python3 scripts/runtime_payload_conformance.py"
    assert "continue-on-error" not in jobs["runtime-payload"]

    assert {
        "docs_only",
        "event_relevant",
        "full_gate",
        "gate_mode",
        "integration",
        "integration_docker",
        "images",
        "cluster_smoke",
        "staging_smoke",
        "coverage_summary",
        "web_checks",
    } <= set(jobs["workflow-plan"]["outputs"])

    aggregate_step = next(
        step
        for step in jobs["repository-checks"]["steps"]
        if step.get("name") == "Enforce selected validation results"
    )
    assert aggregate_step["env"] == {
        "PLAN_RESULT": "${{ needs.workflow-plan.result }}",
        "GATE_MODE": "${{ needs.workflow-plan.outputs.gate_mode }}",
        "FAST_RESULT": "${{ needs.fast-checks.result }}",
        "GO_RESULT": "${{ needs.go-checks.result }}",
        "DOCS_ONLY": "${{ needs.workflow-plan.outputs.docs_only }}",
        "INTEGRATION_SELECTED": "${{ needs.workflow-plan.outputs.integration }}",
        "INTEGRATION_RESULT": "${{ needs.integration.result }}",
        "DOCKER_SELECTED": "${{ needs.workflow-plan.outputs.integration_docker }}",
        "DOCKER_RESULT": "${{ needs.integration-docker.result }}",
        "COVERAGE_SELECTED": "${{ needs.workflow-plan.outputs.coverage_summary }}",
        "COVERAGE_RESULT": "${{ needs.coverage-summary.result }}",
        "WEB_SELECTED": "${{ needs.workflow-plan.outputs.web_checks }}",
        "WEB_RESULT": "${{ needs.web-checks.result }}",
    }
    aggregate_script = aggregate_step["run"]
    for result_name in (
        "PLAN_RESULT",
        "FAST_RESULT",
        "GO_RESULT",
        "INTEGRATION_RESULT",
        "DOCKER_RESULT",
        "COVERAGE_RESULT",
        "WEB_RESULT",
    ):
        assert f'"${result_name}"' in aggregate_script

    assert jobs["web-checks"]["needs"] == "workflow-plan"
    assert "needs.workflow-plan.outputs.web_checks == 'true'" in jobs["web-checks"]["if"]
    web_script = "\n".join(
        step.get("run", "") for step in jobs["web-checks"]["steps"] if "run" in step
    )
    assert "component_ownership.py test-paths --lane frontend" in web_script
    assert "component_ownership.py --help | grep -q -- 'test-paths'" in web_script
    assert "npm run typecheck" in web_script
    assert "npm run lint" in web_script
    assert "vitest run --coverage" in web_script
    assert "npm run test:coverage" in web_script
    assert "npm run build" in web_script
    assert "npm run test:e2e" in web_script


def test_python_test_shards_are_complete_and_non_overlapping() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]

    root_matrix = jobs["tests-root"]["strategy"]["matrix"]["include"]
    assert root_matrix == [
        {"shard": "1-of-2", "shard_index": 0},
        {"shard": "2-of-2", "shard_index": 1},
    ]
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    tracked_paths = component_ownership._tracked_paths(REPO_ROOT)
    root_paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=tracked_paths,
        lane="tests-root",
    )
    root_shards = [
        set(
            component_ownership.shard_paths(
                root_paths,
                shard_index=shard["shard_index"],
                shard_count=len(root_matrix),
            )
        )
        for shard in root_matrix
    ]
    assert root_shards[0].isdisjoint(root_shards[1])
    assert set().union(*root_shards) == set(root_paths)

    integration_matrix = jobs["integration"]["strategy"]["matrix"]["include"]
    assert integration_matrix == [
        {"shard": "1-of-2", "shard_index": 0},
        {"shard": "2-of-2", "shard_index": 1},
    ]
    integration_paths = component_ownership.test_paths_for_lane(
        manifest,
        tracked_paths=tracked_paths,
        lane="integration",
    )
    integration_shards = [
        component_ownership.shard_paths(
            integration_paths,
            shard_index=shard["shard_index"],
            shard_count=len(integration_matrix),
            strategy="contiguous",
        )
        for shard in integration_matrix
    ]
    assert set(integration_shards[0]).isdisjoint(integration_shards[1])
    assert set().union(*map(set, integration_shards)) == set(integration_paths)
    assert integration_shards[0] + integration_shards[1] == integration_paths
    auth_path = "tests/integration/test_username_password_auth.py"
    schema_path = "tests/integration/test_username_password_schema.py"
    assert any(
        auth_path in shard and schema_path in shard for shard in integration_shards
    )
    assert integration_paths.index(auth_path) < integration_paths.index(schema_path)
    integration_script = "\n".join(
        step.get("run", "") for step in jobs["integration"]["steps"]
    )
    assert "--shard-strategy contiguous" in integration_script

    root_upload = next(
        step
        for step in jobs["tests-root"]["steps"]
        if step.get("name") == "Upload root coverage data"
    )
    integration_upload = next(
        step
        for step in jobs["integration"]["steps"]
        if step.get("name") == "Upload integration coverage data"
    )
    assert "${{ matrix.shard }}" in root_upload["with"]["name"]
    assert "${{ matrix.shard }}" in integration_upload["with"]["name"]


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

    assert 'pull_request)\n    git diff --name-only "$BASE_SHA...$HEAD_SHA"' in plan_script
    assert 'merge_group)\n    git diff --name-only "$BASE_SHA" "$HEAD_SHA"' in plan_script
    assert "pull_request|merge_group)" not in plan_script


def test_protected_workflows_cancel_only_superseded_gate_runs() -> None:
    for workflow_path in GATE_CONTRACTS:
        workflow = _workflow(workflow_path)
        cancel = workflow["concurrency"]["cancel-in-progress"]

        assert "github.event_name == 'pull_request'" in cancel
        assert "synchronize" in cancel
        assert "ready_for_review" in cancel
        assert "converted_to_draft" in cancel
        assert "ci:integration" in cancel
        assert "ci:coverage-summary" in cancel
        assert "github.event.changes.base != null" in cancel
        assert "ci:merge-ready" not in cancel


def test_protected_workflows_isolate_irrelevant_metadata_pending_runs() -> None:
    for workflow_path in GATE_CONTRACTS:
        workflow = _workflow(workflow_path)
        group = workflow["concurrency"]["group"]

        assert "authoritative" in group
        assert "background" in group
        assert "github.event_name == 'pull_request'" in group
        assert "synchronize" in group
        assert "ready_for_review" in group
        assert "converted_to_draft" in group
        assert "ci:integration" in group
        assert "ci:coverage-summary" in group
        assert "github.event.changes.base != null" in group
        assert "ci:merge-ready" not in group


def test_cluster_deploy_spikes_cancel_superseded_pr_runs() -> None:
    workflow = _workflow(".github/workflows/cluster-deploy-spikes.yml")

    assert workflow["concurrency"]["cancel-in-progress"] == (
        "${{ github.event_name == 'pull_request' }}"
    )
    assert "github.event.pull_request.number || github.ref" in workflow["concurrency"]["group"]


def test_pinned_ingress_controller_config_has_trusted_raw_path_guard() -> None:
    manifest_path = "deploy/k8s/ingress-nginx-kind.yaml"
    documents = _yaml_documents(manifest_path)
    controller_config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document["metadata"]["name"] == "ingress-nginx-controller"
    )
    data = controller_config["data"]

    assert data["allow-snippet-annotations"] == "false"
    assert data["http-snippet"] == (
        "map $request_uri $loom_ambiguous_path {\n"
        "  default 0;\n"
        "  ~*^[^?]*(?:%2f|%5c|\\x5c|//) 1;\n"
        '  "~*^/[^/?]*%[0-9a-f]{2}[^/?]*(?:/|[?]|$)" 1;\n'
        "  ~^/(?:dev|prod)(?:/|\\?|$) 0;\n"
        "  ~*^/(?:dev|prod)(?:/|\\?|$) 1;\n"
        "}\n"
    )
    assert data["server-snippet"] == (
        "merge_slashes off;\nif ($loom_ambiguous_path) {\n  return 404;\n}\n"
    )
    manifest = (REPO_ROOT / manifest_path).read_text(encoding="utf-8")
    assert "nginx.ingress.kubernetes.io/server-snippet" not in manifest
    assert "nginx.ingress.kubernetes.io/configuration-snippet" not in manifest


def test_staging_route_smoke_locks_exact_ingress_boundary_probes() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    steps = workflow["jobs"]["smoke"]["steps"]
    step_names = [step.get("name") or step.get("uses") for step in steps]
    route_contract_steps = [
        "Install ingress-nginx (so preflight's IngressClass check passes)",
        "loom cluster up (wait for components Ready)",
        "Route kind ingress hosts to the local controller",
        "Set up browser smoke Node",
        "Install pinned browser smoke dependencies",
        "Verify prefixed frontend routes through ingress-nginx",
        "Verify prefixed frontend routes mount in Chromium",
        "Upload frontend route browser trace",
        "Verify staging admin exchange stays hidden in kind",
        "Verify /healthz on every service via port-forward",
    ]
    route_contract_indices = [step_names.index(name) for name in route_contract_steps]
    assert route_contract_indices == sorted(route_contract_indices)

    route_step = next(
        step
        for step in steps
        if step.get("name") == "Verify prefixed frontend routes through ingress-nginx"
    )
    script = route_step["run"]
    syntax = subprocess.run(
        ["bash", "-n"],
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    assert syntax.returncode == 0, syntax.stderr

    probes = (
        "/devil",
        "/devapi",
        "/prodfoo",
        "/DEV",
        "/Dev/",
        "/dEv/monitor",
        "/PROD",
        "/Prod/api/v1/health",
        "/D%45V/monitor",
        "/d%45v/api/v1/health",
        "/d%65v/monitor",
        "/PR%4fD/",
        "/pr%4Fd/",
        "/dev%2Fmonitor",
        "/dev/%2Fmonitor",
        "/dev%5Cmonitor",
        "/dev/%5cmonitor",
        r"/dev\monitor",
        r"/dev/\monitor",
        "/dev//monitor",
        "/dev/monitor//details",
        "/dev/api/v1/%2Fhealth",
        "/dev/api/v1/%5Chealth",
        r"/dev/api/v1\health",
        "/dev/api/v1//health",
        "/dev/api/v1/health//",
    )
    for probe in probes:
        assert probe in script
    assert "--path-as-is --no-location --max-redirs 0" in script
    assert 'if [[ "$status" != "404" ]]' in script
    assert "grep -qi '^location:' \"$headers\"" in script
    assert 'if [[ "$root_asset_status" != "404" ]]' in script
    assert "grep -qi '^location:' \"$root_asset_headers\"" in script
    assert 'if [[ "$health_status" != "200" ]]' in script
    assert "require_singleton_header" in script
    assert '"$health_headers" content-type mime application/json' in script
    assert '! require_singleton_header "$www_headers" location exact \\' in script
    assert "'https://yylx.world/dev?next=%2Fmonitor&x=1'; then" in script
    assert '! require_singleton_header "$canonical_headers" location exact \\' in script
    assert "'/dev/?next=%2Fmonitor&x=1'; then" in script
    assert "head -n 1" not in script
    assert "^content-type:.*application/json" not in script
    assert "folded response headers are not accepted" in script
    assert 'name.decode("ascii", errors="strict")' in script
    assert "port-forward \\\n  svc/loom-web 18081:80" in script
    assert "http://127.0.0.1:18081${path}" in script
    assert "https://yylx.world/dev/api/v1/health" in script
    assert "https://www.yylx.world/dev?next=%2Fmonitor&x=1" in script
    assert "scripts/ops/frontend_security_headers.py" in script
    assert "--route development=https://yylx.world/dev" in script
    assert "--route staging=https://yylx.world/dev" not in script
    assert "--probe web_500=500=http://127.0.0.1:18082/dev/security-header-5xx-probe" in script
    assert "--web-origin-only" in script
    assert "index.html.security-header-smoke" in script
    assert "trap cleanup_security_5xx_probe EXIT" in script
    assert "trap - EXIT" in script


def test_staging_build_and_load_share_manifest_owned_image_matrix() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    smoke = workflow["jobs"]["smoke"]
    steps = {step.get("name"): step for step in smoke["steps"]}
    resolve_script = steps["Resolve manifest-owned staging image matrix"]["run"]
    build_script = steps["Build images (parallel)"]["run"]
    load_script = steps["Load images into kind"]["run"]

    assert "release-images --runtime-policy start" in resolve_script
    assert smoke["env"]["STAGING_IMAGE_MATRIX"].endswith(".json")
    assert "STAGING_IMAGE_MATRIX_SHA256" in resolve_script
    assert "sha256sum --check --status" in build_script
    assert "sha256sum --check --status" in load_script
    assert 'row["dockerfile"]' in build_script
    assert 'row["context"]' in build_script
    assert 'row["image_name"]' in build_script
    assert 'row["image_name"]' in load_script
    assert "builds=(" not in build_script
    assert "for image_name in loom-" not in load_script
    assert "< <(python3" not in build_script
    assert "< <(python3" not in load_script
    assert 'done < "$build_rows"' in build_script
    assert 'done < "$image_names"' in load_script


def test_staging_active_rendered_images_are_covered_by_manifest_matrix() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "loom_cli",
            "cluster",
            "render",
            "--config",
            "deploy/environments/staging.cluster.toml",
        ],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    active_images: set[str] = set()
    for document in yaml.safe_load_all(result.stdout):
        if not isinstance(document, dict) or document.get("kind") not in {
            "DaemonSet",
            "Deployment",
            "StatefulSet",
        }:
            continue
        spec = document.get("spec")
        if not isinstance(spec, dict) or spec.get("replicas", 1) == 0:
            continue
        template = spec.get("template")
        pod_spec = template.get("spec") if isinstance(template, dict) else None
        containers = pod_spec.get("containers") if isinstance(pod_spec, dict) else None
        if not isinstance(containers, list):
            continue
        for container in containers:
            image = container.get("image") if isinstance(container, dict) else None
            if isinstance(image, str) and image.split(":", 1)[0].startswith("loom-"):
                active_images.add(image.split(":", 1)[0])

    manifest = component_ownership.load_manifest(
        REPO_ROOT / "config/component-ownership.toml"
    )
    matrix_images = {
        row["image_name"]
        for row in component_ownership.release_images_for_runtime_policy(
            manifest,
            runtime_policy="start",
        )
    }
    assert active_images <= matrix_images
    assert "loom-family-orchestrator" in active_images
    assert "loom-worker" not in active_images


def test_web_nginx_has_same_raw_path_and_case_guard_as_controller() -> None:
    config = (REPO_ROOT / "deploy/nginx-spa.conf").read_text(encoding="utf-8")
    expected_map = (
        "map $request_uri $loom_ambiguous_path {\n"
        "    default 0;\n"
        "    ~*^[^?]*(?:%2f|%5c|\\x5c|//) 1;\n"
        '    "~*^/[^/?]*%[0-9a-f]{2}[^/?]*(?:/|[?]|$)" 1;\n'
        "    ~^/(?:dev|prod)(?:/|\\?|$) 0;\n"
        "    ~*^/(?:dev|prod)(?:/|\\?|$) 1;\n"
        "}\n"
    )

    assert expected_map in config
    assert "merge_slashes off;" in config
    assert "if ($loom_ambiguous_path) {\n        return 404;\n    }" in config
    assert "location ~ ^/(?:prod|dev)/assets/(.+)$" in config
    assert "location ~* ^/(?:prod|dev)(?:/|$) {\n        return 404;\n    }" in config


def test_staging_browser_route_smoke_uses_pinned_bundled_chromium() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    steps = workflow["jobs"]["smoke"]["steps"]

    setup = next(step for step in steps if step.get("name") == "Set up browser smoke Node")
    install = next(
        step for step in steps if step.get("name") == "Install pinned browser smoke dependencies"
    )
    browser = next(
        step
        for step in steps
        if step.get("name") == "Verify prefixed frontend routes mount in Chromium"
    )
    upload = next(
        step for step in steps if step.get("name") == "Upload frontend route browser trace"
    )

    assert setup["uses"] == (f"actions/setup-node@{_locked_action_sha('actions/setup-node')}")
    assert str(setup["with"]["node-version"]) == "20"
    assert setup["with"]["cache-dependency-path"] == "web/package-lock.json"
    assert install["working-directory"] == "web"
    assert "npm ci" in install["run"]
    assert "npx --no-install playwright install --with-deps chromium" in install["run"]
    assert "CI=true npm --prefix web run smoke:routes --" in browser["run"]
    assert "--route https://yylx.world/dev" in browser["run"]
    assert "--insecure-for-kind" in browser["run"]
    assert "--trace /tmp/loom-frontend-route-browser-trace.zip" in browser["run"]
    assert upload["if"] == "always()"
    assert upload["uses"] == (
        f"actions/upload-artifact@{_locked_action_sha('actions/upload-artifact')}"
    )
    assert upload["with"]["path"] == "/tmp/loom-frontend-route-browser-trace.zip"
    assert upload["with"]["if-no-files-found"] == "ignore"

    package = json.loads((REPO_ROOT / "web/package.json").read_text(encoding="utf-8"))
    lock = json.loads((REPO_ROOT / "web/package-lock.json").read_text(encoding="utf-8"))
    assert package["devDependencies"]["@playwright/test"] == "1.61.1"
    assert lock["packages"]["node_modules/@playwright/test"]["version"] == "1.61.1"
    assert lock["packages"]["node_modules/playwright"]["version"] == "1.61.1"
    assert lock["packages"]["node_modules/playwright-core"]["version"] == "1.61.1"


def test_staging_kind_smoke_keeps_admin_exchange_hidden_without_credentials() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    steps = workflow["jobs"]["smoke"]["steps"]
    names = [step.get("name") for step in steps]
    anonymous_index = names.index("Verify prefixed frontend routes mount in Chromium")
    anonymous_trace_index = names.index("Upload frontend route browser trace")
    deny_index = names.index("Verify staging admin exchange stays hidden in kind")
    assert anonymous_index < anonymous_trace_index < deny_index
    for forbidden_step in (
        "Verify authenticated staging admin browser surfaces",
        "Upload sanitized staging admin browser report",
        "Cleanup staging admin browser secret files",
    ):
        assert forbidden_step not in names
    assert "smoke:staging-admin" not in str(workflow)

    bootstrap = next(step for step in steps if step.get("name") == "Bootstrap namespace + Secrets")[
        "run"
    ]
    assert "/tmp/loom-staging-admin-token" not in bootstrap
    assert "$GITHUB_ENV" not in bootstrap

    cluster_config = next(
        step for step in steps if step.get("name") == "Generate cluster-config.toml"
    )["run"]
    assert 'runtime_environment = "development"' in cluster_config
    assert 'runtime_environment = "staging"' not in cluster_config
    assert 'frontend_environment = "development"' in cluster_config
    assert 'frontend_environment = "staging"' not in cluster_config

    deny_script = steps[deny_index]["run"]
    syntax = subprocess.run(
        ["bash", "-n"],
        input=deny_script,
        text=True,
        capture_output=True,
        check=False,
    )
    assert syntax.returncode == 0, syntax.stderr
    assert "--request POST" in deny_script
    assert "--data-binary '{'" in deny_script
    assert "https://yylx.world/dev/api/v1/auth/staging-admin-browser-session" in deny_script
    assert '[[ "$status" != "404" ]]' in deny_script
    assert '!= \'{"detail":"not found"}\'' in deny_script
    assert "grep -Eqi '^(location|set-cookie|x-loom-build-sha):'" in deny_script
    for forbidden in (
        "Authorization",
        "ADMIN_TOKEN",
        "smoke:staging-admin",
        "loom-staging-admin-browser-smoke.json",
    ):
        assert forbidden not in deny_script

    build = next(step for step in steps if step.get("name") == "Build images (parallel)")
    assert "checkout_sha=$(git rev-parse HEAD)" in build["run"]
    assert 'build_args+=(--build-arg "LOOM_BUILD_SHA=${checkout_sha}")' in build["run"]
    assert "org.opencontainers.image.revision" in build["run"]
    assert '[[ "$service_revision" == "$checkout_sha" ]]' in build["run"]

    adr = (REPO_ROOT / "docs/architecture/adr/independent-staging-rollout-runner.md").read_text(
        encoding="utf-8"
    )
    launch = (REPO_ROOT / "docs/runbooks/staging-launch.md").read_text(
        encoding="utf-8",
    )
    operator = (REPO_ROOT / "docs/runbooks/operator-runbook.md").read_text(
        encoding="utf-8",
    )
    assert "Broker-owned step 16" in adr
    assert "broker-owned step 16" in launch
    assert "Broker-owned step 16" in operator


def test_staging_admin_browser_smoke_is_bounded_and_secret_safe() -> None:
    package = json.loads((REPO_ROOT / "web/package.json").read_text(encoding="utf-8"))
    assert package["scripts"]["smoke:staging-admin"] == (
        "node scripts/staging-admin-browser-smoke.mjs"
    )
    assert package["scripts"]["test:staging-admin-browser-unit"] == (
        "vitest run scripts/staging-admin-browser-smoke.test.mjs"
    )

    smoke = (REPO_ROOT / "web/scripts/staging-admin-browser-smoke.mjs").read_text(
        encoding="utf-8",
    )
    assert "`${options.route}/api/v1/auth/logout`" in smoke
    assert "cleanup.auth_me_after_logout_status" in smoke
    assert "recordVideo: undefined" in smoke
    for api_path in (
        "/api/v1/admin/registration-requests?status=pending",
        "/api/v1/admin/team-registrations?status=pending",
        "/api/v1/admin/password-reset-requests?status=pending",
        "/api/v1/admin/teams",
        "/api/v1/invites?status=pending",
        "/api/v1/tokens",
        "/api/v1/admin/audit-events?limit=50",
        "/api/v1/rate-cards",
    ):
        assert api_path in smoke
    for event in (
        'page.on("console"',
        'page.on("pageerror"',
        'page.on("request"',
        'page.on("requestfinished"',
        'page.on("requestfailed"',
    ):
        assert event in smoke
    for query_name in (
        "registration-requests",
        "team-registrations",
        "password-reset-requests",
        "admin-teams",
        "invites",
        "api-tokens",
        "audit-events",
        "rate-cards",
    ):
        assert f'"{query_name}"' in smoke
    assert "await pageMonitor.waitForQuiet(options.timeoutMs)" in smoke
    assert smoke.index("await pageMonitor.waitForQuiet") < smoke.index("await page.close()")
    assert smoke.index("await page.close()") < smoke.index("pageMonitor.applyChecks(checks)")
    assert "name: auditIdentity.requestId" in smoke
    assert "name: `user:${auditIdentity.targetUserId}`" in smoke
    assert smoke.count("exact: true") >= 6
    assert "verifyAdminTabsAccessibility" in smoke
    for keyboard_key in ("ArrowRight", "ArrowLeft", "Home", "End"):
        assert f'"{keyboard_key}"' in smoke
    assert 'getAttribute("aria-controls")' in smoke
    assert 'getAttribute("role") === "tabpanel"' in smoke
    assert 'getAttribute("aria-labelledby") === tab.id' in smoke
    assert "checks.all_admin_tabs_operable =" in smoke
    assert "screenshot(" not in smoke
    assert "storageState" not in smoke


def test_staging_browser_route_smoke_waits_for_explicit_settled_state() -> None:
    main = (REPO_ROOT / "web/src/main.tsx").read_text(encoding="utf-8")
    smoke = (REPO_ROOT / "web/scripts/frontend-route-browser-smoke.mjs").read_text(encoding="utf-8")

    assert 'data-loom-mounted", "true"' in main
    assert 'data-loom-auth-settled", "true"' in main
    assert 'sessionStatus === "authenticated"' in main
    assert 'sessionStatus === "unavailable"' in main
    assert '? "error"' in main
    assert ': "anonymous"' in main
    assert 'waitUntil: "domcontentloaded"' in smoke
    assert "BLOCKING_ACTIVITY_QUIET_WINDOW_MS" in smoke
    assert "requestBlocksQuiescence" in smoke
    assert 'resourceType === "script"' in smoke
    assert "activeRequests.size === 0" in smoke
    assert "blocking browser activity did not become quiet" in smoke
    assert smoke.index("await page.close();") < smoke.index("const initialAnonymousAuthValid")
    assert smoke.index("await page.close();") < smoke.index("const observation = {")
    assert 'window.history.replaceState(null, "", directUrl)' in smoke
    assert "waitForTimeout(250)" not in smoke


def test_real_aws_s3_storage_smoke_is_not_a_pull_request_gate() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    jobs = workflow["jobs"]
    gate = jobs["staging-smoke-gate"]

    assert "smoke-storage-aws-s3" not in jobs
    assert set(gate["needs"]) == {"plan", "smoke", "system-smoke"}
    assert "ci-aws" not in str(workflow)


def test_staging_gate_consumes_manifest_owned_system_smoke_lane() -> None:
    workflow = _workflow(".github/workflows/staging-smoke.yml")
    jobs = workflow["jobs"]
    system_smoke = jobs["system-smoke"]
    scripts = "\n".join(step.get("run", "") for step in system_smoke["steps"])
    pytest_step = next(
        step
        for step in system_smoke["steps"]
        if step.get("name") == "Pytest — manifest-owned system smoke lane"
    )
    diagnostics_step = next(
        step
        for step in system_smoke["steps"]
        if step.get("name") == "Show system-smoke compose diagnostics"
    )
    cleanup_step = next(
        step
        for step in system_smoke["steps"]
        if step.get("name") == "Cleanup system-smoke compose stack"
    )

    assert system_smoke["needs"] == "plan"
    assert "needs.plan.outputs.required == 'true'" in system_smoke["if"]
    assert "uv sync --locked --all-packages --extra dev --extra cluster --extra rollout" in scripts
    assert "uv pip check --python .venv/bin/python" in scripts
    assert (
        "uv run --no-sync python scripts/component_ownership.py test-paths --lane system-smoke"
        in scripts
    )
    assert 'uv run --no-sync pytest --timeout=1200 "${test_paths[@]}"' in scripts
    assert "--profile worker logs --no-color --tail=300" in scripts
    assert "--profile worker down -v --remove-orphans" in scripts
    assert pytest_step["env"]["LOOM_SYSTEM_SMOKE_DIAGNOSTICS"] == (
        "${{ runner.temp }}/system-smoke-compose.log"
    )
    assert diagnostics_step["if"] == "failure()"
    assert diagnostics_step["env"]["LOOM_SYSTEM_SMOKE_DIAGNOSTICS"] == (
        "${{ runner.temp }}/system-smoke-compose.log"
    )
    assert 'cat "${LOOM_SYSTEM_SMOKE_DIAGNOSTICS}"' in diagnostics_step["run"]
    assert cleanup_step["if"] == "always()"


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

    assert "Install exact uv" in step_names
    assert "Set up Python 3.11" not in step_names
    assert "uv sync --locked --only-group ci-coverage --python 3.11" in run_blocks
    assert "uv run --no-sync coverage combine" in run_blocks
    assert "uv run --no-sync coverage xml" in run_blocks


def test_lint_and_static_does_not_restore_opaque_analysis_state() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    steps = workflow["jobs"]["lint-and-static"]["steps"]
    assert all(
        not str(step.get("uses", "")).startswith("actions/cache@") for step in steps
    )
    assert "Cache mypy" not in {step.get("name") for step in steps}
