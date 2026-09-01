from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path
from typing import Any

import pytest
import scripts.component_ownership as component_ownership
import yaml

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


OLDLAB_UV_MANIFEST = (
    "${{ startsWith(runner.name, 'oldlab5-kvm-') && 'http://127.0.0.1:8181/uv.ndjson' || '' }}"
)

GITHUB_HOSTED_CONTROL_JOBS = {
    ".github/workflows/ci.yml": {
        "workflow-plan",
        "fast-checks",
        "coverage-summary",
        "repository-checks",
    },
    ".github/workflows/images.yml": {"plan", "images-gate"},
    ".github/workflows/cluster-smoke.yml": {"plan", "cluster-smoke-gate"},
    ".github/workflows/staging-smoke.yml": {"plan", "staging-smoke-gate"},
}


def test_accelerated_workflows_use_local_uv_manifest_only_on_oldlab() -> None:
    workflow_paths = (
        ".github/workflows/ci.yml",
        ".github/workflows/cluster-smoke.yml",
        ".github/workflows/staging-smoke.yml",
    )

    for workflow_path in workflow_paths:
        setup_steps = [
            step
            for job in _workflow(workflow_path)["jobs"].values()
            for step in job.get("steps", [])
            if str(step.get("uses", "")).startswith("astral-sh/setup-uv@")
        ]
        assert setup_steps
        for step in setup_steps:
            assert step["with"]["manifest-file"] == OLDLAB_UV_MANIFEST


def test_planners_gates_publish_and_aggregation_stay_github_hosted() -> None:
    for workflow_path, job_ids in GITHUB_HOSTED_CONTROL_JOBS.items():
        jobs = _workflow(workflow_path)["jobs"]
        for job_id in job_ids:
            assert jobs[job_id]["runs-on"] == "ubuntu-latest"


def test_native_image_publish_jobs_stay_on_architecture_matched_github_hosts() -> None:
    jobs = _workflow(".github/workflows/images.yml")["jobs"]

    build_runs_on = jobs["build"]["runs-on"]
    assert "matrix.image == 'capacity-executor'" in build_runs_on
    assert "matrix.image == 'capacity-manager'" in build_runs_on
    assert "matrix.image == 'personal-dev-activation-agent'" in build_runs_on
    assert "matrix.image == 'personal-dev-builder'" in build_runs_on
    assert "matrix.image == 'personal-dev-scanner-cache'" in build_runs_on
    assert "matrix.image == 'pipeline-core-fixture'" in build_runs_on
    assert "matrix.image == 'pipeline-orchestrator'" in build_runs_on
    assert "ubuntu-24.04" in build_runs_on
    publish_runs_on = jobs["publish"]["runs-on"]
    assert "matrix.architecture == 'arm64'" in publish_runs_on
    assert "ubuntu-24.04-arm" in publish_runs_on
    assert "ubuntu-24.04" in publish_runs_on
    assert "vars." not in publish_runs_on
    assert jobs["publish-manifest"]["runs-on"] == "ubuntu-24.04"


def test_hosted_only_amd64_builds_bypass_live_lease_routes(tmp_path: Path) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    route_step = next(
        step
        for step in workflow["jobs"]["image-route"]["steps"]
        if step.get("name") == "Select native AMD64 image keys"
    )
    github_output = tmp_path / "github-output.txt"
    result = subprocess.run(
        ["bash"],
        input=route_step["run"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "NATIVE_BUILDS": json.dumps(
                [
                    {"image": "capacity-executor", "architecture": "amd64"},
                    {"image": "capacity-executor", "architecture": "arm64"},
                    {"image": "capacity-manager", "architecture": "amd64"},
                    {"image": "capacity-manager", "architecture": "arm64"},
                    {"image": "personal-dev-scanner-cache", "architecture": "amd64"},
                    {"image": "personal-dev-scanner-cache", "architecture": "arm64"},
                    {"image": "pipeline-core-fixture", "architecture": "amd64"},
                    {"image": "pipeline-core-fixture", "architecture": "arm64"},
                    {"image": "worker", "architecture": "amd64"},
                ]
            ),
            "GITHUB_OUTPUT": str(github_output),
        },
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(_github_output_value(github_output.read_text(), "job_keys")) == ["worker"]


def test_hosted_only_image_matrix_requires_no_live_lease_route(tmp_path: Path) -> None:
    workflow = _workflow(".github/workflows/images.yml")
    route_job = workflow["jobs"]["image-route"]
    route_step = next(
        step for step in route_job["steps"] if step.get("name") == "Select native AMD64 image keys"
    )
    resolve_step = next(
        step for step in route_job["steps"] if step.get("name") == "Resolve immutable assignments"
    )
    github_output = tmp_path / "github-output.txt"
    result = subprocess.run(
        ["bash"],
        input=route_step["run"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={
            **os.environ,
            "NATIVE_BUILDS": json.dumps(
                [
                    {"image": "capacity-executor", "architecture": "amd64"},
                    {"image": "capacity-executor", "architecture": "arm64"},
                    {"image": "capacity-manager", "architecture": "amd64"},
                    {"image": "capacity-manager", "architecture": "arm64"},
                    {"image": "pipeline-core-fixture", "architecture": "amd64"},
                    {"image": "pipeline-core-fixture", "architecture": "arm64"},
                ]
            ),
            "GITHUB_OUTPUT": str(github_output),
        },
        check=False,
    )

    assert result.returncode == 0, result.stderr
    output = github_output.read_text()
    assert json.loads(_github_output_value(output, "job_keys")) == []
    assert _github_output_value(output, "needs_route") == "false"
    assert resolve_step["if"] == "steps.keys.outputs.needs_route == 'true'"
    assert route_job["outputs"]["routes"] == "${{ steps.route.outputs.routes || '{}' }}"


def test_coverage_artifacts_map_hosted_and_oldlab_checkout_roots() -> None:
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    paths = config["tool"]["coverage"]["paths"]

    for path_group, source_root in (("source", "src"), ("packages", "packages")):
        assert paths[path_group] == [
            source_root,
            f"/home/runner/work/*/*/{source_root}",
            f"/opt/actions-runner/_work/*/*/{source_root}",
        ]


def test_source_workflows_share_native_run_identity() -> None:
    workflows = {path: _workflow(path) for path in GATE_CONTRACTS}
    common_run_names = {
        workflow["run-name"]
        for path, workflow in workflows.items()
        if path != ".github/workflows/images.yml"
    }

    assert len(common_run_names) == 1
    image_run_name = workflows[".github/workflows/images.yml"]["run-name"]
    assert "gate=trusted-publish / head={0} / base={1}" in image_run_name
    assert "inputs.trusted_publish == true" in image_run_name
    for run_name in (common_run_names.pop(), image_run_name):
        assert "28aa5257927a3468ebc35ec7f245fecaf3226dbf" not in run_name
        assert "' ' ||" not in run_name
        for mode in ("manual", "filtered", "full"):
            assert f"gate={mode} / head={{0}} / base={{1}}" in run_name
        for field in ("updated={2}", "action={3}", "label={4}", "labels={5}", "pull={6}"):
            assert field in run_name
        assert run_name.count("format('{0}{1}{2}{3}{4}{5}'") == 3

        assert "github.event.pull_request.draft" in run_name
        assert "github.event.action == 'converted_to_draft'" in run_name
        assert """fromJSON('["labeled","unlabeled"]')""" not in run_name
        assert "github.event.action == 'edited'" not in run_name
        assert "github.event.changes.base == null" not in run_name
        for label in (
            "ci:integration",
            "ci:integration-docker",
            "ci:images",
            "cluster-smoke",
            "staging-smoke",
            "ci:coverage-summary",
        ):
            assert label in run_name
            assert (
                run_name.count(f"contains(github.event.pull_request.labels.*.name, '{label}')") == 3
            )

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


def test_source_workflows_have_no_publisher_generation_contract() -> None:
    for workflow_path, (plan_job_id, plan_step_id) in SOURCE_PLAN_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        plan_job = workflow["jobs"][plan_job_id]
        plan_step = next(step for step in plan_job["steps"] if step.get("id") == plan_step_id)

        assert "publisher_active" not in plan_job["outputs"]
        assert plan_step["env"]["BASE_SHA"].startswith("${{ ")
        assert "TRUSTED_PROMOTION" not in plan_step["env"]
        assert "publisher_active" not in plan_step["run"]
        assert "authoritative-gates.yml" not in plan_step["run"]


def test_draft_events_finish_before_checkout_or_gate() -> None:
    for workflow_path, (plan_job_id, plan_step_id) in SOURCE_PLAN_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        jobs = workflow["jobs"]
        plan_job = jobs[plan_job_id]
        event_step = plan_job["steps"][0]
        checkout_step = next(
            step
            for step in plan_job["steps"]
            if str(step.get("uses", "")).startswith("actions/checkout@")
        )
        planner_step = next(step for step in plan_job["steps"] if step.get("id") == plan_step_id)
        gate_id = GATE_CONTRACTS[workflow_path][0]

        assert event_step["id"] == "event"
        assert _normalized_expression(event_step["env"]["FILTERED_EVENT"]) == (
            "${{ github.event_name == 'pull_request' && "
            "(github.event.pull_request.draft || "
            "github.event.action == 'converted_to_draft') }}"
        )
        assert "checkout_required=false" in event_step["run"]
        assert "gate_mode=filtered" in event_step["run"]
        assert checkout_step["if"] == "steps.event.outputs.checkout_required == 'true'"
        assert planner_step["if"] == "steps.event.outputs.checkout_required == 'true'"
        assert "needs." in jobs[gate_id]["if"]
        assert "outputs.gate_mode != 'filtered'" in jobs[gate_id]["if"]


def test_draft_classifier_emits_no_checkout_contract(tmp_path: Path) -> None:
    for workflow_path, (plan_job_id, _plan_step_id) in SOURCE_PLAN_CONTRACTS.items():
        event_step = _workflow(workflow_path)["jobs"][plan_job_id]["steps"][0]
        github_output = tmp_path / (Path(workflow_path).stem + "-output.txt")
        result = subprocess.run(
            ["bash"],
            input=event_step["run"],
            text=True,
            capture_output=True,
            env={
                **os.environ,
                "FILTERED_EVENT": "true",
                "GITHUB_OUTPUT": str(github_output),
            },
            check=False,
        )

        assert result.returncode == 0, (workflow_path, result.stderr)
        outputs = github_output.read_text(encoding="utf-8")
        assert "checkout_required=false" in outputs
        assert "gate_mode=filtered" in outputs
        assert "publisher_active" not in outputs


def test_source_gate_names_are_native_and_stable() -> None:
    for workflow_path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        workflow = _workflow(workflow_path)
        plan_job_id, _ = SOURCE_PLAN_CONTRACTS[workflow_path]
        plan_ref = f"needs.{plan_job_id}.outputs"
        expression = _normalized_expression(workflow["jobs"][gate_id]["name"])
        trusted_recovery = ""
        if workflow_path == ".github/workflows/images.yml":
            trusted_recovery = (
                "github.event_name == 'workflow_dispatch' && "
                "needs.plan.outputs.trusted_publish == 'true' && "
                "'images-gate-trusted-publish' || "
            )

        assert expression == _normalized_expression(
            "${{ "
            f"{trusted_recovery}"
            f"github.event_name == 'workflow_dispatch' && '{protected_name}-manual' || "
            f"github.event_name == 'push' && '{protected_name}-push' || "
            f"{plan_ref}.gate_mode == 'full' && '{protected_name}' || "
            f"'{protected_name}-filtered' "
            "}}"
        )
        assert "publisher_active" not in expression
        assert f"{protected_name}-attempt" not in expression


def test_push_aggregates_cannot_duplicate_protected_names_on_promotion_heads() -> None:
    for workflow_path, (gate_id, protected_name) in GATE_CONTRACTS.items():
        expression = _normalized_expression(_workflow(workflow_path)["jobs"][gate_id]["name"])

        assert f"github.event_name == 'push' && '{protected_name}-push'" in expression


def test_no_workflow_can_write_custom_authoritative_states() -> None:
    workflow_paths = sorted((REPO_ROOT / ".github/workflows").glob("*.yml"))
    for workflow_file in workflow_paths:
        workflow_path = workflow_file.relative_to(REPO_ROOT).as_posix()
        workflow = _workflow(workflow_path)
        workflow_source = workflow_file.read_text(encoding="utf-8")

        assert "scripts/ops/authoritative_gate.py" not in workflow_source
        assert "AUTHORITATIVE_CONTEXT" not in workflow_source
        workflow_permissions = workflow.get("permissions", {})
        assert isinstance(workflow_permissions, dict)
        if workflow_path == ".github/workflows/ci-runner-route-publisher.yml":
            assert workflow_permissions == {"contents": "read", "checks": "write"}
            assert "scripts/ops/ci_runner_route_publisher.py" in workflow_source
            continue
        assert workflow_permissions.get("checks") != "write"
        assert workflow_permissions.get("statuses") != "write"

        for job_name, job in workflow["jobs"].items():
            effective_permissions = job.get("permissions", workflow_permissions)
            assert isinstance(effective_permissions, dict), job_name
            assert effective_permissions.get("checks") != "write", job_name
            assert effective_permissions.get("statuses") != "write", job_name


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

    required_output = jobs["plan"]["outputs"]["required"]
    assert "steps.plan.outputs.required" in required_output
    assert "steps.event.outputs.required" in required_output
    assert set(jobs["build"]["needs"]) == {
        "plan",
        "image-route",
        "trivy-binary",
        "personal-dev-scanner-cache-assets",
    }
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
    contract = workflow["jobs"]["cluster-contract"]
    scripts = "\n".join(step.get("run", "") for step in contract["steps"])

    assert (
        "uv run --no-sync python scripts/component_ownership.py test-paths --lane cluster-smoke"
        in scripts
    )
    assert "uv sync --locked --all-packages --extra dev --extra cluster" in scripts
    assert "uv pip check --python .venv/bin/python" in scripts
    assert 'uv run --no-sync pytest "${test_paths[@]}"' in scripts
    assert contract["timeout-minutes"] <= 15


def test_images_workflow_uses_path_aware_matrix_plan() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]
    push_trigger = _workflow_on(workflow)["push"]

    assert "plan" in jobs
    assert "images" in jobs["plan"]["outputs"]
    assert "native_builds" in jobs["plan"]["outputs"]
    assert "stage1_images" not in jobs["plan"]["outputs"]
    build = jobs["build"]
    assert set(build["needs"]) == {
        "plan",
        "image-route",
        "trivy-binary",
        "personal-dev-scanner-cache-assets",
    }
    assert build["strategy"]["matrix"]["include"] == (
        "${{ fromJSON(needs.plan.outputs.native_builds) }}"
    )
    plan_script = "\n".join(step.get("run", "") for step in jobs["plan"]["steps"] if "run" in step)
    assert "scripts/component_ownership.py" in plan_script
    assert "plan-images" in plan_script
    assert push_trigger == {"branches": ["dev", "main"]}


def test_ci_push_safety_net_excludes_already_admitted_dev_merges() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    jobs = workflow["jobs"]

    assert _workflow_on(workflow)["push"] == {"branches": ["main"]}
    assert "refs/heads/dev" not in jobs["workflow-plan"].get("if", "")
    assert "refs/heads/dev" not in jobs["fast-checks"]["if"]
    assert "refs/heads/dev" not in jobs["repository-checks"]["if"]


def test_behavior_stage1_image_jobs_are_absent() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    jobs = workflow["jobs"]

    assert {"stage1-build", "stage1-publish", "stage1-publish-index"}.isdisjoint(jobs)
    assert "behavior-stage1-sim" not in str(workflow)


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
    native_matrix = json.loads(_github_output_value(output, "native_builds"))
    assert _github_output_value(output, "required") == "true"
    assert len(matrix) == 21
    assert {entry["image"] for entry in matrix} == {
        "agent-sandbox",
        "capacity-executor",
        "capacity-manager",
        "control-plane",
        "egress-xds",
        "execution-actuator",
        "execution-runtime",
        "family-orchestrator",
        "pipeline-orchestrator",
        "pipeline-core-fixture",
        "llm-gateway",
        "llm-gateway-sandbox",
        "personal-dev-activation-agent",
        "personal-dev-builder",
        "personal-dev-native-builder-agent",
        "personal-dev-scanner-cache",
        "rehearsal-postgres",
        "service",
        "staging-admin-browser-smoke",
        "web",
        "worker",
    }
    assert all(set(entry) == {"image", "image_name", "dockerfile", "context"} for entry in matrix)
    assert len(native_matrix) == 42
    assert {(entry["architecture"], entry["platform"]) for entry in native_matrix} == {
        ("amd64", "linux/amd64"),
        ("arm64", "linux/arm64"),
    }


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
        "capacity-executor",
        "capacity-manager",
        "worker",
        "service",
        "control-plane",
        "egress-xds",
        "execution-actuator",
        "execution-runtime",
        "family-orchestrator",
        "pipeline-orchestrator",
        "pipeline-core-fixture",
        "personal-dev-activation-agent",
        "personal-dev-builder",
        "personal-dev-native-builder-agent",
        "personal-dev-scanner-cache",
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
        "capacity-executor",
        "capacity-manager",
        "control-plane",
        "family-orchestrator",
        "pipeline-orchestrator",
        "llm-gateway",
        "personal-dev-activation-agent",
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
    build_step = next(
        step
        for step in build_steps
        if step.get("name") == "Build without registry or cache write authority"
    )
    build_script = build_step["run"]
    publish = workflow["jobs"]["publish"]

    assert "docker login" not in build_script
    assert "--push" not in build_script
    assert "--cache-to" not in build_script
    assert 'merge_group) image_tag="merge-group-${sha_short}"' in build_script
    assert "type=docker" in build_script
    assert ".docker.tar" in build_script
    assert "type=oci" not in build_script
    assert "type=registry" not in build_script
    assert "github.event_name == 'push'" in publish["if"]
    assert any(step.get("name") == "Log in to GHCR" for step in publish["steps"])


def test_untrusted_image_archives_are_scanned_job_local_and_never_uploaded() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    build = workflow["jobs"]["build"]
    steps = build["steps"]
    build_step = next(
        step
        for step in steps
        if step.get("name") == "Build without registry or cache write authority"
    )
    scan_step = next(step for step in steps if step.get("name") == "Scan native image archive")

    assert build["permissions"] == {"contents": "read"}
    assert "GITHUB_TOKEN" not in json.dumps(build_step)
    assert "secrets." not in json.dumps(build_step)
    assert "docker login" not in json.dumps(build)
    assert "ghcr.io" not in json.dumps(build)
    assert '"$EVENT_NAME" == "pull_request"' in build_step["run"]
    assert "candidate-${HEAD_SHA}-${ARCHITECTURE}" in build_step["run"]
    assert scan_step["env"]["ARCHIVE"].endswith(".docker.tar")
    assert '--input "$ARCHIVE"' in scan_step["run"]
    assert all("upload-artifact" not in step.get("uses", "") for step in steps)


def test_trusted_publisher_rebuilds_without_candidate_resolution() -> None:
    jobs = _workflow(".github/workflows/images.yml")["jobs"]
    publish = jobs["publish"]
    scripts = "\n".join(str(step.get("run", "")) for step in publish["steps"] if "run" in step)
    names = [step.get("name") for step in publish["steps"]]

    assert "resolve-candidate" not in jobs
    assert publish["needs"] == [
        "plan",
        "trivy-binary",
        "personal-dev-scanner-cache-assets",
    ]
    assert publish["strategy"]["matrix"]["include"] == (
        "${{ fromJSON(needs.plan.outputs.native_builds) }}"
    )
    assert "Build trusted image archive" in names
    assert "Download exact PR candidate archive" not in names
    assert "gh run download" not in scripts
    assert "candidate_artifact" not in str(publish)
    assert "verified-pr-candidate" not in str(publish)
    assert "needs.resolve-candidate" not in str(publish)


def test_release_images_are_scanned_attested_and_verified_before_manifest_join() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    trivy_binary = workflow["jobs"]["trivy-binary"]
    build = workflow["jobs"]["build"]
    publish = workflow["jobs"]["publish"]
    manifest = workflow["jobs"]["publish-manifest"]

    assert trivy_binary["needs"] == ["plan"]
    assert trivy_binary["permissions"] == {"contents": "read"}
    assert trivy_binary["runs-on"] == "ubuntu-24.04"
    assert "strategy" not in trivy_binary
    install = next(
        step
        for step in trivy_binary["steps"]
        if step.get("name") == "Install and record pinned Trivy binary"
    )
    upload = next(
        step
        for step in trivy_binary["steps"]
        if step.get("name") == "Upload exact verified Trivy binary"
    )
    assert "for architecture in amd64 arm64" in install["run"]
    assert "python3 scripts/install_trivy.py" in install["run"]
    assert '--architecture "$architecture"' in install["run"]
    assert "sha256sum --check trivy.sha256" in install["run"]
    assert upload["with"]["name"] == "trivy-binaries-run-${{ github.run_id }}"
    assert upload["with"]["overwrite"] is True
    assert build["needs"] == [
        "plan",
        "image-route",
        "trivy-binary",
        "personal-dev-scanner-cache-assets",
    ]
    assert publish["needs"] == [
        "plan",
        "trivy-binary",
        "personal-dev-scanner-cache-assets",
    ]

    build_step_names = [step.get("name") for step in build["steps"]]
    build_script = next(
        step["run"]
        for step in build["steps"]
        if step.get("name") == "Build without registry or cache write authority"
    )
    build_scan = next(
        step for step in build["steps"] if step.get("name") == "Scan native image archive"
    )
    assert "type=docker,dest=${archive}" in build_script
    assert "uses" not in build_scan
    assert build_scan["shell"] == "bash"
    assert build_scan["env"] == {
        "ARCHIVE": "/tmp/${{ matrix.image }}-${{ matrix.architecture }}.docker.tar",
        "REPORT": "/tmp/${{ matrix.image }}-${{ matrix.architecture }}.trivy.json",
        "IMAGE_NAME": "${{ matrix.image }}",
        "ARCHITECTURE": "${{ matrix.architecture }}",
    }
    assert build_scan["run"].strip() == (
        "set -euo pipefail\n"
        'trivy_bin="/tmp/loom-trivy-binaries/${ARCHITECTURE}/trivy"\n'
        "set +e\n"
        '"$trivy_bin" --config /tmp/loom-trivy-release.yaml image \\\n'
        '  --input "$ARCHIVE" \\\n'
        "  --format json \\\n"
        '  --output "$REPORT" \\\n'
        "  --ignorefile /tmp/loom-trivy-release.ignore.yaml \\\n"
        "  --show-suppressed \\\n"
        '  --cache-dir "$RUNNER_TEMP/loom-trivy-cache"\n'
        "scan_status=$?\n"
        "set -e\n"
        "if (( scan_status != 0 )); then\n"
        '  python3 scripts/summarize_trivy_report.py "$REPORT" || true\n'
        '  exit "$scan_status"\n'
        "fi\n"
        "python3 scripts/validate_trivy_release_report.py \\\n"
        '  --component "$IMAGE_NAME" \\\n'
        '  --architecture "$ARCHITECTURE" \\\n'
        '  --report "$REPORT" \\\n'
        "  --ignore-file /tmp/loom-trivy-release.ignore.yaml"
    )
    assert build_step_names.index("Build without registry or cache write authority") < (
        build_step_names.index("Scan native image archive")
    )
    assert build_step_names.index("Download exact verified Trivy binary") < (
        build_step_names.index("Verify distributed Trivy binary")
    )
    assert build_step_names.index("Verify distributed Trivy binary") < (
        build_step_names.index("Scan native image archive")
    )

    publish_names = [step.get("name") for step in publish["steps"]]
    trusted_scan = next(
        step for step in publish["steps"] if step.get("name") == "Scan trusted image archive"
    )
    architecture_publish = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Publish scanned architecture image"
    )
    architecture_attestation = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Attest published architecture digest"
    )
    assert "uses" not in trusted_scan
    assert trusted_scan["shell"] == "bash"
    assert trusted_scan["env"] == {
        "ARCHIVE": ("/tmp/${{ matrix.image }}-${{ matrix.architecture }}.release.docker.tar"),
        "REPORT": ("/tmp/${{ matrix.image }}-${{ matrix.architecture }}.release.trivy.json"),
        "IMAGE_NAME": "${{ matrix.image }}",
        "ARCHITECTURE": "${{ matrix.architecture }}",
    }
    assert trusted_scan["run"] == build_scan["run"]
    assert architecture_publish["id"] == "architecture-publish"
    push_command = 'push_output=$(scripts/ops/docker_push_with_retry.sh "$target")'
    assert push_command in architecture_publish["run"]
    assert "subject_name=$image" in architecture_publish["run"]
    assert "subject_digest=$digest" in architecture_publish["run"]
    push_tail = architecture_publish["run"].split(push_command, maxsplit=1)[1]
    assert 'imagetools inspect --raw "${image}@${digest}"' in push_tail
    assert 'imagetools inspect "$target"' not in push_tail
    assert architecture_attestation["uses"].startswith("actions/attest-build-provenance@")
    assert architecture_attestation["with"]["predicate-type"] == ("https://slsa.dev/provenance/v1")
    assert architecture_attestation["with"]["push-to-registry"] is True
    assert publish_names.index("Scan trusted image archive") < publish_names.index(
        "Record trusted scan digest"
    )
    assert publish_names.index("Record trusted scan digest") < publish_names.index(
        "Publish scanned architecture image"
    )
    assert publish_names.index("Publish scanned architecture image") < publish_names.index(
        "Attest published architecture digest"
    )
    assert publish_names.index("Attest published architecture digest") < publish_names.index(
        "Verify published architecture attestation"
    )

    manifest_names = [step.get("name") for step in manifest["steps"]]
    resolve = next(
        step["run"]
        for step in manifest["steps"]
        if step.get("name") == "Verify architecture attestations"
    )
    join = next(
        step["run"]
        for step in manifest["steps"]
        if step.get("name") == "Join verified native image manifest"
    )
    final_attestation = next(
        step for step in manifest["steps"] if step.get("name") == "Attest published manifest digest"
    )
    assert "gh attestation verify" in resolve
    assert "--signer-workflow" in resolve
    assert "--source-digest" in resolve
    assert "--source-ref" in resolve
    assert "--deny-self-hosted-runners" in resolve
    assert '"${image}@${amd64_digest}"' in join
    assert '"${image}@${arm64_digest}"' in join
    assert final_attestation["uses"].startswith("actions/attest-build-provenance@")
    assert final_attestation["with"]["push-to-registry"] is True
    assert manifest_names.index("Verify architecture attestations") < manifest_names.index(
        "Join verified native image manifest"
    )
    assert manifest_names.index("Attest published manifest digest") < manifest_names.index(
        "Verify published manifest attestation"
    )
    assert manifest_names.index("Verify published manifest attestation") < (
        manifest_names.index("Publish verified manifest tags")
    )


def test_release_architecture_records_are_exact_and_trusted_rebuild_only() -> None:
    jobs = _workflow(".github/workflows/images.yml")["jobs"]
    publish = jobs["publish"]
    manifest = jobs["publish-manifest"]
    publish_names = [step.get("name") for step in publish["steps"]]

    predicate = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Prepare architecture release predicate"
    )
    verify = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Verify published architecture attestation"
    )
    record = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Record verified architecture evidence"
    )
    validate_record = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Validate verified architecture evidence"
    )
    upload = next(
        step
        for step in publish["steps"]
        if step.get("name") == "Upload verified architecture evidence"
    )
    for step in (predicate, verify, record):
        assert "--build-mode" in step["run"]
        assert "trusted-rebuild" in step["run"]
        assert "--candidate-" not in step["run"]
        assert "verified-pr-candidate" not in step["run"]
    assert publish_names.index("Verify published architecture attestation") < (
        publish_names.index("Record verified architecture evidence")
    )
    assert publish_names.index("Record verified architecture evidence") < (
        publish_names.index("Validate verified architecture evidence")
    )
    assert publish_names.index("Validate verified architecture evidence") < (
        publish_names.index("Upload verified architecture evidence")
    )
    assert "validate-architecture-record" in validate_record["run"]
    assert upload["uses"] == (
        f"actions/upload-artifact@{_locked_action_sha('actions/upload-artifact')}"
    )
    assert upload["with"] == {
        "name": (
            "image-release-record-${{ matrix.image }}-${{ matrix.architecture }}-"
            "run-${{ github.run_id }}-attempt-${{ github.run_attempt }}"
        ),
        "path": (
            "/tmp/loom-image-release-records/${{ matrix.image }}-${{ matrix.architecture }}.json"
        ),
        "if-no-files-found": "error",
        "retention-days": 1,
    }

    downloads = [
        step
        for step in manifest["steps"]
        if str(step.get("name", "")).startswith("Download exact ")
        and str(step.get("name", "")).endswith(" architecture evidence")
    ]
    validate = next(
        step
        for step in manifest["steps"]
        if step.get("name") == "Validate exact architecture evidence"
    )
    assert len(downloads) == 2
    expected_downloads = {
        "Download exact AMD64 architecture evidence": (
            "image-release-record-${{ matrix.image }}-amd64-"
            "run-${{ github.run_id }}-attempt-${{ github.run_attempt }}"
        ),
        "Download exact ARM64 architecture evidence": (
            "image-release-record-${{ matrix.image }}-arm64-"
            "run-${{ github.run_id }}-attempt-${{ github.run_attempt }}"
        ),
    }
    for download in downloads:
        architecture = "amd64" if "AMD64" in download["name"] else "arm64"
        assert download["uses"] == (
            f"actions/download-artifact@{_locked_action_sha('actions/download-artifact')}"
        )
        assert download["with"] == {
            "name": expected_downloads[download["name"]],
            "path": f"/tmp/loom-image-release-artifacts/{architecture}",
        }

    concrete_names = {
        template.replace("${{ matrix.image }}", image)
        for template in expected_downloads.values()
        for image in ("llm-gateway", "llm-gateway-sandbox")
    }
    assert len(concrete_names) == 4
    assert "validate-architecture-records" in validate["run"]
    assert "--records-dir /tmp/loom-image-release-artifacts" in validate["run"]
    verify_records = next(
        step for step in manifest["steps"] if step.get("name") == "Verify architecture attestations"
    )
    assert (
        'record="/tmp/loom-image-release-artifacts/${architecture}/'
        '${IMAGE_NAME}-${architecture}.json"' in verify_records["run"]
    )
    assert set(manifest["permissions"]) >= {"actions", "attestations", "contents"}


def test_manifest_digest_is_captured_once_and_tags_follow_verification() -> None:
    manifest = _workflow(".github/workflows/images.yml")["jobs"]["publish-manifest"]
    names = [step.get("name") for step in manifest["steps"]]
    join = next(
        step
        for step in manifest["steps"]
        if step.get("name") == "Join verified native image manifest"
    )
    script = join["run"]

    assert '--tag "${image}:manifest-${HEAD_SHA}"' in script
    assert "docker buildx imagetools create" in script
    assert "--progress plain" in script
    assert "create_output=" not in script
    assert "pushing manifest for" not in script
    assert "imagetools inspect --raw" in script
    assert '"${image}:manifest-${HEAD_SHA}" > "$tag_manifest"' in script
    assert 'manifest_digest="sha256:$(sha256sum "$tag_manifest"' in script
    assert 'imagetools inspect --raw "${image}@${manifest_digest}"' in script
    assert 'cmp --silent "$tag_manifest" "$digest_manifest"' in script
    assert "ci_image_release_evidence.py validate-manifest" in script
    assert '--manifest "/tmp/loom-image-manifest.json"' in script
    assert "python3 - <<'PY'" not in script
    assert 'imagetools inspect "${image}:manifest-${HEAD_SHA}"' not in script
    assert names.index("Verify published manifest attestation") < names.index(
        "Publish verified manifest tags"
    )
    publish_tags = next(
        step["run"]
        for step in manifest["steps"]
        if step.get("name") == "Publish verified manifest tags"
    )
    assert "scripts/ci_registry_readback.py digest" in publish_tags
    assert "--attempts 6 --delay-seconds 2" in publish_tags


def test_release_record_helper_rejects_incomplete_workflow_handoff(tmp_path: Path) -> None:
    records = tmp_path / "records"
    records.mkdir()
    (records / "amd64").mkdir()
    (records / "amd64" / "capacity-manager-amd64.json").write_text("{}\n", encoding="utf-8")
    result = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts/ci_image_release_evidence.py"),
            "validate-architecture-records",
            "--repository",
            "qianyi-sun/loom",
            "--ref-name",
            "dev",
            "--head-sha",
            "a" * 40,
            "--tree-sha",
            "b" * 40,
            "--run-id",
            "123",
            "--run-attempt",
            "2",
            "--event-name",
            "push",
            "--repository-id",
            "123456789",
            "--repository-owner-id",
            "987654321",
            "--runner-environment",
            "github-hosted",
            "--image",
            "capacity-manager",
            "--image-name",
            "loom-capacity-manager",
            "--dockerfile",
            "deploy/Dockerfile.capacity-manager",
            "--build-context",
            ".",
            "--records-dir",
            str(records),
        ],
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "exactly the expected architecture files" in result.stderr


def test_pr_image_archive_stays_job_local_and_is_not_release_input() -> None:
    jobs = _workflow(".github/workflows/images.yml")["jobs"]
    build = jobs["build"]
    publish = jobs["publish"]
    build_scan = next(
        step["run"] for step in build["steps"] if step.get("name") == "Scan native image archive"
    )
    publish_script = next(
        step["run"]
        for step in publish["steps"]
        if step.get("name") == "Publish scanned architecture image"
    )

    assert "resolve-candidate" not in jobs
    assert '--input "$ARCHIVE"' in build_scan
    assert all("upload-artifact" not in step.get("uses", "") for step in build["steps"])
    assert publish["strategy"]["matrix"]["include"] == (
        "${{ fromJSON(needs.plan.outputs.native_builds) }}"
    )
    assert 'docker tag "$local_image" "$target"' in publish_script
    assert 'scripts/ops/docker_push_with_retry.sh "$target"' in publish_script
    assert ".release.docker.tar" in publish_script
    assert 'docker load --input "$archive"' in publish_script


def test_all_release_child_pushes_use_the_bounded_observable_retry_helper() -> None:
    jobs = _workflow(".github/workflows/images.yml")["jobs"]
    architecture_push = next(
        step["run"]
        for step in jobs["publish"]["steps"]
        if step.get("name") == "Publish scanned architecture image"
    )
    expected = 'push_output=$(scripts/ops/docker_push_with_retry.sh "$target")'
    assert expected in architecture_push


def test_manifest_image_build_and_publish_pass_exact_full_head_sha() -> None:
    workflow = _workflow(".github/workflows/images.yml")
    expected_steps = {
        "build": "Build without registry or cache write authority",
        "publish": "Build trusted image archive",
    }

    for job_name, step_name in expected_steps.items():
        step = next(
            step for step in workflow["jobs"][job_name]["steps"] if step.get("name") == step_name
        )
        script = step["run"]
        if job_name == "build":
            assert "github.event.pull_request.head.sha" in step["env"]["HEAD_SHA"]
        else:
            assert step["env"]["HEAD_SHA"] == "${{ github.sha }}"
        assert step["env"]["BUILD_CONTEXT"] == "${{ matrix.context }}"
        assert '--build-arg "LOOM_BUILD_SHA=${HEAD_SHA}"' in script
        if job_name == "build":
            assert 'build_args+=("$BUILD_CONTEXT")' in script
            context_marker = 'build_args+=("$BUILD_CONTEXT")'
        else:
            assert '"$BUILD_CONTEXT"' in script
            context_marker = '"$BUILD_CONTEXT"'
        assert script.index("LOOM_BUILD_SHA=${HEAD_SHA}") < script.rindex(context_marker)
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
            {"CONTRACT_RESULT": "skipped"},
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            {"SYSTEM_SMOKE_RESULT": "skipped"},
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
            ["CONTRACT_RESULT"],
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            ["SYSTEM_SMOKE_RESULT"],
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
    (
        "event_name",
        "required",
        "build_result",
        "publish_result",
        "manifest_result",
    ),
    [
        ("pull_request", "true", "success", "skipped", "skipped"),
        ("merge_group", "true", "success", "skipped", "skipped"),
        ("workflow_dispatch", "true", "success", "skipped", "skipped"),
        ("push", "true", "skipped", "success", "success"),
        ("pull_request", "false", "skipped", "skipped", "skipped"),
    ],
)
def test_images_gate_separates_untrusted_build_from_trusted_publish(
    event_name: str,
    required: str,
    build_result: str,
    publish_result: str,
    manifest_result: str,
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
            "MANIFEST_RESULT": manifest_result,
        },
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("event_name", "required", "release_result", "expected_returncode"),
    [
        ("push", "true", "success", 0),
        ("push", "true", "skipped", 1),
        ("push", "false", "skipped", 0),
        ("pull_request", "true", "skipped", 0),
    ],
)
def test_images_gate_requires_personal_release_only_for_protected_selected_publish(
    event_name: str,
    required: str,
    release_result: str,
    expected_returncode: int,
) -> None:
    selected = json.dumps(
        [
            {"image": "service"},
            {"image": "web"},
            {"image": "personal-dev-builder"},
            {"image": "personal-dev-activation-agent"},
            {"image": "personal-dev-native-builder-agent"},
            {"image": "personal-dev-scanner-cache"},
        ],
        separators=(",", ":"),
    )
    protected_publish = event_name == "push" and required == "true"
    result = subprocess.run(
        ["bash"],
        input=_gate_script(".github/workflows/images.yml", "images-gate"),
        text=True,
        capture_output=True,
        env={
            "EVENT_NAME": event_name,
            "TRUSTED_PUBLISH": "false",
            "PLAN_RESULT": "success",
            "GATE_MODE": "full",
            "REQUIRED": required,
            "BUILD_RESULT": "skipped" if protected_publish or required == "false" else "success",
            "PUBLISH_RESULT": "success" if protected_publish else "skipped",
            "MANIFEST_RESULT": "success" if protected_publish else "skipped",
            "PERSONAL_DEV_RELEASE_RESULT": release_result,
            "STANDARD_IMAGES": selected,
        },
        check=False,
    )

    assert result.returncode == expected_returncode, result.stderr


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


@pytest.mark.parametrize(
    ("docs_only", "go_result", "accepted"),
    [
        ("true", "skipped", True),
        ("true", "success", True),
        ("true", "failure", False),
        ("true", "cancelled", False),
        ("false", "success", True),
        ("false", "skipped", False),
        ("false", "failure", False),
        ("false", "cancelled", False),
    ],
)
def test_repository_checks_enforces_docs_only_go_result_semantics(
    docs_only: str,
    go_result: str,
    accepted: bool,
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
            "GO_RESULT": go_result,
            "DOCS_ONLY": docs_only,
            "INTEGRATION_SELECTED": "false",
            "INTEGRATION_RESULT": "skipped",
            "DOCKER_SELECTED": "false",
            "DOCKER_RESULT": "skipped",
            "COVERAGE_SELECTED": "false",
            "COVERAGE_RESULT": "skipped",
            "WEB_SELECTED": "false",
            "WEB_RESULT": "skipped",
        },
        check=False,
    )

    assert (result.returncode == 0) is accepted, (
        docs_only,
        go_result,
        result.stderr,
    )


def test_optional_validation_workflows_have_stable_gate_contexts() -> None:
    contracts = {
        ".github/workflows/images.yml": (
            "images-gate",
            "images-gate",
            {
                "build": "BUILD_RESULT",
                "publish": "PUBLISH_RESULT",
                "publish-manifest": "MANIFEST_RESULT",
                "personal-dev-trusted-release": "PERSONAL_DEV_RELEASE_RESULT",
            },
        ),
        ".github/workflows/cluster-smoke.yml": (
            "cluster-smoke-gate",
            "cluster-smoke-gate",
            {"cluster-contract": "CONTRACT_RESULT"},
        ),
        ".github/workflows/staging-smoke.yml": (
            "staging-smoke-gate",
            "staging-smoke-gate",
            {"system-smoke": "SYSTEM_SMOKE_RESULT"},
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
        "nebius-iac",
        "tests-root",
        "tests-packages",
    }
    assert "gate_mode == 'full'" in jobs["fast-checks"]["if"]
    assert "gate_mode == 'preflight'" not in jobs["fast-checks"]["if"]
    assert set(jobs["integration"]["needs"]) == {"workflow-plan", "ci-route"}
    assert set(jobs["integration-docker"]["needs"]) == {"workflow-plan", "ci-route"}
    assert "docs_only != 'true'" in jobs["go-checks"]["if"]
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
    for job_name in (
        "lint-and-static",
        "tests-root",
        "tests-packages",
        "runtime-payload",
    ):
        assert set(jobs[job_name]["needs"]) == {"workflow-plan", "ci-route"}
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

    assert set(jobs["web-checks"]["needs"]) == {"workflow-plan", "ci-route"}
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
    assert any(auth_path in shard and schema_path in shard for shard in integration_shards)
    assert integration_paths.index(auth_path) < integration_paths.index(schema_path)
    integration_script = "\n".join(step.get("run", "") for step in jobs["integration"]["steps"])
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


def test_root_test_shard_timeout_has_bounded_growth_headroom() -> None:
    workflow = _workflow(".github/workflows/ci.yml")

    timeout_minutes = workflow["jobs"]["tests-root"]["timeout-minutes"]

    assert 25 <= timeout_minutes <= 45


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


def test_protected_workflows_cancel_superseded_pr_runs() -> None:
    for workflow_path in GATE_CONTRACTS:
        workflow = _workflow(workflow_path)
        cancel = workflow["concurrency"]["cancel-in-progress"]

        assert _normalized_expression(cancel) == "${{ github.event_name == 'pull_request' }}"


def test_protected_workflows_share_one_per_pr_admission_slot() -> None:
    for workflow_path in GATE_CONTRACTS:
        workflow = _workflow(workflow_path)
        group = workflow["concurrency"]["group"]

        assert "github.event.pull_request.number || github.ref" in group
        assert "admission" not in group
        assert "background" not in group
        assert "github.event.action" not in group


def test_staging_active_rendered_images_are_covered_by_manifest_matrix() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "loom_cli",
            "cluster",
            "render",
            "--config",
            "deploy/environments/staging.multinode.cluster.toml",
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
            if not isinstance(image, str):
                continue
            image_name = image.rsplit("/", 1)[-1].split(":", 1)[0]
            if image_name.startswith("loom-"):
                active_images.add(image_name)

    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
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
        "    ~^/(?:dev|prod|staging)(?:/|\\?|$) 0;\n"
        "    ~*^/(?:dev|prod|staging)(?:/|\\?|$) 1;\n"
        "}\n"
    )

    assert expected_map in config
    assert "merge_slashes off;" in config
    assert "if ($loom_ambiguous_path) {\n        return 404;\n    }" in config
    assert "location = /staging {\n        return 308 /staging/$is_args$args;\n    }" in config
    assert "location ~ ^/(?:prod|dev|staging)/assets/(.+)$" in config
    assert ("location ~* ^/(?:.+/)+assets(?:/|$) {\n        return 404;\n    }") in config
    assert ("location ~* ^/(?:prod|dev|staging)(?:/|$) {\n        return 404;\n    }") in config


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
    assert set(gate["needs"]) == {"plan", "system-smoke"}
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
    cleanup_script = " ".join(cleanup_step["run"].replace("\\\n", " ").split())

    assert set(system_smoke["needs"]) == {"plan", "staging-route"}
    assert "needs.plan.outputs.required == 'true'" in system_smoke["if"]
    assert "uv sync --locked --all-packages --extra dev --extra cluster --extra rollout" in scripts
    assert "uv pip check --python .venv/bin/python" in scripts
    assert (
        "uv run --no-sync python scripts/component_ownership.py test-paths --lane system-smoke"
        in scripts
    )
    assert 'uv run --no-sync pytest --timeout=1200 "${test_paths[@]}"' in scripts
    assert (
        "--profile worker --profile task-image-builder down -v --remove-orphans" in cleanup_script
    )
    assert pytest_step["env"]["LOOM_SYSTEM_SMOKE_DIAGNOSTICS"] == (
        "${{ runner.temp }}/system-smoke-compose.log"
    )
    assert diagnostics_step["if"] == "failure()"
    assert diagnostics_step["env"]["LOOM_SYSTEM_SMOKE_DIAGNOSTICS"] == (
        "${{ runner.temp }}/system-smoke-compose.log"
    )
    assert 'cat "${LOOM_SYSTEM_SMOKE_DIAGNOSTICS}"' in diagnostics_step["run"]
    assert cleanup_step["if"] == "always()"
    assert cleanup_step["env"]["LOOM_WORKER_TOKEN"] == "unused-cleanup-token"
    assert cleanup_step["env"]["LOOM_TASK_IMAGE_BUILDER_TOKEN"] == ("unused-cleanup-builder-token")


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
    assert all(not str(step.get("uses", "")).startswith("actions/cache@") for step in steps)
    assert "Cache mypy" not in {step.get("name") for step in steps}
