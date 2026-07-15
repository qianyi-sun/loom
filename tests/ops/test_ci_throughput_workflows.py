from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest
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
    assert len(matrix) == 9
    assert {entry["image"] for entry in matrix} == {
        "agent-sandbox",
        "control-plane",
        "egress-xds",
        "family-orchestrator",
        "llm-gateway",
        "llm-gateway-sandbox",
        "service",
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
        "web",
        "llm-gateway-sandbox",
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
            {"SMOKE_RESULT": "skipped"},
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
        (
            ".github/workflows/cluster-smoke.yml",
            "cluster-smoke-gate",
            ["SMOKE_RESULT"],
        ),
        (
            ".github/workflows/staging-smoke.yml",
            "staging-smoke-gate",
            ["SMOKE_RESULT"],
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
            {"smoke": "SMOKE_RESULT"},
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

    assert 'pull_request)\n    git diff --name-only "$BASE_SHA...$HEAD_SHA"' in plan_script
    assert 'merge_group)\n    git diff --name-only "$BASE_SHA" "$HEAD_SHA"' in plan_script
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
    assert "--route staging=https://yylx.world/dev" in script
    assert "--probe web_500=500=http://127.0.0.1:18082/dev/security-header-5xx-probe" in script
    assert "--web-origin-only" in script
    assert "index.html.security-header-smoke" in script
    assert "trap cleanup_security_5xx_probe EXIT" in script
    assert "trap - EXIT" in script


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


def test_staging_browser_route_smoke_waits_for_explicit_settled_state() -> None:
    main = (REPO_ROOT / "web/src/main.tsx").read_text(encoding="utf-8")
    smoke = (REPO_ROOT / "web/scripts/frontend-route-browser-smoke.mjs").read_text(encoding="utf-8")

    assert 'data-loom-mounted", "true"' in main
    assert 'data-loom-auth-settled", "true"' in main
    assert 'isAuthenticated ? "authenticated" : "anonymous"' in main
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
    assert set(gate["needs"]) == {"plan", "smoke"}
    assert "ci-aws" not in str(workflow)


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
    assert cache_step["uses"] == f"actions/cache@{_locked_action_sha('actions/cache')}"
    assert cache_step["with"]["path"] == ".mypy_cache"
