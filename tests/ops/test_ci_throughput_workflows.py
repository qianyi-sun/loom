"""Single-planner CI selection, reusable lanes, and executable final admission contracts."""

from __future__ import annotations

import json
import os
import subprocess
import tomllib
from pathlib import Path
from typing import Any

import pytest
import scripts.component_ownership as component_ownership
import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
CI_WORKFLOW = ".github/workflows/ci.yml"
OPTIONAL_LANES = {
    "INTEGRATION": ("integration", "integration"),
    "DOCKER": ("integration-docker", "integration_docker"),
    "WEB": ("web-checks", "web_checks"),
    "IMAGES": ("images", "images"),
    "CLUSTER": ("cluster-smoke", "cluster_smoke"),
    "STAGING": ("staging-smoke", "staging_smoke"),
}


def _workflow(path: str) -> dict[str, Any]:
    return yaml.safe_load((REPO_ROOT / path).read_text(encoding="utf-8"))


def _workflow_on(workflow: dict[str, Any]) -> dict[str, Any]:
    # PyYAML treats unquoted GitHub Actions key `on` as YAML 1.1 bool.
    return workflow.get("on", workflow.get(True))


def _normalized_expression(value: str) -> str:
    return " ".join(value.split())


def test_coverage_artifacts_map_hosted_checkout_roots() -> None:
    config = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    paths = config["tool"]["coverage"]["paths"]

    for path_group, source_root in (("source", "src"), ("packages", "packages")):
        assert paths[path_group] == [
            source_root,
            f"/home/runner/work/*/*/{source_root}",
        ]


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
    assert "not legacy_pool" in scripts
    assert '"${test_paths[@]}"' in scripts


def test_nebius_iac_validates_aliased_provider_through_module_tests() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    steps = workflow["jobs"]["nebius-iac"]["steps"]
    script = next(
        step["run"]
        for step in steps
        if step.get("name") == "Validate pinned Nebius module and stack"
    )

    module_command = "terraform -chdir=deploy/terraform/nebius/modules/execution-target"
    assert f"{module_command} test" in script
    assert f"{module_command} validate" not in script
    assert "terraform -chdir=deploy/terraform/nebius/stack validate" in script


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
    root_policy = manifest.test_shard_policy("tests-root")
    assert root_policy is not None
    root_shards = [
        set(
            component_ownership.shard_paths(
                root_paths,
                shard_index=shard["shard_index"],
                shard_count=len(root_matrix),
                strategy=root_policy.strategy,
                salt=root_policy.salt,
                pins=root_policy.pins,
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
    integration_policy = manifest.test_shard_policy("integration")
    assert integration_policy is not None
    integration_shards = [
        component_ownership.shard_paths(
            integration_paths,
            shard_index=shard["shard_index"],
            shard_count=len(integration_matrix),
            strategy=integration_policy.strategy,
            salt=integration_policy.salt,
            pins=integration_policy.pins,
        )
        for shard in integration_matrix
    ]
    assert set(integration_shards[0]).isdisjoint(integration_shards[1])
    assert set().union(*map(set, integration_shards)) == set(integration_paths)
    assert {
        "tests/integration/test_cp_step_tokens.py",
        "tests/integration/test_migration_task_set_materialization_jobs.py",
    } <= set(integration_shards[1])
    auth_path = "tests/integration/test_username_password_auth.py"
    schema_path = "tests/integration/test_username_password_schema.py"
    assert any(auth_path in shard and schema_path in shard for shard in integration_shards)
    assert integration_paths.index(auth_path) < integration_paths.index(schema_path)
    integration_script = "\n".join(step.get("run", "") for step in jobs["integration"]["steps"])
    assert "--shard-strategy" not in integration_script

    root_script = "\n".join(step.get("run", "") for step in jobs["tests-root"]["steps"])
    assert "--durations=25" in root_script

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


def test_fast_checks_only_aggregates_functional_results() -> None:
    jobs = _workflow(CI_WORKFLOW)["jobs"]
    fast = jobs["fast-checks"]
    assert set(fast["needs"]) == {
        "workflow-plan", "locked-environments", "lint-and-static", "runtime-payload",
        "nebius-iac", "tests-root", "tests-packages",
    }
    assert "always()" in fast["if"]
    assert all("uses" not in step for step in fast["steps"])
    scripts = "\n".join(step.get("run", "") for step in fast["steps"])
    assert "coverage" not in scripts
    assert "uv sync" not in scripts


def test_combined_coverage_summary_is_opt_in() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    coverage_summary_if = workflow["jobs"]["coverage-summary"]["if"]

    assert "needs.workflow-plan.outputs.coverage_summary == 'true'" in coverage_summary_if


def test_optional_coverage_report_has_no_merge_authority_or_threshold() -> None:
    workflow = _workflow(CI_WORKFLOW)
    jobs = workflow["jobs"]
    gate = jobs["repository-checks"]
    assert "coverage-summary" not in gate["needs"]
    assert "COVERAGE_RESULT" not in gate["steps"][0]["env"]
    assert "--fail-under" not in json.dumps(workflow)
    for job_name in ("tests-root", "tests-packages", "integration"):
        uploads = [step for step in jobs[job_name]["steps"]
                   if "upload-artifact" in step.get("uses", "")]
        assert uploads
        assert jobs[job_name]["env"]["COVERAGE_ENABLED"] == "${{ needs.workflow-plan.outputs.coverage_summary }}"
        assert all("env.COVERAGE_ENABLED == 'true'" in step["if"] for step in uploads)


def test_lint_and_static_does_not_restore_opaque_analysis_state() -> None:
    workflow = _workflow(".github/workflows/ci.yml")
    steps = workflow["jobs"]["lint-and-static"]["steps"]
    assert all(not str(step.get("uses", "")).startswith("actions/cache@") for step in steps)
    assert "Cache mypy" not in {step.get("name") for step in steps}


def _gate_environment() -> dict[str, str]:
    env = {
        **os.environ,
        "PLAN_RESULT": "success",
        "GATE_MODE": "full",
        "DOCS_ONLY": "false",
        "FAST_RESULT": "success",
        "GO_RESULT": "success",
    }
    for prefix in OPTIONAL_LANES:
        env[f"{prefix}_SELECTED"] = "false"
        env[f"{prefix}_RESULT"] = "skipped"
    return env


def _run_gate(env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash"], input=_gate_script(CI_WORKFLOW, "repository-checks"),
        text=True, capture_output=True, env=env, check=False,
    )


def test_one_planner_calls_reusable_lanes_and_one_final_result() -> None:
    jobs = _workflow(CI_WORKFLOW)["jobs"]
    gate = jobs["repository-checks"]
    assert "always()" in gate["if"]
    assert set(gate["needs"]) == {
        "workflow-plan", "fast-checks", "go-checks",
        *(job for job, _ in OPTIONAL_LANES.values()),
    }
    gate_env = gate["steps"][0]["env"]
    for prefix, (job, selector) in OPTIONAL_LANES.items():
        assert gate_env[f"{prefix}_SELECTED"] == f"${{{{ needs.workflow-plan.outputs.{selector} }}}}"
        assert gate_env[f"{prefix}_RESULT"] == f"${{{{ needs.{job}.result }}}}"
    for job, selector in [OPTIONAL_LANES[key] for key in ("IMAGES", "CLUSTER", "STAGING")]:
        caller = jobs[job]
        assert caller["needs"] == "workflow-plan"
        assert caller["uses"] == f"./.github/workflows/{job}.yml"
        assert f"needs.workflow-plan.outputs.{selector} == 'true'" in caller["if"]
        assert "needs.workflow-plan.outputs.gate_mode == 'full'" in caller["if"]
        callee = _workflow(f".github/workflows/{job}.yml")
        assert set(_workflow_on(callee)) == {"workflow_call"}
        assert "concurrency" not in callee
        assert "plan" not in callee["jobs"]
        assert all(not name.endswith("-gate") for name in callee["jobs"])
    assert jobs["images"]["with"]["native_builds"] == "${{ needs.workflow-plan.outputs.native_builds }}"
    plan = jobs["workflow-plan"]
    assert "steps.image-plan.outputs.required" in plan["outputs"]["images"]
    assert "steps.image-plan.outputs.native_builds" in plan["outputs"]["native_builds"]


@pytest.mark.parametrize("prefix", OPTIONAL_LANES)
@pytest.mark.parametrize("selected", ["true", "false"])
@pytest.mark.parametrize("result", ["success", "skipped", "failure", "cancelled", ""])
def test_final_result_enforces_each_selected_lane(
    prefix: str, selected: str, result: str,
) -> None:
    env = _gate_environment()
    env.update({f"{prefix}_SELECTED": selected, f"{prefix}_RESULT": result})
    completed = _run_gate(env)
    accepted = result == "success" or (selected == "false" and result == "skipped")
    assert (completed.returncode == 0) == accepted, completed.stderr


@pytest.mark.parametrize("name", ["DOCS_ONLY", *(f"{prefix}_SELECTED" for prefix in OPTIONAL_LANES)])
@pytest.mark.parametrize("value", ["", "invalid", "TRUE"])
def test_final_result_rejects_malformed_planner_booleans(name: str, value: str) -> None:
    env = _gate_environment()
    env[name] = value
    completed = _run_gate(env)
    assert completed.returncode != 0
    assert "invalid planner boolean" in completed.stderr


@pytest.mark.parametrize("name", ["PLAN_RESULT", "FAST_RESULT", "GO_RESULT", *(f"{key}_RESULT" for key in OPTIONAL_LANES)])
def test_final_result_rejects_missing_dependency_result(name: str) -> None:
    env = _gate_environment()
    del env[name]
    assert _run_gate(env).returncode != 0


@pytest.mark.parametrize("docs_only", ["true", "false"])
@pytest.mark.parametrize("name", ["FAST_RESULT", "GO_RESULT"])
@pytest.mark.parametrize("result", ["success", "skipped", "failure", "cancelled"])
def test_docs_only_may_skip_fast_work_but_never_masks_failure(
    docs_only: str, name: str, result: str,
) -> None:
    env = _gate_environment()
    env.update(DOCS_ONLY=docs_only)
    env[name] = result
    accepted = result == "success" or (docs_only == "true" and result == "skipped")
    completed = _run_gate(env)
    assert (completed.returncode == 0) == accepted, completed.stderr


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped", ""])
def test_planner_failure_is_never_accepted(result: str) -> None:
    env = _gate_environment()
    env["PLAN_RESULT"] = result
    assert _run_gate(env).returncode != 0


def test_every_subscribed_pr_event_runs_a_real_stable_required_gate() -> None:
    workflow = _workflow(CI_WORKFLOW)
    assert set(_workflow_on(workflow)["pull_request"]["types"]) == {
        "opened",
        "synchronize",
        "reopened",
        "edited",
        "labeled",
        "unlabeled",
    }
    gate = workflow["jobs"]["repository-checks"]
    assert gate["if"] == "always()"
    assert (
        gate["name"]
        == "${{ github.event_name == 'workflow_dispatch' && 'repository-checks-manual' || 'repository-checks' }}"
    )
    planner = workflow["jobs"]["workflow-plan"]
    assert "if" not in planner
    assert planner["steps"][0]["uses"].startswith("actions/checkout@")
    assert all("if" not in step for step in planner["steps"][:3])
    assert "filtered" not in json.dumps(workflow)


def test_subscribed_ci_runs_cannot_cancel_another_required_check_suite() -> None:
    workflow = _workflow(CI_WORKFLOW)
    assert "concurrency" not in workflow


@pytest.mark.parametrize("mode", ["filtered", "", "invalid"])
def test_required_gate_cannot_accept_an_unvalidated_event_mode(mode: str) -> None:
    env = _gate_environment()
    env["GATE_MODE"] = mode
    assert _run_gate(env).returncode != 0


def _run_image_plan(
    tmp_path: Path, *, paths: tuple[str, ...], required: str, unowned: str,
) -> tuple[subprocess.CompletedProcess[str], dict[str, str]]:
    steps = _workflow(CI_WORKFLOW)["jobs"]["workflow-plan"]["steps"]
    step = next(item for item in steps if item.get("id") == "image-plan")
    changed = tmp_path / "changed-paths"
    changed.write_text("\n".join(paths) + "\n")
    output = tmp_path / "image-plan-output"
    result = subprocess.run(
        ["bash"], input=step["run"], cwd=REPO_ROOT, text=True, capture_output=True,
        env={**os.environ, "REQUIRED": required, "UNOWNED_RUNTIME": unowned,
             "CHANGED_FILES": str(changed), "GITHUB_OUTPUT": str(output),
             "EVENT_NAME": "pull_request"}, check=False,
    )
    values = dict(line.split("=", 1) for line in output.read_text().splitlines()) if output.exists() else {}
    return result, values


@pytest.mark.parametrize("paths", [("unowned-runtime/new-input.bin",), ("web/src/App.tsx", "unowned-runtime/new-input.bin")])
def test_unknown_active_input_selects_every_active_native_image(tmp_path: Path, paths: tuple[str, ...]) -> None:
    result, values = _run_image_plan(tmp_path, paths=paths, required="true", unowned="true")
    assert result.returncode == 0, result.stderr
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    expected = component_ownership.release_image_matrix(manifest, image_set="nebius")
    assert len(expected) == 7
    assert json.loads(values["images"]) == list(expected)
    native_builds = json.loads(values["native_builds"])
    assert native_builds == list(component_ownership.native_release_image_matrix(expected))
    assert len(native_builds) == len(expected)
    assert {(row["architecture"], row["platform"]) for row in native_builds} == {
        ("amd64", "linux/amd64"),
    }
    assert values["required"] == "true"


@pytest.mark.parametrize(
    ("path", "required", "images"),
    [("docs/user-guide.md", "false", set()),
     ("deploy/nginx-spa-security-headers.conf", "true", {"web"})],
)
def test_known_inputs_select_only_needed_images(tmp_path: Path, path: str, required: str, images: set[str]) -> None:
    result, values = _run_image_plan(tmp_path, paths=(path,), required=required, unowned="false")
    assert result.returncode == 0, result.stderr
    assert {row["image"] for row in json.loads(values["images"])} == images
    assert values["required"] == str(bool(images)).lower()


@pytest.mark.parametrize("name", ["required", "unowned"])
def test_image_planner_rejects_invalid_input_before_selection(tmp_path: Path, name: str) -> None:
    inputs = {"required": "false", "unowned": "false", name: "invalid"}
    result, _ = _run_image_plan(tmp_path, paths=("docs/user-guide.md",), **inputs)
    assert result.returncode != 0
    assert "invalid image planner boolean" in result.stderr


@pytest.mark.parametrize(
    ("workflow_path", "job", "lane"),
    [(".github/workflows/cluster-smoke.yml", "cluster-contract", "cluster-smoke"),
     (".github/workflows/staging-smoke.yml", "system-smoke", "system-smoke")],
)
def test_reusable_smoke_runs_only_manifest_owned_tests(workflow_path: str, job: str, lane: str) -> None:
    selected = _workflow(workflow_path)["jobs"][job]
    assert selected["runs-on"] == "ubuntu-24.04"
    scripts = "\n".join(step.get("run", "") for step in selected["steps"])
    assert f"component_ownership.py test-paths --lane {lane}" in scripts
    assert '"${test_paths[@]}"' in scripts
    assert 'not legacy_pool' in scripts
    assert "--extra rollout" not in scripts
    assert "continue-on-error" not in selected
    if lane == "cluster-smoke":
        assert selected["env"]["LOOM_RUN_DISPOSABLE_K3S"] == "1"
        assert "docker info" in scripts
    else:
        cleanup = next(step for step in selected["steps"] if step.get("name") == "Cleanup system-smoke compose stack")
        assert cleanup["if"] == "always()"
        assert "down -v --remove-orphans" in cleanup["run"]


@pytest.mark.parametrize("dependency", ["PLAN", "LOCKED", "LINT", "RUNTIME", "IAC", "ROOT", "PACKAGES"])
@pytest.mark.parametrize("result", ["success", "failure", "cancelled", "skipped"])
def test_fast_aggregator_requires_each_functional_dependency(dependency: str, result: str) -> None:
    step = _workflow(CI_WORKFLOW)["jobs"]["fast-checks"]["steps"][0]
    env = {**os.environ, **dict.fromkeys(step["env"], "success")}
    env[f"{dependency}_RESULT"] = result
    completed = subprocess.run(
        ["bash"], input=step["run"], text=True, capture_output=True, env=env, check=False,
    )
    assert (completed.returncode == 0) == (result == "success"), completed.stderr


@pytest.mark.parametrize("job_name", ["tests-root", "tests-packages", "integration"])
@pytest.mark.parametrize("coverage_enabled", ["false", "true"])
def test_test_lanes_run_without_coverage_unless_explicitly_requested(
    tmp_path: Path, job_name: str, coverage_enabled: str,
) -> None:
    job = _workflow(CI_WORKFLOW)["jobs"][job_name]
    step = next(item for item in job["steps"] if item.get("name", "").startswith("Pytest"))
    capture = tmp_path / "pytest-args.json"
    uv = tmp_path / "uv"
    uv.write_text(
        "#!/usr/bin/env python3\nimport json, os, sys\n"
        "if 'test-paths' in sys.argv:\n"
        "    print('tests/unit/test_config.py')\n"
        "elif 'pytest' in sys.argv:\n"
        "    with open(os.environ['CAPTURE_ARGS'], 'w') as stream:\n"
        "        json.dump(sys.argv[1:], stream)\n"
        "else:\n"
        "    raise SystemExit('unexpected invocation')\n"
    )
    uv.chmod(0o755)
    completed = subprocess.run(
        ["bash"], input=step["run"], text=True, capture_output=True, check=False,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}",
             "RUNNER_TEMP": str(tmp_path), "CAPTURE_ARGS": str(capture),
             "COVERAGE_ENABLED": coverage_enabled, "SHARD_INDEX": "0", "SHARD_COUNT": "2"},
    )
    assert completed.returncode == 0, completed.stderr
    args = json.loads(capture.read_text())
    assert "tests/unit/test_config.py" in args
    assert "pytest" in args
    assert "not legacy_pool" in " ".join(args)
    if coverage_enabled == "false":
        assert not any(arg.startswith("--cov") for arg in args)
        assert args[args.index("-p") + 1] == "no:cov"
    else:
        assert "--cov=src" in args and "--cov=packages" in args


@pytest.mark.parametrize("requested", [False, True])
def test_retired_only_image_plan_honors_explicit_validation_request(tmp_path: Path, requested: bool) -> None:
    from scripts.plan_ci_validations import plan_validations

    paths = ("src/loom_control_plane/worker_pool_autoscaler.py",)
    plan = plan_validations(
        changed_paths=paths,
        labels={"ci:images"} if requested else set(),
        event_name="pull_request",
    )
    result, values = _run_image_plan(
        tmp_path, paths=paths, required=str(plan.images).lower(), unowned="false",
    )
    assert result.returncode == 0, result.stderr
    assert values["required"] == str(requested).lower()
    manifest = component_ownership.load_manifest(REPO_ROOT / "config/component-ownership.toml")
    expected = component_ownership.release_image_matrix(manifest, image_set="nebius") if requested else ()
    assert json.loads(values["images"]) == list(expected)


@pytest.mark.parametrize("path", [
    "deploy/Dockerfile.harbor-runtime", "deploy/harbor-runtime-requirements.txt",
])
def test_workflow_image_plan_selects_harbor_without_legacy_builds(tmp_path: Path, path: str) -> None:
    result, values = _run_image_plan(tmp_path, paths=(path,), required="true", unowned="false")
    assert result.returncode == 0, result.stderr
    rows = json.loads(values["native_builds"])
    assert [row["image"] for row in rows] == ["harbor-runtime"]
    assert rows[0]["architecture"] == "amd64"
