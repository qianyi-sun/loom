from pathlib import Path

import pytest
from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize(
    "path",
    [
        "scripts/ops/developer_sandbox_domain_runtime.py",
        "scripts/ops/developer_sandbox_live_acceptance.py",
        "scripts/ops/developer_sandbox_live_authority.py",
        "scripts/ops/developer_sandbox_node_transport.py",
        "scripts/ops/developer_sandbox_slurm_policy.py",
        "scripts/ops/developer_sandbox_staging_promotion.py",
        "scripts/ops/staging_external_slurm_acceptance_authority.py",
        "src/loom_cli/external_slurm_acceptance.py",
        "tests/ops/test_developer_sandbox_domain_runtime.py",
        "tests/ops/test_developer_sandbox_live_acceptance.py",
        "tests/ops/test_developer_sandbox_live_authority.py",
        "tests/ops/test_developer_sandbox_node_transport.py",
        "tests/ops/test_developer_sandbox_slurm_policy.py",
        "tests/ops/test_developer_sandbox_staging_promotion.py",
        "tests/ops/test_staging_external_slurm_acceptance_authority.py",
        "tests/loom_cli/test_external_slurm_acceptance.py",
        "deploy/developer-sandboxes/node-authority-transport.toml",
        "deploy/slurm/developer-sandboxes/gb10.toml",
    ],
)
def test_developer_sandbox_runtime_authority_selects_all_heavy_gates(
    path: str,
) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


def test_every_non_draft_pr_runs_full_protected_gate() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
        pull_request_action="synchronize",
    )

    assert plan.event_relevant is True
    assert plan.full_gate is True
    assert plan.gate_mode == "full"


def test_merge_ready_label_has_no_special_authority() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels={"ci:merge-ready"},
        event_name="pull_request",
        pull_request_action="labeled",
        pull_request_action_label="ci:merge-ready",
    )

    assert plan.event_relevant is False
    assert plan.full_gate is False
    assert plan.gate_mode == "filtered"


def test_draft_pr_is_filtered() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
        pull_request_action="synchronize",
        pull_request_draft=True,
    )

    assert plan.event_relevant is False
    assert plan.full_gate is False
    assert plan.gate_mode == "filtered"


def test_converting_to_draft_filters_gate_until_ready_again() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
        pull_request_action="converted_to_draft",
        pull_request_draft=True,
    )

    assert plan.event_relevant is False
    assert plan.full_gate is False
    assert plan.gate_mode == "filtered"


@pytest.mark.parametrize(
    ("action", "action_label", "base_changed"),
    [
        ("labeled", "triage", False),
        ("unlabeled", "triage", False),
        ("edited", "", False),
    ],
)
def test_irrelevant_pr_metadata_event_is_filtered(
    action: str,
    action_label: str,
    base_changed: bool,
) -> None:
    plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
        pull_request_action=action,
        pull_request_action_label=action_label,
        pull_request_base_changed=base_changed,
    )

    assert plan.event_relevant is False
    assert plan.gate_mode == "filtered"


def test_docs_only_selects_no_heavy_validation() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md", "CONTRIBUTING.md"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.docs_only is True
    assert plan.selected_heavy_checks() == set()


def test_labels_add_corresponding_checks() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={
            "ci:integration",
            "ci:integration-docker",
            "ci:images",
            "cluster-smoke",
            "staging-smoke",
            "ci:coverage-summary",
        },
        event_name="pull_request",
    )
    assert plan.integration is True
    assert plan.integration_docker is True
    assert plan.images is True
    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True
    assert plan.coverage_summary is True


def test_coverage_summary_implies_integration() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={"ci:coverage-summary"},
        event_name="pull_request",
    )
    assert plan.coverage_summary is True
    assert plan.integration is True


def test_ci_internal_pr_labels_disable_docs_only_fast_path() -> None:
    for label in ("ci:integration", "ci:coverage-summary"):
        plan = plan_validations(
            changed_paths=["docs/user-guide.md"],
            labels={label},
            event_name="pull_request",
        )

        assert plan.docs_only is False
        assert plan.integration is True
        assert plan.coverage_summary is True


def test_integration_label_selects_coverage_but_inferred_integration_does_not() -> None:
    labeled_plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={"ci:integration"},
        event_name="pull_request",
    )
    inferred_plan = plan_validations(
        changed_paths=["src/loom/config.py"],
        labels=set(),
        event_name="pull_request",
    )

    assert labeled_plan.integration is True
    assert labeled_plan.coverage_summary is True
    assert inferred_plan.integration is True
    assert inferred_plan.coverage_summary is False


def test_workflow_dispatch_integration_does_not_select_pr_coverage_summary() -> None:
    plan = plan_validations(
        changed_paths=["docs/user-guide.md"],
        labels={"ci:integration"},
        event_name="workflow_dispatch",
    )

    assert plan.integration is True
    assert plan.coverage_summary is False


def test_worker_driver_change_selects_all_runtime_gates() -> None:
    plan = plan_validations(
        changed_paths=["src/loom/driver/docker.py"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.integration is True
    assert plan.integration_docker is True
    assert plan.images is True
    assert plan.staging_smoke is True


def test_cluster_template_change_selects_cluster_and_staging() -> None:
    plan = plan_validations(
        changed_paths=["src/loom_cli/templates/k8s/worker.yaml.j2"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True
    assert plan.images is False


def test_pinned_ingress_controller_change_selects_cluster_and_staging() -> None:
    plan = plan_validations(
        changed_paths=["deploy/k8s/ingress-nginx-kind.yaml"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True


@pytest.mark.parametrize(
    "path",
    [
        "src/loom_cli/rollout/steps/candidate_source.py",
        "src/loom_cli/rollout/steps/s03_kind_cluster.py",
        "src/loom_cli/rollout/steps/subprocess_util.py",
        "tests/loom_cli/rollout/steps/test_candidate_source_invocation.py",
        "tests/loom_cli/rollout/steps/test_s03_kind_cluster.py",
        "tests/loom_cli/rollout/steps/test_subprocess_util.py",
    ],
)
def test_kind_cluster_rollout_contract_selects_cluster_and_staging(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True


@pytest.mark.parametrize(
    "path",
    [
        ".github/workflows/staging-smoke.yml",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        "src/loom_cli/templates/k8s/ingress.yaml.j2",
        "scripts/ops/frontend_security_headers.py",
        "scripts/ops/frontend_route_smoke.py",
        "tests/ops/test_frontend_security_headers.py",
        "web/package-lock.json",
        "web/package.json",
        "web/scripts/frontend-route-browser-smoke.mjs",
        "web/scripts/frontend-route-browser-smoke.test.mjs",
        "web/scripts/staging-admin-browser-smoke.mjs",
        "web/scripts/staging-admin-browser-smoke.test.mjs",
        "web/src/main.tsx",
    ],
)
def test_frontend_route_contract_selects_staging_smoke(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.staging_smoke is True


@pytest.mark.parametrize(
    "path",
    [
        "web/src/App.tsx",
        "web/src/api/schema.d.ts",
        "web/e2e/routes.spec.ts",
        "web/playwright.config.ts",
        "deploy/Dockerfile.web",
        "deploy/nginx-spa.conf",
        "deploy/nginx-spa-security-headers.conf",
        "deploy/web-runtime-config.sh",
        ".github/workflows/ci.yml",
        "config/component-ownership.toml",
        "scripts/component_ownership.py",
    ],
)
def test_frontend_quality_contract_selects_web_checks(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.web_checks is True
    assert f"path:{path}" in plan.reasons["web_checks"]


@pytest.mark.parametrize(
    "path",
    [
        "web/src/__tests__/AuthContext.test.tsx",
        "web/src/auth/AuthContext.tsx",
    ],
)
def test_browser_auth_contract_selects_cluster_and_staging(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.cluster_smoke is True
    assert plan.staging_smoke is True


def test_planner_change_selects_every_heavy_gate() -> None:
    plan = plan_validations(
        changed_paths=["scripts/plan_ci_validations.py"],
        labels=set(),
        event_name="pull_request",
    )
    assert plan.selected_heavy_checks() == {
        "integration",
        "integration_docker",
        "images",
        "cluster_smoke",
        "staging_smoke",
    }


@pytest.mark.parametrize(
    "path",
    [
        "config/component-ownership.toml",
        "scripts/component_ownership.py",
        "tests/ops/test_component_ownership_manifest.py",
    ],
)
def test_ownership_authority_change_selects_every_heavy_gate(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert all("ownership-authority-change" in plan.reasons[name] for name in HEAVY_CHECKS)


@pytest.mark.parametrize(
    "path",
    [
        "config/uv-toolchain.toml",
        "pyproject.toml",
        "uv.lock",
        "packages/loom-launcher/pyproject.toml",
        "packages/loom-benchmarks/pyproject.toml",
        "packages/loom-benchmark-terminal-bench-2/pyproject.toml",
    ],
)
def test_dependency_authority_changes_select_every_heavy_gate(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    expected_reason = f"dependency-authority:{path}"
    assert all(expected_reason in plan.reasons[check] for check in HEAVY_CHECKS)


@pytest.mark.parametrize(
    "path",
    [
        "deploy/environments/staging.cluster.toml",
        "deploy/environment-state/staging.toml",
        "deploy/staging-rollout/loom-staging-rollout.sudoers",
        "deploy/worker-pools/gb10/known_hosts",
        "deploy/worker-pools/gb10/loom-staging-rollout-platform-dev.exports",
        "deploy/worker-pools/gb10/loom-staging-rollout-shared-work2-export-authority.sudoers",
        "deploy/worker-pools/gb10/ssh_config",
        "scripts/ops/staging_rollout_host.py",
        "scripts/ops/staging_rollout_sealed_source.py",
        "scripts/ops/staging_rollout_shared_repo.py",
        "scripts/ops/staging_rollout_shared_work2.py",
        "scripts/ops/staging_rollout_shared_work2_export.py",
        "scripts/ops/staging_rollout_shared_work2_export_authority.py",
        "scripts/ops/staging_rollout_shared_repo_consumer.py",
        "scripts/ops/verify_staging_rollout_secret_boundary.py",
        "src/loom_cli/rollout/operator/broker.py",
        "src/loom_cli/rollout/steps/s04_gb10_prep.py",
        "src/loom_cli/rollout/steps/s10_env_state.py",
        "tests/loom_cli/rollout/operator/test_broker.py",
        "tests/loom_cli/rollout/steps/test_env_state_external_prereqs.py",
        "tests/loom_cli/test_cluster_render.py",
        "tests/loom_cli/test_environment_state.py",
        "tests/ops/test_staging_rollout_host.py",
        "tests/ops/test_staging_rollout_sealed_source.py",
        "tests/ops/test_staging_rollout_shared_repo.py",
        "tests/ops/test_staging_rollout_shared_repo_consumer.py",
        "tests/ops/test_staging_rollout_shared_work2.py",
        "tests/ops/test_staging_rollout_shared_work2_export.py",
        "tests/ops/test_staging_rollout_shared_work2_export_authority.py",
    ],
)
def test_protected_staging_rollout_paths_select_every_heavy_gate(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    assert all("protected-staging-rollout" in plan.reasons[check] for check in HEAVY_CHECKS)


def test_nearby_rollout_module_does_not_gain_protected_staging_authority() -> None:
    plan = plan_validations(
        changed_paths=["src/loom_cli/rollout/operator_notes.py"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.unowned_runtime is False
    assert plan.selected_heavy_checks() == {"integration", "images"}
    assert all("protected-staging-rollout" not in plan.reasons[check] for check in HEAVY_CHECKS)


@pytest.mark.parametrize(
    "path",
    [
        "deploy/catalog/gb10-smoke/tasks/gb10-oracle-hello-world/instruction.md",
        "docs/architecture/cluster-deploy-spikes/01-sandbox-bridge.sh",
        "unowned-runtime/new-input.bin",
    ],
)
def test_runtime_inputs_fail_safe_to_every_heavy_check(path: str) -> None:
    plan = plan_validations(changed_paths=[path], labels=set(), event_name="pull_request")

    assert plan.docs_only is False
    assert plan.unowned_runtime is True
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)
    expected_reason = f"unowned-runtime-path:{path}"
    assert all(expected_reason in plan.reasons[check] for check in HEAVY_CHECKS)


def test_mixed_known_and_unowned_paths_preserve_unowned_runtime_signal() -> None:
    plan = plan_validations(
        changed_paths=["web/src/App.tsx", "unowned-runtime/new-input.bin"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.unowned_runtime is True
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


def test_codeowners_is_not_static_documentation() -> None:
    plan = plan_validations(
        changed_paths=[".github/CODEOWNERS"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.docs_only is False
    assert plan.selected_heavy_checks() == set(HEAVY_CHECKS)


def test_migration_change_selects_integration_images_and_staging() -> None:
    plan = plan_validations(
        changed_paths=["migrations/versions/1234_add_runtime_state.py"],
        labels=set(),
        event_name="pull_request",
    )

    assert plan.selected_heavy_checks() == {
        "integration",
        "images",
        "staging_smoke",
    }


def test_every_docker_marked_integration_module_selects_docker() -> None:
    integration_dir = REPO_ROOT / "tests" / "integration"
    docker_marked_modules = [
        path
        for path in sorted(integration_dir.rglob("*.py"))
        if "pytest.mark.docker" in path.read_text(encoding="utf-8")
    ]
    assert docker_marked_modules

    for module in docker_marked_modules:
        relative_path = module.relative_to(REPO_ROOT).as_posix()
        plan = plan_validations(
            changed_paths=[relative_path],
            labels=set(),
            event_name="pull_request",
        )
        assert plan.integration_docker is True, relative_path


def test_merge_group_selects_every_heavy_gate() -> None:
    plan = plan_validations(changed_paths=[], labels=set(), event_name="merge_group")
    assert plan.selected_heavy_checks() == {
        "integration",
        "integration_docker",
        "images",
        "cluster_smoke",
        "staging_smoke",
    }
    assert plan.web_checks is True
