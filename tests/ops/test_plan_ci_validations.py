from pathlib import Path

import pytest
from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

REPO_ROOT = Path(__file__).resolve().parents[2]


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
        "deploy/catalog/gb10-smoke/tasks/gb10-oracle-hello-world/instruction.md",
        "docs/architecture/cluster-deploy-spikes/01-sandbox-bridge.sh",
        "unowned-runtime/new-input.bin",
    ],
)
def test_runtime_inputs_fail_safe_to_every_heavy_check(path: str) -> None:
    plan = plan_validations(
        changed_paths=[path], labels=set(), event_name="pull_request"
    )

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
    plan = plan_validations(
        changed_paths=[], labels=set(), event_name="merge_group"
    )
    assert plan.selected_heavy_checks() == {
        "integration",
        "integration_docker",
        "images",
        "cluster_smoke",
        "staging_smoke",
    }
