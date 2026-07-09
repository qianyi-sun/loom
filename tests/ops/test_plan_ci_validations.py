from scripts.plan_ci_validations import plan_validations


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
