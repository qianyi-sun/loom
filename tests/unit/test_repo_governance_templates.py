from pathlib import Path

from scripts.plan_ci_validations import HEAVY_CHECKS, plan_validations

ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def test_contributing_and_pr_template_accept_issue_scoped_external_prs() -> None:
    contributing = _read("CONTRIBUTING.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    assert "External pull requests are not accepted" not in contributing
    assert "External pull requests are accepted for issue-scoped work" in contributing
    assert "## Linked Issue" in pr_template
    assert "Refs #" in pr_template
    assert "pull request code must not rely on protected secrets" in pr_template


def test_normal_dev_prs_use_ci_only_squash_auto_merge() -> None:
    contributing = _read("CONTRIBUTING.md")
    quickstart = _read("docs/contributing/contributor-quickstart.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    assert "Every\n> normal `dev` PR uses GitHub squash auto-merge" in contributing
    assert "Every normal `dev` PR uses squash auto-merge" in quickstart
    assert "For this normal `dev` PR" in pr_template

    for document in (contributing, quickstart, pr_template):
        normalized_document = " ".join(document.split()).lower()
        for gate in (
            "repository-checks",
            "images-gate",
            "cluster-smoke-gate",
            "staging-smoke-gate",
        ):
            assert gate in normalized_document
        assert "no human approval" in normalized_document
        assert "no codeowner approval" in normalized_document
        assert "no conversation resolution" in normalized_document


def test_governance_docs_define_path_inferred_validation_gates() -> None:
    contributing = _read("CONTRIBUTING.md")
    quickstart = _read("docs/contributing/contributor-quickstart.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    for gate in (
        "repository-checks",
        "images-gate",
        "cluster-smoke-gate",
        "staging-smoke-gate",
    ):
        assert gate in contributing
        assert gate in quickstart

    assert "Labels may add validation but cannot remove path-inferred validation" in contributing
    assert "GitHub squash auto-merge was enabled" in pr_template

    for document in (contributing, quickstart):
        normalized_document = " ".join(document.split())
        assert "visible and successful on the current head SHA" in normalized_document
        assert "main` accepts only" in normalized_document.lower()
        assert "manual squash merge" in normalized_document.lower()
        assert "never enable auto-merge" in normalized_document.lower()

    assert "succeed on the current head SHA" in pr_template
    assert "Qianyi (`@qianyi-sun`) reviews" in pr_template
    assert "manual squash merge" in pr_template


def test_main_promotion_requires_qianyi_manual_squash_and_separates_approvals() -> None:
    contributing = _read("CONTRIBUTING.md")
    quickstart = _read("docs/contributing/contributor-quickstart.md")
    operator_runbook = _read("docs/runbooks/operator-runbook.md")
    first_prod_runbook = _read("docs/runbooks/first-prod-release-runbook.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")
    release_template = _read(".github/ISSUE_TEMPLATE/release.yml")

    for document in (
        contributing,
        quickstart,
        operator_runbook,
        first_prod_runbook,
        pr_template,
        release_template,
    ):
        normalized_document = " ".join(document.split()).lower()
        assert "@qianyi-sun" in normalized_document
        assert "manual squash" in normalized_document
        assert "never enable auto-merge" in normalized_document

    for document in (contributing, operator_runbook, first_prod_runbook, pr_template):
        normalized_document = " ".join(document.split()).lower()
        assert "release_owner_approval" in normalized_document
        assert "production environment approval" in normalized_document
        assert "not interchangeable" in normalized_document

    assert "allow_auto_merge" in contributing
    assert "repository-wide" in contributing


def test_release_promotion_template_requires_first_prod_evidence() -> None:
    contributing = _read("CONTRIBUTING.md")
    operator_runbook = _read("docs/runbooks/operator-runbook.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    for required_text in (
        "Immutable prod tag",
        "Frontend route evidence",
        "Worker isolation evidence",
        "Raw-delivery/export requirement status",
        "prod tag is new, immutable, and will not be force-moved",
    ):
        assert required_text in pr_template

    assert "immutable `vX.Y.Z` production release tags" in contributing
    assert "Never force-move or reuse a published prod tag" in contributing
    assert "Production tags are immutable SemVer Git tags on `main`" in operator_runbook
    assert "Production deployment dispatches use `main` only" in operator_runbook
    assert "`prod_tag`" in operator_runbook
    assert "`frontend_route_evidence`" in operator_runbook
    assert "`prod_staging_isolation`" in operator_runbook
    assert "`raw_delivery_export_status`" in operator_runbook


def test_issue_templates_use_current_loom_language() -> None:
    template_dir = ROOT / ".github" / "ISSUE_TEMPLATE"
    template_text = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted(template_dir.glob("*.yml"))
    )

    for stale_term in ("Harness", "pre-Loom", "SkillFlow-specific"):
        assert stale_term not in template_text

    assert "User evaluation flow" in template_text
    assert "Model provider / gateway" in template_text
    assert "Benchmark catalog / task data" in template_text
    assert "Agent adapter / sandbox runtime" in template_text
    assert "vX.Y.Z` tag exists on the merged `main` commit" in template_text


def test_codeowners_routes_all_paths_to_qianyi_but_is_advisory_on_dev() -> None:
    codeowners = _read(".github/CODEOWNERS")
    entries = {
        line.strip()
        for line in codeowners.splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert entries == {"* @qianyi-sun"}
    assert "@carinrc" not in codeowners
    normalized = " ".join(codeowners.split()).lower()
    assert "advisory" in normalized
    assert "not a `dev` merge gate" in normalized
    assert "subsequent `main` promotion" in normalized


def test_ci_docs_only_fast_path_includes_repo_metadata_not_workflows() -> None:
    for metadata_path in (
        ".github/PULL_REQUEST_TEMPLATE.md",
        ".github/ISSUE_TEMPLATE/bug.yml",
    ):
        plan = plan_validations(
            changed_paths=[metadata_path], labels=set(), event_name="pull_request"
        )
        assert plan.docs_only is True

    codeowners_plan = plan_validations(
        changed_paths=[".github/CODEOWNERS"], labels=set(), event_name="pull_request"
    )
    assert codeowners_plan.docs_only is False
    assert codeowners_plan.selected_heavy_checks() == set(HEAVY_CHECKS)

    workflow_plan = plan_validations(
        changed_paths=[".github/workflows/ci.yml"],
        labels=set(),
        event_name="pull_request",
    )
    assert workflow_plan.docs_only is False
