from pathlib import Path

from scripts.plan_ci_validations import plan_validations

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


def test_codex_normal_dev_prs_queue_auto_merge_immediately() -> None:
    contributing = _read("CONTRIBUTING.md")
    quickstart = _read("docs/contributing/contributor-quickstart.md")
    pr_template = _read(".github/PULL_REQUEST_TEMPLATE.md")

    assert "Codex\n> enables GitHub auto-merge with squash immediately" in contributing
    assert "Codex turns on squash auto-merge immediately" in quickstart
    assert "For this Codex-authored normal `dev` PR" in pr_template


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

    assert (
        "Labels may add validation but cannot remove path-inferred validation"
        in contributing
    )
    assert "Codex enabled GitHub auto-merge" in pr_template

    for document in (contributing, quickstart):
        normalized_document = " ".join(document.split())
        assert "visible and successful on the current head SHA" in normalized_document
        assert "release-promotion prs to `main`" in normalized_document.lower()
        assert (
            "owner-managed" in normalized_document
            or "managed by the release owner" in normalized_document
        )

    assert "succeed on the current head SHA" in pr_template
    assert "explicitly owner-managed" in pr_template


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


def test_codeowners_points_at_current_maintainers_not_placeholder_teams() -> None:
    codeowners = _read(".github/CODEOWNERS")

    assert "* @qianyi-sun @Hongjian-Gu" in codeowners
    assert "/.github/ @qianyi-sun @Hongjian-Gu" in codeowners
    assert "/deploy/ @qianyi-sun @Hongjian-Gu" in codeowners


def test_ci_docs_only_fast_path_includes_repo_metadata_not_workflows() -> None:
    for metadata_path in (
        ".github/CODEOWNERS",
        ".github/PULL_REQUEST_TEMPLATE.md",
        ".github/ISSUE_TEMPLATE/bug.yml",
    ):
        plan = plan_validations(
            changed_paths=[metadata_path], labels=set(), event_name="pull_request"
        )
        assert plan.docs_only is True

    workflow_plan = plan_validations(
        changed_paths=[".github/workflows/ci.yml"],
        labels=set(),
        event_name="pull_request",
    )
    assert workflow_plan.docs_only is False
