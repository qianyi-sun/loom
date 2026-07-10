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

    assert "Labels may add validation but cannot remove path-inferred validation" in contributing
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


def test_codeowners_is_narrow_ci_and_release_trust_root() -> None:
    owners = "@qianyi-sun @Hongjian-Gu"
    entries = {
        line.strip()
        for line in _read(".github/CODEOWNERS").splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }
    authority_paths = {
        "/.github/",
        "/scripts/plan_ci_validations.py",
        "/scripts/check_install_scripts_pinned.py",
        "/scripts/validate_environment_isolation.py",
        "/tests/ops/test_plan_ci_validations.py",
        "/tests/ops/test_ci_throughput_workflows.py",
        "/pyproject.toml",
        "/scripts/ops/release_gate.py",
        "/scripts/ops/release_identity.py",
        "/scripts/ops/verify_production_release_gate.sh",
        "/scripts/ops/deploy_environment.sh",
        "/deploy/environments/production.toml",
        "/tests/ops/test_release_identity.py",
        "/tests/ops/test_release_promotion_gate.py",
        "/tests/ops/test_first_prod_runbook.py",
        "/tests/ops/test_environment_isolation.py",
        "/tests/unit/test_repo_governance_templates.py",
        "/docs/runbooks/first-prod-release-runbook.md",
        "/docs/runbooks/operator-runbook.md",
        "/CONTRIBUTING.md",
        "/SECURITY.md",
        "/LICENSE",
    }

    assert f"* {owners}" not in entries
    for broad_path in ("/src/", "/packages/", "/web/", "/deploy/", "/docs/"):
        assert not any(
            entry == f"{broad_path} {owners}" or entry.startswith(f"{broad_path} ")
            for entry in entries
        )
    assert entries == {f"{path} {owners}" for path in authority_paths}


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
