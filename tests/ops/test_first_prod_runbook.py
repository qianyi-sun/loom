from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = REPO_ROOT / "docs/runbooks/first-prod-release-runbook.md"


def _runbook_text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def test_first_prod_runbook_is_linked_from_operator_docs_and_index() -> None:
    assert RUNBOOK.is_file()

    operator_runbook = (REPO_ROOT / "docs/runbooks/operator-runbook.md").read_text(
        encoding="utf-8",
    )
    docs_index = (REPO_ROOT / "docs/index.md").read_text(encoding="utf-8")

    assert "first-prod-release-runbook.md" in operator_runbook
    assert "first-prod-release-runbook.md" in docs_index


def test_first_prod_runbook_has_executable_command_coverage() -> None:
    text = _runbook_text()

    required_fragments = [
        "No-secret non-production dry run",
        "LIVE PROD AUTHORITY REQUIRED",
        "DRY-RUN SAFE",
        "First-prod bootstrap",
        "Staging validation lease",
        "Frontend environment checks",
        "Production release",
        "Rollback preparation",
        "Emergency staging drain",
        "scripts/validate_environment_isolation.py",
        "scripts/ops/worker_capacity_manifest.py status",
        "scripts/ops/worker_capacity_manifest.py lease-staging",
        "scripts/ops/worker_capacity_manifest.py release-staging",
        "scripts/ops/worker_capacity_manifest.py drain-staging",
        "scripts/ops/release_gate.py validate",
        "scripts/ops/frontend_route_smoke.py",
        "scripts/ops/operator_free_user_e2e_gate.py",
        "scripts/ops/verify_production_release_gate.sh",
        "gh workflow run release-promotion-gate.yml",
        "gh workflow run deploy-environment.yml",
    ]

    for fragment in required_fragments:
        assert fragment in text


def test_first_prod_runbook_documents_expected_secret_safe_outputs() -> None:
    text = _runbook_text()

    required_fragments = [
        "Expected success output",
        "Expected failure output",
        '"status": "pass"',
        '"status": "fail"',
        '"staging_slots": 0',
        '"new_staging_claims_allowed": false',
        '"prod_pressure"',
        "forbidden evidence value",
        "Operator-free user E2E gate: PASS",
        "missing required cli_api step",
        "forbidden shortcut declared",
        "runtime config response must be no-store",
        "production deploy requires release gate inputs",
        "env:LOOM_PROD_USER_E2E_TOKEN",
        "<redacted>",
        "[REDACTED",
    ]

    for fragment in required_fragments:
        assert fragment in text

    forbidden_secret_examples = [
        "sk-live",
        "loom_api_live",
        "Bearer raw",
        "X-Amz-Signature=abc",
    ]
    for fragment in forbidden_secret_examples:
        assert fragment not in text


def test_first_prod_runbook_requires_beta_leases_to_be_temporary() -> None:
    text = _runbook_text().lower()

    assert "staging leases are temporary" in text
    assert "default prod capacity ownership must be restored" in text
    assert "stop condition" in text
    assert "no-secret" in text
    assert "non-production" in text


def test_first_prod_runbook_documents_round_trip_and_squash_safe_identity() -> None:
    text = _runbook_text()
    normalized = " ".join(text.split())

    for fragment in (
        '"schema_version": 1',
        "same `release-gate-evidence.json`",
        "scripts/ops/release_identity.py verify",
        "Git tree object",
        "does not require candidate commit ancestry",
    ):
        assert fragment in normalized

    assert "non-ancestor production ref" not in text


def test_first_prod_runbook_dispatches_production_from_main_only() -> None:
    text = _runbook_text()

    assert "Production deployment dispatches use `--ref main` only" in text
    assert '--ref "$PROD_TAG"' not in text
    assert '--ref "$PREVIOUS_PROD_TAG"' not in text
    assert "uses `main` or an immutable `vX.Y.Z` tag" not in text


def test_first_prod_runbook_requires_qianyi_manual_promotion_and_bootstrap() -> None:
    text = _runbook_text()
    normalized = " ".join(text.split()).lower()

    assert "@qianyi-sun" in normalized
    assert "never enable auto-merge" in normalized
    assert "manual squash" in normalized
    assert "target branch's codeowners" in normalized
    assert "@carinrc" in normalized
    assert "generic one-approval" in normalized
    assert "cannot approve their own pull request" in normalized
    assert "non-qianyi identity" in normalized
    assert "release_owner_approval" in normalized
    assert "production environment approval" in normalized
    assert "not interchangeable" in normalized
