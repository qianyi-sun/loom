from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = REPO_ROOT / "docs/first-prod-release-runbook.md"


def _runbook_text() -> str:
    return RUNBOOK.read_text(encoding="utf-8")


def test_first_prod_runbook_is_linked_from_operator_docs_and_index() -> None:
    assert RUNBOOK.is_file()

    operator_runbook = (REPO_ROOT / "docs/operator-runbook.md").read_text(
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
        "Beta validation lease",
        "Frontend environment checks",
        "Production release",
        "Rollback preparation",
        "Emergency beta drain",
        "scripts/validate_environment_isolation.py",
        "scripts/ops/worker_capacity_manifest.py status",
        "scripts/ops/worker_capacity_manifest.py lease-beta",
        "scripts/ops/worker_capacity_manifest.py release-beta",
        "scripts/ops/worker_capacity_manifest.py drain-beta",
        "scripts/ops/release_gate.py validate",
        "scripts/ops/frontend_route_smoke.py",
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
        '"beta_slots": 0',
        '"new_beta_claims_allowed": false',
        '"prod_pressure"',
        "forbidden evidence value",
        "runtime config response must be no-store",
        "production deploy requires release gate inputs",
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

    assert "beta leases are temporary" in text
    assert "default prod capacity ownership must be restored" in text
    assert "stop condition" in text
    assert "no-secret" in text
    assert "non-production" in text
