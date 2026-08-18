from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OPERATOR_RUNBOOK = REPO_ROOT / "docs/runbooks/operator-runbook.md"
STAGING_VALIDATION = REPO_ROOT / "docs/runbooks/staging-launch.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_current_release_docs_are_linked_from_indexes() -> None:
    docs_index = _read(REPO_ROOT / "docs/index.md")
    runbook_index = _read(REPO_ROOT / "docs/runbooks/README.md")

    assert OPERATOR_RUNBOOK.is_file()
    assert STAGING_VALIDATION.is_file()
    assert "operator-runbook.md" in docs_index
    assert "operator-runbook.md" in runbook_index
    assert "staging-launch.md" in runbook_index


def test_current_release_docs_cover_executable_validation_and_promotion() -> None:
    operator = _read(OPERATOR_RUNBOOK)
    staging = _read(STAGING_VALIDATION)

    for fragment in (
        "scripts/validate_environment_isolation.py",
        "scripts/ops/release_gate.py validate",
        "scripts/ops/frontend_route_smoke.py --help",
        "scripts/staging_smoke_gate.py",
        "scripts/ops/worker_capacity_manifest.py status",
        "gh workflow run release-promotion-gate.yml",
    ):
        assert fragment in operator or fragment in staging

    assert ".github/workflows/deploy-environment.yml" in operator
    assert "development and production only" in operator
    assert "does not deploy staging" in staging
    assert "loom-staging-rollout --env staging start" in operator
    assert "environment=production" in operator
    assert "successful `release_gate_run_id`" in operator


def test_current_release_docs_define_secret_safe_evidence() -> None:
    operator = _read(OPERATOR_RUNBOOK)
    staging = _read(STAGING_VALIDATION)

    assert "secret references" in staging
    assert "never the value" in staging
    assert "Store only sanitized responses and identifiers" in staging
    assert "bearer tokens, signed URLs, object-store keys" in operator
    assert "release_owner_approval" in operator
    assert "prod_staging_isolation" in staging
    assert "raw_delivery_export_status" in staging

    for forbidden_example in ("sk-live", "loom_api_live", "X-Amz-Signature=abc"):
        assert forbidden_example not in operator
        assert forbidden_example not in staging


def test_current_release_docs_keep_staging_capacity_temporary() -> None:
    staging = _read(STAGING_VALIDATION)

    assert "any staging lease is bounded and released after validation" in staging
    assert "release temporary staging\ncapacity" in staging
    assert "Production-owned capacity remains available" in staging


def test_current_release_docs_bind_identity_and_immutable_tags() -> None:
    operator = _read(OPERATOR_RUNBOOK)
    staging = _read(STAGING_VALIDATION)

    assert "same candidate" in staging
    assert "same `candidate_sha` and `image_tag`" in operator
    assert "immutable SemVer `prod_tag`" in operator
    assert "merged `main` commit" in operator
    assert "Never\n   reuse or force-move" in operator


def test_current_release_docs_dispatch_production_from_main_only() -> None:
    operator = _read(OPERATOR_RUNBOOK)

    assert "deploy-environment.yml` from `main`" in operator
    assert "not deployed from\nan arbitrary branch or tag" in operator


def test_current_release_docs_separate_merge_and_release_authority() -> None:
    operator = _read(OPERATOR_RUNBOOK)
    normalized = " ".join(operator.split()).lower()

    assert "trusted controller enables squash auto-merge" in normalized
    assert "only merge authority" in normalized
    assert "release_owner_approval" in normalized
    assert "production environment approval" in normalized
    assert "not interchangeable" in normalized
