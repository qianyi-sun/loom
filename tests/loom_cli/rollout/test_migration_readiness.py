from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.rollout.migration_readiness import inspect_migration_plan

REPO_ROOT = Path(__file__).resolve().parents[3]


def test_repository_migration_plan_is_linear_and_policy_bound() -> None:
    result = inspect_migration_plan(REPO_ROOT / "migrations/alembic.ini")

    assert result.head == "0066"
    assert result.base == "0001"
    assert result.revision_count == 66
    assert len(result.revision_sha256) == 66
    assert result.graph_policy == "single-linear-head"
    assert result.upgrade_policy == "expand-contract-before-destructive-change"
    assert result.downgrade_policy == "revision-declared-fail-closed"
    assert len(result.policy_digest) == 64
    assert len(result.plan_digest) == 64


def test_policy_head_drift_fails_closed(tmp_path: Path) -> None:
    policy = json.loads((REPO_ROOT / "config/staging-migration-policy.json").read_text())
    policy["expected_head"] = "0065"
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")

    with pytest.raises(ValueError, match="does not match"):
        inspect_migration_plan(REPO_ROOT / "migrations/alembic.ini", policy_path=path)


def test_policy_requires_rehearsal_before_protected_apply(tmp_path: Path) -> None:
    policy = json.loads((REPO_ROOT / "config/staging-migration-policy.json").read_text())
    policy["protected_apply_requires_rehearsal"] = False
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")

    with pytest.raises(ValueError, match="policy is invalid"):
        inspect_migration_plan(REPO_ROOT / "migrations/alembic.ini", policy_path=path)
