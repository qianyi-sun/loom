from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom.db.schema_startup import service_schema_head
from loom_cli.rollout.migration_readiness import inspect_migration_plan

REPO_ROOT = Path(__file__).resolve().parents[3]
EXPECTED_HEAD = service_schema_head()


def test_repository_migration_plan_is_single_head_and_policy_bound() -> None:
    result = inspect_migration_plan(REPO_ROOT / "migrations/alembic.ini")

    assert result.head == service_schema_head()
    assert result.base == "0001"
    assert result.revision_count == len(result.revision_sha256)
    assert result.head in result.revision_sha256
    assert result.graph_policy == "single-head-closed-dag"
    assert result.upgrade_policy == "expand-contract-before-destructive-change"
    assert result.downgrade_policy == "revision-declared-fail-closed"
    assert len(result.policy_digest) == 64
    assert len(result.plan_digest) == 64


def test_repository_migration_plan_is_independent_of_process_cwd(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)

    result = inspect_migration_plan(REPO_ROOT / "migrations/alembic.ini")

    assert result.head == service_schema_head()
    assert result.revision_count == len(result.revision_sha256)


def test_migration_plan_rejects_noncanonical_script_location(tmp_path: Path) -> None:
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    ini = migrations / "alembic.ini"
    ini.write_text("[alembic]\nscript_location = ../outside\n", encoding="utf-8")

    with pytest.raises(ValueError, match="graph is unreadable"):
        inspect_migration_plan(ini, policy_path=REPO_ROOT / "config/staging-migration-policy.json")


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


def test_next_migration_is_automatic_but_forked_graph_still_fails(tmp_path):
    import shutil

    migration_dir = tmp_path / "migrations"
    shutil.copytree(REPO_ROOT / "migrations", migration_dir, ignore=shutil.ignore_patterns("__pycache__"))
    before = inspect_migration_plan(migration_dir / "alembic.ini")
    source = f'revision = "9998"\ndown_revision = "{before.head}"\n'
    (migration_dir / "versions/9998_next_release.py").write_text(source)
    after = inspect_migration_plan(migration_dir / "alembic.ini")
    assert after.head == service_schema_head(migration_dir / "alembic.ini") == "9998"
    assert after.revision_count == before.revision_count + 1
    assert after.policy_digest == before.policy_digest
    (migration_dir / "versions/9999_fork.py").write_text(source.replace('"9998"', '"9999"'))
    with pytest.raises(ValueError, match="does not match"):
        inspect_migration_plan(migration_dir / "alembic.ini")
