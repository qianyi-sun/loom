"""Fail closed if the inert guard foundation acquires executable authority."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

GUARD_ROOT = Path("src/loom_capacity_guard")
AGENT_ROOT = Path("src/loom_capacity_agent")
FORBIDDEN_IMPORTS = {
    "subprocess",
    "loom.worker_token",
    "loom_control_plane.elastic_slurm_worker_controller",
    "loom_control_plane.scheduler.claim",
    "loom_control_plane.slurm_job_cgroup",
    "loom_control_plane.slurm_worker_jobs",
}
FORBIDDEN_TOKENS = {
    "claim_trial",
    "grant_capacity",
    "issue_worker_token",
    "launch_permit",
    "mint_worker_token",
    "sbatch",
    "scancel",
}
ROUTE_DECORATORS = {"delete", "get", "head", "options", "patch", "post", "put"}


def _import_names(tree: ast.AST) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            names.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            names.add(node.module)
    return names


def test_capacity_guard_package_has_no_executable_admission_boundary() -> None:
    sources = sorted(GUARD_ROOT.rglob("*.py"))
    assert sources
    for path in sources:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        imports = _import_names(tree)
        assert not {
            imported
            for imported in imports
            if any(
                imported == forbidden or imported.startswith(f"{forbidden}.")
                for forbidden in FORBIDDEN_IMPORTS
            )
        }, path
        assert not {token for token in FORBIDDEN_TOKENS if token in source.lower()}, path
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                target = decorator.func if isinstance(decorator, ast.Call) else decorator
                assert not (
                    isinstance(target, ast.Attribute) and target.attr.lower() in ROUTE_DECORATORS
                ), path


def test_capacity_agent_has_no_pool_mutation_or_candidate_runtime_wiring() -> None:
    sources = sorted(AGENT_ROOT.rglob("*.py"))
    assert sources
    for path in sources:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
        imports = _import_names(tree)
        assert not {
            imported
            for imported in imports
            if any(
                imported == forbidden or imported.startswith(f"{forbidden}.")
                for forbidden in FORBIDDEN_IMPORTS
            )
        }, path
        assert not {token for token in FORBIDDEN_TOKENS if token in source.lower()}, path
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                target = decorator.func if isinstance(decorator, ast.Call) else decorator
                assert not (
                    isinstance(target, ast.Attribute) and target.attr.lower() in ROUTE_DECORATORS
                ), path

    # The trusted lifecycle authority intentionally installs the separately
    # fenced reporter.  Keep that orchestration out of the untrusted candidate
    # build/activation boundary instead of banning all service-owned wiring.
    runtime_roots = (
        Path("src/loom"),
        Path("src/loom_cli"),
        Path("src/loom_control_plane"),
        Path("src/loom_service"),
    )
    wired = {
        path
        for root in runtime_roots
        for path in root.rglob("*.py")
        if "loom_capacity_agent" in path.read_text(encoding="utf-8")
    }
    assert wired == {
        Path("src/loom/personal_dev_capacity.py"),
        Path("src/loom/personal_dev_capacity_runtime.py"),
        Path("src/loom_service/personal_dev_lifecycle.py"),
    }


def test_capacity_guard_migrations_have_no_candidate_database_fallback() -> None:
    source = Path("capacity_guard_migrations/env.py").read_text(encoding="utf-8")
    assert "LOOM_CAPACITY_GUARD_DB_URL" in source
    assert "LOOM_CAPACITY_GUARD_OWNER_ROLE" in source
    assert "LOOM_CAPACITY_GUARD_AGENT_ROLE" in source
    assert "LOOM_CAPACITY_GUARD_EXECUTOR_ROLE" in source
    assert "LOOM_DB_URL" not in source
    assert "LOOM_CP_DB_URL" not in source
    assert "LOOM_CAPACITY_DB_URL" not in source


def test_capacity_guard_is_in_default_static_validation() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    assert "src/loom_capacity_guard" in project["tool"]["mypy"]["files"]
    assert "src/loom_capacity_agent" in project["tool"]["mypy"]["files"]

    workflow = Path(".github/workflows/ci.yml").read_text(encoding="utf-8")
    ruff_command = "ruff check src tests packages migrations capacity_guard_migrations"
    assert ruff_command in workflow


def test_prepared_admission_migration_remains_zero_executable_and_projection_read_only() -> None:
    source = Path("capacity_guard_migrations/versions/guard_0003_prepared_admission.py").read_text(
        encoding="utf-8"
    )
    lowered = source.lower()
    assert "executable = false" in lowered
    assert "claim_authorization_epoch = 0" in lowered
    assert "security definer" in lowered
    assert "to public" not in lowered
    assert "insert into public.trials" not in lowered
    assert "update public.trials" not in lowered
    assert "delete from public.trials" not in lowered
    assert "update loom_capacity_guard.trial_attempts" not in lowered
    assert "sbatch" not in lowered
    assert "scancel" not in lowered


def test_disconnected_claim_guard_has_no_live_entry_or_candidate_mutation() -> None:
    source = Path(
        "capacity_guard_migrations/versions/guard_0004_disconnected_claim_guard.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    assert "executable_new_capacity_ceiling = 0" in lowered
    assert "live_claim_entry_enabled = false" in lowered
    assert "executable = false" in lowered
    assert "insert into loom_capacity_guard.protected_claim_leases" not in lowered
    assert "insert into public.trials" not in lowered
    assert "update public.trials" not in lowered
    assert "delete from public.trials" not in lowered
    assert "update loom_capacity_guard.trial_attempts" not in lowered
    assert "sbatch" not in lowered
    assert "scancel" not in lowered


def test_legacy_fence_migration_is_inert_and_has_no_candidate_mutation() -> None:
    source = Path(
        "capacity_guard_migrations/versions/guard_0005_inert_legacy_authority_fence.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    assert "proposed_authority_mode = 'legacy-compatibility'" in lowered
    assert "activation_epoch = 0" in lowered
    assert "not new_submission_authority" in lowered
    assert "not new_claim_authority" in lowered
    assert "not scale_up_authority" in lowered
    assert "not cross_pool_placement_authority" in lowered
    assert "not global_allowance_authority" in lowered
    assert "not new_worker_authority" in lowered
    assert "not executable" in lowered
    assert "security definer" in lowered
    assert "to public" not in lowered
    assert "insert into public." not in lowered
    assert "update public." not in lowered
    assert "delete from public." not in lowered
    assert "insert into loom_capacity_guard.protected_claim_leases" not in lowered
    assert "sbatch" not in lowered
    assert "scancel" not in lowered


def test_lifecycle_demand_projection_is_read_only_and_nonexecutable() -> None:
    source = Path(
        "capacity_guard_migrations/versions/guard_0006_lifecycle_demand_projection.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    assert "capture_demand_observation_v1_legacy" in lowered
    assert "capture_lifecycle_demand_observation" in lowered
    assert "attempt_lifecycle_heads" in lowered
    assert "left join lateral" not in lowered
    assert "executable', false" in lowered
    assert "security definer" in lowered
    assert "to public" not in lowered
    assert "insert into public." not in lowered
    assert "update public." not in lowered
    assert "delete from public." not in lowered
    assert "insert into loom_capacity_guard.protected_claim_leases" not in lowered
    assert "insert into loom_capacity_guard.prepared_" not in lowered
    assert "sbatch" not in lowered
    assert "scancel" not in lowered


def test_inert_submission_registration_has_no_live_or_public_mutation() -> None:
    source = Path(
        "capacity_guard_migrations/versions/guard_0007_inert_trial_submission.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    assert "register_inert_trial_submission" in lowered
    assert "attempt_sequence" in lowered
    assert "execution_generation" in lowered
    assert "live_claim_entry_enabled = false" in lowered
    assert "executable', false" in lowered
    assert "security definer" in lowered
    assert "to public" not in lowered
    assert "insert into public." not in lowered
    assert "update public." not in lowered
    assert "delete from public." not in lowered
    assert "insert into loom_capacity_guard.protected_claim_leases" not in lowered
    assert "insert into loom_capacity_guard.prepared_" not in lowered
    assert "sbatch" not in lowered
    assert "scancel" not in lowered


def test_complete_mutation_inventory_migration_only_tightens_inert_policy() -> None:
    source = Path(
        "capacity_guard_migrations/versions/guard_0008_complete_mutation_inventory.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    assert "execution-attempt-queued-to-claimed" in lowered
    assert "pipeline-attempt-cancellation" in lowered
    assert "_assert_fence_is_empty" in lowered
    assert "create or replace function" not in lowered
    assert "insert into public." not in lowered
    assert "update public." not in lowered
    assert "delete from public." not in lowered
    assert "insert into loom_capacity_guard.protected_claim_leases" not in lowered
    assert "sbatch" not in lowered
    assert "scancel" not in lowered


def test_executable_intent_observation_migration_is_read_only_and_role_bounded() -> None:
    source = Path(
        "capacity_guard_migrations/versions/guard_0012_executable_intent_observation.py"
    ).read_text(encoding="utf-8")
    lowered = source.lower()
    assert "observe_executable_intent" in lowered
    assert "security definer" in lowered
    assert "session_user::text <> v_executor_role" in lowered
    assert "to public" not in lowered
    assert "insert into" not in lowered
    assert "update " not in lowered
    assert "delete from" not in lowered
    assert "truncate " not in lowered
    assert "sbatch" not in lowered
    assert "scancel" not in lowered
