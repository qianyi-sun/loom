"""Fail closed if the inert guard foundation acquires executable authority."""

from __future__ import annotations

import ast
from pathlib import Path

GUARD_ROOT = Path("src/loom_capacity_guard")
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


def test_capacity_guard_migrations_have_no_candidate_database_fallback() -> None:
    source = Path("capacity_guard_migrations/env.py").read_text(encoding="utf-8")
    assert "LOOM_CAPACITY_GUARD_DB_URL" in source
    assert "LOOM_CAPACITY_GUARD_OWNER_ROLE" in source
    assert "LOOM_DB_URL" not in source
    assert "LOOM_CP_DB_URL" not in source
    assert "LOOM_CAPACITY_DB_URL" not in source
