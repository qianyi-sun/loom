"""Fail closed if the inert task-image authority acquires runtime composition."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

AUTHORITY_ROOT = Path("src/loom_task_image_authority")
PRODUCTION_ROOT = Path("src")
STORE_MODULE = "loom_task_image_authority.store"
ALLOWED_AUTHORITY_IMPORTS = {
    "__future__",
    "asyncio",
    "base64",
    "binascii",
    "collections.abc",
    "contextlib",
    "dataclasses",
    "datetime",
    "fastapi",
    "fastapi.encoders",
    "fastapi.exceptions",
    "fastapi.responses",
    "hashlib",
    "hmac",
    "json",
    "loom.db.schema",
    "loom.db.schema_startup",
    "loom.security.secret_store",
    "loom_task_image_authority.api",
    "loom_task_image_authority.auth",
    "loom_task_image_authority.config",
    "loom_task_image_authority.contracts",
    "loom_task_image_authority.store",
    "os",
    "pathlib",
    "prometheus_client",
    "pydantic",
    "pydantic_settings",
    "re",
    "rfc8785",
    "secrets",
    "ssl",
    "stat",
    "sqlalchemy",
    "sqlalchemy.ext.asyncio",
    "time",
    "typing",
    "types",
    "uuid",
    "uvicorn",
}


def _tree(path: Path) -> ast.Module:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _imported_modules(tree: ast.AST) -> set[str]:
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    return modules


def _imports_store(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(
                alias.name == STORE_MODULE
                or alias.name.startswith(f"{STORE_MODULE}.")
                for alias in node.names
            ):
                return True
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            if node.module == STORE_MODULE or node.module.startswith(
                f"{STORE_MODULE}."
            ):
                return True
            if node.module == "loom_task_image_authority" and any(
                alias.name == "store" for alias in node.names
            ):
                return True
    return False


def test_authority_package_has_only_the_closed_service_dependency_set() -> None:
    sources = sorted(AUTHORITY_ROOT.glob("*.py"))
    assert sources

    unexpected: dict[Path, list[str]] = {}
    for path in sources:
        imports = _imported_modules(_tree(path))
        if disallowed := imports - ALLOWED_AUTHORITY_IMPORTS:
            unexpected[path] = sorted(disallowed)
    assert not unexpected


def test_only_the_dedicated_authority_api_imports_the_projection_store() -> None:
    importers = {
        path
        for path in PRODUCTION_ROOT.rglob("*.py")
        if _imports_store(_tree(path))
    }
    assert importers == {AUTHORITY_ROOT / "api.py"}


def test_no_other_production_package_imports_the_authority_api() -> None:
    api_module = "loom_task_image_authority.api"
    importers = {
        path
        for path in PRODUCTION_ROOT.rglob("*.py")
        if not path.is_relative_to(AUTHORITY_ROOT)
        and api_module in _imported_modules(_tree(path))
    }
    assert not importers


def test_authority_api_has_no_slurm_worker_provider_or_public_route_dependency() -> None:
    imports = _imported_modules(_tree(AUTHORITY_ROOT / "api.py"))
    forbidden_prefixes = (
        "docker",
        "loom_capacity_",
        "loom_control_plane",
        "loom_service",
        "loom_worker",
        "subprocess",
    )

    assert not {
        imported
        for imported in imports
        if imported.startswith(forbidden_prefixes)
    }


def test_authority_package_is_in_default_static_validation() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert "src/loom_task_image_authority" in project["tool"]["mypy"]["files"]
