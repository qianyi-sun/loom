"""Fail closed if the inert task-image authority acquires runtime composition."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

AUTHORITY_ROOT = Path("src/loom_task_image_authority")
PRODUCTION_ROOT = Path("src")
STORE_MODULE = "loom_task_image_authority.store"
ALLOWED_AUTHORITY_STDLIB_IMPORTS = {
    "asyncio",
    "base64",
    "binascii",
    "collections.abc",
    "contextlib",
    "dataclasses",
    "datetime",
    "hashlib",
    "hmac",
    "ipaddress",
    "json",
    "math",
    "os",
    "pathlib",
    "re",
    "secrets",
    "ssl",
    "stat",
    "time",
    "types",
    "typing",
    "urllib.parse",
    "uuid",
}
ALLOWED_AUTHORITY_IMPORT_ROOTS = {
    "cryptography",
    "fastapi",
    "jwt",
    "loom_task_image_authority",
    "prometheus_client",
    "pydantic",
    "pydantic_settings",
    "rfc8785",
    "sqlalchemy",
    "uvicorn",
}
ALLOWED_AUTHORITY_IMPORTS = {
    "__future__",
    "loom.db.schema",
    "loom.db.schema_startup",
    "loom.security.secret_store",
    "loom.task_image_build_plan",
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


def _unexpected_authority_imports(imports: set[str]) -> set[str]:
    return {
        imported
        for imported in imports
        if imported not in ALLOWED_AUTHORITY_IMPORTS
        and imported not in ALLOWED_AUTHORITY_STDLIB_IMPORTS
        and imported.partition(".")[0] not in ALLOWED_AUTHORITY_IMPORT_ROOTS
    }


def test_authority_package_has_only_the_closed_service_dependency_set() -> None:
    sources = sorted(AUTHORITY_ROOT.glob("*.py"))
    assert sources

    unexpected: dict[Path, list[str]] = {}
    for path in sources:
        imports = _imported_modules(_tree(path))
        disallowed = _unexpected_authority_imports(imports)
        if disallowed:
            unexpected[path] = sorted(disallowed)
    assert not unexpected


def test_authority_package_rejects_unreviewed_stdlib_import_roots() -> None:
    assert _unexpected_authority_imports({"asyncio"}) == set()
    assert _unexpected_authority_imports({"subprocess"}) == {"subprocess"}


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
