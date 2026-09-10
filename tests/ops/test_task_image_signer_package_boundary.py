"""Production signer composition remains separate from authority and allocations."""

import ast
import tomllib
from pathlib import Path

ROOT = Path("src/loom_task_image_signer")


def imports(path):
    return {
        node.module if isinstance(node, ast.ImportFrom) else alias.name
        for node in ast.walk(ast.parse(path.read_text()))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
        if not isinstance(node, ast.ImportFrom) or node.module is not None
    }


def test_signer_is_only_composed_by_its_own_dedicated_package():
    offenders = {
        path for path in Path("src").rglob("*.py") if not path.is_relative_to(ROOT)
        and any(module == "loom_task_image_signer" or module.startswith("loom_task_image_signer.") for module in imports(path))
    }
    assert not offenders
    scripts = tomllib.loads(Path("pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["loom-task-image-signer"] == "loom_task_image_signer.__main__:main"


def test_signer_has_no_scheduler_registry_executor_or_allocation_dependencies():
    allowed = {
        "__future__", "argparse", "asyncio", "base64", "collections.abc", "contextlib",
        "dataclasses", "datetime", "hashlib", "hmac", "json", "math", "os", "pathlib",
        "re", "signal", "socket", "ssl", "stat", "typing", "cryptography", "h11",
        "pydantic", "sqlalchemy", "loom_task_image_signer",
    }
    authority = {
        "loom_task_image_authority.config", "loom_task_image_authority.contracts",
        "loom_task_image_authority.keyset_signing_request",
        "loom_task_image_authority.publication_contracts", "loom_task_image_authority.publication_keyset",
        "loom_task_image_authority.publication_keyset_store", "loom_task_image_authority.publication_signing",
    }
    for path in ROOT.glob("*.py"):
        assert all(module in allowed or module.split(".")[0] in allowed or module in authority for module in imports(path)), path


def test_private_key_provider_cannot_generate_missing_keys_or_enter_authority():
    importers = {path for path in Path("src").rglob("*.py") if "loom_task_image_signer.keys" in imports(path)}
    assert importers == {ROOT / "runtime.py"}
    tree = ast.parse((ROOT / "keys.py").read_text())
    assert not any(isinstance(node, ast.Attribute) and node.attr == "generate" for node in ast.walk(tree))
