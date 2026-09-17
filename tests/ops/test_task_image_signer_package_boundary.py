"""Production signer composition remains separate from authority and allocations."""

import ast
import tomllib
from pathlib import Path

import pytest

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


def _unexpected_imports(path, modules):
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
    # Execution signing verifies a committed grant using two read-only journal
    # models and closed wire validators. Do not admit the issuer, start writer,
    # scheduler, or worker clients, nor expose these models to the listener.
    scoped = {
        "policy.py": {
            "uuid", "loom.db.schema", "loom_task_image_authority.execution_grant",
            "loom_task_image_authority.execution_signing_request",
        },
        "server.py": {
            "loom_task_image_authority.execution_grant",
            "loom_task_image_authority.execution_signing_request",
        },
    }.get(path.name, set()) if path.parent == ROOT else set()
    return {module for module in modules if not (
        module in allowed or module.split(".")[0] in allowed or module in authority or module in scoped
    )}


def test_signer_has_no_scheduler_registry_executor_or_allocation_dependencies():
    for path in ROOT.glob("*.py"):
        assert not _unexpected_imports(path, imports(path)), path
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module == "loom.db.schema":
                assert path == ROOT / "policy.py"
                assert {alias.name for alias in node.names} <= {"TaskImageExecutionGrant", "TaskImageExecutionStart"}
            elif isinstance(node, ast.Import):
                assert all(alias.name != "loom.db.schema" for alias in node.names)


@pytest.mark.parametrize("module", [
    "loom_control_plane.scheduler", "loom_worker.control_plane_client",
    "loom_task_image_authority.execution_store", "loom_task_image_authority.execution_start",
    "docker", "boto3", "subprocess",
])
@pytest.mark.parametrize("filename", ["policy.py", "server.py", "runtime.py"])
def test_signer_dependency_check_rejects_runtime_clients(filename, module):
    assert _unexpected_imports(ROOT / filename, {module}) == {module}


def test_execution_journal_import_is_scoped_to_signing_policy():
    for path in ROOT.glob("*.py"):
        assert bool(_unexpected_imports(path, {"loom.db.schema"})) == (path.name != "policy.py")


def test_private_key_provider_cannot_generate_missing_keys_or_enter_authority():
    importers = {path for path in Path("src").rglob("*.py") if "loom_task_image_signer.keys" in imports(path)}
    assert importers == {ROOT / "runtime.py"}
    tree = ast.parse((ROOT / "keys.py").read_text())
    assert not any(isinstance(node, ast.Attribute) and node.attr == "generate" for node in ast.walk(tree))
