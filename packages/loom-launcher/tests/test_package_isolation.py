"""Package-isolation guarantee.

`loom-launcher` ships to PyPI for installation inside sandbox containers
that don't (and shouldn't) have the full `loom` server-side stack
(sqlalchemy, fastapi, alembic, boto3, etc.). The launcher must depend
ONLY on its declared deps in pyproject.toml. A stray `from loom.x import
y` in any adapter or capture module would silently break sandbox
installs — this test guards against that regression.
"""

from __future__ import annotations

import ast
from pathlib import Path

_PACKAGE_ROOT = Path(__file__).resolve().parents[1] / "loom_launcher"

_ALLOWED_TOP_LEVEL_PREFIXES = frozenset({
    "__future__",
    "collections",
    "dataclasses",
    "logging",
    "pathlib",
    "typing",
    "uuid",
    "re",
    "json",
    "asyncio",
    "shlex",
    # External deps from pyproject.toml [project.dependencies]:
    "pydantic",
    "httpx",
    # Internal launcher imports:
    "loom_launcher",
})


def _walk_imports(py_file: Path) -> list[str]:
    """Return every top-level import name in `py_file`."""
    tree = ast.parse(py_file.read_text())
    names: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                names.append(alias.name.split(".", 1)[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                names.append(node.module.split(".", 1)[0])
    return names


def test_no_loom_imports_in_launcher() -> None:
    """No source file under loom_launcher/ may import from `loom.*`.
    The launcher is sandbox-bound; loom proper has fastapi/sqlalchemy
    transitive deps incompatible with minimal Python images."""
    offenders: list[tuple[str, str]] = []
    for py_file in sorted(_PACKAGE_ROOT.rglob("*.py")):
        for name in _walk_imports(py_file):
            if name == "loom":
                offenders.append((str(py_file.relative_to(_PACKAGE_ROOT)), name))
    assert not offenders, (
        "loom-launcher must not import loom.*; found:\n"
        + "\n".join(f"  {f}: import {n}" for f, n in offenders)
    )


def test_only_declared_dependencies_used() -> None:
    """Every top-level import must come from the allowlist (stdlib +
    declared PyPI deps + loom_launcher itself)."""
    unexpected: dict[str, set[str]] = {}
    for py_file in sorted(_PACKAGE_ROOT.rglob("*.py")):
        for name in _walk_imports(py_file):
            if name in _ALLOWED_TOP_LEVEL_PREFIXES:
                continue
            # builtins / stdlib heuristic: lowercase module that imports
            # cleanly without an external dep manifest.
            unexpected.setdefault(
                str(py_file.relative_to(_PACKAGE_ROOT)),
                set(),
            ).add(name)

    # Allow for stdlib modules we haven't enumerated above. Check each
    # offender is actually a stdlib module.
    import sys
    really_external: dict[str, set[str]] = {}
    stdlib = set(sys.stdlib_module_names)
    for file, names in unexpected.items():
        leftover = names - stdlib
        if leftover:
            really_external[file] = leftover

    assert not really_external, (
        "loom-launcher source imports modules outside the allowlist + "
        "stdlib. Either declare them in pyproject.toml or add them to "
        "_ALLOWED_TOP_LEVEL_PREFIXES if they're newly-allowed stdlib "
        f"modules:\n{really_external}"
    )
