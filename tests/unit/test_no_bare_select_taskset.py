"""Reject bare ``select(TaskSet)`` outside the visibility helper (#242)."""

from __future__ import annotations

import ast
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src"

ALLOWLIST = {
    SRC_ROOT / "loom" / "db" / "task_set_visibility.py",
    # The internal materializer holds a job lease before taking a TaskSet lock;
    # it is not a user-visible read path.
    SRC_ROOT / "loom_service" / "taskset_materializer.py",
    SRC_ROOT / "loom_service" / "taskset_gc.py",
}


def _is_bare_select_taskset(node: ast.AST) -> bool:
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    if not isinstance(func, ast.Name) or func.id != "select":
        return False
    if len(node.args) != 1:
        return False
    arg = node.args[0]
    return isinstance(arg, ast.Name) and arg.id == "TaskSet"


def _violations_in_file(path: Path) -> list[tuple[int, str]]:
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(path))
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if _is_bare_select_taskset(node) and isinstance(node, ast.Call):
            hits.append((node.lineno, ast.get_source_segment(
                source, node,
            ) or "select(TaskSet)"))
    return hits


def test_no_bare_select_taskset_outside_allowlist() -> None:
    violations: list[str] = []
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if path in ALLOWLIST:
            continue
        for lineno, snippet in _violations_in_file(path):
            rel = path.relative_to(REPO_ROOT)
            violations.append(f"{rel}:{lineno}: {snippet}")
    assert violations == []
