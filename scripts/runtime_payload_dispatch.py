#!/usr/bin/env python3
"""Execute one constrained runtime-payload verifier module inside a sandbox."""

from __future__ import annotations

import argparse
import ast
import hashlib
import importlib.util
import inspect
import json
from collections.abc import Callable
from pathlib import Path
from types import ModuleType
from typing import Any


def _declared_tests(source: str, *, logical_path: str) -> tuple[str, ...]:
    tree = ast.parse(source, filename=logical_path)
    names: list[str] = []
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and (
            node.name.startswith("Test")
            or any(
                isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef))
                and item.name.startswith("test_")
                for item in node.body
            )
        ):
            raise RuntimeError(f"class-based payload tests are unsupported: {logical_path}")
        if isinstance(node, ast.AsyncFunctionDef) and node.name.startswith("test_"):
            raise RuntimeError(f"async payload tests are unsupported: {logical_path}")
        if not isinstance(node, ast.FunctionDef) or not node.name.startswith("test_"):
            continue
        if node.decorator_list:
            raise RuntimeError(
                f"decorated payload tests require a new execution policy: "
                f"{logical_path}::{node.name}"
            )
        arguments = node.args
        if (
            arguments.posonlyargs
            or arguments.args
            or arguments.kwonlyargs
            or arguments.vararg is not None
            or arguments.kwarg is not None
        ):
            raise RuntimeError(
                f"payload test fixtures are unsupported: {logical_path}::{node.name}"
            )
        names.append(node.name)
    if not names:
        raise RuntimeError(f"runtime payload module has no executable tests: {logical_path}")
    if len(names) != len(set(names)):
        raise RuntimeError(f"runtime payload module repeats a test name: {logical_path}")
    return tuple(sorted(names))


def _load_module(path: Path, *, logical_path: str) -> ModuleType:
    identity = hashlib.sha256(logical_path.encode("utf-8")).hexdigest()[:16]
    spec = importlib.util.spec_from_file_location(f"runtime_payload_{identity}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load runtime payload module: {logical_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def execute(path: Path, *, logical_path: str) -> tuple[str, ...]:
    source = path.read_text(encoding="utf-8")
    declared = _declared_tests(source, logical_path=logical_path)
    module = _load_module(path, logical_path=logical_path)
    discovered: dict[str, Callable[[], Any]] = {
        name: value
        for name, value in vars(module).items()
        if name.startswith("test_")
        and callable(value)
        and getattr(value, "__module__", None) == module.__name__
    }
    if tuple(sorted(discovered)) != declared:
        raise RuntimeError(
            f"loaded payload tests differ from declared tests: {logical_path}: "
            f"declared={declared!r} loaded={tuple(sorted(discovered))!r}"
        )
    executed: list[str] = []
    for name in declared:
        test = discovered[name]
        if inspect.iscoroutinefunction(test):
            raise RuntimeError(f"async payload test is unsupported: {logical_path}::{name}")
        result = test()
        if inspect.isawaitable(result):
            raise RuntimeError(f"awaitable payload result is unsupported: {logical_path}::{name}")
        if result is not None:
            raise RuntimeError(f"payload test returned a value: {logical_path}::{name}")
        executed.append(name)
    return tuple(executed)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--test-file", type=Path, required=True)
    parser.add_argument("--logical-path", required=True)
    args = parser.parse_args()
    executed = execute(args.test_file, logical_path=args.logical_path)
    print(
        json.dumps(
            {
                "schema_version": 1,
                "path": args.logical_path,
                "payload_sha256": hashlib.sha256(args.test_file.read_bytes()).hexdigest(),
                "executed": executed,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
