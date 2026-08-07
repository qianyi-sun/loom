#!/usr/bin/env python3
"""Fail closed when the controlled GitHub Actions upgrade policy drifts."""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_POLICY = REPO_ROOT / "config" / "ci-upgrade-policy.json"
DEFAULT_LOCK = REPO_ROOT / "config" / "ci-actions-lock.json"
REQUIRED_CONTEXTS = {
    "repository-checks",
    "images-gate",
    "cluster-smoke-gate",
    "staging-smoke-gate",
}
SHA_RE = re.compile(r"[0-9a-f]{40}")
VERSION_RE = re.compile(r"v[0-9][A-Za-z0-9._-]*")


def _load(path: Path) -> tuple[Any, list[str]]:
    try:
        return json.loads(path.read_text(encoding="utf-8")), []
    except (OSError, json.JSONDecodeError) as exc:
        return None, [f"{path}: cannot load JSON: {exc}"]


def _valid_pin(value: Any) -> bool:
    return (
        isinstance(value, Mapping)
        and set(value) == {"sha", "version"}
        and isinstance(value.get("sha"), str)
        and SHA_RE.fullmatch(value["sha"]) is not None
        and isinstance(value.get("version"), str)
        and VERSION_RE.fullmatch(value["version"]) is not None
    )


def check_upgrade_policy(*, policy_file: Path, lock_file: Path) -> tuple[str, ...]:
    policy, errors = _load(policy_file)
    lock, lock_errors = _load(lock_file)
    errors.extend(lock_errors)
    if not isinstance(policy, Mapping) or not isinstance(lock, Mapping):
        return tuple(errors or ["policy and lock roots must be JSON objects"])
    expected_policy_keys = {
        "schema_version",
        "max_actions_per_batch",
        "node24_minimum_runner",
        "required_canary_contexts",
        "batches",
    }
    if set(policy) != expected_policy_keys:
        errors.append(f"{policy_file}: unexpected policy fields")
    if type(policy.get("schema_version")) is not int or policy.get("schema_version") != 1:
        errors.append(f"{policy_file}: schema_version must be integer 1")
    maximum = policy.get("max_actions_per_batch")
    if type(maximum) is not int or not 1 <= maximum <= 2:
        errors.append(f"{policy_file}: max_actions_per_batch must be 1 or 2")
        maximum = 0
    if policy.get("node24_minimum_runner") != "2.327.1":
        errors.append(f"{policy_file}: Node 24 runner floor must remain 2.327.1")
    contexts = policy.get("required_canary_contexts")
    if not isinstance(contexts, list) or set(contexts) != REQUIRED_CONTEXTS:
        errors.append(f"{policy_file}: all four required canary contexts must be exact")
    lock_actions = lock.get("actions")
    if not isinstance(lock_actions, Mapping):
        errors.append(f"{lock_file}: actions must be an object")
        return tuple(errors)
    batches = policy.get("batches")
    if not isinstance(batches, list) or not batches:
        errors.append(f"{policy_file}: batches must be a non-empty array")
        return tuple(errors)
    observed: list[str] = []
    names: set[str] = set()
    for index, batch in enumerate(batches):
        where = f"{policy_file}: batches[{index}]"
        if not isinstance(batch, Mapping) or set(batch) != {
            "name",
            "actions",
            "compatibility_tests",
            "rollback",
        }:
            errors.append(f"{where}: batch fields are invalid")
            continue
        name = batch.get("name")
        if not isinstance(name, str) or not name or name in names:
            errors.append(f"{where}: name must be unique and non-empty")
        else:
            names.add(name)
        actions = batch.get("actions")
        tests = batch.get("compatibility_tests")
        rollback = batch.get("rollback")
        if (
            not isinstance(actions, list)
            or not actions
            or any(not isinstance(action, str) for action in actions)
            or len(actions) > maximum
            or len(actions) != len(set(actions))
        ):
            errors.append(f"{where}: actions must contain 1-{maximum} unique action names")
            continue
        observed.extend(actions)
        if (
            not isinstance(tests, list)
            or not tests
            or any(not isinstance(command, str) or not command.strip() for command in tests)
        ):
            errors.append(f"{where}: compatibility_tests must contain commands")
        if not isinstance(rollback, Mapping) or set(rollback) != set(actions):
            errors.append(f"{where}: rollback must cover exactly the batch actions")
        elif any(not _valid_pin(pin) for pin in rollback.values()):
            errors.append(f"{where}: every rollback entry must be a SHA/version pin")
    if len(observed) != len(set(observed)):
        errors.append(f"{policy_file}: each action must occur in exactly one batch")
    if set(observed) != set(lock_actions):
        errors.append(f"{policy_file}: batches must cover exactly the action lock")
    return tuple(errors)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--policy-file", type=Path, default=DEFAULT_POLICY)
    parser.add_argument("--lock-file", type=Path, default=DEFAULT_LOCK)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    errors = check_upgrade_policy(
        policy_file=args.policy_file.resolve(),
        lock_file=args.lock_file.resolve(),
    )
    if errors:
        for error in errors:
            print(error)
        return 1
    print("CI upgrade policy is valid and covers every locked action exactly once")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
