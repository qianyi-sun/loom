"""Fail closed when GitHub workflow actions drift from the verified SHA lock.

Every remote GitHub Action must use a lowercase, full 40-character commit SHA
that exactly matches ``config/ci-actions-lock.json``. Local ``./`` actions are
allowed, while ``docker://`` actions must use a full ``sha256`` digest.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
DEFAULT_LOCK_FILE = REPO_ROOT / "config" / "ci-actions-lock.json"

_COMMIT_SHA = re.compile(r"[0-9a-f]{40}")
_DOCKER_DIGEST = re.compile(r"docker://[^\s@]+@sha256:[0-9a-f]{64}")
_ACTION_SEGMENT = re.compile(r"[A-Za-z0-9_.-]+")
_VERSION = re.compile(r"v[0-9][A-Za-z0-9._-]*")


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate mapping keys."""


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.nodes.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True)
class LockedAction:
    sha: str
    version: str


@dataclass(frozen=True)
class UsesReference:
    workflow: Path
    location: str
    value: str

    def describe(self) -> str:
        return f"{self.workflow}:{self.location}"


@dataclass(frozen=True)
class CheckResult:
    workflow_count: int
    reference_count: int
    remote_actions: tuple[str, ...]
    errors: tuple[str, ...]


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _load_lock(path: Path) -> tuple[dict[str, LockedAction], list[str]]:
    errors: list[str] = []
    try:
        raw = json.loads(
            path.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_json_object,
        )
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        return {}, [f"{path}: cannot load action lock: {exc}"]

    if not isinstance(raw, dict):
        return {}, [f"{path}: lock root must be a JSON object"]
    if set(raw) != {"schema_version", "actions"}:
        errors.append(
            f"{path}: lock root must contain exactly 'schema_version' and 'actions'",
        )
    if type(raw.get("schema_version")) is not int or raw.get("schema_version") != 1:
        errors.append(f"{path}: schema_version must be integer 1")

    actions_raw = raw.get("actions")
    if not isinstance(actions_raw, dict):
        errors.append(f"{path}: actions must be a JSON object")
        return {}, errors

    actions: dict[str, LockedAction] = {}
    for action_name, entry in sorted(actions_raw.items()):
        if not isinstance(action_name, str) or not _valid_action_root(action_name):
            errors.append(
                f"{path}: action key {action_name!r} must have owner/repository form",
            )
            continue
        if not isinstance(entry, dict) or set(entry) != {"sha", "version"}:
            errors.append(
                f"{path}: {action_name} must contain exactly 'sha' and 'version'",
            )
            continue
        sha = entry.get("sha")
        version = entry.get("version")
        if not isinstance(sha, str) or _COMMIT_SHA.fullmatch(sha) is None:
            errors.append(f"{path}: {action_name}.sha must be a lowercase full commit SHA")
            continue
        if not isinstance(version, str) or _VERSION.fullmatch(version) is None:
            errors.append(f"{path}: {action_name}.version must be a non-empty v-prefixed tag")
            continue
        actions[action_name] = LockedAction(sha=sha, version=version)

    return actions, errors


def _valid_action_root(value: str) -> bool:
    parts = value.split("/")
    return len(parts) == 2 and all(_ACTION_SEGMENT.fullmatch(part) for part in parts)


def _workflow_paths(workflows_dir: Path) -> list[Path]:
    return sorted(
        {
            *workflows_dir.rglob("*.yml"),
            *workflows_dir.rglob("*.yaml"),
        },
    )


def _collect_uses(value: Any, *, workflow: Path, location: str = "root") -> list[UsesReference]:
    references: list[UsesReference] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_location = f"{location}.{key}"
            if key == "uses":
                if isinstance(child, str):
                    references.append(
                        UsesReference(
                            workflow=workflow,
                            location=child_location,
                            value=child,
                        ),
                    )
                else:
                    references.append(
                        UsesReference(
                            workflow=workflow,
                            location=child_location,
                            value=f"<non-string:{type(child).__name__}>",
                        ),
                    )
            references.extend(
                _collect_uses(child, workflow=workflow, location=child_location),
            )
    elif isinstance(value, list):
        for index, child in enumerate(value):
            references.extend(
                _collect_uses(
                    child,
                    workflow=workflow,
                    location=f"{location}[{index}]",
                ),
            )
    return references


def _read_workflows(paths: Sequence[Path]) -> tuple[list[UsesReference], list[str]]:
    references: list[UsesReference] = []
    errors: list[str] = []
    for path in paths:
        try:
            document = yaml.load(path.read_text(encoding="utf-8"), Loader=_UniqueKeyLoader)
        except (OSError, yaml.YAMLError) as exc:
            errors.append(f"{path}: cannot parse workflow: {exc}")
            continue
        references.extend(_collect_uses(document, workflow=path))
    return references, errors


def check_action_pins(*, workflows_dir: Path, lock_file: Path) -> CheckResult:
    """Validate every workflow ``uses`` reference against the action lock."""
    locked_actions, errors = _load_lock(lock_file)
    workflow_paths = _workflow_paths(workflows_dir)
    if not workflow_paths:
        errors.append(f"{workflows_dir}: no .yml or .yaml workflow files found")

    references, workflow_errors = _read_workflows(workflow_paths)
    errors.extend(workflow_errors)
    observed_shas: dict[str, set[str]] = {}

    for reference in references:
        value = reference.value
        description = reference.describe()
        if value.startswith("<non-string:"):
            errors.append(f"{description}: uses must be a string, got {value}")
            continue
        if value != value.strip():
            errors.append(f"{description}: uses must not contain surrounding whitespace")
            continue
        if "${{" in value or "}}" in value:
            errors.append(f"{description}: expressions are forbidden in uses: {value!r}")
            continue
        if value.startswith("./"):
            continue
        if value.startswith("docker://"):
            if _DOCKER_DIGEST.fullmatch(value) is None:
                errors.append(
                    f"{description}: docker action must use a lowercase sha256 digest: {value!r}",
                )
            continue

        source, separator, revision = value.rpartition("@")
        source_parts = source.split("/")
        if (
            separator != "@"
            or len(source_parts) < 2
            or any(_ACTION_SEGMENT.fullmatch(part) is None for part in source_parts)
        ):
            errors.append(
                f"{description}: remote action must use owner/repository[/path]@<full-sha>: "
                f"{value!r}",
            )
            continue

        action_name = "/".join(source_parts[:2])
        if _COMMIT_SHA.fullmatch(revision) is None:
            errors.append(
                f"{description}: {action_name} must use a lowercase full commit SHA, "
                f"not {revision!r}",
            )
            continue
        observed_shas.setdefault(action_name, set()).add(revision)
        locked = locked_actions.get(action_name)
        if locked is None:
            errors.append(
                f"{description}: {action_name} is not declared in {lock_file}",
            )
        elif revision != locked.sha:
            errors.append(
                f"{description}: {action_name}@{revision} does not match locked SHA {locked.sha}",
            )

    for action_name, shas in sorted(observed_shas.items()):
        if len(shas) > 1:
            errors.append(
                f"{action_name}: multiple commit SHAs are used across workflows: "
                f"{', '.join(sorted(shas))}",
            )

    stale_actions = sorted(set(locked_actions) - set(observed_shas))
    for action_name in stale_actions:
        errors.append(f"{lock_file}: stale action lock entry is not used: {action_name}")

    return CheckResult(
        workflow_count=len(workflow_paths),
        reference_count=len(references),
        remote_actions=tuple(sorted(observed_shas)),
        errors=tuple(errors),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflows-dir", type=Path, default=DEFAULT_WORKFLOWS_DIR)
    parser.add_argument("--lock-file", type=Path, default=DEFAULT_LOCK_FILE)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = check_action_pins(
        workflows_dir=args.workflows_dir.resolve(),
        lock_file=args.lock_file.resolve(),
    )
    if result.errors:
        for error in result.errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print(
        f"Validated {result.workflow_count} workflow files, "
        f"{result.reference_count} uses references, and "
        f"{len(result.remote_actions)} locked remote actions.",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
