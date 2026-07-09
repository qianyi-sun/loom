"""Normalize Terminal-Bench-style ``task.toml`` files to Loom TaskConfig.

The 5003-task Source Useful bundle and similar Terminal-Bench imports ship
``task.toml`` files with top-level ``metadata`` instead of Loom's ``task``
section. The worker stores a Loom ``TaskConfig`` in the DB, while preserving the
uploaded bundle files for audit and verifier/runtime use.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

DEFAULT_AGENT_TIMEOUT_SEC = 360.0
DEFAULT_VERIFIER_TIMEOUT_SEC = 60.0
DEFAULT_VERIFIER_SCRIPT_PATH = "/app/verifier/run.sh"

_UNSUPPORTED_ENVIRONMENT_FIELDS: frozenset[str] = frozenset({
    "cpus", "memory", "storage",
})


def is_terminal_bench_shape(raw: dict[str, Any]) -> bool:
    """True if ``raw`` looks like a Terminal-Bench-style task.toml."""
    return isinstance(raw.get("metadata"), dict) and "task" not in raw


def normalize_terminal_bench_task_toml(raw: dict[str, Any]) -> dict[str, Any]:
    """Return a Loom-TaskConfig-shaped dict derived from a TB-shaped source.

    Idempotent for already-Loom-shaped inputs; the input object is never
    mutated.
    """
    payload = deepcopy(raw)
    if not is_terminal_bench_shape(payload):
        return payload

    metadata = payload.pop("metadata")
    payload.pop("version", None)
    payload.setdefault("schema_version", "1")

    task_section: dict[str, Any] = {}
    if "id" in metadata:
        task_section["id"] = metadata["id"]
    if "name" in metadata:
        task_section["name"] = metadata["name"]
    elif "id" in metadata:
        task_section["name"] = metadata["id"]
    if "description" in metadata:
        task_section["description"] = metadata["description"]
    tags = metadata.get("tags")
    if isinstance(tags, list) and all(isinstance(t, str) for t in tags):
        task_section["labels"] = list(tags)
    payload["task"] = task_section

    environment = payload.get("environment")
    if isinstance(environment, dict):
        for field in _UNSUPPORTED_ENVIRONMENT_FIELDS:
            environment.pop(field, None)
        environment.setdefault("os", "linux")
    else:
        payload["environment"] = {"os": "linux"}

    agent = payload.get("agent")
    if not isinstance(agent, dict):
        payload["agent"] = {
            "name": "oracle",
            "timeout_sec": DEFAULT_AGENT_TIMEOUT_SEC,
        }
    else:
        agent.setdefault("name", "oracle")
        agent.setdefault("timeout_sec", DEFAULT_AGENT_TIMEOUT_SEC)

    verifier = payload.get("verifier")
    if not isinstance(verifier, dict):
        payload["verifier"] = {
            "name": "script",
            "timeout_sec": DEFAULT_VERIFIER_TIMEOUT_SEC,
            "args": {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH},
        }
    else:
        verifier.setdefault("name", "script")
        verifier.setdefault("timeout_sec", DEFAULT_VERIFIER_TIMEOUT_SEC)
        args = verifier.get("args")
        if not isinstance(args, dict):
            verifier["args"] = {"script_path": DEFAULT_VERIFIER_SCRIPT_PATH}
        else:
            args.setdefault("script_path", DEFAULT_VERIFIER_SCRIPT_PATH)

    return payload


__all__ = [
    "DEFAULT_AGENT_TIMEOUT_SEC",
    "DEFAULT_VERIFIER_SCRIPT_PATH",
    "DEFAULT_VERIFIER_TIMEOUT_SEC",
    "is_terminal_bench_shape",
    "normalize_terminal_bench_task_toml",
]
