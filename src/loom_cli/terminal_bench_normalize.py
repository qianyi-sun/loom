"""Normalize Terminal-Bench-style ``task.toml`` files to Loom TaskConfig (#341).

The 5003-task Source Useful bundle (and similar user-provided Terminal-Bench
imports) ship task.toml files whose top-level shape looks like::

    version = "1"

    [metadata]
    id = "task-1"
    name = "Task One"

    [environment]
    cpus = 2
    memory = "4G"
    storage = "10G"
    dockerfile = "Dockerfile"

Loom's on-disk :class:`~loom.models.task.TaskConfig` schema expects
``[task]`` instead of ``[metadata]``, requires ``environment.os``,
``agent.name``, ``verifier.name``, and rejects the Terminal-Bench resource
fields (``cpus``, ``memory``, ``storage``). The strict validator therefore
rejects TB bundles wholesale, blocking ``loom datasets publish-local``.

This module detects TB-shaped task.toml payloads and produces a valid
Loom TaskConfig dict with the following mapping:

===========================  =================================================
Terminal-Bench (input)       Loom (output)
===========================  =================================================
``version`` (top-level)      dropped; Loom uses ``schema_version = "1"``
``metadata.id``              ``task.id``
``metadata.name``            ``task.name`` (falls back to ``metadata.id``)
``metadata.description``     ``task.description``
``metadata.tags`` (list)     ``task.labels``
``environment.cpus``         dropped (Loom's scheduler models capacity via
``environment.memory``       ``requires_caps``, not per-task hints)
``environment.storage``      dropped
other ``environment.*``      preserved verbatim
``agent`` (optional)         preserved; ``name`` defaults to ``"oracle"``
``verifier`` (optional)      preserved; ``name`` defaults to ``"script"``
===========================  =================================================

Defaults injected when the source is silent:

* ``schema_version = "1"``
* ``environment.os = "linux"``
* ``agent = {name = "oracle", timeout_sec = 360}``
* ``verifier = {name = "script", timeout_sec = 60, args = {script_path = "/app/verifier/run.sh"}}``

Explicit user choices are always preserved. The normalizer is a pure
function over the parsed TOML dict; caller decides whether to persist the
original bundle unchanged (yes — the on-disk task.toml is uploaded to S3
verbatim so audit/repro is intact) and where to hand the normalized dict
(the DB row's ``config`` JSONB — that's what the worker validates).
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
    """True if ``raw`` looks like a Terminal-Bench-style task.toml.

    Heuristic: top-level ``metadata`` section is present AND top-level
    ``task`` section is absent. Loom's schema always uses ``[task]``;
    TB bundles always use ``[metadata]``, so the two are mutually
    exclusive in practice.
    """
    return isinstance(raw.get("metadata"), dict) and "task" not in raw


def normalize_terminal_bench_task_toml(raw: dict[str, Any]) -> dict[str, Any]:
    """Return a Loom-TaskConfig-shaped dict derived from a TB-shaped source.

    Idempotent for already-Loom-shaped inputs: if
    :func:`is_terminal_bench_shape` returns False, the input is returned
    unchanged (a deep copy for safety).

    Does NOT mutate the input.
    """
    payload = deepcopy(raw)
    if not is_terminal_bench_shape(payload):
        return payload

    metadata = payload.pop("metadata")
    payload.pop("version", None)  # drop TB schema-version — Loom uses "1"
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
