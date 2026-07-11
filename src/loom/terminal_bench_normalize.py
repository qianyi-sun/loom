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
_TB21_VERIFIER_ARTIFACT_GLOB = "logs/verifier/**"

_UNSUPPORTED_ENVIRONMENT_FIELDS: frozenset[str] = frozenset(
    {
        "cpus",
        "memory",
        "storage",
    }
)


def is_terminal_bench_shape(raw: dict[str, Any]) -> bool:
    """True if ``raw`` looks like a Terminal-Bench-style task.toml."""
    if isinstance(raw.get("metadata"), dict) and "task" not in raw:
        return True
    # Harbor-native Terminal-Bench 2.1 packages use schema 1.1. Their
    # ``[task]`` section has the upstream task name but no Loom task id; all
    # native-only metadata remains in ``upstream-task.toml`` after conversion.
    task = raw.get("task")
    return raw.get("schema_version") == "1.1" and isinstance(task, dict) and "id" not in task


def normalize_terminal_bench_task_toml(raw: dict[str, Any]) -> dict[str, Any]:
    """Return a Loom-TaskConfig-shaped dict derived from a TB-shaped source.

    Idempotent for already-Loom-shaped inputs; the input object is never
    mutated.
    """
    payload = deepcopy(raw)
    if not is_terminal_bench_shape(payload):
        return payload

    if payload.get("schema_version") == "1.1" and isinstance(
        payload.get("task"),
        dict,
    ):
        return _normalize_native_tb21_task_toml(payload)

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


def _normalize_native_tb21_task_toml(payload: dict[str, Any]) -> dict[str, Any]:
    """Project a Harbor-native TB2.1 task into Loom's runnable schema.

    Native-only resource, internet, architecture, verifier-env, and solution
    metadata is deliberately not invented in the Loom projection. The adapter
    retains the complete source bytes alongside this normalized file as
    ``upstream-task.toml`` for later profile provenance and preflight work.
    """
    source_task = payload.get("task")
    if not isinstance(source_task, dict):  # protected by shape detection
        return payload
    source_name = source_task.get("name")
    if not isinstance(source_name, str) or not source_name:
        return payload

    task: dict[str, Any] = {"id": source_name, "name": source_name}
    description = source_task.get("description")
    if isinstance(description, str):
        task["description"] = description
    labels = source_task.get("labels")
    if not isinstance(labels, list):
        labels = source_task.get("keywords")
    if isinstance(labels, list) and all(isinstance(item, str) for item in labels):
        task["labels"] = list(labels)

    source_environment = payload.get("environment")
    source_environment = source_environment if isinstance(source_environment, dict) else {}
    environment: dict[str, Any] = {
        "os": source_environment.get("os", "linux"),
        # Harbor-native TB2.1 images and the verifier bridge use /app. Without
        # this explicit projection Loom defaults to /workspace while the
        # normalized script verifier still points at /app/verifier/run.sh.
        "workdir": source_environment.get("workdir", "/app"),
    }
    for field in (
        "cpu_arch",
        "gpu_vendor",
        "docker_image",
        "dockerfile",
        "docker_build_context",
        "extra_hosts",
        "dns",
        "tmpfs",
        "healthcheck",
        "workdir",
        "user",
        "network_policies_supported",
        "baseline_network_policy",
        "skills_dir",
        "mcp_servers",
        "build_timeout_sec",
        "sidecars",
    ):
        if field in source_environment:
            environment[field] = deepcopy(source_environment[field])
    architecture = source_environment.get("architecture")
    if architecture in {"x86_64", "arm64", "any"}:
        environment["cpu_arch"] = architecture
    elif architecture == "amd64":
        environment["cpu_arch"] = "x86_64"
    source_env = source_environment.get("environment")
    if not isinstance(source_env, dict):
        source_env = source_environment.get("env")
    if isinstance(source_env, dict):
        environment["environment"] = {str(key): str(value) for key, value in source_env.items()}

    source_agent = payload.get("agent")
    source_agent = source_agent if isinstance(source_agent, dict) else {}
    agent: dict[str, Any] = {"name": source_agent.get("name", "oracle")}
    for field in (
        "version",
        "model",
        "timeout_sec",
        "setup_timeout_sec",
        "user",
        "extra_mcp_servers",
        "skills",
    ):
        if field in source_agent:
            agent[field] = deepcopy(source_agent[field])

    source_verifier = payload.get("verifier")
    source_verifier = source_verifier if isinstance(source_verifier, dict) else {}
    verifier: dict[str, Any] = {
        "name": source_verifier.get("name", "script"),
        "args": deepcopy(source_verifier.get("args", {})),
    }
    if not isinstance(verifier["args"], dict):
        verifier["args"] = {}
    verifier["args"].setdefault("script_path", DEFAULT_VERIFIER_SCRIPT_PATH)
    for field in ("timeout_sec", "env_mode", "user"):
        if field in source_verifier:
            verifier[field] = deepcopy(source_verifier[field])

    artifacts = payload.get("artifacts")
    source_artifacts = (
        list(artifacts)
        if isinstance(artifacts, list) and all(isinstance(item, str) for item in artifacts)
        else []
    )
    if _TB21_VERIFIER_ARTIFACT_GLOB not in source_artifacts:
        source_artifacts.append(_TB21_VERIFIER_ARTIFACT_GLOB)
    steps = [{"name": "main", "artifacts": source_artifacts}]

    normalized: dict[str, Any] = {
        "schema_version": "1",
        "task": task,
        "environment": environment,
        "agent": agent,
        "verifier": verifier,
    }
    if steps:
        normalized["steps"] = steps
    return normalized


__all__ = [
    "DEFAULT_AGENT_TIMEOUT_SEC",
    "DEFAULT_VERIFIER_SCRIPT_PATH",
    "DEFAULT_VERIFIER_TIMEOUT_SEC",
    "is_terminal_bench_shape",
    "normalize_terminal_bench_task_toml",
]
