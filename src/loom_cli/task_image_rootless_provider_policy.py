"""Strict loader for the inert rootless task-image builder provider policy."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from loom_control_plane.task_image_build_environment import (
    SlurmBuildEnvironmentPolicyV1,
)

_SCHEMA = "loom.task-image-rootless-provider-policies/v1"
_TOP_LEVEL_KEYS = frozenset({"schema", "policies"})


class TaskImageRootlessProviderPolicyError(ValueError):
    """The inert rootless provider policy is malformed or activation-capable."""


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise TaskImageRootlessProviderPolicyError(f"could not read {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise TaskImageRootlessProviderPolicyError(f"invalid TOML in {path}: {exc}") from exc


def load_task_image_rootless_provider_policy(
    path: Path,
) -> tuple[SlurmBuildEnvironmentPolicyV1, ...]:
    """Load exactly two disabled, native provider rows from ``path``."""
    raw = _load_toml(path)
    unknown_top_level = sorted(set(raw) - _TOP_LEVEL_KEYS)
    if unknown_top_level:
        raise TaskImageRootlessProviderPolicyError(
            f"{path}: unknown top-level key(s): {', '.join(unknown_top_level)}"
        )
    if raw.get("schema") != _SCHEMA:
        raise TaskImageRootlessProviderPolicyError(f"{path}: schema must be {_SCHEMA!r}")
    policy_tables = raw.get("policies")
    if not isinstance(policy_tables, list):
        raise TaskImageRootlessProviderPolicyError(f"{path}: policies must be an array")

    policies: list[SlurmBuildEnvironmentPolicyV1] = []
    for index, value in enumerate(policy_tables):
        if not isinstance(value, dict):
            raise TaskImageRootlessProviderPolicyError(
                f"{path}: policies[{index}] must be a table"
            )
        payload = dict(value)
        blockers = payload.get("activation_blockers")
        if isinstance(blockers, list):
            payload["activation_blockers"] = tuple(blockers)
        try:
            policy = SlurmBuildEnvironmentPolicyV1.model_validate(payload)
        except ValidationError as exc:
            raise TaskImageRootlessProviderPolicyError(
                f"{path}: invalid policies[{index}]: {exc}"
            ) from exc
        if policy.enabled:
            raise TaskImageRootlessProviderPolicyError(
                f"{path}: policies[{index}] must remain disabled in this increment"
            )
        if not policy.activation_blockers:
            raise TaskImageRootlessProviderPolicyError(
                f"{path}: policies[{index}] must retain activation blockers"
            )
        policies.append(policy)

    native_pairs = {(policy.slurm_cluster_id, policy.cpu_arch) for policy in policies}
    if len(policies) != 2 or native_pairs != {("oldlab", "x86_64"), ("gb10", "arm64")}:
        raise TaskImageRootlessProviderPolicyError(
            f"{path}: policies must contain exactly oldlab/x86_64 and gb10/arm64"
        )
    return tuple(policies)


__all__ = [
    "TaskImageRootlessProviderPolicyError",
    "load_task_image_rootless_provider_policy",
]
