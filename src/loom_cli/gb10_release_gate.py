"""GB10 release-target convergence checks shared by CLI gates."""

from __future__ import annotations

import re
from typing import Any

_RELEASE_TAG_SHA_RE = re.compile(r"(?:^|[-_])([0-9a-f]{7,40})(?:$|[-_])")

_IGNORED_INTENTS = {"stopped", "draining", "drained", "unavailable"}
_IGNORED_APPLY_STATES = {"stopped", "draining", "unavailable"}


def release_source_prefix(release_image_tag: str | None) -> str | None:
    if not release_image_tag:
        return None
    match = _RELEASE_TAG_SHA_RE.search(release_image_tag)
    return match.group(1) if match else None


def gb10_release_target_mismatches(
    data: dict[str, Any],
    *,
    release_image_tag: str | None,
    release_env_config_version: str | None,
) -> list[str]:
    if release_image_tag is None and release_env_config_version is None:
        return []

    desired_states = [
        row for row in data.get("desired_states", [])
        if isinstance(row, dict)
    ]
    nodes = [
        node for node in data.get("nodes", [])
        if isinstance(node, dict)
    ]
    nodes_by_key = {
        (
            str(node.get("environment") or ""),
            str(node.get("pool_name") or ""),
            str(node.get("hostname") or ""),
        ): node
        for node in nodes
    }

    mismatches: list[str] = []
    for row in desired_states:
        image = row.get("image_tag")
        env = row.get("env_config_version")
        image_bad = release_image_tag is not None and image != release_image_tag
        env_bad = (
            release_env_config_version is not None
            and env != release_env_config_version
        )
        if image_bad or env_bad:
            mismatches.append(
                "desired "
                f"{row.get('environment', '-')}/{row.get('pool_name', '-')} "
                f"image={image or '-'}/{release_image_tag or '-'} "
                f"env={env or '-'}/{release_env_config_version or '-'}"
            )

        host_intents = row.get("host_intents")
        if isinstance(host_intents, dict):
            for hostname, intent in sorted(host_intents.items()):
                if intent in _IGNORED_INTENTS:
                    continue
                key = (
                    str(row.get("environment") or ""),
                    str(row.get("pool_name") or ""),
                    str(hostname),
                )
                if key not in nodes_by_key:
                    mismatches.append(
                        "node "
                        f"{hostname} "
                        f"{row.get('environment', '-')}/{row.get('pool_name', '-')} "
                        "missing active node report "
                        f"image=-/{release_image_tag or '-'} "
                        f"env=-/{release_env_config_version or '-'} "
                        f"max=-/{row.get('max_concurrent') or '-'}"
                    )

    desired_by_pool = {
        (str(row.get("environment") or ""), str(row.get("pool_name") or "")): row
        for row in desired_states
    }
    for node in nodes:
        intent = node.get("desired_intent") or node.get("current_intent")
        apply_state = node.get("apply_state")
        if intent in _IGNORED_INTENTS:
            continue
        if intent is None and apply_state in _IGNORED_APPLY_STATES:
            continue
        image = node.get("current_image_tag")
        env = node.get("current_env_config_version")
        pool_key = (
            str(node.get("environment") or ""),
            str(node.get("pool_name") or ""),
        )
        desired = desired_by_pool.get(pool_key, {})
        desired_max = (
            node.get("desired_max_concurrent")
            if node.get("desired_max_concurrent") is not None
            else desired.get("max_concurrent")
        )
        current_max = node.get("current_max_concurrent")
        image_bad = release_image_tag is not None and image != release_image_tag
        env_bad = (
            release_env_config_version is not None
            and env != release_env_config_version
        )
        max_bad = desired_max is not None and current_max != desired_max
        apply_bad = apply_state != "applied"
        expected_source = desired.get("source_git_commit")
        if not isinstance(expected_source, str) or not expected_source.strip():
            expected_source = release_source_prefix(release_image_tag)
        if isinstance(expected_source, str):
            expected_source = expected_source.strip()
        source_commit = node.get("source_git_commit")
        source_dirty = node.get("source_git_dirty")
        source_bad = (
            expected_source is not None
            and (
                not isinstance(source_commit, str)
                or not source_commit.startswith(expected_source)
                or source_dirty is not False
            )
        )
        if image_bad or env_bad or max_bad or apply_bad or source_bad:
            mismatches.append(
                "node "
                f"{node.get('hostname', '-')} "
                f"image={image or '-'}/{release_image_tag or '-'} "
                f"env={env or '-'}/{release_env_config_version or '-'} "
                f"max={current_max if current_max is not None else '-'}/"
                f"{desired_max if desired_max is not None else '-'} "
                f"source={source_commit or '-'}/"
                f"expected_source={expected_source or '-'} "
                f"dirty={source_dirty if source_dirty is not None else '-'} "
                f"compose_project_dir={node.get('compose_project_dir') or '-'} "
                f"apply_state={apply_state or '-'}"
            )

    return mismatches
