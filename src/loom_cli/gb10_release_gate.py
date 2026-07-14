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

    mismatches: list[str] = []
    desired_states = [row for row in data.get("desired_states", []) if isinstance(row, dict)]
    raw_nodes_value = data.get("nodes", [])
    raw_nodes = raw_nodes_value if isinstance(raw_nodes_value, list) else []
    nodes = [node for node in raw_nodes if isinstance(node, dict)]
    if not isinstance(raw_nodes_value, list):
        mismatches.append("nodes must be a list")
    elif len(nodes) != len(raw_nodes):
        mismatches.append("nodes contains a non-object entry")
    raw_unlinked_workers = data.get("unlinked_workers")
    if not isinstance(raw_unlinked_workers, list):
        mismatches.append("unlinked_workers must be a list")
        raw_unlinked_workers = []
    seen_unlinked_worker_ids: set[str] = set()
    for index, worker in enumerate(raw_unlinked_workers):
        if not isinstance(worker, dict):
            mismatches.append(f"unlinked_workers[{index}] must be an object")
            continue
        worker_id_value = worker.get("worker_id")
        hostname_value = worker.get("hostname")
        pool_name_value = worker.get("pool_name")
        unlinked_worker_id = worker_id_value if isinstance(worker_id_value, str) else ""
        if not unlinked_worker_id:
            mismatches.append(f"unlinked_workers[{index}].worker_id must be a non-empty string")
        elif unlinked_worker_id in seen_unlinked_worker_ids:
            mismatches.append(f"unlinked_workers contains duplicate worker_id {unlinked_worker_id}")
        else:
            seen_unlinked_worker_ids.add(unlinked_worker_id)
        if not isinstance(hostname_value, str) or not hostname_value:
            mismatches.append(f"unlinked_workers[{index}].hostname must be a non-empty string")
        if not isinstance(pool_name_value, str) or not pool_name_value:
            mismatches.append(f"unlinked_workers[{index}].pool_name must be a non-empty string")
        worker_fresh = worker.get("worker_fresh")
        if not isinstance(worker_fresh, bool):
            mismatches.append(f"unlinked_workers[{index}].worker_fresh must be a boolean")
        elif worker_fresh:
            mismatches.append(
                "unlinked fresh worker "
                f"{unlinked_worker_id or '-'} host={worker.get('hostname') or '-'} "
                f"pool={worker.get('pool_name') or '-'} "
                f"status={worker.get('worker_status') or '-'} "
                f"drain_state={worker.get('worker_drain_state') or '-'}"
            )
    nodes_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    node_key_by_worker_id: dict[str, tuple[str, str, str]] = {}
    for index, node in enumerate(nodes):
        key = (
            str(node.get("environment") or ""),
            str(node.get("pool_name") or ""),
            str(node.get("hostname") or ""),
        )
        if not all(key):
            mismatches.append(f"nodes[{index}] has an incomplete environment/pool/hostname key")
            continue
        if key in nodes_by_key:
            mismatches.append(f"nodes contains duplicate {'/'.join(key)}")
            continue
        nodes_by_key[key] = node
        node_worker_id = node.get("worker_id")
        if isinstance(node_worker_id, str) and node_worker_id:
            previous_key = node_key_by_worker_id.get(node_worker_id)
            if previous_key is not None:
                mismatches.append(
                    f"nodes reuse worker_id {node_worker_id} for "
                    f"{'/'.join(previous_key)} and {'/'.join(key)}"
                )
            else:
                node_key_by_worker_id[node_worker_id] = key
            if node_worker_id in seen_unlinked_worker_ids:
                mismatches.append(
                    f"worker_id {node_worker_id} appears in nodes and unlinked_workers"
                )

    for row in desired_states:
        image = row.get("image_tag")
        env = row.get("env_config_version")
        image_bad = release_image_tag is not None and image != release_image_tag
        env_bad = release_env_config_version is not None and env != release_env_config_version
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
        pool_key = (
            str(node.get("environment") or ""),
            str(node.get("pool_name") or ""),
        )
        desired = desired_by_pool.get(pool_key, {})
        hostname = str(node.get("hostname") or "")
        host_intents = desired.get("host_intents")
        apply_state = node.get("apply_state")
        worker_fresh = node.get("worker_fresh")
        worker_status = node.get("worker_status")
        worker_backend_names = node.get("worker_backend_names")
        if not isinstance(worker_backend_names, list):
            worker_backend_names = []
        worker_backend_set = {
            item for item in worker_backend_names if isinstance(item, str) and item
        }
        self_reported_intent = node.get("desired_intent") or node.get("current_intent")
        host_is_declared = isinstance(host_intents, dict) and hostname in host_intents
        if not host_is_declared:
            if (
                worker_fresh is True
                or worker_status == "active"
                or self_reported_intent == "active"
                or apply_state == "applied"
            ):
                mismatches.append(
                    "node "
                    f"{node.get('hostname', '-')} is not declared by release host_intents "
                    f"desired_intent={node.get('desired_intent') or '-'} "
                    f"current_intent={node.get('current_intent') or '-'} "
                    f"worker_status={worker_status or '-'} "
                    f"worker_fresh={worker_fresh if worker_fresh is not None else '-'} "
                    f"apply_state={apply_state or '-'}"
                )
            continue
        assert isinstance(host_intents, dict)
        intent = host_intents.get(hostname)
        if intent in _IGNORED_INTENTS:
            if worker_fresh is True:
                mismatches.append(
                    "node "
                    f"{node.get('hostname', '-')} "
                    f"desired_intent={intent} still has a fresh worker registration "
                    f"worker_status={worker_status or '-'} "
                    f"worker_backends={','.join(sorted(worker_backend_set)) or '-'} "
                    f"apply_state={apply_state or '-'}"
                )
            continue
        if intent is None and apply_state in _IGNORED_APPLY_STATES:
            continue
        image = node.get("current_image_tag")
        env = node.get("current_env_config_version")
        desired_max = (
            node.get("desired_max_concurrent")
            if node.get("desired_max_concurrent") is not None
            else desired.get("max_concurrent")
        )
        current_max = node.get("current_max_concurrent")
        image_bad = release_image_tag is not None and image != release_image_tag
        env_bad = release_env_config_version is not None and env != release_env_config_version
        max_bad = desired_max is not None and current_max != desired_max
        apply_bad = apply_state != "applied"
        worker_id = node.get("worker_id")
        worker_bad = (
            not isinstance(worker_id, str)
            or not worker_id.strip()
            or worker_status != "active"
            or worker_fresh is not True
            or "docker" not in worker_backend_set
        )
        expected_source = desired.get("source_git_commit")
        exact_source = isinstance(expected_source, str) and bool(expected_source.strip())
        if not isinstance(expected_source, str) or not expected_source.strip():
            expected_source = release_source_prefix(release_image_tag)
        if isinstance(expected_source, str):
            expected_source = expected_source.strip()
        source_commit = node.get("source_git_commit")
        source_dirty = node.get("source_git_dirty")
        source_bad = expected_source is not None and (
            not isinstance(source_commit, str)
            or (
                source_commit != expected_source
                if exact_source
                else not source_commit.startswith(expected_source)
            )
            or source_dirty is not False
        )
        if worker_bad:
            mismatches.append(
                "node "
                f"{node.get('hostname', '-')} "
                "missing active/fresh docker worker registration "
                f"image={image or '-'}/{release_image_tag or '-'} "
                f"env={env or '-'}/{release_env_config_version or '-'} "
                f"max={current_max if current_max is not None else '-'}/"
                f"{desired_max if desired_max is not None else '-'} "
                f"worker_id={worker_id or '-'} "
                f"worker_status={worker_status or '-'} "
                f"worker_fresh={worker_fresh if worker_fresh is not None else '-'} "
                f"worker_backends={','.join(sorted(worker_backend_set)) or '-'} "
                f"compose_project_dir={node.get('compose_project_dir') or '-'} "
                f"apply_state={apply_state or '-'}"
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
