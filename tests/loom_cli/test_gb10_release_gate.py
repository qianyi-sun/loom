from __future__ import annotations

from typing import Any

from loom_cli.gb10_release_gate import gb10_release_target_mismatches


def _active_node(hostname: str) -> dict[str, Any]:
    return {
        "environment": "staging",
        "pool_name": "gb10-arm64",
        "hostname": hostname,
        "apply_state": "applied",
        "current_image_tag": "staging-abc1234",
        "current_env_config_version": "staging-abc1234",
        "current_max_concurrent": 10,
        "desired_max_concurrent": 10,
        "desired_intent": "active",
        "current_intent": "active",
        "source_git_commit": "abc1234fffffffffffffffffffffffffffffffff",
        "source_git_dirty": False,
        "worker_id": f"worker-{hostname}",
        "worker_status": "active",
        "worker_fresh": True,
        "worker_backend_names": ["docker"],
    }


def _status_with_excluded_node(node7: dict[str, Any] | None) -> dict[str, Any]:
    nodes = [_active_node("trt-gb10-1")]
    if node7 is not None:
        nodes.append(node7)
    return {
        "desired_states": [
            {
                "environment": "staging",
                "pool_name": "gb10-arm64",
                "image_tag": "staging-abc1234",
                "max_concurrent": 10,
                "env_config_version": "staging-abc1234",
                "source_git_commit": "abc1234fffffffffffffffffffffffffffffffff",
                "host_intents": {
                    "trt-gb10-1": "active",
                    "trt-gb10-7": "stopped",
                },
            }
        ],
        "nodes": nodes,
        "unlinked_workers": [],
    }


def test_merged_stopped_intent_overrides_stale_active_node_snapshot() -> None:
    stale_node7 = _active_node("trt-gb10-7")
    stale_node7.update(
        {
            "current_image_tag": "staging-old",
            "current_env_config_version": "staging-old",
            "source_git_commit": "oldold0fffffffffffffffffffffffffffffffff",
            "worker_status": "offline",
            "worker_fresh": False,
        }
    )

    mismatches = gb10_release_target_mismatches(
        _status_with_excluded_node(stale_node7),
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert mismatches == []


def test_merged_stopped_intent_rejects_fresh_worker_on_excluded_node() -> None:
    unsafe_node7 = _active_node("trt-gb10-7")

    mismatches = gb10_release_target_mismatches(
        _status_with_excluded_node(unsafe_node7),
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert len(mismatches) == 1
    assert "trt-gb10-7" in mismatches[0]
    assert "desired_intent=stopped" in mismatches[0]
    assert "fresh worker registration" in mismatches[0]


def test_active_desired_intent_overrides_stale_stopped_node_snapshot() -> None:
    active_node = _active_node("trt-gb10-1")
    active_node["desired_intent"] = "stopped"

    mismatches = gb10_release_target_mismatches(
        _status_with_excluded_node(None) | {"nodes": [active_node]},
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert mismatches == []


def test_release_gate_rejects_fresh_active_node_missing_from_host_intents() -> None:
    rogue = _active_node("trt-gb10-16")
    status = _status_with_excluded_node(None)
    status["nodes"].append(rogue)

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert len(mismatches) == 1
    assert "trt-gb10-16" in mismatches[0]
    assert "not declared by release host_intents" in mismatches[0]


def test_release_gate_rejects_duplicate_node_evidence() -> None:
    status = _status_with_excluded_node(None)
    status["nodes"].append(_active_node("trt-gb10-1"))

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert mismatches == ["nodes contains duplicate staging/gb10-arm64/trt-gb10-1"]


def test_release_gate_rejects_node_source_with_suffix_after_full_desired_sha() -> None:
    status = _status_with_excluded_node(None)
    status["nodes"][0]["source_git_commit"] += "junk"

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert len(mismatches) == 1
    assert "expected_source=abc1234" in mismatches[0]


def test_release_gate_rejects_fresh_worker_without_node_status() -> None:
    status = _status_with_excluded_node(None)
    status["unlinked_workers"] = [
        {
            "worker_id": "worker-node7-direct",
            "hostname": "trt-gb10-7",
            "pool_name": "gb10-arm64",
            "worker_status": "active",
            "worker_fresh": True,
            "worker_drain_state": "active",
        }
    ]

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert len(mismatches) == 1
    assert "unlinked fresh worker worker-node7-direct" in mismatches[0]
    assert "host=trt-gb10-7" in mismatches[0]


def test_release_gate_rejects_second_fresh_registration_for_active_host() -> None:
    status = _status_with_excluded_node(None)
    status["unlinked_workers"] = [
        {
            "worker_id": "worker-node1-duplicate",
            "hostname": "trt-gb10-1",
            "pool_name": "gb10-arm64",
            "worker_status": "active",
            "worker_fresh": True,
            "worker_drain_state": "active",
        }
    ]

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert len(mismatches) == 1
    assert "unlinked fresh worker worker-node1-duplicate" in mismatches[0]


def test_release_gate_requires_unlinked_worker_inventory() -> None:
    status = _status_with_excluded_node(None)
    status.pop("unlinked_workers")

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert mismatches == ["unlinked_workers must be a list"]


def test_release_gate_rejects_unlinked_worker_without_boolean_freshness() -> None:
    status = _status_with_excluded_node(None)
    status["unlinked_workers"] = [
        {
            "worker_id": "worker-node7-direct",
            "hostname": "trt-gb10-7",
            "pool_name": "gb10-arm64",
            "worker_fresh": "true",
        }
    ]

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert mismatches == ["unlinked_workers[0].worker_fresh must be a boolean"]


def test_release_gate_rejects_worker_id_reused_across_active_nodes() -> None:
    status = _status_with_excluded_node(None)
    status["desired_states"][0]["host_intents"]["trt-gb10-2"] = "active"
    second = _active_node("trt-gb10-2")
    second["worker_id"] = status["nodes"][0]["worker_id"]
    status["nodes"].append(second)

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert len(mismatches) == 1
    assert "nodes reuse worker_id worker-trt-gb10-1" in mismatches[0]


def test_release_gate_rejects_worker_id_in_node_and_unlinked_inventory() -> None:
    status = _status_with_excluded_node(None)
    status["unlinked_workers"] = [
        {
            "worker_id": status["nodes"][0]["worker_id"],
            "hostname": "trt-gb10-7",
            "pool_name": "gb10-arm64",
            "worker_fresh": False,
        }
    ]

    mismatches = gb10_release_target_mismatches(
        status,
        release_image_tag="staging-abc1234",
        release_env_config_version="staging-abc1234",
    )

    assert mismatches == ["worker_id worker-trt-gb10-1 appears in nodes and unlinked_workers"]
