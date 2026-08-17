from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from scripts.ops.task_image_builder_autoscaler_external_once import _parser as builder_parser

from loom_cli.cluster_release_manifest import _external_worker_summary
from loom_cli.environment_state import (
    EnvironmentStateProfileError,
    _normalize_task_image_builder_policy,
    load_environment_state_profile,
)

_VARIABLES = {
    "IMAGE_TAG": "staging-abc1234",
    "ENV_CONFIG_VERSION": "staging-abc1234",
    "GIT_SHA": "abc1234def5678901234567890123456789012ab",
}


def test_staging_declares_separate_inert_exclusive_builder_pools() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables=_VARIABLES,
        expected_environment="staging",
    )

    builders = {row["cpu_arch"]: row for row in profile.task_image_builder_policies}
    assert set(builders) == {"x86_64", "arm64"}
    assert {row["pool_name"] for row in builders.values()} == {
        "task-image-builder-oldlab",
        "task-image-builder-gb10",
    }
    for row in builders.values():
        assert row["enabled"] is False
        assert row["exclusive"] is True
        assert row["requested_concurrency"] == 1
        assert row["max_jobs"] > 0
        assert "registry_retention_not_provisioned" in row["activation_blockers"]
        cluster = row["slurm_cluster_id"]
        assert f"exclusive_{cluster}_nodes_not_provisioned" in row["activation_blockers"]

    trial_pools = {
        row["pool_name"]: row
        for row in profile.autoscaler_policies
        if row["pool_name"] in {"gb10", "oldlab"}
    }
    assert set(trial_pools) == {"gb10", "oldlab"}
    for row in trial_pools.values():
        assert row["enabled"] is True
        assert row["actuator_config"]["exclusive"] is False
        assert row["actuator_config"]["requested_concurrency"] > 1
    trial_env_files = {row["actuator_config"]["env_file"] for row in trial_pools.values()}
    assert all(row["env_file"] not in trial_env_files for row in builders.values())


def test_staging_builder_supervisors_are_present_but_disabled() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables=_VARIABLES,
        expected_environment="staging",
    )
    builder_pool_names = {row["pool_name"] for row in profile.task_image_builder_policies}
    supervisors = {
        row["pool_name"]: row
        for row in profile.external_slurm_autoscaler_supervisors
        if row["pool_name"] in builder_pool_names
    }

    assert set(supervisors) == builder_pool_names
    for row in supervisors.values():
        assert row["enabled"] is False
        assert row["active"] is False
        assert row["script_path"].endswith(
            "/scripts/ops/task_image_builder_autoscaler_external_once.py"
        )
        args = builder_parser().parse_args(row["args"])
        assert args.global_execution_witness_json.name.endswith("-witness.json")
        assert args.manager_public_key.name == "manager-ed25519.pub"
        assert args.expected_manager_public_key_sha256_file.name == (
            "manager-ed25519.pub.sha256"
        )


def test_enabled_builder_policy_requires_complete_slurm_authority() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables=_VARIABLES,
        expected_environment="staging",
    )
    base = {
        **profile.task_image_builder_policies[0],
        "enabled": True,
        "activation_blockers": [],
        "slurm_account": "",
    }

    with pytest.raises(EnvironmentStateProfileError, match="slurm_account"):
        _normalize_task_image_builder_policy(base, environment="staging", index=0)


def test_builder_policy_bounds_jobs_to_declared_nodes() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables=_VARIABLES,
        expected_environment="staging",
    )
    base = {
        **profile.task_image_builder_policies[0],
        "max_jobs": len(profile.task_image_builder_policies[0]["allowed_nodes"]) + 1,
    }

    with pytest.raises(EnvironmentStateProfileError, match="max_jobs"):
        _normalize_task_image_builder_policy(base, environment="staging", index=0)


def test_remote_worker_compose_forwards_builder_lifecycle_settings() -> None:
    compose = yaml.safe_load(
        Path("deploy/docker-compose.remote-worker.yml").read_text(encoding="utf-8")
    )
    raw_environment = compose["services"]["worker"]["environment"]
    environment = {
        item.partition("=")[0]: item.partition("=")[2]
        for item in raw_environment
        if isinstance(item, str)
    }

    assert environment["LOOM_WORKER_TASK_IMAGE_BUILDER_IDLE_EXIT_SECONDS"] == (
        "${LOOM_WORKER_TASK_IMAGE_BUILDER_IDLE_EXIT_SECONDS:-120}"
    )
    assert environment["LOOM_WORKER_TASK_IMAGE_LOCAL_TTL_HOURS"] == (
        "${LOOM_WORKER_TASK_IMAGE_LOCAL_TTL_HOURS:-168}"
    )
    assert environment["LOOM_WORKER_TASK_IMAGE_MIN_FREE_GB"] == (
        "${LOOM_WORKER_TASK_IMAGE_MIN_FREE_GB:-20}"
    )


def test_release_manifest_preserves_inert_builder_policy_evidence() -> None:
    summary = _external_worker_summary(
        environment_state_path=Path("deploy/environment-state/staging.toml"),
        image_tag=_VARIABLES["IMAGE_TAG"],
        env_config_version=_VARIABLES["ENV_CONFIG_VERSION"],
        git_sha=_VARIABLES["GIT_SHA"],
    )

    builders = summary["task_image_builder_policies"]
    assert {row["pool_name"] for row in builders} == {
        "task-image-builder-gb10",
        "task-image-builder-oldlab",
    }
    assert all(row["enabled"] is False for row in builders)
    assert all(row["exclusive"] is True for row in builders)
