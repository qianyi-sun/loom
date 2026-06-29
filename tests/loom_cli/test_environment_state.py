from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from loom_cli.environment_state import (
    EnvironmentStateProfileError,
    diff_environment_state,
    load_environment_state_profile,
)


def _write_profile(path: Path) -> None:
    path.write_text(
        """
environment = "public-beta"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10-arm64"
actuator = "slurm"
enabled = true
min_slots = 0
max_slots = 150
scale_up_threshold_slots = 1
scale_down_idle_seconds = 600
scale_up_cooldown_seconds = 60
scale_down_cooldown_seconds = 300
drain_timeout_seconds = 600
force = false

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "arm64"
partition = "gb10"
allowed_nodes = ["trt-gb10-1", "trt-gb10-2"]
requested_concurrency = 10
max_jobs = 15
pending_job_cap = 2

[[gb10_worker_pool_desired_states]]
pool_name = "gb10-arm64"
image_tag = "${IMAGE_TAG}"
max_concurrent = 10
env_config_version = "${ENV_CONFIG_VERSION}"
target_slots = 150

[gb10_worker_pool_desired_states.host_intents]
trt-gb10-1 = "active"
trt-gb10-2 = "active"

[gb10_worker_pool_desired_states.rollout_policy]
mode = "all"

[catalog_provisioning]
required = true
command = "loom datasets provision-public-beta-catalog"
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_load_environment_state_profile_normalizes_payloads_and_variables(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)

    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "public-beta-57a7509",
            "ENV_CONFIG_VERSION": "public-beta-57a7509",
        },
        expected_environment="public-beta",
    )

    assert profile.environment == "public-beta"
    assert profile.control_plane_environment == "public-beta"
    assert profile.autoscaler_policies == [
        {
            "environment": "public-beta",
            "pool_name": "gb10-arm64",
            "actuator": "slurm",
            "enabled": True,
            "min_slots": 0,
            "max_slots": 150,
            "scale_up_threshold_slots": 1,
            "scale_down_idle_seconds": 600,
            "scale_up_cooldown_seconds": 60,
            "scale_down_cooldown_seconds": 300,
            "drain_timeout_seconds": 600,
            "force": False,
            "actuator_config": {
                "backend": "docker",
                "cpu_arch": "arm64",
                "partition": "gb10",
                "allowed_nodes": ["trt-gb10-1", "trt-gb10-2"],
                "requested_concurrency": 10,
                "max_jobs": 15,
                "pending_job_cap": 2,
            },
        },
    ]
    assert profile.gb10_desired_states[0]["image_tag"] == "public-beta-57a7509"
    assert profile.gb10_desired_states[0]["env_config_version"] == "public-beta-57a7509"
    assert profile.catalog_provisioning["required"] is True


def test_load_environment_state_profile_requires_placeholder_values(tmp_path: Path) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)

    with pytest.raises(EnvironmentStateProfileError, match="IMAGE_TAG"):
        load_environment_state_profile(profile_path, variables={})


def test_diff_environment_state_reports_policy_and_desired_state_drift(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "public-beta-57a7509",
            "ENV_CONFIG_VERSION": "public-beta-57a7509",
        },
    )

    live: dict[str, Any] = {
        "autoscaler_status": {
            "policies": [
                {
                    "environment": "public-beta",
                    "pool_name": "gb10-arm64",
                    "actuator": "gb10",
                    "enabled": True,
                    "min_slots": 0,
                    "max_slots": 150,
                    "scale_up_threshold_slots": 1,
                    "scale_down_idle_seconds": 600,
                    "scale_up_cooldown_seconds": 60,
                    "scale_down_cooldown_seconds": 300,
                    "drain_timeout_seconds": 600,
                    "force": False,
                    "actuator_config": {"backend": "docker", "cpu_arch": "arm64"},
                },
            ],
        },
        "gb10_status": {
            "desired_states": [
                {
                    "environment": "public-beta",
                    "pool_name": "gb10-arm64",
                    "image_tag": "public-beta-old",
                    "max_concurrent": 10,
                    "env_config_version": "public-beta-old",
                    "target_slots": 150,
                    "host_intents": {
                        "trt-gb10-1": "active",
                        "trt-gb10-2": "active",
                    },
                    "rollout_policy": {"mode": "all"},
                    "env": {},
                },
            ],
        },
    }

    drift = diff_environment_state(profile, live)

    assert [item.path for item in drift] == [
        "worker_pool_autoscaler_policies[public-beta/gb10-arm64].actuator",
        "worker_pool_autoscaler_policies[public-beta/gb10-arm64].actuator_config",
        "gb10_worker_pool_desired_states[public-beta/gb10-arm64].image_tag",
        "gb10_worker_pool_desired_states[public-beta/gb10-arm64].env_config_version",
    ]
    assert drift[0].desired == "slurm"
    assert drift[0].live == "gb10"


def test_diff_environment_state_reports_missing_live_policy(tmp_path: Path) -> None:
    profile_path = tmp_path / "public-beta.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "public-beta-57a7509",
            "ENV_CONFIG_VERSION": "public-beta-57a7509",
        },
    )

    drift = diff_environment_state(
        profile,
        {"autoscaler_status": {"policies": []}, "gb10_status": {"desired_states": []}},
    )

    assert drift[0].path == "worker_pool_autoscaler_policies[public-beta/gb10-arm64]"
    assert drift[0].live is None
    assert drift[1].path == "gb10_worker_pool_desired_states[public-beta/gb10-arm64]"
    assert drift[1].live is None


@pytest.mark.parametrize(
    ("path", "environment"),
    [
        (Path("deploy/environment-state/public-beta.toml"), "public-beta"),
        (Path("deploy/environment-state/staging.toml"), "staging"),
    ],
)
def test_committed_environment_state_profiles_cover_gb10_slurm_policy(
    path: Path,
    environment: str,
) -> None:
    profile = load_environment_state_profile(
        path,
        variables={
            "IMAGE_TAG": "public-beta-test",
            "ENV_CONFIG_VERSION": "public-beta-test",
        },
        expected_environment=environment,
    )

    gb10_policy = next(
        policy
        for policy in profile.autoscaler_policies
        if policy["pool_name"] == "gb10-arm64"
    )
    expected_cp_environment = "production" if environment == "public-beta" else environment
    assert profile.environment == environment
    assert gb10_policy["environment"] == expected_cp_environment
    assert gb10_policy["actuator"] == "slurm"
    assert gb10_policy["max_slots"] == 150
    assert gb10_policy["actuator_config"]["backend"] == "docker"
    assert gb10_policy["actuator_config"]["cpu_arch"] == "arm64"
    assert gb10_policy["actuator_config"]["partition"] == "gb10"
    assert len(gb10_policy["actuator_config"]["allowed_nodes"]) == 15

    gb10_state = next(
        state
        for state in profile.gb10_desired_states
        if state["pool_name"] == "gb10-arm64"
    )
    assert gb10_state["environment"] == expected_cp_environment
    assert gb10_state["image_tag"] == "public-beta-test"
    assert gb10_state["env_config_version"] == "public-beta-test"
    assert gb10_state["max_concurrent"] == 10
    assert gb10_state["target_slots"] == 150
    assert profile.catalog_provisioning["required"] is True
