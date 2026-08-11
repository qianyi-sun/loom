from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from loom_cli.environment_state import (
    EnvironmentStateProfileError,
    _normalize_autoscaler_policy,
    apply_external_slurm_autoscaler_supervisors,
    diff_environment_state,
    diff_external_slurm_autoscaler_supervisors,
    diff_external_slurm_runner_prerequisites,
    load_environment_state_profile,
    render_external_slurm_autoscaler_service,
    render_external_slurm_autoscaler_timer,
    staging_gb10_external_activation_blockers,
)


def test_normalize_autoscaler_policy_passes_through_qos_and_slurm_scheduler_fields() -> None:
    normalized = _normalize_autoscaler_policy(
        {
            "pool_name": "gb10",
            "actuator": "slurm",
            "max_slots": 10,
            "actuator_config": {
                "backend": "docker",
                "qos_boost": "loom-boost",
                "qos_normal": "loom-staging-normal",
                "slurm_account": "loom-staging",
                "slurm_qos": "loom-staging-normal",
                "slurm_reservation": "loom-staging-min",
            },
        },
        environment="staging",
        index=0,
    )

    assert normalized["actuator_config"] == {
        "backend": "docker",
        "qos_boost": "loom-boost",
        "qos_normal": "loom-staging-normal",
        "slurm_account": "loom-staging",
        "slurm_qos": "loom-staging-normal",
        "slurm_reservation": "loom-staging-min",
    }


def _write_profile(path: Path, *, host1_intent: str = "active") -> None:
    payload = """
environment = "staging"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10"
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
pool_name = "gb10"
image_tag = "${IMAGE_TAG}"
max_concurrent = 10
env_config_version = "${ENV_CONFIG_VERSION}"
source_git_commit = "${GIT_SHA}"
target_slots = 150

[gb10_worker_pool_desired_states.host_intents]
trt-gb10-1 = "active"
trt-gb10-2 = "active"

[gb10_worker_pool_desired_states.rollout_policy]
mode = "all"

[catalog_provisioning]
required = true
command = "loom datasets provision-catalog"

[rate_card_sync.yibuapi]
enabled = true
group = "default"

[[hosted_provider_pricing_defaults]]
name = "mz_tn_canada_qianyi"
pricing_source = "rate-card"
rate_card_provider = "yibuapi"
""".strip()
    payload = payload.replace(
        'trt-gb10-1 = "active"',
        f'trt-gb10-1 = "{host1_intent}"',
    )
    path.write_text(
        payload + "\n",
        encoding="utf-8",
    )


def test_load_environment_state_profile_normalizes_payloads_and_variables(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)

    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-57a7509",
            "ENV_CONFIG_VERSION": "staging-57a7509",
            "GIT_SHA": "57a750912345678901234567890123456789abcd",
        },
        expected_environment="staging",
    )

    assert profile.environment == "staging"
    assert profile.control_plane_environment == "staging"
    assert profile.autoscaler_policies == [
        {
            "environment": "staging",
            "pool_name": "gb10",
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
    assert profile.gb10_desired_states[0]["image_tag"] == "staging-57a7509"
    assert profile.gb10_desired_states[0]["env_config_version"] == "staging-57a7509"
    assert profile.gb10_desired_states[0]["source_git_commit"] == (
        "57a750912345678901234567890123456789abcd"
    )
    assert profile.catalog_provisioning["required"] is True
    assert profile.rate_card_sync == {
        "yibuapi": {
            "enabled": True,
            "group": "default",
        },
    }
    assert profile.hosted_provider_pricing_defaults == [
        {
            "name": "mz_tn_canada_qianyi",
            "pricing_source": "rate-card",
            "rate_card_provider": "yibuapi",
            "required": True,
        },
    ]


def test_staging_gb10_desired_state_sets_two_hour_worker_idle_ttl() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables={
            "IMAGE_TAG": "staging-fe9b82c",
            "ENV_CONFIG_VERSION": "staging-fe9b82c",
            "GIT_SHA": "fe9b82c81e10347b2aa5d8afb2492984945b641a",
        },
        expected_environment="staging",
    )

    gb10 = next(row for row in profile.gb10_desired_states if row["pool_name"] == "gb10")

    assert gb10["env"]["LOOM_WORKER_IDLE_EXIT_AFTER_SECONDS"] == "7200"


def test_committed_production_profile_ships_fail_closed() -> None:
    # #484 prod bootstrap: the committed prod env-state loads and ships
    # DISABLED — applying it must not activate any prod worker.
    profile = load_environment_state_profile(
        Path("deploy/environment-state/production.toml"),
        variables={
            "IMAGE_TAG": "prod-abc1234",
            "ENV_CONFIG_VERSION": "prod-abc1234",
            "GIT_SHA": "abc1234def5678901234567890123456789012ab",
        },
        expected_environment="production",
    )

    gb10 = next(row for row in profile.autoscaler_policies if row["pool_name"] == "gb10")
    assert gb10["enabled"] is False
    assert gb10["actuator_config"]["exclusive"] is False

    desired = next(row for row in profile.gb10_desired_states if row["pool_name"] == "gb10")
    assert desired["target_slots"] == 0
    assert all(intent == "stopped" for intent in desired["host_intents"].values())

    supervisor = profile.external_slurm_autoscaler_supervisors[0]
    assert supervisor["name"] == "gb10-production"
    assert supervisor["enabled"] is False
    assert supervisor["active"] is False
    assert "15452" in " ".join(supervisor["args"])  # reserved prod gb10 port


def test_committed_slurm_pools_carry_containment_contract() -> None:
    """#896/#1143: every committed non-exclusive Slurm pool must ship the full
    containment contract (per-container caps + a job_pids_max ceiling that fits
    lease). Both staging pools are enabled; development and production remain
    fail-closed. GB10 also carries its bounded service-account QoS."""
    variables = {
        "IMAGE_TAG": "x-abc1234",
        "ENV_CONFIG_VERSION": "x-abc1234",
        "GIT_SHA": "abc1234def5678901234567890123456789012ab",
    }
    for env in ("development", "staging", "production"):
        profile = load_environment_state_profile(
            Path(f"deploy/environment-state/{env}.toml"),
            variables=variables,
            expected_environment=env,
        )
        slurm_pools = [p for p in profile.autoscaler_policies if p.get("actuator") == "slurm"]
        assert slurm_pools, f"{env} has no slurm pool"
        for policy in slurm_pools:
            cfg = policy["actuator_config"]
            expected_enabled = env == "staging"
            assert policy["enabled"] is expected_enabled
            assert cfg["exclusive"] is False
            cpus = cfg["container_cpus"]
            mem = cfg["container_memory_mib"]
            pids = cfg["container_pids"]
            conc = cfg["requested_concurrency"]
            assert cpus > 0 and mem > 0 and pids > 0
            # Job cgroup PID ceiling must cover every concurrent trial container.
            assert cfg["job_pids_max"] >= pids * conc
            # Per-trial caps must fit within the reserved Slurm lease so trials
            # cannot exceed the allocation.
            assert conc * cpus <= cfg["requested_cpus"]
            assert conc * mem <= cfg["requested_memory_mib"]
            if policy["pool_name"] == "gb10":
                # The physical GB10 partition is capped at MaxTime=1-00:00:00;
                # a larger request is rejected by sbatch before any worker can
                # register, so every environment must stay within that bound.
                assert cfg["time_limit"] == "1-00:00:00"
            if policy["pool_name"] == "oldlab":
                assert cfg["time_limit"] == "2-00:00:00"
            if env == "staging" and policy["pool_name"] == "gb10":
                assert cfg["qos_normal"] == "loom-staging"
            else:
                assert not cfg.get("qos_normal")


def test_load_environment_state_profile_requires_placeholder_values(tmp_path: Path) -> None:
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)

    with pytest.raises(EnvironmentStateProfileError, match="IMAGE_TAG"):
        load_environment_state_profile(profile_path, variables={})


def test_diff_environment_state_reports_policy_and_desired_state_drift(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-57a7509",
            "ENV_CONFIG_VERSION": "staging-57a7509",
            "GIT_SHA": "57a750912345678901234567890123456789abcd",
        },
    )

    live: dict[str, Any] = {
        "autoscaler_status": {
            "policies": [
                {
                    "environment": "staging",
                    "pool_name": "gb10",
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
                    "environment": "staging",
                    "pool_name": "gb10",
                    "image_tag": "staging-old",
                    "max_concurrent": 10,
                    "env_config_version": "staging-old",
                    "source_git_commit": "old1111111111111111111111111111111111111",
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
        "worker_pool_autoscaler_policies[staging/gb10].actuator",
        "worker_pool_autoscaler_policies[staging/gb10].actuator_config",
        "gb10_worker_pool_desired_states[staging/gb10].image_tag",
        "gb10_worker_pool_desired_states[staging/gb10].env_config_version",
        "gb10_worker_pool_desired_states[staging/gb10].source_git_commit",
    ]
    assert drift[0].desired == "slurm"
    assert drift[0].live == "gb10"


def _gb10_live_with_node_source(
    *,
    image_tag: str,
    env_config_version: str,
    source_git_commit: str | None,
    source_git_dirty: bool | None,
    hostname: str = "trt-gb10-1",
    apply_state: str = "applied",
    intent: str = "active",
) -> dict[str, Any]:
    """Live-state payload shape matching a converged desired_state plus
    one node report. Only the node-level source fields change per test."""
    return {
        "autoscaler_status": {
            "policies": [
                {
                    "environment": "staging",
                    "pool_name": "gb10",
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
            ],
        },
        "gb10_status": {
            "desired_states": [
                {
                    "environment": "staging",
                    "pool_name": "gb10",
                    "image_tag": image_tag,
                    "max_concurrent": 10,
                    "env_config_version": env_config_version,
                    "source_git_commit": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
                    "target_slots": 150,
                    "host_intents": {
                        "trt-gb10-1": "active",
                        "trt-gb10-2": "active",
                    },
                    "rollout_policy": {"mode": "all"},
                    "env": {},
                },
            ],
            "nodes": [
                {
                    "environment": "staging",
                    "pool_name": "gb10",
                    "hostname": hostname,
                    "current_intent": intent,
                    "desired_intent": intent,
                    "apply_state": apply_state,
                    "current_image_tag": image_tag,
                    "current_env_config_version": env_config_version,
                    "source_git_commit": source_git_commit,
                    "source_git_dirty": source_git_dirty,
                },
            ],
        },
    }


def test_diff_environment_state_reports_gb10_node_source_git_commit_drift(
    tmp_path: Path,
) -> None:
    """#356 regression: DB-side image_tag/env_config_version can converge
    while a node still runs stale source. environment-state check must
    fail hard so the release gate does not silently pass."""
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-c72f50d",
            "ENV_CONFIG_VERSION": "staging-c72f50d",
            "GIT_SHA": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        },
    )

    # Node reports converged image/env tags but a stale source_git_commit
    # (the exact pathology from `staging-c72f50d` on 2026-07-02).
    live = _gb10_live_with_node_source(
        image_tag="staging-c72f50d",
        env_config_version="staging-c72f50d",
        source_git_commit="ce55a358d8472bce4b580a363806993678d8f116",
        source_git_dirty=False,
    )

    drift = diff_environment_state(profile, live)

    source_drift = [item for item in drift if "source_git_commit" in item.path]
    assert len(source_drift) == 1, (
        f"expected exactly one source_git_commit drift entry, got {drift}"
    )
    assert source_drift[0].path == (
        "gb10_worker_node_status[staging/gb10/trt-gb10-1].source_git_commit"
    )
    assert source_drift[0].desired == "c72f50d67f0d571fef55a9abbbced4e37752ca0e"
    assert source_drift[0].live == "ce55a358d8472bce4b580a363806993678d8f116"


def test_diff_environment_state_rejects_suffix_after_explicit_source_commit(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    expected_source = "c72f50d67f0d571fef55a9abbbced4e37752ca0e"
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-c72f50d",
            "ENV_CONFIG_VERSION": "staging-c72f50d",
            "GIT_SHA": expected_source,
        },
    )
    live = _gb10_live_with_node_source(
        image_tag="staging-c72f50d",
        env_config_version="staging-c72f50d",
        source_git_commit=f"{expected_source}-junk",
        source_git_dirty=False,
    )

    drift = diff_environment_state(profile, live)

    source_drift = [item for item in drift if "source_git_commit" in item.path]
    assert len(source_drift) == 1
    assert source_drift[0].desired == expected_source
    assert source_drift[0].live == f"{expected_source}-junk"


def test_diff_environment_state_reports_gb10_node_dirty_source(
    tmp_path: Path,
) -> None:
    """Dirty host-local checkout is drift even if the SHA matches: a
    dirty runner directory means a human patched files locally and the
    release gate cannot vouch for what code the workers are actually
    running."""
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-c72f50d",
            "ENV_CONFIG_VERSION": "staging-c72f50d",
            "GIT_SHA": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        },
    )

    live = _gb10_live_with_node_source(
        image_tag="staging-c72f50d",
        env_config_version="staging-c72f50d",
        source_git_commit="c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        source_git_dirty=True,
    )

    drift = diff_environment_state(profile, live)

    dirty_drift = [item for item in drift if item.path.endswith(".source_git_dirty")]
    assert len(dirty_drift) == 1, drift
    assert dirty_drift[0].desired is False
    assert dirty_drift[0].live is True


def test_diff_environment_state_accepts_matching_gb10_node_source(
    tmp_path: Path,
) -> None:
    """No source drift when the node reports a commit that starts with
    the release-tag SHA prefix AND the checkout is clean."""
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-c72f50d",
            "ENV_CONFIG_VERSION": "staging-c72f50d",
            "GIT_SHA": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        },
    )

    live = _gb10_live_with_node_source(
        image_tag="staging-c72f50d",
        env_config_version="staging-c72f50d",
        source_git_commit="c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        source_git_dirty=False,
    )

    drift = diff_environment_state(profile, live)
    source_related = [item for item in drift if "source_git" in item.path]
    assert source_related == [], (
        f"fresh source with matching prefix should not produce drift; got {source_related}"
    )


def test_diff_environment_state_ignores_source_drift_on_stopped_gb10_node(
    tmp_path: Path,
) -> None:
    """A node whose intent is 'stopped' or 'draining' is not part of
    active capacity — the release cannot demand it be fresh."""
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path, host1_intent="stopped")
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-c72f50d",
            "ENV_CONFIG_VERSION": "staging-c72f50d",
            "GIT_SHA": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        },
    )

    live = _gb10_live_with_node_source(
        image_tag="staging-c72f50d",
        env_config_version="staging-c72f50d",
        source_git_commit="stalesha11111111111111111111111111111111",
        source_git_dirty=True,
        intent="stopped",
    )

    drift = diff_environment_state(profile, live)
    source_related = [item for item in drift if "source_git" in item.path]
    assert source_related == []


def test_diff_environment_state_uses_authoritative_stopped_intent_over_stale_node(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path, host1_intent="stopped")
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-c72f50d",
            "ENV_CONFIG_VERSION": "staging-c72f50d",
            "GIT_SHA": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        },
    )
    live = _gb10_live_with_node_source(
        image_tag="staging-c72f50d",
        env_config_version="staging-c72f50d",
        source_git_commit="stalesha11111111111111111111111111111111",
        source_git_dirty=True,
        intent="active",
    )
    live["gb10_status"]["desired_states"][0]["host_intents"]["trt-gb10-1"] = "stopped"

    drift = diff_environment_state(profile, live)

    assert [item for item in drift if "source_git" in item.path] == []


def test_diff_environment_state_authoritative_active_intent_checks_stale_stopped_node(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-c72f50d",
            "ENV_CONFIG_VERSION": "staging-c72f50d",
            "GIT_SHA": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        },
    )
    live = _gb10_live_with_node_source(
        image_tag="staging-c72f50d",
        env_config_version="staging-c72f50d",
        source_git_commit="stalesha11111111111111111111111111111111",
        source_git_dirty=True,
        intent="stopped",
        apply_state="stopped",
    )

    drift = diff_environment_state(profile, live)

    source_paths = [item.path for item in drift if "source_git" in item.path]
    assert source_paths == [
        "gb10_worker_node_status[staging/gb10/trt-gb10-1].source_git_commit",
    ]


def test_diff_environment_state_uses_explicit_source_when_image_tag_has_no_sha(
    tmp_path: Path,
) -> None:
    """A tag without an embedded SHA (e.g. 'latest', '0.7') can still
    enforce source convergence when the profile declares GIT_SHA."""
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "latest",
            "ENV_CONFIG_VERSION": "latest",
            "GIT_SHA": "c72f50d67f0d571fef55a9abbbced4e37752ca0e",
        },
    )

    live = _gb10_live_with_node_source(
        image_tag="latest",
        env_config_version="latest",
        source_git_commit="ce55a358d8472bce4b580a363806993678d8f116",
        source_git_dirty=False,
    )

    drift = diff_environment_state(profile, live)
    source_related = [item for item in drift if "source_git_commit" in item.path]
    assert len(source_related) == 1
    assert source_related[0].desired == "c72f50d67f0d571fef55a9abbbced4e37752ca0e"


def test_diff_environment_state_reports_missing_live_policy(tmp_path: Path) -> None:
    profile_path = tmp_path / "staging.state.toml"
    _write_profile(profile_path)
    profile = load_environment_state_profile(
        profile_path,
        variables={
            "IMAGE_TAG": "staging-57a7509",
            "ENV_CONFIG_VERSION": "staging-57a7509",
            "GIT_SHA": "57a750912345678901234567890123456789abcd",
        },
    )

    drift = diff_environment_state(
        profile,
        {"autoscaler_status": {"policies": []}, "gb10_status": {"desired_states": []}},
    )

    assert drift[0].path == "worker_pool_autoscaler_policies[staging/gb10]"
    assert drift[0].live is None
    assert drift[1].path == "gb10_worker_pool_desired_states[staging/gb10]"
    assert drift[1].live is None


def test_diff_environment_state_reports_active_slurm_job_runtime_drift(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "x86_64"
allowed_nodes = ["oldlab-1"]
env_file = "/shared_work/qianyi/loom-worker-capacity/staging-oldlab-worker.env"
repo_dir = "/shared_work/qianyi/loom-remote-worker"
requested_cpus = 2
requested_memory_mib = 8192
requested_concurrency = 1
external_runner = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_environment_state(
        profile,
        {
            "autoscaler_status": {
                "policies": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "actuator": "slurm",
                        "enabled": True,
                        "min_slots": 1,
                        "max_slots": 40,
                        "scale_up_threshold_slots": 1,
                        "scale_down_idle_seconds": 600,
                        "scale_up_cooldown_seconds": 60,
                        "scale_down_cooldown_seconds": 300,
                        "drain_timeout_seconds": 600,
                        "force": False,
                        "actuator_config": {
                            "backend": "docker",
                            "cpu_arch": "x86_64",
                            "allowed_nodes": ["oldlab-1"],
                            "env_file": "/shared_work/qianyi/loom-worker-capacity/staging-oldlab-worker.env",
                            "repo_dir": "/shared_work/qianyi/loom-remote-worker",
                            "requested_cpus": 2,
                            "requested_memory_mib": 8192,
                            "requested_concurrency": 1,
                            "external_runner": True,
                        },
                    },
                ],
            },
            "gb10_status": {"desired_states": []},
            "slurm_status": {
                "jobs": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "job_id": "14893",
                        "state": "running",
                        "nodelist": "oldlab-1",
                        "redacted_env": {
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/shared_work/qianyi/loom-worker-capacity/issue45-oldlab-4-warm-1608b05.env",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/shared_work/qianyi/loom-remote-worker-1608b05",
                            "LOOM_WORKER_MAX_CONCURRENT": "1",
                        },
                    },
                ],
            },
        },
    )

    assert [item.path for item in drift] == [
        "slurm_worker_jobs[production/oldlab/14893].LOOM_REMOTE_WORKER_ENV_FILE",
        "slurm_worker_jobs[production/oldlab/14893].LOOM_REMOTE_WORKER_REPO_DIR",
    ]
    assert drift[0].desired.endswith("staging-oldlab-worker.env")
    assert drift[0].live.endswith("issue45-oldlab-4-warm-1608b05.env")


def test_diff_environment_state_rejects_active_job_outside_allowed_nodes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "loom_cli.environment_state.staging_gb10_external_activation_blockers",
        lambda **_kwargs: (),
    )
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "gb10"
actuator = "slurm"
enabled = true
min_slots = 0
max_slots = 10

[worker_pool_autoscaler_policies.actuator_config]
allowed_nodes = ["trt-gb10-1"]
env_file = "/secure/.env.remote-worker"
repo_dir = "/opt/loom"
requested_concurrency = 10
external_runner = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_environment_state(
        profile,
        {
            "autoscaler_status": {"policies": []},
            "gb10_status": {"desired_states": []},
            "slurm_status": {
                "jobs": [
                    {
                        "environment": "production",
                        "pool_name": "gb10",
                        "job_id": "gb10-job-7",
                        "state": "running",
                        "nodelist": "trt-gb10-7",
                        "redacted_env": {
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/secure/.env.remote-worker",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/opt/loom",
                        },
                    },
                ],
            },
        },
    )

    node_drift = next(
        item
        for item in drift
        if item.path == "slurm_worker_jobs[production/gb10/gb10-job-7].nodelist"
    )
    assert node_drift.desired == ["trt-gb10-1"]
    assert node_drift.live == "trt-gb10-7"


def test_diff_environment_state_reports_active_slurm_job_worker_token_fingerprint_drift(
    tmp_path: Path,
) -> None:
    active_token = "loom_w_current_environment_token"
    stale_token = "loom_w_stale_slurm_job_token"
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
allowed_nodes = ["oldlab-1"]
env_file = "/shared_work/qianyi/loom-worker-capacity/staging-oldlab-worker.env"
repo_dir = "/shared_work/qianyi/loom-remote-worker"
requested_concurrency = 1
external_runner = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_environment_state(
        profile,
        {
            "autoscaler_status": {"policies": []},
            "gb10_status": {"desired_states": []},
            "slurm_status": {
                "jobs": [
                    {
                        "environment": "production",
                        "pool_name": "oldlab",
                        "job_id": "14893",
                        "state": "running",
                        "nodelist": "oldlab-1",
                        "redacted_env": {
                            "LOOM_REMOTE_WORKER_ENV_FILE": "/shared_work/qianyi/loom-worker-capacity/staging-oldlab-worker.env",
                            "LOOM_REMOTE_WORKER_REPO_DIR": "/shared_work/qianyi/loom-remote-worker",
                            "LOOM_WORKER_AUTH_FINGERPRINT": (
                                f"sha256:{hashlib.sha256(stale_token.encode()).hexdigest()[:12]} "
                                f"len={len(stale_token)}"
                            ),
                        },
                    },
                ],
            },
        },
        expected_worker_token=active_token,
    )

    assert any(
        item.path == "slurm_worker_jobs[production/oldlab/14893].LOOM_WORKER_AUTH_FINGERPRINT"
        for item in drift
    )
    token_drift = next(item for item in drift if item.path.endswith("LOOM_WORKER_AUTH_FINGERPRINT"))
    assert token_drift.desired == (
        f"sha256:{hashlib.sha256(active_token.encode()).hexdigest()[:12]} len={len(active_token)}"
    )
    assert stale_token not in str(token_drift)
    assert active_token not in str(token_drift)


def test_external_slurm_runner_prerequisite_check_reports_missing_env_and_dirty_repo(
    tmp_path: Path,
) -> None:
    repo_dir = tmp_path / "loom-remote-worker"
    repo_dir.mkdir()
    (repo_dir / ".git").mkdir()
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
backend = "docker"
cpu_arch = "x86_64"
env_file = "{tmp_path / "missing.env"}"
repo_dir = "{repo_dir}"
requested_concurrency = 1
external_runner = true

[external_slurm_runner_prerequisites]
expected_repo_ref = "staging-57a7509"
require_clean_repo = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    def _runner(command: list[str], *, cwd: Path | None = None) -> tuple[int, str, str]:
        assert command[:2] == ["git", "-C"]
        if command[-2:] == ["rev-parse", "HEAD"]:
            return 0, "62eb0a6d0000000000000000000000000000000\n", ""
        if command[-3:] == ["status", "--short", "--untracked-files=no"]:
            return 0, " M src/loom/trial/step_runner.py\n", ""
        raise AssertionError(command)

    drift = diff_external_slurm_runner_prerequisites(profile, runner=_runner)

    assert [item.path for item in drift] == [
        "external_slurm_runner_prerequisites[production/oldlab].env_file",
        "external_slurm_runner_prerequisites[production/oldlab].repo_dir.git_head",
        "external_slurm_runner_prerequisites[production/oldlab].repo_dir.git_status",
    ]
    assert drift[0].desired == str(tmp_path / "missing.env")
    assert drift[0].live == "missing"
    assert drift[1].desired == "staging-57a7509"
    assert drift[1].live.startswith("62eb0a6")
    assert drift[2].desired == "clean"
    assert "step_runner.py" in drift[2].live


def test_external_slurm_runner_prerequisite_check_reports_worker_token_fingerprint_drift(
    tmp_path: Path,
) -> None:
    stale_token = "loom_w_stale_remote_worker_token"
    active_token = "loom_w_current_environment_token"
    env_file = tmp_path / "remote-worker.env"
    env_file.write_text(
        f"LOOM_WORKER_TOKEN={stale_token}\n",
        encoding="utf-8",
    )
    repo_dir = tmp_path / "loom-remote-worker"
    repo_dir.mkdir()
    (repo_dir / ".git").mkdir()
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
env_file = "{env_file}"
repo_dir = "{repo_dir}"
requested_concurrency = 1
external_runner = true

[external_slurm_runner_prerequisites]
pools = ["oldlab"]
require_worker_token_parity = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_external_slurm_runner_prerequisites(
        profile,
        expected_worker_token=active_token,
    )

    assert [item.path for item in drift] == [
        "external_slurm_runner_prerequisites[production/oldlab].worker_token_fingerprint",
    ]
    assert drift[0].desired == (
        f"sha256:{hashlib.sha256(active_token.encode()).hexdigest()[:12]} len={len(active_token)}"
    )
    assert drift[0].live == (
        f"sha256:{hashlib.sha256(stale_token.encode()).hexdigest()[:12]} len={len(stale_token)}"
    )
    assert stale_token not in str(drift[0])
    assert active_token not in str(drift[0])


def test_external_slurm_runner_prerequisite_requires_worker_token_when_parity_enabled(
    tmp_path: Path,
) -> None:
    env_file = tmp_path / "remote-worker.env"
    env_file.write_text("LOOM_WORKER_TOKEN=loom_w_remote\n", encoding="utf-8")
    repo_dir = tmp_path / "loom-remote-worker"
    repo_dir.mkdir()
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
env_file = "{env_file}"
repo_dir = "{repo_dir}"
requested_concurrency = 1
external_runner = true

[external_slurm_runner_prerequisites]
pools = ["oldlab"]
require_worker_token_parity = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_external_slurm_runner_prerequisites(profile)

    assert [item.path for item in drift] == [
        "external_slurm_runner_prerequisites[production/oldlab].worker_token_fingerprint",
    ]
    assert drift[0].desired == "active worker token fingerprint"
    assert drift[0].live == "missing --worker-token"


def test_external_slurm_runner_prerequisite_reports_missing_worker_token_key(
    tmp_path: Path,
) -> None:
    active_token = "loom_w_current_environment_token"
    env_file = tmp_path / "remote-worker.env"
    env_file.write_text("LOOM_WORKER_POOL_NAME=oldlab\n", encoding="utf-8")
    repo_dir = tmp_path / "loom-remote-worker"
    repo_dir.mkdir()
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
env_file = "{env_file}"
repo_dir = "{repo_dir}"
requested_concurrency = 1
external_runner = true

[external_slurm_runner_prerequisites]
pools = ["oldlab"]
require_worker_token_parity = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_external_slurm_runner_prerequisites(
        profile,
        expected_worker_token=active_token,
    )

    assert [item.path for item in drift] == [
        "external_slurm_runner_prerequisites[production/oldlab].worker_token_fingerprint",
    ]
    assert drift[0].live == "missing LOOM_WORKER_TOKEN"
    assert active_token not in str(drift[0])


def test_external_slurm_runner_prerequisite_reads_exported_quoted_worker_token(
    tmp_path: Path,
) -> None:
    active_token = "loom_w_current_environment_token"
    env_file = tmp_path / "remote-worker.env"
    env_file.write_text(
        f'export LOOM_WORKER_TOKEN="{active_token}"\n',
        encoding="utf-8",
    )
    repo_dir = tmp_path / "loom-remote-worker"
    repo_dir.mkdir()
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"
control_plane_environment = "production"

[[worker_pool_autoscaler_policies]]
pool_name = "oldlab"
actuator = "slurm"
enabled = true
min_slots = 1
max_slots = 40

[worker_pool_autoscaler_policies.actuator_config]
env_file = "{env_file}"
repo_dir = "{repo_dir}"
requested_concurrency = 1
external_runner = true

[external_slurm_runner_prerequisites]
pools = ["oldlab"]
require_worker_token_parity = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)

    drift = diff_external_slurm_runner_prerequisites(
        profile,
        expected_worker_token=active_token,
    )

    assert drift == []


def test_external_slurm_autoscaler_supervisor_profile_is_normalized(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}"
python_path = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}/.venv/bin/python"
script_path = "/home/qianyi/dev/loom-worktrees/${IMAGE_TAG}/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab", "--namespace", "loom-staging"]
requires = ["network-online.target", "loom-staging-postgres-port-forward.service"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )

    profile = load_environment_state_profile(
        profile_path,
        variables={"IMAGE_TAG": "staging-052e420"},
    )

    assert profile.external_slurm_autoscaler_supervisors == [
        {
            "environment": "staging",
            "control_plane_environment": "staging",
            "name": "oldlab",
            "pool_name": "oldlab",
            "execution_host": "local",
            "service_name": "loom-oldlab-autoscaler.service",
            "timer_name": "loom-oldlab-autoscaler.timer",
            "working_directory": "/home/qianyi/dev/loom-worktrees/staging-052e420",
            "python_path": ("/home/qianyi/dev/loom-worktrees/staging-052e420/.venv/bin/python"),
            "script_path": (
                "/home/qianyi/dev/loom-worktrees/staging-052e420/"
                "scripts/ops/worker_pool_autoscaler_external_once.py"
            ),
            "args": [
                "--environment",
                "staging",
                "--pool-name",
                "oldlab",
                "--namespace",
                "loom-staging",
            ],
            "requires": [
                "network-online.target",
                "loom-staging-postgres-port-forward.service",
            ],
            "timer_on_boot_sec": "45",
            "timer_on_unit_active_sec": "30",
            "timer_accuracy_sec": "5",
            "service_timeout_sec": "180",
            "enabled": True,
            "active": True,
        },
    ]


def test_external_supervisor_environment_arg_binds_control_plane_alias(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """\
environment = "staging"
control_plane_environment = "production"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging"
python_path = "/srv/loom/staging/.venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "production", "--pool-name", "oldlab"]
requires = ["network-online.target"]
""",
        encoding="utf-8",
    )

    profile = load_environment_state_profile(profile_path)

    assert profile.environment == "staging"
    assert profile.control_plane_environment == "production"
    supervisor = profile.external_slurm_autoscaler_supervisors[0]
    assert supervisor["environment"] == "staging"
    assert supervisor["control_plane_environment"] == "production"
    assert supervisor["args"][:2] == ["--environment", "production"]


def test_external_supervisor_rejects_deployment_environment_for_aliased_cp(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """\
environment = "staging"
control_plane_environment = "production"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging"
python_path = "/srv/loom/staging/.venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
""",
        encoding="utf-8",
    )

    with pytest.raises(
        EnvironmentStateProfileError,
        match="exactly one --environment production",
    ):
        load_environment_state_profile(profile_path)


@pytest.mark.parametrize("field", ["enabled", "active"])
def test_external_slurm_autoscaler_supervisor_requires_strict_boolean(
    tmp_path: Path,
    field: str,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""\
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging"
python_path = "/srv/loom/staging/.venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
{field} = "false"
""",
        encoding="utf-8",
    )

    with pytest.raises(EnvironmentStateProfileError, match=rf"{field} must be a boolean"):
        load_environment_state_profile(profile_path)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("pool_name", 'pool_name = """oldlab\nExecStart=/bin/false"""'),
        (
            "working_directory",
            'working_directory = """/srv/loom/staging\nExecStart=/bin/false"""',
        ),
        (
            "python_path",
            'python_path = """/srv/loom/staging/.venv/bin/python\nExecStart=/bin/false"""',
        ),
        (
            "script_path",
            'script_path = """scripts/ops/worker_pool_autoscaler_external_once.py\n'
            'ExecStart=/bin/false"""',
        ),
        (
            "args",
            'args = ["--environment", "staging", "--pool-name", "oldlab", '
            '"""safe\nExecStart=/bin/false"""]',
        ),
        (
            "requires",
            'requires = ["network-online.target", """safe.target\nExecStart=/bin/false"""]',
        ),
    ],
)
def test_external_slurm_autoscaler_supervisor_rejects_directive_injection(
    tmp_path: Path,
    field: str,
    replacement: str,
) -> None:
    lines = [
        'environment = "staging"',
        "",
        "[[external_slurm_autoscaler_supervisors]]",
        'name = "oldlab"',
        'pool_name = "oldlab"',
        'service_name = "loom-oldlab-autoscaler.service"',
        'timer_name = "loom-oldlab-autoscaler.timer"',
        'working_directory = "/srv/loom/staging"',
        'python_path = "/srv/loom/staging/.venv/bin/python"',
        'script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"',
        'args = ["--environment", "staging", "--pool-name", "oldlab"]',
        'requires = ["network-online.target"]',
    ]
    prefix = f"{field} = "
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        "\n".join(replacement if line.startswith(prefix) else line for line in lines) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(EnvironmentStateProfileError, match="control-free"):
        load_environment_state_profile(profile_path)


def test_external_slurm_autoscaler_supervisor_rejects_unsafe_dependency_basename(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """\
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging"
python_path = "/srv/loom/staging/.venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["../unsafe.target"]
""",
        encoding="utf-8",
    )

    with pytest.raises(EnvironmentStateProfileError, match="safe systemd unit basename"):
        load_environment_state_profile(profile_path)


def test_external_slurm_autoscaler_supervisor_rejects_c1_control_character(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """\
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom\u0085ExecStart=/bin/false"
python_path = "/srv/loom/staging/.venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
""",
        encoding="utf-8",
    )

    with pytest.raises(EnvironmentStateProfileError, match="control-free"):
        load_environment_state_profile(profile_path)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("service_name", "loom-safe.service\\nEnvironment=BAD=1"),
        ("timer_name", "../loom-safe.timer"),
        ("timer_on_boot_sec", "0"),
        ("timer_on_unit_active_sec", "30s"),
        ("timer_accuracy_sec", "5\\nOnBootSec=1"),
        ("service_timeout_sec", "7201"),
    ],
)
def test_external_slurm_autoscaler_supervisor_rejects_unsafe_unit_fields(
    tmp_path: Path,
    field: str,
    value: str,
) -> None:
    values = {
        "service_name": "loom-oldlab-autoscaler.service",
        "timer_name": "loom-oldlab-autoscaler.timer",
        "timer_on_boot_sec": "45",
        "timer_on_unit_active_sec": "30",
        "timer_accuracy_sec": "5",
        "service_timeout_sec": "180",
    }
    values[field] = value
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f'''\
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "{values["service_name"]}"
timer_name = "{values["timer_name"]}"
working_directory = "/srv/loom/staging"
python_path = "/srv/loom/staging/.venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
timer_on_boot_sec = "{values["timer_on_boot_sec"]}"
timer_on_unit_active_sec = "{values["timer_on_unit_active_sec"]}"
timer_accuracy_sec = "{values["timer_accuracy_sec"]}"
service_timeout_sec = "{values["service_timeout_sec"]}"
''',
        encoding="utf-8",
    )

    with pytest.raises(EnvironmentStateProfileError):
        load_environment_state_profile(profile_path)


def test_external_slurm_autoscaler_supervisor_check_reports_stale_inactive_unit(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "loom"
    python_path = workdir / ".venv" / "bin" / "python"
    script = workdir / "scripts" / "ops" / "worker_pool_autoscaler_external_once.py"
    python_path.parent.mkdir(parents=True)
    script.parent.mkdir(parents=True)
    python_path.write_text("#!/bin/sh\n", encoding="utf-8")
    script.write_text("print('ok')\n", encoding="utf-8")
    python_path.chmod(0o755)
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "{workdir}"
python_path = "{python_path}"
script_path = "{script}"
args = ["--environment", "staging", "--pool-name", "oldlab", "--namespace", "loom-staging"]
requires = ["network-online.target", "loom-staging-postgres-port-forward.service"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    stale_service_unit = """
[Service]
Type=oneshot
WorkingDirectory=/home/qianyi/dev/loom-worktrees/staging-b453057
Environment=PYTHONPATH=/home/qianyi/dev/loom-worktrees/staging-b453057/src
ExecStart=/home/qianyi/dev/loom-worktrees/staging-b453057/.venv/bin/python /home/qianyi/dev/loom-ops/oldlab_autoscaler_external_once.py
""".strip()
    desired_timer_unit = """
[Unit]
Description=Run Loom oldlab external autoscaler reconcile

[Timer]
OnBootSec=45
OnUnitActiveSec=30
AccuracySec=5
Unit=loom-oldlab-autoscaler.service

[Install]
WantedBy=timers.target
""".strip()

    def _runner(command: list[str]) -> tuple[int, str, str]:
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.service"]:
            return 0, stale_service_unit, ""
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.timer"]:
            return 0, desired_timer_unit, ""
        if command == ["systemctl", "--user", "is-enabled", "loom-oldlab-autoscaler.timer"]:
            return 0, "enabled\n", ""
        if command == ["systemctl", "--user", "is-active", "loom-oldlab-autoscaler.timer"]:
            return 3, "inactive\n", ""
        if command == [
            "systemctl",
            "--user",
            "show",
            "loom-oldlab-autoscaler.service",
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=ExecMainCode",
            "--property=ActiveState",
            "--property=SubState",
        ]:
            return 0, "Result=success\nExecMainStatus=0\nActiveState=inactive\n", ""
        raise AssertionError(command)

    drift = diff_external_slurm_autoscaler_supervisors(profile, runner=_runner)

    assert [item.path for item in drift] == [
        "external_slurm_autoscaler_supervisors[staging/oldlab].service_unit",
        "external_slurm_autoscaler_supervisors[staging/oldlab].timer_active",
    ]
    assert "--pool-name oldlab" in drift[0].desired
    assert "staging-b453057" in drift[0].live
    assert drift[1].desired == "active"
    assert drift[1].live == "inactive"


def test_external_slurm_autoscaler_supervisor_check_reports_unusable_execstart(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "loom"
    workdir.mkdir()
    script = workdir / "scripts" / "ops" / "worker_pool_autoscaler_external_once.py"
    script.parent.mkdir(parents=True)
    script.write_text("print('ok')\n", encoding="utf-8")
    python_path = workdir / ".venv" / "bin" / "python"
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "{workdir}"
python_path = "{python_path}"
script_path = "{script}"
args = ["--environment", "staging", "--pool-name", "oldlab", "--namespace", "loom-staging"]
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    supervisor = profile.external_slurm_autoscaler_supervisors[0]

    def _runner(command: list[str]) -> tuple[int, str, str]:
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.service"]:
            return 0, render_external_slurm_autoscaler_service(supervisor), ""
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.timer"]:
            return 0, render_external_slurm_autoscaler_timer(supervisor), ""
        if command == ["systemctl", "--user", "is-enabled", "loom-oldlab-autoscaler.timer"]:
            return 0, "enabled\n", ""
        if command == ["systemctl", "--user", "is-active", "loom-oldlab-autoscaler.timer"]:
            return 0, "active\n", ""
        if command == [
            "systemctl",
            "--user",
            "show",
            "loom-oldlab-autoscaler.service",
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=ExecMainCode",
            "--property=ActiveState",
            "--property=SubState",
        ]:
            return 0, "Result=success\nExecMainStatus=0\nActiveState=inactive\n", ""
        raise AssertionError(command)

    drift = diff_external_slurm_autoscaler_supervisors(profile, runner=_runner)

    assert [item.path for item in drift] == [
        "external_slurm_autoscaler_supervisors[staging/oldlab].exec_start.python_path",
    ]
    assert drift[0].desired == str(python_path)
    assert drift[0].live == "missing"


def test_external_slurm_autoscaler_supervisor_check_reports_unexecutable_python(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "loom"
    python_path = workdir / ".venv" / "bin" / "python"
    script = workdir / "scripts" / "ops" / "worker_pool_autoscaler_external_once.py"
    python_path.parent.mkdir(parents=True)
    script.parent.mkdir(parents=True)
    python_path.write_text("#!/bin/sh\n", encoding="utf-8")
    script.write_text("print('ok')\n", encoding="utf-8")
    python_path.chmod(0o644)
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "{workdir}"
python_path = "{python_path}"
script_path = "{script}"
args = ["--environment", "staging", "--pool-name", "oldlab"]
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    supervisor = profile.external_slurm_autoscaler_supervisors[0]

    def _runner(command: list[str]) -> tuple[int, str, str]:
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.service"]:
            return 0, render_external_slurm_autoscaler_service(supervisor), ""
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.timer"]:
            return 0, render_external_slurm_autoscaler_timer(supervisor), ""
        if command == ["systemctl", "--user", "is-enabled", "loom-oldlab-autoscaler.timer"]:
            return 0, "enabled\n", ""
        if command == ["systemctl", "--user", "is-active", "loom-oldlab-autoscaler.timer"]:
            return 0, "active\n", ""
        if command == [
            "systemctl",
            "--user",
            "show",
            "loom-oldlab-autoscaler.service",
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=ExecMainCode",
            "--property=ActiveState",
            "--property=SubState",
        ]:
            return 0, "Result=success\nExecMainStatus=0\nActiveState=inactive\n", ""
        raise AssertionError(command)

    drift = diff_external_slurm_autoscaler_supervisors(profile, runner=_runner)

    assert [item.path for item in drift] == [
        "external_slurm_autoscaler_supervisors[staging/oldlab].exec_start.python_path",
    ]
    assert drift[0].desired == str(python_path)
    assert drift[0].live == "not executable"


def test_external_slurm_autoscaler_supervisor_check_reports_failed_service_status(
    tmp_path: Path,
) -> None:
    workdir = tmp_path / "loom"
    python_path = workdir / ".venv" / "bin" / "python"
    script = workdir / "scripts" / "ops" / "worker_pool_autoscaler_external_once.py"
    python_path.parent.mkdir(parents=True)
    script.parent.mkdir(parents=True)
    python_path.write_text("#!/bin/sh\n", encoding="utf-8")
    script.write_text("print('ok')\n", encoding="utf-8")
    python_path.chmod(0o755)
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        f"""
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "{workdir}"
python_path = "{python_path}"
script_path = "{script}"
args = ["--environment", "staging", "--pool-name", "oldlab", "--namespace", "loom-staging"]
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    supervisor = profile.external_slurm_autoscaler_supervisors[0]

    def _runner(command: list[str]) -> tuple[int, str, str]:
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.service"]:
            return 0, render_external_slurm_autoscaler_service(supervisor), ""
        if command == ["systemctl", "--user", "cat", "loom-oldlab-autoscaler.timer"]:
            return 0, render_external_slurm_autoscaler_timer(supervisor), ""
        if command == ["systemctl", "--user", "is-enabled", "loom-oldlab-autoscaler.timer"]:
            return 0, "enabled\n", ""
        if command == ["systemctl", "--user", "is-active", "loom-oldlab-autoscaler.timer"]:
            return 0, "active\n", ""
        if command == [
            "systemctl",
            "--user",
            "show",
            "loom-oldlab-autoscaler.service",
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=ExecMainCode",
            "--property=ActiveState",
            "--property=SubState",
        ]:
            return (
                0,
                "Result=exit-code\nExecMainStatus=203\nExecMainCode=1\n"
                "ActiveState=failed\nSubState=failed\n",
                "",
            )
        raise AssertionError(command)

    drift = diff_external_slurm_autoscaler_supervisors(profile, runner=_runner)

    assert [item.path for item in drift] == [
        "external_slurm_autoscaler_supervisors[staging/oldlab].service_status",
    ]
    assert drift[0].desired == "service result success"
    assert drift[0].live == {
        "active_state": "failed",
        "exec_main_code": "1",
        "exec_main_status": "203",
        "result": "exit-code",
        "sub_state": "failed",
    }


def test_external_slurm_autoscaler_supervisor_apply_writes_units_and_starts_timer(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging-052e420"
python_path = "/srv/loom/staging-052e420/.venv/bin/python"
script_path = "/srv/loom/staging-052e420/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = true
active = true
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    unit_dir = tmp_path / "systemd-user"
    commands: list[list[str]] = []

    def _runner(command: list[str]) -> tuple[int, str, str]:
        commands.append(command)
        return 0, "", ""

    applied = apply_external_slurm_autoscaler_supervisors(
        profile,
        unit_dir=unit_dir,
        runner=_runner,
    )

    service_unit = (unit_dir / "loom-oldlab-autoscaler.service").read_text(
        encoding="utf-8",
    )
    timer_unit = (unit_dir / "loom-oldlab-autoscaler.timer").read_text(
        encoding="utf-8",
    )
    assert "WorkingDirectory=/srv/loom/staging-052e420" in service_unit
    assert "--pool-name oldlab" in service_unit
    assert "Unit=loom-oldlab-autoscaler.service" in timer_unit
    assert commands == [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", "--now", "loom-oldlab-autoscaler.timer"],
        ["systemctl", "--user", "restart", "loom-oldlab-autoscaler.timer"],
    ]
    assert applied == [
        {
            "kind": "external_slurm_autoscaler_supervisor",
            "service": "loom-oldlab-autoscaler.service",
            "timer": "loom-oldlab-autoscaler.timer",
        },
    ]


def test_external_supervisor_apply_manages_local_and_retires_foreign_units(
    tmp_path: Path,
) -> None:
    profile_path = tmp_path / "profile.toml"
    profile_path.write_text(
        """
environment = "test"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab-staging"
pool_name = "oldlab"
execution_host = "TRT-EAI-OLDLAB-1"
service_name = "loom-autoscaler-oldlab-staging.service"
timer_name = "loom-autoscaler-oldlab-staging.timer"
working_directory = "/opt/loom/oldlab"
python_path = "/opt/loom/oldlab/bin/python"
script_path = "/opt/loom/oldlab/autoscaler.py"
args = ["--environment", "test", "--pool-name", "oldlab"]
enabled = true
active = true

[[external_slurm_autoscaler_supervisors]]
name = "gb10-staging"
pool_name = "gb10"
execution_host = "gx10-01c7"
service_name = "loom-autoscaler-gb10-staging.service"
timer_name = "loom-autoscaler-gb10-staging.timer"
working_directory = "/opt/loom/gb10"
python_path = "/opt/loom/gb10/bin/python"
script_path = "/opt/loom/gb10/autoscaler.py"
args = ["--environment", "test", "--pool-name", "gb10"]
enabled = true
active = true
""",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(
        profile_path,
        expected_environment="test",
    )
    commands: list[list[str]] = []
    unit_dir = tmp_path / "units"
    unit_dir.mkdir()
    (unit_dir / "loom-autoscaler-oldlab-staging.service").write_text("stale\n")
    (unit_dir / "loom-autoscaler-oldlab-staging.timer").write_text("stale\n")

    def _runner(command: list[str]) -> tuple[int, str, str]:
        commands.append(command)
        return 0, "", ""

    applied = apply_external_slurm_autoscaler_supervisors(
        profile,
        unit_dir=unit_dir,
        runner=_runner,
        hostname="gx10-01c7",
    )

    assert applied == [
        {
            "kind": "external_slurm_autoscaler_supervisor",
            "service": "loom-autoscaler-gb10-staging.service",
            "timer": "loom-autoscaler-gb10-staging.timer",
        }
    ]
    assert not (unit_dir / "loom-autoscaler-oldlab-staging.service").exists()
    assert not (unit_dir / "loom-autoscaler-oldlab-staging.timer").exists()
    assert (unit_dir / "loom-autoscaler-gb10-staging.service").is_file()
    assert commands == [
        ["systemctl", "--user", "stop", "loom-autoscaler-oldlab-staging.timer"],
        ["systemctl", "--user", "disable", "loom-autoscaler-oldlab-staging.timer"],
        ["systemctl", "--user", "daemon-reload"],
        [
            "systemctl",
            "--user",
            "enable",
            "--now",
            "loom-autoscaler-gb10-staging.timer",
        ],
        ["systemctl", "--user", "restart", "loom-autoscaler-gb10-staging.timer"],
    ]


def test_external_supervisor_diff_rejects_foreign_controller_unit(tmp_path: Path) -> None:
    profile_path = tmp_path / "profile.toml"
    profile_path.write_text(
        """
environment = "test"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab-staging"
pool_name = "oldlab"
execution_host = "TRT-EAI-OLDLAB-1"
service_name = "loom-autoscaler-oldlab-staging.service"
timer_name = "loom-autoscaler-oldlab-staging.timer"
working_directory = "/opt/loom/oldlab"
python_path = "/opt/loom/oldlab/bin/python"
script_path = "/opt/loom/oldlab/autoscaler.py"
args = ["--environment", "test", "--pool-name", "oldlab"]
enabled = true
active = true
""",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(
        profile_path,
        expected_environment="test",
    )

    def _runner(command: list[str]) -> tuple[int, str, str]:
        unit = command[-1]
        if command[:3] == ["systemctl", "--user", "cat"]:
            if unit.startswith("loom-autoscaler-oldlab-"):
                return 0, "[Unit]\nDescription=stale foreign authority\n", ""
            return 1, "", "unit not found"
        raise AssertionError(command)

    drift = diff_external_slurm_autoscaler_supervisors(
        profile,
        runner=_runner,
        hostname="gx10-01c7",
    )

    assert [item.path for item in drift] == [
        "external_slurm_autoscaler_supervisors[test/oldlab].service_unit",
        "external_slurm_autoscaler_supervisors[test/oldlab].timer_unit",
    ]
    assert all(item.desired == "absent on foreign controller" for item in drift)


def test_committed_staging_supervisors_bind_to_their_physical_controllers() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables={
            "IMAGE_TAG": "staging-test",
            "ENV_CONFIG_VERSION": "staging-test",
            "GIT_SHA": "a" * 40,
        },
        expected_environment="staging",
    )

    by_pool = {
        supervisor["pool_name"]: supervisor["execution_host"]
        for supervisor in profile.external_slurm_autoscaler_supervisors
    }
    assert by_pool == {
        "gb10": "gx10-01c7",
        "oldlab": "TRT-EAI-OLDLAB-1",
    }


def test_external_slurm_autoscaler_supervisor_apply_disables_when_enabled_false(
    tmp_path: Path,
) -> None:
    """#331: enabled=false must translate to `disable --now`.

    Observed during 2026-07-02 OLDLAB exclusion: the operator prepared
    a scoped profile with `enabled=false` and `active=false`, expecting
    `environment-state apply` to stop the timer. Instead the apply left
    the running `loom-oldlab-autoscaler.timer` in place and it kept
    submitting Slurm jobs. Fix: negative desired state must produce
    negative systemctl calls (stop + disable), idempotently.
    """
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging-052e420"
python_path = "/srv/loom/staging-052e420/.venv/bin/python"
script_path = "/srv/loom/staging-052e420/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = false
active = false
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    unit_dir = tmp_path / "systemd-user"
    commands: list[list[str]] = []

    def _runner(command: list[str]) -> tuple[int, str, str]:
        commands.append(command)
        return 0, "", ""

    applied = apply_external_slurm_autoscaler_supervisors(
        profile,
        unit_dir=unit_dir,
        runner=_runner,
    )

    # `stop` first so the timer stops firing new triggers even if the
    # subsequent `disable` were to fail; `disable` after so the unit
    # doesn't re-enable across a systemd user restart.
    assert commands == [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "stop", "loom-oldlab-autoscaler.timer"],
        ["systemctl", "--user", "disable", "loom-oldlab-autoscaler.timer"],
    ]
    assert applied == [
        {
            "kind": "external_slurm_autoscaler_supervisor",
            "service": "loom-oldlab-autoscaler.service",
            "timer": "loom-oldlab-autoscaler.timer",
        },
    ]


def test_external_slurm_autoscaler_supervisor_apply_active_false_only_stops(
    tmp_path: Path,
) -> None:
    """#331: active=false + enabled=true means stop but keep enabled.

    Useful for a temporary pause where the operator wants systemd to
    remember the timer for the next boot but doesn't want it firing now.
    """
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging-052e420"
python_path = "/srv/loom/staging-052e420/.venv/bin/python"
script_path = "/srv/loom/staging-052e420/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = true
active = false
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    unit_dir = tmp_path / "systemd-user"
    commands: list[list[str]] = []

    def _runner(command: list[str]) -> tuple[int, str, str]:
        commands.append(command)
        return 0, "", ""

    apply_external_slurm_autoscaler_supervisors(
        profile,
        unit_dir=unit_dir,
        runner=_runner,
    )

    assert commands == [
        ["systemctl", "--user", "daemon-reload"],
        ["systemctl", "--user", "enable", "loom-oldlab-autoscaler.timer"],
        ["systemctl", "--user", "stop", "loom-oldlab-autoscaler.timer"],
    ]


@pytest.mark.parametrize("missing_rc", [1, 5])
def test_external_slurm_autoscaler_supervisor_apply_idempotent_when_unit_missing(
    tmp_path: Path,
    missing_rc: int,
) -> None:
    """#331 acceptance: keep idempotency when the unit is already
    disabled/inactive or missing.

    systemctl exit code 5 = 'not loaded' / no such unit. That happens on
    an environment where the supervisor was never installed, and calling
    disable/stop on a nothing is a legitimate no-op. Must NOT raise.
    """
    profile_path = tmp_path / "staging.state.toml"
    profile_path.write_text(
        """
environment = "staging"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab"
pool_name = "oldlab"
service_name = "loom-oldlab-autoscaler.service"
timer_name = "loom-oldlab-autoscaler.timer"
working_directory = "/srv/loom/staging-052e420"
python_path = "/srv/loom/staging-052e420/.venv/bin/python"
script_path = "/srv/loom/staging-052e420/scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "staging", "--pool-name", "oldlab"]
requires = ["network-online.target"]
timer_on_boot_sec = "45"
timer_on_unit_active_sec = "30"
timer_accuracy_sec = "5"
enabled = false
active = false
""".strip()
        + "\n",
        encoding="utf-8",
    )
    profile = load_environment_state_profile(profile_path)
    unit_dir = tmp_path / "systemd-user"

    def _runner(command: list[str]) -> tuple[int, str, str]:
        # systemd versions disagree on whether a missing stop/disable is rc=1
        # or the LSB rc=5.  A LoadState probe distinguishes absence from a
        # real failure without relying on localized stderr text.
        if command[-1] == "daemon-reload":
            return 0, "", ""
        if "--property=LoadState" in command:
            return 0, "not-found\n", ""
        return (
            missing_rc,
            "",
            "Failed to disable unit: Unit file loom-oldlab-autoscaler.timer does not exist.",
        )

    # Must not raise — the supervisor is simply not installed.
    apply_external_slurm_autoscaler_supervisors(
        profile,
        unit_dir=unit_dir,
        runner=_runner,
    )


@pytest.mark.parametrize(
    ("path", "environment"),
    [
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
            "IMAGE_TAG": "staging-test",
            "ENV_CONFIG_VERSION": "staging-test",
            "GIT_SHA": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        expected_environment=environment,
    )

    gb10_policy = next(
        policy for policy in profile.autoscaler_policies if policy["pool_name"] == "gb10"
    )
    assert profile.environment == environment
    assert profile.control_plane_environment == environment
    assert gb10_policy["environment"] == environment
    assert gb10_policy["actuator"] == "slurm"
    assert gb10_policy["max_slots"] == 150
    assert gb10_policy["actuator_config"]["backend"] == "docker"
    assert gb10_policy["actuator_config"]["cpu_arch"] == "arm64"
    assert gb10_policy["actuator_config"]["partition"] == "gb10"
    assert gb10_policy["actuator_config"]["allowed_nodes"] == [
        "trt-gb10-1",
        "trt-gb10-2",
        "trt-gb10-3",
        "trt-gb10-4",
        "trt-gb10-5",
        "trt-gb10-6",
        "trt-gb10-7",
        "trt-gb10-8",
        "trt-gb10-9",
        "trt-gb10-11",
        "trt-gb10-12",
        "trt-gb10-13",
        "trt-gb10-14",
        "trt-gb10-15",
        "trt-gb10-16",
    ]
    assert gb10_policy["actuator_config"]["max_jobs"] == 15
    assert (
        gb10_policy["actuator_config"]["env_file"]
        == "/shared_work2/loom-staging-rollout/worker-envs/staging-gb10-worker-staging-test.env"
    )
    suffix = "loom-remote-worker-staging-test"
    assert gb10_policy["actuator_config"]["repo_dir"].endswith(suffix)

    gb10_state = next(
        state for state in profile.gb10_desired_states if state["pool_name"] == "gb10"
    )
    assert gb10_state["environment"] == environment
    assert gb10_state["image_tag"] == "staging-test"
    assert gb10_state["env_config_version"] == "staging-test"
    assert gb10_state["source_git_commit"] == ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    assert gb10_state["max_concurrent"] == 10
    assert gb10_state["target_slots"] == 0
    assert gb10_state["host_intents"]["trt-gb10-1"] == "stopped"
    assert gb10_state["host_intents"]["trt-gb10-14"] == "stopped"
    assert gb10_state["host_intents"]["trt-gb10-7"] == "stopped"
    assert set(gb10_state["host_intents"].values()) == {"stopped"}
    assert gb10_state["rollout_policy"] == {"mode": "all"}
    assert profile.catalog_provisioning["required"] is True
    command = profile.catalog_provisioning["command"]
    assert "loom datasets provision-catalog" not in command
    assert "loom datasets register skilllearnbench" in command
    assert "--mirror-to-object-store" in command
    assert "loom datasets publish-local deploy/catalog/gb10-smoke" in command
    assert "loom datasets audit --all --verify-bundles" in command
    assert "LOOM_CATALOG_SOURCE_" not in command
    assert (
        profile.catalog_provisioning["env_file"]
        == "/shared_work/qianyi/loom-worker-capacity/staging-catalog-provisioning.env"
    )
    assert profile.catalog_provisioning["env"] == {
        "PUBLISHED_SHA": "79087002d62bb22169a704bc941c8d614082d880",
    }
    assert profile.catalog_provisioning["kubernetes_port_forward"] == {
        "enabled": True,
        "postgres_service": "service/loom-postgres",
        "postgres_remote_port": 5432,
        "minio_service": "service/loom-minio",
        "minio_remote_port": 9000,
    }
    assert profile.catalog_provisioning["required_env"] == [
        "PUBLISHED_SHA",
        "HF_TOKEN",
        "LOOM_SVC_DB_URL",
        "LOOM_SVC_MINIO_ENDPOINT",
        "LOOM_SVC_MINIO_ACCESS_KEY",
        "LOOM_SVC_MINIO_SECRET_KEY",
    ]
    assert not {
        "LOOM_CATALOG_SOURCE_DB_URL",
        "LOOM_CATALOG_SOURCE_MINIO_ENDPOINT",
        "LOOM_CATALOG_SOURCE_MINIO_ACCESS_KEY",
        "LOOM_CATALOG_SOURCE_MINIO_SECRET_KEY",
    } & set(profile.catalog_provisioning["required_env"])
    assert set(profile.external_slurm_runner_prerequisites["pools"]) == {
        "gb10",
        "oldlab",
    }
    assert profile.external_slurm_runner_prerequisites["require_worker_token_parity"] is True
    assert profile.external_slurm_runner_prerequisites["materialize"] is True
    assert profile.hosted_provider_pricing_defaults == [
        {
            "name": "yibuapi-glm",
            "pricing_source": "rate-card",
            "rate_card_provider": "yibuapi",
            "required": True,
        }
    ]
    assert (
        profile.external_slurm_runner_prerequisites["require_external_allocation_authority"] is True
    )
    assert (
        profile.external_slurm_runner_prerequisites["env_template_glob"]
        == "/var/lib/loom-staging-rollout/generated/staging-gb10-worker-staging-*.env"
    )


def test_committed_development_profile_ships_fail_closed_supervisors() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/development.toml"),
        variables={
            "IMAGE_TAG": "development-test",
            "ENV_CONFIG_VERSION": "development-test",
        },
        expected_environment="development",
    )

    supervisors = {
        supervisor["name"]: supervisor
        for supervisor in profile.external_slurm_autoscaler_supervisors
    }
    assert set(supervisors) == {"oldlab-development", "gb10-development"}

    oldlab = supervisors["oldlab-development"]
    assert oldlab["pool_name"] == "oldlab"
    assert oldlab["service_name"] == "loom-autoscaler-oldlab-dev.service"
    assert oldlab["timer_name"] == "loom-autoscaler-oldlab-dev.timer"
    assert oldlab["enabled"] is False
    assert oldlab["active"] is False
    assert "15447" in oldlab["args"]

    gb10 = supervisors["gb10-development"]
    assert gb10["pool_name"] == "gb10"
    assert gb10["service_name"] == "loom-autoscaler-gb10-dev.service"
    assert gb10["timer_name"] == "loom-autoscaler-gb10-dev.timer"
    assert gb10["enabled"] is False
    assert gb10["active"] is False
    assert "15450" in gb10["args"]

    # Both dev supervisors are fail-closed: neither is enabled nor active.
    assert not any(
        supervisor["enabled"] or supervisor["active"]
        for supervisor in profile.external_slurm_autoscaler_supervisors
    )


def test_committed_staging_profile_activates_both_slurm_supervisors() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables={
            "IMAGE_TAG": "staging-test",
            "ENV_CONFIG_VERSION": "staging-test",
            "GIT_SHA": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        expected_environment="staging",
    )

    supervisors = profile.external_slurm_autoscaler_supervisors
    assert len(supervisors) == 2
    by_name = {supervisor["name"]: supervisor for supervisor in supervisors}
    gb10 = by_name["gb10-staging"]
    assert gb10["pool_name"] == "gb10"
    assert gb10["service_name"] == "loom-autoscaler-gb10-staging.service"
    assert gb10["timer_name"] == "loom-autoscaler-gb10-staging.timer"
    assert gb10["enabled"] is True
    assert gb10["active"] is True
    assert "15451" in gb10["args"]
    assert "loom-external-slurm-autoscaler-db" in gb10["args"]

    oldlab = by_name["oldlab-staging"]
    assert oldlab["pool_name"] == "oldlab"
    assert oldlab["service_name"] == "loom-autoscaler-oldlab-staging.service"
    assert oldlab["timer_name"] == "loom-autoscaler-oldlab-staging.timer"
    assert oldlab["enabled"] is True
    assert oldlab["active"] is True
    assert "15448" in oldlab["args"]
    assert "service/loom-postgres-rw" in oldlab["args"]
    assert oldlab["working_directory"].startswith("/opt/loom-staging-runner/candidates/")


def test_staging_profile_loader_rejects_candidate_self_attested_activation(
    tmp_path: Path,
) -> None:
    profile_text = Path("deploy/environment-state/staging.toml").read_text(encoding="utf-8")
    profile_path = tmp_path / "staging.toml"
    profile_path.write_text(
        profile_text.replace("enabled = false", "enabled = true", 1)
        + """

[external_slurm_runner_prerequisites.service_identity]
username = "loom-rollout"
uid = 995
gid = 982
supplementary_groups = ["docker"]
slurm_account = "loom-staging"
submit_host = "candidate-controlled.example"

[external_slurm_runner_prerequisites.allocation_attestation]
candidate_sha = "${GIT_SHA}"
artifact_path = "/candidate-controlled/attestation.json"
artifact_sha256 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
passed = true
nodes = ["trt-gb10-1"]
""",
        encoding="utf-8",
    )

    with pytest.raises(
        EnvironmentStateProfileError,
        match="candidate_external_slurm_self_attestation_forbidden",
    ):
        load_environment_state_profile(
            profile_path,
            variables={
                "IMAGE_TAG": "staging-test",
                "ENV_CONFIG_VERSION": "staging-test",
                "GIT_SHA": "a" * 40,
            },
            expected_environment="staging",
        )


@pytest.mark.parametrize(
    ("policy_enabled", "materialize", "supervisor_enabled"),
    [
        (True, False, False),
        (False, True, False),
        (False, False, True),
    ],
)
def test_staging_gb10_external_activation_requires_external_authority(
    policy_enabled: bool,
    materialize: bool,
    supervisor_enabled: bool,
) -> None:
    blockers = staging_gb10_external_activation_blockers(
        environment="staging",
        autoscaler_policies=[
            {
                "pool_name": "gb10",
                "actuator": "slurm",
                "enabled": policy_enabled,
                "disabled_reason": "gated" if not policy_enabled else None,
                "actuator_config": {
                    "external_runner": True,
                    "allowed_nodes": ["trt-gb10-1"],
                },
            }
        ],
        prerequisites={"pools": ["gb10"], "materialize": materialize},
        supervisors=[
            {
                "pool_name": "gb10",
                "enabled": supervisor_enabled,
                "active": supervisor_enabled,
            }
        ],
    )

    expected = {
        "external_slurm_allocation_authority_requirement_missing",
    }
    if not materialize:
        expected.add("external_slurm_gb10_materialization_required")
    if not supervisor_enabled:
        expected.add("external_slurm_gb10_supervisor_activation_incomplete")
    assert blockers == tuple(sorted(expected))


def test_staging_gb10_supervisor_activation_without_policy_requires_external_authority() -> None:
    blockers = staging_gb10_external_activation_blockers(
        environment="staging",
        autoscaler_policies=[],
        prerequisites={},
        supervisors=[{"pool_name": "gb10", "enabled": True, "active": True}],
    )

    assert blockers == (
        "external_slurm_allocation_authority_requirement_missing",
        "external_slurm_gb10_materialization_required",
    )


def test_staging_gb10_external_activation_rejects_candidate_self_attestation() -> None:
    blockers = staging_gb10_external_activation_blockers(
        environment="staging",
        autoscaler_policies=[
            {
                "pool_name": "gb10",
                "actuator": "slurm",
                "enabled": True,
                "actuator_config": {
                    "external_runner": True,
                    "allowed_nodes": ["trt-gb10-1"],
                },
            }
        ],
        prerequisites={
            "pools": ["gb10"],
            "materialize": True,
            "service_identity": {
                "username": "loom-rollout",
                "uid": 995,
                "gid": 982,
                "supplementary_groups": ["docker"],
                "slurm_account": "loom-staging",
                "submit_host": "gb10-submit.example",
            },
            "allocation_attestation": {
                "candidate_sha": "a" * 40,
                "artifact_path": "/var/lib/loom/attestations/gb10.json",
                "artifact_sha256": "b" * 64,
                "passed": True,
                "nodes": ["trt-gb10-1"],
            },
        },
        supervisors=[{"pool_name": "gb10", "enabled": True, "active": True}],
    )

    assert blockers == (
        "candidate_external_slurm_self_attestation_forbidden",
        "external_slurm_allocation_authority_requirement_missing",
    )


def _supervisor_profile_payload(*, second_service: str, second_port: str) -> str:
    return (
        f"""
environment = "development"

[[external_slurm_autoscaler_supervisors]]
name = "oldlab-development"
pool_name = "oldlab"
service_name = "loom-autoscaler-oldlab-dev.service"
timer_name = "loom-autoscaler-oldlab-dev.timer"
working_directory = "/opt/loom-development-runner/repo"
python_path = "/opt/loom-development-runner/venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "development", "--pool-name", "oldlab", "--db-local-port", "15447"]
enabled = false
active = false

[[external_slurm_autoscaler_supervisors]]
name = "gb10-development"
pool_name = "gb10"
service_name = "{second_service}"
timer_name = "loom-autoscaler-gb10-dev.timer"
working_directory = "/opt/loom-development-runner/repo"
python_path = "/opt/loom-development-runner/venv/bin/python"
script_path = "scripts/ops/worker_pool_autoscaler_external_once.py"
args = ["--environment", "development", "--pool-name", "gb10", "--db-local-port", "{second_port}"]
enabled = false
active = false
""".strip()
        + "\n"
    )


def test_supervisor_collision_rejects_duplicate_service_name(tmp_path: Path) -> None:
    profile_path = tmp_path / "development.state.toml"
    profile_path.write_text(
        _supervisor_profile_payload(
            second_service="loom-autoscaler-oldlab-dev.service",
            second_port="15450",
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        EnvironmentStateProfileError,
        match=r"duplicate service_name 'loom-autoscaler-oldlab-dev.service'",
    ):
        load_environment_state_profile(profile_path)


def test_supervisor_collision_rejects_duplicate_db_local_port(tmp_path: Path) -> None:
    profile_path = tmp_path / "development.state.toml"
    profile_path.write_text(
        _supervisor_profile_payload(
            second_service="loom-autoscaler-gb10-dev.service",
            second_port="15447",
        ),
        encoding="utf-8",
    )

    with pytest.raises(
        EnvironmentStateProfileError,
        match=r"duplicate --db-local-port '15447'",
    ):
        load_environment_state_profile(profile_path)


def test_supervisor_collision_rejects_duplicate_pool_authority(tmp_path: Path) -> None:
    profile_path = tmp_path / "development.state.toml"
    payload = _supervisor_profile_payload(
        second_service="loom-autoscaler-gb10-dev.service",
        second_port="15450",
    )
    payload = payload.replace('pool_name = "gb10"', 'pool_name = "oldlab"', 1)
    payload = payload.replace('"--pool-name", "gb10"', '"--pool-name", "oldlab"', 1)
    profile_path.write_text(payload, encoding="utf-8")

    with pytest.raises(
        EnvironmentStateProfileError,
        match=r"duplicate pool_name 'oldlab'",
    ):
        load_environment_state_profile(profile_path)


def test_render_supervisor_service_and_timer_contain_full_execstart() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables={
            "IMAGE_TAG": "staging-test",
            "ENV_CONFIG_VERSION": "staging-test",
            "GIT_SHA": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        expected_environment="staging",
    )
    supervisor = profile.external_slurm_autoscaler_supervisors[0]

    service_unit = render_external_slurm_autoscaler_service(supervisor)
    assert "ExecStart=" in service_unit
    assert "worker_pool_autoscaler_external_once.py" in service_unit
    assert "--environment staging" in service_unit
    assert "--pool-name gb10" in service_unit
    assert "--namespace loom-staging" in service_unit
    assert "--db-local-port 15451" in service_unit
    runtime = "/opt/loom-staging-runner/candidates/" + "a" * 40
    assert f"WorkingDirectory={runtime}/repo" in service_unit
    assert f"Environment=PYTHONPATH={runtime}/repo/src" in service_unit
    assert "Environment=PYTHONDONTWRITEBYTECODE=1" in service_unit
    assert f"ExecStart={runtime}/venv/bin/python {runtime}/repo/" in service_unit

    timer_unit = render_external_slurm_autoscaler_timer(supervisor)
    assert "Unit=loom-autoscaler-gb10-staging.service" in timer_unit
    assert "OnUnitActiveSec=30" in timer_unit


def test_staging_profile_scales_both_slurm_pools_from_zero() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables={
            "IMAGE_TAG": "staging-test",
            "ENV_CONFIG_VERSION": "staging-test",
            "GIT_SHA": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        expected_environment="staging",
    )

    policies = {policy["pool_name"]: policy for policy in profile.autoscaler_policies}
    assert set(policies) == {"gb10", "oldlab"}
    gb10 = policies["gb10"]
    assert gb10["enabled"] is True
    assert gb10["min_slots"] == 0
    assert gb10["max_slots"] == 150
    assert gb10["actuator_config"]["candidate_sha"] == "a" * 40
    assert gb10["actuator_config"]["slurm_account"] == "loom-staging"
    assert gb10["actuator_config"]["qos_normal"] == "loom-staging"
    oldlab = policies["oldlab"]
    assert oldlab["enabled"] is True
    assert oldlab["min_slots"] == 0
    assert oldlab["max_slots"] == 18
    assert oldlab["actuator_config"]["exclusive"] is False
    assert oldlab["actuator_config"]["allowed_nodes"] == [
        "trt-eai-oldlab-3",
        "trt-eai-oldlab-4",
        "trt-eai-oldlab-5",
    ]
    assert oldlab["actuator_config"]["repo_dir"].startswith(
        "/shared_work/loom/staging-rollout/worker-repos/"
    )
    assert oldlab["actuator_config"]["env_file"].startswith(
        "/shared_work/loom/staging-rollout/worker-envs/"
    )
    assert oldlab["actuator_config"]["env_file"] == (
        "/shared_work/loom/staging-rollout/worker-envs/staging-oldlab-worker-staging-test.env"
    )
    assert oldlab["actuator_config"]["repo_dir"] == (
        "/shared_work/loom/staging-rollout/worker-repos/loom-remote-worker-staging-test"
    )
    assert oldlab["actuator_config"]["candidate_sha"] == "a" * 40
    assert oldlab["actuator_config"]["job_output_dir"] == (
        "/shared_work/loom/staging-rollout/job-output"
    )
    assert oldlab["actuator_config"]["resource_aware"] is True

    gb10_state = next(
        state for state in profile.gb10_desired_states if state["pool_name"] == "gb10"
    )
    assert len(gb10_state["host_intents"]) == 15
    assert gb10_state["target_slots"] == 0
    assert gb10_state["host_intents"]["trt-gb10-7"] == "stopped"
    assert sum(intent == "stopped" for intent in gb10_state["host_intents"].values()) == 15
