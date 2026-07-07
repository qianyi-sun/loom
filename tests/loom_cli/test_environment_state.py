from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pytest

from loom_cli.environment_state import (
    EnvironmentStateProfileError,
    apply_external_slurm_autoscaler_supervisors,
    diff_environment_state,
    diff_external_slurm_autoscaler_supervisors,
    diff_external_slurm_runner_prerequisites,
    load_environment_state_profile,
    render_external_slurm_autoscaler_service,
    render_external_slurm_autoscaler_timer,
)


def _write_profile(path: Path) -> None:
    path.write_text(
        """
environment = "staging"

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
        + "\n",
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
                    "environment": "staging",
                    "pool_name": "gb10-arm64",
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
        "worker_pool_autoscaler_policies[staging/gb10-arm64].actuator",
        "worker_pool_autoscaler_policies[staging/gb10-arm64].actuator_config",
        "gb10_worker_pool_desired_states[staging/gb10-arm64].image_tag",
        "gb10_worker_pool_desired_states[staging/gb10-arm64].env_config_version",
        "gb10_worker_pool_desired_states[staging/gb10-arm64].source_git_commit",
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
            ],
        },
        "gb10_status": {
            "desired_states": [
                {
                    "environment": "staging",
                    "pool_name": "gb10-arm64",
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
                    "pool_name": "gb10-arm64",
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
        "gb10_worker_node_status[staging/gb10-arm64/trt-gb10-1].source_git_commit"
    )
    assert source_drift[0].desired == "c72f50d67f0d571fef55a9abbbced4e37752ca0e"
    assert source_drift[0].live == "ce55a358d8472bce4b580a363806993678d8f116"


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
    )

    drift = diff_environment_state(profile, live)
    source_related = [item for item in drift if "source_git" in item.path]
    assert source_related == []


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

    assert drift[0].path == "worker_pool_autoscaler_policies[staging/gb10-arm64]"
    assert drift[0].live is None
    assert drift[1].path == "gb10_worker_pool_desired_states[staging/gb10-arm64]"
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
args = ["--pool-name", "oldlab", "--namespace", "loom-staging"]
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
            "name": "oldlab",
            "pool_name": "oldlab",
            "service_name": "loom-oldlab-autoscaler.service",
            "timer_name": "loom-oldlab-autoscaler.timer",
            "working_directory": "/home/qianyi/dev/loom-worktrees/staging-052e420",
            "python_path": ("/home/qianyi/dev/loom-worktrees/staging-052e420/.venv/bin/python"),
            "script_path": (
                "/home/qianyi/dev/loom-worktrees/staging-052e420/"
                "scripts/ops/worker_pool_autoscaler_external_once.py"
            ),
            "args": ["--pool-name", "oldlab", "--namespace", "loom-staging"],
            "requires": [
                "network-online.target",
                "loom-staging-postgres-port-forward.service",
            ],
            "timer_on_boot_sec": "45",
            "timer_on_unit_active_sec": "30",
            "timer_accuracy_sec": "5",
            "enabled": True,
            "active": True,
        },
    ]


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
args = ["--pool-name", "oldlab", "--namespace", "loom-staging"]
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
args = ["--pool-name", "oldlab", "--namespace", "loom-staging"]
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
args = ["--pool-name", "oldlab"]
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
args = ["--pool-name", "oldlab", "--namespace", "loom-staging"]
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
args = ["--pool-name", "oldlab"]
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
args = ["--pool-name", "oldlab"]
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
args = ["--pool-name", "oldlab"]
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


def test_external_slurm_autoscaler_supervisor_apply_idempotent_when_unit_missing(
    tmp_path: Path,
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
args = ["--pool-name", "oldlab"]
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
        # daemon-reload succeeds; stop/disable return 5 (not loaded).
        if command[-1] == "daemon-reload":
            return 0, "", ""
        return (
            5,
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
        policy for policy in profile.autoscaler_policies if policy["pool_name"] == "gb10-arm64"
    )
    assert profile.environment == environment
    assert profile.control_plane_environment == environment
    assert gb10_policy["environment"] == environment
    assert gb10_policy["actuator"] == "slurm"
    assert gb10_policy["max_slots"] == 150
    assert gb10_policy["actuator_config"]["backend"] == "docker"
    assert gb10_policy["actuator_config"]["cpu_arch"] == "arm64"
    assert gb10_policy["actuator_config"]["partition"] == "gb10"
    assert len(gb10_policy["actuator_config"]["allowed_nodes"]) == 15
    assert (
        gb10_policy["actuator_config"]["env_file"]
        == f"/shared_work/qianyi/loom-worker-capacity/{environment}-gb10-worker-staging-test.env"
    )
    suffix = "loom-remote-worker-staging-test"
    assert gb10_policy["actuator_config"]["repo_dir"].endswith(suffix)

    gb10_state = next(
        state for state in profile.gb10_desired_states if state["pool_name"] == "gb10-arm64"
    )
    assert gb10_state["environment"] == environment
    assert gb10_state["image_tag"] == "staging-test"
    assert gb10_state["env_config_version"] == "staging-test"
    assert gb10_state["source_git_commit"] == ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    assert gb10_state["max_concurrent"] == 10
    assert gb10_state["target_slots"] == 150
    assert gb10_state["host_intents"]["trt-gb10-1"] == "active"
    assert gb10_state["host_intents"]["trt-gb10-14"] == "active"
    assert set(gb10_state["host_intents"].values()) == {"active"}
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
    assert set(profile.external_slurm_runner_prerequisites["pools"]) == {"gb10-arm64"}
    assert profile.external_slurm_runner_prerequisites["require_worker_token_parity"] is True
    assert profile.external_slurm_runner_prerequisites["materialize"] is True
    assert (
        profile.external_slurm_runner_prerequisites["env_template_glob"]
        == "/shared_work/qianyi/loom-worker-capacity/staging-gb10-worker-staging-*.env"
    )


def test_staging_profile_is_gb10_only_for_first_prod_validation() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables={
            "IMAGE_TAG": "staging-test",
            "ENV_CONFIG_VERSION": "staging-test",
            "GIT_SHA": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        },
        expected_environment="staging",
    )

    pool_names = {policy["pool_name"] for policy in profile.autoscaler_policies}
    assert pool_names == {"gb10-arm64"}

    gb10_state = next(
        state for state in profile.gb10_desired_states if state["pool_name"] == "gb10-arm64"
    )
    assert len(gb10_state["host_intents"]) == 15
    assert set(gb10_state["host_intents"].values()) == {"active"}
