from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy" / "environment-state" / "staging.toml"
_README = _ROOT / "deploy" / "worker-pools" / "oldlab" / "README.md"
_EXPECTED_NODES = (
    "trt-eai-oldlab-3",
    "trt-eai-oldlab-4",
    "trt-eai-oldlab-5",
)


def _profile() -> dict[str, Any]:
    return tomllib.loads(_PROFILE.read_text(encoding="utf-8"))


def _policy() -> dict[str, Any]:
    return next(
        item
        for item in _profile()["worker_pool_autoscaler_policies"]
        if item["pool_name"] == "oldlab"
    )


def _supervisor() -> dict[str, Any]:
    return next(
        item
        for item in _profile()["external_slurm_autoscaler_supervisors"]
        if item["pool_name"] == "oldlab"
    )


def test_oldlab_current_policy_uses_the_declared_staging_nodes() -> None:
    policy = _policy()
    actuator = policy["actuator_config"]

    assert policy["enabled"] is True
    assert policy["actuator"] == "slurm"
    assert tuple(actuator["allowed_nodes"]) == _EXPECTED_NODES
    assert actuator["partition"] == "loom-staging"
    assert actuator["time_limit"] == "2-00:00:00"
    assert policy["min_slots"] == 0
    assert policy["max_slots"] == 18


def test_oldlab_current_policy_defines_nonexclusive_container_caps() -> None:
    actuator = _policy()["actuator_config"]

    assert actuator["requested_cpus"] == 12
    assert actuator["requested_memory_mib"] == 49152
    assert actuator["requested_concurrency"] == 6
    assert actuator["container_cpus"] == 2.0
    assert actuator["container_memory_mib"] == 4096
    assert actuator["container_pids"] == 512
    assert actuator["exclusive"] is False
    assert actuator["external_runner"] is True


def test_oldlab_current_policy_opts_into_cache_aware_memory_admission_only() -> None:
    policies = {
        item["pool_name"]: item["actuator_config"]
        for item in _profile()["worker_pool_autoscaler_policies"]
    }

    assert policies["oldlab"]["probe_mem_available"] is True
    assert policies["gb10"].get("probe_mem_available", False) is False


def test_oldlab_current_policy_uses_candidate_bound_shared_paths() -> None:
    actuator = _policy()["actuator_config"]

    assert actuator["env_file"].startswith("/shared_work/loom/staging-rollout/")
    assert actuator["repo_dir"].startswith("/shared_work/loom/staging-rollout/")
    assert actuator["candidate_sha"] == "${GIT_SHA}"
    assert actuator["job_output_dir"].startswith("/shared_work/loom/staging-rollout/")


def test_oldlab_bootstrap_supervisor_is_disabled_and_inactive() -> None:
    supervisor = _supervisor()

    assert supervisor["name"] == "oldlab-staging"
    assert supervisor["execution_host"] == "TRT-EAI-OLDLAB-1"
    assert supervisor["service_name"] == "loom-autoscaler-oldlab-staging.service"
    assert supervisor["timer_name"] == "loom-autoscaler-oldlab-staging.timer"
    assert supervisor["enabled"] is False
    assert supervisor["active"] is False
    assert supervisor["args"][:4] == [
        "--environment",
        "staging",
        "--pool-name",
        "oldlab",
    ]


def test_oldlab_readme_matches_current_policy_authority() -> None:
    readme = _README.read_text(encoding="utf-8")

    assert "authoritative policy" in readme
    assert "deploy/environment-state/staging.toml" in readme
    assert "scales from zero to 18 slots" in readme
    for node in _EXPECTED_NODES:
        assert node in readme
    assert "dedicated `loom-staging` Slurm partition" in readme
    assert "`PriorityTier=100`" in readme
    assert "never cancels or preempts foreign jobs" in readme
    assert "loom-autoscaler-oldlab-staging.timer" in readme
    assert "/shared_work/loom/staging-rollout/" in readme
