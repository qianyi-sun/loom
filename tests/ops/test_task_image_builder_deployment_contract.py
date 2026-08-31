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


def test_staging_activates_both_provisioned_native_builders() -> None:
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
        assert row["exclusive"] is True
        assert row["requested_concurrency"] == 1
        assert row["max_jobs"] > 0
        assert row["failure_backoff_seconds"] == 300
        assert "builder_token_not_provisioned" not in row["activation_blockers"]
        assert "registry_push_credentials_not_provisioned" not in row["activation_blockers"]
        assert "registry_retention_not_provisioned" not in row["activation_blockers"]
    assert builders["x86_64"]["enabled"] is True
    assert builders["x86_64"]["activation_blockers"] == []
    assert builders["x86_64"]["allowed_nodes"] == ["trt-eai-oldlab-6"]
    assert builders["arm64"]["enabled"] is True
    assert builders["arm64"]["allowed_nodes"] == ["trt-gb10-2"]
    assert builders["arm64"]["activation_blockers"] == []

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


def test_staging_gb10_builder_fits_the_slurm_schedulable_node_boundary() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables=_VARIABLES,
        expected_environment="staging",
    )

    builder = next(
        row
        for row in profile.task_image_builder_policies
        if row["pool_name"] == "task-image-builder-gb10"
    )

    # GB10 reserves one of each node's 20 CPUs for Slurm and advertises
    # CfgTRES=cpu=19,mem=110000M. A larger test-only request is rejected before
    # the builder autoscaler can reconcile any queued materialization.
    assert builder["requested_cpus"] == 19
    assert builder["requested_memory_mib"] == 110_000


def test_staging_builder_supervisors_match_native_activation() -> None:
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
        assert row["script_path"].endswith(
            "/scripts/ops/task_image_builder_autoscaler_external_once.py"
        )
        args = builder_parser().parse_args(row["args"])
        assert args.global_execution_witness_json is None
        assert args.manager_public_key is None
        assert args.expected_manager_public_key_sha256_file is None
        assert args.global_execution_witness_config_map == ("loom-global-execution-witness-v1")
        assert args.global_execution_witness_namespace == "loom-dev"
        assert args.global_execution_witness_kubeconfig == (
            "/var/lib/loom-staging-rollout/external-supervisor.kubeconfig"
        )
        assert args.global_execution_manager_export is None
        assert args.global_execution_manager_namespace is None
        assert args.global_execution_manager_kubeconfig is None
        assert args.kubeconfig == ("/var/lib/loom-staging-rollout/external-supervisor.kubeconfig")
        assert args.expected_manager_public_key_sha256 == (
            "54b44788af0dc10dc5f0a8277396a35fedf2f143b39e14d5ee35ce09b56b18cd"
        )
    assert supervisors["task-image-builder-oldlab"]["enabled"] is True
    assert supervisors["task-image-builder-oldlab"]["active"] is True
    assert supervisors["task-image-builder-gb10"]["enabled"] is True
    assert supervisors["task-image-builder-gb10"]["active"] is True


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


def test_enabled_builder_policy_requires_exclusive_capacity_reservation() -> None:
    profile = load_environment_state_profile(
        Path("deploy/environment-state/staging.toml"),
        variables=_VARIABLES,
        expected_environment="staging",
    )
    base = {
        **profile.task_image_builder_policies[0],
        "enabled": True,
        "activation_blockers": [],
        "slurm_reservation": "",
    }

    with pytest.raises(EnvironmentStateProfileError, match="slurm_reservation"):
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


def test_release_manifest_preserves_dual_arch_builder_activation() -> None:
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
    by_arch = {row["cpu_arch"]: row for row in builders}
    assert by_arch["x86_64"]["enabled"] is True
    assert by_arch["x86_64"]["activation_blockers"] == []
    assert by_arch["arm64"]["enabled"] is True
    assert by_arch["arm64"]["activation_blockers"] == []
    assert all(row["exclusive"] is True for row in builders)


def test_phase2_boundary_does_not_block_native_phase1_acceptance_rerun() -> None:
    runbook = Path(
        "docs/runbooks/task-image-builder-phase1-site-convergence.md"
    ).read_text(encoding="utf-8")
    phase2_boundary = " ".join(
        runbook.split("## Closed Phase 2 boundary", maxsplit=1)[1].split()
    )

    assert "do not activate a rootless provider, policy, or supervisor" in phase2_boundary
    assert "does not block the active native Phase 1 builder" in phase2_boundary
    assert "rerun task `4139e767`" in phase2_boundary
    assert "must still wait for every active Phase 1 convergence gate above" in phase2_boundary


def test_phase1_runbook_removes_complete_or_absent_transition_exec_set() -> None:
    runbook = Path(
        "docs/runbooks/task-image-builder-phase1-site-convergence.md"
    ).read_text(encoding="utf-8")
    normalized = " ".join(runbook.split())

    assert (
        'TRANSITION_WITNESS_EXEC_NAME="loom-external-slurm-autoscaler-manager-export"'
        in runbook
    )
    assert (
        'test "$TRANSITION_PRESENT_COUNT" -eq 0 || '
        'test "$TRANSITION_PRESENT_COUNT" -eq 4'
    ) in normalized
    assert 'delete rolebinding "$TRANSITION_WITNESS_EXEC_NAME"' in normalized
    assert 'delete role "$TRANSITION_WITNESS_EXEC_NAME"' in normalized
    assert (
        'delete validatingadmissionpolicybinding "$TRANSITION_WITNESS_EXEC_NAME"'
        in normalized
    )
    assert (
        'delete validatingadmissionpolicy "$TRANSITION_WITNESS_EXEC_NAME"'
        in normalized
    )
    assert 'error("unexpected pods/exec Role remains")' in normalized
