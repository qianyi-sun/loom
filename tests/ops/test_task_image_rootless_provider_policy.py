from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from loom_cli.task_image_rootless_provider_policy import (
    TaskImageRootlessProviderPolicyError,
    load_task_image_rootless_provider_policy,
)

_POLICY_PATH = Path("deploy/task-image-builder/rootless-provider-v1.toml")
_PREREQUISITES_PATH = Path("deploy/task-image-builder/prerequisites-v1.toml")


def test_checked_in_policy_loads_exactly_two_disabled_native_providers() -> None:
    policies = load_task_image_rootless_provider_policy(_POLICY_PATH)

    assert len(policies) == 2
    by_cluster = {policy.slurm_cluster_id: policy for policy in policies}
    assert set(by_cluster) == {"oldlab", "gb10"}
    assert (by_cluster["oldlab"].cpu_arch, by_cluster["oldlab"].qos) == (
        "x86_64",
        "loom-task-image-builder-rootless-oldlab",
    )
    assert (by_cluster["gb10"].cpu_arch, by_cluster["gb10"].qos) == (
        "arm64",
        "loom-task-image-builder-rootless-gb10",
    )
    for policy in policies:
        assert policy.enabled is False
        assert policy.activation_blockers
        assert (policy.partition, policy.account, policy.submitting_identity) == (
            "loom-task-builder",
            "loom-task-builder",
            "loom-builder",
        )
        assert policy.feature_constraint == "loom_rootless_buildkit"
        assert policy.resources.model_dump() == {
            "cpus": 8,
            "memory_mib": 32768,
            "pids": 4096,
            "scratch_bytes": 107374182400,
            "scratch_inodes": 1000000,
            "wall_time": "02:00:00",
            "swap_bytes": 0,
        }


def test_provider_resources_match_the_rootless_prerequisite_profile() -> None:
    policies = load_task_image_rootless_provider_policy(_POLICY_PATH)
    prerequisites = tomllib.loads(_PREREQUISITES_PATH.read_text(encoding="utf-8"))
    resource_profile = prerequisites["resource_profile"]
    expected = {
        field: resource_profile[field]
        for field in (
            "cpus",
            "memory_mib",
            "pids",
            "scratch_bytes",
            "scratch_inodes",
            "wall_time",
            "swap_bytes",
        )
    }

    assert all(policy.resources.model_dump() == expected for policy in policies)


def _write_mutated_policy(tmp_path: Path, old: str, new: str) -> Path:
    policy = _POLICY_PATH.read_text(encoding="utf-8")
    assert old in policy
    path = tmp_path / "rootless-provider-v1.toml"
    path.write_text(policy.replace(old, new, 1), encoding="utf-8")
    return path


def test_enabled_policy_with_an_activation_blocker_is_rejected(tmp_path: Path) -> None:
    path = _write_mutated_policy(tmp_path, "enabled = false", "enabled = true")

    with pytest.raises(TaskImageRootlessProviderPolicyError, match="activation blockers"):
        load_task_image_rootless_provider_policy(path)


def test_disabled_policy_without_activation_blockers_is_rejected(tmp_path: Path) -> None:
    blockers = """activation_blockers = [
  \"allocation_executor_not_accepted\",
  \"node_guard_not_accepted\",
  \"publication_acceptance_not_complete\",
  \"renewable_registry_credential_broker_not_accepted\",
]"""
    path = _write_mutated_policy(tmp_path, blockers, "activation_blockers = []")

    with pytest.raises(TaskImageRootlessProviderPolicyError, match="retain activation blockers"):
        load_task_image_rootless_provider_policy(path)


@pytest.mark.parametrize(
    "forbidden_key",
    [
        "reservation",
        "allowed_nodes",
        "nodelist",
        "exclusive",
        "docker_socket",
        "registry_credentials",
        "builder_token",
    ],
)
def test_legacy_allocation_or_secret_authority_is_rejected(
    tmp_path: Path,
    forbidden_key: str,
) -> None:
    path = _write_mutated_policy(
        tmp_path,
        "enabled = false",
        f'enabled = false\n{forbidden_key} = "forbidden"',
    )

    with pytest.raises(TaskImageRootlessProviderPolicyError, match=forbidden_key):
        load_task_image_rootless_provider_policy(path)


def test_policy_file_rejects_unknown_top_level_composition(tmp_path: Path) -> None:
    path = _write_mutated_policy(
        tmp_path,
        'schema = "loom.task-image-rootless-provider-policies/v1"',
        'schema = "loom.task-image-rootless-provider-policies/v1"\nservice = "active"',
    )

    with pytest.raises(TaskImageRootlessProviderPolicyError, match="service"):
        load_task_image_rootless_provider_policy(path)
