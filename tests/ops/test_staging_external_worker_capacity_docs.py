from __future__ import annotations

import time
import tomllib
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from loom_control_plane.elastic_slurm_worker_controller import (
    SlurmNodeResource,
    compute_node_capacity_plan,
    slurm_submission_config_for_node,
)
from loom_control_plane.worker_pool_autoscaler import _slurm_config_from_policy

ROOT = Path(__file__).resolve().parents[2]


def _toml(path: str) -> dict[str, Any]:
    return tomllib.loads((ROOT / path).read_text(encoding="utf-8"))


def test_staging_profiles_use_external_worker_pools() -> None:
    for path in (
        "deploy/environments/staging.cluster.toml",
        "deploy/environments/staging.multinode.cluster.toml",
    ):
        profile = _toml(path)
        assert profile["k8s_worker"]["enabled"] is False


def test_staging_external_pool_ceilings_match_current_policy() -> None:
    profile = _toml("deploy/environment-state/staging.toml")
    policies = {item["pool_name"]: item for item in profile["worker_pool_autoscaler_policies"]}

    assert set(policies) == {"gb10", "oldlab"}
    assert policies["gb10"]["enabled"] is True
    assert policies["gb10"]["min_slots"] == 0
    assert policies["gb10"]["max_slots"] == 140
    assert policies["gb10"]["actuator_config"]["partition"] == "loom-staging"
    assert policies["oldlab"]["enabled"] is True
    assert policies["oldlab"]["min_slots"] == 0
    assert policies["oldlab"]["max_slots"] == 18
    for policy in policies.values():
        assert policy["actuator_config"]["external_runner"] is True
        assert policy["actuator_config"]["exclusive"] is False


def test_gb10_policy_keeps_slurm_reserved_cpu_out_of_worker_allocations() -> None:
    profile = _toml("deploy/environment-state/staging.toml")
    policy = next(
        item for item in profile["worker_pool_autoscaler_policies"] if item["pool_name"] == "gb10"
    )
    actuator_config = dict(policy["actuator_config"])
    actuator_config["candidate_sha"] = "a" * 40
    config = _slurm_config_from_policy(
        SimpleNamespace(
            environment="staging",
            pool_name="gb10",
            max_slots=policy["max_slots"],
            actuator_config=actuator_config,
        )
    )
    for node in config.allowed_nodes:
        resource = SlurmNodeResource(
            hostname=node,
            state="mixed",
            cpus_total=20,
            free_memory_mib=78_000,
            cpu_load=3.1,
            idle_cpus=4,
            available_memory_mib=78_000,
            total_memory_mib=110_000,
            schedulable_memory_mib=78_000,
            memory_observed_at=time.monotonic(),
        )

        plan = compute_node_capacity_plan(
            config,
            node=node,
            resource=resource,
            active_nodes=set(),
        )
        submission = slurm_submission_config_for_node(
            config,
            SimpleNamespace(node_capacity={node: plan}),
            node=node,
        )

        assert plan.safe_slots == 1
        assert submission.requested_concurrency == 1
        assert submission.requested_cpus == 2


def test_staging_validation_uses_current_external_pool_evidence() -> None:
    runbook = (ROOT / "docs/runbooks/staging-launch.md").read_text(encoding="utf-8")

    assert "--required-worker-pool gb10" in runbook
    assert "external worker pools" in runbook
    assert "worker_capacity_manifest.py status" in runbook


@pytest.mark.parametrize("memory_evidence", ["missing", "stale"])
def test_gb10_policy_requires_fresh_memory_before_admitting_cpu_budget(
    memory_evidence: str,
) -> None:
    profile = _toml("deploy/environment-state/staging.toml")
    policy = next(
        item for item in profile["worker_pool_autoscaler_policies"] if item["pool_name"] == "gb10"
    )
    actuator_config = {**policy["actuator_config"], "candidate_sha": "a" * 40}
    config = _slurm_config_from_policy(
        SimpleNamespace(
            environment="staging",
            pool_name="gb10",
            max_slots=policy["max_slots"],
            actuator_config=actuator_config,
        )
    )
    assert config.probe_mem_available is True
    for node in config.allowed_nodes:
        resource = SlurmNodeResource(
            hostname=node,
            state="mixed",
            cpus_total=20,
            free_memory_mib=78_000,
            cpu_load=3.1,
            idle_cpus=4,
            available_memory_mib=78_000,
            total_memory_mib=110_000,
            schedulable_memory_mib=78_000,
            memory_observed_at=None if memory_evidence == "missing" else time.monotonic() - 60,
        )
        plan = compute_node_capacity_plan(config, node=node, resource=resource, active_nodes=set())
        assert plan.safe_slots == 0
        assert plan.reason == "missing_fresh_memory_probe"
