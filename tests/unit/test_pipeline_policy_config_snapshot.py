from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest

from loom.db.schema import PipelineScopedPolicyActivation, WorkerPoolAutoscalerPolicy
from loom.pipeline.image_runtime import ImageRuntimeRecord, ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest
from loom.pipeline.policy_config import (
    PipelineScopedPolicyActivationV1,
    PolicyConfigRegistry,
)
from loom.pipeline.resource_profiles import load_resource_profiles
from loom.pipeline.work_protocol import ImageRuntimeContractV1
from loom_control_plane.worker_pool_autoscaler import (
    _apply_pipeline_scoped_activation,
    _policy_to_config,
)


def test_exact_three_policy_snapshots_are_stable_and_disabled_separately() -> None:
    registry = PolicyConfigRegistry.load(resource_profiles=load_resource_profiles())
    records = registry.list()
    assert [record.snapshot.policy_id for record in records] == [
        "behavior-cpu-data",
        "behavior-gpu-gb10",
        "behavior-gpu-oldlab",
    ]
    rows = registry.disabled_autoscaler_rows(environment="staging")
    assert len(rows) == 3
    assert all(row["enabled"] is False and row["min_slots"] == 0 for row in rows)
    assert all("enabled" not in record.snapshot.model_fields_set for record in records)


def test_gpu_clusters_and_digests_are_disjoint_while_host7_remains_inventory() -> None:
    registry = PolicyConfigRegistry.load(resource_profiles=load_resource_profiles())
    gb10 = registry.get("behavior-gpu-gb10")
    oldlab = registry.get("behavior-gpu-oldlab")
    assert gb10.snapshot.slurm_cluster_id == "gb10"
    assert oldlab.snapshot.slurm_cluster_id == "oldlab"
    assert gb10.snapshot.slurm_cluster_config_sha256 != oldlab.snapshot.slurm_cluster_config_sha256
    assert "trt-gb10-7" in gb10.snapshot.allowed_nodes
    assert set(gb10.snapshot.allowed_nodes).isdisjoint(oldlab.snapshot.allowed_nodes)
    first_digest = gb10.policy_config_sha256
    assert PolicyConfigRegistry.load(resource_profiles=load_resource_profiles()).get("behavior-gpu-gb10").policy_config_sha256 == first_digest


def test_gb10_checked_in_surface_is_complete_quarantined_and_disabled() -> None:
    plan = Path("deploy/worker-pools/gb10/worker-plan.csv").read_text()
    assert len(plan.splitlines()) == 16
    assert "trt-gb10-7,gb10,gpu:gb10:1,drain,false" in plan
    gres = Path("deploy/worker-pools/gb10/slurm/gres.conf").read_text()
    assert len(gres.splitlines()) == 15
    assert all("Name=gpu Type=gb10 File=/dev/nvidia0" in line for line in gres.splitlines())
    slurm = Path("deploy/worker-pools/gb10/slurm/slurm.conf").read_text()
    assert "NodeName=trt-gb10-7 CPUs=20 RealMemory=120000 Gres=gpu:gb10:1 State=DOWN" in slurm
    controller = Path("deploy/worker-pools/gb10/controller.env.example").read_text()
    assert "LOOM_DIRECT_WORKER_ENABLED=false" in controller
    assert "LOOM_AUTOSCALER_ENABLED=false" in controller
    assert "LOOM_AUTOSCALER_DESIRED_SLOTS=0" in controller


def test_driver_constraints_are_derived_from_attested_platform_contracts() -> None:
    image = "registry.example.com/loom/sim@sha256:" + "a" * 64
    records: dict[tuple[str, str], ImageRuntimeRecord] = {}
    for platform, arch, marker in (
        ("linux/amd64", "x86_64", "b"),
        ("linux/arm64", "arm64", "c"),
    ):
        digest = "sha256:" + marker * 64
        contract = ImageRuntimeContractV1.model_validate(
            {
                "image_index_digest": image,
                "platform": platform,
                "platform_manifest_digest": digest,
                "cpu_arch": arch,
                "gpu_vendor": "nvidia",
                "cuda_userspace_version": "13.0",
                "min_nvidia_driver_version": "580.12.0",
                "application_features": ["isaac-sim-5.1", "omnigibson-3.8"],
                "provider_assets": [],
                "preflight_argv": ["/opt/loom/preflight"],
                "preflight_digest": digest,
                "sbom_digest": digest,
                "attestation_digest": digest,
            }
        )
        records[(image, platform)] = ImageRuntimeRecord(
            contract=contract,
            snapshot_sha256=canonical_digest(contract),
        )
    registry = PolicyConfigRegistry.load(
        resource_profiles=load_resource_profiles(),
        image_runtime_contracts=ImageRuntimeRegistry(records),
    )
    assert registry.get(
        "behavior-gpu-gb10"
    ).snapshot.driver_constraints.minimum_versions == ["580.12.0"]
    assert registry.get(
        "behavior-gpu-oldlab"
    ).snapshot.driver_constraints.minimum_versions == ["580.12.0"]


def test_scoped_activation_is_authority_bound_and_fail_closed() -> None:
    value = {
        "schema_version": "loom.pipeline-scoped-policy-activation.v1",
        "environment": "staging",
        "policy_id": "behavior-cpu-data",
        "policy_config_sha256": "sha256:" + "a" * 64,
        "authority_kind": "acceptance",
        "authority_id": UUID("9e8174fa-7ad2-4386-869b-aadcfcc2cfa6"),
        "activation_epoch": 3,
        "state": "active",
        "desired_slots": 2,
    }
    assert PipelineScopedPolicyActivationV1.model_validate(value).activation_epoch == 3
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate({**value, "activation_epoch": 0})
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate({**value, "state": "disabled"})
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate({**value, "environment": " staging"})
    with pytest.raises(ValueError):
        PipelineScopedPolicyActivationV1.model_validate(
            {**value, "authority_kind": "profile_calibration", "desired_slots": 2}
        )


def test_behavior_autoscaler_uses_scoped_desired_slots_only() -> None:
    row = WorkerPoolAutoscalerPolicy(
        environment="staging",
        pool_name="behavior-gpu-gb10",
        actuator="slurm",
        enabled=True,
        min_slots=0,
        max_slots=1,
        actuator_config={"policy_config_sha256": "sha256:" + "a" * 64},
    )
    disabled = _apply_pipeline_scoped_activation(_policy_to_config(row), row, None)
    assert disabled.enabled is False
    assert disabled.max_slots == 0
    activation = PipelineScopedPolicyActivation(
        environment="staging",
        policy_id="behavior-gpu-gb10",
        policy_config_sha256="sha256:" + "a" * 64,
        authority_kind="acceptance",
        authority_id=UUID("9e8174fa-7ad2-4386-869b-aadcfcc2cfa6"),
        activation_epoch=4,
        state="active",
        desired_slots=1,
    )
    active = _apply_pipeline_scoped_activation(_policy_to_config(row), row, activation)
    assert active.enabled is True
    assert active.min_slots == active.max_slots == 1
