"""Closed disabled-by-default BEHAVIOR Pipeline policy snapshots."""

from __future__ import annotations

import csv
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import StringConstraints, field_validator, model_validator

from loom.pipeline.image_runtime import ImageRuntimeRegistry
from loom.pipeline.keys import canonical_digest
from loom.pipeline.resource_profiles import (
    ResourceProfileRegistry,
)
from loom.pipeline.spec import Digest, NonNegativeSafeInt, PipelineModel, PositiveSafeInt

PolicyId = Annotated[str, StringConstraints(pattern=r"^behavior-[a-z0-9-]+$")]


class PipelinePolicyConfigError(ValueError):
    pass


def _digest_bytes(value: bytes) -> str:
    return "sha256:" + sha256(value).hexdigest()


def digest_installed_inventory(*, root: Path, paths: Sequence[str]) -> str:
    ordered = sorted(paths, key=str.encode)
    if list(paths) != ordered or len(ordered) != len(set(ordered)):
        raise PipelinePolicyConfigError("Slurm config bundle paths must be sorted and unique")
    digest = sha256()
    for relative in ordered:
        path = Path(relative)
        if path.is_absolute() or ".." in path.parts:
            raise PipelinePolicyConfigError("Slurm config bundle path escapes the repository")
        content = (root / path).read_bytes()
        digest.update(relative.encode("utf-8"))
        digest.update(b"\x00")
        digest.update(len(content).to_bytes(8, "big"))
        digest.update(content)
    return "sha256:" + digest.hexdigest()


def read_worker_plan(path: Path, *, cluster_id: str) -> tuple[str, ...]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != [
            "hostname",
            "partition",
            "gpu_tres",
            "desired_state",
            "direct_worker_enabled",
        ]:
            raise PipelinePolicyConfigError("worker-plan.csv columns are not exact")
        rows = list(reader)
    hostnames = [row["hostname"] for row in rows]
    if hostnames != sorted(hostnames, key=str.encode) or len(hostnames) != len(set(hostnames)):
        raise PipelinePolicyConfigError("worker plan hostnames must be bytewise sorted and unique")
    if any(row["direct_worker_enabled"] != "false" for row in rows):
        raise PipelinePolicyConfigError("direct worker capacity must remain disabled")
    expected_hosts = {
        "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(1, 6)),
        "gb10": tuple(
            sorted((f"trt-gb10-{index}" for index in range(1, 16)), key=str.encode)
        ),
    }.get(cluster_id)
    if expected_hosts is None or tuple(hostnames) != expected_hosts:
        raise PipelinePolicyConfigError("worker plan is not the exact repo-owned inventory")
    expected_partition, expected_tres = {
        "oldlab": ("all", "gpu:rtx5080:2"),
        "gb10": ("gb10", "gpu:gb10:1"),
    }[cluster_id]
    if any(
        row["partition"] != expected_partition or row["gpu_tres"] != expected_tres
        for row in rows
    ):
        raise PipelinePolicyConfigError("worker plan partition/GRES drift")
    expected_state = {
        hostname: ("drain" if hostname == "trt-gb10-7" else "slurm_only")
        for hostname in hostnames
    }
    if any(row["desired_state"] != expected_state[row["hostname"]] for row in rows):
        raise PipelinePolicyConfigError("worker plan desired-state drift")
    return tuple(hostnames)


class SlurmClusterConfigV1(PipelineModel):
    cluster_id: Literal["oldlab", "gb10"]
    cluster_name: Literal["trt-oldlab", "trt-gb10"]
    submit_host_ref: str
    worker_plan_sha256: Digest
    autoscaler_supervisor_id: str
    slurm_conf_bundle_sha256: Digest

    @model_validator(mode="after")
    def cluster_fields_are_exact(self) -> SlurmClusterConfigV1:
        expected = {
            "oldlab": ("trt-oldlab", "loom-autoscaler-oldlab"),
            "gb10": ("trt-gb10", "loom-autoscaler-gb10"),
        }[self.cluster_id]
        if self.cluster_name != expected[0] or self.autoscaler_supervisor_id != expected[1]:
            raise ValueError("Slurm cluster identity drift")
        if self.submit_host_ref != f"loom://slurm/{self.cluster_id}":
            raise ValueError("Slurm submit host must remain an opaque repo-owned reference")
        return self


class PolicyResourceProfileV1(PipelineModel):
    name: str
    version: PositiveSafeInt
    snapshot_sha256: Digest
    allowed_variant_ids: list[str]

    @field_validator("allowed_variant_ids")
    @classmethod
    def variants_are_sorted(cls, values: list[str]) -> list[str]:
        if not values or values != sorted(values, key=str.encode) or len(values) != len(set(values)):
            raise ValueError("allowed variant IDs must be nonempty, sorted, and unique")
        return values


class PolicyDriverConstraintsV1(PipelineModel):
    comparison: Literal["dotted_integer"]
    minimum_versions: list[str]

    @field_validator("minimum_versions")
    @classmethod
    def versions_are_canonical(cls, values: list[str]) -> list[str]:
        if values != sorted(values, key=str.encode) or len(values) != len(set(values)):
            raise ValueError("minimum driver versions must be sorted and unique")
        return values


class PolicyRuntimeConstraintV1(PipelineModel):
    variant_id: str
    cpu_arch: Literal["x86_64", "arm64"]
    gpu_vendor: Literal["nvidia"] | None
    allowed_gpu_models: list[str]
    memory_accounting_kind: Literal["separate", "unified_shared"]
    required_host_runtime_features: list[str]
    required_image_features: list[str]
    network_profiles: list[Literal["gateway", "none"]]


class PolicyConfigSnapshotV1(PipelineModel):
    schema_version: Literal["loom.policy-config-snapshot.v1"]
    policy_id: Literal[
        "behavior-cpu-data", "behavior-gpu-oldlab", "behavior-gpu-gb10"
    ]
    version: PositiveSafeInt
    actuator: Literal["slurm"]
    slurm_cluster_id: Literal["oldlab", "gb10"]
    slurm_cluster_config_sha256: Digest
    pool_class: Literal[
        "behavior-cpu-data", "behavior-gpu-oldlab", "behavior-gpu-gb10"
    ]
    requested_concurrency: Literal[1]
    min_slots: Literal[0]
    max_slots: PositiveSafeInt
    required_gpu_count_per_slot: NonNegativeSafeInt
    allowed_nodes: list[str]
    account: str | None
    partition: Literal["all", "gb10"]
    qos: Literal["normal"]
    reservation: str | None
    gpu_tres: Literal["gpu:rtx5080:2", "gpu:gb10:1"] | None
    resource_profiles: list[PolicyResourceProfileV1]
    input_cache_capacity_bytes_min: Literal[1_649_267_441_664]
    input_cache_raw_bytes_min: Literal[1_940_314_637_252]
    scratch_bytes_min: PositiveSafeInt
    driver_constraints: PolicyDriverConstraintsV1
    runtime_constraints: list[PolicyRuntimeConstraintV1]

    @field_validator("allowed_nodes")
    @classmethod
    def nodes_are_sorted(cls, values: list[str]) -> list[str]:
        if not values or values != sorted(values, key=str.encode) or len(values) != len(set(values)):
            raise ValueError("allowed nodes must be nonempty, bytewise sorted, and unique")
        return values

    @field_validator("resource_profiles")
    @classmethod
    def profile_refs_are_canonical(
        cls, values: list[PolicyResourceProfileV1]
    ) -> list[PolicyResourceProfileV1]:
        identities = [f"{item.name}@{item.version}" for item in values]
        if identities != sorted(identities, key=str.encode) or len(identities) != len(
            set(identities)
        ):
            raise ValueError("policy ResourceProfile refs must be sorted and unique")
        return values

    @field_validator("runtime_constraints")
    @classmethod
    def runtime_constraints_are_canonical(
        cls, values: list[PolicyRuntimeConstraintV1]
    ) -> list[PolicyRuntimeConstraintV1]:
        variants = [item.variant_id for item in values]
        if variants != sorted(variants, key=str.encode) or len(variants) != len(
            set(variants)
        ):
            raise ValueError("policy runtime constraints must be sorted and unique")
        return values

    @model_validator(mode="after")
    def policy_cluster_resources_are_exact(self) -> PolicyConfigSnapshotV1:
        if self.policy_id != self.pool_class:
            raise ValueError("policy ID and pool class must be identical")
        expected = {
            "behavior-cpu-data": ("oldlab", "all", 0, None, 4, 768 << 30),
            "behavior-gpu-oldlab": (
                "oldlab",
                "all",
                2,
                "gpu:rtx5080:2",
                1,
                150 << 30,
            ),
            "behavior-gpu-gb10": (
                "gb10",
                "gb10",
                1,
                "gpu:gb10:1",
                1,
                150 << 30,
            ),
        }[self.policy_id]
        if (
            self.slurm_cluster_id,
            self.partition,
            self.required_gpu_count_per_slot,
            self.gpu_tres,
            self.max_slots,
            self.scratch_bytes_min,
        ) != expected:
            raise ValueError("policy resources drift from the disabled BEHAVIOR contract")
        if self.account is not None or self.reservation is not None:
            raise ValueError("BEHAVIOR policies do not carry account or reservation overrides")
        expected_profiles = {
            "behavior-cpu-data": [
                ("behavior-export-io@1", ["cpu-data-x86_64"]),
                ("behavior-offline-gateway@1", ["cpu-data-x86_64"]),
                ("behavior-offline-none@1", ["cpu-data-x86_64"]),
            ],
            "behavior-gpu-gb10": [
                ("behavior-sim-local-gateway@1", ["gb10-shared-1gpu"]),
                ("behavior-sim-local-none@1", ["gb10-shared-1gpu"]),
            ],
            "behavior-gpu-oldlab": [
                ("behavior-sim-local-gateway@1", ["oldlab-rtx5080-2gpu"]),
                ("behavior-sim-local-none@1", ["oldlab-rtx5080-2gpu"]),
            ],
        }[self.policy_id]
        actual_profiles = [
            (f"{item.name}@{item.version}", item.allowed_variant_ids)
            for item in self.resource_profiles
        ]
        if actual_profiles != expected_profiles:
            raise ValueError("policy ResourceProfile/variant assignment drift")
        expected_variant = {
            "behavior-cpu-data": "cpu-data-x86_64",
            "behavior-gpu-gb10": "gb10-shared-1gpu",
            "behavior-gpu-oldlab": "oldlab-rtx5080-2gpu",
        }[self.policy_id]
        if [item.variant_id for item in self.runtime_constraints] != [expected_variant]:
            raise ValueError("policy runtime variant assignment drift")
        return self

    @property
    def policy_config_sha256(self) -> Digest:
        return canonical_digest(self)


class PipelineScopedPolicyActivationV1(PipelineModel):
    """Mutable authorization-scoped capacity, separate from immutable config."""

    schema_version: Literal["loom.pipeline-scoped-policy-activation.v1"]
    environment: str
    policy_id: Literal[
        "behavior-cpu-data", "behavior-gpu-oldlab", "behavior-gpu-gb10"
    ]
    policy_config_sha256: Digest
    authority_kind: Literal["acceptance", "profile_calibration"]
    authority_id: UUID
    activation_epoch: PositiveSafeInt
    state: Literal["active", "draining", "disabled"]
    desired_slots: NonNegativeSafeInt

    @field_validator("environment")
    @classmethod
    def environment_is_exact(cls, value: str) -> str:
        if (
            not value
            or value != value.strip()
            or any(ord(character) < 32 or ord(character) == 127 for character in value)
        ):
            raise ValueError("activation environment must be an exact nonempty value")
        return value

    @model_validator(mode="after")
    def desired_capacity_is_exact(self) -> PipelineScopedPolicyActivationV1:
        maximum = 2 if self.policy_id == "behavior-cpu-data" else 1
        if self.desired_slots > maximum:
            raise ValueError("activation desired slots exceed the closed policy target")
        if (self.state == "active") != (self.desired_slots > 0):
            raise ValueError("only an active activation may own positive desired slots")
        if self.authority_kind == "profile_calibration" and self.desired_slots > 1:
            raise ValueError("profile calibration may activate only one CPU slot")
        return self


@dataclass(frozen=True)
class PolicyConfigRecord:
    snapshot: PolicyConfigSnapshotV1
    policy_config_sha256: str
    cluster: SlurmClusterConfigV1


class PolicyConfigRegistry:
    def __init__(self, records: Mapping[str, PolicyConfigRecord]) -> None:
        self._records = MappingProxyType(dict(records))

    @classmethod
    def load(
        cls,
        *,
        resource_profiles: ResourceProfileRegistry,
        image_runtime_contracts: ImageRuntimeRegistry | None = None,
        path: Path = Path("config/pipeline-policy-config.toml"),
        repository_root: Path = Path("."),
    ) -> PolicyConfigRegistry:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        if set(raw) != {"schema_version", "clusters", "policies"}:
            raise PipelinePolicyConfigError("policy registry keys are not closed")
        if raw["schema_version"] != "loom.pipeline-policy-registry.v1":
            raise PipelinePolicyConfigError("unsupported policy registry schema")
        if image_runtime_contracts is None:
            image_runtime_contracts = ImageRuntimeRegistry.load(
                repository_root / "config/image-runtime-contracts.toml"
            )
        clusters: dict[str, tuple[SlurmClusterConfigV1, tuple[str, ...]]] = {}
        for value in raw["clusters"]:
            source = dict(value)
            plan_path = str(source.pop("worker_plan_path"))
            bundle_paths = list(source.pop("slurm_conf_bundle_paths"))
            plan_bytes = (repository_root / plan_path).read_bytes()
            nodes = read_worker_plan(
                repository_root / plan_path,
                cluster_id=str(source["cluster_id"]),
            )
            source["worker_plan_sha256"] = _digest_bytes(plan_bytes)
            source["slurm_conf_bundle_sha256"] = digest_installed_inventory(
                root=repository_root, paths=bundle_paths
            )
            cluster = SlurmClusterConfigV1.model_validate(source)
            if cluster.cluster_id in clusters:
                raise PipelinePolicyConfigError("duplicate Slurm cluster config")
            clusters[cluster.cluster_id] = cluster, nodes
        if set(clusters) != {"oldlab", "gb10"}:
            raise PipelinePolicyConfigError("exactly oldlab and gb10 clusters are required")

        records: dict[str, PolicyConfigRecord] = {}
        for value in raw["policies"]:
            source = dict(value)
            cluster, inventory_nodes = clusters[str(source["slurm_cluster_id"])]
            if tuple(source.pop("allowed_nodes")) != inventory_nodes:
                raise PipelinePolicyConfigError("policy nodes differ from its worker plan")
            source["allowed_nodes"] = list(inventory_nodes)
            refs: list[dict[str, Any]] = []
            runtime: dict[str, dict[str, Any]] = {}
            for raw_ref in source.pop("resource_profiles"):
                ref = dict(raw_ref)
                identity = f"{ref['name']}@{ref['version']}"
                record = resource_profiles.get(identity)
                allowed_variants = list(ref["allowed_variant_ids"])
                refs.append(
                    {
                        **ref,
                        "snapshot_sha256": record.snapshot_sha256,
                    }
                )
                for variant in record.profile.execution_variants:
                    if variant.variant_id not in allowed_variants:
                        continue
                    item = runtime.setdefault(
                        variant.variant_id,
                        {
                            "variant_id": variant.variant_id,
                            "cpu_arch": variant.cpu_arch,
                            "gpu_vendor": variant.gpu_vendor,
                            "allowed_gpu_models": variant.allowed_gpu_models,
                            "memory_accounting_kind": variant.memory_accounting_kind,
                            "required_host_runtime_features": set(),
                            "required_image_features": set(),
                            "network_profiles": set(),
                        },
                    )
                    item["required_host_runtime_features"].update(
                        record.profile.required_host_runtime_features
                    )
                    item["required_image_features"].update(
                        record.profile.required_image_features
                    )
                    item["network_profiles"].add(record.profile.network_profile)
            source["resource_profiles"] = refs
            source["slurm_cluster_config_sha256"] = canonical_digest(cluster)
            minimum_driver_versions: set[str] = set()
            for item in runtime.values():
                required_features = set(item["required_image_features"])
                for image_record in image_runtime_contracts.list():
                    contract = image_record.contract
                    if (
                        contract.cpu_arch == item["cpu_arch"]
                        and contract.gpu_vendor == (item["gpu_vendor"] or "none")
                        and required_features <= set(contract.application_features)
                        and contract.min_nvidia_driver_version is not None
                    ):
                        minimum_driver_versions.add(
                            contract.min_nvidia_driver_version
                        )
            source["driver_constraints"] = {
                "comparison": "dotted_integer",
                "minimum_versions": sorted(minimum_driver_versions, key=str.encode),
            }
            source["runtime_constraints"] = [
                {
                    **item,
                    "required_host_runtime_features": sorted(
                        item["required_host_runtime_features"], key=str.encode
                    ),
                    "required_image_features": sorted(
                        item["required_image_features"], key=str.encode
                    ),
                    "network_profiles": sorted(item["network_profiles"], key=str.encode),
                }
                for _, item in sorted(runtime.items(), key=lambda pair: pair[0].encode())
            ]
            source.setdefault("account", None)
            source.setdefault("reservation", None)
            source.setdefault("gpu_tres", None)
            snapshot = PolicyConfigSnapshotV1.model_validate(source)
            if snapshot.policy_id in records:
                raise PipelinePolicyConfigError("duplicate Pipeline policy")
            records[snapshot.policy_id] = PolicyConfigRecord(
                snapshot=snapshot,
                policy_config_sha256=canonical_digest(snapshot),
                cluster=cluster,
            )
        expected = ["behavior-cpu-data", "behavior-gpu-gb10", "behavior-gpu-oldlab"]
        if list(records) != expected:
            raise PipelinePolicyConfigError("exact disabled policy set/order is required")
        return cls(records)

    def get(self, policy_id: str) -> PolicyConfigRecord:
        try:
            return self._records[policy_id]
        except KeyError as exc:
            raise PipelinePolicyConfigError("unknown server-owned Pipeline policy") from exc

    def list(self) -> tuple[PolicyConfigRecord, ...]:
        return tuple(self._records.values())

    def disabled_autoscaler_rows(self, *, environment: str) -> tuple[dict[str, Any], ...]:
        """Render desired=0 DB payloads without mutating immutable config snapshots."""

        if not environment or any(character.isspace() for character in environment):
            raise PipelinePolicyConfigError("environment identity is invalid")
        return tuple(
            {
                "environment": environment,
                "pool_name": record.snapshot.pool_class,
                "actuator": "slurm",
                "enabled": False,
                "min_slots": 0,
                "max_slots": record.snapshot.max_slots,
                "disabled_reason": "pipeline_gpu_policy_not_activated",
                "actuator_config": {
                    "policy_id": record.snapshot.policy_id,
                    "policy_config_sha256": record.policy_config_sha256,
                    "slurm_cluster_id": record.snapshot.slurm_cluster_id,
                    "slurm_cluster_config_sha256": (
                        record.snapshot.slurm_cluster_config_sha256
                    ),
                    "requested_concurrency": 1,
                    "partition": record.snapshot.partition,
                    "gpu_tres": record.snapshot.gpu_tres,
                    "allowed_nodes": record.snapshot.allowed_nodes,
                },
            }
            for record in self.list()
        )
