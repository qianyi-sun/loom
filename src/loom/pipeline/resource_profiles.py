"""Server-owned Pipeline ResourceProfile registry."""

from __future__ import annotations

import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any

from loom.pipeline.keys import canonical_digest
from loom.pipeline.work_protocol import ResourceProfileV1

INPUT_CACHE_ALLOCATABLE_BYTES_MIN = 1_649_267_441_664
INPUT_CACHE_RAW_BYTES_MIN = 1_940_314_637_252

_NULLABLE_VARIANT_FIELDS = (
    "gpu_vendor",
    "gpu_memory_kind",
    "gpu_memory_mb_min",
    "gpu_unified_memory_mb_min",
    "container_memory_bytes_override",
    "device_roles",
)
_FORBIDDEN_USER_RESOURCE_KEYS = frozenset(
    {
        "allowed_gpu_models",
        "cpu_arch",
        "device_ids",
        "device_roles",
        "execution_variant_id",
        "gpu_backend_selection_sha256",
        "gpu_device_uuids",
        "gpu_model",
        "gpu_vendor",
        "min_nvidia_driver_version",
        "network_profile",
        "pids",
        "pids_limit",
        "pool_class",
        "required_host_runtime_features",
        "required_image_features",
        "resource_profile",
        "slurm_cluster_id",
        "variant_id",
        "worker_pool",
    }
)


class ResourceProfileRegistryError(ValueError):
    """The checked-in ResourceProfile registry is invalid or ambiguous."""


def reject_user_resource_overrides(value: object) -> None:
    """Reject resource authority in arbitrary user/API parameter trees."""

    if isinstance(value, Mapping):
        forbidden = sorted(
            (str(key) for key in value if str(key) in _FORBIDDEN_USER_RESOURCE_KEYS),
            key=str.encode,
        )
        if forbidden:
            raise ResourceProfileRegistryError(
                f"user resource override is forbidden: {forbidden[0]}"
            )
        for item in value.values():
            reject_user_resource_overrides(item)
    elif isinstance(value, list | tuple):
        for item in value:
            reject_user_resource_overrides(item)


@dataclass(frozen=True)
class ResourceProfileRecord:
    profile: ResourceProfileV1
    snapshot_sha256: str

    @property
    def identity(self) -> str:
        return f"{self.profile.name}@{self.profile.version}"


class ResourceProfileRegistry:
    """Immutable lookup surface; callers never supply profile definitions."""

    def __init__(self, records: Mapping[str, ResourceProfileRecord]) -> None:
        self._records = MappingProxyType(dict(records))

    @classmethod
    def load(
        cls,
        path: Path = Path("config/resource-profiles.toml"),
        *,
        require_builtin_contract: bool = True,
    ) -> ResourceProfileRegistry:
        try:
            raw = tomllib.loads(path.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise ResourceProfileRegistryError(f"cannot load ResourceProfiles: {exc}") from exc
        if set(raw) != {"schema_version", "profiles"}:
            raise ResourceProfileRegistryError("ResourceProfile registry keys are not closed")
        if raw["schema_version"] != "loom.resource-profile-registry.v1":
            raise ResourceProfileRegistryError("unsupported ResourceProfile registry version")
        profiles = raw["profiles"]
        if not isinstance(profiles, list):
            raise ResourceProfileRegistryError("profiles must be an array")

        records: dict[str, ResourceProfileRecord] = {}
        for value in profiles:
            if not isinstance(value, dict):
                raise ResourceProfileRegistryError("profile entries must be tables")
            normalized = _normalize_toml_profile(value)
            profile = ResourceProfileV1.model_validate(normalized)
            identity = f"{profile.name}@{profile.version}"
            if identity in records:
                raise ResourceProfileRegistryError(f"duplicate ResourceProfile {identity}")
            records[identity] = ResourceProfileRecord(profile, canonical_digest(profile))
        ordered = sorted(records, key=lambda item: item.encode("utf-8"))
        if list(records) != ordered:
            raise ResourceProfileRegistryError("ResourceProfiles must be bytewise sorted")
        registry = cls(records)
        if require_builtin_contract:
            _validate_builtin_contract(registry)
        return registry

    def get(self, identity: str) -> ResourceProfileRecord:
        try:
            return self._records[identity]
        except KeyError as exc:
            raise ResourceProfileRegistryError("unknown server-owned ResourceProfile") from exc

    def list(self) -> tuple[ResourceProfileRecord, ...]:
        return tuple(self._records.values())


def _normalize_toml_profile(value: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(value)
    variants = normalized.get("execution_variants")
    if not isinstance(variants, list):
        raise ResourceProfileRegistryError("execution_variants must be an array")
    normalized_variants: list[dict[str, Any]] = []
    for raw_variant in variants:
        if not isinstance(raw_variant, dict):
            raise ResourceProfileRegistryError("execution variant must be a table")
        variant = dict(raw_variant)
        for field in _NULLABLE_VARIANT_FIELDS:
            variant.setdefault(field, None)
        normalized_variants.append(variant)
    normalized["execution_variants"] = normalized_variants
    return normalized


def _validate_builtin_contract(registry: ResourceProfileRegistry) -> None:
    identities = tuple(record.identity for record in registry.list())
    expected_identities = (
        "behavior-export-io@1",
        "behavior-offline-gateway@1",
        "behavior-offline-none@1",
        "behavior-sim-local-gateway@1",
        "behavior-sim-local-none@1",
        "pipeline-test-cpu-gateway@1",
        "pipeline-test-cpu-none@1",
        "terminalgen-generate-gateway@1",
        "terminalgen-package-none@1",
        "terminalgen-plan-none@1",
        "terminalgen-validate-none@1",
    )
    if identities != expected_identities:
        raise ResourceProfileRegistryError("built-in ResourceProfile set is not exact")
    expected_scalars = {
        "behavior-export-io@1": (8, 64 << 30, 768 << 30, None, 7_200, "none"),
        "behavior-offline-gateway@1": (8, 16 << 30, 50 << 30, None, 3_600, "gateway"),
        "behavior-offline-none@1": (8, 16 << 30, 50 << 30, None, 3_600, "none"),
        "behavior-sim-local-gateway@1": (16, 64 << 30, 150 << 30, None, 14_400, "gateway"),
        "behavior-sim-local-none@1": (16, 64 << 30, 150 << 30, None, 14_400, "none"),
        "pipeline-test-cpu-gateway@1": (2, 1 << 30, 2 << 30, None, 600, "gateway"),
        "pipeline-test-cpu-none@1": (2, 1 << 30, 2 << 30, None, 600, "none"),
        "terminalgen-generate-gateway@1": (4, 16 << 30, 320 << 30, 1_024, 3_600, "gateway"),
        "terminalgen-package-none@1": (8, 32 << 30, 1_280 << 30, 512, 7_200, "none"),
        "terminalgen-plan-none@1": (2, 4 << 30, 20 << 30, 256, 900, "none"),
        "terminalgen-validate-none@1": (4, 16 << 30, 320 << 30, 2_048, 7_200, "none"),
    }
    cpu_variant = {
        "variant_id": "cpu-data-x86_64",
        "cpu_arch": "x86_64",
        "gpu_count_exact": 0,
        "gpu_vendor": None,
        "allowed_gpu_models": [],
        "gpu_memory_kind": None,
        "gpu_memory_mb_min": None,
        "gpu_unified_memory_mb_min": None,
        "memory_accounting_kind": "separate",
        "container_memory_bytes_override": None,
        "same_gpu_model_required": False,
        "pool_class": "behavior-cpu-data",
        "device_roles": None,
    }
    pipeline_test_cpu_variant = {
        **cpu_variant,
        "variant_id": "pipeline-test-cpu-x86_64",
        "pool_class": "pipeline-test-cpu",
    }
    terminalgen_pool_features = {
        "terminalgen-generate-gateway@1": (
            "terminalgen-generate-gateway",
            "terminalgen-generator",
        ),
        "terminalgen-package-none@1": (
            "terminalgen-package-none",
            "terminalgen-packager",
        ),
        "terminalgen-plan-none@1": ("terminalgen-plan-none", "terminalgen-planner"),
        "terminalgen-validate-none@1": (
            "terminalgen-validate-none",
            "terminalgen-validator",
        ),
    }
    gpu_variants = [
        {
            "variant_id": "gb10-shared-1gpu",
            "cpu_arch": "arm64",
            "gpu_count_exact": 1,
            "gpu_vendor": "nvidia",
            "allowed_gpu_models": ["NVIDIA GB10"],
            "gpu_memory_kind": "unified",
            "gpu_memory_mb_min": None,
            "gpu_unified_memory_mb_min": 120_000,
            "memory_accounting_kind": "unified_shared",
            "container_memory_bytes_override": 125_829_120_000,
            "same_gpu_model_required": True,
            "pool_class": "behavior-gpu-gb10",
            "device_roles": {"sim_gpu_index": 0, "vla_gpu_index": 0},
        },
        {
            "variant_id": "oldlab-rtx5080-2gpu",
            "cpu_arch": "x86_64",
            "gpu_count_exact": 2,
            "gpu_vendor": "nvidia",
            "allowed_gpu_models": ["NVIDIA GeForce RTX 5080"],
            "gpu_memory_kind": "dedicated",
            "gpu_memory_mb_min": 16_000,
            "gpu_unified_memory_mb_min": None,
            "memory_accounting_kind": "separate",
            "container_memory_bytes_override": None,
            "same_gpu_model_required": True,
            "pool_class": "behavior-gpu-oldlab",
            "device_roles": {"sim_gpu_index": 0, "vla_gpu_index": 1},
        },
    ]
    expected_features = {
        "behavior-export-io@1": ([], ["behavior-cpu-data"]),
        "behavior-offline-gateway@1": (
            ["loom-secret-tmpfs-v1"],
            ["behavior-cpu-data"],
        ),
        "behavior-offline-none@1": ([], ["behavior-cpu-data"]),
        "behavior-sim-local-gateway@1": (
            ["egl", "loom-secret-tmpfs-v1", "nvidia-container-runtime"],
            ["isaac-sim-5.1", "omnigibson-3.8"],
        ),
        "behavior-sim-local-none@1": (
            ["egl", "nvidia-container-runtime"],
            ["isaac-sim-5.1", "omnigibson-3.8"],
        ),
        "pipeline-test-cpu-gateway@1": (
            ["loom-secret-tmpfs-v1"],
            ["pipeline-test-cpu"],
        ),
        "pipeline-test-cpu-none@1": ([], ["pipeline-test-cpu"]),
        "terminalgen-generate-gateway@1": (
            ["loom-secret-tmpfs-v1"],
            ["terminalgen-generator"],
        ),
        "terminalgen-package-none@1": ([], ["terminalgen-packager"]),
        "terminalgen-plan-none@1": ([], ["terminalgen-planner"]),
        "terminalgen-validate-none@1": (
            ["loom-terminal-task-validator-v1"],
            ["terminalgen-validator"],
        ),
    }
    for record in registry.list():
        profile = record.profile
        actual = (
            profile.cpu_cores,
            profile.memory_bytes,
            profile.scratch_bytes,
            profile.pids_limit,
            profile.timeout_seconds_max,
            profile.network_profile,
        )
        if actual != expected_scalars[record.identity]:
            raise ResourceProfileRegistryError(f"{record.identity} scalar contract drift")
        expected_cache_capacity = (
            0
            if record.identity.startswith("pipeline-test-cpu-")
            else (100 << 30)
            if record.identity in {
                "terminalgen-generate-gateway@1",
                "terminalgen-plan-none@1",
            }
            else (400 << 30)
            if record.identity == "terminalgen-validate-none@1"
            else (1_100 << 30)
            if record.identity == "terminalgen-package-none@1"
            else INPUT_CACHE_ALLOCATABLE_BYTES_MIN
        )
        if profile.input_cache_capacity_bytes_min != expected_cache_capacity:
            raise ResourceProfileRegistryError(f"{record.identity} cache capacity drift")
        if (
            profile.required_host_runtime_features,
            profile.required_image_features,
        ) != expected_features[record.identity]:
            raise ResourceProfileRegistryError(f"{record.identity} feature contract drift")
        variant_ids = tuple(item.variant_id for item in profile.execution_variants)
        expected_variants = (
            ("gb10-shared-1gpu", "oldlab-rtx5080-2gpu")
            if record.identity.startswith("behavior-sim-local-")
            else ("pipeline-test-cpu-x86_64",)
            if record.identity.startswith("pipeline-test-cpu-")
            else ("terminalgen-cpu-x86_64",)
            if record.identity.startswith("terminalgen-")
            else ("cpu-data-x86_64",)
        )
        if variant_ids != expected_variants:
            raise ResourceProfileRegistryError(f"{record.identity} variant set drift")
        actual_variants = [
            item.model_dump(mode="json") for item in profile.execution_variants
        ]
        terminalgen_variant = None
        if record.identity in terminalgen_pool_features:
            pool_class, _image_feature = terminalgen_pool_features[record.identity]
            terminalgen_variant = {
                **cpu_variant,
                "variant_id": "terminalgen-cpu-x86_64",
                "pool_class": pool_class,
            }
        if actual_variants != (
            gpu_variants
            if record.identity.startswith("behavior-sim-local-")
            else [pipeline_test_cpu_variant]
            if record.identity.startswith("pipeline-test-cpu-")
            else [terminalgen_variant]
            if terminalgen_variant is not None
            else [cpu_variant]
        ):
            raise ResourceProfileRegistryError(f"{record.identity} variant contract drift")


def load_resource_profiles(
    path: Path = Path("config/resource-profiles.toml"),
) -> ResourceProfileRegistry:
    return ResourceProfileRegistry.load(path)
