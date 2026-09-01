"""Typed render model for the inert global capacity authority in ``loom-dev``."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import tomllib
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlsplit
from uuid import UUID

import yaml  # type: ignore[import-untyped]
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom_capacity_executor.config import ImmutablePoolManifest
from loom_capacity_executor.launch_renderer import canonical_launch_policy_digest
from loom_capacity_executor.runtime import (
    ActivationRuntimeArtifactV2,
    ApprovedLaunchProfileSetV2,
    RuntimeAssemblyError,
    canonical_approved_profiles_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationPolicyV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.schema_startup import _capacity_head
from loom_capacity_pool_executor.config import (
    SlurmInventoryPolicyDocument,
    canonical_slurm_inventory_policy_bytes,
)

_DNS_LABEL_RE = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?")
_LABEL_NAME_RE = re.compile(r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?")
_OCI_DIGEST_RE = re.compile(
    r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)*"
    r"(?::[A-Za-z0-9][A-Za-z0-9._-]{0,127})?@sha256:[0-9a-f]{64}"
)
_CPU_RE = re.compile(r"([1-9][0-9]*)(m)?")
_MEMORY_RE = re.compile(r"([1-9][0-9]*)(Ki|Mi|Gi)")
_MAX_CPU_MILLICORES = 64_000
_MAX_RESOURCE_MEMORY_BYTES = 1024**4
_MAX_POSTGRES_STORAGE_BYTES = 64 * 1024**4
_MAX_CONFIGMAP_POLICY_BYTES = 1024 * 1024
_EXECUTION_POLICY_DIRECTORY = "/etc/loom-capacity-manager/execution-policy"
_EXECUTION_POLICY_FILENAME = "execution-policy.json"
_EXECUTION_POLICY_PATH = f"{_EXECUTION_POLICY_DIRECTORY}/{_EXECUTION_POLICY_FILENAME}"
_EXECUTION_POLICY_PROJECTED_VOLUME = "execution-policy-projected"
_EXECUTION_POLICY_RUNTIME_VOLUME = "execution-policy-runtime"
_MANAGER_SERVICE_ORIGIN = "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443"
_MANAGER_ROUTER_NAMESPACE = "loom-capacity-router"
_MANAGER_ROUTER_NAME = "loom-capacity-manager-router"
_MANAGER_ROUTER_HOST = "192.168.50.103"
_MANAGER_ROUTER_NODE = "trt-eai-oldlab-1"
_MANAGER_ROUTER_PORT = 31443
_KUBERNETES_API_SERVER = "https://192.168.50.103:6443"
_KUBERNETES_API_SERVER_CIDR = "192.168.50.103/32"
_KUBERNETES_API_SERVER_PORT = 6443
_WITNESS_CONFIG_MAP = "loom-global-execution-witness-v1"
_WITNESS_PUBLISHER = "loom-capacity-witness-publisher"
_WITNESS_API_DIRECTORY = "/var/run/secrets/loom-witness"
_PRIVATE_IPV4_NETWORKS = tuple(
    ipaddress.ip_network(value) for value in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
)
_PRIVATE_IPV6_NETWORK = ipaddress.ip_network("fc00::/7")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


def _is_immutable_oci_reference(value: str) -> bool:
    if _OCI_DIGEST_RE.fullmatch(value) is None or value.endswith("@sha256:" + "0" * 64):
        return False
    name = value.rsplit("@sha256:", 1)[0]
    return len(name) <= 255 and ("/" in name or name.count(":") <= 1)


def _validate_dns_label(value: str, *, label: str) -> str:
    if _DNS_LABEL_RE.fullmatch(value) is None:
        raise ValueError(f"{label} must be a Kubernetes DNS label")
    return value


def _validate_label_key(value: str) -> str:
    parts = value.split("/")
    if len(parts) > 2 or not parts[-1] or _LABEL_NAME_RE.fullmatch(parts[-1]) is None:
        raise ValueError("Kubernetes label key is invalid")
    if len(parts) == 2 and (
        len(parts[0]) > 253
        or any(_DNS_LABEL_RE.fullmatch(segment) is None for segment in parts[0].split("."))
    ):
        raise ValueError("Kubernetes label key is invalid")
    return value


def _cpu_millicores(value: str) -> int:
    matched = _CPU_RE.fullmatch(value)
    if matched is None:
        raise ValueError("CPU resource quantity must be a positive integer or millicores")
    amount = int(matched.group(1))
    return amount if matched.group(2) else amount * 1000


def _memory_bytes(value: str) -> int:
    matched = _MEMORY_RE.fullmatch(value)
    if matched is None:
        raise ValueError("memory resource quantity must use positive Ki, Mi, or Gi")
    multipliers = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3}
    return int(matched.group(1)) * multipliers[matched.group(2)]


class ResourceEnvelope(_StrictModel):
    cpu_request: str
    memory_request: str
    cpu_limit: str
    memory_limit: str

    @model_validator(mode="after")
    def _bounded(self) -> ResourceEnvelope:
        cpu_request = _cpu_millicores(self.cpu_request)
        cpu_limit = _cpu_millicores(self.cpu_limit)
        memory_request = _memory_bytes(self.memory_request)
        memory_limit = _memory_bytes(self.memory_limit)
        if cpu_request > cpu_limit:
            raise ValueError("CPU request exceeds its limit")
        if cpu_limit > _MAX_CPU_MILLICORES:
            raise ValueError("CPU resource limit exceeds the control-plane bound")
        if memory_request > memory_limit:
            raise ValueError("memory request exceeds its limit")
        if memory_limit > _MAX_RESOURCE_MEMORY_BYTES:
            raise ValueError("memory resource limit exceeds the control-plane bound")
        return self

    def kubernetes(self) -> dict[str, dict[str, str]]:
        return {
            "requests": {"cpu": self.cpu_request, "memory": self.memory_request},
            "limits": {"cpu": self.cpu_limit, "memory": self.memory_limit},
        }


class PodSelector(_StrictModel):
    pod_label_key: str = Field(min_length=1, max_length=317)
    pod_label_value: str = Field(min_length=1, max_length=63)

    @field_validator("pod_label_key")
    @classmethod
    def _key(cls, value: str) -> str:
        return _validate_label_key(value)

    @field_validator("pod_label_value")
    @classmethod
    def _value(cls, value: str) -> str:
        if _LABEL_NAME_RE.fullmatch(value) is None:
            raise ValueError("Kubernetes label value is invalid")
        return value

    def match_labels(self) -> dict[str, str]:
        return {self.pod_label_key: self.pod_label_value}


class KubernetesEndpointSelector(PodSelector):
    namespace: str
    port: int = Field(ge=1, le=65535)

    @field_validator("namespace")
    @classmethod
    def _namespace(cls, value: str) -> str:
        return _validate_dns_label(value, label="selector namespace")


class CapacityControlPlaneProfile(_StrictModel):
    schema_version: Literal[1]
    namespace: Literal["loom-dev"]
    secret_name: str
    postgres_image: str
    postgres_storage: str
    postgres_resources: ResourceEnvelope
    migration_resources: ResourceEnvelope
    manager_resources: ResourceEnvelope
    dns: KubernetesEndpointSelector
    capacity_agent_client: PodSelector
    lifecycle_client: PodSelector
    storage_class_name: str | None = None

    @field_validator("secret_name")
    @classmethod
    def _secret_name(cls, value: str) -> str:
        return _validate_dns_label(value, label="capacity Secret name")

    @field_validator("postgres_image")
    @classmethod
    def _postgres_image(cls, value: str) -> str:
        if not _is_immutable_oci_reference(value):
            raise ValueError("capacity PostgreSQL image must be an immutable OCI reference")
        return value

    @field_validator("postgres_storage")
    @classmethod
    def _postgres_storage(cls, value: str) -> str:
        if _MEMORY_RE.fullmatch(value) is None:
            raise ValueError("capacity PostgreSQL storage must use positive Ki, Mi, or Gi")
        if _memory_bytes(value) > _MAX_POSTGRES_STORAGE_BYTES:
            raise ValueError("capacity PostgreSQL storage exceeds the control-plane bound")
        return value

    @field_validator("storage_class_name")
    @classmethod
    def _storage_class(cls, value: str | None) -> str | None:
        if value is not None and (
            len(value) > 253
            or any(_DNS_LABEL_RE.fullmatch(segment) is None for segment in value.split("."))
        ):
            raise ValueError("capacity storage class name is invalid")
        return value


class CapacityPoolSlurmExecutables(_StrictModel):
    scontrol: str = Field(min_length=1, max_length=4096)
    sacctmgr: str = Field(min_length=1, max_length=4096)
    squeue: str = Field(min_length=1, max_length=4096)
    sbatch: str = Field(min_length=1, max_length=4096)
    scancel: str = Field(min_length=1, max_length=4096)
    sacct: str = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def _absolute_distinct_paths(self) -> CapacityPoolSlurmExecutables:
        values = tuple(self.model_dump().values())
        if len(set(values)) != len(values):
            raise ValueError("Slurm executable paths must be distinct")
        for value in values:
            path = Path(value)
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError("Slurm executables must use absolute paths")
        return self


class CapacityPoolExecutorBinding(_StrictModel):
    pool_id: Literal["gb10", "oldlab"]
    pool_generation: int = Field(gt=0)
    executor_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    executor_incarnation: str = Field(min_length=36, max_length=36)
    controller_authority_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    local_authority_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    signing_key_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    signing_key_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    local_uid: int = Field(ge=0)
    slurm_cluster: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    controller_host: str = Field(min_length=1, max_length=253)
    partition: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    association: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    submitter: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    qos: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    profile_id: str = Field(min_length=1, max_length=128, pattern=r"^[a-z0-9._-]+$")
    profile_generation: int = Field(gt=0)
    profile_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    slurm_executables: CapacityPoolSlurmExecutables
    config_file: str = Field(min_length=1, max_length=4096)
    state_directory: str = Field(min_length=1, max_length=4096)
    journal_file: str = Field(min_length=1, max_length=4096)
    bearer_token_file: str = Field(min_length=1, max_length=4096)
    tls_ca_file: str = Field(min_length=1, max_length=4096)
    tls_certificate_file: str = Field(min_length=1, max_length=4096)
    tls_private_key_file: str = Field(min_length=1, max_length=4096)
    ownership_key_file: str = Field(min_length=1, max_length=4096)
    inventory: SlurmInventoryPolicyDocument

    @model_validator(mode="after")
    def _exact_binding(self) -> CapacityPoolExecutorBinding:
        if not self.executor_id.startswith(f"{self.pool_id}-"):
            raise ValueError("executor id differs from its pool binding")
        if not self.signing_key_id.startswith(f"{self.pool_id}-"):
            raise ValueError("signing key differs from its pool binding")
        try:
            incarnation = UUID(self.executor_incarnation)
        except ValueError as exc:
            raise ValueError("executor incarnation must be a canonical UUID") from exc
        if incarnation.int == 0 or str(incarnation) != self.executor_incarnation:
            raise ValueError("executor incarnation must be a canonical non-nil UUID")
        if re.fullmatch(r"/[A-Za-z0-9._/-]+", self.config_file) is None:
            raise ValueError("pool executor configuration path is unsafe for service rendering")
        for label, value in (
            ("configuration", self.config_file),
            ("state directory", self.state_directory),
            ("journal", self.journal_file),
            ("bearer credential", self.bearer_token_file),
            ("TLS CA", self.tls_ca_file),
            ("TLS certificate", self.tls_certificate_file),
            ("TLS private-key credential", self.tls_private_key_file),
            ("ownership-key credential", self.ownership_key_file),
        ):
            path = Path(value)
            if not path.is_absolute() or ".." in path.parts:
                raise ValueError(f"pool executor {label} must be an absolute path")
        if Path(self.config_file).suffix != ".json":
            raise ValueError("pool executor configuration must be controller-local JSON")
        if Path(self.journal_file).parent != Path(self.state_directory):
            raise ValueError("pool executor journal must be directly inside its state directory")
        if self.inventory.pool_id != self.pool_id:
            raise ValueError("inventory policy differs from its pool binding")
        if self.inventory.pool_generation != self.pool_generation:
            raise ValueError("inventory policy differs from its pool generation")
        if self.inventory.controller_cluster != self.slurm_cluster:
            raise ValueError("inventory controller differs from its Slurm cluster binding")
        if self.inventory.query_uid != self.local_uid:
            raise ValueError("inventory query uid differs from its pool binding")
        if self.partition not in self.inventory.relevant_partitions:
            raise ValueError("inventory policy omits the bound Slurm partition")
        return self


class CapacityPoolExecutorProfile(_StrictModel):
    schema_version: Literal[1]
    namespace: Literal["loom-dev"]
    executable_new_capacity_ceiling: Literal[0]
    executor_image: str
    service_user: str = Field(pattern=r"^[a-z_][a-z0-9_-]{0,31}$")
    authority_incarnation: str = Field(min_length=36, max_length=36)
    writer_epoch: int = Field(gt=0)
    configuration_epoch: int = Field(gt=0)
    execution_epoch: int = Field(gt=0)
    execution_manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    trusted_fleet_release_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    manager_origin: str
    pools: tuple[CapacityPoolExecutorBinding, CapacityPoolExecutorBinding]

    @field_validator("pools", mode="before")
    @classmethod
    def _pool_tuple(cls, value: object) -> object:
        if isinstance(value, list):
            return tuple(value)
        return value

    @field_validator("executor_image")
    @classmethod
    def _immutable_executor_image(cls, value: str) -> str:
        if not _is_immutable_oci_reference(value):
            raise ValueError("pool executor image must be an immutable OCI reference")
        return value

    @field_validator("authority_incarnation")
    @classmethod
    def _authority_uuid(cls, value: str) -> str:
        try:
            authority = UUID(value)
        except ValueError as exc:
            raise ValueError("capacity authority must be a canonical UUID") from exc
        if authority.int == 0 or str(authority) != value:
            raise ValueError("capacity authority must be a canonical non-nil UUID")
        return value

    @field_validator("manager_origin")
    @classmethod
    def _internal_manager_origin(cls, value: str) -> str:
        if value == _MANAGER_SERVICE_ORIGIN:
            return value
        try:
            parsed = urlsplit(value)
            port = parsed.port
            address = ipaddress.ip_address(parsed.hostname or "")
        except ValueError as exc:
            raise ValueError("manager origin must be one canonical internal HTTPS origin") from exc
        private = (
            any(address in network for network in _PRIVATE_IPV4_NETWORKS)
            if isinstance(address, ipaddress.IPv4Address)
            else address in _PRIVATE_IPV6_NETWORK
        )
        canonical_host = (
            address.compressed
            if isinstance(address, ipaddress.IPv4Address)
            else f"[{address.compressed}]"
        )
        if (
            not private
            or parsed.scheme != "https"
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path
            or parsed.query
            or parsed.fragment
            or port != _MANAGER_ROUTER_PORT
            or value != f"https://{canonical_host}:{_MANAGER_ROUTER_PORT}"
        ):
            raise ValueError("manager origin must be one canonical internal HTTPS origin")
        return value

    @model_validator(mode="after")
    def _two_independent_pools(self) -> CapacityPoolExecutorProfile:
        if tuple(pool.pool_id for pool in self.pools) != ("gb10", "oldlab"):
            raise ValueError("pool bindings must contain canonical gb10 and oldlab entries")
        unique_fields = {
            "executor": tuple(pool.executor_id for pool in self.pools),
            "executor incarnation": tuple(pool.executor_incarnation for pool in self.pools),
            "controller authority": tuple(pool.controller_authority_sha256 for pool in self.pools),
            "local authority": tuple(pool.local_authority_sha256 for pool in self.pools),
            "signing key": tuple(pool.signing_key_sha256 for pool in self.pools),
            "configuration": tuple(pool.config_file for pool in self.pools),
            "state": tuple(pool.state_directory for pool in self.pools),
            "bearer credential": tuple(pool.bearer_token_file for pool in self.pools),
            "TLS certificate credential": tuple(pool.tls_certificate_file for pool in self.pools),
            "TLS private-key credential": tuple(pool.tls_private_key_file for pool in self.pools),
            "ownership-key credential": tuple(pool.ownership_key_file for pool in self.pools),
            "Slurm cluster": tuple(pool.slurm_cluster for pool in self.pools),
            "controller host": tuple(pool.controller_host for pool in self.pools),
            "inventory reporter": tuple(pool.inventory.reporter_incarnation for pool in self.pools),
            "partition": tuple(pool.partition for pool in self.pools),
        }
        for label, values in unique_fields.items():
            if len(set(values)) != len(values):
                raise ValueError(f"pool {label} bindings must be distinct")
        node_ids = tuple(
            node.node_id.casefold() for pool in self.pools for node in pool.inventory.nodes
        )
        if len(node_ids) != len(set(node_ids)):
            raise ValueError("pool inventory nodes must be distinct across controllers")
        return self


def _pool_binding(
    profile: CapacityPoolExecutorProfile,
    pool_id: Literal["gb10", "oldlab"] | str,
) -> CapacityPoolExecutorBinding:
    if pool_id not in {"gb10", "oldlab"}:
        raise ValueError("pool executor rendering requires gb10 or oldlab")
    return next(pool for pool in profile.pools if pool.pool_id == pool_id)


def _pool_executor_config(
    profile: CapacityPoolExecutorProfile,
    pool: CapacityPoolExecutorBinding,
) -> dict[str, object]:
    return {
        "association": pool.association,
        "authority_incarnation": profile.authority_incarnation,
        "bearer_token_file": pool.bearer_token_file,
        "approved_profiles_sha256": "0" * 64,
        "configuration_epoch": profile.configuration_epoch,
        "controller_authority_sha256": pool.controller_authority_sha256,
        "controller_host": pool.controller_host,
        "execution_epoch": profile.execution_epoch,
        "execution_manifest_sha256": profile.execution_manifest_sha256,
        "executor_image": profile.executor_image,
        "executor_id": pool.executor_id,
        "executor_incarnation": pool.executor_incarnation,
        "journal_file": pool.journal_file,
        "local_authority_sha256": pool.local_authority_sha256,
        "local_uid": pool.local_uid,
        "manager_origin": profile.manager_origin,
        "ownership_key_file": pool.ownership_key_file,
        "partition": pool.partition,
        "pool_generation": pool.pool_generation,
        "pool_id": pool.pool_id,
        "profile_digest": pool.profile_digest,
        "profile_generation": pool.profile_generation,
        "profile_id": pool.profile_id,
        "qos": pool.qos,
        "signing_key_id": pool.signing_key_id,
        "signing_key_sha256": pool.signing_key_sha256,
        "slurm_cluster": pool.slurm_cluster,
        "slurm_executables": pool.slurm_executables.model_dump(),
        "state_directory": pool.state_directory,
        "service_user": profile.service_user,
        "submitter": pool.submitter,
        "tls_ca_file": pool.tls_ca_file,
        "tls_certificate_file": pool.tls_certificate_file,
        "tls_private_key_file": pool.tls_private_key_file,
        "trusted_fleet_release_sha256": profile.trusted_fleet_release_sha256,
        "writer_epoch": profile.writer_epoch,
    }


def capacity_pool_executor_manifest_sha256(
    profile: CapacityPoolExecutorProfile,
    pool_id: Literal["gb10", "oldlab"] | str,
) -> str:
    """Derive the exact production-loader manifest pin without reading secrets."""

    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("capacity pool-executor profile is invalid")
    pool = _pool_binding(profile, pool_id)
    return _capacity_pool_executor_manifest(
        profile,
        pool,
        approved_profiles_sha256="0" * 64,
    ).sha256()


def _capacity_pool_executor_manifest(
    profile: CapacityPoolExecutorProfile,
    pool: CapacityPoolExecutorBinding,
    *,
    approved_profiles_sha256: str,
) -> ImmutablePoolManifest:
    return ImmutablePoolManifest(
        pool_id=pool.pool_id,
        pool_generation=pool.pool_generation,
        controller_authority_sha256=pool.controller_authority_sha256,
        approved_profiles_sha256=approved_profiles_sha256,
        executor_id=pool.executor_id,
        executor_incarnation=UUID(pool.executor_incarnation),
        local_authority_sha256=pool.local_authority_sha256,
        signing_key_id=pool.signing_key_id,
        signing_key_sha256=pool.signing_key_sha256,
        ownership_key_file=Path(pool.ownership_key_file),
        manager_origin=profile.manager_origin,
        bearer_token_file=Path(pool.bearer_token_file),
        tls_ca_file=Path(pool.tls_ca_file),
        tls_certificate_file=Path(pool.tls_certificate_file),
        tls_private_key_file=Path(pool.tls_private_key_file),
        state_directory=Path(pool.state_directory),
        journal_file=Path(pool.journal_file),
        local_uid=pool.local_uid,
        slurm_cluster=pool.slurm_cluster,
        controller_host=pool.controller_host,
        partition=pool.partition,
        association=pool.association,
        submitter=pool.submitter,
        qos=pool.qos,
        profile_id=pool.profile_id,
        profile_generation=pool.profile_generation,
        profile_digest=pool.profile_digest,
        slurm_executables=tuple(
            sorted(
                (name, Path(value)) for name, value in pool.slurm_executables.model_dump().items()
            )
        ),
        executor_image=profile.executor_image,
        service_user=profile.service_user,
    )


def _active_profile_set_digest(
    profile: CapacityPoolExecutorProfile,
    pool: CapacityPoolExecutorBinding,
    profiles: ApprovedLaunchProfileSetV2,
) -> str:
    if not isinstance(profiles, ApprovedLaunchProfileSetV2):
        raise TypeError("approved launch profiles are invalid")
    try:
        digest = canonical_approved_profiles_digest(profiles.profiles)
    except RuntimeAssemblyError as exc:
        raise ValueError("approved launch profile set is invalid") from exc
    for runtime_profile in profiles.profiles:
        if (
            runtime_profile.pool_id != pool.pool_id
            or runtime_profile.pool_generation != pool.pool_generation
            or runtime_profile.controller_authority_sha256 != pool.controller_authority_sha256
            or canonical_launch_policy_digest(runtime_profile)
            != runtime_profile.controller_authority_sha256
            or runtime_profile.slurm_cluster != pool.slurm_cluster
            or runtime_profile.controller_host != pool.controller_host
            or runtime_profile.partition != pool.partition
            or runtime_profile.association != pool.association
            or runtime_profile.submitter != pool.submitter
            or runtime_profile.qos != pool.qos
            or runtime_profile.trusted_launcher_release_sha256
            != profile.trusted_fleet_release_sha256
        ):
            raise ValueError("approved launch profile differs from pool binding")
    if not any(
        runtime_profile.profile_id == pool.profile_id
        and runtime_profile.profile_generation == pool.profile_generation
        and runtime_profile.profile_digest == pool.profile_digest
        for runtime_profile in profiles.profiles
    ):
        raise ValueError("approved launch profile set omits the local profile binding")
    if digest == "0" * 64:
        raise ValueError("approved launch profile set digest is not active")
    return digest


def render_capacity_pool_executor_active_manifest_sha256(
    profile: CapacityPoolExecutorProfile,
    pool_id: Literal["gb10", "oldlab"] | str,
    profiles: ApprovedLaunchProfileSetV2,
) -> str:
    """Render the positive immutable-manifest digest for one reviewed profile set."""

    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("capacity pool-executor profile is invalid")
    pool = _pool_binding(profile, pool_id)
    approved_profiles_sha256 = _active_profile_set_digest(profile, pool, profiles)
    return _capacity_pool_executor_manifest(
        profile,
        pool,
        approved_profiles_sha256=approved_profiles_sha256,
    ).sha256()


def _validate_active_runtime_artifact(
    profile: CapacityPoolExecutorProfile,
    pool: CapacityPoolExecutorBinding,
    artifact: ActivationRuntimeArtifactV2,
) -> str:
    if not isinstance(artifact, ActivationRuntimeArtifactV2):
        raise TypeError("activation runtime artifact is invalid")
    execution = artifact.execution
    if (
        execution.authority_incarnation != UUID(profile.authority_incarnation)
        or execution.writer_epoch != profile.writer_epoch
        or execution.configuration_epoch != profile.configuration_epoch
        or execution.execution_epoch != profile.execution_epoch
        or execution.execution_manifest_sha256 != profile.execution_manifest_sha256
        or execution.execution_state != "active"
        or execution.executable_new_capacity_ceiling <= 0
        or execution.executable_new_capacity_rate_per_minute <= 0
        or execution.trusted_fleet_release_sha256 != profile.trusted_fleet_release_sha256
    ):
        raise ValueError("activation runtime artifact differs from the active execution fence")
    profiles = ApprovedLaunchProfileSetV2(profiles=artifact.profiles)
    approved_profiles_sha256 = _active_profile_set_digest(profile, pool, profiles)
    expected_manifest_sha256 = _capacity_pool_executor_manifest(
        profile,
        pool,
        approved_profiles_sha256=approved_profiles_sha256,
    ).sha256()
    if (
        artifact.pool_id != pool.pool_id
        or artifact.pool_generation != pool.pool_generation
        or artifact.executor_id != pool.executor_id
        or artifact.executor_incarnation != UUID(pool.executor_incarnation)
        or artifact.controller_authority_sha256 != pool.controller_authority_sha256
        or artifact.approved_profiles_sha256 != approved_profiles_sha256
        or artifact.local_authority_sha256 != pool.local_authority_sha256
        or artifact.signing_key_id != pool.signing_key_id
        or artifact.signing_key_sha256 != pool.signing_key_sha256
        or artifact.immutable_manifest_sha256 != expected_manifest_sha256
        or Path(artifact.state_directory) != Path(pool.state_directory)
        or Path(artifact.journal_file) != Path(pool.journal_file)
    ):
        raise ValueError("activation runtime artifact differs from the pool binding")
    slurm = artifact.slurm_authority
    executable_paths = tuple(
        sorted(
            (name, Path(getattr(slurm.executables, name).path))
            for name in ("scontrol", "sacctmgr", "squeue", "sbatch", "scancel", "sacct")
        )
    )
    configured_paths = tuple(
        sorted((name, Path(value)) for name, value in pool.slurm_executables.model_dump().items())
    )
    if (
        slurm.cluster != pool.slurm_cluster
        or slurm.controller_host != pool.controller_host
        or slurm.partition != pool.partition
        or slurm.account != pool.association
        or slurm.submitter != pool.submitter
        or slurm.qos != pool.qos
        or slurm.local_uid != pool.local_uid
        or executable_paths != configured_paths
    ):
        raise ValueError("activation runtime artifact differs from the Slurm binding")
    return expected_manifest_sha256


def render_capacity_pool_executor_active_config(
    profile: CapacityPoolExecutorProfile,
    pool_id: Literal["gb10", "oldlab"] | str,
    artifact: ActivationRuntimeArtifactV2,
) -> str:
    """Render one positive controller-local config bound to an activation artifact."""

    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("capacity pool-executor profile is invalid")
    pool = _pool_binding(profile, pool_id)
    _validate_active_runtime_artifact(profile, pool, artifact)
    value = _pool_executor_config(profile, pool)
    value["approved_profiles_sha256"] = artifact.approved_profiles_sha256
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
    )


def render_capacity_pool_executor_active_service_environment(
    profile: CapacityPoolExecutorProfile,
    pool_id: Literal["gb10", "oldlab"] | str,
    artifact: ActivationRuntimeArtifactV2,
) -> str:
    """Render the non-secret environment for the separately enabled active timer."""

    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("capacity pool-executor profile is invalid")
    pool = _pool_binding(profile, pool_id)
    expected_manifest_sha256 = _validate_active_runtime_artifact(profile, pool, artifact)
    active_config = Path(pool.config_file).with_name(f"{pool.pool_id}-active.json")
    runtime_artifact = Path(pool.config_file).with_name(f"{pool.pool_id}-activation-runtime.json")
    return (
        "LOOM_CAPACITY_EXECUTOR_ACTIVATION_RUNTIME_ARTIFACT="
        f"{runtime_artifact}\n"
        f"LOOM_CAPACITY_EXECUTOR_CONFIG={active_config}\n"
        "LOOM_CAPACITY_EXECUTOR_EXPECTED_MANIFEST_SHA256="
        f"{expected_manifest_sha256}\n"
        f"LOOM_CAPACITY_EXECUTOR_POOL={pool.pool_id}\n"
    )


def load_capacity_control_plane_profile(path: Path) -> CapacityControlPlaneProfile:
    """Load one strict non-secret infrastructure profile."""

    return CapacityControlPlaneProfile.model_validate(
        tomllib.loads(path.read_text(encoding="utf-8"))
    )


def load_capacity_pool_executor_profile(path: Path) -> CapacityPoolExecutorProfile:
    """Load one strict, non-secret, permanently inert pool-executor profile."""

    return CapacityPoolExecutorProfile.model_validate(
        tomllib.loads(path.read_text(encoding="utf-8"))
    )


def render_capacity_pool_executor_configs(
    profile: CapacityPoolExecutorProfile,
) -> dict[str, str]:
    """Render complete deterministic controller-local production configurations."""

    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("capacity pool-executor profile is invalid")
    return {
        pool.pool_id: json.dumps(
            _pool_executor_config(profile, pool),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        )
        + "\n"
        for pool in profile.pools
    }


def render_capacity_pool_inventory_policies(
    profile: CapacityPoolExecutorProfile,
) -> dict[str, str]:
    """Render canonical controller-local read-only inventory policies."""

    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("capacity pool-executor profile is invalid")
    return {
        pool.pool_id: canonical_slurm_inventory_policy_bytes(pool.inventory).decode("ascii") + "\n"
        for pool in profile.pools
    }


def render_capacity_pool_executor_service_environment(
    profile: CapacityPoolExecutorProfile,
    pool_id: Literal["gb10", "oldlab"] | str,
) -> str:
    """Render the non-secret environment consumed by the checked-in systemd unit."""

    if not isinstance(profile, CapacityPoolExecutorProfile):
        raise TypeError("capacity pool-executor profile is invalid")
    pool = _pool_binding(profile, pool_id)
    inventory_policy = canonical_slurm_inventory_policy_bytes(pool.inventory).decode("ascii") + "\n"
    inventory_policy_sha256 = hashlib.sha256(inventory_policy.encode("ascii")).hexdigest()
    inventory_policy_file = Path(pool.config_file).with_name(
        f"{pool.pool_id}-inventory-policy.json"
    )
    return (
        f"LOOM_CAPACITY_EXECUTOR_CONFIG={pool.config_file}\n"
        "LOOM_CAPACITY_EXECUTOR_EXECUTABLE_CEILING=0\n"
        "LOOM_CAPACITY_EXECUTOR_EXPECTED_INVENTORY_POLICY_SHA256="
        f"{inventory_policy_sha256}\n"
        "LOOM_CAPACITY_EXECUTOR_EXPECTED_MANIFEST_SHA256="
        f"{capacity_pool_executor_manifest_sha256(profile, pool.pool_id)}\n"
        f"LOOM_CAPACITY_EXECUTOR_INVENTORY_POLICY={inventory_policy_file}\n"
        f"LOOM_CAPACITY_EXECUTOR_POOL={pool.pool_id}\n"
    )


_MANAGED_LABELS = {
    "app.kubernetes.io/managed-by": "loom-capacity-control-plane",
    "app.kubernetes.io/part-of": "loom",
}
_COMPONENT_LABEL = "loom.yylx.dev/capacity-component"
_RUNTIME_ROOT = "/var/run/loom-capacity-manager"
_CREDENTIALS = f"{_RUNTIME_ROOT}/runtime/credentials"
_MANAGER_CREDENTIAL_FILES = (
    "client-ca.pem",
    "database-url",
    "global-execution-signing-key",
    "health-certificate.pem",
    "health-private-key.pem",
    "ownership-public-keys.json",
    "principals.json",
    "server-ca.pem",
    "server-certificate.pem",
    "server-private-key.pem",
)


def _metadata(name: str, *, namespace: bool = True) -> dict[str, Any]:
    value: dict[str, Any] = {"name": name, "labels": dict(_MANAGED_LABELS)}
    if namespace:
        value["namespace"] = "loom-dev"
    return value


def _pod_labels(name: str, component: str) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": name,
        "app.kubernetes.io/part-of": "loom",
        _COMPONENT_LABEL: component,
    }


def _container_security(*, read_only_root: bool) -> dict[str, Any]:
    return {
        "allowPrivilegeEscalation": False,
        "capabilities": {"drop": ["ALL"]},
        "readOnlyRootFilesystem": read_only_root,
    }


def _pod_security(user: int) -> dict[str, Any]:
    return {
        "runAsNonRoot": True,
        "runAsUser": user,
        "runAsGroup": user,
        "fsGroup": user,
        "fsGroupChangePolicy": "OnRootMismatch",
        "seccompProfile": {"type": "RuntimeDefault"},
    }


def _credential_parts(
    *,
    manager_image: str,
    secret_name: str,
    profile: Literal["manager", "migration"],
    resources: ResourceEnvelope,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    files = _MANAGER_CREDENTIAL_FILES if profile == "manager" else ("database-url",)
    init_mounts: list[dict[str, Any]] = [
        {
            "name": "projected",
            "mountPath": f"{_RUNTIME_ROOT}/projected",
            "readOnly": True,
        },
        {"name": "runtime", "mountPath": f"{_RUNTIME_ROOT}/runtime"},
    ]
    application_mounts: list[dict[str, Any]] = [
        {
            "name": "runtime",
            "mountPath": f"{_RUNTIME_ROOT}/runtime",
            "readOnly": True,
        }
    ]
    init: dict[str, Any] = {
        "name": "prepare-credentials",
        "image": manager_image,
        "imagePullPolicy": "IfNotPresent",
        "command": ["python", "-m", "loom_capacity_manager.secret_init"],
        "args": [
            "--profile",
            profile,
            "--source",
            f"{_RUNTIME_ROOT}/projected",
            "--destination",
            _CREDENTIALS,
        ],
        "securityContext": _container_security(read_only_root=True),
        "resources": resources.kubernetes(),
        "volumeMounts": init_mounts,
    }
    volumes: list[dict[str, Any]] = [
        {
            "name": "projected",
            "secret": {
                "secretName": secret_name,
                "defaultMode": 0o440,
                "items": [{"key": filename, "path": filename} for filename in files],
            },
        },
        {
            "name": "runtime",
            "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"},
        },
    ]
    return [init], application_mounts, volumes


def _postgres_service() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata("loom-capacity-postgres"),
        "spec": {
            "clusterIP": "None",
            "selector": {"app.kubernetes.io/name": "loom-capacity-postgres"},
            "ports": [
                {
                    "name": "postgres",
                    "protocol": "TCP",
                    "port": 5432,
                    "targetPort": 5432,
                }
            ],
        },
    }


def _postgres_statefulset(profile: CapacityControlPlaneProfile) -> dict[str, Any]:
    labels = _pod_labels("loom-capacity-postgres", "database")
    secret_env = [
        {
            "name": environment,
            "valueFrom": {"secretKeyRef": {"name": profile.secret_name, "key": secret_key}},
        }
        for environment, secret_key in (
            ("POSTGRES_USER", "postgres-user"),
            ("POSTGRES_PASSWORD", "postgres-password"),
            ("POSTGRES_DB", "postgres-database"),
        )
    ]
    claim_spec: dict[str, Any] = {
        "accessModes": ["ReadWriteOnce"],
        "resources": {"requests": {"storage": profile.postgres_storage}},
    }
    if profile.storage_class_name is not None:
        claim_spec["storageClassName"] = profile.storage_class_name
    return {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": _metadata("loom-capacity-postgres"),
        "spec": {
            "serviceName": "loom-capacity-postgres",
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "securityContext": _pod_security(999),
                    "containers": [
                        {
                            "name": "postgres",
                            "image": profile.postgres_image,
                            "imagePullPolicy": "IfNotPresent",
                            "args": ["-c", "max_connections=200"],
                            "env": [
                                *secret_env,
                                {
                                    "name": "PGDATA",
                                    "value": "/var/lib/postgresql/data/pgdata",
                                },
                            ],
                            "ports": [{"name": "postgres", "containerPort": 5432}],
                            "readinessProbe": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-ec",
                                        'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
                                    ]
                                },
                                "periodSeconds": 5,
                                "failureThreshold": 12,
                                "timeoutSeconds": 3,
                            },
                            "startupProbe": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-ec",
                                        'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
                                    ]
                                },
                                "periodSeconds": 5,
                                "failureThreshold": 120,
                                "timeoutSeconds": 3,
                            },
                            "livenessProbe": {
                                "exec": {
                                    "command": [
                                        "/bin/sh",
                                        "-ec",
                                        'exec pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB"',
                                    ]
                                },
                                "periodSeconds": 10,
                                "failureThreshold": 6,
                                "timeoutSeconds": 3,
                            },
                            "securityContext": _container_security(read_only_root=True),
                            "resources": profile.postgres_resources.kubernetes(),
                            "volumeMounts": [
                                {
                                    "name": "data",
                                    "mountPath": "/var/lib/postgresql/data",
                                },
                                {"name": "run", "mountPath": "/var/run/postgresql"},
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {"name": "run", "emptyDir": {"medium": "Memory"}},
                        {"name": "tmp", "emptyDir": {"medium": "Memory"}},
                    ],
                },
            },
            "volumeClaimTemplates": [{"metadata": {"name": "data"}, "spec": claim_spec}],
        },
    }


def _migration_job(
    profile: CapacityControlPlaneProfile,
    *,
    manager_image: str,
    authority_incarnation: UUID,
    migration_head: str,
    image_digest: str,
) -> dict[str, Any]:
    labels = _pod_labels("loom-capacity-migrate", "migration")
    init, mounts, volumes = _credential_parts(
        manager_image=manager_image,
        secret_name=profile.secret_name,
        profile="migration",
        resources=profile.migration_resources,
    )
    job_spec: dict[str, Any] = {
        "activeDeadlineSeconds": 900,
        "backoffLimit": 6,
        "template": {
            "metadata": {"labels": labels},
            "spec": {
                "automountServiceAccountToken": False,
                "restartPolicy": "Never",
                "securityContext": _pod_security(65532),
                "initContainers": init,
                "containers": [
                    {
                        "name": "migration",
                        "image": manager_image,
                        "imagePullPolicy": "IfNotPresent",
                        "command": ["python", "-m", "loom_capacity_manager.migrate"],
                        "args": [
                            "--db-url-file",
                            f"{_CREDENTIALS}/database-url",
                            "--expected-authority-incarnation",
                            str(authority_incarnation),
                        ],
                        "securityContext": _container_security(read_only_root=True),
                        "resources": profile.migration_resources.kubernetes(),
                        "volumeMounts": mounts,
                    }
                ],
                "volumes": volumes,
            },
        },
    }
    template_identity = hashlib.sha256(
        json.dumps(
            {"migration_head": migration_head, "spec": job_spec},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    head_slug = re.sub(
        r"[^a-z0-9-]+",
        "-",
        migration_head.lower().replace("_", "-"),
    ).strip("-")
    head_prefix = head_slug[:19].rstrip("-") or "migration"
    name = f"loom-capacity-migrate-{head_prefix}-{image_digest[:10]}-{template_identity[:10]}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": _metadata(name),
        "spec": job_spec,
    }


def _manager_service() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _metadata("loom-capacity-manager"),
        "spec": {
            "type": "ClusterIP",
            "selector": {"app.kubernetes.io/name": "loom-capacity-manager"},
            "ports": [
                {
                    "name": "https",
                    "protocol": "TCP",
                    "port": 8443,
                    "targetPort": 8443,
                }
            ],
        },
    }


def _router_metadata(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "namespace": _MANAGER_ROUTER_NAMESPACE,
        "labels": dict(_MANAGED_LABELS),
    }


def _manager_router_namespace() -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": _MANAGER_ROUTER_NAMESPACE,
            "labels": {
                **_MANAGED_LABELS,
                "pod-security.kubernetes.io/enforce": "privileged",
                "pod-security.kubernetes.io/audit": "restricted",
                "pod-security.kubernetes.io/audit-version": "latest",
                "pod-security.kubernetes.io/warn": "restricted",
                "pod-security.kubernetes.io/warn-version": "latest",
            },
        },
    }


def _manager_router_deployment(
    profile: CapacityControlPlaneProfile,
    *,
    manager_image: str,
    external_manager_client_cidrs: tuple[str, ...],
) -> dict[str, Any]:
    labels = _pod_labels(_MANAGER_ROUTER_NAME, "router")
    allowed_client_arguments = [
        item
        for cidr in external_manager_client_cidrs
        for item in (
            "--allowed-client-ip",
            ipaddress.ip_network(cidr).network_address.compressed,
        )
    ]
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _router_metadata(_MANAGER_ROUTER_NAME),
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 2,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "nodeSelector": {"kubernetes.io/hostname": _MANAGER_ROUTER_NODE},
                    "securityContext": _pod_security(65532),
                    "containers": [
                        {
                            "name": "router",
                            "image": manager_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "python",
                                "-m",
                                "loom_capacity_manager.tcp_proxy",
                            ],
                            "args": allowed_client_arguments,
                            "ports": [
                                {
                                    "name": "manager-tls",
                                    "containerPort": _MANAGER_ROUTER_PORT,
                                    "hostPort": _MANAGER_ROUTER_PORT,
                                    "hostIP": _MANAGER_ROUTER_HOST,
                                    "protocol": "TCP",
                                }
                            ],
                            "startupProbe": {
                                "tcpSocket": {"port": _MANAGER_ROUTER_PORT},
                                "periodSeconds": 2,
                                "failureThreshold": 30,
                                "timeoutSeconds": 1,
                            },
                            "readinessProbe": {
                                "tcpSocket": {"port": _MANAGER_ROUTER_PORT},
                                "periodSeconds": 5,
                                "failureThreshold": 3,
                                "timeoutSeconds": 1,
                            },
                            "livenessProbe": {
                                "tcpSocket": {"port": _MANAGER_ROUTER_PORT},
                                "periodSeconds": 10,
                                "failureThreshold": 3,
                                "timeoutSeconds": 1,
                            },
                            "securityContext": _container_security(read_only_root=True),
                            "resources": profile.manager_resources.kubernetes(),
                        }
                    ],
                },
            },
        },
    }


def _manager_deployment(
    profile: CapacityControlPlaneProfile,
    *,
    manager_image: str,
    authority_incarnation: UUID,
    execution_policy_config_map: str | None = None,
    execution_policy_sha256: str | None = None,
) -> dict[str, Any]:
    labels = _pod_labels("loom-capacity-manager", "manager")
    init, mounts, volumes = _credential_parts(
        manager_image=manager_image,
        secret_name=profile.secret_name,
        profile="manager",
        resources=profile.manager_resources,
    )
    health_command = [
        "python",
        "-m",
        "loom_capacity_manager.health_probe",
        "--url",
        "https://127.0.0.1:8443/healthz",
        "--ca-file",
        f"{_CREDENTIALS}/server-ca.pem",
        "--certificate-file",
        f"{_CREDENTIALS}/health-certificate.pem",
        "--private-key-file",
        f"{_CREDENTIALS}/health-private-key.pem",
        "--server-certificate-file",
        f"{_CREDENTIALS}/server-certificate.pem",
        "--allow-positive-ceiling",
    ]
    if execution_policy_config_map is not None:
        health_command.extend(["--required-server-ip-san", _MANAGER_ROUTER_HOST])
    environment = [
        {"name": name, "value": value}
        for name, value in (
            ("LOOM_CAPACITY_PRINCIPALS_FILE", f"{_CREDENTIALS}/principals.json"),
            ("LOOM_CAPACITY_DB_URL_FILE", f"{_CREDENTIALS}/database-url"),
            (
                "LOOM_CAPACITY_EXPECTED_AUTHORITY_INCARNATION",
                str(authority_incarnation),
            ),
            (
                "LOOM_CAPACITY_GLOBAL_EXECUTION_SIGNING_KEY_FILE",
                f"{_CREDENTIALS}/global-execution-signing-key",
            ),
            (
                "LOOM_CAPACITY_GLOBAL_EXECUTION_SIGNING_KEY_ID",
                "global-capacity-manager-2026-08",
            ),
            (
                "LOOM_CAPACITY_TLS_CERT_FILE",
                f"{_CREDENTIALS}/server-certificate.pem",
            ),
            ("LOOM_CAPACITY_TLS_KEY_FILE", f"{_CREDENTIALS}/server-private-key.pem"),
            ("LOOM_CAPACITY_TLS_CLIENT_CA_FILE", f"{_CREDENTIALS}/client-ca.pem"),
            (
                "LOOM_CAPACITY_OWNERSHIP_PUBLIC_KEYS_FILE",
                f"{_CREDENTIALS}/ownership-public-keys.json",
            ),
            ("LOOM_CAPACITY_HOST", "0.0.0.0"),
            ("LOOM_CAPACITY_PORT", "8443"),
        )
    ]
    if execution_policy_config_map is not None:
        assert execution_policy_sha256 is not None
        environment.extend(
            [
                {
                    "name": "LOOM_CAPACITY_EXECUTION_POLICY_FILE",
                    "value": _EXECUTION_POLICY_PATH,
                },
                {
                    "name": "LOOM_CAPACITY_EXECUTION_POLICY_SHA256",
                    "value": execution_policy_sha256,
                },
            ]
        )
        init.append(
            {
                "name": "execution-policy-init",
                "image": manager_image,
                "imagePullPolicy": "IfNotPresent",
                "command": ["python", "-m", "loom_capacity_manager.secret_init"],
                "args": [
                    "--profile",
                    "execution-policy",
                    "--source",
                    f"{_RUNTIME_ROOT}/projected-policy",
                    "--destination",
                    f"{_RUNTIME_ROOT}/runtime-policy/execution-policy",
                ],
                "securityContext": _container_security(read_only_root=True),
                "resources": profile.manager_resources.kubernetes(),
                "volumeMounts": [
                    {
                        "name": _EXECUTION_POLICY_PROJECTED_VOLUME,
                        "mountPath": f"{_RUNTIME_ROOT}/projected-policy",
                        "readOnly": True,
                    },
                    {
                        "name": _EXECUTION_POLICY_RUNTIME_VOLUME,
                        "mountPath": f"{_RUNTIME_ROOT}/runtime-policy",
                    },
                ],
            }
        )
        mounts.append(
            {
                "name": _EXECUTION_POLICY_RUNTIME_VOLUME,
                "mountPath": _EXECUTION_POLICY_DIRECTORY,
                "subPath": "execution-policy",
                "readOnly": True,
            }
        )
        volumes.extend(
            [
                {
                    "name": _EXECUTION_POLICY_PROJECTED_VOLUME,
                    "configMap": {
                        "name": execution_policy_config_map,
                        "defaultMode": 0o444,
                        "items": [
                            {
                                "key": _EXECUTION_POLICY_FILENAME,
                                "path": _EXECUTION_POLICY_FILENAME,
                            }
                        ],
                    },
                },
                {
                    "name": _EXECUTION_POLICY_RUNTIME_VOLUME,
                    "emptyDir": {"medium": "Memory", "sizeLimit": "2Mi"},
                },
            ]
        )
    volumes.append(
        {
            "name": "witness-api",
            "projected": {
                "defaultMode": 0o440,
                "sources": [
                    {
                        "serviceAccountToken": {
                            "expirationSeconds": 3600,
                            "path": "token",
                        }
                    },
                    {
                        "configMap": {
                            "name": "kube-root-ca.crt",
                            "items": [{"key": "ca.crt", "path": "ca.crt"}],
                        }
                    },
                ],
            },
        }
    )
    publisher_environment = [
        {"name": name, "value": value}
        for name, value in (
            ("LOOM_CAPACITY_WITNESS_DB_URL_FILE", f"{_CREDENTIALS}/database-url"),
            (
                "LOOM_CAPACITY_WITNESS_EXPECTED_AUTHORITY_INCARNATION",
                str(authority_incarnation),
            ),
            (
                "LOOM_CAPACITY_WITNESS_KUBERNETES_API_SERVER",
                _KUBERNETES_API_SERVER,
            ),
            (
                "LOOM_CAPACITY_WITNESS_KUBERNETES_CA_FILE",
                f"{_WITNESS_API_DIRECTORY}/ca.crt",
            ),
            (
                "LOOM_CAPACITY_WITNESS_KUBERNETES_TOKEN_FILE",
                f"{_WITNESS_API_DIRECTORY}/token",
            ),
            (
                "LOOM_CAPACITY_WITNESS_SIGNING_KEY_FILE",
                f"{_CREDENTIALS}/global-execution-signing-key",
            ),
            (
                "LOOM_CAPACITY_WITNESS_SIGNING_KEY_ID",
                "global-capacity-manager-2026-08",
            ),
        )
    ]
    publisher_mounts = [
        {
            "name": "runtime",
            "mountPath": f"{_CREDENTIALS}/database-url",
            "subPath": "credentials/database-url",
            "readOnly": True,
        },
        {
            "name": "runtime",
            "mountPath": f"{_CREDENTIALS}/global-execution-signing-key",
            "subPath": "credentials/global-execution-signing-key",
            "readOnly": True,
        },
        {
            "name": "witness-api",
            "mountPath": _WITNESS_API_DIRECTORY,
            "readOnly": True,
        },
    ]
    return {
        "apiVersion": "apps/v1",
        "kind": "Deployment",
        "metadata": _metadata("loom-capacity-manager"),
        "spec": {
            "replicas": 1,
            "revisionHistoryLimit": 2,
            "strategy": {"type": "Recreate"},
            "selector": {"matchLabels": labels},
            "template": {
                "metadata": {"labels": labels},
                "spec": {
                    "automountServiceAccountToken": False,
                    "serviceAccountName": _WITNESS_PUBLISHER,
                    "securityContext": _pod_security(65532),
                    "initContainers": init,
                    "containers": [
                        {
                            "name": "manager",
                            "image": manager_image,
                            "imagePullPolicy": "IfNotPresent",
                            "env": environment,
                            "ports": [{"name": "https", "containerPort": 8443}],
                            "startupProbe": {
                                "exec": {"command": health_command},
                                "periodSeconds": 5,
                                "failureThreshold": 60,
                                "timeoutSeconds": 4,
                            },
                            "readinessProbe": {
                                "exec": {"command": health_command},
                                "periodSeconds": 5,
                                "failureThreshold": 3,
                                "timeoutSeconds": 4,
                            },
                            "livenessProbe": {
                                "tcpSocket": {"port": 8443},
                                "periodSeconds": 10,
                                "failureThreshold": 3,
                                "timeoutSeconds": 3,
                            },
                            "securityContext": _container_security(read_only_root=True),
                            "resources": profile.manager_resources.kubernetes(),
                            "volumeMounts": mounts,
                        },
                        {
                            "name": "witness-publisher",
                            "image": manager_image,
                            "imagePullPolicy": "IfNotPresent",
                            "command": [
                                "python",
                                "-m",
                                "loom_capacity_manager.global_execution_witness_publisher",
                            ],
                            "env": publisher_environment,
                            "securityContext": _container_security(read_only_root=True),
                            "resources": profile.manager_resources.kubernetes(),
                            "volumeMounts": publisher_mounts,
                        },
                    ],
                    "volumes": volumes,
                },
            },
        },
    }


def _witness_publication_documents() -> list[dict[str, Any]]:
    return [
        {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": _metadata(_WITNESS_PUBLISHER),
            "automountServiceAccountToken": False,
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": _metadata(_WITNESS_PUBLISHER),
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["configmaps"],
                    "resourceNames": [_WITNESS_CONFIG_MAP],
                    "verbs": ["get", "patch"],
                }
            ],
        },
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": _metadata(_WITNESS_PUBLISHER),
            "subjects": [
                {
                    "kind": "ServiceAccount",
                    "name": _WITNESS_PUBLISHER,
                    "namespace": "loom-dev",
                }
            ],
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "Role",
                "name": _WITNESS_PUBLISHER,
            },
        },
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": _metadata(_WITNESS_CONFIG_MAP),
            "data": {"gb10.json": "", "oldlab.json": ""},
        },
    ]


def _component_selector(*components: str) -> dict[str, Any]:
    return {
        "matchExpressions": [
            {"key": _COMPONENT_LABEL, "operator": "In", "values": list(components)}
        ]
    }


def _network_policies(
    profile: CapacityControlPlaneProfile,
    *,
    external_manager_client_cidrs: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    namespace = {
        "namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": profile.namespace}}
    }
    return [
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-default-deny"),
            "spec": {
                "podSelector": {
                    "matchExpressions": [{"key": _COMPONENT_LABEL, "operator": "Exists"}]
                },
                "policyTypes": ["Ingress", "Egress"],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-dns-egress"),
            "spec": {
                "podSelector": _component_selector("manager", "migration"),
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": profile.dns.namespace
                                    }
                                },
                                "podSelector": {"matchLabels": profile.dns.match_labels()},
                            }
                        ],
                        "ports": [
                            {"protocol": "UDP", "port": profile.dns.port},
                            {"protocol": "TCP", "port": profile.dns.port},
                        ],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-database-egress"),
            "spec": {
                "podSelector": _component_selector("manager", "migration"),
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                **namespace,
                                "podSelector": {
                                    "matchLabels": {
                                        "app.kubernetes.io/name": "loom-capacity-postgres"
                                    }
                                },
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 5432}],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-witness-publisher-api-egress"),
            "spec": {
                "podSelector": {"matchLabels": {"app.kubernetes.io/name": "loom-capacity-manager"}},
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [{"ipBlock": {"cidr": _KUBERNETES_API_SERVER_CIDR}}],
                        "ports": [{"protocol": "TCP", "port": _KUBERNETES_API_SERVER_PORT}],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-postgres-ingress"),
            "spec": {
                "podSelector": {
                    "matchLabels": {"app.kubernetes.io/name": "loom-capacity-postgres"}
                },
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [{"podSelector": _component_selector("manager", "migration")}],
                        "ports": [{"protocol": "TCP", "port": 5432}],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _metadata("capacity-manager-ingress"),
            "spec": {
                "podSelector": {"matchLabels": {"app.kubernetes.io/name": "loom-capacity-manager"}},
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [
                            {
                                "namespaceSelector": {},
                                "podSelector": {
                                    "matchLabels": profile.capacity_agent_client.match_labels()
                                },
                            },
                            {
                                "podSelector": {
                                    "matchLabels": profile.lifecycle_client.match_labels()
                                }
                            },
                            {
                                "podSelector": {
                                    "matchLabels": {"app": "loom-personal-dev-management"}
                                }
                            },
                            *(
                                (
                                    {
                                        "namespaceSelector": {
                                            "matchLabels": {
                                                "kubernetes.io/metadata.name": (
                                                    _MANAGER_ROUTER_NAMESPACE
                                                )
                                            }
                                        },
                                        "podSelector": {
                                            "matchLabels": {
                                                "app.kubernetes.io/name": (_MANAGER_ROUTER_NAME)
                                            }
                                        },
                                    },
                                )
                                if external_manager_client_cidrs
                                else ()
                            ),
                        ],
                        "ports": [{"protocol": "TCP", "port": 8443}],
                    }
                ],
            },
        },
    ]


def _manager_router_network_policies(
    profile: CapacityControlPlaneProfile,
    *,
    external_manager_client_cidrs: tuple[str, ...],
) -> list[dict[str, Any]]:
    router_selector = {"matchLabels": {"app.kubernetes.io/name": _MANAGER_ROUTER_NAME}}
    return [
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _router_metadata("capacity-manager-router-default-deny"),
            "spec": {
                "podSelector": {},
                "policyTypes": ["Ingress", "Egress"],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _router_metadata("capacity-manager-router-ingress"),
            "spec": {
                "podSelector": router_selector,
                "policyTypes": ["Ingress"],
                "ingress": [
                    {
                        "from": [
                            {"ipBlock": {"cidr": cidr}} for cidr in external_manager_client_cidrs
                        ],
                        "ports": [{"protocol": "TCP", "port": _MANAGER_ROUTER_PORT}],
                    }
                ],
            },
        },
        {
            "apiVersion": "networking.k8s.io/v1",
            "kind": "NetworkPolicy",
            "metadata": _router_metadata("capacity-manager-router-egress"),
            "spec": {
                "podSelector": router_selector,
                "policyTypes": ["Egress"],
                "egress": [
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": profile.namespace
                                    }
                                },
                                "podSelector": {
                                    "matchLabels": {
                                        "app.kubernetes.io/name": "loom-capacity-manager"
                                    }
                                },
                            }
                        ],
                        "ports": [{"protocol": "TCP", "port": 8443}],
                    },
                    {
                        "to": [
                            {
                                "namespaceSelector": {
                                    "matchLabels": {
                                        "kubernetes.io/metadata.name": profile.dns.namespace
                                    }
                                },
                                "podSelector": {"matchLabels": profile.dns.match_labels()},
                            }
                        ],
                        "ports": [
                            {"protocol": "UDP", "port": profile.dns.port},
                            {"protocol": "TCP", "port": profile.dns.port},
                        ],
                    },
                ],
            },
        },
    ]


def render_capacity_control_plane_manifests(
    profile: CapacityControlPlaneProfile,
    *,
    manager_image: str,
    authority_incarnation: UUID,
    execution_policy: ExecutionPreparationPolicyV2 | None = None,
    execution_policy_sha256: str | None = None,
    external_manager_client_cidrs: tuple[str, ...] = (),
) -> str:
    """Render one exact, zero-execution authority release."""

    if not isinstance(profile, CapacityControlPlaneProfile):
        raise TypeError("capacity control-plane profile is invalid")
    if not _is_immutable_oci_reference(manager_image):
        raise ValueError("capacity manager image must be an immutable OCI reference")
    if not isinstance(authority_incarnation, UUID):
        raise TypeError("capacity authority incarnation must be a UUID")
    if authority_incarnation.int == 0:
        raise ValueError("capacity authority incarnation must be non-nil")
    if (execution_policy is None) != (execution_policy_sha256 is None):
        raise ValueError("execution policy and digest must be supplied together")
    if not isinstance(external_manager_client_cidrs, tuple):
        raise TypeError("external manager client CIDRs must be a tuple")
    if (execution_policy is not None) != bool(external_manager_client_cidrs):
        raise ValueError(
            "execution policy and external manager client CIDRs must be supplied together"
        )
    if len(external_manager_client_cidrs) > 8:
        raise ValueError("external manager client CIDRs exceed the fixed bound")
    canonical_external_cidrs: list[str] = []
    for value in external_manager_client_cidrs:
        if not isinstance(value, str):
            raise TypeError("external manager client CIDRs must be strings")
        try:
            network = ipaddress.ip_network(value, strict=True)
        except ValueError as exc:
            raise ValueError("external manager client CIDRs must be canonical host routes") from exc
        address = network.network_address
        private_address = (
            any(address in private for private in _PRIVATE_IPV4_NETWORKS)
            if isinstance(address, ipaddress.IPv4Address)
            else address in _PRIVATE_IPV6_NETWORK
        )
        if (
            network.prefixlen != network.max_prefixlen
            or str(network) != value
            or not private_address
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address.is_reserved
            or address.is_unspecified
        ):
            raise ValueError("external manager client CIDRs must be safe canonical host routes")
        canonical_external_cidrs.append(value)
    if canonical_external_cidrs != sorted(set(canonical_external_cidrs)):
        raise ValueError("external manager client CIDRs must be unique and canonical")
    execution_policy_document: dict[str, Any] | None = None
    execution_policy_config_map: str | None = None
    if execution_policy is not None:
        if not isinstance(execution_policy, ExecutionPreparationPolicyV2):
            raise TypeError("execution policy must be ExecutionPreparationPolicyV2")
        if not isinstance(execution_policy_sha256, str) or (
            re.fullmatch(r"[0-9a-f]{64}", execution_policy_sha256) is None
            or execution_policy_sha256 == "0" * 64
            or execution_policy_sha256 != canonical_executable_digest(execution_policy)
        ):
            raise ValueError("execution policy digest differs from its canonical payload")
        policy_bytes = canonical_executable_bytes(execution_policy)
        if len(policy_bytes) > _MAX_CONFIGMAP_POLICY_BYTES:
            raise ValueError("execution policy exceeds the ConfigMap byte bound")
        policy_payload = policy_bytes.decode("ascii")
        execution_policy_config_map = (
            f"loom-capacity-execution-policy-{execution_policy_sha256[:32]}"
        )
        metadata = _metadata(execution_policy_config_map)
        metadata["annotations"] = {"loom.yylx.dev/execution-policy-sha256": execution_policy_sha256}
        execution_policy_document = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": metadata,
            "immutable": True,
            "data": {_EXECUTION_POLICY_FILENAME: policy_payload},
        }
    image_digest = manager_image.rsplit("@sha256:", 1)[1]
    migration_head = _capacity_head()
    router_enabled = execution_policy_document is not None
    documents = [
        {
            "apiVersion": "v1",
            "kind": "Namespace",
            "metadata": {
                **_metadata("loom-dev", namespace=False),
                "labels": {
                    **_MANAGED_LABELS,
                    "app.kubernetes.io/managed-by": "loom-operator",
                    "pod-security.kubernetes.io/enforce": "restricted",
                    "pod-security.kubernetes.io/enforce-version": "latest",
                    "pod-security.kubernetes.io/audit": "restricted",
                    "pod-security.kubernetes.io/audit-version": "latest",
                    "pod-security.kubernetes.io/warn": "restricted",
                    "pod-security.kubernetes.io/warn-version": "latest",
                },
            },
        },
        *((_manager_router_namespace(),) if router_enabled else ()),
        *_witness_publication_documents(),
        *(() if execution_policy_document is None else (execution_policy_document,)),
        _postgres_service(),
        _postgres_statefulset(profile),
        _migration_job(
            profile,
            manager_image=manager_image,
            authority_incarnation=authority_incarnation,
            migration_head=migration_head,
            image_digest=image_digest,
        ),
        _manager_service(),
        _manager_deployment(
            profile,
            manager_image=manager_image,
            authority_incarnation=authority_incarnation,
            execution_policy_config_map=execution_policy_config_map,
            execution_policy_sha256=execution_policy_sha256,
        ),
        *(
            (
                _manager_router_deployment(
                    profile,
                    manager_image=manager_image,
                    external_manager_client_cidrs=tuple(canonical_external_cidrs),
                ),
            )
            if router_enabled
            else ()
        ),
        *_network_policies(
            profile,
            external_manager_client_cidrs=tuple(canonical_external_cidrs),
        ),
        *(
            _manager_router_network_policies(
                profile,
                external_manager_client_cidrs=tuple(canonical_external_cidrs),
            )
            if router_enabled
            else ()
        ),
    ]
    return cast(
        str,
        yaml.safe_dump_all(documents, sort_keys=False, explicit_start=False),
    )


__all__ = [
    "CapacityControlPlaneProfile",
    "CapacityPoolExecutorBinding",
    "CapacityPoolExecutorProfile",
    "CapacityPoolSlurmExecutables",
    "KubernetesEndpointSelector",
    "PodSelector",
    "ResourceEnvelope",
    "capacity_pool_executor_manifest_sha256",
    "load_capacity_control_plane_profile",
    "load_capacity_pool_executor_profile",
    "render_capacity_control_plane_manifests",
    "render_capacity_pool_executor_active_config",
    "render_capacity_pool_executor_active_manifest_sha256",
    "render_capacity_pool_executor_active_service_environment",
    "render_capacity_pool_executor_configs",
    "render_capacity_pool_executor_service_environment",
    "render_capacity_pool_inventory_policies",
]
