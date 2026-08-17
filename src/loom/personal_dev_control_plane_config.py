"""Strict non-secret profile and trusted release for personal-dev infrastructure."""

from __future__ import annotations

import hashlib
import hmac
import ipaddress
import json
import os
import re
import stat
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

NAMESPACE = "loom-dev"
PERSONAL_NAMESPACE_PREFIX = "loom-dev-"
REQUIRED_POOLS = {"oldlab": "x86_64", "gb10": "arm64"}
REQUIRED_IMAGE_KEYS = {
    "loom_service",
    "personal_dev_builder",
    "personal_dev_activation_agent",
    "postgres",
    "minio",
    "minio_client",
}
MAX_TRUSTED_RELEASE_BYTES = 1024 * 1024

_DNS_LABEL = re.compile(r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?")
_DNS_SUBDOMAIN = re.compile(
    r"[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*"
)
_LABEL_KEY = re.compile(
    r"(?:[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?"
    r"(?:\.[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?)*/)?"
    r"[A-Za-z0-9](?:[-A-Za-z0-9_.]{0,61}[A-Za-z0-9])?"
)
_DIGEST = re.compile(r"[0-9a-f]{64}")
_GIT_IDENTITY = re.compile(r"[0-9a-f]{40}")
_STORAGE = re.compile(r"[1-9][0-9]*(?:Mi|Gi)")
_CPU = re.compile(r"(?:[1-9][0-9]*m|[1-9][0-9]*)")
_REGISTRY_PREFIX = re.compile(
    r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[1-9][0-9]{0,4})?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+"
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _PoolInput(_StrictModel):
    pool_id: str
    architecture: Literal["x86_64", "arm64"]

    @field_validator("pool_id")
    @classmethod
    def _pool_id_is_safe(cls, value: str) -> str:
        if _DNS_LABEL.fullmatch(value) is None:
            raise ValueError("pool identity is invalid")
        return value


class _IdentitiesInput(_StrictModel):
    management_secret: str
    activation_public_secret: str
    activation_private_secret: str
    management_service_account: str
    builder_service_account: str
    activation_service_account: str
    management_service: str
    management_ingress: str
    scanner_cache_pvc: str

    @field_validator("*")
    @classmethod
    def _identity_is_safe(cls, value: str) -> str:
        if _DNS_LABEL.fullmatch(value) is None:
            raise ValueError("Kubernetes identity is invalid")
        return value

    @model_validator(mode="after")
    def _identities_are_exact(self) -> _IdentitiesInput:
        expected = {
            "management_secret": "loom-personal-dev-management",
            "activation_public_secret": "loom-personal-dev-activation-public",
            "activation_private_secret": "loom-personal-dev-activation-agent",
            "management_service_account": "loom-personal-dev-management",
            "builder_service_account": "loom-personal-dev-builder",
            "activation_service_account": "loom-personal-dev-activation-agent",
            "management_service": "loom-personal-dev-management",
            "management_ingress": "loom-personal-dev-management",
            "scanner_cache_pvc": "loom-personal-dev-scanner-cache",
        }
        if self.model_dump() != expected:
            raise ValueError("personal-dev Kubernetes identities differ from the contract")
        return self


class _StorageInput(_StrictModel):
    storage_class_name: str
    postgres_storage: str
    minio_storage: str
    scanner_cache_storage: str

    @field_validator("storage_class_name")
    @classmethod
    def _storage_class_is_safe(cls, value: str) -> str:
        if len(value) > 253 or _DNS_SUBDOMAIN.fullmatch(value) is None:
            raise ValueError("storage class identity is invalid")
        return value

    @field_validator("postgres_storage", "minio_storage", "scanner_cache_storage")
    @classmethod
    def _storage_is_finite(cls, value: str) -> str:
        if _STORAGE.fullmatch(value) is None:
            raise ValueError("storage size is invalid")
        return value


class _BuilderInput(_StrictModel):
    prepared: Literal[False]
    runtime_class_name: str
    publisher_identity: str
    registry_prefix: str

    @field_validator("runtime_class_name")
    @classmethod
    def _runtime_class_is_safe(cls, value: str) -> str:
        if len(value) > 253 or _DNS_SUBDOMAIN.fullmatch(value) is None:
            raise ValueError("runtime class identity is invalid")
        return value

    @field_validator("publisher_identity")
    @classmethod
    def _operator_identity_is_exact(cls, value: str) -> str:
        if value != "system:serviceaccount:loom-dev:loom-personal-dev-management":
            raise ValueError("builder publisher identity differs from management")
        return value

    @field_validator("registry_prefix")
    @classmethod
    def _registry_prefix_is_bounded(cls, value: str) -> str:
        if len(value) > 253 or _REGISTRY_PREFIX.fullmatch(value) is None:
            raise ValueError("builder registry prefix is invalid")
        return value


class _NetworkInput(_StrictModel):
    public_origin: str
    ingress_class_name: str
    ingress_cluster_issuer: str
    kubernetes_api_cidr: str
    kubernetes_api_port: int = Field(ge=1, le=65535)
    dns_namespace: str
    dns_pod_label_key: str
    dns_pod_label_value: str
    dns_port: int = Field(ge=1, le=65535)
    capacity_manager_origin: str
    capacity_manager_pod_label_key: str
    capacity_manager_pod_label_value: str
    capacity_manager_port: int = Field(ge=1, le=65535)

    @field_validator("ingress_class_name", "ingress_cluster_issuer", "dns_namespace")
    @classmethod
    def _dns_identity_is_safe(cls, value: str) -> str:
        if len(value) > 253 or _DNS_SUBDOMAIN.fullmatch(value) is None:
            raise ValueError("network identity is invalid")
        return value

    @field_validator("dns_pod_label_key", "capacity_manager_pod_label_key")
    @classmethod
    def _label_key_is_safe(cls, value: str) -> str:
        if len(value) > 253 or _LABEL_KEY.fullmatch(value) is None:
            raise ValueError("pod label key is invalid")
        return value

    @field_validator("dns_pod_label_value", "capacity_manager_pod_label_value")
    @classmethod
    def _label_value_is_safe(cls, value: str) -> str:
        if _DNS_LABEL.fullmatch(value) is None:
            raise ValueError("pod label value is invalid")
        return value

    @field_validator("public_origin")
    @classmethod
    def _public_origin_is_https(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.port not in {None, 443}
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or value.endswith("/")
        ):
            raise ValueError("public origin must be one HTTPS origin")
        try:
            hostname = parsed.hostname.encode("ascii").decode("ascii")
        except UnicodeError:
            raise ValueError("public origin host must be ASCII DNS") from None
        if len(hostname) > 253 or _DNS_SUBDOMAIN.fullmatch(hostname) is None:
            raise ValueError("public origin host must be ASCII DNS")
        return value

    @field_validator("capacity_manager_origin")
    @classmethod
    def _capacity_manager_origin_is_exact(cls, value: str) -> str:
        if value != "https://loom-capacity-manager.loom-dev.svc.cluster.local:8443":
            raise ValueError("capacity manager origin differs from loom-dev")
        return value

    @field_validator("kubernetes_api_cidr")
    @classmethod
    def _api_cidr_is_one_host(cls, value: str) -> str:
        try:
            network = ipaddress.ip_network(value, strict=True)
        except ValueError:
            raise ValueError("Kubernetes API CIDR is invalid") from None
        if network.num_addresses != 1 or network.is_unspecified:
            raise ValueError("Kubernetes API CIDR must name one exact host")
        return value


class _LimitsInput(_StrictModel):
    global_live_instances: int = Field(ge=1, le=64)
    per_owner_live_instances: int = Field(ge=1, le=8)
    per_owner_aggregate_min_slots: int = Field(ge=0, le=16)
    per_owner_aggregate_max_slots: int = Field(ge=1, le=32)
    builder_global_concurrency: int = Field(ge=1, le=16)
    builder_per_owner_concurrency: int = Field(ge=1, le=4)
    source_max_archive_bytes: int = Field(ge=1, le=1024 * 1024 * 1024)
    candidate_retained_count: int = Field(ge=1, le=32)
    candidate_retained_bytes: int = Field(ge=1, le=16 * 1024 * 1024 * 1024)

    @model_validator(mode="after")
    def _limits_are_consistent(self) -> _LimitsInput:
        if (
            self.per_owner_live_instances > self.global_live_instances
            or self.per_owner_aggregate_min_slots > self.per_owner_aggregate_max_slots
            or self.builder_per_owner_concurrency > self.builder_global_concurrency
        ):
            raise ValueError("personal-dev quotas are inconsistent")
        return self


class _ResourceEnvelopeInput(_StrictModel):
    cpu_request: str
    memory_request: str
    cpu_limit: str
    memory_limit: str

    @field_validator("cpu_request", "cpu_limit")
    @classmethod
    def _cpu_is_finite(cls, value: str) -> str:
        if _CPU.fullmatch(value) is None:
            raise ValueError("CPU resource is invalid")
        return value

    @field_validator("memory_request", "memory_limit")
    @classmethod
    def _memory_is_finite(cls, value: str) -> str:
        if _STORAGE.fullmatch(value) is None:
            raise ValueError("memory resource is invalid")
        return value

    @model_validator(mode="after")
    def _request_does_not_exceed_limit(self) -> _ResourceEnvelopeInput:
        request_cpu = (
            int(self.cpu_request[:-1])
            if self.cpu_request.endswith("m")
            else int(self.cpu_request) * 1000
        )
        limit_cpu = (
            int(self.cpu_limit[:-1]) if self.cpu_limit.endswith("m") else int(self.cpu_limit) * 1000
        )
        request_memory = int(self.memory_request[:-2]) * (
            1 if self.memory_request.endswith("Mi") else 1024
        )
        limit_memory = int(self.memory_limit[:-2]) * (
            1 if self.memory_limit.endswith("Mi") else 1024
        )
        if request_cpu > limit_cpu or request_memory > limit_memory:
            raise ValueError("resource request exceeds its limit")
        return self


class _ResourcesInput(_StrictModel):
    postgres: _ResourceEnvelopeInput
    minio: _ResourceEnvelopeInput
    migration: _ResourceEnvelopeInput
    management: _ResourceEnvelopeInput
    activation: _ResourceEnvelopeInput


class _ProfileInput(_StrictModel):
    schema_version: Literal[1]
    namespace: str
    personal_namespace_prefix: str
    min_slots_default: Literal[0]
    max_slots_limit: int = Field(ge=0, le=8)
    executable_new_capacity_ceiling: Literal[0]
    dev_instances_enabled: Literal[False]
    personal_dev_builder_enabled: Literal[False]
    activation_agent_replicas: Literal[0]
    protocol_versions_json: str
    identities: _IdentitiesInput
    storage: _StorageInput
    builder: _BuilderInput
    network: _NetworkInput
    limits: _LimitsInput
    resources: _ResourcesInput
    pools: list[_PoolInput]

    @field_validator("namespace")
    @classmethod
    def _namespace_is_exact(cls, value: str) -> str:
        if value != NAMESPACE:
            raise ValueError("shared namespace must be loom-dev")
        return value

    @field_validator("personal_namespace_prefix")
    @classmethod
    def _prefix_is_exact(cls, value: str) -> str:
        if value != PERSONAL_NAMESPACE_PREFIX:
            raise ValueError("personal namespace prefix must be loom-dev-")
        return value

    @field_validator("protocol_versions_json")
    @classmethod
    def _protocol_versions_are_canonical(cls, value: str) -> str:
        try:
            parsed = json.loads(value)
            canonical = _canonical_json(parsed).decode("ascii")
        except (UnicodeError, TypeError, ValueError, json.JSONDecodeError):
            raise ValueError("protocol versions are invalid") from None
        if (
            canonical != value
            or not isinstance(parsed, dict)
            or not parsed
            or any(
                not isinstance(key, str) or not key or not isinstance(item, str) or not item
                for key, item in parsed.items()
            )
        ):
            raise ValueError("protocol versions must be one canonical nonempty string map")
        return value

    @model_validator(mode="after")
    def _profile_is_exact_shadow(self) -> _ProfileInput:
        observed = {item.pool_id: item.architecture for item in self.pools}
        if len(observed) != len(self.pools) or observed != REQUIRED_POOLS:
            raise ValueError("personal-dev pools must be exactly OLDLAB and GB10")
        if self.network.capacity_manager_port != 8443:
            raise ValueError("capacity manager port differs from the protected service")
        if self.network.dns_port != 53:
            raise ValueError("DNS port differs from the cluster service")
        return self


class _ImagesInput(_StrictModel):
    loom_service: str
    personal_dev_builder: str
    personal_dev_activation_agent: str
    postgres: str
    minio: str
    minio_client: str

    @model_validator(mode="after")
    def _images_are_exact_and_distinct(self) -> _ImagesInput:
        repositories = {
            "loom_service": "ghcr.io/qianyi-sun/loom-service",
            "personal_dev_builder": "ghcr.io/qianyi-sun/loom-personal-dev-builder",
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent"
            ),
            "postgres": "docker.io/library/postgres",
            "minio": "quay.io/minio/minio",
            "minio_client": "quay.io/minio/mc",
        }
        digests: list[str] = []
        for key, repository in repositories.items():
            reference = getattr(self, key)
            prefix = f"{repository}@sha256:"
            if not reference.startswith(prefix):
                raise ValueError("trusted image repository is invalid")
            digest = reference.removeprefix(prefix)
            if _DIGEST.fullmatch(digest) is None or digest == "0" * 64:
                raise ValueError("trusted image digest is invalid")
            digests.append(digest)
        if len(set(digests)) != len(digests):
            raise ValueError("trusted image digests must be distinct")
        return self


class _TrustedReleaseInput(_StrictModel):
    schema_version: Literal[1]
    source_sha: str
    source_tree: str
    images: _ImagesInput
    release_evidence_sha256: str

    @field_validator("source_sha", "source_tree")
    @classmethod
    def _git_identity_is_exact(cls, value: str) -> str:
        if _GIT_IDENTITY.fullmatch(value) is None or value == "0" * 40:
            raise ValueError("source identity is invalid")
        return value

    @field_validator("release_evidence_sha256")
    @classmethod
    def _release_evidence_is_exact(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None or value == "0" * 64:
            raise ValueError("release evidence digest is invalid")
        return value


@dataclass(frozen=True, slots=True)
class PoolCapability:
    pool_id: str
    architecture: str


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneIdentities:
    management_secret: str
    activation_public_secret: str
    activation_private_secret: str
    management_service_account: str
    builder_service_account: str
    activation_service_account: str
    management_service: str
    management_ingress: str
    scanner_cache_pvc: str


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneStorage:
    storage_class_name: str
    postgres_storage: str
    minio_storage: str
    scanner_cache_storage: str


@dataclass(frozen=True, slots=True)
class PersonalDevBuilderTrust:
    prepared: bool
    runtime_class_name: str
    publisher_identity: str
    registry_prefix: str


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneNetwork:
    public_origin: str
    ingress_class_name: str
    ingress_cluster_issuer: str
    kubernetes_api_cidr: str
    kubernetes_api_port: int
    dns_namespace: str
    dns_pod_label_key: str
    dns_pod_label_value: str
    dns_port: int
    capacity_manager_origin: str
    capacity_manager_pod_label_key: str
    capacity_manager_pod_label_value: str
    capacity_manager_port: int


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneLimits:
    global_live_instances: int
    per_owner_live_instances: int
    per_owner_aggregate_min_slots: int
    per_owner_aggregate_max_slots: int
    builder_global_concurrency: int
    builder_per_owner_concurrency: int
    source_max_archive_bytes: int
    candidate_retained_count: int
    candidate_retained_bytes: int


@dataclass(frozen=True, slots=True)
class ResourceEnvelope:
    cpu_request: str
    memory_request: str
    cpu_limit: str
    memory_limit: str


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneResources:
    postgres: ResourceEnvelope
    minio: ResourceEnvelope
    migration: ResourceEnvelope
    management: ResourceEnvelope
    activation: ResourceEnvelope


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneProfile:
    schema_version: int
    namespace: str
    personal_namespace_prefix: str
    min_slots_default: int
    max_slots_limit: int
    executable_new_capacity_ceiling: int
    dev_instances_enabled: bool
    personal_dev_builder_enabled: bool
    activation_agent_replicas: int
    protocol_versions: Mapping[str, str]
    identities: PersonalDevControlPlaneIdentities
    storage: PersonalDevControlPlaneStorage
    builder: PersonalDevBuilderTrust
    network: PersonalDevControlPlaneNetwork
    limits: PersonalDevControlPlaneLimits
    resources: PersonalDevControlPlaneResources
    pools: tuple[PoolCapability, ...]

    def canonical_value(self) -> dict[str, Any]:
        """Return the complete primitive profile value used for render binding."""

        return {
            "activation_agent_replicas": self.activation_agent_replicas,
            "builder": _dataclass_value(self.builder),
            "dev_instances_enabled": self.dev_instances_enabled,
            "executable_new_capacity_ceiling": self.executable_new_capacity_ceiling,
            "identities": _dataclass_value(self.identities),
            "limits": _dataclass_value(self.limits),
            "max_slots_limit": self.max_slots_limit,
            "min_slots_default": self.min_slots_default,
            "namespace": self.namespace,
            "network": _dataclass_value(self.network),
            "personal_dev_builder_enabled": self.personal_dev_builder_enabled,
            "personal_namespace_prefix": self.personal_namespace_prefix,
            "pools": [_dataclass_value(item) for item in self.pools],
            "protocol_versions": dict(sorted(self.protocol_versions.items())),
            "resources": {
                name: _dataclass_value(getattr(self.resources, name))
                for name in ("activation", "management", "migration", "minio", "postgres")
            },
            "schema_version": self.schema_version,
            "storage": _dataclass_value(self.storage),
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.canonical_value())


@dataclass(frozen=True, slots=True)
class PersonalDevTrustedImages:
    loom_service: str
    personal_dev_builder: str
    personal_dev_activation_agent: str
    postgres: str
    minio: str
    minio_client: str


@dataclass(frozen=True, slots=True)
class PersonalDevTrustedRelease:
    schema_version: int
    source_sha: str
    source_tree: str
    images: PersonalDevTrustedImages
    release_evidence_sha256: str

    def canonical_value(self) -> dict[str, Any]:
        return {
            "images": _dataclass_value(self.images),
            "release_evidence_sha256": self.release_evidence_sha256,
            "schema_version": self.schema_version,
            "source_sha": self.source_sha,
            "source_tree": self.source_tree,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.canonical_value())


class PersonalDevTrustedReleaseError(ValueError):
    """The trusted release file is unsafe, unstable, or invalid."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _dataclass_value(value: object) -> dict[str, Any]:
    return {
        field: getattr(value, field)
        for field in value.__dataclass_fields__  # type: ignore[attr-defined]
    }


def _resource_envelope(value: _ResourceEnvelopeInput) -> ResourceEnvelope:
    return ResourceEnvelope(**value.model_dump())


def load_personal_dev_control_plane_profile(path: Path) -> PersonalDevControlPlaneProfile:
    """Load the strict non-secret shadow profile."""

    payload = path.read_bytes()
    if not payload or len(payload) > MAX_TRUSTED_RELEASE_BYTES:
        raise ValueError("personal-dev control-plane profile is invalid")
    try:
        document = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError):
        raise ValueError("personal-dev control-plane profile is invalid") from None
    parsed = _ProfileInput.model_validate(document)
    protocols = json.loads(parsed.protocol_versions_json)
    return PersonalDevControlPlaneProfile(
        schema_version=parsed.schema_version,
        namespace=parsed.namespace,
        personal_namespace_prefix=parsed.personal_namespace_prefix,
        min_slots_default=parsed.min_slots_default,
        max_slots_limit=parsed.max_slots_limit,
        executable_new_capacity_ceiling=parsed.executable_new_capacity_ceiling,
        dev_instances_enabled=parsed.dev_instances_enabled,
        personal_dev_builder_enabled=parsed.personal_dev_builder_enabled,
        activation_agent_replicas=parsed.activation_agent_replicas,
        protocol_versions=MappingProxyType(dict(sorted(protocols.items()))),
        identities=PersonalDevControlPlaneIdentities(**parsed.identities.model_dump()),
        storage=PersonalDevControlPlaneStorage(**parsed.storage.model_dump()),
        builder=PersonalDevBuilderTrust(**parsed.builder.model_dump()),
        network=PersonalDevControlPlaneNetwork(**parsed.network.model_dump()),
        limits=PersonalDevControlPlaneLimits(**parsed.limits.model_dump()),
        resources=PersonalDevControlPlaneResources(
            postgres=_resource_envelope(parsed.resources.postgres),
            minio=_resource_envelope(parsed.resources.minio),
            migration=_resource_envelope(parsed.resources.migration),
            management=_resource_envelope(parsed.resources.management),
            activation=_resource_envelope(parsed.resources.activation),
        ),
        pools=tuple(
            PoolCapability(pool_id=item.pool_id, architecture=item.architecture)
            for item in sorted(parsed.pools, key=lambda item: item.pool_id)
        ),
    )


def _invalid_release() -> PersonalDevTrustedReleaseError:
    return PersonalDevTrustedReleaseError("personal-dev trusted release is invalid")


def _duplicate_rejecting_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON key")
        value[key] = item
    return value


def _read_trusted_release(path: Path) -> bytes:
    try:
        before_path = path.lstat()
        if (
            not stat.S_ISREG(before_path.st_mode)
            or stat.S_ISLNK(before_path.st_mode)
            or before_path.st_uid != os.geteuid()
            or stat.S_IMODE(before_path.st_mode) != 0o600
            or before_path.st_nlink != 1
            or not 0 < before_path.st_size <= MAX_TRUSTED_RELEASE_BYTES
        ):
            raise _invalid_release()
        flags = (
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0)
        )
        descriptor = os.open(path, flags)
        try:
            before = os.fstat(descriptor)
            if (
                (before.st_dev, before.st_ino) != (before_path.st_dev, before_path.st_ino)
                or not stat.S_ISREG(before.st_mode)
                or before.st_uid != os.geteuid()
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_nlink != 1
                or not 0 < before.st_size <= MAX_TRUSTED_RELEASE_BYTES
            ):
                raise _invalid_release()
            remaining = MAX_TRUSTED_RELEASE_BYTES + 1
            chunks: list[bytes] = []
            while remaining:
                chunk = os.read(descriptor, min(64 * 1024, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
        finally:
            os.close(descriptor)
    except PersonalDevTrustedReleaseError:
        raise
    except OSError:
        raise _invalid_release() from None
    stable_fields = (
        "st_dev",
        "st_ino",
        "st_mode",
        "st_uid",
        "st_gid",
        "st_nlink",
        "st_size",
        "st_mtime_ns",
        "st_ctime_ns",
    )
    payload = b"".join(chunks)
    if (
        len(payload) != before.st_size
        or len(payload) > MAX_TRUSTED_RELEASE_BYTES
        or any(getattr(before, field) != getattr(after, field) for field in stable_fields)
    ):
        raise _invalid_release()
    return payload


def load_personal_dev_trusted_release(
    path: Path,
    expected_sha256: str,
) -> PersonalDevTrustedRelease:
    """Load one digest-pinned, owner-only canonical trusted release document."""

    if (
        not isinstance(expected_sha256, str)
        or _DIGEST.fullmatch(expected_sha256) is None
        or expected_sha256 == "0" * 64
    ):
        raise _invalid_release()
    payload = _read_trusted_release(path)
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected_sha256):
        raise _invalid_release()
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(value, dict) or _canonical_json(value) != payload:
            raise ValueError("release JSON is not canonical")
        parsed = _TrustedReleaseInput.model_validate(value)
    except (UnicodeError, ValueError):
        raise _invalid_release() from None
    release = PersonalDevTrustedRelease(
        schema_version=parsed.schema_version,
        source_sha=parsed.source_sha,
        source_tree=parsed.source_tree,
        images=PersonalDevTrustedImages(**parsed.images.model_dump()),
        release_evidence_sha256=parsed.release_evidence_sha256,
    )
    if release.canonical_bytes() != payload:
        raise _invalid_release()
    return release


__all__ = [
    "MAX_TRUSTED_RELEASE_BYTES",
    "NAMESPACE",
    "PERSONAL_NAMESPACE_PREFIX",
    "REQUIRED_IMAGE_KEYS",
    "REQUIRED_POOLS",
    "PersonalDevBuilderTrust",
    "PersonalDevControlPlaneIdentities",
    "PersonalDevControlPlaneLimits",
    "PersonalDevControlPlaneNetwork",
    "PersonalDevControlPlaneProfile",
    "PersonalDevControlPlaneResources",
    "PersonalDevControlPlaneStorage",
    "PersonalDevTrustedImages",
    "PersonalDevTrustedRelease",
    "PersonalDevTrustedReleaseError",
    "PoolCapability",
    "ResourceEnvelope",
    "load_personal_dev_control_plane_profile",
    "load_personal_dev_trusted_release",
]
