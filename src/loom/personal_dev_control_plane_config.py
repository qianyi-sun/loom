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
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from loom.db.schema_startup import service_schema_heads
from loom.personal_dev_native_builder_protocol import (
    NATIVE_BUILDER_MAX_CONCURRENCY,
    NATIVE_BUILDER_PLATFORM,
    NATIVE_BUILDER_PROTOCOL_VERSION,
    NATIVE_BUILDER_PROVIDER,
)

NAMESPACE = "loom-dev"
PERSONAL_NAMESPACE_PREFIX = "loom-dev-"
REQUIRED_POOLS = {"oldlab": "x86_64", "gb10": "arm64"}
REQUIRED_IMAGE_KEYS = {
    "loom_service",
    "loom_web",
    "personal_dev_builder",
    "personal_dev_activation_agent",
    "personal_dev_native_builder_agent",
    "personal_dev_scanner_cache",
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
_PROC_SELF_FD = re.compile(r"/proc/self/fd/([1-9][0-9]*)")
_STORAGE = re.compile(r"[1-9][0-9]*(?:Mi|Gi)")
_CPU = re.compile(r"(?:[1-9][0-9]*m|[1-9][0-9]*)")
_REGISTRY_PREFIX = re.compile(
    r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?(?::[1-9][0-9]{0,4})?"
    r"(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+"
)
_PRINCIPAL_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,126}[a-z0-9])?")
_KEY_ID = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,62}[a-z0-9])?")
_SCHEMA_HEAD = re.compile(r"[0-9]{4}")
_CANONICAL_TIMESTAMP = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")
_PRIVATE_USE_IPV4_NETWORKS = (
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.168.0.0/16"),
)
_PRIVATE_USE_IPV6_NETWORK = ipaddress.ip_network("fc00::/7")


def _is_private_use_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if isinstance(address, ipaddress.IPv4Address):
        return any(address in network for network in _PRIVATE_USE_IPV4_NETWORKS)
    return address in _PRIVATE_USE_IPV6_NETWORK


def _public_store_endpoint_cidrs(value: list[str]) -> list[str]:
    networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
    for cidr in value:
        try:
            network = ipaddress.ip_network(cidr, strict=True)
        except ValueError:
            raise ValueError("public store endpoint CIDR is invalid") from None
        address = network.network_address
        if (
            cidr != str(network)
            or network.prefixlen != network.max_prefixlen
            or not address.is_global
            or address.is_loopback
            or address.is_link_local
            or address.is_multicast
            or address.is_unspecified
            or address.is_reserved
        ):
            raise ValueError("public store endpoint must be one public host")
        networks.append(network)
    if len({str(network) for network in networks}) != len(networks):
        raise ValueError("public store endpoint CIDRs must be unique")
    canonical = [
        str(network)
        for network in sorted(
            networks,
            key=lambda item: (item.version, int(item.network_address)),
        )
    ]
    if value != canonical:
        raise ValueError("public store endpoint CIDRs must be in canonical order")
    return value


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
    native_builder_public_secret: str | None = None

    @field_validator("*")
    @classmethod
    def _identity_is_safe(cls, value: str | None) -> str | None:
        if value is not None and _DNS_LABEL.fullmatch(value) is None:
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
            "native_builder_public_secret": self.native_builder_public_secret,
        }
        if self.native_builder_public_secret not in {
            None,
            "loom-personal-dev-native-builder-public",
        } or self.model_dump() != expected:
            raise ValueError("personal-dev Kubernetes identities differ from the contract")
        return self


class _StorageInput(_StrictModel):
    storage_class_name: str
    postgres_storage: str
    minio_storage: str
    scanner_cache_storage: str
    lineage_render_input_sha256: str | None = None
    lineage_trusted_release_sha256: str | None = None

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

    @field_validator("lineage_render_input_sha256", "lineage_trusted_release_sha256")
    @classmethod
    def _storage_lineage_digest_is_valid(cls, value: str | None) -> str | None:
        if value is not None and (_DIGEST.fullmatch(value) is None or value == "0" * 64):
            raise ValueError("storage lineage digest is invalid")
        return value

    @model_validator(mode="after")
    def _storage_lineage_is_complete(self) -> _StorageInput:
        if (self.lineage_render_input_sha256 is None) != (
            self.lineage_trusted_release_sha256 is None
        ):
            raise ValueError("storage lineage must be completely pinned")
        return self


class _BuilderInput(_StrictModel):
    prepared: Literal[False]
    runtime_class_name: str
    runtime_handler: str
    runtime_profile_sha256: str
    publisher_identity: str
    registry_prefix: str

    @field_validator("runtime_class_name")
    @classmethod
    def _runtime_class_is_safe(cls, value: str) -> str:
        if len(value) > 253 or _DNS_SUBDOMAIN.fullmatch(value) is None:
            raise ValueError("runtime class identity is invalid")
        return value

    @field_validator("runtime_handler")
    @classmethod
    def _runtime_handler_is_exact(cls, value: str) -> str:
        if value != "runsc-personal-dev":
            raise ValueError("runtime handler differs from the measured contract")
        return value

    @field_validator("runtime_profile_sha256")
    @classmethod
    def _runtime_profile_is_exact_digest(cls, value: str) -> str:
        if _DIGEST.fullmatch(value) is None:
            raise ValueError("runtime profile digest is invalid")
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
        try:
            parsed = urlsplit(f"//{value}")
            port = parsed.port
        except ValueError:
            raise ValueError("builder registry prefix is invalid") from None
        hostname = parsed.hostname
        if (
            len(value) > 253
            or _REGISTRY_PREFIX.fullmatch(value) is None
            or hostname is None
            or len(hostname) > 253
            or _DNS_SUBDOMAIN.fullmatch(hostname) is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ValueError("builder registry prefix is invalid")
        return value


class _NativeBuilderInput(_StrictModel):
    prepared: bool
    agent_instance_id: str
    agent_key_id: str
    public_key_sha256: str
    host_name: str
    runtime_profile_sha256: str
    public_store_origin: str
    public_store_endpoint_cidrs: list[str] = Field(default_factory=list, max_length=8)
    provider: Literal["gb10-gvisor-docker-v1"]
    platform: Literal["linux/arm64"]
    protocol_version: Literal[1]
    freshness_seconds: int = Field(ge=15, le=300)
    max_concurrency: Literal[2]

    @model_validator(mode="after")
    def _identity_matches_prepared_state(self) -> _NativeBuilderInput:
        identity = (
            self.agent_instance_id,
            self.agent_key_id,
            self.public_key_sha256,
            self.host_name,
            self.runtime_profile_sha256,
            self.public_store_origin,
            self.public_store_endpoint_cidrs,
        )
        if not self.prepared:
            if any(identity):
                raise ValueError("unprepared native builder identity must be empty")
            return self
        _canonical_uuid(self.agent_instance_id, "native builder agent instance")
        if _KEY_ID.fullmatch(self.agent_key_id) is None:
            raise ValueError("native builder agent key is invalid")
        _nonzero_digest(self.public_key_sha256, "native builder public key digest")
        _nonzero_digest(self.runtime_profile_sha256, "native builder runtime profile")
        if self.host_name != "gx10-01c7":
            raise ValueError("native builder host identity is invalid")
        if not self.public_store_endpoint_cidrs:
            raise ValueError("native builder public store endpoint is unavailable")
        try:
            parsed = urlsplit(self.public_store_origin)
            port = parsed.port
        except ValueError:
            raise ValueError("native builder public store origin is invalid") from None
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or self.public_store_origin.endswith("/")
            or _DNS_SUBDOMAIN.fullmatch(parsed.hostname) is None
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ValueError("native builder public store origin is invalid")
        if (
            self.provider != NATIVE_BUILDER_PROVIDER
            or self.platform != NATIVE_BUILDER_PLATFORM
            or self.protocol_version != NATIVE_BUILDER_PROTOCOL_VERSION
            or self.max_concurrency != NATIVE_BUILDER_MAX_CONCURRENCY
        ):
            raise ValueError("native builder protocol identity is invalid")
        return self

    @field_validator("public_store_endpoint_cidrs")
    @classmethod
    def _public_store_endpoints_are_exact(cls, value: list[str]) -> list[str]:
        return _public_store_endpoint_cidrs(value)


class _NetworkInput(_StrictModel):
    public_origin: str
    ingress_class_name: str
    ingress_cluster_issuer: str
    # Legacy rollback input. Cross-node ingress source identity is not stable
    # under the installed VXLAN topology, so renderers must not consume it.
    ingress_controller_source_cidrs: list[str] = Field(default_factory=list, max_length=32)
    acme_http01_solver_port: int = Field(default=8089, ge=1, le=65535)
    kubernetes_api_cidr: str
    kubernetes_api_port: int = Field(ge=1, le=65535)
    kubernetes_api_endpoint_cidrs: list[str] = Field(min_length=1, max_length=32)
    kubernetes_api_endpoint_port: int = Field(ge=1, le=65535)
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
        try:
            parsed = urlsplit(value)
            _ = parsed.port
        except ValueError:
            raise ValueError("public origin must be one HTTPS origin") from None
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

    @field_validator("ingress_controller_source_cidrs")
    @classmethod
    def _ingress_controller_sources_are_exact_private_hosts(
        cls,
        value: list[str],
    ) -> list[str]:
        networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        for cidr in value:
            try:
                network = ipaddress.ip_network(cidr, strict=True)
            except ValueError:
                raise ValueError("ingress controller source CIDR is invalid") from None
            address = network.network_address
            if (
                cidr != str(network)
                or network.prefixlen != network.max_prefixlen
                or not _is_private_use_address(address)
                or address.is_loopback
                or address.is_link_local
                or address.is_multicast
                or address.is_unspecified
                or address.is_reserved
            ):
                raise ValueError("ingress controller source must be one private host")
            networks.append(network)
        if len({str(network) for network in networks}) != len(networks):
            raise ValueError("ingress controller source CIDRs must be unique")
        if value != [
            str(network) for network in sorted(networks, key=lambda item: int(item.network_address))
        ]:
            raise ValueError("ingress controller source CIDRs must be in canonical order")
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

    @field_validator("kubernetes_api_endpoint_cidrs")
    @classmethod
    def _api_endpoint_cidrs_are_exact_private_hosts(cls, value: list[str]) -> list[str]:
        networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        for cidr in value:
            try:
                network = ipaddress.ip_network(cidr, strict=True)
            except ValueError:
                raise ValueError("Kubernetes API endpoint CIDR is invalid") from None
            address = network.network_address
            if (
                cidr != str(network)
                or network.prefixlen != network.max_prefixlen
                or not _is_private_use_address(address)
                or address.is_loopback
                or address.is_link_local
                or address.is_multicast
                or address.is_unspecified
                or address.is_reserved
            ):
                raise ValueError("Kubernetes API endpoint must be one private host")
            networks.append(network)
        if len({str(network) for network in networks}) != len(networks):
            raise ValueError("Kubernetes API endpoint CIDRs must be unique")
        if value != [
            str(network) for network in sorted(networks, key=lambda item: int(item.network_address))
        ]:
            raise ValueError("Kubernetes API endpoint CIDRs must be in canonical order")
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
    web: _ResourceEnvelopeInput | None = None
    activation: _ResourceEnvelopeInput


class _ProfileInput(_StrictModel):
    schema_version: Literal[1, 2, 3]
    namespace: str
    personal_namespace_prefix: str
    min_slots_default: Literal[0]
    max_slots_limit: int = Field(ge=0, le=8)
    executable_new_capacity_ceiling: Literal[0]
    dev_instances_enabled: Literal[False]
    personal_dev_builder_enabled: Literal[False]
    personal_dev_native_builder_enabled: Literal[False] = False
    activation_agent_replicas: Literal[0]
    protocol_versions_json: str
    identities: _IdentitiesInput
    storage: _StorageInput
    builder: _BuilderInput
    native_builder: _NativeBuilderInput | None = None
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
        if (self.schema_version >= 2) != (self.resources.web is not None):
            raise ValueError("personal-dev web resources do not match profile schema")
        native_schema = self.schema_version == 3
        if native_schema != (self.native_builder is not None) or native_schema != (
            self.identities.native_builder_public_secret is not None
        ):
            raise ValueError("personal-dev native builder does not match profile schema")
        observed = {item.pool_id: item.architecture for item in self.pools}
        if len(observed) != len(self.pools) or observed != REQUIRED_POOLS:
            raise ValueError("personal-dev pools must be exactly OLDLAB and GB10")
        if self.network.capacity_manager_port != 8443:
            raise ValueError("capacity manager port differs from the protected service")
        if self.network.acme_http01_solver_port != 8089:
            raise ValueError("ACME HTTP-01 solver port differs from cert-manager")
        if self.network.dns_port != 53:
            raise ValueError("DNS port differs from the cluster service")
        return self


class _ImagesInput(_StrictModel):
    loom_service: str
    loom_web: str | None = None
    personal_dev_builder: str
    personal_dev_activation_agent: str
    personal_dev_native_builder_agent: str | None = None
    personal_dev_scanner_cache: str
    postgres: str
    minio: str
    minio_client: str

    @model_validator(mode="after")
    def _images_are_exact_and_distinct(self) -> _ImagesInput:
        repositories = {
            "loom_service": "ghcr.io/qianyi-sun/loom-service",
            "loom_web": "ghcr.io/qianyi-sun/loom-web",
            "personal_dev_builder": "ghcr.io/qianyi-sun/loom-personal-dev-builder",
            "personal_dev_activation_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-activation-agent"
            ),
            "personal_dev_native_builder_agent": (
                "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent"
            ),
            "personal_dev_scanner_cache": ("ghcr.io/qianyi-sun/loom-personal-dev-scanner-cache"),
            "postgres": "docker.io/library/postgres",
            "minio": "quay.io/minio/minio",
            "minio_client": "quay.io/minio/mc",
        }
        digests: list[str] = []
        for key, repository in repositories.items():
            reference = getattr(self, key)
            if reference is None:
                continue
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


class _TrustedScannerInput(_StrictModel):
    binary_platform: Literal["linux/amd64"]
    binary_sha256: str
    cache_identity_sha256: str
    database_metadata_sha256: str
    database_sha256: str
    java_database_metadata_sha256: str
    java_database_sha256: str
    lock_sha256: str
    trivy_version: Literal["v0.70.0"]

    @field_validator(
        "binary_sha256",
        "cache_identity_sha256",
        "database_metadata_sha256",
        "database_sha256",
        "java_database_metadata_sha256",
        "java_database_sha256",
        "lock_sha256",
    )
    @classmethod
    def _scanner_digest_is_exact(cls, value: str) -> str:
        return _nonzero_digest(value, "trusted scanner digest")

    @model_validator(mode="after")
    def _cache_identity_is_exact(self) -> _TrustedScannerInput:
        without_identity = {
            "binary_platform": self.binary_platform,
            "binary_sha256": self.binary_sha256,
            "database_metadata_sha256": self.database_metadata_sha256,
            "database_sha256": self.database_sha256,
            "java_database_metadata_sha256": self.java_database_metadata_sha256,
            "java_database_sha256": self.java_database_sha256,
            "lock_sha256": self.lock_sha256,
            "trivy_version": self.trivy_version,
        }
        expected = hashlib.sha256(
            b"loom-personal-dev-scanner-cache-v1\0" + _canonical_json(without_identity)
        ).hexdigest()
        if not hmac.compare_digest(self.cache_identity_sha256, expected):
            raise ValueError("trusted scanner cache identity is invalid")
        return self


class _TrustedReleaseInput(_StrictModel):
    schema_version: Literal[2, 3, 4]
    source_sha: str
    source_tree: str
    images: _ImagesInput
    scanner: _TrustedScannerInput
    release_evidence_sha256: str

    @model_validator(mode="after")
    def _images_match_schema(self) -> _TrustedReleaseInput:
        if (self.schema_version >= 3) != (self.images.loom_web is not None):
            raise ValueError("trusted web image does not match release schema")
        if (self.schema_version == 4) != (
            self.images.personal_dev_native_builder_agent is not None
        ):
            raise ValueError("trusted native builder image does not match release schema")
        return self

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


def _nonzero_digest(value: str, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None or value == "0" * 64:
        raise ValueError(f"{label} is invalid")
    return value


def _canonical_uuid(value: str, label: str) -> str:
    try:
        parsed = UUID(value)
    except (AttributeError, TypeError, ValueError):
        raise ValueError(f"{label} is invalid") from None
    if parsed.int == 0 or str(parsed) != value:
        raise ValueError(f"{label} is invalid")
    return value


def _canonical_timestamp(value: str) -> str:
    if _CANONICAL_TIMESTAMP.fullmatch(value) is None:
        raise ValueError("acceptance timestamp is invalid")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)
    except ValueError:
        raise ValueError("acceptance timestamp is invalid") from None
    if parsed.strftime("%Y-%m-%dT%H:%M:%SZ") != value:
        raise ValueError("acceptance timestamp is invalid")
    return value


class _AcceptanceSourceInput(_StrictModel):
    commit: str
    tree: str

    @field_validator("commit", "tree")
    @classmethod
    def _source_identity_is_exact(cls, value: str) -> str:
        if _GIT_IDENTITY.fullmatch(value) is None or value == "0" * 40:
            raise ValueError("acceptance source identity is invalid")
        return value


class _AcceptanceReleaseInput(_StrictModel):
    trusted_release_sha256: str
    release_evidence_sha256: str
    shadow_manifest_sha256: str
    images: _ImagesInput

    @field_validator(
        "trusted_release_sha256",
        "release_evidence_sha256",
        "shadow_manifest_sha256",
    )
    @classmethod
    def _release_digest_is_exact(cls, value: str) -> str:
        return _nonzero_digest(value, "acceptance release digest")


class _AcceptanceStorageInput(_StrictModel):
    schema_head: str
    backup_restore_evidence_sha256: str

    @field_validator("schema_head")
    @classmethod
    def _schema_head_is_exact(cls, value: str) -> str:
        if _SCHEMA_HEAD.fullmatch(value) is None:
            raise ValueError("acceptance schema head is invalid")
        return value

    @field_validator("backup_restore_evidence_sha256")
    @classmethod
    def _backup_evidence_is_exact(cls, value: str) -> str:
        return _nonzero_digest(value, "acceptance backup/restore evidence digest")


class _AcceptanceActivationInput(_StrictModel):
    public_key_sha256: str
    key_id: str

    @field_validator("public_key_sha256")
    @classmethod
    def _public_key_is_exact(cls, value: str) -> str:
        return _nonzero_digest(value, "acceptance public key digest")

    @field_validator("key_id")
    @classmethod
    def _key_id_is_safe(cls, value: str) -> str:
        if _KEY_ID.fullmatch(value) is None:
            raise ValueError("acceptance activation key id is invalid")
        return value


class _AcceptanceNativeBuilderInput(_StrictModel):
    agent_instance_id: str
    agent_key_id: str
    public_key_sha256: str
    host_name: str
    host_boot_id: str
    runtime_profile_sha256: str
    public_store_origin: str
    public_store_endpoint_cidrs: list[str] = Field(min_length=1, max_length=8)
    provider: Literal["gb10-gvisor-docker-v1"]
    platform: Literal["linux/arm64"]
    protocol_version: Literal[1]
    freshness_seconds: int = Field(ge=15, le=300)
    max_concurrency: Literal[2]

    @field_validator("agent_instance_id", "host_boot_id")
    @classmethod
    def _uuid_is_exact(cls, value: str) -> str:
        return _canonical_uuid(value, "acceptance native builder identity")

    @field_validator("agent_key_id")
    @classmethod
    def _key_id_is_exact(cls, value: str) -> str:
        if _KEY_ID.fullmatch(value) is None:
            raise ValueError("acceptance native builder key id is invalid")
        return value

    @field_validator("public_key_sha256", "runtime_profile_sha256")
    @classmethod
    def _digest_is_exact(cls, value: str) -> str:
        return _nonzero_digest(value, "acceptance native builder digest")

    @field_validator("host_name")
    @classmethod
    def _host_is_exact(cls, value: str) -> str:
        if value != "gx10-01c7":
            raise ValueError("acceptance native builder host is invalid")
        return value

    @field_validator("public_store_origin")
    @classmethod
    def _public_store_is_exact(cls, value: str) -> str:
        try:
            parsed = urlsplit(value)
            port = parsed.port
        except ValueError:
            raise ValueError(
                "acceptance native builder public store is invalid"
            ) from None
        if (
            parsed.scheme != "https"
            or parsed.hostname is None
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
            or value.endswith("/")
            or _DNS_SUBDOMAIN.fullmatch(parsed.hostname) is None
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ValueError("acceptance native builder public store is invalid")
        return value

    @field_validator("public_store_endpoint_cidrs")
    @classmethod
    def _public_store_endpoints_are_exact(cls, value: list[str]) -> list[str]:
        return _public_store_endpoint_cidrs(value)


class _AcceptanceBuilderInput(_StrictModel):
    runtime_class_name: str
    runtime_handler: str
    runtime_profile_sha256: str
    trusted_launcher_profile_sha256: str
    scanner_binary_sha256: str
    scanner_cache_identity_sha256: str
    scanner_database_sha256: str
    scanner_database_metadata_sha256: str
    scanner_java_database_sha256: str
    scanner_java_database_metadata_sha256: str
    scanner_finding_policy_sha256: str
    publisher_identity: str
    registry_prefix: str
    protocol_map_sha256: str

    @field_validator("runtime_class_name")
    @classmethod
    def _runtime_class_is_safe(cls, value: str) -> str:
        if len(value) > 253 or _DNS_SUBDOMAIN.fullmatch(value) is None:
            raise ValueError("acceptance RuntimeClass is invalid")
        return value

    @field_validator("runtime_handler")
    @classmethod
    def _runtime_handler_is_safe(cls, value: str) -> str:
        if _DNS_LABEL.fullmatch(value) is None:
            raise ValueError("acceptance runtime handler is invalid")
        return value

    @field_validator(
        "runtime_profile_sha256",
        "trusted_launcher_profile_sha256",
        "scanner_binary_sha256",
        "scanner_cache_identity_sha256",
        "scanner_database_sha256",
        "scanner_database_metadata_sha256",
        "scanner_java_database_sha256",
        "scanner_java_database_metadata_sha256",
        "scanner_finding_policy_sha256",
        "protocol_map_sha256",
    )
    @classmethod
    def _builder_digest_is_exact(cls, value: str) -> str:
        return _nonzero_digest(value, "acceptance builder digest")

    @field_validator("publisher_identity")
    @classmethod
    def _publisher_identity_is_safe(cls, value: str) -> str:
        parts = value.split(":")
        if (
            len(parts) != 4
            or parts[:2] != ["system", "serviceaccount"]
            or any(_DNS_LABEL.fullmatch(part) is None for part in parts[2:])
        ):
            raise ValueError("acceptance publisher identity is invalid")
        return value

    @field_validator("registry_prefix")
    @classmethod
    def _registry_prefix_is_safe(cls, value: str) -> str:
        return _BuilderInput._registry_prefix_is_bounded(value)


class _AcceptanceManagerInput(_StrictModel):
    authority_incarnation: str
    configuration_epoch: int = Field(gt=0)
    execution_state: Literal["shadow", "prepared", "drain-only"]
    execution_epoch: int = Field(ge=0)
    executable_new_capacity_ceiling: Literal[0]

    @field_validator("authority_incarnation")
    @classmethod
    def _authority_is_exact(cls, value: str) -> str:
        return _canonical_uuid(value, "manager authority incarnation")

    @model_validator(mode="after")
    def _manager_state_is_non_executable(self) -> _AcceptanceManagerInput:
        coherent = (
            self.execution_epoch == 0
            if self.execution_state == "shadow"
            else self.execution_epoch > 0
        )
        if not coherent:
            raise ValueError("acceptance manager checkpoint is incoherent")
        return self


class _AcceptancePrincipalsInput(_StrictModel):
    lifecycle_principal_id: str
    reporter_principal_id: str

    @field_validator("lifecycle_principal_id", "reporter_principal_id")
    @classmethod
    def _principal_is_safe(cls, value: str) -> str:
        if _PRINCIPAL_ID.fullmatch(value) is None:
            raise ValueError("acceptance principal id is invalid")
        return value

    @model_validator(mode="after")
    def _principals_are_distinct(self) -> _AcceptancePrincipalsInput:
        if self.lifecycle_principal_id == self.reporter_principal_id:
            raise ValueError("acceptance principals must be distinct")
        return self


class _AcceptanceQuotasInput(_StrictModel):
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
    def _quotas_are_consistent(self) -> _AcceptanceQuotasInput:
        if (
            self.per_owner_live_instances > self.global_live_instances
            or self.per_owner_aggregate_min_slots > self.per_owner_aggregate_max_slots
            or self.builder_per_owner_concurrency > self.builder_global_concurrency
        ):
            raise ValueError("acceptance quotas are inconsistent")
        return self


class _AcceptanceOwnerInput(_StrictModel):
    team_id: str
    user_id: str

    @field_validator("team_id", "user_id")
    @classmethod
    def _owner_id_is_exact(cls, value: str) -> str:
        return _canonical_uuid(value, "acceptance owner id")


class _AcceptanceWindowInput(_StrictModel):
    started_at: str
    expires_at: str
    rollback_expires_at: str

    @field_validator("started_at", "expires_at", "rollback_expires_at")
    @classmethod
    def _timestamp_is_canonical(cls, value: str) -> str:
        return _canonical_timestamp(value)


class _AcceptancePlanCommonInput(_StrictModel):
    source: _AcceptanceSourceInput
    release: _AcceptanceReleaseInput
    storage: _AcceptanceStorageInput
    activation: _AcceptanceActivationInput
    builder: _AcceptanceBuilderInput
    native_builder: _AcceptanceNativeBuilderInput | None = None
    manager: _AcceptanceManagerInput
    principals: _AcceptancePrincipalsInput
    quotas: _AcceptanceQuotasInput
    window: _AcceptanceWindowInput


class _AcceptancePlanV1Input(_AcceptancePlanCommonInput):
    schema_version: Literal[1]
    acceptance_owner: _AcceptanceOwnerInput

    @model_validator(mode="after")
    def _native_builder_is_absent(self) -> _AcceptancePlanV1Input:
        if self.native_builder is not None:
            raise ValueError("v1 acceptance plan cannot bind a native builder")
        return self


class _AcceptancePlanV2Input(_AcceptancePlanCommonInput):
    schema_version: Literal[2]
    acceptance_owners: tuple[_AcceptanceOwnerInput, _AcceptanceOwnerInput]

    @field_validator("acceptance_owners", mode="before")
    @classmethod
    def _owners_are_json_array(cls, value: object) -> tuple[object, ...]:
        if not isinstance(value, list):
            raise ValueError("acceptance owners must be a JSON array")
        return tuple(value)

    @model_validator(mode="after")
    def _owners_and_quotas_are_exact(self) -> _AcceptancePlanV2Input:
        owners = self.acceptance_owners
        if (
            self.native_builder is not None
            or owners
            != tuple(sorted(owners, key=lambda owner: (owner.team_id, owner.user_id)))
            or len({owner.team_id for owner in owners}) != 2
            or len({owner.user_id for owner in owners}) != 2
            or self.quotas.global_live_instances < 2
            or self.quotas.builder_global_concurrency < 2
        ):
            raise ValueError("two-owner acceptance plan contract is invalid")
        return self


class _AcceptancePlanV3Input(_AcceptancePlanCommonInput):
    schema_version: Literal[3]
    native_builder: _AcceptanceNativeBuilderInput
    acceptance_owners: tuple[_AcceptanceOwnerInput, _AcceptanceOwnerInput]

    @field_validator("acceptance_owners", mode="before")
    @classmethod
    def _owners_are_json_array(cls, value: object) -> tuple[object, ...]:
        return _AcceptancePlanV2Input._owners_are_json_array(value)

    @model_validator(mode="after")
    def _owners_and_quotas_are_exact(self) -> _AcceptancePlanV3Input:
        owners = self.acceptance_owners
        if (
            owners != tuple(sorted(owners, key=lambda owner: (owner.team_id, owner.user_id)))
            or len({owner.team_id for owner in owners}) != 2
            or len({owner.user_id for owner in owners}) != 2
            or self.quotas.global_live_instances < 2
            or self.quotas.builder_global_concurrency < 2
        ):
            raise ValueError("two-owner native acceptance plan contract is invalid")
        return self


class _OperationalApprovalInput(_StrictModel):
    acceptance_result_sha256: str
    approved_at: str
    rollback_evidence_sha256: str

    @field_validator("acceptance_result_sha256", "rollback_evidence_sha256")
    @classmethod
    def _evidence_digest_is_exact(cls, value: str) -> str:
        return _nonzero_digest(value, "operational evidence digest")

    @field_validator("approved_at")
    @classmethod
    def _approval_timestamp_is_canonical(cls, value: str) -> str:
        return _canonical_timestamp(value)


class _OperationalPlanCommonInput(_StrictModel):
    source: _AcceptanceSourceInput
    release: _AcceptanceReleaseInput
    storage: _AcceptanceStorageInput
    activation: _AcceptanceActivationInput
    builder: _AcceptanceBuilderInput
    native_builder: _AcceptanceNativeBuilderInput | None = None
    manager: _AcceptanceManagerInput
    principals: _AcceptancePrincipalsInput
    quotas: _AcceptanceQuotasInput
    approval: _OperationalApprovalInput


class _OperationalPlanV1Input(_OperationalPlanCommonInput):
    schema_version: Literal[1]

    @model_validator(mode="after")
    def _native_builder_is_absent(self) -> _OperationalPlanV1Input:
        if self.native_builder is not None:
            raise ValueError("v1 operational plan cannot bind a native builder")
        return self


class _OperationalPlanV2Input(_OperationalPlanCommonInput):
    schema_version: Literal[2]
    native_builder: _AcceptanceNativeBuilderInput


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
    native_builder_public_secret: str | None = None


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneStorage:
    storage_class_name: str
    postgres_storage: str
    minio_storage: str
    scanner_cache_storage: str
    lineage_render_input_sha256: str | None
    lineage_trusted_release_sha256: str | None


@dataclass(frozen=True, slots=True)
class PersonalDevBuilderTrust:
    prepared: bool
    runtime_class_name: str
    runtime_handler: str
    runtime_profile_sha256: str
    publisher_identity: str
    registry_prefix: str


@dataclass(frozen=True, slots=True)
class PersonalDevNativeBuilderTrust:
    prepared: bool
    agent_instance_id: str
    agent_key_id: str
    public_key_sha256: str
    host_name: str
    runtime_profile_sha256: str
    public_store_origin: str
    public_store_endpoint_cidrs: tuple[str, ...]
    provider: str
    platform: str
    protocol_version: int
    freshness_seconds: int
    max_concurrency: int


@dataclass(frozen=True, slots=True)
class PersonalDevControlPlaneNetwork:
    public_origin: str
    ingress_class_name: str
    ingress_cluster_issuer: str
    ingress_controller_source_cidrs: tuple[str, ...]
    acme_http01_solver_port: int
    kubernetes_api_cidr: str
    kubernetes_api_port: int
    kubernetes_api_endpoint_cidrs: tuple[str, ...]
    kubernetes_api_endpoint_port: int
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
    web: ResourceEnvelope | None
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
    personal_dev_native_builder_enabled: bool
    activation_agent_replicas: int
    protocol_versions: Mapping[str, str]
    identities: PersonalDevControlPlaneIdentities
    storage: PersonalDevControlPlaneStorage
    builder: PersonalDevBuilderTrust
    native_builder: PersonalDevNativeBuilderTrust | None
    network: PersonalDevControlPlaneNetwork
    limits: PersonalDevControlPlaneLimits
    resources: PersonalDevControlPlaneResources
    pools: tuple[PoolCapability, ...]

    def canonical_value(self) -> dict[str, Any]:
        """Return the complete primitive profile value used for render binding."""

        identities = _dataclass_value(self.identities)
        if self.identities.native_builder_public_secret is None:
            identities.pop("native_builder_public_secret")
        value = {
            "activation_agent_replicas": self.activation_agent_replicas,
            "builder": _dataclass_value(self.builder),
            "dev_instances_enabled": self.dev_instances_enabled,
            "executable_new_capacity_ceiling": self.executable_new_capacity_ceiling,
            "identities": identities,
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
                for name in (
                    "activation",
                    "management",
                    "migration",
                    "minio",
                    "postgres",
                    *(("web",) if self.resources.web is not None else ()),
                )
            },
            "schema_version": self.schema_version,
            "storage": _dataclass_value(self.storage),
        }
        if self.schema_version >= 3:
            value["personal_dev_native_builder_enabled"] = (
                self.personal_dev_native_builder_enabled
            )
            if self.native_builder is None:  # pragma: no cover - parser enforces schema
                raise ValueError("native builder profile is unavailable")
            value["native_builder"] = _dataclass_value(self.native_builder)
        return value

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.canonical_value())


@dataclass(frozen=True, slots=True)
class PersonalDevTrustedImages:
    loom_service: str
    loom_web: str | None
    personal_dev_builder: str
    personal_dev_activation_agent: str
    personal_dev_scanner_cache: str
    postgres: str
    minio: str
    minio_client: str
    personal_dev_native_builder_agent: str | None = None


def _canonical_images(images: PersonalDevTrustedImages) -> dict[str, Any]:
    value = _dataclass_value(images)
    if images.loom_web is None:
        value.pop("loom_web")
    if images.personal_dev_native_builder_agent is None:
        value.pop("personal_dev_native_builder_agent")
    return value


@dataclass(frozen=True, slots=True)
class PersonalDevTrustedScanner:
    binary_platform: str
    binary_sha256: str
    cache_identity_sha256: str
    database_metadata_sha256: str
    database_sha256: str
    java_database_metadata_sha256: str
    java_database_sha256: str
    lock_sha256: str
    trivy_version: str


@dataclass(frozen=True, slots=True)
class PersonalDevTrustedRelease:
    schema_version: int
    source_sha: str
    source_tree: str
    images: PersonalDevTrustedImages
    scanner: PersonalDevTrustedScanner
    release_evidence_sha256: str

    def canonical_value(self) -> dict[str, Any]:
        return {
            "images": _canonical_images(self.images),
            "release_evidence_sha256": self.release_evidence_sha256,
            "scanner": _dataclass_value(self.scanner),
            "schema_version": self.schema_version,
            "source_sha": self.source_sha,
            "source_tree": self.source_tree,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.canonical_value())


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceSource:
    commit: str
    tree: str


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceRelease:
    trusted_release_sha256: str
    release_evidence_sha256: str
    shadow_manifest_sha256: str
    images: PersonalDevTrustedImages


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceStorage:
    schema_head: str
    backup_restore_evidence_sha256: str


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceActivation:
    public_key_sha256: str
    key_id: str


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceNativeBuilder:
    agent_instance_id: UUID
    agent_key_id: str
    public_key_sha256: str
    host_name: str
    host_boot_id: UUID
    runtime_profile_sha256: str
    public_store_origin: str
    public_store_endpoint_cidrs: tuple[str, ...]
    provider: str
    platform: str
    protocol_version: int
    freshness_seconds: int
    max_concurrency: int


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceBuilder:
    runtime_class_name: str
    runtime_handler: str
    runtime_profile_sha256: str
    trusted_launcher_profile_sha256: str
    scanner_binary_sha256: str
    scanner_cache_identity_sha256: str
    scanner_database_sha256: str
    scanner_database_metadata_sha256: str
    scanner_java_database_sha256: str
    scanner_java_database_metadata_sha256: str
    scanner_finding_policy_sha256: str
    publisher_identity: str
    registry_prefix: str
    protocol_map_sha256: str


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceManager:
    authority_incarnation: UUID
    configuration_epoch: int
    execution_state: Literal["shadow", "prepared", "drain-only"]
    execution_epoch: int
    executable_new_capacity_ceiling: int


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptancePrincipals:
    lifecycle_principal_id: str
    reporter_principal_id: str


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceOwner:
    team_id: UUID
    user_id: UUID


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptanceWindow:
    started_at: datetime
    expires_at: datetime
    rollback_expires_at: datetime


@dataclass(frozen=True, slots=True)
class PersonalDevAcceptancePlan:
    schema_version: int
    source: PersonalDevAcceptanceSource
    release: PersonalDevAcceptanceRelease
    storage: PersonalDevAcceptanceStorage
    activation: PersonalDevAcceptanceActivation
    builder: PersonalDevAcceptanceBuilder
    native_builder: PersonalDevAcceptanceNativeBuilder | None
    manager: PersonalDevAcceptanceManager
    principals: PersonalDevAcceptancePrincipals
    quotas: PersonalDevControlPlaneLimits
    acceptance_owners: tuple[PersonalDevAcceptanceOwner, ...]
    window: PersonalDevAcceptanceWindow

    @property
    def acceptance_owner(self) -> PersonalDevAcceptanceOwner:
        if self.schema_version != 1:
            raise PersonalDevAcceptancePlanError(
                "two-owner acceptance plans have no singular owner"
            )
        return self.acceptance_owners[0]

    def canonical_value(self) -> dict[str, Any]:
        value = {
            "activation": _dataclass_value(self.activation),
            "builder": _dataclass_value(self.builder),
            "manager": {
                **_dataclass_value(self.manager),
                "authority_incarnation": str(self.manager.authority_incarnation),
            },
            "principals": _dataclass_value(self.principals),
            "quotas": _dataclass_value(self.quotas),
            "release": {
                "images": _canonical_images(self.release.images),
                "release_evidence_sha256": self.release.release_evidence_sha256,
                "shadow_manifest_sha256": self.release.shadow_manifest_sha256,
                "trusted_release_sha256": self.release.trusted_release_sha256,
            },
            "schema_version": self.schema_version,
            "source": _dataclass_value(self.source),
            "storage": _dataclass_value(self.storage),
            "window": {
                "expires_at": _format_timestamp(self.window.expires_at),
                "rollback_expires_at": _format_timestamp(self.window.rollback_expires_at),
                "started_at": _format_timestamp(self.window.started_at),
            },
        }
        if self.native_builder is not None:
            value["native_builder"] = {
                **_dataclass_value(self.native_builder),
                "agent_instance_id": str(self.native_builder.agent_instance_id),
                "host_boot_id": str(self.native_builder.host_boot_id),
            }
        if self.schema_version == 1:
            value["acceptance_owner"] = {
                "team_id": str(self.acceptance_owner.team_id),
                "user_id": str(self.acceptance_owner.user_id),
            }
        else:
            value["acceptance_owners"] = [
                {"team_id": str(owner.team_id), "user_id": str(owner.user_id)}
                for owner in self.acceptance_owners
            ]
        return value

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.canonical_value())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def manager_runtime_json(self) -> str:
        """Return the canonical secret-free service interlock binding."""

        return _canonical_json(
            {
                "acceptance_plan_sha256": self.sha256,
                "expires_at": _format_timestamp(self.window.expires_at),
                "manager": {
                    "authority_incarnation": str(self.manager.authority_incarnation),
                    "configuration_epoch": self.manager.configuration_epoch,
                    "executable_new_capacity_ceiling": (
                        self.manager.executable_new_capacity_ceiling
                    ),
                    "execution_epoch": self.manager.execution_epoch,
                    "execution_state": self.manager.execution_state,
                    "observer_principal_id": (self.principals.lifecycle_principal_id),
                },
                "schema_version": 1,
                "started_at": _format_timestamp(self.window.started_at),
            }
        ).decode("ascii")


@dataclass(frozen=True, slots=True)
class PersonalDevOperationalApproval:
    acceptance_result_sha256: str
    approved_at: datetime
    rollback_evidence_sha256: str


@dataclass(frozen=True, slots=True)
class PersonalDevOperationalPlan:
    """One durable, zero-capacity personal-development enablement contract."""

    schema_version: int
    source: PersonalDevAcceptanceSource
    release: PersonalDevAcceptanceRelease
    storage: PersonalDevAcceptanceStorage
    activation: PersonalDevAcceptanceActivation
    builder: PersonalDevAcceptanceBuilder
    native_builder: PersonalDevAcceptanceNativeBuilder | None
    manager: PersonalDevAcceptanceManager
    principals: PersonalDevAcceptancePrincipals
    quotas: PersonalDevControlPlaneLimits
    approval: PersonalDevOperationalApproval

    def canonical_value(self) -> dict[str, Any]:
        value = {
            "activation": _dataclass_value(self.activation),
            "approval": {
                "acceptance_result_sha256": self.approval.acceptance_result_sha256,
                "approved_at": _format_timestamp(self.approval.approved_at),
                "rollback_evidence_sha256": self.approval.rollback_evidence_sha256,
            },
            "builder": _dataclass_value(self.builder),
            "manager": {
                **_dataclass_value(self.manager),
                "authority_incarnation": str(self.manager.authority_incarnation),
            },
            "principals": _dataclass_value(self.principals),
            "quotas": _dataclass_value(self.quotas),
            "release": {
                "images": _canonical_images(self.release.images),
                "release_evidence_sha256": self.release.release_evidence_sha256,
                "shadow_manifest_sha256": self.release.shadow_manifest_sha256,
                "trusted_release_sha256": self.release.trusted_release_sha256,
            },
            "schema_version": self.schema_version,
            "source": _dataclass_value(self.source),
            "storage": _dataclass_value(self.storage),
        }
        if self.native_builder is not None:
            value["native_builder"] = {
                **_dataclass_value(self.native_builder),
                "agent_instance_id": str(self.native_builder.agent_instance_id),
                "host_boot_id": str(self.native_builder.host_boot_id),
            }
        return value

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.canonical_value())

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def manager_runtime_json(self) -> str:
        """Return the canonical, non-expiring zero-capacity runtime binding."""

        return _canonical_json(
            {
                "acceptance_result_sha256": self.approval.acceptance_result_sha256,
                "manager": {
                    "authority_incarnation": str(self.manager.authority_incarnation),
                    "configuration_epoch": self.manager.configuration_epoch,
                    "executable_new_capacity_ceiling": (
                        self.manager.executable_new_capacity_ceiling
                    ),
                    "execution_epoch": self.manager.execution_epoch,
                    "execution_state": self.manager.execution_state,
                    "observer_principal_id": self.principals.lifecycle_principal_id,
                },
                "operational_plan_sha256": self.sha256,
                "schema_version": 1,
            }
        ).decode("ascii")


class PersonalDevTrustedReleaseError(ValueError):
    """The trusted release file is unsafe, unstable, or invalid."""


class PersonalDevAcceptancePlanError(ValueError):
    """The personal-development acceptance plan is unsafe or inconsistent."""


class PersonalDevOperationalPlanError(ValueError):
    """The durable personal-development operational plan is unsafe."""


def _canonical_json(value: object) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _format_timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("acceptance timestamp is naive")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dataclass_value(value: object) -> dict[str, Any]:
    return {
        field: getattr(value, field)
        for field in value.__dataclass_fields__  # type: ignore[attr-defined]
    }


def _resource_envelope(value: _ResourceEnvelopeInput) -> ResourceEnvelope:
    return ResourceEnvelope(**value.model_dump())


def _profile_file_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_profile(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        path_before = path.lstat()
        if (
            not stat.S_ISREG(path_before.st_mode)
            or stat.S_ISLNK(path_before.st_mode)
            or path_before.st_uid != os.geteuid()
            or path_before.st_nlink != 1
            or not 0 < path_before.st_size <= MAX_TRUSTED_RELEASE_BYTES
        ):
            raise ValueError
        descriptor = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        opened = os.fstat(descriptor)
        if _profile_file_identity(opened) != _profile_file_identity(path_before):
            raise ValueError
        payload = bytearray()
        while len(payload) <= MAX_TRUSTED_RELEASE_BYTES:
            chunk = os.read(
                descriptor,
                min(64 * 1024, MAX_TRUSTED_RELEASE_BYTES + 1 - len(payload)),
            )
            if not chunk:
                break
            payload.extend(chunk)
        if (
            len(payload) != opened.st_size
            or _profile_file_identity(os.fstat(descriptor)) != _profile_file_identity(opened)
            or _profile_file_identity(path.lstat()) != _profile_file_identity(path_before)
        ):
            raise ValueError
        return bytes(payload)
    except (OSError, ValueError):
        raise ValueError("personal-dev control-plane profile is invalid") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


def load_personal_dev_control_plane_profile(path: Path) -> PersonalDevControlPlaneProfile:
    """Load the strict non-secret shadow profile."""

    payload = _read_profile(path)
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
        personal_dev_native_builder_enabled=(
            parsed.personal_dev_native_builder_enabled
        ),
        activation_agent_replicas=parsed.activation_agent_replicas,
        protocol_versions=MappingProxyType(dict(sorted(protocols.items()))),
        identities=PersonalDevControlPlaneIdentities(**parsed.identities.model_dump()),
        storage=PersonalDevControlPlaneStorage(**parsed.storage.model_dump()),
        builder=PersonalDevBuilderTrust(**parsed.builder.model_dump()),
        native_builder=(
            PersonalDevNativeBuilderTrust(
                **{
                    **parsed.native_builder.model_dump(
                        exclude={"public_store_endpoint_cidrs"}
                    ),
                    "public_store_endpoint_cidrs": tuple(
                        parsed.native_builder.public_store_endpoint_cidrs
                    ),
                }
            )
            if parsed.native_builder is not None
            else None
        ),
        network=PersonalDevControlPlaneNetwork(
            **{
                **parsed.network.model_dump(),
                "ingress_controller_source_cidrs": tuple(
                    parsed.network.ingress_controller_source_cidrs
                ),
                "kubernetes_api_endpoint_cidrs": tuple(
                    parsed.network.kubernetes_api_endpoint_cidrs
                ),
            }
        ),
        limits=PersonalDevControlPlaneLimits(**parsed.limits.model_dump()),
        resources=PersonalDevControlPlaneResources(
            postgres=_resource_envelope(parsed.resources.postgres),
            minio=_resource_envelope(parsed.resources.minio),
            migration=_resource_envelope(parsed.resources.migration),
            management=_resource_envelope(parsed.resources.management),
            web=(
                _resource_envelope(parsed.resources.web)
                if parsed.resources.web is not None
                else None
            ),
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


def _read_trusted_release_descriptor(descriptor: int) -> tuple[bytes, os.stat_result]:
    before = os.fstat(descriptor)
    if (
        not stat.S_ISREG(before.st_mode)
        or before.st_uid != os.geteuid()
        or stat.S_IMODE(before.st_mode) != 0o600
        or before.st_nlink != 1
        or not 0 < before.st_size <= MAX_TRUSTED_RELEASE_BYTES
    ):
        raise _invalid_release()
    remaining = MAX_TRUSTED_RELEASE_BYTES + 1
    offset = 0
    chunks: list[bytes] = []
    while remaining:
        chunk = os.pread(descriptor, min(64 * 1024, remaining), offset)
        if not chunk:
            break
        chunks.append(chunk)
        offset += len(chunk)
        remaining -= len(chunk)
    after = os.fstat(descriptor)
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
    return payload, before


def _read_trusted_release(path: Path) -> bytes:
    descriptor: int | None = None
    try:
        inherited = _PROC_SELF_FD.fullmatch(str(path))
        if inherited is not None:
            inherited_descriptor = int(inherited.group(1))
            if inherited_descriptor < 3:
                raise _invalid_release()
            descriptor = os.dup(inherited_descriptor)
            payload, _ = _read_trusted_release_descriptor(descriptor)
            return payload
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
        payload, before = _read_trusted_release_descriptor(descriptor)
        if (before.st_dev, before.st_ino) != (before_path.st_dev, before_path.st_ino):
            raise _invalid_release()
        after_path = path.lstat()
    except PersonalDevTrustedReleaseError:
        raise
    except (OSError, OverflowError):
        raise _invalid_release() from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
    if _profile_file_identity(before_path) != _profile_file_identity(after_path):
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
    except (RecursionError, UnicodeError, ValueError):
        raise _invalid_release() from None
    release = PersonalDevTrustedRelease(
        schema_version=parsed.schema_version,
        source_sha=parsed.source_sha,
        source_tree=parsed.source_tree,
        images=PersonalDevTrustedImages(**parsed.images.model_dump()),
        scanner=PersonalDevTrustedScanner(**parsed.scanner.model_dump()),
        release_evidence_sha256=parsed.release_evidence_sha256,
    )
    if release.canonical_bytes() != payload:
        raise _invalid_release()
    return release


def _invalid_acceptance_plan() -> PersonalDevAcceptancePlanError:
    return PersonalDevAcceptancePlanError("personal-dev acceptance plan is invalid")


def _parse_acceptance_timestamp(value: str) -> datetime:
    _canonical_timestamp(value)
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=UTC)


def _acceptance_native_builder(
    value: _AcceptanceNativeBuilderInput | None,
) -> PersonalDevAcceptanceNativeBuilder | None:
    if value is None:
        return None
    fields = value.model_dump()
    fields["agent_instance_id"] = UUID(value.agent_instance_id)
    fields["host_boot_id"] = UUID(value.host_boot_id)
    fields["public_store_endpoint_cidrs"] = tuple(
        value.public_store_endpoint_cidrs
    )
    return PersonalDevAcceptanceNativeBuilder(**fields)


def load_personal_dev_acceptance_plan(
    path: Path,
    expected_sha256: str,
) -> PersonalDevAcceptancePlan:
    """Load one digest-pinned, owner-only canonical acceptance plan."""

    if (
        not isinstance(expected_sha256, str)
        or _DIGEST.fullmatch(expected_sha256) is None
        or expected_sha256 == "0" * 64
    ):
        raise _invalid_acceptance_plan()
    try:
        payload = _read_trusted_release(path)
    except PersonalDevTrustedReleaseError:
        raise _invalid_acceptance_plan() from None
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected_sha256):
        raise _invalid_acceptance_plan()
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(value, dict) or _canonical_json(value) != payload:
            raise ValueError("acceptance plan JSON is not canonical")
        schema_version = value.get("schema_version")
        owners: tuple[_AcceptanceOwnerInput, ...]
        if schema_version == 1:
            parsed_v1 = _AcceptancePlanV1Input.model_validate(value)
            parsed: (
                _AcceptancePlanV1Input
                | _AcceptancePlanV2Input
                | _AcceptancePlanV3Input
            ) = parsed_v1
            owners = (parsed_v1.acceptance_owner,)
        elif schema_version == 2:
            parsed_v2 = _AcceptancePlanV2Input.model_validate(value)
            parsed = parsed_v2
            owners = parsed_v2.acceptance_owners
        elif schema_version == 3:
            parsed_v3 = _AcceptancePlanV3Input.model_validate(value)
            parsed = parsed_v3
            owners = parsed_v3.acceptance_owners
        else:
            raise ValueError("acceptance plan schema version is invalid")
        plan = PersonalDevAcceptancePlan(
            schema_version=parsed.schema_version,
            source=PersonalDevAcceptanceSource(**parsed.source.model_dump()),
            release=PersonalDevAcceptanceRelease(
                trusted_release_sha256=parsed.release.trusted_release_sha256,
                release_evidence_sha256=parsed.release.release_evidence_sha256,
                shadow_manifest_sha256=parsed.release.shadow_manifest_sha256,
                images=PersonalDevTrustedImages(**parsed.release.images.model_dump()),
            ),
            storage=PersonalDevAcceptanceStorage(**parsed.storage.model_dump()),
            activation=PersonalDevAcceptanceActivation(**parsed.activation.model_dump()),
            builder=PersonalDevAcceptanceBuilder(**parsed.builder.model_dump()),
            native_builder=_acceptance_native_builder(parsed.native_builder),
            manager=PersonalDevAcceptanceManager(
                authority_incarnation=UUID(parsed.manager.authority_incarnation),
                configuration_epoch=parsed.manager.configuration_epoch,
                execution_state=parsed.manager.execution_state,
                execution_epoch=parsed.manager.execution_epoch,
                executable_new_capacity_ceiling=(parsed.manager.executable_new_capacity_ceiling),
            ),
            principals=PersonalDevAcceptancePrincipals(**parsed.principals.model_dump()),
            quotas=PersonalDevControlPlaneLimits(**parsed.quotas.model_dump()),
            acceptance_owners=tuple(
                PersonalDevAcceptanceOwner(
                    team_id=UUID(owner.team_id),
                    user_id=UUID(owner.user_id),
                )
                for owner in owners
            ),
            window=PersonalDevAcceptanceWindow(
                started_at=_parse_acceptance_timestamp(parsed.window.started_at),
                expires_at=_parse_acceptance_timestamp(parsed.window.expires_at),
                rollback_expires_at=_parse_acceptance_timestamp(parsed.window.rollback_expires_at),
            ),
        )
    except (RecursionError, UnicodeError, ValueError):
        raise _invalid_acceptance_plan() from None
    if plan.canonical_bytes() != payload:
        raise _invalid_acceptance_plan()
    return plan


def _invalid_operational_plan() -> PersonalDevOperationalPlanError:
    return PersonalDevOperationalPlanError("personal-dev operational plan is invalid")


def load_personal_dev_operational_plan(
    path: Path,
    expected_sha256: str,
) -> PersonalDevOperationalPlan:
    """Load one digest-pinned, owner-only durable operational contract."""

    if (
        not isinstance(expected_sha256, str)
        or _DIGEST.fullmatch(expected_sha256) is None
        or expected_sha256 == "0" * 64
    ):
        raise _invalid_operational_plan()
    try:
        payload = _read_trusted_release(path)
    except PersonalDevTrustedReleaseError:
        raise _invalid_operational_plan() from None
    if not hmac.compare_digest(hashlib.sha256(payload).hexdigest(), expected_sha256):
        raise _invalid_operational_plan()
    try:
        value = json.loads(
            payload,
            object_pairs_hook=_duplicate_rejecting_object,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
        if not isinstance(value, dict) or _canonical_json(value) != payload:
            raise ValueError("operational plan JSON is not canonical")
        schema_version = value.get("schema_version")
        if schema_version == 1:
            parsed: _OperationalPlanV1Input | _OperationalPlanV2Input = (
                _OperationalPlanV1Input.model_validate(value)
            )
        elif schema_version == 2:
            parsed = _OperationalPlanV2Input.model_validate(value)
        else:
            raise ValueError("operational plan schema version is invalid")
        plan = PersonalDevOperationalPlan(
            schema_version=parsed.schema_version,
            source=PersonalDevAcceptanceSource(**parsed.source.model_dump()),
            release=PersonalDevAcceptanceRelease(
                trusted_release_sha256=parsed.release.trusted_release_sha256,
                release_evidence_sha256=parsed.release.release_evidence_sha256,
                shadow_manifest_sha256=parsed.release.shadow_manifest_sha256,
                images=PersonalDevTrustedImages(**parsed.release.images.model_dump()),
            ),
            storage=PersonalDevAcceptanceStorage(**parsed.storage.model_dump()),
            activation=PersonalDevAcceptanceActivation(**parsed.activation.model_dump()),
            builder=PersonalDevAcceptanceBuilder(**parsed.builder.model_dump()),
            native_builder=_acceptance_native_builder(parsed.native_builder),
            manager=PersonalDevAcceptanceManager(
                authority_incarnation=UUID(parsed.manager.authority_incarnation),
                configuration_epoch=parsed.manager.configuration_epoch,
                execution_state=parsed.manager.execution_state,
                execution_epoch=parsed.manager.execution_epoch,
                executable_new_capacity_ceiling=(parsed.manager.executable_new_capacity_ceiling),
            ),
            principals=PersonalDevAcceptancePrincipals(**parsed.principals.model_dump()),
            quotas=PersonalDevControlPlaneLimits(**parsed.quotas.model_dump()),
            approval=PersonalDevOperationalApproval(
                acceptance_result_sha256=parsed.approval.acceptance_result_sha256,
                approved_at=_parse_acceptance_timestamp(parsed.approval.approved_at),
                rollback_evidence_sha256=parsed.approval.rollback_evidence_sha256,
            ),
        )
    except (RecursionError, UnicodeError, ValueError):
        raise _invalid_operational_plan() from None
    if plan.canonical_bytes() != payload:
        raise _invalid_operational_plan()
    return plan


def _validate_personal_dev_enabled_plan(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    shadow_yaml_sha256: str,
    plan: PersonalDevAcceptancePlan | PersonalDevOperationalPlan,
) -> None:
    if not isinstance(profile, PersonalDevControlPlaneProfile) or not isinstance(
        release, PersonalDevTrustedRelease
    ):
        raise ValueError
    if not isinstance(plan, (PersonalDevAcceptancePlan, PersonalDevOperationalPlan)):
        raise ValueError
    if (
        isinstance(plan, PersonalDevAcceptancePlan)
        and plan.schema_version != 3
    ) or (
        isinstance(plan, PersonalDevOperationalPlan)
        and plan.schema_version != 2
    ):
        raise ValueError
    profile_value = profile.canonical_value()
    protocol_versions = profile_value.pop("protocol_versions")
    profile_value["protocol_versions_json"] = _canonical_json(
        protocol_versions
    ).decode("ascii")
    validated_profile = _ProfileInput.model_validate(
        json.loads(_canonical_json(profile_value))
    )
    if validated_profile.schema_version != 3:
        raise ValueError
    plan_value = json.loads(plan.canonical_bytes())
    if isinstance(plan, PersonalDevAcceptancePlan):
        _AcceptancePlanV3Input.model_validate(plan_value)
    else:
        _OperationalPlanV2Input.model_validate(plan_value)
    if (
        type(profile.schema_version) is not int
        or profile.schema_version != 3
        or profile.namespace != NAMESPACE
        or profile.personal_namespace_prefix != PERSONAL_NAMESPACE_PREFIX
        or type(profile.min_slots_default) is not int
        or profile.min_slots_default != 0
        or type(profile.max_slots_limit) is not int
        or not 0 <= profile.max_slots_limit <= 8
        or type(profile.executable_new_capacity_ceiling) is not int
        or profile.executable_new_capacity_ceiling != 0
        or profile.dev_instances_enabled is not False
        or profile.personal_dev_builder_enabled is not False
        or profile.personal_dev_native_builder_enabled is not False
        or type(profile.activation_agent_replicas) is not int
        or profile.activation_agent_replicas != 0
        or len(profile.pools) != len(REQUIRED_POOLS)
        or {item.pool_id: item.architecture for item in profile.pools} != REQUIRED_POOLS
    ):
        raise ValueError
    native = plan.native_builder
    profile_native = profile.native_builder
    if (
        native is None
        or profile_native is None
        or not profile_native.prepared
        or release.schema_version != 4
        or release.images.personal_dev_native_builder_agent is None
        or native.agent_instance_id != UUID(profile_native.agent_instance_id)
        or native.agent_key_id != profile_native.agent_key_id
        or native.public_key_sha256 != profile_native.public_key_sha256
        or native.host_name != profile_native.host_name
        or native.runtime_profile_sha256 != profile_native.runtime_profile_sha256
        or native.public_store_origin != profile_native.public_store_origin
        or native.public_store_endpoint_cidrs
        != profile_native.public_store_endpoint_cidrs
        or native.provider != profile_native.provider
        or native.platform != profile_native.platform
        or native.protocol_version != profile_native.protocol_version
        or native.freshness_seconds != profile_native.freshness_seconds
        or native.max_concurrency != profile_native.max_concurrency
    ):
        raise ValueError
    if (
        type(native.public_store_endpoint_cidrs) is not tuple
        or type(profile_native.public_store_endpoint_cidrs) is not tuple
        or not native.public_store_endpoint_cidrs
    ):
        raise ValueError
    _public_store_endpoint_cidrs(list(native.public_store_endpoint_cidrs))
    shadow_yaml_sha256 = _nonzero_digest(shadow_yaml_sha256, "shadow manifest digest")
    if (
        plan.source.commit != release.source_sha
        or plan.source.tree != release.source_tree
        or plan.release.trusted_release_sha256
        != hashlib.sha256(release.canonical_bytes()).hexdigest()
        or plan.release.release_evidence_sha256 != release.release_evidence_sha256
        or plan.release.shadow_manifest_sha256 != shadow_yaml_sha256
        or plan.release.images != release.images
    ):
        raise ValueError
    heads = service_schema_heads()
    if len(heads) != 1 or plan.storage.schema_head not in heads:
        raise ValueError
    if (
        plan.builder.runtime_class_name != profile.builder.runtime_class_name
        or plan.builder.runtime_handler != profile.builder.runtime_handler
        or plan.builder.runtime_profile_sha256 != profile.builder.runtime_profile_sha256
        or plan.builder.publisher_identity != profile.builder.publisher_identity
        or plan.builder.registry_prefix != profile.builder.registry_prefix
        or plan.builder.protocol_map_sha256
        != hashlib.sha256(_canonical_json(dict(profile.protocol_versions))).hexdigest()
        or plan.builder.scanner_binary_sha256 != release.scanner.binary_sha256
        or plan.builder.scanner_cache_identity_sha256 != release.scanner.cache_identity_sha256
        or plan.builder.scanner_database_sha256 != release.scanner.database_sha256
        or plan.builder.scanner_database_metadata_sha256 != release.scanner.database_metadata_sha256
        or plan.builder.scanner_java_database_sha256 != release.scanner.java_database_sha256
        or plan.builder.scanner_java_database_metadata_sha256
        != release.scanner.java_database_metadata_sha256
    ):
        raise ValueError
    if plan.quotas != profile.limits:
        raise ValueError
    if plan.manager.executable_new_capacity_ceiling != profile.executable_new_capacity_ceiling:
        raise ValueError


def validate_personal_dev_acceptance_plan(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    shadow_yaml_sha256: str,
    plan: PersonalDevAcceptancePlan,
    *,
    now: datetime,
) -> None:
    """Fail closed unless one plan exactly binds the release and inert profile."""

    try:
        if not isinstance(plan, PersonalDevAcceptancePlan):
            raise ValueError
        _validate_personal_dev_enabled_plan(profile, release, shadow_yaml_sha256, plan)
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError
        observed_at = now.astimezone(UTC)
        if not (
            plan.window.started_at
            <= observed_at
            < plan.window.expires_at
            <= plan.window.rollback_expires_at
        ):
            raise ValueError
    except (OSError, RuntimeError, ValueError):
        raise _invalid_acceptance_plan() from None


def validate_personal_dev_operational_plan(
    profile: PersonalDevControlPlaneProfile,
    release: PersonalDevTrustedRelease,
    shadow_yaml_sha256: str,
    plan: PersonalDevOperationalPlan,
    *,
    now: datetime,
) -> None:
    """Fail closed unless one durable plan binds accepted, zero-capacity state."""

    try:
        if not isinstance(plan, PersonalDevOperationalPlan):
            raise ValueError
        _validate_personal_dev_enabled_plan(profile, release, shadow_yaml_sha256, plan)
        if not isinstance(now, datetime) or now.tzinfo is None or now.utcoffset() is None:
            raise ValueError
        if plan.approval.approved_at > now.astimezone(UTC):
            raise ValueError
        _nonzero_digest(plan.approval.acceptance_result_sha256, "acceptance result digest")
        _nonzero_digest(plan.approval.rollback_evidence_sha256, "rollback evidence digest")
    except (OSError, RuntimeError, ValueError):
        raise _invalid_operational_plan() from None


__all__ = [
    "MAX_TRUSTED_RELEASE_BYTES",
    "NAMESPACE",
    "PERSONAL_NAMESPACE_PREFIX",
    "REQUIRED_IMAGE_KEYS",
    "REQUIRED_POOLS",
    "PersonalDevAcceptanceActivation",
    "PersonalDevAcceptanceBuilder",
    "PersonalDevAcceptanceManager",
    "PersonalDevAcceptanceOwner",
    "PersonalDevAcceptancePlan",
    "PersonalDevAcceptancePlanError",
    "PersonalDevAcceptancePrincipals",
    "PersonalDevAcceptanceRelease",
    "PersonalDevAcceptanceSource",
    "PersonalDevAcceptanceStorage",
    "PersonalDevAcceptanceWindow",
    "PersonalDevBuilderTrust",
    "PersonalDevControlPlaneIdentities",
    "PersonalDevControlPlaneLimits",
    "PersonalDevControlPlaneNetwork",
    "PersonalDevControlPlaneProfile",
    "PersonalDevControlPlaneResources",
    "PersonalDevControlPlaneStorage",
    "PersonalDevOperationalApproval",
    "PersonalDevOperationalPlan",
    "PersonalDevOperationalPlanError",
    "PersonalDevTrustedImages",
    "PersonalDevTrustedRelease",
    "PersonalDevTrustedReleaseError",
    "PersonalDevTrustedScanner",
    "PoolCapability",
    "ResourceEnvelope",
    "load_personal_dev_acceptance_plan",
    "load_personal_dev_control_plane_profile",
    "load_personal_dev_operational_plan",
    "load_personal_dev_trusted_release",
    "validate_personal_dev_acceptance_plan",
    "validate_personal_dev_operational_plan",
]
