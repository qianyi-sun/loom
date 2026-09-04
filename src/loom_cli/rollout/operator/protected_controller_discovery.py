"""Strict read-only discovery contracts for protected capacity controllers."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal, cast

from .protected_controller_prerequisite_component import (
    controller_local_authority_sha256,
)

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_DATA_PARSER_RE = re.compile(r"^data_parser/v[0-9]+\.[0-9]+\.[0-9]+$")
_POOLS = frozenset({"gb10", "oldlab"})
_CONTROLLER_HOSTS = {"gb10": "gx10-01c7", "oldlab": "TRT-EAI-OLDLAB-1"}
_ARCHITECTURES = {"gb10": "arm64", "oldlab": "amd64"}
_SLURM_CLUSTERS = {"gb10": "trt-gb10", "oldlab": "trt-oldlab"}
_TARGET_NODES = {
    "gb10": tuple(f"trt-gb10-{index}" for index in (1, *range(3, 16))),
    "oldlab": tuple(f"trt-eai-oldlab-{index}" for index in range(3, 6)),
}
_EXECUTABLE_NAMES = frozenset({"sacct", "sacctmgr", "sbatch", "scancel", "scontrol", "squeue"})
_CONFIGURATION_NAMES = frozenset({"slurm.conf"})
_MAX_DISCOVERY_BYTES = 256 * 1024
_SERVICE_USER = "loom_capacity_executor"


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def _digest(value: object, *, label: str) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError(f"controller discovery {label} is invalid")
    return value


def _digest_map(
    value: Mapping[str, str],
    *,
    expected: frozenset[str],
    label: str,
) -> Mapping[str, str]:
    copied = dict(value)
    if set(copied) != set(expected):
        raise ValueError(f"controller discovery {label} is invalid")
    for digest in copied.values():
        _digest(digest, label=label)
    return MappingProxyType(dict(sorted(copied.items())))


def _decode(payload: bytes, *, label: str) -> Mapping[str, object]:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= _MAX_DISCOVERY_BYTES:
        raise ValueError(f"controller discovery {label} bytes are invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except json.JSONDecodeError as exc:
        raise ValueError(f"controller discovery {label} bytes are invalid") from exc
    if not isinstance(value, Mapping):
        raise ValueError(f"controller discovery {label} fields are invalid")
    if _canonical_json(value) != payload:
        raise ValueError(f"controller discovery {label} is not canonical")
    return value


def _string_field(value: Mapping[str, object], name: str) -> str:
    found = value.get(name)
    if not isinstance(found, str):
        raise ValueError("controller discovery fields are invalid")
    return found


def _integer_field(value: Mapping[str, object], name: str) -> int:
    found = value.get(name)
    if type(found) is not int:
        raise ValueError("controller discovery fields are invalid")
    return found


def _string_tuple(value: object, *, length: int | None = None) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or (length is not None and len(value) != length)
        or any(not isinstance(item, str) for item in value)
    ):
        raise ValueError("controller discovery fields are invalid")
    return tuple(value)


def _version_tuple(value: object) -> tuple[int, int, int]:
    if (
        not isinstance(value, list)
        or len(value) != 3
        or any(type(item) is not int for item in value)
    ):
        raise ValueError("controller discovery fields are invalid")
    return cast(tuple[int, int, int], tuple(value))


def _string_map(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping) or any(
        not isinstance(name, str) or not isinstance(item, str) for name, item in value.items()
    ):
        raise ValueError("controller discovery fields are invalid")
    return dict(value)


def controller_job_visibility_evidence_sha256(
    *,
    pool_id: str,
    partition_fields: Mapping[str, str],
    association_fields: tuple[str, ...],
) -> str:
    """Bind the exact scheduler admission observed as the executor principal."""

    if pool_id == "oldlab":
        admission = {
            "allow_groups": partition_fields.get("AllowGroups"),
            "query_principal": _SERVICE_USER,
        }
        if admission["allow_groups"] != "loom-rollout" or association_fields:
            raise ValueError("controller discovery scheduler admission is invalid")
    elif pool_id == "gb10":
        admission = {
            "allow_accounts": partition_fields.get("AllowAccounts"),
            "allow_qos": partition_fields.get("AllowQos"),
            "query_principal": _SERVICE_USER,
        }
        if (
            admission["allow_accounts"] != "loom-staging"
            or admission["allow_qos"] != "loom-staging"
            or association_fields
            != (
                "trt-gb10",
                "loom-staging",
                _SERVICE_USER,
                "loom-staging",
                "loom-staging",
                "loom-staging",
            )
        ):
            raise ValueError("controller discovery scheduler admission is invalid")
    else:
        raise ValueError("controller discovery scheduler admission is invalid")
    return hashlib.sha256(
        _canonical_json(
            {
                "admission": admission,
                "association_fields": list(association_fields),
                "partition": "loom-staging",
                "pool_id": pool_id,
                "schema_version": 1,
            }
        )
    ).hexdigest()


@dataclass(frozen=True, slots=True)
class ControllerDiscoveryRequest:
    """One pool-bound request that grants no mutation authority."""

    schema_version: Literal[1]
    pool_id: str
    transport_authority_sha256: str

    def __post_init__(self) -> None:
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or self.pool_id not in _POOLS
        ):
            raise ValueError("controller discovery request identity is invalid")
        _digest(self.transport_authority_sha256, label="request authority")

    def to_bytes(self) -> bytes:
        return _canonical_json(
            {
                "pool_id": self.pool_id,
                "schema_version": self.schema_version,
                "transport_authority_sha256": self.transport_authority_sha256,
            }
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ControllerDiscoveryRequest:
        value = _decode(payload, label="request")
        if (
            set(value) != {"pool_id", "schema_version", "transport_authority_sha256"}
            or type(value.get("schema_version")) is not int
            or value.get("schema_version") != 1
        ):
            raise ValueError("controller discovery request fields are invalid")
        try:
            return cls(
                schema_version=1,
                pool_id=_string_field(value, "pool_id"),
                transport_authority_sha256=_string_field(value, "transport_authority_sha256"),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("controller discovery request identity is invalid") from exc


@dataclass(frozen=True, slots=True)
class ControllerDiscoveryEvidence:
    """A stable, secret-free observation of one physical controller."""

    schema_version: Literal[1]
    pool_id: str
    transport_authority_sha256: str
    controller_hostname: str
    architecture: str
    service_user: str
    service_uid: int
    service_gid: int
    slurm_cluster: str
    partition: str
    target_nodes: tuple[str, ...]
    slurm_version: tuple[int, int, int]
    data_parser: str
    query_principal: str
    manager_client_cidr: str
    executable_sha256: Mapping[str, str]
    configuration_sha256: Mapping[str, str]
    job_visibility_evidence_sha256: str
    local_authority_sha256: str

    def __post_init__(self) -> None:
        expected_nodes = _TARGET_NODES.get(self.pool_id)
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or self.pool_id not in _POOLS
            or self.controller_hostname != _CONTROLLER_HOSTS.get(self.pool_id)
            or self.architecture != _ARCHITECTURES.get(self.pool_id)
            or self.service_user != "loom_capacity_executor"
            or self.query_principal != self.service_user
            or type(self.service_uid) is not int
            or type(self.service_gid) is not int
            or self.service_uid <= 0
            or self.service_gid <= 0
            or self.slurm_cluster != _SLURM_CLUSTERS.get(self.pool_id)
            or self.partition != "loom-staging"
            or not isinstance(self.target_nodes, tuple)
            or self.target_nodes != expected_nodes
            or not isinstance(self.slurm_version, tuple)
            or len(self.slurm_version) != 3
            or any(type(item) is not int or item < 0 for item in self.slurm_version)
            or self.slurm_version[:2] != (23, 11)
            or not isinstance(self.data_parser, str)
            or _DATA_PARSER_RE.fullmatch(self.data_parser) is None
        ):
            raise ValueError("controller discovery evidence identity is invalid")
        try:
            route = ipaddress.ip_network(self.manager_client_cidr, strict=True)
        except (TypeError, ValueError) as exc:
            raise ValueError("controller discovery manager route is invalid") from exc
        if (
            not isinstance(route, ipaddress.IPv4Network)
            or route.prefixlen != 32
            or not route.network_address.is_private
            or self.manager_client_cidr != str(route)
        ):
            raise ValueError("controller discovery manager route is invalid")
        executables = _digest_map(
            self.executable_sha256,
            expected=_EXECUTABLE_NAMES,
            label="executable inventory",
        )
        configurations = _digest_map(
            self.configuration_sha256,
            expected=_CONFIGURATION_NAMES,
            label="configuration inventory",
        )
        _digest(self.transport_authority_sha256, label="transport authority")
        _digest(self.job_visibility_evidence_sha256, label="job visibility evidence")
        _digest(self.local_authority_sha256, label="local authority")
        expected_local_authority = controller_local_authority_sha256(
            pool_id=self.pool_id,
            architecture=self.architecture,
            controller_hostname=self.controller_hostname,
            service_uid=self.service_uid,
            service_gid=self.service_gid,
            slurm_cluster=self.slurm_cluster,
            partition=self.partition,
            target_nodes=self.target_nodes,
            executable_sha256=executables,
            configuration_sha256=configurations,
            job_visibility_evidence_sha256=self.job_visibility_evidence_sha256,
        )
        if expected_local_authority != self.local_authority_sha256:
            raise ValueError("controller discovery local authority is invalid")
        object.__setattr__(self, "executable_sha256", executables)
        object.__setattr__(self, "configuration_sha256", configurations)

    @property
    def evidence_sha256(self) -> str:
        return hashlib.sha256(self.to_bytes()).hexdigest()

    def to_bytes(self) -> bytes:
        return _canonical_json(
            {
                "architecture": self.architecture,
                "configuration_sha256": dict(self.configuration_sha256),
                "controller_hostname": self.controller_hostname,
                "data_parser": self.data_parser,
                "executable_sha256": dict(self.executable_sha256),
                "local_authority_sha256": self.local_authority_sha256,
                "job_visibility_evidence_sha256": self.job_visibility_evidence_sha256,
                "manager_client_cidr": self.manager_client_cidr,
                "partition": self.partition,
                "pool_id": self.pool_id,
                "query_principal": self.query_principal,
                "schema_version": self.schema_version,
                "service_gid": self.service_gid,
                "service_uid": self.service_uid,
                "service_user": self.service_user,
                "slurm_cluster": self.slurm_cluster,
                "slurm_version": list(self.slurm_version),
                "target_nodes": list(self.target_nodes),
                "transport_authority_sha256": self.transport_authority_sha256,
            }
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> ControllerDiscoveryEvidence:
        value = _decode(payload, label="evidence")
        expected = {
            "architecture",
            "configuration_sha256",
            "controller_hostname",
            "data_parser",
            "executable_sha256",
            "local_authority_sha256",
            "job_visibility_evidence_sha256",
            "manager_client_cidr",
            "partition",
            "pool_id",
            "query_principal",
            "schema_version",
            "service_gid",
            "service_uid",
            "service_user",
            "slurm_cluster",
            "slurm_version",
            "target_nodes",
            "transport_authority_sha256",
        }
        if set(value) != expected:
            raise ValueError("controller discovery evidence fields are invalid")
        if type(value.get("schema_version")) is not int or value.get("schema_version") != 1:
            raise ValueError("controller discovery evidence fields are invalid")
        try:
            return cls(
                schema_version=1,
                pool_id=_string_field(value, "pool_id"),
                transport_authority_sha256=_string_field(value, "transport_authority_sha256"),
                controller_hostname=_string_field(value, "controller_hostname"),
                architecture=_string_field(value, "architecture"),
                service_user=_string_field(value, "service_user"),
                service_uid=_integer_field(value, "service_uid"),
                service_gid=_integer_field(value, "service_gid"),
                slurm_cluster=_string_field(value, "slurm_cluster"),
                partition=_string_field(value, "partition"),
                target_nodes=_string_tuple(value.get("target_nodes")),
                slurm_version=_version_tuple(value.get("slurm_version")),
                data_parser=_string_field(value, "data_parser"),
                query_principal=_string_field(value, "query_principal"),
                manager_client_cidr=_string_field(value, "manager_client_cidr"),
                executable_sha256=_string_map(value.get("executable_sha256")),
                configuration_sha256=_string_map(value.get("configuration_sha256")),
                job_visibility_evidence_sha256=_string_field(
                    value, "job_visibility_evidence_sha256"
                ),
                local_authority_sha256=_string_field(value, "local_authority_sha256"),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("controller discovery evidence identity is invalid") from exc


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("controller discovery document contains duplicate fields")
        value[key] = item
    return value


__all__ = [
    "ControllerDiscoveryEvidence",
    "ControllerDiscoveryRequest",
    "controller_job_visibility_evidence_sha256",
]
