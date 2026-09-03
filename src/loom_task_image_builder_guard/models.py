"""Immutable local policy values for the task-image builder guard."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class IdentityConfig:
    uid: int
    gid: int
    forbidden_supplementary_gids: tuple[int, ...]
    supervisor_path: Path
    supervisor_sha256: str


@dataclass(frozen=True, slots=True)
class ProtocolConfig:
    socket_path: Path
    socket_mode: int
    socket_gid: int
    max_packet_bytes: int
    max_pending_peers: int
    requests_per_second: int
    ack_timeout_seconds: int


@dataclass(frozen=True, slots=True)
class AuthorityConfig:
    base_url: str
    ca_path: Path
    cert_path: Path
    key_path: Path
    bearer_path: Path
    timeout_seconds: int
    max_response_bytes: int


@dataclass(frozen=True, slots=True)
class CommandIdentity:
    path: Path
    sha256: str


@dataclass(frozen=True, slots=True)
class CommandConfig:
    scontrol: CommandIdentity
    sacct: CommandIdentity
    bpftool: CommandIdentity


@dataclass(frozen=True, slots=True)
class SlurmConfig:
    cluster_name: str
    request_sha256: str
    account: str
    partition: str
    qos: str
    feature: str
    cpus: int
    memory_mib: int
    wall_time: str


@dataclass(frozen=True, slots=True)
class IoLimit:
    device: str
    rbps: int
    wbps: int
    riops: int
    wiops: int


@dataclass(frozen=True, slots=True)
class ContainmentConfig:
    cgroup_root: Path
    bpffs_root: Path
    ledger_root: Path
    bpf_object_path: Path
    network_policy_path: Path
    pids_max: int
    io_limits: tuple[IoLimit, ...]
    containment_policy_sha256: str
    resource_profile_sha256: str
    bpf_program_sha256: str
    bpf_map_schema_sha256: str


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    attestation_interval_seconds: int
    attestation_lifetime_seconds: int
    max_ledger_entries: int


@dataclass(frozen=True, slots=True)
class GuardConfigValue:
    cluster_id: str
    cpu_arch: str
    node_name: str
    identity: IdentityConfig
    protocol: ProtocolConfig
    authority: AuthorityConfig
    commands: CommandConfig
    slurm: SlurmConfig
    containment: ContainmentConfig
    service: ServiceConfig


__all__ = [
    "AuthorityConfig",
    "CommandConfig",
    "CommandIdentity",
    "ContainmentConfig",
    "GuardConfigValue",
    "IdentityConfig",
    "IoLimit",
    "ProtocolConfig",
    "ServiceConfig",
    "SlurmConfig",
]
