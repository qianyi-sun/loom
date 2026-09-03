"""Strict root-owned configuration for the task-image builder node guard."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path, PurePosixPath
from typing import cast
from urllib.parse import urlsplit

from loom_task_image_builder_guard.errors import GuardError
from loom_task_image_builder_guard.models import (
    AuthorityConfig,
    CommandConfig,
    CommandIdentity,
    ContainmentConfig,
    GuardConfigValue,
    IdentityConfig,
    IoLimit,
    ProtocolConfig,
    ServiceConfig,
    SlurmConfig,
)
from loom_task_image_builder_guard.safeio import read_stable_file

_CONFIG_SCHEMA = "loom.task-image-builder-node-guard-config/v1"
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,252}$")
_DEVICE = re.compile(r"^(0|[1-9][0-9]{0,9}):(0|[1-9][0-9]{0,9})$")
_WALL_TIME = re.compile(r"^[0-9]{2}:[0-5][0-9]:[0-5][0-9]$")
_MAX_CONFIG_BYTES = 1024 * 1024


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate key")
        result[key] = value
    return result


def _object(value: object, *, keys: frozenset[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != keys:
        raise GuardError("config_fields_invalid")
    if not all(isinstance(key, str) for key in value):
        raise GuardError("config_fields_invalid")
    return cast(dict[str, object], value)


def _integer(value: object, *, minimum: int, maximum: int = (1 << 31) - 1) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise GuardError("config_limit_invalid")
    return value


def _string(value: object, *, pattern: re.Pattern[str] = _NAME) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise GuardError("config_value_invalid")
    return value


def _digest(value: object) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None or value == "0" * 64:
        raise GuardError("config_digest_invalid")
    return value


def _path(value: object) -> Path:
    if not isinstance(value, str):
        raise GuardError("config_path_invalid")
    pure = PurePosixPath(value)
    if (
        not pure.is_absolute()
        or pure == PurePosixPath("/")
        or value.startswith("//")
        or value.endswith("/")
        or "\x00" in value
        or any(part in {"", ".", ".."} for part in value.split("/")[1:])
        or pure.as_posix() != value
    ):
        raise GuardError("config_path_invalid")
    return Path(value)


def _command(value: object) -> CommandIdentity:
    raw = _object(value, keys=frozenset({"path", "sha256"}))
    return CommandIdentity(path=_path(raw["path"]), sha256=_digest(raw["sha256"]))


class GuardConfig(GuardConfigValue):
    """Validated configuration with no permissive defaults."""

    @classmethod
    def from_file(cls, path: Path) -> GuardConfig:
        payload = read_stable_file(
            path,
            uid=os.geteuid(),
            gid=os.getegid(),
            mode=0o600,
            maximum=_MAX_CONFIG_BYTES,
        )
        try:
            document = json.loads(payload, object_pairs_hook=_pairs)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            raise GuardError("config_json_invalid") from None
        raw = _object(
            document,
            keys=frozenset(
                {
                    "schema",
                    "cluster_id",
                    "cpu_arch",
                    "node_name",
                    "identity",
                    "protocol",
                    "authority",
                    "commands",
                    "slurm",
                    "containment",
                    "service",
                }
            ),
        )
        if raw["schema"] != _CONFIG_SCHEMA:
            raise GuardError("config_schema_invalid")
        cluster = raw["cluster_id"]
        architecture = raw["cpu_arch"]
        node_name = _string(raw["node_name"])
        native = {
            "oldlab": ("x86_64", "trt-eai-oldlab-", "loom-task-image-builder-rootless-oldlab"),
            "gb10": ("arm64", "trt-gb10-", "loom-task-image-builder-rootless-gb10"),
        }
        if not isinstance(cluster, str) or cluster not in native:
            raise GuardError("config_native_pair_invalid")
        expected_arch, node_prefix, expected_qos = native[cluster]
        if architecture != expected_arch or not node_name.startswith(node_prefix):
            raise GuardError("config_native_pair_invalid")

        identity_raw = _object(
            raw["identity"],
            keys=frozenset({"uid", "gid", "forbidden_supplementary_gids", "supervisor_sha256"}),
        )
        uid = _integer(identity_raw["uid"], minimum=1)
        gid = _integer(identity_raw["gid"], minimum=1)
        forbidden_raw = identity_raw["forbidden_supplementary_gids"]
        if not isinstance(forbidden_raw, list):
            raise GuardError("config_identity_invalid")
        forbidden = tuple(_integer(item, minimum=0) for item in forbidden_raw)
        if forbidden != tuple(sorted(set(forbidden))) or gid in forbidden:
            raise GuardError("config_identity_invalid")
        identity = IdentityConfig(uid, gid, forbidden, _digest(identity_raw["supervisor_sha256"]))

        protocol_raw = _object(
            raw["protocol"],
            keys=frozenset(
                {
                    "socket_path",
                    "socket_mode",
                    "socket_gid",
                    "max_packet_bytes",
                    "max_pending_peers",
                    "requests_per_second",
                    "ack_timeout_seconds",
                }
            ),
        )
        socket_mode = _integer(protocol_raw["socket_mode"], minimum=0, maximum=0o777)
        socket_gid = _integer(protocol_raw["socket_gid"], minimum=1)
        if socket_mode != 0o660 or socket_gid != gid:
            raise GuardError("config_protocol_invalid")
        protocol = ProtocolConfig(
            socket_path=_path(protocol_raw["socket_path"]),
            socket_mode=socket_mode,
            socket_gid=socket_gid,
            max_packet_bytes=_integer(
                protocol_raw["max_packet_bytes"], minimum=256, maximum=64 * 1024
            ),
            max_pending_peers=_integer(protocol_raw["max_pending_peers"], minimum=1, maximum=128),
            requests_per_second=_integer(
                protocol_raw["requests_per_second"], minimum=1, maximum=1024
            ),
            ack_timeout_seconds=_integer(
                protocol_raw["ack_timeout_seconds"], minimum=1, maximum=30
            ),
        )

        authority_raw = _object(
            raw["authority"],
            keys=frozenset(
                {
                    "base_url",
                    "ca_path",
                    "cert_path",
                    "key_path",
                    "bearer_path",
                    "timeout_seconds",
                    "max_response_bytes",
                }
            ),
        )
        base_url = authority_raw["base_url"]
        if not isinstance(base_url, str):
            raise GuardError("config_authority_invalid")
        parsed_url = urlsplit(base_url)
        if (
            parsed_url.scheme != "https"
            or parsed_url.hostname is None
            or parsed_url.username is not None
            or parsed_url.password is not None
            or parsed_url.query
            or parsed_url.fragment
            or parsed_url.path not in {"", "/"}
        ):
            raise GuardError("config_authority_invalid")
        authority = AuthorityConfig(
            base_url=base_url.rstrip("/"),
            ca_path=_path(authority_raw["ca_path"]),
            cert_path=_path(authority_raw["cert_path"]),
            key_path=_path(authority_raw["key_path"]),
            bearer_path=_path(authority_raw["bearer_path"]),
            timeout_seconds=_integer(authority_raw["timeout_seconds"], minimum=1, maximum=60),
            max_response_bytes=_integer(
                authority_raw["max_response_bytes"], minimum=1024, maximum=1024 * 1024
            ),
        )

        commands_raw = _object(raw["commands"], keys=frozenset({"scontrol", "sacct", "bpftool"}))
        commands = CommandConfig(
            scontrol=_command(commands_raw["scontrol"]),
            sacct=_command(commands_raw["sacct"]),
            bpftool=_command(commands_raw["bpftool"]),
        )

        slurm_raw = _object(
            raw["slurm"],
            keys=frozenset(
                {
                    "request_sha256",
                    "account",
                    "partition",
                    "qos",
                    "feature",
                    "cpus",
                    "memory_mib",
                    "wall_time",
                }
            ),
        )
        qos = _string(slurm_raw["qos"])
        if (
            slurm_raw["account"] != "loom-task-builder"
            or slurm_raw["partition"] != "loom-task-builder"
            or slurm_raw["feature"] != "loom_rootless_buildkit"
            or qos != expected_qos
        ):
            raise GuardError("config_slurm_invalid")
        wall_time = slurm_raw["wall_time"]
        if not isinstance(wall_time, str) or _WALL_TIME.fullmatch(wall_time) is None:
            raise GuardError("config_slurm_invalid")
        slurm = SlurmConfig(
            request_sha256=_digest(slurm_raw["request_sha256"]),
            account="loom-task-builder",
            partition="loom-task-builder",
            qos=qos,
            feature="loom_rootless_buildkit",
            cpus=_integer(slurm_raw["cpus"], minimum=1, maximum=65_536),
            memory_mib=_integer(slurm_raw["memory_mib"], minimum=1),
            wall_time=wall_time,
        )

        containment_raw = _object(
            raw["containment"],
            keys=frozenset(
                {
                    "cgroup_root",
                    "bpffs_root",
                    "ledger_root",
                    "bpf_object_path",
                    "pids_max",
                    "io_limits",
                    "containment_policy_sha256",
                    "resource_profile_sha256",
                    "bpf_program_sha256",
                    "bpf_map_schema_sha256",
                }
            ),
        )
        io_raw = containment_raw["io_limits"]
        if not isinstance(io_raw, list) or not io_raw or len(io_raw) > 64:
            raise GuardError("config_io_invalid")
        io_limits: list[IoLimit] = []
        for item in io_raw:
            row = _object(
                item,
                keys=frozenset({"device", "rbps", "wbps", "riops", "wiops"}),
            )
            device = row["device"]
            if not isinstance(device, str) or _DEVICE.fullmatch(device) is None:
                raise GuardError("config_io_invalid")
            io_limits.append(
                IoLimit(
                    device=device,
                    rbps=_integer(row["rbps"], minimum=1, maximum=(1 << 63) - 1),
                    wbps=_integer(row["wbps"], minimum=1, maximum=(1 << 63) - 1),
                    riops=_integer(row["riops"], minimum=1, maximum=(1 << 63) - 1),
                    wiops=_integer(row["wiops"], minimum=1, maximum=(1 << 63) - 1),
                )
            )
        if [item.device for item in io_limits] != sorted({item.device for item in io_limits}):
            raise GuardError("config_io_invalid")
        containment = ContainmentConfig(
            cgroup_root=_path(containment_raw["cgroup_root"]),
            bpffs_root=_path(containment_raw["bpffs_root"]),
            ledger_root=_path(containment_raw["ledger_root"]),
            bpf_object_path=_path(containment_raw["bpf_object_path"]),
            pids_max=_integer(containment_raw["pids_max"], minimum=1),
            io_limits=tuple(io_limits),
            containment_policy_sha256=_digest(containment_raw["containment_policy_sha256"]),
            resource_profile_sha256=_digest(containment_raw["resource_profile_sha256"]),
            bpf_program_sha256=_digest(containment_raw["bpf_program_sha256"]),
            bpf_map_schema_sha256=_digest(containment_raw["bpf_map_schema_sha256"]),
        )

        service_raw = _object(
            raw["service"],
            keys=frozenset(
                {
                    "attestation_interval_seconds",
                    "attestation_lifetime_seconds",
                    "max_ledger_entries",
                }
            ),
        )
        interval = _integer(service_raw["attestation_interval_seconds"], minimum=1, maximum=60)
        lifetime = _integer(service_raw["attestation_lifetime_seconds"], minimum=2, maximum=60)
        if interval * 2 > lifetime:
            raise GuardError("config_attestation_invalid")
        service = ServiceConfig(
            attestation_interval_seconds=interval,
            attestation_lifetime_seconds=lifetime,
            max_ledger_entries=_integer(service_raw["max_ledger_entries"], minimum=1, maximum=4096),
        )
        return cls(
            cluster_id=cluster,
            cpu_arch=architecture,
            node_name=node_name,
            identity=identity,
            protocol=protocol,
            authority=authority,
            commands=commands,
            slurm=slurm,
            containment=containment,
            service=service,
        )


__all__ = ["GuardConfig"]
