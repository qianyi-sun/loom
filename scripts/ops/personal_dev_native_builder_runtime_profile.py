"""Exact inert host profile for the native personal-development builder."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import NoReturn

_MAX_PROFILE_BYTES = 64 * 1024
_DIGEST_ALPHABET = frozenset("0123456789abcdef")
_MEMBERS: dict[str, dict[str, object]] = {
    "containerd-shim-runsc-v1": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "01d14562fa37f8364e03daeb9d7c075303fc5a855b9e391d9dd48332e378372b",
        "size": 40_996_082,
    },
    "gvisor-bin/checkpointgofer": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "1657354d07034c88b04bfa1fbeae5f6b0e604b5c06014ad69074570bb5bf4697",
        "size": 65_454_284,
    },
    "gvisor-bin/gvisor_sentry": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "e6cd92fc1e8ef48b4b8eebb48885fbfd53b3d0887104b0ba098f734f8ace1ec5",
        "size": 45_132_467,
    },
    "gvisor-bin/runsc-metric-server": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "634e880d71f598f0e356b9a865ca88e8413cbd0da0e3ac031e7a37b28dc2828d",
        "size": 49_185_196,
    },
    "runsc": {
        "archive_mode": 0o755,
        "install_mode": 0o555,
        "sha256": "ac5dbcbacc73f5999b24d322e7ef127db6bd5a41645de816afde228efbaaea70",
        "size": 97_995_630,
    },
}
_RUNTIME_FLAGS = {
    "allow-flag-override": "false",
    "allow-packet-socket-write": "false",
    "allow-suid": "false",
    "debug": "false",
    "directfs": "false",
    "file-access": "exclusive",
    "file-access-mounts": "shared",
    "gvisor-marker-file": "true",
    "host-fifo": "none",
    "host-settings": "check",
    "host-uds": "none",
    "ignore-cgroups": "false",
    "mount-cgroup-v2": "true",
    "net-raw": "false",
    "network": "sandbox",
    "oci-seccomp": "true",
    "platform": "kvm",
    "platform_device_path": "/dev/kvm",
    "profile": "false",
    "restore-spec-validation": "enforce",
    "rootless": "false",
    "sidecar-release-enforcement-policy": "ALWAYS",
    "strace": "false",
    "systemd-cgroup": "true",
    "watchdog-action": "panic",
}
_EXPECTED_PROFILE: dict[str, object] = {
    "archive": {
        "members": _MEMBERS,
        "sha512": (
            "dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff529"
            "2d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7"
        ),
        "url": (
            "https://storage.googleapis.com/gvisor/releases/"
            "release/20260810/aarch64/gvisor.tar.bz2"
        ),
    },
    "host": {
        "architecture": "aarch64",
        "cgroup_driver": "systemd",
        "cgroup_version": 2,
        "device": "/dev/kvm",
        "docker_api_version": "1.51",
        "docker_storage_driver": "overlay2",
        "docker_version": "28.3.3",
        "minimum_cpus": 20,
        "minimum_disk_free_bytes": 100_000_000_000,
        "minimum_memory_bytes": 120_000_000_000,
        "name": "gx10-01c7",
    },
    "identity": {
        "agent_gid": 24_850,
        "agent_name": "loom-pdev-native",
        "agent_uid": 24_850,
        "private_key_mode": 0o400,
        "socket_gid": 24_851,
        "socket_group": "loom-pdev-docker",
        "socket_mode": 0o660,
    },
    "installation": {
        "agent_service": (
            "/etc/systemd/system/loom-personal-dev-native-builder-agent.service"
        ),
        "agent_service_template": (
            "/etc/loom/personal-dev-native-builder/"
            "loom-personal-dev-native-builder-agent.service.in"
        ),
        "agent_state": "/var/lib/loom-personal-dev-builder/agent",
        "ca_file": "/etc/loom/personal-dev-native-builder/service-ca.pem",
        "config_root": "/etc/loom/personal-dev-native-builder",
        "data_root": "/var/lib/loom-personal-dev-builder",
        "dockerd_config": "/etc/loom/personal-dev-native-builder/dockerd.json",
        "dockerd_service": (
            "/etc/systemd/system/loom-personal-dev-builder-dockerd.service"
        ),
        "docker_socket": "/run/loom-personal-dev-builder/docker.sock",
        "exec_root": "/run/loom-personal-dev-builder",
        "nftables": (
            "/etc/loom/personal-dev-native-builder/provider-network.nft"
        ),
        "primary_docker_socket": "/var/run/docker.sock",
        "private_key": (
            "/etc/loom/personal-dev-native-builder/agent-ed25519"
        ),
        "profile": (
            "/etc/loom/personal-dev-native-builder/runtime-profile-v1.json"
        ),
        "release_root": "/opt/loom/gvisor/release-20260810.0",
        "runsc_config": "/etc/loom/personal-dev-native-builder/runsc.toml",
        "slice_unit": (
            "/etc/systemd/system/loom-personal-dev-builder.slice"
        ),
        "sysusers": (
            "/etc/sysusers.d/loom-personal-dev-native-builder.conf"
        ),
    },
    "network": {
        "address_pool": "172.28.0.0/16",
        "bridge_prefix_length": 24,
        "dns_servers": ["1.1.1.1", "1.0.0.1"],
        "egress_tcp_ports": [80, 443],
        "nft_table": "loom_personal_dev_builder",
    },
    "release": {
        "runsc_spec_version": "1.2.1",
        "tag_commit": "5ceb9a5fd5750d6c73dd166441f28306039300d0",
        "version": "release-20260810.0",
    },
    "resources": {
        "agent_cpu_nanos": 1_000_000_000,
        "agent_memory_bytes": 1024**3,
        "agent_pids_max": 256,
        "buildkit_cpu_nanos": 3_000_000_000,
        "buildkit_memory_bytes": 16 * 1024**3,
        "client_cpu_nanos": 1_000_000_000,
        "client_memory_bytes": 16 * 1024**3,
        "max_concurrency": 2,
        "slice_cpu_quota_percent": 900,
        "slice_memory_max_bytes": 72 * 1024**3,
        "slice_tasks_max": 8192,
    },
    "runtime": {
        "flags": _RUNTIME_FLAGS,
        "handler": "runsc-personal-dev-native",
        "runtime_type": "io.containerd.runsc.v1",
    },
    "schema": "loom.personal-dev-native-builder-runtime-profile.v1",
}


class NativeBuilderRuntimeProfileError(RuntimeError):
    """The native builder runtime profile is unavailable or unreviewed."""


@dataclass(frozen=True, slots=True)
class NativeBuilderRuntimeArchiveMember:
    size: int
    archive_mode: int
    install_mode: int
    sha256: str


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise NativeBuilderRuntimeProfileError(
                "native builder runtime profile contains duplicate fields"
            )
        result[key] = value
    return result


def _reject_constant(value: str) -> NoReturn:
    del value
    raise NativeBuilderRuntimeProfileError(
        "native builder runtime profile is invalid JSON"
    )


def _canonical_json(value: object) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            indent=2,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    ).encode("ascii")


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _read_profile(path: Path) -> bytes:
    if not isinstance(path, Path) or not path.is_absolute():
        raise NativeBuilderRuntimeProfileError(
            "native builder runtime profile path is invalid"
        )
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or not 0 < before.st_size <= _MAX_PROFILE_BYTES
        ):
            raise NativeBuilderRuntimeProfileError(
                "native builder runtime profile file is unsafe"
            )
        payload = bytearray()
        while chunk := os.read(
            descriptor,
            min(64 * 1024, before.st_size + 1 - len(payload)),
        ):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if (
            len(payload) != before.st_size
            or _file_identity(before) != _file_identity(after)
        ):
            raise NativeBuilderRuntimeProfileError(
                "native builder runtime profile changed while being read"
            )
        return bytes(payload)
    except NativeBuilderRuntimeProfileError:
        raise
    except OSError as exc:
        raise NativeBuilderRuntimeProfileError(
            "native builder runtime profile is unavailable"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class NativeBuilderRuntimeProfile:
    payload: bytes
    sha256: str
    members: Mapping[str, NativeBuilderRuntimeArchiveMember]
    runtime_flags: Mapping[str, str]
    archive_sha512: str
    archive_url: str
    version: str
    tag_commit: str
    runsc_spec_version: str
    host_name: str
    architecture: str
    docker_version: str
    docker_api_version: str
    docker_storage_driver: str
    docker_cgroup_driver: str
    cgroup_version: int
    minimum_cpus: int
    minimum_memory_bytes: int
    minimum_disk_free_bytes: int
    device_path: Path
    agent_name: str
    agent_uid: int
    agent_gid: int
    socket_group: str
    socket_gid: int
    private_key_mode: int
    socket_mode: int
    release_root: Path
    config_root: Path
    data_root: Path
    exec_root: Path
    profile_path: Path
    runsc_config_path: Path
    dockerd_config_path: Path
    nftables_path: Path
    docker_socket: Path
    primary_docker_socket: Path
    private_key_path: Path
    ca_file_path: Path
    dockerd_service_path: Path
    agent_service_path: Path
    agent_service_template_path: Path
    slice_unit_path: Path
    sysusers_path: Path
    agent_state_path: Path
    address_pool: str
    bridge_prefix_length: int
    dns_servers: tuple[str, ...]
    egress_tcp_ports: tuple[int, ...]
    nft_table: str
    handler: str
    runtime_type: str
    max_concurrency: int
    agent_cpu_nanos: int
    agent_memory_bytes: int
    agent_pids_max: int
    slice_cpu_quota_percent: int
    slice_memory_max_bytes: int
    slice_tasks_max: int
    client_cpu_nanos: int
    buildkit_cpu_nanos: int
    client_memory_bytes: int
    buildkit_memory_bytes: int

    @property
    def runsc_path(self) -> Path:
        return self.release_root / "runsc"

    @property
    def shim_path(self) -> Path:
        return self.release_root / "containerd-shim-runsc-v1"

    @property
    def runsc_toml(self) -> bytes:
        lines = [f'binary_name = "{self.runsc_path}"', "", "[runsc_config]"]
        lines.extend(
            f'  {name} = "{self.runtime_flags[name]}"'
            for name in sorted(self.runtime_flags)
        )
        return ("\n".join(lines) + "\n").encode("ascii")

    @property
    def dockerd_json(self) -> bytes:
        return _canonical_json(
            {
                "bridge": "none",
                "data-root": str(self.data_root / "docker"),
                "default-address-pools": [
                    {
                        "base": self.address_pool,
                        "size": self.bridge_prefix_length,
                    }
                ],
                "default-runtime": self.handler,
                "exec-root": str(self.exec_root / "docker"),
                "hosts": [f"unix://{self.docker_socket}"],
                "ip-forward": True,
                "ip-masq": False,
                "ip6tables": False,
                "iptables": False,
                "ipv6": False,
                "live-restore": False,
                "log-driver": "local",
                "log-opts": {"max-file": "3", "max-size": "10m"},
                "no-new-privileges": False,
                "pidfile": str(self.exec_root / "dockerd.pid"),
                "runtimes": {
                    self.handler: {
                        "options": {
                            "ConfigPath": str(self.runsc_config_path),
                            "TypeUrl": "io.containerd.runsc.v1.options",
                        },
                        "runtimeType": str(self.shim_path),
                    }
                },
                "storage-driver": self.docker_storage_driver,
                "userland-proxy": False,
            }
        )

    @property
    def nftables(self) -> bytes:
        dns = ", ".join(self.dns_servers)
        ports = ", ".join(str(value) for value in self.egress_tcp_ports)
        return f"""table inet {self.nft_table} {{
  chain input {{
    type filter hook input priority filter; policy accept;
    ip saddr {self.address_pool} counter drop
  }}

  chain forward {{
    type filter hook forward priority filter; policy accept;
    ip daddr {self.address_pool} ct state established,related accept
    ip saddr {self.address_pool} ip daddr {self.address_pool} drop
    ip saddr {self.address_pool} ip daddr 0.0.0.0/8 drop
    ip saddr {self.address_pool} ip daddr 10.0.0.0/8 drop
    ip saddr {self.address_pool} ip daddr 100.64.0.0/10 drop
    ip saddr {self.address_pool} ip daddr 127.0.0.0/8 drop
    ip saddr {self.address_pool} ip daddr 169.254.0.0/16 drop
    ip saddr {self.address_pool} ip daddr 172.16.0.0/12 drop
    ip saddr {self.address_pool} ip daddr 192.0.0.0/24 drop
    ip saddr {self.address_pool} ip daddr 192.0.2.0/24 drop
    ip saddr {self.address_pool} ip daddr 192.168.0.0/16 drop
    ip saddr {self.address_pool} ip daddr 198.18.0.0/15 drop
    ip saddr {self.address_pool} ip daddr 198.51.100.0/24 drop
    ip saddr {self.address_pool} ip daddr 203.0.113.0/24 drop
    ip saddr {self.address_pool} ip daddr 224.0.0.0/4 drop
    ip saddr {self.address_pool} ip daddr 240.0.0.0/4 drop
    ip saddr {self.address_pool} ip daddr {{ {dns} }} udp dport 53 accept
    ip saddr {self.address_pool} ip daddr {{ {dns} }} tcp dport 53 accept
    ip saddr {self.address_pool} udp dport 53 drop
    ip saddr {self.address_pool} tcp dport 53 drop
    ip saddr {self.address_pool} tcp dport {{ {ports} }} accept
    ip saddr {self.address_pool} drop
  }}

  chain postrouting {{
    type nat hook postrouting priority srcnat; policy accept;
    ip saddr {self.address_pool} ip daddr != {self.address_pool} masquerade
  }}
}}
""".encode("ascii")

    @property
    def slice_unit(self) -> bytes:
        return f"""[Unit]
Description=Loom personal development native builder resource envelope

[Slice]
CPUAccounting=yes
CPUQuota={self.slice_cpu_quota_percent}%
MemoryAccounting=yes
MemoryMax={self.slice_memory_max_bytes}
TasksAccounting=yes
TasksMax={self.slice_tasks_max}
""".encode("ascii")

    @property
    def sysusers(self) -> bytes:
        return f"""g {self.agent_name} {self.agent_gid}
g {self.socket_group} {self.socket_gid}
u {self.agent_name} {self.agent_uid}:{self.agent_gid} \"Loom native builder agent\" /nonexistent /usr/sbin/nologin
m {self.agent_name} {self.socket_group}
""".encode("ascii")

    @property
    def dockerd_service(self) -> bytes:
        return f"""[Unit]
Description=Loom personal development dedicated native builder Docker daemon
After=network-online.target
Wants=network-online.target
ConditionPathIsReadWrite={self.data_root}
ConditionPathIsReadWrite={self.exec_root}
ConditionPathExists={self.device_path}

[Service]
Type=notify
ExecStart=/usr/bin/dockerd --config-file={self.dockerd_config_path}
ExecStartPost=/usr/bin/chgrp {self.socket_gid} {self.docker_socket}
ExecStartPost=/usr/bin/chmod {self.socket_mode:o} {self.docker_socket}
Restart=on-failure
RestartSec=5s
TimeoutStartSec=120s
TimeoutStopSec=120s
KillMode=process
Delegate=yes
TasksMax=infinity
LimitNOFILE=1048576
UMask=0027
Slice=loom-personal-dev-builder.slice
""".encode("ascii")

    @property
    def agent_service_template(self) -> bytes:
        key_mount = "/run/loom-native-builder/agent-ed25519"
        ca_mount = "/run/loom-native-builder/service-ca.pem"
        return f"""[Unit]
Description=Loom personal development native builder agent
After=network-online.target loom-personal-dev-builder-dockerd.service
Wants=network-online.target
Requires=loom-personal-dev-builder-dockerd.service

[Service]
Type=simple
ExecStartPre=/usr/bin/docker image inspect @@AGENT_IMAGE@@
ExecStart=/usr/bin/docker run --rm --name=loom-personal-dev-native-builder-agent --pull=never --cgroup-parent={self.slice_unit_path.name} --cpus={self.agent_cpu_nanos // 1_000_000_000} --memory={self.agent_memory_bytes} --memory-swap={self.agent_memory_bytes} --pids-limit={self.agent_pids_max} --read-only --cap-drop=ALL --security-opt=no-new-privileges:true --user={self.agent_uid}:{self.agent_gid} --group-add={self.socket_gid} --hostname={self.host_name} --mount=type=bind,src={self.private_key_path},dst={key_mount},readonly --mount=type=bind,src={self.ca_file_path},dst={ca_mount},readonly --mount=type=bind,src={self.docker_socket},dst={self.docker_socket} --tmpfs=/tmp:rw,nosuid,nodev,noexec,size=67108864,mode=0700,uid={self.agent_uid},gid={self.agent_gid} --env=LOOM_NATIVE_BUILDER_SERVICE_URL=@@SERVICE_URL@@ --env=LOOM_NATIVE_BUILDER_AGENT_INSTANCE_ID=@@AGENT_INSTANCE_ID@@ --env=LOOM_NATIVE_BUILDER_KEY_ID=@@KEY_ID@@ --env=LOOM_NATIVE_BUILDER_PRIVATE_KEY_FILE={key_mount} --env=LOOM_NATIVE_BUILDER_CA_FILE={ca_mount} --env=LOOM_NATIVE_BUILDER_AGENT_IMAGE=@@AGENT_IMAGE@@ --env=LOOM_NATIVE_BUILDER_BUILDER_IMAGE=@@BUILDER_IMAGE@@ --env=LOOM_NATIVE_BUILDER_RUNTIME_PROFILE_SHA256=@@RUNTIME_PROFILE_SHA256@@ --env=LOOM_NATIVE_BUILDER_DOCKER_SOCKET={self.docker_socket} --env=LOOM_NATIVE_BUILDER_AGENT_UID={self.agent_uid} --env=LOOM_NATIVE_BUILDER_SOCKET_GID={self.socket_gid} --env=LOOM_NATIVE_BUILDER_MAX_CONCURRENCY={self.max_concurrency} --env=LOOM_NATIVE_BUILDER_POLL_INTERVAL_SECONDS=2 --env=LOOM_NATIVE_BUILDER_HEARTBEAT_INTERVAL_SECONDS=10 --env=LOOM_NATIVE_BUILDER_HEARTBEAT_GRACE_SECONDS=30 --env=LOOM_NATIVE_BUILDER_HTTP_TIMEOUT_SECONDS=15 --env=LOOM_NATIVE_BUILDER_HEALTH_TIMEOUT_SECONDS=60 --env=LOOM_NATIVE_BUILDER_HEALTH_POLL_SECONDS=0.5 --env=LOOM_NATIVE_BUILDER_LOG_LEVEL=INFO @@AGENT_IMAGE@@
ExecStop=/usr/bin/docker stop --time=30 loom-personal-dev-native-builder-agent
Restart=on-failure
RestartSec=5s
TimeoutStartSec=120s
TimeoutStopSec=60s
Slice=loom-personal-dev-builder.slice
UMask=0077
""".encode("ascii")


def load_native_builder_runtime_profile(
    path: Path,
) -> NativeBuilderRuntimeProfile:
    payload = _read_profile(path)
    try:
        value = json.loads(
            payload.decode("ascii"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except NativeBuilderRuntimeProfileError:
        raise
    except (
        RecursionError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        TypeError,
        ValueError,
    ) as exc:
        raise NativeBuilderRuntimeProfileError(
            "native builder runtime profile is invalid JSON"
        ) from exc
    if (
        not isinstance(value, dict)
        or payload != _canonical_json(value)
        or value != _EXPECTED_PROFILE
    ):
        raise NativeBuilderRuntimeProfileError(
            "native builder runtime profile differs from the reviewed contract"
        )
    archive = value["archive"]
    host = value["host"]
    identity = value["identity"]
    installation = value["installation"]
    network = value["network"]
    release = value["release"]
    resources = value["resources"]
    runtime = value["runtime"]
    assert isinstance(archive, dict)
    assert isinstance(host, dict)
    assert isinstance(identity, dict)
    assert isinstance(installation, dict)
    assert isinstance(network, dict)
    assert isinstance(release, dict)
    assert isinstance(resources, dict)
    assert isinstance(runtime, dict)
    raw_members = archive["members"]
    raw_flags = runtime["flags"]
    assert isinstance(raw_members, dict)
    assert isinstance(raw_flags, dict)
    members = MappingProxyType(
        {
            name: NativeBuilderRuntimeArchiveMember(
                size=int(member["size"]),
                archive_mode=int(member["archive_mode"]),
                install_mode=int(member["install_mode"]),
                sha256=str(member["sha256"]),
            )
            for name, member in raw_members.items()
            if isinstance(member, dict)
        }
    )
    digest = hashlib.sha256(payload).hexdigest()
    if len(digest) != 64 or not set(digest) <= _DIGEST_ALPHABET:
        raise NativeBuilderRuntimeProfileError(
            "native builder runtime profile digest is invalid"
        )  # pragma: no cover
    return NativeBuilderRuntimeProfile(
        payload=payload,
        sha256=digest,
        members=members,
        runtime_flags=MappingProxyType(
            {str(name): str(item) for name, item in raw_flags.items()}
        ),
        archive_sha512=str(archive["sha512"]),
        archive_url=str(archive["url"]),
        version=str(release["version"]),
        tag_commit=str(release["tag_commit"]),
        runsc_spec_version=str(release["runsc_spec_version"]),
        host_name=str(host["name"]),
        architecture=str(host["architecture"]),
        docker_version=str(host["docker_version"]),
        docker_api_version=str(host["docker_api_version"]),
        docker_storage_driver=str(host["docker_storage_driver"]),
        docker_cgroup_driver=str(host["cgroup_driver"]),
        cgroup_version=int(host["cgroup_version"]),
        minimum_cpus=int(host["minimum_cpus"]),
        minimum_memory_bytes=int(host["minimum_memory_bytes"]),
        minimum_disk_free_bytes=int(host["minimum_disk_free_bytes"]),
        device_path=Path(str(host["device"])),
        agent_name=str(identity["agent_name"]),
        agent_uid=int(identity["agent_uid"]),
        agent_gid=int(identity["agent_gid"]),
        socket_group=str(identity["socket_group"]),
        socket_gid=int(identity["socket_gid"]),
        private_key_mode=int(identity["private_key_mode"]),
        socket_mode=int(identity["socket_mode"]),
        release_root=Path(str(installation["release_root"])),
        config_root=Path(str(installation["config_root"])),
        data_root=Path(str(installation["data_root"])),
        exec_root=Path(str(installation["exec_root"])),
        profile_path=Path(str(installation["profile"])),
        runsc_config_path=Path(str(installation["runsc_config"])),
        dockerd_config_path=Path(str(installation["dockerd_config"])),
        nftables_path=Path(str(installation["nftables"])),
        docker_socket=Path(str(installation["docker_socket"])),
        primary_docker_socket=Path(str(installation["primary_docker_socket"])),
        private_key_path=Path(str(installation["private_key"])),
        ca_file_path=Path(str(installation["ca_file"])),
        dockerd_service_path=Path(str(installation["dockerd_service"])),
        agent_service_path=Path(str(installation["agent_service"])),
        agent_service_template_path=Path(
            str(installation["agent_service_template"])
        ),
        slice_unit_path=Path(str(installation["slice_unit"])),
        sysusers_path=Path(str(installation["sysusers"])),
        agent_state_path=Path(str(installation["agent_state"])),
        address_pool=str(network["address_pool"]),
        bridge_prefix_length=int(network["bridge_prefix_length"]),
        dns_servers=tuple(str(item) for item in network["dns_servers"]),
        egress_tcp_ports=tuple(int(item) for item in network["egress_tcp_ports"]),
        nft_table=str(network["nft_table"]),
        handler=str(runtime["handler"]),
        runtime_type=str(runtime["runtime_type"]),
        max_concurrency=int(resources["max_concurrency"]),
        agent_cpu_nanos=int(resources["agent_cpu_nanos"]),
        agent_memory_bytes=int(resources["agent_memory_bytes"]),
        agent_pids_max=int(resources["agent_pids_max"]),
        slice_cpu_quota_percent=int(resources["slice_cpu_quota_percent"]),
        slice_memory_max_bytes=int(resources["slice_memory_max_bytes"]),
        slice_tasks_max=int(resources["slice_tasks_max"]),
        client_cpu_nanos=int(resources["client_cpu_nanos"]),
        buildkit_cpu_nanos=int(resources["buildkit_cpu_nanos"]),
        client_memory_bytes=int(resources["client_memory_bytes"]),
        buildkit_memory_bytes=int(resources["buildkit_memory_bytes"]),
    )


__all__ = [
    "NativeBuilderRuntimeArchiveMember",
    "NativeBuilderRuntimeProfile",
    "NativeBuilderRuntimeProfileError",
    "load_native_builder_runtime_profile",
]
