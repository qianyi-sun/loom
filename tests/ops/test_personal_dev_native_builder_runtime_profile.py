from __future__ import annotations

import ipaddress
import json
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from scripts.ops.personal_dev_native_builder_runtime_profile import (
    NativeBuilderRuntimeProfileError,
    load_native_builder_runtime_profile,
)

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE_PATH = _ROOT / "deploy/personal-dev-native-builder/runtime-profile-v1.json"
_DEPLOY_ROOT = _PROFILE_PATH.parent
_ARCHIVE_SHA512 = (
    "dc21bdc7a4f52d049f4da74a337fc7437b2ac1465c7479816a852120a8cff529"
    "2d72ae78bc4c581f857836bc9a56a1ba18ad687e6bef13d03fdd670d6f2071f7"
)
_MEMBERS = {
    "containerd-shim-runsc-v1": (
        40_996_082,
        "01d14562fa37f8364e03daeb9d7c075303fc5a855b9e391d9dd48332e378372b",
    ),
    "gvisor-bin/checkpointgofer": (
        65_454_284,
        "1657354d07034c88b04bfa1fbeae5f6b0e604b5c06014ad69074570bb5bf4697",
    ),
    "gvisor-bin/gvisor_sentry": (
        45_132_467,
        "e6cd92fc1e8ef48b4b8eebb48885fbfd53b3d0887104b0ba098f734f8ace1ec5",
    ),
    "gvisor-bin/runsc-metric-server": (
        49_185_196,
        "634e880d71f598f0e356b9a865ca88e8413cbd0da0e3ac031e7a37b28dc2828d",
    ),
    "runsc": (
        97_995_630,
        "ac5dbcbacc73f5999b24d322e7ef127db6bd5a41645de816afde228efbaaea70",
    ),
}


def _value() -> dict[str, Any]:
    return json.loads(_PROFILE_PATH.read_text(encoding="ascii"))


def _canonical(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, indent=2, ensure_ascii=True, allow_nan=False)
        + "\n"
    ).encode("ascii")


def test_missing_native_runtime_profile_fails_closed(tmp_path: Path) -> None:
    with pytest.raises(NativeBuilderRuntimeProfileError, match="profile"):
        load_native_builder_runtime_profile(tmp_path / "missing.json")


def test_checked_profile_binds_measured_arm64_release_and_host() -> None:
    profile = load_native_builder_runtime_profile(_PROFILE_PATH)

    assert profile.archive_sha512 == _ARCHIVE_SHA512
    assert {
        name: (member.size, member.sha256)
        for name, member in profile.members.items()
    } == _MEMBERS
    assert all(member.archive_mode == 0o755 for member in profile.members.values())
    assert all(member.install_mode == 0o555 for member in profile.members.values())
    assert profile.archive_url == (
        "https://storage.googleapis.com/gvisor/releases/"
        "release/20260810/aarch64/gvisor.tar.bz2"
    )
    assert profile.version == "release-20260810.0"
    assert profile.tag_commit == "5ceb9a5fd5750d6c73dd166441f28306039300d0"
    assert profile.runsc_spec_version == "1.2.1"
    assert profile.host_name == "gx10-01c7"
    assert profile.architecture == "aarch64"
    assert profile.docker_version == "28.3.3"
    assert profile.docker_api_version == "1.51"
    assert profile.docker_storage_driver == "overlay2"
    assert profile.docker_cgroup_driver == "systemd"
    assert profile.minimum_cpus == 20
    assert profile.minimum_memory_bytes == 120_000_000_000
    assert profile.minimum_disk_free_bytes == 100_000_000_000


def test_profile_binds_distinct_identity_paths_and_resource_envelopes() -> None:
    profile = load_native_builder_runtime_profile(_PROFILE_PATH)

    assert (profile.agent_uid, profile.agent_gid, profile.socket_gid) == (
        24_850,
        24_850,
        24_851,
    )
    assert profile.agent_uid != profile.socket_gid
    assert profile.private_key_mode == 0o400
    assert profile.socket_mode == 0o660
    assert profile.max_concurrency == 2
    assert profile.agent_cpu_nanos == 1_000_000_000
    assert profile.agent_memory_bytes == 1024**3
    assert profile.agent_pids_max == 256
    assert profile.slice_cpu_quota_percent == 900
    assert profile.slice_memory_max_bytes == 72 * 1024**3
    assert profile.client_cpu_nanos == 1_000_000_000
    assert profile.buildkit_cpu_nanos == 3_000_000_000
    assert profile.client_memory_bytes == 16 * 1024**3
    assert profile.buildkit_memory_bytes == 16 * 1024**3
    assert profile.release_root == Path("/opt/loom/gvisor/release-20260810.0")
    assert profile.config_root == Path("/etc/loom/personal-dev-native-builder")
    assert profile.data_root == Path("/var/lib/loom-personal-dev-builder")
    assert profile.exec_root == Path("/run/loom-personal-dev-builder")
    assert profile.docker_socket == Path(
        "/run/loom-personal-dev-builder/docker.sock"
    )
    assert profile.primary_docker_socket == Path("/var/run/docker.sock")
    assert profile.docker_socket != profile.primary_docker_socket


def test_profile_binds_nonoverlapping_pool_and_public_dns() -> None:
    profile = load_native_builder_runtime_profile(_PROFILE_PATH)

    pool = ipaddress.ip_network(profile.address_pool, strict=True)
    occupied = tuple(
        ipaddress.ip_network(value, strict=True)
        for value in (
            "10.42.0.0/24",
            "172.17.0.0/16",
            "172.18.0.0/16",
            "172.19.0.0/16",
            "172.20.0.0/16",
            "192.168.20.0/24",
        )
    )
    assert pool == ipaddress.ip_network("172.28.0.0/16")
    assert profile.bridge_prefix_length == 24
    assert all(not pool.overlaps(value) for value in occupied)
    assert profile.dns_servers == ("1.1.1.1", "1.0.0.1")
    assert all(ipaddress.ip_address(value).is_global for value in profile.dns_servers)
    assert profile.egress_tcp_ports == (80, 443)
    assert profile.nft_table == "loom_personal_dev_builder"


def test_generated_runtime_files_match_checked_in_bytes() -> None:
    profile = load_native_builder_runtime_profile(_PROFILE_PATH)

    expected = {
        "runsc.toml": profile.runsc_toml,
        "dockerd.json": profile.dockerd_json,
        "provider-network.nft": profile.nftables,
        "loom-personal-dev-builder.slice": profile.slice_unit,
        "loom-personal-dev-native-builder.sysusers": profile.sysusers,
        "loom-personal-dev-builder-dockerd.service": profile.dockerd_service,
        "loom-personal-dev-native-builder-agent.service.in": (
            profile.agent_service_template
        ),
    }
    assert {
        name: (_DEPLOY_ROOT / name).read_bytes()
        for name in expected
    } == expected


def test_generated_dockerd_and_runsc_configs_are_exact_and_inert() -> None:
    profile = load_native_builder_runtime_profile(_PROFILE_PATH)
    dockerd = json.loads(profile.dockerd_json)
    runsc = tomllib.loads(profile.runsc_toml.decode("ascii"))

    assert dockerd == {
        "bridge": "none",
        "data-root": str(profile.data_root / "docker"),
        "default-address-pools": [
            {"base": "172.28.0.0/16", "size": 24},
        ],
        "default-runtime": "runsc-personal-dev-native",
        "dns": ["1.1.1.1", "1.0.0.1"],
        "exec-root": str(profile.exec_root / "docker"),
        "hosts": [f"unix://{profile.docker_socket}"],
        "ip-forward": True,
        "ip-masq": False,
        "ip6tables": False,
        "iptables": False,
        "ipv6": False,
        "live-restore": False,
        "log-driver": "local",
        "log-opts": {"max-file": "3", "max-size": "10m"},
        "no-new-privileges": False,
        "pidfile": str(profile.exec_root / "dockerd.pid"),
        "runtimes": {
            "runsc-personal-dev-native": {
                "options": {
                    "ConfigPath": str(profile.runsc_config_path),
                    "TypeUrl": "io.containerd.runsc.v1.options",
                },
                "runtimeType": str(profile.shim_path),
            }
        },
        "storage-driver": "overlay2",
        "userland-proxy": False,
    }
    assert runsc["binary_name"] == str(profile.runsc_path)
    flags = runsc["runsc_config"]
    assert flags == dict(profile.runtime_flags)
    assert flags["platform"] == "kvm"
    assert flags["network"] == "sandbox"
    assert flags["file-access"] == "exclusive"
    assert flags["host-uds"] == "none"
    assert flags["host-fifo"] == "none"
    assert flags["net-raw"] == "false"
    assert flags["oci-seccomp"] == "true"
    assert flags["allow-suid"] == "false"
    assert flags["allow-flag-override"] == "false"
    assert flags["sidecar-release-enforcement-policy"] == "ALWAYS"


def test_generated_network_and_units_keep_authorities_separate() -> None:
    profile = load_native_builder_runtime_profile(_PROFILE_PATH)
    nftables = profile.nftables.decode("ascii")
    dockerd = profile.dockerd_service.decode("ascii")
    agent = profile.agent_service_template.decode("ascii")
    sysusers = profile.sysusers.decode("ascii")

    assert "table inet loom_personal_dev_builder" in nftables
    assert "172.28.0.0/16" in nftables
    assert "1.1.1.1" in nftables and "1.0.0.1" in nftables
    assert "tcp dport { 80, 443 } accept" in nftables
    assert "type filter hook output priority filter; policy accept;" in nftables
    assert nftables.count("ip daddr 172.28.0.0/16 counter drop") == 2
    assert (
        "ip saddr 172.28.0.0/24 ip daddr 172.28.0.0/24 accept" in nftables
    )
    assert (
        "ip saddr 172.28.1.0/24 ip daddr 172.28.1.0/24 accept" in nftables
    )
    assert nftables.index(
        "ip daddr 172.28.0.0/16 ct state established,related accept"
    ) < nftables.index(
        "ip saddr 172.28.0.0/24 ip daddr 172.28.0.0/24 accept"
    ) < nftables.rindex("ip daddr 172.28.0.0/16 counter drop")
    assert "ip daddr 10.0.0.0/8 drop" in nftables
    assert "ip daddr 192.168.0.0/16 drop" in nftables
    assert "masquerade" in nftables
    assert "ip6" not in nftables.casefold()

    assert "WantedBy=" not in dockerd
    assert "Restart=no" in dockerd
    assert "KillMode=control-group" in dockerd
    assert "Slice=loom-personal-dev-builder.slice" in dockerd
    assert str(profile.dockerd_config_path) in dockerd
    assert str(profile.primary_docker_socket) not in dockerd
    assert "slurm" not in (dockerd + agent + nftables).casefold()
    assert "kubectl" not in (dockerd + agent + nftables).casefold()
    assert "WantedBy=" not in agent
    assert "@@AGENT_IMAGE@@" in agent
    assert "@@BUILDER_IMAGE@@" in agent
    assert "@@SERVICE_URL@@" in agent
    assert "@@AGENT_INSTANCE_ID@@" in agent
    assert "@@KEY_ID@@" in agent
    assert "@@RUNTIME_PROFILE_SHA256@@" in agent
    assert str(profile.docker_socket) in agent
    assert str(profile.primary_docker_socket) not in agent
    assert f"{profile.agent_uid}:{profile.agent_gid}" in agent
    assert f"--group-add={profile.socket_gid}" in agent
    assert "--cgroup-parent=loom-personal-dev-builder.slice" in agent
    assert "--cpus=1" in agent
    assert "--memory=1073741824" in agent
    assert "--memory-swap=1073741824" in agent
    assert "--pids-limit=256" in agent
    assert "Slice=loom-personal-dev-builder.slice" in agent
    assert f"u {profile.agent_name} {profile.agent_uid}:{profile.agent_gid}" in sysusers
    assert f"g {profile.socket_group} {profile.socket_gid}" in sysusers


def _mutate_extra(value: dict[str, Any]) -> None:
    value["extra"] = True


def _mutate_host(value: dict[str, Any]) -> None:
    value["host"]["name"] = "other"


def _mutate_arch(value: dict[str, Any]) -> None:
    value["host"]["architecture"] = "amd64"


def _mutate_docker(value: dict[str, Any]) -> None:
    value["host"]["docker_version"] = "28.3.4"


def _mutate_platform(value: dict[str, Any]) -> None:
    value["runtime"]["flags"]["platform"] = "ptrace"


def _mutate_runtime_relaxation(value: dict[str, Any]) -> None:
    value["runtime"]["flags"]["host-uds"] = "open"


def _mutate_route(value: dict[str, Any]) -> None:
    value["network"]["address_pool"] = "172.17.0.0/16"


def _mutate_path(value: dict[str, Any]) -> None:
    value["installation"]["config_root"] = "/tmp/loom"


def _mutate_mode(value: dict[str, Any]) -> None:
    value["identity"]["private_key_mode"] = 0o440


def _mutate_dns(value: dict[str, Any]) -> None:
    value["network"]["dns_servers"] = ["192.168.20.1", "1.0.0.1"]


def _mutate_resource(value: dict[str, Any]) -> None:
    value["resources"]["slice_memory_max_bytes"] = 80 * 1024**3


def _mutate_identity_collision(value: dict[str, Any]) -> None:
    value["identity"]["socket_gid"] = value["identity"]["agent_uid"]


def _mutate_member(value: dict[str, Any]) -> None:
    value["archive"]["members"]["runsc"]["size"] += 1


@pytest.mark.parametrize(
    "mutation",
    (
        _mutate_extra,
        _mutate_host,
        _mutate_arch,
        _mutate_docker,
        _mutate_platform,
        _mutate_runtime_relaxation,
        _mutate_route,
        _mutate_path,
        _mutate_mode,
        _mutate_dns,
        _mutate_resource,
        _mutate_identity_collision,
        _mutate_member,
    ),
)
def test_profile_rejects_every_reviewed_contract_drift(
    tmp_path: Path,
    mutation: Callable[[dict[str, Any]], None],
) -> None:
    value = _value()
    mutation(value)
    path = tmp_path / "profile.json"
    path.write_bytes(_canonical(value))

    with pytest.raises(NativeBuilderRuntimeProfileError, match="profile"):
        load_native_builder_runtime_profile(path)


def test_profile_rejects_duplicate_noncanonical_and_unsafe_files(
    tmp_path: Path,
) -> None:
    duplicate = tmp_path / "duplicate.json"
    duplicate.write_bytes(b'{"schema":"one","schema":"two"}\n')
    noncanonical = tmp_path / "noncanonical.json"
    noncanonical.write_bytes(json.dumps(_value(), sort_keys=True).encode("ascii"))
    symlink = tmp_path / "profile-link.json"
    symlink.symlink_to(_PROFILE_PATH)

    for path in (duplicate, noncanonical, symlink):
        with pytest.raises(NativeBuilderRuntimeProfileError, match="profile"):
            load_native_builder_runtime_profile(path)
