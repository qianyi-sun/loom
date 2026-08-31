from __future__ import annotations

import os
import socket
from pathlib import Path
from uuid import UUID

import certifi
import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from loom_personal_dev_native_builder_agent.__main__ import (
    create_native_builder_docker_client,
    create_native_builder_http_client,
    load_native_builder_agent_config,
)

_INSTANCE_ID = UUID("00000000-0000-0000-0000-000000000001")
_BOOT_ID = UUID("00000000-0000-0000-0000-000000000002")


def _files(tmp_path: Path) -> tuple[Path, Path, Path]:
    key = tmp_path / "agent.key"
    key.write_bytes(Ed25519PrivateKey.generate().private_bytes_raw())
    key.chmod(0o400)
    ca = tmp_path / "ca.pem"
    ca.write_bytes(Path(certifi.where()).read_bytes())
    ca.chmod(0o444)
    docker_socket = tmp_path / "docker.sock"
    listener = socket.socket(socket.AF_UNIX)
    listener.bind(str(docker_socket))
    listener.close()
    socket_gid = next(group for group in os.getgroups() if group != os.geteuid())
    os.chown(docker_socket, -1, socket_gid)
    docker_socket.chmod(0o660)
    return key, ca, docker_socket


def _environment(tmp_path: Path) -> dict[str, str]:
    key, ca, docker_socket = _files(tmp_path)
    uid = os.geteuid()
    socket_gid = docker_socket.stat().st_gid
    return {
        "HOME": "/nonexistent",
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LOOM_NATIVE_BUILDER_SERVICE_URL": "https://management.example",
        "LOOM_NATIVE_BUILDER_AGENT_INSTANCE_ID": str(_INSTANCE_ID),
        "LOOM_NATIVE_BUILDER_KEY_ID": "gb10-native-builder-v1",
        "LOOM_NATIVE_BUILDER_PRIVATE_KEY_FILE": str(key),
        "LOOM_NATIVE_BUILDER_CA_FILE": str(ca),
        "LOOM_NATIVE_BUILDER_AGENT_IMAGE": (
            "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:" + "a" * 64
        ),
        "LOOM_NATIVE_BUILDER_BUILDER_IMAGE": (
            "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:" + "b" * 64
        ),
        "LOOM_NATIVE_BUILDER_RUNTIME_PROFILE_SHA256": "c" * 64,
        "LOOM_NATIVE_BUILDER_DOCKER_SOCKET": str(docker_socket),
        "LOOM_NATIVE_BUILDER_AGENT_UID": str(uid),
        "LOOM_NATIVE_BUILDER_SOCKET_GID": str(socket_gid),
        "LOOM_NATIVE_BUILDER_MAX_CONCURRENCY": "2",
        "LOOM_NATIVE_BUILDER_POLL_INTERVAL_SECONDS": "2",
        "LOOM_NATIVE_BUILDER_HEARTBEAT_INTERVAL_SECONDS": "10",
        "LOOM_NATIVE_BUILDER_HEARTBEAT_GRACE_SECONDS": "30",
        "LOOM_NATIVE_BUILDER_HTTP_TIMEOUT_SECONDS": "15",
        "LOOM_NATIVE_BUILDER_HEALTH_TIMEOUT_SECONDS": "60",
        "LOOM_NATIVE_BUILDER_HEALTH_POLL_SECONDS": "0.5",
    }


def _load(environment: dict[str, str]):
    return load_native_builder_agent_config(
        environment,
        real_uid=os.getuid(),
        effective_uid=os.geteuid(),
        supplemental_groups=tuple(os.getgroups()),
        host_name="gx10-01c7",
        host_boot_id=_BOOT_ID,
        host_architecture="aarch64",
        required_docker_socket=(
            Path(environment["LOOM_NATIVE_BUILDER_PRIVATE_KEY_FILE"]).parent / "docker.sock"
        ),
        expected_socket_owner_uid=os.geteuid(),
    )


def test_native_builder_agent_config_accepts_only_exact_nonroot_runtime(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path)

    config = _load(environment)

    assert config.identity.agent_instance_id == _INSTANCE_ID
    assert config.identity.host_name == "gx10-01c7"
    assert config.identity.host_boot_id == _BOOT_ID
    assert config.identity.max_concurrency == 2
    assert config.poll_interval_seconds == 2
    assert config.heartbeat_interval_seconds == 10
    assert config.heartbeat_grace_seconds == 30

    client = create_native_builder_http_client(config)
    try:
        assert client.base_url == httpx.URL("https://management.example")
        assert client._trust_env is False
    finally:
        import asyncio

        asyncio.run(client.aclose())

    calls: list[dict[str, object]] = []

    def docker_factory(**kwargs):
        calls.append(kwargs)
        return object()

    created = create_native_builder_docker_client(config, factory=docker_factory)
    assert created is not None
    assert calls == [
        {
            "base_url": f"unix://{config.docker_socket}",
            "timeout": config.http_timeout_seconds,
            "version": "1.51",
        }
    ]


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda env: env.update({"UNEXPECTED_SECRET": "value"}), "environment"),
        (lambda env: env.update({"HTTPS_PROXY": "http://proxy"}), "proxy"),
        (
            lambda env: env.update({"LOOM_NATIVE_BUILDER_SERVICE_URL": "http://management"}),
            "service URL",
        ),
        (
            lambda env: env.update({"LOOM_NATIVE_BUILDER_DOCKER_SOCKET": "/var/run/docker.sock"}),
            "Docker socket",
        ),
        (lambda env: env.update({"LOOM_NATIVE_BUILDER_CA_FILE": "relative.pem"}), "CA"),
        (
            lambda env: env.update({"LOOM_NATIVE_BUILDER_AGENT_IMAGE": "agent:latest"}),
            "image",
        ),
        (
            lambda env: env.update({"LOOM_NATIVE_BUILDER_MAX_CONCURRENCY": "1"}),
            "concurrency",
        ),
        (
            lambda env: env.update({"LOOM_NATIVE_BUILDER_POLL_INTERVAL_SECONDS": "0"}),
            "interval",
        ),
        (
            lambda env: env.update({"LOOM_NATIVE_BUILDER_HEARTBEAT_GRACE_SECONDS": "301"}),
            "interval",
        ),
    ],
)
def test_native_builder_agent_config_rejects_environment_relaxation(
    tmp_path: Path,
    mutation,
    message: str,
) -> None:
    environment = _environment(tmp_path)
    mutation(environment)

    with pytest.raises(RuntimeError, match=message):
        _load(environment)


def test_native_builder_agent_config_rejects_privilege_and_file_drift(
    tmp_path: Path,
) -> None:
    environment = _environment(tmp_path)
    uid = os.geteuid()
    required_socket = tmp_path / "docker.sock"

    with pytest.raises(RuntimeError, match="non-root"):
        load_native_builder_agent_config(
            environment,
            real_uid=0,
            effective_uid=0,
            supplemental_groups=(int(environment["LOOM_NATIVE_BUILDER_SOCKET_GID"]),),
            host_name="gx10-01c7",
            host_boot_id=_BOOT_ID,
            host_architecture="aarch64",
            required_docker_socket=required_socket,
            expected_socket_owner_uid=os.geteuid(),
        )
    with pytest.raises(RuntimeError, match="UID"):
        load_native_builder_agent_config(
            environment,
            real_uid=uid,
            effective_uid=uid + 1,
            supplemental_groups=(int(environment["LOOM_NATIVE_BUILDER_SOCKET_GID"]),),
            host_name="gx10-01c7",
            host_boot_id=_BOOT_ID,
            host_architecture="aarch64",
            required_docker_socket=required_socket,
            expected_socket_owner_uid=os.geteuid(),
        )
    with pytest.raises(RuntimeError, match="socket group"):
        load_native_builder_agent_config(
            environment,
            real_uid=uid,
            effective_uid=uid,
            supplemental_groups=(),
            host_name="gx10-01c7",
            host_boot_id=_BOOT_ID,
            host_architecture="aarch64",
            required_docker_socket=required_socket,
            expected_socket_owner_uid=os.geteuid(),
        )

    key = Path(environment["LOOM_NATIVE_BUILDER_PRIVATE_KEY_FILE"])
    key.chmod(0o440)
    with pytest.raises(RuntimeError, match="private key"):
        _load(environment)

    key.chmod(0o400)
    docker_socket = Path(environment["LOOM_NATIVE_BUILDER_DOCKER_SOCKET"])
    docker_socket.chmod(0o600)
    with pytest.raises(RuntimeError, match="Docker socket"):
        _load(environment)
