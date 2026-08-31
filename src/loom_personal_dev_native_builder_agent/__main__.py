"""Run the authority-minimal native personal-development builder agent."""

from __future__ import annotations

import asyncio
import logging
import os
import platform
import socket
import ssl
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

import docker
import httpx

from loom.personal_dev_native_builder_agent import (
    DockerPersonalDevNativeBuildRuntime,
    HttpPersonalDevNativeBuilderAuthority,
    NativeBuilderAgentIdentity,
    NativeBuilderRuntimeInventory,
    PersonalDevNativeBuilderAgent,
)
from loom.personal_dev_native_builder_protocol import (
    NATIVE_BUILDER_MAX_CONCURRENCY,
    NATIVE_BUILDER_PLATFORM,
    NATIVE_BUILDER_PROTOCOL_VERSION,
    NATIVE_BUILDER_PROVIDER,
    load_personal_dev_native_builder_signer,
)

logger = logging.getLogger(__name__)

_DOCKER_SOCKET = Path("/run/loom-personal-dev-builder/docker.sock")
_PROXY_NAMES = {
    "ALL_PROXY",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "NO_PROXY",
    "all_proxy",
    "http_proxy",
    "https_proxy",
    "no_proxy",
}
_RUNTIME_NAMES = {
    "LOOM_NATIVE_BUILDER_SERVICE_URL",
    "LOOM_NATIVE_BUILDER_AGENT_INSTANCE_ID",
    "LOOM_NATIVE_BUILDER_KEY_ID",
    "LOOM_NATIVE_BUILDER_PRIVATE_KEY_FILE",
    "LOOM_NATIVE_BUILDER_CA_FILE",
    "LOOM_NATIVE_BUILDER_AGENT_IMAGE",
    "LOOM_NATIVE_BUILDER_BUILDER_IMAGE",
    "LOOM_NATIVE_BUILDER_RUNTIME_PROFILE_SHA256",
    "LOOM_NATIVE_BUILDER_DOCKER_SOCKET",
    "LOOM_NATIVE_BUILDER_AGENT_UID",
    "LOOM_NATIVE_BUILDER_SOCKET_GID",
    "LOOM_NATIVE_BUILDER_MAX_CONCURRENCY",
    "LOOM_NATIVE_BUILDER_POLL_INTERVAL_SECONDS",
    "LOOM_NATIVE_BUILDER_HEARTBEAT_INTERVAL_SECONDS",
    "LOOM_NATIVE_BUILDER_HEARTBEAT_GRACE_SECONDS",
    "LOOM_NATIVE_BUILDER_HTTP_TIMEOUT_SECONDS",
    "LOOM_NATIVE_BUILDER_HEALTH_TIMEOUT_SECONDS",
    "LOOM_NATIVE_BUILDER_HEALTH_POLL_SECONDS",
    "LOOM_NATIVE_BUILDER_LOG_LEVEL",
}
_CONTAINER_NAMES = {
    "HOME",
    "HOSTNAME",
    "LANG",
    "LC_ALL",
    "PATH",
    "PYTHONPATH",
    "PYTHONUNBUFFERED",
    "TZ",
}


@dataclass(frozen=True, slots=True)
class NativeBuilderAgentConfig:
    identity: NativeBuilderAgentIdentity
    service_url: str
    private_key_file: Path
    ca_file: Path
    docker_socket: Path
    socket_gid: int
    poll_interval_seconds: float
    heartbeat_interval_seconds: float
    heartbeat_grace_seconds: int
    http_timeout_seconds: float
    health_timeout_seconds: int
    health_poll_seconds: float


def _required(environment: Mapping[str, str], name: str) -> str:
    value = environment.get(name)
    if not isinstance(value, str) or not value or value != value.strip():
        raise RuntimeError("native builder environment is invalid")
    return value


def _integer(
    environment: Mapping[str, str],
    name: str,
    *,
    minimum: int,
    maximum: int,
    error_label: str = "environment integer",
) -> int:
    try:
        value = int(_required(environment, name))
    except ValueError:
        raise RuntimeError(f"native builder {error_label} is invalid") from None
    if not minimum <= value <= maximum:
        raise RuntimeError(f"native builder {error_label} is invalid")
    return value


def _interval(
    environment: Mapping[str, str],
    name: str,
    *,
    minimum: float,
    maximum: float,
) -> float:
    try:
        value = float(_required(environment, name))
    except ValueError:
        raise RuntimeError("native builder interval is invalid") from None
    if not minimum <= value <= maximum:
        raise RuntimeError("native builder interval is invalid")
    return value


def _origin(value: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError("native builder service URL is invalid")
    return value.rstrip("/")


def _regular_file(
    path: Path,
    *,
    label: str,
    owner_uid: int | None,
    modes: set[int],
    exact_size: int | None = None,
) -> None:
    if not path.is_absolute():
        raise RuntimeError(f"native builder {label} path is invalid")
    try:
        metadata = path.lstat()
    except OSError:
        raise RuntimeError(f"native builder {label} is invalid") from None
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or (owner_uid is not None and metadata.st_uid != owner_uid)
        or stat.S_IMODE(metadata.st_mode) not in modes
        or (exact_size is not None and metadata.st_size != exact_size)
        or (exact_size is None and not 0 < metadata.st_size <= 1024 * 1024)
    ):
        raise RuntimeError(f"native builder {label} is invalid")


def load_native_builder_agent_config(
    environment: Mapping[str, str],
    *,
    real_uid: int | None = None,
    effective_uid: int | None = None,
    supplemental_groups: Sequence[int] | None = None,
    host_name: str | None = None,
    host_boot_id: UUID | None = None,
    host_architecture: str | None = None,
    required_docker_socket: Path = _DOCKER_SOCKET,
    expected_socket_owner_uid: int = 0,
) -> NativeBuilderAgentConfig:
    """Parse and verify the complete startup authority and privilege boundary."""
    names = set(environment)
    if names & _PROXY_NAMES:
        raise RuntimeError("native builder proxy environment is forbidden")
    if not names <= _RUNTIME_NAMES | _CONTAINER_NAMES:
        raise RuntimeError("native builder environment contains unknown fields")

    uid = os.getuid() if real_uid is None else real_uid
    euid = os.geteuid() if effective_uid is None else effective_uid
    groups = tuple(os.getgroups()) if supplemental_groups is None else tuple(supplemental_groups)
    configured_uid = _integer(
        environment,
        "LOOM_NATIVE_BUILDER_AGENT_UID",
        minimum=1,
        maximum=2**31 - 1,
    )
    if uid == 0 or euid == 0:
        raise RuntimeError("native builder agent must run as non-root")
    if uid != euid or configured_uid != uid:
        raise RuntimeError("native builder agent UID is invalid")
    socket_gid = _integer(
        environment,
        "LOOM_NATIVE_BUILDER_SOCKET_GID",
        minimum=1,
        maximum=2**31 - 1,
    )
    if socket_gid == configured_uid or socket_gid not in groups:
        raise RuntimeError("native builder socket group is invalid")

    private_key_file = Path(_required(environment, "LOOM_NATIVE_BUILDER_PRIVATE_KEY_FILE"))
    _regular_file(
        private_key_file,
        label="private key",
        owner_uid=euid,
        modes={0o400},
        exact_size=32,
    )
    ca_file = Path(_required(environment, "LOOM_NATIVE_BUILDER_CA_FILE"))
    _regular_file(
        ca_file,
        label="CA file",
        owner_uid=None,
        modes={0o400, 0o440, 0o444},
    )

    docker_socket = Path(_required(environment, "LOOM_NATIVE_BUILDER_DOCKER_SOCKET"))
    if docker_socket != required_docker_socket:
        raise RuntimeError("native builder Docker socket path is invalid")
    try:
        socket_metadata = docker_socket.lstat()
    except OSError:
        raise RuntimeError("native builder Docker socket is invalid") from None
    if (
        not stat.S_ISSOCK(socket_metadata.st_mode)
        or socket_metadata.st_uid != expected_socket_owner_uid
        or socket_metadata.st_gid != socket_gid
        or stat.S_IMODE(socket_metadata.st_mode) != 0o660
    ):
        raise RuntimeError("native builder Docker socket is invalid")

    poll_interval = _interval(
        environment,
        "LOOM_NATIVE_BUILDER_POLL_INTERVAL_SECONDS",
        minimum=0.1,
        maximum=30,
    )
    heartbeat_interval = _interval(
        environment,
        "LOOM_NATIVE_BUILDER_HEARTBEAT_INTERVAL_SECONDS",
        minimum=1,
        maximum=60,
    )
    heartbeat_grace = _integer(
        environment,
        "LOOM_NATIVE_BUILDER_HEARTBEAT_GRACE_SECONDS",
        minimum=2,
        maximum=300,
        error_label="interval",
    )
    if poll_interval > heartbeat_interval or heartbeat_grace < 2 * heartbeat_interval:
        raise RuntimeError("native builder interval relationship is invalid")
    http_timeout = _interval(
        environment,
        "LOOM_NATIVE_BUILDER_HTTP_TIMEOUT_SECONDS",
        minimum=1,
        maximum=60,
    )
    health_timeout = _integer(
        environment,
        "LOOM_NATIVE_BUILDER_HEALTH_TIMEOUT_SECONDS",
        minimum=1,
        maximum=300,
    )
    health_poll = _interval(
        environment,
        "LOOM_NATIVE_BUILDER_HEALTH_POLL_SECONDS",
        minimum=0.05,
        maximum=5,
    )
    if health_poll >= health_timeout:
        raise RuntimeError("native builder interval relationship is invalid")

    concurrency = _integer(
        environment,
        "LOOM_NATIVE_BUILDER_MAX_CONCURRENCY",
        minimum=NATIVE_BUILDER_MAX_CONCURRENCY,
        maximum=NATIVE_BUILDER_MAX_CONCURRENCY,
        error_label="concurrency",
    )
    actual_host = socket.gethostname() if host_name is None else host_name
    actual_architecture = platform.machine() if host_architecture is None else host_architecture
    if actual_host != "gx10-01c7" or actual_architecture != "aarch64":
        raise RuntimeError("native builder host identity is invalid")
    if host_boot_id is None:
        try:
            host_boot_id = UUID(
                Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
            )
        except (OSError, ValueError):
            raise RuntimeError("native builder host boot identity is invalid") from None

    try:
        identity = NativeBuilderAgentIdentity(
            agent_instance_id=UUID(_required(environment, "LOOM_NATIVE_BUILDER_AGENT_INSTANCE_ID")),
            agent_key_id=_required(environment, "LOOM_NATIVE_BUILDER_KEY_ID"),
            provider=NATIVE_BUILDER_PROVIDER,
            platform=NATIVE_BUILDER_PLATFORM,
            protocol_version=NATIVE_BUILDER_PROTOCOL_VERSION,
            host_name=actual_host,
            host_architecture=actual_architecture,
            host_boot_id=host_boot_id,
            agent_image=_required(environment, "LOOM_NATIVE_BUILDER_AGENT_IMAGE"),
            builder_image=_required(environment, "LOOM_NATIVE_BUILDER_BUILDER_IMAGE"),
            runtime_profile_sha256=_required(
                environment,
                "LOOM_NATIVE_BUILDER_RUNTIME_PROFILE_SHA256",
            ),
            max_concurrency=concurrency,
        )
        identity.status(
            NativeBuilderRuntimeInventory(
                managed_grant_ids=(),
                active_grant_ids=(),
                available=True,
                unavailable_reason=None,
                readiness_evidence_sha256="1" * 64,
            )
        )
    except (TypeError, ValueError):
        raise RuntimeError("native builder image or identity is invalid") from None

    return NativeBuilderAgentConfig(
        identity=identity,
        service_url=_origin(_required(environment, "LOOM_NATIVE_BUILDER_SERVICE_URL")),
        private_key_file=private_key_file,
        ca_file=ca_file,
        docker_socket=docker_socket,
        socket_gid=socket_gid,
        poll_interval_seconds=poll_interval,
        heartbeat_interval_seconds=heartbeat_interval,
        heartbeat_grace_seconds=heartbeat_grace,
        http_timeout_seconds=http_timeout,
        health_timeout_seconds=health_timeout,
        health_poll_seconds=health_poll,
    )


def create_native_builder_http_client(
    config: NativeBuilderAgentConfig,
) -> httpx.AsyncClient:
    tls_context = ssl.create_default_context(cafile=str(config.ca_file))
    return httpx.AsyncClient(
        base_url=config.service_url,
        timeout=config.http_timeout_seconds,
        verify=tls_context,
        trust_env=False,
    )


def create_native_builder_docker_client(
    config: NativeBuilderAgentConfig,
    *,
    factory: Callable[..., Any] = docker.DockerClient,
) -> Any:
    return factory(
        base_url=f"unix://{config.docker_socket}",
        timeout=config.http_timeout_seconds,
        version="1.51",
    )


async def _run() -> None:
    config = load_native_builder_agent_config(os.environ)
    signer = load_personal_dev_native_builder_signer(
        config.private_key_file,
        key_id=config.identity.agent_key_id,
    )
    docker_client = create_native_builder_docker_client(config)
    try:
        runtime = DockerPersonalDevNativeBuildRuntime(
            client=docker_client,
            socket_path=str(config.docker_socket),
            identity=config.identity,
            health_timeout_seconds=config.health_timeout_seconds,
            health_poll_seconds=config.health_poll_seconds,
        )
        async with create_native_builder_http_client(config) as http_client:
            agent = PersonalDevNativeBuilderAgent(
                authority=HttpPersonalDevNativeBuilderAuthority(http_client),
                runtime=runtime,
                signer=signer,
                identity=config.identity,
                heartbeat_grace_seconds=config.heartbeat_grace_seconds,
                heartbeat_interval_seconds=config.heartbeat_interval_seconds,
            )
            while True:
                try:
                    await agent.reconcile_once(now=datetime.now(UTC))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.error(
                        "personal_dev_native_builder_iteration_failed",
                        extra={"error_type": type(exc).__name__},
                    )
                await asyncio.sleep(config.poll_interval_seconds)
    finally:
        docker_client.close()


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOOM_NATIVE_BUILDER_LOG_LEVEL", "INFO").upper(),
    )
    asyncio.run(_run())


if __name__ == "__main__":
    main()


__all__ = [
    "NativeBuilderAgentConfig",
    "create_native_builder_docker_client",
    "create_native_builder_http_client",
    "load_native_builder_agent_config",
    "main",
]
