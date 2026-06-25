"""Unit coverage for DockerDriver StartOptions plumbing."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

import pytest

import loom.driver.docker as docker_module
from loom.driver.base import StartOptions
from loom.driver.docker import DockerDriver


async def test_docker_driver_passes_extra_hosts_to_container_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_kwargs: dict[str, Any] = {}

    class _Containers:
        def run(self, image: str, **kwargs: Any) -> object:
            run_kwargs["image"] = image
            run_kwargs.update(kwargs)
            return object()

    class _Client:
        containers = _Containers()

        def close(self) -> None:
            pass

    monkeypatch.setattr(docker_module.docker, "from_env", lambda: _Client())
    monkeypatch.setattr(DockerDriver, "_ensure_image", lambda self, opts: None)

    async def _noop_wait(self: DockerDriver) -> None:
        return None

    async def _noop_policy(self: DockerDriver, policy: object) -> None:
        return None

    monkeypatch.setattr(DockerDriver, "_wait_until_running", _noop_wait)
    monkeypatch.setattr(DockerDriver, "set_network_policy", _noop_policy)

    driver = DockerDriver(
        image="loom-agent-sandbox:dev",
        workspace=PurePosixPath("/workspace"),
    )
    await driver.start(
        options=StartOptions(
            extra_hosts=(("host.docker.internal", "host-gateway"),),
        )
    )

    assert run_kwargs["image"] == "loom-agent-sandbox:dev"
    assert run_kwargs["extra_hosts"] == {
        "host.docker.internal": "host-gateway",
    }


async def test_docker_driver_passes_dns_and_tmpfs_to_container_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_kwargs: dict[str, Any] = {}

    class _Containers:
        def run(self, image: str, **kwargs: Any) -> object:
            run_kwargs["image"] = image
            run_kwargs.update(kwargs)
            return object()

    class _Client:
        containers = _Containers()

        def close(self) -> None:
            pass

    monkeypatch.setattr(docker_module.docker, "from_env", lambda: _Client())
    monkeypatch.setattr(DockerDriver, "_ensure_image", lambda self, opts: None)

    async def _noop_wait(self: DockerDriver) -> None:
        return None

    async def _noop_policy(self: DockerDriver, policy: object) -> None:
        return None

    monkeypatch.setattr(DockerDriver, "_wait_until_running", _noop_wait)
    monkeypatch.setattr(DockerDriver, "set_network_policy", _noop_policy)

    driver = DockerDriver(
        image="loom-agent-sandbox:dev",
        workspace=PurePosixPath("/workspace"),
    )
    await driver.start(
        options=StartOptions(
            dns=("192.0.2.1", "198.51.100.1"),
            tmpfs=("/root:size=100M,mode=755", "/run"),
        )
    )

    assert run_kwargs["image"] == "loom-agent-sandbox:dev"
    assert run_kwargs["dns"] == ["192.0.2.1", "198.51.100.1"]
    assert run_kwargs["tmpfs"] == {
        "/root": "size=100M,mode=755",
        "/run": "",
    }


async def test_docker_driver_passes_api_timeout_to_docker_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from_env_timeouts: list[float | None] = []

    class _Containers:
        def run(self, image: str, **kwargs: Any) -> object:
            return object()

    class _Client:
        containers = _Containers()

        def close(self) -> None:
            pass

    def _from_env(*, timeout: float | None = None) -> _Client:
        from_env_timeouts.append(timeout)
        return _Client()

    monkeypatch.setattr(docker_module.docker, "from_env", _from_env)
    monkeypatch.setattr(DockerDriver, "_ensure_image", lambda self, opts: None)

    async def _noop_wait(self: DockerDriver) -> None:
        return None

    async def _noop_policy(self: DockerDriver, policy: object) -> None:
        return None

    monkeypatch.setattr(DockerDriver, "_wait_until_running", _noop_wait)
    monkeypatch.setattr(DockerDriver, "set_network_policy", _noop_policy)

    driver = DockerDriver(
        image="loom-agent-sandbox:dev",
        workspace=PurePosixPath("/workspace"),
        docker_api_timeout_sec=900.0,
    )
    await driver.start()

    assert from_env_timeouts == [900.0]
