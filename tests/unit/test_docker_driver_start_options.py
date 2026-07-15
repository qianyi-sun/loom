"""Unit coverage for DockerDriver StartOptions plumbing."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

import pytest

import loom.driver.docker as docker_module
from loom.driver.base import StartOptions
from loom.driver.docker import DockerDriver


class _FakeContainer:
    def __init__(self) -> None:
        self.started = False

    def start(self) -> None:
        self.started = True


async def test_docker_driver_passes_extra_hosts_to_container_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_kwargs: dict[str, Any] = {}
    container = _FakeContainer()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
            create_kwargs["image"] = image
            create_kwargs.update(kwargs)
            return container

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

    assert create_kwargs["image"] == "loom-agent-sandbox:dev"
    assert create_kwargs["extra_hosts"] == {
        "host.docker.internal": "host-gateway",
    }
    assert "remove" not in create_kwargs
    assert container.started is True


async def test_docker_driver_passes_dns_and_tmpfs_to_container_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_kwargs: dict[str, Any] = {}
    container = _FakeContainer()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
            create_kwargs["image"] = image
            create_kwargs.update(kwargs)
            return container

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
            cpus=2,
            memory_mb=4096,
            storage_mb=10240,
        )
    )

    assert create_kwargs["image"] == "loom-agent-sandbox:dev"
    assert create_kwargs["dns"] == ["192.0.2.1", "198.51.100.1"]
    assert create_kwargs["tmpfs"] == {
        "/root": "size=100M,mode=755",
        "/run": "",
    }
    assert create_kwargs["nano_cpus"] == 2_000_000_000
    assert create_kwargs["mem_limit"] == "4096m"
    assert create_kwargs["storage_opt"] == {"size": "10240M"}
    assert "remove" not in create_kwargs
    assert container.started is True


async def test_docker_driver_passes_api_timeout_to_docker_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from_env_timeouts: list[float | None] = []
    container = _FakeContainer()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
            return container

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
    assert container.started is True


async def test_docker_driver_passes_labels_to_container_create(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_kwargs: dict[str, Any] = {}
    container = _FakeContainer()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
            create_kwargs["image"] = image
            create_kwargs.update(kwargs)
            return container

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
            labels=(
                ("loom.trial-container", "true"),
                ("loom.trial_id", "00000000-0000-0000-0000-000000000001"),
            ),
        )
    )

    assert create_kwargs["labels"] == {
        "loom.trial-container": "true",
        "loom.trial_id": "00000000-0000-0000-0000-000000000001",
    }
    assert container.started is True
