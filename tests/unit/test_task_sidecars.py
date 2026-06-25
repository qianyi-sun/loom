from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import pytest
from docker.errors import ImageNotFound

from loom.models.healthcheck import HealthcheckSpec
from loom.models.task import (
    AgentDefaults,
    EnvironmentConfig,
    TaskConfig,
    TaskMetadata,
    TaskSidecarConfig,
    VerifierDefaults,
)
from loom_worker import task_sidecars
from loom_worker.task_sidecars import DockerTaskSidecarRuntime


def _task_config() -> TaskConfig:
    return TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="tb2/api-task", name="API task"),
        environment=EnvironmentConfig(
            os="linux",
            dockerfile=PurePosixPath(".loom-build/client/Dockerfile"),
            docker_build_context=PurePosixPath(".loom-build/client"),
            sidecars=[
                TaskSidecarConfig(name="db", docker_image="postgres:15"),
                TaskSidecarConfig(
                    name="api",
                    dockerfile=PurePosixPath(
                        ".loom-build/sidecars/api/Dockerfile",
                    ),
                    docker_build_context=PurePosixPath(
                        ".loom-build/sidecars/api",
                    ),
                    command=["python", "app.py"],
                    environment={"DEBUG": "1"},
                    depends_on=["db"],
                    healthcheck=HealthcheckSpec(
                        command="python -c 'print(1)'",
                        interval_sec=5,
                        timeout_sec=5,
                        retries=5,
                    ),
                ),
            ],
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="script"),
    )


class _FakeImages:
    def __init__(self) -> None:
        self.get_calls: list[str] = []
        self.pull_calls: list[str] = []
        self.build_calls: list[dict[str, Any]] = []

    def get(self, image: str) -> object:
        self.get_calls.append(image)
        if image == "postgres:15":
            raise ImageNotFound("missing")
        if image.startswith("loom-sidecar:"):
            raise ImageNotFound("missing")
        return object()

    def pull(self, image: str) -> object:
        self.pull_calls.append(image)
        return object()

    def build(self, **kwargs: Any) -> tuple[object, list[object]]:
        self.build_calls.append(kwargs)
        return object(), []


class _FakeContainer:
    def __init__(self, name: str, health_statuses: list[str] | None = None) -> None:
        self.name = name
        self.removed = False
        self.reload_calls = 0
        self._health_statuses = health_statuses or []
        self.attrs: dict[str, Any] = {"State": {}}
        if self._health_statuses:
            self.attrs["State"]["Health"] = {"Status": self._health_statuses[0]}

    def reload(self) -> None:
        self.reload_calls += 1
        if self._health_statuses:
            index = min(self.reload_calls, len(self._health_statuses) - 1)
            self.attrs["State"]["Health"] = {
                "Status": self._health_statuses[index],
            }

    def remove(self, *, force: bool) -> None:
        assert force is True
        self.removed = True


class _FakeContainers:
    def __init__(self) -> None:
        self.run_calls: list[dict[str, Any]] = []
        self.created: list[_FakeContainer] = []

    def run(self, image: str, **kwargs: Any) -> _FakeContainer:
        call = {"image": image, **kwargs}
        self.run_calls.append(call)
        container = _FakeContainer(
            str(kwargs["name"]),
            health_statuses=(
                ["starting", "healthy"] if "healthcheck" in kwargs else None
            ),
        )
        self.created.append(container)
        return container


class _FakeNetwork:
    def __init__(self, name: str) -> None:
        self.name = name
        self.removed = False

    def remove(self) -> None:
        self.removed = True


class _FakeNetworks:
    def __init__(self) -> None:
        self.create_calls: list[dict[str, Any]] = []
        self.created: list[_FakeNetwork] = []

    def create(self, name: str, **kwargs: Any) -> _FakeNetwork:
        self.create_calls.append({"name": name, **kwargs})
        network = _FakeNetwork(name)
        self.created.append(network)
        return network


class _FakeDockerClient:
    def __init__(self) -> None:
        self.images = _FakeImages()
        self.containers = _FakeContainers()
        self.networks = _FakeNetworks()
        self.api = self
        self.closed = False

    def create_endpoint_config(self, **kwargs: Any) -> dict[str, Any]:
        return {"EndpointConfig": kwargs}

    def close(self) -> None:
        self.closed = True


async def test_sidecar_runtime_builds_and_starts_in_dependency_order(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    task_dir = tmp_path / "task"
    api_context = task_dir / ".loom-build" / "sidecars" / "api"
    api_context.mkdir(parents=True)
    (api_context / "Dockerfile").write_text("FROM python:3.11-slim\n")
    fake_client = _FakeDockerClient()
    monkeypatch.setattr(
        task_sidecars.docker,
        "from_env",
        lambda: fake_client,
    )
    trial_id = uuid4()
    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=task_dir,
        task_checksum="abc123",
        trial_id=trial_id,
        health_poll_interval_sec=0,
    )

    network = await runtime.start(network_name="loom-task-net")
    await runtime.stop()

    assert network == "loom-task-net"
    assert fake_client.images.pull_calls == ["postgres:15"]
    assert fake_client.images.build_calls[0]["path"] == str(api_context)
    assert fake_client.images.build_calls[0]["dockerfile"] == "Dockerfile"
    assert [c["name"] for c in fake_client.containers.run_calls] == [
        f"loom-sidecar-{trial_id}-db",
        f"loom-sidecar-{trial_id}-api",
    ]
    api_call = fake_client.containers.run_calls[1]
    assert api_call["network"] == "loom-task-net"
    assert "network_aliases" not in api_call
    assert api_call["networking_config"] == {
        "loom-task-net": {"EndpointConfig": {"aliases": ["api"]}},
    }
    assert api_call["command"] == ["python", "app.py"]
    assert api_call["environment"] == {"DEBUG": "1"}
    assert api_call["healthcheck"] == {
        "test": ["CMD-SHELL", "python -c 'print(1)'"],
        "interval": 5_000_000_000,
        "timeout": 5_000_000_000,
        "retries": 5,
        "start_period": 0,
    }
    assert fake_client.containers.created[1].reload_calls >= 1
    assert [container.removed for container in fake_client.containers.created] == [
        True,
        True,
    ]
    assert fake_client.closed is True


async def test_sidecar_runtime_creates_and_removes_network_when_needed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    api_context = tmp_path / ".loom-build" / "sidecars" / "api"
    api_context.mkdir(parents=True)
    (api_context / "Dockerfile").write_text("FROM python:3.11-slim\n")
    fake_client = _FakeDockerClient()
    monkeypatch.setattr(
        task_sidecars.docker,
        "from_env",
        lambda: fake_client,
    )
    trial_id = uuid4()
    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=tmp_path,
        task_checksum="abc123",
        trial_id=trial_id,
        health_poll_interval_sec=0,
    )

    network = await runtime.start()
    await runtime.stop()

    assert network == f"loom-sidecars-{trial_id}"
    assert fake_client.networks.create_calls == [
        {"name": f"loom-sidecars-{trial_id}", "driver": "bridge"},
    ]
    assert fake_client.networks.created[0].removed is True


async def test_sidecar_runtime_passes_docker_api_timeout(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    api_context = tmp_path / ".loom-build" / "sidecars" / "api"
    api_context.mkdir(parents=True)
    (api_context / "Dockerfile").write_text("FROM python:3.11-slim\n")
    fake_client = _FakeDockerClient()
    from_env_timeouts: list[float | None] = []

    def _from_env(*, timeout: float | None = None) -> _FakeDockerClient:
        from_env_timeouts.append(timeout)
        return fake_client

    monkeypatch.setattr(task_sidecars.docker, "from_env", _from_env)
    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=tmp_path,
        task_checksum="abc123",
        trial_id=uuid4(),
        health_poll_interval_sec=0,
        docker_api_timeout_sec=900.0,
    )

    await runtime.start(network_name="loom-task-net")
    await runtime.stop()

    assert from_env_timeouts == [900.0]
