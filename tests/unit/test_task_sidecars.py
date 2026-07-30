from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any
from uuid import uuid4

import pytest
from docker.errors import APIError, ImageNotFound

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
    def __init__(
        self,
        name: str,
        health_statuses: list[str] | None = None,
        *,
        start_raises: bool = False,
    ) -> None:
        self.name = name
        self.removed = False
        self.started = False
        self.start_raises = start_raises
        self.reload_calls = 0
        self._health_statuses = health_statuses or []
        self.attrs: dict[str, Any] = {"State": {}}
        if self._health_statuses:
            self.attrs["State"]["Health"] = {"Status": self._health_statuses[0]}

    def start(self) -> None:
        if self.start_raises:
            raise APIError("simulated sidecar start failure")
        self.started = True

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
        self.fail_next_start = False
        self.run_calls: list[dict[str, Any]] = []
        self.create_calls: list[dict[str, Any]] = []
        self.created: list[_FakeContainer] = []

    def create(self, image: str, **kwargs: Any) -> _FakeContainer:
        call = {"image": image, **kwargs}
        self.create_calls.append(call)
        container = _FakeContainer(
            str(kwargs["name"]),
            health_statuses=(["starting", "healthy"] if "healthcheck" in kwargs else None),
            start_raises=self.fail_next_start,
        )
        self.fail_next_start = False
        self.created.append(container)
        return container

    def run(self, image: str, **kwargs: Any) -> _FakeContainer:
        self.run_calls.append({"image": image, **kwargs})
        container = self.create(image, **kwargs)
        container.start()
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


class _TrackingSlot:
    def __init__(self) -> None:
        self.depth = 0
        self.entered = 0
        self.exited = 0

    def __call__(self):
        slot = self

        class _Ctx:
            async def __aenter__(self) -> None:
                slot.entered += 1
                slot.depth += 1

            async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore[no-untyped-def]
                slot.depth -= 1
                slot.exited += 1

        return _Ctx()


async def test_sidecar_runtime_builds_and_starts_in_dependency_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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
        runtime_identity_labels=(
            ("loom.sandbox", "dev-a"),
            ("loom.candidate_sha", "a" * 40),
            ("loom.slurm_job_id", "12345"),
            ("loom.compose_project", "loom-dev-a-12345"),
            (
                "loom.env_id",
                "denv-00000000000000000000000000000001",
            ),
            ("loom.resource_generation", "7"),
            ("loom.candidate_id", f"cand-{'b' * 40}"),
            ("loom.candidate_tree", "c" * 40),
            ("loom.registry_generation", "42"),
            ("loom.registry_payload_sha256", "d" * 64),
        ),
    )

    network = await runtime.start(network_name="loom-task-net")
    await runtime.stop()

    assert network == "loom-task-net"
    assert fake_client.images.pull_calls == ["postgres:15"]
    assert fake_client.images.build_calls[0]["path"] == str(api_context)
    assert fake_client.images.build_calls[0]["dockerfile"] == "Dockerfile"
    assert [c["name"] for c in fake_client.containers.create_calls] == [
        f"loom-sidecar-{trial_id}-db",
        f"loom-sidecar-{trial_id}-api",
    ]
    api_call = fake_client.containers.create_calls[1]
    assert api_call["network"] == "loom-task-net"
    assert api_call["labels"] == {
        "loom.sandbox": "dev-a",
        "loom.candidate_sha": "a" * 40,
        "loom.slurm_job_id": "12345",
        "loom.compose_project": "loom-dev-a-12345",
        "loom.env_id": "denv-00000000000000000000000000000001",
        "loom.resource_generation": "7",
        "loom.candidate_id": f"cand-{'b' * 40}",
        "loom.candidate_tree": "c" * 40,
        "loom.registry_generation": "42",
        "loom.registry_payload_sha256": "d" * 64,
        "loom.setup-container": "true",
        "loom.task-sidecar": "true",
        "loom.task_id": "tb2/api-task",
        "loom.task_sidecar": "api",
        "loom.trial_id": str(trial_id),
    }
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
    assert "remove" not in api_call
    assert [container.started for container in fake_client.containers.created] == [
        True,
        True,
    ]
    assert fake_client.containers.created[1].reload_calls >= 1
    assert [container.removed for container in fake_client.containers.created] == [
        True,
        True,
    ]
    assert fake_client.closed is True


async def test_sidecar_runtime_enters_setup_slot_for_pull_and_build(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """#275: sidecar image pull/build is trial setup work and must use
    the same daemon-wide admission boundary as task-image and layered
    trial-cache builds. Without this, sidecar-heavy benchmarks can
    still fan out Docker setup pressure at trial concurrency.
    """
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
    slot = _TrackingSlot()
    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=task_dir,
        task_checksum="abc123",
        trial_id=uuid4(),
        health_poll_interval_sec=0,
        setup_slot_provider=slot,
    )

    await runtime.start(network_name="loom-task-net")
    await runtime.stop()

    assert slot.entered == 2
    assert slot.exited == 2
    assert slot.depth == 0


async def test_sidecar_runtime_removes_container_when_start_fails_after_create(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    task_config = TaskConfig(
        schema_version="1",
        task=TaskMetadata(id="tb2/api-task", name="API task"),
        environment=EnvironmentConfig(
            os="linux",
            dockerfile=PurePosixPath(".loom-build/client/Dockerfile"),
            sidecars=[TaskSidecarConfig(name="db", docker_image="postgres:15")],
        ),
        agent=AgentDefaults(name="oracle"),
        verifier=VerifierDefaults(name="script"),
    )
    fake_client = _FakeDockerClient()
    fake_client.containers.fail_next_start = True
    monkeypatch.setattr(
        task_sidecars.docker,
        "from_env",
        lambda: fake_client,
    )
    runtime = DockerTaskSidecarRuntime(
        task_config=task_config,
        task_dir=tmp_path,
        task_checksum="abc123",
        trial_id=uuid4(),
        health_poll_interval_sec=0,
    )

    with pytest.raises(APIError, match="simulated sidecar start failure"):
        await runtime.start(network_name="loom-task-net")

    assert len(fake_client.containers.created) == 1
    assert fake_client.containers.created[0].removed is True
    assert fake_client.closed is True


async def test_sidecar_runtime_creates_and_removes_network_when_needed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
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


async def test_sidecar_runtime_applies_container_caps_when_set(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # #896: positive per-container caps map to nano_cpus / mem_limit /
    # pids_limit on every setup-sidecar container this runtime creates.
    api_context = tmp_path / ".loom-build" / "sidecars" / "api"
    api_context.mkdir(parents=True)
    (api_context / "Dockerfile").write_text("FROM python:3.11-slim\n")
    fake_client = _FakeDockerClient()
    monkeypatch.setattr(task_sidecars.docker, "from_env", lambda: fake_client)
    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=tmp_path,
        task_checksum="abc123",
        trial_id=uuid4(),
        health_poll_interval_sec=0,
        container_cpus=2.0,
        container_memory_mib=512,
        container_pids=256,
    )

    await runtime.start(network_name="loom-task-net")
    await runtime.stop()

    assert fake_client.containers.create_calls, "expected sidecar containers"
    for call in fake_client.containers.create_calls:
        assert call["nano_cpus"] == 2_000_000_000
        assert call["mem_limit"] == 512 * 1024 * 1024
        assert call["pids_limit"] == 256


async def test_sidecar_runtime_applies_exact_cgroup_parent(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    api_context = tmp_path / ".loom-build" / "sidecars" / "api"
    api_context.mkdir(parents=True)
    (api_context / "Dockerfile").write_text("FROM python:3.11-slim\n")
    fake_client = _FakeDockerClient()
    monkeypatch.setattr(task_sidecars.docker, "from_env", lambda: fake_client)
    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=tmp_path,
        task_checksum="abc123",
        trial_id=uuid4(),
        health_poll_interval_sec=0,
        container_cgroup_parent="/system.slice/slurmstepd.scope/job_123",
    )

    await runtime.start(network_name="loom-task-net")
    await runtime.stop()

    assert fake_client.containers.create_calls
    for call in fake_client.containers.create_calls:
        assert call["cgroup_parent"] == ("/system.slice/slurmstepd.scope/job_123")


async def test_sidecar_runtime_omits_container_caps_when_unset(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # #896: default (0) caps leave sidecar creation unbounded.
    api_context = tmp_path / ".loom-build" / "sidecars" / "api"
    api_context.mkdir(parents=True)
    (api_context / "Dockerfile").write_text("FROM python:3.11-slim\n")
    fake_client = _FakeDockerClient()
    monkeypatch.setattr(task_sidecars.docker, "from_env", lambda: fake_client)
    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=tmp_path,
        task_checksum="abc123",
        trial_id=uuid4(),
        health_poll_interval_sec=0,
    )

    await runtime.start(network_name="loom-task-net")
    await runtime.stop()

    assert fake_client.containers.create_calls, "expected sidecar containers"
    for call in fake_client.containers.create_calls:
        assert "nano_cpus" not in call
        assert "mem_limit" not in call
        assert "pids_limit" not in call


async def test_sidecar_health_wait_allows_probe_at_interval_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clock = {"now": 0.0}
    loop = task_sidecars.asyncio.get_running_loop()
    monkeypatch.setattr(loop, "time", lambda: clock["now"])

    async def _fake_sleep(seconds: float) -> None:
        clock["now"] += seconds

    monkeypatch.setattr(task_sidecars.asyncio, "sleep", _fake_sleep)

    class _BoundaryContainer:
        name = "loom-sidecar-trial-server"

        def __init__(self) -> None:
            self.reload_calls = 0
            self.attrs: dict[str, Any] = {
                "State": {"Health": {"Status": "starting"}},
            }

        def reload(self) -> None:
            self.reload_calls += 1
            status = "healthy" if clock["now"] >= 10.5 else "starting"
            self.attrs = {"State": {"Health": {"Status": status}}}

    runtime = DockerTaskSidecarRuntime(
        task_config=_task_config(),
        task_dir=tmp_path,
        task_checksum="abc123",
        trial_id=uuid4(),
        health_poll_interval_sec=0.5,
    )
    container = _BoundaryContainer()
    healthcheck = HealthcheckSpec(
        command="curl -f http://localhost:8000",
        interval_sec=10.0,
        timeout_sec=5.0,
        retries=1,
    )

    await runtime._wait_for_healthy(container, healthcheck)

    assert container.reload_calls > 1
