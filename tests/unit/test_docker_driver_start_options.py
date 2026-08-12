"""Unit coverage for DockerDriver StartOptions plumbing."""

from __future__ import annotations

from pathlib import Path, PurePosixPath
from typing import Any

import pytest

import loom.driver.docker as docker_module
from loom.driver.base import StartOptions
from loom.driver.docker import DockerDriver
from loom.errors import DriverError
from loom_worker.pipeline_container_runner import (
    PipelineContainerContractError,
    build_pipeline_container_spec,
)


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


async def test_docker_driver_applies_container_caps_when_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # #896: positive per-container caps map to nano_cpus / mem_limit /
    # pids_limit at container create for packed (non-exclusive) workers.
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
            container_cpus=2.0,
            container_memory_mib=512,
            container_pids=256,
        )
    )

    assert create_kwargs["nano_cpus"] == 2_000_000_000
    assert create_kwargs["mem_limit"] == 512 * 1024 * 1024
    assert create_kwargs["pids_limit"] == 256
    assert container.started is True


async def test_docker_driver_applies_exact_cgroup_parent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_kwargs: dict[str, Any] = {}
    container = _FakeContainer()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
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

    driver = DockerDriver(image="loom-agent-sandbox:dev")
    await driver.start(
        options=StartOptions(
            cgroup_parent="/system.slice/slurmstepd.scope/job_123",
        )
    )

    assert create_kwargs["cgroup_parent"] == ("/system.slice/slurmstepd.scope/job_123")


async def test_docker_driver_applies_guard_owned_systemd_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_kwargs: dict[str, Any] = {}
    container = _FakeContainer()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
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

    driver = DockerDriver(image="loom-agent-sandbox:dev")
    await driver.start(options=StartOptions(cgroup_parent="loom-job-123.slice"))

    assert create_kwargs["cgroup_parent"] == "loom-job-123.slice"


@pytest.mark.parametrize(
    "cgroup_parent",
    [
        "",
        "/",
        "relative/path",
        "/a/../b",
        "loom-job-0.slice",
        "loom-job-01.slice",
        "loom-job--1.slice",
        "loom-job-1.service",
        "other-job-1.slice",
    ],
)
async def test_docker_driver_rejects_unsafe_cgroup_parent(
    monkeypatch: pytest.MonkeyPatch,
    cgroup_parent: str,
) -> None:
    class _Images:
        def get(self, image: str) -> object:
            return object()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
            raise AssertionError("unsafe cgroup parent reached Docker")

    class _Client:
        images = _Images()
        containers = _Containers()

        def close(self) -> None:
            pass

    monkeypatch.setattr(docker_module.docker, "from_env", lambda: _Client())

    driver = DockerDriver(image="loom-agent-sandbox:dev")
    with pytest.raises(DriverError, match="cgroup parent"):
        await driver.start(options=StartOptions(cgroup_parent=cgroup_parent))


async def test_docker_driver_omits_container_caps_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # #896: default (0) caps leave the create call unbounded, so exclusive
    # GB10 pools are byte-for-byte unchanged.
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
    await driver.start(options=StartOptions())

    assert "nano_cpus" not in create_kwargs
    assert "mem_limit" not in create_kwargs
    assert "pids_limit" not in create_kwargs
    assert container.started is True


async def test_docker_driver_rejects_gpu_request_above_slurm_allocation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Client:
        def close(self) -> None:
            pass

    monkeypatch.setattr(docker_module.docker, "from_env", lambda: _Client())
    monkeypatch.setattr(DockerDriver, "_ensure_image", lambda self, opts: None)
    driver = DockerDriver(
        image="loom-agent-sandbox:dev",
        workspace=PurePosixPath("/workspace"),
    )

    with pytest.raises(DriverError, match="exceeds the Slurm allocation"):
        await driver.start(
            options=StartOptions(
                gpus=2,
                slurm_allocated_gpus=1,
                slurm_gpu_device_ids=("0",),
            ),
        )


async def test_docker_driver_binds_only_slurm_allocated_gpu_devices(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_kwargs: dict[str, Any] = {}
    container = _FakeContainer()

    class _Containers:
        def create(self, image: str, **kwargs: Any) -> object:
            create_kwargs.update(kwargs)
            return container

    class _Client:
        containers = _Containers()

        def close(self) -> None:
            pass

    monkeypatch.setattr(docker_module.docker, "from_env", lambda: _Client())
    monkeypatch.setattr(DockerDriver, "_ensure_image", lambda self, opts: None)
    monkeypatch.setattr(
        docker_module.docker.types,
        "DeviceRequest",
        lambda **kwargs: kwargs,
    )

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
            gpus=1,
            slurm_allocated_gpus=2,
            slurm_gpu_device_ids=("3", "7"),
        ),
    )

    assert create_kwargs["device_requests"] == [
        {
            "device_ids": ["3"],
            "capabilities": [["gpu"]],
        },
    ]


def test_pipeline_container_passes_only_sorted_allocation_gpu_uuids(
    tmp_path: Path,
) -> None:
    inputs = tmp_path / "inputs"
    outputs = tmp_path / "outputs"
    scratch = tmp_path / "scratch"
    for path in (inputs, outputs, scratch):
        path.mkdir()
    spec = build_pipeline_container_spec(
        image="registry.example.com/loom/sim@sha256:" + "a" * 64,
        argv=["/opt/loom/gpu-preflight"],
        workdir="/workspace",
        uid=1000,
        gid=1000,
        input_dir=inputs,
        outputs_dir=outputs,
        scratch_dir=scratch,
        network_profile="none",
        cpus=16,
        memory_bytes=64 << 30,
        pids=4096,
        scratch_bytes=150 << 30,
        gpu_device_uuids=["GPU-AAAA", "GPU-BBBB"],
    )
    assert spec.docker_create_kwargs()["device_requests"] == [
        {
            "device_ids": ["GPU-AAAA", "GPU-BBBB"],
            "capabilities": [["gpu"]],
        }
    ]
    with pytest.raises(PipelineContainerContractError, match="GPU UUID set"):
        build_pipeline_container_spec(
            image="registry.example.com/loom/sim@sha256:" + "a" * 64,
            argv=["/opt/loom/gpu-preflight"],
            workdir="/workspace",
            uid=1000,
            gid=1000,
            input_dir=inputs,
            outputs_dir=outputs,
            scratch_dir=scratch,
            network_profile="none",
            cpus=16,
            memory_bytes=64 << 30,
            pids=4096,
            scratch_bytes=150 << 30,
            gpu_device_uuids=["0"],
        )
