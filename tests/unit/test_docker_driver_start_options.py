"""Unit coverage for DockerDriver StartOptions plumbing."""

from __future__ import annotations

from pathlib import PurePosixPath
from typing import Any

import pytest

import loom.driver.docker as docker_module
from loom.driver.base import StartOptions
from loom.driver.docker import DockerDriver
from loom.errors import DriverError


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


def test_docker_driver_accepts_only_current_job_systemd_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = f"loom-job-123-{'a' * 40}.slice"
    monkeypatch.setenv("LOOM_WORKER_SLURM_JOB_ID", "123")
    monkeypatch.delenv("SLURM_JOB_ID", raising=False)

    assert docker_module._validated_cgroup_parent(parent) == parent


def test_docker_driver_does_not_trust_ambient_slurm_job_id_for_systemd_slice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = f"loom-job-123-{'a' * 40}.slice"
    monkeypatch.delenv("LOOM_WORKER_SLURM_JOB_ID", raising=False)
    monkeypatch.setenv("SLURM_JOB_ID", "123")

    with pytest.raises(DriverError, match="valid Slurm job ID"):
        docker_module._validated_cgroup_parent(parent)


@pytest.mark.parametrize(
    ("parent", "job_id"),
    [
        (f"loom-job-456-{'a' * 40}.slice", "123"),
        (f"loom-job-123-{'A' * 40}.slice", "123"),
        (f"/loom-job-123-{'a' * 40}.slice", "123"),
        ("loom-job-123.slice", "123"),
        ("loom-job-123-" + "a" * 40 + ".slice/child", "123"),
        (f"loom-job-123-{'a' * 40}.slice", ""),
    ],
)
def test_docker_driver_rejects_unbound_or_malformed_systemd_slice(
    monkeypatch: pytest.MonkeyPatch,
    parent: str,
    job_id: str,
) -> None:
    if job_id:
        monkeypatch.setenv("LOOM_WORKER_SLURM_JOB_ID", job_id)
    else:
        monkeypatch.delenv("LOOM_WORKER_SLURM_JOB_ID", raising=False)

    with pytest.raises(DriverError, match="cgroup parent"):
        docker_module._validated_cgroup_parent(parent)


@pytest.mark.parametrize("cgroup_parent", ["", "/", "relative/path", "/a/../b"])
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
