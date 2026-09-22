"""Native process retention and private verification across one trial network."""
from __future__ import annotations

import asyncio
from pathlib import PurePosixPath

import docker
import pytest

from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths
from tests.integration.test_task_identity_installation_docker import native_binary  # noqa: F401

pytestmark = [pytest.mark.docker, pytest.mark.timeout(180)]


@pytest.fixture
async def service_sandboxes(native_binary, tmp_path):  # noqa: F811
    client = docker.from_env()
    containers, drivers = [], []
    try:
        for role in ("agent", "verifier"):
            socket = tmp_path / role
            socket.mkdir(mode=0o777)
            socket.chmod(0o2777)
            container = client.containers.run(
                "python:3.11-slim", ["--socket", "/socket/sandbox.sock"], detach=True,
                entrypoint="/loom/bin/loom-sandbox-runtime", user="0:0",
                network_mode="none" if not containers else f"container:{containers[0].id}",
                cap_drop=["ALL"], security_opt=["no-new-privileges"],
                volumes={str(native_binary): {"bind": "/loom/bin/loom-sandbox-runtime", "mode": "ro"},
                         str(socket): {"bind": "/socket", "mode": "rw"}},
            )
            containers.append(container)
            driver = ServiceSandboxDriver(socket / "sandbox.sock",
                capabilities=Capabilities(os="linux", gpu_vendor="none", network_policies=frozenset({"no-network"}),
                                          dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({"limit"})),
                network_policy=NoNetwork())
            for attempt in range(100):
                try:
                    await driver.start()
                    break
                except (OSError, RuntimeError):
                    if attempt == 99:
                        raise
                    await asyncio.sleep(0.05)
            drivers.append(driver)
            assert (await driver.exec("mkdir -p /app /data")).return_code == 0
        yield drivers
    finally:
        for driver in drivers:
            await driver.stop()
        for container in reversed(containers):
            container.remove(force=True)
        client.close()


async def test_service_survives_consistent_snapshot_and_private_verifier(service_sandboxes, tmp_path):
    agent, verifier = service_sandboxes
    checked = await verifier.exec("mkdir -p /tests; echo private > /tests/secret")
    assert checked.return_code == 0
    started = await agent.exec(
        "test ! -e /tests/secret && echo response > /data/answer && "
        "(python -m http.server 8000 --bind 127.0.0.1 --directory /data >/tmp/service.log 2>&1 & "
        "echo $! > /tmp/service.pid) && "
        "(while true; do echo tick >> /data/heartbeat; sleep .02; done) >/dev/null 2>&1 &",
    )
    assert started.return_code == 0, started.stderr
    request = "python -c \"import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:8000/answer').read().decode().strip())\""
    for _attempt in range(50):
        ready = await verifier.exec(request)
        if ready.return_code == 0:
            break
        await asyncio.sleep(0.02)
    assert ready.stdout.strip() == b"response", ready.stderr
    await agent.pause_processes()
    before = await agent.exec("wc -c < /data/heartbeat")
    await asyncio.sleep(0.1)
    after = await agent.exec("wc -c < /data/heartbeat")
    assert before.stdout == after.stdout
    paths = (PurePosixPath("/data"),)
    await export_mutable_paths(agent, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await import_mutable_paths(verifier, paths, tmp_path / "snapshot", workdir=PurePosixPath("/app"))
    await agent.resume_processes()
    assert (await verifier.exec(request)).stdout.strip() == b"response"
    checked = await verifier.exec("test \"$(cat /tests/secret)\" = private && test \"$(cat /data/answer)\" = response")
    assert checked.return_code == 0, checked.stderr
    assert (await agent.exec("test ! -e /tests/secret")).return_code == 0
    await agent.stop_processes()
    assert (await verifier.exec(request)).return_code != 0
    assert (await agent.exec("test ! -e /proc/$(cat /tmp/service.pid)")).return_code == 0
