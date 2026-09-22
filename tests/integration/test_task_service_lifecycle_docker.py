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


async def test_real_sigterm_unwinds_verifier_and_stops_retained_service(service_sandboxes, tmp_path):
    import os
    import signal
    import sys

    from loom.trial.workspace_snapshot import _export_workspace_archive
    from tests.unit.test_service_execution_terminus_plan import _inputs

    agent, verifier = service_sandboxes
    task, trial, _ = _inputs()
    raw = task.model_dump(mode="json")
    raw["environment"]["service_lifecycle"] = {"readiness": {"command": "true"}}
    raw["verifier"]["args"]["script_path"] = "verifier/run.sh"
    workspace = tmp_path / "workspace"
    (workspace / "verifier").mkdir(parents=True)
    (workspace / "verifier/run.sh").write_text("touch /tmp/verifier-entered\nsleep 300\n")
    (workspace / ".loom").mkdir()
    await _export_workspace_archive(agent, PurePosixPath("/app"), workspace / ".loom/workspace.tar")
    assert (await agent.exec("sleep 300 >/dev/null 2>&1 & echo $! > /tmp/retained.pid")).return_code == 0
    child = """
import asyncio, json, os
from pathlib import Path
from loom import service_execution_sandbox_task as module
from loom.driver.service_sandbox import ServiceSandboxDriver
from loom.models.capabilities import Capabilities
from loom.models.networking import NoNetwork
from loom.models.task import TaskConfig
from loom.models.trial import TrialConfig
root = Path(os.environ['LOOM_TEST_ROOT'])
def connect(role, task):
    name = 'agent' if role == 'task-sandbox' else 'verifier'
    return ServiceSandboxDriver(root / name / 'sandbox.sock',
        capabilities=Capabilities(os='linux', gpu_vendor='none', network_policies=frozenset({'no-network'}),
                                  dynamic_network_policy=False, mounted_fs=False, resource_modes=frozenset({'limit'})),
        network_policy=NoNetwork())
module.sandbox_driver = connect
asyncio.run(module.run_verifier(root / 'workspace', TaskConfig.model_validate_json(os.environ['LOOM_TEST_TASK']),
                               TrialConfig.model_validate_json(os.environ['LOOM_TEST_TRIAL'])))
"""
    import json
    import time

    process = await asyncio.create_subprocess_exec(sys.executable, "-c", child, env={
        **os.environ, "LOOM_TEST_ROOT": str(tmp_path), "LOOM_TEST_TASK": json.dumps(raw),
        "LOOM_TEST_TRIAL": trial.model_dump_json(),
        "LOOM_EXECUTION_PHASE_DEADLINE": str(time.time() + 60),
        "LOOM_EXECUTION_TERMINATION_GRACE_SECONDS": "5",
    }, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    try:
        for _ in range(100):
            if (await verifier.exec("test -e /tmp/verifier-entered")).return_code == 0:
                break
            await asyncio.sleep(0.02)
        else:
            if process.returncode is not None:
                stdout, stderr = await process.communicate()
                raise AssertionError(f"verifier did not enter: {stdout!r} {stderr!r}")
            raise AssertionError("verifier did not enter")
        process.send_signal(signal.SIGTERM)
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=10)
        assert process.returncode != 0, (stdout, stderr)
        checked = await agent.exec("test ! -e /proc/$(cat /tmp/retained.pid)")
        assert checked.return_code == 0, "retained service survived verifier SIGTERM"
    finally:
        if process.returncode is None:
            process.kill()
            await process.communicate()
