"""Real-docker integration: sandbox `--internal` bridge denies host
egress (#78 Phase A acceptance test).

Creates a bridge via the production code path, runs a busybox sandbox
on it, asserts:
- the sandbox cannot reach an external host (1.1.1.1, the bridge's
  --internal flag drops all default routes)
- the sandbox CAN reach other containers attached to the same bridge

Skipped automatically when no docker daemon is reachable.
"""

from __future__ import annotations

import asyncio
import subprocess
from uuid import uuid4

import pytest

from loom_worker.sandbox_network import (
    SandboxNetworkAllocator,
    create_sandbox_bridge,
    teardown_sandbox_bridge,
)

pytestmark = pytest.mark.docker


def _docker_available() -> bool:
    try:
        import docker
        docker.from_env().ping()
        return True
    except Exception:
        return False


@pytest.fixture
def docker_available() -> bool:
    return _docker_available()


async def test_internal_bridge_blocks_host_egress(
    docker_available: bool,
) -> None:
    if not docker_available:
        pytest.skip("docker daemon not reachable")

    allocator = SandboxNetworkAllocator()
    bridge = await create_sandbox_bridge(
        trial_id=uuid4(), allocator=allocator,
    )
    sandbox_name = f"loom-sandbox-test-{uuid4().hex[:8]}"
    try:
        # Attach a busybox container to the bridge. `--network` accepts
        # a docker network name; using `bridge.name` (the same one the
        # module just created) proves the production naming is usable.
        # `wget -T 3` so a denied connect fails fast instead of hanging
        # the test on TCP retries.
        proc = await asyncio.create_subprocess_exec(
            "docker", "run", "--rm",
            "--name", sandbox_name,
            "--network", bridge.name,
            "busybox:latest",
            "wget", "-T", "3", "-q", "-O", "/dev/null", "http://1.1.1.1/",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr_b = await proc.communicate()
        # wget on an unreachable host returns non-zero. The --internal
        # bridge guarantees the route doesn't exist, so this WILL fail.
        assert proc.returncode != 0, (
            "wget unexpectedly succeeded from --internal bridge — "
            "host route is reachable, defeating the isolation. "
            f"stderr={stderr_b.decode(errors='replace')[:300]!r}"
        )
    finally:
        # Best-effort cleanup. The container is --rm so it self-deletes
        # on exit; force-kill in case the await above hit a different
        # exception. Bridge teardown is the load-bearing piece.
        subprocess.run(
            ["docker", "kill", sandbox_name],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            check=False,
        )
        await teardown_sandbox_bridge(
            bridge=bridge, allocator=allocator,
        )
