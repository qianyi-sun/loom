"""Real-Docker regression for isolated verifier workspace handoff."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import PurePosixPath

import pytest

from loom.driver.base import Driver, StartOptions
from loom.trial.workspace import (
    TB21_AGENT_WORKSPACE_POLICY,
    WorkspaceStagingPolicy,
)
from loom.trial.workspace_snapshot import handoff_workspace_snapshot

pytestmark = pytest.mark.docker


@pytest.fixture
async def docker_drivers() -> AsyncGenerator[tuple[Driver, Driver], None]:
    pytest.importorskip("docker")
    import docker

    try:
        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not available")
    from loom.driver.docker import DockerDriver

    agent = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    verifier = DockerDriver(image="alpine:3.19", workspace=PurePosixPath("/workspace"))
    await agent.start(options=StartOptions())
    await verifier.start(options=StartOptions())
    try:
        yield agent, verifier
    finally:
        await agent.stop(delete=True)
        await verifier.stop(delete=True)


async def test_handoff_preserves_workspace_filesystem_semantics(
    docker_drivers: tuple[Driver, Driver],
) -> None:
    agent, verifier = docker_drivers
    created = await agent.exec(
        "mkdir -p /workspace/bin /workspace/links && "
        "mkdir -m 0710 /workspace/empty && "
        "printf '#!/bin/sh\\necho snapshot-ok\\n' > /workspace/bin/tool && "
        "chmod 0751 /workspace/bin /workspace/bin/tool && "
        "ln /workspace/bin/tool /workspace/bin/tool-hard && "
        "ln -s ../bin/tool /workspace/links/tool",
        user="root",
    )
    assert created.return_code == 0, created.stderr
    trusted = await verifier.exec(
        "mkdir -p /workspace/verifier && "
        "printf 'trusted\\n' > /workspace/verifier/run.sh",
        user="root",
    )
    assert trusted.return_code == 0, trusted.stderr

    await handoff_workspace_snapshot(
        agent_driver=agent,
        verifier_driver=verifier,
        workdir=PurePosixPath("/workspace"),
        policy=WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY),
    )

    checked = await verifier.exec(
        "set -eu; "
        "test -d /workspace/empty; "
        "test \"$(stat -c %a /workspace/empty)\" = 710; "
        "test \"$(stat -c %a /workspace/bin)\" = 751; "
        "test \"$(stat -c %a /workspace/bin/tool)\" = 751; "
        "test -x /workspace/bin/tool; "
        "test \"$(/workspace/bin/tool)\" = snapshot-ok; "
        "test \"$(stat -c %i /workspace/bin/tool)\" = "
        "\"$(stat -c %i /workspace/bin/tool-hard)\"; "
        "test -L /workspace/links/tool; "
        "test \"$(readlink /workspace/links/tool)\" = ../bin/tool; "
        "test \"$(cat /workspace/verifier/run.sh)\" = trusted",
        user="root",
    )
    assert checked.return_code == 0, checked.stderr
