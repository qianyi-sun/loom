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
from loom.trial.workspace_snapshot import WorkspaceSnapshotError, handoff_workspace_snapshot

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


async def test_handoff_preserves_absolute_link_targets_and_resolution(
    docker_drivers: tuple[Driver, Driver],
) -> None:
    agent, verifier = docker_drivers
    created = await agent.exec(
        "mkdir -p /workspace/data/sub /workspace/tests && "
        "printf answer > /workspace/data/file && "
        "printf private > /workspace/tests/secret && "
        "ln -s /workspace/data /workspace/absolute-dir && "
        "ln -s /workspace/data/file /workspace/absolute-file && "
        "ln -s /workspace/data/sub /workspace/nested && "
        "ln -s nested/../file /workspace/chain && "
        "ln -s /workspace/data/future /workspace/dangling",
        user="root",
    )
    assert created.return_code == 0, created.stderr
    assert (await verifier.exec(
        "mkdir -p /workspace/tests && printf trusted > /workspace/tests/secret",
        user="root",
    )).return_code == 0

    await handoff_workspace_snapshot(
        agent_driver=agent, verifier_driver=verifier,
        workdir=PurePosixPath("/workspace"),
        policy=WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY),
    )

    checked = await verifier.exec(
        "set -eu; "
        'test "$(readlink /workspace/absolute-dir)" = /workspace/data; '
        'test "$(readlink /workspace/absolute-file)" = /workspace/data/file; '
        'test "$(cat /workspace/absolute-dir/file)" = answer; '
        'test "$(cat /workspace/absolute-file)" = answer; '
        'test "$(readlink /workspace/chain)" = nested/../file; '
        'test "$(cat /workspace/chain)" = answer; '
        'test "$(readlink /workspace/dangling)" = /workspace/data/future; '
        'test "$(cat /workspace/tests/secret)" = trusted',
        user="root",
    )
    assert checked.return_code == 0, checked.stderr


async def test_handoff_replaces_public_state_and_preserves_nested_private_files(
    docker_drivers: tuple[Driver, Driver],
) -> None:
    agent, verifier = docker_drivers
    for driver in (agent, verifier):
        created = await driver.exec(
            "mkdir -p /workspace/data /workspace/obsolete /workspace/shared && "
            "printf keep > /workspace/data/keep && "
            "printf stale > /workspace/obsolete/file && "
            "printf stale > '/workspace/deleted file' && "
            "ln -s data /workspace/foo",
            user="root",
        )
        assert created.return_code == 0, created.stderr
    changed = await agent.exec(
        "rm -rf /workspace/obsolete '/workspace/deleted file' /workspace/foo && "
        "mkdir /workspace/redirect && printf answer > /workspace/redirect/result",
        user="root",
    )
    assert changed.return_code == 0, changed.stderr
    staged = await verifier.exec(
        "mkdir -p /outside && printf untouched > /outside/result && "
        "ln -s /outside /workspace/redirect && "
        "printf trusted > /workspace/shared/private.txt && "
        "printf stale > /workspace/shared/public.txt",
        user="root",
    )
    assert staged.return_code == 0, staged.stderr
    policy = WorkspaceStagingPolicy(("shared/private.txt",), ("shared/private.txt",), ())

    await handoff_workspace_snapshot(
        agent_driver=agent, verifier_driver=verifier,
        workdir=PurePosixPath("/workspace"), policy=policy,
    )

    checked = await verifier.exec(
        "set -eu; test ! -e /workspace/foo; test ! -L /workspace/foo; "
        "test ! -e /workspace/obsolete; test ! -e '/workspace/deleted file'; "
        "test ! -e /workspace/shared/public.txt; "
        'test "$(cat /workspace/data/keep)" = keep; '
        'test "$(cat /workspace/shared/private.txt)" = trusted; '
        'test "$(cat /workspace/redirect/result)" = answer; '
        'test "$(cat /outside/result)" = untouched; '
        "test ! -L /workspace/redirect",
        user="root",
    )
    assert checked.return_code == 0, checked.stderr


@pytest.mark.parametrize("replacement", ["symlink", "file"])
async def test_handoff_rejects_replacing_private_ancestor_before_cleanup(
    docker_drivers: tuple[Driver, Driver], replacement: str,
) -> None:
    agent, verifier = docker_drivers
    command = ("ln -s target /workspace/shared" if replacement == "symlink"
               else "printf public > /workspace/shared")
    assert (await agent.exec(command, user="root")).return_code == 0
    assert (await verifier.exec(
        "mkdir /workspace/shared && printf trusted > /workspace/shared/private.txt && "
        "printf baseline > /workspace/baseline", user="root",
    )).return_code == 0
    policy = WorkspaceStagingPolicy(("shared/private.txt",), ("shared/private.txt",), ())
    with pytest.raises(WorkspaceSnapshotError, match="private"):
        await handoff_workspace_snapshot(
            agent_driver=agent, verifier_driver=verifier,
            workdir=PurePosixPath("/workspace"), policy=policy,
        )
    checked = await verifier.exec(
        'test "$(cat /workspace/shared/private.txt)" = trusted && '
        'test "$(cat /workspace/baseline)" = baseline', user="root",
    )
    assert checked.return_code == 0, checked.stderr


async def test_handoff_rejects_symlink_destination_before_cleanup(
    docker_drivers: tuple[Driver, Driver],
) -> None:
    agent, verifier = docker_drivers
    assert (await agent.exec("mkdir /workspace/nested", user="root")).return_code == 0
    assert (await verifier.exec(
        "mkdir /outside && printf safe > /outside/keep && "
        "ln -s /outside /workspace/nested", user="root",
    )).return_code == 0
    with pytest.raises(WorkspaceSnapshotError, match="destination"):
        await handoff_workspace_snapshot(
            agent_driver=agent, verifier_driver=verifier,
            workdir=PurePosixPath("/workspace/nested"),
            policy=WorkspaceStagingPolicy.from_provenance(TB21_AGENT_WORKSPACE_POLICY),
        )
    assert (await verifier.exec(
        'test "$(cat /outside/keep)" = safe', user="root",
    )).return_code == 0


async def test_acl_requirement_rejects_busybox_without_leaving_probe_files(
    docker_drivers: tuple[Driver, Driver],
) -> None:
    from loom.trial.workspace_acls import require_acl_support

    agent, _ = docker_drivers
    with pytest.raises(WorkspaceSnapshotError, match="POSIX ACL"):
        await require_acl_support(agent, PurePosixPath("/workspace"))
    result = await agent.exec("find /workspace -name '.loom-acl-probe.*'", user="root")
    assert result.return_code == 0
    assert result.stdout == b""
