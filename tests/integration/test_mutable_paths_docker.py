"""Directory handoff against real, disposable containers; no model calls."""
from pathlib import Path, PurePosixPath

import pytest

from tests.integration.test_workspace_snapshot_docker import docker_drivers  # noqa: F401

pytestmark = pytest.mark.docker


async def test_multiple_absolute_roots_preserve_changes_deletions_and_attributes(
    docker_drivers, tmp_path: Path,  # noqa: F811
):
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    paths = (PurePosixPath("/data"), PurePosixPath("/home/task"))
    for driver in (agent, verifier):
        result = await driver.exec("mkdir -p /data /home/task; echo old > /data/deleted", user="root")
        assert result.return_code == 0, result.stderr
    result = await agent.exec(
        "rm /data/deleted; mkdir -m 0710 /data/empty; "
        "echo unique > /data/marker; chmod 0751 /data/marker; "
        "ln /data/marker /data/hard; ln -s marker /data/link; "
        "chown 1234:1235 /data/marker; echo kernel > /home/task/kernelspec",
        user="root",
    )
    assert result.return_code == 0, result.stderr
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec(
        "set -eu; test ! -e /data/deleted; test \"$(cat /data/marker)\" = unique; "
        "test \"$(cat /home/task/kernelspec)\" = kernel; "
        "test \"$(stat -c %a /data/empty)\" = 710; "
        "test \"$(stat -c %a /data/marker)\" = 751; "
        "test \"$(stat -c %u:%g /data/marker)\" = 1234:1235; "
        "test \"$(stat -c %i /data/marker)\" = \"$(stat -c %i /data/hard)\"; "
        "test \"$(readlink /data/link)\" = marker", user="root",
    )
    assert result.return_code == 0, result.stderr


@pytest.mark.parametrize("setup", [
    "mkdir -p /data; ln -s /tests/private /data/escape",
    "mkdir -p /other; ln -s /other /data",
    "mkdir -p /data; mkfifo /data/pipe",
    "true",
])
async def test_unrepresentable_or_missing_state_fails_explicitly(docker_drivers, tmp_path, setup):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths

    agent, _ = docker_drivers
    result = await agent.exec(setup, user="root")
    assert result.return_code == 0
    with pytest.raises(RuntimeError):
        await export_mutable_paths(agent, (PurePosixPath("/data"),), tmp_path,
                                   workdir=PurePosixPath("/workspace"))


async def test_import_refuses_symlink_destination_without_touching_private_inputs(docker_drivers, tmp_path):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    await agent.exec("mkdir /data; echo forged > /data/private", user="root")
    await verifier.exec("mkdir /tests; echo trusted > /tests/private; ln -s /tests /data", user="root")
    paths = (PurePosixPath("/data"),)
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    with pytest.raises(RuntimeError):
        await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec("cat /tests/private", user="root")
    assert result.stdout.strip() == b"trusted"


async def test_tampered_manifest_cannot_change_destination(docker_drivers, tmp_path):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    await agent.exec("mkdir /data; echo content > /data/marker", user="root")
    paths = (PurePosixPath("/data"),)
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    manifest = tmp_path / "manifest.json"
    manifest.write_text(manifest.read_text().replace("/data", "/tests"))
    with pytest.raises(RuntimeError, match="manifest"):
        await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
