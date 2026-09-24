"""Directory handoff against real, disposable containers; no model calls."""
import json
from pathlib import Path, PurePosixPath

import pytest

from tests.integration.test_task_identity_installation_docker import (  # noqa: F401
    native_binary,
    sandboxes,
)
from tests.integration.test_workspace_snapshot_docker import docker_drivers  # noqa: F401

pytestmark = pytest.mark.docker


@pytest.fixture
async def reference_drivers(sandboxes):  # noqa: F811
    yield tuple(sandboxes[:2])


async def _reference_fixture(drivers):
    for driver in drivers:
        result = await driver.exec(
            "mkdir -p /workspace /usr/local/bin /cache; cp /usr/local/bin/python3.11 /usr/local/bin/interpreter; "
            "echo baseline > /cache/baseline", user="root")
        assert result.return_code == 0, result.stderr
    result = await drivers[0].exec(
        "mkdir -p /cache/venv/bin; ln -s /usr/local/bin/interpreter /cache/venv/bin/python; "
        "ln -s python /cache/venv/bin/python3; ln -s python /cache/venv/bin/python3.9", user="root")
    assert result.return_code == 0, result.stderr


async def test_declared_external_interpreter_is_checked_and_preserved(reference_drivers, tmp_path):
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    await _reference_fixture(reference_drivers)
    agent, verifier = reference_drivers
    options = {"workdir": PurePosixPath("/workspace"),
               "reference_files": (PurePosixPath("/usr/local/bin/interpreter"),)}
    paths = (PurePosixPath("/cache"),)
    await export_mutable_paths(agent, paths, tmp_path, **options)
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["schema_version"] == 2
    assert manifest["reference_files"][0]["path"] == "/usr/local/bin/interpreter"
    await import_mutable_paths(verifier, paths, tmp_path, **options)
    checked = await verifier.exec(
        "set -eu; test \"$(readlink /cache/venv/bin/python)\" = /usr/local/bin/interpreter; "
        "test \"$(readlink /cache/venv/bin/python3)\" = python; "
        "test \"$(readlink /cache/venv/bin/python3.9)\" = python; "
        "/cache/venv/bin/python3.9 -c 'print(\"reference-preserved\")'", user="root")
    assert checked.return_code == 0 and checked.stdout.strip() == b"reference-preserved"


@pytest.mark.parametrize("change", [
    "printf changed >> /usr/local/bin/interpreter",
    "chmod 0700 /usr/local/bin/interpreter",
    "rm /usr/local/bin/interpreter",
    "rm /usr/local/bin/interpreter; ln -s /bin/busybox /usr/local/bin/interpreter",
    "mv /usr/local/bin /usr/local/other; ln -s other /usr/local/bin",
])
async def test_changed_reference_rejects_before_verifier_state_is_cleared(reference_drivers, tmp_path, change):
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    await _reference_fixture(reference_drivers)
    agent, verifier = reference_drivers
    paths = (PurePosixPath("/cache"),)
    options = {"workdir": PurePosixPath("/workspace"),
               "reference_files": (PurePosixPath("/usr/local/bin/interpreter"),)}
    await export_mutable_paths(agent, paths, tmp_path, **options)
    changed = await verifier.exec(change, user="root")
    assert changed.return_code == 0, changed.stderr
    with pytest.raises(RuntimeError, match="reference"):
        await import_mutable_paths(verifier, paths, tmp_path, **options)
    assert (await verifier.exec("cat /cache/baseline", user="root")).stdout.strip() == b"baseline"


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
        "ln -s /data/marker /data/absolute-link; "
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
        "test \"$(readlink /data/link)\" = marker; "
        "test \"$(readlink /data/absolute-link)\" = /data/marker; "
        "test \"$(cat /data/absolute-link)\" = unique", user="root",
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


async def test_new_directory_can_be_restored_into_fresh_verifier(docker_drivers, tmp_path):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    await agent.exec("mkdir -p /data/new; echo created > /data/new/marker", user="root")
    paths = (PurePosixPath("/data/new"),)
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec("cat /data/new/marker", user="root")
    assert result.stdout.strip() == b"created"


async def test_cross_root_hardlinks_are_rejected_instead_of_silently_copied(docker_drivers, tmp_path):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths

    agent, _ = docker_drivers
    await agent.exec("mkdir -p /data /home/task; echo linked > /data/marker; ln /data/marker /home/task/link", user="root")
    with pytest.raises(RuntimeError, match=r"hardlinks.*declared roots"):
        await export_mutable_paths(agent, (PurePosixPath("/data"), PurePosixPath("/home/task")),
                                   tmp_path, workdir=PurePosixPath("/workspace"))
    assert not (tmp_path / "manifest.json").exists()
