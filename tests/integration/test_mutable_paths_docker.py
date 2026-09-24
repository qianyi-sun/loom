"""Directory handoff against real, disposable containers; no model calls."""
import json
import tarfile
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


async def _reference_fixture(drivers, external="/usr/local/bin/interpreter"):
    for driver in drivers:
        result = await driver.exec(
            "mkdir -p /workspace /usr/local/bin /cache; cp /usr/local/bin/python3.11 /usr/local/bin/interpreter; "
            "echo baseline > /cache/baseline", user="root")
        assert result.return_code == 0, result.stderr
    result = await drivers[0].exec(
        f"mkdir -p /cache/venv/bin; ln -s {external} /cache/venv/bin/python; "
        "ln -s python /cache/venv/bin/python3; ln -s python /cache/venv/bin/python3.9", user="root")
    assert result.return_code == 0, result.stderr


async def test_native_handoff_restores_the_shell_runtime_libraries(sandboxes, tmp_path):  # noqa: F811
    """Directory replacement cannot need a shell after removing its loader."""
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier, other_trial = sandboxes
    discovered = await agent.exec(
        "python -c 'import glob,os; "
        "p=glob.glob(\"/lib/*-linux-gnu/ld-linux-*.so.*\"); "
        "assert len(p)==1,p; print(os.path.dirname(os.path.realpath(p[0])))'",
    )
    assert discovered.return_code == 0, discovered.stderr
    root = PurePosixPath(discovered.stdout.decode().strip())
    assert root.parent == PurePosixPath('/usr/lib')
    for driver in (agent, verifier):
        result = await driver.exec(f'mkdir -p /app; echo baseline > {root}/loom-deleted-marker')
        assert result.return_code == 0, result.stderr
    changed = await agent.exec(
        f'rm {root}/loom-deleted-marker; echo transferred > {root}/loom-library-marker',
    )
    assert changed.return_code == 0, changed.stderr
    await export_mutable_paths(agent, (root,), tmp_path / 'libraries', workdir=PurePosixPath('/app'))
    await import_mutable_paths(verifier, (root,), tmp_path / 'libraries', workdir=PurePosixPath('/app'))
    checked = await verifier.exec(
        f'set -eu; test ! -e {root}/loom-deleted-marker; '
        f'test "$(cat {root}/loom-library-marker)" = transferred; '
        "python -c 'import ssl; print(ssl.OPENSSL_VERSION)'",
    )
    assert checked.return_code == 0 and b'OpenSSL' in checked.stdout, checked.stderr
    untouched = await other_trial.exec(f'set -eu; test ! -e {root}/loom-library-marker; /bin/sh -c true')
    assert untouched.return_code == 0, untouched.stderr


@pytest.mark.parametrize("external", ["/usr/local/bin/interpreter", "../../../usr/local/bin/interpreter"])
async def test_declared_external_interpreter_is_checked_and_preserved(reference_drivers, tmp_path, external):
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    await _reference_fixture(reference_drivers, external)
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
        f"set -eu; test \"$(readlink /cache/venv/bin/python)\" = {external}; "
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
])
async def test_unrepresentable_state_fails_explicitly(docker_drivers, tmp_path, setup):  # noqa: F811
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


async def test_declared_workdir_and_external_state_reach_only_the_private_verifier(
    sandboxes, tmp_path,  # noqa: F811
):
    from loom.mutable_paths import validate_task_workdir
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths
    from loom.trial.workspace import WorkspaceStagingPolicy, materialize_workspace
    from loom.trial.workspace_snapshot import handoff_workspace_snapshot

    agent, verifier, other_trial = sandboxes
    workdir = PurePosixPath(validate_task_workdir("/media/project"))
    paths = (PurePosixPath("/opt/project-state"),)
    policy = WorkspaceStagingPolicy(("tests/**", "verifier/**"), ("tests/**", "verifier/**"), ())
    source = tmp_path / "task-input"
    (source / "tests").mkdir(parents=True)
    (source / "instruction.md").write_text("Update the project and its external state.\n")
    (source / "tests/secret").write_text("private-original-check\n")
    for driver in (agent, verifier):
        made = await driver.exec(
            "mkdir -p /media/project /opt/project-state; "
            "printf stale > /media/project/deleted; printf old > /opt/project-state/head",
        )
        assert made.return_code == 0, made.stderr
    await materialize_workspace(driver=agent, task_dir=source, dst=workdir, policy=policy)
    assert (await agent.exec("test ! -e tests/secret", cwd=workdir)).return_code == 0
    changed = await agent.exec(
        "set -eu; test \"$PWD\" = /media/project; rm deleted; "
        "printf changed > answer; printf new > /opt/project-state/head",
        cwd=workdir,
    )
    assert changed.return_code == 0, changed.stderr
    await materialize_workspace(
        driver=verifier, task_dir=source, dst=workdir, policy=policy, phase="verifier",
    )
    await handoff_workspace_snapshot(
        agent_driver=agent, verifier_driver=verifier, workdir=workdir, policy=policy,
    )
    await export_mutable_paths(agent, paths, tmp_path / "state", workdir=workdir)
    await import_mutable_paths(verifier, paths, tmp_path / "state", workdir=workdir)
    checked = await verifier.exec(
        "set -eu; test \"$PWD\" = /media/project; test ! -e deleted; "
        "test \"$(cat answer)\" = changed; test \"$(cat /opt/project-state/head)\" = new; "
        "test \"$(cat tests/secret)\" = private-original-check",
        cwd=workdir,
    )
    assert checked.return_code == 0, checked.stderr
    untouched = await other_trial.exec("test ! -e /media/project; test ! -e /opt/project-state")
    assert untouched.return_code == 0, untouched.stderr
    assert (await agent.exec("test ! -e tests/secret", cwd=workdir)).return_code == 0


async def test_declared_workdir_symlink_cannot_redirect_private_handoff(sandboxes):  # noqa: F811
    from loom.trial.workspace import WorkspaceStagingPolicy
    from loom.trial.workspace_snapshot import WorkspaceSnapshotError, handoff_workspace_snapshot

    agent, verifier, _ = sandboxes
    assert (await agent.exec("mkdir -p /media/project; printf forged > /media/project/secret")).return_code == 0
    assert (await verifier.exec(
        "mkdir -p /media /tests; printf trusted > /tests/secret; ln -s /tests /media/project",
    )).return_code == 0
    with pytest.raises(WorkspaceSnapshotError):
        await handoff_workspace_snapshot(
            agent_driver=agent, verifier_driver=verifier, workdir=PurePosixPath("/media/project"),
            policy=WorkspaceStagingPolicy(("tests/**",), ("tests/**",), ()),
        )
    assert (await verifier.exec("cat /tests/secret")).stdout == b"trusted"
@pytest.mark.parametrize("baseline", [False, True])
async def test_absent_mutable_root_stays_absent_in_fresh_verifier(docker_drivers, tmp_path, baseline):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    paths = (PurePosixPath("/home/task/jupyter"), PurePosixPath("/data"))
    for driver in (agent, verifier):
        result = await driver.exec("mkdir -p /data; echo baseline > /data/value", user="root")
        assert result.return_code == 0
        if baseline:
            result = await driver.exec("mkdir -p /home/task/jupyter; echo old > /home/task/jupyter/kernel", user="root")
            assert result.return_code == 0
    assert (await agent.exec("rm -rf /home/task/jupyter; echo changed > /data/value", user="root")).return_code == 0
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec(
        "test ! -e /home/task/jupyter && test ! -L /home/task/jupyter && cat /data/value", user="root")
    assert result.return_code == 0 and result.stdout.strip() == b"changed"
    if not baseline:
        assert (await verifier.exec("test ! -e /home/task", user="root")).return_code == 0


@pytest.mark.parametrize("setup", [
    "ln -s /missing /data",
    "mkdir /other; ln -s /other /data",
    "touch /data",
    "mkdir /data; ln -s /missing /data/new",
])
async def test_missing_root_with_invalid_ancestor_is_rejected(docker_drivers, tmp_path, setup):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths

    agent, _ = docker_drivers
    assert (await agent.exec(setup, user="root")).return_code == 0
    with pytest.raises(RuntimeError, match="directory"):
        await export_mutable_paths(agent, (PurePosixPath("/data/new"),), tmp_path,
                                   workdir=PurePosixPath("/workspace"))
    assert not (tmp_path / "manifest.json").exists()


async def test_new_export_replaces_stale_archive_when_root_is_deleted(docker_drivers, tmp_path):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    paths = (PurePosixPath("/data"),)
    assert (await agent.exec("mkdir /data; echo old > /data/value", user="root")).return_code == 0
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    assert (tmp_path / "0.tar").is_file()
    assert (await agent.exec("rm -rf /data", user="root")).return_code == 0
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    with tarfile.open(tmp_path / "0.tar") as stream:
        assert stream.getmembers() == []
    await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    assert (await verifier.exec("test ! -e /data", user="root")).return_code == 0


@pytest.mark.parametrize("corruption", ["path", "state", "archive", "schema", "extra-field"])
async def test_absence_manifest_is_validated_before_any_destination_changes(docker_drivers, tmp_path, corruption):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    paths = (PurePosixPath("/data"), PurePosixPath("/home/task"))
    assert (await agent.exec("mkdir /data; echo changed > /data/value", user="root")).return_code == 0
    assert (await verifier.exec("mkdir -p /data /home/task; echo baseline > /data/value", user="root")).return_code == 0
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    manifest_path = tmp_path / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if corruption == "path":
        manifest["paths"][1]["path"] = "/tests"
    elif corruption == "state":
        manifest["paths"][1]["state"] = "present"
    elif corruption == "schema":
        manifest["schema_version"] = 1
    elif corruption == "extra-field":
        manifest["paths"][1]["unexpected"] = "value"
    else:
        (tmp_path / "1.tar").write_bytes((tmp_path / "0.tar").read_bytes())
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(RuntimeError):
        await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec("test -d /home/task && cat /data/value", user="root")
    assert result.return_code == 0 and result.stdout.strip() == b"baseline"


async def test_absence_restore_rejects_symlink_before_deleting_any_root(docker_drivers, tmp_path):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    paths = (PurePosixPath("/data"), PurePosixPath("/home/task"))
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec(
        "mkdir -p /data /tests; echo trusted > /tests/private; ln -s /tests /home/task", user="root")
    assert result.return_code == 0
    with pytest.raises(RuntimeError, match="directory"):
        await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec("test -d /data && cat /tests/private", user="root")
    assert result.return_code == 0 and result.stdout.strip() == b"trusted"


@pytest.mark.parametrize("mode", ["0555", "0000"])
async def test_absence_restore_can_remove_nonwritable_empty_leaf_as_task_user(docker_drivers, tmp_path, monkeypatch, mode):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    paths = (PurePosixPath("/data/state"),)
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec(
        f"mkdir -p /data/state; chown -R 1000:1000 /data; chmod {mode} /data/state", user="root")
    assert result.return_code == 0
    execute = verifier.exec

    async def as_task(command, **kwargs):
        return await execute(command, **{**kwargs, "user": "1000:1000"})

    monkeypatch.setattr(verifier, "exec", as_task)
    await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    assert (await execute("test ! -e /data/state", user="root")).return_code == 0


async def test_absence_restore_checks_parent_permissions_before_removing_roots(docker_drivers, tmp_path, monkeypatch):  # noqa: F811
    from loom.trial.mutable_snapshot import export_mutable_paths, import_mutable_paths

    agent, verifier = docker_drivers
    paths = (PurePosixPath("/home/task"), PurePosixPath("/data/state"))
    await export_mutable_paths(agent, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    result = await verifier.exec(
        "mkdir -p /home/task /data/state; chown -R 1000:1000 /home/task /data; chmod 0777 /home; chmod 0555 /data",
        user="root")
    assert result.return_code == 0
    execute = verifier.exec

    async def as_task(command, **kwargs):
        return await execute(command, **{**kwargs, "user": "1000:1000"})

    monkeypatch.setattr(verifier, "exec", as_task)
    with pytest.raises(RuntimeError):
        await import_mutable_paths(verifier, paths, tmp_path, workdir=PurePosixPath("/workspace"))
    assert (await execute("test -d /home/task && test -d /data/state", user="root")).return_code == 0
