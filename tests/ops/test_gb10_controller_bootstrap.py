from __future__ import annotations

import fcntl
import os
import subprocess
from dataclasses import replace
from pathlib import Path

import pytest
from scripts.ops import gb10_controller_bootstrap as bootstrap


def _git(repository: Path, *arguments: str) -> str:
    completed = subprocess.run(
        ["/usr/bin/git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def _source_repository(tmp_path: Path) -> tuple[Path, str]:
    source = tmp_path / "source"
    source.mkdir()
    _git(source, "init", "--quiet")
    _git(source, "config", "user.email", "bootstrap-test@example.invalid")
    _git(source, "config", "user.name", "Bootstrap Test")
    assets = {
        "deploy/slurm/install-loom-gb10-autoscaler-controller.sh": (
            "#!/usr/bin/env bash\nexit 0\n"
        ),
        "deploy/slurm/loom-gb10-slurm-authority.tmpfiles": (
            "d /run/loom-gb10-slurm-authority 0700 root root -\n"
        ),
        "scripts/ops/gb10_controller_bootstrap.py": "#!/usr/bin/env python3\n",
        "scripts/ops/gb10_external_supervisor_broker.py": "#!/usr/bin/env python3\n",
        "scripts/ops/gb10_slurm_acceptance_authority.py": "#!/usr/bin/env python3\n",
        "scripts/ops/install_gb10_autoscaler_controller.py": "#!/usr/bin/env python3\n",
    }
    for relative, payload in assets.items():
        path = source / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(payload, encoding="utf-8")
        path.chmod(0o755 if path.suffix in {".py", ".sh"} else 0o644)
    _git(source, "add", ".")
    _git(source, "commit", "--quiet", "-m", "bootstrap fixture")
    return source, _git(source, "rev-parse", "HEAD")


def _context(tmp_path: Path) -> bootstrap.BootstrapContext:
    source, source_sha = _source_repository(tmp_path)
    return bootstrap.BootstrapContext(
        source_sha=source_sha,
        trusted_root=tmp_path / "trusted",
        remote_url=str(source),
        authority_uid=os.getuid(),
        authority_gid=os.getgid(),
        allow_file_remote=True,
    )


def test_prepare_source_clones_and_seals_the_exact_commit(tmp_path: Path) -> None:
    context = _context(tmp_path)

    prepared = bootstrap.prepare_source(context)

    assert prepared.source_root == context.trusted_root / context.source_sha
    assert prepared.launcher_path == (
        prepared.source_root / "deploy/slurm/install-loom-gb10-autoscaler-controller.sh"
    )
    assert _git(prepared.source_root, "rev-parse", "HEAD") == context.source_sha
    assert _git(prepared.source_root, "branch", "--show-current") == ""
    assert _git(prepared.source_root, "status", "--porcelain=v1", "--untracked-files=all") == ""
    assert prepared.source_root.stat().st_mode & 0o777 == 0o700
    assert (prepared.source_root / ".git").stat().st_mode & 0o777 == 0o700
    assert set(prepared.artifact_sha256) == {
        "deploy/slurm/install-loom-gb10-autoscaler-controller.sh",
        "deploy/slurm/loom-gb10-slurm-authority.tmpfiles",
        "scripts/ops/gb10_controller_bootstrap.py",
        "scripts/ops/gb10_external_supervisor_broker.py",
        "scripts/ops/gb10_slurm_acceptance_authority.py",
        "scripts/ops/install_gb10_autoscaler_controller.py",
    }


def test_bootstrap_executable_does_not_resolve_python_from_path(tmp_path: Path) -> None:
    executable = tmp_path / "loom-gb10-controller-bootstrap"
    executable.write_bytes(Path(bootstrap.__file__).read_bytes())
    executable.chmod(0o755)
    marker = tmp_path / "hostile-python-executed"
    hostile_bin = tmp_path / "hostile-bin"
    hostile_bin.mkdir()
    hostile_python = hostile_bin / "python3"
    hostile_python.write_text(
        f"#!/bin/sh\n: > '{marker}'\nexit 0\n",
        encoding="utf-8",
    )
    hostile_python.chmod(0o755)

    completed = subprocess.run(
        [str(executable), "--help"],
        capture_output=True,
        check=False,
        env={**os.environ, "PATH": str(hostile_bin)},
        text=True,
    )

    assert completed.returncode == 0
    assert not marker.exists()
    assert "--source-sha" in completed.stdout


def test_prepare_source_ignores_hostile_tmpdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = _context(tmp_path)
    hostile_tmp = tmp_path / "hostile-tmp"
    hostile_tmp.mkdir()
    monkeypatch.setenv("TMPDIR", str(hostile_tmp))

    bootstrap.prepare_source(context)

    assert list(hostile_tmp.iterdir()) == []


def test_prepare_source_removes_new_trusted_root_when_parent_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    real_fsync = bootstrap._fsync_directory
    failed = False

    def fail_first_root_parent_fsync(path: Path) -> None:
        nonlocal failed
        if path == context.trusted_root.parent and context.trusted_root.exists() and not failed:
            failed = True
            raise OSError("injected trusted-root parent fsync failure")
        real_fsync(path)

    monkeypatch.setattr(bootstrap, "_fsync_directory", fail_first_root_parent_fsync)

    with pytest.raises(
        bootstrap.ControllerBootstrapError,
        match="trusted root creation failed",
    ):
        bootstrap.prepare_source(context)

    assert failed is True
    assert not context.trusted_root.exists()


def test_prepare_source_removes_new_install_lock_when_parent_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    install_lock = context.trusted_root / ".install.lock"
    real_fsync = bootstrap._fsync_directory
    failed = False

    def fail_first_lock_parent_fsync(path: Path) -> None:
        nonlocal failed
        if path == context.trusted_root and install_lock.exists() and not failed:
            failed = True
            raise OSError("injected install-lock parent fsync failure")
        real_fsync(path)

    monkeypatch.setattr(bootstrap, "_fsync_directory", fail_first_lock_parent_fsync)

    with pytest.raises(
        bootstrap.ControllerBootstrapError,
        match="install lock creation failed",
    ):
        bootstrap.prepare_source(context)

    assert failed is True
    assert context.trusted_root.is_dir()
    assert not install_lock.exists()


def test_prepare_source_removes_new_checkout_when_parent_fsync_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    source_root = context.trusted_root / context.source_sha
    real_fsync = bootstrap._fsync_directory
    failed = False

    def fail_first_checkout_parent_fsync(path: Path) -> None:
        nonlocal failed
        if path == context.trusted_root and source_root.exists() and not failed:
            failed = True
            raise OSError("injected checkout parent fsync failure")
        real_fsync(path)

    monkeypatch.setattr(bootstrap, "_fsync_directory", fail_first_checkout_parent_fsync)

    with pytest.raises(
        bootstrap.ControllerBootstrapError,
        match="source publication failed",
    ):
        bootstrap.prepare_source(context)

    assert failed is True
    assert not source_root.exists()
    assert list(context.trusted_root.glob(f".{context.source_sha}.candidate.*")) == []


def test_prepare_source_removes_new_checkout_when_final_validation_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    source_root = context.trusted_root / context.source_sha
    launcher = source_root / "deploy/slurm/install-loom-gb10-autoscaler-controller.sh"
    real_validate = bootstrap._validate_prepared_source
    mutated = False

    def mutate_artifact_then_validate(
        validation_context: bootstrap.BootstrapContext,
        validation_root: Path,
    ) -> bootstrap.PreparedSource:
        nonlocal mutated
        if validation_root == source_root and not mutated:
            launcher.write_text("#!/usr/bin/env bash\nexit 99\n", encoding="utf-8")
            mutated = True
        return real_validate(validation_context, validation_root)

    monkeypatch.setattr(bootstrap, "_validate_prepared_source", mutate_artifact_then_validate)

    with pytest.raises(bootstrap.ControllerBootstrapError, match="artifact"):
        bootstrap.prepare_source(context)

    assert mutated is True
    assert not source_root.exists()


def test_prepare_source_rerun_accepts_hard_stop_after_checkout_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    source_root = context.trusted_root / context.source_sha
    real_rename = bootstrap.os.rename

    class SimulatedHardStop(BaseException):
        pass

    def rename_then_stop(source: Path, destination: Path) -> None:
        real_rename(source, destination)
        raise SimulatedHardStop

    monkeypatch.setattr(bootstrap.os, "rename", rename_then_stop)

    with pytest.raises(SimulatedHardStop):
        bootstrap.prepare_source(context)

    assert source_root.is_dir()
    monkeypatch.setattr(bootstrap.os, "rename", real_rename)

    prepared = bootstrap.prepare_source(context)

    assert prepared.source_root == source_root
    assert prepared.launcher_path.is_file()


def test_prepare_source_never_removes_replacement_checkout_during_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    source_root = context.trusted_root / context.source_sha
    displaced = tmp_path / "displaced-checkout"
    replacement_sentinel = source_root / "replacement-sentinel"

    def replace_checkout_then_fail(
        _validation_context: bootstrap.BootstrapContext,
        validation_root: Path,
    ) -> bootstrap.PreparedSource:
        validation_root.rename(displaced)
        validation_root.mkdir(mode=0o700)
        replacement_sentinel.write_text("external replacement\n", encoding="ascii")
        raise bootstrap.ControllerBootstrapError("injected final validation failure")

    monkeypatch.setattr(bootstrap, "_validate_prepared_source", replace_checkout_then_fail)

    with pytest.raises(
        bootstrap.ControllerBootstrapError,
        match="source cleanup target changed",
    ):
        bootstrap.prepare_source(context)

    assert displaced.is_dir()
    assert replacement_sentinel.read_text(encoding="ascii") == "external replacement\n"


def test_prepare_source_never_removes_replacement_staging_checkout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    context = _context(tmp_path)
    displaced = tmp_path / "displaced-staging-checkout"
    replacement_sentinel: Path | None = None
    real_run_git = bootstrap._run_git

    def replace_staging_then_fail(
        run_context: bootstrap.BootstrapContext,
        arguments: list[str],
        **kwargs: object,
    ) -> subprocess.CompletedProcess[bytes]:
        nonlocal replacement_sentinel
        if arguments[0] == "clone":
            candidates = list(context.trusted_root.glob(f".{context.source_sha}.candidate.*"))
            assert len(candidates) == 1
            staging = candidates[0]
            staging.rename(displaced)
            staging.mkdir(mode=0o700)
            replacement_sentinel = staging / "replacement-sentinel"
            replacement_sentinel.write_text("external replacement\n", encoding="ascii")
            raise bootstrap.ControllerBootstrapError("injected staging failure")
        return real_run_git(run_context, arguments, **kwargs)

    monkeypatch.setattr(bootstrap, "_run_git", replace_staging_then_fail)

    with pytest.raises(bootstrap.ControllerBootstrapError):
        bootstrap.prepare_source(context)

    assert displaced.is_dir()
    assert replacement_sentinel is not None
    assert replacement_sentinel.read_text(encoding="ascii") == "external replacement\n"


def test_revalidation_does_not_execute_repository_configured_fsmonitor(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    prepared = bootstrap.prepare_source(context)
    marker = tmp_path / "fsmonitor-executed"
    fsmonitor = tmp_path / "malicious-fsmonitor"
    fsmonitor.write_text(
        f"#!/bin/sh\n: > '{marker}'\nprintf '{{}}'\n",
        encoding="utf-8",
    )
    fsmonitor.chmod(0o755)
    _git(prepared.source_root, "config", "core.fsmonitor", str(fsmonitor))

    with pytest.raises(bootstrap.ControllerBootstrapError):
        try:
            bootstrap.prepare_source(context)
        finally:
            assert not marker.exists()


@pytest.mark.parametrize(
    "relative",
    [
        ".git/commondir",
        ".git/config.worktree",
        ".git/info/grafts",
        ".git/objects/info/alternates",
        ".git/refs/replace",
        ".git/shallow",
        ".git/worktrees",
    ],
)
def test_revalidation_rejects_git_indirection_before_invoking_git(
    tmp_path: Path,
    relative: str,
) -> None:
    context = _context(tmp_path)
    marker = tmp_path / "git-invoked"
    git_wrapper = tmp_path / "git-wrapper"
    git_wrapper.write_text(
        f"#!/bin/sh\n: > '{marker}'\nexec /usr/bin/git \"$@\"\n",
        encoding="utf-8",
    )
    git_wrapper.chmod(0o755)
    context = replace(context, git_path=git_wrapper)
    prepared = bootstrap.prepare_source(context)
    marker.unlink()
    forbidden = prepared.source_root / relative
    forbidden.parent.mkdir(parents=True, exist_ok=True)
    for parent in forbidden.parents:
        if parent == prepared.source_root:
            break
        parent.chmod(0o700)
    forbidden.write_text(".\n", encoding="ascii")
    forbidden.chmod(0o600)

    with pytest.raises(bootstrap.ControllerBootstrapError):
        try:
            bootstrap.prepare_source(context)
        finally:
            assert not marker.exists()


def test_revalidation_rejects_symlinked_git_metadata_before_invoking_git(
    tmp_path: Path,
) -> None:
    context = _context(tmp_path)
    marker = tmp_path / "git-invoked"
    git_wrapper = tmp_path / "git-wrapper"
    git_wrapper.write_text(
        f"#!/bin/sh\n: > '{marker}'\nexec /usr/bin/git \"$@\"\n",
        encoding="utf-8",
    )
    git_wrapper.chmod(0o755)
    context = replace(context, git_path=git_wrapper)
    prepared = bootstrap.prepare_source(context)
    marker.unlink()
    objects = prepared.source_root / ".git/objects"
    external_objects = tmp_path / "external-objects"
    objects.rename(external_objects)
    objects.symlink_to(external_objects, target_is_directory=True)

    with pytest.raises(bootstrap.ControllerBootstrapError):
        try:
            bootstrap.prepare_source(context)
        finally:
            assert not marker.exists()


def test_launch_rejects_symlinked_artifact_ancestor_before_execution(tmp_path: Path) -> None:
    context = _context(tmp_path)
    prepared = bootstrap.prepare_source(context)
    tracked_deploy_paths = _git(prepared.source_root, "ls-files", "deploy").splitlines()
    for tracked in tracked_deploy_paths:
        _git(prepared.source_root, "update-index", "--skip-worktree", tracked)
    (prepared.source_root / ".git/index").chmod(0o600)
    deploy = prepared.source_root / "deploy"
    external_deploy = tmp_path / "external-deploy"
    deploy.rename(external_deploy)
    deploy.symlink_to(external_deploy, target_is_directory=True)
    exclude = prepared.source_root / ".git/info/exclude"
    exclude.write_text("deploy\n", encoding="ascii")
    exclude.chmod(0o600)
    assert (
        _git(
            prepared.source_root,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
        )
        == ""
    )
    (prepared.source_root / ".git/index").chmod(0o600)
    controller_key = tmp_path / "controller.pub"
    legacy_key = tmp_path / "legacy.pub"
    controller_key.write_text("controller\n", encoding="ascii")
    legacy_key.write_text("legacy\n", encoding="ascii")
    controller_key.chmod(0o600)
    legacy_key.chmod(0o600)
    executed = tmp_path / "executed"

    def execute(_path: Path, _arguments: tuple[str, ...], _environment: dict[str, str]) -> int:
        executed.write_text("executed\n", encoding="ascii")
        return 0

    with pytest.raises(bootstrap.ControllerBootstrapError) as raised:
        bootstrap.launch_controller(
            context,
            controller_public_key=controller_key,
            legacy_public_key=legacy_key,
            executor=execute,
        )

    assert str(raised.value) == "GB10 controller bootstrap source artifact parent is unsafe"
    assert not executed.exists()


def test_launch_controller_refuses_artifact_drift_before_execution(tmp_path: Path) -> None:
    context = _context(tmp_path)
    prepared = bootstrap.prepare_source(context)
    prepared.launcher_path.write_text("#!/usr/bin/env bash\nexit 99\n", encoding="utf-8")
    executed = tmp_path / "executed"
    controller_key = tmp_path / "controller.pub"
    legacy_key = tmp_path / "legacy.pub"
    controller_key.write_text("controller\n", encoding="ascii")
    legacy_key.write_text("legacy\n", encoding="ascii")
    controller_key.chmod(0o600)
    legacy_key.chmod(0o600)

    def execute(_path: Path, _arguments: tuple[str, ...], _environment: dict[str, str]) -> int:
        executed.write_text("executed\n", encoding="ascii")
        return 0

    with pytest.raises(bootstrap.ControllerBootstrapError, match="artifact"):
        bootstrap.launch_controller(
            context,
            controller_public_key=controller_key,
            legacy_public_key=legacy_key,
            executor=execute,
        )

    assert not executed.exists()


def test_launch_controller_holds_exclusive_lock_through_execution(tmp_path: Path) -> None:
    context = _context(tmp_path)
    controller_key = tmp_path / "controller.pub"
    legacy_key = tmp_path / "legacy.pub"
    controller_key.write_text("controller\n", encoding="ascii")
    legacy_key.write_text("legacy\n", encoding="ascii")
    controller_key.chmod(0o600)
    legacy_key.chmod(0o600)
    observed_lock = False

    def execute(_path: Path, _arguments: tuple[str, ...], environment: dict[str, str]) -> int:
        nonlocal observed_lock
        inherited_descriptor = int(environment["LOOM_GB10_BOOTSTRAP_LOCK_FD"])
        os.fstat(inherited_descriptor)
        contender = os.open(context.trusted_root / ".install.lock", os.O_RDWR)
        try:
            with pytest.raises(BlockingIOError):
                fcntl.flock(contender, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(contender)
        observed_lock = True
        return 0

    result = bootstrap.launch_controller(
        context,
        controller_public_key=controller_key,
        legacy_public_key=legacy_key,
        executor=execute,
    )

    assert result == 0
    assert observed_lock is True


def test_production_parser_rejects_source_location_overrides() -> None:
    with pytest.raises(SystemExit) as raised:
        bootstrap._parser().parse_args(
            [
                "--source-sha",
                "1" * 40,
                "--controller-public-key",
                "/root/controller.pub",
                "--legacy-public-key",
                "/root/legacy.pub",
                "--trusted-root",
                "/tmp/untrusted",
            ]
        )

    assert raised.value.code == 2
