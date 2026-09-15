"""Exercise activation publication and crash recovery on a real temporary filesystem."""

import os
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
import scripts.ops.install_capacity_executor as installer_module
from scripts.ops.install_capacity_executor import (
    CapacityExecutorInstallError,
    CommandResult,
    ControllerInstaller,
)
from tests.loom_cli.rollout.operator.test_protected_active_controller import _request
from tests.ops.test_install_capacity_executor import (
    FakeHostRunner,
    _context,
    _fake_release_extractor,
)

from loom_cli.rollout.operator.protected_active_controller import ActiveControllerEvidence

_TIMER = "loom-capacity-pool-executor-active.timer"


class ActiveHostRunner(FakeHostRunner):
    def __init__(self, root):
        super().__init__(root)
        self.validation_result = CommandResult(0)
        self.validation_hook = lambda: None
        self.fail_start = False

    def run(self, argv, *, check=True, env=None):
        call = tuple(argv)
        if call[0] == "/usr/sbin/runuser" and "--validate-activation-only" in call:
            self.calls.append(call)
            assert call[1:4] == ("--user", "loom_capacity_executor", "--")
            assert call[5:9] == ("-I", "-B", "-m", "loom_capacity_pool_controller")
            self.validation_hook()
            return self.validation_result
        if (
            call[:2] in {("/usr/bin/systemctl", "enable"), ("/usr/bin/systemctl", "start")}
            and call[-1] == _TIMER
        ):
            self.calls.append(call)
            assert len(call) == 3
            if call[1] == "enable":
                self.enabled_units.add(_TIMER)
            elif self.fail_start:
                raise CapacityExecutorInstallError("simulated interruption before timer start")
            else:
                self.active_units.add(_TIMER)
            return CommandResult(0)
        return super().run(argv, check=check, env=env)


def _installed(tmp_path):
    request = _request(tmp_path)
    runner = ActiveHostRunner(tmp_path)
    runner.group_present = runner.user_present = True
    installer = ControllerInstaller(
        context=_context(tmp_path),
        runner=runner,
        extractor=_fake_release_extractor,
        machine="x86_64",
        hostname="TRT-EAI-OLDLAB-1",
        effective_uid=0,
    )
    installer.converge_prerequisite(request.prepared.prerequisite)
    installer.converge_prepared_files(request.prepared)
    return request, installer, runner


def _path(root, absolute):
    return root.joinpath(*Path(absolute).parts[1:])


def test_active_publication_and_enable_replay(tmp_path):
    request, installer, runner = _installed(tmp_path)
    assert installer.observe_active(request) is None
    staged = installer.converge_active_files(request)
    assert staged.state == "staged"
    assert ActiveControllerEvidence.from_bytes(staged.to_bytes()) == staged
    for path, payload in request.files.items():
        assert _path(tmp_path, path).read_bytes() == payload
        assert _path(tmp_path, path).stat().st_mode & 0o777 == 0o600
    assert not runner.active_units and not runner.enabled_units
    active = installer.enable_active_timer(request)
    assert active.state == "active"
    start_calls = [call for call in runner.calls if call == ("/usr/bin/systemctl", "start", _TIMER)]
    assert installer.enable_active_timer(request) == active
    assert [
        call for call in runner.calls if call == ("/usr/bin/systemctl", "start", _TIMER)
    ] == start_calls
    assert sum("--validate-activation-only" in call for call in runner.calls) == 2
    with pytest.raises(CapacityExecutorInstallError, match="all executor units stopped"):
        installer.converge_active_files(request)


def test_active_files_resume_after_partial_publication(tmp_path, monkeypatch):
    request, installer, runner = _installed(tmp_path)
    publish = installer._publish_active_input

    def interrupted(path, payload, **kwargs):
        if str(path) == list(request.files)[1]:
            raise OSError("simulated crash")
        return publish(path, payload, **kwargs)

    monkeypatch.setattr(installer, "_publish_active_input", interrupted)
    with pytest.raises(OSError, match="simulated crash"):
        installer.converge_active_files(request)
    assert installer.observe_active(request) is None
    assert not runner.active_units and not runner.enabled_units
    monkeypatch.setattr(installer, "_publish_active_input", publish)
    assert installer.converge_active_files(request).state == "staged"
    assert installer.enable_active_timer(request).state == "active"


def test_active_timer_resumes_enabled_but_inactive(tmp_path):
    request, installer, runner = _installed(tmp_path)
    installer.converge_active_files(request)
    runner.fail_start = True
    with pytest.raises(CapacityExecutorInstallError, match="simulated interruption"):
        installer.enable_active_timer(request)
    assert installer.observe_active(request).state == "enabling"
    runner.fail_start = False
    assert installer.enable_active_timer(request).state == "active"
    assert runner.calls.count(("/usr/bin/systemctl", "enable", _TIMER)) == 1


@pytest.mark.parametrize(
    "failure", ["manager", "stderr", "stdout", "file-drift", "release-drift", "prepared-overlap"]
)
def test_active_validation_failure_or_drift_never_enables_timer(tmp_path, failure):
    request, installer, runner = _installed(tmp_path)
    installer.converge_active_files(request)
    if failure == "manager":
        runner.validation_result = CommandResult(1)
    elif failure == "stderr":
        runner.validation_result = CommandResult(0, "", "unexpected")
    elif failure == "stdout":
        runner.validation_result = CommandResult(0, "unexpected", "")
    elif failure == "file-drift":
        runner.validation_hook = lambda: _path(tmp_path, next(iter(request.files))).write_bytes(
            b"changed"
        )
    elif failure == "release-drift":
        runner.validation_hook = lambda: setattr(runner, "image_revision", "9" * 40)
    else:
        runner.validation_hook = lambda: runner.active_units.add(
            "loom-capacity-pool-executor-prepared.timer"
        )
    with pytest.raises(CapacityExecutorInstallError):
        installer.enable_active_timer(request)
    assert _TIMER not in runner.enabled_units and _TIMER not in runner.active_units


@pytest.mark.parametrize(
    "failure", ["different-operation", "different-file", "symlink", "hardlink", "public-mode"]
)
def test_active_publication_never_replaces_conflicting_evidence(tmp_path, failure):
    request, installer, runner = _installed(tmp_path)
    installer.converge_active_files(request)
    path = _path(tmp_path, next(iter(request.files)))
    if failure == "different-operation":
        request = replace(request, operation_id=uuid4())
    elif failure == "different-file":
        path.write_bytes(b"conflict")
    elif failure == "symlink":
        target = tmp_path / "foreign"
        path.rename(target)
        path.symlink_to(target)
    elif failure == "hardlink":
        os.link(path, tmp_path / "foreign")
    else:
        path.chmod(0o644)
    before = path.read_bytes()
    with pytest.raises(CapacityExecutorInstallError):
        installer.converge_active_files(request)
    assert path.read_bytes() == before
    assert not runner.enabled_units and not runner.active_units


def test_active_operation_lock_refuses_concurrent_publisher(tmp_path):
    request, installer, _runner = _installed(tmp_path)
    other = ControllerInstaller(context=installer.context, runner=installer.runner, effective_uid=0)
    with other._controller_operation_lock(), pytest.raises(BlockingIOError):
        installer.converge_active_files(request)


@pytest.mark.parametrize(
    "operation", ["observe-active", "converge-active-files", "enable-active-timer"]
)
def test_active_wire_bounds_and_dispatch(tmp_path, operation):
    request, installer, _runner = _installed(tmp_path)
    handler = installer_module._active_controller_operation
    if operation == "enable-active-timer":
        installer.converge_active_files(request)
    value = handler(installer, operation, request.to_bytes())
    if operation == "observe-active":
        assert value == b"null\n"
    else:
        evidence = ActiveControllerEvidence.from_bytes(value)
        assert evidence.request_sha256 == request.request_sha256
    with pytest.raises(CapacityExecutorInstallError, match="request"):
        handler(installer, operation, request.to_bytes() + b" ")


def test_prepared_enable_and_active_staging_share_one_mutation_lock(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Event

    request, installer, runner = _installed(tmp_path)
    entered, resume = Event(), Event()
    run = installer._run
    def paused(*argv, **kwargs):
        if argv == ("/usr/bin/systemctl", "enable", "--now", "loom-capacity-pool-executor-prepared.timer"):
            entered.set()
            assert resume.wait(10)
        return run(*argv, **kwargs)
    monkeypatch.setattr(installer, "_run", paused)
    with ThreadPoolExecutor(max_workers=1) as pool:
        prepared = pool.submit(installer.enable_prepared_timer, request.prepared)
        try:
            assert entered.wait(10)
            with pytest.raises(BlockingIOError):
                installer.converge_active_files(request)
        finally:
            resume.set()
            prepared.result(timeout=10)
    assert _TIMER not in runner.enabled_units


def test_retained_active_intent_fences_prepared_enable_and_lives_outside_service_directory(tmp_path):
    request, installer, runner = _installed(tmp_path)
    installer.converge_active_files(request)
    marker = installer._active_marker_path()
    assert str(marker).startswith("/opt/loom-capacity-executor-releases/")
    metadata = _path(tmp_path, marker).stat()
    parent = _path(tmp_path, marker).parent.stat()
    assert (metadata.st_uid, metadata.st_gid) == (installer.context.authority_uid, installer.context.authority_gid)
    assert (parent.st_uid, parent.st_gid, parent.st_mode & 0o777) == (installer.context.authority_uid, installer.context.authority_gid, 0o700)
    with pytest.raises(CapacityExecutorInstallError, match="retained active operation"):
        installer.enable_prepared_timer(request.prepared)
    assert not runner.enabled_units
