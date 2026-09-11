"""One-use trusted handoff composes with the fixed worker container lifecycle."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta
from uuid import UUID

import pytest

from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffError, BootstrapHandoffStore
from tests.unit.test_capacity_executor_bootstrap_handoff import _NOW, _Admission, _physical
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture
from tests.unit.test_native_worker_container import _allocation, _prepared
from tests.unit.test_worker_native_entrypoint import _configured_bootstrap


@pytest.fixture(autouse=True)
def native_cgroup(tmp_path, monkeypatch):
    import os

    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_cgroup import open_native_cgroup
    from tests.unit.test_native_worker_cgroup import _job

    root = tmp_path / "cgroups"
    root.mkdir(mode=0o700)
    directory = _job(root)
    # Real production directory/control checks against a disposable filesystem.
    # No production bypass flag is exposed by the launcher.
    monkeypatch.setattr(launch, "open_native_cgroup", lambda allocation: open_native_cgroup(
        allocation, cgroup_root=root, trusted_uid=os.geteuid()), raising=False)
    return directory


def _handoff(tmp_path):
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding.model_copy(update={
        "intent_id": UUID(_allocation().intent_id),
        "resources": launch_context_fixture().binding.resources.model_copy(update={
            "gpu_count": 0, "generic": {}, "cpu_millicores": _allocation().cpu_millicores,
            "memory_bytes": _allocation().memory_bytes}),
        "candidate": launch_context_fixture().binding.candidate.model_copy(update={"identity": _allocation().candidate_sha}),
    })
    binding = type(binding).model_validate_json(binding.model_dump_json())
    lease = BootstrapHandoffStore(directory).prepare(binding, bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256)
    bootstrap = _configured_bootstrap()
    native = bootstrap.native_execution.model_copy(update={
        "root_activated_at": (_NOW - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "root_expires_at": (_NOW + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
    })
    return directory, lease.reference, _physical(binding), replace(bootstrap, native_execution=native)


class CLI:
    stop_requested = None

    def __init__(self):
        self.calls = []
        self.create_failed = False
        self.remaining = b""

    def call(self, *args, **kwargs):
        from loom_capacity_executor.native_worker_container import NativeContainerError

        self.calls.append(args)
        if args[:2] == ("container", "create"):
            if self.create_failed:
                raise NativeContainerError("create outcome uncertain")
            return b"a" * 64 + b"\n"
        if args[:2] == ("container", "ls"):
            return self.remaining
        return b""

    def json(self, *args, **kwargs):
        self.calls.append(args)
        return [{"Id": "a" * 64, "Image": _prepared().image_id}]


async def test_consumed_handoff_starts_only_once_and_cleans_after_attached_exit(tmp_path, monkeypatch):
    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_container import NativeWorkerContainerPolicyV2

    directory, reference, physical, bootstrap = _handoff(tmp_path)
    cli, admission, observed = CLI(), _Admission(), []
    policy = NativeWorkerContainerPolicyV2(native_execution=bootstrap.native_execution,
        canonical_worker_settings=bootstrap.canonical_worker_settings,
        docker_config_directory="/etc/loom/empty-docker", pids_max=128)

    async def attached(client, container_id, frame):
        assert (directory / reference).with_suffix(".launched").is_file()
        assert not (directory / reference).with_suffix(".credential").exists()
        observed.append(frame)
        return 0

    monkeypatch.setattr(launch, "run_attached_native_worker", attached)
    monkeypatch.setattr(launch, "_disable_bootstrap_dumps", lambda: None)
    kwargs = dict(directory=directory, reference=reference, physical=physical, admission=admission,
        policy=policy, cli=cli, image=_prepared(), allocation=_allocation(), now=lambda: _NOW)
    await launch.launch_native_worker_once(**kwargs)
    assert len(observed) == 1
    assert any(call[:3] == ("container", "rm", "--force") for call in cli.calls)
    assert observed[0].worker_settings()["pool_name"] == "oldlab"
    assert observed[0].worker_credential not in repr(cli.calls)
    with pytest.raises(BootstrapHandoffError, match="already launched"):
        await launch.launch_native_worker_once(**kwargs)
    assert len(observed) == 1


@pytest.mark.parametrize("failure", ["create", "start", "cleanup", "expired"])
async def test_failed_native_launch_never_retries_or_claims_unconfirmed_cleanup(tmp_path, monkeypatch, failure):
    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        NativeWorkerContainerPolicyV2,
    )

    directory, reference, physical, bootstrap = _handoff(tmp_path)
    cli = CLI()
    if failure == "create":
        cli.create_failed = True
    if failure == "cleanup":
        cli.remaining = b"a" * 64
    policy = NativeWorkerContainerPolicyV2(native_execution=bootstrap.native_execution,
        canonical_worker_settings=bootstrap.canonical_worker_settings,
        docker_config_directory="/etc/loom/empty-docker", pids_max=128)

    async def attached(*args):
        if failure == "start":
            raise NativeContainerError("start outcome uncertain")
        return 0

    monkeypatch.setattr(launch, "run_attached_native_worker", attached)
    monkeypatch.setattr(launch, "_disable_bootstrap_dumps", lambda: None)
    with pytest.raises((NativeContainerError, BootstrapHandoffError)):
        await launch.launch_native_worker_once(directory=directory, reference=reference,
            physical=physical, admission=_Admission(), policy=policy, cli=cli,
            image=_prepared(), allocation=_allocation(),
            now=lambda: _NOW + timedelta(days=2) if failure == "expired" else _NOW)
    created = [call for call in cli.calls if call[:2] == ("container", "create")]
    assert len(created) == (0 if failure == "expired" else 1)
    if failure != "expired":
        assert (directory / reference).with_suffix(".launched").is_file()


async def test_trusted_process_selects_native_branch_without_legacy_credential_environment(tmp_path, monkeypatch):
    from loom_capacity_executor import native_worker_launch
    from loom_capacity_executor.trusted_launcher import run_trusted_launcher_process
    from tests.unit.test_capacity_executor_bootstrap_handoff import (
        _trusted_candidate_config_payload,
        _trusted_launcher_process_argv_for_candidate_config,
        _write_candidate,
    )

    directory, admission_directory = tmp_path / "handoff", tmp_path / "admission"
    directory.mkdir(mode=0o700)
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "docker"
    _write_candidate(candidate)
    config = _trusted_candidate_config_payload(handoff_directory=directory,
        admission_directory=admission_directory, candidate_path=candidate)
    bootstrap = _configured_bootstrap()
    config.update(candidate_argv=(str(candidate),), native_worker={
        "native_execution": bootstrap.native_execution.model_dump(mode="json"),
        "canonical_worker_settings": bootstrap.canonical_worker_settings,
        "docker_config_directory": "/etc/loom/empty-docker", "pids_max": 128})
    argv = _trusted_launcher_process_argv_for_candidate_config(tmp_path, config_payload=config)
    observed = []

    async def native(**kwargs):
        observed.append(kwargs)

    def forbidden_exec(*args):
        pytest.fail("native launch fell back to legacy credential environment")

    monkeypatch.setattr(native_worker_launch, "run_native_worker_on_host", native)
    await run_trusted_launcher_process(argv, environment={"SLURM_JOB_ID": "101", "EVIL": "ambient"},
        now=lambda: _NOW, admission_factory=lambda *args, **kwargs: _Admission(), execvpe=forbidden_exec)
    assert len(observed) == 1
    assert observed[0]["physical"].slurm_job_id == "101"
    assert observed[0]["image_digest"] == config["candidate_image_digest"]
    assert "ambient" not in repr(observed)


@pytest.mark.parametrize("field,value", [("pool_id", "gb10"), ("hostname", "foreign"),
    ("cpu_millicores", 2000), ("candidate_sha", "e" * 64)])
async def test_substituted_allocation_refused_before_registration(tmp_path, monkeypatch, field, value):
    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        NativeWorkerContainerPolicyV2,
    )

    directory, reference, physical, bootstrap = _handoff(tmp_path)
    cli, admission = CLI(), _Admission()
    policy = NativeWorkerContainerPolicyV2(native_execution=bootstrap.native_execution,
        canonical_worker_settings=bootstrap.canonical_worker_settings,
        docker_config_directory="/etc/loom/empty-docker", pids_max=128)
    with pytest.raises(NativeContainerError, match="physical binding"):
        await launch.launch_native_worker_once(directory=directory, reference=reference,
            physical=physical, admission=admission, policy=policy, cli=cli, image=_prepared(),
            allocation=replace(_allocation(), **{field: value}), now=lambda: _NOW)
    assert not admission.requests
    assert not cli.calls


def test_native_daemon_readback_uses_machine_readable_docker_info():
    from loom_capacity_executor.native_worker_launch import native_daemon_cgroup_driver

    class DaemonCLI:
        def json(self, *args):
            assert args == ("info", "--format={{json .}}")
            return {"CgroupVersion": "2", "CgroupDriver": "cgroupfs"}

    assert native_daemon_cgroup_driver(DaemonCLI()) == "cgroupfs"


def test_native_daemon_rejects_systemd_sibling_slice_topology():
    from loom_capacity_executor.native_worker_container import NativeContainerError
    from loom_capacity_executor.native_worker_launch import native_daemon_cgroup_driver

    class DaemonCLI:
        def json(self, *args):
            return {"CgroupVersion": "2", "CgroupDriver": "systemd"}

    with pytest.raises(NativeContainerError, match="cgroupfs"):
        native_daemon_cgroup_driver(DaemonCLI())


async def test_complete_bootstrap_frame_checked_before_handoff(tmp_path, monkeypatch):
    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_bootstrap import NativeBootstrapError
    from loom_capacity_executor.native_worker_container import NativeWorkerContainerPolicyV2

    directory, reference, physical, bootstrap = _handoff(tmp_path)
    admission = _Admission()
    policy = NativeWorkerContainerPolicyV2(native_execution=bootstrap.native_execution,
        canonical_worker_settings=bootstrap.canonical_worker_settings,
        docker_config_directory="/etc/loom/empty-docker", pids_max=128)

    def refuses_frame(value):
        assert value.worker_settings()["pool_name"] == "oldlab"
        raise NativeBootstrapError("complete frame exceeds bound")

    monkeypatch.setattr(launch, "encode_native_bootstrap", refuses_frame)
    with pytest.raises(NativeBootstrapError):
        await launch.launch_native_worker_once(directory=directory, reference=reference,
            physical=physical, admission=admission, policy=policy, cli=CLI(), image=_prepared(),
            allocation=_allocation(), now=lambda: _NOW)
    assert not admission.requests
    assert not (directory / reference).with_suffix(".launched").exists()


@pytest.mark.parametrize("phase", ["control", "attached"])
def test_real_sigterm_interrupts_native_cli_and_allows_cleanup(phase):
    import subprocess
    import sys

    script = '''
import asyncio, os, signal, subprocess, sys, threading
from loom_capacity_executor.native_worker_launch import native_termination_boundary, run_attached_native_worker
from loom_capacity_executor.native_worker_container import NativeContainerError, capture_native_control_output
from tests.unit.test_worker_native_entrypoint import _configured_bootstrap
phase = sys.argv[1]
async def main():
    with native_termination_boundary() as stopping:
        timer = threading.Timer(0.2, lambda: os.kill(os.getpid(), signal.SIGTERM))
        timer.start()
        try:
            if phase == 'control':
                with subprocess.Popen([sys.executable, '-I', '-c', 'import time; time.sleep(30)'],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE) as child:
                    try:
                        capture_native_control_output(child, timeout=30, stop_requested=stopping)
                    finally:
                        assert child.poll() is not None
            else:
                class CLI:
                    executable = sys.executable
                    descriptor = os.open('/dev/null', os.O_RDONLY)
                    stop_requested = staticmethod(stopping)
                    def argv(self, *args):
                        return (sys.executable, '-I', '-c', 'import time; time.sleep(30)')
                cli = CLI()
                try:
                    await run_attached_native_worker(cli, 'a'*64, _configured_bootstrap())
                finally:
                    os.close(cli.descriptor)
        except NativeContainerError:
            assert stopping()
            os.kill(os.getpid(), signal.SIGTERM)
            print('cleanup-can-finish', flush=True)
        else:
            raise AssertionError('termination did not interrupt the operation')
        finally:
            timer.join()
asyncio.run(main())
'''
    result = subprocess.run([sys.executable, "-c", script, phase], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "cleanup-can-finish"


def test_unused_native_scratch_is_removed_and_same_intent_can_retry(tmp_path):
    from loom_capacity_executor.native_worker_launch import create_native_scratch

    first = create_native_scratch(tmp_path, _allocation().intent_id)
    path = first.directory
    assert path.is_dir()
    first.discard_if_unused()
    assert not path.exists()
    second = create_native_scratch(tmp_path, _allocation().intent_id)
    assert second.directory != path
    second.discard_if_unused()


def test_runtime_possible_scratch_is_retained_until_descendant_cleanup(tmp_path):
    from loom_capacity_executor.native_worker_launch import create_native_scratch

    scratch = create_native_scratch(tmp_path, _allocation().intent_id)
    scratch.creation_possible = True
    scratch.discard_if_unused()
    assert scratch.directory.is_dir()


def test_unused_scratch_cleanup_refuses_replaced_child(tmp_path):
    from loom_capacity_executor.native_worker_container import NativeContainerError
    from loom_capacity_executor.native_worker_launch import create_native_scratch

    scratch = create_native_scratch(tmp_path, _allocation().intent_id)
    child = scratch.directory / "tmp"
    child.rename(scratch.directory / "original")
    child.mkdir()
    with pytest.raises(NativeContainerError, match="identity"):
        scratch.discard_if_unused()
    assert child.is_dir()


def test_partial_scratch_initialization_removes_only_new_unused_directories(tmp_path, monkeypatch):
    from pathlib import Path

    from loom_capacity_executor.native_worker_container import NativeContainerError
    from loom_capacity_executor.native_worker_launch import create_native_scratch

    original = Path.mkdir

    def fail_last(path, *args, **kwargs):
        if path.name == "benchmarks":
            raise OSError("simulated filesystem failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", fail_last)
    with pytest.raises(NativeContainerError, match="initialization"):
        create_native_scratch(tmp_path, _allocation().intent_id)
    assert not list(tmp_path.iterdir())


async def test_escaped_settings_that_fit_inner_limit_cannot_burn_handoff(tmp_path):
    import json

    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_bootstrap import (
        NativeBootstrapError,
        NativeWorkerBootstrap,
    )
    from loom_capacity_executor.native_worker_container import (
        NativeWorkerContainerPolicyV2,
        bind_native_settings,
    )

    directory, reference, physical, bootstrap = _handoff(tmp_path)
    settings = json.dumps(bootstrap.worker_settings() | {"token": '"' * 610},
        sort_keys=True, separators=(",", ":"))
    policy = NativeWorkerContainerPolicyV2(native_execution=bootstrap.native_execution,
        canonical_worker_settings=settings, docker_config_directory="/etc/loom/empty-docker", pids_max=128)
    provisional = bind_native_settings(NativeWorkerBootstrap(native_execution=bootstrap.native_execution,
        worker_credential="x" * 512, canonical_worker_settings=settings), _allocation())
    assert len(provisional.canonical_worker_settings) <= 2048
    admission, cli = _Admission(), CLI()
    with pytest.raises(NativeBootstrapError):
        await launch.launch_native_worker_once(directory=directory, reference=reference,
            physical=physical, admission=admission, policy=policy, cli=cli, image=_prepared(),
            allocation=_allocation(), now=lambda: _NOW)
    assert not admission.requests
    assert not cli.calls
    assert not (directory / reference).with_suffix(".launched").exists()


async def test_stop_during_registration_does_not_burn_launch_marker(tmp_path, monkeypatch):
    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_container import (
        NativeContainerError,
        NativeWorkerContainerPolicyV2,
    )

    directory, reference, physical, bootstrap = _handoff(tmp_path)
    cli, admission = CLI(), _Admission()
    cli.stop_requested = lambda: bool(admission.requests)
    policy = NativeWorkerContainerPolicyV2(native_execution=bootstrap.native_execution,
        canonical_worker_settings=bootstrap.canonical_worker_settings,
        docker_config_directory="/etc/loom/empty-docker", pids_max=128)
    monkeypatch.setattr(launch, "_disable_bootstrap_dumps", lambda: None)
    with pytest.raises(NativeContainerError, match="before consumption"):
        await launch.launch_native_worker_once(directory=directory, reference=reference,
            physical=physical, admission=admission, policy=policy, cli=cli, image=_prepared(),
            allocation=_allocation(), now=lambda: _NOW)
    assert admission.requests
    assert not cli.calls
    assert not (directory / reference).with_suffix(".launched").exists()


@pytest.mark.parametrize("phase", ["before-handoff", "during-registration", "during-marker", "after-create"])
async def test_cgroup_drift_never_starts_native_worker(tmp_path, monkeypatch, native_cgroup, phase):
    from loom_capacity_executor import native_worker_launch as launch
    from loom_capacity_executor.native_worker_container import NativeContainerError, NativeWorkerContainerPolicyV2

    directory, reference, physical, bootstrap = _handoff(tmp_path)
    policy = NativeWorkerContainerPolicyV2(native_execution=bootstrap.native_execution,
        canonical_worker_settings=bootstrap.canonical_worker_settings,
        docker_config_directory="/etc/loom/empty-docker", pids_max=128)
    admission, cli, attached = _Admission(), CLI(), []

    def drift():
        (native_cgroup / "memory.max").write_text("max\n")

    if phase == "before-handoff":
        drift()
    elif phase == "during-registration":
        register = admission.register_worker

        async def drifting_register(*args, **kwargs):
            result = await register(*args, **kwargs)
            drift()
            return result

        admission.register_worker = drifting_register
    elif phase == "during-marker":
        consume = launch.claim_bootstrap_handoff_launch

        def drifting_consume(*args, **kwargs):
            credential = consume(*args, **kwargs)
            drift()
            return credential

        monkeypatch.setattr(launch, "claim_bootstrap_handoff_launch", drifting_consume)
    else:
        inspect = cli.json

        def drifting_inspect(*args, **kwargs):
            result = inspect(*args, **kwargs)
            drift()
            return result

        cli.json = drifting_inspect

    async def attach(*args):
        attached.append(args)
        return 0

    monkeypatch.setattr(launch, "run_attached_native_worker", attach)
    monkeypatch.setattr(launch, "_disable_bootstrap_dumps", lambda: None)
    with pytest.raises(NativeContainerError, match="cgroup"):
        await launch.launch_native_worker_once(directory=directory, reference=reference,
            physical=physical, admission=admission, policy=policy, cli=cli, image=_prepared(),
            allocation=_allocation(), now=lambda: _NOW)
    assert not attached
    if phase != "after-create":
        assert not cli.calls
    else:
        assert any(call[:3] == ("container", "rm", "--force") for call in cli.calls)
    assert bool(admission.requests) == (phase != "before-handoff")
    assert (directory / reference).with_suffix(".launched").exists() == (phase in {"during-marker", "after-create"})
