"""One-use trusted handoff composes with the fixed worker container lifecycle."""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

import pytest

from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffError, BootstrapHandoffStore
from tests.unit.test_capacity_executor_bootstrap_handoff import _NOW, _Admission, _physical
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture
from tests.unit.test_native_worker_container import _allocation, _prepared
from tests.unit.test_worker_native_entrypoint import _configured_bootstrap


def _handoff(tmp_path):
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
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
    from loom_capacity_executor.native_worker_container import NativeContainerError, NativeWorkerContainerPolicyV2

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
