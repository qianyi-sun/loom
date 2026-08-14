from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest

import loom_capacity_executor.bootstrap_handoff as handoff_module
from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2, PhysicalJobBindingV2
from loom_capacity_executor.bootstrap_handoff import (
    BootstrapHandoffError,
    BootstrapHandoffRecordV2,
    BootstrapHandoffStore,
    consume_bootstrap_handoff,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture

_NOW = datetime(2026, 8, 13, 16, 0, tzinfo=UTC)


class _Admission:
    route_sha256 = "1" * 64

    def __init__(self) -> None:
        self.requests: list[ExecutableWorkerRegistrationV2] = []
        self.capabilities: list[str] = []

    def bootstrap_handoff_route_sha256(self, _binding: object) -> str:
        return self.route_sha256

    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str,
    ) -> SimpleNamespace:
        self.requests.append(request)
        self.capabilities.append(bootstrap_capability)
        return SimpleNamespace(
            intent_id=request.binding.intent_id,
            worker_id=request.worker_id,
            worker_incarnation=request.worker_incarnation,
            protected_registration_epoch=request.protected_registration_epoch,
        )


class _CommitThenCrashAdmission(_Admission):
    def __init__(self) -> None:
        super().__init__()
        self.crashes_remaining = 1

    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str,
    ) -> SimpleNamespace:
        result = await super().register_worker(
            request,
            bootstrap_capability=bootstrap_capability,
        )
        if self.crashes_remaining:
            self.crashes_remaining -= 1
            raise RuntimeError("protected registration committed before response loss")
        return result


def _physical(binding: ExecutableIntentBindingV2) -> PhysicalJobBindingV2:
    return PhysicalJobBindingV2(
        operation_id=uuid5(UUID("cb359b0c-a844-4bc5-9592-a4c35e344f3d"), "physical-bind"),
        binding=binding,
        bootstrap_registration_epoch=1,
        slurm_job_id="101",
        ownership_evidence_sha256="a" * 64,
    )


# Production break caught: bootstrap evidence must come from a one-time CSPRNG
# handoff file; the manager and journal receive only its SHA-256 digest.
def test_bootstrap_handoff_generates_private_random_capability(tmp_path: Path) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)

    lease = store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )

    deterministic = hashlib.sha256(f"bootstrap-{binding.intent_id}".encode("ascii")).hexdigest()
    assert lease.bootstrap_sha256 != deterministic
    assert len(lease.bootstrap_sha256) == 64
    assert lease.reference in {
        item.name for item in directory.iterdir() if item.is_file() and not item.is_symlink()
    }
    path = directory / lease.reference
    assert path.stat().st_mode & 0o777 == 0o600


# Production break caught: the trusted wrapper must exchange the bootstrap
# capability once, remove the clear capability before worker code runs, and keep
# the scoped worker credential replayable across the exec boundary.
async def test_trusted_wrapper_consumes_handoff_once_and_registers_worker(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    physical = _physical(binding)
    admission = _Admission()

    credential = await consume_bootstrap_handoff(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )

    assert (
        hashlib.sha256(admission.capabilities[0].encode("ascii")).hexdigest()
        == lease.bootstrap_sha256
    )
    assert admission.requests[0].binding == binding
    assert admission.requests[0].slurm_job_id == "101"
    assert (
        admission.requests[0].worker_credential_sha256
        == hashlib.sha256(credential.encode("ascii")).hexdigest()
    )
    assert credential != admission.capabilities[0]
    assert not (directory / lease.reference).exists()
    replayed = await consume_bootstrap_handoff(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )
    assert replayed == credential
    assert len(admission.requests) == 1
    assert len(admission.capabilities) == 1


async def test_trusted_wrapper_replays_committed_registration_byte_identically(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    physical = _physical(binding)
    admission = _CommitThenCrashAdmission()

    with pytest.raises(RuntimeError, match="response loss"):
        await consume_bootstrap_handoff(
            directory,
            lease.reference,
            physical,
            admission,
            now=lambda: _NOW,
        )
    assert not (directory / lease.reference).exists()
    assert (directory / lease.reference).with_suffix(".used").exists()

    credential = await consume_bootstrap_handoff(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )

    assert admission.requests[1] == admission.requests[0]
    assert admission.capabilities[1] == admission.capabilities[0]
    assert (
        admission.requests[0].worker_credential_sha256
        == hashlib.sha256(credential.encode("ascii")).hexdigest()
    )
    assert not (directory / lease.reference).exists()
    assert not (directory / lease.reference).with_suffix(".used").exists()


async def test_trusted_wrapper_replays_credential_after_success_before_candidate_exec(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    physical = _physical(binding)
    admission = _Admission()

    credential = await consume_bootstrap_handoff(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )
    credential_files = tuple(directory.glob("*.credential"))
    assert len(credential_files) == 1
    persisted = credential_files[0].read_text(encoding="utf-8")
    assert admission.capabilities[0] not in persisted

    recovered = await consume_bootstrap_handoff(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )

    assert recovered == credential
    assert len(admission.requests) == 1
    assert len(admission.capabilities) == 1


def test_prepare_keeps_concurrent_existing_capability_without_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)
    reference = store.reference_for(binding)
    path = directory / reference
    winner = BootstrapHandoffRecordV2(
        binding=binding,
        bootstrap_registration_epoch=1,
        capability="w" * 43,
        capability_sha256=hashlib.sha256(("w" * 43).encode("ascii")).hexdigest(),
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    original_exists = Path.exists
    injected = False

    def racing_exists(candidate: Path) -> bool:
        nonlocal injected
        if candidate == path and not injected:
            injected = True
            path.write_bytes(canonical_executable_bytes(winner))
            path.chmod(0o600)
            return False
        return original_exists(candidate)

    monkeypatch.setattr(Path, "exists", racing_exists)

    lease = store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )

    assert lease.bootstrap_sha256 == winner.capability_sha256
    stored = BootstrapHandoffRecordV2.model_validate_json(path.read_bytes())
    assert stored.capability == winner.capability


async def test_consumer_uses_concurrent_claim_without_replacing_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    physical = _physical(binding)
    record = BootstrapHandoffRecordV2.model_validate_json(
        (directory / lease.reference).read_bytes()
    )
    winner = handoff_module._claim_for(record, physical)
    used = (directory / lease.reference).with_suffix(".used")
    original_exists = Path.exists
    injected = False

    def racing_exists(candidate: Path) -> bool:
        nonlocal injected
        if candidate == used and not injected:
            injected = True
            used.write_bytes(canonical_executable_bytes(winner))
            used.chmod(0o600)
            return False
        return original_exists(candidate)

    monkeypatch.setattr(Path, "exists", racing_exists)
    admission = _Admission()

    credential = await consume_bootstrap_handoff(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )

    assert admission.requests == [winner.worker_registration]
    assert credential == winner.worker_credential
    assert not (directory / lease.reference).exists()
    assert not used.exists()


async def test_trusted_wrapper_rejects_handoff_for_changed_admission_route(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256="1" * 64,
    )
    physical = _physical(binding)
    admission = _Admission()
    admission.route_sha256 = "2" * 64

    with pytest.raises(BootstrapHandoffError, match="route"):
        await consume_bootstrap_handoff(
            directory,
            lease.reference,
            physical,
            admission,
            now=lambda: _NOW,
        )


def test_handoff_reference_enters_launcher_argv_without_clear_secret(tmp_path: Path) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    lease = BootstrapHandoffStore(directory).prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )

    from loom_capacity_executor.launch_renderer import render_signed_launch

    rendered = launch_context_fixture()
    launch = render_signed_launch(rendered)
    launch_request = launch.request.model_copy(
        update={"bootstrap_handoff_reference": lease.reference}
    )
    arguments = launch_request.trusted_launcher_argv()
    clear_secret = (directory / lease.reference).read_text(encoding="utf-8")

    assert f"--bootstrap-handoff={lease.reference}" in arguments
    assert all(clear_secret not in argument for argument in arguments)
    assert all("postgres" not in argument.lower() for argument in arguments)
    assert canonical_executable_digest(binding) not in arguments
