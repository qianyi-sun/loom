from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import shlex
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest

import loom_capacity_executor.bootstrap_handoff as handoff_module
from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2, PhysicalJobBindingV2
from loom_capacity_executor.bootstrap_handoff import (
    BootstrapHandoffError,
    BootstrapHandoffLaunchV2,
    BootstrapHandoffRecordV2,
    BootstrapHandoffStore,
    bind_bootstrap_handoff_ownership,
    claim_bootstrap_handoff_launch,
    consume_bootstrap_handoff,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from tests.unit.test_capacity_executor_launch_renderer import launch_context_fixture

_NOW = datetime(2026, 8, 13, 16, 0, tzinfo=UTC)
_CANDIDATE_IMAGE = f"registry.example.com/loom/candidate@sha256:{'1' * 64}"
_OTHER_CANDIDATE_IMAGE = f"registry.example.com/loom/candidate@sha256:{'2' * 64}"


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


class _ExecBoundaryError(Exception):
    """Raised by tests when the trusted wrapper reaches the process exec boundary."""


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


# Production break caught: after the trusted wrapper reaches the candidate exec
# boundary, the persisted crash-recovery credential must be consumed and a later
# wrapper invocation must fail closed instead of launching the candidate again.
async def test_trusted_wrapper_exec_boundary_makes_handoff_non_reusable(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.trusted_launcher import (
        WORKER_CREDENTIAL_ENV,
        exec_bootstrap_handoff_candidate,
    )

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
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    def fake_execvpe(file: str, argv: tuple[str, ...], env: dict[str, str]) -> None:
        assert not (directory / lease.reference).exists()
        assert not (directory / lease.reference).with_suffix(".used").exists()
        assert not (directory / lease.reference).with_suffix(".credential").exists()
        assert admission.capabilities[0] not in "\n".join(
            item.read_text(encoding="utf-8") for item in directory.iterdir() if item.is_file()
        )
        exec_calls.append((file, argv, env))
        raise _ExecBoundaryError

    with pytest.raises(_ExecBoundaryError):
        await exec_bootstrap_handoff_candidate(
            directory,
            lease.reference,
            physical,
            admission,
            candidate_argv=("/opt/loom/bin/worker", "--once"),
            now=lambda: _NOW,
            environment={"LOOM_EXISTING": "1"},
            execvpe=fake_execvpe,
        )

    assert len(exec_calls) == 1
    assert exec_calls[0][0] == "/opt/loom/bin/worker"
    assert exec_calls[0][1] == ("/opt/loom/bin/worker", "--once")
    worker_credential = exec_calls[0][2][WORKER_CREDENTIAL_ENV]
    assert exec_calls[0][2]["LOOM_EXISTING"] == "1"
    assert (
        admission.requests[0].worker_credential_sha256
        == hashlib.sha256(worker_credential.encode("ascii")).hexdigest()
    )
    with pytest.raises(BootstrapHandoffError, match="already"):
        await consume_bootstrap_handoff(
            directory,
            lease.reference,
            physical,
            admission,
            now=lambda: _NOW,
        )


# Production break caught: if the trusted wrapper crashes after claiming the
# irreversible launch boundary but before execvpe transfers control, recovery
# must fail closed without minting another worker or revealing the credential.
async def test_trusted_wrapper_crash_after_launch_claim_is_irrecoverable_fail_closed(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.trusted_launcher import exec_bootstrap_handoff_candidate

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

    launched_credential = claim_bootstrap_handoff_launch(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )

    assert launched_credential == credential
    assert len(admission.requests) == 1
    assert not (directory / lease.reference).exists()
    assert not (directory / lease.reference).with_suffix(".used").exists()
    assert not (directory / lease.reference).with_suffix(".credential").exists()
    assert (directory / lease.reference).with_suffix(".launched").exists()
    with pytest.raises(BootstrapHandoffError, match="already"):
        await exec_bootstrap_handoff_candidate(
            directory,
            lease.reference,
            physical,
            admission,
            candidate_argv=("/opt/loom/bin/worker",),
            now=lambda: _NOW,
            environment={},
            execvpe=lambda *_args: (_ for _ in ()).throw(_ExecBoundaryError),
        )
    assert len(admission.requests) == 1


# Production break caught: a trusted-wrapper crash after protected registration
# but before candidate exec must recover the exact persisted worker credential
# without registering a second worker.
async def test_trusted_wrapper_recovers_successful_exchange_before_exec_boundary(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.trusted_launcher import (
        WORKER_CREDENTIAL_ENV,
        exec_bootstrap_handoff_candidate,
    )

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
    recovered: list[str] = []

    credential = await consume_bootstrap_handoff(
        directory,
        lease.reference,
        physical,
        admission,
        now=lambda: _NOW,
    )

    def fake_execvpe(_file: str, _argv: tuple[str, ...], env: dict[str, str]) -> None:
        recovered.append(env[WORKER_CREDENTIAL_ENV])
        raise _ExecBoundaryError

    with pytest.raises(_ExecBoundaryError):
        await exec_bootstrap_handoff_candidate(
            directory,
            lease.reference,
            physical,
            admission,
            candidate_argv=("/opt/loom/bin/worker",),
            now=lambda: _NOW,
            environment={},
            execvpe=fake_execvpe,
        )

    assert recovered == [credential]
    assert len(admission.requests) == 1
    assert len(admission.capabilities) == 1


# Production break caught: competing trusted-wrapper recoveries may replay the
# protected registration, but only one process may claim the candidate exec
# boundary and expose the worker credential to candidate code.
async def test_trusted_wrapper_concurrent_recovery_launches_candidate_once(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.trusted_launcher import (
        WORKER_CREDENTIAL_ENV,
        exec_bootstrap_handoff_candidate,
    )

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
    exec_credentials: list[str] = []

    def fake_execvpe(_file: str, _argv: tuple[str, ...], env: dict[str, str]) -> None:
        exec_credentials.append(env[WORKER_CREDENTIAL_ENV])
        raise _ExecBoundaryError

    async def run_one() -> str:
        try:
            await exec_bootstrap_handoff_candidate(
                directory,
                lease.reference,
                physical,
                admission,
                candidate_argv=("/opt/loom/bin/worker",),
                now=lambda: _NOW,
                environment={},
                execvpe=fake_execvpe,
            )
        except _ExecBoundaryError:
            return "exec"
        except BootstrapHandoffError as exc:
            return str(exc)
        raise AssertionError("candidate exec returned")

    results = await asyncio.gather(run_one(), run_one())

    assert results.count("exec") == 1
    assert exec_credentials == [credential]
    assert any("already" in result for result in results)


# Production break caught: the shipped trusted-launcher process entry must
# construct the physical binding and admission route from Slurm launch inputs
# instead of relying on a test-only helper to inject them.
async def test_trusted_launcher_process_entry_derives_physical_binding_from_slurm_inputs(
    tmp_path: Path,
) -> None:
    from dataclasses import replace

    from loom_capacity_executor.launch_renderer import (
        canonical_launch_policy_digest,
        render_signed_launch,
    )
    from loom_capacity_executor.slurm_contracts import (
        SlurmExecutableIdentityV2,
        SlurmFileIdentityV2,
    )
    from loom_capacity_executor.trusted_launcher import (
        WORKER_CREDENTIAL_ENV,
        TrustedLauncherConfigV2,
        run_trusted_launcher_process,
    )

    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    launcher_path = tmp_path / "trusted-launcher"
    launcher_path.write_text("#!/bin/sh\nexit 70\n", encoding="utf-8")
    launcher_path.chmod(0o755)
    candidate_path = tmp_path / "candidate-worker"
    _write_candidate(candidate_path)
    context = launch_context_fixture()
    trusted_config = TrustedLauncherConfigV2(
        handoff_directory=str(directory),
        admission_directory=str(admission_directory),
        admission_directory_sha256=_Admission.route_sha256,
        candidate_executable={
            "path": str(candidate_path),
            "sha256": hashlib.sha256(candidate_path.read_bytes()).hexdigest(),
            "owner_uid": candidate_path.stat().st_uid,
            "mode": candidate_path.stat().st_mode & 0o777,
        },
        candidate_image_digest=context.profile.image_digest,
        candidate_argv=(str(candidate_path), "--once"),
    )
    config_path = tmp_path / "trusted-launcher-config.json"
    config_path.write_bytes(canonical_executable_bytes(trusted_config))
    config_path.chmod(0o600)
    profile = context.profile.model_copy(
        update={
            "launcher": SlurmExecutableIdentityV2(
                path=str(launcher_path),
                sha256=hashlib.sha256(launcher_path.read_bytes()).hexdigest(),
                owner_uid=launcher_path.stat().st_uid,
            ),
            "trusted_launcher_config": SlurmFileIdentityV2(
                path=str(config_path),
                sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                owner_uid=config_path.stat().st_uid,
            ),
        }
    )
    profile = profile.model_copy(update={"controller_authority_sha256": "0" * 64})
    profile = profile.model_copy(
        update={"controller_authority_sha256": canonical_launch_policy_digest(profile)}
    )
    rendered = render_signed_launch(
        replace(
            context,
            profile=profile,
            controller_authority=context.controller_authority.model_copy(
                update={"controller_authority_sha256": profile.controller_authority_sha256}
            ),
        )
    )
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        context.binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=context.binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    bind_bootstrap_handoff_ownership(
        directory,
        lease.reference,
        context.binding,
        bootstrap_registration_epoch=1,
        ownership_evidence_sha256=canonical_executable_digest(rendered.ownership_proof),
        trusted_launcher_release_sha256=context.binding.execution.trusted_fleet_release_sha256,
        now=lambda: _NOW,
    )
    launch_request = rendered.request.model_copy(
        update={"bootstrap_handoff_reference": lease.reference}
    )
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    def admission_factory(directory_arg: Path, *, expected_directory_sha256: str) -> _Admission:
        assert directory_arg == admission_directory
        assert expected_directory_sha256 == _Admission.route_sha256
        return admission

    def fake_execvpe(file: str, argv: tuple[str, ...], env: dict[str, str]) -> None:
        exec_calls.append((file, argv, env))
        raise _ExecBoundaryError

    with pytest.raises(_ExecBoundaryError):
        await run_trusted_launcher_process(
            launch_request.trusted_launcher_argv(),
            environment={"SLURM_JOB_ID": "101"},
            now=lambda: _NOW,
            admission_factory=admission_factory,
            execvpe=fake_execvpe,
        )

    assert len(exec_calls) == 1
    assert exec_calls[0][0].startswith("/proc/self/fd/")
    assert exec_calls[0][1] == (str(candidate_path), "--once")
    worker_credential = exec_calls[0][2][WORKER_CREDENTIAL_ENV]
    assert (
        admission.requests[0].worker_credential_sha256
        == hashlib.sha256(worker_credential.encode("ascii")).hexdigest()
    )
    assert admission.requests[0].binding == context.binding
    assert admission.requests[0].slurm_job_id == "101"
    launch = BootstrapHandoffLaunchV2.model_validate_json(
        (directory / lease.reference).with_suffix(".launched").read_bytes()
    )
    assert launch.physical.binding == context.binding
    assert launch.physical.slurm_job_id == "101"
    assert launch.physical.ownership_evidence_sha256 == canonical_executable_digest(
        rendered.ownership_proof
    )


# Production break caught: the wrapper must not accept an arbitrary changed
# Slurm ownership token and derive a different local physical binding; the
# expected signed ownership evidence digest is pinned before registration.
def test_handoff_physical_resolution_rejects_changed_ownership_token(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.bootstrap_handoff import (
        bind_bootstrap_handoff_ownership,
        resolve_bootstrap_handoff_physical_binding,
    )
    from loom_capacity_executor.launch_renderer import render_signed_launch

    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    context = launch_context_fixture()
    rendered = render_signed_launch(context)
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        context.binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=context.binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    bind_bootstrap_handoff_ownership(
        directory,
        lease.reference,
        context.binding,
        bootstrap_registration_epoch=1,
        ownership_evidence_sha256=canonical_executable_digest(rendered.ownership_proof),
        trusted_launcher_release_sha256=context.binding.execution.trusted_fleet_release_sha256,
        now=lambda: _NOW,
    )
    wrong_token = base64.urlsafe_b64encode(b"\x00" * 32).rstrip(b"=").decode("ascii")

    with pytest.raises(BootstrapHandoffError, match="ownership"):
        resolve_bootstrap_handoff_physical_binding(
            directory,
            lease.reference,
            operation_id=context.binding.intent_id,
            slurm_job_id="101",
            ownership_token=wrong_token,
            trusted_launcher_release_sha256=context.binding.execution.trusted_fleet_release_sha256,
            now=lambda: _NOW,
        )


def _write_candidate(path: Path, payload: bytes = b"#!/bin/sh\nexit 0\n") -> bytes:
    path.write_bytes(payload)
    path.chmod(0o555)
    return payload


def _trusted_candidate_config_payload(
    *,
    handoff_directory: Path,
    admission_directory: Path,
    candidate_path: Path,
    candidate_sha256: str | None = None,
    candidate_owner_uid: int | None = None,
    candidate_mode: int | None = None,
    candidate_image_digest: str = _CANDIDATE_IMAGE,
) -> dict[str, object]:
    metadata = candidate_path.stat()
    return {
        "schema_version": 2,
        "handoff_directory": str(handoff_directory),
        "admission_directory": str(admission_directory),
        "admission_directory_sha256": _Admission.route_sha256,
        "candidate_argv": (str(candidate_path), "--once"),
        "candidate_executable": {
            "schema_version": 2,
            "path": str(candidate_path),
            "sha256": candidate_sha256 or hashlib.sha256(candidate_path.read_bytes()).hexdigest(),
            "owner_uid": (
                candidate_owner_uid if candidate_owner_uid is not None else metadata.st_uid
            ),
            "mode": (candidate_mode if candidate_mode is not None else metadata.st_mode & 0o777),
        },
        "candidate_image_digest": candidate_image_digest,
    }


def _trusted_launcher_process_argv_for_candidate_config(
    tmp_path: Path,
    *,
    config_payload: dict[str, object],
    process_image_digest: str = _CANDIDATE_IMAGE,
) -> tuple[str, ...]:
    from dataclasses import replace

    from loom_capacity_executor.launch_renderer import (
        canonical_launch_policy_digest,
        render_signed_launch,
    )
    from loom_capacity_executor.slurm_contracts import (
        SlurmExecutableIdentityV2,
        SlurmFileIdentityV2,
    )

    directory = Path(config_payload["handoff_directory"])
    launcher_path = tmp_path / "trusted-launcher"
    launcher_path.write_text("#!/bin/sh\nexit 70\n", encoding="utf-8")
    launcher_path.chmod(0o755)
    config_path = tmp_path / "trusted-launcher-config.json"
    config_path.write_bytes(
        json.dumps(
            config_payload,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("ascii")
    )
    config_path.chmod(0o600)

    context = launch_context_fixture()
    profile = context.profile.model_copy(
        update={
            "launcher": SlurmExecutableIdentityV2(
                path=str(launcher_path),
                sha256=hashlib.sha256(launcher_path.read_bytes()).hexdigest(),
                owner_uid=launcher_path.stat().st_uid,
            ),
            "trusted_launcher_config": SlurmFileIdentityV2(
                path=str(config_path),
                sha256=hashlib.sha256(config_path.read_bytes()).hexdigest(),
                owner_uid=config_path.stat().st_uid,
            ),
        }
    )
    profile = profile.model_copy(update={"controller_authority_sha256": "0" * 64})
    profile = profile.model_copy(
        update={"controller_authority_sha256": canonical_launch_policy_digest(profile)}
    )
    rendered = render_signed_launch(
        replace(
            context,
            profile=profile,
            controller_authority=context.controller_authority.model_copy(
                update={"controller_authority_sha256": profile.controller_authority_sha256}
            ),
        )
    )
    store = BootstrapHandoffStore(directory)
    lease = store.prepare(
        context.binding,
        bootstrap_registration_epoch=1,
        expires_at=_NOW + timedelta(minutes=5),
        trusted_launcher_release_sha256=context.binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )
    bind_bootstrap_handoff_ownership(
        directory,
        lease.reference,
        context.binding,
        bootstrap_registration_epoch=1,
        ownership_evidence_sha256=canonical_executable_digest(rendered.ownership_proof),
        trusted_launcher_release_sha256=context.binding.execution.trusted_fleet_release_sha256,
        now=lambda: _NOW,
    )
    launch_request = rendered.request.model_copy(
        update={
            "bootstrap_handoff_reference": lease.reference,
            "image_digest": process_image_digest,
        }
    )
    return launch_request.trusted_launcher_argv()


async def _run_trusted_process_with_candidate_config(
    tmp_path: Path,
    *,
    config_payload: dict[str, object],
    process_image_digest: str = _CANDIDATE_IMAGE,
    on_exec: Callable[[str, tuple[str, ...], dict[str, str]], None] | None = None,
    admission: _Admission | None = None,
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] | None = None,
) -> tuple[_Admission, list[tuple[str, tuple[str, ...], dict[str, str]]]]:
    from loom_capacity_executor.trusted_launcher import run_trusted_launcher_process

    launch_argv = _trusted_launcher_process_argv_for_candidate_config(
        tmp_path,
        config_payload=config_payload,
        process_image_digest=process_image_digest,
    )
    admission_directory = Path(config_payload["admission_directory"])
    admission = admission or _Admission()
    exec_calls = exec_calls if exec_calls is not None else []

    def admission_factory(directory_arg: Path, *, expected_directory_sha256: str) -> _Admission:
        assert directory_arg == admission_directory
        assert expected_directory_sha256 == _Admission.route_sha256
        return admission

    def fake_execvpe(file: str, argv: tuple[str, ...], env: dict[str, str]) -> None:
        if on_exec is not None:
            on_exec(file, argv, env)
        exec_calls.append((file, argv, env))
        raise _ExecBoundaryError

    await run_trusted_launcher_process(
        launch_argv,
        environment={"SLURM_JOB_ID": "101"},
        now=lambda: _NOW,
        admission_factory=admission_factory,
        execvpe=fake_execvpe,
    )
    raise AssertionError("trusted process returned without exec")


# Production break caught: a candidate path with changed bytes must be rejected
# before the wrapper exchanges the one-time capability or exposes a credential.
async def test_trusted_launcher_rejects_wrong_candidate_hash_before_credential(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "candidate-worker"
    _write_candidate(candidate)
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
        candidate_sha256="0" * 64,
    )
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    with pytest.raises(BootstrapHandoffError, match="candidate executable digest"):
        await _run_trusted_process_with_candidate_config(
            tmp_path,
            config_payload=config,
            admission=admission,
            exec_calls=exec_calls,
        )
    assert admission.requests == []
    assert admission.capabilities == []
    assert exec_calls == []


# Production break caught: a candidate executable owned by a different UID than
# the pinned config identity must not receive the scoped worker credential.
async def test_trusted_launcher_rejects_wrong_candidate_owner_before_credential(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "candidate-worker"
    _write_candidate(candidate)
    wrong_owner = os.geteuid() + 1 if os.geteuid() < (1 << 31) - 1 else 0
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
        candidate_owner_uid=wrong_owner,
    )
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    with pytest.raises(BootstrapHandoffError, match="candidate executable identity"):
        await _run_trusted_process_with_candidate_config(
            tmp_path,
            config_payload=config,
            admission=admission,
            exec_calls=exec_calls,
        )
    assert admission.requests == []
    assert admission.capabilities == []
    assert exec_calls == []


# Production break caught: if the candidate is owned by the trusted-wrapper UID,
# a writable mode lets same-UID candidate code mutate the verified inode before
# exec; reject it before protected registration/credential issue.
async def test_trusted_launcher_rejects_current_uid_writable_candidate_mode(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "candidate-worker"
    _write_candidate(candidate)
    candidate.chmod(0o755)
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
    )
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    with pytest.raises(BootstrapHandoffError, match="candidate executable mode"):
        await _run_trusted_process_with_candidate_config(
            tmp_path,
            config_payload=config,
            admission=admission,
            exec_calls=exec_calls,
        )
    assert admission.requests == []
    assert admission.capabilities == []
    assert exec_calls == []


# Production break caught: image identity belongs to the trusted wrapper
# boundary; a Slurm argv image different from the config-pinned image must be
# rejected before the handoff is consumed.
async def test_trusted_launcher_rejects_image_digest_mismatch_before_credential(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "candidate-worker"
    _write_candidate(candidate)
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
        candidate_image_digest=_CANDIDATE_IMAGE,
    )
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    with pytest.raises(BootstrapHandoffError, match="image digest"):
        await _run_trusted_process_with_candidate_config(
            tmp_path,
            config_payload=config,
            process_image_digest=_OTHER_CANDIDATE_IMAGE,
            admission=admission,
            exec_calls=exec_calls,
        )
    assert admission.requests == []
    assert admission.capabilities == []
    assert exec_calls == []


# Production break caught: replacement of the configured candidate pathname
# before wrapper startup must be detected before registration/credential issue.
async def test_trusted_launcher_rejects_replaced_candidate_before_credential(
    tmp_path: Path,
) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "candidate-worker"
    _write_candidate(candidate, b"#!/bin/sh\nexit 0\n")
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
    )
    candidate.unlink()
    _write_candidate(candidate, b"#!/bin/sh\nexit 99\n")
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    with pytest.raises(BootstrapHandoffError, match="candidate executable digest"):
        await _run_trusted_process_with_candidate_config(
            tmp_path,
            config_payload=config,
            admission=admission,
            exec_calls=exec_calls,
        )
    assert admission.requests == []
    assert admission.capabilities == []
    assert exec_calls == []


# Production break caught: after successful authentication, the candidate exec
# target must be the already-open verified object, not a pathname that attacker
# code can replace between verification and exec.
async def test_trusted_launcher_executes_already_open_verified_candidate(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.trusted_launcher import WORKER_CREDENTIAL_ENV

    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "candidate-worker"
    original_payload = _write_candidate(candidate, b"#!/bin/sh\nexit 0\n")
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
    )
    observed: list[tuple[str, tuple[str, ...], str]] = []
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []

    def replace_path_before_exec(file: str, argv: tuple[str, ...], env: dict[str, str]) -> None:
        candidate.unlink()
        _write_candidate(candidate, b"#!/bin/sh\nexit 99\n")
        assert file.startswith("/proc/self/fd/")
        assert Path(file).read_bytes() == original_payload
        observed.append((file, argv, env[WORKER_CREDENTIAL_ENV]))

    with pytest.raises(_ExecBoundaryError):
        await _run_trusted_process_with_candidate_config(
            tmp_path,
            config_payload=config,
            on_exec=replace_path_before_exec,
            admission=admission,
            exec_calls=exec_calls,
        )

    assert len(exec_calls) == 1
    assert observed[0][1] == (str(candidate), "--once")
    assert (
        admission.requests[0].worker_credential_sha256
        == hashlib.sha256(observed[0][2].encode("ascii")).hexdigest()
    )


# Production break caught: a current-UID-owned 0555 candidate can be chmodded
# and rewritten in-place after authentication; the descriptor handed to exec
# must still expose only the exact preverified bytes.
def test_trusted_launcher_verified_candidate_descriptor_is_immutable_after_same_inode_rewrite(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.trusted_launcher import (
        TrustedCandidateExecutableV2,
        _open_verified_candidate,
    )

    candidate = tmp_path / "candidate-worker"
    original_payload = _write_candidate(candidate, b"#!/bin/sh\nprintf 'original\\n'\n")
    identity = TrustedCandidateExecutableV2(
        path=str(candidate),
        sha256=hashlib.sha256(original_payload).hexdigest(),
        owner_uid=candidate.stat().st_uid,
        mode=candidate.stat().st_mode & 0o777,
    )

    descriptor = _open_verified_candidate(identity)
    try:
        candidate.chmod(0o755)
        candidate.write_bytes(b"#!/bin/sh\nprintf 'mutated\\n'\n")
        candidate.chmod(0o555)
        os.lseek(descriptor, 0, os.SEEK_SET)
        observed_payload = os.read(descriptor, 4096)
    finally:
        os.close(descriptor)

    assert observed_payload == original_payload


# Production break caught: if a same-UID process rewrites the unsealed memfd
# snapshot after the initial source copy/hash but before seals are added, the
# wrapper must independently verify the immutable sealed bytes before protected
# registration, credential exposure, or candidate exec.
async def test_trusted_launcher_rejects_memfd_mutation_between_copy_hash_and_seal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import loom_capacity_executor.trusted_launcher as trusted_launcher

    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    candidate = tmp_path / "candidate-worker"
    original_payload = _write_candidate(candidate, b"#!/bin/sh\nprintf 'original\\n'\n")
    mutated_payload = b"#!/bin/sh\nprintf 'mutated!\\n'\n"
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
    )
    admission = _Admission()
    exec_calls: list[tuple[str, tuple[str, ...], dict[str, str]]] = []
    touched_descriptors: list[int] = []
    real_seal = trusted_launcher._seal_candidate_snapshot

    def mutate_then_seal(descriptor: int) -> None:
        touched_descriptors.append(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.write(descriptor, mutated_payload)
        os.ftruncate(descriptor, len(mutated_payload))
        os.lseek(descriptor, 0, os.SEEK_SET)
        real_seal(descriptor)

    monkeypatch.setattr(trusted_launcher, "_seal_candidate_snapshot", mutate_then_seal)

    with pytest.raises(BootstrapHandoffError, match="candidate executable digest"):
        await _run_trusted_process_with_candidate_config(
            tmp_path,
            config_payload=config,
            admission=admission,
            exec_calls=exec_calls,
        )

    assert hashlib.sha256(original_payload).hexdigest() == config["candidate_executable"]["sha256"]
    assert touched_descriptors
    with pytest.raises(OSError):
        os.fstat(touched_descriptors[0])
    assert admission.requests == []
    assert admission.capabilities == []
    assert exec_calls == []
    assert not tuple(directory.glob("*.used"))
    assert not tuple(directory.glob("*.credential"))
    assert not tuple(directory.glob("*.launched"))


# Production break caught: the real exec boundary for shebang candidates must
# keep the authenticated descriptor available after exec so the interpreter can
# reopen /proc/self/fd/<fd>; fake execvpe tests cannot observe this.
def test_trusted_launcher_real_exec_supports_shebang_candidate_descriptor(
    tmp_path: Path,
) -> None:
    from loom_capacity_executor.trusted_launcher import run_trusted_launcher_process

    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    admission_directory = tmp_path / "admission"
    admission_directory.mkdir(mode=0o700)
    output = tmp_path / "candidate-output.txt"
    child_error = tmp_path / "child-error.txt"
    candidate = tmp_path / "candidate-worker"
    script = (
        "#!/bin/sh\n"
        'if [ -n "$LOOM_EXECUTOR_WORKER_CREDENTIAL" ]; then\n'
        "  credential_state=present\n"
        "else\n"
        "  credential_state=missing\n"
        "fi\n"
        f"printf 'payload=original\\narg1=%s\\ncredential=%s\\n' "
        f'"$1" "$credential_state" > {shlex.quote(str(output))}\n'
    ).encode()
    _write_candidate(candidate, script)
    config = _trusted_candidate_config_payload(
        handoff_directory=directory,
        admission_directory=admission_directory,
        candidate_path=candidate,
    )
    launch_argv = _trusted_launcher_process_argv_for_candidate_config(
        tmp_path,
        config_payload=config,
    )

    def admission_factory(directory_arg: Path, *, expected_directory_sha256: str) -> _Admission:
        assert directory_arg == admission_directory
        assert expected_directory_sha256 == _Admission.route_sha256
        return _Admission()

    pid = os.fork()
    if pid == 0:
        try:
            asyncio.run(
                run_trusted_launcher_process(
                    launch_argv,
                    environment={"SLURM_JOB_ID": "101"},
                    now=lambda: _NOW,
                    admission_factory=admission_factory,
                    execvpe=os.execvpe,
                )
            )
        except BaseException as exc:
            child_error.write_text(f"{type(exc).__name__}: {exc}", encoding="utf-8")
            os._exit(125)
        os._exit(126)

    _waited_pid, status = os.waitpid(pid, 0)
    assert os.WIFEXITED(status), status
    exit_code = os.WEXITSTATUS(status)
    error_message = (
        child_error.read_text(encoding="utf-8")
        if child_error.exists()
        else "candidate process did not write a Python exception"
    )
    assert exit_code == 0, error_message
    assert output.read_text(encoding="utf-8") == (
        "payload=original\narg1=--once\ncredential=present\n"
    )


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


# Production break caught: an existing handoff record is reusable only for the
# exact requested UTC-normalized expiry; otherwise a stale long-lived capability
# can be silently rebound to a new launch request.
def test_prepare_rejects_existing_record_when_expiry_changes(tmp_path: Path) -> None:
    directory = tmp_path / "handoff"
    directory.mkdir(mode=0o700)
    binding = launch_context_fixture().binding
    store = BootstrapHandoffStore(directory)
    original_expiry = _NOW + timedelta(minutes=5)
    changed_expiry = _NOW + timedelta(minutes=10)

    store.prepare(
        binding,
        bootstrap_registration_epoch=1,
        expires_at=original_expiry,
        trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
        protected_admission_route_sha256=_Admission.route_sha256,
    )

    with pytest.raises(BootstrapHandoffError, match="expiry"):
        store.prepare(
            binding,
            bootstrap_registration_epoch=1,
            expires_at=changed_expiry,
            trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
            protected_admission_route_sha256=_Admission.route_sha256,
        )


# Production break caught: after protected prepared revocation commits, the
# clear local handoff capability must be physically removed and replaying that
# deletion must be idempotent.
def test_revoke_prepared_removes_only_the_exact_unconsumed_handoff(
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
    path = directory / lease.reference

    assert store.revoke_prepared(binding, bootstrap_registration_epoch=1) is True

    assert not path.exists()
    assert store.revoke_prepared(binding, bootstrap_registration_epoch=1) is False


# Production break caught: a stale or replaced handoff record at the expected
# reference must fail closed instead of deleting evidence for a different
# protected bootstrap epoch.
def test_revoke_prepared_rejects_changed_handoff_binding(tmp_path: Path) -> None:
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
    path = directory / lease.reference
    changed = BootstrapHandoffRecordV2.model_validate_json(path.read_bytes()).model_copy(
        update={"bootstrap_registration_epoch": 2}
    )
    path.write_bytes(canonical_executable_bytes(changed))
    path.chmod(0o600)

    with pytest.raises(BootstrapHandoffError, match="binding changed"):
        store.revoke_prepared(binding, bootstrap_registration_epoch=1)

    assert path.exists()


# Production break caught: sidecar evidence means a worker, physical binding, or
# launch handoff may already exist, so local prepared revocation must preserve
# the capability and fail closed.
@pytest.mark.parametrize("suffix", (".used", ".credential", ".ownership", ".launched"))
def test_revoke_prepared_rejects_consumed_or_physical_handoff_evidence(
    tmp_path: Path,
    suffix: str,
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
    path = directory / lease.reference
    sidecar = path.with_suffix(suffix)
    sidecar.write_bytes(b"evidence")
    sidecar.chmod(0o600)

    with pytest.raises(BootstrapHandoffError, match="physical or consumed"):
        store.revoke_prepared(binding, bootstrap_registration_epoch=1)

    assert path.exists()


# Production break caught: symlink sidecar evidence must be treated as evidence,
# not as an absent or unreadable regular file that permits deletion.
def test_revoke_prepared_rejects_symlink_consumed_evidence(tmp_path: Path) -> None:
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
    path = directory / lease.reference
    target = tmp_path / "target"
    target.write_bytes(b"evidence")
    path.with_suffix(".used").symlink_to(target)

    with pytest.raises(BootstrapHandoffError, match="physical or consumed"):
        store.revoke_prepared(binding, bootstrap_registration_epoch=1)

    assert path.exists()


# Production break caught: a concurrently published winner must match the same
# requested expiry as the losing preparer, not just the same binding/profile.
def test_prepare_rejects_concurrent_record_when_expiry_changes(
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

    with pytest.raises(BootstrapHandoffError, match="expiry"):
        store.prepare(
            binding,
            bootstrap_registration_epoch=1,
            expires_at=_NOW + timedelta(minutes=10),
            trusted_launcher_release_sha256=binding.execution.trusted_fleet_release_sha256,
            protected_admission_route_sha256=_Admission.route_sha256,
        )


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
