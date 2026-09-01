"""Service-only detached worker for one finalized staging attempt."""

from __future__ import annotations

import argparse
import json
import os
import pwd
import re
import signal
import subprocess
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Never, Protocol, TextIO

from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.failure_authority import RolloutFailureEvidence
from loom_cli.rollout.final_attestation_admission import (
    FinalAttestationAdmission,
    FinalAttestationAdmissionError,
)
from loom_cli.rollout.lifecycle_protocol import LifecycleAction, LifecyclePhase
from loom_cli.rollout.preflight_attestation_store import PreflightAttestationStore
from loom_cli.rollout.preflight_contract import (
    CheckOperation,
    PreflightAttestation,
    StageCapability,
)
from loom_cli.rollout.preflight_pipeline import PreflightAssessmentDriftError

from .backup import BackupCreator, BackupError, backup_public_reason_for_code
from .backup_job import (
    BackupJobState,
    PreflightBackupJobEnvelope,
    transition_backup_job,
)
from .backup_retirement import BackupPayloadActivator
from .config import OperatorConfig
from .envelope import (
    fixed_operator_config_path,
    load_validated_envelope,
    load_validated_envelope_with_config,
)
from .failure_diagnostics import unclassified_failure_diagnostic
from .final_admission_store import FinalAdmissionStore
from .final_gate_store import FinalGateExecutionStore
from .installed_backup_retention import converge_verified_backup_candidate
from .lifecycle import LifecycleCoordinator
from .model import (
    ActivePointer,
    CandidateBinding,
    DriverEnvelope,
    PreflightRequest,
    RequestEvent,
    RolloutRequest,
    validate_safe_identifier,
)
from .policy import sanitized_child_environment
from .protected_apply_recovery import find_advanced_epoch_attempt
from .redaction import redact_rollout_text
from .resume_runtime_upgrade import (
    AdmittedResumeRuntimeUpgrade,
    build_installed_resume_runtime_upgrade_authority,
)
from .staging_mutation_guard import MutationGuardManager
from .store import RequestStore
from .systemd import MUTATION_GUARD_CLIENT_OPERATION_TIMEOUT_SECONDS, SystemdUserManager


class _ArgumentError(ValueError):
    pass


class _CancellationSignal(BaseException):
    pass


_FAILURE_CODE_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,95}$")


def _backup_failure_code(error: BaseException) -> str:
    """Classify only explicitly secret-safe backup failure identities."""
    if isinstance(error, PreflightAssessmentDriftError):
        return "preflight_assessment_drift"
    if isinstance(error, BackupError) and _FAILURE_CODE_RE.fullmatch(error.code) is not None:
        return error.code
    return "backup_failed"


def _backup_failure_diagnostic_text(error: BaseException) -> str | None:
    """Secret-safe diagnostic text for a sealed backup failure.

    A ``BackupError`` carries a curated, already-secret-safe ``diagnostic``;
    ``PreflightAssessmentDriftError`` carries only a validated check ID and a
    fixed reason enum. An unanticipated failure previously produced no
    diagnostic at all, so the worker collapsed it to a bare ``backup_failed``
    with no operator-visible reason (#924) — leaving the real cause
    unrecoverable.

    An arbitrary exception's *message* cannot be assumed secret-safe (only
    best-effort redaction applies), so we deliberately do not surface it here;
    failure paths with known-safe context must raise ``BackupError`` with a
    curated code+diagnostic instead. What we can always surface safely is the
    exception *type name* and the raise-site *code location* (file, line, and
    function) — neither carries runtime values or secrets — which is enough to
    attribute a generic ``backup_failed`` to a specific point in the source and
    distinguish an unclassified crash from a handled backup outcome.
    """
    if isinstance(error, PreflightAssessmentDriftError):
        return (
            "pre-backup assessment evidence drifted: "
            f"check_id={error.check_id} reason={error.reason.value}"
        )
    if isinstance(error, BackupError):
        return error.diagnostic
    return unclassified_failure_diagnostic(error, activity="backup")


def _write_backup_failure_diagnostic(
    dependencies: WorkerDependencies,
    *,
    request: PreflightRequest,
    envelope: PreflightBackupJobEnvelope,
    failure_code: str,
    error: BaseException,
) -> None:
    diagnostic = _backup_failure_diagnostic_text(error)
    if diagnostic is None:
        return
    safe_diagnostic = redact_rollout_text(diagnostic, limit=8 * 1024)
    try:
        dependencies.stderr.write(
            json.dumps(
                {
                    "diagnostic": safe_diagnostic,
                    "failure_code": failure_code,
                    "job_id": envelope.job_id,
                    "request_id": request.request_id,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        )
    except Exception:
        pass


def _append_backup_failed_event(
    dependencies: WorkerDependencies,
    *,
    request: PreflightRequest,
    reason: str,
) -> None:
    dependencies.store.append_event(
        RequestEvent(
            request_id=request.request_id,
            event="backup_failed",
            occurred_at=dependencies.now(),
            operator=request.caller.username,
            operator_uid=request.caller.uid,
            unit_name=f"loom-staging-backup-{request.request_id}.service",
            status="failed",
            reason=reason,
        )
    )


class _Parser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)

    def error(self, message: str) -> Never:
        raise _ArgumentError(message)


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="loom-staging-rollout-worker")
    commands = parser.add_subparsers(dest="command", required=True)
    attempt = commands.add_parser("run-attempt")
    attempt.add_argument("--envelope", type=Path, required=True)
    backup = commands.add_parser("run-backup")
    backup.add_argument("--job", type=Path, required=True)
    return parser


def _worker_path_request_id(
    path: Path,
    *,
    state_root: Path | None,
    command: str,
) -> str:
    if (
        state_root is None
        or not state_root.is_absolute()
        or ".." in state_root.parts
        or not path.is_absolute()
        or ".." in path.parts
    ):
        raise ValueError("worker immutable path authority is invalid")
    try:
        relative = path.relative_to(state_root)
    except ValueError as exc:
        raise ValueError("worker immutable path authority is invalid") from exc
    if command == "run-attempt":
        if (
            len(relative.parts) != 5
            or relative.parts[0] != "requests"
            or relative.parts[2] != "attempts"
            or relative.parts[4] != "envelope.json"
            or not relative.parts[3].isascii()
            or not relative.parts[3].isdecimal()
            or relative.parts[3].startswith("0")
        ):
            raise ValueError("worker immutable attempt path is invalid")
        request_id = relative.parts[1]
    elif command == "run-backup":
        if (
            len(relative.parts) != 4
            or relative.parts[0] != "requests"
            or relative.parts[2:] != ("preflight-backup", "job.json")
        ):
            raise ValueError("worker immutable backup path is invalid")
        request_id = relative.parts[1]
    else:
        raise ValueError("worker command identity is invalid")
    return validate_safe_identifier(request_id, "request_id")


@dataclass(frozen=True, slots=True)
class VerifiedBackupJob:
    manifest_path: Path
    manifest_sha256: str
    lease_digest: str
    preflight_attestation_sha256: str

    def __post_init__(self) -> None:
        if (
            not self.manifest_path.is_absolute()
            or ".." in self.manifest_path.parts
            or len(self.manifest_sha256) != 64
            or len(self.lease_digest) != 64
            or any(
                character not in "0123456789abcdef"
                for character in (
                    self.manifest_sha256 + self.lease_digest + self.preflight_attestation_sha256
                )
            )
            or len(self.preflight_attestation_sha256) != 64
        ):
            raise ValueError("verified backup job identity is invalid")


@dataclass(slots=True)
class WorkerDependencies:
    store: Any
    lifecycle: Any
    run_driver: Callable[[Path, bool], int]
    now: Callable[[], str]
    stderr: TextIO
    state_root: Path | None = None
    mutation_guard: Any | None = None
    envelope_path: Callable[[DriverEnvelope], Path] | None = None
    load_envelope: Callable[[Path], DriverEnvelope] | None = None
    load_backup_job: Callable[[Path], PreflightBackupJobEnvelope] | None = None
    run_backup: (
        Callable[
            [PreflightRequest, PreflightBackupJobEnvelope, Callable[[], bool]],
            VerifiedBackupJob,
        ]
        | None
    ) = None
    finalize_backup: Callable[[PreflightRequest, VerifiedBackupJob], DriverEnvelope] | None = None
    reconcile_verified_backup: (
        Callable[[PreflightBackupJobEnvelope, VerifiedBackupJob], object] | None
    ) = None
    read_driver_failure: Callable[[DriverEnvelope], RolloutFailureEvidence | None] | None = None
    final_admission: Callable[[DriverEnvelope], FinalAttestationAdmission] | None = None
    run_final_gates: Callable[[DriverEnvelope, FinalAttestationAdmission], int] | None = None


def _mutation_guard(dependencies: WorkerDependencies) -> Any:
    if dependencies.mutation_guard is None:
        raise ValueError("staging mutation guard is unavailable")
    return dependencies.mutation_guard


def _assert_mutation_guard_ready(
    dependencies: WorkerDependencies,
    *,
    request_id: str,
    candidate_sha: str,
    candidate_tree: str | None = None,
    mutation_epoch: int | None = None,
) -> Any:
    evidence = _mutation_guard(dependencies).assert_ready(request_id)
    if (
        evidence.request_id != request_id
        or evidence.candidate_sha != candidate_sha
        or evidence.state != "ready"
        or (candidate_tree is not None and evidence.candidate_tree != candidate_tree)
        or (mutation_epoch is not None and evidence.mutation_epoch != mutation_epoch)
    ):
        raise ValueError("staging mutation guard binding drifted")
    return evidence


def _release_mutation_guard(dependencies: WorkerDependencies, request_id: str) -> None:
    evidence = _mutation_guard(dependencies).release(request_id)
    if evidence.request_id != request_id or evidence.state != "released":
        raise ValueError("staging mutation guard release drifted")


class _FinalAdmissionAuthority(Protocol):
    def admit_final(
        self,
        candidate: CandidateBinding,
        *,
        attestation_digest: str,
        expected_registry_digest: str,
        expected_coverage_digest: str,
    ) -> FinalAttestationAdmission: ...

    def admit_post_apply_resume(
        self,
        candidate: CandidateBinding,
        *,
        prior_admission: FinalAttestationAdmission,
        attestation_digest: str,
        expected_registry_digest: str,
        expected_coverage_digest: str,
    ) -> FinalAttestationAdmission: ...


class _AttestationReader(Protocol):
    def read(self, digest: str) -> PreflightAttestation: ...


def _admit_final_attempt(
    envelope: DriverEnvelope,
    *,
    deep_preflight: _FinalAdmissionAuthority,
    attestation_store: _AttestationReader,
    state_root: Path,
    service_uid: int,
) -> FinalAttestationAdmission:
    """Persist initial admission or re-admit one proven post-apply resume."""
    candidate = CandidateBinding(
        remote_url=envelope.remote_url,
        target_ref=envelope.target_ref,
        resolved_sha=envelope.resolved_sha,
        image_tag=envelope.image_tag,
        fetched_at=envelope.fetched_at,
        source_mode=envelope.source_mode,
        resolved_tree=envelope.resolved_tree,
        approved_base_sha=envelope.approved_base_sha,
    )
    attestation = attestation_store.read(envelope.preflight_attestation_sha256)
    current_store = FinalAdmissionStore(
        state_root,
        request_id=envelope.request_id,
        attempt_number=envelope.attempt_number,
        service_uid=service_uid,
    )
    prior_admission: FinalAttestationAdmission | None = None
    recovery_attempts = [envelope.attempt_number]
    if envelope.resume:
        recovery_attempts.extend(range(envelope.attempt_number - 1, 0, -1))
    for attempt_number in recovery_attempts:
        executions = FinalGateExecutionStore(
            state_root,
            request_id=envelope.request_id,
            attempt_number=attempt_number,
            service_uid=service_uid,
        ).read_all()
        protected_apply = executions.get("final.protected-apply")
        if protected_apply is None or not protected_apply.passed:
            continue
        evidence = protected_apply.evidence
        if (
            protected_apply.tier != 4
            or protected_apply.stage is not StageCapability.FINAL_ONLY
            or protected_apply.operation is not CheckOperation.APPLY
            or evidence.get("ready") is not True
            or evidence.get("candidate-sha") != envelope.resolved_sha
            or evidence.get("attestation-digest") != envelope.preflight_attestation_sha256
            or evidence.get("observed-epoch") != attestation.bindings.staging_mutation_epoch + 1
            or evidence.get("protected-mutation") is not True
            or evidence.get("blockers") != {}
        ):
            raise ValueError("prior protected apply evidence drifted")
        prior_admission = FinalAdmissionStore(
            state_root,
            request_id=envelope.request_id,
            attempt_number=attempt_number,
            service_uid=service_uid,
        ).read(attestation)
        break
    if prior_admission is None and envelope.resume:
        recovery_attempt = find_advanced_epoch_attempt(
            state_root,
            request_id=envelope.request_id,
            through_attempt=envelope.attempt_number - 1,
            candidate_sha=envelope.resolved_sha,
            attestation_digest=envelope.preflight_attestation_sha256,
            starting_mutation_epoch=attestation.bindings.staging_mutation_epoch,
            service_uid=service_uid,
        )
        if recovery_attempt is not None:
            prior_admission = FinalAdmissionStore(
                state_root,
                request_id=envelope.request_id,
                attempt_number=recovery_attempt,
                service_uid=service_uid,
            ).read(attestation)
    if prior_admission is None:
        admission = deep_preflight.admit_final(
            candidate,
            attestation_digest=envelope.preflight_attestation_sha256,
            expected_registry_digest=envelope.preflight_registry_sha256,
            expected_coverage_digest=envelope.preflight_coverage_sha256,
        )
    else:
        admission = deep_preflight.admit_post_apply_resume(
            candidate,
            prior_admission=prior_admission,
            attestation_digest=envelope.preflight_attestation_sha256,
            expected_registry_digest=envelope.preflight_registry_sha256,
            expected_coverage_digest=envelope.preflight_coverage_sha256,
        )
    current_store.publish(admission)
    return admission


@dataclass(slots=True)
class _SignalController:
    requested: bool = False
    driver_interruptible: bool = False
    sealed: bool = False

    def handle(self, signum: int, frame: object) -> None:
        del signum, frame
        if self.sealed:
            return
        self.requested = True
        if self.driver_interruptible:
            raise _CancellationSignal

    @contextmanager
    def driver_window(self) -> Iterator[None]:
        self.driver_interruptible = True
        try:
            if self.requested:
                raise _CancellationSignal
            yield
        finally:
            self.driver_interruptible = False

    def seal_terminal(self, *, event_cancelled: bool) -> bool:
        cancelled = self.requested or event_cancelled
        self.sealed = True
        return cancelled


def _unit_name(envelope: DriverEnvelope) -> str:
    return f"loom-staging-rollout-{envelope.request_id}-{envelope.attempt_number}.service"


def _path(dependencies: WorkerDependencies, envelope: DriverEnvelope) -> Path:
    if dependencies.envelope_path is not None:
        return dependencies.envelope_path(envelope)
    root = getattr(dependencies.store, "root", None)
    if isinstance(root, Path):
        return (
            root
            / "requests"
            / envelope.request_id
            / "attempts"
            / str(envelope.attempt_number)
            / "envelope.json"
        )
    return Path(
        f"/var/lib/loom-staging-rollout/requests/{envelope.request_id}/"
        f"attempts/{envelope.attempt_number}/envelope.json"
    )


def _load_backup_job(
    config: OperatorConfig,
    store: RequestStore,
    path: Path,
) -> PreflightBackupJobEnvelope:
    if not path.is_absolute() or ".." in path.parts:
        raise ValueError("backup job path is outside the protected request store")
    try:
        relative = path.relative_to(config.state_root)
    except ValueError as exc:
        raise ValueError("backup job path is outside the protected request store") from exc
    if len(relative.parts) != 4:
        raise ValueError("backup job path does not identify one immutable job")
    requests_dir, request_id, backup_dir, filename = relative.parts
    if requests_dir != "requests" or backup_dir != "preflight-backup" or filename != "job.json":
        raise ValueError("backup job path does not identify one immutable job")
    return store.read_preflight_backup_job(request_id)


def _event(
    envelope: DriverEnvelope,
    *,
    dependencies: WorkerDependencies,
    event: str,
    status: str,
    reason: str | None = None,
    current_step: str | None = None,
) -> RequestEvent:
    return RequestEvent(
        request_id=envelope.request_id,
        event=event,  # type: ignore[arg-type]
        occurred_at=dependencies.now(),
        operator=envelope.attempt_operator,
        operator_uid=envelope.attempt_uid,
        attempt_number=envelope.attempt_number,
        unit_name=_unit_name(envelope),
        status=status,  # type: ignore[arg-type]
        reason=reason,
        current_step=current_step,
    )


def _cancel_requested(dependencies: WorkerDependencies, envelope: DriverEnvelope) -> bool:
    events = dependencies.store.read_events(envelope.request_id)
    directives = [
        event
        for event in events
        if event.attempt_number == envelope.attempt_number
        and event.event in {"cancel_requested", "cancel_failed"}
    ]
    return bool(directives and directives[-1].event == "cancel_requested")


def _worker_now(dependencies: WorkerDependencies) -> datetime:
    value = datetime.fromisoformat(dependencies.now().replace("Z", "+00:00"))
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("worker clock must be timezone-aware")
    return value.astimezone(UTC)


def _run_attempt_owned(
    envelope: DriverEnvelope,
    dependencies: WorkerDependencies,
    *,
    signals: _SignalController | None = None,
    release_guard: Callable[[], None],
) -> int:
    """Run one immutable attempt while holding the full-driver lifecycle lock."""
    pointer = ActivePointer(
        request_id=envelope.request_id,
        attempt_number=envelope.attempt_number,
        unit_name=_unit_name(envelope),
        status="pending",
    )
    running_pointer = replace(pointer, status="running")
    signal_controller = signals or _SignalController()
    envelope_path = _path(dependencies, envelope)
    with dependencies.lifecycle.driver_guard():
        persisted = dependencies.store.read_attempt_envelope(
            envelope.request_id,
            envelope.attempt_number,
        )
        if persisted != envelope:
            raise ValueError("worker envelope does not match immutable attempt")
        active = dependencies.store.read_active()
        if active is None or (
            active.request_id,
            active.attempt_number,
            active.unit_name,
        ) != (
            pointer.request_id,
            pointer.attempt_number,
            pointer.unit_name,
        ):
            raise ValueError("worker attempt does not own the active staging pointer")
        final_admission: FinalAttestationAdmission | None = None
        if dependencies.final_admission is None and dependencies.run_final_gates is not None:
            signal_controller.seal_terminal(event_cancelled=False)
            release_guard()
            dependencies.store.append_event(
                _event(
                    envelope,
                    dependencies=dependencies,
                    event="attempt_failed",
                    status="failed",
                    reason="preflight.attestation.final-admission-missing@static",
                    current_step="00-final-admission",
                )
            )
            dependencies.lifecycle.release_active(pointer)
            return 1
        if dependencies.final_admission is not None:
            try:
                final_admission = dependencies.final_admission(envelope)
            except FinalAttestationAdmissionError as exc:
                signal_controller.seal_terminal(event_cancelled=False)
                release_guard()
                dependencies.store.append_event(
                    _event(
                        envelope,
                        dependencies=dependencies,
                        event="attempt_failed",
                        status="failed",
                        reason=(f"preflight.attestation.final-admission.{exc.failure_code}@static"),
                        current_step="00-final-admission",
                    )
                )
                dependencies.lifecycle.release_active(pointer)
                return 1
            except Exception:
                signal_controller.seal_terminal(event_cancelled=False)
                release_guard()
                dependencies.store.append_event(
                    _event(
                        envelope,
                        dependencies=dependencies,
                        event="attempt_failed",
                        status="failed",
                        reason="preflight.attestation.final-admission@static",
                        current_step="00-final-admission",
                    )
                )
                dependencies.lifecycle.release_active(pointer)
                return 1
            if dependencies.run_final_gates is None:
                signal_controller.seal_terminal(event_cancelled=False)
                release_guard()
                dependencies.store.append_event(
                    _event(
                        envelope,
                        dependencies=dependencies,
                        event="attempt_failed",
                        status="failed",
                        reason="final.protected-apply.runner-unavailable@final-only",
                        current_step="00-final-gate-runner",
                    )
                )
                dependencies.lifecycle.release_active(pointer)
                return 1
        dependencies.store.set_active(running_pointer)
        dependencies.store.append_event(
            _event(
                envelope,
                dependencies=dependencies,
                event="attempt_running",
                status="running",
            )
        )
        driver_rc = 1
        cancelled_by_signal = False
        if signal_controller.requested:
            cancelled_by_signal = True
        else:
            try:
                with signal_controller.driver_window():
                    if dependencies.run_final_gates is not None:
                        if final_admission is None:
                            raise ValueError("final gate admission evidence is missing")
                        driver_rc = dependencies.run_final_gates(envelope, final_admission)
                    else:
                        driver_rc = dependencies.run_driver(envelope_path, envelope.resume)
            except _CancellationSignal:
                cancelled_by_signal = True
            except BaseException:
                driver_rc = 1

        cancelled = signal_controller.seal_terminal(
            event_cancelled=(cancelled_by_signal or _cancel_requested(dependencies, envelope))
        )
        if cancelled:
            terminal_event = _event(
                envelope,
                dependencies=dependencies,
                event="cancelled",
                status="cancelled",
                reason="cancel_requested",
            )
            return_code = 130
        elif driver_rc == 0:
            terminal_event = _event(
                envelope,
                dependencies=dependencies,
                event="attempt_done",
                status="done",
            )
            return_code = 0
        else:
            failure = (
                dependencies.read_driver_failure(envelope)
                if dependencies.read_driver_failure is not None
                else None
            )
            terminal_event = _event(
                envelope,
                dependencies=dependencies,
                event="attempt_failed",
                status="failed",
                reason=(
                    f"{failure.failure_code}@{failure.discovered_stage.value}"
                    if failure is not None
                    else "driver_failed"
                ),
                current_step=(
                    f"{failure.step_number:02d}-{failure.step_name}"
                    if failure is not None
                    else None
                ),
            )
            return_code = 1
        release_guard()
        dependencies.store.append_event(terminal_event)
        dependencies.lifecycle.release_active(running_pointer)
        return return_code


def run_attempt(
    envelope: DriverEnvelope,
    dependencies: WorkerDependencies,
    *,
    signals: _SignalController | None = None,
) -> int:
    """Run one immutable attempt while retaining request-bound guard ownership."""

    guard_owned = False
    release_attempted = False

    def release_guard() -> None:
        nonlocal release_attempted
        release_attempted = True
        _release_mutation_guard(dependencies, envelope.request_id)

    try:
        guard_owned = True
        evidence = _assert_mutation_guard_ready(
            dependencies,
            request_id=envelope.request_id,
            candidate_sha=envelope.resolved_sha,
            candidate_tree=envelope.resolved_tree,
        )
        original = dependencies.store.read_preflight_request(envelope.request_id)
        if (
            envelope.resolved_tree is None
            or original.request_id != envelope.request_id
            or original.rollout_id != envelope.rollout_id
            or original.candidate.resolved_sha != envelope.resolved_sha
            or original.candidate_tree != envelope.resolved_tree
            or original.environment != envelope.environment
            or original.namespace != envelope.namespace
        ):
            raise ValueError("staging mutation guard binding drifted")
        expected_mutation_epoch = original.mutation_epoch
        if envelope.resume and dependencies.state_root is not None:
            recovery_attempt = find_advanced_epoch_attempt(
                dependencies.state_root,
                request_id=envelope.request_id,
                through_attempt=envelope.attempt_number - 1,
                candidate_sha=envelope.resolved_sha,
                attestation_digest=envelope.preflight_attestation_sha256,
                starting_mutation_epoch=original.mutation_epoch,
                service_uid=os.geteuid(),
            )
            if recovery_attempt is not None:
                expected_mutation_epoch += 1
        if evidence.mutation_epoch != expected_mutation_epoch:
            raise ValueError("staging mutation guard binding drifted")
        return _run_attempt_owned(
            envelope,
            dependencies,
            signals=signals,
            release_guard=release_guard,
        )
    finally:
        if guard_owned and not release_attempted:
            release_guard()


def _seal_backup_failure(
    dependencies: WorkerDependencies,
    state: BackupJobState,
    *,
    action: LifecycleAction,
    failure_code: str,
) -> None:
    failed = transition_backup_job(
        state,
        action,
        updated_at=_worker_now(dependencies),
        failure_code=failure_code,
    )
    dependencies.store.replace_preflight_backup_job_state(
        failed,
        expected_sequence=state.sequence,
    )


def _run_backup_job_owned(
    envelope: PreflightBackupJobEnvelope,
    dependencies: WorkerDependencies,
    *,
    signals: _SignalController | None = None,
    handoff_guard: Callable[[], None],
) -> int:
    """Run one detached backup and publish only verified CAS state."""
    if dependencies.run_backup is None:
        raise ValueError("backup worker implementation is unavailable")
    persisted = dependencies.store.read_preflight_backup_job(envelope.request_id)
    if persisted != envelope:
        raise ValueError("worker backup envelope does not match immutable job")
    request = dependencies.store.read_preflight_request(envelope.request_id)
    dependencies.store.read_preflight_assessment(envelope.request_id)
    state = dependencies.store.read_preflight_backup_job_state(envelope.request_id)
    if state.phase is not LifecyclePhase.BACKUP_PENDING:
        raise ValueError("backup worker job is not pending")
    running = transition_backup_job(
        state,
        LifecycleAction.START_BACKUP,
        updated_at=_worker_now(dependencies),
    )
    dependencies.store.replace_preflight_backup_job_state(
        running,
        expected_sequence=state.sequence,
    )
    signal_controller = signals or _SignalController()

    def cancelled() -> bool:
        current = dependencies.store.read_preflight_backup_job_state(envelope.request_id)
        return (
            signal_controller.requested or current.phase is LifecyclePhase.BACKUP_CANCEL_REQUESTED
        )

    try:
        with signal_controller.driver_window():
            verified = dependencies.run_backup(request, envelope, cancelled)
    except _CancellationSignal:
        current = dependencies.store.read_preflight_backup_job_state(envelope.request_id)
        if current.phase is LifecyclePhase.BACKUP_RUNNING:
            requested = transition_backup_job(
                current,
                LifecycleAction.REQUEST_CANCEL,
                updated_at=_worker_now(dependencies),
            )
            dependencies.store.replace_preflight_backup_job_state(
                requested,
                expected_sequence=current.sequence,
            )
            current = requested
        _seal_backup_failure(
            dependencies,
            current,
            action=LifecycleAction.SEAL_CANCELLED,
            failure_code="backup_cancelled",
        )
        return 130
    except BaseException as error:
        current = dependencies.store.read_preflight_backup_job_state(envelope.request_id)
        action = (
            LifecycleAction.SEAL_CANCELLED
            if current.phase is LifecyclePhase.BACKUP_CANCEL_REQUESTED
            else LifecycleAction.FAIL_BACKUP
        )
        failure_code = (
            "backup_cancelled"
            if action is LifecycleAction.SEAL_CANCELLED
            else _backup_failure_code(error)
        )
        _seal_backup_failure(
            dependencies,
            current,
            action=action,
            failure_code=failure_code,
        )
        if action is LifecycleAction.FAIL_BACKUP:
            _append_backup_failed_event(
                dependencies,
                request=request,
                reason=backup_public_reason_for_code(failure_code),
            )
            _write_backup_failure_diagnostic(
                dependencies,
                request=request,
                envelope=envelope,
                failure_code=failure_code,
                error=error,
            )
        return 1

    current = dependencies.store.read_preflight_backup_job_state(envelope.request_id)
    if cancelled():
        if current.phase is LifecyclePhase.BACKUP_RUNNING:
            requested = transition_backup_job(
                current,
                LifecycleAction.REQUEST_CANCEL,
                updated_at=_worker_now(dependencies),
            )
            dependencies.store.replace_preflight_backup_job_state(
                requested,
                expected_sequence=current.sequence,
            )
            current = requested
        _seal_backup_failure(
            dependencies,
            current,
            action=LifecycleAction.SEAL_CANCELLED,
            failure_code="backup_cancelled",
        )
        return 130
    if dependencies.reconcile_verified_backup is not None:
        try:
            dependencies.reconcile_verified_backup(envelope, verified)
        except BaseException as error:
            current = dependencies.store.read_preflight_backup_job_state(envelope.request_id)
            _seal_backup_failure(
                dependencies,
                current,
                action=LifecycleAction.FAIL_BACKUP,
                failure_code=_backup_failure_code(error),
            )
            _append_backup_failed_event(
                dependencies,
                request=request,
                reason=backup_public_reason_for_code(_backup_failure_code(error)),
            )
            _write_backup_failure_diagnostic(
                dependencies,
                request=request,
                envelope=envelope,
                failure_code=_backup_failure_code(error),
                error=error,
            )
            return 1
    completed = transition_backup_job(
        current,
        LifecycleAction.VERIFY_BACKUP,
        updated_at=_worker_now(dependencies),
        manifest_sha256=verified.manifest_sha256,
        lease_digest=verified.lease_digest,
        preflight_attestation_sha256=verified.preflight_attestation_sha256,
    )
    dependencies.store.replace_preflight_backup_job_state(
        completed,
        expected_sequence=current.sequence,
    )
    if dependencies.finalize_backup is None:
        return 0
    driver_envelope = dependencies.finalize_backup(request, verified)
    if (
        driver_envelope.request_id != request.request_id
        or driver_envelope.resolved_sha != request.candidate.resolved_sha
        or driver_envelope.preflight_attestation_sha256 != verified.preflight_attestation_sha256
        or driver_envelope.backup_manifest_sha256 != verified.manifest_sha256
    ):
        raise ValueError("finalized rollout envelope drifts from verified backup")
    launch_pending = transition_backup_job(
        completed,
        LifecycleAction.PUBLISH_LAUNCH,
        updated_at=_worker_now(dependencies),
    )
    dependencies.store.replace_preflight_backup_job_state(
        launch_pending,
        expected_sequence=completed.sequence,
    )
    dependencies.lifecycle.launch(driver_envelope)
    handoff_guard()
    launch_running = transition_backup_job(
        launch_pending,
        LifecycleAction.START_LAUNCH,
        updated_at=_worker_now(dependencies),
    )
    dependencies.store.replace_preflight_backup_job_state(
        launch_running,
        expected_sequence=launch_pending.sequence,
    )
    return 0


def run_backup_job(
    envelope: PreflightBackupJobEnvelope,
    dependencies: WorkerDependencies,
    *,
    signals: _SignalController | None = None,
) -> int:
    """Run one backup while retaining guard ownership through attempt launch."""

    guard = _mutation_guard(dependencies)
    guard_owned = False
    guard_transferred = False
    try:
        guard_owned = True
        evidence = guard.assert_ready(envelope.request_id)
        if (
            evidence.request_id != envelope.request_id
            or evidence.candidate_sha != envelope.candidate_sha
            or evidence.candidate_tree != envelope.candidate_tree
            or evidence.mutation_epoch != envelope.mutation_epoch
            or evidence.state != "ready"
        ):
            raise ValueError("staging mutation guard binding drifted")

        if dependencies.run_backup is None:
            raise ValueError("backup worker implementation is unavailable")

        def handoff_guard() -> None:
            nonlocal guard_transferred
            guard_transferred = True

        return _run_backup_job_owned(
            envelope,
            dependencies,
            signals=signals,
            handoff_guard=handoff_guard,
        )
    finally:
        if guard_owned and not guard_transferred:
            _release_mutation_guard(dependencies, envelope.request_id)


def _finalize_verified_backup(
    config: OperatorConfig,
    store: RequestStore,
    request: PreflightRequest,
    verified: VerifiedBackupJob,
) -> DriverEnvelope:
    """Publish final request and attempt only after exact backup verification."""
    rollout_request = RolloutRequest(
        request_id=request.request_id,
        rollout_id=request.rollout_id,
        caller=request.caller,
        candidate=request.candidate,
        requested_at=request.requested_at,
        runner_config_sha256=request.runner_config_sha256,
        preflight_attestation_sha256=verified.preflight_attestation_sha256,
        preflight_registry_sha256=request.preflight_registry_sha256,
        preflight_coverage_sha256=request.preflight_coverage_sha256,
        command=request.command,
        status="pending",
    )
    store.promote_preflight_request(rollout_request)
    envelope = DriverEnvelope(
        schema_version=1,
        request_id=request.request_id,
        rollout_id=request.rollout_id,
        initiating_operator=request.caller.username,
        initiating_uid=request.caller.uid,
        attempt_number=1,
        attempt_operator=request.caller.username,
        attempt_uid=request.caller.uid,
        remote_url=request.candidate.remote_url,
        target_ref=request.candidate.target_ref,
        resolved_sha=request.candidate.resolved_sha,
        image_tag=request.candidate.image_tag,
        fetched_at=request.candidate.fetched_at,
        backup_manifest_path=str(verified.manifest_path),
        backup_manifest_sha256=verified.manifest_sha256,
        runner_config_sha256=request.runner_config_sha256,
        preflight_attestation_sha256=verified.preflight_attestation_sha256,
        preflight_registry_sha256=request.preflight_registry_sha256,
        preflight_coverage_sha256=request.preflight_coverage_sha256,
        cluster_name=config.cluster_name,
        namespace=config.namespace,
        environment=config.environment,
        cp_url=config.cp_url,
        cluster_config_path=str(config.cluster_config_path),
        rollout_root=str(config.rollout_root),
        admin_token_source=config.admin_token_source,
        worker_token_source=config.worker_token_source,
        service_token_source=config.service_token_source,
        expect_admin_token_fingerprint=config.expect_admin_token_fingerprint,
        smoke_on_behalf_username=config.smoke_on_behalf_username,
        smoke_on_behalf_team_id=config.smoke_on_behalf_team_id,
        scope=config.scope,
        gb10_prep_concurrency=config.gb10_prep_concurrency,
        resume=False,
        source_mode=request.candidate.source_mode,
        resolved_tree=request.candidate.resolved_tree,
        approved_base_sha=request.candidate.approved_base_sha,
    )
    store.publish_attempt_envelope(envelope)
    return envelope


def _run(
    argv: list[str],
    *,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=MUTATION_GUARD_CLIENT_OPERATION_TIMEOUT_SECONDS,
        env=environment,
    )


def _default_dependencies(
    config: OperatorConfig,
    *,
    service_uid: int,
    installed_config: OperatorConfig | None = None,
    runner_install_digest: str | None = None,
    mutation_guard: MutationGuardManager | None = None,
    resume_runtime_upgrade: AdmittedResumeRuntimeUpgrade | None = None,
) -> WorkerDependencies:
    from loom_cli.rollout.preflight_authority import CandidatePreflightPlan

    from .deep_preflight_authority import RuntimePurpose
    from .final_gate_action_source import FinalGateActionSource
    from .final_gate_runner import FinalGateRunner
    from .installed_deep_preflight_factory import build_installed_deep_preflight_composition
    from .installed_detached_preflight import build_installed_detached_preflight_runner

    control_config = config if installed_config is None else installed_config
    store = RequestStore(config.state_root)
    child_environment = sanitized_child_environment(control_config, service_uid=service_uid)
    systemd = SystemdUserManager(
        control_config,
        service_uid=service_uid,
        run=lambda argv: _run(argv, environment=child_environment),
    )
    lifecycle = LifecycleCoordinator(control_config, store=store, systemd=systemd)
    if mutation_guard is None:
        mutation_guard = MutationGuardManager(
            config=control_config,
            service_uid=service_uid,
            systemd=systemd,
        )

    def clock() -> datetime:
        return datetime.now(UTC)

    try:
        service_gid = pwd.getpwnam(config.service_user).pw_gid
    except (KeyError, OSError) as exc:
        raise ValueError("worker service account is unavailable") from exc
    composition = build_installed_deep_preflight_composition(
        config,
        service_uid=service_uid,
        service_gid=service_gid,
        store=store,
        now=clock,
        rollout_runner_install_digest=runner_install_digest,
        installed_config=control_config,
        resume_runtime_upgrade=resume_runtime_upgrade,
    )
    deep_preflight = composition.authority()
    detached_preflight = build_installed_detached_preflight_runner(
        config,
        service_uid=service_uid,
        service_gid=service_gid,
        store=store,
        now=clock,
        authority=deep_preflight,
    )
    recovery_creator = BackupCreator(config, service_uid=service_uid)
    recovery_activator = BackupPayloadActivator(
        creator=recovery_creator,
        enforce_freshness=False,
    )

    def run_driver(envelope_path: Path, resume: bool) -> int:
        from loom_cli.cluster_cmd import dispatch

        argv = [
            "rollout",
            "staging",
            "--request-envelope",
            str(envelope_path),
        ]
        if resume:
            argv.append("--resume")
        return dispatch(argv)

    def final_admission(envelope: DriverEnvelope) -> FinalAttestationAdmission:
        return _admit_final_attempt(
            envelope,
            deep_preflight=deep_preflight,
            attestation_store=composition.attestation_store,
            state_root=config.state_root,
            service_uid=service_uid,
        )

    def post_apply_plan(
        candidate: CandidateBinding,
        mutation_epoch: int,
    ) -> CandidatePreflightPlan:
        sources = composition.sources(
            candidate,
            mutation_epoch,
            RuntimePurpose.ADMISSION,
        )
        if sources.candidate != candidate:
            raise ValueError("post-apply source candidate drifted")
        return sources.build(mutation_epoch=mutation_epoch).prebackup_plan(candidate)

    final_actions = FinalGateActionSource(
        request_store=store,
        artifact_store=composition.artifact_store,
        state_root=config.state_root,
        service_uid=service_uid,
        run=composition.final_gate_run,
        read_mutation_epoch=composition.read_mutation_epoch,
        now=clock,
        post_apply_plan_factory=post_apply_plan,
    )
    final_gates = FinalGateRunner(
        attestation_store=composition.attestation_store,
        actions_factory=final_actions,
        read_mutation_epoch=composition.read_mutation_epoch,
        now=clock,
        state_root=config.state_root,
        service_uid=service_uid,
    )

    return WorkerDependencies(
        store=store,
        lifecycle=lifecycle,
        run_driver=run_driver,
        now=lambda: clock().isoformat().replace("+00:00", "Z"),
        stderr=sys.stderr,
        state_root=config.state_root,
        mutation_guard=mutation_guard,
        envelope_path=lambda envelope: (
            config.state_root
            / "requests"
            / envelope.request_id
            / "attempts"
            / str(envelope.attempt_number)
            / "envelope.json"
        ),
        load_envelope=lambda path: load_validated_envelope(
            path,
            config,
            effective_uid=service_uid,
        ),
        load_backup_job=lambda path: _load_backup_job(config, store, path),
        run_backup=detached_preflight,
        finalize_backup=lambda request, verified: _finalize_verified_backup(
            config,
            store,
            request,
            verified,
        ),
        reconcile_verified_backup=lambda envelope, verified: converge_verified_backup_candidate(
            store,
            request_id=envelope.request_id,
            payload_id=envelope.payload_id,
            bundle_name=envelope.bundle_name,
            manifest_sha256=verified.manifest_sha256,
            lease_digest=verified.lease_digest,
            activate_payload=recovery_activator,
        ),
        read_driver_failure=lambda envelope: _read_driver_failure(config, envelope),
        final_admission=final_admission,
        run_final_gates=final_gates,
    )


def _default_attempt_dependencies(
    installed_config: OperatorConfig,
    envelope_path: Path,
    *,
    service_uid: int,
) -> WorkerDependencies:
    """Compose historical execution inputs under the current installed guard."""
    request_id = _worker_path_request_id(
        envelope_path,
        state_root=installed_config.state_root,
        command="run-attempt",
    )
    child_environment = sanitized_child_environment(
        installed_config,
        service_uid=service_uid,
    )

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        return _run(argv, environment=child_environment)

    systemd = SystemdUserManager(
        installed_config,
        service_uid=service_uid,
        run=run,
    )
    mutation_guard: MutationGuardManager | None = None
    try:
        resume_runtime_upgrade = (
            build_installed_resume_runtime_upgrade_authority(
                installed_config,
                service_uid=service_uid,
                run=run,
            )
            if installed_config.source_mode == "merged-dev"
            else None
        )
        envelope, effective_config = load_validated_envelope_with_config(
            envelope_path,
            installed_config,
            effective_uid=service_uid,
            resume_runtime_upgrade=resume_runtime_upgrade,
        )
        if envelope.resolved_tree is None:
            raise ValueError("worker resume candidate tree is unavailable")
        candidate_identity = (envelope.resolved_sha, envelope.resolved_tree)
        mutation_guard = MutationGuardManager(
            config=installed_config,
            service_uid=service_uid,
            systemd=systemd,
            resolve_candidate=lambda _config: candidate_identity,
        )
        guard_evidence = mutation_guard.assert_ready(request_id)
        if (
            guard_evidence.request_id != request_id
            or guard_evidence.candidate_sha != envelope.resolved_sha
            or guard_evidence.candidate_tree != envelope.resolved_tree
            or guard_evidence.state != "ready"
        ):
            raise ValueError("staging mutation guard binding drifted")
        admitted_runtime_upgrade = None
        if effective_config != installed_config:
            if resume_runtime_upgrade is None:
                raise ValueError("worker resume runtime upgrade authority is unavailable")
            admitted_runtime_upgrade = resume_runtime_upgrade.admit(
                installed_config,
                candidate_sha=envelope.resolved_sha,
                candidate_tree=envelope.resolved_tree,
                runner_config_sha256=envelope.runner_config_sha256,
                cluster_config_path=envelope.cluster_config_path,
            )
            if admitted_runtime_upgrade.config != effective_config:
                raise ValueError("worker resume runtime upgrade authority drifted")
        attestation = PreflightAttestationStore(installed_config.state_root).read(
            envelope.preflight_attestation_sha256
        )
        bindings = attestation.bindings
        if (
            attestation.attestation_digest != envelope.preflight_attestation_sha256
            or attestation.registry_digest != envelope.preflight_registry_sha256
            or attestation.coverage_digest != envelope.preflight_coverage_sha256
            or bindings.candidate_sha != envelope.resolved_sha
            or bindings.candidate_tree != envelope.resolved_tree
            or bindings.runner_config_hash != envelope.runner_config_sha256
            or bindings.environment != envelope.environment
            or bindings.namespace != envelope.namespace
        ):
            raise ValueError("worker resume attestation binding drifted")
        return _default_dependencies(
            effective_config,
            service_uid=service_uid,
            installed_config=installed_config,
            runner_install_digest=bindings.runner_install_hash,
            mutation_guard=mutation_guard,
            resume_runtime_upgrade=admitted_runtime_upgrade,
        )
    except BaseException:
        if mutation_guard is not None:
            released = mutation_guard.release(request_id)
            if released.request_id != request_id or released.state != "released":
                raise ValueError("staging mutation guard release drifted") from None
        raise


def _read_driver_failure(
    config: OperatorConfig,
    envelope: DriverEnvelope,
) -> RolloutFailureEvidence | None:
    evidence = EvidenceDirectory(config.rollout_root, envelope.rollout_id)
    payload = evidence.read_failure()
    return None if payload is None else RolloutFailureEvidence.from_dict(payload)


def main(
    argv: Sequence[str] | None = None,
    *,
    dependencies: WorkerDependencies | None = None,
) -> int:
    """Expose only the service-owned ``run-attempt --envelope`` surface."""
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
    except (_ArgumentError, SystemExit):
        return 2
    signal_controller = _SignalController()
    previous_term = signal.signal(signal.SIGTERM, signal_controller.handle)
    previous_int = signal.signal(signal.SIGINT, signal_controller.handle)
    try:
        if dependencies is None:
            config = OperatorConfig.load(fixed_operator_config_path())
            service_uid = pwd.getpwnam(config.service_user).pw_uid
            if os.geteuid() != service_uid:
                raise ValueError("worker effective UID does not match service account")
            dependencies = (
                _default_attempt_dependencies(
                    config,
                    args.envelope,
                    service_uid=service_uid,
                )
                if args.command == "run-attempt"
                else _default_dependencies(config, service_uid=service_uid)
            )
        if args.command == "run-attempt":
            if dependencies.load_envelope is None:
                raise ValueError("worker envelope loader is unavailable")
            immutable_path = args.envelope
        if args.command == "run-backup":
            if dependencies.load_backup_job is None:
                raise ValueError("worker backup loader is unavailable")
            immutable_path = args.job
        if args.command not in {"run-attempt", "run-backup"}:
            return 2
        request_id = _worker_path_request_id(
            immutable_path,
            state_root=dependencies.state_root,
            command=args.command,
        )
        guard_owned = True
        try:
            evidence = _mutation_guard(dependencies).assert_ready(request_id)
            if evidence.request_id != request_id or evidence.state != "ready":
                raise ValueError("staging mutation guard binding drifted")
            if args.command == "run-attempt":
                assert dependencies.load_envelope is not None
                envelope = dependencies.load_envelope(immutable_path)
                if envelope.request_id != request_id:
                    raise ValueError("worker envelope path binding drifted")
                guard_owned = False
                return run_attempt(envelope, dependencies, signals=signal_controller)
            assert dependencies.load_backup_job is not None
            backup_job = dependencies.load_backup_job(immutable_path)
            if backup_job.request_id != request_id:
                raise ValueError("worker backup path binding drifted")
            guard_owned = False
            return run_backup_job(backup_job, dependencies, signals=signal_controller)
        finally:
            if guard_owned:
                _release_mutation_guard(dependencies, request_id)
    except BaseException as exc:
        if isinstance(exc, KeyboardInterrupt):
            raise
        if dependencies is not None:
            # Last-resort boundary: an error escaped run-attempt/run-backup
            # without a coded reason. Surface the exception type + raise-site
            # (secret-safe) instead of a dead-end "failed safely" (#1085 p1).
            activity = getattr(args, "command", None) or "worker"
            dependencies.stderr.write(
                f"error: {redact_rollout_text(unclassified_failure_diagnostic(exc, activity=str(activity)))}\n"
            )
        return 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)


if __name__ == "__main__":  # pragma: no cover - service entrypoint
    raise SystemExit(main())


__all__ = [
    "VerifiedBackupJob",
    "WorkerDependencies",
    "main",
    "run_attempt",
    "run_backup_job",
]
