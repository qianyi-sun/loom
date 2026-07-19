"""Service-only detached worker for one finalized staging attempt."""

from __future__ import annotations

import argparse
import os
import pwd
import signal
import subprocess
import sys
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Never, TextIO

from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.failure_authority import RolloutFailureEvidence
from loom_cli.rollout.final_attestation_admission import FinalAttestationAdmission
from loom_cli.rollout.lifecycle_protocol import LifecycleAction, LifecyclePhase

from .backup_job import (
    BackupJobState,
    PreflightBackupJobEnvelope,
    transition_backup_job,
)
from .config import OperatorConfig
from .envelope import fixed_operator_config_path, load_validated_envelope
from .lifecycle import LifecycleCoordinator
from .model import (
    ActivePointer,
    CandidateBinding,
    DriverEnvelope,
    PreflightRequest,
    RequestEvent,
    RolloutRequest,
)
from .policy import sanitized_child_environment
from .redaction import redact_rollout_text
from .store import RequestStore
from .systemd import SystemdUserManager


class _ArgumentError(ValueError):
    pass


class _CancellationSignal(BaseException):
    pass


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
    read_driver_failure: Callable[[DriverEnvelope], RolloutFailureEvidence | None] | None = None
    final_admission: Callable[[DriverEnvelope], FinalAttestationAdmission] | None = None
    run_final_gates: Callable[[DriverEnvelope, FinalAttestationAdmission], int] | None = None


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


def run_attempt(
    envelope: DriverEnvelope,
    dependencies: WorkerDependencies,
    *,
    signals: _SignalController | None = None,
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
            signal_controller.seal_terminal(event_cancelled=False)
            dependencies.lifecycle.release_active(pointer)
            return 1
        if dependencies.final_admission is not None:
            try:
                final_admission = dependencies.final_admission(envelope)
            except Exception:
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
                signal_controller.seal_terminal(event_cancelled=False)
                dependencies.lifecycle.release_active(pointer)
                return 1
            if dependencies.run_final_gates is None:
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
                signal_controller.seal_terminal(event_cancelled=False)
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
        dependencies.store.append_event(terminal_event)
        dependencies.lifecycle.release_active(running_pointer)
        return return_code


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


def run_backup_job(
    envelope: PreflightBackupJobEnvelope,
    dependencies: WorkerDependencies,
    *,
    signals: _SignalController | None = None,
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
    except BaseException:
        current = dependencies.store.read_preflight_backup_job_state(envelope.request_id)
        action = (
            LifecycleAction.SEAL_CANCELLED
            if current.phase is LifecyclePhase.BACKUP_CANCEL_REQUESTED
            else LifecycleAction.FAIL_BACKUP
        )
        _seal_backup_failure(
            dependencies,
            current,
            action=action,
            failure_code=(
                "backup_cancelled" if action is LifecycleAction.SEAL_CANCELLED else "backup_failed"
            ),
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
        timeout=120,
        env=environment,
    )


def _default_dependencies(config: OperatorConfig, *, service_uid: int) -> WorkerDependencies:
    from .final_gate_action_source import FinalGateActionSource
    from .final_gate_runner import FinalGateRunner
    from .installed_deep_preflight_factory import build_installed_deep_preflight_composition
    from .installed_detached_preflight import build_installed_detached_preflight_runner

    store = RequestStore(config.state_root)
    child_environment = sanitized_child_environment(config, service_uid=service_uid)
    systemd = SystemdUserManager(
        config,
        service_uid=service_uid,
        run=lambda argv: _run(argv, environment=child_environment),
    )
    lifecycle = LifecycleCoordinator(config, store=store, systemd=systemd)

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
        return deep_preflight.admit_final(
            candidate,
            attestation_digest=envelope.preflight_attestation_sha256,
            expected_registry_digest=envelope.preflight_registry_sha256,
            expected_coverage_digest=envelope.preflight_coverage_sha256,
        )

    final_actions = FinalGateActionSource(
        request_store=store,
        artifact_store=composition.artifact_store,
        state_root=config.state_root,
        service_uid=service_uid,
        run=composition.final_gate_run,
        read_mutation_epoch=composition.read_mutation_epoch,
        now=clock,
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
        read_driver_failure=lambda envelope: _read_driver_failure(config, envelope),
        final_admission=final_admission,
        run_final_gates=final_gates,
    )


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
            dependencies = _default_dependencies(config, service_uid=service_uid)
        if args.command == "run-attempt":
            if dependencies.load_envelope is None:
                raise ValueError("worker envelope loader is unavailable")
            envelope = dependencies.load_envelope(args.envelope)
            return run_attempt(envelope, dependencies, signals=signal_controller)
        if args.command == "run-backup":
            if dependencies.load_backup_job is None:
                raise ValueError("worker backup loader is unavailable")
            backup_job = dependencies.load_backup_job(args.job)
            return run_backup_job(backup_job, dependencies, signals=signal_controller)
        return 2
    except BaseException as exc:
        if isinstance(exc, KeyboardInterrupt):
            raise
        if dependencies is not None:
            dependencies.stderr.write(
                f"error: {redact_rollout_text('worker attempt failed safely')}\n"
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
