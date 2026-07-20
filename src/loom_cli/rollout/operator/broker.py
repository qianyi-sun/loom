"""Authenticated command broker for independent protected staging rollouts."""

from __future__ import annotations

import argparse
import grp
import json
import os
import pwd
import stat
import subprocess
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Never, TextIO, cast
from uuid import uuid4

from loom_cli.rollout.evidence import new_rollout_id
from loom_cli.rollout.lifecycle_protocol import LifecycleAction
from loom_cli.rollout.preflight_pipeline import PreflightAssessment, PreflightPipelineResult

from .backup import (
    BackupCreator,
    BackupError,
    SubprocessBackupCommandRunner,
    VerifiedBackup,
    normalize_backup_public_reason,
)
from .backup_job import PreflightBackupJobEnvelope, transition_backup_job
from .backup_rotation import begin_candidate, fail_candidate
from .candidate import CandidateBindingError, bind_configured_candidate
from .checkpoint_inventory_provider import ReadonlyLifecycleInventoryProvider
from .config import OperatorConfig
from .envelope import fixed_operator_config_path
from .installed_deep_preflight_factory import build_installed_deep_preflight_composition
from .lifecycle import LifecycleBusyError, LifecycleCoordinator, LifecycleError
from .model import (
    CallerIdentity,
    CandidateBinding,
    DriverEnvelope,
    EventStatus,
    PreflightRequest,
    RequestEvent,
    RequestEventType,
    RolloutRequest,
    validate_safe_identifier,
)
from .policy import PolicyError, caller_from_sudo, sanitized_child_environment
from .preflight import PreflightReport, catalog_secret_values, collect_preflight
from .readonly_database_client import InstalledReadonlyDatabaseEvidenceSource
from .redaction import known_secrets_from_sources, redact_rollout_text
from .store import RequestStore, RequestStoreError
from .systemd import (
    JournalLineStream,
    SystemdOperationError,
    SystemdUserManager,
    UnitLaunchError,
)

_MAX_CANCEL_REASON = 500
_MAX_LOG_BYTES = 8 * 1024 * 1024


class _ArgumentError(ValueError):
    pass


class _Parser(argparse.ArgumentParser):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("allow_abbrev", False)
        super().__init__(*args, **kwargs)

    def error(self, message: str) -> Never:
        raise _ArgumentError(message)


def _cancel_reason(value: str) -> str:
    if (
        not value.strip()
        or len(value) > _MAX_CANCEL_REASON
        or any(ord(char) < 32 and char != "\t" for char in value)
    ):
        raise argparse.ArgumentTypeError(
            "cancellation reason must be non-empty and at most 500 characters"
        )
    return value


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="loom-staging-rollout", add_help=True)
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start")
    start.add_argument("--dry-run", action="store_true")

    commands.add_parser("preflight")

    status = commands.add_parser("status")
    status.add_argument("request_id", nargs="?")

    logs = commands.add_parser("logs")
    logs.add_argument("request_id")
    logs.add_argument("--follow", action="store_true")

    resume = commands.add_parser("resume")
    resume.add_argument("request_id")

    cancel = commands.add_parser("cancel")
    cancel.add_argument("request_id")
    cancel.add_argument("--reason", required=True, type=_cancel_reason)

    cleanup_backup = commands.add_parser("cleanup-incomplete-backup")
    cleanup_backup.add_argument("request_id")
    return parser


@dataclass(slots=True)
class BrokerDependencies:
    """Narrow injectable boundaries for parser and orchestration tests."""

    config: OperatorConfig
    authenticate: Callable[[], CallerIdentity]
    preflight: Callable[[], PreflightReport]
    bind_candidate: Callable[[], CandidateBinding]
    backup: Any
    store: Any
    lifecycle: Any
    systemd: Any
    now: Callable[[], datetime]
    new_request_id: Callable[[], str]
    new_rollout_id: Callable[[CandidateBinding], str]
    stdout: TextIO
    stderr: TextIO
    known_secrets: Callable[[], Iterable[str]]
    authorize_preflight: Callable[[CandidateBinding], PreflightPipelineResult] | None = None
    assess_preflight: Callable[[CandidateBinding, int], PreflightAssessment] | None = None
    read_mutation_epoch: Callable[[], int] | None = None
    new_backup_job_id: Callable[[], str] | None = None
    new_payload_id: Callable[[], str] | None = None


def _timestamp(now: Callable[[], datetime]) -> str:
    value = now()
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("broker clock must return a timezone-aware datetime")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _event(
    request_id: str,
    caller: CallerIdentity,
    *,
    now: Callable[[], datetime],
    event: RequestEventType,
    attempt_number: int | None = None,
    unit_name: str | None = None,
    status: EventStatus | None = None,
    reason: str | None = None,
    known_secrets: Iterable[str] = (),
    current_step: str | None = None,
) -> RequestEvent:
    return RequestEvent(
        request_id=request_id,
        event=event,
        occurred_at=_timestamp(now),
        operator=caller.username,
        operator_uid=caller.uid,
        attempt_number=attempt_number,
        unit_name=unit_name,
        status=status,
        reason=(
            None
            if reason is None
            else redact_rollout_text(
                reason,
                known_secrets=known_secrets,
                limit=_MAX_CANCEL_REASON,
            )
        ),
        current_step=current_step,
    )


def _write_json(stream: TextIO, payload: dict[str, object]) -> None:
    stream.write(json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n")


def _safe_error(dependencies: BrokerDependencies, message: str) -> int:
    dependencies.stderr.write(f"error: {redact_rollout_text(message, limit=500)}\n")
    return 1


def _assert_available(dependencies: BrokerDependencies) -> None:
    result = dependencies.lifecycle.reconcile_active()
    if result.outcome == "busy" or (result.pointer is not None and not result.cleared):
        raise LifecycleBusyError(
            "a staging rollout attempt is already pending or running",
            result.safe_status,
        )


def _request(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
    candidate: CandidateBinding,
    preflight: PreflightPipelineResult,
    *,
    preview: bool,
) -> RolloutRequest:
    if not preflight.passed or preflight.attestation is None:
        raise ValueError("rollout request requires a complete preflight attestation")
    request_id = validate_safe_identifier(dependencies.new_request_id(), "request_id")
    rollout_id = validate_safe_identifier(
        dependencies.new_rollout_id(candidate),
        "rollout_id",
    )
    return RolloutRequest(
        request_id=request_id,
        rollout_id=rollout_id,
        caller=caller,
        candidate=candidate,
        requested_at=_timestamp(dependencies.now),
        runner_config_sha256=dependencies.config.config_sha256,
        preflight_attestation_sha256=preflight.attestation.attestation_digest,
        preflight_registry_sha256=preflight.registry_digest,
        preflight_coverage_sha256=preflight.coverage_digest,
        status="preview" if preview else "pending",
    )


def _envelope(
    config: OperatorConfig,
    request: RolloutRequest,
    backup: VerifiedBackup,
    caller: CallerIdentity,
    *,
    attempt_number: int,
    resume: bool,
) -> DriverEnvelope:
    return DriverEnvelope(
        schema_version=1,
        request_id=request.request_id,
        rollout_id=request.rollout_id,
        initiating_operator=request.caller.username,
        initiating_uid=request.caller.uid,
        attempt_number=attempt_number,
        attempt_operator=caller.username,
        attempt_uid=caller.uid,
        remote_url=request.candidate.remote_url,
        target_ref=request.candidate.target_ref,
        resolved_sha=request.candidate.resolved_sha,
        image_tag=request.candidate.image_tag,
        fetched_at=request.candidate.fetched_at,
        backup_manifest_path=str(backup.manifest_path),
        backup_manifest_sha256=backup.manifest_sha256,
        runner_config_sha256=request.runner_config_sha256,
        preflight_attestation_sha256=request.preflight_attestation_sha256,
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
        resume=resume,
        source_mode=request.candidate.source_mode,
        resolved_tree=request.candidate.resolved_tree,
        approved_base_sha=request.candidate.approved_base_sha,
    )


def _staged_preflight_request(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
    candidate: CandidateBinding,
    assessment: PreflightAssessment,
    *,
    request_id: str,
    rollout_id: str,
    requested_at: str,
    mutation_epoch: int,
    preview: bool,
) -> PreflightRequest:
    if not assessment.passed or candidate.resolved_tree is None or mutation_epoch < 0:
        raise ValueError("pre-backup authority is incomplete")
    return PreflightRequest(
        request_id=request_id,
        rollout_id=rollout_id,
        caller=caller,
        candidate=candidate,
        candidate_tree=candidate.resolved_tree,
        requested_at=requested_at,
        runner_config_sha256=dependencies.config.config_sha256,
        preflight_assessment_sha256=assessment.assessment_digest,
        preflight_registry_sha256=assessment.registry_digest,
        preflight_coverage_sha256=assessment.coverage_digest,
        mutation_epoch=mutation_epoch,
        environment=dependencies.config.environment,
        namespace=dependencies.config.namespace,
        status="preview" if preview else "pending",
    )


def _preflight_only(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
) -> int:
    """Assess the exact Tier 0-2 graph without publishing request authority."""
    if dependencies.config.source_mode == "sealed-cumulative" and caller.username != "qianyi":
        return _safe_error(
            dependencies,
            "sealed cumulative preflight requires coordinator authority",
        )
    if dependencies.assess_preflight is None or dependencies.read_mutation_epoch is None:
        return _safe_error(dependencies, "deep rollout preflight is not configured")
    report = dependencies.preflight()
    if not report.passed:
        _write_json(dependencies.stderr, report.to_dict())
        return 1
    candidate = dependencies.bind_candidate()
    mutation_epoch = dependencies.read_mutation_epoch()
    if type(mutation_epoch) is not int or mutation_epoch < 0:
        raise ValueError("staging mutation epoch is invalid")
    assessment = dependencies.assess_preflight(candidate, mutation_epoch)
    if not assessment.passed:
        _write_json(dependencies.stderr, assessment.to_dict())
        return 1
    _write_json(
        dependencies.stdout,
        {
            "candidate_sha": candidate.resolved_sha,
            "candidate_tree": candidate.resolved_tree,
            "coverage_sha256": assessment.coverage_digest,
            "mutation_epoch": mutation_epoch,
            "preflight_assessment_sha256": assessment.assessment_digest,
            "registry_sha256": assessment.registry_digest,
            "status": "passed",
        },
    )
    return 0


def _start_staged(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
    *,
    dry_run: bool,
) -> int:
    """Publish one short-lock detached checkpoint job after Tier 0-2."""
    assert dependencies.assess_preflight is not None
    assert dependencies.read_mutation_epoch is not None
    report = dependencies.preflight()
    if not report.passed:
        _write_json(dependencies.stderr, report.to_dict())
        return 1
    candidate = dependencies.bind_candidate()
    mutation_epoch = dependencies.read_mutation_epoch()
    if type(mutation_epoch) is not int or mutation_epoch < 0:
        raise ValueError("staging mutation epoch is invalid")
    assessment = dependencies.assess_preflight(candidate, mutation_epoch)
    if not assessment.passed:
        _write_json(dependencies.stderr, assessment.to_dict())
        return 1
    request_id = validate_safe_identifier(dependencies.new_request_id(), "request_id")
    rollout_id = validate_safe_identifier(
        dependencies.new_rollout_id(candidate),
        "rollout_id",
    )
    created_at = dependencies.now()
    if created_at.tzinfo is None or created_at.utcoffset() is None:
        raise ValueError("broker clock must return a timezone-aware datetime")
    created_at = created_at.astimezone(UTC)
    request = _staged_preflight_request(
        dependencies,
        caller,
        candidate,
        assessment,
        request_id=request_id,
        rollout_id=rollout_id,
        requested_at=created_at.isoformat().replace("+00:00", "Z"),
        mutation_epoch=mutation_epoch,
        preview=dry_run,
    )

    with dependencies.lifecycle.launch_guard():
        dependencies.lifecycle.assert_admission_open()
        _assert_available(dependencies)
        if dependencies.read_mutation_epoch() != mutation_epoch:
            raise LifecycleBusyError(
                "staging mutation epoch changed during preflight",
                {"reason": "mutation_epoch_drift"},
            )
        rotation = dependencies.store.read_backup_rotation()
        if rotation.candidate is not None or rotation.retirements:
            raise LifecycleBusyError(
                "a detached backup or retirement is already pending",
                {"reason": "backup_lifecycle_busy"},
            )
        dependencies.store.create_preflight_request(request)
        dependencies.store.publish_preflight_assessment(request.request_id, assessment)
        dependencies.store.append_event(
            _event(
                request.request_id,
                caller,
                now=lambda: created_at,
                event="requested",
                status="preview" if dry_run else "pending",
            )
        )
        if dry_run:
            dependencies.store.append_event(
                _event(
                    request.request_id,
                    caller,
                    now=lambda: created_at,
                    event="preview",
                    status="preview",
                )
            )
            _write_json(
                dependencies.stdout,
                {
                    "preflight_assessment_sha256": assessment.assessment_digest,
                    "request_id": request.request_id,
                    "resolved_sha": candidate.resolved_sha,
                    "status": "preview",
                },
            )
            return 0

        job_id = validate_safe_identifier(
            (
                dependencies.new_backup_job_id()
                if dependencies.new_backup_job_id is not None
                else f"job-{uuid4().hex[:16]}"
            ),
            "job_id",
        )
        payload_id = validate_safe_identifier(
            (
                dependencies.new_payload_id()
                if dependencies.new_payload_id is not None
                else f"payload-{uuid4().hex[:16]}"
            ),
            "payload_id",
        )
        bundle_name = BackupCreator.bundle_name(request.request_id, created_at)
        job = PreflightBackupJobEnvelope(
            job_id=job_id,
            request_id=request.request_id,
            payload_id=payload_id,
            candidate_sha=candidate.resolved_sha,
            candidate_tree=request.candidate_tree,
            preflight_assessment_sha256=assessment.assessment_digest,
            preflight_registry_sha256=assessment.registry_digest,
            preflight_coverage_sha256=assessment.coverage_digest,
            mutation_epoch=mutation_epoch,
            environment=request.environment,
            namespace=request.namespace,
            bundle_name=bundle_name,
            created_at=created_at,
        )
        job_path = dependencies.store.publish_preflight_backup_job(job)
        reservation = begin_candidate(
            rotation,
            payload_id=payload_id,
            request_id=request.request_id,
            bundle_name=bundle_name,
            created_at=created_at,
        )
        dependencies.store.replace_backup_rotation(
            reservation.state,
            expected_generation=rotation.generation,
        )
        dependencies.store.append_event(
            _event(
                request.request_id,
                caller,
                now=lambda: created_at,
                event="backup_started",
                status="pending",
                current_step=bundle_name,
            )
        )
        unit_name = f"loom-staging-backup-{request.request_id}.service"
        try:
            dependencies.systemd.start_backup(job_path, unit_name)
        except Exception:
            current_job = dependencies.store.read_preflight_backup_job_state(request.request_id)
            failed_job = transition_backup_job(
                current_job,
                LifecycleAction.FAIL_BACKUP,
                updated_at=created_at,
                failure_code="backup_launch_failed",
            )
            dependencies.store.replace_preflight_backup_job_state(
                failed_job,
                expected_sequence=current_job.sequence,
            )
            current_rotation = dependencies.store.read_backup_rotation()
            failed_rotation = fail_candidate(
                current_rotation,
                payload_id=payload_id,
                failure_code="backup_launch_failed",
            )
            dependencies.store.replace_backup_rotation(
                failed_rotation.state,
                expected_generation=current_rotation.generation,
            )
            dependencies.store.append_event(
                _event(
                    request.request_id,
                    caller,
                    now=lambda: created_at,
                    event="backup_failed",
                    status="failed",
                    reason="backup_launch_failed",
                )
            )
            raise
    _write_json(
        dependencies.stdout,
        {
            "backup_unit": unit_name,
            "preflight_assessment_sha256": assessment.assessment_digest,
            "request_id": request.request_id,
            "resolved_sha": candidate.resolved_sha,
            "status": "backup_pending",
        },
    )
    return 0


def _start(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
    *,
    dry_run: bool,
) -> int:
    if dependencies.config.source_mode == "sealed-cumulative" and caller.username != "qianyi":
        return _safe_error(dependencies, "sealed cumulative rollout requires coordinator authority")
    if dependencies.assess_preflight is not None or dependencies.read_mutation_epoch is not None:
        if dependencies.assess_preflight is None or dependencies.read_mutation_epoch is None:
            return _safe_error(dependencies, "detached preflight dependencies are incomplete")
        return _start_staged(dependencies, caller, dry_run=dry_run)
    with dependencies.lifecycle.launch_guard():
        dependencies.lifecycle.assert_admission_open()
        _assert_available(dependencies)
        report = dependencies.preflight()
        if not report.passed:
            _write_json(dependencies.stderr, report.to_dict())
            return 1
        candidate = dependencies.bind_candidate()
        if dependencies.authorize_preflight is None:
            return _safe_error(dependencies, "deep rollout preflight is not configured")
        deep_preflight = dependencies.authorize_preflight(candidate)
        if not deep_preflight.passed:
            _write_json(dependencies.stderr, deep_preflight.to_dict())
            return 1
        request = _request(
            dependencies,
            caller,
            candidate,
            deep_preflight,
            preview=dry_run,
        )
        dependencies.store.create_request(request)
        dependencies.store.append_event(
            _event(
                request.request_id,
                caller,
                now=dependencies.now,
                event="requested",
                status="preview" if dry_run else "pending",
            )
        )
        if dry_run:
            dependencies.store.append_event(
                _event(
                    request.request_id,
                    caller,
                    now=dependencies.now,
                    event="preview",
                    status="preview",
                )
            )
            _write_json(
                dependencies.stdout,
                {
                    "request_id": request.request_id,
                    "resolved_sha": candidate.resolved_sha,
                    "image_tag": candidate.image_tag,
                    "status": "preview",
                },
            )
            return 0

        backup_created_at = dependencies.now()
        if (
            not isinstance(backup_created_at, datetime)
            or backup_created_at.tzinfo is None
            or backup_created_at.utcoffset() is None
        ):
            raise ValueError("broker clock must return a timezone-aware datetime")
        backup_created_at = backup_created_at.astimezone(UTC)
        bundle_name = BackupCreator.bundle_name(request.request_id, backup_created_at)
        dependencies.store.append_event(
            _event(
                request.request_id,
                caller,
                now=lambda: backup_created_at,
                event="backup_started",
                status="pending",
                current_step=bundle_name,
            )
        )
        try:
            backup = dependencies.backup.create(request, created_at=backup_created_at)
        except BackupError as exc:
            dependencies.store.append_event(
                _event(
                    request.request_id,
                    caller,
                    now=dependencies.now,
                    event="backup_failed",
                    status="failed",
                    reason=exc.public_reason,
                )
            )
            return _safe_error(dependencies, "staging backup failed safely")
        except Exception:
            dependencies.store.append_event(
                _event(
                    request.request_id,
                    caller,
                    now=dependencies.now,
                    event="backup_failed",
                    status="failed",
                    reason="backup_failed",
                )
            )
            return _safe_error(dependencies, "staging backup failed safely")

        envelope = _envelope(
            dependencies.config,
            request,
            backup,
            caller,
            attempt_number=1,
            resume=False,
        )
        dependencies.store.publish_attempt_envelope(envelope)
        dependencies.store.append_event(
            _event(
                request.request_id,
                caller,
                now=dependencies.now,
                event="envelope_published",
                attempt_number=1,
                status="pending",
            )
        )
        pointer = dependencies.lifecycle.launch(envelope)
        _write_json(
            dependencies.stdout,
            {
                "request_id": request.request_id,
                "resolved_sha": candidate.resolved_sha,
                "image_tag": candidate.image_tag,
                "unit": pointer.unit_name,
                "status": "pending",
            },
        )
        return 0


def _latest_attempt_event(events: list[RequestEvent]) -> RequestEvent | None:
    attempt_events = [event for event in events if event.attempt_number is not None]
    if not attempt_events:
        return None
    latest_attempt = max(cast(int, event.attempt_number) for event in attempt_events)
    current_attempt = [event for event in attempt_events if event.attempt_number == latest_attempt]
    terminal_events = {
        "attempt_done",
        "attempt_failed",
        "cancelled",
        "launch_failed",
    }
    for event in reversed(current_attempt):
        if event.event in terminal_events:
            return event
    return current_attempt[-1]


def _resume_binding_matches(
    config: OperatorConfig,
    request: RolloutRequest,
    envelope: DriverEnvelope,
) -> bool:
    """Bind resume to the original request and every protected config input."""
    return (
        request.runner_config_sha256 == config.config_sha256
        and envelope.runner_config_sha256 == config.config_sha256
        and request.preflight_attestation_sha256 == envelope.preflight_attestation_sha256
        and request.preflight_registry_sha256 == envelope.preflight_registry_sha256
        and request.preflight_coverage_sha256 == envelope.preflight_coverage_sha256
        and envelope.request_id == request.request_id
        and envelope.rollout_id == request.rollout_id
        and envelope.initiating_operator == request.caller.username
        and envelope.initiating_uid == request.caller.uid
        and envelope.remote_url == request.candidate.remote_url == config.remote_url
        and envelope.target_ref == request.candidate.target_ref
        and config.target_ref == "refs/heads/dev"
        and envelope.resolved_sha == request.candidate.resolved_sha
        and envelope.image_tag == request.candidate.image_tag
        and envelope.fetched_at == request.candidate.fetched_at
        and envelope.source_mode == request.candidate.source_mode == config.source_mode
        and envelope.resolved_tree == request.candidate.resolved_tree
        and envelope.approved_base_sha == request.candidate.approved_base_sha
        and (
            config.source_mode == "merged-dev"
            or (
                envelope.resolved_sha == config.source_commit_sha
                and envelope.resolved_tree == config.source_tree_sha
                and envelope.approved_base_sha == config.source_base_sha
            )
        )
        and envelope.cluster_name == config.cluster_name
        and envelope.namespace == config.namespace
        and envelope.environment == config.environment
        and envelope.cp_url == config.cp_url
        and envelope.cluster_config_path == str(config.cluster_config_path)
        and envelope.rollout_root == str(config.rollout_root)
        and envelope.admin_token_source == config.admin_token_source
        and envelope.worker_token_source == config.worker_token_source
        and envelope.service_token_source == config.service_token_source
        and envelope.expect_admin_token_fingerprint == config.expect_admin_token_fingerprint
        and envelope.smoke_on_behalf_username == config.smoke_on_behalf_username
        and envelope.smoke_on_behalf_team_id == config.smoke_on_behalf_team_id
        and envelope.scope == config.scope
        and envelope.gb10_prep_concurrency == config.gb10_prep_concurrency
    )


def _is_prelaunch_orphan(events: list[RequestEvent], attempt_number: int) -> bool:
    matching = [event for event in events if event.attempt_number == attempt_number]
    event_types = {event.event for event in matching}
    return not bool(
        event_types
        & {
            "launch_pending",
            "launch_failed",
            "attempt_running",
            "attempt_done",
            "attempt_failed",
            "cancel_requested",
            "cancelled",
        }
    )


def _unit_name(request_id: str, attempt_number: int) -> str:
    return f"loom-staging-rollout-{request_id}-{attempt_number}.service"


def _resume(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
    request_id: str,
) -> int:
    if dependencies.config.source_mode == "sealed-cumulative" and caller.username != "qianyi":
        return _safe_error(dependencies, "sealed cumulative rollout requires coordinator authority")
    validate_safe_identifier(request_id, "request_id")
    with dependencies.lifecycle.launch_guard():
        dependencies.lifecycle.assert_admission_open()
        _assert_available(dependencies)
        report = dependencies.preflight()
        if not report.passed:
            _write_json(dependencies.stderr, report.to_dict())
            return 1
        try:
            request = dependencies.store.read_request(request_id)
            events = dependencies.store.read_events(request_id)
        except Exception:
            return _safe_error(dependencies, "request does not exist")
        if request.status == "preview":
            return _safe_error(dependencies, "preview requests cannot be resumed")
        next_attempt = dependencies.store.next_attempt_number(request_id)
        latest_attempt = next_attempt - 1
        if latest_attempt < 1:
            return _safe_error(dependencies, "first finalized attempt is unavailable")
        try:
            first = dependencies.store.read_attempt_envelope(request_id, 1)
            latest_envelope = dependencies.store.read_attempt_envelope(
                request_id,
                latest_attempt,
            )
        except Exception:
            return _safe_error(dependencies, "first finalized attempt is unavailable")
        if not _resume_binding_matches(dependencies.config, request, first):
            return _safe_error(dependencies, "request config binding no longer matches")
        if not _resume_binding_matches(dependencies.config, request, latest_envelope):
            return _safe_error(dependencies, "request config binding no longer matches")
        if (
            latest_envelope.backup_manifest_path != first.backup_manifest_path
            or latest_envelope.backup_manifest_sha256 != first.backup_manifest_sha256
        ):
            return _safe_error(dependencies, "request backup binding no longer matches")
        if _is_prelaunch_orphan(events, latest_attempt):
            if dependencies.store.read_active() is not None:
                return _safe_error(dependencies, "request is not a recoverable prelaunch attempt")
            expected_unit = _unit_name(request_id, latest_attempt)
            if dependencies.systemd.show(expected_unit) is not None:
                return _safe_error(dependencies, "request is not a recoverable prelaunch attempt")
            pointer = dependencies.lifecycle.launch(latest_envelope)
            _write_json(
                dependencies.stdout,
                {
                    "request_id": request_id,
                    "resolved_sha": latest_envelope.resolved_sha,
                    "image_tag": latest_envelope.image_tag,
                    "unit": pointer.unit_name,
                    "status": "pending",
                    "attempt_number": latest_attempt,
                },
            )
            return 0
        latest = _latest_attempt_event(events)
        if latest is None or latest.event not in {
            "attempt_failed",
            "cancelled",
            "launch_failed",
        }:
            return _safe_error(dependencies, "request is not in a resumable failed state")
        attempt_number = next_attempt
        if attempt_number <= 1:
            return _safe_error(dependencies, "first finalized attempt is unavailable")
        envelope = replace(
            first,
            attempt_number=attempt_number,
            attempt_operator=caller.username,
            attempt_uid=caller.uid,
            resume=True,
        )
        dependencies.store.publish_attempt_envelope(envelope)
        dependencies.store.append_event(
            _event(
                request_id,
                caller,
                now=dependencies.now,
                event="attempt_pending",
                attempt_number=attempt_number,
                status="pending",
            )
        )
        pointer = dependencies.lifecycle.launch(envelope)
        _write_json(
            dependencies.stdout,
            {
                "request_id": request_id,
                "resolved_sha": envelope.resolved_sha,
                "image_tag": envelope.image_tag,
                "unit": pointer.unit_name,
                "status": "pending",
                "attempt_number": attempt_number,
            },
        )
        return 0


def _latest_request(dependencies: BrokerDependencies) -> RolloutRequest | None:
    custom = getattr(dependencies.store, "latest_request", None)
    if callable(custom):
        return cast(RolloutRequest | None, custom())
    requests_root = dependencies.config.state_root / "requests"
    try:
        entries = list(os.scandir(requests_root))
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RequestStoreError("request ledger is unavailable") from exc
    requests: list[RolloutRequest] = []
    for entry in entries:
        if not entry.is_dir(follow_symlinks=False):
            continue
        try:
            requests.append(dependencies.store.read_request(entry.name))
        except Exception:
            continue
    return max(requests, key=lambda item: (item.requested_at, item.request_id), default=None)


def _request_status(dependencies: BrokerDependencies, request: RolloutRequest) -> dict[str, object]:
    events = dependencies.store.read_events(request.request_id)
    latest = _latest_attempt_event(events)
    if latest is None and events:
        latest = events[-1]
    payload: dict[str, object] = {
        "request_id": request.request_id,
        "rollout_id": request.rollout_id,
        "initiator": request.caller.username,
        "resolved_sha": request.candidate.resolved_sha,
        "image_tag": request.candidate.image_tag,
        "status": request.status if latest is None or latest.status is None else latest.status,
    }
    if latest is not None:
        payload["stage"] = latest.event
        if latest.attempt_number is not None:
            payload["attempt_number"] = latest.attempt_number
        if latest.unit_name is not None:
            payload["unit"] = latest.unit_name
        if latest.current_step is not None:
            payload["current_step"] = redact_rollout_text(
                latest.current_step,
                known_secrets=dependencies.known_secrets(),
                limit=128,
            )
        if latest.reason is not None:
            payload["reason"] = redact_rollout_text(
                latest.reason,
                known_secrets=dependencies.known_secrets(),
                limit=_MAX_CANCEL_REASON,
            )
        payload["updated_at"] = latest.occurred_at
    return payload


def _status(
    dependencies: BrokerDependencies,
    request_id: str | None,
) -> int:
    request: RolloutRequest | None = None
    reconciled = dependencies.lifecycle.reconcile_active()
    if request_id is None:
        if reconciled.pointer is not None:
            request_id = reconciled.pointer.request_id
        elif reconciled.outcome == "busy":
            _write_json(dependencies.stdout, reconciled.safe_status)
            return 0
        else:
            request = _latest_request(dependencies)
            if request is None:
                _write_json(dependencies.stdout, {"status": "idle"})
                return 0
    if request is None:
        try:
            request = dependencies.store.read_request(cast(str, request_id))
        except Exception:
            return _safe_error(dependencies, "request does not exist")
    payload = _request_status(dependencies, request)
    if reconciled.pointer is not None and reconciled.pointer.request_id == request.request_id:
        payload.update(reconciled.safe_status)
    elif reconciled.safe_status.get("request_id") == request.request_id:
        payload.update(reconciled.safe_status)
    _write_json(dependencies.stdout, payload)
    return 0


def _cleanup_backup(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
    request_id: str,
) -> int:
    validate_safe_identifier(request_id, "request_id")
    with dependencies.lifecycle.launch_guard():
        dependencies.lifecycle.assert_admission_open()
        _assert_available(dependencies)
        try:
            request = dependencies.store.read_request(request_id)
            events = dependencies.store.read_events(request_id)
        except Exception:
            return _safe_error(dependencies, "request does not exist")
        if request.status != "pending":
            return _safe_error(dependencies, "request has no cleanable backup")
        backup_failures = [event for event in events if event.event == "backup_failed"]
        backup_starts = [event for event in events if event.event == "backup_started"]
        forbidden_events = {
            "envelope_published",
            "launch_pending",
            "launch_failed",
            "attempt_pending",
            "attempt_running",
            "attempt_done",
            "attempt_failed",
            "cancel_requested",
            "cancel_failed",
            "cancelled",
        }
        if any(event.event in forbidden_events for event in events):
            return _safe_error(dependencies, "request has no cleanable backup")
        if dependencies.store.next_attempt_number(request_id) != 1:
            return _safe_error(dependencies, "request has no cleanable backup")
        if not backup_failures:
            if len(backup_starts) != 1:
                return _safe_error(dependencies, "request has no cleanable backup")
            failure_reason = "backup_failed"
            dependencies.store.append_event(
                _event(
                    request_id,
                    caller,
                    now=dependencies.now,
                    event="backup_failed",
                    status="failed",
                    reason=failure_reason,
                )
            )
        else:
            failure_reason = normalize_backup_public_reason(backup_failures[-1].reason)
        planned_roots = [
            event.current_step
            for event in backup_starts
            if event.event == "backup_started" and event.current_step is not None
        ]
        if len(planned_roots) > 1:
            return _safe_error(dependencies, "request has no cleanable backup")
        bundle_name = planned_roots[0] if planned_roots else None
        dependencies.store.append_event(
            _event(
                request_id,
                caller,
                now=dependencies.now,
                event="backup_cleanup_started",
                status="failed",
                reason=failure_reason,
            )
        )
        try:
            removed = bool(
                dependencies.backup.cleanup_incomplete(
                    request_id,
                    bundle_name=bundle_name,
                )
            )
        except Exception:
            dependencies.store.append_event(
                _event(
                    request_id,
                    caller,
                    now=dependencies.now,
                    event="backup_cleanup_failed",
                    status="failed",
                    reason=failure_reason,
                )
            )
            return _safe_error(dependencies, "incomplete backup cleanup failed safely")
        dependencies.store.append_event(
            _event(
                request_id,
                caller,
                now=dependencies.now,
                event="backup_cleanup_done",
                status="failed",
                reason=failure_reason,
            )
        )
    _write_json(
        dependencies.stdout,
        {
            "request_id": request_id,
            "status": "failed",
            "reason": failure_reason,
            "cleanup": "removed" if removed else "already_absent",
        },
    )
    return 0


def _known_unit(dependencies: BrokerDependencies, request_id: str) -> str | None:
    events = dependencies.store.read_events(request_id)
    for event in reversed(events):
        if event.unit_name is not None:
            return cast(str, event.unit_name)
    attempts = dependencies.store.next_attempt_number(request_id) - 1
    if attempts < 1:
        return None
    envelope = dependencies.store.read_attempt_envelope(request_id, attempts)
    return f"loom-staging-rollout-{request_id}-{envelope.attempt_number}.service"


def _read_rollout_log(path: Path) -> str:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError:
        return ""
    except OSError as exc:
        raise RequestStoreError("rollout log is unavailable") from exc
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > _MAX_LOG_BYTES:
            raise RequestStoreError("rollout log is unavailable")
        payload = os.read(fd, _MAX_LOG_BYTES + 1)
        if len(payload) > _MAX_LOG_BYTES:
            raise RequestStoreError("rollout log is unavailable")
        return payload.decode("utf-8", errors="replace")
    finally:
        os.close(fd)


def _logs(
    dependencies: BrokerDependencies,
    request_id: str,
    *,
    follow: bool,
) -> int:
    validate_safe_identifier(request_id, "request_id")
    try:
        request = dependencies.store.read_request(request_id)
        unit = _known_unit(dependencies, request_id)
    except Exception:
        return _safe_error(dependencies, "request does not exist")
    if unit is None:
        return _safe_error(dependencies, "request has no rollout attempt logs")
    secrets = tuple(dependencies.known_secrets())
    rollout_log = (
        dependencies.config.rollout_root / "rollouts" / request.rollout_id / "logs" / "driver.log"
    )
    try:
        historical = _read_rollout_log(rollout_log)
        if historical:
            dependencies.stdout.write(redact_rollout_text(historical, known_secrets=secrets))
        lines = dependencies.systemd.stream_journal(unit, follow)
        try:
            for line in lines:
                dependencies.stdout.write(redact_rollout_text(line, known_secrets=secrets))
                dependencies.stdout.flush()
        finally:
            close = getattr(lines, "close", None)
            if callable(close):
                close()
    except Exception:
        return _safe_error(dependencies, "rollout logs are unavailable")
    return 0


def _cancel(
    dependencies: BrokerDependencies,
    caller: CallerIdentity,
    request_id: str,
    reason: str,
) -> int:
    validate_safe_identifier(request_id, "request_id")
    with dependencies.lifecycle.launch_guard():
        dependencies.lifecycle.reconcile_active()
        try:
            request = dependencies.store.read_request(request_id)
            events = dependencies.store.read_events(request_id)
        except Exception:
            return _safe_error(dependencies, "request does not exist")
        del request
        latest = _latest_attempt_event(events)
        if latest is not None and latest.event in {
            "attempt_done",
            "attempt_failed",
            "cancelled",
            "launch_failed",
        }:
            return _safe_error(dependencies, "terminal requests cannot be cancelled")
        pointer = dependencies.store.read_active()
        if pointer is None or pointer.request_id != request_id:
            return _safe_error(dependencies, "request is not the active staging owner")
        cancel_event = _event(
            request_id,
            caller,
            now=dependencies.now,
            event="cancel_requested",
            attempt_number=pointer.attempt_number,
            unit_name=pointer.unit_name,
            status=pointer.status,
            reason=reason,
            known_secrets=dependencies.known_secrets(),
        )
        dependencies.store.append_event(cancel_event)
        try:
            dependencies.systemd.terminate(pointer.unit_name)
        except Exception:
            dependencies.store.append_event(
                _event(
                    request_id,
                    caller,
                    now=dependencies.now,
                    event="cancel_failed",
                    attempt_number=pointer.attempt_number,
                    unit_name=pointer.unit_name,
                    status=pointer.status,
                    reason="unit_termination_failed",
                )
            )
            raise
    _write_json(
        dependencies.stdout,
        {
            "request_id": request_id,
            "attempt_number": pointer.attempt_number,
            "status": "cancel_requested",
        },
    )
    return 0


class _PopenLineStream:
    def __init__(self, process: subprocess.Popen[str]) -> None:
        self._process = process
        if process.stdout is None:
            raise OSError("journal stream stdout is unavailable")
        self._stdout = process.stdout

    def __iter__(self) -> _PopenLineStream:
        return self

    def __next__(self) -> str:
        line = self._stdout.readline()
        if line:
            return line
        raise StopIteration

    def close(self) -> None:
        self._stdout.close()
        if self._process.poll() is None:
            self._process.terminate()
        self._process.wait(timeout=5)


def _run(
    argv: list[str],
    *,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    timeout = 120 if argv and argv[0] in {"systemctl", "systemd-run"} else 30
    return subprocess.run(
        argv,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=environment,
    )


def _stream(
    argv: list[str],
    *,
    environment: dict[str, str],
) -> JournalLineStream:
    return cast(
        JournalLineStream,
        _PopenLineStream(
            subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                bufsize=1,
                env=environment,
            )
        ),
    )


def _groups(username: str) -> set[str]:
    groups = {entry.gr_name for entry in grp.getgrall() if username in entry.gr_mem}
    passwd_entry = pwd.getpwnam(username)
    groups.add(grp.getgrgid(passwd_entry.pw_gid).gr_name)
    return groups


def _operator_known_secrets(
    config: OperatorConfig,
    *,
    service_uid: int,
) -> tuple[str, ...]:
    tokens = known_secrets_from_sources(
        (
            config.admin_token_source,
            config.worker_token_source,
            config.service_token_source,
        )
    )
    return (*tokens, *catalog_secret_values(config, service_uid=service_uid))


def _default_dependencies() -> BrokerDependencies:
    config = OperatorConfig.load(fixed_operator_config_path())
    service_account = pwd.getpwnam(config.service_user)
    service_uid = service_account.pw_uid
    child_environment = sanitized_child_environment(config, service_uid=service_uid)

    def run(argv: list[str]) -> subprocess.CompletedProcess[str]:
        return _run(argv, environment=child_environment)

    def stream(argv: list[str]) -> JournalLineStream:
        return _stream(argv, environment=child_environment)

    store = RequestStore(config.state_root)
    systemd = SystemdUserManager(
        config,
        service_uid=service_uid,
        run=run,
        stream=stream,
    )
    lifecycle = LifecycleCoordinator(config, store=store, systemd=systemd)
    backup_runner = SubprocessBackupCommandRunner()
    inventory_provider = ReadonlyLifecycleInventoryProvider(
        config,
        evidence_source=InstalledReadonlyDatabaseEvidenceSource(service_uid=service_uid),
    )

    def clock() -> datetime:
        return datetime.now(UTC)

    deep_preflight = build_installed_deep_preflight_composition(
        config,
        service_uid=service_uid,
        service_gid=service_account.pw_gid,
        store=store,
        now=clock,
    ).authority()
    return BrokerDependencies(
        config=config,
        authenticate=lambda: caller_from_sudo(
            config,
            os.environ,
            euid=os.geteuid(),
            groups=_groups,
        ),
        preflight=lambda: collect_preflight(config, service_uid=service_uid),
        bind_candidate=lambda: bind_configured_candidate(
            config,
            run=run,
            now=clock,
        ),
        backup=BackupCreator(
            config,
            service_uid=service_uid,
            runner=backup_runner,
            object_inventory_provider=inventory_provider,
        ),
        store=store,
        lifecycle=lifecycle,
        systemd=systemd,
        now=clock,
        new_request_id=lambda: f"req-{uuid4().hex[:16]}",
        new_rollout_id=lambda candidate: new_rollout_id(image_tag=candidate.image_tag),
        stdout=sys.stdout,
        stderr=sys.stderr,
        known_secrets=lambda: _operator_known_secrets(
            config,
            service_uid=service_uid,
        ),
        assess_preflight=deep_preflight.assess,
        read_mutation_epoch=deep_preflight.current_mutation_epoch,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    dependencies: BrokerDependencies | None = None,
) -> int:
    """Parse the fixed operator surface and execute one authenticated action."""
    previous_umask = os.umask(0o077)
    try:
        return _main(argv, dependencies=dependencies)
    finally:
        os.umask(previous_umask)


def _main(
    argv: Sequence[str] | None = None,
    *,
    dependencies: BrokerDependencies | None = None,
) -> int:
    deps = dependencies
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
    except (_ArgumentError, SystemExit):
        return 2
    try:
        deps = dependencies or _default_dependencies()
        caller = deps.authenticate()
        if args.command == "preflight":
            return _preflight_only(deps, caller)
        if args.command == "start":
            return _start(deps, caller, dry_run=bool(args.dry_run))
        if args.command == "status":
            return _status(deps, args.request_id)
        if args.command == "logs":
            return _logs(deps, args.request_id, follow=bool(args.follow))
        if args.command == "resume":
            return _resume(deps, caller, args.request_id)
        if args.command == "cancel":
            return _cancel(deps, caller, args.request_id, args.reason)
        if args.command == "cleanup-incomplete-backup":
            return _cleanup_backup(deps, caller, args.request_id)
        return 2
    except (ValueError, PolicyError):
        if deps is None:
            sys.stderr.write("error: request authorization or validation failed\n")
            return 1
        return _safe_error(deps, "request authorization or validation failed")
    except LifecycleBusyError as exc:
        if deps is None:
            sys.stderr.write("error: staging rollout operation failed safely\n")
            return 1
        _write_json(deps.stderr, {"error": "staging_busy", **exc.safe_status})
        return 1
    except (
        BackupError,
        CandidateBindingError,
        LifecycleError,
        RequestStoreError,
        SystemdOperationError,
        UnitLaunchError,
    ):
        if deps is None:
            sys.stderr.write("error: staging rollout operation failed safely\n")
            return 1
        return _safe_error(deps, "staging rollout operation failed safely")
    except Exception:
        if deps is None:
            sys.stderr.write("error: staging rollout operation failed safely\n")
            return 1
        return _safe_error(deps, "staging rollout operation failed safely")


if __name__ == "__main__":  # pragma: no cover - service entrypoint
    raise SystemExit(main())


__all__ = ["BrokerDependencies", "main"]
