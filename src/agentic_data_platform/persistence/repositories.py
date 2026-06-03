from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import urlparse, urlunparse
from uuid import uuid4

from sqlalchemy import delete, func, select, text
from sqlalchemy.orm import Session

from agentic_data_platform.dashboard.projections import RunDashboardProjection
from agentic_data_platform.benchmarks.fixtures import (
    BenchmarkFixtureCatalog,
    BenchmarkFixtureFamily,
    BenchmarkFixtureInstance,
)
from agentic_data_platform.domain.artifact_metadata import ArtifactChunkKind, ArtifactChunkMetadata, ArtifactUploadStatus
from agentic_data_platform.domain.execution_events import (
    RecoveryReasonCode,
    RunEventType,
    event_type_value,
    recovery_event_metadata,
)
from agentic_data_platform.domain.execution_metadata import (
    RunnerProcessStatus,
    SchedulerCapacityBlock,
    SchedulerLeaseStatus,
    runner_process_metadata,
    scheduler_capacity_blocked_metadata,
    scheduler_lease_metadata,
)
from agentic_data_platform.domain.run_records import (
    ArtifactRef,
    BenchmarkTaskInstance,
    EvaluatorConfig,
    EvaluatorResult,
    JudgeConfig,
    ModelConfig,
    RunnerConfig,
    RunRecord,
    RunStatus,
    RunStatusEvent,
    TerminalTurn,
)
from agentic_data_platform.persistence.models import (
    ArtifactChunkRow,
    ArtifactRow,
    AuditEventRow,
    BenchmarkSuiteRow,
    BenchmarkSuiteVersionRow,
    ProjectRow,
    RunAttemptRow,
    RunRow,
    RunStatusEventRow,
    RunTerminalTurnRow,
    TaskFamilyRow,
    TaskInstanceRow,
    TeamMembershipRow,
    TeamRow,
    UserRow,
    EvaluatorResultRow,
    RunDashboardProjectionRow,
    utc_now,
)

_MAX_INLINE_TERMINAL_STREAM_BYTES = 64 * 1024
_TRUNCATED_STREAM_MARKER = "\n[truncated: full stream is available in object-store artifacts]\n"
_TERMINAL_STATUS_VALUES = {RunStatus.SUCCEEDED.value, RunStatus.FAILED.value, RunStatus.CANCELED.value}
_SCHEDULER_DISPATCH_ADVISORY_LOCK_KEY = 4_117_402_174
_SCHEDULER_DISPATCH_PROCESS_LOCK = threading.Lock()


class StaleExecutionTaskError(ValueError):
    """Raised when a worker acts on an execution task that is no longer current."""


class DuplicateExecutionTaskError(ValueError):
    """Raised when a worker tries to execute an already-running task."""


@dataclass(frozen=True)
class DispatchQueuedRunsResult:
    dispatched_runs: list[RunRecord]
    capacity_blocked_runs: list[SchedulerCapacityBlock]


@dataclass(frozen=True)
class ExpiredArtifactUploadRecord:
    run_id: str
    attempt_id: str | None
    artifact_id: str
    previous_upload_status: str
    upload_status: str
    scheduler_id: str
    expired_at: datetime


def _ranked_queued_run_id_query(*, limit: int):
    queued_fairness_rank = (
        select(
            RunRow.run_id.label("run_id"),
            RunRow.created_at.label("created_at"),
            func.row_number()
            .over(partition_by=RunRow.project_id, order_by=(RunRow.created_at, RunRow.run_id))
            .label("project_queue_rank"),
        )
        .where(RunRow.status == RunStatus.QUEUED.value)
        .subquery()
    )
    return (
        select(queued_fairness_rank.c.run_id)
        .order_by(
            queued_fairness_rank.c.project_queue_rank,
            queued_fairness_rank.c.created_at,
            queued_fairness_rank.c.run_id,
        )
        .limit(limit)
    )


def _queued_dispatch_candidate_lock_query(run_ids: list[str]):
    return (
        select(RunRow)
        .where(RunRow.status == RunStatus.QUEUED.value, RunRow.run_id.in_(run_ids))
        .with_for_update(skip_locked=True)
    )


def _scheduler_dispatch_advisory_lock_statement():
    return text(f"SELECT pg_advisory_xact_lock({_SCHEDULER_DISPATCH_ADVISORY_LOCK_KEY}::bigint)")


def _acquire_scheduler_dispatch_lock(session: Session):
    bind = session.get_bind()
    if bind.dialect.name == "postgresql":
        session.execute(_scheduler_dispatch_advisory_lock_statement())
        return None
    _SCHEDULER_DISPATCH_PROCESS_LOCK.acquire()
    return _SCHEDULER_DISPATCH_PROCESS_LOCK


@dataclass(frozen=True)
class RunDashboardProgressRecord:
    run_id: str
    project_id: str
    owner_team: str | None
    status: str
    is_terminal: bool
    artifact_count: int
    turn_count: int
    evaluator_completed: bool
    evaluator_score: float | None
    updated_at: datetime


@dataclass(frozen=True)
class TeamRecord:
    team_id: str
    name: str
    created_at: datetime


@dataclass(frozen=True)
class UserRecord:
    user_id: str
    email: str
    display_name: str
    team_id: str | None
    created_at: datetime


@dataclass(frozen=True)
class ProjectRecord:
    project_id: str
    name: str
    owner_team_id: str | None
    created_by_user_id: str | None
    description: str
    status: str
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class AuditEventRecord:
    event_id: str
    event_type: str
    payload: dict[str, Any]
    occurred_at: datetime
    actor_user_id: str | None = None
    project_id: str | None = None
    run_id: str | None = None
    attempt_id: str | None = None
    subject_type: str | None = None
    subject_id: str | None = None
    metadata: dict[str, Any] | None = None
    request_id: str | None = None


class IdentityRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_team(self, *, team_id: str, name: str) -> TeamRecord:
        row = self.session.get(TeamRow, team_id)
        if row is None:
            row = TeamRow(team_id=team_id, name=name)
            self.session.add(row)
        else:
            row.name = name
        self.session.flush()
        return _team_record(row)

    def get_team(self, team_id: str) -> TeamRecord:
        return _team_record(_required(self.session.get(TeamRow, team_id), "team", team_id))

    def list_teams(self) -> list[TeamRecord]:
        return [_team_record(row) for row in self.session.scalars(select(TeamRow).order_by(TeamRow.team_id))]

    def create_user(
        self,
        *,
        user_id: str,
        email: str,
        display_name: str,
        team_id: str | None = None,
    ) -> UserRecord:
        row = self.session.get(UserRow, user_id)
        if row is None:
            row = UserRow(user_id=user_id, email=email, display_name=display_name, team_id=team_id)
            self.session.add(row)
        else:
            row.email = email
            row.display_name = display_name
            row.team_id = team_id
        if team_id is not None:
            self.add_member(team_id=team_id, user_id=user_id)
        self.session.flush()
        return _user_record(row)

    def get_user(self, user_id: str) -> UserRecord:
        return _user_record(_required(self.session.get(UserRow, user_id), "user", user_id))

    def get_user_by_email(self, email: str) -> UserRecord:
        row = self.session.scalar(select(UserRow).where(UserRow.email == email))
        return _user_record(_required(row, "user email", email))

    def add_member(self, *, team_id: str, user_id: str, role: str = "member") -> None:
        existing = self.session.scalar(
            select(TeamMembershipRow).where(
                TeamMembershipRow.team_id == team_id,
                TeamMembershipRow.user_id == user_id,
            )
        )
        if existing is None:
            self.session.add(TeamMembershipRow(team_id=team_id, user_id=user_id, role=role))
        else:
            existing.role = role

    def get_membership_role(self, *, team_id: str | None, user_id: str) -> str | None:
        if team_id is None:
            return None
        row = self.session.scalar(
            select(TeamMembershipRow).where(
                TeamMembershipRow.team_id == team_id,
                TeamMembershipRow.user_id == user_id,
            )
        )
        return row.role if row is not None else None


class ProjectRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_project(
        self,
        *,
        project_id: str,
        name: str,
        owner_team_id: str | None = None,
        created_by_user_id: str | None = None,
        description: str = "",
        status: str = "active",
    ) -> ProjectRecord:
        now = utc_now()
        row = self.session.get(ProjectRow, project_id)
        if row is None:
            row = ProjectRow(
                project_id=project_id,
                name=name,
                owner_team_id=owner_team_id,
                created_by_user_id=created_by_user_id,
                description=description,
                status=status,
                created_at=now,
                updated_at=now,
            )
            self.session.add(row)
        else:
            row.name = name
            row.owner_team_id = owner_team_id
            row.created_by_user_id = created_by_user_id
            row.description = description
            row.status = status
            row.updated_at = now
        self.session.flush()
        return _project_record(row)

    def update_project(
        self,
        *,
        project_id: str,
        name: str | None = None,
        description: str | None = None,
        status: str | None = None,
    ) -> ProjectRecord:
        row = _required(self.session.get(ProjectRow, project_id), "project", project_id)
        if name is not None:
            row.name = name
        if description is not None:
            row.description = description
        if status is not None:
            row.status = status
        row.updated_at = utc_now()
        self.session.flush()
        return _project_record(row)

    def get_project(self, project_id: str) -> ProjectRecord:
        return _project_record(_required(self.session.get(ProjectRow, project_id), "project", project_id))

    def list_projects(self, *, owner_team_id: str | None = None) -> list[ProjectRecord]:
        query = select(ProjectRow).order_by(ProjectRow.project_id)
        if owner_team_id is not None:
            query = query.where(ProjectRow.owner_team_id == owner_team_id)
        return [_project_record(row) for row in self.session.scalars(query)]

    def list_project_ids_for_user(self, user_id: str) -> list[str]:
        query = (
            select(ProjectRow.project_id)
            .join(TeamMembershipRow, ProjectRow.owner_team_id == TeamMembershipRow.team_id)
            .where(TeamMembershipRow.user_id == user_id)
            .order_by(ProjectRow.project_id)
        )
        return list(self.session.scalars(query))


class BenchmarkCatalogRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_fixture_catalog(self, catalog: BenchmarkFixtureCatalog) -> None:
        now = utc_now()
        suite = self.session.get(BenchmarkSuiteRow, catalog.suite_name)
        if suite is None:
            suite = BenchmarkSuiteRow(
                suite_name=catalog.suite_name,
                metadata_json=dict(catalog.metadata),
                created_at=now,
                updated_at=now,
            )
            self.session.add(suite)
        else:
            suite.metadata_json = dict(catalog.metadata)
            suite.updated_at = now

        existing_version = self.session.scalar(
            select(BenchmarkSuiteVersionRow).where(
                BenchmarkSuiteVersionRow.suite_name == catalog.suite_name,
                BenchmarkSuiteVersionRow.benchmark_version == catalog.benchmark_version,
            )
        )
        if existing_version is not None:
            self.session.delete(existing_version)
            self.session.flush()

        version = BenchmarkSuiteVersionRow(
            suite_name=catalog.suite_name,
            benchmark_version=catalog.benchmark_version,
            source_uri=catalog.source_uri,
            source_version=catalog.source_version,
            source_version_type=catalog.source_version_type,
            metadata_json=dict(catalog.metadata),
            created_at=now,
            updated_at=now,
        )
        for family in catalog.task_families:
            family_row = TaskFamilyRow(name=family.name, metadata_json={})
            family_row.task_instances = [
                TaskInstanceRow(
                    instance_id=instance.instance_id,
                    instruction_ref=instance.instruction_ref,
                    input_files=list(instance.input_files),
                    input_artifact_refs=list(instance.input_artifact_refs),
                    required_artifacts=list(instance.required_artifacts),
                    runner_image=instance.runner_image,
                    runner_entrypoint=list(instance.runner_entrypoint),
                    runner_contract=instance.runner_contract,
                    metadata_json=dict(instance.metadata),
                )
                for instance in family.instances
            ]
            version.task_families.append(family_row)
        suite.versions.append(version)
        self.session.flush()

    def get_fixture_catalog(self, *, suite_name: str, benchmark_version: str) -> BenchmarkFixtureCatalog:
        version = self._suite_version(suite_name=suite_name, benchmark_version=benchmark_version)
        return _fixture_catalog(version)

    def list_fixture_catalogs(self) -> list[BenchmarkFixtureCatalog]:
        query = select(BenchmarkSuiteVersionRow).order_by(
            BenchmarkSuiteVersionRow.suite_name,
            BenchmarkSuiteVersionRow.benchmark_version,
        )
        return [_fixture_catalog(row) for row in self.session.scalars(query)]

    def get_task_instance(
        self,
        *,
        suite_name: str,
        benchmark_version: str,
        task_family: str,
        instance_id: str,
    ) -> BenchmarkFixtureInstance:
        version = self._suite_version(suite_name=suite_name, benchmark_version=benchmark_version)
        for family in version.task_families:
            if family.name != task_family:
                continue
            for instance in family.task_instances:
                if instance.instance_id == instance_id:
                    return _fixture_instance(instance)

        raise KeyError(f"Unknown benchmark task: {suite_name}@{benchmark_version}/{task_family}/{instance_id}")

    def list_task_instances(self, *, suite_name: str, benchmark_version: str) -> list[BenchmarkFixtureInstance]:
        return self.get_fixture_catalog(
            suite_name=suite_name,
            benchmark_version=benchmark_version,
        ).task_instances()

    def _suite_version(self, *, suite_name: str, benchmark_version: str) -> BenchmarkSuiteVersionRow:
        row = self.session.scalar(
            select(BenchmarkSuiteVersionRow).where(
                BenchmarkSuiteVersionRow.suite_name == suite_name,
                BenchmarkSuiteVersionRow.benchmark_version == benchmark_version,
            )
        )
        return _required(row, "benchmark suite version", f"{suite_name}@{benchmark_version}")


def _fixture_catalog(version: BenchmarkSuiteVersionRow) -> BenchmarkFixtureCatalog:
    families = [
        BenchmarkFixtureFamily(
            name=family.name,
            instances=[_fixture_instance(row) for row in family.task_instances],
        )
        for family in version.task_families
    ]
    return BenchmarkFixtureCatalog(
        suite_name=version.suite_name,
        benchmark_version=version.benchmark_version,
        source_uri=version.source_uri,
        source_version=version.source_version,
        source_version_type=version.source_version_type,
        task_families=families,
        metadata=dict(version.metadata_json or {}),
    )


class RunRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create_run(
        self,
        run: RunRecord,
        *,
        created_by_user_id: str | None = None,
        request_id: str | None = None,
    ) -> RunRecord:
        if self.session.get(RunRow, run.run_id) is not None:
            raise ValueError(f"Run already exists: {run.run_id}")
        if run.status is not RunStatus.QUEUED:
            raise ValueError("new runs must be queued")

        run.created_by_user_id = created_by_user_id
        self.save_run(run)
        self._append_status_event(
            run_id=run.run_id,
            attempt_id=_attempt_id(run.run_id, 1),
            event_type=RunEventType.CREATED,
            from_status=None,
            to_status=RunStatus.QUEUED,
            actor_user_id=created_by_user_id,
            request_id=request_id,
        )
        self.session.flush()
        return self.get_run(run.run_id)

    def cancel_run(
        self,
        run_id: str,
        *,
        reason: str,
        actor_user_id: str | None = None,
        request_id: str | None = None,
    ) -> RunRecord:
        return self.transition_run(
            run_id,
            RunStatus.CANCELED,
            event_type=RunEventType.CANCELED,
            reason=reason,
            actor_user_id=actor_user_id,
            request_id=request_id,
        )

    def transition_run(
        self,
        run_id: str,
        next_status: RunStatus | str,
        *,
        event_type: RunEventType | str = RunEventType.STATUS_CHANGED,
        reason: str | None = None,
        actor_user_id: str | None = None,
        request_id: str | None = None,
    ) -> RunRecord:
        row = self._run_row_for_update(run_id)
        previous_status = RunStatus(row.status)
        run = self._run_record(row)
        run.transition_to(next_status)
        if run.status in {RunStatus.FAILED, RunStatus.CANCELED}:
            run.failure_reason = reason

        row.status = run.status.value
        row.failure_reason = run.failure_reason
        row.updated_at = run.updated_at

        attempt = self._latest_attempt_row(run_id)
        attempt.status = run.status.value
        attempt.failure_reason = run.failure_reason
        if run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}:
            attempt.completed_at = run.updated_at
        attempt.updated_at = utc_now()

        self._append_status_event(
            run_id=run_id,
            attempt_id=attempt.attempt_id,
            event_type=event_type,
            from_status=previous_status,
            to_status=run.status,
            reason=reason,
            actor_user_id=actor_user_id,
            request_id=request_id,
        )
        self.session.flush()
        if run.status.value in _TERMINAL_STATUS_VALUES:
            self._upsert_dashboard_projection(
                run_id=run_id,
                refresh_reason="terminal_status_transition",
            )
        else:
            self._mark_dashboard_projection_dirty(run_id, reason="status_transition")
        return self.get_run(run_id)

    def retry_run(
        self,
        run_id: str,
        *,
        reason: str,
        actor_user_id: str | None = None,
        request_id: str | None = None,
    ) -> RunRecord:
        row = self._run_row_for_update(run_id)
        previous_status = RunStatus(row.status)
        if previous_status not in {RunStatus.FAILED, RunStatus.CANCELED}:
            raise ValueError("can only retry failed or canceled runs")

        next_attempt_number = self._latest_attempt_row(run_id).attempt_number + 1
        attempt_id = _attempt_id(run_id, next_attempt_number)
        now = utc_now()
        self.session.add(
            RunAttemptRow(
                attempt_id=attempt_id,
                run_id=run_id,
                attempt_number=next_attempt_number,
                status=RunStatus.QUEUED.value,
                metadata_json={},
                created_at=now,
                updated_at=now,
            )
        )

        row.status = RunStatus.QUEUED.value
        row.failure_reason = None
        row.updated_at = now

        self._append_status_event(
            run_id=run_id,
            attempt_id=attempt_id,
            event_type=RunEventType.RETRIED,
            from_status=previous_status,
            to_status=RunStatus.QUEUED,
            reason=reason,
            actor_user_id=actor_user_id,
            request_id=request_id,
        )
        self._mark_dashboard_projection_dirty(run_id, reason="run_retried")
        self.session.flush()
        return self.get_run(run_id)

    def dispatch_queued_runs(
        self,
        *,
        scheduler_id: str,
        max_runs: int,
        backend_limits: dict[str, int] | None = None,
        project_limits: dict[str, int] | None = None,
        provider_limits: dict[str, int] | None = None,
        model_limits: dict[str, int] | None = None,
        agent_limits: dict[str, int] | None = None,
        benchmark_limits: dict[str, int] | None = None,
        request_id: str | None = None,
    ) -> list[RunRecord]:
        return self.dispatch_queued_runs_with_diagnostics(
            scheduler_id=scheduler_id,
            max_runs=max_runs,
            backend_limits=backend_limits,
            project_limits=project_limits,
            provider_limits=provider_limits,
            model_limits=model_limits,
            agent_limits=agent_limits,
            benchmark_limits=benchmark_limits,
            request_id=request_id,
        ).dispatched_runs

    def dispatch_queued_runs_with_diagnostics(
        self,
        *,
        scheduler_id: str,
        max_runs: int,
        backend_limits: dict[str, int] | None = None,
        project_limits: dict[str, int] | None = None,
        provider_limits: dict[str, int] | None = None,
        model_limits: dict[str, int] | None = None,
        agent_limits: dict[str, int] | None = None,
        benchmark_limits: dict[str, int] | None = None,
        request_id: str | None = None,
    ) -> DispatchQueuedRunsResult:
        _require_non_empty("scheduler_id", scheduler_id)
        if max_runs <= 0:
            return DispatchQueuedRunsResult(dispatched_runs=[], capacity_blocked_runs=[])
        dispatch_lock = _acquire_scheduler_dispatch_lock(self.session)
        try:
            return self._dispatch_queued_runs_with_diagnostics_locked(
                scheduler_id=scheduler_id,
                max_runs=max_runs,
                backend_limits=backend_limits,
                project_limits=project_limits,
                provider_limits=provider_limits,
                model_limits=model_limits,
                agent_limits=agent_limits,
                benchmark_limits=benchmark_limits,
                request_id=request_id,
            )
        finally:
            if dispatch_lock is not None:
                dispatch_lock.release()

    def _dispatch_queued_runs_with_diagnostics_locked(
        self,
        *,
        scheduler_id: str,
        max_runs: int,
        backend_limits: dict[str, int] | None = None,
        project_limits: dict[str, int] | None = None,
        provider_limits: dict[str, int] | None = None,
        model_limits: dict[str, int] | None = None,
        agent_limits: dict[str, int] | None = None,
        benchmark_limits: dict[str, int] | None = None,
        request_id: str | None = None,
    ) -> DispatchQueuedRunsResult:
        _require_non_empty("scheduler_id", scheduler_id)

        backend_limits = _positive_limits(backend_limits or {})
        project_limits = _positive_limits(project_limits or {})
        provider_limits = _positive_limits(provider_limits or {})
        model_limits = _positive_limits(model_limits or {})
        agent_limits = _positive_limits(agent_limits or {})
        benchmark_limits = _positive_limits(benchmark_limits or {})
        active_statuses = {
            RunStatus.DISPATCHED.value,
            RunStatus.PROVISIONING.value,
            RunStatus.RUNNING.value,
            RunStatus.EVALUATING.value,
        }
        active_rows = list(self.session.scalars(select(RunRow).where(RunRow.status.in_(active_statuses))))
        active_total = len(active_rows)
        remaining_global_capacity = max_runs - active_total

        active_by_backend: dict[str, int] = {}
        active_by_project: dict[str, int] = {}
        active_by_provider: dict[str, int] = {}
        active_by_model: dict[str, int] = {}
        active_by_agent: dict[str, int] = {}
        active_by_benchmark: dict[str, int] = {}
        for row in active_rows:
            _increment(active_by_backend, _backend_capacity_key(row))
            _increment(active_by_project, row.project_id)
            _increment(active_by_provider, _provider_capacity_key(row))
            _increment(active_by_model, _model_capacity_key(row))
            _increment(active_by_agent, _agent_capacity_key(row))
            _increment(active_by_benchmark, _benchmark_capacity_key(row))

        ranked_candidate_ids = list(
            self.session.scalars(_ranked_queued_run_id_query(limit=max(max_runs * 5, 25)))
        )
        if ranked_candidate_ids:
            locked_candidates = list(
                self.session.scalars(_queued_dispatch_candidate_lock_query(ranked_candidate_ids))
            )
            candidate_rank = {run_id: index for index, run_id in enumerate(ranked_candidate_ids)}
            candidates = sorted(
                locked_candidates,
                key=lambda row: candidate_rank.get(row.run_id, len(candidate_rank)),
            )
        else:
            candidates = []
        dispatched_ids: list[str] = []
        capacity_blocked: list[SchedulerCapacityBlock] = []
        for row in candidates:
            backend_key = _backend_capacity_key(row)
            provider_key = _provider_capacity_key(row)
            model_key = _model_capacity_key(row)
            agent_key = _agent_capacity_key(row)
            benchmark_key = _benchmark_capacity_key(row)
            if len(dispatched_ids) >= remaining_global_capacity:
                capacity_blocked.append(
                    self._record_scheduler_capacity_block(
                        row=row,
                        scheduler_id=scheduler_id,
                        dimension="global",
                        key="global",
                        active_count=active_total + len(dispatched_ids),
                        limit=max_runs,
                        reason="global capacity reached",
                        request_id=request_id,
                        backend_key=backend_key,
                        provider_key=provider_key,
                        model_key=model_key,
                        agent_key=agent_key,
                        benchmark_key=benchmark_key,
                    )
                )
                continue
            blocked_dimension = _first_capacity_blocker(
                (
                    ("backend", backend_key, active_by_backend, backend_limits, "backend capacity reached"),
                    ("project", row.project_id, active_by_project, project_limits, "project capacity reached"),
                    ("provider", provider_key, active_by_provider, provider_limits, "provider capacity reached"),
                    ("model", model_key, active_by_model, model_limits, "model capacity reached"),
                    ("agent", agent_key, active_by_agent, agent_limits, "agent capacity reached"),
                    (
                        "benchmark",
                        benchmark_key,
                        active_by_benchmark,
                        benchmark_limits,
                        "benchmark capacity reached",
                    ),
                )
            )
            if blocked_dimension is not None:
                dimension, key, active_count, limit, reason = blocked_dimension
                capacity_blocked.append(
                    self._record_scheduler_capacity_block(
                        row=row,
                        scheduler_id=scheduler_id,
                        dimension=dimension,
                        key=key,
                        active_count=active_count,
                        limit=limit,
                        reason=reason,
                        request_id=request_id,
                        backend_key=backend_key,
                        provider_key=provider_key,
                        model_key=model_key,
                        agent_key=agent_key,
                        benchmark_key=benchmark_key,
                    )
                )
                continue

            previous_status = RunStatus(row.status)
            run = self._run_record(row)
            run.transition_to(RunStatus.DISPATCHED)
            row.status = run.status.value
            row.updated_at = run.updated_at

            attempt = self._latest_attempt_row(run.run_id)
            attempt.status = run.status.value
            attempt.metadata_json = scheduler_lease_metadata(
                attempt.metadata_json,
                scheduler_id=scheduler_id,
                lease_status=SchedulerLeaseStatus.DISPATCHED,
                observed_at=run.updated_at,
                execution_task_id=attempt.attempt_id,
                backend_key=backend_key,
                project_id=row.project_id,
                provider_key=provider_key,
                model_key=model_key,
                agent_key=agent_key,
                benchmark_key=benchmark_key,
            )
            attempt.updated_at = utc_now()

            self._append_status_event(
                run_id=run.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.DISPATCHED,
                from_status=previous_status,
                to_status=RunStatus.DISPATCHED,
                request_id=request_id,
                metadata={
                    "scheduler_id": scheduler_id,
                    "execution_task_id": attempt.attempt_id,
                    "backend_key": backend_key,
                    "project_id": row.project_id,
                    "provider_key": provider_key,
                    "model_key": model_key,
                    "agent_key": agent_key,
                    "benchmark_key": benchmark_key,
                },
            )
            self._mark_dashboard_projection_dirty(run.run_id, reason="run_dispatched")
            _increment(active_by_backend, backend_key)
            _increment(active_by_project, row.project_id)
            _increment(active_by_provider, provider_key)
            _increment(active_by_model, model_key)
            _increment(active_by_agent, agent_key)
            _increment(active_by_benchmark, benchmark_key)
            dispatched_ids.append(run.run_id)

        self.session.flush()
        return DispatchQueuedRunsResult(
            dispatched_runs=[self.get_run(run_id) for run_id in dispatched_ids],
            capacity_blocked_runs=capacity_blocked,
        )

    def list_scheduler_capacity_blocks(
        self,
        *,
        project_ids: list[str] | None = None,
        limit: int = 25,
    ) -> list[SchedulerCapacityBlock]:
        if limit <= 0:
            return []
        if project_ids is not None and not project_ids:
            return []
        query = (
            select(RunRow)
            .where(RunRow.status == RunStatus.QUEUED.value)
            .order_by(RunRow.created_at, RunRow.run_id)
            .limit(max(limit * 5, limit))
        )
        if project_ids is not None:
            query = query.where(RunRow.project_id.in_(project_ids))

        blocks: list[SchedulerCapacityBlock] = []
        for row in self.session.scalars(query):
            attempt = self._latest_attempt_row(row.run_id)
            block = _scheduler_capacity_block_from_attempt(row, attempt)
            if block is None:
                continue
            blocks.append(block)
            if len(blocks) >= limit:
                break
        return blocks

    def _record_scheduler_capacity_block(
        self,
        *,
        row: RunRow,
        scheduler_id: str,
        dimension: str,
        key: str,
        active_count: int,
        limit: int,
        reason: str,
        request_id: str | None,
        backend_key: str,
        provider_key: str,
        model_key: str,
        agent_key: str,
        benchmark_key: str,
    ) -> SchedulerCapacityBlock:
        attempt = self._latest_attempt_row(row.run_id)
        observed_at = utc_now()
        block = SchedulerCapacityBlock(
            run_id=row.run_id,
            project_id=row.project_id,
            scheduler_id=scheduler_id,
            execution_task_id=attempt.attempt_id,
            dimension=dimension,
            key=key,
            active_count=active_count,
            limit=limit,
            reason=reason,
            observed_at=observed_at,
            backend_key=backend_key,
            provider_key=provider_key,
            model_key=model_key,
            agent_key=agent_key,
            benchmark_key=benchmark_key,
        )
        previous_signature = _capacity_block_signature_from_attempt(attempt)
        attempt.metadata_json = scheduler_capacity_blocked_metadata(attempt.metadata_json, block=block)
        attempt.updated_at = observed_at
        if previous_signature != _capacity_block_signature(block.to_dict()):
            self._append_status_event(
                run_id=row.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.SCHEDULER_CAPACITY_BLOCKED,
                from_status=RunStatus.QUEUED,
                to_status=RunStatus.QUEUED,
                reason=reason,
                request_id=request_id,
                metadata=block.to_dict(),
            )
        return block

    def requeue_stale_dispatched_runs(
        self,
        *,
        older_than: datetime,
        scheduler_id: str,
        max_runs: int,
        reason: str = "stale dispatched run expired",
        request_id: str | None = None,
    ) -> list[RunRecord]:
        _require_non_empty("scheduler_id", scheduler_id)
        if max_runs <= 0:
            return []
        if older_than.tzinfo is None:
            raise ValueError("older_than must be timezone-aware")

        candidates = self.session.scalars(
            select(RunRow)
            .where(RunRow.status == RunStatus.DISPATCHED.value)
            .where(RunRow.updated_at < older_than)
            .order_by(RunRow.updated_at, RunRow.run_id)
            .with_for_update(skip_locked=True)
            .limit(max_runs)
        )
        requeued_ids: list[str] = []
        for row in candidates:
            previous_status = RunStatus(row.status)
            run = self._run_record(row)
            run.transition_to(RunStatus.QUEUED)

            row.status = run.status.value
            row.failure_reason = None
            row.updated_at = run.updated_at

            attempt = self._latest_attempt_row(run.run_id)
            attempt.status = run.status.value
            attempt.failure_reason = None
            attempt.completed_at = None
            attempt.metadata_json = scheduler_lease_metadata(
                attempt.metadata_json,
                scheduler_id=scheduler_id,
                lease_status=SchedulerLeaseStatus.RECOVERED,
                observed_at=run.updated_at,
                execution_task_id=attempt.attempt_id,
                backend_key=_backend_capacity_key(row),
                project_id=row.project_id,
            )
            attempt.updated_at = utc_now()

            self._append_status_event(
                run_id=run.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.RECOVERED,
                from_status=previous_status,
                to_status=RunStatus.QUEUED,
                reason=reason,
                request_id=request_id,
                metadata=recovery_event_metadata(
                    RecoveryReasonCode.STALE_DISPATCHED,
                    scheduler_id=scheduler_id,
                    execution_task_id=attempt.attempt_id,
                    stale_before=older_than.isoformat(),
                ),
            )
            self._mark_dashboard_projection_dirty(run.run_id, reason=RecoveryReasonCode.STALE_DISPATCHED.value)
            requeued_ids.append(run.run_id)

        self.session.flush()
        return [self.get_run(run_id) for run_id in requeued_ids]

    def record_worker_heartbeat(
        self,
        run_id: str,
        *,
        worker_id: str,
        status: RunStatus | str,
        execution_task_id: str | None = None,
        request_id: str | None = None,
    ) -> RunRecord:
        del request_id
        _require_non_empty("worker_id", worker_id)
        row = self._run_row_for_update(run_id)
        attempt = self._latest_attempt_row(run_id)
        self._validate_execution_task_row(
            row,
            attempt,
            execution_task_id=execution_task_id,
            worker_id=worker_id,
        )
        run_status = RunStatus(row.status)
        if run_status not in {RunStatus.PROVISIONING, RunStatus.RUNNING, RunStatus.EVALUATING}:
            raise ValueError("worker heartbeats can only be recorded for active runs")
        heartbeat_status = _coerce_run_status(status)
        if heartbeat_status not in {RunStatus.PROVISIONING, RunStatus.RUNNING, RunStatus.EVALUATING}:
            raise ValueError("worker heartbeat status must be active")

        now = utc_now()
        attempt.metadata_json = _worker_attempt_metadata(
            attempt.metadata_json,
            worker_id=worker_id,
            process_status=RunnerProcessStatus.HEARTBEATING,
            heartbeat_status=heartbeat_status,
            now=now,
            claimed_at=None,
            completed_at=None,
        )
        attempt.updated_at = now
        row.updated_at = now
        self.session.flush()
        return self.get_run(run_id)

    def current_execution_task_id(self, run_id: str) -> str:
        return self._latest_attempt_row(run_id).attempt_id

    def record_worker_subprocess_event(
        self,
        run_id: str,
        *,
        event_type: RunEventType,
        worker_id: str,
        execution_task_id: str,
        request_id: str | None = None,
        child_entrypoint: str | None = None,
        timeout_seconds: int | None = None,
        return_code: int | None = None,
    ) -> None:
        _require_non_empty("worker_id", worker_id)
        _require_non_empty("execution_task_id", execution_task_id)
        row = self._run_row_for_update(run_id)
        attempt = self._latest_attempt_row(run_id)
        self._validate_execution_task_row(
            row,
            attempt,
            execution_task_id=execution_task_id,
            worker_id=worker_id,
        )
        current_status = RunStatus(row.status)
        metadata = {
            "worker_id": worker_id,
            "execution_task_id": execution_task_id,
            **({} if child_entrypoint is None else {"child_entrypoint": child_entrypoint}),
            **({} if timeout_seconds is None else {"timeout_seconds": timeout_seconds}),
            **({} if return_code is None else {"return_code": return_code}),
        }
        self._append_status_event(
            run_id=run_id,
            attempt_id=attempt.attempt_id,
            event_type=event_type,
            from_status=current_status,
            to_status=current_status,
            request_id=request_id,
            metadata=metadata,
        )
        self.session.flush()

    def validate_current_execution_task(
        self,
        run_id: str,
        *,
        worker_id: str,
        execution_task_id: str,
    ) -> RunRecord:
        _require_non_empty("worker_id", worker_id)
        row = self._run_row_for_update(run_id)
        attempt = self._latest_attempt_row(run_id)
        self._validate_execution_task_row(
            row,
            attempt,
            execution_task_id=execution_task_id,
            worker_id=worker_id,
        )
        if RunStatus(row.status) not in {RunStatus.PROVISIONING, RunStatus.RUNNING, RunStatus.EVALUATING}:
            raise StaleExecutionTaskError(
                f"stale execution task {execution_task_id}: run {run_id} is {row.status}"
            )
        return self._run_record(row)

    def acquire_execution_task_lock(
        self,
        run_id: str,
        *,
        worker_id: str,
        execution_task_id: str,
        request_id: str | None = None,
    ) -> RunRecord:
        del request_id
        _require_non_empty("worker_id", worker_id)
        row = self._run_row_for_update(run_id)
        attempt = self._latest_attempt_row(run_id)
        self._validate_execution_task_row(
            row,
            attempt,
            execution_task_id=execution_task_id,
            worker_id=worker_id,
        )
        if RunStatus(row.status) not in {RunStatus.PROVISIONING, RunStatus.RUNNING, RunStatus.EVALUATING}:
            raise StaleExecutionTaskError(
                f"stale execution task {execution_task_id}: run {run_id} is {row.status}"
            )

        runner = _runner_metadata(attempt.metadata_json)
        existing_lock_id = runner.get("execution_lock_id")
        if isinstance(existing_lock_id, str) and existing_lock_id.strip():
            raise DuplicateExecutionTaskError(
                f"execution task {execution_task_id} is already executing"
            )

        now = utc_now()
        attempt.metadata_json = runner_process_metadata(
            attempt.metadata_json,
            worker_id=worker_id,
            process_status=RunnerProcessStatus.EXECUTING,
            heartbeat_status=row.status,
            observed_at=now,
            execution_lock_id=execution_task_id,
            execution_lock_acquired_at=now,
        )
        attempt.updated_at = now
        row.updated_at = now
        self.session.flush()
        return self.get_run(run_id)

    def fail_stale_active_runs_by_heartbeat(
        self,
        *,
        older_than: datetime,
        scheduler_id: str,
        max_runs: int,
        reason: str = "stale worker heartbeat expired",
        request_id: str | None = None,
    ) -> list[RunRecord]:
        _require_non_empty("scheduler_id", scheduler_id)
        if max_runs <= 0:
            return []
        if older_than.tzinfo is None:
            raise ValueError("older_than must be timezone-aware")

        active_statuses = {
            RunStatus.PROVISIONING.value,
            RunStatus.RUNNING.value,
            RunStatus.EVALUATING.value,
        }
        candidates = self.session.scalars(
            select(RunRow)
            .where(RunRow.status.in_(active_statuses))
            .order_by(RunRow.updated_at, RunRow.run_id)
            .with_for_update(skip_locked=True)
            .limit(max(max_runs * 5, 25))
        )
        failed_ids: list[str] = []
        for row in candidates:
            if len(failed_ids) >= max_runs:
                break
            attempt = self._latest_attempt_row(row.run_id)
            worker_metadata = _worker_metadata(attempt.metadata_json)
            last_heartbeat_at = _parse_worker_datetime(worker_metadata.get("last_heartbeat_at"))
            if last_heartbeat_at is None or last_heartbeat_at >= older_than:
                continue

            previous_status = RunStatus(row.status)
            run = self._run_record(row)
            run.transition_to(RunStatus.FAILED)
            run.failure_reason = reason

            row.status = run.status.value
            row.failure_reason = run.failure_reason
            row.updated_at = run.updated_at

            attempt.status = run.status.value
            attempt.failure_reason = run.failure_reason
            attempt.completed_at = run.updated_at
            attempt.metadata_json = _worker_attempt_metadata(
                attempt.metadata_json,
                worker_id=str(worker_metadata.get("worker_id") or ""),
                process_status=RunnerProcessStatus.FAILED,
                heartbeat_status=previous_status,
                now=run.updated_at,
                claimed_at=None,
                completed_at=run.updated_at,
            )
            attempt.updated_at = utc_now()

            self._append_status_event(
                run_id=run.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.RECOVERED,
                from_status=previous_status,
                to_status=RunStatus.FAILED,
                reason=reason,
                request_id=request_id,
                metadata=recovery_event_metadata(
                    RecoveryReasonCode.STALE_WORKER_HEARTBEAT,
                    scheduler_id=scheduler_id,
                    execution_task_id=attempt.attempt_id,
                    worker_id=worker_metadata.get("worker_id"),
                    stale_before=older_than.isoformat(),
                    last_heartbeat_at=worker_metadata.get("last_heartbeat_at"),
                ),
            )
            self._upsert_dashboard_projection(
                run_id=run.run_id,
                refresh_reason="terminal_worker_recovery",
            )
            failed_ids.append(run.run_id)

        self.session.flush()
        return [self.get_run(run_id) for run_id in failed_ids]

    def recover_terminal_result_mismatches(
        self,
        *,
        scheduler_id: str,
        max_runs: int,
        reason: str = "terminal worker result did not persist to run state",
        request_id: str | None = None,
    ) -> list[RunRecord]:
        _require_non_empty("scheduler_id", scheduler_id)
        if max_runs <= 0:
            return []

        active_statuses = {
            RunStatus.PROVISIONING.value,
            RunStatus.RUNNING.value,
            RunStatus.EVALUATING.value,
        }
        terminal_process_statuses = {
            RunnerProcessStatus.COMPLETED.value,
            RunnerProcessStatus.FAILED.value,
            RunnerProcessStatus.CANCELED.value,
        }
        candidates = self.session.scalars(
            select(RunRow)
            .where(RunRow.status.in_(active_statuses))
            .order_by(RunRow.updated_at, RunRow.run_id)
            .with_for_update(skip_locked=True)
            .limit(max(max_runs * 5, 25))
        )
        failed_ids: list[str] = []
        for row in candidates:
            if len(failed_ids) >= max_runs:
                break
            attempt = self._latest_attempt_row(row.run_id)
            runner_metadata = _runner_metadata(attempt.metadata_json)
            runner_process_status = str(runner_metadata.get("process_status") or "").strip()
            if runner_process_status not in terminal_process_statuses:
                continue

            previous_status = RunStatus(row.status)
            run = self._run_record(row)
            run.transition_to(RunStatus.FAILED)
            run.failure_reason = reason

            row.status = run.status.value
            row.failure_reason = run.failure_reason
            row.updated_at = run.updated_at

            worker_id = str(runner_metadata.get("worker_id") or "")
            attempt.status = run.status.value
            attempt.failure_reason = run.failure_reason
            attempt.completed_at = run.updated_at
            attempt.metadata_json = _worker_attempt_metadata(
                attempt.metadata_json,
                worker_id=worker_id,
                process_status=RunnerProcessStatus.FAILED,
                heartbeat_status=previous_status,
                now=run.updated_at,
                claimed_at=None,
                completed_at=run.updated_at,
            )
            attempt.updated_at = utc_now()

            self._append_status_event(
                run_id=run.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.RECOVERED,
                from_status=previous_status,
                to_status=RunStatus.FAILED,
                reason=reason,
                request_id=request_id,
                metadata=recovery_event_metadata(
                    RecoveryReasonCode.TERMINAL_RESULT_MISMATCH,
                    scheduler_id=scheduler_id,
                    execution_task_id=attempt.attempt_id,
                    worker_id=runner_metadata.get("worker_id"),
                    runner_process_status=runner_process_status,
                    runner_heartbeat_status=runner_metadata.get("heartbeat_status"),
                    runner_completed_at=runner_metadata.get("completed_at"),
                    runner_return_code=runner_metadata.get("return_code"),
                ),
            )
            self._upsert_dashboard_projection(
                run_id=run.run_id,
                refresh_reason="terminal_result_mismatch_recovery",
            )
            failed_ids.append(run.run_id)

        self.session.flush()
        return [self.get_run(run_id) for run_id in failed_ids]

    def claim_next_queued_run(
        self,
        *,
        worker_id: str,
        request_id: str | None = None,
    ) -> RunRecord | None:
        return self._claim_next_run(
            status=RunStatus.QUEUED,
            worker_id=worker_id,
            request_id=request_id,
        )

    def claim_queued_run(
        self,
        run_id: str,
        *,
        worker_id: str,
        request_id: str | None = None,
    ) -> RunRecord:
        _require_non_empty("run_id", run_id)
        _require_non_empty("worker_id", worker_id)
        row = self._run_row_for_update(run_id)
        if row.status != RunStatus.QUEUED.value:
            raise ValueError(f"run {run_id} is not queued")
        return self._claim_run_row(row, worker_id=worker_id, request_id=request_id)

    def claim_next_dispatched_run(
        self,
        *,
        worker_id: str,
        request_id: str | None = None,
    ) -> RunRecord | None:
        return self._claim_next_run(
            status=RunStatus.DISPATCHED,
            worker_id=worker_id,
            request_id=request_id,
        )

    def _claim_next_run(
        self,
        *,
        status: RunStatus,
        worker_id: str,
        request_id: str | None,
    ) -> RunRecord | None:
        _require_non_empty("worker_id", worker_id)
        row = self.session.scalar(
            select(RunRow)
            .where(RunRow.status == status.value)
            .order_by(RunRow.created_at, RunRow.run_id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if row is None:
            return None
        return self._claim_run_row(row, worker_id=worker_id, request_id=request_id)

    def _claim_run_row(
        self,
        row: RunRow,
        *,
        worker_id: str,
        request_id: str | None,
    ) -> RunRecord:
        previous_status = RunStatus(row.status)
        run = self._run_record(row)
        run.transition_to(RunStatus.PROVISIONING)

        row.status = run.status.value
        row.updated_at = run.updated_at

        attempt = self._latest_attempt_row(run.run_id)
        attempt.status = run.status.value
        attempt.metadata_json = _worker_attempt_metadata(
            attempt.metadata_json,
            worker_id=worker_id,
            process_status=RunnerProcessStatus.CLAIMED,
            heartbeat_status=run.status,
            now=run.updated_at,
            claimed_at=run.updated_at,
            completed_at=None,
        )
        attempt.updated_at = utc_now()

        self._append_status_event(
            run_id=run.run_id,
            attempt_id=attempt.attempt_id,
            event_type=RunEventType.CLAIMED,
            from_status=previous_status,
            to_status=RunStatus.PROVISIONING,
            request_id=request_id,
            metadata={"worker_id": worker_id, "execution_task_id": attempt.attempt_id},
        )
        self._mark_dashboard_projection_dirty(run.run_id, reason="run_claimed")
        self.session.flush()
        return self.get_run(run.run_id)

    def save_worker_result(
        self,
        run: RunRecord,
        *,
        worker_id: str,
        execution_task_id: str | None = None,
        request_id: str | None = None,
    ) -> RunRecord:
        _require_non_empty("worker_id", worker_id)
        row = self._run_row_for_update(run.run_id)
        attempt = self._latest_attempt_row(run.run_id)
        self._validate_execution_task_row(
            row,
            attempt,
            execution_task_id=execution_task_id,
            worker_id=worker_id,
        )
        previous_status = RunStatus(row.status)
        if previous_status is RunStatus.CANCELED:
            return self.get_run(run.run_id)
        if previous_status not in {RunStatus.PROVISIONING, RunStatus.RUNNING, RunStatus.EVALUATING}:
            raise ValueError("worker results can only be saved for a claimed active run")
        if run.status not in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}:
            raise ValueError("worker result must be terminal")

        self.save_run(run)
        attempt = self._latest_attempt_row(run.run_id)
        attempt.metadata_json = _worker_attempt_metadata(
            attempt.metadata_json,
            worker_id=worker_id,
            process_status=_terminal_runner_process_status(run.status),
            heartbeat_status=run.status,
            now=run.updated_at,
            claimed_at=None,
            completed_at=run.updated_at,
        )
        attempt.updated_at = utc_now()
        self._append_worker_lifecycle_events(
            run=run,
            attempt_id=attempt.attempt_id,
            from_status=previous_status,
            worker_id=worker_id,
            request_id=request_id,
        )
        self.session.flush()
        self._upsert_dashboard_projection(
            run_id=run.run_id,
            refresh_reason="terminal_worker_result",
        )
        return self.get_run(run.run_id)

    def save_run(self, run: RunRecord) -> None:
        existing = self.session.get(RunRow, run.run_id)
        owner_team_id = self._team_id_for_name(run.owner_team)
        if existing is not None:
            run_row = existing
            attempt_number = self._latest_attempt_row(run.run_id).attempt_number
            attempt_id = _attempt_id(run.run_id, attempt_number)
            self._delete_run_snapshot_children(run.run_id, attempt_id=attempt_id)
            self.session.flush()
        else:
            run_row = RunRow(run_id=run.run_id)
            self.session.add(run_row)
            attempt_number = 1
            attempt_id = _attempt_id(run.run_id, attempt_number)
        self._apply_run_fields(run_row, run, owner_team_id=owner_team_id)

        attempt = self.session.get(RunAttemptRow, attempt_id)
        if attempt is None:
            attempt = RunAttemptRow(attempt_id=attempt_id, run_id=run.run_id, attempt_number=attempt_number)
            self.session.add(attempt)
        self._apply_attempt_fields(attempt, run)

        for turn in run.trajectory:
            self.session.add(_turn_row(run_id=run.run_id, attempt_id=attempt_id, turn=turn))

        for index, artifact in enumerate(run.artifacts):
            self.session.add(
                _artifact_row(
                    run_id=run.run_id,
                    attempt_id=attempt_id,
                    artifact=artifact,
                    index=index,
                    artifact_id=self._artifact_id_for_attempt(artifact.artifact_id, attempt_id=attempt_id),
                )
            )

        for evaluator_result in run.all_evaluator_results():
            self.session.add(_evaluator_result_row(run_id=run.run_id, attempt_id=attempt_id, result=evaluator_result))

        self.session.flush()

    def _delete_run_snapshot_children(self, run_id: str, *, attempt_id: str) -> None:
        self.session.execute(
            delete(EvaluatorResultRow).where(
                EvaluatorResultRow.run_id == run_id,
                EvaluatorResultRow.attempt_id == attempt_id,
            )
        )
        self.session.execute(
            delete(ArtifactChunkRow).where(
                ArtifactChunkRow.run_id == run_id,
                ArtifactChunkRow.attempt_id == attempt_id,
            )
        )
        self.session.execute(
            delete(ArtifactRow).where(
                ArtifactRow.run_id == run_id,
                ArtifactRow.attempt_id == attempt_id,
            )
        )
        self.session.execute(
            delete(RunTerminalTurnRow).where(
                RunTerminalTurnRow.run_id == run_id,
                RunTerminalTurnRow.attempt_id == attempt_id,
            )
        )

    def _apply_run_fields(self, row: RunRow, run: RunRecord, *, owner_team_id: str | None) -> None:
        row.project_id = run.project_id
        row.created_by_user_id = run.created_by_user_id
        row.owner_team_id = owner_team_id
        row.owner_team_name_snapshot = run.owner_team
        row.benchmark_suite = run.task.benchmark_suite
        row.benchmark_version = run.task.benchmark_version
        row.task_family = run.task.task_family
        row.task_instance_id = run.task.instance_id
        row.task_source_uri = run.task.source_uri
        row.task_input_artifact_refs = list(run.task.input_artifact_refs)
        row.task_required_artifacts = list(run.task.required_artifacts)
        row.task_metadata = dict(run.task.metadata)
        row.model_provider = run.model.provider
        row.model_name = run.model.model_name
        row.model_mode = run.model.mode.value
        row.prompt_template_version = run.model.prompt_template_version
        row.model_version = run.model.model_version
        row.model_metadata = dict(run.model.metadata)
        row.runner_kind = run.runner.kind.value
        row.sandbox_backend = run.runner.sandbox_backend.value
        row.runner_image = run.runner.image
        row.runner_entrypoint = list(run.runner.entrypoint)
        row.runner_internet_access = run.runner.internet_access
        row.runner_resource_limits = dict(run.runner.resource_limits)
        row.runner_metadata = dict(run.runner.metadata)
        row.evaluator_configs = [_evaluator_config_payload(config) for config in run.evaluator_configs]
        row.status = run.status.value
        row.failure_reason = run.failure_reason
        row.metadata_json = dict(run.metadata)
        row.created_at = run.created_at
        row.updated_at = run.updated_at

    def _apply_attempt_fields(self, row: RunAttemptRow, run: RunRecord) -> None:
        row.status = run.status.value
        row.started_at = run.trajectory[0].started_at if run.trajectory else None
        row.completed_at = run.updated_at if run.status.value in {"succeeded", "failed", "canceled"} else None
        row.failure_reason = run.failure_reason
        row.metadata_json = dict(row.metadata_json or {})
        row.updated_at = utc_now()

    def get_run(self, run_id: str) -> RunRecord:
        row = _required(self.session.get(RunRow, run_id), "run", run_id)
        return self._run_record(row)

    def get_run_dashboard_summary(self, run_id: str) -> dict[str, Any]:
        row = _required(self.session.get(RunRow, run_id), "run", run_id)
        projection = self.session.get(RunDashboardProjectionRow, run_id)
        payload = _dashboard_summary_payload_from_projection(projection)
        if payload is not None:
            return payload
        return RunDashboardProjection.from_run(self._run_record(row)).to_dict()

    def list_runs(
        self,
        *,
        project_id: str | None = None,
        status: str | None = None,
        benchmark_suite: str | None = None,
        task_family: str | None = None,
        task_instance_id: str | None = None,
        created_by_user_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> list[RunRecord]:
        query = _apply_run_list_filters(
            select(RunRow).order_by(RunRow.created_at, RunRow.run_id),
            project_id=project_id,
            status=status,
            benchmark_suite=benchmark_suite,
            task_family=task_family,
            task_instance_id=task_instance_id,
            created_by_user_id=created_by_user_id,
            created_after=created_after,
            created_before=created_before,
        )
        return [self._run_record(row) for row in self.session.scalars(query)]

    def list_run_dashboard_summaries(
        self,
        *,
        project_ids: set[str] | None = None,
        project_id: str | None = None,
        status: str | None = None,
        benchmark_suite: str | None = None,
        task_family: str | None = None,
        task_instance_id: str | None = None,
        created_by_user_id: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
    ) -> list[dict[str, Any]]:
        if project_ids is not None and not project_ids:
            return []

        query = _apply_run_list_filters(
            select(RunRow, RunDashboardProjectionRow)
            .outerjoin(RunDashboardProjectionRow, RunDashboardProjectionRow.run_id == RunRow.run_id)
            .order_by(RunRow.created_at, RunRow.run_id),
            project_ids=project_ids,
            project_id=project_id,
            status=status,
            benchmark_suite=benchmark_suite,
            task_family=task_family,
            task_instance_id=task_instance_id,
            created_by_user_id=created_by_user_id,
            created_after=created_after,
            created_before=created_before,
        )

        summaries: list[dict[str, Any]] = []
        for row, projection in self.session.execute(query):
            payload = _dashboard_summary_payload_from_projection(projection)
            if payload is not None:
                summaries.append(payload)
            else:
                summaries.append(RunDashboardProjection.from_run(self._run_record(row)).to_dict())
        return summaries

    def list_dashboard_progress_records(
        self,
        *,
        project_ids: set[str] | None = None,
        project_id: str | None = None,
    ) -> list[RunDashboardProgressRecord]:
        if project_ids is not None and not project_ids:
            return []

        query = (
            select(RunRow, RunDashboardProjectionRow)
            .outerjoin(RunDashboardProjectionRow, RunDashboardProjectionRow.run_id == RunRow.run_id)
            .order_by(RunRow.created_at, RunRow.run_id)
        )
        if project_id is not None:
            query = query.where(RunRow.project_id == project_id)
        if project_ids is not None:
            query = query.where(RunRow.project_id.in_(project_ids))

        records: list[RunDashboardProgressRecord] = []
        for row, projection in self.session.execute(query):
            if projection is not None and not projection.dirty:
                records.append(_dashboard_progress_record_from_projection(row, projection))
            else:
                records.append(_dashboard_progress_record_from_run(self._run_record(row)))
        return records

    def list_status_events(
        self,
        run_id: str,
        *,
        after_seq: int | None = None,
        limit: int | None = None,
    ) -> list[RunStatusEvent]:
        _required(self.session.get(RunRow, run_id), "run", run_id)
        if after_seq is not None and after_seq < 0:
            raise ValueError("after_seq must be non-negative")
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")
        query = (
            select(RunStatusEventRow)
            .where(RunStatusEventRow.run_id == run_id)
            .order_by(RunStatusEventRow.id)
        )
        if after_seq is not None:
            query = query.where(RunStatusEventRow.id > after_seq)
        if limit is not None:
            query = query.limit(limit)
        return [_status_event_record(row) for row in self.session.scalars(query)]

    def get_dashboard_projection(self, run_id: str) -> RunDashboardProjectionRow:
        row = self.session.get(RunDashboardProjectionRow, run_id)
        return _required(row, "run dashboard projection", run_id)

    def refresh_dashboard_projection(
        self,
        run_id: str,
        *,
        refresh_reason: str,
        request_id: str | None = None,
    ) -> RunDashboardProjectionRow:
        del request_id
        _require_non_empty("refresh_reason", refresh_reason)
        row = self._upsert_dashboard_projection(
            run_id=run_id,
            refresh_reason=refresh_reason,
        )
        self.session.flush()
        return row

    def refresh_terminal_dashboard_projections(
        self,
        *,
        scheduler_id: str,
        max_runs: int,
        request_id: str | None = None,
    ) -> list[RunDashboardProjectionRow]:
        _require_non_empty("scheduler_id", scheduler_id)
        if max_runs <= 0:
            return []

        terminal_rows = self.session.scalars(
            select(RunRow)
            .where(RunRow.status.in_(_TERMINAL_STATUS_VALUES))
            .order_by(RunRow.updated_at, RunRow.run_id)
            .with_for_update(skip_locked=True)
            .limit(max(max_runs * 5, 25))
        )
        refreshed: list[RunDashboardProjectionRow] = []
        for row in terminal_rows:
            if len(refreshed) >= max_runs:
                break
            projection = self.session.get(RunDashboardProjectionRow, row.run_id)
            if projection is not None and not _dashboard_projection_needs_refresh(projection, row):
                continue
            projection_was_missing = projection is None
            projection_was_dirty = bool(projection.dirty) if projection is not None else False
            projection_status_before = projection.status if projection is not None else None
            projection_source_event_seq_before = (
                projection.source_event_seq if projection is not None else None
            )
            refreshed_projection = self._upsert_dashboard_projection(
                run_id=row.run_id,
                refresh_reason="projection_recovery",
            )
            refreshed.append(refreshed_projection)
            attempt = self._latest_attempt_row(row.run_id)
            run_status = RunStatus(row.status)
            self._append_status_event(
                run_id=row.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.PROJECTION_REFRESHED,
                from_status=run_status,
                to_status=run_status,
                reason="terminal dashboard projection refreshed",
                request_id=request_id,
                metadata={
                    "scheduler_id": scheduler_id,
                    "execution_task_id": attempt.attempt_id,
                    "refresh_reason": refreshed_projection.refresh_reason,
                    "projection_missing_before_refresh": projection_was_missing,
                    "projection_dirty_before_refresh": projection_was_dirty,
                    "projection_status_before_refresh": projection_status_before,
                    "projection_source_event_seq_before_refresh": projection_source_event_seq_before,
                    "source_event_seq": refreshed_projection.source_event_seq,
                },
            )

        self.session.flush()
        return refreshed

    def record_artifact_chunk(self, chunk: ArtifactChunkMetadata) -> ArtifactChunkMetadata:
        run_row, _, _ = self._require_artifact_chunk_parents(
            run_id=chunk.run_id,
            attempt_id=chunk.attempt_id,
            artifact_id=chunk.artifact_id,
        )

        chunk_kind = _artifact_chunk_kind_value(chunk.chunk_kind)
        upload_status = _artifact_upload_status_value(chunk.upload_status)
        existing = self._artifact_chunk_row_for_update(
            artifact_id=chunk.artifact_id,
            chunk_kind=chunk_kind,
            chunk_sequence=chunk.chunk_sequence,
        )
        previous_upload_status = existing.upload_status if existing is not None else None
        now = utc_now()
        if existing is None:
            existing = ArtifactChunkRow(
                run_id=chunk.run_id,
                attempt_id=chunk.attempt_id,
                artifact_id=chunk.artifact_id,
                chunk_kind=chunk_kind,
                chunk_sequence=chunk.chunk_sequence,
                created_at=chunk.created_at,
            )
            self.session.add(existing)

        existing.storage_key = chunk.storage_key
        existing.media_type = chunk.media_type
        existing.size_bytes = chunk.size_bytes
        existing.sha256 = chunk.sha256
        existing.upload_status = upload_status
        existing.upload_error_reason = chunk.upload_error_reason
        existing.metadata_json = dict(chunk.metadata)
        existing.updated_at = now
        run_status = RunStatus(run_row.status)
        self._append_status_event(
            run_id=chunk.run_id,
            attempt_id=chunk.attempt_id,
            event_type=_artifact_chunk_event_type(chunk_kind),
            from_status=run_status,
            to_status=run_status,
            reason=chunk.upload_error_reason,
            metadata=_artifact_chunk_event_metadata(chunk, upload_status=upload_status),
        )
        if previous_upload_status is not None and previous_upload_status != upload_status:
            self._append_status_event(
                run_id=chunk.run_id,
                attempt_id=chunk.attempt_id,
                event_type=RunEventType.ARTIFACT_UPLOAD_STATUS_CHANGED,
                from_status=run_status,
                to_status=run_status,
                reason=chunk.upload_error_reason,
                metadata=_artifact_chunk_upload_transition_event_metadata(
                    chunk,
                    previous_upload_status=previous_upload_status,
                    upload_status=upload_status,
                ),
            )
        self.session.flush()
        return _artifact_chunk_metadata(existing)

    def start_artifact_chunk_upload(
        self,
        *,
        run_id: str,
        attempt_id: str,
        artifact_id: str,
        chunk_kind: ArtifactChunkKind | str,
        chunk_sequence: int,
        storage_key: str,
        media_type: str,
        started_at: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        request_id: str | None = None,
    ) -> ArtifactChunkMetadata:
        run_row, _, _ = self._require_artifact_chunk_parents(
            run_id=run_id,
            attempt_id=attempt_id,
            artifact_id=artifact_id,
        )
        now = utc_now()
        created_at = _aware(started_at or now)
        chunk = ArtifactChunkMetadata(
            run_id=run_id,
            attempt_id=attempt_id,
            artifact_id=artifact_id,
            chunk_kind=chunk_kind,
            chunk_sequence=chunk_sequence,
            storage_key=storage_key,
            media_type=media_type,
            size_bytes=None,
            sha256=None,
            upload_status=ArtifactUploadStatus.STARTED,
            created_at=created_at,
            metadata={
                **dict(metadata or {}),
                "upload_started_at": _datetime_json(created_at),
            },
        )
        kind = _artifact_chunk_kind_value(chunk.chunk_kind)
        existing = self._artifact_chunk_row_for_update(
            artifact_id=artifact_id,
            chunk_kind=kind,
            chunk_sequence=chunk_sequence,
        )
        if existing is None:
            row = ArtifactChunkRow(
                run_id=run_id,
                attempt_id=attempt_id,
                artifact_id=artifact_id,
                chunk_kind=kind,
                chunk_sequence=chunk_sequence,
                storage_key=storage_key,
                media_type=media_type,
                size_bytes=None,
                sha256=None,
                upload_status=ArtifactUploadStatus.STARTED.value,
                upload_error_reason=None,
                metadata_json=dict(chunk.metadata),
                created_at=created_at,
                updated_at=now,
            )
            self.session.add(row)
            run_status = RunStatus(run_row.status)
            self._append_status_event(
                run_id=run_id,
                attempt_id=attempt_id,
                event_type=_artifact_chunk_event_type(kind),
                from_status=run_status,
                to_status=run_status,
                metadata=_artifact_chunk_event_metadata(
                    chunk,
                    upload_status=ArtifactUploadStatus.STARTED.value,
                ),
                request_id=request_id,
            )
            self.session.flush()
            return _artifact_chunk_metadata(row)

        self._ensure_artifact_chunk_upload_identity(existing, storage_key=storage_key, media_type=media_type)
        previous_upload_status = existing.upload_status
        if previous_upload_status == ArtifactUploadStatus.STARTED.value:
            return _artifact_chunk_metadata(existing)
        if previous_upload_status == ArtifactUploadStatus.COMPLETED.value:
            raise ValueError("completed artifact chunk upload cannot be restarted")

        existing.size_bytes = None
        existing.sha256 = None
        existing.upload_status = ArtifactUploadStatus.STARTED.value
        existing.upload_error_reason = None
        existing.metadata_json = {
            **dict(existing.metadata_json or {}),
            **dict(chunk.metadata),
        }
        existing.updated_at = now
        run_status = RunStatus(run_row.status)
        self._append_status_event(
            run_id=run_id,
            attempt_id=attempt_id,
            event_type=RunEventType.ARTIFACT_UPLOAD_STATUS_CHANGED,
            from_status=run_status,
            to_status=run_status,
            reason=None,
            request_id=request_id,
            metadata=_artifact_chunk_upload_transition_event_metadata(
                _artifact_chunk_metadata(existing),
                previous_upload_status=previous_upload_status,
                upload_status=ArtifactUploadStatus.STARTED.value,
            ),
        )
        self.session.flush()
        return _artifact_chunk_metadata(existing)

    def complete_artifact_chunk_upload(
        self,
        *,
        run_id: str,
        artifact_id: str,
        chunk_kind: ArtifactChunkKind | str,
        chunk_sequence: int,
        size_bytes: int,
        sha256: str,
        completed_at: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        request_id: str | None = None,
    ) -> ArtifactChunkMetadata:
        run_row = _required(self.session.get(RunRow, run_id), "run", run_id)
        kind = _artifact_chunk_kind_value(chunk_kind)
        row = self._required_artifact_chunk_row_for_update(
            run_id=run_id,
            artifact_id=artifact_id,
            chunk_kind=kind,
            chunk_sequence=chunk_sequence,
        )
        completed = _aware(completed_at or utc_now())
        previous_upload_status = row.upload_status
        if previous_upload_status == ArtifactUploadStatus.COMPLETED.value:
            if row.size_bytes == size_bytes and row.sha256 == sha256:
                return _artifact_chunk_metadata(row)
            raise ValueError("completed artifact chunk upload object metadata cannot change")
        if previous_upload_status in {ArtifactUploadStatus.FAILED.value, ArtifactUploadStatus.EXPIRED.value}:
            raise ValueError(f"artifact chunk upload cannot complete from {previous_upload_status}")

        row.size_bytes = size_bytes
        row.sha256 = sha256
        row.upload_status = ArtifactUploadStatus.COMPLETED.value
        row.upload_error_reason = None
        row.metadata_json = {
            **dict(row.metadata_json or {}),
            **dict(metadata or {}),
            "upload_completed_at": _datetime_json(completed),
        }
        row.updated_at = utc_now()
        chunk = _artifact_chunk_metadata(row)
        run_status = RunStatus(run_row.status)
        self._append_status_event(
            run_id=run_id,
            attempt_id=row.attempt_id,
            event_type=RunEventType.ARTIFACT_UPLOAD_STATUS_CHANGED,
            from_status=run_status,
            to_status=run_status,
            request_id=request_id,
            metadata=_artifact_chunk_upload_transition_event_metadata(
                chunk,
                previous_upload_status=previous_upload_status,
                upload_status=ArtifactUploadStatus.COMPLETED.value,
            ),
        )
        self.session.flush()
        return chunk

    def fail_artifact_chunk_upload(
        self,
        *,
        run_id: str,
        artifact_id: str,
        chunk_kind: ArtifactChunkKind | str,
        chunk_sequence: int,
        reason: str,
        failed_at: datetime | None = None,
        metadata: dict[str, Any] | None = None,
        request_id: str | None = None,
    ) -> ArtifactChunkMetadata:
        _require_non_empty("reason", reason)
        run_row = _required(self.session.get(RunRow, run_id), "run", run_id)
        kind = _artifact_chunk_kind_value(chunk_kind)
        row = self._required_artifact_chunk_row_for_update(
            run_id=run_id,
            artifact_id=artifact_id,
            chunk_kind=kind,
            chunk_sequence=chunk_sequence,
        )
        failed = _aware(failed_at or utc_now())
        previous_upload_status = row.upload_status
        if previous_upload_status == ArtifactUploadStatus.FAILED.value and row.upload_error_reason == reason:
            return _artifact_chunk_metadata(row)
        if previous_upload_status == ArtifactUploadStatus.COMPLETED.value:
            raise ValueError("completed artifact chunk upload cannot be failed")

        row.size_bytes = None
        row.sha256 = None
        row.upload_status = ArtifactUploadStatus.FAILED.value
        row.upload_error_reason = reason
        row.metadata_json = {
            **dict(row.metadata_json or {}),
            **dict(metadata or {}),
            "upload_failed_at": _datetime_json(failed),
        }
        row.updated_at = utc_now()
        chunk = _artifact_chunk_metadata(row)
        run_status = RunStatus(run_row.status)
        self._append_status_event(
            run_id=run_id,
            attempt_id=row.attempt_id,
            event_type=RunEventType.ARTIFACT_UPLOAD_STATUS_CHANGED,
            from_status=run_status,
            to_status=run_status,
            reason=reason,
            request_id=request_id,
            metadata=_artifact_chunk_upload_transition_event_metadata(
                chunk,
                previous_upload_status=previous_upload_status,
                upload_status=ArtifactUploadStatus.FAILED.value,
            ),
        )
        self.session.flush()
        return chunk

    def _require_artifact_chunk_parents(
        self,
        *,
        run_id: str,
        attempt_id: str,
        artifact_id: str,
    ) -> tuple[RunRow, RunAttemptRow, ArtifactRow]:
        run_row = _required(self.session.get(RunRow, run_id), "run", run_id)
        attempt = _required(self.session.get(RunAttemptRow, attempt_id), "run attempt", attempt_id)
        artifact = _required(self.session.get(ArtifactRow, artifact_id), "artifact", artifact_id)
        if attempt.run_id != run_id:
            raise ValueError("chunk attempt_id does not belong to run_id")
        if artifact.run_id != run_id:
            raise ValueError("chunk artifact_id does not belong to run_id")
        if artifact.attempt_id != attempt_id:
            raise ValueError("chunk artifact_id does not belong to attempt_id")
        return run_row, attempt, artifact

    def _artifact_chunk_row_for_update(
        self,
        *,
        artifact_id: str,
        chunk_kind: str,
        chunk_sequence: int,
    ) -> ArtifactChunkRow | None:
        return self.session.scalar(
            select(ArtifactChunkRow)
            .where(ArtifactChunkRow.artifact_id == artifact_id)
            .where(ArtifactChunkRow.chunk_kind == chunk_kind)
            .where(ArtifactChunkRow.chunk_sequence == chunk_sequence)
            .with_for_update()
        )

    def _required_artifact_chunk_row_for_update(
        self,
        *,
        run_id: str,
        artifact_id: str,
        chunk_kind: str,
        chunk_sequence: int,
    ) -> ArtifactChunkRow:
        row = self._artifact_chunk_row_for_update(
            artifact_id=artifact_id,
            chunk_kind=chunk_kind,
            chunk_sequence=chunk_sequence,
        )
        if row is None or row.run_id != run_id:
            raise KeyError(f"Unknown artifact chunk: {artifact_id}:{chunk_kind}:{chunk_sequence}")
        return row

    def _ensure_artifact_chunk_upload_identity(
        self,
        row: ArtifactChunkRow,
        *,
        storage_key: str,
        media_type: str,
    ) -> None:
        if row.storage_key != storage_key:
            raise ValueError("artifact chunk upload storage_key cannot change")
        if row.media_type != media_type:
            raise ValueError("artifact chunk upload media_type cannot change")

    def list_artifact_chunks(
        self,
        *,
        run_id: str,
        attempt_id: str | None = None,
        artifact_id: str | None = None,
        chunk_kind: ArtifactChunkKind | str | None = None,
        after_sequence: int | None = None,
        limit: int | None = None,
    ) -> list[ArtifactChunkMetadata]:
        _required(self.session.get(RunRow, run_id), "run", run_id)
        if after_sequence is not None and after_sequence < 0:
            raise ValueError("after_sequence must be non-negative")
        if limit is not None and limit <= 0:
            raise ValueError("limit must be positive")

        query = select(ArtifactChunkRow).where(ArtifactChunkRow.run_id == run_id)
        if attempt_id is not None:
            query = query.where(ArtifactChunkRow.attempt_id == attempt_id)
        if artifact_id is not None:
            query = query.where(ArtifactChunkRow.artifact_id == artifact_id)
        if chunk_kind is not None:
            query = query.where(ArtifactChunkRow.chunk_kind == _artifact_chunk_kind_value(chunk_kind))
        if after_sequence is not None:
            query = query.where(ArtifactChunkRow.chunk_sequence > after_sequence)
        query = query.order_by(
            ArtifactChunkRow.artifact_id,
            ArtifactChunkRow.chunk_kind,
            ArtifactChunkRow.chunk_sequence,
        )
        if limit is not None:
            query = query.limit(limit)
        return [_artifact_chunk_metadata(row) for row in self.session.scalars(query)]

    def get_artifact_chunk(
        self,
        *,
        run_id: str,
        artifact_id: str,
        chunk_kind: ArtifactChunkKind | str,
        chunk_sequence: int,
    ) -> ArtifactChunkMetadata:
        _required(self.session.get(RunRow, run_id), "run", run_id)
        if chunk_sequence < 0:
            raise ValueError("chunk_sequence must be non-negative")
        kind = _artifact_chunk_kind_value(chunk_kind)
        row = self.session.scalar(
            select(ArtifactChunkRow)
            .where(ArtifactChunkRow.run_id == run_id)
            .where(ArtifactChunkRow.artifact_id == artifact_id)
            .where(ArtifactChunkRow.chunk_kind == kind)
            .where(ArtifactChunkRow.chunk_sequence == chunk_sequence)
        )
        return _artifact_chunk_metadata(_required(row, "artifact chunk", f"{artifact_id}:{kind}:{chunk_sequence}"))

    def expire_stale_artifact_uploads(
        self,
        *,
        older_than: datetime,
        scheduler_id: str,
        max_artifacts: int,
        reason: str = "artifact upload expired",
        request_id: str | None = None,
    ) -> list[ExpiredArtifactUploadRecord]:
        _require_non_empty("scheduler_id", scheduler_id)
        if max_artifacts <= 0:
            return []
        if older_than.tzinfo is None:
            raise ValueError("older_than must be timezone-aware")

        pending_statuses = {
            ArtifactUploadStatus.PENDING.value,
            ArtifactUploadStatus.STARTED.value,
        }
        status_expr = ArtifactRow.metadata_json["upload_status"].as_string()
        candidates = self.session.scalars(
            select(ArtifactRow)
            .where(status_expr.in_(pending_statuses))
            .where(ArtifactRow.created_at < older_than)
            .order_by(ArtifactRow.created_at, ArtifactRow.artifact_id)
            .with_for_update(skip_locked=True)
            .limit(max(max_artifacts * 5, 25))
        )
        expired: list[ExpiredArtifactUploadRecord] = []
        refreshed_run_ids: set[str] = set()
        for artifact in candidates:
            if len(expired) >= max_artifacts:
                break
            metadata = dict(artifact.metadata_json or {})
            upload_status = str(metadata.get("upload_status") or "").strip()
            if upload_status not in pending_statuses:
                continue
            upload_started_at = _parse_metadata_datetime(metadata.get("upload_started_at")) or _aware(
                artifact.created_at
            )
            if upload_started_at >= older_than:
                continue

            now = utc_now()
            metadata["upload_status"] = ArtifactUploadStatus.EXPIRED.value
            metadata["upload_previous_status"] = upload_status
            metadata["upload_recovery"] = RecoveryReasonCode.ARTIFACT_UPLOAD_EXPIRED.value
            metadata["upload_recovery_scheduler_id"] = scheduler_id
            metadata["upload_expired_at"] = _datetime_json(now)
            metadata["upload_error_reason"] = reason
            artifact.metadata_json = metadata

            run_row = _required(self.session.get(RunRow, artifact.run_id), "run", artifact.run_id)
            attempt = self._latest_attempt_row(artifact.run_id)
            run_status = RunStatus(run_row.status)
            event_metadata = recovery_event_metadata(
                RecoveryReasonCode.ARTIFACT_UPLOAD_EXPIRED,
                scheduler_id=scheduler_id,
                artifact_id=artifact.artifact_id,
                execution_task_id=attempt.attempt_id,
                previous_upload_status=upload_status,
                upload_status=ArtifactUploadStatus.EXPIRED.value,
                stale_before=older_than.isoformat(),
                upload_started_at=_datetime_json(upload_started_at),
                expired_at=_datetime_json(now),
            )
            self._append_status_event(
                run_id=artifact.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.RECOVERED,
                from_status=run_status,
                to_status=run_status,
                reason=reason,
                request_id=request_id,
                metadata=event_metadata,
            )
            self._append_status_event(
                run_id=artifact.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.ARTIFACT_UPLOAD_EXPIRED,
                from_status=run_status,
                to_status=run_status,
                reason=reason,
                request_id=request_id,
                metadata=event_metadata,
            )
            self._append_status_event(
                run_id=artifact.run_id,
                attempt_id=attempt.attempt_id,
                event_type=RunEventType.ARTIFACT_UPLOAD_STATUS_CHANGED,
                from_status=run_status,
                to_status=run_status,
                reason=reason,
                request_id=request_id,
                metadata={
                    "artifact_id": artifact.artifact_id,
                    "execution_task_id": attempt.attempt_id,
                    "previous_upload_status": upload_status,
                    "upload_status": ArtifactUploadStatus.EXPIRED.value,
                    "transition_reason": reason,
                    "scheduler_id": scheduler_id,
                    "recovery": RecoveryReasonCode.ARTIFACT_UPLOAD_EXPIRED.value,
                    "stale_before": older_than.isoformat(),
                    "upload_started_at": _datetime_json(upload_started_at),
                    "expired_at": _datetime_json(now),
                },
            )
            expired.append(
                ExpiredArtifactUploadRecord(
                    artifact_id=artifact.artifact_id,
                    run_id=artifact.run_id,
                    attempt_id=artifact.attempt_id,
                    previous_upload_status=upload_status,
                    upload_status=ArtifactUploadStatus.EXPIRED.value,
                    scheduler_id=scheduler_id,
                    expired_at=now,
                )
            )
            refreshed_run_ids.add(artifact.run_id)

        for run_id in sorted(refreshed_run_ids):
            self._upsert_dashboard_projection(
                run_id=run_id,
                refresh_reason=RecoveryReasonCode.ARTIFACT_UPLOAD_EXPIRED.value,
            )

        self.session.flush()
        return expired

    def record_sandbox_container_cleanup(
        self,
        *,
        run_id: str,
        scheduler_id: str,
        cleanup_status: str,
        container_ids: list[str] | None = None,
        removed_container_ids: list[str] | None = None,
        list_exit_code: int | None = None,
        removal_exit_code: int | None = None,
        attempt_filter: str | None = None,
        cleanup_error_reason: str | None = None,
        reason: str = "owned Docker sandbox container cleanup",
        request_id: str | None = None,
    ) -> RunStatusEvent:
        _require_non_empty("run_id", run_id)
        _require_non_empty("scheduler_id", scheduler_id)
        _require_non_empty("cleanup_status", cleanup_status)

        row = _required(self.session.get(RunRow, run_id), "run", run_id)
        attempt = self._latest_attempt_row(run_id)
        run_status = RunStatus(row.status)
        metadata = recovery_event_metadata(
            RecoveryReasonCode.DOCKER_CONTAINER_CLEANUP,
            scheduler_id=scheduler_id,
            execution_task_id=attempt.attempt_id,
            cleanup_status=cleanup_status,
            container_ids=list(container_ids or []),
            removed_container_ids=list(removed_container_ids or []),
            container_count=len(container_ids or []),
            removed_container_count=len(removed_container_ids or []),
            list_exit_code=list_exit_code,
            removal_exit_code=removal_exit_code,
            attempt_filter=attempt_filter,
            cleanup_error_reason=cleanup_error_reason,
        )
        event = self._append_status_event(
            run_id=run_id,
            attempt_id=attempt.attempt_id,
            event_type=RunEventType.SANDBOX_CONTAINER_CLEANUP,
            from_status=run_status,
            to_status=run_status,
            reason=reason,
            request_id=request_id,
            metadata=metadata,
        )
        self.session.flush()
        return event

    def _run_record(self, row: RunRow) -> RunRecord:
        latest_attempt = self._latest_attempt_row(row.run_id)
        turns = [
            _terminal_turn(turn)
            for turn in self.session.scalars(
                select(RunTerminalTurnRow)
                .where(RunTerminalTurnRow.run_id == row.run_id)
                .where(RunTerminalTurnRow.attempt_id == latest_attempt.attempt_id)
                .order_by(RunTerminalTurnRow.turn_index)
            )
        ]
        artifacts = [
            _artifact_ref(artifact)
            for artifact in self.session.scalars(
                select(ArtifactRow)
                .where(ArtifactRow.run_id == row.run_id)
                .where(ArtifactRow.attempt_id == latest_attempt.attempt_id)
                .order_by(ArtifactRow.artifact_index)
            )
        ]
        evaluator_rows = list(
            self.session.scalars(
                select(EvaluatorResultRow)
                .where(EvaluatorResultRow.run_id == row.run_id)
                .where(EvaluatorResultRow.attempt_id == latest_attempt.attempt_id)
                .order_by(EvaluatorResultRow.id)
            )
        )
        evaluator_results = [_evaluator_result(evaluator_row) for evaluator_row in evaluator_rows]
        evaluator_result = evaluator_results[-1] if evaluator_results else None
        return RunRecord(
            run_id=row.run_id,
            project_id=row.project_id,
            owner_team=row.owner_team_name_snapshot,
            task=BenchmarkTaskInstance(
                benchmark_suite=row.benchmark_suite,
                benchmark_version=row.benchmark_version,
                task_family=row.task_family,
                instance_id=row.task_instance_id,
                source_uri=row.task_source_uri,
                input_artifact_refs=list(row.task_input_artifact_refs or []),
                required_artifacts=list(row.task_required_artifacts or []),
                metadata=dict(row.task_metadata or {}),
            ),
            model=ModelConfig(
                provider=row.model_provider,
                model_name=row.model_name,
                mode=row.model_mode,
                prompt_template_version=row.prompt_template_version,
                model_version=row.model_version,
                metadata=dict(row.model_metadata or {}),
            ),
            runner=RunnerConfig(
                kind=row.runner_kind,
                sandbox_backend=row.sandbox_backend,
                image=row.runner_image,
                entrypoint=list(row.runner_entrypoint or []),
                internet_access=row.runner_internet_access,
                resource_limits=dict(row.runner_resource_limits or {}),
                metadata=dict(row.runner_metadata or {}),
            ),
            status=row.status,
            created_at=_aware(row.created_at),
            updated_at=_aware(row.updated_at),
            trajectory=turns,
            artifacts=artifacts,
            evaluator_configs=[_evaluator_config(config) for config in row.evaluator_configs or []],
            evaluator_results=evaluator_results,
            evaluator_result=evaluator_result,
            failure_reason=row.failure_reason,
            created_by_user_id=row.created_by_user_id,
            metadata=dict(row.metadata_json or {}),
        )

    def _upsert_dashboard_projection(
        self,
        *,
        run_id: str,
        refresh_reason: str,
    ) -> RunDashboardProjectionRow:
        _require_non_empty("refresh_reason", refresh_reason)
        row = _required(self.session.get(RunRow, run_id), "run", run_id)
        run = self._run_record(row)
        attempt = self._latest_attempt_row(run_id)
        now = utc_now()
        projection = self.session.get(RunDashboardProjectionRow, run_id)
        if projection is None:
            projection = RunDashboardProjectionRow(
                run_id=run_id,
                project_id=row.project_id,
                owner_team_name_snapshot=row.owner_team_name_snapshot,
                status=row.status,
                is_terminal=row.status in _TERMINAL_STATUS_VALUES,
                payload={},
                refresh_reason=refresh_reason,
                dirty=False,
                refreshed_at=now,
                created_at=now,
                updated_at=now,
            )
            self.session.add(projection)

        projection.project_id = row.project_id
        projection.owner_team_name_snapshot = row.owner_team_name_snapshot
        projection.status = row.status
        projection.is_terminal = row.status in _TERMINAL_STATUS_VALUES
        projection.payload = RunDashboardProjection.from_run(run).to_dict()
        projection.source_attempt_id = attempt.attempt_id
        projection.source_event_seq = self._latest_status_event_seq(run_id)
        projection.refresh_reason = refresh_reason
        projection.dirty = False
        projection.error_reason = None
        projection.refreshed_at = now
        projection.updated_at = now
        return projection

    def _mark_dashboard_projection_dirty(self, run_id: str, *, reason: str) -> None:
        projection = self.session.get(RunDashboardProjectionRow, run_id)
        if projection is None:
            return
        projection.dirty = True
        projection.refresh_reason = reason
        projection.updated_at = utc_now()

    def _latest_status_event_seq(self, run_id: str) -> int | None:
        return self.session.scalar(
            select(RunStatusEventRow.id)
            .where(RunStatusEventRow.run_id == run_id)
            .order_by(RunStatusEventRow.id.desc())
            .limit(1)
        )

    def _team_id_for_name(self, team_name: str) -> str | None:
        row = self.session.scalar(select(TeamRow).where(TeamRow.name == team_name))
        return row.team_id if row is not None else None

    def _latest_attempt_row(self, run_id: str) -> RunAttemptRow:
        row = self.session.scalar(
            select(RunAttemptRow)
            .where(RunAttemptRow.run_id == run_id)
            .order_by(RunAttemptRow.attempt_number.desc())
            .limit(1)
        )
        return _required(row, "run attempt", run_id)

    def _artifact_id_for_attempt(self, artifact_id: str, *, attempt_id: str) -> str:
        existing = self.session.get(ArtifactRow, artifact_id)
        if existing is None or existing.attempt_id == attempt_id:
            return artifact_id
        suffix = f":attempt:{_attempt_number(attempt_id)}"
        return f"{artifact_id[:255 - len(suffix)]}{suffix}"

    def _run_row_for_update(self, run_id: str) -> RunRow:
        row = self.session.scalar(select(RunRow).where(RunRow.run_id == run_id).with_for_update())
        return _required(row, "run", run_id)

    def _validate_execution_task_row(
        self,
        row: RunRow,
        attempt: RunAttemptRow,
        *,
        execution_task_id: str | None,
        worker_id: str | None,
    ) -> None:
        if execution_task_id is None:
            return
        _require_non_empty("execution_task_id", execution_task_id)
        if attempt.attempt_id != execution_task_id:
            raise StaleExecutionTaskError(
                f"stale execution task {execution_task_id}: current task is {attempt.attempt_id}"
            )
        if worker_id is None:
            return
        worker_metadata = _worker_metadata(attempt.metadata_json)
        claimed_worker_id = worker_metadata.get("worker_id")
        if claimed_worker_id and claimed_worker_id != worker_id:
            raise StaleExecutionTaskError(
                f"stale execution task {execution_task_id}: claimed by worker {claimed_worker_id}"
            )

    def _append_status_event(
        self,
        *,
        run_id: str,
        attempt_id: str,
        event_type: RunEventType | str,
        from_status: RunStatus | None,
        to_status: RunStatus,
        reason: str | None = None,
        actor_user_id: str | None = None,
        request_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self.session.add(
            RunStatusEventRow(
                event_id=uuid4().hex,
                run_id=run_id,
                attempt_id=attempt_id,
                event_type=event_type_value(event_type),
                from_status=from_status.value if from_status is not None else None,
                to_status=to_status.value,
                reason=reason,
                actor_user_id=actor_user_id,
                request_id=request_id,
                metadata_json=dict(metadata or {}),
                created_at=utc_now(),
            )
        )

    def _append_worker_lifecycle_events(
        self,
        *,
        run: RunRecord,
        attempt_id: str,
        from_status: RunStatus,
        worker_id: str,
        request_id: str | None,
    ) -> None:
        transitions: list[tuple[RunEventType, RunStatus, RunStatus, str | None]] = []
        has_evaluator_results = bool(run.all_evaluator_results())
        emit_evaluator_events_before_terminal = False
        if from_status is RunStatus.PROVISIONING:
            transitions.append((RunEventType.STARTED, RunStatus.PROVISIONING, RunStatus.RUNNING, None))
            from_status = RunStatus.RUNNING
        if from_status is RunStatus.RUNNING and has_evaluator_results:
            transitions.append((RunEventType.EVALUATING, RunStatus.RUNNING, RunStatus.EVALUATING, None))
            from_status = RunStatus.EVALUATING
        elif from_status is RunStatus.EVALUATING and has_evaluator_results:
            emit_evaluator_events_before_terminal = True

        terminal_event = {
            RunStatus.SUCCEEDED: RunEventType.SUCCEEDED,
            RunStatus.FAILED: RunEventType.FAILED,
            RunStatus.CANCELED: RunEventType.CANCELED,
        }[run.status]
        transitions.append((terminal_event, from_status, run.status, run.failure_reason))

        for event_type, previous_status, next_status, reason in transitions:
            if emit_evaluator_events_before_terminal and event_type is terminal_event:
                self._append_evaluator_events(
                    run=run,
                    attempt_id=attempt_id,
                    worker_id=worker_id,
                    request_id=request_id,
                )
                emit_evaluator_events_before_terminal = False
            self._append_status_event(
                run_id=run.run_id,
                attempt_id=attempt_id,
                event_type=event_type,
                from_status=previous_status,
                to_status=next_status,
                reason=reason,
                request_id=request_id,
                metadata={"worker_id": worker_id, "execution_task_id": attempt_id},
            )
            if event_type is RunEventType.EVALUATING:
                self._append_evaluator_events(
                    run=run,
                    attempt_id=attempt_id,
                    worker_id=worker_id,
                    request_id=request_id,
                )

    def _append_evaluator_events(
        self,
        *,
        run: RunRecord,
        attempt_id: str,
        worker_id: str,
        request_id: str | None,
    ) -> None:
        for result in run.all_evaluator_results():
            event_type = (
                RunEventType.EVALUATOR_COMPLETED
                if result.status == "completed"
                else RunEventType.EVALUATOR_FAILED
            )
            self._append_status_event(
                run_id=run.run_id,
                attempt_id=attempt_id,
                event_type=event_type,
                from_status=RunStatus.EVALUATING,
                to_status=RunStatus.EVALUATING,
                reason=result.failure_reason,
                request_id=request_id,
                metadata=_evaluator_event_metadata(
                    result,
                    worker_id=worker_id,
                    execution_task_id=attempt_id,
                ),
            )


class AuditEventRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def record_event(
        self,
        *,
        event_type: str,
        payload: dict[str, Any],
        actor_user_id: str | None = None,
        project_id: str | None = None,
        run_id: str | None = None,
        attempt_id: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        metadata: dict[str, Any] | None = None,
        request_id: str | None = None,
    ) -> AuditEventRecord:
        row = AuditEventRow(
            event_id=uuid4().hex,
            actor_user_id=actor_user_id,
            project_id=project_id,
            run_id=run_id,
            attempt_id=attempt_id,
            event_type=event_type,
            subject_type=subject_type,
            subject_id=subject_id,
            payload_json=dict(payload),
            metadata_json=dict(metadata or {}),
            request_id=request_id,
        )
        self.session.add(row)
        self.session.flush()
        return _audit_event_record(row)

    def list_events(
        self,
        *,
        project_id: str | None = None,
        run_id: str | None = None,
    ) -> list[AuditEventRecord]:
        query = select(AuditEventRow).order_by(AuditEventRow.occurred_at, AuditEventRow.event_id)
        if project_id is not None:
            query = query.where(AuditEventRow.project_id == project_id)
        if run_id is not None:
            query = query.where(AuditEventRow.run_id == run_id)
        return [_audit_event_record(row) for row in self.session.scalars(query)]


def _positive_limits(limits: dict[str, int]) -> dict[str, int]:
    cleaned: dict[str, int] = {}
    for key, value in limits.items():
        if value > 0:
            cleaned[key] = value
    return cleaned


def _scheduler_capacity_block_from_attempt(
    row: RunRow,
    attempt: RunAttemptRow,
) -> SchedulerCapacityBlock | None:
    metadata = dict(attempt.metadata_json or {})
    execution = dict(metadata.get("execution") or {})
    scheduler = dict(execution.get("scheduler") or {})
    blocked = scheduler.get("capacity_blocked")
    if not isinstance(blocked, dict):
        return None
    try:
        observed_at = _parse_datetime(str(blocked["observed_at"]))
        return SchedulerCapacityBlock(
            run_id=row.run_id,
            project_id=row.project_id,
            scheduler_id=str(blocked["scheduler_id"]),
            execution_task_id=str(blocked["execution_task_id"]),
            dimension=str(blocked["dimension"]),
            key=str(blocked["key"]),
            active_count=int(blocked["active_count"]),
            limit=int(blocked["limit"]),
            reason=str(blocked["reason"]),
            observed_at=observed_at,
            backend_key=str(blocked["backend_key"]),
            provider_key=str(blocked["provider_key"]),
            model_key=str(blocked["model_key"]),
            agent_key=str(blocked["agent_key"]),
            benchmark_key=str(blocked["benchmark_key"]),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _capacity_block_signature_from_attempt(attempt: RunAttemptRow) -> tuple[object, ...] | None:
    metadata = dict(attempt.metadata_json or {})
    execution = dict(metadata.get("execution") or {})
    scheduler = dict(execution.get("scheduler") or {})
    blocked = scheduler.get("capacity_blocked")
    if not isinstance(blocked, dict):
        return None
    return _capacity_block_signature(blocked)


def _capacity_block_signature(blocked: dict[str, Any]) -> tuple[object, ...]:
    return (
        blocked.get("dimension"),
        blocked.get("key"),
        blocked.get("active_count"),
        blocked.get("limit"),
        blocked.get("reason"),
        blocked.get("backend_key"),
        blocked.get("provider_key"),
        blocked.get("model_key"),
        blocked.get("agent_key"),
        blocked.get("benchmark_key"),
    )


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _at_capacity(counts: dict[str, int], key: str, limits: dict[str, int]) -> bool:
    limit = limits.get(key)
    return limit is not None and counts.get(key, 0) >= limit


def _first_capacity_blocker(
    checks: tuple[tuple[str, str, dict[str, int], dict[str, int], str], ...],
) -> tuple[str, str, int, int, str] | None:
    for dimension, key, counts, limits, reason in checks:
        limit = limits.get(key)
        active_count = counts.get(key, 0)
        if limit is not None and active_count >= limit:
            return dimension, key, active_count, limit, reason
    return None


def _backend_capacity_key(row: RunRow) -> str:
    metadata = dict(row.runner_metadata or {})
    for key in ("harness_id", "backend_id", "runner_backend"):
        value = metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    runner_contract = metadata.get("runner_contract")
    if runner_contract == "harbor-local-docker-v0":
        return "harbor-local-docker"
    return row.sandbox_backend


def _provider_capacity_key(row: RunRow) -> str:
    return row.model_provider


def _model_capacity_key(row: RunRow) -> str:
    return row.model_name


def _agent_capacity_key(row: RunRow) -> str:
    run_metadata = dict(row.metadata_json or {})
    harbor_run = run_metadata.get("harbor_run")
    if isinstance(harbor_run, dict):
        value = harbor_run.get("agent")
        if isinstance(value, str) and value.strip():
            return value.strip()
    runner_metadata = dict(row.runner_metadata or {})
    for key in ("agent_id", "agent", "harbor_agent"):
        value = runner_metadata.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return "unknown-agent"


def _benchmark_capacity_key(row: RunRow) -> str:
    run_metadata = dict(row.metadata_json or {})
    harbor_run = run_metadata.get("harbor_run")
    if isinstance(harbor_run, dict):
        for key in ("dataset_ref", "benchmark_ref", "benchmark"):
            value = harbor_run.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return f"{row.benchmark_suite}@{row.benchmark_version}"


def _team_record(row: TeamRow) -> TeamRecord:
    return TeamRecord(team_id=row.team_id, name=row.name, created_at=_aware(row.created_at))


def _user_record(row: UserRow) -> UserRecord:
    return UserRecord(
        user_id=row.user_id,
        email=row.email,
        display_name=row.display_name,
        team_id=row.team_id,
        created_at=_aware(row.created_at),
    )


def _project_record(row: ProjectRow) -> ProjectRecord:
    return ProjectRecord(
        project_id=row.project_id,
        name=row.name,
        owner_team_id=row.owner_team_id,
        created_by_user_id=row.created_by_user_id,
        description=row.description,
        status=row.status,
        created_at=_aware(row.created_at),
        updated_at=_aware(row.updated_at),
    )


def _fixture_instance(row: TaskInstanceRow) -> BenchmarkFixtureInstance:
    return BenchmarkFixtureInstance(
        task_family=row.task_family.name,
        instance_id=row.instance_id,
        instruction_ref=row.instruction_ref,
        input_files=list(row.input_files or []),
        input_artifact_refs=list(row.input_artifact_refs or []),
        required_artifacts=list(row.required_artifacts or []),
        runner_image=row.runner_image,
        runner_entrypoint=list(row.runner_entrypoint or []),
        runner_contract=row.runner_contract,
        metadata=dict(row.metadata_json or {}),
    )


def _turn_row(*, run_id: str, attempt_id: str, turn: TerminalTurn) -> RunTerminalTurnRow:
    stdout, stdout_metadata = _bounded_terminal_stream(turn.stdout, stream_name="stdout")
    stderr, stderr_metadata = _bounded_terminal_stream(turn.stderr, stream_name="stderr")
    metadata = dict(turn.metadata)
    metadata.update(stdout_metadata)
    metadata.update(stderr_metadata)
    return RunTerminalTurnRow(
        run_id=run_id,
        attempt_id=attempt_id,
        turn_index=turn.turn_index,
        command=turn.command,
        cwd=turn.cwd,
        started_at=turn.started_at,
        completed_at=turn.completed_at,
        exit_code=turn.exit_code,
        stdout=stdout,
        stderr=stderr,
        changed_paths=list(turn.changed_paths),
        model_call_id=turn.model_call_id,
        metadata_json=metadata,
    )


def _bounded_terminal_stream(value: str, *, stream_name: str) -> tuple[str, dict[str, Any]]:
    payload = value.encode("utf-8")
    if len(payload) <= _MAX_INLINE_TERMINAL_STREAM_BYTES:
        return value, {}

    marker = _TRUNCATED_STREAM_MARKER
    marker_bytes = marker.encode("utf-8")
    preview_budget = max(_MAX_INLINE_TERMINAL_STREAM_BYTES - len(marker_bytes), 0)
    preview = payload[:preview_budget].decode("utf-8", errors="ignore") + marker
    preview_bytes = len(preview.encode("utf-8"))
    return preview, {
        f"{stream_name}_truncated": True,
        f"{stream_name}_original_bytes": len(payload),
        f"{stream_name}_inline_bytes": preview_bytes,
        f"{stream_name}_inline_limit_bytes": _MAX_INLINE_TERMINAL_STREAM_BYTES,
        f"{stream_name}_truncation_reason": "object_first_stream_preview",
    }


def _terminal_turn(row: RunTerminalTurnRow) -> TerminalTurn:
    return TerminalTurn(
        turn_index=row.turn_index,
        command=row.command,
        cwd=row.cwd,
        started_at=_aware(row.started_at),
        completed_at=_aware(row.completed_at),
        exit_code=row.exit_code,
        stdout=row.stdout,
        stderr=row.stderr,
        changed_paths=list(row.changed_paths or []),
        model_call_id=row.model_call_id,
        metadata=dict(row.metadata_json or {}),
    )


def _artifact_row(
    *,
    run_id: str,
    attempt_id: str,
    artifact: ArtifactRef,
    index: int,
    artifact_id: str | None = None,
) -> ArtifactRow:
    return ArtifactRow(
        artifact_id=artifact_id or artifact.artifact_id,
        run_id=run_id,
        attempt_id=attempt_id,
        artifact_index=index,
        kind=artifact.kind.value,
        uri=artifact.uri,
        storage_key=artifact.metadata.get("storage_key"),
        media_type=artifact.media_type,
        sha256=artifact.sha256,
        size_bytes=artifact.size_bytes,
        metadata_json=dict(artifact.metadata),
    )


def _artifact_ref(row: ArtifactRow) -> ArtifactRef:
    return ArtifactRef(
        artifact_id=row.artifact_id,
        kind=row.kind,
        uri=row.uri,
        media_type=row.media_type,
        sha256=row.sha256,
        size_bytes=row.size_bytes,
        metadata=dict(row.metadata_json or {}),
    )


def _artifact_chunk_metadata(row: ArtifactChunkRow) -> ArtifactChunkMetadata:
    return ArtifactChunkMetadata(
        run_id=row.run_id,
        attempt_id=row.attempt_id,
        artifact_id=row.artifact_id,
        chunk_kind=row.chunk_kind,
        chunk_sequence=row.chunk_sequence,
        storage_key=row.storage_key,
        media_type=row.media_type,
        size_bytes=row.size_bytes,
        sha256=row.sha256,
        upload_status=row.upload_status,
        upload_error_reason=row.upload_error_reason,
        created_at=_aware(row.created_at),
        metadata=dict(row.metadata_json or {}),
    )


def _artifact_chunk_kind_value(value: ArtifactChunkKind | str) -> str:
    if isinstance(value, ArtifactChunkKind):
        return value.value
    return ArtifactChunkKind(value).value


def _artifact_chunk_event_type(chunk_kind: str) -> RunEventType:
    if chunk_kind in {ArtifactChunkKind.STDOUT.value, ArtifactChunkKind.STDERR.value}:
        return RunEventType.LOG_CHUNK_RECORDED
    return RunEventType.ARTIFACT_CHUNK_RECORDED


def _artifact_chunk_event_metadata(
    chunk: ArtifactChunkMetadata,
    *,
    upload_status: str,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "artifact_id": chunk.artifact_id,
        "chunk_kind": _artifact_chunk_kind_value(chunk.chunk_kind),
        "chunk_sequence": chunk.chunk_sequence,
        "storage_key": chunk.storage_key,
        "media_type": chunk.media_type,
        "upload_status": upload_status,
        "schema_version": chunk.schema_version,
    }
    if chunk.size_bytes is not None:
        metadata["size_bytes"] = chunk.size_bytes
    if chunk.sha256 is not None:
        metadata["sha256"] = chunk.sha256
    if chunk.upload_error_reason is not None:
        metadata["upload_error_reason"] = chunk.upload_error_reason
    return metadata


def _artifact_chunk_upload_transition_event_metadata(
    chunk: ArtifactChunkMetadata,
    *,
    previous_upload_status: str | None,
    upload_status: str,
) -> dict[str, Any]:
    metadata = _artifact_chunk_event_metadata(chunk, upload_status=upload_status)
    metadata["previous_upload_status"] = previous_upload_status
    return metadata


def _artifact_upload_status_value(value: ArtifactUploadStatus | str) -> str:
    if isinstance(value, ArtifactUploadStatus):
        return value.value
    return ArtifactUploadStatus(value).value


def _evaluator_result_row(*, run_id: str, attempt_id: str, result: EvaluatorResult) -> EvaluatorResultRow:
    return EvaluatorResultRow(
        run_id=run_id,
        attempt_id=attempt_id,
        evaluator_id=result.evaluator_id,
        mode=result.mode,
        status=result.status,
        score=result.score,
        metrics=dict(result.metrics),
        verbal_feedback=result.verbal_feedback,
        judge_provider=result.judge.provider if result.judge is not None else None,
        judge_model_name=result.judge.model_name if result.judge is not None else None,
        judge_model_version=result.judge.model_version if result.judge is not None else None,
        judge_rubric_version=result.judge.rubric_version if result.judge is not None else None,
        judge_metadata=dict(result.judge.metadata) if result.judge is not None else {},
        artifact_refs=list(result.artifact_refs),
        failure_reason=result.failure_reason,
        metadata_json=dict(result.metadata),
        created_at=result.created_at,
    )


def _evaluator_event_metadata(
    result: EvaluatorResult,
    *,
    worker_id: str,
    execution_task_id: str,
) -> dict[str, Any]:
    metadata: dict[str, Any] = {
        "evaluator_id": result.evaluator_id,
        "mode": result.mode,
        "status": result.status,
        "score": result.score,
        "artifact_refs": [_safe_evaluator_artifact_ref(ref) for ref in result.artifact_refs],
        "worker_id": worker_id,
        "execution_task_id": execution_task_id,
    }
    if result.failure_reason is not None:
        metadata["failure_reason"] = result.failure_reason
    return metadata


def _safe_evaluator_artifact_ref(ref: str) -> str:
    parsed = urlparse(ref)
    if parsed.scheme == "file" or (parsed.scheme == "" and ref.startswith("/")):
        return PurePosixPath(parsed.path or ref).name or "artifact"
    if parsed.query or parsed.fragment:
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))
    return ref


def _evaluator_result(row: EvaluatorResultRow) -> EvaluatorResult:
    has_judge = (
        row.judge_provider is not None
        and row.judge_model_name is not None
        and row.judge_rubric_version is not None
    )
    return EvaluatorResult(
        evaluator_id=row.evaluator_id,
        mode=row.mode,
        status=row.status,
        score=row.score,
        metrics=dict(row.metrics or {}),
        verbal_feedback=row.verbal_feedback,
        judge=(
            JudgeConfig(
                provider=row.judge_provider,
                model_name=row.judge_model_name,
                rubric_version=row.judge_rubric_version,
                model_version=row.judge_model_version,
                metadata=dict(row.judge_metadata or {}),
            )
            if has_judge
            else None
        ),
        artifact_refs=list(row.artifact_refs or []),
        failure_reason=row.failure_reason,
        metadata=dict(row.metadata_json or {}),
        created_at=_aware(row.created_at),
    )


def _evaluator_config_payload(config: EvaluatorConfig) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "evaluator_id": config.evaluator_id,
        "mode": config.mode,
        "metadata": dict(config.metadata),
    }
    if config.judge is not None:
        payload["judge"] = {
            "provider": config.judge.provider,
            "model_name": config.judge.model_name,
            "model_version": config.judge.model_version,
            "rubric_version": config.judge.rubric_version,
            "metadata": dict(config.judge.metadata),
        }
    return payload


def _evaluator_config(payload: dict[str, Any]) -> EvaluatorConfig:
    judge_payload = payload.get("judge")
    judge = JudgeConfig(**judge_payload) if isinstance(judge_payload, dict) else None
    return EvaluatorConfig(
        evaluator_id=payload["evaluator_id"],
        mode=payload["mode"],
        judge=judge,
        metadata=dict(payload.get("metadata") or {}),
    )


def _audit_event_record(row: AuditEventRow) -> AuditEventRecord:
    return AuditEventRecord(
        event_id=row.event_id,
        event_type=row.event_type,
        payload=dict(row.payload_json or {}),
        occurred_at=_aware(row.occurred_at),
        actor_user_id=row.actor_user_id,
        project_id=row.project_id,
        run_id=row.run_id,
        attempt_id=row.attempt_id,
        subject_type=row.subject_type,
        subject_id=row.subject_id,
        metadata=dict(row.metadata_json or {}),
        request_id=row.request_id,
    )


def _status_event_record(row: RunStatusEventRow) -> RunStatusEvent:
    return RunStatusEvent(
        event_id=row.event_id,
        seq=row.id,
        run_id=row.run_id,
        attempt_id=row.attempt_id,
        event_type=row.event_type,
        from_status=row.from_status,
        to_status=row.to_status,
        created_at=_aware(row.created_at),
        reason=row.reason,
        actor_user_id=row.actor_user_id,
        request_id=row.request_id,
        metadata=dict(row.metadata_json or {}),
    )


def _dashboard_projection_needs_refresh(projection: RunDashboardProjectionRow, run: RunRow) -> bool:
    if projection.dirty:
        return True
    if projection.status != run.status:
        return True
    if projection.project_id != run.project_id:
        return True
    if projection.owner_team_name_snapshot != run.owner_team_name_snapshot:
        return True
    if projection.is_terminal != (run.status in _TERMINAL_STATUS_VALUES):
        return True
    if projection.updated_at is None:
        return True
    return _aware(projection.updated_at) < _aware(run.updated_at)


def _dashboard_progress_record_from_projection(
    row: RunRow,
    projection: RunDashboardProjectionRow,
) -> RunDashboardProgressRecord:
    payload = projection.payload if isinstance(projection.payload, dict) else {}
    project = payload.get("project") if isinstance(payload.get("project"), dict) else {}
    progress = payload.get("progress") if isinstance(payload.get("progress"), dict) else {}
    evaluator = payload.get("evaluator") if isinstance(payload.get("evaluator"), dict) else {}
    status = _string_or_default(payload.get("status"), projection.status or row.status)
    return RunDashboardProgressRecord(
        run_id=row.run_id,
        project_id=projection.project_id or row.project_id,
        owner_team=_string_or_none(project.get("owner_team")) or projection.owner_team_name_snapshot,
        status=status,
        is_terminal=_bool_or_default(progress.get("is_terminal"), projection.is_terminal),
        artifact_count=_int_or_zero(progress.get("artifact_count")),
        turn_count=_int_or_zero(progress.get("turn_count")),
        evaluator_completed=evaluator.get("status") == "completed",
        evaluator_score=_float_or_none(evaluator.get("score")),
        updated_at=_aware(row.updated_at),
    )


def _dashboard_progress_record_from_run(run: RunRecord) -> RunDashboardProgressRecord:
    return RunDashboardProgressRecord(
        run_id=run.run_id,
        project_id=run.project_id,
        owner_team=run.owner_team,
        status=run.status.value,
        is_terminal=run.status in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED},
        artifact_count=len(run.artifacts),
        turn_count=len(run.trajectory),
        evaluator_completed=run.evaluator_result is not None and run.evaluator_result.status == "completed",
        evaluator_score=run.evaluator_result.score if run.evaluator_result is not None else None,
        updated_at=run.updated_at,
    )


def _dashboard_summary_payload_from_projection(projection: RunDashboardProjectionRow | None) -> dict[str, Any] | None:
    if projection is None or projection.dirty:
        return None
    if not isinstance(projection.payload, dict) or not projection.payload:
        return None
    return dict(projection.payload)


def _apply_run_list_filters(
    query: Any,
    *,
    project_ids: set[str] | None = None,
    project_id: str | None = None,
    status: str | None = None,
    benchmark_suite: str | None = None,
    task_family: str | None = None,
    task_instance_id: str | None = None,
    created_by_user_id: str | None = None,
    created_after: datetime | None = None,
    created_before: datetime | None = None,
) -> Any:
    if project_ids is not None:
        query = query.where(RunRow.project_id.in_(project_ids))
    if project_id is not None:
        query = query.where(RunRow.project_id == project_id)
    if status is not None:
        query = query.where(RunRow.status == status)
    if benchmark_suite is not None:
        query = query.where(RunRow.benchmark_suite == benchmark_suite)
    if task_family is not None:
        query = query.where(RunRow.task_family == task_family)
    if task_instance_id is not None:
        query = query.where(RunRow.task_instance_id == task_instance_id)
    if created_by_user_id is not None:
        query = query.where(RunRow.created_by_user_id == created_by_user_id)
    if created_after is not None:
        query = query.where(RunRow.created_at >= created_after)
    if created_before is not None:
        query = query.where(RunRow.created_at <= created_before)
    return query


def _string_or_default(value: object, default: str) -> str:
    return value if isinstance(value, str) and value.strip() else default


def _string_or_none(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _bool_or_default(value: object, default: bool) -> bool:
    return value if isinstance(value, bool) else default


def _int_or_zero(value: object) -> int:
    if isinstance(value, bool):
        return 0
    if isinstance(value, int):
        return max(value, 0)
    return 0


def _float_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    return None


def _attempt_id(run_id: str, attempt_number: int) -> str:
    return f"{run_id}:attempt:{attempt_number}"


def _attempt_number(attempt_id: str) -> int:
    try:
        return int(attempt_id.rsplit(":attempt:", 1)[1])
    except (IndexError, ValueError) as exc:
        raise ValueError(f"invalid attempt id: {attempt_id}") from exc


def _worker_attempt_metadata(
    metadata: dict[str, Any] | None,
    *,
    worker_id: str,
    process_status: RunnerProcessStatus,
    heartbeat_status: RunStatus,
    now: datetime,
    claimed_at: datetime | None,
    completed_at: datetime | None,
) -> dict[str, Any]:
    updated = dict(metadata or {})
    worker = dict(updated.get("worker") or {})
    if worker_id:
        worker["worker_id"] = worker_id
    if claimed_at is not None:
        worker["claimed_at"] = _datetime_json(claimed_at)
    worker["heartbeat_status"] = heartbeat_status.value
    worker["last_heartbeat_at"] = _datetime_json(now)
    if completed_at is not None:
        worker["completed_at"] = _datetime_json(completed_at)
    updated["worker"] = worker
    if worker_id:
        updated = runner_process_metadata(
            updated,
            worker_id=worker_id,
            process_status=process_status,
            heartbeat_status=heartbeat_status.value,
            observed_at=now,
            claimed_at=claimed_at,
            completed_at=completed_at,
        )
    return updated


def _worker_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    worker = (metadata or {}).get("worker")
    if isinstance(worker, dict):
        return dict(worker)
    return _runner_metadata(metadata)


def _runner_metadata(metadata: dict[str, Any] | None) -> dict[str, Any]:
    execution = (metadata or {}).get("execution")
    if isinstance(execution, dict):
        runner = execution.get("runner")
        if isinstance(runner, dict):
            return dict(runner)
    return {}


def _terminal_runner_process_status(status: RunStatus) -> RunnerProcessStatus:
    if status is RunStatus.SUCCEEDED:
        return RunnerProcessStatus.COMPLETED
    if status is RunStatus.CANCELED:
        return RunnerProcessStatus.CANCELED
    return RunnerProcessStatus.FAILED


def _parse_worker_datetime(value: Any) -> datetime | None:
    return _parse_metadata_datetime(value)


def _parse_metadata_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return _aware(parsed)


def _datetime_json(value: datetime) -> str:
    return _aware(value).isoformat()


def _coerce_run_status(value: RunStatus | str) -> RunStatus:
    if isinstance(value, RunStatus):
        return value
    return RunStatus(value)


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: str) -> datetime:
    return _aware(datetime.fromisoformat(value.replace("Z", "+00:00")))


def _required(value, label: str, key: str):
    if value is None:
        raise KeyError(f"Unknown {label}: {key}")
    return value
