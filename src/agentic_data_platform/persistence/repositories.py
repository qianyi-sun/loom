from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from agentic_data_platform.benchmarks.fixtures import (
    BenchmarkFixtureCatalog,
    BenchmarkFixtureFamily,
    BenchmarkFixtureInstance,
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
    utc_now,
)


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
            event_type="run.created",
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
            event_type="run.canceled",
            reason=reason,
            actor_user_id=actor_user_id,
            request_id=request_id,
        )

    def transition_run(
        self,
        run_id: str,
        next_status: RunStatus | str,
        *,
        event_type: str = "run.status_changed",
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
            event_type="run.retried",
            from_status=previous_status,
            to_status=RunStatus.QUEUED,
            reason=reason,
            actor_user_id=actor_user_id,
            request_id=request_id,
        )
        self.session.flush()
        return self.get_run(run_id)

    def claim_next_queued_run(
        self,
        *,
        worker_id: str,
        request_id: str | None = None,
    ) -> RunRecord | None:
        _require_non_empty("worker_id", worker_id)
        row = self.session.scalar(
            select(RunRow)
            .where(RunRow.status == RunStatus.QUEUED.value)
            .order_by(RunRow.created_at, RunRow.run_id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if row is None:
            return None

        previous_status = RunStatus(row.status)
        run = self._run_record(row)
        run.transition_to(RunStatus.PROVISIONING)

        row.status = run.status.value
        row.updated_at = run.updated_at

        attempt = self._latest_attempt_row(run.run_id)
        attempt.status = run.status.value
        attempt.updated_at = utc_now()

        self._append_status_event(
            run_id=run.run_id,
            attempt_id=attempt.attempt_id,
            event_type="run.claimed",
            from_status=previous_status,
            to_status=RunStatus.PROVISIONING,
            request_id=request_id,
            metadata={"worker_id": worker_id},
        )
        self.session.flush()
        return self.get_run(run.run_id)

    def save_worker_result(
        self,
        run: RunRecord,
        *,
        worker_id: str,
        request_id: str | None = None,
    ) -> RunRecord:
        _require_non_empty("worker_id", worker_id)
        row = self._run_row_for_update(run.run_id)
        previous_status = RunStatus(row.status)
        if previous_status is RunStatus.CANCELED:
            return self.get_run(run.run_id)
        if previous_status not in {RunStatus.PROVISIONING, RunStatus.RUNNING, RunStatus.EVALUATING}:
            raise ValueError("worker results can only be saved for a claimed active run")
        if run.status not in {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELED}:
            raise ValueError("worker result must be terminal")

        attempt = self._latest_attempt_row(run.run_id)
        self.save_run(run)
        self._append_worker_lifecycle_events(
            run=run,
            attempt_id=attempt.attempt_id,
            from_status=previous_status,
            worker_id=worker_id,
            request_id=request_id,
        )
        self.session.flush()
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
            self.session.add(_artifact_row(run_id=run.run_id, attempt_id=attempt_id, artifact=artifact, index=index))

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
        row.metadata_json = {}
        row.updated_at = utc_now()

    def get_run(self, run_id: str) -> RunRecord:
        row = _required(self.session.get(RunRow, run_id), "run", run_id)
        return self._run_record(row)

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
        query = select(RunRow).order_by(RunRow.created_at, RunRow.run_id)
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
        return [self._run_record(row) for row in self.session.scalars(query)]

    def list_status_events(self, run_id: str) -> list[RunStatusEvent]:
        _required(self.session.get(RunRow, run_id), "run", run_id)
        query = (
            select(RunStatusEventRow)
            .where(RunStatusEventRow.run_id == run_id)
            .order_by(RunStatusEventRow.id)
        )
        return [_status_event_record(row) for row in self.session.scalars(query)]

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

    def _run_row_for_update(self, run_id: str) -> RunRow:
        row = self.session.scalar(select(RunRow).where(RunRow.run_id == run_id).with_for_update())
        return _required(row, "run", run_id)

    def _append_status_event(
        self,
        *,
        run_id: str,
        attempt_id: str,
        event_type: str,
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
                event_type=event_type,
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
        transitions: list[tuple[str, RunStatus, RunStatus, str | None]] = []
        if from_status is RunStatus.PROVISIONING:
            transitions.append(("run.started", RunStatus.PROVISIONING, RunStatus.RUNNING, None))
            from_status = RunStatus.RUNNING
        if from_status is RunStatus.RUNNING and run.evaluator_result is not None:
            transitions.append(("run.evaluating", RunStatus.RUNNING, RunStatus.EVALUATING, None))
            from_status = RunStatus.EVALUATING

        terminal_event = {
            RunStatus.SUCCEEDED: "run.succeeded",
            RunStatus.FAILED: "run.failed",
            RunStatus.CANCELED: "run.canceled",
        }[run.status]
        transitions.append((terminal_event, from_status, run.status, run.failure_reason))

        for event_type, previous_status, next_status, reason in transitions:
            self._append_status_event(
                run_id=run.run_id,
                attempt_id=attempt_id,
                event_type=event_type,
                from_status=previous_status,
                to_status=next_status,
                reason=reason,
                request_id=request_id,
                metadata={"worker_id": worker_id},
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
    return RunTerminalTurnRow(
        run_id=run_id,
        attempt_id=attempt_id,
        turn_index=turn.turn_index,
        command=turn.command,
        cwd=turn.cwd,
        started_at=turn.started_at,
        completed_at=turn.completed_at,
        exit_code=turn.exit_code,
        stdout=turn.stdout,
        stderr=turn.stderr,
        changed_paths=list(turn.changed_paths),
        model_call_id=turn.model_call_id,
        metadata_json=dict(turn.metadata),
    )


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


def _artifact_row(*, run_id: str, attempt_id: str, artifact: ArtifactRef, index: int) -> ArtifactRow:
    return ArtifactRow(
        artifact_id=artifact.artifact_id,
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


def _attempt_id(run_id: str, attempt_number: int) -> str:
    return f"{run_id}:attempt:{attempt_number}"


def _require_non_empty(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _required(value, label: str, key: str):
    if value is None:
        raise KeyError(f"Unknown {label}: {key}")
    return value
