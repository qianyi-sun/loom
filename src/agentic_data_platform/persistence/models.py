from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship


class Base(DeclarativeBase):
    pass


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


class TeamRow(Base):
    __tablename__ = "teams"

    team_id = mapped_column(String(128), primary_key=True)
    name = mapped_column(String(255), nullable=False, unique=True)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    users = relationship("UserRow", back_populates="team")
    projects = relationship("ProjectRow", back_populates="owner_team")


class UserRow(Base):
    __tablename__ = "users"

    user_id = mapped_column(String(128), primary_key=True)
    email = mapped_column(String(255), nullable=False, unique=True)
    display_name = mapped_column(String(255), nullable=False)
    team_id = mapped_column(String(128), ForeignKey("teams.team_id"), nullable=True)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    team = relationship("TeamRow", back_populates="users")


class TeamMembershipRow(Base):
    __tablename__ = "team_memberships"
    __table_args__ = (UniqueConstraint("team_id", "user_id", name="uq_team_membership"),)

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    team_id = mapped_column(String(128), ForeignKey("teams.team_id"), nullable=False)
    user_id = mapped_column(String(128), ForeignKey("users.user_id"), nullable=False)
    role = mapped_column(String(64), nullable=False, default="member")
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class ProjectRow(Base):
    __tablename__ = "projects"

    project_id = mapped_column(String(128), primary_key=True)
    name = mapped_column(String(255), nullable=False)
    owner_team_id = mapped_column(String(128), ForeignKey("teams.team_id"), nullable=True)
    created_by_user_id = mapped_column(String(128), ForeignKey("users.user_id"), nullable=True)
    description = mapped_column(Text, nullable=False, default="")
    status = mapped_column(String(64), nullable=False, default="active")
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    owner_team = relationship("TeamRow", back_populates="projects")


class BenchmarkSuiteRow(Base):
    __tablename__ = "benchmark_suites"

    suite_name = mapped_column(String(128), primary_key=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    versions = relationship(
        "BenchmarkSuiteVersionRow",
        back_populates="suite",
        cascade="all, delete-orphan",
    )


class BenchmarkSuiteVersionRow(Base):
    __tablename__ = "benchmark_suite_versions"
    __table_args__ = (
        UniqueConstraint("suite_name", "benchmark_version", name="uq_benchmark_suite_version"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    suite_name = mapped_column(String(128), ForeignKey("benchmark_suites.suite_name"), nullable=False)
    benchmark_version = mapped_column(String(255), nullable=False)
    source_uri = mapped_column(Text, nullable=False)
    source_version = mapped_column(String(255), nullable=False)
    source_version_type = mapped_column(String(128), nullable=False)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    suite = relationship("BenchmarkSuiteRow", back_populates="versions")
    task_families = relationship(
        "TaskFamilyRow",
        back_populates="suite_version",
        cascade="all, delete-orphan",
        order_by="TaskFamilyRow.name",
    )


class TaskFamilyRow(Base):
    __tablename__ = "task_families"
    __table_args__ = (
        UniqueConstraint("suite_version_id", "name", name="uq_task_family_suite_version"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    suite_version_id = mapped_column(Integer, ForeignKey("benchmark_suite_versions.id"), nullable=False)
    name = mapped_column(String(255), nullable=False)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)

    suite_version = relationship("BenchmarkSuiteVersionRow", back_populates="task_families")
    task_instances = relationship(
        "TaskInstanceRow",
        back_populates="task_family",
        cascade="all, delete-orphan",
        order_by="TaskInstanceRow.instance_id",
    )


class TaskInstanceRow(Base):
    __tablename__ = "task_instances"
    __table_args__ = (
        UniqueConstraint("task_family_id", "instance_id", name="uq_task_instance_family"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_family_id = mapped_column(Integer, ForeignKey("task_families.id"), nullable=False)
    instance_id = mapped_column(String(255), nullable=False)
    instruction_ref = mapped_column(Text, nullable=False)
    input_files = mapped_column(JSON, nullable=False, default=list)
    input_artifact_refs = mapped_column(JSON, nullable=False, default=list)
    required_artifacts = mapped_column(JSON, nullable=False, default=list)
    runner_image = mapped_column(Text, nullable=False)
    runner_entrypoint = mapped_column(JSON, nullable=False, default=list)
    runner_contract = mapped_column(String(255), nullable=False)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)

    task_family = relationship("TaskFamilyRow", back_populates="task_instances")


class RunRow(Base):
    __tablename__ = "runs"

    run_id = mapped_column(String(128), primary_key=True)
    project_id = mapped_column(String(128), ForeignKey("projects.project_id"), nullable=False)
    created_by_user_id = mapped_column(String(128), ForeignKey("users.user_id"), nullable=True)
    owner_team_id = mapped_column(String(128), ForeignKey("teams.team_id"), nullable=True)
    owner_team_name_snapshot = mapped_column(String(255), nullable=False)
    benchmark_suite = mapped_column(String(128), nullable=False)
    benchmark_version = mapped_column(String(255), nullable=False)
    task_family = mapped_column(String(255), nullable=False)
    task_instance_id = mapped_column(String(255), nullable=False)
    task_source_uri = mapped_column(Text, nullable=False)
    task_input_artifact_refs = mapped_column(JSON, nullable=False, default=list)
    task_required_artifacts = mapped_column(JSON, nullable=False, default=list)
    task_metadata = mapped_column(JSON, nullable=False, default=dict)
    model_provider = mapped_column(String(128), nullable=False)
    model_name = mapped_column(String(255), nullable=False)
    model_mode = mapped_column(String(64), nullable=False)
    prompt_template_version = mapped_column(String(255), nullable=False)
    model_version = mapped_column(String(255), nullable=True)
    model_metadata = mapped_column(JSON, nullable=False, default=dict)
    runner_kind = mapped_column(String(64), nullable=False)
    sandbox_backend = mapped_column(String(64), nullable=False)
    runner_image = mapped_column(Text, nullable=False)
    runner_entrypoint = mapped_column(JSON, nullable=False, default=list)
    runner_internet_access = mapped_column(Boolean, nullable=False, default=True)
    runner_resource_limits = mapped_column(JSON, nullable=False, default=dict)
    runner_metadata = mapped_column(JSON, nullable=False, default=dict)
    evaluator_configs = mapped_column(JSON, nullable=True, default=list)
    status = mapped_column(String(64), nullable=False)
    failure_reason = mapped_column(Text, nullable=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at = mapped_column(DateTime(timezone=True), nullable=False)

    attempts = relationship("RunAttemptRow", back_populates="run", cascade="all, delete-orphan")
    turns = relationship("RunTerminalTurnRow", back_populates="run", cascade="all, delete-orphan")
    artifacts = relationship("ArtifactRow", back_populates="run", cascade="all, delete-orphan")
    artifact_chunks = relationship("ArtifactChunkRow", back_populates="run", cascade="all, delete-orphan")
    evaluator_results = relationship("EvaluatorResultRow", back_populates="run", cascade="all, delete-orphan")
    status_events = relationship("RunStatusEventRow", back_populates="run", cascade="all, delete-orphan")


class RunAttemptRow(Base):
    __tablename__ = "run_attempts"
    __table_args__ = (UniqueConstraint("run_id", "attempt_number", name="uq_run_attempt_number"),)

    attempt_id = mapped_column(String(160), primary_key=True)
    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=False)
    attempt_number = mapped_column(Integer, nullable=False)
    status = mapped_column(String(64), nullable=False)
    started_at = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at = mapped_column(DateTime(timezone=True), nullable=True)
    failure_reason = mapped_column(Text, nullable=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    run = relationship("RunRow", back_populates="attempts")
    turns = relationship("RunTerminalTurnRow", back_populates="attempt", cascade="all, delete-orphan")
    artifacts = relationship("ArtifactRow", back_populates="attempt")
    artifact_chunks = relationship("ArtifactChunkRow", back_populates="attempt", cascade="all, delete-orphan")
    evaluator_results = relationship("EvaluatorResultRow", back_populates="attempt")
    status_events = relationship("RunStatusEventRow", back_populates="attempt")


class RunStatusEventRow(Base):
    __tablename__ = "run_status_events"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id = mapped_column(String(128), nullable=False, unique=True)
    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=False)
    attempt_id = mapped_column(String(160), ForeignKey("run_attempts.attempt_id"), nullable=True)
    event_type = mapped_column(String(128), nullable=False)
    from_status = mapped_column(String(64), nullable=True)
    to_status = mapped_column(String(64), nullable=False)
    reason = mapped_column(Text, nullable=True)
    actor_user_id = mapped_column(String(128), ForeignKey("users.user_id"), nullable=True)
    request_id = mapped_column(String(128), nullable=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    run = relationship("RunRow", back_populates="status_events")
    attempt = relationship("RunAttemptRow", back_populates="status_events")


class RunDashboardProjectionRow(Base):
    __tablename__ = "run_dashboard_projections"

    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), primary_key=True)
    project_id = mapped_column(String(128), nullable=False)
    owner_team_name_snapshot = mapped_column(String(255), nullable=False)
    status = mapped_column(String(64), nullable=False)
    is_terminal = mapped_column(Boolean, nullable=False, default=False)
    payload = mapped_column(JSON, nullable=False, default=dict)
    source_attempt_id = mapped_column(String(160), nullable=True)
    source_event_seq = mapped_column(Integer, nullable=True)
    refresh_reason = mapped_column(String(128), nullable=False)
    dirty = mapped_column(Boolean, nullable=False, default=False)
    error_reason = mapped_column(Text, nullable=True)
    refreshed_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class RunTerminalTurnRow(Base):
    __tablename__ = "run_terminal_turns"
    __table_args__ = (UniqueConstraint("attempt_id", "turn_index", name="uq_attempt_turn_index"),)

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=False)
    attempt_id = mapped_column(String(160), ForeignKey("run_attempts.attempt_id"), nullable=False)
    turn_index = mapped_column(Integer, nullable=False)
    command = mapped_column(Text, nullable=False)
    cwd = mapped_column(Text, nullable=False)
    started_at = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at = mapped_column(DateTime(timezone=True), nullable=False)
    exit_code = mapped_column(Integer, nullable=False)
    stdout = mapped_column(Text, nullable=False)
    stderr = mapped_column(Text, nullable=False)
    changed_paths = mapped_column(JSON, nullable=False, default=list)
    model_call_id = mapped_column(String(255), nullable=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)

    run = relationship("RunRow", back_populates="turns")
    attempt = relationship("RunAttemptRow", back_populates="turns")


class ArtifactRow(Base):
    __tablename__ = "artifacts"

    artifact_id = mapped_column(String(255), primary_key=True)
    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=False)
    attempt_id = mapped_column(String(160), ForeignKey("run_attempts.attempt_id"), nullable=True)
    artifact_index = mapped_column(Integer, nullable=False, default=0)
    kind = mapped_column(String(64), nullable=False)
    uri = mapped_column(Text, nullable=False)
    storage_key = mapped_column(Text, nullable=True)
    media_type = mapped_column(String(255), nullable=False)
    sha256 = mapped_column(String(64), nullable=True)
    size_bytes = mapped_column(Integer, nullable=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    run = relationship("RunRow", back_populates="artifacts")
    attempt = relationship("RunAttemptRow", back_populates="artifacts")
    chunks = relationship("ArtifactChunkRow", back_populates="artifact", cascade="all, delete-orphan")


class ArtifactChunkRow(Base):
    __tablename__ = "artifact_chunks"
    __table_args__ = (
        UniqueConstraint("artifact_id", "chunk_kind", "chunk_sequence", name="uq_artifact_chunk_sequence"),
        Index(
            "ix_artifact_chunks_run_attempt_kind_sequence",
            "run_id",
            "attempt_id",
            "chunk_kind",
            "chunk_sequence",
        ),
        Index("ix_artifact_chunks_upload_status_created", "upload_status", "created_at"),
    )

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=False)
    attempt_id = mapped_column(String(160), ForeignKey("run_attempts.attempt_id"), nullable=False)
    artifact_id = mapped_column(
        String(255),
        ForeignKey("artifacts.artifact_id", ondelete="CASCADE"),
        nullable=False,
    )
    chunk_kind = mapped_column(String(64), nullable=False)
    chunk_sequence = mapped_column(Integer, nullable=False)
    storage_key = mapped_column(Text, nullable=False)
    media_type = mapped_column(String(255), nullable=False)
    size_bytes = mapped_column(Integer, nullable=True)
    sha256 = mapped_column(String(64), nullable=True)
    upload_status = mapped_column(String(64), nullable=False)
    upload_error_reason = mapped_column(Text, nullable=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    updated_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    run = relationship("RunRow", back_populates="artifact_chunks")
    attempt = relationship("RunAttemptRow", back_populates="artifact_chunks")
    artifact = relationship("ArtifactRow", back_populates="chunks")


class EvaluatorResultRow(Base):
    __tablename__ = "evaluator_results"

    id = mapped_column(Integer, primary_key=True, autoincrement=True)
    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=False)
    attempt_id = mapped_column(String(160), ForeignKey("run_attempts.attempt_id"), nullable=True)
    evaluator_id = mapped_column(String(128), nullable=False)
    mode = mapped_column(String(64), nullable=False, default="llm_judge")
    status = mapped_column(String(64), nullable=False)
    score = mapped_column(Float, nullable=True)
    metrics = mapped_column(JSON, nullable=False, default=dict)
    verbal_feedback = mapped_column(Text, nullable=False, default="")
    judge_provider = mapped_column(String(128), nullable=True)
    judge_model_name = mapped_column(String(255), nullable=True)
    judge_model_version = mapped_column(String(255), nullable=True)
    judge_rubric_version = mapped_column(String(255), nullable=True)
    judge_metadata = mapped_column(JSON, nullable=False, default=dict)
    artifact_refs = mapped_column(JSON, nullable=False, default=list)
    failure_reason = mapped_column(Text, nullable=True)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)

    run = relationship("RunRow", back_populates="evaluator_results")
    attempt = relationship("RunAttemptRow", back_populates="evaluator_results")


class SkillObjectRow(Base):
    __tablename__ = "skill_objects"

    skill_id = mapped_column(String(128), primary_key=True)
    producing_run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=False)
    representation = mapped_column(String(64), nullable=False)
    artifact_refs = mapped_column(JSON, nullable=False, default=list)
    benchmark_suite = mapped_column(String(128), nullable=False)
    task_family = mapped_column(String(255), nullable=False)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    created_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)


class AuditEventRow(Base):
    __tablename__ = "audit_events"

    event_id = mapped_column(String(128), primary_key=True)
    occurred_at = mapped_column(DateTime(timezone=True), nullable=False, default=utc_now)
    actor_user_id = mapped_column(String(128), ForeignKey("users.user_id"), nullable=True)
    project_id = mapped_column(String(128), ForeignKey("projects.project_id"), nullable=True)
    run_id = mapped_column(String(128), ForeignKey("runs.run_id"), nullable=True)
    attempt_id = mapped_column(String(160), ForeignKey("run_attempts.attempt_id"), nullable=True)
    event_type = mapped_column(String(128), nullable=False)
    subject_type = mapped_column(String(128), nullable=True)
    subject_id = mapped_column(String(255), nullable=True)
    before_json = mapped_column("before", JSON, nullable=True)
    after_json = mapped_column("after", JSON, nullable=True)
    payload_json = mapped_column("payload", JSON, nullable=False, default=dict)
    metadata_json = mapped_column("metadata", JSON, nullable=False, default=dict)
    request_id = mapped_column(String(128), nullable=True)
