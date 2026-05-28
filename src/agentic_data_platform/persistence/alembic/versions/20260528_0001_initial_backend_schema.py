from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260528_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "teams",
        sa.Column("team_id", sa.String(length=128), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "users",
        sa.Column("user_id", sa.String(length=128), primary_key=True),
        sa.Column("email", sa.String(length=255), nullable=False, unique=True),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("team_id", sa.String(length=128), sa.ForeignKey("teams.team_id"), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "team_memberships",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("team_id", sa.String(length=128), sa.ForeignKey("teams.team_id"), nullable=False),
        sa.Column("user_id", sa.String(length=128), sa.ForeignKey("users.user_id"), nullable=False),
        sa.Column("role", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("team_id", "user_id", name="uq_team_membership"),
    )
    op.create_table(
        "projects",
        sa.Column("project_id", sa.String(length=128), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("owner_team_id", sa.String(length=128), sa.ForeignKey("teams.team_id"), nullable=True),
        sa.Column("created_by_user_id", sa.String(length=128), sa.ForeignKey("users.user_id"), nullable=True),
        sa.Column("description", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "benchmark_suites",
        sa.Column("suite_name", sa.String(length=128), primary_key=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "benchmark_suite_versions",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "suite_name",
            sa.String(length=128),
            sa.ForeignKey("benchmark_suites.suite_name"),
            nullable=False,
        ),
        sa.Column("benchmark_version", sa.String(length=255), nullable=False),
        sa.Column("source_uri", sa.Text(), nullable=False),
        sa.Column("source_version", sa.String(length=255), nullable=False),
        sa.Column("source_version_type", sa.String(length=128), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("suite_name", "benchmark_version", name="uq_benchmark_suite_version"),
    )
    op.create_table(
        "task_families",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "suite_version_id",
            sa.Integer(),
            sa.ForeignKey("benchmark_suite_versions.id"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.UniqueConstraint("suite_version_id", "name", name="uq_task_family_suite_version"),
    )
    op.create_table(
        "task_instances",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("task_family_id", sa.Integer(), sa.ForeignKey("task_families.id"), nullable=False),
        sa.Column("instance_id", sa.String(length=255), nullable=False),
        sa.Column("instruction_ref", sa.Text(), nullable=False),
        sa.Column("input_files", sa.JSON(), nullable=False),
        sa.Column("input_artifact_refs", sa.JSON(), nullable=False),
        sa.Column("required_artifacts", sa.JSON(), nullable=False),
        sa.Column("runner_image", sa.Text(), nullable=False),
        sa.Column("runner_entrypoint", sa.JSON(), nullable=False),
        sa.Column("runner_contract", sa.String(length=255), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.UniqueConstraint("task_family_id", "instance_id", name="uq_task_instance_family"),
    )
    op.create_table(
        "runs",
        sa.Column("run_id", sa.String(length=128), primary_key=True),
        sa.Column("project_id", sa.String(length=128), sa.ForeignKey("projects.project_id"), nullable=False),
        sa.Column("owner_team_id", sa.String(length=128), sa.ForeignKey("teams.team_id"), nullable=True),
        sa.Column("owner_team_name_snapshot", sa.String(length=255), nullable=False),
        sa.Column("benchmark_suite", sa.String(length=128), nullable=False),
        sa.Column("benchmark_version", sa.String(length=255), nullable=False),
        sa.Column("task_family", sa.String(length=255), nullable=False),
        sa.Column("task_instance_id", sa.String(length=255), nullable=False),
        sa.Column("task_source_uri", sa.Text(), nullable=False),
        sa.Column("task_input_artifact_refs", sa.JSON(), nullable=False),
        sa.Column("task_required_artifacts", sa.JSON(), nullable=False),
        sa.Column("task_metadata", sa.JSON(), nullable=False),
        sa.Column("model_provider", sa.String(length=128), nullable=False),
        sa.Column("model_name", sa.String(length=255), nullable=False),
        sa.Column("model_mode", sa.String(length=64), nullable=False),
        sa.Column("prompt_template_version", sa.String(length=255), nullable=False),
        sa.Column("model_version", sa.String(length=255), nullable=True),
        sa.Column("model_metadata", sa.JSON(), nullable=False),
        sa.Column("runner_kind", sa.String(length=64), nullable=False),
        sa.Column("sandbox_backend", sa.String(length=64), nullable=False),
        sa.Column("runner_image", sa.Text(), nullable=False),
        sa.Column("runner_entrypoint", sa.JSON(), nullable=False),
        sa.Column("runner_internet_access", sa.Boolean(), nullable=False),
        sa.Column("runner_resource_limits", sa.JSON(), nullable=False),
        sa.Column("runner_metadata", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "run_attempts",
        sa.Column("attempt_id", sa.String(length=160), primary_key=True),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("run_id", "attempt_number", name="uq_run_attempt_number"),
    )
    op.create_table(
        "run_terminal_turns",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("attempt_id", sa.String(length=160), sa.ForeignKey("run_attempts.attempt_id"), nullable=False),
        sa.Column("turn_index", sa.Integer(), nullable=False),
        sa.Column("command", sa.Text(), nullable=False),
        sa.Column("cwd", sa.Text(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("exit_code", sa.Integer(), nullable=False),
        sa.Column("stdout", sa.Text(), nullable=False),
        sa.Column("stderr", sa.Text(), nullable=False),
        sa.Column("changed_paths", sa.JSON(), nullable=False),
        sa.Column("model_call_id", sa.String(length=255), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.UniqueConstraint("attempt_id", "turn_index", name="uq_attempt_turn_index"),
    )
    op.create_table(
        "artifacts",
        sa.Column("artifact_id", sa.String(length=255), primary_key=True),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("attempt_id", sa.String(length=160), sa.ForeignKey("run_attempts.attempt_id"), nullable=True),
        sa.Column("artifact_index", sa.Integer(), nullable=False),
        sa.Column("kind", sa.String(length=64), nullable=False),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("storage_key", sa.Text(), nullable=True),
        sa.Column("media_type", sa.String(length=255), nullable=False),
        sa.Column("sha256", sa.String(length=64), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "evaluator_results",
        sa.Column("id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("attempt_id", sa.String(length=160), sa.ForeignKey("run_attempts.attempt_id"), nullable=True),
        sa.Column("evaluator_id", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=64), nullable=False),
        sa.Column("score", sa.Float(), nullable=True),
        sa.Column("metrics", sa.JSON(), nullable=False),
        sa.Column("verbal_feedback", sa.Text(), nullable=False),
        sa.Column("judge_provider", sa.String(length=128), nullable=False),
        sa.Column("judge_model_name", sa.String(length=255), nullable=False),
        sa.Column("judge_model_version", sa.String(length=255), nullable=True),
        sa.Column("judge_rubric_version", sa.String(length=255), nullable=False),
        sa.Column("judge_metadata", sa.JSON(), nullable=False),
        sa.Column("artifact_refs", sa.JSON(), nullable=False),
        sa.Column("failure_reason", sa.Text(), nullable=True),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "skill_objects",
        sa.Column("skill_id", sa.String(length=128), primary_key=True),
        sa.Column("producing_run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=False),
        sa.Column("representation", sa.String(length=64), nullable=False),
        sa.Column("artifact_refs", sa.JSON(), nullable=False),
        sa.Column("benchmark_suite", sa.String(length=128), nullable=False),
        sa.Column("task_family", sa.String(length=255), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "audit_events",
        sa.Column("event_id", sa.String(length=128), primary_key=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("actor_user_id", sa.String(length=128), sa.ForeignKey("users.user_id"), nullable=True),
        sa.Column("project_id", sa.String(length=128), sa.ForeignKey("projects.project_id"), nullable=True),
        sa.Column("run_id", sa.String(length=128), sa.ForeignKey("runs.run_id"), nullable=True),
        sa.Column("attempt_id", sa.String(length=160), sa.ForeignKey("run_attempts.attempt_id"), nullable=True),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("subject_type", sa.String(length=128), nullable=True),
        sa.Column("subject_id", sa.String(length=255), nullable=True),
        sa.Column("before", sa.JSON(), nullable=True),
        sa.Column("after", sa.JSON(), nullable=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("metadata", sa.JSON(), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=True),
    )


def downgrade() -> None:
    for table_name in (
        "audit_events",
        "skill_objects",
        "evaluator_results",
        "artifacts",
        "run_terminal_turns",
        "run_attempts",
        "runs",
        "task_instances",
        "task_families",
        "benchmark_suite_versions",
        "benchmark_suites",
        "projects",
        "team_memberships",
        "users",
        "teams",
    ):
        op.drop_table(table_name)
