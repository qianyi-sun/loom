from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260529_0003"
down_revision = "20260528_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("evaluator_results") as batch_op:
        batch_op.add_column(sa.Column("mode", sa.String(length=64), nullable=False, server_default="llm_judge"))
        batch_op.alter_column("judge_provider", existing_type=sa.String(length=128), nullable=True)
        batch_op.alter_column("judge_model_name", existing_type=sa.String(length=255), nullable=True)
        batch_op.alter_column("judge_rubric_version", existing_type=sa.String(length=255), nullable=True)

    with op.batch_alter_table("evaluator_results") as batch_op:
        batch_op.alter_column("mode", server_default=None)


def downgrade() -> None:
    op.execute(
        sa.text(
            """
            UPDATE evaluator_results
            SET
                judge_provider = COALESCE(judge_provider, 'unknown'),
                judge_model_name = COALESCE(judge_model_name, 'unknown'),
                judge_rubric_version = COALESCE(judge_rubric_version, 'unknown')
            """
        )
    )
    with op.batch_alter_table("evaluator_results") as batch_op:
        batch_op.alter_column("judge_rubric_version", existing_type=sa.String(length=255), nullable=False)
        batch_op.alter_column("judge_model_name", existing_type=sa.String(length=255), nullable=False)
        batch_op.alter_column("judge_provider", existing_type=sa.String(length=128), nullable=False)
        batch_op.drop_column("mode")
