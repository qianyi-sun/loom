from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "20260602_0006"
down_revision = "20260602_0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("artifact_chunks") as batch_op:
        batch_op.alter_column("size_bytes", existing_type=sa.Integer(), nullable=True)
        batch_op.alter_column("sha256", existing_type=sa.String(length=64), nullable=True)


def downgrade() -> None:
    with op.batch_alter_table("artifact_chunks") as batch_op:
        batch_op.alter_column("sha256", existing_type=sa.String(length=64), nullable=False)
        batch_op.alter_column("size_bytes", existing_type=sa.Integer(), nullable=False)
