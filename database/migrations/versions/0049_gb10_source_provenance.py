"""Track GB10 node-agent source checkout provenance.

Two nullable columns keep existing node status rows migratable. The release
gate treats missing values from active nodes as drift when a release target is
specified.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0049"
down_revision = "0048"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "gb10_worker_node_statuses",
        sa.Column("source_git_commit", sa.Text(), nullable=True),
    )
    op.add_column(
        "gb10_worker_node_statuses",
        sa.Column("source_git_dirty", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("gb10_worker_node_statuses", "source_git_dirty")
    op.drop_column("gb10_worker_node_statuses", "source_git_commit")
