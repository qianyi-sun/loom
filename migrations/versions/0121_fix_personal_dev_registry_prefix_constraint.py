"""Fix the personal-development registry-prefix check constraint.

Revision ID: 0121
Revises: 0120
"""

from alembic import op

revision = "0121"
down_revision = "0120"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint(
        "personal_dev_candidates_registry_prefix_check",
        "personal_dev_candidates",
        type_="check",
    )
    op.create_check_constraint(
        "personal_dev_candidates_registry_prefix_check",
        "personal_dev_candidates",
        "registry_prefix IS NULL OR ("
        "length(registry_prefix) BETWEEN 1 AND 309 "
        "AND registry_prefix ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]*$' "
        "AND right(registry_prefix, 1) NOT IN ('/', ':') "
        "AND position('://' in registry_prefix) = 0 "
        "AND position('@' in registry_prefix) = 0)",
    )


def downgrade() -> None:
    raise RuntimeError("cannot downgrade 0121: registry-prefix constraint repair is irreversible")
