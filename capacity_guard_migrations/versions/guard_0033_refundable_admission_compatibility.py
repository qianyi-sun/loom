"""Preserve protected admission across the application refundable-slot index.

Revision ID: guard_0033
Revises: guard_0032
"""

import sqlalchemy as sa
from alembic import op

revision = "guard_0033"
down_revision = "guard_0032"
branch_labels = None
depends_on = None

_FUNCTION = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"
_OLD = "ON CONFLICT (trial_id, attempt, execution_role) DO NOTHING"
_NEW = "-- guard_0033: refundable admission compatibility\n          ON CONFLICT DO NOTHING"


def upgrade() -> None:
    function = op.get_bind().execute(sa.text(
        "SELECT pg_catalog.pg_get_functiondef(oid) AS definition, prosecdef, proconfig "
        "FROM pg_catalog.pg_proc WHERE oid = CAST(:function AS regprocedure)"
    ), {"function": _FUNCTION}).mappings().one()
    if function["prosecdef"] is not True or function["proconfig"] != ["search_path=pg_catalog"]:
        raise RuntimeError("refundable admission requires the admitted protected claim function")
    definition = function["definition"]
    if definition.count(_NEW) == 1 and _OLD not in definition:
        return
    if definition.count(_OLD) != 1 or "guard_0033" in definition:
        raise RuntimeError("refundable admission requires one unmodified conflict target")
    # Target-free DO NOTHING retains every uniqueness constraint. This works
    # with both the old full constraint and the new conditional index, while
    # preserving owner, SECURITY DEFINER, search_path and all previous fences.
    op.execute(definition.replace(_OLD, _NEW))


def downgrade() -> None:
    # Safe on both application schemas. Retain compatibility on label rollback;
    # restoring the old target would break a still-upgraded application schema.
    pass
