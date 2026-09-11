"""Exclude native publications from both unsigned protected claim boundaries.

Revision ID: guard_0031
Revises: guard_0030
Create Date: 2026-09-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0031"
down_revision: str | None = "guard_0030"
branch_labels: str | None = None
depends_on: str | None = None

_FUNCTION = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"
_OLD = "AND materialization.state = 'ready'"
_NEW = _OLD + "\n             AND materialization.ready_publication_operation_id IS NULL"


def upgrade() -> None:
    bind = op.get_bind()
    function = bind.execute(sa.text(
        "SELECT pg_catalog.pg_get_functiondef(oid) AS definition, prosecdef, proconfig, "
        "pg_catalog.has_column_privilege(proowner, 'public.task_image_materializations', "
        "'ready_publication_operation_id', 'SELECT') AS can_read_native_identity "
        "FROM pg_catalog.pg_proc WHERE oid = CAST(:function AS regprocedure)"
    ), {"function": _FUNCTION}).mappings().one()
    if (
        function["prosecdef"] is not True
        or function["proconfig"] != ["search_path=pg_catalog"]
        or function["can_read_native_identity"] is not True
    ):
        raise RuntimeError("native reader fence requires the admitted function and application column grant")
    definition = function["definition"]
    if definition.count(_OLD) != 2:
        raise RuntimeError("native reader fence requires exactly two existing ready predicates")
    if definition.count(_NEW) == 2:
        return  # A downgrade retains this security fence; upgrading is idempotent.
    if "materialization.ready_publication_operation_id" in definition:
        raise RuntimeError("native reader fence found a partial or different installed predicate")
    # CREATE OR REPLACE preserves identity, owner, privileges and function settings.
    # Patch the effective definition, including all guard_0025 retry amendments.
    op.execute(definition.replace(_OLD, _NEW))


def downgrade() -> None:
    # Do not reopen unsigned native execution when rolling back a schema label.
    # No object/signature/privilege is removed or added by this migration. Earlier
    # owning migrations still control eventual function removal. The independently
    # owned application-side SELECT grant is likewise retained, not revoked here.
    pass
