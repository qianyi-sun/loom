"""Allow the existing application trigger definers to resolve guarded calls.

Revision ID: guard_0030
Revises: guard_0029
Create Date: 2026-09-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0030"
down_revision: str | None = "guard_0029"
branch_labels: str | None = None
depends_on: str | None = None

_BRIDGES = (
    (
        "public.loom_close_protected_runtime_trial_claim()",
        "loom_capacity_guard.close_protected_runtime_trial_claim(uuid,text,text,uuid,integer)",
    ),
    (
        "public.loom_transform_protected_runtime_trial_requeue()",
        "loom_capacity_guard.transform_protected_runtime_trial_requeue"
        "(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)",
    ),
)


def upgrade() -> None:
    bind = op.get_bind()
    owners: set[str] = set()
    for bridge, guarded in _BRIDGES:
        row = (
            bind.execute(
                sa.text(
                    "SELECT pg_catalog.pg_get_userbyid(bridge.proowner) AS owner, "
                    "bridge.prosecdef, bridge.proconfig, "
                    "pg_catalog.has_function_privilege(bridge.proowner, guarded.oid, 'EXECUTE') "
                    "AS executable FROM pg_catalog.pg_proc AS bridge "
                    "JOIN pg_catalog.pg_proc AS guarded "
                    "ON guarded.oid = pg_catalog.to_regprocedure(:guarded) "
                    "WHERE bridge.oid = pg_catalog.to_regprocedure(:bridge)"
                ),
                {"bridge": bridge, "guarded": guarded},
            )
            .mappings()
            .one_or_none()
        )
        if (
            row is None
            or not isinstance(row["owner"], str)
            or not row["owner"]
            or row["prosecdef"] is not True
            or row["proconfig"] != ["search_path=pg_catalog"]
            or row["executable"] is not True
        ):
            raise RuntimeError("application trigger guard authority is unavailable")
        owners.add(row["owner"])
    for owner in sorted(owners):
        quoted = bind.dialect.identifier_preparer.quote(owner)
        op.execute(f"GRANT USAGE ON SCHEMA loom_capacity_guard TO {quoted}")


def downgrade() -> None:
    # Schema resolution is shared by both pre-existing guarded trigger calls.
    # Do not revoke a potentially pre-existing grant or break those callers on
    # rollback. USAGE conveys neither table access nor function execution; the
    # earlier migrations retain ownership of their exact EXECUTE grants.
    pass
