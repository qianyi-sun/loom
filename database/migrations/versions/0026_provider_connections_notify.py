"""provider_connections: NOTIFY trigger on mutation (#190)

Revision ID: 0026
Revises: 0025
Create Date: 2026-06-18

Adds a Postgres NOTIFY trigger on provider_connections so the
`loom_egress_xds` control plane (#78 Phase C) gets sub-second
pushes when connection rows change, without polling.

The trigger fires on INSERT/UPDATE/DELETE with channel name
`provider_connections_changed`. Payload is empty; consumers re-read
the table on signal (the row set is small — bounded by team count ×
average connections-per-team, currently <1k rows).
"""

from __future__ import annotations

from alembic import op

revision = "0026"
down_revision = "0025"
branch_labels = None
depends_on = None


_CREATE_FN = """
CREATE OR REPLACE FUNCTION notify_provider_connections_changed()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('provider_connections_changed', '');
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

_CREATE_TRIGGER = """
CREATE TRIGGER provider_connections_changed_trigger
AFTER INSERT OR UPDATE OR DELETE ON provider_connections
FOR EACH STATEMENT
EXECUTE FUNCTION notify_provider_connections_changed();
"""

_DROP_TRIGGER = (
    "DROP TRIGGER IF EXISTS provider_connections_changed_trigger "
    "ON provider_connections;"
)
_DROP_FN = "DROP FUNCTION IF EXISTS notify_provider_connections_changed();"


def upgrade() -> None:
    op.execute(_CREATE_FN)
    op.execute(_CREATE_TRIGGER)


def downgrade() -> None:
    op.execute(_DROP_TRIGGER)
    op.execute(_DROP_FN)
