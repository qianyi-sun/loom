"""trial_events: NOTIFY trigger on INSERT — #5 Slice 3d

Revision ID: 0041
Revises: 0040
Create Date: 2026-06-25

Adds a Postgres NOTIFY trigger on `trial_events` so the service's
`/trials/{id}/stream` SSE endpoint can subscribe via LISTEN and emit
events as they're inserted by the worker, replacing the previous
poll-Postgres-every-1.5s loop with push semantics.

Channel name is `trial_events_inserted`; payload is
`<trial_id>:<seq>` so a single listener can fan-out filter by trial
without hitting the table to check ownership. The trigger is
FOR EACH ROW so each insert produces one NOTIFY — batched worker
inserts (CpEventSink flushes ~50 events at once) get a NOTIFY per
row, but PostgreSQL coalesces duplicate notifications within a
single transaction, so the consumer wakes up once per commit.
"""

from __future__ import annotations

from alembic import op

revision = "0041"
down_revision = "0040"
branch_labels = None
depends_on = None


_CHANNEL = "trial_events_inserted"


_CREATE_FN = f"""
CREATE OR REPLACE FUNCTION notify_trial_events_inserted()
RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify(
        '{_CHANNEL}',
        NEW.trial_id::text || ':' || NEW.seq::text
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;
"""

_CREATE_TRIGGER = """
CREATE TRIGGER trial_events_inserted_notify_trigger
AFTER INSERT ON trial_events
FOR EACH ROW
EXECUTE FUNCTION notify_trial_events_inserted();
"""

_DROP_TRIGGER = (
    "DROP TRIGGER IF EXISTS trial_events_inserted_notify_trigger "
    "ON trial_events;"
)
_DROP_FN = "DROP FUNCTION IF EXISTS notify_trial_events_inserted();"


def upgrade() -> None:
    op.execute(_CREATE_FN)
    op.execute(_CREATE_TRIGGER)


def downgrade() -> None:
    op.execute(_DROP_TRIGGER)
    op.execute(_DROP_FN)
