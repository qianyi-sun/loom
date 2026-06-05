"""in_flight_count trigger

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-05
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


_UP_FN = """
CREATE OR REPLACE FUNCTION trials_inflight_delta() RETURNS TRIGGER AS $$
DECLARE
    was_active boolean := OLD.state IN ('claimed', 'running');
    is_active  boolean := NEW.state IN ('claimed', 'running');
BEGIN
    IF was_active AND NOT is_active THEN
        UPDATE team_quotas SET in_flight_count = in_flight_count - 1
         WHERE team_id = OLD.team_id;
    ELSIF is_active AND NOT was_active THEN
        UPDATE team_quotas SET in_flight_count = in_flight_count + 1
         WHERE team_id = NEW.team_id;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

_UP_TRIG = """
CREATE TRIGGER trials_inflight_count
    AFTER UPDATE OF state ON trials
    FOR EACH ROW EXECUTE FUNCTION trials_inflight_delta();
"""


def upgrade() -> None:
    op.execute(_UP_FN)
    op.execute(_UP_TRIG)


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trials_inflight_count ON trials;")
    op.execute("DROP FUNCTION IF EXISTS trials_inflight_delta();")
