"""trials: CHECK (state='succeeded' implies result IS NOT NULL) NOT VALID — #416 Slice 4

Revision ID: 0039
Revises: 0038
Create Date: 2026-06-25

Public-beta evidence on #416 documented `state='succeeded' AND result IS NULL`
rows surviving the writeback path: the SPA/ATIF showed no verifier
metadata even though the row was terminal-successful, and the #426
benchmark-reward gate could not consume them.

The intended writeback flow in `loom_worker.trial_runner` patches the
trial's `result` column FIRST (via `PATCH /trials/{id}/trajectory_index`)
and only then patches `state='succeeded'` (deferred_success_patch). So
the invariant `state='succeeded' ⇒ result IS NOT NULL` should always
hold, and a CHECK constraint pins it at the DB level so any future
regression in the writeback path errors out at PATCH time rather than
quietly producing useless rows.

Migration safety:
- Added as `NOT VALID` so existing rows are not re-checked at apply
  time — public-beta DBs may already contain violations, and we don't
  want this migration to refuse to start the service.
- Audit query runs first and emits a NOTICE with the violation count so
  operators see exactly how many rows need cleanup before a follow-up
  migration runs `ALTER TABLE ... VALIDATE CONSTRAINT`.
- New writes (after this migration applies) are still gated by the
  constraint — `NOT VALID` only skips the back-check of existing rows.

See docs/runbooks/operator-runbook.md "Trial state/result consistency" for the
operator cleanup command.
"""

from __future__ import annotations

from alembic import op

revision = "0039"
down_revision = "0038"
branch_labels = None
depends_on = None


_CONSTRAINT_NAME = "trials_succeeded_has_result"


def upgrade() -> None:
    # Audit pass: report violation count so an operator running
    # `alembic upgrade head` sees how many legacy rows need cleanup
    # before the follow-up VALIDATE migration can apply.
    op.execute(
        """
        DO $$
        DECLARE
            violation_count integer;
        BEGIN
            SELECT count(*) INTO violation_count
              FROM trials
             WHERE state = 'succeeded' AND result IS NULL;
            IF violation_count > 0 THEN
                RAISE NOTICE '#416 Slice 4: % trial(s) violate '
                             'state=succeeded ⇒ result IS NOT NULL. '
                             'New writes are blocked but legacy rows '
                             'remain. See operator-runbook.md.',
                             violation_count;
            END IF;
        END $$;
        """,
    )
    op.execute(
        f"""
        ALTER TABLE trials
          ADD CONSTRAINT {_CONSTRAINT_NAME}
          CHECK (state != 'succeeded' OR result IS NOT NULL)
          NOT VALID;
        """,
    )


def downgrade() -> None:
    op.execute(
        f"ALTER TABLE trials DROP CONSTRAINT IF EXISTS {_CONSTRAINT_NAME};",
    )
