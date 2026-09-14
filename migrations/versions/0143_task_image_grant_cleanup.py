"""Preserve release history when task-image build authority is revoked.

Revision ID: 0143
Revises: 0142
"""

from alembic import op

revision = "0143"
down_revision = "0142"
branch_labels = None
depends_on = None

_TABLE = "task_image_build_grants"
_CHECK = "task_image_build_grants_state_fields_check"
_ACTIVE_STATES = """
    (state = 'issued' AND invocation_started_at IS NULL
      AND slurm_job_id IS NULL AND ambiguity_settle_until IS NULL
      AND bound_at IS NULL AND released_at IS NULL
      AND revoked_at IS NULL AND revoke_reason IS NULL)
    OR
    (state = 'submitting' AND invocation_started_at IS NOT NULL
      AND slurm_job_id IS NULL AND ambiguity_settle_until IS NOT NULL
      AND bound_at IS NULL AND released_at IS NULL
      AND revoked_at IS NULL AND revoke_reason IS NULL)
    OR
    (state = 'bound' AND invocation_started_at IS NOT NULL
      AND slurm_job_id IS NOT NULL AND ambiguity_settle_until IS NOT NULL
      AND bound_at IS NOT NULL AND released_at IS NULL
      AND revoked_at IS NULL AND revoke_reason IS NULL)
    OR
    (state = 'released' AND invocation_started_at IS NOT NULL
      AND slurm_job_id IS NOT NULL AND ambiguity_settle_until IS NOT NULL
      AND bound_at IS NOT NULL AND released_at IS NOT NULL
      AND revoked_at IS NULL AND revoke_reason IS NULL)
    OR
    (state = 'revoked' AND ambiguity_settle_until IS NOT NULL
      AND revoked_at IS NOT NULL AND revoke_reason IS NOT NULL
"""


def upgrade() -> None:
    op.execute("LOCK TABLE task_image_build_grants IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.drop_constraint(_CHECK, _TABLE, type_="check")
    op.create_check_constraint(
        _CHECK,
        _TABLE,
        _ACTIVE_STATES + """AND (released_at IS NULL OR
          (invocation_started_at IS NOT NULL AND slurm_job_id IS NOT NULL
           AND bound_at IS NOT NULL)))""",
    )


def downgrade() -> None:
    op.execute("LOCK TABLE task_image_build_grants IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""DO $block$ BEGIN
        IF EXISTS (SELECT 1 FROM task_image_build_grants
                   WHERE state = 'revoked' AND released_at IS NOT NULL) THEN
          RAISE EXCEPTION 'cannot downgrade 0143 with retained released-grant revocation';
        END IF;
        END $block$""")
    op.drop_constraint(_CHECK, _TABLE, type_="check")
    op.create_check_constraint(_CHECK, _TABLE, _ACTIVE_STATES + "AND released_at IS NULL)")
