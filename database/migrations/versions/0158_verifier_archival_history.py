"""Record archival recovery timestamps in execution lease history.

Revision ID: 0158
Revises: 0157
"""
import sqlalchemy as sa
from alembic import op

revision = "0158"
down_revision = "0157"
branch_labels = None
depends_on = None

_TRIGGER = "execution_leases_history_trigger"
_COLUMN = "materialization_recovery_requested_at"


def _replace_trigger(*, enable: bool) -> None:
    definition = op.get_bind().scalar(sa.text(
        "SELECT pg_get_triggerdef(oid) FROM pg_trigger "
        "WHERE tgrelid = 'public.execution_leases'::regclass AND tgname = :name"
    ), {"name": _TRIGGER})
    if not isinstance(definition, str) or definition.count("UPDATE OF ") != 1:
        raise RuntimeError("missing or unexpected execution lease history trigger")
    before, after = "UPDATE OF ", f"UPDATE OF {_COLUMN}, "
    if not enable:
        before, after = after, before
    if definition.count(before) != 1 or (enable and _COLUMN in definition):
        raise RuntimeError("unexpected archival recovery history trigger")
    op.execute(sa.text(f"DROP TRIGGER {_TRIGGER} ON public.execution_leases"))
    op.execute(sa.text(definition.replace(before, after, 1)))


def upgrade() -> None:
    _replace_trigger(enable=True)
    # 0157 recorded an immutable timestamp but did not trigger a history row.
    # Observe those leases now without changing their values or pretending the
    # new snapshot was recorded at the earlier recovery time. Trigger replacement
    # holds the table lock through this transaction, fencing concurrent recovery.
    op.execute(sa.text("""
        UPDATE public.execution_leases AS lease
        SET materialization_recovery_requested_at = lease.materialization_recovery_requested_at
        WHERE lease.materialization_recovery_requested_at IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM public.execution_lease_history AS history
            WHERE history.lease_id = lease.id
              AND (history.snapshot_json->>'materialization_recovery_requested_at')::timestamptz
                  = lease.materialization_recovery_requested_at
          )
    """))


def downgrade() -> None:
    # Preserve every audit row, including snapshots observed during upgrade.
    _replace_trigger(enable=False)
