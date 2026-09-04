"""Route protected trial requeues through the capacity guard.

Revision ID: 0128
Revises: 0127
Create Date: 2026-09-02
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision: str = "0128"
down_revision: str | None = "0127"
branch_labels: str | None = None
depends_on: str | None = None


_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION public.loom_transform_protected_runtime_trial_requeue()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
  transformed jsonb;
BEGIN
  IF OLD.state IN ('claimed', 'running')
     AND NEW.state = 'queued'
     AND pg_catalog.to_regprocedure(
           'loom_capacity_guard.transform_protected_runtime_trial_requeue'
           '(uuid,text,uuid,integer,uuid,integer,text,text,timestamp with time zone)'
         ) IS NOT NULL THEN
    transformed := loom_capacity_guard.transform_protected_runtime_trial_requeue(
      NEW.id, OLD.state, OLD.worker_id, OLD.attempt_count,
      NEW.worker_id, NEW.attempt_count, NEW.failure_reason,
      NEW.failure_message, NEW.next_attempt_at
    );
    IF transformed IS NOT NULL THEN
      IF transformed->>'state' = 'protected-pending' THEN
        NEW.state := 'protected-pending';
        NEW.worker_id := NULL;
        NEW.attempt_count := (transformed->>'attempt_count')::integer;
        NEW.failure_reason := transformed->>'failure_reason';
        NEW.failure_message := transformed->>'failure_message';
        NEW.next_attempt_at := (transformed->>'next_attempt_at')::timestamptz;
      ELSIF transformed->>'state' = 'failed' THEN
        NEW.state := 'failed';
        NEW.worker_id := OLD.worker_id;
        NEW.attempt_count := OLD.attempt_count;
        NEW.failure_reason := transformed->>'failure_reason';
        NEW.failure_message := transformed->>'failure_message';
        NEW.next_attempt_at := NULL;
        NEW.finished_at := COALESCE(
          NEW.finished_at,
          (transformed->>'finished_at')::timestamptz
        );
      ELSE
        RAISE EXCEPTION 'protected runtime requeue returned an invalid state'
          USING ERRCODE = '55000';
      END IF;
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;
REVOKE ALL PRIVILEGES ON FUNCTION
  public.loom_transform_protected_runtime_trial_requeue() FROM PUBLIC;
"""


def upgrade() -> None:
    op.execute(_TRIGGER_FUNCTION)
    op.execute(
        """
        CREATE TRIGGER capacity_guard_transform_protected_runtime_trial_requeue
        BEFORE UPDATE OF state ON public.trials
        FOR EACH ROW EXECUTE FUNCTION
          public.loom_transform_protected_runtime_trial_requeue()
        """
    )


def downgrade() -> None:
    bind = op.get_bind()
    op.execute(
        "LOCK TABLE public.trials, public.execution_admission_reservations IN ACCESS EXCLUSIVE MODE"
    )
    active = bind.execute(
        sa.text(
            """
            SELECT EXISTS (
              SELECT 1
                FROM public.trials AS trial
                JOIN public.execution_admission_reservations AS reservation
                  ON reservation.trial_id = trial.id
               WHERE trial.state IN ('claimed', 'running')
                 AND reservation.execution_role = 'attempt'
                 AND reservation.owner_kind = 'protected_worker_claim'
                 AND reservation.state = 'active'
            )
            """
        )
    ).scalar_one()
    if active:
        raise RuntimeError("cannot downgrade 0128 while protected claims can be requeued")
    op.execute(
        "DROP TRIGGER capacity_guard_transform_protected_runtime_trial_requeue ON public.trials"
    )
    op.execute("DROP FUNCTION public.loom_transform_protected_runtime_trial_requeue()")
