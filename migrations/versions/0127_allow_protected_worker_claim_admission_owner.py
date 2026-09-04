"""Allow protected worker claims to own execution admission reservations.

Revision ID: 0127
Revises: 0126
Create Date: 2026-09-02
"""

from __future__ import annotations

from alembic import op

revision: str = "0127"
down_revision: str | None = "0126"
branch_labels: str | None = None
depends_on: str | None = None


_CONSTRAINT = "execution_admission_reservations_owner_kind_check"

_PROTECTED_RELEASE_FUNCTION = """
CREATE OR REPLACE FUNCTION public.loom_release_legacy_execution_admission()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
  released RECORD;
BEGIN
  PERFORM pg_catalog.pg_advisory_xact_lock_shared(
    pg_catalog.hashtextextended('execution-admission-policy-mutation', 1552)
  );
  IF OLD.state IN ('claimed','running')
     AND NEW.state NOT IN ('claimed','running') THEN
    UPDATE public.execution_admission_reservations
       SET state = 'released', released_at = pg_catalog.statement_timestamp(),
           release_reason = 'trial_left_active_state'
     WHERE trial_id = NEW.id
       AND execution_role = 'attempt'
       AND (
         (owner_kind = 'legacy_worker_claim' AND attempt = NEW.attempt_count)
         OR owner_kind = 'protected_worker_claim'
       )
       AND state = 'active'
    RETURNING team_id, batch_id, environment, region,
              execution_class_id, pool_id
         INTO released;
    IF FOUND THEN
      UPDATE public.execution_admission_policies AS policy
         SET active_count = GREATEST(0, policy.active_count - 1),
             counter_updated_at = pg_catalog.statement_timestamp()
       WHERE CASE policy.scope_kind
               WHEN 'global' THEN policy.scope_key = '*'
               WHEN 'environment' THEN released.environment = policy.scope_key
               WHEN 'region' THEN released.region = policy.scope_key
               WHEN 'team' THEN released.team_id::text = policy.scope_key
               WHEN 'batch' THEN released.batch_id::text = policy.scope_key
               WHEN 'execution_class' THEN
                 released.execution_class_id = policy.scope_key
               WHEN 'pool' THEN released.pool_id = policy.scope_key
               ELSE false
             END;
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;
"""

_LEGACY_RELEASE_FUNCTION = _PROTECTED_RELEASE_FUNCTION.replace(
    "       AND (\n"
    "         (owner_kind = 'legacy_worker_claim' AND attempt = NEW.attempt_count)\n"
    "         OR owner_kind = 'protected_worker_claim'\n"
    "       )",
    "       AND attempt = NEW.attempt_count\n       AND owner_kind = 'legacy_worker_claim'",
)

_TERMINAL_TRIGGER_FUNCTION = """
CREATE OR REPLACE FUNCTION public.loom_close_protected_runtime_trial_claim()
RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
BEGIN
  IF OLD.state IN ('claimed', 'running')
     AND NEW.state IN ('succeeded', 'failed', 'cancelled')
     AND pg_catalog.to_regprocedure(
           'loom_capacity_guard.close_protected_runtime_trial_claim'
           '(uuid,text,text,uuid,integer)'
         ) IS NOT NULL THEN
    PERFORM loom_capacity_guard.close_protected_runtime_trial_claim(
      NEW.id, OLD.state, NEW.state, NEW.worker_id, NEW.attempt_count
    );
  END IF;
  RETURN NEW;
END;
$function$;
REVOKE ALL PRIVILEGES ON FUNCTION
  public.loom_close_protected_runtime_trial_claim() FROM PUBLIC;
"""


def upgrade() -> None:
    op.drop_constraint(
        _CONSTRAINT,
        "execution_admission_reservations",
        schema="public",
        type_="check",
    )
    op.create_check_constraint(
        _CONSTRAINT,
        "execution_admission_reservations",
        "owner_kind IN ('legacy_worker_claim','service_execution_lease','protected_worker_claim')",
        schema="public",
    )
    op.execute(_PROTECTED_RELEASE_FUNCTION)
    op.execute(_TERMINAL_TRIGGER_FUNCTION)
    op.execute(
        """
        CREATE TRIGGER capacity_guard_close_protected_runtime_trial_claim
        AFTER UPDATE OF state ON public.trials
        FOR EACH ROW EXECUTE FUNCTION
          public.loom_close_protected_runtime_trial_claim()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER capacity_guard_close_protected_runtime_trial_claim ON public.trials")
    op.execute("DROP FUNCTION public.loom_close_protected_runtime_trial_claim()")
    op.execute(_LEGACY_RELEASE_FUNCTION)
    op.drop_constraint(
        _CONSTRAINT,
        "execution_admission_reservations",
        schema="public",
        type_="check",
    )
    # Preserve historical protected reservations during rollback while
    # preventing any new protected owner from being inserted.
    op.execute(
        f"""
        ALTER TABLE public.execution_admission_reservations
        ADD CONSTRAINT {_CONSTRAINT}
        CHECK (owner_kind IN ('legacy_worker_claim','service_execution_lease'))
        NOT VALID
        """
    )
