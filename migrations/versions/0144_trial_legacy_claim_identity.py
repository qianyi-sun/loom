"""Persist legacy worker claim identities independently of refundable attempts.

Revision ID: 0144
Revises: 0143
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0144"
down_revision = "0143"
branch_labels = None
depends_on = None

_RESERVATION_INDEX = "execution_admission_reservations_trial_attempt_role_uidx"
_RESERVATION_PREDICATE = (
    "state = 'active' OR owner_kind <> 'legacy_worker_claim' "
    "OR release_reason IS DISTINCT FROM 'trial_setup_refund'"
)
_PROTECTED_CLAIM = "loom_capacity_guard.claim_staging_assigned_trial(uuid,text,jsonb)"

_RESERVE = """
CREATE OR REPLACE FUNCTION public.loom_execution_admission_reserve(
  p_trial_id UUID, p_attempt INTEGER, p_execution_role TEXT, p_team_id UUID,
  p_batch_id UUID, p_environment TEXT, p_region TEXT, p_execution_class_id TEXT,
  p_pool_id TEXT, p_owner_kind TEXT, p_owner_id UUID, p_acquired_at TIMESTAMPTZ
) RETURNS UUID LANGUAGE plpgsql AS $function$
DECLARE
  reservation_id UUID;
  policy_row RECORD;
BEGIN
  PERFORM pg_advisory_xact_lock_shared(
    hashtextextended('execution-admission-policy-mutation', 1552)
  );
  FOR policy_row IN
    SELECT policy.scope_kind, policy.scope_key,
           policy.max_concurrent, policy.active_count
      FROM execution_admission_policies policy
     WHERE policy.enabled
       AND CASE policy.scope_kind
             WHEN 'global' THEN policy.scope_key = '*'
             WHEN 'environment' THEN p_environment = policy.scope_key
             WHEN 'region' THEN p_region = policy.scope_key
             WHEN 'team' THEN p_team_id::text = policy.scope_key
             WHEN 'batch' THEN p_batch_id::text = policy.scope_key
             WHEN 'execution_class' THEN p_execution_class_id = policy.scope_key
             WHEN 'pool' THEN p_pool_id = policy.scope_key
             ELSE false
           END
     ORDER BY CASE policy.scope_kind
                WHEN 'global' THEN 1 WHEN 'environment' THEN 2
                WHEN 'region' THEN 3 WHEN 'team' THEN 4
                WHEN 'batch' THEN 5 WHEN 'execution_class' THEN 6
                WHEN 'pool' THEN 7 ELSE 8
              END, policy.scope_key
     FOR UPDATE
  LOOP
    IF policy_row.active_count >= policy_row.max_concurrent THEN
      RETURN NULL;
    END IF;
  END LOOP;
  INSERT INTO execution_admission_reservations (
    trial_id, attempt, execution_role, team_id, batch_id,
    environment, region, execution_class_id, pool_id,
    owner_kind, owner_id, acquired_at
  ) VALUES (
    p_trial_id, p_attempt, p_execution_role, p_team_id, p_batch_id,
    p_environment, p_region, p_execution_class_id, p_pool_id,
    p_owner_kind, p_owner_id, p_acquired_at
  )
  __CONFLICT__
  RETURNING id INTO reservation_id;
  IF reservation_id IS NULL THEN
    RETURN NULL;
  END IF;
  UPDATE execution_admission_policies policy
     SET active_count = policy.active_count + 1, counter_updated_at = NOW()
   WHERE policy.enabled
     AND CASE policy.scope_kind
           WHEN 'global' THEN policy.scope_key = '*'
           WHEN 'environment' THEN p_environment = policy.scope_key
           WHEN 'region' THEN p_region = policy.scope_key
           WHEN 'team' THEN p_team_id::text = policy.scope_key
           WHEN 'batch' THEN p_batch_id::text = policy.scope_key
           WHEN 'execution_class' THEN p_execution_class_id = policy.scope_key
           WHEN 'pool' THEN p_pool_id = policy.scope_key
           ELSE false
         END;
  RETURN reservation_id;
END;
$function$;
"""

_REFUND_REASON = """CASE WHEN owner_kind = 'legacy_worker_claim'
             AND OLD.state = 'claimed' AND OLD.started_at IS NULL
             AND NEW.state = 'queued' AND NEW.failure_reason = 'node_setup_health'
             AND NEW.attempt_count = OLD.attempt_count - 1
           THEN 'trial_setup_refund' ELSE 'trial_left_active_state' END"""

_RELEASE = """
CREATE OR REPLACE FUNCTION public.loom_release_legacy_execution_admission()
RETURNS TRIGGER LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
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
           release_reason = __REASON__
     WHERE trial_id = NEW.id
       AND execution_role = 'attempt'
       AND (
         (owner_kind = 'legacy_worker_claim' AND attempt = __ATTEMPT__)
         OR owner_kind = 'protected_worker_claim'
       )
       AND state = 'active'
    RETURNING team_id, batch_id, environment, region,
              execution_class_id, pool_id INTO released;
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
               WHEN 'execution_class' THEN released.execution_class_id = policy.scope_key
               WHEN 'pool' THEN released.pool_id = policy.scope_key
               ELSE false
             END;
    END IF;
  END IF;
  RETURN NEW;
END;
$function$;
"""


def upgrade() -> None:
    # The guard is owned by a separate migration authority. Never alter its
    # function from the application migration or break an installed old caller.
    definition = op.get_bind().scalar(sa.text(
        "SELECT pg_catalog.pg_get_functiondef(oid) FROM pg_catalog.pg_proc "
        "WHERE oid = pg_catalog.to_regprocedure(:signature)"
    ), {"signature": _PROTECTED_CLAIM})
    if definition is not None and (
        "-- guard_0033: refundable admission compatibility" not in definition
        or "ON CONFLICT (trial_id, attempt, execution_role)" in definition
    ):
        raise RuntimeError("legacy claim identity requires installed guard_0033 compatibility")
    op.execute("LOCK TABLE public.trials IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute(
        "LOCK TABLE public.execution_admission_reservations, "
        "public.execution_admission_policies IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    # No default or backfill: old claims were not issued with this identity.
    op.add_column("trials", sa.Column("legacy_claim_id", postgresql.UUID(as_uuid=True)))
    op.create_check_constraint(
        "trials_legacy_claim_id_nonzero", "trials",
        "legacy_claim_id IS NULL OR "
        "legacy_claim_id <> '00000000-0000-0000-0000-000000000000'::uuid",
    )
    op.create_index(
        "trials_legacy_claim_id_uidx", "trials", ["legacy_claim_id"], unique=True,
        postgresql_where=sa.text("legacy_claim_id IS NOT NULL"),
    )
    # Only released, explicitly refundable legacy slots leave uniqueness.
    # Their rows remain immutable history; all other owners/history still fence
    # the same logical attempt and role, and every active slot remains unique.
    op.drop_constraint(_RESERVATION_INDEX, "execution_admission_reservations", type_="unique")
    op.create_index(
        _RESERVATION_INDEX, "execution_admission_reservations",
        ["trial_id", "attempt", "execution_role"], unique=True,
        postgresql_where=sa.text(_RESERVATION_PREDICATE),
    )
    op.execute(_RESERVE.replace("__CONFLICT__", "ON CONFLICT DO NOTHING"))
    op.execute(_RELEASE.replace("__REASON__", _REFUND_REASON).replace("__ATTEMPT__", "OLD.attempt_count"))


def downgrade() -> None:
    op.execute("LOCK TABLE public.trials IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute(
        "LOCK TABLE public.execution_admission_reservations, "
        "public.execution_admission_policies IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    op.execute("""DO $block$ BEGIN
        IF EXISTS (SELECT 1 FROM public.trials WHERE legacy_claim_id IS NOT NULL)
           OR EXISTS (SELECT 1 FROM public.execution_admission_reservations
                       WHERE release_reason = 'trial_setup_refund') THEN
          RAISE EXCEPTION 'cannot downgrade 0144 with retained legacy claim identities';
        END IF;
        END $block$""")
    op.drop_index("trials_legacy_claim_id_uidx", table_name="trials")
    op.drop_constraint("trials_legacy_claim_id_nonzero", "trials", type_="check")
    op.drop_column("trials", "legacy_claim_id")
    op.drop_index(_RESERVATION_INDEX, table_name="execution_admission_reservations")
    op.create_unique_constraint(
        _RESERVATION_INDEX, "execution_admission_reservations",
        ["trial_id", "attempt", "execution_role"],
    )
    op.execute(_RESERVE.replace("__CONFLICT__", "ON CONFLICT (trial_id, attempt, execution_role) DO NOTHING"))
    op.execute(_RELEASE.replace("__REASON__", "'trial_left_active_state'").replace("__ATTEMPT__", "NEW.attempt_count"))
