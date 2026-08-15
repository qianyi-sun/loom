"""Require every executable claim to consume its exact assigned intent.

Revision ID: guard_0014
Revises: guard_0013
Create Date: 2026-08-15
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0014"
down_revision: str | Sequence[str] | None = "guard_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"


def _install_exact_assignment_guard() -> None:
    op.execute(
        f"""
        CREATE FUNCTION {SCHEMA}.enforce_executable_claim_assignment()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $function$
        BEGIN
          IF NOT EXISTS (
            SELECT 1
              FROM {SCHEMA}.attempt_lifecycle_heads AS head
              JOIN {SCHEMA}.attempt_lifecycle_events AS lifecycle
                ON lifecycle.transition_id = head.transition_id
               AND lifecycle.protected_attempt_id = head.protected_attempt_id
              JOIN {SCHEMA}.executable_admission_events AS worker
                ON worker.intent_id = NEW.intent_id
               AND worker.subject_id = NEW.subject_id
               AND worker.subject_incarnation = NEW.subject_incarnation
               AND worker.worker_id = NEW.worker_id
               AND worker.worker_incarnation = NEW.worker_incarnation
               AND worker.event_kind = 'worker-registered'
             WHERE head.protected_attempt_id = NEW.protected_attempt_id
               AND head.lifecycle_state = 'assigned'
               AND head.executable = false
               AND lifecycle.execution_generation = NEW.execution_generation
               AND lifecycle.requirements_digest = NEW.requirements_digest
               AND lifecycle.lifecycle_state = 'assigned'
               AND lifecycle.executable = false
               AND lifecycle.allowance_id IS NOT NULL
               AND lifecycle.plan_id IS NOT NULL
               AND lifecycle.admission_incarnation IS NOT NULL
               AND lifecycle.submission_intent_id = NEW.intent_id
               AND lifecycle.manager_allocation_epoch =
                   (worker.binding->'execution'->>'allocation_epoch')::bigint
               AND lifecycle.pool_id = worker.binding->>'pool_id'
               AND lifecycle.shape_instance_id = worker.binding->>'shape_instance_id'
               AND NOT EXISTS (
                 SELECT 1
                   FROM {SCHEMA}.executable_admission_events AS successor
                  WHERE successor.intent_id = worker.intent_id
                    AND (
                      successor.event_kind IN ('draining', 'released')
                      OR (
                        successor.event_kind = 'worker-registered'
                        AND successor.protected_registration_epoch >
                            worker.protected_registration_epoch
                      )
                    )
               )
          ) THEN
            RAISE EXCEPTION
              'executable claim requires its exact assigned executable intent'
              USING ERRCODE = '55000';
          END IF;
          RETURN NEW;
        END
        $function$
        """
    )
    op.execute(
        f"""
        CREATE TRIGGER executable_claim_leases_exact_assignment
        BEFORE INSERT ON {SCHEMA}.executable_claim_leases
        FOR EACH ROW EXECUTE FUNCTION {SCHEMA}.enforce_executable_claim_assignment()
        """
    )


def upgrade() -> None:
    op.execute(
        f"""
        LOCK TABLE {SCHEMA}.executable_admission_authority,
                   {SCHEMA}.attempt_lifecycle_events,
                   {SCHEMA}.executable_claim_leases,
                   {SCHEMA}.executable_claim_state,
                   {SCHEMA}.executable_admission_events,
                   {SCHEMA}.executable_claim_terminal_events,
                   {SCHEMA}.attempt_lifecycle_heads IN ACCESS EXCLUSIVE MODE
        """
    )
    op.execute(
        f"""
        DO $block$
        BEGIN
          IF EXISTS (
            SELECT 1
              FROM {SCHEMA}.executable_claim_leases
          ) THEN
            RAISE EXCEPTION
              'pre-0014 executable claim cannot prove its exact assigned executable intent'
              USING ERRCODE = '55000';
          END IF;
        END
        $block$
        """
    )
    _install_exact_assignment_guard()


def downgrade() -> None:
    op.execute(
        f"""
        LOCK TABLE {SCHEMA}.executable_admission_authority,
                   {SCHEMA}.attempt_lifecycle_events,
                   {SCHEMA}.executable_claim_leases,
                   {SCHEMA}.executable_claim_state,
                   {SCHEMA}.executable_admission_events,
                   {SCHEMA}.executable_claim_terminal_events,
                   {SCHEMA}.attempt_lifecycle_heads IN ACCESS EXCLUSIVE MODE
        """
    )
    bind = op.get_bind()
    if bind.execute(
        sa.text(
            f"SELECT EXISTS ("
            f"SELECT 1 FROM {SCHEMA}.executable_admission_events "
            f"UNION ALL SELECT 1 FROM {SCHEMA}.executable_claim_leases)"
        )
    ).scalar_one():
        raise RuntimeError("cannot downgrade guard_0014 while protected executable evidence exists")
    op.execute(
        f"DROP TRIGGER executable_claim_leases_exact_assignment ON {SCHEMA}.executable_claim_leases"
    )
    op.execute(f"DROP FUNCTION {SCHEMA}.enforce_executable_claim_assignment()")
