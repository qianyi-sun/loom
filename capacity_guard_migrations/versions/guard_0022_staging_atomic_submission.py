"""Admit atomic staging submissions with protected seven-day retention.

Revision ID: guard_0022
Revises: guard_0021
Create Date: 2026-09-02
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "guard_0022"
down_revision: str | Sequence[str] | None = "guard_0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "loom_capacity_guard"
FUNCTION = (
    "submit_inert_trial_projection"
    "(uuid,jsonb,bytea,text,jsonb,bytea,text,bytea,text)"
)
_SCOPE_READY_CLAUSE = """IF NOT FOUND THEN
            RAISE EXCEPTION 'atomic trial submission lifecycle scope is unavailable'
              USING ERRCODE = '55000';
          END IF;

          v_requested_trial_id := (p_payload->>'trial_id')::uuid;"""
_STAGING_REJECTION_CLAUSE = """IF NOT FOUND THEN
            RAISE EXCEPTION 'atomic trial submission lifecycle scope is unavailable'
              USING ERRCODE = '55000';
          END IF;
          IF v_scope.lifecycle_environment = 'staging' THEN
            RAISE EXCEPTION
              'atomic trial submission protected retention is unavailable for staging'
              USING ERRCODE = '55000';
          END IF;

          v_requested_trial_id := (p_payload->>'trial_id')::uuid;"""


def _replace_function_clause(old: str, new: str) -> None:
    escaped_old = old.replace("'", "''")
    escaped_new = new.replace("'", "''")
    op.execute(
        f"""
        DO $$
        DECLARE
          v_definition text;
        BEGIN
          SELECT pg_get_functiondef(
            '{SCHEMA}.{FUNCTION}'::regprocedure
          ) INTO v_definition;
          IF position('{escaped_old}' in v_definition) = 0 THEN
            RAISE EXCEPTION
              'staging atomic-submission function clause was not found';
          END IF;
          IF length(v_definition) - length(replace(v_definition, '{escaped_old}', ''))
               <> length('{escaped_old}') THEN
            RAISE EXCEPTION
              'staging atomic-submission function clause was ambiguous';
          END IF;
          EXECUTE replace(v_definition, '{escaped_old}', '{escaped_new}');
        END $$;
        """
    )


def upgrade() -> None:
    op.execute(
        f"""
        LOCK TABLE {SCHEMA}.authority_state,
                   {SCHEMA}.agent_runtime_authority,
                   {SCHEMA}.atomic_trial_submissions,
                   {SCHEMA}.trial_requirements,
                   {SCHEMA}.trial_attempts,
                   {SCHEMA}.attempt_lifecycle_events,
                   {SCHEMA}.attempt_lifecycle_heads IN ACCESS EXCLUSIVE MODE
        """
    )
    _replace_function_clause(_STAGING_REJECTION_CLAUSE, _SCOPE_READY_CLAUSE)


def downgrade() -> None:
    bind = op.get_bind()
    if bind.execute(
        sa.text(
            f"SELECT EXISTS (SELECT 1 FROM {SCHEMA}.atomic_trial_submissions "
            "WHERE lifecycle_environment = 'staging')"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade guard_0022 while protected staging submissions exist"
        )
    op.execute(
        f"""
        LOCK TABLE {SCHEMA}.authority_state,
                   {SCHEMA}.agent_runtime_authority,
                   {SCHEMA}.atomic_trial_submissions,
                   {SCHEMA}.trial_requirements,
                   {SCHEMA}.trial_attempts,
                   {SCHEMA}.attempt_lifecycle_events,
                   {SCHEMA}.attempt_lifecycle_heads IN ACCESS EXCLUSIVE MODE
        """
    )
    _replace_function_clause(_SCOPE_READY_CLAUSE, _STAGING_REJECTION_CLAUSE)
