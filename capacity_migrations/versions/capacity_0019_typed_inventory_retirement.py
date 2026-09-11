"""Pair typed inventory with its manifest and retain exact retirement checks.

Revision ID: capacity_0019
Revises: capacity_0018
"""

from __future__ import annotations

from collections.abc import Sequence
from importlib import import_module

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0019"
down_revision: str | Sequence[str] | None = "capacity_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "capacity_executable_executor_states"
_CHECK = "capacity_executable_executor_retirement_check"
_ROLLBACK_CHECK = "capacity_executor_legacy_inventory_check"
_LEGACY_RETIREMENT = import_module(
    "capacity_migrations.versions.capacity_0011_retirement_heartbeat_freshness"
)._RETIREMENT_CHECK
_TYPED_RETIREMENT = _LEGACY_RETIREMENT.replace(
    "inventory_payload -> 'schema_version' = '2'::jsonb",
    "inventory_payload -> 'schema_version' IN ('2'::jsonb, '3'::jsonb)",
)


def _replace_retirement_check(expression: str) -> None:
    op.drop_constraint(_CHECK, _TABLE, type_="check")
    op.create_check_constraint(_CHECK, _TABLE, expression)


def upgrade() -> None:
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    op.execute(f"ALTER TABLE public.{_TABLE} DROP CONSTRAINT IF EXISTS {_ROLLBACK_CHECK}")
    op.execute("""
        CREATE FUNCTION public.capacity_executor_inventory_version_guard()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER
        SET search_path = pg_catalog AS $$
        DECLARE manifest_version jsonb;
        BEGIN
          IF NEW.inventory_payload IS NULL AND NEW.inventory_high_water = 0 THEN
            RETURN NEW;
          END IF;
          SELECT manifest_payload -> 'schema_version' INTO manifest_version
            FROM public.capacity_execution_epochs
           WHERE execution_epoch = NEW.execution_epoch
             AND execution_manifest_sha256 = NEW.execution_manifest_sha256
           FOR SHARE;
          IF NOT FOUND OR NOT (
              (manifest_version IN ('2'::jsonb, '3'::jsonb)
               AND NEW.inventory_payload -> 'schema_version' = '2'::jsonb)
              OR (manifest_version = '4'::jsonb
                  AND NEW.inventory_payload -> 'schema_version' = '3'::jsonb)
          ) IS TRUE THEN
            RAISE EXCEPTION 'inventory version differs from execution manifest'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$;
        REVOKE ALL ON FUNCTION public.capacity_executor_inventory_version_guard() FROM PUBLIC;
        CREATE TRIGGER capacity_executor_inventory_version_guard
          BEFORE INSERT OR UPDATE ON public.capacity_executable_executor_states
          FOR EACH ROW EXECUTE FUNCTION public.capacity_executor_inventory_version_guard();
    """)
    # Existing rows must obey the same version pairing; do not bless previously
    # malformed evidence merely because it predates the trigger.
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (
            SELECT 1 FROM public.capacity_executable_executor_states s
              LEFT JOIN public.capacity_execution_epochs e
                ON e.execution_epoch = s.execution_epoch
               AND e.execution_manifest_sha256 = s.execution_manifest_sha256
             WHERE (s.inventory_payload IS NOT NULL OR s.inventory_high_water > 0) AND NOT (
               (e.manifest_payload -> 'schema_version' IN ('2'::jsonb, '3'::jsonb)
                AND s.inventory_payload -> 'schema_version' = '2'::jsonb)
               OR (e.manifest_payload -> 'schema_version' = '4'::jsonb
                   AND s.inventory_payload -> 'schema_version' = '3'::jsonb)
             ) IS TRUE
          ) THEN
            RAISE EXCEPTION 'retained inventory version differs from execution manifest';
          END IF;
        END $$;
    """)
    _replace_retirement_check(_TYPED_RETIREMENT)


def downgrade() -> None:
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text(f"""
        SELECT EXISTS (SELECT 1 FROM public.{_TABLE}
          WHERE inventory_payload -> 'schema_version' = '3'::jsonb)
    """)):
        raise RuntimeError("cannot downgrade capacity_0019 with retained typed inventory")
    # A persistent table constraint also fences writers that entered a function
    # before rollback and resume after its old body has been replaced.
    op.create_check_constraint(_ROLLBACK_CHECK, _TABLE,
        "inventory_payload -> 'schema_version' IS DISTINCT FROM '3'::jsonb")
    _replace_retirement_check(_LEGACY_RETIREMENT)
    op.execute(f"DROP TRIGGER capacity_executor_inventory_version_guard ON public.{_TABLE}")
    op.execute("DROP FUNCTION public.capacity_executor_inventory_version_guard()")
