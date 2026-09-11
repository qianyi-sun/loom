"""Preserve exact retirement sequence checks for bounded typed inventory batches.

Revision ID: capacity_0022
Revises: capacity_0021
"""

from collections.abc import Sequence
from importlib import import_module

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0022"
down_revision: str | Sequence[str] | None = "capacity_0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "capacity_executable_executor_states"
_CHECK = "capacity_executable_executor_retirement_check"
_FENCE = "capacity_executor_inline_inventory_check"
_PREVIOUS = import_module(
    "capacity_migrations.versions.capacity_0019_typed_inventory_retirement"
)._TYPED_RETIREMENT
# All valid inventory strings are ASCII identifiers, enums, UUIDs, digests or
# canonical base64. This serializer therefore has exactly Python's byte length.
_SIZE = "octet_length(public.capacity_executable_canonical_jsonb_text(inventory_payload))"
_CHUNKED = f"(inventory_payload -> 'schema_version' = '3'::jsonb AND {_SIZE} > 32768)"
# The persistent rollback fence must not depend on a function removed by older
# migrations. JSONB's spaced representation is a conservative upper bound for
# valid ASCII inventory bytes; near-threshold small payloads may refuse rollback.
_ROLLBACK_TOO_LARGE = "(inventory_payload -> 'schema_version' = '3'::jsonb AND octet_length(inventory_payload::text) > 32768)"
_COUNT = f"(2 + CASE WHEN {_CHUNKED} THEN ({_SIZE} + 32767) / 32768 ELSE 0 END)"
_RETIREMENT = _PREVIOUS.replace("journal_high_water - 2", f"journal_high_water - {_COUNT}")


def _replace(expression: str) -> None:
    op.drop_constraint(_CHECK, _TABLE, type_="check")
    op.create_check_constraint(_CHECK, _TABLE, expression)


def upgrade() -> None:
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    op.execute(f"ALTER TABLE public.{_TABLE} DROP CONSTRAINT IF EXISTS {_FENCE}")
    op.add_column(_TABLE, sa.Column("chunked_inventory_seen", sa.Boolean(), nullable=False, server_default=sa.text("false")))
    op.execute(f"UPDATE public.{_TABLE} SET chunked_inventory_seen=true WHERE {_CHUNKED}")
    op.execute(f"""
        CREATE FUNCTION public.capacity_executor_chunked_inventory_history_guard()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$
        BEGIN
          NEW.chunked_inventory_seen := coalesce(NEW.chunked_inventory_seen,false)
            OR CASE WHEN TG_OP='UPDATE' THEN OLD.chunked_inventory_seen ELSE false END
            OR coalesce(({_CHUNKED.replace('inventory_payload', 'NEW.inventory_payload')}),false);
          RETURN NEW;
        END $$;
        REVOKE ALL ON FUNCTION public.capacity_executor_chunked_inventory_history_guard() FROM PUBLIC;
        CREATE TRIGGER capacity_executor_chunked_inventory_history_guard
          BEFORE INSERT OR UPDATE ON public.{_TABLE} FOR EACH ROW
          EXECUTE FUNCTION public.capacity_executor_chunked_inventory_history_guard();
    """)
    _replace(_RETIREMENT)


def downgrade() -> None:
    op.execute(f"LOCK TABLE public.{_TABLE} IN ACCESS EXCLUSIVE MODE")
    if op.get_bind().scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM public.{_TABLE} WHERE chunked_inventory_seen)")):
        raise RuntimeError("cannot downgrade capacity_0022 with retained chunked inventory")
    if op.get_bind().scalar(sa.text(f"SELECT EXISTS (SELECT 1 FROM public.{_TABLE} WHERE {_ROLLBACK_TOO_LARGE})")):
        raise RuntimeError("cannot downgrade capacity_0022: retained inventory exceeds legacy rollback bound")
    # Reject late typed large writers after downgrade, not only currently safe
    # rows. Old code cannot reconstruct their confirmation hash or replay batch.
    op.create_check_constraint(_FENCE, _TABLE, f"NOT coalesce({_ROLLBACK_TOO_LARGE}, false)")
    _replace(_PREVIOUS)
    op.execute(f"DROP TRIGGER capacity_executor_chunked_inventory_history_guard ON public.{_TABLE}")
    op.execute("DROP FUNCTION public.capacity_executor_chunked_inventory_history_guard()")
    op.drop_column(_TABLE, "chunked_inventory_seen")
