"""bind executable allocations to durable execution epochs

Revision ID: capacity_0005
Revises: capacity_0004
Create Date: 2026-08-14
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "capacity_0005"
down_revision: str | Sequence[str] | None = "capacity_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "capacity_allocation_epochs",
        sa.Column("execution_epoch", sa.BigInteger(), nullable=True),
        schema="public",
    )
    op.add_column(
        "capacity_allocation_epochs",
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=True),
        schema="public",
    )
    op.add_column(
        "capacity_allocation_epochs",
        sa.Column(
            "sealed",
            sa.Boolean(),
            nullable=False,
            server_default=sa.true(),
        ),
        schema="public",
    )
    op.add_column(
        "capacity_allocation_epochs",
        sa.Column("allocation_count", sa.BigInteger(), nullable=True),
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocation_status_check",
        "capacity_allocation_epochs",
        type_="check",
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocation_epoch_shadow_only_check",
        "capacity_allocation_epochs",
        type_="check",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_allocation_status_check",
        "capacity_allocation_epochs",
        "status IN ('shadow','failed','executable')",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_allocation_epoch_mode_check",
        "capacity_allocation_epochs",
        "(status IN ('shadow','failed') AND executable = false "
        "AND execution_epoch IS NULL AND execution_manifest_sha256 IS NULL "
        "AND sealed = true AND allocation_count IS NULL) OR "
        "(status = 'executable' AND executable = true "
        "AND execution_epoch IS NOT NULL AND execution_manifest_sha256 IS NOT NULL "
        "AND allocation_count IS NOT NULL AND allocation_count >= 0 "
        "AND COALESCE(jsonb_typeof(complete_payload -> 'allocations') = 'array', false) "
        "AND COALESCE(jsonb_array_length(complete_payload -> 'allocations') "
        "= allocation_count, false))",
        schema="public",
    )
    op.create_foreign_key(
        "capacity_allocation_epoch_execution_fkey",
        "capacity_allocation_epochs",
        "capacity_execution_epochs",
        ["execution_epoch", "execution_manifest_sha256"],
        ["execution_epoch", "execution_manifest_sha256"],
        ondelete="RESTRICT",
        source_schema="public",
        referent_schema="public",
    )
    op.create_unique_constraint(
        "capacity_allocation_epoch_execution_binding_key",
        "capacity_allocation_epochs",
        ["allocation_epoch", "execution_epoch", "execution_manifest_sha256"],
        schema="public",
    )

    op.add_column(
        "capacity_allocations",
        sa.Column("execution_epoch", sa.BigInteger(), nullable=True),
        schema="public",
    )
    op.add_column(
        "capacity_allocations",
        sa.Column("execution_manifest_sha256", sa.Text(), nullable=True),
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocations_shadow_only_check",
        "capacity_allocations",
        type_="check",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_allocations_mode_check",
        "capacity_allocations",
        "(mode = 'shadow' AND executable = false "
        "AND execution_epoch IS NULL AND execution_manifest_sha256 IS NULL) OR "
        "(mode = 'executable' AND executable = true "
        "AND execution_epoch IS NOT NULL AND execution_manifest_sha256 IS NOT NULL)",
        schema="public",
    )
    op.create_foreign_key(
        "capacity_allocation_execution_binding_fkey",
        "capacity_allocations",
        "capacity_allocation_epochs",
        ["allocation_epoch", "execution_epoch", "execution_manifest_sha256"],
        ["allocation_epoch", "execution_epoch", "execution_manifest_sha256"],
        ondelete="RESTRICT",
        source_schema="public",
        referent_schema="public",
    )

    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_allocation_admission_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          authority_incarnation uuid;
          authority_writer_epoch bigint;
          authority_execution_epoch bigint;
          authority_execution_manifest_sha256 text;
          authority_execution_state text;
          authority_effective_ceiling bigint;
          authority_increase_freeze boolean;
          epoch_authority_incarnation uuid;
          epoch_writer_epoch bigint;
          epoch_configuration_epoch bigint;
          epoch_state text;
          epoch_effective_ceiling bigint;
          epoch_effective_rate bigint;
        BEGIN
          IF NEW.status <> 'executable' THEN
            RETURN NEW;
          END IF;

          SELECT
            state.authority_incarnation,
            state.writer_epoch,
            state.execution_epoch,
            state.execution_manifest_sha256,
            state.execution_state,
            state.executable_new_capacity_ceiling,
            state.increase_freeze
          INTO
            authority_incarnation,
            authority_writer_epoch,
            authority_execution_epoch,
            authority_execution_manifest_sha256,
            authority_execution_state,
            authority_effective_ceiling,
            authority_increase_freeze
          FROM public.capacity_authority_state AS state
          WHERE state.singleton_id = 1
          FOR SHARE;

          IF NOT FOUND
             OR authority_execution_state <> 'active'
             OR authority_effective_ceiling <= 0
             OR authority_increase_freeze
             OR authority_writer_epoch <> NEW.writer_epoch
             OR authority_execution_epoch <> NEW.execution_epoch
             OR authority_execution_manifest_sha256
                IS DISTINCT FROM NEW.execution_manifest_sha256 THEN
            RAISE EXCEPTION
              'executable allocation requires the exact active authority'
              USING ERRCODE = '23514';
          END IF;

          SELECT
            epoch.authority_incarnation,
            epoch.current_writer_epoch,
            epoch.configuration_epoch,
            epoch.state,
            epoch.effective_ceiling,
            epoch.effective_rate_per_minute
          INTO
            epoch_authority_incarnation,
            epoch_writer_epoch,
            epoch_configuration_epoch,
            epoch_state,
            epoch_effective_ceiling,
            epoch_effective_rate
          FROM public.capacity_execution_epochs AS epoch
          WHERE epoch.execution_epoch = NEW.execution_epoch
            AND epoch.execution_manifest_sha256 = NEW.execution_manifest_sha256
          FOR SHARE;

          IF NOT FOUND
             OR epoch_authority_incarnation <> authority_incarnation
             OR epoch_writer_epoch <> authority_writer_epoch
             OR epoch_configuration_epoch <> NEW.configuration_epoch
             OR epoch_state <> 'active'
             OR epoch_effective_ceiling <> authority_effective_ceiling
             OR epoch_effective_rate <= 0 THEN
            RAISE EXCEPTION
              'executable allocation requires the exact active authority'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_executable_allocation_admission_guard
        BEFORE INSERT ON public.capacity_allocation_epochs
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_allocation_admission_guard()
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.capacity_executable_allocation_seal_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          current_status text;
          current_sealed boolean;
          expected_allocation_count bigint;
          actual_allocation_count bigint;
        BEGIN
          SELECT status, sealed, allocation_count
          INTO current_status, current_sealed, expected_allocation_count
          FROM public.capacity_allocation_epochs
          WHERE allocation_epoch = NEW.allocation_epoch;

          IF FOUND AND current_status = 'executable' THEN
            IF NOT current_sealed THEN
              RAISE EXCEPTION 'executable allocation epoch must be sealed before commit'
                USING ERRCODE = '23514';
            END IF;
            SELECT count(*)
            INTO actual_allocation_count
            FROM public.capacity_allocations
            WHERE allocation_epoch = NEW.allocation_epoch;
            IF actual_allocation_count <> expected_allocation_count THEN
              RAISE EXCEPTION
                'executable allocation child count does not match sealed parent'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NULL;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE CONSTRAINT TRIGGER capacity_executable_allocation_seal_guard
        AFTER INSERT OR UPDATE ON public.capacity_allocation_epochs
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_executable_allocation_seal_guard()
        """
    )

    op.execute(
        """
        CREATE FUNCTION public.capacity_allocation_epoch_binding_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        BEGIN
          IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
              SELECT 1
              FROM public.capacity_allocation_epochs
              WHERE status = 'executable'
            ) THEN
              RAISE EXCEPTION 'executable allocation epochs are append-only'
                USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
          END IF;
          IF TG_OP = 'DELETE' THEN
            IF OLD.status = 'executable' THEN
              RAISE EXCEPTION 'executable allocation epoch is append-only'
                USING ERRCODE = '23514';
            END IF;
            RETURN OLD;
          END IF;
          IF OLD.status = 'executable' THEN
            IF NOT OLD.sealed
               AND NEW.sealed
               AND (to_jsonb(NEW) - 'sealed')
                   IS NOT DISTINCT FROM (to_jsonb(OLD) - 'sealed') THEN
              RETURN NEW;
            END IF;
            IF NEW IS DISTINCT FROM OLD THEN
              RAISE EXCEPTION 'executable allocation epoch is immutable'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;
          IF ROW(
            NEW.status,
            NEW.executable,
            NEW.execution_epoch,
            NEW.execution_manifest_sha256,
            NEW.sealed,
            NEW.allocation_count
          ) IS DISTINCT FROM ROW(
            OLD.status,
            OLD.executable,
            OLD.execution_epoch,
            OLD.execution_manifest_sha256,
            OLD.sealed,
            OLD.allocation_count
          ) THEN
            RAISE EXCEPTION 'allocation epoch mode binding is immutable'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_allocation_epoch_binding_guard
        BEFORE UPDATE OR DELETE ON public.capacity_allocation_epochs
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_allocation_epoch_binding_guard()
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_allocation_epoch_truncate_guard
        BEFORE TRUNCATE ON public.capacity_allocation_epochs
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.capacity_allocation_epoch_binding_guard()
        """
    )
    op.execute(
        """
        CREATE FUNCTION public.capacity_allocation_binding_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SET search_path = pg_catalog
        AS $$
        DECLARE
          parent_status text;
          parent_executable boolean;
          parent_execution_epoch bigint;
          parent_execution_manifest_sha256 text;
          parent_sealed boolean;
        BEGIN
          IF TG_OP = 'TRUNCATE' THEN
            IF EXISTS (
              SELECT 1
              FROM public.capacity_allocations
              WHERE mode = 'executable'
            ) THEN
              RAISE EXCEPTION 'executable allocations are append-only'
                USING ERRCODE = '23514';
            END IF;
            RETURN NULL;
          END IF;
          IF TG_OP = 'DELETE' THEN
            IF OLD.mode = 'executable' THEN
              RAISE EXCEPTION 'executable allocation is append-only'
                USING ERRCODE = '23514';
            END IF;
            RETURN OLD;
          END IF;
          IF TG_OP = 'UPDATE' AND OLD.mode = 'executable' THEN
            IF NEW IS DISTINCT FROM OLD THEN
              RAISE EXCEPTION 'executable allocation is immutable'
                USING ERRCODE = '23514';
            END IF;
            RETURN NEW;
          END IF;
          IF TG_OP = 'UPDATE' AND ROW(
            NEW.allocation_epoch,
            NEW.mode,
            NEW.executable,
            NEW.execution_epoch,
            NEW.execution_manifest_sha256
          ) IS DISTINCT FROM ROW(
            OLD.allocation_epoch,
            OLD.mode,
            OLD.executable,
            OLD.execution_epoch,
            OLD.execution_manifest_sha256
          ) THEN
            RAISE EXCEPTION 'allocation mode binding is immutable'
              USING ERRCODE = '23514';
          END IF;

          SELECT status, executable, execution_epoch, execution_manifest_sha256, sealed
          INTO parent_status, parent_executable, parent_execution_epoch,
               parent_execution_manifest_sha256, parent_sealed
          FROM public.capacity_allocation_epochs
          WHERE allocation_epoch = NEW.allocation_epoch;

          IF NOT FOUND
             OR parent_status = 'failed'
             OR (parent_status = 'executable' AND parent_sealed)
             OR (
               parent_status = 'executable'
               AND ROW(
                 NEW.mode,
                 NEW.executable,
                 NEW.execution_epoch,
                 NEW.execution_manifest_sha256
               ) IS DISTINCT FROM ROW(
                 'executable'::text,
                 parent_executable,
                 parent_execution_epoch,
                 parent_execution_manifest_sha256
               )
             )
             OR (
               parent_status = 'shadow'
               AND ROW(
                 NEW.mode,
                 NEW.executable,
                 NEW.execution_epoch,
                 NEW.execution_manifest_sha256
               ) IS DISTINCT FROM ROW(
                 'shadow'::text,
                 parent_executable,
                 parent_execution_epoch,
                 parent_execution_manifest_sha256
               )
             ) THEN
            IF parent_status = 'executable' AND parent_sealed THEN
              RAISE EXCEPTION 'executable allocation epoch is sealed'
                USING ERRCODE = '23514';
            END IF;
            RAISE EXCEPTION 'allocation binding must match its parent epoch'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_allocation_binding_guard
        BEFORE INSERT OR UPDATE OR DELETE ON public.capacity_allocations
        FOR EACH ROW
        EXECUTE FUNCTION public.capacity_allocation_binding_guard()
        """
    )
    op.execute(
        """
        CREATE TRIGGER capacity_allocation_truncate_guard
        BEFORE TRUNCATE ON public.capacity_allocations
        FOR EACH STATEMENT
        EXECUTE FUNCTION public.capacity_allocation_binding_guard()
        """
    )


def downgrade() -> None:
    connection = op.get_bind()
    if connection.get_isolation_level().upper() != "READ COMMITTED":
        raise RuntimeError("capacity_0005 downgrade requires READ COMMITTED")
    connection.execute(
        sa.text(
            "LOCK TABLE public.capacity_allocation_epochs, "
            "public.capacity_allocations IN ACCESS EXCLUSIVE MODE"
        )
    )
    if connection.execute(
        sa.text(
            "SELECT EXISTS ("
            "SELECT 1 FROM public.capacity_allocation_epochs "
            "WHERE status = 'executable'"
            ")"
        )
    ).scalar_one():
        raise RuntimeError(
            "cannot downgrade capacity_0005 with executable allocation history"
        )

    op.execute(
        "DROP TRIGGER capacity_executable_allocation_admission_guard "
        "ON public.capacity_allocation_epochs"
    )
    op.execute(
        "DROP FUNCTION public.capacity_executable_allocation_admission_guard()"
    )
    op.execute(
        "DROP TRIGGER capacity_executable_allocation_seal_guard "
        "ON public.capacity_allocation_epochs"
    )
    op.execute("DROP FUNCTION public.capacity_executable_allocation_seal_guard()")
    op.execute(
        "DROP TRIGGER capacity_allocation_truncate_guard "
        "ON public.capacity_allocations"
    )
    op.execute(
        "DROP TRIGGER capacity_allocation_binding_guard "
        "ON public.capacity_allocations"
    )
    op.execute("DROP FUNCTION public.capacity_allocation_binding_guard()")
    op.execute(
        "DROP TRIGGER capacity_allocation_epoch_truncate_guard "
        "ON public.capacity_allocation_epochs"
    )
    op.execute(
        "DROP TRIGGER capacity_allocation_epoch_binding_guard "
        "ON public.capacity_allocation_epochs"
    )
    op.execute("DROP FUNCTION public.capacity_allocation_epoch_binding_guard()")

    op.drop_constraint(
        "capacity_allocation_execution_binding_fkey",
        "capacity_allocations",
        type_="foreignkey",
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocations_mode_check",
        "capacity_allocations",
        type_="check",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_allocations_shadow_only_check",
        "capacity_allocations",
        "mode = 'shadow' AND executable = false",
        schema="public",
    )
    op.drop_column(
        "capacity_allocations",
        "execution_manifest_sha256",
        schema="public",
    )
    op.drop_column("capacity_allocations", "execution_epoch", schema="public")

    op.drop_constraint(
        "capacity_allocation_epoch_execution_fkey",
        "capacity_allocation_epochs",
        type_="foreignkey",
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocation_epoch_execution_binding_key",
        "capacity_allocation_epochs",
        type_="unique",
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocation_epoch_mode_check",
        "capacity_allocation_epochs",
        type_="check",
        schema="public",
    )
    op.drop_constraint(
        "capacity_allocation_status_check",
        "capacity_allocation_epochs",
        type_="check",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_allocation_status_check",
        "capacity_allocation_epochs",
        "status IN ('shadow','failed')",
        schema="public",
    )
    op.create_check_constraint(
        "capacity_allocation_epoch_shadow_only_check",
        "capacity_allocation_epochs",
        "executable = false",
        schema="public",
    )
    op.drop_column(
        "capacity_allocation_epochs",
        "execution_manifest_sha256",
        schema="public",
    )
    op.drop_column(
        "capacity_allocation_epochs",
        "execution_epoch",
        schema="public",
    )
    op.drop_column(
        "capacity_allocation_epochs",
        "allocation_count",
        schema="public",
    )
    op.drop_column(
        "capacity_allocation_epochs",
        "sealed",
        schema="public",
    )
