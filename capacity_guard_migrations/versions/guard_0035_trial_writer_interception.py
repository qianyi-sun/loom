"""Account and fence actual trial mutations transactionally.

Revision ID: guard_0035
Revises: guard_0034
Create Date: 2026-09-09
"""

from __future__ import annotations

from alembic import op

revision: str = "guard_0035"
down_revision: str | None = "guard_0034"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE loom_capacity_guard.trial_writer_fence (
          singleton_id smallint PRIMARY KEY CHECK (singleton_id = 1),
          writer_incarnation uuid UNIQUE,
          writer_epoch bigint NOT NULL DEFAULT 1 CHECK (writer_epoch > 0),
          subject_id uuid,
          registration jsonb,
          authority_binding jsonb,
          high_water bigint NOT NULL DEFAULT 0 CHECK (high_water >= 0),
          frozen boolean NOT NULL DEFAULT false,
          freeze_operation_id uuid UNIQUE,
          CHECK (
            (writer_incarnation IS NULL AND subject_id IS NULL
             AND registration IS NULL AND authority_binding IS NULL
             AND high_water = 0 AND NOT frozen)
            OR (writer_incarnation IS NOT NULL AND subject_id IS NOT NULL
                AND registration IS NOT NULL AND authority_binding IS NOT NULL)
          ),
          CHECK (frozen = (freeze_operation_id IS NOT NULL)),
          CHECK (writer_incarnation <> '00000000-0000-0000-0000-000000000000'::uuid),
          CHECK (freeze_operation_id <> '00000000-0000-0000-0000-000000000000'::uuid)
        );
        INSERT INTO loom_capacity_guard.trial_writer_fence(singleton_id) VALUES (1);

        CREATE TABLE loom_capacity_guard.trial_writer_mutations (
          mutation_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
          writer_incarnation uuid NOT NULL REFERENCES
            loom_capacity_guard.trial_writer_fence(writer_incarnation),
          trial_id uuid NOT NULL,
          operation text NOT NULL CHECK (operation IN ('INSERT', 'UPDATE', 'DELETE'))
        );
        CREATE INDEX trial_writer_mutations_incarnation
          ON loom_capacity_guard.trial_writer_mutations(writer_incarnation);
        CREATE TRIGGER trial_writer_mutations_append_only_row
          BEFORE UPDATE OR DELETE ON loom_capacity_guard.trial_writer_mutations
          FOR EACH ROW EXECUTE FUNCTION loom_capacity_guard.reject_append_only_mutation();
        CREATE TRIGGER trial_writer_mutations_append_only_truncate
          BEFORE TRUNCATE ON loom_capacity_guard.trial_writer_mutations
          FOR EACH STATEMENT EXECUTE FUNCTION loom_capacity_guard.reject_append_only_mutation();

        CREATE FUNCTION loom_capacity_guard.lock_trial_writer_statement()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_fence loom_capacity_guard.trial_writer_fence%ROWTYPE;
        BEGIN
          -- Writers retain compatible SHARE locks through commit. An exclusive
          -- counter mutex deadlocks with the existing lock-trial-then-UPDATE
          -- result route. Locking reads still reject stale RR/SERIALIZABLE
          -- snapshots when initialization or freeze changed this sentinel.
          SELECT * INTO STRICT v_fence FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 FOR SHARE;
          IF v_fence.frozen THEN
            RAISE EXCEPTION 'legacy trial writer is frozen' USING ERRCODE = '55000';
          END IF;
          IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'trial writer cannot account truncation' USING ERRCODE = '55000';
          END IF;
          RETURN NULL;
        END
        $function$;

        CREATE FUNCTION loom_capacity_guard.account_trial_writer_mutation()
        RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_fence loom_capacity_guard.trial_writer_fence%ROWTYPE;
        BEGIN
          SELECT * INTO STRICT v_fence FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 FOR SHARE;
          IF v_fence.frozen THEN
            RAISE EXCEPTION 'legacy trial writer is frozen' USING ERRCODE = '55000';
          END IF;
          IF v_fence.writer_incarnation IS NOT NULL THEN
            INSERT INTO loom_capacity_guard.trial_writer_mutations
              (writer_incarnation, trial_id, operation)
            VALUES (v_fence.writer_incarnation,
                    CASE WHEN TG_OP = 'DELETE' THEN OLD.id ELSE NEW.id END, TG_OP);
          END IF;
          RETURN NULL;
        END
        $function$;

        -- Trigger creation takes the public table DDL lock and drains writers
        -- predating interception. This migration is one atomic transaction.
        CREATE TRIGGER capacity_guard_lock_trial_writer
          BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON public.trials
          FOR EACH STATEMENT EXECUTE FUNCTION loom_capacity_guard.lock_trial_writer_statement();
        CREATE TRIGGER zz_capacity_guard_account_trial_writer
          AFTER INSERT OR UPDATE OR DELETE ON public.trials
          FOR EACH ROW EXECUTE FUNCTION loom_capacity_guard.account_trial_writer_mutation();

        CREATE FUNCTION loom_capacity_guard.lock_trial_writer_registration(p_agent uuid)
        RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE v_binding jsonb;
        BEGIN
          IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'trial writer control requires READ COMMITTED'
              USING ERRCODE = '25001';
          END IF;
          SELECT pg_catalog.jsonb_build_object(
                   'registration', pg_catalog.to_jsonb(r),
                   'authority', pg_catalog.to_jsonb(f) - 'reporter_high_water' - 'updated_at')
            INTO v_binding
            FROM loom_capacity_guard.authority_state AS f
            JOIN loom_capacity_guard.agent_registrations AS r
             ON r.singleton_id = f.singleton_id
             AND pg_catalog.to_jsonb(r) @>
                 (pg_catalog.to_jsonb(f) - ARRAY[
                    'reporter_high_water', 'updated_at',
                    'lifecycle_environment', 'lifecycle_namespace'])
           WHERE f.singleton_id = 1 AND r.agent_incarnation = p_agent
             AND r.registration_state = 'registered'
           FOR SHARE OF f, r NOWAIT;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'trial writer registration is unavailable' USING ERRCODE = '55000';
          END IF;
          RETURN v_binding;
        END
        $function$;

        CREATE FUNCTION loom_capacity_guard.initialize_trial_writer_fence(
          p_agent uuid, p_writer uuid
        ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_binding jsonb;
          v_fence loom_capacity_guard.trial_writer_fence%ROWTYPE;
        BEGIN
          IF p_agent IS NULL OR p_writer IS NULL
             OR p_writer = '00000000-0000-0000-0000-000000000000'::uuid THEN
            RAISE EXCEPTION 'trial writer initialization identity is invalid'
              USING ERRCODE = '22023';
          END IF;
          v_binding := loom_capacity_guard.lock_trial_writer_registration(p_agent);
          -- Do not hold authority locks while waiting for writers. NOWAIT
          -- aborts this control transaction; a caller must roll back before
          -- a bounded retry with the same durable operation identity.
          SELECT * INTO STRICT v_fence FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          IF v_fence.writer_incarnation IS NOT NULL THEN
            IF v_fence.writer_incarnation <> p_writer
               OR v_fence.registration IS DISTINCT FROM v_binding->'registration'
               OR v_fence.authority_binding IS DISTINCT FROM v_binding->'authority' THEN
              RAISE EXCEPTION 'trial writer initialization replay changed' USING ERRCODE = '55000';
            END IF;
            RETURN pg_catalog.to_jsonb(v_fence);
          END IF;
          UPDATE loom_capacity_guard.trial_writer_fence
             SET writer_incarnation = p_writer,
                 subject_id = (v_binding->'registration'->>'subject_id')::uuid,
                 registration = v_binding->'registration',
                 authority_binding = v_binding->'authority'
           WHERE singleton_id = 1 RETURNING * INTO STRICT v_fence;
          RETURN pg_catalog.to_jsonb(v_fence);
        END
        $function$;

        CREATE FUNCTION loom_capacity_guard.freeze_trial_writer(
          p_writer uuid, p_operation uuid
        ) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
        AS $function$
        DECLARE
          v_fence loom_capacity_guard.trial_writer_fence%ROWTYPE;
          v_binding jsonb;
          v_high_water bigint;
        BEGIN
          IF p_writer IS NULL OR p_operation IS NULL
             OR p_operation = '00000000-0000-0000-0000-000000000000'::uuid THEN
            RAISE EXCEPTION 'trial writer freeze identity is invalid' USING ERRCODE = '22023';
          END IF;
          SELECT * INTO STRICT v_fence FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1;
          v_binding := loom_capacity_guard.lock_trial_writer_registration(
            (v_fence.registration->>'agent_incarnation')::uuid);
          SELECT * INTO STRICT v_fence FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          IF v_fence.writer_incarnation IS DISTINCT FROM p_writer
             OR v_fence.registration IS DISTINCT FROM v_binding->'registration'
             OR v_fence.authority_binding IS DISTINCT FROM v_binding->'authority' THEN
            RAISE EXCEPTION 'trial writer freeze binding changed' USING ERRCODE = '55000';
          END IF;
          IF v_fence.frozen THEN
            IF v_fence.freeze_operation_id <> p_operation THEN
              RAISE EXCEPTION 'trial writer freeze replay changed' USING ERRCODE = '55000';
            END IF;
            RETURN pg_catalog.to_jsonb(v_fence);
          END IF;
          -- A separate READ COMMITTED statement after exclusive lock acquisition
          -- sees the committed ledger. Sequence MAX is not a committed count:
          -- rollbacks leave sequence gaps and commits may arrive out of order.
          SELECT count(*) INTO v_high_water
            FROM loom_capacity_guard.trial_writer_mutations
           WHERE writer_incarnation = p_writer;
          UPDATE loom_capacity_guard.trial_writer_fence
             SET frozen = true, freeze_operation_id = p_operation, high_water = v_high_water
           WHERE singleton_id = 1 RETURNING * INTO STRICT v_fence;
          RETURN pg_catalog.to_jsonb(v_fence);
        END
        $function$;

        REVOKE ALL ON TABLE loom_capacity_guard.trial_writer_fence FROM PUBLIC;
        REVOKE ALL ON TABLE loom_capacity_guard.trial_writer_mutations FROM PUBLIC;
        REVOKE ALL ON FUNCTION loom_capacity_guard.lock_trial_writer_registration(uuid) FROM PUBLIC;
        REVOKE ALL ON FUNCTION loom_capacity_guard.lock_trial_writer_statement() FROM PUBLIC;
        REVOKE ALL ON FUNCTION loom_capacity_guard.account_trial_writer_mutation() FROM PUBLIC;
        REVOKE ALL ON FUNCTION loom_capacity_guard.initialize_trial_writer_fence(uuid,uuid)
          FROM PUBLIC;
        REVOKE ALL ON FUNCTION loom_capacity_guard.freeze_trial_writer(uuid,uuid) FROM PUBLIC;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        -- Older migrations in this same downgrade transaction also touch
        -- private relations used by terminal processing. Do not retain the
        -- public DDL lock while waiting for any of those relations: that can
        -- invert terminal processing's private -> public order. Acquire the
        -- complete owned guard relation set without waiting before public DDL.
        DO $locks$
        DECLARE v_relation record;
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'loom_capacity_guard' AND c.relkind IN ('r', 'p')
              AND c.relowner <> current_user::regrole::oid
          ) THEN
            RAISE EXCEPTION 'guard retirement relation ownership changed'
              USING ERRCODE = '42501';
          END IF;
          FOR v_relation IN
            SELECT c.relname FROM pg_catalog.pg_class AS c
            JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
            WHERE n.nspname = 'loom_capacity_guard' AND c.relkind IN ('r', 'p')
            ORDER BY c.relname
          LOOP
            EXECUTE pg_catalog.format(
              'LOCK TABLE ONLY loom_capacity_guard.%I IN ACCESS EXCLUSIVE MODE NOWAIT',
              v_relation.relname);
          END LOOP;
          -- DROP TABLE itself can lock inheritance descendants before its
          -- RESTRICT dependency check. Check edges after the ONLY parent locks
          -- stabilize inheritance, before any cleanup can reach another scope.
          IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_inherits AS i
            JOIN pg_catalog.pg_class AS parent ON parent.oid = i.inhparent
            JOIN pg_catalog.pg_namespace AS pn ON pn.oid = parent.relnamespace
            JOIN pg_catalog.pg_class AS child ON child.oid = i.inhrelid
            JOIN pg_catalog.pg_namespace AS cn ON cn.oid = child.relnamespace
            WHERE pn.nspname = 'loom_capacity_guard'
              AND (cn.nspname <> 'loom_capacity_guard'
                   OR child.relowner <> current_user::regrole::oid)
          ) THEN
            RAISE EXCEPTION 'guard retirement inheritance crosses authority'
              USING ERRCODE = '55000';
          END IF;
        END
        $locks$;
        -- Public DDL is also NOWAIT. Any refusal rolls back all partial locks
        -- and removals; callers must retry in a fresh migration transaction.
        SELECT public.loom_drop_trial_writer_triggers();
        DO $block$
        BEGIN
          PERFORM 1 FROM loom_capacity_guard.trial_writer_fence
           WHERE singleton_id = 1 FOR UPDATE NOWAIT;
          IF EXISTS (SELECT 1 FROM loom_capacity_guard.trial_writer_fence
                      WHERE writer_incarnation IS NOT NULL) THEN
            RAISE EXCEPTION 'initialized trial writer fence requires protected retirement'
              USING ERRCODE = '55000';
          END IF;
        END
        $block$;
        DROP FUNCTION loom_capacity_guard.freeze_trial_writer(uuid,uuid);
        DROP FUNCTION loom_capacity_guard.initialize_trial_writer_fence(uuid,uuid);
        DROP FUNCTION loom_capacity_guard.lock_trial_writer_registration(uuid);
        DROP FUNCTION loom_capacity_guard.account_trial_writer_mutation();
        DROP FUNCTION loom_capacity_guard.lock_trial_writer_statement();
        DROP TABLE loom_capacity_guard.trial_writer_mutations;
        DROP TABLE loom_capacity_guard.trial_writer_fence;
        """
    )
