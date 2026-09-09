"""Add inert publication key/epoch authority and immutable signed envelopes.

Revision ID: 0135
Revises: 0134
Create Date: 2026-09-05
"""

from alembic import op

revision: str = "0135"
down_revision: str | None = "0134"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Acquire every preexisting table touched by DDL/FKs before retaining any
    # incompatible lock. Fail fast on a busy service, rather than waiting on a
    # parent while holding audit locks needed by its in-flight transaction.
    op.execute("""
        LOCK TABLE public.task_image_materializations,
          public.task_image_materialization_attempts,
          public.task_image_registry_credentials,
          public.task_image_publication_candidates,
          public.trials
          IN ACCESS EXCLUSIVE MODE NOWAIT;
    """)
    # A terminal reference cannot become live again after retention releases it.
    # Check the final row after all BEFORE-trigger transformations, including
    # updates that did not explicitly name state. No cross-row locks or queries.
    op.execute("""
        CREATE FUNCTION public.trials_reject_terminal_reopening() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
          RAISE EXCEPTION 'terminal trial cannot become nonterminal; submit a new trial'
            USING ERRCODE = '23514', CONSTRAINT = 'trials_terminal_state_monotonic';
        END $$;
        REVOKE ALL ON FUNCTION public.trials_reject_terminal_reopening() FROM PUBLIC;
        CREATE TRIGGER trials_terminal_state_monotonic
          AFTER UPDATE ON public.trials FOR EACH ROW
          WHEN (OLD.state IN ('succeeded', 'failed', 'cancelled')
            AND NEW.state NOT IN ('succeeded', 'failed', 'cancelled'))
          EXECUTE FUNCTION public.trials_reject_terminal_reopening();
    """)
    # Issuance is the durable pre-push inventory, even before a candidate exists.
    # Keep published 0131 unchanged; protect both existing and future audit rows.
    op.execute("""
        CREATE TABLE public.task_image_attempt_retention (
          attempt_id UUID PRIMARY KEY REFERENCES public.task_image_materialization_attempts(id)
            ON DELETE RESTRICT,
          observed_at TIMESTAMPTZ NOT NULL,
          unreferenced_since TIMESTAMPTZ,
          retired_at TIMESTAMPTZ,
          canonical_inventory BYTEA,
          inventory_sha256 VARCHAR(64),
          CONSTRAINT task_image_attempt_retention_time_check CHECK (
            isfinite(observed_at) AND (unreferenced_since IS NULL OR
              (isfinite(unreferenced_since) AND unreferenced_since <= observed_at))),
          CONSTRAINT task_image_attempt_retention_shape_check CHECK (
            (retired_at IS NULL AND canonical_inventory IS NULL AND inventory_sha256 IS NULL) OR
            (retired_at IS NOT NULL AND unreferenced_since IS NOT NULL
              AND canonical_inventory IS NOT NULL AND inventory_sha256 IS NOT NULL
              AND retired_at = observed_at AND retired_at >= unreferenced_since
              AND octet_length(canonical_inventory) BETWEEN 1 AND 131072
              AND inventory_sha256 = encode(sha256(canonical_inventory), 'hex')))
        );
        CREATE INDEX task_image_attempt_retention_pending_idx
          ON public.task_image_attempt_retention(unreferenced_since, attempt_id)
          WHERE retired_at IS NULL;
        CREATE INDEX task_image_attempt_retention_retired_idx
          ON public.task_image_attempt_retention(retired_at, attempt_id)
          WHERE retired_at IS NOT NULL;
        CREATE FUNCTION public.task_image_preserve_retirement() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
          IF TG_OP = 'TRUNCATE' THEN
            RAISE EXCEPTION 'task-image retirement evidence is immutable' USING ERRCODE = '23514';
          ELSIF TG_OP = 'DELETE' THEN
            IF OLD.retired_at IS NOT NULL THEN
              RAISE EXCEPTION 'task-image retirement evidence is immutable' USING ERRCODE = '23514';
            END IF;
            RETURN OLD;
          END IF;
          IF OLD.retired_at IS NOT NULL AND NEW IS DISTINCT FROM OLD THEN
            RAISE EXCEPTION 'task-image retirement evidence is immutable' USING ERRCODE = '23514';
          END IF;
          IF NEW.attempt_id IS DISTINCT FROM OLD.attempt_id OR NEW.observed_at < OLD.observed_at THEN
            RAISE EXCEPTION 'task-image retention identity or observation clock regressed'
              USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END $$;
        REVOKE ALL ON FUNCTION public.task_image_preserve_retirement() FROM PUBLIC;
        CREATE TRIGGER task_image_attempt_retention_preserve
          AFTER UPDATE OR DELETE ON public.task_image_attempt_retention
          FOR EACH ROW EXECUTE FUNCTION public.task_image_preserve_retirement();
        CREATE TRIGGER task_image_attempt_retention_no_truncate
          BEFORE TRUNCATE ON public.task_image_attempt_retention
          FOR EACH STATEMENT EXECUTE FUNCTION public.task_image_preserve_retirement();
    """)
    op.execute("""
        CREATE FUNCTION public.task_image_registry_reject_retired_attempt() RETURNS trigger
        LANGUAGE plpgsql VOLATILE SECURITY DEFINER
        SET search_path = pg_catalog SET row_security = off AS $$
        BEGIN
          -- A locking read cannot refresh a RR/SERIALIZABLE transaction snapshot
          -- when retirement changes the marker rather than the immutable attempt.
          IF pg_catalog.current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'task-image credential INSERT requires READ COMMITTED'
              USING ERRCODE = '23514',
                CONSTRAINT = 'task_image_registry_credentials_read_committed';
          END IF;
          -- No earlier materialization/grant/session locks here. This is the same
          -- lock as the existing attempt FK, conflicting with retirement's UPDATE.
          PERFORM 1 FROM public.task_image_materialization_attempts
            WHERE id = NEW.materialization_attempt_id FOR KEY SHARE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'task-image credential attempt is unavailable'
              USING ERRCODE = '23503',
                CONSTRAINT = 'task_image_registry_credentials_attempt_fkey';
          END IF;
          -- Separate SPI statement is essential: VOLATILE + READ COMMITTED sees
          -- a retirement committed while the preceding lock acquisition waited.
          IF EXISTS (
            SELECT 1 FROM public.task_image_attempt_retention
            WHERE attempt_id = NEW.materialization_attempt_id AND retired_at IS NOT NULL
          ) THEN
            RAISE EXCEPTION 'task-image credential attempt is permanently retired'
              USING ERRCODE = '23514',
                CONSTRAINT = 'task_image_registry_credentials_not_retired';
          END IF;
          RETURN NEW;
        END $$;
        REVOKE ALL ON FUNCTION public.task_image_registry_reject_retired_attempt() FROM PUBLIC;
        -- Immediate AFTER ROW checks final identities after BEFORE transformations.
        CREATE TRIGGER task_image_registry_credentials_not_retired
          AFTER INSERT ON public.task_image_registry_credentials FOR EACH ROW
          EXECUTE FUNCTION public.task_image_registry_reject_retired_attempt();
    """)
    op.execute("""
        CREATE FUNCTION task_image_registry_preserve_audit() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
          RAISE EXCEPTION 'task-image registry audit is immutable' USING ERRCODE = '23514';
        END $$;
        CREATE TRIGGER task_image_registry_credentials_preserve
          BEFORE UPDATE OR DELETE OR TRUNCATE ON task_image_registry_credentials
          FOR EACH STATEMENT EXECUTE FUNCTION task_image_registry_preserve_audit();
        CREATE TRIGGER task_image_publication_candidates_preserve
          BEFORE UPDATE OR DELETE OR TRUNCATE ON task_image_publication_candidates
          FOR EACH STATEMENT EXECUTE FUNCTION task_image_registry_preserve_audit();
    """)
    op.execute("""
        CREATE TABLE task_image_publication_jobs (
          operation_id UUID PRIMARY KEY,
          materialization_attempt_id UUID NOT NULL,
          materialization_id UUID NOT NULL,
          attempt_number INTEGER NOT NULL,
          lease_epoch BIGINT NOT NULL,
          builder_id VARCHAR(128) NOT NULL,
          grant_id UUID NOT NULL,
          canonical_snapshot BYTEA NOT NULL,
          snapshot_sha256 VARCHAR(64) NOT NULL,
          created_at TIMESTAMPTZ NOT NULL,
          deadline TIMESTAMPTZ NOT NULL,
          available_at TIMESTAMPTZ NOT NULL,
          state VARCHAR(16) NOT NULL,
          worker_id UUID,
          worker_generation BIGINT NOT NULL,
          worker_expires_at TIMESTAMPTZ,
          failure_code VARCHAR(32),
          completed_at TIMESTAMPTZ,
          canonical_receipt BYTEA,
          receipt_sha256 VARCHAR(64),
          CONSTRAINT task_image_publication_jobs_completion_check CHECK (
            (state = 'completed' AND completed_at IS NOT NULL AND canonical_receipt IS NOT NULL
              AND receipt_sha256 IS NOT NULL AND isfinite(completed_at)
              AND completed_at >= created_at AND completed_at < deadline
              AND octet_length(canonical_receipt) BETWEEN 1 AND 2048
              AND receipt_sha256 = encode(sha256(canonical_receipt), 'hex')) OR
            (state <> 'completed' AND completed_at IS NULL AND canonical_receipt IS NULL AND receipt_sha256 IS NULL)),
          CONSTRAINT task_image_publication_jobs_attempt_uidx UNIQUE (materialization_attempt_id),
          CONSTRAINT task_image_publication_jobs_materialization_uidx UNIQUE (operation_id, materialization_id),
          CONSTRAINT task_image_publication_jobs_attempt_fkey FOREIGN KEY
            (materialization_attempt_id, materialization_id, attempt_number, lease_epoch, builder_id, grant_id)
            REFERENCES task_image_materialization_attempts
            (id, materialization_id, attempt_number, lease_epoch, builder_id, grant_id) ON DELETE RESTRICT,
          CONSTRAINT task_image_publication_jobs_identity_check CHECK (
            operation_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
            attempt_number BETWEEN 1 AND 9007199254740991 AND
            lease_epoch BETWEEN 1 AND 9007199254740991 AND
            worker_generation BETWEEN 0 AND 9007199254740991),
          CONSTRAINT task_image_publication_jobs_snapshot_check CHECK (
            octet_length(canonical_snapshot) BETWEEN 1 AND 4194304 AND
            snapshot_sha256 = encode(sha256(canonical_snapshot), 'hex')),
          CONSTRAINT task_image_publication_jobs_time_check CHECK (
            isfinite(created_at) AND isfinite(deadline) AND isfinite(available_at) AND
            deadline > created_at AND deadline <= created_at + interval '7200 seconds' AND
            available_at >= created_at AND (worker_expires_at IS NULL OR
              (isfinite(worker_expires_at) AND worker_expires_at <= deadline))),
          CONSTRAINT task_image_publication_jobs_state_check CHECK (
            (state = 'running' AND worker_id IS NOT NULL AND
              worker_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
              worker_generation > 0 AND worker_expires_at IS NOT NULL AND failure_code IS NULL) OR
            (state IN ('queued', 'completed') AND worker_id IS NULL AND
              worker_expires_at IS NULL AND failure_code IS NULL) OR
            (state = 'failed' AND worker_id IS NULL AND worker_expires_at IS NULL AND failure_code IS NOT NULL AND
              failure_code IN ('integrity', 'authority_lost', 'verification_failed', 'deadline')))
        );
        CREATE INDEX task_image_publication_jobs_work_idx
          ON task_image_publication_jobs (state, available_at, deadline);
        CREATE FUNCTION task_image_publication_preserve_job() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'publication job is retained' USING ERRCODE = '23514';
          END IF;
          IF (NEW.operation_id, NEW.materialization_attempt_id, NEW.materialization_id,
              NEW.attempt_number, NEW.lease_epoch, NEW.builder_id, NEW.grant_id,
              NEW.canonical_snapshot, NEW.snapshot_sha256, NEW.created_at, NEW.deadline)
             IS DISTINCT FROM
             (OLD.operation_id, OLD.materialization_attempt_id, OLD.materialization_id,
              OLD.attempt_number, OLD.lease_epoch, OLD.builder_id, OLD.grant_id,
              OLD.canonical_snapshot, OLD.snapshot_sha256, OLD.created_at, OLD.deadline)
             OR NEW.worker_generation < OLD.worker_generation
             OR (OLD.state IN ('failed', 'completed') AND NEW IS DISTINCT FROM OLD)
             OR (NEW.worker_id IS DISTINCT FROM OLD.worker_id AND NEW.worker_id IS NOT NULL
                 AND NEW.worker_generation <= OLD.worker_generation) THEN
            RAISE EXCEPTION 'publication job cannot be rewritten' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER task_image_publication_jobs_preserve
          BEFORE UPDATE OR DELETE ON task_image_publication_jobs
          FOR EACH ROW EXECUTE FUNCTION task_image_publication_preserve_job();
    """)
    op.execute("""
        ALTER TABLE task_image_materializations
          ADD COLUMN ready_publication_operation_id UUID,
          ADD CONSTRAINT task_image_materializations_ready_publication_fkey
            FOREIGN KEY (ready_publication_operation_id, id)
            REFERENCES task_image_publication_jobs (operation_id, materialization_id)
            ON DELETE RESTRICT,
          ADD CONSTRAINT task_image_materializations_ready_publication_check CHECK (
            ready_publication_operation_id IS NULL OR
            (state = 'ready' AND jsonb_typeof(registry_images) = 'object'
             AND registry_images <> '{}'::jsonb AND ready_at IS NOT NULL));
        CREATE FUNCTION task_image_materialization_preserve_ready() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
          IF OLD.ready_publication_operation_id IS NOT NULL THEN
            IF NEW.ready_publication_operation_id IS NULL THEN
              IF NEW.registry_images <> '{}'::jsonb OR NEW.ready_at IS NOT NULL
                 OR NEW.state = 'ready' THEN
                RAISE EXCEPTION 'publication ownership requires a full ready reset'
                  USING ERRCODE = '23514';
              END IF;
            ELSIF (NEW.ready_publication_operation_id, NEW.registry_images, NEW.ready_at)
                IS DISTINCT FROM
                (OLD.ready_publication_operation_id, OLD.registry_images, OLD.ready_at) THEN
              RAISE EXCEPTION 'bound publication readiness cannot be rewritten'
                USING ERRCODE = '23514';
            END IF;
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER task_image_materializations_preserve_ready
          BEFORE UPDATE ON task_image_materializations
          FOR EACH ROW EXECUTE FUNCTION task_image_materialization_preserve_ready();
    """)
    op.execute("""
        CREATE TABLE task_image_publication_state (
          singleton_id INTEGER PRIMARY KEY DEFAULT 1,
          revocation_epoch BIGINT NOT NULL DEFAULT 0,
          keyset_version BIGINT NOT NULL DEFAULT 0,
          CONSTRAINT task_image_publication_state_singleton_check CHECK (singleton_id = 1),
          CONSTRAINT task_image_publication_state_counters_check CHECK (
            revocation_epoch BETWEEN 0 AND 9007199254740991 AND
            keyset_version BETWEEN 0 AND 9007199254740991)
        );
        INSERT INTO task_image_publication_state DEFAULT VALUES;
        CREATE TABLE task_image_publication_keys (
          key_id VARCHAR(128) PRIMARY KEY,
          public_key BYTEA NOT NULL,
          activated_at TIMESTAMPTZ NOT NULL,
          status VARCHAR(16) NOT NULL DEFAULT 'active',
          retired_at TIMESTAMPTZ,
          revoked_at TIMESTAMPTZ,
          CONSTRAINT task_image_publication_keys_identity_check CHECK (
            key_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$' AND octet_length(public_key) = 32),
          CONSTRAINT task_image_publication_keys_lifecycle_check CHECK (
            (status = 'active' AND retired_at IS NULL AND revoked_at IS NULL) OR
            (status = 'verify_only' AND retired_at IS NOT NULL AND revoked_at IS NULL) OR
            (status = 'revoked' AND revoked_at IS NOT NULL)),
          CONSTRAINT task_image_publication_keys_interval_check CHECK (
            isfinite(activated_at) AND date_trunc('second', activated_at) = activated_at AND
            (retired_at IS NULL OR (isfinite(retired_at) AND retired_at >= activated_at AND
              date_trunc('second', retired_at) = retired_at)) AND
            (revoked_at IS NULL OR (isfinite(revoked_at) AND revoked_at >= activated_at AND
              date_trunc('second', revoked_at) = revoked_at)) AND
            (retired_at IS NULL OR revoked_at IS NULL OR revoked_at >= retired_at))
        );
        ALTER TABLE task_image_publication_candidates ADD CONSTRAINT
          task_image_publication_candidates_envelope_binding_uidx
          UNIQUE (candidate_id, materialization_attempt_id, component);
        CREATE TABLE task_image_publication_envelopes (
          envelope_id UUID PRIMARY KEY,
          candidate_id UUID NOT NULL,
          materialization_attempt_id UUID NOT NULL,
          component VARCHAR(136) NOT NULL,
          key_id VARCHAR(128) NOT NULL,
          canonical_statement BYTEA NOT NULL,
          statement_sha256 VARCHAR(64) NOT NULL,
          algorithm VARCHAR(16) NOT NULL,
          signature VARCHAR(86) NOT NULL,
          issued_at TIMESTAMPTZ NOT NULL,
          recorded_at TIMESTAMPTZ NOT NULL,
          distributed_keyset_version BIGINT NOT NULL,
          revocation_epoch BIGINT NOT NULL,
          CONSTRAINT task_image_publication_envelopes_candidate_fkey
            FOREIGN KEY (candidate_id, materialization_attempt_id, component)
            REFERENCES task_image_publication_candidates
              (candidate_id, materialization_attempt_id, component) ON DELETE RESTRICT,
          CONSTRAINT task_image_publication_envelopes_key_fkey
            FOREIGN KEY (key_id) REFERENCES task_image_publication_keys(key_id) ON DELETE RESTRICT,
          CONSTRAINT task_image_publication_envelopes_candidate_uidx UNIQUE (candidate_id),
          CONSTRAINT task_image_publication_envelopes_attempt_component_uidx
            UNIQUE (materialization_attempt_id, component),
          CONSTRAINT task_image_publication_envelopes_binding_check CHECK (
            envelope_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
            distributed_keyset_version BETWEEN 1 AND 9007199254740991 AND
            revocation_epoch BETWEEN 0 AND 9007199254740991),
          CONSTRAINT task_image_publication_envelopes_bytes_check CHECK (
            octet_length(canonical_statement) BETWEEN 1 AND 65536 AND
            statement_sha256 = encode(sha256(canonical_statement), 'hex') AND
            algorithm = 'Ed25519' AND signature ~ '^[A-Za-z0-9_-]{86}$'),
          CONSTRAINT task_image_publication_envelopes_time_check CHECK (
            isfinite(issued_at) AND isfinite(recorded_at) AND
            date_trunc('second', issued_at) = issued_at AND
            recorded_at >= issued_at - interval '5 seconds')
        );
        CREATE INDEX task_image_publication_envelopes_key_idx
          ON task_image_publication_envelopes (key_id, envelope_id);
    """)
    op.execute("""
        CREATE FUNCTION task_image_publication_lock_state() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
          -- Statement-level trigger runs BEFORE any key row is locked. All
          -- authority callers must also take this singleton before grant,
          -- projection, current session, materialization, attempt, candidate/job
          -- and later trial-start locks. No signer/registry I/O under any lock.
          PERFORM singleton_id FROM public.task_image_publication_state
            WHERE singleton_id = 1 FOR UPDATE;
          IF NOT FOUND THEN
            RAISE EXCEPTION 'publication state missing' USING ERRCODE = '23514';
          END IF;
          RETURN NULL;
        END $$;
        CREATE TRIGGER task_image_publication_keys_lock_state
          BEFORE INSERT OR UPDATE OR DELETE ON task_image_publication_keys
          FOR EACH STATEMENT EXECUTE FUNCTION task_image_publication_lock_state();
        CREATE TRIGGER task_image_publication_envelopes_lock_state
          BEFORE INSERT ON task_image_publication_envelopes
          FOR EACH STATEMENT EXECUTE FUNCTION task_image_publication_lock_state();

        CREATE FUNCTION task_image_publication_preserve_state() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'publication state is durable' USING ERRCODE = '23514';
          END IF;
          IF NEW.singleton_id <> OLD.singleton_id OR
             NEW.revocation_epoch < OLD.revocation_epoch OR
             NEW.keyset_version < OLD.keyset_version THEN
            RAISE EXCEPTION 'publication state cannot regress' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER task_image_publication_state_preserve
          BEFORE UPDATE OR DELETE ON task_image_publication_state
          FOR EACH ROW EXECUTE FUNCTION task_image_publication_preserve_state();

        CREATE FUNCTION task_image_publication_preserve_key() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            RAISE EXCEPTION 'publication key is retained' USING ERRCODE = '23514';
          END IF;
          IF NEW.key_id <> OLD.key_id OR NEW.public_key <> OLD.public_key OR
             NEW.activated_at <> OLD.activated_at OR
             (OLD.retired_at IS NOT NULL AND NEW.retired_at IS DISTINCT FROM OLD.retired_at) OR
             (OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at) OR
             (OLD.status = 'revoked' AND NEW IS DISTINCT FROM OLD) OR
             (OLD.status = 'verify_only' AND NEW.status = 'active') THEN
            RAISE EXCEPTION 'publication key cannot be rewritten' USING ERRCODE = '23514';
          END IF;
          IF OLD.status <> 'revoked' AND NEW.status = 'revoked' THEN
            UPDATE public.task_image_publication_state
              SET revocation_epoch = revocation_epoch + 1 WHERE singleton_id = 1;
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER task_image_publication_keys_preserve
          BEFORE UPDATE OR DELETE ON task_image_publication_keys
          FOR EACH ROW EXECUTE FUNCTION task_image_publication_preserve_key();

        CREATE FUNCTION task_image_publication_preserve_envelope() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
          RAISE EXCEPTION 'publication envelope is immutable' USING ERRCODE = '23514';
        END $$;
        CREATE TRIGGER task_image_publication_envelopes_preserve
          BEFORE UPDATE OR DELETE ON task_image_publication_envelopes
          FOR EACH ROW EXECUTE FUNCTION task_image_publication_preserve_envelope();
    """)


def downgrade() -> None:
    # Empty, inactive installations are reversible. Once publication authority
    # exists, rollback must preserve its keys, epochs and immutable audit trail.
    # Acquire the entire state-first set without waiting: application readers
    # and writers have different table-access orders. A busy database must abort
    # this downgrade and release all acquired locks, never form a deadlock that
    # could choose a legitimate publication transaction as its victim.
    op.execute("""
        LOCK TABLE public.task_image_publication_state,
          public.task_image_publication_keys,
          public.task_image_publication_envelopes,
          public.task_image_publication_jobs,
          public.task_image_registry_credentials,
          public.task_image_publication_candidates,
          public.task_image_materializations,
          public.task_image_materialization_attempts,
          public.trials,
          public.task_image_attempt_retention
          IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.task_image_attempt_retention WHERE retired_at IS NOT NULL) THEN
            RAISE EXCEPTION 'retirement authority cannot be discarded' USING ERRCODE = '23514';
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM public.task_image_publication_state
            WHERE singleton_id = 1 AND revocation_epoch = 0 AND keyset_version = 0
          ) OR EXISTS (SELECT 1 FROM public.task_image_publication_keys)
            OR EXISTS (SELECT 1 FROM public.task_image_publication_envelopes)
            OR EXISTS (SELECT 1 FROM public.task_image_publication_jobs)
            OR EXISTS (SELECT 1 FROM public.task_image_registry_credentials)
            OR EXISTS (SELECT 1 FROM public.task_image_publication_candidates) THEN
            RAISE EXCEPTION 'publication authority cannot be discarded'
              USING ERRCODE = '23514';
          END IF;
        END $$;
    """)
    op.execute("""
        DROP TRIGGER task_image_registry_credentials_not_retired
          ON public.task_image_registry_credentials;
        DROP FUNCTION public.task_image_registry_reject_retired_attempt();
        DROP TABLE public.task_image_attempt_retention;
        DROP FUNCTION public.task_image_preserve_retirement();
        DROP TRIGGER trials_terminal_state_monotonic ON public.trials;
        DROP FUNCTION public.trials_reject_terminal_reopening();
        DROP TRIGGER task_image_materializations_preserve_ready ON task_image_materializations;
        DROP FUNCTION task_image_materialization_preserve_ready();
        ALTER TABLE task_image_materializations
          DROP CONSTRAINT task_image_materializations_ready_publication_fkey,
          DROP CONSTRAINT task_image_materializations_ready_publication_check,
          DROP COLUMN ready_publication_operation_id;
        DROP TABLE task_image_publication_envelopes;
        DROP TABLE task_image_publication_jobs;
        DROP FUNCTION task_image_publication_preserve_job();
        ALTER TABLE task_image_publication_candidates DROP CONSTRAINT
          task_image_publication_candidates_envelope_binding_uidx;
        DROP TABLE task_image_publication_keys;
        DROP TABLE task_image_publication_state;
        DROP FUNCTION task_image_publication_preserve_envelope();
        DROP FUNCTION task_image_publication_preserve_key();
        DROP FUNCTION task_image_publication_preserve_state();
        DROP FUNCTION task_image_publication_lock_state();
        DROP TRIGGER task_image_publication_candidates_preserve
          ON task_image_publication_candidates;
        DROP TRIGGER task_image_registry_credentials_preserve
          ON task_image_registry_credentials;
        DROP FUNCTION task_image_registry_preserve_audit();
    """)
