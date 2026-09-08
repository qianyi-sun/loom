"""Add inert publication key/epoch authority and immutable signed envelopes.

Revision ID: 0133
Revises: 0132
Create Date: 2026-09-05
"""

from alembic import op

revision: str = "0133"
down_revision: str | None = "0132"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
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
    # Serialize the check with the same state-first order as publication writes.
    op.execute("""
        LOCK TABLE public.task_image_publication_state IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE public.task_image_publication_keys IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE public.task_image_publication_envelopes IN ACCESS EXCLUSIVE MODE;
        LOCK TABLE public.task_image_publication_jobs IN ACCESS EXCLUSIVE MODE;
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM public.task_image_publication_state
            WHERE singleton_id = 1 AND revocation_epoch = 0 AND keyset_version = 0
          ) OR EXISTS (SELECT 1 FROM public.task_image_publication_keys)
            OR EXISTS (SELECT 1 FROM public.task_image_publication_envelopes)
            OR EXISTS (SELECT 1 FROM public.task_image_publication_jobs) THEN
            RAISE EXCEPTION 'publication authority cannot be discarded'
              USING ERRCODE = '23514';
          END IF;
        END $$;
    """)
    op.execute("""
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
    """)
