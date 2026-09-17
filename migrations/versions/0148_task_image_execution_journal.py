"""Retain signed execution revisions and cross-revision one-use starts.

Revision ID: 0148
Revises: 0147
"""

from alembic import op

revision = "0148"
down_revision = "0147"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        LOCK TABLE public.task_image_publication_state,
          public.workers, public.trials, public.task_image_publication_jobs,
          public.task_image_publication_keysets IN SHARE ROW EXCLUSIVE MODE NOWAIT
    """)
    op.execute("""
        CREATE TABLE public.task_image_execution_grants (
          grant_id UUID NOT NULL,
          revision BIGINT NOT NULL,
          claim_id UUID NOT NULL,
          trial_id UUID NOT NULL,
          worker_id UUID NOT NULL,
          operation_id UUID NOT NULL REFERENCES public.task_image_publication_jobs(operation_id) ON DELETE RESTRICT,
          keyset_version BIGINT NOT NULL REFERENCES public.task_image_publication_keysets(keyset_version) ON DELETE RESTRICT,
          canonical_grant BYTEA NOT NULL,
          grant_sha256 VARCHAR(64) NOT NULL,
          canonical_envelope BYTEA,
          envelope_sha256 VARCHAR(64),
          created_at TIMESTAMPTZ NOT NULL,
          revoked_at TIMESTAMPTZ,
          PRIMARY KEY (grant_id, revision),
          CONSTRAINT exec_grants_claim_revision_unique UNIQUE (claim_id, revision),
          CONSTRAINT exec_grants_start_binding_unique UNIQUE (grant_id, revision, claim_id),
          CONSTRAINT exec_grants_identity_check CHECK (
            grant_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND claim_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND revision BETWEEN 1 AND 9007199254740991),
          CONSTRAINT exec_grants_bytes_check CHECK (
            octet_length(canonical_grant) BETWEEN 1 AND 262144
            AND grant_sha256 = encode(sha256(canonical_grant), 'hex')
            AND ((canonical_envelope IS NULL AND envelope_sha256 IS NULL)
              OR (canonical_envelope IS NOT NULL AND envelope_sha256 IS NOT NULL
                AND octet_length(canonical_envelope) BETWEEN 1 AND 524288
                AND envelope_sha256 = encode(sha256(canonical_envelope), 'hex')))),
          CONSTRAINT exec_grants_time_check CHECK (
            isfinite(created_at) AND (revoked_at IS NULL OR
              (isfinite(revoked_at) AND revoked_at >= created_at)))
        )
    """)
    op.execute("""
        CREATE INDEX exec_grants_trial_idx ON public.task_image_execution_grants (trial_id, claim_id, revision)
    """)
    op.execute("""
        CREATE TABLE public.task_image_execution_starts (
          claim_id UUID PRIMARY KEY,
          grant_id UUID NOT NULL,
          revision BIGINT NOT NULL,
          start_id UUID NOT NULL UNIQUE,
          request_sha256 VARCHAR(64) NOT NULL,
          canonical_receipt BYTEA NOT NULL,
          receipt_sha256 VARCHAR(64) NOT NULL,
          consumed_at TIMESTAMPTZ NOT NULL,
          expires_at TIMESTAMPTZ NOT NULL,
          CONSTRAINT exec_starts_grant_fkey FOREIGN KEY (grant_id, revision, claim_id)
            REFERENCES public.task_image_execution_grants (grant_id, revision, claim_id) ON DELETE RESTRICT,
          CONSTRAINT exec_starts_identity_check CHECK (
            start_id <> '00000000-0000-0000-0000-000000000000'::uuid
            AND request_sha256 ~ '^[0-9a-f]{64}$'),
          CONSTRAINT exec_starts_bytes_check CHECK (
            octet_length(canonical_receipt) BETWEEN 1 AND 8192
            AND receipt_sha256 = encode(sha256(canonical_receipt), 'hex')),
          CONSTRAINT exec_starts_time_check CHECK (
            isfinite(consumed_at) AND isfinite(expires_at)
            AND date_trunc('second', consumed_at) = consumed_at
            AND date_trunc('second', expires_at) = expires_at
            AND expires_at > consumed_at AND expires_at <= consumed_at + interval '30 seconds')
        )
    """)
    op.execute("""
        CREATE FUNCTION public.loom_execution_journal_lock() RETURNS TRIGGER
        LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
        BEGIN
          PERFORM singleton_id FROM public.task_image_publication_state
            WHERE singleton_id=1 FOR UPDATE;
          IF NOT FOUND THEN RAISE EXCEPTION 'publication state unavailable' USING ERRCODE='23514'; END IF;
          RETURN NULL;
        END;
        $body$
    """)
    op.execute("""
        CREATE FUNCTION public.loom_execution_journal_immutable() RETURNS TRIGGER
        LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
        BEGIN
          RAISE EXCEPTION 'execution journal is immutable' USING ERRCODE='23514';
        END;
        $body$
    """)
    op.execute("""
        CREATE FUNCTION public.loom_execution_grant_fill_once() RETURNS TRIGGER
        LANGUAGE plpgsql SET search_path=pg_catalog AS $body$
        BEGIN
          IF (to_jsonb(NEW) - ARRAY['canonical_envelope','envelope_sha256','revoked_at'])
              IS DISTINCT FROM
             (to_jsonb(OLD) - ARRAY['canonical_envelope','envelope_sha256','revoked_at'])
            OR (OLD.canonical_envelope IS NOT NULL AND
              ROW(NEW.canonical_envelope,NEW.envelope_sha256) IS DISTINCT FROM
              ROW(OLD.canonical_envelope,OLD.envelope_sha256))
            OR (OLD.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at)
          THEN RAISE EXCEPTION 'execution grant identity is immutable' USING ERRCODE='23514'; END IF;
          RETURN NEW;
        END;
        $body$
    """)
    for table in ("task_image_execution_grants", "task_image_execution_starts"):
        op.execute(f"""
            CREATE TRIGGER execution_journal_state_lock BEFORE INSERT OR UPDATE ON public.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION public.loom_execution_journal_lock()
        """)
        op.execute(f"""
            CREATE TRIGGER execution_journal_no_erasure BEFORE DELETE OR TRUNCATE ON public.{table}
            FOR EACH STATEMENT EXECUTE FUNCTION public.loom_execution_journal_immutable()
        """)
    op.execute("""
        CREATE TRIGGER execution_start_no_update BEFORE UPDATE ON public.task_image_execution_starts
          FOR EACH STATEMENT EXECUTE FUNCTION public.loom_execution_journal_immutable()
    """)
    op.execute("""
        CREATE TRIGGER execution_grant_fill_once BEFORE UPDATE ON public.task_image_execution_grants
          FOR EACH ROW EXECUTE FUNCTION public.loom_execution_grant_fill_once()
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE public.task_image_publication_state,
          public.task_image_execution_grants, public.task_image_execution_starts
          IN ACCESS EXCLUSIVE MODE NOWAIT
    """)
    op.execute("""
        DO $body$ BEGIN
          IF EXISTS (SELECT 1 FROM public.task_image_execution_grants)
            OR EXISTS (SELECT 1 FROM public.task_image_execution_starts)
          THEN RAISE EXCEPTION 'retained execution journal forbids downgrade'; END IF;
        END $body$
    """)
    op.drop_table("task_image_execution_starts")
    op.drop_table("task_image_execution_grants")
    op.execute("DROP FUNCTION public.loom_execution_grant_fill_once()")
    op.execute("DROP FUNCTION public.loom_execution_journal_immutable()")
    op.execute("DROP FUNCTION public.loom_execution_journal_lock()")
