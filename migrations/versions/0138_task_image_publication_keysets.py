"""Retain authenticated signed keysets without enabling runtime distribution.

Revision ID: 0138
Revises: 0137
Create Date: 2026-09-10
"""

from alembic import op

revision: str = "0138"
down_revision: str | None = "0137"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # State-first, nonwaiting acquisition of every preexisting DDL/FK parent.
    op.execute("""
      LOCK TABLE public.task_image_publication_state, public.task_image_publication_keys
        IN ACCESS EXCLUSIVE MODE NOWAIT;
      CREATE TABLE task_image_publication_keysets (
        keyset_version bigint PRIMARY KEY,
        revocation_epoch bigint NOT NULL,
        environment varchar(128) NOT NULL,
        execution_key_id varchar(128) NOT NULL,
        root_sha256 varchar(64) NOT NULL,
        keyset_sha256 varchar(64) NOT NULL,
        snapshot_sha256 varchar(64) NOT NULL,
        canonical_envelope bytea NOT NULL,
        issued_at timestamptz NOT NULL,
        expires_at timestamptz NOT NULL,
        CONSTRAINT task_image_publication_keysets_counters_check CHECK (
          keyset_version BETWEEN 1 AND 9007199254740991 AND revocation_epoch BETWEEN 0 AND 9007199254740991),
        CONSTRAINT task_image_publication_keysets_identity_check CHECK (
          environment ~ '^[a-z0-9][a-z0-9_.-]{0,127}$' AND execution_key_id ~ '^[a-z0-9][a-z0-9_.-]{0,127}$'
          AND root_sha256 ~ '^[0-9a-f]{64}$' AND keyset_sha256 ~ '^[0-9a-f]{64}$'),
        CONSTRAINT task_image_publication_keysets_bytes_check CHECK (
          octet_length(canonical_envelope) BETWEEN 1 AND 131072
          AND snapshot_sha256 = encode(sha256(canonical_envelope), 'hex')),
        CONSTRAINT task_image_publication_keysets_interval_check CHECK (
          isfinite(issued_at) AND isfinite(expires_at)
          AND date_trunc('second', issued_at) = issued_at AND date_trunc('second', expires_at) = expires_at
          AND expires_at > issued_at AND expires_at <= issued_at + interval '15 minutes')
      );
      CREATE TABLE task_image_publication_keyset_members (
        keyset_version bigint NOT NULL REFERENCES task_image_publication_keysets(keyset_version) ON DELETE RESTRICT,
        key_id varchar(128) NOT NULL REFERENCES task_image_publication_keys(key_id) ON DELETE RESTRICT,
        PRIMARY KEY (keyset_version, key_id)
      );
      CREATE FUNCTION public.task_image_keyset_preserve_audit() RETURNS trigger
      LANGUAGE plpgsql SET search_path=pg_catalog AS $$
      BEGIN
        RAISE EXCEPTION 'publication keyset audit is immutable' USING ERRCODE='23514';
      END $$;
      REVOKE ALL ON FUNCTION public.task_image_keyset_preserve_audit() FROM PUBLIC;
      CREATE TRIGGER task_image_keysets_preserve BEFORE UPDATE OR DELETE OR TRUNCATE
        ON task_image_publication_keysets FOR EACH STATEMENT
        EXECUTE FUNCTION public.task_image_keyset_preserve_audit();
      CREATE TRIGGER task_image_keyset_members_preserve BEFORE UPDATE OR DELETE OR TRUNCATE
        ON task_image_publication_keyset_members FOR EACH STATEMENT
        EXECUTE FUNCTION public.task_image_keyset_preserve_audit();
    """)


def downgrade() -> None:
    op.execute("""
      LOCK TABLE public.task_image_publication_state, public.task_image_publication_keys,
        public.task_image_publication_keysets, public.task_image_publication_keyset_members
        IN ACCESS EXCLUSIVE MODE NOWAIT;
      DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM public.task_image_publication_keysets)
          OR EXISTS (SELECT 1 FROM public.task_image_publication_keyset_members) THEN
          RAISE EXCEPTION 'publication keyset audit cannot be discarded' USING ERRCODE='23514';
        END IF;
      END $$;
      DROP TABLE public.task_image_publication_keyset_members;
      DROP TABLE public.task_image_publication_keysets;
      DROP FUNCTION public.task_image_keyset_preserve_audit();
    """)
