"""Journal immutable source incarnations, write versions and reference admission.

Revision ID: 0137
Revises: 0136
Create Date: 2026-09-10
"""

from alembic import op

revision: str = "0137"
down_revision: str | None = "0136"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Empty authority only: no discovery or promotion of historical mutable data.
    op.execute("""
      CREATE TABLE task_bundle_sources (
        id varchar(64) PRIMARY KEY,
        source_uri text NOT NULL UNIQUE,
        spec_json jsonb NOT NULL,
        created_at timestamptz NOT NULL,
        CONSTRAINT task_bundle_sources_id_check CHECK (id ~ '^[0-9a-f]{64}$')
      );
      CREATE TABLE task_bundle_source_incarnations (
        id uuid PRIMARY KEY,
        source_id varchar(64) NOT NULL REFERENCES task_bundle_sources(id),
        state varchar NOT NULL,
        expires_at timestamptz NOT NULL,
        created_at timestamptz NOT NULL,
        updated_at timestamptz NOT NULL,
        CONSTRAINT task_bundle_incarnation_state_check CHECK (state IN ('uploading','available','deleting','retired')),
        CONSTRAINT task_bundle_incarnation_deadline_check CHECK (expires_at > created_at)
      );
      CREATE INDEX task_bundle_incarnation_source_idx ON task_bundle_source_incarnations(source_id,state);
      CREATE UNIQUE INDEX task_bundle_incarnation_available_uidx ON task_bundle_source_incarnations(source_id) WHERE state='available';
      CREATE TABLE task_bundle_source_writes (
        id uuid PRIMARY KEY,
        incarnation_id uuid NOT NULL REFERENCES task_bundle_source_incarnations(id),
        bucket text NOT NULL, object_key text NOT NULL,
        content_sha256 varchar(64) NOT NULL, size_bytes bigint NOT NULL,
        issued_at timestamptz,
        inventory_epoch bigint NOT NULL DEFAULT 0,
        inventory_active boolean NOT NULL DEFAULT false,
        inventory_cursor jsonb,
        last_observed_end_at timestamptz,
        CONSTRAINT task_bundle_write_object_uidx UNIQUE (incarnation_id,object_key),
        CONSTRAINT task_bundle_write_storage_uidx UNIQUE (id,bucket,object_key),
        CONSTRAINT task_bundle_write_content_check CHECK (content_sha256 ~ '^[0-9a-f]{64}$' AND size_bytes >= 0),
        CONSTRAINT task_bundle_write_epoch_check CHECK (inventory_epoch >= 0)
      );
      CREATE INDEX task_bundle_write_reconcile_idx ON task_bundle_source_writes(last_observed_end_at,id);
      CREATE TABLE task_bundle_source_versions (
        id uuid PRIMARY KEY,
        write_id uuid NOT NULL,
        bucket text NOT NULL, object_key text NOT NULL,
        version_id varchar(1024) NOT NULL,
        state varchar NOT NULL,
        observed_at timestamptz NOT NULL, deleted_at timestamptz,
        CONSTRAINT task_bundle_version_write_fkey FOREIGN KEY (write_id,bucket,object_key)
          REFERENCES task_bundle_source_writes(id,bucket,object_key),
        CONSTRAINT task_bundle_version_identity_uidx UNIQUE (bucket,object_key,version_id),
        CONSTRAINT task_bundle_version_immutable_check CHECK (version_id <> 'null' AND version_id <> '' AND version_id=btrim(version_id)),
        CONSTRAINT task_bundle_version_state_check CHECK (state IN ('available','deleting','deleted')),
        CONSTRAINT task_bundle_version_deleted_check CHECK ((state='deleted') = (deleted_at IS NOT NULL))
      );
      CREATE INDEX task_bundle_version_write_idx ON task_bundle_source_versions(write_id,state);
      CREATE TABLE task_bundle_source_references (
        source_id varchar(64) NOT NULL REFERENCES task_bundle_sources(id),
        kind varchar NOT NULL, owner_id text NOT NULL,
        created_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (source_id,kind,owner_id),
        CONSTRAINT task_bundle_reference_kind_check CHECK (kind IN ('catalog','materialization','trial'))
      );
      CREATE FUNCTION public.task_bundle_journal_immutable() RETURNS trigger
      LANGUAGE plpgsql SET search_path=pg_catalog AS $$
      DECLARE changed boolean;
      BEGIN
        IF TG_OP IN ('DELETE','TRUNCATE') THEN
          RAISE EXCEPTION 'task bundle journal recovery tombstones are immutable' USING ERRCODE='23514';
        END IF;
        IF TG_TABLE_NAME='task_bundle_sources' THEN
          changed := NEW IS DISTINCT FROM OLD;
        ELSIF TG_TABLE_NAME='task_bundle_source_incarnations' THEN
          changed := (NEW.id,NEW.source_id,NEW.expires_at,NEW.created_at) IS DISTINCT FROM
                     (OLD.id,OLD.source_id,OLD.expires_at,OLD.created_at)
            OR (OLD.state='available' AND NEW.state NOT IN ('available','deleting'))
            OR (OLD.state='deleting' AND NEW.state NOT IN ('deleting','retired'))
            OR (OLD.state='retired' AND NEW.state <> 'retired');
        ELSIF TG_TABLE_NAME='task_bundle_source_writes' THEN
          changed := (NEW.id,NEW.incarnation_id,NEW.bucket,NEW.object_key,NEW.content_sha256,NEW.size_bytes)
            IS DISTINCT FROM (OLD.id,OLD.incarnation_id,OLD.bucket,OLD.object_key,OLD.content_sha256,OLD.size_bytes)
            OR (OLD.issued_at IS NOT NULL AND NEW.issued_at IS DISTINCT FROM OLD.issued_at)
            OR NEW.inventory_epoch < OLD.inventory_epoch;
        ELSE
          changed := (NEW.id,NEW.write_id,NEW.bucket,NEW.object_key,NEW.version_id,NEW.observed_at)
            IS DISTINCT FROM (OLD.id,OLD.write_id,OLD.bucket,OLD.object_key,OLD.version_id,OLD.observed_at)
            OR (OLD.state='deleting' AND NEW.state NOT IN ('deleting','deleted'))
            OR (OLD.state='deleted' AND NEW IS DISTINCT FROM OLD);
        END IF;
        IF changed THEN
          RAISE EXCEPTION 'task bundle journal identity or retirement is immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
      END $$;
      REVOKE ALL ON FUNCTION public.task_bundle_journal_immutable() FROM PUBLIC;
      CREATE TRIGGER task_bundle_source_immutable AFTER UPDATE ON task_bundle_sources
        FOR EACH ROW EXECUTE FUNCTION public.task_bundle_journal_immutable();
      CREATE TRIGGER task_bundle_incarnation_immutable AFTER UPDATE ON task_bundle_source_incarnations
        FOR EACH ROW EXECUTE FUNCTION public.task_bundle_journal_immutable();
      CREATE TRIGGER task_bundle_write_immutable AFTER UPDATE ON task_bundle_source_writes
        FOR EACH ROW EXECUTE FUNCTION public.task_bundle_journal_immutable();
      CREATE TRIGGER task_bundle_version_immutable AFTER UPDATE ON task_bundle_source_versions
        FOR EACH ROW EXECUTE FUNCTION public.task_bundle_journal_immutable();
      CREATE TRIGGER task_bundle_source_no_delete BEFORE DELETE OR TRUNCATE ON task_bundle_sources
        FOR EACH STATEMENT EXECUTE FUNCTION public.task_bundle_journal_immutable();
      CREATE TRIGGER task_bundle_incarnation_no_delete BEFORE DELETE OR TRUNCATE ON task_bundle_source_incarnations
        FOR EACH STATEMENT EXECUTE FUNCTION public.task_bundle_journal_immutable();
      CREATE TRIGGER task_bundle_write_no_delete BEFORE DELETE OR TRUNCATE ON task_bundle_source_writes
        FOR EACH STATEMENT EXECUTE FUNCTION public.task_bundle_journal_immutable();
      CREATE TRIGGER task_bundle_version_no_delete BEFORE DELETE OR TRUNCATE ON task_bundle_source_versions
        FOR EACH STATEMENT EXECUTE FUNCTION public.task_bundle_journal_immutable();
    """)


def downgrade() -> None:
    op.execute("""
      LOCK TABLE task_bundle_sources,task_bundle_source_incarnations,task_bundle_source_writes,
        task_bundle_source_versions,task_bundle_source_references IN ACCESS EXCLUSIVE MODE NOWAIT;
      DO $$ BEGIN
        IF EXISTS (SELECT 1 FROM task_bundle_sources) THEN
          RAISE EXCEPTION 'task bundle journal contains authority; cannot discard recovery tombstones' USING ERRCODE='23514';
        END IF;
      END $$;
      DROP TABLE task_bundle_source_references,task_bundle_source_versions,task_bundle_source_writes,
        task_bundle_source_incarnations,task_bundle_sources;
      DROP FUNCTION public.task_bundle_journal_immutable();
    """)
