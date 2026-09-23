"""Reserve manifest-qualified identities without upgrading legacy authority.

Revision ID: 0136
Revises: 0135
Create Date: 2026-09-09
"""

from alembic import op

revision: str = "0136"
down_revision: str | None = "0135"
branch_labels: str | None = None
depends_on: str | None = None


def upgrade() -> None:
    # Lock before checking provenance or replacing uniqueness. Do not wait behind
    # active writers while retaining a lock that their transaction needs.
    op.execute("LOCK TABLE public.task_image_materializations IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.task_image_materializations
            WHERE task_source_provenance ? 'bundle_content_manifest_sha256') THEN
            RAISE EXCEPTION 'unexpected content-manifest provenance; cannot infer authority'
              USING ERRCODE = '23514';
          END IF;
        END $$;
        ALTER TABLE public.task_image_materializations
          ADD COLUMN bundle_content_manifest_sha256 VARCHAR(64) NOT NULL DEFAULT '',
          DROP CONSTRAINT task_image_materializations_task_arch_uidx,
          ADD CONSTRAINT task_image_materializations_task_arch_uidx UNIQUE
            (task_id, task_checksum, cpu_arch, bundle_content_manifest_sha256),
          ADD CONSTRAINT task_image_materializations_manifest_binding_check CHECK (
            (bundle_content_manifest_sha256 = '' AND NOT
              (task_source_provenance ? 'bundle_content_manifest_sha256')) OR
            (bundle_content_manifest_sha256 ~ '^[0-9a-f]{64}$' AND COALESCE(
              jsonb_typeof(task_source_provenance) = 'object' AND
              jsonb_typeof(task_source_provenance -> 'bundle_content_manifest_sha256') = 'string' AND
              task_source_provenance ->> 'bundle_content_manifest_sha256' =
                bundle_content_manifest_sha256, false))),
          ADD CONSTRAINT task_image_materializations_manifest_key_check CHECK (
            bundle_content_manifest_sha256 = '' OR materialization_key = encode(sha256(
              convert_to('task-image-materialization-v2', 'UTF8') || decode('00', 'hex') ||
              convert_to(task_id, 'UTF8') || decode('00', 'hex') ||
              convert_to(task_checksum, 'UTF8') || decode('00', 'hex') ||
              convert_to(cpu_arch, 'UTF8') || decode('00', 'hex') ||
              convert_to(bundle_content_manifest_sha256, 'UTF8')), 'hex'));
        CREATE FUNCTION public.task_image_preserve_manifest_identity() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog AS $$
        BEGIN
          IF NEW.bundle_content_manifest_sha256 IS DISTINCT FROM OLD.bundle_content_manifest_sha256
            OR (OLD.bundle_content_manifest_sha256 <> '' AND
              (NEW.id, NEW.materialization_key, NEW.task_id, NEW.task_checksum, NEW.cpu_arch,
               NEW.task_config, NEW.task_source, NEW.task_source_provenance) IS DISTINCT FROM
              (OLD.id, OLD.materialization_key, OLD.task_id, OLD.task_checksum, OLD.cpu_arch,
               OLD.task_config, OLD.task_source, OLD.task_source_provenance)) THEN
            RAISE EXCEPTION 'task-image manifest identity is immutable; create a new materialization'
              USING ERRCODE = '23514', CONSTRAINT = 'task_image_materializations_manifest_immutable';
          END IF;
          RETURN NEW;
        END $$;
        REVOKE ALL ON FUNCTION public.task_image_preserve_manifest_identity() FROM PUBLIC;
        CREATE TRIGGER task_image_materializations_manifest_immutable
          AFTER UPDATE ON public.task_image_materializations FOR EACH ROW
          EXECUTE FUNCTION public.task_image_preserve_manifest_identity();
    """)


def downgrade() -> None:
    op.execute("LOCK TABLE public.task_image_materializations IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM public.task_image_materializations
            WHERE bundle_content_manifest_sha256 <> '') THEN
            RAISE EXCEPTION 'manifest-qualified materializations prevent downgrade; preserve their authority'
              USING ERRCODE = '23514';
          END IF;
        END $$;
        DROP TRIGGER task_image_materializations_manifest_immutable ON public.task_image_materializations;
        DROP FUNCTION public.task_image_preserve_manifest_identity();
        ALTER TABLE public.task_image_materializations
          DROP CONSTRAINT task_image_materializations_manifest_key_check,
          DROP CONSTRAINT task_image_materializations_manifest_binding_check,
          DROP CONSTRAINT task_image_materializations_task_arch_uidx,
          DROP COLUMN bundle_content_manifest_sha256,
          ADD CONSTRAINT task_image_materializations_task_arch_uidx UNIQUE (task_id, task_checksum, cpu_arch);
    """)
