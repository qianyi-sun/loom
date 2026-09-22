"""Retired provider key grace and row-level reference attachment guards.

Revision ID: 0156
Revises: 0155
"""

from alembic import op

revision = "0156"
down_revision = "0155"
branch_labels = None
depends_on = None

# Retain all historical generic consumers. Only newly attached canonical local
# provider refs gain integrity checks; other schemes/namespaces are unchanged.
_CONSUMERS = {
    "provider_connections": ("encrypted_api_key_ref",),
    "dev_instances": ("secret_ref",),
    "task_image_build_projections": ("bootstrap_secret_ref", "session_secret_ref"),
    "task_image_build_session_generations": ("session_secret_ref",),
    "pipeline_stage_runs": ("secret_refs",),
}


def upgrade() -> None:
    op.execute("""
        ALTER TABLE secrets ADD COLUMN provider_retired_at timestamptz;
        CREATE INDEX secrets_provider_retired_idx ON secrets (provider_retired_at, ref)
            WHERE provider_retired_at IS NOT NULL;
        CREATE FUNCTION guard_provider_secret_attachment() RETURNS trigger
        LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
        DECLARE
            field_name text;
            new_values jsonb;
            old_values jsonb;
            candidate text;
        BEGIN
            FOREACH field_name IN ARRAY TG_ARGV LOOP
                new_values := to_jsonb(NEW) -> field_name;
                old_values := CASE WHEN TG_OP = 'UPDATE'
                    THEN to_jsonb(OLD) -> field_name ELSE 'null'::jsonb END;
                -- A restored provider needs its credential even when the ref
                -- itself did not change. Historical updates remain valid.
                IF TG_TABLE_NAME = 'provider_connections' AND TG_OP = 'UPDATE'
                   AND to_jsonb(OLD) ->> 'deleted_at' IS NOT NULL
                   AND to_jsonb(NEW) ->> 'deleted_at' IS NULL THEN
                    old_values := 'null'::jsonb;
                END IF;
                IF new_values IS NOT DISTINCT FROM old_values THEN CONTINUE; END IF;
                IF jsonb_typeof(new_values) <> 'array' THEN
                    new_values := jsonb_build_array(new_values);
                END IF;
                IF jsonb_typeof(old_values) <> 'array' THEN
                    old_values := jsonb_build_array(old_values);
                END IF;
                FOR candidate IN SELECT jsonb_array_elements_text(new_values) LOOP
                    IF candidate ~ '^loom://team:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
                       AND NOT old_values @> jsonb_build_array(candidate) THEN
                        PERFORM 1 FROM public.secrets WHERE ref = candidate FOR KEY SHARE;
                        IF NOT FOUND THEN
                            RAISE EXCEPTION 'local provider secret does not exist'
                                USING ERRCODE = '23503';
                        END IF;
                    END IF;
                END LOOP;
            END LOOP;
            RETURN NEW;
        END $$;
        REVOKE ALL ON FUNCTION guard_provider_secret_attachment() FROM PUBLIC;
    """)
    for table, columns in _CONSUMERS.items():
        arguments = ", ".join(f"'{column}'" for column in columns)
        watched = (*columns, "deleted_at") if table == "provider_connections" else columns
        op.execute(f"""
            CREATE TRIGGER {table}_provider_secret_attachment
            BEFORE INSERT OR UPDATE OF {', '.join(watched)} ON {table}
            FOR EACH ROW EXECUTE FUNCTION guard_provider_secret_attachment({arguments})
        """)


def downgrade() -> None:
    for table in _CONSUMERS:
        op.execute(f"DROP TRIGGER {table}_provider_secret_attachment ON {table}")
    op.execute("""
        DROP FUNCTION guard_provider_secret_attachment();
        DROP INDEX secrets_provider_retired_idx;
        ALTER TABLE secrets DROP COLUMN provider_retired_at;
    """)
