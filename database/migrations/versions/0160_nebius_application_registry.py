"""Distinct shared-data application registration and atomic cross-model names.

Revision ID: 0160
Revises: 0159
"""
from alembic import op

revision = "0160"
down_revision = "0159"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nebius_applications (
            application_id uuid PRIMARY KEY,
            incarnation uuid NOT NULL,
            owner_user_id uuid NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
            owner_team_id uuid NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
            data_environment_id uuid NOT NULL,
            cluster_id text NOT NULL,
            slug text NOT NULL,
            application_namespace text NOT NULL,
            public_host text NOT NULL,
            release_id uuid NOT NULL,
            deployment_generation bigint NOT NULL,
            access_generation bigint NOT NULL,
            desired_state text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            purged_at timestamptz,
            CONSTRAINT nebius_application_incarnation_key UNIQUE (incarnation),
            CONSTRAINT nebius_application_generation_check CHECK (deployment_generation > 0 AND access_generation > 0),
            CONSTRAINT nebius_application_state_check CHECK (desired_state IN ('active','suspended','destroyed')),
            CONSTRAINT nebius_application_purge_check CHECK (purged_at IS NULL OR desired_state = 'destroyed'),
            CONSTRAINT nebius_application_slug_check CHECK (
                slug ~ '^[a-z0-9]([-a-z0-9]{0,52}[a-z0-9])?$' AND slug NOT IN ('dev','staging','prod','shared')),
            CONSTRAINT nebius_application_namespace_check CHECK (application_namespace = 'loom-dev-' || slug),
            CONSTRAINT nebius_application_cluster_check CHECK (cluster_id ~ '^[a-zA-Z0-9_-]{1,128}$'),
            CONSTRAINT nebius_application_host_check CHECK (
                length(public_host) <= 253 AND public_host ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?([.][a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)+$'),
            CONSTRAINT nebius_application_identity_check CHECK (
                application_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                incarnation <> '00000000-0000-0000-0000-000000000000'::uuid AND
                owner_user_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                owner_team_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                data_environment_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                release_id <> '00000000-0000-0000-0000-000000000000'::uuid)
        );
        CREATE INDEX nebius_application_owner_idx ON nebius_applications(owner_user_id, desired_state);
        CREATE TABLE nebius_deployment_name_claims (
            kind text NOT NULL,
            scope text NOT NULL,
            name text NOT NULL,
            environment_id uuid REFERENCES nebius_environments(environment_id) ON DELETE CASCADE,
            application_id uuid REFERENCES nebius_applications(application_id) ON DELETE CASCADE,
            PRIMARY KEY (kind, scope, name),
            CONSTRAINT nebius_deployment_claim_owner_check CHECK (num_nonnulls(environment_id, application_id) = 1),
            CONSTRAINT nebius_deployment_claim_kind_check CHECK (
                (kind IN ('slug','host') AND scope = '') OR (kind = 'namespace' AND scope <> '')),
            CONSTRAINT nebius_deployment_claim_name_check CHECK (name <> '')
        );
        CREATE INDEX nebius_deployment_claim_environment_idx ON nebius_deployment_name_claims(environment_id);
        CREATE INDEX nebius_deployment_claim_application_idx ON nebius_deployment_name_claims(application_id);

        -- Serialize backfill with existing registration writers. No old records
        -- are rewritten and no namespace ownership is inferred from names.
        LOCK TABLE nebius_environments, nebius_environment_namespaces IN SHARE ROW EXCLUSIVE MODE;
        INSERT INTO nebius_deployment_name_claims(kind,scope,name,environment_id)
          SELECT 'slug','',slug,environment_id FROM nebius_environments WHERE purged_at IS NULL
          UNION ALL SELECT 'host','',public_host,environment_id FROM nebius_environments WHERE purged_at IS NULL
          UNION ALL SELECT 'namespace',cluster_id,namespace_name,environment_id FROM nebius_environment_namespaces;

        CREATE FUNCTION public.loom_environment_name_claims() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
            DELETE FROM public.nebius_deployment_name_claims
              WHERE environment_id = NEW.environment_id AND kind IN ('slug','host');
            IF NEW.purged_at IS NULL THEN
                INSERT INTO public.nebius_deployment_name_claims(kind,scope,name,environment_id)
                  VALUES ('slug','',NEW.slug,NEW.environment_id), ('host','',NEW.public_host,NEW.environment_id);
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER loom_environment_name_claims AFTER INSERT OR UPDATE ON nebius_environments
          FOR EACH ROW EXECUTE FUNCTION public.loom_environment_name_claims();

        CREATE FUNCTION public.loom_environment_namespace_claim() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
            IF TG_OP <> 'INSERT' THEN
                DELETE FROM public.nebius_deployment_name_claims WHERE kind = 'namespace'
                  AND scope = OLD.cluster_id AND name = OLD.namespace_name AND environment_id = OLD.environment_id;
            END IF;
            IF TG_OP <> 'DELETE' THEN
                INSERT INTO public.nebius_deployment_name_claims(kind,scope,name,environment_id)
                  VALUES ('namespace',NEW.cluster_id,NEW.namespace_name,NEW.environment_id);
                RETURN NEW;
            END IF;
            RETURN OLD;
        END $$;
        CREATE TRIGGER loom_environment_namespace_claim AFTER INSERT OR UPDATE OR DELETE ON nebius_environment_namespaces
          FOR EACH ROW EXECUTE FUNCTION public.loom_environment_namespace_claim();

        CREATE FUNCTION public.loom_application_name_claims() RETURNS trigger
        LANGUAGE plpgsql SET search_path = pg_catalog, public AS $$
        BEGIN
            DELETE FROM public.nebius_deployment_name_claims WHERE application_id = NEW.application_id;
            IF NEW.purged_at IS NULL THEN
                INSERT INTO public.nebius_deployment_name_claims(kind,scope,name,application_id) VALUES
                  ('slug','',NEW.slug,NEW.application_id), ('host','',NEW.public_host,NEW.application_id),
                  ('namespace',NEW.cluster_id,NEW.application_namespace,NEW.application_id);
            END IF;
            RETURN NEW;
        END $$;
        CREATE TRIGGER loom_application_name_claims AFTER INSERT OR UPDATE ON nebius_applications
          FOR EACH ROW EXECUTE FUNCTION public.loom_application_name_claims();
    """)


def downgrade() -> None:
    op.execute("""
        LOCK TABLE nebius_environments, nebius_environment_namespaces, nebius_applications,
                   nebius_deployment_name_claims IN ACCESS EXCLUSIVE MODE NOWAIT;
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_applications) THEN
                RAISE EXCEPTION 'cannot remove application registration history';
            END IF;
        END $$;
        DROP TRIGGER loom_application_name_claims ON nebius_applications;
        DROP TRIGGER loom_environment_namespace_claim ON nebius_environment_namespaces;
        DROP TRIGGER loom_environment_name_claims ON nebius_environments;
        DROP FUNCTION public.loom_application_name_claims();
        DROP FUNCTION public.loom_environment_namespace_claim();
        DROP FUNCTION public.loom_environment_name_claims();
        DROP TABLE nebius_deployment_name_claims;
        DROP TABLE nebius_applications;
    """)
