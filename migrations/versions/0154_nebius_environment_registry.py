"""Add independent Nebius management identities; retain all legacy fleet records.

Revision ID: 0154
Revises: 0153
"""

from alembic import op

revision = "0154"
down_revision = "0153"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE nebius_environments (
            environment_id uuid PRIMARY KEY,
            incarnation uuid NOT NULL,
            owner_user_id uuid REFERENCES users(id) ON DELETE RESTRICT,
            owner_team_id uuid NOT NULL REFERENCES teams(id) ON DELETE RESTRICT,
            scope text NOT NULL,
            kind text NOT NULL,
            slug text NOT NULL,
            cluster_id text NOT NULL,
            physical_pool_id text NOT NULL,
            application_namespace text NOT NULL,
            execution_namespace text NOT NULL,
            build_namespace text NOT NULL,
            public_host text NOT NULL,
            target_id text NOT NULL,
            binding_mode text NOT NULL,
            candidate_id uuid,
            deployment_generation bigint NOT NULL,
            desired_state text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            purged_at timestamptz,
            CONSTRAINT nebius_environment_incarnation_key UNIQUE (incarnation),
            CONSTRAINT nebius_environment_target_key UNIQUE (target_id),
            CONSTRAINT nebius_environment_cluster_key UNIQUE (environment_id, cluster_id),
            CONSTRAINT nebius_environment_scope_check CHECK (
                (scope = 'personal' AND kind = 'development' AND owner_user_id IS NOT NULL
                 AND slug NOT IN ('dev', 'staging', 'prod', 'shared')) OR
                (scope = 'shared' AND ((kind = 'development' AND slug = 'dev') OR
                 (kind = 'staging' AND slug = 'staging') OR (kind = 'production' AND slug = 'prod')))
            ),
            CONSTRAINT nebius_environment_slug_check CHECK (slug ~ '^[a-z0-9]([-a-z0-9]{0,52}[a-z0-9])?$'),
            CONSTRAINT nebius_environment_generation_check CHECK (deployment_generation > 0),
            CONSTRAINT nebius_environment_state_check CHECK (desired_state IN ('active', 'suspended', 'destroyed')),
            CONSTRAINT nebius_environment_binding_check CHECK (binding_mode IN ('generated', 'imported')),
            CONSTRAINT nebius_environment_purge_check CHECK (purged_at IS NULL OR desired_state = 'destroyed'),
            CONSTRAINT nebius_environment_namespaces_check CHECK (
                application_namespace <> execution_namespace AND application_namespace <> build_namespace
                AND build_namespace = execution_namespace || '-build'
            ),
            CONSTRAINT nebius_environment_identity_check CHECK (
                environment_id <> '00000000-0000-0000-0000-000000000000'::uuid AND
                incarnation <> '00000000-0000-0000-0000-000000000000'::uuid
            )
        );
        CREATE UNIQUE INDEX nebius_environment_slug_key ON nebius_environments (slug) WHERE purged_at IS NULL;
        CREATE UNIQUE INDEX nebius_environment_host_key ON nebius_environments (public_host) WHERE purged_at IS NULL;
        CREATE INDEX nebius_environment_owner_idx ON nebius_environments (owner_user_id, desired_state);
        CREATE TABLE nebius_environment_namespaces (
            cluster_id text NOT NULL,
            namespace_name text NOT NULL,
            environment_id uuid NOT NULL,
            role text NOT NULL,
            PRIMARY KEY (cluster_id, namespace_name),
            CONSTRAINT nebius_environment_namespace_owner_fk FOREIGN KEY (environment_id, cluster_id)
                REFERENCES nebius_environments (environment_id, cluster_id) ON DELETE RESTRICT,
            CONSTRAINT nebius_environment_namespace_role_key UNIQUE (environment_id, role),
            CONSTRAINT nebius_environment_namespace_role_check CHECK (role IN ('application', 'execution', 'build')),
            CONSTRAINT nebius_environment_namespace_name_check CHECK (namespace_name ~ '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$')
        );
    """)


def downgrade() -> None:
    # A source rollback is not permission to erase a retained environment's
    # ownership. Empty/disposable migrations remain reversible for verification.
    op.execute("LOCK TABLE nebius_environment_namespaces, nebius_environments IN ACCESS EXCLUSIVE MODE NOWAIT")
    op.execute("""
        DO $$ BEGIN
            IF EXISTS (SELECT 1 FROM nebius_environments) THEN
                RAISE EXCEPTION 'cannot remove registered Nebius environment history';
            END IF;
        END $$;
        DROP TABLE nebius_environment_namespaces;
        DROP TABLE nebius_environments;
    """)
