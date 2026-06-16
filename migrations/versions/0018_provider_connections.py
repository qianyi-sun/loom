"""provider_connections + provider_models_cache + secrets

Revision ID: 0018
Revises: 0017
Create Date: 2026-06-15

Three new tables for user-supplied LLM provider endpoints
(cluster-deploy spec §Schema additions):

- `secrets` backs the `local-encrypted` SecretStore impl. Stores
  AES-GCM ciphertext + nonce keyed by an opaque ref string
  ("loom://<namespace>/<uuid>"). `master_key_version` lets the
  rotation walker re-encrypt all rows when LOOM_SECRET_STORE_MASTER_KEY
  changes.

- `provider_connections` is the team-scoped record for one upstream
  provider endpoint (OpenAI, Anthropic, Google, custom). Soft-deleted
  via `deleted_at` so in-flight trials' FKs stay valid for billing /
  audit even after a user clicks delete. `resolved_egress_ips` is
  populated by a background re-resolver and gates what the egress
  proxy permits per call.

- `provider_models_cache` is the per-connection list of models the
  upstream exposes. Refreshed on a 1-hour TTL on read; rows aren't
  hard-deleted when a model goes missing upstream — `upstream_present`
  flips to false instead (audit trail).

The Trial FK extension (Trial.provider_connection_id) is a separate
column add not done in this migration; lands with the Phase 2 routes
PR that uses it. Doing it here would couple two concerns.

Postgres prerequisite: 13+ for built-in `gen_random_uuid()` in
pg_catalog. All current managed offerings (RDS, Cloud SQL, AlloyDB)
and self-hosted Postgres support this. The CI test container is
`postgres:16`. Older Postgres would need the `pgcrypto` extension
enabled.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0018"
down_revision = "0017"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- secrets -------------------------------------------------------
    op.create_table(
        "secrets",
        sa.Column("ref", sa.Text(), primary_key=True),
        sa.Column("ciphertext", sa.LargeBinary(), nullable=False),
        sa.Column("nonce", sa.LargeBinary(), nullable=False),
        sa.Column("master_key_version", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # --- provider_connections ------------------------------------------
    op.create_table(
        "provider_connections",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            # Python-side default lives on the ORM model (default=uuid4),
            # matching the convention used by Trial / LlmCall / etc.
            # Raw-SQL callers (tests, future admin scripts) must supply
            # the UUID explicitly; gen_random_uuid() server-default was
            # removed in self-review for consistency with existing
            # tables.
        ),
        sa.Column(
            "team_id",
            postgresql.UUID(as_uuid=True),
            # RESTRICT (not CASCADE) so team-delete can't silently
            # nuke provider connections that have in-flight or
            # historical Trial FKs pointing at them. Operators must
            # soft-delete connections (and let the future
            # `loom admin providers purge` reclaim them after a
            # quiet period) before deleting a team. The spec
            # originally said CASCADE; the Trial FK conflict
            # (no-cascade per spec) makes RESTRICT the right
            # default.
            sa.ForeignKey("teams.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("provider_type", sa.Text(), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("base_url", sa.Text(), nullable=False),
        # upstream_host: parsed from base_url at create/PATCH; the egress
        # proxy validates SNI against this string. Stored explicitly so
        # operators can grep distinct upstream hosts in use without
        # re-parsing every base_url.
        sa.Column("upstream_host", sa.Text(), nullable=False),
        sa.Column(
            "resolved_egress_ips",
            postgresql.ARRAY(postgresql.INET),
            nullable=False,
            server_default=sa.text("ARRAY[]::inet[]"),
        ),
        sa.Column(
            "egress_ips_refreshed_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "egress_ips_min_ttl_seconds",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("300"),
        ),
        sa.Column("encrypted_api_key_ref", sa.Text(), nullable=False),
        sa.Column(
            "allowed_models",
            postgresql.ARRAY(sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "status",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'pending'"),
        ),
        sa.Column(
            "last_validated_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column("last_validation_error", sa.Text(), nullable=True),
        sa.Column(
            "pricing_source",
            sa.Text(),
            nullable=False,
            server_default=sa.text("'tokens-only'"),
        ),
        sa.Column(
            "pricing_data",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("created_by", sa.Text(), nullable=False),
        sa.Column(
            "deleted_at",
            postgresql.TIMESTAMP(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "created_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )

    # CHECK: status enum.
    op.create_check_constraint(
        "ck_provider_connections_status",
        "provider_connections",
        "status IN ('pending', 'valid', 'invalid', 'disabled')",
    )

    # CHECK: provider_type enum.
    op.create_check_constraint(
        "ck_provider_connections_provider_type",
        "provider_connections",
        "provider_type IN ('openai-compatible', 'anthropic', 'google', 'custom')",
    )

    # CHECK: pricing_source enum.
    op.create_check_constraint(
        "ck_provider_connections_pricing_source",
        "provider_connections",
        "pricing_source IN ('rate-card', 'tokens-only', 'operator-supplied')",
    )

    # Soft-delete-aware uniqueness: a team can't have two ACTIVE
    # connections with the same display_name; once one is soft-deleted
    # the name is freed for reuse.
    op.create_index(
        "uq_provider_connections_team_name_active",
        "provider_connections",
        ["team_id", "display_name"],
        unique=True,
        postgresql_where=sa.text("deleted_at IS NULL"),
    )

    # Index for the indexed-updated_at cache invalidation pattern
    # (cluster-deploy.md §Cache + config update durability). Every
    # gateway call SELECTs id, updated_at by PK — already covered by
    # the PK index. No extra index needed here.

    # Index for the soft-deleted filter on list routes.
    op.create_index(
        "ix_provider_connections_team_not_deleted",
        "provider_connections",
        ["team_id"],
        postgresql_where=sa.text("deleted_at IS NULL"),
    )

    # --- provider_models_cache -----------------------------------------
    op.create_table(
        "provider_models_cache",
        sa.Column(
            "provider_connection_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey(
                "provider_connections.id", ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        sa.Column("model_id", sa.Text(), primary_key=True),
        sa.Column("family", sa.Text(), nullable=True),
        sa.Column("context_length", sa.Integer(), nullable=True),
        sa.Column(
            "capabilities",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "visible",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
        sa.Column("hidden_reason", sa.Text(), nullable=True),
        # last_seen_at: defaults to now() on INSERT. Not auto-updated
        # on UPDATE (unlike provider_connections.updated_at which has
        # a trigger). The refresh code sets this explicitly when an
        # upstream observation actually happened — not every UPDATE
        # represents a fresh observation (operator hide/unhide does
        # not touch this).
        sa.Column(
            "last_seen_at",
            postgresql.TIMESTAMP(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "upstream_present",
            sa.Boolean(),
            nullable=False,
            server_default=sa.text("true"),
        ),
    )

    op.create_check_constraint(
        "ck_provider_models_cache_hidden_reason",
        "provider_models_cache",
        "hidden_reason IS NULL OR hidden_reason IN ("
        "'operator-hidden', 'missing-upstream')",
    )

    # Trigger: keep provider_connections.updated_at fresh on every
    # UPDATE so the gateway's indexed-updated_at cache invalidation
    # pattern (cluster-deploy.md) catches every row mutation
    # automatically, including rewrap walker swaps that don't touch
    # any application-visible column.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION touch_provider_connections_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = now();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER provider_connections_updated_at
        BEFORE UPDATE ON provider_connections
        FOR EACH ROW EXECUTE FUNCTION touch_provider_connections_updated_at();
        """,
    )


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS provider_connections_updated_at "
        "ON provider_connections;",
    )
    op.execute(
        "DROP FUNCTION IF EXISTS touch_provider_connections_updated_at();",
    )
    op.drop_table("provider_models_cache")
    op.drop_index(
        "ix_provider_connections_team_not_deleted",
        table_name="provider_connections",
    )
    op.drop_index(
        "uq_provider_connections_team_name_active",
        table_name="provider_connections",
    )
    op.drop_table("provider_connections")
    op.drop_table("secrets")
