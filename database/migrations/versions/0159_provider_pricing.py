"""Provider pricing configuration and isolated supplier/team catalogs.

Revision ID: 0159
Revises: 0158
"""

from alembic import op

revision = "0159"
down_revision = "0158"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE provider_connections ADD COLUMN pricing_config jsonb;
        CREATE TABLE price_catalogs (
            id text PRIMARY KEY,
            team_id uuid REFERENCES teams(id) ON DELETE CASCADE,
            name text NOT NULL,
            supplier_id text,
            source_url text,
            prices jsonb NOT NULL DEFAULT '{}'::jsonb,
            aliases jsonb NOT NULL DEFAULT '{}'::jsonb,
            revision integer NOT NULL DEFAULT 0,
            source_metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
            updated_at timestamptz,
            checked_at timestamptz,
            sync_error text,
            CONSTRAINT price_catalog_supplier_public CHECK (supplier_id IS NULL OR team_id IS NULL)
        );
        CREATE INDEX price_catalogs_team_idx ON price_catalogs(team_id);
        INSERT INTO price_catalogs(id, name, supplier_id, source_url)
        VALUES ('supplier:yibuapi', 'YibuAPI (default group)', 'yibuapi',
                'https://yibuapi.com/api/pricing'),
               ('supplier:az-gptplus5', 'AZ GPTPlus5 (default group)', 'az-gptplus5',
                'https://az.gptplus5.com/api/pricing');
    """)


def downgrade() -> None:
    # A model-specific table cannot be represented by the old uniform pair.
    # Fail before any mutation instead of silently dropping configured prices.
    op.execute("""
        DO $$ BEGIN
          IF EXISTS (SELECT 1 FROM provider_connections WHERE pricing_config IS NOT NULL)
             OR EXISTS (SELECT 1 FROM price_catalogs WHERE revision > 0 OR team_id IS NOT NULL)
          THEN RAISE EXCEPTION 'Export new provider pricing and catalogs and explicitly revert connections before downgrading 0159';
          END IF;
        END $$;
        DROP TABLE price_catalogs;
        ALTER TABLE provider_connections DROP COLUMN pricing_config;
    """)
