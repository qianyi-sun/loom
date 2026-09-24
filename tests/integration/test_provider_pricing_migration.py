"""A legacy custom connection survives upgrade and rollback without repricing."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from uuid import uuid4

from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer


def test_legacy_pricing_roundtrip_and_new_data_downgrade_guard() -> None:
    with PostgresContainer("postgres:16") as pg:
        url = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql+psycopg://")
        env = {**os.environ, "LOOM_DB_URL": url}

        def migrate(direction: str, revision: str, *, succeeds: bool = True) -> None:
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "alembic",
                    "-c",
                    "database/migrations/alembic.ini",
                    direction,
                    revision,
                ],
                env=env,
                capture_output=True,
                text=True,
            )
            if succeeds:
                assert result.returncode == 0, result.stderr
            else:
                assert result.returncode != 0 and "Export new provider pricing" in result.stderr

        migrate("upgrade", "0158")
        engine = create_engine(url)
        team, connection = uuid4(), uuid4()
        price = {"input_usd_per_1m": 1.2, "output_usd_per_1m": 3.4}
        with engine.begin() as db:
            db.execute(
                text("INSERT INTO teams(id,name) VALUES (:id,'legacy-pricing')"), {"id": team}
            )
            db.execute(
                text("""INSERT INTO provider_connections
                (id,team_id,provider_type,display_name,base_url,upstream_host,encrypted_api_key_ref,pricing_source,pricing_data,created_by)
                VALUES (:id,:team,'custom','legacy','https://example.com','example.com','fixture:legacy','operator-supplied',cast(:price as jsonb),'fixture')"""),
                {"id": connection, "team": team, "price": json.dumps(price)},
            )
        migrate("upgrade", "0159")
        with engine.connect() as db:
            row = db.execute(
                text(
                    "SELECT pricing_config,pricing_source,pricing_data FROM provider_connections WHERE id=:id"
                ),
                {"id": connection},
            ).one()
            assert row == (None, "operator-supplied", price)
        migrate("downgrade", "0158")
        with engine.connect() as db:
            assert (
                db.scalar(
                    text("SELECT pricing_data FROM provider_connections WHERE id=:id"),
                    {"id": connection},
                )
                == price
            )
        migrate("upgrade", "0159")
        with engine.begin() as db:
            db.execute(
                text(
                    "UPDATE provider_connections SET pricing_config='{"
                    + '"pricing_mode":"usage_only"'
                    + "}'::jsonb WHERE id=:id"
                ),
                {"id": connection},
            )
        migrate("downgrade", "0158", succeeds=False)
        with engine.connect() as db:
            assert db.scalar(text("SELECT version_num FROM alembic_version")) == "0159"
        engine.dispose()
