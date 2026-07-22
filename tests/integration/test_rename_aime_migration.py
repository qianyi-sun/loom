"""Migration 0015: rename `aime` → `aime-aimo-validation`.

Three cases that need to upgrade cleanly:
- no old row → no-op
- only old row exists → promote PK + drop tasks
- both rows exist → drop the old duplicate
Plus one FK-violation case that must fail loudly (trial references
old aime task) — silently dropping trial history would be worse than
the migration failing.
"""

from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import IntegrityError


def _cfg(db_url: str) -> Config:
    cfg = Config("migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", db_url)
    return cfg


@pytest.fixture
def at_0014(isolated_migration_postgres_url: str) -> Engine:
    cfg = _cfg(isolated_migration_postgres_url)
    command.downgrade(cfg, "0014")
    engine = create_engine(isolated_migration_postgres_url, future=True)
    # Wipe any leftover AIME rows from a previous test run so each test
    # starts from the same baseline.
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM tasks WHERE benchmark_id IN "
                "('aime', 'aime-aimo-validation', 'aime-2025', 'aime-25')",
            )
        )
        conn.execute(
            text(
                "DELETE FROM benchmarks WHERE id IN "
                "('aime', 'aime-aimo-validation', 'aime-2025', 'aime-25')",
            )
        )
    return engine


def _insert_old_aime_benchmark(conn) -> None:
    conn.execute(
        text(
            "INSERT INTO benchmarks (id, display_name, upstream_kind, "
            "upstream_locator, upstream_revision, license_spdx, license_url, "
            "splits) VALUES ('aime', 'AIME', 'huggingface', "
            "'AI-MO/aimo-validation-aime', 'main', 'proprietary-MAA', '', "
            "ARRAY['train'])"
        )
    )


def _insert_old_aime_task(conn, task_id: str = "aime/47") -> None:
    conn.execute(
        text(
            "INSERT INTO tasks (id, checksum, config, source, license, "
            "benchmark_id) VALUES (:id, '0', '{}', 'hf://x/', "
            "'proprietary-MAA', 'aime')"
        ),
        {"id": task_id},
    )


def test_0015_no_old_row_is_noop(
    at_0014: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    command.upgrade(_cfg(isolated_migration_postgres_url), "0015")
    with at_0014.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT id FROM benchmarks WHERE id IN ('aime', 'aime-aimo-validation')",
            )
        ).fetchall()
    assert rows == []


def test_0015_promotes_old_row_when_alone(
    at_0014: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0014.begin() as conn:
        _insert_old_aime_benchmark(conn)
        _insert_old_aime_task(conn)
    command.upgrade(_cfg(isolated_migration_postgres_url), "0015")
    with at_0014.begin() as conn:
        new = conn.execute(
            text(
                "SELECT id, display_name, series FROM benchmarks WHERE id = 'aime-aimo-validation'",
            )
        ).first()
        old = conn.execute(
            text(
                "SELECT 1 FROM benchmarks WHERE id = 'aime'",
            )
        ).first()
        task_count = conn.execute(
            text(
                "SELECT COUNT(*) FROM tasks WHERE id LIKE 'aime/%'",
            )
        ).scalar_one()
    assert new is not None
    assert new[1] == "AIME (AIMO validation 2022–2024)"
    assert new[2] == "aime"
    assert old is None
    assert task_count == 0  # old-format tasks dropped


def test_0015_drops_duplicate_old_row_when_both_exist(
    at_0014: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0014.begin() as conn:
        _insert_old_aime_benchmark(conn)
        _insert_old_aime_task(conn)
        # Pre-existing new row (operator already ran post-PR-1 register)
        conn.execute(
            text(
                "INSERT INTO benchmarks (id, display_name, upstream_kind, "
                "upstream_locator, upstream_revision, license_spdx, "
                "license_url, splits, series) VALUES "
                "('aime-aimo-validation', 'AIME (AIMO validation 2022–2024)', "
                "'huggingface', 'AI-MO/aimo-validation-aime', 'main', "
                "'proprietary-MAA', '', ARRAY['train'], 'aime')"
            )
        )
    command.upgrade(_cfg(isolated_migration_postgres_url), "0015")
    with at_0014.begin() as conn:
        ids = {
            row[0]
            for row in conn.execute(
                text(
                    "SELECT id FROM benchmarks WHERE id LIKE 'aime%'",
                )
            ).fetchall()
        }
    assert ids == {"aime-aimo-validation"}


def test_0015_fails_loudly_when_trial_references_old_task(
    at_0014: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    """A trial pointing at an old `aime/47` row blocks the task DELETE
    via the trials.task_id FK. The migration must fail rather than
    silently nuking trial history."""
    team_id = uuid.uuid4()
    with at_0014.begin() as conn:
        _insert_old_aime_benchmark(conn)
        _insert_old_aime_task(conn)
        conn.execute(
            text(
                "INSERT INTO teams (id, name) VALUES (:id, :name)",
            ),
            {"id": team_id, "name": f"t-{team_id}"},
        )
        conn.execute(
            text(
                "INSERT INTO tokens (token_hash, type, scopes, team_id, "
                "issued_at) VALUES (:h, 'team', ARRAY['read:own'], :t, :now)",
            ),
            {
                "h": hashlib.sha256(f"loom_team_{uuid.uuid4().hex}".encode()).digest(),
                "t": team_id,
                "now": datetime.now(UTC),
            },
        )
        conn.execute(
            text(
                "INSERT INTO trials (id, team_id, task_id, config, "
                "requires_caps, state) VALUES "
                "(:id, :team, 'aime/47', '{}', '{}', 'queued')",
            ),
            {"id": uuid.uuid4(), "team": team_id},
        )
    with pytest.raises(IntegrityError):
        command.upgrade(_cfg(isolated_migration_postgres_url), "0015")
    # Re-prep for downstream tests: the failed upgrade left us at 0014;
    # clean the trial + tasks + benchmark + team so the next test can
    # run a fresh upgrade if it wants.
    with at_0014.begin() as conn:
        conn.execute(text("DELETE FROM trials WHERE team_id = :t"), {"t": team_id})
        conn.execute(text("DELETE FROM tasks WHERE id = 'aime/47'"))
        conn.execute(text("DELETE FROM benchmarks WHERE id = 'aime'"))
        conn.execute(text("DELETE FROM tokens WHERE team_id = :t"), {"t": team_id})
        conn.execute(text("DELETE FROM teams WHERE id = :t"), {"t": team_id})
    # Roll forward so subsequent tests find the schema at head.
    command.upgrade(_cfg(isolated_migration_postgres_url), "head")


def test_0015_downgrade_restores_old_row(
    at_0014: Engine,
    isolated_migration_postgres_url: str,
) -> None:
    with at_0014.begin() as conn:
        _insert_old_aime_benchmark(conn)
    command.upgrade(_cfg(isolated_migration_postgres_url), "0015")
    command.downgrade(_cfg(isolated_migration_postgres_url), "0014")
    with at_0014.begin() as conn:
        row = conn.execute(
            text(
                "SELECT id, display_name FROM benchmarks "
                "WHERE id IN ('aime', 'aime-aimo-validation')",
            )
        ).first()
    assert row is not None
    assert row[0] == "aime"
    assert row[1] == "AIME"
    # Restore head for downstream tests.
    command.upgrade(_cfg(isolated_migration_postgres_url), "head")
