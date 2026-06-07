"""Migration 0007 — campaigns table + trials.campaign_id + idempotency_key
(Plan 19 Task 1)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect


def _cfg(url: str) -> Config:
    cfg = Config("migrations/alembic.ini")
    cfg.set_main_option("sqlalchemy.url", url)
    return cfg


@pytest.fixture
def at_revision_0006(postgres_url: str) -> Iterator[None]:
    """Roll back to pre-Plan-19, yield, then bring back to head."""
    cfg = _cfg(postgres_url)
    command.downgrade(cfg, "0006")
    try:
        yield
    finally:
        command.upgrade(cfg, "head")


def test_upgrade_creates_table(
    postgres_url: str, at_revision_0006: None,
) -> None:
    cfg = _cfg(postgres_url)
    command.upgrade(cfg, "0007")
    engine = create_engine(postgres_url)
    insp = inspect(engine)
    assert "campaigns" in insp.get_table_names()
    trial_cols = {c["name"] for c in insp.get_columns("trials")}
    assert "campaign_id" in trial_cols
    assert "idempotency_key" in trial_cols
    indexes = {i["name"] for i in insp.get_indexes("trials")}
    assert "trials_campaign_idx" in indexes
    assert "trials_idempotency_key_uidx" in indexes
    # Partial unique on idempotency_key
    uniq = [
        i for i in insp.get_indexes("trials")
        if i["name"] == "trials_idempotency_key_uidx"
    ]
    assert uniq and uniq[0]["unique"] is True
    engine.dispose()


def test_downgrade_drops_table(
    postgres_url: str, at_revision_0006: None,
) -> None:
    cfg = _cfg(postgres_url)
    command.upgrade(cfg, "0007")
    command.downgrade(cfg, "0006")
    engine = create_engine(postgres_url)
    insp = inspect(engine)
    assert "campaigns" not in insp.get_table_names()
    trial_cols = {c["name"] for c in insp.get_columns("trials")}
    assert "campaign_id" not in trial_cols
    assert "idempotency_key" not in trial_cols
    engine.dispose()
