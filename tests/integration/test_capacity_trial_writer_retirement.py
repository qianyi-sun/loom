"""Exercise trial-trigger retirement with the actual protected migrator role."""

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config as AlembicConfig
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_trial_writer_fence import _control_session, _initialize


def _downgrade(database: dict[str, object], monkeypatch: pytest.MonkeyPatch) -> None:
    root = Path(__file__).resolve().parents[2]
    config = AlembicConfig(str(root / "capacity_guard_migrations" / "alembic.ini"))
    config.set_main_option("script_location", str(root / "capacity_guard_migrations"))
    with monkeypatch.context() as environment:
        for suffix, key in (
            ("DB_URL", "migrator_url"),
            ("OWNER_ROLE", "owner_role"),
            ("AGENT_ROLE", "agent_role"),
            ("EXECUTOR_ROLE", "executor_role"),
            ("OBSERVER_ROLE", "observer_role"),
            ("RUNTIME_ROLE", "runtime_role"),
        ):
            environment.setenv(f"LOOM_CAPACITY_GUARD_{suffix}", _value(database, key))
        command.downgrade(config, "guard_0030")


@pytest.mark.asyncio
async def test_uninitialized_trial_fence_downgrades_with_actual_migration_authority(
    capacity_guard_database: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    database = capacity_guard_database
    _downgrade(database, monkeypatch)
    async with _control_session(database) as owner:
        assert (
            await owner.execute(text("SELECT to_regclass('loom_capacity_guard.trial_writer_fence')"))
        ).scalar_one() is None
        assert (
            await owner.execute(
                text("SELECT version_num FROM loom_capacity_guard.capacity_guard_alembic_version")
            )
        ).scalar_one() == "guard_0030"


@pytest.mark.asyncio
async def test_initialized_trial_fence_downgrade_refuses_for_state_not_missing_privilege(
    capacity_guard_database: dict[str, object], monkeypatch: pytest.MonkeyPatch
) -> None:
    database = capacity_guard_database
    initial = await _initialize(database)
    with pytest.raises(DBAPIError) as refusal:
        _downgrade(database, monkeypatch)
    assert refusal.value.orig.sqlstate == "55000"
    assert "requires protected retirement" in str(refusal.value.orig)
    async with _control_session(database) as owner:
        assert str(
            (
                await owner.execute(
                    text("SELECT writer_incarnation FROM loom_capacity_guard.trial_writer_fence")
                )
            ).scalar_one()
        ) == initial["writer_incarnation"]
        assert (
            await owner.execute(
                text(
                    "SELECT count(*) FROM pg_trigger WHERE tgrelid = 'public.trials'::regclass "
                    "AND tgname IN ('capacity_guard_lock_trial_writer', "
                    "'zz_capacity_guard_account_trial_writer') AND tgenabled = 'O'"
                )
            )
        ).scalar_one() == 2
