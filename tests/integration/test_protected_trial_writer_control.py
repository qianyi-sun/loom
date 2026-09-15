"""Installed trial-ledger adapter against real committed PostgreSQL mutations."""

from contextlib import contextmanager
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import text
from sqlalchemy.engine import make_url

from loom_cli.rollout.operator.protected_trial_writer_control import (
    ProtectedTrialWriterControl,
    TrialWriterControlBinding,
)
from tests.integration.test_capacity_agent_store import (
    _initialize_and_register,
    _seed_trial,
    _value,
)
from tests.integration.test_capacity_trial_writer_fence import _legacy_engine


def _control(database, registration, *, writer=None, operation=None):
    @contextmanager
    def open_database():
        url = make_url(_value(database, "migrator_url"))
        with psycopg.connect(
            host=url.host, port=url.port, dbname=url.database, user=url.username,
            password=url.password, autocommit=True,
        ) as connection:
            connection.execute(sql.SQL("SET ROLE {}").format(sql.Identifier(_value(database, "owner_role"))))
            yield connection

    return ProtectedTrialWriterControl(
        open_database=open_database,
        binding=TrialWriterControlBinding(
            registration=registration,
            writer_incarnation=writer or uuid4(),
            freeze_operation_id=operation or uuid4(),
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("committed_writes", [0, 1])
async def test_installed_adapter_preserves_committed_count_and_exact_replay(
    capacity_guard_database, committed_writes,
):
    database = capacity_guard_database
    trial = _seed_trial(database)
    _, registration = await _initialize_and_register(database)
    control = _control(database, registration)
    initial = control.initialize()
    assert initial.high_water == 0
    assert not initial.frozen
    assert control.capture() == initial
    engine = _legacy_engine(database)
    try:
        if committed_writes:
            with engine.begin() as writer:
                writer.execute(text("UPDATE public.trials SET submit_priority = 101 WHERE id = :id"), {"id": trial})
        with engine.connect() as writer:
            writer.execute(text("UPDATE public.trials SET submit_priority = 102 WHERE id = :id"), {"id": trial})
            writer.rollback()
        assert control.capture().high_water == committed_writes
        frozen = control.freeze()
        assert frozen.high_water == committed_writes
        assert frozen.frozen
        assert frozen.freeze_operation_id == control.binding.freeze_operation_id
        assert control.capture() == frozen
        assert control.freeze() == frozen
        assert control.initialize() == frozen
        assert frozen.evidence_sha256 != initial.evidence_sha256
        with pytest.raises(Exception, match="frozen"):
            with engine.begin() as writer:
                writer.execute(text("UPDATE public.trials SET submit_priority = 103 WHERE id = :id"), {"id": trial})
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_installed_adapter_rolls_back_busy_control_before_retry(capacity_guard_database):
    database = capacity_guard_database
    trial = _seed_trial(database)
    _, registration = await _initialize_and_register(database)
    control = _control(database, registration)
    control.initialize()
    engine = _legacy_engine(database)
    try:
        with engine.connect() as writer:
            writer.execute(text("UPDATE public.trials SET submit_priority = 101 WHERE id = :id"), {"id": trial})
            with pytest.raises(psycopg.errors.LockNotAvailable):
                control.freeze()
            writer.commit()
        assert control.freeze().high_water == 1
    finally:
        engine.dispose()


@pytest.mark.asyncio
async def test_installed_adapter_refuses_changed_operation_and_registration(capacity_guard_database):
    database = capacity_guard_database
    _, registration = await _initialize_and_register(database)
    control = _control(database, registration)
    control.initialize()
    frozen = control.freeze()
    changed_operation = _control(database, registration, writer=control.binding.writer_incarnation)
    with pytest.raises((ValueError, psycopg.errors.ObjectNotInPrerequisiteState)):
        changed_operation.freeze()
    changed_registration = _control(
        database, registration.model_copy(update={"configuration_generation": registration.configuration_generation + 1}),
        writer=control.binding.writer_incarnation, operation=control.binding.freeze_operation_id,
    )
    with pytest.raises(ValueError, match="registration"):
        changed_registration.initialize()
    assert control.capture() == frozen
