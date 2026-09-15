"""Real user cancellation must survive freeze without inventing worker authority."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from loom_control_plane.protected_worker_session import (
    ProtectedTrialCancellationError,
    ProtectedWorkerSessionStore,
)
from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_worker_session import (
    test_protected_pending_trial_cancel_is_guarded_atomic_and_idempotent as exercise_cancellation,
)
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize


@pytest.mark.parametrize("freeze_before_cancel", [False, True])
def test_user_pending_cancellation_and_replay_survive_frozen_writer(
    capacity_guard_database, monkeypatch, tmp_path, freeze_before_cancel,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.cancel_pending_trial
    observations = []
    diagnostics = []
    operation = uuid4()

    async def freeze_then_cancel(self, **kwargs):
        if not observations:
            registration = await self.current_registration()
            initial = await _initialize(database, registration=registration)
            frozen = (
                await _freeze(database, initial["writer_incarnation"], operation)
                if freeze_before_cancel else None
            )
            observations.append((initial, frozen))
        try:
            return await original(self, **kwargs)
        except ProtectedTrialCancellationError as error:
            original_error = getattr(error.__cause__, "orig", None)
            diagnostic = getattr(original_error, "diag", None)
            diagnostics.append((
                getattr(original_error, "sqlstate", None),
                getattr(diagnostic, "message_primary", None),
            ))
            raise

    monkeypatch.setattr(ProtectedWorkerSessionStore, "cancel_pending_trial", freeze_then_cancel)
    try:
        exercise_cancellation(database, monkeypatch, tmp_path)
    except AssertionError as error:
        error.add_note("isolated pending cancellation SQL refusal: " + repr(diagnostics))
        raise
    assert len(observations) == 1
    initial, frozen = observations[0]
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            mutations = connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations"
            )).scalar_one()
    finally:
        engine.dispose()
    final = asyncio.run(_freeze(database, initial["writer_incarnation"], operation))
    if frozen is not None:
        assert final == frozen
        assert mutations == frozen["high_water"] == 0
    else:
        assert mutations == final["high_water"] == 1
