"""A real assigned worker must retain narrowly authorized claim after freeze."""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text

from loom_control_plane.protected_worker_session import (
    ProtectedWorkerSessionRejected,
    ProtectedWorkerSessionStore,
)
from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_worker_session import _seed_claimed_protected_trial
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize


@pytest.mark.parametrize("freeze_before_claim", [False, True])
def test_authenticated_assigned_claim_survives_trial_writer_freeze(
    capacity_guard_database, monkeypatch, tmp_path, freeze_before_claim,
):
    database = capacity_guard_database
    claim = ProtectedWorkerSessionStore.claim_assigned_trial
    observations = []
    diagnostics = []
    operation = uuid4()

    async def freeze_then_claim(self, **kwargs):
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        frozen = (
            await _freeze(database, initial["writer_incarnation"], operation)
            if freeze_before_claim else None
        )
        observations.append((initial, frozen))
        try:
            return await claim(self, **kwargs)
        except ProtectedWorkerSessionRejected as error:
            original = getattr(error.__cause__, "orig", None)
            diagnostic = getattr(original, "diag", None)
            diagnostics.append((
                getattr(original, "sqlstate", None),
                getattr(diagnostic, "message_primary", None),
            ))
            raise

    monkeypatch.setattr(ProtectedWorkerSessionStore, "claim_assigned_trial", freeze_then_claim)
    try:
        seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    except AssertionError as error:
        error.add_note("isolated claim SQL refusal: " + repr(diagnostics))
        raise
    assert len(observations) == 1
    initial, frozen = observations[0]
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT state,worker_id,attempt_count FROM public.trials WHERE id=:trial"
            ), {"trial": seeded.trial_id}).one() == ("claimed", seeded.worker.worker.worker_id, 1)
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
