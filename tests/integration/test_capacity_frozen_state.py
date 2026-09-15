"""Real authenticated state reports must survive the legacy trial-writer freeze."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text

from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_worker_session import _seed_claimed_protected_trial
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize


@pytest.mark.parametrize("target_state", ["running", "materializing", "failed"])
@pytest.mark.parametrize("freeze_before_report", [False, True])
def test_authenticated_state_report_survives_trial_writer_freeze(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    target_state: str,
    freeze_before_report: bool,
) -> None:
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    operation = uuid4()
    if freeze_before_report:
        frozen = asyncio.run(_freeze(database, initial["writer_incarnation"], operation))
        assert frozen["frozen"] is True

    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            before = connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations"
            )).scalar_one()
        payload = {"worker_id": str(seeded.worker.worker.worker_id), "state": target_state}
        if target_state == "failed":
            payload["failure_reason"] = "agent_error"
        errors = []
        with TestClient(seeded.app, raise_server_exceptions=False) as client:
            def observe_error(context):
                original = context.original_exception
                errors.append((getattr(original, "sqlstate", None),
                               getattr(getattr(original, "diag", None), "message_primary", None)))

            event.listen(seeded.app.state.session_factory.kw["bind"].sync_engine,
                         "handle_error", observe_error)
            response = client.patch(
                f"/trials/{seeded.trial_id}/state", headers=seeded.claim_headers, json=payload,
            )
        assert response.status_code == 200, (response.text, errors)
        assert response.json()["state"] == target_state
        with engine.connect() as connection:
            row = connection.execute(text(
                "SELECT state, worker_id, started_at IS NOT NULL AS started, "
                "finished_at IS NOT NULL AS finished FROM public.trials WHERE id = :id"
            ), {"id": seeded.trial_id}).mappings().one()
            after = connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations"
            )).scalar_one()
        assert row["state"] == target_state
        assert row["worker_id"] == seeded.worker.worker.worker_id
        assert row["started"] is (target_state == "running")
        assert row["finished"] is (target_state == "failed")
        assert after - before == (0 if freeze_before_report else 1)
        if freeze_before_report:
            assert asyncio.run(_freeze(database, initial["writer_incarnation"], operation)) == frozen
    finally:
        engine.dispose()
