"""Real authenticated retry continuity without reopening frozen legacy writes."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_worker_session import (
    _seed_claimed_protected_trial,
)
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize


@pytest.mark.parametrize(
    "failure_reason,expected_attempt_count",
    [("env_start_failure", 1), ("node_setup_health", 0)],
)
def test_authenticated_retry_survives_trial_writer_freeze(
    capacity_guard_database: dict[str, object],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_reason: str,
    expected_attempt_count: int,
) -> None:
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    freeze_operation = uuid4()
    frozen = asyncio.run(_freeze(database, initial["writer_incarnation"], freeze_operation))
    assert frozen["frozen"] is True

    with TestClient(seeded.app) as client:
        response = client.post(
            f"/trials/{seeded.trial_id}/retry",
            headers=seeded.claim_headers,
            json={
                "worker_id": str(seeded.worker.worker.worker_id),
                "failure_reason": failure_reason,
                "failure_message": "retry under the protected writer",
                "retry_after_sec": 0,
            },
        )
    assert response.status_code == 200, response.text
    assert response.json()["state"] == "protected-pending"

    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    "SELECT trial.state, trial.worker_id, trial.attempt_count, "
                    "trial.failure_reason, "
                    "(SELECT count(*) FROM loom_capacity_guard.trial_attempts AS item "
                    " WHERE item.trial_id = trial.id) AS attempt_rows, "
                    "(SELECT count(*) FROM "
                    " loom_capacity_guard.protected_runtime_trial_submissions AS item "
                    " WHERE item.trial_id = trial.id) AS runtime_rows, "
                    "(SELECT count(*) FROM "
                    " loom_capacity_guard.executable_claim_terminal_events AS item "
                    " WHERE item.protected_attempt_id = :attempt_id) AS terminal_rows "
                    "FROM public.trials AS trial WHERE trial.id = :trial_id"
                ),
                {
                    "trial_id": seeded.trial_id,
                    "attempt_id": seeded.first_attempt["protected_attempt_id"],
                },
            ).mappings().one()
    finally:
        engine.dispose()
    assert dict(result) == {
        "state": "protected-pending",
        "worker_id": None,
        "attempt_count": expected_attempt_count,
        "failure_reason": failure_reason,
        "attempt_rows": 2,
        "runtime_rows": 2,
        "terminal_rows": 1,
    }
    assert asyncio.run(
        _freeze(database, initial["writer_incarnation"], freeze_operation)
    ) == frozen
