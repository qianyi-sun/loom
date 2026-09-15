"""Real authenticated state reports must survive the legacy trial-writer freeze."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_control_plane.protected_worker_session import (
    ProtectedWorkerSessionRejected,
    ProtectedWorkerSessionStore,
)
from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_worker_session import (
    _WORKER_CREDENTIAL,
    _seed_claimed_protected_trial,
)
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
        report = ProtectedWorkerSessionStore.report_trial_progress

        async def observed_report(self, **kwargs):
            try:
                return await report(self, **kwargs)
            except ProtectedWorkerSessionRejected as exc:
                original = getattr(exc.__cause__, "orig", None)
                errors.append((getattr(original, "sqlstate", None),
                               getattr(getattr(original, "diag", None), "message_primary", None)))
                raise

        monkeypatch.setattr(ProtectedWorkerSessionStore, "report_trial_progress", observed_report)
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


async def _report(database, seeded, *, credential=_WORKER_CREDENTIAL, extra=None):
    engine = create_async_engine(_value(database, "runtime_url"), isolation_level="SERIALIZABLE")
    try:
        store = ProtectedWorkerSessionStore(async_sessionmaker(engine, expire_on_commit=False))
        payload = {"trial_id": str(seeded.trial_id), "state": "running", "result": None,
                   "failure_reason": None, "failure_message": None,
                   "execution_lease_id": None, "execution_generation": None}
        payload.update(extra or {})
        return await store.report_trial_progress(
            worker_id=seeded.worker.worker.worker_id, worker_credential=credential, report=payload,
        )
    finally:
        await engine.dispose()


@pytest.mark.parametrize("bad_input", ["credential", "extra_field", "terminal_state"])
def test_frozen_progress_rejects_unadmitted_input(capacity_guard_database, monkeypatch, tmp_path, bad_input):
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
    extra = {"worker_id": str(uuid4())} if bad_input == "extra_field" else (
        {"state": "failed"} if bad_input == "terminal_state" else None)
    with pytest.raises(ProtectedWorkerSessionRejected):
        asyncio.run(_report(database, seeded, credential=("wrong" if bad_input == "credential" else _WORKER_CREDENTIAL), extra=extra))
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text("SELECT state FROM public.trials WHERE id = :id"),
                                      {"id": seeded.trial_id}).scalar_one() == "claimed"
            assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_mutation_permits")).scalar_one() == 0
            assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")).scalar_one() == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("interference", ["extra_column", "suppress"])
def test_frozen_progress_rolls_back_unapproved_trigger_effects(capacity_guard_database, monkeypatch, tmp_path, interference):
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            body = "RETURN NULL;" if interference == "suppress" else "NEW.config := NEW.config || '{\"unauthorized\":true}'::jsonb; RETURN NEW;"
            connection.exec_driver_sql(
                "CREATE FUNCTION public.frozen_progress_interference() RETURNS trigger LANGUAGE plpgsql "
                "AS $test$ BEGIN " + body + " END $test$; "
                "CREATE TRIGGER frozen_progress_interference BEFORE UPDATE ON public.trials "
                "FOR EACH ROW EXECUTE FUNCTION public.frozen_progress_interference();"
            )
            before = connection.execute(text("SELECT to_jsonb(t) FROM public.trials t WHERE id = :id"),
                                        {"id": seeded.trial_id}).scalar_one()
        initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
        asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
        with pytest.raises(ProtectedWorkerSessionRejected):
            asyncio.run(_report(database, seeded))
        with engine.connect() as connection:
            assert connection.execute(text("SELECT to_jsonb(t) FROM public.trials t WHERE id = :id"),
                                      {"id": seeded.trial_id}).scalar_one() == before
            assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_mutation_permits")).scalar_one() == 0
            assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")).scalar_one() == 0
    finally:
        engine.dispose()
