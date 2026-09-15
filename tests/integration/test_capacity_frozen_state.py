"""Real authenticated state reports must survive the legacy trial-writer freeze."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from uuid import UUID, uuid4

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


@pytest.mark.parametrize("target_state", ["running", "materializing", "failed", "succeeded", "cancelled"])
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
        if target_state == "succeeded":
            payload["result"] = {"state": "succeeded", "reward": 0}
        errors = []
        report = ProtectedWorkerSessionStore.report_trial_state

        async def observed_report(self, **kwargs):
            try:
                return await report(self, **kwargs)
            except ProtectedWorkerSessionRejected as exc:
                original = getattr(exc.__cause__, "orig", None)
                errors.append((getattr(original, "sqlstate", None),
                               getattr(getattr(original, "diag", None), "message_primary", None)))
                raise

        monkeypatch.setattr(ProtectedWorkerSessionStore, "report_trial_state", observed_report)
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
        assert row["finished"] is (target_state in {"failed", "succeeded", "cancelled"})
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
                   "execution_lease_id": None, "execution_generation": None, "expected": None, "family": None}
        payload.update(extra or {})
        return await store.report_trial_state(
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


@pytest.mark.parametrize("frozen", [False, True])
def test_materializing_terminal_report_closes_exact_claim(capacity_guard_database, monkeypatch, tmp_path, frozen):
    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
    if frozen:
        asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
    with TestClient(seeded.app, raise_server_exceptions=False) as client:
        materializing = client.patch(f"/trials/{seeded.trial_id}/state", headers=seeded.claim_headers,
            json={"worker_id": str(seeded.worker.worker.worker_id), "state": "materializing"})
        assert materializing.status_code == 200, materializing.text
        terminal = client.patch(f"/trials/{seeded.trial_id}/state", headers=seeded.claim_headers,
            json={"worker_id": str(seeded.worker.worker.worker_id), "state": "failed", "failure_reason": "agent_error"})
        assert terminal.status_code == 200, terminal.text
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.executable_claim_terminal_events "
                "WHERE protected_attempt_id = :attempt"),
                {"attempt": seeded.first_attempt["protected_attempt_id"]}).scalar_one() == 1
    finally:
        engine.dispose()


@pytest.mark.parametrize("interference", [None, "suppress", "stale"])
@pytest.mark.parametrize("decision,target,count,index", [("advance", "adapting", 0, 0), ("retry", "pending", 1, 0),
                                                        ("skip", "done", 0, 1), ("abort", "aborted", 0, 0)])
def test_frozen_terminal_report_commits_family_decision(capacity_guard_database, monkeypatch, tmp_path, decision, target, count, index, interference):
    from loom.family_run.spec import AdvanceDecision

    database = capacity_guard_database
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    batch_id = uuid4()
    spec = {"enabled": True, "family_key_extractor": {"name": "instance_id_prefix", "params": {}},
            "sequencer": {"name": "alphabetical", "params": {}},
            "advance_predicate": {"name": "always_on_terminal", "params": {}},
            "adapter": {"name": "noop", "params": {}}, "failure_policy": {"name": "stall_family", "params": {}},
            "state_backend": {"name": "s3_artifacts", "params": {}}, "mount_path": "/root/.skills"}

    class Predicate:
        def decide(self, **kwargs):
            assert kwargs["trial"].state == "failed"
            return AdvanceDecision(decision)

    monkeypatch.setattr("loom_control_plane.routes.state.resolve_plugin", lambda *_: Predicate())
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.begin() as connection:
            trial = connection.execute(text("SELECT team_id, task_id FROM public.trials WHERE id = :id"),
                                       {"id": seeded.trial_id}).mappings().one()
            connection.execute(text("INSERT INTO public.batches "
                "(id, team_id, name, task_filter, trial_config, state, created_by_token_prefix, family_run_spec) "
                "VALUES (:batch, :team, :name, '{}'::jsonb, '{}'::jsonb, 'running', 'fixture', CAST(:spec AS jsonb))"),
                {"batch": batch_id, "team": trial["team_id"], "name": f"frozen-family-{batch_id}", "spec": json.dumps(spec)})
            connection.execute(text("UPDATE public.trials SET batch_id = :batch, family_key = 'family' WHERE id = :id"),
                               {"batch": batch_id, "id": seeded.trial_id})
            connection.execute(text("INSERT INTO public.batch_family_state "
                "(batch_id, family_key, task_sequence, current_index, state, attempt_count) "
                "VALUES (:batch, 'family', ARRAY[:task], 0, 'running', 0)"), {"batch": batch_id, "task": trial["task_id"]})
            if interference == "suppress":
                connection.exec_driver_sql(
                    "CREATE FUNCTION public.suppress_family_state() RETURNS trigger LANGUAGE plpgsql "
                    "AS $test$ BEGIN RETURN NULL; END $test$; "
                    "CREATE TRIGGER suppress_family_state BEFORE UPDATE ON public.batch_family_state "
                    "FOR EACH ROW EXECUTE FUNCTION public.suppress_family_state();"
                )
        sibling_id = None
        if decision in {"skip", "abort"}:
            with TestClient(seeded.app) as client:
                submitted = client.post("/trials", headers={"Authorization": f"Bearer {seeded.submit_token}"},
                    json={"task_id": trial["task_id"], "required_worker_pool": "oldlab",
                          "config": {"agent_name": "oracle", "agent_model": None}})
            assert submitted.status_code == 201, submitted.text
            sibling_id = UUID(submitted.json()["trial_id"])
            with engine.begin() as connection:
                connection.execute(text("UPDATE public.trials SET batch_id = :batch, family_key = 'family' WHERE id = :id"),
                                   {"batch": batch_id, "id": sibling_id})
        if interference == "stale":
            report = ProtectedWorkerSessionStore.report_trial_state

            async def change_family_before_write(self, **kwargs):
                with engine.begin() as connection:
                    connection.execute(text("UPDATE public.batch_family_state SET attempt_count = 2 WHERE batch_id = :id"), {"id": batch_id})
                return await report(self, **kwargs)

            monkeypatch.setattr(ProtectedWorkerSessionStore, "report_trial_state", change_family_before_write)
        initial = asyncio.run(_initialize(database, registration=seeded.worker.registration))
        asyncio.run(_freeze(database, initial["writer_incarnation"], uuid4()))
        with TestClient(seeded.app, raise_server_exceptions=False) as client:
            response = client.patch(f"/trials/{seeded.trial_id}/state", headers=seeded.claim_headers,
                json={"worker_id": str(seeded.worker.worker.worker_id), "state": "failed", "failure_reason": "agent_error"})
        assert response.status_code == (200 if interference is None else 409), response.text
        with engine.connect() as connection:
            family = connection.execute(text("SELECT state, current_index, attempt_count FROM public.batch_family_state WHERE batch_id = :id"),
                                        {"id": batch_id}).mappings().one()
            assert dict(family) == ({"state": target, "current_index": index, "attempt_count": count} if interference is None else
                                   {"state": "running", "current_index": 0, "attempt_count": 2 if interference == "stale" else 0})
            assert connection.execute(text("SELECT state FROM public.trials WHERE id = :id"), {"id": seeded.trial_id}).scalar_one() == (
                "failed" if interference is None else "claimed")
            assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations")).scalar_one() == 0
            if sibling_id is not None:
                assert connection.execute(text("SELECT state FROM public.trials WHERE id = :id"),
                                          {"id": sibling_id}).scalar_one() == (
                    "cancelled" if interference is None else "protected-pending")
            if interference is not None:
                assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.trial_mutation_permits")).scalar_one() == 0
                assert connection.execute(text("SELECT count(*) FROM loom_capacity_guard.executable_claim_terminal_events")).scalar_one() == 0
    finally:
        engine.dispose()
