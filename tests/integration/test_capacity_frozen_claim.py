"""A real assigned worker must retain narrowly authorized claim after freeze."""

import asyncio
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom_control_plane.protected_worker_session import (
    ProtectedWorkerSessionRejected,
    ProtectedWorkerSessionStore,
)
from tests.integration import test_capacity_protected_worker_session as worker_fixtures
from tests.integration.test_capacity_agent_store import _seed_trial, _value
from tests.integration.test_capacity_protected_worker_session import (
    _WORKER_CREDENTIAL,
    _assign_retry_attempt,
    _seed_claimed_protected_trial,
)
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


def _freeze_on_first_claim(database, monkeypatch):
    original = ProtectedWorkerSessionStore.claim_assigned_trial
    observations = []

    async def wrapped(self, **kwargs):
        if not observations:
            registration = await self.current_registration()
            initial = await _initialize(database, registration=registration)
            observations.append(await _freeze(database, initial["writer_incarnation"], uuid4()))
        return await original(self, **kwargs)

    monkeypatch.setattr(ProtectedWorkerSessionStore, "claim_assigned_trial", wrapped)
    return observations


@pytest.mark.parametrize("failure_reason", ["env_start_failure", "node_setup_health"])
def test_frozen_claim_retry_reassignment_and_reclaim_preserve_attempt_identity(
    capacity_guard_database, monkeypatch, tmp_path, failure_reason,
):
    database = capacity_guard_database
    observations = _freeze_on_first_claim(database, monkeypatch)
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    with TestClient(seeded.app) as client:
        retry = client.post(
            f"/trials/{seeded.trial_id}/retry", headers=seeded.claim_headers,
            json={"worker_id": str(seeded.worker.worker.worker_id),
                  "failure_reason": failure_reason, "retry_after_sec": 0},
        )
    assert retry.status_code == 200, retry.text
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            due = connection.execute(text(
                "SELECT next_attempt_at FROM public.trials WHERE id=:trial"
            ), {"trial": seeded.trial_id}).scalar_one()
        assert due is not None  # guard_0025's due retry is not an initial NULL-only claim.
        second_attempt = _assign_retry_attempt(database, seeded)
        with TestClient(seeded.app) as client:
            claimed = client.post(
                "/work/claim", headers=seeded.claim_headers, json=seeded.claim_payload,
            )
        assert claimed.status_code == 200, claimed.text
        count = 1 if failure_reason == "node_setup_health" else 2
        assert claimed.json()["payload"]["attempt_count"] == count
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT next_attempt_at FROM public.trials WHERE id=:trial"
            ), {"trial": seeded.trial_id}).scalar_one() == due
            assert connection.execute(text(
                "SELECT attempt FROM public.execution_admission_reservations "
                "WHERE trial_id=:trial ORDER BY attempt"
            ), {"trial": seeded.trial_id}).scalars().all() == [1, 2]
            claims = connection.execute(text(
                "SELECT permit.protected_attempt_id, permit.claim_operation_id, "
                "permit.observed_old_row, permit.observed_new_row, permit.changes, "
                "claim.operation_id AS actual_claim "
                "FROM loom_capacity_guard.trial_mutation_permits AS permit "
                "JOIN loom_capacity_guard.executable_claim_leases AS claim "
                "ON claim.operation_id=permit.claim_operation_id "
                "WHERE permit.operation='claim' AND permit.state='consumed'"
            )).mappings().all()
            assert {row["protected_attempt_id"] for row in claims} == {
                seeded.first_attempt["protected_attempt_id"],
                second_attempt["protected_attempt_id"],
            }
            assert len({row["claim_operation_id"] for row in claims}) == 2
            for row in claims:
                assert row["claim_operation_id"] == row["actual_claim"]
                assert row["observed_new_row"] == row["observed_old_row"] | row["changes"]
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.trial_writer_mutations"
            )).scalar_one() == observations[0]["high_water"] == 0
    finally:
        engine.dispose()


def _claim_snapshot(connection):
    # Full disposable state images, including admission effects preceding UPDATE.
    relations = (
        "public.trials", "public.team_quotas", "public.execution_admission_reservations",
        "public.execution_admission_policies", "public.batch_family_state",
        "loom_capacity_guard.executable_claim_state", "loom_capacity_guard.executable_claim_leases",
        "loom_capacity_guard.trial_attempts", "loom_capacity_guard.attempt_lifecycle_heads",
        "loom_capacity_guard.trial_mutation_permits", "loom_capacity_guard.trial_writer_mutations",
    )
    return {relation: connection.execute(text(
        f"SELECT to_jsonb(item) FROM {relation} AS item ORDER BY to_jsonb(item)::text"
    )).scalars().all() for relation in relations}


@pytest.mark.parametrize("fault", ["extra-column", "extra-row", "suppressed-row", "rollback"])
def test_frozen_claim_failure_rolls_back_all_admission_effects(
    capacity_guard_database, monkeypatch, tmp_path, fault,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.claim_assigned_trial
    checked = []

    async def fault_then_clean_claim(self, **kwargs):
        other_trial = _seed_trial(database)
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())
        engine = create_engine(_value(database, "admin_url"))
        body = {
            "extra-column": "NEW.submit_priority := OLD.submit_priority + 1; RETURN NEW;",
            "extra-row": (
                f"UPDATE public.trials SET submit_priority=999 WHERE id='{other_trial}'::uuid; RETURN NEW;"
            ),
            "suppressed-row": "RETURN NULL;",
            "rollback": "RAISE EXCEPTION 'injected claim rollback' USING ERRCODE='55000';",
        }[fault]
        timing = "AFTER" if fault in {"extra-row", "rollback"} else "BEFORE"
        try:
            with engine.begin() as connection:
                before = _claim_snapshot(connection)
                connection.exec_driver_sql(
                    "CREATE FUNCTION public.inject_claim_fault() RETURNS trigger "
                    f"LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN {body} END $$"
                )
                connection.exec_driver_sql(
                    f"CREATE TRIGGER zzzz_claim_fault {timing} UPDATE ON public.trials "
                    "FOR EACH ROW EXECUTE FUNCTION public.inject_claim_fault()"
                )
            with pytest.raises(ProtectedWorkerSessionRejected):
                await original(self, **kwargs)
            with engine.begin() as connection:
                assert _claim_snapshot(connection) == before
                connection.exec_driver_sql("DROP TRIGGER zzzz_claim_fault ON public.trials")
                connection.exec_driver_sql("DROP FUNCTION public.inject_claim_fault()")
            checked.append(fault)
            return await original(self, **kwargs)
        finally:
            engine.dispose()

    monkeypatch.setattr(ProtectedWorkerSessionStore, "claim_assigned_trial", fault_then_clean_claim)
    _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    assert checked == [fault]


def test_frozen_claim_bad_credential_cannot_issue_permission(
    capacity_guard_database, monkeypatch, tmp_path,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.claim_assigned_trial
    checked = []

    async def bad_then_clean_claim(self, **kwargs):
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())
        engine = create_engine(_value(database, "admin_url"))
        try:
            with engine.connect() as connection:
                before = _claim_snapshot(connection)
            with pytest.raises(ProtectedWorkerSessionRejected) as refusal:
                await original(self, **(kwargs | {"worker_credential": "wrong-credential"}))
            assert refusal.value.__cause__.orig.sqlstate == "42501"
            with engine.connect() as connection:
                assert _claim_snapshot(connection) == before
            checked.append(True)
            return await original(self, **kwargs)
        finally:
            engine.dispose()

    monkeypatch.setattr(ProtectedWorkerSessionStore, "claim_assigned_trial", bad_then_clean_claim)
    _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    assert checked == [True]


def test_frozen_claim_concurrency_consumes_only_one_assignment(
    capacity_guard_database, monkeypatch, tmp_path,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.claim_assigned_trial
    checked = []

    async def racing_claim(self, **kwargs):
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())

        async def attempt():
            try:
                return await original(self, **kwargs)
            except ProtectedWorkerSessionRejected as error:
                assert error.__cause__.orig.sqlstate != "40P01", "claim deadlocked"
                return error

        results = await asyncio.wait_for(asyncio.gather(attempt(), attempt()), timeout=30)
        winners = [result for result in results if isinstance(result, dict)]
        assert len(winners) == 1
        checked.append(True)
        return winners[0]

    monkeypatch.setattr(ProtectedWorkerSessionStore, "claim_assigned_trial", racing_claim)
    _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    assert checked == [True]
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT state FROM loom_capacity_guard.trial_mutation_permits"
            )).scalars().all() == ["consumed"]
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.executable_claim_leases"
            )).scalar_one() == 1
    finally:
        engine.dispose()


def test_frozen_claim_without_manager_assignment_cannot_issue_permission(
    capacity_guard_database, monkeypatch, tmp_path,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.claim_assigned_trial
    assign = worker_fixtures._assign_protected_attempt
    delayed = []
    checked = []

    async def delay_assignment(*args, **kwargs):
        delayed.append((args, kwargs))

    async def unassigned_then_assigned_claim(self, **kwargs):
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())
        engine = create_engine(_value(database, "admin_url"))
        try:
            with engine.connect() as connection:
                before = _claim_snapshot(connection)
            assert await original(self, **kwargs) is None
            with engine.connect() as connection:
                assert _claim_snapshot(connection) == before
            assert len(delayed) == 1
            args, assignment = delayed[0]
            await assign(*args, **assignment)
            checked.append(True)
            return await original(self, **kwargs)
        finally:
            engine.dispose()

    monkeypatch.setattr(worker_fixtures, "_assign_protected_attempt", delay_assignment)
    monkeypatch.setattr(ProtectedWorkerSessionStore, "claim_assigned_trial", unassigned_then_assigned_claim)
    _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    assert checked == [True]


def test_frozen_claim_and_retry_contention_preserves_single_successor(
    capacity_guard_database, monkeypatch, tmp_path,
):
    database = capacity_guard_database
    _freeze_on_first_claim(database, monkeypatch)
    seeded = _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    claim_request = {key: value for key, value in seeded.claim_payload.items() if key != "schema_version"}
    claim_request.update(schema_version=1, protocol="work")
    retry_request = {
        "schema_version": 1, "trial_id": str(seeded.trial_id),
        "worker_id": str(seeded.worker.worker.worker_id), "failure_reason": "node_setup_health",
        "failure_message": None, "retry_after_sec": 0,
    }

    async def race():
        engine = create_async_engine(_value(database, "runtime_url"), isolation_level="SERIALIZABLE")
        store = ProtectedWorkerSessionStore(async_sessionmaker(engine, expire_on_commit=False))

        async def invoke(operation):
            try:
                shared = {"worker_id": seeded.worker.worker.worker_id, "worker_credential": _WORKER_CREDENTIAL}
                if operation == "claim":
                    return await store.claim_assigned_trial(**shared, claim_request=claim_request)
                return await store.retry_claimed_trial(**shared, retry_request=retry_request)
            except ProtectedWorkerSessionRejected as error:
                assert error.__cause__.orig.sqlstate != "40P01", "mixed operation deadlocked"
                return error

        try:
            claimed, retried = await asyncio.wait_for(
                asyncio.gather(invoke("claim"), invoke("retry")), timeout=30,
            )
            # No new assignment exists yet, regardless of which transaction won.
            assert claimed is None or isinstance(claimed, ProtectedWorkerSessionRejected)
            if isinstance(retried, ProtectedWorkerSessionRejected):
                retried = await invoke("retry")
            assert isinstance(retried, dict)
        finally:
            await engine.dispose()

    asyncio.run(race())
    _assign_retry_attempt(database, seeded)
    with TestClient(seeded.app) as client:
        result = client.post("/work/claim", headers=seeded.claim_headers, json=seeded.claim_payload)
    assert result.status_code == 200, result.text
    assert result.json()["payload"]["attempt_count"] == 1
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT operation,state FROM loom_capacity_guard.trial_mutation_permits ORDER BY operation"
            )).all() == [("claim", "consumed"), ("claim", "consumed"), ("refund", "consumed"), ("retry", "consumed")]
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.trial_attempts WHERE trial_id=:trial"
            ), {"trial": seeded.trial_id}).scalar_one() == 2
    finally:
        engine.dispose()
