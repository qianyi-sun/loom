"""Real user cancellation must survive freeze without inventing worker authority."""

import asyncio
import json
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from loom_control_plane.protected_worker_session import (
    _CANCEL_PENDING_TRIAL,
    _CLAIM_ASSIGNED_TRIAL,
    ProtectedTrialCancellationError,
    ProtectedWorkerSessionRejected,
    ProtectedWorkerSessionStore,
)
from tests.integration.test_capacity_agent_store import _seed_trial, _value
from tests.integration.test_capacity_frozen_claim import _claim_snapshot
from tests.integration.test_capacity_protected_worker_session import (
    _seed_claimed_protected_trial,
)
from tests.integration.test_capacity_protected_worker_session import (
    test_family_orchestrator_cancels_assigned_unclaimed_protected_trial as exercise_assigned_cancellation,
)
from tests.integration.test_capacity_protected_worker_session import (
    test_protected_pending_trial_cancel_is_guarded_atomic_and_idempotent as exercise_cancellation,
)
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize
from tests.integration.test_capacity_trial_writer_retirement import _downgrade


@pytest.mark.parametrize("freeze_before_cancel", [False, True])
@pytest.mark.parametrize("exercise", [exercise_cancellation, exercise_assigned_cancellation], ids=["unassigned-user", "assigned-family"])
def test_user_pending_cancellation_and_replay_survive_frozen_writer(
    capacity_guard_database, monkeypatch, tmp_path, freeze_before_cancel, exercise,
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
            result = await original(self, **kwargs)
            replayed = await original(self, **kwargs)
            assert replayed == dict(result) | {"replayed": True}
            return result
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
        exercise(database, monkeypatch, tmp_path)
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
            permits = connection.execute(text(
                "SELECT permit.*, head.transition_id AS current_transition "
                "FROM loom_capacity_guard.trial_mutation_permits AS permit "
                "JOIN loom_capacity_guard.attempt_lifecycle_heads AS head "
                "ON head.protected_attempt_id=permit.protected_attempt_id"
            )).mappings().all()
    finally:
        engine.dispose()


    final = asyncio.run(_freeze(database, initial["writer_incarnation"], operation))
    if frozen is not None:
        assert final == frozen
        assert mutations == frozen["high_water"] == 0
        assert len(permits) == 1
        permit = permits[0]
        assert permit["operation"] == "pending_cancel"
        assert permit["state"] == "consumed"
        assert permit["worker_id"] is permit["worker_incarnation"] is permit["claim_operation_id"] is None
        assert permit["cancellation_transition_id"] == permit["current_transition"]
        assert permit["old_binding"]["team_id"] is not None
        assert permit["observed_new_row"] == permit["observed_old_row"] | permit["changes"]
        assert set(permit["changes"]) == {
            "state", "cancellation_requested_at", "cancellation_observed_at", "finished_at",
        }
        with pytest.raises(RuntimeError, match="permission evidence requires protected retirement"):
            _downgrade(database, monkeypatch)
    else:
        assert mutations == final["high_water"] == 1
        assert permits == []


def _cancel_snapshot(connection):
    state = _claim_snapshot(connection)
    for relation in (
        "loom_capacity_guard.attempt_lifecycle_events",
        "loom_capacity_guard.protected_runtime_trial_submissions",
        "loom_capacity_guard.executable_claim_terminal_events",
    ):
        state[relation] = connection.execute(text(
            f"SELECT to_jsonb(item) FROM {relation} AS item ORDER BY to_jsonb(item)::text"
        )).scalars().all()
    return state


@pytest.mark.parametrize("fault", ["extra-column", "extra-row", "suppressed-row", "rollback"])
def test_frozen_cancel_fault_rolls_back_lifecycle_and_public_state(
    capacity_guard_database, monkeypatch, tmp_path, fault,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.cancel_pending_trial
    checked = []

    async def fault_then_cancel(self, **kwargs):
        if checked:
            return await original(self, **kwargs)
        other_trial = _seed_trial(database)
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())
        body = {
            "extra-column": "NEW.submit_priority := OLD.submit_priority + 1; RETURN NEW;",
            "extra-row": (
                f"UPDATE public.trials SET submit_priority=999 WHERE id='{other_trial}'::uuid; RETURN NEW;"
            ),
            "suppressed-row": "RETURN NULL;",
            "rollback": "RAISE EXCEPTION 'injected cancel rollback' USING ERRCODE='55000';",
        }[fault]
        timing = "AFTER" if fault in {"extra-row", "rollback"} else "BEFORE"
        engine = create_engine(_value(database, "admin_url"))
        try:
            with engine.begin() as connection:
                before = _cancel_snapshot(connection)
                connection.exec_driver_sql(
                    "CREATE FUNCTION public.inject_cancel_fault() RETURNS trigger "
                    f"LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $$ BEGIN {body} END $$"
                )
                connection.exec_driver_sql(
                    f"CREATE TRIGGER zzzz_cancel_fault {timing} UPDATE ON public.trials "
                    "FOR EACH ROW EXECUTE FUNCTION public.inject_cancel_fault()"
                )
            with pytest.raises(ProtectedTrialCancellationError) as refusal:
                await original(self, **kwargs)
            assert refusal.value.__cause__.orig.sqlstate == ("40001" if fault == "suppressed-row" else "55000")
            with engine.begin() as connection:
                assert _cancel_snapshot(connection) == before
                connection.exec_driver_sql("DROP TRIGGER zzzz_cancel_fault ON public.trials")
                connection.exec_driver_sql("DROP FUNCTION public.inject_cancel_fault()")
            checked.append(fault)
            return await original(self, **kwargs)
        finally:
            engine.dispose()

    monkeypatch.setattr(ProtectedWorkerSessionStore, "cancel_pending_trial", fault_then_cancel)
    exercise_cancellation(database, monkeypatch, tmp_path)
    assert checked == [fault]


def test_frozen_cancel_preserves_team_and_private_function_authorization(
    capacity_guard_database, monkeypatch, tmp_path,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.cancel_pending_trial
    checked = []

    async def refused_then_cancel(self, **kwargs):
        if checked:
            return await original(self, **kwargs)
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())
        engine = create_engine(_value(database, "admin_url"))
        runtime = create_async_engine(_value(database, "runtime_url"), isolation_level="SERIALIZABLE")
        try:
            with engine.connect() as connection:
                before = _cancel_snapshot(connection)
            assert await original(self, **(kwargs | {"team_id": uuid4()})) is None
            # The runtime may call the guarded cancellation entry point, never its issuer.
            with pytest.raises(DBAPIError) as private_refusal:
                async with runtime.begin() as connection:
                    await connection.execute(text(
                        "SELECT loom_capacity_guard.authorize_frozen_pending_cancel("
                        "CAST(:trial AS uuid),gen_random_uuid(),1,gen_random_uuid(),"
                        "CAST(:team AS uuid),statement_timestamp())"
                    ), {"trial": kwargs["trial_id"], "team": kwargs["team_id"]})
            assert private_refusal.value.orig.sqlstate == "42501"
            # Even a privileged session cannot impersonate the bound runtime login.
            with pytest.raises(DBAPIError) as runtime_refusal:
                with engine.connect().execution_options(isolation_level="SERIALIZABLE") as connection:
                    with connection.begin():
                        connection.execute(text(
                            "SELECT loom_capacity_guard.cancel_protected_runtime_pending_trial("
                            "CAST(:trial AS uuid),CAST(:team AS uuid))"
                        ), {"trial": kwargs["trial_id"], "team": kwargs["team_id"]})
            assert runtime_refusal.value.orig.sqlstate == "42501"
            with engine.connect() as connection:
                assert _cancel_snapshot(connection) == before
            checked.append(True)
            return await original(self, **kwargs)
        finally:
            await runtime.dispose()
            engine.dispose()

    monkeypatch.setattr(ProtectedWorkerSessionStore, "cancel_pending_trial", refused_then_cancel)
    exercise_cancellation(database, monkeypatch, tmp_path)
    assert checked == [True]


def test_frozen_concurrent_cancellations_emit_one_transition_and_replay(
    capacity_guard_database, monkeypatch, tmp_path,
):
    database = capacity_guard_database
    original = ProtectedWorkerSessionStore.cancel_pending_trial
    checked = []

    async def racing_cancel(self, **kwargs):
        if checked:
            return await original(self, **kwargs)
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())

        async def attempt():
            try:
                return await original(self, **kwargs)
            except ProtectedTrialCancellationError as error:
                assert error.__cause__.orig.sqlstate in {"55P03", "40001"}
                return error

        results = await asyncio.wait_for(asyncio.gather(attempt(), attempt()), timeout=30)
        successes = [result for result in results if isinstance(result, dict)]
        assert sum(not result["replayed"] for result in successes) == 1
        replay = await original(self, **kwargs)
        assert replay == dict(successes[0]) | {"replayed": True}
        checked.append(True)
        return successes[0]

    monkeypatch.setattr(ProtectedWorkerSessionStore, "cancel_pending_trial", racing_cancel)
    exercise_cancellation(database, monkeypatch, tmp_path)
    assert checked == [True]
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT operation,state FROM loom_capacity_guard.trial_mutation_permits"
            )).all() == [("pending_cancel", "consumed")]
    finally:
        engine.dispose()
@pytest.mark.parametrize("first_operation", ["cancel", "claim"])
def test_frozen_cancel_claim_contention_is_exclusive_and_rollback_recoverable(
    capacity_guard_database, monkeypatch, tmp_path, first_operation,
):
    database = capacity_guard_database
    original_claim = ProtectedWorkerSessionStore.claim_assigned_trial
    original_cancel = ProtectedWorkerSessionStore.cancel_pending_trial
    checked = []

    async def contend_then_claim(self, **kwargs):
        registration = await self.current_registration()
        initial = await _initialize(database, registration=registration)
        await _freeze(database, initial["writer_incarnation"], uuid4())
        engine = create_engine(_value(database, "admin_url"))
        try:
            with engine.connect() as connection:
                before = _cancel_snapshot(connection)
                trial, team = connection.execute(text(
                    "SELECT trial.id,trial.team_id FROM public.trials AS trial "
                    "JOIN loom_capacity_guard.protected_runtime_trial_submissions AS runtime "
                    "ON runtime.trial_id=trial.id"
                )).one()
            # Complete the first real SQL operation but hold its transaction open.
            # This deterministically exercises both lock orders, without sleeps.
            async with self._session_factory() as session:
                transaction = await session.begin()
                try:
                    if first_operation == "cancel":
                        result = (await session.execute(
                            _CANCEL_PENDING_TRIAL, {"trial_id": trial, "team_id": team},
                        )).scalar_one()
                        assert result["state"] == "cancelled" and not result["replayed"]
                        with pytest.raises(ProtectedWorkerSessionRejected) as refusal:
                            await asyncio.wait_for(original_claim(self, **kwargs), timeout=10)
                    else:
                        result = (await session.execute(_CLAIM_ASSIGNED_TRIAL, {
                            "worker_id": kwargs["worker_id"],
                            "credential": kwargs["worker_credential"],
                            "claim_request": json.dumps(dict(kwargs["claim_request"])),
                        })).scalar_one()
                        assert result is not None
                        with pytest.raises(ProtectedTrialCancellationError) as refusal:
                            await asyncio.wait_for(
                                original_cancel(self, trial_id=trial, team_id=team), timeout=10,
                            )
                    assert refusal.value.__cause__.orig.sqlstate == "55P03"
                finally:
                    await transaction.rollback()
            with engine.connect() as connection:
                assert _cancel_snapshot(connection) == before
            # The rolled-back operation leaves no orphan transition, claim, or permit.
            claimed = await original_claim(self, **kwargs)
            assert claimed is not None
            assert await original_cancel(self, trial_id=trial, team_id=team) is None
            checked.append(first_operation)
            return claimed
        finally:
            engine.dispose()

    monkeypatch.setattr(ProtectedWorkerSessionStore, "claim_assigned_trial", contend_then_claim)
    _seed_claimed_protected_trial(database, monkeypatch, tmp_path)
    assert checked == [first_operation]
    engine = create_engine(_value(database, "admin_url"))
    try:
        with engine.connect() as connection:
            assert connection.execute(text(
                "SELECT operation,state FROM loom_capacity_guard.trial_mutation_permits"
            )).all() == [("claim", "consumed")]
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.attempt_lifecycle_events WHERE operation='cancel'"
            )).scalar_one() == 0
    finally:
        engine.dispose()


@pytest.mark.parametrize("operation,worker,incarnation,claim,cancellation", [
    ("claim", False, True, True, False),
    ("retry", True, False, True, False),
    ("refund", True, True, False, False),
    ("claim", True, True, True, True),
    ("pending_cancel", True, True, True, True),
    ("pending_cancel", False, False, False, False),
])
def test_mutation_ledger_rejects_missing_or_mixed_actor_identities(
    capacity_guard_database, operation, worker, incarnation, claim, cancellation,
):
    engine = create_engine(_value(capacity_guard_database, "admin_url"))
    try:
        with pytest.raises(DBAPIError) as refusal:
            with engine.begin() as connection:
                connection.execute(text(
                    "INSERT INTO loom_capacity_guard.trial_mutation_permits "
                    "(permit_id,transaction_id,backend_pid,writer_incarnation,writer_epoch,"
                    "freeze_operation_id,authority_binding,registration,trial_id,"
                    "protected_attempt_id,execution_generation,worker_id,worker_incarnation,"
                    "claim_operation_id,cancellation_transition_id,operation,old_binding,changes) "
                    "VALUES (gen_random_uuid(),pg_current_xact_id(),pg_backend_pid(),"
                    "gen_random_uuid(),1,gen_random_uuid(),'{}','{}',gen_random_uuid(),"
                    "gen_random_uuid(),1,:worker,:incarnation,:claim,:cancellation,:operation,'{}','{}')"
                ), {"worker": uuid4() if worker else None,
                    "incarnation": uuid4() if incarnation else None,
                    "claim": uuid4() if claim else None,
                    "cancellation": uuid4() if cancellation else None,
                    "operation": operation})
        assert refusal.value.orig.sqlstate == "23514"
        assert refusal.value.orig.diag.constraint_name == "trial_mutation_permit_actor_binding"
    finally:
        engine.dispose()
