"""Preserve existing queued trial identity while admitting protected execution."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.db.schema import Batch, DataLifecycleAuthority, Trial
from loom_control_plane.protected_worker_session import (
    ProtectedTrialSubmissionError,
    ProtectedWorkerSessionStore,
)
from tests.integration.test_capacity_agent_store import _value
from tests.integration.test_capacity_protected_trial_submission_route import _initialize_guard
from tests.integration.test_capacity_submission_store import _atomic_submission, _seed_trial_inputs
from tests.integration.test_capacity_trial_writer_fence import _freeze, _initialize


@pytest.mark.asyncio
@pytest.mark.parametrize("frozen", [False, True])
@pytest.mark.parametrize("drift", [None, "team", "config", "timestamp", "lifecycle", "attempt", "cancelled", "expired", "deleting", "ordinary-submit", "wrong-role"])
async def test_adoption_preserves_original_batch_trial_and_retention(
    capacity_guard_database: dict[str, object], frozen: bool, drift: str | None,
    rewrite_trigger: bool = False,
    concurrent: bool = False,
    replay_drift: str | None = None,
) -> None:
    database = capacity_guard_database
    registration = await _initialize_guard(database)
    team_id, task_id = _seed_trial_inputs(database)
    submission = _atomic_submission(
        registration, team_id=team_id, task_id=task_id,
        idempotency_key=f"original-batch-trial-{uuid4().hex}",
    ).model_copy(update={"batch_id": uuid4(), "sample_idx": 2, "combination_idx": 3})
    caps = {
        "backend": "docker", "os": "linux", "cpu_arch": "any",
        "gpu_vendor": "none", "network_policies": ["public"],
        "terminus2_model_switch": False,
    }
    submitted_at = datetime.now(UTC) - timedelta(days=8 if drift == "expired" else 2)
    lifecycle_id = uuid4()
    admin = create_engine(_value(database, "admin_url"))
    snapshot = text("SELECT to_jsonb(trial) FROM public.trials AS trial WHERE id=:trial")
    parameters = {"trial": submission.trial_id}
    try:
        with admin.begin() as connection:
            scope = connection.execute(text(
                "SELECT lifecycle_environment, lifecycle_namespace "
                "FROM loom_capacity_guard.authority_state WHERE singleton_id=1"
            )).one()
            connection.execute(Batch.__table__.insert().values(
                id=submission.batch_id, team_id=team_id, name="original queued batch",
                task_filter={}, trial_config=submission.config, state="running",
                created_by_token_prefix="adoption-test",
            ))
            connection.execute(DataLifecycleAuthority.__table__.insert().values(
                id=lifecycle_id, environment=scope[0], namespace=scope[1], team_id=team_id,
                data_class="trial", owner_kind="trial", owner_id=str(submission.trial_id),
                created_at=submitted_at, expires_at=submitted_at + timedelta(days=7),
                pinned=False, state="deleting" if drift == "deleting" else "active",
            ))
            connection.execute(Trial.__table__.insert().values(
                id=submission.trial_id, team_id=team_id, task_id=task_id,
                batch_id=submission.batch_id, config=submission.config, requires_caps=caps,
                state="cancelled" if drift == "cancelled" else "queued",
                attempt_count=1 if drift == "attempt" else 0,
                submit_priority=submission.submit_priority,
                idempotency_key=submission.idempotency_key, sample_idx=submission.sample_idx,
                combination_idx=submission.combination_idx, submitted_at=submitted_at,
                lifecycle_authority_id=lifecycle_id,
            ))
            before = connection.execute(snapshot, parameters).scalar_one()
            retention_before = connection.execute(text(
                "SELECT to_jsonb(authority) FROM public.data_lifecycle_authorities AS authority "
                "WHERE id=:id"
            ), {"id": lifecycle_id}).scalar_one()

        writer = await _initialize(database, registration=registration)
        if frozen:
            await _freeze(database, writer["writer_incarnation"], uuid4())
        if rewrite_trigger:
            with admin.begin() as connection:
                connection.exec_driver_sql("""
                    CREATE FUNCTION public.adoption_rewrite_test() RETURNS trigger
                    LANGUAGE plpgsql AS $$ BEGIN NEW.config='{"changed":true}'::jsonb; RETURN NEW; END $$;
                    CREATE TRIGGER adoption_rewrite_test BEFORE UPDATE ON public.trials
                    FOR EACH ROW EXECUTE FUNCTION public.adoption_rewrite_test();
                """)
        engine = create_async_engine(_value(database, "agent_url" if drift == "wrong-role" else "runtime_url"), isolation_level="SERIALIZABLE")
        operation_id = uuid4()
        try:
            store = ProtectedWorkerSessionStore(async_sessionmaker(engine, expire_on_commit=False))
            request = dict(
                registration=registration, submission=submission, public_requires_caps=caps,
                expected_submitted_at=submitted_at, expected_lifecycle_authority_id=lifecycle_id,
                operation_id=operation_id,
            )
            if drift == "team":
                request["submission"] = submission.model_copy(update={"team_id": uuid4()})
            elif drift == "config":
                request["submission"] = submission.model_copy(update={"config": {"agent_name": "changed"}})
            elif drift == "timestamp":
                request["expected_submitted_at"] = submitted_at + timedelta(seconds=1)
            elif drift == "lifecycle":
                request["expected_lifecycle_authority_id"] = uuid4()
            if drift is not None or rewrite_trigger:
                with pytest.raises(ProtectedTrialSubmissionError):
                    if drift == "ordinary-submit":
                        await store.submit_trial(registration=registration, submission=submission, public_requires_caps=caps)
                    else:
                        await store.adopt_legacy_trial(**request)
                with admin.connect() as connection:
                    assert connection.execute(snapshot, parameters).scalar_one() == before
                    for relation in ("trial_adoptions", "atomic_trial_submissions", "protected_runtime_trial_submissions", "trial_attempts", "trial_mutation_permits"):
                        assert connection.execute(text(
                            f"SELECT count(*) FROM loom_capacity_guard.{relation} WHERE trial_id=:trial"
                        ), parameters).scalar_one() == 0
                return
            if concurrent:
                results = await asyncio.gather(
                    store.adopt_legacy_trial(**request), store.adopt_legacy_trial(**request),
                    return_exceptions=True,
                )
                receipts = []
                for result in results:
                    if isinstance(result, Exception):
                        assert isinstance(result, ProtectedTrialSubmissionError)
                        assert result.__cause__.orig.sqlstate in {"40001", "55P03"}
                    else:
                        receipts.append(result)
                assert receipts
                initial_receipts = [value for value in receipts if not value.replayed]
                assert len(initial_receipts) == 1
                receipt = initial_receipts[0]
            else:
                receipt = await store.adopt_legacy_trial(**request)
            if replay_drift is not None:
                def corrupt_requirements(conn, cursor, statement, parameters, context, executemany):
                    if "adopt_protected_runtime_trial_projection" in statement:
                        parameters = dict(parameters)
                        parameters[replay_drift] = (
                            b"{}" if replay_drift == "requirements_payload" else "0" * 64
                        )
                    return statement, parameters

                event.listen(engine.sync_engine, "before_cursor_execute", corrupt_requirements, retval=True)
                try:
                    with pytest.raises(ProtectedTrialSubmissionError) as rejected:
                        await store.adopt_legacy_trial(**request)
                    assert rejected.value.__cause__.orig.sqlstate == "22023"
                finally:
                    event.remove(engine.sync_engine, "before_cursor_execute", corrupt_requirements)
            replay = await store.adopt_legacy_trial(**request)
            with pytest.raises(ProtectedTrialSubmissionError):
                await store.adopt_legacy_trial(**(request | {"operation_id": uuid4()}))
        finally:
            await engine.dispose()
        assert receipt.trial_id == replay.trial_id == submission.trial_id
        assert receipt.protected_attempt_id == replay.protected_attempt_id == submission.protected_attempt_id
        assert receipt.lifecycle_authority_id == replay.lifecycle_authority_id == lifecycle_id
        assert receipt.submitted_at == replay.submitted_at == submitted_at
        assert receipt.replayed is False and replay.replayed is True
        assert receipt.executable is replay.executable is False
        with admin.connect() as connection:
            assert connection.execute(snapshot, parameters).scalar_one() == before | {"state": "protected-pending"}
            assert connection.execute(text(
                "SELECT to_jsonb(authority) FROM public.data_lifecycle_authorities AS authority "
                "WHERE id=:id"
            ), {"id": lifecycle_id}).scalar_one() == retention_before
            for relation in ("atomic_trial_submissions", "protected_runtime_trial_submissions", "trial_attempts"):
                assert connection.execute(text(
                    f"SELECT count(*) FROM loom_capacity_guard.{relation} WHERE trial_id=:trial"
                ), parameters).scalar_one() == 1
            assert connection.execute(text(
                "SELECT count(*) FROM loom_capacity_guard.protected_runtime_trial_readiness WHERE trial_id=:trial"
            ), parameters).scalar_one() == 0
            permissions = connection.execute(text(
                "SELECT operation,state,worker_id,worker_incarnation,claim_operation_id,adoption_operation_id "
                "FROM loom_capacity_guard.trial_mutation_permits WHERE trial_id=:trial"
            ), parameters).all()
            assert permissions == ([("adopt", "consumed", None, None, None, operation_id)] if frozen else [])
    finally:
        admin.dispose()


@pytest.mark.asyncio
async def test_frozen_adoption_rejects_trigger_rewrite_and_rolls_back_every_projection(
    capacity_guard_database: dict[str, object],
) -> None:
    await test_adoption_preserves_original_batch_trial_and_retention(
        capacity_guard_database, frozen=True, drift=None, rewrite_trigger=True,
    )


@pytest.mark.asyncio
async def test_concurrent_frozen_adoption_has_one_origin_and_replayable_receipt(
    capacity_guard_database: dict[str, object],
) -> None:
    await test_adoption_preserves_original_batch_trial_and_retention(
        capacity_guard_database, frozen=True, drift=None, concurrent=True,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("argument", ["requirements_payload", "requirements_digest"])
async def test_frozen_adoption_replay_rejects_changed_requirements_arguments(
    capacity_guard_database: dict[str, object], argument: str,
) -> None:
    await test_adoption_preserves_original_batch_trial_and_retention(
        capacity_guard_database, frozen=True, drift=None, replay_drift=argument,
    )
