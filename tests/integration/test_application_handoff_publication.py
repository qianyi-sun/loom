"""Publication and terminal guards remain effective after actual ownership handoff."""

from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.application_runtime_login import restore_application_runtime_login
from loom.db.schema import TaskImageRegistryCredentialGeneration
from tests.integration.test_application_ownership_transfer import (
    transfer_database,  # noqa: F401
    transfer_postgres,  # noqa: F401
    transfer_postgres_url,  # noqa: F401
)
from tests.integration.test_application_runtime_login import _prepare
from tests.integration.test_task_bundle_source_journal import (
    _publish,
    _receipts,
    _spec,
    _upload,
)
from tests.support.historical_task_images import _insert, _prepared_insert, _retire

pytestmark = pytest.mark.parametrize("transfer_postgres", [16, 17], indirect=True)


async def test_source_journal_publication_and_immutability_survive_handoff(
    transfer_database, tmp_path,  # noqa: F811
):
    admin, url, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = uuid4().hex
    with admin:
        restore_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target,
        )
        assert admin.execute(
            "SELECT pg_get_userbyid(proowner),prosecdef FROM pg_proc "
            "WHERE oid='public.task_bundle_journal_immutable()'::regprocedure"
        ).fetchone() == (owner, False)
        client_url = make_url(url).set(
            drivername="postgresql+psycopg", username=runtime, password=password,
        )
        engine = create_async_engine(client_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            spec = _spec(tmp_path)
            ticket = await _upload(factory, spec)
            await _receipts(factory, ticket)
            assert await _publish(factory, ticket) == spec
            # Each transaction uses the ordinary restored runtime account.
            # Publication exercises all five transferred journal tables; these
            # rejected mutations prove the transferred invoker guard still runs.
            for statement in (
                "UPDATE public.task_bundle_sources SET spec_json='{}'::jsonb",
                "UPDATE public.task_bundle_source_incarnations "
                "SET expires_at=expires_at+interval '1 hour'",
                "UPDATE public.task_bundle_source_writes SET content_sha256=repeat('e',64)",
                "UPDATE public.task_bundle_source_versions SET version_id='replacement'",
                "DELETE FROM public.task_bundle_sources",
            ):
                async with factory() as session:
                    with pytest.raises(IntegrityError, match=r"journal.*immutable"):
                        await session.execute(text(statement))
                    await session.rollback()
            assert await _publish(factory, ticket) == spec
            with psycopg.connect(url, user=runtime, password=password, autocommit=True) as client:
                for statement in (
                    "ALTER TABLE public.task_bundle_sources DISABLE TRIGGER ALL",
                    "ALTER FUNCTION public.task_bundle_journal_immutable() SECURITY DEFINER",
                ):
                    with pytest.raises(psycopg.errors.InsufficientPrivilege):
                        client.execute(statement)
        finally:
            await engine.dispose()


async def test_credential_retirement_guard_survives_sealed_owner_handoff(
    transfer_database,  # noqa: F811
):
    admin, url, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = uuid4().hex
    with admin:
        restore_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target,
        )
        assert admin.execute(
            "SELECT pg_get_userbyid(proowner),prosecdef FROM pg_proc "
            "WHERE oid='public.task_image_registry_reject_retired_attempt()'::regprocedure"
        ).fetchone() == (owner, True)
        assert admin.execute(
            "SELECT has_function_privilege(%s, "
            "'public.task_image_registry_reject_retired_attempt()', 'EXECUTE')", (runtime,),
        ).fetchone() == (False,)
        client_url = make_url(url).set(
            drivername="postgresql+psycopg", username=runtime, password=password,
        )
        engine = create_async_engine(client_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            # Restore historical rows as backup setup, then exercise all guards
            # through the restored non-owner LOGIN.
            fixture_engine = create_async_engine(make_url(url).set(drivername="postgresql+psycopg"))
            try:
                attempt_id, values = await _prepared_insert(
                    async_sessionmaker(fixture_engine, expire_on_commit=False)
                )
            finally:
                await fixture_engine.dispose()
            async with factory() as session:
                await _insert(session, values)
                assert await session.scalar(
                    select(func.count()).select_from(TaskImageRegistryCredentialGeneration)
                ) == 1
                await session.rollback()
            await _retire(factory, attempt_id)
            async with factory() as session:
                with pytest.raises(IntegrityError) as rejected:
                    await _insert(session, values)
                assert rejected.value.orig.diag.constraint_name == "task_image_registry_credentials_not_retired"
                await session.rollback()
                assert await session.scalar(
                    select(func.count()).select_from(TaskImageRegistryCredentialGeneration)
                ) == 0
        finally:
            await engine.dispose()


async def test_terminal_trial_guard_survives_sealed_owner_handoff(transfer_database):  # noqa: F811
    admin, url, owner, runtime, bindings, target = await _prepare(transfer_database)
    password = uuid4().hex
    with admin:
        restore_application_runtime_login(
            admin, owner_role=owner, role_bindings=bindings, password=password, target=target,
        )
        with psycopg.connect(url, user=runtime, password=password, autocommit=True) as client:
            team, trial, worker = uuid4(), uuid4(), uuid4()
            task = "handoff-terminal/" + trial.hex
            client.execute("INSERT INTO public.teams(id,name) VALUES (%s,'handoff')", (team,))
            client.execute(
                "INSERT INTO public.tasks(id,checksum,config) VALUES (%s,repeat('a',64),'{}')",
                (task,),
            )
            client.execute(
                "INSERT INTO public.workers(id,hostname,version,capabilities,registered_at,last_seen_at,status) "
                "VALUES (%s,'disposable-handoff','test','[]',now(),now(),'ready')", (worker,),
            )
            client.execute(
                "INSERT INTO public.trials(id,team_id,task_id,config,requires_caps,state,worker_id,attempt_count) "
                "VALUES (%s,%s,%s,'{}','{}','running',%s,1)", (trial, team, task, worker),
            )
            # The ordinary terminal update still traverses the transferred
            # capacity helper; terminal-to-queued must remain forbidden afterward.
            client.execute("UPDATE public.trials SET state='failed' WHERE id=%s", (trial,))
            with pytest.raises(psycopg.errors.CheckViolation) as rejected:
                client.execute("UPDATE public.trials SET state='queued' WHERE id=%s", (trial,))
            assert rejected.value.diag.constraint_name == "trials_terminal_state_monotonic"
            assert client.execute(
                "SELECT state FROM public.trials WHERE id=%s", (trial,),
            ).fetchone() == ("failed",)
            for statement in (
                "ALTER TABLE public.trials DISABLE TRIGGER ALL",
                "ALTER FUNCTION public.trials_reject_terminal_reopening() SECURITY DEFINER",
                sql.SQL("SET ROLE {}").format(sql.Identifier(owner)),
            ):
                with pytest.raises(psycopg.errors.InsufficientPrivilege):
                    client.execute(statement)
