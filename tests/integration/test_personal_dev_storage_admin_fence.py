"""The administrative backend retains retirement across races and cleanup."""

import asyncio

import psycopg
import pytest
from psycopg import sql
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom.dev_instance_provision import provisioning_plan_for_identity
from loom.dev_instance_runtime import DevInstanceRuntimeError, PsycopgSharedFixtureSqlExecutor
from loom.personal_dev_capacity_runtime import (
    PersonalDevCapacityInstallationError,
    PsycopgPersonalDevCapacityDatabase,
)
from loom.personal_dev_storage_admin_fence import (
    PersonalDevStorageRetiredError,
    storage_admin_connection,
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
from tests.unit.test_personal_dev_storage_vault import _PASSWORD


@pytest.fixture
def admin_url():
    with PostgresContainer("postgres:16") as postgres:
        yield postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


async def _provision(url, identity):
    plan = provisioning_plan_for_identity(identity, _PASSWORD)
    await PsycopgSharedFixtureSqlExecutor(url).apply_role_and_database(
        identity, role_sql=plan["role_sql"], create_database_sql=plan["create_database_sql"],
    )


@pytest.mark.parametrize("cleanup", ("seal", "destroy", "fixture"))
async def test_retirement_before_first_provision_is_permanent(admin_url, cleanup):
    identity = _bound_claim().operation.storage_binding.identity
    if cleanup == "fixture":
        await PsycopgSharedFixtureSqlExecutor(admin_url).drop_database_and_role(identity)
    else:
        await getattr(PsycopgPersonalDevCapacityDatabase(admin_url), cleanup)(identity)
    with pytest.raises(DevInstanceRuntimeError):
        await _provision(admin_url, identity)
    async with await psycopg.AsyncConnection.connect(admin_url) as connection:
        assert await (await connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (identity.database,))).fetchone() is None


async def test_cleanup_drops_runtime_resources_but_retains_retirement_guard(admin_url):
    identity = _bound_claim().operation.storage_binding.identity
    await _provision(admin_url, identity)
    await PsycopgPersonalDevCapacityDatabase(admin_url).destroy(identity)
    await PsycopgPersonalDevCapacityDatabase(admin_url).destroy(identity)
    with pytest.raises(DevInstanceRuntimeError):
        await _provision(admin_url, identity)
    async with await psycopg.AsyncConnection.connect(admin_url) as connection:
        result = await connection.execute("SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)",
            ([identity.db_role, f"ld_fence_{identity.storage_incarnation.hex}"],))
        assert await result.fetchall() == [(f"ld_fence_{identity.storage_incarnation.hex}",)]
        assert await (await connection.execute("SELECT 1 FROM pg_database WHERE datname = %s", (identity.database,))).fetchone() is None


async def test_different_admin_url_databases_share_the_executing_backend_fence(admin_url, monkeypatch):
    identity = _bound_claim().operation.storage_binding.identity
    plan = provisioning_plan_for_identity(identity, _PASSWORD)
    original = psycopg.AsyncConnection.execute
    paused, resume = asyncio.Event(), asyncio.Event()
    backend_pid = None

    async def delayed(self, query, *args, **kwargs):
        nonlocal backend_pid
        if query == plan["role_sql"]:
            backend_pid = self.info.backend_pid
            paused.set()
            await resume.wait()
        return await original(self, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.AsyncConnection, "execute", delayed)
    # This nonexistent URL database must be replaced by canonical postgres.
    alternate = make_url(admin_url).set(database="unrelated_admin_database").render_as_string(hide_password=False)
    writer = asyncio.create_task(_provision(alternate, identity))
    tasks = [writer]
    try:
        async with asyncio.timeout(30):
            await paused.wait()
            retire = asyncio.create_task(PsycopgPersonalDevCapacityDatabase(admin_url).seal(identity))
            tasks.append(retire)
            async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as observer:
                while True:
                    waiting = await observer.execute("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted)")
                    if (await waiting.fetchone())[0]:
                        break
                    await asyncio.sleep(0.01)
                executing = await observer.execute("SELECT datname FROM pg_stat_activity WHERE pid = %s", (backend_pid,))
                assert await executing.fetchone() == ("postgres",)
                assert not retire.done()
            resume.set()
            await writer
            await retire
        with pytest.raises(DevInstanceRuntimeError):
            await _provision(admin_url, identity)
    finally:
        resume.set()
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.parametrize("drift", ("comment", "login", "password", "membership", "setting"))
async def test_guard_drift_is_never_reset_or_adopted(admin_url, drift):
    identity = _bound_claim().operation.storage_binding.identity
    async with storage_admin_connection(admin_url, identity, action="provision"):
        pass
    guard = sql.Identifier(f"ld_fence_{identity.storage_incarnation.hex}")
    async with await psycopg.AsyncConnection.connect(admin_url, autocommit=True) as admin:
        if drift == "comment":
            await admin.execute(sql.SQL("COMMENT ON ROLE {} IS 'foreign'").format(guard))
        elif drift == "login":
            await admin.execute(sql.SQL("ALTER ROLE {} LOGIN").format(guard))
        elif drift == "password":
            await admin.execute(sql.SQL("ALTER ROLE {} PASSWORD 'nonsecret-fixture'").format(guard))
        elif drift == "setting":
            await admin.execute(sql.SQL("ALTER ROLE {} SET search_path = public").format(guard))
        else:
            await admin.execute("CREATE ROLE probe_member")
            await admin.execute(sql.SQL("GRANT {} TO probe_member").format(guard))
    with pytest.raises(PersonalDevStorageRetiredError, match="authentic"):
        async with storage_admin_connection(admin_url, identity, action="retire"):
            pytest.fail("drift must not authorize administrative work")
    with pytest.raises(DevInstanceRuntimeError):
        await _provision(admin_url, identity)


async def test_interrupted_retirement_commits_guard_before_revocation(admin_url, monkeypatch):
    identity = _bound_claim().operation.storage_binding.identity
    await _provision(admin_url, identity)
    original = psycopg.AsyncConnection.execute

    async def interrupted(self, query, *args, **kwargs):
        if query == "SELECT rolname FROM pg_roles WHERE rolname = ANY(%s)":
            raise RuntimeError("interrupted after committing retirement")
        return await original(self, query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(psycopg.AsyncConnection, "execute", interrupted)
        with pytest.raises(PersonalDevCapacityInstallationError):
            await PsycopgPersonalDevCapacityDatabase(admin_url).seal(identity)
    with pytest.raises(DevInstanceRuntimeError):
        await _provision(admin_url, identity)
    # Repeating retirement must finish the revocation work after that failure.
    await PsycopgPersonalDevCapacityDatabase(admin_url).seal(identity)
    async with await psycopg.AsyncConnection.connect(admin_url) as admin:
        result = await admin.execute("SELECT rolcanlogin FROM pg_roles WHERE rolname = %s", (identity.db_role,))
        assert await result.fetchone() == (False,)


async def test_retirement_cannot_acknowledge_failed_session_termination(admin_url, monkeypatch):
    identity = _bound_claim().operation.storage_binding.identity
    await _provision(admin_url, identity)
    url = make_url(admin_url).set(database=identity.database, username=identity.db_role, password=_PASSWORD)
    connection = await psycopg.AsyncConnection.connect(url.render_as_string(hide_password=False), autocommit=True)
    original = psycopg.AsyncConnection.execute

    async def failed_signal(self, query, *args, **kwargs):
        if isinstance(query, str) and "pg_terminate_backend" in query:
            query = query.replace("pg_terminate_backend(pid, 10000)", "false").replace("pg_terminate_backend(pid)", "false")
        return await original(self, query, *args, **kwargs)

    try:
        with monkeypatch.context() as patch:
            patch.setattr(psycopg.AsyncConnection, "execute", failed_signal)
            with pytest.raises(PersonalDevCapacityInstallationError):
                await PsycopgPersonalDevCapacityDatabase(admin_url).seal(identity)
            assert await (await connection.execute("SELECT 1")).fetchone() == (1,)
        await PsycopgPersonalDevCapacityDatabase(admin_url).seal(identity)
        with pytest.raises(psycopg.Error):
            await connection.execute("SELECT 1")
    finally:
        await connection.close()
