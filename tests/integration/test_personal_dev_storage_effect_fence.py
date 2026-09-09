"""A stale provisioner must not restore retired storage credentials."""

import asyncio
import io

import psycopg
import pytest
from minio import Minio
from minio.error import S3Error
from sqlalchemy.engine import make_url
from testcontainers.postgres import PostgresContainer

from loom.dev_instance_provision import provisioning_plan_for_identity
from loom.dev_instance_runtime import (
    DevInstanceRuntimeError,
    KubectlClient,
    KubectlMinioTenantProvisioner,
    PsycopgSharedFixtureSqlExecutor,
)
from loom.personal_dev_capacity_runtime import PsycopgPersonalDevCapacityDatabase
from tests.integration.test_personal_dev_storage_minio import (
    _MinioRunner,
    pinned_minio,  # noqa: F401
)
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim
from tests.unit.test_personal_dev_storage_vault import _PASSWORD, _Cluster, _vault


async def test_stale_primary_database_provisioning_cannot_reopen_sealed_incarnation():
    identity = _bound_claim().operation.storage_binding.identity
    with PostgresContainer("postgres:16") as postgres:
        admin_url = postgres.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")
        executor = PsycopgSharedFixtureSqlExecutor(admin_url)
        plan = provisioning_plan_for_identity(identity, _PASSWORD)

        async def provision():
            await executor.apply_role_and_database(
                identity, role_sql=plan["role_sql"], create_database_sql=plan["create_database_sql"],
            )

        await provision()
        url = make_url(admin_url).set(database=identity.database, username=identity.db_role, password=_PASSWORD)
        async with await psycopg.AsyncConnection.connect(url.render_as_string(hide_password=False)) as connection:
            assert (await (await connection.execute("SELECT current_user")).fetchone())[0] == identity.db_role
        await PsycopgPersonalDevCapacityDatabase(admin_url).seal(identity)
        with pytest.raises(psycopg.OperationalError):
            async with await psycopg.AsyncConnection.connect(url.render_as_string(hide_password=False)):
                pass
        # The original immutable plan remains in a delayed caller's memory.
        # Its replay may be rejected or a safe no-op, but cannot restore LOGIN.
        try:
            await provision()
        except DevInstanceRuntimeError:
            pass
        with pytest.raises(psycopg.OperationalError):
            async with await psycopg.AsyncConnection.connect(url.render_as_string(hide_password=False)):
                pass


async def test_already_started_minio_converge_cannot_restore_deleted_tenant(pinned_minio):  # noqa: F811
    server, client_image = pinned_minio
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(_Cluster())
    await vault.store(identity, _PASSWORD)
    runner = _MinioRunner(server, client_image)
    provisioner = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=runner), vault)
    admin = server.get_client()
    admin.make_bucket(identity.task_bucket)
    await provisioner.converge(identity)
    access, secret = await vault.object_credentials(identity)
    client = Minio(server.get_config()["endpoint"], access_key=access, secret_key=secret, secure=False)
    client.put_object(identity.task_bucket, "retained", io.BytesIO(b"owned"), 5)
    paused, resume = asyncio.Event(), asyncio.Event()

    class DelayedExecution:
        async def run(self, argv, *, stdin=None, timeout_seconds=60):
            paused.set()
            await resume.wait()
            return await runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    stale = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=DelayedExecution()), vault)
    task = asyncio.create_task(stale.converge(identity))
    try:
        async with asyncio.timeout(60):
            await paused.wait()
            await provisioner.delete(identity)
            with pytest.raises(S3Error):
                client.put_object(identity.task_bucket, "after-delete", io.BytesIO(b"bad"), 3)
            resume.set()
            outcome = (await asyncio.gather(task, return_exceptions=True))[0]
            assert outcome is None or isinstance(outcome, DevInstanceRuntimeError)
            with pytest.raises(S3Error):
                client.put_object(identity.task_bucket, "after-stale-converge", io.BytesIO(b"bad"), 3)
            assert admin.stat_object(identity.task_bucket, "retained").size == 5
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
