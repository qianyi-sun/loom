"""A stale provisioner must not restore retired storage credentials."""

import asyncio
import io
import json

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
from loom.personal_dev_minio_retirement import _deny_name, _execute, _lookup
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


@pytest.mark.parametrize("initial_tenant", (True, False))
async def test_already_started_minio_converge_cannot_restore_deleted_tenant(pinned_minio, initial_tenant):  # noqa: F811
    server, client_container = pinned_minio
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(_Cluster())
    await vault.store(identity, _PASSWORD)
    runner = _MinioRunner(server, client_container)
    provisioner = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=runner), vault)
    admin = server.get_client()
    if initial_tenant:
        admin.make_bucket(identity.task_bucket)
        await provisioner.converge(identity)
    access, secret = await vault.object_credentials(identity)
    client = Minio(server.get_config()["endpoint"], access_key=access, secret_key=secret, secure=False)
    if initial_tenant:
        client.put_object(identity.task_bucket, "retained", io.BytesIO(b"owned"), 5)
    paused, resume = asyncio.Event(), asyncio.Event()

    class DelayedExecution:
        async def run(self, argv, *, stdin=None, timeout_seconds=60):
            if "mc admin user add" in argv[-1]:
                paused.set()
                await resume.wait()
            return await runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    stale = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=DelayedExecution()), vault)
    task = asyncio.create_task(stale.converge(identity))
    try:
        async with asyncio.timeout(60):
            await paused.wait()
            await provisioner.delete(identity)
            if not initial_tenant:
                # Retirement must also work before any tenant or bucket exists.
                admin.make_bucket(identity.task_bucket)
                admin.put_object(identity.task_bucket, "retained", io.BytesIO(b"owned"), 5)
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


async def test_retained_deny_policy_prevents_adopting_a_deleted_minio_user(pinned_minio):  # noqa: F811
    server, client_container = pinned_minio
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(_Cluster())
    await vault.store(identity, _PASSWORD)
    runner = _MinioRunner(server, client_container)
    kubectl = KubectlClient("kubectl", runner=runner)
    provisioner = KubectlMinioTenantProvisioner(kubectl, vault)
    admin = server.get_client()
    admin.make_bucket(identity.task_bucket)
    await provisioner.converge(identity)
    await provisioner.delete(identity)
    access, _ = await vault.object_credentials(identity)
    # Independently remove only the retired principal, not its permanent
    # policy record. A later normal provisioner must not adopt that absence.
    await kubectl.exec_stdin(namespace="loom-dev", pod="loom-dev-minio-0", container="admin", script=(
        'export MC_HOST_fixture="http://${MINIO_ROOT_USER}:${MINIO_ROOT_PASSWORD}@127.0.0.1:9000"\n'
        f"mc admin user rm fixture {access} >/dev/null"
    ))
    with pytest.raises(DevInstanceRuntimeError):
        await provisioner.converge(identity)


@pytest.mark.parametrize("mutation", ("policy create", "user add", "policy attach"))
async def test_minio_retirement_recovers_after_lost_mutation_reply(pinned_minio, mutation):  # noqa: F811
    server, client_container = pinned_minio
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(_Cluster())
    await vault.store(identity, _PASSWORD)
    runner = _MinioRunner(server, client_container)
    provisioner = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=runner), vault)
    admin = server.get_client()
    admin.make_bucket(identity.task_bucket)
    await provisioner.converge(identity)
    access, secret = await vault.object_credentials(identity)
    client = Minio(server.get_config()["endpoint"], access_key=access, secret_key=secret, secure=False)
    client.put_object(identity.task_bucket, "retained", io.BytesIO(b"owned"), 5)

    class LostReply:
        lost = False

        async def run(self, argv, *, stdin=None, timeout_seconds=60):
            result = await runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)
            if not self.lost and f"mc admin {mutation}" in argv[-1]:
                self.lost = True
                raise DevInstanceRuntimeError("injected lost mutation reply")
            return result

    transport = LostReply()
    interrupted = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=transport), vault)
    try:
        await interrupted.delete(identity)
    except DevInstanceRuntimeError as exc:
        assert str(exc) == "injected lost mutation reply"
    assert transport.lost
    # A recorded decision must reject stale convergence even when Deny has
    # not yet been attached. Retirement is not complete until a retry verifies it.
    with pytest.raises(DevInstanceRuntimeError, match="permanently retired"):
        await provisioner.converge(identity)
    await provisioner.delete(identity)
    await provisioner.delete(identity)
    with pytest.raises(S3Error):
        client.put_object(identity.task_bucket, "after-retirement", io.BytesIO(b"bad"), 3)
    assert admin.stat_object(identity.task_bucket, "retained").size == 5


async def test_incomplete_minio_retirement_rejects_already_started_convergence(pinned_minio):  # noqa: F811
    server, client_container = pinned_minio
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(_Cluster())
    await vault.store(identity, _PASSWORD)
    runner = _MinioRunner(server, client_container)
    provisioner = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=runner), vault)
    server.get_client().make_bucket(identity.task_bucket)
    await provisioner.converge(identity)
    paused, resume = asyncio.Event(), asyncio.Event()

    class DelayedConvergence:
        async def run(self, argv, *, stdin=None, timeout_seconds=60):
            if "mc admin user add" in argv[-1]:
                paused.set()
                await resume.wait()
            return await runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    class InterruptedRetirement:
        async def run(self, argv, *, stdin=None, timeout_seconds=60):
            if "mc admin user add" in argv[-1]:
                raise DevInstanceRuntimeError("interrupted before credential mutation")
            return await runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    stale = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=DelayedConvergence()), vault)
    retiring = KubectlMinioTenantProvisioner(KubectlClient("kubectl", runner=InterruptedRetirement()), vault)
    task = asyncio.create_task(stale.converge(identity))
    try:
        async with asyncio.timeout(45):
            await paused.wait()
            with pytest.raises(DevInstanceRuntimeError, match="interrupted before"):
                await retiring.delete(identity)
            resume.set()
            with pytest.raises(DevInstanceRuntimeError, match="permanently retired"):
                await task
            await provisioner.delete(identity)
    finally:
        resume.set()
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.parametrize("policy_kind", ("allow", "deny"))
async def test_minio_policy_drift_is_rejected_without_overwrite(pinned_minio, policy_kind):  # noqa: F811
    server, client_container = pinned_minio
    identity = _bound_claim().operation.storage_binding.identity
    vault = _vault(_Cluster())
    await vault.store(identity, _PASSWORD)
    provisioner = KubectlMinioTenantProvisioner(
        KubectlClient("kubectl", runner=_MinioRunner(server, client_container)), vault,
    )
    await provisioner.converge(identity)
    name = provisioner._names(identity)[1] if policy_kind == "allow" else _deny_name(identity)
    # Simulate actual administrative drift; neither normal converge nor
    # retirement may silently adopt or overwrite an unexplained policy.
    policy = {"Version": "2012-10-17", "Statement": [{
        "Sid": "foreign", "Effect": "Allow", "Action": ["s3:*"], "Resource": ["*"],
    }]}
    await _execute(provisioner, '\n'.join((
        "umask 077", "policy_file=$(mktemp)",
        "trap 'rm -f -- \"$policy_file\"' EXIT HUP INT TERM",
        'cat >"$policy_file"',
        f'mc admin policy create fixture {name} "$policy_file" >/dev/null',
    )), stdin=json.dumps(policy))
    before = await _lookup(provisioner, "policy", name)
    with pytest.raises(DevInstanceRuntimeError, match="immutable intent"):
        await provisioner.converge(identity)
    if policy_kind == "deny":
        with pytest.raises(DevInstanceRuntimeError, match="immutable intent"):
            await provisioner.delete(identity)
    assert await _lookup(provisioner, "policy", name) == before
