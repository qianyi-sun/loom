"""Actual Kubernetes version conflicts are distinct from changed workload authority."""

import json
from datetime import timedelta

import pytest

from loom.dev_instance_runtime import (
    DevInstanceRuntimeError,
    KubectlClient,
    WorkloadStatusConflictError,
)
from loom.personal_dev_storage_workload_write import write_storage_workload
from tests.integration.test_personal_dev_storage_namespace import (
    disposable_storage_kubectl,  # noqa: F401
)
from tests.integration.test_personal_dev_storage_workload_write import _namespace, _workload
from tests.unit.test_personal_dev_storage_runtime_identity import _bound_claim


@pytest.mark.parametrize("phase", ("dry_run", "activation"))
@pytest.mark.parametrize("mutation", ("status", "spec", "denial_status"))
async def test_real_workload_conflict_classification(disposable_storage_kubectl, phase, mutation):  # noqa: F811
    kubectl = disposable_storage_kubectl
    identity = _bound_claim().operation.storage_binding.identity
    await _namespace(kubectl, identity)
    document = _workload(identity)
    intercepted = 0

    class RacingWriter:
        async def run(self, argv, *, stdin=None, timeout_seconds=120):
            nonlocal intercepted
            if "replace" in argv and ("--dry-run=server" in argv) == (phase == "dry_run"):
                intercepted += 1
                assert intercepted == 1, "failed CAS must not be retried internally"
                patch = {"spec": {"replicas": 2}} if mutation == "spec" else {"status": {"observedGeneration": 99}}
                await kubectl.runner.run(kubectl._argv(
                    "patch", "deployment", document["metadata"]["name"], "-n", identity.namespace,
                    "--type=merge", *([] if mutation == "spec" else ["--subresource=status"]),
                    "-p", json.dumps(patch),
                ))
                if mutation == "denial_status":
                    argv = [argv[0], "--as=system:serviceaccount:default:no-workload-update", *argv[1:]]
            return await kubectl.runner.run(argv, stdin=stdin, timeout_seconds=timeout_seconds)

    with pytest.raises(DevInstanceRuntimeError) as raised:
        await write_storage_workload(KubectlClient("kubectl", runner=RacingWriter()), identity, document, operation_epoch=1)
    assert isinstance(raised.value, WorkloadStatusConflictError) is (mutation == "status")
    assert intercepted == 1
    observed = await kubectl.read_resource_json(namespace=identity.namespace, kind="deployment", name=document["metadata"]["name"])
    assert observed["spec"]["replicas"] == (2 if mutation == "spec" else 0)


@pytest.mark.parametrize("revoked", (False, True))
async def test_durable_reclaim_reloads_access_after_status_conflict(isolated_migration_postgres_url, revoked):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from loom.db.schema import DevLifecycleOperation
    from loom.personal_dev_environment_store import (
        PersonalDevEnvironmentOperationFencedError,
        SqlAlchemyPersonalDevEnvironmentAuthority,
    )
    from loom.personal_dev_reconciler import PersonalDevEnvironmentReconciler
    from tests.integration.test_personal_dev_incarnation_storage import _candidate
    from tests.unit.test_personal_dev_reconciler import _NOW, _Installer, _observation, _Projector

    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    claims, loaded, bootstrapped = [], [], []

    class Executor:
        async def prepare(self, claim, *, access):
            claims.append(claim)
            assert access == len(loaded)
            if len(claims) == 1:
                raise WorkloadStatusConflictError("fixture")
            return _observation()

        async def bootstrap_access(self, claim, *, access):
            bootstrapped.append((claim.attempt.id, access))

    async def load_access(claim):
        loaded.append(claim.attempt.id)
        if len(loaded) > 1 and revoked:
            raise RuntimeError("owner access revoked")
        return len(loaded)

    async def reconcile(owner, now):
        async with sessions() as session:
            return await PersonalDevEnvironmentReconciler(
                authority=SqlAlchemyPersonalDevEnvironmentAuthority(session), executor=Executor(),
                capacity_installer=_Installer(), capacity_projector=_Projector(), access_loader=load_access,
                reconciler_id=owner, lease_seconds=60,
            ).reconcile_once(now=now)

    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            created = await SqlAlchemyPersonalDevEnvironmentAuthority(
                session, storage_layout="incarnation-v1",
            ).apply(request, access_binding=access, now=_NOW)
        assert await reconcile("worker-a", _NOW)
        async with sessions() as session:
            operation = await session.get(DevLifecycleOperation, created.operation.id)
            assert operation.state == "running" and operation.failure_reason is None
        assert not await reconcile("worker-b", _NOW + timedelta(seconds=30))
        assert len(claims) == len(loaded) == 1
        assert await reconcile("worker-b", _NOW + timedelta(seconds=61))
        async with sessions() as session:
            operation = await session.get(DevLifecycleOperation, created.operation.id)
            assert operation.state == ("failed" if revoked else "activating")
            if not revoked:
                assert operation.failure_reason is None
                assert len(claims) == 2 and len(loaded) == 3
                assert claims[1].attempt.id == claims[0].attempt.id
                assert claims[1].attempt.lease_epoch > claims[0].attempt.lease_epoch
                assert bootstrapped == [(claims[1].attempt.id, 3)]
            else:
                assert len(claims) == 1 and len(loaded) == 2 and not bootstrapped
        async with sessions() as session:
            old = claims[0]
            with pytest.raises(PersonalDevEnvironmentOperationFencedError):
                await SqlAlchemyPersonalDevEnvironmentAuthority(session).heartbeat_reconciliation(
                    operation_id=old.operation.id, operation_epoch=old.operation.operation_epoch,
                    attempt_id=old.attempt.id, reconciler_id="worker-a", lease_epoch=old.attempt.lease_epoch,
                    lease_seconds=60, now=_NOW + timedelta(seconds=62),
                )
    finally:
        await engine.dispose()
