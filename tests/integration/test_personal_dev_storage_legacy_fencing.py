"""Legacy reservations must not take authority over incarnation-bound records."""

from dataclasses import replace
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.dev_instance_provisioner import DevInstanceConflictError
from loom.dev_instance_store import SqlAlchemyDevInstanceStore
from loom.personal_dev_environment_store import SqlAlchemyPersonalDevEnvironmentAuthority
from tests.integration.test_personal_dev_incarnation_storage import _candidate
from tests.unit.test_personal_dev_reconciler import _NOW


@pytest.mark.parametrize("action", ("create", "destroy"))
async def test_legacy_reservation_rejects_failed_bound_environment(
    isolated_migration_postgres_url, action,
):
    engine = create_async_engine(isolated_migration_postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    try:
        request, access = await _candidate(sessions)
        async with sessions() as session:
            authority = SqlAlchemyPersonalDevEnvironmentAuthority(
                session, storage_layout="incarnation-v1",
            )
            created = await authority.apply(request, access_binding=access, now=_NOW)
            claim = await authority.claim_next_reconciliation(
                reconciler_id="legacy-fence", now=_NOW, lease_seconds=60,
            )
            await authority.fail_pre_activation(
                operation_id=created.operation.id,
                operation_epoch=created.operation.operation_epoch,
                attempt_id=claim.attempt.id, reconciler_id="legacy-fence",
                lease_epoch=claim.attempt.lease_epoch,
                failure_reason="candidate_build_failed", now=_NOW,
            )
        async with sessions() as session:
            store = SqlAlchemyDevInstanceStore(session)
            before = await store.get(request.name)
            assert before.status == "failed" and before.storage_binding is not None
            with pytest.raises(DevInstanceConflictError, match="personal"):
                if action == "create":
                    await store.claim_create(replace(
                        before, storage_binding=None, operation_id=uuid4(), candidate_sha="b" * 40,
                    ))
                else:
                    await store.claim_destroy(
                        request.name, operation_id=uuid4(), keep_data=False, now=_NOW,
                    )
            # Catching the rejection must leave even a subsequent commit safe.
            await session.commit()
        async with sessions() as session:
            assert await SqlAlchemyDevInstanceStore(session).get(request.name) == before
    finally:
        await engine.dispose()
