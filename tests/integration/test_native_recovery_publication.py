"""Recovery history is committed by existing admission, never inferred locally."""

from datetime import UTC, datetime, timedelta
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_agent.build_admission import BuildOutcomeRequestV1
from loom_capacity_agent.native_recovery import NativeInstalledAttemptV1, NativeInstalledAttemptV2, NativeRecoveryPreparationV1
from loom_capacity_executor.native_recovery_observation import NativeRecoveryHostIdentityV1
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import owner_sessions as owner_sessions
from tests.integration.test_personal_dev_build_guard_migrations import build_guard_database as build_guard_database
from tests.integration.test_personal_dev_build_guard_prepare import prepared_input as prepared_input
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL
from tests.integration.test_personal_dev_native_builder_store import sessions as sessions


async def recovery_input(prepared_input, owner_sessions, monkeypatch, *, admit=True):
    contracts = import_module("loom_capacity_agent.native_recovery_publication")
    policy_store = import_module("loom_capacity_build_guard.native_recovery_store").NativeRecoveryInstallationStore
    factory, engine, installation, proposal, *_ = prepared_input
    binding = proposal.shapes[0].binding
    pool = next(item for item in installation.document.runtime.pools if item.pool_id == binding.pool_id)
    host = NativeRecoveryHostIdentityV1(node_id=binding.node_ids[0], boot_id=uuid4(), original_uid=24850,
        original_gid=24851, cgroup_namespace_device=4, cgroup_namespace_inode=100)
    policy = contracts.NativeRecoveryProfileV1(installation_id=installation.id, pool_id=pool.pool_id,
        launch_profile_sha256=pool.launch_profile_sha256, worker_config_sha256="a" * 64,
        release_manifest_sha256="b" * 64)
    if admit:
        owner_factory, owner = owner_sessions
        async with owner_factory.begin() as session:
            await session.execute(text(f"SET LOCAL ROLE {owner}"))
            assert await policy_store(session, expected_owner_role=owner).retain(policy, hosts=(host,)) == policy
    claim = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        physical = PhysicalJobBindingV2.model_validate(connection.scalar(text(
            "SELECT payload FROM loom_capacity_build_guard.execution_events WHERE kind='bound'")))
    locator = NativeInstalledAttemptV1(physical=physical, worker_id=claim.worker_id,
        worker_incarnation=claim.worker_incarnation, config_sha256=policy.worker_config_sha256,
        release_manifest_sha256=policy.release_manifest_sha256, directory="/scratch/attempt-123", device=8, inode=100)
    prepared = NativeRecoveryPreparationV1(locator=locator, launch_profile_sha256=pool.launch_profile_sha256,
        node_configuration_sha256=canonical_digest(host), node_id=host.node_id, boot_id=host.boot_id,
        original_uid=host.original_uid, original_gid=host.original_gid,
        cgroup_path=f"/system.slice/slurmstepd.scope/job_{physical.slurm_job_id}",
        cgroup_device=29, cgroup_inode=300, cgroup_mount_id=40)
    final = NativeInstalledAttemptV2(preparation=prepared, runtime_spec_sha256="e" * 64,
        uid_map=({"inside": 0, "outside": host.original_uid, "count": 1}, {"inside": 1, "outside": 100000, "count": 65536}),
        gid_map=({"inside": 0, "outside": host.original_gid, "count": 1}, {"inside": 1, "outside": 200000, "count": 65536}))
    return contracts, claim, policy, prepared, final


@pytest.mark.parametrize("boundary", ["exact", "no-policy", "uncommitted", "credential", "config", "boot", "physical", "conflict"])
async def test_recovery_publication_requires_admitted_exact_committed_preparation(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    contracts, claim, _policy, prepared, final = await recovery_input(prepared_input, owner_sessions, monkeypatch,
        admit=boundary != "no-policy")
    factory, engine, installation, *_ = prepared_input
    if boundary == "config":
        prepared = prepared.model_copy(update={"locator": prepared.locator.model_copy(update={"config_sha256": "f" * 64})})
    elif boundary == "boot":
        prepared = prepared.model_copy(update={"boot_id": uuid4()})
    elif boundary == "physical":
        prepared = prepared.model_copy(update={"locator": prepared.locator.model_copy(update={
            "physical": prepared.locator.physical.model_copy(update={"ownership_evidence_sha256": "f" * 64})})})
    request = contracts.NativeRecoveryPublicationV1(claim=claim, record=prepared)
    if boundary not in {"exact", "uncommitted", "conflict"}:
        async with factory.begin() as session:
            with pytest.raises((ValueError, DBAPIError)):
                await store(session, installation).publish_recovery(request,
                    worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_records")) == 0
        return
    async with factory.begin() as session:
        first = await store(session, installation).publish_recovery(request, worker_credential=CREDENTIAL)
        if boundary == "uncommitted":
            with pytest.raises((ValueError, DBAPIError)):
                await store(session, installation).publish_recovery(
                    contracts.NativeRecoveryPublicationV1(claim=claim, record=final), worker_credential=CREDENTIAL)
            return
    async with factory.begin() as session:
        assert await store(session, installation).publish_recovery(request, worker_credential=CREDENTIAL) == first
        if boundary == "conflict":
            changed = contracts.NativeRecoveryPublicationV1(claim=claim, record=prepared.model_copy(update={"cgroup_inode": 301}))
            with pytest.raises((ValueError, DBAPIError)):
                await store(session, installation).publish_recovery(changed, worker_credential=CREDENTIAL)
            return
        second = await store(session, installation).publish_recovery(
            contracts.NativeRecoveryPublicationV1(claim=claim, record=final), worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        history = await store(session, installation).read_recovery(claim, worker_credential=CREDENTIAL)
    assert history.preparation == first and history.finalization == second
    assert first.request == request and first.request_digest == canonical_digest(request)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_records")) == 2
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.request_holds")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.worker_releases")) == 0


@pytest.mark.parametrize("boundary", ["expired", "outcome", "rollback"])
async def test_recovery_readback_survives_closed_execution_but_never_allows_new_publication(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    contracts, claim, _policy, prepared, final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    factory, engine, installation, _plan, source, _platform = prepared_input
    request = contracts.NativeRecoveryPublicationV1(claim=claim, record=prepared)
    async with factory() as session:
        await session.begin()
        first = await store(session, installation).publish_recovery(request, worker_credential=CREDENTIAL)
        if boundary == "rollback":
            await session.rollback()
        else:
            await session.commit()
    if boundary == "expired":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=:deadline WHERE id=:id"),
                {"id": source.build_attempt.id, "deadline": datetime.now(UTC) - timedelta(seconds=1)})
    elif boundary == "outcome":
        async with factory.begin() as session:
            await store(session, installation).record_outcome(BuildOutcomeRequestV1(claim=claim,
                operation_id=uuid4(), result="failed"), worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        history = await store(session, installation).read_recovery(claim, worker_credential=CREDENTIAL)
        assert history.preparation == (None if boundary == "rollback" else first)
        assert history.finalization is None
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).read_recovery(claim, worker_credential="x" * 43)
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).publish_recovery(
                contracts.NativeRecoveryPublicationV1(claim=claim, record=final), worker_credential=CREDENTIAL)
