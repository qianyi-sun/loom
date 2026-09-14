"""Recovery history is committed by existing admission, never inferred locally."""

from datetime import UTC, datetime, timedelta
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_agent.build_admission import BuildOutcomeRequestV1
from loom_capacity_agent.native_recovery import (
    NativeInstalledAttemptV1,
    NativeInstalledAttemptV2,
    NativeRecoveryPreparationV1,
)
from loom_capacity_executor.native_recovery_observation import NativeRecoveryHostIdentityV1
from loom_capacity_manager.contracts import canonical_digest
from tests.integration.test_personal_dev_build_guard_claims import claim_input
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_installations import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_personal_dev_build_guard_migrations import (
    build_guard_database as build_guard_database,
)
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
            assert await policy_store(session, expected_owner_role=owner).retain_recovery(policy, hosts=(host,)) == policy
    claim = await claim_input(prepared_input, monkeypatch)
    async with factory.begin() as session:
        await store(session, installation).claim_platform(claim, worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        physical = PhysicalJobBindingV2.model_validate_json(bytes(connection.scalar(text(
            "SELECT wire_payload FROM loom_capacity_build_guard.execution_events WHERE kind='bound'"))))
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
        with pytest.raises((ValueError, DBAPIError), match="committed"):
            await store(session, installation).read_recovery(claim, worker_credential=CREDENTIAL)
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


@pytest.mark.parametrize("boundary", ["exact", "disabled", "credential", "controller", "http"])
async def test_recovery_http_acknowledges_only_committed_history(prepared_input, owner_sessions, tmp_path, monkeypatch, boundary):
    import httpx

    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for

    contracts, claim, _policy, prepared, final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    _factory, engine, *_ = prepared_input
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "prepare-bind-only" if boundary == "disabled" else "native-artifacts"
    responses = []

    async def observe(response):
        await response.aread()
        responses.append(response)
        if response.status_code == 200 and response.request.url.path.endswith("/recovery-publish"):
            # A distinct connection can see the commit before HTTP acknowledgment.
            with engine.connect() as connection:
                assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_records")) > 0

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), event_hooks={"response": [observe]}) as http:
        client = client_for(http, claim)
        client._token = "wrong" if boundary == "controller" else "executor-secret"
        if boundary == "http":
            client._origin = "http://management.test"
        request = contracts.NativeRecoveryPublicationV1(claim=claim, record=prepared)
        if boundary != "exact":
            from loom_capacity_executor.build_admission_client import BuildAdmissionTransportError

            with pytest.raises(BuildAdmissionTransportError):
                await client.publish_recovery(request, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
        else:
            first = await client.publish_recovery(request, worker_credential=CREDENTIAL)
            second = await client.publish_recovery(contracts.NativeRecoveryPublicationV1(claim=claim, record=final), worker_credential=CREDENTIAL)
            history = await client.read_recovery(claim, worker_credential=CREDENTIAL)
            assert history.preparation == first and history.finalization == second
            assert all(response.headers["cache-control"] == "no-store" for response in responses)
        assert all(CREDENTIAL not in response.text and "executor-secret" not in response.text for response in responses)


async def test_recovery_required_pool_denies_legacy_permission_before_publication(prepared_input, owner_sessions, monkeypatch):
    from loom_capacity_agent.build_admission import BuildExecutionRequestV1

    _contracts, claim, _policy, _prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    factory, _engine, installation, _proposal, _source, platform = prepared_input
    async with factory.begin() as session:
        with pytest.raises(DBAPIError, match="digest-bound V2"):
            await store(session, installation).authorize_execution(BuildExecutionRequestV1(
                claim=claim, challenge=uuid4(), source_binding_sha256=platform.source_binding_sha256), worker_credential=CREDENTIAL)


@pytest.mark.parametrize("boundary", ["late", "reboot", "static", "agent"])
async def test_recovery_profile_admission_is_pre_execution_and_hosts_append_only(prepared_input, owner_sessions, monkeypatch, boundary):
    contracts, _claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch, admit=boundary != "late")
    from loom_capacity_build_guard.native_recovery_store import NativeRecoveryInstallationStore

    host = contracts.NativeRecoveryHostIdentityV1(node_id=prepared.node_id, boot_id=uuid4(), original_uid=prepared.original_uid,
        original_gid=prepared.original_gid, cgroup_namespace_device=4, cgroup_namespace_inode=101)
    factory, engine, *_ = prepared_input
    owner_factory, owner = owner_sessions
    async with (factory if boundary == "agent" else owner_factory).begin() as session:
        if boundary != "agent":
            await session.execute(text(f"SET LOCAL ROLE {owner}"))
        policy_store = NativeRecoveryInstallationStore(session, expected_owner_role=owner)
        if boundary == "reboot":
            assert await policy_store.retain_recovery(profile, hosts=(host,)) == profile
        else:
            with pytest.raises((ValueError, DBAPIError)):
                await policy_store.retain_recovery(profile.model_copy(update={"worker_config_sha256": "c" * 64})
                    if boundary == "static" else profile, hosts=(host,))
    if boundary == "reboot":
        with engine.connect() as connection:
            assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_hosts")) == 2


@pytest.mark.parametrize("boundary", ["physical-float", "cgroup-suffix", "mapping-float", "mapping-overlap", "unknown", "path"])
async def test_recovery_raw_sql_rejects_records_strict_readers_cannot_recover(prepared_input, owner_sessions, monkeypatch, boundary):
    import hashlib
    import json

    contracts, claim, _profile, prepared, final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    factory, engine, installation, *_ = prepared_input
    if boundary.startswith("mapping"):
        async with factory.begin() as session:
            await store(session, installation).publish_recovery(contracts.NativeRecoveryPublicationV1(claim=claim, record=prepared), worker_credential=CREDENTIAL)
    payload = contracts.NativeRecoveryPublicationV1(claim=claim, record=final if boundary.startswith("mapping") else prepared).model_dump(mode="json")
    record = payload["record"]
    if boundary == "physical-float":
        record["locator"]["physical"]["schema_version"] = 2.0
    elif boundary == "cgroup-suffix":
        record["cgroup_path"] = f"/system.slice/xslurmstepd.scope/job_{prepared.locator.physical.slurm_job_id}"
    elif boundary == "mapping-float":
        record["uid_map"][1]["count"] = 65536.0
    elif boundary == "mapping-overlap":
        record["uid_map"][1]["outside"] = prepared.original_uid
    elif boundary == "unknown":
        record["cleanup_permitted"] = True
    else:
        record["locator"]["directory"] = "/scratch/../foreign"
    wire = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    async with factory.begin() as session:
        with pytest.raises(DBAPIError):
            await session.scalar(text("""SELECT loom_capacity_build_guard.publish_recovery(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:credential)"""),
                {"installation": installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": hashlib.sha256(wire).hexdigest(), "credential": hashlib.sha256(CREDENTIAL.encode("ascii")).hexdigest()})
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_records")) == int(boundary.startswith("mapping"))


@pytest.mark.parametrize("boundary", ["execute", "public", "search-path"])
def test_recovery_publication_acl_drift_fails_migration_readback(build_guard_database, boundary):
    from alembic import command

    config, engine, _owner, agent, _url = build_guard_database
    command.upgrade(config, "head")
    signature = "loom_capacity_build_guard.publish_recovery(uuid,jsonb,bytea,text,text)"
    statement = {
        "execute": f"REVOKE EXECUTE ON FUNCTION {signature} FROM {engine.dialect.identifier_preparer.quote(agent)}",
        "public": f"GRANT EXECUTE ON FUNCTION {signature} TO PUBLIC",
        "search-path": f"ALTER FUNCTION {signature} SET search_path=public",
    }[boundary]
    with engine.begin() as connection:
        connection.execute(text(statement))
    with pytest.raises(RuntimeError, match=r"privilege|surface"):
        command.upgrade(config, "head")


async def test_recovery_invalid_receipt_rolls_back_even_if_caller_catches_error(prepared_input, owner_sessions, monkeypatch):
    contracts, claim, _profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    factory, engine, installation, *_ = prepared_input
    async with factory.begin() as session:
        execution = store(session, installation)
        original = execution._recovery_call

        async def changed_reply(*args):
            await original(*args)
            return "{}"

        monkeypatch.setattr(execution, "_recovery_call", changed_reply)
        with pytest.raises(ValueError):
            await execution.publish_recovery(contracts.NativeRecoveryPublicationV1(claim=claim, record=prepared), worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_records")) == 0
