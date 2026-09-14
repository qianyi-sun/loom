"""Installed startup reads current boot admission through the existing claim."""

from datetime import UTC, datetime, timedelta
from importlib import import_module
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from tests.integration.test_native_recovery_publication import (
    build_guard_database as build_guard_database,
)
from tests.integration.test_native_recovery_publication import (
    owner_sessions as owner_sessions,
)
from tests.integration.test_native_recovery_publication import (
    prepared_input as prepared_input,
)
from tests.integration.test_native_recovery_publication import (
    recovery_input,
)
from tests.integration.test_native_recovery_publication import (
    sessions as sessions,
)
from tests.integration.test_personal_dev_build_guard_execution import store
from tests.integration.test_personal_dev_build_guard_registration import CREDENTIAL


@pytest.fixture(params=[0, 1], ids=["first-node", "second-node"])
def two_node_pool(monkeypatch, request):
    """Re-pin real pool profiles; retain one-node allocation shapes."""
    from dataclasses import replace

    from tests.unit import test_personal_dev_build_admission as admission_fixture
    from tests.unit import test_personal_dev_build_runtime_installation as installation_fixture

    original = installation_fixture.typed_context
    nodes = {"oldlab": ("oldlab-5", "oldlab-6"), "gb10": ("trt-gb10-3", "trt-gb10-4")}

    def pool_context(*, pool, **kwargs):
        context = original(pool=pool, **kwargs)
        profiles = tuple(profile.model_copy(update={"resource_domains": tuple(
            domain.model_copy(update={"node_ids": nodes[pool]}) for domain in profile.resource_domains)})
            for profile in context.profiles)
        return replace(context, profiles=profiles)

    def allocated_context(*, pool, **kwargs):
        context = original(pool=pool, **kwargs)
        return replace(context, binding=context.binding.model_copy(update={"node_ids": (nodes[pool][request.param],)}))

    monkeypatch.setattr(installation_fixture, "typed_context", pool_context)
    monkeypatch.setattr(admission_fixture, "typed_context", allocated_context)
    return nodes


@pytest.mark.usefixtures("two_node_pool")
async def test_each_pool_node_resolves_under_one_common_recovery_profile(
    prepared_input, owner_sessions, monkeypatch,
):
    from loom_capacity_build_guard.native_recovery_store import NativeRecoveryInstallationStore

    contracts, claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    factory, engine, installation, *_ = prepared_input
    pool = next(pool for pool in installation.document.runtime.pools if pool.pool_id == profile.pool_id)
    assert len(pool.node_ids) == 2 and len(claim.binding.node_ids) == 1
    sibling_node = next(node for node in pool.node_ids if node != prepared.node_id)
    sibling = contracts.NativeRecoveryHostIdentityV1(node_id=sibling_node, boot_id=uuid4(),
        original_uid=24850, original_gid=24851, cgroup_namespace_device=4, cgroup_namespace_inode=202)
    owner_factory, owner_role = owner_sessions
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner_role}"))
        assert await NativeRecoveryInstallationStore(session, expected_owner_role=owner_role).retain_recovery(
            profile, hosts=(sibling,)) == profile
    request = contracts.NativeRecoveryAdmissionRequestV1(claim=claim, boot_id=prepared.boot_id)
    async with factory.begin() as session:
        result = await store(session, installation).read_recovery_admission(request, worker_credential=CREDENTIAL)
        assert result.profile == profile and result.host.node_id == prepared.node_id
        # The same pool profile must not authorize a sibling outside this job.
        with pytest.raises((ValueError, DBAPIError)):
            await store(session, installation).read_recovery_admission(
                request.model_copy(update={"boot_id": sibling.boot_id}), worker_credential=CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_profiles")) == 1
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_hosts")) == 2


@pytest.mark.parametrize("boundary", ["exact", "credential", "boot", "no-profile", "expired", "claim"])
async def test_recovery_admission_requires_live_exact_claim_and_committed_boot(prepared_input, owner_sessions, monkeypatch, boundary):
    contracts = import_module("loom_capacity_agent.native_recovery_publication")
    _contracts, claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch,
        admit=boundary != "no-profile")
    factory, engine, installation, _proposal, source, *_ = prepared_input
    request = contracts.NativeRecoveryAdmissionRequestV1(claim=claim,
        boot_id=uuid4() if boundary == "boot" else prepared.boot_id)
    if boundary == "claim":
        request = request.model_copy(update={"claim": claim.model_copy(update={"operation_id": uuid4()})})
    if boundary == "expired":
        with engine.begin() as connection:
            connection.execute(text("UPDATE personal_dev_candidate_build_attempts SET lease_expires_at=:deadline WHERE id=:id"),
                {"id": source.build_attempt.id, "deadline": datetime.now(UTC) - timedelta(seconds=1)})
    async with factory.begin() as session:
        if boundary == "exact":
            result = await store(session, installation).read_recovery_admission(request, worker_credential=CREDENTIAL)
            assert result.request == request and result.profile == profile
            assert result.host.boot_id == prepared.boot_id and result.host.node_id == prepared.node_id
        else:
            with pytest.raises((ValueError, DBAPIError)):
                await store(session, installation).read_recovery_admission(request,
                    worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)
    with engine.connect() as connection:
        assert connection.scalar(text("SELECT count(*) FROM loom_capacity_build_guard.native_recovery_records")) == 0


@pytest.mark.parametrize("boundary", ["exact", "disabled", "credential", "http"])
async def test_installed_admission_client_uses_authenticated_no_store_readback(prepared_input, owner_sessions, tmp_path, monkeypatch, boundary):
    import httpx

    from tests.integration.test_personal_dev_build_guard_http import application
    from tests.unit.test_capacity_build_admission_client import client_for

    contracts, claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    app = application(prepared_input, tmp_path)
    app.state.personal_dev_build_admission_mode = "disabled" if boundary == "disabled" else "native-execution"
    request = contracts.NativeRecoveryAdmissionRequestV1(claim=claim, boot_id=prepared.boot_id)
    responses = []

    async def observe(response):
        responses.append(response)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), event_hooks={"response": [observe]}) as http:
        client = client_for(http, claim)
        client._token = "executor-secret"
        if boundary == "http":
            client._origin = "http://management.test"
        if boundary == "exact":
            result = await client.read_recovery_admission(request, worker_credential=CREDENTIAL)
            assert result.request == request and result.profile == profile
            assert responses[-1].headers["cache-control"] == "no-store"
        else:
            from loom_capacity_executor.build_admission_client import BuildAdmissionTransportError

            with pytest.raises(BuildAdmissionTransportError):
                await client.read_recovery_admission(request, worker_credential="x" * 43 if boundary == "credential" else CREDENTIAL)


@pytest.mark.parametrize("boundary", ["reboot", "uncommitted", "ambiguous"])
async def test_reboot_admission_preserves_static_config_and_rejects_ambiguous_or_uncommitted_host(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    from loom_capacity_build_guard.native_recovery_store import NativeRecoveryInstallationStore

    contracts, claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    factory, _engine, installation, *_ = prepared_input
    owner_factory, owner_role = owner_sessions
    host = contracts.NativeRecoveryHostIdentityV1(node_id=prepared.node_id, boot_id=uuid4(), original_uid=24850,
        original_gid=24851, cgroup_namespace_device=4, cgroup_namespace_inode=201)
    request = contracts.NativeRecoveryAdmissionRequestV1(claim=claim, boot_id=host.boot_id)
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner_role}"))
        retained = NativeRecoveryInstallationStore(session, expected_owner_role=owner_role)
        assert await retained.retain_recovery(profile, hosts=(host,)) == profile
        if boundary == "ambiguous":
            await retained.retain_recovery(profile, hosts=(host.model_copy(update={"original_uid": 24852}),))
        if boundary == "uncommitted":
            with pytest.raises((ValueError, DBAPIError), match="committed"):
                await store(session, installation).read_recovery_admission(request, worker_credential=CREDENTIAL)
    async with factory.begin() as session:
        if boundary == "ambiguous":
            with pytest.raises((ValueError, DBAPIError)):
                await store(session, installation).read_recovery_admission(request, worker_credential=CREDENTIAL)
        else:
            result = await store(session, installation).read_recovery_admission(request, worker_credential=CREDENTIAL)
            assert result.profile == profile and result.host == host
            original = request.model_copy(update={"boot_id": prepared.boot_id})
            assert (await store(session, installation).read_recovery_admission(original, worker_credential=CREDENTIAL)).profile == profile


@pytest.mark.parametrize("boundary", ["foreign-boot", "shared-boot"])
async def test_boot_lookup_never_selects_host_outside_committed_allocation(
    prepared_input, owner_sessions, monkeypatch, boundary,
):
    from loom_capacity_manager.contracts import canonical_bytes, canonical_digest

    contracts, claim, profile, prepared, _final = await recovery_input(prepared_input, owner_sessions, monkeypatch)
    factory, _engine, installation, *_ = prepared_input
    owner_factory, owner_role = owner_sessions
    foreign = contracts.NativeRecoveryHostIdentityV1(node_id="foreign-node",
        boot_id=prepared.boot_id if boundary == "shared-boot" else uuid4(),
        original_uid=24850, original_gid=24851, cgroup_namespace_device=4, cgroup_namespace_inode=202)
    assert foreign.node_id not in claim.binding.node_ids
    # Deliberately seed a host outside this allocation through the disposable DB
    # owner, to exercise SQL selection rather than Python installation validation.
    wire = canonical_bytes(foreign)
    async with owner_factory.begin() as session:
        await session.execute(text(f"SET LOCAL ROLE {owner_role}"))
        await session.execute(text("""INSERT INTO loom_capacity_build_guard.native_recovery_hosts
            (installation_id,pool_id,payload,wire_payload,payload_sha256)
            VALUES(:installation,:pool,CAST(:payload AS jsonb),:wire,:digest)"""),
            {"installation": installation.id, "pool": profile.pool_id, "payload": wire.decode("ascii"),
                "wire": wire, "digest": canonical_digest(foreign)})
    request = contracts.NativeRecoveryAdmissionRequestV1(claim=claim, boot_id=foreign.boot_id)
    async with factory.begin() as session:
        if boundary == "foreign-boot":
            with pytest.raises(DBAPIError):
                await store(session, installation)._recovery_call("read_recovery_admission", request, CREDENTIAL)
        else:
            result = await store(session, installation).read_recovery_admission(request, worker_credential=CREDENTIAL)
            assert result.profile == profile and result.host.node_id == prepared.node_id
