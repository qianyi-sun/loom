"""Native mint audit is the fixture authority; legacy worker IDs are not fabricated."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from loom.auth import verify_step_jwt
from loom.db.schema import AdminAuditEvent, GatewayDispatchReceipt, ProviderConnection, Trial
from loom_control_plane.service_execution_output import (
    ServiceExecutionPeerV1,
    authorize_service_execution_peer,
    mint_service_execution_peer_token,
)
from loom_llm_gateway.deadline_canary_receipts import resolve_receipt
from tests.integration.test_service_execution_leases import (
    _cleanup_service_execution_test_rows,  # noqa: F401
    _reserve,
    _seed_ready_trial,
)


@pytest.mark.parametrize("observed_state", ["creating", "running"])
@pytest.mark.parametrize(
    "tamper",
    [
        "grant",
        "generation",
        "revoked",
        "provider",
        "missing_pod",
        "replaced_pod",
        "actor",
        "deadline",
        "resource_generation",
        "deleted",
        "delivery",
        "expired_grant",
    ],
)
async def test_native_receipt_requires_real_mint_and_current_lease(
    postgres_url: str, tamper: str, observed_state: str
) -> None:
    engine = create_async_engine(postgres_url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    provider_id = uuid4()
    try:
        async with sessions() as session:
            tid, target = await _seed_ready_trial(session, now=now)
            lease = await _reserve(session, trial_id=tid, target=target, now=now)
            lease.observed_state = observed_state
            lease.pod_uid = str(uuid4())
            lease.pod_ip = "10.24.7.19"
            trial = await session.get(Trial, tid)
            session.add(
                ProviderConnection(
                    id=provider_id,
                    team_id=lease.team_id,
                    provider_type="openai-compatible",
                    display_name="native-deadline",
                    base_url="https://fixture.invalid/v1",
                    upstream_host="fixture.invalid",
                    encrypted_api_key_ref="fixture-only",
                    created_by="test",
                    pricing_source="tokens-only",
                )
            )
            await session.flush()
            trial.provider_connection_id = provider_id
            await session.flush()
            lease = await authorize_service_execution_peer(
                session,
                peer_ip=str(lease.pod_ip),
                identity=ServiceExecutionPeerV1(
                    lease_id=lease.id,
                    generation=lease.generation,
                    execution_role="attempt",
                ),
            )
            token, _, grant_id = await mint_service_execution_peer_token(
                session,
                lease=lease,
                ttl_seconds=480,
                signing_key="test-native-" + "x" * 40,
                attempt_deadline_wall_clock=now + timedelta(seconds=60),
            )
            ctx = verify_step_jwt(token, signing_key="test-native-" + "x" * 40)
            assert ctx.agent_attempt_id is None and ctx.service_execution_lease_id == lease.id
            row = GatewayDispatchReceipt(
                id=uuid4(),
                request_id=uuid4(),
                dispatch_ordinal=1,
                attempt=1,
                team_id=lease.team_id,
                trial_id=tid,
                step_id="agent",
                step_jwt_id=grant_id,
                agent_attempt_id=None,
                provider_connection_id=provider_id,
                dialect="facade_openai",
                purpose="model_call",
                attempt_deadline_wall_clock=ctx.attempt_deadline_wall_clock,
            )
            session.add(row)
            await session.commit()
            args = dict(
                receipt_id=row.id,
                team_id=lease.team_id,
                trial_id=tid,
                provider_connection_id=provider_id,
                step_id="agent",
            )
            approval = await resolve_receipt(session, **args)
            assert approval.agent_attempt_id is None
            assert approval.service_execution_lease_id == lease.id
            assert approval.service_execution_generation == lease.generation
            assert approval.step_jwt_id == grant_id
            assert token not in approval.model_dump_json()
            # Each tampering case has its own transaction; lease generations
            # remain monotonic, even in the disposable database.
            if tamper == "grant":
                row.step_jwt_id = uuid4()
            elif tamper == "generation":
                lease.generation += 1
            elif tamper == "revoked":
                lease.revoked_at = now
            elif tamper == "resource_generation":
                # The resource generation is immutable. Advance the lease and
                # audit together to isolate its older Pod resource fence.
                lease.generation += 1
                grant = await session.scalar(
                    select(AdminAuditEvent).where(
                        AdminAuditEvent.event_metadata["step_jwt_id"].astext == str(grant_id),
                    )
                )
                grant.event_metadata = {**grant.event_metadata, "generation": lease.generation}
            elif tamper == "deleted":
                lease.deleted_at = now
                lease.observed_state = "deleted"
                lease.desired_state = "deleted"
            elif tamper == "missing_pod":
                lease.pod_uid = None
            elif tamper == "replaced_pod":
                lease.pod_uid = str(uuid4())
            elif tamper == "deadline":
                row.attempt_deadline_wall_clock = now - timedelta(seconds=1)
            else:
                grant = await session.scalar(
                    select(AdminAuditEvent).where(
                        AdminAuditEvent.event_metadata["step_jwt_id"].astext == str(grant_id),
                    )
                )
                if tamper == "actor":
                    grant.actor = "service-execution-pod:" + str(uuid4())
                elif tamper == "delivery":
                    grant.event_metadata = {**grant.event_metadata, "credential_delivery": "other"}
                elif tamper == "expired_grant":
                    grant.created_at = now - timedelta(seconds=481)
                else:
                    grant.event_metadata = {
                        **grant.event_metadata,
                        "provider_connection_id": str(uuid4()),
                    }
            await session.flush()
            with pytest.raises(ValueError, match=r"grant|lease|receipt"):
                await resolve_receipt(session, **args)
            await session.rollback()
    finally:
        async with sessions() as session:
            await session.execute(
                update(Trial)
                .where(
                    Trial.provider_connection_id == provider_id,
                )
                .values(provider_connection_id=None)
            )
            await session.execute(
                delete(ProviderConnection).where(ProviderConnection.id == provider_id)
            )
            await session.commit()
        await engine.dispose()
