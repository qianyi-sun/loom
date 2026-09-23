"""Read-only, secret-free receipt projection inside the installed Gateway image.

This is an operator process, not a public HTTP endpoint. Existing Gateway DB
credentials stay in that container. The fault fixture gets no database access.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from loom.db.schema import (
    AdminAuditEvent,
    GatewayDispatchReceipt,
    ServiceExecutionLease,
    TrialEvent,
)
from loom.deadline_canary import ReceiptApproval


async def resolve_receipt(
    session: AsyncSession,
    *,
    receipt_id: UUID,
    team_id: UUID,
    trial_id: UUID,
    provider_connection_id: UUID,
    step_id: str,
    previous_attempt_id: UUID | None = None,
) -> ReceiptApproval:
    row = await session.scalar(
        select(GatewayDispatchReceipt).where(
            GatewayDispatchReceipt.id == receipt_id,
            GatewayDispatchReceipt.team_id == team_id,
            GatewayDispatchReceipt.trial_id == trial_id,
            GatewayDispatchReceipt.provider_connection_id == provider_connection_id,
            GatewayDispatchReceipt.step_id == step_id,
            GatewayDispatchReceipt.dialect == "facade_openai",
            GatewayDispatchReceipt.purpose == "model_call",
            GatewayDispatchReceipt.execution_attempt_id.is_(None),
            GatewayDispatchReceipt.provider_outcome == "admitted",
            GatewayDispatchReceipt.gateway_outcome == "pending",
        )
    )
    if (
        row is None
        or row.step_jwt_id is None
        or row.attempt_deadline_wall_clock is None
        or row.attempt_deadline_wall_clock <= datetime.now(UTC)
    ):
        raise ValueError("receipt is not eligible")
    native_lease_id = None
    native_generation = None
    if row.agent_attempt_id is None:
        # Native credentials rotate; the durable mint audit binds each grant
        # to its observed Pod lease/generation. Never infer this from time or
        # manufacture a legacy worker attempt UUID for a native request.
        grants = list(
            (
                await session.scalars(
                    select(AdminAuditEvent)
                    .where(
                        AdminAuditEvent.action == "service_execution.step_token.minted",
                        AdminAuditEvent.target_type == "execution_lease",
                        AdminAuditEvent.event_metadata["step_jwt_id"].astext
                        == str(row.step_jwt_id),
                        AdminAuditEvent.event_metadata["trial_id"].astext == str(trial_id),
                        AdminAuditEvent.event_metadata["team_id"].astext == str(team_id),
                    )
                    .limit(2)
                )
            ).all()
        )
        if len(grants) != 1 or previous_attempt_id is not None:
            raise ValueError("native receipt grant is not eligible")
        grant = grants[0]
        metadata = grant.event_metadata
        lease = await session.get(ServiceExecutionLease, UUID(grant.target_id))
        if (
            lease is None
            or lease.trial_id != trial_id
            or lease.team_id != team_id
            or lease.execution_role != "attempt"
            or row.step_id != "agent"
            or lease.observed_state != "running"
            or lease.revoked_at is not None
            or lease.desired_state not in {"create", "start"}
            or metadata.get("generation") != lease.generation
            or metadata.get("execution_role") != "attempt"
            or metadata.get("provider_connection_id") != str(provider_connection_id)
            or metadata.get("runtime_contract_sha256") != lease.runtime_contract_sha256
            or metadata.get("attempt_deadline_wall_clock")
            != row.attempt_deadline_wall_clock.isoformat()
            or lease.deadline_at < row.attempt_deadline_wall_clock
            or not isinstance(metadata.get("expires_in_seconds"), int)
            or grant.created_at + timedelta(seconds=metadata["expires_in_seconds"])
            <= datetime.now(UTC)
        ):
            raise ValueError("native receipt lease is not eligible")
        native_lease_id, native_generation = lease.id, lease.generation
    stopped = False
    if previous_attempt_id is not None:
        observations = list(
            (
                await session.scalars(
                    select(TrialEvent.payload)
                    .where(
                        TrialEvent.trial_id == trial_id,
                        TrialEvent.kind == "agent_timeout",
                        TrialEvent.source == "worker",
                        TrialEvent.payload["agent_attempt_id"].astext == str(previous_attempt_id),
                    )
                    .limit(2)
                )
            ).all()
        )
        if (
            len(observations) != 1
            or observations[0].get("task_stopped") is not True
            or observations[0].get("configured_timeout_sec") != 10
            or previous_attempt_id == row.agent_attempt_id
        ):
            raise ValueError("previous attempt stop is not proven")
        stopped = True
    return ReceiptApproval(
        receipt_id=row.id,
        team_id=team_id,
        trial_id=trial_id,
        provider_connection_id=provider_connection_id,
        step_id=step_id,
        agent_attempt_id=row.agent_attempt_id,
        service_execution_lease_id=native_lease_id,
        service_execution_generation=native_generation,
        step_jwt_id=row.step_jwt_id,
        deadline=row.attempt_deadline_wall_clock,
        previous_attempt_stopped=stopped,
    )


async def _read(args: argparse.Namespace) -> ReceiptApproval:
    from loom_llm_gateway.config import GatewaySettings

    settings = GatewaySettings()
    engine = create_async_engine(
        settings.db_engine_url,
        connect_args=settings.db_engine_connect_args,
        echo=False,
        hide_parameters=True,
    )
    try:
        async with asyncio.timeout(2):
            async with async_sessionmaker(engine)() as session:
                await session.execute(text("SET TRANSACTION READ ONLY"))
                return await resolve_receipt(
                    session,
                    receipt_id=args.receipt_id,
                    team_id=args.team_id,
                    trial_id=args.trial_id,
                    provider_connection_id=args.provider_connection_id,
                    step_id=args.step_id,
                    previous_attempt_id=args.previous_attempt_id,
                )
    finally:
        await engine.dispose()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("receipt-id", "team-id", "trial-id", "provider-connection-id"):
        parser.add_argument("--" + name, required=True, type=UUID)
    parser.add_argument("--step-id", required=True)
    parser.add_argument("--previous-attempt-id", type=UUID)
    args = parser.parse_args()
    try:
        result = asyncio.run(_read(args))
    except Exception:
        # Driver errors may contain SQL bindings or a DSN; never print them.
        print(json.dumps({"error": "canary_receipt_unavailable"}))
        return 1
    print(result.model_dump_json())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
