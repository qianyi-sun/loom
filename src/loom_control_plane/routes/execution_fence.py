"""Shared HTTP header projection for service execution generation fencing."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from loom_control_plane.service_execution import (
    ExecutionFence,
    ServiceExecutionFenceError,
    verify_trial_execution_fence,
)

OptionalExecutionLeaseIdHeader = Annotated[UUID | None, Header(alias="X-Loom-Execution-Lease-Id")]
OptionalExecutionGenerationHeader = Annotated[
    int | None, Header(alias="X-Loom-Execution-Generation")
]


async def enforce_trial_execution_fence(
    session: AsyncSession,
    *,
    trial_id: UUID,
    lease_id: UUID | None,
    generation: int | None,
    surface: str,
    lock: bool = False,
) -> ExecutionFence | None:
    try:
        return await verify_trial_execution_fence(
            session,
            trial_id=trial_id,
            lease_id=lease_id,
            generation=generation,
            surface=surface,
            lock=lock,
        )
    except ServiceExecutionFenceError as exc:
        raise HTTPException(status_code=409, detail="execution_generation_fenced") from exc


__all__ = [
    "OptionalExecutionGenerationHeader",
    "OptionalExecutionLeaseIdHeader",
    "enforce_trial_execution_fence",
]
