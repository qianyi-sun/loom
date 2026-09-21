"""Small database bridge executed inside the existing control-plane container.

No registry credentials or network I/O belong here. Reuse the materialization
lease for retirement; never delete a ready cache behind the scheduler's back.
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from loom.db.schema import Agent, Batch, Task, TaskImageMaterialization
from loom_control_plane.task_image_materializations import (
    _durable_reference_exists,
    claim_task_image_registry_gc,
    complete_task_image_registry_gc,
)


def image_refs(value: object) -> set[str]:
    if isinstance(value, str):
        return {value} if re.fullmatch(r"cr\.[a-z0-9-]+\.nebius\.cloud/[a-z0-9/_.-]+(?:@sha256:[a-f0-9]{64}|:[A-Za-z0-9_.-]+)", value) else set()
    if isinstance(value, dict):
        return set().union(*(image_refs(item) for item in value.values()))
    if isinstance(value, list):
        return set().union(*(image_refs(item) for item in value))
    return set()


async def snapshot(session: AsyncSession) -> dict:
    protected: set[str] = set()
    # Frozen Batch profiles retain historical runtime choices. Catalog runtime
    # versions remain usable until explicitly retired by their owning surface.
    for column in (Task.config, Agent.spec, Batch.service_execution_runtime_profile):
        for value in await session.scalars(select(column)):
            protected.update(image_refs(value))
    rows = []
    for row, referenced in await session.execute(select(
        TaskImageMaterialization, _durable_reference_exists(TaskImageMaterialization),
    )):
        refs = image_refs(row.registry_images) | image_refs(row.registry_image_history)
        rows.append({
            "id": str(row.id), "key": row.materialization_key, "state": row.state,
            "references": sorted(refs), "referenced": bool(referenced),
            "unreferenced_at": row.unreferenced_at.isoformat() if row.unreferenced_at else None,
            "legacy_publication": row.ready_publication_operation_id is not None,
            "lease_expires_at": row.lease_expires_at.isoformat() if row.lease_expires_at else None,
        })
    return {"protected": sorted(protected), "materializations": rows}


async def run(session: AsyncSession, payload: dict) -> dict:
    await session.execute(text("SET LOCAL statement_timeout = '10s'"))
    if payload["action"] == "snapshot":
        return await snapshot(session)
    if not await session.scalar(text(
        "SELECT EXISTS (SELECT 1 FROM nebius_rollout_guard WHERE owner = :owner)"
    ), {"owner": payload["owner"]}):
        raise ValueError("maintenance does not own the idle guard")
    if payload["action"] == "claim":
        row = await claim_task_image_registry_gc(
            session, gc_id=payload["owner"], grace_hours=payload["days"] * 24,
            lease_seconds=900, materialization_ids=[UUID(value) for value in payload["ids"]],
        )
        if row is None:
            return {"claim": None}
        return {"claim": {"id": str(row.id), "lease_epoch": row.lease_epoch,
                          "references": sorted(image_refs(row.registry_images) | image_refs(row.registry_image_history))}}
    if payload["action"] == "complete":
        row = await complete_task_image_registry_gc(
            session, materialization_id=UUID(payload["id"]),
            gc_id=payload["owner"], lease_epoch=payload["lease_epoch"],
        )
        return {"state": row.state}
    raise ValueError("unknown maintenance action")


async def main() -> None:
    from loom_control_plane.config import ControlPlaneSettings

    settings = ControlPlaneSettings()
    engine = create_async_engine(settings.db_engine_url, connect_args=settings.db_engine_connect_args)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session, session.begin():
            result = await run(session, json.loads(sys.argv[1]))
        print(json.dumps(result))
    finally:
        await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception:
        print("Image retention database operation failed", file=sys.stderr)
        sys.exit(1)
