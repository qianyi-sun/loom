"""Single-source, read-only dependency readiness for service and rollout gates."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@dataclass(frozen=True, slots=True)
class DependencyReadiness:
    """Secret-free result shared by the HTTP route and rollout preflight."""

    postgres_ready: bool
    object_store_ready: bool
    resource_digest: str
    blockers: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.postgres_ready and self.object_store_ready and not self.blockers

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": "ready" if self.ready else "not-ready",
            "postgres": "ready" if self.postgres_ready else "not-ready",
            "object_store": "ready" if self.object_store_ready else "not-ready",
            "resource_digest": self.resource_digest,
            "blockers": list(self.blockers),
        }


async def probe_dependencies(
    session: AsyncSession,
    *,
    minio_client: Any,
    buckets: tuple[str, ...],
) -> DependencyReadiness:
    """Probe PostgreSQL and exact configured buckets without writing state.

    The object-store call is HEAD-only and executes off the event loop.  Results
    deliberately expose stable component codes, never connection strings,
    credentials, provider error text, or object names.
    """
    normalized_buckets = tuple(sorted(set(buckets)))
    if not normalized_buckets or any(
        not bucket or len(bucket) > 63 for bucket in normalized_buckets
    ):
        raise ValueError("readiness bucket authority is invalid")

    blockers: list[str] = []
    postgres_ready = False
    try:
        value = (await session.execute(text("SELECT 1"))).scalar_one()
        postgres_ready = value == 1
    except Exception:  # pragma: no cover - driver/provider classes vary
        blockers.append("postgres-unavailable")
    if not postgres_ready and "postgres-unavailable" not in blockers:
        blockers.append("postgres-unexpected-result")

    object_store_ready = True
    for bucket in normalized_buckets:
        try:
            await asyncio.to_thread(minio_client.head_bucket, Bucket=bucket)
        except Exception:  # pragma: no cover - botocore/provider classes vary
            object_store_ready = False
            blockers.append(f"object-store-bucket-unavailable:{bucket}")

    digest = hashlib.sha256(
        json.dumps(
            {
                "buckets": normalized_buckets,
                "object_store_ready": object_store_ready,
                "postgres_ready": postgres_ready,
                "version": "v1",
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return DependencyReadiness(
        postgres_ready=postgres_ready,
        object_store_ready=object_store_ready,
        resource_digest=digest,
        blockers=tuple(sorted(blockers)),
    )


__all__ = ["DependencyReadiness", "probe_dependencies"]
