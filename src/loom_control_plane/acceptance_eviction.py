"""Control-plane authority seam for one fenced acceptance cache eviction."""

from __future__ import annotations

from typing import Protocol
from uuid import UUID

from loom.pipeline.work_protocol import AcceptanceEvictionGrantV1


class AcceptanceEvictionAuthorityV1(Protocol):
    async def authorize(
        self,
        *,
        authorization_id: UUID,
        candidate_sha256: str,
        worker_id: UUID,
        ordered_manifest_sha256s: tuple[str, str, str, str, str],
    ) -> AcceptanceEvictionGrantV1:
        """Return one active fence/worker/candidate-bound non-secret grant."""


__all__ = ["AcceptanceEvictionAuthorityV1"]
