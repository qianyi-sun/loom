"""Protected pending/held observation before native claim admission is installed.

Management supplies complete current source snapshots; SQL independently checks
that set, excludes holds from pending and derives assignments from retained plans.
This is not worker readiness and must grow actual fixed-claim projection together
with the purpose-specific native bootstrap/claim implementation.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.personal_dev_build_platform_requests import canonical_build_source
from loom.personal_dev_candidate import CandidateRegistration
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import DemandSnapshotV1, canonical_bytes


class BuildDemandCoordinator:
    """Commit a complete observation with bounded whole-transaction conflict retry.

    No external publication happens inside these retries. Ambiguous connection
    failures and authority/source errors propagate rather than inventing success.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *, installation: RetainedBuildInstallation) -> None:
        self._sessions = session_factory
        self._installation = installation

    async def capture(self, *, configuration_generation: int,
        sources: Mapping[UUID, CandidateRegistration],
    ) -> DemandSnapshotV1:
        async with asyncio.timeout(30):
            for attempt in range(3):
                try:
                    async with self._sessions.begin() as session:
                        snapshot = await BuildGuardDemandStore(session, installation=self._installation).capture(
                            configuration_generation=configuration_generation, sources=sources)
                    return snapshot
                except DBAPIError as exc:
                    if attempt == 2 or getattr(exc.orig, "sqlstate", None) not in {"40001", "40P01"}:
                        raise
        raise AssertionError("bounded demand transaction retry did not return or raise")


class BuildGuardDemandStore:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build demand installation receipt changed")
        self._session = session
        self._installation = document

    def _decode(self, wire: str) -> DemandSnapshotV1:
        snapshot = DemandSnapshotV1.model_validate_json(wire)
        if (canonical_bytes(snapshot).decode("ascii") != wire
            or snapshot.subject_id != self._installation.subject_id
            or snapshot.subject_incarnation != self._installation.subject_incarnation
            or snapshot.deployment_generation != self._installation.deployment_generation
            or snapshot.reporter_incarnation != self._installation.reporter_incarnation):
            raise ValueError("build demand report binding changed")
        return snapshot

    async def capture(self, *, configuration_generation: int,
        sources: Mapping[UUID, CandidateRegistration],
    ) -> DemandSnapshotV1:
        if not self._session.in_transaction():
            raise ValueError("build demand requires an outer transaction")
        if type(configuration_generation) is not int or not 0 < configuration_generation <= 2**63-1:
            raise ValueError("build demand configuration generation is invalid")
        if any(not isinstance(key, UUID) or key.int == 0 for key in sources):
            raise ValueError("build demand sources require exact request UUIDs")
        source_wire = {str(key): canonical_build_source(value).decode("ascii") for key,value in sources.items()}
        async with self._session.begin_nested():
            wire = await self._session.scalar(text("""
                SELECT loom_capacity_build_guard.capture_demand(:installation, :generation, CAST(:sources AS jsonb))
            """), {"installation": self._installation.id, "generation": configuration_generation,
                "sources": json.dumps(source_wire)})
            snapshot = self._decode(wire)
            if snapshot.configuration_generation != configuration_generation:
                raise ValueError("build demand configuration binding changed")
            return snapshot

    async def read_latest(self) -> DemandSnapshotV1 | None:
        if not self._session.in_transaction():
            raise ValueError("build demand requires an outer transaction")
        wire = await self._session.scalar(text("SELECT loom_capacity_build_guard.read_demand(:installation)"),
            {"installation": self._installation.id})
        return None if wire is None else self._decode(wire)
