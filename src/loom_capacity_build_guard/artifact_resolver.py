"""Select an accepted claim's archive under current management source authority."""

import asyncio
from hashlib import sha256
from typing import Literal

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.personal_dev_build_demand import personal_build_work_identity
from loom.personal_dev_build_platform_requests import canonical_build_source
from loom.personal_dev_candidate import CandidateRegistration, PersonalDevPlatform
from loom_capacity_agent.build_admission import BuildOutcomeReceiptV1
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest


class BuildAcceptedArtifactResolver:
    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("accepted artifact installation changed")
        self._sessions = session_factory
        self._installation = document

    async def resolve(self, registration: CandidateRegistration, *, platform: PersonalDevPlatform) -> BuildOutcomeReceiptV1:
        receipt = await self._read(registration, platform=platform, operation="read_accepted_artifact")
        if receipt is None or receipt.request.result != "artifact-ready":
            raise ValueError("accepted native artifact receipt changed")
        return receipt

    async def observe(self, registration: CandidateRegistration, *, platform: PersonalDevPlatform) -> BuildOutcomeReceiptV1 | None:
        """Only SQL NULL is pending; stale authority and database failures propagate."""
        return await self._read(registration, platform=platform, operation="read_platform_outcome")

    async def _read(self, registration: CandidateRegistration, *, platform: PersonalDevPlatform,
        operation: Literal["read_accepted_artifact", "read_platform_outcome"],
    ) -> BuildOutcomeReceiptV1 | None:
        # Heartbeats and outcome commits can conflict with this serializable
        # read. Only definite transaction aborts may retry, within one budget.
        async with asyncio.timeout(30):
            for attempt in range(3):
                try:
                    return await self._read_once(registration, platform=platform, operation=operation)
                except DBAPIError as exc:
                    if attempt == 2 or getattr(exc.orig, "sqlstate", None) not in {"40001", "40P01"}:
                        raise
        raise AssertionError("bounded native observation retry did not return or raise")

    async def _read_once(self, registration: CandidateRegistration, *, platform: PersonalDevPlatform,
        operation: Literal["read_accepted_artifact", "read_platform_outcome"],
    ) -> BuildOutcomeReceiptV1 | None:
        if platform not in {"linux/amd64", "linux/arm64"}:
            raise ValueError("native platform observation requires a native architecture")
        request_id = personal_build_work_identity(registration, platform)[1]
        wire = canonical_build_source(registration)
        async with self._sessions.begin() as session:
            returned = await session.scalar(text(f"""SELECT loom_capacity_build_guard.{operation}(
                :installation,:request,CAST(:source AS jsonb),:wire,:digest)"""),
                {"installation": self._installation.id, "request": request_id,
                    "source": wire.decode("ascii"), "wire": wire, "digest": sha256(wire).hexdigest()})
            if returned is None:
                return None
            receipt = BuildOutcomeReceiptV1.model_validate_json(returned)
            outcome, binding = receipt.request, receipt.request.claim.binding
            if (canonical_bytes(receipt).decode("ascii") != returned
                or receipt.request_digest != canonical_digest(outcome)
                or outcome.claim.request_id != request_id
                or binding.subject_id != self._installation.subject_id
                or binding.subject_incarnation != self._installation.subject_incarnation
                or binding.deployment_generation != self._installation.deployment_generation
                or binding.candidate_generation != self._installation.candidate_generation
                or binding.candidate != self._installation.runtime.candidate
                or binding.pool_id != ("oldlab" if platform == "linux/amd64" else "gb10")):
                raise ValueError("accepted native artifact receipt changed")
        return receipt
