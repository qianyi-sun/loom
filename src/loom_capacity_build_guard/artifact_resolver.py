"""Select an accepted claim's archive under current management source authority."""

import asyncio
from hashlib import sha256

from sqlalchemy import text
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
        request_id = personal_build_work_identity(registration, platform)[1]
        wire = canonical_build_source(registration)
        async with asyncio.timeout(30), self._sessions.begin() as session:
            returned = await session.scalar(text("""SELECT loom_capacity_build_guard.read_accepted_artifact(
                :installation,:request,CAST(:source AS jsonb),:wire,:digest)"""),
                {"installation": self._installation.id, "request": request_id,
                    "source": wire.decode("ascii"), "wire": wire, "digest": sha256(wire).hexdigest()})
            receipt = BuildOutcomeReceiptV1.model_validate_json(returned)
            outcome, binding = receipt.request, receipt.request.claim.binding
            if (canonical_bytes(receipt).decode("ascii") != returned
                or receipt.request_digest != canonical_digest(outcome) or outcome.result != "artifact-ready"
                or outcome.claim.request_id != request_id
                or binding.subject_id != self._installation.subject_id
                or binding.subject_incarnation != self._installation.subject_incarnation
                or binding.deployment_generation != self._installation.deployment_generation
                or binding.candidate_generation != self._installation.candidate_generation
                or binding.candidate != self._installation.runtime.candidate
                or binding.pool_id != ("oldlab" if platform == "linux/amd64" else "gb10")):
                raise ValueError("accepted native artifact receipt changed")
        return receipt
