"""Private discovery of held registered workers before protected release."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.build_admission import BuildClaimRequestV1
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import (
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    canonical_bytes,
)
from loom_capacity_manager.executable_contracts import ExecutableIntentBindingV2


class PendingNativeWorkerV1(StrictV1Model):
    event_id: PositiveQuantity
    binding: ExecutableIntentBindingV2
    operation_id: UUID
    worker_id: UUID
    worker_incarnation: UUID
    claim: BuildClaimRequestV1 | None


class PendingNativeWorkerPageV1(StrictV1Model):
    installation_id: UUID
    after_event_id: Quantity
    through_event_id: Quantity
    workers: Annotated[tuple[PendingNativeWorkerV1, ...], Field(max_length=64)]
    executable: Literal[False] = False


class BuildGuardTerminalDiscovery:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("native terminal discovery installation changed")
        self._session = session
        self._installation = document

    async def read_pending(self, *, after_event_id: int = 0, through_event_id: int | None = None,
        limit: int = 16,
    ) -> PendingNativeWorkerPageV1:
        if not self._session.in_transaction():
            raise ValueError("native terminal discovery requires an outer transaction")
        if (type(after_event_id) is not int or not 0 <= after_event_id < 2**63
            or (through_event_id is not None and (type(through_event_id) is not int or not after_event_id <= through_event_id < 2**63))
            or type(limit) is not int or not 1 <= limit <= 64):
            raise ValueError("native terminal discovery pagination bounds changed")
        returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.read_pending_native_workers(
            :installation,:after,:through,:limit)"""),
            {"installation": self._installation.id, "after": after_event_id, "through": through_event_id, "limit": limit})
        page = PendingNativeWorkerPageV1.model_validate_json(returned)
        if (canonical_bytes(page).decode("ascii") != returned or page.installation_id != self._installation.id
            or page.after_event_id != after_event_id or page.through_event_id < after_event_id
            or (through_event_id is not None and page.through_event_id != through_event_id)
            or len(page.workers) > limit):
            raise ValueError("native terminal discovery receipt changed")
        previous = after_event_id
        for worker in page.workers:
            binding, claim = worker.binding, worker.claim
            if (not previous < worker.event_id <= page.through_event_id
                or binding.subject_id != self._installation.subject_id
                or binding.subject_incarnation != self._installation.subject_incarnation
                or binding.deployment_generation != self._installation.deployment_generation
                or binding.candidate_generation != self._installation.candidate_generation
                or binding.candidate != self._installation.runtime.candidate
                or (claim is not None and (claim.binding != binding or claim.worker_id != worker.worker_id
                    or claim.worker_incarnation != worker.worker_incarnation))):
                raise ValueError("native terminal discovery worker binding changed")
            previous = worker.event_id
        return page
