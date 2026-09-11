"""Private restart-safe discovery; this read grants no execution authority."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes

_ZERO = UUID(int=0)


class PendingBuildPublicationV1(StrictV1Model):
    plan_id: UUID
    proposal_digest: Digest


class PendingBuildPublicationPageV1(StrictV1Model):
    installation_id: UUID
    after_plan_id: UUID
    through_plan_id: UUID
    plans: Annotated[tuple[PendingBuildPublicationV1, ...], Field(max_length=16)]
    executable: Literal[False] = False


class BuildGuardPublicationDiscovery:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build publication discovery installation changed")
        self._session = session
        self._installation = document

    async def read_pending(self, *, after_plan_id: UUID = _ZERO, through_plan_id: UUID | None = None,
        limit: int = 16,
    ) -> PendingBuildPublicationPageV1:
        if not self._session.in_transaction():
            raise ValueError("build publication discovery requires an outer transaction")
        if (not isinstance(after_plan_id, UUID)
            or (through_plan_id is not None and (not isinstance(through_plan_id, UUID) or through_plan_id < after_plan_id))
            or type(limit) is not int or not 1 <= limit <= 16):
            raise ValueError("build publication discovery pagination bounds changed")
        wire = await self._session.scalar(text("""SELECT loom_capacity_build_guard.read_pending_publications(
            :installation,:after,:through,:limit)"""), {"installation": self._installation.id,
            "after": after_plan_id, "through": through_plan_id, "limit": limit})
        page = PendingBuildPublicationPageV1.model_validate_json(wire)
        if (canonical_bytes(page).decode("ascii") != wire or page.installation_id != self._installation.id
            or page.after_plan_id != after_plan_id or page.through_plan_id < after_plan_id
            or (through_plan_id is not None and page.through_plan_id != through_plan_id) or len(page.plans) > limit):
            raise ValueError("build publication discovery receipt changed")
        previous = after_plan_id
        for pending in page.plans:
            if not previous < pending.plan_id <= page.through_plan_id:
                raise ValueError("build publication discovery plan order changed")
            previous = pending.plan_id
        return page
