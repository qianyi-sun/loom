"""Retire unregistered native holds only from authenticated manager evidence.

The management reporter fetches the witness over the authenticated manager
transport. Structural parsing is not signature verification. This procedure is
not exposed through executor admission HTTP and never grants build credentials.
"""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import PublishableExecutableProtectedReleaseV2
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import (
    Digest,
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    canonical_bytes,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableFinalReleaseWitnessV2,
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)


class RetiredBuildHoldV1(StrictV1Model):
    installation_id: UUID
    request_id: UUID
    assignment_id: UUID
    binding: ExecutableIntentBindingV2
    witness_sha256: Digest
    protected_high_water: PositiveQuantity
    retirement_state: Literal["retired"] = "retired"
    executable: Literal[False] = False


class PendingBuildRetirementPageV1(StrictV1Model):
    installation_id: UUID
    after_event_id: Quantity
    through_event_id: Quantity
    publications: Annotated[tuple[PublishableExecutableProtectedReleaseV2, ...], Field(max_length=64)]
    executable: Literal[False] = False


class BuildGuardHoldRetirementStore:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build hold retirement installation receipt changed")
        self._session = session
        self._installation = document

    async def read_pending(self, *, after_event_id: int = 0, through_event_id: int | None = None,
        limit: int = 16,
    ) -> PendingBuildRetirementPageV1:
        if not self._session.in_transaction():
            raise ValueError("build hold discovery requires an outer transaction")
        if (type(after_event_id) is not int or not 0 <= after_event_id < 2**63
            or (through_event_id is not None and (type(through_event_id) is not int or not after_event_id <= through_event_id < 2**63))
            or type(limit) is not int or not 1 <= limit <= 64):
            raise ValueError("build hold discovery pagination bounds changed")
        returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.read_pending_retirements(
            :installation,:after,:through,:limit)"""),
            {"installation": self._installation.id, "after": after_event_id, "through": through_event_id, "limit": limit})
        page = PendingBuildRetirementPageV1.model_validate_json(returned)
        if (canonical_bytes(page).decode("ascii") != returned or page.installation_id != self._installation.id
            or page.after_event_id != after_event_id or page.through_event_id < after_event_id
            or (through_event_id is not None and page.through_event_id != through_event_id)
            or len(page.publications) > limit):
            raise ValueError("build hold discovery receipt changed")
        previous = after_event_id
        for publication in page.publications:
            binding = publication.release.binding
            if (not previous < publication.event_id <= page.through_event_id
                or publication.event_kind not in {"prepared-revoked", "withdrawn"}
                or binding.subject_id != self._installation.subject_id
                or binding.subject_incarnation != self._installation.subject_incarnation
                or binding.deployment_generation != self._installation.deployment_generation
                or binding.candidate_generation != self._installation.candidate_generation
                or binding.candidate != self._installation.runtime.candidate
                or publication.release.reporter_incarnation != self._installation.reporter_incarnation):
                raise ValueError("build hold discovery publication binding changed")
            previous = publication.event_id
        return page

    async def retire(self, witness: ExecutableFinalReleaseWitnessV2) -> RetiredBuildHoldV1:
        if not self._session.in_transaction():
            raise ValueError("build hold retirement requires an outer transaction")
        witness = ExecutableFinalReleaseWitnessV2.model_validate_json(witness.model_dump_json())
        binding = witness.release.binding
        installation = self._installation
        if (binding.subject_id != installation.subject_id or binding.subject_incarnation != installation.subject_incarnation
            or binding.deployment_generation != installation.deployment_generation
            or binding.candidate_generation != installation.candidate_generation
            or binding.candidate != installation.runtime.candidate
            or witness.protected_release.reporter_incarnation != installation.reporter_incarnation):
            raise ValueError("build hold retirement installation binding changed")
        wire = canonical_executable_bytes(witness)
        digest = canonical_executable_digest(witness)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.retire_request_hold(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation": installation.id, "payload": wire.decode("ascii"), "wire": wire, "digest": digest})
            receipt = RetiredBuildHoldV1.model_validate_json(returned)
            if (canonical_bytes(receipt).decode("ascii") != returned
                or receipt.installation_id != installation.id or receipt.request_id.int == 0 or receipt.assignment_id.int == 0
                or receipt.binding != binding or receipt.witness_sha256 != digest):
                raise ValueError("build hold retirement receipt changed")
            return receipt
