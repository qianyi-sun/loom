"""Retire unregistered native holds only from authenticated manager evidence.

The management reporter fetches the witness over the authenticated manager
transport. Structural parsing is not signature verification. This procedure is
not exposed through executor admission HTTP and never grants build credentials.
"""

from typing import Literal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import Digest, PositiveQuantity, StrictV1Model, canonical_bytes
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


class BuildGuardHoldRetirementStore:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build hold retirement installation receipt changed")
        self._session = session
        self._installation = document

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
