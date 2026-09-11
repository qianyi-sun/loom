"""Retain manager-authenticated native terminal witnesses, not release permits.

The management reporter supplies evidence fetched from the authenticated manager.
Parsing here is structural validation, not local Ed25519 signature verification.
This interface is deliberately absent from pool-executor admission HTTP routes.
"""

from typing import Literal
from uuid import UUID, uuid5

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import ExecutableReleaseReceiptV2, ExecutableReleaseRequestV2
from loom_capacity_agent.build_admission import (
    BuildClaimRequestV1,
    BuildInterruptedOutcomeRequestV1,
    BuildOutcomeReceiptV1,
)
from loom_capacity_build_guard.execution_store import native_release_receipt
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import (
    Digest,
    Identifier,
    PositiveQuantity,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.typed_inventory_contracts import (
    ExecutableTerminalInventoryEvidenceV3,
    parse_terminal_inventory_evidence,
)


class ImportedBuildTerminalEvidenceV1(StrictV1Model):
    installation_id: UUID
    assignment_id: UUID
    binding: ExecutableIntentBindingV2
    physical_job_id: Identifier
    inventory_sequence: PositiveQuantity
    terminal_evidence_sha256: Digest
    evidence_digest: Digest
    protected_high_water: PositiveQuantity
    import_state: Literal["imported"] = "imported"
    executable: Literal[False] = False


class BuildGuardTerminalStore:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build terminal installation receipt changed")
        self._session = session
        self._installation = document

    async def release_terminal_worker(self, request: ExecutableReleaseRequestV2, *, terminal_inventory_sha256: str) -> ExecutableReleaseReceiptV2:
        """Management-only release after exact terminal import, without a lost secret."""
        if not self._session.in_transaction():
            raise ValueError("native terminal release requires an outer transaction")
        request = ExecutableReleaseRequestV2.model_validate_json(request.model_dump_json())
        if (not isinstance(terminal_inventory_sha256, str) or len(terminal_inventory_sha256) != 64
            or any(character not in "0123456789abcdef" for character in terminal_inventory_sha256)):
            raise ValueError("native terminal release inventory digest is invalid")
        wire = canonical_executable_bytes(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.release_terminal_worker(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:terminal)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": canonical_executable_digest(request), "terminal": terminal_inventory_sha256})
            return native_release_receipt(returned, request)

    async def settle_interrupted(self, claim: BuildClaimRequestV1, *, terminal_inventory_sha256: str) -> BuildOutcomeReceiptV1:
        """Settle a lost result only from a previously committed terminal import.

        This is management-only recovery, not an executor HTTP operation. A prior
        worker outcome wins unchanged; terminal evidence never implies success.
        """
        if not self._session.in_transaction():
            raise ValueError("native interruption requires an outer transaction")
        claim = BuildClaimRequestV1.model_validate_json(claim.model_dump_json())
        request = BuildInterruptedOutcomeRequestV1(claim=claim,
            operation_id=uuid5(claim.operation_id, f"native-terminal:{terminal_inventory_sha256}"),
            terminal_inventory_sha256=terminal_inventory_sha256)
        wire, digest = canonical_bytes(request), canonical_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.settle_interrupted_claim(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire, "digest": digest})
            receipt = BuildOutcomeReceiptV1.model_validate_json(returned)
            if (canonical_bytes(receipt).decode("ascii") != returned or receipt.request.claim != claim
                or receipt.request_digest != canonical_digest(receipt.request)
                or (isinstance(receipt.request, BuildInterruptedOutcomeRequestV1) and receipt.request != request)):
                raise ValueError("native interruption receipt changed")
            return receipt

    async def import_evidence(self, evidence: ExecutableTerminalInventoryEvidenceV3) -> ImportedBuildTerminalEvidenceV1:
        if not self._session.in_transaction():
            raise ValueError("build terminal import requires an outer transaction")
        if type(evidence) is not ExecutableTerminalInventoryEvidenceV3:
            raise ValueError("build terminal import requires native V3 evidence")
        checked = parse_terminal_inventory_evidence(canonical_executable_bytes(evidence))
        if not isinstance(checked, ExecutableTerminalInventoryEvidenceV3):
            raise ValueError("build terminal import requires native V3 evidence")
        proof = checked.record.ownership_proof
        if (proof is None
            or proof.metadata.subject_authority.purpose != "personal-build-worker"
            or checked.record.physical_kind != "slurm-job"):
            raise ValueError("build terminal import requires native Slurm evidence")
        wire = canonical_executable_bytes(checked)
        digest = canonical_executable_digest(checked)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.import_terminal_inventory(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire, "digest": digest})
            receipt = ImportedBuildTerminalEvidenceV1.model_validate_json(returned)
            if (canonical_bytes(receipt).decode("ascii") != returned
                or receipt.installation_id != self._installation.id or receipt.assignment_id.int == 0
                or receipt.binding != checked.binding
                or receipt.binding.subject_id != self._installation.subject_id
                or receipt.binding.subject_incarnation != self._installation.subject_incarnation
                or receipt.physical_job_id != checked.record.physical_identity
                or receipt.inventory_sequence != checked.inventory_sequence
                or receipt.terminal_evidence_sha256 != checked.record.terminal_evidence_sha256
                or receipt.evidence_digest != digest):
                raise ValueError("build terminal import receipt changed")
            return receipt
