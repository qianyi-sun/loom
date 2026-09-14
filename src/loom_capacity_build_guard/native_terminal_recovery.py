"""Management-only retained recovery readback, never a node deletion permit.

The fixed management sender must obtain these facts from this store. Parsing a
caller-supplied model is not authentication. No worker HTTP operation exposes
this read, and local terminal/quiescence/identity checks remain mandatory.
"""

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import Field, model_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import ExecutableReleaseReceiptV2
from loom_capacity_agent.build_admission import build_installation_id
from loom_capacity_agent.native_recovery import NativeRecoveryPreparationV1
from loom_capacity_agent.native_recovery_publication import (
    NativeRecoveryHistoryV1,
    NativeRecoveryHostIdentityV1,
    NativeRecoveryProfileV1,
    NativeRecoveryReceiptV1,
)
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_build_guard.terminal_store import ImportedBuildTerminalEvidenceV1
from loom_capacity_manager.contracts import (
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)


class NativeTerminalRecoveryV1(StrictV1Model):
    preparation: NativeRecoveryReceiptV1
    finalization: NativeRecoveryReceiptV1 | None
    profile: NativeRecoveryProfileV1
    host: NativeRecoveryHostIdentityV1
    terminal: ImportedBuildTerminalEvidenceV1
    release: ExecutableReleaseReceiptV2
    executable: Literal[False] = False

    @model_validator(mode="after")
    def _binding(self) -> Self:
        NativeRecoveryHistoryV1(preparation=self.preparation, finalization=self.finalization)
        prepared = self.preparation.request.record
        if not isinstance(prepared, NativeRecoveryPreparationV1):
            raise ValueError("terminal recovery preparation phase changed")
        claim = self.preparation.request.claim
        binding, locator = claim.binding, prepared.locator
        installation = build_installation_id(binding.subject_id, binding.subject_incarnation, binding.deployment_generation)
        if (self.profile.installation_id != installation or self.terminal.installation_id != installation
            or self.profile.pool_id != binding.pool_id or self.terminal.binding != binding or self.release.binding != binding
            or self.terminal.physical_job_id != locator.physical.slurm_job_id
            or self.profile.worker_config_sha256 != locator.config_sha256
            or self.profile.release_manifest_sha256 != locator.release_manifest_sha256
            or self.profile.launch_profile_sha256 != prepared.launch_profile_sha256
            or canonical_digest(self.host) != prepared.node_configuration_sha256
            or self.host.node_id != prepared.node_id or self.host.boot_id != prepared.boot_id
            or self.host.original_uid != prepared.original_uid or self.host.original_gid != prepared.original_gid
            or not self.release.bootstrap_revoked or not self.release.worker_credentials_revoked
            or self.release.live_claim_count != 0 or self.release.claim_high_water != 1):
            raise ValueError("terminal recovery historical binding changed")
        return self


class NativeTerminalRecoveryReferenceV1(StrictV1Model):
    """A lookup selector, not authenticated node-operation evidence."""

    event_id: PositiveQuantity
    claim_id: UUID


class NativeTerminalRecoveryPageV1(StrictV1Model):
    installation_id: UUID
    after_event_id: Quantity
    through_event_id: Quantity
    attempts: Annotated[tuple[NativeTerminalRecoveryReferenceV1, ...], Field(max_length=64)]
    executable: Literal[False] = False


class NativeTerminalRecoveryStore:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("terminal recovery installation changed")
        self._session = session
        self._installation = installation

    async def discover(self, *, after_event_id: int = 0, through_event_id: int | None = None,
        limit: int = 16,
    ) -> NativeTerminalRecoveryPageV1:
        """Finite historical sweep; reset after each pass to revisit retained work."""
        if (not self._session.in_transaction() or type(after_event_id) is not int or not 0 <= after_event_id < 2**63
            or (through_event_id is not None and (type(through_event_id) is not int or not after_event_id <= through_event_id < 2**63))
            or type(limit) is not int or not 1 <= limit <= 64):
            raise ValueError("terminal recovery requires a transaction and bounded cursor")
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.discover_terminal_native_recovery(
                :installation,:installation_wire,:after,:through,:limit)"""),
                {"installation": self._installation.id, "installation_wire": self._installation.wire_payload,
                    "after": after_event_id, "through": through_event_id, "limit": limit})
            page = NativeTerminalRecoveryPageV1.model_validate_json(returned)
            if (canonical_bytes(page).decode("ascii") != returned or page.installation_id != self._installation.id
                or page.after_event_id != after_event_id or page.through_event_id < after_event_id
                or (through_event_id is not None and page.through_event_id != through_event_id)
                or len(page.attempts) > limit):
                raise ValueError("terminal recovery page binding changed")
            previous, seen = after_event_id, set()
            for attempt in page.attempts:
                if not previous < attempt.event_id <= page.through_event_id or attempt.claim_id in seen:
                    raise ValueError("terminal recovery page ordering changed")
                previous = attempt.event_id
                seen.add(attempt.claim_id)
            return page

    async def read(self, claim_operation_id: UUID) -> NativeTerminalRecoveryV1 | None:
        """Read committed terminal history without a live lease or lost secret."""
        if not self._session.in_transaction() or not isinstance(claim_operation_id, UUID):
            raise ValueError("terminal recovery requires a transaction and exact claim selector")
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.read_terminal_native_recovery(
                :installation,:installation_wire,:claim)"""),
                {"installation": self._installation.id, "installation_wire": self._installation.wire_payload,
                    "claim": claim_operation_id})
            if returned is None:
                return None
            result = NativeTerminalRecoveryV1.model_validate_json(returned)
            if (canonical_bytes(result).decode("ascii") != returned
                or result.profile.installation_id != self._installation.id
                or result.preparation.request.claim.operation_id != claim_operation_id):
                raise ValueError("terminal recovery response changed")
            return result
