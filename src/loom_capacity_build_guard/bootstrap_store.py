"""Hash-only native bootstrap registration; never a worker or build capability."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.client import (
    DemandReporterClient,
    ExecutableBootstrapAcknowledgementReceiptV2,
)
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import StrictV1Model, canonical_bytes, canonical_digest
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapAcknowledgementV2,
    ExecutableBootstrapProposalV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)


class BuildBootstrapRegistrationV1(StrictV1Model):
    installation_id: UUID
    proposal: ExecutableBootstrapProposalV2
    bootstrap_registration_epoch: Literal[1] = 1
    executable: Literal[False] = False

    @property
    def digest(self) -> str:
        return canonical_digest(self)


@dataclass(frozen=True, slots=True)
class BuildBootstrapPublication:
    acknowledgement: ExecutableBootstrapAcknowledgementV2
    idempotency_key: UUID


class BuildBootstrapCoordinator:
    """Own the registration commit and bounded authenticated delivery transaction."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *,
        installation: RetainedBuildInstallation, publisher: DemandReporterClient,
    ) -> None:
        self._sessions = session_factory
        self._installation = installation
        self._publisher = publisher

    async def register(self, proposal: ExecutableBootstrapProposalV2) -> BuildBootstrapRegistrationV1:
        async with asyncio.timeout(30), self._sessions.begin() as session:
            result = await BuildGuardBootstrapStore(session, installation=self._installation).register(proposal)
        return result

    async def publish(self, intent_id: UUID) -> ExecutableBootstrapAcknowledgementReceiptV2:
        async with asyncio.timeout(30), self._sessions.begin() as session:
            work = await BuildGuardBootstrapStore(session, installation=self._installation).authorize_publication(intent_id)
            result = await self._publisher.publish_executable_bootstrap_acknowledgement(
                work.acknowledgement, idempotency_key=work.idempotency_key)
        return result


class BuildGuardBootstrapStore:
    """Retain before acknowledgement; physical execution requires later admission.

    The installed manager/executor contract is epoch-one-only. Expired evidence
    remains immutable; retry recovery never rotates a secret under the same intent.
    """

    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build bootstrap installation receipt changed")
        self._session = session
        self._installation = document

    async def register(self, proposal: ExecutableBootstrapProposalV2) -> BuildBootstrapRegistrationV1:
        if not self._session.in_transaction():
            raise ValueError("build bootstrap requires an outer transaction")
        proposal = ExecutableBootstrapProposalV2.model_validate_json(proposal.model_dump_json())
        if proposal.proposal_epoch != 1:
            raise ValueError("build bootstrap supports epoch one only")
        wire = canonical_executable_bytes(proposal)
        async with self._session.begin_nested():
            retained_wire = await self._session.scalar(text("""SELECT loom_capacity_build_guard.register_bootstrap(
                :installation, CAST(:payload AS jsonb), :wire, :digest)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": canonical_executable_digest(proposal)})
            result = BuildBootstrapRegistrationV1.model_validate_json(retained_wire)
            if (canonical_bytes(result).decode("ascii") != retained_wire or result.proposal != proposal
                or result.installation_id != self._installation.id):
                raise ValueError("build bootstrap retained proposal changed")
            return result

    async def authorize_publication(self, intent_id: UUID) -> BuildBootstrapPublication:
        if not self._session.in_transaction():
            raise ValueError("build bootstrap publication requires an outer transaction")
        async with self._session.begin_nested():
            wire = await self._session.scalar(text("SELECT loom_capacity_build_guard.authorize_bootstrap_publication(:installation,:intent)"),
                {"installation": self._installation.id, "intent": intent_id})
            acknowledgement = ExecutableBootstrapAcknowledgementV2.model_validate_json(wire)
            binding = acknowledgement.binding
            if (canonical_executable_bytes(acknowledgement).decode("ascii") != wire or binding.intent_id != intent_id
                or binding.subject_id != self._installation.subject_id
                or binding.subject_incarnation != self._installation.subject_incarnation
                or binding.deployment_generation != self._installation.deployment_generation
                or acknowledgement.reporter_incarnation != self._installation.reporter_incarnation
                or acknowledgement.protected_admission_sha256 != self._installation.protected_admission_sha256
                or acknowledgement.proposal_epoch != 1 or acknowledgement.bootstrap_registration_epoch != 1):
                raise ValueError("build bootstrap acknowledgement binding changed")
            return BuildBootstrapPublication(acknowledgement=acknowledgement,
                idempotency_key=uuid5(NAMESPACE_URL, f"loom:protected-executable-bootstrap:{acknowledgement.bootstrap_evidence_sha256}"))
