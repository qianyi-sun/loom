"""Durable native claim-revocation reporting, never physical capacity release."""

import asyncio

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom_capacity_agent.admission import (
    ProtectedReleasePublicationCheckpointV2,
    PublishableExecutableProtectedReleaseV2,
)
from loom_capacity_agent.client import ExecutableProtectedReleasePublishReceiptV2
from loom_capacity_agent.executable_release_reporter import (
    ExecutableProtectedReleasePublisher,
    stable_release_publication_key,
)
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)


class BuildGuardReleaseOutbox:
    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build release installation receipt changed")
        self._session = session
        self._installation = document

    def _publication(self, value: PublishableExecutableProtectedReleaseV2) -> PublishableExecutableProtectedReleaseV2:
        value = PublishableExecutableProtectedReleaseV2.model_validate_json(value.model_dump_json())
        release = value.release
        binding = release.binding
        installation = self._installation
        if (value.event_kind not in {"withdrawn", "prepared-revoked"}
            or binding.subject_id != installation.subject_id or binding.subject_incarnation != installation.subject_incarnation
            or binding.deployment_generation != installation.deployment_generation
            or binding.candidate_generation != installation.candidate_generation
            or binding.candidate != installation.runtime.candidate
            or release.reporter_incarnation != installation.reporter_incarnation):
            raise ValueError("build release publication installation binding changed")
        return value

    async def read_next(self) -> PublishableExecutableProtectedReleaseV2 | None:
        if not self._session.in_transaction():
            raise ValueError("build release outbox requires an outer transaction")
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("SELECT loom_capacity_build_guard.read_next_protected_release(:installation)"),
                {"installation": self._installation.id})
            if returned is None:
                return None
            publication = self._publication(PublishableExecutableProtectedReleaseV2.model_validate_json(returned))
            if canonical_executable_bytes(publication).decode("ascii") != returned:
                raise ValueError("build release publication canonical receipt changed")
            return publication

    async def acknowledge(self, publication: PublishableExecutableProtectedReleaseV2, *,
        manager_acknowledgement_digest: str,
    ) -> ProtectedReleasePublicationCheckpointV2:
        if not self._session.in_transaction():
            raise ValueError("build release acknowledgement requires an outer transaction")
        publication = self._publication(publication)
        if manager_acknowledgement_digest != publication.publication_digest:
            raise ValueError("build release manager acknowledgement digest changed")
        wire = canonical_executable_bytes(publication)
        expected = ProtectedReleasePublicationCheckpointV2(event_id=publication.event_id, event_kind=publication.event_kind,
            publication_digest=publication.publication_digest, manager_acknowledgement_digest=manager_acknowledgement_digest)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.acknowledge_protected_release(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:manager_digest)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": canonical_executable_digest(publication), "manager_digest": manager_acknowledgement_digest})
            receipt = ProtectedReleasePublicationCheckpointV2.model_validate_json(returned)
            if receipt != expected or canonical_executable_bytes(receipt).decode("ascii") != returned:
                raise ValueError("build release acknowledgement receipt changed")
            return receipt


class BuildReleaseCoordinator:
    """One replay-safe publication step; successful reporting is not readiness."""

    def __init__(self, *, session_factory: async_sessionmaker[AsyncSession], installation: RetainedBuildInstallation,
        publisher: ExecutableProtectedReleasePublisher, timeout_seconds: float = 30,
    ) -> None:
        if not 0 < timeout_seconds <= 300:
            raise ValueError("build release timeout must be between zero and 300 seconds")
        self._sessions = session_factory
        self._installation = installation
        self._publisher = publisher
        self._timeout = timeout_seconds

    async def publish_next(self) -> ProtectedReleasePublicationCheckpointV2 | None:
        async with asyncio.timeout(self._timeout):
            async with self._sessions.begin() as session:
                publication = await BuildGuardReleaseOutbox(session, installation=self._installation).read_next()
            if publication is None:
                return None
            response = await self._publisher.publish_executable_protected_release(publication,
                idempotency_key=stable_release_publication_key(publication))
            if not isinstance(response, ExecutableProtectedReleasePublishReceiptV2):
                raise ValueError("build release manager receipt is not typed evidence")
            response = ExecutableProtectedReleasePublishReceiptV2.model_validate_json(response.model_dump_json())
            if (response.intent_id != publication.release.binding.intent_id
                or response.protected_release_sha256 != publication.release.protected_release_sha256
                or response.receipt_digest != publication.publication_digest):
                raise ValueError("build release manager receipt binding changed")
            async with self._sessions.begin() as session:
                return await BuildGuardReleaseOutbox(session, installation=self._installation).acknowledge(
                    publication, manager_acknowledgement_digest=response.receipt_digest)
