"""Transaction-owning management adapter for protected build-plan convergence.

Use the existing authenticated DemandReporterClient as publisher. Preparation and
closure commit before their acknowledgements can escape. This adapter supplies no
readiness, bootstrap, build capability, source staging or physical-release proof.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Protocol
from uuid import UUID

from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from loom.personal_dev_candidate import CandidateRegistration
from loom_capacity_agent.client import (
    ExecutableAdmissionAcknowledgementReceiptV2,
    ExecutableAdmissionPlanClosureAcknowledgementReceiptV2,
)
from loom_capacity_build_guard.installation_store import RetainedBuildInstallation
from loom_capacity_build_guard.plan_store import (
    BuildGuardPlanStore,
    PreparedBuildPlan,
    RetainedBuildClosureV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAcknowledgementV2,
    ExecutableAdmissionPlanClosureAcknowledgementV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    canonical_executable_digest,
)


class BuildPlanPublisher(Protocol):
    async def publish_executable_admission_acknowledgement(self,
        acknowledgement: ExecutableAdmissionAcknowledgementV2, *, idempotency_key: UUID,
    ) -> ExecutableAdmissionAcknowledgementReceiptV2: ...

    async def publish_executable_admission_closure_acknowledgement(self,
        acknowledgement: ExecutableAdmissionPlanClosureAcknowledgementV2, *, idempotency_key: UUID,
    ) -> ExecutableAdmissionPlanClosureAcknowledgementReceiptV2: ...


class BuildPlanCoordinator:
    """Commit protected state, then publish under locks with bounded response time.

    A failed/lost delivery retries publish(plan_id), not a new preparation. If
    current source authority expires, converge the manager's exact closure.
    Never retry by replacing the proposal, freeing holds, or fabricating success.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession], *,
        installation: RetainedBuildInstallation, publisher: BuildPlanPublisher,
        operation_timeout_seconds: float = 30.0,
    ) -> None:
        if (isinstance(operation_timeout_seconds, bool)
            or not isinstance(operation_timeout_seconds, (int, float))
            or not 0.05 <= operation_timeout_seconds <= 60):
            raise ValueError("build coordinator operation timeout must be between 0.05 and 60 seconds")
        self._sessions = session_factory
        self._installation = installation
        self._publisher = publisher
        self._timeout = operation_timeout_seconds

    async def prepare(self, proposal: ExecutableAdmissionPlanProposalV2, *,
        sources: Mapping[UUID, CandidateRegistration],
    ) -> PreparedBuildPlan:
        async with asyncio.timeout(self._timeout), self._sessions.begin() as session:
            prepared = await BuildGuardPlanStore(session, installation=self._installation).prepare(proposal, sources=sources)
        return prepared

    async def publish(self, plan_id: UUID) -> ExecutableAdmissionAcknowledgementReceiptV2:
        async with asyncio.timeout(self._timeout), self._sessions.begin() as session:
            work = await BuildGuardPlanStore(session, installation=self._installation).authorize_publication(plan_id)
            result = await self._publisher.publish_executable_admission_acknowledgement(
                work.acknowledgement, idempotency_key=work.idempotency_key)
            if not isinstance(result, ExecutableAdmissionAcknowledgementReceiptV2):
                raise ValueError("build publication manager receipt is not typed evidence")
            result = ExecutableAdmissionAcknowledgementReceiptV2.model_validate_json(result.model_dump_json())
            if (result.proposal_id != work.acknowledgement.proposal_id
                or result.prepared_plan_digest != work.acknowledgement.prepared_plan_digest
                or result.receipt_digest != canonical_executable_digest(work.acknowledgement)):
                raise ValueError("build publication manager receipt binding changed")
        return result

    async def close(self, closure: ExecutableAdmissionPlanClosureV2) -> RetainedBuildClosureV1:
        async with asyncio.timeout(self._timeout), self._sessions.begin() as session:
            closed = await BuildGuardPlanStore(session, installation=self._installation).close_plan(closure)
        return closed

    async def reconcile_closure(self, closure: ExecutableAdmissionPlanClosureV2) -> ExecutableAdmissionPlanClosureAcknowledgementReceiptV2:
        """Prefer the retained reason/ID if the manager's current reason changed."""
        closure = ExecutableAdmissionPlanClosureV2.model_validate_json(closure.model_dump_json())
        proposal_digest = canonical_executable_digest(closure.proposal)
        try:
            return await self.publish_closure(closure.proposal.plan_id, expected_proposal_digest=proposal_digest)
        except DBAPIError as exc:
            # Only the protected missing-plan/missing-closure result permits
            # retention. Authorization drift, transport loss and receipt errors
            # must not be mistaken for absence or replace terminal evidence.
            if getattr(exc.orig, "sqlstate", None) != "P0002":
                raise
        await self.close(closure)
        return await self.publish_closure(closure.proposal.plan_id, expected_proposal_digest=proposal_digest)

    async def publish_closure(self, plan_id: UUID, *,
        expected_proposal_digest: str | None = None,
    ) -> ExecutableAdmissionPlanClosureAcknowledgementReceiptV2:
        async with asyncio.timeout(self._timeout), self._sessions.begin() as session:
            work = await BuildGuardPlanStore(session, installation=self._installation).authorize_closure_publication(plan_id)
            if expected_proposal_digest is not None and work.acknowledgement.proposal_digest != expected_proposal_digest:
                raise ValueError("build cleanup retained proposal differs from manager work")
            result = await self._publisher.publish_executable_admission_closure_acknowledgement(
                work.acknowledgement, idempotency_key=work.idempotency_key)
            if not isinstance(result, ExecutableAdmissionPlanClosureAcknowledgementReceiptV2):
                raise ValueError("build cleanup manager receipt is not typed evidence")
            result = ExecutableAdmissionPlanClosureAcknowledgementReceiptV2.model_validate_json(result.model_dump_json())
            if (result.closure_id != work.acknowledgement.closure_id
                or result.disposition_kind != work.acknowledgement.disposition_kind
                or result.disposition_digest != work.acknowledgement.disposition_digest
                or result.receipt_digest != canonical_executable_digest(work.acknowledgement)):
                raise ValueError("build cleanup manager receipt binding changed")
        return result
