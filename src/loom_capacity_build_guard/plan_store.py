"""Typed protected preparation receipts; preparation is not publication authority."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom.personal_dev_build_platform_requests import canonical_build_source
from loom.personal_dev_candidate import CandidateRegistration
from loom_capacity_agent.admission_convergence import ProtectedAdmissionPlanWork
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
)
from loom_capacity_manager.contracts import (
    Digest,
    Identifier,
    PositiveQuantity,
    Quantity,
    StrictV1Model,
    canonical_bytes,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableAdmissionAcknowledgementV2,
    ExecutableAdmissionPlanClosureAcknowledgementV2,
    ExecutableAdmissionPlanClosureV2,
    ExecutableAdmissionPlanProposalV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)


class BuildGuardAssignment(BaseModel):
    """Database-generated lifecycle facts, never caller-generated assignments."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    id: UUID
    request_id: UUID
    plan_id: UUID
    request_sequence: PositiveQuantity
    execution_generation: PositiveQuantity
    allowance_id: UUID
    submission_intent_id: UUID
    shape_instance_id: Identifier
    shape_slot_index: Quantity
    source_binding_sha256: Digest
    source_canonical_json: str
    runtime_installation_sha256: Digest
    lease_not_after_epoch_microseconds: PositiveQuantity


class _SQLReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    proposal_digest: Digest
    assignments: tuple[BuildGuardAssignment, ...]


class RetainedBuildClosureV1(StrictV1Model):
    """Terminal plan evidence, not evidence of released physical capacity."""

    installation_id: UUID
    closure: ExecutableAdmissionPlanClosureV2
    disposition_kind: Literal["abandoned", "never-converged"]
    assignment_ids: tuple[UUID, ...]

    @property
    def digest(self) -> str:
        return sha256(canonical_bytes(self)).hexdigest()


@dataclass(frozen=True, slots=True)
class BuildClosurePublicationWork:
    acknowledgement: ExecutableAdmissionPlanClosureAcknowledgementV2
    idempotency_key: UUID


@dataclass(frozen=True, slots=True)
class PreparedBuildPlan:
    """Exact internal receipt; no acknowledgement or execution capability."""

    installation_id: UUID
    proposal: ExecutableAdmissionPlanProposalV2
    assignments: tuple[BuildGuardAssignment, ...]

    @property
    def wire_payload(self) -> bytes:
        return json.dumps({"schema_version": 1, "installation_id": str(self.installation_id),
            "proposal": json.loads(canonical_executable_bytes(self.proposal)),
            "assignments": [item.model_dump(mode="json") for item in self.assignments]},
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")

    @property
    def digest(self) -> str:
        return sha256(self.wire_payload).hexdigest()


def _microseconds(value: datetime) -> int:
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


class BuildGuardPlanStore:
    """Use agent-only SQL within caller-owned transactions; retain locks until commit."""

    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation) -> None:
        self._session = session
        document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
        if document != installation.document or canonical_bytes(document) != installation.wire_payload:
            raise ValueError("build plan installation receipt changed")
        self._installation = document

    async def close_plan(self, closure: ExecutableAdmissionPlanClosureV2) -> RetainedBuildClosureV1:
        """Retain an authenticated manager closure; caller commits before reporting it."""
        if not self._session.in_transaction():
            raise ValueError("build closure requires an outer transaction")
        closure = ExecutableAdmissionPlanClosureV2.model_validate_json(closure.model_dump_json())
        wire = canonical_executable_bytes(closure)
        async with self._session.begin_nested():
            retained_wire = await self._session.scalar(text("""
                SELECT loom_capacity_build_guard.close_plan(:installation, CAST(:payload AS jsonb), :wire, :digest)
            """), {"installation": self._installation.id, "payload": wire.decode("ascii"),
                "wire": wire, "digest": canonical_executable_digest(closure)})
            retained = RetainedBuildClosureV1.model_validate_json(retained_wire)
            if (canonical_bytes(retained).decode("ascii") != retained_wire or retained.closure != closure
                or retained.installation_id != self._installation.id
                or len(set(retained.assignment_ids)) != len(retained.assignment_ids)
                or retained.assignment_ids != tuple(sorted(retained.assignment_ids, key=lambda value: value.int))
                or (retained.disposition_kind == "never-converged") != (not retained.assignment_ids)):
                raise ValueError("build closure retained evidence changed")
            return retained

    async def authorize_closure_publication(self, plan_id: UUID) -> BuildClosurePublicationWork:
        """Replay a previously committed terminal disposition, including after expiry."""
        if not self._session.in_transaction():
            raise ValueError("build closure publication requires an outer transaction")
        async with self._session.begin_nested():
            wire = await self._session.scalar(text(
                "SELECT loom_capacity_build_guard.authorize_closure_publication(:installation, :plan)"),
                {"installation": self._installation.id, "plan": plan_id})
            acknowledgement = ExecutableAdmissionPlanClosureAcknowledgementV2.model_validate_json(wire)
            if (canonical_executable_bytes(acknowledgement).decode("ascii") != wire
                or acknowledgement.plan_id != plan_id
                or acknowledgement.subject_id != self._installation.subject_id
                or acknowledgement.subject_incarnation != self._installation.subject_incarnation
                or acknowledgement.reporter_incarnation != self._installation.reporter_incarnation
                or acknowledgement.protected_admission_sha256 != self._installation.protected_admission_sha256):
                raise ValueError("build closure acknowledgement binding changed")
            return BuildClosurePublicationWork(acknowledgement=acknowledgement,
                idempotency_key=uuid5(NAMESPACE_URL,
                    f"loom:protected-executable-admission-cleanup:{canonical_executable_digest(acknowledgement)}"))

    async def authorize_publication(self, plan_id: UUID) -> ProtectedAdmissionPlanWork:
        """Hold source/plan locks through the caller's manager publication receipt.

        The durable disposition records authorization, not delivery confirmation.
        Lost delivery replies must retry this exact path or converge closure.
        """
        if not self._session.in_transaction():
            raise ValueError("build publication requires an outer transaction")
        async with self._session.begin_nested():
            wire = await self._session.scalar(text(
                "SELECT loom_capacity_build_guard.authorize_publication(:installation, :plan)"),
                {"installation": self._installation.id, "plan": plan_id})
            acknowledgement = ExecutableAdmissionAcknowledgementV2.model_validate_json(wire)
            if (canonical_executable_bytes(acknowledgement).decode("ascii") != wire
                or acknowledgement.plan_id != plan_id
                or acknowledgement.subject_id != self._installation.subject_id
                or acknowledgement.subject_incarnation != self._installation.subject_incarnation
                or acknowledgement.reporter_incarnation != self._installation.reporter_incarnation
                or acknowledgement.protected_admission_sha256 != self._installation.protected_admission_sha256):
                raise ValueError("build publication acknowledgement binding changed")
            return ProtectedAdmissionPlanWork(acknowledgement=acknowledgement,
                idempotency_key=uuid5(NAMESPACE_URL,
                    f"loom:protected-executable-admission:{canonical_executable_digest(acknowledgement)}"))

    async def prepare(self, proposal: ExecutableAdmissionPlanProposalV2, *,
        sources: Mapping[UUID, CandidateRegistration] | None = None,
    ) -> PreparedBuildPlan:
        if not self._session.in_transaction():
            raise ValueError("build plan preparation requires an outer transaction")
        proposal = ExecutableAdmissionPlanProposalV2.model_validate_json(proposal.model_dump_json())
        allowances = {item.protected_attempt_id: item for item in proposal.allowances}
        if not allowances or (sources is not None and set(sources) != set(allowances)):
            raise ValueError("build plan requires the complete source set")
        if sources is None:
            current = await self._session.scalar(text("SELECT loom_capacity_build_guard.read_pending_sources(:installation)"),
                {"installation": self._installation.id})
            if not isinstance(current, dict) or any(not isinstance(current.get(str(key)), dict) for key in allowances):
                raise ValueError("build plan current source set is unavailable")
            source_wire = {key: json.dumps(current[str(key)], sort_keys=True, separators=(",", ":"),
                ensure_ascii=True, allow_nan=False) for key in allowances}
        else:
            source_wire = {key: canonical_build_source(value).decode("ascii") for key, value in sources.items()}
        source_epochs = {key: json.loads(value).get("lease_epoch") for key, value in source_wire.items()}
        if any(type(epoch) is not int or not 0 < epoch < 2**63 for epoch in source_epochs.values()):
            raise ValueError("build plan current source lease epoch changed")
        wire = canonical_executable_bytes(proposal)
        # A malformed database response must not leave unusable immutable holds,
        # even if the caller catches the error and commits its outer transaction.
        async with self._session.begin_nested():
            raw = await self._session.scalar(text("""
                SELECT loom_capacity_build_guard.prepare_plan(
                    :installation, CAST(:payload AS jsonb), :wire, :digest, CAST(:sources AS jsonb))
            """), {"installation": self._installation.id, "payload": wire.decode("ascii"),
                "wire": wire, "digest": canonical_executable_digest(proposal),
                "sources": json.dumps({str(key): value for key, value in source_wire.items()})})
            receipt = _SQLReceipt.model_validate_json(json.dumps(raw, allow_nan=False))
            if (receipt.proposal_digest != canonical_executable_digest(proposal)
                or len(receipt.assignments) != len(allowances)
                or {item.request_id for item in receipt.assignments} != set(allowances)
                or len({item.id for item in receipt.assignments}) != len(allowances)):
                raise ValueError("build plan receipt assignment set or proposal changed")
            for item in receipt.assignments:
                allowance = allowances[item.request_id]
                if (item.id.int == 0 or item.plan_id != proposal.plan_id
                    or item.allowance_id != allowance.allowance_id
                    or item.submission_intent_id != allowance.submission_intent_id
                    or item.shape_instance_id != allowance.shape_instance_id
                    or item.shape_slot_index != allowance.shape_slot_index
                    or item.execution_generation != source_epochs[item.request_id]
                    or item.source_canonical_json != source_wire[item.request_id]
                    or item.source_binding_sha256 != sha256(source_wire[item.request_id].encode("ascii")).hexdigest()
                    or item.runtime_installation_sha256 != self._installation.runtime_installation_sha256
                    or item.lease_not_after_epoch_microseconds > _microseconds(proposal.lease_not_after)):
                    raise ValueError("build plan receipt assignment binding changed")
            return PreparedBuildPlan(installation_id=self._installation.id, proposal=proposal,
                assignments=tuple(sorted(receipt.assignments, key=lambda item: item.allowance_id.int)))
