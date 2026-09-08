"""Secret-free durable requests for explicitly opted-in personal membership.

These bindings validate consistency, not installer authenticity. Only the trusted
installer may produce observations; only authenticated manager transport may
produce results. Reconciler lease ownership is enforced by the lifecycle store.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.contracts import (
    Digest,
    Identifier,
    PositiveQuantity,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionAuthorityV2,
    SubjectExecutionAcknowledgementV2,
)
from loom_capacity_manager.membership_contracts import (
    PersonalApplicationMembershipMutationV1,
    PersonalApplicationMembershipResponseV1,
    PersonalMembershipCheckpointV1,
)
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.membership_outcomes import (
    PersonalMembershipOperationCommittedV1,
    PersonalMembershipOperationOutcomeQueryV1,
    PersonalMembershipOperationOutcomeV1,
    PersonalMembershipOperationTerminalNotCommittedV1,
)
from loom_capacity_manager.membership_subject_status import (
    PersonalMembershipReleaseVerifiedV1,
    PersonalMembershipSubjectQueryV1,
)

PersonalDevMembershipHistoricalOutcomeV1 = Annotated[
    PersonalMembershipOperationCommittedV1 | PersonalMembershipOperationTerminalNotCommittedV1,
    Field(discriminator="outcome"),
]


def _nonzero_identity(value: UUID) -> UUID:
    if value.int == 0:
        raise ValueError("membership observation identity must be nonzero")
    return value


class PersonalDevMembershipObservationV1(StrictV1Model):
    """Original installer observation, retained unchanged across lease takeover."""

    operation_id: UUID
    operation_epoch: PositiveQuantity
    attempt_id: UUID
    observation_lease_epoch: PositiveQuantity
    observed_at: datetime
    execution: ExecutionAuthorityV2
    local_activation_sha256: Digest
    capacity_agent_installation_sha256: Digest
    acknowledgement: SubjectExecutionAcknowledgementV2

    _identities = field_validator("operation_id", "attempt_id")(_nonzero_identity)

    @field_validator("observed_at")
    @classmethod
    def _utc_observation(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("membership observation must be timezone-aware")
        return value.astimezone(UTC)


def validate_membership_response(
    envelope: PersonalDevMembershipEnvelopeV1,
    response: PersonalApplicationMembershipResponseV1,
) -> None:
    """Verify an authenticated response belongs to this exact submitted request."""

    request = envelope.request
    projection = request.projection
    result = response.result
    member = result.member
    subject = member.configuration
    retiring = projection.operation_kind == "destroy"
    if (
        response.checkpoint.execution != request.execution
        or response.checkpoint.namespace_id != request.namespace_id
        or response.checkpoint.revision != request.expected_revision + 1
        or response.checkpoint.revision != result.revision
        or response.checkpoint.head_sha256 != result.head_sha256
        or member.revision != result.revision
        or member.owner_id != projection.owner_id
        or member.acknowledgement != request.acknowledgement
        or subject.subject_id != projection.subject_id
        or subject.subject_incarnation != projection.subject_incarnation
        or subject.display_name != f"dev-{projection.environment_name}"
        or subject.account_id != f"dev-owner-{projection.owner_id.hex}"
        or subject.tier_id != "development"
        or subject.lifecycle_state != ("disabled" if retiring else "active")
        or subject.min_slots != (0 if retiring else projection.min_slots)
        or subject.max_slots != (0 if retiring else projection.max_slots)
        or subject.candidate_generation != projection.candidate_generation
        or subject.deployment_generation != projection.deployment_generation
        or subject.configuration_generation != projection.configuration_generation
        or subject.demand_reporter_incarnation != projection.demand_reporter_incarnation
        or result.head_sha256
        != canonical_membership_event_head(
            actor=envelope.management_principal_id,
            execution_epoch=request.execution.execution_epoch,
            idempotency_key=envelope.idempotency_key,
            operation_id=projection.operation_id,
            previous_sha256=envelope.expected_checkpoint.head_sha256,
            request_digest=envelope.request_sha256,
            request_payload=request.model_dump(mode="json", exclude_none=False),
            member=member,
            revision=result.revision,
        )
    ):
        raise ValueError("membership result differs from the durable request")


def validate_membership_outcome(
    envelope: PersonalDevMembershipEnvelopeV1,
    outcome: PersonalMembershipOperationOutcomeV1,
) -> None:
    """Bind a current observer's historical answer to the entire original query."""

    query = PersonalMembershipOperationOutcomeQueryV1(
        original_actor=envelope.management_principal_id,
        idempotency_key=envelope.idempotency_key,
        request=envelope.request,
    )
    if (
        outcome.query_sha256 != canonical_digest(query)
        or outcome.request_sha256 != envelope.request_sha256
        or outcome.original_actor != query.original_actor
        or outcome.idempotency_key != query.idempotency_key
        or outcome.operation_id != query.request.projection.operation_id
        or outcome.execution_epoch != query.request.execution.execution_epoch
        or outcome.execution_manifest_sha256 != query.request.execution.execution_manifest_sha256
        or outcome.namespace_id != query.request.namespace_id
    ):
        raise ValueError("membership outcome identifies a different request")
    if isinstance(outcome, PersonalMembershipOperationCommittedV1):
        validate_membership_response(envelope, outcome.receipt)


def validate_membership_release(
    envelope: PersonalDevMembershipEnvelopeV1,
    release: PersonalMembershipReleaseVerifiedV1,
) -> None:
    """Bind verified zero-work evidence to the exact committed disabled request."""

    verified = PersonalMembershipReleaseVerifiedV1.model_validate_json(canonical_bytes(release))
    receipt = envelope.result
    if receipt is None and isinstance(
        envelope.historical_outcome, PersonalMembershipOperationCommittedV1
    ):
        receipt = envelope.historical_outcome.receipt
    if receipt is None or envelope.request.projection.operation_kind != "destroy":
        raise ValueError("membership release requires a committed destroy request")
    query = PersonalMembershipSubjectQueryV1(membership_receipt=receipt)
    if verified.query_sha256 != canonical_digest(query):
        raise ValueError("membership release identifies a different query")
    validate_membership_response(envelope, verified.membership_receipt)


class PersonalDevMembershipEnvelopeV1(StrictV1Model):
    """Full canonical outbound request and original observation, not just digests."""

    mode: Literal["membership-v1"] = "membership-v1"
    management_principal_id: Identifier
    idempotency_key: UUID
    expected_checkpoint: PersonalMembershipCheckpointV1
    request: PersonalApplicationMembershipMutationV1
    request_sha256: Digest
    observation: PersonalDevMembershipObservationV1
    result: PersonalApplicationMembershipResponseV1 | None = None
    historical_outcome: PersonalDevMembershipHistoricalOutcomeV1 | None = None
    release: PersonalMembershipReleaseVerifiedV1 | None = None

    @model_validator(mode="after")
    def _exact_bindings(self) -> PersonalDevMembershipEnvelopeV1:
        request = self.request
        projection = request.projection
        observation = self.observation
        acknowledgement = request.acknowledgement
        if (
            canonical_digest(request) != self.request_sha256
            or request.execution != self.expected_checkpoint.execution
            or request.namespace_id != self.expected_checkpoint.namespace_id
            or request.expected_revision != self.expected_checkpoint.revision
            or request.execution.execution_state != "active"
            or projection.expected_configuration_epoch != request.execution.configuration_epoch
            or observation.execution != request.execution
            or observation.operation_id != projection.operation_id
            or observation.operation_epoch != projection.operation_epoch
            or observation.local_activation_sha256 != projection.local_activation_sha256
            or observation.capacity_agent_installation_sha256
            != projection.capacity_agent_installation_sha256
            or observation.acknowledgement != acknowledgement
            or acknowledgement.subject_id != projection.subject_id
            or acknowledgement.subject_incarnation != projection.subject_incarnation
            or acknowledgement.configuration_generation != projection.configuration_generation
            or acknowledgement.deployment_generation != projection.deployment_generation
            or acknowledgement.reporter_incarnation != projection.demand_reporter_incarnation
            or acknowledgement.protected_admission_sha256 != projection.protected_admission_sha256
            or acknowledgement.candidate.algorithm != "source-sha256"
            or acknowledgement.candidate.identity != projection.candidate_sha256
            or acknowledgement.candidate.publication_sha256
            != projection.candidate_publication_sha256
        ):
            raise ValueError("membership request differs from its checkpoint or observation")
        if self.result is not None:
            if self.historical_outcome is not None:
                raise ValueError("historical outcome is not current membership acceptance")
            validate_membership_response(self, self.result)
        if self.historical_outcome is not None:
            validate_membership_outcome(self, self.historical_outcome)
        if self.release is not None:
            validate_membership_release(self, self.release)
        return self


def refresh_membership_checkpoint(
    envelope: PersonalDevMembershipEnvelopeV1,
    checkpoint: PersonalMembershipCheckpointV1,
) -> PersonalDevMembershipEnvelopeV1:
    """Refresh only after a typed revision conflict, never after an ambiguous send.

    This pure transformation grants no permission to retry. The lease-fenced store
    persists it before network mutation; the reconciler controls the conflict path.
    """

    if (
        envelope.result is not None
        or envelope.historical_outcome is not None
        or checkpoint.execution != envelope.expected_checkpoint.execution
        or checkpoint.namespace_id != envelope.expected_checkpoint.namespace_id
        or checkpoint.revision <= envelope.expected_checkpoint.revision
    ):
        raise ValueError("membership refresh changed authority or did not advance revision")
    request = envelope.request.model_copy(update={"expected_revision": checkpoint.revision})
    return PersonalDevMembershipEnvelopeV1.model_validate(
        envelope.model_dump(mode="python")
        | {
            "expected_checkpoint": checkpoint,
            "request": request,
            "request_sha256": canonical_digest(request),
        }
    )
