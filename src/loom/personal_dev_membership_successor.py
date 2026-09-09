"""Reviewed cross-authority continuation, never an implicit historical import.

These pure checks grant no manager or database authority. The lifecycle must
authenticate the current checkpoint and persist a fresh linked operation under
its lease before running the ordinary installation/membership protocol.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal
from uuid import UUID

from pydantic import field_validator, model_validator

from loom.personal_dev_environment import (
    PersonalDevEnvironmentApplyRequest,
    PersonalDevEnvironmentDestroyRequest,
    PersonalDevLifecycleOperationRecord,
    PersonalDevReconciliationClaim,
)
from loom.personal_dev_membership_admission import PersonalDevMembershipAcceptanceBindingV1
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    ConfigurationSnapshotV1,
    Digest,
    DynamicDevelopmentSubjectProjectionV1,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
    checked_add,
)
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.membership_outcomes import PersonalMembershipOperationCommittedV1


class PersonalDevMembershipSuccessorBindingV1(StrictV1Model):
    """Protected operator input, pinned to the complete historical owner request."""

    reason: Literal["historical-membership-authority-transition"] = (
        "historical-membership-authority-transition"
    )
    authority: PersonalDevMembershipAcceptanceBindingV1
    reviewed_at: datetime
    expires_at: datetime
    current_configuration: ConfigurationSnapshotV1
    predecessor_operation_id: UUID
    predecessor_envelope_sha256: Digest
    owner_team_id: UUID
    request_sha256: Digest
    adopted_member: PersonalApplicationMemberV1 | None
    accepted_operation_id: UUID | None = None
    accepted_membership_envelope_sha256: Digest | None = None
    accepted_shadow_configuration: ConfigurationSnapshotV1 | None = None
    accepted_shadow_projection: DynamicDevelopmentSubjectProjectionV1 | None = None

    @field_validator("reviewed_at", "expires_at")
    @classmethod
    def _utc_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("successor review time must be timezone-aware")
        return value.astimezone(UTC)

    @field_validator("predecessor_operation_id", "owner_team_id")
    @classmethod
    def _nonzero_id(cls, value: UUID) -> UUID:
        if value.int == 0:
            raise ValueError("successor identities must be nonzero")
        return value

    @field_validator("predecessor_envelope_sha256", "request_sha256")
    @classmethod
    def _nonzero_digest(cls, value: str) -> str:
        if value == "0" * 64:
            raise ValueError("successor evidence digest must be nonzero")
        return value

    @model_validator(mode="after")
    def _reviewed_adoption(self) -> PersonalDevMembershipSuccessorBindingV1:
        preparation = self.authority.preparation
        snapshot = self.current_configuration
        if not timedelta(0) < self.expires_at - self.reviewed_at <= timedelta(hours=24):
            raise ValueError("successor review must have a positive window of at most 24 hours")
        if (self.accepted_shadow_configuration is None) != (
            self.accepted_shadow_projection is None
        ):
            raise ValueError("successor shadow evidence must contain both snapshot and projection")
        if (
            snapshot.configuration_epoch != preparation.configuration_epoch
            or snapshot.fleet.generation != preparation.fleet_generation
            or snapshot.fleet.digest != preparation.fleet_digest
        ):
            raise ValueError("successor configuration differs from prepared authority")
        member = self.adopted_member
        if member is not None:
            subject = member.configuration
            reference = next(
                (item for item in snapshot.subjects if item.subject_id == subject.subject_id), None
            )
            acknowledgement = next(
                (
                    item
                    for item in preparation.subject_acknowledgements
                    if item.subject_id == subject.subject_id
                ),
                None,
            )
            if (
                subject.lifecycle_state != "active"
                or subject.subject_id
                not in preparation.personal_membership.managed_base_subject_ids
                or reference is None
                or reference.subject_incarnation != subject.subject_incarnation
                or reference.generation != subject.configuration_generation
                or reference.digest != canonical_digest(subject)
                or acknowledgement != member.acknowledgement
                or subject.account_id != f"dev-owner-{member.owner_id.hex}"
                or subject.tier_id != "development"
            ):
                raise ValueError("successor requires exact active managed-base adoption")
        return self


@dataclass(frozen=True, slots=True)
class PersonalDevMembershipSuccessorDecision:
    kind: Literal["create", "update", "destroy"]
    operation_epoch: int
    deployment_generation: int


def parse_membership_successor_binding(
    payload: bytes | str, *, expected_binding_sha256: str
) -> PersonalDevMembershipSuccessorBindingV1:
    encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
    if not isinstance(encoded, bytes) or not 0 < len(encoded) <= MAX_CONTRACT_BYTES:
        raise ValueError("successor binding exceeds its byte bound")
    try:
        binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(encoded)
        if (
            canonical_bytes(binding) != encoded
            or canonical_digest(binding) != expected_binding_sha256
        ):
            raise ValueError("successor binding differs from the canonical reviewed document")
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("successor binding is invalid") from exc
    return binding


def _intent_digest(operation: PersonalDevLifecycleOperationRecord) -> str:
    if operation.kind == "destroy":
        return PersonalDevEnvironmentDestroyRequest(
            name=operation.environment_name,
            owner_user_id=operation.owner_user_id,
            owner_team_id=operation.owner_team_id,
            expected_operation_epoch=operation.expected_operation_epoch,
            idempotency_key=operation.idempotency_key,
            keep_data=operation.keep_data,
        ).request_sha256
    return PersonalDevEnvironmentApplyRequest(
        name=operation.environment_name,
        owner_user_id=operation.owner_user_id,
        owner_team_id=operation.owner_team_id,
        candidate_id=operation.candidate_id,
        candidate_sha=operation.candidate_sha,
        min_slots=operation.min_slots,
        max_slots=operation.max_slots,
        expected_operation_epoch=operation.expected_operation_epoch,
        idempotency_key=operation.idempotency_key,
    ).request_sha256


def _validate_projection_intent(
    operation: PersonalDevLifecycleOperationRecord,
    projection: DynamicDevelopmentSubjectProjectionV1,
) -> None:
    expected_slots = (0, 0) if operation.kind == "destroy" else (
        operation.min_slots, operation.max_slots,
    )
    if (
        _intent_digest(operation) != operation.request_sha256
        or projection.operation_id != operation.id
        or projection.operation_epoch != operation.operation_epoch
        or projection.configuration_generation != operation.operation_epoch
        or projection.operation_kind != operation.kind
        or projection.subject_id != operation.subject_id
        or projection.subject_incarnation != operation.subject_incarnation
        or projection.owner_id != operation.owner_user_id
        or projection.environment_name != operation.environment_name
        or projection.candidate_sha256 != operation.candidate_sha
        or projection.candidate_generation != operation.deployment_generation
        or projection.deployment_generation != operation.deployment_generation
        or (projection.min_slots, projection.max_slots) != expected_slots
        or projection.demand_reporter_incarnation != operation.capacity_reporter_incarnation
        or projection.demand_reporter_token_sha256 != operation.capacity_reporter_token_sha256
        or projection.local_activation_sha256 != operation.local_activation_sha256
        or projection.protected_admission_sha256 != operation.protected_admission_sha256
        or projection.capacity_agent_installation_sha256
        != operation.capacity_agent_installation_sha256
        or projection.supported_pool_ids != operation.capacity_supported_pool_ids
        or projection.supported_architectures != operation.capacity_supported_architectures
    ):
        raise ValueError("successor projection differs from durable owner intent and evidence")


def _validate_envelope_intent(
    operation: PersonalDevLifecycleOperationRecord,
    envelope: PersonalDevMembershipEnvelopeV1,
) -> None:
    _validate_projection_intent(operation, envelope.request.projection)
    if (
        envelope.idempotency_key != operation.idempotency_key
        or envelope.observation.attempt_id != operation.attempt_id
    ):
        raise ValueError("successor envelope differs from durable operation intent")


_RETAINED_FIELDS = (
    "capacity_reporter_incarnation", "capacity_reporter_token_sha256",
    "local_activation_sha256", "protected_admission_sha256",
    "capacity_agent_installation_sha256", "capacity_supported_pool_ids",
    "capacity_supported_architectures",
)


def validate_membership_successor(
    binding: PersonalDevMembershipSuccessorBindingV1,
    *,
    claim: PersonalDevReconciliationClaim,
    accepted_operation: PersonalDevLifecycleOperationRecord | None,
    now: datetime,
) -> PersonalDevMembershipSuccessorDecision:
    """Choose the ordinary transition without generating IDs or changing history.

    For terminal noncommit updates, accepted_operation is loaded independently
    from durable accepted history, never supplied by the owner or inferred from
    an unsuccessful request. The manager still decides current existence and
    globally unused identity under its own authority lock.
    """
    binding = PersonalDevMembershipSuccessorBindingV1.model_validate_json(canonical_bytes(binding))
    operation, environment = claim.operation, claim.environment
    saved = operation.capacity_membership_envelope
    if (
        operation.capacity_mode != "membership-v1"
        or operation.checkpoint != "membership_outcome_resolved"
        or claim.attempt.checkpoint != "membership_outcome_resolved"
        or operation.state not in {"running", "activating"}
        or saved is None
        or saved.result is not None
        or saved.historical_outcome is None
        or saved.release is not None
    ):
        raise ValueError("successor requires a resolved historical membership operation")
    envelope = PersonalDevMembershipEnvelopeV1.model_validate_json(canonical_bytes(saved))
    projection = envelope.request.projection
    _validate_envelope_intent(operation, envelope)
    if (
        now.tzinfo is None
        or now.utcoffset() is None
        or not binding.reviewed_at <= now < binding.expires_at
        or (
            operation.kind != "destroy"
            and not binding.authority.started_at <= now < binding.authority.expires_at
        )
        or binding.authority.execution == envelope.request.execution
        or binding.predecessor_operation_id != operation.id
        or binding.predecessor_envelope_sha256 != canonical_digest(envelope)
        or binding.owner_team_id != operation.owner_team_id
        or binding.request_sha256 != operation.request_sha256
        or environment.operation_id != operation.id
        or environment.operation_epoch != operation.operation_epoch
        or environment.subject_id != operation.subject_id
        or environment.subject_incarnation != operation.subject_incarnation
        or environment.owner_user_id != operation.owner_user_id
        or environment.owner_team_id != operation.owner_team_id
        or claim.attempt.id != operation.attempt_id
        or claim.attempt.operation_id != operation.id
        or claim.attempt.operation_epoch != operation.operation_epoch
        or claim.attempt.subject_id != operation.subject_id
        or claim.attempt.subject_incarnation != operation.subject_incarnation
        or claim.attempt.attempt_sequence != operation.attempt_sequence
        or claim.candidate.id != operation.candidate_id
        or claim.candidate.candidate_sha != operation.candidate_sha
        or claim.candidate.owner_user_id != operation.owner_user_id
        or claim.candidate.owner_team_id != operation.owner_team_id
        or claim.candidate.publication_sha256 != projection.candidate_publication_sha256
        or (operation.kind != "destroy" and claim.candidate.status != "ready")
    ):
        raise ValueError("successor differs from reviewed historical owner intent")
    historical = envelope.historical_outcome
    member = binding.adopted_member
    if isinstance(historical, PersonalMembershipOperationCommittedV1):
        if (
            binding.accepted_shadow_configuration is not None
            or binding.accepted_operation_id is not None
            or binding.accepted_membership_envelope_sha256 is not None
        ):
            raise ValueError("historical membership commit cannot use shadow acceptance")
        if operation.kind == "destroy":
            raise ValueError("committed destroy must use existing release recovery")
        if member != historical.receipt.result.member:
            raise ValueError("successor adoption differs from historical commit")
    elif operation.kind == "create":
        if (
            member is not None
            or binding.accepted_shadow_configuration is not None
            or accepted_operation is not None
            or environment.ready_at is not None
            or environment.accepted_capacity_membership_checkpoint is not None
            or environment.capacity_configuration_epoch is not None
            or environment.capacity_configuration_sha256 is not None
            or environment.accepted_capacity_mode != "shadow-v1"
            or binding.accepted_operation_id is not None
            or binding.accepted_membership_envelope_sha256 is not None
            or any(getattr(environment, field) is not None for field in _RETAINED_FIELDS)
            or operation.subject_id
            in binding.authority.preparation.personal_membership.managed_base_subject_ids
            or any(
                item.subject_id == operation.subject_id
                or item.subject_incarnation == operation.subject_incarnation
                for item in binding.current_configuration.subjects
            )
            or any(
                item.subject_id == operation.subject_id
                or item.subject_incarnation == operation.subject_incarnation
                for item in binding.authority.preparation.subject_acknowledgements
            )
        ):
            raise ValueError("first-create successor cannot assume committed identity is absent")
    else:
        previous = accepted_operation
        if (
            previous is None
            or previous.state != "succeeded"
            or previous.checkpoint != "complete"
            or previous.kind not in {"create", "update", "capacity"}
            or member is None
        ):
            raise ValueError("successor requires independently retained accepted membership")
        if (
            previous.subject_id != operation.subject_id
            or previous.subject_incarnation != operation.subject_incarnation
            or previous.owner_user_id != operation.owner_user_id
            or previous.owner_team_id != operation.owner_team_id
            or previous.environment_name != operation.environment_name
            or previous.operation_epoch >= operation.operation_epoch
            or binding.accepted_operation_id != previous.id
            or environment.candidate_id != previous.candidate_id
            or environment.candidate_sha != previous.candidate_sha
            or environment.deployment_generation != previous.deployment_generation
            or environment.min_slots != previous.min_slots
            or environment.max_slots != previous.max_slots
            or any(
                getattr(environment, field) != getattr(previous, field)
                for field in _RETAINED_FIELDS
            )
        ):
            raise ValueError("successor adoption differs from locally accepted history")
        if previous.capacity_mode == "membership-v1":
            accepted = previous.capacity_membership_envelope
            if accepted is None or binding.accepted_shadow_configuration is not None:
                raise ValueError("successor accepted history has no membership receipt")
            accepted = PersonalDevMembershipEnvelopeV1.model_validate_json(
                canonical_bytes(accepted)
            )
            _validate_envelope_intent(previous, accepted)
            if canonical_digest(accepted) != binding.accepted_membership_envelope_sha256:
                raise ValueError("successor accepted receipt differs from operator review")
            accepted_projection = accepted.request.projection
            receipt = accepted.result
            if (
                receipt is None
                or member != receipt.result.member
                or environment.accepted_capacity_mode != "membership-v1"
                or environment.accepted_capacity_membership_checkpoint != receipt.checkpoint
                or environment.capacity_configuration_epoch is not None
                or environment.capacity_configuration_sha256 is not None
            ):
                raise ValueError("successor adoption differs from locally accepted membership")
        elif previous.capacity_mode == "shadow-v1":
            if binding.accepted_membership_envelope_sha256 is not None:
                raise ValueError("shadow acceptance cannot use a membership source receipt")
            _validate_shadow_adoption(binding, claim=claim, previous=previous, member=member)
            assert binding.accepted_shadow_projection is not None
            accepted_projection = binding.accepted_shadow_projection
        else:
            raise ValueError("successor accepted capacity mode is unsupported")
        if operation.kind in {"capacity", "destroy"}:
            # Only capacity/configuration intent can change on a retained deployment.
            varying = {
                "expected_configuration_epoch", "operation_kind", "operation_id",
                "operation_epoch", "configuration_generation", "min_slots", "max_slots",
            }
            if (
                operation.candidate_id != previous.candidate_id
                or projection.model_dump(exclude=varying)
                != accepted_projection.model_dump(exclude=varying)
            ):
                raise ValueError("successor retirement or capacity request changed retained deployment")
    if member is not None and (
        member.configuration.subject_id != operation.subject_id
        or member.configuration.subject_incarnation != operation.subject_incarnation
        or member.configuration.display_name != f"dev-{operation.environment_name}"
        or member.owner_id != operation.owner_user_id
        or member.configuration.configuration_generation > operation.operation_epoch
    ):
        raise ValueError("successor adopted subject identity or generation changed")
    kind: Literal["create", "update", "destroy"] = (
        "destroy" if operation.kind == "destroy" else "create" if member is None else "update"
    )
    return PersonalDevMembershipSuccessorDecision(
        kind=kind,
        operation_epoch=checked_add(operation.operation_epoch, 1),
        deployment_generation=(
            member.configuration.deployment_generation
            if kind == "destroy" and member is not None
            else checked_add(
                max(
                    operation.deployment_generation,
                    0 if member is None else member.configuration.deployment_generation,
                ),
                1,
            )
        ),
    )


def _validate_shadow_adoption(
    binding: PersonalDevMembershipSuccessorBindingV1,
    *,
    claim: PersonalDevReconciliationClaim,
    previous: PersonalDevLifecycleOperationRecord,
    member: PersonalApplicationMemberV1,
) -> None:
    """A digest is useful only with the complete original accepted documents."""
    snapshot, projection = binding.accepted_shadow_configuration, binding.accepted_shadow_projection
    environment = claim.environment
    if snapshot is None or projection is None:
        raise ValueError("successor requires complete retained shadow evidence")
    _validate_projection_intent(previous, projection)
    subject, ack = member.configuration, member.acknowledgement
    reference = next(
        (item for item in snapshot.subjects if item.subject_id == subject.subject_id), None
    )
    if (
        previous.capacity_membership_envelope is not None
        or canonical_digest(snapshot) != previous.capacity_configuration_sha256
        or snapshot.configuration_epoch != previous.capacity_configuration_epoch
        or snapshot.configuration_epoch != projection.expected_configuration_epoch + 1
        or canonical_digest(projection) != previous.capacity_projection_request_sha256
        or environment.accepted_capacity_mode != "shadow-v1"
        or environment.accepted_capacity_membership_checkpoint is not None
        or environment.capacity_configuration_epoch != previous.capacity_configuration_epoch
        or environment.capacity_configuration_sha256 != previous.capacity_configuration_sha256
        or reference is None
        or reference.subject_incarnation != subject.subject_incarnation
        or reference.generation != subject.configuration_generation
        or reference.digest != canonical_digest(subject)
        or subject.configuration_generation != projection.configuration_generation
        or subject.deployment_generation != projection.deployment_generation
        or subject.candidate_generation != projection.candidate_generation
        or subject.min_slots != projection.min_slots
        or subject.max_slots != projection.max_slots
        or subject.demand_reporter_incarnation != projection.demand_reporter_incarnation
        or ack.candidate.identity != projection.candidate_sha256
        or ack.candidate.publication_sha256 != projection.candidate_publication_sha256
        or ack.protected_admission_sha256 != projection.protected_admission_sha256
    ):
        raise ValueError("successor adoption differs from complete shadow acceptance")
