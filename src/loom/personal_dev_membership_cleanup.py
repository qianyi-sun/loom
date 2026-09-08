"""Snapshot consistency gate before membership cleanup side effects.

The store and heartbeat runner independently fence the current reconciliation
lease. A caller-supplied digest never substitutes for their trusted stored claim.
"""

from __future__ import annotations

from loom.personal_dev_environment import PersonalDevReconciliationClaim
from loom.personal_dev_membership_checkpoint import PersonalDevMembershipEnvelopeV1
from loom_capacity_manager.contracts import canonical_bytes


def validated_membership_destroy(
    claim: PersonalDevReconciliationClaim,
    *,
    checkpoints: tuple[str, ...],
    require_release: bool = True,
) -> PersonalDevMembershipEnvelopeV1:
    operation, attempt, environment = claim.operation, claim.attempt, claim.environment
    if (
        operation.capacity_mode != "membership-v1"
        or operation.kind != "destroy"
        or operation.state != "running"
        or operation.checkpoint not in checkpoints
        or attempt.operation_id != operation.id
        or attempt.id != operation.attempt_id
        or attempt.operation_epoch != operation.operation_epoch
        or attempt.attempt_sequence != operation.attempt_sequence
        or attempt.subject_id != operation.subject_id
        or attempt.subject_incarnation != operation.subject_incarnation
        or attempt.checkpoint != operation.checkpoint
        or attempt.state != "running"
        or environment.operation_id != operation.id
        or environment.operation_epoch != operation.operation_epoch
        or environment.subject_id != operation.subject_id
        or environment.subject_incarnation != operation.subject_incarnation
        or environment.status != "deleting"
        or environment.keep_data != operation.keep_data
        or operation.capacity_membership_envelope is None
    ):
        raise ValueError("membership cleanup claim or checkpoint is invalid")
    envelope = PersonalDevMembershipEnvelopeV1.model_validate_json(
        canonical_bytes(operation.capacity_membership_envelope)
    )
    projection, observation = envelope.request.projection, envelope.observation
    if (
        projection.operation_kind != "destroy"
        or projection.operation_id != operation.id
        or projection.operation_epoch != operation.operation_epoch
        or projection.configuration_generation != operation.operation_epoch
        or projection.owner_id != operation.owner_user_id
        or projection.environment_name != operation.environment_name
        or projection.subject_id != operation.subject_id
        or projection.subject_incarnation != operation.subject_incarnation
        or projection.deployment_generation != operation.deployment_generation
        or projection.candidate_sha256 != operation.candidate_sha
        or projection.candidate_publication_sha256 != claim.candidate.publication_sha256
        or projection.local_activation_sha256 != operation.local_activation_sha256
        or projection.min_slots != 0
        or projection.max_slots != 0
        or observation.attempt_id != operation.attempt_id
        or envelope.idempotency_key != operation.idempotency_key
    ):
        raise ValueError("membership cleanup receipt belongs to another operation")
    if require_release and envelope.release is None:
        raise ValueError("membership cleanup requires a persisted authenticated release")
    return envelope
