"""Operator-pinned managed application origins, independent of mutable DB rows.

These are consistency contracts, not proof of installation or current admission.
The preparation owner must authenticate their provenance before pinning them;
consumers join the full configuration to its immutable generation reference.
No fabricated membership revision or predecessor event is assigned to a base.
"""

from __future__ import annotations

from uuid import UUID

from pydantic import field_validator, model_validator

from loom_capacity_manager.contracts import (
    DynamicDevelopmentSubjectProjectionV1,
    StrictV1Model,
    SubjectConfigurationV1,
    canonical_bytes,
)
from loom_capacity_manager.executable_contracts import (
    SubjectExecutionAcknowledgementV2,
    canonical_executable_bytes,
)


class ManagedApplicationOriginV1(StrictV1Model):
    configuration: SubjectConfigurationV1
    installation_projection: DynamicDevelopmentSubjectProjectionV1
    base_projection: DynamicDevelopmentSubjectProjectionV1
    acknowledgement: SubjectExecutionAcknowledgementV2

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("managed application origin schema must be integer 1")
        return value

    @model_validator(mode="after")
    def _installation_binding(self) -> ManagedApplicationOriginV1:
        base, installed = self.base_projection, self.installation_projection
        subject, ack = self.configuration, self.acknowledgement
        for projection in (base, installed):
            if any(not isinstance(value, UUID) or value.int == 0 for value in (
                projection.operation_id, projection.subject_id, projection.subject_incarnation,
                projection.owner_id, projection.demand_reporter_incarnation,
            )) or any(value == "0" * 64 for value in (
                projection.candidate_sha256, projection.candidate_publication_sha256,
                projection.demand_reporter_token_sha256,
            )):
                raise ValueError("managed application origin requires nonzero identities")
        # An operation's input configuration epoch is historical, not the epoch
        # importing it. Capacity/teardown may follow the installation across epochs.
        mutable = {"expected_configuration_epoch", "operation_kind", "operation_id", "operation_epoch",
            "configuration_generation", "min_slots", "max_slots"}
        if (
            installed.operation_kind not in {"create", "update"}
            or installed.expected_configuration_epoch > base.expected_configuration_epoch
            or installed.model_dump_json(exclude=mutable) != base.model_dump_json(exclude=mutable)
            or (base.operation_kind in {"create", "update"} and canonical_bytes(base) != canonical_bytes(installed))
            or (base.operation_kind in {"capacity", "destroy"} and (
                base.configuration_generation <= installed.configuration_generation
                or base.operation_id == installed.operation_id
            ))
        ):
            raise ValueError("managed application installation origin changed")
        if (
            subject.subject_id != base.subject_id or subject.subject_incarnation != base.subject_incarnation
            or subject.account_id != f"dev-owner-{base.owner_id.hex}" or subject.tier_id != "development"
            or subject.display_name != f"dev-{base.environment_name}"
            or subject.configuration_generation != base.configuration_generation
            or subject.candidate_generation != base.candidate_generation
            or subject.deployment_generation != base.deployment_generation
            or subject.demand_reporter_incarnation != base.demand_reporter_incarnation
            or subject.lifecycle_state != ("disabled" if base.operation_kind == "destroy" else "active")
            or subject.min_slots != (0 if base.operation_kind == "destroy" else base.min_slots)
            or subject.max_slots != (0 if base.operation_kind == "destroy" else base.max_slots)
            or ack.subject_id != subject.subject_id or ack.subject_incarnation != subject.subject_incarnation
            or ack.configuration_generation != subject.configuration_generation
            or ack.deployment_generation != subject.deployment_generation
            or ack.reporter_incarnation != subject.demand_reporter_incarnation
            or ack.candidate.algorithm != "source-sha256" or ack.candidate.identity != base.candidate_sha256
            or ack.candidate.publication_sha256 != base.candidate_publication_sha256
            or ack.protected_admission_sha256 != base.protected_admission_sha256
        ):
            raise ValueError("managed application base configuration or acknowledgement changed")
        return self


def validate_managed_application_origins(
    origins: tuple[ManagedApplicationOriginV1, ...], managed_ids: tuple[UUID, ...],
    acknowledgements: tuple[SubjectExecutionAcknowledgementV2, ...],
    *, configuration_epoch: int | None = None,
) -> None:
    """Check exact coverage inside an authenticated operator policy/preparation."""
    ids = [item.configuration.subject_id for item in origins]
    by_subject = {item.subject_id: item for item in acknowledgements}
    if len(ids) != len(set(ids)) or set(ids) != set(managed_ids):
        raise ValueError("managed application origins must exactly cover managed base identities")
    for origin in origins:
        ack = by_subject.get(origin.configuration.subject_id)
        if ack is None or canonical_executable_bytes(ack) != canonical_executable_bytes(origin.acknowledgement):
            raise ValueError("managed application origin acknowledgement differs from preparation")
        if configuration_epoch is not None and origin.base_projection.expected_configuration_epoch > configuration_epoch:
            raise ValueError("managed application origin cannot refer to a future configuration epoch")
