"""Exact bounded manager-to-executor subject facts, not a submission permit."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    ExecutableLaunchPermitV2,
    StrictV2Model,
    SubjectExecutionAcknowledgementV2,
    _utc_time,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.typed_ownership_contracts import (
    ExecutableSubjectAuthorityV3,
    _exact_schema_types,
)

MAX_LAUNCH_SUBJECT_BYTES = MAX_CONTRACT_BYTES


class ExecutableLaunchSubjectV3(StrictV2Model):
    schema_version: Literal[3] = 3  # type: ignore[assignment]
    binding: ExecutableIntentBindingV2
    configuration: SubjectConfigurationV1
    acknowledgement: SubjectExecutionAcknowledgementV2
    authority: ExecutableSubjectAuthorityV3

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("launch subject schema must be integer 3")
        return value

    @model_validator(mode="after")
    def _exact_facts(self) -> ExecutableLaunchSubjectV3:
        binding, subject, ack, authority = (
            self.binding,
            self.configuration,
            self.acknowledgement,
            self.authority,
        )
        reference = authority.configuration
        if (
            reference.subject_id != subject.subject_id
            or reference.subject_incarnation != subject.subject_incarnation
            or reference.generation != subject.configuration_generation
            or reference.digest != canonical_digest(subject)
            or authority.acknowledgement_sha256 != canonical_executable_digest(ack)
            or ack.subject_id != subject.subject_id
            or ack.subject_incarnation != subject.subject_incarnation
            or ack.configuration_generation != subject.configuration_generation
            or ack.deployment_generation != subject.deployment_generation
            or ack.reporter_incarnation != subject.demand_reporter_incarnation
            or binding.subject_id != subject.subject_id
            or binding.subject_incarnation != subject.subject_incarnation
            or binding.candidate != ack.candidate
            or binding.account_id != subject.account_id
            or binding.tier_id != subject.tier_id
            or binding.candidate_generation != subject.candidate_generation
            or binding.deployment_generation != subject.deployment_generation
            or subject.lifecycle_state != "active"
        ):
            raise ValueError("launch subject facts differ from intent or authenticated references")
        event = authority.membership
        if event is not None and (
            event.execution_manifest_sha256 != binding.execution.execution_manifest_sha256
            or binding.tier_id != "development"
            or binding.account_id != f"dev-owner-{event.owner_id.hex}"
        ):
            raise ValueError("launch subject owner or execution differs")
        profile = next((item for item in subject.profiles if item.pool_id == binding.pool_id), None)
        shape = (
            None
            if profile is None
            else next(
                (item for item in profile.worker_shapes if item.shape_id == binding.shape_id), None
            )
        )
        if (
            profile is None
            or shape is None
            or profile.pool_generation != binding.pool_generation
            or shape.shape_id != binding.profile_id
            or profile.profile_generation != binding.profile_generation
            or profile.profile_digest != binding.profile_digest
            or shape.total_resources != binding.resources
            or shape.concurrency_slots != binding.concurrency_slots
            or len(shape.node_resources) != len(binding.node_ids)
        ):
            raise ValueError("launch subject profile differs from intent")
        return self


def canonical_launch_subject_bytes(value: ExecutableLaunchSubjectV3) -> bytes:
    if type(value) is not ExecutableLaunchSubjectV3:
        raise ValueError("launch subject requires its exact contract")
    encoded = canonical_executable_bytes(value)
    if len(encoded) > MAX_LAUNCH_SUBJECT_BYTES:
        raise ValueError("launch subject exceeds response byte bound")
    _exact_schema_types(json.loads(encoded))
    checked = ExecutableLaunchSubjectV3.model_validate_json(encoded)
    if canonical_executable_bytes(checked) != encoded:
        raise ValueError("launch subject is not canonical")
    return encoded


def parse_launch_subject(payload: bytes) -> ExecutableLaunchSubjectV3:
    if not isinstance(payload, bytes) or len(payload) > MAX_LAUNCH_SUBJECT_BYTES:
        raise ValueError("launch subject exceeds response byte bound")
    _exact_schema_types(json.loads(payload))
    value = ExecutableLaunchSubjectV3.model_validate_json(payload)
    if canonical_launch_subject_bytes(value) != payload:
        raise ValueError("launch subject response is not canonical")
    return value


APPLICATION_ALLOCATION_OBSERVATION_TTL = timedelta(seconds=10)


class CurrentApplicationAllocationV3(StrictV2Model):
    """Unsigned, short-lived manager evidence, never host or runtime admission.

    Permit expiry bounds submission, not the lifetime of already allocated work.
    The consumer must separately authenticate current local bootstrap and exact
    scheduler incarnation before issuing purpose-specific host authority.
    """

    schema_version: Literal[3] = 3  # type: ignore[assignment]
    subject: ExecutableLaunchSubjectV3
    permit: ExecutableLaunchPermitV2
    permit_consumed_at: datetime
    observed_slurm_job_id: Annotated[str, Field(pattern=r"^[1-9][0-9]{0,19}$")] | None = None
    observed_at: datetime
    expires_at: datetime
    executable: Literal[False] = False

    _times_utc = field_validator("permit_consumed_at", "observed_at", "expires_at")(_utc_time)

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("application allocation schema must be integer 3")
        return value

    @field_validator("executable", mode="before")
    @classmethod
    def _not_executable(cls, value: object) -> object:
        if value is not False:
            raise ValueError("application allocation observation is not executable")
        return value

    @model_validator(mode="after")
    def _exact_application(self) -> CurrentApplicationAllocationV3:
        canonical_launch_subject_bytes(self.subject)
        if (
            self.subject.authority.purpose != "application-worker"
            or self.permit.binding != self.subject.binding
            or self.permit.permit_id.int == 0
            or self.permit_consumed_at >= self.permit.expires_at
            or self.permit_consumed_at > self.observed_at
            or not self.observed_at < self.expires_at <= self.observed_at + APPLICATION_ALLOCATION_OBSERVATION_TTL
        ):
            raise ValueError("application allocation binding or lifetime changed")
        return self


def canonical_current_application_allocation_bytes(value: CurrentApplicationAllocationV3) -> bytes:
    if type(value) is not CurrentApplicationAllocationV3:
        raise ValueError("application allocation requires its exact contract")
    encoded = canonical_executable_bytes(value)
    if len(encoded) > MAX_LAUNCH_SUBJECT_BYTES:
        raise ValueError("application allocation exceeds response byte bound")
    _exact_schema_types(json.loads(encoded))
    checked = CurrentApplicationAllocationV3.model_validate_json(encoded)
    if canonical_executable_bytes(checked) != encoded:
        raise ValueError("application allocation is not canonical")
    return encoded


def parse_current_application_allocation(payload: bytes) -> CurrentApplicationAllocationV3:
    if not isinstance(payload, bytes) or len(payload) > MAX_LAUNCH_SUBJECT_BYTES:
        raise ValueError("application allocation exceeds response byte bound")
    _exact_schema_types(json.loads(payload))
    value = CurrentApplicationAllocationV3.model_validate_json(payload)
    if canonical_current_application_allocation_bytes(value) != payload:
        raise ValueError("application allocation response is not canonical")
    return value
