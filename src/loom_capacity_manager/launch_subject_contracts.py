"""Exact bounded manager-to-executor subject facts, not a submission permit."""

from __future__ import annotations

import json
from typing import Literal

from pydantic import field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    SubjectConfigurationV1,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableIntentBindingV2,
    StrictV2Model,
    SubjectExecutionAcknowledgementV2,
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
