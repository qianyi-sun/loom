"""Purpose-preserving successor values; durable source authentication is required.

These contracts are not yet accepted by preparation or execution. Pending build
installation is not readiness. Old application origin bytes remain unchanged.
"""

from __future__ import annotations

from typing import Literal

from pydantic import field_validator, model_validator

from loom_capacity_manager.application_origin_contracts import ManagedApplicationOriginV1
from loom_capacity_manager.build_value_contracts import (
    PersonalBuildMemberV1,
    PersonalBuildProjectionV1,
    PersonalBuildTemplateV1,
    personal_build_subject_name,
)
from loom_capacity_manager.contracts import Digest, SubjectConfigurationV1, canonical_bytes
from loom_capacity_manager.executable_contracts import (
    SubjectExecutionAcknowledgementV2,
    canonical_executable_bytes,
)
from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1
from loom_capacity_manager.retired_member_origin_contracts import (
    RetiredPersonalMemberOriginV1,
    _StrictOriginV1,
)


class ManagedApplicationOriginV2(ManagedApplicationOriginV1):
    schema_version: Literal[2] = 2  # type: ignore[assignment]
    inherited: RetiredPersonalMemberOriginV1

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 2:
            raise ValueError("inherited application origin schema must be integer 2")
        return value

    @model_validator(mode="after")
    def _inherited_member(self) -> ManagedApplicationOriginV2:
        member = self.inherited.anchor.member
        if (not isinstance(member, PersonalApplicationMemberV1)
            or member.owner_id != self.base_projection.owner_id
            or canonical_bytes(member.configuration) != canonical_bytes(self.configuration)
            or canonical_executable_bytes(member.acknowledgement) != canonical_executable_bytes(self.acknowledgement)):
            raise ValueError("application origin differs from inherited member")
        return self


class ManagedBuildOriginV1(_StrictOriginV1):
    configuration: SubjectConfigurationV1
    acknowledgement: SubjectExecutionAcknowledgementV2
    installation_projection: PersonalBuildProjectionV1
    base_projection: PersonalBuildProjectionV1
    template: PersonalBuildTemplateV1
    trusted_fleet_release_sha256: Digest
    readiness_state: Literal["pending"] = "pending"
    inherited: RetiredPersonalMemberOriginV1

    @model_validator(mode="after")
    def _installation_binding(self) -> ManagedBuildOriginV1:
        subject, ack = self.configuration, self.acknowledgement
        base, installed, template = self.base_projection, self.installation_projection, self.template
        member = self.inherited.anchor.member
        if (not isinstance(member, PersonalBuildMemberV1)
            or member.owner_id != base.owner_id
            or canonical_bytes(member.configuration) != canonical_bytes(subject)
            or canonical_executable_bytes(member.acknowledgement) != canonical_executable_bytes(ack)):
            raise ValueError("build origin differs from inherited member")
        mutable = {"operation_kind", "operation_id", "operation_epoch", "configuration_generation", "max_slots"}
        if (installed.operation_kind not in {"create", "update"}
            or installed.max_slots > template.max_slots_per_subject
            or base.max_slots > template.max_slots_per_subject
            or installed.model_dump_json(exclude=mutable) != base.model_dump_json(exclude=mutable)
            or (base.operation_kind in {"create", "update"} and canonical_bytes(base) != canonical_bytes(installed))
            or (base.operation_kind in {"capacity", "destroy"} and (
                base.configuration_generation <= installed.configuration_generation
                or base.operation_id == installed.operation_id))):
            raise ValueError("build installation origin changed")
        if (subject.subject_incarnation != base.subject_incarnation
            or subject.display_name != personal_build_subject_name(base.owner_id)
            or subject.configuration_generation != base.configuration_generation
            or subject.candidate_generation != base.candidate_generation
            or subject.deployment_generation != base.deployment_generation
            or subject.demand_reporter_incarnation != base.demand_reporter_incarnation
            or subject.lifecycle_state != ("disabled" if base.operation_kind == "destroy" else "active")
            or subject.max_slots != (0 if base.operation_kind == "destroy" else base.max_slots)
            or subject.max_slots > template.max_slots_per_subject
            or subject.max_pending_slots != template.max_pending_slots_per_subject
            or subject.max_pending_jobs != template.max_pending_jobs_per_subject
            or canonical_executable_bytes(ack.candidate) != canonical_executable_bytes(template.runtime_candidate)
            or subject.profiles != template.profiles
            or self.trusted_fleet_release_sha256 == "0" * 64):
            raise ValueError("build origin service configuration or runtime changed")
        return self
