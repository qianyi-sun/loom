"""Typed build-membership contracts, not yet admitted by execution stores/APIs.

Pure allocation composition can exercise these contracts before the complete
purpose-preserving execution, admission, recovery and cleanup bridge is enabled.
"""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.application_origin_contracts import (
    ManagedApplicationOriginV1,
    validate_managed_application_origins,
)
from loom_capacity_manager.build_value_contracts import (
    PersonalBuildMemberV1 as PersonalBuildMemberV1,
)
from loom_capacity_manager.build_value_contracts import (
    PersonalBuildTemplateV1 as PersonalBuildTemplateV1,
)
from loom_capacity_manager.build_value_contracts import (
    PersonalMemberV2 as PersonalMemberV2,
)
from loom_capacity_manager.build_value_contracts import (
    _nonzero_uuid,
    _StrictBuildV1,
)
from loom_capacity_manager.build_value_contracts import (
    personal_build_subject_id as personal_build_subject_id,
)
from loom_capacity_manager.build_value_contracts import (
    personal_build_subject_name as personal_build_subject_name,
)
from loom_capacity_manager.contracts import (
    MAX_SUBJECTS,
    AllocationInputV1,
    Digest,
    Quantity,
    SubjectConfigurationV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutionPreparationPolicyV2,
    ExecutionPreparationV2,
)
from loom_capacity_manager.membership_contracts import (
    PersonalMembershipPolicyV1,
)


class ExecutionPreparationPolicyV4(ExecutionPreparationPolicyV2):
    """Explicit direct-V2 successor; never routed as application-only V3."""

    schema_version: Literal[4] = 4  # type: ignore[assignment]
    personal_membership: PersonalMembershipPolicyV1
    personal_builds: PersonalBuildTemplateV1
    managed_application_origins: Annotated[tuple[ManagedApplicationOriginV1, ...], Field(max_length=MAX_SUBJECTS)] = ()

    @field_validator("managed_application_origins")
    @classmethod
    def _canonical_origins(cls, values: tuple[ManagedApplicationOriginV1, ...]) -> tuple[ManagedApplicationOriginV1, ...]:
        return tuple(sorted(values, key=lambda item: item.configuration.subject_id.int))

    @model_validator(mode="after")
    def _managed_origins(self) -> ExecutionPreparationPolicyV4:
        validate_managed_application_origins(self.managed_application_origins,
            self.personal_membership.managed_base_subject_ids, self.subject_acknowledgements)
        return self

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 4:
            raise ValueError("build execution policy schema must be integer 4")
        return value


class ExecutionPreparationV4(ExecutionPreparationV2):
    schema_version: Literal[4] = 4  # type: ignore[assignment]
    personal_membership: PersonalMembershipPolicyV1
    personal_builds: PersonalBuildTemplateV1
    managed_application_origins: Annotated[tuple[ManagedApplicationOriginV1, ...], Field(max_length=MAX_SUBJECTS)] = ()

    @field_validator("managed_application_origins")
    @classmethod
    def _canonical_origins(cls, values: tuple[ManagedApplicationOriginV1, ...]) -> tuple[ManagedApplicationOriginV1, ...]:
        return ExecutionPreparationPolicyV4._canonical_origins(values)

    @model_validator(mode="after")
    def _managed_origins(self) -> ExecutionPreparationV4:
        validate_managed_application_origins(self.managed_application_origins,
            self.personal_membership.managed_base_subject_ids, self.subject_acknowledgements,
            configuration_epoch=self.configuration_epoch)
        return self

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 4:
            raise ValueError("build execution preparation schema must be integer 4")
        return value




class PersonalMembershipSnapshotV2(_StrictBuildV1):
    """One combined revision/bound for application and build members."""

    schema_version: Literal[2] = 2  # type: ignore[assignment]
    namespace_id: UUID
    revision: Quantity
    head_sha256: Digest
    members: Annotated[tuple[PersonalMemberV2, ...], Field(max_length=MAX_SUBJECTS)] = ()

    _namespace_nonzero = field_validator("namespace_id")(_nonzero_uuid)

    @field_validator("members")
    @classmethod
    def _canonical_members(cls, members: tuple[PersonalMemberV2, ...]) -> tuple[PersonalMemberV2, ...]:
        ids = [item.configuration.subject_id for item in members]
        revisions = [item.revision for item in members]
        names = [item.configuration.display_name for item in members]
        build_owners = [item.owner_id for item in members if isinstance(item, PersonalBuildMemberV1)]
        if any(len(values) != len(set(values)) for values in (ids, revisions, names, build_owners)):
            raise ValueError("duplicate membership identity, revision, name or owner build service")
        return tuple(sorted(members, key=lambda item: item.revision))

    @model_validator(mode="after")
    def _exact_head(self) -> PersonalMembershipSnapshotV2:
        if (self.revision == 0) != (self.head_sha256 == "0" * 64):
            raise ValueError("build membership snapshot head differs from revision")
        if any(item.revision > self.revision for item in self.members):
            raise ValueError("member revision is ahead of the shared head")
        return self


class DelegatedAllocationInputV3(AllocationInputV1):
    """Pure V4 preparation composition; executable promotion remains disabled."""

    schema_version: Literal[3] = 3  # type: ignore[assignment]
    preparation: ExecutionPreparationV4
    managed_base_subjects: Annotated[tuple[SubjectConfigurationV1, ...], Field(max_length=MAX_SUBJECTS)] = ()
    membership: PersonalMembershipSnapshotV2

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("build allocation input schema must be integer 3")
        return value

    @field_validator("managed_base_subjects")
    @classmethod
    def _canonical_base(cls, values: tuple[SubjectConfigurationV1, ...]) -> tuple[SubjectConfigurationV1, ...]:
        ids = [item.subject_id for item in values]
        if any(item.int == 0 for item in ids) or len(ids) != len(set(ids)):
            raise ValueError("build allocation managed base identities are invalid")
        return tuple(sorted(values, key=lambda item: item.subject_id.int))

    @model_validator(mode="after")
    def _combined_bounds(self) -> DelegatedAllocationInputV3:
        ids = {item.subject_id for item in self.managed_base_subjects}
        ids.update(item.configuration.subject_id for item in self.membership.members)
        if len(ids) > self.preparation.personal_membership.max_subjects:
            raise ValueError("combined membership exceeds its subject bound")
        return self
