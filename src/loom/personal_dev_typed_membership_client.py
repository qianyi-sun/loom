"""Authenticated typed onboarding transport, with exact retained retry identity.

The protected caller persists the envelope before sending and supplies separately
pinned preparation/fleet. A lost reply or retired execution is unconfirmed, never
permission to invent a fresh key or source of installed runtime readiness.
"""

from __future__ import annotations

from typing import Self
from uuid import UUID

from pydantic import model_validator

from loom.personal_dev_membership_client import (
    PersonalDevMembershipError,
    _CapacityManagerMembershipTransport,
)
from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.contracts import (
    FleetManifestV1,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.membership_contracts import PersonalMembershipCheckpointV1
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.typed_membership_commands import (
    PersonalBuildCommandV2,
    PersonalMembershipMutationV2,
    PersonalMembershipResultV2,
    derive_application_member,
    derive_build_member,
    parse_typed_membership_mutation,
    parse_typed_membership_result,
    typed_membership_subject_id,
    validate_typed_membership_result,
)


class PersonalDevTypedMembershipEnvelopeV1(StrictV1Model):
    request: PersonalMembershipMutationV2
    expected_checkpoint: PersonalMembershipCheckpointV1
    idempotency_key: UUID

    @model_validator(mode="after")
    def _binding(self) -> Self:
        request = parse_typed_membership_mutation(canonical_bytes(self.request))
        checkpoint = self.expected_checkpoint
        if (self.idempotency_key.int == 0 or request.execution != checkpoint.execution
            or request.namespace_id != checkpoint.namespace_id or request.expected_revision != checkpoint.revision):
            raise ValueError("typed membership saved checkpoint or key changed")
        return self


class CapacityManagerPersonalDevTypedMembershipClient(_CapacityManagerMembershipTransport):
    async def membership_checkpoint(self) -> PersonalMembershipCheckpointV1:
        wire = await self._exchange("GET", "/v2/personal-memberships/checkpoint")
        try:
            return PersonalMembershipCheckpointV1.model_validate_json(wire)
        except ValueError as exc:
            raise PersonalDevMembershipError("typed membership checkpoint is invalid") from exc

    async def mutate_membership(
        self, envelope: PersonalDevTypedMembershipEnvelopeV1, *,
        preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
    ) -> PersonalMembershipResultV2:
        try:
            saved = PersonalDevTypedMembershipEnvelopeV1.model_validate_json(canonical_bytes(envelope))
            request = saved.request
            # Derive before IO so invalid nested copies or incompatible operator
            # inputs cannot reach the authenticated management endpoint.
            if isinstance(request.command, PersonalBuildCommandV2):
                derive_build_member(request, preparation, fleet)
            else:
                derive_application_member(request, preparation, fleet)
            subject_id = typed_membership_subject_id(request)
        except ValueError as exc:
            raise PersonalDevMembershipError("saved typed membership authority is invalid") from exc
        wire = await self._exchange("PUT", f"/v2/personal-memberships/{subject_id}",
            content=canonical_bytes(request), idempotency_key=str(saved.idempotency_key),
            allow_revision_conflict=True, response_schema_version=2)
        try:
            result = parse_typed_membership_result(wire)
            validate_typed_membership_result(request, result, preparation, fleet)
            expected_head = canonical_membership_event_head(
                actor=preparation.personal_membership.management_principal_id,
                execution_epoch=request.execution.execution_epoch, idempotency_key=saved.idempotency_key,
                operation_id=request.command.projection.operation_id,
                previous_sha256=saved.expected_checkpoint.head_sha256, request_digest=canonical_digest(request),
                request_payload=request.model_dump(mode="json"), member=result.member, revision=result.revision)
            if result.head_sha256 != expected_head:
                raise ValueError("typed membership event head differs from saved request")
            return result
        except ValueError as exc:
            raise PersonalDevMembershipError("typed membership outcome differs from saved request") from exc
