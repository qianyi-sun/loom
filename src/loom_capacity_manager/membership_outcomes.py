"""Read-only exact historical membership outcomes, separate from mutation replay."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated, Any, Literal, TypedDict, cast
from uuid import UUID

from pydantic import Field, TypeAdapter, field_validator, model_validator
from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    Digest,
    Identifier,
    PositiveQuantity,
    StrictV1Model,
    canonical_digest,
)
from loom_capacity_manager.membership_contracts import (
    ExecutionPreparationV3,
    PersonalApplicationMembershipMutationV1,
    PersonalApplicationMembershipResponseV1,
    PersonalMembershipCheckpointV1,
)
from loom_capacity_manager.membership_store import _validated_membership_history
from loom_capacity_manager.models import (
    CapacityAuthorityState,
    CapacityExecutionEpoch,
    CapacityPersonalMembershipEvent,
)
from loom_capacity_manager.store import (
    CapacityManagementStore,
    ConfigurationConflictError,
    ExecutionConflictError,
    IdempotencyConflictError,
    _canonical_json_digest,
    _write_transaction,
)


class PersonalMembershipOperationOutcomeQueryV1(StrictV1Model):
    original_actor: Identifier
    idempotency_key: UUID
    request: PersonalApplicationMembershipMutationV1

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 1:
            raise ValueError("outcome query requires exact schema version 1")
        return value

    @model_validator(mode="after")
    def _active_original(self) -> PersonalMembershipOperationOutcomeQueryV1:
        if self.request.execution.execution_state != "active":
            raise ValueError("original membership request requires active execution")
        return self


class _OutcomeBinding(StrictV1Model):
    query_sha256: Digest
    request_sha256: Digest
    original_actor: Identifier
    idempotency_key: UUID
    operation_id: UUID
    execution_epoch: PositiveQuantity
    execution_manifest_sha256: Digest
    namespace_id: UUID


class PersonalMembershipOperationCommittedV1(_OutcomeBinding):
    outcome: Literal["committed"] = "committed"
    receipt: PersonalApplicationMembershipResponseV1

    @model_validator(mode="after")
    def _historical_receipt(self) -> PersonalMembershipOperationCommittedV1:
        checkpoint = self.receipt.checkpoint
        if (
            checkpoint.execution.execution_epoch != self.execution_epoch
            or checkpoint.execution.execution_manifest_sha256 != self.execution_manifest_sha256
            or checkpoint.namespace_id != self.namespace_id
            or self.receipt.result.replayed
        ):
            raise ValueError("outcome must contain the original immutable receipt")
        return self


class PersonalMembershipOperationUnresolvedV1(_OutcomeBinding):
    outcome: Literal["unresolved"] = "unresolved"
    epoch_state: Literal["prepared", "active", "drain-only"]


class PersonalMembershipOperationTerminalNotCommittedV1(_OutcomeBinding):
    outcome: Literal["terminal-not-committed"] = "terminal-not-committed"
    retired_at: datetime
    retirement_sha256: Digest


PersonalMembershipOperationOutcomeV1 = Annotated[
    PersonalMembershipOperationCommittedV1
    | PersonalMembershipOperationUnresolvedV1
    | PersonalMembershipOperationTerminalNotCommittedV1,
    Field(discriminator="outcome"),
]
_OUTCOME_ADAPTER: TypeAdapter[PersonalMembershipOperationOutcomeV1] = TypeAdapter(
    PersonalMembershipOperationOutcomeV1
)


class _BindingValues(TypedDict):
    query_sha256: str
    request_sha256: str
    original_actor: str
    idempotency_key: UUID
    operation_id: UUID
    execution_epoch: int
    execution_manifest_sha256: str
    namespace_id: UUID


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate outcome JSON key")
        if key == "schema_version" and type(value) is not int:
            raise ValueError("outcome schema versions must be exact integers")
        result[key] = value
    return result


def _bounded_json(payload: bytes | str) -> bytes:
    encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise ValueError("outcome contract exceeds maximum byte size")
    try:
        value = json.loads(encoded, object_pairs_hook=_unique_object)
    except (UnicodeError, RecursionError) as exc:
        raise ValueError("invalid outcome JSON") from exc
    if (
        not isinstance(value, dict)
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
    ):
        raise ValueError("outcome requires exact schema version 1")
    return encoded


def parse_membership_operation_outcome_query(
    payload: bytes | str,
) -> PersonalMembershipOperationOutcomeQueryV1:
    return PersonalMembershipOperationOutcomeQueryV1.model_validate_json(_bounded_json(payload))


def parse_membership_operation_outcome(
    payload: bytes | str,
) -> PersonalMembershipOperationOutcomeV1:
    return _OUTCOME_ADAPTER.validate_json(_bounded_json(payload))


async def query_membership_operation_outcome(
    session: AsyncSession,
    management: CapacityManagementStore,
    query: PersonalMembershipOperationOutcomeQueryV1,
) -> PersonalMembershipOperationOutcomeV1:
    """Read under the same authority-first lock as mutation; never update authority."""

    request = query.request
    execution = request.execution
    request_digest = canonical_digest(request)
    binding = _BindingValues(
        query_sha256=canonical_digest(query),
        request_sha256=request_digest,
        original_actor=query.original_actor,
        idempotency_key=query.idempotency_key,
        operation_id=request.projection.operation_id,
        execution_epoch=execution.execution_epoch,
        execution_manifest_sha256=execution.execution_manifest_sha256,
        namespace_id=request.namespace_id,
    )
    async with _write_transaction(session):
        authority = (
            await session.execute(
                select(CapacityAuthorityState)
                .where(CapacityAuthorityState.singleton_id == 1)
                .with_for_update(read=True)
            )
        ).scalar_one_or_none()
        if authority is None:
            raise ExecutionConflictError("membership outcome authority is unavailable")
        epoch = (
            await session.execute(
                select(CapacityExecutionEpoch)
                .where(CapacityExecutionEpoch.execution_epoch == execution.execution_epoch)
                .with_for_update(read=True)
            )
        ).scalar_one_or_none()
        if epoch is None:
            raise ExecutionConflictError("membership outcome execution is unknown")
        preparation = management._execution_preparation_from_row(epoch)
        if (
            not isinstance(preparation, ExecutionPreparationV3)
            or preparation.personal_membership.management_principal_id != query.original_actor
            or preparation.personal_membership.namespace_id != request.namespace_id
            or execution.authority_incarnation != epoch.authority_incarnation
            or execution.authority_incarnation != preparation.authority_incarnation
            or execution.execution_manifest_sha256 != epoch.execution_manifest_sha256
            or execution.configuration_epoch != epoch.configuration_epoch
            or execution.configuration_epoch != preparation.configuration_epoch
            or request.projection.expected_configuration_epoch != epoch.configuration_epoch
            or execution.writer_epoch != preparation.expected_writer_epoch
            or execution.writer_epoch != epoch.prepared_writer_epoch
            or execution.trusted_fleet_release_sha256 != preparation.trusted_fleet_release_sha256
            or execution.executable_new_capacity_ceiling != preparation.requested_ceiling
            or execution.executable_new_capacity_rate_per_minute
            != preparation.requested_rate_per_minute
        ):
            raise ExecutionConflictError("membership outcome original delegation changed")
        rows = (
            await session.scalars(
                select(CapacityPersonalMembershipEvent)
                .where(CapacityPersonalMembershipEvent.execution_epoch == epoch.execution_epoch)
                .order_by(CapacityPersonalMembershipEvent.revision)
                .with_for_update(read=True)
            )
        ).all()
        results = await _validated_membership_history(session, rows, epoch)
        if any(row.actor != query.original_actor for row in rows):
            raise ConfigurationConflictError("membership outcome historical delegation changed")
        # Preserve the mutation API's global operation/key uniqueness, including
        # cross-epoch collisions; neither identity can be silently reinterpreted.
        matches = (
            await session.scalars(
                select(CapacityPersonalMembershipEvent)
                .where(
                    or_(
                        CapacityPersonalMembershipEvent.operation_id
                        == request.projection.operation_id,
                        CapacityPersonalMembershipEvent.idempotency_key == query.idempotency_key,
                    )
                )
                .with_for_update(read=True)
            )
        ).all()
        if len(matches) > 1:
            raise IdempotencyConflictError("membership outcome identities name different requests")
        if matches:
            row = matches[0]
            if (
                row.execution_epoch != epoch.execution_epoch
                or row.operation_id != request.projection.operation_id
                or row.idempotency_key != query.idempotency_key
                or row.actor != query.original_actor
                or row.request_digest != request_digest
                or row.request_payload != request.model_dump(mode="json", exclude_none=False)
            ):
                raise IdempotencyConflictError("membership outcome identity was reused")
            result = next(
                result for item, result in zip(rows, results, strict=True) if item.id == row.id
            )
            return PersonalMembershipOperationCommittedV1(
                **binding,
                receipt=PersonalApplicationMembershipResponseV1(
                    checkpoint=PersonalMembershipCheckpointV1(
                        execution=execution,
                        namespace_id=request.namespace_id,
                        revision=result.revision,
                        head_sha256=result.head_sha256,
                    ),
                    result=result,
                ),
            )
        if epoch.state in {"active", "drain-only", "prepared"}:
            management._execution_context(authority, epoch)
            return PersonalMembershipOperationUnresolvedV1(
                **binding,
                epoch_state=cast(Literal["prepared", "active", "drain-only"], epoch.state),
            )
        payload = epoch.retirement_request_payload
        if (
            epoch.state != "retired"
            or epoch.retired_at is None
            or epoch.effective_ceiling != 0
            or epoch.effective_rate_per_minute != 0
            or epoch.retirement_actor is None
            or epoch.retirement_idempotency_key is None
            or payload is None
            or _canonical_json_digest(payload) != epoch.retirement_request_digest
            or payload.get("execution_epoch") != epoch.execution_epoch
            or payload.get("execution_manifest_sha256") != epoch.execution_manifest_sha256
            or payload.get("authority_incarnation") != str(epoch.authority_incarnation)
            or authority.execution_epoch == epoch.execution_epoch
        ):
            raise ExecutionConflictError("membership outcome retirement evidence changed")
        return PersonalMembershipOperationTerminalNotCommittedV1(
            **binding,
            retired_at=epoch.retired_at,
            retirement_sha256=epoch.retirement_request_digest,
        )
