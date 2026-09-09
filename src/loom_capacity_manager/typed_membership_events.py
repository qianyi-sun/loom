"""Read-only typed event checks for the successor membership persistence path.

These verify stored values and prefix hashing, not durable insertion, historical
lifecycle/release authenticity or current admission. They deliberately do not
widen legacy history parsers. The owning store must obtain the preparation/fleet
from authenticated durable authority and verify lifecycle, retained generations,
predecessor release and materialization before consuming the resulting members.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from uuid import UUID

from loom_capacity_manager.build_membership_contracts import ExecutionPreparationV4
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES, FleetManifestV1, canonical_digest
from loom_capacity_manager.membership_digest import canonical_membership_event_head
from loom_capacity_manager.models import CapacityPersonalMembershipEvent
from loom_capacity_manager.typed_membership_commands import (
    PersonalMembershipMutationV2,
    PersonalMembershipResultV2,
    parse_typed_membership_mutation,
    parse_typed_membership_result,
    validate_typed_membership_result,
)


def _payload(value: dict[str, object]) -> bytes:
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")
    except (TypeError, RecursionError) as exc:
        raise ValueError("invalid typed event payload") from exc
    if len(encoded) > MAX_CONTRACT_BYTES:
        raise ValueError("typed event payload exceeds byte bound")
    return encoded


def validate_typed_membership_event(
    row: CapacityPersonalMembershipEvent, preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
) -> tuple[PersonalMembershipMutationV2, PersonalMembershipResultV2]:
    """Check the complete original request/result against each indexed row value."""
    request = parse_typed_membership_mutation(_payload(row.request_payload))
    result = parse_typed_membership_result(_payload(row.result_payload))
    validate_typed_membership_result(request, result, preparation, fleet)
    projection, member, execution = request.command.projection, result.member, request.execution
    configuration = member.configuration
    if (
        result.replayed
        or row.actor != preparation.personal_membership.management_principal_id
        or row.execution_epoch != execution.execution_epoch
        or row.execution_manifest_sha256 != execution.execution_manifest_sha256
        or row.authority_incarnation != execution.authority_incarnation
        or row.writer_epoch != execution.writer_epoch
        or row.namespace_id != request.namespace_id
        or row.revision != result.revision
        or row.head_sha256 != result.head_sha256
        or row.operation_id != projection.operation_id
        or row.request_digest != canonical_digest(request)
        or row.subject_id != configuration.subject_id
        or row.subject_incarnation != configuration.subject_incarnation
        or row.owner_id != member.owner_id
        or row.configuration_generation != configuration.configuration_generation
        or row.deployment_generation != configuration.deployment_generation
        or row.reporter_incarnation != configuration.demand_reporter_incarnation
        or row.head_sha256 != canonical_membership_event_head(
            actor=row.actor, execution_epoch=row.execution_epoch, idempotency_key=row.idempotency_key,
            operation_id=row.operation_id, previous_sha256=row.previous_sha256, request_digest=row.request_digest,
            request_payload=row.request_payload, member=member, revision=row.revision,
        )
    ):
        raise ValueError("typed membership event binding changed")
    return request, result


def validate_typed_membership_event_prefix(
    rows: Sequence[CapacityPersonalMembershipEvent], preparation: ExecutionPreparationV4, fleet: FleetManifestV1,
    *, execution_epoch: int,
) -> tuple[PersonalMembershipResultV2, ...]:
    """Validate a consecutive mixed-purpose prefix, not lifecycle authenticity.

Operation and idempotency IDs share one domain across purposes. The durable
unique indexes also enforce their uniqueness across other execution epochs.
This helper cannot establish that the supplied prefix is the latest history.
"""
    if type(execution_epoch) is not int or execution_epoch <= 0:
        raise ValueError("typed membership execution epoch must be positive")
    previous = "0" * 64
    operations: set[UUID] = set()
    keys: set[UUID] = set()
    results: list[PersonalMembershipResultV2] = []
    for revision, row in enumerate(rows, start=1):
        _request, result = validate_typed_membership_event(row, preparation, fleet)
        if (
            row.execution_epoch != execution_epoch or row.revision != revision
            or row.previous_sha256 != previous or row.operation_id in operations
            or row.idempotency_key in keys
        ):
            raise ValueError("typed membership prefix or replay identity changed")
        previous = row.head_sha256
        operations.add(row.operation_id)
        keys.add(row.idempotency_key)
        results.append(result)
    return tuple(results)
