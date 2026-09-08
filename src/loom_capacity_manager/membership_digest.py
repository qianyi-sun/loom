"""Canonical membership event hashing shared by persistence and receipt readers."""

import hashlib
import json
from uuid import UUID

from loom_capacity_manager.membership_contracts import PersonalApplicationMemberV1


def canonical_membership_event_head(
    *,
    actor: str,
    execution_epoch: int,
    idempotency_key: UUID,
    operation_id: UUID,
    previous_sha256: str,
    request_digest: str,
    request_payload: dict[str, object],
    member: PersonalApplicationMemberV1,
    revision: int,
) -> str:
    """Retain the original event preimage and canonical bytes exactly."""

    payload = {
        "actor": actor,
        "execution_epoch": execution_epoch,
        "idempotency_key": str(idempotency_key),
        "operation_id": str(operation_id),
        "previous_sha256": previous_sha256,
        "request_digest": request_digest,
        "request_payload": request_payload,
        "result_member": member.model_dump(mode="json", exclude_none=False),
        "revision": revision,
    }
    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode("ascii")
    ).hexdigest()
