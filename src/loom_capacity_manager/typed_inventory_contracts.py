"""Purpose-preserving executable inventory, distinct from legacy V2 admission.

Parsing proves structural coherence only. The durable consumer must verify the
signature and join its subject authority to the retained allocation and intent.
"""

from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_FIXED_CLAIMS_PER_REPORT,
    Digest,
    Identifier,
    PositiveQuantity,
    Quantity,
    ResourceVectorV1,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableExecutorInventoryV2,
    ExecutableIntentBindingV2,
    ExecutableInventoryRecordV2,
    ExecutableTerminalInventoryEvidenceV2,
    ExecutionContextV2,
    StrictV2Model,
    _utc_time,
    _validate_journal_head,
    canonical_executable_bytes,
    canonical_inventory_confirmation_journal_head,
)
from loom_capacity_manager.typed_ownership_contracts import (
    SignedExecutableOwnershipProofV3,
    _exact_schema_types,
)

MAX_TERMINAL_INVENTORY_EVIDENCE_BYTES = MAX_CONTRACT_BYTES


class _StrictInventoryV3(StrictV2Model):
    schema_version: Literal[3] = 3  # type: ignore[assignment]

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("typed inventory schema must be integer 3")
        return value


class ExecutableInventoryRecordV3(_StrictInventoryV3):
    physical_identity: Identifier
    physical_kind: Literal["slurm-job", "worker"]
    authority_scope: Literal["registered-loom", "dedicated-loom-association", "foreign"]
    state: Literal["pending", "active", "draining", "terminal", "unknown"]
    resources: ResourceVectorV1
    node_ids: tuple[Identifier, ...] = ()
    controller_evidence_sha256: Digest
    ownership_proof: SignedExecutableOwnershipProofV3 | None = None
    terminal_evidence_sha256: Digest | None = None

    @field_validator("node_ids")
    @classmethod
    def _canonical_nodes(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("duplicate executor inventory node identity")
        return tuple(sorted(value))

    @model_validator(mode="after")
    def _proof_and_terminal_evidence(self) -> ExecutableInventoryRecordV3:
        if self.resources.slots == 0:
            raise ValueError("executor inventory must charge positive slots")
        if self.authority_scope == "foreign" and self.ownership_proof is not None:
            raise ValueError("foreign inventory cannot carry Loom ownership proof")
        if (self.state == "terminal") != (self.terminal_evidence_sha256 is not None):
            raise ValueError("terminal evidence is required exactly for terminal inventory")
        return self


class ExecutableExecutorInventoryV3(_StrictInventoryV3):
    execution: ExecutionContextV2
    executor_id: Identifier
    executor_incarnation: UUID
    pool_id: Identifier
    pool_generation: PositiveQuantity
    inventory_sequence: PositiveQuantity
    journal_sequence: Quantity
    journal_digest: Digest
    journal_checkpoint_sequence: Quantity = 0
    journal_checkpoint_digest: Digest = "0" * 64
    complete: Literal[True] = True
    records: Annotated[
        tuple[ExecutableInventoryRecordV3, ...], Field(max_length=MAX_FIXED_CLAIMS_PER_REPORT)
    ] = ()
    executable: Literal[True] = True

    @field_validator("records")
    @classmethod
    def _canonical_records(
        cls, value: tuple[ExecutableInventoryRecordV3, ...]
    ) -> tuple[ExecutableInventoryRecordV3, ...]:
        if len({record.physical_identity for record in value}) != len(value):
            raise ValueError("duplicate executor inventory identity")
        return tuple(sorted(value, key=lambda record: record.physical_identity))

    @model_validator(mode="after")
    def _canonical_journal(self) -> ExecutableExecutorInventoryV3:
        _validate_journal_head(self.journal_sequence, self.journal_digest)
        _validate_journal_head(self.journal_checkpoint_sequence, self.journal_checkpoint_digest)
        if self.journal_checkpoint_sequence > self.journal_sequence:
            raise ValueError("journal checkpoint cannot exceed the reported head")
        for record in self.records:
            proof = record.ownership_proof
            if proof is None:
                continue
            binding = proof.metadata.binding
            # execution_state may progress prepared -> active. Compare the same
            # immutable identity fields as legacy inventory, not today's state.
            if (
                any(
                    getattr(binding.execution, field) != getattr(self.execution, field)
                    for field in (
                        "authority_incarnation",
                        "writer_epoch",
                        "configuration_epoch",
                        "execution_epoch",
                        "execution_manifest_sha256",
                        "trusted_fleet_release_sha256",
                    )
                )
                or binding.executor_id != self.executor_id
                or binding.executor_incarnation != self.executor_incarnation
                or binding.pool_id != self.pool_id
                or binding.pool_generation != self.pool_generation
            ):
                raise ValueError("inventory ownership proof has another executor binding")
        return self


ExecutorInventory = ExecutableExecutorInventoryV2 | ExecutableExecutorInventoryV3
InventoryRecord = ExecutableInventoryRecordV2 | ExecutableInventoryRecordV3


class ExecutableTerminalInventoryEvidenceV3(_StrictInventoryV3):
    binding: ExecutableIntentBindingV2
    inventory_execution: ExecutionContextV2
    inventory_sequence: PositiveQuantity
    inventory_digest: Digest
    journal_sequence: Quantity
    journal_digest: Digest
    record: ExecutableInventoryRecordV3
    observed_at: datetime
    executable: Literal[True] = True

    _observed_at_utc = field_validator("observed_at")(_utc_time)

    @model_validator(mode="after")
    def _exact_terminal_binding(self) -> ExecutableTerminalInventoryEvidenceV3:
        _validate_journal_head(self.journal_sequence, self.journal_digest)
        proof = self.record.ownership_proof
        if (
            self.record.state != "terminal"
            or self.record.terminal_evidence_sha256 is None
            or proof is None
            or self.record.authority_scope != "dedicated-loom-association"
        ):
            raise ValueError(
                "terminal inventory evidence requires one authenticated terminal record"
            )
        if (
            proof.metadata.binding != self.binding
            or self.inventory_execution.model_dump(mode="python")
            != self.binding.execution.model_dump(
                mode="python", exclude={"allocation_epoch", "executable"}
            )
            or self.record.resources != self.binding.resources
            or self.record.node_ids != self.binding.node_ids
        ):
            raise ValueError("terminal inventory evidence binding changed")
        return self


TerminalInventoryEvidence = (
    ExecutableTerminalInventoryEvidenceV2 | ExecutableTerminalInventoryEvidenceV3
)


def _unique_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("inventory payload has duplicate object fields")
        result[key] = value
    return result


def _versioned_inventory_payload(payload: bytes | str) -> tuple[bytes, int]:
    encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
    if not isinstance(encoded, bytes) or len(encoded) > MAX_CONTRACT_BYTES:
        raise ValueError("inventory exceeds its byte bound")
    value = json.loads(encoded, object_pairs_hook=_unique_pairs)
    if not isinstance(value, dict) or type(value.get("schema_version")) is not int:
        raise ValueError("inventory schema must be an exact integer")
    if value["schema_version"] not in (2, 3):
        raise ValueError("unsupported executor inventory schema")
    if value["schema_version"] == 3:
        _exact_schema_types(value)
    return encoded, value["schema_version"]


def parse_executor_inventory(payload: bytes | str) -> ExecutorInventory:
    encoded, version = _versioned_inventory_payload(payload)
    if version == 2:
        return ExecutableExecutorInventoryV2.model_validate_json(encoded)
    return ExecutableExecutorInventoryV3.model_validate_json(encoded)


def parse_terminal_inventory_evidence(payload: bytes | str) -> TerminalInventoryEvidence:
    encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
    if not isinstance(encoded, bytes) or len(encoded) > MAX_TERMINAL_INVENTORY_EVIDENCE_BYTES:
        raise ValueError("terminal inventory evidence exceeds its byte bound")
    encoded, version = _versioned_inventory_payload(payload)
    if version == 2:
        return ExecutableTerminalInventoryEvidenceV2.model_validate_json(encoded)
    return ExecutableTerminalInventoryEvidenceV3.model_validate_json(encoded)


def inventory_confirmation_journal_head(inventory: ExecutorInventory) -> tuple[int, str]:
    if type(inventory) is ExecutableExecutorInventoryV3:
        return typed_inventory_confirmation_journal_head(inventory)
    if type(inventory) is ExecutableExecutorInventoryV2:
        return canonical_inventory_confirmation_journal_head(inventory)
    raise ValueError("unsupported inventory confirmation contract")


def typed_inventory_confirmation_journal_head(
    inventory: ExecutableExecutorInventoryV3,
) -> tuple[int, str]:
    """Commit the actual V3 bytes in the unchanged V2 journal-record protocol."""
    if type(inventory) is not ExecutableExecutorInventoryV3:
        raise ValueError("typed inventory confirmation requires its exact contract")
    payload = canonical_executable_bytes(inventory)
    checked = parse_executor_inventory(payload)
    if canonical_executable_bytes(checked) != payload:
        raise ValueError("typed inventory confirmation is not canonical")
    digest = hashlib.sha256(payload).hexdigest()
    sequence, previous = inventory.journal_sequence, inventory.journal_digest
    for event in ("inventory-publish-requested", "inventory-publish-confirmed"):
        sequence += 1
        record = {
            "schema_version": 2,
            "sequence": sequence,
            "previous_digest": previous,
            "event_kind": event,
            "object_kind": "inventory",
            "object_id": str(inventory.executor_incarnation),
            "payload_digest": digest,
            "payload_base64": base64.b64encode(payload).decode("ascii"),
        }
        previous = hashlib.sha256(
            json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode(
                "ascii"
            )
        ).hexdigest()
    return sequence, previous
