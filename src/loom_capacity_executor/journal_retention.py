"""Conservative runtime dependency closure for manager-acknowledged checkpoints.

Telemetry may be reclaimed, but lifecycle data is never discarded merely because
an inventory is empty. Unknown event shapes stop compaction rather than guessing
that a future consumer cannot need them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from loom_capacity_executor.heartbeat import ExecutableHeartbeatLoop
from loom_capacity_executor.inventory_journal import load_journal_inventory
from loom_capacity_executor.journal import (
    ExecutorJournal,
    JournalCapacityError,
    JournalHead,
    JournalRecord,
    JournalRegressionError,
)
from loom_capacity_executor.journal_release_retention import lifecycle_retention
from loom_capacity_executor.launch_facts_journal import (
    _MIN_RECOVERY_RESERVE_BYTES,
    _PER_JOB_RECOVERY_RESERVE_BYTES,
)
from loom_capacity_manager.executable_contracts import ExecutablePartialReleaseV2

if TYPE_CHECKING:
    from loom_capacity_executor.executable import ExecutablePoolExecutor

CHECKPOINT_TRIGGER_BYTES = 16 * 1024 * 1024

_CENTRAL_EVENTS = {
    f"{operation}-{result}"
    for operation in ("reservation-accept", "reservation-release", "bootstrap-propose",
        "permit-consume", "intent-close")
    for result in ("requested", "confirmed", "rejected")
}
_LIFECYCLE_EVENTS = _CENTRAL_EVENTS | {
    "slurm-submit-requested", "slurm-submit-unknown", "slurm-submit-confirmed",
    "physical-bind-requested", "physical-bind-confirmed", "launch-facts-retained",
    "protected-bootstrap-requested", "protected-bootstrap-confirmed",
    "protected-drain-requested", "protected-drain-confirmed",
    "protected-withdraw-requested", "protected-withdraw-confirmed",
    "protected-prepared-revocation-requested", "protected-prepared-revocation-confirmed",
    "prepared-handoff-deleted", "pending-cancel-requested", "pending-cancel-confirmed-cancelled",
    "pending-cancel-already-terminal", "pending-cancel-running-drain-only",
    "pending-cancel-ambiguous-quarantined",
}
_TELEMETRY_EVENTS = {
    "heartbeat-requested", "heartbeat-received", "heartbeat-confirmed",
    "inventory-publish-requested", "inventory-publish-confirmed", "inventory-publish-rejected",
    "inventory-chunk-retained", "journal-checkpoint-prepared",
}


@dataclass(frozen=True, slots=True)
class RuntimeCheckpointPlan:
    head: JournalHead
    retained_sequences: tuple[int, ...]
    retained_anchors: tuple[int, ...]
    reserved_bytes: int

    def prepare(self, journal: ExecutorJournal) -> JournalRecord:
        if journal.head != self.head:
            raise JournalRegressionError("checkpoint selection head changed")
        return journal.prepare_checkpoint(retained_sequences=self.retained_sequences,
            retained_anchors=self.retained_anchors, reserved_bytes=self.reserved_bytes)


async def maintain_runtime_journal(
    executor: ExecutablePoolExecutor,
) -> Literal["not-needed", "compacted", "capacity-constrained"]:
    """Serialize checkpoint recovery before ordinary executor work.

    A preflight capacity refusal leaves the original journal usable for drain
    work. Once preparation succeeds, failures must finish that checkpoint before
    any ordinary operation can enter its heartbeat-only tail.
    """
    journal = executor.journal
    latest_inventory = journal.latest("inventory", str(executor.registration.executor_incarnation))
    needs_final_inventory = journal._compaction_floor > 0 and (
        latest_inventory is None or latest_inventory.event_kind != "inventory-publish-confirmed"
        or latest_inventory.sequence <= journal._compaction_floor
    )
    if journal.pending_checkpoint() is None and not needs_final_inventory:
        # Snapshot bytes are irreducible retained baseline, not reclaimable
        # growth. Total footprint still bounds every atomic capacity preflight.
        if journal.path.stat().st_size < CHECKPOINT_TRIGGER_BYTES or journal.pending_requests() or any(
            record.event_kind == "heartbeat-received" for record in journal.latest_records("heartbeat")
        ):
            return "not-needed"
        plan = plan_runtime_checkpoint(executor, await executor._checkpoint())
        if len(plan.retained_sequences) >= len(journal._history):
            return "capacity-constrained"
        try:
            plan.prepare(journal)
        except JournalCapacityError:
            return "capacity-constrained"
    heartbeats = ExecutableHeartbeatLoop(executor.registration, journal, executor.client)
    await heartbeats.finish_checkpoint()
    # Checkpoint acknowledgement advanced the journal beyond the old inventory.
    # Republish before returning so the next retirement sees exact final evidence.
    await executor._publish_inventory(await executor._checkpoint())
    await heartbeats.heartbeat()
    return "compacted"


def plan_runtime_checkpoint(
    executor: ExecutablePoolExecutor, checkpoint: Any,
) -> RuntimeCheckpointPlan:
    """Retain lifecycle history plus the inventories that cleanup can reference.

    Reclamation of lifecycle evidence requires exact release confirmation and
    manager command high-water. The snapshot still awaits journal acknowledgement.
    """
    journal = executor.journal
    journal.assert_covers(checkpoint.journal_sequence, checkpoint.journal_digest)
    if journal.pending_requests() or any(record.event_kind == "heartbeat-received"
        for record in journal.latest_records("heartbeat")):
        raise JournalRegressionError("unresolved requests prevent runtime checkpoint")
    if journal.pending_checkpoint() is not None:
        raise JournalRegressionError("pending checkpoint must finish before selection")
    history = tuple(journal._history)
    if any(record.event_kind not in _LIFECYCLE_EVENTS | _TELEMETRY_EVENTS for record in history):
        raise JournalRegressionError("unsupported runtime checkpoint event")
    incarnation = str(executor.registration.executor_incarnation)
    for record in history:
        event = record.event_kind
        if ((event.startswith("heartbeat-") and
            (record.object_kind != "heartbeat" or record.object_id != incarnation))
            or (event.startswith("inventory-publish-") and
            (record.object_kind != "inventory" or record.object_id != incarnation))
            or (event == "inventory-chunk-retained" and
            (record.object_kind != "executor" or
             re.fullmatch(r"inventory:[0-9a-f]{64}:[0-9]+", record.object_id) is None))):
            raise JournalRegressionError("unsupported telemetry object binding")

    selected, released, active_jobs = lifecycle_retention(executor,
        tuple(record for record in history if record.event_kind in _LIFECYCLE_EVENTS),
        checkpoint.command_sequence)
    anchors: set[int] = set()
    for record in journal.latest_records("heartbeat"):
        if (record.event_kind != "heartbeat-confirmed"
            or record.object_id != str(executor.registration.executor_incarnation)):
            raise JournalRegressionError("unsupported runtime heartbeat state")
        selected.add(record.sequence)

    inventories = []
    for record in history:
        if record.object_kind != "inventory":
            continue
        if record.event_kind not in {"inventory-publish-confirmed", "inventory-publish-rejected"}:
            continue
        inventory = load_journal_inventory(journal, record)
        executor._assert_inventory_binding(inventory)
        inventories.append((record, inventory))

    retained_inventories: set[int] = set()
    if inventories:
        # Keep both the last attempt and last accepted state if a rejection
        # followed it. A rejected publication is not manager-current inventory.
        retained_inventories.add(inventories[-1][0].sequence)
    accepted = [(record, value) for record, value in inventories
        if record.event_kind == "inventory-publish-confirmed"]
    if accepted:
        if accepted[-1][1].inventory_sequence != checkpoint.inventory_sequence:
            raise JournalRegressionError("manager inventory high-water differs from retention")
        retained_inventories.add(accepted[-1][0].sequence)
    elif checkpoint.inventory_sequence != 0:
        raise JournalRegressionError("manager inventory is absent from retention")

    terminal_intents = set()
    for record, inventory in accepted:
        for item in inventory.records:
            if item.state != "terminal" or item.ownership_proof is None:
                continue
            identity = item.ownership_proof.metadata.binding.intent_id
            if identity not in terminal_intents and identity not in released:
                retained_inventories.add(record.sequence)
                terminal_intents.add(identity)
    for record in history:
        if record.event_kind == "intent-close-confirmed" and record.sequence in selected:
            preceding = [item for item, _ in accepted if item.sequence < record.sequence]
            if not preceding:
                raise JournalRegressionError("intent close lacks its preceding inventory")
            retained_inventories.add(preceding[-1].sequence)
    referenced_intents: set[UUID] = set()
    for record, inventory in inventories:
        if record.sequence in retained_inventories:
            anchors.add(inventory.journal_sequence)
            selected.update(range(inventory.journal_sequence + 1, record.sequence + 1))
            referenced_intents.update(item.ownership_proof.metadata.binding.intent_id
                for item in inventory.records if item.ownership_proof is not None)
    # A retained current inventory can still describe an already released job.
    # Keep its release tombstone until that last reference disappears, otherwise
    # the next checkpoint would mistake the old terminal for unreleased work.
    for record in history:
        if record.event_kind == "reservation-release-confirmed":
            payload = record.durable_payload()
            assert payload is not None  # Validated by lifecycle_retention.
            release = ExecutablePartialReleaseV2.model_validate_json(payload)
            if any(item.binding.intent_id in referenced_intents for item in release.releases):
                selected.add(record.sequence)

    return RuntimeCheckpointPlan(journal.head, tuple(sorted(selected)), tuple(sorted(anchors)),
        _MIN_RECOVERY_RESERVE_BYTES
        + active_jobs * _PER_JOB_RECOVERY_RESERVE_BYTES)
