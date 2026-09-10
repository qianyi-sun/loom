"""Conservative runtime dependency closure for manager-acknowledged checkpoints.

Telemetry may be reclaimed, but lifecycle data is never discarded merely because
an inventory is empty. Unknown event shapes stop compaction rather than guessing
that a future consumer cannot need them.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from loom_capacity_executor.inventory_journal import load_journal_inventory
from loom_capacity_executor.journal import (
    ExecutorJournal,
    JournalHead,
    JournalRecord,
    JournalRegressionError,
)
from loom_capacity_executor.launch_facts_journal import (
    _MIN_RECOVERY_RESERVE_BYTES,
    _PER_JOB_RECOVERY_RESERVE_BYTES,
)

if TYPE_CHECKING:
    from loom_capacity_executor.executable import ExecutablePoolExecutor

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


def plan_runtime_checkpoint(
    executor: ExecutablePoolExecutor, checkpoint: Any,
) -> RuntimeCheckpointPlan:
    """Retain lifecycle history plus the inventories that cleanup can reference.

    This first conservative selector does not reclaim released lifecycle history;
    authenticated release/high-water reclamation is a separate required closure.
    No runtime calls this selector until both closures are covered together.
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

    selected = {record.sequence for record in history if record.event_kind in _LIFECYCLE_EVENTS}
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
            if identity not in terminal_intents:
                retained_inventories.add(record.sequence)
                terminal_intents.add(identity)
    for record in history:
        if record.event_kind == "intent-close-confirmed":
            preceding = [item for item, _ in accepted if item.sequence < record.sequence]
            if not preceding:
                raise JournalRegressionError("intent close lacks its preceding inventory")
            retained_inventories.add(preceding[-1].sequence)
    for record, inventory in inventories:
        if record.sequence in retained_inventories:
            anchors.add(inventory.journal_sequence)
            selected.update(range(inventory.journal_sequence + 1, record.sequence + 1))

    return RuntimeCheckpointPlan(journal.head, tuple(sorted(selected)), tuple(sorted(anchors)),
        _MIN_RECOVERY_RESERVE_BYTES
        + len(journal.latest_records("job")) * _PER_JOB_RECOVERY_RESERVE_BYTES)
