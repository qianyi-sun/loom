"""Bounded, journal-first inventory retention and exact historical replay."""

from __future__ import annotations

import hashlib
import json
import re

from loom_capacity_executor.journal import ExecutorJournal, JournalRecord, JournalRegressionError
from loom_capacity_manager.contracts import MAX_CONTRACT_BYTES
from loom_capacity_manager.executable_contracts import canonical_executable_bytes
from loom_capacity_manager.typed_inventory_contracts import (
    INVENTORY_JOURNAL_CHUNK_BYTES,
    ExecutableExecutorInventoryV3,
    ExecutorInventory,
    parse_executor_inventory,
    typed_inventory_journal_frames,
)

Frame = tuple[str, str, str, bytes]


def _frames(inventory: ExecutorInventory) -> tuple[Frame, ...]:
    if type(inventory) is ExecutableExecutorInventoryV3:
        return typed_inventory_journal_frames(inventory)
    payload = canonical_executable_bytes(inventory)
    return tuple((event, "inventory", str(inventory.executor_incarnation), payload)
        for event in ("inventory-publish-requested", "inventory-publish-confirmed"))


def _append(journal: ExecutorJournal, frame: Frame) -> None:
    event, kind, object_id, payload = frame
    journal.append(event, hashlib.sha256(payload).hexdigest(), object_kind=kind,
        object_id=object_id, payload=payload)


def retain_inventory_request(journal: ExecutorJournal, inventory: ExecutorInventory) -> None:
    """Reserve the complete batch before any durable write or external request.

    Publication may spend recovery headroom; new launch admission must preserve
    it. This preflight does not constitute journal compaction or steady-state GC.
    """
    if (inventory.journal_sequence, inventory.journal_digest) != (
        journal.head.sequence, journal.head.digest,
    ):
        raise JournalRegressionError("inventory batch anchor differs from journal head")
    if any(record.object_kind == "inventory" for record in journal.pending_requests()):
        raise JournalRegressionError("inventory publication already pending")
    frames = _frames(inventory)
    journal.assert_payload_capacity(tuple(len(frame[3]) for frame in frames), reserved_bytes=0)
    for frame in frames[:-1]:
        _append(journal, frame)


def complete_inventory_request(
    journal: ExecutorJournal, inventory: ExecutorInventory, *, rejected: bool,
) -> None:
    requested = journal.latest("inventory", str(inventory.executor_incarnation))
    if (requested is None or requested.event_kind != "inventory-publish-requested"
        or requested.sequence != journal.head.sequence
        or canonical_executable_bytes(load_journal_inventory(journal, requested))
        != canonical_executable_bytes(inventory)):
        raise JournalRegressionError("inventory response differs from pending request")
    event, kind, object_id, payload = _frames(inventory)[-1]
    _append(journal, ("inventory-publish-rejected" if rejected else event, kind, object_id, payload))


def load_journal_inventory(journal: ExecutorJournal, record: JournalRecord) -> ExecutorInventory:
    """Resolve canonical bytes and verify their exact contiguous retained batch."""
    payload = record.durable_payload()
    if payload is None:
        raise JournalRegressionError("inventory payload is absent from journal")
    try:
        value = json.loads(payload)
        if isinstance(value, dict) and "inventory_journal_reference" in value:
            reference = value["inventory_journal_reference"]
            if not isinstance(reference, dict):
                raise ValueError("invalid reference")
            digest = reference.get("sha256")
            size, count = reference.get("byte_count"), reference.get("chunk_count")
            if (not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                or type(size) is not int or not INVENTORY_JOURNAL_CHUNK_BYTES < size <= MAX_CONTRACT_BYTES
                or type(count) is not int
                or count != (size + INVENTORY_JOURNAL_CHUNK_BYTES - 1) // INVENTORY_JOURNAL_CHUNK_BYTES):
                raise ValueError("invalid reference framing")
            chunks = []
            for index in range(count):
                chunk_record = journal.latest("executor", f"inventory:{digest}:{index}")
                chunk = None if chunk_record is None else chunk_record.durable_payload()
                if chunk is None or len(chunk) != min(INVENTORY_JOURNAL_CHUNK_BYTES,
                    size - index * INVENTORY_JOURNAL_CHUNK_BYTES):
                    raise ValueError("missing or invalid chunk")
                chunks.append(chunk)
            payload = b"".join(chunks)
            if hashlib.sha256(payload).hexdigest() != digest:
                raise ValueError("inventory digest changed")
        inventory = parse_executor_inventory(payload)
        if canonical_executable_bytes(inventory) != payload:
            raise ValueError("inventory bytes are not canonical")
        frames = _frames(inventory)
        if record.event_kind == "inventory-publish-requested":
            frames = frames[:-1]
        elif record.event_kind == "inventory-publish-rejected":
            _, kind, object_id, response = frames[-1]
            frames = (*frames[:-1], (record.event_kind, kind, object_id, response))
        elif record.event_kind != "inventory-publish-confirmed":
            raise ValueError("invalid inventory event")
        if record.sequence != inventory.journal_sequence + len(frames):
            raise ValueError("inventory batch sequence changed")
        journal.assert_evidence_covers(inventory.journal_sequence, inventory.journal_digest)
        previous = inventory.journal_digest
        for sequence, frame in enumerate(frames, start=inventory.journal_sequence + 1):
            event, kind, object_id, expected = frame
            candidates = [item for item in journal.records(kind, object_id) if item.sequence == sequence]
            if len(candidates) != 1:
                raise ValueError("inventory batch record missing")
            retained = candidates[0]
            if (retained.event_kind != event or retained.previous_digest != previous
                or retained.durable_payload() != expected
                or retained.payload_digest != hashlib.sha256(expected).hexdigest()):
                raise ValueError("inventory batch record changed")
            previous = retained.record_digest
        if record.record_digest != previous:
            raise ValueError("inventory final record changed")
        return inventory
    except (ValueError, TypeError, KeyError) as exc:
        raise JournalRegressionError("retained inventory batch is invalid") from exc
