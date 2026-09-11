"""Strict snapshot decoding for manager-acknowledged journal checkpoints."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass

from loom_capacity_executor.journal import (
    _DIGEST_RE,
    _MAX_JOURNAL_BYTES,
    _MAX_RECORD_BYTES,
    _MAX_RECORDS,
    ExecutorJournal,
    JournalCorruptionError,
    JournalRecord,
    _canonical_bytes,
    _open_regular,
)


@dataclass(frozen=True, slots=True)
class CheckpointSnapshot:
    records: tuple[JournalRecord, ...]
    anchors: tuple[tuple[int, str], ...]
    reserved_bytes: int


def read_checkpoint_snapshot(
    journal: ExecutorJournal, value: object,
) -> tuple[JournalRecord, CheckpointSnapshot]:
    try:
        if not isinstance(value, dict):
            raise ValueError("invalid checkpoint record")
        checkpoint = journal._validate_record(value, value["sequence"], value["previous_digest"])
        payload = checkpoint.durable_payload()
        if (checkpoint.schema_version != 2 or checkpoint.object_kind != "executor"
            or checkpoint.event_kind != "journal-checkpoint-prepared" or payload is None):
            raise ValueError("invalid checkpoint record binding")
        reference = json.loads(payload)
        if not isinstance(reference, dict) or set(reference) != {"snapshot_sha256"}:
            raise ValueError("invalid snapshot reference")
        digest = reference["snapshot_sha256"]
        if not isinstance(digest, str) or _DIGEST_RE.fullmatch(digest) is None:
            raise ValueError("invalid snapshot digest")
        if (checkpoint.object_id != f"checkpoint:{digest}"
            or payload != _canonical_bytes(reference)):
            raise ValueError("invalid checkpoint reference binding")
        descriptor = _open_regular(journal._snapshot_path(digest), create=False)
        try:
            size = os.fstat(descriptor).st_size
            if size > _MAX_JOURNAL_BYTES:
                raise ValueError("snapshot exceeds size bound")
            chunks = []
            remaining = size
            while remaining:
                chunk = os.read(descriptor, min(remaining, 1_048_576))
                if not chunk:
                    raise ValueError("snapshot truncated")
                chunks.append(chunk)
                remaining -= len(chunk)
            encoded = b"".join(chunks)
        finally:
            os.close(descriptor)
        if hashlib.sha256(encoded).hexdigest() != digest:
            raise ValueError("snapshot digest changed")
        snapshot = json.loads(encoded)
        if (not isinstance(snapshot, dict) or set(snapshot) != {
            "schema_version", "head_sequence", "head_digest", "reserved_bytes", "records", "anchors",
        } or encoded != _canonical_bytes(snapshot)):
            raise ValueError("snapshot fields are invalid")
        head, head_digest = snapshot["head_sequence"], snapshot["head_digest"]
        reserve = snapshot["reserved_bytes"]
        if (type(snapshot["schema_version"]) is not int or snapshot["schema_version"] != 1
            or type(head) is not int or not 0 <= head < (1 << 63) - 1
            or type(reserve) is not int or not 0 <= reserve <= _MAX_JOURNAL_BYTES
            or checkpoint.sequence != head + 1 or checkpoint.previous_digest != head_digest):
            raise ValueError("snapshot anchor is invalid")
        raw_anchors = snapshot["anchors"]
        raw_records = snapshot["records"]
        if (not isinstance(raw_anchors, list) or len(raw_anchors) > 2 * _MAX_RECORDS + 2
            or not isinstance(raw_records, list) or len(raw_records) >= _MAX_RECORDS):
            raise ValueError("snapshot collections exceed bounds")
        anchors: dict[int, str] = {}
        previous_sequence = -1
        for item in raw_anchors:
            if (not isinstance(item, list) or len(item) != 2 or type(item[0]) is not int
                or not previous_sequence < item[0] <= head or not isinstance(item[1], str)
                or _DIGEST_RE.fullmatch(item[1]) is None):
                raise ValueError("invalid snapshot anchor")
            sequence, anchor = item
            anchors[sequence] = anchor
            previous_sequence = sequence
        if anchors.get(0) != "0" * 64 or anchors.get(head) != head_digest:
            raise ValueError("snapshot does not bind its head")
        records = []
        previous_sequence = 0
        latest = {}
        for item in raw_records:
            if not isinstance(item, dict) or len(_canonical_bytes(item)) + 1 > _MAX_RECORD_BYTES:
                raise ValueError("invalid snapshot record")
            record = journal._validate_record(item, item["sequence"], item["previous_digest"])
            if (not previous_sequence < record.sequence <= head
                or anchors.get(record.sequence) != record.record_digest
                or anchors.get(record.sequence - 1) != record.previous_digest
                or record.event_kind == "journal-checkpoint-prepared"):
                raise ValueError("snapshot record anchor is invalid or recursive")
            previous_sequence = record.sequence
            records.append(record)
            latest[(record.object_kind, record.object_id)] = record
        if any(record.event_kind.endswith("-requested") or record.event_kind == "heartbeat-received"
            for record in latest.values()):
            raise ValueError("snapshot resurrects unresolved work")
        return checkpoint, CheckpointSnapshot(tuple(records), tuple(anchors.items()), reserve)
    except (ValueError, TypeError, KeyError, OSError) as exc:
        raise JournalCorruptionError("checkpoint snapshot is invalid") from exc
