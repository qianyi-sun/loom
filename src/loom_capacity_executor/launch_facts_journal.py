"""Retain canonical launch facts without widening executor journal record limits.

The complete facts remain in the same locked, hash-chained, fsync'd journal.
An envelope references their digest and framing, never a mutable manager lookup.
Interrupted retention is safe to finish before consuming a launch permit.
"""

from __future__ import annotations

import hashlib
from typing import Annotated, Literal

from pydantic import Field, field_validator, model_validator

from loom_capacity_executor.journal import ExecutorJournal, JournalRegressionError
from loom_capacity_manager.contracts import Digest
from loom_capacity_manager.executable_contracts import StrictV2Model
from loom_capacity_manager.launch_subject_contracts import (
    MAX_LAUNCH_SUBJECT_BYTES,
    ExecutableLaunchSubjectV3,
    canonical_launch_subject_bytes,
    parse_launch_subject,
)

LAUNCH_FACTS_CHUNK_BYTES = 32 * 1024
_MIN_RECOVERY_RESERVE_BYTES = 8 * 1024 * 1024
# Conservative allowance for a next recovery pass per retained job, including
# the prospective new launch. Terminal history remains counted until compaction.
_PER_JOB_RECOVERY_RESERVE_BYTES = 16 * 64 * 1024


class LaunchFactsReferenceV3(StrictV2Model):
    schema_version: Literal[3] = 3  # type: ignore[assignment]
    sha256: Digest
    byte_count: Annotated[int, Field(strict=True, gt=0, le=MAX_LAUNCH_SUBJECT_BYTES)]
    chunk_count: Annotated[int, Field(strict=True, gt=0, le=MAX_LAUNCH_SUBJECT_BYTES)]

    @field_validator("schema_version", mode="before")
    @classmethod
    def _exact_version(cls, value: object) -> object:
        if type(value) is not int or value != 3:
            raise ValueError("launch facts reference requires integer schema 3")
        return value

    @model_validator(mode="after")
    def _exact_framing(self) -> LaunchFactsReferenceV3:
        if (
            self.chunk_count
            != (self.byte_count + LAUNCH_FACTS_CHUNK_BYTES - 1) // LAUNCH_FACTS_CHUNK_BYTES
        ):
            raise ValueError("launch facts chunk count differs from byte count")
        return self


def _chunk_id(digest: str, index: int) -> str:
    return f"launch-facts:{digest}:{index}"


def retain_launch_facts(
    journal: ExecutorJournal,
    value: ExecutableLaunchSubjectV3,
) -> LaunchFactsReferenceV3:
    payload = canonical_launch_subject_bytes(value)
    digest = hashlib.sha256(payload).hexdigest()
    reference = LaunchFactsReferenceV3(
        sha256=digest,
        byte_count=len(payload),
        chunk_count=(len(payload) + LAUNCH_FACTS_CHUNK_BYTES - 1) // LAUNCH_FACTS_CHUNK_BYTES,
    )
    missing = []
    for index in range(reference.chunk_count):
        chunk = payload[index * LAUNCH_FACTS_CHUNK_BYTES : (index + 1) * LAUNCH_FACTS_CHUNK_BYTES]
        object_id = _chunk_id(digest, index)
        retained = journal.latest("executor", object_id)
        chunk_digest = hashlib.sha256(chunk).hexdigest()
        if retained is not None:
            if (
                retained.event_kind != "launch-facts-retained"
                or retained.payload_digest != chunk_digest
                or retained.durable_payload() != chunk
            ):
                raise JournalRegressionError("retained launch facts chunk changed")
            continue
        missing.append((object_id, chunk_digest, chunk))
    if missing:
        journal.assert_payload_capacity(
            tuple(len(chunk) for _, _, chunk in missing),
            reserved_bytes=max(
                _MIN_RECOVERY_RESERVE_BYTES,
                (len(journal.latest_records("job")) + 1) * _PER_JOB_RECOVERY_RESERVE_BYTES,
            ),
        )
    for object_id, chunk_digest, chunk in missing:
        journal.append(
            "launch-facts-retained",
            chunk_digest,
            object_kind="executor",
            object_id=object_id,
            payload=chunk,
        )
    return reference


def load_launch_facts(
    journal: ExecutorJournal,
    reference: LaunchFactsReferenceV3,
) -> ExecutableLaunchSubjectV3:
    if type(reference) is not LaunchFactsReferenceV3:
        raise ValueError("launch facts reference requires its exact contract")
    reference = LaunchFactsReferenceV3.model_validate_json(reference.model_dump_json())
    chunks = []
    for index in range(reference.chunk_count):
        record = journal.latest("executor", _chunk_id(reference.sha256, index))
        if record is None or record.event_kind != "launch-facts-retained":
            raise JournalRegressionError("retained launch facts chunk is absent")
        chunk = record.durable_payload()
        expected_size = min(
            LAUNCH_FACTS_CHUNK_BYTES, reference.byte_count - index * LAUNCH_FACTS_CHUNK_BYTES
        )
        if (
            chunk is None
            or len(chunk) != expected_size
            or hashlib.sha256(chunk).hexdigest() != record.payload_digest
        ):
            raise JournalRegressionError("retained launch facts chunk changed")
        chunks.append(chunk)
    payload = b"".join(chunks)
    if hashlib.sha256(payload).hexdigest() != reference.sha256:
        raise JournalRegressionError("retained launch facts digest changed")
    try:
        return parse_launch_subject(payload)
    except ValueError as exc:
        raise JournalRegressionError("retained launch facts contract is invalid") from exc
