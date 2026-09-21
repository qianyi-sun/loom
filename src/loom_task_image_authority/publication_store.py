"""Validate retained publication job records and their signed receipt bindings."""

from __future__ import annotations

import hashlib
from datetime import timedelta
from typing import Any
from uuid import UUID

from loom.db.schema import (
    TaskImagePublicationJob,
)
from loom_task_image_authority.publication_jobs import (
    PublicationJob,
    PublicationJobConflictError,
    PublicationWorkerLease,
    canonical_snapshot_bytes,
    decode_publication_snapshot,
)
from loom_task_image_authority.publication_receipts import (
    canonical_receipt_bytes,
    decode_publication_receipt,
)


def _uuid(value: UUID) -> UUID:
    if type(value) is not UUID or value.int == 0:
        raise ValueError("publication identity must be a nonzero UUID")
    return value


def _result(row: TaskImagePublicationJob) -> PublicationJob:
    try:
        snapshot = decode_publication_snapshot(row.canonical_snapshot)
        if (
            canonical_snapshot_bytes(snapshot) != row.canonical_snapshot
            or hashlib.sha256(row.canonical_snapshot).hexdigest() != row.snapshot_sha256
        ):
            raise ValueError("snapshot digest mismatch")
        for field, source in (
            ("attempt_id", "materialization_attempt_id"),
            ("materialization_id", "materialization_id"),
            ("attempt_number", "attempt_number"),
            ("lease_epoch", "lease_epoch"),
            ("builder_id", "builder_id"),
            ("grant_id", "grant_id"),
        ):
            if str(getattr(snapshot, field)) != str(getattr(row, source)):
                raise ValueError("snapshot row binding mismatch")
        lease = None
        if row.state == "running":
            if row.worker_id is None or row.worker_expires_at is None:
                raise ValueError("missing worker fence")
            lease = PublicationWorkerLease(
                owner_id=str(row.worker_id),
                generation=row.worker_generation,
                expires_at=row.worker_expires_at,
            )
        elif row.worker_id is not None or row.worker_expires_at is not None:
            raise ValueError("unexpected worker fence")
        if (
            (row.state == "failed") != (row.failure_code is not None)
            or not row.created_at < row.deadline <= row.created_at + timedelta(seconds=7200)
            or row.available_at < row.created_at
            or (lease is not None and lease.expires_at > row.deadline)
        ):
            raise ValueError("invalid job state")
        values: dict[str, Any] = dict(
            operation_id=str(row.operation_id),
            state=row.state,
            snapshot_sha256=row.snapshot_sha256,
            snapshot=snapshot,
            created_at=row.created_at,
            deadline=row.deadline,
            available_at=row.available_at,
            worker_generation=row.worker_generation,
        )
        if lease is not None:
            values["lease"] = lease
        if row.failure_code is not None:
            values["failure_code"] = row.failure_code
        if row.state == "completed":
            if (
                row.completed_at is None
                or row.canonical_receipt is None
                or row.receipt_sha256 is None
            ):
                raise ValueError("publication completion receipt missing")
            receipt = decode_publication_receipt(row.canonical_receipt)
            if (
                canonical_receipt_bytes(receipt) != row.canonical_receipt
                or hashlib.sha256(row.canonical_receipt).hexdigest() != row.receipt_sha256
                or receipt.operation_id != str(row.operation_id)
                or receipt.materialization_id != str(row.materialization_id)
                or receipt.attempt_id != str(row.materialization_attempt_id)
                or receipt.lease_epoch != row.lease_epoch
                or receipt.worker_generation != row.worker_generation
                or receipt.snapshot_sha256 != row.snapshot_sha256
                or receipt.component_count != len(snapshot.components)
                or receipt.completed_at != row.completed_at.strftime("%Y-%m-%dT%H:%M:%SZ")
                or not row.created_at <= row.completed_at < row.deadline
            ):
                raise ValueError("publication completion binding changed")
        elif any(
            value is not None
            for value in (row.completed_at, row.canonical_receipt, row.receipt_sha256)
        ):
            raise ValueError("unexpected publication completion receipt")
        return PublicationJob.model_validate(values)
    except (ValueError, TypeError, OverflowError) as exc:
        raise PublicationJobConflictError("stored publication job changed") from exc
