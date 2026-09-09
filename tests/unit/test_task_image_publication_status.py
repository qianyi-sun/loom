from __future__ import annotations

import hashlib
import importlib
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import rfc8785

from loom_task_image_authority.publication_jobs import (
    PublicationJob,
    PublicationWorkerLease,
    canonical_snapshot_bytes,
    decode_publication_snapshot,
)
from loom_task_image_authority.publication_receipts import (
    PublicationCandidateIdentity,
    PublicationReceipt,
    candidate_set_sha256,
)
from tests.unit.test_task_image_publication_jobs import snapshot_payload


def status():
    name = "loom_task_image_authority.publication_status"
    assert importlib.util.find_spec(name) is not None, "bounded publication status missing"
    return importlib.import_module(name)


def _job(state="queued", count=1, observations=0):
    snapshot = decode_publication_snapshot(rfc8785.dumps(snapshot_payload(count, observations)))
    now = datetime(2026, 9, 8, tzinfo=UTC)
    fields = dict(
        operation_id=str(UUID(int=99)),
        state=state,
        snapshot_sha256=hashlib.sha256(canonical_snapshot_bytes(snapshot)).hexdigest(),
        snapshot=snapshot,
        created_at=now,
        deadline=now + timedelta(hours=1),
        available_at=now,
        worker_generation=1,
    )
    if state == "running":
        fields["lease"] = PublicationWorkerLease(
            owner_id=str(UUID(int=98)), generation=1, expires_at=now + timedelta(seconds=60)
        )
    if state == "failed":
        fields["failure_code"] = "integrity"
    return PublicationJob.model_validate(fields)


def _receipt(job):
    return PublicationReceipt.model_validate(
        dict(
            schema="loom.task-image-publication-receipt/v1",
            operation_id=job.operation_id,
            materialization_id=job.snapshot.materialization_id,
            attempt_id=job.snapshot.attempt_id,
            lease_epoch=job.snapshot.lease_epoch,
            worker_generation=job.worker_generation,
            snapshot_sha256=job.snapshot_sha256,
            candidate_set_sha256=candidate_set_sha256(
                tuple(
                    PublicationCandidateIdentity(
                        candidate_id=str(item.candidate.candidate_id),
                        component=item.candidate.component,
                    )
                    for item in job.snapshot.components
                )
            ),
            publication_set_sha256="a" * 64,
            component_count=len(job.snapshot.components),
            completed_at="2026-09-08T00:00:14Z",
        )
    )


@pytest.mark.parametrize("state", ["queued", "running", "failed", "completed"])
def test_status_is_bounded_projection_of_maximum_durable_job(state):
    s = status()
    job = _job(state, 128, 128)
    receipt = _receipt(job) if state == "completed" else None
    projected = s.project_publication_status(job, receipt=receipt)
    wire = s.canonical_status_bytes(projected)
    assert s.decode_publication_status(wire) == projected
    assert len(wire) < s.MAX_STATUS_BYTES <= 4096
    assert projected.component_count == 128
    assert projected.grant_id == job.snapshot.grant_id
    assert projected.candidate_set_sha256 == _receipt(job).candidate_set_sha256
    for private_field in (
        b'"snapshot":',
        b'"components":',
        b'"registry_origin":',
        b'"lease":',
        b'"session_token":',
    ):
        assert private_field not in wire
    assert projected.receipt == receipt
    assert projected.failure_code == ("integrity" if state == "failed" else None)


@pytest.mark.parametrize("state", ["queued", "running", "failed", "completed"])
def test_status_rejects_wrong_terminal_receipt_presence(state):
    s = status()
    job = _job(state)
    with pytest.raises(ValueError):
        s.project_publication_status(job, receipt=None if state == "completed" else _receipt(job))


@pytest.mark.parametrize(
    "field",
    [
        "operation_id",
        "materialization_id",
        "attempt_id",
        "lease_epoch",
        "worker_generation",
        "snapshot_sha256",
        "candidate_set_sha256",
        "component_count",
    ],
)
def test_status_rejects_receipt_substitution(field):
    s = status()
    job = _job("completed")
    receipt = _receipt(job)
    previous = getattr(receipt, field)
    changed = (
        previous + 1
        if isinstance(previous, int)
        else ("f" * 64 if field.endswith("sha256") else str(UUID(int=777)))
    )
    with pytest.raises(ValueError):
        s.project_publication_status(job, receipt=receipt.model_copy(update={field: changed}))


@pytest.mark.parametrize(
    "mutation",
    ["unknown", "null", "duplicate", "spacing", "oversize", "state", "receipt", "failure", "bool"],
)
def test_status_strict_wire_rejects_malformed_shape(mutation):
    s = status()
    projected = s.project_publication_status(_job())
    fields = projected.model_dump(mode="json", by_alias=True, exclude_none=True)
    if mutation == "unknown":
        fields["snapshot"] = {}
    elif mutation == "null":
        fields["receipt"] = None
    elif mutation == "state":
        fields["state"] = "completed"
    elif mutation == "receipt":
        fields["receipt"] = _receipt(_job()).model_dump(mode="json", by_alias=True)
    elif mutation == "failure":
        fields["failure_code"] = "integrity"
    elif mutation == "bool":
        fields["lease_epoch"] = True
    wire = rfc8785.dumps(fields)
    if mutation == "duplicate":
        wire = b'{"state":"queued",' + wire[1:]
    elif mutation == "spacing":
        wire += b" "
    elif mutation == "oversize":
        wire += b" " * s.MAX_STATUS_BYTES
    with pytest.raises(ValueError):
        s.decode_publication_status(wire)


def test_status_revalidates_unchecked_job_and_status_instances():
    s = status()
    job = _job()
    with pytest.raises(ValueError):
        s.project_publication_status(job.model_copy(update={"snapshot_sha256": "f" * 64}))
    projected = s.project_publication_status(job)
    with pytest.raises(ValueError):
        s.canonical_status_bytes(projected.model_copy(update={"state": "completed"}))
