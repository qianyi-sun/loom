from __future__ import annotations

import hashlib
import importlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
import rfc8785

from loom_task_image_authority.registry_token import publication_repository
from tests.unit.test_task_image_publication_contracts import unsigned_payload


def jobs():
    name = "loom_task_image_authority.publication_jobs"
    assert importlib.util.find_spec(name) is not None, "durable publication contracts missing"
    return importlib.import_module(name)


def snapshot_payload(count=1, observations=0):
    common = unsigned_payload()
    for name in (
        "schema",
        "component",
        "repository",
        "root",
        "manifest",
        "config",
        "layers",
        "observed_base_digests",
    ):
        common.pop(name)
    common["builder_id"] = "rootless:" + common["original_claim_session_id"].replace("-", "")
    components = []
    for index in range(count):
        component = "task" if index == 0 else f"sidecar:s{index:03d}"
        ack = {
            "schema_version": "loom.task-image-publication-candidate.v2",
            "candidate_id": str(UUID(int=index + 1)),
            "operation_id": str(UUID(int=index + 256)),
            "credential_id": str(UUID(int=index + 512)),
            "credential_generation": 1,
            "grant_id": common["grant_id"],
            "session_id": common["original_claim_session_id"],
            "session_generation": 1,
            "materialization_id": common["materialization_id"],
            "attempt_id": common["attempt_id"],
            "attempt_number": common["attempt_number"],
            "lease_epoch": common["lease_epoch"],
            "builder_id": common["builder_id"],
            "component": component,
            "repository": publication_repository(
                purpose="production",
                shadow_campaign_id=None,
                cpu_arch="arm64",
                attempt_id=UUID(common["attempt_id"]),
                component=component,
            ),
            "manifest_digest": "sha256:" + "1" * 64,
            "manifest_size": 512,
            "oci_file_sha256": "a" * 64,
            "oci_file_size": 1024,
            "platform": common["platform"],
            "recorded_at": "2026-09-08T00:00:00Z",
            "base_resolution": {
                "schema": "loom.task-image-base-resolution/v1",
                "solve_ref": "solve1",
                "platform": common["platform"],
                "output_digest": "sha256:" + "1" * 64,
                "observed_base_digests": ["sha256:" + f"{n + 1:064x}" for n in range(observations)],
            },
        }
        components.append(
            {"candidate": ack, "candidate_sha256": hashlib.sha256(rfc8785.dumps(ack)).hexdigest()}
        )
    return dict(common, schema="loom.task-image-publication-job-input/v1", components=components)


def test_complete_128_by_128_snapshot_roundtrip_and_ownership():
    j = jobs()
    payload = snapshot_payload(128, 128)
    encoded = rfc8785.dumps(payload)
    assert 65536 < len(encoded) < 4 * 1024**2
    snapshot = j.decode_publication_snapshot(encoded)
    assert j.canonical_snapshot_bytes(snapshot) == encoded
    payload["components"][0]["candidate"]["base_resolution"]["observed_base_digests"].clear()
    assert len(snapshot.components[0].candidate.base_resolution.observed_base_digests) == 128
    with pytest.raises(ValueError):
        snapshot.components[0].candidate.manifest_size = 5


@pytest.mark.parametrize(
    "mutation", ["unknown", "duplicate", "nonfinite", "hash", "binding", "oversize", "components"]
)
def test_snapshot_rejects_untrusted_bytes(mutation):
    j = jobs()
    payload = snapshot_payload()
    if mutation == "unknown":
        payload["unexpected"] = "value"
    if mutation == "hash":
        payload["components"][0]["candidate_sha256"] = "f" * 64
    if mutation == "binding":
        payload["lease_epoch"] += 1
    if mutation == "components":
        payload = snapshot_payload(129)
    encoded = json.dumps(payload).encode()
    if mutation == "duplicate":
        encoded = encoded.replace(b'"lease_epoch": 2', b'"lease_epoch": 2, "lease_epoch": 2', 1)
    if mutation == "nonfinite":
        encoded = encoded.replace(b'"lease_epoch": 2', b'"lease_epoch": NaN', 1)
    if mutation == "oversize":
        encoded += b" " * (4 * 1024**2)
    with pytest.raises(ValueError):
        j.decode_publication_snapshot(encoded)


def test_worker_lease_rejects_naive_time():
    j = jobs()
    with pytest.raises(ValueError):
        j.PublicationWorkerLease(
            owner_id=str(UUID(int=1)), generation=1, expires_at=datetime(2026, 9, 8)
        )


@pytest.mark.parametrize(
    "mutation",
    ["running_without_owner", "queued_with_owner", "expired_deadline", "generation", "hash"],
)
def test_job_contract_rejects_inconsistent_state(mutation):
    j = jobs()
    now = datetime(2026, 9, 8, tzinfo=UTC)
    snapshot = j.decode_publication_snapshot(rfc8785.dumps(snapshot_payload()))
    values = dict(
        operation_id=str(UUID(int=99)),
        state="queued",
        snapshot=snapshot,
        snapshot_sha256=hashlib.sha256(j.canonical_snapshot_bytes(snapshot)).hexdigest(),
        created_at=now,
        deadline=now + timedelta(seconds=3600),
        available_at=now,
        worker_generation=0,
    )
    if mutation == "running_without_owner":
        values["state"] = "running"
    elif mutation in ("queued_with_owner", "generation"):
        values["lease"] = j.PublicationWorkerLease(
            owner_id=str(UUID(int=1)), generation=1, expires_at=now + timedelta(seconds=60)
        )
        if mutation == "generation":
            values["state"] = "running"
    elif mutation == "expired_deadline":
        values["deadline"] = now
    else:
        values["snapshot_sha256"] = "e" * 64
    with pytest.raises(ValueError):
        j.PublicationJob.model_validate(values)
