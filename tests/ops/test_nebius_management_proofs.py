"""Installed proof needs authenticated management and actual off-node dump bytes."""
from __future__ import annotations

import hashlib
import io
import json
from uuid import uuid4

import httpx
import pytest


def test_public_probe_checks_management_readiness_and_auth_without_workload_routes():
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe

    requests = []

    def handle(request):
        requests.append(request)
        if request.url.path.endswith("/health/ready"):
            assert "authorization" not in request.headers
            return httpx.Response(200, json={"status": "ready", "mode": "management", "postgres": "ready", "provisioner": "ready"})
        if request.url.path == "/api/v1/tasks":
            return httpx.Response(404)
        if request.headers.get("authorization") == "Bearer private-admin":
            return httpx.Response(200, json={"items": [], "next_cursor": None})
        return httpx.Response(401, json={"detail": "unauthorized"})

    with ManagementPublicProbe(host="manage.example.com") as probe:
        probe.client.close()
        probe.client = httpx.Client(transport=httpx.MockTransport(handle))
        probe.verify(admin_token="private-admin")
    assert len([request for request in requests if request.headers.get("authorization") == "Bearer private-admin"]) == 1
    assert all(request.method == "GET" for request in requests)


@pytest.mark.parametrize("failure", ["wrong_mode", "worker_missing", "anonymous_allowed", "redirect"])
def test_public_probe_rejects_wrong_service_auth_bypass_or_redirect(failure):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_proofs import ManagementPublicProbe

    sent_admin = []

    def handle(request):
        sent_admin.append(request.headers.get("authorization") == "Bearer private-admin")
        if failure == "redirect":
            return httpx.Response(302, headers={"Location": "https://foreign.example/token"})
        if request.url.path.endswith("/health/ready"):
            value = {"status": "ready", "mode": "management", "postgres": "ready", "provisioner": "ready"}
            if failure == "wrong_mode":
                value["mode"] = "service"
            if failure == "worker_missing":
                del value["provisioner"]
            return httpx.Response(200, json=value)
        return httpx.Response(200, json={"items": []})

    with ManagementPublicProbe(host="manage.example.com") as probe:
        probe.client.close()
        probe.client = httpx.Client(transport=httpx.MockTransport(handle))
        with pytest.raises(ManagementInstallError):
            probe.verify(admin_token="private-admin")
    assert not any(sent_admin)


class Objects:
    def __init__(self, payload, checksum):
        self.payload = payload
        self.checksum = checksum
        self.calls = []
        self.body = io.BytesIO(payload)

    def head_object(self, **kwargs):
        self.calls.append(("head", kwargs))
        return {"ContentLength": len(self.payload), "ETag": '"object-etag"', "Metadata": {"Sha256": self.checksum}}

    def get_object(self, **kwargs):
        self.calls.append(("get", kwargs))
        return {"ContentLength": len(self.payload), "ETag": '"object-etag"', "Body": self.body}


@pytest.fixture
def backup():
    payload = b"PGDMP" + b"real-streamed-test-content" * 100
    checksum = hashlib.sha256(payload).hexdigest()
    return Objects(payload, checksum), {"backup_key": "loom-nebius-management/2026/09/24/180000-" + checksum[:12] + ".dump",
                                       "sha256": checksum, "bytes": len(payload)}


def test_backup_proof_hashes_streamed_object_and_pins_object_version(backup):
    from scripts.ops.nebius_management_proofs import verify_backup_object

    objects, report = backup
    job = str(uuid4())
    proof = verify_backup_object(client=objects, bucket="management-backup", namespace="loom-nebius-management",
                                 job_uid=job, report=report, max_bytes=10000)
    assert proof == {"key": report["backup_key"], "sha256": report["sha256"], "bytes": report["bytes"], "job_uid": job}
    assert objects.calls[1] == ("get", {"Bucket": "management-backup", "Key": report["backup_key"], "IfMatch": '"object-etag"'})
    assert objects.body.closed


@pytest.mark.parametrize("failure", ["foreign_key", "oversized", "digest", "metadata", "not_dump"])
def test_backup_metadata_or_successful_job_alone_cannot_prove_dump(backup, failure):
    from scripts.ops.nebius_management_install import ManagementInstallError
    from scripts.ops.nebius_management_proofs import verify_backup_object

    objects, report = backup
    if failure == "foreign_key":
        report["backup_key"] = report["backup_key"].replace("loom-nebius-management/", "foreign/")
    elif failure == "oversized":
        report["bytes"] = 10001
    elif failure == "digest":
        objects.body = io.BytesIO(b"PGDMP" + b"x" * (report["bytes"] - 5))
    elif failure == "metadata":
        objects.checksum = "0" * 64
    else:
        objects.payload = b"not-a-custom-postgres-dump"
        objects.body = io.BytesIO(objects.payload)
        objects.checksum = hashlib.sha256(objects.payload).hexdigest()
        report.update(sha256=objects.checksum, bytes=len(objects.payload))
        report["backup_key"] = "loom-nebius-management/2026/09/24/180000-" + objects.checksum[:12] + ".dump"
    with pytest.raises(ManagementInstallError) as error:
        verify_backup_object(client=objects, bucket="management-backup", namespace="loom-nebius-management",
                             job_uid=str(uuid4()), report=report, max_bytes=10000)
    assert report["backup_key"] not in str(error.value)
    if failure in {"foreign_key", "oversized"}:
        assert not objects.calls
    assert "private-admin" not in json.dumps(objects.calls)
