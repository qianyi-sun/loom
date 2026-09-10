"""Registered publisher -> real HTTP native claim -> configured TLS MinIO bundle."""

import asyncio
import hashlib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Event
from uuid import uuid4

import pytest

from loom.task_bundle_registration import prepare_task_bundle_registration
from loom.task_bundle_source import TaskBundleSourceSpecV1
from loom.task_bundle_source_journal import publish_task_bundle_source
from loom.task_bundle_source_publisher import TaskBundleSourcePublisher
from loom.task_image_materialization import ensure_task_image_materializations
from loom_task_image_authority.api import create_app
from loom_task_image_authority.bundle_capability import TaskImageBundleCapabilityV2
from loom_task_image_authority.bundle_s3_backend import MinioTaskImageBundleBackend
from loom_task_image_authority.http_contracts import TaskImageMaterializationClaimResponseV1
from tests.integration import test_task_image_authority_api as api_helpers
from tests.integration import test_task_image_projection_store as projection_helpers
from tests.integration.test_task_bundle_source_admission import _task
from tests.integration.test_task_bundle_source_admission import journal as journal
from tests.integration.test_task_bundle_source_storage import _store
from tests.integration.test_task_image_bundle_minio_signing import minio_tls as minio_tls
from tests.integration.test_task_image_bundle_runtime import _configured
from tests.integration.test_task_image_native_source_admission import _release_source
from tests.unit.test_task_bundle_registration import _bundle

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


@pytest.mark.parametrize("source_loss", ["none", "before", "during", "replay"])
async def test_registered_native_http_bundle_rechecks_source_and_preserves_exact_replay(
    journal, tmp_path, minio_tls, monkeypatch, source_loss,
):
    directory = _bundle(tmp_path)
    config = directory / "task.toml"
    config.write_text(config.read_text().replace("[environment]", '[environment]\ncpu_arch = "arm64"'))
    spec = TaskBundleSourceSpecV1.from_registration(
        prepare_task_bundle_registration(directory, task_id="benchmark/" + uuid4().hex),
        bucket="loom-bundles",
    )
    minio_tls[3].put_bucket_versioning(Bucket=spec.bucket, VersioningConfiguration={"Status": "Enabled"})
    publisher = TaskBundleSourcePublisher(journal, _store(minio_tls), clock=lambda: datetime.now(UTC))
    ticket = await publisher.prepare(spec, directory)
    async with journal.begin() as session:
        await publish_task_bundle_source(
            session, incarnation_id=ticket.incarnation_id, reference_kind="catalog",
            owner_id="catalog", now=datetime.now(UTC),
        )
        image_id = (await ensure_task_image_materializations(session, task_row=_task(spec)))[0].id

    # Reuse the projection request fixtures at a real wall-clock epoch. Explicit
    # expiry avoids their historical default argument; storage/signing/TLS and
    # source publication are not mocked or weakened for the authority clock.
    epoch = datetime.now(UTC) - timedelta(seconds=14)
    monkeypatch.setattr(projection_helpers, "NOW", epoch)
    monkeypatch.setattr(api_helpers, "NOW", epoch)
    async with journal.begin() as session:
        await projection_helpers._release_grant(session, expires_at=epoch + timedelta(hours=2))
    settings = _configured(tmp_path, minio_tls, api_helpers._settings(
        tmp_path, journal.kw["bind"].url.render_as_string(hide_password=False),
    ).model_dump(mode="python"))
    current = [epoch + timedelta(seconds=4)]
    app = create_app(settings, now_factory=lambda: current[0], challenge_nonce_factory=lambda: projection_helpers.CHALLENGE_NONCE)
    entered, release = Event(), Event()
    original = MinioTaskImageBundleBackend.get_verified_bundle_manifest
    reads = []

    async def guarded_read(backend, **kwargs):
        reads.append(kwargs)
        result = await original(backend, **kwargs)
        if source_loss == "during":
            entered.set()
            while not release.is_set():
                await asyncio.sleep(0.01)
        return result

    monkeypatch.setattr(MinioTaskImageBundleBackend, "get_verified_bundle_manifest", guarded_read)
    with api_helpers.TestClient(app) as client:
        assert client.get("/healthz").status_code == 200
        context = api_helpers._ApiContext(client, app, current, settings, api_helpers._FakeBundleBackend())
        build_session = api_helpers._renewed_session(context)
        response = api_helpers._post(context, f"/v1/projections/{projection_helpers.GRANT_ID}/materializations/claim", api_helpers._claim_request(build_session))
        assert response.status_code == 200, response.text
        claim = TaskImageMaterializationClaimResponseV1.model_validate_json(response.content)
        assert claim.materialization_id == image_id
        assert claim.plan.content_manifest_digest == spec.manifest.digest
        path = f"/v1/projections/{projection_helpers.GRANT_ID}/materializations/{image_id}/bundle"
        body = api_helpers._operation_request(build_session, claim, operation_id=uuid4()).model_dump(mode="json")
        if source_loss == "before":
            await _release_source(journal, spec, ticket, image_id, retire=True)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(client.put, path, headers=api_helpers._HEADERS, json=body)
            try:
                if source_loss == "during":
                    assert entered.wait(10), "configured backend never read the registered manifest"
                    await _release_source(journal, spec, ticket, image_id, retire=True)
            finally:
                release.set()
            response = pending.result(timeout=15)
        if source_loss in {"before", "during"}:
            assert response.status_code == 409, response.text
            assert len(reads) == (0 if source_loss == "before" else 1)
            return
        assert response.status_code == 200, response.text
        capability = TaskImageBundleCapabilityV2.model_validate_json(response.content)
        assert capability.content_manifest == spec.manifest
        for item in capability.objects:
            downloaded = minio_tls[2].get(item.url)
            assert downloaded.status_code == 200
            assert hashlib.sha256(downloaded.content).hexdigest() == item.sha256
        if source_loss == "replay":
            await _release_source(journal, spec, ticket, image_id, retire=True)
        current[0] += timedelta(seconds=1)
        replay = client.put(path, headers=api_helpers._HEADERS, json=body)
        assert replay.status_code == (409 if source_loss == "replay" else 200)
        if source_loss == "none":
            assert replay.content == response.content
            claim_replay = api_helpers._post(context, f"/v1/projections/{projection_helpers.GRANT_ID}/materializations/claim", api_helpers._claim_request(build_session))
            assert claim_replay.status_code == 200
            assert TaskImageMaterializationClaimResponseV1.model_validate_json(claim_replay.content) == claim
        assert len(reads) == 1, "retained bundle replay re-read storage"
