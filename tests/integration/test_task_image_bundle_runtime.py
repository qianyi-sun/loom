"""Configured runtime: real TLS storage plus HTTP/SQL lifecycle composition."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from loom_task_image_authority.api import create_app
from loom_task_image_authority.bundle_capability import TaskImageBundleCapabilityV1
from loom_task_image_authority.bundle_s3_backend import MinioTaskImageBundleBackend
from loom_task_image_authority.bundle_s3_transport import HTTPSBundleListingReader
from loom_task_image_authority.config import TaskImageAuthoritySettings
from tests.integration.test_task_image_authority_api import (
    _HEADERS,
    TestClient,
    _operation_request,
    _owner_file,
)
from tests.integration.test_task_image_authority_api import authority_api as authority_api
from tests.integration.test_task_image_authority_api import (
    registry_token_issuer as registry_token_issuer,
)
from tests.integration.test_task_image_bundle_minio_signing import KEYS, PAYLOAD
from tests.integration.test_task_image_bundle_minio_signing import minio_tls as minio_tls
from tests.integration.test_task_image_bundle_unlocked_issuance import _claim
from tests.integration.test_task_image_projection_store import GRANT_ID
from tests.unit.test_task_image_authority_config import _settings_values
from tests.unit.test_task_image_bundle_capability import _plan

pytestmark = [pytest.mark.docker, pytest.mark.timeout(120)]


def _configured(tmp_path, minio_tls, values):
    identity = _owner_file(tmp_path / "native-bundle-identity.json", json.dumps(dict(
        schema_version=1, access_key=minio_tls[1].access_key, secret_key=minio_tls[1].secret_key,
    )))
    return TaskImageAuthoritySettings(**dict(
        values, bundle_backend="minio", bundle_public_https_origin=minio_tls[0],
        bundle_expected_bucket="loom-bundles", bundle_region="us-east-1",
        bundle_credentials_file=identity, bundle_reader_ca_file=minio_tls[4],
    ))


async def test_configured_runtime_issues_capabilities_accepted_by_real_tls_minio(tmp_path, minio_tls):
    from loom_task_image_authority.bundle_runtime import configured_bundle_provider

    settings = _configured(tmp_path, minio_tls, _settings_values(tmp_path))
    now = datetime.now(UTC)
    plan = _plan(bundle_prefix="revision/", authorization_expires_at=now + timedelta(seconds=60))
    async with configured_bundle_provider(settings) as provider:
        assert provider is not None
        capability = await provider.issue(plan, now=now)
        assert {plan.bundle_prefix + item.relative_path for item in capability.objects} == set(KEYS)
        for item in capability.objects:
            response = minio_tls[2].get(item.url)
            assert response.status_code == 200 and response.content == PAYLOAD
    with pytest.raises(RuntimeError):
        await provider.issue(plan, now=datetime.now(UTC))


async def test_http_lifespan_constructs_native_provider_and_recreates_closed_backend(authority_api, isolated_migration_postgres_url, tmp_path, minio_tls, monkeypatch):
    context = authority_api
    build_session, claim = await _claim(context, isolated_migration_postgres_url)
    settings = _configured(tmp_path, minio_tls, context.settings.model_dump(mode="python"))
    reads, closed = [], []
    original_close = MinioTaskImageBundleBackend.aclose

    async def fetch(reader, url, *, deadline):
        # This HTTP/SQL fixture uses the authority's historical clock. Stub only
        # the wire read; the separate test above uses real wall time + TLS MinIO.
        reads.append(reader)
        return b'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>loom-bundles</Name><Prefix>phase2c/session-bound/</Prefix><MaxKeys>256</MaxKeys><KeyCount>1</KeyCount><EncodingType>url</EncodingType><IsTruncated>false</IsTruncated><Contents><Key>phase2c/session-bound/task.toml</Key><Size>20</Size></Contents></ListBucketResult>'

    async def close(backend):
        closed.append(backend)
        await original_close(backend)

    monkeypatch.setattr(HTTPSBundleListingReader, "fetch", fetch)
    monkeypatch.setattr(MinioTaskImageBundleBackend, "aclose", close)
    app = create_app(settings, now_factory=lambda: context.now[0])
    for _ in range(2):
        with TestClient(app) as client:
            response = client.put(
                f"/v1/projections/{GRANT_ID}/materializations/{claim.materialization_id}/bundle",
                headers=_HEADERS, json=_operation_request(build_session, claim, operation_id=uuid4()).model_dump(mode="json"),
            )
            assert response.status_code == 200
            capability = TaskImageBundleCapabilityV1.model_validate_json(response.content)
            assert capability.objects[0].url.startswith(minio_tls[0] + "/loom-bundles/phase2c/session-bound/")
            assert "X-Amz-Signature=" in capability.objects[0].url
            assert capability.session_id == build_session.session_id
        assert app.state.ready is False
    assert len(reads) == len(closed) == 2
    assert reads[0] is not reads[1] and closed[0] is not closed[1]


@pytest.mark.parametrize("failure", ["credentials", "schema"])
async def test_configured_startup_fails_closed_and_releases_owned_backend(authority_api, tmp_path, minio_tls, monkeypatch, failure):
    import loom_task_image_authority.api as api

    settings = _configured(tmp_path, minio_tls, authority_api.settings.model_dump(mode="python"))
    closed = []
    original = MinioTaskImageBundleBackend.aclose

    async def close(backend):
        closed.append(backend)
        await original(backend)

    async def fail_schema(*args, **kwargs):
        raise RuntimeError("invalid schema fixture")

    monkeypatch.setattr(MinioTaskImageBundleBackend, "aclose", close)
    if failure == "credentials":
        settings.bundle_credentials_file.chmod(0o644)
    else:
        monkeypatch.setattr(api, "assert_schema_at_head", fail_schema)
    app = create_app(settings)
    with TestClient(app) as client:
        assert client.get("/healthz").status_code == 503
        assert app.state.ready is False
        assert len(closed) == (1 if failure == "schema" else 0)
        assert app.state.engine is None, "failed startup retained a database pool while serving unready"
    assert len(closed) == (1 if failure == "schema" else 0)


async def test_cancelled_startup_closes_native_backend_and_disposes_engine(authority_api, tmp_path, minio_tls, monkeypatch):
    import loom_task_image_authority.api as api

    settings = _configured(tmp_path, minio_tls, authority_api.settings.model_dump(mode="python"))
    closed, disposed = [], []
    entered = asyncio.Event()
    original_close, original_dispose = MinioTaskImageBundleBackend.aclose, api.AsyncEngine.dispose

    async def close(backend):
        closed.append(backend)
        await original_close(backend)

    async def dispose(engine, *args, **kwargs):
        disposed.append(engine)
        await original_dispose(engine, *args, **kwargs)

    async def schema_wait(*args, **kwargs):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(MinioTaskImageBundleBackend, "aclose", close)
    monkeypatch.setattr(api.AsyncEngine, "dispose", dispose)
    monkeypatch.setattr(api, "assert_schema_at_head", schema_wait)
    app = create_app(settings)

    async def startup():
        async with app.router.lifespan_context(app):
            pytest.fail("cancelled startup became ready")

    pending = asyncio.create_task(startup())
    try:
        await asyncio.wait_for(entered.wait(), 5)
    finally:
        pending.cancel()
        with pytest.raises(asyncio.CancelledError):
            await pending
    assert len(closed) == len(disposed) == 1
    assert app.state.ready is False
