"""S3-artifacts state backend.

Uses the in-memory FakeObjectStore for fast unit coverage; a MinIO
testcontainer round-trip is exercised in the end-to-end integration
suite (Task 19).
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from loom.family_run.state_backends import S3ArtifactsStateBackend
from loom.trajectory.storage import FakeObjectStore


@pytest.mark.asyncio
async def test_initialize_provisions_empty_prefix() -> None:
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")
    batch_id = uuid4()
    uri = await backend.initialize(
        batch_id=batch_id,
        family_key="fam",
        params={},
    )
    assert uri.startswith(f"s3://artifacts/family-state/{batch_id}/fam/")


@pytest.mark.asyncio
async def test_upload_download_round_trip(tmp_path: Path) -> None:
    store = FakeObjectStore()
    await store.ensure_bucket("artifacts")
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")
    batch_id = uuid4()
    uri = await backend.initialize(
        batch_id=batch_id,
        family_key="fam",
        params={},
    )
    src = tmp_path / "src"
    src.mkdir()
    (src / "skill.txt").write_text("hello\n")

    new_uri = await backend.upload(uri, src, {})
    dst = tmp_path / "dst"
    await backend.download(new_uri, dst, {})
    assert (dst / "skill.txt").read_text() == "hello\n"


@pytest.mark.asyncio
async def test_download_upload_respect_uri_bucket_not_ctor_default(
    tmp_path: Path,
) -> None:
    """#727 regression: worker was hardcoded to the constructor bucket.

    The service seeds ``state_uri = s3://loom-staging-artifacts/...``
    even when the worker's constructor default is ``artifacts``. The
    download/upload calls MUST parse the bucket out of the URI, not
    substitute ``self.bucket`` — otherwise the worker hits
    ``InvalidObjectName`` against the wrong bucket.
    """
    store = FakeObjectStore()
    # The URI's bucket, not the constructor default.
    await store.ensure_bucket("loom-staging-artifacts")

    # Constructor default is a *different* bucket to make sure download/
    # upload consult the URI, not ``self.bucket``.
    backend = S3ArtifactsStateBackend(store=store, bucket="artifacts")

    batch_id = uuid4()
    key = f"family-state/{batch_id}/fam/state-init.tar.gz"
    # Seed a tarball at the staging bucket directly (as the service would).
    import io as _io
    import tarfile as _tarfile

    seed = _io.BytesIO()
    with _tarfile.open(fileobj=seed, mode="w:gz"):
        pass
    await store.put_object(
        bucket="loom-staging-artifacts", key=key, body=seed.getvalue()
    )
    seed_uri = f"s3://loom-staging-artifacts/{key}"

    dst = tmp_path / "dst"
    await backend.download(seed_uri, dst, {})
    # The bucket in the returned upload URI should stay ``loom-staging-artifacts``.
    src = tmp_path / "src"
    src.mkdir()
    (src / "s.txt").write_text("x\n")
    new_uri = await backend.upload(seed_uri, src, {})
    assert new_uri.startswith("s3://loom-staging-artifacts/family-state/")

    # And the constructor default bucket should have NO family-state objects.
    assert not any(
        b == "artifacts" and k.startswith("family-state/") for (b, k) in store.objects
    )
    # Uploaded state should live in the staging bucket, per the incoming URI.
    assert any(
        b == "loom-staging-artifacts" and k.startswith("family-state/")
        for (b, k) in store.objects
    )


def test_parse_uri_rejects_non_s3() -> None:
    with pytest.raises(ValueError):
        S3ArtifactsStateBackend._parse_uri("http://foo/bar")


def test_parse_uri_rejects_missing_key() -> None:
    with pytest.raises(ValueError):
        S3ArtifactsStateBackend._parse_uri("s3://only-bucket")
