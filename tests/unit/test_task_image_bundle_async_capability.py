"""Async bundle issuance shares exact deadlines and explicit addressing rules."""

import asyncio
from datetime import timedelta
from urllib.parse import urlsplit

import pytest

from loom_task_image_authority import bundle_capability as module
from loom_task_image_authority.bundle_s3_signing import S3SigningCredentials, presign_bundle_get
from tests.unit.test_task_image_bundle_capability import NOW, _objects, _plan


class Backend:
    def __init__(self):
        self.now = NOW
        self.listing = []
        self.signing = []
        self.objects = _objects()
        self.listing_seconds = 0
        self.signing_seconds = 0
        self.wait = None

    async def list_objects(self, **options):
        self.listing.append(options)
        if self.wait is not None:
            await self.wait.wait()
        self.now += timedelta(seconds=self.listing_seconds)
        return self.objects

    def presign_get(self, *, bucket, key, expires_at):
        self.signing.append(expires_at)
        self.now += timedelta(seconds=self.signing_seconds)
        return presign_bundle_get(
            public_origin="https://objects.example:9443", bucket=bucket, key=key, region="us-east-1",
            credentials=S3SigningCredentials(access_key="fixture-access", secret_key="fixture-secret"),
            expires_at=expires_at, clock=lambda: self.now,
        )


def _provider(backend, **changes):
    options = dict(
        backend=backend, public_https_origin="https://objects.example:9443",
        expected_bucket="loom-bundles", maximum_objects=2000, maximum_bytes=536870912,
        url_expiry_seconds=600, clock=lambda: backend.now, addressing_style="path",
    )
    options.update(changes)
    return module.AsyncTaskImageBundleCapabilityProvider(**options)


async def test_async_provider_issues_exact_deadline_path_style_capabilities():
    backend = Backend()
    backend.listing_seconds, backend.signing_seconds = 1, 1
    capability = await _provider(backend).issue(_plan(), now=NOW)
    assert capability.file_count == 3
    assert capability.total_bytes == 62
    assert capability.expires_at == NOW + timedelta(seconds=40)
    assert backend.listing == [dict(bucket="loom-bundles", prefix=_plan().bundle_prefix, maximum_objects=2000, maximum_bytes=536870912, expires_at=capability.expires_at)]
    assert backend.signing == [capability.expires_at] * 3
    for item in capability.objects:
        assert urlsplit(item.url).path == "/loom-bundles/" + _plan().bundle_prefix + item.relative_path


@pytest.mark.parametrize("kind", ["listing_expired", "listing_regressed", "signing_expired", "duplicate", "foreign", "too_many", "too_large", "wrong_addressing"])
async def test_async_provider_enforces_existing_validation_contract(kind):
    backend = Backend()
    changes = {}
    if kind == "listing_expired":
        backend.listing_seconds = 40
    elif kind == "listing_regressed":
        backend.listing_seconds = -1
    elif kind == "signing_expired":
        backend.signing_seconds = 20
    elif kind == "duplicate":
        backend.objects = (_objects()[0],) * 2
    elif kind == "foreign":
        backend.objects = (module.TaskImageBundleObject(key="foreign/a", size_bytes=1),)
    elif kind == "too_many":
        changes["maximum_objects"] = 2
    elif kind == "too_large":
        changes["maximum_bytes"] = 61
    elif kind == "wrong_addressing":
        changes["addressing_style"] = "bucket-host"
    with pytest.raises(RuntimeError):
        await _provider(backend, **changes).issue(_plan(), now=NOW)


async def test_async_listing_yields_to_loop_and_cancellation_stops_signing():
    backend = Backend()
    backend.wait = asyncio.Event()
    task = asyncio.create_task(_provider(backend).issue(_plan(), now=NOW))
    await asyncio.sleep(0)
    assert backend.listing and not backend.signing
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not backend.signing


def test_addressing_style_must_be_explicit_supported_contract():
    with pytest.raises(ValueError):
        _provider(Backend(), addressing_style="guess")
