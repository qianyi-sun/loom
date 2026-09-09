"""Elapsed listing/signing time never extends a frozen bundle deadline."""

from datetime import datetime, timedelta
from urllib.parse import parse_qs, urlsplit

import pytest

from loom_task_image_authority import bundle_capability as module
from tests.unit.test_task_image_bundle_capability import NOW, _objects, _plan


def _scenario(monkeypatch, *, listing_seconds=0, signing_seconds=0):
    current = [NOW]

    class ClockDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return current[0]

    monkeypatch.setattr(module, "datetime", ClockDateTime)

    class Backend:
        calls = 0

        def list_objects(self, **kwargs):
            current[0] += timedelta(seconds=listing_seconds)
            return _objects()

        def presign_get(self, *, bucket, key, **options):
            self.calls += 1
            current[0] += timedelta(seconds=signing_seconds)
            stamp = current[0].replace(microsecond=0)
            # Supports the current duration contract and proposed absolute
            # deadline so RED reaches real validation, not a missing parameter.
            duration = options.get("expires_in_seconds")
            if duration is None:
                duration = int((options["expires_at"] - stamp).total_seconds())
            return (
                f"https://objects.example/{key}?X-Amz-Date={stamp:%Y%m%dT%H%M%SZ}"
                f"&X-Amz-Expires={duration}&X-Amz-Signature=private-fixture"
            )

    backend = Backend()
    provider = module.TaskImageBundleCapabilityProvider(
        backend=backend, public_https_origin="https://objects.example",
        expected_bucket="loom-bundles", maximum_objects=2000,
        maximum_bytes=512 * 1024 * 1024, url_expiry_seconds=600,
    )
    return provider, backend, current


@pytest.mark.parametrize(("listing_seconds", "signing_seconds"), [(1, 0), (0, 1), (1, 1)])
def test_elapsed_io_uses_absolute_deadline_without_rejecting_valid_signing(
    monkeypatch, listing_seconds, signing_seconds,
):
    provider, backend, current = _scenario(
        monkeypatch, listing_seconds=listing_seconds, signing_seconds=signing_seconds,
    )
    capability = provider.issue(_plan(), now=NOW)
    assert backend.calls == 3
    assert capability.issued_at == NOW
    assert capability.expires_at == _plan().authorization_expires_at
    assert current[0] < capability.expires_at
    for item in capability.objects:
        query = parse_qs(urlsplit(item.url).query)
        signed_at = datetime.strptime(query["X-Amz-Date"][0], "%Y%m%dT%H%M%SZ").replace(tzinfo=NOW.tzinfo)
        assert signed_at <= current[0]
        assert signed_at + timedelta(seconds=int(query["X-Amz-Expires"][0])) == capability.expires_at


def test_listing_that_consumes_authorization_never_calls_signer(monkeypatch):
    provider, backend, _ = _scenario(monkeypatch, listing_seconds=40)
    with pytest.raises(module.TaskImageBundleCapabilityError, match="expired"):
        provider.issue(_plan(), now=NOW)
    assert backend.calls == 0


def test_expiration_during_signing_stops_before_next_object(monkeypatch):
    provider, backend, _ = _scenario(monkeypatch, signing_seconds=20)
    with pytest.raises(module.TaskImageBundleCapabilityError, match="expired"):
        provider.issue(_plan(), now=NOW)
    assert backend.calls == 2
