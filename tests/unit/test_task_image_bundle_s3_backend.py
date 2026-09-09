"""Whole-inventory budgets and explicit native MinIO authority contracts."""

import asyncio
import hashlib
import importlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, quote_plus, urlsplit
from xml.sax.saxutils import escape

import pytest

from loom.task_image_bundle_manifest import capture_task_image_bundle_manifest
from loom_task_image_authority.bundle_s3_signing import S3SigningCredentials

NOW = datetime(2026, 9, 9, 12, tzinfo=UTC)


def _module():
    return importlib.import_module("loom_task_image_authority.bundle_s3_backend")


def _page(query, *, keys=("revision/a space+%2F",), size=7, next_token=None):
    prefix, maximum = query["prefix"][0], query["max-keys"][0]
    fields = ""
    if "continuation-token" in query:
        fields += "<ContinuationToken>" + escape(query["continuation-token"][0]) + "</ContinuationToken>"
    if next_token is not None:
        fields += "<NextContinuationToken>" + escape(next_token) + "</NextContinuationToken>"
    entries = "".join(f"<Contents><Key>{quote_plus(key)}</Key><Size>{size}</Size></Contents>" for key in keys)
    return (f'<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>loom-bundles</Name><Prefix>{quote_plus(prefix)}</Prefix><MaxKeys>{maximum}</MaxKeys><KeyCount>{len(keys)}</KeyCount><EncodingType>url</EncodingType><IsTruncated>{str(next_token is not None).lower()}</IsTruncated>{entries}{fields}</ListBucketResult>').encode()


class _Reader:
    def __init__(self):
        self.requests = []
        self.pages = [lambda q: _page(q)]
        self.on_fetch = lambda: None
        self.closed = False

    async def fetch(self, url, *, deadline):
        self.requests.append((url, deadline))
        self.on_fetch()
        return self.pages[len(self.requests) - 1](parse_qs(urlsplit(url).query))

    async def aclose(self):
        self.closed = True


def _backend(monkeypatch, *, reader=None, clock=lambda: NOW, **changes):
    module = _module()
    reader = reader or _Reader()
    monkeypatch.setattr(module, "HTTPSBundleListingReader", lambda **kwargs: reader)
    options = dict(
        origin="https://objects.example:9443", bucket="loom-bundles", region="us-east-1",
        credentials=S3SigningCredentials(access_key="fixture-access", secret_key="private-secret"),
        ca_file=Path("/fixture/ca.pem"), clock=clock,
    )
    options.update(changes)
    return module.MinioTaskImageBundleBackend(**options), reader


async def _list(backend, **changes):
    options = dict(bucket="loom-bundles", prefix="revision/", maximum_objects=2000, maximum_bytes=1000, expires_at=NOW + timedelta(seconds=60))
    options.update(changes)
    return await backend.list_objects(**options)


async def test_lists_exact_inventory_with_fixed_deadline_and_owned_close(monkeypatch):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="page-2"), lambda q: _page(q, keys=("revision/z",))]
    backend, _ = _backend(monkeypatch, reader=reader)
    async with backend:
        objects = await _list(backend)
        assert [(obj.key, obj.size_bytes) for obj in objects] == [("revision/a space+%2F", 7), ("revision/z", 7)]
        get = backend.presign_get(bucket="loom-bundles", key=objects[0].key, expires_at=NOW + timedelta(seconds=60))
        assert urlsplit(get).path == "/loom-bundles/revision/a%20space%2B%252F"
    assert reader.closed
    assert reader.requests[0][1] == reader.requests[1][1]
    assert parse_qs(urlsplit(reader.requests[1][0]).query)["continuation-token"] == ["page-2"]
    for url, _ in reader.requests:
        query = parse_qs(urlsplit(url).query)
        assert query["X-Amz-Date"] == ["20260909T120000Z"]
        assert query["X-Amz-Expires"] == ["60"]


@pytest.mark.parametrize("second_keys", [("revision/a space+%2F",), ("revision/0",)])
async def test_rejects_duplicate_or_backward_keys_across_pages(monkeypatch, second_keys):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=second_keys)]
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _list(backend)


async def test_rejects_continuation_cycle_across_multiple_pages(monkeypatch):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=("revision/b",), next_token="p3"), lambda q: _page(q, keys=("revision/c",), next_token="p2")]
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _list(backend)
    assert len(reader.requests) == 3


@pytest.mark.parametrize("limits", [{"maximum_pages": 1}, {"maximum_listing_bytes": 500}])
async def test_bounds_whole_listing_pages_and_xml_bytes(monkeypatch, limits):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=("revision/b",))]
    backend, _ = _backend(monkeypatch, reader=reader, limits=_module().S3InventoryLimits(**limits))
    with pytest.raises(RuntimeError):
        await _list(backend)
    assert len(reader.requests) <= 2


@pytest.mark.parametrize("changes", [{"maximum_objects": 1}, {"maximum_bytes": 10}])
async def test_bounds_accumulated_objects_and_object_bytes(monkeypatch, changes):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, next_token="p2"), lambda q: _page(q, keys=("revision/b",))]
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _list(backend, **changes)


@pytest.mark.parametrize("advanced", [NOW - timedelta(seconds=1), NOW + timedelta(seconds=60), NOW.replace(tzinfo=None)])
async def test_rejects_clock_regression_or_expiry_after_network(monkeypatch, advanced):
    observed = [NOW]
    reader = _Reader()
    reader.on_fetch = lambda: observed.__setitem__(0, advanced)
    backend, _ = _backend(monkeypatch, reader=reader, clock=lambda: observed[0])
    with pytest.raises(RuntimeError):
        await _list(backend)


async def test_expired_authority_never_starts_network(monkeypatch):
    backend, reader = _backend(monkeypatch)
    with pytest.raises(RuntimeError):
        await _list(backend, expires_at=NOW)
    assert not reader.requests


@pytest.mark.parametrize("changes", [{"bucket": "foreign"}, {"maximum_objects": True}, {"maximum_objects": 2001}, {"maximum_bytes": -1}, {"maximum_bytes": 536870913}, {"prefix": "../"}])
async def test_invalid_scope_and_limits_fail_before_network(monkeypatch, changes):
    backend, reader = _backend(monkeypatch)
    with pytest.raises(RuntimeError):
        await _list(backend, **changes)
    assert not reader.requests


async def test_total_timeout_and_cancellation_remain_cancellable(monkeypatch):
    entered = asyncio.Event()

    class WaitingReader(_Reader):
        async def fetch(self, url, *, deadline):
            entered.set()
            await asyncio.Future()

    backend, _ = _backend(monkeypatch, reader=WaitingReader(), limits=_module().S3InventoryLimits(total_timeout_seconds=0.05))
    with pytest.raises(RuntimeError):
        await _list(backend)
    assert entered.is_set()
    task = asyncio.create_task(_list(backend))
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_production_backend_refuses_temporary_or_ambient_identity(monkeypatch):
    with pytest.raises(RuntimeError):
        _backend(monkeypatch, credentials=None)
    with pytest.raises(RuntimeError):
        _backend(monkeypatch, credentials=S3SigningCredentials(access_key="a", secret_key="b", session_token="token", expires_at=NOW + timedelta(seconds=60)))


def test_get_rejects_clock_regression_after_actual_signing_stamp(monkeypatch):
    observations = iter([NOW, NOW + timedelta(seconds=10), NOW + timedelta(seconds=5)])
    backend, _ = _backend(monkeypatch, clock=lambda: next(observations))
    with pytest.raises(RuntimeError):
        backend.presign_get(bucket="loom-bundles", key="revision/a", expires_at=NOW + timedelta(seconds=60))


async def test_forward_clock_during_signing_shortens_network_deadline(monkeypatch):
    observations = [NOW, NOW, NOW + timedelta(seconds=59)]

    def clock():
        return observations.pop(0) if len(observations) > 1 else observations[0]

    reader = _Reader()
    remaining = []
    reader.on_fetch = lambda: remaining.append(reader.requests[-1][1] - asyncio.get_running_loop().time())
    backend, _ = _backend(monkeypatch, reader=reader, clock=clock)
    await _list(backend)
    assert 0 < remaining[0] <= 1.0


async def test_zero_byte_objects_are_valid_but_empty_inventory_is_not(monkeypatch):
    reader = _Reader()
    reader.pages = [lambda q: _page(q, size=0), lambda q: _page(q, keys=())]
    backend, _ = _backend(monkeypatch, reader=reader)
    assert len(await _list(backend, maximum_bytes=0)) == 1
    with pytest.raises(RuntimeError):
        await _list(backend)


async def test_backend_close_disables_listing_and_signing(monkeypatch):
    backend, reader = _backend(monkeypatch)
    await backend.aclose()
    with pytest.raises(RuntimeError):
        await _list(backend)
    with pytest.raises(RuntimeError):
        backend.presign_get(bucket="loom-bundles", key="revision/a", expires_at=NOW + timedelta(seconds=60))
    assert reader.closed and not reader.requests


async def test_backend_redacts_unexpected_storage_failure(monkeypatch):
    reader = _Reader()

    def fail():
        raise RuntimeError("private-storage-response")

    reader.on_fetch = fail
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError) as error:
        await _list(backend)
    assert "private" not in str(error.value)


@pytest.mark.parametrize("limits", [
    {"maximum_pages": True}, {"maximum_pages": 0}, {"maximum_pages": 65},
    {"page_size": 0}, {"page_size": 1001}, {"maximum_listing_bytes": 33554433},
    {"total_timeout_seconds": float("nan")}, {"total_timeout_seconds": float("inf")},
    {"total_timeout_seconds": 121.0},
])
def test_inventory_configuration_is_finite_typed_and_bounded(limits):
    with pytest.raises(RuntimeError):
        _module().S3InventoryLimits(**limits)


@pytest.fixture
def manifest(tmp_path):
    (tmp_path / "Dockerfile").write_bytes(b"FROM scratch\n")
    return capture_task_image_bundle_manifest(tmp_path)


class _ManifestReader(_Reader):
    def __init__(self, payload):
        super().__init__()
        self.payload = payload

    async def fetch_manifest(self, url, *, expected_sha256, deadline):
        self.requests.append((url, deadline, expected_sha256))
        self.on_fetch()
        return self.payload


async def _get_manifest(backend, manifest, **changes):
    options = dict(
        bucket="loom-bundles", expected_sha256=manifest.digest,
        task_checksum=manifest.task_checksum,
        bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256,
        expires_at=NOW + timedelta(seconds=60),
    )
    options.update(changes)
    return await backend.get_manifest(**options)


async def test_get_manifest_verifies_registered_bytes_and_exact_signed_key(monkeypatch, manifest):
    reader = _ManifestReader(manifest.canonical_bytes)
    backend, _ = _backend(monkeypatch, reader=reader)
    async with backend:
        assert await _get_manifest(backend, manifest) == manifest
    url, deadline, expected = reader.requests[0]
    assert urlsplit(url).path == f"/loom-bundles/loom-bundle-manifests/v1/sha256/{manifest.digest}.json"
    assert parse_qs(urlsplit(url).query)["X-Amz-Expires"] == ["60"]
    assert expected == manifest.digest
    assert 0 < deadline - asyncio.get_running_loop().time() <= 30
    assert reader.closed


@pytest.mark.parametrize("changes", [
    {"bucket": "foreign"}, {"expected_sha256": "A" * 64}, {"expected_sha256": "0" * 64},
    {"expected_sha256": None}, {"task_checksum": "sha256:" + "a" * 64},
    {"task_checksum": True}, {"bundle_file_metadata_sha256": ""},
    {"expires_at": NOW},
])
async def test_manifest_rejects_invalid_frozen_scope_before_network(monkeypatch, manifest, changes):
    reader = _ManifestReader(manifest.canonical_bytes)
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _get_manifest(backend, manifest, **changes)
    assert not reader.requests


@pytest.mark.parametrize("change", ["content", "noncanonical", "checksum", "modes", "oversized"])
async def test_manifest_rejects_corruption_encoding_and_frozen_binding_drift(monkeypatch, manifest, change):
    payload = manifest.canonical_bytes
    options = {}
    if change == "content":
        payload = payload.replace(b"Dockerfile", b"Dockerfild")
    elif change == "noncanonical":
        payload += b"\n"
        options["expected_sha256"] = hashlib.sha256(payload).hexdigest()
    elif change == "checksum":
        options["task_checksum"] = "b" * 64
    elif change == "modes":
        options["bundle_file_metadata_sha256"] = "b" * 64
    else:
        payload = b" " * (4 * 1024 * 1024 + 1)
        options["expected_sha256"] = hashlib.sha256(payload).hexdigest()
    backend, _ = _backend(monkeypatch, reader=_ManifestReader(payload))
    with pytest.raises(RuntimeError):
        await _get_manifest(backend, manifest, **options)


@pytest.mark.parametrize("advanced", [NOW - timedelta(seconds=1), NOW + timedelta(seconds=60), NOW.replace(tzinfo=None)])
async def test_manifest_rejects_post_fetch_clock_regression_or_expiry(monkeypatch, manifest, advanced):
    observed = [NOW]
    reader = _ManifestReader(manifest.canonical_bytes)
    reader.on_fetch = lambda: observed.__setitem__(0, advanced)
    backend, _ = _backend(monkeypatch, reader=reader, clock=lambda: observed[0])
    with pytest.raises(RuntimeError):
        await _get_manifest(backend, manifest)


async def test_manifest_signing_shortens_deadline_on_forward_clock_movement(monkeypatch, manifest):
    observations = [NOW, NOW, NOW + timedelta(seconds=59)]

    def clock():
        return observations.pop(0) if len(observations) > 1 else observations[0]

    reader = _ManifestReader(manifest.canonical_bytes)
    remaining = []
    reader.on_fetch = lambda: remaining.append(reader.requests[-1][1] - asyncio.get_running_loop().time())
    backend, _ = _backend(monkeypatch, reader=reader, clock=clock)
    await _get_manifest(backend, manifest)
    assert 0 < remaining[0] <= 1


async def test_manifest_rechecks_authorization_after_canonical_parsing(monkeypatch, manifest):
    observed = [NOW]
    module = _module()
    original = getattr(module, "parse_task_image_bundle_manifest", None)
    assert original is not None

    def parse(*args, **kwargs):
        result = original(*args, **kwargs)
        observed[0] = NOW + timedelta(seconds=60)
        return result

    monkeypatch.setattr(module, "parse_task_image_bundle_manifest", parse)
    backend, _ = _backend(monkeypatch, reader=_ManifestReader(manifest.canonical_bytes), clock=lambda: observed[0])
    with pytest.raises(RuntimeError):
        await _get_manifest(backend, manifest)


async def test_manifest_timeout_cancellation_and_closed_backend(monkeypatch, manifest):
    entered = asyncio.Event()

    class WaitingReader(_ManifestReader):
        async def fetch_manifest(self, url, *, expected_sha256, deadline):
            entered.set()
            await asyncio.Future()

    backend, reader = _backend(monkeypatch, reader=WaitingReader(b""), limits=_module().S3InventoryLimits(total_timeout_seconds=0.02))
    with pytest.raises(RuntimeError):
        await _get_manifest(backend, manifest)
    assert entered.is_set()
    entered.clear()
    task = asyncio.create_task(_get_manifest(backend, manifest))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await backend.aclose()
    with pytest.raises(RuntimeError):
        await _get_manifest(backend, manifest)
    assert reader.closed


async def test_manifest_redacts_storage_errors(monkeypatch, manifest):
    reader = _ManifestReader(manifest.canonical_bytes)

    def fail():
        raise RuntimeError("private-storage-response")

    reader.on_fetch = fail
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError) as error:
        await _get_manifest(backend, manifest)
    assert "private" not in str(error.value)


class _VerifiedReader(_ManifestReader):
    def __init__(self, manifest):
        super().__init__(manifest.canonical_bytes)
        self.manifest = manifest
        self.entries = None
        self.list_requests = []
        self.on_list = lambda: None

    async def fetch(self, url, *, deadline):
        from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME

        self.list_requests.append((url, deadline))
        self.on_list()
        query = parse_qs(urlsplit(url).query)
        prefix = query["prefix"][0]
        entries = self.entries
        if entries is None:
            entries = [(item.path, item.size_bytes) for item in self.manifest.files]
            entries.append((BUNDLE_FILE_METADATA_NAME, len(self.manifest.mode_metadata_bytes)))
        entries = sorted(entries)
        index = int(query.get("continuation-token", ["0"])[0])
        maximum = int(query["max-keys"][0])
        selected = entries[index:index + maximum]
        next_index = index + len(selected)
        token = str(next_index) if next_index < len(entries) else None
        # Reuse the production-facing XML fixture with heterogeneous file sizes.
        payload = _page(query, keys=(), next_token=token).decode()
        contents = "".join(f"<Contents><Key>{quote_plus(prefix + path)}</Key><Size>{size}</Size></Contents>" for path, size in selected)
        return payload.replace("<KeyCount>0</KeyCount>", f"<KeyCount>{len(selected)}</KeyCount>").replace("</ListBucketResult>", contents + "</ListBucketResult>").encode()


async def _verified(backend, manifest, **changes):
    options = dict(
        bucket="loom-bundles", prefix=f"revision/{manifest.digest}/",
        expected_sha256=manifest.digest, task_checksum=manifest.task_checksum,
        bundle_file_metadata_sha256=manifest.bundle_file_metadata_sha256,
        maximum_objects=2000, maximum_bytes=512 * 1024 * 1024,
        expires_at=NOW + timedelta(seconds=60),
    )
    options.update(changes)
    return await backend.get_verified_bundle_manifest(**options)


async def test_verified_manifest_requires_complete_inventory_under_one_deadline(monkeypatch, manifest):
    reader = _VerifiedReader(manifest)
    backend, _ = _backend(monkeypatch, reader=reader, limits=_module().S3InventoryLimits(page_size=1))
    assert await _verified(backend, manifest) == manifest
    assert len(reader.requests) == 1 and len(reader.list_requests) == 2
    assert {deadline for _, deadline in reader.list_requests} == {reader.requests[0][1]}


@pytest.mark.parametrize("change", ["omit_data", "omit_sidecar", "extra", "data_size", "sidecar_size"])
async def test_verified_manifest_rejects_inventory_mismatch(monkeypatch, manifest, change):
    from loom.trajectory.storage import BUNDLE_FILE_METADATA_NAME

    reader = _VerifiedReader(manifest)
    entries = {item.path: item.size_bytes for item in manifest.files}
    entries[BUNDLE_FILE_METADATA_NAME] = len(manifest.mode_metadata_bytes)
    if change == "omit_data":
        del entries["Dockerfile"]
    elif change == "omit_sidecar":
        del entries[BUNDLE_FILE_METADATA_NAME]
    elif change == "extra":
        entries["extra"] = 0
    elif change == "data_size":
        entries["Dockerfile"] -= 1
    else:
        entries[BUNDLE_FILE_METADATA_NAME] -= 1
    reader.entries = list(entries.items())
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _verified(backend, manifest)


@pytest.mark.parametrize("changes", [
    {"prefix": "revision/legacy/"}, {"prefix": "../invalid/"}, {"bucket": "foreign"},
    {"maximum_objects": True}, {"maximum_objects": 2001}, {"maximum_bytes": -1},
    {"maximum_bytes": 512 * 1024 * 1024 + 1}, {"expected_sha256": "A" * 64},
])
async def test_verified_scope_rejects_before_any_network(monkeypatch, manifest, changes):
    reader = _VerifiedReader(manifest)
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _verified(backend, manifest, **changes)
    assert not reader.requests and not reader.list_requests


async def test_verified_manifest_data_limit_is_not_spent_on_transport_sidecar(monkeypatch, tmp_path):
    for index in range(2000):
        (tmp_path / f"f{index:04d}").touch()
    manifest = capture_task_image_bundle_manifest(tmp_path)
    reader = _VerifiedReader(manifest)
    backend, _ = _backend(monkeypatch, reader=reader)
    assert await _verified(backend, manifest, maximum_bytes=0) == manifest
    assert len(reader.list_requests) == 8
    # Public legacy entrypoint keeps its old ceiling; only authenticated native
    # composition can add the manifest-derived exact transport overhead.
    with pytest.raises(RuntimeError):
        await _list(backend, maximum_objects=2001)


async def test_verified_limits_reject_registered_data_before_listing(monkeypatch, manifest):
    reader = _VerifiedReader(manifest)
    backend, _ = _backend(monkeypatch, reader=reader)
    with pytest.raises(RuntimeError):
        await _verified(backend, manifest, maximum_bytes=1)
    assert len(reader.requests) == 1 and not reader.list_requests


async def test_verified_forward_wall_clock_shrinks_next_phase_deadline(monkeypatch, manifest):
    observed = [NOW]
    reader = _VerifiedReader(manifest)
    reader.on_fetch = lambda: observed.__setitem__(0, NOW + timedelta(seconds=59))
    backend, _ = _backend(monkeypatch, reader=reader, clock=lambda: observed[0])
    assert await _verified(backend, manifest) == manifest
    assert reader.list_requests[0][1] < reader.requests[0][1]
    assert reader.list_requests[0][1] - asyncio.get_running_loop().time() <= 1


async def test_verified_clock_regression_between_phases_is_not_reset(monkeypatch, manifest):
    observed = [NOW]
    reader = _VerifiedReader(manifest)
    reader.on_fetch = lambda: observed.__setitem__(0, NOW + timedelta(seconds=10))
    reader.on_list = lambda: observed.__setitem__(0, NOW + timedelta(seconds=5))
    backend, _ = _backend(monkeypatch, reader=reader, clock=lambda: observed[0])
    with pytest.raises(RuntimeError):
        await _verified(backend, manifest)


async def test_verified_manifest_and_inventory_share_total_timeout_and_cancellation(monkeypatch, manifest):
    entered = asyncio.Event()

    class WaitingReader(_VerifiedReader):
        async def fetch(self, url, *, deadline):
            entered.set()
            await asyncio.Future()

    reader = WaitingReader(manifest)
    backend, _ = _backend(monkeypatch, reader=reader, limits=_module().S3InventoryLimits(total_timeout_seconds=0.03))
    with pytest.raises(RuntimeError):
        await _verified(backend, manifest)
    assert entered.is_set()
    entered.clear()
    task = asyncio.create_task(_verified(backend, manifest))
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
