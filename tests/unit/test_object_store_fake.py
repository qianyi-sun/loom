import pytest

from loom.trajectory.storage import FakeObjectStore, MultipartUpload


@pytest.fixture
def store() -> FakeObjectStore:
    return FakeObjectStore()


async def test_create_multipart_and_complete(store: FakeObjectStore):
    upload: MultipartUpload = await store.create_multipart_upload(
        bucket="trajectories", key="t/123/events.jsonl",
    )
    await store.upload_part(upload, part_number=1, body=b"part-1-data")
    await store.upload_part(upload, part_number=2, body=b"part-2-data")
    uri = await store.complete_multipart_upload(upload)
    assert uri == "s3://trajectories/t/123/events.jsonl"
    assert store.objects[("trajectories", "t/123/events.jsonl")] == b"part-1-datapart-2-data"


async def test_multipart_reassembles_in_part_number_order(store: FakeObjectStore):
    """Parts uploaded out of order must still concatenate in part_number order."""
    upload = await store.create_multipart_upload(bucket="t", key="k")
    await store.upload_part(upload, part_number=2, body=b"B")
    await store.upload_part(upload, part_number=1, body=b"A")
    await store.complete_multipart_upload(upload)
    assert store.objects[("t", "k")] == b"AB"


async def test_abort_drops_multipart(store: FakeObjectStore):
    upload = await store.create_multipart_upload(bucket="t", key="k")
    await store.upload_part(upload, part_number=1, body=b"x")
    await store.abort_multipart_upload(upload)
    assert ("t", "k") not in store.objects


async def test_put_object_single_shot(store: FakeObjectStore):
    await store.put_object(
        bucket="trajectories", key="t/123/atif.json", body=b'{"x":1}',
    )
    assert store.objects[("trajectories", "t/123/atif.json")] == b'{"x":1}'


async def test_get_object_roundtrip(store: FakeObjectStore):
    await store.put_object(bucket="t", key="k", body=b"hi")
    data = await store.get_object(bucket="t", key="k")
    assert data == b"hi"


async def test_get_missing_raises(store: FakeObjectStore):
    with pytest.raises(KeyError):
        await store.get_object(bucket="t", key="nope")


async def test_signed_url_returns_string(store: FakeObjectStore):
    url = await store.presign_put(bucket="t", key="k", expires_sec=300)
    assert url.startswith("https://fake/")
