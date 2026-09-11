"""Management-mediated artifact return is bounded, fenced and non-overwriting."""

import asyncio
import hashlib
from importlib import import_module
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from botocore.exceptions import ClientError

from loom_capacity_agent.build_admission import BuildArtifactV1, native_build_artifact_key
from tests.unit.test_personal_dev_build_source_reader import reader_input


class Objects:
    def __init__(self):
        self.calls = []
        self.parts = []
        self.object = None
        self.metadata = None
        self.lose_completion = False

    def create_multipart_upload(self, **kwargs):
        self.calls.append(("create", kwargs))
        self.metadata = kwargs["Metadata"]
        return {"UploadId": "private-upload-id"}

    def upload_part(self, **kwargs):
        self.calls.append(("part", {key: value for key, value in kwargs.items() if key != "Body"}))
        self.parts.append(kwargs["Body"])
        return {"ETag": f'"part-{kwargs["PartNumber"]}"'}

    def complete_multipart_upload(self, **kwargs):
        self.calls.append(("complete", kwargs))
        assert kwargs["IfNoneMatch"] == "*", "artifact output must never overwrite"
        if self.object is not None:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "CompleteMultipartUpload")
        self.object = {"Body": b"".join(self.parts), "Metadata": self.metadata}
        if self.lose_completion:
            raise TimeoutError("lost completed upload reply")
        return {"ETag": '"completed"'}

    def head_object(self, **kwargs):
        self.calls.append(("head", kwargs))
        if self.object is None:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "HeadObject")
        return {"ContentLength": len(self.object["Body"]), "Metadata": self.object["Metadata"]}

    def abort_multipart_upload(self, **kwargs):
        self.calls.append(("abort", kwargs))


def inputs(monkeypatch, data=b"artifact"):
    module = import_module("loom_capacity_build_guard.artifact_writer")
    _reader, claim, source, _authorize, _response, _calls = reader_input(monkeypatch)
    objects = Objects()
    writer = module.BuildArtifactWriter(session_factory=object(), object_store=objects,
        max_artifact_bytes=16 * 1024 * 1024)
    authorize = AsyncMock(return_value=source)
    monkeypatch.setattr(writer, "_authorize", authorize)
    artifact = BuildArtifactV1(archive_size_bytes=len(data), archive_sha256=hashlib.sha256(data).hexdigest())
    return module, writer, claim, source, authorize, objects, artifact


@pytest.mark.parametrize("boundary", ["exact", "multipart", "before", "part-fence", "after", "changed",
    "hash", "short", "long", "oversized-chunk", "lost-completion", "existing-exact", "existing-other"])
async def test_artifact_stream_checks_full_content_fences_and_conditional_identity(monkeypatch, boundary):
    data = b"artifact" if boundary != "multipart" else b"a" * (9 * 1024 * 1024)
    module, writer, claim, source, authorize, objects, artifact = inputs(monkeypatch, data)
    if boundary == "before":
        authorize.side_effect = ValueError("revoked")
    elif boundary == "part-fence":
        authorize.side_effect = [source, ValueError("revoked")]
    elif boundary == "after":
        authorize.side_effect = [source, source, source, ValueError("revoked")]
    elif boundary == "changed":
        authorize.side_effect = [source, source.model_copy(update={"object_bucket": "foreign"})]
    elif boundary == "hash":
        artifact = artifact.model_copy(update={"archive_sha256": "f" * 64})
    elif boundary == "short":
        data = data[:-1]
    elif boundary == "long":
        data += b"!"
    elif boundary == "lost-completion":
        objects.lose_completion = True
    elif boundary.startswith("existing-"):
        objects.object = {"Body": b"artifact", "Metadata": {"claim-sha256": source.claim_digest,
            "artifact-sha256": artifact.archive_sha256 if boundary == "existing-exact" else "f" * 64}}
    async def chunks():
        if boundary == "oversized-chunk":
            yield b"x" * (module.MAX_ARTIFACT_CHUNK_BYTES + 1)
            return
        for offset in range(0, len(data), module.MAX_ARTIFACT_CHUNK_BYTES):
            yield data[offset:offset + module.MAX_ARTIFACT_CHUNK_BYTES]
    if boundary in {"exact", "multipart", "lost-completion", "existing-exact"}:
        result = await writer.write(claim, worker_credential="x" * 43, artifact=artifact, chunks=chunks())
        assert result == artifact
        assert objects.object["Body"] == data
    else:
        with pytest.raises((RuntimeError, ValueError)):
            await writer.write(claim, worker_credential="x" * 43, artifact=artifact, chunks=chunks())
    for _operation, kwargs in objects.calls:
        assert kwargs["Bucket"] == source.object_bucket
        assert kwargs["Key"] == native_build_artifact_key(claim)
        assert "x" * 43 not in repr(kwargs)
    operations = [operation for operation, _ in objects.calls]
    if boundary == "before":
        assert operations == []
    elif boundary in {"hash", "short", "long", "oversized-chunk", "changed", "part-fence"}:
        assert "complete" not in operations
        assert operations[-1] == "abort"
    if boundary in {"after", "existing-other"}:
        assert objects.object is not None
        assert operations.count("complete") == 1
    if boundary == "multipart":
        assert len(objects.parts) == 2
        assert all(len(part) >= 5 * 1024 * 1024 for part in objects.parts[:-1])


@pytest.mark.parametrize("phase", ["create", "part", "complete"])
async def test_cancelled_artifact_io_drains_and_aborts_late_upload(monkeypatch, phase):
    from threading import Event

    _module, writer, claim, _source, _authorize, objects, artifact = inputs(monkeypatch)
    started, release = Event(), Event()
    method = {"create": "create_multipart_upload", "part": "upload_part", "complete": "complete_multipart_upload"}[phase]
    original = getattr(objects, method)
    def slow(**kwargs):
        started.set()
        assert release.wait(5)
        return original(**kwargs)
    monkeypatch.setattr(objects, method, slow)
    async def chunks():
        yield b"artifact"
    task = asyncio.create_task(writer.write(claim, worker_credential="x" * 43, artifact=artifact, chunks=chunks()))
    closing = None
    try:
        assert await asyncio.to_thread(started.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        closing = asyncio.create_task(writer.aclose())
        await asyncio.sleep(0)
        assert not closing.done() and not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await closing
    operations = [operation for operation, _ in objects.calls]
    assert operations[-1] == "abort"
    assert operations.count("complete") == (1 if phase == "complete" else 0)
    with pytest.raises(ValueError, match="closed"):
        await writer.write(claim, worker_credential="x" * 43, artifact=artifact, chunks=chunks())
