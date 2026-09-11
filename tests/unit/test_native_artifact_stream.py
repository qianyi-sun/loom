"""Artifact framing keeps scoped credentials out of URLs and bounds buffering."""

from importlib import import_module

import pytest

from tests.unit.test_capacity_build_outcomes import claim_request


def envelope(module):
    return module.BuildArtifactUploadV1(claim=claim_request(), worker_credential="w" * 43,
        artifact={"archive_size_bytes": 8, "archive_sha256": "a" * 64})


@pytest.mark.parametrize("fragment", [1, 7, 1048576])
async def test_artifact_frame_round_trip_across_arbitrary_boundaries(fragment):
    module = import_module("loom_capacity_agent.build_artifact_stream")
    packet = envelope(module)
    async def payload():
        yield b"artifact"
    wire = b"".join([chunk async for chunk in module.encode_artifact_stream(packet, payload())])
    assert wire.startswith(b"LOOMART1")
    async def fragments():
        for offset in range(0, len(wire), fragment):
            yield wire[offset:offset + fragment]
        yield b""  # ASGI EOF marker
    decoded, chunks = await module.decode_artifact_stream(fragments())
    assert decoded == packet
    assert b"".join([chunk async for chunk in chunks]) == b"artifact"


@pytest.mark.parametrize("boundary", ["magic", "short", "length", "noncanonical", "secret"])
async def test_invalid_artifact_header_fails_bounded_without_echoing_credentials(boundary):
    module = import_module("loom_capacity_agent.build_artifact_stream")
    from loom_capacity_manager.contracts import canonical_bytes

    packet = envelope(module)
    header = canonical_bytes(packet)
    if boundary == "noncanonical":
        header += b" "
    elif boundary == "secret":
        header = header.replace(b"w" * 43, b"private-invalid-secret!")
    wire = b"LOOMART1" + len(header).to_bytes(4, "big") + header
    if boundary == "magic":
        wire = b"!" + wire[1:]
    elif boundary == "short":
        wire = wire[:-1]
    elif boundary == "length":
        wire = wire[:8] + (65537).to_bytes(4, "big")
    async def chunks():
        yield wire
    with pytest.raises(ValueError) as error:
        await module.decode_artifact_stream(chunks())
    assert packet.worker_credential not in str(error.value)
    assert "private-invalid-secret" not in str(error.value)


async def test_artifact_frame_accepts_coalesced_transport_without_unbounded_logical_chunks():
    module = import_module("loom_capacity_agent.build_artifact_stream")
    packet = envelope(module)
    packet = packet.model_copy(update={"artifact": packet.artifact.model_copy(update={"archive_size_bytes": 2 * 1048576})})
    async def payload():
        yield b"a" * 1048576
        yield b"b" * 1048576
    wire = b"".join([chunk async for chunk in module.encode_artifact_stream(packet, payload())])
    async def coalesced():
        yield wire
    decoded, chunks = await module.decode_artifact_stream(coalesced())
    observed = [chunk async for chunk in chunks]
    assert decoded == packet
    assert all(0 < len(chunk) <= 1048576 for chunk in observed)
    assert b"".join(observed) == b"a" * 1048576 + b"b" * 1048576


@pytest.mark.parametrize("boundary", ["exact", "claim", "artifact"])
async def test_native_client_streams_artifact_and_checks_exact_receipt(boundary):
    import httpx

    from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
    from tests.unit.test_capacity_build_admission_client import client_for

    module = import_module("loom_capacity_agent.build_artifact_stream")
    packet = envelope(module)
    async def handle(outgoing):
        assert outgoing.url.path.endswith("/artifact")
        assert outgoing.headers["content-type"] == module.ARTIFACT_STREAM_CONTENT_TYPE
        assert "w" * 43 not in str(outgoing.url) and "w" * 43 not in repr(outgoing.headers)
        decoded, chunks = await module.decode_artifact_stream(outgoing.stream.__aiter__())
        assert decoded == packet
        assert b"".join([chunk async for chunk in chunks]) == b"artifact"
        artifact = packet.artifact.model_copy(update={"archive_sha256": "f" * 64}) if boundary == "artifact" else packet.artifact
        receipt = module.BuildArtifactUploadReceiptV1(claim_digest="f" * 64 if boundary == "claim" else canonical_digest(packet.claim), artifact=artifact)
        return httpx.Response(200, content=canonical_bytes(receipt))
    async def payload():
        yield b"artifact"
    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = client_for(http, packet.claim)
        if boundary == "exact":
            receipt = await client.upload_artifact(packet.claim, worker_credential=packet.worker_credential,
                artifact=packet.artifact, chunks=payload())
            assert receipt.artifact == packet.artifact
        else:
            with pytest.raises(RuntimeError):
                await client.upload_artifact(packet.claim, worker_credential=packet.worker_credential,
                    artifact=packet.artifact, chunks=payload())


@pytest.mark.parametrize("purpose", ["application-worker", "personal-build-worker"])
@pytest.mark.parametrize("boundary", ["exact", "error", "invalid"])
async def test_artifact_router_selects_only_pinned_build_authority(tmp_path, purpose, boundary):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from loom_capacity_manager.contracts import canonical_digest
    from tests.unit.test_capacity_typed_admission_routing import configured

    module = import_module("loom_capacity_agent.build_artifact_stream")
    router_module, registration, document, path, digest = configured(tmp_path, "gb10", purpose)
    packet = envelope(module)
    packet = packet.model_copy(update={"claim": packet.claim.model_copy(update={"binding": registration.binding})})
    response = module.BuildArtifactUploadReceiptV1(claim_digest=canonical_digest(packet.claim), artifact=packet.artifact)
    upload = AsyncMock(return_value=object() if boundary == "invalid" else response,
        side_effect=RuntimeError("transport failed") if boundary == "error" else None)
    close = AsyncMock()
    def application(*args, **kwargs):
        pytest.fail("artifact upload reached application database")
    def build(identity, connection):
        assert identity == document.executor and connection == document.entries[0].build
        return SimpleNamespace(upload_artifact=upload, aclose=close)
    router = router_module.TypedAdmissionRouter(path, expected_sha256=digest, executor=document.executor,
        application_client_factory=application, build_client_factory=build)
    async def payload():
        yield b"artifact"
    chunks = payload()
    if purpose == "personal-build-worker" and boundary == "exact":
        assert await router.upload_artifact(packet.claim, worker_credential=packet.worker_credential,
            artifact=packet.artifact, chunks=chunks) == response
    else:
        with pytest.raises((RuntimeError, ValueError)):
            await router.upload_artifact(packet.claim, worker_credential=packet.worker_credential,
                artifact=packet.artifact, chunks=chunks)
    assert close.await_count == (1 if purpose == "personal-build-worker" else 0)
    if purpose == "personal-build-worker":
        upload.assert_awaited_once_with(packet.claim, worker_credential=packet.worker_credential,
            artifact=packet.artifact, chunks=chunks)
    else:
        upload.assert_not_awaited()
    await chunks.aclose()
