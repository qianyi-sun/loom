from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import AsyncGenerator
from dataclasses import replace
from typing import Any, Literal

import pytest

from loom_task_image_authority.oci_verification import (
    OCIDescriptor,
    OCIVerificationError,
    OCIVerificationLimits,
    verify_oci_graph,
)

OCI = "application/vnd.oci.image."
DOCKER = "application/vnd.docker.distribution."

# Exact bytes from the pinned v0.32.2-loom.1 OCI exporter for FROM scratch + LABEL.
# Independent exporter digests and sizes are asserted by the acceptance test.
SCRATCH_CONFIG = (
    b'{"architecture":"amd64","config":{"Env":["PATH=/usr/local/sbin:/usr/local/bin:'
    b'/usr/sbin:/usr/bin:/sbin:/bin"],"WorkingDir":"/","Labels":'
    b'{"loom.metadata-probe":"scratch"}},"created":null,"history":'
    b'[{"created_by":"LABEL loom.metadata-probe=scratch","comment":'
    b'"buildkit.dockerfile.v0","empty_layer":true}],"os":"linux",'
    b'"rootfs":{"type":"layers","diff_ids":null}}'
)
SCRATCH_MANIFEST = b"""{
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {
    "mediaType": "application/vnd.oci.image.config.v1+json",
    "digest": "sha256:bca44aac00510a017b2bdf51659857715fb6df19802e8a7e8e340f6cacbeca92",
    "size": 359
  },
  "layers": null
}"""


class Reader:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.requests: list[tuple[str, str]] = []
        self.closed: list[str] = []
        self.chunk_size = 19
        self.failure: BaseException | None = None

    def put(self, value: Any, media_type: str) -> OCIDescriptor:
        payload = value if isinstance(value, bytes) else json.dumps(value).encode()
        digest = "sha256:" + hashlib.sha256(payload).hexdigest()
        self.objects[digest] = payload
        return OCIDescriptor(media_type, digest, len(payload))

    async def read(
        self,
        kind: Literal["manifest", "blob"],
        descriptor: OCIDescriptor,
    ) -> AsyncGenerator[bytes, None]:
        self.requests.append((kind, descriptor.digest))
        try:
            payload = self.objects[descriptor.digest]
            for offset in range(0, len(payload), self.chunk_size):
                yield payload[offset : offset + self.chunk_size]
                if self.failure is not None:
                    raise self.failure
        finally:
            self.closed.append(descriptor.digest)


def _descriptor(value: OCIDescriptor, **extra: object) -> dict[str, object]:
    return {
        "mediaType": value.media_type,
        "digest": value.digest,
        "size": value.size,
        **extra,
    }


def _graph(
    arch: str = "amd64",
    *,
    docker: bool = False,
    indexed: bool = False,
    config_changes: dict[str, object] | None = None,
    layer_changes: dict[str, object] | None = None,
) -> tuple[Reader, OCIDescriptor, OCIDescriptor]:
    reader = Reader()
    layer = reader.put(
        b"compressed-layer-bytes",
        (DOCKER + "image.rootfs.diff.tar.gzip" if docker else OCI + "layer.v1.tar+gzip"),
    )
    config = reader.put(
        {
            "architecture": arch,
            "os": "linux",
            "rootfs": {"type": "layers", "diff_ids": ["sha256:" + "d" * 64]},
            "config": {"Env": ["BENCHMARK=test"]},
            **(config_changes or {}),
        },
        "application/vnd.docker.container.image.v1+json" if docker else OCI + "config.v1+json",
    )
    media = DOCKER + "manifest.v2+json" if docker else OCI + "manifest.v1+json"
    manifest = reader.put(
        {
            "schemaVersion": 2,
            "mediaType": media,
            "config": _descriptor(config),
            "layers": [_descriptor(layer, **(layer_changes or {}))],
        },
        media,
    )
    if not indexed:
        return reader, manifest, layer
    media = DOCKER + "manifest.list.v2+json" if docker else OCI + "index.v1+json"
    index = reader.put(
        {
            "schemaVersion": 2,
            "mediaType": media,
            "manifests": [_descriptor(manifest, platform={"os": "linux", "architecture": arch})],
        },
        media,
    )
    return reader, index, layer


async def test_verifies_exact_buildkit_empty_scratch_export_without_rewriting_bytes() -> None:
    reader = Reader()
    config = reader.put(SCRATCH_CONFIG, OCI + "config.v1+json")
    root = reader.put(SCRATCH_MANIFEST, OCI + "manifest.v1+json")
    result = await verify_oci_graph(reader, root, "linux/amd64")
    assert result.root.digest == (
        "sha256:b6ec3b0cc39f2c4050beca5bc232260a4a09925ac19bfaac80e830066b5b1297"
    )
    assert result.root.size == 288
    assert result.manifest == result.root
    assert result.config.digest == (
        "sha256:bca44aac00510a017b2bdf51659857715fb6df19802e8a7e8e340f6cacbeca92"
    )
    assert result.config.size == 359
    assert result.total_bytes == 647
    assert result.layers == ()
    assert reader.requests == [("manifest", root.digest), ("blob", config.digest)]
    assert reader.closed == [root.digest, config.digest]


@pytest.mark.parametrize("layers", [None, []])
@pytest.mark.parametrize("diff_ids", [None, []])
async def test_empty_graph_accepts_explicit_null_or_empty_arrays(
    layers: list[object] | None,
    diff_ids: list[object] | None,
) -> None:
    reader = Reader()
    config_doc = json.loads(SCRATCH_CONFIG)
    config_doc["rootfs"]["diff_ids"] = diff_ids
    config = reader.put(config_doc, OCI + "config.v1+json")
    manifest = json.loads(SCRATCH_MANIFEST)
    manifest["config"] = _descriptor(config)
    manifest["layers"] = layers
    root = reader.put(manifest, OCI + "manifest.v1+json")
    result = await verify_oci_graph(reader, root, "linux/amd64")
    assert result.layers == ()
    assert result.root == root
    assert result.config == config


@pytest.mark.parametrize("field", ["layers", "diff_ids"])
@pytest.mark.parametrize("value", ["missing", False, 0, "", {}, [None]])
async def test_empty_graph_still_requires_explicit_typed_layer_lists(
    field: str,
    value: object,
) -> None:
    reader = Reader()
    config_doc = json.loads(SCRATCH_CONFIG)
    manifest = json.loads(SCRATCH_MANIFEST)
    target = manifest if field == "layers" else config_doc["rootfs"]
    if value == "missing":
        del target[field]
    else:
        target[field] = value
    config = reader.put(config_doc, OCI + "config.v1+json")
    manifest["config"] = _descriptor(config)
    root = reader.put(manifest, OCI + "manifest.v1+json")
    with pytest.raises(OCIVerificationError, match="invalid OCI graph schema"):
        await verify_oci_graph(reader, root, "linux/amd64")


@pytest.mark.parametrize("empty_side", ["layers", "diff_ids"])
async def test_null_layer_list_cannot_discard_nonempty_counterpart(empty_side: str) -> None:
    reader, original, _ = _graph()
    manifest = json.loads(reader.objects[original.digest])
    if empty_side == "layers":
        manifest["layers"] = None
    else:
        config_doc = json.loads(reader.objects[manifest["config"]["digest"]])
        config_doc["rootfs"]["diff_ids"] = None
        config = reader.put(config_doc, OCI + "config.v1+json")
        manifest["config"] = _descriptor(config)
    root = reader.put(manifest, OCI + "manifest.v1+json")
    with pytest.raises(OCIVerificationError, match="OCI rootfs layer mismatch"):
        await verify_oci_graph(reader, root, "linux/amd64")


@pytest.mark.parametrize("mutation", ["digest", "size", "subject"])
async def test_empty_graph_retains_descriptor_and_unknown_field_checks(mutation: str) -> None:
    reader = Reader()
    config = reader.put(SCRATCH_CONFIG, OCI + "config.v1+json")
    manifest = json.loads(SCRATCH_MANIFEST)
    if mutation == "digest":
        reader.objects[config.digest] = SCRATCH_CONFIG.replace(b"scratch", b"altered")
    elif mutation == "size":
        manifest["config"]["size"] += 1
    else:
        manifest["subject"] = None
    root = reader.put(manifest, OCI + "manifest.v1+json")
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")
    assert reader.closed == [digest for _, digest in reader.requests]


@pytest.mark.parametrize("arch", ["amd64", "arm64"])
@pytest.mark.parametrize("docker", [False, True])
@pytest.mark.parametrize("indexed", [False, True])
async def test_verifies_each_referenced_byte_and_native_platform(
    arch: str,
    docker: bool,
    indexed: bool,
) -> None:
    reader, root, layer = _graph(arch, docker=docker, indexed=indexed)
    result = await verify_oci_graph(reader, root, f"linux/{arch}")
    assert result.root == root
    assert result.layers == (layer,)
    assert result.platform == f"linux/{arch}"
    assert result.total_bytes == sum(len(reader.objects[digest]) for _, digest in reader.requests)
    assert reader.requests[-1] == ("blob", layer.digest)
    assert len(reader.requests) == (4 if indexed else 3)
    assert reader.closed == [digest for _, digest in reader.requests]


@pytest.mark.parametrize("target", ["manifest", "config", "layer"])
@pytest.mark.parametrize("mutation", ["truncate", "extend", "replace"])
async def test_rejects_corruption_at_every_graph_level(target: str, mutation: str) -> None:
    reader, root, layer = _graph()
    manifest = json.loads(reader.objects[root.digest])
    digest = {
        "manifest": root.digest,
        "config": manifest["config"]["digest"],
        "layer": layer.digest,
    }[target]
    payload = reader.objects[digest]
    reader.objects[digest] = {
        "truncate": payload[:-1],
        "extend": payload + b"x",
        "replace": b"x" * len(payload),
    }[mutation]
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")
    assert reader.closed == [value for _, value in reader.requests]


@pytest.mark.parametrize(
    "changes",
    [
        {"os": "windows"},
        {"architecture": "arm64"},
        {"rootfs": {"type": "unknown", "diff_ids": ["sha256:" + "d" * 64]}},
        {"rootfs": {"type": "layers", "diff_ids": []}},
        {"rootfs": {"type": "layers", "diff_ids": ["sha256:BAD"]}},
    ],
)
async def test_rejects_config_platform_and_rootfs_mismatch(changes: dict[str, object]) -> None:
    reader, root, _ = _graph(config_changes=changes)
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")


@pytest.mark.parametrize(
    "changes",
    [
        {"urls": ["https://untrusted.example/layer"]},
        {"mediaType": OCI + "layer.nondistributable.v1.tar+gzip"},
        {"size": True},
        {"size": -1},
        {"digest": "sha256:" + "A" * 64},
        {"data": "embedded-content"},
    ],
)
async def test_rejects_unsafe_descriptors(changes: dict[str, object]) -> None:
    reader, root, _ = _graph(layer_changes=changes)
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")


@pytest.mark.parametrize(
    "payload",
    [
        b'{"schemaVersion":2,"schemaVersion":2}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b"\xff",
        b"[" * 1000 + b"0" + b"]" * 1000,
    ],
)
async def test_rejects_ambiguous_or_unbounded_json(payload: bytes) -> None:
    reader = Reader()
    root = reader.put(payload, OCI + "manifest.v1+json")
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")


@pytest.mark.parametrize(
    "field,value",
    [
        ("maximum_json_bytes", 32),
        ("maximum_total_bytes", 32),
        ("maximum_descriptors", 2),
        ("maximum_chunk_bytes", 8),
        ("maximum_json_depth", 2),
    ],
)
async def test_enforces_graph_resource_limits(field: str, value: int) -> None:
    reader, root, _ = _graph()
    limits = replace(OCIVerificationLimits(), **{field: value})
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64", limits=limits)
    assert reader.closed == [digest for _, digest in reader.requests]


@pytest.mark.parametrize("failure", [asyncio.CancelledError(), OSError("transport down")])
async def test_closes_reader_and_preserves_cancellation_or_transport_error(
    failure: BaseException,
) -> None:
    reader, root, _ = _graph()
    reader.failure = failure
    with pytest.raises(type(failure)):
        await verify_oci_graph(reader, root, "linux/amd64")
    assert reader.closed == [root.digest]


@pytest.mark.parametrize("change", ["multiple", "nested", "platform", "subject"])
async def test_index_is_one_native_runnable_image(change: str) -> None:
    reader, root, _ = _graph(indexed=True)
    doc = json.loads(reader.objects[root.digest])
    if change == "multiple":
        doc["manifests"] *= 2
    elif change == "nested":
        doc["manifests"][0]["mediaType"] = OCI + "index.v1+json"
    elif change == "platform":
        doc["manifests"][0]["platform"]["architecture"] = "arm64"
    else:
        doc["subject"] = doc["manifests"][0]
    root = reader.put(doc, root.media_type)
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")


async def test_repeated_digest_with_conflicting_descriptor_is_rejected() -> None:
    reader, root, layer = _graph()
    doc = json.loads(reader.objects[root.digest])
    config = json.loads(reader.objects[doc["config"]["digest"]])
    config["rootfs"]["diff_ids"] *= 2
    config_descriptor = reader.put(config, OCI + "config.v1+json")
    doc["config"] = _descriptor(config_descriptor)
    # Both references read valid identical bytes: only the contradictory media
    # type is invalid, with no independent size or rootfs failure masking it.
    doc["layers"].append(_descriptor(layer, mediaType=OCI + "layer.v1.tar+zstd"))
    root = reader.put(doc, root.media_type)
    with pytest.raises(OCIVerificationError, match="conflicting OCI descriptor"):
        await verify_oci_graph(reader, root, "linux/amd64")
    assert reader.requests == [("manifest", root.digest)]


@pytest.mark.parametrize(
    "changes",
    [
        {"os.version": "unsupported-version"},
        {"os.features": ["unsupported-required-feature"]},
        {"variant": "v9"},
    ],
)
@pytest.mark.parametrize("indexed", [False, True])
async def test_rejects_platform_requirements_without_certified_support(
    changes: dict[str, object],
    indexed: bool,
) -> None:
    reader, root, _ = _graph("arm64", indexed=indexed, config_changes=changes)
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/arm64")


@pytest.mark.parametrize(
    "field,value",
    [
        ("schemaVersion", 2.0),
        ("schemaVersion", True),
        ("annotations", {"overflow": float("inf")}),
        ("annotations", {"surrogate": "\ud800"}),
    ],
)
async def test_manifest_json_has_exact_numbers_and_valid_unicode(field: str, value: Any) -> None:
    reader, root, _ = _graph()
    doc = json.loads(reader.objects[root.digest])
    doc[field] = value
    root = reader.put(doc, root.media_type)
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")


async def test_json_exponent_overflow_is_rejected_in_config() -> None:
    reader, root, _ = _graph()
    doc = json.loads(reader.objects[root.digest])
    old = doc["config"]["digest"]
    payload = reader.objects[old].replace(b'"BENCHMARK=test"', b"1e9999")
    config = reader.put(payload, OCI + "config.v1+json")
    doc["config"] = _descriptor(config)
    root = reader.put(doc, root.media_type)
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")


@pytest.mark.parametrize("count", [0, 2])
async def test_empty_and_repeated_layer_images_preserve_exact_order(count: int) -> None:
    reader, root, layer = _graph()
    doc = json.loads(reader.objects[root.digest])
    config = json.loads(reader.objects[doc["config"]["digest"]])
    config["rootfs"]["diff_ids"] *= count
    config_descriptor = reader.put(config, OCI + "config.v1+json")
    doc["config"] = _descriptor(config_descriptor)
    doc["layers"] *= count
    root = reader.put(doc, root.media_type)
    result = await verify_oci_graph(reader, root, "linux/amd64")
    assert result.layers == (layer,) * count
    assert reader.requests.count(("blob", layer.digest)) == count
    if count:
        with pytest.raises(OCIVerificationError):
            await verify_oci_graph(
                reader, root, "linux/amd64", limits=OCIVerificationLimits(maximum_layers=1)
            )


async def test_manifest_cannot_mix_docker_config_into_oci_media_profile() -> None:
    reader, root, _ = _graph()
    doc = json.loads(reader.objects[root.digest])
    doc["config"]["mediaType"] = "application/vnd.docker.container.image.v1+json"
    root = reader.put(doc, root.media_type)
    with pytest.raises(OCIVerificationError):
        await verify_oci_graph(reader, root, "linux/amd64")
