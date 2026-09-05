"""Bounded verification of registry bytes; no persistence or readiness authority."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import AsyncGenerator, Iterator
from contextlib import aclosing
from dataclasses import dataclass, fields
from typing import Annotated, Any, Literal, Protocol

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, ValidationError

_OCI = "application/vnd.oci.image."
_DOCKER = "application/vnd.docker.distribution."
_MANIFESTS = frozenset({_OCI + "manifest.v1+json", _DOCKER + "manifest.v2+json"})
_INDEXES = frozenset({_OCI + "index.v1+json", _DOCKER + "manifest.list.v2+json"})
_LAYERS = frozenset(
    {
        _OCI + "layer.v1.tar",
        _OCI + "layer.v1.tar+gzip",
        _OCI + "layer.v1.tar+zstd",
        _DOCKER + "image.rootfs.diff.tar.gzip",
    }
)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


class OCIVerificationError(ValueError):
    """Untrusted registry evidence is invalid; never a deterministic build error."""


@dataclass(frozen=True)
class OCIDescriptor:
    media_type: str
    digest: str
    size: int

    def __post_init__(self) -> None:
        if (
            type(self.media_type) is not str
            or not 1 <= len(self.media_type) <= 128
            or type(self.digest) is not str
            or _DIGEST.fullmatch(self.digest) is None
            or type(self.size) is not int
            or not 0 <= self.size <= 100 * 1024**3
        ):
            raise OCIVerificationError("invalid OCI descriptor")


@dataclass(frozen=True)
class OCIVerificationLimits:
    maximum_json_bytes: int = 4 * 1024**2
    maximum_total_bytes: int = 100 * 1024**3
    maximum_descriptors: int = 256
    maximum_layers: int = 128
    maximum_chunk_bytes: int = 1024**2
    maximum_json_depth: int = 32

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            if (
                type(item.default) is not int
                or type(value) is not int
                or not 0 < value <= item.default
            ):
                raise ValueError("OCI limits must be positive and within hard ceilings")


_DEFAULT_LIMITS = OCIVerificationLimits()


class OCIContentReader(Protocol):
    """Repository-bound transport that owns response closure in its generator."""

    def read(
        self,
        kind: Literal["manifest", "blob"],
        descriptor: OCIDescriptor,
    ) -> AsyncGenerator[bytes, None]: ...


@dataclass(frozen=True)
class VerifiedOCIGraph:
    root: OCIDescriptor
    manifest: OCIDescriptor
    config: OCIDescriptor
    layers: tuple[OCIDescriptor, ...]
    platform: str
    total_bytes: int


class _ClosedModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class _Platform(_ClosedModel):
    os: str
    architecture: str
    variant: str = ""


class _Descriptor(_ClosedModel):
    media_type: str = Field(alias="mediaType")
    digest: str
    size: int
    platform: _Platform | None = None
    annotations: dict[str, str] = Field(default_factory=dict)

    def value(self) -> OCIDescriptor:
        return OCIDescriptor(self.media_type, self.digest, self.size)


class _Index(_ClosedModel):
    schema_version: int = Field(alias="schemaVersion", ge=2, le=2)
    media_type: str = Field(alias="mediaType")
    manifests: Annotated[list[_Descriptor], Field(min_length=1, max_length=1)]
    annotations: dict[str, str] = Field(default_factory=dict)


def _empty_array_from_null(value: Any) -> Any:
    # BuildKit encodes empty Go slices as null. This is used only for the two
    # explicit layer lists; missing fields and every other type still reject.
    return [] if value is None else value


class _Manifest(_ClosedModel):
    schema_version: int = Field(alias="schemaVersion", ge=2, le=2)
    media_type: str = Field(alias="mediaType")
    config: _Descriptor
    layers: Annotated[
        list[_Descriptor], Field(max_length=128), BeforeValidator(_empty_array_from_null)
    ]
    annotations: dict[str, str] = Field(default_factory=dict)


class _RootFS(_ClosedModel):
    type: Literal["layers"]
    diff_ids: Annotated[list[str], Field(max_length=128), BeforeValidator(_empty_array_from_null)]


class _Config(_ClosedModel):
    architecture: str
    os: str
    variant: str = ""
    os_version: str = Field(default="", alias="os.version")
    os_features: list[str] = Field(default_factory=list, alias="os.features")
    created: str | None = None
    author: str = ""
    rootfs: _RootFS
    config: dict[str, Any] = Field(default_factory=dict)
    history: list[dict[str, Any]] = Field(default_factory=list)


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise OCIVerificationError("duplicate OCI JSON key")
        result[key] = value
    return result


def _reject_constant(value: str) -> Any:
    raise OCIVerificationError("non-finite OCI JSON number")


def _json(payload: bytes, maximum_depth: int) -> dict[str, Any]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
        if not isinstance(value, dict):
            raise OCIVerificationError("OCI JSON must be an object")
        # Retain one iterator per depth, not one tuple per JSON value. Wide
        # documents must not multiply the parser's bounded memory footprint.
        pending: list[Iterator[Any]] = [iter((value,))]
        while pending:
            try:
                node = next(pending[-1])
            except StopIteration:
                pending.pop()
                continue
            if len(pending) > maximum_depth:
                raise OCIVerificationError("OCI JSON depth exceeded")
            if isinstance(node, dict):
                for key in node:
                    key.encode("utf-8")
                pending.append(iter(node.values()))
            elif isinstance(node, list):
                pending.append(iter(node))
            elif isinstance(node, str):
                node.encode("utf-8")
            elif isinstance(node, float) and not math.isfinite(node):
                raise OCIVerificationError("non-finite OCI JSON number")
        return value
    except (UnicodeError, ValueError, RecursionError, OverflowError):
        raise OCIVerificationError("invalid OCI JSON") from None


def _check_platform(os: str, architecture: str, variant: str, expected: str) -> None:
    if (
        f"{os}/{architecture}" != expected
        or (architecture == "amd64" and variant != "")
        or (architecture == "arm64" and variant not in {"", "v8"})
    ):
        raise OCIVerificationError("OCI platform mismatch")


class _GraphReader:
    def __init__(self, reader: OCIContentReader, limits: OCIVerificationLimits) -> None:
        self.reader = reader
        self.limits = limits
        self.descriptors: dict[str, OCIDescriptor] = {}
        self.total_bytes = 0
        self.count = 0

    def account(self, descriptor: OCIDescriptor) -> None:
        previous = self.descriptors.get(descriptor.digest)
        if previous is not None and previous != descriptor:
            raise OCIVerificationError("conflicting OCI descriptor")
        self.descriptors[descriptor.digest] = descriptor
        self.count += 1
        if self.count > self.limits.maximum_descriptors:
            raise OCIVerificationError("OCI descriptor count exceeded")
        self.total_bytes += descriptor.size
        if self.total_bytes > self.limits.maximum_total_bytes:
            raise OCIVerificationError("OCI total size exceeded")

    async def fetch(
        self,
        descriptor: OCIDescriptor,
        kind: Literal["manifest", "blob"],
        *,
        document: bool,
    ) -> bytes:
        if document and descriptor.size > self.limits.maximum_json_bytes:
            raise OCIVerificationError("OCI JSON size exceeded")
        observed = 0
        digest = hashlib.sha256()
        payload = bytearray()
        async with aclosing(self.reader.read(kind, descriptor)) as stream:
            async for chunk in stream:
                if (
                    type(chunk) is not bytes
                    or not 0 < len(chunk) <= self.limits.maximum_chunk_bytes
                ):
                    raise OCIVerificationError("invalid OCI stream chunk")
                observed += len(chunk)
                if observed > descriptor.size:
                    raise OCIVerificationError("OCI byte count mismatch")
                digest.update(chunk)
                if document:
                    payload.extend(chunk)
        if observed != descriptor.size or "sha256:" + digest.hexdigest() != descriptor.digest:
            raise OCIVerificationError("OCI size or digest mismatch")
        return bytes(payload)

    async def document(
        self,
        descriptor: OCIDescriptor,
        kind: Literal["manifest", "blob"],
    ) -> dict[str, Any]:
        return _json(
            await self.fetch(descriptor, kind, document=True),
            self.limits.maximum_json_depth,
        )


async def verify_oci_graph(
    reader: OCIContentReader,
    root: OCIDescriptor,
    platform: str,
    *,
    limits: OCIVerificationLimits = _DEFAULT_LIMITS,
) -> VerifiedOCIGraph:
    """Verify every descriptor by GET bytes; never trust registry HEAD or labels.

    The caller owns the operation deadline and liveness checks. Transport errors
    and cancellation propagate after stream cleanup; validation errors contain
    no registry payloads. Only JSON documents are retained in memory.
    """
    if platform not in {"linux/amd64", "linux/arm64"}:
        raise OCIVerificationError("unsupported OCI platform")
    graph = _GraphReader(reader, limits)
    graph.account(root)
    if root.media_type not in _MANIFESTS | _INDEXES:
        raise OCIVerificationError("unsupported OCI root type")
    try:
        manifest_descriptor = root
        document = await graph.document(root, "manifest")
        if root.media_type in _INDEXES:
            index = _Index.model_validate(document)
            if index.media_type != root.media_type:
                raise OCIVerificationError("OCI index type mismatch")
            selected = index.manifests[0]
            expected_manifest = (
                _OCI + "manifest.v1+json"
                if root.media_type == _OCI + "index.v1+json"
                else _DOCKER + "manifest.v2+json"
            )
            if selected.media_type != expected_manifest or selected.platform is None:
                raise OCIVerificationError("OCI index requires one runnable image")
            _check_platform(
                selected.platform.os,
                selected.platform.architecture,
                selected.platform.variant,
                platform,
            )
            manifest_descriptor = selected.value()
            graph.account(manifest_descriptor)
            document = await graph.document(manifest_descriptor, "manifest")
        manifest = _Manifest.model_validate(document)
        if manifest.media_type != manifest_descriptor.media_type:
            raise OCIVerificationError("OCI manifest type mismatch")
        config_descriptor = manifest.config.value()
        layers = tuple(layer.value() for layer in manifest.layers)
        is_oci = manifest.media_type == _OCI + "manifest.v1+json"
        expected_config = (
            _OCI + "config.v1+json" if is_oci else "application/vnd.docker.container.image.v1+json"
        )
        expected_layers = (
            _LAYERS - {_DOCKER + "image.rootfs.diff.tar.gzip"}
            if is_oci
            else {_DOCKER + "image.rootfs.diff.tar.gzip"}
        )
        if (
            config_descriptor.media_type != expected_config
            or len(layers) > limits.maximum_layers
            or any(layer.media_type not in expected_layers for layer in layers)
        ):
            raise OCIVerificationError("unsupported OCI config or layer")
        for descriptor in (manifest.config, *manifest.layers):
            if descriptor.platform is not None:
                _check_platform(
                    descriptor.platform.os,
                    descriptor.platform.architecture,
                    descriptor.platform.variant,
                    platform,
                )
        # Account for the entire declared graph before starting large downloads.
        for descriptor_value in (config_descriptor, *layers):
            graph.account(descriptor_value)
        config = _Config.model_validate(await graph.document(config_descriptor, "blob"))
        _check_platform(config.os, config.architecture, config.variant, platform)
        if config.os_version or config.os_features:
            raise OCIVerificationError("unsupported OCI platform requirements")
        if len(config.rootfs.diff_ids) != len(layers) or any(
            _DIGEST.fullmatch(value) is None for value in config.rootfs.diff_ids
        ):
            raise OCIVerificationError("OCI rootfs layer mismatch")
        for layer in layers:
            await graph.fetch(layer, "blob", document=False)
    except ValidationError:
        raise OCIVerificationError("invalid OCI graph schema") from None
    return VerifiedOCIGraph(
        root,
        manifest_descriptor,
        config_descriptor,
        layers,
        platform,
        graph.total_bytes,
    )
