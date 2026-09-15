"""Activation-bound node routes and private controller TLS identity for delivery."""

from __future__ import annotations

import json
from dataclasses import fields
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, Annotated, Self
from uuid import UUID

from pydantic import Field, TypeAdapter, field_validator, model_validator

from loom_capacity_executor.native_bootstrap_delivery import (
    BootstrapDeliveryError,
    NativeBootstrapDeliveryReceiptV1,
)
from loom_capacity_executor.native_bootstrap_outbox import NativeBootstrapOutbox
from loom_capacity_executor.native_bootstrap_transport import (
    NativeBootstrapRoute,
    NativeBootstrapTLSClient,
    NativeBootstrapTLSIdentity,
    _tls_context,
)
from loom_capacity_executor.pinned_admission_transport import PinnedAdmissionFileV1
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_digest

if TYPE_CHECKING:
    from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffStore
    from loom_capacity_executor.journal import ExecutorJournal
    from loom_capacity_executor.runtime import _ActivationRuntimeDocumentBaseV2


class NativeDeliveryFileV1(StrictV1Model):
    """Portable pin: local ownership and symlink checks run only on the target."""

    path: str = Field(min_length=1, max_length=4096)
    sha256: Digest

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if not path.is_absolute() or str(path) != value or value == "/" or ".." in path.parts or "\0" in value:
            raise ValueError("native delivery file path is not canonical")
        return value

    def local_pin(self) -> PinnedAdmissionFileV1:
        return PinnedAdmissionFileV1(path=self.path, sha256=self.sha256)


class NativeBootstrapIdentityV1(StrictV1Model):
    ca: NativeDeliveryFileV1
    certificate: NativeDeliveryFileV1
    private_key: NativeDeliveryFileV1

    def local_identity(self) -> NativeBootstrapTLSIdentity:
        return NativeBootstrapTLSIdentity(ca=self.ca.local_pin(), certificate=self.certificate.local_pin(),
            private_key=self.private_key.local_pin())


class _LazyNativeDeliveryClient:
    """Cleanup can assemble without loading expired or unavailable delivery keys."""

    def __init__(self, route: NativeBootstrapRoute, identity: NativeBootstrapIdentityV1) -> None:
        self._route, self._identity = route, identity
        self._transport: NativeBootstrapTLSClient | None = None
        self._closed = False

    def _client(self) -> NativeBootstrapTLSClient:
        if self._closed:
            raise BootstrapDeliveryError("native delivery client is closed")
        if self._transport is None:
            self._transport = NativeBootstrapTLSClient(route=self._route, identity=self._identity.local_identity())
        return self._transport

    async def deliver(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1:
        return await self._client().deliver(raw)

    async def observe_receipt(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1 | None:
        return await self._client().observe_receipt(raw)

    async def aclose(self) -> None:
        self._closed = True
        if self._transport is not None:
            await self._transport.aclose()


class NativeBootstrapConfigurationV1(StrictV1Model):
    executor_id: str
    executor_incarnation: UUID
    identity: NativeBootstrapIdentityV1
    routes: Annotated[tuple[NativeBootstrapRoute, ...], Field(min_length=1, max_length=64)]

    @field_validator("routes", mode="before")
    @classmethod
    def _route_json_boundary(cls, value: object) -> object:
        if not isinstance(value, (list, tuple)):
            return value
        result = []
        for item in value:
            if isinstance(item, dict):
                if set(item) != {field.name for field in fields(NativeBootstrapRoute)}:
                    raise ValueError("native delivery route fields changed")
                item = TypeAdapter(NativeBootstrapRoute).validate_json(json.dumps(item, allow_nan=False), strict=True)
            result.append(item)
        return tuple(result)

    @model_validator(mode="after")
    def _unique_routes(self) -> Self:
        nodes = [route.target_node for route in self.routes]
        endpoints = [(route.address, route.port) for route in self.routes]
        if nodes != sorted(set(nodes)) or len(endpoints) != len(set(endpoints)):
            raise ValueError("native delivery routes must have unique sorted nodes and endpoints")
        return self

    def assert_document(self, document: _ActivationRuntimeDocumentBaseV2) -> None:
        expected_nodes = {node for profile in document.profiles if profile.native_execution is not None
            for domain in profile.resource_domains for node in domain.node_ids}
        directory = PurePosixPath("/etc/loom-capacity-executor")
        if (self.executor_id != document.executor_id or self.executor_incarnation != document.executor_incarnation
            or {route.target_node for route in self.routes} != expected_nodes
            or any(route.pool_id != document.pool_id
                or route.trusted_release_sha256 != document.execution.trusted_fleet_release_sha256
                for route in self.routes)
            or any(pin.path != str(directory / f"{document.pool_id}-native-{name}") or pin.sha256 == "0" * 64
                for pin, name in ((self.identity.ca, "ca.pem"), (self.identity.certificate, "client.pem"),
                    (self.identity.private_key, "client-key.pem")))):
            raise ValueError("native delivery configuration differs from activation authority")

    def validate_local(self) -> None:
        if any(route.expires_at <= datetime.now(UTC) for route in self.routes):
            raise ValueError("native delivery route has expired")
        _tls_context(self.identity.local_identity(), server=False)

    def build(self, journal: ExecutorJournal, store: BootstrapHandoffStore) -> NativeBootstrapOutbox:
        # TLS is opened only by delivery. Drain and terminal recovery remain
        # available when a receiver route or its TLS files are no longer usable.
        return NativeBootstrapOutbox(journal=journal, store=store,
            clients={route.target_node: _LazyNativeDeliveryClient(route=route, identity=self.identity)
                for route in self.routes}, configuration_sha256=canonical_digest(self),
            now=lambda: datetime.now(UTC))
