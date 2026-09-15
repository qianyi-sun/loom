"""Retain exact node delivery identity before transmitting a bootstrap capability.

A delivery receipt is historical evidence of publication, not worker admission.
This component neither submits scheduler jobs nor creates worker credentials.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Protocol

from loom_capacity_agent.admission import PhysicalJobBindingV2
from loom_capacity_executor.bootstrap_handoff import BootstrapHandoffStore
from loom_capacity_executor.journal import ExecutorJournal
from loom_capacity_executor.native_bootstrap_delivery import (
    BootstrapDeliveryError,
    NativeBootstrapDeliveryReceiptV1,
    encode_native_delivery_query,
    expected_native_delivery_receipt,
    export_native_bootstrap,
    parse_native_delivery_query,
)


class NativeBootstrapDeliveryClient(Protocol):
    async def deliver(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1: ...

    async def observe_receipt(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1 | None: ...


class NativeBootstrapOutbox:
    """One controller's serialized, durable delivery under fixed installed routes."""

    def __init__(self, *, journal: ExecutorJournal, store: BootstrapHandoffStore,
                 clients: Mapping[str, NativeBootstrapDeliveryClient],
                 configuration_sha256: str, now: Callable[[], datetime]) -> None:
        if (not isinstance(configuration_sha256, str)
                or re.fullmatch(r"[0-9a-f]{64}", configuration_sha256) is None
                or configuration_sha256 == "0" * 64 or not clients):
            raise ValueError("native delivery configuration is invalid")
        self._journal, self._store = journal, store
        self._clients = dict(clients)
        self._configuration = configuration_sha256
        self._now = now
        self._lock = asyncio.Lock()

    def _payload(self, query: bytes) -> bytes:
        return json.dumps({"configuration_sha256": self._configuration,
                           "query": json.loads(query)},
                          sort_keys=True, separators=(",", ":"), allow_nan=False).encode()

    async def deliver(self, physical: PhysicalJobBindingV2) -> NativeBootstrapDeliveryReceiptV1:
        async with self._lock:
            return await self._deliver(physical)

    async def _deliver(self, physical: PhysicalJobBindingV2) -> NativeBootstrapDeliveryReceiptV1:
        if len(physical.binding.node_ids) != 1:
            raise BootstrapDeliveryError("native delivery requires one exact node")
        client = self._clients.get(physical.binding.node_ids[0])
        if client is None:
            raise BootstrapDeliveryError("native delivery node is not installed")
        identity = "native-delivery:" + str(physical.binding.intent_id)
        retained = self._journal.latest("executor", identity)
        capability: bytes | None = None
        if retained is None:
            capability = export_native_bootstrap(self._store, physical, now=self._now)
            expected = expected_native_delivery_receipt(capability)
            query = encode_native_delivery_query(physical, expected)
            payload = self._payload(query)
            self._journal.append("native-delivery-requested", hashlib.sha256(payload).hexdigest(),
                                 object_kind="executor", object_id=identity, payload=payload)
        else:
            if retained.event_kind not in {"native-delivery-requested", "native-delivery-confirmed"}:
                raise BootstrapDeliveryError("native delivery journal state changed")
            durable = retained.durable_payload()
            if durable is None:
                raise BootstrapDeliveryError("native delivery intent is unavailable")
            try:
                payload = durable
                value = json.loads(payload)
                query = json.dumps(value["query"], sort_keys=True, separators=(",", ":"),
                                   allow_nan=False).encode()
                saved = parse_native_delivery_query(query)
                if saved.physical != physical or payload != self._payload(query):
                    raise ValueError("changed identity")
                expected = saved.expected
            except (ValueError, KeyError, TypeError, AttributeError) as exc:
                raise BootstrapDeliveryError("native delivery identity or configuration changed") from exc
            if retained.event_kind == "native-delivery-confirmed":
                return expected
            # Query first: remote publication/consumption may have succeeded even
            # when the local capability is now expired or unavailable.
            observed = await client.observe_receipt(query)
            if observed is not None:
                return self._confirm(identity, payload, expected, observed)
        if capability is None:
            capability = export_native_bootstrap(self._store, physical, now=self._now)
        if expected_native_delivery_receipt(capability) != expected:
            raise BootstrapDeliveryError("native delivery source changed after retention")
        observed = await client.deliver(capability)
        return self._confirm(identity, payload, expected, observed)

    def _confirm(self, identity: str, payload: bytes, expected: NativeBootstrapDeliveryReceiptV1,
                 observed: NativeBootstrapDeliveryReceiptV1) -> NativeBootstrapDeliveryReceiptV1:
        if not isinstance(observed, NativeBootstrapDeliveryReceiptV1) or observed != expected:
            raise BootstrapDeliveryError("native delivery receipt differs from retained identity")
        self._journal.append("native-delivery-confirmed", hashlib.sha256(payload).hexdigest(),
                             object_kind="executor", object_id=identity, payload=payload)
        return expected
