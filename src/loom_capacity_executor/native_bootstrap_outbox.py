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
from loom_capacity_executor.journal import ExecutorJournal, JournalRecord, JournalRegressionError
from loom_capacity_executor.native_bootstrap_delivery import (
    BootstrapDeliveryError,
    NativeBootstrapDeliveryQueryV1,
    NativeBootstrapDeliveryReceiptV1,
    encode_native_delivery_query,
    expected_native_delivery_receipt,
    export_native_bootstrap,
    parse_native_delivery_query,
)

NATIVE_DELIVERY_EVENTS = {"native-delivery-retained", "native-delivery-confirmed"}


def retained_native_delivery(record: JournalRecord) -> tuple[str, NativeBootstrapDeliveryQueryV1]:
    """Validate capability-free durable identity for both recovery and retention."""
    try:
        payload = record.durable_payload()
        if payload is None:
            raise ValueError
        value = json.loads(payload)
        configuration = value["configuration_sha256"]
        query = parse_native_delivery_query(json.dumps(value["query"], sort_keys=True,
            separators=(",", ":"), allow_nan=False).encode())
        if (record.event_kind not in NATIVE_DELIVERY_EVENTS or record.object_kind != "executor"
            or record.object_id != "native-delivery:" + str(query.physical.binding.intent_id)
            or set(value) != {"configuration_sha256", "query"}
            or not isinstance(configuration, str)
            or re.fullmatch(r"[0-9a-f]{64}", configuration) is None or configuration == "0" * 64
            or json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode() != payload
            or hashlib.sha256(payload).hexdigest() != record.payload_digest):
            raise ValueError
        return configuration, query
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise JournalRegressionError("native delivery retained identity changed") from exc


class NativeBootstrapDeliveryClient(Protocol):
    async def aclose(self) -> None: ...

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

    async def aclose(self) -> None:
        await asyncio.gather(*(client.aclose() for client in self._clients.values()))

    def confirmed(self, physical: PhysicalJobBindingV2) -> bool:
        record = self._journal.latest("executor", "native-delivery:" + str(physical.binding.intent_id))
        if record is None:
            return False
        configuration, saved = retained_native_delivery(record)
        if saved.physical != physical or configuration != self._configuration:
            raise BootstrapDeliveryError("native delivery identity or configuration changed")
        return record.event_kind == "native-delivery-confirmed"

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
            self._journal.append("native-delivery-retained", hashlib.sha256(payload).hexdigest(),
                                 object_kind="executor", object_id=identity, payload=payload)
        else:
            configuration, saved = retained_native_delivery(retained)
            query = encode_native_delivery_query(saved.physical, saved.expected)
            if saved.physical != physical or configuration != self._configuration:
                raise BootstrapDeliveryError("native delivery identity or configuration changed")
            payload = self._payload(query)
            expected = saved.expected
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
