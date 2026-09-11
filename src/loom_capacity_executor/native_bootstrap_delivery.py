"""Job-scoped bootstrap delivery between separate controller and node storage.

The fixed confidential transport owns peer/node authentication and supplies the
independently configured receiver. Never expose these bytes through argv, logs,
or public receipts. This module transfers a bootstrap capability, not a worker
credential, manager credential, signing key, or authority to prepare a cgroup.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import hashlib
import json
import math
import os
import tempfile
from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal, Protocol
from uuid import uuid5

from pydantic import BaseModel, ConfigDict, Field

from loom_capacity_agent.admission import CurrentExecutableBootstrapV2, PhysicalJobBindingV2
from loom_capacity_executor.bootstrap_handoff import (
    _OPERATION_NAMESPACE,
    BootstrapHandoffOwnershipV2,
    BootstrapHandoffRecordV2,
    BootstrapHandoffStore,
    _fsync_directory,
    _open_private_regular,
    _private_directory,
    _publish_private_new,
    _record_path,
    _reference,
    _route_sha256,
)
from loom_capacity_manager.executable_contracts import (
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.typed_ownership_contracts import _exact_schema_types

_MAX_DELIVERY_BYTES = 256 * 1024
_MAX_QUERY_BYTES = 64 * 1024
_MAX_RECEIPT_BYTES = 4096
_RECEIPT = "delivery-receipt.json"


class BootstrapDeliveryError(ValueError):
    """Sanitized refusal; secret transport bytes must not appear in diagnostics."""


class NativeBootstrapDeliveryV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_name: Literal["loom.native-bootstrap-delivery/v1"] = Field(default="loom.native-bootstrap-delivery/v1", alias="schema")
    record: BootstrapHandoffRecordV2 = Field(repr=False)
    ownership: BootstrapHandoffOwnershipV2
    physical: PhysicalJobBindingV2


class NativeBootstrapDeliveryReceiptV1(BaseModel):
    """Historical delivery evidence only, never registration or runtime authority."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_name: Literal["loom.native-bootstrap-delivery-receipt/v1"] = Field(default="loom.native-bootstrap-delivery-receipt/v1", alias="schema")
    target_node: str
    reference: str
    binding_sha256: str
    physical_binding_sha256: str
    bootstrap_sha256: str
    source_payload_sha256: str
    expires_at: datetime
    executable: Literal[False] = False


class NativeBootstrapDeliveryQueryV1(BaseModel):
    """Exact historical lookup, with no bootstrap capability or new authority."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_name: Literal["loom.native-bootstrap-delivery-query/v1"] = Field(default="loom.native-bootstrap-delivery-query/v1", alias="schema")
    physical: PhysicalJobBindingV2
    expected: NativeBootstrapDeliveryReceiptV1


class _Admission(Protocol):
    async def observe_current_bootstrap(self, request: PhysicalJobBindingV2) -> CurrentExecutableBootstrapV2: ...


def _canonical(value: BaseModel) -> bytes:
    return json.dumps(value.model_dump(mode="json", by_alias=True), sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False).encode("ascii")


def _decode(raw: bytes) -> NativeBootstrapDeliveryV1:
    try:
        if type(raw) is not bytes or not 0 < len(raw) <= _MAX_DELIVERY_BYTES:
            raise ValueError
        _exact_schema_types(json.loads(raw))
        value = NativeBootstrapDeliveryV1.model_validate_json(raw)
        record, ownership, physical = value.record, value.ownership, value.physical
        if (_canonical(value) != raw or len(physical.binding.node_ids) != 1
            or record.binding != physical.binding or ownership.binding != physical.binding
            or record.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
            or ownership.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
            or record.trusted_launcher_release_sha256 != physical.binding.execution.trusted_fleet_release_sha256
            or ownership.trusted_launcher_release_sha256 != record.trusted_launcher_release_sha256
            or record.expires_at != ownership.expires_at
            or physical.operation_id != uuid5(_OPERATION_NAMESPACE, f"physical-bind:{physical.binding.intent_id}")
            or ownership.ownership_evidence_sha256 != physical.ownership_evidence_sha256
            or hashlib.sha256(record.capability.encode("ascii")).hexdigest() != record.capability_sha256
            or physical.executable is not True):
            raise ValueError
        return value
    except (ValueError, TypeError, AttributeError, RecursionError):
        raise BootstrapDeliveryError("native bootstrap delivery is invalid") from None


def expected_native_delivery_receipt(raw: bytes) -> NativeBootstrapDeliveryReceiptV1:
    value = _decode(raw)
    record, physical = value.record, value.physical
    binding = physical.binding
    return NativeBootstrapDeliveryReceiptV1(target_node=binding.node_ids[0], reference=_reference(binding),
        binding_sha256=canonical_executable_digest(binding), physical_binding_sha256=canonical_executable_digest(physical),
        bootstrap_sha256=record.capability_sha256, source_payload_sha256=hashlib.sha256(raw).hexdigest(),
        expires_at=record.expires_at)


def parse_native_delivery_receipt(raw: bytes) -> NativeBootstrapDeliveryReceiptV1:
    try:
        if type(raw) is not bytes or not 0 < len(raw) <= _MAX_RECEIPT_BYTES:
            raise ValueError
        receipt = NativeBootstrapDeliveryReceiptV1.model_validate_json(raw)
        _record_path(Path("/"), receipt.reference)  # Grammar only, no filesystem access.
        digests = (receipt.binding_sha256, receipt.physical_binding_sha256,
            receipt.bootstrap_sha256, receipt.source_payload_sha256)
        if (_canonical(receipt) != raw or receipt.executable is not False
            or receipt.binding_sha256 != receipt.reference[:-5]
            or not 0 < len(receipt.target_node) <= 128
            or any(len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest) for digest in digests)
            or receipt.expires_at.tzinfo is None or receipt.expires_at.utcoffset() is None):
            raise ValueError
        return receipt
    except (ValueError, RuntimeError, OSError, TypeError, AttributeError):
        raise BootstrapDeliveryError("native bootstrap delivery receipt is invalid") from None


def parse_native_delivery_query(raw: bytes) -> NativeBootstrapDeliveryQueryV1:
    try:
        if type(raw) is not bytes or not 0 < len(raw) <= _MAX_QUERY_BYTES:
            raise ValueError
        query = NativeBootstrapDeliveryQueryV1.model_validate_json(raw)
        physical, expected = query.physical, query.expected
        parse_native_delivery_receipt(_canonical(expected))
        if (_canonical(query) != raw or physical.binding.node_ids != (expected.target_node,)
            or physical.executable is not True
            or physical.operation_id != uuid5(_OPERATION_NAMESPACE, f"physical-bind:{physical.binding.intent_id}")
            or canonical_executable_digest(physical) != expected.physical_binding_sha256
            or canonical_executable_digest(physical.binding) != expected.binding_sha256):
            raise ValueError
        return query
    except (ValueError, RuntimeError, OSError, TypeError, AttributeError):
        raise BootstrapDeliveryError("native bootstrap delivery query is invalid") from None


def encode_native_delivery_query(physical: PhysicalJobBindingV2,
    expected: NativeBootstrapDeliveryReceiptV1) -> bytes:
    raw = _canonical(NativeBootstrapDeliveryQueryV1(physical=physical, expected=expected))
    parse_native_delivery_query(raw)
    return raw


def export_native_bootstrap(store: BootstrapHandoffStore, physical: PhysicalJobBindingV2,
    *, now: Callable[[], datetime]) -> bytes:
    """Export only an unused capability and ownership, not mutable launch state."""
    try:
        _private_directory(store.directory)
        path = _record_path(store.directory, store.reference_for(physical.binding))
        for suffix in (".used", ".credential", ".launched"):
            candidate = path.with_suffix(suffix)
            if candidate.exists() or candidate.is_symlink():
                raise BootstrapDeliveryError("native bootstrap cannot export consumed state")
        record = BootstrapHandoffRecordV2.model_validate_json(_open_private_regular(path))
        ownership = BootstrapHandoffOwnershipV2.model_validate_json(_open_private_regular(path.with_suffix(".ownership")))
        raw = _canonical(NativeBootstrapDeliveryV1(record=record, ownership=ownership, physical=physical))
        value = _decode(raw)
        current = now()
        if current.tzinfo is None or current.utcoffset() is None or value.record.expires_at <= current:
            raise BootstrapDeliveryError("native bootstrap delivery expired")
        return raw
    except (ValueError, RuntimeError, OSError, TypeError, AttributeError):
        raise BootstrapDeliveryError("native bootstrap export is unavailable or invalid") from None


def native_delivery_directory(base: Path, reference: str) -> Path:
    """Resolve an atomically published node directory, never a packet pathname."""
    _record_path(base, reference)  # Strict existing reference grammar.
    return base / f"delivery-{reference[:-5]}"


def read_native_delivery_receipt(directory: Path, reference: str) -> NativeBootstrapDeliveryReceiptV1:
    try:
        _record_path(directory, reference)
        _private_directory(directory)
        raw = _open_private_regular(directory / _RECEIPT)
        receipt = parse_native_delivery_receipt(raw)
        if receipt.reference != reference:
            raise ValueError
        return receipt
    except (ValueError, RuntimeError, OSError, TypeError, AttributeError):
        raise BootstrapDeliveryError("native bootstrap delivery receipt is unavailable or invalid") from None


async def wait_native_bootstrap_delivery(base: Path, reference: str, *, now: Callable[[], datetime],
    timeout_seconds: float = 30) -> Path:
    """Bound the post-submission race; a partial directory is an error, not readiness."""
    if (type(timeout_seconds) not in {int, float} or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 120):
        raise BootstrapDeliveryError("native bootstrap delivery wait bound is invalid")
    try:
        _private_directory(base)
        directory = native_delivery_directory(base, reference)
        async with asyncio.timeout(timeout_seconds):
            while True:
                try:
                    directory.lstat()
                except FileNotFoundError:
                    await asyncio.sleep(0.05)
                    continue
                receipt = read_native_delivery_receipt(directory, reference)
                current = now()
                if current.tzinfo is None or current.utcoffset() is None or current >= receipt.expires_at:
                    raise BootstrapDeliveryError("native bootstrap delivery expired before worker launch")
                return directory
    except (TimeoutError, RuntimeError, OSError):
        raise BootstrapDeliveryError("native bootstrap delivery did not become available") from None


def _publish_directory(staging: Path, final: Path) -> None:
    """Publish a whole private directory without replacing even an empty target."""
    libc = ctypes.CDLL(None, use_errno=True)
    try:
        rename = libc.renameat2
    except AttributeError:
        raise BootstrapDeliveryError("atomic no-replace directory publication is unavailable") from None
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    # Linux AT_FDCWD and RENAME_NOREPLACE through the standard libc entrypoint,
    # not an architecture-dependent raw syscall or rename/check sequence.
    if rename(-100, os.fsencode(staging), -100, os.fsencode(final), 1) != 0:
        raise OSError(ctypes.get_errno(), "native bootstrap directory publication failed")


class NativeBootstrapReceiver:
    """A configured node receiver; transport input cannot select scope or paths."""

    def __init__(self, *, directory: Path, target_node: str, pool_id: str,
        trusted_release_sha256: str, admission: _Admission, now: Callable[[], datetime]) -> None:
        _private_directory(directory)
        self.directory, self.target_node, self.pool_id = directory, target_node, pool_id
        self.trusted_release_sha256, self.admission, self.now = trusted_release_sha256, admission, now
        info = directory.lstat()
        self._identity = info.st_dev, info.st_ino

    def _assert_directory(self) -> None:
        _private_directory(self.directory)
        info = self.directory.lstat()
        if (info.st_dev, info.st_ino) != self._identity or self.directory.resolve(strict=True) != self.directory:
            raise BootstrapDeliveryError("native bootstrap destination changed")

    def _retained(self, directory: Path, receipt: NativeBootstrapDeliveryReceiptV1) -> NativeBootstrapDeliveryReceiptV1:
        self._assert_directory()
        _private_directory(directory)
        raw = _open_private_regular(directory / _RECEIPT)
        if raw != _canonical(receipt):
            raise BootstrapDeliveryError("native bootstrap delivery conflicts with retained identity")
        _fsync_directory(directory)
        _fsync_directory(self.directory)
        self._assert_directory()
        # Never copy capability files back: the node may already have consumed
        # them, even when the controller lost the original delivery response.
        return receipt

    async def receive(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1:
        try:
            async with asyncio.timeout(10):
                return await self._receive(raw)
        except (ValueError, RuntimeError, OSError, TypeError, AttributeError):
            raise BootstrapDeliveryError("native bootstrap delivery refused") from None

    async def observe_receipt(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1 | None:
        """Read historical delivery only; absence is unknown, never safe cleanup.

        This does not reread unused-bootstrap admission: a historical receipt
        remains useful after registration/expiry. It neither renews validity nor
        reconstructs deleted files. The transport authenticates the caller.
        """
        try:
            self._assert_directory()
            query = parse_native_delivery_query(raw)
            binding = query.physical.binding
            if (binding.node_ids != (self.target_node,) or binding.pool_id != self.pool_id
                or binding.execution.trusted_fleet_release_sha256 != self.trusted_release_sha256):
                raise BootstrapDeliveryError("native bootstrap status scope differs")
            directory = native_delivery_directory(self.directory, query.expected.reference)
            try:
                directory.lstat()
            except FileNotFoundError:
                return None
            return self._retained(directory, query.expected)
        except (ValueError, RuntimeError, OSError, TypeError, AttributeError):
            raise BootstrapDeliveryError("native bootstrap status refused") from None

    async def _receive(self, raw: bytes) -> NativeBootstrapDeliveryReceiptV1:
        self._assert_directory()
        value = _decode(raw)
        record, physical = value.record, value.physical
        binding = physical.binding
        if (binding.node_ids != (self.target_node,) or binding.pool_id != self.pool_id
            or record.trusted_launcher_release_sha256 != self.trusted_release_sha256
            or record.protected_admission_route_sha256 != _route_sha256(self.admission, binding)):
            raise BootstrapDeliveryError("native bootstrap receiver scope differs")
        reference = _reference(binding)
        receipt = expected_native_delivery_receipt(raw)
        final = native_delivery_directory(self.directory, reference)
        if final.exists() or final.is_symlink():
            return self._retained(final, receipt)
        initial = self.now()
        if initial.tzinfo is None or initial.utcoffset() is None or initial >= record.expires_at:
            raise BootstrapDeliveryError("native bootstrap delivery expired")
        observed = await self.admission.observe_current_bootstrap(physical)
        observed = CurrentExecutableBootstrapV2.model_validate_json(observed.model_dump_json())
        current = self.now()
        if (observed.physical_binding != physical or observed.bootstrap_sha256 != record.capability_sha256
            or observed.bootstrap_expires_at != record.expires_at
            or not initial <= current < min(record.expires_at, observed.observed_at + timedelta(seconds=10))
            or observed.observed_at > current):
            raise BootstrapDeliveryError("native bootstrap current authority differs or expired")
        self._assert_directory()
        with tempfile.TemporaryDirectory(prefix=".native-delivery-", dir=self.directory) as staging_name:
            staging = Path(staging_name)
            _private_directory(staging)
            for name, payload in (
                (reference, canonical_executable_bytes(record)),
                (Path(reference).with_suffix(".ownership").name, canonical_executable_bytes(value.ownership)),
                (_RECEIPT, _canonical(receipt)),
            ):
                if not _publish_private_new(staging / name, payload):
                    raise BootstrapDeliveryError("native bootstrap staging unexpectedly exists")
            self._assert_directory()
            if not current <= self.now() < min(record.expires_at, observed.observed_at + timedelta(seconds=10)):
                raise BootstrapDeliveryError("native bootstrap expired before publication")
            try:
                _publish_directory(staging, final)
            except OSError as exc:
                if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                return self._retained(final, receipt)
            _fsync_directory(self.directory)
        return self._retained(final, receipt)
