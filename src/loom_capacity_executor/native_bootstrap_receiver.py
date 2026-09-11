"""Dedicated one-shot process boundary for confidential bootstrap delivery.

The authenticated transport supervisor supplies a fixed installed factory, not
an import name, command, node, path or observation from request bytes. This entry
boundary is not a network listener, an installer, or authority to launch a worker.
Run only in a dedicated process: dump limits and stdin replacement are permanent.
"""

from __future__ import annotations

import asyncio
import math
import os
import select
import stat
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

from loom_capacity_executor.native_bootstrap_delivery import (
    _MAX_DELIVERY_BYTES,
    BootstrapDeliveryError,
    NativeBootstrapReceiver,
    _canonical,
)
from loom_capacity_executor.native_worker_bootstrap import (
    _detach_bootstrap_stdin,
    _disable_bootstrap_dumps,
)


@contextmanager
def _protected_destination(directory: Path) -> Iterator[None]:
    """Keep a no-symlink walk open through root/receiver-owned private ancestry.

    Shared writable ancestors (including sticky /tmp) are not an installation
    namespace. Same-UID code and the protected installer remain trusted; this is
    not a sandbox against either authority changing its own directories.
    """
    if (not directory.is_absolute() or directory == Path("/")
        or ".." in directory.parts or str(directory).startswith("//")):
        raise BootstrapDeliveryError("native receiver destination is invalid")
    descriptors: list[int] = []
    try:
        for component in ("/", *directory.parts[1:]):
            descriptor = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=descriptors[-1] if descriptors else None)
            descriptors.append(descriptor)
            info = os.fstat(descriptor)
            if (not stat.S_ISDIR(info.st_mode) or info.st_uid not in {0, os.geteuid()}
                or stat.S_IMODE(info.st_mode) & 0o022):
                raise BootstrapDeliveryError("native receiver destination ancestry is unprotected")
        yield
    finally:
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _read_delivery_stdin(*, timeout_seconds: float = 5) -> bytes:
    """Read canonical document bytes only from a bounded EOF-terminated pipe."""
    if (type(timeout_seconds) not in {int, float} or not math.isfinite(timeout_seconds)
        or not 0 < timeout_seconds <= 30 or not stat.S_ISFIFO(os.fstat(0).st_mode)):
        raise BootstrapDeliveryError("native receiver input is invalid")
    os.set_blocking(0, False)
    deadline = time.monotonic() + timeout_seconds
    wire = bytearray()
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([0], [], [], remaining)[0]:
            raise BootstrapDeliveryError("native receiver input expired")
        try:
            chunk = os.read(0, _MAX_DELIVERY_BYTES + 1 - len(wire))
        except BlockingIOError:
            continue
        if not chunk:
            if not wire:
                raise BootstrapDeliveryError("native receiver input is empty")
            return bytes(wire)
        wire.extend(chunk)
        if len(wire) > _MAX_DELIVERY_BYTES:
            raise BootstrapDeliveryError("native receiver input exceeds bound")


def _write_receipt_stdout(raw: bytes) -> None:
    # A closed or stalled supervisor makes delivery outcome unknown. The next
    # exact request recovers the durable receipt, never recreates capability.
    if len(raw) > 4096 or not stat.S_ISFIFO(os.fstat(1).st_mode):
        raise BootstrapDeliveryError("native receiver output is invalid")
    os.set_blocking(1, False)
    deadline = time.monotonic() + 5
    offset = 0
    while offset < len(raw):
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([], [1], [], remaining)[1]:
            raise BootstrapDeliveryError("native receiver output expired")
        try:
            count = os.write(1, raw[offset:])
        except BlockingIOError:
            continue
        if count <= 0:
            raise BootstrapDeliveryError("native receiver output closed")
        offset += count


def run_native_bootstrap_receiver_process(factory: Callable[[], NativeBootstrapReceiver]) -> int:
    """Harden before configuration/secret access, detach input before admission.

    Only the configured transport supervisor may choose the factory. It must
    load protected local configuration and authenticated admission credentials.
    No CLI or environment fallback selects a factory or supplies capability bytes.
    """
    try:
        try:
            _disable_bootstrap_dumps()
            receiver = factory()
            with _protected_destination(receiver.directory):
                try:
                    raw = _read_delivery_stdin()
                finally:
                    _detach_bootstrap_stdin()
                receipt = asyncio.run(receiver.receive(raw))
            _write_receipt_stdout(_canonical(receipt) + b"\n")
        finally:
            _detach_bootstrap_stdin()
        return 0
    except BaseException:
        # An adapter may embed credentials in arbitrary exceptions. Never print
        # those exceptions or traceback locals from this credential boundary.
        # This is the dedicated process's terminal handler, not a library retry:
        # cancellation, SystemExit and custom control exceptions all terminate
        # with the same nonzero result. Library cancellation remains untouched.
        try:
            os.set_blocking(2, False)
            os.write(2, b"native bootstrap receiver refused\n")
        except OSError:
            pass
        return 2
