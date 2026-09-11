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
import sys
import time
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

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
from loom_capacity_executor.runtime import RoutedExecutableAdmissionClient
from loom_capacity_executor.slurm_contracts import SlurmFileIdentityV2
from loom_capacity_executor.trusted_launcher import _read_verified_file
from loom_capacity_manager.contracts import Digest, Identifier


class NativeBootstrapReceiverConfigV1(BaseModel):
    """Immutable local operator configuration, never a transport request."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_name: Literal["loom.native-bootstrap-receiver-config/v1"] = Field(default="loom.native-bootstrap-receiver-config/v1", alias="schema")
    directory: Annotated[str, Field(max_length=4096)]
    target_node: Identifier
    pool_id: Identifier
    trusted_release_sha256: Digest
    admission_directory: Annotated[str, Field(max_length=4096)]
    admission_directory_sha256: Digest

    @field_validator("directory", "admission_directory")
    @classmethod
    def _canonical_directory(cls, value: str) -> str:
        path = Path(value)
        if (not path.is_absolute() or path == Path("/") or str(path) != value
            or ".." in path.parts or "\0" in value or value.startswith("//")):
            raise ValueError("native receiver configuration directory is invalid")
        return value


def _load_fixed_receiver(identity: SlurmFileIdentityV2) -> NativeBootstrapReceiver:
    with _protected_destination(Path(identity.path).parent):
        raw = _read_verified_file(identity, label="receiver configuration", executable=False)
    config = NativeBootstrapReceiverConfigV1.model_validate_json(raw)
    if raw != _canonical(config):
        raise BootstrapDeliveryError("native receiver configuration is not canonical")
    with _protected_destination(Path(config.admission_directory)):
        admission = RoutedExecutableAdmissionClient(Path(config.admission_directory),
            expected_directory_sha256=config.admission_directory_sha256)
    return NativeBootstrapReceiver(directory=Path(config.directory), target_node=config.target_node,
        pool_id=config.pool_id, trusted_release_sha256=config.trusted_release_sha256,
        admission=admission, now=lambda: datetime.now(UTC))


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


def run_native_bootstrap_receiver_process(factory: Callable[[], NativeBootstrapReceiver],
    *, operation: Literal["deliver", "status"] = "deliver") -> int:
    """Harden before configuration/secret access, detach input before admission.

    Only the configured transport supervisor may choose the factory. It must
    load protected local configuration and authenticated admission credentials.
    No CLI or environment fallback selects a factory or supplies capability bytes.
    """
    try:
        try:
            _disable_bootstrap_dumps()
            if operation not in {"deliver", "status"}:
                raise BootstrapDeliveryError("native receiver operation is invalid")
            receiver = factory()
            with _protected_destination(receiver.directory):
                try:
                    raw = _read_delivery_stdin()
                finally:
                    _detach_bootstrap_stdin()
                receipt = (asyncio.run(receiver.observe_receipt(raw)) if operation == "status"
                    else asyncio.run(receiver.receive(raw)))
            if receipt is None and operation != "status":
                raise BootstrapDeliveryError("native receiver did not confirm delivery")
            _write_receipt_stdout(_canonical(receipt) + b"\n" if receipt is not None else b"null\n")
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


def main(argv: Sequence[str] | None = None) -> int:
    """Fixed module entrypoint for the verified local process adapter.

    Flags identify an operator-pinned configuration and one of two operations.
    No executable, factory import, environment override or command suffix is
    accepted. Validation occurs inside the hardened process error boundary.
    """
    arguments = tuple(sys.argv[1:] if argv is None else argv)

    def factory() -> NativeBootstrapReceiver:
        if (len(arguments) != 8 or arguments[::2] != ("--configuration",
            "--configuration-sha256", "--configuration-owner-uid", "--operation")
            or arguments[7] not in {"deliver", "status"}):
            raise BootstrapDeliveryError("native receiver arguments are invalid")
        owner = int(arguments[5])
        if str(owner) != arguments[5]:
            raise BootstrapDeliveryError("native receiver configuration owner is invalid")
        return _load_fixed_receiver(SlurmFileIdentityV2(path=arguments[1], sha256=arguments[3], owner_uid=owner))

    operation: Literal["deliver", "status"] = "status" if len(arguments) == 8 and arguments[7] == "status" else "deliver"
    return run_native_bootstrap_receiver_process(factory, operation=operation)


if __name__ == "__main__":
    raise SystemExit(main())
