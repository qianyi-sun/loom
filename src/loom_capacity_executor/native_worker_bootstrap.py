"""Bounded memory-only native worker handoff, never a claim/start authority.

The trusted launch adapter writes this once to attached non-TTY container stdin.
No environment/file fallback exists. Reading requires both an exact frame and
EOF before a short deadline, so a restart with open/empty stdin fails closed.
The native worker adapter must retain the credential in memory, disable core
dumps and dotenv loading, and close stdin before starting child processes.
"""

from __future__ import annotations

import fcntl
import json
import math
import os
import re
import select
import stat
import time
from dataclasses import dataclass, field

from loom_capacity_executor.launch_renderer import NativeTaskImageExecutionV2
from loom_task_image_authority.publication_contracts import _unique_object

_MAX_FRAME_BYTES = 4096
_HEADER_BYTES = 4
_SCHEMA = "loom.native-worker-bootstrap/v1"
_CREDENTIAL = re.compile(r"[A-Za-z0-9._~-]{43,512}", re.ASCII)
_FAILURE = "native worker bootstrap unavailable or malformed"


class NativeBootstrapError(ValueError):
    """Safe diagnostic that never contains the bootstrap payload or credential."""


@dataclass(frozen=True, slots=True)
class NativeWorkerBootstrap:
    native_execution: NativeTaskImageExecutionV2
    worker_credential: str = field(repr=False)

    def __post_init__(self) -> None:
        if (
            type(self.native_execution) is not NativeTaskImageExecutionV2
            or type(self.worker_credential) is not str
            or _CREDENTIAL.fullmatch(self.worker_credential) is None
        ):
            raise NativeBootstrapError(_FAILURE)


def encode_native_bootstrap(bootstrap: NativeWorkerBootstrap) -> bytes:
    """Encode one short frame; callers must not persist or log these bytes."""

    if type(bootstrap) is not NativeWorkerBootstrap:
        raise NativeBootstrapError(_FAILURE)
    try:
        checked = NativeWorkerBootstrap(
            native_execution=NativeTaskImageExecutionV2.model_validate(
                bootstrap.native_execution.model_dump(mode="json")
            ),
            worker_credential=bootstrap.worker_credential,
        )
        payload = json.dumps(
            {
                "schema": _SCHEMA,
                "native_execution": checked.native_execution.model_dump(mode="json"),
                "worker_credential": checked.worker_credential,
            },
            sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False,
        ).encode("ascii")
        if not 0 < len(payload) <= _MAX_FRAME_BYTES - _HEADER_BYTES:
            raise ValueError
        return len(payload).to_bytes(_HEADER_BYTES, "big") + payload
    except (ValueError, TypeError, OverflowError):
        raise NativeBootstrapError(_FAILURE) from None


def native_bootstrap_pipe(bootstrap: NativeWorkerBootstrap) -> int:
    """Return the non-inheritable read end of a fully written and closed pipe.

    Check actual capacity before preloading: writing a large frame before exec
    can otherwise deadlock without any reader. This private empty pipe has no
    competing writers, and the complete frame fits one atomic write.
    """

    wire = encode_native_bootstrap(bootstrap)
    reader, writer = os.pipe()
    try:
        capacity = fcntl.fcntl(writer, fcntl.F_GETPIPE_SZ)
        atomic_limit = os.fpathconf(writer, "PC_PIPE_BUF")
        if len(wire) > min(capacity, atomic_limit) or os.write(writer, wire) != len(wire):
            raise NativeBootstrapError(_FAILURE)
        return reader
    except (OSError, ValueError):
        os.close(reader)
        raise NativeBootstrapError(_FAILURE) from None
    finally:
        os.close(writer)


def read_native_bootstrap(descriptor: int, *, timeout_seconds: float = 5) -> NativeWorkerBootstrap:
    """Consume an exact EOF-terminated pipe frame within a bounded deadline.

    Descriptor ownership stays with the caller so the native entrypoint can
    explicitly rebind fd 0 to /dev/null before worker/subprocess startup.
    """

    try:
        if (
            type(timeout_seconds) not in {int, float}
            or not math.isfinite(timeout_seconds)
            or not 0 < timeout_seconds <= 30
            or not stat.S_ISFIFO(os.fstat(descriptor).st_mode)
        ):
            raise ValueError
        # Readiness can be invalidated by a second inherited reader. Never let
        # a subsequent blocking read escape the end-to-end deadline.
        os.set_blocking(descriptor, False)
        deadline = time.monotonic() + timeout_seconds
        wire = bytearray()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([descriptor], [], [], remaining)[0]:
                raise ValueError
            try:
                chunk = os.read(descriptor, _MAX_FRAME_BYTES + 1 - len(wire))
            except BlockingIOError:
                continue
            if not chunk:
                break
            wire.extend(chunk)
            if len(wire) > _MAX_FRAME_BYTES:
                raise ValueError
        if len(wire) <= _HEADER_BYTES or int.from_bytes(wire[:_HEADER_BYTES], "big") != len(wire) - _HEADER_BYTES:
            raise ValueError
        payload = json.loads(wire[_HEADER_BYTES:].decode("ascii"), object_pairs_hook=_unique_object)
        if (
            type(payload) is not dict
            or set(payload) != {"schema", "native_execution", "worker_credential"}
            or payload["schema"] != _SCHEMA
        ):
            raise ValueError
        bootstrap = NativeWorkerBootstrap(
            native_execution=NativeTaskImageExecutionV2.model_validate(payload["native_execution"]),
            worker_credential=payload["worker_credential"],
        )
        if encode_native_bootstrap(bootstrap) != wire:
            raise ValueError
        return bootstrap
    except (OSError, ValueError, TypeError, OverflowError, RecursionError):
        raise NativeBootstrapError(_FAILURE) from None
