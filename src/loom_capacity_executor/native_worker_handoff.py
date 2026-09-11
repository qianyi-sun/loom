"""Sealed allocation/credential handoff to the trusted native worker executable."""

from __future__ import annotations

import fcntl
import hmac
import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from hashlib import sha256
from typing import Self

from pydantic import Field, model_validator

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2, PhysicalJobBindingV2
from loom_capacity_executor.build_admission_client import BuildAdmissionExecutorV1
from loom_capacity_executor.pinned_admission_transport import PinnedAdmissionFileV1
from loom_capacity_manager.contracts import StrictV1Model, canonical_bytes

NATIVE_WORKER_HANDOFF_ENV = "LOOM_NATIVE_WORKER_HANDOFF_FD"
_MAX_HANDOFF_BYTES = 64 * 1024
_REQUIRED_SEALS = 0x0001 | 0x0002 | 0x0004 | 0x0008
_GET_SEALS = getattr(fcntl, "F_GET_SEALS", 1034)


class NativeWorkerHandoffV1(StrictV1Model):
    registration: ExecutableWorkerRegistrationV2
    physical: PhysicalJobBindingV2
    executor: BuildAdmissionExecutorV1
    admission: PinnedAdmissionFileV1
    worker_credential: str = Field(min_length=43, max_length=512,
        pattern=r"^[A-Za-z0-9_-]+$", repr=False)

    @model_validator(mode="after")
    def _identity(self) -> Self:
        worker, physical, executor = self.registration, self.physical, self.executor
        binding = physical.binding
        if (worker.binding != binding or worker.slurm_job_id != physical.slurm_job_id
            or worker.bootstrap_registration_epoch != physical.bootstrap_registration_epoch
            or worker.bootstrap_registration_epoch != 1 or worker.protected_registration_epoch != 2
            or worker.predecessor_worker_incarnation is not None
            or binding.pool_id not in {"gb10", "oldlab"}
            or executor.pool_id != binding.pool_id or executor.pool_generation != binding.pool_generation
            or executor.executor_id != binding.executor_id or executor.executor_incarnation != binding.executor_incarnation
            or not hmac.compare_digest(worker.worker_credential_sha256, sha256(self.worker_credential.encode("ascii")).hexdigest())):
            raise ValueError("native worker handoff identity changed")
        return self


@contextmanager
def sealed_native_worker_handoff(packet: NativeWorkerHandoffV1) -> Iterator[int]:
    """Inherit only a bounded, immutable packet; close on a failed exec boundary."""
    from loom_capacity_executor.trusted_launcher import (
        _create_candidate_snapshot_descriptor,
        _seal_candidate_snapshot,
        _write_all,
    )

    try:
        packet = NativeWorkerHandoffV1.model_validate_json(packet.model_dump_json())
    except ValueError:
        raise ValueError("native worker handoff identity is invalid") from None
    wire = canonical_bytes(packet)
    if len(wire) > _MAX_HANDOFF_BYTES:
        raise ValueError("native worker handoff exceeds byte bound")
    descriptor = _create_candidate_snapshot_descriptor()
    try:
        os.fchmod(descriptor, 0o600)
        _write_all(descriptor, wire)
        _seal_candidate_snapshot(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        os.set_inheritable(descriptor, True)
        yield descriptor
    finally:
        os.close(descriptor)


def consume_native_worker_handoff(descriptor: int) -> NativeWorkerHandoffV1:
    """Own and close the inherited descriptor before any sandbox child starts."""
    if type(descriptor) is not int or descriptor < 3:
        raise ValueError("native worker handoff descriptor is invalid")
    try:
        os.set_inheritable(descriptor, False)
        metadata = os.fstat(descriptor)
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600 or not 1 <= metadata.st_size <= _MAX_HANDOFF_BYTES
            or int(fcntl.fcntl(descriptor, _GET_SEALS)) & _REQUIRED_SEALS != _REQUIRED_SEALS):
            raise ValueError("native worker handoff is not a bounded sealed owner file")
        wire = os.pread(descriptor, _MAX_HANDOFF_BYTES + 1, 0)
        try:
            packet = NativeWorkerHandoffV1.model_validate_json(wire)
        except ValueError:
            raise ValueError("native worker handoff identity is invalid") from None
        if len(wire) != metadata.st_size or canonical_bytes(packet) != wire:
            raise ValueError("native worker handoff canonical bytes changed")
        return packet
    finally:
        os.close(descriptor)
