"""Connect the trusted native process to its allocated, verified build source.

Source staging is not a build-start permit or a sandbox implementation. The
trusted runtime must separately fence execution and contain every build helper.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from loom_capacity_agent.admission import ExecutableWorkerRegistrationV2
from loom_capacity_agent.build_admission import (
    BuildAllocatedClaimRequestV1,
    BuildClaimReceiptV1,
    BuildClaimRequestV1,
)
from loom_capacity_executor.build_admission_client import BuildAdmissionTransportError
from loom_capacity_executor.native_allocated_io import NativeAllocatedIO, scoped_native_allocated_io
from loom_capacity_executor.native_build_source import (
    NativeClaimBuildSource,
    NativeStagedBuildSource,
)
from loom_capacity_executor.native_worker_handoff import consume_native_worker_handoff
from loom_capacity_executor.typed_admission import TypedAdmissionRouter
from loom_capacity_manager.contracts import canonical_digest
from loom_capacity_manager.executable_contracts import canonical_executable_digest
from loom_control_plane.slurm_job_cgroup import _slurm_job_scope, _unified_cgroup_path


@dataclass(frozen=True, slots=True)
class NativeAllocatedSource:
    claim: BuildClaimRequestV1
    source: NativeStagedBuildSource


def allocated_claim_request(worker: ExecutableWorkerRegistrationV2) -> BuildAllocatedClaimRequestV1:
    """A lost reply cannot turn one registered launch into another claim."""
    worker = ExecutableWorkerRegistrationV2.model_validate_json(worker.model_dump_json())
    operation_id = uuid5(NAMESPACE_URL,
        f"loom:allocated-native-claim:v1:{canonical_executable_digest(worker)}")
    return BuildAllocatedClaimRequestV1(binding=worker.binding, operation_id=operation_id,
        worker_id=worker.worker_id, worker_incarnation=worker.worker_incarnation)


@asynccontextmanager
async def allocated_worker_io(
    descriptor: int, *, job_id: str, workspace: Path, max_archive_bytes: int,
    admission_factory: Callable[..., TypedAdmissionRouter] = TypedAdmissionRouter,
) -> AsyncIterator[NativeAllocatedIO]:
    """Own the exec handoff and stage only management-assigned source.

The descriptor is consumed before configuration, cgroup, or network operations.
An exact Slurm process scope is required even for source intake; this does not
certify child-process containment, runtime installation or fresh start authority.
Retries are bounded and retain the same registered-worker operation identity.
Terminal/lost workers remain the management recovery loop's responsibility.
"""
    packet = consume_native_worker_handoff(descriptor)
    if job_id != packet.physical.slurm_job_id:
        raise ValueError("native worker Slurm job identity changed")
    process = _unified_cgroup_path(Path("/proc/self/cgroup"))
    scope = _slurm_job_scope(process, job_id)
    if scope not in process.parents:
        raise ValueError("native worker is not below its Slurm job scope")
    router = admission_factory(Path(packet.admission.path),
        expected_sha256=packet.admission.sha256, executor=packet.executor)
    if router.purpose(packet.physical.binding) != "personal-build-worker":
        raise ValueError("native worker requires a build-purpose route")
    source = NativeClaimBuildSource(client=router, workspace=workspace, max_archive_bytes=max_archive_bytes)
    request = allocated_claim_request(packet.registration)
    for attempt in range(3):
        try:
            receipt = await router.claim_assigned_platform(request, worker_credential=packet.worker_credential)
            break
        except BuildAdmissionTransportError:
            if attempt == 2:
                raise BuildAdmissionTransportError("native worker assigned claim is unavailable") from None
            await asyncio.sleep(0.2 * (attempt + 1))
    receipt = BuildClaimReceiptV1.model_validate_json(receipt.model_dump_json())
    if (receipt.request.model_dump(exclude={"request_id"}) != request.model_dump()
        or receipt.request_digest != canonical_digest(receipt.request)):
        raise ValueError("native worker assigned claim identity changed")
    async with source.stage_claim(receipt.request, worker_credential=packet.worker_credential) as staged:
        async with scoped_native_allocated_io(claim=receipt.request, source=staged,
            client=router, worker_credential=packet.worker_credential) as owner:
            yield owner


@asynccontextmanager
async def stage_allocated_worker_source(
    descriptor: int, *, job_id: str, workspace: Path, max_archive_bytes: int,
    admission_factory: Callable[..., TypedAdmissionRouter] = TypedAdmissionRouter,
) -> AsyncIterator[NativeAllocatedSource]:
    """Source-only compatibility view; authenticated IO remains scoped inside."""
    async with allocated_worker_io(descriptor, job_id=job_id, workspace=workspace,
        max_archive_bytes=max_archive_bytes, admission_factory=admission_factory) as owner:
        yield NativeAllocatedSource(claim=owner.claim, source=owner.source)
