"""Pinned purpose-aware admission routes; typed execution remains interlocked.

The activation root supplies purpose, independently of expiring launch permits.
Each backend still verifies the exact protected installation and intent. Missing
native lifecycle consumers never fall back to application database authority.
"""

from __future__ import annotations

import hmac
from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from typing import Annotated, Any, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from loom_capacity_agent.admission import (
    ExecutableDrainRequestV2,
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
)
from loom_capacity_agent.build_admission import (
    BuildAllocatedClaimRequestV1,
    BuildClaimReceiptV1,
    BuildClaimRequestV1,
    BuildOutcomeRequestV1,
    BuildSourceContextV1,
    BuildSourceReadReceiptV1,
)
from loom_capacity_agent.claim_guard import ExecutableClaimProposalV2
from loom_capacity_agent.client import read_owner_only_bytes
from loom_capacity_executor.admission_client import DatabaseExecutableAdmissionClient
from loom_capacity_executor.build_admission_client import (
    BuildAdmissionClient,
    BuildAdmissionExecutorV1,
)
from loom_capacity_executor.launch_policy_set import LaunchPurpose, _StrictLaunchV3
from loom_capacity_executor.pinned_admission_transport import (
    PinnedAdmissionFileV1,
    PinnedBuildAdmissionConnectionV1,
    _read_pinned,
)
from loom_capacity_manager.contracts import (
    MAX_CONTRACT_BYTES,
    MAX_SUBJECTS,
    Digest,
    Identifier,
    PositiveQuantity,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)


class TypedAdmissionEntryV3(_StrictLaunchV3):
    subject_id: UUID
    subject_incarnation: UUID
    configuration_epoch: PositiveQuantity
    deployment_generation: PositiveQuantity
    candidate_generation: PositiveQuantity
    candidate_sha256: Digest
    account_id: Identifier
    purpose: LaunchPurpose
    protected_admission_sha256: Digest
    database: PinnedAdmissionFileV1 | None = None
    build: PinnedBuildAdmissionConnectionV1 | None = None

    @model_validator(mode="after")
    def _one_route(self) -> Self:
        if self.purpose == "application-worker":
            valid = self.database is not None and self.build is None
        else:
            valid = self.build is not None and self.database is None
        if not valid:
            raise ValueError("typed admission requires exactly its purpose-specific route")
        return self


class TypedAdmissionDirectoryV3(_StrictLaunchV3):
    executor: BuildAdmissionExecutorV1
    entries: Annotated[tuple[TypedAdmissionEntryV3, ...], Field(max_length=MAX_SUBJECTS)]

    @field_validator("entries")
    @classmethod
    def _unique_sorted(cls, entries: tuple[TypedAdmissionEntryV3, ...]) -> tuple[TypedAdmissionEntryV3, ...]:
        keys = [(entry.subject_id, entry.subject_incarnation) for entry in entries]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate typed admission subject binding")
        return tuple(sorted(entries, key=lambda entry: (entry.subject_id.int, entry.subject_incarnation.int)))


_BUILD_CONSUMERS = frozenset({
    "prepare_worker", "bind_slurm_job", "observe_intent", "revoke_prepared_bootstrap",
    "withdraw_unregistered_worker", "register_worker", "claim_platform", "begin_drain", "record_outcome", "acknowledge_release",
    "read_source", "read_source_context", "claim_assigned_platform",
})


def load_typed_admission_directory(
    path: Path, *, expected_sha256: str, executor: BuildAdmissionExecutorV1,
) -> TypedAdmissionDirectoryV3:
    root = PinnedAdmissionFileV1(path=str(path), sha256=expected_sha256)
    identity = BuildAdmissionExecutorV1.model_validate_json(executor.model_dump_json())
    wire = read_owner_only_bytes(Path(root.path), max_bytes=MAX_CONTRACT_BYTES)
    if not hmac.compare_digest(sha256(wire).hexdigest(), root.sha256):
        raise ValueError("typed admission directory digest changed")
    document = TypedAdmissionDirectoryV3.model_validate_json(wire)
    if canonical_executable_bytes(document) != wire:
        raise ValueError("typed admission directory is not canonical")
    if document.executor != identity:
        raise ValueError("typed admission executor binding changed")
    return document


class TypedAdmissionRouter:
    """Verify the immutable directory on every operation and dispose its client."""

    def __init__(
        self, path: Path, *, expected_sha256: str, executor: BuildAdmissionExecutorV1,
        application_client_factory: Callable[..., Any] = DatabaseExecutableAdmissionClient.from_database_url_bytes,
        build_client_factory: Callable[..., Any] = BuildAdmissionClient.from_pinned_files,
    ) -> None:
        # Reuse the pinned-file contract for canonical path and digest validation.
        self._root = PinnedAdmissionFileV1(path=str(path), sha256=expected_sha256)
        self._executor = BuildAdmissionExecutorV1.model_validate_json(executor.model_dump_json())
        self._application_factory = application_client_factory
        self._build_factory = build_client_factory
        self._load_verified()

    def _load_verified(self) -> TypedAdmissionDirectoryV3:
        return load_typed_admission_directory(Path(self._root.path),
            expected_sha256=self._root.sha256, executor=self._executor)

    def _resolve(self, binding: ExecutableIntentBindingV2) -> TypedAdmissionEntryV3:
        if not isinstance(binding, ExecutableIntentBindingV2):
            raise ValueError("typed admission requires an executable binding")
        binding = ExecutableIntentBindingV2.model_validate_json(binding.model_dump_json())
        document = self._load_verified()
        identity = document.executor
        if (binding.pool_id != identity.pool_id or binding.pool_generation != identity.pool_generation
            or binding.executor_id != identity.executor_id or binding.executor_incarnation != identity.executor_incarnation):
            raise ValueError("typed admission operation executor binding changed")
        entry = next((entry for entry in document.entries
            if entry.subject_id == binding.subject_id and entry.subject_incarnation == binding.subject_incarnation), None)
        if entry is None:
            raise ValueError("typed admission subject binding is absent")
        if (entry.configuration_epoch != binding.execution.configuration_epoch
            or entry.deployment_generation != binding.deployment_generation
            or entry.candidate_generation != binding.candidate_generation
            or entry.candidate_sha256 != canonical_executable_digest(binding.candidate)
            or entry.account_id != binding.account_id):
            raise ValueError("typed admission generation or account binding changed")
        return entry

    def purpose(self, binding: ExecutableIntentBindingV2) -> LaunchPurpose:
        return self._resolve(binding).purpose

    def bootstrap_handoff_route_sha256(self, binding: ExecutableIntentBindingV2) -> str:
        return canonical_executable_digest(self._resolve(binding))

    async def _call(self, binding: ExecutableIntentBindingV2, method: str, *args: object, **kwargs: object) -> Any:
        entry = self._resolve(binding)
        if entry.purpose == "personal-build-worker":
            if method not in _BUILD_CONSUMERS:
                raise RuntimeError("native build admission lifecycle consumer is not implemented")
            assert entry.build is not None
            client = self._build_factory(self._executor, entry.build)
        else:
            assert entry.database is not None
            database_url = _read_pinned(entry.database, maximum=16 * 1024)
            client = self._application_factory(database_url,
                subject_id=entry.subject_id, subject_incarnation=entry.subject_incarnation)
        try:
            return await getattr(client, method)(*args, **kwargs)
        finally:
            await client.aclose()

    async def prepare_worker(self, request: ExecutableBootstrapRegistrationV2, *, bootstrap_sha256: str) -> Any:
        return await self._call(request.binding, "prepare_worker", request, bootstrap_sha256=bootstrap_sha256)

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> Any:
        return await self._call(request.binding, "bind_slurm_job", request)

    async def observe_intent(self, binding: ExecutableIntentBindingV2) -> Any:
        return await self._call(binding, "observe_intent", binding)

    async def revoke_prepared_bootstrap(self, request: ExecutablePreparedBootstrapRevocationV2) -> Any:
        return await self._call(request.binding, "revoke_prepared_bootstrap", request)

    async def begin_drain(self, request: ExecutableDrainRequestV2) -> Any:
        return await self._call(request.binding, "begin_drain", request)

    async def withdraw_unregistered_worker(self, request: ExecutableWorkerWithdrawalRequestV2) -> Any:
        return await self._call(request.binding, "withdraw_unregistered_worker", request)

    async def register_worker(self, request: ExecutableWorkerRegistrationV2, *, bootstrap_capability: str) -> Any:
        return await self._call(request.binding, "register_worker", request, bootstrap_capability=bootstrap_capability)

    async def acknowledge_release(self, request: ExecutableReleaseRequestV2, *, current_worker_credential: str) -> Any:
        return await self._call(request.binding, "acknowledge_release", request, current_worker_credential=current_worker_credential)

    async def admit_claim(self, binding: ExecutableIntentBindingV2, proposal: ExecutableClaimProposalV2) -> Any:
        return await self._call(binding, "admit_claim_for_intent", binding, proposal)

    async def claim_platform(self, request: BuildClaimRequestV1, *, worker_credential: str) -> Any:
        if self.purpose(request.binding) != "personal-build-worker":
            raise ValueError("native platform claim requires a build-purpose route")
        return await self._call(request.binding, "claim_platform", request, worker_credential=worker_credential)

    async def record_outcome(self, request: BuildOutcomeRequestV1, *, worker_credential: str) -> Any:
        if self.purpose(request.claim.binding) != "personal-build-worker":
            raise ValueError("native outcome requires a build-purpose route")
        return await self._call(request.claim.binding, "record_outcome", request, worker_credential=worker_credential)

    async def read_source(self, claim: BuildClaimRequestV1, *, worker_credential: str,
        offset: int, length: int,
    ) -> BuildSourceReadReceiptV1:
        if self.purpose(claim.binding) != "personal-build-worker":
            raise ValueError("native source requires a build-purpose route")
        receipt = await self._call(claim.binding, "read_source", claim,
            worker_credential=worker_credential, offset=offset, length=length)
        if not isinstance(receipt, BuildSourceReadReceiptV1):
            raise ValueError("native source route returned an invalid receipt")
        return receipt

    async def read_source_context(self, claim: BuildClaimRequestV1, *, worker_credential: str) -> BuildSourceContextV1:
        if self.purpose(claim.binding) != "personal-build-worker":
            raise ValueError("native context requires a build-purpose route")
        receipt = await self._call(claim.binding, "read_source_context", claim, worker_credential=worker_credential)
        if not isinstance(receipt, BuildSourceContextV1):
            raise ValueError("native context route returned an invalid receipt")
        return receipt

    async def claim_assigned_platform(self, request: BuildAllocatedClaimRequestV1, *, worker_credential: str) -> BuildClaimReceiptV1:
        if self.purpose(request.binding) != "personal-build-worker":
            raise ValueError("allocated native claim requires a build-purpose route")
        receipt = await self._call(request.binding, "claim_assigned_platform", request, worker_credential=worker_credential)
        if not isinstance(receipt, BuildClaimReceiptV1):
            raise ValueError("allocated native claim route returned an invalid receipt")
        return receipt
