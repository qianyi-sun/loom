"""Protected native build preparation/physical retention, never worker exchange."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import (
    BoundExecutableWorkerV2,
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
    PreparedExecutableAdmissionV2,
    ProtectedIntentObservationV2,
    RevokedExecutableBootstrapV2,
    WithdrawnExecutableWorkerV2,
)
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
    _identity,
)
from loom_capacity_manager.contracts import canonical_bytes
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)


@dataclass(frozen=True, slots=True)
class _ExecutionScope:
    id: UUID
    subject_id: UUID
    subject_incarnation: UUID


class BuildGuardExecutionStore:
    """Call private management procedures inside a caller-owned transaction.

    These receipts are compatible with the executor journal, but do not wire its
    authentication route, grant a source/worker credential, or enable readiness.
    """

    def __init__(self, session: AsyncSession, *, installation: RetainedBuildInstallation | None = None,
        binding: ExecutableIntentBindingV2 | None = None,
    ) -> None:
        if (installation is None) == (binding is None):
            raise ValueError("build execution requires exactly one scope source")
        if installation is not None:
            document = BuildGuardInstallationV1.model_validate_json(installation.wire_payload)
            if document != installation.document or canonical_bytes(document) != installation.wire_payload:
                raise ValueError("build execution installation receipt changed")
            scope = _ExecutionScope(document.id, document.subject_id, document.subject_incarnation)
        else:
            assert binding is not None
            binding = ExecutableIntentBindingV2.model_validate_json(binding.model_dump_json())
            # Derive the lookup identity, never trust a caller-selected installation.
            # Private SQL still resolves and validates the real installed native facts.
            scope = _ExecutionScope(_identity(binding.subject_id,binding.subject_incarnation,binding.deployment_generation),
                binding.subject_id,binding.subject_incarnation)
        self._session = session
        self._installation = scope

    async def prepare_worker(self, request: ExecutableBootstrapRegistrationV2, *,
        bootstrap_sha256: str,
    ) -> PreparedExecutableAdmissionV2:
        if not self._session.in_transaction():
            raise ValueError("build worker preparation requires an outer transaction")
        request = ExecutableBootstrapRegistrationV2.model_validate_json(request.model_dump_json())
        wire = canonical_executable_bytes(request)
        digest = canonical_executable_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.prepare_worker(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:bootstrap)"""),
                {"installation":self._installation.id,"payload":wire.decode("ascii"),"wire":wire,
                    "digest":digest,"bootstrap":bootstrap_sha256})
            receipt = PreparedExecutableAdmissionV2.model_validate_json(returned)
            if (canonical_executable_bytes(receipt).decode("ascii") != returned
                or receipt.subject_id != self._installation.subject_id
                or receipt.subject_incarnation != self._installation.subject_incarnation
                or receipt.intent_id != request.binding.intent_id
                or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
                or receipt.bootstrap_sha256 != bootstrap_sha256
                or receipt.request_digest != digest or receipt.admission_digest != digest):
                raise ValueError("build worker preparation receipt changed")
            return receipt

    async def observe_intent(self, binding: ExecutableIntentBindingV2) -> ProtectedIntentObservationV2:
        """Recover exact retained preparation, never a claim or release permit."""
        if not self._session.in_transaction():
            raise ValueError("build observation requires an outer transaction")
        binding = ExecutableIntentBindingV2.model_validate_json(binding.model_dump_json())
        if (binding.subject_id != self._installation.subject_id
            or binding.subject_incarnation != self._installation.subject_incarnation):
            raise ValueError("build observation subject binding changed")
        wire = canonical_executable_bytes(binding)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.observe_intent(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation":self._installation.id,"payload":wire.decode("ascii"),"wire":wire,
                    "digest":canonical_executable_digest(binding)})
            receipt = ProtectedIntentObservationV2.model_validate_json(returned)
            if canonical_executable_bytes(receipt).decode("ascii") != returned or receipt.binding != binding:
                raise ValueError("build observation receipt changed")
            return receipt

    async def revoke_prepared_bootstrap(self, request: ExecutablePreparedBootstrapRevocationV2) -> RevokedExecutableBootstrapV2:
        """Fence an exact unbound bootstrap; never remove its capacity hold."""
        if not self._session.in_transaction():
            raise ValueError("build bootstrap revocation requires an outer transaction")
        request = ExecutablePreparedBootstrapRevocationV2.model_validate_json(request.model_dump_json())
        wire = canonical_executable_bytes(request)
        digest = canonical_executable_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.revoke_prepared_bootstrap(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation":self._installation.id,"payload":wire.decode("ascii"),"wire":wire,"digest":digest})
            receipt = RevokedExecutableBootstrapV2.model_validate_json(returned)
            if (canonical_executable_bytes(receipt).decode("ascii") != returned or receipt.binding != request.binding
                or receipt.binding.subject_id != self._installation.subject_id
                or receipt.binding.subject_incarnation != self._installation.subject_incarnation
                or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
                or receipt.protected_registration_epoch != request.protected_registration_epoch
                or receipt.request_digest != digest or receipt.protected_release_sha256 != digest):
                raise ValueError("build bootstrap revocation receipt changed")
            return receipt

    async def withdraw_unregistered_worker(self, request: ExecutableWorkerWithdrawalRequestV2) -> WithdrawnExecutableWorkerV2:
        if not self._session.in_transaction():
            raise ValueError("build withdrawal requires an outer transaction")
        request = ExecutableWorkerWithdrawalRequestV2.model_validate_json(request.model_dump_json())
        wire = canonical_executable_bytes(request)
        digest = canonical_executable_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.withdraw_unregistered_worker(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation":self._installation.id,"payload":wire.decode("ascii"),"wire":wire,"digest":digest})
            receipt = WithdrawnExecutableWorkerV2.model_validate_json(returned)
            if (canonical_executable_bytes(receipt).decode("ascii") != returned
                or receipt.subject_id != self._installation.subject_id
                or receipt.subject_incarnation != self._installation.subject_incarnation
                or receipt.intent_id != request.binding.intent_id
                or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
                or receipt.protected_registration_epoch != request.protected_registration_epoch
                or receipt.slurm_job_id != request.slurm_job_id
                or receipt.ownership_evidence_sha256 != request.ownership_evidence_sha256
                or receipt.request_digest != digest or receipt.withdrawal_digest != digest):
                raise ValueError("build withdrawal receipt changed")
            return receipt

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> BoundExecutableWorkerV2:
        if not self._session.in_transaction():
            raise ValueError("build physical binding requires an outer transaction")
        request = PhysicalJobBindingV2.model_validate_json(request.model_dump_json())
        wire = canonical_executable_bytes(request)
        digest = canonical_executable_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.bind_slurm_job(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation":self._installation.id,"payload":wire.decode("ascii"),"wire":wire,"digest":digest})
            receipt = BoundExecutableWorkerV2.model_validate_json(returned)
            if (canonical_executable_bytes(receipt).decode("ascii") != returned
                or receipt.subject_id != self._installation.subject_id
                or receipt.subject_incarnation != self._installation.subject_incarnation
                or receipt.intent_id != request.binding.intent_id
                or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
                or receipt.slurm_job_id != request.slurm_job_id
                or receipt.ownership_evidence_sha256 != request.ownership_evidence_sha256
                or receipt.request_digest != digest or receipt.binding_digest != digest):
                raise ValueError("build physical binding receipt changed")
            return receipt
