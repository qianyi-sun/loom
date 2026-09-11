"""Protected native build preparation, physical binding and scoped registration."""

from __future__ import annotations

import re
from dataclasses import dataclass
from hashlib import sha256
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import (
    BoundExecutableWorkerV2,
    DrainedExecutableWorkerV2,
    ExecutableDrainRequestV2,
    ExecutablePreparedBootstrapRevocationV2,
    ExecutableReleaseReceiptV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    ExecutableWorkerWithdrawalRequestV2,
    PhysicalJobBindingV2,
    PreparedExecutableAdmissionV2,
    ProtectedIntentObservationV2,
    RegisteredExecutableWorkerV2,
    RevokedExecutableBootstrapV2,
    WithdrawnExecutableWorkerV2,
)
from loom_capacity_agent.build_admission import (
    BuildClaimReceiptV1,
    BuildClaimRequestV1,
    BuildOutcomeReceiptV1,
    BuildOutcomeRequestV1,
)
from loom_capacity_build_guard.installation_store import (
    BuildGuardInstallationV1,
    RetainedBuildInstallation,
    _identity,
)
from loom_capacity_build_guard.source_access import BuildClaimSourceV1
from loom_capacity_manager.contracts import canonical_bytes, canonical_digest
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


def native_release_receipt(returned: str, request: ExecutableReleaseRequestV2) -> ExecutableReleaseReceiptV2:
    receipt = ExecutableReleaseReceiptV2.model_validate_json(returned)
    digest = canonical_executable_digest(request)
    if (canonical_executable_bytes(receipt).decode("ascii") != returned or receipt.binding != request.binding
        or receipt.reporter_incarnation != request.reporter_incarnation
        or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
        or receipt.protected_registration_epoch != request.protected_registration_epoch
        or receipt.release_epoch != request.release_epoch or receipt.claim_high_water != request.expected_claim_high_water
        or receipt.request_digest != digest or receipt.protected_release_sha256 != digest):
        raise ValueError("native release receipt changed")
    return receipt


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

    async def register_worker(self, request: ExecutableWorkerRegistrationV2, *,
        bootstrap_capability: str,
    ) -> RegisteredExecutableWorkerV2:
        """Retain only a native credential hash; no app worker or source grant."""
        if not self._session.in_transaction():
            raise ValueError("build registration requires an outer transaction")
        request = ExecutableWorkerRegistrationV2.model_validate_json(request.model_dump_json())
        if (not isinstance(bootstrap_capability, str)
            or re.fullmatch(r"[A-Za-z0-9_-]{43,512}", bootstrap_capability) is None):
            raise ValueError("build registration bootstrap capability is invalid")
        if request.predecessor_worker_incarnation is not None or request.protected_registration_epoch != 2 or request.bootstrap_registration_epoch != 1:
            raise ValueError("build registration requires initial native epochs")
        bootstrap_hash = sha256(bootstrap_capability.encode("ascii")).hexdigest()
        wire, digest = canonical_executable_bytes(request), canonical_executable_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.register_worker(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:bootstrap)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": digest, "bootstrap": bootstrap_hash})
            receipt = RegisteredExecutableWorkerV2.model_validate_json(returned)
            if (canonical_executable_bytes(receipt).decode("ascii") != returned
                or receipt.subject_id != self._installation.subject_id or receipt.subject_incarnation != self._installation.subject_incarnation
                or receipt.intent_id != request.binding.intent_id or receipt.worker_id != request.worker_id
                or receipt.worker_incarnation != request.worker_incarnation or receipt.predecessor_worker_incarnation is not None
                or receipt.protected_registration_epoch != 2 or receipt.request_digest != digest or receipt.registration_digest != digest):
                raise ValueError("build registration receipt changed")
            return receipt

    async def begin_drain(self, request: ExecutableDrainRequestV2) -> DrainedExecutableWorkerV2:
        """Fence new native work, retaining live claims and all capacity charges."""
        if not self._session.in_transaction():
            raise ValueError("native drain requires an outer transaction")
        request = ExecutableDrainRequestV2.model_validate_json(request.model_dump_json())
        wire, digest = canonical_executable_bytes(request), canonical_executable_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.begin_drain(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire, "digest": digest})
            receipt = DrainedExecutableWorkerV2.model_validate_json(returned)
            if (canonical_executable_bytes(receipt).decode("ascii") != returned
                or receipt.subject_id != request.binding.subject_id or receipt.subject_incarnation != request.binding.subject_incarnation
                or receipt.intent_id != request.binding.intent_id or receipt.worker_id != request.worker_id
                or receipt.worker_incarnation != request.worker_incarnation or receipt.claim_high_water != request.expected_claim_high_water
                or receipt.live_claim_count > request.expected_claim_high_water or receipt.drain_epoch != request.drain_epoch
                or receipt.request_digest != digest or receipt.drain_digest != digest):
                raise ValueError("native drain receipt changed")
            return receipt

    async def record_outcome(self, request: BuildOutcomeRequestV1, *, worker_credential: str) -> BuildOutcomeReceiptV1:
        """Retain exact worker results, not candidate success or physical release."""
        if not self._session.in_transaction():
            raise ValueError("native outcome requires an outer transaction")
        request = BuildOutcomeRequestV1.model_validate_json(request.model_dump_json())
        if not isinstance(worker_credential, str) or re.fullmatch(r"[A-Za-z0-9_-]{43,512}", worker_credential) is None:
            raise ValueError("native outcome credential is invalid")
        wire, digest = canonical_bytes(request), canonical_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.record_outcome(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:credential)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": digest, "credential": sha256(worker_credential.encode("ascii")).hexdigest()})
            receipt = BuildOutcomeReceiptV1.model_validate_json(returned)
            if canonical_bytes(receipt).decode("ascii") != returned or receipt.request != request or receipt.request_digest != digest:
                raise ValueError("native outcome receipt changed")
            return receipt

    async def acknowledge_release(self, request: ExecutableReleaseRequestV2, *, current_worker_credential: str) -> ExecutableReleaseReceiptV2:
        if not self._session.in_transaction():
            raise ValueError("native release requires an outer transaction")
        request = ExecutableReleaseRequestV2.model_validate_json(request.model_dump_json())
        if not isinstance(current_worker_credential, str) or re.fullmatch(r"[A-Za-z0-9_-]{43,512}", current_worker_credential) is None:
            raise ValueError("native release credential is invalid")
        wire = canonical_executable_bytes(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.acknowledge_release(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:credential)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": canonical_executable_digest(request), "credential": sha256(current_worker_credential.encode("ascii")).hexdigest()})
            return native_release_receipt(returned, request)

    async def read_outcome(self, claim: BuildClaimRequestV1) -> BuildOutcomeReceiptV1 | None:
        if not self._session.in_transaction():
            raise ValueError("native outcome observation requires an outer transaction")
        claim = BuildClaimRequestV1.model_validate_json(claim.model_dump_json())
        wire = canonical_bytes(claim)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.read_outcome(
                :installation,CAST(:payload AS jsonb),:wire,:digest)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": canonical_digest(claim)})
            if returned is None:
                return None
            receipt = BuildOutcomeReceiptV1.model_validate_json(returned)
            if (canonical_bytes(receipt).decode("ascii") != returned or receipt.request.claim != claim
                or receipt.request_digest != canonical_digest(receipt.request)):
                raise ValueError("native outcome observation receipt changed")
            return receipt

    async def claim_platform(self, request: BuildClaimRequestV1, *, worker_credential: str) -> BuildClaimReceiptV1:
        """Claim the allocated native request, not arbitrary application work."""
        if not self._session.in_transaction():
            raise ValueError("native claim requires an outer transaction")
        request = BuildClaimRequestV1.model_validate_json(request.model_dump_json())
        if not isinstance(worker_credential, str) or re.fullmatch(r"[A-Za-z0-9_-]{43,512}", worker_credential) is None:
            raise ValueError("native claim credential is invalid")
        wire, digest = canonical_bytes(request), canonical_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.claim_platform(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:credential)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": digest, "credential": sha256(worker_credential.encode("ascii")).hexdigest()})
            receipt = BuildClaimReceiptV1.model_validate_json(returned)
            if (canonical_bytes(receipt).decode("ascii") != returned or receipt.request != request
                or receipt.request_digest != digest):
                raise ValueError("native claim receipt changed")
            return receipt

    async def authorize_source(self, request: BuildClaimRequestV1, *, worker_credential: str) -> BuildClaimSourceV1:
        """Recheck live authority; a claim replay alone never grants source IO."""
        if not self._session.in_transaction():
            raise ValueError("native source requires an outer transaction")
        request = BuildClaimRequestV1.model_validate_json(request.model_dump_json())
        if not isinstance(worker_credential, str) or re.fullmatch(r"[A-Za-z0-9_-]{43,512}", worker_credential) is None:
            raise ValueError("native source credential is invalid")
        wire, digest = canonical_bytes(request), canonical_digest(request)
        async with self._session.begin_nested():
            returned = await self._session.scalar(text("""SELECT loom_capacity_build_guard.authorize_source(
                :installation,CAST(:payload AS jsonb),:wire,:digest,:credential)"""),
                {"installation": self._installation.id, "payload": wire.decode("ascii"), "wire": wire,
                    "digest": digest, "credential": sha256(worker_credential.encode("ascii")).hexdigest()})
            receipt = BuildClaimSourceV1.model_validate_json(returned)
            if (canonical_bytes(receipt).decode("ascii") != returned or receipt.claim != request
                or receipt.claim_digest != digest):
                raise ValueError("native source receipt changed")
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
