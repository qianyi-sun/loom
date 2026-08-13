"""Protected executable-v2 admission transactions for one environment."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from loom_capacity_agent.admission import (
    BoundExecutableWorkerV2,
    DrainedExecutableWorkerV2,
    ExecutableDrainRequestV2,
    ExecutableReleaseReceiptV2,
    ExecutableReleaseRequestV2,
    ExecutableWorkerRegistrationV2,
    PhysicalJobBindingV2,
    PreparedExecutableAdmissionV2,
    RegisteredExecutableWorkerV2,
)
from loom_capacity_agent.claim_guard import (
    ExecutableClaimProposalV2,
    ExecutableClaimReceiptV2,
)
from loom_capacity_agent.contracts import AgentRegistrationV1
from loom_capacity_agent.prepared_store import (
    CapacityPreparedAdmissionError,
    parse_protected_response,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
)

_SCHEMA = "loom_capacity_guard"
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_ReceiptT = TypeVar("_ReceiptT", bound=BaseModel)


class ExecutableAdmissionError(CapacityPreparedAdmissionError):
    """Executable admission was not exactly protected or acknowledged."""


def _clear_credential(value: str | None, *, label: str) -> str | None:
    if value is None:
        return None
    if (
        not isinstance(value, str)
        or not 1 <= len(value.encode("utf-8")) <= 4096
        or not value.isascii()
        or any(not 0x21 <= ord(character) <= 0x7E for character in value)
    ):
        raise ExecutableAdmissionError(f"{label} is invalid")
    return value


class ExecutableAdmissionStore:
    """Invoke only the executor-role executable admission procedure surface."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        registration: AgentRegistrationV1 | None = None,
        subject_id: UUID | None = None,
        subject_incarnation: UUID | None = None,
    ) -> None:
        if not isinstance(session, AsyncSession):
            raise TypeError("executable admission requires an asynchronous database session")
        if registration is not None:
            if not isinstance(registration, AgentRegistrationV1):
                raise TypeError("executable admission registration is invalid")
            if subject_id is not None or subject_incarnation is not None:
                raise TypeError("executable admission scope must have one source")
            subject_id = registration.subject_id
            subject_incarnation = registration.subject_incarnation
        if not isinstance(subject_id, UUID) or not isinstance(subject_incarnation, UUID):
            raise TypeError("executable admission requires an exact subject incarnation")
        self._session = session
        self._registration = registration
        self.subject_id = subject_id
        self.subject_incarnation = subject_incarnation

    def _assert_binding(self, value: StrictV2Model) -> None:
        binding = getattr(value, "binding", None)
        if (
            binding is None
            or binding.subject_id != self.subject_id
            or binding.subject_incarnation != self.subject_incarnation
            or getattr(value, "executable", None) is not True
        ):
            raise ExecutableAdmissionError("executable admission subject binding changed")
        if self._registration is not None and (
            binding.deployment_generation != self._registration.deployment_generation
            or binding.candidate.publication_sha256 != self._registration.candidate_digest
        ):
            raise ExecutableAdmissionError("executable admission candidate binding changed")

    async def _invoke(
        self,
        function_name: str,
        value: StrictV2Model,
        receipt_type: type[_ReceiptT],
        *,
        extra: Mapping[str, object] | None = None,
    ) -> _ReceiptT:
        self._assert_binding(value)
        request_bytes = canonical_executable_bytes(value)
        request_digest = canonical_executable_digest(value)
        parameters: dict[str, object] = {
            "subject_id": self.subject_id,
            "subject_incarnation": self.subject_incarnation,
            "payload": request_bytes.decode("ascii"),
            "canonical_payload": request_bytes,
            "request_digest": request_digest,
            **dict(extra or {}),
        }
        extra_arguments = "".join(f", :{name}" for name in (extra or {}))
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.{function_name}("
                        ":subject_id, :subject_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :request_digest"
                        f"{extra_arguments})"
                    ),
                    parameters,
                )
            ).scalar_one()
            try:
                receipt = parse_protected_response(
                    returned,
                    receipt_type,
                    label="executable admission procedure",
                )
            except CapacityPreparedAdmissionError as exc:
                raise ExecutableAdmissionError(str(exc)) from exc
            if getattr(receipt, "request_digest", None) != request_digest:
                raise ExecutableAdmissionError(
                    "protected executable receipt differs from its full request digest"
                )
        return receipt

    async def prepare_worker(
        self,
        request: ExecutableBootstrapRegistrationV2,
        *,
        bootstrap_sha256: str,
    ) -> PreparedExecutableAdmissionV2:
        """Seal one unused bootstrap digest before scheduler submission."""

        if not isinstance(request, ExecutableBootstrapRegistrationV2):
            raise TypeError("executable preparation requires its schema-v2 contract")
        if not isinstance(bootstrap_sha256, str) or _DIGEST_RE.fullmatch(bootstrap_sha256) is None:
            raise ExecutableAdmissionError("bootstrap digest is invalid")
        receipt = await self._invoke(
            "prepare_executable_worker",
            request,
            PreparedExecutableAdmissionV2,
            extra={"bootstrap_sha256": bootstrap_sha256},
        )
        if (
            receipt.intent_id != request.binding.intent_id
            or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
            or receipt.bootstrap_sha256 != bootstrap_sha256
            or receipt.admission_digest != receipt.request_digest
        ):
            raise ExecutableAdmissionError("protected executable preparation receipt changed")
        return receipt

    async def bind_slurm_job(
        self,
        request: PhysicalJobBindingV2,
    ) -> BoundExecutableWorkerV2:
        """Bind the exact returned or adopted Slurm job before exchange."""

        if not isinstance(request, PhysicalJobBindingV2):
            raise TypeError("physical binding requires its schema-v2 contract")
        receipt = await self._invoke(
            "bind_executable_slurm_job",
            request,
            BoundExecutableWorkerV2,
        )
        if (
            receipt.intent_id != request.binding.intent_id
            or receipt.slurm_job_id != request.slurm_job_id
            or receipt.ownership_evidence_sha256 != request.ownership_evidence_sha256
            or receipt.binding_digest != receipt.request_digest
        ):
            raise ExecutableAdmissionError("protected physical binding receipt changed")
        return receipt

    async def register_worker(
        self,
        request: ExecutableWorkerRegistrationV2,
        *,
        bootstrap_capability: str | None = None,
        predecessor_worker_credential: str | None = None,
    ) -> RegisteredExecutableWorkerV2:
        """Exchange once or requeue by authenticating and revoking the predecessor."""

        if not isinstance(request, ExecutableWorkerRegistrationV2):
            raise TypeError("worker registration requires its schema-v2 contract")
        bootstrap = _clear_credential(bootstrap_capability, label="bootstrap capability")
        predecessor = _clear_credential(
            predecessor_worker_credential,
            label="predecessor worker credential",
        )
        is_requeue = request.predecessor_worker_incarnation is not None
        if is_requeue != (predecessor is not None) or is_requeue == (bootstrap is not None):
            raise ExecutableAdmissionError(
                "worker registration requires exactly one current credential"
            )
        receipt = await self._invoke(
            "register_executable_worker",
            request,
            RegisteredExecutableWorkerV2,
            extra={
                "bootstrap_capability": bootstrap,
                "predecessor_worker_credential": predecessor,
            },
        )
        if (
            receipt.intent_id != request.binding.intent_id
            or receipt.worker_id != request.worker_id
            or receipt.worker_incarnation != request.worker_incarnation
            or receipt.predecessor_worker_incarnation != request.predecessor_worker_incarnation
            or receipt.protected_registration_epoch != request.protected_registration_epoch
            or receipt.registration_digest != receipt.request_digest
        ):
            raise ExecutableAdmissionError("protected worker registration receipt changed")
        return receipt

    async def begin_drain(
        self,
        request: ExecutableDrainRequestV2,
    ) -> DrainedExecutableWorkerV2:
        if not isinstance(request, ExecutableDrainRequestV2):
            raise TypeError("worker drain requires its schema-v2 contract")
        receipt = await self._invoke(
            "begin_executable_worker_drain",
            request,
            DrainedExecutableWorkerV2,
        )
        if (
            receipt.worker_id != request.worker_id
            or receipt.worker_incarnation != request.worker_incarnation
            or receipt.claim_high_water != request.expected_claim_high_water
            or receipt.drain_epoch != request.drain_epoch
            or receipt.drain_digest != receipt.request_digest
        ):
            raise ExecutableAdmissionError("protected drain receipt changed")
        return receipt

    async def acknowledge_release(
        self,
        request: ExecutableReleaseRequestV2,
        *,
        current_worker_credential: str,
    ) -> ExecutableReleaseReceiptV2:
        if not isinstance(request, ExecutableReleaseRequestV2):
            raise TypeError("worker release requires its schema-v2 contract")
        credential = _clear_credential(
            current_worker_credential,
            label="current worker credential",
        )
        assert credential is not None
        receipt = await self._invoke(
            "acknowledge_executable_release",
            request,
            ExecutableReleaseReceiptV2,
            extra={"current_worker_credential": credential},
        )
        if (
            receipt.binding != request.binding
            or receipt.reporter_incarnation != request.reporter_incarnation
            or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
            or receipt.protected_registration_epoch != request.protected_registration_epoch
            or receipt.claim_high_water != request.expected_claim_high_water
            or receipt.release_epoch != request.release_epoch
            or receipt.bootstrap_revoked is not True
            or receipt.worker_credentials_revoked is not True
            or receipt.live_claim_count != 0
        ):
            raise ExecutableAdmissionError("protected release receipt changed")
        return receipt

    async def admit_claim(
        self,
        proposal: ExecutableClaimProposalV2,
    ) -> ExecutableClaimReceiptV2 | None:
        if not isinstance(proposal, ExecutableClaimProposalV2):
            raise TypeError("executable claim requires its schema-v2 contract")
        request_bytes = canonical_executable_bytes(proposal)
        request_digest = canonical_executable_digest(proposal)
        async with self._session.begin_nested():
            returned = (
                await self._session.execute(
                    text(
                        f"SELECT {_SCHEMA}.admit_executable_claim("
                        ":subject_id, :subject_incarnation, CAST(:payload AS jsonb), "
                        "CAST(:canonical_payload AS bytea), :request_digest)"
                    ),
                    {
                        "subject_id": self.subject_id,
                        "subject_incarnation": self.subject_incarnation,
                        "payload": request_bytes.decode("ascii"),
                        "canonical_payload": request_bytes,
                        "request_digest": request_digest,
                    },
                )
            ).scalar_one()
            if returned is None:
                return None
            try:
                receipt = parse_protected_response(
                    returned,
                    ExecutableClaimReceiptV2,
                    label="executable claim procedure",
                )
            except CapacityPreparedAdmissionError as exc:
                raise ExecutableAdmissionError(str(exc)) from exc
            if (
                receipt.operation_id != proposal.operation_id
                or receipt.protected_attempt_id != proposal.protected_attempt_id
                or receipt.execution_generation != proposal.execution_generation
                or receipt.requirements_digest != proposal.requirements_digest
                or receipt.worker_id != proposal.worker_id
                or receipt.worker_incarnation != proposal.worker_incarnation
                or receipt.claim_high_water != proposal.expected_claim_high_water + 1
                or receipt.request_digest != request_digest
            ):
                raise ExecutableAdmissionError("protected executable claim receipt changed")
        return receipt


__all__ = ["ExecutableAdmissionError", "ExecutableAdmissionStore"]
