"""Bounded controller-only HTTPS client for the management build guard."""

from __future__ import annotations

import asyncio
from typing import TypeVar, cast
from uuid import UUID

import httpx

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
    BuildAllocatedClaimExchangeV1,
    BuildAllocatedClaimRequestV1,
    BuildClaimExchangeV1,
    BuildClaimReceiptV1,
    BuildClaimRequestV1,
    BuildOutcomeExchangeV1,
    BuildOutcomeReceiptV1,
    BuildOutcomeRequestV1,
    BuildPreparationRequestV1,
    BuildRegistrationRequestV1,
    BuildReleaseExchangeV1,
    BuildSourceContextV1,
    BuildSourceReadExchangeV1,
    BuildSourceReadReceiptV1,
)
from loom_capacity_agent.client import (
    DemandReporterConnection,
    build_reporter_tls_context,
    canonical_manager_origin,
    read_owner_only_bearer_token,
)
from loom_capacity_executor.pinned_admission_transport import (
    PinnedBuildAdmissionConnectionV1,
    load_pinned_admission_credentials,
)
from loom_capacity_manager.auth import MAX_BEARER_TOKEN_BYTES
from loom_capacity_manager.contracts import (
    Identifier,
    PositiveQuantity,
    StrictV1Model,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableIntentBindingV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
)

_Receipt = TypeVar("_Receipt", bound=StrictV1Model | StrictV2Model)
_MAX_RESPONSE_BYTES = 64 * 1024


class BuildAdmissionTransportError(RuntimeError):
    """Delivery or exact evidence validation failed; do not infer admission."""


class BuildAdmissionExecutorV1(StrictV1Model):
    pool_id: Identifier
    pool_generation: PositiveQuantity
    executor_id: Identifier
    executor_incarnation: UUID


def _validate_connection(origin: str, token: str, timeout: float) -> str:
    resolved = canonical_manager_origin(origin)
    if (not isinstance(token,str) or not token.isascii()
        or not 1 <= len(token) <= MAX_BEARER_TOKEN_BYTES
        or any(not 0x21 <= ord(character) <= 0x7e for character in token)):
        raise ValueError("build admission bearer credential is invalid")
    if (isinstance(timeout,bool) or not isinstance(timeout,(int,float))
        or not 0.05 <= timeout <= 60):
        raise ValueError("build admission timeout must be between 0.05 and 60 seconds")
    return resolved


class BuildAdmissionClient:
    """Pool-authenticated lifecycle and bounded source IO; no application DB access."""

    def __init__(self, identity: BuildAdmissionExecutorV1, *, origin: str,
        bearer_token: str, http_client: httpx.AsyncClient, owns_http_client: bool = False,
        timeout_seconds: float = 30.0,
    ) -> None:
        self.identity = BuildAdmissionExecutorV1.model_validate_json(identity.model_dump_json())
        self._origin = _validate_connection(origin,bearer_token,timeout_seconds)
        if not isinstance(http_client,httpx.AsyncClient):
            raise TypeError("build admission requires an async HTTP client")
        self._token = bearer_token
        self._http = http_client
        self._owns_http = owns_http_client
        self._timeout = timeout_seconds

    @classmethod
    def from_files(cls, identity: BuildAdmissionExecutorV1, connection: DemandReporterConnection) -> BuildAdmissionClient:
        identity = BuildAdmissionExecutorV1.model_validate_json(identity.model_dump_json())
        token = read_owner_only_bearer_token(connection.bearer_token_file)
        _validate_connection(connection.manager_origin,token,connection.timeout_seconds)
        tls = build_reporter_tls_context(connection.tls_files)
        http = httpx.AsyncClient(verify=tls,timeout=httpx.Timeout(connection.timeout_seconds),
            trust_env=False,follow_redirects=False)
        return cls(identity,origin=connection.manager_origin,bearer_token=token,http_client=http,
            owns_http_client=True,timeout_seconds=connection.timeout_seconds)

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    @classmethod
    def from_pinned_files(cls, identity: BuildAdmissionExecutorV1, connection: PinnedBuildAdmissionConnectionV1) -> BuildAdmissionClient:
        identity = BuildAdmissionExecutorV1.model_validate_json(identity.model_dump_json())
        connection = PinnedBuildAdmissionConnectionV1.model_validate_json(connection.model_dump_json())
        tls, token = load_pinned_admission_credentials(connection)
        _validate_connection(connection.origin,token,connection.timeout_seconds)
        http = httpx.AsyncClient(verify=tls,timeout=httpx.Timeout(connection.timeout_seconds),
            trust_env=False,follow_redirects=False)
        return cls(identity,origin=connection.origin,bearer_token=token,http_client=http,
            owns_http_client=True,timeout_seconds=connection.timeout_seconds)

    def _assert_binding(self, binding: ExecutableIntentBindingV2) -> None:
        identity = self.identity
        if (binding.pool_id != identity.pool_id or binding.pool_generation != identity.pool_generation
            or binding.executor_id != identity.executor_id or binding.executor_incarnation != identity.executor_incarnation):
            raise ValueError("build admission executor binding changed")

    async def _post(self,binding: ExecutableIntentBindingV2,operation: str,payload: bytes,
        receipt_type: type[_Receipt], *, max_response_bytes: int = _MAX_RESPONSE_BYTES,
    ) -> _Receipt:
        self._assert_binding(binding)
        url = f"{self._origin}/api/v1/internal/capacity-build/pools/{binding.pool_id}/intents/{binding.intent_id}/{operation}"
        try:
            async with asyncio.timeout(self._timeout), self._http.stream("POST",url,content=payload,
                headers={"Authorization":f"Bearer {self._token}","Content-Type":"application/json"},
                timeout=self._timeout,follow_redirects=False) as response:
                if response.status_code != 200:
                    raise BuildAdmissionTransportError(f"build admission rejected request with status {response.status_code}")
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    if len(body)+len(chunk) > max_response_bytes:
                        raise BuildAdmissionTransportError("build admission receipt exceeds byte bound")
                    body.extend(chunk)
        except (httpx.HTTPError,TimeoutError):
            raise BuildAdmissionTransportError("build admission transport failed") from None
        try:
            receipt = receipt_type.model_validate_json(bytes(body))
            canonical = canonical_bytes(receipt) if isinstance(receipt, StrictV1Model) else canonical_executable_bytes(receipt)
            if canonical != bytes(body):
                raise ValueError("noncanonical receipt")
        except ValueError:
            raise BuildAdmissionTransportError("build admission receipt is invalid") from None
        return cast(_Receipt, receipt)

    async def read_source(self, claim: BuildClaimRequestV1, *, worker_credential: str,
        offset: int, length: int,
    ) -> BuildSourceReadReceiptV1:
        request = BuildSourceReadExchangeV1.model_validate_json(BuildSourceReadExchangeV1(
            claim=claim, worker_credential=worker_credential, offset=offset, length=length).model_dump_json())
        receipt = await self._post(request.claim.binding, "source", canonical_bytes(request),
            BuildSourceReadReceiptV1, max_response_bytes=1400000)
        if (receipt.claim_digest != canonical_digest(request.claim) or receipt.offset != request.offset
            or len(receipt.data) != min(request.length, receipt.archive_size_bytes - request.offset)):
            raise BuildAdmissionTransportError("native source response binding changed")
        return receipt

    async def read_source_context(self, claim: BuildClaimRequestV1, *, worker_credential: str) -> BuildSourceContextV1:
        request = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
            claim=claim, worker_credential=worker_credential).model_dump_json())
        receipt = await self._post(request.claim.binding, "context", canonical_bytes(request), BuildSourceContextV1)
        if (receipt.claim_digest != canonical_digest(request.claim) or receipt.request_id != request.claim.request_id
            or request.claim.binding.pool_id != ("gb10" if receipt.platform == "linux/arm64" else "oldlab")):
            raise BuildAdmissionTransportError("native context response binding changed")
        return receipt

    async def prepare_worker(self, request: ExecutableBootstrapRegistrationV2, *,
        bootstrap_sha256: str,
    ) -> PreparedExecutableAdmissionV2:
        envelope = BuildPreparationRequestV1.model_validate_json(BuildPreparationRequestV1(
            registration=request,bootstrap_sha256=bootstrap_sha256).model_dump_json())
        request = envelope.registration
        receipt = await self._post(request.binding,"prepare",canonical_bytes(envelope),PreparedExecutableAdmissionV2)
        digest = canonical_executable_digest(request)
        if (receipt.subject_id != request.binding.subject_id or receipt.subject_incarnation != request.binding.subject_incarnation
            or receipt.intent_id != request.binding.intent_id
            or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
            or receipt.bootstrap_sha256 != bootstrap_sha256 or receipt.request_digest != digest or receipt.admission_digest != digest):
            raise BuildAdmissionTransportError("build admission preparation receipt binding changed")
        return receipt

    async def register_worker(self, request: ExecutableWorkerRegistrationV2, *,
        bootstrap_capability: str,
    ) -> RegisteredExecutableWorkerV2:
        try:
            envelope = BuildRegistrationRequestV1.model_validate_json(BuildRegistrationRequestV1(
                registration=request, bootstrap_capability=bootstrap_capability).model_dump_json())
        except ValueError:
            raise ValueError("build registration request is invalid") from None
        request = envelope.registration
        receipt = await self._post(request.binding, "register", canonical_bytes(envelope), RegisteredExecutableWorkerV2)
        digest = canonical_executable_digest(request)
        if (receipt.subject_id != request.binding.subject_id or receipt.subject_incarnation != request.binding.subject_incarnation
            or receipt.intent_id != request.binding.intent_id or receipt.worker_id != request.worker_id
            or receipt.worker_incarnation != request.worker_incarnation
            or receipt.predecessor_worker_incarnation != request.predecessor_worker_incarnation
            or receipt.protected_registration_epoch != request.protected_registration_epoch
            or receipt.request_digest != digest or receipt.registration_digest != digest):
            raise BuildAdmissionTransportError("build admission registration binding changed")
        return receipt

    async def begin_drain(self, request: ExecutableDrainRequestV2) -> DrainedExecutableWorkerV2:
        request = ExecutableDrainRequestV2.model_validate_json(request.model_dump_json())
        receipt = await self._post(request.binding, "drain", canonical_executable_bytes(request), DrainedExecutableWorkerV2)
        digest = canonical_executable_digest(request)
        if (receipt.subject_id != request.binding.subject_id or receipt.subject_incarnation != request.binding.subject_incarnation
            or receipt.intent_id != request.binding.intent_id or receipt.worker_id != request.worker_id
            or receipt.worker_incarnation != request.worker_incarnation or receipt.claim_high_water != request.expected_claim_high_water
            or receipt.live_claim_count > request.expected_claim_high_water or receipt.drain_epoch != request.drain_epoch
            or receipt.request_digest != digest or receipt.drain_digest != digest):
            raise BuildAdmissionTransportError("native drain receipt binding changed")
        return receipt

    async def claim_platform(self, request: BuildClaimRequestV1, *, worker_credential: str) -> BuildClaimReceiptV1:
        try:
            exchange = BuildClaimExchangeV1.model_validate_json(BuildClaimExchangeV1(
                claim=request, worker_credential=worker_credential).model_dump_json())
        except ValueError:
            raise ValueError("native claim request is invalid") from None
        request = exchange.claim
        receipt = await self._post(request.binding, "claim", canonical_bytes(exchange), BuildClaimReceiptV1)
        if receipt.request != request or receipt.request_digest != canonical_digest(request):
            raise BuildAdmissionTransportError("native claim receipt binding changed")
        return receipt

    async def claim_assigned_platform(self, request: BuildAllocatedClaimRequestV1, *, worker_credential: str) -> BuildClaimReceiptV1:
        # Validate before an envelope's base-typed serialization could silently
        # omit a caller-selected request_id from a BuildClaimRequestV1 subclass.
        request = BuildAllocatedClaimRequestV1.model_validate_json(request.model_dump_json())
        envelope = BuildAllocatedClaimExchangeV1.model_validate_json(BuildAllocatedClaimExchangeV1(
            claim=request, worker_credential=worker_credential).model_dump_json())
        receipt = await self._post(envelope.claim.binding, "claim-assigned", canonical_bytes(envelope), BuildClaimReceiptV1)
        if (receipt.request.model_dump(exclude={"request_id"}) != envelope.claim.model_dump()
            or receipt.request_digest != canonical_digest(receipt.request)):
            raise BuildAdmissionTransportError("allocated native claim response binding changed")
        return receipt

    async def record_outcome(self, request: BuildOutcomeRequestV1, *, worker_credential: str) -> BuildOutcomeReceiptV1:
        try:
            exchange = BuildOutcomeExchangeV1.model_validate_json(BuildOutcomeExchangeV1(
                outcome=request, worker_credential=worker_credential).model_dump_json())
        except ValueError:
            raise ValueError("native outcome exchange is invalid") from None
        request = exchange.outcome
        receipt = await self._post(request.claim.binding, "outcome", canonical_bytes(exchange), BuildOutcomeReceiptV1)
        if receipt.request != request or receipt.request_digest != canonical_digest(request):
            raise BuildAdmissionTransportError("native outcome receipt binding changed")
        return receipt

    async def acknowledge_release(self, request: ExecutableReleaseRequestV2, *, current_worker_credential: str) -> ExecutableReleaseReceiptV2:
        try:
            exchange = BuildReleaseExchangeV1.model_validate_json(BuildReleaseExchangeV1(
                release=request, worker_credential=current_worker_credential).model_dump_json())
        except ValueError:
            raise ValueError("native release exchange is invalid") from None
        request = exchange.release
        receipt = await self._post(request.binding, "release", canonical_bytes(exchange), ExecutableReleaseReceiptV2)
        digest = canonical_executable_digest(request)
        if (receipt.binding != request.binding or receipt.reporter_incarnation != request.reporter_incarnation
            or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
            or receipt.protected_registration_epoch != request.protected_registration_epoch
            or receipt.claim_high_water != request.expected_claim_high_water or receipt.release_epoch != request.release_epoch
            or receipt.request_digest != digest or receipt.protected_release_sha256 != digest):
            raise BuildAdmissionTransportError("native release receipt binding changed")
        return receipt

    async def bind_slurm_job(self, request: PhysicalJobBindingV2) -> BoundExecutableWorkerV2:
        request = PhysicalJobBindingV2.model_validate_json(request.model_dump_json())
        receipt = await self._post(request.binding,"bind",canonical_executable_bytes(request),BoundExecutableWorkerV2)
        digest = canonical_executable_digest(request)
        if (receipt.subject_id != request.binding.subject_id or receipt.subject_incarnation != request.binding.subject_incarnation
            or receipt.intent_id != request.binding.intent_id
            or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
            or receipt.slurm_job_id != request.slurm_job_id or receipt.ownership_evidence_sha256 != request.ownership_evidence_sha256
            or receipt.request_digest != digest or receipt.binding_digest != digest):
            raise BuildAdmissionTransportError("build admission physical receipt binding changed")
        return receipt

    async def withdraw_unregistered_worker(self, request: ExecutableWorkerWithdrawalRequestV2) -> WithdrawnExecutableWorkerV2:
        request = ExecutableWorkerWithdrawalRequestV2.model_validate_json(request.model_dump_json())
        receipt = await self._post(request.binding,"withdraw",canonical_executable_bytes(request),WithdrawnExecutableWorkerV2)
        digest = canonical_executable_digest(request)
        if (receipt.subject_id != request.binding.subject_id or receipt.subject_incarnation != request.binding.subject_incarnation
            or receipt.intent_id != request.binding.intent_id
            or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
            or receipt.protected_registration_epoch != request.protected_registration_epoch
            or receipt.slurm_job_id != request.slurm_job_id
            or receipt.ownership_evidence_sha256 != request.ownership_evidence_sha256
            or receipt.request_digest != digest or receipt.withdrawal_digest != digest):
            raise BuildAdmissionTransportError("build admission withdrawal binding changed")
        return receipt

    async def observe_intent(self, binding: ExecutableIntentBindingV2) -> ProtectedIntentObservationV2:
        binding = ExecutableIntentBindingV2.model_validate_json(binding.model_dump_json())
        receipt = await self._post(binding,"observe",canonical_executable_bytes(binding),ProtectedIntentObservationV2)
        if receipt.binding != binding:
            raise BuildAdmissionTransportError("build admission observation binding changed")
        return receipt

    async def revoke_prepared_bootstrap(self, request: ExecutablePreparedBootstrapRevocationV2) -> RevokedExecutableBootstrapV2:
        request = ExecutablePreparedBootstrapRevocationV2.model_validate_json(request.model_dump_json())
        receipt = await self._post(request.binding,"revoke-bootstrap",canonical_executable_bytes(request),RevokedExecutableBootstrapV2)
        digest = canonical_executable_digest(request)
        if (receipt.binding != request.binding or receipt.bootstrap_registration_epoch != request.bootstrap_registration_epoch
            or receipt.protected_registration_epoch != request.protected_registration_epoch
            or receipt.request_digest != digest or receipt.protected_release_sha256 != digest):
            raise BuildAdmissionTransportError("build admission revocation binding changed")
        return receipt
