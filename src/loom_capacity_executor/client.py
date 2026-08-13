"""Fail-closed mTLS transport for one pool-bound dry-run executor."""

from __future__ import annotations

import hashlib
import json
import ssl
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Literal, TypeAlias, TypeVar
from urllib.parse import SplitResult, urlsplit
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError

from loom_capacity_agent.client import (
    DemandReporterTLSFiles,
    build_reporter_tls_context,
    read_owner_only_bytes,
)
from loom_capacity_executor.dry_run import DryRunExecutorBinding
from loom_capacity_manager.auth import MAX_BEARER_TOKEN_BYTES
from loom_capacity_manager.contracts import Digest, StrictV1Model, canonical_bytes
from loom_capacity_manager.executable_contracts import (
    ExecutableBootstrapRegistrationV2,
    ExecutableExecutorHeartbeatV2,
    ExecutableExecutorInventoryV2,
    ExecutableExecutorRegistrationV2,
    ExecutableIntentBindingV2,
    ExecutableIntentCloseV2,
    ExecutableLaunchPermitV2,
    ExecutablePartialReleaseV2,
    ExecutablePermitConsumptionV2,
    ExecutableReservationAcceptanceV2,
    ExecutableReservationProposalV2,
    ExecutionContextV2,
    ExecutionFenceV2,
    StrictV2Model,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import (
    DryRunBootstrapRegistrationV1,
    DryRunExecutorHeartbeatV1,
    DryRunExecutorInventoryV1,
    DryRunIntentCloseV1,
    DryRunPartialReleaseV1,
    DryRunPermitConsumptionV1,
    DryRunReservationAcceptanceV1,
    canonical_grant_digest,
)

_MAX_CREDENTIAL_BYTES = MAX_BEARER_TOKEN_BYTES
_MAX_RECEIPT_BYTES = 64 * 1024
_ReceiptT = TypeVar("_ReceiptT", bound=BaseModel)


class ExecutorTransportError(RuntimeError):
    """A remote dry-run transition was not verifiably acknowledged."""


class ExecutorRejectedError(ExecutorTransportError):
    """The authenticated manager definitively rejected one executor request."""


class _Receipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class ExecutorCheckpointReceiptV1(_Receipt):
    executor_row_id: UUID
    authority_incarnation: UUID
    writer_epoch: int
    executor_id: str
    executor_incarnation: UUID
    pool_id: str
    pool_generation: int
    command_sequence: int
    journal_sequence: int
    journal_digest: Digest
    inventory_sequence: int
    lease_expires_at: datetime
    executable: Literal[False]


class ExecutorHeartbeatReceiptV1(_Receipt):
    executor_row_id: UUID
    heartbeat_sequence: int
    journal_sequence: int
    lease_expires_at: datetime
    replayed: bool
    executable: Literal[False]


class ExecutorInventoryReceiptV1(_Receipt):
    observation_id: UUID
    inventory_digest: Digest
    inventory_sequence: int
    authenticated_count: int
    quarantined_count: int
    foreign_count: int
    replayed: bool
    executable: Literal[False]


class AcceptedReservationReceiptV1(_Receipt):
    tranche_id: UUID
    intent_ids: tuple[UUID, ...]
    replayed: bool
    executable: Literal[False]


class IntentReadyReceiptV1(_Receipt):
    intent_id: UUID
    bootstrap_registration_epoch: int
    replayed: bool
    executable: Literal[False]


class ConsumedPermitReceiptV1(_Receipt):
    permit_id: UUID
    intent_id: UUID
    replayed: bool
    executable: Literal[False]


class ClosingIntentReceiptV1(_Receipt):
    intent_id: UUID
    replayed: bool
    executable: Literal[False]


class ReleasedShapesReceiptV1(_Receipt):
    tranche_id: UUID
    released_shape_ids: tuple[str, ...]
    replayed: bool
    executable: Literal[False]


class ExecutableCheckpointReceiptV2(_Receipt):
    execution_epoch: int
    execution_manifest_sha256: Digest
    executor_id: str
    executor_incarnation: UUID
    pool_id: str
    pool_generation: int
    command_sequence: int
    journal_sequence: int
    journal_digest: Digest
    inventory_sequence: int
    lease_expires_at: datetime
    executable: Literal[True]


class ExecutableHeartbeatReceiptV2(_Receipt):
    heartbeat_sequence: int
    lease_expires_at: datetime
    replayed: bool
    executable: Literal[True]


class ExecutableInventoryReceiptV2(_Receipt):
    inventory_sequence: int
    inventory_digest: Digest
    replayed: bool
    executable: Literal[True]


class AcceptedExecutableReservationReceiptV2(_Receipt):
    tranche_id: UUID
    intent_ids: tuple[UUID, ...]
    receipt_digest: Digest
    replayed: bool
    executable: Literal[True]


class RegisteredExecutableBootstrapReceiptV2(_Receipt):
    intent_id: UUID
    bootstrap_registration_epoch: int
    receipt_digest: Digest
    replayed: bool
    executable: Literal[True]


class ConsumedExecutablePermitReceiptV2(_Receipt):
    permit_id: UUID
    intent_id: UUID
    receipt_digest: Digest
    replayed: bool
    executable: Literal[True]


class ClosingExecutableIntentReceiptV2(_Receipt):
    intent_id: UUID
    receipt_digest: Digest
    replayed: bool
    executable: Literal[True]


class ReleasedExecutableShapesReceiptV2(_Receipt):
    tranche_id: UUID
    released_shape_ids: tuple[str, ...]
    receipt_digest: Digest
    replayed: bool
    executable: Literal[True]


ExecutablePoolWorkV2: TypeAlias = (
    ExecutableReservationProposalV2
    | ExecutableIntentBindingV2
    | ExecutableLaunchPermitV2
    | ExecutableIntentCloseV2
    | ExecutablePartialReleaseV2
)
_EXECUTABLE_WORK: TypeAdapter[ExecutablePoolWorkV2] = TypeAdapter(ExecutablePoolWorkV2)


@dataclass(frozen=True, slots=True)
class ExecutorTLSFiles:
    ca_file: Path
    certificate_file: Path
    private_key_file: Path


@dataclass(frozen=True, slots=True)
class ExecutorConnection:
    manager_origin: str
    bearer_token_file: Path
    tls_files: ExecutorTLSFiles
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        _canonical_manager_origin(self.manager_origin)
        if not isinstance(self.bearer_token_file, Path):
            raise TypeError("executor bearer credential must be an explicit path")
        if not isinstance(self.tls_files, ExecutorTLSFiles):
            raise TypeError("executor TLS files must use the trusted file contract")
        if type(self.timeout_seconds) is not float or not 0 < self.timeout_seconds <= 60:
            raise ValueError("executor timeout must be a float between 0 and 60 seconds")


def _canonical_manager_origin(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or not value.isascii()
        or any(ord(character) < 0x21 for character in value)
    ):
        raise ValueError("manager origin must be an explicit HTTPS origin")
    try:
        parsed: SplitResult = urlsplit(value)
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("manager origin must be an explicit HTTPS origin") from exc
    if (
        parsed.scheme != "https"
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in ("", "/")
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("manager origin must be an explicit HTTPS origin")
    return value.rstrip("/")


def _validate_bearer(value: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > _MAX_CREDENTIAL_BYTES
        or not value.isascii()
        or any(not 0x21 <= ord(character) <= 0x7E for character in value)
    ):
        raise ValueError("executor bearer credential is invalid")
    return value


def _read_bearer(path: Path) -> str:
    try:
        value = read_owner_only_bytes(path, max_bytes=_MAX_CREDENTIAL_BYTES).decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("executor bearer credential is not UTF-8") from exc
    if value.endswith("\n"):
        value = value[:-1]
    return _validate_bearer(value)


class CapacityExecutorClient:
    """Send only exact pool-bound, zero-executable contracts to the manager."""

    def __init__(
        self,
        binding: DryRunExecutorBinding,
        *,
        manager_origin: str,
        bearer_token: str,
        http_client: httpx.AsyncClient,
        owns_http_client: bool = False,
    ) -> None:
        if not isinstance(binding, DryRunExecutorBinding):
            raise TypeError("executor binding must be the trusted dry-run contract")
        if not isinstance(http_client, httpx.AsyncClient):
            raise TypeError("executor HTTP client must be asynchronous")
        self.binding = binding
        self._manager_origin = _canonical_manager_origin(manager_origin)
        self._bearer_token = _validate_bearer(bearer_token)
        self._http = http_client
        self._owns_http = owns_http_client

    @classmethod
    def from_files(
        cls,
        binding: DryRunExecutorBinding,
        connection: ExecutorConnection,
    ) -> CapacityExecutorClient:
        tls = build_reporter_tls_context(
            DemandReporterTLSFiles(
                ca_file=connection.tls_files.ca_file,
                certificate_file=connection.tls_files.certificate_file,
                private_key_file=connection.tls_files.private_key_file,
            )
        )
        if not isinstance(tls, ssl.SSLContext):
            raise ValueError("executor TLS context is unavailable")
        bearer_token = _read_bearer(connection.bearer_token_file)
        client = httpx.AsyncClient(
            verify=tls,
            timeout=httpx.Timeout(connection.timeout_seconds),
            follow_redirects=False,
            trust_env=False,
        )
        return cls(
            binding,
            manager_origin=connection.manager_origin,
            bearer_token=bearer_token,
            http_client=client,
            owns_http_client=True,
        )

    def _assert_binding(self, contract: object) -> None:
        checks = (
            ("authority_incarnation", self.binding.authority_incarnation),
            ("writer_epoch", self.binding.writer_epoch),
            ("executor_id", self.binding.executor_id),
            ("executor_incarnation", self.binding.executor_incarnation),
            ("pool_id", self.binding.pool_id),
            ("pool_generation", self.binding.pool_generation),
        )
        if (
            any(
                hasattr(contract, field) and getattr(contract, field) != expected
                for field, expected in checks
            )
            or getattr(contract, "executable", None) is not False
        ):
            raise ExecutorTransportError("dry-run executor contract binding changed")

    async def _request(
        self,
        method: str,
        path: str,
        receipt_model: type[_ReceiptT],
        *,
        contract: StrictV1Model | None = None,
    ) -> _ReceiptT:
        content: bytes | None = None
        if contract is not None:
            self._assert_binding(contract)
            content = canonical_bytes(contract)
        try:
            response = await self._http.request(
                method,
                f"{self._manager_origin}{path}",
                content=content,
                headers={
                    "Authorization": f"Bearer {self._bearer_token}",
                    **({"Content-Type": "application/json"} if content is not None else {}),
                },
            )
        except httpx.HTTPError:
            raise ExecutorTransportError("capacity manager transport failed") from None
        if 400 <= response.status_code < 500:
            raise ExecutorRejectedError(
                f"capacity manager rejected executor request with status {response.status_code}"
            )
        if response.status_code != 200:
            raise ExecutorTransportError(
                f"capacity manager rejected executor request with status {response.status_code}"
            )
        if len(response.content) > _MAX_RECEIPT_BYTES:
            raise ExecutorTransportError("capacity manager receipt exceeds its byte bound")
        try:
            return receipt_model.model_validate_json(response.content)
        except (ValidationError, ValueError) as exc:
            raise ExecutorTransportError("capacity manager receipt is invalid") from exc

    async def checkpoint(self) -> ExecutorCheckpointReceiptV1:
        receipt = await self._request(
            "GET",
            f"/v1/executors/{self.binding.pool_id}/checkpoint",
            ExecutorCheckpointReceiptV1,
        )
        if any(
            (
                receipt.authority_incarnation != self.binding.authority_incarnation,
                receipt.writer_epoch != self.binding.writer_epoch,
                receipt.executor_id != self.binding.executor_id,
                receipt.executor_incarnation != self.binding.executor_incarnation,
                receipt.pool_id != self.binding.pool_id,
                receipt.pool_generation != self.binding.pool_generation,
            )
        ):
            raise ExecutorTransportError("capacity manager checkpoint binding changed")
        return receipt

    async def heartbeat(
        self,
        value: DryRunExecutorHeartbeatV1,
    ) -> ExecutorHeartbeatReceiptV1:
        receipt = await self._request(
            "PUT",
            f"/v1/executors/{self.binding.pool_id}/heartbeat",
            ExecutorHeartbeatReceiptV1,
            contract=value,
        )
        if (
            receipt.heartbeat_sequence != value.heartbeat_sequence
            or receipt.journal_sequence != value.journal_sequence
        ):
            raise ExecutorTransportError("capacity manager heartbeat receipt changed")
        return receipt

    async def inventory(
        self,
        value: DryRunExecutorInventoryV1,
    ) -> ExecutorInventoryReceiptV1:
        receipt = await self._request(
            "PUT",
            f"/v1/executors/{self.binding.pool_id}/inventory",
            ExecutorInventoryReceiptV1,
            contract=value,
        )
        if (
            receipt.inventory_sequence != value.inventory_sequence
            or receipt.inventory_digest != canonical_grant_digest(value)
        ):
            raise ExecutorTransportError("capacity manager inventory receipt changed")
        return receipt

    async def accept_reservation(
        self,
        value: DryRunReservationAcceptanceV1,
    ) -> AcceptedReservationReceiptV1:
        receipt = await self._request(
            "POST",
            f"/v1/executors/{self.binding.pool_id}/reservations/{value.tranche_id}/accept",
            AcceptedReservationReceiptV1,
            contract=value,
        )
        if receipt.tranche_id != value.tranche_id:
            raise ExecutorTransportError("capacity manager acceptance receipt changed")
        return receipt

    async def register_bootstrap(
        self,
        value: DryRunBootstrapRegistrationV1,
    ) -> IntentReadyReceiptV1:
        receipt = await self._request(
            "POST",
            f"/v1/executors/{self.binding.pool_id}/intents/{value.intent_id}/bootstrap",
            IntentReadyReceiptV1,
            contract=value,
        )
        if (
            receipt.intent_id != value.intent_id
            or receipt.bootstrap_registration_epoch != value.bootstrap_registration_epoch
        ):
            raise ExecutorTransportError("capacity manager bootstrap receipt changed")
        return receipt

    async def consume_launch_permit(
        self,
        value: DryRunPermitConsumptionV1,
    ) -> ConsumedPermitReceiptV1:
        receipt = await self._request(
            "POST",
            f"/v1/executors/{self.binding.pool_id}/permits/{value.permit_id}/consume",
            ConsumedPermitReceiptV1,
            contract=value,
        )
        if receipt.permit_id != value.permit_id or receipt.intent_id != value.intent_id:
            raise ExecutorTransportError("capacity manager permit receipt changed")
        return receipt

    async def begin_intent_close(
        self,
        value: DryRunIntentCloseV1,
    ) -> ClosingIntentReceiptV1:
        receipt = await self._request(
            "POST",
            f"/v1/executors/{self.binding.pool_id}/intents/{value.intent_id}/close",
            ClosingIntentReceiptV1,
            contract=value,
        )
        if receipt.intent_id != value.intent_id:
            raise ExecutorTransportError("capacity manager close receipt changed")
        return receipt

    async def release_shapes(
        self,
        value: DryRunPartialReleaseV1,
    ) -> ReleasedShapesReceiptV1:
        receipt = await self._request(
            "POST",
            f"/v1/executors/{self.binding.pool_id}/reservations/{value.tranche_id}/release",
            ReleasedShapesReceiptV1,
            contract=value,
        )
        expected = tuple(sorted(item.shape_instance_id for item in value.releases))
        if receipt.tranche_id != value.tranche_id or receipt.released_shape_ids != expected:
            raise ExecutorTransportError("capacity manager release receipt changed")
        return receipt

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> CapacityExecutorClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()


def _receipt_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


class ExecutableCapacityExecutorClient:
    """Send only exact executable-v2 contracts for one registered pool executor."""

    def __init__(
        self,
        registration: ExecutableExecutorRegistrationV2,
        *,
        manager_origin: str,
        bearer_token: str,
        http_client: httpx.AsyncClient,
        owns_http_client: bool = False,
    ) -> None:
        if not isinstance(registration, ExecutableExecutorRegistrationV2):
            raise TypeError("executable client requires its exact registration")
        if not isinstance(http_client, httpx.AsyncClient):
            raise TypeError("executor HTTP client must be asynchronous")
        self.registration = registration
        self._manager_origin = _canonical_manager_origin(manager_origin)
        self._bearer_token = _validate_bearer(bearer_token)
        self._http = http_client
        self._owns_http = owns_http_client

    def _binding(self, value: object) -> ExecutableIntentBindingV2 | None:
        if isinstance(value, ExecutableIntentBindingV2):
            return value
        binding = getattr(value, "binding", None)
        if isinstance(binding, ExecutableIntentBindingV2):
            return binding
        releases = getattr(value, "releases", ())
        if releases:
            release_binding = getattr(releases[0], "binding", None)
            if isinstance(release_binding, ExecutableIntentBindingV2):
                return release_binding
        return None

    def _assert_contract_binding(self, value: StrictV2Model) -> None:
        if (
            not isinstance(value, ExecutableIntentBindingV2)
            and getattr(value, "executable", None) is not True
        ):
            raise ExecutorTransportError("executable executor contract binding changed")
        binding = self._binding(value)
        execution = binding.execution if binding is not None else getattr(value, "execution", None)
        heartbeat = isinstance(value, ExecutableExecutorHeartbeatV2)
        if heartbeat:
            if not isinstance(execution, ExecutionContextV2):
                raise ExecutorTransportError("executable executor contract binding changed")
        elif not isinstance(execution, ExecutionFenceV2):
            raise ExecutorTransportError("executable executor contract binding changed")
        checks = (
            ("executor_id", self.registration.executor_id),
            ("executor_incarnation", self.registration.executor_incarnation),
            ("pool_generation", self.registration.pool_generation),
            *(
                ()
                if isinstance(value, ExecutableReservationAcceptanceV2)
                else (("pool_id", self.registration.pool_id),)
            ),
        )
        target = binding if binding is not None else value
        if any(getattr(target, field, None) != expected for field, expected in checks):
            raise ExecutorTransportError("executable executor contract binding changed")
        execution_exclusions = {"executable"} if heartbeat else {"allocation_epoch", "executable"}
        actual_execution = execution.model_dump(exclude=execution_exclusions)
        expected_execution = self.registration.execution.model_dump(exclude={"executable"})
        if actual_execution != expected_execution:
            raise ExecutorTransportError("executable execution binding changed")

    async def _request(
        self,
        method: str,
        path: str,
        receipt_model: type[_ReceiptT],
        *,
        contract: StrictV2Model | None = None,
    ) -> _ReceiptT:
        content: bytes | None = None
        if contract is not None:
            self._assert_contract_binding(contract)
            content = canonical_executable_bytes(contract)
        try:
            response = await self._http.request(
                method,
                f"{self._manager_origin}{path}",
                content=content,
                headers={
                    "Authorization": f"Bearer {self._bearer_token}",
                    **({"Content-Type": "application/json"} if content is not None else {}),
                },
            )
        except httpx.HTTPError:
            raise ExecutorTransportError("capacity manager transport failed") from None
        if 400 <= response.status_code < 500:
            raise ExecutorRejectedError(
                f"capacity manager rejected executor request with status {response.status_code}"
            )
        if response.status_code != 200:
            raise ExecutorTransportError(
                f"capacity manager rejected executor request with status {response.status_code}"
            )
        if len(response.content) > _MAX_RECEIPT_BYTES:
            raise ExecutorTransportError("capacity manager receipt exceeds its byte bound")
        try:
            return receipt_model.model_validate_json(response.content)
        except (ValidationError, ValueError) as exc:
            raise ExecutorTransportError("capacity manager receipt is invalid") from exc

    @staticmethod
    def _assert_receipt_digest(receipt_digest: str, payload: dict[str, object]) -> None:
        if receipt_digest != _receipt_digest(payload):
            raise ExecutorTransportError("capacity manager receipt digest changed")

    async def executable_checkpoint(self) -> ExecutableCheckpointReceiptV2:
        receipt = await self._request(
            "GET",
            f"/v2/executors/{self.registration.pool_id}/checkpoint",
            ExecutableCheckpointReceiptV2,
        )
        if (
            receipt.execution_epoch != self.registration.execution.execution_epoch
            or receipt.execution_manifest_sha256
            != self.registration.execution.execution_manifest_sha256
            or receipt.executor_id != self.registration.executor_id
            or receipt.executor_incarnation != self.registration.executor_incarnation
            or receipt.pool_id != self.registration.pool_id
            or receipt.pool_generation != self.registration.pool_generation
        ):
            raise ExecutorTransportError("capacity manager checkpoint binding changed")
        return receipt

    async def heartbeat_executable_executor(
        self,
        value: ExecutableExecutorHeartbeatV2,
    ) -> ExecutableHeartbeatReceiptV2:
        receipt = await self._request(
            "PUT",
            f"/v2/executors/{self.registration.pool_id}/heartbeat",
            ExecutableHeartbeatReceiptV2,
            contract=value,
        )
        if (
            receipt.heartbeat_sequence != value.heartbeat_sequence
            or receipt.lease_expires_at.tzinfo is None
            or receipt.lease_expires_at.utcoffset() is None
        ):
            raise ExecutorTransportError("capacity manager heartbeat receipt changed")
        return receipt

    async def next_executable_work(
        self,
        command_sequence: int,
    ) -> ExecutablePoolWorkV2 | None:
        if type(command_sequence) is not int or command_sequence < 0:
            raise ValueError("executable command high-water is invalid")
        try:
            response = await self._http.request(
                "GET",
                f"{self._manager_origin}/v2/executors/{self.registration.pool_id}/work",
                headers={"Authorization": f"Bearer {self._bearer_token}"},
            )
        except httpx.HTTPError:
            raise ExecutorTransportError("capacity manager transport failed") from None
        if 400 <= response.status_code < 500:
            raise ExecutorRejectedError(
                f"capacity manager rejected executor request with status {response.status_code}"
            )
        if response.status_code != 200:
            raise ExecutorTransportError(
                f"capacity manager rejected executor request with status {response.status_code}"
            )
        if len(response.content) > _MAX_RECEIPT_BYTES:
            raise ExecutorTransportError("capacity manager work exceeds its byte bound")
        if response.content == b"null":
            return None
        try:
            work = _EXECUTABLE_WORK.validate_json(response.content)
        except (ValidationError, ValueError) as exc:
            raise ExecutorTransportError("capacity manager work is invalid") from exc
        self._assert_contract_binding(work)
        work_sequence = getattr(work, "command_sequence", None)
        if work_sequence is not None and work_sequence != command_sequence + 1:
            raise ExecutorTransportError("capacity manager work command sequence changed")
        return work

    async def accept_executable_reservation(
        self,
        value: ExecutableReservationAcceptanceV2,
    ) -> AcceptedExecutableReservationReceiptV2:
        receipt = await self._request(
            "POST",
            f"/v2/executors/{self.registration.pool_id}/reservations/{value.tranche_id}/accept",
            AcceptedExecutableReservationReceiptV2,
            contract=value,
        )
        payload: dict[str, object] = {
            "tranche_id": str(value.tranche_id),
            "intent_ids": [str(item) for item in receipt.intent_ids],
            "executable": True,
        }
        if receipt.tranche_id != value.tranche_id:
            raise ExecutorTransportError("capacity manager acceptance receipt changed")
        self._assert_receipt_digest(receipt.receipt_digest, payload)
        return receipt

    async def register_executable_bootstrap(
        self,
        value: ExecutableBootstrapRegistrationV2,
    ) -> RegisteredExecutableBootstrapReceiptV2:
        receipt = await self._request(
            "POST",
            f"/v2/executors/{self.registration.pool_id}/intents/{value.binding.intent_id}/bootstrap",
            RegisteredExecutableBootstrapReceiptV2,
            contract=value,
        )
        payload: dict[str, object] = {
            "intent_id": str(value.binding.intent_id),
            "bootstrap_registration_epoch": value.bootstrap_registration_epoch,
            "executable": True,
        }
        if (
            receipt.intent_id != value.binding.intent_id
            or receipt.bootstrap_registration_epoch != value.bootstrap_registration_epoch
        ):
            raise ExecutorTransportError("capacity manager bootstrap receipt changed")
        self._assert_receipt_digest(receipt.receipt_digest, payload)
        return receipt

    async def consume_executable_permit(
        self,
        value: ExecutablePermitConsumptionV2,
    ) -> ConsumedExecutablePermitReceiptV2:
        receipt = await self._request(
            "POST",
            f"/v2/executors/{self.registration.pool_id}/permits/{value.permit_id}/consume",
            ConsumedExecutablePermitReceiptV2,
            contract=value,
        )
        payload: dict[str, object] = {
            "permit_id": str(value.permit_id),
            "intent_id": str(value.binding.intent_id),
            "executable": True,
        }
        if receipt.permit_id != value.permit_id or receipt.intent_id != value.binding.intent_id:
            raise ExecutorTransportError("capacity manager permit receipt changed")
        self._assert_receipt_digest(receipt.receipt_digest, payload)
        return receipt

    async def ingest_executable_inventory(
        self,
        value: ExecutableExecutorInventoryV2,
    ) -> ExecutableInventoryReceiptV2:
        receipt = await self._request(
            "PUT",
            f"/v2/executors/{self.registration.pool_id}/inventory",
            ExecutableInventoryReceiptV2,
            contract=value,
        )
        if (
            receipt.inventory_sequence != value.inventory_sequence
            or receipt.inventory_digest != canonical_executable_digest(value)
        ):
            raise ExecutorTransportError("capacity manager inventory receipt changed")
        return receipt

    async def close_executable_intent(
        self,
        value: ExecutableIntentCloseV2,
    ) -> ClosingExecutableIntentReceiptV2:
        receipt = await self._request(
            "POST",
            f"/v2/executors/{self.registration.pool_id}/intents/{value.binding.intent_id}/close",
            ClosingExecutableIntentReceiptV2,
            contract=value,
        )
        payload: dict[str, object] = {
            "intent_id": str(value.binding.intent_id),
            "executable": True,
        }
        if receipt.intent_id != value.binding.intent_id:
            raise ExecutorTransportError("capacity manager close receipt changed")
        self._assert_receipt_digest(receipt.receipt_digest, payload)
        return receipt

    async def release_executable_shapes(
        self,
        value: ExecutablePartialReleaseV2,
    ) -> ReleasedExecutableShapesReceiptV2:
        receipt = await self._request(
            "POST",
            f"/v2/executors/{self.registration.pool_id}/reservations/{value.tranche_id}/release",
            ReleasedExecutableShapesReceiptV2,
            contract=value,
        )
        released = tuple(item.binding.shape_instance_id for item in value.releases)
        payload: dict[str, object] = {
            "tranche_id": str(value.tranche_id),
            "released_shape_ids": list(released),
            "executable": True,
        }
        if receipt.tranche_id != value.tranche_id or receipt.released_shape_ids != released:
            raise ExecutorTransportError("capacity manager release receipt changed")
        self._assert_receipt_digest(receipt.receipt_digest, payload)
        return receipt

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> ExecutableCapacityExecutorClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()


__all__ = [
    "AcceptedReservationReceiptV1",
    "CapacityExecutorClient",
    "ClosingIntentReceiptV1",
    "ConsumedPermitReceiptV1",
    "ExecutableCapacityExecutorClient",
    "ExecutableCheckpointReceiptV2",
    "ExecutableHeartbeatReceiptV2",
    "ExecutableInventoryReceiptV2",
    "ExecutablePoolWorkV2",
    "ExecutorCheckpointReceiptV1",
    "ExecutorConnection",
    "ExecutorHeartbeatReceiptV1",
    "ExecutorInventoryReceiptV1",
    "ExecutorRejectedError",
    "ExecutorTLSFiles",
    "ExecutorTransportError",
    "IntentReadyReceiptV1",
    "ReleasedShapesReceiptV1",
]
