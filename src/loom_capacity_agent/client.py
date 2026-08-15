"""Fail-closed mTLS publication of protected demand snapshots."""

from __future__ import annotations

import hashlib
import json
import os
import ssl
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Literal
from urllib.parse import SplitResult, urlsplit
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, ValidationError

from loom_capacity_agent.admission import (
    PreparedProtectedReleaseV1,
    PublishableExecutableProtectedReleaseV2,
)
from loom_capacity_agent.contracts import ReporterConfigurationV1
from loom_capacity_guard.contracts import canonical_digest as guard_canonical_digest
from loom_capacity_manager.auth import MAX_BEARER_TOKEN_BYTES
from loom_capacity_manager.contracts import (
    CapacityContractError,
    DemandSnapshotV1,
    Digest,
    PositiveQuantity,
    canonical_bytes,
    canonical_digest,
)
from loom_capacity_manager.executable_contracts import (
    ExecutableProtectedReleaseV2,
    canonical_executable_bytes,
    canonical_executable_digest,
)
from loom_capacity_manager.grant_contracts import (
    DryRunProtectedReleaseAcknowledgementV1,
    canonical_grant_digest,
)

_MAX_CREDENTIAL_BYTES = MAX_BEARER_TOKEN_BYTES
_MAX_RECEIPT_BYTES = 16 * 1024
_PUBLISH_BINDINGS = (
    "subject_id",
    "subject_incarnation",
    "reporter_incarnation",
    "deployment_generation",
    "configuration_generation",
)


class DemandPublishError(RuntimeError):
    """A demand snapshot was not verifiably accepted by the manager."""


class DemandPublishReceiptV1(BaseModel):
    """Exact bounded acknowledgement returned by the manager API."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    snapshot_id: UUID
    digest: Digest
    sequence: PositiveQuantity
    replayed: bool


class ProtectedReleasePublishReceiptV1(BaseModel):
    """Exact bounded manager receipt for one locally fenced release."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    acknowledgement_id: UUID
    shape_instance_id: str
    acknowledgement_digest: Digest
    replayed: bool
    executable: Literal[False]


class ExecutableProtectedReleasePublishReceiptV2(BaseModel):
    """Exact bounded manager receipt for one executable protected release."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    intent_id: UUID
    protected_release_sha256: Digest
    receipt_digest: Digest
    replayed: bool
    executable: Literal[True]


def _receipt_digest(payload: dict[str, object]) -> str:
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True, slots=True)
class DemandReporterTLSFiles:
    ca_file: Path
    certificate_file: Path
    private_key_file: Path

    def __post_init__(self) -> None:
        if not all(
            isinstance(path, Path)
            for path in (self.ca_file, self.certificate_file, self.private_key_file)
        ):
            raise TypeError("reporter TLS credentials must be explicit paths")


@dataclass(frozen=True, slots=True)
class DemandReporterConnection:
    manager_origin: str
    bearer_token_file: Path
    tls_files: DemandReporterTLSFiles
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        canonical_manager_origin(self.manager_origin)
        if not isinstance(self.bearer_token_file, Path):
            raise TypeError("reporter bearer credential must be an explicit path")
        if not isinstance(self.tls_files, DemandReporterTLSFiles):
            raise TypeError("reporter TLS files must use the trusted file contract")
        if type(self.timeout_seconds) is not float or not 0 < self.timeout_seconds <= 60:
            raise ValueError("reporter timeout must be a float between 0 and 60 seconds")


@contextmanager
def _open_owner_only_file(path: Path, *, max_bytes: int = 1024 * 1024) -> Iterator[int]:
    if not isinstance(path, Path):
        raise TypeError("credential path must be explicit")
    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("maximum credential bytes must be a positive integer")
    metadata = path.lstat()
    if metadata.st_size > max_bytes:
        raise ValueError("credential exceeds its maximum byte size")
    if (
        not stat.S_ISREG(metadata.st_mode)
        or path.is_symlink()
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise ValueError("credential must be a current-UID-owned 0600 regular nonsymlink file")
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if opened.st_size > max_bytes:
            raise ValueError("credential exceeds its maximum byte size")
        if (
            (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or opened.st_uid != os.getuid()
            or stat.S_IMODE(opened.st_mode) != 0o600
        ):
            raise ValueError("credential file metadata changed while opening")
        yield descriptor
    finally:
        os.close(descriptor)


def read_owner_only_bytes(path: Path, *, max_bytes: int = _MAX_CREDENTIAL_BYTES) -> bytes:
    """Read one bounded credential without following or racing a path symlink."""

    if type(max_bytes) is not int or max_bytes <= 0:
        raise ValueError("maximum credential bytes must be a positive integer")
    with _open_owner_only_file(path, max_bytes=max_bytes) as descriptor:
        chunks: list[bytes] = []
        total = 0
        while total <= max_bytes:
            chunk = os.read(descriptor, min(4096, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
    payload = b"".join(chunks)
    if len(payload) > max_bytes:
        raise ValueError("credential exceeds its maximum byte size")
    if not payload:
        raise ValueError("credential file is empty")
    return payload


def read_owner_only_bearer_token(path: Path) -> str:
    payload = read_owner_only_bytes(path)
    try:
        value = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("reporter bearer credential is not UTF-8") from exc
    if value.endswith("\n"):
        value = value[:-1]
    if (
        not value
        or value != value.strip()
        or any(character in value for character in ("\r", "\n", "\x00"))
    ):
        raise ValueError("reporter bearer credential must contain one exact nonempty line")
    return value


def build_reporter_tls_context(files: DemandReporterTLSFiles) -> ssl.SSLContext:
    """Build a server-verifying client context from already-verified file descriptors."""

    try:
        ca_data = read_owner_only_bytes(files.ca_file, max_bytes=1024 * 1024).decode("ascii")
    except UnicodeDecodeError as exc:
        raise ValueError("reporter CA bundle is not ASCII PEM") from exc
    context = ssl.create_default_context()
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.check_hostname = True
    context.verify_mode = ssl.CERT_REQUIRED
    context.load_verify_locations(cadata=ca_data)

    with (
        _open_owner_only_file(files.certificate_file) as certificate_fd,
        _open_owner_only_file(files.private_key_file) as private_key_fd,
    ):
        certificate_path = f"/proc/self/fd/{certificate_fd}"
        private_key_path = f"/proc/self/fd/{private_key_fd}"
        if not Path(certificate_path).exists() or not Path(private_key_path).exists():
            raise ValueError("verified descriptor paths are unavailable for TLS credentials")
        context.load_cert_chain(certfile=certificate_path, keyfile=private_key_path)
    return context


def canonical_manager_origin(value: str) -> str:
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


class DemandReporterClient:
    """Publish only snapshots bound to one trusted reporter configuration."""

    def __init__(
        self,
        configuration: ReporterConfigurationV1,
        *,
        manager_origin: str,
        bearer_token: str,
        http_client: httpx.AsyncClient,
        owns_http_client: bool = False,
    ) -> None:
        if not isinstance(configuration, ReporterConfigurationV1):
            raise TypeError("reporter configuration must be a trusted schema-v1 contract")
        if (
            not isinstance(bearer_token, str)
            or not bearer_token
            or len(bearer_token.encode("utf-8")) > _MAX_CREDENTIAL_BYTES
            or not bearer_token.isascii()
            or any(not 0x21 <= ord(character) <= 0x7E for character in bearer_token)
        ):
            raise ValueError("reporter bearer credential is invalid")
        if not isinstance(http_client, httpx.AsyncClient):
            raise TypeError("reporter HTTP client must be asynchronous")
        self._configuration = configuration
        self._manager_origin = canonical_manager_origin(manager_origin)
        self._bearer_token = bearer_token
        self._http = http_client
        self._owns_http = owns_http_client

    @classmethod
    def from_files(
        cls,
        configuration: ReporterConfigurationV1,
        connection: DemandReporterConnection,
    ) -> DemandReporterClient:
        token = read_owner_only_bearer_token(connection.bearer_token_file)
        tls = build_reporter_tls_context(connection.tls_files)
        client = httpx.AsyncClient(
            verify=tls,
            timeout=httpx.Timeout(connection.timeout_seconds),
            follow_redirects=False,
            trust_env=False,
        )
        return cls(
            configuration,
            manager_origin=connection.manager_origin,
            bearer_token=token,
            http_client=client,
            owns_http_client=True,
        )

    async def publish(self, snapshot: DemandSnapshotV1) -> DemandPublishReceiptV1:
        if not isinstance(snapshot, DemandSnapshotV1):
            raise DemandPublishError("demand report is not a schema-v1 snapshot")
        mismatches = tuple(
            field
            for field in _PUBLISH_BINDINGS
            if getattr(snapshot, field) != getattr(self._configuration, field)
        )
        if mismatches:
            raise DemandPublishError(
                f"demand report binding differs from trusted configuration: {', '.join(mismatches)}"
            )
        try:
            payload = canonical_bytes(snapshot)
        except CapacityContractError as exc:
            raise DemandPublishError("demand report exceeds its canonical contract bound") from exc
        endpoint = f"{self._manager_origin}/v1/reports/demand/{self._configuration.subject_id}"
        try:
            response = await self._http.put(
                endpoint,
                content=payload,
                headers={
                    "Authorization": f"Bearer {self._bearer_token}",
                    "Content-Type": "application/json",
                },
            )
        except httpx.HTTPError:
            raise DemandPublishError(
                "capacity manager demand publication transport failed"
            ) from None
        if response.status_code != 200:
            raise DemandPublishError(
                f"capacity manager rejected demand report with status {response.status_code}"
            )
        if len(response.content) > _MAX_RECEIPT_BYTES:
            raise DemandPublishError("capacity manager demand receipt exceeds its byte bound")
        try:
            receipt = DemandPublishReceiptV1.model_validate_json(response.content)
        except (ValidationError, ValueError) as exc:
            raise DemandPublishError("capacity manager returned an invalid demand receipt") from exc
        if receipt.sequence != snapshot.sequence or receipt.digest != canonical_digest(snapshot):
            raise DemandPublishError("capacity manager demand receipt does not match the report")
        return receipt

    async def publish_protected_release(
        self,
        release: PreparedProtectedReleaseV1,
        *,
        idempotency_key: UUID,
    ) -> ProtectedReleasePublishReceiptV1:
        """Publish only a release already committed by the protected local store."""

        if not isinstance(release, PreparedProtectedReleaseV1):
            raise DemandPublishError("protected release is not a schema-v1 acknowledgement")
        if not isinstance(idempotency_key, UUID):
            raise DemandPublishError("protected release idempotency key must be a UUID")
        mismatches = tuple(
            field
            for field in _PUBLISH_BINDINGS
            if getattr(release, field) != getattr(self._configuration, field)
        )
        if mismatches:
            raise DemandPublishError(
                "protected release binding differs from trusted configuration: "
                + ", ".join(mismatches)
            )
        acknowledgement = DryRunProtectedReleaseAcknowledgementV1(
            authority_incarnation=release.manager_authority_incarnation,
            writer_epoch=release.manager_writer_epoch,
            configuration_epoch=release.manager_configuration_epoch,
            allocation_epoch=release.manager_allocation_epoch,
            tranche_id=release.tranche_id,
            shape_instance_id=release.shape_instance_id,
            intent_id=release.submission_intent_id,
            subject_id=release.subject_id,
            subject_incarnation=release.subject_incarnation,
            reporter_incarnation=release.reporter_incarnation,
            deployment_generation=release.deployment_generation,
            pool_id=release.pool_id,
            pool_generation=release.pool_generation,
            bootstrap_registration_epoch=release.bootstrap_registration_epoch,
            protected_registration_epoch=release.protected_registration_epoch,
            bootstrap_revoked=release.bootstrap_revoked,
            protected_release_sha256=guard_canonical_digest(release),
        )
        endpoint = (
            f"{self._manager_origin}/v1/reports/protected-releases/"
            f"{release.subject_id}/{release.shape_instance_id}"
        )
        try:
            response = await self._http.put(
                endpoint,
                content=canonical_bytes(acknowledgement),
                headers={
                    "Authorization": f"Bearer {self._bearer_token}",
                    "Content-Type": "application/json",
                    "Idempotency-Key": str(idempotency_key),
                },
            )
        except httpx.HTTPError:
            raise DemandPublishError(
                "capacity manager protected release transport failed"
            ) from None
        if response.status_code != 200:
            raise DemandPublishError(
                f"capacity manager rejected protected release with status {response.status_code}"
            )
        if len(response.content) > _MAX_RECEIPT_BYTES:
            raise DemandPublishError(
                "capacity manager protected release receipt exceeds its byte bound"
            )
        try:
            receipt = ProtectedReleasePublishReceiptV1.model_validate_json(response.content)
        except (ValidationError, ValueError) as exc:
            raise DemandPublishError(
                "capacity manager returned an invalid protected release receipt"
            ) from exc
        if (
            receipt.shape_instance_id != release.shape_instance_id
            or receipt.acknowledgement_digest != canonical_grant_digest(acknowledgement)
            or receipt.executable is not False
        ):
            raise DemandPublishError(
                "capacity manager protected release receipt does not match the report"
            )
        return receipt

    async def publish_executable_protected_release(
        self,
        publication: PublishableExecutableProtectedReleaseV2,
        *,
        idempotency_key: UUID,
    ) -> ExecutableProtectedReleasePublishReceiptV2:
        """Publish one strict executable protected-release outbox event."""

        if not isinstance(publication, PublishableExecutableProtectedReleaseV2):
            raise DemandPublishError(
                "protected release publication is not a schema-v2 executable report"
            )
        if not isinstance(idempotency_key, UUID):
            raise DemandPublishError("protected release idempotency key must be a UUID")
        release = publication.release
        if not isinstance(release, ExecutableProtectedReleaseV2):
            raise DemandPublishError("protected release publication carries an invalid release")
        if canonical_executable_digest(release) != publication.publication_digest:
            raise DemandPublishError("protected release publication digest changed")
        if release.binding.subject_id != self._configuration.subject_id:
            raise DemandPublishError(
                "protected release binding differs from trusted configuration: subject_id"
            )
        if release.binding.subject_incarnation != self._configuration.subject_incarnation:
            raise DemandPublishError(
                "protected release binding differs from trusted configuration: subject_incarnation"
            )
        if release.reporter_incarnation != self._configuration.reporter_incarnation:
            raise DemandPublishError(
                "protected release binding differs from trusted configuration: reporter_incarnation"
            )
        if release.binding.deployment_generation != self._configuration.deployment_generation:
            raise DemandPublishError(
                "protected release binding differs from trusted configuration: deployment_generation"
            )
        if (
            release.binding.candidate.algorithm != self._configuration.candidate_identity_algorithm
            or release.binding.candidate.identity != self._configuration.candidate_identity
            or release.binding.candidate.publication_sha256
            != self._configuration.candidate_publication_sha256
        ):
            raise DemandPublishError("protected release binding differs from trusted configuration")
        try:
            payload = canonical_executable_bytes(release)
        except CapacityContractError as exc:
            raise DemandPublishError(
                "protected release exceeds its canonical contract bound"
            ) from exc
        endpoint = (
            f"{self._manager_origin}/v2/reports/protected-releases/"
            f"{release.binding.subject_id}/{release.binding.shape_instance_id}"
        )
        try:
            response = await self._http.put(
                endpoint,
                content=payload,
                headers={
                    "Authorization": f"Bearer {self._bearer_token}",
                    "Content-Type": "application/json",
                    "Idempotency-Key": str(idempotency_key),
                },
            )
        except httpx.HTTPError:
            raise DemandPublishError(
                "capacity manager executable protected release transport failed"
            ) from None
        if response.status_code != 200:
            raise DemandPublishError(
                "capacity manager rejected executable protected release "
                f"with status {response.status_code}"
            )
        if len(response.content) > _MAX_RECEIPT_BYTES:
            raise DemandPublishError(
                "capacity manager executable protected release receipt exceeds its byte bound"
            )
        try:
            receipt = ExecutableProtectedReleasePublishReceiptV2.model_validate_json(
                response.content
            )
        except (ValidationError, ValueError) as exc:
            raise DemandPublishError(
                "capacity manager returned an invalid executable protected release receipt"
            ) from exc
        if (
            receipt.intent_id != release.binding.intent_id
            or receipt.protected_release_sha256 != release.protected_release_sha256
        ):
            raise DemandPublishError(
                "capacity manager executable protected release receipt does not match the report"
            )
        if receipt.receipt_digest != _receipt_digest(
            {
                "intent_id": receipt.intent_id,
                "protected_release_sha256": receipt.protected_release_sha256,
                "executable": receipt.executable,
            }
        ):
            raise DemandPublishError(
                "capacity manager executable protected release receipt digest changed"
            )
        return receipt

    async def aclose(self) -> None:
        if self._owns_http:
            await self._http.aclose()

    async def __aenter__(self) -> DemandReporterClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.aclose()


__all__ = [
    "DemandPublishError",
    "DemandPublishReceiptV1",
    "DemandReporterClient",
    "DemandReporterConnection",
    "DemandReporterTLSFiles",
    "ExecutableProtectedReleasePublishReceiptV2",
    "ProtectedReleasePublishReceiptV1",
    "build_reporter_tls_context",
    "canonical_manager_origin",
    "read_owner_only_bearer_token",
    "read_owner_only_bytes",
]
