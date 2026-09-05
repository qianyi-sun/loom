"""Repository-bound HTTPS OCI reader with fixed HTTP/1.1 transport authority.

httpcore's HTTP/1.1 implementation rejects an incomplete header event above
100 KiB before parsing. This reader additionally applies a lower configurable
limit to the complete parsed response header block. HTTP/2 is disabled so that
the reviewed HTTP/1.1 pre-parse bound remains the only protocol path.
"""

from __future__ import annotations

import asyncio
import ssl
from collections.abc import AsyncGenerator, Awaitable
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import TracebackType
from typing import Literal, Self, TypeVar
from uuid import uuid4

import httpx

from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    TaskImageAuthoritySettings,
)
from loom_task_image_authority.oci_verification import OCIDescriptor
from loom_task_image_authority.registry_token import (
    DistributionRegistryTokenIssuer,
    _validate_repository,
    load_distribution_registry_token_issuer,
)

_TOKEN_LIFETIME = timedelta(seconds=45)
_USER_AGENT = "loom-task-image-verifier/1"
_T = TypeVar("_T")


async def _before_deadline(awaitable: Awaitable[_T], deadline: float) -> _T:
    remaining = deadline - asyncio.get_running_loop().time()
    return await asyncio.wait_for(awaitable, timeout=max(0.0, remaining))


class RegistryReadError(RuntimeError):
    """A safe transport failure without response bytes or bearer credentials."""

    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


@dataclass(frozen=True, slots=True)
class RegistryReaderLimits:
    connect_timeout_seconds: float = 5.0
    idle_timeout_seconds: float = 10.0
    total_timeout_seconds: float = 120.0
    maximum_response_header_bytes: int = 32 * 1024
    maximum_chunk_bytes: int = 1024 * 1024
    maximum_manifest_bytes: int = 4 * 1024**2
    maximum_response_bytes: int = 100 * 1024**3
    maximum_concurrent_reads: int = 4

    def __post_init__(self) -> None:
        ceilings: dict[str, float | int] = {
            "connect_timeout_seconds": 30.0,
            "idle_timeout_seconds": 60.0,
            "total_timeout_seconds": 600.0,
            "maximum_response_header_bytes": 64 * 1024,
            "maximum_chunk_bytes": 1024 * 1024,
            "maximum_manifest_bytes": 4 * 1024**2,
            "maximum_response_bytes": 100 * 1024**3,
            "maximum_concurrent_reads": 32,
        }
        for item in fields(self):
            value = getattr(self, item.name)
            ceiling = ceilings[item.name]
            if (
                isinstance(item.default, float)
                and (type(value) is not float or not 0.0 < value <= ceiling)
            ) or (
                isinstance(item.default, int)
                and (type(value) is not int or not 0 < value <= ceiling)
            ):
                raise ValueError("registry reader limits must be positive and bounded")


_DEFAULT_LIMITS = RegistryReaderLimits()


class HTTPSRegistryReader:
    """Read only exact digests from one validated Distribution repository."""

    def __init__(
        self,
        *,
        repository: str,
        token_issuer: DistributionRegistryTokenIssuer,
        ca_file: Path,
        limits: RegistryReaderLimits = _DEFAULT_LIMITS,
    ) -> None:
        self._repository = _validate_repository(repository)
        if type(token_issuer) is not DistributionRegistryTokenIssuer:
            raise TypeError("fixed registry token issuer is required")
        if not isinstance(ca_file, Path):
            raise TypeError("registry CA file path is required")
        if type(limits) is not RegistryReaderLimits:
            raise TypeError("registry reader limits are required")
        self._token_issuer = token_issuer
        self._limits = limits
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.verify_mode = ssl.CERT_REQUIRED
        context.check_hostname = True
        try:
            context.load_verify_locations(cafile=str(ca_file))
        except (OSError, ssl.SSLError) as exc:
            raise ValueError("registry CA file is invalid") from exc
        connection_limits = httpx.Limits(
            max_connections=limits.maximum_concurrent_reads,
            max_keepalive_connections=limits.maximum_concurrent_reads,
        )
        transport = httpx.AsyncHTTPTransport(
            verify=context,
            trust_env=False,
            http1=True,
            http2=False,
            limits=connection_limits,
            retries=0,
        )
        self._client = httpx.AsyncClient(
            base_url=token_issuer.registry_origin,
            transport=transport,
            timeout=httpx.Timeout(
                connect=limits.connect_timeout_seconds,
                read=limits.idle_timeout_seconds,
                write=limits.connect_timeout_seconds,
                pool=limits.connect_timeout_seconds,
            ),
            follow_redirects=False,
            trust_env=False,
            headers={"User-Agent": _USER_AGENT},
        )
        self._semaphore = asyncio.Semaphore(limits.maximum_concurrent_reads)
        self._closed = False

    async def __aenter__(self) -> Self:
        if self._closed:
            raise RuntimeError("registry reader is closed")
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if not self._closed:
            self._closed = True
            await self._client.aclose()

    async def read(
        self,
        kind: Literal["manifest", "blob"],
        descriptor: OCIDescriptor,
    ) -> AsyncGenerator[bytes, None]:
        if self._closed:
            raise RuntimeError("registry reader is closed")
        if kind not in {"manifest", "blob"}:
            raise ValueError("registry object kind is invalid")
        if type(descriptor) is not OCIDescriptor:
            raise TypeError("exact OCI descriptor is required")
        if descriptor.size > self._limits.maximum_response_bytes:
            raise RegistryReadError("registry response size limit exceeded", retryable=False)
        if kind == "manifest" and descriptor.size > self._limits.maximum_manifest_bytes:
            raise RegistryReadError("registry manifest size limit exceeded", retryable=False)

        collection = "manifests" if kind == "manifest" else "blobs"
        path = f"/v2/{self._repository}/{collection}/{descriptor.digest}"
        deadline = asyncio.get_running_loop().time() + self._limits.total_timeout_seconds
        response: httpx.Response | None = None
        acquired = False
        try:
            await _before_deadline(self._semaphore.acquire(), deadline)
            acquired = True
            now = datetime.now(UTC).replace(microsecond=0)
            issued = self._token_issuer.issue_pull(
                credential_id=uuid4(),
                repository=self._repository,
                issued_at=now,
                expires_at=now + _TOKEN_LIFETIME,
            )
            request = self._client.build_request(
                "GET",
                path,
                headers={
                    "Accept": descriptor.media_type,
                    "Accept-Encoding": "identity",
                    "Authorization": f"Bearer {issued.token}",
                },
            )
            response = await _before_deadline(
                self._client.send(request, stream=True),
                deadline,
            )
            self._validate_response(response, kind, descriptor)
            observed = 0
            stream = response.aiter_raw()
            while True:
                try:
                    chunk = await _before_deadline(anext(stream), deadline)
                except StopAsyncIteration:
                    break
                if not chunk:
                    continue
                if len(chunk) > self._limits.maximum_chunk_bytes:
                    raise RegistryReadError(
                        "registry response chunk limit exceeded",
                        retryable=False,
                    )
                observed += len(chunk)
                if (
                    observed > descriptor.size
                    or observed > self._limits.maximum_response_bytes
                ):
                    raise RegistryReadError(
                        "registry response size limit exceeded",
                        retryable=False,
                    )
                yield chunk
            if observed != descriptor.size:
                raise RegistryReadError(
                    "registry response size mismatch",
                    retryable=False,
                )
        except TimeoutError:
            raise RegistryReadError("registry total timeout", retryable=True) from None
        except httpx.TimeoutException:
            raise RegistryReadError("registry transport timeout", retryable=True) from None
        except httpx.HTTPError:
            raise RegistryReadError("registry transport failure", retryable=True) from None
        finally:
            try:
                if response is not None:
                    await response.aclose()
            finally:
                if acquired:
                    self._semaphore.release()

    def _validate_response(
        self,
        response: httpx.Response,
        kind: Literal["manifest", "blob"],
        descriptor: OCIDescriptor,
    ) -> None:
        header_bytes = 2 + sum(
            len(name) + 2 + len(value) + 2 for name, value in response.headers.raw
        )
        if header_bytes > self._limits.maximum_response_header_bytes:
            raise RegistryReadError("registry response header limit exceeded", retryable=False)
        if response.status_code != 200:
            retryable = response.status_code in {401, 408, 425, 429} or 500 <= response.status_code
            raise RegistryReadError("registry rejected digest GET", retryable=retryable)
        content_encoding = response.headers.get("Content-Encoding")
        if content_encoding is not None and content_encoding.lower().strip() != "identity":
            raise RegistryReadError("registry response encoding is unsupported", retryable=False)
        content_length = response.headers.get("Content-Length")
        if content_length is not None and (
            not content_length.isascii()
            or not content_length.isdecimal()
            or content_length != str(descriptor.size)
        ):
            raise RegistryReadError("registry response size mismatch", retryable=False)
        content_digest = response.headers.get("Docker-Content-Digest")
        if content_digest is not None and content_digest != descriptor.digest:
            raise RegistryReadError("registry response digest header mismatch", retryable=False)
        content_type = response.headers.get("Content-Type")
        expected_content_type = (
            descriptor.media_type if kind == "manifest" else "application/octet-stream"
        )
        if content_type is not None and content_type != expected_content_type:
            raise RegistryReadError("registry response media type mismatch", retryable=False)


def load_https_registry_reader(
    settings: TaskImageAuthoritySettings,
    repository: str,
) -> HTTPSRegistryReader:
    """Load an optional reader only when its fixed CA and signer are configured."""

    if type(settings) is not TaskImageAuthoritySettings:
        raise TypeError("task-image authority settings are required")
    if settings.registry_reader_ca_file is None:
        raise TaskImageAuthorityConfigurationError(
            "registry reader configuration is unavailable"
        )
    return HTTPSRegistryReader(
        repository=repository,
        token_issuer=load_distribution_registry_token_issuer(settings),
        ca_file=settings.registry_reader_ca_file,
        limits=RegistryReaderLimits(
            connect_timeout_seconds=settings.registry_connect_timeout_seconds,
            idle_timeout_seconds=settings.registry_idle_timeout_seconds,
            total_timeout_seconds=settings.registry_total_timeout_seconds,
            maximum_response_header_bytes=settings.registry_maximum_response_header_bytes,
            maximum_chunk_bytes=settings.registry_maximum_chunk_bytes,
            maximum_manifest_bytes=settings.registry_maximum_manifest_bytes,
            maximum_response_bytes=settings.registry_maximum_response_bytes,
            maximum_concurrent_reads=settings.registry_read_concurrency_limit,
        ),
    )


__all__ = [
    "HTTPSRegistryReader",
    "RegistryReadError",
    "RegistryReaderLimits",
    "load_https_registry_reader",
]
