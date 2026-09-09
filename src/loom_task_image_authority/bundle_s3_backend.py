"""Explicit static-identity MinIO inventories under whole-operation budgets."""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Self

from loom.task_image_build_plan import (
    MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
    MAX_TASK_IMAGE_BUILD_BUNDLE_FILES,
)
from loom_task_image_authority.bundle_capability import (
    TaskImageBundleCapabilityError,
    TaskImageBundleObject,
    _bucket,
)
from loom_task_image_authority.bundle_s3_listing import parse_list_objects_v2
from loom_task_image_authority.bundle_s3_signing import (
    S3SigningCredentials,
    presign_bundle_get,
    presign_bundle_list,
)
from loom_task_image_authority.bundle_s3_transport import (
    HTTPSBundleListingReader,
    S3ListingReadLimits,
)
from loom_task_image_authority.config import _validate_https_origin


@dataclass(frozen=True, slots=True)
class S3InventoryLimits:
    page_size: int = 256
    maximum_pages: int = 16
    maximum_listing_bytes: int = 16 * 1024 * 1024
    total_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        for item, ceiling in zip(fields(self), (1000, 64, 32 * 1024 * 1024, 120.0), strict=True):
            value = getattr(self, item.name)
            if (
                isinstance(item.default, int) and (type(value) is not int or not 0 < value <= ceiling)
            ) or (
                isinstance(item.default, float) and (type(value) is not float or not 0.0 < value <= ceiling)
            ):
                raise TaskImageBundleCapabilityError("S3 inventory limits must be positive and bounded")


_DEFAULT_LIMITS = S3InventoryLimits()
_DEFAULT_READ_LIMITS = S3ListingReadLimits()


class MinioTaskImageBundleBackend:
    """Own a real reader, static credentials and bounded form-decoded inventories.

    Does not certify source immutability or grant/session validity. The API owner
    must release its database locks before awaiting this backend, then perform
    fresh admission before committing capabilities. No ambient identity, refresh,
    background threads, retries, or builder-visible listing authority are used.
    """

    def __init__(
        self, *, origin: str, bucket: str, region: str, credentials: S3SigningCredentials,
        ca_file: Path, limits: S3InventoryLimits = _DEFAULT_LIMITS,
        read_limits: S3ListingReadLimits = _DEFAULT_READ_LIMITS,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        try:
            _validate_https_origin(origin, label="MinIO origin")
            _bucket(bucket)
            if (
                type(region) is not str or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", region) is None
                or type(credentials) is not S3SigningCredentials or credentials.session_token is not None
                or type(limits) is not S3InventoryLimits or (clock is not None and not callable(clock))
            ):
                raise ValueError("invalid configuration")
            credentials.__post_init__()
            limits.__post_init__()
            self._reader = HTTPSBundleListingReader(origin=origin, bucket=bucket, ca_file=ca_file, limits=read_limits)
        except (TypeError, ValueError, AttributeError):
            raise TaskImageBundleCapabilityError("MinIO bundle configuration is invalid") from None
        self._origin = origin
        self._bucket = bucket
        self._region = region
        self._credentials = credentials
        self._limits = limits
        self._clock = clock or (lambda: datetime.now(UTC))
        self._closed = False

    async def __aenter__(self) -> Self:
        self._require_open()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        self._closed = True
        await self._reader.aclose()

    def _require_open(self) -> None:
        if self._closed:
            raise TaskImageBundleCapabilityError("MinIO bundle backend is closed")

    def _observe(self, expires_at: datetime, previous: datetime | None = None) -> datetime:
        self._require_open()
        try:
            now = self._clock()
            if (
                not isinstance(now, datetime) or now.utcoffset() is None
                or not isinstance(expires_at, datetime) or expires_at.utcoffset() is None
                or expires_at.microsecond != 0 or now >= expires_at
                or (previous is not None and now < previous)
            ):
                raise ValueError("invalid authorization clock")
            return now.astimezone(UTC)
        except Exception:
            raise TaskImageBundleCapabilityError("MinIO bundle authorization clock is invalid or expired") from None

    def presign_get(self, *, bucket: str, key: str, expires_at: datetime) -> str:
        self._require_open()
        if bucket != self._bucket:
            raise TaskImageBundleCapabilityError("MinIO bundle bucket is not authorized")
        now = self._observe(expires_at)

        def observe() -> datetime:
            nonlocal now
            now = self._observe(expires_at, now)
            return now

        result = presign_bundle_get(
            public_origin=self._origin, bucket=bucket, key=key, region=self._region,
            credentials=self._credentials, expires_at=expires_at,
            clock=observe,
        )
        observe()
        return result

    async def list_objects(
        self, *, bucket: str, prefix: str, maximum_objects: int,
        maximum_bytes: int, expires_at: datetime,
    ) -> tuple[TaskImageBundleObject, ...]:
        """Return one nonempty complete inventory or fail without partial results."""
        self._require_open()
        if (
            bucket != self._bucket or type(maximum_objects) is not int
            or not 0 < maximum_objects <= MAX_TASK_IMAGE_BUILD_BUNDLE_FILES
            or type(maximum_bytes) is not int or not 0 <= maximum_bytes <= MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES
        ):
            raise TaskImageBundleCapabilityError("MinIO inventory scope or limits are invalid")
        previous = self._observe(expires_at)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + min(self._limits.total_timeout_seconds, (expires_at - previous).total_seconds())

        def observe() -> datetime:
            nonlocal previous
            previous = self._observe(expires_at, previous)
            return previous

        objects: list[TaskImageBundleObject] = []
        tokens: set[str] = set()
        token: str | None = None
        total_bytes = 0
        listing_bytes = 0
        try:
            async with asyncio.timeout_at(deadline) as budget:
                for _ in range(self._limits.maximum_pages):
                    current = observe()
                    # Forward wall-clock movement can shorten a read's authority,
                    # but no elapsed network call can reset the overall budget.
                    deadline = min(deadline, loop.time() + (expires_at - current).total_seconds())
                    maximum_keys = min(self._limits.page_size, maximum_objects - len(objects) + 1)
                    url = presign_bundle_list(
                        public_origin=self._origin, bucket=bucket, prefix=prefix,
                        maximum_keys=maximum_keys, continuation_token=token, region=self._region,
                        credentials=self._credentials, expires_at=expires_at, clock=observe,
                    )
                    current = observe()
                    deadline = min(deadline, loop.time() + (expires_at - current).total_seconds())
                    if loop.time() >= deadline:
                        raise TimeoutError
                    budget.reschedule(deadline)
                    payload = await self._reader.fetch(url, deadline=deadline)
                    observe()
                    listing_bytes += len(payload)
                    if listing_bytes > self._limits.maximum_listing_bytes:
                        raise ValueError("listing byte limit")
                    page = parse_list_objects_v2(
                        payload, expected_bucket=bucket, prefix=prefix, maximum_keys=maximum_keys,
                        continuation_token=token, url_encoding="form",
                    )
                    observe()
                    for item in page.objects:
                        if objects and item.key.encode("utf-8") <= objects[-1].key.encode("utf-8"):
                            raise ValueError("inventory order changed")
                        total_bytes += item.size_bytes
                        if len(objects) >= maximum_objects or total_bytes > maximum_bytes:
                            raise ValueError("inventory exceeds limits")
                        objects.append(item)
                    token = page.next_token
                    if token is None:
                        if not objects:
                            raise ValueError("empty inventory")
                        result = tuple(objects)
                        observe()
                        if loop.time() >= deadline:
                            raise TimeoutError
                        return result
                    if token in tokens or len(objects) >= maximum_objects:
                        raise ValueError("inventory progression exceeds limits")
                    tokens.add(token)
                raise ValueError("listing page limit")
        except TaskImageBundleCapabilityError:
            raise
        except Exception:
            raise TaskImageBundleCapabilityError("MinIO bundle inventory is unavailable or exceeds limits") from None
