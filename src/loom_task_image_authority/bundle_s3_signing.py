"""CPU-only S3 object/list signing; no ambient credentials or network I/O."""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import quote

from botocore.auth import S3SigV4QueryAuth
from botocore.awsrequest import AWSRequest
from botocore.credentials import ReadOnlyCredentials

from loom_task_image_authority.bundle_capability import (
    MAX_TASK_IMAGE_BUNDLE_URL_BYTES,
    TaskImageBundleCapabilityError,
    _bucket,
    _relative_path,
)
from loom_task_image_authority.bundle_s3_listing import _token
from loom_task_image_authority.config import _validate_https_origin


@dataclass(frozen=True, slots=True)
class S3SigningCredentials:
    """Explicit validated identity snapshot, never a default credential chain."""

    access_key: str = field(repr=False)
    secret_key: str = field(repr=False)
    session_token: str | None = field(default=None, repr=False)
    expires_at: datetime | None = None

    def __post_init__(self) -> None:
        for value, maximum, optional in (
            (self.access_key, 256, False), (self.secret_key, 4096, False),
            (self.session_token, 4096, True),
        ):
            if optional and value is None:
                continue
            if (
                type(value) is not str or not 0 < len(value) <= maximum
                or not value.isascii() or any(ord(char) < 33 or ord(char) == 127 for char in value)
            ):
                raise TaskImageBundleCapabilityError("S3 signing credentials are invalid")
        if (
            (self.session_token is not None and self.expires_at is None)
            or (self.expires_at is not None and (
                not isinstance(self.expires_at, datetime) or self.expires_at.utcoffset() is None
            ))
        ):
            raise TaskImageBundleCapabilityError("S3 signing credential expiry is unavailable")


class _DeadlineQueryAuth(S3SigV4QueryAuth):  # type: ignore[misc]
    def __init__(
        self, *, credentials: S3SigningCredentials, region: str,
        expires_at: datetime, clock: Callable[[], datetime],
    ) -> None:
        super().__init__(
            ReadOnlyCredentials(credentials.access_key, credentials.secret_key, credentials.session_token),
            "s3", region,
        )
        self._deadline = expires_at
        self._clock = clock

    def add_auth(self, request: Any) -> None:
        # Own the actual signing timestamp once. Calling the base add_auth would
        # sample it again AND debug-log credential-bearing canonical queries and
        # signatures. Reuse botocore's S3 canonicalization/HMAC, never those logs.
        now = self._clock()
        if not isinstance(now, datetime) or now.utcoffset() is None:
            raise TaskImageBundleCapabilityError("S3 signing clock is invalid")
        now = now.astimezone(UTC)
        stamp = now.replace(microsecond=0)
        self._expires = int((self._deadline - stamp).total_seconds())
        if now >= self._deadline or not 0 < self._expires <= 900:
            raise TaskImageBundleCapabilityError("S3 signing deadline is invalid")
        request.context["timestamp"] = stamp.strftime("%Y%m%dT%H%M%SZ")
        self._modify_request_before_signing(request)
        canonical = self.canonical_request(request)
        signature = self.signature(self.string_to_sign(request, canonical), request)
        self._inject_signature_to_request(request, signature)


def presign_bundle_get(
    *, public_origin: str, bucket: str, key: str, region: str,
    credentials: S3SigningCredentials, expires_at: datetime,
    clock: Callable[[], datetime] | None = None,
) -> str:
    """Sign exact path-style /bucket/key at its public origin to a fixed deadline.

    This function does not prove object existence, immutable source provenance,
    network routing, IAM scope or live credential revocation. The async backend
    owns those boundaries and must not put these credentials into allocations.
    """
    try:
        _validate_https_origin(public_origin, label="S3 public origin")
        _bucket(bucket)
        _relative_path(key)
        if (
            len(key.encode("utf-8")) > 1024
            or any(ord(char) < 32 or ord(char) == 127 for char in key)
            or type(region) is not str or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", region) is None
        ):
            raise ValueError("invalid target")
    except (TypeError, ValueError, AttributeError):
        raise TaskImageBundleCapabilityError("S3 signing target is invalid") from None
    return _presign(
        AWSRequest(method="GET", url=f"{public_origin}/{bucket}/{quote(key, safe='/~')}"),
        credentials=credentials, region=region, expires_at=expires_at, clock=clock,
    )


def presign_bundle_list(
    *, public_origin: str, bucket: str, prefix: str, maximum_keys: int,
    region: str, credentials: S3SigningCredentials, expires_at: datetime,
    continuation_token: str | None = None, clock: Callable[[], datetime] | None = None,
) -> str:
    """Sign one exact path-style ListObjectsV2 request for authority-only I/O.

    This bearer URL must never be included in a builder's bundle capability.
    It carries the same immutable deadline as the eventual object GETs; the
    async owner must enforce one total budget and pagination progress separately.
    """
    try:
        _validate_https_origin(public_origin, label="S3 public origin")
        _bucket(bucket)
        if (
            type(prefix) is not str or not prefix.endswith("/")
            or len(prefix.encode("utf-8")) > 1024
            or any(ord(char) < 32 or ord(char) == 127 for char in prefix)
            or type(maximum_keys) is not int or not 1 <= maximum_keys <= 1000
            or type(region) is not str or re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", region) is None
        ):
            raise ValueError("invalid target")
        _relative_path(prefix[:-1])
        _token(continuation_token)
    except (TypeError, ValueError, AttributeError):
        raise TaskImageBundleCapabilityError("S3 listing target is invalid") from None
    params = {"list-type": "2", "prefix": prefix, "max-keys": str(maximum_keys), "encoding-type": "url"}
    if continuation_token is not None:
        params["continuation-token"] = continuation_token
    return _presign(
        AWSRequest(method="GET", url=f"{public_origin}/{bucket}", params=params),
        credentials=credentials, region=region, expires_at=expires_at, clock=clock,
    )


def _presign(
    request: Any, *, credentials: S3SigningCredentials, region: str,
    expires_at: datetime, clock: Callable[[], datetime] | None,
) -> str:
    if (
        not isinstance(expires_at, datetime) or expires_at.utcoffset() is None
        or expires_at.microsecond != 0
    ):
        raise TaskImageBundleCapabilityError("S3 signing deadline is invalid")
    if type(credentials) is not S3SigningCredentials:
        raise TaskImageBundleCapabilityError("S3 signing credentials are invalid")
    credentials.__post_init__()
    if credentials.expires_at is not None and credentials.expires_at < expires_at:
        raise TaskImageBundleCapabilityError("S3 signing credentials expire before the deadline")
    try:
        _DeadlineQueryAuth(
            credentials=credentials, region=region, expires_at=expires_at.astimezone(UTC),
            clock=clock or (lambda: datetime.now(UTC)),
        ).add_auth(request)
    except TaskImageBundleCapabilityError:
        raise
    except Exception:
        raise TaskImageBundleCapabilityError("S3 signing is unavailable") from None
    result = request.url
    if not isinstance(result, str) or len(result.encode("utf-8")) > MAX_TASK_IMAGE_BUNDLE_URL_BYTES:
        raise TaskImageBundleCapabilityError("S3 signed URL exceeds the capability limit")
    return result
