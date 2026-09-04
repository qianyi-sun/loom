"""Bounded, short-lived per-object read capabilities for frozen task bundles."""

from __future__ import annotations

import math
import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal, Protocol
from urllib.parse import parse_qsl, unquote, urlsplit
from uuid import UUID, uuid4

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from loom.task_image_build_plan import (
    MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
    MAX_TASK_IMAGE_BUILD_BUNDLE_FILES,
    TaskImageBuildPlanV1,
)

MAX_TASK_IMAGE_BUNDLE_CAPABILITY_LIFETIME = timedelta(minutes=15)
MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES = 8 * 1024 * 1024
MAX_TASK_IMAGE_BUNDLE_URL_BYTES = 4096

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_BUCKET_RE = re.compile(r"[a-z0-9](?:[a-z0-9.-]{1,61}[a-z0-9])?")


class TaskImageBundleCapabilityError(RuntimeError):
    """Frozen bundle state cannot safely produce a bounded capability."""


@dataclass(frozen=True, slots=True)
class TaskImageBundleObject:
    """Nonsecret object-listing result returned by the injected S3 backend."""

    key: str
    size_bytes: int
    redirect: bool = False


class TaskImageBundleBackend(Protocol):
    def list_objects(
        self,
        *,
        bucket: str,
        prefix: str,
        maximum_objects: int,
    ) -> Sequence[TaskImageBundleObject]: ...

    def presign_get(
        self,
        *,
        bucket: str,
        key: str,
        expires_in_seconds: int,
    ) -> str: ...


def _nonzero_uuid(value: UUID) -> UUID:
    if value.int == 0:
        raise ValueError("bundle capability UUID must be nonzero")
    return value


def _nonzero_digest(value: str) -> str:
    if _DIGEST_RE.fullmatch(value) is None or value == "0" * 64:
        raise ValueError("bundle capability digest must be nonzero lowercase SHA-256")
    return value


NonzeroUUID = Annotated[UUID, AfterValidator(_nonzero_uuid)]
Digest = Annotated[
    str,
    Field(pattern=r"^[0-9a-f]{64}$"),
    AfterValidator(_nonzero_digest),
]


def _relative_path(value: str) -> str:
    if (
        not value
        or len(value) > 4096
        or value.startswith("/")
        or value.endswith("/")
        or "\x00" in value
        or "\\" in value
        or PurePosixPath(value).as_posix() != value
        or any(part in {"", ".", ".."} for part in value.split("/"))
    ):
        raise ValueError("bundle object path is not canonical relative POSIX")
    return value


class TaskImageBundleObjectCapabilityV1(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    relative_path: Annotated[str, Field(min_length=1, max_length=4096)]
    size_bytes: Annotated[int, Field(ge=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES)]
    url: Annotated[
        str,
        Field(min_length=1, max_length=MAX_TASK_IMAGE_BUNDLE_URL_BYTES, repr=False),
    ]

    @field_validator("relative_path")
    @classmethod
    def _path_is_canonical(cls, value: str) -> str:
        return _relative_path(value)


class TaskImageBundleCapabilityV1(BaseModel):
    """Secret-bearing object URLs bound to one current build session."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: Literal["loom.task-image-bundle-capability.v1"] = (
        "loom.task-image-bundle-capability.v1"
    )
    capability_id: NonzeroUUID
    grant_id: NonzeroUUID
    session_id: NonzeroUUID
    session_generation: Annotated[int, Field(gt=0)]
    materialization_id: NonzeroUUID
    task_checksum: Digest
    bundle_file_metadata_sha256: Digest
    file_count: Annotated[int, Field(gt=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_FILES)]
    total_bytes: Annotated[int, Field(ge=0, le=MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES)]
    issued_at: datetime
    expires_at: datetime
    objects: Annotated[
        tuple[TaskImageBundleObjectCapabilityV1, ...],
        Field(
            min_length=1,
            max_length=MAX_TASK_IMAGE_BUILD_BUNDLE_FILES,
            repr=False,
        ),
    ]

    @model_validator(mode="before")
    @classmethod
    def _restore_json_objects(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        normalized = dict(value)
        if isinstance(normalized.get("objects"), list):
            normalized["objects"] = tuple(normalized["objects"])
        for field_name in ("issued_at", "expires_at"):
            candidate = normalized.get(field_name)
            if isinstance(candidate, str):
                try:
                    normalized[field_name] = datetime.fromisoformat(
                        candidate[:-1] + "+00:00" if candidate.endswith("Z") else candidate
                    )
                except ValueError:
                    pass
        return normalized

    @field_validator("issued_at", "expires_at")
    @classmethod
    def _timestamp_is_canonical(cls, value: datetime) -> datetime:
        if value.utcoffset() is None:
            raise ValueError("bundle capability timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def _object_set_is_exact(self) -> TaskImageBundleCapabilityV1:
        lifetime = self.expires_at - self.issued_at
        if lifetime <= timedelta(0) or lifetime > MAX_TASK_IMAGE_BUNDLE_CAPABILITY_LIFETIME:
            raise ValueError("bundle capability lifetime is invalid")
        paths = tuple(item.relative_path for item in self.objects)
        urls = tuple(item.url for item in self.objects)
        if paths != tuple(sorted(paths)) or len(paths) != len(set(paths)):
            raise ValueError("bundle capability objects are not canonical")
        if len(urls) != len(set(urls)):
            raise ValueError("bundle capability URLs are not unique")
        if self.file_count != len(self.objects):
            raise ValueError("bundle capability file count does not match objects")
        if self.total_bytes != sum(item.size_bytes for item in self.objects):
            raise ValueError("bundle capability byte count does not match objects")
        return self


def _origin(value: str) -> tuple[str, str]:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or parsed.hostname is None
    ):
        raise ValueError("bundle public origin must be an origin-only HTTPS URL")
    return parsed.scheme, parsed.netloc


def _bucket(value: str) -> str:
    if (
        _BUCKET_RE.fullmatch(value) is None
        or ".." in value
        or value.startswith("xn--")
        or value.endswith("-s3alias")
    ):
        raise ValueError("bundle expected bucket is invalid")
    return value


class TaskImageBundleCapabilityProvider:
    """List one frozen prefix and mint only exact single-object GET URLs."""

    def __init__(
        self,
        *,
        backend: TaskImageBundleBackend,
        public_https_origin: str,
        expected_bucket: str,
        maximum_objects: int,
        maximum_bytes: int,
        url_expiry_seconds: int,
        capability_id_factory: Callable[[], UUID] = uuid4,
        maximum_capability_bytes: int = MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES,
    ) -> None:
        self._backend = backend
        self._origin = _origin(public_https_origin)
        self._expected_bucket = _bucket(expected_bucket)
        if (
            type(maximum_objects) is not int
            or not 0 < maximum_objects <= MAX_TASK_IMAGE_BUILD_BUNDLE_FILES
        ):
            raise ValueError("bundle maximum object count is invalid")
        if (
            type(maximum_bytes) is not int
            or not 0 < maximum_bytes <= MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES
        ):
            raise ValueError("bundle maximum byte count is invalid")
        if type(url_expiry_seconds) is not int or not 0 < url_expiry_seconds <= 900:
            raise ValueError("bundle URL expiry is invalid")
        if (
            type(maximum_capability_bytes) is not int
            or not 0 < maximum_capability_bytes <= MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES
        ):
            raise ValueError("bundle capability response limit is invalid")
        self._maximum_objects = maximum_objects
        self._maximum_bytes = maximum_bytes
        self._url_expiry_seconds = url_expiry_seconds
        self._capability_id_factory = capability_id_factory
        self._maximum_capability_bytes = maximum_capability_bytes

    def _validated_plan(self, plan: TaskImageBuildPlanV1) -> TaskImageBuildPlanV1:
        try:
            validated = TaskImageBuildPlanV1.model_validate(plan.model_dump(mode="python"))
        except (AttributeError, ValueError):
            raise TaskImageBundleCapabilityError("task-image bundle plan is invalid") from None
        if validated.bundle_bucket != self._expected_bucket:
            raise TaskImageBundleCapabilityError("task-image bundle source is not authorized")
        return validated

    def _listed_objects(
        self,
        plan: TaskImageBuildPlanV1,
    ) -> tuple[tuple[str, TaskImageBundleObject], ...]:
        effective_objects = min(plan.bundle_file_limit, self._maximum_objects)
        effective_bytes = min(plan.bundle_byte_limit, self._maximum_bytes)
        try:
            listed = tuple(
                self._backend.list_objects(
                    bucket=plan.bundle_bucket,
                    prefix=plan.bundle_prefix,
                    maximum_objects=effective_objects + 1,
                )
            )
        except Exception:
            raise TaskImageBundleCapabilityError(
                "task-image bundle listing is unavailable"
            ) from None
        if not listed or len(listed) > effective_objects:
            raise TaskImageBundleCapabilityError("task-image bundle exceeds capability limits")

        objects: list[tuple[str, TaskImageBundleObject]] = []
        seen_paths: set[str] = set()
        total_bytes = 0
        for item in listed:
            if (
                not isinstance(item, TaskImageBundleObject)
                or type(item.size_bytes) is not int
                or item.size_bytes < 0
                or type(item.redirect) is not bool
                or item.redirect
                or not isinstance(item.key, str)
                or not item.key.startswith(plan.bundle_prefix)
            ):
                raise TaskImageBundleCapabilityError("task-image bundle contains an invalid object")
            relative_path = item.key[len(plan.bundle_prefix) :]
            try:
                _relative_path(relative_path)
            except ValueError:
                raise TaskImageBundleCapabilityError(
                    "task-image bundle contains an invalid object"
                ) from None
            if relative_path in seen_paths:
                raise TaskImageBundleCapabilityError("task-image bundle contains an invalid object")
            seen_paths.add(relative_path)
            if item.size_bytes > effective_bytes - total_bytes:
                raise TaskImageBundleCapabilityError("task-image bundle exceeds capability limits")
            total_bytes += item.size_bytes
            objects.append((relative_path, item))
        return tuple(sorted(objects, key=lambda item: item[0]))

    def _validated_url(
        self,
        value: object,
        *,
        key: str,
        now: datetime,
        expires_at: datetime,
    ) -> str:
        if not isinstance(value, str) or len(value.encode("utf-8")) > (
            MAX_TASK_IMAGE_BUNDLE_URL_BYTES
        ):
            raise TaskImageBundleCapabilityError("task-image bundle presigned URL is invalid")
        parsed = urlsplit(value)
        try:
            query_items = parse_qsl(
                parsed.query,
                keep_blank_values=True,
                strict_parsing=True,
                max_num_fields=64,
            )
            query = dict(query_items)
            signed_at = datetime.strptime(
                query["X-Amz-Date"],
                "%Y%m%dT%H%M%SZ",
            ).replace(tzinfo=UTC)
            signed_lifetime = int(query["X-Amz-Expires"])
        except (KeyError, TypeError, ValueError):
            raise TaskImageBundleCapabilityError(
                "task-image bundle presigned URL is invalid"
            ) from None
        if (
            (parsed.scheme, parsed.netloc) != self._origin
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or not parsed.query
            or unquote(parsed.path) != f"/{key}"
            or any(ord(character) < 0x20 for character in value)
            or len(query_items) != len(query)
            or type(signed_lifetime) is not int
            or not 0 < signed_lifetime <= self._url_expiry_seconds
            or signed_at > now
            or signed_at + timedelta(seconds=signed_lifetime) <= now
            or signed_at + timedelta(seconds=signed_lifetime) > expires_at
        ):
            raise TaskImageBundleCapabilityError("task-image bundle presigned URL is invalid")
        return value

    def issue(
        self,
        plan: TaskImageBuildPlanV1,
        *,
        now: datetime,
    ) -> TaskImageBundleCapabilityV1:
        if now.utcoffset() is None:
            raise ValueError("bundle capability issue time must be timezone-aware")
        now = now.astimezone(UTC)
        plan = self._validated_plan(plan)
        remaining_seconds = math.floor((plan.authorization_expires_at - now).total_seconds())
        expiry_seconds = min(self._url_expiry_seconds, remaining_seconds)
        if expiry_seconds <= 0:
            raise TaskImageBundleCapabilityError("task-image bundle authorization expired")
        expires_at = now + timedelta(seconds=expiry_seconds)
        listed = self._listed_objects(plan)

        objects: list[TaskImageBundleObjectCapabilityV1] = []
        for relative_path, item in listed:
            try:
                url = self._backend.presign_get(
                    bucket=plan.bundle_bucket,
                    key=item.key,
                    expires_in_seconds=expiry_seconds,
                )
            except Exception:
                raise TaskImageBundleCapabilityError(
                    "task-image bundle presigning is unavailable"
                ) from None
            objects.append(
                TaskImageBundleObjectCapabilityV1(
                    relative_path=relative_path,
                    size_bytes=item.size_bytes,
                    url=self._validated_url(
                        url,
                        key=item.key,
                        now=now,
                        expires_at=expires_at,
                    ),
                )
            )

        capability = TaskImageBundleCapabilityV1(
            capability_id=_nonzero_uuid(self._capability_id_factory()),
            grant_id=plan.grant_id,
            session_id=plan.session_id,
            session_generation=plan.session_generation,
            materialization_id=plan.materialization_id,
            task_checksum=plan.task_checksum,
            bundle_file_metadata_sha256=plan.bundle_file_metadata_sha256,
            file_count=len(objects),
            total_bytes=sum(item.size_bytes for item in objects),
            issued_at=now,
            expires_at=expires_at,
            objects=tuple(objects),
        )
        if len(capability.model_dump_json().encode("utf-8")) > self._maximum_capability_bytes:
            raise TaskImageBundleCapabilityError(
                "task-image bundle capability response is too large"
            )
        return capability


__all__ = [
    "MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES",
    "MAX_TASK_IMAGE_BUNDLE_CAPABILITY_LIFETIME",
    "MAX_TASK_IMAGE_BUNDLE_URL_BYTES",
    "TaskImageBundleBackend",
    "TaskImageBundleCapabilityError",
    "TaskImageBundleCapabilityProvider",
    "TaskImageBundleCapabilityV1",
    "TaskImageBundleObject",
    "TaskImageBundleObjectCapabilityV1",
]
