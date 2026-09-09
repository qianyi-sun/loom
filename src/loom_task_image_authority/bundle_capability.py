"""Bounded, short-lived per-object read capabilities for frozen task bundles."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import PurePosixPath
from typing import Annotated, Any, Generic, Literal, Protocol, Self, TypeVar, runtime_checkable
from urllib.parse import parse_qsl, unquote, urlsplit
from uuid import UUID, uuid4

from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    field_validator,
    model_validator,
)

from loom.task_image_build_plan import (
    MAX_TASK_IMAGE_BUILD_BUNDLE_BYTES,
    MAX_TASK_IMAGE_BUILD_BUNDLE_FILES,
    TaskImageBuildPlan,
    TaskImageBuildPlanV1,
    TaskImageBuildPlanV2,
    parse_task_image_build_plan,
)
from loom.task_image_bundle_manifest import (
    TaskImageBundleContentManifestV1,
    TaskImageBundleManifestFileV1,
    parse_task_image_bundle_manifest,
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


class TaskImageBundlePresigner(Protocol):
    def presign_get(
        self,
        *,
        bucket: str,
        key: str,
        expires_at: datetime,
    ) -> str:
        """Sign to this exact UTC-second deadline using the actual signing stamp.

        Do not convert the deadline to a duration before delayed signing work.
        Credentials must also remain usable through the requested deadline.
        """
        ...


class TaskImageBundleBackend(TaskImageBundlePresigner, Protocol):
    def list_objects(
        self, *, bucket: str, prefix: str, maximum_objects: int,
    ) -> Sequence[TaskImageBundleObject]: ...


class AsyncTaskImageBundleBackend(TaskImageBundlePresigner, Protocol):
    async def list_objects(
        self, *, bucket: str, prefix: str, maximum_objects: int,
        maximum_bytes: int, expires_at: datetime,
    ) -> Sequence[TaskImageBundleObject]: ...


@runtime_checkable
class VerifiedTaskImageBundleBackend(TaskImageBundlePresigner, Protocol):
    async def get_verified_bundle_manifest(
        self, *, bucket: str, prefix: str, expected_sha256: str,
        task_checksum: str, bundle_file_metadata_sha256: str,
        maximum_objects: int, maximum_bytes: int, expires_at: datetime,
    ) -> TaskImageBundleContentManifestV1:
        """Authenticate the registered manifest and exact complete prefix inventory."""
        ...


_Backend = TypeVar("_Backend", bound=TaskImageBundlePresigner)


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


class TaskImageBundleObjectCapabilityV2(TaskImageBundleObjectCapabilityV1):
    sha256: Digest
    mode: Literal["0644", "0755"]


_Object = TypeVar("_Object", bound=TaskImageBundleObjectCapabilityV1)


class _TaskImageBundleCapability(BaseModel, Generic[_Object]):
    """Secret-bearing object URLs bound to one current build session."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    schema_version: str
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
        tuple[_Object, ...],
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
    def _object_set_is_exact(self) -> Self:
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


class TaskImageBundleCapabilityV1(_TaskImageBundleCapability[TaskImageBundleObjectCapabilityV1]):
    schema_version: Literal["loom.task-image-bundle-capability.v1"] = "loom.task-image-bundle-capability.v1"

    @property
    def content_manifest_digest(self) -> str:
        return ""


class TaskImageBundleCapabilityV2(_TaskImageBundleCapability[TaskImageBundleObjectCapabilityV2]):
    schema_version: Literal["loom.task-image-bundle-capability.v2"] = "loom.task-image-bundle-capability.v2"
    bundle_content_manifest_sha256: Digest

    @property
    def content_manifest_digest(self) -> str:
        return self.bundle_content_manifest_sha256

    @property
    def content_manifest(self) -> TaskImageBundleContentManifestV1:
        # URLs are transport secrets, never part of registered content identity.
        return TaskImageBundleContentManifestV1(
            task_checksum=self.task_checksum,
            bundle_file_metadata_sha256=self.bundle_file_metadata_sha256,
            files=tuple(TaskImageBundleManifestFileV1(
                path=item.relative_path, size_bytes=item.size_bytes, sha256=item.sha256, mode=item.mode,
            ) for item in self.objects),
        )

    @model_validator(mode="after")
    def _registered_descriptors_are_exact(self) -> Self:
        if self.content_manifest.digest != self.bundle_content_manifest_sha256:
            raise ValueError("bundle capability registered content changed")
        return self


TaskImageBundleCapability = Annotated[
    TaskImageBundleCapabilityV1 | TaskImageBundleCapabilityV2, Field(discriminator="schema_version"),
]
_CAPABILITY_ADAPTER: TypeAdapter[TaskImageBundleCapability] = TypeAdapter(TaskImageBundleCapability)


def parse_task_image_bundle_capability(payload: str | bytes) -> TaskImageBundleCapability:
    """Require an explicit version and bound the secret-bearing wire before parsing."""
    if not isinstance(payload, (str, bytes)) or not 0 < len(payload) <= MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES:
        raise ValueError("task-image bundle capability exceeds the response limit")
    if isinstance(payload, str) and len(payload.encode("utf-8")) > MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES:
        raise ValueError("task-image bundle capability exceeds the response limit")
    return _CAPABILITY_ADAPTER.validate_json(payload)


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


class _TaskImageBundleProviderBase(Generic[_Backend]):
    """Shared validation/signing; subclasses own sync versus async listing."""

    def __init__(
        self,
        *,
        backend: _Backend,
        public_https_origin: str,
        expected_bucket: str,
        maximum_objects: int,
        maximum_bytes: int,
        url_expiry_seconds: int,
        capability_id_factory: Callable[[], UUID] = uuid4,
        maximum_capability_bytes: int = MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES,
        clock: Callable[[], datetime] | None = None,
        addressing_style: Literal["bucket-host", "path"] = "bucket-host",
    ) -> None:
        self._backend = backend
        self._origin = _origin(public_https_origin)
        self._expected_bucket = _bucket(expected_bucket)
        if addressing_style not in {"bucket-host", "path"}:
            raise ValueError("bundle addressing style is invalid")
        self._addressing_style = addressing_style
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
        self._clock = clock or (lambda: datetime.now(UTC))

    def _checked_time(self, *, previous: datetime, expires_at: datetime) -> datetime:
        current = self._clock()
        if current.utcoffset() is None or current < previous:
            raise TaskImageBundleCapabilityError("task-image bundle clock is invalid")
        current = current.astimezone(UTC)
        if current >= expires_at:
            raise TaskImageBundleCapabilityError("task-image bundle authorization expired")
        return current

    def _validated_plan(self, plan: TaskImageBuildPlan) -> TaskImageBuildPlan:
        try:
            validated = TaskImageBuildPlanV1.model_validate(plan.model_dump(mode="python"))
        except (AttributeError, ValueError):
            raise TaskImageBundleCapabilityError("task-image bundle plan is invalid") from None
        if validated.bundle_bucket != self._expected_bucket:
            raise TaskImageBundleCapabilityError("task-image bundle source is not authorized")
        return validated

    def _validated_objects(
        self,
        plan: TaskImageBuildPlan,
        listed: Sequence[TaskImageBundleObject],
    ) -> tuple[tuple[str, TaskImageBundleObject], ...]:
        effective_objects = min(plan.bundle_file_limit, self._maximum_objects)
        effective_bytes = min(plan.bundle_byte_limit, self._maximum_bytes)
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
            or unquote(parsed.path) != (
                f"/{self._expected_bucket}/{key}" if self._addressing_style == "path" else f"/{key}"
            )
            or any(ord(character) < 0x20 for character in value)
            or len(query_items) != len(query)
            or type(signed_lifetime) is not int
            or not 0 < signed_lifetime <= self._url_expiry_seconds
            or signed_at > now
            or signed_at + timedelta(seconds=signed_lifetime) <= now
            or signed_at + timedelta(seconds=signed_lifetime) != expires_at
        ):
            raise TaskImageBundleCapabilityError("task-image bundle presigned URL is invalid")
        return value

    def _prepare_issue(
        self,
        plan: TaskImageBuildPlan,
        *,
        now: datetime,
    ) -> tuple[TaskImageBuildPlan, datetime, datetime, datetime]:
        if now.utcoffset() is None:
            raise ValueError("bundle capability issue time must be timezone-aware")
        now = now.astimezone(UTC)
        plan = self._validated_plan(plan)
        # SigV4 timestamps have second precision. One rounded-down absolute
        # deadline binds every object; elapsed I/O cannot extend it or leave
        # the capability advertising validity beyond one object's URL.
        expires_at = min(
            plan.authorization_expires_at,
            now + timedelta(seconds=self._url_expiry_seconds),
        ).replace(microsecond=0)
        if expires_at <= now:
            raise TaskImageBundleCapabilityError("task-image bundle authorization expired")
        observed_at = self._checked_time(previous=now, expires_at=expires_at)
        return plan, now, expires_at, observed_at

    def validate(
        self, capability: TaskImageBundleCapability, plan: TaskImageBuildPlan,
        *, now: datetime,
    ) -> None:
        """Validate new or encrypted replay capabilities against current inputs.

        CPU-only: no listing or signing. Final admission must apply this to a
        concurrent winner as well as the candidate it just generated.
        """
        plan = self._validated_plan(plan)
        try:
            capability = parse_task_image_bundle_capability(capability.model_dump_json())
            if (
                capability.grant_id != plan.grant_id or capability.session_id != plan.session_id
                or capability.session_generation != plan.session_generation
                or capability.materialization_id != plan.materialization_id
                or capability.task_checksum != plan.task_checksum
                or capability.bundle_file_metadata_sha256 != plan.bundle_file_metadata_sha256
                or capability.content_manifest_digest != plan.content_manifest_digest
                or now.utcoffset() is None or capability.issued_at > now
                or capability.expires_at <= now or capability.expires_at > plan.authorization_expires_at
                or capability.expires_at > capability.issued_at + timedelta(seconds=self._url_expiry_seconds)
                or len(capability.model_dump_json().encode("utf-8")) > self._maximum_capability_bytes
            ):
                raise ValueError("capability binding changed")
            self._validated_objects(plan, tuple(
                TaskImageBundleObject(key=plan.bundle_prefix + item.relative_path, size_bytes=item.size_bytes)
                for item in capability.objects
            ))
            for item in capability.objects:
                self._validated_url(item.url, key=plan.bundle_prefix + item.relative_path, now=now, expires_at=capability.expires_at)
        except (ValueError, TypeError, AttributeError):
            raise TaskImageBundleCapabilityError("task-image bundle capability binding is invalid") from None

    def _finish_issue(
        self, plan: TaskImageBuildPlanV1, *, now: datetime, expires_at: datetime,
        observed_at: datetime, listed: tuple[tuple[str, TaskImageBundleObject], ...],
    ) -> TaskImageBundleCapabilityV1:
        objects: list[TaskImageBundleObjectCapabilityV1] = []
        for relative_path, item in listed:
            observed_at = self._checked_time(previous=observed_at, expires_at=expires_at)
            try:
                url = self._backend.presign_get(
                    bucket=plan.bundle_bucket,
                    key=item.key,
                    expires_at=expires_at,
                )
            except Exception:
                raise TaskImageBundleCapabilityError(
                    "task-image bundle presigning is unavailable"
                ) from None
            observed_at = self._checked_time(previous=observed_at, expires_at=expires_at)
            objects.append(
                TaskImageBundleObjectCapabilityV1(
                    relative_path=relative_path,
                    size_bytes=item.size_bytes,
                    url=self._validated_url(
                        url,
                        key=item.key,
                        now=observed_at,
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
        self._checked_time(previous=observed_at, expires_at=expires_at)
        return capability


class TaskImageBundleCapabilityProvider(_TaskImageBundleProviderBase[TaskImageBundleBackend]):
    """Synchronous injected-backend compatibility; native adapter requires async."""

    def issue(self, plan: TaskImageBuildPlan, *, now: datetime) -> TaskImageBundleCapabilityV1:
        plan, now, expires_at, observed_at = self._prepare_issue(plan, now=now)
        assert isinstance(plan, TaskImageBuildPlanV1)  # V1 validation rejects strong plans before I/O.
        try:
            objects = tuple(self._backend.list_objects(
                bucket=plan.bundle_bucket, prefix=plan.bundle_prefix,
                maximum_objects=min(plan.bundle_file_limit, self._maximum_objects) + 1,
            ))
        except Exception:
            raise TaskImageBundleCapabilityError("task-image bundle listing is unavailable") from None
        listed = self._validated_objects(plan, objects)
        observed_at = self._checked_time(previous=observed_at, expires_at=expires_at)
        return self._finish_issue(plan, now=now, expires_at=expires_at, observed_at=observed_at, listed=listed)


class AsyncTaskImageBundleCapabilityProvider(_TaskImageBundleProviderBase[AsyncTaskImageBundleBackend]):
    """Await bounded storage I/O; caller must not retain database authority locks."""

    def _validated_plan(self, plan: TaskImageBuildPlan) -> TaskImageBuildPlan:
        try:
            validated = parse_task_image_build_plan(plan.model_dump_json())
        except (AttributeError, ValueError):
            raise TaskImageBundleCapabilityError("task-image bundle plan is invalid") from None
        if validated.bundle_bucket != self._expected_bucket:
            raise TaskImageBundleCapabilityError("task-image bundle source is not authorized")
        return validated

    async def _issue_registered(
        self, plan: TaskImageBuildPlanV2, *, now: datetime, expires_at: datetime, observed_at: datetime,
    ) -> TaskImageBundleCapabilityV2:
        if not isinstance(self._backend, VerifiedTaskImageBundleBackend):
            raise TaskImageBundleCapabilityError("task-image registered bundle backend is unavailable")
        try:
            manifest = await self._backend.get_verified_bundle_manifest(
                bucket=plan.bundle_bucket, prefix=plan.bundle_prefix,
                expected_sha256=plan.bundle_content_manifest_sha256,
                task_checksum=plan.task_checksum, bundle_file_metadata_sha256=plan.bundle_file_metadata_sha256,
                maximum_objects=min(plan.bundle_file_limit, self._maximum_objects),
                maximum_bytes=min(plan.bundle_byte_limit, self._maximum_bytes), expires_at=expires_at,
            )
            manifest = parse_task_image_bundle_manifest(
                manifest.canonical_bytes, expected_sha256=plan.bundle_content_manifest_sha256,
            )
            if manifest.task_checksum != plan.task_checksum or manifest.bundle_file_metadata_sha256 != plan.bundle_file_metadata_sha256:
                raise ValueError("registered bundle provenance changed")
        except Exception:
            raise TaskImageBundleCapabilityError("task-image registered bundle is unavailable") from None
        observed_at = self._checked_time(previous=observed_at, expires_at=expires_at)
        self._validated_objects(plan, tuple(
            TaskImageBundleObject(key=plan.bundle_prefix + item.path, size_bytes=item.size_bytes)
            for item in manifest.files
        ))
        objects: list[TaskImageBundleObjectCapabilityV2] = []
        for item in manifest.files:
            observed_at = self._checked_time(previous=observed_at, expires_at=expires_at)
            key = plan.bundle_prefix + item.path
            try:
                url = self._backend.presign_get(bucket=plan.bundle_bucket, key=key, expires_at=expires_at)
            except Exception:
                raise TaskImageBundleCapabilityError("task-image bundle presigning is unavailable") from None
            observed_at = self._checked_time(previous=observed_at, expires_at=expires_at)
            objects.append(TaskImageBundleObjectCapabilityV2(
                relative_path=item.path, size_bytes=item.size_bytes, sha256=item.sha256, mode=item.mode,
                url=self._validated_url(url, key=key, now=observed_at, expires_at=expires_at),
            ))
        capability = TaskImageBundleCapabilityV2(
            capability_id=_nonzero_uuid(self._capability_id_factory()),
            grant_id=plan.grant_id, session_id=plan.session_id, session_generation=plan.session_generation,
            materialization_id=plan.materialization_id, task_checksum=plan.task_checksum,
            bundle_file_metadata_sha256=plan.bundle_file_metadata_sha256,
            bundle_content_manifest_sha256=plan.bundle_content_manifest_sha256,
            file_count=len(objects), total_bytes=sum(item.size_bytes for item in objects),
            issued_at=now, expires_at=expires_at, objects=tuple(objects),
        )
        self.validate(capability, plan, now=observed_at)
        self._checked_time(previous=observed_at, expires_at=expires_at)
        return capability

    async def issue(self, plan: TaskImageBuildPlan, *, now: datetime) -> TaskImageBundleCapability:
        plan, now, expires_at, observed_at = self._prepare_issue(plan, now=now)
        if isinstance(plan, TaskImageBuildPlanV2):
            return await self._issue_registered(plan, now=now, expires_at=expires_at, observed_at=observed_at)
        try:
            objects = await self._backend.list_objects(
                bucket=plan.bundle_bucket, prefix=plan.bundle_prefix,
                maximum_objects=min(plan.bundle_file_limit, self._maximum_objects),
                maximum_bytes=min(plan.bundle_byte_limit, self._maximum_bytes),
                expires_at=expires_at,
            )
        except Exception:
            raise TaskImageBundleCapabilityError("task-image bundle listing is unavailable") from None
        listed = self._validated_objects(plan, objects)
        observed_at = self._checked_time(previous=observed_at, expires_at=expires_at)
        return self._finish_issue(plan, now=now, expires_at=expires_at, observed_at=observed_at, listed=listed)


__all__ = [
    "MAX_TASK_IMAGE_BUNDLE_CAPABILITY_BYTES",
    "MAX_TASK_IMAGE_BUNDLE_CAPABILITY_LIFETIME",
    "MAX_TASK_IMAGE_BUNDLE_URL_BYTES",
    "AsyncTaskImageBundleBackend",
    "AsyncTaskImageBundleCapabilityProvider",
    "TaskImageBundleBackend",
    "TaskImageBundleCapability",
    "TaskImageBundleCapabilityError",
    "TaskImageBundleCapabilityProvider",
    "TaskImageBundleCapabilityV1",
    "TaskImageBundleCapabilityV2",
    "TaskImageBundleObject",
    "TaskImageBundleObjectCapabilityV1",
    "TaskImageBundleObjectCapabilityV2",
    "VerifiedTaskImageBundleBackend",
    "parse_task_image_bundle_capability",
]
