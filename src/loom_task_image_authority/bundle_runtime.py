"""Lifespan-owned native bundle provider with an explicit private identity."""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from loom_task_image_authority.bundle_capability import AsyncTaskImageBundleCapabilityProvider
from loom_task_image_authority.bundle_s3_backend import MinioTaskImageBundleBackend
from loom_task_image_authority.bundle_s3_signing import S3SigningCredentials
from loom_task_image_authority.config import (
    TaskImageAuthorityConfigurationError,
    TaskImageAuthoritySettings,
    read_owner_only_bytes,
)


def load_bundle_credentials(path: Path) -> S3SigningCredentials:
    """One bounded, stable owner-only file; no refresh or ambient credential chain."""

    def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate field")
            result[key] = value
        return result

    try:
        raw = read_owner_only_bytes(path, max_bytes=16 * 1024)
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
        if (
            type(document) is not dict
            or set(document) != {"schema_version", "access_key", "secret_key"}
            or type(document["schema_version"]) is not int or document["schema_version"] != 1
        ):
            raise ValueError("invalid schema")
        return S3SigningCredentials(access_key=document["access_key"], secret_key=document["secret_key"])
    except Exception:
        raise TaskImageAuthorityConfigurationError("native bundle credentials are invalid") from None


@asynccontextmanager
async def configured_bundle_provider(
    settings: TaskImageAuthoritySettings, *, clock: Callable[[], datetime] | None = None,
) -> AsyncIterator[AsyncTaskImageBundleCapabilityProvider | None]:
    """Own construction and shutdown; disabled settings never create a backend."""
    # Reject model_copy/model_construct callers that bypass the settings contract.
    settings = TaskImageAuthoritySettings(**settings.model_dump(mode="python"))
    if settings.bundle_backend == "disabled":
        yield None
        return
    assert settings.bundle_public_https_origin is not None
    assert settings.bundle_expected_bucket is not None
    assert settings.bundle_region is not None
    assert settings.bundle_credentials_file is not None
    assert settings.bundle_reader_ca_file is not None
    backend = MinioTaskImageBundleBackend(
        origin=settings.bundle_public_https_origin, bucket=settings.bundle_expected_bucket,
        region=settings.bundle_region, credentials=load_bundle_credentials(settings.bundle_credentials_file),
        ca_file=settings.bundle_reader_ca_file, clock=clock,
    )
    try:
        yield AsyncTaskImageBundleCapabilityProvider(
            backend=backend, public_https_origin=settings.bundle_public_https_origin,
            expected_bucket=settings.bundle_expected_bucket, maximum_objects=settings.bundle_maximum_objects,
            maximum_bytes=settings.bundle_maximum_bytes, url_expiry_seconds=settings.bundle_url_expiry_seconds,
            addressing_style="path", clock=clock,
        )
    finally:
        await backend.aclose()
