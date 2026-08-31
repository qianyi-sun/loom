"""MinIO / S3 storage helpers for loom_service."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse, urlsplit, urlunparse

from loom.personal_dev_builder_runtime import S3PersonalDevBuildCapabilityProvider
from loom.storage_credentials import build_s3_client
from loom_service.config import LoomServiceSettings


def create_minio_client(
    settings: LoomServiceSettings, *, endpoint_url: str,
) -> Any:
    """Create a boto3 S3 client for the configured MinIO-compatible store.

    Dispatches on ``settings.storage_auth_kind`` via the central
    factory. ``static_keys`` (default) is byte-identical to the
    previous behavior; ``irsa`` lights up automatically on EKS without
    further code changes.
    """
    return build_s3_client(
        endpoint_url=endpoint_url,
        auth_kind=settings.storage_auth_kind,
        access_key=settings.minio_access_key.get_secret_value(),
        secret_key=settings.minio_secret_key.get_secret_value(),
        region=settings.minio_region,
    )


def create_minio_presign_client(settings: LoomServiceSettings) -> Any:
    """Create the legacy client for MinIO presigned GET URLs.

    SigV4 presigned URLs bind the request ``Host`` header through
    ``X-Amz-SignedHeaders=host``. If Loom serves API callers outside the
    cluster, the URL must therefore be signed with the public endpoint the
    caller will use, not signed internally and rewritten afterward. Trial
    detail downloads are service-proxied and do not use this client.
    """
    return create_minio_client(
        settings,
        endpoint_url=settings.minio_public_endpoint or settings.minio_endpoint,
    )


def create_personal_dev_native_builder_presign_client(
    settings: LoomServiceSettings,
) -> Any:
    """Create the native-builder capability client for one exact public origin."""
    configured = settings.minio_public_endpoint
    value = str(configured) if configured is not None else ""
    try:
        parsed = urlsplit(value)
        valid_port = parsed.port
    except ValueError:
        parsed = None
        valid_port = None
    if (
        parsed is None
        or not 1 <= len(value) <= 2048
        or parsed.scheme != "https"
        or not parsed.netloc
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or (valid_port is not None and not 1 <= valid_port <= 65535)
        or any(character in value for character in "\r\n\0")
    ):
        raise RuntimeError("personal-dev native builder public object-store origin is invalid")
    return create_minio_client(settings, endpoint_url=value)


def configure_personal_dev_native_builder_storage(
    app_state: Any,
    settings: LoomServiceSettings,
) -> None:
    """Install public capability state only for explicitly enabled native mode."""
    if not getattr(settings, "personal_dev_native_builder_enabled", False):
        return
    public_client = create_personal_dev_native_builder_presign_client(settings)
    if public_client is getattr(app_state, "minio_client", None):
        raise RuntimeError(
            "personal-dev native builder public presign client must be separate"
        )
    try:
        capabilities = S3PersonalDevBuildCapabilityProvider(
            object_store=public_client,
            expected_bucket=settings.artifacts_bucket,
            expiry_seconds=settings.personal_dev_builder_lease_sec,
            max_artifact_bytes=settings.personal_dev_builder_max_artifact_bytes,
        )
    except Exception:
        close = getattr(public_client, "close", None)
        if callable(close):
            close()
        raise
    app_state.personal_dev_native_builder_presign_client = public_client
    app_state.personal_dev_native_builder_capabilities = capabilities
    app_state._owned_personal_dev_native_builder_presign_client = public_client


def get_minio_presign_client(app_state: Any) -> Any:
    """Return the legacy presign client, falling back for older fixtures."""
    return getattr(app_state, "minio_presign_client", app_state.minio_client)


def rewrite_to_public(url: str, settings: LoomServiceSettings) -> str:
    """Rewrite a URL's host:port to the public MinIO endpoint.

    This helper is retained for non-SigV4 and legacy callers. Trial detail
    downloads are service-proxied and should not use this helper.

    If ``settings.minio_public_endpoint`` is *not* set the URL is returned
    unchanged, preserving byte-identical behaviour for existing deployments.

    Args:
        url: URL to rewrite.
        settings: Loaded ``LoomServiceSettings`` instance.

    Returns:
        The original URL with its netloc replaced by the public endpoint's
        netloc (scheme + host + port), or the original URL if no public
        endpoint is configured.
    """
    if not settings.minio_public_endpoint:
        return url

    parsed = urlparse(url)
    public = urlparse(settings.minio_public_endpoint)
    # Use the public scheme; fall back to the original scheme if the public
    # endpoint was provided without one (unlikely but safe).
    new_scheme = public.scheme or parsed.scheme
    new_netloc = public.netloc or parsed.netloc
    return urlunparse(parsed._replace(scheme=new_scheme, netloc=new_netloc))
