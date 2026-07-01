"""MinIO / S3 storage helpers for loom_service."""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse, urlunparse

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
