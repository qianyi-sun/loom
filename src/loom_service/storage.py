"""MinIO / S3 storage helpers for loom_service.

Currently provides the presigned-URL public-endpoint rewrite so that URLs
generated against the cluster-internal MinIO hostname can be returned as
publicly resolvable URLs when ``LOOM_SVC_MINIO_PUBLIC_ENDPOINT`` is set.
"""

from __future__ import annotations

from urllib.parse import urlparse, urlunparse

from loom_service.config import LoomServiceSettings


def rewrite_to_public(url: str, settings: LoomServiceSettings) -> str:
    """Rewrite a presigned URL's host:port to the public MinIO endpoint.

    If ``settings.minio_public_endpoint`` is *not* set the URL is returned
    unchanged, preserving byte-identical behaviour for existing deployments.

    The query string (``?X-Amz-Algorithm=...``) is preserved verbatim — the
    AWS signature is bound to the path, not the hostname, so only host:port
    replacement is needed when MinIO uses path-style addressing (the default
    Loom configuration).

    Args:
        url: Presigned URL as returned by ``boto3.generate_presigned_url()``.
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
