"""Public URL helpers for one-time browser links."""

from __future__ import annotations

import os
from urllib.parse import urlsplit

from fastapi import Request
from starlette.datastructures import URL


def public_base_url(request: Request) -> str:
    configured = _configured_public_base_url(request)
    if configured:
        return configured

    forwarded = _forwarded_public_base_url(request)
    if forwarded:
        return forwarded

    request_base = getattr(request, "base_url", None)
    if request_base is not None:
        return str(request_base).rstrip("/")
    return ""


def public_url_for(request: Request, name: str, **path_params: object) -> URL:
    """Build a public URL for a named service route.

    ``request.url_for`` sees the service's internal root-mounted ``/api`` path.
    Deployed staging/prod routes are mounted under browser prefixes such as
    ``/dev`` and ``/prod``; ``public_base_url`` carries that browser prefix.
    """

    internal = urlsplit(str(request.url_for(name, **path_params)))
    path = internal.path or ""
    query = f"?{internal.query}" if internal.query else ""
    base = public_base_url(request)
    if not base and internal.scheme and internal.netloc:
        base = f"{internal.scheme}://{internal.netloc}"
    return URL(f"{base}{path}{query}")


def _configured_public_base_url(request: Request) -> str | None:
    app = getattr(request, "app", None)
    state = getattr(app, "state", None)
    settings = getattr(state, "settings", None)
    settings_base = getattr(settings, "public_base_url", None)
    candidates = (
        str(settings_base) if settings_base else None,
        os.environ.get("LOOM_PUBLIC_BASE_URL"),
    )
    for candidate in candidates:
        if candidate and candidate.strip():
            return candidate.strip().rstrip("/")
    return None


def _forwarded_public_base_url(request: Request) -> str | None:
    headers = getattr(request, "headers", {})
    forwarded = headers.get("forwarded")
    if forwarded:
        parsed = _parse_forwarded_header(forwarded)
        if parsed is not None:
            return parsed

    proto = _first_header_value(headers.get("x-forwarded-proto"))
    host = _first_header_value(headers.get("x-forwarded-host"))
    port = _first_header_value(headers.get("x-forwarded-port"))
    if proto is None or host is None:
        return None
    if proto not in {"http", "https"}:
        return None
    return _base_from_forwarded_parts(proto=proto, host=host, port=port)


def _parse_forwarded_header(value: str) -> str | None:
    first_hop = value.split(",", 1)[0]
    parts: dict[str, str] = {}
    for segment in first_hop.split(";"):
        key, sep, raw = segment.partition("=")
        if not sep:
            continue
        parts[key.strip().lower()] = raw.strip().strip('"')
    proto = parts.get("proto")
    host = parts.get("host")
    if proto not in {"http", "https"} or not host:
        return None
    return _base_from_forwarded_parts(proto=proto, host=host, port=None)


def _first_header_value(value: str | None) -> str | None:
    if value is None:
        return None
    first = value.split(",", 1)[0].strip()
    return first or None


def _base_from_forwarded_parts(
    *,
    proto: str,
    host: str,
    port: str | None,
) -> str | None:
    candidate_host = host.strip().strip('"')
    if not candidate_host:
        return None
    if port and ":" not in candidate_host and not _is_default_port(proto, port):
        candidate_host = f"{candidate_host}:{port}"
    parsed = urlsplit(f"{proto}://{candidate_host}")
    if not parsed.netloc:
        return None
    return f"{proto}://{parsed.netloc}"


def _is_default_port(proto: str, port: str) -> bool:
    return (proto == "https" and port == "443") or (proto == "http" and port == "80")
