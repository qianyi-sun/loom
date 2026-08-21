#!/usr/bin/env python3
"""HTTP smoke checks for executable dev/staging/prod Loom frontend routes.

The check reads only public shells, static assets, and the
`loom-frontend-config.json` document. It does not require live user, admin,
provider, or worker secrets.
"""
from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass
from functools import partial
from html.parser import HTMLParser
from typing import Any, Literal
from urllib.error import HTTPError
from urllib.parse import ParseResult, urljoin, urlparse
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

SECRET_PATTERNS = (
    re.compile(r"authorization:\s*bearer", re.IGNORECASE),
    re.compile(r"\bbearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"[?&](X-Amz-Signature|AWSAccessKeyId|Signature)=", re.IGNORECASE),
    re.compile(r"\bgithub-environment:", re.IGNORECASE),
)
JAVASCRIPT_MIME_ESSENCES = frozenset(
    {
        "application/ecmascript",
        "application/javascript",
        "application/x-ecmascript",
        "application/x-javascript",
        "text/ecmascript",
        "text/javascript",
        "text/javascript1.0",
        "text/javascript1.1",
        "text/javascript1.2",
        "text/javascript1.3",
        "text/javascript1.4",
        "text/javascript1.5",
        "text/jscript",
        "text/livescript",
        "text/x-ecmascript",
        "text/x-javascript",
    }
)
EVIDENCE_MIME_ESSENCES = JAVASCRIPT_MIME_ESSENCES | {
    "application/json",
    "text/css",
    "text/html",
}
CANONICAL_BUILD_ASSET_PATH = re.compile(
    r"^/(?:dev|staging|prod)/assets/"
    r"(?:[A-Za-z0-9._-]+/)*[A-Za-z0-9._-]+\.(?:js|css)$"
)
ENCODED_PATH_SEPARATOR_OR_CONTROL = re.compile(
    r"%(?:0[0-9A-Fa-f]|2[fF]|5[cC])"
)
MAX_SHELL_HTML_BYTES = 1024 * 1024
ROUTE_SMOKE_QUERY = "next=%2Fmonitor&x=1"
SHELL_PATHS = (
    "",
    "monitor",
    "batches/example-id",
    "providers/example-id",
    "library/batches/example-id",
)


@dataclass(frozen=True)
class HttpResponse:
    url: str
    status: int
    headers: dict[str, str | tuple[str, ...]]
    body: bytes


AssetKind = Literal["script", "stylesheet"]
AssetDisposition = Literal["fetch", "ignore", "reject", "malformed"]


@dataclass(frozen=True)
class _AssetReference:
    raw: str
    kind: AssetKind
    has_unsafe_duplicate: bool = False


@dataclass(frozen=True)
class _ClassifiedAssetReference:
    reference: _AssetReference
    disposition: AssetDisposition
    resolved_url: str | None = None


@dataclass(frozen=True)
class _ExtractedAssetReferences:
    references: list[_AssetReference]
    limit_exceeded: bool


@dataclass(frozen=True)
class CheckedResponse:
    url: str
    method: str
    status: int
    content_type: str


@dataclass(frozen=True)
class RouteCheck:
    route_url: str
    config_url: str
    expected_environment: str
    expected_api_base: str
    status: str
    errors: list[str]
    responses: list[CheckedResponse]


def _header_values(response: HttpResponse, name: str) -> tuple[str, ...]:
    wanted = name.lower()
    values: list[str] = []
    for key, value in response.headers.items():
        if key.lower() != wanted:
            continue
        if isinstance(value, str):
            values.append(value)
        else:
            values.extend(value)
    return tuple(values)


def _combined_header(response: HttpResponse, name: str) -> str:
    return ", ".join(_header_values(response, name))


def _mime_essence(content_type: str) -> str:
    return content_type.split(";", 1)[0].strip().lower()


def validate_canonical_redirect(
    *,
    route_url: str,
    response: HttpResponse,
) -> list[str]:
    errors: list[str] = []
    parsed = urlparse(route_url)
    expected_location = f"{parsed.path.rstrip('/')}/"
    if parsed.query:
        expected_location = f"{expected_location}?{parsed.query}"
    if response.status != 308:
        errors.append(f"exact route returned HTTP {response.status}; expected 308")
    locations = _header_values(response, "location")
    if len(locations) != 1:
        errors.append("exact route must return exactly one Location header")
    elif locations[0] != expected_location:
        errors.append(
            f"canonical redirect Location must be {expected_location}",
        )
    return errors


class _AssetReferenceParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.references: list[_AssetReference] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        # HTML parsing keeps the first occurrence of a duplicate attribute.
        # Mirroring that rule prevents the smoke check from validating a later
        # canonical value while the browser consumes an earlier unsafe value.
        attributes: dict[str, str | None] = {}
        duplicate_names: set[str] = set()
        rel_mentions_stylesheet = False
        for name, value in attrs:
            normalized_name = name.lower()
            if normalized_name == "rel" and "stylesheet" in (
                value or ""
            ).lower().split():
                rel_mentions_stylesheet = True
            if normalized_name in attributes:
                duplicate_names.add(normalized_name)
                continue
            attributes[normalized_name] = value
        tag = tag.lower()
        if tag == "script":
            if "src" not in attributes:
                return
            candidate = attributes["src"] or ""
            kind: AssetKind = "script"
            has_unsafe_duplicate = "src" in duplicate_names
        elif tag == "link" and rel_mentions_stylesheet:
            if "href" not in attributes:
                if "rel" in duplicate_names:
                    self.references.append(
                        _AssetReference(
                            raw="",
                            kind="stylesheet",
                            has_unsafe_duplicate=True,
                        )
                    )
                return
            candidate = attributes["href"] or ""
            kind = "stylesheet"
            has_unsafe_duplicate = bool(
                duplicate_names.intersection({"href", "rel"})
            )
        else:
            return
        self.references.append(
            _AssetReference(
                raw=candidate,
                kind=kind,
                has_unsafe_duplicate=has_unsafe_duplicate,
            )
        )

    def handle_startendtag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        self.handle_starttag(tag, attrs)


def _extract_asset_references(
    shell_url: str,
    body: bytes,
) -> _ExtractedAssetReferences:
    # Keep the argument because callers naturally pair the shell and body, but
    # deliberately do not resolve references here. Browser URL resolution can
    # normalize unsafe raw spellings such as ``https:///`` or backslashes.
    del shell_url
    parser = _AssetReferenceParser()
    parser.feed(
        body[:MAX_SHELL_HTML_BYTES].decode("utf-8", errors="surrogateescape"),
    )
    parser.close()
    return _ExtractedAssetReferences(
        references=parser.references,
        limit_exceeded=len(body) > MAX_SHELL_HTML_BYTES,
    )


def _origin(parsed: ParseResult) -> tuple[str, str, int | None]:
    scheme = parsed.scheme.lower()
    port = parsed.port
    if port is None:
        port = {"http": 80, "https": 443}.get(scheme)
    return scheme, (parsed.hostname or "").lower(), port


def _has_unsafe_raw_url_characters(raw: str) -> bool:
    return (
        not raw
        or raw != raw.strip()
        or "\\" in raw
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in raw)
    )


def _has_abnormal_path_syntax(path: str) -> bool:
    return (
        not path.startswith("/")
        or "//" in path
        or ENCODED_PATH_SEPARATOR_OR_CONTROL.search(path) is not None
        or any(segment in {".", ".."} for segment in path.split("/"))
    )


def _classify_asset_reference(
    *,
    route_url: str,
    shell_url: str,
    reference: _AssetReference,
) -> _ClassifiedAssetReference:
    """Classify a raw HTML asset reference before resolving it.

    The route smoke owns same-origin build assets. Clean cross-origin
    stylesheets are left to the browser/security checks (notably #780), while
    scripts, credential-bearing or malformed references, and non-canonical
    same-origin build references fail closed.
    """
    raw = reference.raw
    if reference.has_unsafe_duplicate:
        return _ClassifiedAssetReference(reference, "reject")
    try:
        raw.encode("utf-8")
        route = urlparse(route_url)
        _ = route.port
    except (UnicodeError, ValueError):
        return _ClassifiedAssetReference(reference, "malformed")

    if _has_unsafe_raw_url_characters(raw) or raw.startswith("//"):
        return _ClassifiedAssetReference(reference, "reject")
    if any(pattern.search(raw) for pattern in SECRET_PATTERNS):
        return _ClassifiedAssetReference(reference, "reject")

    is_root_relative = raw.startswith("/")
    is_absolute_https = raw.startswith("https://")
    if not is_root_relative and not is_absolute_https:
        return _ClassifiedAssetReference(reference, "reject")

    try:
        asset = urlparse(raw)
        _ = asset.port
    except (UnicodeError, ValueError):
        return _ClassifiedAssetReference(reference, "malformed")

    if is_root_relative:
        if asset.scheme or asset.netloc:
            return _ClassifiedAssetReference(reference, "reject")
        asset_origin = _origin(route)
    else:
        if asset.scheme != "https" or asset.hostname is None:
            return _ClassifiedAssetReference(reference, "malformed")
        asset_origin = _origin(asset)

    if asset.username is not None or asset.password is not None:
        return _ClassifiedAssetReference(reference, "reject")
    if asset.params or "#" in raw or _has_abnormal_path_syntax(asset.path):
        return _ClassifiedAssetReference(reference, "reject")

    route_origin = _origin(route)
    if asset_origin != route_origin:
        if reference.kind != "stylesheet" or asset.scheme != "https":
            return _ClassifiedAssetReference(reference, "reject")
        return _ClassifiedAssetReference(reference, "ignore")

    if not is_root_relative or "?" in raw:
        return _ClassifiedAssetReference(reference, "reject")
    route_path = route.path.rstrip("/")
    if route_path not in {"/dev", "/staging", "/prod"}:
        return _ClassifiedAssetReference(reference, "reject")
    if (
        not asset.path.startswith(f"{route_path}/assets/")
        or CANONICAL_BUILD_ASSET_PATH.fullmatch(asset.path) is None
    ):
        return _ClassifiedAssetReference(reference, "reject")
    expected_suffix = ".js" if reference.kind == "script" else ".css"
    if not asset.path.endswith(expected_suffix):
        return _ClassifiedAssetReference(reference, "reject")

    # URL resolution is intentionally last: every raw spelling and policy
    # check above must pass before browser-compatible normalization occurs.
    resolved_url = raw if is_absolute_https else urljoin(shell_url, raw)
    return _ClassifiedAssetReference(reference, "fetch", resolved_url)


def _classified_asset_references(
    *,
    route_url: str,
    shell_url: str,
    body: bytes,
) -> tuple[_ExtractedAssetReferences, list[_ClassifiedAssetReference]]:
    extracted = _extract_asset_references(shell_url, body)
    return extracted, [
        _classify_asset_reference(
            route_url=route_url,
            shell_url=shell_url,
            reference=reference,
        )
        for reference in extracted.references
    ]


def _route_url_from_shell(shell_url: str) -> str | None:
    try:
        parsed = urlparse(shell_url)
        _ = parsed.port
    except ValueError:
        return None
    path_parts = [part for part in parsed.path.split("/") if part]
    if not path_parts or path_parts[0] not in {"dev", "prod"}:
        return None
    return parsed._replace(
        path=f"/{path_parts[0]}",
        params="",
        query="",
        fragment="",
    ).geturl()


def extract_asset_urls(shell_url: str, body: bytes) -> list[str]:
    """Return only canonical same-origin build assets safe to fetch."""
    route_url = _route_url_from_shell(shell_url)
    if route_url is None:
        return []
    _, classified = _classified_asset_references(
        route_url=route_url,
        shell_url=shell_url,
        body=body,
    )
    return sorted(
        {
            item.resolved_url
            for item in classified
            if item.disposition == "fetch" and item.resolved_url is not None
        }
    )


def _looks_like_html(body: bytes) -> bool:
    prefix = body[:2048].lstrip()
    if prefix.startswith(b"\xef\xbb\xbf"):
        prefix = prefix[3:].lstrip()
    while prefix.startswith(b"<!--"):
        comment_end = prefix.find(b"-->")
        if comment_end < 0:
            return False
        prefix = prefix[comment_end + 3 :].lstrip()
    prefix = prefix.lower()
    if prefix.startswith(b"<!doctype html"):
        return True
    if re.match(rb"^<(?:html|head|body)(?:\s|>)", prefix):
        return True
    return bool(
        re.match(rb"^<div(?:\s|>)", prefix)
        and re.search(
            rb'''\bid\s*=\s*(?:["']root["']|root(?:\s|/?>))''',
            prefix.split(b">", 1)[0] + b">",
        )
    )


def validate_executable_shell(
    *,
    route_url: str,
    shell: HttpResponse,
    assets: list[HttpResponse],
) -> list[str]:
    errors: list[str] = []
    if shell.status != 200:
        errors.append(f"canonical shell returned HTTP {shell.status}")
    shell_content_types = _header_values(shell, "content-type")
    if len(shell_content_types) != 1:
        errors.append("canonical shell must return exactly one Content-Type header")
    elif _mime_essence(shell_content_types[0]) != "text/html":
        errors.append("canonical shell must return text/html")
    extracted, classified = _classified_asset_references(
        route_url=route_url,
        shell_url=shell.url,
        body=shell.body,
    )
    if extracted.limit_exceeded:
        errors.append("shell exceeds HTML parsing limit")
    safe_refs: dict[str, AssetKind] = {}
    for item in classified:
        if item.disposition == "ignore":
            continue
        if item.disposition == "malformed":
            errors.append("shell contains malformed asset reference")
            continue
        if item.disposition == "reject":
            errors.append("shell contains unsafe asset reference")
            continue
        assert item.resolved_url is not None
        safe_refs[item.resolved_url] = item.reference.kind
    if not safe_refs:
        errors.append("canonical shell contains no same-origin build assets")
    response_urls = {
        response.url for response in assets if response.url in safe_refs
    }
    for ref in safe_refs:
        if ref not in response_urls:
            errors.append(f"asset response missing: {ref}")
    for response in assets:
        kind = safe_refs.get(response.url)
        if kind is None:
            continue
        content_types = _header_values(response, "content-type")
        expected_mime = (
            {"text/css"}
            if kind == "stylesheet"
            else JAVASCRIPT_MIME_ESSENCES
        )
        has_single_content_type = len(content_types) == 1
        if not has_single_content_type:
            errors.append(
                "asset must return exactly one Content-Type header: "
                f"{response.url}"
            )
        if response.status != 200:
            errors.append(f"asset returned HTTP {response.status}: {response.url}")
        elif not has_single_content_type:
            continue
        elif _mime_essence(content_types[0]) == "text/html" or _looks_like_html(
            response.body
        ):
            errors.append(f"asset returned HTML fallback: {response.url}")
        elif _mime_essence(content_types[0]) not in expected_mime:
            errors.append(
                f"asset has unexpected MIME: {response.url}",
            )
    return errors


def _iter_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        out: list[str] = []
        for child in value.values():
            out.extend(_iter_strings(child))
        return out
    if isinstance(value, list):
        out = []
        for child in value:
            out.extend(_iter_strings(child))
        return out
    return []


def _route_path(route_url: str) -> str:
    path = urlparse(route_url).path.rstrip("/")
    return "" if path == "/" else path


def validate_config_document(
    *,
    route_url: str,
    expected_environment: str,
    expected_api_base: str,
    cache_control: str,
    document: dict[str, Any],
) -> list[str]:
    errors: list[str] = []
    route_path = _route_path(route_url)
    if document.get("environment") != expected_environment:
        errors.append(f"environment must be {expected_environment}")
    if document.get("routePath") != route_path:
        errors.append(f"routePath must be {route_path or '/'}")
    if document.get("apiBase") != route_path:
        errors.append(f"apiBase must match routePath {route_path or '/'}")
    if document.get("apiRouteBase") != expected_api_base:
        errors.append(f"apiRouteBase must be {expected_api_base}")

    label = document.get("environmentLabel")
    if not isinstance(label, str) or not label.strip():
        errors.append("environmentLabel must be a non-empty string")
    if expected_environment == "production" and isinstance(label, str):
        if "beta" in label.lower():
            errors.append("production environmentLabel must not contain beta wording")

    cache_directives = {
        directive.strip().lower()
        for directive in cache_control.split(",")
    }
    if "no-store" not in cache_directives:
        errors.append("runtime config response must be no-store")

    for value in _iter_strings(document):
        for pattern in SECRET_PATTERNS:
            if pattern.search(value):
                errors.append("runtime config must not expose secret-looking values")
                return errors
    return errors


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


def _normalized_response_headers(
    headers: Any,
) -> dict[str, str | tuple[str, ...]]:
    collected: dict[str, list[str]] = {}
    get_all = getattr(headers, "get_all", None)
    keys = getattr(headers, "keys", None)
    if callable(get_all) and callable(keys):
        seen: set[str] = set()
        for key in keys():
            normalized = str(key).lower()
            if normalized in seen:
                continue
            seen.add(normalized)
            values = get_all(key) or []
            collected[normalized] = [str(value) for value in values]
    else:
        for key, value in headers.items():
            normalized = str(key).lower()
            if isinstance(value, (list, tuple)):
                collected.setdefault(normalized, []).extend(
                    str(item) for item in value
                )
            else:
                collected.setdefault(normalized, []).append(str(value))
    return {
        key: values[0] if len(values) == 1 else tuple(values)
        for key, values in collected.items()
    }


def _to_http_response(url: str, response: Any) -> HttpResponse:
    return HttpResponse(
        url=response.geturl() or url,
        status=response.getcode(),
        headers=_normalized_response_headers(response.headers),
        body=response.read(),
    )


def _fetch_http(
    url: str,
    *,
    timeout: float,
    method: str = "GET",
    follow_redirects: bool = False,
    ssl_context: ssl.SSLContext | None = None,
) -> HttpResponse:
    request = Request(url, headers={"Accept": "*/*"}, method=method)
    handlers: list[Any] = []
    if not follow_redirects:
        handlers.append(_NoRedirectHandler())
    if ssl_context is not None:
        handlers.append(HTTPSHandler(context=ssl_context))
    open_request = build_opener(*handlers).open
    try:
        with open_request(request, timeout=timeout) as response:
            return _to_http_response(url, response)
    except HTTPError as exc:
        with exc:
            return _to_http_response(url, exc)


def _parse_config_response(response: HttpResponse) -> dict[str, Any]:
    data = json.loads(response.body.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("config document must be a JSON object")
    return data


def _redirect_probe_url(route_url: str) -> str:
    parsed = urlparse(route_url)
    return parsed._replace(query=ROUTE_SMOKE_QUERY, fragment="").geturl()


def _canonical_route_url(route_url: str) -> str:
    parsed = urlparse(route_url)
    return parsed._replace(
        path=f"{parsed.path.rstrip('/')}/",
        params="",
        query="",
        fragment="",
    ).geturl()


def _nested_pseudo_asset_url(route_url: str) -> str:
    parsed = urlparse(route_url)
    return parsed._replace(
        path=(
            f"{parsed.path.rstrip('/')}/batches/assets/"
            "loom-route-smoke-nonexistent.js"
        ),
        params="",
        query="",
        fragment="",
    ).geturl()


def _checked_response(response: HttpResponse, *, method: str) -> CheckedResponse:
    content_types = _header_values(response, "content-type")
    content_type = ""
    if len(content_types) == 1:
        mime_essence = _mime_essence(content_types[0])
        if mime_essence in EVIDENCE_MIME_ESSENCES and not any(
            pattern.search(content_types[0]) for pattern in SECRET_PATTERNS
        ):
            content_type = mime_essence
    return CheckedResponse(
        url=response.url,
        method=method,
        status=response.status,
        content_type=content_type,
    )


def check_route(
    *,
    route_url: str,
    expected_environment: str,
    expected_api_base: str,
    timeout: float,
    fetcher: Callable[..., HttpResponse] | None = None,
    ssl_context: ssl.SSLContext | None = None,
) -> RouteCheck:
    canonical_url = _canonical_route_url(route_url)
    config_url = urljoin(canonical_url, "loom-frontend-config.json")
    fetch = (
        partial(_fetch_http, ssl_context=ssl_context)
        if fetcher is None
        else fetcher
    )
    errors: list[str] = []
    checked: list[CheckedResponse] = []

    def fetch_and_record(
        url: str,
        *,
        method: str = "GET",
        follow_redirects: bool = False,
    ) -> HttpResponse:
        fetched = fetch(
            url,
            timeout=timeout,
            method=method,
            follow_redirects=follow_redirects,
        )
        response = HttpResponse(
            url=url,
            status=fetched.status,
            headers=fetched.headers,
            body=fetched.body,
        )
        checked.append(_checked_response(response, method=method))
        return response

    redirect_url = _redirect_probe_url(route_url)
    for method in ("HEAD", "GET"):
        try:
            redirect = fetch_and_record(redirect_url, method=method)
            errors.extend(
                f"{method} {error}"
                for error in validate_canonical_redirect(
                    route_url=redirect_url,
                    response=redirect,
                )
            )
        except Exception as exc:  # pragma: no cover - operator network failure
            errors.append(f"exact {method} route request failed: {exc}")

    try:
        config = fetch_and_record(config_url)
        if config.status != 200:
            errors.append(f"runtime config returned HTTP {config.status}")
        config_content_types = _header_values(config, "content-type")
        if len(config_content_types) != 1:
            errors.append("runtime config must return exactly one Content-Type header")
        elif _mime_essence(config_content_types[0]) != "application/json":
            errors.append("runtime config must return application/json")
        document = _parse_config_response(config)
        errors.extend(
            validate_config_document(
                route_url=route_url,
                expected_environment=expected_environment,
                expected_api_base=expected_api_base,
                cache_control=_combined_header(config, "cache-control"),
                document=document,
            ),
        )
    except Exception as exc:  # pragma: no cover - operator network failure
        errors.append(f"runtime config request failed: {exc}")

    pseudo_asset_url = _nested_pseudo_asset_url(route_url)
    try:
        pseudo_asset = fetch_and_record(pseudo_asset_url)
        if pseudo_asset.status != 404:
            errors.append(
                "nested pseudo-asset path returned "
                f"HTTP {pseudo_asset.status}; expected 404"
            )
    except Exception as exc:  # pragma: no cover - operator network failure
        errors.append(f"nested pseudo-asset request failed: {exc}")

    shell_responses: list[HttpResponse] = []
    refs_by_shell: dict[str, list[_ClassifiedAssetReference]] = {}
    for shell_url in [
        canonical_url,
        *(urljoin(canonical_url, path) for path in SHELL_PATHS[1:]),
    ]:
        try:
            shell = fetch_and_record(shell_url)
        except Exception as exc:  # pragma: no cover - operator network failure
            errors.append(f"shell request failed for {shell_url}: {exc}")
            continue
        _, refs = _classified_asset_references(
            route_url=route_url,
            shell_url=shell.url,
            body=shell.body,
        )
        shell_responses.append(shell)
        refs_by_shell[shell.url] = refs

    asset_responses: dict[str, HttpResponse] = {}
    asset_urls = sorted(
        {
            ref.resolved_url
            for refs in refs_by_shell.values()
            for ref in refs
            if ref.disposition == "fetch" and ref.resolved_url is not None
        },
    )
    for asset_url in asset_urls:
        try:
            asset_responses[asset_url] = fetch_and_record(asset_url)
        except Exception as exc:  # pragma: no cover - operator network failure
            errors.append(f"asset request failed for {asset_url}: {exc}")

    for shell in shell_responses:
        shell_assets = [
            asset_responses[ref.resolved_url]
            for ref in refs_by_shell[shell.url]
            if ref.resolved_url in asset_responses
        ]
        errors.extend(
            f"{shell.url}: {error}"
            for error in validate_executable_shell(
                route_url=route_url,
                shell=shell,
                assets=shell_assets,
            )
        )

    return RouteCheck(
        route_url=route_url,
        config_url=config_url,
        expected_environment=expected_environment,
        expected_api_base=expected_api_base,
        status="pass" if not errors else "fail",
        errors=errors,
        responses=checked,
    )


def _parse_cli_url(value: str, *, label: str) -> ParseResult:
    try:
        parsed = urlparse(value)
        _ = parsed.port
    except ValueError:
        raise argparse.ArgumentTypeError(f"{label} is malformed") from None
    if parsed.hostname is None:
        raise argparse.ArgumentTypeError(f"{label} is malformed")
    return parsed


def _parse_route(value: str) -> tuple[str, str, str]:
    parts = value.split("=", 2)
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "route must be ENVIRONMENT=ROUTE_URL=EXPECTED_API_BASE",
        )
    environment, route_url, expected_api_base = parts
    if not environment or not route_url.startswith("https://"):
        raise argparse.ArgumentTypeError("route URL must be https")
    parsed_route = _parse_cli_url(route_url, label="route URL")
    if (
        parsed_route.username is not None
        or parsed_route.password is not None
        or parsed_route.query
        or parsed_route.fragment
    ):
        raise argparse.ArgumentTypeError(
            "route URL must not contain userinfo, query, or fragment",
        )
    if not expected_api_base.startswith("https://"):
        raise argparse.ArgumentTypeError("expected API base must be https")
    parsed_api_base = _parse_cli_url(expected_api_base, label="expected API base")
    if (
        parsed_api_base.username is not None
        or parsed_api_base.password is not None
        or parsed_api_base.query
        or parsed_api_base.fragment
    ):
        raise argparse.ArgumentTypeError(
            "expected API base must not contain userinfo, query, or fragment",
        )
    return environment, route_url.rstrip("/"), expected_api_base.rstrip("/")


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify Loom frontend route metadata and API bases.",
    )
    parser.add_argument(
        "--route",
        action="append",
        type=_parse_route,
        required=True,
        metavar="ENV=URL=API_BASE",
        help=(
            "Route to check, for example "
            "production=https://yylx.world/prod=https://yylx.world/prod/api"
        ),
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    checks = [
        check_route(
            route_url=route_url,
            expected_environment=environment,
            expected_api_base=expected_api_base,
            timeout=args.timeout,
            ssl_context=None,
        )
        for environment, route_url, expected_api_base in args.route
    ]
    report = {
        "status": "pass" if all(check.status == "pass" for check in checks) else "fail",
        "routes": [asdict(check) for check in checks],
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for check in checks:
            print(f"{check.status.upper()} {check.route_url} -> {check.expected_api_base}")
            for error in check.errors:
                print(f"  - {error}", file=sys.stderr)
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
