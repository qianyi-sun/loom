#!/usr/bin/env python3
"""Validate Loom frontend security headers without recording response bodies."""

from __future__ import annotations

import argparse
import json
import re
import ssl
import sys
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from functools import partial
from typing import Any
from urllib.error import HTTPError
from urllib.parse import ParseResult, urljoin, urlparse
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener

CSP_POLICY = (
    "default-src 'self'; "
    "base-uri 'self'; "
    "object-src 'none'; "
    "frame-ancestors 'none'; "
    "form-action 'self'; "
    "script-src 'self'; "
    "style-src 'self'; "
    "font-src 'self'; "
    "connect-src 'self'; "
    "img-src 'self' data: blob:; "
    "media-src 'self' blob:; "
    "worker-src 'self' blob:; "
    "manifest-src 'self'"
)
WEB_ORIGIN_HEADERS: Mapping[str, str] = {
    "content-security-policy": CSP_POLICY,
    "x-content-type-options": "nosniff",
    "referrer-policy": "no-referrer",
    "permissions-policy": "camera=(), microphone=(), geolocation=()",
}
HSTS_VALUE = "max-age=31536000; includeSubDomains"
ROUTE_PROBE_QUERY = "next=%2Fmonitor&x=1"
PROBE_LABEL = re.compile(r"[a-z][a-z0-9_-]{0,63}")


@dataclass(frozen=True)
class HttpResponse:
    status: int
    headers: Mapping[str, str | tuple[str, ...]]


@dataclass(frozen=True)
class Probe:
    label: str
    url: str
    expected_status: int
    expected_location: str | None = None


@dataclass(frozen=True)
class ProbeResult:
    label: str
    url: str
    expected_status: int
    observed_status: int | None
    status: str
    errors: tuple[str, ...]


class _NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


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


def validate_security_headers(
    response: HttpResponse,
    *,
    require_hsts: bool,
) -> list[str]:
    """Return redacted exact-policy errors for one frontend response."""
    errors: list[str] = []
    expected = dict(WEB_ORIGIN_HEADERS)
    if require_hsts:
        expected["strict-transport-security"] = HSTS_VALUE
    for header_name, expected_value in expected.items():
        values = _header_values(response, header_name)
        if len(values) != 1:
            errors.append(f"expected exactly one {header_name} header")
        elif values[0] != expected_value:
            errors.append(f"unexpected {header_name} policy")
    return errors


def validate_probe_response(
    probe: Probe,
    response: HttpResponse,
    *,
    require_hsts: bool,
) -> list[str]:
    errors: list[str] = []
    if response.status != probe.expected_status:
        errors.append(
            f"expected HTTP {probe.expected_status}, received HTTP {response.status}",
        )
    if probe.expected_location is not None:
        locations = _header_values(response, "location")
        if len(locations) != 1:
            errors.append("expected exactly one location header")
        elif locations[0] != probe.expected_location:
            errors.append("unexpected redirect location")
    errors.extend(validate_security_headers(response, require_hsts=require_hsts))
    return errors


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
            collected[normalized] = [str(value) for value in (get_all(key) or [])]
    else:
        for key, value in headers.items():
            normalized = str(key).lower()
            if isinstance(value, (list, tuple)):
                collected.setdefault(normalized, []).extend(str(item) for item in value)
            else:
                collected.setdefault(normalized, []).append(str(value))
    return {
        key: values[0] if len(values) == 1 else tuple(values) for key, values in collected.items()
    }


def _to_http_response(response: Any) -> HttpResponse:
    return HttpResponse(
        status=response.getcode(),
        headers=_normalized_response_headers(response.headers),
    )


def _fetch_http(
    url: str,
    *,
    timeout: float,
    ssl_context: ssl.SSLContext | None = None,
) -> HttpResponse:
    request = Request(url, headers={"Accept": "*/*"}, method="GET")
    handlers: list[Any] = [_NoRedirectHandler()]
    if ssl_context is not None:
        handlers.append(HTTPSHandler(context=ssl_context))
    open_request = build_opener(*handlers).open
    try:
        with open_request(request, timeout=timeout) as response:
            return _to_http_response(response)
    except HTTPError as exc:
        with exc:
            return _to_http_response(exc)


def _parse_url(value: str, *, allow_loopback_http: bool) -> ParseResult:
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError:
        raise argparse.ArgumentTypeError("probe URL is malformed") from None
    if parsed.hostname is None or parsed.username is not None or parsed.password is not None:
        raise argparse.ArgumentTypeError("probe URL must not contain userinfo")
    if parsed.params or parsed.query or parsed.fragment:
        raise argparse.ArgumentTypeError("probe URL must not contain params, query, or fragment")
    is_loopback_http = (
        parsed.scheme == "http"
        and parsed.hostname in {"127.0.0.1", "localhost", "::1"}
        and port is not None
    )
    if parsed.scheme != "https" and not (allow_loopback_http and is_loopback_http):
        raise argparse.ArgumentTypeError("probe URL must use HTTPS")
    return parsed


def _parse_route(value: str) -> tuple[str, str]:
    label, separator, route_url = value.partition("=")
    if not separator or PROBE_LABEL.fullmatch(label) is None:
        raise argparse.ArgumentTypeError("route must be LABEL=HTTPS_URL")
    parsed = _parse_url(route_url, allow_loopback_http=False)
    if parsed.path in {"", "/"}:
        raise argparse.ArgumentTypeError("route URL must include its deployment prefix")
    return label, route_url.rstrip("/")


def _parse_probe(value: str) -> Probe:
    label, separator, remainder = value.partition("=")
    status_text, second_separator, url = remainder.partition("=")
    if (
        not separator
        or not second_separator
        or PROBE_LABEL.fullmatch(label) is None
        or not status_text.isdigit()
    ):
        raise argparse.ArgumentTypeError("probe must be LABEL=STATUS=URL")
    expected_status = int(status_text)
    if not 100 <= expected_status <= 599:
        raise argparse.ArgumentTypeError("probe status must be between 100 and 599")
    return Probe(label=label, url=url, expected_status=expected_status)


def route_probes(label: str, route_url: str) -> list[Probe]:
    parsed = urlparse(route_url)
    route_path = parsed.path.rstrip("/")
    canonical_url = parsed._replace(
        path=f"{route_path}/",
        params="",
        query="",
        fragment="",
    ).geturl()
    redirect_url = parsed._replace(
        path=route_path,
        params="",
        query=ROUTE_PROBE_QUERY,
        fragment="",
    ).geturl()
    return [
        Probe(
            label=f"{label}_redirect",
            url=redirect_url,
            expected_status=308,
            expected_location=f"{route_path}/?{ROUTE_PROBE_QUERY}",
        ),
        Probe(label=f"{label}_shell", url=canonical_url, expected_status=200),
        Probe(
            label=f"{label}_config",
            url=urljoin(canonical_url, "loom-frontend-config.json"),
            expected_status=200,
        ),
        Probe(
            label=f"{label}_missing_asset",
            url=urljoin(canonical_url, "assets/loom-security-header-smoke-missing.js"),
            expected_status=404,
        ),
    ]


def run_probes(
    probes: list[Probe],
    *,
    timeout: float,
    require_hsts: bool,
    ssl_context: ssl.SSLContext | None = None,
    fetcher: Callable[..., HttpResponse] | None = None,
) -> list[ProbeResult]:
    fetch = partial(_fetch_http, ssl_context=ssl_context) if fetcher is None else fetcher
    results: list[ProbeResult] = []
    for probe in probes:
        try:
            response = fetch(probe.url, timeout=timeout)
        except Exception as exc:  # pragma: no cover - operator network failure
            results.append(
                ProbeResult(
                    label=probe.label,
                    url=probe.url,
                    expected_status=probe.expected_status,
                    observed_status=None,
                    status="fail",
                    errors=(f"request failed: {type(exc).__name__}",),
                ),
            )
            continue
        errors = validate_probe_response(probe, response, require_hsts=require_hsts)
        results.append(
            ProbeResult(
                label=probe.label,
                url=probe.url,
                expected_status=probe.expected_status,
                observed_status=response.status,
                status="pass" if not errors else "fail",
                errors=tuple(errors),
            ),
        )
    return results


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify exact Loom frontend browser security headers.",
    )
    parser.add_argument(
        "--route",
        action="append",
        type=_parse_route,
        default=[],
        metavar="LABEL=HTTPS_URL",
    )
    parser.add_argument(
        "--probe",
        action="append",
        type=_parse_probe,
        default=[],
        metavar="LABEL=STATUS=URL",
        help="Check one explicit response; useful for an ephemeral web-origin 5xx probe.",
    )
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--json", action="store_true")
    parser.add_argument(
        "--web-origin-only",
        action="store_true",
        help="Skip ingress-owned HSTS; allowed only for explicit loopback probes.",
    )
    args = parser.parse_args(argv)
    if not args.route and not args.probe:
        parser.error("at least one --route or --probe is required")
    if args.web_origin_only and args.route:
        parser.error("--web-origin-only accepts explicit --probe values only")
    for probe in args.probe:
        try:
            _parse_url(probe.url, allow_loopback_http=args.web_origin_only)
        except argparse.ArgumentTypeError as exc:
            parser.error(str(exc))
    if args.web_origin_only and any(urlparse(probe.url).scheme != "http" for probe in args.probe):
        parser.error("--web-origin-only probes must use loopback HTTP")
    if not args.web_origin_only and any(
        urlparse(probe.url).scheme != "https" for probe in args.probe
    ):
        parser.error("combined probes must use HTTPS")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    probes = [probe for label, route_url in args.route for probe in route_probes(label, route_url)]
    probes.extend(args.probe)
    labels = [probe.label for probe in probes]
    if len(labels) != len(set(labels)):
        raise SystemExit("probe labels must be unique")
    results = run_probes(
        probes,
        timeout=args.timeout,
        require_hsts=not args.web_origin_only,
        ssl_context=None,
    )
    report = {
        "status": "pass" if all(result.status == "pass" for result in results) else "fail",
        "hsts_scope": "web-origin-only" if args.web_origin_only else "combined-ingress",
        "probes": [asdict(result) for result in results],
    }
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        for result in results:
            print(f"{result.status.upper()} {result.label}: {result.url}")
            for error in result.errors:
                print(f"  - {error}", file=sys.stderr)
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
