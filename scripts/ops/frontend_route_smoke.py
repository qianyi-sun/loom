#!/usr/bin/env python3
"""HTTP smoke checks for prod/dev Loom frontend route metadata.

The check reads only the public `loom-frontend-config.json` document served by
the web pod. It does not require live user, admin, provider, or worker secrets.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

SECRET_PATTERNS = (
    re.compile(r"authorization:\s*bearer", re.IGNORECASE),
    re.compile(r"\bbearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"[?&](X-Amz-Signature|AWSAccessKeyId|Signature)=", re.IGNORECASE),
    re.compile(r"\bgithub-environment:", re.IGNORECASE),
)


@dataclass(frozen=True)
class RouteCheck:
    route_url: str
    config_url: str
    expected_environment: str
    expected_api_base: str
    status: str
    errors: list[str]


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

    if "no-store" not in cache_control.lower():
        errors.append("runtime config response must be no-store")

    for value in _iter_strings(document):
        for pattern in SECRET_PATTERNS:
            if pattern.search(value):
                errors.append("runtime config must not expose secret-looking values")
                return errors
    return errors


def _fetch_json(url: str, timeout: float) -> tuple[dict[str, Any], str]:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=timeout) as response:
        raw = response.read()
        cache_control = response.headers.get("Cache-Control", "")
    data = json.loads(raw.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{url}: config document must be a JSON object")
    return data, cache_control


def check_route(
    *,
    route_url: str,
    expected_environment: str,
    expected_api_base: str,
    timeout: float,
) -> RouteCheck:
    config_url = urljoin(route_url.rstrip("/") + "/", "loom-frontend-config.json")
    try:
        document, cache_control = _fetch_json(config_url, timeout)
        errors = validate_config_document(
            route_url=route_url,
            expected_environment=expected_environment,
            expected_api_base=expected_api_base,
            cache_control=cache_control,
            document=document,
        )
    except Exception as exc:  # pragma: no cover - covered by operator CLI use
        errors = [str(exc)]
    return RouteCheck(
        route_url=route_url,
        config_url=config_url,
        expected_environment=expected_environment,
        expected_api_base=expected_api_base,
        status="pass" if not errors else "fail",
        errors=errors,
    )


def _parse_route(value: str) -> tuple[str, str, str]:
    parts = value.split("=", 2)
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            "route must be ENVIRONMENT=ROUTE_URL=EXPECTED_API_BASE",
        )
    environment, route_url, expected_api_base = parts
    if not environment or not route_url.startswith("https://"):
        raise argparse.ArgumentTypeError("route URL must be https")
    if not expected_api_base.startswith("https://"):
        raise argparse.ArgumentTypeError("expected API base must be https")
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
