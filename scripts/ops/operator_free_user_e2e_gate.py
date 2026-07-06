#!/usr/bin/env python3
"""Validate #493 operator-free normal-user E2E evidence packages.

This is an offline evidence gate. It validates a redacted package produced by a
later authorized first-prod user journey, but it does not submit workloads,
contact production, read secrets, or inspect operator-only infrastructure.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

DEFAULT_PROD_ROUTE = "https://yylx.world/prod"
DEFAULT_DEV_ROUTE = "https://yylx.world/dev"
DEFAULT_PROD_API_BASE = "https://yylx.world/prod/api"
DEFAULT_DEV_API_BASE = "https://yylx.world/dev/api"

ALLOWED_USER_ROLES = {"normal_user", "scoped_user", "user_agent"}
ALLOWED_SURFACES = {"cli", "api"}

REQUIRED_CLI_API_STEPS = (
    "submit",
    "monitor",
    "batch_detail",
    "batch_debug",
    "trial_detail",
    "trial_debug",
    "download_atif",
    "download_trajectory",
    "download_artifact",
    "delivery_bundle",
    "integrity",
)
REQUIRED_FRONTEND_NAVIGATION = (
    "app_loaded",
    "runs_list",
    "batch_detail",
    "trial_detail",
    "run_library",
)
REQUIRED_FRONTEND_BUTTONS = (
    "submit_batch",
    "refresh_status",
    "load_debug",
    "download_atif",
    "download_trajectory",
    "download_artifact",
    "download_delivery_bundle",
)
INTEGRITY_STEPS = {
    "download_atif",
    "download_trajectory",
    "download_artifact",
    "delivery_bundle",
    "integrity",
}

HTTP_ROUTE_RE = re.compile(r"^(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+(/\S+)$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
URL_CREDENTIAL_RE = re.compile(r"://([^:/@\s]+):([^@\s]+)@")
SECRET_VALUE_PATTERNS = (
    re.compile(r"authorization:\s*bearer", re.IGNORECASE),
    re.compile(r"\bbearer\s+[A-Za-z0-9._~+/=-]{8,}", re.IGNORECASE),
    re.compile(r"\bsk-[A-Za-z0-9][A-Za-z0-9_-]{10,}"),
    re.compile(r"\bhf_[A-Za-z0-9_]{20,}"),
    re.compile(r"\bghp_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bgithub_pat_[A-Za-z0-9_]{10,}"),
    re.compile(r"\bloom_(?:api|w)_[A-Za-z0-9._-]{8,}", re.IGNORECASE),
    re.compile(r"[?&](X-Amz-Signature|AWSAccessKeyId|Signature)=", re.IGNORECASE),
    re.compile(r"(?i)(token|api_key|access_key|secret|password|signature)=[^&\s]+"),
    URL_CREDENTIAL_RE,
)
FORBIDDEN_SHORTCUT_PATTERNS = (
    *SECRET_VALUE_PATTERNS,
    re.compile(r"\bkubectl\b", re.IGNORECASE),
    re.compile(r"\bpsql\b", re.IGNORECASE),
    re.compile(r"\bpostgres(?:ql)?://", re.IGNORECASE),
    re.compile(r"\bmc\s+(?:alias|admin|cat|cp|ls|mirror|pipe|stat)\b", re.IGNORECASE),
    re.compile(r"\bminio\b", re.IGNORECASE),
    re.compile(r"\bssh\s+[-\w@.]+", re.IGNORECASE),
    re.compile(r"\bloom\s+admin\b", re.IGNORECASE),
    re.compile(r"\bloom\s+cluster\b", re.IGNORECASE),
    re.compile(
        r"\bscripts/(?:ops|staging_smoke_gate|validate_environment_isolation)", re.IGNORECASE
    ),
    re.compile(r"\.svc\.cluster\.local\b", re.IGNORECASE),
    re.compile(r"\bhost\.docker\.internal\b", re.IGNORECASE),
    re.compile(r"\bworker[-_\s]ssh\b", re.IGNORECASE),
    re.compile(r"/admin(?:/|\b)", re.IGNORECASE),
    re.compile(r"\boperator[-\s]generated\b", re.IGNORECASE),
)
ARGV_SECRET_PATTERNS = (
    re.compile(r"--(?:api-)?token(?:=|\s+)", re.IGNORECASE),
    re.compile(r"--(?:auth|authorization)(?:=|\s+)", re.IGNORECASE),
    re.compile(r"-H\s+['\"]?Authorization:", re.IGNORECASE),
)


def _load_evidence(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("evidence root must be a JSON object")
    return raw


def _iter_strings(value: Any, path: str = "") -> list[tuple[str, str]]:
    if isinstance(value, str):
        return [(path, value)]
    if isinstance(value, dict):
        pairs: list[tuple[str, str]] = []
        for key, child in value.items():
            pairs.extend(_iter_strings(child, f"{path}.{key}" if path else str(key)))
        return pairs
    if isinstance(value, list):
        pairs = []
        for index, child in enumerate(value):
            pairs.extend(_iter_strings(child, f"{path}[{index}]"))
        return pairs
    return []


def _is_non_empty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _as_dict(value: Any, path: str, errors: list[str]) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    errors.append(f"{path} must be an object")
    return None


def _as_string(value: Any, path: str, errors: list[str]) -> str | None:
    if _is_non_empty_string(value):
        return str(value)
    errors.append(f"{path} must be a non-empty string")
    return None


def _expect_equal(
    value: Any,
    expected: str,
    path: str,
    errors: list[str],
) -> None:
    if value != expected:
        errors.append(f"{path} must be {expected}")


def _validate_status_pass(container: dict[str, Any], path: str, errors: list[str]) -> None:
    if container.get("status") != "pass":
        errors.append(f"{path}.status must be 'pass'")


def _normalize_api_route(value: str) -> str:
    match = HTTP_ROUTE_RE.match(value.strip())
    if match:
        return match.group(1)
    return value.strip()


def _validate_api_route(value: Any, path: str, errors: list[str]) -> None:
    route = _as_string(value, path, errors)
    if route is None:
        return
    normalized = _normalize_api_route(route)
    if normalized.startswith(DEFAULT_PROD_API_BASE):
        normalized = normalized.removeprefix(DEFAULT_PROD_API_BASE)
    if not normalized.startswith("/api/v1/"):
        errors.append(f"{path} must use an exposed /api/v1 route")


def _validate_environment(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    env = _as_dict(evidence.get("environment"), "environment", errors)
    if env is None:
        return errors

    _expect_equal(env.get("name"), "production", "environment.name", errors)
    _expect_equal(env.get("route"), DEFAULT_PROD_ROUTE, "environment.route", errors)
    _expect_equal(env.get("api_base"), DEFAULT_PROD_API_BASE, "environment.api_base", errors)

    role = env.get("user_role")
    if role not in ALLOWED_USER_ROLES:
        errors.append("environment.user_role must be normal_user, scoped_user, or user_agent")
    if role in {"admin", "operator", "owner"}:
        errors.append("environment.user_role must not be admin/operator")

    token_source = env.get("token_source")
    if token_source is not None:
        token = _as_string(token_source, "environment.token_source", errors)
        if token is not None and not token.startswith(("env:", "file:")):
            errors.append("environment.token_source must start with env: or file:")
    return errors


def _validate_prod_dev_separation(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    separation = _as_dict(evidence.get("prod_dev_separation"), "prod_dev_separation", errors)
    if separation is None:
        return errors

    expected = {
        "production_route": DEFAULT_PROD_ROUTE,
        "development_route": DEFAULT_DEV_ROUTE,
        "production_api_base": DEFAULT_PROD_API_BASE,
        "development_api_base": DEFAULT_DEV_API_BASE,
    }
    for field, expected_value in expected.items():
        _expect_equal(separation.get(field), expected_value, f"prod_dev_separation.{field}", errors)

    if separation.get("production_route") == separation.get("development_route"):
        errors.append("prod_dev_separation production and development routes must differ")
    if separation.get("production_api_base") == separation.get("development_api_base"):
        errors.append("prod_dev_separation production and development API bases must differ")
    return errors


def _validate_cli_api(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    cli_api = _as_dict(evidence.get("cli_api"), "cli_api", errors)
    if cli_api is None:
        return errors

    for step_name in REQUIRED_CLI_API_STEPS:
        raw_step = cli_api.get(step_name)
        if not isinstance(raw_step, dict):
            errors.append(f"missing required cli_api step '{step_name}'")
            continue
        path = f"cli_api.{step_name}"
        _validate_status_pass(raw_step, path, errors)
        surface = raw_step.get("surface")
        if surface not in ALLOWED_SURFACES:
            errors.append(f"{path}.surface must be cli or api")
        actor_role = raw_step.get("actor_role")
        if actor_role not in ALLOWED_USER_ROLES:
            errors.append(f"{path}.actor_role must be a normal user role")
        command = raw_step.get("command")
        if surface == "cli":
            command_text = _as_string(command, f"{path}.command", errors)
            if command_text is not None:
                for pattern in ARGV_SECRET_PATTERNS:
                    if pattern.search(command_text):
                        errors.append(f"forbidden argv secret source at {path}.command")
                        break
        _validate_api_route(raw_step.get("api_route"), f"{path}.api_route", errors)
        if step_name in INTEGRITY_STEPS:
            expected_sha = raw_step.get("expected_sha256")
            observed_sha = raw_step.get("observed_sha256")
            if not isinstance(expected_sha, str) or not SHA256_RE.fullmatch(expected_sha):
                errors.append(f"{path}.expected_sha256 must be a lowercase sha256 digest")
            if not isinstance(observed_sha, str) or not SHA256_RE.fullmatch(observed_sha):
                errors.append(f"{path}.observed_sha256 must be a lowercase sha256 digest")
            if (
                isinstance(expected_sha, str)
                and isinstance(observed_sha, str)
                and expected_sha != observed_sha
            ):
                errors.append(f"{path} sha256 mismatch")
    return errors


def _validate_named_statuses(
    values: Any,
    *,
    required_names: tuple[str, ...],
    path: str,
    label: str,
    errors: list[str],
) -> None:
    checks = _as_dict(values, path, errors)
    if checks is None:
        return
    for name in required_names:
        if name not in checks:
            errors.append(f"missing required frontend {label} check '{name}'")
            continue
        if checks[name] != "pass":
            errors.append(f"{path}.{name} must be 'pass'")


def _validate_frontend(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    frontend = _as_dict(evidence.get("frontend"), "frontend", errors)
    if frontend is None:
        return errors

    _expect_equal(frontend.get("route"), DEFAULT_PROD_ROUTE, "frontend.route", errors)
    _expect_equal(frontend.get("api_base"), DEFAULT_PROD_API_BASE, "frontend.api_base", errors)
    label = frontend.get("environment_label")
    if not _is_non_empty_string(label):
        errors.append("frontend.environment_label must be a non-empty string")
    elif "beta" in str(label).lower():
        errors.append("frontend.environment_label must not contain beta")

    _validate_named_statuses(
        frontend.get("navigation_checks"),
        required_names=REQUIRED_FRONTEND_NAVIGATION,
        path="frontend.navigation_checks",
        label="navigation",
        errors=errors,
    )
    _validate_named_statuses(
        frontend.get("button_checks"),
        required_names=REQUIRED_FRONTEND_BUTTONS,
        path="frontend.button_checks",
        label="button",
        errors=errors,
    )

    download_routes = frontend.get("download_routes")
    if not isinstance(download_routes, list) or not download_routes:
        errors.append("frontend.download_routes must be a non-empty list")
        return errors
    for index, route in enumerate(download_routes):
        if not isinstance(route, str) or not route:
            errors.append(f"frontend.download_routes[{index}] must be a non-empty string")
            continue
        if not route.startswith(DEFAULT_PROD_API_BASE + "/v1/"):
            errors.append(f"frontend.download_routes[{index}] must use the production API base")
    return errors


def _validate_forbidden_shortcut_declarations(evidence: dict[str, Any]) -> list[str]:
    shortcuts = evidence.get("forbidden_shortcuts")
    if shortcuts in (None, []):
        return []
    if not isinstance(shortcuts, list):
        return ["forbidden_shortcuts must be an empty list"]
    return [
        f"forbidden shortcut declared at forbidden_shortcuts[{index}]"
        for index, _ in enumerate(shortcuts)
    ]


def _validate_no_forbidden_strings(evidence: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    for path, value in _iter_strings(evidence):
        for pattern in FORBIDDEN_SHORTCUT_PATTERNS:
            if pattern.search(value):
                errors.append(f"forbidden evidence value at {path}")
                break
    return errors


def validate_evidence(evidence: dict[str, Any]) -> list[str]:
    """Return validation errors for a redacted #493 evidence package."""
    errors: list[str] = []
    if evidence.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if evidence.get("issue") != 493:
        errors.append("issue must be 493")
    errors.extend(_validate_environment(evidence))
    errors.extend(_validate_prod_dev_separation(evidence))
    errors.extend(_validate_cli_api(evidence))
    errors.extend(_validate_frontend(evidence))
    errors.extend(_validate_forbidden_shortcut_declarations(evidence))
    errors.extend(_validate_no_forbidden_strings(evidence))
    return errors


def _report(evidence: dict[str, Any]) -> dict[str, Any]:
    environment = evidence["environment"]
    return {
        "status": "pass",
        "issue": 493,
        "environment": {
            "name": environment["name"],
            "route": environment["route"],
            "api_base": environment["api_base"],
            "user_role": environment["user_role"],
            "token_source": environment.get("token_source"),
        },
        "prod_dev_separation": {
            "production_route": DEFAULT_PROD_ROUTE,
            "development_route": DEFAULT_DEV_ROUTE,
            "production_api_base": DEFAULT_PROD_API_BASE,
            "development_api_base": DEFAULT_DEV_API_BASE,
        },
        "validated_cli_api_steps": list(REQUIRED_CLI_API_STEPS),
        "validated_frontend_navigation": list(REQUIRED_FRONTEND_NAVIGATION),
        "validated_frontend_buttons": list(REQUIRED_FRONTEND_BUTTONS),
    }


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate redacted #493 evidence.")
    validate.add_argument("--evidence", required=True, type=Path)
    validate.add_argument("--output-json", type=Path)

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(sys.argv[1:] if argv is None else argv)
    try:
        evidence = _load_evidence(args.evidence)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(
            f"Operator-free user E2E gate: FAIL\n- failed to read evidence: {exc}", file=sys.stderr
        )
        return 1

    errors = validate_evidence(evidence)
    if errors:
        print("Operator-free user E2E gate: FAIL", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    if args.output_json is not None:
        args.output_json.write_text(
            json.dumps(_report(evidence), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    print("Operator-free user E2E gate: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
