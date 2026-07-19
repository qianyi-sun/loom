"""Single-source sanitized browser acceptance report authority."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TypeAlias

BROWSER_REPORT_SCHEMA_VERSION = 4
BROWSER_ACCEPTANCE_USERNAME = "qianyi"
BROWSER_REPORT_CHECK_IDS = frozenset(
    {
        "bootstrap_status_204",
        "bootstrap_empty_body",
        "bootstrap_no_store",
        "deployed_build_sha_present",
        "deployed_build_sha_matches_expected",
        "secure_http_only_lax_cookie",
        "authenticated_target_user",
        "platform_admin_authority",
        "audit_event_correlated",
        "admin_access_document_2xx",
        "authenticated_react_mount",
        "admin_tabs_accessibility",
        "admin_requests_apis_200",
        "admin_requests_ui_visible",
        "admin_accounts_apis_200",
        "admin_accounts_ui_visible",
        "admin_teams_api_200",
        "admin_teams_ui_visible",
        "admin_invites_apis_200",
        "admin_invites_ui_visible",
        "admin_tokens_api_200",
        "admin_tokens_ui_visible",
        "admin_audit_api_200",
        "all_admin_tabs_operable",
        "audit_tab_event_visible",
        "rate_cards_api_200",
        "rate_cards_ui_visible",
        "browser_console_clean",
        "browser_page_errors_clean",
        "browser_request_failures_clean",
        "browser_server_errors_clean",
    }
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_REQUEST_RE = re.compile(r"^req-[0-9a-f]{16}$")
_ISOLATION_RE = re.compile(r"^[0-9a-f]{24}$")
_TOP_LEVEL_KEYS = frozenset(
    {
        "schema_version",
        "status",
        "deployment_identity",
        "route",
        "request_id",
        "target",
        "audit_event_id",
        "browser",
        "checks",
        "cleanup",
        "failure_code",
    }
)


@dataclass(frozen=True, slots=True)
class RolloutBrowserReportAuthority:
    request_id: str
    attempt_number: int
    request_envelope_sha256: str
    candidate_sha: str
    route: str
    username: str = BROWSER_ACCEPTANCE_USERNAME

    def __post_init__(self) -> None:
        if (
            _REQUEST_RE.fullmatch(self.request_id) is None
            or not 1 <= self.attempt_number <= 1000
            or _SHA256_RE.fullmatch(self.request_envelope_sha256) is None
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or self.route != "https://yylx.world/dev"
            or self.username != BROWSER_ACCEPTANCE_USERNAME
        ):
            raise ValueError("rollout browser report authority is invalid")


@dataclass(frozen=True, slots=True)
class RehearsalBrowserReportAuthority:
    plan_sha256: str
    isolation_id: str
    candidate_sha: str
    route: str
    username: str = BROWSER_ACCEPTANCE_USERNAME

    def __post_init__(self) -> None:
        if (
            _SHA256_RE.fullmatch(self.plan_sha256) is None
            or _ISOLATION_RE.fullmatch(self.isolation_id) is None
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or self.route != f"https://yylx.world/dev/rehearsal/{self.isolation_id}"
            or self.username != BROWSER_ACCEPTANCE_USERNAME
        ):
            raise ValueError("rehearsal browser report authority is invalid")


BrowserReportAuthority: TypeAlias = RolloutBrowserReportAuthority | RehearsalBrowserReportAuthority


def browser_report_schema_digest() -> str:
    """Digest the complete report shape consumed by both final and rehearsal gates."""
    contract = {
        "binding_modes": {
            "rehearsal_binding": ["plan_sha256", "isolation_id", "resolved_sha"],
            "rollout_binding": [
                "request_id",
                "attempt_number",
                "request_envelope_sha256",
                "resolved_sha",
            ],
        },
        "browser": ["name", "version"],
        "checks": sorted(BROWSER_REPORT_CHECK_IDS),
        "cleanup": ["logout_status", "auth_me_after_logout_status"],
        "deployment_identity": [
            "expected_deployed_sha",
            "observed_deployed_sha",
            "matched",
        ],
        "failure_code": "string-or-null",
        "schema_version": BROWSER_REPORT_SCHEMA_VERSION,
        "status": "pass-or-fail",
        "target": ["username", "user_id"],
        "top_level": sorted(_TOP_LEVEL_KEYS),
    }
    return hashlib.sha256(
        json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def browser_report_ready(payload: object, *, authority: BrowserReportAuthority) -> bool:
    """Validate one complete sanitized schema-v4 report against exact authority."""
    binding_key, request_id, expected_binding = _binding(authority)
    if not isinstance(payload, Mapping) or set(payload) != _TOP_LEVEL_KEYS | {binding_key}:
        return False
    deployment = payload.get("deployment_identity")
    binding = payload.get(binding_key)
    target = payload.get("target")
    browser = payload.get("browser")
    checks = payload.get("checks")
    cleanup = payload.get("cleanup")
    return bool(
        payload.get("schema_version") == BROWSER_REPORT_SCHEMA_VERSION
        and payload.get("status") == "pass"
        and payload.get("failure_code") is None
        and payload.get("route") == authority.route
        and payload.get("request_id") == request_id
        and isinstance(deployment, Mapping)
        and deployment
        == {
            "expected_deployed_sha": authority.candidate_sha,
            "observed_deployed_sha": authority.candidate_sha,
            "matched": True,
        }
        and isinstance(binding, Mapping)
        and binding == expected_binding
        and isinstance(target, Mapping)
        and target.get("username") == authority.username
        and _nonempty_text(target.get("user_id"))
        and _nonempty_text(payload.get("audit_event_id"))
        and isinstance(browser, Mapping)
        and browser.get("name") == "chromium"
        and _nonempty_text(browser.get("version"))
        and isinstance(checks, Mapping)
        and set(checks) == BROWSER_REPORT_CHECK_IDS
        and all(value is True for value in checks.values())
        and isinstance(cleanup, Mapping)
        and cleanup.get("logout_status") == 204
        and cleanup.get("auth_me_after_logout_status") == 401
    )


def _binding(
    authority: BrowserReportAuthority,
) -> tuple[str, str, dict[str, object]]:
    if isinstance(authority, RolloutBrowserReportAuthority):
        return (
            "rollout_binding",
            authority.request_id,
            {
                "request_id": authority.request_id,
                "attempt_number": authority.attempt_number,
                "request_envelope_sha256": authority.request_envelope_sha256,
                "resolved_sha": authority.candidate_sha,
            },
        )
    return (
        "rehearsal_binding",
        f"rehearsal-{authority.isolation_id}",
        {
            "plan_sha256": authority.plan_sha256,
            "isolation_id": authority.isolation_id,
            "resolved_sha": authority.candidate_sha,
        },
    )


def _nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


__all__ = [
    "BROWSER_ACCEPTANCE_USERNAME",
    "BROWSER_REPORT_CHECK_IDS",
    "BROWSER_REPORT_SCHEMA_VERSION",
    "RehearsalBrowserReportAuthority",
    "RolloutBrowserReportAuthority",
    "browser_report_ready",
    "browser_report_schema_digest",
]
