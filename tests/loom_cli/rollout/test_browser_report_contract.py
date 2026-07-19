from __future__ import annotations

from copy import deepcopy

import pytest

from loom_cli.rollout.browser_report_contract import (
    BROWSER_REPORT_CHECK_IDS,
    RehearsalBrowserReportAuthority,
    RolloutBrowserReportAuthority,
    browser_report_ready,
    browser_report_schema_digest,
)


def _report(*, authority: RolloutBrowserReportAuthority) -> dict[str, object]:
    return {
        "schema_version": 4,
        "status": "pass",
        "failure_code": None,
        "deployment_identity": {
            "expected_deployed_sha": authority.candidate_sha,
            "observed_deployed_sha": authority.candidate_sha,
            "matched": True,
        },
        "route": authority.route,
        "request_id": authority.request_id,
        "rollout_binding": {
            "request_id": authority.request_id,
            "attempt_number": authority.attempt_number,
            "request_envelope_sha256": authority.request_envelope_sha256,
            "resolved_sha": authority.candidate_sha,
        },
        "target": {"username": authority.username, "user_id": "user-qianyi"},
        "audit_event_id": "audit-event",
        "browser": {"name": "chromium", "version": "1.2.3"},
        "checks": {check_id: True for check_id in BROWSER_REPORT_CHECK_IDS},
        "cleanup": {"logout_status": 204, "auth_me_after_logout_status": 401},
    }


def _authority() -> RolloutBrowserReportAuthority:
    return RolloutBrowserReportAuthority(
        request_id="req-1111111111111111",
        attempt_number=1,
        request_envelope_sha256="b" * 64,
        candidate_sha="a" * 40,
        route="https://yylx.world/dev",
    )


def test_rollout_report_requires_complete_single_source_contract() -> None:
    authority = _authority()
    report = _report(authority=authority)

    assert browser_report_ready(report, authority=authority)
    assert len(browser_report_schema_digest()) == 64


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("checks", "browser_console_clean"), False),
        (("deployment_identity", "matched"), False),
        (("rollout_binding", "attempt_number"), 2),
        (("cleanup", "logout_status"), 500),
        (("browser", "name"), "firefox"),
        (("target", "username"), "other"),
        (("route",), "https://yylx.world/prod"),
    ],
)
def test_rollout_report_fails_closed_on_binding_or_acceptance_drift(
    path: tuple[str, ...],
    value: object,
) -> None:
    authority = _authority()
    report = deepcopy(_report(authority=authority))
    target: dict[str, object] = report
    for key in path[:-1]:
        child = target[key]
        assert isinstance(child, dict)
        target = child
    target[path[-1]] = value

    assert not browser_report_ready(report, authority=authority)


def test_report_rejects_opposite_binding_and_extra_fields() -> None:
    authority = _authority()
    report = _report(authority=authority)
    report["rehearsal_binding"] = {}

    assert not browser_report_ready(report, authority=authority)


def test_rehearsal_authority_is_exact_and_cannot_accept_rollout_report() -> None:
    rollout = _authority()
    report = _report(authority=rollout)
    rehearsal = RehearsalBrowserReportAuthority(
        plan_sha256="c" * 64,
        isolation_id="d" * 24,
        candidate_sha=rollout.candidate_sha,
        route="https://yylx.world/dev/rehearsal/" + "d" * 24,
    )

    assert not browser_report_ready(report, authority=rehearsal)
