from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from scripts.ops import prod_pressure_worker_control as control

from loom_control_plane.prod_pressure_control import ProdPressureSignal, _grace_evidence

_ROOT = Path(__file__).resolve().parents[2]


def test_run_once_bridges_prod_pressure_to_staging_worker_control(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def _http_json(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        if kwargs["method"] == "GET":
            return {
                "has_pressure": True,
                "cause": "prod_capacity_pressure",
                "prod_pending_count": 3,
                "prod_active_count": 1,
                "prod_capacity_shortfall": 2,
                "source": "control-plane prod queue summary",
            }
        return {
            "action": "draining",
            "new_staging_claims_allowed": False,
            "host_intents": {"trt-gb10-1": "draining"},
            "grace": {"action": "wait"},
        }

    monkeypatch.setattr(control, "_http_json", _http_json)

    report = control.run_once(
        prod_cp_url="http://prod-cp:8080",
        prod_admin_token="prod-secret",
        staging_cp_url="http://staging-cp:8080",
        staging_admin_token="staging-secret",
        staging_environment="staging",
        pool_name="gb10-arm64",
        preemptible=True,
        grace_period_seconds=600,
        freshness_seconds=120,
        timeout=10.0,
    )

    assert report["status"] == "pass"
    assert report["worker_control"]["new_staging_claims_allowed"] is False
    assert calls[0]["method"] == "GET"
    assert calls[0]["path"] == (
        "/admin/worker-pools/gb10-arm64/prod-pressure?freshness_sec=120"
    )
    assert calls[1]["method"] == "POST"
    assert calls[1]["path"] == (
        "/admin/gb10-worker-pools/staging/gb10-arm64/prod-pressure"
    )
    assert calls[1]["body"] == {
        "prod_pending_count": 3,
        "prod_active_count": 1,
        "prod_capacity_shortfall": 2,
        "source": "control-plane prod queue summary",
        "preemptible": True,
        "grace_period_seconds": 600,
    }


def test_run_once_forwards_clear_signal_for_recovery(monkeypatch) -> None:
    posted: dict[str, Any] = {}

    def _http_json(**kwargs):  # type: ignore[no-untyped-def]
        if kwargs["method"] == "GET":
            return {
                "has_pressure": False,
                "cause": "none",
                "prod_pending_count": 0,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 0,
            }
        posted.update(kwargs["body"])
        return {
            "action": "recovered",
            "new_staging_claims_allowed": True,
        }

    monkeypatch.setattr(control, "_http_json", _http_json)

    report = control.run_once(
        prod_cp_url="http://prod-cp:8080",
        prod_admin_token="prod-secret",
        staging_cp_url="http://staging-cp:8080",
        staging_admin_token="staging-secret",
        staging_environment="staging",
        pool_name="gb10-arm64",
        preemptible=False,
        grace_period_seconds=0,
        freshness_seconds=120,
        timeout=10.0,
    )

    assert posted["prod_pending_count"] == 0
    assert posted["preemptible"] is False
    assert report["worker_control"]["action"] == "recovered"


def test_run_once_fails_closed_when_prod_pressure_source_is_unavailable(monkeypatch) -> None:
    posted: dict[str, Any] = {}

    def _http_json(**kwargs):  # type: ignore[no-untyped-def]
        if kwargs["method"] == "GET":
            raise RuntimeError("GET failed token=loom_admin_secret")
        posted.update(kwargs["body"])
        return {
            "action": "draining",
            "new_staging_claims_allowed": False,
        }

    monkeypatch.setattr(control, "_http_json", _http_json)

    report = control.run_once(
        prod_cp_url="http://prod-cp:8080",
        prod_admin_token="prod-secret",
        staging_cp_url="http://staging-cp:8080",
        staging_admin_token="staging-secret",
        staging_environment="staging",
        pool_name="gb10-arm64",
        preemptible=True,
        grace_period_seconds=600,
        freshness_seconds=120,
        timeout=10.0,
    )

    assert report["fail_closed"] is True
    assert "loom_admin_secret" not in report["pressure_fetch_error"]
    assert posted["prod_capacity_shortfall"] == 1
    assert "fail-closed" in posted["source"]


def test_redact_text_removes_worker_control_credentials() -> None:
    rendered = control.redact_text(
        "Authorization: Bearer loom_admin_secret token=loom_w_secret sk-secret-value",
    )
    assert "loom_admin_secret" not in rendered
    assert "loom_w_secret" not in rendered
    assert "sk-secret-value" not in rendered


def test_preemptible_control_only_becomes_retryable_after_grace() -> None:
    signal = ProdPressureSignal(1, 0, 0)
    before = _grace_evidence(
        control={"started_at": "2026-07-15T12:00:00+00:00"},
        signal=signal,
        preemptible=True,
        grace_period_seconds=600,
        now=datetime(2026, 7, 15, 12, 9, 59, tzinfo=UTC),
    )
    after = _grace_evidence(
        control={"started_at": "2026-07-15T12:00:00+00:00"},
        signal=signal,
        preemptible=True,
        grace_period_seconds=600,
        now=datetime(2026, 7, 15, 12, 10, tzinfo=UTC),
    )

    assert before["action"] == "wait"
    assert before["retryable"] is False
    assert after["action"] == "cancel_retryable"
    assert after["retryable"] is True


def test_systemd_timer_is_a_continuous_secret_reference_only_runtime_path() -> None:
    service = (
        _ROOT
        / "deploy/worker-capacity/loom-prod-pressure-worker-control.service"
    ).read_text(encoding="utf-8")
    timer = (
        _ROOT / "deploy/worker-capacity/loom-prod-pressure-worker-control.timer"
    ).read_text(encoding="utf-8")
    env_example = (
        _ROOT / "deploy/worker-capacity/prod-pressure-worker-control.env.example"
    ).read_text(encoding="utf-8")

    assert "User=loom-rollout" in service
    assert "scripts/ops/prod_pressure_worker_control.py" in service
    assert "--prod-admin-token ${LOOM_PROD_PRESSURE_PROD_TOKEN_SOURCE}" in service
    assert "--staging-admin-token ${LOOM_PROD_PRESSURE_STAGING_TOKEN_SOURCE}" in service
    assert "OnUnitActiveSec=30s" in timer
    assert "Persistent=true" in timer
    assert "TOKEN_SOURCE=file:" in env_example
    assert "loom_admin_" not in service + env_example
