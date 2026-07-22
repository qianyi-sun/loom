from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from scripts.ops import prod_pressure_worker_control as control
from scripts.ops import render_prod_pressure_worker_control_service as service_renderer

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
        targets=[("staging", "gb10")],
        preemptible=True,
        grace_period_seconds=600,
        freshness_seconds=120,
        timeout=10.0,
    )

    assert report["status"] == "pass"
    assert report["artifact_type"] == "prod-pressure-worker-control"
    assert len(report["targets"]) == 1
    entry = report["targets"][0]
    assert entry["environment"] == "staging"
    assert entry["pool_name"] == "gb10"
    assert entry["worker_control"]["new_staging_claims_allowed"] is False
    assert calls[0]["method"] == "GET"
    assert calls[0]["path"] == (
        "/admin/worker-pools/gb10/prod-pressure?freshness_sec=120"
    )
    assert calls[1]["method"] == "POST"
    # #892: POST to the actuator-neutral route, not the GB10 lifecycle route.
    assert calls[1]["path"] == (
        "/admin/worker-pools/staging/gb10/prod-pressure"
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
        targets=[("staging", "gb10")],
        preemptible=False,
        grace_period_seconds=0,
        freshness_seconds=120,
        timeout=10.0,
    )

    assert posted["prod_pending_count"] == 0
    assert posted["preemptible"] is False
    assert report["status"] == "pass"
    assert report["targets"][0]["worker_control"]["action"] == "recovered"


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
        targets=[("staging", "gb10")],
        preemptible=True,
        grace_period_seconds=600,
        freshness_seconds=120,
        timeout=10.0,
    )

    entry = report["targets"][0]
    assert entry["fail_closed"] is True
    assert "loom_admin_secret" not in entry["pressure_fetch_error"]
    assert posted["prod_capacity_shortfall"] == 1
    assert "fail-closed" in posted["source"]


def test_run_once_fans_out_to_multiple_targets(monkeypatch) -> None:
    calls: list[dict[str, Any]] = []

    def _http_json(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        if kwargs["method"] == "GET":
            return {
                "has_pressure": True,
                "cause": "prod_capacity_pressure",
                "prod_pending_count": 2,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 2,
                "source": "control-plane prod queue summary",
            }
        return {"action": "draining", "new_staging_claims_allowed": False}

    monkeypatch.setattr(control, "_http_json", _http_json)

    report = control.run_once(
        prod_cp_url="http://prod-cp:8080",
        prod_admin_token="prod-secret",
        staging_cp_url="http://staging-cp:8080",
        staging_admin_token="staging-secret",
        targets=[("staging", "gb10"), ("staging", "oldlab")],
        preemptible=True,
        grace_period_seconds=600,
        freshness_seconds=90,
        timeout=10.0,
    )

    assert report["status"] == "pass"
    assert [(t["environment"], t["pool_name"]) for t in report["targets"]] == [
        ("staging", "gb10"),
        ("staging", "oldlab"),
    ]
    # One GET + one POST per target, in order.
    methods = [(c["method"], c["path"]) for c in calls]
    assert methods == [
        ("GET", "/admin/worker-pools/gb10/prod-pressure?freshness_sec=90"),
        ("POST", "/admin/worker-pools/staging/gb10/prod-pressure"),
        ("GET", "/admin/worker-pools/oldlab/prod-pressure?freshness_sec=90"),
        ("POST", "/admin/worker-pools/staging/oldlab/prod-pressure"),
    ]


def test_run_once_marks_status_fail_when_one_target_errors(monkeypatch) -> None:
    def _http_json(**kwargs):  # type: ignore[no-untyped-def]
        if kwargs["method"] == "GET":
            return {
                "has_pressure": True,
                "cause": "prod_capacity_pressure",
                "prod_pending_count": 1,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 1,
                "source": "control-plane prod queue summary",
            }
        if kwargs["path"].endswith("/oldlab/prod-pressure"):
            raise RuntimeError("POST failed token=loom_admin_secret")
        return {"action": "draining", "new_staging_claims_allowed": False}

    monkeypatch.setattr(control, "_http_json", _http_json)

    report = control.run_once(
        prod_cp_url="http://prod-cp:8080",
        prod_admin_token="prod-secret",
        staging_cp_url="http://staging-cp:8080",
        staging_admin_token="staging-secret",
        targets=[("staging", "gb10"), ("staging", "oldlab")],
        preemptible=True,
        grace_period_seconds=600,
        freshness_seconds=120,
        timeout=10.0,
    )

    assert report["status"] == "fail"
    good, bad = report["targets"]
    assert "worker_control" in good
    assert "error" in bad
    assert "loom_admin_secret" not in bad["error"]


def test_main_defaults_to_single_target_backward_compatible(monkeypatch, capsys) -> None:
    calls: list[dict[str, Any]] = []

    def _http_json(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        if kwargs["method"] == "GET":
            return {
                "has_pressure": True,
                "cause": "prod_capacity_pressure",
                "prod_pending_count": 1,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 1,
                "source": "control-plane prod queue summary",
            }
        return {"action": "draining", "new_staging_claims_allowed": False}

    monkeypatch.setattr(control, "_http_json", _http_json)
    monkeypatch.setenv("PROD_TOKEN", "prod-secret")
    monkeypatch.setenv("STAGING_TOKEN", "staging-secret")

    rc = control.main(
        [
            "--prod-cp-url", "http://prod-cp:8080",
            "--prod-admin-token", "env:PROD_TOKEN",
            "--staging-cp-url", "http://staging-cp:8080",
            "--staging-admin-token", "env:STAGING_TOKEN",
        ],
    )

    assert rc == 0
    methods = [(c["method"], c["path"]) for c in calls]
    assert methods == [
        ("GET", "/admin/worker-pools/gb10/prod-pressure?freshness_sec=120"),
        ("POST", "/admin/worker-pools/staging/gb10/prod-pressure"),
    ]
    report = json.loads(capsys.readouterr().out)
    assert report["status"] == "pass"
    assert [(t["environment"], t["pool_name"]) for t in report["targets"]] == [
        ("staging", "gb10"),
    ]


def test_main_fans_out_repeated_target_flags(monkeypatch, capsys) -> None:
    calls: list[dict[str, Any]] = []

    def _http_json(**kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs)
        if kwargs["method"] == "GET":
            return {
                "has_pressure": False,
                "cause": "none",
                "prod_pending_count": 0,
                "prod_active_count": 0,
                "prod_capacity_shortfall": 0,
            }
        return {"action": "recovered", "new_staging_claims_allowed": True}

    monkeypatch.setattr(control, "_http_json", _http_json)
    monkeypatch.setenv("PROD_TOKEN", "prod-secret")
    monkeypatch.setenv("STAGING_TOKEN", "staging-secret")

    rc = control.main(
        [
            "--prod-cp-url", "http://prod-cp:8080",
            "--prod-admin-token", "env:PROD_TOKEN",
            "--staging-cp-url", "http://staging-cp:8080",
            "--staging-admin-token", "env:STAGING_TOKEN",
            "--target", "staging:gb10",
            "--target", "staging:oldlab",
        ],
    )

    assert rc == 0
    posts = [c["path"] for c in calls if c["method"] == "POST"]
    assert posts == [
        "/admin/worker-pools/staging/gb10/prod-pressure",
        "/admin/worker-pools/staging/oldlab/prod-pressure",
    ]
    report = json.loads(capsys.readouterr().out)
    assert [(t["environment"], t["pool_name"]) for t in report["targets"]] == [
        ("staging", "gb10"),
        ("staging", "oldlab"),
    ]


def test_main_rejects_malformed_target(monkeypatch) -> None:
    monkeypatch.setattr(control, "_http_json", lambda **kwargs: {})  # type: ignore[misc]
    monkeypatch.setenv("PROD_TOKEN", "prod-secret")
    monkeypatch.setenv("STAGING_TOKEN", "staging-secret")
    with pytest.raises(SystemExit):
        control.main(
            [
                "--prod-cp-url", "http://prod-cp:8080",
                "--prod-admin-token", "env:PROD_TOKEN",
                "--staging-cp-url", "http://staging-cp:8080",
                "--staging-admin-token", "env:STAGING_TOKEN",
                "--target", "no-colon-here",
            ],
        )


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
    install_docs = (_ROOT / "deploy/worker-capacity/README.md").read_text(encoding="utf-8")

    assert "User=loom-rollout" in service
    assert service.count("${GIT_SHA}") == 4
    assert "/opt/loom-staging-runner/repo" not in service
    assert "/opt/loom-staging-runner/venv" not in service
    assert "--prod-admin-token ${LOOM_PROD_PRESSURE_PROD_TOKEN_SOURCE}" in service
    assert "--staging-admin-token ${LOOM_PROD_PRESSURE_STAGING_TOKEN_SOURCE}" in service
    assert "OnUnitActiveSec=30s" in timer
    assert "Persistent=true" in timer
    assert "TOKEN_SOURCE=file:" in env_example
    assert "render_prod_pressure_worker_control_service.py" in install_docs
    assert 'CANDIDATE_ROOT="/opt/loom-staging-runner/candidates/$GIT_SHA"' in install_docs
    assert "loom_admin_" not in service + env_example


def test_worker_control_service_renderer_binds_one_exact_candidate() -> None:
    template = (
        _ROOT / "deploy/worker-capacity/loom-prod-pressure-worker-control.service"
    ).read_text(encoding="utf-8")
    git_sha = "a" * 40

    rendered = service_renderer.render_service_unit(template, git_sha=git_sha)

    candidate_root = f"/opt/loom-staging-runner/candidates/{git_sha}"
    assert "${GIT_SHA}" not in rendered
    assert f"WorkingDirectory={candidate_root}/repo" in rendered
    assert f"Environment=PATH={candidate_root}/venv/bin:" in rendered
    assert f"ExecStart={candidate_root}/venv/bin/python -I -B" in rendered
    assert f"{candidate_root}/repo/scripts/ops/prod_pressure_worker_control.py" in rendered


@pytest.mark.parametrize("git_sha", ["a" * 39, "A" * 40, "../" + "a" * 37])
def test_worker_control_service_renderer_rejects_noncanonical_sha(git_sha: str) -> None:
    template = (
        _ROOT / "deploy/worker-capacity/loom-prod-pressure-worker-control.service"
    ).read_text(encoding="utf-8")

    with pytest.raises(ValueError, match="40-character lowercase hexadecimal"):
        service_renderer.render_service_unit(template, git_sha=git_sha)


def test_worker_control_service_renderer_rejects_mutable_runtime_path() -> None:
    template = (
        _ROOT / "deploy/worker-capacity/loom-prod-pressure-worker-control.service"
    ).read_text(encoding="utf-8")
    template = template.replace(
        "/opt/loom-staging-runner/candidates/${GIT_SHA}/repo",
        "/opt/loom-staging-runner/repo",
        1,
    )

    with pytest.raises(ValueError, match="mutable legacy runtime path"):
        service_renderer.render_service_unit(template, git_sha="a" * 40)
