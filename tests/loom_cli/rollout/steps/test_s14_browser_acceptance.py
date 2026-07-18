from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.s14_browser_acceptance import BrowserAcceptanceStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


def _private_file(path: Path, payload: bytes) -> None:
    path.write_bytes(payload)
    path.chmod(0o600)


def _context(tmp_path: Path):
    request_id = "req-1111111111111111"
    envelope = tmp_path / "envelope.json"
    envelope_payload = {
        "request_id": request_id,
        "attempt_number": 1,
        "resolved_sha": "a" * 40,
    }
    _private_file(envelope, json.dumps(envelope_payload).encode())
    token = tmp_path / "admin-token"
    _private_file(token, b"loom_admin_" + b"A" * 43 + b"\n")
    ctx = make_ctx(
        tmp_path,
        resolved_sha="a" * 40,
        request_id=request_id,
        attempt_number=1,
        request_envelope_path=envelope,
        admin_token_source=f"file:{token}",
    )
    ctx.cluster_config_path.write_text(
        'ingress_host = "yylx.world"\nfrontend_route_path = "/dev"\n',
        encoding="utf-8",
    )
    step_path = tmp_path / "16-staging-admin-browser-acceptance"
    step_path.mkdir()
    return ctx, StepDir(number=16, name="staging-admin-browser-acceptance", path=step_path)


def test_browser_acceptance_runs_hardened_candidate_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, step_dir = _context(tmp_path)
    commands: list[list[str]] = []

    def fake_run(argv: list[str], **_kwargs: object) -> SubprocessResult:
        command = list(argv)
        commands.append(command)
        envelope = ctx.request_envelope_path.read_bytes()
        report = {
            "schema_version": 3,
            "status": "pass",
            "failure_code": None,
            "request_id": ctx.request_id,
            "deployment_identity": {
                "expected_deployed_sha": ctx.resolved_sha,
                "observed_deployed_sha": ctx.resolved_sha,
                "matched": True,
            },
            "rollout_binding": {
                "request_id": ctx.request_id,
                "attempt_number": ctx.attempt_number,
                "request_envelope_sha256": hashlib.sha256(envelope).hexdigest(),
                "resolved_sha": ctx.resolved_sha,
            },
            "cleanup": {
                "logout_status": 204,
                "auth_me_after_logout_status": 401,
            },
        }
        report_path = step_dir.path / "browser-output" / "staging-admin-browser-acceptance.json"
        report_path.write_text(json.dumps(report), encoding="utf-8")
        report_path.chmod(0o600)
        return SubprocessResult(argv=command, returncode=0, stdout='{"status":"pass"}', stderr="")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s14_browser_acceptance.run_captured",
        fake_run,
    )

    result = BrowserAcceptanceStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts["browser_report"].endswith("staging-admin-browser-acceptance.json")
    command = commands[0]
    assert command[:4] == ["docker", "run", "--rm", "--pull=never"]
    assert "--read-only" in command
    assert "--cap-drop=ALL" in command
    assert "--security-opt=no-new-privileges" in command
    assert "--network=bridge" in command
    assert f"{os.geteuid()}:{os.getegid()}" in command
    assert f"loom-staging-admin-browser-smoke:{ctx.image_tag}" in command
    assert "file:/run/secrets/admin-token" in command
    assert "qianyi" in command
    assert ctx.request_id in command
    assert ctx.resolved_sha in command


def test_browser_acceptance_fails_closed_without_broker_envelope(tmp_path: Path) -> None:
    ctx = make_ctx(tmp_path)
    step_dir = StepDir(number=16, name="staging-admin-browser-acceptance", path=tmp_path)

    result = BrowserAcceptanceStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "broker-owned staging attempt envelope" in (result.error or "")


def test_browser_acceptance_rejects_mismatched_sanitized_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx, step_dir = _context(tmp_path)

    def fake_run(argv: list[str], **_kwargs: object) -> SubprocessResult:
        report_path = step_dir.path / "browser-output" / "staging-admin-browser-acceptance.json"
        report_path.write_text(
            json.dumps(
                {
                    "schema_version": 3,
                    "status": "pass",
                    "failure_code": None,
                    "request_id": ctx.request_id,
                    "deployment_identity": {
                        "expected_deployed_sha": ctx.resolved_sha,
                        "observed_deployed_sha": "b" * 40,
                        "matched": False,
                    },
                    "rollout_binding": {},
                    "cleanup": {
                        "logout_status": 204,
                        "auth_me_after_logout_status": 401,
                    },
                }
            ),
            encoding="utf-8",
        )
        report_path.chmod(0o600)
        return SubprocessResult(argv=list(argv), returncode=0, stdout="", stderr="")

    monkeypatch.setattr(
        "loom_cli.rollout.steps.s14_browser_acceptance.run_captured",
        fake_run,
    )

    result = BrowserAcceptanceStep().run(ctx, step_dir)

    assert result.exit_code == 1
    assert "candidate-bound" in (result.error or "")
