from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import pytest

from loom_cli.rollout.admin_smoke_contract import AdminSmokeAuthority
from loom_cli.rollout.rehearsal_smoke_probe import (
    RehearsalSmokeProbeError,
    load_rehearsal_admin_token,
    run_probe,
)


def _authority() -> AdminSmokeAuthority:
    return AdminSmokeAuthority(
        represented_username="devansh",
        team_id="11111111-1111-4111-8111-111111111111",
        admin_actor="loom-staging-rollout",
        task_id="loom-smoke/gb10-oracle-hello-world",
        required_worker_pool="gb10-arm64",
        agent="oracle",
    )


def _secret(tmp_path: Path) -> Path:
    path = tmp_path / "secrets.toml"
    path.write_text('[admin]\ntoken = "loom_admin_' + "s" * 40 + '"\n', encoding="utf-8")
    path.chmod(0o440)
    return path


def test_probe_validates_admission_and_returns_only_hashed_evidence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = _secret(tmp_path)
    authority = _authority()
    requests: list[tuple[str, str]] = []
    batch = {
        "id": "batch-1",
        "name": "rehearsal-abc123",
        "team_id": authority.team_id,
        "submitted_by_user": {"username": "Devansh", "team_id": authority.team_id},
        "task_filter": {"task_ids": [authority.task_id]},
        "required_worker_pools": ["gb10-arm64"],
        "state": "pending",
        "result_status": None,
        "failure_reason": None,
        "fanout_errors": None,
    }

    def http(method, path, *, token, payload=None, headers=None):
        assert token.startswith("loom_admin_")
        requests.append((method, path))
        if path == "/api/v1/health":
            value = {"status": "ok"}
        elif path == "/api/v1/auth/whoami":
            value = {"credential_type": "admin_bearer_token", "scopes": []}
        elif path == "/api/v1/benchmarks":
            value = {"items": [{"id": "loom-smoke"}]}
        elif path.startswith("/api/v1/tasks/"):
            value = {"id": authority.task_id}
        elif path.startswith("/api/v1/batches?"):
            value = {"items": []}
        elif path == "/api/v1/admin/batches/on-behalf":
            assert payload == {
                "name": "rehearsal-abc123",
                "represented_username": "devansh",
                "team_id": authority.team_id,
                "task_filter": {"task_ids": [authority.task_id]},
                "trial_config": {"agent_name": "oracle", "agent_model": None},
                "n_per_task": 1,
                "required_worker_pools": ["gb10-arm64"],
            }
            assert headers == {"X-Loom-Admin-Actor": "loom-staging-rollout"}
            return 201, b'{"batch_id":"batch-1"}'
        elif path == "/api/v1/batches/batch-1":
            value = batch
        else:
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    monkeypatch.setattr("loom_cli.rollout.rehearsal_smoke_probe._http", http)
    result = run_probe(
        plan_sha256="a" * 64,
        batch_name="rehearsal-abc123",
        authority=authority,
        admin_secret_path=secret,
        expected_owner_uid=os.geteuid(),
        allowed_group_gid=os.getegid(),
    )

    assert result["status"] == "ready"
    assert result["batch_id"] == "batch-1"
    assert result["persisted"] is True
    serialized = json.dumps(result, sort_keys=True)
    assert "loom_admin_" not in serialized
    assert "Devansh" not in serialized
    assert ("POST", "/api/v1/admin/batches/on-behalf") in requests


def test_secret_reader_rejects_symlink_mode_owner_and_read_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = _secret(tmp_path)
    link = tmp_path / "linked.toml"
    link.symlink_to(secret)
    with pytest.raises(RehearsalSmokeProbeError, match="opened safely"):
        load_rehearsal_admin_token(
            link,
            expected_owner_uid=os.geteuid(),
            allowed_group_gid=os.getegid(),
        )

    secret.chmod(0o444)
    with pytest.raises(RehearsalSmokeProbeError, match="metadata"):
        load_rehearsal_admin_token(
            secret,
            expected_owner_uid=os.geteuid(),
            allowed_group_gid=os.getegid(),
        )


def test_probe_rejects_persisted_authority_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = _secret(tmp_path)
    authority = _authority()

    def http(method, path, *, token, payload=None, headers=None):
        del method, token, payload, headers
        if path == "/api/v1/health":
            value = {"status": "ok"}
        elif path == "/api/v1/auth/whoami":
            value = {"credential_type": "admin_bearer_token"}
        elif path == "/api/v1/benchmarks":
            value = {"items": [{}]}
        elif path.startswith("/api/v1/tasks/"):
            value = {"id": authority.task_id}
        elif path.startswith("/api/v1/batches?"):
            value = {
                "items": [
                    {
                        "id": "batch-1",
                        "name": "rehearsal-abc123",
                        "team_id": authority.team_id,
                        "submitted_by_user": {
                            "username": "devansh",
                            "team_id": authority.team_id,
                        },
                        "task_filter": {"task_ids": [authority.task_id]},
                    }
                ]
            }
        else:
            value = {
                "id": "batch-1",
                "name": "rehearsal-abc123",
                "team_id": authority.team_id,
                "submitted_by_user": {
                    "username": "devansh",
                    "team_id": authority.team_id,
                },
                "task_filter": {"task_ids": [authority.task_id]},
                "required_worker_pools": [],
                "state": "pending",
            }
        return 200, json.dumps(value).encode()

    monkeypatch.setattr("loom_cli.rollout.rehearsal_smoke_probe._http", http)
    with pytest.raises(RehearsalSmokeProbeError, match="worker-pool") as captured:
        run_probe(
            plan_sha256="a" * 64,
            batch_name="rehearsal-abc123",
            authority=authority,
            admin_secret_path=secret,
            expected_owner_uid=os.geteuid(),
            allowed_group_gid=os.getegid(),
        )
    assert captured.value.request_id == "batch-readback"
    assert captured.value.reason_code == "contract-invalid"


def test_probe_reports_safe_transport_locus(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = _secret(tmp_path)

    def http(method, path, *, token, payload=None, headers=None):
        del method, path, token, payload, headers
        raise RehearsalSmokeProbeError(
            "secret-shaped transport detail",
            reason_code="transport-unavailable",
        )

    monkeypatch.setattr("loom_cli.rollout.rehearsal_smoke_probe._http", http)
    with pytest.raises(RehearsalSmokeProbeError) as captured:
        run_probe(
            plan_sha256="a" * 64,
            batch_name="rehearsal-abc123",
            authority=_authority(),
            admin_secret_path=secret,
            expected_owner_uid=os.geteuid(),
            allowed_group_gid=os.getegid(),
        )

    assert captured.value.failure_code == "rehearsal-api-smoke-failed"
    assert captured.value.request_id == "health"
    assert captured.value.reason_code == "transport-unavailable"
    assert captured.value.response_sha256 is None


@pytest.mark.parametrize(
    ("detail", "reason_code"),
    [
        ("no active worker advertises backend 'docker'", "no-active-worker"),
        ("task_filter matched zero tasks", "empty-filter"),
        ("invalid task config for exact task", "invalid-task-config"),
        ("agent\u00d7task capability mismatch", "agent-task-incompatible"),
        ("invalid family run request", "invalid-family-run"),
        ("unclassified internal validation", "generic-http-response"),
        (
            {"reason": "staging_capacity_evidence_stale", "retryable": True},
            "staging-capacity-evidence-stale",
        ),
        (
            {"reason": "staging_capacity_evidence_missing", "retryable": True},
            "staging-capacity-evidence-missing",
        ),
        (
            {"reason": "staging_capacity_evidence_corrupt", "retryable": True},
            "staging-capacity-evidence-corrupt",
        ),
        (
            {"reason": "staging_capacity_policy_drift", "retryable": True},
            "staging-capacity-policy-drift",
        ),
        (
            {"reason": "staging_capacity_high_water", "retryable": True},
            "staging-capacity-high-water",
        ),
        (
            {"reason": "untrusted_capacity_reason", "retryable": True},
            "generic-http-response",
        ),
        (
            {
                "reason": "staging_capacity_evidence_stale",
                "retryable": True,
                "raw": "must-not-escape",
            },
            "generic-http-response",
        ),
    ],
)
def test_probe_normalizes_http_failure_without_exposing_body(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    detail: object,
    reason_code: str,
) -> None:
    secret = _secret(tmp_path)
    authority = _authority()
    response = json.dumps({"detail": detail, "token": "must-not-escape"}).encode()

    def http(method, path, *, token, payload=None, headers=None):
        del method, token, payload, headers
        if path == "/api/v1/health":
            value = {"status": "ok"}
        elif path == "/api/v1/auth/whoami":
            value = {"credential_type": "admin_bearer_token"}
        elif path == "/api/v1/benchmarks":
            value = {"items": [{}]}
        elif path.startswith("/api/v1/tasks/"):
            value = {"id": authority.task_id}
        elif path.startswith("/api/v1/batches?"):
            value = {"items": []}
        elif path == "/api/v1/admin/batches/on-behalf":
            return 400, response
        else:
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    monkeypatch.setattr("loom_cli.rollout.rehearsal_smoke_probe._http", http)
    with pytest.raises(RehearsalSmokeProbeError) as captured:
        run_probe(
            plan_sha256="a" * 64,
            batch_name="rehearsal-abc123",
            authority=authority,
            admin_secret_path=secret,
            expected_owner_uid=os.geteuid(),
            allowed_group_gid=os.getegid(),
        )

    failure = captured.value
    assert str(failure) == "service request returned HTTP 400"
    assert failure.failure_code == "rehearsal-api-smoke-http-400"
    assert failure.reason_code == reason_code
    assert failure.request_id == "batch-submit"
    assert failure.response_sha256 == hashlib.sha256(response).hexdigest()
    assert "must-not-escape" not in str(failure)
