from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

from loom_cli.rollout.admin_smoke_contract import AdminSmokeAuthority
from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
from loom_cli.rollout.operator.final_smoke_executor import FinalSmokeExecutor
from loom_cli.rollout.preflight_contract import CheckOperation
from tests.loom_cli.rollout.operator.test_final_gate_plan import _plan


class _Transport:
    base_url = "https://yylx.world/dev"

    def __init__(self, request: Callable[..., tuple[int, bytes]]) -> None:
        self.request = request

    def __call__(self, method, path, token, payload, headers):
        return self.request(method, path, token, payload, headers)


def _executor(tmp_path: Path, *, request):
    token = tmp_path / "admin-token"
    token.write_bytes(b"admin-token\n")
    token.chmod(0o600)
    trusted = read_trusted_file(
        token,
        service_uid=os.geteuid(),
        private=True,
        max_bytes=1024,
        require_nonempty=True,
    )
    plan = replace(
        _plan(tmp_path),
        request_id="req-1111111111111111",
        secret_metadata_fingerprints={"admin": f"sha256:{trusted.metadata_fingerprint}"},
    )
    return (
        plan,
        FinalSmokeExecutor(
            service_uid=os.geteuid(),
            token_path=token,
            expected_token_fingerprint=safe_content_fingerprint(b"admin-token"),
            authority=AdminSmokeAuthority(
                represented_username="devansh",
                team_id="11111111-1111-4111-8111-111111111111",
                admin_actor="codex-v1-release-gate",
                task_id="loom-smoke/gb10-oracle-hello-world",
                required_worker_pool="gb10-arm64",
                agent="oracle",
            ),
            request=_Transport(request),
            monotonic=lambda: 1.0,
            sleep=lambda _seconds: None,
        ),
    )


def _terminal(batch_id: str, batch_name: str) -> dict[str, object]:
    return {
        "id": batch_id,
        "name": batch_name,
        "team_id": "11111111-1111-4111-8111-111111111111",
        "submitted_by_user": {
            "username": "devansh",
            "team_id": "11111111-1111-4111-8111-111111111111",
        },
        "task_filter": {"task_ids": ["loom-smoke/gb10-oracle-hello-world"]},
        "required_worker_pools": ["gb10-arm64"],
        "state": "finished",
        "result_status": "succeeded",
        "expected_trial_count": 1,
        "trial_summary": {"succeeded": 1},
    }


def test_final_smoke_uses_shared_contract_and_reaches_terminal_success(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []

    def request(method, path, token, payload, headers):
        calls.append((method, path))
        assert token == "admin-token"
        if path == "/api/v1/health":
            value = {"status": "ok"}
        elif path == "/api/v1/auth/whoami":
            value = {"credential_type": "admin_bearer_token"}
        elif path == "/api/v1/benchmarks":
            value = {"items": [{"id": "benchmark"}]}
        elif path.startswith("/api/v1/tasks/"):
            value = {"id": "loom-smoke/gb10-oracle-hello-world"}
        elif path.startswith("/api/v1/batches?"):
            value = {"items": []}
        elif path == "/api/v1/admin/batches/on-behalf":
            assert payload is not None
            assert headers == {"X-Loom-Admin-Actor": "codex-v1-release-gate"}
            value = {"id": "batch-1"}
        elif path == "/api/v1/batches/batch-1":
            value = _terminal("batch-1", "rollout-1111111111111111-1")
        else:  # pragma: no cover - exact route assertion
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.ready
    assert result.protected_mutation
    assert ("POST", "/api/v1/admin/batches/on-behalf") in calls


def test_final_smoke_recovers_exact_batch_without_duplicate_submit(tmp_path: Path) -> None:
    calls: list[tuple[str, str]] = []
    batch = _terminal("batch-1", "rollout-1111111111111111-1")

    def request(method, path, _token, _payload, _headers):
        calls.append((method, path))
        if path == "/api/v1/health":
            value = {"status": "ok"}
        elif path == "/api/v1/auth/whoami":
            value = {"credential_type": "admin_bearer_token"}
        elif path == "/api/v1/benchmarks":
            value = {"items": [{"id": "benchmark"}]}
        elif path.startswith("/api/v1/tasks/"):
            value = {"id": "loom-smoke/gb10-oracle-hello-world"}
        elif path.startswith("/api/v1/batches?"):
            value = {"items": [batch]}
        elif path == "/api/v1/batches/batch-1":
            value = batch
        else:  # pragma: no cover - exact route assertion
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)
    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.ready
    assert result.protected_mutation
    assert all(method != "POST" for method, _path in calls)


def test_final_smoke_rejects_nonrecoverable_worker_pool_failure(tmp_path: Path) -> None:
    batch = _terminal("batch-1", "rollout-1111111111111111-1")
    batch.update(
        {
            "state": "finished",
            "result_status": "all_failed",
            "failure_reason": "fanout_submit_failed",
            "fanout_errors": [{"reason": "required_worker_pool_incompatible"}],
        }
    )

    def request(_method, path, _token, _payload, _headers):
        if path == "/api/v1/health":
            value = {"status": "ok"}
        elif path == "/api/v1/auth/whoami":
            value = {"credential_type": "admin_bearer_token"}
        elif path == "/api/v1/benchmarks":
            value = {"items": [{"id": "benchmark"}]}
        elif path.startswith("/api/v1/tasks/"):
            value = {"id": "loom-smoke/gb10-oracle-hello-world"}
        elif path.startswith("/api/v1/batches?"):
            value = {"items": [batch]}
        elif path == "/api/v1/batches/batch-1":
            value = batch
        else:  # pragma: no cover - exact route assertion
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)
    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-batch-nonrecoverable"}
    assert result.protected_mutation


def test_final_smoke_rejects_token_drift_before_http(tmp_path: Path) -> None:
    calls: list[str] = []
    plan, executor = _executor(
        tmp_path,
        request=lambda _method, path, _token, _payload, _headers: (
            calls.append(path) or (200, b"{}")
        ),
    )
    executor.token_path.write_bytes(b"changed\n")

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-token-binding-drift"}
    assert not result.protected_mutation
    assert calls == []
