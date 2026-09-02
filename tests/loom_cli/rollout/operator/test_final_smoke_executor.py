from __future__ import annotations

import json
import os
import urllib.parse
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path

import pytest

from loom.models.types import ModelSpec
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


def _oracle_authority() -> AdminSmokeAuthority:
    return AdminSmokeAuthority(
        represented_username="devansh",
        team_id="11111111-1111-4111-8111-111111111111",
        admin_actor="codex-v1-release-gate",
        task_id="loom-smoke/gb10-oracle-hello-world",
        required_worker_pool="gb10",
        agent="oracle",
    )


def _model_authority() -> AdminSmokeAuthority:
    return replace(
        _oracle_authority(),
        agent="direct-completion",
        agent_model=ModelSpec(
            provider="yibu",
            name="gpt-4o-mini",
            source="local-server",
            local_server="yibu",
            max_output_tokens=64,
        ),
    )


def _executor(tmp_path: Path, *, request, authority: AdminSmokeAuthority | None = None):
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
            authority=authority or _oracle_authority(),
            request=_Transport(request),
            refresh_capacity=lambda _plan: "c" * 64,
            monotonic=lambda: 1.0,
            sleep=lambda _seconds: None,
        ),
    )


def _model_trial_config() -> dict[str, object]:
    return {
        "agent_name": "direct-completion",
        "agent_model": {
            "provider": "yibu",
            "name": "gpt-4o-mini",
            "source": "local-server",
            "local_server": "yibu",
            "max_output_tokens": 64,
        },
        "request_params": {"temperature": 0, "max_tokens": 64},
        "override_agent_timeout_sec": 180,
    }


def _terminal(
    batch_id: str,
    batch_name: str,
    *,
    trial_config: dict[str, object] | None = None,
) -> dict[str, object]:
    return {
        "id": batch_id,
        "name": batch_name,
        "team_id": "11111111-1111-4111-8111-111111111111",
        "submitted_by_user": {
            "username": "devansh",
            "team_id": "11111111-1111-4111-8111-111111111111",
        },
        "task_filter": {"task_ids": ["loom-smoke/gb10-oracle-hello-world"]},
        "trial_config": trial_config or {"agent_name": "oracle", "agent_model": None},
        "required_worker_pools": ["gb10"],
        "state": "finished",
        "result_status": "succeeded",
        "expected_trial_count": 1,
        "trial_summary": {"succeeded": 1},
    }


def _model_trial(trial_id: str, *, llm_calls_count: int = 1) -> dict[str, object]:
    observed = llm_calls_count > 0
    return {
        "id": trial_id,
        "task_id": "loom-smoke/gb10-oracle-hello-world",
        "team_id": "11111111-1111-4111-8111-111111111111",
        "batch_id": "batch-1",
        "state": "succeeded",
        "failure_reason": None,
        "failure_message": None,
        "agent_name": "direct-completion",
        "model": {
            "provider": "yibu",
            "name": "gpt-4o-mini",
            "source": "local-server",
            "local_server": "yibu",
            "max_output_tokens": 64,
        },
        "llm_calls_count": llm_calls_count,
        "total_prompt_tokens": 10 if observed else 0,
        "total_completion_tokens": 4 if observed else 0,
        "failed_upstream_llm_calls_count": 0,
        "llm_evidence_status": "calls_observed" if observed else "no_call",
        "no_call": not observed,
        "no_call_reason": None if observed else "no_gateway_calls",
        "trajectory_ready": True,
        "atif_ready": True,
    }


def _trajectory() -> bytes:
    events = [
        {"kind": "trial_start"},
        {
            "kind": "llm_call",
            "model": {
                "provider": "yibu",
                "name": "gpt-4o-mini",
                "source": "local-server",
                "local_server": "yibu",
                "max_output_tokens": 64,
            },
            "input_tokens": 10,
            "output_tokens": 4,
        },
        {"kind": "trial_end"},
    ]
    return b"".join(json.dumps(event).encode() + b"\n" for event in events)


def _model_smoke_request(
    *,
    missing_calls_trial: str | None = None,
    missing_llm_event_trial: str | None = None,
    empty_atif_trial: str | None = None,
):
    trial_ids = (
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    )
    calls: list[str] = []

    def request(method, path, token, payload, headers):
        calls.append(path)
        assert token == "admin-token"
        if path == "/api/v1/health":
            return 200, b'{"status":"ok"}'
        if path == "/api/v1/auth/whoami":
            return 200, b'{"credential_type":"admin_bearer_token"}'
        if path == "/api/v1/benchmarks":
            return 200, b'{"items":[{"id":"loom-smoke"}]}'
        if path.startswith("/api/v1/tasks/"):
            return 200, b'{"id":"loom-smoke/gb10-oracle-hello-world"}'
        if path.startswith("/api/v1/batches?"):
            return 200, b'{"items":[]}'
        if path == "/api/v1/admin/batches/on-behalf":
            assert headers == {"X-Loom-Admin-Actor": "codex-v1-release-gate"}
            assert payload is not None
            assert payload["trial_config"] == _model_trial_config()
            return 201, b'{"batch_id":"batch-1"}'
        if path == "/api/v1/batches/batch-1":
            value = _terminal(
                "batch-1",
                "rollout-1111111111111111-1",
                trial_config=_model_trial_config(),
            )
            value["expected_trial_count"] = 2
            value["trial_summary"] = {"succeeded": 2}
            return 200, json.dumps(value).encode()
        if path == "/api/v1/trials?batch_id=batch-1&limit=200":
            return 200, json.dumps(
                {
                    "items": [
                        {"id": trial_id, "batch_id": "batch-1", "state": "succeeded"}
                        for trial_id in trial_ids
                    ],
                    "next_cursor": None,
                }
            ).encode()
        for trial_id in trial_ids:
            if path == f"/api/v1/trials/{trial_id}":
                count = 0 if trial_id == missing_calls_trial else 1
                return 200, json.dumps(_model_trial(trial_id, llm_calls_count=count)).encode()
            if path == f"/api/v1/trials/{trial_id}/trajectory/download":
                if trial_id == missing_llm_event_trial:
                    return 200, b'{"type":"trial_start"}\n{"type":"trial_end"}\n'
                return 200, _trajectory()
            if path == f"/api/v1/trials/{trial_id}/atif":
                if trial_id == empty_atif_trial:
                    return 200, b'{"trajectory_id":"empty","steps":[]}'
                return 200, json.dumps(
                    {"trajectory_id": f"atif-{trial_id}", "steps": [{"step_id": "main"}]}
                ).encode()
        raise AssertionError((method, path))

    return request, calls


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


def test_model_backed_final_smoke_requires_usage_trajectory_and_atif_for_every_trial(
    tmp_path: Path,
) -> None:
    request, calls = _model_smoke_request()
    plan, executor = _executor(
        tmp_path,
        request=request,
        authority=_model_authority(),
    )

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.ready
    assert result.protected_mutation
    assert "/api/v1/trials?batch_id=batch-1&limit=200" in calls
    for trial_id in (
        "11111111-1111-4111-8111-111111111111",
        "22222222-2222-4222-8222-222222222222",
    ):
        assert f"/api/v1/trials/{trial_id}" in calls
        assert f"/api/v1/trials/{trial_id}/trajectory/download" in calls
        assert f"/api/v1/trials/{trial_id}/atif" in calls


def test_model_backed_final_smoke_rejects_one_trial_without_gateway_calls(
    tmp_path: Path,
) -> None:
    request, _calls = _model_smoke_request(
        missing_calls_trial="22222222-2222-4222-8222-222222222222",
    )
    plan, executor = _executor(
        tmp_path,
        request=request,
        authority=_model_authority(),
    )

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-trial-llm-evidence-invalid"}
    assert result.protected_mutation


def test_model_backed_final_smoke_rejects_trajectory_without_llm_event(
    tmp_path: Path,
) -> None:
    request, _calls = _model_smoke_request(
        missing_llm_event_trial="22222222-2222-4222-8222-222222222222",
    )
    plan, executor = _executor(
        tmp_path,
        request=request,
        authority=_model_authority(),
    )

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-trajectory-evidence-invalid"}


def test_model_backed_final_smoke_rejects_empty_atif_projection(tmp_path: Path) -> None:
    request, _calls = _model_smoke_request(
        empty_atif_trial="22222222-2222-4222-8222-222222222222",
    )
    plan, executor = _executor(
        tmp_path,
        request=request,
        authority=_model_authority(),
    )

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-atif-evidence-invalid"}


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


def test_final_smoke_resume_reuses_terminal_prior_attempt_without_duplicate_submit(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str]] = []
    refreshes: list[int] = []
    prior_name = "rollout-1111111111111111-1"
    current_name = "rollout-1111111111111111-2"
    prior = _terminal("batch-prior", prior_name)

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
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            value = {"items": [prior] if query["q"] == [prior_name] else []}
        elif path == "/api/v1/admin/batches/on-behalf":
            value = {"id": "batch-current"}
        elif path == "/api/v1/batches/batch-prior":
            value = prior
        elif path == "/api/v1/batches/batch-current":
            value = _terminal("batch-current", current_name)
        else:  # pragma: no cover - exact route assertion
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)
    plan = replace(plan, attempt_number=2)
    executor = replace(
        executor,
        refresh_capacity=lambda _plan: refreshes.append(1) or "c" * 64,
    )

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.ready
    assert result.protected_mutation
    assert result.observed_epoch == plan.starting_mutation_epoch + 1
    assert refreshes == []
    assert ("GET", "/api/v1/batches/batch-prior") in calls
    assert all(method != "POST" for method, _path in calls)


def test_final_smoke_resume_fails_closed_on_prior_attempt_identity_drift(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str]] = []
    prior_name = "rollout-1111111111111111-1"
    prior = _terminal("batch-prior", prior_name)
    prior["required_worker_pools"] = []

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
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            value = {"items": [prior] if query["q"] == [prior_name] else []}
        elif path == "/api/v1/batches/batch-prior":
            value = prior
        else:  # pragma: no cover - identity drift must stop before submit
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)
    plan = replace(plan, attempt_number=2)

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-batch-identity-invalid"}
    assert result.protected_mutation
    assert all(method != "POST" for method, _path in calls)


def test_final_smoke_resume_boundedly_waits_for_nonterminal_prior_attempt(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str]] = []
    sleeps: list[float] = []
    prior_name = "rollout-1111111111111111-1"
    pending = _terminal("batch-prior", prior_name)
    pending.update({"state": "pending", "result_status": None, "trial_summary": {}})
    terminal = _terminal("batch-prior", prior_name)
    polls = 0

    def request(method, path, _token, _payload, _headers):
        nonlocal polls
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
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            value = {"items": [pending] if query["q"] == [prior_name] else []}
        elif path == "/api/v1/batches/batch-prior":
            polls += 1
            value = pending if polls == 1 else terminal
        else:  # pragma: no cover - prior wait must not submit
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)
    plan = replace(plan, attempt_number=2)
    ticks = iter((0.0, 0.0, 1.0))
    executor = replace(
        executor,
        monotonic=lambda: next(ticks),
        sleep=sleeps.append,
        terminal_timeout_seconds=10.0,
    )

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.ready
    assert polls == 2
    assert sleeps == [executor.poll_interval_seconds]
    assert all(method != "POST" for method, _path in calls)


def test_final_smoke_resume_fails_on_newest_terminal_prior_attempt(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str]] = []
    queried_names: list[str] = []
    newest_name = "rollout-1111111111111111-2"
    older_name = "rollout-1111111111111111-1"
    failed = _terminal("batch-newest", newest_name)
    failed.update(
        {
            "state": "failed",
            "result_status": "all_failed",
            "failure_reason": "trial_failed",
            "fanout_errors": [],
        }
    )
    older = _terminal("batch-older", older_name)

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
            query = urllib.parse.parse_qs(urllib.parse.urlsplit(path).query)
            batch_name = query["q"][0]
            queried_names.append(batch_name)
            value = (
                {"items": [failed]}
                if batch_name == newest_name
                else {"items": [older]}
                if batch_name == older_name
                else {"items": []}
            )
        elif path == "/api/v1/batches/batch-newest":
            value = failed
        else:  # pragma: no cover - newest terminal failure must stop recovery
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)
    plan = replace(plan, attempt_number=3)

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-terminal-invalid"}
    assert queried_names == ["rollout-1111111111111111-3", newest_name]
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


def test_final_smoke_refreshes_capacity_immediately_before_new_submission(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    def request(method, path, _token, _payload, _headers):
        events.append(path)
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
            value = {"id": "batch-1"}
        elif path == "/api/v1/batches/batch-1":
            value = _terminal("batch-1", "rollout-1111111111111111-1")
        else:  # pragma: no cover - exact route assertion
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    plan, executor = _executor(tmp_path, request=request)
    try:
        executor = replace(
            executor,
            refresh_capacity=lambda _plan: events.append("capacity-refresh") or "c" * 64,
        )
    except TypeError:
        pytest.fail("final smoke has no protected capacity refresh boundary")

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.ready
    submit_index = events.index("/api/v1/admin/batches/on-behalf")
    assert events[submit_index - 1 : submit_index + 1] == [
        "capacity-refresh",
        "/api/v1/admin/batches/on-behalf",
    ]


def test_final_smoke_fails_closed_when_capacity_refresh_fails(tmp_path: Path) -> None:
    calls: list[str] = []

    def request(_method, path, _token, _payload, _headers):
        calls.append(path)
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
        else:  # pragma: no cover - refresh must stop before submit
            raise AssertionError(path)
        return 200, json.dumps(value).encode()

    def fail_refresh(_plan):
        raise RuntimeError("private capacity diagnostic")

    plan, executor = _executor(tmp_path, request=request)
    try:
        executor = replace(executor, refresh_capacity=fail_refresh)
    except TypeError:
        pytest.fail("final smoke has no protected capacity refresh boundary")

    result = executor("final.smoke", CheckOperation.APPLY, plan)

    assert result.blockers == {"smoke": "smoke-capacity-refresh-failed"}
    assert result.protected_mutation
    assert "/api/v1/admin/batches/on-behalf" not in calls
