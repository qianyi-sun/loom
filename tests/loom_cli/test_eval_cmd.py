"""`loom eval {run, batch {create,list,show,cancel}, trial {list,show}}`
against an httpx MockTransport. The route layer is exercised by the
integration tests in tests/integration; this file pins the CLI surface
(argparse, payload shape, output, error paths)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from loom_cli.__main__ import main

_CONN_ID = "00000000-0000-0000-0000-0000000000aa"
_BATCH_ID = "00000000-0000-0000-0000-0000000000bb"
_TRIAL_ID = "00000000-0000-0000-0000-0000000000cc"


@pytest.fixture(autouse=True)
def _isolated_logged_in_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    monkeypatch.setenv("MY_TOK", "loom_admin_test123456")
    main([
        "auth", "login",
        "--server", "https://loom.test",
        "--token", "env:MY_TOK",
    ])


class MockServer:
    """Same shape as the providers_cmd test MockServer — records every
    request and lets tests inject canned responses by (method, path)."""

    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []
        self.canned: dict[tuple[str, str], httpx.Response] = {}

    def __getitem__(self, idx: int) -> httpx.Request:
        return self.requests[idx]

    def __len__(self) -> int:
        return len(self.requests)


@pytest.fixture
def mock_server(monkeypatch: pytest.MonkeyPatch) -> MockServer:
    """Patches eval_cmd's authed_client to hit a MockTransport. Also
    patches providers_cmd's authed_client because eval_cmd calls
    `_resolve_by_name` which uses providers_cmd's authed_client binding."""
    server = MockServer()

    def _handler(request: httpx.Request) -> httpx.Response:
        server.requests.append(request)
        key = (request.method, request.url.path)
        if key in server.canned:
            return server.canned[key]
        return httpx.Response(404, json={"detail": f"no mock for {key}"})

    transport = httpx.MockTransport(_handler)

    def _patched(cfg: Any, *, timeout: float = 30.0) -> httpx.Client:
        return httpx.Client(
            base_url=cfg.server_url,
            headers={"Authorization": f"Bearer {cfg.auth_token}"},
            transport=transport,
            timeout=timeout,
        )

    # eval_cmd binds `authed_client` at import via `from … import …`,
    # so we patch its module-level name. Same for providers_cmd's
    # _resolve_by_name lookup path.
    monkeypatch.setattr("loom_cli.eval_cmd.authed_client", _patched)
    monkeypatch.setattr("loom_cli.providers_cmd.authed_client", _patched)
    return server


def _make_connection(
    *, name: str = "openai-prod", type_: str = "openai-compatible",
) -> dict[str, Any]:
    return {
        "id": _CONN_ID,
        "team_id": "00000000-0000-0000-0000-000000000000",
        "name": name,
        "type": type_,
        "base_url": "https://api.openai.com/v1",
        "upstream_host": "api.openai.com",
        "resolved_egress_ips": ["104.18.0.1"],
        "allowed_models": None,
        "status": "valid",
        "last_validated_at": None,
        "last_validation_error": None,
        "pricing_source": "tokens-only",
        "pricing_data": None,
        "created_by": "admin:abc",
        "created_at": "2026-06-16T00:00:00Z",
        "updated_at": "2026-06-16T00:00:00Z",
    }


def _stub_connection_lookup(server: MockServer, **kwargs: Any) -> dict[str, Any]:
    conn = _make_connection(**kwargs)
    server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": [conn]},
    )
    return conn


# ──────────────────────────────────────────────────────────────────────
# eval run
# ──────────────────────────────────────────────────────────────────────


def test_run_resolves_provider_then_posts_trial(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201, json={
            "id": _TRIAL_ID,
            "task_id": "humaneval/HumanEval/0",
            "state": "queued",
            "agent_name": "claude-code",
            "model": {"provider": "openai", "name": "gpt-4o"},
            "submitted_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main([
        "eval", "run",
        "--provider", "openai-prod",
        "--model", "gpt-4o",
        "--agent", "claude-code",
        "--task", "humaneval/HumanEval/0",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert "Submitted trial" in out
    assert _TRIAL_ID in out

    # 1 GET (provider lookup) + 1 POST (submit)
    assert len(mock_server) == 2
    get_req, post_req = mock_server[0], mock_server[1]
    assert get_req.method == "GET"
    assert get_req.url.path == "/api/v1/provider-connections"
    assert post_req.method == "POST"
    assert post_req.url.path == "/api/v1/trials"
    body = json.loads(post_req.content)
    assert body == {
        "task_id": "humaneval/HumanEval/0",
        "config": {
            "agent_name": "claude-code",
            "agent_model": {
                "provider": "openai",
                "name": "gpt-4o",
                "source": "api",
            },
        },
        "provider_connection_id": _CONN_ID,
        "provider_model_id": "gpt-4o",
    }
    assert post_req.headers["Authorization"] == "Bearer loom_admin_test123456"


def test_run_summary_uses_submitted_task_when_response_omits_task_id(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201, json={"id": _TRIAL_ID, "state": "queued"},
    )

    rc = main([
        "eval", "run",
        "--provider", "openai-prod",
        "--model", "gpt-4o",
        "--agent", "claude-code",
        "--task", "hello-world",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "task_id:          hello-world" in out
    assert "task_id:          (unknown)" not in out


def test_run_with_anthropic_provider_maps_type_to_provider(
    mock_server: MockServer,
) -> None:
    """type='anthropic' → agent_model.provider='anthropic'."""
    _stub_connection_lookup(
        mock_server, name="anthropic-prod", type_="anthropic",
    )
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201, json={"id": _TRIAL_ID, "task_id": "t", "state": "queued"},
    )
    rc = main([
        "eval", "run",
        "--provider", "anthropic-prod",
        "--model", "claude-opus-4-7",
        "--agent", "claude-code",
        "--task", "humaneval/HumanEval/0",
    ])
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["config"]["agent_model"]["provider"] == "anthropic"
    assert body["config"]["agent_model"]["name"] == "claude-opus-4-7"


def test_run_agent_provider_override_wins_over_type_mapping(
    mock_server: MockServer,
) -> None:
    """Issue #69: operator with a Together-hosted openai-compatible
    connection passes `--agent-provider together`; the override wins
    over the type→provider default (which would have produced 'openai'
    and silently dropped the rate-card lookup)."""
    _stub_connection_lookup(
        mock_server, name="together-prod", type_="openai-compatible",
    )
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        201, json={"id": _TRIAL_ID, "task_id": "t", "state": "queued"},
    )
    rc = main([
        "eval", "run",
        "--provider", "together-prod",
        "--model", "meta-llama/Llama-3.1-70B-Instruct",
        "--agent", "claude-code",
        "--task", "humaneval/HumanEval/0",
        "--agent-provider", "together",
    ])
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["config"]["agent_model"]["provider"] == "together"
    assert body["config"]["agent_model"]["name"] == (
        "meta-llama/Llama-3.1-70B-Instruct"
    )


def test_batch_create_agent_provider_override(
    mock_server: MockServer,
) -> None:
    """Same override on batch create."""
    _stub_connection_lookup(
        mock_server, name="fireworks-prod", type_="custom",
    )
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={
            "batch_id": _BATCH_ID, "expected_trial_count": 1,
            "n_per_task": 1, "backend": "docker",
            "combinations": [], "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main([
        "eval", "batch", "create",
        "--provider", "fireworks-prod",
        "--model", "accounts/fireworks/models/llama-v3p1-70b-instruct",
        "--agent", "claude-code",
        "--benchmark", "humaneval",
        "--name", "fw-run",
        "--agent-provider", "fireworks_ai",
    ])
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["trial_config"]["agent_model"]["provider"] == "fireworks_ai"


def test_run_unknown_provider_returns_1(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Provider lookup miss → exit 1, no POST issued."""
    mock_server.canned[("GET", "/api/v1/provider-connections")] = httpx.Response(
        200, json={"items": []},
    )
    rc = main([
        "eval", "run",
        "--provider", "nope",
        "--model", "gpt-4o",
        "--agent", "claude-code",
        "--task", "t",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "no provider connection named 'nope'" in err
    # GET happened; no POST.
    assert len(mock_server) == 1


def test_run_server_error_surfaces_detail(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/trials")] = httpx.Response(
        400, json={"detail": "unknown agent_name 'bogus'"},
    )
    rc = main([
        "eval", "run",
        "--provider", "openai-prod",
        "--model", "gpt-4o",
        "--agent", "bogus",
        "--task", "t",
    ])
    assert rc == 1
    err = capsys.readouterr().err
    assert "HTTP 400" in err
    assert "unknown agent_name" in err


# ──────────────────────────────────────────────────────────────────────
# eval batch create
# ──────────────────────────────────────────────────────────────────────


def test_batch_create_with_benchmark_shortcut(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 5,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main([
        "eval", "batch", "create",
        "--provider", "openai-prod",
        "--model", "gpt-4o",
        "--agent", "claude-code",
        "--benchmark", "humaneval",
        "--name", "smoke-run",
    ])
    assert rc == 0
    assert "Created batch 'smoke-run'" in capsys.readouterr().out

    body = json.loads(mock_server[1].content)
    assert body["name"] == "smoke-run"
    assert body["task_filter"] == {"benchmark_id": "humaneval"}
    assert body["trial_config"] == {
        "agent_name": "claude-code",
        "agent_model": {
            "provider": "openai", "name": "gpt-4o", "source": "api",
        },
    }
    assert body["provider_connection_id"] == _CONN_ID
    assert body["provider_model_id"] == "gpt-4o"


def test_batch_create_summary_uses_submitted_name_when_response_omits_name(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={
            "batch_id": _BATCH_ID,
            "expected_trial_count": 5,
            "n_per_task": 1,
            "backend": "docker",
            "combinations": [],
            "state": "submitted",
            "created_at": "2026-06-16T00:00:00Z",
        },
    )
    rc = main([
        "eval", "batch", "create",
        "--provider", "openai-prod",
        "--model", "gpt-4o",
        "--agent", "claude-code",
        "--benchmark", "humaneval",
        "--name", "smoke-run",
    ])

    assert rc == 0
    out = capsys.readouterr().out
    assert "name:                  smoke-run" in out
    assert "name:                  (unset)" not in out


def test_batch_create_task_filter_json(
    mock_server: MockServer,
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "expected_trial_count": 3,
                   "n_per_task": 1, "backend": "docker",
                   "combinations": [], "state": "submitted",
                   "created_at": "2026-06-16T00:00:00Z"},
    )
    filt = '{"benchmark_id":"humaneval","subset_kind":"first_n","n":3}'
    rc = main([
        "eval", "batch", "create",
        "--provider", "openai-prod", "--model", "gpt-4o",
        "--agent", "claude-code", "--task-filter", filt,
        "--name", "first3",
    ])
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["task_filter"] == {
        "benchmark_id": "humaneval",
        "subset_kind": "first_n",
        "n": 3,
    }


def test_batch_create_task_filter_at_path(
    mock_server: MockServer, tmp_path: Path,
) -> None:
    """`--task-filter @path/to/file.json` reads from disk."""
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "expected_trial_count": 1,
                   "n_per_task": 1, "backend": "docker",
                   "combinations": [], "state": "submitted",
                   "created_at": "2026-06-16T00:00:00Z"},
    )
    f = tmp_path / "filt.json"
    f.write_text('{"task_ids":["humaneval/HumanEval/0"]}')
    rc = main([
        "eval", "batch", "create",
        "--provider", "openai-prod", "--model", "gpt-4o",
        "--agent", "claude-code", "--task-filter", f"@{f}",
        "--name", "n",
    ])
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["task_filter"] == {"task_ids": ["humaneval/HumanEval/0"]}


def test_batch_create_benchmark_and_task_filter_mutually_exclusive(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main([
        "eval", "batch", "create",
        "--provider", "openai-prod", "--model", "gpt-4o",
        "--agent", "claude-code",
        "--benchmark", "humaneval",
        "--task-filter", '{"benchmark_id":"humaneval"}',
        "--name", "n",
    ])
    assert rc == 2
    assert "mutually exclusive" in capsys.readouterr().err
    # Connection lookup happened, but no POST.
    paths = [(r.method, r.url.path) for r in mock_server.requests]
    assert ("POST", "/api/v1/batches") not in paths


def test_batch_create_requires_benchmark_or_filter(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    _stub_connection_lookup(mock_server)
    rc = main([
        "eval", "batch", "create",
        "--provider", "openai-prod", "--model", "gpt-4o",
        "--agent", "claude-code", "--name", "n",
    ])
    assert rc == 2
    assert "one of --benchmark or --task-filter" in capsys.readouterr().err


def test_batch_create_invalid_task_filter_json_rejected_at_argparse(
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main([
            "eval", "batch", "create",
            "--provider", "openai-prod", "--model", "gpt-4o",
            "--agent", "claude-code",
            "--task-filter", "{not json",
            "--name", "n",
        ])
    assert exc.value.code == 2
    assert "invalid JSON" in capsys.readouterr().err


def test_batch_create_forwards_optional_fields(
    mock_server: MockServer,
) -> None:
    _stub_connection_lookup(mock_server)
    mock_server.canned[("POST", "/api/v1/batches")] = httpx.Response(
        201, json={"batch_id": _BATCH_ID, "expected_trial_count": 10,
                   "n_per_task": 2, "backend": "fake",
                   "combinations": [], "state": "submitted",
                   "created_at": "2026-06-16T00:00:00Z"},
    )
    rc = main([
        "eval", "batch", "create",
        "--provider", "openai-prod", "--model", "gpt-4o",
        "--agent", "claude-code", "--benchmark", "humaneval",
        "--name", "n", "--description", "smoke",
        "--n-per-task", "2", "--backend", "fake",
    ])
    assert rc == 0
    body = json.loads(mock_server[1].content)
    assert body["description"] == "smoke"
    assert body["n_per_task"] == 2
    assert body["backend"] == "fake"


# ──────────────────────────────────────────────────────────────────────
# eval batch list / show / cancel
# ──────────────────────────────────────────────────────────────────────


def test_batch_list_empty(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200, json={"items": [], "next_cursor": None},
    )
    rc = main(["eval", "batch", "list"])
    assert rc == 0
    assert "no batches" in capsys.readouterr().out


def test_batch_list_table_and_state_filter_param(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200, json={"items": [{
            "id": _BATCH_ID, "team_id": "x", "name": "b1",
            "description": None, "task_filter": {}, "trial_config": {},
            "state": "running", "result_status": None,
            "created_at": "2026-06-16T00:00:00Z", "finished_at": None,
            "created_by_token_prefix": "abc",
            "expected_trial_count": 7, "n_per_task": 1,
            "backend": "docker", "combinations": [],
        }], "next_cursor": None},
    )
    rc = main(["eval", "batch", "list", "--state", "running,queued"])
    assert rc == 0
    out = capsys.readouterr().out
    assert _BATCH_ID in out
    assert "b1" in out
    # Verify state param forwarded to server.
    assert mock_server[0].url.params.get("state") == "running,queued"


def test_batch_list_warns_on_truncation(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #67: non-null `next_cursor` triggers a stderr hint so
    operators know they're seeing a partial result. Stdout still
    contains just the table so `| awk` pipelines keep working."""
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200, json={"items": [{
            "id": _BATCH_ID, "team_id": "x", "name": "b1",
            "description": None, "task_filter": {}, "trial_config": {},
            "state": "running", "result_status": None,
            "created_at": "2026-06-16T00:00:00Z", "finished_at": None,
            "created_by_token_prefix": "abc",
            "expected_trial_count": 7, "n_per_task": 1,
            "backend": "docker", "combinations": [],
        }], "next_cursor": "opaque-cursor-token"},
    )
    rc = main(["eval", "batch", "list"])
    assert rc == 0
    out_err = capsys.readouterr()
    # Hint on stderr only.
    assert "more" in out_err.err
    assert "--limit" in out_err.err
    assert "more" not in out_err.out  # stdout stays clean


def test_batch_list_no_warning_when_not_truncated(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """`next_cursor=None` means full result; no hint."""
    mock_server.canned[("GET", "/api/v1/batches")] = httpx.Response(
        200, json={"items": [{
            "id": _BATCH_ID, "team_id": "x", "name": "b1",
            "description": None, "task_filter": {}, "trial_config": {},
            "state": "running", "result_status": None,
            "created_at": "2026-06-16T00:00:00Z", "finished_at": None,
            "created_by_token_prefix": "abc",
            "expected_trial_count": 7, "n_per_task": 1,
            "backend": "docker", "combinations": [],
        }], "next_cursor": None},
    )
    rc = main(["eval", "batch", "list"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "more" not in err


def test_trial_list_warns_on_truncation(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    """Issue #67: matching hint on trial list."""
    mock_server.canned[("GET", "/api/v1/trials")] = httpx.Response(
        200, json={"items": [{
            "id": _TRIAL_ID, "task_id": "t", "team_id": "x",
            "state": "succeeded", "failure_reason": None,
            "submitted_at": "2026-06-16T00:00:00Z", "started_at": None,
            "finished_at": None, "attempt_count": 1,
            "aggregate_reward": 0.5, "cost_usd": 0.01,
            "agent_name": "claude-code", "model": None,
        }], "next_cursor": "opaque-cursor-token"},
    )
    rc = main(["eval", "trial", "list"])
    assert rc == 0
    err = capsys.readouterr().err
    assert "more" in err
    assert "--task-id" in err  # trial list mentions task-id filter


def test_batch_show_renders_rollup(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/batches/{_BATCH_ID}")] = httpx.Response(
        200, json={
            "id": _BATCH_ID, "team_id": "x", "name": "b1",
            "description": None, "task_filter": {}, "trial_config": {},
            "state": "succeeded", "result_status": "ok",
            "created_at": "2026-06-16T00:00:00Z", "finished_at": None,
            "created_by_token_prefix": "abc",
            "expected_trial_count": 4, "n_per_task": 1,
            "backend": "docker", "combinations": [],
            "trial_summary": {"queued": 0, "claimed": 0, "running": 0,
                              "succeeded": 3, "failed": 1, "cancelled": 0},
            "aggregate_reward": 0.75,
            "total_cost_usd": 1.234,
        },
    )
    rc = main(["eval", "batch", "show", _BATCH_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "s=3" in out
    assert "f=1" in out
    assert "0.75" in out
    assert "1.234" in out


def test_batch_cancel(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[
        ("POST", f"/api/v1/batches/{_BATCH_ID}/cancel")
    ] = httpx.Response(200, json={"batch_id": _BATCH_ID, "state": "cancelled"})
    rc = main(["eval", "batch", "cancel", _BATCH_ID])
    assert rc == 0
    assert "Cancelled batch" in capsys.readouterr().out


# ──────────────────────────────────────────────────────────────────────
# eval trial list / show
# ──────────────────────────────────────────────────────────────────────


def test_trial_list_with_task_id_filter(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", "/api/v1/trials")] = httpx.Response(
        200, json={"items": [{
            "id": _TRIAL_ID, "task_id": "humaneval/HumanEval/0",
            "team_id": "x", "state": "succeeded", "failure_reason": None,
            "submitted_at": "2026-06-16T00:00:00Z", "started_at": None,
            "finished_at": None, "attempt_count": 1,
            "aggregate_reward": 0.875, "cost_usd": 0.0123,
            "agent_name": "claude-code",
            "model": {"provider": "openai", "name": "gpt-4o"},
        }], "next_cursor": None},
    )
    rc = main([
        "eval", "trial", "list",
        "--task-id", "humaneval/HumanEval/0",
    ])
    assert rc == 0
    out = capsys.readouterr().out
    assert _TRIAL_ID in out
    assert "0.875" in out
    assert mock_server[0].url.params.get("task_id") == "humaneval/HumanEval/0"


def test_trial_show_renders_presigned_urls_when_ready(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        200, json={
            "id": _TRIAL_ID, "task_id": "t", "team_id": "x",
            "state": "succeeded", "failure_reason": None,
            "submitted_at": "2026-06-16T00:00:00Z",
            "started_at": "2026-06-16T00:01:00Z",
            "finished_at": "2026-06-16T00:02:00Z",
            "attempt_count": 1, "aggregate_reward": 1.0,
            "cost_usd": 0.05, "agent_name": "claude-code",
            "model": {"provider": "openai", "name": "gpt-4o"},
            "atif_url": "https://minio.test/atif?sig=abc",
            "trajectory_url": "https://minio.test/traj?sig=abc",
            "atif_ready": True,
            "trajectory_ready": True,
            "artifacts": [],
        },
    )
    rc = main(["eval", "trial", "show", _TRIAL_ID])
    assert rc == 0
    out = capsys.readouterr().out
    assert "atif_url:" in out
    assert "trajectory_url:" in out


def test_trial_show_json_format(
    mock_server: MockServer, capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "id": _TRIAL_ID, "task_id": "t", "team_id": "x",
        "state": "queued", "failure_reason": None,
        "submitted_at": "2026-06-16T00:00:00Z",
        "started_at": None, "finished_at": None,
        "attempt_count": 0, "aggregate_reward": None, "cost_usd": 0.0,
        "agent_name": None, "model": None,
        "atif_url": "https://minio.test/atif?sig=abc",
        "trajectory_url": "https://minio.test/traj?sig=abc",
        "atif_ready": False, "trajectory_ready": False, "artifacts": [],
    }
    mock_server.canned[("GET", f"/api/v1/trials/{_TRIAL_ID}")] = httpx.Response(
        200, json=payload,
    )
    rc = main(["eval", "trial", "show", _TRIAL_ID, "--format", "json"])
    assert rc == 0
    parsed = json.loads(capsys.readouterr().out)
    assert parsed == payload


# ──────────────────────────────────────────────────────────────────────
# not-logged-in path
# ──────────────────────────────────────────────────────────────────────


def test_eval_run_not_logged_in(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main(["auth", "logout"])
    capsys.readouterr()
    rc = main([
        "eval", "run",
        "--provider", "n", "--model", "m", "--agent", "a", "--task", "t",
    ])
    assert rc == 2
    err = capsys.readouterr().err
    assert "not logged in" in err


def test_eval_batch_list_not_logged_in(
    capsys: pytest.CaptureFixture[str],
) -> None:
    main(["auth", "logout"])
    capsys.readouterr()
    rc = main(["eval", "batch", "list"])
    assert rc == 2
    assert "not logged in" in capsys.readouterr().err
