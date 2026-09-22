"""Personal login exchanges proof without copying management authority."""

from __future__ import annotations

import copy
import json

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, config_path, load_config, save_config
from loom_cli.contexts import selected_context

ENVIRONMENT = "20000000-0000-4000-8000-000000000001"
INCARNATION = "30000000-0000-4000-8000-000000000001"
OWNER = "40000000-0000-4000-8000-000000000001"
TEAM = "50000000-0000-4000-8000-000000000001"
CONTEXT = "dev-alice-" + ENVIRONMENT.replace("-", "")
PROOF = "loom_env_login_" + "a" * 43


@pytest.fixture
def login_http(monkeypatch, tmp_xdg_home):
    from loom_cli import environment_client, environment_login
    from loom_cli.server_client import authed_client

    save_config(LoomConfig(server_url="https://management.example.com", auth_token="management-secret",
                          tokens={"openai": "private-model-key"}))
    registration = {"environment_id": ENVIRONMENT, "incarnation": INCARNATION, "owner_user_id": OWNER,
                    "owner_team_id": TEAM, "scope": "personal", "kind": "development", "slug": "alice",
                    "cluster_id": "cluster", "physical_pool_id": "pool", "application_namespace": "loom-dev-alice",
                    "execution_namespace": "loom-run-" + INCARNATION.replace("-", ""),
                    "build_namespace": "loom-run-" + INCARNATION.replace("-", "") + "-build",
                    "public_host": "alice.example.com", "target_id": "env-" + ENVIRONMENT.replace("-", ""),
                    "deployment_generation": 1, "desired_state": "active"}
    status = {"registration": registration, "operation": {
        "operation_id": INCARNATION, "environment_id": ENVIRONMENT, "deployment_generation": 1,
        "action": "create", "phase": "completed", "error_code": None, "execution_enabled": False,
    }}
    proof = {"environment_id": ENVIRONMENT, "incarnation": INCARNATION, "owner_user_id": OWNER,
             "owner_team_id": TEAM, "origin": "https://alice.example.com", "login_token": PROOF, "expires_in": 90}
    session = {"user": {"id": OWNER, "username": "owner-" + OWNER.replace("-", ""), "email": None,
                        "display_name": "alice", "is_platform_admin": False},
               "teams": [{"id": TEAM, "name": "Development alice", "role": "owner"}],
               "current_team": {"id": TEAM, "name": "Development alice", "role": "owner"},
               "role": "owner", "scopes": ["read:own", "submit", "tokens:manage", "providers:manage", "team:manage"],
               "is_platform_admin": False, "csrf_token": "new-child-csrf"}
    cookie = "__Host-loom_session=new-child-session; Secure; HttpOnly; Path=/; SameSite=Lax"
    state = {"status": status, "proof": proof, "session": session, "cookie": cookie, "child_status": 200}
    requests = []

    def handle(request):
        requests.append(request)
        if request.url.host == "management.example.com":
            assert request.headers.get("authorization") == "Bearer management-secret"
            assert "cookie" not in request.headers
            if request.method == "GET":
                return httpx.Response(200, json=copy.deepcopy(state["status"]))
            assert request.url.path == f"/api/v1/environments/{ENVIRONMENT}/login"
            return httpx.Response(200, json=copy.deepcopy(state["proof"]))
        assert request.url.host == "alice.example.com"
        assert request.url.path == "/api/v1/auth/login/complete"
        assert not any(header in request.headers for header in ("authorization", "cookie", "x-loom-csrf"))
        assert json.loads(request.content) == {"token": PROOF}
        return httpx.Response(state["child_status"], json=state["session"], headers={
            "set-cookie": state["cookie"], "location": "https://foreign.example.com/stolen",
        })

    monkeypatch.setattr(environment_client, "authed_client", lambda cfg: authed_client(cfg, transport=httpx.MockTransport(handle)))
    monkeypatch.setattr(environment_login, "child_http_client", lambda origin: httpx.Client(
        base_url=origin, transport=httpx.MockTransport(handle), follow_redirects=False,
    ))
    return state, requests


def test_personal_login_preserves_management_and_saves_only_separate_child_credentials(login_http, capsys):
    _, requests = login_http
    original = config_path().read_bytes()
    assert main(["dev", "login", ENVIRONMENT]) == 0
    assert config_path().read_bytes() == original
    with selected_context(CONTEXT):
        cfg = load_config()
        assert cfg.server_url == "https://alice.example.com"
        assert cfg.auth_session_cookie == "new-child-session"
        assert cfg.auth_session_cookie_name == "__Host-loom_session"
        assert cfg.auth_csrf_token == "new-child-csrf"
        assert cfg.auth_token is None and cfg.tokens == {} and cfg.local_providers == {}
        assert cfg.managed_environment.environment_id == ENVIRONMENT
        assert cfg.managed_environment.incarnation == INCARNATION
    assert [r.url.host for r in requests] == ["management.example.com", "management.example.com", "alice.example.com"]
    output = capsys.readouterr()
    assert "loom --context " + CONTEXT in output.out
    assert not any(secret in output.out + output.err for secret in (PROOF, "new-child-session", "new-child-csrf", "management-secret"))


@pytest.mark.parametrize("field,value", [("origin", "https://foreign.example.com"), ("incarnation", ENVIRONMENT),
                                        ("owner_user_id", TEAM), ("expires_in", True), ("login_token", "wrong")])
def test_changed_proof_cannot_select_another_child_or_save_credentials(login_http, field, value, capsys):
    state, requests = login_http
    state["proof"][field] = value
    assert main(["dev", "login", ENVIRONMENT]) == 1
    assert all(request.url.host == "management.example.com" for request in requests)
    with selected_context(CONTEXT):
        assert not config_path().exists()
    assert PROOF not in capsys.readouterr().err


@pytest.mark.parametrize("fault", ["redirect", "foreign-user", "foreign-team", "admin", "missing-csrf", "insecure-cookie", "domain-cookie"])
def test_child_response_cannot_install_wrong_or_insecure_session(login_http, fault, capsys):
    state, requests = login_http
    if fault == "redirect":
        state["child_status"] = 302
    elif fault == "foreign-user":
        state["session"]["user"]["id"] = TEAM
    elif fault == "foreign-team":
        state["session"]["current_team"]["id"] = OWNER
    elif fault == "admin":
        state["session"]["is_platform_admin"] = True
    elif fault == "missing-csrf":
        state["session"].pop("csrf_token")
    elif fault == "insecure-cookie":
        state["cookie"] = "__Host-loom_session=insecure; Path=/"
    else:
        state["cookie"] += "; Domain=alice.example.com"
    assert main(["dev", "login", ENVIRONMENT]) == 1
    assert len(requests) == 3
    with selected_context(CONTEXT):
        assert not config_path().exists()
    assert load_config().auth_token == "management-secret"
    assert PROOF not in capsys.readouterr().err


def test_login_to_destroyed_environment_does_not_issue_or_consume_a_proof(login_http):
    state, requests = login_http
    state["status"]["registration"]["desired_state"] = "destroyed"
    assert main(["dev", "login", ENVIRONMENT]) == 1
    assert len(requests) == 1
