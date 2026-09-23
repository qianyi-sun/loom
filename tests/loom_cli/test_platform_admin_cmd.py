"""Platform-admin command contract and safe errors."""

from __future__ import annotations

import json
from uuid import uuid4

import httpx
import pytest

from loom_cli.__main__ import main
from loom_cli.config import LoomConfig, save_config
from loom_cli.server_client import authed_client


@pytest.mark.parametrize("operation", ["grant", "revoke"])
@pytest.mark.parametrize("status", [200, 403, 404, 409])
def test_platform_admin_command(tmp_xdg_home, monkeypatch, capsys, operation, status):
    user_id = str(uuid4())
    secret = "loom_api_" + "T" * 43
    cfg = LoomConfig(server_url="http://localhost:8081", auth_token=secret)
    save_config(cfg)

    def handle(request):
        assert request.method == "POST"
        assert request.url.path == f"/api/v1/admin/users/{user_id}/platform-admin/{operation}"
        assert request.headers["X-Loom-Admin-Actor"] == "operator"
        expected = {"ensure_admin_team": False} if operation == "grant" else {"credential_policy": "revoke_all"}
        assert json.loads(request.content) == expected
        return httpx.Response(status, json={"changed": False} if status == 200 else {"detail": "denied"})

    monkeypatch.setattr("loom_cli.platform_admin_cmd.authed_client", lambda config: authed_client(
        config, transport=httpx.MockTransport(handle),
    ))
    flags = ["--no-ensure-admin-team"] if operation == "grant" else ["--credential-policy", "revoke_all"]
    assert main(["admin", "platform-admin", operation, "--user-id", user_id,
                 "--admin-actor", "operator", *flags]) == (0 if status == 200 else 1)
    output = capsys.readouterr()
    assert secret not in output.out + output.err
    if status == 200:
        assert json.loads(output.out) == {"changed": False}


@pytest.mark.parametrize("args", [
    ["grant", "--user-id", "ambiguous-name"],
    ["revoke", "--user-id", str(uuid4())],
    ["revoke", "--user-id", str(uuid4()), "--credential-policy", "preserve"],
])
def test_invalid_selector_or_missing_revoke_policy(tmp_xdg_home, args):
    with pytest.raises(SystemExit) as error:
        main(["admin", "platform-admin", *args])
    assert error.value.code == 2


def test_platform_admin_requires_login(tmp_xdg_home, capsys):
    assert main(["admin", "platform-admin", "grant", "--user-id", str(uuid4())]) == 2
    assert "not logged in" in capsys.readouterr().err
