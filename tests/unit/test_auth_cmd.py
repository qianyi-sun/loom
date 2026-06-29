from __future__ import annotations

from loom_cli.auth_cmd import _principal_label


def test_principal_label_uses_server_credential_type() -> None:
    assert _principal_label({
        "auth_kind": "bearer",
        "principal_type": "team",
        "credential_type": "user_owned_api_token",
    }) == "user-owned API token"
    assert _principal_label({
        "auth_kind": "bearer",
        "principal_type": "team",
        "credential_type": "legacy_team_token",
    }) == "legacy team token"


def test_principal_label_distinguishes_browser_session() -> None:
    assert _principal_label({
        "auth_kind": "session",
        "principal_type": "user",
        "credential_type": "browser_session",
    }) == "browser session"
