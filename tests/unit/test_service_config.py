"""LoomServiceSettings env-var parsing (Plan 17 Task 1)."""

from __future__ import annotations

import pytest

from loom_service.config import LoomServiceSettings


def _base_env() -> dict[str, str]:
    return {
        "LOOM_SVC_DB_URL": "postgresql+psycopg://u:p@h/db",
        "LOOM_SVC_MINIO_ENDPOINT": "http://minio:9000",
        "LOOM_SVC_MINIO_ACCESS_KEY": "k",
        "LOOM_SVC_MINIO_SECRET_KEY": "s",
        "LOOM_SVC_CONTROL_PLANE_URL": "http://cp:8080/",
        "LOOM_SVC_GATEWAY_URL": "http://gw:9100/",
    }


def test_env_vars_parse(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    s = LoomServiceSettings(_env_file=None)
    assert s.bind_port == 8090
    assert s.bind_host == "0.0.0.0"
    assert s.log_level == "info"
    assert s.minio_access_key.get_secret_value() == "k"
    assert s.minio_secret_key.get_secret_value() == "s"
    assert str(s.control_plane_url).startswith("http://cp:8080")
    # Forward-compat defaults are stable.
    assert s.signed_url_expiry_sec == 3600
    assert s.batch_runner_batch_size == 50
    assert s.auth_session_cookie_name == "loom_session"
    assert s.auth_csrf_header_name == "X-Loom-CSRF"
    assert s.auth_session_ttl_sec == 604800
    assert s.auth_login_challenge_ttl_sec == 900
    assert s.auth_return_login_token is False
    # Dev-reload defaults off so production never accidentally
    # ships with a file-watcher in each container.
    assert s.dev_reload is False
    assert s.personal_dev_acceptance_binding_json == "{}"
    assert s.personal_dev_acceptance_plan_sha256 == ""
    assert s.personal_dev_runtime_mode == "shadow"
    assert s.personal_dev_operational_binding_json == "{}"
    assert s.personal_dev_operational_plan_sha256 == ""
    assert s.personal_dev_activation_public_key_sha256 == ""


def test_dev_reload_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("LOOM_SVC_DEV_RELOAD", "1")
    s = LoomServiceSettings(_env_file=None)
    assert s.dev_reload is True


def test_admin_secret_file_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv(
        "LOOM_SVC_ADMIN_SECRET_FILE",
        "/var/run/loom/secrets/admin/secrets.toml",
    )

    s = LoomServiceSettings(_env_file=None)

    assert str(s.admin_secret_file).endswith("/secrets.toml")


def test_team_registration_defaults_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)

    s = LoomServiceSettings(_env_file=None)

    assert s.team_registration_open is False


def test_team_registration_open_env_var(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    monkeypatch.setenv("LOOM_SVC_TEAM_REGISTRATION_OPEN", "1")

    s = LoomServiceSettings(_env_file=None)

    assert s.team_registration_open is True


def test_extra_forbid_on_dict_init(monkeypatch: pytest.MonkeyPatch) -> None:
    """`extra="forbid"` rejects unknown fields passed to the constructor.

    NOTE: pydantic-settings does NOT iterate `os.environ` for unknown
    `LOOM_SVC_*` keys — it only fetches declared fields. So a misnamed
    env var is silently ignored at runtime. The forbid setting still
    matters for dict-init paths (test fixtures, programmatic config)
    where extra keys are caught."""
    for k, v in _base_env().items():
        monkeypatch.setenv(k, v)
    # Reading the base env succeeds.
    LoomServiceSettings(_env_file=None)
    # But adding an extra key via constructor kwargs fails.
    with pytest.raises(Exception):  # noqa: B017
        LoomServiceSettings(_env_file=None, unknown_field="x")  # type: ignore[call-arg]


def test_required_fields_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """db_url is required; missing it fails at construction."""
    env = _base_env()
    env.pop("LOOM_SVC_DB_URL")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("LOOM_SVC_DB_URL", raising=False)
    with pytest.raises(Exception):  # noqa: B017
        LoomServiceSettings(_env_file=None)
