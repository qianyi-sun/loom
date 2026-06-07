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
    assert s.bind_host == "0.0.0.0"  # noqa: S104
    assert s.log_level == "info"
    assert s.minio_access_key.get_secret_value() == "k"
    assert s.minio_secret_key.get_secret_value() == "s"
    assert str(s.control_plane_url).startswith("http://cp:8080")
    # Forward-compat defaults are stable.
    assert s.signed_url_expiry_sec == 3600
    assert s.campaign_runner_batch_size == 50


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
    with pytest.raises(Exception):  # noqa: B017, PT011
        LoomServiceSettings(_env_file=None, unknown_field="x")  # type: ignore[call-arg]


def test_required_fields_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """db_url is required; missing it fails at construction."""
    env = _base_env()
    env.pop("LOOM_SVC_DB_URL")
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.delenv("LOOM_SVC_DB_URL", raising=False)
    with pytest.raises(Exception):  # noqa: B017, PT011
        LoomServiceSettings(_env_file=None)
