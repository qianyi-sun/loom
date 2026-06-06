import pytest
from pydantic import ValidationError

from loom_llm_gateway.config import GatewaySettings


def test_required_fields_missing_raise(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("LOOM_GW_DB_URL", raising=False)
    monkeypatch.delenv("LOOM_GW_ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("LOOM_GW_OPENAI_API_KEY", raising=False)
    with pytest.raises(ValidationError):
        GatewaySettings(_env_file=None)


def test_loads_from_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("LOOM_GW_DB_URL", "postgresql+psycopg://u:p@h/db")
    monkeypatch.setenv("LOOM_GW_ANTHROPIC_API_KEY", "ak-test")
    monkeypatch.setenv("LOOM_GW_OPENAI_API_KEY", "ok-test")
    monkeypatch.setenv("LOOM_GW_BIND_PORT", "9000")
    s = GatewaySettings(_env_file=None)
    assert str(s.db_url).startswith("postgresql")
    assert s.anthropic_api_key is not None
    assert s.anthropic_api_key.get_secret_value() == "ak-test"
    assert s.bind_port == 9000
    assert s.log_level == "info"
