import pytest

from loom.errors import ConfigError
from loom_drivers.daytona.config import DaytonaConfig


def test_loads_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAYTONA_API_KEY", "key-abc")
    monkeypatch.setenv("DAYTONA_API_URL", "https://app.daytona.io")
    monkeypatch.setenv("DAYTONA_TARGET", "us")
    cfg = DaytonaConfig.from_env()
    assert cfg.api_key == "key-abc"
    assert cfg.api_url == "https://app.daytona.io"
    assert cfg.target == "us"
    assert cfg.warm_pool_size == 0
    assert cfg.delete_timeout_sec == 60.0


def test_missing_api_key_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    monkeypatch.delenv("DAYTONA_JWT_TOKEN", raising=False)
    with pytest.raises(ConfigError, match="DAYTONA_API_KEY"):
        DaytonaConfig.from_env()


def test_jwt_alternative(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DAYTONA_API_KEY", raising=False)
    monkeypatch.setenv("DAYTONA_JWT_TOKEN", "jwt-xyz")
    cfg = DaytonaConfig.from_env()
    assert cfg.jwt_token == "jwt-xyz"
    assert cfg.api_key is None


def test_warm_pool_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DAYTONA_API_KEY", "k")
    monkeypatch.setenv("LOOM_DAYTONA_WARM_POOL", "4")
    cfg = DaytonaConfig.from_env()
    assert cfg.warm_pool_size == 4
