from __future__ import annotations

from typing import Any

import pytest

from loom_control_plane.config import ControlPlaneSettings
from loom_llm_gateway.config import GatewaySettings
from loom_service.config import LoomServiceSettings


DIRECT = "postgresql+psycopg://loom:pw@loom-postgres:5432/loom"
POOL = "postgresql+psycopg://loom:pw@loom-pgbouncer:6432/loom"


def _stub_required_env(cls: type[Any], monkeypatch: pytest.MonkeyPatch) -> None:
    """Fill in stubs for every required Settings field so instantiation
    succeeds during the test. Reads model_fields to discover required fields
    and applies a type-appropriate stub value."""
    import os
    prefix_map = {
        "ControlPlaneSettings": "LOOM_CP_",
        "LoomServiceSettings": "LOOM_SVC_",
        "GatewaySettings": "LOOM_GW_",
    }
    prefix = prefix_map[cls.__name__]
    for name, field in cls.model_fields.items():
        if not field.is_required():
            continue
        env_name = prefix + name.upper()
        if os.environ.get(env_name):
            continue
        ann = str(field.annotation)
        if "PostgresDsn" in ann or "HttpUrl" in ann or "AnyUrl" in ann:
            stub = "postgresql+psycopg://loom:pw@loom-postgres:5432/loom"
        elif "int" in ann.lower():
            stub = "1"
        else:
            stub = "stub"
        monkeypatch.setenv(env_name, stub)


@pytest.mark.parametrize(
    "settings_cls",
    [ControlPlaneSettings, LoomServiceSettings, GatewaySettings],
)
def test_db_engine_url_falls_back_to_db_url_when_pool_empty(
    settings_cls: type[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix_map = {
        "ControlPlaneSettings": "LOOM_CP_",
        "LoomServiceSettings": "LOOM_SVC_",
        "GatewaySettings": "LOOM_GW_",
    }
    prefix = prefix_map[settings_cls.__name__]
    monkeypatch.setenv(f"{prefix}DB_URL", DIRECT)
    # db_url_pool is Optional[PostgresDsn] = None; unset the env var so pydantic
    # uses the None default (an empty string is not a valid PostgresDsn).
    monkeypatch.delenv(f"{prefix}DB_URL_POOL", raising=False)
    _stub_required_env(settings_cls, monkeypatch)

    s = settings_cls()
    assert s.db_engine_url == DIRECT
    assert s.db_engine_connect_args == {}


@pytest.mark.parametrize(
    "settings_cls",
    [ControlPlaneSettings, LoomServiceSettings, GatewaySettings],
)
def test_db_engine_url_uses_pool_when_set(
    settings_cls: type[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    prefix_map = {
        "ControlPlaneSettings": "LOOM_CP_",
        "LoomServiceSettings": "LOOM_SVC_",
        "GatewaySettings": "LOOM_GW_",
    }
    prefix = prefix_map[settings_cls.__name__]
    monkeypatch.setenv(f"{prefix}DB_URL", DIRECT)
    monkeypatch.setenv(f"{prefix}DB_URL_POOL", POOL)
    _stub_required_env(settings_cls, monkeypatch)

    s = settings_cls()
    assert s.db_engine_url == POOL
    assert s.db_engine_connect_args == {"prepare_threshold": None}
