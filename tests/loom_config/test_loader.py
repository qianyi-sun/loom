"""Loader for `config/loom-schema.toml`."""
from __future__ import annotations

from pathlib import Path

import pytest

from loom_config.loader import (
    Schema,
    load_schema,
)


def _write_minimal_schema(tmp_path: Path) -> Path:
    """Smallest schema that exercises every entry shape."""
    p = tmp_path / "schema.toml"
    p.write_text(
        """
[meta]
version = 1

[service_prefix]
control-plane = "CP"
llm-gateway   = "GW"

[service_config.step_jwt_signing_key]
used_by     = ["control-plane", "llm-gateway"]
python_type = "SecretStr"
required    = true
secret      = { key = "step-jwt-signing-key", generate = "openssl rand -hex 64" }

[service_config.db_url]
used_by     = ["control-plane", "llm-gateway"]
python_type = "PostgresDsn"
required    = true
secret      = { key_per_service = { control-plane = "cp-db-url", llm-gateway = "gw-db-url" } }

[service_config.minio_endpoint]
used_by     = ["control-plane"]
python_type = "str"
default     = "http://loom-minio:9000"

[service_config.bind_port]
used_by             = ["control-plane", "llm-gateway"]
python_type         = "int"
default_per_service = { control-plane = 8080, llm-gateway = 9100 }

[render_config.image_tag]
python_type = "str"
default     = "0.7"

[render_config.provider_egress_allowlist]
python_type = "str_list"
default     = []
""",
        encoding="utf-8",
    )
    return p


def test_loads_valid_schema(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    assert isinstance(schema, Schema)
    assert schema.version == 1
    assert schema.service_prefix == {"control-plane": "CP", "llm-gateway": "GW"}
    assert sorted(schema.service_config.keys()) == [
        "bind_port", "db_url", "minio_endpoint", "step_jwt_signing_key",
    ]
    assert "image_tag" in schema.render_config
    assert schema.render_config["provider_egress_allowlist"].default == []


def test_service_config_for_filters_by_used_by(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    entries = list(schema.service_config_for("control-plane"))
    names = {e.name for e in entries}
    assert names == {"step_jwt_signing_key", "db_url", "minio_endpoint", "bind_port"}
    gw_entries = list(schema.service_config_for("llm-gateway"))
    gw_names = {e.name for e in gw_entries}
    assert gw_names == {"step_jwt_signing_key", "db_url", "bind_port"}


def test_env_var_for_derives_from_prefix(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    step = schema.service_config["step_jwt_signing_key"]
    assert step.env_var_for("control-plane") == "LOOM_CP_STEP_JWT_SIGNING_KEY"
    assert step.env_var_for("llm-gateway") == "LOOM_GW_STEP_JWT_SIGNING_KEY"


def test_secret_key_for_single_vs_per_service(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    step = schema.service_config["step_jwt_signing_key"]
    assert step.secret_key_for("control-plane") == "step-jwt-signing-key"
    assert step.secret_key_for("llm-gateway") == "step-jwt-signing-key"
    db = schema.service_config["db_url"]
    assert db.secret_key_for("control-plane") == "cp-db-url"
    assert db.secret_key_for("llm-gateway") == "gw-db-url"


def test_value_for_uses_per_service_default(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    bind = schema.service_config["bind_port"]
    assert bind.value_for("control-plane") == 8080
    assert bind.value_for("llm-gateway") == 9100


def test_value_for_uses_shared_default(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    minio = schema.service_config["minio_endpoint"]
    assert minio.value_for("control-plane") == "http://loom-minio:9000"


def test_rejects_secret_with_both_key_and_per_service(tmp_path: Path) -> None:
    p = tmp_path / "bad.toml"
    p.write_text(
        """
[meta]
version = 1
[service_prefix]
control-plane = "CP"
[service_config.x]
used_by = ["control-plane"]
python_type = "SecretStr"
secret = { key = "x", key_per_service = { control-plane = "x" } }
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="exactly one of"):
        load_schema(p)


def test_rejects_unknown_python_type(tmp_path: Path) -> None:
    p = tmp_path / "bad.toml"
    p.write_text(
        """
[meta]
version = 1
[service_prefix]
control-plane = "CP"
[service_config.x]
used_by = ["control-plane"]
python_type = "Banana"
default = "y"
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="unknown python_type"):
        load_schema(p)


def test_rejects_used_by_service_not_in_prefix(tmp_path: Path) -> None:
    p = tmp_path / "bad.toml"
    p.write_text(
        """
[meta]
version = 1
[service_prefix]
control-plane = "CP"
[service_config.x]
used_by = ["control-plane", "ghost-service"]
python_type = "str"
default = "y"
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="ghost-service"):
        load_schema(p)


def test_secret_key_for_raises_on_non_secret_entry(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    minio = schema.service_config["minio_endpoint"]
    with pytest.raises(ValueError, match="not secret-backed"):
        minio.secret_key_for("control-plane")


def test_value_for_raises_on_secret_entry(tmp_path: Path) -> None:
    schema = load_schema(_write_minimal_schema(tmp_path))
    step = schema.service_config["step_jwt_signing_key"]
    with pytest.raises(ValueError, match="secret-backed"):
        step.value_for("control-plane")


def test_value_for_raises_when_no_default(tmp_path: Path) -> None:
    """A non-secret entry with no default and no default_per_service
    raises when value_for is called — codegen should never hit this
    (such fields are emitted as `T | None = None`) but the contract is
    enforced explicitly."""
    p = tmp_path / "no_default.toml"
    p.write_text(
        '''
[meta]
version = 1
[service_prefix]
control-plane = "CP"
[service_config.x]
used_by = ["control-plane"]
python_type = "str"
''',
        encoding="utf-8",
    )
    schema = load_schema(p)
    x = schema.service_config["x"]
    with pytest.raises(ValueError, match="no default"):
        x.value_for("control-plane")
