"""Test the `env:` macro renders correct YAML for each entry kind."""

from __future__ import annotations

from pathlib import Path

import yaml
from jinja2 import Environment, FileSystemLoader, StrictUndefined

from loom_config.loader import load_schema


def _env() -> Environment:
    return Environment(
        loader=FileSystemLoader("src/loom_cli/templates/k8s"),
        undefined=StrictUndefined,
        trim_blocks=True,
        lstrip_blocks=True,
    )


def _render(template_str: str, schema) -> str:
    env = _env()
    tmpl = env.from_string(template_str)
    return tmpl.render(schema=schema)


def test_macro_emits_secret_with_value_from() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    src = "{% import '_env.j2' as e %}{{ e.env_block('control-plane', schema) }}"
    out = _render(src, schema)
    parsed = yaml.safe_load(out)
    by_name = {entry["name"]: entry for entry in parsed["env"]}
    step = by_name["LOOM_CP_STEP_JWT_SIGNING_KEY"]
    assert step["valueFrom"]["secretKeyRef"]["name"] == "loom-secrets"
    assert step["valueFrom"]["secretKeyRef"]["key"] == "step-jwt-signing-key"


def test_macro_emits_per_service_secret_key() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    out = _render(
        "{% import '_env.j2' as e %}{{ e.env_block('control-plane', schema) }}",
        schema,
    )
    parsed = yaml.safe_load(out)
    by_name = {entry["name"]: entry for entry in parsed["env"]}
    assert by_name["LOOM_CP_DB_URL"]["valueFrom"]["secretKeyRef"]["key"] == "cp-db-url"
    out2 = _render(
        "{% import '_env.j2' as e %}{{ e.env_block('llm-gateway', schema) }}",
        schema,
    )
    by_name2 = {e["name"]: e for e in yaml.safe_load(out2)["env"]}
    assert by_name2["LOOM_GW_DB_URL"]["valueFrom"]["secretKeyRef"]["key"] == "gw-db-url"


def test_macro_mounts_optional_deployment_fence_capability_only_in_service() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    out = _render(
        "{% import '_env.j2' as e %}{{ e.env_block('loom-service', schema) }}",
        schema,
    )
    by_name = {entry["name"]: entry for entry in yaml.safe_load(out)["env"]}
    assert by_name["LOOM_SVC_TASKSET_FENCE_CANARY_TOKEN"] == {
        "name": "LOOM_SVC_TASKSET_FENCE_CANARY_TOKEN",
        "valueFrom": {
            "secretKeyRef": {
                "name": "loom-secrets",
                "key": "taskset-fence-canary-token",
                "optional": True,
            },
        },
    }
    control_plane = _render(
        "{% import '_env.j2' as e %}{{ e.env_block('control-plane', schema) }}",
        schema,
    )
    control_plane_names = {
        entry["name"] for entry in yaml.safe_load(control_plane)["env"]
    }
    assert "LOOM_CP_TASKSET_FENCE_CANARY_TOKEN" not in control_plane_names


def test_macro_emits_literal_value_as_string() -> None:
    """Critical: k8s rejects unquoted ints for `value:`."""
    schema = load_schema(Path("config/loom-schema.toml"))
    out = _render(
        "{% import '_env.j2' as e %}{{ e.env_block('control-plane', schema) }}",
        schema,
    )
    parsed = yaml.safe_load(out)
    by_name = {entry["name"]: entry for entry in parsed["env"]}
    bind = by_name["LOOM_CP_BIND_PORT"]
    assert bind["value"] == "8080", f"got {bind['value']!r} (type {type(bind['value']).__name__})"


def test_macro_includes_all_expected_envs_for_gateway() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    out = _render(
        "{% import '_env.j2' as e %}{{ e.env_block('llm-gateway', schema) }}",
        schema,
    )
    by_name = {entry["name"]: entry for entry in yaml.safe_load(out)["env"]}
    assert "LOOM_GW_DB_URL" in by_name
    assert "LOOM_GW_STEP_JWT_SIGNING_KEY" in by_name
    assert "LOOM_GW_ANTHROPIC_API_KEY" in by_name
    assert "LOOM_GW_OPENAI_API_KEY" in by_name
    assert "LOOM_GW_BIND_PORT" in by_name
    assert "LOOM_GW_WORKER_HEARTBEAT_EXPIRY_SEC" not in by_name
