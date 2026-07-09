"""`loom cluster bootstrap-secrets` output shape."""
from __future__ import annotations

from pathlib import Path

from loom_config.bootstrap import render_bootstrap_command
from loom_config.loader import load_schema


def test_bootstrap_covers_every_required_secret_key() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    line = render_bootstrap_command(schema, namespace="loom", smoke_defaults=True)
    assert line.startswith("kubectl create secret generic loom-secrets")
    assert "--namespace=loom" in line
    for name in schema.service_config:
        e = schema.service_config[name]
        if e.secret is None or not e.required:
            continue
        for svc in e.used_by:
            assert f"--from-literal={e.secret_key_for(svc)}=" in line


def test_bootstrap_smoke_defaults_substitutes_placeholders() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    line = render_bootstrap_command(schema, namespace="loom", smoke_defaults=True)
    assert "--from-literal=step-jwt-signing-key=smoke-jwt-key" in line
    assert "--from-literal=cp-db-url=" in line


def test_bootstrap_without_smoke_defaults_uses_placeholders() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    line = render_bootstrap_command(schema, namespace="loom", smoke_defaults=False)
    assert "<EDIT_ME>" in line


def test_bootstrap_includes_infra_secrets() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    line = render_bootstrap_command(schema, namespace="loom", smoke_defaults=True)
    assert "--from-literal=postgres-user=loom" in line
    assert "--from-literal=postgres-password=loom" in line
    assert "--from-literal=secret-store-master-key=" in line


def test_bootstrap_omits_optional_family_orchestrator_gateway_secrets() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    line = render_bootstrap_command(schema, namespace="loom", smoke_defaults=False)
    assert "--from-literal=family-orchestrator-team-id=" not in line
    assert "--from-literal=family-orchestrator-token=" not in line
