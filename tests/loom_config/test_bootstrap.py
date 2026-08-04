"""`loom cluster bootstrap-secrets` output shape."""
from __future__ import annotations

from pathlib import Path

from loom.admin_secret import load_admin_secret_file
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


def test_bootstrap_smoke_defaults_emits_admin_secret(tmp_path: Path) -> None:
    """`cluster up` preflight requires `loom-admin-secret`; under smoke
    defaults, bootstrap-secrets must emit it too so a single
    `eval "$(...)"` unblocks local/dev bring-up."""
    from loom_config.bootstrap import _SMOKE_ADMIN_TOKEN

    out = render_bootstrap_command(
        load_schema(Path("config/loom-schema.toml")),
        namespace="loom-local",
        smoke_defaults=True,
    )
    assert "kubectl create secret generic loom-admin-secret" in out
    assert "--namespace=loom-local" in out
    assert "--from-literal=secrets.toml=" in out
    # The emitted `secrets.toml` is exactly what the pods mount, and its
    # token is a valid singleton admin bearer (loom_admin_ + >=32 chars).
    secrets_toml = tmp_path / "secrets.toml"
    secrets_toml.write_text(f'[admin]\ntoken = "{_SMOKE_ADMIN_TOKEN}"\n')
    verifier = load_admin_secret_file(secrets_toml, require_safe_permissions=False)
    assert verifier.verify(_SMOKE_ADMIN_TOKEN)


def test_bootstrap_without_smoke_defaults_omits_admin_secret() -> None:
    """Real deploys provision `loom-admin-secret` out of band with a real
    token; bootstrap-secrets must not inject a throwaway one there."""
    out = render_bootstrap_command(
        load_schema(Path("config/loom-schema.toml")),
        namespace="loom",
        smoke_defaults=False,
    )
    assert "loom-admin-secret" not in out


def test_bootstrap_omits_optional_family_orchestrator_gateway_secrets() -> None:
    schema = load_schema(Path("config/loom-schema.toml"))
    line = render_bootstrap_command(schema, namespace="loom", smoke_defaults=False)
    assert "--from-literal=family-orchestrator-token=" not in line
