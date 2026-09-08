"""Nebius identities use the existing CNPG role reconciliation boundary."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config

_PROFILE = (
    Path(__file__).resolve().parents[2] / "deploy/environments/staging.multinode.cluster.toml"
)


def _render(tmp_path: Path, *, enabled: bool) -> list[dict]:
    config = tmp_path / "cluster.toml"
    values = {
        "enabled": enabled,
        "source_secret_name": "loom-nebius-staging-spool",
        "runtime_profile_secret_name": "loom-nebius-staging-runtime",
        "image_admission_secret_name": "loom-nebius-staging-admission",
        "configuration_revision": "a" * 64,
        "source_egress_allowlist": ["192.0.2.15:443"],
        "execution_ingress_cidrs": ["192.0.2.16/32"],
    }
    config.write_text(
        _PROFILE.read_text()
        + "\n[nebius_execution]\n"
        + "\n".join(f"{key} = {json.dumps(value)}" for key, value in values.items())
    )
    return [doc for doc in yaml.safe_load_all(render_manifests(load_cluster_config(config))) if doc]


def _postgres(docs: list[dict]) -> dict:
    return next(
        doc
        for doc in docs
        if doc["kind"] == "Cluster" and doc["metadata"]["name"] == "loom-postgres"
    )


def test_nebius_roles_inherit_existing_canonical_app_role(tmp_path: Path) -> None:
    spec = _postgres(_render(tmp_path, enabled=True))["spec"]
    assert spec["managed"]["roles"] == [
        {
            "name": f"loom_nebius_staging_{component}",
            "ensure": "present",
            "login": True,
            "inherit": True,
            "superuser": False,
            "createdb": False,
            "createrole": False,
            "replication": False,
            "bypassrls": False,
            "inRoles": ["loom"],
            "passwordSecret": {"name": f"loom-nebius-staging-db-{component}"},
        }
        for component in ("gateway", "actuator")
    ]
    assert spec["bootstrap"]["initdb"]["database"] == "loom"
    assert spec["bootstrap"]["initdb"]["owner"] == "loom"


def test_nebius_roles_have_no_calendar_expiry_and_no_inline_passwords(tmp_path: Path) -> None:
    docs = _render(tmp_path, enabled=True)
    roles = _postgres(docs)["spec"]["managed"]["roles"]
    for role in roles:
        assert "validUntil" not in role
        assert "password" not in role
        assert "disablePassword" not in role
    assert not any(
        doc["kind"] == "Secret" and doc["metadata"]["name"].startswith("loom-nebius-staging-db-")
        for doc in docs
    )


def test_disabled_nebius_does_not_change_canonical_database_roles(tmp_path: Path) -> None:
    spec = _postgres(_render(tmp_path, enabled=False))["spec"]
    assert "managed" not in spec
    assert "loom-nebius-staging-db-" not in str(spec)
