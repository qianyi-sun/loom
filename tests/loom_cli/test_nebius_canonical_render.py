"""Opt-in canonical staging attachment stays in protected cluster rendering."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "deploy/environments/staging.multinode.cluster.toml"


def _render(tmp_path: Path, extra: str = "") -> list[dict]:
    config = tmp_path / "cluster.toml"
    config.write_text(_PROFILE.read_text() + "\n" + extra)
    return [doc for doc in yaml.safe_load_all(render_manifests(load_cluster_config(config))) if doc]


def _attachment(**overrides: object) -> str:
    values = {
        "enabled": True,
        "source_secret_name": "loom-nebius-staging-spool",
        "runtime_profile_secret_name": "loom-nebius-staging-runtime",
        "image_admission_secret_name": "loom-nebius-staging-admission",
        "configuration_revision": "a" * 64,
        "source_egress_allowlist": ["192.0.2.15:9000"],
        "execution_ingress_cidrs": ["192.0.2.16/32"],
    }
    values.update(overrides)
    import json

    return "[nebius_execution]\n" + "\n".join(
        f"{key} = {json.dumps(value)}" for key, value in values.items()
    )


def _deployment(docs: list[dict], name: str) -> dict:
    return next(
        doc for doc in docs if doc["kind"] == "Deployment" and doc["metadata"]["name"] == name
    )


def _env(deployment: dict) -> dict:
    rows = deployment["spec"]["template"]["spec"]["containers"][0]["env"]
    assert len({row["name"] for row in rows}) == len(rows)
    return {row["name"]: row for row in rows}


def test_staging_attachment_renders_persistent_spool_and_runtime_secrets(tmp_path: Path) -> None:
    docs = _render(tmp_path, _attachment())
    cp = _deployment(docs, "loom-control-plane")
    env = _env(cp)
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED"]["value"] == "true"
    assert env["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENVIRONMENT"]["value"] == "staging"
    assert env["LOOM_CP_SERVICE_EXECUTION_MATERIALIZER_ENABLED"]["value"] == "true"
    for field, key in {
        "ENDPOINT": "endpoint",
        "REGION": "region",
        "BUCKET": "bucket",
        "ACCESS_KEY": "access-key",
        "SECRET_KEY": "secret-key",
    }.items():
        assert env[f"LOOM_CP_SERVICE_EXECUTION_SOURCE_{field}"]["valueFrom"]["secretKeyRef"] == {
            "name": "loom-nebius-staging-spool",
            "key": key,
        }
    assert env["LOOM_CP_MINIO_ENDPOINT"]["value"] == "http://loom-minio:9000"
    assert env["LOOM_CP_ARTIFACTS_BUCKET"]["value"] == "loom-staging-artifacts"
    svc = _env(_deployment(docs, "loom-service"))
    assert (
        svc["LOOM_SVC_SERVICE_EXECUTION_RUNTIME_PROFILE_JSON"]["valueFrom"]["secretKeyRef"]["name"]
        == "loom-nebius-staging-runtime"
    )
    assert (
        cp["spec"]["template"]["metadata"]["annotations"]["loom.ca/nebius-configuration-revision"]
        == "a" * 64
    )
    names = {doc["metadata"]["name"] for doc in docs}
    assert "loom-execution-actuator" not in names
    assert "loom-execution-runtime" not in names
    policy = next(doc for doc in docs if doc["metadata"]["name"] == "loom-nebius-canonical-access")
    assert policy["spec"]["egress"] == [
        {
            "to": [{"ipBlock": {"cidr": "192.0.2.15/32"}}],
            "ports": [{"port": 9000, "protocol": "TCP"}],
        }
    ]


def test_default_staging_does_not_enable_nebius_or_require_attachment_secrets(
    tmp_path: Path,
) -> None:
    docs = _render(tmp_path)
    cp = _env(_deployment(docs, "loom-control-plane"))
    assert cp["LOOM_CP_SERVICE_EXECUTION_SCHEDULER_ENABLED"]["value"] == "False"
    assert not any(doc["metadata"]["name"] == "loom-nebius-canonical-access" for doc in docs)
    assert "loom-nebius-staging-spool" not in str(docs)


@pytest.mark.parametrize(
    "overrides",
    [
        {"source_secret_name": ""},
        {"source_egress_allowlist": []},
        {"execution_ingress_cidrs": []},
        {"execution_ingress_cidrs": ["0.0.0.0/0"]},
        {"source_egress_allowlist": ["127.0.0.1:9000"]},
        {"configuration_revision": ""},
    ],
)
def test_incomplete_attachment_fails_before_render(tmp_path: Path, overrides: dict) -> None:
    with pytest.raises(ValueError, match="nebius_execution"):
        _render(tmp_path, _attachment(**overrides))


@pytest.mark.parametrize(
    ("original", "replacement"),
    [
        ('runtime_environment = "staging"', 'runtime_environment = "development"'),
        (
            'artifacts_bucket = "loom-staging-artifacts"',
            'artifacts_bucket = "loom-development-artifacts"',
        ),
        (
            'trajectories_bucket = "loom-staging-trajectories"',
            'trajectories_bucket = "loom-development-trajectories"',
        ),
    ],
)
def test_attachment_rejects_wrong_canonical_identity(
    tmp_path: Path, original: str, replacement: str
) -> None:
    content = _PROFILE.read_text()
    assert original in content
    config = tmp_path / "cluster.toml"
    config.write_text(content.replace(original, replacement) + "\n" + _attachment())
    with pytest.raises(ValueError, match="canonical staging"):
        render_manifests(load_cluster_config(config))


def test_public_cluster_render_cli_attachment_and_incomplete_binding(
    tmp_path: Path, capsys
) -> None:
    config = tmp_path / "cluster.toml"
    config.write_text(_PROFILE.read_text() + "\n" + _attachment())
    assert main(["cluster", "render", "--config", str(config)]) == 0
    output = capsys.readouterr()
    docs = [row for row in yaml.safe_load_all(output.out) if row]
    assert (
        _env(_deployment(docs, "loom-control-plane"))["LOOM_CP_SERVICE_EXECUTION_SOURCE_ENDPOINT"][
            "valueFrom"
        ]["secretKeyRef"]["name"]
        == "loom-nebius-staging-spool"
    )
    config.write_text(_PROFILE.read_text() + "\n" + _attachment(source_secret_name=""))
    assert main(["cluster", "render", "--config", str(config)]) != 0
    output = capsys.readouterr()
    assert "nebius_execution" in output.err
    assert "apiVersion:" not in output.out
