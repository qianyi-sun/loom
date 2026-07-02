from __future__ import annotations

import ast
import hashlib
import json
from collections import Counter
from pathlib import Path

from loom_cli.__main__ import main
from loom_cli.cluster_cmd import render_manifests
from loom_cli.cluster_config import load_cluster_config
from loom_cli.cluster_release_manifest import (
    _alembic_heads,
    build_release_manifest,
    render_release_manifest_json,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _migration_revision(path: Path) -> str:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in tree.body:
        value: ast.expr | None = None
        if isinstance(node, ast.Assign):
            if not any(
                isinstance(target, ast.Name) and target.id == "revision"
                for target in node.targets
            ):
                continue
            value = node.value
        elif isinstance(node, ast.AnnAssign):
            if not isinstance(node.target, ast.Name) or node.target.id != "revision":
                continue
            value = node.value
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            return value.value
    raise AssertionError(f"{path} does not define a string revision")


def test_alembic_migration_revisions_are_unique() -> None:
    migrations = sorted((REPO_ROOT / "migrations" / "versions").glob("*.py"))
    revisions = [_migration_revision(path) for path in migrations]
    duplicates = {
        revision: count
        for revision, count in Counter(revisions).items()
        if count > 1
    }
    assert duplicates == {}


def test_build_release_manifest_records_expected_state_without_raw_secrets(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "cluster-config.toml"
    config_path.write_text(
        'image_tag = "public-beta-abc123"\n'
        'namespace = "loom-public-beta"\n'
        'ingress_host = "public-beta.example.com"\n',
        encoding="utf-8",
    )
    environment_state_path = tmp_path / "public-beta.toml"
    environment_state_path.write_text(
        'environment = "public-beta"\n'
        'secret_example = "super-secret-token"\n'
        "[[worker_pool_autoscaler_policies]]\n"
        'pool_name = "oldlab"\n'
        'actuator = "slurm"\n'
        "[worker_pool_autoscaler_policies.actuator_config]\n"
        'env_file = "/secure/public-beta-oldlab-${IMAGE_TAG}.env"\n'
        'repo_dir = "/srv/loom-${IMAGE_TAG}"\n'
        "external_runner = true\n"
        "[[gb10_worker_pool_desired_states]]\n"
        'pool_name = "gb10-arm64"\n'
        'image_tag = "${IMAGE_TAG}"\n'
        'env_config_version = "${ENV_CONFIG_VERSION}"\n',
        encoding="utf-8",
    )
    config = load_cluster_config(config_path)
    rendered = render_manifests(config)

    manifest = build_release_manifest(
        config=config,
        config_path=config_path,
        rendered_manifests=rendered,
        environment="public-beta",
        image_tag="public-beta-abc123",
        git_sha="a" * 40,
        environment_state_path=environment_state_path,
        env_config_version="public-beta-abc123",
        generated_at="2026-07-01T00:00:00Z",
        loom_cli_version="test-version",
    )
    rendered_json = render_release_manifest_json(manifest)

    assert manifest["schema_version"] == 1
    assert manifest["release"] == {
        "environment": "public-beta",
        "git_sha": "a" * 40,
        "image_tag": "public-beta-abc123",
        "generated_at": "2026-07-01T00:00:00Z",
    }
    assert manifest["tooling"]["loom_cli_version"] == "test-version"
    assert manifest["cluster_config"]["path"] == str(config_path)
    assert (
        manifest["cluster_config"]["sha256"]
        == hashlib.sha256(
            config_path.read_bytes(),
        ).hexdigest()
    )
    assert manifest["rendered_manifest"]["sha256"] == _sha256_text(rendered)
    assert manifest["rendered_manifest"]["deployment_images"]["loom-service"][
        "loom-service"
    ].endswith(":public-beta-abc123")
    manifest_with_identities = build_release_manifest(
        config=config,
        config_path=config_path,
        rendered_manifests=rendered,
        environment="public-beta",
        image_tag="public-beta-abc123",
        git_sha="a" * 40,
        expected_image_identities={
            "loom-service": {
                "loom-service": {
                    "image": "loom-service:public-beta-abc123",
                    "repo_digest": (
                        "loom-service@sha256:"
                        + "1" * 64
                    ),
                    "image_id": "sha256:" + "2" * 64,
                },
            },
        },
        generated_at="2026-07-01T00:00:00Z",
        loom_cli_version="test-version",
    )
    assert manifest_with_identities["rendered_manifest"][
        "deployment_image_identities"
    ] == {
        "loom-service": {
            "loom-service": {
                "image": "loom-service:public-beta-abc123",
                "repo_digest": "loom-service@sha256:" + "1" * 64,
                "image_id": "sha256:" + "2" * 64,
            },
        },
    }
    alembic_heads = _alembic_heads()
    assert manifest["alembic"]["expected_heads"] == alembic_heads
    assert manifest["alembic"]["compatible_heads"] == alembic_heads
    assert manifest["external_workers"]["environment_state_file"]["sha256"] == (
        hashlib.sha256(environment_state_path.read_bytes()).hexdigest()
    )
    assert manifest["external_workers"]["slurm_pools"] == [
        {
            "pool_name": "oldlab",
            "actuator": "slurm",
            "external_runner": True,
            "env_file": "/secure/public-beta-oldlab-public-beta-abc123.env",
            "repo_dir": "/srv/loom-public-beta-abc123",
        },
    ]
    assert manifest["external_workers"]["gb10_desired_states"] == [
        {
            "pool_name": "gb10-arm64",
            "image_tag": "public-beta-abc123",
            "env_config_version": "public-beta-abc123",
        },
    ]
    assert "super-secret-token" not in rendered_json
    assert render_release_manifest_json(manifest) == rendered_json


def test_cluster_release_manifest_cli_writes_manifest(tmp_path: Path) -> None:
    config_path = tmp_path / "cluster-config.toml"
    config_path.write_text(
        'image_tag = "public-beta-def456"\nnamespace = "loom-public-beta"\n',
        encoding="utf-8",
    )
    output_path = tmp_path / "release-manifest-public-beta-def456.json"

    rc = main(
        [
            "cluster",
            "release-manifest",
            "--config",
            str(config_path),
            "--environment",
            "public-beta",
            "--image-tag",
            "public-beta-def456",
            "--git-sha",
            "b" * 40,
            "--generated-at",
            "2026-07-01T01:02:03Z",
            "--output",
            str(output_path),
        ]
    )

    assert rc == 0
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["release"]["git_sha"] == "b" * 40
    assert manifest["release"]["image_tag"] == "public-beta-def456"
    assert manifest["cluster_config"]["path"] == str(config_path)


def test_cluster_release_manifest_cli_accepts_expected_image_identities(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "cluster-config.toml"
    config_path.write_text(
        'image_tag = "public-beta-def456"\nnamespace = "loom-public-beta"\n',
        encoding="utf-8",
    )
    identities_path = tmp_path / "image-identities.json"
    identities_path.write_text(
        json.dumps({
            "loom-service": {
                "loom-service": {
                    "image": "loom-service:public-beta-def456",
                    "repo_digest": "loom-service@sha256:" + "3" * 64,
                },
            },
        }),
        encoding="utf-8",
    )
    output_path = tmp_path / "release-manifest-public-beta-def456.json"

    rc = main(
        [
            "cluster",
            "release-manifest",
            "--config",
            str(config_path),
            "--environment",
            "public-beta",
            "--image-tag",
            "public-beta-def456",
            "--git-sha",
            "b" * 40,
            "--expected-image-identities-json",
            str(identities_path),
            "--output",
            str(output_path),
        ]
    )

    assert rc == 0
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["rendered_manifest"]["deployment_image_identities"][
        "loom-service"
    ]["loom-service"]["repo_digest"] == "loom-service@sha256:" + "3" * 64
