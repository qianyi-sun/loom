from __future__ import annotations

import ast
import hashlib
import json
from collections import Counter
from pathlib import Path

import pytest

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
                isinstance(target, ast.Name) and target.id == "revision" for target in node.targets
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
    duplicates = {revision: count for revision, count in Counter(revisions).items() if count > 1}
    assert duplicates == {}


def test_build_release_manifest_records_expected_state_without_raw_secrets(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "cluster-config.toml"
    config_path.write_text(
        'image_tag = "staging-abc123"\n'
        'namespace = "loom-staging"\n'
        'ingress_host = "staging.example.com"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    environment_state_path = tmp_path / "staging.toml"
    environment_state_path.write_text(
        'environment = "staging"\n'
        'control_plane_environment = "staging"\n'
        'secret_example = "super-secret-token"\n'
        "[[worker_pool_autoscaler_policies]]\n"
        'pool_name = "oldlab"\n'
        'actuator = "slurm"\n'
        "[worker_pool_autoscaler_policies.actuator_config]\n"
        'env_file = "/secure/staging-oldlab-${IMAGE_TAG}.env"\n'
        'repo_dir = "/srv/loom-${IMAGE_TAG}"\n'
        "external_runner = true\n"
        "[[gb10_worker_pool_desired_states]]\n"
        'pool_name = "gb10"\n'
        'image_tag = "${IMAGE_TAG}"\n'
        "max_concurrent = 10\n"
        'env_config_version = "${ENV_CONFIG_VERSION}"\n'
        'source_git_commit = "${GIT_SHA}"\n'
        "target_slots = 10\n"
        "[gb10_worker_pool_desired_states.host_intents]\n"
        'trt-gb10-1 = "active"\n'
        'trt-gb10-2 = "stopped"\n'
        "[catalog_provisioning]\n"
        "required = true\n"
        'command = "loom datasets register skilllearnbench --hf-org PRHW '
        '--revision \\"$PUBLISHED_SHA\\" --mirror-to-object-store '
        "--bucket loom-benchmarks && "
        'loom datasets audit --all --verify-bundles"\n'
        'env_file = "/secure/staging-catalog.env"\n'
        'required_env = ["PUBLISHED_SHA", "HF_TOKEN", "LOOM_SVC_DB_URL", '
        '"LOOM_SVC_MINIO_ENDPOINT", "LOOM_SVC_MINIO_ACCESS_KEY", '
        '"LOOM_SVC_MINIO_SECRET_KEY"]\n'
        "[catalog_provisioning.env]\n"
        'PUBLISHED_SHA = "79087002d62bb22169a704bc941c8d614082d880"\n',
        encoding="utf-8",
    )
    config = load_cluster_config(config_path)
    rendered = render_manifests(config)

    manifest = build_release_manifest(
        config=config,
        config_path=config_path,
        rendered_manifests=rendered,
        environment="staging",
        image_tag="staging-abc123",
        git_sha="a" * 40,
        environment_state_path=environment_state_path,
        env_config_version="staging-abc123",
        generated_at="2026-07-01T00:00:00Z",
        loom_cli_version="test-version",
    )
    rendered_json = render_release_manifest_json(manifest)

    assert manifest["schema_version"] == 1
    assert manifest["release"] == {
        "environment": "staging",
        "git_sha": "a" * 40,
        "image_tag": "staging-abc123",
        "generated_at": "2026-07-01T00:00:00Z",
    }
    assert manifest["tooling"]["loom_cli_version"] == "test-version"
    assert manifest["workload_contract"] == {
        "workload_trust_mode": "internal_trusted",
        "taskset_transforms_enabled": False,
        "taskset_transform_network_isolated": False,
        "untrusted_workload_isolation": False,
    }
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
    ].endswith(":staging-abc123")
    manifest_with_identities = build_release_manifest(
        config=config,
        config_path=config_path,
        rendered_manifests=rendered,
        environment="staging",
        image_tag="staging-abc123",
        git_sha="a" * 40,
        expected_image_identities={
            "loom-service": {
                "loom-service": {
                    "image": "loom-service:staging-abc123",
                    "repo_digest": ("loom-service@sha256:" + "1" * 64),
                    "image_id": "sha256:" + "2" * 64,
                },
            },
        },
        generated_at="2026-07-01T00:00:00Z",
        loom_cli_version="test-version",
    )
    assert manifest_with_identities["rendered_manifest"]["deployment_image_identities"] == {
        "loom-service": {
            "loom-service": {
                "image": "loom-service:staging-abc123",
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
    assert manifest["external_workers"]["control_plane_environment"] == "staging"
    assert manifest["external_workers"]["slurm_pools"] == [
        {
            "pool_name": "oldlab",
            "actuator": "slurm",
            "enabled": False,
            "disabled_reason": None,
            "external_runner": True,
            "env_file": "/secure/staging-oldlab-staging-abc123.env",
            "repo_dir": "/srv/loom-staging-abc123",
        },
    ]
    assert manifest["external_workers"]["external_slurm_runner_prerequisites"] == {}
    assert manifest["external_workers"]["external_slurm_autoscaler_supervisors"] == []
    assert manifest["external_workers"]["gb10_desired_states"] == [
        {
            "environment": "staging",
            "pool_name": "gb10",
            "image_tag": "staging-abc123",
            "max_concurrent": 10,
            "env_config_version": "staging-abc123",
            "source_git_commit": "a" * 40,
            "target_slots": 10,
            "host_intents": {
                "trt-gb10-1": "active",
                "trt-gb10-2": "stopped",
            },
        },
    ]
    assert manifest["catalog_provisioning"] == {
        "required": True,
        "command": (
            "loom datasets register skilllearnbench --hf-org PRHW "
            '--revision "$PUBLISHED_SHA" --mirror-to-object-store '
            "--bucket loom-benchmarks && "
            "loom datasets audit --all --verify-bundles"
        ),
        "env_file": "/secure/staging-catalog.env",
        "env": {
            "PUBLISHED_SHA": "79087002d62bb22169a704bc941c8d614082d880",
        },
        "required_env": [
            "PUBLISHED_SHA",
            "HF_TOKEN",
            "LOOM_SVC_DB_URL",
            "LOOM_SVC_MINIO_ENDPOINT",
            "LOOM_SVC_MINIO_ACCESS_KEY",
            "LOOM_SVC_MINIO_SECRET_KEY",
        ],
    }
    assert "super-secret-token" not in rendered_json
    assert render_release_manifest_json(manifest) == rendered_json


def test_cluster_release_manifest_cli_writes_manifest(tmp_path: Path) -> None:
    config_path = tmp_path / "cluster-config.toml"
    config_path.write_text(
        'image_tag = "staging-def456"\nnamespace = "loom-staging"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    output_path = tmp_path / "release-manifest-staging-def456.json"

    rc = main(
        [
            "cluster",
            "release-manifest",
            "--config",
            str(config_path),
            "--environment",
            "staging",
            "--image-tag",
            "staging-def456",
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
    assert manifest["release"]["image_tag"] == "staging-def456"
    assert manifest["cluster_config"]["path"] == str(config_path)


def test_cluster_release_manifest_cli_accepts_expected_image_identities(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "cluster-config.toml"
    config_path.write_text(
        'image_tag = "staging-def456"\nnamespace = "loom-staging"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    identities_path = tmp_path / "image-identities.json"
    identities_path.write_text(
        json.dumps(
            {
                "loom-service": {
                    "loom-service": {
                        "image": "loom-service:staging-def456",
                        "repo_digest": "loom-service@sha256:" + "3" * 64,
                    },
                },
            }
        ),
        encoding="utf-8",
    )
    output_path = tmp_path / "release-manifest-staging-def456.json"

    rc = main(
        [
            "cluster",
            "release-manifest",
            "--config",
            str(config_path),
            "--environment",
            "staging",
            "--image-tag",
            "staging-def456",
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
    assert (
        manifest["rendered_manifest"]["deployment_image_identities"]["loom-service"][
            "loom-service"
        ]["repo_digest"]
        == "loom-service@sha256:" + "3" * 64
    )


def test_build_protected_release_manifest_rejects_non_v1_workload_contract(
    tmp_path: Path,
) -> None:
    raw_mode = "hf_abcdefghijklmnopqrstuvwxyz1234567890"
    config_path = tmp_path / "staging.cluster.toml"
    config_path.write_text(
        'namespace = "loom-staging"\n'
        "[workload_contract]\n"
        f'workload_trust_mode = "{raw_mode}"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    config = load_cluster_config(config_path)

    with pytest.raises(ValueError) as exc_info:
        build_release_manifest(
            config=config,
            config_path=config_path,
            rendered_manifests=render_manifests(config),
            environment="staging",
            image_tag="staging-abc123",
            git_sha="a" * 40,
            generated_at="2026-07-01T00:00:00Z",
            loom_cli_version="test-version",
        )

    assert raw_mode not in str(exc_info.value)


def test_build_release_manifest_rejects_protected_namespace_environment_downgrade(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "staging.cluster.toml"
    config_path.write_text(
        'namespace = "loom-staging"\n'
        "[workload_contract]\n"
        'workload_trust_mode = "internal_trusted"\n'
        "taskset_transforms_enabled = false\n"
        "taskset_transform_network_isolated = false\n"
        "untrusted_workload_isolation = false\n",
        encoding="utf-8",
    )
    config = load_cluster_config(config_path)

    with pytest.raises(ValueError) as exc_info:
        build_release_manifest(
            config=config,
            config_path=config_path,
            rendered_manifests=render_manifests(config),
            environment="development",
            image_tag="staging-abc123",
            git_sha="a" * 40,
            generated_at="2026-07-01T00:00:00Z",
            loom_cli_version="test-version",
        )

    assert "development" not in str(exc_info.value)
