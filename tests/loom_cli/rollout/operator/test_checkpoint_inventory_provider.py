from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from loom_cli.rollout.operator.checkpoint_inventory_provider import (
    ReadonlyLifecycleInventoryProvider,
)
from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.rollout_checkpoint import ImmutableObjectReference
from loom_cli.rollout.readonly_database_authority import ReadonlyDatabaseEvidence

NOW = datetime(2026, 7, 19, 23, tzinfo=UTC)


def _config(tmp_path: Path) -> OperatorConfig:
    runner_repo = tmp_path / "runner" / "repo"
    cluster_config = runner_repo / "deploy" / "environments" / "staging.cluster.toml"
    cluster_config.parent.mkdir(parents=True)
    cluster_config.write_text('namespace = "loom-staging"\n', encoding="utf-8")
    return OperatorConfig(
        schema_version=1,
        service_user="loom-rollout",
        operator_group="loom-staging-operators",
        remote_url="https://github.com/qianyi-sun/loom.git",
        target_ref="refs/heads/dev",
        runner_repo=runner_repo,
        state_root=tmp_path / "state",
        runtime_root=tmp_path / "runtime",
        rollout_root=tmp_path / "data",
        kubeconfig_path=tmp_path / "state" / "kubeconfig",
        cluster_config_path=cluster_config,
        admin_token_source=f"file:{tmp_path / 'state' / 'admin'}",
        worker_token_source=f"file:{tmp_path / 'state' / 'worker'}",
        service_token_source=f"file:{tmp_path / 'state' / 'service'}",
        expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        cluster_name="loom-staging",
        namespace="loom-staging",
        environment="staging",
        cp_url="http://127.0.0.1:18081",
        smoke_on_behalf_username="devansh",
        smoke_on_behalf_team_id="11111111-1111-4111-8111-111111111111",
        scope="current-gb10",
        gb10_prep_concurrency=8,
        config_path=tmp_path / "operator.toml",
        config_sha256="a" * 64,
    )


def test_provider_builds_sorted_exact_inventory_without_object_payloads(
    tmp_path: Path,
) -> None:
    evidence = ReadonlyDatabaseEvidence(
        schema_revision="0069",
        mutation_epoch=11,
        epoch_authority="staging-mutation-epoch-v1",
        baseline_counts={"agents": 1, "provider_models": 1, "tasks": 1, "teams": 1, "users": 1},
        capacity=None,
        evidence_sha256="e" * 64,
        immutable_objects=(
            ImmutableObjectReference(
                authoritative_source="catalog:sha256:" + "d" * 64,
                bucket="loom-staging-artifacts",
                content_sha256="a" * 64,
                data_class="benchmark",
                object_key="benchmarks/b",
                size_bytes=5,
                version_id="v1",
            ),
        ),
    )
    verified: list[tuple[ImmutableObjectReference, ...]] = []
    provider = ReadonlyLifecycleInventoryProvider(
        _config(tmp_path),
        evidence_source=lambda: evidence,
        object_verifier=lambda objects: verified.append(tuple(objects)) or objects,
    )

    inventory = provider(NOW)

    assert inventory.mutation_epoch == 11
    assert inventory.schema_revision == "0069"
    assert inventory.objects[0].object_key == "benchmarks/b"
    assert inventory.objects == evidence.immutable_objects
    assert verified == [evidence.immutable_objects]


def test_provider_binds_legacy_empty_inventory_to_shared_snapshot(tmp_path: Path) -> None:
    evidence = ReadonlyDatabaseEvidence(
        schema_revision="0065",
        mutation_epoch=0,
        epoch_authority="legacy-pre-0069",
        baseline_counts={"agents": 1, "provider_models": 1, "tasks": 1, "teams": 1, "users": 1},
        capacity=None,
        evidence_sha256="e" * 64,
    )
    provider = ReadonlyLifecycleInventoryProvider(
        _config(tmp_path),
        evidence_source=lambda: evidence,
        object_verifier=lambda objects: objects,
    )

    inventory = provider(NOW)

    assert inventory.mutation_epoch == 0
    assert inventory.schema_revision == "0065"
    assert inventory.objects == ()
