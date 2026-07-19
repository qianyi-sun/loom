from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from loom_cli.rollout.operator.checkpoint_inventory_provider import (
    KubernetesLifecycleInventoryProvider,
)
from loom_cli.rollout.operator.config import OperatorConfig

NOW = datetime(2026, 7, 19, 23, tzinfo=UTC)


class Runner:
    def __init__(self, document: object) -> None:
        self.document = document
        self.calls: list[tuple[list[str], dict[str, str], float | None]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds=None):
        self.calls.append((list(argv), dict(env), timeout_seconds))
        return json.dumps(self.document).encode()


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
    runner = Runner(
        {
            "mutation_epoch": 11,
            "schema_revision": "0066",
            "objects": [
                {
                    "authoritative_source": "catalog:sha256:" + "d" * 64,
                    "bucket": "loom-staging-artifacts",
                    "content_sha256": "a" * 64,
                    "data_class": "benchmark",
                    "object_key": "benchmarks/b",
                    "size_bytes": 5,
                    "version_id": "v1",
                }
            ],
        }
    )
    provider = KubernetesLifecycleInventoryProvider(
        _config(tmp_path),
        runner=runner,
        environment={"PATH": "/usr/bin"},
    )

    inventory = provider(NOW)

    assert inventory.mutation_epoch == 11
    assert inventory.schema_revision == "0066"
    assert inventory.objects[0].object_key == "benchmarks/b"
    argv, env, timeout = runner.calls[0]
    assert argv[:5] == ["kubectl", "-n", "loom-staging", "exec", "statefulset/loom-postgres"]
    assert "data_lifecycle_objects" in argv[-1]
    assert env == {"PATH": "/usr/bin"}
    assert timeout == 60.0


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"mutation_epoch": None, "schema_revision": "0066", "objects": []},
        {"mutation_epoch": 1, "schema_revision": "0066", "objects": "all"},
        {
            "mutation_epoch": 1,
            "schema_revision": "0066",
            "objects": [
                {
                    "authoritative_source": "",
                    "bucket": "bucket",
                    "content_sha256": "a" * 64,
                    "data_class": "artifact",
                    "object_key": "run/output",
                    "size_bytes": 1,
                    "version_id": "v1",
                }
            ],
        },
    ],
)
def test_provider_fails_closed_on_missing_epoch_or_unclassified_objects(
    tmp_path: Path,
    document: object,
) -> None:
    provider = KubernetesLifecycleInventoryProvider(
        _config(tmp_path),
        runner=Runner(document),
        environment={},
    )

    with pytest.raises(ValueError):
        provider(NOW)
