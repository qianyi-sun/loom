from __future__ import annotations

import json
from pathlib import Path

import pytest

from loom_cli.rollout.operator.config import OperatorConfig
from loom_cli.rollout.operator.mutation_epoch_provider import (
    KubernetesMutationEpochProvider,
)


class Runner:
    def __init__(self, document: object) -> None:
        self.document = document
        self.calls: list[tuple[list[str], dict[str, str], float]] = []

    def capture_stdout(self, argv, *, env, timeout_seconds):
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


def test_reads_exact_staging_epoch_through_fixed_readonly_query(tmp_path: Path) -> None:
    runner = Runner({"environment": "staging", "namespace": "loom-staging", "epoch": 17})
    provider = KubernetesMutationEpochProvider(
        _config(tmp_path), runner=runner, environment={"PATH": "/usr/bin"}
    )

    assert provider() == 17
    argv, environment, timeout = runner.calls[0]
    assert argv[:5] == ["kubectl", "-n", "loom-staging", "exec", "service/loom-postgres-rw"]
    assert "staging_mutation_epochs" in argv[-1]
    assert "INSERT" not in argv[-1] and "UPDATE" not in argv[-1] and "DELETE" not in argv[-1]
    assert environment == {"PATH": "/usr/bin"}
    assert timeout == 30.0


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"environment": "prod", "namespace": "loom-staging", "epoch": 1},
        {"environment": "staging", "namespace": "other", "epoch": 1},
        {"environment": "staging", "namespace": "loom-staging", "epoch": None},
        {"environment": "staging", "namespace": "loom-staging", "epoch": -1},
        {
            "environment": "staging",
            "namespace": "loom-staging",
            "epoch": 1,
            "extra": "drift",
        },
    ],
)
def test_fails_closed_on_missing_cross_environment_or_drifted_authority(
    tmp_path: Path, document: object
) -> None:
    provider = KubernetesMutationEpochProvider(
        _config(tmp_path), runner=Runner(document), environment={}
    )

    with pytest.raises(ValueError, match="authority"):
        provider()
