from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps import s03_cluster_target
from loom_cli.rollout.steps.s03_cluster_target import ClusterTargetStep


def test_verifies_existing_multinode_k3s_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = make_ctx(tmp_path)
    ctx.cluster_config_path.write_text(
        'namespace = "loom-staging"\n'
        'container_registry = "192.168.50.13:5000"\n'
        'container_registry_push = "localhost:5000"\n',
        encoding="utf-8",
    )
    step_dir = StepDir(3, "cluster-target", tmp_path / "03-cluster-target")
    step_dir.path.mkdir()
    calls: list[tuple[str, ...]] = []

    def run(argv, **_kwargs):  # type: ignore[no-untyped-def]
        command = tuple(argv)
        calls.append(command)
        stdout = "ok\n"
        if command == ("kubectl", "config", "current-context"):
            stdout = "loom-staging\n"
        elif command == ("kubectl", "get", "nodes", "-o", "json"):
            stdout = json.dumps(
                {
                    "items": [
                        {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
                        {"status": {"conditions": [{"type": "Ready", "status": "True"}]}},
                    ]
                }
            )
        elif "jsonpath={.subsets[0].addresses[0].ip}" in command:
            stdout = "10.42.0.10"
        return SimpleNamespace(returncode=0, stdout=stdout, stderr="")

    monkeypatch.setattr(s03_cluster_target, "run_captured", run)

    result = ClusterTargetStep().run(ctx, step_dir)

    assert result.exit_code == 0
    assert result.artifacts["target_type"] == "multinode-k3s"
    assert all("apply" not in command and "label" not in command for command in calls)


def test_protected_target_requires_registry_publication(tmp_path: Path) -> None:
    ctx = make_ctx(tmp_path)
    ctx.cluster_config_path.write_text('namespace = "loom-staging"\n', encoding="utf-8")
    step_dir = StepDir(3, "cluster-target", tmp_path / "03-cluster-target")
    step_dir.path.mkdir()

    result = ClusterTargetStep().run(ctx, step_dir)

    assert result.exit_code == 2
    assert "require container_registry" in (result.error or "")
