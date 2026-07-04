"""ClusterUpStep argv contract (#340, #450)."""

from __future__ import annotations

from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s11_cluster_up import ClusterUpStep


class TestClusterUpStepArgv:
    def test_invokes_supported_cluster_up_subcommand(self, tmp_path: Path) -> None:
        ctx = make_ctx(
            tmp_path,
            namespace="loom-public-beta",
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(11, "cluster-up")

        argv = list(ClusterUpStep().argv(ctx, step_dir))

        assert argv == [
            "loom", "cluster", "up",
            "--namespace", "loom-public-beta",
            "--config", str(ctx.cluster_config_path),
        ]

    def test_does_not_pass_unsupported_wait_flag(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(11, "cluster-up")

        argv = list(ClusterUpStep().argv(ctx, step_dir))

        assert "--wait" not in argv
