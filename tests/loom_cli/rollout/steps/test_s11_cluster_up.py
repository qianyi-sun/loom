"""ClusterUpStep argv contract (#340, #450)."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s11_cluster_up import ClusterUpStep


class TestClusterUpStepArgv:
    def test_invokes_supported_cluster_up_subcommand(self, tmp_path: Path) -> None:
        ctx = make_ctx(
            tmp_path,
            namespace="loom-staging",
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(10, "cluster-up")

        argv = list(ClusterUpStep().argv(ctx, step_dir))
        config_path = Path(argv[argv.index("--config") + 1])

        assert argv[:8] == [
            sys.executable,
            "-m",
            "loom_cli",
            "cluster",
            "up",
            "--namespace",
            "loom-staging",
            "--config",
        ]
        assert config_path != ctx.cluster_config_path
        assert tomllib.loads(config_path.read_text())["image_tag"] == ctx.image_tag

    def test_does_not_pass_unsupported_wait_flag(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(10, "cluster-up")

        argv = list(ClusterUpStep().argv(ctx, step_dir))

        assert "--wait" not in argv

    def test_enables_bounded_sandbox_deadline_recovery(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path, namespace="loom-staging")
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(10, "cluster-up")

        argv = list(ClusterUpStep().argv(ctx, step_dir))

        assert "--recover-sandbox-deadlines" in argv
        assert argv[argv.index("--sandbox-deadline-max-pods") + 1] == "4"

    def test_passes_backup_manifest_to_internal_preflight(self, tmp_path: Path) -> None:
        backup_manifest = tmp_path / "staging-backup-manifest.json"
        backup_manifest.write_text("{}", encoding="utf-8")
        ctx = make_ctx(
            tmp_path,
            namespace="loom-staging",
            backup_manifest_path=backup_manifest,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(10, "cluster-up")

        argv = list(ClusterUpStep().argv(ctx, step_dir))

        assert argv[argv.index("--backup-manifest") + 1] == str(backup_manifest)
