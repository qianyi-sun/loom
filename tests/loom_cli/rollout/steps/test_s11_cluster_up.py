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

        assert argv[:3] == [sys.executable, "-I", "-c"]
        assert "run_module('loom_cli'" in argv[3]
        assert argv[4:9] == [
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

    def test_broker_attempt_passes_only_the_private_request_envelope(
        self,
        tmp_path: Path,
    ) -> None:
        envelope_path = tmp_path / "private" / "envelope.json"
        ctx = make_ctx(
            tmp_path,
            namespace="loom-staging",
            request_envelope_path=envelope_path,
            request_id="request-20260713-hongjian",
            initiating_operator="hongjian",
            initiating_uid=2011,
            attempt_number=2,
            attempt_operator="devansh",
            attempt_uid=2501,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(10, "cluster-up")

        argv = list(ClusterUpStep().argv(ctx, step_dir))

        assert argv[argv.index("--rollout-request-envelope") + 1] == str(envelope_path)
        assert "--rollout-id" not in argv
        assert "--rollout-lock-dir" not in argv
        assert "--rollout-lock-evidence" not in argv
        assert "--force-rollout-lock" not in argv
