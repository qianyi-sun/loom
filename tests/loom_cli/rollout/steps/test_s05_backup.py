"""BackupStep argv contract (#340, #363)."""

from __future__ import annotations

from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s05_backup import BackupStep


class TestBackupStepArgv:
    def test_invokes_backup_check_subcommand(self, tmp_path: Path) -> None:
        manifest = tmp_path / "backup-manifest.json"
        manifest.write_text("{}")
        ctx = make_ctx(
            tmp_path,
            environment="staging",
            namespace="loom-staging",
            backup_manifest_path=manifest,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(5, "backup")

        argv = list(BackupStep().argv(ctx, step_dir))

        assert argv == [
            "loom", "cluster", "backup", "check",
            "--environment", "staging",
            "--namespace", "loom-staging",
            "--manifest", str(manifest),
        ]

    def test_rejects_obsolete_flags(self, tmp_path: Path) -> None:
        """Regression guard for #363: the pre-fix invocation used
        `loom cluster backup --namespace ... --label ...` which is
        invalid against the current subcommand-based CLI."""
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(5, "backup")

        argv = list(BackupStep().argv(ctx, step_dir))

        assert "--label" not in argv
        assert argv[:4] == ["loom", "cluster", "backup", "check"]
