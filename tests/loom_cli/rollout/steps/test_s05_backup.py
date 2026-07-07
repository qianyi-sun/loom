"""BackupStep argv contract (#340, #363)."""

from __future__ import annotations

import sys
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
            sys.executable,
            "-m",
            "loom_cli",
            "cluster",
            "backup",
            "check",
            "--environment",
            "staging",
            "--namespace",
            "loom-staging",
            "--manifest",
            str(manifest),
            "--min-remaining-hours",
            "2",
        ]

    def test_uses_context_min_remaining_hours(self, tmp_path: Path) -> None:
        manifest = tmp_path / "backup-manifest.json"
        manifest.write_text("{}")
        ctx = make_ctx(
            tmp_path,
            backup_manifest_path=manifest,
            backup_manifest_min_remaining_hours=4,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(5, "backup")

        argv = list(BackupStep().argv(ctx, step_dir))

        assert argv[argv.index("--min-remaining-hours") + 1] == "4"

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
        assert argv[:6] == [
            sys.executable,
            "-m",
            "loom_cli",
            "cluster",
            "backup",
            "check",
        ]
