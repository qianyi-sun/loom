"""BackupStep argv contract (#340, #363)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.candidate_source import candidate_loom_argv
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

        assert argv == candidate_loom_argv(
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
        )

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

    def test_passes_broker_bound_traversal_limits(self, tmp_path: Path) -> None:
        ctx = make_ctx(
            tmp_path,
            request_id="req-123",
            backup_manifest_max_files=1_000_004,
            backup_manifest_max_entries=16_000_000,
            backup_manifest_max_total_bytes=16 * 1024**4,
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(5, "backup")

        argv = list(BackupStep().argv(ctx, step_dir))

        assert argv[argv.index("--backup-max-files") + 1] == "1000004"
        assert argv[argv.index("--backup-max-entries") + 1] == "16000000"
        assert argv[argv.index("--backup-max-total-bytes") + 1] == str(16 * 1024**4)

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
        expected_command = candidate_loom_argv("cluster", "backup", "check")
        assert argv[: len(expected_command)] == expected_command

    def test_rejects_partial_traversal_limit_context(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="must be provided together"):
            make_ctx(tmp_path, backup_manifest_max_files=1_000_004)
