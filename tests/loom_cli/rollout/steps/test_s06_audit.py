"""AuditStep argv contract (#340, #448)."""

from __future__ import annotations

from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s06_audit import AuditStep


class TestAuditStepArgv:
    def test_invokes_supported_audit_subcommand(self, tmp_path: Path) -> None:
        ctx = make_ctx(
            tmp_path,
            namespace="loom-public-beta",
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(6, "audit")

        argv = list(AuditStep().argv(ctx, step_dir))

        assert argv == [
            "loom", "cluster", "audit",
            "--config", str(ctx.cluster_config_path),
        ]

    def test_does_not_pass_unsupported_namespace_flag(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path, namespace="loom-public-beta")
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(6, "audit")

        argv = list(AuditStep().argv(ctx, step_dir))

        assert "--namespace" not in argv
