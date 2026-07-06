"""AuditStep argv contract (#340, #448)."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.s06_audit import AuditStep


class TestAuditStepArgv:
    def test_invokes_supported_audit_subcommand(self, tmp_path: Path) -> None:
        ctx = make_ctx(
            tmp_path,
            namespace="loom-staging",
        )
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(6, "audit")

        argv = list(AuditStep().argv(ctx, step_dir))
        config_path = Path(argv[argv.index("--config") + 1])

        assert argv[:5] == [
            sys.executable,
            "-m",
            "loom_cli",
            "cluster",
            "audit",
        ]
        assert config_path != ctx.cluster_config_path
        assert tomllib.loads(config_path.read_text())["image_tag"] == ctx.image_tag

    def test_does_not_pass_unsupported_namespace_flag(self, tmp_path: Path) -> None:
        ctx = make_ctx(tmp_path, namespace="loom-staging")
        ev = EvidenceDirectory(tmp_path, "test-rid")
        ev.ensure()
        step_dir = ev.step_dir(6, "audit")

        argv = list(AuditStep().argv(ctx, step_dir))

        assert "--namespace" not in argv
