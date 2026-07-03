"""SubcommandStep tests (#340)."""

from __future__ import annotations

from pathlib import Path

import pytest

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps.subcommand_step import SubcommandStep
from loom_cli.rollout.steps.subprocess_util import SubprocessResult


class _EchoStep(SubcommandStep):
    number = 5
    name = "echo"

    def argv(self, ctx, step_dir):
        return ["echo", "hello", ctx.image_tag]


class TestSubcommandStep:
    def test_run_captures_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        seen: dict = {}

        def fake(argv, **kwargs):
            seen["argv"] = list(argv)
            seen["kwargs"] = kwargs
            if kwargs.get("stdout_log"):
                kwargs["stdout_log"].write_text("hello staging-abc123\n")
            return SubprocessResult(
                argv=list(argv), returncode=0,
                stdout="hello staging-abc123\n", stderr="",
            )

        monkeypatch.setattr(
            "loom_cli.rollout.steps.subcommand_step.run_captured", fake,
        )
        step = _EchoStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(5, "echo")

        result = step.run(ctx, step_dir)
        assert result.exit_code == 0
        assert "exited 0" in result.summary
        assert seen["argv"] == ["echo", "hello", "staging-abc123"]
        assert step_dir.stdout_path().read_text() == "hello staging-abc123\n"

    def test_run_propagates_nonzero_exit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def fake(argv, **kwargs):
            if kwargs.get("stderr_log"):
                kwargs["stderr_log"].write_text("failure: reasons\n")
            return SubprocessResult(
                argv=list(argv), returncode=3,
                stdout="", stderr="failure: reasons\n",
            )

        monkeypatch.setattr(
            "loom_cli.rollout.steps.subcommand_step.run_captured", fake,
        )
        step = _EchoStep()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(5, "echo")

        result = step.run(ctx, step_dir)
        assert result.exit_code == 3
        assert result.error == "failure: reasons"

    def test_argv_must_be_overridden(self, tmp_path: Path) -> None:
        class Bad(SubcommandStep):
            number = 6
            name = "bad"
        step = Bad()
        ctx = make_ctx(tmp_path)
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        step_dir = ev.step_dir(6, "bad")
        with pytest.raises(NotImplementedError, match="argv"):
            step.run(ctx, step_dir)
