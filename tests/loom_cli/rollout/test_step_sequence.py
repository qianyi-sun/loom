"""Step-sequence integration tests (#340)."""

from __future__ import annotations

from loom_cli.rollout.base_context_fixture import make_ctx
from loom_cli.rollout.evidence import EvidenceDirectory
from loom_cli.rollout.steps import default_step_sequence
from loom_cli.rollout.steps.base import Step
from loom_cli.rollout.steps.s12_release_gate import ReleaseGateStep


class TestDefaultStepSequence:
    def test_all_steps_have_unique_numbers(self) -> None:
        seq = default_step_sequence()
        numbers = [s.number for s in seq]
        assert len(numbers) == len(set(numbers)), (
            f"duplicate step numbers: {numbers}"
        )

    def test_all_steps_have_unique_names(self) -> None:
        seq = default_step_sequence()
        names = [s.name for s in seq]
        assert len(names) == len(set(names)), (
            f"duplicate step names: {names}"
        )

    def test_numbers_are_monotonic(self) -> None:
        seq = default_step_sequence()
        numbers = [s.number for s in seq]
        assert numbers == sorted(numbers), (
            f"step numbers must be monotonically increasing; got {numbers}"
        )

    def test_all_steps_conform_to_protocol(self) -> None:
        for step in default_step_sequence():
            assert isinstance(step, Step), (
                f"step {step!r} does not conform to the Step protocol"
            )

    def test_summary_is_last(self) -> None:
        seq = default_step_sequence()
        assert seq[-1].name == "summary"
        assert seq[-1].number == 99

    def test_expected_step_names_present(self) -> None:
        """Regression guard: every step referenced by the design brief
        must exist. Prevents accidental deletion."""
        expected = {
            "resolve-target",
            "worktree",
            "build-images",
            "kind-load-images",
            "gb10-prep",
            "backup",
            "audit",
            "render",
            "preflight",
            "migrate",
            "env-state",
            "cluster-up",
            "production-defaults",
            "release-gate",
            "smoke",
            "summary",
        }
        got = {s.name for s in default_step_sequence()}
        assert got == expected, f"unexpected step names: {got ^ expected}"

    def test_release_gate_step_uses_env_state_and_gb10_status_artifacts(
        self,
        tmp_path,
    ) -> None:
        ctx = make_ctx(tmp_path, cp_url="http://control-node.lan:18081")
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        render_dir = ev.step_dir(7, "render")
        render_dir.path.mkdir(parents=True, exist_ok=True)
        render_dir.artifact_path("rendered.yaml").write_text("yaml")
        env_state_dir = ev.step_dir(10, "env-state")
        env_state_dir.path.mkdir(parents=True, exist_ok=True)
        env_state_dir.artifact_path("environment-state-check.json").write_text("{}")
        gb10_dir = ev.step_dir(13, "release-gate")
        gb10_dir.path.mkdir(parents=True, exist_ok=True)

        argv = list(ReleaseGateStep().argv(ctx, gb10_dir))

        assert "--environment-state-check" in argv
        assert str(env_state_dir.artifact_path("environment-state-check.json")) in argv
        assert "--gb10-workers-status" in argv
        assert str(gb10_dir.artifact_path("gb10-workers-status-staging-abc123.json")) in argv

        gb10_argv = list(ReleaseGateStep().gb10_status_argv(ctx, gb10_dir))
        assert "--cp-url" in gb10_argv
        assert gb10_argv[gb10_argv.index("--cp-url") + 1] == "http://control-node.lan:18081"
