"""Step-sequence integration tests (#340)."""

from __future__ import annotations

from loom_cli.rollout.steps import default_step_sequence
from loom_cli.rollout.steps.base import Step


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
            "release-gate",
            "smoke",
            "summary",
        }
        got = {s.name for s in default_step_sequence()}
        assert got == expected, f"unexpected step names: {got ^ expected}"
