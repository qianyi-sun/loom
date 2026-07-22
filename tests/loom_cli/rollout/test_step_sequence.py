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
        assert len(numbers) == len(set(numbers)), f"duplicate step numbers: {numbers}"

    def test_all_steps_have_unique_names(self) -> None:
        seq = default_step_sequence()
        names = [s.name for s in seq]
        assert len(names) == len(set(names)), f"duplicate step names: {names}"

    def test_numbers_are_monotonic(self) -> None:
        seq = default_step_sequence()
        numbers = [s.number for s in seq]
        assert numbers == sorted(numbers), (
            f"step numbers must be monotonically increasing; got {numbers}"
        )

    def test_all_steps_conform_to_protocol(self) -> None:
        for step in default_step_sequence():
            assert isinstance(step, Step), f"step {step!r} does not conform to the Step protocol"

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
            "kind-cluster",
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
            "staging-admin-browser-acceptance",
            "summary",
        }
        got = {s.name for s in default_step_sequence()}
        assert got == expected, f"unexpected step names: {got ^ expected}"

    def test_missing_kind_cluster_recovery_runs_before_image_load(self) -> None:
        names = [s.name for s in default_step_sequence()]

        assert names.index("kind-cluster") < names.index("kind-load-images")
        assert names.index("kind-load-images") < names.index("migrate")

    def test_cluster_up_runs_after_migration_before_desired_state_apply(self) -> None:
        """GB10 node-agent must not apply a stale release target.

        Missing-kind recovery has no standing Control Plane after migration, so
        cluster-up must recreate platform services before env-state uses the CP
        API. GB10 prep can start the host-local node-agent, so desired state
        must still be updated before prep starts.
        """
        names = [s.name for s in default_step_sequence()]
        assert names.index("migrate") < names.index("cluster-up")
        assert names.index("cluster-up") < names.index("env-state")
        assert names.index("env-state") < names.index("gb10-prep")
        assert names.index("gb10-prep") < names.index("release-gate")

    def test_release_gate_step_uses_env_state_and_gb10_status_artifacts(
        self,
        tmp_path,
    ) -> None:
        ctx = make_ctx(
            tmp_path,
            cp_url="http://control-node.lan:18081",
            admin_token_source="file:/secure/path/staging-admin-token",
            expect_admin_token_fingerprint="sha256:abc123def456 len=64",
        )
        ev = EvidenceDirectory(tmp_path, "rid")
        ev.ensure()
        render_dir = ev.step_dir(7, "render")
        render_dir.path.mkdir(parents=True, exist_ok=True)
        render_dir.artifact_path("rendered.yaml").write_text("yaml")
        gb10_dir = ev.step_dir(14, "release-gate")
        gb10_dir.path.mkdir(parents=True, exist_ok=True)
        release_gate_env_state_check = gb10_dir.artifact_path("environment-state-check.json")
        release_gate_env_state_check.write_text("{}")

        argv = list(ReleaseGateStep().argv(ctx, gb10_dir))

        assert "--environment-state-check" in argv
        assert str(release_gate_env_state_check) in argv
        assert "--gb10-workers-status" in argv
        assert str(gb10_dir.artifact_path("gb10-workers-status-staging-abc123.json")) in argv

        gb10_argv = list(ReleaseGateStep().gb10_status_argv(ctx, gb10_dir))
        assert "--cp-url" in gb10_argv
        assert gb10_argv[gb10_argv.index("--cp-url") + 1] == "http://control-node.lan:18081"
        assert gb10_argv[gb10_argv.index("--admin-token") + 1] == (
            "file:/secure/path/staging-admin-token"
        )
        assert gb10_argv[gb10_argv.index("--expect-admin-token-fingerprint") + 1] == (
            "sha256:abc123def456 len=64"
        )
