"""Step 10 — environment-state apply + check (#340).

Applies the release environment-state profile (from cluster-config's
declared path) and then runs the check to confirm convergence. The
#331 fix to environment-state apply ensures negative desired states
(enabled=false / active=false) actually stop and disable supervisors.
"""

from __future__ import annotations

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.subprocess_util import run_captured


def _profile_path_for(ctx: RolloutContext) -> str | None:
    """Locate the environment-state TOML for the target scope.

    Convention: cluster-config declares ``env_state_profile`` (a path
    resolved relative to cluster-config's own dir). If unset, returns
    None → the step is a no-op.
    """
    from loom_cli.cluster_config import load_cluster_config

    try:
        cfg = load_cluster_config(ctx.cluster_config_path)
    except Exception:
        return None
    profile = getattr(cfg, "env_state_profile", None)
    if not profile:
        return None
    return str(profile)


class EnvStateStep(BaseStep):
    number = 10
    name = "env-state"

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        profile = _profile_path_for(ctx)
        if profile is None:
            step_dir.stdout_path().write_text(
                "no env_state_profile declared in cluster-config; skipping.\n"
            )
            return RunResult(
                exit_code=0,
                summary="no env-state profile; step is a no-op",
            )

        apply_ = run_captured([
            "loom", "admin", "environment-state", "apply",
            "--file", profile,
        ])
        check = run_captured([
            "loom", "admin", "environment-state", "check",
            "--file", profile,
        ])
        step_dir.stdout_path().write_text(
            f"# apply\n{apply_.stdout}\n"
            f"# check\n{check.stdout}\n"
        )
        step_dir.stderr_path().write_text(
            f"# apply\n{apply_.stderr}\n"
            f"# check\n{check.stderr}\n"
        )
        if apply_.returncode != 0:
            return RunResult(
                exit_code=apply_.returncode,
                error=f"env-state apply failed: {apply_.stderr.strip()[:200]}",
            )
        if check.returncode != 0:
            return RunResult(
                exit_code=check.returncode,
                error=f"env-state check reported drift: {check.stdout.strip()[:200]}",
            )
        return RunResult(
            exit_code=0,
            summary="env-state apply + check clean",
        )
