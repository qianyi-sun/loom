"""Step 01 — isolated git worktree at target sha (#340).

Creates a worktree at ``<rollout-dir>/src`` pointing at the resolved
sha. The build step (02) runs docker builds against this worktree so
the operator's main checkout is untouched.
"""

from __future__ import annotations

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import run_captured


def worktree_path(step_dir: StepDir) -> str:
    """Return the string path the git worktree lives at."""
    return str(step_dir.path / "src")


def worktree_branch_name(rollout_id: str) -> str:
    """Deterministic branch name so a stale worktree can be detected."""
    return f"loom-rollout/{rollout_id}"


class WorktreeStep(BaseStep):
    number = 1
    name = "worktree"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "resolved_sha": ctx.resolved_sha,
        }

    def _verify_impl(
        self, ctx: RolloutContext, step_dir: StepDir,
    ) -> VerifyOutcome:
        wt = worktree_path(step_dir)
        # Cheap check: is there a HEAD at the expected sha?
        result = run_captured(["git", "-C", wt, "rev-parse", "HEAD"])
        if result.returncode == 0:
            if result.stdout.strip() == ctx.resolved_sha:
                return VerifyOutcome.MATCH
            return VerifyOutcome.MISMATCH
        return VerifyOutcome.MISMATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        wt = worktree_path(step_dir)
        rid = ctx.metadata.get("rollout_id", "unknown")
        branch = worktree_branch_name(rid)

        # If a stale worktree from a previous attempt exists, remove it
        # first (idempotent recovery).
        remove = run_captured(
            ["git", "worktree", "remove", "--force", wt],
        )
        # Ignore removal failures — if the dir didn't exist, git errors;
        # if it did and remove worked, all good; either way, proceed.

        result = run_captured([
            "git", "worktree", "add", "-B", branch, wt, ctx.resolved_sha,
        ])
        stdout_log = step_dir.stdout_path()
        stderr_log = step_dir.stderr_path()
        # Combine both git invocations' logs for the operator.
        stdout_log.write_text(
            f"# git worktree remove\n{remove.stdout}\n"
            f"# git worktree add -B {branch} {wt} {ctx.resolved_sha}\n"
            f"{result.stdout}\n"
        )
        stderr_log.write_text(
            f"# git worktree remove\n{remove.stderr}\n"
            f"# git worktree add ...\n{result.stderr}\n"
        )
        if result.returncode != 0:
            return RunResult(
                exit_code=result.returncode,
                error=(
                    result.stderr.strip().splitlines()[-1]
                    if result.stderr.strip() else
                    f"git worktree add exited {result.returncode}"
                ),
            )
        return RunResult(
            exit_code=0,
            summary=f"worktree at {wt} on {ctx.resolved_sha[:7]}",
            artifacts={"worktree_path": wt, "branch": branch},
        )
