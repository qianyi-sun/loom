"""Step 00 — resolve target ref/SHA (#340).

Read-only. Given ``--ref origin/dev`` (or a tag/SHA), resolve to a full
40-char sha and cross-check that the operator-supplied ``--image-tag``
matches. On mismatch the step fails so the operator sees the drift
before any mutation.

Refuses conflicting scope flags via the driver-level preflight; this
step's own preflight is just SHA verification.
"""

from __future__ import annotations

from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult, VerifyOutcome
from loom_cli.rollout.steps.subprocess_util import run_captured


def expected_short_sha(image_tag: str) -> str | None:
    """Extract the ``sha7`` suffix from a staging tag, else None.

    Convention: ``staging-<sha7>``. Non-conforming tags return None
    (the SHA cross-check is skipped rather than failing on custom tags).
    """
    if "-" not in image_tag:
        return None
    parts = image_tag.rsplit("-", 1)
    if len(parts) != 2:
        return None
    candidate = parts[1]
    if len(candidate) < 7 or not all(
        c in "0123456789abcdef" for c in candidate.lower()
    ):
        return None
    return candidate.lower()


class ResolveTargetStep(BaseStep):
    number = 0
    name = "resolve-target"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "target_ref": ctx.target_ref,
            "resolved_sha": ctx.resolved_sha,
            "image_tag": ctx.image_tag,
        }

    def _verify_impl(
        self, ctx: RolloutContext, step_dir: StepDir,
    ) -> VerifyOutcome:
        # If ``resolved.sha`` artifact exists and matches ctx.resolved_sha,
        # the step already succeeded. Cheap file read; no cluster calls.
        artifact = step_dir.artifact_path("resolved.sha")
        if artifact.is_file() and artifact.read_text().strip() == ctx.resolved_sha:
            return VerifyOutcome.MATCH
        return VerifyOutcome.MISMATCH

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        # Cross-check the operator's --image-tag against the resolved sha.
        expected = expected_short_sha(ctx.image_tag)
        if expected is not None and not ctx.resolved_sha.lower().startswith(
            expected,
        ):
            return RunResult(
                exit_code=1,
                error=(
                    f"image-tag {ctx.image_tag!r} implies sha starting "
                    f"with {expected!r}, but --ref resolved to "
                    f"{ctx.resolved_sha!r}. Update --image-tag or "
                    "--ref so they agree."
                ),
            )
        # Persist the resolved SHA as the step's artifact.
        step_dir.artifact_path("resolved.sha").write_text(
            ctx.resolved_sha + "\n",
        )
        return RunResult(
            exit_code=0,
            summary=(
                f"resolved {ctx.target_ref} → {ctx.resolved_sha[:7]} "
                f"(image_tag {ctx.image_tag})"
            ),
            artifacts={"resolved.sha": ctx.resolved_sha},
        )


def resolve_ref_to_sha(
    ref: str, *, cwd: Path | None = None,
) -> str:
    """Utility: shell out to ``git rev-parse ref``.

    Not called during step execution — the driver resolves this once at
    launch and passes the resolved sha into every step via
    :class:`RolloutContext`. Exposed here so the launcher (CLI wire-up)
    can call it.
    """
    result = run_captured(["git", "rev-parse", ref], cwd=cwd)
    if result.returncode != 0:
        raise RuntimeError(
            f"git rev-parse {ref!r} failed: {result.stderr.strip()}"
        )
    sha = result.stdout.strip()
    if len(sha) != 40:
        raise RuntimeError(
            f"git rev-parse {ref!r} returned {sha!r} — expected 40-char sha"
        )
    return sha
