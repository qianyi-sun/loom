"""Step 10 — environment-state apply + desired-state check (#340, #593).

Applies the release environment-state profile (from cluster-config's
declared path) and records an immediate check. Pure GB10 node-status drift is
deferred because GB10 prep now starts after desired state is written; final
node convergence is checked again by release-gate. The #331 fix to
environment-state apply ensures negative desired states (enabled=false /
active=false) actually stop and disable supervisors.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from pathlib import Path

from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    candidate_relative_path,
)
from loom_cli.rollout.steps.subprocess_util import run_captured


def _is_gb10_node_status_drift_only(stdout: str) -> bool:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
        return False
    autoscaler_blockers = payload.get("autoscaler_blockers", [])
    if not isinstance(autoscaler_blockers, list) or autoscaler_blockers:
        return False
    drift = payload.get("drift")
    if not isinstance(drift, list) or not drift:
        return False
    for item in drift:
        if not isinstance(item, dict):
            return False
        path = item.get("path")
        if not isinstance(path, str):
            return False
        if not path.startswith("gb10_worker_node_status["):
            return False
    return True


def environment_state_check_argv(
    ctx: RolloutContext,
    step_dir: StepDir,
) -> Sequence[str] | None:
    profile = _profile_path_for(ctx)
    if profile is None:
        return None
    profile_path = candidate_relative_path(Path(profile), step_dir)
    release_vars = [
        "--var", f"IMAGE_TAG={ctx.image_tag}",
        "--var", f"ENV_CONFIG_VERSION={ctx.image_tag}",
        "--var", f"GIT_SHA={ctx.resolved_sha}",
    ]
    admin_args = [
        "--admin-token",
        ctx.admin_token_source,
    ]
    if ctx.expect_admin_token_fingerprint:
        admin_args.extend([
            "--expect-admin-token-fingerprint",
            ctx.expect_admin_token_fingerprint,
        ])
    worker_check_args: list[str] = []
    if ctx.worker_token_source:
        worker_check_args.extend([
            "--worker-token",
            ctx.worker_token_source,
        ])
    return candidate_loom_argv(
        "admin", "environment-state", "check",
        "--cp-url", ctx.cp_url,
        *admin_args,
        "--file", str(profile_path),
        "--environment", ctx.environment,
        *release_vars,
        *worker_check_args,
        "--format", "json",
    )


def _profile_path_for(ctx: RolloutContext, config_path: Path | None = None) -> str | None:
    """Locate the environment-state TOML for the target scope.

    Convention: cluster-config declares ``env_state_profile`` (a path
    resolved relative to cluster-config's own dir). If unset, returns
    None → the step is a no-op.
    """
    from loom_cli.cluster_config import load_cluster_config

    source_config_path = config_path or ctx.cluster_config_path
    try:
        cfg = load_cluster_config(source_config_path)
    except Exception:
        return None
    profile = getattr(cfg, "env_state_profile", None)
    if not profile:
        return None
    profile_path = Path(str(profile)).expanduser()
    if not profile_path.is_absolute():
        profile_path = source_config_path.parent / profile_path
    return str(profile_path.resolve(strict=False))


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

        try:
            cwd = candidate_loom_cwd(step_dir)
            env = candidate_loom_env(step_dir)
        except CandidateToolingError as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))

        profile_path = candidate_relative_path(Path(profile), step_dir)
        release_vars = [
            "--var", f"IMAGE_TAG={ctx.image_tag}",
            "--var", f"ENV_CONFIG_VERSION={ctx.image_tag}",
            "--var", f"GIT_SHA={ctx.resolved_sha}",
        ]
        admin_args = [
            "--admin-token",
            ctx.admin_token_source,
        ]
        if ctx.expect_admin_token_fingerprint:
            admin_args.extend([
                "--expect-admin-token-fingerprint",
                ctx.expect_admin_token_fingerprint,
            ])
        apply_ = run_captured(
            candidate_loom_argv(
                "admin", "environment-state", "apply",
                "--cp-url", ctx.cp_url,
                *admin_args,
                "--file", str(profile_path),
                "--environment", ctx.environment,
                *release_vars,
            ),
            cwd=cwd,
            env=env,
        )
        check_argv = environment_state_check_argv(ctx, step_dir)
        assert check_argv is not None
        check = run_captured(check_argv, cwd=cwd, env=env)
        step_dir.artifact_path("environment-state-check-attempt-1.json").write_text(
            check.stdout,
            encoding="utf-8",
        )
        deferred_gb10_status = (
            check.returncode != 0
            and _is_gb10_node_status_drift_only(check.stdout)
        )
        check_log = ""
        if deferred_gb10_status:
            check_log = (
                "gb10 node-status drift deferred to release-gate; "
                "gb10-prep runs after env-state and starts node-agent apply\n"
            )
        retry_log = step_dir.artifact_path("environment-state-check.retries.log")
        retry_log.write_text(check_log, encoding="utf-8")
        step_dir.artifact_path("environment-state-check.json").write_text(
            check.stdout,
            encoding="utf-8",
        )
        step_dir.stdout_path().write_text(
            f"# apply\n{apply_.stdout}\n"
            f"# check\n{check.stdout}\n",
        )
        step_dir.stderr_path().write_text(
            f"# apply\n{apply_.stderr}\n"
            f"# check\n{check.stderr}\n",
        )
        if apply_.returncode != 0:
            return RunResult(
                exit_code=apply_.returncode,
                error=f"env-state apply failed: {apply_.stderr.strip()[:200]}",
            )
        if check.returncode != 0:
            if deferred_gb10_status:
                return RunResult(
                    exit_code=0,
                    summary=(
                        "env-state apply clean; GB10 node-status convergence "
                        "deferred to release-gate"
                    ),
                    artifacts={
                        "environment_state_check": str(
                            step_dir.artifact_path("environment-state-check.json")
                        ),
                    },
                )
            return RunResult(
                exit_code=check.returncode,
                error=f"env-state check reported drift: {check.stdout.strip()[:200]}",
            )
        return RunResult(
            exit_code=0,
            summary="env-state apply + check clean",
            artifacts={
                "environment_state_check": str(
                    step_dir.artifact_path("environment-state-check.json")
                ),
            },
        )
