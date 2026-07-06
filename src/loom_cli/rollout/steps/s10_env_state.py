"""Step 10 — environment-state apply + check (#340).

Applies the release environment-state profile (from cluster-config's
declared path) and then runs the check to confirm convergence. The
#331 fix to environment-state apply ensures negative desired states
(enabled=false / active=false) actually stop and disable supervisors.
"""

from __future__ import annotations

import json
import time
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

_ENV_STATE_CHECK_MAX_ATTEMPTS = 30
_ENV_STATE_CHECK_RETRY_DELAY_SEC = 2.0


def _is_retryable_gb10_convergence_drift(stdout: str) -> bool:
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError:
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
        apply_ = run_captured(
            candidate_loom_argv(
                "admin", "environment-state", "apply",
                "--cp-url", ctx.cp_url,
                "--file", str(profile_path),
                "--environment", ctx.environment,
                *release_vars,
            ),
            cwd=cwd,
            env=env,
        )
        check_argv = candidate_loom_argv(
            "admin", "environment-state", "check",
            "--cp-url", ctx.cp_url,
            "--file", str(profile_path),
            "--environment", ctx.environment,
            *release_vars,
            "--format", "json",
        )
        check_attempt_logs: list[str] = []
        check = run_captured(check_argv, cwd=cwd, env=env)
        for attempt in range(1, _ENV_STATE_CHECK_MAX_ATTEMPTS + 1):
            if attempt > 1:
                check = run_captured(check_argv, cwd=cwd, env=env)
            step_dir.artifact_path(
                f"environment-state-check-attempt-{attempt}.json",
            ).write_text(check.stdout, encoding="utf-8")
            if check.returncode == 0:
                break
            if not _is_retryable_gb10_convergence_drift(check.stdout):
                break
            check_attempt_logs.append(
                f"attempt {attempt}/{_ENV_STATE_CHECK_MAX_ATTEMPTS}: "
                "gb10 convergence drift; waiting for node-agent status\n",
            )
            if attempt < _ENV_STATE_CHECK_MAX_ATTEMPTS:
                time.sleep(_ENV_STATE_CHECK_RETRY_DELAY_SEC)
        retry_log = step_dir.artifact_path("environment-state-check.retries.log")
        retry_log.write_text("".join(check_attempt_logs), encoding="utf-8")
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
