"""Step 10 — environment-state apply + desired-state check (#340, #593).

Applies the release environment-state profile (from cluster-config's
declared path) and records an immediate check. Pure GB10 node-status drift is
deferred because GB10 prep now starts after desired state is written; final
node convergence is checked again by release-gate. The #331 fix to
environment-state apply ensures negative desired states (enabled=false /
active=false) actually stop and disable supervisors.
"""

from __future__ import annotations

import glob
import json
import os
import re
import shutil
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from loom.security.redaction import redact_text
from loom.worker_token import DEFAULT_WORKER_TOKEN_ENV_KEY, worker_token_fingerprint
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    candidate_relative_path,
    candidate_worktree,
)
from loom_cli.rollout.steps.subprocess_util import run_captured
from loom_cli.secret_source import SecretSourceError, resolve_secret_source


class ExternalSlurmPrereqMaterializationError(RuntimeError):
    """Raised when rollout cannot converge external Slurm runner prerequisites."""


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
        "--var",
        f"IMAGE_TAG={ctx.image_tag}",
        "--var",
        f"ENV_CONFIG_VERSION={ctx.image_tag}",
        "--var",
        f"GIT_SHA={ctx.resolved_sha}",
    ]
    admin_args = [
        "--admin-token",
        ctx.admin_token_source,
    ]
    if ctx.expect_admin_token_fingerprint:
        admin_args.extend(
            [
                "--expect-admin-token-fingerprint",
                ctx.expect_admin_token_fingerprint,
            ]
        )
    worker_check_args: list[str] = []
    if ctx.worker_token_source:
        worker_check_args.extend(
            [
                "--worker-token",
                ctx.worker_token_source,
            ]
        )
    return candidate_loom_argv(
        "admin",
        "environment-state",
        "check",
        "--cp-url",
        ctx.cp_url,
        *admin_args,
        "--file",
        str(profile_path),
        "--environment",
        ctx.environment,
        *release_vars,
        *worker_check_args,
        "--format",
        "json",
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


def _release_vars(ctx: RolloutContext) -> dict[str, str]:
    return {
        "IMAGE_TAG": ctx.image_tag,
        "ENV_CONFIG_VERSION": ctx.image_tag,
        "GIT_SHA": ctx.resolved_sha,
    }


def _secret_safe_value(value: str) -> str:
    if "\n" in value or "\r" in value:
        raise ExternalSlurmPrereqMaterializationError(
            "external runner env values must be single-line",
        )
    return value


def _update_env_text(existing: str, updates: dict[str, str]) -> str:
    out: list[str] = []
    seen: set[str] = set()
    for line in existing.splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            out.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            if key not in seen:
                out.append(f"{key}={_secret_safe_value(updates[key])}")
                seen.add(key)
            continue
        out.append(line)
    for key, value in updates.items():
        if key not in seen:
            out.append(f"{key}={_secret_safe_value(value)}")
    return "\n".join(out).rstrip() + "\n"


def _select_env_template(
    *,
    target: Path,
    settings: dict[str, Any],
) -> Path:
    template = settings.get("env_template")
    if isinstance(template, str) and template.strip():
        path = Path(template).expanduser()
        if not path.is_file():
            raise ExternalSlurmPrereqMaterializationError(
                f"external runner env template does not exist: {path}",
            )
        return path

    pattern = settings.get("env_template_glob")
    if not isinstance(pattern, str) or not pattern.strip():
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env file {target} is missing and "
            "external_slurm_runner_prerequisites.env_template_glob is not set",
        )
    candidates = [
        Path(path)
        for path in glob.glob(str(Path(pattern).expanduser()))
        if Path(path).is_file() and Path(path) != target
    ]
    if not candidates:
        raise ExternalSlurmPrereqMaterializationError(
            f"external runner env file {target} is missing and no template matched {pattern}",
        )
    return max(candidates, key=lambda path: path.stat().st_mtime_ns)


def _materialize_env_file(
    *,
    env_file: Path,
    settings: dict[str, Any],
    image_tag: str,
    pool_name: str,
    requested_concurrency: object,
    worker_token: str | None,
    worker_token_env_key: str,
) -> dict[str, Any]:
    source = (
        env_file
        if env_file.is_file()
        else _select_env_template(
            target=env_file,
            settings=settings,
        )
    )
    existing = source.read_text(encoding="utf-8")
    updates = {
        "IMAGE_TAG": image_tag,
        "ENV_CONFIG_VERSION": image_tag,
        "LOOM_IMAGE_TAG": image_tag,
        "LOOM_WORKER_ENV_CONFIG_VERSION": image_tag,
        "LOOM_WORKER_POOL_NAME": pool_name,
    }
    if requested_concurrency is not None:
        updates["LOOM_WORKER_MAX_CONCURRENT"] = str(requested_concurrency)
    if worker_token is not None:
        updates[worker_token_env_key] = worker_token

    env_file.parent.mkdir(parents=True, exist_ok=True)
    rendered = _update_env_text(existing, updates)
    tmp = env_file.with_name(f".{env_file.name}.tmp-{os.getpid()}")
    tmp.write_text(rendered, encoding="utf-8")
    os.chmod(tmp, 0o600)
    os.replace(tmp, env_file)
    os.chmod(env_file, 0o600)
    token_fingerprint = worker_token_fingerprint(worker_token) if worker_token is not None else None
    return {
        "env_file": str(env_file),
        "env_action": "updated" if source == env_file else "created",
        "env_template": None if source == env_file else str(source),
        "env_mode": oct(env_file.stat().st_mode & 0o777),
        "worker_token_key": worker_token_env_key if worker_token is not None else None,
        "worker_token": "[REDACTED]" if worker_token is not None else None,
        "worker_token_fingerprint": token_fingerprint,
    }


def _git_stdout(argv: list[str]) -> str:
    result = run_captured(argv)
    if result.returncode != 0:
        raw_message = (result.stderr or result.stdout).strip() or (
            f"{' '.join(argv)} exited {result.returncode}"
        )
        raise ExternalSlurmPrereqMaterializationError(redact_text(raw_message))
    return result.stdout.strip()


def _expected_git_prefix(expected_ref: str) -> str:
    match = re.search(r"([0-9a-f]{7,40})$", expected_ref.strip())
    return match.group(1) if match else expected_ref.strip()


def _repo_status(repo_dir: Path) -> tuple[str, str] | None:
    if not (repo_dir / ".git").exists():
        return None
    try:
        head = _git_stdout(["git", "-C", str(repo_dir), "rev-parse", "HEAD"])
        status = _git_stdout(
            [
                "git",
                "-C",
                str(repo_dir),
                "status",
                "--short",
                "--untracked-files=no",
            ]
        )
    except ExternalSlurmPrereqMaterializationError:
        return None
    return head, status


def _repo_matches(repo_dir: Path, expected_ref: str) -> dict[str, Any] | None:
    status = _repo_status(repo_dir)
    if status is None:
        return None
    head, dirty = status
    if dirty:
        return None
    expected_prefix = _expected_git_prefix(expected_ref)
    if not head.startswith(expected_prefix):
        return None
    return {
        "repo_dir": str(repo_dir),
        "repo_action": "matched",
        "repo_head": head,
        "repo_status": "clean",
    }


def _clone_repo_checkout(
    *,
    source_repo: Path,
    tmp_dir: Path,
    resolved_sha: str,
) -> None:
    origin_result = run_captured(
        [
            "git",
            "-C",
            str(source_repo),
            "config",
            "--get",
            "remote.origin.url",
        ]
    )
    source_url = (
        origin_result.stdout.strip()
        if origin_result.returncode == 0 and origin_result.stdout.strip()
        else str(source_repo)
    )
    try:
        _git_stdout(["git", "clone", "--quiet", source_url, str(tmp_dir)])
        _git_stdout(["git", "-C", str(tmp_dir), "checkout", "--detach", resolved_sha])
    except ExternalSlurmPrereqMaterializationError:
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)
        _git_stdout(
            [
                "git",
                "clone",
                "--quiet",
                "--no-hardlinks",
                str(source_repo),
                str(tmp_dir),
            ]
        )
        _git_stdout(["git", "-C", str(tmp_dir), "checkout", "--detach", resolved_sha])
    dirty = _git_stdout(
        [
            "git",
            "-C",
            str(tmp_dir),
            "status",
            "--short",
            "--untracked-files=no",
        ]
    )
    if dirty:
        raise ExternalSlurmPrereqMaterializationError(
            f"fresh external runner checkout is dirty: {tmp_dir}",
        )


def _materialize_repo_dir(
    *,
    repo_dir: Path,
    source_repo: Path,
    resolved_sha: str,
    expected_ref: str,
) -> dict[str, Any]:
    matched = _repo_matches(repo_dir, expected_ref)
    if matched is not None:
        return matched

    repo_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = repo_dir.with_name(f".{repo_dir.name}.tmp-{os.getpid()}")
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    _clone_repo_checkout(
        source_repo=source_repo,
        tmp_dir=tmp_dir,
        resolved_sha=resolved_sha,
    )

    action = "created"
    if repo_dir.exists():
        backup = repo_dir.with_name(
            f".{repo_dir.name}.previous-{time.time_ns()}",
        )
        repo_dir.rename(backup)
        action = "replaced"
    tmp_dir.rename(repo_dir)
    os.chmod(repo_dir, 0o2700)
    head = _git_stdout(["git", "-C", str(repo_dir), "rev-parse", "HEAD"])
    return {
        "repo_dir": str(repo_dir),
        "repo_action": action,
        "repo_head": head,
        "repo_status": "clean",
    }


def _materialize_external_slurm_runner_prerequisites(
    ctx: RolloutContext,
    profile_path: Path | str,
    step_dir: StepDir,
) -> list[dict[str, Any]]:
    from loom_cli.environment_state import (
        _external_slurm_policies,
        load_environment_state_profile,
    )

    try:
        profile = load_environment_state_profile(
            profile_path,
            variables=_release_vars(ctx),
            expected_environment=ctx.environment,
        )
    except Exception:
        return []

    settings = profile.external_slurm_runner_prerequisites
    if not settings or settings.get("materialize") is not True:
        return []

    configured_pools = settings.get("pools")
    checked_pools = set(configured_pools) if isinstance(configured_pools, list) else None
    require_worker_token_parity = bool(
        settings.get("require_worker_token_parity", False),
    )
    worker_token_env_key = str(
        settings.get("worker_token_env_key") or DEFAULT_WORKER_TOKEN_ENV_KEY,
    )
    worker_token: str | None = None
    if require_worker_token_parity:
        if not ctx.worker_token_source:
            raise ExternalSlurmPrereqMaterializationError(
                "external runner materialization requires --worker-token "
                "because require_worker_token_parity=true",
            )
        try:
            worker_token = resolve_secret_source(
                ctx.worker_token_source,
                flag_name="--worker-token",
            )
        except SecretSourceError as exc:
            raise ExternalSlurmPrereqMaterializationError(str(exc)) from exc

    records: list[dict[str, Any]] = []
    source_repo = candidate_worktree(step_dir)
    expected_repo_ref = str(settings.get("expected_repo_ref") or ctx.image_tag)
    for policy in _external_slurm_policies(profile):
        pool_name = str(policy["pool_name"])
        if checked_pools is not None and pool_name not in checked_pools:
            continue
        actuator_config = policy.get("actuator_config", {})
        if not isinstance(actuator_config, dict):
            continue
        env_file = actuator_config.get("env_file")
        repo_dir = actuator_config.get("repo_dir")
        if not isinstance(env_file, str) or not isinstance(repo_dir, str):
            continue
        record = {
            "environment": policy["environment"],
            "pool_name": pool_name,
        }
        record.update(
            _materialize_env_file(
                env_file=Path(env_file).expanduser(),
                settings=settings,
                image_tag=ctx.image_tag,
                pool_name=pool_name,
                requested_concurrency=actuator_config.get("requested_concurrency"),
                worker_token=worker_token,
                worker_token_env_key=worker_token_env_key,
            )
        )
        record.update(
            _materialize_repo_dir(
                repo_dir=Path(repo_dir).expanduser(),
                source_repo=source_repo,
                resolved_sha=ctx.resolved_sha,
                expected_ref=expected_repo_ref,
            )
        )
        records.append(record)

    if records:
        step_dir.artifact_path("external-slurm-runner-prerequisites.json").write_text(
            json.dumps({"records": records}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return records


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
            "--var",
            f"IMAGE_TAG={ctx.image_tag}",
            "--var",
            f"ENV_CONFIG_VERSION={ctx.image_tag}",
            "--var",
            f"GIT_SHA={ctx.resolved_sha}",
        ]
        admin_args = [
            "--admin-token",
            ctx.admin_token_source,
        ]
        if ctx.expect_admin_token_fingerprint:
            admin_args.extend(
                [
                    "--expect-admin-token-fingerprint",
                    ctx.expect_admin_token_fingerprint,
                ]
            )
        try:
            materialized = _materialize_external_slurm_runner_prerequisites(
                ctx,
                profile_path,
                step_dir,
            )
        except ExternalSlurmPrereqMaterializationError as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n")
            return RunResult(exit_code=2, error=str(exc))
        apply_ = run_captured(
            candidate_loom_argv(
                "admin",
                "environment-state",
                "apply",
                "--cp-url",
                ctx.cp_url,
                *admin_args,
                "--file",
                str(profile_path),
                "--environment",
                ctx.environment,
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
        deferred_gb10_status = check.returncode != 0 and _is_gb10_node_status_drift_only(
            check.stdout
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
            "# external-slurm-runner-prerequisites\n"
            f"materialized {len(materialized)} external runner prerequisite set(s)\n"
            f"# apply\n{apply_.stdout}\n"
            f"# check\n{check.stdout}\n",
        )
        step_dir.stderr_path().write_text(
            f"# apply\n{apply_.stderr}\n# check\n{check.stderr}\n",
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
