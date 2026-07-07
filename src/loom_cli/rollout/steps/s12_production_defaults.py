"""Step 13 — production service defaults (#482).

Protected rollouts must restore DB-backed production defaults that are not
Kubernetes manifests: hosted provider pricing modes and official rate-card
catalogs. This step runs after cluster-up so the public service/gateway path is
serving the newly rolled image, and before release-gate/smoke so production
canaries cannot proceed with missing cost-attribution defaults.
"""

from __future__ import annotations

import json
import re
import tempfile
from pathlib import Path
from typing import Any

import tomli_w

from loom.security.redaction import redact_text
from loom_cli.cluster_config import load_cluster_config
from loom_cli.environment_state import (
    EnvironmentStateProfile,
    EnvironmentStateProfileError,
    load_environment_state_profile,
)
from loom_cli.rollout.context import RolloutContext
from loom_cli.rollout.evidence import StepDir
from loom_cli.rollout.steps.base import BaseStep, RunResult
from loom_cli.rollout.steps.candidate_source import (
    CandidateToolingError,
    candidate_loom_argv,
    candidate_loom_cwd,
    candidate_loom_env,
    rollout_cluster_config,
)
from loom_cli.rollout.steps.s10_env_state import _profile_path_for
from loom_cli.rollout.steps.subprocess_util import SubprocessResult, run_captured
from loom_cli.secret_source import SecretSourceError, resolve_secret_source

_SAFE_ARTIFACT_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_PRODUCTION_DEFAULTS_ADMIN_ACTOR = "rollout-production-defaults"


def _release_vars(ctx: RolloutContext) -> dict[str, str]:
    return {
        "IMAGE_TAG": ctx.image_tag,
        "ENV_CONFIG_VERSION": ctx.image_tag,
        "GIT_SHA": ctx.resolved_sha,
    }


def _artifact_safe(value: str) -> str:
    cleaned = _SAFE_ARTIFACT_RE.sub("-", value.strip())
    return cleaned.strip("-") or "provider"


def _yibuapi_sync_config(profile: EnvironmentStateProfile) -> dict[str, Any] | None:
    raw = profile.rate_card_sync.get("yibuapi")
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise EnvironmentStateProfileError("rate_card_sync.yibuapi must be a table")
    if not bool(raw.get("enabled", False)):
        return None
    return dict(raw)


def _normalise_route_path(value: str) -> str:
    path = value.strip()
    if not path or path == "/":
        return ""
    if not path.startswith("/"):
        path = f"/{path}"
    return path.rstrip("/")


def _service_server_url(ctx: RolloutContext, step_dir: StepDir) -> str:
    cfg = load_cluster_config(rollout_cluster_config(ctx, step_dir))
    route_path = _normalise_route_path(
        cfg.frontend_api_base_path or cfg.frontend_route_path,
    )
    return f"https://{cfg.ingress_host.strip()}{route_path}"


def _write_service_cli_config(
    *,
    xdg_config_home: Path,
    server_url: str,
    auth_token: str,
) -> None:
    loom_config_dir = xdg_config_home / "loom"
    loom_config_dir.mkdir(parents=True, exist_ok=True)
    if hasattr(loom_config_dir, "chmod"):
        loom_config_dir.chmod(0o700)
    config_path = loom_config_dir / "config.toml"
    config_path.write_text(
        tomli_w.dumps(
            {
                "server_url": server_url,
                "auth_token": auth_token,
            }
        ),
        encoding="utf-8",
    )
    if hasattr(config_path, "chmod"):
        config_path.chmod(0o600)


def _redact_step_text(value: str, *, secret_values: tuple[str, ...]) -> str:
    redacted = value
    for secret in secret_values:
        if secret:
            redacted = redacted.replace(secret, "[REDACTED:service-token]")
    return redact_text(redacted)


def _error_excerpt(value: str, *, secret_values: tuple[str, ...]) -> str:
    return _redact_step_text(
        value.strip(),
        secret_values=secret_values,
    )[:200]


class ProductionDefaultsStep(BaseStep):
    number = 13
    name = "production-defaults"

    def _inputs_fingerprint(self, ctx: RolloutContext) -> dict[str, object]:
        return {
            "cluster_config_sha256": ctx.cluster_config_sha256,
            "environment": ctx.environment,
            "image_tag": ctx.image_tag,
            "resolved_sha": ctx.resolved_sha,
            "service_token_source": ctx.service_token_source,
        }

    def _load_profile(
        self,
        ctx: RolloutContext,
        profile_path: str,
    ) -> EnvironmentStateProfile:
        return load_environment_state_profile(
            Path(profile_path),
            variables=_release_vars(ctx),
            expected_environment=ctx.environment,
        )

    def _run_cmd(
        self,
        *,
        argv: list[str],
        cwd: Path,
        env: dict[str, str],
        stdout_chunks: list[str],
        stderr_chunks: list[str],
        secret_values: tuple[str, ...] = (),
        artifact_path: Path | None = None,
    ) -> SubprocessResult:
        result = run_captured(argv, cwd=cwd, env=env)
        label = " ".join(argv[3:])
        stdout = _redact_step_text(result.stdout, secret_values=secret_values)
        stderr = _redact_step_text(result.stderr, secret_values=secret_values)
        stdout_chunks.append(f"# {label}\n{stdout}\n")
        stderr_chunks.append(f"# {label}\n{stderr}\n")
        if artifact_path is not None:
            artifact_path.write_text(stdout, encoding="utf-8")
        return result

    def _write_logs(
        self,
        step_dir: StepDir,
        *,
        stdout_chunks: list[str],
        stderr_chunks: list[str],
    ) -> None:
        step_dir.stdout_path().write_text("".join(stdout_chunks), encoding="utf-8")
        step_dir.stderr_path().write_text("".join(stderr_chunks), encoding="utf-8")

    def _run_impl(self, ctx: RolloutContext, step_dir: StepDir) -> RunResult:
        profile_path = _profile_path_for(ctx)
        if profile_path is None:
            step_dir.stdout_path().write_text(
                "no env_state_profile declared in cluster-config; skipping.\n",
            )
            return RunResult(
                exit_code=0,
                summary="no env-state profile; step is a no-op",
            )

        try:
            profile = self._load_profile(ctx, profile_path)
            yibuapi_sync = _yibuapi_sync_config(profile)
        except EnvironmentStateProfileError as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=str(exc))

        provider_defaults = profile.hosted_provider_pricing_defaults
        if yibuapi_sync is None and not provider_defaults:
            step_dir.stdout_path().write_text(
                "no production defaults declared in env_state_profile; skipping.\n",
                encoding="utf-8",
            )
            return RunResult(
                exit_code=0,
                summary="no production defaults declared; step is a no-op",
            )

        if ctx.service_token_source is None:
            message = (
                "production defaults require --service-token env:VAR or "
                "file:PATH so rollout-owned Service API calls do not depend "
                "on ambient `loom auth login` state"
            )
            step_dir.stderr_path().write_text(message + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=message)

        try:
            service_token = resolve_secret_source(
                ctx.service_token_source,
                flag_name="--service-token",
            )
        except SecretSourceError as exc:
            message = str(exc)
            step_dir.stderr_path().write_text(message + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=message)

        try:
            cwd = candidate_loom_cwd(step_dir)
            env = candidate_loom_env(step_dir)
            service_server_url = _service_server_url(ctx, step_dir)
        except (CandidateToolingError, OSError, ValueError) as exc:
            step_dir.stderr_path().write_text(str(exc) + "\n", encoding="utf-8")
            return RunResult(exit_code=2, error=str(exc))

        stdout_chunks: list[str] = []
        stderr_chunks: list[str] = []
        artifacts: dict[str, str] = {}
        secret_values = (service_token,)

        with tempfile.TemporaryDirectory(prefix="loom-rollout-service-auth-") as tmp:
            env = dict(env)
            env["XDG_CONFIG_HOME"] = tmp
            _write_service_cli_config(
                xdg_config_home=Path(tmp),
                server_url=service_server_url,
                auth_token=service_token,
            )

            if yibuapi_sync is not None:
                argv = candidate_loom_argv(
                    "admin",
                    "rate-cards",
                    "sync-yibuapi",
                    "--group",
                    str(yibuapi_sync.get("group") or "default"),
                    "--format",
                    "json",
                )
                source_url = yibuapi_sync.get("source_url")
                if source_url:
                    argv.extend(["--source-url", str(source_url)])
                artifact = step_dir.artifact_path("rate-card-sync-yibuapi.json")
                result = self._run_cmd(
                    argv=argv,
                    cwd=cwd,
                    env=env,
                    stdout_chunks=stdout_chunks,
                    stderr_chunks=stderr_chunks,
                    secret_values=secret_values,
                    artifact_path=artifact,
                )
                artifacts["rate_card_sync_yibuapi"] = str(artifact)
                if result.returncode != 0:
                    self._write_logs(
                        step_dir,
                        stdout_chunks=stdout_chunks,
                        stderr_chunks=stderr_chunks,
                    )
                    return RunResult(
                        exit_code=result.returncode,
                        error=(
                            "YibuAPI rate-card sync failed: "
                            f"{_error_excerpt(result.stderr, secret_values=secret_values)}"
                        ),
                        artifacts=artifacts,
                    )

            for desired in provider_defaults:
                name = str(desired["name"])
                pricing_source = str(desired["pricing_source"])
                update_argv = candidate_loom_argv(
                    "providers",
                    "update",
                    name,
                    "--pricing-source",
                    pricing_source,
                )
                rate_card_provider = desired.get("rate_card_provider")
                if rate_card_provider is not None:
                    update_argv.extend(["--rate-card-provider", str(rate_card_provider)])
                update_argv.extend([
                    "--admin-actor",
                    _PRODUCTION_DEFAULTS_ADMIN_ACTOR,
                ])
                update = self._run_cmd(
                    argv=update_argv,
                    cwd=cwd,
                    env=env,
                    stdout_chunks=stdout_chunks,
                    stderr_chunks=stderr_chunks,
                    secret_values=secret_values,
                )
                if update.returncode != 0:
                    self._write_logs(
                        step_dir,
                        stdout_chunks=stdout_chunks,
                        stderr_chunks=stderr_chunks,
                    )
                    return RunResult(
                        exit_code=update.returncode,
                        error=(
                            f"provider {name!r} default update failed: "
                            f"{_error_excerpt(update.stderr, secret_values=secret_values)}"
                        ),
                        artifacts=artifacts,
                    )

                provider_artifact = step_dir.artifact_path(
                    f"provider-{_artifact_safe(name)}.json",
                )
                show = self._run_cmd(
                    argv=candidate_loom_argv(
                        "providers",
                        "show",
                        name,
                        "--format",
                        "json",
                    ),
                    cwd=cwd,
                    env=env,
                    stdout_chunks=stdout_chunks,
                    stderr_chunks=stderr_chunks,
                    secret_values=secret_values,
                    artifact_path=provider_artifact,
                )
                artifacts[f"provider_{_artifact_safe(name)}"] = str(provider_artifact)
                if show.returncode != 0:
                    self._write_logs(
                        step_dir,
                        stdout_chunks=stdout_chunks,
                        stderr_chunks=stderr_chunks,
                    )
                    return RunResult(
                        exit_code=show.returncode,
                        error=(
                            f"provider {name!r} verification failed: "
                            f"{_error_excerpt(show.stderr, secret_values=secret_values)}"
                        ),
                        artifacts=artifacts,
                    )
                try:
                    observed = json.loads(show.stdout)
                except json.JSONDecodeError as exc:
                    self._write_logs(
                        step_dir,
                        stdout_chunks=stdout_chunks,
                        stderr_chunks=stderr_chunks,
                    )
                    return RunResult(
                        exit_code=1,
                        error=f"provider {name!r} show output was not JSON: {exc}",
                        artifacts=artifacts,
                    )
                if observed.get("pricing_source") != pricing_source:
                    self._write_logs(
                        step_dir,
                        stdout_chunks=stdout_chunks,
                        stderr_chunks=stderr_chunks,
                    )
                    return RunResult(
                        exit_code=1,
                        error=(
                            f"provider {name!r} pricing_source drift: "
                            f"desired={pricing_source!r} "
                            f"live={observed.get('pricing_source')!r}"
                        ),
                        artifacts=artifacts,
                    )
                if (
                    rate_card_provider is not None
                    and observed.get("rate_card_provider") != rate_card_provider
                ):
                    self._write_logs(
                        step_dir,
                        stdout_chunks=stdout_chunks,
                        stderr_chunks=stderr_chunks,
                    )
                    return RunResult(
                        exit_code=1,
                        error=(
                            f"provider {name!r} rate_card_provider drift: "
                            f"desired={rate_card_provider!r} "
                            f"live={observed.get('rate_card_provider')!r}"
                        ),
                        artifacts=artifacts,
                    )

        self._write_logs(
            step_dir,
            stdout_chunks=stdout_chunks,
            stderr_chunks=stderr_chunks,
        )
        return RunResult(
            exit_code=0,
            summary="production defaults applied and verified",
            artifacts=artifacts,
        )
