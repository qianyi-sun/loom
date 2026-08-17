#!/usr/bin/env python3
"""Run one fail-closed external task-image builder reconciliation."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

import worker_pool_autoscaler_external_once as transport  # noqa: E402

from loom.db.schema import Token  # noqa: E402
from loom.worker_token import DEFAULT_WORKER_TOKEN_ENV_KEY, read_env_file_value  # noqa: E402
from loom_cli.environment_state import (  # noqa: E402
    EnvironmentStateProfileError,
    load_environment_state_profile,
)
from loom_control_plane.global_execution_fence import (  # noqa: E402
    GlobalExecutionFenceError,
    assert_legacy_scale_up_allowed,
    load_global_execution_witness,
)
from loom_control_plane.task_image_builder_autoscaler import (  # noqa: E402
    SubprocessTaskImageBuilderSlurmRunner,
    TaskImageBuilderPoolConfig,
    reconcile_task_image_builder_autoscaler_once,
)

_REGISTRY_REPO_ENV_KEY = "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO"


class TaskImageBuilderPolicyError(transport.ExternalAutoscalerError, ValueError):
    """The dedicated builder policy is absent, disabled, or unsafe."""


def _global_execution_scale_up_allowed(
    args: argparse.Namespace,
    *,
    slurm_cluster_id: str,
) -> bool:
    try:
        witness = load_global_execution_witness(
            args.global_execution_witness_json,
            manager_public_key_path=args.manager_public_key,
            expected_manager_public_key_sha256=args.expected_manager_public_key_sha256,
            expected_manager_public_key_sha256_file=(
                args.expected_manager_public_key_sha256_file
            ),
        )
        assert_legacy_scale_up_allowed(
            witness,
            expected_authority="global-capacity-manager",
            expected_pool_id=slurm_cluster_id,
            now=datetime.now(UTC),
        )
    except GlobalExecutionFenceError:
        return False
    return True


def _parser() -> argparse.ArgumentParser:
    parser = transport._parser()
    parser.description = "Run one scoped external task-image builder reconcile."
    parser.add_argument("--profile", type=Path, required=True)
    parser.add_argument("--image-tag", required=True)
    parser.add_argument("--env-config-version", required=True)
    parser.add_argument("--git-sha", required=True)
    return parser


def _load_enabled_builder_config(args: argparse.Namespace) -> TaskImageBuilderPoolConfig:
    environment = transport._scoped_environment(args.environment)
    pool_names = transport._scoped_pool_names(args.pool_name)
    if len(pool_names) != 1:
        raise TaskImageBuilderPolicyError("exactly one task-image builder pool is required")
    if args.capacity_grants_json is not None or args.deployment_generation is not None:
        raise TaskImageBuilderPolicyError(
            "task-image builders do not accept trial-capacity grants",
        )
    try:
        profile = load_environment_state_profile(
            args.profile,
            variables={
                "IMAGE_TAG": args.image_tag,
                "ENV_CONFIG_VERSION": args.env_config_version,
                "GIT_SHA": args.git_sha,
            },
            expected_environment=environment,
        )
    except EnvironmentStateProfileError as exc:
        raise TaskImageBuilderPolicyError("task-image builder profile is invalid") from exc
    pool_name = pool_names[0]
    matches = [
        policy for policy in profile.task_image_builder_policies if policy["pool_name"] == pool_name
    ]
    if len(matches) != 1:
        raise TaskImageBuilderPolicyError(
            "task-image builder policy must exist exactly once",
        )
    policy = matches[0]
    if policy["enabled"] is not True:
        raise TaskImageBuilderPolicyError("task-image builder policy is disabled")
    if policy["activation_blockers"]:
        raise TaskImageBuilderPolicyError(
            "task-image builder policy still has activation blockers",
        )
    expected_cluster_name = {
        "oldlab": "trt-oldlab",
        "gb10": "trt-gb10",
    }[str(policy["slurm_cluster_id"])]
    if args.expected_slurm_cluster_name != expected_cluster_name:
        raise TaskImageBuilderPolicyError(
            "task-image builder policy does not match the expected Slurm cluster",
        )
    try:
        config = TaskImageBuilderPoolConfig(
            environment=str(policy["environment"]),
            pool_name=str(policy["pool_name"]),
            slurm_cluster_id=str(policy["slurm_cluster_id"]),  # type: ignore[arg-type]
            cpu_arch=str(policy["cpu_arch"]),  # type: ignore[arg-type]
            allowed_nodes=tuple(str(node) for node in policy["allowed_nodes"]),
            env_file=str(policy["env_file"]),
            repo_dir=str(policy["repo_dir"]),
            registry_docker_config_dir=str(policy["registry_docker_config_dir"]),
            partition=str(policy["partition"]),
            time_limit=str(policy["time_limit"]),
            requested_cpus=int(policy["requested_cpus"]),
            requested_memory_mib=int(policy["requested_memory_mib"]),
            requested_concurrency=int(policy["requested_concurrency"]),
            max_jobs=int(policy["max_jobs"]),
            pending_job_cap=int(policy["pending_job_cap"]),
            idle_exit_after_seconds=int(policy["idle_exit_after_seconds"]),
            sbatch_path=str(policy["sbatch_path"]),
            squeue_path=str(policy["squeue_path"]),
            sacct_path=str(policy["sacct_path"]),
            scancel_path=str(policy["scancel_path"]),
            command_timeout_seconds=float(policy["command_timeout_seconds"]),
            exclusive=bool(policy["exclusive"]),
            slurm_account=str(policy["slurm_account"]),
            slurm_qos=str(policy["slurm_qos"]),
            slurm_reservation=str(policy["slurm_reservation"]),
            job_output_dir=str(policy["job_output_dir"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder runtime configuration is invalid",
        ) from exc
    if not Path(config.env_file).is_file():
        raise TaskImageBuilderPolicyError("task-image builder env file is unavailable")
    if not Path(config.repo_dir).is_dir():
        raise TaskImageBuilderPolicyError("task-image builder repository is unavailable")
    docker_config_dir = Path(config.registry_docker_config_dir)
    docker_config_file = docker_config_dir / "config.json"
    try:
        directory_stat = docker_config_dir.stat()
        config_stat = docker_config_file.stat()
    except OSError as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials are unavailable"
        ) from exc
    if (
        not docker_config_dir.is_dir()
        or not docker_config_file.is_file()
        or directory_stat.st_uid != os.geteuid()
        or config_stat.st_uid != os.geteuid()
        or directory_stat.st_mode & 0o077
        or config_stat.st_mode & 0o077
    ):
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credential metadata is unsafe"
        )
    return config


async def _validate_builder_credentials(
    session: Any,
    *,
    env_file: str,
    registry_docker_config_dir: str,
) -> None:
    path = Path(env_file)
    try:
        raw_token = read_env_file_value(path, DEFAULT_WORKER_TOKEN_ENV_KEY)
        registry_repo = read_env_file_value(path, _REGISTRY_REPO_ENV_KEY)
    except (OSError, UnicodeError) as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder credentials are unavailable",
        ) from exc
    if not raw_token:
        raise TaskImageBuilderPolicyError("task-image builder token is missing")
    if not registry_repo:
        raise TaskImageBuilderPolicyError("task-image builder registry is missing")
    registry_host = registry_repo.split("/", 1)[0]
    docker_config_file = Path(registry_docker_config_dir) / "config.json"
    try:
        if docker_config_file.stat().st_size > 64 * 1024:
            raise TaskImageBuilderPolicyError(
                "task-image builder registry credentials are invalid"
            )
        docker_config = json.loads(docker_config_file.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials are unavailable"
        ) from exc
    auths = docker_config.get("auths") if isinstance(docker_config, dict) else None
    if not isinstance(auths, dict):
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials are invalid"
        )

    def normalized_registry(value: str) -> str:
        normalized = value.removeprefix("https://").removeprefix("http://").rstrip("/")
        return normalized.removesuffix("/v1")

    matching_auth = next(
        (
            value
            for key, value in auths.items()
            if isinstance(key, str) and normalized_registry(key) == registry_host
        ),
        None,
    )
    if not isinstance(matching_auth, dict) or not any(
        isinstance(matching_auth.get(key), str) and matching_auth[key]
        for key in ("auth", "identitytoken", "username")
    ):
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials do not authorize the configured registry"
        )
    token_hash = hashlib.sha256(raw_token.encode("utf-8")).digest()
    result = await session.execute(
        select(Token.type, Token.scopes, Token.expires_at, Token.revoked_at).where(
            Token.token_hash == token_hash,
        )
    )
    row = result.one_or_none()
    if row is None:
        raise TaskImageBuilderPolicyError("task-image builder token is unknown")
    token_type, scopes, expires_at, revoked_at = row
    if token_type != "worker" or set(scopes or ()) != {"task-image:build"}:
        raise TaskImageBuilderPolicyError(
            "task-image builder token requires the dedicated task-image:build scope",
        )
    now = datetime.now(UTC)
    if revoked_at is not None or (expires_at is not None and expires_at <= now):
        raise TaskImageBuilderPolicyError("task-image builder token is not active")


async def _reconcile_with_credentials(
    session: Any,
    *,
    config: TaskImageBuilderPoolConfig,
    runner: Any | None,
    scale_up_allowed: bool,
) -> Any | None:
    async with session.begin():
        await _validate_builder_credentials(
            session,
            env_file=config.env_file,
            registry_docker_config_dir=config.registry_docker_config_dir,
        )
        if runner is None:
            return None
        return await reconcile_task_image_builder_autoscaler_once(
            session,
            config=config,
            runner=runner,
            scale_up_allowed=scale_up_allowed,
        )


async def _main_async(args: argparse.Namespace) -> None:
    config = _load_enabled_builder_config(args)
    authority = transport._validate_local_slurm_authority(args)
    expected_cluster = {"oldlab": "trt-oldlab", "gb10": "trt-gb10"}[config.slurm_cluster_id]
    if authority.cluster_name != expected_cluster:
        raise TaskImageBuilderPolicyError(
            "local Slurm authority does not match the builder policy",
        )
    scale_up_allowed = args.validate_only or _global_execution_scale_up_allowed(
        args,
        slurm_cluster_id=config.slurm_cluster_id,
    )
    port_forward = transport._database_port_forward_config(args)
    db_connect_timeout_sec = transport._validated_timeout(
        args.db_connect_timeout_sec,
        "--db-connect-timeout-sec",
        maximum=transport._MAX_PORT_FORWARD_READY_TIMEOUT_SEC,
    )
    db_url = transport._load_cp_db_url(args, timeout_sec=db_connect_timeout_sec)
    url = transport._preflight_database_url(db_url, port_forward=port_forward)
    with transport._database_port_forward(port_forward):
        engine = create_async_engine(url, pool_pre_ping=True)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        try:
            async with session_factory() as session:
                if args.validate_only:
                    async with asyncio.timeout(db_connect_timeout_sec):
                        await _reconcile_with_credentials(
                            session,
                            config=config,
                            runner=None,
                            scale_up_allowed=True,
                        )
                    print(
                        json.dumps(
                            {
                                "mode": "validate-only",
                                "pool_name": config.pool_name,
                                "cpu_arch": config.cpu_arch,
                                "exclusive": config.exclusive,
                                "requested_concurrency": config.requested_concurrency,
                            },
                            sort_keys=True,
                        )
                    )
                    return
                runner = SubprocessTaskImageBuilderSlurmRunner(config)
                result = await _reconcile_with_credentials(
                    session,
                    config=config,
                    runner=runner,
                    scale_up_allowed=scale_up_allowed,
                )
                if result is None:
                    raise RuntimeError(
                        "task-image builder reconciliation returned no result",
                    )
                print(json.dumps(result.__dict__, sort_keys=True))
        finally:
            await engine.dispose()
    if not scale_up_allowed:
        raise TaskImageBuilderPolicyError(
            "global execution witness is unavailable; builder capacity was drained",
        )


def main() -> None:
    try:
        asyncio.run(_main_async(_parser().parse_args()))
    except transport.ExternalAutoscalerError as exc:
        sys.stderr.write(f"error: {exc}\n")
        raise SystemExit(1) from None
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception:
        sys.stderr.write("error: task-image builder autoscaler failed safely\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
