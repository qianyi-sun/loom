#!/usr/bin/env python3
"""Run one fail-closed external task-image builder reconciliation."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import secrets
import stat
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

if TYPE_CHECKING:
    from scripts.ops import worker_pool_autoscaler_external_once as transport
else:
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
)
from loom_control_plane.task_image_builder_autoscaler import (  # noqa: E402
    SubprocessTaskImageBuilderSlurmRunner,
    TaskImageBuilderPoolConfig,
    build_task_image_builder_sbatch_request,
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
        witness = transport._load_current_global_execution_witness(
            args,
            pool_id=slurm_cluster_id,
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
            env_template_file=str(policy["env_template_file"]),
            builder_token_file=str(policy["builder_token_file"]),
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
    return config


def _read_private_builder_input(path: Path, *, label: str, max_bytes: int) -> bytes:
    try:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) & 0o077
            or metadata.st_size <= 0
            or metadata.st_size > max_bytes
        ):
            raise TaskImageBuilderPolicyError(
                f"task-image builder {label} metadata is unsafe"
            )
        with path.open("rb") as handle:
            payload = handle.read(max_bytes + 1)
            opened = os.fstat(handle.fileno())
        if (
            len(payload) > max_bytes
            or (opened.st_dev, opened.st_ino, opened.st_size)
            != (metadata.st_dev, metadata.st_ino, metadata.st_size)
        ):
            raise TaskImageBuilderPolicyError(
                f"task-image builder {label} changed while being read"
            )
        return payload
    except TaskImageBuilderPolicyError:
        raise
    except OSError as exc:
        raise TaskImageBuilderPolicyError(
            f"task-image builder {label} is unavailable"
        ) from exc


def _replace_env_values(existing: str, updates: dict[str, str]) -> str:
    output: list[str] = []
    seen: set[str] = set()
    for line in existing.splitlines():
        if "=" not in line or line.lstrip().startswith("#"):
            output.append(line)
            continue
        key = line.split("=", 1)[0].strip()
        if key in updates:
            if key not in seen:
                output.append(f"{key}={updates[key]}")
                seen.add(key)
            continue
        output.append(line)
    for key, value in updates.items():
        if key not in seen:
            output.append(f"{key}={value}")
    return "\n".join(output).rstrip() + "\n"


def _materialize_builder_env(config: Any) -> dict[str, str]:
    """Derive a candidate builder env from the protected trial-worker env.

    The only secret substitution is the stable, least-privilege builder token;
    endpoint and object-store settings stay release-owned by the ordinary
    candidate env materialization path.
    """
    template_path = Path(config.env_template_file)
    token_path = Path(config.builder_token_file)
    target = Path(config.env_file)
    template_payload = _read_private_builder_input(
        template_path,
        label="env template",
        max_bytes=1 << 20,
    )
    token_payload = _read_private_builder_input(
        token_path,
        label="token file",
        max_bytes=64 * 1024,
    )
    try:
        template_text = template_payload.decode("utf-8")
        builder_token = token_payload.strip().decode("ascii")
    except UnicodeError as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder lifecycle input encoding is invalid"
        ) from exc
    if not builder_token or any(character.isspace() for character in builder_token):
        raise TaskImageBuilderPolicyError("task-image builder token file is invalid")
    try:
        parent_metadata = target.parent.lstat()
    except OSError as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder env destination is unavailable"
        ) from exc
    if (
        not stat.S_ISDIR(parent_metadata.st_mode)
        or stat.S_ISLNK(parent_metadata.st_mode)
        or parent_metadata.st_uid != os.geteuid()
        or stat.S_IMODE(parent_metadata.st_mode) & 0o022
    ):
        raise TaskImageBuilderPolicyError(
            "task-image builder env destination metadata is unsafe"
        )
    if target.exists() or target.is_symlink():
        _read_private_builder_input(target, label="env destination", max_bytes=1 << 20)
    rendered = _replace_env_values(
        template_text,
        {
            DEFAULT_WORKER_TOKEN_ENV_KEY: builder_token,
            "LOOM_WORKER_POOL_NAME": str(config.pool_name),
            "LOOM_WORKER_MAX_CONCURRENT": str(config.requested_concurrency),
        },
    ).encode("utf-8")
    temporary = target.parent / f".{target.name}.{secrets.token_hex(12)}.tmp"
    descriptor = -1
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(rendered)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        directory_descriptor = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    except OSError as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder env could not be materialized safely"
        ) from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    target_payload = _read_private_builder_input(
        target,
        label="env destination",
        max_bytes=1 << 20,
    )
    if target_payload != rendered:
        raise TaskImageBuilderPolicyError(
            "task-image builder env changed after materialization"
        )
    return {"env_sha256": hashlib.sha256(rendered).hexdigest()}


def _validate_builder_runtime_files(config: TaskImageBuilderPoolConfig) -> None:
    """Validate scale-up inputs without making emergency drain depend on them."""
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


def _usable_registry_auth(value: object) -> bool:
    return isinstance(value, dict) and any(
        isinstance(value.get(key), str) and value[key]
        for key in ("auth", "identitytoken")
    )


def _builder_registry_auths(registry_docker_config_dir: str) -> dict[str, object]:
    directory = Path(registry_docker_config_dir)
    try:
        metadata = directory.lstat()
    except OSError as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials are unavailable"
        ) from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credential metadata is unsafe"
        )
    payload = _read_private_builder_input(
        directory / "config.json",
        label="registry credential",
        max_bytes=64 * 1024,
    )
    try:
        docker_config = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials are invalid"
        ) from exc
    auths = docker_config.get("auths") if isinstance(docker_config, dict) else None
    if not isinstance(auths, dict):
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials are invalid"
        )
    return auths


async def _validate_builder_token(session: Any, raw_token: str) -> bytes:
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
    return token_hash


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
    auths = _builder_registry_auths(registry_docker_config_dir)

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
    if not _usable_registry_auth(matching_auth):
        raise TaskImageBuilderPolicyError(
            "task-image builder registry credentials do not authorize the configured registry"
        )
    await _validate_builder_token(session, raw_token)


def _rehearsal_validation_evidence(
    config: TaskImageBuilderPoolConfig,
) -> dict[str, object]:
    requests = {
        node: build_task_image_builder_sbatch_request(config, node=node)
        for node in config.allowed_nodes
    }
    request_digests = {
        node: hashlib.sha256(
            json.dumps(
                {"args": list(request.args), "stdin": request.stdin},
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        for node, request in requests.items()
    }
    request_set_sha256 = hashlib.sha256(
        json.dumps(
            request_digests,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "request_nodes": sorted(request_digests),
        "request_set_sha256": request_set_sha256,
    }


async def _reconcile_with_credentials(
    session: Any,
    *,
    config: TaskImageBuilderPoolConfig,
    runner: Any | None,
    scale_up_allowed: bool,
) -> Any | None:
    async with session.begin():
        if scale_up_allowed:
            _materialize_builder_env(config)
            _validate_builder_runtime_files(config)
            await _validate_builder_credentials(
                session,
                env_file=config.env_file,
                registry_docker_config_dir=config.registry_docker_config_dir,
            )
            if runner is None:
                raise TaskImageBuilderPolicyError(
                    "task-image builder activation runner is unavailable"
                )
            for node in config.allowed_nodes:
                await runner.validate_builder_request(node=node, config=config)
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
    if args.validate_only:
        if not _global_execution_scale_up_allowed(
            args,
            slurm_cluster_id=config.slurm_cluster_id,
        ):
            raise TaskImageBuilderPolicyError(
                "global execution witness is unavailable"
            )
        evidence = _rehearsal_validation_evidence(config)
        print(
            json.dumps(
                {
                    "mode": "rehearsal-validate-only",
                    "pool_name": config.pool_name,
                    "cpu_arch": config.cpu_arch,
                    "exclusive": config.exclusive,
                    "requested_concurrency": config.requested_concurrency,
                    **evidence,
                },
                sort_keys=True,
            )
        )
        return
    scale_up_allowed = _global_execution_scale_up_allowed(
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
