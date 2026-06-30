"""Versioned environment desired-state profiles for deploy rollouts."""

from __future__ import annotations

import re
import shlex
import subprocess
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from loom.worker_token import (
    DEFAULT_WORKER_TOKEN_ENV_KEY,
    WORKER_AUTH_FINGERPRINT_ENV_KEY,
    read_env_file_value,
    worker_token_fingerprint,
)

_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

_AUTOSCALER_DEFAULTS: dict[str, Any] = {
    "enabled": False,
    "min_slots": 0,
    "scale_up_threshold_slots": 1,
    "scale_down_idle_seconds": 600,
    "scale_up_cooldown_seconds": 60,
    "scale_down_cooldown_seconds": 300,
    "drain_timeout_seconds": 600,
    "force": False,
    "disabled_reason": None,
    "actuator_config": {},
}
_AUTOSCALER_COMPARE_FIELDS = (
    "actuator",
    "enabled",
    "min_slots",
    "max_slots",
    "scale_up_threshold_slots",
    "scale_down_idle_seconds",
    "scale_up_cooldown_seconds",
    "scale_down_cooldown_seconds",
    "drain_timeout_seconds",
    "force",
    "disabled_reason",
    "actuator_config",
)

_GB10_DEFAULTS: dict[str, Any] = {
    "target_slots": None,
    "host_intents": {},
    "rollout_policy": {},
    "env": {},
    "force": False,
}
_GB10_COMPARE_FIELDS = (
    "image_tag",
    "max_concurrent",
    "env_config_version",
    "target_slots",
    "host_intents",
    "rollout_policy",
    "env",
    "force",
)

_EXTERNAL_AUTOSCALER_SUPERVISOR_DEFAULTS: dict[str, Any] = {
    "args": [],
    "requires": ["network-online.target"],
    "timer_on_boot_sec": "45",
    "timer_on_unit_active_sec": "30",
    "timer_accuracy_sec": "5",
    "enabled": True,
    "active": True,
}


class EnvironmentStateProfileError(ValueError):
    """Raised when an environment desired-state profile is invalid."""


@dataclass(frozen=True)
class EnvironmentStateProfile:
    environment: str
    control_plane_environment: str
    autoscaler_policies: list[dict[str, Any]]
    gb10_desired_states: list[dict[str, Any]]
    catalog_provisioning: dict[str, Any]
    external_slurm_runner_prerequisites: dict[str, Any]
    external_slurm_autoscaler_supervisors: list[dict[str, Any]]


@dataclass(frozen=True)
class StateDrift:
    path: str
    desired: Any
    live: Any


def _clean_nonempty(value: object, field: str) -> str:
    cleaned = str(value).strip()
    if not cleaned:
        raise EnvironmentStateProfileError(f"{field} must be a non-empty string")
    return cleaned


def _replace_placeholders(value: Any, variables: dict[str, str], *, path: str) -> Any:
    if isinstance(value, str):
        missing: list[str] = []

        def _replacement(match: re.Match[str]) -> str:
            name = match.group(1)
            if name not in variables:
                missing.append(name)
                return match.group(0)
            return str(variables[name])

        replaced = _PLACEHOLDER_RE.sub(_replacement, value)
        if missing:
            names = ", ".join(sorted(set(missing)))
            raise EnvironmentStateProfileError(
                f"{path} references missing variable(s): {names}",
            )
        return replaced
    if isinstance(value, list):
        return [
            _replace_placeholders(item, variables, path=f"{path}[{idx}]")
            for idx, item in enumerate(value)
        ]
    if isinstance(value, dict):
        return {
            str(key): _replace_placeholders(item, variables, path=f"{path}.{key}")
            for key, item in value.items()
        }
    return value


def _load_toml(path: Path, variables: dict[str, str]) -> dict[str, Any]:
    try:
        with path.open("rb") as f:
            raw = tomllib.load(f)
    except OSError as exc:
        raise EnvironmentStateProfileError(f"could not read {path}: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise EnvironmentStateProfileError(f"invalid TOML in {path}: {exc}") from exc
    return cast(dict[str, Any], _replace_placeholders(raw, variables, path=str(path)))


def _as_dict(value: object, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EnvironmentStateProfileError(f"{field} must be a table")
    return dict(value)


def _as_list(value: object, field: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise EnvironmentStateProfileError(f"{field} must be an array of tables")
    out: list[dict[str, Any]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            raise EnvironmentStateProfileError(f"{field}[{idx}] must be a table")
        out.append(dict(item))
    return out


def _as_string_list(value: object, field: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise EnvironmentStateProfileError(f"{field} must be an array")
    out: list[str] = []
    for idx, item in enumerate(value):
        if not isinstance(item, str):
            raise EnvironmentStateProfileError(f"{field}[{idx}] must be a string")
        out.append(item)
    return out


def _args_include_pool_name(args: list[str], pool_name: str) -> bool:
    for idx, arg in enumerate(args):
        if arg == "--pool-name" and idx + 1 < len(args) and args[idx + 1] == pool_name:
            return True
        if arg == f"--pool-name={pool_name}":
            return True
    return False


def _normalize_autoscaler_policy(
    item: dict[str, Any],
    *,
    environment: str,
    index: int,
) -> dict[str, Any]:
    field = f"worker_pool_autoscaler_policies[{index}]"
    pool_name = _clean_nonempty(item.get("pool_name"), f"{field}.pool_name")
    actuator = _clean_nonempty(item.get("actuator"), f"{field}.actuator")
    if "max_slots" not in item:
        raise EnvironmentStateProfileError(f"{field}.max_slots is required")
    payload = dict(_AUTOSCALER_DEFAULTS)
    payload.update(item)
    payload["environment"] = environment
    payload["pool_name"] = pool_name
    payload["actuator"] = actuator
    payload["actuator_config"] = _as_dict(
        payload.get("actuator_config", {}),
        f"{field}.actuator_config",
    )
    if payload.get("disabled_reason") is None and "disabled_reason" not in item:
        payload.pop("disabled_reason", None)
    return payload


def _normalize_gb10_desired_state(
    item: dict[str, Any],
    *,
    environment: str,
    index: int,
) -> dict[str, Any]:
    field = f"gb10_worker_pool_desired_states[{index}]"
    pool_name = _clean_nonempty(item.get("pool_name"), f"{field}.pool_name")
    image_tag = _clean_nonempty(item.get("image_tag"), f"{field}.image_tag")
    env_config_version = _clean_nonempty(
        item.get("env_config_version"),
        f"{field}.env_config_version",
    )
    if "max_concurrent" not in item:
        raise EnvironmentStateProfileError(f"{field}.max_concurrent is required")
    payload = dict(_GB10_DEFAULTS)
    payload.update(item)
    payload["environment"] = environment
    payload["pool_name"] = pool_name
    payload["image_tag"] = image_tag
    payload["env_config_version"] = env_config_version
    payload["host_intents"] = _as_dict(
        payload.get("host_intents", {}),
        f"{field}.host_intents",
    )
    payload["rollout_policy"] = _as_dict(
        payload.get("rollout_policy", {}),
        f"{field}.rollout_policy",
    )
    payload["env"] = _as_dict(payload.get("env", {}), f"{field}.env")
    return payload


def _normalize_external_slurm_autoscaler_supervisor(
    item: dict[str, Any],
    *,
    environment: str,
    index: int,
) -> dict[str, Any]:
    field = f"external_slurm_autoscaler_supervisors[{index}]"
    payload = dict(_EXTERNAL_AUTOSCALER_SUPERVISOR_DEFAULTS)
    payload.update(item)
    name = _clean_nonempty(payload.get("name"), f"{field}.name")
    pool_name = _clean_nonempty(payload.get("pool_name"), f"{field}.pool_name")
    args = _as_string_list(payload.get("args"), f"{field}.args")
    if not _args_include_pool_name(args, pool_name):
        raise EnvironmentStateProfileError(
            f"{field}.args must include --pool-name {pool_name}",
        )
    normalized = {
        "environment": environment,
        "name": name,
        "pool_name": pool_name,
        "service_name": _clean_nonempty(
            payload.get("service_name"),
            f"{field}.service_name",
        ),
        "timer_name": _clean_nonempty(
            payload.get("timer_name"),
            f"{field}.timer_name",
        ),
        "working_directory": _clean_nonempty(
            payload.get("working_directory"),
            f"{field}.working_directory",
        ),
        "python_path": _clean_nonempty(
            payload.get("python_path"),
            f"{field}.python_path",
        ),
        "script_path": _clean_nonempty(
            payload.get("script_path"),
            f"{field}.script_path",
        ),
        "args": args,
        "requires": _as_string_list(payload.get("requires"), f"{field}.requires"),
        "timer_on_boot_sec": _clean_nonempty(
            payload.get("timer_on_boot_sec"),
            f"{field}.timer_on_boot_sec",
        ),
        "timer_on_unit_active_sec": _clean_nonempty(
            payload.get("timer_on_unit_active_sec"),
            f"{field}.timer_on_unit_active_sec",
        ),
        "timer_accuracy_sec": _clean_nonempty(
            payload.get("timer_accuracy_sec"),
            f"{field}.timer_accuracy_sec",
        ),
        "enabled": bool(payload.get("enabled", True)),
        "active": bool(payload.get("active", True)),
    }
    return normalized


def load_environment_state_profile(
    path: Path | str,
    *,
    variables: dict[str, str] | None = None,
    expected_environment: str | None = None,
) -> EnvironmentStateProfile:
    profile_path = Path(path)
    raw = _load_toml(profile_path, dict(variables or {}))
    environment = _clean_nonempty(raw.get("environment"), "environment")
    if expected_environment is not None and environment != expected_environment:
        raise EnvironmentStateProfileError(
            f"profile environment {environment!r} does not match "
            f"--environment {expected_environment!r}",
        )
    control_plane_environment = _clean_nonempty(
        raw.get("control_plane_environment", environment),
        "control_plane_environment",
    )

    autoscaler_policies = [
        _normalize_autoscaler_policy(
            item,
            environment=control_plane_environment,
            index=idx,
        )
        for idx, item in enumerate(
            _as_list(
                raw.get("worker_pool_autoscaler_policies"),
                "worker_pool_autoscaler_policies",
            ),
        )
    ]
    gb10_desired_states = [
        _normalize_gb10_desired_state(
            item,
            environment=control_plane_environment,
            index=idx,
        )
        for idx, item in enumerate(
            _as_list(
                raw.get("gb10_worker_pool_desired_states"),
                "gb10_worker_pool_desired_states",
            ),
        )
    ]
    catalog = raw.get("catalog_provisioning", {})
    external_slurm_runner_prerequisites = raw.get(
        "external_slurm_runner_prerequisites",
        {},
    )
    external_slurm_autoscaler_supervisors = [
        _normalize_external_slurm_autoscaler_supervisor(
            item,
            environment=environment,
            index=idx,
        )
        for idx, item in enumerate(
            _as_list(
                raw.get("external_slurm_autoscaler_supervisors"),
                "external_slurm_autoscaler_supervisors",
            ),
        )
    ]
    return EnvironmentStateProfile(
        environment=environment,
        control_plane_environment=control_plane_environment,
        autoscaler_policies=autoscaler_policies,
        gb10_desired_states=gb10_desired_states,
        catalog_provisioning=_as_dict(catalog, "catalog_provisioning"),
        external_slurm_runner_prerequisites=_as_dict(
            external_slurm_runner_prerequisites,
            "external_slurm_runner_prerequisites",
        ),
        external_slurm_autoscaler_supervisors=(
            external_slurm_autoscaler_supervisors
        ),
    )


def _index_live(rows: object) -> dict[tuple[str, str], dict[str, Any]]:
    if not isinstance(rows, list):
        return {}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        environment = row.get("environment")
        pool_name = row.get("pool_name")
        if isinstance(environment, str) and isinstance(pool_name, str):
            out[(environment, pool_name)] = dict(row)
    return out


def _append_field_drift(
    drift: list[StateDrift],
    *,
    prefix: str,
    desired: dict[str, Any],
    live: dict[str, Any],
    fields: tuple[str, ...],
    live_defaults: dict[str, Any],
) -> None:
    for field in fields:
        desired_value = desired.get(field)
        live_value = live.get(field, live_defaults.get(field))
        if desired_value != live_value:
            drift.append(
                StateDrift(
                    path=f"{prefix}.{field}",
                    desired=desired_value,
                    live=live_value,
                ),
            )


def _external_slurm_policies(
    profile: EnvironmentStateProfile,
) -> list[dict[str, Any]]:
    return [
        policy
        for policy in profile.autoscaler_policies
        if policy.get("actuator") == "slurm"
        and policy.get("actuator_config", {}).get("external_runner") is True
    ]


_TERMINAL_SLURM_JOB_STATES = {"completed", "failed", "cancelled", "stale"}


def _append_active_slurm_job_drift(
    drift: list[StateDrift],
    *,
    desired_policies: dict[tuple[str, str], dict[str, Any]],
    jobs: object,
    expected_worker_token: str | None = None,
) -> None:
    if not isinstance(jobs, list):
        return
    for job in jobs:
        if not isinstance(job, dict):
            continue
        environment = job.get("environment")
        pool_name = job.get("pool_name")
        if not isinstance(environment, str) or not isinstance(pool_name, str):
            continue
        desired = desired_policies.get((environment, pool_name))
        if desired is None:
            continue
        state = str(job.get("state") or "").strip().lower()
        if state in _TERMINAL_SLURM_JOB_STATES:
            continue
        redacted_env = job.get("redacted_env")
        if not isinstance(redacted_env, dict):
            continue
        actuator_config = desired.get("actuator_config", {})
        if not isinstance(actuator_config, dict):
            continue
        job_id = str(job.get("job_id") or job.get("id") or "unknown")
        prefix = f"slurm_worker_jobs[{environment}/{pool_name}/{job_id}]"
        expected = {
            "LOOM_REMOTE_WORKER_ENV_FILE": actuator_config.get("env_file"),
            "LOOM_REMOTE_WORKER_REPO_DIR": actuator_config.get("repo_dir"),
        }
        for env_key, desired_value in expected.items():
            if desired_value is None:
                continue
            live_value = redacted_env.get(env_key)
            if live_value != desired_value:
                drift.append(
                    StateDrift(
                        path=f"{prefix}.{env_key}",
                        desired=desired_value,
                        live=live_value,
                    ),
                )
        if expected_worker_token:
            desired_fingerprint = worker_token_fingerprint(expected_worker_token)
            live_fingerprint = redacted_env.get(WORKER_AUTH_FINGERPRINT_ENV_KEY)
            if live_fingerprint != desired_fingerprint:
                drift.append(
                    StateDrift(
                        path=f"{prefix}.{WORKER_AUTH_FINGERPRINT_ENV_KEY}",
                        desired=desired_fingerprint,
                        live=live_fingerprint or "missing",
                    ),
                )


def diff_environment_state(
    profile: EnvironmentStateProfile,
    live: dict[str, Any],
    *,
    expected_worker_token: str | None = None,
) -> list[StateDrift]:
    drift: list[StateDrift] = []
    live_autoscalers = _index_live(
        _as_dict(live.get("autoscaler_status", {}), "autoscaler_status").get(
            "policies",
            [],
        ),
    )
    live_gb10 = _index_live(
        _as_dict(live.get("gb10_status", {}), "gb10_status").get(
            "desired_states",
            [],
        ),
    )

    for desired in profile.autoscaler_policies:
        key = (desired["environment"], desired["pool_name"])
        prefix = f"worker_pool_autoscaler_policies[{key[0]}/{key[1]}]"
        live_row = live_autoscalers.get(key)
        if live_row is None:
            drift.append(StateDrift(path=prefix, desired=desired, live=None))
            continue
        _append_field_drift(
            drift,
            prefix=prefix,
            desired=desired,
            live=live_row,
            fields=_AUTOSCALER_COMPARE_FIELDS,
            live_defaults=_AUTOSCALER_DEFAULTS,
        )

    for desired in profile.gb10_desired_states:
        key = (desired["environment"], desired["pool_name"])
        prefix = f"gb10_worker_pool_desired_states[{key[0]}/{key[1]}]"
        live_row = live_gb10.get(key)
        if live_row is None:
            drift.append(StateDrift(path=prefix, desired=desired, live=None))
            continue
        _append_field_drift(
            drift,
            prefix=prefix,
            desired=desired,
            live=live_row,
            fields=_GB10_COMPARE_FIELDS,
            live_defaults=_GB10_DEFAULTS,
        )
    _append_active_slurm_job_drift(
        drift,
        desired_policies={
            (policy["environment"], policy["pool_name"]): policy
            for policy in _external_slurm_policies(profile)
        },
        jobs=_as_dict(live.get("slurm_status", {}), "slurm_status").get(
            "jobs",
            [],
        ),
        expected_worker_token=expected_worker_token,
    )
    return drift


SubprocessRunner = Callable[[list[str]], tuple[int, str, str]]


def _default_subprocess_runner(command: list[str]) -> tuple[int, str, str]:
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.returncode, completed.stdout, completed.stderr


def _expected_git_prefix(expected_ref: str) -> str:
    match = re.search(r"([0-9a-f]{7,40})$", expected_ref.strip())
    return match.group(1) if match else expected_ref.strip()


def diff_external_slurm_runner_prerequisites(
    profile: EnvironmentStateProfile,
    *,
    runner: SubprocessRunner | None = None,
    expected_worker_token: str | None = None,
) -> list[StateDrift]:
    settings = profile.external_slurm_runner_prerequisites
    if not settings:
        return []

    command_runner = runner or _default_subprocess_runner
    expected_repo_ref = settings.get("expected_repo_ref")
    require_clean_repo = bool(settings.get("require_clean_repo", False))
    require_worker_token_parity = bool(
        settings.get("require_worker_token_parity", False),
    )
    worker_token_env_key = str(
        settings.get("worker_token_env_key") or DEFAULT_WORKER_TOKEN_ENV_KEY,
    )
    configured_pools = settings.get("pools")
    checked_pools = set(configured_pools) if isinstance(configured_pools, list) else None
    drift: list[StateDrift] = []
    for policy in _external_slurm_policies(profile):
        if checked_pools is not None and policy["pool_name"] not in checked_pools:
            continue
        key = (policy["environment"], policy["pool_name"])
        prefix = f"external_slurm_runner_prerequisites[{key[0]}/{key[1]}]"
        actuator_config = policy.get("actuator_config", {})
        if not isinstance(actuator_config, dict):
            continue

        env_file = actuator_config.get("env_file")
        if isinstance(env_file, str) and not Path(env_file).is_file():
            drift.append(
                StateDrift(
                    path=f"{prefix}.env_file",
                    desired=env_file,
                    live="missing",
                ),
            )
        elif require_worker_token_parity and isinstance(env_file, str):
            if not expected_worker_token:
                drift.append(
                    StateDrift(
                        path=f"{prefix}.worker_token_fingerprint",
                        desired="active worker token fingerprint",
                        live="missing --worker-token",
                    ),
                )
            else:
                desired_fingerprint = worker_token_fingerprint(expected_worker_token)
                try:
                    live_worker_token = read_env_file_value(
                        Path(env_file),
                        worker_token_env_key,
                    )
                except OSError as exc:
                    drift.append(
                        StateDrift(
                            path=f"{prefix}.worker_token_fingerprint",
                            desired=desired_fingerprint,
                            live=f"unreadable env file: {exc}",
                        ),
                    )
                else:
                    live_fingerprint = (
                        worker_token_fingerprint(live_worker_token)
                        if live_worker_token
                        else f"missing {worker_token_env_key}"
                    )
                    if live_fingerprint != desired_fingerprint:
                        drift.append(
                            StateDrift(
                                path=f"{prefix}.worker_token_fingerprint",
                                desired=desired_fingerprint,
                                live=live_fingerprint,
                            ),
                        )

        repo_dir = actuator_config.get("repo_dir")
        if not isinstance(repo_dir, str):
            continue
        repo_path = Path(repo_dir)
        if not repo_path.is_dir():
            drift.append(
                StateDrift(
                    path=f"{prefix}.repo_dir",
                    desired=repo_dir,
                    live="missing",
                ),
            )
            continue

        if isinstance(expected_repo_ref, str) and expected_repo_ref.strip():
            rc, stdout, stderr = command_runner(["git", "-C", repo_dir, "rev-parse", "HEAD"])
            live_head = stdout.strip()
            if rc != 0:
                drift.append(
                    StateDrift(
                        path=f"{prefix}.repo_dir.git_head",
                        desired=expected_repo_ref,
                        live=(stderr or stdout).strip() or f"git exited {rc}",
                    ),
                )
            elif not live_head.startswith(_expected_git_prefix(expected_repo_ref)):
                drift.append(
                    StateDrift(
                        path=f"{prefix}.repo_dir.git_head",
                        desired=expected_repo_ref,
                        live=live_head,
                    ),
                )

        if require_clean_repo:
            rc, stdout, stderr = command_runner(
                ["git", "-C", repo_dir, "status", "--short", "--untracked-files=no"],
            )
            live_status = stdout.strip()
            if rc != 0 or live_status:
                drift.append(
                    StateDrift(
                        path=f"{prefix}.repo_dir.git_status",
                        desired="clean",
                        live=live_status or stderr.strip() or f"git exited {rc}",
                    ),
                )

    return drift


def _quote_command(parts: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def render_external_slurm_autoscaler_service(supervisor: dict[str, Any]) -> str:
    requires = supervisor.get("requires", [])
    after = " ".join(requires)
    command = [
        str(supervisor["python_path"]),
        str(supervisor["script_path"]),
        *[str(arg) for arg in supervisor.get("args", [])],
    ]
    return "\n".join(
        [
            "[Unit]",
            f"Description=Loom {supervisor['pool_name']} external worker-pool autoscaler reconcile",
            f"After={after}",
            f"Wants={after}",
            "",
            "[Service]",
            "Type=oneshot",
            f"WorkingDirectory={supervisor['working_directory']}",
            f"Environment=PYTHONPATH={supervisor['working_directory']}/src",
            f"ExecStart={_quote_command(command)}",
        ],
    )


def render_external_slurm_autoscaler_timer(supervisor: dict[str, Any]) -> str:
    return "\n".join(
        [
            "[Unit]",
            f"Description=Run Loom {supervisor['pool_name']} external autoscaler reconcile",
            "",
            "[Timer]",
            f"OnBootSec={supervisor['timer_on_boot_sec']}",
            f"OnUnitActiveSec={supervisor['timer_on_unit_active_sec']}",
            f"AccuracySec={supervisor['timer_accuracy_sec']}",
            f"Unit={supervisor['service_name']}",
            "",
            "[Install]",
            "WantedBy=timers.target",
        ],
    )


def _unit_payload(text: str) -> str:
    lines = [
        line.rstrip()
        for line in text.splitlines()
        if not line.startswith("# ")
    ]
    return "\n".join(lines).strip()


def _append_unit_drift(
    drift: list[StateDrift],
    *,
    path: str,
    desired: str,
    command: list[str],
    runner: SubprocessRunner,
) -> None:
    rc, stdout, stderr = runner(command)
    live = _unit_payload(stdout)
    if rc != 0:
        live = (stderr or stdout).strip() or "missing"
    if _unit_payload(desired) != live:
        drift.append(StateDrift(path=path, desired=desired, live=live))


def diff_external_slurm_autoscaler_supervisors(
    profile: EnvironmentStateProfile,
    *,
    runner: SubprocessRunner | None = None,
) -> list[StateDrift]:
    command_runner = runner or _default_subprocess_runner
    drift: list[StateDrift] = []
    for supervisor in profile.external_slurm_autoscaler_supervisors:
        prefix = (
            "external_slurm_autoscaler_supervisors"
            f"[{supervisor['environment']}/{supervisor['pool_name']}]"
        )
        _append_unit_drift(
            drift,
            path=f"{prefix}.service_unit",
            desired=render_external_slurm_autoscaler_service(supervisor),
            command=["systemctl", "--user", "cat", supervisor["service_name"]],
            runner=command_runner,
        )
        _append_unit_drift(
            drift,
            path=f"{prefix}.timer_unit",
            desired=render_external_slurm_autoscaler_timer(supervisor),
            command=["systemctl", "--user", "cat", supervisor["timer_name"]],
            runner=command_runner,
        )
        if supervisor["enabled"]:
            rc, stdout, stderr = command_runner(
                ["systemctl", "--user", "is-enabled", supervisor["timer_name"]],
            )
            live = stdout.strip() if rc == 0 else (stderr or stdout).strip()
            if live != "enabled":
                drift.append(
                    StateDrift(
                        path=f"{prefix}.timer_enabled",
                        desired="enabled",
                        live=live or f"systemctl exited {rc}",
                    ),
                )
        if supervisor["active"]:
            rc, stdout, stderr = command_runner(
                ["systemctl", "--user", "is-active", supervisor["timer_name"]],
            )
            live = stdout.strip() if rc == 0 else (stdout or stderr).strip()
            if live != "active":
                drift.append(
                    StateDrift(
                        path=f"{prefix}.timer_active",
                        desired="active",
                        live=live or f"systemctl exited {rc}",
                    ),
                )
    return drift


def _run_supervisor_command(
    command: list[str],
    *,
    runner: SubprocessRunner,
) -> None:
    rc, stdout, stderr = runner(command)
    if rc != 0:
        message = (stderr or stdout).strip() or f"command exited {rc}"
        raise EnvironmentStateProfileError(f"{' '.join(command)} failed: {message}")


def apply_external_slurm_autoscaler_supervisors(
    profile: EnvironmentStateProfile,
    *,
    unit_dir: Path | None = None,
    runner: SubprocessRunner | None = None,
) -> list[dict[str, str]]:
    if not profile.external_slurm_autoscaler_supervisors:
        return []

    command_runner = runner or _default_subprocess_runner
    target_dir = unit_dir or (Path.home() / ".config" / "systemd" / "user")
    target_dir.mkdir(parents=True, exist_ok=True)
    applied: list[dict[str, str]] = []
    for supervisor in profile.external_slurm_autoscaler_supervisors:
        service_path = target_dir / supervisor["service_name"]
        timer_path = target_dir / supervisor["timer_name"]
        service_path.write_text(
            render_external_slurm_autoscaler_service(supervisor) + "\n",
            encoding="utf-8",
        )
        timer_path.write_text(
            render_external_slurm_autoscaler_timer(supervisor) + "\n",
            encoding="utf-8",
        )
        applied.append(
            {
                "kind": "external_slurm_autoscaler_supervisor",
                "service": supervisor["service_name"],
                "timer": supervisor["timer_name"],
            },
        )

    _run_supervisor_command(
        ["systemctl", "--user", "daemon-reload"],
        runner=command_runner,
    )
    for supervisor in profile.external_slurm_autoscaler_supervisors:
        timer_name = supervisor["timer_name"]
        if supervisor["enabled"]:
            _run_supervisor_command(
                ["systemctl", "--user", "enable", "--now", timer_name],
                runner=command_runner,
            )
        if supervisor["active"]:
            _run_supervisor_command(
                ["systemctl", "--user", "restart", timer_name],
                runner=command_runner,
            )
    return applied


def autoscaler_policy_payload(policy: dict[str, Any]) -> dict[str, Any]:
    return {field: policy.get(field) for field in _AUTOSCALER_COMPARE_FIELDS}


def gb10_desired_state_payload(state: dict[str, Any]) -> dict[str, Any]:
    return {field: state.get(field) for field in _GB10_COMPARE_FIELDS}
