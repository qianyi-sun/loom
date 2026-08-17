"""Versioned environment desired-state profiles for deploy rollouts."""

from __future__ import annotations

import re
import shlex
import socket
import subprocess
import tomllib
import unicodedata
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
_SYSTEMD_UNIT_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.@:-]{0,127}\Z")
_SYSTEMD_SECONDS_RE = re.compile(r"[1-9][0-9]{0,4}\Z")

# Matches the git-SHA suffix in a release tag like `staging-c72f50d`.
# Kept in sync with the copy in `loom_cli.admin_cmd` — both derive the
# expected node-agent source_git_commit prefix from the same tag shape.
# See #356 for why the drift check must include per-node source_git_commit,
# not just DB-side image_tag / env_config_version convergence.
_RELEASE_TAG_SHA_RE = re.compile(r"(?:^|[-_])([0-9a-f]{7,40})$")


def _release_source_prefix(image_tag: Any) -> str | None:
    if not isinstance(image_tag, str) or not image_tag:
        return None
    match = _RELEASE_TAG_SHA_RE.search(image_tag)
    return match.group(1) if match else None


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
    "source_git_commit": None,
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
    "source_git_commit",
    "target_slots",
    "host_intents",
    "rollout_policy",
    "env",
    "force",
)

_AUTOSCALER_HARD_BLOCKERS = frozenset(
    {
        "no_safe_slurm_nodes",
        "missing_slurm_allowed_nodes",
        "slurm_autoscaler_config_invalid",
        "release_state_drift",
    },
)

_EXTERNAL_AUTOSCALER_SUPERVISOR_DEFAULTS: dict[str, Any] = {
    "args": [],
    "execution_host": "local",
    "requires": ["network-online.target"],
    "timer_on_boot_sec": "45",
    "timer_on_unit_active_sec": "30",
    "timer_accuracy_sec": "5",
    "service_timeout_sec": "180",
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
    task_image_builder_policies: list[dict[str, Any]]
    gb10_desired_states: list[dict[str, Any]]
    catalog_provisioning: dict[str, Any]
    rate_card_sync: dict[str, Any]
    hosted_provider_pricing_defaults: list[dict[str, Any]]
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


def _systemd_unit_name(value: object, field: str, *, suffix: str) -> str:
    cleaned = _clean_nonempty(value, field)
    if (
        _SYSTEMD_UNIT_RE.fullmatch(cleaned) is None
        or not cleaned.endswith(suffix)
        or "/" in cleaned
        or "\\" in cleaned
    ):
        raise EnvironmentStateProfileError(
            f"{field} must be one safe {suffix} unit basename",
        )
    return cleaned


def _systemd_seconds(value: object, field: str, *, maximum: int) -> str:
    cleaned = _clean_nonempty(value, field)
    if _SYSTEMD_SECONDS_RE.fullmatch(cleaned) is None or int(cleaned) > maximum:
        raise EnvironmentStateProfileError(
            f"{field} must be whole seconds in 1..{maximum}",
        )
    return cleaned


def _systemd_text(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise EnvironmentStateProfileError(f"{field} must be a string")
    cleaned = value.strip()
    if (
        not cleaned
        or cleaned != value
        or any(unicodedata.category(character) == "Cc" for character in value)
    ):
        raise EnvironmentStateProfileError(
            f"{field} must be a non-empty control-free string",
        )
    return cleaned


def _systemd_string_list(value: object, field: str) -> list[str]:
    items = _as_string_list(value, field)
    return [_systemd_text(item, f"{field}[{index}]") for index, item in enumerate(items)]


def _systemd_dependency_list(value: object, field: str) -> list[str]:
    items = _systemd_string_list(value, field)
    for index, item in enumerate(items):
        if _SYSTEMD_UNIT_RE.fullmatch(item) is None or "/" in item or "\\" in item:
            raise EnvironmentStateProfileError(
                f"{field}[{index}] must be one safe systemd unit basename",
            )
    return items


def _strict_boolean(value: object, field: str) -> bool:
    if type(value) is not bool:
        raise EnvironmentStateProfileError(f"{field} must be a boolean")
    return value


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


def _args_exact_option_values(args: list[str], option: str) -> tuple[str, ...]:
    values: list[str] = []
    for idx, arg in enumerate(args):
        if arg == option and idx + 1 < len(args):
            values.append(args[idx + 1])
        elif arg.startswith(f"{option}="):
            values.append(arg.split("=", 1)[1])
    return tuple(values)


def _args_db_local_port(args: list[str]) -> str | None:
    for idx, arg in enumerate(args):
        if arg == "--db-local-port" and idx + 1 < len(args):
            return args[idx + 1]
        if arg.startswith("--db-local-port="):
            return arg.split("=", 1)[1]
    return None


def _validate_supervisor_collisions(
    supervisors: list[dict[str, Any]],
) -> None:
    """Reject cross-entry supervisor collisions within a single profile (#875).

    Two supervisors on the same host that share a systemd unit name would
    overwrite each other's unit file, and a shared --db-local-port would
    make their port-forwards fight for the same local socket. Enforce
    uniqueness of the identity fields plus the DB tunnel port so a mistyped
    copy-paste in the env-state TOML fails loudly at load time.
    """
    for field in ("name", "pool_name", "service_name", "timer_name"):
        seen: set[str] = set()
        for supervisor in supervisors:
            value = str(supervisor[field])
            if value in seen:
                raise EnvironmentStateProfileError(
                    f"external_slurm_autoscaler_supervisors: duplicate {field} {value!r}",
                )
            seen.add(value)
    seen_ports: set[str] = set()
    for supervisor in supervisors:
        port = _args_db_local_port(supervisor.get("args", []))
        if port is None:
            continue
        if port in seen_ports:
            raise EnvironmentStateProfileError(
                f"external_slurm_autoscaler_supervisors: duplicate --db-local-port {port!r}",
            )
        seen_ports.add(port)


def _validate_task_image_builder_contract(
    policies: list[dict[str, Any]],
    *,
    trial_policies: list[dict[str, Any]],
    supervisors: list[dict[str, Any]],
) -> None:
    pool_names = [str(policy["pool_name"]) for policy in policies]
    cpu_arches = [str(policy["cpu_arch"]) for policy in policies]
    if len(pool_names) != len(set(pool_names)):
        raise EnvironmentStateProfileError(
            "task_image_builder_policies contains duplicate pool_name values",
        )
    if len(cpu_arches) != len(set(cpu_arches)):
        raise EnvironmentStateProfileError(
            "task_image_builder_policies contains duplicate cpu_arch values",
        )
    trial_pool_names = {str(policy["pool_name"]) for policy in trial_policies}
    overlap = sorted(set(pool_names) & trial_pool_names)
    if overlap:
        raise EnvironmentStateProfileError(
            "task-image builder pool identities overlap trial pools: " + ", ".join(overlap),
        )
    for policy in policies:
        pool_name = str(policy["pool_name"])
        matches = [row for row in supervisors if row["pool_name"] == pool_name]
        if len(matches) != 1:
            raise EnvironmentStateProfileError(
                f"task-image builder pool {pool_name!r} requires exactly one supervisor",
            )
        supervisor = matches[0]
        enabled = bool(policy["enabled"])
        if supervisor["enabled"] is not enabled or supervisor["active"] is not enabled:
            raise EnvironmentStateProfileError(
                f"task-image builder pool {pool_name!r} policy and supervisor activation differ",
            )
        if not str(supervisor["script_path"]).endswith(
            "/scripts/ops/task_image_builder_autoscaler_external_once.py"
        ):
            raise EnvironmentStateProfileError(
                f"task-image builder pool {pool_name!r} uses the wrong supervisor entrypoint",
            )


def staging_gb10_external_activation_blockers(
    *,
    environment: object,
    autoscaler_policies: object,
    prerequisites: object,
    supervisors: object,
) -> tuple[str, ...]:
    """Return fail-closed blockers for staging's external GB10 Slurm path."""

    if environment != "staging":
        return ()
    policy_rows = autoscaler_policies if isinstance(autoscaler_policies, list) else []
    gb10_policies: list[dict[str, Any]] = []
    for raw in policy_rows:
        if not isinstance(raw, dict):
            continue
        actuator_config = raw.get("actuator_config")
        external_runner = (
            actuator_config.get("external_runner")
            if isinstance(actuator_config, dict)
            else raw.get("external_runner")
        )
        if (
            raw.get("pool_name") == "gb10"
            and raw.get("actuator") == "slurm"
            and external_runner is True
        ):
            gb10_policies.append(raw)
    blockers: list[str] = []
    for policy in gb10_policies:
        enabled = policy.get("enabled")
        if type(enabled) is not bool:
            blockers.append("gb10_policy_enabled_invalid")
        elif enabled is False:
            disabled_reason = policy.get("disabled_reason")
            if not isinstance(disabled_reason, str) or not disabled_reason.strip():
                blockers.append("gb10_policy_disabled_reason_missing")

    prereq = prerequisites if isinstance(prerequisites, dict) else {}
    candidate_attestation_fields = {
        "service_identity",
        "allocation_attestation",
        "authority_path",
        "authority_digest",
        "authority_passed",
    }
    if candidate_attestation_fields & set(prereq):
        blockers.append("candidate_external_slurm_self_attestation_forbidden")
    pools = prereq.get("pools")
    materializes_gb10 = prereq.get("materialize") is True and (
        not isinstance(pools, list) or not pools or "gb10" in pools
    )
    supervisor_rows = supervisors if isinstance(supervisors, list) else []
    gb10_supervisors = [
        raw for raw in supervisor_rows if isinstance(raw, dict) and raw.get("pool_name") == "gb10"
    ]
    supervisor_active = any(
        raw.get("enabled") is not False or raw.get("active") is not False
        for raw in gb10_supervisors
    )
    activation_requested = (
        any(policy.get("enabled") is not False for policy in gb10_policies)
        or materializes_gb10
        or supervisor_active
    )
    if not activation_requested:
        return tuple(sorted(set(blockers)))
    if prereq.get("require_external_allocation_authority") is not True:
        blockers.append("external_slurm_allocation_authority_requirement_missing")
    if not materializes_gb10:
        blockers.append("external_slurm_gb10_materialization_required")
    if len(gb10_supervisors) != 1:
        blockers.append("external_slurm_gb10_supervisor_count_invalid")
    elif (
        gb10_supervisors[0].get("enabled") is not True
        or gb10_supervisors[0].get("active") is not True
    ):
        blockers.append("external_slurm_gb10_supervisor_activation_incomplete")

    return tuple(sorted(set(blockers)))


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


def _normalize_task_image_builder_policy(
    item: dict[str, Any],
    *,
    environment: str,
    index: int,
) -> dict[str, Any]:
    field = f"task_image_builder_policies[{index}]"
    required = (
        "pool_name",
        "slurm_cluster_id",
        "cpu_arch",
        "allowed_nodes",
        "env_file",
        "env_template_file",
        "builder_token_file",
        "repo_dir",
        "registry_docker_config_dir",
        "partition",
        "time_limit",
        "requested_cpus",
        "requested_memory_mib",
        "max_jobs",
        "pending_job_cap",
    )
    missing = [name for name in required if name not in item]
    if missing:
        raise EnvironmentStateProfileError(
            f"{field} is missing required fields: {', '.join(missing)}",
        )
    payload = {
        "environment": environment,
        "enabled": False,
        "exclusive": True,
        "requested_concurrency": 1,
        "idle_exit_after_seconds": 120,
        "sbatch_path": "sbatch",
        "squeue_path": "squeue",
        "sacct_path": "sacct",
        "scancel_path": "scancel",
        "command_timeout_seconds": 20.0,
        "slurm_account": "",
        "slurm_qos": "",
        "slurm_reservation": "",
        "job_output_dir": "",
        "activation_blockers": [],
        **item,
    }
    payload["pool_name"] = _clean_nonempty(payload["pool_name"], f"{field}.pool_name")
    payload["slurm_cluster_id"] = _clean_nonempty(
        payload["slurm_cluster_id"], f"{field}.slurm_cluster_id"
    )
    payload["cpu_arch"] = _clean_nonempty(payload["cpu_arch"], f"{field}.cpu_arch")
    expected_cluster = {"x86_64": "oldlab", "arm64": "gb10"}.get(payload["cpu_arch"])
    if expected_cluster is None or payload["slurm_cluster_id"] != expected_cluster:
        raise EnvironmentStateProfileError(
            f"{field} native architecture does not match its Slurm cluster",
        )
    payload["enabled"] = _strict_boolean(payload["enabled"], f"{field}.enabled")
    payload["exclusive"] = _strict_boolean(payload["exclusive"], f"{field}.exclusive")
    if not payload["exclusive"]:
        raise EnvironmentStateProfileError(f"{field}.exclusive must be true")
    nodes = _as_string_list(payload["allowed_nodes"], f"{field}.allowed_nodes")
    if not nodes or any(not node.strip() for node in nodes):
        raise EnvironmentStateProfileError(f"{field}.allowed_nodes must not be empty")
    payload["allowed_nodes"] = nodes
    blockers = _as_string_list(payload["activation_blockers"], f"{field}.activation_blockers")
    payload["activation_blockers"] = blockers
    if payload["enabled"] and blockers:
        raise EnvironmentStateProfileError(
            f"{field} cannot be enabled while activation blockers remain",
        )
    for name in (
        "env_file",
        "env_template_file",
        "builder_token_file",
        "repo_dir",
        "registry_docker_config_dir",
        "partition",
        "time_limit",
        "sbatch_path",
        "squeue_path",
        "sacct_path",
        "scancel_path",
    ):
        payload[name] = _clean_nonempty(payload[name], f"{field}.{name}")
    for name in ("slurm_account", "slurm_qos", "slurm_reservation", "job_output_dir"):
        payload[name] = str(payload[name]).strip()
    if payload["enabled"]:
        for name in (
            "slurm_account",
            "slurm_qos",
            "slurm_reservation",
            "job_output_dir",
        ):
            if not payload[name]:
                raise EnvironmentStateProfileError(
                    f"{field}.{name} must be non-empty when enabled",
                )
    for name in (
        "requested_cpus",
        "requested_memory_mib",
        "requested_concurrency",
        "max_jobs",
        "pending_job_cap",
        "idle_exit_after_seconds",
    ):
        value = payload[name]
        if type(value) is not int or value <= 0:
            raise EnvironmentStateProfileError(f"{field}.{name} must be a positive integer")
    if payload["requested_concurrency"] != 1:
        raise EnvironmentStateProfileError(
            f"{field}.requested_concurrency must equal one",
        )
    if payload["max_jobs"] > len(nodes):
        raise EnvironmentStateProfileError(
            f"{field}.max_jobs must not exceed allowed_nodes",
        )
    if payload["pending_job_cap"] > payload["max_jobs"]:
        raise EnvironmentStateProfileError(
            f"{field}.pending_job_cap must not exceed max_jobs",
        )
    timeout = payload["command_timeout_seconds"]
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
        raise EnvironmentStateProfileError(
            f"{field}.command_timeout_seconds must be positive",
        )
    payload["command_timeout_seconds"] = float(timeout)
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
    source_git_commit = item.get("source_git_commit")
    if "max_concurrent" not in item:
        raise EnvironmentStateProfileError(f"{field}.max_concurrent is required")
    payload = dict(_GB10_DEFAULTS)
    payload.update(item)
    payload["environment"] = environment
    payload["pool_name"] = pool_name
    payload["image_tag"] = image_tag
    payload["env_config_version"] = env_config_version
    payload["source_git_commit"] = (
        _clean_nonempty(source_git_commit, f"{field}.source_git_commit")
        if source_git_commit is not None
        else None
    )
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
    control_plane_environment: str,
    index: int,
) -> dict[str, Any]:
    field = f"external_slurm_autoscaler_supervisors[{index}]"
    payload = dict(_EXTERNAL_AUTOSCALER_SUPERVISOR_DEFAULTS)
    payload.update(item)
    name = _clean_nonempty(payload.get("name"), f"{field}.name")
    pool_name = _systemd_text(payload.get("pool_name"), f"{field}.pool_name")
    args = _systemd_string_list(payload.get("args"), f"{field}.args")
    if not _args_include_pool_name(args, pool_name):
        raise EnvironmentStateProfileError(
            f"{field}.args must include --pool-name {pool_name}",
        )
    if _args_exact_option_values(args, "--environment") != (control_plane_environment,):
        raise EnvironmentStateProfileError(
            f"{field}.args must include exactly one --environment {control_plane_environment}",
        )
    normalized = {
        "environment": environment,
        "control_plane_environment": control_plane_environment,
        "name": name,
        "pool_name": pool_name,
        "execution_host": _systemd_text(
            payload.get("execution_host"),
            f"{field}.execution_host",
        ),
        "service_name": _systemd_unit_name(
            payload.get("service_name"),
            f"{field}.service_name",
            suffix=".service",
        ),
        "timer_name": _systemd_unit_name(
            payload.get("timer_name"),
            f"{field}.timer_name",
            suffix=".timer",
        ),
        "working_directory": _systemd_text(
            payload.get("working_directory"),
            f"{field}.working_directory",
        ),
        "python_path": _systemd_text(
            payload.get("python_path"),
            f"{field}.python_path",
        ),
        "script_path": _systemd_text(
            payload.get("script_path"),
            f"{field}.script_path",
        ),
        "args": args,
        "requires": _systemd_dependency_list(
            payload.get("requires"),
            f"{field}.requires",
        ),
        "timer_on_boot_sec": _systemd_seconds(
            payload.get("timer_on_boot_sec"),
            f"{field}.timer_on_boot_sec",
            maximum=3600,
        ),
        "timer_on_unit_active_sec": _systemd_seconds(
            payload.get("timer_on_unit_active_sec"),
            f"{field}.timer_on_unit_active_sec",
            maximum=3600,
        ),
        "timer_accuracy_sec": _systemd_seconds(
            payload.get("timer_accuracy_sec"),
            f"{field}.timer_accuracy_sec",
            maximum=3600,
        ),
        "service_timeout_sec": _systemd_seconds(
            payload.get("service_timeout_sec"),
            f"{field}.service_timeout_sec",
            maximum=7200,
        ),
        "enabled": _strict_boolean(payload.get("enabled"), f"{field}.enabled"),
        "active": _strict_boolean(payload.get("active"), f"{field}.active"),
    }
    return normalized


def _normalize_hosted_provider_pricing_default(
    item: dict[str, Any],
    *,
    index: int,
) -> dict[str, Any]:
    field = f"hosted_provider_pricing_defaults[{index}]"
    name = _clean_nonempty(item.get("name"), f"{field}.name")
    pricing_source = _clean_nonempty(
        item.get("pricing_source"),
        f"{field}.pricing_source",
    )
    if pricing_source not in {"rate-card", "tokens-only"}:
        raise EnvironmentStateProfileError(
            f"{field}.pricing_source must be 'rate-card' or 'tokens-only'",
        )
    rate_card_provider = item.get("rate_card_provider")
    normalized: dict[str, Any] = {
        "name": name,
        "pricing_source": pricing_source,
        "required": bool(item.get("required", True)),
    }
    if rate_card_provider is not None:
        normalized["rate_card_provider"] = _clean_nonempty(
            rate_card_provider,
            f"{field}.rate_card_provider",
        )
    if pricing_source == "rate-card" and "rate_card_provider" not in normalized:
        raise EnvironmentStateProfileError(
            f"{field}.rate_card_provider is required when pricing_source='rate-card'",
        )
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
    task_image_builder_policies = [
        _normalize_task_image_builder_policy(
            item,
            environment=control_plane_environment,
            index=idx,
        )
        for idx, item in enumerate(
            _as_list(
                raw.get("task_image_builder_policies"),
                "task_image_builder_policies",
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
    rate_card_sync = raw.get("rate_card_sync", {})
    hosted_provider_pricing_defaults = [
        _normalize_hosted_provider_pricing_default(item, index=idx)
        for idx, item in enumerate(
            _as_list(
                raw.get("hosted_provider_pricing_defaults"),
                "hosted_provider_pricing_defaults",
            ),
        )
    ]
    external_slurm_runner_prerequisites = raw.get(
        "external_slurm_runner_prerequisites",
        {},
    )
    external_slurm_autoscaler_supervisors = [
        _normalize_external_slurm_autoscaler_supervisor(
            item,
            environment=environment,
            control_plane_environment=control_plane_environment,
            index=idx,
        )
        for idx, item in enumerate(
            _as_list(
                raw.get("external_slurm_autoscaler_supervisors"),
                "external_slurm_autoscaler_supervisors",
            ),
        )
    ]
    _validate_supervisor_collisions(external_slurm_autoscaler_supervisors)
    _validate_task_image_builder_contract(
        task_image_builder_policies,
        trial_policies=autoscaler_policies,
        supervisors=external_slurm_autoscaler_supervisors,
    )
    external_slurm_runner_prerequisites = _as_dict(
        external_slurm_runner_prerequisites,
        "external_slurm_runner_prerequisites",
    )
    blockers = staging_gb10_external_activation_blockers(
        environment=environment,
        autoscaler_policies=autoscaler_policies,
        prerequisites=external_slurm_runner_prerequisites,
        supervisors=external_slurm_autoscaler_supervisors,
    )
    if blockers:
        raise EnvironmentStateProfileError(
            "staging GB10 external Slurm acceptance is blocked: " + ", ".join(blockers),
        )
    return EnvironmentStateProfile(
        environment=environment,
        control_plane_environment=control_plane_environment,
        autoscaler_policies=autoscaler_policies,
        task_image_builder_policies=task_image_builder_policies,
        gb10_desired_states=gb10_desired_states,
        catalog_provisioning=_as_dict(catalog, "catalog_provisioning"),
        rate_card_sync=_as_dict(rate_card_sync, "rate_card_sync"),
        hosted_provider_pricing_defaults=hosted_provider_pricing_defaults,
        external_slurm_runner_prerequisites=external_slurm_runner_prerequisites,
        external_slurm_autoscaler_supervisors=(external_slurm_autoscaler_supervisors),
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


_GB10_NODE_SOURCE_DRIFT_IGNORED_INTENTS = frozenset(
    {"stopped", "draining", "drained", "unavailable"},
)
_GB10_NODE_SOURCE_DRIFT_IGNORED_APPLY_STATES = frozenset(
    {"stopped", "draining", "unavailable"},
)


def _append_gb10_node_source_drift(
    drift: list[StateDrift],
    *,
    desired_states: list[dict[str, Any]],
    nodes: object,
) -> None:
    """Detect per-node GB10 source-code drift (#356).

    DB-side `desired_states` / node-reported `image_tag` +
    `env_config_version` can converge while node-agents still run
    from a stale host-local checkout — e.g. `image=staging-c72f50d`
    but `source_git_commit=ce55a358d847...`. That path produced silent
    release-gate passes on `staging-baa1d327` / `staging-c72f50d`
    while GB10 workers actually ran pre-#350 code.

    For each desired state, verify every active node in the same
    (environment, pool) reports the exact explicitly declared
    `source_git_commit`. Only legacy desired states without that field may
    fall back to the SHA prefix embedded in `image_tag`. The checkout must
    also report `source_git_dirty is False`. Otherwise emit a StateDrift so
    the same `environment-state check` artifact consumed by the release gate
    fails hard instead of silently passing.
    """
    if not isinstance(nodes, list):
        return

    desired_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for desired in desired_states:
        env = desired.get("environment")
        pool = desired.get("pool_name")
        if not isinstance(env, str) or not isinstance(pool, str):
            continue
        desired_by_key[(env, pool)] = desired

    for node in nodes:
        if not isinstance(node, dict):
            continue
        env = node.get("environment")
        pool = node.get("pool_name")
        hostname = node.get("hostname")
        if not (isinstance(env, str) and isinstance(pool, str) and isinstance(hostname, str)):
            continue
        matched_desired = desired_by_key.get((env, pool))
        if matched_desired is None:
            continue
        host_intents = matched_desired.get("host_intents")
        authoritative_intent = (
            host_intents.get(hostname)
            if isinstance(host_intents, dict) and hostname in host_intents
            else None
        )
        if authoritative_intent is not None:
            if authoritative_intent in _GB10_NODE_SOURCE_DRIFT_IGNORED_INTENTS:
                continue
        else:
            intent = node.get("desired_intent") or node.get("current_intent")
            apply_state = node.get("apply_state")
            if (
                intent in _GB10_NODE_SOURCE_DRIFT_IGNORED_INTENTS
                or apply_state in _GB10_NODE_SOURCE_DRIFT_IGNORED_APPLY_STATES
            ):
                continue
        declared_source = matched_desired.get("source_git_commit")
        source_is_explicit = isinstance(declared_source, str) and bool(declared_source.strip())
        expected_source = declared_source if source_is_explicit else None
        if expected_source is None:
            expected_source = _release_source_prefix(matched_desired.get("image_tag"))
        if expected_source is None:
            continue
        expected_source = expected_source.strip()
        source_commit = node.get("source_git_commit")
        source_dirty = node.get("source_git_dirty")
        source_commit_bad = not isinstance(source_commit, str) or (
            source_commit != expected_source
            if source_is_explicit
            else not source_commit.startswith(expected_source)
        )
        source_dirty_bad = source_dirty is not False
        if source_commit_bad:
            drift.append(
                StateDrift(
                    path=(f"gb10_worker_node_status[{env}/{pool}/{hostname}].source_git_commit"),
                    desired=expected_source,
                    live=source_commit,
                ),
            )
        elif source_dirty_bad:
            drift.append(
                StateDrift(
                    path=(f"gb10_worker_node_status[{env}/{pool}/{hostname}].source_git_dirty"),
                    desired=False,
                    live=source_dirty,
                ),
            )


_TERMINAL_SLURM_JOB_STATES = {"completed", "failed", "cancelled", "stale"}


def _normalized_allowed_slurm_nodes(value: object) -> list[str]:
    raw_nodes: list[object]
    if isinstance(value, str):
        raw_nodes = list(value.split(","))
    elif isinstance(value, list | tuple):
        raw_nodes = list(value)
    else:
        raw_nodes = []
    return list(
        dict.fromkeys(node for node in (str(raw_node).strip() for raw_node in raw_nodes) if node),
    )


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
        actuator_config = desired.get("actuator_config", {})
        if not isinstance(actuator_config, dict):
            continue
        job_id = str(job.get("job_id") or job.get("id") or "unknown")
        prefix = f"slurm_worker_jobs[{environment}/{pool_name}/{job_id}]"
        allowed_nodes = _normalized_allowed_slurm_nodes(
            actuator_config.get("allowed_nodes"),
        )
        live_nodelist = job.get("nodelist")
        if not isinstance(live_nodelist, str) or live_nodelist not in allowed_nodes:
            drift.append(
                StateDrift(
                    path=f"{prefix}.nodelist",
                    desired=allowed_nodes,
                    live=live_nodelist,
                ),
            )
        redacted_env = job.get("redacted_env")
        if not isinstance(redacted_env, dict):
            continue
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
    _append_gb10_node_source_drift(
        drift,
        desired_states=profile.gb10_desired_states,
        nodes=_as_dict(live.get("gb10_status", {}), "gb10_status").get(
            "nodes",
            [],
        ),
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


def autoscaler_blockers(
    profile: EnvironmentStateProfile,
    live: dict[str, Any],
) -> list[dict[str, Any]]:
    expected_keys = {
        (policy["environment"], policy["pool_name"]) for policy in profile.autoscaler_policies
    }
    policies = _as_dict(live.get("autoscaler_status", {}), "autoscaler_status").get(
        "policies",
        [],
    )
    if not isinstance(policies, list):
        return []
    blockers: list[dict[str, Any]] = []
    for policy in policies:
        if not isinstance(policy, dict):
            continue
        key = (policy.get("environment"), policy.get("pool_name"))
        if key not in expected_keys:
            continue
        reason = policy.get("last_blocked_reason")
        if not isinstance(reason, str) or reason not in _AUTOSCALER_HARD_BLOCKERS:
            continue
        blockers.append(
            {
                "environment": policy.get("environment"),
                "pool_name": policy.get("pool_name"),
                "actuator": policy.get("actuator"),
                "last_decision": policy.get("last_decision"),
                "last_decision_reason": policy.get("last_decision_reason"),
                "last_blocked_reason": reason,
                "last_blocked_details": policy.get("last_blocked_details"),
                "last_error": policy.get("last_error"),
            }
        )
    return blockers


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
            f"TimeoutStartSec={supervisor['service_timeout_sec']}",
            f"WorkingDirectory={supervisor['working_directory']}",
            f"Environment=PYTHONPATH={supervisor['working_directory']}/src",
            "Environment=PYTHONDONTWRITEBYTECODE=1",
            f"ExecStart={_quote_command(command)}",
        ],
    )


def render_external_slurm_autoscaler_timer(supervisor: dict[str, Any]) -> str:
    desired_state = (
        "active"
        if supervisor.get("enabled") is True and supervisor.get("active") is True
        else "disabled"
    )
    return "\n".join(
        [
            "[Unit]",
            f"Description=Run Loom {supervisor['pool_name']} external autoscaler reconcile",
            f"# LoomDesiredState={desired_state}",
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
    lines = [line.rstrip() for line in text.splitlines() if not line.startswith("# ")]
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


def _append_exec_start_component_drift(
    drift: list[StateDrift],
    *,
    prefix: str,
    supervisor: dict[str, Any],
) -> None:
    python_path = Path(str(supervisor["python_path"]))
    if not python_path.is_file():
        drift.append(
            StateDrift(
                path=f"{prefix}.exec_start.python_path",
                desired=str(python_path),
                live="missing",
            ),
        )
    elif not python_path.stat().st_mode & 0o111:
        drift.append(
            StateDrift(
                path=f"{prefix}.exec_start.python_path",
                desired=str(python_path),
                live="not executable",
            ),
        )

    script_path = Path(str(supervisor["script_path"]))
    if not script_path.is_file():
        drift.append(
            StateDrift(
                path=f"{prefix}.exec_start.script_path",
                desired=str(script_path),
                live="missing",
            ),
        )


def _parse_systemctl_show_properties(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep and key:
            values[key] = value
    return values


def _append_service_status_drift(
    drift: list[StateDrift],
    *,
    prefix: str,
    service_name: str,
    runner: SubprocessRunner,
) -> None:
    rc, stdout, stderr = runner(
        [
            "systemctl",
            "--user",
            "show",
            service_name,
            "--property=Result",
            "--property=ExecMainStatus",
            "--property=ExecMainCode",
            "--property=ActiveState",
            "--property=SubState",
        ],
    )
    if rc != 0:
        drift.append(
            StateDrift(
                path=f"{prefix}.service_status",
                desired="readable service status",
                live=(stderr or stdout).strip() or f"systemctl exited {rc}",
            ),
        )
        return

    status = _parse_systemctl_show_properties(stdout)
    result = status.get("Result", "")
    exec_status = status.get("ExecMainStatus", "")
    active_state = status.get("ActiveState", "")
    failed = (
        active_state == "failed" or result not in {"", "success"} or exec_status not in {"", "0"}
    )
    if not failed:
        return

    drift.append(
        StateDrift(
            path=f"{prefix}.service_status",
            desired="service result success",
            live={
                "active_state": active_state,
                "exec_main_code": status.get("ExecMainCode", ""),
                "exec_main_status": exec_status,
                "result": result,
                "sub_state": status.get("SubState", ""),
            },
        ),
    )


def _external_supervisor_is_local(
    supervisor: dict[str, Any],
    *,
    hostname: str | None,
) -> bool:
    local_hostname = (hostname or socket.gethostname()).split(".", 1)[0].casefold()
    execution_host = str(supervisor.get("execution_host", "local"))
    desired_hostname = execution_host.split(".", 1)[0].casefold()
    return desired_hostname in {"local", local_hostname}


def _external_supervisors_for_host(
    supervisors: list[dict[str, Any]],
    *,
    hostname: str | None,
) -> list[dict[str, Any]]:
    """Return only supervisors owned by this physical Slurm controller."""

    return [
        supervisor
        for supervisor in supervisors
        if _external_supervisor_is_local(supervisor, hostname=hostname)
    ]


def diff_external_slurm_autoscaler_supervisors(
    profile: EnvironmentStateProfile,
    *,
    runner: SubprocessRunner | None = None,
    hostname: str | None = None,
) -> list[StateDrift]:
    command_runner = runner or _default_subprocess_runner
    drift: list[StateDrift] = []
    supervisors = _external_supervisors_for_host(
        profile.external_slurm_autoscaler_supervisors,
        hostname=hostname,
    )
    foreign_supervisors = [
        supervisor
        for supervisor in profile.external_slurm_autoscaler_supervisors
        if not _external_supervisor_is_local(supervisor, hostname=hostname)
    ]
    for supervisor in foreign_supervisors:
        prefix = (
            "external_slurm_autoscaler_supervisors"
            f"[{supervisor['environment']}/{supervisor['pool_name']}]"
        )
        for field, unit_name in (
            ("service_unit", supervisor["service_name"]),
            ("timer_unit", supervisor["timer_name"]),
        ):
            rc, stdout, _stderr = command_runner(
                ["systemctl", "--user", "cat", unit_name],
            )
            if rc == 0:
                drift.append(
                    StateDrift(
                        path=f"{prefix}.{field}",
                        desired="absent on foreign controller",
                        live=stdout.strip() or "installed",
                    ),
                )
    for supervisor in supervisors:
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
        _append_exec_start_component_drift(
            drift,
            prefix=prefix,
            supervisor=supervisor,
        )
        _append_service_status_drift(
            drift,
            prefix=prefix,
            service_name=str(supervisor["service_name"]),
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


def _run_supervisor_command_idempotent(
    command: list[str],
    *,
    runner: SubprocessRunner,
) -> None:
    """Run a systemctl command that must succeed on a missing unit (#331).

    Systemd exit code 5 (LSB EXIT_NOTINSTALLED) means "unit not loaded" —
    the timer/service was never installed, or was already removed. Some
    systemd releases instead return 1 for the same stop/disable request, so
    verify ``LoadState=not-found`` before accepting that result. This avoids
    depending on localized stderr while preserving real rc=1 failures.
    """
    rc, stdout, stderr = runner(command)
    if rc == 0:
        return
    if rc == 5:
        return
    if (
        rc == 1
        and len(command) == 4
        and command[:2] == ["systemctl", "--user"]
        and command[2] in {"disable", "stop"}
    ):
        probe_rc, probe_stdout, _probe_stderr = runner(
            [
                "systemctl",
                "--user",
                "show",
                command[3],
                "--property=LoadState",
                "--value",
            ]
        )
        if probe_rc == 0 and probe_stdout.strip() == "not-found":
            return
    message = (stderr or stdout).strip() or f"command exited {rc}"
    raise EnvironmentStateProfileError(f"{' '.join(command)} failed: {message}")


def apply_external_slurm_autoscaler_supervisors(
    profile: EnvironmentStateProfile,
    *,
    unit_dir: Path | None = None,
    runner: SubprocessRunner | None = None,
    hostname: str | None = None,
) -> list[dict[str, str]]:
    all_supervisors = profile.external_slurm_autoscaler_supervisors
    supervisors = _external_supervisors_for_host(
        all_supervisors,
        hostname=hostname,
    )
    foreign_supervisors = [
        supervisor
        for supervisor in all_supervisors
        if not _external_supervisor_is_local(supervisor, hostname=hostname)
    ]
    if not all_supervisors:
        return []

    command_runner = runner or _default_subprocess_runner
    target_dir = unit_dir or (Path.home() / ".config" / "systemd" / "user")
    target_dir.mkdir(parents=True, exist_ok=True)
    applied: list[dict[str, str]] = []
    for supervisor in foreign_supervisors:
        timer_name = supervisor["timer_name"]
        _run_supervisor_command_idempotent(
            ["systemctl", "--user", "stop", timer_name],
            runner=command_runner,
        )
        _run_supervisor_command_idempotent(
            ["systemctl", "--user", "disable", timer_name],
            runner=command_runner,
        )
        (target_dir / supervisor["service_name"]).unlink(missing_ok=True)
        (target_dir / timer_name).unlink(missing_ok=True)
    for supervisor in supervisors:
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
    for supervisor in supervisors:
        timer_name = supervisor["timer_name"]
        enabled = supervisor["enabled"]
        active = supervisor["active"]
        # #331: apply must enforce NEGATIVE desired state too. The four
        # combinations map explicitly so a reader can see what each one
        # produces. Positive transitions raise on failure (something is
        # actually wrong); negative transitions tolerate exit code 5
        # (unit already gone).
        if enabled and active:
            # Preserved from the original implementation — the dominant
            # fully-on rollout.
            _run_supervisor_command(
                ["systemctl", "--user", "enable", "--now", timer_name],
                runner=command_runner,
            )
            _run_supervisor_command(
                ["systemctl", "--user", "restart", timer_name],
                runner=command_runner,
            )
        elif enabled and not active:
            # Enable for boot, keep stopped now (a temporary pause).
            _run_supervisor_command(
                ["systemctl", "--user", "enable", timer_name],
                runner=command_runner,
            )
            _run_supervisor_command_idempotent(
                ["systemctl", "--user", "stop", timer_name],
                runner=command_runner,
            )
        elif not enabled and active:
            # Explicit "run but not persistent"; rare but supported.
            _run_supervisor_command_idempotent(
                ["systemctl", "--user", "disable", timer_name],
                runner=command_runner,
            )
            _run_supervisor_command(
                ["systemctl", "--user", "restart", timer_name],
                runner=command_runner,
            )
        else:
            # The #331 failure mode: OLDLAB scoped out; operator wants
            # the timer stopped AND to not come back after a boot. Stop
            # before disable so an in-flight timer trigger does not get
            # to fire; disable after so the unit does not survive a
            # systemd user restart.
            _run_supervisor_command_idempotent(
                ["systemctl", "--user", "stop", timer_name],
                runner=command_runner,
            )
            _run_supervisor_command_idempotent(
                ["systemctl", "--user", "disable", timer_name],
                runner=command_runner,
            )
    return applied


def autoscaler_policy_payload(policy: dict[str, Any]) -> dict[str, Any]:
    return {field: policy.get(field) for field in _AUTOSCALER_COMPARE_FIELDS}


def gb10_desired_state_payload(state: dict[str, Any]) -> dict[str, Any]:
    return {field: state.get(field) for field in _GB10_COMPARE_FIELDS}
