"""Versioned environment desired-state profiles for deploy rollouts."""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

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


class EnvironmentStateProfileError(ValueError):
    """Raised when an environment desired-state profile is invalid."""


@dataclass(frozen=True)
class EnvironmentStateProfile:
    environment: str
    control_plane_environment: str
    autoscaler_policies: list[dict[str, Any]]
    gb10_desired_states: list[dict[str, Any]]
    catalog_provisioning: dict[str, Any]


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
    return EnvironmentStateProfile(
        environment=environment,
        control_plane_environment=control_plane_environment,
        autoscaler_policies=autoscaler_policies,
        gb10_desired_states=gb10_desired_states,
        catalog_provisioning=_as_dict(catalog, "catalog_provisioning"),
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


def diff_environment_state(
    profile: EnvironmentStateProfile,
    live: dict[str, Any],
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
    return drift


def autoscaler_policy_payload(policy: dict[str, Any]) -> dict[str, Any]:
    return {field: policy.get(field) for field in _AUTOSCALER_COMPARE_FIELDS}


def gb10_desired_state_payload(state: dict[str, Any]) -> dict[str, Any]:
    return {field: state.get(field) for field in _GB10_COMPARE_FIELDS}
