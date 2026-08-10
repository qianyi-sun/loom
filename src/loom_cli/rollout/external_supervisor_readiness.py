"""Immutable exact-candidate external autoscaler supervisor readiness."""

from __future__ import annotations

import hashlib
import ipaddress
import json
import math
import os
import re
import stat
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Protocol

from loom_cli.environment_state import (
    EnvironmentStateProfile,
    load_environment_state_profile,
    render_external_slurm_autoscaler_service,
    render_external_slurm_autoscaler_timer,
)
from loom_cli.rollout.credential_authority import TrustedFileRead, read_trusted_file

PROFILE_PATH = "deploy/environment-state/staging.toml"
SCRIPT_PATH = "scripts/ops/worker_pool_autoscaler_external_once.py"
STAGING_RUNNER_ROOT = "/opt/loom-staging-runner"
STAGING_CANDIDATE_RUNTIME_ROOT = f"{STAGING_RUNNER_ROOT}/candidates"
STAGING_NAMESPACE = "loom-staging"
STAGING_KUBECONFIG = "/var/lib/loom-staging-rollout/kubeconfig"
STAGING_ROLLOUT_EXECUTION_HOST = "TRT-EAI-OLDLAB-1"
REHEARSAL_KUBECONFIG = "/var/lib/loom-staging-rollout/credentials/rehearsal-kubeconfig"
_REHEARSAL_DB_LOCAL_PORT_OFFSET = 10_000
_MAX_REHEARSAL_SOURCE_PORT = 65_535 - _REHEARSAL_DB_LOCAL_PORT_OFFSET

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_RUNTIME_ROOT_RE = re.compile(
    rf"^{re.escape(STAGING_CANDIDATE_RUNTIME_ROOT)}/(?P<sha>[0-9a-f]{{40}})$"
)
_IMAGE_TAG_RE = re.compile(r"^staging-[a-z0-9][a-z0-9-]{5,63}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_SYSTEMD_UNIT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,127}$")
_PROTECTED_SYSTEMD_UNIT_RE = re.compile(
    r"^loom-autoscaler-[a-z0-9][a-z0-9-]{1,95}\.(?:service|timer)$"
)
_KUBERNETES_NAME_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_INTEGER_DURATION_RE = re.compile(r"^[1-9][0-9]{0,4}$")
_STAGING_SLURM_AUTHORITIES = {
    "gb10": ("trt-gb10", "gx10-01c7"),
    "oldlab": ("trt-oldlab", "TRT-EAI-OLDLAB-1"),
}
_STAGING_DATABASE_SECRETS = {
    "gb10": "loom-external-slurm-autoscaler-db",
    "oldlab": "loom-secrets",
}

_MAX_PROFILE_BYTES = 1024 * 1024
_MAX_SCRIPT_BYTES = 2 * 1024 * 1024
_MAX_ARTIFACT_BYTES = 2 * 1024 * 1024
_MAX_SUPERVISORS = 16
_MAX_STRING_LENGTH = 4096

_REQUIRED_ARGUMENTS = frozenset(
    {
        "--environment",
        "--pool-name",
        "--expected-slurm-cluster-name",
        "--expected-slurm-controller-host",
        "--namespace",
        "--kubeconfig",
        "--db-local-host",
        "--db-local-port",
        "--db-service",
        "--db-remote-port",
        "--db-port-forward-ready-timeout-sec",
        "--db-port-forward-stop-timeout-sec",
        "--db-connect-timeout-sec",
        "--freshness-sec",
    }
)
_OPTIONAL_ARGUMENTS = frozenset(
    {
        "--kubectl",
        "--db-secret-name",
        "--db-secret-key",
        "--scontrol",
    }
)
_ALLOWED_ARGUMENTS = _REQUIRED_ARGUMENTS | _OPTIONAL_ARGUMENTS


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...


SystemdAnalyzeRunner = Callable[[Sequence[str]], CommandResult]


def _rehearsal_db_local_port(live_port: int) -> int:
    if not 1024 <= live_port <= _MAX_REHEARSAL_SOURCE_PORT:
        raise ValueError("external supervisor rehearsal DB local port is out of bounds")
    return live_port + _REHEARSAL_DB_LOCAL_PORT_OFFSET


def staging_runtime_root(candidate_sha: str) -> str:
    """Return the immutable local runtime root for one exact candidate."""
    if _SHA_RE.fullmatch(candidate_sha) is None:
        raise ValueError("external supervisor candidate SHA is invalid")
    return f"{STAGING_CANDIDATE_RUNTIME_ROOT}/{candidate_sha}"


def staging_working_directory(candidate_sha: str) -> str:
    return f"{staging_runtime_root(candidate_sha)}/repo"


def staging_python_path(candidate_sha: str) -> str:
    return f"{staging_runtime_root(candidate_sha)}/venv/bin/python"


def staging_script_path(candidate_sha: str) -> str:
    return f"{staging_working_directory(candidate_sha)}/{SCRIPT_PATH}"


def _json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        + b"\n"
    )


def _hash_json(value: object) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _string(values: Mapping[str, object], field: str) -> str:
    value = values.get(field)
    if not isinstance(value, str):
        raise ValueError(f"external supervisor {field} must be a string")
    return value


def _integer(values: Mapping[str, object], field: str) -> int:
    value = values.get(field)
    if type(value) is not int:
        raise ValueError(f"external supervisor {field} must be an integer")
    return value


def _boolean(values: Mapping[str, object], field: str) -> bool:
    value = values.get(field)
    if type(value) is not bool:
        raise ValueError(f"external supervisor {field} must be a boolean")
    return value


def _clean_text(value: object, field: str, *, maximum: int = _MAX_STRING_LENGTH) -> str:
    if (
        not isinstance(value, str)
        or not value
        or value != value.strip()
        or len(value) > maximum
        or any(ord(character) < 32 or ord(character) == 127 for character in value)
    ):
        raise ValueError(f"external supervisor {field} is invalid")
    return value


def _safe_absolute_path(value: str, field: str) -> str:
    cleaned = _clean_text(value, field, maximum=512)
    parsed = PurePosixPath(cleaned)
    if (
        not parsed.is_absolute()
        or cleaned == "/"
        or str(parsed) != cleaned
        or ".." in parsed.parts
        or any(character.isspace() for character in cleaned)
    ):
        raise ValueError(f"external supervisor {field} must be a normalized absolute path")
    return cleaned


def _safe_relative_path(value: str, field: str) -> str:
    cleaned = _clean_text(value, field, maximum=512)
    parsed = PurePosixPath(cleaned)
    if (
        parsed.is_absolute()
        or str(parsed) != cleaned
        or not parsed.parts
        or ".." in parsed.parts
        or any(character.isspace() for character in cleaned)
    ):
        raise ValueError(f"external supervisor {field} must be a normalized relative path")
    return cleaned


def _bounded_integer(value: str, field: str, *, minimum: int, maximum: int) -> int:
    if not value.isascii() or not value.isdecimal() or value.startswith("0"):
        raise ValueError(f"external supervisor {field} is invalid")
    parsed = int(value)
    if not minimum <= parsed <= maximum:
        raise ValueError(f"external supervisor {field} is out of bounds")
    return parsed


def _bounded_decimal(value: str, field: str, *, maximum: float) -> str:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"external supervisor {field} is invalid") from exc
    if not math.isfinite(parsed) or not 0 < parsed <= maximum:
        raise ValueError(f"external supervisor {field} is out of bounds")
    return format(parsed, ".15g")


def _argument_map(args: tuple[str, ...]) -> dict[str, str]:
    if not args or len(args) > 64:
        raise ValueError("external supervisor args are invalid")
    if "--validate-only" in args or any(token.startswith("--validate-only=") for token in args):
        raise ValueError("external supervisor live argv already contains validate-only")
    parsed: dict[str, str] = {}
    index = 0
    while index < len(args):
        token = _clean_text(args[index], "argument", maximum=512)
        if "=" in token:
            flag, value = token.split("=", 1)
            index += 1
        else:
            flag = token
            if index + 1 >= len(args):
                raise ValueError(f"external supervisor argument {flag!r} has no value")
            value = _clean_text(args[index + 1], "argument value", maximum=512)
            index += 2
        if flag not in _ALLOWED_ARGUMENTS or flag in parsed or not value:
            raise ValueError(f"external supervisor argument {flag!r} is unauthorized")
        parsed[flag] = value
    missing = _REQUIRED_ARGUMENTS - set(parsed)
    if missing:
        raise ValueError(
            "external supervisor bounded tunnel arguments are missing: "
            + ", ".join(sorted(missing))
        )
    return parsed


def _duration(value: str, field: str, *, maximum: int = 3600) -> str:
    cleaned = _clean_text(value, field, maximum=8)
    if _INTEGER_DURATION_RE.fullmatch(cleaned) is None or int(cleaned) > maximum:
        raise ValueError(f"external supervisor {field} is invalid")
    return cleaned


def _unit_name(value: str, field: str, suffix: str) -> str:
    cleaned = _clean_text(value, field, maximum=128)
    if (
        _SYSTEMD_UNIT_RE.fullmatch(cleaned) is None
        or not cleaned.endswith(suffix)
        or "/" in cleaned
        or "\\" in cleaned
    ):
        raise ValueError(f"external supervisor {field} is invalid")
    return cleaned


def _protected_unit_name(value: str, field: str, suffix: str) -> str:
    cleaned = _clean_text(value, field, maximum=128)
    if _PROTECTED_SYSTEMD_UNIT_RE.fullmatch(cleaned) is None or not cleaned.endswith(suffix):
        raise ValueError(f"external supervisor {field} is not a protected unit name")
    return cleaned


@dataclass(frozen=True, slots=True)
class ExternalSupervisorIdentity:
    """One normalized supervisor and its exact final installed unit bytes."""

    environment: str
    control_plane_environment: str
    name: str
    pool_name: str
    execution_host: str
    service_name: str
    timer_name: str
    runtime_root: str
    working_directory: str
    python_path: str
    script_path: str
    args: tuple[str, ...]
    requires: tuple[str, ...]
    timer_on_boot_sec: str
    timer_on_unit_active_sec: str
    timer_accuracy_sec: str
    service_timeout_sec: str
    enabled: bool
    active: bool
    db_local_host: str
    db_local_port: int
    db_service: str
    db_remote_port: int
    db_port_forward_ready_timeout_sec: str
    db_port_forward_stop_timeout_sec: str
    db_connect_timeout_sec: str
    freshness_sec: int
    service_unit: str
    timer_unit: str

    def __post_init__(self) -> None:
        if self.environment != "staging":
            raise ValueError("external supervisor environment is invalid")
        if _IDENTIFIER_RE.fullmatch(_clean_text(self.name, "name", maximum=128)) is None:
            raise ValueError("external supervisor name is invalid")
        if _IDENTIFIER_RE.fullmatch(_clean_text(self.pool_name, "pool_name", maximum=128)) is None:
            raise ValueError("external supervisor pool name is invalid")
        expected_slurm_authority = _STAGING_SLURM_AUTHORITIES.get(self.pool_name)
        execution_host = _clean_text(
            self.execution_host,
            "execution_host",
            maximum=253,
        )
        if (
            expected_slurm_authority is None
            or execution_host.split(".", 1)[0].casefold()
            != expected_slurm_authority[1].split(".", 1)[0].casefold()
        ):
            raise ValueError("external supervisor execution host drifted")
        service_name = _protected_unit_name(
            self.service_name,
            "service_name",
            ".service",
        )
        timer_name = _protected_unit_name(
            self.timer_name,
            "timer_name",
            ".timer",
        )
        if service_name.removesuffix(".service") != timer_name.removesuffix(".timer"):
            raise ValueError("external supervisor service/timer unit pair is invalid")
        runtime_root = _safe_absolute_path(self.runtime_root, "runtime_root")
        runtime_match = _RUNTIME_ROOT_RE.fullmatch(runtime_root)
        if runtime_match is None:
            raise ValueError("external supervisor runtime root is not canonical")
        runtime_sha = runtime_match.group("sha")
        if _safe_absolute_path(
            self.working_directory, "working_directory"
        ) != staging_working_directory(runtime_sha):
            raise ValueError("external supervisor working directory is not canonical")
        if _safe_absolute_path(self.python_path, "python_path") != staging_python_path(runtime_sha):
            raise ValueError("external supervisor Python path is not canonical")
        if _safe_absolute_path(self.script_path, "script_path") != staging_script_path(runtime_sha):
            raise ValueError("external supervisor script path is not canonical")
        if not self.args or any(not isinstance(item, str) for item in self.args):
            raise ValueError("external supervisor args are invalid")
        arguments = _argument_map(self.args)
        if (
            _clean_text(
                self.control_plane_environment,
                "control_plane_environment",
                maximum=128,
            )
            != self.control_plane_environment
            or arguments["--environment"] != self.control_plane_environment
        ):
            raise ValueError("external supervisor environment argument drifted")
        if arguments["--pool-name"] != self.pool_name:
            raise ValueError("external supervisor pool argument drifted")
        if (
            arguments["--expected-slurm-cluster-name"],
            arguments["--expected-slurm-controller-host"],
        ) != expected_slurm_authority:
            raise ValueError("external supervisor Slurm authority drifted")
        if arguments["--namespace"] != STAGING_NAMESPACE:
            raise ValueError("external supervisor staging namespace is not canonical")
        if arguments["--kubeconfig"] != STAGING_KUBECONFIG:
            raise ValueError("external supervisor staging kubeconfig is not canonical")
        optional_authority = {
            "--kubectl": "/usr/local/bin/kubectl",
            "--db-secret-key": "cp-db-url",
            "--scontrol": "/usr/bin/scontrol",
        }
        if any(
            flag in arguments and arguments[flag] != expected
            for flag, expected in optional_authority.items()
        ) or arguments.get("--db-secret-name", "loom-secrets") != (
            _STAGING_DATABASE_SECRETS[self.pool_name]
        ):
            raise ValueError("external supervisor optional authority is not canonical")
        try:
            local_host = str(ipaddress.ip_address(arguments["--db-local-host"]))
        except ValueError as exc:
            raise ValueError("external supervisor DB local host is invalid") from exc
        if not ipaddress.ip_address(local_host).is_loopback or local_host != self.db_local_host:
            raise ValueError("external supervisor DB local host must be normalized loopback")
        if (
            _bounded_integer(
                arguments["--db-local-port"],
                "db local port",
                minimum=1024,
                maximum=65535,
            )
            != self.db_local_port
        ):
            raise ValueError("external supervisor DB local port drifted")
        # `loom-postgres` is an ExternalName compatibility service and cannot
        # be used by `kubectl port-forward`; bind the supervisor to CNPG's
        # concrete read/write service instead.
        if arguments["--db-service"] != "service/loom-postgres-rw":
            raise ValueError("external supervisor DB service is not canonical")
        if self.db_service != arguments["--db-service"]:
            raise ValueError("external supervisor DB service drifted")
        if (
            _bounded_integer(
                arguments["--db-remote-port"],
                "db remote port",
                minimum=1,
                maximum=65535,
            )
            != self.db_remote_port
            or self.db_remote_port != 5432
        ):
            raise ValueError("external supervisor DB remote port drifted")
        normalized_timeouts = (
            _bounded_decimal(
                arguments["--db-port-forward-ready-timeout-sec"],
                "port-forward ready timeout",
                maximum=60.0,
            ),
            _bounded_decimal(
                arguments["--db-port-forward-stop-timeout-sec"],
                "port-forward stop timeout",
                maximum=30.0,
            ),
            _bounded_decimal(
                arguments["--db-connect-timeout-sec"],
                "DB connect timeout",
                maximum=60.0,
            ),
        )
        if normalized_timeouts != (
            self.db_port_forward_ready_timeout_sec,
            self.db_port_forward_stop_timeout_sec,
            self.db_connect_timeout_sec,
        ):
            raise ValueError("external supervisor timeout identity drifted")
        if (
            _bounded_integer(
                arguments["--freshness-sec"],
                "freshness",
                minimum=1,
                maximum=3600,
            )
            != self.freshness_sec
        ):
            raise ValueError("external supervisor freshness drifted")
        if not self.requires or len(set(self.requires)) != len(self.requires):
            raise ValueError("external supervisor requirements are invalid")
        for requirement in self.requires:
            _unit_name(requirement, "requires", ".target")
        if "network-online.target" not in self.requires:
            raise ValueError("external supervisor network-online dependency is missing")
        _duration(self.timer_on_boot_sec, "timer_on_boot_sec")
        timer_interval = int(
            _duration(self.timer_on_unit_active_sec, "timer_on_unit_active_sec")
        )
        if timer_interval > self.freshness_sec:
            raise ValueError("external supervisor timer exceeds its freshness bound")
        _duration(self.timer_accuracy_sec, "timer_accuracy_sec")
        _duration(self.service_timeout_sec, "service_timeout_sec", maximum=7200)
        if type(self.enabled) is not bool or type(self.active) is not bool:
            raise ValueError("external supervisor enablement is invalid")
        rendered = self._renderer_input()
        if self.service_unit != render_external_slurm_autoscaler_service(rendered) + "\n":
            raise ValueError("external supervisor service bytes drifted")
        if self.timer_unit != render_external_slurm_autoscaler_timer(rendered) + "\n":
            raise ValueError("external supervisor timer bytes drifted")
        for field, unit in (("service", self.service_unit), ("timer", self.timer_unit)):
            if (
                not unit.endswith("\n")
                or unit.endswith("\n\n")
                or "\r" in unit
                or "\x00" in unit
                or len(unit.encode("utf-8")) > 128 * 1024
            ):
                raise ValueError(f"external supervisor {field} bytes are invalid")

    def _renderer_input(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "control_plane_environment": self.control_plane_environment,
            "name": self.name,
            "pool_name": self.pool_name,
            "execution_host": self.execution_host,
            "service_name": self.service_name,
            "timer_name": self.timer_name,
            "runtime_root": self.runtime_root,
            "working_directory": self.working_directory,
            "python_path": self.python_path,
            "script_path": self.script_path,
            "args": list(self.args),
            "requires": list(self.requires),
            "timer_on_boot_sec": self.timer_on_boot_sec,
            "timer_on_unit_active_sec": self.timer_on_unit_active_sec,
            "timer_accuracy_sec": self.timer_accuracy_sec,
            "service_timeout_sec": self.service_timeout_sec,
            "enabled": self.enabled,
            "active": self.active,
        }

    def validation_argv(self, namespace: str, kubeconfig: str) -> tuple[str, ...]:
        """Derive the isolated validate-only command without changing other args."""
        isolated_namespace = _clean_text(namespace, "rehearsal namespace", maximum=63)
        if (
            _KUBERNETES_NAME_RE.fullmatch(isolated_namespace) is None
            or not isolated_namespace.startswith("loom-rehearsal-")
            or isolated_namespace == STAGING_NAMESPACE
        ):
            raise ValueError("external supervisor rehearsal namespace is not isolated")
        if kubeconfig != REHEARSAL_KUBECONFIG:
            raise ValueError("external supervisor rehearsal kubeconfig is not canonical")
        _argument_map(self.args)

        rewritten: list[str] = []
        index = 0
        replacements = {
            "--namespace": namespace,
            "--kubeconfig": kubeconfig,
            "--db-local-port": str(_rehearsal_db_local_port(self.db_local_port)),
        }
        while index < len(self.args):
            token = self.args[index]
            if "=" in token:
                flag, _value = token.split("=", 1)
                rewritten.append(f"{flag}={replacements.get(flag, token.split('=', 1)[1])}")
                index += 1
                continue
            rewritten.append(token)
            rewritten.append(replacements.get(token, self.args[index + 1]))
            index += 2
        return (self.python_path, self.script_path, *rewritten, "--validate-only")

    def to_dict(self) -> dict[str, object]:
        return {
            "environment": self.environment,
            "control_plane_environment": self.control_plane_environment,
            "name": self.name,
            "pool_name": self.pool_name,
            "execution_host": self.execution_host,
            "service_name": self.service_name,
            "timer_name": self.timer_name,
            "runtime_root": self.runtime_root,
            "working_directory": self.working_directory,
            "python_path": self.python_path,
            "script_path": self.script_path,
            "args": list(self.args),
            "requires": list(self.requires),
            "timer_on_boot_sec": self.timer_on_boot_sec,
            "timer_on_unit_active_sec": self.timer_on_unit_active_sec,
            "timer_accuracy_sec": self.timer_accuracy_sec,
            "service_timeout_sec": self.service_timeout_sec,
            "enabled": self.enabled,
            "active": self.active,
            "db_local_host": self.db_local_host,
            "db_local_port": self.db_local_port,
            "db_service": self.db_service,
            "db_remote_port": self.db_remote_port,
            "db_port_forward_ready_timeout_sec": self.db_port_forward_ready_timeout_sec,
            "db_port_forward_stop_timeout_sec": self.db_port_forward_stop_timeout_sec,
            "db_connect_timeout_sec": self.db_connect_timeout_sec,
            "freshness_sec": self.freshness_sec,
            "service_unit": self.service_unit,
            "timer_unit": self.timer_unit,
        }


_IDENTITY_FIELDS = frozenset(ExternalSupervisorIdentity.__dataclass_fields__)


def _identity_from_dict(raw: object) -> ExternalSupervisorIdentity:
    if not isinstance(raw, dict) or set(raw) != _IDENTITY_FIELDS:
        raise ValueError("external supervisor identity fields are invalid")
    args = raw["args"]
    requires = raw["requires"]
    if (
        not isinstance(args, list)
        or not isinstance(requires, list)
        or any(not isinstance(item, str) for item in (*args, *requires))
    ):
        raise ValueError("external supervisor identity lists are invalid")
    return ExternalSupervisorIdentity(
        environment=_string(raw, "environment"),
        control_plane_environment=_string(raw, "control_plane_environment"),
        name=_string(raw, "name"),
        pool_name=_string(raw, "pool_name"),
        execution_host=_string(raw, "execution_host"),
        service_name=_string(raw, "service_name"),
        timer_name=_string(raw, "timer_name"),
        runtime_root=_string(raw, "runtime_root"),
        working_directory=_string(raw, "working_directory"),
        python_path=_string(raw, "python_path"),
        script_path=_string(raw, "script_path"),
        args=tuple(args),
        requires=tuple(requires),
        timer_on_boot_sec=_string(raw, "timer_on_boot_sec"),
        timer_on_unit_active_sec=_string(raw, "timer_on_unit_active_sec"),
        timer_accuracy_sec=_string(raw, "timer_accuracy_sec"),
        service_timeout_sec=_string(raw, "service_timeout_sec"),
        enabled=_boolean(raw, "enabled"),
        active=_boolean(raw, "active"),
        db_local_host=_string(raw, "db_local_host"),
        db_local_port=_integer(raw, "db_local_port"),
        db_service=_string(raw, "db_service"),
        db_remote_port=_integer(raw, "db_remote_port"),
        db_port_forward_ready_timeout_sec=_string(raw, "db_port_forward_ready_timeout_sec"),
        db_port_forward_stop_timeout_sec=_string(raw, "db_port_forward_stop_timeout_sec"),
        db_connect_timeout_sec=_string(raw, "db_connect_timeout_sec"),
        freshness_sec=_integer(raw, "freshness_sec"),
        service_unit=_string(raw, "service_unit"),
        timer_unit=_string(raw, "timer_unit"),
    )


@dataclass(frozen=True, slots=True)
class ExternalSupervisorArtifact:
    """Secret-free exact-candidate supervisor input and final unit bytes."""

    schema_version: int
    candidate_sha: str
    candidate_tree: str
    environment: str
    image_tag: str
    runtime_root: str
    profile_path: str
    profile_sha256: str
    script_sha256: Mapping[str, str]
    supervisors: tuple[ExternalSupervisorIdentity, ...]
    artifact_digest: str

    def __post_init__(self) -> None:
        scripts = dict(self.script_sha256)
        units = [
            unit_name
            for supervisor in self.supervisors
            for unit_name in (supervisor.service_name, supervisor.timer_name)
        ]
        ports = [supervisor.db_local_port for supervisor in self.supervisors]
        rehearsal_ports = [_rehearsal_db_local_port(port) for port in ports]
        if (
            self.schema_version != 3
            or _SHA_RE.fullmatch(self.candidate_sha) is None
            or _SHA_RE.fullmatch(self.candidate_tree) is None
            or self.environment != "staging"
            or _IMAGE_TAG_RE.fullmatch(self.image_tag) is None
            or self.runtime_root != staging_runtime_root(self.candidate_sha)
            or self.profile_path != PROFILE_PATH
            or _SHA256_RE.fullmatch(self.profile_sha256) is None
            or set(scripts) != {SCRIPT_PATH}
            or any(_SHA256_RE.fullmatch(value) is None for value in scripts.values())
            or not 1 <= len(self.supervisors) <= _MAX_SUPERVISORS
            or tuple(sorted(self.supervisors, key=lambda item: item.name)) != self.supervisors
            or len({item.name for item in self.supervisors}) != len(self.supervisors)
            or len(units) != len(set(units))
            or len(ports) != len(set(ports))
            or len(rehearsal_ports) != len(set(rehearsal_ports))
            or not set(ports).isdisjoint(rehearsal_ports)
            or any(item.environment != self.environment for item in self.supervisors)
            or any(item.runtime_root != self.runtime_root for item in self.supervisors)
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
        ):
            raise ValueError("external supervisor artifact identity is invalid")
        object.__setattr__(self, "script_sha256", MappingProxyType(dict(sorted(scripts.items()))))
        if _hash_json(self.payload()) != self.artifact_digest:
            raise ValueError("external supervisor artifact digest drifted")

    @property
    def unit_sha256(self) -> Mapping[str, str]:
        values = {
            supervisor.service_name: hashlib.sha256(
                supervisor.service_unit.encode("utf-8")
            ).hexdigest()
            for supervisor in self.supervisors
        }
        values.update(
            {
                supervisor.timer_name: hashlib.sha256(
                    supervisor.timer_unit.encode("utf-8")
                ).hexdigest()
                for supervisor in self.supervisors
            }
        )
        return MappingProxyType(dict(sorted(values.items())))

    def validation_argv(
        self,
        namespace: str,
        kubeconfig: str,
    ) -> Mapping[str, tuple[str, ...]]:
        """Return one exact isolated validation argv per retained supervisor."""
        return MappingProxyType(
            {
                supervisor.name: supervisor.validation_argv(namespace, kubeconfig)
                for supervisor in self.supervisors
                if supervisor.enabled and supervisor.active
            }
        )

    def payload(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            "candidate_sha": self.candidate_sha,
            "candidate_tree": self.candidate_tree,
            "environment": self.environment,
            "image_tag": self.image_tag,
            "runtime_root": self.runtime_root,
            "profile_path": self.profile_path,
            "profile_sha256": self.profile_sha256,
            "script_sha256": dict(self.script_sha256),
            "supervisors": [supervisor.to_dict() for supervisor in self.supervisors],
        }

    def to_bytes(self) -> bytes:
        return _json_bytes({**self.payload(), "artifact_digest": self.artifact_digest})

    @classmethod
    def from_bytes(cls, payload: bytes) -> ExternalSupervisorArtifact:
        if not isinstance(payload, bytes) or not 1 <= len(payload) <= _MAX_ARTIFACT_BYTES:
            raise ValueError("external supervisor artifact bytes are invalid")
        try:
            raw = json.loads(payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise ValueError("external supervisor artifact is invalid") from exc
        expected = {
            "schema_version",
            "candidate_sha",
            "candidate_tree",
            "environment",
            "image_tag",
            "runtime_root",
            "profile_path",
            "profile_sha256",
            "script_sha256",
            "supervisors",
            "artifact_digest",
        }
        if not isinstance(raw, dict) or set(raw) != expected:
            raise ValueError("external supervisor artifact fields are invalid")
        scripts = raw["script_sha256"]
        supervisors = raw["supervisors"]
        if (
            not isinstance(scripts, dict)
            or any(
                not isinstance(key, str) or not isinstance(value, str)
                for key, value in scripts.items()
            )
            or not isinstance(supervisors, list)
        ):
            raise ValueError("external supervisor artifact collections are invalid")
        artifact = cls(
            schema_version=_integer(raw, "schema_version"),
            candidate_sha=_string(raw, "candidate_sha"),
            candidate_tree=_string(raw, "candidate_tree"),
            environment=_string(raw, "environment"),
            image_tag=_string(raw, "image_tag"),
            runtime_root=_string(raw, "runtime_root"),
            profile_path=_string(raw, "profile_path"),
            profile_sha256=_string(raw, "profile_sha256"),
            script_sha256=MappingProxyType(dict(scripts)),
            supervisors=tuple(_identity_from_dict(item) for item in supervisors),
            artifact_digest=_string(raw, "artifact_digest"),
        )
        if payload != artifact.to_bytes():
            raise ValueError("external supervisor artifact encoding is not canonical")
        return artifact


@dataclass(frozen=True, slots=True)
class ExternalSupervisorVerification:
    """Fail-closed result from static verification of temporary exact units."""

    ready: bool
    artifact_digest: str
    unit_names: tuple[str, ...]
    unit_sha256: Mapping[str, str]
    failed_units: Mapping[str, str]
    verification_digest: str

    def __post_init__(self) -> None:
        hashes = dict(self.unit_sha256)
        failures = dict(self.failed_units)
        expected = set(self.unit_names)
        valid_ready = self.ready and set(hashes) == expected and not failures
        valid_failed = not self.ready and not hashes and set(failures) == expected
        if (
            type(self.ready) is not bool
            or _SHA256_RE.fullmatch(self.artifact_digest) is None
            or not self.unit_names
            or tuple(sorted(self.unit_names)) != self.unit_names
            or len(expected) != len(self.unit_names)
            or not (valid_ready or valid_failed)
            or any(_SHA256_RE.fullmatch(value) is None for value in hashes.values())
            or any(value != "systemd-analyze" for value in failures.values())
            or _SHA256_RE.fullmatch(self.verification_digest) is None
        ):
            raise ValueError("external supervisor verification is inconsistent")
        object.__setattr__(self, "unit_sha256", MappingProxyType(dict(sorted(hashes.items()))))
        object.__setattr__(self, "failed_units", MappingProxyType(dict(sorted(failures.items()))))
        if _hash_json(self.payload()) != self.verification_digest:
            raise ValueError("external supervisor verification digest drifted")

    def payload(self) -> dict[str, object]:
        return {
            "ready": self.ready,
            "artifact_digest": self.artifact_digest,
            "unit_names": list(self.unit_names),
            "unit_sha256": dict(self.unit_sha256),
            "failed_units": dict(self.failed_units),
        }


def _candidate_source(root: Path, relative: str) -> Path:
    parsed = PurePosixPath(_safe_relative_path(relative, "candidate source"))
    candidate = root.joinpath(*parsed.parts)
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ValueError("external supervisor candidate source is unavailable") from exc
    if not resolved.is_relative_to(root):
        raise ValueError("external supervisor candidate source escapes candidate root")
    return candidate


def _trusted_source(path: Path, *, maximum: int) -> TrustedFileRead:
    return read_trusted_file(
        path,
        service_uid=os.geteuid(),
        private=False,
        max_bytes=maximum,
        require_nonempty=True,
    )


def _same_trusted_source(first: TrustedFileRead, second: TrustedFileRead) -> bool:
    return (
        first.payload == second.payload
        and first.metadata_fingerprint == second.metadata_fingerprint
        and first.acl_fingerprint == second.acl_fingerprint
    )


def _load_profile_snapshot(
    payload: bytes,
    *,
    candidate_sha: str,
    image_tag: str,
) -> EnvironmentStateProfile:
    with tempfile.TemporaryDirectory(prefix="loom-external-supervisor-profile-") as raw_dir:
        path = Path(raw_dir) / "staging.toml"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(path, flags, 0o600)
        try:
            written = 0
            while written < len(payload):
                written += os.write(descriptor, payload[written:])
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        return load_environment_state_profile(
            path,
            variables={
                "IMAGE_TAG": image_tag,
                "ENV_CONFIG_VERSION": image_tag,
                "GIT_SHA": candidate_sha,
            },
            expected_environment="staging",
        )


def _normalize_supervisor(raw: Mapping[str, object]) -> ExternalSupervisorIdentity:
    args_value = raw.get("args")
    requires_value = raw.get("requires")
    if (
        not isinstance(args_value, list)
        or not isinstance(requires_value, list)
        or any(not isinstance(item, str) for item in (*args_value, *requires_value))
    ):
        raise ValueError("external supervisor normalized lists are invalid")
    args = tuple(args_value)
    arguments = _argument_map(args)
    try:
        local_address = ipaddress.ip_address(arguments["--db-local-host"])
    except ValueError as exc:
        raise ValueError("external supervisor DB local host is invalid") from exc
    if not local_address.is_loopback:
        raise ValueError("external supervisor DB local host must be loopback")
    working_directory = str(raw.get("working_directory", ""))
    runtime_root = str(PurePosixPath(working_directory).parent)
    renderer_input = dict(raw)
    service_unit = render_external_slurm_autoscaler_service(renderer_input) + "\n"
    timer_unit = render_external_slurm_autoscaler_timer(renderer_input) + "\n"
    return ExternalSupervisorIdentity(
        environment=str(raw.get("environment", "")),
        control_plane_environment=str(raw.get("control_plane_environment", "")),
        name=str(raw.get("name", "")),
        pool_name=str(raw.get("pool_name", "")),
        execution_host=str(raw.get("execution_host", "")),
        service_name=str(raw.get("service_name", "")),
        timer_name=str(raw.get("timer_name", "")),
        runtime_root=runtime_root,
        working_directory=working_directory,
        python_path=str(raw.get("python_path", "")),
        script_path=str(raw.get("script_path", "")),
        args=args,
        requires=tuple(requires_value),
        timer_on_boot_sec=str(raw.get("timer_on_boot_sec", "")),
        timer_on_unit_active_sec=str(raw.get("timer_on_unit_active_sec", "")),
        timer_accuracy_sec=str(raw.get("timer_accuracy_sec", "")),
        service_timeout_sec=str(raw.get("service_timeout_sec", "")),
        enabled=_boolean(raw, "enabled"),
        active=_boolean(raw, "active"),
        db_local_host=str(local_address),
        db_local_port=_bounded_integer(
            arguments["--db-local-port"], "db local port", minimum=1024, maximum=65535
        ),
        db_service=arguments["--db-service"],
        db_remote_port=_bounded_integer(
            arguments["--db-remote-port"], "db remote port", minimum=1, maximum=65535
        ),
        db_port_forward_ready_timeout_sec=_bounded_decimal(
            arguments["--db-port-forward-ready-timeout-sec"],
            "port-forward ready timeout",
            maximum=60.0,
        ),
        db_port_forward_stop_timeout_sec=_bounded_decimal(
            arguments["--db-port-forward-stop-timeout-sec"],
            "port-forward stop timeout",
            maximum=30.0,
        ),
        db_connect_timeout_sec=_bounded_decimal(
            arguments["--db-connect-timeout-sec"], "DB connect timeout", maximum=60.0
        ),
        freshness_sec=_bounded_integer(
            arguments["--freshness-sec"], "freshness", minimum=1, maximum=3600
        ),
        service_unit=service_unit,
        timer_unit=timer_unit,
    )


def build_external_supervisor_artifact(
    candidate_root: Path,
    *,
    candidate_sha: str,
    candidate_tree: str,
    image_tag: str,
    environment: str = "staging",
    execution_host: str | None = None,
) -> ExternalSupervisorArtifact:
    """Build an immutable secret-free artifact from exact safe candidate sources."""
    if (
        not isinstance(candidate_root, Path)
        or not candidate_root.is_absolute()
        or _SHA_RE.fullmatch(candidate_sha) is None
        or _SHA_RE.fullmatch(candidate_tree) is None
        or _IMAGE_TAG_RE.fullmatch(image_tag) is None
        or environment != "staging"
        or (
            execution_host is not None
            and _IDENTIFIER_RE.fullmatch(execution_host) is None
        )
    ):
        raise ValueError("external supervisor candidate binding is invalid")
    try:
        root_metadata = candidate_root.lstat()
        root = candidate_root.resolve(strict=True)
    except OSError as exc:
        raise ValueError("external supervisor candidate root is unavailable") from exc
    if not stat.S_ISDIR(root_metadata.st_mode) or stat.S_ISLNK(root_metadata.st_mode):
        raise ValueError("external supervisor candidate root is unsafe")

    profile_path = _candidate_source(root, PROFILE_PATH)
    script_path = _candidate_source(root, SCRIPT_PATH)
    profile_first = _trusted_source(profile_path, maximum=_MAX_PROFILE_BYTES)
    script_first = _trusted_source(script_path, maximum=_MAX_SCRIPT_BYTES)
    if not stat.S_IMODE(script_first.metadata.st_mode) & stat.S_IXUSR:
        raise ValueError("external supervisor script is not owner-executable")

    profile = _load_profile_snapshot(
        profile_first.payload,
        candidate_sha=candidate_sha,
        image_tag=image_tag,
    )
    if any(
        raw.get("enabled") is not raw.get("active")
        for raw in profile.external_slurm_autoscaler_supervisors
    ):
        raise ValueError("external supervisor enabled and active state must converge together")
    protected_pools = {
        str(pool)
        for pool in profile.external_slurm_runner_prerequisites.get("pools", ())
        if isinstance(pool, str) and pool
    }
    desired_execution_host = (
        execution_host.split(".", 1)[0].casefold()
        if execution_host is not None
        else None
    )
    supervisors = tuple(
        sorted(
            (
                _normalize_supervisor(raw)
                for raw in profile.external_slurm_autoscaler_supervisors
                if (
                    (
                        raw.get("enabled") is True
                        or raw.get("active") is True
                        or raw.get("pool_name") in protected_pools
                    )
                    and (
                        desired_execution_host is None
                        or str(raw.get("execution_host", ""))
                        .split(".", 1)[0]
                        .casefold()
                        == desired_execution_host
                    )
                )
            ),
            key=lambda item: item.name,
        )
    )
    if not supervisors:
        raise ValueError("external supervisor execution host has no managed supervisors")

    profile_second = _trusted_source(profile_path, maximum=_MAX_PROFILE_BYTES)
    script_second = _trusted_source(script_path, maximum=_MAX_SCRIPT_BYTES)
    if not _same_trusted_source(profile_first, profile_second):
        raise ValueError("external supervisor profile changed while artifact was built")
    if not _same_trusted_source(script_first, script_second):
        raise ValueError("external supervisor script changed while artifact was built")

    payload = {
        "schema_version": 3,
        "candidate_sha": candidate_sha,
        "candidate_tree": candidate_tree,
        "environment": environment,
        "image_tag": image_tag,
        "runtime_root": staging_runtime_root(candidate_sha),
        "profile_path": PROFILE_PATH,
        "profile_sha256": hashlib.sha256(profile_first.payload).hexdigest(),
        "script_sha256": {SCRIPT_PATH: hashlib.sha256(script_first.payload).hexdigest()},
        "supervisors": [supervisor.to_dict() for supervisor in supervisors],
    }
    return ExternalSupervisorArtifact(
        schema_version=3,
        candidate_sha=candidate_sha,
        candidate_tree=candidate_tree,
        environment=environment,
        image_tag=image_tag,
        runtime_root=staging_runtime_root(candidate_sha),
        profile_path=PROFILE_PATH,
        profile_sha256=str(payload["profile_sha256"]),
        script_sha256=payload["script_sha256"],  # type: ignore[arg-type]
        supervisors=supervisors,
        artifact_digest=_hash_json(payload),
    )


def _write_exact_unit(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        written = 0
        while written < len(payload):
            written += os.write(descriptor, payload[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _verification(
    artifact: ExternalSupervisorArtifact,
    *,
    ready: bool,
) -> ExternalSupervisorVerification:
    unit_names = tuple(sorted(artifact.unit_sha256))
    payload = {
        "ready": ready,
        "artifact_digest": artifact.artifact_digest,
        "unit_names": list(unit_names),
        "unit_sha256": dict(artifact.unit_sha256) if ready else {},
        "failed_units": {} if ready else dict.fromkeys(unit_names, "systemd-analyze"),
    }
    return ExternalSupervisorVerification(
        ready=ready,
        artifact_digest=artifact.artifact_digest,
        unit_names=unit_names,
        unit_sha256=payload["unit_sha256"],  # type: ignore[arg-type]
        failed_units=payload["failed_units"],  # type: ignore[arg-type]
        verification_digest=_hash_json(payload),
    )


def verify_external_supervisor_artifact(
    artifact: ExternalSupervisorArtifact,
    run: SystemdAnalyzeRunner,
) -> ExternalSupervisorVerification:
    """Verify exact rendered units from a temporary directory without mutation."""
    if ExternalSupervisorArtifact.from_bytes(artifact.to_bytes()) != artifact:
        raise ValueError("external supervisor artifact could not be revalidated")
    with tempfile.TemporaryDirectory(prefix="loom-external-supervisor-verify-") as raw_dir:
        directory = Path(raw_dir)
        paths: list[Path] = []
        for supervisor in artifact.supervisors:
            for name, text in (
                (supervisor.service_name, supervisor.service_unit),
                (supervisor.timer_name, supervisor.timer_unit),
            ):
                path = directory / name
                _write_exact_unit(path, text.encode("utf-8"))
                paths.append(path)
        try:
            result = run(("systemd-analyze", "verify", *(str(path) for path in paths)))
            ready = type(result.returncode) is int and result.returncode == 0
        except Exception:
            return _verification(artifact, ready=False)
        return _verification(artifact, ready=ready)


__all__ = [
    "PROFILE_PATH",
    "REHEARSAL_KUBECONFIG",
    "SCRIPT_PATH",
    "STAGING_CANDIDATE_RUNTIME_ROOT",
    "STAGING_KUBECONFIG",
    "STAGING_NAMESPACE",
    "STAGING_ROLLOUT_EXECUTION_HOST",
    "STAGING_RUNNER_ROOT",
    "ExternalSupervisorArtifact",
    "ExternalSupervisorIdentity",
    "ExternalSupervisorVerification",
    "SystemdAnalyzeRunner",
    "build_external_supervisor_artifact",
    "staging_python_path",
    "staging_runtime_root",
    "staging_script_path",
    "staging_working_directory",
    "verify_external_supervisor_artifact",
]
