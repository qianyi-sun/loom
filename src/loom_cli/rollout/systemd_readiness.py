"""Reusable, secret-safe systemd readiness predicates for rollout checks.

The broker preflight and the final GB10 convergence step must classify the
same user-manager, node-agent service, and timer states.  This module contains
the side-effect-free predicates and a bounded read-only user-manager probe;
callers provide the command runner and decide whether the evidence is used by
preflight or final verification.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol

_SYSTEMD_VERSION_RE = re.compile(r"[0-9]+(?:[.][0-9]+)*(?:[-+~.A-Za-z0-9]*)?\Z")
_BOOT_ID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\Z"
)
_REHEARSAL_UNIT_RE = re.compile(r"loom-preflight-[0-9a-f]{24}[.]service\Z")
_SHA256_RE = re.compile(r"[0-9a-f]{64}\Z")
DEFAULT_USER_MANAGER_RPC_BUDGET_MS = 5_000


class CommandResult(Protocol):
    """Minimal result surface required by the read-only probe."""

    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...


CommandRunner = Callable[[Sequence[str]], CommandResult]


class NodeAgentTimerState(StrEnum):
    """Only the two documented healthy timer states plus fail-closed invalid."""

    PREPARED = "prepared"
    TRANSIENT_RUNNING = "transient-running"
    INVALID = "invalid"


@dataclass(frozen=True, slots=True)
class UserManagerReadiness:
    """Bounded evidence from a non-mutating user-manager connectivity probe."""

    version: str
    linger_enabled: bool
    boot_id: str
    rpc_latency_ms: int
    rpc_budget_ms: int = DEFAULT_USER_MANAGER_RPC_BUDGET_MS

    @property
    def ready(self) -> bool:
        return (
            _SYSTEMD_VERSION_RE.fullmatch(self.version) is not None
            and self.linger_enabled
            and _BOOT_ID_RE.fullmatch(self.boot_id) is not None
            and 0 <= self.rpc_latency_ms <= self.rpc_budget_ms
        )

    @property
    def evidence_digest(self) -> str:
        payload = json.dumps(
            {
                "boot_id": self.boot_id,
                "linger_enabled": self.linger_enabled,
                "rpc_budget_ms": self.rpc_budget_ms,
                "rpc_latency_ms": self.rpc_latency_ms,
                "version": self.version,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class GB10HostReadiness:
    """Validated readonly systemd evidence for one GB10 host."""

    boot_id: str
    manager_version: str
    linger_enabled: bool
    service_ready: bool
    timer_state: NodeAgentTimerState
    timer_enabled: bool

    @property
    def ready(self) -> bool:
        return (
            _BOOT_ID_RE.fullmatch(self.boot_id) is not None
            and _SYSTEMD_VERSION_RE.fullmatch(self.manager_version) is not None
            and self.linger_enabled
            and self.service_ready
            and self.timer_state is NodeAgentTimerState.PREPARED
            and self.timer_enabled
        )

    @property
    def transient_timer(self) -> bool:
        return self.timer_state is NodeAgentTimerState.TRANSIENT_RUNNING

    @property
    def evidence_digest(self) -> str:
        payload = json.dumps(
            {
                "boot_id": self.boot_id,
                "linger_enabled": self.linger_enabled,
                "manager_version": self.manager_version,
                "service_ready": self.service_ready,
                "timer_enabled": self.timer_enabled,
                "timer_state": self.timer_state.value,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class RehearsalSystemdActivation:
    """One isolated transient-unit contract shared by plan/apply/verify/cleanup."""

    unit: str
    plan_digest: str
    latency_budget_ms: int = DEFAULT_USER_MANAGER_RPC_BUDGET_MS

    def __post_init__(self) -> None:
        if (
            _REHEARSAL_UNIT_RE.fullmatch(self.unit) is None
            or _SHA256_RE.fullmatch(self.plan_digest) is None
            or not 1 <= self.latency_budget_ms <= 60_000
        ):
            raise ValueError("rehearsal systemd activation authority is invalid")

    @property
    def description(self) -> str:
        return f"Loom isolated rehearsal {self.plan_digest}"

    @property
    def start_argv(self) -> tuple[str, ...]:
        """Return the only transient activation accepted by the rehearsal helper."""
        return (
            "systemd-run",
            "--user",
            f"--unit={self.unit}",
            f"--description={self.description}",
            "--property=Type=oneshot",
            "--property=RemainAfterExit=yes",
            "--property=NoNewPrivileges=yes",
            "--property=PrivateTmp=yes",
            "--property=ProtectSystem=strict",
            "--property=ProtectHome=yes",
            "--property=RestrictAddressFamilies=AF_UNIX",
            "--property=IPAddressDeny=any",
            "--",
            "/usr/bin/true",
        )

    @property
    def show_argv(self) -> tuple[str, ...]:
        properties = (
            "LoadState",
            "ActiveState",
            "SubState",
            "Type",
            "Result",
            "ExecMainStatus",
            "NeedDaemonReload",
            "Transient",
            "Description",
        )
        return (
            "systemctl",
            "--user",
            "show",
            self.unit,
            *(f"--property={name}" for name in properties),
        )

    @property
    def stop_argv(self) -> tuple[str, ...]:
        return ("systemctl", "--user", "stop", self.unit)

    @property
    def reset_argv(self) -> tuple[str, ...]:
        return ("systemctl", "--user", "reset-failed", self.unit)

    @property
    def load_state_argv(self) -> tuple[str, ...]:
        return (
            "systemctl",
            "--user",
            "show",
            self.unit,
            "--property=LoadState",
            "--value",
        )

    @property
    def expected_properties(self) -> dict[str, str]:
        """Return the sole unit identity safe for verification or cleanup."""
        return {
            "LoadState": "loaded",
            "ActiveState": "active",
            "SubState": "exited",
            "Type": "oneshot",
            "Result": "success",
            "ExecMainStatus": "0",
            "NeedDaemonReload": "no",
            "Transient": "yes",
            "Description": self.description,
        }

    def ready(self, properties: dict[str, str], *, latency_ms: int) -> bool:
        """Verify exact identity, sandbox result, and bounded activation latency."""
        return 0 <= latency_ms <= self.latency_budget_ms and properties == self.expected_properties

    @staticmethod
    def absent(properties: dict[str, str] | None) -> bool:
        """Accept only an absent transient unit before activation."""
        return properties is None or properties.get("LoadState") == "not-found"


def parse_systemctl_properties(stdout: str) -> dict[str, str]:
    """Parse the fixed ``systemctl show`` key/value output."""
    properties: dict[str, str] = {}
    for line in stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator:
            properties[key] = value
    return properties


def node_agent_service_is_prepared(properties: dict[str, str]) -> bool:
    """Classify a successful completed ``Type=oneshot`` invocation."""
    return (
        properties.get("LoadState") == "loaded"
        and properties.get("Type") == "oneshot"
        and properties.get("Result") == "success"
        and properties.get("ExecMainStatus") == "0"
        and properties.get("NeedDaemonReload") == "no"
    )


def classify_node_agent_timer(
    properties: dict[str, str],
    *,
    service: str,
) -> NodeAgentTimerState:
    """Recognize waiting and the bounded in-flight oneshot state only."""
    common = (
        properties.get("LoadState") == "loaded"
        and properties.get("ActiveState") == "active"
        and properties.get("Unit") == service
        and properties.get("NeedDaemonReload") == "no"
    )
    if common and properties.get("SubState") == "waiting":
        return NodeAgentTimerState.PREPARED
    if common and properties.get("SubState") == "running":
        return NodeAgentTimerState.TRANSIENT_RUNNING
    return NodeAgentTimerState.INVALID


def node_agent_service_status_summary(properties: dict[str, str]) -> str:
    """Render only allowlisted, non-secret service status properties."""
    keys = (
        "LoadState",
        "Type",
        "Result",
        "ExecMainStatus",
        "ActiveState",
        "SubState",
        "NeedDaemonReload",
    )
    return " ".join(f"{key}={properties.get(key, '<missing>')}" for key in keys)


def node_agent_timer_status_summary(properties: dict[str, str]) -> str:
    """Render only allowlisted, non-secret timer status properties."""
    keys = ("LoadState", "ActiveState", "SubState", "Unit", "NeedDaemonReload")
    return " ".join(f"{key}={properties.get(key, '<missing>')}" for key in keys)


def probe_user_manager_readonly(
    run: CommandRunner,
    *,
    uid: int,
    rpc_budget_ms: int = DEFAULT_USER_MANAGER_RPC_BUDGET_MS,
    monotonic: Callable[[], float] = time.monotonic,
) -> UserManagerReadiness | None:
    """Probe manager RPC, linger, and boot identity without creating a unit.

    The elapsed budget covers all three fixed read-only calls.  Actual
    ``systemd-run`` activation belongs to the isolated rehearsal tier, where a
    request-specific unit and cleanup journal can be used safely.
    """
    if uid < 0 or not 1 <= rpc_budget_ms <= 60_000:
        return None
    started = monotonic()
    commands = (
        ("version", ("systemctl", "--user", "show", "--property=Version", "--value")),
        (
            "linger",
            ("loginctl", "show-user", str(uid), "--property=Linger", "--value"),
        ),
        ("boot_id", ("cat", "/proc/sys/kernel/random/boot_id")),
    )
    outputs: dict[str, str] = {}
    try:
        for name, argv in commands:
            result = run(argv)
            if result.returncode != 0 or not isinstance(result.stdout, str):
                return None
            value = result.stdout.strip()
            if not value or "\n" in value:
                return None
            outputs[name] = value
    except Exception:
        return None
    elapsed_ms = max(0, round((monotonic() - started) * 1000))
    evidence = UserManagerReadiness(
        version=outputs["version"],
        linger_enabled=outputs["linger"] == "yes",
        boot_id=outputs["boot_id"],
        rpc_latency_ms=elapsed_ms,
        rpc_budget_ms=rpc_budget_ms,
    )
    return evidence if evidence.ready else None


def parse_gb10_host_readiness(
    payload: str,
    *,
    service: str,
) -> GB10HostReadiness | None:
    """Validate one fixed remote JSON payload without exposing raw output."""
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, dict) or set(decoded) != {
        "schema_version",
        "boot_id",
        "manager_version",
        "linger_enabled",
        "service",
        "timer",
        "timer_enabled",
    }:
        return None
    if decoded["schema_version"] != 1:
        return None
    service_keys = {
        "LoadState",
        "Type",
        "Result",
        "ExecMainStatus",
        "ActiveState",
        "SubState",
        "NeedDaemonReload",
    }
    timer_keys = {"LoadState", "ActiveState", "SubState", "Unit", "NeedDaemonReload"}
    if (
        not isinstance(decoded["boot_id"], str)
        or not isinstance(decoded["manager_version"], str)
        or type(decoded["linger_enabled"]) is not bool
        or type(decoded["timer_enabled"]) is not bool
        or not isinstance(decoded["service"], dict)
        or not isinstance(decoded["timer"], dict)
        or set(decoded["service"]) != service_keys
        or set(decoded["timer"]) != timer_keys
        or not all(
            isinstance(key, str) and isinstance(value, str)
            for properties in (decoded["service"], decoded["timer"])
            for key, value in properties.items()
        )
    ):
        return None
    evidence = GB10HostReadiness(
        boot_id=decoded["boot_id"],
        manager_version=decoded["manager_version"],
        linger_enabled=decoded["linger_enabled"],
        service_ready=node_agent_service_is_prepared(decoded["service"]),
        timer_state=classify_node_agent_timer(decoded["timer"], service=service),
        timer_enabled=decoded["timer_enabled"],
    )
    if (
        _BOOT_ID_RE.fullmatch(evidence.boot_id) is None
        or _SYSTEMD_VERSION_RE.fullmatch(evidence.manager_version) is None
    ):
        return None
    return evidence


__all__ = [
    "DEFAULT_USER_MANAGER_RPC_BUDGET_MS",
    "GB10HostReadiness",
    "NodeAgentTimerState",
    "RehearsalSystemdActivation",
    "UserManagerReadiness",
    "classify_node_agent_timer",
    "node_agent_service_is_prepared",
    "node_agent_service_status_summary",
    "node_agent_timer_status_summary",
    "parse_gb10_host_readiness",
    "parse_systemctl_properties",
    "probe_user_manager_readonly",
]
