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


__all__ = [
    "DEFAULT_USER_MANAGER_RPC_BUDGET_MS",
    "NodeAgentTimerState",
    "UserManagerReadiness",
    "classify_node_agent_timer",
    "node_agent_service_is_prepared",
    "node_agent_service_status_summary",
    "node_agent_timer_status_summary",
    "parse_systemctl_properties",
    "probe_user_manager_readonly",
]
