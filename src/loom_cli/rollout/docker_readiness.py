"""Single-source, non-mutating Docker runtime readiness predicate."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...


CommandRunner = Callable[[Sequence[str]], CommandResult]
_SYSCTL = "/usr/sbin/sysctl"


@dataclass(frozen=True, slots=True)
class DockerRuntimeReadiness:
    """Safe aggregate for daemon, buildx, and host inotify capacity."""

    daemon_ready: bool
    buildx_ready: bool
    inotify_capacity_ready: bool

    @property
    def ready(self) -> bool:
        return self.daemon_ready and self.buildx_ready and self.inotify_capacity_ready

    @property
    def evidence_digest(self) -> str:
        payload = json.dumps(
            {
                "buildx_ready": self.buildx_ready,
                "daemon_ready": self.daemon_ready,
                "inotify_capacity_ready": self.inotify_capacity_ready,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()


def _command_ready(run: CommandRunner, argv: tuple[str, ...]) -> bool:
    try:
        result = run(argv)
    except Exception:
        return False
    return type(result.returncode) is int and result.returncode == 0


def _inotify_capacity_ready(run: CommandRunner) -> bool:
    try:
        result = run((_SYSCTL, "-n", "fs.inotify.max_user_instances"))
        value = result.stdout.strip()
    except Exception:
        return False
    return (
        type(result.returncode) is int
        and result.returncode == 0
        and value.isascii()
        and value.isdecimal()
        and int(value) >= 1024
    )


def probe_docker_runtime(run: CommandRunner) -> DockerRuntimeReadiness:
    """Run all fixed read-only probes and retain every independent blocker."""
    return DockerRuntimeReadiness(
        daemon_ready=_command_ready(run, ("docker", "info")),
        buildx_ready=_command_ready(run, ("docker", "buildx", "version")),
        inotify_capacity_ready=_inotify_capacity_ready(run),
    )


__all__ = ["DockerRuntimeReadiness", "probe_docker_runtime"]
