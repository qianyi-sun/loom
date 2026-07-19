"""One sanitized subprocess boundary for every installed preflight adapter."""

from __future__ import annotations

import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .config import OperatorConfig
from .readonly_preflight_authority import READONLY_KUBECONFIG_PATH


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


class SubprocessRun(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None,
        env: Mapping[str, str],
        input: str | None,
        timeout: int,
    ) -> CommandResult: ...


def _subprocess_run(
    argv: Sequence[str],
    *,
    cwd: Path | None,
    env: Mapping[str, str],
    input: str | None,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        cwd=cwd,
        env=dict(env),
        input=input,
        stdin=subprocess.DEVNULL if input is None else None,
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )


@dataclass(frozen=True, slots=True)
class InstalledPreflightCommands:
    """Typed adapters backed by one exact, secret-free child environment."""

    config: OperatorConfig
    child_environment: Mapping[str, str]
    run_subprocess: SubprocessRun = _subprocess_run

    def __post_init__(self) -> None:
        environment = dict(self.child_environment)
        required = {"HOME", "LANG", "LC_ALL", "PATH", "USER"}
        if (
            self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or not required <= environment.keys()
            or any("TOKEN" in key or "SECRET" in key for key in environment)
        ):
            raise ValueError("installed preflight command environment is invalid")
        object.__setattr__(self, "child_environment", environment)

    def executable(self, name: str) -> str | None:
        if not name or "/" in name or "\x00" in name:
            return None
        return shutil.which(name, path=self.child_environment["PATH"])

    def simple(self, argv: Sequence[str]) -> CommandResult:
        return self._execute(argv, timeout=120)

    def git(self, argv: list[str]) -> CommandResult:
        return self._execute(argv, cwd=self.config.runner_repo, timeout=120)

    def image(self, argv: Sequence[str], cwd: Path | None) -> CommandResult:
        if cwd is not None and cwd != self.config.runner_repo:
            raise ValueError("preflight image build escaped exact candidate root")
        return self._execute(argv, cwd=cwd, timeout=1800)

    def readonly_json(self, argv: Sequence[str], payload: bytes) -> CommandResult:
        try:
            rendered = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("readonly Kubernetes request is not UTF-8") from exc
        environment = {**self.child_environment, "KUBECONFIG": str(READONLY_KUBECONFIG_PATH)}
        return self._execute(argv, environment=environment, input=rendered, timeout=30)

    def manifest_server_dry_run(self, rendered: str) -> CommandResult:
        if not rendered or len(rendered.encode("utf-8")) > 16 * 1024 * 1024:
            raise ValueError("preflight manifest payload is invalid")
        return self._execute(
            (
                "kubectl",
                "--kubeconfig",
                str(self.config.kubeconfig_path),
                "--namespace",
                self.config.namespace,
                "apply",
                "--dry-run=server",
                "--validate=strict",
                "--request-timeout=30s",
                "-f",
                "-",
            ),
            input=rendered,
            timeout=60,
        )

    def rehearsal_helper(
        self,
        argv: Sequence[str],
        environment: Mapping[str, str],
        timeout: int,
    ) -> CommandResult:
        expected = {
            "HOME",
            "LANG",
            "LC_ALL",
            "PATH",
            "XDG_RUNTIME_DIR",
        }
        if set(environment) != expected or not 1 <= timeout <= 1800:
            raise ValueError("rehearsal helper execution authority is invalid")
        return self._execute(argv, environment=environment, timeout=timeout)

    def _execute(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        environment: Mapping[str, str] | None = None,
        input: str | None = None,
        timeout: int,
    ) -> CommandResult:
        command = tuple(argv)
        if (
            not command
            or any(not isinstance(item, str) or not item or "\x00" in item for item in command)
            or (cwd is not None and (not cwd.is_absolute() or ".." in cwd.parts))
            or not 1 <= timeout <= 1800
        ):
            raise ValueError("installed preflight command is invalid")
        child = dict(self.child_environment if environment is None else environment)
        if any("TOKEN" in key or "SECRET" in key for key in child):
            raise ValueError("installed preflight command environment contains secret authority")
        result = self.run_subprocess(
            command,
            cwd=cwd,
            env=child,
            input=input,
            timeout=timeout,
        )
        if (
            type(result.returncode) is not int
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
        ):
            raise RuntimeError("installed preflight command result is invalid")
        return result


__all__ = ["CommandResult", "InstalledPreflightCommands", "SubprocessRun"]
