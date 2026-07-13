"""Fresh, fixed-origin candidate binding for the staging rollout broker."""

from __future__ import annotations

import os
import pwd
import re
import stat
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from .config import OperatorConfig
from .model import (
    APPROVED_FETCH_REF,
    APPROVED_REMOTE_URL,
    PINNED_TARGET_REF,
    CandidateBinding,
)

_RESOLVED_SHA_OUTPUT_RE = re.compile(r"^(?P<sha>[0-9a-f]{40})\n?$")
_FETCH_REFSPEC = "+refs/heads/dev:refs/remotes/origin/dev"
_RESOLVE_REF = "refs/remotes/origin/dev^{commit}"


class CommandResult(Protocol):
    """Captured result returned by the narrow injected Git runner."""

    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


GitRunner = Callable[[list[str]], CommandResult]
Clock = Callable[[], datetime]


class CandidateBindingError(RuntimeError):
    """Raised when the trusted checkout cannot yield an approved candidate."""


def _service_uid(config: OperatorConfig) -> int:
    try:
        return pwd.getpwnam(config.service_user).pw_uid
    except (KeyError, OSError) as exc:
        raise CandidateBindingError("configured service account is not available") from exc


def _validate_protected_config(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise CandidateBindingError(f"protected config is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise CandidateBindingError(f"protected config must not be a symlink: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise CandidateBindingError(f"protected config must be a regular file: {path}")
    if metadata.st_uid != 0:
        raise CandidateBindingError(f"protected config owner UID {metadata.st_uid} is not root")
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise CandidateBindingError(f"protected config is group/world writable: {path}")


def _validate_trusted_directory(path: Path, *, service_uid: int) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise CandidateBindingError(f"trusted checkout path is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise CandidateBindingError(f"trusted checkout path must not be a symlink: {path}")
    if not stat.S_ISDIR(metadata.st_mode):
        raise CandidateBindingError(f"trusted checkout path must be a directory: {path}")
    if metadata.st_uid not in {0, service_uid}:
        raise CandidateBindingError(
            f"trusted checkout owner UID {metadata.st_uid} is neither root nor the service account"
        )
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise CandidateBindingError(f"trusted checkout path is group/world writable: {path}")


def _validate_trusted_git_config(path: Path, *, service_uid: int) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise CandidateBindingError(f"trusted git config is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise CandidateBindingError(f"trusted git config must not be a symlink: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise CandidateBindingError(f"trusted git config must be a regular file: {path}")
    if metadata.st_uid not in {0, service_uid}:
        raise CandidateBindingError(
            f"trusted git config owner UID {metadata.st_uid} is neither root nor the service account"
        )
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise CandidateBindingError(f"trusted git config is group/world writable: {path}")


def _git_argv(repo: Path, *args: str) -> list[str]:
    return ["git", "-C", str(repo), *args]


def _invoke(run: GitRunner, argv: list[str], *, operation: str) -> CommandResult:
    try:
        return run(argv)
    except OSError as exc:
        raise CandidateBindingError(f"git {operation} could not be executed") from exc


def _require_success(result: CommandResult, *, operation: str) -> str:
    if result.returncode != 0:
        raise CandidateBindingError(f"git {operation} failed")
    return result.stdout


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def bind_fresh_origin_dev(
    config: OperatorConfig,
    *,
    run: GitRunner,
    now: Clock,
) -> CandidateBinding:
    """Fetch and bind the only approved merged ``origin/dev`` candidate."""
    if config.remote_url != APPROVED_REMOTE_URL:
        raise CandidateBindingError("config does not contain the approved remote URL")
    if config.target_ref != APPROVED_FETCH_REF:
        raise CandidateBindingError("config does not contain the approved target ref")

    _validate_protected_config(config.config_path)
    service_uid = _service_uid(config)
    _validate_trusted_directory(config.runner_repo, service_uid=service_uid)
    _validate_trusted_directory(config.runner_repo / ".git", service_uid=service_uid)
    _validate_trusted_git_config(
        config.runner_repo / ".git" / "config",
        service_uid=service_uid,
    )

    remote_result = _invoke(
        run,
        _git_argv(config.runner_repo, "remote"),
        operation="remote inspection",
    )
    remotes = _require_success(remote_result, operation="remote inspection").splitlines()
    if remotes != ["origin"]:
        raise CandidateBindingError("trusted checkout must have only remote origin")

    url_result = _invoke(
        run,
        _git_argv(config.runner_repo, "remote", "get-url", "--all", "origin"),
        operation="origin fetch URL inspection",
    )
    urls = _require_success(url_result, operation="origin fetch URL inspection").splitlines()
    if urls != [APPROVED_REMOTE_URL]:
        raise CandidateBindingError("origin must have exactly one approved fetch URL")

    pushurl_result = _invoke(
        run,
        _git_argv(config.runner_repo, "config", "--get-all", "remote.origin.pushurl"),
        operation="origin pushurl inspection",
    )
    if not (
        pushurl_result.returncode == 1
        and pushurl_result.stdout == ""
        and pushurl_result.stderr == ""
    ):
        raise CandidateBindingError("remote.origin.pushurl must be absent")

    status_result = _invoke(
        run,
        _git_argv(config.runner_repo, "status", "--porcelain=v1", "--untracked-files=all"),
        operation="status inspection",
    )
    status_output = _require_success(status_result, operation="status inspection")
    if status_output != "":
        raise CandidateBindingError("trusted checkout must be clean")

    fetch_result = _invoke(
        run,
        _git_argv(
            config.runner_repo,
            "fetch",
            "--force",
            "--no-tags",
            "--prune",
            "--no-recurse-submodules",
            "origin",
            _FETCH_REFSPEC,
        ),
        operation="fetch",
    )
    _require_success(fetch_result, operation="fetch")

    resolve_result = _invoke(
        run,
        _git_argv(config.runner_repo, "rev-parse", "--verify", _RESOLVE_REF),
        operation="rev-parse",
    )
    resolved_output = _require_success(resolve_result, operation="rev-parse")
    match = _RESOLVED_SHA_OUTPUT_RE.fullmatch(resolved_output)
    if match is None:
        raise CandidateBindingError("resolved SHA must be exactly 40 lowercase hexadecimal chars")
    resolved_sha = match.group("sha")

    return CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref=PINNED_TARGET_REF,
        resolved_sha=resolved_sha,
        image_tag=f"staging-{resolved_sha[:7]}",
        fetched_at=_utc_timestamp(now()),
    )


__all__ = [
    "CandidateBindingError",
    "Clock",
    "CommandResult",
    "GitRunner",
    "bind_fresh_origin_dev",
]
