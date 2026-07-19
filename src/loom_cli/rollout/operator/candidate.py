"""Fresh, fixed-origin candidate binding for the staging rollout broker."""

from __future__ import annotations

import hashlib
import json
import os
import pwd
import re
import stat
from collections.abc import Callable
from dataclasses import dataclass
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
# The root-side sealed-source validator lives outside the installed Python
# package.  Keep this runtime admission bound locked to the same value with a
# cross-layer contract test so the broker cannot reject a source the installer
# already accepted.
MAX_CUMULATIVE_COMMITS = 64


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


@dataclass(frozen=True, slots=True)
class CandidateIdentityEvidence:
    resolved_sha: str
    resolved_tree: str
    source_mode: str
    approved_base_sha: str | None
    linear_history_count: int
    evidence_digest: str


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


def bind_configured_candidate(
    config: OperatorConfig,
    *,
    run: GitRunner,
    now: Clock,
) -> CandidateBinding:
    """Bind merged dev by default or one exact installed cumulative source."""
    if config.source_mode == "merged-dev":
        return bind_fresh_origin_dev(config, run=run, now=now)
    if (
        config.source_mode != "sealed-cumulative"
        or config.source_commit_sha is None
        or config.source_tree_sha is None
        or config.source_base_sha is None
    ):
        raise CandidateBindingError("sealed candidate config binding is incomplete")

    binding = CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref=PINNED_TARGET_REF,
        resolved_sha=config.source_commit_sha,
        image_tag=f"staging-{config.source_commit_sha[:7]}",
        fetched_at=_utc_timestamp(now()),
        source_mode="sealed-cumulative",
        resolved_tree=config.source_tree_sha,
        approved_base_sha=config.source_base_sha,
    )
    verify_bound_candidate(config, binding, run=run)
    return binding


def verify_bound_candidate(
    config: OperatorConfig,
    binding: CandidateBinding,
    *,
    run: GitRunner,
) -> CandidateIdentityEvidence:
    """Prove one already-resolved candidate through the shared read-only predicate."""
    _validate_protected_config(config.config_path)
    service_uid = _service_uid(config)
    _validate_trusted_directory(config.runner_repo, service_uid=service_uid)
    _validate_trusted_directory(config.runner_repo / ".git", service_uid=service_uid)
    _validate_trusted_git_config(config.runner_repo / ".git" / "config", service_uid=service_uid)

    def exact(*args: str, operation: str) -> str:
        return _require_success(
            _invoke(run, _git_argv(config.runner_repo, *args), operation=operation),
            operation=operation,
        ).strip()

    if exact("remote", operation="remote inspection") != "origin":
        raise CandidateBindingError("trusted checkout must have only remote origin")
    if (
        exact("remote", "get-url", "--all", "origin", operation="origin fetch URL inspection")
        != APPROVED_REMOTE_URL
    ):
        raise CandidateBindingError("origin must have exactly one approved fetch URL")
    pushurl = _invoke(
        run,
        _git_argv(config.runner_repo, "config", "--get-all", "remote.origin.pushurl"),
        operation="origin pushurl inspection",
    )
    if not (pushurl.returncode == 1 and pushurl.stdout == "" and pushurl.stderr == ""):
        raise CandidateBindingError("remote.origin.pushurl must be absent")
    if exact("status", "--porcelain=v1", "--untracked-files=all", operation="status inspection"):
        raise CandidateBindingError("trusted checkout must be clean")
    history_count = 0
    approved_base: str | None = None
    if binding.source_mode == "sealed-cumulative":
        symbolic = _invoke(
            run,
            _git_argv(config.runner_repo, "symbolic-ref", "-q", "HEAD"),
            operation="HEAD mode inspection",
        )
        if not (symbolic.returncode == 1 and symbolic.stdout == "" and symbolic.stderr == ""):
            raise CandidateBindingError("sealed candidate must use detached HEAD")
        if (
            config.source_mode != "sealed-cumulative"
            or config.source_commit_sha != binding.resolved_sha
            or config.source_tree_sha != binding.resolved_tree
            or config.source_base_sha != binding.approved_base_sha
            or binding.resolved_tree is None
            or binding.approved_base_sha is None
        ):
            raise CandidateBindingError("sealed candidate config binding drifted")
        if (
            exact("rev-parse", "--verify", "HEAD^{commit}", operation="commit inspection")
            != binding.resolved_sha
        ):
            raise CandidateBindingError("sealed candidate commit identity drifted")
        resolved_tree = exact("rev-parse", "--verify", "HEAD^{tree}", operation="tree inspection")
        if resolved_tree != binding.resolved_tree:
            raise CandidateBindingError("sealed candidate tree identity drifted")
        if (
            exact(
                "merge-base",
                binding.approved_base_sha,
                binding.resolved_sha,
                operation="base inspection",
            )
            != binding.approved_base_sha
        ):
            raise CandidateBindingError("sealed candidate base identity drifted")
        history = exact(
            "rev-list",
            "--reverse",
            "--parents",
            f"{binding.approved_base_sha}..{binding.resolved_sha}",
            operation="history inspection",
        ).splitlines()
        expected_parent = binding.approved_base_sha
        for line in history:
            fields = line.split()
            if len(fields) != 2 or fields[1] != expected_parent:
                raise CandidateBindingError("sealed candidate history is not linear")
            expected_parent = fields[0]
        if (
            not 1 <= len(history) <= MAX_CUMULATIVE_COMMITS
            or expected_parent != binding.resolved_sha
        ):
            raise CandidateBindingError("sealed candidate history does not match config")
        history_count = len(history)
        approved_base = binding.approved_base_sha
    else:
        if config.source_mode != "merged-dev":
            raise CandidateBindingError("merged candidate config binding drifted")
        if (
            exact(
                "rev-parse",
                "--verify",
                "refs/remotes/origin/dev^{commit}",
                operation="remote candidate inspection",
            )
            != binding.resolved_sha
        ):
            raise CandidateBindingError("merged candidate commit identity drifted")
        resolved_tree = exact(
            "rev-parse",
            "--verify",
            f"{binding.resolved_sha}^{{tree}}",
            operation="tree inspection",
        )

    digest_payload = {
        "approved_base_sha": approved_base,
        "linear_history_count": history_count,
        "resolved_sha": binding.resolved_sha,
        "resolved_tree": resolved_tree,
        "source_mode": binding.source_mode,
    }
    evidence_digest = hashlib.sha256(
        json.dumps(digest_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return CandidateIdentityEvidence(
        resolved_sha=binding.resolved_sha,
        resolved_tree=resolved_tree,
        source_mode=binding.source_mode,
        approved_base_sha=approved_base,
        linear_history_count=history_count,
        evidence_digest=evidence_digest,
    )


__all__ = [
    "CandidateBindingError",
    "CandidateIdentityEvidence",
    "Clock",
    "CommandResult",
    "GitRunner",
    "bind_configured_candidate",
    "bind_fresh_origin_dev",
    "verify_bound_candidate",
]
