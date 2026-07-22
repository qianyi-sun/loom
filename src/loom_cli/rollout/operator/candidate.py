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

from .config import (
    CANDIDATE_CLUSTER_CONFIG,
    ConfigError,
    OperatorConfig,
    _read_protected_config,
    candidate_sha_from_runner_repo,
)
from .model import (
    APPROVED_FETCH_REF,
    APPROVED_REMOTE_URL,
    PINNED_TARGET_REF,
    CandidateBinding,
)

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_SYSTEM_PYTHON = Path("/usr/bin/python3")
_MAX_GIT_OUTPUT_BYTES = 1 << 20
# The root-side sealed-source validator lives outside the installed Python
# package.  Keep this runtime admission bound locked to the same value with a
# cross-layer contract test so the broker cannot reject a source the installer
# already accepted.
MAX_CUMULATIVE_COMMITS = 512


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


def _validate_protected_config(config: OperatorConfig) -> None:
    path = config.config_path
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
    try:
        payload = _read_protected_config(path, 0)
    except ConfigError as exc:
        raise CandidateBindingError("protected config cannot be read safely") from exc
    if (
        _SHA256_RE.fullmatch(config.config_sha256) is None
        or hashlib.sha256(payload).hexdigest() != config.config_sha256
    ):
        raise CandidateBindingError("protected config fingerprint drifted")


def _validate_root_directory(path: Path, *, label: str) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise CandidateBindingError(f"{label} is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise CandidateBindingError(f"{label} must not be a symlink: {path}")
    if not stat.S_ISDIR(metadata.st_mode):
        raise CandidateBindingError(f"{label} must be a directory: {path}")
    if metadata.st_uid != 0 or metadata.st_gid != 0:
        raise CandidateBindingError(f"{label} must be root-owned: {path}")
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise CandidateBindingError(f"{label} is group/world writable: {path}")
    if stat.S_IMODE(metadata.st_mode) & 0o005 != 0o005:
        raise CandidateBindingError(f"{label} is not readable by the service account: {path}")


def _validate_trusted_git_config(path: Path) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise CandidateBindingError(f"trusted git config is unavailable: {path}") from exc
    if stat.S_ISLNK(metadata.st_mode):
        raise CandidateBindingError(f"trusted git config must not be a symlink: {path}")
    if not stat.S_ISREG(metadata.st_mode):
        raise CandidateBindingError(f"trusted git config must be a regular file: {path}")
    if metadata.st_uid != 0 or metadata.st_gid != 0:
        raise CandidateBindingError("trusted git config must be root-owned")
    if stat.S_IMODE(metadata.st_mode) & 0o022:
        raise CandidateBindingError(f"trusted git config is group/world writable: {path}")
    if stat.S_IMODE(metadata.st_mode) & 0o004 != 0o004:
        raise CandidateBindingError(
            f"trusted git config is not readable by the service account: {path}"
        )


def _validate_runtime_python(path: Path) -> None:
    try:
        link_metadata = os.lstat(path)
        resolved = path.resolve(strict=True)
        expected = _SYSTEM_PYTHON.resolve(strict=True)
        metadata = os.lstat(resolved)
    except (OSError, RuntimeError) as exc:
        raise CandidateBindingError(f"candidate Python runtime is unavailable: {path}") from exc
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISLNK(link_metadata.st_mode)
        or link_metadata.st_uid != 0
        or link_metadata.st_gid != 0
        or resolved != expected
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or mode & 0o022
        or not stat.S_ISREG(metadata.st_mode)
        or mode & 0o005 != 0o005
    ):
        raise CandidateBindingError(f"candidate Python runtime is unsafe: {path}")


def _validate_immutable_runtime_tree(runtime_root: Path) -> None:
    """Validate the fixed broker boundary without rescanning repo and venv trees.

    The root installer validates and hardens every descendant exactly once,
    probes the installed package as the service UID, then issues the install
    attestation consumed by Tier 0.  These fixed non-writable roots preserve
    that proof while keeping every broker bind metadata-scale.
    """
    _validate_root_directory(runtime_root / "repo", label="trusted checkout path")
    _validate_root_directory(
        runtime_root / "repo" / ".git",
        label="trusted checkout Git directory",
    )
    _validate_root_directory(runtime_root / "venv", label="candidate Python runtime")
    _validate_root_directory(runtime_root / "venv" / "bin", label="candidate Python bin")
    _validate_runtime_python(runtime_root / "venv" / "bin" / "python")


def _configured_candidate_sha(config: OperatorConfig) -> str:
    try:
        return candidate_sha_from_runner_repo(config.runner_repo)
    except ConfigError as exc:
        raise CandidateBindingError("candidate runtime path is not exact") from exc


def _validate_installed_runtime(config: OperatorConfig) -> str:
    expected_sha = _configured_candidate_sha(config)
    service_uid = _service_uid(config)
    if service_uid <= 0:
        raise CandidateBindingError("configured service account must be non-root")
    runtime_root = config.runner_repo.parent
    try:
        resolved_repo = config.runner_repo.resolve(strict=True)
    except OSError as exc:
        raise CandidateBindingError("candidate runtime path is unavailable") from exc
    if resolved_repo != config.runner_repo:
        raise CandidateBindingError("candidate runtime path contains a symlink")
    if config.cluster_config_path != config.runner_repo / CANDIDATE_CLUSTER_CONFIG:
        raise CandidateBindingError("candidate cluster config is not bound to its runtime")
    if config.source_mode == "sealed-cumulative" and config.source_commit_sha != expected_sha:
        raise CandidateBindingError("sealed candidate runtime path drifted")
    _validate_root_directory(runtime_root.parent, label="candidate runtime authority root")
    _validate_root_directory(runtime_root, label="candidate runtime root")
    _validate_immutable_runtime_tree(runtime_root)
    _validate_trusted_git_config(config.runner_repo / ".git" / "config")
    return expected_sha


def _git_argv(repo: Path, *args: str) -> list[str]:
    return [
        "git",
        "--no-pager",
        "--no-replace-objects",
        "--no-optional-locks",
        "-c",
        f"safe.directory={repo}",
        "-c",
        "core.fsmonitor=false",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.untrackedCache=false",
        "-c",
        "submodule.recurse=false",
        "-C",
        str(repo),
        *args,
    ]


def _invoke(run: GitRunner, argv: list[str], *, operation: str) -> CommandResult:
    try:
        return run(argv)
    except OSError as exc:
        raise CandidateBindingError(f"git {operation} could not be executed") from exc


def _require_success(result: CommandResult, *, operation: str) -> str:
    if (
        type(result.returncode) is not int
        or not isinstance(result.stdout, str)
        or not isinstance(result.stderr, str)
        or len(result.stdout.encode()) > _MAX_GIT_OUTPUT_BYTES
        or len(result.stderr.encode()) > _MAX_GIT_OUTPUT_BYTES
    ):
        raise CandidateBindingError(f"git {operation} returned invalid evidence")
    if result.returncode != 0 or result.stderr != "":
        raise CandidateBindingError(f"git {operation} failed")
    return result.stdout


def _is_exact_absence(result: CommandResult) -> bool:
    return (
        type(result.returncode) is int
        and result.returncode == 1
        and result.stdout == ""
        and result.stderr == ""
    )


def _utc_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def bind_fresh_origin_dev(
    config: OperatorConfig,
    *,
    run: GitRunner,
    now: Clock,
) -> CandidateBinding:
    """Bind the exact merged candidate already published by the root installer.

    The historical function name is retained for the broker API.  Freshness is
    now established before installation; the service runtime is immutable and
    this boundary intentionally performs no fetch, checkout, or ref update.
    """
    if config.remote_url != APPROVED_REMOTE_URL:
        raise CandidateBindingError("config does not contain the approved remote URL")
    if config.target_ref != APPROVED_FETCH_REF:
        raise CandidateBindingError("config does not contain the approved target ref")
    resolved_sha = _configured_candidate_sha(config)
    binding = CandidateBinding(
        remote_url=APPROVED_REMOTE_URL,
        target_ref=PINNED_TARGET_REF,
        resolved_sha=resolved_sha,
        image_tag=f"staging-{resolved_sha[:7]}",
        fetched_at=_utc_timestamp(now()),
    )
    verify_bound_candidate(config, binding, run=run)
    return binding


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
    _validate_protected_config(config)
    expected_sha = _validate_installed_runtime(config)
    if binding.resolved_sha != expected_sha:
        raise CandidateBindingError("candidate binding does not match its immutable runtime path")

    def output(*args: str, operation: str) -> str:
        return _require_success(
            _invoke(run, _git_argv(config.runner_repo, *args), operation=operation),
            operation=operation,
        )

    if output("remote", operation="remote inspection") not in {"origin", "origin\n"}:
        raise CandidateBindingError("trusted checkout must have only remote origin")
    if output(
        "remote",
        "get-url",
        "--all",
        "origin",
        operation="origin fetch URL inspection",
    ) not in {APPROVED_REMOTE_URL, f"{APPROVED_REMOTE_URL}\n"}:
        raise CandidateBindingError("origin must have exactly one approved fetch URL")
    pushurl = _invoke(
        run,
        _git_argv(config.runner_repo, "config", "--get-all", "remote.origin.pushurl"),
        operation="origin pushurl inspection",
    )
    if not _is_exact_absence(pushurl):
        raise CandidateBindingError("remote.origin.pushurl must be absent")
    symbolic = _invoke(
        run,
        _git_argv(config.runner_repo, "symbolic-ref", "-q", "HEAD"),
        operation="HEAD mode inspection",
    )
    if not _is_exact_absence(symbolic):
        raise CandidateBindingError("candidate runtime must use detached HEAD")
    observed_head_output = output(
        "rev-parse",
        "--verify",
        "HEAD^{commit}",
        operation="commit inspection",
    )
    if not re.fullmatch(r"[0-9a-f]{40}\n?", observed_head_output):
        raise CandidateBindingError("resolved SHA must be exactly 40 lowercase hexadecimal chars")
    observed_head = observed_head_output.removesuffix("\n")
    if observed_head != expected_sha:
        raise CandidateBindingError("candidate runtime commit identity drifted")
    resolved_tree_output = output(
        "rev-parse",
        "--verify",
        "HEAD^{tree}",
        operation="tree inspection",
    )
    if not re.fullmatch(r"[0-9a-f]{40}\n?", resolved_tree_output):
        raise CandidateBindingError("resolved tree must be exactly 40 lowercase hexadecimal chars")
    resolved_tree = resolved_tree_output.removesuffix("\n")
    history_count = 0
    approved_base: str | None = None
    if binding.source_mode == "sealed-cumulative":
        if (
            config.source_mode != "sealed-cumulative"
            or config.source_commit_sha != binding.resolved_sha
            or config.source_tree_sha != binding.resolved_tree
            or config.source_base_sha != binding.approved_base_sha
            or binding.resolved_tree is None
            or binding.approved_base_sha is None
        ):
            raise CandidateBindingError("sealed candidate config binding drifted")
        if resolved_tree != binding.resolved_tree:
            raise CandidateBindingError("sealed candidate tree identity drifted")
        if (
            output(
                "merge-base",
                binding.approved_base_sha,
                binding.resolved_sha,
                operation="base inspection",
            ).removesuffix("\n")
            != binding.approved_base_sha
        ):
            raise CandidateBindingError("sealed candidate base identity drifted")
        history_output = output(
            "rev-list",
            "--reverse",
            "--parents",
            f"{binding.approved_base_sha}..{binding.resolved_sha}",
            operation="history inspection",
        )
        history = history_output.splitlines()
        expected_parent = binding.approved_base_sha
        for line in history:
            fields = line.split(" ")
            if (
                len(fields) != 2
                or _SHA_RE.fullmatch(fields[0]) is None
                or _SHA_RE.fullmatch(fields[1]) is None
                or fields[1] != expected_parent
            ):
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

    digest_payload = {
        "approved_base_sha": approved_base,
        "config_sha256": config.config_sha256,
        "image_tag": binding.image_tag,
        "linear_history_count": history_count,
        "resolved_sha": binding.resolved_sha,
        "resolved_tree": resolved_tree,
        "runtime_root": str(config.runner_repo.parent),
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
