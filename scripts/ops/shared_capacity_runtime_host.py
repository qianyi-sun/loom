#!/usr/bin/env python3
"""Install the exact-candidate shared-capacity supervisor and adapters.

Public mutation commands are plan-only unless ``--execute`` is supplied.  The
live path is fixed to oldlab2 and installs only the six sandbox adapter
instances plus the broker supervisor.  Capacity remains fail-closed until a
valid broker handoff and runtime receipt reach an adapter.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PROFILE = REPO_ROOT / "deploy/developer-sandboxes/shared-capacity-runtime-host.toml"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
EXPECTED_HOSTNAME = "trt-eai-oldlab-2"
PROGRAM_PATH = Path("/usr/local/libexec/loom-shared-capacity-runtime-host")
PROFILE_PATH = Path("/etc/loom/shared-capacity-runtime-host.toml")
CONFIG_ROOT = Path("/etc/loom")
ADAPTER_CONFIG_ROOT = CONFIG_ROOT / "shared-capacity-adapters"
SUPERVISOR_CONFIG_PATH = CONFIG_ROOT / "shared-capacity-supervisor.toml"
STATE_ROOT = Path("/var/lib/loom-shared-capacity")
INSTALLER_ROOT = STATE_ROOT / "runtime-host-installer"
JOURNAL_ROOT = INSTALLER_ROOT / "transactions"
STATE_PATH = INSTALLER_ROOT / "state.json"
ACTIVE_JOURNAL_PATH = INSTALLER_ROOT / "active-transaction.json"
RECOVERY_PROGRAM_PATH = INSTALLER_ROOT / "runtime-host-recovery"
LOCK_PATH = Path("/run/loom-shared-capacity-runtime-host.lock")
CANDIDATE_PARENT = Path("/opt/loom-shared-capacity/candidates")
CURRENT_LINK = Path("/opt/loom-shared-capacity/current")
ADAPTER_SERVICE_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-adapter@.service",
)
ADAPTER_TIMER_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-adapter@.timer",
)
SUPERVISOR_SERVICE_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-supervisor.service",
)
SUPERVISOR_TIMER_PATH = Path(
    "/etc/systemd/system/loom-shared-capacity-supervisor.timer",
)
ADAPTER_SERVICE_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-adapter@.service"
)
ADAPTER_TIMER_SOURCE = REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-adapter@.timer"
SUPERVISOR_SERVICE_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-supervisor.service"
)
SUPERVISOR_TIMER_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/loom-shared-capacity-supervisor.timer"
)
SUPERVISOR_CONFIG_SOURCE = (
    REPO_ROOT / "deploy/developer-sandboxes/shared-capacity-supervisor/config.toml"
)
ADAPTER_CONFIG_SOURCE_ROOT = REPO_ROOT / "deploy/developer-sandboxes/shared-capacity-adapters"
INSTANCES = (
    "qianyi-gb10",
    "qianyi-oldlab",
    "hongjian-gb10",
    "hongjian-oldlab",
    "devansh-gb10",
    "devansh-oldlab",
)
SUPERVISOR_SERVICE = "loom-shared-capacity-supervisor.service"
SUPERVISOR_TIMER = "loom-shared-capacity-supervisor.timer"
ADAPTER_TIMERS = tuple(f"loom-shared-capacity-adapter@{instance}.timer" for instance in INSTANCES)
ADAPTER_SERVICES = tuple(
    f"loom-shared-capacity-adapter@{instance}.service" for instance in INSTANCES
)
ALL_TIMERS = (SUPERVISOR_TIMER, *ADAPTER_TIMERS)
ALL_SERVICES = (SUPERVISOR_SERVICE, *ADAPTER_SERVICES)
ALL_UNITS = (*ALL_TIMERS, *ALL_SERVICES)
RETIREMENT_MAX_CYCLES = 60
RETIREMENT_POLL_SECONDS = 5.0
UNIT_PATHS = (
    ADAPTER_SERVICE_PATH,
    ADAPTER_TIMER_PATH,
    SUPERVISOR_SERVICE_PATH,
    SUPERVISOR_TIMER_PATH,
)
UNIT_FRAGMENT_PATHS = {
    SUPERVISOR_SERVICE: SUPERVISOR_SERVICE_PATH,
    SUPERVISOR_TIMER: SUPERVISOR_TIMER_PATH,
    **{service: ADAPTER_SERVICE_PATH for service in ADAPTER_SERVICES},
    **{timer: ADAPTER_TIMER_PATH for timer in ADAPTER_TIMERS},
}
REQUIRED_UNIT_FILES = {
    ADAPTER_SERVICE_PATH.name,
    ADAPTER_TIMER_PATH.name,
    SUPERVISOR_SERVICE_PATH.name,
    SUPERVISOR_TIMER_PATH.name,
}


class RuntimeHostError(RuntimeError):
    """The requested host convergence could not be completed safely."""


@dataclass(frozen=True, slots=True)
class Candidate:
    sha: str
    tree: str
    source: Path

    @property
    def root(self) -> Path:
        return CANDIDATE_PARENT / self.sha

    @property
    def repo(self) -> Path:
        return self.root / "repo"

    @property
    def venv(self) -> Path:
        return self.root / "venv"


def _run(
    argv: Sequence[str],
    *,
    env: Mapping[str, str] | None = None,
    expected: set[int] | frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        env=dict(env) if env is not None else None,
    )
    if completed.returncode not in expected:
        name = Path(argv[0]).name if argv else "command"
        raise RuntimeHostError(
            f"{name} failed safely with exit code {completed.returncode}",
        )
    return completed


def _git_environment() -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C",
        "LC_ALL": "C",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_ATTR_NOSYSTEM": "1",
        "GIT_OPTIONAL_LOCKS": "0",
        "GIT_NO_REPLACE_OBJECTS": "1",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_PROTOCOL_FROM_USER": "0",
        "GIT_PAGER": "cat",
        "GIT_EXTERNAL_DIFF": "/usr/bin/false",
        "GIT_SSH_COMMAND": "/usr/bin/false",
        "HOME": "/nonexistent",
        "XDG_CONFIG_HOME": "/nonexistent",
    }


def _git_raw(repo: Path, *args: str) -> bytes:
    completed = subprocess.run(
        (
            "git",
            "--no-replace-objects",
            "-c",
            f"safe.directory={repo}",
            "-c",
            "credential.helper=",
            "-c",
            "core.sshCommand=/usr/bin/false",
            "-c",
            "fetch.recurseSubmodules=false",
            "-C",
            str(repo),
            *args,
        ),
        check=False,
        capture_output=True,
        text=False,
        env=_git_environment(),
    )
    if completed.returncode != 0:
        raise RuntimeHostError(
            f"git failed safely with exit code {completed.returncode}",
        )
    return completed.stdout


def _git(repo: Path, *args: str) -> str:
    try:
        return _git_raw(repo, *args).decode("utf-8", errors="strict").strip()
    except UnicodeDecodeError as exc:
        raise RuntimeHostError("git output is invalid") from exc


def _repository_entries(
    raw: bytes,
    *,
    tree: bool,
) -> dict[str, tuple[str, str]]:
    entries: dict[str, tuple[str, str]] = {}
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        metadata, separator, encoded_path = encoded.partition(b"\t")
        fields = metadata.split()
        try:
            relative = encoded_path.decode("utf-8", errors="strict")
            ascii_fields = [field.decode("ascii", errors="strict") for field in fields]
        except UnicodeDecodeError as exc:
            raise RuntimeHostError("repository metadata is invalid") from exc
        path = PurePosixPath(relative)
        if tree:
            valid_metadata = (
                len(ascii_fields) == 3
                and ascii_fields[0] in {"100644", "100755", "120000"}
                and ascii_fields[1] == "blob"
                and SHA_RE.fullmatch(ascii_fields[2]) is not None
            )
            value = (
                (
                    ascii_fields[0],
                    ascii_fields[2],
                )
                if valid_metadata
                else ("", "")
            )
        else:
            valid_metadata = (
                len(ascii_fields) == 3
                and ascii_fields[0] in {"100644", "100755", "120000"}
                and SHA_RE.fullmatch(ascii_fields[1]) is not None
                and ascii_fields[2] == "0"
            )
            value = (
                (
                    ascii_fields[0],
                    ascii_fields[1],
                )
                if valid_metadata
                else ("", "")
            )
        if (
            separator != b"\t"
            or not valid_metadata
            or not relative
            or path.is_absolute()
            or ".." in path.parts
            or path.parts[0] == ".git"
            or relative in entries
        ):
            raise RuntimeHostError("repository metadata is invalid")
        entries[relative] = value
    if not entries:
        raise RuntimeHostError("repository metadata is empty")
    return entries


def _reject_git_indirection(repo: Path) -> None:
    if _git(repo, "rev-parse", "--is-shallow-repository") != "false":
        raise RuntimeHostError("repository history is not self-contained")
    if _git(repo, "replace", "-l"):
        raise RuntimeHostError("repository replacement objects are forbidden")
    for git_path in ("objects/info/alternates", "info/grafts", "shallow"):
        resolved = Path(_git(repo, "rev-parse", "--git-path", git_path))
        if resolved.exists() or resolved.is_symlink():
            raise RuntimeHostError("repository object indirection is forbidden")


def _validate_repository(repo: Path, sha: str) -> str:
    if _git(repo, "rev-parse", "--verify", "HEAD") != sha:
        raise RuntimeHostError("candidate source HEAD does not match requested SHA")
    if _git(repo, "rev-parse", "--verify", f"{sha}^{{commit}}") != sha:
        raise RuntimeHostError("candidate commit does not resolve exactly")
    _reject_git_indirection(repo)
    index = _repository_entries(
        _git_raw(repo, "ls-files", "--stage", "-z", "--"),
        tree=False,
    )
    commit_tree = _repository_entries(
        _git_raw(repo, "ls-tree", "-r", "-z", "--full-tree", sha),
        tree=True,
    )
    if index != commit_tree:
        raise RuntimeHostError("candidate index does not match the commit tree")
    flag_rows = [row for row in _git_raw(repo, "ls-files", "-v", "-z", "--").split(b"\0") if row]
    if len(flag_rows) != len(index) or any(not row.startswith(b"H ") for row in flag_rows):
        raise RuntimeHostError("candidate index flags are unsafe")
    _git(repo, "diff-files", "--quiet", "--")
    _git(repo, "diff-index", "--quiet", sha, "--")
    if _git(repo, "status", "--porcelain=v1", "--untracked-files=all"):
        raise RuntimeHostError("candidate source is not clean")
    tree_sha = _git(repo, "rev-parse", "--verify", f"{sha}^{{tree}}")
    if SHA_RE.fullmatch(tree_sha) is None:
        raise RuntimeHostError("candidate tree is invalid")
    return tree_sha


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonical_json(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(dict(payload), sort_keys=True, separators=(",", ":")) + "\n").encode()


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_directory(path: Path, *, mode: int) -> None:
    path.mkdir(parents=True, exist_ok=True)
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeHostError(f"unsafe directory: {path}")
    os.chown(path, 0, 0)
    os.chmod(path, mode)


def _atomic_write(path: Path, content: bytes, *, mode: int) -> None:
    private_parent = path.parent == STATE_ROOT or STATE_ROOT in path.parent.parents
    _ensure_directory(path.parent, mode=0o700 if private_parent else 0o755)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chown(temporary, 0, 0)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _atomic_symlink(path: Path, target: str) -> None:
    _ensure_directory(path.parent, mode=0o755)
    temporary = path.parent / f".{path.name}.{uuid.uuid4().hex}.tmp"
    try:
        temporary.symlink_to(target)
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _remove_path(path: Path) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
        shutil.rmtree(path)
    else:
        path.unlink()
    _fsync_directory(path.parent)


def _validate_profile_bytes(content: bytes) -> dict[str, Any]:
    try:
        raw = tomllib.loads(content.decode("utf-8", errors="strict"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeHostError("runtime-host profile is unavailable") from exc
    expected = {
        "schema_version",
        "expected_hostname",
        "candidate_parent",
        "current_link",
        "state_root",
        "adapter_instances",
        "slurm_domains",
    }
    expected_domains = {
        "oldlab": {
            "submit_host": "trt-EAI-OLDLAB-2",
            "controller": "TRT-EAI-OLDLAB-1",
        },
        "gb10": {
            "submit_host": "trt-gb10-1",
            "controller": "trt-gb10-1",
        },
    }
    if (
        set(raw) != expected
        or raw.get("schema_version") != 1
        or raw.get("expected_hostname") != EXPECTED_HOSTNAME
        or raw.get("candidate_parent") != str(CANDIDATE_PARENT)
        or raw.get("current_link") != str(CURRENT_LINK)
        or raw.get("state_root") != str(STATE_ROOT)
        or raw.get("adapter_instances") != list(INSTANCES)
        or raw.get("slurm_domains") != expected_domains
    ):
        raise RuntimeHostError("runtime-host profile drifted from the closed contract")
    return raw


def _load_profile(path: Path = SOURCE_PROFILE) -> dict[str, Any]:
    try:
        return _validate_profile_bytes(path.read_bytes())
    except OSError as exc:
        raise RuntimeHostError("runtime-host profile is unavailable") from exc


def _load_candidate_profile(candidate: Candidate) -> dict[str, Any]:
    return _validate_profile_bytes(
        _read_candidate_file(
            candidate,
            SOURCE_PROFILE.relative_to(REPO_ROOT),
        ),
    )


def _candidate_identity(source: Path, sha: str) -> Candidate:
    if SHA_RE.fullmatch(sha) is None or not source.is_absolute():
        raise RuntimeHostError("candidate source and full SHA are required")
    try:
        metadata = source.lstat()
    except OSError as exc:
        raise RuntimeHostError("candidate source is unavailable") from exc
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeHostError("candidate source must be a non-symlink directory")
    tree = _validate_repository(source, sha)
    candidate = Candidate(sha=sha, tree=tree, source=source)
    _read_candidate_file(candidate, Path("uv.lock"))
    _load_candidate_profile(candidate)
    return candidate


def _read_candidate_file(candidate: Candidate, relative: Path) -> bytes:
    raw_entry = _git_raw(
        candidate.source,
        "ls-tree",
        "-z",
        candidate.sha,
        "--",
        relative.as_posix(),
    )
    entries = _repository_entries(raw_entry, tree=True)
    entry = entries.get(relative.as_posix())
    if len(entries) != 1 or entry is None or entry[0] not in {"100644", "100755"}:
        raise RuntimeHostError("candidate deployment asset is unsafe")
    return _git_raw(
        candidate.source,
        "cat-file",
        "blob",
        f"{candidate.sha}:{relative.as_posix()}",
    )


def _render_service(template: bytes, candidate: Candidate) -> bytes:
    token = b"${GIT_SHA}"
    if template.count(token) != 4:
        raise RuntimeHostError("service template placeholder count is invalid")
    rendered = template.replace(token, candidate.sha.encode())
    exact_root = str(candidate.root).encode()
    if token in rendered or exact_root not in rendered:
        raise RuntimeHostError("service did not render to the exact candidate")
    if b"/opt/loom-shared-capacity/current" in rendered:
        raise RuntimeHostError("service references a mutable candidate pointer")
    return rendered


def _desired_files(candidate: Candidate) -> dict[Path, tuple[bytes, int]]:
    files: dict[Path, tuple[bytes, int]] = {
        PROGRAM_PATH: (
            _read_candidate_file(
                candidate,
                Path("scripts/ops/shared_capacity_runtime_host.py"),
            ),
            0o755,
        ),
        PROFILE_PATH: (
            _read_candidate_file(
                candidate,
                SOURCE_PROFILE.relative_to(REPO_ROOT),
            ),
            0o600,
        ),
        SUPERVISOR_CONFIG_PATH: (
            _read_candidate_file(
                candidate,
                SUPERVISOR_CONFIG_SOURCE.relative_to(REPO_ROOT),
            ),
            0o600,
        ),
        ADAPTER_SERVICE_PATH: (
            _render_service(
                _read_candidate_file(
                    candidate,
                    ADAPTER_SERVICE_SOURCE.relative_to(REPO_ROOT),
                ),
                candidate,
            ),
            0o644,
        ),
        ADAPTER_TIMER_PATH: (
            _read_candidate_file(
                candidate,
                ADAPTER_TIMER_SOURCE.relative_to(REPO_ROOT),
            ),
            0o644,
        ),
        SUPERVISOR_SERVICE_PATH: (
            _render_service(
                _read_candidate_file(
                    candidate,
                    SUPERVISOR_SERVICE_SOURCE.relative_to(REPO_ROOT),
                ),
                candidate,
            ),
            0o644,
        ),
        SUPERVISOR_TIMER_PATH: (
            _read_candidate_file(
                candidate,
                SUPERVISOR_TIMER_SOURCE.relative_to(REPO_ROOT),
            ),
            0o644,
        ),
    }
    for instance in INSTANCES:
        relative = ADAPTER_CONFIG_SOURCE_ROOT.relative_to(REPO_ROOT) / f"{instance}.toml"
        files[ADAPTER_CONFIG_ROOT / f"{instance}.toml"] = (
            _read_candidate_file(candidate, relative),
            0o600,
        )
    return files


def plan(candidate: Candidate, operation: str) -> dict[str, Any]:
    desired = _desired_files(candidate)
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-runtime-host-plan",
        "operation": operation,
        "mutation_authorized": False,
        "host": EXPECTED_HOSTNAME,
        "candidate_sha": candidate.sha,
        "candidate_tree": candidate.tree,
        "candidate_root": str(candidate.root),
        "lockfile_sha256": _sha256(
            _read_candidate_file(candidate, Path("uv.lock")),
        ),
        "instances": list(INSTANCES),
        "slurm_domains": _load_candidate_profile(candidate)["slurm_domains"],
        "files": [
            {
                "path": str(path),
                "mode": f"{mode:04o}",
                "sha256": _sha256(content),
            }
            for path, (content, mode) in sorted(
                desired.items(),
                key=lambda item: str(item[0]),
            )
        ],
        "activation_order": [
            "journal-before-any-opt-or-systemd-mutation",
            "stop-and-disable-existing-services-and-timers",
            "materialize-exact-repo-and-frozen-venv",
            "publish-configs-and-exact-units",
            "leave-supervisor-and-six-adapters-disabled-and-inactive",
            "closed-world-readback",
        ],
        "capacity_enabled_by_installer": False,
    }


def _require_live_host() -> None:
    if os.geteuid() != 0:
        raise RuntimeHostError("live convergence requires root")
    hostname = socket.gethostname().split(".", 1)[0].rstrip(".").lower()
    if hostname != EXPECTED_HOSTNAME:
        raise RuntimeHostError("live convergence is restricted to oldlab2")


@contextmanager
def _lock() -> Iterator[None]:
    descriptor = os.open(
        LOCK_PATH,
        os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        0o600,
    )
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != 0 or metadata.st_gid != 0:
            raise RuntimeHostError("installer lock metadata is invalid")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        os.close(descriptor)


def _systemctl_state(unit: str) -> dict[str, bool]:
    enabled_result = _run(
        ("systemctl", "is-enabled", unit),
        expected={0, 1, 3, 4},
    )
    enabled = enabled_result.stdout.strip() in {"enabled", "enabled-runtime"}
    active_result = _run(
        ("systemctl", "is-active", unit),
        expected={0, 3, 4},
    )
    active = active_result.stdout.strip() == "active"
    return {"enabled": enabled, "active": active}


_MANAGED_UNIT_RE = re.compile(
    r"^loom-shared-capacity-(?:"
    r"supervisor\.(?:service|timer)|"
    r"adapter@([a-z0-9-]*)\.(?:service|timer)"
    r")$",
)


def _validate_managed_unit_rows(rows: Sequence[str]) -> set[str]:
    names = [row.split(maxsplit=1)[0] for row in rows if row.strip()]
    if len(names) != len(set(names)):
        raise RuntimeHostError("duplicate managed systemd unit readback")
    managed: set[str] = set()
    for name in names:
        match = _MANAGED_UNIT_RE.fullmatch(name)
        if match is None:
            raise RuntimeHostError("orphan shared-capacity systemd unit exists")
        instance = match.group(1)
        if instance not in {None, "", *INSTANCES}:
            raise RuntimeHostError("orphan shared-capacity adapter unit is loaded")
        managed.add(name)
    return managed


def _loaded_managed_units() -> set[str]:
    rows = _run(
        (
            "systemctl",
            "list-units",
            "--all",
            "--plain",
            "--no-legend",
            "loom-shared-capacity-*",
        ),
    ).stdout.splitlines()
    return _validate_managed_unit_rows(rows)


def _installed_managed_unit_files() -> set[str]:
    rows = _run(
        (
            "systemctl",
            "list-unit-files",
            "--all",
            "--plain",
            "--no-legend",
            "loom-shared-capacity-*",
        ),
    ).stdout.splitlines()
    return _validate_managed_unit_rows(rows)


def _unit_fragment(unit: str) -> tuple[str, str]:
    load_state = _run(
        ("systemctl", "show", unit, "--property=LoadState", "--value"),
    ).stdout.strip()
    fragment_path = _run(
        ("systemctl", "show", unit, "--property=FragmentPath", "--value"),
    ).stdout.strip()
    return load_state, fragment_path


def _validate_unit_fragment(unit: str, expected_path: Path) -> None:
    load_state, fragment_path = _unit_fragment(unit)
    if load_state != "loaded" or fragment_path != str(expected_path):
        raise RuntimeHostError(f"managed systemd fragment drifted: {unit}")


def _reject_orphan_unit_files() -> None:
    installed = _installed_managed_unit_files()
    if not REQUIRED_UNIT_FILES <= installed:
        raise RuntimeHostError("required shared-capacity unit file is missing")
    allowed = {*REQUIRED_UNIT_FILES, *ALL_UNITS}
    if installed - allowed:
        raise RuntimeHostError("orphan shared-capacity unit file is installed")


def _reject_orphan_configs() -> None:
    if not ADAPTER_CONFIG_ROOT.exists():
        return
    expected = {f"{instance}.toml" for instance in INSTANCES}
    actual = {
        path.name for path in ADAPTER_CONFIG_ROOT.iterdir() if path.is_file() or path.is_symlink()
    }
    if actual - expected:
        raise RuntimeHostError("orphan shared-capacity adapter config is installed")


def _capture_files(paths: Sequence[Path]) -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for path in paths:
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            result[str(path)] = {"present": False}
            continue
        if stat.S_ISLNK(metadata.st_mode):
            result[str(path)] = {
                "present": True,
                "kind": "symlink",
                "target": os.readlink(path),
            }
        elif stat.S_ISREG(metadata.st_mode):
            result[str(path)] = {
                "present": True,
                "kind": "file",
                "mode": stat.S_IMODE(metadata.st_mode),
                "content_b64": base64.b64encode(path.read_bytes()).decode(),
            }
        else:
            raise RuntimeHostError(f"cannot snapshot unsafe path: {path}")
    return result


def _snapshot_paths() -> tuple[Path, ...]:
    return (
        PROGRAM_PATH,
        PROFILE_PATH,
        SUPERVISOR_CONFIG_PATH,
        *(ADAPTER_CONFIG_ROOT / f"{instance}.toml" for instance in INSTANCES),
        *UNIT_PATHS,
        CURRENT_LINK,
        STATE_PATH,
    )


def _write_journal(
    candidate: Candidate,
    *,
    operation: str,
) -> tuple[Path, dict[str, Any]]:
    if operation not in {"install", "activate"}:
        raise RuntimeHostError("runtime-host transaction operation is invalid")
    transaction_id = uuid.uuid4().hex
    _ensure_directory(STATE_ROOT, mode=0o700)
    _ensure_directory(INSTALLER_ROOT, mode=0o700)
    _ensure_directory(JOURNAL_ROOT, mode=0o700)
    path = JOURNAL_ROOT / f"{transaction_id}.json"
    staging_path = CANDIDATE_PARENT / f".install-{candidate.sha}-{transaction_id}"
    payload: dict[str, Any] = {
        "schema_version": 1,
        "transaction_id": transaction_id,
        "operation": operation,
        "phase": "prepared",
        "candidate_sha": candidate.sha,
        "candidate_tree": candidate.tree,
        "candidate_previously_existed": (candidate.root.exists() or candidate.root.is_symlink()),
        "staging_path": str(staging_path),
        "started_at": datetime.now(UTC).isoformat(),
        "files": _capture_files(_snapshot_paths()),
        "units": {unit: _systemctl_state(unit) for unit in ALL_UNITS},
    }
    _atomic_write(path, _canonical_json(payload), mode=0o600)
    _atomic_write(
        ACTIVE_JOURNAL_PATH,
        _canonical_json({"transaction_id": transaction_id}),
        mode=0o600,
    )
    return path, payload


def _update_journal(path: Path, payload: dict[str, Any], phase: str) -> None:
    payload["phase"] = phase
    _atomic_write(path, _canonical_json(payload), mode=0o600)


def _prepare_rollback_recovery(
    path: Path,
    payload: dict[str, Any],
    candidate: Candidate,
) -> None:
    content = _read_candidate_file(
        candidate,
        Path("scripts/ops/shared_capacity_runtime_host.py"),
    )
    try:
        compile(content, str(RECOVERY_PROGRAM_PATH), "exec")
    except (SyntaxError, ValueError) as exc:
        raise RuntimeHostError("rollback recovery program is invalid") from exc
    _atomic_write(RECOVERY_PROGRAM_PATH, content, mode=0o700)
    payload["rollback_recovery_path"] = str(RECOVERY_PROGRAM_PATH)
    payload["rollback_recovery_sha256"] = _sha256(content)
    _update_journal(path, payload, "rollback-recovery-ready")


def _validate_rollback_recovery(payload: Mapping[str, Any]) -> None:
    digest = payload.get("rollback_recovery_sha256")
    if (
        payload.get("rollback_recovery_path") != str(RECOVERY_PROGRAM_PATH)
        or not isinstance(digest, str)
        or re.fullmatch(r"[0-9a-f]{64}", digest) is None
    ):
        raise RuntimeHostError("rollback recovery binding is invalid")
    try:
        metadata = RECOVERY_PROGRAM_PATH.lstat()
        content = RECOVERY_PROGRAM_PATH.read_bytes()
    except OSError as exc:
        raise RuntimeHostError("rollback recovery program is unavailable") from exc
    if (
        stat.S_ISLNK(metadata.st_mode)
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_nlink != 1
        or metadata.st_uid != os.geteuid()
        or metadata.st_gid != os.getegid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or _sha256(content) != digest
    ):
        raise RuntimeHostError("rollback recovery program is unsafe or drifted")
    try:
        compile(content, str(RECOVERY_PROGRAM_PATH), "exec")
    except (SyntaxError, ValueError) as exc:
        raise RuntimeHostError("rollback recovery program is invalid") from exc


def _load_json(path: Path, label: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeHostError(f"{label} is unavailable") from exc
    if not isinstance(payload, dict):
        raise RuntimeHostError(f"{label} is invalid")
    return payload


def _active_journal() -> tuple[Path, dict[str, Any]] | None:
    if not ACTIVE_JOURNAL_PATH.exists():
        return None
    pointer = _load_json(ACTIVE_JOURNAL_PATH, "active transaction pointer")
    transaction_id = pointer.get("transaction_id")
    if not isinstance(transaction_id, str) or not re.fullmatch(r"[0-9a-f]{32}", transaction_id):
        raise RuntimeHostError("active transaction pointer is invalid")
    path = JOURNAL_ROOT / f"{transaction_id}.json"
    return path, _load_json(path, "runtime-host transaction")


def _stop_units() -> None:
    for unit in ALL_TIMERS:
        _run(("systemctl", "stop", unit), expected={0, 5})
    for unit in ALL_SERVICES:
        _run(("systemctl", "stop", unit), expected={0, 5})


def _restore_files(snapshot: Mapping[str, Any]) -> None:
    for raw_path, raw in snapshot.items():
        path = Path(raw_path)
        if not isinstance(raw, dict) or raw.get("present") is not True:
            _remove_path(path)
            continue
        kind = raw.get("kind")
        if kind == "symlink":
            target = raw.get("target")
            if not isinstance(target, str):
                raise RuntimeHostError("transaction symlink snapshot is invalid")
            _remove_path(path)
            _atomic_symlink(path, target)
        elif kind == "file":
            content = raw.get("content_b64")
            mode = raw.get("mode")
            if not isinstance(content, str) or type(mode) is not int:
                raise RuntimeHostError("transaction file snapshot is invalid")
            _atomic_write(path, base64.b64decode(content, validate=True), mode=mode)
        else:
            raise RuntimeHostError("transaction snapshot kind is invalid")


def _restore_units(states: Mapping[str, Any]) -> None:
    _run(("systemctl", "daemon-reload"))
    for unit in ALL_UNITS:
        state = states.get(unit)
        if not isinstance(state, dict):
            raise RuntimeHostError("transaction unit snapshot is invalid")
        if state.get("enabled") is True:
            _run(("systemctl", "enable", unit), expected={0, 1})
        else:
            _run(("systemctl", "disable", unit), expected={0, 1, 5})
        if state.get("active") is True:
            _run(("systemctl", "start", unit))
        else:
            _run(("systemctl", "stop", unit), expected={0, 5})


def _validate_transaction(
    path: Path,
    payload: dict[str, Any],
) -> tuple[str, str, str, str, Mapping[str, Any], Mapping[str, Any]]:
    transaction_id = payload.get("transaction_id")
    operation = payload.get("operation")
    sha = payload.get("candidate_sha")
    tree = payload.get("candidate_tree")
    staging_path = payload.get("staging_path")
    if (
        not isinstance(transaction_id, str)
        or re.fullmatch(r"[0-9a-f]{32}", transaction_id) is None
        or path != JOURNAL_ROOT / f"{transaction_id}.json"
        or operation not in {"install", "activate"}
        or not isinstance(sha, str)
        or SHA_RE.fullmatch(sha) is None
        or not isinstance(tree, str)
        or SHA_RE.fullmatch(tree) is None
        or type(payload.get("candidate_previously_existed")) is not bool
        or staging_path != str(CANDIDATE_PARENT / f".install-{sha}-{transaction_id}")
    ):
        raise RuntimeHostError("runtime-host transaction ownership is invalid")
    files = payload.get("files")
    units = payload.get("units")
    if (
        not isinstance(files, dict)
        or set(files) != {str(item) for item in _snapshot_paths()}
        or not isinstance(units, dict)
        or set(units) != set(ALL_UNITS)
    ):
        raise RuntimeHostError("runtime-host transaction snapshot is invalid")
    for state in units.values():
        if (
            not isinstance(state, dict)
            or set(state) != {"enabled", "active"}
            or type(state.get("enabled")) is not bool
            or type(state.get("active")) is not bool
        ):
            raise RuntimeHostError("runtime-host transaction unit state is invalid")
    return transaction_id, operation, sha, tree, files, units


def _restore_local_transaction(
    path: Path,
    payload: dict[str, Any],
    *,
    remove_candidate: bool,
) -> tuple[str, str, str, str]:
    transaction_id, operation, sha, tree, files, units = _validate_transaction(
        path,
        payload,
    )
    _stop_units()
    _remove_path(Path(str(payload["staging_path"])))
    _restore_files(files)
    _restore_units(units)
    if remove_candidate and payload.get("candidate_previously_existed") is False:
        _remove_path(CANDIDATE_PARENT / sha)
    return transaction_id, operation, sha, tree


def _restore_transaction(
    path: Path,
    payload: dict[str, Any],
) -> None:
    transaction_id, operation, sha, tree = _restore_local_transaction(
        path,
        payload,
        remove_candidate=True,
    )
    _update_journal(path, payload, "rolled-back")
    if operation == "activate":
        _open_activation_admission(
            Candidate(
                sha=sha,
                tree=tree,
                source=CANDIDATE_PARENT / sha / "repo",
            ),
            transaction_id,
        )
    ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
    _fsync_directory(INSTALLER_ROOT)


def _recover_orphan() -> None:
    active = _active_journal()
    if active is None:
        return
    path, payload = active
    if (
        payload.get("operation") == "install"
        and str(payload.get("phase", "")).startswith("rollback-")
    ):
        _resume_activated_rollback(path, payload)
        return
    if payload.get("phase") == "committed":
        if payload.get("operation") == "activate":
            sha = payload.get("candidate_sha")
            tree = payload.get("candidate_tree")
            transaction_id = payload.get("transaction_id")
            if (
                not isinstance(sha, str)
                or SHA_RE.fullmatch(sha) is None
                or not isinstance(tree, str)
                or SHA_RE.fullmatch(tree) is None
                or not isinstance(transaction_id, str)
                or re.fullmatch(r"[0-9a-f]{32}", transaction_id) is None
            ):
                raise RuntimeHostError("committed activation journal is invalid")
            _open_activation_admission(
                Candidate(
                    sha=sha,
                    tree=tree,
                    source=CANDIDATE_PARENT / sha / "repo",
                ),
                transaction_id,
            )
        ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
        _fsync_directory(INSTALLER_ROOT)
        return
    _restore_transaction(path, payload)


def _reject_orphan_stages() -> None:
    if not CANDIDATE_PARENT.exists():
        return
    for path in CANDIDATE_PARENT.iterdir():
        if path.name.startswith(".install-"):
            raise RuntimeHostError("unjournaled candidate staging path exists")


def _make_read_only(root: Path) -> None:
    for current, directories, files in os.walk(root, followlinks=False):
        current_path = Path(current)
        os.chown(current_path, 0, 0)
        os.chmod(current_path, stat.S_IMODE(current_path.stat().st_mode) & ~0o222)
        for name in (*directories, *files):
            path = current_path / name
            metadata = path.lstat()
            os.lchown(path, 0, 0)
            if not stat.S_ISLNK(metadata.st_mode):
                os.chmod(path, stat.S_IMODE(metadata.st_mode) & ~0o222)


def _verify_installed_candidate(candidate: Candidate) -> None:
    try:
        root_metadata = candidate.root.lstat()
    except OSError as exc:
        raise RuntimeHostError("installed candidate root is unavailable") from exc
    if stat.S_ISLNK(root_metadata.st_mode) or not stat.S_ISDIR(root_metadata.st_mode):
        raise RuntimeHostError("installed candidate root is unsafe")
    if not candidate.repo.is_dir() or candidate.repo.is_symlink():
        raise RuntimeHostError("installed candidate repo is unavailable")
    if _validate_repository(candidate.repo, candidate.sha) != candidate.tree:
        raise RuntimeHostError("installed candidate tree drifted")
    python = candidate.venv / "bin/python"
    if not python.is_file():
        raise RuntimeHostError("installed frozen venv is unavailable")
    for current, directories, files in os.walk(candidate.root, followlinks=False):
        for path in (
            Path(current),
            *(Path(current) / name for name in (*directories, *files)),
        ):
            metadata = path.lstat()
            if (metadata.st_uid, metadata.st_gid) != (0, 0):
                raise RuntimeHostError("installed candidate ownership drifted")
            if not stat.S_ISLNK(metadata.st_mode) and metadata.st_mode & 0o222:
                raise RuntimeHostError("installed candidate is writable")


def _materialize_candidate(candidate: Candidate, staging_path: Path) -> bool:
    expected_prefix = f".install-{candidate.sha}-"
    if (
        staging_path.parent != CANDIDATE_PARENT
        or not staging_path.name.startswith(expected_prefix)
        or staging_path.exists()
        or staging_path.is_symlink()
    ):
        raise RuntimeHostError("candidate staging path is invalid")
    _ensure_directory(STATE_ROOT, mode=0o700)
    _ensure_directory(INSTALLER_ROOT, mode=0o700)
    _ensure_directory(INSTALLER_ROOT / "uv-cache", mode=0o700)
    _ensure_directory(CANDIDATE_PARENT, mode=0o755)
    if candidate.root.exists() or candidate.root.is_symlink():
        _verify_installed_candidate(candidate)
        return False
    try:
        _run(
            (
                "git",
                "--no-replace-objects",
                "-c",
                "protocol.file.allow=always",
                "-c",
                "credential.helper=",
                "-c",
                "core.sshCommand=/usr/bin/false",
                "clone",
                "--no-hardlinks",
                "--no-checkout",
                str(candidate.source),
                str(staging_path / "repo"),
            ),
            env=_git_environment(),
        )
        _run(
            (
                "git",
                "--no-replace-objects",
                "-c",
                "credential.helper=",
                "-C",
                str(staging_path / "repo"),
                "checkout",
                "--detach",
                candidate.sha,
            ),
            env=_git_environment(),
        )
        env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "HOME": "/nonexistent",
            "XDG_CONFIG_HOME": "/nonexistent",
            "UV_PROJECT_ENVIRONMENT": str(staging_path / "venv"),
            "UV_CACHE_DIR": str(INSTALLER_ROOT / "uv-cache"),
            "UV_NO_PROGRESS": "1",
        }
        _run(
            (
                "uv",
                "sync",
                "--frozen",
                "--no-dev",
                "--project",
                str(staging_path / "repo"),
            ),
            env=env,
        )
        installed = Candidate(
            sha=candidate.sha,
            tree=candidate.tree,
            source=staging_path / "repo",
        )
        if _validate_repository(installed.source, installed.sha) != installed.tree:
            raise RuntimeHostError("materialized candidate tree drifted")
        _make_read_only(staging_path)
        os.rename(staging_path, candidate.root)
        _fsync_directory(CANDIDATE_PARENT)
    finally:
        if staging_path.exists() or staging_path.is_symlink():
            _remove_path(staging_path)
    _verify_installed_candidate(candidate)
    return True


def _publish_files(candidate: Candidate) -> None:
    for path, (content, mode) in _desired_files(candidate).items():
        _atomic_write(path, content, mode=mode)
    _atomic_symlink(CURRENT_LINK, f"candidates/{candidate.sha}")


def _publish_unit_state() -> None:
    _run(("systemctl", "daemon-reload"))
    for unit in ALL_UNITS:
        _run(("systemctl", "disable", "--now", unit), expected={0, 1, 5})
    _stop_units()


def _service_result(unit: str) -> tuple[str, str]:
    result = _run(
        ("systemctl", "show", unit, "--property=Result", "--value"),
    ).stdout.strip()
    status = _run(
        ("systemctl", "show", unit, "--property=ExecMainStatus", "--value"),
    ).stdout.strip()
    return result, status


def _run_candidate_python(
    candidate: Candidate,
    code: str,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    expected_arguments = _EMBEDDED_PROGRAM_ARGUMENT_COUNTS.get(code)
    if expected_arguments is None or len(args) != expected_arguments:
        raise RuntimeHostError("embedded candidate program argument contract is invalid")
    try:
        compile(code, "<shared-capacity-runtime-host-embedded>", "exec")
    except SyntaxError as exc:
        raise RuntimeHostError("embedded candidate program is invalid") from exc
    return _run(
        (
            str(candidate.venv / "bin/python"),
            "-I",
            "-B",
            "-c",
            code,
            str(candidate.repo),
            *args,
        ),
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "HOME": "/nonexistent",
            "XDG_CONFIG_HOME": "/nonexistent",
        },
    )


def _candidate_broker_state_db(candidate: Candidate) -> Path:
    try:
        payload = tomllib.loads(
            _read_candidate_file(
                candidate,
                SUPERVISOR_CONFIG_SOURCE.relative_to(REPO_ROOT),
            ).decode("utf-8", errors="strict"),
        )
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise RuntimeHostError("candidate supervisor config is invalid") from exc
    raw = payload.get("state_db")
    if not isinstance(raw, str):
        raise RuntimeHostError("candidate broker authority path is invalid")
    path = Path(raw)
    if not path.is_absolute() or ".." in path.parts:
        raise RuntimeHostError("candidate broker authority path is invalid")
    return path


def _retirement_request_ids(
    report: Mapping[str, Any],
    candidate_sha: str,
) -> tuple[str, ...]:
    records = report.get("requests")
    if not isinstance(records, list):
        raise RuntimeHostError("broker retirement report is invalid")
    by_instance: dict[str, list[tuple[Mapping[str, Any], Mapping[str, Any]]]] = {
        instance: [] for instance in INSTANCES
    }
    request_ids: set[str] = set()
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        request = record.get("request")
        lease = record.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        request_id = request.get("id")
        instance = f"{request.get('sandbox')}-{request.get('pool')}"
        if (
            not isinstance(request_id, str)
            or not request_id
            or request_id in request_ids
            or instance not in by_instance
        ):
            raise RuntimeHostError("broker retirement request binding is invalid")
        request_ids.add(request_id)
        by_instance[instance].append((request, lease))

    retire: list[str] = []
    for instance, lane_records in by_instance.items():
        exact = [
            (request, lease)
            for request, lease in lane_records
            if request.get("candidate_sha") == candidate_sha
        ]
        if not exact:
            raise RuntimeHostError(f"broker retirement lane is missing: {instance}")
        nonterminal = [
            (request, lease)
            for request, lease in lane_records
            if request.get("state") != "terminal"
        ]
        if len(nonterminal) > 1:
            raise RuntimeHostError(f"broker retirement lane is ambiguous: {instance}")
        if nonterminal:
            request, _lease = nonterminal[0]
            if request.get("candidate_sha") != candidate_sha:
                raise RuntimeHostError(
                    f"broker retirement lane belongs to another candidate: {instance}",
                )
            retire.append(str(request["id"]))
    return tuple(retire)


def _retirement_is_drained(
    report: Mapping[str, Any],
    candidate_sha: str,
) -> bool:
    outstanding = _retirement_request_ids(report, candidate_sha)
    records = report.get("requests")
    aggregate = report.get("aggregate")
    if not isinstance(records, list) or not isinstance(aggregate, dict):
        raise RuntimeHostError("broker retirement aggregate is invalid")
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        request = record.get("request")
        lease = record.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise RuntimeHostError("broker retirement request is invalid")
        if request.get("candidate_sha") != candidate_sha:
            continue
        if request.get("state") != "terminal" or any(
            lease.get(field) != 0
            for field in (
                "granted_slots",
                "pending_slots",
                "active_slots",
                "draining_slots",
                "committed_slots",
            )
        ):
            return False
    if outstanding:
        return False
    return not any(
        aggregate.get(field) != 0
        for field in (
            "granted_slots",
            "pending_slots",
            "active_slots",
            "draining_slots",
            "committed_slots",
        )
    )


def _validate_zero_broker_handoffs(
    report: Mapping[str, Any],
    selected: Mapping[str, Any],
    candidate_sha: str,
) -> None:
    if set(selected) != set(INSTANCES):
        raise RuntimeHostError("broker activation preflight is not closed-world")
    records = report.get("requests")
    aggregate = report.get("aggregate")
    if not isinstance(records, list) or not isinstance(aggregate, dict):
        raise RuntimeHostError("broker activation report is invalid")
    requests: dict[str, Mapping[str, Any]] = {}
    leases: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            raise RuntimeHostError("broker activation request is invalid")
        request = record.get("request")
        lease = record.get("lease")
        if not isinstance(request, dict) or not isinstance(lease, dict):
            raise RuntimeHostError("broker activation request is invalid")
        request_id = request.get("id")
        if not isinstance(request_id, str) or request_id in leases:
            raise RuntimeHostError("broker activation request binding is invalid")
        requests[request_id] = request
        leases[request_id] = lease
    for instance in INSTANCES:
        handoff = selected.get(instance)
        if (
            not isinstance(handoff, dict)
            or handoff.get("enabled") is not False
            or handoff.get("min_slots") != 0
            or handoff.get("max_slots") != 0
            or handoff.get("candidate_sha") != candidate_sha
        ):
            raise RuntimeHostError("activation requires six zero-capacity handoffs")
        request_id = str(handoff.get("request_id"))
        request = requests.get(request_id)
        lease = leases.get(request_id)
        if request is None or request.get("state") != "terminal":
            raise RuntimeHostError("activation requires terminal zero-capacity requests")
        if lease is None or any(
            lease.get(field) != 0 for field in ("pending_slots", "active_slots", "draining_slots")
        ):
            raise RuntimeHostError("broker activation lease is not fully drained")
    if any(
        aggregate.get(field) != 0
        for field in (
            "committed_slots",
            "pending_slots",
            "active_slots",
            "draining_slots",
        )
    ):
        raise RuntimeHostError("broker activation aggregate is not fully drained")


def _validate_zero_cp_policy(
    policy: Mapping[str, Any],
    *,
    candidate_sha: str,
) -> None:
    actuator = policy.get("actuator_config")
    if (
        policy.get("enabled") is not False
        or policy.get("min_slots") != 0
        or policy.get("max_slots") != 0
        or not isinstance(actuator, dict)
        or actuator.get("candidate_sha") != candidate_sha
    ):
        raise RuntimeHostError("control-plane autoscaler policy is not disabled")
    for field in (
        "last_pending_slots",
        "last_actual_slots",
        "last_draining_slots",
        "last_occupied_slots",
        "last_queued_slots",
    ):
        if policy.get(field) != 0:
            raise RuntimeHostError("control-plane autoscaler still has live capacity")


def _validate_zero_worker_status(
    payload: Mapping[str, Any],
    *,
    environment: str,
    pool_name: str,
) -> None:
    summary = payload.get("summary")
    jobs = payload.get("jobs")
    if not isinstance(summary, list) or not isinstance(jobs, list):
        raise RuntimeHostError("control-plane worker status is invalid")
    matching_summary = [
        item
        for item in summary
        if isinstance(item, dict)
        and item.get("environment") == environment
        and item.get("pool_name") == pool_name
    ]
    if len(matching_summary) > 1:
        raise RuntimeHostError("control-plane worker summary is duplicated")
    for item in matching_summary:
        if any(
            item.get(field) != 0
            for field in (
                "desired_slots",
                "active_slots",
                "pending_slots",
                "stale_slots",
                "running_jobs",
                "pending_jobs",
                "stale_jobs",
            )
        ):
            raise RuntimeHostError("control-plane worker summary is not drained")
    for item in jobs:
        if not isinstance(item, dict):
            raise RuntimeHostError("control-plane worker job status is invalid")
        if (
            item.get("environment") == environment
            and item.get("pool_name") == pool_name
            and item.get("state") in {"pending", "running"}
        ):
            raise RuntimeHostError("control-plane still has a live Slurm worker job")


_BROKER_PREFLIGHT = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
from scripts.ops.shared_capacity_runtime_host import _validate_zero_broker_handoffs
from scripts.ops.shared_capacity_supervisor import (
    _publication_handoffs,
    _validate_report_budgets,
    load_config,
)
config = load_config(Path(sys.argv[2]))
broker = SharedCapacityBroker(config.state_db)
broker.close_admission(sys.argv[4])
report = broker.status()
_validate_report_budgets(report, config)
selected = _publication_handoffs(report, config)
_validate_zero_broker_handoffs(report, selected, sys.argv[3])
"""

_BROKER_OPEN = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
SharedCapacityBroker(Path(sys.argv[2])).open_admission(sys.argv[3])
"""

_BROKER_RETIRE = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
from scripts.ops.shared_capacity_runtime_host import (
    _retirement_is_drained,
    _retirement_request_ids,
)
from scripts.ops.shared_capacity_supervisor import (
    _validate_report_budgets,
    load_config,
)
config = load_config(Path(sys.argv[2]))
if config.state_db != Path(sys.argv[5]):
    raise RuntimeError("broker retirement authority binding mismatch")
broker = SharedCapacityBroker(config.state_db)
broker.close_admission(sys.argv[4])
report = broker.status()
_validate_report_budgets(report, config)
for request_id in _retirement_request_ids(report, sys.argv[3]):
    broker.cancel(request_id, reason="runtime_host_rollback")
report = broker.status()
_validate_report_budgets(report, config)
print("drained" if _retirement_is_drained(report, sys.argv[3]) else "pending")
"""

_ADAPTER_PREFLIGHT = """
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts.ops.shared_capacity_adapter import (
    _bootstrap_policy_body,
    _get_policy,
    _http_json,
    _load_admin_token,
    _load_sandbox_binding,
    _policy_path,
    _validate_bootstrap_policy,
    _validate_policy,
    _validate_runtime_attestation,
    load_config,
)
from scripts.ops.shared_capacity_runtime_host import (
    _validate_zero_cp_policy,
    _validate_zero_worker_status,
)
config = load_config(Path(sys.argv[2]))
binding = _load_sandbox_binding(config)
if binding.sha != sys.argv[3] or binding.tree != sys.argv[4]:
    raise RuntimeError("sandbox candidate binding mismatch")
_token = _load_admin_token(config.admin_secret_file)
_validate_runtime_attestation(
    config,
    candidate=binding,
    now=datetime.now(UTC),
    minimum_remaining=timedelta(seconds=max(30.0, config.timeout_seconds * 3)),
)
policy, missing = _get_policy(
    config,
    token=_token,
    path=_policy_path(config),
    http_json=_http_json,
)
if missing or policy is None:
    raise RuntimeError("control-plane autoscaler policy is missing")
expected = _bootstrap_policy_body(config, candidate_sha=binding.sha)
_validate_policy(policy, config=config, candidate_sha=binding.sha)
_validate_bootstrap_policy(
    policy,
    config=config,
    candidate_sha=binding.sha,
    expected_body=expected,
)
_validate_zero_cp_policy(policy, candidate_sha=binding.sha)
worker_status = _http_json(
    method="GET",
    base_url=config.control_plane_url,
    token=_token,
    path="/admin/slurm-worker-jobs/status",
    timeout=config.timeout_seconds,
)
_validate_zero_worker_status(
    worker_status,
    environment=config.environment,
    pool_name=config.pool_name,
)
"""

_GENERATION_READBACK = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from loom_control_plane.shared_capacity_broker import SharedCapacityBroker
from scripts.ops.shared_capacity_supervisor import (
    _current_generation,
    _load_supervisor_state,
    _publication_handoffs,
    _read_json,
    _validate_generation_contents,
    load_config,
)
config = load_config(Path(sys.argv[2]))
state = _load_supervisor_state(config.supervisor_state_path)
generation = _current_generation(config.handoff_dir)
if state is None or generation != state.get("generation"):
    raise RuntimeError("supervisor generation state mismatch")
published = state.get("published")
if not isinstance(published, dict) or set(published) != set(config.instances):
    raise RuntimeError("supervisor generation is not closed-world")
manifest = _read_json(
    config.handoff_dir / generation / "manifest.json",
    label="activation generation manifest",
)
if not isinstance(manifest, dict) or manifest.get("instances") != published:
    raise RuntimeError("supervisor generation manifest mismatch")
report = SharedCapacityBroker(config.state_db).status()
selected = _publication_handoffs(report, config)
_validate_generation_contents(
    config.handoff_dir / generation,
    manifest=manifest,
    selected=selected,
    config=config,
)
"""

_ACTIVATED_ADAPTER_READBACK = """
import sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from scripts.ops.shared_capacity_adapter import (
    _bootstrap_policy_body,
    _get_policy,
    _handoff_binding,
    _http_json,
    _load_adapter_state,
    _load_admin_token,
    _load_sandbox_binding,
    _policy_path,
    _validate_policy_update_readback,
    load_config,
    load_handoff,
)
from scripts.ops.shared_capacity_runtime_host import (
    _validate_zero_cp_policy,
    _validate_zero_worker_status,
)
config = load_config(Path(sys.argv[2]))
candidate = _load_sandbox_binding(config)
if candidate.sha != sys.argv[3] or candidate.tree != sys.argv[4]:
    raise RuntimeError("activated sandbox candidate binding mismatch")
handoff = load_handoff(config)
state = _load_adapter_state(config.adapter_state_path)
if state is None:
    raise RuntimeError("activated adapter state is missing")
token = _load_admin_token(config.admin_secret_file)
policy, missing = _get_policy(
    config,
    token=token,
    path=_policy_path(config),
    http_json=_http_json,
)
if missing or policy is None:
    raise RuntimeError("activated control-plane policy is missing")
binding = _handoff_binding(handoff)
_validate_policy_update_readback(
    policy,
    config=config,
    candidate_sha=candidate.sha,
    expected_policy=_bootstrap_policy_body(config, candidate_sha=candidate.sha),
    binding=binding,
    enabled=handoff.enabled,
    max_slots=handoff.max_slots,
)
if (
    state.get("request_id") != handoff.request_id
    or state.get("lease_epoch") != handoff.lease_epoch
    or state.get("candidate_sha") != handoff.candidate_sha
    or state.get("applied_enabled") != handoff.enabled
    or state.get("applied_max_slots") != handoff.max_slots
):
    raise RuntimeError("activated adapter state is not current")
if not handoff.enabled:
    _validate_zero_cp_policy(policy, candidate_sha=candidate.sha)
    worker_status = _http_json(
        method="GET",
        base_url=config.control_plane_url,
        token=token,
        path="/admin/slurm-worker-jobs/status",
        timeout=config.timeout_seconds,
    )
    _validate_zero_worker_status(
        worker_status,
        environment=config.environment,
        pool_name=config.pool_name,
    )
    if any(
        state.get(field) != 0
        for field in ("pending_slots", "active_slots", "draining_slots")
    ):
        raise RuntimeError("disabled activated adapter still has live capacity")
"""

_EMBEDDED_PROGRAM_ARGUMENT_COUNTS = {
    _BROKER_PREFLIGHT: 3,
    _BROKER_OPEN: 2,
    _BROKER_RETIRE: 4,
    _ADAPTER_PREFLIGHT: 3,
    _GENERATION_READBACK: 1,
    _ACTIVATED_ADAPTER_READBACK: 3,
}


def _activation_preflight(
    candidate: Candidate,
    *,
    transaction_id: str,
) -> None:
    _run_candidate_python(
        candidate,
        _BROKER_PREFLIGHT,
        str(SUPERVISOR_CONFIG_PATH),
        candidate.sha,
        transaction_id,
    )
    for instance in INSTANCES:
        _run_candidate_python(
            candidate,
            _ADAPTER_PREFLIGHT,
            str(ADAPTER_CONFIG_ROOT / f"{instance}.toml"),
            candidate.sha,
            candidate.tree,
        )


def _open_activation_admission(
    candidate: Candidate,
    transaction_id: str,
) -> None:
    _run_candidate_python(
        candidate,
        _BROKER_OPEN,
        str(_candidate_broker_state_db(candidate)),
        transaction_id,
    )


def _request_capacity_retirement(
    candidate: Candidate,
    transaction_id: str,
) -> str:
    completed = _run_candidate_python(
        candidate,
        _BROKER_RETIRE,
        str(SUPERVISOR_CONFIG_PATH),
        candidate.sha,
        transaction_id,
        str(_candidate_broker_state_db(candidate)),
    )
    status = completed.stdout.strip()
    if status not in {"pending", "drained"}:
        raise RuntimeHostError("broker retirement status is invalid")
    return status


def _run_retirement_cycle() -> None:
    for unit in (SUPERVISOR_SERVICE, *ADAPTER_SERVICES, SUPERVISOR_SERVICE):
        _run(("systemctl", "start", unit))
        if _service_result(unit) != ("success", "0"):
            raise RuntimeHostError("shared-capacity retirement cycle failed")


def _drain_activated_capacity(
    candidate: Candidate,
    transaction_id: str,
) -> None:
    for cycle in range(RETIREMENT_MAX_CYCLES):
        _request_capacity_retirement(candidate, transaction_id)
        _run_retirement_cycle()
        if _request_capacity_retirement(candidate, transaction_id) == "drained":
            _activated_adapter_readback(candidate)
            return
        if cycle + 1 < RETIREMENT_MAX_CYCLES:
            time.sleep(RETIREMENT_POLL_SECONDS)
    raise RuntimeHostError("shared-capacity retirement did not drain before timeout")


def _verify_activated_capacity_drained(
    candidate: Candidate,
    transaction_id: str,
) -> None:
    if _request_capacity_retirement(candidate, transaction_id) != "drained":
        raise RuntimeHostError("shared-capacity retirement regressed before local restore")
    _activated_adapter_readback(candidate)


def _rollback_candidate(payload: Mapping[str, Any]) -> Candidate:
    sha = payload.get("candidate_sha")
    tree = payload.get("candidate_tree")
    if (
        not isinstance(sha, str)
        or SHA_RE.fullmatch(sha) is None
        or not isinstance(tree, str)
        or SHA_RE.fullmatch(tree) is None
    ):
        raise RuntimeHostError("activated rollback candidate binding is invalid")
    return Candidate(
        sha=sha,
        tree=tree,
        source=CANDIDATE_PARENT / sha / "repo",
    )


def _complete_activated_rollback(
    path: Path,
    payload: dict[str, Any],
) -> None:
    transaction_id, operation, _sha, _tree, _files, _units = _validate_transaction(
        path,
        payload,
    )
    if operation != "install":
        raise RuntimeHostError("activated rollback transaction is invalid")
    _validate_rollback_recovery(payload)
    candidate = _rollback_candidate(payload)
    phase = payload.get("phase")
    if phase == "rollback-recovery-ready":
        _update_journal(path, payload, "rollback-closing-admission")
        phase = "rollback-closing-admission"
    if phase == "rollback-closing-admission":
        _request_capacity_retirement(candidate, transaction_id)
        _update_journal(path, payload, "rollback-draining")
        phase = "rollback-draining"
    if phase == "rollback-draining":
        _drain_activated_capacity(candidate, transaction_id)
        _update_journal(path, payload, "rollback-drained")
        phase = "rollback-drained"
    if phase == "rollback-drained":
        _verify_activated_capacity_drained(candidate, transaction_id)
        _update_journal(path, payload, "rollback-restoring")
        phase = "rollback-restoring"
    if phase == "rollback-restoring":
        _restore_local_transaction(
            path,
            payload,
            remove_candidate=False,
        )
        _update_journal(path, payload, "rollback-restored-fenced")
        phase = "rollback-restored-fenced"
    if phase == "rollback-restored-fenced":
        _open_activation_admission(candidate, transaction_id)
        _update_journal(path, payload, "rollback-admission-open")
        phase = "rollback-admission-open"
    if phase == "rollback-admission-open":
        if payload.get("candidate_previously_existed") is False:
            _remove_path(candidate.root)
        _update_journal(path, payload, "rolled-back")
        ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
        _fsync_directory(INSTALLER_ROOT)
        return
    raise RuntimeHostError("activated rollback journal phase is invalid")


def _resume_activated_rollback(
    path: Path,
    payload: dict[str, Any],
) -> None:
    if payload.get("phase") not in {
        "rollback-recovery-ready",
        "rollback-closing-admission",
        "rollback-draining",
        "rollback-drained",
        "rollback-restoring",
        "rollback-restored-fenced",
        "rollback-admission-open",
    }:
        raise RuntimeHostError("activated rollback journal phase is invalid")
    _complete_activated_rollback(path, payload)


def _activate_units(candidate: Candidate) -> None:
    _run(("systemctl", "start", SUPERVISOR_SERVICE))
    if _service_result(SUPERVISOR_SERVICE) != ("success", "0"):
        raise RuntimeHostError("supervisor activation cycle failed")
    _run_candidate_python(
        candidate,
        _GENERATION_READBACK,
        str(SUPERVISOR_CONFIG_PATH),
    )
    _run(("systemctl", "enable", "--now", SUPERVISOR_TIMER))
    for service, timer in zip(ADAPTER_SERVICES, ADAPTER_TIMERS, strict=True):
        _run(("systemctl", "start", service))
        if _service_result(service) != ("success", "0"):
            raise RuntimeHostError("adapter activation cycle failed")
        _run(("systemctl", "enable", "--now", timer))


def _activated_adapter_readback(candidate: Candidate) -> None:
    _run_candidate_python(
        candidate,
        _GENERATION_READBACK,
        str(SUPERVISOR_CONFIG_PATH),
    )
    for instance in INSTANCES:
        _run_candidate_python(
            candidate,
            _ACTIVATED_ADAPTER_READBACK,
            str(ADAPTER_CONFIG_ROOT / f"{instance}.toml"),
            candidate.sha,
            candidate.tree,
        )


def check(
    candidate: Candidate,
    *,
    activation_mode: str = "installed",
) -> dict[str, Any]:
    if activation_mode not in {"installed", "activated"}:
        raise RuntimeHostError("runtime-host check mode is invalid")
    _verify_installed_candidate(candidate)
    desired = _desired_files(candidate)
    for path, (content, mode) in desired.items():
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise RuntimeHostError(f"installed file is unavailable: {path}") from exc
        if (
            stat.S_ISLNK(metadata.st_mode)
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or path.read_bytes() != content
        ):
            raise RuntimeHostError(f"installed file drifted: {path}")
    if not CURRENT_LINK.is_symlink() or os.readlink(CURRENT_LINK) != f"candidates/{candidate.sha}":
        raise RuntimeHostError("current candidate pointer drifted")
    for directory, mode in (
        (STATE_ROOT, 0o700),
        (INSTALLER_ROOT, 0o700),
        (JOURNAL_ROOT, 0o700),
        (INSTALLER_ROOT / "uv-cache", 0o700),
        (ADAPTER_CONFIG_ROOT, 0o755),
    ):
        metadata = directory.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or (metadata.st_uid, metadata.st_gid) != (0, 0)
            or stat.S_IMODE(metadata.st_mode) != mode
        ):
            raise RuntimeHostError(f"installed directory drifted: {directory}")
    _reject_orphan_configs()
    loaded_units = _loaded_managed_units()
    unexpected = loaded_units - set(ALL_UNITS)
    if unexpected:
        raise RuntimeHostError("orphan shared-capacity unit is loaded")
    _reject_orphan_unit_files()
    for unit, fragment_path in UNIT_FRAGMENT_PATHS.items():
        _validate_unit_fragment(unit, fragment_path)
    if activation_mode == "installed":
        for unit in ALL_UNITS:
            if _systemctl_state(unit) != {"enabled": False, "active": False}:
                raise RuntimeHostError(f"managed unit is not fail-closed: {unit}")
    else:
        for timer in ALL_TIMERS:
            if _systemctl_state(timer) != {"enabled": True, "active": True}:
                raise RuntimeHostError(f"managed timer is not active: {timer}")
        for service in ALL_SERVICES:
            if _service_result(service) != ("success", "0"):
                raise RuntimeHostError(f"managed service result failed: {service}")
        _activated_adapter_readback(candidate)
    installed_state = _load_json(STATE_PATH, "runtime-host state")
    if (
        installed_state.get("candidate_sha") != candidate.sha
        or installed_state.get("candidate_tree") != candidate.tree
        or installed_state.get("activation_status") != activation_mode
    ):
        raise RuntimeHostError("runtime-host state candidate drifted")
    return {
        "schema_version": 1,
        "status": "pass",
        "candidate_sha": candidate.sha,
        "candidate_tree": candidate.tree,
        "candidate_root": str(candidate.root),
        "instances": list(INSTANCES),
        "activation_status": activation_mode,
        "timers_active": len(ALL_TIMERS) if activation_mode == "activated" else 0,
        "managed_units_disabled_and_inactive": (
            len(ALL_UNITS) if activation_mode == "installed" else 0
        ),
        "capacity_enabled_by_install_command": False,
        "adapter_activation_authorized": activation_mode == "activated",
        "capacity_enabled_by_installer": False,
    }


def install(candidate: Candidate) -> dict[str, Any]:
    _require_live_host()
    _load_candidate_profile(candidate)
    with _lock():
        _recover_orphan()
        if STATE_PATH.exists() or STATE_PATH.is_symlink():
            current_state = _load_json(STATE_PATH, "runtime-host state")
            if current_state.get("activation_status") == "activated":
                raise RuntimeHostError(
                    "activated runtime must be retired through rollback before install",
                )
        _reject_orphan_stages()
        _reject_orphan_configs()
        _loaded_managed_units()
        _installed_managed_unit_files()
        journal_path, journal = _write_journal(candidate, operation="install")
        try:
            _stop_units()
            _update_journal(journal_path, journal, "stopped")
            staging_path = Path(str(journal["staging_path"]))
            _materialize_candidate(candidate, staging_path)
            _update_journal(journal_path, journal, "materialized")
            installed_candidate = Candidate(
                sha=candidate.sha,
                tree=candidate.tree,
                source=candidate.repo,
            )
            _publish_files(installed_candidate)
            _update_journal(journal_path, journal, "published")
            state = {
                "schema_version": 1,
                "candidate_sha": candidate.sha,
                "candidate_tree": candidate.tree,
                "activation_status": "installed",
                "installed_at": datetime.now(UTC).isoformat(),
                "transaction_id": journal["transaction_id"],
            }
            _atomic_write(STATE_PATH, _canonical_json(state), mode=0o600)
            _publish_unit_state()
            _update_journal(journal_path, journal, "fail-closed")
            report = check(installed_candidate)
            _update_journal(journal_path, journal, "committed")
            ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
            _fsync_directory(INSTALLER_ROOT)
            return report
        except Exception:
            _restore_transaction(journal_path, journal)
            raise


def activation_plan(sha: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("activation requires the installed full candidate SHA")
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-runtime-host-activation-plan",
        "mutation_authorized": False,
        "candidate_sha": sha,
        "steps": [
            "verify-installed-inactive-exact-candidate",
            "journal-and-fence-new-broker-requests-by-transaction",
            "require-six-terminal-disabled-zero-handoffs-and-zero-broker-counters",
            "validate-six-config-secret-combined-receipt-and-zero-cp-policies",
            "run-supervisor-once-and-read-back-complete-generation",
            "enable-supervisor-timer",
            "run-and-enable-six-adapter-timers",
            "commit-activated-state-before-releasing-exact-admission-fence",
            "rollback-to-all-disabled-on-any-failure",
        ],
    }


def activate(sha: str) -> dict[str, Any]:
    _require_live_host()
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("activation requires the installed full candidate SHA")
    with _lock():
        _recover_orphan()
        _reject_orphan_stages()
        state = _load_json(STATE_PATH, "runtime-host state")
        tree = state.get("candidate_tree")
        if (
            state.get("candidate_sha") != sha
            or not isinstance(tree, str)
            or SHA_RE.fullmatch(tree) is None
        ):
            raise RuntimeHostError("activation SHA does not match the installed candidate")
        candidate = Candidate(
            sha=sha,
            tree=tree,
            source=CANDIDATE_PARENT / sha / "repo",
        )
        if state.get("activation_status") == "activated":
            return check(candidate, activation_mode="activated")
        if state.get("activation_status") != "installed":
            raise RuntimeHostError("runtime-host activation state is invalid")
        check(candidate, activation_mode="installed")
        journal_path, journal = _write_journal(candidate, operation="activate")
        try:
            _update_journal(journal_path, journal, "activation-closing-admission")
            _activation_preflight(
                candidate,
                transaction_id=str(journal["transaction_id"]),
            )
            _update_journal(journal_path, journal, "activation-preflight-passed")
            _activate_units(candidate)
            _update_journal(journal_path, journal, "units-activated")
            activated_state = dict(state)
            activated_state["activation_status"] = "activated"
            activated_state["activated_at"] = datetime.now(UTC).isoformat()
            activated_state["activation_transaction_id"] = journal["transaction_id"]
            _atomic_write(
                STATE_PATH,
                _canonical_json(activated_state),
                mode=0o600,
            )
            report = check(candidate, activation_mode="activated")
            _update_journal(journal_path, journal, "committed")
            _open_activation_admission(
                candidate,
                str(journal["transaction_id"]),
            )
            ACTIVE_JOURNAL_PATH.unlink(missing_ok=True)
            _fsync_directory(INSTALLER_ROOT)
            return report
        except Exception:
            _restore_transaction(journal_path, journal)
            raise


def rollback_plan(sha: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise RuntimeHostError("rollback requires the current full candidate SHA")
    return {
        "schema_version": 1,
        "artifact_type": "shared-capacity-runtime-host-rollback-plan",
        "mutation_authorized": False,
        "candidate_sha": sha,
        "steps": [
            "persist-and-journal-root-owned-recovery-entrypoint",
            "journal-and-persistently-fence-new-broker-requests",
            "cancel-only-the-six-exact-candidate-sandbox-pool-requests",
            "reconcile-and-read-back-six-terminal-zero-handoffs",
            "read-back-six-disabled-zero-control-plane-policies-and-worker-jobs",
            "stop-current-services-and-timers-after-external-capacity-is-zero",
            "restore-previous-configs-and-exact-units",
            "restore-previous-enabled-and-active-state",
            "reopen-the-exact-admission-fence-after-local-restore",
            "remove-journal-owned-stage-and-candidate-last",
        ],
    }


def rollback(sha: str) -> dict[str, Any]:
    _require_live_host()
    with _lock():
        _recover_orphan()
        state = _load_json(STATE_PATH, "runtime-host state")
        if state.get("candidate_sha") != sha:
            raise RuntimeHostError("rollback SHA does not match the active candidate")
        transaction_id = state.get("transaction_id")
        if not isinstance(transaction_id, str) or not re.fullmatch(
            r"[0-9a-f]{32}",
            transaction_id,
        ):
            raise RuntimeHostError("runtime-host rollback binding is invalid")
        path = JOURNAL_ROOT / f"{transaction_id}.json"
        payload = _load_json(path, "runtime-host rollback transaction")
        if payload.get("phase") != "committed" or payload.get("candidate_sha") != sha:
            raise RuntimeHostError("runtime-host rollback transaction is invalid")
        activation_status = state.get("activation_status")
        if activation_status not in {"activated", "installed"}:
            raise RuntimeHostError("runtime-host rollback activation state is invalid")
        _atomic_write(
            ACTIVE_JOURNAL_PATH,
            _canonical_json({"transaction_id": transaction_id}),
            mode=0o600,
        )
        if activation_status == "activated":
            _prepare_rollback_recovery(
                path,
                payload,
                _rollback_candidate(payload),
            )
            _complete_activated_rollback(path, payload)
        else:
            _restore_transaction(path, payload)
        return {
            "schema_version": 1,
            "status": "rolled-back",
            "candidate_sha": sha,
            "capacity_enabled_by_installer": False,
        }


def recover() -> dict[str, Any]:
    _require_live_host()
    with _lock():
        active = _active_journal()
        if active is None:
            return {
                "schema_version": 1,
                "status": "no-active-transaction",
            }
        transaction_id = active[1].get("transaction_id")
        _recover_orphan()
        return {
            "schema_version": 1,
            "status": "recovered",
            "transaction_id": transaction_id,
        }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "install"):
        child = subparsers.add_parser(command)
        child.add_argument("--source-repo", type=Path, required=True)
        child.add_argument("--candidate-sha", required=True)
        if command == "install":
            child.add_argument("--execute", action="store_true")
    activate_parser = subparsers.add_parser("activate")
    activate_parser.add_argument("--candidate-sha", required=True)
    activate_parser.add_argument("--execute", action="store_true")
    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--candidate-sha", required=True)
    check_parser.add_argument(
        "--mode",
        choices=("installed", "activated"),
        default="installed",
    )
    check_parser.add_argument("--execute", action="store_true")
    rollback_parser = subparsers.add_parser("rollback")
    rollback_parser.add_argument("--candidate-sha", required=True)
    rollback_parser.add_argument("--execute", action="store_true")
    recover_parser = subparsers.add_parser("recover")
    recover_parser.add_argument("--execute", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command in {"plan", "install"}:
            candidate = _candidate_identity(args.source_repo, args.candidate_sha)
            if args.command == "install" and args.execute:
                result = install(candidate)
            else:
                result = plan(candidate, args.command)
        elif args.command == "activate":
            result = (
                activate(args.candidate_sha)
                if args.execute
                else activation_plan(args.candidate_sha)
            )
        elif args.command == "check":
            if not args.execute:
                result = {
                    "schema_version": 1,
                    "artifact_type": "shared-capacity-runtime-host-check-plan",
                    "mutation_authorized": False,
                    "candidate_sha": args.candidate_sha,
                }
            else:
                state = _load_json(STATE_PATH, "runtime-host state")
                candidate = Candidate(
                    sha=args.candidate_sha,
                    tree=str(state.get("candidate_tree", "")),
                    source=CANDIDATE_PARENT / args.candidate_sha / "repo",
                )
                result = check(candidate, activation_mode=args.mode)
        elif args.command == "rollback" and args.execute:
            result = rollback(args.candidate_sha)
        elif args.command == "recover" and args.execute:
            result = recover()
        elif args.command == "recover":
            result = {
                "schema_version": 1,
                "artifact_type": "shared-capacity-runtime-host-recovery-plan",
                "mutation_authorized": False,
                "entrypoint": str(RECOVERY_PROGRAM_PATH),
            }
        else:
            result = rollback_plan(args.candidate_sha)
        sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")
        return 0
    except (OSError, RuntimeHostError, ValueError):
        sys.stderr.write("error: shared-capacity runtime host failed safely\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
