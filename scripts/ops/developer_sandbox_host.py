#!/usr/bin/env python3
"""Install and converge the three persistent oldlab-2 developer sandboxes.

All public mutation commands are plan-only unless ``--execute`` is supplied.
The installed systemd entry point has no repository, path, host, or secret
overrides. Secret values are generated once, written atomically, and never
included in command output.
"""

from __future__ import annotations

import argparse
import base64
import grp
import hashlib
import json
import os
import pwd
import re
import secrets
import socket
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_PROFILES = REPO_ROOT / "deploy/developer-sandboxes"
SOURCE_UNIT = SOURCE_PROFILES / "loom-developer-sandbox@.service"

REMOTE_URL = "https://github.com/qianyi-sun/loom.git"
SANDBOXES = ("qianyi", "hongjian", "devansh")
EXPECTED_HOSTNAME = "trt-eai-oldlab-2"
PUBLISH_USER = "loom-rollout"
SHARED_GROUP = "sharedwork"
NFS_ROOT = Path("/shared_work/loom/candidates/sandboxes")
STATE_PARENT = Path("/srv/loom/developer-sandboxes")
CONFIG_ROOT = Path("/etc/loom/developer-sandboxes")
DESIRED_ROOT = CONFIG_ROOT / "desired"
PROFILE_CONFIG_ROOT = CONFIG_ROOT / "profiles"
UNIT_PATH = Path("/etc/systemd/system/loom-developer-sandbox@.service")
INSTALLED_PROGRAM = Path("/usr/local/libexec/loom-developer-sandbox-host")
UNIT_NAME = "loom-developer-sandbox@{sandbox}.service"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")

SECRET_KEYS = (
    "LOOM_DEV_POSTGRES_USER",
    "LOOM_DEV_POSTGRES_PASSWORD",
    "LOOM_DEV_MINIO_ROOT_USER",
    "LOOM_DEV_MINIO_ROOT_PASSWORD",
    "LOOM_CP_STEP_JWT_SIGNING_KEY",
    "LOOM_SECRET_STORE_MASTER_KEY",
    "LOOM_WORKER_TOKEN",
)


class HostConvergeError(RuntimeError):
    """A secret-safe host convergence failure."""


@dataclass(frozen=True, slots=True)
class Profile:
    sandbox: str
    compose_project: str
    canonical_hostname: str
    candidate_root: Path
    state_root: Path
    cache_root: Path
    evidence_root: Path
    runtime_root: Path
    ports: dict[str, int]

    @property
    def secrets_root(self) -> Path:
        return self.state_root / "secrets"

    @property
    def secrets_env(self) -> Path:
        return self.secrets_root / "sandbox.env"

    @property
    def admin_secret(self) -> Path:
        return self.secrets_root / "admin.toml"

    @property
    def state_file(self) -> Path:
        return self.state_root / "sandbox-state.json"

    @property
    def desired_file(self) -> Path:
        return DESIRED_ROOT / f"{self.sandbox}.json"


@dataclass(frozen=True, slots=True)
class Identity:
    user: str
    group: str
    uid: int
    gid: int


def _load_profile(path: Path) -> Profile:
    try:
        with path.open("rb") as handle:
            raw = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError(f"could not load profile {path}") from exc
    sandbox = raw.get("sandbox")
    if sandbox not in SANDBOXES or path.stem != sandbox:
        raise HostConvergeError(f"invalid sandbox profile identity: {path}")
    ports = raw.get("ports")
    if not isinstance(ports, dict) or not ports:
        raise HostConvergeError(f"profile ports are invalid: {path}")
    parsed_ports: dict[str, int] = {}
    for name, value in ports.items():
        if not isinstance(name, str) or type(value) is not int or not 1 <= value <= 65535:
            raise HostConvergeError(f"profile ports are invalid: {path}")
        parsed_ports[name] = value
    profile = Profile(
        sandbox=sandbox,
        compose_project=str(raw.get("compose_project", "")),
        canonical_hostname=str(raw.get("canonical_hostname", "")),
        candidate_root=Path(str(raw.get("candidate_root", ""))),
        state_root=Path(str(raw.get("state_root", ""))),
        cache_root=Path(str(raw.get("cache_root", ""))),
        evidence_root=Path(str(raw.get("evidence_root", ""))),
        runtime_root=Path(str(raw.get("runtime_root", ""))),
        ports=parsed_ports,
    )
    if profile.compose_project != f"loom-sandbox-{sandbox}":
        raise HostConvergeError(f"invalid Compose project in {path}")
    if profile.canonical_hostname != EXPECTED_HOSTNAME:
        raise HostConvergeError(f"invalid host binding in {path}")
    if profile.candidate_root != NFS_ROOT / sandbox:
        raise HostConvergeError(f"invalid candidate root in {path}")
    expected_state = STATE_PARENT / sandbox
    if profile.state_root != expected_state:
        raise HostConvergeError(f"invalid state root in {path}")
    expected_children = {
        profile.cache_root: expected_state / "cache",
        profile.evidence_root: expected_state / "evidence",
        profile.runtime_root: expected_state / "runtime",
    }
    if any(actual != expected for actual, expected in expected_children.items()):
        raise HostConvergeError(f"invalid private roots in {path}")
    return profile


def load_profiles(root: Path = SOURCE_PROFILES) -> tuple[Profile, ...]:
    profiles = tuple(_load_profile(root / f"{sandbox}.toml") for sandbox in SANDBOXES)
    all_ports = [port for profile in profiles for port in profile.ports.values()]
    if len(all_ports) != len(set(all_ports)):
        raise HostConvergeError("sandbox host ports collide")
    for field in ("compose_project", "candidate_root", "state_root"):
        values = [getattr(profile, field) for profile in profiles]
        if len(values) != len(set(values)):
            raise HostConvergeError(f"sandbox {field} values collide")
    return profiles


def _identity(user: str, group: str) -> Identity:
    try:
        account = pwd.getpwnam(user)
        group_row = grp.getgrnam(group)
    except KeyError as exc:
        raise HostConvergeError(f"required host identity is absent: {exc}") from exc
    return Identity(user=user, group=group, uid=account.pw_uid, gid=group_row.gr_gid)


def _run(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
    identity: Identity | None = None,
    init_groups: bool = False,
    expected: set[int] | frozenset[int] = frozenset({0}),
) -> subprocess.CompletedProcess[str]:
    command = list(argv)
    if identity is not None and os.geteuid() != identity.uid:
        prefix = ["runuser", "--user", identity.user]
        if not init_groups:
            prefix.extend(("--group", identity.group))
        child_environment = env or {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "LANG": "C.UTF-8",
        }
        command = [
            *prefix,
            "--",
            "env",
            "-i",
            *(f"{key}={value}" for key, value in sorted(child_environment.items())),
            *command,
        ]
        env = None
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=dict(env) if env is not None else None,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode not in expected:
        purpose = Path(argv[0]).name if argv else "command"
        raise HostConvergeError(
            f"{purpose} failed safely with exit code {completed.returncode}",
        )
    return completed


def _path_exists_as(path: Path, identity: Identity) -> bool:
    return (
        _run(
            ("test", "-e", str(path)),
            identity=identity,
            expected={0, 1},
        ).returncode
        == 0
    )


def _clean_git_environment() -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _atomic_write(
    path: Path,
    content: bytes,
    *,
    mode: int,
    identity: Identity | None = None,
) -> None:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        if identity is not None:
            os.chown(temporary, identity.uid, identity.gid)
        os.replace(temporary, path)
        os.chmod(path, mode)
        if identity is not None:
            os.chown(path, identity.uid, identity.gid)
    finally:
        if temporary.exists():
            temporary.unlink()


def _ensure_root_private_directory(path: Path) -> None:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    os.chown(path, 0, 0)
    os.chmod(path, 0o700)
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or (metadata.st_uid, metadata.st_gid) != (0, 0)
    ):
        raise HostConvergeError(f"root-private directory did not converge: {path}")


def _assert_secure_file(path: Path, identity: Identity, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise HostConvergeError(f"{label} is unavailable") from exc
    if not stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise HostConvergeError(f"{label} must be a regular non-symlink file")
    if stat.S_IMODE(metadata.st_mode) != 0o600:
        raise HostConvergeError(f"{label} must have mode 0600")
    if (metadata.st_uid, metadata.st_gid) != (identity.uid, identity.gid):
        raise HostConvergeError(f"{label} owner is invalid")


def verify_private_roots(profile: Profile, identity: Identity) -> None:
    for path in (
        profile.state_root,
        profile.cache_root,
        profile.evidence_root,
        profile.runtime_root,
        profile.secrets_root,
    ):
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise HostConvergeError(f"private sandbox root is unavailable: {path}") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or (metadata.st_uid, metadata.st_gid) != (identity.uid, identity.gid)
        ):
            raise HostConvergeError(f"private sandbox root is invalid: {path}")


def _parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator or not key or key in values:
            raise HostConvergeError("sandbox secret env file is malformed")
        values[key] = value
    return values


def _render_env(values: Mapping[str, str]) -> bytes:
    return "".join(f"{key}={values[key]}\n" for key in sorted(values)).encode()


def _new_secret_values(sandbox: str) -> dict[str, str]:
    return {
        "LOOM_DEV_POSTGRES_USER": f"loom_{sandbox}",
        "LOOM_DEV_POSTGRES_PASSWORD": secrets.token_urlsafe(32),
        "LOOM_DEV_MINIO_ROOT_USER": f"loom-{sandbox}",
        "LOOM_DEV_MINIO_ROOT_PASSWORD": secrets.token_urlsafe(32),
        "LOOM_CP_STEP_JWT_SIGNING_KEY": secrets.token_urlsafe(48),
        "LOOM_SECRET_STORE_MASTER_KEY": base64.b64encode(os.urandom(32)).decode(),
        # This bootstrap value is intentionally not authoritative until the
        # local Control Plane mints and persists its hash after first boot.
        "LOOM_WORKER_TOKEN": f"loom_w_{secrets.token_hex(32)}",
        "LOOM_SVC_BATCH_RUNNER_CP_TOKEN": "",
    }


def ensure_private_roots(profile: Profile, identity: Identity) -> None:
    for path in (
        profile.state_root,
        profile.cache_root,
        profile.evidence_root,
        profile.runtime_root,
        profile.secrets_root,
    ):
        try:
            existing = path.lstat()
        except FileNotFoundError:
            existing = None
        except OSError as exc:
            raise HostConvergeError(f"could not inspect private sandbox root: {path}") from exc
        if existing is not None and (
            stat.S_ISLNK(existing.st_mode) or not stat.S_ISDIR(existing.st_mode)
        ):
            raise HostConvergeError(f"private sandbox root is unsafe: {path}")
        path.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chown(path, identity.uid, identity.gid)
        os.chmod(path, 0o700)
    verify_private_roots(profile, identity)


def verify_secret_files(profile: Profile, identity: Identity) -> None:
    verify_private_roots(profile, identity)
    _assert_secure_file(profile.secrets_env, identity, "sandbox secret env file")
    values = _parse_env_file(profile.secrets_env)
    missing = [key for key in SECRET_KEYS if not values.get(key)]
    if missing:
        raise HostConvergeError(
            "sandbox secret env file is incomplete: " + ", ".join(missing),
        )
    _assert_secure_file(profile.admin_secret, identity, "sandbox admin secret file")
    try:
        payload = tomllib.loads(profile.admin_secret.read_text(encoding="utf-8"))
        token = payload["admin"]["token"]
    except (KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("sandbox admin secret file is invalid") from exc
    if not isinstance(token, str) or not token.startswith("loom_admin_") or len(token) < 43:
        raise HostConvergeError("sandbox admin secret file is invalid")


def ensure_secret_files(profile: Profile, identity: Identity) -> None:
    ensure_private_roots(profile, identity)
    if not profile.secrets_env.exists():
        _atomic_write(
            profile.secrets_env,
            _render_env(_new_secret_values(profile.sandbox)),
            mode=0o600,
            identity=identity,
        )
    if not profile.admin_secret.exists():
        token = f"loom_admin_{secrets.token_urlsafe(32)}"
        content = (f'[admin]\ntoken = "{token}"\nversion = 1\n').encode()
        _atomic_write(
            profile.admin_secret,
            content,
            mode=0o600,
            identity=identity,
        )
    verify_secret_files(profile, identity)


def _git(
    candidate: Path,
    *args: str,
    identity: Identity | None = None,
) -> str:
    result = _run(
        ("git", "-c", f"safe.directory={candidate}", "-C", str(candidate), *args),
        env=_clean_git_environment(),
        identity=identity,
    )
    return result.stdout.strip()


def verify_candidate(
    profile: Profile,
    path: Path,
    sha: str,
    identity: Identity,
) -> str:
    if path != profile.candidate_root / sha or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate path is not exact-SHA bound")
    directory = _run(
        ("test", "-d", str(path)),
        identity=identity,
        expected={0, 1},
    )
    symlink = _run(
        ("test", "-L", str(path)),
        identity=identity,
        expected={0, 1},
    )
    if directory.returncode != 0 or symlink.returncode == 0:
        raise HostConvergeError("candidate directory is unavailable")
    if _git(path, "rev-parse", "--verify", "HEAD", identity=identity) != sha:
        raise HostConvergeError("candidate HEAD does not match requested SHA")
    if _git(path, "rev-parse", "--verify", f"{sha}^{{commit}}", identity=identity) != sha:
        raise HostConvergeError("candidate commit does not resolve exactly")
    if _git(
        path,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
        identity=identity,
    ):
        raise HostConvergeError("candidate checkout is not clean")
    tree = _git(path, "rev-parse", "--verify", "HEAD^{tree}", identity=identity)
    if SHA_RE.fullmatch(tree) is None:
        raise HostConvergeError("candidate tree is invalid")
    if os.geteuid() == identity.uid and os.getgid() == identity.gid:
        for root, directories, files in os.walk(path):
            entries = [Path(root), *(Path(root) / name for name in directories + files)]
            for entry in entries:
                metadata = entry.lstat()
                if stat.S_ISLNK(metadata.st_mode):
                    continue
                if metadata.st_mode & 0o222:
                    raise HostConvergeError("candidate contains a writable entry")
                if (metadata.st_uid, metadata.st_gid) != (identity.uid, identity.gid):
                    raise HostConvergeError("candidate ownership is invalid")
    else:
        writable = _run(
            (
                "find",
                str(path),
                "-xdev",
                "!",
                "-type",
                "l",
                "-perm",
                "/222",
                "-print",
                "-quit",
            ),
            identity=identity,
        )
        if writable.stdout.strip():
            raise HostConvergeError("candidate contains a writable entry")
        wrong_owner = _run(
            (
                "find",
                str(path),
                "-xdev",
                "!",
                "-type",
                "l",
                "(",
                "!",
                "-user",
                identity.user,
                "-o",
                "!",
                "-group",
                identity.group,
                ")",
                "-print",
                "-quit",
            ),
            identity=identity,
        )
        if wrong_owner.stdout.strip():
            raise HostConvergeError("candidate ownership is invalid")
    return tree


def verify_candidate_root(profile: Profile, publisher: Identity) -> None:
    directory = _run(
        ("test", "-d", str(profile.candidate_root)),
        identity=publisher,
        expected={0, 1},
    )
    symlink = _run(
        ("test", "-L", str(profile.candidate_root)),
        identity=publisher,
        expected={0, 1},
    )
    metadata = _run(
        ("stat", "-Lc", "%u:%g:%a", str(profile.candidate_root)),
        identity=publisher,
    ).stdout.strip()
    if (
        directory.returncode != 0
        or symlink.returncode == 0
        or metadata != f"{publisher.uid}:{publisher.gid}:2750"
    ):
        raise HostConvergeError("candidate root owner or mode is invalid")


def publish_candidate(profile: Profile, sha: str, publisher: Identity) -> str:
    if SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate SHA must be full lowercase 40-hex")
    if profile.candidate_root != NFS_ROOT / profile.sandbox:
        raise HostConvergeError("candidate root escaped the fixed NFS namespace")
    _run(
        ("install", "-d", "-m", "2750", str(profile.candidate_root)),
        identity=publisher,
    )
    verify_candidate_root(profile, publisher)
    candidate = profile.candidate_root / sha
    if _path_exists_as(candidate, publisher):
        return verify_candidate(profile, candidate, sha, publisher)

    temporary = profile.candidate_root / f".publish-{sha}-{os.getpid()}"
    if _path_exists_as(temporary, publisher):
        raise HostConvergeError("candidate publication temporary path already exists")
    try:
        _run(
            (
                "git",
                "-c",
                "protocol.file.allow=never",
                "clone",
                "--no-checkout",
                "--filter=blob:none",
                REMOTE_URL,
                str(temporary),
            ),
            env=_clean_git_environment(),
            identity=publisher,
        )
        _run(
            ("git", "-C", str(temporary), "fetch", "--no-tags", "origin", sha),
            env=_clean_git_environment(),
            identity=publisher,
        )
        _run(
            ("git", "-C", str(temporary), "checkout", "--detach", sha),
            env=_clean_git_environment(),
            identity=publisher,
        )
        if _git(temporary, "rev-parse", "--verify", "HEAD", identity=publisher) != sha:
            raise HostConvergeError("published candidate did not bind exact SHA")
        if _git(
            temporary,
            "status",
            "--porcelain=v1",
            "--untracked-files=all",
            identity=publisher,
        ):
            raise HostConvergeError("published candidate is not clean")
        _run(
            (
                "chmod",
                "-R",
                "u=rX,g=rX,o=,a-w",
                str(temporary),
            ),
            identity=publisher,
        )
        _run(
            ("mv", "--no-target-directory", str(temporary), str(candidate)),
            identity=publisher,
        )
    finally:
        if _path_exists_as(temporary, publisher):
            _run(("chmod", "-R", "u+w", str(temporary)), identity=publisher)
            _run(("rm", "-rf", "--", str(temporary)), identity=publisher)
    return verify_candidate(profile, candidate, sha, publisher)


def _desired_payload(
    profile: Profile,
    sha: str,
    tree: str,
    *,
    previous_sha: str | None,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_path": str(profile.candidate_root / sha),
        "previous_sha": previous_sha,
        "secrets_env": str(profile.secrets_env),
        "admin_secret_file": str(profile.admin_secret),
    }


def _load_json(path: Path, label: str) -> dict[str, Any] | None:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise HostConvergeError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) & 0o077
    ):
        raise HostConvergeError(f"{label} is unsafe")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HostConvergeError(f"{label} is invalid") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError(f"{label} is invalid")
    return payload


def write_desired(profile: Profile, sha: str, tree: str) -> dict[str, Any] | None:
    previous = _load_json(profile.desired_file, "sandbox desired state")
    previous_sha = None
    if previous is not None:
        current = previous.get("candidate_sha")
        if isinstance(current, str) and current != sha:
            previous_sha = current
        elif isinstance(previous.get("previous_sha"), str):
            previous_sha = previous["previous_sha"]
    payload = _desired_payload(
        profile,
        sha,
        tree,
        previous_sha=previous_sha,
    )
    _atomic_write(
        profile.desired_file,
        (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )
    return previous


def _install_assets() -> None:
    _ensure_root_private_directory(CONFIG_ROOT)
    _ensure_root_private_directory(DESIRED_ROOT)
    _ensure_root_private_directory(PROFILE_CONFIG_ROOT)
    _atomic_write(
        INSTALLED_PROGRAM,
        Path(__file__).read_bytes(),
        mode=0o755,
    )
    _atomic_write(UNIT_PATH, SOURCE_UNIT.read_bytes(), mode=0o644)
    for sandbox in SANDBOXES:
        _atomic_write(
            PROFILE_CONFIG_ROOT / f"{sandbox}.toml",
            (SOURCE_PROFILES / f"{sandbox}.toml").read_bytes(),
            mode=0o600,
        )
    _run(("systemctl", "daemon-reload"))


def _read_admin_token(path: Path) -> str:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
        token = payload["admin"]["token"]
    except (OSError, KeyError, TypeError, tomllib.TOMLDecodeError) as exc:
        raise HostConvergeError("sandbox admin secret is invalid") from exc
    if not isinstance(token, str):
        raise HostConvergeError("sandbox admin secret is invalid")
    return token


def _request_json(
    url: str,
    *,
    token: str | None,
    expected: set[int],
) -> tuple[int, dict[str, Any]]:
    headers = {"Content-Type": "application/json"}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(
        url,
        data=b"{}",
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            status = response.status
            body = response.read()
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read()
    except urllib.error.URLError as exc:
        raise HostConvergeError("sandbox Control Plane is unavailable") from exc
    if status not in expected:
        raise HostConvergeError(f"sandbox Control Plane returned unexpected status {status}")
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError as exc:
        raise HostConvergeError("sandbox Control Plane returned invalid JSON") from exc
    if not isinstance(payload, dict):
        raise HostConvergeError("sandbox Control Plane returned invalid JSON")
    return status, payload


def _wait_for_control_plane(profile: Profile) -> None:
    url = f"http://127.0.0.1:{profile.ports['control_plane']}/healthz"
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(1)
    raise HostConvergeError("sandbox Control Plane did not become healthy")


def _update_secret_tokens(
    profile: Profile,
    identity: Identity,
    updates: Mapping[str, str],
) -> None:
    _assert_secure_file(profile.secrets_env, identity, "sandbox secret env file")
    values = _parse_env_file(profile.secrets_env)
    values.update(updates)
    _atomic_write(
        profile.secrets_env,
        _render_env(values),
        mode=0o600,
        identity=identity,
    )


def bootstrap_runtime_tokens(profile: Profile, identity: Identity) -> bool:
    _wait_for_control_plane(profile)
    values = _parse_env_file(profile.secrets_env)
    worker_token = values.get("LOOM_WORKER_TOKEN", "")
    register_url = f"http://127.0.0.1:{profile.ports['control_plane']}/workers/register"
    worker_status, _ = _request_json(
        register_url,
        token=worker_token,
        expected={400, 401},
    )
    batch_token = values.get("LOOM_SVC_BATCH_RUNNER_CP_TOKEN", "")
    if worker_status == 400 and batch_token:
        return False

    admin_token = _read_admin_token(profile.admin_secret)
    base = f"http://127.0.0.1:{profile.ports['control_plane']}/admin"
    updates: dict[str, str] = {}
    if worker_status == 401:
        _, worker_payload = _request_json(
            f"{base}/worker-tokens",
            token=admin_token,
            expected={201},
        )
        raw_worker = worker_payload.get("token")
        if not isinstance(raw_worker, str) or not raw_worker.startswith("loom_w_"):
            raise HostConvergeError("Control Plane returned an invalid worker token")
        updates["LOOM_WORKER_TOKEN"] = raw_worker
    if not batch_token or worker_status == 401:
        _, batch_payload = _request_json(
            f"{base}/batch-runner-tokens",
            token=admin_token,
            expected={201},
        )
        raw_batch = batch_payload.get("token")
        if not isinstance(raw_batch, str) or not raw_batch.startswith("loom_br_"):
            raise HostConvergeError("Control Plane returned an invalid batch token")
        updates["LOOM_SVC_BATCH_RUNNER_CP_TOKEN"] = raw_batch
    _update_secret_tokens(profile, identity, updates)
    return bool(updates)


def _candidate_environment(profile: Profile, candidate: Path) -> dict[str, str]:
    return {
        "PATH": "/usr/local/bin:/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "HOME": str(profile.runtime_root),
        "PYTHONPATH": str(candidate / "src"),
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_COUNT": "1",
        "GIT_CONFIG_KEY_0": "safe.directory",
        "GIT_CONFIG_VALUE_0": str(candidate),
        "PYTHONDONTWRITEBYTECODE": "1",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _invoke_lifecycle(profile: Profile, sha: str, operation: str) -> None:
    candidate = profile.candidate_root / sha
    program = candidate / "scripts/ops/developer_sandbox.py"
    profile_path = candidate / f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    owner = _identity(profile.sandbox, SHARED_GROUP)
    if (
        _run(
            ("test", "-f", str(program)),
            identity=owner,
            init_groups=True,
            expected={0, 1},
        ).returncode
        != 0
        or _run(
            ("test", "-f", str(profile_path)),
            identity=owner,
            init_groups=True,
            expected={0, 1},
        ).returncode
        != 0
    ):
        raise HostConvergeError("candidate sandbox lifecycle assets are unavailable")
    _run(
        (
            sys.executable,
            str(program),
            operation,
            "--profile",
            str(profile_path),
            "--source-repo",
            str(candidate),
            "--candidate-sha",
            sha,
            "--secrets-env",
            str(profile.secrets_env),
            "--admin-secret-file",
            str(profile.admin_secret),
            "--execute",
        ),
        env=_candidate_environment(profile, candidate),
        identity=owner,
        init_groups=True,
    )


def _desired_for_service(sandbox: str) -> tuple[Profile, dict[str, Any]]:
    profile = _load_profile(PROFILE_CONFIG_ROOT / f"{sandbox}.toml")
    desired = _load_json(profile.desired_file, "sandbox desired state")
    if desired is None or desired.get("sandbox") != sandbox:
        raise HostConvergeError("sandbox desired state is absent or invalid")
    return profile, desired


def _sandbox_state_sha(profile: Profile) -> str | None:
    state = _load_json(profile.state_file, "sandbox lifecycle state")
    if state is None:
        return None
    sha = state.get("candidate_sha")
    if not isinstance(sha, str) or SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("sandbox lifecycle state SHA is invalid")
    return sha


def _validate_desired_binding(
    profile: Profile,
    desired: Mapping[str, Any],
    *,
    sha: str,
    tree: str,
) -> None:
    expected = {
        "schema_version": 1,
        "sandbox": profile.sandbox,
        "candidate_sha": sha,
        "candidate_tree": tree,
        "candidate_path": str(profile.candidate_root / sha),
        "secrets_env": str(profile.secrets_env),
        "admin_secret_file": str(profile.admin_secret),
    }
    if any(desired.get(key) != value for key, value in expected.items()):
        raise HostConvergeError("sandbox desired state binding is invalid")
    previous = desired.get("previous_sha")
    if previous is not None and (
        not isinstance(previous, str) or SHA_RE.fullmatch(previous) is None or previous == sha
    ):
        raise HostConvergeError("sandbox desired rollback binding is invalid")


def service_converge(sandbox: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    profile, desired = _desired_for_service(sandbox)
    sha = str(desired["candidate_sha"])
    publisher = _identity(PUBLISH_USER, SHARED_GROUP)
    owner = _identity(sandbox, SHARED_GROUP)
    verify_candidate_root(profile, publisher)
    tree = verify_candidate(profile, profile.candidate_root / sha, sha, publisher)
    _validate_desired_binding(profile, desired, sha=sha, tree=tree)
    ensure_secret_files(profile, owner)
    current = _sandbox_state_sha(profile)
    if current is None:
        _invoke_lifecycle(profile, sha, "create")
    else:
        _invoke_lifecycle(profile, sha, "update")
    if bootstrap_runtime_tokens(profile, owner):
        _invoke_lifecycle(profile, sha, "update")
    _invoke_lifecycle(profile, sha, "check")
    verify_listening_ports(profile)


def service_check(sandbox: str) -> None:
    _require_live_host()
    verify_nfs_mount()
    verify_state_parent()
    profile, desired = _desired_for_service(sandbox)
    sha = str(desired["candidate_sha"])
    verify_candidate_root(profile, _identity(PUBLISH_USER, SHARED_GROUP))
    tree = verify_candidate(
        profile,
        profile.candidate_root / sha,
        sha,
        _identity(PUBLISH_USER, SHARED_GROUP),
    )
    _validate_desired_binding(profile, desired, sha=sha, tree=tree)
    verify_secret_files(profile, _identity(sandbox, SHARED_GROUP))
    _invoke_lifecycle(profile, sha, "check")
    verify_listening_ports(profile)


def verify_listening_ports(profile: Profile) -> None:
    result = _run(("ss", "-H", "-ltn"))
    listeners: set[tuple[str, int]] = set()
    for line in result.stdout.splitlines():
        fields = line.split()
        if len(fields) < 4:
            continue
        local = fields[3]
        host, separator, raw_port = local.rpartition(":")
        if not separator:
            continue
        try:
            port = int(raw_port)
        except ValueError:
            continue
        listeners.add((host.strip("[]"), port))
    missing = sorted(
        port for port in profile.ports.values() if ("127.0.0.1", port) not in listeners
    )
    if missing:
        raise HostConvergeError(
            "sandbox loopback ports are not listening: " + ", ".join(str(port) for port in missing),
        )


def verify_nfs_mount() -> None:
    result = _run(("findmnt", "-n", "-o", "FSTYPE,TARGET", "-T", str(NFS_ROOT)))
    fields = result.stdout.split()
    if len(fields) != 2 or fields[0] not in {"nfs", "nfs4"} or fields[1] != "/shared_work":
        raise HostConvergeError("candidate namespace is not on the expected /shared_work NFS mount")


def verify_state_parent() -> None:
    shared = _identity(PUBLISH_USER, SHARED_GROUP)
    try:
        metadata = STATE_PARENT.lstat()
    except OSError as exc:
        raise HostConvergeError("sandbox state parent is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or stat.S_IMODE(metadata.st_mode) != 0o2750
        or (metadata.st_uid, metadata.st_gid) != (0, shared.gid)
    ):
        raise HostConvergeError("sandbox state parent owner or mode is invalid")


def _require_live_host() -> None:
    if os.geteuid() != 0:
        raise HostConvergeError("host convergence must run as root")
    hostname = socket.gethostname().rstrip(".").lower()
    if hostname != EXPECTED_HOSTNAME:
        raise HostConvergeError(
            f"host convergence requires {EXPECTED_HOSTNAME}, got {hostname}",
        )


def _migration_tree(candidate: Path, publisher: Identity) -> str:
    result = _run(
        (
            "git",
            "-c",
            f"safe.directory={candidate}",
            "-C",
            str(candidate),
            "rev-parse",
            "--verify",
            "HEAD:migrations",
        ),
        env=_clean_git_environment(),
        identity=publisher,
    )
    tree = result.stdout.strip()
    if SHA_RE.fullmatch(tree) is None:
        raise HostConvergeError("candidate migration tree is invalid")
    return tree


def verify_developer_docker_access(identity: Identity) -> None:
    _run(
        ("docker", "info", "--format", "{{.ServerVersion}}"),
        env={"PATH": "/usr/local/bin:/usr/bin:/bin", "LANG": "C.UTF-8"},
        identity=identity,
        init_groups=True,
    )


def verify_candidate_consumer(profile: Profile, sha: str, identity: Identity) -> None:
    candidate = profile.candidate_root / sha
    for relative in (
        "scripts/ops/developer_sandbox.py",
        f"deploy/developer-sandboxes/{profile.sandbox}.toml",
        "deploy/docker-compose.dev.yml",
    ):
        result = _run(
            ("test", "-r", str(candidate / relative)),
            identity=identity,
            init_groups=True,
            expected={0, 1},
        )
        if result.returncode != 0:
            raise HostConvergeError(
                f"{profile.sandbox} cannot read the immutable candidate through sharedwork",
            )


def verify_candidate_profile_bytes(profile: Profile, sha: str, publisher: Identity) -> None:
    source = SOURCE_PROFILES / f"{profile.sandbox}.toml"
    candidate = profile.candidate_root / sha / f"deploy/developer-sandboxes/{profile.sandbox}.toml"
    expected = hashlib.sha256(source.read_bytes()).hexdigest()
    result = _run(("sha256sum", str(candidate)), identity=publisher)
    actual = result.stdout.split(maxsplit=1)[0] if result.stdout else ""
    if actual != expected:
        raise HostConvergeError(
            f"candidate changed the fixed host profile for {profile.sandbox}",
        )


def require_migration_compatible_update(
    profile: Profile,
    target_sha: str,
    publisher: Identity,
) -> None:
    desired = _load_json(profile.desired_file, "sandbox desired state")
    if desired is None:
        return
    current_sha = desired.get("candidate_sha")
    if not isinstance(current_sha, str) or SHA_RE.fullmatch(current_sha) is None:
        raise HostConvergeError("sandbox desired SHA is invalid")
    if current_sha == target_sha:
        return
    current = profile.candidate_root / current_sha
    verify_candidate(profile, current, current_sha, publisher)
    if _migration_tree(current, publisher) != _migration_tree(
        profile.candidate_root / target_sha,
        publisher,
    ):
        raise HostConvergeError(
            "candidate update crosses a migration-tree change; "
            "use a reviewed backup and restore workflow",
        )


def rollback(profile: Profile, target_sha: str) -> None:
    desired = _load_json(profile.desired_file, "sandbox desired state")
    if desired is None:
        raise HostConvergeError("sandbox desired state is absent")
    current_sha = desired.get("candidate_sha")
    if target_sha != desired.get("previous_sha") or not isinstance(current_sha, str):
        raise HostConvergeError("rollback target must equal the recorded previous SHA")
    publisher = _identity(PUBLISH_USER, SHARED_GROUP)
    current = profile.candidate_root / current_sha
    target = profile.candidate_root / target_sha
    verify_candidate(profile, current, current_sha, publisher)
    target_tree = verify_candidate(profile, target, target_sha, publisher)
    if _migration_tree(current, publisher) != _migration_tree(target, publisher):
        raise HostConvergeError(
            "rollback crosses a migration-tree change; restore a reviewed data backup instead",
        )
    replacement = _desired_payload(
        profile,
        target_sha,
        target_tree,
        previous_sha=current_sha,
    )
    _atomic_write(
        profile.desired_file,
        (json.dumps(replacement, sort_keys=True, separators=(",", ":")) + "\n").encode(),
        mode=0o600,
    )
    try:
        _run(("systemctl", "start", UNIT_NAME.format(sandbox=profile.sandbox)))
    except HostConvergeError:
        _atomic_write(
            profile.desired_file,
            (json.dumps(desired, sort_keys=True, separators=(",", ":")) + "\n").encode(),
            mode=0o600,
        )
        raise


def _nfs_readback_commands(profile: Profile, sha: str) -> list[list[str]]:
    path = profile.candidate_root / sha
    remote = ["stat", "-Lc", "%i:%u:%g:%a:%n", str(profile.candidate_root), str(path)]
    return [
        ["ssh", "-o", "BatchMode=yes", host, "--", *remote]
        for host in ("oldlab-1", "oldlab-2", "oldlab-3", "oldlab-4", "oldlab-5")
    ]


def plan_document(profiles: Sequence[Profile], sha: str, operation: str) -> dict[str, Any]:
    if SHA_RE.fullmatch(sha) is None:
        raise HostConvergeError("candidate SHA must be full lowercase 40-hex")
    rows = []
    for profile in profiles:
        rows.append(
            {
                "sandbox": profile.sandbox,
                "compose_project": profile.compose_project,
                "candidate": str(profile.candidate_root / sha),
                "candidate_owner": f"{PUBLISH_USER}:{SHARED_GROUP}",
                "candidate_writable": False,
                "state_root": str(profile.state_root),
                "private_owner": f"{profile.sandbox}:{SHARED_GROUP}",
                "private_mode": "0700",
                "secrets_env": str(profile.secrets_env),
                "admin_secret_file": str(profile.admin_secret),
                "secret_mode": "0600",
                "ports": profile.ports,
                "unit": UNIT_NAME.format(sandbox=profile.sandbox),
                "nfs_readback_commands": _nfs_readback_commands(profile, sha),
            },
        )
    return {
        "schema_version": 1,
        "artifact_type": "developer-sandbox-host-plan",
        "operation": operation,
        "mutation_authorized": False,
        "host": EXPECTED_HOSTNAME,
        "remote_url": REMOTE_URL,
        "candidate_sha": sha,
        "sandboxes": rows,
        "rollback": {
            "preserves_compose_volumes": True,
            "requires_recorded_previous_sha": True,
            "requires_equal_migration_tree": True,
        },
    }


def install(profiles: Sequence[Profile], sha: str) -> None:
    _require_live_host()
    publisher = _identity(PUBLISH_USER, SHARED_GROUP)
    verify_nfs_mount()
    verify_state_parent()
    _install_assets()
    fingerprints: dict[tuple[str, str], str] = {}
    candidates: list[tuple[Profile, str]] = []
    for profile in profiles:
        owner = _identity(profile.sandbox, SHARED_GROUP)
        verify_developer_docker_access(owner)
        ensure_secret_files(profile, owner)
        tree = publish_candidate(profile, sha, publisher)
        verify_candidate_profile_bytes(profile, sha, publisher)
        verify_candidate_consumer(profile, sha, owner)
        require_migration_compatible_update(profile, sha, publisher)
        values = _parse_env_file(profile.secrets_env)
        admin = _read_admin_token(profile.admin_secret)
        for key in (
            "LOOM_DEV_POSTGRES_PASSWORD",
            "LOOM_DEV_MINIO_ROOT_PASSWORD",
            "LOOM_CP_STEP_JWT_SIGNING_KEY",
            "LOOM_SECRET_STORE_MASTER_KEY",
            "LOOM_WORKER_TOKEN",
        ):
            fingerprints[(profile.sandbox, key)] = hashlib.sha256(
                values[key].encode(),
            ).hexdigest()
        fingerprints[(profile.sandbox, "admin")] = hashlib.sha256(
            admin.encode(),
        ).hexdigest()
        candidates.append((profile, tree))
    for key in {key for _, key in fingerprints}:
        values = [
            fingerprint
            for (sandbox, candidate_key), fingerprint in fingerprints.items()
            if candidate_key == key
        ]
        if len(values) != len(set(values)):
            raise HostConvergeError(f"cross-sandbox secret collision detected for {key}")
    prepared = [(profile, write_desired(profile, sha, tree)) for profile, tree in candidates]
    for profile, previous in prepared:
        unit = UNIT_NAME.format(sandbox=profile.sandbox)
        try:
            _run(("systemctl", "enable", unit))
            _run(("systemctl", "start", unit))
        except HostConvergeError:
            if previous is not None:
                _atomic_write(
                    profile.desired_file,
                    (json.dumps(previous, sort_keys=True, separators=(",", ":")) + "\n").encode(),
                    mode=0o600,
                )
                try:
                    _run(("systemctl", "reset-failed", unit))
                    _run(("systemctl", "start", unit))
                except HostConvergeError as recovery_exc:
                    raise HostConvergeError(
                        f"{profile.sandbox} activation and previous-candidate recovery both failed",
                    ) from recovery_exc
            raise


def _select_profiles(all_profiles: Sequence[Profile], sandbox: str) -> tuple[Profile, ...]:
    if sandbox == "all":
        return tuple(all_profiles)
    return tuple(profile for profile in all_profiles if profile.sandbox == sandbox)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("plan", "install", "check"):
        child = subparsers.add_parser(command)
        child.add_argument("--candidate-sha", required=True)
        child.add_argument("--sandbox", choices=(*SANDBOXES, "all"), default="all")
        if command != "plan":
            child.add_argument("--execute", action="store_true")
    rollback_parser = subparsers.add_parser("rollback")
    rollback_parser.add_argument("--sandbox", choices=SANDBOXES, required=True)
    rollback_parser.add_argument("--candidate-sha", required=True)
    rollback_parser.add_argument("--execute", action="store_true")
    for command in ("service-converge", "service-check"):
        child = subparsers.add_parser(command)
        child.add_argument("--sandbox", choices=SANDBOXES, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(list(argv) if argv is not None else None)
    try:
        if args.command == "service-converge":
            service_converge(args.sandbox)
            result = {"status": "succeeded", "sandbox": args.sandbox}
        elif args.command == "service-check":
            service_check(args.sandbox)
            result = {"status": "succeeded", "sandbox": args.sandbox}
        else:
            profiles = load_profiles()
            selected = _select_profiles(profiles, args.sandbox)
            result = plan_document(selected, args.candidate_sha, args.command)
            execute = bool(getattr(args, "execute", False))
            if execute and args.command == "install":
                install(selected, args.candidate_sha)
                result = {**result, "mutation_authorized": True, "status": "succeeded"}
            elif execute and args.command == "check":
                for profile in selected:
                    service_check(profile.sandbox)
                result = {
                    **result,
                    "mutation_authorized": False,
                    "verified": True,
                    "status": "succeeded",
                }
            elif execute and args.command == "rollback":
                rollback(selected[0], args.candidate_sha)
                result = {**result, "mutation_authorized": True, "status": "succeeded"}
        sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
        return 0
    except HostConvergeError as exc:
        sys.stderr.write(f"error: {exc}\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
