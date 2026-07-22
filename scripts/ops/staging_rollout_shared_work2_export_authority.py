#!/usr/bin/python3
"""Install and enforce the fixed GB10 exporter authority boundary.

The runtime surface accepts only ``install`` and ``check``.  A separate
one-time ``bootstrap`` verb must be run by an external root administrator from
the fixed, root-owned sealed checkout; it is intentionally absent from the
sudoers rule installed for the coordinator.
"""

from __future__ import annotations

import argparse
import ctypes
import errno
import fcntl
import hashlib
import importlib.util
import json
import os
import pwd
import stat
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

APPROVED_BASE_SHA = "eed7ff5eb438cb1d9a715a8afa49da94e9fee5eb"
OPERATOR = "qianyi"
SOURCE_ROOT = Path("/opt/loom-staging-exporter-authority/source")
LIBEXEC = Path("/usr/local/libexec/loom-staging-rollout-shared-work2-export-authority")
VALIDATOR = Path("/usr/local/libexec/staging_rollout_sealed_source.py")
POLICY = Path("/etc/loom/staging-rollout-shared-work2-export-authority.json")
SUDOERS = Path("/etc/sudoers.d/loom-staging-rollout-shared-work2-export-authority")
STATE_ROOT = Path("/var/lib/loom-staging-exporter-authority")
LOCK = STATE_ROOT / "authority.lock"
JOURNAL = STATE_ROOT / "journal.jsonl"
HELPER_RELATIVE = Path("scripts/ops/staging_rollout_shared_work2_export.py")
VALIDATOR_RELATIVE = Path("scripts/ops/staging_rollout_sealed_source.py")
SUDOERS_RELATIVE = Path(
    "deploy/worker-pools/gb10/loom-staging-rollout-shared-work2-export-authority.sudoers"
)
SCHEMA_VERSION = 1
_SHA = frozenset("0123456789abcdef")
_MAX_ASSET_BYTES = 1_048_576


class AuthorityError(RuntimeError):
    """A bounded exporter-authority failure safe for operator output."""


Runner = Callable[[Sequence[str], Mapping[str, str]], subprocess.CompletedProcess[str]]


@dataclass(frozen=True, slots=True)
class AuthorityPolicy:
    source_sha: str
    source_tree_sha: str
    source_base_sha: str
    wrapper_sha256: str
    validator_sha256: str
    sudoers_sha256: str

    def __post_init__(self) -> None:
        for value in (self.source_sha, self.source_tree_sha, self.source_base_sha):
            if len(value) != 40 or any(character not in _SHA for character in value):
                raise AuthorityError("export authority source identity is invalid")
        for value in (self.wrapper_sha256, self.validator_sha256, self.sudoers_sha256):
            if len(value) != 64 or any(character not in _SHA for character in value):
                raise AuthorityError("export authority asset identity is invalid")
        if self.source_base_sha != APPROVED_BASE_SHA:
            raise AuthorityError("export authority approved base is invalid")

    def payload(self) -> bytes:
        value = {
            "schema_version": SCHEMA_VERSION,
            "source_mode": "sealed-cumulative",
            "source_sha": self.source_sha,
            "source_tree_sha": self.source_tree_sha,
            "source_base_sha": self.source_base_sha,
            "wrapper_sha256": self.wrapper_sha256,
            "validator_sha256": self.validator_sha256,
            "sudoers_sha256": self.sudoers_sha256,
        }
        return (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")


def _run(argv: Sequence[str], env: Mapping[str, str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=False, capture_output=True, text=True, env=dict(env))


def _clean_env() -> dict[str, str]:
    return {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "GIT_OPTIONAL_LOCKS": "0",
    }


def _sha256(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _read_all(descriptor: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, min(65_536, _MAX_ASSET_BYTES + 1 - total))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        total += len(chunk)
        if total > _MAX_ASSET_BYTES:
            raise AuthorityError("export authority asset exceeds its size bound")


def _write_all(descriptor: int, payload: bytes) -> None:
    view = memoryview(payload)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:  # pragma: no cover - operating-system invariant
            raise AuthorityError("export authority write failed safely")
        view = view[written:]


def _regular_root_file(path: Path, *, mode: int, payload: bytes | None = None) -> bytes:
    try:
        lexical = os.lstat(path)
        descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW)
    except OSError as exc:
        raise AuthorityError("export authority asset is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        content = _read_all(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_ISLNK(lexical.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_nlink != 1
            or (metadata.st_dev, metadata.st_ino) != (lexical.st_dev, lexical.st_ino)
            or (payload is not None and content != payload)
        ):
            raise AuthorityError("export authority asset metadata is unsafe")
        return content
    finally:
        os.close(descriptor)


def _safe_root_directory(path: Path, *, mode: int) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise AuthorityError("export authority directory is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != mode
    ):
        raise AuthorityError("export authority directory is unsafe")


def _ensure_root_directory(path: Path, *, mode: int) -> bool:
    try:
        _safe_root_directory(path, mode=mode)
        return False
    except AuthorityError:
        try:
            os.lstat(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise AuthorityError("export authority directory is unavailable") from exc
        else:
            raise
    _safe_root_directory(path.parent, mode=0o755)
    created = False
    try:
        os.mkdir(path, mode)
        created = True
        os.chown(path, 0, 0)
        os.chmod(path, mode)
        _safe_root_directory(path, mode=mode)
    except (AuthorityError, OSError) as exc:
        if created:
            try:
                os.rmdir(path)
            except OSError as rollback_exc:
                raise AuthorityError(
                    "export authority directory creation rollback failed safely"
                ) from rollback_exc
        raise AuthorityError("export authority directory creation failed safely") from exc
    return True


def _read_policy() -> AuthorityPolicy:
    payload = _regular_root_file(POLICY, mode=0o600)
    try:
        raw = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AuthorityError("export authority policy is invalid") from exc
    expected = {
        "schema_version",
        "source_mode",
        "source_sha",
        "source_tree_sha",
        "source_base_sha",
        "wrapper_sha256",
        "validator_sha256",
        "sudoers_sha256",
    }
    if (
        not isinstance(raw, dict)
        or set(raw) != expected
        or raw["schema_version"] != SCHEMA_VERSION
        or raw["source_mode"] != "sealed-cumulative"
        or any(not isinstance(raw[key], str) for key in expected - {"schema_version"})
    ):
        raise AuthorityError("export authority policy is invalid")
    return AuthorityPolicy(
        source_sha=raw["source_sha"],
        source_tree_sha=raw["source_tree_sha"],
        source_base_sha=raw["source_base_sha"],
        wrapper_sha256=raw["wrapper_sha256"],
        validator_sha256=raw["validator_sha256"],
        sudoers_sha256=raw["sudoers_sha256"],
    )


def _validate_runtime_assets(policy: AuthorityPolicy) -> None:
    if _sha256(_regular_root_file(LIBEXEC, mode=0o755)) != policy.wrapper_sha256:
        raise AuthorityError("export authority wrapper drifted")
    if _sha256(_regular_root_file(VALIDATOR, mode=0o644)) != policy.validator_sha256:
        raise AuthorityError("export authority validator drifted")
    if _sha256(_regular_root_file(SUDOERS, mode=0o440)) != policy.sudoers_sha256:
        raise AuthorityError("export authority sudoers drifted")


def _load_validator(path: Path) -> Any:
    module_name = "_loom_sealed_source_validator"
    specification = importlib.util.spec_from_file_location(module_name, path)
    if specification is None or specification.loader is None:
        raise AuthorityError("export authority source validator is unavailable")
    module = importlib.util.module_from_spec(specification)
    previous = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        specification.loader.exec_module(module)
    except Exception as exc:
        raise AuthorityError("export authority source validator failed safely") from exc
    finally:
        if previous is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = previous
    if not hasattr(module, "SealedSource") or not hasattr(module, "validate_sealed_source"):
        raise AuthorityError("export authority source validator is invalid")
    return module


def _validate_source(policy: AuthorityPolicy) -> object:
    validator = _load_validator(VALIDATOR)
    source = validator.SealedSource(
        path=SOURCE_ROOT,
        commit_sha=policy.source_sha,
        tree_sha=policy.source_tree_sha,
        base_sha=policy.source_base_sha,
    )
    try:
        validator.validate_sealed_source(source)
    except Exception as exc:
        raise AuthorityError("export authority sealed source failed validation") from exc
    helper = SOURCE_ROOT / HELPER_RELATIVE
    sudoers = SOURCE_ROOT / SUDOERS_RELATIVE
    if not helper.is_file() or helper.is_symlink() or not sudoers.is_file() or sudoers.is_symlink():
        raise AuthorityError("export authority source assets are unavailable")
    return source


def _validate_invoker(verb: str, environ: Mapping[str, str]) -> None:
    try:
        account = pwd.getpwnam(OPERATOR)
    except KeyError as exc:
        raise AuthorityError("export authority operator identity is unavailable") from exc
    expected_command = f"{LIBEXEC} {verb}"
    if (
        os.geteuid() != 0
        or environ.get("SUDO_USER") != OPERATOR
        or environ.get("SUDO_UID") != str(account.pw_uid)
        or environ.get("SUDO_GID") != str(account.pw_gid)
        or environ.get("SUDO_COMMAND") != expected_command
    ):
        raise AuthorityError("export authority invocation is not approved")


def _open_lock(*, exclusive: bool) -> int:
    flags = os.O_RDWR if exclusive else os.O_RDONLY
    try:
        descriptor = os.open(LOCK, flags | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW)
        metadata = os.fstat(descriptor)
    except OSError as exc:
        raise AuthorityError("export authority lock is unavailable") from exc
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o600
        or metadata.st_nlink != 1
    ):
        os.close(descriptor)
        raise AuthorityError("export authority lock is unsafe")
    operation = fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH
    try:
        fcntl.flock(descriptor, operation | fcntl.LOCK_NB)
    except BlockingIOError as exc:
        os.close(descriptor)
        raise AuthorityError("export authority is busy") from exc
    return descriptor


def _journal_install(policy: AuthorityPolicy, *, changed: bool) -> None:
    record = {
        "action": "install",
        "changed": changed,
        "operator": OPERATOR,
        "source_base_sha": policy.source_base_sha,
        "source_sha": policy.source_sha,
        "source_tree_sha": policy.source_tree_sha,
        "timestamp_ns": time.time_ns(),
    }
    payload = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
    descriptor = os.open(
        JOURNAL,
        os.O_WRONLY | os.O_APPEND | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
    )
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
        ):
            raise AuthorityError("export authority journal is unsafe")
        _write_all(descriptor, payload)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_journal(policy: AuthorityPolicy) -> None:
    payload = _regular_root_file(JOURNAL, mode=0o600)
    for line in payload.splitlines():
        try:
            record = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AuthorityError("export authority journal is invalid") from exc
        if (
            not isinstance(record, dict)
            or set(record)
            != {
                "action",
                "changed",
                "operator",
                "source_base_sha",
                "source_sha",
                "source_tree_sha",
                "timestamp_ns",
            }
            or record["action"] != "install"
            or not isinstance(record["changed"], bool)
            or record["operator"] != OPERATOR
            or record["source_base_sha"] != policy.source_base_sha
            or record["source_sha"] != policy.source_sha
            or record["source_tree_sha"] != policy.source_tree_sha
            or not isinstance(record["timestamp_ns"], int)
            or record["timestamp_ns"] <= 0
        ):
            raise AuthorityError("export authority journal is invalid")


def _invoke_helper(verb: str, policy: AuthorityPolicy, run: Runner) -> bool:
    argv = (
        "/usr/bin/python3",
        str(SOURCE_ROOT / HELPER_RELATIVE),
        verb,
        "--sealed-source-sha",
        policy.source_sha,
        "--sealed-source-tree",
        policy.source_tree_sha,
        "--sealed-approved-base-sha",
        policy.source_base_sha,
    )
    result = run(argv, _clean_env())
    if result.returncode != 0 or result.stderr or result.stdout not in {"ok\n", "changed\n"}:
        raise AuthorityError("fixed exporter helper failed safely")
    return result.stdout == "changed\n"


def dispatch(verb: str, *, environ: Mapping[str, str], run: Runner = _run) -> dict[str, object]:
    if verb not in {"install", "check"}:
        raise AuthorityError("export authority verb is invalid")
    _validate_invoker(verb, environ)
    descriptor = _open_lock(exclusive=verb == "install")
    try:
        policy = _read_policy()
        _validate_runtime_assets(policy)
        _validate_source(policy)
        if verb == "install":
            _validate_journal(policy)
        changed = _invoke_helper(verb, policy, run)
        if verb == "install":
            _journal_install(policy, changed=changed)
        return {
            "action": verb,
            "changed": changed,
            "source_base_sha": policy.source_base_sha,
            "source_sha": policy.source_sha,
            "source_tree_sha": policy.source_tree_sha,
            "status": "ok",
        }
    finally:
        os.close(descriptor)


def _atomic_install(path: Path, payload: bytes, mode: int) -> bool:
    _safe_root_directory(path.parent, mode=0o755)
    try:
        existing = _regular_root_file(path, mode=mode)
    except AuthorityError:
        if path.exists() or path.is_symlink():
            raise
    else:
        if existing != payload:
            raise AuthorityError("export authority installed asset drifted")
        return False
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    temporary = f".{path.name}.new-{os.getpid()}"
    published = False
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0) | os.O_NOFOLLOW,
            0o600,
            dir_fd=directory,
        )
        try:
            try:
                _write_all(descriptor, payload)
                os.fchown(descriptor, 0, 0)
                os.fchmod(descriptor, mode)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except (AuthorityError, OSError) as exc:
            try:
                os.unlink(temporary, dir_fd=directory)
            except FileNotFoundError:
                pass
            except OSError as rollback_exc:
                raise AuthorityError(
                    "export authority asset publication rollback failed safely"
                ) from rollback_exc
            raise AuthorityError("export authority asset publication failed safely") from exc
        try:
            _rename_noreplace(directory, temporary, path.name)
            published = True
            os.fsync(directory)
        except FileExistsError as exc:
            try:
                os.unlink(temporary, dir_fd=directory)
            except OSError as rollback_exc:
                raise AuthorityError(
                    "export authority asset publication rollback failed safely"
                ) from rollback_exc
            raise AuthorityError("export authority asset raced with another writer") from exc
        except (AuthorityError, OSError) as exc:
            try:
                os.unlink(path.name if published else temporary, dir_fd=directory)
            except FileNotFoundError:
                pass
            except OSError as rollback_exc:
                raise AuthorityError(
                    "export authority asset publication rollback failed safely"
                ) from rollback_exc
            try:
                os.fsync(directory)
            except OSError as rollback_exc:
                raise AuthorityError(
                    "export authority asset publication rollback failed safely"
                ) from rollback_exc
            raise AuthorityError("export authority asset publication failed safely") from exc
    except OSError as exc:
        raise AuthorityError("export authority asset publication failed safely") from exc
    finally:
        os.close(directory)
    return True


def _rename_noreplace(directory: int, source: str, destination: str) -> None:
    library = ctypes.CDLL(None, use_errno=True)
    try:
        rename = library.renameat2
    except AttributeError as exc:
        raise AuthorityError("atomic export authority publication is unavailable") from exc
    rename.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint]
    rename.restype = ctypes.c_int
    if rename(directory, os.fsencode(source), directory, os.fsencode(destination), 1) == 0:
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, "destination exists")
    raise AuthorityError("atomic export authority publication failed safely")


def _rollback_created(paths: Sequence[Path]) -> None:
    failures = []
    for path in reversed(paths):
        try:
            os.unlink(path)
        except OSError:
            failures.append(str(path))
    if failures:
        raise AuthorityError("export authority bootstrap rollback failed safely")


def _rollback_bootstrap(
    created_assets: Sequence[Path], created_directories: Sequence[Path]
) -> None:
    failures: list[str] = []
    try:
        _rollback_created(created_assets)
    except AuthorityError:
        failures.extend(str(path) for path in created_assets)
    for directory in reversed(created_directories):
        try:
            os.rmdir(directory)
        except OSError:
            failures.append(str(directory))
    if failures:
        raise AuthorityError("export authority bootstrap rollback failed safely")


def bootstrap(source_sha: str, source_tree_sha: str, *, run: Runner = _run) -> dict[str, object]:
    if os.geteuid() != 0 or any(key.startswith("SUDO_") for key in os.environ):
        raise AuthorityError(
            "export authority bootstrap requires a direct external root administrator"
        )
    expected_entrypoint = SOURCE_ROOT / Path("scripts/ops") / Path(__file__).name
    if Path(__file__).resolve() != expected_entrypoint:
        raise AuthorityError("export authority bootstrap entrypoint is not the fixed sealed source")
    _safe_root_directory(SOURCE_ROOT, mode=0o700)
    source_validator_path = SOURCE_ROOT / VALIDATOR_RELATIVE
    _regular_root_file(source_validator_path, mode=0o644)
    validator = _load_validator(source_validator_path)
    source = validator.SealedSource(SOURCE_ROOT, source_sha, source_tree_sha, APPROVED_BASE_SHA)
    try:
        validator.validate_sealed_source(source)
    except Exception as exc:
        raise AuthorityError("export authority sealed source failed validation") from exc
    relative_wrapper = Path(__file__).resolve().relative_to(Path(__file__).resolve().parents[2])
    wrapper_payload = _regular_root_file(SOURCE_ROOT / relative_wrapper, mode=0o644)
    validator_payload = _regular_root_file(SOURCE_ROOT / VALIDATOR_RELATIVE, mode=0o644)
    sudoers_payload = _regular_root_file(SOURCE_ROOT / SUDOERS_RELATIVE, mode=0o644)
    policy = AuthorityPolicy(
        source_sha=source_sha,
        source_tree_sha=source_tree_sha,
        source_base_sha=APPROVED_BASE_SHA,
        wrapper_sha256=_sha256(wrapper_payload),
        validator_sha256=_sha256(validator_payload),
        sudoers_sha256=_sha256(sudoers_payload),
    )
    validation = run(("/usr/sbin/visudo", "-cf", str(SOURCE_ROOT / SUDOERS_RELATIVE)), _clean_env())
    if validation.returncode != 0:
        raise AuthorityError("export authority sudoers asset is invalid")
    created_directories: list[Path] = []
    created: list[Path] = []
    try:
        # Directory convergence is part of the same transaction as asset
        # publication.  In particular, bootstrap owns creation of libexec;
        # an external administrator must not precreate it as an extra step.
        for directory in (POLICY.parent, STATE_ROOT, LIBEXEC.parent):
            if _ensure_root_directory(directory, mode=0o755):
                created_directories.append(directory)
        for directory in (SUDOERS.parent, LOCK.parent):
            _safe_root_directory(directory, mode=0o755)
        # Sudoers is deliberately published last, after every dependency exists.
        for path, payload, mode in (
            (LIBEXEC, wrapper_payload, 0o755),
            (VALIDATOR, validator_payload, 0o644),
            (POLICY, policy.payload(), 0o600),
            (LOCK, b"", 0o600),
        ):
            if _atomic_install(path, payload, mode):
                created.append(path)
        try:
            _validate_journal(policy)
        except AuthorityError:
            if JOURNAL.exists() or JOURNAL.is_symlink():
                raise
            if _atomic_install(JOURNAL, b"", 0o600):
                created.append(JOURNAL)
        if _atomic_install(SUDOERS, sudoers_payload, 0o440):
            created.append(SUDOERS)
        installed_validation = run(("/usr/sbin/visudo", "-cf", str(SUDOERS)), _clean_env())
        if installed_validation.returncode != 0:
            raise AuthorityError("installed export authority sudoers is invalid")
    except (AuthorityError, OSError):
        _rollback_bootstrap(created, created_directories)
        raise
    return {
        "action": "bootstrap",
        "changed": [str(path) for path in created],
        "source_base_sha": policy.source_base_sha,
        "source_sha": policy.source_sha,
        "source_tree_sha": policy.source_tree_sha,
        "status": "ok",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    bootstrap_parser = subparsers.add_parser("bootstrap")
    bootstrap_parser.add_argument("--source-sha", required=True)
    bootstrap_parser.add_argument("--source-tree-sha", required=True)
    subparsers.add_parser("install")
    subparsers.add_parser("check")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "bootstrap":
            report = bootstrap(args.source_sha, args.source_tree_sha)
        else:
            report = dispatch(args.command, environ=os.environ)
    except (AuthorityError, OSError):
        print("error: shared_work2 exporter authority failed safely", file=sys.stderr)
        return 1
    print(json.dumps(report, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
