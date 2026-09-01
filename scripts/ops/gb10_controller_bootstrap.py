#!/usr/bin/python3
"""Trusted root entrypoint for the GB10 autoscaler-controller installer."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import os
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import NoReturn

_REMOTE_URL = "https://github.com/qianyi-sun/loom.git"
_TRUSTED_ROOT = Path("/opt/loom-gb10-controller-bootstrap")
_GIT = Path("/usr/bin/git")
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_MAX_ARTIFACT_BYTES = 4 * 1024 * 1024
_SOURCE_ARTIFACTS = (
    Path("deploy/slurm/install-loom-gb10-autoscaler-controller.sh"),
    Path("deploy/slurm/loom-gb10-slurm-authority.tmpfiles"),
    Path("scripts/ops/gb10_controller_bootstrap.py"),
    Path("scripts/ops/gb10_external_supervisor_broker.py"),
    Path("scripts/ops/gb10_slurm_acceptance_authority.py"),
    Path("scripts/ops/install_gb10_autoscaler_controller.py"),
)
_LAUNCHER_RELATIVE = Path("deploy/slurm/install-loom-gb10-autoscaler-controller.sh")
_ROOT_ENVIRONMENT = {
    "GIT_CONFIG_GLOBAL": "/dev/null",
    "GIT_CONFIG_NOSYSTEM": "1",
    "GIT_OPTIONAL_LOCKS": "0",
    "GIT_TERMINAL_PROMPT": "0",
    "HOME": "/",
    "LANG": "C.UTF-8",
    "LC_ALL": "C.UTF-8",
    "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}


class ControllerBootstrapError(RuntimeError):
    """The trusted controller bootstrap could not prove a safe source."""


@dataclass(frozen=True, slots=True)
class BootstrapContext:
    """Immutable source authority for one exact controller bootstrap."""

    source_sha: str
    trusted_root: Path = _TRUSTED_ROOT
    remote_url: str = _REMOTE_URL
    authority_uid: int = 0
    authority_gid: int = 0
    git_path: Path = _GIT
    allow_file_remote: bool = False


@dataclass(frozen=True, slots=True)
class PreparedSource:
    """A sealed exact checkout that is safe for the root launcher to execute."""

    source_root: Path
    launcher_path: Path
    artifact_sha256: dict[str, str]


Executor = Callable[[Path, tuple[str, ...], dict[str, str]], int]


def _fail(message: str) -> NoReturn:
    raise ControllerBootstrapError(message)


def _stat_identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_context(context: BootstrapContext) -> None:
    if (
        _SHA_RE.fullmatch(context.source_sha) is None
        or not context.trusted_root.is_absolute()
        or ".." in context.trusted_root.parts
        or not context.remote_url
        or "\x00" in context.remote_url
        or not context.git_path.is_absolute()
        or context.authority_uid < 0
        or context.authority_gid < 0
    ):
        _fail("GB10 controller bootstrap context is invalid")
    if not context.allow_file_remote and (
        context.trusted_root != _TRUSTED_ROOT
        or context.remote_url != _REMOTE_URL
        or context.git_path != _GIT
        or context.authority_uid != 0
        or context.authority_gid != 0
    ):
        _fail("GB10 controller bootstrap production authority is invalid")


def _validate_directory(
    path: Path,
    *,
    uid: int,
    gid: int,
    mode: int | None,
    label: str,
) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise ControllerBootstrapError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or (mode is not None and stat.S_IMODE(metadata.st_mode) != mode)
        or (mode is None and stat.S_IMODE(metadata.st_mode) & 0o022)
    ):
        _fail(f"{label} is unsafe")


def _remove_exact_empty_directory(
    path: Path,
    identity: tuple[int, int],
    *,
    label: str,
) -> None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ControllerBootstrapError(f"{label} cleanup failed") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_dev, metadata.st_ino) != identity
    ):
        _fail(f"{label} cleanup target changed")
    try:
        os.rmdir(path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise ControllerBootstrapError(f"{label} cleanup failed") from exc


def _remove_exact_open_file(path: Path, descriptor: int, *, label: str) -> None:
    try:
        opened = os.fstat(descriptor)
        current = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ControllerBootstrapError(f"{label} cleanup failed") from exc
    if (
        not stat.S_ISREG(opened.st_mode)
        or opened.st_nlink != 1
        or not stat.S_ISREG(current.st_mode)
        or stat.S_ISLNK(current.st_mode)
        or current.st_nlink != 1
        or (opened.st_dev, opened.st_ino) != (current.st_dev, current.st_ino)
    ):
        _fail(f"{label} cleanup target changed")
    try:
        os.unlink(path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise ControllerBootstrapError(f"{label} cleanup failed") from exc


def _remove_exact_directory_tree(
    path: Path,
    identity: tuple[int, int],
    *,
    label: str,
) -> None:
    try:
        metadata = os.lstat(path)
    except FileNotFoundError:
        return
    except OSError as exc:
        raise ControllerBootstrapError(f"{label} cleanup failed") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or (metadata.st_dev, metadata.st_ino) != identity
        or not shutil.rmtree.avoids_symlink_attacks
    ):
        _fail(f"{label} cleanup target changed")
    try:
        shutil.rmtree(path)
        _fsync_directory(path.parent)
    except OSError as exc:
        raise ControllerBootstrapError(f"{label} cleanup failed") from exc


def _ensure_trusted_root(context: BootstrapContext) -> None:
    root = context.trusted_root
    _validate_directory(
        root.parent,
        uid=context.authority_uid,
        gid=context.authority_gid,
        mode=None,
        label="GB10 controller bootstrap root parent",
    )
    created_identity: tuple[int, int] | None = None
    if not os.path.lexists(root):
        try:
            os.mkdir(root, mode=0o700)
        except FileExistsError:
            pass
        except OSError as exc:
            raise ControllerBootstrapError(
                "GB10 controller bootstrap trusted root creation failed"
            ) from exc
        else:
            try:
                created = os.lstat(root)
                if not stat.S_ISDIR(created.st_mode) or stat.S_ISLNK(created.st_mode):
                    _fail("GB10 controller bootstrap trusted root creation is unsafe")
                created_identity = (created.st_dev, created.st_ino)
                os.chown(root, context.authority_uid, context.authority_gid)
                os.chmod(root, 0o700)
                _fsync_directory(root.parent)
            except Exception as exc:
                if created_identity is not None:
                    _remove_exact_empty_directory(
                        root,
                        created_identity,
                        label="GB10 controller bootstrap trusted root",
                    )
                if isinstance(exc, ControllerBootstrapError):
                    raise
                raise ControllerBootstrapError(
                    "GB10 controller bootstrap trusted root creation failed"
                ) from exc
    try:
        _validate_directory(
            root,
            uid=context.authority_uid,
            gid=context.authority_gid,
            mode=0o700,
            label="GB10 controller bootstrap trusted root",
        )
    except Exception:
        if created_identity is not None:
            _remove_exact_empty_directory(
                root,
                created_identity,
                label="GB10 controller bootstrap trusted root",
            )
        raise


def _acquire_install_lock(context: BootstrapContext) -> int:
    path = context.trusted_root / ".install.lock"
    flags = os.O_RDWR | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    created = False
    descriptor: int | None = None
    try:
        try:
            descriptor = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
            created = True
        except FileExistsError:
            descriptor = os.open(path, flags)
        if created:
            os.fchown(descriptor, context.authority_uid, context.authority_gid)
            os.fchmod(descriptor, 0o600)
            os.fsync(descriptor)
            _fsync_directory(context.trusted_root)
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != context.authority_uid
            or metadata.st_gid != context.authority_gid
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_size != 0
        ):
            _fail("GB10 controller bootstrap install lock is unsafe")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ControllerBootstrapError(
                "GB10 controller bootstrap installation is already active"
            ) from exc
        os.set_inheritable(descriptor, True)
        return descriptor
    except BaseException as exc:
        cleanup_error: Exception | None = None
        if created and descriptor is not None and isinstance(exc, Exception):
            try:
                _remove_exact_open_file(
                    path,
                    descriptor,
                    label="GB10 controller bootstrap install lock",
                )
            except Exception as cleanup_exc:
                cleanup_error = cleanup_exc
        if descriptor is not None:
            os.close(descriptor)
        if cleanup_error is not None:
            raise ControllerBootstrapError(
                "GB10 controller bootstrap install lock cleanup failed"
            ) from cleanup_error
        if isinstance(exc, OSError):
            message = (
                "GB10 controller bootstrap install lock creation failed"
                if created
                else "GB10 controller bootstrap install lock acquisition failed"
            )
            raise ControllerBootstrapError(message) from exc
        raise


def _git_arguments(context: BootstrapContext) -> list[str]:
    file_policy = "always" if context.allow_file_remote else "never"
    return [
        str(context.git_path),
        "--no-replace-objects",
        "-c",
        "core.hooksPath=/dev/null",
        "-c",
        "core.fsmonitor=false",
        "-c",
        f"protocol.file.allow={file_policy}",
        "-c",
        "protocol.ext.allow=never",
        "-c",
        "fetch.fsckObjects=true",
    ]


def _run_git(
    context: BootstrapContext,
    arguments: list[str],
    *,
    repository: Path | None = None,
    allowed_returncodes: tuple[int, ...] = (0,),
    timeout: int = 300,
) -> subprocess.CompletedProcess[bytes]:
    if any(not argument or "\x00" in argument for argument in arguments):
        _fail("GB10 controller bootstrap Git request is invalid")
    command = _git_arguments(context)
    if repository is not None:
        command.extend(
            [
                f"--git-dir={repository / '.git'}",
                f"--work-tree={repository}",
            ]
        )
    command.extend(arguments)
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            check=False,
            cwd=context.trusted_root,
            env=_ROOT_ENVIRONMENT,
            input=b"",
            timeout=timeout,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ControllerBootstrapError("GB10 controller bootstrap Git is unavailable") from exc
    if (
        completed.returncode not in allowed_returncodes
        or len(completed.stdout) > 16 * 1024 * 1024
        or len(completed.stderr) > 16 * 1024 * 1024
    ):
        _fail("GB10 controller bootstrap Git request failed")
    return completed


def _git_text(
    context: BootstrapContext,
    repository: Path,
    *arguments: str,
    allowed_returncodes: tuple[int, ...] = (0,),
) -> tuple[int, str]:
    completed = _run_git(
        context,
        list(arguments),
        repository=repository,
        allowed_returncodes=allowed_returncodes,
    )
    try:
        output = completed.stdout.decode("utf-8")
    except UnicodeError as exc:
        raise ControllerBootstrapError("GB10 controller bootstrap Git output is invalid") from exc
    return completed.returncode, output


def _seal_tree(source_root: Path, *, uid: int, gid: int) -> None:
    for directory, directory_names, file_names in os.walk(source_root, followlinks=False):
        directory_path = Path(directory)
        try:
            os.chown(directory_path, uid, gid)
            os.chmod(directory_path, 0o700)
        except OSError as exc:
            raise ControllerBootstrapError("GB10 controller bootstrap tree sealing failed") from exc
        for name in (*directory_names, *file_names):
            path = directory_path / name
            try:
                metadata = os.lstat(path)
                if stat.S_ISLNK(metadata.st_mode):
                    os.lchown(path, uid, gid)
                elif stat.S_ISDIR(metadata.st_mode):
                    os.chown(path, uid, gid)
                    os.chmod(path, 0o700)
                elif stat.S_ISREG(metadata.st_mode) and metadata.st_nlink == 1:
                    executable = bool(stat.S_IMODE(metadata.st_mode) & 0o111)
                    os.chown(path, uid, gid)
                    os.chmod(path, 0o700 if executable else 0o600)
                else:
                    _fail("GB10 controller bootstrap source tree entry is unsafe")
            except OSError as exc:
                raise ControllerBootstrapError(
                    "GB10 controller bootstrap tree sealing failed"
                ) from exc


def _validate_sealed_tree(source_root: Path, *, uid: int, gid: int) -> None:
    for directory, directory_names, file_names in os.walk(source_root, followlinks=False):
        directory_path = Path(directory)
        _validate_directory(
            directory_path,
            uid=uid,
            gid=gid,
            mode=0o700,
            label="GB10 controller bootstrap sealed directory",
        )
        for name in (*directory_names, *file_names):
            path = directory_path / name
            try:
                metadata = os.lstat(path)
            except OSError as exc:
                raise ControllerBootstrapError(
                    "GB10 controller bootstrap sealed tree is unavailable"
                ) from exc
            if metadata.st_uid != uid or metadata.st_gid != gid:
                _fail("GB10 controller bootstrap sealed tree ownership is unsafe")
            if stat.S_ISLNK(metadata.st_mode):
                continue
            if stat.S_ISDIR(metadata.st_mode):
                if stat.S_IMODE(metadata.st_mode) != 0o700:
                    _fail("GB10 controller bootstrap sealed directory is unsafe")
                continue
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) not in {0o600, 0o700}
            ):
                _fail("GB10 controller bootstrap sealed file is unsafe")


def _validate_git_metadata_tree(git_root: Path) -> None:
    for directory, directory_names, file_names in os.walk(git_root, followlinks=False):
        directory_path = Path(directory)
        for name in (*directory_names, *file_names):
            try:
                metadata = os.lstat(directory_path / name)
            except OSError as exc:
                raise ControllerBootstrapError(
                    "GB10 controller bootstrap Git metadata is unavailable"
                ) from exc
            if stat.S_ISLNK(metadata.st_mode):
                _fail("GB10 controller bootstrap Git metadata is unsafe")


def _validate_artifact_ancestors(
    context: BootstrapContext,
    source_root: Path,
) -> None:
    validated: set[Path] = set()
    for relative in _SOURCE_ARTIFACTS:
        current = source_root
        for part in relative.parts[:-1]:
            current /= part
            if current in validated:
                continue
            _validate_directory(
                current,
                uid=context.authority_uid,
                gid=context.authority_gid,
                mode=0o700,
                label="GB10 controller bootstrap source artifact parent",
            )
            validated.add(current)


def _read_stable_artifact(
    path: Path,
    *,
    uid: int,
    gid: int,
    executable: bool,
) -> bytes:
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        expected_mode = 0o700 if executable else 0o600
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != uid
            or before.st_gid != gid
            or stat.S_IMODE(before.st_mode) != expected_mode
            or before.st_size > _MAX_ARTIFACT_BYTES
        ):
            _fail("GB10 controller bootstrap source artifact is unsafe")
        payload = bytearray()
        while chunk := os.read(descriptor, 64 * 1024):
            payload.extend(chunk)
        after = os.fstat(descriptor)
        if _stat_identity(before) != _stat_identity(after):
            _fail("GB10 controller bootstrap source artifact changed during verification")
        return bytes(payload)
    except OSError as exc:
        raise ControllerBootstrapError(
            "GB10 controller bootstrap source artifact is unavailable"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _verify_artifacts(context: BootstrapContext, source_root: Path) -> dict[str, str]:
    artifact_sha256: dict[str, str] = {}
    for relative in _SOURCE_ARTIFACTS:
        completed = _run_git(
            context,
            ["ls-tree", "-z", context.source_sha, "--", str(relative)],
            repository=source_root,
        )
        entries = completed.stdout.rstrip(b"\0").split(b"\0") if completed.stdout else []
        if len(entries) != 1 or b"\t" not in entries[0]:
            _fail("GB10 controller bootstrap source artifact is absent from exact commit")
        identity, encoded_path = entries[0].split(b"\t", 1)
        fields = identity.split()
        if len(fields) != 3 or fields[1] != b"blob":
            _fail("GB10 controller bootstrap source artifact identity is invalid")
        mode, _object_type, object_id = fields
        if mode not in {b"100644", b"100755"}:
            _fail("GB10 controller bootstrap source artifact mode is unsafe")
        try:
            committed_path = encoded_path.decode("utf-8")
            object_name = object_id.decode("ascii")
        except UnicodeError as exc:
            raise ControllerBootstrapError(
                "GB10 controller bootstrap source artifact identity is invalid"
            ) from exc
        if committed_path != str(relative) or re.fullmatch(r"[0-9a-f]{40}", object_name) is None:
            _fail("GB10 controller bootstrap source artifact identity is invalid")
        expected = _run_git(
            context,
            ["cat-file", "blob", object_name],
            repository=source_root,
        ).stdout
        payload = _read_stable_artifact(
            source_root / relative,
            uid=context.authority_uid,
            gid=context.authority_gid,
            executable=mode == b"100755",
        )
        if payload != expected:
            _fail("GB10 controller bootstrap source artifact does not match exact commit")
        artifact_sha256[str(relative)] = hashlib.sha256(payload).hexdigest()
    return artifact_sha256


def _validate_git_identity(context: BootstrapContext, source_root: Path) -> None:
    _validate_directory(
        source_root,
        uid=context.authority_uid,
        gid=context.authority_gid,
        mode=0o700,
        label="GB10 controller bootstrap source root",
    )
    _validate_directory(
        source_root / ".git",
        uid=context.authority_uid,
        gid=context.authority_gid,
        mode=0o700,
        label="GB10 controller bootstrap Git metadata",
    )
    _validate_sealed_tree(
        source_root,
        uid=context.authority_uid,
        gid=context.authority_gid,
    )
    _validate_git_metadata_tree(source_root / ".git")
    _validate_artifact_ancestors(context, source_root)
    for forbidden in (
        source_root / ".git/commondir",
        source_root / ".git/config.worktree",
        source_root / ".git/info/grafts",
        source_root / ".git/objects/info/alternates",
        source_root / ".git/refs/replace",
        source_root / ".git/shallow",
        source_root / ".git/worktrees",
    ):
        if os.path.lexists(forbidden):
            _fail("GB10 controller bootstrap Git indirection is unsupported")
    config_path = source_root / ".git/config"
    config = _read_stable_artifact(
        config_path,
        uid=context.authority_uid,
        gid=context.authority_gid,
        executable=False,
    )
    expected_lines = [
        b"[core]\n",
        b"\trepositoryformatversion = 0\n",
        b"\tfilemode = true\n",
        b"\tbare = false\n",
        b"\tlogallrefupdates = true\n",
        b'[remote "origin"]\n',
        f"\turl = {context.remote_url}\n".encode(),
        b"\tfetch = +refs/heads/*:refs/remotes/origin/*\n",
    ]
    config_lines = config.splitlines(keepends=True)
    if config_lines[: len(expected_lines)] != expected_lines:
        _fail("GB10 controller bootstrap Git configuration is unsafe")
    branch_lines = config_lines[len(expected_lines) :]
    if branch_lines:
        if len(branch_lines) != 3:
            _fail("GB10 controller bootstrap Git configuration is unsafe")
        match = re.fullmatch(rb'\[branch "([-A-Za-z0-9_./]+)"\]\n', branch_lines[0])
        if (
            match is None
            or b".." in match.group(1)
            or branch_lines[1] != b"\tremote = origin\n"
            or branch_lines[2] != b"\tmerge = refs/heads/" + match.group(1) + b"\n"
        ):
            _fail("GB10 controller bootstrap Git configuration is unsafe")
    _returncode, head = _git_text(context, source_root, "rev-parse", "--verify", "HEAD^{commit}")
    if head != f"{context.source_sha}\n":
        _fail("GB10 controller bootstrap source commit is invalid")
    branch_status, branch = _git_text(
        context,
        source_root,
        "symbolic-ref",
        "-q",
        "HEAD",
        allowed_returncodes=(0, 1),
    )
    if branch_status != 1 or branch:
        _fail("GB10 controller bootstrap source is not detached")
    _returncode, remotes = _git_text(context, source_root, "remote")
    if remotes != "origin\n":
        _fail("GB10 controller bootstrap source remote identity is invalid")
    _returncode, origin = _git_text(context, source_root, "config", "--get", "remote.origin.url")
    if origin != f"{context.remote_url}\n":
        _fail("GB10 controller bootstrap source origin is invalid")
    push_status, push_url = _git_text(
        context,
        source_root,
        "config",
        "--get-all",
        "remote.origin.pushurl",
        allowed_returncodes=(0, 1),
    )
    if push_status != 1 or push_url:
        _fail("GB10 controller bootstrap source push authority is unsafe")
    _returncode, shallow = _git_text(
        context,
        source_root,
        "rev-parse",
        "--is-shallow-repository",
    )
    if shallow != "false\n":
        _fail("GB10 controller bootstrap shallow source is unsupported")
    _returncode, replacements = _git_text(context, source_root, "replace", "-l")
    if replacements:
        _fail("GB10 controller bootstrap Git replacement is unsupported")


def _validate_prepared_source(context: BootstrapContext, source_root: Path) -> PreparedSource:
    if source_root != context.trusted_root / context.source_sha:
        _fail("GB10 controller bootstrap exact source path is invalid")
    _validate_git_identity(context, source_root)
    artifact_sha256 = _verify_artifacts(context, source_root)
    _returncode, status = _git_text(
        context,
        source_root,
        "status",
        "--porcelain=v1",
        "--untracked-files=all",
    )
    if status:
        _fail("GB10 controller bootstrap source identity drifted")
    _run_git(
        context,
        ["fsck", "--full", "--strict", "--no-reflogs", "--no-progress"],
        repository=source_root,
    )
    _validate_sealed_tree(
        source_root,
        uid=context.authority_uid,
        gid=context.authority_gid,
    )
    return PreparedSource(
        source_root=source_root,
        launcher_path=source_root / _LAUNCHER_RELATIVE,
        artifact_sha256=artifact_sha256,
    )


def _prepare_source_locked(context: BootstrapContext) -> PreparedSource:
    source_root = context.trusted_root / context.source_sha
    if os.path.lexists(source_root):
        return _validate_prepared_source(context, source_root)

    staging = Path(
        tempfile.mkdtemp(
            prefix=f".{context.source_sha}.candidate.",
            dir=context.trusted_root,
        )
    )
    staging_metadata = os.lstat(staging)
    if not stat.S_ISDIR(staging_metadata.st_mode) or stat.S_ISLNK(staging_metadata.st_mode):
        _fail("GB10 controller bootstrap staging checkout is unsafe")
    staging_identity = (staging_metadata.st_dev, staging_metadata.st_ino)
    published = False
    try:
        _run_git(
            context,
            [
                "clone",
                "--no-checkout",
                "--no-hardlinks",
                "--origin",
                "origin",
                "--",
                context.remote_url,
                str(staging),
            ],
        )
        _run_git(
            context,
            ["checkout", "--detach", "--force", context.source_sha],
            repository=staging,
        )
        _seal_tree(
            staging,
            uid=context.authority_uid,
            gid=context.authority_gid,
        )
        provisional = BootstrapContext(
            source_sha=context.source_sha,
            trusted_root=context.trusted_root,
            remote_url=context.remote_url,
            authority_uid=context.authority_uid,
            authority_gid=context.authority_gid,
            git_path=context.git_path,
            allow_file_remote=context.allow_file_remote,
        )
        # Validate at the final exact path after publication; before publication,
        # independently bind every executable asset to the approved commit.
        _validate_git_identity(provisional, staging)
        _verify_artifacts(provisional, staging)
        if os.path.lexists(source_root):
            _fail("GB10 controller bootstrap exact source appeared concurrently")
        os.rename(staging, source_root)
        published = True
        _fsync_directory(context.trusted_root)
        return _validate_prepared_source(context, source_root)
    except Exception as exc:
        if published:
            _remove_exact_directory_tree(
                source_root,
                staging_identity,
                label="GB10 controller bootstrap source",
            )
        if isinstance(exc, ControllerBootstrapError):
            raise
        if isinstance(exc, OSError):
            raise ControllerBootstrapError(
                "GB10 controller bootstrap source publication failed"
            ) from exc
        raise
    finally:
        if not published and os.path.lexists(staging):
            _remove_exact_directory_tree(
                staging,
                staging_identity,
                label="GB10 controller bootstrap staging source",
            )


def prepare_source(context: BootstrapContext) -> PreparedSource:
    """Create or revalidate one exact root-sealed checkout from the fixed origin."""

    _validate_context(context)
    _ensure_trusted_root(context)
    lock_descriptor = _acquire_install_lock(context)
    try:
        return _prepare_source_locked(context)
    finally:
        os.close(lock_descriptor)


def _read_authority_input(path: Path, *, uid: int, gid: int) -> None:
    if not path.is_absolute() or ".." in path.parts:
        _fail("GB10 controller bootstrap public key path is invalid")
    _validate_directory(
        path.parent,
        uid=uid,
        gid=gid,
        mode=None,
        label="GB10 controller bootstrap public key parent",
    )
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != uid
            or before.st_gid != gid
            or stat.S_IMODE(before.st_mode) & 0o022
            or before.st_size <= 0
            or before.st_size > 16 * 1024
        ):
            _fail("GB10 controller bootstrap public key input is unsafe")
        while os.read(descriptor, 16 * 1024):
            pass
        if _stat_identity(before) != _stat_identity(os.fstat(descriptor)):
            _fail("GB10 controller bootstrap public key input changed during validation")
    except OSError as exc:
        raise ControllerBootstrapError(
            "GB10 controller bootstrap public key input is unavailable"
        ) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _exec_launcher(path: Path, arguments: tuple[str, ...], environment: dict[str, str]) -> int:
    os.execve("/usr/bin/bash", ("/usr/bin/bash", str(path), *arguments[1:]), environment)


def launch_controller(
    context: BootstrapContext,
    *,
    controller_public_key: Path,
    legacy_public_key: Path,
    executor: Executor = _exec_launcher,
) -> int:
    """Verify authority inputs and source independently, then execute the launcher."""

    _validate_context(context)
    _read_authority_input(
        controller_public_key,
        uid=context.authority_uid,
        gid=context.authority_gid,
    )
    _read_authority_input(
        legacy_public_key,
        uid=context.authority_uid,
        gid=context.authority_gid,
    )
    _ensure_trusted_root(context)
    lock_descriptor = _acquire_install_lock(context)
    try:
        prepared = _prepare_source_locked(context)
        arguments = (
            str(prepared.launcher_path),
            "--source-sha",
            context.source_sha,
            "--controller-public-key",
            str(controller_public_key),
            "--legacy-deploy-public-key",
            str(legacy_public_key),
        )
        environment = {
            **_ROOT_ENVIRONMENT,
            "LOOM_GB10_BOOTSTRAP_LOCK_FD": str(lock_descriptor),
            "LOOM_GB10_TRUSTED_BOOTSTRAP": "1",
        }
        return executor(prepared.launcher_path, arguments, environment)
    finally:
        os.close(lock_descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--controller-public-key", type=Path, required=True)
    parser.add_argument("--legacy-deploy-public-key", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        arguments = _parser().parse_args(argv)
        if os.geteuid() != 0 or os.getegid() != 0:
            _fail("GB10 controller bootstrap requires root")
        context = BootstrapContext(source_sha=arguments.source_sha)
        return launch_controller(
            context,
            controller_public_key=arguments.controller_public_key,
            legacy_public_key=arguments.legacy_deploy_public_key,
        )
    except ControllerBootstrapError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
