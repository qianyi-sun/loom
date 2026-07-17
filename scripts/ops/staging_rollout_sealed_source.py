#!/usr/bin/python3
"""Validate one explicit, root-controlled cumulative rollout source.

This module is deliberately independent of the installer and exporter so both
mutating entrypoints consume the same fail-closed source contract.  It never
resolves a branch or fetches a remote ref.
"""

from __future__ import annotations

import os
import re
import stat
import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

SHA_RE = re.compile(r"^[0-9a-f]{40}$")
APPROVED_REMOTE_URL = "https://github.com/qianyi-sun/loom.git"


class SealedSourceError(RuntimeError):
    """A bounded sealed-source validation failure."""


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[Sequence[str]], CommandResult]


@dataclass(frozen=True, slots=True)
class SealedSource:
    path: Path
    commit_sha: str
    tree_sha: str
    base_sha: str

    def __post_init__(self) -> None:
        if not self.path.is_absolute() or ".." in self.path.parts:
            raise SealedSourceError("sealed source path must be absolute and normalized")
        for label, value in (
            ("commit", self.commit_sha),
            ("tree", self.tree_sha),
            ("base", self.base_sha),
        ):
            if SHA_RE.fullmatch(value) is None:
                raise SealedSourceError(f"sealed source {label} SHA is invalid")
        if len({self.commit_sha, self.tree_sha, self.base_sha}) != 3:
            raise SealedSourceError("sealed source identities must be distinct")

    def metadata(self) -> dict[str, str]:
        return {
            "source_mode": "sealed-cumulative",
            "source_sha": self.commit_sha,
            "source_tree_sha": self.tree_sha,
            "source_base_sha": self.base_sha,
        }


def _default_run(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        env={
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_OPTIONAL_LOCKS": "0",
        },
    )


def _git(source: SealedSource, run: CommandRunner, *args: str) -> str:
    result = run(("/usr/bin/git", "--no-replace-objects", "-C", str(source.path), *args))
    if result.returncode != 0:
        raise SealedSourceError("sealed source Git validation failed")
    return result.stdout.strip()


def _validate_authority(source: SealedSource, *, expected_uid: int, expected_gid: int) -> None:
    for parent in source.path.parents:
        try:
            metadata = os.lstat(parent)
        except OSError as exc:
            raise SealedSourceError("sealed source parent authority is unavailable") from exc
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            raise SealedSourceError("sealed source parent authority is unsafe")

    try:
        root = os.lstat(source.path)
        git = os.lstat(source.path / ".git")
    except OSError as exc:
        raise SealedSourceError("sealed source checkout is unavailable") from exc
    if (
        not stat.S_ISDIR(root.st_mode)
        or stat.S_ISLNK(root.st_mode)
        or not stat.S_ISDIR(git.st_mode)
        or stat.S_ISLNK(git.st_mode)
        or root.st_uid != expected_uid
        or root.st_gid != expected_gid
        or git.st_uid != expected_uid
        or git.st_gid != expected_gid
        or stat.S_IMODE(root.st_mode) & 0o022
        or stat.S_IMODE(git.st_mode) & 0o022
    ):
        raise SealedSourceError("sealed source checkout authority is unsafe")

    for current, directories, filenames in os.walk(source.path, followlinks=False):
        for name in (*directories, *filenames):
            path = Path(current) / name
            metadata = os.lstat(path)
            if (
                stat.S_ISLNK(metadata.st_mode)
                or not (stat.S_ISDIR(metadata.st_mode) or stat.S_ISREG(metadata.st_mode))
                or metadata.st_uid != expected_uid
                or metadata.st_gid != expected_gid
                or stat.S_IMODE(metadata.st_mode) & 0o022
                or (stat.S_ISREG(metadata.st_mode) and metadata.st_nlink != 1)
            ):
                raise SealedSourceError("sealed source checkout tree is unsafe")


def validate_sealed_source(
    source: SealedSource,
    *,
    run: CommandRunner = _default_run,
    expected_uid: int = 0,
    expected_gid: int = 0,
) -> None:
    """Prove exact detached identity and a one-commit approved lineage."""
    _validate_authority(source, expected_uid=expected_uid, expected_gid=expected_gid)

    forbidden = (
        source.path / ".git/objects/info/alternates",
        source.path / ".git/info/grafts",
        source.path / ".git/shallow",
    )
    if any(path.exists() or path.is_symlink() for path in forbidden):
        raise SealedSourceError("sealed source uses unsupported Git indirection")
    replace_root = source.path / ".git/refs/replace"
    if replace_root.exists() or replace_root.is_symlink():
        raise SealedSourceError("sealed source uses replacement objects")

    symbolic = run(
        (
            "/usr/bin/git",
            "--no-replace-objects",
            "-C",
            str(source.path),
            "symbolic-ref",
            "-q",
            "HEAD",
        )
    )
    if not (symbolic.returncode == 1 and symbolic.stdout == "" and symbolic.stderr == ""):
        raise SealedSourceError("sealed source must use detached HEAD")
    if _git(source, run, "rev-parse", "--verify", "HEAD^{commit}") != source.commit_sha:
        raise SealedSourceError("sealed source commit identity does not match")
    if _git(source, run, "rev-parse", "--verify", "HEAD^{tree}") != source.tree_sha:
        raise SealedSourceError("sealed source tree identity does not match")
    merge_base = _git(source, run, "merge-base", source.base_sha, source.commit_sha)
    if merge_base != source.base_sha:
        raise SealedSourceError("sealed source approved base is not the exact history boundary")
    if _git(source, run, "status", "--porcelain=v1", "--untracked-files=all"):
        raise SealedSourceError("sealed source checkout must be clean")
    if _git(source, run, "remote") != "origin":
        raise SealedSourceError("sealed source must have only remote origin")
    if _git(source, run, "remote", "get-url", "--all", "origin") != APPROVED_REMOTE_URL:
        raise SealedSourceError("sealed source origin is not approved")
    pushurl = run(
        (
            "/usr/bin/git",
            "--no-replace-objects",
            "-C",
            str(source.path),
            "config",
            "--get-all",
            "remote.origin.pushurl",
        )
    )
    if not (pushurl.returncode == 1 and pushurl.stdout == "" and pushurl.stderr == ""):
        raise SealedSourceError("sealed source push URL must be absent")
    history = _git(
        source,
        run,
        "rev-list",
        "--reverse",
        "--parents",
        f"{source.base_sha}..{source.commit_sha}",
    ).splitlines()
    if not 1 <= len(history) <= 32:
        raise SealedSourceError("sealed source cumulative history is empty or exceeds its bound")
    expected_parent = source.base_sha
    for line in history:
        fields = line.split()
        if len(fields) != 2 or fields[1] != expected_parent:
            raise SealedSourceError("sealed source cumulative history is not a linear chain")
        expected_parent = fields[0]
    if expected_parent != source.commit_sha:
        raise SealedSourceError("sealed source cumulative history does not end at exact HEAD")
    if _git(source, run, "cat-file", "-t", source.tree_sha) != "tree":
        raise SealedSourceError("sealed source tree object is unavailable")


__all__ = [
    "APPROVED_REMOTE_URL",
    "CommandRunner",
    "SealedSource",
    "SealedSourceError",
    "validate_sealed_source",
]
