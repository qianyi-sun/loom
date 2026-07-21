#!/usr/bin/env python3
"""Fail-closed qianyi consumer verification for one immutable shared checkout."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path, PurePosixPath

_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
_OBJECT_ID_RE = re.compile(r"[0-9a-f]{40}\Z")
_CANONICAL_GIT_CONFIG = (
    b"[core]\n"
    b"\trepositoryformatversion = 0\n"
    b"\tfilemode = true\n"
    b"\tbare = false\n"
    b"\tlogallrefupdates = true\n"
)
_DIRECTORY_FLAGS = (
    os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)


class ConsumerVerificationError(RuntimeError):
    """The shared checkout is not exactly safe for the qianyi consumer."""


def _git(repo: Path, *arguments: str) -> bytes:
    configuration = (
        ("safe.directory", str(repo)),
        ("core.worktree", str(repo)),
        ("core.bare", "false"),
        ("core.fsmonitor", "false"),
        ("core.hooksPath", "/dev/null"),
        ("core.attributesFile", "/dev/null"),
        ("core.excludesFile", "/dev/null"),
        ("core.untrackedCache", "false"),
        ("submodule.recurse", "false"),
        ("fetch.recurseSubmodules", "false"),
        ("protocol.file.allow", "never"),
        ("credential.helper", ""),
        ("core.sshCommand", "/usr/bin/false"),
    )
    command = [
        "/usr/bin/git",
        "--git-dir",
        str(repo / ".git"),
        "--work-tree",
        str(repo),
    ]
    for key, value in configuration:
        command.extend(("-c", f"{key}={value}"))
    command.extend(arguments)
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            timeout=15,
            env={
                "GIT_CONFIG_NOSYSTEM": "1",
                "GIT_NO_REPLACE_OBJECTS": "1",
                "GIT_CONFIG_SYSTEM": "/dev/null",
                "GIT_CONFIG_GLOBAL": "/dev/null",
                "GIT_ATTR_NOSYSTEM": "1",
                "GIT_OPTIONAL_LOCKS": "0",
                "GIT_TERMINAL_PROMPT": "0",
                "GIT_PROTOCOL_FROM_USER": "0",
                "GIT_PAGER": "cat",
                "GIT_EXTERNAL_DIFF": "/usr/bin/false",
                "GIT_SSH_COMMAND": "/usr/bin/false",
                "HOME": "/nonexistent",
                "XDG_CONFIG_HOME": "/nonexistent",
                "LANG": "C",
                "LC_ALL": "C",
                "PATH": "/usr/bin:/bin",
            },
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise ConsumerVerificationError("git verification failed safely") from exc
    if result.returncode != 0 or result.stderr:
        raise ConsumerVerificationError("git verification failed safely")
    return result.stdout


def _index(repo: Path) -> tuple[dict[str, tuple[str, str]], bytes]:
    raw = _git(repo, "ls-files", "--stage", "-z")
    entries: dict[str, tuple[str, str]] = {}
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        metadata, separator, encoded_path = encoded.partition(b"\t")
        fields = metadata.split()
        try:
            relative = encoded_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ConsumerVerificationError("repository index is invalid") from exc
        path = PurePosixPath(relative)
        if (
            separator != b"\t"
            or len(fields) != 3
            or fields[2] != b"0"
            or fields[0] not in {b"100644", b"100755", b"120000"}
            or _OBJECT_ID_RE.fullmatch(fields[1].decode("ascii", errors="ignore")) is None
            or not relative
            or path.is_absolute()
            or ".." in path.parts
            or path.parts[0] == ".git"
            or relative in entries
        ):
            raise ConsumerVerificationError("repository index is invalid")
        entries[relative] = (fields[0].decode("ascii"), fields[1].decode("ascii"))
    if not entries:
        raise ConsumerVerificationError("repository index is empty")
    return entries, raw


def _commit_tree(repo: Path, sha: str) -> dict[str, tuple[str, str]]:
    raw = _git(repo, "ls-tree", "-r", "-z", "--full-tree", sha)
    entries: dict[str, tuple[str, str]] = {}
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        metadata, separator, encoded_path = encoded.partition(b"\t")
        fields = metadata.split()
        try:
            relative = encoded_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ConsumerVerificationError("repository commit tree is invalid") from exc
        path = PurePosixPath(relative)
        if (
            separator != b"\t"
            or len(fields) != 3
            or fields[0] not in {b"100644", b"100755", b"120000"}
            or fields[1] != b"blob"
            or _OBJECT_ID_RE.fullmatch(fields[2].decode("ascii", errors="ignore")) is None
            or not relative
            or path.is_absolute()
            or ".." in path.parts
            or path.parts[0] == ".git"
            or relative in entries
        ):
            raise ConsumerVerificationError("repository commit tree is invalid")
        entries[relative] = (fields[0].decode("ascii"), fields[2].decode("ascii"))
    if not entries:
        raise ConsumerVerificationError("repository commit tree is empty")
    return entries


def _validate_git_metadata(directory_fd: int, *, uid: int, gid: int) -> None:
    for name in os.listdir(directory_fd):
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (metadata.st_uid, metadata.st_gid) != (uid, gid):
            raise ConsumerVerificationError("git metadata authority drifted")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise ConsumerVerificationError("git metadata mode drifted")
            child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
            try:
                _validate_git_metadata(child_fd, uid=uid, gid=gid)
            finally:
                os.close(child_fd)
        elif stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o640:
                raise ConsumerVerificationError("git metadata file drifted")
        else:
            raise ConsumerVerificationError("git metadata type drifted")


def _validate_single_git_authority(directory_fd: int, *, uid: int, gid: int) -> None:
    entries = set(os.listdir(directory_fd))
    if entries & {"commondir", "config.worktree"}:
        raise ConsumerVerificationError("git common authority redirection is forbidden")
    for name in ("objects", "refs"):
        child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
        try:
            held = os.fstat(child_fd)
            lexical = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                (held.st_uid, held.st_gid) != (uid, gid)
                or stat.S_IMODE(held.st_mode) != 0o750
                or not stat.S_ISDIR(lexical.st_mode)
                or (held.st_dev, held.st_ino) != (lexical.st_dev, lexical.st_ino)
            ):
                raise ConsumerVerificationError("git object or ref authority drifted")
        finally:
            os.close(child_fd)
    try:
        git_info_fd = os.open("info", _DIRECTORY_FLAGS, dir_fd=directory_fd)
    except FileNotFoundError:
        pass
    else:
        try:
            if "grafts" in set(os.listdir(git_info_fd)):
                raise ConsumerVerificationError("git legacy graft authority is forbidden")
        finally:
            os.close(git_info_fd)
    objects_fd = os.open("objects", _DIRECTORY_FLAGS, dir_fd=directory_fd)
    try:
        try:
            info_fd = os.open("info", _DIRECTORY_FLAGS, dir_fd=objects_fd)
        except FileNotFoundError:
            return
        else:
            try:
                if set(os.listdir(info_fd)) & {"alternates", "http-alternates"}:
                    raise ConsumerVerificationError("git object authority redirection is forbidden")
            finally:
                os.close(info_fd)
    finally:
        os.close(objects_fd)


def _validate_canonical_git_config(directory_fd: int, *, uid: int, gid: int) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    config_fd = os.open("config", flags, dir_fd=directory_fd)
    try:
        before = os.fstat(config_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or (before.st_uid, before.st_gid) != (uid, gid)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o640
            or before.st_size != len(_CANONICAL_GIT_CONFIG)
        ):
            raise ConsumerVerificationError("git configuration authority drifted")
        payload = os.read(config_fd, len(_CANONICAL_GIT_CONFIG) + 1)
        after = os.fstat(config_fd)
        if payload != _CANONICAL_GIT_CONFIG or (
            after.st_dev,
            after.st_ino,
            after.st_mtime_ns,
            after.st_size,
        ) != (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size):
            raise ConsumerVerificationError("git configuration drifted")
    finally:
        os.close(config_fd)


def _validate_physical_metadata(
    directory_fd: int,
    *,
    uid: int,
    gid: int,
    top_level: bool = True,
) -> None:
    for name in os.listdir(directory_fd):
        if top_level and name == ".git":
            continue
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (metadata.st_uid, metadata.st_gid) != (uid, gid):
            raise ConsumerVerificationError("repository content authority drifted")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise ConsumerVerificationError("repository directory mode drifted")
            child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
            try:
                _validate_physical_metadata(
                    child_fd,
                    uid=uid,
                    gid=gid,
                    top_level=False,
                )
            finally:
                os.close(child_fd)
        elif stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) not in {0o640, 0o750}:
                raise ConsumerVerificationError("repository file drifted")
        elif stat.S_ISLNK(metadata.st_mode):
            if metadata.st_nlink != 1:
                raise ConsumerVerificationError("repository symlink drifted")
        else:
            raise ConsumerVerificationError("repository content type drifted")


def _blob_digests(data: bytes) -> tuple[str, str]:
    prefix = f"blob {len(data)}\0".encode("ascii")
    return hashlib.sha1(prefix + data, usedforsecurity=False).hexdigest(), hashlib.sha256(
        data
    ).hexdigest()


def _regular_blob_digests(
    directory_fd: int,
    name: str,
    *,
    metadata: os.stat_result,
) -> tuple[str, str]:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    file_fd = os.open(name, flags, dir_fd=directory_fd)
    try:
        before = os.fstat(file_fd)
        if (before.st_dev, before.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise ConsumerVerificationError("repository file binding drifted")
        git_hash = hashlib.sha1(usedforsecurity=False)
        git_hash.update(f"blob {before.st_size}\0".encode("ascii"))
        content_hash = hashlib.sha256()
        size = 0
        while chunk := os.read(file_fd, 1024 * 1024):
            git_hash.update(chunk)
            content_hash.update(chunk)
            size += len(chunk)
        after = os.fstat(file_fd)
        if size != before.st_size or (
            after.st_dev,
            after.st_ino,
            after.st_mtime_ns,
            after.st_size,
        ) != (before.st_dev, before.st_ino, before.st_mtime_ns, before.st_size):
            raise ConsumerVerificationError("repository file changed while read")
        return git_hash.hexdigest(), content_hash.hexdigest()
    finally:
        os.close(file_fd)


def _validate_worktree(
    directory_fd: int,
    *,
    index: dict[str, tuple[str, str]],
    uid: int,
    gid: int,
    prefix: str = "",
) -> tuple[set[str], set[str], dict[str, str]]:
    files: set[str] = set()
    directories: set[str] = set()
    content_sha256: dict[str, str] = {}
    for name in os.listdir(directory_fd):
        if not prefix and name == ".git":
            continue
        relative = f"{prefix}/{name}" if prefix else name
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (metadata.st_uid, metadata.st_gid) != (uid, gid):
            raise ConsumerVerificationError("repository content authority drifted")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise ConsumerVerificationError("repository directory mode drifted")
            directories.add(relative)
            child_fd = os.open(name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
            try:
                child_files, child_directories, child_content = _validate_worktree(
                    child_fd,
                    index=index,
                    uid=uid,
                    gid=gid,
                    prefix=relative,
                )
            finally:
                os.close(child_fd)
            files.update(child_files)
            directories.update(child_directories)
            content_sha256.update(child_content)
            continue
        expected = index.get(relative)
        if stat.S_ISLNK(metadata.st_mode):
            target = os.fsencode(os.readlink(name, dir_fd=directory_fd))
            object_id, content_digest = _blob_digests(target)
            if expected is None or expected[0] != "120000" or expected[1] != object_id:
                raise ConsumerVerificationError("repository symlink drifted")
            content_sha256[relative] = content_digest
        elif stat.S_ISREG(metadata.st_mode):
            expected_mode = 0o750 if expected is not None and expected[0] == "100755" else 0o640
            if (
                expected is None
                or expected[0] not in {"100644", "100755"}
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != expected_mode
            ):
                raise ConsumerVerificationError("repository file drifted")
            object_id, content_digest = _regular_blob_digests(
                directory_fd,
                name,
                metadata=metadata,
            )
            if object_id != expected[1]:
                raise ConsumerVerificationError("repository file content drifted")
            content_sha256[relative] = content_digest
        else:
            raise ConsumerVerificationError("repository content type drifted")
        files.add(relative)
    return files, directories, content_sha256


def _open_absolute_directory_with_parent(path: Path) -> tuple[int, int]:
    if not path.is_absolute() or ".." in path.parts or path == Path("/"):
        raise ConsumerVerificationError("directory binding is invalid")
    directory_fd = os.open("/", _DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:-1]:
            next_fd = os.open(component, _DIRECTORY_FLAGS, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        child_fd = os.open(path.name, _DIRECTORY_FLAGS, dir_fd=directory_fd)
        return directory_fd, child_fd
    except Exception:
        os.close(directory_fd)
        raise


def verify(
    *,
    root: Path,
    repo: Path,
    sha: str,
    owner_uid: int,
    shared_gid: int,
    consumer_uid: int,
) -> dict[str, object]:
    if (
        not root.is_absolute()
        or not repo.is_absolute()
        or repo.parent != root
        or ".." in root.parts
        or ".." in repo.parts
        or _SHA_RE.fullmatch(sha) is None
        or min(owner_uid, shared_gid, consumer_uid) <= 0
        or os.geteuid() != consumer_uid
        or shared_gid not in {*os.getgroups(), os.getegid()}
    ):
        raise ConsumerVerificationError("consumer binding is invalid")

    try:
        root_parent_fd, root_fd = _open_absolute_directory_with_parent(root)
    except OSError as exc:
        raise ConsumerVerificationError("consumer verification failed safely") from exc
    try:
        root_metadata = os.fstat(root_fd)
        root_lexical = os.stat(root.name, dir_fd=root_parent_fd, follow_symlinks=False)
        if (
            (root_metadata.st_dev, root_metadata.st_ino)
            != (root_lexical.st_dev, root_lexical.st_ino)
            or (root_metadata.st_uid, root_metadata.st_gid) != (owner_uid, shared_gid)
            or stat.S_IMODE(root_metadata.st_mode) != 0o2750
            or not os.access(".", os.R_OK | os.X_OK, dir_fd=root_fd, effective_ids=True)
            or os.access(".", os.W_OK, dir_fd=root_fd, effective_ids=True)
        ):
            raise ConsumerVerificationError("repository root contract drifted")
        repo_fd = os.open(repo.name, _DIRECTORY_FLAGS, dir_fd=root_fd)
        try:
            repo_metadata = os.fstat(repo_fd)
            lexical = os.stat(repo.name, dir_fd=root_fd, follow_symlinks=False)
            if (
                (repo_metadata.st_dev, repo_metadata.st_ino) != (lexical.st_dev, lexical.st_ino)
                or (repo_metadata.st_uid, repo_metadata.st_gid) != (owner_uid, shared_gid)
                or stat.S_IMODE(repo_metadata.st_mode) != 0o750
                or not os.access(".", os.R_OK | os.X_OK, dir_fd=repo_fd, effective_ids=True)
                or os.access(".", os.W_OK, dir_fd=repo_fd, effective_ids=True)
            ):
                raise ConsumerVerificationError("repository target contract drifted")

            git_fd = os.open(".git", _DIRECTORY_FLAGS, dir_fd=repo_fd)
            try:
                git_metadata = os.fstat(git_fd)
                git_lexical = os.stat(".git", dir_fd=repo_fd, follow_symlinks=False)
                if (
                    (git_metadata.st_uid, git_metadata.st_gid)
                    != (
                        owner_uid,
                        shared_gid,
                    )
                    or stat.S_IMODE(git_metadata.st_mode) != 0o750
                    or (
                        git_metadata.st_dev,
                        git_metadata.st_ino,
                    )
                    != (git_lexical.st_dev, git_lexical.st_ino)
                ):
                    raise ConsumerVerificationError("git authority drifted")
                _validate_single_git_authority(git_fd, uid=owner_uid, gid=shared_gid)
                _validate_canonical_git_config(git_fd, uid=owner_uid, gid=shared_gid)
                _validate_git_metadata(git_fd, uid=owner_uid, gid=shared_gid)
                _validate_physical_metadata(repo_fd, uid=owner_uid, gid=shared_gid)

                object_format = _git(repo, "rev-parse", "--show-object-format").decode().strip()
                if object_format != "sha1":
                    raise ConsumerVerificationError("repository object format is unsupported")
                head = _git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode("ascii").strip()
                if head != sha:
                    raise ConsumerVerificationError("repository head drifted")
                index, raw_index = _index(repo)
                if index != _commit_tree(repo, sha):
                    raise ConsumerVerificationError("repository index drifted from commit tree")
                expected_directories = {
                    str(parent)
                    for relative in index
                    for parent in PurePosixPath(relative).parents
                    if str(parent) != "."
                }
                files, directories, content_sha256 = _validate_worktree(
                    repo_fd,
                    index=index,
                    uid=owner_uid,
                    gid=shared_gid,
                )
                if files != set(index) or directories != expected_directories:
                    raise ConsumerVerificationError("repository physical index drifted")
                probe_file = min(
                    relative
                    for relative, (mode, _object_id) in index.items()
                    if mode in {"100644", "100755"}
                )
                probe_file_sha256 = content_sha256[probe_file]
                tree_content_hash = hashlib.sha256()
                for relative in sorted(index):
                    mode, object_id = index[relative]
                    for value in (relative, mode, object_id, content_sha256[relative]):
                        tree_content_hash.update(value.encode("utf-8"))
                        tree_content_hash.update(b"\0")
                tree_content_sha256 = tree_content_hash.hexdigest()

                _validate_canonical_git_config(git_fd, uid=owner_uid, gid=shared_gid)
                _validate_single_git_authority(git_fd, uid=owner_uid, gid=shared_gid)
                _validate_git_metadata(git_fd, uid=owner_uid, gid=shared_gid)
                git_after = os.fstat(git_fd)
                git_lexical_after = os.stat(".git", dir_fd=repo_fd, follow_symlinks=False)
                if (
                    git_after.st_dev,
                    git_after.st_ino,
                    git_after.st_uid,
                    git_after.st_gid,
                    stat.S_IMODE(git_after.st_mode),
                ) != (
                    git_metadata.st_dev,
                    git_metadata.st_ino,
                    git_metadata.st_uid,
                    git_metadata.st_gid,
                    0o750,
                ) or (git_lexical_after.st_dev, git_lexical_after.st_ino) != (
                    git_metadata.st_dev,
                    git_metadata.st_ino,
                ):
                    raise ConsumerVerificationError("git binding changed during verification")
            finally:
                os.close(git_fd)

            root_after = os.fstat(root_fd)
            root_lexical_after = os.stat(
                root.name,
                dir_fd=root_parent_fd,
                follow_symlinks=False,
            )
            repo_after = os.fstat(repo_fd)
            repo_lexical_after = os.stat(
                repo.name,
                dir_fd=root_fd,
                follow_symlinks=False,
            )
            if (
                (root_after.st_dev, root_after.st_ino, root_after.st_uid, root_after.st_gid)
                != (
                    root_metadata.st_dev,
                    root_metadata.st_ino,
                    root_metadata.st_uid,
                    root_metadata.st_gid,
                )
                or (root_lexical_after.st_dev, root_lexical_after.st_ino)
                != (root_metadata.st_dev, root_metadata.st_ino)
                or stat.S_IMODE(root_after.st_mode) != 0o2750
                or not os.access(".", os.R_OK | os.X_OK, dir_fd=root_fd, effective_ids=True)
                or os.access(".", os.W_OK, dir_fd=root_fd, effective_ids=True)
                or (repo_after.st_dev, repo_after.st_ino, repo_after.st_uid, repo_after.st_gid)
                != (
                    repo_metadata.st_dev,
                    repo_metadata.st_ino,
                    repo_metadata.st_uid,
                    repo_metadata.st_gid,
                )
                or (repo_lexical_after.st_dev, repo_lexical_after.st_ino)
                != (repo_metadata.st_dev, repo_metadata.st_ino)
                or stat.S_IMODE(repo_after.st_mode) != 0o750
                or not os.access(".", os.R_OK | os.X_OK, dir_fd=repo_fd, effective_ids=True)
                or os.access(".", os.W_OK, dir_fd=repo_fd, effective_ids=True)
            ):
                raise ConsumerVerificationError("repository binding changed during verification")
        finally:
            os.close(repo_fd)
    except (OSError, UnicodeError, ValueError) as exc:
        raise ConsumerVerificationError("consumer verification failed safely") from exc
    finally:
        os.close(root_fd)
        os.close(root_parent_fd)

    return {
        "head": head,
        "index_sha256": hashlib.sha256(raw_index).hexdigest(),
        "probe_file_sha256": probe_file_sha256,
        "root_device": root_metadata.st_dev,
        "root_inode": root_metadata.st_ino,
        "target_device": repo_metadata.st_dev,
        "target_inode": repo_metadata.st_ino,
        "tree_content_sha256": tree_content_sha256,
        "tracked_entries": len(index),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--repo", type=Path, required=True)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--owner-uid", type=int, required=True)
    parser.add_argument("--shared-gid", type=int, required=True)
    parser.add_argument("--consumer-uid", type=int, required=True)
    args = parser.parse_args(argv)
    try:
        evidence = verify(
            root=args.root,
            repo=args.repo,
            sha=args.sha,
            owner_uid=args.owner_uid,
            shared_gid=args.shared_gid,
            consumer_uid=args.consumer_uid,
        )
    except ConsumerVerificationError:
        print("shared repository consumer verification failed", file=sys.stderr)
        return 1
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
