#!/usr/bin/env python3
"""Root-installed, candidate-bound acceptance authority for GB10 Slurm."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import re
import signal
import stat
import subprocess
import sys
import tempfile
import time
import tomllib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

SERVICE_USER = "loom-rollout"
SERVICE_UID = 995
SERVICE_GID = 2007
SLURM_ACCOUNT = "loom-staging"
SLURM_QOS = "loom-staging"
SLURM_PARTITION = "loom-staging"
CLUSTER_NAME = "trt-gb10"
CONTROLLER_HOST = "gx10-01c7"
# Loom's canonical GB10 inventory remains nodes 1-15 even while a node is busy
# in another partition.  Node 16 is outside Loom's allocation boundary.
SLURM_NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
LEGACY_AGENT_NODES = tuple(f"trt-gb10-{index}" for index in range(1, 16))
PRIVATE_WORKER_SERVICE_ENV = {
    pool_name: {
        "LOOM_WORKER_CONTROL_PLANE_URL": "http://192.168.50.103:18081",
        "LOOM_WORKER_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_SUBPROCESS_GATEWAY_URL": "http://192.168.50.103:19100",
        "LOOM_WORKER_MINIO_ENDPOINT": "http://192.168.50.103:19000",
        "LOOM_WORKER_TRAJECTORIES_BUCKET": "loom-staging-trajectories",
        "LOOM_WORKER_ARTIFACTS_BUCKET": "loom-staging-artifacts",
        "LOOM_WORKER_TRIAL_CACHE_REGISTRY_REPO": ("192.168.50.103:5443/loom-trial-cache"),
    }
    for pool_name in ("gb10", "oldlab")
}
TRIAL_CACHE_REGISTRY_REPO = "192.168.50.103:5443/loom-trial-cache"
TRIAL_CACHE_CA_SHA256 = "539c97669d322f4fe91b91b4b8187a62a6618f5a9ec3f409e1ca5f9d7c56ecc3"
TRIAL_CACHE_CANARY_TAG = "transport-canary"
TRIAL_CACHE_CANARY_DIGEST = (
    "sha256:c64c687cbea9300178b30c95835354e34c4e4febc4badfe27102879de0483b5e"
)
STAGING_PROFILE_RELATIVE_PATH = Path("deploy/environment-state/staging.toml")
TRIAL_CACHE_CA_RELATIVE_PATH = Path("deploy/worker-pools/trial-cache/staging-ca.crt")
TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH = Path(
    "scripts/ops/staging_trial_cache_registry_node_probe.py"
)
CANDIDATE_ROOT = Path("/opt/loom-staging-runner/candidates")
INSTALLED_PATH = Path("/usr/local/libexec/loom-gb10-slurm-acceptance-authority")
STATE_ROOT = Path("/var/lib/loom-gb10-slurm-authority")
ARTIFACT_PATH = STATE_ROOT / "current.json"
RUNUSER = "/usr/sbin/runuser"
SRUN = "/usr/bin/srun"
SACCT = "/usr/bin/sacct"
ALLOCATION_START_MARKER = "LOOM_GB10_ALLOCATION_STARTED_V1"
SHA_RE = re.compile(r"[0-9a-f]{40}")
JOB_ID_RE = re.compile(r"[1-9][0-9]*")
# The broker's independent hard timeout is 1200 seconds.  Reserve two minutes
# outside this authority, and reserve the final minute inside it exclusively
# for cancelling and verifying the current fixed-name Slurm probe job.
AUTHORITY_BUDGET_SECONDS = 1080.0
CLEANUP_RESERVE_SECONDS = 60.0


_WORKER_INPUT_VERIFIER = r"""
import hashlib
import os
import re
import stat
import subprocess
from pathlib import Path, PurePosixPath


class WorkerInputVerificationError(RuntimeError):
    pass


WORKER_SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
WORKER_OBJECT_ID_RE = re.compile(r"[0-9a-f]{40}\Z")
WORKER_ENV_KEY_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
WORKER_CANONICAL_GIT_CONFIG = (
    b"[core]\n"
    b"\trepositoryformatversion = 0\n"
    b"\tfilemode = true\n"
    b"\tbare = false\n"
    b"\tlogallrefupdates = true\n"
)
WORKER_DIRECTORY_FLAGS = (
    os.O_RDONLY
    | os.O_DIRECTORY
    | getattr(os, "O_CLOEXEC", 0)
    | getattr(os, "O_NOFOLLOW", 0)
)
WORKER_FILE_FLAGS = (
    os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
)
WORKER_MAX_ENV_BYTES = 1 << 20
WORKER_MAX_GIT_OUTPUT_BYTES = 64 << 20
WORKER_MAX_REPO_ENTRIES = 200_000
WORKER_MAX_REPO_BYTES = 4 << 30


def worker_consume_budget(budget, *, entries=0, size=0):
    budget[0] += entries
    budget[1] += size
    if budget[0] > WORKER_MAX_REPO_ENTRIES or budget[1] > WORKER_MAX_REPO_BYTES:
        raise WorkerInputVerificationError("worker repository exceeds verification bounds")


def worker_git(repo, *arguments):
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
            timeout=30,
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
        raise WorkerInputVerificationError("worker git verification failed safely") from exc
    if (
        result.returncode != 0
        or result.stderr
        or len(result.stdout) > WORKER_MAX_GIT_OUTPUT_BYTES
    ):
        raise WorkerInputVerificationError("worker git verification failed safely")
    return result.stdout


def worker_index(repo):
    raw = worker_git(repo, "ls-files", "--stage", "-z")
    entries = {}
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        metadata, separator, encoded_path = encoded.partition(b"\t")
        fields = metadata.split()
        try:
            relative = encoded_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WorkerInputVerificationError("worker repository index is invalid") from exc
        path = PurePosixPath(relative)
        if (
            separator != b"\t"
            or len(fields) != 3
            or fields[2] != b"0"
            or fields[0] not in {b"100644", b"100755", b"120000"}
            or WORKER_OBJECT_ID_RE.fullmatch(fields[1].decode("ascii", errors="ignore")) is None
            or not relative
            or path.is_absolute()
            or ".." in path.parts
            or path.parts[0] == ".git"
            or relative in entries
        ):
            raise WorkerInputVerificationError("worker repository index is invalid")
        entries[relative] = (fields[0].decode("ascii"), fields[1].decode("ascii"))
    if not entries or len(entries) > WORKER_MAX_REPO_ENTRIES:
        raise WorkerInputVerificationError("worker repository index is invalid")
    return entries, raw


def worker_commit_tree(repo, sha):
    raw = worker_git(repo, "ls-tree", "-r", "-z", "--full-tree", sha)
    entries = {}
    for encoded in raw.split(b"\0"):
        if not encoded:
            continue
        metadata, separator, encoded_path = encoded.partition(b"\t")
        fields = metadata.split()
        try:
            relative = encoded_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise WorkerInputVerificationError("worker commit tree is invalid") from exc
        path = PurePosixPath(relative)
        if (
            separator != b"\t"
            or len(fields) != 3
            or fields[0] not in {b"100644", b"100755", b"120000"}
            or fields[1] != b"blob"
            or WORKER_OBJECT_ID_RE.fullmatch(fields[2].decode("ascii", errors="ignore")) is None
            or not relative
            or path.is_absolute()
            or ".." in path.parts
            or path.parts[0] == ".git"
            or relative in entries
        ):
            raise WorkerInputVerificationError("worker commit tree is invalid")
        entries[relative] = (fields[0].decode("ascii"), fields[2].decode("ascii"))
    if not entries or len(entries) > WORKER_MAX_REPO_ENTRIES:
        raise WorkerInputVerificationError("worker commit tree is invalid")
    return entries


def worker_directory_identity(metadata):
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_uid,
        metadata.st_gid,
        stat.S_IMODE(metadata.st_mode),
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def worker_assert_bound_directory(directory_fd, parent_fd, name, before, *, mode, uid, gid):
    after = os.fstat(directory_fd)
    lexical = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    if (
        not stat.S_ISDIR(after.st_mode)
        or worker_directory_identity(after) != worker_directory_identity(before)
        or (lexical.st_dev, lexical.st_ino) != (before.st_dev, before.st_ino)
        or (after.st_uid, after.st_gid) != (uid, gid)
        or stat.S_IMODE(after.st_mode) != mode
    ):
        raise WorkerInputVerificationError("worker directory binding changed")


def worker_git_metadata_identity(directory_fd, *, uid, gid, prefix="", budget=None):
    if budget is None:
        budget = [0, 0]
    digest = hashlib.sha256()
    before = os.fstat(directory_fd)
    for name in sorted(os.listdir(directory_fd)):
        relative = f"{prefix}/{name}" if prefix else name
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        worker_consume_budget(
            budget,
            entries=1,
            size=metadata.st_size if stat.S_ISREG(metadata.st_mode) else 0,
        )
        if (metadata.st_uid, metadata.st_gid) != (uid, gid):
            raise WorkerInputVerificationError("worker git metadata authority drifted")
        if stat.S_ISDIR(metadata.st_mode):
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise WorkerInputVerificationError("worker git metadata mode drifted")
            child_fd = os.open(name, WORKER_DIRECTORY_FLAGS, dir_fd=directory_fd)
            try:
                child_before = os.fstat(child_fd)
                child_digest = worker_git_metadata_identity(
                    child_fd,
                    uid=uid,
                    gid=gid,
                    prefix=relative,
                    budget=budget,
                )
                worker_assert_bound_directory(
                    child_fd,
                    directory_fd,
                    name,
                    child_before,
                    mode=0o750,
                    uid=uid,
                    gid=gid,
                )
            finally:
                os.close(child_fd)
            kind = "directory"
            content_identity = child_digest
        elif stat.S_ISREG(metadata.st_mode):
            if metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o640:
                raise WorkerInputVerificationError("worker git metadata file drifted")
            kind = "regular"
            content_identity = ""
        else:
            raise WorkerInputVerificationError("worker git metadata type drifted")
        for value in (
            relative,
            kind,
            str(metadata.st_dev),
            str(metadata.st_ino),
            str(metadata.st_size),
            str(metadata.st_mtime_ns),
            str(metadata.st_ctime_ns),
            content_identity,
        ):
            digest.update(value.encode("utf-8"))
            digest.update(b"\0")
    after = os.fstat(directory_fd)
    if worker_directory_identity(after) != worker_directory_identity(before):
        raise WorkerInputVerificationError("worker git metadata changed during verification")
    return digest.hexdigest()


def worker_validate_single_git_authority(directory_fd, *, uid, gid):
    entries = set(os.listdir(directory_fd))
    if entries & {"commondir", "config.worktree"}:
        raise WorkerInputVerificationError("worker git authority redirection is forbidden")
    for name in ("objects", "refs"):
        child_fd = os.open(name, WORKER_DIRECTORY_FLAGS, dir_fd=directory_fd)
        try:
            held = os.fstat(child_fd)
            lexical = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                (held.st_uid, held.st_gid) != (uid, gid)
                or stat.S_IMODE(held.st_mode) != 0o750
                or not stat.S_ISDIR(lexical.st_mode)
                or (held.st_dev, held.st_ino) != (lexical.st_dev, lexical.st_ino)
            ):
                raise WorkerInputVerificationError("worker git authority drifted")
        finally:
            os.close(child_fd)
    try:
        git_info_fd = os.open("info", WORKER_DIRECTORY_FLAGS, dir_fd=directory_fd)
    except FileNotFoundError:
        pass
    else:
        try:
            if "grafts" in set(os.listdir(git_info_fd)):
                raise WorkerInputVerificationError("worker git graft authority is forbidden")
        finally:
            os.close(git_info_fd)
    objects_fd = os.open("objects", WORKER_DIRECTORY_FLAGS, dir_fd=directory_fd)
    try:
        try:
            info_fd = os.open("info", WORKER_DIRECTORY_FLAGS, dir_fd=objects_fd)
        except FileNotFoundError:
            return
        try:
            if set(os.listdir(info_fd)) & {"alternates", "http-alternates"}:
                raise WorkerInputVerificationError("worker git object redirection is forbidden")
        finally:
            os.close(info_fd)
    finally:
        os.close(objects_fd)


def worker_validate_canonical_git_config(directory_fd, *, uid, gid):
    config_fd = os.open("config", WORKER_FILE_FLAGS, dir_fd=directory_fd)
    try:
        before = os.fstat(config_fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or (before.st_uid, before.st_gid) != (uid, gid)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o640
            or before.st_size != len(WORKER_CANONICAL_GIT_CONFIG)
        ):
            raise WorkerInputVerificationError("worker git configuration authority drifted")
        payload = os.read(config_fd, len(WORKER_CANONICAL_GIT_CONFIG) + 1)
        after = os.fstat(config_fd)
        if payload != WORKER_CANONICAL_GIT_CONFIG or (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ):
            raise WorkerInputVerificationError("worker git configuration drifted")
    finally:
        os.close(config_fd)


def worker_blob_digests(data):
    prefix = f"blob {len(data)}\0".encode("ascii")
    return (
        hashlib.sha1(prefix + data, usedforsecurity=False).hexdigest(),
        hashlib.sha256(data).hexdigest(),
    )


def worker_regular_blob_digests(directory_fd, name, *, metadata, budget):
    worker_consume_budget(budget, entries=1, size=metadata.st_size)
    file_fd = os.open(name, WORKER_FILE_FLAGS, dir_fd=directory_fd)
    try:
        before = os.fstat(file_fd)
        if (
            (before.st_dev, before.st_ino) != (metadata.st_dev, metadata.st_ino)
            or before.st_nlink != 1
        ):
            raise WorkerInputVerificationError("worker repository file binding drifted")
        git_hash = hashlib.sha1(usedforsecurity=False)
        git_hash.update(f"blob {before.st_size}\0".encode("ascii"))
        content_hash = hashlib.sha256()
        size = 0
        while chunk := os.read(file_fd, min(1 << 20, WORKER_MAX_REPO_BYTES + 1 - size)):
            git_hash.update(chunk)
            content_hash.update(chunk)
            size += len(chunk)
            if size > WORKER_MAX_REPO_BYTES:
                raise WorkerInputVerificationError("worker repository exceeds verification bounds")
        after = os.fstat(file_fd)
        if size != before.st_size or (
            after.st_dev,
            after.st_ino,
            after.st_uid,
            after.st_gid,
            after.st_nlink,
            stat.S_IMODE(after.st_mode),
            after.st_size,
            after.st_mtime_ns,
            after.st_ctime_ns,
        ) != (
            before.st_dev,
            before.st_ino,
            before.st_uid,
            before.st_gid,
            before.st_nlink,
            stat.S_IMODE(before.st_mode),
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ):
            raise WorkerInputVerificationError("worker repository file changed while read")
        return git_hash.hexdigest(), content_hash.hexdigest()
    finally:
        os.close(file_fd)


def worker_validate_worktree(
    directory_fd,
    *,
    index,
    uid,
    gid,
    budget,
    prefix="",
):
    files = set()
    directories = set()
    content_sha256 = {}
    before = os.fstat(directory_fd)
    for name in sorted(os.listdir(directory_fd)):
        if not prefix and name == ".git":
            continue
        relative = f"{prefix}/{name}" if prefix else name
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if (metadata.st_uid, metadata.st_gid) != (uid, gid):
            raise WorkerInputVerificationError("worker repository content authority drifted")
        if stat.S_ISDIR(metadata.st_mode):
            worker_consume_budget(budget, entries=1)
            if stat.S_IMODE(metadata.st_mode) != 0o750:
                raise WorkerInputVerificationError("worker repository directory mode drifted")
            directories.add(relative)
            child_fd = os.open(name, WORKER_DIRECTORY_FLAGS, dir_fd=directory_fd)
            try:
                child_before = os.fstat(child_fd)
                child_files, child_directories, child_content = worker_validate_worktree(
                    child_fd,
                    index=index,
                    uid=uid,
                    gid=gid,
                    budget=budget,
                    prefix=relative,
                )
                worker_assert_bound_directory(
                    child_fd,
                    directory_fd,
                    name,
                    child_before,
                    mode=0o750,
                    uid=uid,
                    gid=gid,
                )
            finally:
                os.close(child_fd)
            files.update(child_files)
            directories.update(child_directories)
            content_sha256.update(child_content)
            continue
        expected = index.get(relative)
        if stat.S_ISLNK(metadata.st_mode):
            worker_consume_budget(budget, entries=1, size=metadata.st_size)
            target = os.fsencode(os.readlink(name, dir_fd=directory_fd))
            current = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            object_id, content_digest = worker_blob_digests(target)
            if (
                expected is None
                or expected[0] != "120000"
                or expected[1] != object_id
                or metadata.st_nlink != 1
                or (
                    current.st_dev,
                    current.st_ino,
                    current.st_uid,
                    current.st_gid,
                    current.st_nlink,
                    current.st_size,
                    current.st_mtime_ns,
                    current.st_ctime_ns,
                )
                != (
                    metadata.st_dev,
                    metadata.st_ino,
                    metadata.st_uid,
                    metadata.st_gid,
                    metadata.st_nlink,
                    metadata.st_size,
                    metadata.st_mtime_ns,
                    metadata.st_ctime_ns,
                )
            ):
                raise WorkerInputVerificationError("worker repository symlink drifted")
            content_sha256[relative] = content_digest
        elif stat.S_ISREG(metadata.st_mode):
            expected_mode = 0o750 if expected is not None and expected[0] == "100755" else 0o640
            if (
                expected is None
                or expected[0] not in {"100644", "100755"}
                or metadata.st_nlink != 1
                or stat.S_IMODE(metadata.st_mode) != expected_mode
            ):
                raise WorkerInputVerificationError("worker repository file drifted")
            object_id, content_digest = worker_regular_blob_digests(
                directory_fd,
                name,
                metadata=metadata,
                budget=budget,
            )
            if object_id != expected[1]:
                raise WorkerInputVerificationError("worker repository content drifted")
            content_sha256[relative] = content_digest
        else:
            raise WorkerInputVerificationError("worker repository content type drifted")
        files.add(relative)
    after = os.fstat(directory_fd)
    if worker_directory_identity(after) != worker_directory_identity(before):
        raise WorkerInputVerificationError("worker repository directory changed")
    return files, directories, content_sha256


def worker_open_absolute_directory_with_parent(path):
    if not path.is_absolute() or ".." in path.parts or path == Path("/"):
        raise WorkerInputVerificationError("worker directory binding is invalid")
    directory_fd = os.open("/", WORKER_DIRECTORY_FLAGS)
    try:
        for component in path.parts[1:-1]:
            next_fd = os.open(component, WORKER_DIRECTORY_FLAGS, dir_fd=directory_fd)
            os.close(directory_fd)
            directory_fd = next_fd
        child_fd = os.open(path.name, WORKER_DIRECTORY_FLAGS, dir_fd=directory_fd)
        return directory_fd, child_fd
    except Exception:
        os.close(directory_fd)
        raise


def verify_worker_checkout(*, repo_dir, candidate_sha, uid, gid):
    repo = Path(repo_dir)
    root = repo.parent
    if (
        not repo.is_absolute()
        or repo.parent != root
        or ".." in repo.parts
        or WORKER_SHA_RE.fullmatch(candidate_sha) is None
        or min(uid, gid) < 0
    ):
        raise WorkerInputVerificationError("worker checkout binding is invalid")
    try:
        root_parent_fd, root_fd = worker_open_absolute_directory_with_parent(root)
    except OSError as exc:
        raise WorkerInputVerificationError("worker checkout verification failed safely") from exc
    try:
        root_metadata = os.fstat(root_fd)
        root_lexical = os.stat(root.name, dir_fd=root_parent_fd, follow_symlinks=False)
        if (
            not stat.S_ISDIR(root_metadata.st_mode)
            or (root_metadata.st_dev, root_metadata.st_ino)
            != (root_lexical.st_dev, root_lexical.st_ino)
            or (root_metadata.st_uid, root_metadata.st_gid) != (uid, gid)
            or stat.S_IMODE(root_metadata.st_mode) != 0o2750
        ):
            raise WorkerInputVerificationError("worker repository root contract drifted")
        repo_fd = os.open(repo.name, WORKER_DIRECTORY_FLAGS, dir_fd=root_fd)
        try:
            repo_metadata = os.fstat(repo_fd)
            repo_lexical = os.stat(repo.name, dir_fd=root_fd, follow_symlinks=False)
            if (
                not stat.S_ISDIR(repo_metadata.st_mode)
                or (repo_metadata.st_dev, repo_metadata.st_ino)
                != (repo_lexical.st_dev, repo_lexical.st_ino)
                or (repo_metadata.st_uid, repo_metadata.st_gid) != (uid, gid)
                or stat.S_IMODE(repo_metadata.st_mode) != 0o750
            ):
                raise WorkerInputVerificationError("worker repository target contract drifted")
            git_fd = os.open(".git", WORKER_DIRECTORY_FLAGS, dir_fd=repo_fd)
            try:
                git_metadata = os.fstat(git_fd)
                git_lexical = os.stat(".git", dir_fd=repo_fd, follow_symlinks=False)
                if (
                    not stat.S_ISDIR(git_metadata.st_mode)
                    or (git_metadata.st_dev, git_metadata.st_ino)
                    != (git_lexical.st_dev, git_lexical.st_ino)
                    or (git_metadata.st_uid, git_metadata.st_gid) != (uid, gid)
                    or stat.S_IMODE(git_metadata.st_mode) != 0o750
                ):
                    raise WorkerInputVerificationError("worker git authority drifted")
                worker_validate_single_git_authority(git_fd, uid=uid, gid=gid)
                worker_validate_canonical_git_config(git_fd, uid=uid, gid=gid)
                git_identity = worker_git_metadata_identity(git_fd, uid=uid, gid=gid)
                object_format = worker_git(repo, "rev-parse", "--show-object-format").decode().strip()
                if object_format != "sha1":
                    raise WorkerInputVerificationError("worker repository object format is unsupported")
                head = worker_git(repo, "rev-parse", "--verify", "HEAD^{commit}").decode("ascii").strip()
                if head != candidate_sha:
                    raise WorkerInputVerificationError("worker repository head drifted")
                index, raw_index = worker_index(repo)
                if index != worker_commit_tree(repo, candidate_sha):
                    raise WorkerInputVerificationError("worker index drifted from commit tree")
                expected_directories = {
                    str(parent)
                    for relative in index
                    for parent in PurePosixPath(relative).parents
                    if str(parent) != "."
                }
                files, directories, content_sha256 = worker_validate_worktree(
                    repo_fd,
                    index=index,
                    uid=uid,
                    gid=gid,
                    budget=[0, 0],
                )
                if files != set(index) or directories != expected_directories:
                    raise WorkerInputVerificationError("worker physical index drifted")
                tree_content_hash = hashlib.sha256()
                for relative in sorted(index):
                    mode, object_id = index[relative]
                    for value in (relative, mode, object_id, content_sha256[relative]):
                        tree_content_hash.update(value.encode("utf-8"))
                        tree_content_hash.update(b"\0")
                tree_content_sha256 = tree_content_hash.hexdigest()
                worker_validate_canonical_git_config(git_fd, uid=uid, gid=gid)
                worker_validate_single_git_authority(git_fd, uid=uid, gid=gid)
                if worker_git_metadata_identity(git_fd, uid=uid, gid=gid) != git_identity:
                    raise WorkerInputVerificationError("worker git metadata changed")
                worker_assert_bound_directory(
                    git_fd,
                    repo_fd,
                    ".git",
                    git_metadata,
                    mode=0o750,
                    uid=uid,
                    gid=gid,
                )
            finally:
                os.close(git_fd)
            worker_assert_bound_directory(
                repo_fd,
                root_fd,
                repo.name,
                repo_metadata,
                mode=0o750,
                uid=uid,
                gid=gid,
            )
        finally:
            os.close(repo_fd)
        worker_assert_bound_directory(
            root_fd,
            root_parent_fd,
            root.name,
            root_metadata,
            mode=0o2750,
            uid=uid,
            gid=gid,
        )
    except (OSError, UnicodeError, ValueError, subprocess.SubprocessError) as exc:
        raise WorkerInputVerificationError("worker checkout verification failed safely") from exc
    finally:
        os.close(root_fd)
        os.close(root_parent_fd)
    return {
        "head": head,
        "index_sha256": hashlib.sha256(raw_index).hexdigest(),
        "tree_content_sha256": tree_content_sha256,
        "tracked_entries": len(index),
        "root_device": root_metadata.st_dev,
        "root_inode": root_metadata.st_ino,
        "target_device": repo_metadata.st_dev,
        "target_inode": repo_metadata.st_ino,
        "git_device": git_metadata.st_dev,
        "git_inode": git_metadata.st_ino,
    }


def verify_worker_environment(*, env_file, image_tag, requested_concurrency, uid, gid, service_env):
    path = Path(env_file)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or not isinstance(image_tag, str)
        or not image_tag
        or type(requested_concurrency) is not int
        or requested_concurrency <= 0
        or min(uid, gid) < 0
    ):
        raise WorkerInputVerificationError("worker environment binding is invalid")
    descriptor = os.open(path, WORKER_FILE_FLAGS)
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or (before.st_uid, before.st_gid) != (uid, gid)
            or before.st_nlink != 1
            or stat.S_IMODE(before.st_mode) != 0o600
            or not 0 < before.st_size <= WORKER_MAX_ENV_BYTES
        ):
            raise WorkerInputVerificationError("worker environment metadata is unsafe")
        payload = bytearray()
        while len(payload) <= WORKER_MAX_ENV_BYTES:
            chunk = os.read(descriptor, min(65536, WORKER_MAX_ENV_BYTES + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        after = os.fstat(descriptor)
        lexical = os.lstat(path)
        before_identity = (
            before.st_dev,
            before.st_ino,
            before.st_uid,
            before.st_gid,
            before.st_nlink,
            stat.S_IMODE(before.st_mode),
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        )
        if (
            len(payload) != before.st_size
            or (
                after.st_dev,
                after.st_ino,
                after.st_uid,
                after.st_gid,
                after.st_nlink,
                stat.S_IMODE(after.st_mode),
                after.st_size,
                after.st_mtime_ns,
                after.st_ctime_ns,
            )
            != before_identity
            or (
                lexical.st_dev,
                lexical.st_ino,
                lexical.st_uid,
                lexical.st_gid,
                lexical.st_nlink,
                stat.S_IMODE(lexical.st_mode),
                lexical.st_size,
                lexical.st_mtime_ns,
                lexical.st_ctime_ns,
            )
            != before_identity
        ):
            raise WorkerInputVerificationError("worker environment changed during verification")
    finally:
        os.close(descriptor)
    encoded = bytes(payload)
    if b"\0" in encoded or b"\r" in encoded:
        raise WorkerInputVerificationError("worker environment syntax is invalid")
    try:
        text = encoded.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise WorkerInputVerificationError("worker environment encoding is invalid") from exc
    values = {}
    for line in text.split("\n"):
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if (
            separator != "="
            or WORKER_ENV_KEY_RE.fullmatch(key) is None
            or key in values
        ):
            raise WorkerInputVerificationError("worker environment syntax is invalid")
        values[key] = value
    expected = {
        "IMAGE_TAG": image_tag,
        "ENV_CONFIG_VERSION": image_tag,
        "LOOM_IMAGE_TAG": image_tag,
        "LOOM_WORKER_ENV_CONFIG_VERSION": image_tag,
        "LOOM_WORKER_POOL_NAME": "gb10",
        "LOOM_WORKER_MAX_CONCURRENT": str(requested_concurrency),
        **service_env,
    }
    if any(values.get(key) != value for key, value in expected.items()) or any(
        not values.get(key)
        for key in (
            "LOOM_WORKER_TOKEN",
            "LOOM_WORKER_MINIO_ACCESS_KEY",
            "LOOM_WORKER_MINIO_SECRET_KEY",
        )
    ):
        raise WorkerInputVerificationError("worker environment contract drifted")
    return {
        "env_sha256": hashlib.sha256(encoded).hexdigest(),
        "env_device": before.st_dev,
        "env_inode": before.st_ino,
        "env_size": before.st_size,
        "env_mtime_ns": before.st_mtime_ns,
        "env_ctime_ns": before.st_ctime_ns,
    }


def verify_worker_inputs(*, repo_dir, env_file, candidate_sha, image_tag, requested_concurrency, uid, gid, service_env):
    checkout = verify_worker_checkout(
        repo_dir=repo_dir,
        candidate_sha=candidate_sha,
        uid=uid,
        gid=gid,
    )
    environment = verify_worker_environment(
        env_file=env_file,
        image_tag=image_tag,
        requested_concurrency=requested_concurrency,
        uid=uid,
        gid=gid,
        service_env=service_env,
    )
    return {**checkout, **environment}
"""


_WORKER_VERIFIER_NAMESPACE: dict[str, Any] = {}
exec(
    compile(_WORKER_INPUT_VERIFIER, "<gb10-worker-input-verifier>", "exec"),
    _WORKER_VERIFIER_NAMESPACE,
)


class AcceptanceError(RuntimeError):
    """A secret-safe acceptance failure."""


class AuthorityInterruptedError(AcceptanceError):
    """The authority received a termination signal and is unwinding safely."""


_ACTIVE_PROCESS: subprocess.Popen[str] | None = None


def _handle_authority_signal(signum: int, _frame: object) -> None:
    process = _ACTIVE_PROCESS
    if process is not None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    raise AuthorityInterruptedError(f"authority interrupted safely by signal {signum}")


def _install_signal_handlers() -> dict[int, Any]:
    previous: dict[int, Any] = {}
    for signum in (signal.SIGTERM, signal.SIGINT):
        previous[signum] = signal.signal(signum, _handle_authority_signal)
    return previous


def _restore_signal_handlers(previous: dict[int, Any]) -> None:
    for signum, handler in previous.items():
        signal.signal(signum, handler)


def _kill_and_reap(process: subprocess.Popen[str]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.communicate(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()


def _call_worker_verifier(name: str, **arguments: object) -> dict[str, object]:
    verifier = _WORKER_VERIFIER_NAMESPACE[name]
    try:
        evidence = verifier(**arguments)
    except Exception as exc:
        raise AcceptanceError("worker input verification failed safely") from exc
    if not isinstance(evidence, dict) or any(type(key) is not str for key in evidence):
        raise AcceptanceError("worker input verification returned invalid evidence")
    return cast(dict[str, object], evidence)


def _verify_worker_environment(
    env_file: Path,
    *,
    image_tag: str,
    requested_concurrency: int,
    uid: int,
    gid: int,
) -> dict[str, object]:
    try:
        return _call_worker_verifier(
            "verify_worker_environment",
            env_file=env_file,
            image_tag=image_tag,
            requested_concurrency=requested_concurrency,
            uid=uid,
            gid=gid,
            service_env=PRIVATE_WORKER_SERVICE_ENV["gb10"],
        )
    except AcceptanceError as exc:
        raise AcceptanceError("worker environment verification failed safely") from exc


def _verify_worker_inputs(
    *,
    repo_dir: Path,
    env_file: Path,
    candidate_sha: str,
    image_tag: str,
    requested_concurrency: int,
) -> dict[str, object]:
    return _call_worker_verifier(
        "verify_worker_inputs",
        repo_dir=repo_dir,
        env_file=env_file,
        candidate_sha=candidate_sha,
        image_tag=image_tag,
        requested_concurrency=requested_concurrency,
        uid=SERVICE_UID,
        gid=SERVICE_GID,
        service_env=PRIVATE_WORKER_SERVICE_ENV["gb10"],
    )


class VerifiedCandidateInputs:
    """Root-authenticated identities safe to carry into service-owned jobs."""

    __slots__ = (
        "candidate_tree",
        "registry_ca_sha256",
        "registry_probe_sha256",
        "worker_inputs",
    )

    def __init__(
        self,
        *,
        candidate_tree: str,
        registry_probe_sha256: str,
        registry_ca_sha256: str,
        worker_inputs: dict[str, object],
    ) -> None:
        self.candidate_tree = candidate_tree
        self.registry_probe_sha256 = registry_probe_sha256
        self.registry_ca_sha256 = registry_ca_sha256
        self.worker_inputs = worker_inputs


def _service_command(*argv: str) -> list[str]:
    return [RUNUSER, "-u", SERVICE_USER, "--", *argv]


def _bounded_timeout(timeout: float, deadline: float | None) -> float:
    if deadline is None:
        return timeout
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise AcceptanceError("authority overall time budget exhausted safely")
    return min(timeout, remaining)


def _run(
    argv: list[str],
    *,
    timeout: float = 30,
    check: bool = True,
    deadline: float | None = None,
) -> subprocess.CompletedProcess[str]:
    global _ACTIVE_PROCESS
    effective_timeout = _bounded_timeout(timeout, deadline)
    process: subprocess.Popen[str] | None = None
    try:
        blocked = {signal.SIGTERM, signal.SIGINT}
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, blocked)
        try:
            process = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                start_new_session=True,
            )
            _ACTIVE_PROCESS = process
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        stdout, stderr = process.communicate(timeout=effective_timeout)
    except subprocess.TimeoutExpired as exc:
        if process is not None:
            _kill_and_reap(process)
        raise AcceptanceError(f"command timed out safely: {Path(argv[0]).name}") from exc
    except BaseException:
        if process is not None:
            _kill_and_reap(process)
        raise
    finally:
        if _ACTIVE_PROCESS is process:
            _ACTIVE_PROCESS = None
    if process is None:
        raise AcceptanceError(f"command failed safely: {Path(argv[0]).name}")
    result = subprocess.CompletedProcess(argv, process.returncode, stdout, stderr)
    if check and result.returncode != 0:
        raise AcceptanceError(f"command failed safely: {Path(argv[0]).name}")
    return result


def _replace_release_values(value: Any, variables: dict[str, str]) -> Any:
    if isinstance(value, str):
        resolved = value
        for name, replacement in variables.items():
            resolved = resolved.replace(f"${{{name}}}", replacement)
        if "${" in resolved:
            raise AcceptanceError("profile contains an unresolved release value")
        return resolved
    if isinstance(value, list):
        return [_replace_release_values(item, variables) for item in value]
    if isinstance(value, dict):
        return {key: _replace_release_values(item, variables) for key, item in value.items()}
    return value


def _one_row(rows: object, *, pool_name: str) -> dict[str, Any]:
    if (
        not isinstance(rows, list)
        or not rows
        or any(
            not isinstance(row, dict)
            or type(row.get("pool_name")) is not str
            or not row["pool_name"]
            for row in rows
        )
    ):
        raise AcceptanceError(f"profile contains malformed {pool_name} rows")
    pool_names = [row["pool_name"] for row in rows]
    if len(set(pool_names)) != len(pool_names):
        raise AcceptanceError(f"profile contains duplicate {pool_name} rows")
    matches = [row for row in rows if row["pool_name"] == pool_name]
    if len(matches) != 1:
        raise AcceptanceError(f"profile must contain one {pool_name} row")
    return cast(dict[str, Any], matches[0])


def _exact_value(value: object, expected: object) -> bool:
    return type(value) is type(expected) and value == expected


def _exact_string_map(value: object, expected: dict[str, str]) -> bool:
    return bool(
        isinstance(value, dict)
        and set(value) == set(expected)
        and all(type(value[key]) is str and value[key] == item for key, item in expected.items())
    )


def _parse_contract(
    *,
    candidate_sha: str,
    image_tag: str,
    profile_path: Path,
    profile_bytes: bytes,
) -> dict[str, Any]:
    try:
        raw = tomllib.loads(profile_bytes.decode("utf-8"))
    except (UnicodeError, tomllib.TOMLDecodeError) as exc:
        raise AcceptanceError("exact candidate profile is unavailable") from exc
    profile = _replace_release_values(
        raw,
        {
            "IMAGE_TAG": image_tag,
            "ENV_CONFIG_VERSION": image_tag,
            "GIT_SHA": candidate_sha,
        },
    )
    policy = _one_row(profile.get("worker_pool_autoscaler_policies"), pool_name="gb10")
    config = policy.get("actuator_config")
    if not isinstance(config, dict):
        raise AcceptanceError("GB10 Slurm actuator configuration is unavailable")
    expected_config = {
        "slurm_cluster_name": CLUSTER_NAME,
        "slurm_controller_host": CONTROLLER_HOST,
        "partition": SLURM_PARTITION,
        "external_runner": True,
        "slurm_account": SLURM_ACCOUNT,
        "qos_normal": SLURM_QOS,
        "candidate_sha": candidate_sha,
        "max_jobs": len(SLURM_NODES),
        "repo_dir": (
            f"/shared_work2/loom-staging-rollout/worker-repos/loom-remote-worker-{image_tag}"
        ),
        "env_file": (
            f"/shared_work2/loom-staging-rollout/worker-envs/staging-gb10-worker-{image_tag}.env"
        ),
        "requested_concurrency": 10,
    }
    if (
        profile.get("environment") != "staging"
        or not _exact_value(policy.get("actuator"), "slurm")
        or policy.get("enabled") is not True
        or not _exact_value(policy.get("min_slots"), 0)
        or not _exact_value(policy.get("max_slots"), 150)
        or any(not _exact_value(config.get(key), value) for key, value in expected_config.items())
        or not isinstance(config.get("allowed_nodes"), list)
        or any(type(node) is not str for node in config["allowed_nodes"])
        or config["allowed_nodes"] != list(SLURM_NODES)
    ):
        raise AcceptanceError("GB10 Slurm policy does not match the accepted contract")
    desired = _one_row(profile.get("gb10_worker_pool_desired_states"), pool_name="gb10")
    expected_host_intents = {node: "stopped" for node in LEGACY_AGENT_NODES}
    if not _exact_value(desired.get("target_slots"), 0) or not _exact_string_map(
        desired.get("host_intents"), expected_host_intents
    ):
        raise AcceptanceError("legacy GB10 node-agent authority is not retired")
    prerequisites = profile.get("external_slurm_runner_prerequisites")
    pools = prerequisites.get("pools") if isinstance(prerequisites, dict) else None
    worker_service_env = (
        prerequisites.get("worker_service_env") if isinstance(prerequisites, dict) else None
    )
    if not isinstance(prerequisites, dict) or (
        prerequisites.get("materialize") is not True
        or prerequisites.get("require_external_allocation_authority") is not True
        or type(prerequisites.get("manager_witness_export_bootstrap", False)) is not bool
        or not isinstance(pools, list)
        or any(type(pool) is not str for pool in pools)
        or "gb10" not in pools
        or not isinstance(worker_service_env, dict)
        or set(worker_service_env) != set(PRIVATE_WORKER_SERVICE_ENV)
        or any(
            not _exact_string_map(worker_service_env.get(pool), expected)
            for pool, expected in PRIVATE_WORKER_SERVICE_ENV.items()
        )
    ):
        raise AcceptanceError("external Slurm authority prerequisites are incomplete")
    supervisors = profile.get("external_slurm_autoscaler_supervisors")
    supervisor = _one_row(supervisors, pool_name="gb10")
    manager_bootstrap = prerequisites.get("manager_witness_export_bootstrap") is True
    if not _exact_value(supervisor.get("execution_host"), CONTROLLER_HOST):
        raise AcceptanceError("GB10 supervisor is not controller-bound and active")
    if manager_bootstrap:
        if not isinstance(supervisors, list) or any(
            row.get("enabled") is not False or row.get("active") is not False for row in supervisors
        ):
            raise AcceptanceError("manager witness bootstrap supervisors are not inert")
    elif supervisor.get("enabled") is not True or supervisor.get("active") is not True:
        raise AcceptanceError("GB10 supervisor is not controller-bound and active")
    return {
        "profile_path": profile_path,
        "profile_sha256": hashlib.sha256(profile_bytes).hexdigest(),
        "repo_dir": Path(str(config.get("repo_dir", ""))),
        "env_file": Path(str(config.get("env_file", ""))),
        "image_tag": image_tag,
        "requested_concurrency": config.get("requested_concurrency"),
    }


def _load_contract(
    candidate_sha: str,
    image_tag: str,
    *,
    deadline: float | None = None,
) -> dict[str, Any]:
    runtime_repo = CANDIDATE_ROOT / candidate_sha / "repo"
    candidate_tree = _git_identity(
        runtime_repo,
        uid=0,
        gid=0,
        modes=frozenset({0o555, 0o755}),
        sha=candidate_sha,
        deadline=deadline,
    )
    profile_bytes = _read_trusted_candidate_asset(
        runtime_repo,
        candidate_tree=candidate_tree,
        relative_path=STAGING_PROFILE_RELATIVE_PATH,
        maximum_bytes=1024 * 1024,
        deadline=deadline,
    )
    return _parse_contract(
        candidate_sha=candidate_sha,
        image_tag=image_tag,
        profile_path=runtime_repo / STAGING_PROFILE_RELATIVE_PATH,
        profile_bytes=profile_bytes,
    )


def _verify_installed_authority() -> None:
    source = Path(__file__)
    try:
        metadata = source.lstat()
    except OSError as exc:
        raise AcceptanceError("installed authority source is unavailable") from exc
    if (
        source != INSTALLED_PATH
        or source.is_symlink()
        or not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755
    ):
        raise AcceptanceError("authority is not the fixed root-installed executable")


def _verify_controller(*, deadline: float | None = None) -> None:
    if os.geteuid() != 0:
        raise AcceptanceError("GB10 acceptance authority requires root")
    if platform.machine() != "aarch64" or platform.node().split(".", 1)[0] != CONTROLLER_HOST:
        raise AcceptanceError("GB10 acceptance authority is controller-only")
    config = _run(["/usr/bin/scontrol", "show", "config"], deadline=deadline).stdout
    if (
        re.search(rf"^ClusterName\s*=\s*{CLUSTER_NAME}$", config, re.MULTILINE) is None
        or re.search(
            rf"^SlurmctldHost(?:\[0\])?\s*=\s*{CONTROLLER_HOST}(?:\([^)]*\))?$",
            config,
            re.MULTILINE,
        )
        is None
    ):
        raise AcceptanceError("local Slurm controller authority does not match GB10")
    identity = _run(["/usr/bin/id", "-u", SERVICE_USER], deadline=deadline).stdout.strip()
    group = _run(["/usr/bin/id", "-g", SERVICE_USER], deadline=deadline).stdout.strip()
    groups = _run(["/usr/bin/id", "-nG", SERVICE_USER], deadline=deadline).stdout.split()
    if identity != str(SERVICE_UID) or group != str(SERVICE_GID) or "docker" not in groups:
        raise AcceptanceError("GB10 service identity does not match the fixed contract")


def _git_identity(
    repo: Path,
    *,
    uid: int,
    gid: int,
    modes: frozenset[int],
    sha: str,
    deadline: float | None = None,
) -> str:
    try:
        metadata = repo.lstat()
    except OSError as exc:
        raise AcceptanceError("candidate repository is unavailable") from exc
    if (
        repo.is_symlink()
        or not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or metadata.st_gid != gid
        or stat.S_IMODE(metadata.st_mode) not in modes
    ):
        raise AcceptanceError("candidate repository metadata is invalid")
    git = ["/usr/bin/git"]
    if uid == SERVICE_UID:
        git = _service_command("/usr/bin/git")
    git_timeout = 120 if uid == SERVICE_UID else 30
    head = _run(
        [*git, "-C", str(repo), "rev-parse", "HEAD"],
        timeout=git_timeout,
        deadline=deadline,
    ).stdout.strip()
    tree = _run(
        [*git, "-C", str(repo), "rev-parse", "HEAD^{tree}"],
        timeout=git_timeout,
        deadline=deadline,
    ).stdout.strip()
    dirty = _run(
        [*git, "-C", str(repo), "status", "--porcelain", "--untracked-files=no"],
        timeout=git_timeout,
        deadline=deadline,
    ).stdout
    if head != sha or SHA_RE.fullmatch(tree) is None or dirty:
        raise AcceptanceError("candidate repository identity does not match")
    return tree


def _read_trusted_candidate_asset(
    repo: Path,
    *,
    candidate_tree: str,
    relative_path: Path,
    maximum_bytes: int,
    deadline: float | None,
) -> bytes:
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise AcceptanceError("candidate asset path is unsafe")
    try:
        repo_metadata = repo.lstat()
    except OSError as exc:
        raise AcceptanceError("candidate asset is unavailable") from exc
    path = repo / relative_path
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise AcceptanceError("candidate asset is unavailable") from exc
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != repo_metadata.st_uid
            or metadata.st_gid != repo_metadata.st_gid
            or metadata.st_nlink != 1
            or metadata.st_mode & 0o002
            or not 0 < metadata.st_size <= maximum_bytes
        ):
            raise AcceptanceError("candidate asset metadata is unsafe")
        payload = bytearray()
        while len(payload) <= maximum_bytes:
            chunk = os.read(descriptor, min(65536, maximum_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        if len(payload) != metadata.st_size:
            raise AcceptanceError("candidate asset changed during validation")
        try:
            current = path.lstat()
        except OSError as exc:
            raise AcceptanceError("candidate asset changed during validation") from exc
        if (
            current.st_dev,
            current.st_ino,
            current.st_size,
            current.st_mtime_ns,
            current.st_ctime_ns,
        ) != (
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_size,
            metadata.st_mtime_ns,
            metadata.st_ctime_ns,
        ):
            raise AcceptanceError("candidate asset changed during validation")
    finally:
        os.close(descriptor)
    encoded = bytes(payload)
    expected_blob = _run(
        [
            "/usr/bin/git",
            "-C",
            str(repo),
            "rev-parse",
            f"{candidate_tree}:{relative_path.as_posix()}",
        ],
        deadline=deadline,
    ).stdout.strip()
    actual_blob = hashlib.sha1(
        f"blob {len(encoded)}\0".encode() + encoded,
        usedforsecurity=False,
    ).hexdigest()
    if SHA_RE.fullmatch(expected_blob) is None or actual_blob != expected_blob:
        raise AcceptanceError("candidate asset is outside the exact candidate tree")
    return encoded


def _verify_inputs(
    candidate_sha: str,
    contract: dict[str, Any],
    *,
    deadline: float | None = None,
) -> VerifiedCandidateInputs:
    runtime_repo = CANDIDATE_ROOT / candidate_sha / "repo"
    candidate_tree = _git_identity(
        runtime_repo,
        uid=0,
        gid=0,
        modes=frozenset({0o555, 0o755}),
        sha=candidate_sha,
        deadline=deadline,
    )
    worker_repo = contract["repo_dir"]
    env_file = contract["env_file"]
    worker_inputs = _verify_worker_inputs(
        repo_dir=worker_repo,
        env_file=env_file,
        candidate_sha=candidate_sha,
        image_tag=contract["image_tag"],
        requested_concurrency=contract["requested_concurrency"],
    )
    registry_probe = _read_trusted_candidate_asset(
        runtime_repo,
        candidate_tree=candidate_tree,
        relative_path=TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH,
        maximum_bytes=256 * 1024,
        deadline=deadline,
    )
    registry_ca = _read_trusted_candidate_asset(
        runtime_repo,
        candidate_tree=candidate_tree,
        relative_path=TRIAL_CACHE_CA_RELATIVE_PATH,
        maximum_bytes=64 * 1024,
        deadline=deadline,
    )
    registry_ca_sha256 = hashlib.sha256(registry_ca).hexdigest()
    if registry_ca_sha256 != TRIAL_CACHE_CA_SHA256:
        raise AcceptanceError("candidate registry CA does not match the fixed contract")
    return VerifiedCandidateInputs(
        candidate_tree=candidate_tree,
        registry_probe_sha256=hashlib.sha256(registry_probe).hexdigest(),
        registry_ca_sha256=registry_ca_sha256,
        worker_inputs=worker_inputs,
    )


_NODE_PROBE = (
    _WORKER_INPUT_VERIFIER
    + f"\nprint({ALLOCATION_START_MARKER!r}, flush=True)\n"
    + r"""
import hashlib, json, os, re, stat, subprocess, sys, tempfile


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def read_verified(path, expected_sha256, label, maximum_bytes):
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        require(
            stat.S_ISREG(metadata.st_mode)
            and metadata.st_uid == os.geteuid()
            and metadata.st_gid == os.getegid()
            and metadata.st_nlink == 1
            and not metadata.st_mode & 0o002
            and 0 < metadata.st_size <= maximum_bytes,
            f"{label} metadata is unsafe",
        )
        payload = bytearray()
        while len(payload) <= maximum_bytes:
            chunk = os.read(descriptor, min(65536, maximum_bytes + 1 - len(payload)))
            if not chunk:
                break
            payload.extend(chunk)
        require(len(payload) == metadata.st_size, f"{label} changed during validation")
        current = os.lstat(path)
        require(
            (
                current.st_dev,
                current.st_ino,
                current.st_size,
                current.st_mtime_ns,
                current.st_ctime_ns,
            )
            == (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_size,
                metadata.st_mtime_ns,
                metadata.st_ctime_ns,
            ),
            f"{label} changed during validation",
        )
    finally:
        os.close(descriptor)
    encoded = bytes(payload)
    require(
        hashlib.sha256(encoded).hexdigest() == expected_sha256,
        f"{label} identity mismatched",
    )
    return encoded


def snapshot_verified(directory, name, payload):
    path = os.path.join(directory, name)
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    descriptor = os.open(path, flags, 0o400)
    try:
        os.fchmod(descriptor, 0o400)
        offset = 0
        while offset < len(payload):
            offset += os.write(descriptor, payload[offset:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    return path


(
    node,
    repo,
    env_file,
    expected_sha,
    expected_tree,
    image_tag,
    requested_concurrency,
    expected_worker_inputs_json,
    worker_service_env_json,
    registry_repo,
    ca_sha256,
    canary_digest,
    registry_probe_path,
    registry_ca_path,
    trusted_probe_sha256,
    trusted_ca_sha256,
    service_uid,
    service_gid,
) = sys.argv[1:]
require(re.fullmatch(r"[0-9a-f]{40}", expected_sha) is not None, "candidate SHA is invalid")
require(re.fullmatch(r"[0-9a-f]{40}", expected_tree) is not None, "candidate tree is invalid")
require(
    re.fullmatch(r"[0-9a-f]{64}", trusted_probe_sha256) is not None,
    "registry probe identity is invalid",
)
require(
    re.fullmatch(r"[0-9a-f]{64}", trusted_ca_sha256) is not None
    and trusted_ca_sha256 == ca_sha256,
    "registry CA identity is invalid",
)
actual_node = subprocess.check_output(
    ["/usr/bin/scontrol", "show", "hostnames", os.environ["SLURM_JOB_NODELIST"]],
    text=True,
    timeout=15,
).strip()
require(actual_node == node, "Slurm allocation node mismatched")
require(
    os.geteuid() == int(service_uid) and os.getegid() == int(service_gid),
    "Slurm allocation service identity mismatched",
)
require(
    "docker" in subprocess.check_output(["/usr/bin/id", "-nG"], text=True, timeout=15).split(),
    "Slurm allocation Docker group is unavailable",
)
require(
    subprocess.run(
        ["/usr/bin/systemctl", "is-active", "loom-slurm-job-cgroup-guard.service"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    ).returncode
    == 0,
    "Slurm allocation cgroup guard is unavailable",
)
require(
    subprocess.run(
        ["docker", "info"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=15,
    ).returncode
    == 0,
    "Slurm allocation Docker daemon is unavailable",
)
try:
    expected_worker_inputs = json.loads(expected_worker_inputs_json)
    worker_service_env = json.loads(worker_service_env_json)
    actual_worker_inputs = verify_worker_inputs(
        repo_dir=repo,
        env_file=env_file,
        candidate_sha=expected_sha,
        image_tag=image_tag,
        requested_concurrency=int(requested_concurrency),
        uid=int(service_uid),
        gid=int(service_gid),
        service_env=worker_service_env,
    )
except (TypeError, ValueError, WorkerInputVerificationError) as exc:
    raise RuntimeError("worker input evidence mismatched") from exc
require(
    actual_worker_inputs == expected_worker_inputs,
    "worker input evidence mismatched",
)
probe_bytes = read_verified(
    registry_probe_path,
    trusted_probe_sha256,
    "registry probe",
    256 * 1024,
)
ca_bytes = read_verified(registry_ca_path, trusted_ca_sha256, "registry CA", 64 * 1024)
with tempfile.TemporaryDirectory(prefix="loom-accept-registry-", dir="/tmp") as snapshot_root:
    snapshot_metadata = os.lstat(snapshot_root)
    require(
        stat.S_ISDIR(snapshot_metadata.st_mode)
        and snapshot_metadata.st_uid == os.geteuid()
        and snapshot_metadata.st_gid == os.getegid()
        and stat.S_IMODE(snapshot_metadata.st_mode) == 0o700,
        "registry snapshot directory metadata is unsafe",
    )
    snapshot_probe = snapshot_verified(snapshot_root, "registry-probe.py", probe_bytes)
    snapshot_ca = snapshot_verified(snapshot_root, "registry-ca.crt", ca_bytes)
    snapshot_executor = (
        "import hashlib,os,stat,sys\n"
        "path,expected,*arguments=sys.argv[1:]\n"
        "flags=os.O_RDONLY|getattr(os,'O_CLOEXEC',0)|getattr(os,'O_NOFOLLOW',0)\n"
        "descriptor=os.open(path,flags)\n"
        "try:\n"
        " metadata=os.fstat(descriptor)\n"
        " if not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 "
        "or not 0 < metadata.st_size <= 262144: raise RuntimeError('snapshot metadata unsafe')\n"
        " payload=b''\n"
        " while len(payload) <= 262144:\n"
        "  chunk=os.read(descriptor,min(65536,262145-len(payload)))\n"
        "  if not chunk: break\n"
        "  payload+=chunk\n"
        "finally:\n"
        " os.close(descriptor)\n"
        "if len(payload) != metadata.st_size or hashlib.sha256(payload).hexdigest() != expected: "
        "raise RuntimeError('snapshot identity mismatched')\n"
        "sys.argv=[path,*arguments]\n"
        "exec(compile(payload,path,'exec'),{'__name__':'__main__','__file__':path})\n"
    )
    registry_probe = subprocess.check_output(
        [
            "/usr/bin/python3",
            "-c",
            snapshot_executor,
            snapshot_probe,
            trusted_probe_sha256,
            "--env-file",
            env_file,
            "--ca-file",
            snapshot_ca,
            "--docker-bin",
            "/usr/bin/docker",
            "--expected-registry-repo",
            registry_repo,
            "--expected-ca-sha256",
            ca_sha256,
            "--canary-digest",
            canary_digest,
        ],
        text=True,
        timeout=55,
    )
print(json.dumps(
    {
        "node": node,
        "candidate_sha": expected_sha,
        "trial_cache_registry": json.loads(registry_probe),
    },
    sort_keys=True,
))
"""
)


def _cleanup_probe_jobs(job_name: str, *, deadline: float | None = None) -> None:
    queue_command = _service_command(
        "/usr/bin/squeue",
        "--noheader",
        f"--user={SERVICE_USER}",
        f"--name={job_name}",
        "--format=%A|%j",
    )
    queued = _run(queue_command, timeout=10, check=False, deadline=deadline)
    if queued.returncode != 0:
        raise AcceptanceError("could not verify acceptance-probe cleanup")
    job_ids: list[str] = []
    for raw_line in queued.stdout.splitlines():
        job_id, separator, queued_name = raw_line.strip().partition("|")
        if not separator or queued_name != job_name or JOB_ID_RE.fullmatch(job_id) is None:
            raise AcceptanceError("acceptance-probe cleanup evidence is invalid")
        job_ids.append(job_id)
    if len(job_ids) > 1:
        raise AcceptanceError("acceptance-probe cleanup evidence is ambiguous")
    if job_ids:
        _run(
            _service_command(
                "/usr/bin/scancel",
                job_ids[0],
            ),
            timeout=10,
            deadline=deadline,
        )
        readback = _run(queue_command, timeout=10, check=False, deadline=deadline)
        if readback.returncode != 0 or readback.stdout.strip():
            raise AcceptanceError("acceptance-probe cleanup did not converge")


def _node_is_deferred_busy(
    *,
    node_config: str,
    result: subprocess.CompletedProcess[str],
    scheduler_never_started: bool,
) -> bool:
    state_match = re.search(r"(?:^| )State=([A-Z]+)", node_config)
    cpu_match = re.search(r"(?:^| )CPUAlloc=([0-9]+)", node_config)
    memory_match = re.search(r"(?:^| )AllocMem=([0-9]+)", node_config)
    busy_error = (
        "Requested nodes are busy" in result.stderr
        or "temporarily unable to accept work" in result.stderr
    )
    return bool(
        result.returncode != 0
        and scheduler_never_started
        and ALLOCATION_START_MARKER not in result.stdout.splitlines()
        and busy_error
        and state_match is not None
        and state_match.group(1) in {"ALLOCATED", "MIXED"}
        and (
            (cpu_match is not None and int(cpu_match.group(1)) > 0)
            or (memory_match is not None and int(memory_match.group(1)) > 0)
        )
    )


def _allocation_never_started(
    job_name: str,
    *,
    node: str,
    deadline: float | None,
) -> bool:
    result = _run(
        _service_command(
            SACCT,
            "--noheader",
            "--parsable2",
            "--allocations",
            f"--user={SERVICE_USER}",
            f"--name={job_name}",
            "--starttime=now-10minutes",
            "--format=JobIDRaw,JobName,State,Start,NodeList",
        ),
        timeout=10,
        check=False,
        deadline=deadline,
    )
    lines = [line for line in result.stdout.splitlines() if line]
    if result.returncode != 0 or result.stderr or len(lines) != 1:
        return False
    fields = lines[0].split("|")
    if len(fields) != 5:
        return False
    job_id, recorded_name, state, started_at, recorded_node = fields
    return bool(
        JOB_ID_RE.fullmatch(job_id) is not None
        and recorded_name == job_name
        and state.partition(" ")[0] in {"CANCELLED", "PENDING"}
        and started_at == "Unknown"
        and recorded_node == node
    )


def _probe_nodes(
    candidate_sha: str,
    verified: VerifiedCandidateInputs,
    contract: dict[str, Any],
    *,
    work_deadline: float | None = None,
    cleanup_deadline: float | None = None,
) -> tuple[list[str], list[str]]:
    passed: list[str] = []
    deferred_busy: list[str] = []
    for index, node in enumerate(SLURM_NODES, start=1):
        node_config = _run(
            ["/usr/bin/scontrol", "show", "node", node, "-o"],
            deadline=work_deadline,
        ).stdout
        partition_pattern = rf"(?:^| )Partitions=[^ ]*\b{re.escape(SLURM_PARTITION)}\b"
        if re.search(partition_pattern, node_config) is None:
            raise AcceptanceError(f"canonical node is outside {SLURM_PARTITION} partition: {node}")
        job_name = f"loom-accept-{candidate_sha[:7]}-{index}-{os.getpid()}"
        command = _service_command(
            SRUN,
            "--quiet",
            "--immediate=15",
            f"--job-name={job_name}",
            "--nodes=1",
            "--ntasks=1",
            "--cpus-per-task=1",
            "--mem=128M",
            "--time=00:02:00",
            f"--partition={SLURM_PARTITION}",
            f"--account={SLURM_ACCOUNT}",
            f"--qos={SLURM_QOS}",
            f"--nodelist={node}",
            "/usr/bin/python3",
            "-c",
            _NODE_PROBE,
            node,
            str(contract["repo_dir"]),
            str(contract["env_file"]),
            candidate_sha,
            verified.candidate_tree,
            contract["image_tag"],
            str(contract["requested_concurrency"]),
            json.dumps(verified.worker_inputs, sort_keys=True, separators=(",", ":")),
            json.dumps(
                PRIVATE_WORKER_SERVICE_ENV["gb10"],
                sort_keys=True,
                separators=(",", ":"),
            ),
            TRIAL_CACHE_REGISTRY_REPO,
            TRIAL_CACHE_CA_SHA256,
            TRIAL_CACHE_CANARY_DIGEST,
            str(contract["repo_dir"] / TRIAL_CACHE_NODE_PROBE_RELATIVE_PATH),
            str(contract["repo_dir"] / TRIAL_CACHE_CA_RELATIVE_PATH),
            verified.registry_probe_sha256,
            verified.registry_ca_sha256,
            str(SERVICE_UID),
            str(SERVICE_GID),
        )
        try:
            result = _run(
                command,
                timeout=60,
                check=False,
                deadline=work_deadline,
            )
        finally:
            _cleanup_probe_jobs(job_name, deadline=cleanup_deadline)
        potential_unstarted_busy = bool(
            result.returncode != 0
            and ALLOCATION_START_MARKER not in result.stdout.splitlines()
            and (
                "Requested nodes are busy" in result.stderr
                or "temporarily unable to accept work" in result.stderr
            )
        )
        scheduler_never_started = bool(
            potential_unstarted_busy
            and _allocation_never_started(
                job_name,
                node=node,
                deadline=cleanup_deadline,
            )
        )
        if _node_is_deferred_busy(
            node_config=node_config,
            result=result,
            scheduler_never_started=scheduler_never_started,
        ):
            deferred_busy.append(node)
            continue
        if result.returncode != 0:
            raise AcceptanceError(f"node allocation failed safely: {node}")
        output_lines = result.stdout.splitlines()
        if len(output_lines) != 2 or output_lines[0] != ALLOCATION_START_MARKER:
            raise AcceptanceError(f"node allocation returned invalid evidence: {node}")
        try:
            payload = json.loads(output_lines[1])
        except json.JSONDecodeError as exc:
            raise AcceptanceError(f"node allocation returned invalid evidence: {node}") from exc
        if payload != {
            "candidate_sha": candidate_sha,
            "node": node,
            "trial_cache_registry": {
                "ca_sha256": TRIAL_CACHE_CA_SHA256,
                "registry_image": (f"{TRIAL_CACHE_REGISTRY_REPO}:{TRIAL_CACHE_CANARY_TAG}"),
                "repo_digest": (f"{TRIAL_CACHE_REGISTRY_REPO}@{TRIAL_CACHE_CANARY_DIGEST}"),
            },
        }:
            raise AcceptanceError(f"node allocation evidence mismatched: {node}")
        passed.append(node)
    if not passed:
        raise AcceptanceError("no GB10 node accepted the real Slurm allocation probe")
    return passed, deferred_busy


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, encoded: bytes) -> None:
    offset = 0
    while offset < len(encoded):
        written = os.write(descriptor, encoded[offset:])
        if written <= 0:
            raise AcceptanceError("authority artifact write failed safely")
        offset += written


def _read_published_artifact(path: Path, expected: bytes) -> None:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or stat.S_IMODE(metadata.st_mode) != 0o644
            or metadata.st_size != len(expected)
        ):
            raise AcceptanceError("authority evidence digest collision")
        encoded = bytearray()
        while len(encoded) <= len(expected):
            chunk = os.read(descriptor, len(expected) + 1 - len(encoded))
            if not chunk:
                break
            encoded.extend(chunk)
        if bytes(encoded) != expected:
            raise AcceptanceError("authority evidence digest collision")
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _write_artifact(payload: dict[str, Any]) -> Path:
    state_root_existed = STATE_ROOT.exists()
    STATE_ROOT.mkdir(mode=0o755, parents=True, exist_ok=True)
    metadata = STATE_ROOT.lstat()
    if (
        STATE_ROOT.is_symlink()
        or metadata.st_uid != 0
        or metadata.st_gid != 0
        or stat.S_IMODE(metadata.st_mode) != 0o755
    ):
        raise AcceptanceError("authority state root metadata is invalid")
    if not state_root_existed:
        _fsync_directory(STATE_ROOT.parent)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    encoded = canonical + b"\n"
    evidence_digest = hashlib.sha256(canonical).hexdigest()
    evidence_root = STATE_ROOT / "evidence"
    evidence_root_existed = evidence_root.exists()
    evidence_root.mkdir(mode=0o755, exist_ok=True)
    evidence_metadata = evidence_root.lstat()
    if (
        evidence_root.is_symlink()
        or not stat.S_ISDIR(evidence_metadata.st_mode)
        or stat.S_IMODE(evidence_metadata.st_mode) != 0o755
    ):
        raise AcceptanceError("authority evidence root metadata is invalid")
    if not evidence_root_existed:
        _fsync_directory(STATE_ROOT)
    immutable = evidence_root / f"{evidence_digest}.json"
    descriptor, temporary_name = tempfile.mkstemp(prefix=".evidence.", dir=evidence_root)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o644)
        _write_all(descriptor, encoded)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        try:
            os.link(temporary, immutable, follow_symlinks=False)
        except FileExistsError:
            pass
        else:
            _fsync_directory(evidence_root)
        temporary.unlink()
        _fsync_directory(evidence_root)
        _read_published_artifact(immutable, encoded)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if os.path.lexists(temporary):
            temporary.unlink()
            _fsync_directory(evidence_root)

    descriptor, temporary_name = tempfile.mkstemp(prefix=".current.", dir=STATE_ROOT)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o644)
        _write_all(descriptor, encoded)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = -1
        os.replace(temporary, ARTIFACT_PATH)
        _fsync_directory(STATE_ROOT)
        _read_published_artifact(ARTIFACT_PATH, encoded)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        if temporary.exists():
            temporary.unlink()
            _fsync_directory(STATE_ROOT)
    return immutable


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--image-tag", required=True)
    return parser


def _main() -> int:
    started = time.monotonic()
    cleanup_deadline = started + AUTHORITY_BUDGET_SECONDS
    work_deadline = cleanup_deadline - CLEANUP_RESERVE_SECONDS
    args = _parser().parse_args()
    if SHA_RE.fullmatch(args.candidate_sha) is None:
        raise AcceptanceError("candidate SHA must be exact")
    if args.image_tag != f"staging-{args.candidate_sha[:7]}":
        raise AcceptanceError("image tag does not match the exact candidate")
    _verify_installed_authority()
    _verify_controller(deadline=work_deadline)
    contract = _load_contract(
        args.candidate_sha,
        args.image_tag,
        deadline=work_deadline,
    )
    verified = _verify_inputs(args.candidate_sha, contract, deadline=work_deadline)
    probed_nodes, deferred_busy_nodes = _probe_nodes(
        args.candidate_sha,
        verified,
        contract,
        work_deadline=work_deadline,
        cleanup_deadline=cleanup_deadline,
    )
    _bounded_timeout(AUTHORITY_BUDGET_SECONDS, cleanup_deadline)
    generated_at = datetime.now(UTC)
    artifact = {
        "schema_version": 1,
        "kind": "loom_gb10_slurm_acceptance",
        "result": "pass",
        "candidate_sha": args.candidate_sha,
        "candidate_tree": verified.candidate_tree,
        "profile_sha256": contract["profile_sha256"],
        "cluster_name": CLUSTER_NAME,
        "controller_host": CONTROLLER_HOST,
        "service_identity": {
            "user": SERVICE_USER,
            "uid": SERVICE_UID,
            "gid": SERVICE_GID,
            "account": SLURM_ACCOUNT,
            "qos": SLURM_QOS,
        },
        "nodes": list(SLURM_NODES),
        "node_count": len(SLURM_NODES),
        "probed_nodes": probed_nodes,
        "probed_node_count": len(probed_nodes),
        "deferred_busy_nodes": deferred_busy_nodes,
        "trial_cache_registry": {
            "ca_sha256": TRIAL_CACHE_CA_SHA256,
            "canary_digest": TRIAL_CACHE_CANARY_DIGEST,
            "repository": TRIAL_CACHE_REGISTRY_REPO,
        },
        "generated_at": generated_at.isoformat(),
        "expires_at": (generated_at + timedelta(minutes=30)).isoformat(),
    }
    _write_artifact(artifact)
    print(json.dumps(artifact, sort_keys=True, separators=(",", ":")))
    return 0


def main() -> int:
    previous = _install_signal_handlers()
    try:
        return _main()
    finally:
        _restore_signal_handlers(previous)


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (AcceptanceError, OSError, subprocess.TimeoutExpired) as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(1) from None
