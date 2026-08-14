#!/usr/bin/env python3
"""Validate and materialize the closed Stage 1 simulator image source set."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import subprocess
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO


class SourceLockError(ValueError):
    """The checked-in lock or an observed source violated the closed contract."""


_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_SOURCE_NAMES = ("b1k", "bddl", "curobo", "dlimp", "lerobot", "omnigibson", "openpi")
_LOCK_TOP_KEYS = {
    "base_image",
    "loom_runtime",
    "runtime_assets",
    "schema_version",
    "sim_python",
    "sources",
    "vla_python",
}
_BASE_KEYS = {"index_digest", "platform", "platform_manifest_digest", "reference"}
_RUNTIME_ASSET_KEYS = {
    "binary_version",
    "license",
    "name",
    "release_tag",
    "sha256",
    "url",
    "version",
}
_PYTHON_KEYS = {"accepted_freeze_sha256", "lock_sha256", "python_version"}
_LOOM_RUNTIME_KEYS = {"lock_sha256"}
_PUBLIC_KEYS = {"commit", "name", "repository", "tree", "visibility"}
_PUBLIC_EXCLUSIONS = {
    "curobo": ("images", "src/curobo/content/assets"),
    "lerobot": ("tests",),
}
_VENDORED_KEYS = {
    "commit",
    "excluded_upstream_entries",
    "name",
    "projection_sha256",
    "source_identity",
    "tree",
    "vendor_path",
    "visibility",
}
_OPENPI_CACHE_PATCH_PATH = PurePosixPath("openpi/src/openpi/models_pytorch/gemma_pytorch.py")
_OPENPI_CACHE_PATCH_SOURCE_SHA256 = (
    "sha256:08fd8d750519f0fb44fc5173311e50a30f4c8f32c02e51244b4f8e47b32cd52f"
)
_OPENPI_CACHE_PATCH_RESULT_SHA256 = (
    "sha256:4f75d3647fadb7d00c0fee884579cf5a3ef33a6af53a3908fc237358d9606cf5"
)


@dataclass(frozen=True)
class Source:
    name: str
    commit: str
    tree: str
    visibility: str
    repository: str | None = None
    subdirectory: str | None = None
    vendor_path: str | None = None
    projection_sha256: str | None = None
    excluded_upstream_entries: tuple[str, ...] = ()


@dataclass(frozen=True)
class SourceLock:
    raw: dict[str, Any]
    sources: tuple[Source, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def _closed(value: dict[str, Any], expected: set[str], label: str) -> None:
    if set(value) != expected:
        raise SourceLockError(f"{label} keys are not closed")


def _digest(value: object, label: str) -> str:
    if not isinstance(value, str) or _DIGEST.fullmatch(value) is None:
        raise SourceLockError(f"{label} is not a canonical SHA-256 digest")
    return value


def _git_sha(value: object, label: str) -> str:
    if not isinstance(value, str) or _GIT_SHA.fullmatch(value) is None:
        raise SourceLockError(f"{label} is not a full lowercase Git SHA")
    return value


def load_source_lock(repo_root: Path) -> SourceLock:
    path = repo_root / "deploy/behavior-stage1-sim/source-lock.json"
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SourceLockError(f"cannot read Stage 1 image source lock: {exc}") from exc
    if not isinstance(raw, dict):
        raise SourceLockError("Stage 1 image source lock must be an object")
    _closed(raw, _LOCK_TOP_KEYS, "source lock")
    if raw["schema_version"] != "loom.behavior-stage1-image-sources.v1":
        raise SourceLockError("unsupported Stage 1 source lock schema")

    base = raw["base_image"]
    if not isinstance(base, dict):
        raise SourceLockError("base_image must be an object")
    _closed(base, _BASE_KEYS, "base_image")
    _digest(base["index_digest"], "base image index")
    _digest(base["platform_manifest_digest"], "base image platform manifest")
    if base["platform"] != "linux/amd64":
        raise SourceLockError("Stage 1 base image must be linux/amd64")
    if base["reference"] != "nvcr.io/nvidia/isaac-sim:5.1.0":
        raise SourceLockError("Stage 1 base image reference drift")

    runtime_assets = raw["runtime_assets"]
    if not isinstance(runtime_assets, list) or len(runtime_assets) != 1:
        raise SourceLockError("runtime_assets must contain exactly FFmpeg")
    ffmpeg = runtime_assets[0]
    if not isinstance(ffmpeg, dict):
        raise SourceLockError("FFmpeg runtime asset must be an object")
    _closed(ffmpeg, _RUNTIME_ASSET_KEYS, "FFmpeg runtime asset")
    _digest(ffmpeg["sha256"], "FFmpeg runtime asset")
    if (
        ffmpeg["name"] != "ffmpeg"
        or ffmpeg["version"] != "7.1.5-12-g1fdbca85aa"
        or ffmpeg["binary_version"] != "n7.1.5-12-g1fdbca85aa-20260813"
        or ffmpeg["release_tag"] != "autobuild-2026-08-13-17-03"
        or ffmpeg["license"] != "LGPL-2.1-or-later"
        or ffmpeg["url"]
        != "https://github.com/BtbN/FFmpeg-Builds/releases/download/"
        "autobuild-2026-08-13-17-03/"
        "ffmpeg-n7.1.5-12-g1fdbca85aa-linux64-lgpl-shared-7.1.tar.xz"
    ):
        raise SourceLockError("FFmpeg runtime asset drift")

    for key, filename in (
        ("sim_python", "sim.requirements.lock.txt"),
        ("vla_python", "vla.uv.lock"),
    ):
        value = raw[key]
        if not isinstance(value, dict):
            raise SourceLockError(f"{key} must be an object")
        _closed(value, _PYTHON_KEYS, key)
        _digest(value["accepted_freeze_sha256"], f"{key} accepted freeze")
        expected = _digest(value["lock_sha256"], f"{key} lock")
        if value["python_version"] != "3.11.13":
            raise SourceLockError(f"{key} Python version drift")
        if _sha256(repo_root / "deploy/behavior-stage1-sim" / filename) != expected:
            raise SourceLockError(f"{key} checked-in lock digest drift")

    loom_runtime = raw["loom_runtime"]
    if not isinstance(loom_runtime, dict):
        raise SourceLockError("loom_runtime must be an object")
    _closed(loom_runtime, _LOOM_RUNTIME_KEYS, "loom_runtime")
    expected_loom_runtime = _digest(loom_runtime["lock_sha256"], "Loom runtime lock")
    if (
        _sha256(repo_root / "deploy/behavior-stage1-sim/loom-runtime.requirements.lock.txt")
        != expected_loom_runtime
    ):
        raise SourceLockError("Loom runtime checked-in lock digest drift")

    values = raw["sources"]
    if not isinstance(values, list) or len(values) != len(_SOURCE_NAMES):
        raise SourceLockError("sources must contain the exact Stage 1 source set")
    sources: list[Source] = []
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            raise SourceLockError("source entry must be an object")
        name = value.get("name")
        if name != _SOURCE_NAMES[index]:
            raise SourceLockError("sources must be bytewise ordered and complete")
        visibility = value.get("visibility")
        expected_keys = _VENDORED_KEYS if visibility == "vendored-runtime" else _PUBLIC_KEYS
        if name == "bddl" and visibility == "public":
            expected_keys = _PUBLIC_KEYS | {"subdirectory"}
        if name in _PUBLIC_EXCLUSIONS and visibility == "public":
            expected_keys = _PUBLIC_KEYS | {"excluded_upstream_entries"}
        _closed(value, expected_keys, f"source {name}")
        commit = _git_sha(value.get("commit"), f"source {name} commit")
        tree = _git_sha(value.get("tree"), f"source {name} tree")
        repository = value.get("repository")
        if visibility == "public":
            if (
                not isinstance(repository, str)
                or not repository.startswith("https://github.com/")
                or not repository.endswith(".git")
            ):
                raise SourceLockError(f"source {name} public repository is invalid")
            excluded_upstream_entries = value.get("excluded_upstream_entries", [])
            if name in _PUBLIC_EXCLUSIONS:
                if excluded_upstream_entries != list(_PUBLIC_EXCLUSIONS[name]):
                    raise SourceLockError(f"source {name} public source exclusion drift")
            elif excluded_upstream_entries:
                raise SourceLockError(f"source {name} must not exclude upstream entries")
        elif visibility == "vendored-runtime":
            if name != "omnigibson":
                raise SourceLockError("vendored source identity drift")
            if value.get("source_identity") != "authorized-behavior-1k-restore":
                raise SourceLockError("vendored source authority drift")
            vendor_path = value.get("vendor_path")
            if vendor_path != "third_party/behavior-stage1/omnigibson":
                raise SourceLockError("vendored source path drift")
            if value.get("excluded_upstream_entries") != [
                "OmniGibson/omnigibson/learning/configs/policy/hybrid_mp.yaml"
            ]:
                raise SourceLockError("vendored source exclusion drift")
            expected_projection = _digest(
                value.get("projection_sha256"), "vendored source projection"
            )
            if _vendored_projection_digest(repo_root / vendor_path) != expected_projection:
                raise SourceLockError("vendored source projection digest drift")
        else:
            raise SourceLockError(f"source {name} visibility is invalid")
        sources.append(
            Source(
                name=name,
                commit=commit,
                tree=tree,
                visibility=visibility,
                repository=repository if isinstance(repository, str) else None,
                subdirectory=value.get("subdirectory"),
                vendor_path=value.get("vendor_path"),
                projection_sha256=value.get("projection_sha256"),
                excluded_upstream_entries=tuple(value.get("excluded_upstream_entries", [])),
            )
        )
    return SourceLock(raw=raw, sources=tuple(sources))


def _run(
    argv: list[str],
    *,
    cwd: Path | None = None,
    stdout: BinaryIO | None = None,
    environment: dict[str, str] | None = None,
) -> None:
    try:
        subprocess.run(argv, cwd=cwd, stdout=stdout, check=True, env=environment)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SourceLockError(f"source command failed: {argv[0]}") from exc


def _member_path(name: str, expected_root: str) -> PurePosixPath:
    path = PurePosixPath(name)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise SourceLockError("source archive contains an unsafe path")
    if path.parts[0] != expected_root:
        raise SourceLockError("source archive root drift")
    return path


def _resolve_confined_symlink(
    member_name: str,
    link_name: str,
    expected_root: str,
) -> PurePosixPath:
    link = PurePosixPath(link_name)
    if link.is_absolute():
        raise SourceLockError("source archive contains an absolute symlink")
    parts = list(PurePosixPath(member_name).parent.parts)
    for part in link.parts:
        if part in {"", "."}:
            continue
        if part == "..":
            if len(parts) <= 1:
                raise SourceLockError("source archive symlink escapes its root")
            parts.pop()
        else:
            parts.append(part)
    return _member_path(PurePosixPath(*parts).as_posix(), expected_root)


def _extract_regular_archive(
    archive_path: Path,
    destination: Path,
    expected_root: str,
    *,
    excluded_symlinks: tuple[tuple[str, str], ...] = (),
    excluded_prefixes: tuple[str, ...] = (),
) -> None:
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    seen: set[PurePosixPath] = set()
    excluded = dict(excluded_symlinks)
    observed_exclusions: set[str] = set()
    observed_prefixes: set[str] = set()
    total = 0
    try:
        with tarfile.open(archive_path, mode="r:*") as archive:
            members = archive.getmembers()
            by_name: dict[str, tarfile.TarInfo] = {}
            for member in members:
                if member.name in by_name:
                    raise SourceLockError("source archive contains a duplicate path")
                by_name[member.name] = member
            for member in members:
                path = _member_path(member.name, expected_root)
                if path in seen:
                    raise SourceLockError("source archive contains a duplicate path")
                seen.add(path)
                relative_path = PurePosixPath(*path.parts[1:]).as_posix()
                matching_prefix = next(
                    (
                        prefix
                        for prefix in excluded_prefixes
                        if relative_path == prefix or relative_path.startswith(f"{prefix}/")
                    ),
                    None,
                )
                if matching_prefix is not None:
                    observed_prefixes.add(matching_prefix)
                    continue
                if member.issym() and member.name in excluded:
                    if member.linkname != excluded[member.name]:
                        raise SourceLockError("source archive excluded symlink target drift")
                    observed_exclusions.add(member.name)
                    continue
                source_member = member
                if member.issym():
                    resolved = _resolve_confined_symlink(
                        member.name,
                        member.linkname,
                        expected_root,
                    )
                    resolved_member = by_name.get(resolved.as_posix())
                    if resolved_member is None or not resolved_member.isfile():
                        raise SourceLockError(
                            "source archive symlink does not resolve to a regular member"
                        )
                    source_member = resolved_member
                relative = PurePosixPath(*path.parts[1:])
                if not relative.parts:
                    if not member.isdir():
                        raise SourceLockError("source archive root must be a directory")
                    continue
                target = destination.joinpath(*relative.parts)
                if member.isdir():
                    target.mkdir(mode=0o700, parents=True, exist_ok=True)
                    continue
                if not (member.isfile() or member.issym()) or source_member.size < 0:
                    raise SourceLockError("source archive contains a non-regular entry")
                total += source_member.size
                if total > 1_073_741_824 or len(seen) > 200_000:
                    raise SourceLockError("source archive exceeds the bounded source budget")
                target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                if target.exists() or target.is_symlink():
                    raise SourceLockError("source archive target already exists")
                source = archive.extractfile(source_member)
                if source is None:
                    raise SourceLockError("source archive member cannot be read")
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
                flags |= getattr(os, "O_NOFOLLOW", 0)
                descriptor = os.open(target, flags, 0o444)
                with os.fdopen(descriptor, "wb") as output:
                    shutil.copyfileobj(source, output, length=1024 * 1024)
                    output.flush()
                    os.fsync(output.fileno())
                os.chmod(target, 0o555 if source_member.mode & 0o111 else 0o444)
        if observed_exclusions != set(excluded):
            raise SourceLockError("source archive excluded symlink set drift")
        if observed_prefixes != set(excluded_prefixes):
            raise SourceLockError("source archive excluded prefix set drift")
        for directory in sorted(
            (path for path in destination.rglob("*") if path.is_dir()),
            key=lambda path: len(path.parts),
            reverse=True,
        ):
            os.chmod(directory, 0o555)
        os.chmod(destination, 0o555)
    except (OSError, tarfile.TarError) as exc:
        raise SourceLockError(f"cannot extract source archive: {exc}") from exc


def _materialize_public(source: Source, destination: Path, staging: Path) -> None:
    if source.repository is None:
        raise SourceLockError(f"source {source.name} repository is unavailable")
    repository = staging / f"git-{source.name}"
    _run(["git", "init", "--quiet", str(repository)])
    _run(["git", "remote", "add", "origin", source.repository], cwd=repository)
    _run(
        [
            "git",
            "-c",
            "protocol.version=2",
            "fetch",
            "--quiet",
            "--no-tags",
            "--depth=1",
            "--filter=blob:none",
            "origin",
            source.commit,
        ],
        cwd=repository,
    )
    try:
        observed_commit = subprocess.check_output(
            ["git", "rev-parse", "FETCH_HEAD"], cwd=repository, text=True
        ).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise SourceLockError(f"cannot verify source {source.name} commit") from exc
    if observed_commit != source.commit:
        raise SourceLockError(f"source {source.name} commit drift")
    treeish = source.commit
    if source.subdirectory is not None:
        try:
            observed_tree = subprocess.check_output(
                ["git", "rev-parse", f"{source.commit}:{source.subdirectory}"],
                cwd=repository,
                text=True,
            ).strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise SourceLockError(f"cannot verify source {source.name} tree") from exc
        treeish = f"{source.commit}:{source.subdirectory}"
    else:
        try:
            observed_tree = subprocess.check_output(
                ["git", "rev-parse", f"{source.commit}^{{tree}}"], cwd=repository, text=True
            ).strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise SourceLockError(f"cannot verify source {source.name} tree") from exc
    if observed_tree != source.tree:
        raise SourceLockError(f"source {source.name} tree drift")
    archive_path = staging / f"{source.name}.tar"
    with archive_path.open("xb") as output:
        _run(
            ["git", "archive", "--format=tar", f"--prefix={source.name}/", treeish],
            cwd=repository,
            stdout=output,
            environment={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1"},
        )
    try:
        target = destination / source.name
        _extract_regular_archive(
            archive_path,
            target,
            source.name,
            excluded_prefixes=source.excluded_upstream_entries,
        )
        _reject_lfs_pointers(target)
    except SourceLockError as exc:
        raise SourceLockError(f"source {source.name} extraction failed: {exc}") from exc


def _reject_lfs_pointers(root: Path) -> None:
    marker = b"version https://git-lfs.github.com/spec/v1\n"
    for path in root.rglob("*"):
        if path.is_file() and path.stat().st_size <= 1024 and path.read_bytes().startswith(marker):
            raise SourceLockError(
                "source projection contains a Git LFS pointer: "
                f"{path.relative_to(root).as_posix()}"
            )


def _vendored_files(root: Path) -> tuple[tuple[Path, dict[str, object]], ...]:
    if root.is_symlink() or not root.is_dir():
        raise SourceLockError("vendored source root must be a real directory")
    values: list[tuple[Path, dict[str, object]]] = []
    total = 0
    for path in sorted(
        root.rglob("*"), key=lambda item: item.relative_to(root).as_posix().encode()
    ):
        observed = path.lstat()
        if stat.S_ISDIR(observed.st_mode):
            continue
        if not stat.S_ISREG(observed.st_mode) or observed.st_nlink != 1:
            raise SourceLockError("vendored source contains a non-private regular file")
        total += observed.st_size
        if total > 1_073_741_824 or len(values) >= 200_000:
            raise SourceLockError("vendored source exceeds the bounded source budget")
        values.append(
            (
                path,
                {
                    "executable": bool(observed.st_mode & 0o111),
                    "path": path.relative_to(root).as_posix(),
                    "sha256": _sha256(path),
                    "size_bytes": observed.st_size,
                },
            )
        )
    if not values:
        raise SourceLockError("vendored source is empty")
    return tuple(values)


def _vendored_projection_digest(root: Path) -> str:
    inventory = [value for _, value in _vendored_files(root)]
    payload = (
        json.dumps(inventory, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
        + b"\n"
    )
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def _materialize_vendored(source: Source, repo_root: Path, destination: Path) -> None:
    if source.vendor_path is None:
        raise SourceLockError("vendored source path is unavailable")
    source_root = repo_root / source.vendor_path
    files = _vendored_files(source_root)
    target_root = destination / source.name
    target_root.mkdir(mode=0o700, parents=True, exist_ok=False)
    for path, value in files:
        relative = PurePosixPath(str(value["path"]))
        target = target_root.joinpath(*relative.parts)
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        before = path.lstat()
        source_flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        source_fd = os.open(path, source_flags)
        target_flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        target_flags |= getattr(os, "O_NOFOLLOW", 0)
        target_fd = os.open(target, target_flags, 0o444)
        with (
            os.fdopen(source_fd, "rb") as input_stream,
            os.fdopen(target_fd, "wb") as output_stream,
        ):
            shutil.copyfileobj(input_stream, output_stream, length=1024 * 1024)
            output_stream.flush()
            os.fsync(output_stream.fileno())
        after = path.lstat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise SourceLockError("vendored source changed while reading")
        os.chmod(target, 0o555 if value["executable"] else 0o444)
    for directory in sorted(
        (path for path in target_root.rglob("*") if path.is_dir()),
        key=lambda path: len(path.parts),
        reverse=True,
    ):
        os.chmod(directory, 0o555)
    os.chmod(target_root, 0o555)


def _apply_openpi_cache_patch(output: Path) -> dict[str, str]:
    """Remove an upstream test-only type dependency under an exact byte lock."""

    path = output.joinpath(*_OPENPI_CACHE_PATCH_PATH.parts)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise SourceLockError("OpenPI cache patch target must be a private regular file")
    if _sha256(path) != _OPENPI_CACHE_PATCH_SOURCE_SHA256:
        raise SourceLockError("OpenPI cache patch source drift")
    source = path.read_bytes()
    import_before = b"from typing import Literal\n\nimport pytest\n"
    import_after = b"from typing import Literal\n\nfrom transformers.cache_utils import Cache\n"
    annotation_before = b"list[torch.FloatTensor] | pytest.Cache | None"
    annotation_after = b"list[torch.FloatTensor] | Cache | None"
    if source.count(import_before) != 1 or source.count(annotation_before) != 1:
        raise SourceLockError("OpenPI cache patch replacement cardinality drift")
    result = source.replace(import_before, import_after).replace(
        annotation_before,
        annotation_after,
    )
    if f"sha256:{hashlib.sha256(result).hexdigest()}" != _OPENPI_CACHE_PATCH_RESULT_SHA256:
        raise SourceLockError("OpenPI cache patch result drift")

    parent = path.parent
    os.chmod(parent, 0o700)
    temporary = parent / ".gemma_pytorch.py.loom-patch"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(temporary, flags, 0o444)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(result)
            stream.flush()
            os.fsync(stream.fileno())
        after = path.lstat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise SourceLockError("OpenPI cache patch target changed while reading")
        os.replace(temporary, path)
        os.chmod(path, 0o444)
        directory_fd = os.open(parent, os.O_RDONLY | getattr(os, "O_CLOEXEC", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if temporary.exists():
            temporary.unlink()
        os.chmod(parent, 0o555)
    return {
        "name": "openpi-transformers-cache-type",
        "path": _OPENPI_CACHE_PATCH_PATH.as_posix(),
        "result_sha256": _OPENPI_CACHE_PATCH_RESULT_SHA256,
        "source_sha256": _OPENPI_CACHE_PATCH_SOURCE_SHA256,
    }


def materialize(repo_root: Path, *, output: Path) -> None:
    lock = load_source_lock(repo_root)
    if output.exists() or output.is_symlink():
        raise SourceLockError("source output must not already exist")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    try:
        with tempfile.TemporaryDirectory(prefix="loom-stage1-sources-") as raw_staging:
            staging = Path(raw_staging)
            for source in lock.sources:
                if source.visibility == "vendored-runtime":
                    _materialize_vendored(source, repo_root, output)
                else:
                    _materialize_public(source, output, staging)
        integration_patches = [_apply_openpi_cache_patch(output)]
        evidence = {
            "integration_patches": integration_patches,
            "schema_version": "loom.behavior-stage1-image-source-evidence.v1",
            "source_lock_sha256": _sha256(
                repo_root / "deploy/behavior-stage1-sim/source-lock.json"
            ),
            "sources": [
                {"commit": item.commit, "name": item.name, "tree": item.tree}
                for item in lock.sources
            ],
        }
        evidence_path = output / "source-evidence.json"
        payload = (
            json.dumps(evidence, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
            + b"\n"
        )
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(evidence_path, flags, 0o444)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
    except BaseException:
        for path in sorted(output.rglob("*"), key=lambda item: len(item.parts), reverse=True):
            if path.is_dir() and not path.is_symlink():
                os.chmod(path, 0o700)
        os.chmod(output, 0o700)
        shutil.rmtree(output)
        raise


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate")
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "validate":
            lock = load_source_lock(args.repo_root.resolve())
            print(f"validated {len(lock.sources)} Stage 1 image sources")
        else:
            materialize(
                args.repo_root.resolve(),
                output=args.output.resolve(),
            )
        return 0
    except SourceLockError as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
