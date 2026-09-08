#!/usr/bin/env python3
"""Assemble one verified, offline Phase 1 task-image-builder host bundle."""

from __future__ import annotations

import argparse
import ctypes
import errno
import json
import os
import shutil
import stat
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Sequence
from pathlib import Path, PurePosixPath
from typing import Any, Protocol

from scripts.ops import task_image_builder_host_release as host_release
from scripts.ops.task_image_builder_host_release import SubprocessCommandRunner

_AT_FDCWD = -100
_RENAME_NOREPLACE = 1
_FETCH_TIMEOUT_SECONDS = 30.0
_COPY_BUFFER_BYTES = 1024 * 1024
_MAX_ERROR_LENGTH = 512


class BundleAssemblyError(RuntimeError):
    """The requested host bundle could not be assembled safely."""


class ArtifactFetcher(Protocol):
    def fetch(self, url: str, destination: Path, maximum: int) -> None: ...


class _HttpsOnlyRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(
        self,
        req: urllib.request.Request,
        fp: Any,
        code: int,
        msg: str,
        headers: Any,
        newurl: str,
    ) -> urllib.request.Request | None:
        if urllib.parse.urlsplit(newurl).scheme != "https":
            raise BundleAssemblyError("artifact fetch rejected a non-HTTPS redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


def _validate_https_url(url: str, label: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
    ):
        raise BundleAssemblyError(f"{label} is invalid")
    return url


class HttpsArtifactFetcher:
    def fetch(self, url: str, destination: Path, maximum: int) -> None:
        _validate_https_url(url, "artifact URL")
        if maximum <= 0:
            raise BundleAssemblyError("artifact size limit is invalid")
        descriptor = -1
        complete = False
        try:
            descriptor = os.open(
                destination,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_NOFOLLOW
                | getattr(os, "O_CLOEXEC", 0),
                0o400,
            )
            opener = urllib.request.build_opener(_HttpsOnlyRedirectHandler())
            request = urllib.request.Request(url, method="GET")
            with opener.open(request, timeout=_FETCH_TIMEOUT_SECONDS) as response:
                copied = 0
                while True:
                    chunk = response.read(min(_COPY_BUFFER_BYTES, maximum - copied + 1))
                    if not chunk:
                        break
                    copied += len(chunk)
                    if copied > maximum:
                        raise BundleAssemblyError("artifact response exceeds its size limit")
                    view = memoryview(chunk)
                    while view:
                        written = os.write(descriptor, view)
                        if written <= 0:
                            raise BundleAssemblyError("artifact write failed")
                        view = view[written:]
            try:
                os.fsync(descriptor)
            except OSError as exc:
                raise BundleAssemblyError("artifact file fsync failed") from exc
            complete = True
        except BundleAssemblyError:
            raise
        except (OSError, TimeoutError, urllib.error.URLError) as exc:
            raise BundleAssemblyError("artifact fetch failed") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            if not complete:
                try:
                    destination.unlink(missing_ok=True)
                except OSError:
                    pass


def _runtime_artifacts(
    runtime: host_release.RuntimeManifest,
) -> tuple[host_release.RuntimeArtifact, ...]:
    artifacts = runtime.artifacts
    if len(artifacts) != 4:
        raise BundleAssemblyError("runtime artifact set is invalid")
    if len({item.name for item in artifacts}) != len(artifacts):
        raise BundleAssemblyError("runtime artifact names are not unique")
    for artifact in artifacts:
        if artifact.url is not None:
            _validate_https_url(artifact.url, "runtime URL")
    return artifacts


def _validate_runtime_artifact_root(path: Path, expected: set[str]) -> Path:
    if not path.is_absolute():
        raise BundleAssemblyError("runtime artifact root must be an absolute path")
    try:
        metadata = path.lstat()
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise BundleAssemblyError("runtime artifact root is unavailable") from exc
    if (
        path.is_symlink()
        or resolved != path
        or not stat.S_ISDIR(metadata.st_mode)
        or bool(stat.S_IMODE(metadata.st_mode) & 0o022)
    ):
        raise BundleAssemblyError("runtime artifact root is unsafe")
    try:
        entries = sorted(path.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise BundleAssemblyError("runtime artifact root is unavailable") from exc
    if {item.name for item in entries} != expected:
        raise BundleAssemblyError("runtime artifact inventory is invalid")
    return path


def _stage_local_runtime_artifacts(
    runtime_artifact_root: Path,
    artifacts: Sequence[host_release.RuntimeArtifact],
    destination: Path,
) -> None:
    source_root = _validate_runtime_artifact_root(
        runtime_artifact_root,
        {item.name for item in artifacts},
    )
    for artifact in artifacts:
        payload = host_release._read_regular(
            source_root / artifact.name,
            host_release.MAX_ARTIFACT_BYTES,
            "runtime artifact",
            reject_group_world_write=True,
        )
        _write_private_file(destination / artifact.name, payload)


def _write_private_file(path: Path, payload: bytes) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_NOFOLLOW
            | getattr(os, "O_CLOEXEC", 0),
            0o400,
        )
        view = memoryview(payload)
        while view:
            written = os.write(descriptor, view)
            if written <= 0:
                raise BundleAssemblyError("private file write failed")
            view = view[written:]
        try:
            os.fsync(descriptor)
        except OSError as exc:
            raise BundleAssemblyError("file fsync failed") from exc
    except OSError as exc:
        raise BundleAssemblyError("private file cannot be created") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = -1
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY
            | os.O_NOFOLLOW
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_CLOEXEC", 0),
        )
        os.fsync(descriptor)
    except OSError as exc:
        raise BundleAssemblyError("directory fsync failed") from exc
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_tree(root: Path) -> None:
    directories: list[Path] = []
    for current, directory_names, file_names in os.walk(root, followlinks=False):
        current_path = Path(current)
        directories.append(current_path)
        for name in directory_names:
            metadata = (current_path / name).lstat()
            if not stat.S_ISDIR(metadata.st_mode):
                raise BundleAssemblyError("assembled bundle contains an unsafe directory")
        for name in file_names:
            path = current_path / name
            descriptor = -1
            try:
                descriptor = os.open(
                    path,
                    os.O_RDONLY
                    | os.O_NOFOLLOW
                    | getattr(os, "O_CLOEXEC", 0),
                )
                metadata = os.fstat(descriptor)
                if not stat.S_ISREG(metadata.st_mode):
                    raise BundleAssemblyError("assembled bundle contains an unsafe file")
                os.fsync(descriptor)
            except OSError as exc:
                raise BundleAssemblyError("file fsync failed") from exc
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
    for directory in reversed(directories):
        _fsync_directory(directory)


def _rename_noreplace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise BundleAssemblyError("atomic publication is unavailable")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        _AT_FDCWD,
        os.fsencode(source),
        _AT_FDCWD,
        os.fsencode(destination),
        _RENAME_NOREPLACE,
    )
    if result != 0:
        error = ctypes.get_errno()
        if error in {errno.EEXIST, errno.ENOTEMPTY}:
            raise BundleAssemblyError("output already exists")
        raise BundleAssemblyError("atomic publication failed")


def _validate_output(output: Path) -> None:
    if output.name in {"", ".", ".."}:
        raise BundleAssemblyError("output path is invalid")
    if os.path.lexists(output):
        raise BundleAssemblyError("output already exists")
    try:
        parent = output.parent.lstat()
    except OSError as exc:
        raise BundleAssemblyError("output parent is unavailable") from exc
    if (
        not stat.S_ISDIR(parent.st_mode)
        or parent.st_uid != os.geteuid()
        or parent.st_mode & 0o022
    ):
        raise BundleAssemblyError("output parent is not owner-controlled")


def _fetch_url(base_url: str, relative: str) -> str:
    return f"{base_url}/{relative}"


def assemble_host_bundle(
    *,
    release_path: Path,
    runtime_manifest_path: Path,
    keyring_path: Path,
    architecture: str,
    output: Path,
    fetcher: ArtifactFetcher,
    runtime_artifact_root: Path | None = None,
) -> str:
    temporary: Path | None = None
    published = False
    verified: host_release.VerifiedHostBundle | None = None
    try:
        _validate_output(output)
        release = host_release.load_host_release(release_path)
        if runtime_manifest_path.name != release.runtime_manifest:
            raise BundleAssemblyError("runtime manifest path does not match the release")
        debian_architecture = release.architecture_map.get(architecture)
        if debian_architecture is None:
            raise BundleAssemblyError("architecture is not in the host release")
        runtime = host_release.load_runtime_manifest(
            runtime_manifest_path,
            architecture=architecture,
            debian_architecture=debian_architecture,
        )
        runtime_artifacts = _runtime_artifacts(runtime)
        repository = release.repositories[debian_architecture]
        expected_base_url = (
            f"https://snapshot.ubuntu.com/ubuntu/{release.snapshot}"
        )
        if repository.base_url != expected_base_url:
            raise BundleAssemblyError("repository base URL is invalid")

        keyring = host_release._read_regular(
            keyring_path,
            host_release.MAX_METADATA_BYTES,
            "Ubuntu archive keyring",
        )
        temporary = Path(
            tempfile.mkdtemp(
                prefix=f".{output.name}.loom-tmp-",
                dir=output.parent,
            )
        )
        temporary.chmod(0o700)
        for directory in ("apt", "packages", "runtime"):
            (temporary / directory).mkdir(mode=0o700)
        _write_private_file(temporary / release.keyring_name, keyring)

        for suite, index in repository.indexes.items():
            fetcher.fetch(
                _fetch_url(repository.base_url, index.inrelease_path),
                temporary / "apt" / f"{suite}.InRelease",
                index.inrelease_size,
            )
            fetcher.fetch(
                _fetch_url(repository.base_url, index.packages_path),
                temporary / "apt" / f"{suite}.Packages.xz",
                index.packages_size,
            )
        for package_artifact in release.packages[debian_architecture].values():
            fetcher.fetch(
                _fetch_url(repository.base_url, package_artifact.filename),
                temporary / "packages" / PurePosixPath(package_artifact.filename).name,
                package_artifact.size,
            )
        if all(artifact.url is not None for artifact in runtime_artifacts):
            for artifact in runtime_artifacts:
                assert artifact.url is not None
                fetcher.fetch(
                    _validate_https_url(artifact.url, "runtime URL"),
                    temporary / "runtime" / artifact.name,
                    host_release.MAX_ARTIFACT_BYTES,
                )
        else:
            if runtime_artifact_root is None:
                raise BundleAssemblyError("runtime artifact root is required")
            _stage_local_runtime_artifacts(
                runtime_artifact_root,
                runtime_artifacts,
                temporary / "runtime",
            )

        verified = host_release.verify_host_bundle(
            temporary,
            release,
            architecture,
            SubprocessCommandRunner(),
            runtime_manifest_path=runtime_manifest_path,
            required_snapshot_owner=os.geteuid(),
        )
        digest = verified.bundle_digest
        verified.close()
        verified = None
        _fsync_tree(temporary)
        _fsync_directory(output.parent)
        _rename_noreplace(temporary, output)
        published = True
        return digest
    except BundleAssemblyError:
        raise
    except host_release.HostReleaseError as exc:
        raise BundleAssemblyError(str(exc)) from exc
    except OSError as exc:
        raise BundleAssemblyError("bundle assembly failed") from exc
    finally:
        cleanup_errors: list[BaseException] = []
        if verified is not None:
            try:
                verified.close()
            except host_release.HostReleaseError as exc:
                cleanup_errors.append(exc)
        if temporary is not None and not published and os.path.lexists(temporary):
            try:
                shutil.rmtree(temporary)
            except OSError as exc:
                cleanup_errors.append(exc)
        if cleanup_errors:
            raise BundleAssemblyError("bundle assembly cleanup failed") from cleanup_errors[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    assemble = subparsers.add_parser("assemble")
    assemble.add_argument("--release", type=Path, required=True)
    assemble.add_argument("--runtime-manifest", type=Path, required=True)
    assemble.add_argument("--keyring", type=Path, required=True)
    assemble.add_argument("--runtime-artifact-root", type=Path)
    assemble.add_argument("--architecture", choices=tuple(host_release.ARCHITECTURE_MAP), required=True)
    assemble.add_argument("--output", type=Path, required=True)
    return parser


def _bounded_error(error: BaseException) -> str:
    message = " ".join(str(error).split()) or "bundle assembly failed"
    return message[:_MAX_ERROR_LENGTH]


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        digest = assemble_host_bundle(
            release_path=args.release,
            runtime_manifest_path=args.runtime_manifest,
            keyring_path=args.keyring,
            architecture=args.architecture,
            output=args.output,
            fetcher=HttpsArtifactFetcher(),
            runtime_artifact_root=args.runtime_artifact_root,
        )
    except BundleAssemblyError as exc:
        print(
            json.dumps(
                {"assembled": False, "error": _bounded_error(exc)},
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "architecture": args.architecture,
                "assembled": True,
                "bundle_digest": digest,
                "output": str(args.output),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
