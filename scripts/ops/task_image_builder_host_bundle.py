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
from collections.abc import Mapping, Sequence
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
    runtime: Mapping[str, object],
    architecture: str,
) -> tuple[tuple[str, str], ...]:
    architectures = host_release._object(runtime.get("architectures"), "runtime architectures")
    selected = host_release._object(
        architectures.get(architecture),
        "runtime architecture",
    )
    raw_artifacts = selected.get("artifacts")
    if not isinstance(raw_artifacts, list) or len(raw_artifacts) != 4:
        raise BundleAssemblyError("runtime artifact set is invalid")
    artifacts: list[tuple[str, str]] = []
    for raw in raw_artifacts:
        item = host_release._object(raw, "runtime artifact")
        name = host_release._safe_relative(item.get("name"), "runtime artifact name")
        if PurePosixPath(name).name != name:
            raise BundleAssemblyError("runtime artifact name is invalid")
        url = _validate_https_url(
            host_release._string(item.get("url"), "runtime artifact URL"),
            "runtime URL",
        )
        artifacts.append((name, url))
    if len({name for name, _ in artifacts}) != len(artifacts):
        raise BundleAssemblyError("runtime artifact names are not unique")
    return tuple(artifacts)


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
        runtime = host_release._load_json(runtime_manifest_path, "runtime manifest")
        runtime_artifacts = _runtime_artifacts(runtime, architecture)
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
        for artifact in release.packages[debian_architecture].values():
            fetcher.fetch(
                _fetch_url(repository.base_url, artifact.filename),
                temporary / "packages" / PurePosixPath(artifact.filename).name,
                artifact.size,
            )
        for name, url in runtime_artifacts:
            fetcher.fetch(
                url,
                temporary / "runtime" / name,
                host_release.MAX_ARTIFACT_BYTES,
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
        if verified is not None:
            try:
                verified.close()
            except host_release.HostReleaseError:
                pass
        if temporary is not None and not published and os.path.lexists(temporary):
            shutil.rmtree(temporary, ignore_errors=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    assemble = subparsers.add_parser("assemble")
    assemble.add_argument("--release", type=Path, required=True)
    assemble.add_argument("--runtime-manifest", type=Path, required=True)
    assemble.add_argument("--keyring", type=Path, required=True)
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
