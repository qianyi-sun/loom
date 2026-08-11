"""Deterministic source sealing for personal shared-fleet deployments.

The snapshot is intentionally a content artifact, not a Git ref. It includes
the current contents of tracked and non-ignored untracked regular files, so a
developer can deploy unfinished work without implying CI approval. Sensitive
paths, ignored outputs, Git metadata, links, and special files never enter the
archive.
"""

from __future__ import annotations

import errno
import hashlib
import io
import json
import os
import stat
import subprocess
import tarfile
import tempfile
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass
from pathlib import Path, PurePosixPath

_DEFAULT_MAX_FILES = 100_000
_DEFAULT_MAX_TOTAL_BYTES = 256 * 1024 * 1024
_DEFAULT_MAX_FILE_BYTES = 64 * 1024 * 1024
_MAX_PATH_BYTES = 240
_PRIVATE_KEY_SUFFIXES = (".key", ".pem", ".p12", ".pfx")
_SENSITIVE_NAMES = frozenset({".env", "credentials", "secrets.toml", "id_rsa", "id_ed25519"})
_SAFE_TEMPLATE_SUFFIXES = (".example", ".sample", ".template")


class PersonalDevSourceError(RuntimeError):
    """The checkout cannot be represented as a safe immutable source artifact."""


@dataclass(frozen=True, slots=True)
class PersonalDevSourceFileV1:
    path: str
    mode: int
    size: int
    sha256: str


@dataclass(frozen=True, slots=True)
class PersonalDevSourceManifestV1:
    schema_version: int
    attestation_scope: str
    source_commit: str
    dirty: bool
    worktree_state_sha256: str
    contexts: tuple[str, ...]
    files: tuple[PersonalDevSourceFileV1, ...]
    deleted_tracked_paths: tuple[str, ...]
    excluded_sensitive_paths: tuple[str, ...]
    file_count: int
    total_bytes: int


@dataclass(frozen=True, slots=True)
class PersonalDevSourceSnapshotV1:
    manifest: PersonalDevSourceManifestV1
    source_digest: str
    archive_sha256: str


@dataclass(frozen=True, slots=True)
class _Scan:
    manifest: PersonalDevSourceManifestV1
    manifest_bytes: bytes
    candidate_paths: tuple[str, ...]


def _git(root: Path, *args: str) -> bytes:
    try:
        result = subprocess.run(
            ["git", "-c", "core.quotepath=false", "-C", str(root), *args],
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise PersonalDevSourceError("Git is unavailable for source provenance") from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise PersonalDevSourceError(f"Git source inspection failed: {detail or 'unknown error'}")
    return result.stdout


def _decode_git_paths(payload: bytes, *, label: str) -> tuple[str, ...]:
    values: list[str] = []
    for raw in payload.rstrip(b"\0").split(b"\0") if payload else ():
        try:
            value = raw.decode("utf-8", errors="strict")
        except UnicodeDecodeError as exc:
            raise PersonalDevSourceError(f"{label} contains a non-UTF-8 path") from exc
        _validate_relative_path(value, label=label)
        values.append(value)
    if len(values) != len(set(values)):
        raise PersonalDevSourceError(f"{label} contains duplicate paths")
    return tuple(sorted(values))


def _validate_relative_path(value: str, *, label: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value
        or value == "."
        or path.is_absolute()
        or ".." in path.parts
        or ".git" in path.parts
        or len(value.encode("utf-8")) > _MAX_PATH_BYTES
        or any(part in {"", "."} for part in path.parts)
    ):
        raise PersonalDevSourceError(f"{label} contains an unsafe path: {value!r}")
    return path


def _normalize_contexts(contexts: Iterable[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for raw in contexts:
        if raw == ".":
            value = "."
        else:
            value = _validate_relative_path(raw, label="source context").as_posix().rstrip("/")
        normalized.append(value)
    if not normalized or len(normalized) != len(set(normalized)):
        raise PersonalDevSourceError("source contexts must be a non-empty unique set")
    return tuple(sorted(normalized))


def _open_directory(root_fd: int, relative: str) -> int:
    current = os.dup(root_fd)
    try:
        if relative == ".":
            return current
        for component in PurePosixPath(relative).parts:
            next_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=current,
            )
            os.close(current)
            current = next_fd
        return current
    except Exception:
        os.close(current)
        raise


def _validate_context_directories(root_fd: int, contexts: tuple[str, ...]) -> None:
    for context in contexts:
        try:
            descriptor = _open_directory(root_fd, context)
        except OSError as exc:
            raise PersonalDevSourceError(
                f"source context is unavailable through the no-follow boundary: {context!r}"
            ) from exc
        os.close(descriptor)


def _is_sensitive(path: str) -> bool:
    name = PurePosixPath(path).name.lower()
    if name in _SENSITIVE_NAMES or name.endswith(_PRIVATE_KEY_SUFFIXES):
        return True
    if name.startswith(".env.") and not name.endswith(_SAFE_TEMPLATE_SUFFIXES):
        return True
    stem = name.rsplit(".", 1)[0]
    return stem in {"credential", "credentials", "secret", "secrets"} and not name.endswith(
        _SAFE_TEMPLATE_SUFFIXES
    )


def _read_regular_file(
    root_fd: int,
    path: str,
    *,
    max_file_bytes: int,
) -> tuple[PersonalDevSourceFileV1, bytes]:
    pure = _validate_relative_path(path, label="source inventory")
    parent = "." if len(pure.parts) == 1 else PurePosixPath(*pure.parts[:-1]).as_posix()
    try:
        parent_fd = _open_directory(root_fd, parent)
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise PersonalDevSourceError(
            f"source path parent is not a safe directory: {path!r}"
        ) from exc
    try:
        try:
            descriptor = os.open(
                pure.name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
                dir_fd=parent_fd,
            )
        except FileNotFoundError:
            raise
        except OSError as exc:
            if exc.errno == errno.ELOOP:
                raise PersonalDevSourceError(
                    f"source inventory contains a symbolic link: {path!r}"
                ) from exc
            raise PersonalDevSourceError(f"source path cannot be opened safely: {path!r}") from exc
    finally:
        os.close(parent_fd)
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            kind = "symbolic link" if stat.S_ISLNK(before.st_mode) else "non-regular file"
            raise PersonalDevSourceError(f"source inventory contains a {kind}: {path!r}")
        if before.st_nlink != 1:
            raise PersonalDevSourceError(f"source file must have exactly one link: {path!r}")
        if before.st_size > max_file_bytes:
            raise PersonalDevSourceError(f"source file exceeds the per-file byte limit: {path!r}")
        chunks: list[bytes] = []
        observed = 0
        while True:
            chunk = os.read(descriptor, min(1024 * 1024, max_file_bytes + 1 - observed))
            if not chunk:
                break
            chunks.append(chunk)
            observed += len(chunk)
            if observed > max_file_bytes:
                raise PersonalDevSourceError(
                    f"source file exceeds the per-file byte limit: {path!r}"
                )
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    before_identity = (
        before.st_dev,
        before.st_ino,
        before.st_mode,
        before.st_nlink,
        before.st_size,
        before.st_mtime_ns,
        before.st_ctime_ns,
    )
    after_identity = (
        after.st_dev,
        after.st_ino,
        after.st_mode,
        after.st_nlink,
        after.st_size,
        after.st_mtime_ns,
        after.st_ctime_ns,
    )
    if before_identity != after_identity or observed != after.st_size:
        raise PersonalDevSourceError(f"source file changed while it was being sealed: {path!r}")
    data = b"".join(chunks)
    mode = 0o755 if after.st_mode & 0o111 else 0o644
    return (
        PersonalDevSourceFileV1(
            path=path,
            mode=mode,
            size=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        ),
        data,
    )


def _candidate_paths(root: Path, contexts: tuple[str, ...]) -> tuple[str, ...]:
    return _decode_git_paths(
        _git(
            root,
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            *contexts,
        ),
        label="Git source inventory",
    )


def _tracked_paths(root: Path, contexts: tuple[str, ...]) -> frozenset[str]:
    return frozenset(
        _decode_git_paths(
            _git(root, "ls-files", "-z", "--cached", "--", *contexts),
            label="Git tracked source inventory",
        )
    )


def _git_ignored(root: Path, relative: str) -> bool:
    result = subprocess.run(
        [
            "git",
            "-c",
            "core.quotepath=false",
            "-C",
            str(root),
            "check-ignore",
            "--quiet",
            "--no-index",
            "--",
            relative,
        ],
        check=False,
        capture_output=True,
    )
    if result.returncode not in {0, 1}:
        detail = result.stderr.decode("utf-8", errors="replace").strip()
        raise PersonalDevSourceError(f"Git ignore inspection failed: {detail or 'unknown error'}")
    return result.returncode == 0


def _reject_unignored_special_files(root: Path, contexts: tuple[str, ...]) -> None:
    """Reject special filesystem objects Git intentionally omits from ls-files."""

    for context in contexts:
        start = root if context == "." else root / context
        for directory, directories, filenames, directory_fd in os.fwalk(
            start,
            topdown=True,
            follow_symlinks=False,
        ):
            directory_path = Path(directory)
            relative_directory = directory_path.relative_to(root)
            retained: list[str] = []
            for name in directories:
                relative = (relative_directory / name).as_posix()
                if name == ".git" or _git_ignored(root, relative + "/"):
                    continue
                retained.append(name)
            directories[:] = retained
            for name in filenames:
                try:
                    metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
                except FileNotFoundError as exc:
                    raise PersonalDevSourceError(
                        "source checkout changed while it was being sealed"
                    ) from exc
                if stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                    continue
                relative = (relative_directory / name).as_posix()
                if not _git_ignored(root, relative):
                    raise PersonalDevSourceError(
                        f"source inventory contains a non-regular file: {relative!r}"
                    )


def _manifest_bytes(manifest: PersonalDevSourceManifestV1) -> bytes:
    return json.dumps(
        asdict(manifest),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _exact_int(value: object, *, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise PersonalDevSourceError(f"personal-dev source manifest has invalid {label}")
    return value


def _exact_string(value: object, *, label: str) -> str:
    if not isinstance(value, str):
        raise PersonalDevSourceError(f"personal-dev source manifest has invalid {label}")
    return value


def _string_tuple(value: object, *, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise PersonalDevSourceError(f"personal-dev source manifest has invalid {label}")
    values = tuple(value)
    if values != tuple(sorted(values)) or len(values) != len(set(values)):
        raise PersonalDevSourceError(f"personal-dev source manifest has noncanonical {label}")
    return values


def _parse_manifest(payload: bytes) -> PersonalDevSourceManifestV1:
    try:
        value = json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PersonalDevSourceError("personal-dev source manifest is not canonical JSON") from exc
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "attestation_scope",
        "source_commit",
        "dirty",
        "worktree_state_sha256",
        "contexts",
        "files",
        "deleted_tracked_paths",
        "excluded_sensitive_paths",
        "file_count",
        "total_bytes",
    }:
        raise PersonalDevSourceError("personal-dev source manifest has an invalid shape")
    if value["schema_version"] != 1 or value["attestation_scope"] != "personal-dev-only":
        raise PersonalDevSourceError("personal-dev source manifest has invalid authority scope")
    source_commit = _exact_string(value["source_commit"], label="source commit")
    worktree_digest = _exact_string(value["worktree_state_sha256"], label="worktree digest")
    if len(source_commit) != 40 or any(
        character not in "0123456789abcdef" for character in source_commit
    ):
        raise PersonalDevSourceError("personal-dev source manifest has invalid source commit")
    if len(worktree_digest) != 64 or any(
        character not in "0123456789abcdef" for character in worktree_digest
    ):
        raise PersonalDevSourceError("personal-dev source manifest has invalid worktree digest")
    if type(value["dirty"]) is not bool:
        raise PersonalDevSourceError("personal-dev source manifest has invalid dirty state")
    raw_contexts = value["contexts"]
    if not isinstance(raw_contexts, list) or any(
        not isinstance(item, str) for item in raw_contexts
    ):
        raise PersonalDevSourceError("personal-dev source manifest has invalid contexts")
    contexts = _normalize_contexts(tuple(raw_contexts))
    deleted = _string_tuple(value["deleted_tracked_paths"], label="deleted paths")
    excluded = _string_tuple(value["excluded_sensitive_paths"], label="excluded paths")
    for path in (*deleted, *excluded):
        _validate_relative_path(path, label="personal-dev source manifest")
    if any(not _is_sensitive(path) for path in excluded):
        raise PersonalDevSourceError("personal-dev source manifest has an invalid exclusion")
    raw_files = value["files"]
    if not isinstance(raw_files, list):
        raise PersonalDevSourceError("personal-dev source manifest has invalid files")
    files: list[PersonalDevSourceFileV1] = []
    for raw_file in raw_files:
        if not isinstance(raw_file, dict) or set(raw_file) != {"path", "mode", "size", "sha256"}:
            raise PersonalDevSourceError("personal-dev source manifest has an invalid file entry")
        path = _exact_string(raw_file["path"], label="file path")
        _validate_relative_path(path, label="personal-dev source manifest")
        if _is_sensitive(path):
            raise PersonalDevSourceError("personal-dev source manifest includes a sensitive path")
        mode = _exact_int(raw_file["mode"], label="file mode")
        size = _exact_int(raw_file["size"], label="file size")
        digest = _exact_string(raw_file["sha256"], label="file digest")
        if (
            mode not in {0o644, 0o755}
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise PersonalDevSourceError("personal-dev source manifest has an invalid file entry")
        files.append(PersonalDevSourceFileV1(path=path, mode=mode, size=size, sha256=digest))
    if tuple(item.path for item in files) != tuple(sorted(item.path for item in files)) or len(
        {item.path for item in files}
    ) != len(files):
        raise PersonalDevSourceError("personal-dev source manifest file order is not canonical")
    path_sets = ({item.path for item in files}, set(deleted), set(excluded))
    if any(path_sets[left] & path_sets[right] for left, right in ((0, 1), (0, 2), (1, 2))):
        raise PersonalDevSourceError("personal-dev source manifest path classes overlap")
    file_count = _exact_int(value["file_count"], label="file count")
    total_bytes = _exact_int(value["total_bytes"], label="total bytes")
    if file_count != len(files) or total_bytes != sum(item.size for item in files):
        raise PersonalDevSourceError("personal-dev source manifest totals do not match its files")
    manifest = PersonalDevSourceManifestV1(
        schema_version=1,
        attestation_scope="personal-dev-only",
        source_commit=source_commit,
        dirty=value["dirty"],
        worktree_state_sha256=worktree_digest,
        contexts=contexts,
        files=tuple(files),
        deleted_tracked_paths=deleted,
        excluded_sensitive_paths=excluded,
        file_count=file_count,
        total_bytes=total_bytes,
    )
    if _manifest_bytes(manifest) != payload:
        raise PersonalDevSourceError("personal-dev source manifest is not canonically encoded")
    return manifest


def _scan(
    root: Path,
    root_fd: int,
    contexts: tuple[str, ...],
    *,
    max_files: int,
    max_total_bytes: int,
    max_file_bytes: int,
) -> _Scan:
    _reject_unignored_special_files(root, contexts)
    try:
        source_commit = _git(root, "rev-parse", "--verify", "HEAD").decode("ascii").strip()
    except UnicodeDecodeError as exc:  # pragma: no cover - Git SHA is ASCII
        raise PersonalDevSourceError("Git returned an invalid source commit") from exc
    if len(source_commit) != 40 or any(
        character not in "0123456789abcdef" for character in source_commit
    ):
        raise PersonalDevSourceError("source repository HEAD is not a full Git commit")
    status_payload = _git(root, "status", "--porcelain=v1", "-z", "--untracked-files=all")
    paths = _candidate_paths(root, contexts)
    tracked_paths = _tracked_paths(root, contexts)
    files: list[PersonalDevSourceFileV1] = []
    deleted: list[str] = []
    excluded: list[str] = []
    total_bytes = 0
    for path in paths:
        if _is_sensitive(path):
            excluded.append(path)
            continue
        try:
            entry, _data = _read_regular_file(root_fd, path, max_file_bytes=max_file_bytes)
        except FileNotFoundError as exc:
            if path not in tracked_paths:
                raise PersonalDevSourceError(
                    "source checkout changed while it was being sealed"
                ) from exc
            deleted.append(path)
            continue
        files.append(entry)
        if len(files) > max_files:
            raise PersonalDevSourceError("personal-dev source exceeds the file-count limit")
        total_bytes += entry.size
        if total_bytes > max_total_bytes:
            raise PersonalDevSourceError("personal-dev source exceeds the aggregate byte limit")
    manifest = PersonalDevSourceManifestV1(
        schema_version=1,
        attestation_scope="personal-dev-only",
        source_commit=source_commit,
        dirty=bool(status_payload),
        worktree_state_sha256=hashlib.sha256(status_payload).hexdigest(),
        contexts=contexts,
        files=tuple(files),
        deleted_tracked_paths=tuple(deleted),
        excluded_sensitive_paths=tuple(excluded),
        file_count=len(files),
        total_bytes=total_bytes,
    )
    return _Scan(
        manifest=manifest,
        manifest_bytes=_manifest_bytes(manifest),
        candidate_paths=paths,
    )


def _tar_info(name: str, *, size: int, mode: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name)
    info.size = size
    info.mode = mode
    info.mtime = 0
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    return info


def _write_archive(
    raw: io.BufferedRandom,
    root_fd: int,
    expected: _Scan,
    *,
    max_file_bytes: int,
) -> str:
    with tarfile.open(
        fileobj=raw,
        mode="w",
        format=tarfile.USTAR_FORMAT,
    ) as archive:
        archive.addfile(
            _tar_info("SOURCE-MANIFEST.json", size=len(expected.manifest_bytes), mode=0o600),
            io.BytesIO(expected.manifest_bytes),
        )
        for expected_entry in expected.manifest.files:
            try:
                entry, data = _read_regular_file(
                    root_fd,
                    expected_entry.path,
                    max_file_bytes=max_file_bytes,
                )
            except FileNotFoundError as exc:
                raise PersonalDevSourceError(
                    "source checkout changed while it was being sealed"
                ) from exc
            if entry != expected_entry:
                raise PersonalDevSourceError("source checkout changed while it was being sealed")
            archive.addfile(
                _tar_info(entry.path, size=entry.size, mode=entry.mode),
                io.BytesIO(data),
            )
    raw.flush()
    os.fsync(raw.fileno())
    raw.seek(0)
    digest = hashlib.sha256()
    while chunk := raw.read(1024 * 1024):
        digest.update(chunk)
    return digest.hexdigest()


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def create_personal_dev_source_snapshot(
    source_root: Path,
    output_path: Path,
    *,
    contexts: tuple[str, ...] = (".",),
    max_files: int = _DEFAULT_MAX_FILES,
    max_total_bytes: int = _DEFAULT_MAX_TOTAL_BYTES,
    max_file_bytes: int = _DEFAULT_MAX_FILE_BYTES,
    _between_passes: Callable[[], None] | None = None,
) -> PersonalDevSourceSnapshotV1:
    """Seal one stable checkout into a deterministic personal-dev-only tar.

    The private hook exists solely to make the between-pass race test
    deterministic; production call sites leave it unset.
    """

    if any(
        type(value) is not int or value <= 0
        for value in (max_files, max_total_bytes, max_file_bytes)
    ):
        raise ValueError("personal-dev source limits must be positive integers")
    try:
        root = source_root.resolve(strict=True)
    except OSError as exc:
        raise PersonalDevSourceError("source repository is unavailable") from exc
    if not root.is_dir():
        raise PersonalDevSourceError("source repository is not a directory")
    if not output_path.name or output_path.name in {".", ".."}:
        raise PersonalDevSourceError("snapshot output must name one archive file")
    output_parent = output_path.absolute().parent.resolve(strict=False)
    output = output_parent / output_path.name
    if _is_within(output, root):
        raise PersonalDevSourceError("snapshot output must be outside the source repository")
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        repository_root = Path(
            _git(root, "rev-parse", "--show-toplevel").decode("utf-8", errors="strict").strip()
        ).resolve(strict=True)
    except (OSError, UnicodeDecodeError) as exc:
        raise PersonalDevSourceError("source root is not a readable Git repository") from exc
    if repository_root != root:
        raise PersonalDevSourceError("source root must be the exact Git worktree root")
    normalized_contexts = _normalize_contexts(contexts)
    try:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except OSError as exc:
        raise PersonalDevSourceError("source repository failed no-follow admission") from exc
    temporary: Path | None = None
    try:
        _validate_context_directories(root_fd, normalized_contexts)
        initial = _scan(
            root,
            root_fd,
            normalized_contexts,
            max_files=max_files,
            max_total_bytes=max_total_bytes,
            max_file_bytes=max_file_bytes,
        )
        if _between_passes is not None:
            _between_passes()
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{output.name}.tmp-",
            dir=output.parent,
        )
        temporary = Path(temporary_name)
        with os.fdopen(descriptor, "w+b") as raw:
            os.fchmod(raw.fileno(), 0o600)
            archive_sha256 = _write_archive(
                raw,
                root_fd,
                initial,
                max_file_bytes=max_file_bytes,
            )
            final = _scan(
                root,
                root_fd,
                normalized_contexts,
                max_files=max_files,
                max_total_bytes=max_total_bytes,
                max_file_bytes=max_file_bytes,
            )
            if final != initial:
                raise PersonalDevSourceError("source checkout changed while it was being sealed")
            path_identity = os.stat(temporary, follow_symlinks=False)
            descriptor_identity = os.fstat(raw.fileno())
            if (path_identity.st_dev, path_identity.st_ino) != (
                descriptor_identity.st_dev,
                descriptor_identity.st_ino,
            ):
                raise PersonalDevSourceError("snapshot output changed before publication")
            os.replace(temporary, output)
        temporary = None
        return PersonalDevSourceSnapshotV1(
            manifest=initial.manifest,
            source_digest=hashlib.sha256(initial.manifest_bytes).hexdigest(),
            archive_sha256=archive_sha256,
        )
    finally:
        os.close(root_fd)
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def verify_personal_dev_source_snapshot(
    archive_path: Path,
    *,
    expected_source_digest: str,
    expected_archive_sha256: str,
    max_files: int = _DEFAULT_MAX_FILES,
    max_total_bytes: int = _DEFAULT_MAX_TOTAL_BYTES,
    max_file_bytes: int = _DEFAULT_MAX_FILE_BYTES,
) -> PersonalDevSourceManifestV1:
    """Verify a sealed archive without extracting or following any member path."""

    for label, digest in (
        ("source", expected_source_digest),
        ("archive", expected_archive_sha256),
    ):
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise PersonalDevSourceError(f"expected {label} digest is invalid")
    if any(
        type(value) is not int or value <= 0
        for value in (max_files, max_total_bytes, max_file_bytes)
    ):
        raise ValueError("personal-dev source limits must be positive integers")
    try:
        descriptor = os.open(
            archive_path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
    except OSError as exc:
        raise PersonalDevSourceError("personal-dev source archive is unavailable") from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
            raise PersonalDevSourceError("personal-dev source archive is not a single-link file")
        if before.st_size == 0 or before.st_size % tarfile.RECORDSIZE != 0:
            raise PersonalDevSourceError("personal-dev source archive is not canonical tar")
        max_archive_bytes = max_total_bytes + 16 * 1024 * 1024 + (max_files + 2) * 1024 + 10_240
        if before.st_size > max_archive_bytes:
            raise PersonalDevSourceError("personal-dev source archive exceeds the byte limit")
        archive_digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            archive_digest.update(chunk)
        after_hash = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
            before.st_ctime_ns,
        ) != (
            after_hash.st_dev,
            after_hash.st_ino,
            after_hash.st_size,
            after_hash.st_mtime_ns,
            after_hash.st_ctime_ns,
        ):
            raise PersonalDevSourceError("personal-dev source archive changed during verification")
        if archive_digest.hexdigest() != expected_archive_sha256:
            raise PersonalDevSourceError("personal-dev source archive digest does not match")
        os.lseek(descriptor, 0, os.SEEK_SET)
        with (
            os.fdopen(os.dup(descriptor), "rb") as raw,
            tarfile.open(fileobj=raw, mode="r:") as archive,
        ):
            manifest_member = archive.next()
            if manifest_member is None or manifest_member.name != "SOURCE-MANIFEST.json":
                raise PersonalDevSourceError("personal-dev source archive is missing its manifest")
            if (
                not manifest_member.isfile()
                or manifest_member.mtime != 0
                or manifest_member.uid != 0
                or manifest_member.gid != 0
                or manifest_member.uname != ""
                or manifest_member.gname != ""
                or manifest_member.pax_headers
                or manifest_member.mode != 0o600
                or manifest_member.offset != 0
                or manifest_member.offset_data != tarfile.BLOCKSIZE
            ):
                raise PersonalDevSourceError(
                    "personal-dev source archive has noncanonical member metadata"
                )
            if manifest_member.size > 16 * 1024 * 1024:
                raise PersonalDevSourceError("personal-dev source manifest is oversized")
            manifest_file = archive.extractfile(manifest_member)
            if manifest_file is None:  # pragma: no cover - member.isfile checked
                raise PersonalDevSourceError("personal-dev source archive manifest is unreadable")
            manifest_payload = manifest_file.read(16 * 1024 * 1024 + 1)
            manifest = _parse_manifest(manifest_payload)
            if hashlib.sha256(manifest_payload).hexdigest() != expected_source_digest:
                raise PersonalDevSourceError("personal-dev source manifest digest does not match")
            if manifest.file_count > max_files or manifest.total_bytes > max_total_bytes:
                raise PersonalDevSourceError("personal-dev source manifest exceeds verifier limits")
            for expected in manifest.files:
                member = archive.next()
                if member is None or member.name != expected.path:
                    raise PersonalDevSourceError(
                        "personal-dev source archive members differ from the manifest"
                    )
                if (
                    not member.isfile()
                    or member.mtime != 0
                    or member.uid != 0
                    or member.gid != 0
                    or member.uname != ""
                    or member.gname != ""
                    or member.pax_headers
                    or member.offset_data != member.offset + tarfile.BLOCKSIZE
                    or member.mode != expected.mode
                    or member.size != expected.size
                    or member.size > max_file_bytes
                ):
                    raise PersonalDevSourceError(
                        "personal-dev source archive member metadata differs from the manifest"
                    )
                member_file = archive.extractfile(member)
                if member_file is None:  # pragma: no cover - member.isfile checked
                    raise PersonalDevSourceError("personal-dev source archive member is unreadable")
                member_digest = hashlib.sha256()
                observed = 0
                while chunk := member_file.read(min(1024 * 1024, max_file_bytes + 1 - observed)):
                    observed += len(chunk)
                    if observed > max_file_bytes:
                        raise PersonalDevSourceError(
                            "personal-dev source archive member exceeds the byte limit"
                        )
                    member_digest.update(chunk)
                if observed != expected.size or member_digest.hexdigest() != expected.sha256:
                    raise PersonalDevSourceError(
                        "personal-dev source archive content differs from the manifest"
                    )
            if archive.next() is not None:
                raise PersonalDevSourceError(
                    "personal-dev source archive members differ from the manifest"
                )
        after_parse = os.fstat(descriptor)
        if (after_hash.st_size, after_hash.st_mtime_ns, after_hash.st_ctime_ns) != (
            after_parse.st_size,
            after_parse.st_mtime_ns,
            after_parse.st_ctime_ns,
        ):
            raise PersonalDevSourceError("personal-dev source archive changed during verification")
        return manifest
    except tarfile.TarError as exc:
        raise PersonalDevSourceError("personal-dev source archive is not canonical tar") from exc
    finally:
        os.close(descriptor)


__all__ = [
    "PersonalDevSourceError",
    "PersonalDevSourceFileV1",
    "PersonalDevSourceManifestV1",
    "PersonalDevSourceSnapshotV1",
    "create_personal_dev_source_snapshot",
    "verify_personal_dev_source_snapshot",
]
