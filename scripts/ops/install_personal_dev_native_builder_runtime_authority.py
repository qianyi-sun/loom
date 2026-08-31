#!/usr/bin/python3 -I
"""Bootstrap the fixed sealed personal native-builder runtime authority."""

from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import importlib.abc
import importlib.util
import json
import os
import re
import stat
import subprocess
import sys
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import ModuleType
from typing import cast

SOURCE_ROOT = Path("/opt/loom-personal-dev-native-builder-runtime-authority/source")
APPROVED_BASE_SHA = "22393a30e073276cc42add493061c1ab0c67674b"
HOST_ROOT = Path("/")
ROOT_UID = 0
ROOT_GID = 0

_INSTALLER_RELATIVE = Path(
    "scripts/ops/install_personal_dev_native_builder_runtime_authority.py"
)
_VALIDATOR_RELATIVE = Path("scripts/ops/staging_rollout_sealed_source.py")
_LAUNCHER_RELATIVE = Path(
    "scripts/ops/personal_dev_native_builder_runtime_authority_launcher.py"
)
_SUDOERS_RELATIVE = Path(
    "deploy/personal-dev-native-builder/"
    "loom-personal-dev-native-builder-runtime-authority.sudoers"
)
_TMPFILES_RELATIVE = Path(
    "deploy/personal-dev-native-builder/"
    "loom-personal-dev-native-builder-runtime-authority.tmpfiles"
)
_SUDOERS_PAYLOAD = (
    b'qianyi ALL=(root) NOPASSWD:NOSETENV: '
    b'/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority ""\n'
)
_TMPFILES_PAYLOAD = (
    b"d /var/lib/loom/personal-dev-native-builder-runtime-authority 0700 root root -\n"
    b"f /run/lock/loom-personal-dev-native-builder-runtime-authority.lock "
    b"0600 root root -\n"
    b"d /run/loom-personal-dev-native-builder-runtime-authority 0700 root root -\n"
)
_HEX_40 = re.compile(r"[0-9a-f]{40}")
_MAX_ASSET_BYTES = 8 * 1024 * 1024
_MAX_RECEIPT_BYTES = 64 * 1024
_RENAME_NOREPLACE = 1
_AT_FDCWD = -100
_UNSAFE_ENVIRONMENT_NAMES = frozenset({"BASH_ENV", "CDPATH", "ENV", "IFS"})
_UNSAFE_ENVIRONMENT_PREFIXES = (
    "DOCKER_",
    "GIT_",
    "LD_",
    "NFTABLES_",
    "PYTHON",
    "SUDO_",
    "SYSTEMD_",
)
ROOT_ENVIRONMENT = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/sbin:/usr/bin:/sbin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}


class BootstrapError(RuntimeError):
    """The authority could not be published without weakening its boundary."""

    def __init__(self, code: str) -> None:
        self.code = code if re.fullmatch(r"[a-z][a-z0-9_]{0,63}", code) else "bootstrap_failed"
        super().__init__(self.code)


CommandRunner = Callable[
    [Sequence[str], Mapping[str, str]], subprocess.CompletedProcess[str]
]


def _run(
    argv: Sequence[str], environment: Mapping[str, str]
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        check=False,
        capture_output=True,
        text=True,
        env=dict(environment),
    )


_COMMAND_RUNNER: CommandRunner = _run


@dataclass(frozen=True, slots=True)
class _SourceAsset:
    name: str
    source_relative: Path
    installed_path: Path
    mode: int
    payload: bytes
    sha256: str


def _identity(metadata: os.stat_result) -> tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_nlink,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_size,
        metadata.st_mtime_ns,
        metadata.st_ctime_ns,
    )


def _read_all(descriptor: int, maximum: int = _MAX_ASSET_BYTES) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = os.read(descriptor, min(64 * 1024, maximum + 1 - total))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        total += len(chunk)
        if total > maximum:
            raise BootstrapError("source_asset_invalid")


def _read_source_file(path: Path, *, error: str = "source_asset_invalid") -> bytes:
    descriptor: int | None = None
    try:
        lexical_before = os.lstat(path)
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(lexical_before.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or stat.S_IMODE(before.st_mode) != 0o644
            or before.st_nlink != 1
            or before.st_uid != ROOT_UID
            or before.st_gid != ROOT_GID
            or (before.st_dev, before.st_ino)
            != (lexical_before.st_dev, lexical_before.st_ino)
            or not 0 < before.st_size <= _MAX_ASSET_BYTES
        ):
            raise BootstrapError(error)
        payload = _read_all(descriptor)
        after = os.fstat(descriptor)
        lexical_after = os.lstat(path)
        if _identity(before) != _identity(after) or (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_nlink,
            after.st_uid,
            after.st_gid,
            after.st_size,
        ) != (
            lexical_after.st_dev,
            lexical_after.st_ino,
            lexical_after.st_mode,
            lexical_after.st_nlink,
            lexical_after.st_uid,
            lexical_after.st_gid,
            lexical_after.st_size,
        ):
            raise BootstrapError(error)
        return payload
    except BootstrapError:
        raise
    except OSError as exc:
        raise BootstrapError(error) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


class _BytesLoader(importlib.abc.Loader):
    def __init__(self, fullname: str, filename: Path, payload: bytes) -> None:
        self.fullname = fullname
        self.filename = filename
        self.payload = payload

    def create_module(self, spec: importlib.machinery.ModuleSpec) -> None:
        del spec
        return None

    def exec_module(self, module: ModuleType) -> None:
        code = compile(self.payload, str(self.filename), "exec", dont_inherit=True)
        exec(code, module.__dict__)


class _CapturedFinder(importlib.abc.MetaPathFinder):
    def __init__(self, modules: Mapping[str, tuple[Path, bytes]]) -> None:
        self.modules = modules

    def find_spec(
        self,
        fullname: str,
        path: Sequence[str] | None,
        target: ModuleType | None = None,
    ) -> importlib.machinery.ModuleSpec | None:
        del path, target
        captured = self.modules.get(fullname)
        if captured is None:
            return None
        filename, payload = captured
        return importlib.util.spec_from_loader(
            fullname,
            _BytesLoader(fullname, filename, payload),
            origin=str(filename),
        )


def _load_bytes(name: str, path: Path, payload: bytes) -> ModuleType:
    specification = importlib.util.spec_from_loader(
        name,
        _BytesLoader(name, path, payload),
        origin=str(path),
    )
    if specification is None or specification.loader is None:
        raise BootstrapError("sealed_source_invalid")
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    specification.loader.exec_module(module)
    return module


def _safe_source_root() -> None:
    try:
        metadata = os.lstat(SOURCE_ROOT)
    except OSError as exc:
        raise BootstrapError("sealed_source_invalid") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != ROOT_UID
        or metadata.st_gid != ROOT_GID
        or stat.S_IMODE(metadata.st_mode) != 0o700
    ):
        raise BootstrapError("sealed_source_invalid")
    expected = SOURCE_ROOT / _INSTALLER_RELATIVE
    if Path(os.path.abspath(__file__)) != expected:
        raise BootstrapError("sealed_source_invalid")
    _read_source_file(expected, error="sealed_source_invalid")


def _load_validator() -> ModuleType:
    path = SOURCE_ROOT / _VALIDATOR_RELATIVE
    payload = _read_source_file(path, error="sealed_source_invalid")
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        return _load_bytes("_loom_native_authority_sealed_source", path, payload)
    except BaseException as exc:
        if isinstance(exc, BootstrapError):
            raise
        raise BootstrapError("sealed_source_invalid") from exc
    finally:
        sys.dont_write_bytecode = previous


def _validate_sealed_source(
    validator: ModuleType,
    source_sha: str,
    source_tree_sha: str,
) -> None:
    try:
        source = validator.SealedSource(
            SOURCE_ROOT,
            source_sha,
            source_tree_sha,
            APPROVED_BASE_SHA,
        )
        validator.validate_sealed_source(
            source,
            expected_uid=ROOT_UID,
            expected_gid=ROOT_GID,
        )
    except BaseException as exc:
        raise BootstrapError("sealed_source_invalid") from exc


def _source_relative(
    name: str,
    specification: object,
    launcher: ModuleType,
) -> Path:
    path = getattr(specification, "path", None)
    mode = getattr(specification, "mode", None)
    if not isinstance(path, Path) or not path.is_absolute() or not isinstance(mode, int):
        raise BootstrapError("source_inventory_invalid")
    if name == "launcher":
        if path != launcher.LIBEXEC_PATH or mode != 0o555:
            raise BootstrapError("source_inventory_invalid")
        return _LAUNCHER_RELATIVE
    if name == "sudoers":
        if path != launcher.ASSET_SPECS["sudoers"].path or mode != 0o440:
            raise BootstrapError("source_inventory_invalid")
        return _SUDOERS_RELATIVE
    if name == "tmpfiles":
        if path != launcher.ASSET_SPECS["tmpfiles"].path or mode != 0o444:
            raise BootstrapError("source_inventory_invalid")
        return _TMPFILES_RELATIVE
    try:
        relative = path.relative_to(launcher.LIBRARY_ROOT)
    except ValueError as exc:
        raise BootstrapError("source_inventory_invalid") from exc
    pure = PurePosixPath(relative.as_posix())
    if (
        not relative.parts
        or pure.is_absolute()
        or any(part in {"", ".", ".."} for part in pure.parts)
        or mode != 0o444
    ):
        raise BootstrapError("source_inventory_invalid")
    if name == "broker" and path != launcher.BROKER_PATH:
        raise BootstrapError("source_inventory_invalid")
    return relative


def _capture_inventory(
    launcher: ModuleType,
    launcher_payload: bytes,
) -> tuple[_SourceAsset, ...]:
    specifications = getattr(launcher, "ASSET_SPECS", None)
    if not isinstance(specifications, Mapping) or not specifications:
        raise BootstrapError("source_inventory_invalid")
    assets: list[_SourceAsset] = []
    sources: set[Path] = set()
    destinations: set[Path] = set()
    for name, specification in specifications.items():
        if not isinstance(name, str) or not name:
            raise BootstrapError("source_inventory_invalid")
        relative = _source_relative(name, specification, launcher)
        installed_path = specification.path
        mode = specification.mode
        if relative in sources or installed_path in destinations:
            raise BootstrapError("source_inventory_invalid")
        payload = (
            launcher_payload
            if name == "launcher"
            else _read_source_file(SOURCE_ROOT / relative)
        )
        sources.add(relative)
        destinations.add(installed_path)
        assets.append(
            _SourceAsset(
                name=name,
                source_relative=relative,
                installed_path=installed_path,
                mode=mode,
                payload=payload,
                sha256=hashlib.sha256(payload).hexdigest(),
            )
        )
    by_name = {asset.name: asset for asset in assets}
    if set(by_name) != set(specifications) or by_name["broker"].installed_path != launcher.BROKER_PATH:
        raise BootstrapError("source_inventory_invalid")
    if by_name["sudoers"].payload != _SUDOERS_PAYLOAD:
        raise BootstrapError("source_inventory_invalid")
    if by_name["tmpfiles"].payload != _TMPFILES_PAYLOAD:
        raise BootstrapError("source_inventory_invalid")
    return tuple(assets)


@contextmanager
def _pinned_source_contract() -> Iterator[tuple[ModuleType, ModuleType, tuple[_SourceAsset, ...]]]:
    saved = {
        name: module
        for name, module in tuple(sys.modules.items())
        if name == "scripts" or name.startswith("scripts.")
    }
    for name in saved:
        sys.modules.pop(name, None)
    scripts_package = ModuleType("scripts")
    scripts_package.__package__ = "scripts"
    scripts_package.__path__ = []
    ops_package = ModuleType("scripts.ops")
    ops_package.__package__ = "scripts.ops"
    ops_package.__path__ = []
    scripts_package.__dict__["ops"] = ops_package
    sys.modules["scripts"] = scripts_package
    sys.modules["scripts.ops"] = ops_package
    previous_bytecode = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    finder: _CapturedFinder | None = None
    try:
        launcher_path = SOURCE_ROOT / _LAUNCHER_RELATIVE
        launcher_payload = _read_source_file(launcher_path)
        launcher = _load_bytes(
            "scripts.ops.personal_dev_native_builder_runtime_authority_launcher",
            launcher_path,
            launcher_payload,
        )
        assets = _capture_inventory(launcher, launcher_payload)
        modules = {
            f"scripts.ops.{asset.source_relative.stem}": (
                SOURCE_ROOT / asset.source_relative,
                asset.payload,
            )
            for asset in assets
            if asset.source_relative.parts[:2] == ("scripts", "ops")
        }
        finder = _CapturedFinder(modules)
        sys.meta_path.insert(0, finder)
        broker_name = "scripts.ops.personal_dev_native_builder_runtime_authority"
        broker_path, broker_payload = modules[broker_name]
        broker = _load_bytes(broker_name, broker_path, broker_payload)
        yield launcher, broker, assets
    except BootstrapError:
        raise
    except BaseException as exc:
        raise BootstrapError("source_inventory_invalid") from exc
    finally:
        if finder is not None and finder in sys.meta_path:
            sys.meta_path.remove(finder)
        for name in tuple(sys.modules):
            if name == "scripts" or name.startswith("scripts."):
                sys.modules.pop(name, None)
        sys.modules.update(saved)
        sys.dont_write_bytecode = previous_bytecode


def _host_path(path: Path) -> Path:
    if not path.is_absolute() or ".." in path.parts:
        raise BootstrapError("source_inventory_invalid")
    if HOST_ROOT == Path("/"):
        return path
    return HOST_ROOT.joinpath(*path.parts[1:])


def _safe_directory(path: Path, *, exact_mode: int | None = None) -> None:
    try:
        metadata = os.lstat(path)
    except OSError as exc:
        raise BootstrapError("installed_drift") from exc
    mode = stat.S_IMODE(metadata.st_mode)
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or stat.S_ISLNK(metadata.st_mode)
        or metadata.st_uid != ROOT_UID
        or metadata.st_gid != ROOT_GID
        or (exact_mode is not None and mode != exact_mode)
        or (exact_mode is None and mode & 0o002 and not mode & stat.S_ISVTX)
    ):
        raise BootstrapError("installed_drift")


def _fsync_parent(path: Path) -> None:
    descriptor = os.open(
        path.parent,
        os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _ensure_directory(path: Path, mode: int, created: list[Path]) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        _safe_directory(path.parent)
        try:
            os.mkdir(path, 0o700)
            created.append(path)
            os.chown(path, ROOT_UID, ROOT_GID)
            os.chmod(path, mode)
            _fsync_parent(path)
        except OSError as exc:
            raise BootstrapError("publication_failed") from exc
        return True
    _safe_directory(path, exact_mode=mode)
    return False


def _ensure_parents(path: Path, created: list[Path]) -> None:
    missing: list[Path] = []
    current = path
    while True:
        try:
            os.lstat(current)
            break
        except FileNotFoundError:
            missing.append(current)
            if current == HOST_ROOT or current.parent == current:
                raise BootstrapError("publication_failed") from None
            current = current.parent
    _safe_directory(current)
    for directory in reversed(missing):
        _ensure_directory(directory, 0o755, created)


def _read_installed_file(path: Path, mode: int) -> bytes | None:
    descriptor: int | None = None
    try:
        lexical = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise BootstrapError("installed_drift") from exc
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        before = os.fstat(descriptor)
        observed = _read_all(descriptor)
        after = os.fstat(descriptor)
        lexical_after = os.lstat(path)
        if (
            not stat.S_ISREG(lexical.st_mode)
            or not stat.S_ISREG(before.st_mode)
            or (lexical.st_dev, lexical.st_ino) != (before.st_dev, before.st_ino)
            or before.st_nlink != 1
            or before.st_uid != ROOT_UID
            or before.st_gid != ROOT_GID
            or stat.S_IMODE(before.st_mode) != mode
            or _identity(before) != _identity(after)
            or (after.st_dev, after.st_ino, after.st_mode, after.st_nlink)
            != (
                lexical_after.st_dev,
                lexical_after.st_ino,
                lexical_after.st_mode,
                lexical_after.st_nlink,
            )
        ):
            raise BootstrapError("installed_drift")
        return observed
    except OSError as exc:
        raise BootstrapError("installed_drift") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _existing_file(path: Path, payload: bytes, mode: int) -> bool:
    observed = _read_installed_file(path, mode)
    if observed is None:
        return False
    if observed != payload:
        raise BootstrapError("installed_drift")
    return True


def _write_all(descriptor: int, payload: bytes) -> None:
    remaining = memoryview(payload)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise BootstrapError("publication_failed")
        remaining = remaining[written:]


def _rename_noreplace(source: Path, destination: Path) -> None:
    library = ctypes.CDLL(None, use_errno=True)
    try:
        rename = library.renameat2
    except AttributeError as exc:
        raise BootstrapError("publication_failed") from exc
    rename.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    rename.restype = ctypes.c_int
    if (
        rename(
            _AT_FDCWD,
            os.fsencode(source),
            _AT_FDCWD,
            os.fsencode(destination),
            _RENAME_NOREPLACE,
        )
        == 0
    ):
        return
    error_number = ctypes.get_errno()
    if error_number in {errno.EEXIST, errno.ENOTEMPTY}:
        raise FileExistsError(error_number, "destination exists")
    raise BootstrapError("publication_failed")


def _install_file(
    path: Path,
    payload: bytes,
    mode: int,
    created_files: list[Path],
    created_directories: list[Path],
) -> bool:
    _ensure_parents(path.parent, created_directories)
    if _existing_file(path, payload, mode):
        return False
    temporary = path.parent / f".{path.name}.new-{os.getpid()}"
    descriptor: int | None = None
    temporary_created = False
    published = False
    try:
        descriptor = os.open(
            temporary,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        temporary_created = True
        _write_all(descriptor, payload)
        os.fchown(descriptor, ROOT_UID, ROOT_GID)
        os.fchmod(descriptor, mode)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        _rename_noreplace(temporary, path)
        published = True
        created_files.append(path)
        _fsync_parent(path)
        return True
    except FileExistsError as exc:
        raise BootstrapError("installed_drift") from exc
    except BootstrapError:
        raise
    except OSError as exc:
        raise BootstrapError("publication_failed") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if temporary_created and not published:
            try:
                os.unlink(temporary)
                _fsync_parent(temporary)
            except FileNotFoundError:
                pass


def _validate_sudoers_payload(payload: bytes) -> None:
    runtime_root = _host_path(Path("/run"))
    _safe_directory(runtime_root)
    staged = runtime_root / f".loom-native-authority-sudoers.validate-{os.getpid()}"
    descriptor: int | None = None
    staged_created = False
    try:
        descriptor = os.open(
            staged,
            os.O_WRONLY
            | os.O_CREAT
            | os.O_EXCL
            | os.O_CLOEXEC
            | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        staged_created = True
        _write_all(descriptor, payload)
        os.fchown(descriptor, ROOT_UID, ROOT_GID)
        os.fchmod(descriptor, 0o440)
        os.fsync(descriptor)
        os.close(descriptor)
        descriptor = None
        _fsync_parent(staged)
        result = _COMMAND_RUNNER(
            ("/usr/sbin/visudo", "-cf", str(staged)),
            ROOT_ENVIRONMENT,
        )
        if result.returncode != 0:
            raise BootstrapError("sudoers_invalid")
    except BootstrapError:
        raise
    except OSError as exc:
        raise BootstrapError("sudoers_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if staged_created:
            try:
                os.unlink(staged)
                _fsync_parent(staged)
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise BootstrapError("sudoers_invalid") from exc


def _validate_installed_sudoers(path: Path) -> None:
    result = _COMMAND_RUNNER(
        ("/usr/sbin/visudo", "-cf", str(path)),
        ROOT_ENVIRONMENT,
    )
    if result.returncode != 0:
        raise BootstrapError("sudoers_invalid")


def _validate_installed_policy(
    launcher_asset: _SourceAsset,
    policy_path: Path,
    assets: Sequence[_SourceAsset],
) -> None:
    launcher_path = _host_path(launcher_asset.installed_path)
    installed_payload = _read_installed_file(launcher_path, launcher_asset.mode)
    if installed_payload != launcher_asset.payload:
        raise BootstrapError("installed_validation_failed")
    module_name = f"_loom_installed_native_authority_launcher_{os.getpid()}"
    try:
        module = _load_bytes(module_name, launcher_path, installed_payload)
        specifications = {
            asset.name: module.AssetSpec(_host_path(asset.installed_path), asset.mode)
            for asset in assets
        }
        module.load_policy(
            policy_path=policy_path,
            asset_specs=specifications,
            expected_uid=ROOT_UID,
            expected_gid=ROOT_GID,
        )
    except BaseException as exc:
        raise BootstrapError("installed_validation_failed") from exc
    finally:
        sys.modules.pop(module_name, None)


def _rollback(created_files: Sequence[Path], created_directories: Sequence[Path]) -> None:
    failed = False
    for path in reversed(created_files):
        try:
            os.unlink(path)
            _fsync_parent(path)
        except OSError:
            failed = True
    for path in reversed(created_directories):
        try:
            os.rmdir(path)
            _fsync_parent(path)
        except OSError:
            failed = True
    if failed:
        raise BootstrapError("rollback_failed")


def _validate_direct_root() -> None:
    unsafe = any(
        name in _UNSAFE_ENVIRONMENT_NAMES
        or name.startswith(_UNSAFE_ENVIRONMENT_PREFIXES)
        for name in os.environ
    )
    if (
        os.getresuid() != (0, 0, 0)
        or os.getresgid() != (0, 0, 0)
        or unsafe
    ):
        raise BootstrapError("direct_root_required")


def _canonical_receipt(value: Mapping[str, object]) -> bytes:
    try:
        payload = (
            json.dumps(
                dict(value),
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise BootstrapError("receipt_invalid") from exc
    if len(payload) > _MAX_RECEIPT_BYTES:
        raise BootstrapError("receipt_invalid")
    return payload


def bootstrap(source_sha: str, source_tree_sha: str) -> dict[str, object]:
    """Install one validated sealed authority without publishing partial privilege."""
    _validate_direct_root()
    if (
        not isinstance(source_sha, str)
        or _HEX_40.fullmatch(source_sha) is None
        or not isinstance(source_tree_sha, str)
        or _HEX_40.fullmatch(source_tree_sha) is None
        or source_sha == source_tree_sha
    ):
        raise BootstrapError("sealed_source_invalid")
    _safe_source_root()
    validator = _load_validator()
    _validate_sealed_source(validator, source_sha, source_tree_sha)

    with _pinned_source_contract() as (launcher, broker, assets):
        digests = {asset.name: asset.sha256 for asset in assets}
        profile_digest = digests.get("runtime_asset_profile")
        if profile_digest is None:
            raise BootstrapError("source_inventory_invalid")
        try:
            policy = broker.AuthorityPolicy(
                authority_source_sha=source_sha,
                authority_source_tree=source_tree_sha,
                runtime_profile_sha256=profile_digest,
                asset_sha256=digests,
            )
            policy_payload = cast(bytes, broker.encode_policy(policy))
            launcher_asset = next(asset for asset in assets if asset.name == "launcher")
            sudoers_asset = next(asset for asset in assets if asset.name == "sudoers")
        except BaseException as exc:
            raise BootstrapError("source_inventory_invalid") from exc

    _validate_sudoers_payload(sudoers_asset.payload)
    created_files: list[Path] = []
    created_directories: list[Path] = []
    changed = False
    try:
        for asset in assets:
            if asset.name == "sudoers":
                continue
            changed = (
                _install_file(
                    _host_path(asset.installed_path),
                    asset.payload,
                    asset.mode,
                    created_files,
                    created_directories,
                )
                or changed
            )
        policy_path = _host_path(launcher.POLICY_PATH)
        changed = (
            _install_file(
                policy_path,
                policy_payload,
                0o444,
                created_files,
                created_directories,
            )
            or changed
        )
        state_path = _host_path(broker.STATE_PATH)
        if state_path.exists() or state_path.is_symlink():
            raise BootstrapError("installed_drift")
        for logical in (broker.STATE_ROOT, broker.EPHEMERAL_SECRET_ROOT):
            path = _host_path(logical)
            _ensure_parents(path.parent, created_directories)
            changed = _ensure_directory(path, 0o700, created_directories) or changed
        lock_path = _host_path(broker.LOCK_PATH)
        changed = (
            _install_file(
                lock_path,
                b"",
                0o600,
                created_files,
                created_directories,
            )
            or changed
        )
        sudoers_path = _host_path(sudoers_asset.installed_path)
        changed = (
            _install_file(
                sudoers_path,
                sudoers_asset.payload,
                sudoers_asset.mode,
                created_files,
                created_directories,
            )
            or changed
        )
        _validate_installed_policy(launcher_asset, policy_path, assets)
        _validate_installed_sudoers(sudoers_path)
    except BaseException as exc:
        try:
            _rollback(created_files, created_directories)
        except BootstrapError as rollback_error:
            raise rollback_error from exc
        if isinstance(exc, BootstrapError):
            raise
        raise BootstrapError("publication_failed") from exc

    receipt: dict[str, object] = {
        "asset_sha256": digests,
        "changed": changed,
        "policy_sha256": hashlib.sha256(policy_payload).hexdigest(),
        "runtime_profile_sha256": profile_digest,
        "source_base_sha": APPROVED_BASE_SHA,
        "source_sha": source_sha,
        "source_tree_sha": source_tree_sha,
        "status": "ok",
    }
    _canonical_receipt(receipt)
    return receipt


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--source-tree-sha", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        receipt = bootstrap(args.source_sha, args.source_tree_sha)
        payload = _canonical_receipt(receipt)
    except BaseException:
        sys.stderr.write("error:native_authority_bootstrap_failed\n")
        return 1
    sys.stdout.buffer.write(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = ["BootstrapError", "bootstrap", "main"]
