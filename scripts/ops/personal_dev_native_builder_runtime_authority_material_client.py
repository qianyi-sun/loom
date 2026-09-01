#!/usr/bin/python3 -I
"""Open fixed protected material and invoke the policy-bound FD-only client."""

from __future__ import annotations

import fcntl
import importlib.machinery
import importlib.util
import os
import stat
import sys
from collections.abc import Sequence
from pathlib import Path
from types import ModuleType
from typing import NoReturn

LAUNCHER_PATH = Path(
    "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority"
)
MATERIAL_CLIENT_PATH = Path(
    "/usr/local/libexec/"
    "loom-personal-dev-native-builder-runtime-authority-material-client"
)
LIBRARY_ROOT = Path(
    "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority"
)
CLIENT_PATH = (
    LIBRARY_ROOT
    / "scripts"
    / "ops"
    / "personal_dev_native_builder_runtime_authority_client.py"
)
PRIVATE_KEY_PATH = Path(
    "/etc/loom/personal-dev-native-builder-authority-material/agent-ed25519"
)
SERVICE_CA_PATH = Path(
    "/etc/loom/personal-dev-native-builder-authority-material/service-ca.pem"
)
ROOT_UID = 0
ROOT_GID = 0
_PRIVATE_KEY_BYTES = 32
_MAX_CA_BYTES = 1024 * 1024
_FAILURE = "native runtime authority material request failed\n"


class MaterialClientError(RuntimeError):
    """The fixed material boundary is invalid."""


def _load_module(name: str, path: Path) -> ModuleType:
    loader = importlib.machinery.SourceFileLoader(name, str(path))
    specification = importlib.util.spec_from_loader(name, loader)
    if specification is None or specification.loader is None:
        raise MaterialClientError("module invalid")
    module = importlib.util.module_from_spec(specification)
    sys.modules[name] = module
    try:
        specification.loader.exec_module(module)
    except BaseException:
        sys.modules.pop(name, None)
        raise
    return module


def _load_validated_client() -> ModuleType:
    launcher_name = (
        "scripts.ops.personal_dev_native_builder_runtime_authority_launcher"
    )
    launcher = _load_module(launcher_name, LAUNCHER_PATH)
    try:
        launcher.load_policy(expected_uid=ROOT_UID, expected_gid=ROOT_GID)
        if (
            launcher.ASSET_SPECS.get("material_client")
            != launcher.AssetSpec(MATERIAL_CLIENT_PATH, 0o555)
            or launcher.ASSET_SPECS.get("authority_client")
            != launcher.AssetSpec(CLIENT_PATH, 0o444)
        ):
            raise MaterialClientError("inventory invalid")
        launcher._pin_application_packages(LIBRARY_ROOT)
        sys.modules[launcher_name] = launcher
        client = _load_module(
            "scripts.ops.personal_dev_native_builder_runtime_authority_client",
            CLIENT_PATH,
        )
        if not callable(getattr(client, "main", None)):
            raise MaterialClientError("client invalid")
        return client
    except BaseException as exc:
        if isinstance(exc, MaterialClientError):
            raise
        raise MaterialClientError("policy invalid") from exc


def _directory_is_safe(metadata: os.stat_result) -> bool:
    mode = stat.S_IMODE(metadata.st_mode)
    owner_ok = metadata.st_uid in {0, ROOT_UID} and metadata.st_gid in {0, ROOT_GID}
    writable_ok = not mode & 0o022 or bool(mode & stat.S_ISVTX)
    return stat.S_ISDIR(metadata.st_mode) and owner_ok and writable_ok


def _open_fixed_file(
    path: Path,
    *,
    mode: int,
    minimum: int,
    maximum: int,
) -> int:
    if not path.is_absolute() or ".." in path.parts or minimum < 0 or maximum < minimum:
        raise MaterialClientError("material invalid")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    if not no_follow or not directory_only:
        raise MaterialClientError("material invalid")
    directory = os.open(
        "/",
        os.O_RDONLY | os.O_CLOEXEC | directory_only | no_follow,
    )
    descriptor: int | None = None
    try:
        if not _directory_is_safe(os.fstat(directory)):
            raise MaterialClientError("material invalid")
        for component in path.parent.parts[1:]:
            child = os.open(
                component,
                os.O_RDONLY | os.O_CLOEXEC | directory_only | no_follow,
                dir_fd=directory,
            )
            os.close(directory)
            directory = child
            if not _directory_is_safe(os.fstat(directory)):
                raise MaterialClientError("material invalid")
        lexical = os.stat(path.name, dir_fd=directory, follow_symlinks=False)
        descriptor = os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | no_follow,
            dir_fd=directory,
        )
        opened = os.fstat(descriptor)
        identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_nlink,
            opened.st_uid,
            opened.st_gid,
            opened.st_size,
        )
        if (
            not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) != mode
            or opened.st_nlink != 1
            or opened.st_uid != ROOT_UID
            or opened.st_gid != ROOT_GID
            or not minimum <= opened.st_size <= maximum
            or identity
            != (
                lexical.st_dev,
                lexical.st_ino,
                lexical.st_mode,
                lexical.st_nlink,
                lexical.st_uid,
                lexical.st_gid,
                lexical.st_size,
            )
        ):
            raise MaterialClientError("material invalid")
        inherited = fcntl.fcntl(descriptor, fcntl.F_DUPFD_CLOEXEC, 3)
        os.close(descriptor)
        descriptor = None
        return inherited
    except (MaterialClientError, OSError, ValueError) as exc:
        if isinstance(exc, MaterialClientError):
            raise
        raise MaterialClientError("material invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)
        os.close(directory)


def _reject_reserved_options(arguments: Sequence[str]) -> None:
    for argument in arguments:
        if argument.startswith(("--private-key-", "--service-ca-")):
            raise MaterialClientError("material option forbidden")


def _run(arguments: Sequence[str]) -> int:
    values = list(arguments)
    if not values or values[0] not in {"stage-agent", "emit-public-key"}:
        raise MaterialClientError("operation invalid")
    _reject_reserved_options(values[1:])
    client = _load_validated_client()
    key_fd: int | None = None
    ca_fd: int | None = None
    try:
        key_fd = _open_fixed_file(
            PRIVATE_KEY_PATH,
            mode=0o400,
            minimum=_PRIVATE_KEY_BYTES,
            maximum=_PRIVATE_KEY_BYTES,
        )
        values.extend(("--private-key-fd", str(key_fd)))
        if values[0] == "stage-agent":
            ca_fd = _open_fixed_file(
                SERVICE_CA_PATH,
                mode=0o444,
                minimum=1,
                maximum=_MAX_CA_BYTES,
            )
            if ca_fd == key_fd:
                raise MaterialClientError("descriptors invalid")
            values.extend(("--service-ca-fd", str(ca_fd)))
        result = client.main(values)
        if not isinstance(result, int):
            raise MaterialClientError("client invalid")
        return result
    finally:
        if ca_fd is not None:
            os.close(ca_fd)
        if key_fd is not None:
            os.close(key_fd)


def _verify_root() -> None:
    if os.getresuid() != (0, 0, 0) or os.getresgid() != (0, 0, 0):
        raise MaterialClientError("root required")


def _fail() -> NoReturn:
    sys.stderr.write(_FAILURE)
    raise SystemExit(2)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        _verify_root()
        return _run(sys.argv[1:] if argv is None else argv)
    except (MaterialClientError, OSError, ValueError):
        _fail()


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "CLIENT_PATH",
    "MATERIAL_CLIENT_PATH",
    "PRIVATE_KEY_PATH",
    "SERVICE_CA_PATH",
    "MaterialClientError",
    "main",
]
