#!/usr/bin/python3 -I
"""Validate the fixed installed authority before importing application code."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import pwd
import re
import stat
import sys
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import NoReturn, cast

LIBEXEC_PATH = Path(
    "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority"
)
MATERIAL_CLIENT_PATH = Path(
    "/usr/local/libexec/"
    "loom-personal-dev-native-builder-runtime-authority-material-client"
)
LIBRARY_ROOT = Path(
    "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority"
)
POLICY_PATH = Path(
    "/etc/loom/personal-dev-native-builder-runtime-authority.json"
)
OPERATOR_MATERIAL_POLICY_PATH = Path(
    "/etc/loom/personal-dev-native-builder-operator-material-authority.json"
)
BROKER_PATH = (
    LIBRARY_ROOT
    / "scripts"
    / "ops"
    / "personal_dev_native_builder_runtime_authority.py"
)

_POLICY_SCHEMA = "loom.personal-dev-native-builder-runtime-authority-policy.v1"
_OPERATOR_MATERIAL_POLICY_SCHEMA = (
    "loom.personal-dev-native-builder-operator-material-authority-policy.v1"
)
_MAX_POLICY_BYTES = 64 * 1024
_MAX_ASSET_BYTES = 8 * 1024 * 1024
_HEX_40 = re.compile(r"[0-9a-f]{40}")
_HEX_64 = re.compile(r"[0-9a-f]{64}")
_ROOT_ENVIRONMENT = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}
_UNSAFE_ENVIRONMENT_NAMES = frozenset(
    {
        "BASH_ENV",
        "CDPATH",
        "ENV",
        "IFS",
        "PYTHONHOME",
        "PYTHONPATH",
    }
)
_UNSAFE_ENVIRONMENT_PREFIXES = (
    "DOCKER_",
    "GIT_",
    "LD_",
    "NFTABLES_",
    "PYTHON",
    "SYSTEMD_",
)


class LauncherError(RuntimeError):
    """The installed pre-import authority boundary is invalid."""


@dataclass(frozen=True, slots=True)
class AssetSpec:
    path: Path
    mode: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.path, Path)
            or not self.path.is_absolute()
            or ".." in self.path.parts
            or not 0 <= self.mode <= 0o7777
        ):
            raise LauncherError("asset_spec_invalid")


_PYTHON_ASSETS = {
    "authority_client": "personal_dev_native_builder_runtime_authority_client.py",
    "crypto_helper": "personal_dev_native_builder_runtime_crypto.py",
    "protocol": "personal_dev_native_builder_runtime_authority_protocol.py",
    "installer": "install_personal_dev_native_builder_runtime.py",
    "runtime_profile_helper": "personal_dev_native_builder_runtime_profile.py",
    "converger": "converge_personal_dev_native_builder_release.py",
    "conformance": "personal_dev_native_builder_conformance.py",
}
_RUNTIME_ASSETS = {
    "runtime_asset_agent_service_template": (
        "loom-personal-dev-native-builder-agent.service.in"
    ),
    "runtime_asset_dockerd_config": "dockerd.json",
    "runtime_asset_dockerd_service": "loom-personal-dev-builder-dockerd.service",
    "runtime_asset_nftables": "provider-network.nft",
    "runtime_asset_profile": "runtime-profile-v1.json",
    "runtime_asset_runsc_config": "runsc.toml",
    "runtime_asset_slice_unit": "loom-personal-dev-builder.slice",
    "runtime_asset_sysusers": "loom-personal-dev-native-builder.sysusers",
}
ASSET_SPECS: Mapping[str, AssetSpec] = MappingProxyType(
    {
        "launcher": AssetSpec(LIBEXEC_PATH, 0o555),
        "material_client": AssetSpec(MATERIAL_CLIENT_PATH, 0o555),
        "broker": AssetSpec(BROKER_PATH, 0o444),
        **{
            name: AssetSpec(LIBRARY_ROOT / "scripts" / "ops" / filename, 0o444)
            for name, filename in _PYTHON_ASSETS.items()
        },
        **{
            name: AssetSpec(
                LIBRARY_ROOT / "deploy" / "personal-dev-native-builder" / filename,
                0o444,
            )
            for name, filename in _RUNTIME_ASSETS.items()
        },
        "sudoers": AssetSpec(
            Path(
                "/etc/sudoers.d/"
                "loom-personal-dev-native-builder-runtime-authority"
            ),
            0o440,
        ),
        "tmpfiles": AssetSpec(
            Path(
                "/usr/lib/tmpfiles.d/"
                "loom-personal-dev-native-builder-runtime-authority.conf"
            ),
            0o444,
        ),
    }
)
_OPERATOR_MATERIAL_ASSET_NAMES = frozenset(
    {
        "authority_client",
        "crypto_helper",
        "launcher",
        "material_client",
        "protocol",
    }
)
OPERATOR_MATERIAL_ASSET_SPECS: Mapping[str, AssetSpec] = MappingProxyType(
    {name: ASSET_SPECS[name] for name in sorted(_OPERATOR_MATERIAL_ASSET_NAMES)}
)


def _open_directory_no_follow(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
) -> int:
    if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts:
        raise OSError("unsafe path")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    if not no_follow or not directory_only:
        raise OSError("safe path traversal unavailable")
    descriptor = os.open(
        "/",
        os.O_RDONLY | os.O_CLOEXEC | directory_only | no_follow,
    )
    try:
        metadata = os.fstat(descriptor)
        mode = stat.S_IMODE(metadata.st_mode)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != 0
            or metadata.st_gid != 0
            or mode & 0o022
        ):
            raise OSError("unsafe root directory")
        for component in path.parts[1:]:
            child = os.open(
                component,
                os.O_RDONLY | os.O_CLOEXEC | directory_only | no_follow,
                dir_fd=descriptor,
            )
            os.close(descriptor)
            descriptor = child
            metadata = os.fstat(descriptor)
            mode = stat.S_IMODE(metadata.st_mode)
            permitted_owner = metadata.st_uid in {0, expected_uid}
            permitted_group = metadata.st_gid == (
                0 if metadata.st_uid == 0 else expected_gid
            )
            sticky_root = metadata.st_uid == 0 and bool(mode & stat.S_ISVTX)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or not permitted_owner
                or not permitted_group
                or (mode & 0o022 and not sticky_root)
            ):
                raise OSError("unsafe directory metadata")
        result = descriptor
        descriptor = -1
        return result
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _read_safe_file(
    path: Path,
    *,
    maximum: int,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    error: str,
) -> bytes:
    descriptor: int | None = None
    try:
        parent = _open_directory_no_follow(
            path.parent,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
        try:
            descriptor = os.open(
                path.name,
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=parent,
            )
        finally:
            os.close(parent)
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != expected_uid
            or before.st_gid != expected_gid
            or stat.S_IMODE(before.st_mode) != expected_mode
            or not 0 < before.st_size <= maximum
        ):
            raise LauncherError(error)
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                raise LauncherError(error)
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise LauncherError(error)
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_mode,
            before.st_nlink,
            before.st_uid,
            before.st_gid,
            before.st_size,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_nlink,
            after.st_uid,
            after.st_gid,
            after.st_size,
        ):
            raise LauncherError(error)
        return b"".join(chunks)
    except LauncherError:
        raise
    except OSError as exc:
        raise LauncherError(error) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise LauncherError("policy_invalid")
        result[name] = value
    return result


def _reject_constant(value: str) -> NoReturn:
    del value
    raise LauncherError("policy_invalid")


def _canonical_policy(value: Mapping[str, object]) -> bytes:
    try:
        return (
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("ascii")
    except (TypeError, ValueError) as exc:
        raise LauncherError("policy_invalid") from exc


def _load_asset_policy(
    *,
    policy_path: Path,
    asset_specs: Mapping[str, AssetSpec],
    expected_uid: int,
    expected_gid: int,
    schema: str,
    runtime_profile: bool,
) -> Mapping[str, object]:
    payload = _read_safe_file(
        policy_path,
        maximum=_MAX_POLICY_BYTES,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        expected_mode=0o444,
        error="policy_invalid",
    )
    try:
        loaded = json.loads(
            payload.decode("ascii"),
            object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        raise LauncherError("policy_invalid") from exc
    fields = {
        "asset_sha256",
        "authority_source_sha",
        "authority_source_tree",
        "schema",
    }
    if runtime_profile:
        fields.add("runtime_profile_sha256")
    if not isinstance(loaded, dict) or set(loaded) != fields:
        raise LauncherError("policy_invalid")
    assets = loaded.get("asset_sha256")
    source_sha = loaded.get("authority_source_sha")
    source_tree = loaded.get("authority_source_tree")
    profile_sha = loaded.get("runtime_profile_sha256")
    if (
        loaded.get("schema") != schema
        or not isinstance(assets, dict)
        or set(assets) != set(asset_specs)
        or not isinstance(source_sha, str)
        or _HEX_40.fullmatch(source_sha) is None
        or not isinstance(source_tree, str)
        or _HEX_40.fullmatch(source_tree) is None
        or source_sha == source_tree
        or (
            runtime_profile
            and (
                not isinstance(profile_sha, str)
                or _HEX_64.fullmatch(profile_sha) is None
            )
        )
        or any(
            not isinstance(name, str)
            or not isinstance(digest, str)
            or _HEX_64.fullmatch(digest) is None
            for name, digest in assets.items()
        )
        or payload != _canonical_policy(cast(dict[str, object], loaded))
    ):
        raise LauncherError("policy_invalid")
    digests = cast(dict[str, str], assets)
    for name, specification in asset_specs.items():
        asset = _read_safe_file(
            specification.path,
            maximum=_MAX_ASSET_BYTES,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=specification.mode,
            error="asset_invalid",
        )
        if hashlib.sha256(asset).hexdigest() != digests[name]:
            raise LauncherError("asset_invalid")
    profile_digest = digests.get("runtime_asset_profile")
    if runtime_profile and profile_digest is not None and profile_digest != profile_sha:
        raise LauncherError("policy_invalid")
    return MappingProxyType(cast(dict[str, object], loaded))


def load_policy(
    *,
    policy_path: Path = POLICY_PATH,
    asset_specs: Mapping[str, AssetSpec] = ASSET_SPECS,
    expected_uid: int = 0,
    expected_gid: int = 0,
) -> Mapping[str, object]:
    """Validate the canonical runtime policy and complete fixed inventory."""
    return _load_asset_policy(
        policy_path=policy_path,
        asset_specs=asset_specs,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        schema=_POLICY_SCHEMA,
        runtime_profile=True,
    )


def load_operator_material_policy(
    *,
    policy_path: Path = OPERATOR_MATERIAL_POLICY_PATH,
    asset_specs: Mapping[str, AssetSpec] = OPERATOR_MATERIAL_ASSET_SPECS,
    expected_uid: int = 0,
    expected_gid: int = 0,
) -> Mapping[str, object]:
    """Validate only the sealed OLDLAB material-client inventory."""
    if set(asset_specs) != _OPERATOR_MATERIAL_ASSET_NAMES:
        raise LauncherError("policy_invalid")
    return _load_asset_policy(
        policy_path=policy_path,
        asset_specs=asset_specs,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
        schema=_OPERATOR_MATERIAL_POLICY_SCHEMA,
        runtime_profile=False,
    )


def verify_invocation(
    *,
    argv: Sequence[str],
    environ: Mapping[str, str],
    uid_triplet: tuple[int, int, int],
    gid_triplet: tuple[int, int, int],
    operator_uid: int,
    operator_gid: int,
) -> None:
    unsafe = any(
        name in _UNSAFE_ENVIRONMENT_NAMES
        or any(name.startswith(prefix) for prefix in _UNSAFE_ENVIRONMENT_PREFIXES)
        for name in environ
    )
    if (
        unsafe
        or list(argv) != [str(LIBEXEC_PATH)]
        or uid_triplet != (0, 0, 0)
        or gid_triplet != (0, 0, 0)
        or environ.get("SUDO_USER") != "qianyi"
        or environ.get("SUDO_UID") != str(operator_uid)
        or environ.get("SUDO_GID") != str(operator_gid)
        or environ.get("SUDO_COMMAND") != str(LIBEXEC_PATH)
    ):
        raise LauncherError("invocation_invalid")


def sanitize_environment(environ: MutableMapping[str, str]) -> None:
    environ.clear()
    environ.update(_ROOT_ENVIRONMENT)


def _pin_application_packages(library_root: Path) -> None:
    if (
        not isinstance(library_root, Path)
        or not library_root.is_absolute()
        or ".." in library_root.parts
    ):
        raise LauncherError("asset_spec_invalid")
    scripts_package = ModuleType("scripts")
    scripts_package.__package__ = "scripts"
    scripts_package.__path__ = [str(library_root / "scripts")]
    ops_package = ModuleType("scripts.ops")
    ops_package.__package__ = "scripts.ops"
    ops_package.__path__ = [str(library_root / "scripts" / "ops")]
    scripts_package.__dict__["ops"] = ops_package
    sys.modules["scripts"] = scripts_package
    sys.modules["scripts.ops"] = ops_package


def launch(
    *,
    policy_path: Path = POLICY_PATH,
    asset_specs: Mapping[str, AssetSpec] = ASSET_SPECS,
    broker_path: Path = BROKER_PATH,
    library_root: Path = LIBRARY_ROOT,
    expected_uid: int = 0,
    expected_gid: int = 0,
) -> None:
    """Validate every installed byte before loading the large broker runtime."""
    policy = load_policy(
        policy_path=policy_path,
        asset_specs=asset_specs,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
    )
    if asset_specs.get("broker") != AssetSpec(broker_path, 0o444):
        raise LauncherError("asset_spec_invalid")
    _pin_application_packages(library_root)
    try:
        sys.modules[
            "scripts.ops.personal_dev_native_builder_runtime_authority_launcher"
        ] = sys.modules[__name__]
        specification = importlib.util.spec_from_file_location(
            "scripts.ops.personal_dev_native_builder_runtime_authority",
            broker_path,
        )
        if specification is None or specification.loader is None:
            raise LauncherError("broker_invalid")
        module = importlib.util.module_from_spec(specification)
        sys.modules[specification.name] = module
        specification.loader.exec_module(module)
        serve = getattr(module, "serve_validated", None)
        if not callable(serve):
            raise LauncherError("broker_invalid")
        serve(policy)
    except LauncherError:
        raise
    except BaseException as exc:
        raise LauncherError("broker_failed") from exc


def main() -> None:
    try:
        operator = pwd.getpwnam("qianyi")
        verify_invocation(
            argv=sys.argv,
            environ=os.environ,
            uid_triplet=os.getresuid(),
            gid_triplet=os.getresgid(),
            operator_uid=operator.pw_uid,
            operator_gid=operator.pw_gid,
        )
        sanitize_environment(os.environ)
        launch()
    except BaseException:
        sys.stderr.write("error:authority_failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "ASSET_SPECS",
    "BROKER_PATH",
    "LIBEXEC_PATH",
    "LIBRARY_ROOT",
    "MATERIAL_CLIENT_PATH",
    "OPERATOR_MATERIAL_ASSET_SPECS",
    "OPERATOR_MATERIAL_POLICY_PATH",
    "POLICY_PATH",
    "AssetSpec",
    "LauncherError",
    "launch",
    "load_operator_material_policy",
    "load_policy",
    "main",
    "sanitize_environment",
    "verify_invocation",
]
