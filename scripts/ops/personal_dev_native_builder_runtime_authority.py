#!/usr/bin/python3 -I
"""Fixed root authority for the inert personal native-builder runtime."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import selectors
import signal
import stat
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator, Mapping, MutableMapping, Sequence
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, BinaryIO, NoReturn, Protocol, cast

if __name__ == "__main__" and __package__ in {None, ""}:
    sys.dont_write_bytecode = True
    sys.path.insert(
        0,
        "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority",
    )

if TYPE_CHECKING:
    from scripts.ops.converge_personal_dev_native_builder_release import (
        NativeBuilderReleaseConfig,
        NativeBuilderReleaseImage,
        create_broker_release_converger,
    )
    from scripts.ops.install_personal_dev_native_builder_runtime import (
        NativeBuilderCommandResult,
        NativeBuilderInstallContext,
        PersonalDevNativeBuilderRuntimeInstaller,
    )
    from scripts.ops.personal_dev_native_builder_conformance import (
        CommandResult,
        ConformanceInputs,
        Runner,
        run_conformance,
    )
    from scripts.ops.personal_dev_native_builder_runtime_authority_protocol import (
        AuthorityRequest,
        ProtocolError,
        encode_request,
        parse_request,
    )
    from scripts.ops.personal_dev_native_builder_runtime_profile import (
        load_native_builder_runtime_profile,
    )


_APPLICATION_MODULES_LOADED = False


def _load_application_modules() -> None:
    """Import policy-bound application code only after bootstrap validation."""
    global _APPLICATION_MODULES_LOADED
    if _APPLICATION_MODULES_LOADED:
        return
    from scripts.ops.converge_personal_dev_native_builder_release import (
        NativeBuilderReleaseConfig as _ReleaseConfig,
    )
    from scripts.ops.converge_personal_dev_native_builder_release import (
        NativeBuilderReleaseImage as _ReleaseImage,
    )
    from scripts.ops.converge_personal_dev_native_builder_release import (
        create_broker_release_converger as create_converger,
    )
    from scripts.ops.install_personal_dev_native_builder_runtime import (
        NativeBuilderCommandResult as _InstallerResult,
    )
    from scripts.ops.install_personal_dev_native_builder_runtime import (
        NativeBuilderInstallContext as _InstallContext,
    )
    from scripts.ops.install_personal_dev_native_builder_runtime import (
        PersonalDevNativeBuilderRuntimeInstaller as _RuntimeInstaller,
    )
    from scripts.ops.personal_dev_native_builder_conformance import (
        CommandResult as _CommandResult,
    )
    from scripts.ops.personal_dev_native_builder_conformance import (
        ConformanceInputs as _ConformanceInputs,
    )
    from scripts.ops.personal_dev_native_builder_conformance import (
        Runner as _Runner,
    )
    from scripts.ops.personal_dev_native_builder_conformance import (
        run_conformance as conformance_operation,
    )
    from scripts.ops.personal_dev_native_builder_runtime_authority_protocol import (
        AuthorityRequest as _AuthorityRequest,
    )
    from scripts.ops.personal_dev_native_builder_runtime_authority_protocol import (
        ProtocolError as _ProtocolError,
    )
    from scripts.ops.personal_dev_native_builder_runtime_authority_protocol import (
        encode_request as encode_authority_request,
    )
    from scripts.ops.personal_dev_native_builder_runtime_authority_protocol import (
        parse_request as parse_authority_request,
    )
    from scripts.ops.personal_dev_native_builder_runtime_profile import (
        load_native_builder_runtime_profile as load_runtime_profile,
    )

    globals().update(
        {
            "AuthorityRequest": _AuthorityRequest,
            "CommandResult": _CommandResult,
            "ConformanceInputs": _ConformanceInputs,
            "NativeBuilderCommandResult": _InstallerResult,
            "NativeBuilderInstallContext": _InstallContext,
            "NativeBuilderReleaseConfig": _ReleaseConfig,
            "NativeBuilderReleaseImage": _ReleaseImage,
            "PersonalDevNativeBuilderRuntimeInstaller": _RuntimeInstaller,
            "ProtocolError": _ProtocolError,
            "Runner": _Runner,
            "create_broker_release_converger": create_converger,
            "encode_request": encode_authority_request,
            "load_native_builder_runtime_profile": load_runtime_profile,
            "parse_request": parse_authority_request,
            "run_conformance": conformance_operation,
        }
    )
    _APPLICATION_MODULES_LOADED = True


if __name__ != "__main__":
    _load_application_modules()

LIBEXEC_PATH = Path(
    "/usr/local/libexec/loom-personal-dev-native-builder-runtime-authority"
)
LIBRARY_ROOT = Path(
    "/usr/local/lib/loom-personal-dev-native-builder-runtime-authority"
)
POLICY_PATH = Path(
    "/etc/loom/personal-dev-native-builder-runtime-authority.json"
)
STATE_ROOT = Path(
    "/var/lib/loom/personal-dev-native-builder-runtime-authority"
)
STATE_PATH = STATE_ROOT / "state-v1.json"
LOCK_PATH = Path(
    "/run/lock/loom-personal-dev-native-builder-runtime-authority.lock"
)
EPHEMERAL_SECRET_ROOT = Path(
    "/run/loom-personal-dev-native-builder-runtime-authority"
)

_POLICY_SCHEMA = "loom.personal-dev-native-builder-runtime-authority-policy.v1"
_RECEIPT_SCHEMA = "loom.personal-dev-native-builder-runtime-authority-receipt.v1"
_EXPECTED_HOST = "gx10-01c7"
_EXPECTED_ARCHITECTURE = "aarch64"
_MAX_POLICY_BYTES = 64 * 1024
_MAX_ASSET_BYTES = 8 * 1024 * 1024
_MAX_RECEIPT_BYTES = 64 * 1024
_MAX_STATE_BYTES = 64 * 1024
_MAX_ARCHIVE_BYTES = 1024**3
_HEX_40 = re.compile(r"[0-9a-f]{40}")
_HEX_64 = re.compile(r"[0-9a-f]{64}")
_HEX_128 = re.compile(r"[0-9a-f]{128}")
_SYSTEM_ENVIRONMENT = {
    "LANG": "C",
    "LC_ALL": "C",
    "PATH": "/usr/bin:/bin",
    "PYTHONDONTWRITEBYTECODE": "1",
}
_DOCKER_ENDPOINT = "unix:///run/loom-personal-dev-builder/docker.sock"
_DOCKER_DATA_ROOT = Path("/var/lib/loom-personal-dev-builder/docker")
_DOCKERD_UNIT = "loom-personal-dev-builder-dockerd.service"
_AGENT_UNIT = "loom-personal-dev-native-builder-agent.service"
_NFT_PATH = Path("/etc/loom/personal-dev-native-builder/provider-network.nft")
_NFT_TABLE = "loom_personal_dev_builder"
_INSTALLED_PROFILE_PATH = (
    LIBRARY_ROOT
    / "deploy"
    / "personal-dev-native-builder"
    / "runtime-profile-v1.json"
)
_ERROR_CODE = re.compile(r"[a-z][a-z0-9_]{0,63}")
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


class AuthorityError(RuntimeError):
    """The fixed authority contract could not be satisfied."""

    def __init__(self, code: str) -> None:
        safe_code = code if _ERROR_CODE.fullmatch(code) is not None else "authority_failed"
        self.code = safe_code
        super().__init__(safe_code)


class _CommandOutputLimitError(RuntimeError):
    pass


class _CommandSignal(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum
        super().__init__(signum)


class BoundedSubprocessRunner:
    """Run fixed child commands with bounded output, time, and process groups."""

    def __init__(self, *, timeout_seconds: float, maximum_output: int) -> None:
        if (
            not isinstance(timeout_seconds, (int, float))
            or isinstance(timeout_seconds, bool)
            or not 0 < timeout_seconds <= 600
            or type(maximum_output) is not int
            or not 0 < maximum_output <= 4 * 1024 * 1024
        ):
            raise AuthorityError("command_boundary_invalid")
        self.timeout_seconds = float(timeout_seconds)
        self.maximum_output = maximum_output

    def _drain(self, process: subprocess.Popen[bytes]) -> tuple[bytes, bytes]:
        if process.stdout is None or process.stderr is None:
            raise AuthorityError("command_failed")
        chunks: dict[str, bytearray] = {
            "stdout": bytearray(),
            "stderr": bytearray(),
        }
        deadline = time.monotonic() + self.timeout_seconds
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(
                        process.args,
                        self.timeout_seconds,
                    )
                for key, _ in selector.select(remaining):
                    stream = key.fileobj
                    descriptor = stream if isinstance(stream, int) else stream.fileno()
                    chunk = os.read(descriptor, 8192)
                    if not chunk:
                        selector.unregister(stream)
                        continue
                    output = chunks[cast(str, key.data)]
                    if len(output) + len(chunk) > self.maximum_output:
                        raise _CommandOutputLimitError()
                    output.extend(chunk)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise subprocess.TimeoutExpired(process.args, self.timeout_seconds)
        process.wait(timeout=remaining)
        return bytes(chunks["stdout"]), bytes(chunks["stderr"])

    @staticmethod
    def _group_exists(process_group: int) -> bool:
        try:
            os.killpg(process_group, 0)
        except ProcessLookupError:
            return False
        return True

    def _terminate(self, process: subprocess.Popen[bytes]) -> None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        for stream in (process.stdout, process.stderr):
            if stream is not None:
                stream.close()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired as exc:
                raise AuthorityError("command_cleanup_failed") from exc
        if self._group_exists(process.pid):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            for _ in range(100):
                if not self._group_exists(process.pid):
                    break
                time.sleep(0.01)
            else:
                raise AuthorityError("command_cleanup_failed")

    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> CommandResult:
        if (
            not argv
            or any(
                not isinstance(value, str) or not value or "\0" in value
                for value in argv
            )
            or type(check) is not bool
            or (env is not None
            and (
                not isinstance(env, dict)
                or any(
                    not isinstance(name, str)
                    or not name
                    or not isinstance(value, str)
                    or "\0" in name
                    or "\0" in value
                    for name, value in env.items()
                )
            ))
        ):
            raise AuthorityError("command_invalid")
        process: subprocess.Popen[bytes] | None = None
        previous_handlers: dict[int, object] = {}
        pending_signal: int | None = None
        caught: BaseException | None = None
        stdout = b""
        stderr = b""
        try:
            process = subprocess.Popen(
                list(argv),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=_SYSTEM_ENVIRONMENT if env is None else env,
                start_new_session=True,
            )

            def forward(signum: int, _frame: object) -> None:
                try:
                    os.killpg(process.pid, signum)
                except ProcessLookupError:
                    pass
                raise _CommandSignal(signum)

            if threading.current_thread() is threading.main_thread():
                for signum in (signal.SIGINT, signal.SIGTERM):
                    previous_handlers[signum] = signal.signal(signum, forward)
            stdout, stderr = self._drain(process)
        except BaseException as exc:
            caught = exc
            if isinstance(exc, _CommandSignal):
                pending_signal = exc.signum

        group_leaked = (
            process is not None
            and caught is None
            and self._group_exists(process.pid)
        )
        cleanup_failure: BaseException | None = None
        if process is not None and (caught is not None or group_leaked):
            if previous_handlers:
                def defer(signum: int, _frame: object) -> None:
                    nonlocal pending_signal
                    if pending_signal is None:
                        pending_signal = signum

                for deferred_signum in previous_handlers:
                    signal.signal(deferred_signum, defer)
            try:
                self._terminate(process)
            except BaseException as exc:
                cleanup_failure = exc

        if previous_handlers:
            for restored_signum, handler in previous_handlers.items():
                signal.signal(restored_signum, cast(signal.Handlers, handler))

        if pending_signal is not None:
            handler = previous_handlers[pending_signal]
            if handler == signal.SIG_DFL:
                signal.raise_signal(pending_signal)
            elif handler != signal.SIG_IGN and callable(handler):
                cast(Callable[[int, object], object], handler)(pending_signal, None)

        if cleanup_failure is not None:
            if isinstance(cleanup_failure, AuthorityError):
                raise cleanup_failure
            raise AuthorityError("command_cleanup_failed") from cleanup_failure
        if group_leaked:
            raise AuthorityError("command_cleanup_failed")
        if caught is not None:
            if isinstance(caught, subprocess.TimeoutExpired):
                raise AuthorityError("command_timeout") from caught
            if isinstance(caught, _CommandOutputLimitError):
                raise AuthorityError("command_output_invalid") from caught
            if isinstance(caught, _CommandSignal):
                raise AuthorityError("command_interrupted") from caught
            if isinstance(caught, AuthorityError):
                raise caught
            raise AuthorityError("command_failed") from caught
        if process is None:
            raise AuthorityError("command_failed")
        result = CommandResult(
            process.returncode,
            stdout.decode("utf-8", errors="replace"),
            stderr.decode("utf-8", errors="replace"),
        )
        if check and result.returncode != 0:
            raise AuthorityError("command_failed")
        return result


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
            raise AuthorityError("asset_spec_invalid")


_PYTHON_ASSETS = {
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
        "broker": AssetSpec(LIBEXEC_PATH, 0o555),
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


def _validated_hex(value: object, expression: re.Pattern[str]) -> str:
    if not isinstance(value, str) or expression.fullmatch(value) is None:
        raise AuthorityError("policy_invalid")
    return value


@dataclass(frozen=True, slots=True)
class AuthorityPolicy:
    authority_source_sha: str
    authority_source_tree: str
    runtime_profile_sha256: str
    asset_sha256: Mapping[str, str]

    def __post_init__(self) -> None:
        source_sha = _validated_hex(self.authority_source_sha, _HEX_40)
        source_tree = _validated_hex(self.authority_source_tree, _HEX_40)
        _validated_hex(self.runtime_profile_sha256, _HEX_64)
        if source_sha == source_tree or not isinstance(self.asset_sha256, Mapping):
            raise AuthorityError("policy_invalid")
        copied: dict[str, str] = {}
        for name, digest in self.asset_sha256.items():
            if not isinstance(name, str) or not name:
                raise AuthorityError("policy_invalid")
            copied[name] = _validated_hex(digest, _HEX_64)
        object.__setattr__(self, "asset_sha256", MappingProxyType(copied))

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> AuthorityPolicy:
        if set(value) != {
            "asset_sha256",
            "authority_source_sha",
            "authority_source_tree",
            "runtime_profile_sha256",
            "schema",
        } or value.get("schema") != _POLICY_SCHEMA:
            raise AuthorityError("policy_invalid")
        assets = value.get("asset_sha256")
        if not isinstance(assets, dict):
            raise AuthorityError("policy_invalid")
        return cls(
            authority_source_sha=_validated_hex(value.get("authority_source_sha"), _HEX_40),
            authority_source_tree=_validated_hex(
                value.get("authority_source_tree"), _HEX_40
            ),
            runtime_profile_sha256=_validated_hex(
                value.get("runtime_profile_sha256"), _HEX_64
            ),
            asset_sha256=cast(dict[str, str], assets),
        )

    def public(self) -> dict[str, object]:
        return {
            "asset_sha256": dict(self.asset_sha256),
            "authority_source_sha": self.authority_source_sha,
            "authority_source_tree": self.authority_source_tree,
            "runtime_profile_sha256": self.runtime_profile_sha256,
            "schema": _POLICY_SCHEMA,
        }


def _canonical_json(value: object) -> bytes:
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
        raise AuthorityError("encoding_invalid") from exc


def encode_policy(policy: AuthorityPolicy) -> bytes:
    """Return the only accepted on-disk policy representation."""
    if not isinstance(policy, AuthorityPolicy):
        raise AuthorityError("policy_invalid")
    return _canonical_json(policy.public())


def encode_receipt(receipt: Mapping[str, object]) -> bytes:
    """Encode one bounded canonical public receipt."""
    payload = _canonical_json(dict(receipt))
    if len(payload) > _MAX_RECEIPT_BYTES:
        raise AuthorityError("receipt_invalid")
    return payload


def _file_identity(value: os.stat_result) -> tuple[int, ...]:
    return (
        value.st_dev,
        value.st_ino,
        value.st_mode,
        value.st_uid,
        value.st_gid,
        value.st_nlink,
        value.st_size,
        value.st_mtime_ns,
        value.st_ctime_ns,
    )


def _open_directory_no_follow(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int | None = None,
) -> int:
    if not isinstance(path, Path) or not path.is_absolute() or ".." in path.parts:
        raise OSError("unsafe path")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    directory_only = getattr(os, "O_DIRECTORY", 0)
    if not no_follow or not directory_only:
        raise OSError("safe path traversal unavailable")
    directory = os.open(
        "/",
        os.O_RDONLY | os.O_CLOEXEC | directory_only | no_follow,
    )
    try:
        components = path.parts[1:]
        root_metadata = os.fstat(directory)
        root_mode = stat.S_IMODE(root_metadata.st_mode)
        if (
            not stat.S_ISDIR(root_metadata.st_mode)
            or root_metadata.st_uid != 0
            or root_metadata.st_gid != 0
            or root_mode & 0o022
            or (
                not components
                and expected_mode is not None
                and (
                    expected_uid != 0
                    or expected_gid != 0
                    or root_mode != expected_mode
                )
            )
        ):
            raise OSError("unsafe directory metadata")
        for index, component in enumerate(components):
            child = os.open(
                component,
                os.O_RDONLY | os.O_CLOEXEC | directory_only | no_follow,
                dir_fd=directory,
            )
            os.close(directory)
            directory = child
            metadata = os.fstat(directory)
            mode = stat.S_IMODE(metadata.st_mode)
            final = index == len(components) - 1
            permitted_owner = metadata.st_uid in {0, expected_uid}
            permitted_group = (
                metadata.st_gid == (0 if metadata.st_uid == 0 else expected_gid)
            )
            sticky_root = metadata.st_uid == 0 and bool(mode & stat.S_ISVTX)
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or not permitted_owner
                or not permitted_group
                or (mode & 0o022 and not sticky_root)
                or (
                    final
                    and expected_mode is not None
                    and (
                        metadata.st_uid != expected_uid
                        or metadata.st_gid != expected_gid
                        or mode != expected_mode
                    )
                )
            ):
                raise OSError("unsafe directory metadata")
        result = directory
        directory = -1
        return result
    finally:
        if directory >= 0:
            os.close(directory)


def _open_readonly_no_follow(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
) -> int:
    if not isinstance(path, Path) or path == Path("/"):
        raise OSError("unsafe path")
    directory = _open_directory_no_follow(
        path.parent,
        expected_uid=expected_uid,
        expected_gid=expected_gid,
    )
    try:
        return os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory,
        )
    finally:
        os.close(directory)


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
        descriptor = _open_readonly_no_follow(
            path,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
        )
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_nlink != 1
            or before.st_uid != expected_uid
            or before.st_gid != expected_gid
            or stat.S_IMODE(before.st_mode) != expected_mode
            or not 0 < before.st_size <= maximum
        ):
            raise AuthorityError(error)
        chunks: list[bytes] = []
        remaining = before.st_size
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                raise AuthorityError(error)
            chunks.append(chunk)
            remaining -= len(chunk)
        if os.read(descriptor, 1):
            raise AuthorityError(error)
        after = os.fstat(descriptor)
        if _file_identity(before) != _file_identity(after):
            raise AuthorityError(error)
        return b"".join(chunks)
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError(error) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for name, value in pairs:
        if name in result:
            raise AuthorityError("policy_invalid")
        result[name] = value
    return result


def _reject_constant(value: str) -> NoReturn:
    del value
    raise AuthorityError("policy_invalid")


def load_policy(
    *,
    policy_path: Path = POLICY_PATH,
    asset_specs: Mapping[str, AssetSpec] = ASSET_SPECS,
    expected_uid: int = 0,
    expected_gid: int = 0,
) -> AuthorityPolicy:
    """Load the fixed policy and verify every path selected by compiled code."""
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
        raise AuthorityError("policy_invalid") from exc
    if not isinstance(loaded, dict):
        raise AuthorityError("policy_invalid")
    policy = AuthorityPolicy.from_mapping(loaded)
    if payload != encode_policy(policy) or set(policy.asset_sha256) != set(asset_specs):
        raise AuthorityError("policy_invalid")
    for name, spec in asset_specs.items():
        asset = _read_safe_file(
            spec.path,
            maximum=_MAX_ASSET_BYTES,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=spec.mode,
            error="asset_invalid",
        )
        if hashlib.sha256(asset).hexdigest() != policy.asset_sha256[name]:
            raise AuthorityError("asset_invalid")
    profile_digest = policy.asset_sha256.get("runtime_asset_profile")
    if profile_digest is not None and profile_digest != policy.runtime_profile_sha256:
        raise AuthorityError("policy_invalid")
    return policy


def verify_invocation(
    *,
    argv: Sequence[str],
    environ: Mapping[str, str],
    uid_triplet: tuple[int, int, int],
    gid_triplet: tuple[int, int, int],
    operator_uid: int,
    operator_gid: int,
) -> None:
    """Verify the exact no-argument sudo identity before reading a request."""
    unsafe = any(
        name in _UNSAFE_ENVIRONMENT_NAMES
        or any(name.startswith(prefix) for prefix in _UNSAFE_ENVIRONMENT_PREFIXES)
        for name in environ
    )
    if unsafe:
        raise AuthorityError("environment_invalid")
    if (
        list(argv) != [str(LIBEXEC_PATH)]
        or uid_triplet != (0, 0, 0)
        or gid_triplet != (0, 0, 0)
        or environ.get("SUDO_USER") != "qianyi"
        or environ.get("SUDO_UID") != str(operator_uid)
        or environ.get("SUDO_GID") != str(operator_gid)
        or environ.get("SUDO_COMMAND") != str(LIBEXEC_PATH)
    ):
        raise AuthorityError("invocation_invalid")


def sanitize_environment(environ: MutableMapping[str, str]) -> None:
    """Replace inherited values before any child boundary can observe them."""
    environ.clear()
    environ.update(_ROOT_ENVIRONMENT)


@contextmanager
def authority_lock(
    path: Path = LOCK_PATH,
    *,
    expected_uid: int = 0,
    expected_gid: int = 0,
) -> Iterator[None]:
    """Hold the one pre-created root-owned authority lock without waiting."""
    descriptor: int | None = None
    try:
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
        )
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_uid != expected_uid
            or metadata.st_gid != expected_gid
            or stat.S_IMODE(metadata.st_mode) != 0o600
        ):
            raise AuthorityError("lock_invalid")
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise AuthorityError("authority_busy") from exc
        yield
    except AuthorityError:
        raise
    except OSError as exc:
        raise AuthorityError("lock_invalid") from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class HostStatus:
    host_name: str
    architecture: str
    dockerd_active: bool
    agent_active: bool
    nft_present: bool
    managed_containers: int
    managed_networks: int


class SystemHostAdapter:
    """Expose only the compiled service, nftables, and managed-object scope."""

    def __init__(
        self,
        *,
        runner: Runner,
        expected_uid: int = 0,
        expected_gid: int = 0,
    ) -> None:
        self.runner = runner
        self.expected_uid = expected_uid
        self.expected_gid = expected_gid

    def _run(self, *argv: str) -> CommandResult:
        result = self.runner.run(
            argv,
            check=False,
            env=dict(_SYSTEM_ENVIRONMENT),
        )
        if (
            not isinstance(result, CommandResult)
            or len(result.stdout.encode("utf-8", errors="replace")) > 64 * 1024
            or len(result.stderr.encode("utf-8", errors="replace")) > 64 * 1024
        ):
            raise AuthorityError("host_command_invalid")
        return result

    def _service_active(self, unit: str) -> bool:
        result = self._run("/usr/bin/systemctl", "is-active", unit)
        if result.stderr:
            raise AuthorityError("host_state_invalid")
        if result == CommandResult(0, "active\n", ""):
            return True
        if result in {
            CommandResult(3, "inactive\n", ""),
            CommandResult(4, "unknown\n", ""),
        }:
            return False
        raise AuthorityError("host_state_invalid")

    def _nft_present(self) -> bool:
        result = self._run("/usr/sbin/nft", "list", "tables")
        if result.returncode != 0 or result.stderr:
            raise AuthorityError("host_state_invalid")
        target = f"table inet {_NFT_TABLE}"
        lines = tuple(line.strip() for line in result.stdout.splitlines() if line.strip())
        if lines.count(target) > 1:
            raise AuthorityError("host_state_invalid")
        return target in lines

    def _managed_count(self, kind: str) -> int:
        arguments = (
            ("container", "ls", "--all", "--quiet", "--no-trunc")
            if kind == "container"
            else ("network", "ls", "--quiet", "--no-trunc")
        )
        filters = () if kind == "container" else ("--filter", "type=custom")
        result = self._run(
            "/usr/bin/docker",
            "-H",
            _DOCKER_ENDPOINT,
            *arguments,
            *filters,
        )
        identifiers = tuple(line for line in result.stdout.splitlines() if line)
        if (
            result.returncode != 0
            or result.stderr
            or len(set(identifiers)) != len(identifiers)
            or any(_HEX_64.fullmatch(identifier) is None for identifier in identifiers)
        ):
            raise AuthorityError("managed_objects_invalid")
        return len(identifiers)

    def _open_inventory_directory(self, parent: int, name: str) -> int:
        descriptor = os.open(
            name,
            os.O_RDONLY
            | os.O_CLOEXEC
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=parent,
        )
        metadata = os.fstat(descriptor)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != self.expected_uid
            or metadata.st_gid != self.expected_gid
            or stat.S_IMODE(metadata.st_mode) & 0o022
        ):
            os.close(descriptor)
            raise AuthorityError("managed_objects_invalid")
        return descriptor

    def _offline_inventory(self) -> tuple[int, int]:
        try:
            docker_descriptor = _open_directory_no_follow(
                _DOCKER_DATA_ROOT,
                expected_uid=self.expected_uid,
                expected_gid=self.expected_gid,
            )
        except FileNotFoundError:
            return 0, 0
        except OSError as exc:
            raise AuthorityError("managed_objects_invalid") from exc
        container_ids: set[str] = set()
        containers_descriptor: int | None = None
        try:
            containers_descriptor = self._open_inventory_directory(
                docker_descriptor,
                "containers",
            )
        except FileNotFoundError:
            pass
        except (AuthorityError, OSError) as exc:
            os.close(docker_descriptor)
            raise AuthorityError("managed_objects_invalid") from exc
        else:
            try:
                entries = tuple(os.listdir(containers_descriptor))
            except OSError as exc:
                os.close(containers_descriptor)
                os.close(docker_descriptor)
                raise AuthorityError("managed_objects_invalid") from exc
            for entry in entries:
                try:
                    metadata = os.stat(
                        entry,
                        dir_fd=containers_descriptor,
                        follow_symlinks=False,
                    )
                except OSError as exc:
                    os.close(containers_descriptor)
                    os.close(docker_descriptor)
                    raise AuthorityError("managed_objects_invalid") from exc
                if (
                    _HEX_64.fullmatch(entry) is None
                    or not stat.S_ISDIR(metadata.st_mode)
                    or metadata.st_uid != self.expected_uid
                    or metadata.st_gid != self.expected_gid
                ):
                    os.close(containers_descriptor)
                    os.close(docker_descriptor)
                    raise AuthorityError("managed_objects_invalid")
                container_ids.add(entry)
            os.close(containers_descriptor)

        network_ids: set[str] = set()
        network_descriptor: int | None = None
        files_descriptor: int | None = None
        descriptor: int | None = None
        try:
            network_descriptor = self._open_inventory_directory(
                docker_descriptor,
                "network",
            )
            files_descriptor = self._open_inventory_directory(
                network_descriptor,
                "files",
            )
            descriptor = os.open(
                "local-kv.db",
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=files_descriptor,
            )
        except FileNotFoundError:
            pass
        except (AuthorityError, OSError) as exc:
            raise AuthorityError("managed_objects_invalid") from exc
        else:
            try:
                metadata = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_uid != self.expected_uid
                    or metadata.st_gid != self.expected_gid
                    or metadata.st_size > 64 * 1024 * 1024
                ):
                    raise AuthorityError("managed_objects_invalid")
                payload = bytearray()
                while len(payload) < metadata.st_size:
                    chunk = os.read(descriptor, min(64 * 1024, metadata.st_size - len(payload)))
                    if not chunk:
                        raise AuthorityError("managed_objects_invalid")
                    payload.extend(chunk)
                marker = re.compile(
                    rb"docker/network/v1\.0/network/([0-9a-f]{64})/"
                )
                network_ids.update(
                    match.group(1).decode("ascii") for match in marker.finditer(payload)
                )
            finally:
                os.close(descriptor)
        finally:
            if files_descriptor is not None:
                os.close(files_descriptor)
            if network_descriptor is not None:
                os.close(network_descriptor)
            os.close(docker_descriptor)
        return len(container_ids), len(network_ids)

    def status(self) -> HostStatus:
        dockerd_active = self._service_active(_DOCKERD_UNIT)
        agent_active = self._service_active(_AGENT_UNIT)
        nft_present = self._nft_present()
        if dockerd_active:
            containers = self._managed_count("container")
            networks = self._managed_count("network")
        else:
            containers, networks = self._offline_inventory()
        host = os.uname()
        return HostStatus(
            host_name=host.nodename,
            architecture=host.machine,
            dockerd_active=dockerd_active,
            agent_active=agent_active,
            nft_present=nft_present,
            managed_containers=containers,
            managed_networks=networks,
        )

    def verify_inert(self, *, require_empty: bool) -> HostStatus:
        if type(require_empty) is not bool:
            raise AuthorityError("host_state_invalid")
        observed = self.status()
        if (
            observed.dockerd_active
            or observed.agent_active
            or observed.nft_present
            or (require_empty
            and (observed.managed_containers or observed.managed_networks))
        ):
            raise AuthorityError("host_state_invalid")
        return observed

    def _service(self, action: str, unit: str, *, active: bool) -> None:
        if self._service_active(unit) is active:
            return
        result = self._run("/usr/bin/systemctl", action, unit)
        if result != CommandResult(0, "", "") or self._service_active(unit) is not active:
            raise AuthorityError("host_mutation_failed")

    def load_nft(self) -> None:
        if self._nft_present():
            raise AuthorityError("host_state_invalid")
        for arguments in (
            ("--check", "--file", str(_NFT_PATH)),
            ("--file", str(_NFT_PATH)),
        ):
            if self._run("/usr/sbin/nft", *arguments) != CommandResult(0, "", ""):
                raise AuthorityError("host_mutation_failed")
        if not self._nft_present():
            raise AuthorityError("host_mutation_failed")

    def delete_nft(self) -> None:
        if not self._nft_present():
            return
        result = self._run(
            "/usr/sbin/nft",
            "delete",
            "table",
            "inet",
            _NFT_TABLE,
        )
        if result != CommandResult(0, "", "") or self._nft_present():
            raise AuthorityError("host_mutation_failed")

    def start_dockerd(self) -> None:
        if any(self._offline_inventory()):
            raise AuthorityError("managed_objects_invalid")
        self._service("start", _DOCKERD_UNIT, active=True)
        try:
            if self._managed_count("container") or self._managed_count("network"):
                raise AuthorityError("managed_objects_invalid")
        except BaseException:
            self._service("stop", _DOCKERD_UNIT, active=False)
            raise

    def stop_dockerd(self) -> None:
        self._service("stop", _DOCKERD_UNIT, active=False)

    def start_agent(self) -> None:
        self._service("start", _AGENT_UNIT, active=True)

    def stop_agent(self) -> None:
        self._service("stop", _AGENT_UNIT, active=False)


class InstallerRunnerAdapter:
    """Translate the bounded broker result into the installer's typed result."""

    def __init__(self, runner: Runner) -> None:
        self.runner = runner

    def run(
        self,
        argv: Sequence[str],
        *,
        check: bool = True,
        env: dict[str, str] | None = None,
    ) -> NativeBuilderCommandResult:
        result = self.runner.run(argv, check=check, env=env)
        return NativeBuilderCommandResult(
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )


class ConformanceOperations:
    """Bind conformance to the reviewed fixed function and bounded runner."""

    def __init__(self, runner: Runner) -> None:
        self.runner = runner

    def run(self, inputs: ConformanceInputs) -> Mapping[str, object]:
        return run_conformance(inputs, self.runner)


@dataclass(frozen=True, slots=True)
class StateSnapshot:
    value: Mapping[str, object]
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.value, Mapping) or _HEX_64.fullmatch(self.sha256) is None:
            raise AuthorityError("state_invalid")
        validated = _validate_state(self.value)
        payload = _canonical_json(validated)
        if hashlib.sha256(payload).hexdigest() != self.sha256:
            raise AuthorityError("state_invalid")
        object.__setattr__(self, "value", MappingProxyType(validated))


_PREPARED_STATE_FIELDS = frozenset(
    {
        "authority_source_sha",
        "authority_source_tree",
        "conformance",
        "current_agent",
        "current_builder",
        "current_revision",
        "phase",
        "previous_agent",
        "previous_builder",
        "previous_revision",
        "public_store_origin",
        "runtime_profile_sha256",
        "schema",
    }
)
_STAGED_STATE_FIELDS = _PREPARED_STATE_FIELDS | frozenset(
    {
        "agent_instance_id",
        "agent_key_id",
        "public_key_sha256",
        "service_origin",
    }
)
_CONFORMANCE_FIELDS = frozenset(
    {
        "architecture",
        "buildkit_sandbox_id",
        "client_sandbox_id",
        "cross_provider_network",
        "foreign_to_provider",
        "host_to_provider",
        "managed_containers_after",
        "managed_networks_after",
        "platform",
        "private_control_plane",
        "public_https",
        "runtime",
        "schema",
        "status",
    }
)


def _state_string(value: Mapping[str, object], name: str) -> str:
    item = value.get(name)
    if not isinstance(item, str):
        raise AuthorityError("state_invalid")
    return item


def _validate_conformance(value: object) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != _CONFORMANCE_FIELDS:
        raise AuthorityError("state_invalid")
    expected_literals: Mapping[str, object] = {
        "architecture": "arm64",
        "cross_provider_network": "denied",
        "foreign_to_provider": "denied",
        "host_to_provider": "denied",
        "managed_containers_after": 0,
        "managed_networks_after": 0,
        "platform": "linux/arm64",
        "private_control_plane": "denied",
        "public_https": "allowed",
        "runtime": "runsc-personal-dev-native",
        "schema": "loom-personal-dev-native-builder-conformance-v1",
        "status": "passed",
    }
    if any(value.get(name) != expected for name, expected in expected_literals.items()):
        raise AuthorityError("state_invalid")
    for name in ("buildkit_sandbox_id", "client_sandbox_id"):
        identifier = value.get(name)
        if not isinstance(identifier, str) or _HEX_64.fullmatch(identifier) is None:
            raise AuthorityError("state_invalid")
    return dict(value)


def _validate_state(value: Mapping[str, object]) -> dict[str, object]:
    if not isinstance(value, Mapping):
        raise AuthorityError("state_invalid")
    copied = dict(value)
    phase = copied.get("phase")
    expected_fields = (
        _PREPARED_STATE_FIELDS if phase == "prepared" else _STAGED_STATE_FIELDS
    )
    if (
        phase not in {"prepared", "staged", "active"}
        or set(copied) != expected_fields
        or copied.get("schema")
        != "loom.personal-dev-native-builder-runtime-authority-state.v1"
    ):
        raise AuthorityError("state_invalid")
    source_sha = _state_string(copied, "authority_source_sha")
    source_tree = _state_string(copied, "authority_source_tree")
    profile_sha = _state_string(copied, "runtime_profile_sha256")
    if (
        _HEX_40.fullmatch(source_sha) is None
        or _HEX_40.fullmatch(source_tree) is None
        or source_sha == source_tree
        or _HEX_64.fullmatch(profile_sha) is None
    ):
        raise AuthorityError("state_invalid")
    current_agent = _state_string(copied, "current_agent")
    current_builder = _state_string(copied, "current_builder")
    current_revision = _state_string(copied, "current_revision")
    previous_agent = _state_string(copied, "previous_agent")
    previous_builder = _state_string(copied, "previous_builder")
    previous_revision = _state_string(copied, "previous_revision")
    if (
        not current_agent.startswith(
            "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
        )
        or _HEX_64.fullmatch(current_agent.rsplit(":", 1)[-1]) is None
        or not current_builder.startswith(
            "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:"
        )
        or _HEX_64.fullmatch(current_builder.rsplit(":", 1)[-1]) is None
        or _HEX_40.fullmatch(current_revision) is None
    ):
        raise AuthorityError("state_invalid")
    previous = (previous_agent, previous_builder, previous_revision)
    if not all(item == "" for item in previous):
        if (
            not all(previous)
            or not previous_agent.startswith(
                "ghcr.io/qianyi-sun/loom-personal-dev-native-builder-agent@sha256:"
            )
            or _HEX_64.fullmatch(previous_agent.rsplit(":", 1)[-1]) is None
            or not previous_builder.startswith(
                "ghcr.io/qianyi-sun/loom-personal-dev-builder@sha256:"
            )
            or _HEX_64.fullmatch(previous_builder.rsplit(":", 1)[-1]) is None
            or _HEX_40.fullmatch(previous_revision) is None
            or previous_revision == current_revision
        ):
            raise AuthorityError("state_invalid")
    origin = _state_string(copied, "public_store_origin")
    if not origin.startswith("https://") or any(character in origin for character in "\r\n\0"):
        raise AuthorityError("state_invalid")
    if phase in {"staged", "active"}:
        instance_id = _state_string(copied, "agent_instance_id")
        key_id = _state_string(copied, "agent_key_id")
        public_key_sha256 = _state_string(copied, "public_key_sha256")
        service_origin = _state_string(copied, "service_origin")
        if (
            re.fullmatch(
                r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
                instance_id,
            )
            is None
            or re.fullmatch(r"[a-z][a-z0-9._-]{0,63}", key_id) is None
            or _HEX_64.fullmatch(public_key_sha256) is None
            or not service_origin.startswith("https://")
            or any(character in service_origin for character in "\r\n\0")
        ):
            raise AuthorityError("state_invalid")
    copied["conformance"] = _validate_conformance(copied.get("conformance"))
    return copied


def encode_state(value: Mapping[str, object]) -> bytes:
    """Validate and encode the exact public state bytes used for transition guards."""
    payload = _canonical_json(_validate_state(value))
    if len(payload) > _MAX_STATE_BYTES:
        raise AuthorityError("state_invalid")
    return payload


def _verify_directory(
    path: Path,
    *,
    expected_uid: int,
    expected_gid: int,
    expected_mode: int,
    error: str,
) -> None:
    descriptor: int | None = None
    try:
        descriptor = _open_directory_no_follow(
            path,
            expected_uid=expected_uid,
            expected_gid=expected_gid,
            expected_mode=expected_mode,
        )
    except OSError as exc:
        raise AuthorityError(error) from exc
    finally:
        if descriptor is not None:
            os.close(descriptor)


class FileStateStore:
    """Publish only canonical public state through an atomic root-private file."""

    def __init__(
        self,
        *,
        root: Path = STATE_ROOT,
        path: Path = STATE_PATH,
        expected_uid: int = 0,
        expected_gid: int = 0,
    ) -> None:
        if path.parent != root:
            raise AuthorityError("state_invalid")
        self.root = root
        self.path = path
        self.expected_uid = expected_uid
        self.expected_gid = expected_gid

    def _open_root(self) -> int:
        try:
            return _open_directory_no_follow(
                self.root,
                expected_uid=self.expected_uid,
                expected_gid=self.expected_gid,
                expected_mode=0o700,
            )
        except OSError as exc:
            raise AuthorityError("state_invalid") from exc

    def _read_at(self, root_descriptor: int) -> StateSnapshot | None:
        descriptor: int | None = None
        try:
            descriptor = os.open(
                self.path.name,
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_descriptor,
            )
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise AuthorityError("state_invalid") from exc
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_uid != self.expected_uid
                or metadata.st_gid != self.expected_gid
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or not 0 < metadata.st_size <= _MAX_STATE_BYTES
            ):
                raise AuthorityError("state_invalid")
            payload = bytearray()
            while len(payload) < metadata.st_size:
                chunk = os.read(descriptor, metadata.st_size - len(payload))
                if not chunk:
                    raise AuthorityError("state_invalid")
                payload.extend(chunk)
            if os.read(descriptor, 1) or _file_identity(metadata) != _file_identity(
                os.fstat(descriptor)
            ):
                raise AuthorityError("state_invalid")
            encoded = bytes(payload)
        finally:
            os.close(descriptor)
        try:
            loaded = json.loads(
                encoded.decode("ascii"),
                object_pairs_hook=_unique_object,
                parse_constant=_reject_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise AuthorityError("state_invalid") from exc
        if not isinstance(loaded, dict) or encoded != encode_state(loaded):
            raise AuthorityError("state_invalid")
        return StateSnapshot(
            MappingProxyType(loaded),
            hashlib.sha256(encoded).hexdigest(),
        )

    def read(self) -> StateSnapshot | None:
        root_descriptor = self._open_root()
        try:
            return self._read_at(root_descriptor)
        finally:
            os.close(root_descriptor)

    def _write_temporary(self, root_descriptor: int, payload: bytes) -> str:
        descriptor: int | None = None
        temporary = ""
        try:
            for _ in range(100):
                temporary = f".state-v1.{os.urandom(16).hex()}"
                try:
                    descriptor = os.open(
                        temporary,
                        os.O_WRONLY
                        | os.O_CREAT
                        | os.O_EXCL
                        | os.O_CLOEXEC
                        | getattr(os, "O_NOFOLLOW", 0),
                        0o600,
                        dir_fd=root_descriptor,
                    )
                    break
                except FileExistsError:
                    continue
            if descriptor is None:
                raise AuthorityError("state_publish_failed")
            os.fchmod(descriptor, 0o600)
            os.fchown(descriptor, self.expected_uid, self.expected_gid)
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise AuthorityError("state_publish_failed")
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = None
            return temporary
        except BaseException:
            if descriptor is not None:
                os.close(descriptor)
            if temporary:
                try:
                    os.unlink(temporary, dir_fd=root_descriptor)
                except FileNotFoundError:
                    pass
            raise

    def publish(self, value: Mapping[str, object]) -> StateSnapshot:
        payload = encode_state(value)
        root_descriptor = self._open_root()
        try:
            previous = self._read_at(root_descriptor)
            temporary = self._write_temporary(root_descriptor, payload)
            replaced = False
            try:
                os.replace(
                    temporary,
                    self.path.name,
                    src_dir_fd=root_descriptor,
                    dst_dir_fd=root_descriptor,
                )
                replaced = True
                os.fsync(root_descriptor)
            except BaseException as primary:
                try:
                    if replaced:
                        if previous is None:
                            os.unlink(self.path.name, dir_fd=root_descriptor)
                        else:
                            restored = self._write_temporary(
                                root_descriptor,
                                encode_state(previous.value),
                            )
                            os.replace(
                                restored,
                                self.path.name,
                                src_dir_fd=root_descriptor,
                                dst_dir_fd=root_descriptor,
                            )
                        os.fsync(root_descriptor)
                    else:
                        os.unlink(temporary, dir_fd=root_descriptor)
                except BaseException as cleanup:
                    raise AuthorityError("cleanup_failed") from cleanup
                if isinstance(primary, AuthorityError):
                    raise
                raise AuthorityError("state_publish_failed") from primary
            return StateSnapshot(
                MappingProxyType(dict(value)),
                hashlib.sha256(payload).hexdigest(),
            )
        finally:
            os.close(root_descriptor)

    def remove(self, *, expected_sha256: str) -> None:
        root_descriptor = self._open_root()
        try:
            current = self._read_at(root_descriptor)
            if current is None or current.sha256 != expected_sha256:
                raise AuthorityError("state_changed")
            payload = encode_state(current.value)
            tombstone = ".state-v1.remove"
            moved = False
            removed = False
            try:
                os.rename(
                    self.path.name,
                    tombstone,
                    src_dir_fd=root_descriptor,
                    dst_dir_fd=root_descriptor,
                )
                moved = True
                os.fsync(root_descriptor)
                os.unlink(tombstone, dir_fd=root_descriptor)
                removed = True
                os.fsync(root_descriptor)
            except BaseException as primary:
                try:
                    if moved and not removed:
                        os.rename(
                            tombstone,
                            self.path.name,
                            src_dir_fd=root_descriptor,
                            dst_dir_fd=root_descriptor,
                        )
                        os.fsync(root_descriptor)
                    elif removed:
                        restored = self._write_temporary(root_descriptor, payload)
                        os.replace(
                            restored,
                            self.path.name,
                            src_dir_fd=root_descriptor,
                            dst_dir_fd=root_descriptor,
                        )
                        os.fsync(root_descriptor)
                except BaseException as cleanup:
                    raise AuthorityError("cleanup_failed") from cleanup
                if isinstance(primary, AuthorityError):
                    raise
                raise AuthorityError("state_remove_failed") from primary
        finally:
            os.close(root_descriptor)


@dataclass(frozen=True, slots=True)
class SecretPaths:
    private_key: Path
    ca_file: Path


class EphemeralSecretFiles:
    """Split the one framed payload into two exact root-only temporary files."""

    def __init__(
        self,
        *,
        root: Path = EPHEMERAL_SECRET_ROOT,
        expected_uid: int = 0,
        expected_gid: int = 0,
        private_key_mode: int = 0o400,
    ) -> None:
        self.root = root
        self.expected_uid = expected_uid
        self.expected_gid = expected_gid
        self.private_key_mode = private_key_mode

    def _create(
        self,
        root_descriptor: int,
        name: str,
        payload: bytes,
        mode: int,
    ) -> tuple[int, ...]:
        descriptor: int | None = None
        created_inode: tuple[int, int] | None = None
        try:
            descriptor = os.open(
                name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                mode,
                dir_fd=root_descriptor,
            )
            opened = os.fstat(descriptor)
            created_inode = (opened.st_dev, opened.st_ino)
            os.fchmod(descriptor, mode)
            os.fchown(descriptor, self.expected_uid, self.expected_gid)
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                if written <= 0:
                    raise AuthorityError("secret_stage_invalid")
                view = view[written:]
            os.fsync(descriptor)
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_nlink != 1
                or metadata.st_uid != self.expected_uid
                or metadata.st_gid != self.expected_gid
                or stat.S_IMODE(metadata.st_mode) != mode
                or metadata.st_size != len(payload)
            ):
                raise AuthorityError("secret_stage_invalid")
            return _file_identity(metadata)
        except BaseException as primary:
            if created_inode is not None:
                try:
                    metadata = os.stat(
                        name,
                        dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                    if (
                        (metadata.st_dev, metadata.st_ino) != created_inode
                        or not stat.S_ISREG(metadata.st_mode)
                        or metadata.st_nlink != 1
                    ):
                        raise AuthorityError("secret_cleanup_failed")
                    os.unlink(name, dir_fd=root_descriptor)
                except BaseException as cleanup:
                    raise AuthorityError("secret_cleanup_failed") from cleanup
            if isinstance(primary, AuthorityError):
                raise
            raise AuthorityError("secret_stage_invalid") from primary
        finally:
            if descriptor is not None:
                os.close(descriptor)

    @contextmanager
    def files(
        self,
        payload: bytes,
        *,
        private_key_length: int,
        service_ca_length: int,
        request_id: str,
    ) -> Iterator[SecretPaths]:
        if (
            not isinstance(payload, bytes)
            or private_key_length != 32
            or service_ca_length <= 0
            or len(payload) != private_key_length + service_ca_length
        ):
            raise AuthorityError("secret_stage_invalid")
        try:
            root_descriptor = _open_directory_no_follow(
                self.root,
                expected_uid=self.expected_uid,
                expected_gid=self.expected_gid,
                expected_mode=0o700,
            )
        except OSError as exc:
            raise AuthorityError("secret_stage_invalid") from exc
        private_key_name = f".agent-key-{request_id}"
        ca_file_name = f".service-ca-{request_id}"
        retained_root = Path(f"/proc/self/fd/{root_descriptor}")
        paths = SecretPaths(
            private_key=retained_root / private_key_name,
            ca_file=retained_root / ca_file_name,
        )
        identities: dict[str, tuple[int, ...]] = {}
        try:
            identities[private_key_name] = self._create(
                root_descriptor,
                private_key_name,
                payload[:private_key_length],
                self.private_key_mode,
            )
            identities[ca_file_name] = self._create(
                root_descriptor,
                ca_file_name,
                payload[private_key_length:],
                0o444,
            )
            os.fsync(root_descriptor)
            yield paths
        finally:
            cleanup_failure: BaseException | None = None
            for name in (ca_file_name, private_key_name):
                expected_identity = identities.get(name)
                try:
                    metadata = os.stat(
                        name,
                        dir_fd=root_descriptor,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    if expected_identity is not None and cleanup_failure is None:
                        cleanup_failure = AuthorityError("secret_cleanup_failed")
                    continue
                except OSError as exc:
                    if cleanup_failure is None:
                        cleanup_failure = exc
                    continue
                if (
                    expected_identity is None
                    or _file_identity(metadata) != expected_identity
                    or not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                ):
                    if cleanup_failure is None:
                        cleanup_failure = AuthorityError("secret_cleanup_failed")
                    continue
                try:
                    os.unlink(name, dir_fd=root_descriptor)
                except OSError as exc:
                    if cleanup_failure is None:
                        cleanup_failure = exc
            try:
                os.fsync(root_descriptor)
            except OSError as exc:
                if cleanup_failure is None:
                    cleanup_failure = exc
            os.close(root_descriptor)
            if cleanup_failure is not None:
                raise AuthorityError("secret_cleanup_failed") from cleanup_failure


class RootArchiveCopies:
    """Take one descriptor-bound copy from the fixed operator archive path."""

    def __init__(
        self,
        *,
        root: Path = STATE_ROOT,
        operator_uid: int,
        operator_gid: int,
        root_uid: int = 0,
        root_gid: int = 0,
    ) -> None:
        self.root = root
        self.operator_uid = operator_uid
        self.operator_gid = operator_gid
        self.root_uid = root_uid
        self.root_gid = root_gid

    @contextmanager
    def copy(
        self,
        source: Path,
        *,
        expected_sha512: str,
        request_id: str,
    ) -> Iterator[Path]:
        if _HEX_128.fullmatch(expected_sha512) is None:
            raise AuthorityError("archive_invalid")
        try:
            root_descriptor = _open_directory_no_follow(
                self.root,
                expected_uid=self.root_uid,
                expected_gid=self.root_gid,
                expected_mode=0o700,
            )
        except OSError as exc:
            raise AuthorityError("archive_invalid") from exc
        destination_name = f".archive-{request_id}"
        destination = Path(f"/proc/self/fd/{root_descriptor}") / destination_name
        source_directory_descriptor: int | None = None
        source_descriptor: int | None = None
        destination_descriptor: int | None = None
        destination_inode: tuple[int, int] | None = None
        destination_identity: tuple[int, ...] | None = None
        try:
            source_directory_descriptor = _open_directory_no_follow(
                source.parent,
                expected_uid=self.operator_uid,
                expected_gid=self.operator_gid,
            )
            source_descriptor = os.open(
                source.name,
                os.O_RDONLY | os.O_CLOEXEC | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=source_directory_descriptor,
            )
            before = os.fstat(source_descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_nlink != 1
                or before.st_uid != self.operator_uid
                or before.st_gid != self.operator_gid
                or stat.S_IMODE(before.st_mode) != 0o600
                or not 0 < before.st_size <= _MAX_ARCHIVE_BYTES
            ):
                raise AuthorityError("archive_invalid")
            destination_descriptor = os.open(
                destination_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_CLOEXEC
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=root_descriptor,
            )
            opened_destination = os.fstat(destination_descriptor)
            destination_inode = (
                opened_destination.st_dev,
                opened_destination.st_ino,
            )
            os.fchmod(destination_descriptor, 0o600)
            os.fchown(destination_descriptor, self.root_uid, self.root_gid)
            digest = hashlib.sha512()
            remaining = before.st_size
            while remaining:
                chunk = os.read(source_descriptor, min(1024 * 1024, remaining))
                if not chunk:
                    raise AuthorityError("archive_invalid")
                digest.update(chunk)
                view = memoryview(chunk)
                while view:
                    written = os.write(destination_descriptor, view)
                    if written <= 0:
                        raise AuthorityError("archive_invalid")
                    view = view[written:]
                remaining -= len(chunk)
            if os.read(source_descriptor, 1):
                raise AuthorityError("archive_invalid")
            after = os.fstat(source_descriptor)
            if (
                digest.hexdigest() != expected_sha512
                or _file_identity(before) != _file_identity(after)
            ):
                raise AuthorityError("archive_invalid")
            os.fsync(destination_descriptor)
            destination_identity = _file_identity(os.fstat(destination_descriptor))
            os.close(destination_descriptor)
            destination_descriptor = None
            os.fsync(root_descriptor)
            yield destination
        except AuthorityError:
            raise
        except OSError as exc:
            raise AuthorityError("archive_invalid") from exc
        finally:
            if source_descriptor is not None:
                os.close(source_descriptor)
            if source_directory_descriptor is not None:
                os.close(source_directory_descriptor)
            if destination_descriptor is not None:
                os.close(destination_descriptor)
            try:
                metadata = os.stat(
                    destination_name,
                    dir_fd=root_descriptor,
                    follow_symlinks=False,
                )
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise AuthorityError("archive_cleanup_failed") from exc
            else:
                if (
                    destination_inode is None
                    or (metadata.st_dev, metadata.st_ino) != destination_inode
                    or (
                        destination_identity is not None
                        and _file_identity(metadata) != destination_identity
                    )
                    or not stat.S_ISREG(metadata.st_mode)
                    or metadata.st_nlink != 1
                    or metadata.st_uid != self.root_uid
                    or metadata.st_gid != self.root_gid
                    or stat.S_IMODE(metadata.st_mode) != 0o600
                ):
                    raise AuthorityError("archive_cleanup_failed")
                try:
                    os.unlink(destination_name, dir_fd=root_descriptor)
                    os.fsync(root_descriptor)
                except OSError as exc:
                    raise AuthorityError("archive_cleanup_failed") from exc
            finally:
                os.close(root_descriptor)


class InstallerAdapter(Protocol):
    def preflight(self, archive: Path) -> Mapping[str, object]: ...
    def install(self, archive: Path) -> Mapping[str, object]: ...
    def verify_staged(self) -> Mapping[str, object]: ...
    def stage_agent_authorized(
        self,
        *,
        agent_image: str,
        builder_image: str,
        service_url: str,
        agent_instance_id: str,
        key_id: str,
        private_key: Path,
        ca_file: Path,
        expected_public_key_sha256: str,
    ) -> Mapping[str, object]: ...
    def discard_agent_stage(self) -> None: ...
    def verify_active(self) -> Mapping[str, object]: ...
    def remove(self) -> Mapping[str, object]: ...


class ConvergerOperations(Protocol):
    def plan(self) -> Mapping[str, object]: ...
    def apply(self) -> Mapping[str, object]: ...
    def verify(self) -> Mapping[str, object]: ...


class ConformanceAdapter(Protocol):
    def run(self, inputs: ConformanceInputs) -> Mapping[str, object]: ...


class HostAdapter(Protocol):
    def status(self) -> HostStatus: ...
    def verify_inert(self, *, require_empty: bool) -> HostStatus: ...
    def load_nft(self) -> None: ...
    def delete_nft(self) -> None: ...
    def start_dockerd(self) -> None: ...
    def stop_dockerd(self) -> None: ...
    def start_agent(self) -> None: ...
    def stop_agent(self) -> None: ...


class StateAdapter(Protocol):
    def read(self) -> StateSnapshot | None: ...
    def publish(self, value: Mapping[str, object]) -> StateSnapshot: ...
    def remove(self, *, expected_sha256: str) -> None: ...


class ArchiveAdapter(Protocol):
    def copy(
        self,
        source: Path,
        *,
        expected_sha512: str,
        request_id: str,
    ) -> AbstractContextManager[Path]: ...


class SecretAdapter(Protocol):
    def files(
        self,
        payload: bytes,
        *,
        private_key_length: int,
        service_ca_length: int,
        request_id: str,
    ) -> AbstractContextManager[SecretPaths]: ...


ConvergerFactory = Callable[["NativeBuilderReleaseConfig"], ConvergerOperations]


@contextmanager
def _defer_cleanup_signals() -> Iterator[None]:
    previous: dict[int, object] = {}
    pending: int | None = None
    if threading.current_thread() is threading.main_thread():
        def defer(signum: int, _frame: object) -> None:
            nonlocal pending
            if pending is None:
                pending = signum

        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, defer)
    try:
        yield
    finally:
        for restored_signum, handler in previous.items():
            signal.signal(restored_signum, cast(signal.Handlers, handler))
    if pending is not None:
        handler = previous[pending]
        if handler == signal.SIG_DFL:
            signal.raise_signal(pending)
        elif handler != signal.SIG_IGN and callable(handler):
            cast(Callable[[int, object], object], handler)(pending, None)


class RuntimeAuthority:
    """Dispatch validated typed requests across narrow fixed boundaries."""

    def __init__(
        self,
        *,
        policy: AuthorityPolicy,
        installer: InstallerAdapter,
        converger_factory: ConvergerFactory,
        conformance: ConformanceAdapter,
        host: HostAdapter,
        states: StateAdapter,
        archives: ArchiveAdapter,
        secrets: SecretAdapter,
    ) -> None:
        if not isinstance(policy, AuthorityPolicy):
            raise AuthorityError("policy_invalid")
        self.policy = policy
        self.installer = installer
        self.converger_factory = converger_factory
        self.conformance = conformance
        self.host = host
        self.states = states
        self.archives = archives
        self.secrets = secrets

    def _verify_request_identity(self, request: AuthorityRequest) -> None:
        header = request.header.as_mapping()
        if (
            header.get("authority_source_sha") != self.policy.authority_source_sha
            or header.get("authority_source_tree") != self.policy.authority_source_tree
            or header.get("runtime_profile_sha256")
            != self.policy.runtime_profile_sha256
        ):
            raise AuthorityError("request_identity_invalid")

    def _host_status(self) -> HostStatus:
        return self._validated_host_status(self.host.status())

    def _validated_host_status(self, status_value: HostStatus) -> HostStatus:
        if (
            not isinstance(status_value, HostStatus)
            or status_value.host_name != _EXPECTED_HOST
            or status_value.architecture != _EXPECTED_ARCHITECTURE
            or type(status_value.dockerd_active) is not bool
            or type(status_value.agent_active) is not bool
            or type(status_value.nft_present) is not bool
            or type(status_value.managed_containers) is not int
            or status_value.managed_containers < 0
            or type(status_value.managed_networks) is not int
            or status_value.managed_networks < 0
        ):
            raise AuthorityError("host_identity_invalid")
        return status_value

    def _read_state(self) -> StateSnapshot | None:
        snapshot = self.states.read()
        if snapshot is None:
            return None
        if not isinstance(snapshot, StateSnapshot):
            raise AuthorityError("state_invalid")
        if (
            snapshot.value.get("authority_source_sha")
            != self.policy.authority_source_sha
            or snapshot.value.get("authority_source_tree")
            != self.policy.authority_source_tree
            or snapshot.value.get("runtime_profile_sha256")
            != self.policy.runtime_profile_sha256
        ):
            raise AuthorityError("state_invalid")
        return snapshot

    def _compensate_inert(self) -> None:
        first_failure: BaseException | None = None
        with _defer_cleanup_signals():
            for operation in (
                self.host.stop_agent,
                self.host.stop_dockerd,
                self.host.delete_nft,
            ):
                try:
                    operation()
                except BaseException as exc:
                    if first_failure is None:
                        first_failure = exc
            try:
                self._validated_host_status(
                    self.host.verify_inert(require_empty=True)
                )
            except BaseException as exc:
                if first_failure is None:
                    first_failure = exc
        if first_failure is not None:
            raise AuthorityError("cleanup_failed") from first_failure

    def _prepared_state(
        self,
        header: Mapping[str, object],
        conformance: Mapping[str, object],
    ) -> dict[str, object]:
        state = {
            "authority_source_sha": self.policy.authority_source_sha,
            "authority_source_tree": self.policy.authority_source_tree,
            "conformance": dict(conformance),
            "current_agent": header["current_agent"],
            "current_builder": header["current_builder"],
            "current_revision": header["current_revision"],
            "phase": "prepared",
            "previous_agent": header["previous_agent"],
            "previous_builder": header["previous_builder"],
            "previous_revision": header["previous_revision"],
            "public_store_origin": header["public_store_origin"],
            "runtime_profile_sha256": self.policy.runtime_profile_sha256,
            "schema": "loom.personal-dev-native-builder-runtime-authority-state.v1",
        }
        return _validate_state(state)

    def _release_config(
        self,
        header: Mapping[str, object],
    ) -> NativeBuilderReleaseConfig:
        previous_revision = cast(str, header["previous_revision"])
        return NativeBuilderReleaseConfig(
            current_agent=NativeBuilderReleaseImage(
                reference=cast(str, header["current_agent"]),
                revision=cast(str, header["current_revision"]),
            ),
            current_builder=NativeBuilderReleaseImage(
                reference=cast(str, header["current_builder"]),
                revision=cast(str, header["current_revision"]),
            ),
            previous_agent=(
                NativeBuilderReleaseImage(
                    reference=cast(str, header["previous_agent"]),
                    revision=previous_revision,
                )
                if previous_revision
                else None
            ),
            previous_builder=(
                NativeBuilderReleaseImage(
                    reference=cast(str, header["previous_builder"]),
                    revision=previous_revision,
                )
                if previous_revision
                else None
            ),
        )

    def _prepare(
        self,
        request: AuthorityRequest,
        current: StateSnapshot | None,
    ) -> dict[str, object]:
        if current is not None:
            raise AuthorityError("phase_invalid")
        header = request.header.as_mapping()
        self._validated_host_status(self.host.verify_inert(require_empty=True))
        mutating = False
        final_host: HostStatus | None = None
        try:
            with self.archives.copy(
                Path(cast(str, header["archive_path"])),
                expected_sha512=cast(str, header["archive_sha512"]),
                request_id=cast(str, header["request_id"]),
            ) as private_archive:
                self.installer.preflight(private_archive)
                mutating = True
                self.installer.install(private_archive)
                self.installer.verify_staged()
                self.host.load_nft()
                self.host.start_dockerd()
                converger = self.converger_factory(self._release_config(header))
                first_plan = dict(converger.plan())
                second_plan = dict(converger.plan())
                first_bytes = _canonical_json(first_plan)
                second_bytes = _canonical_json(second_plan)
                if (
                    len(first_bytes) > _MAX_RECEIPT_BYTES
                    or first_bytes != second_bytes
                ):
                    raise AuthorityError("convergence_plan_invalid")
                converger.apply()
                converger.verify()
                conformance = dict(
                    self.conformance.run(
                        ConformanceInputs(
                            builder_image=cast(str, header["current_builder"]),
                            agent_image=cast(str, header["current_agent"]),
                            public_https=cast(str, header["public_store_origin"]),
                        )
                    )
                )
                _validate_conformance(conformance)
                self.host.stop_dockerd()
                self.host.delete_nft()
                final_host = self._validated_host_status(
                    self.host.verify_inert(require_empty=True)
                )
            state = self._prepared_state(header, conformance)
            snapshot = self.states.publish(state)
            if not isinstance(snapshot, StateSnapshot):
                raise AuthorityError("state_invalid")
        except BaseException as primary_failure:
            if mutating:
                try:
                    self._compensate_inert()
                except BaseException as cleanup_failure:
                    if isinstance(cleanup_failure, AuthorityError):
                        raise
                    raise AuthorityError("cleanup_failed") from cleanup_failure
            if isinstance(primary_failure, AuthorityError):
                raise
            raise AuthorityError("transition_failed") from primary_failure
        if final_host is None:
            raise AuthorityError("transition_failed")
        return self._receipt(
            "prepare",
            header.get("request_id"),
            snapshot,
            final_host,
        )

    def _require_state(
        self,
        current: StateSnapshot | None,
        header: Mapping[str, object],
        *,
        phases: frozenset[str],
    ) -> StateSnapshot:
        if current is None or current.value.get("phase") not in phases:
            raise AuthorityError("phase_invalid")
        if header.get("expected_state_sha256") != current.sha256:
            raise AuthorityError("state_changed")
        return current

    def _rollback_agent_stage(self) -> None:
        first_failure: BaseException | None = None
        with _defer_cleanup_signals():
            try:
                self.installer.discard_agent_stage()
            except BaseException as exc:
                first_failure = exc
            try:
                self._compensate_inert()
            except BaseException as exc:
                if first_failure is None:
                    first_failure = exc
        if first_failure is not None:
            raise AuthorityError("cleanup_failed") from first_failure

    def _stage_agent(
        self,
        request: AuthorityRequest,
        current: StateSnapshot | None,
    ) -> dict[str, object]:
        header = request.header.as_mapping()
        prepared = self._require_state(
            current,
            header,
            phases=frozenset({"prepared"}),
        )
        if (
            header.get("agent_image") != prepared.value.get("current_agent")
            or header.get("builder_image") != prepared.value.get("current_builder")
        ):
            raise AuthorityError("release_identity_invalid")
        self._validated_host_status(self.host.verify_inert(require_empty=True))
        mutating = False
        final_host: HostStatus | None = None
        try:
            files_context = self.secrets.files(
                request.payload,
                private_key_length=cast(int, header["private_key_length"]),
                service_ca_length=cast(int, header["service_ca_length"]),
                request_id=cast(str, header["request_id"]),
            )
            with files_context as paths:
                mutating = True
                self.installer.stage_agent_authorized(
                    agent_image=cast(str, header["agent_image"]),
                    builder_image=cast(str, header["builder_image"]),
                    service_url=cast(str, header["service_origin"]),
                    agent_instance_id=cast(str, header["agent_instance_id"]),
                    key_id=cast(str, header["agent_key_id"]),
                    private_key=paths.private_key,
                    ca_file=paths.ca_file,
                    expected_public_key_sha256=cast(
                        str, header["expected_public_key_sha256"]
                    ),
                )
                self.installer.verify_staged()
                final_host = self._validated_host_status(
                    self.host.verify_inert(require_empty=True)
                )
            staged = dict(prepared.value)
            staged.update(
                {
                    "agent_instance_id": header["agent_instance_id"],
                    "agent_key_id": header["agent_key_id"],
                    "phase": "staged",
                    "public_key_sha256": header["expected_public_key_sha256"],
                    "service_origin": header["service_origin"],
                }
            )
            staged = _validate_state(staged)
            snapshot = self.states.publish(staged)
            if not isinstance(snapshot, StateSnapshot):
                raise AuthorityError("state_invalid")
        except BaseException as primary_failure:
            if mutating:
                try:
                    self._rollback_agent_stage()
                except BaseException as cleanup_failure:
                    if isinstance(cleanup_failure, AuthorityError):
                        raise
                    raise AuthorityError("cleanup_failed") from cleanup_failure
            if isinstance(primary_failure, AuthorityError):
                raise
            raise AuthorityError("transition_failed") from primary_failure
        if final_host is None:
            raise AuthorityError("transition_failed")
        return self._receipt(
            "stage-agent",
            header.get("request_id"),
            snapshot,
            final_host,
        )

    def _activate(
        self,
        request: AuthorityRequest,
        current: StateSnapshot | None,
    ) -> dict[str, object]:
        header = request.header.as_mapping()
        staged = self._require_state(
            current,
            header,
            phases=frozenset({"staged"}),
        )
        self._validated_host_status(self.host.verify_inert(require_empty=True))
        mutating = False
        try:
            mutating = True
            self.host.load_nft()
            self.host.start_dockerd()
            self.host.start_agent()
            self.installer.verify_active()
            final_host = self._host_status()
            if (
                not final_host.dockerd_active
                or not final_host.agent_active
                or not final_host.nft_present
                or final_host.managed_containers
                or final_host.managed_networks
            ):
                raise AuthorityError("host_state_invalid")
            active = dict(staged.value)
            active["phase"] = "active"
            active = _validate_state(active)
            snapshot = self.states.publish(active)
            if not isinstance(snapshot, StateSnapshot):
                raise AuthorityError("state_invalid")
        except BaseException as primary_failure:
            if mutating:
                try:
                    self._compensate_inert()
                except BaseException as cleanup_failure:
                    if isinstance(cleanup_failure, AuthorityError):
                        raise
                    raise AuthorityError("cleanup_failed") from cleanup_failure
            if isinstance(primary_failure, AuthorityError):
                raise
            raise AuthorityError("transition_failed") from primary_failure
        return self._receipt(
            "activate",
            header.get("request_id"),
            snapshot,
            final_host,
        )

    def _remove(
        self,
        request: AuthorityRequest,
        current: StateSnapshot | None,
    ) -> dict[str, object]:
        header = request.header.as_mapping()
        existing = self._require_state(
            current,
            header,
            phases=frozenset({"prepared", "staged", "active"}),
        )
        initial_host = self._host_status()
        if initial_host.managed_containers or initial_host.managed_networks:
            raise AuthorityError("managed_objects_present")
        mutating = False
        try:
            mutating = True
            self.host.stop_agent()
            self.host.stop_dockerd()
            self.host.delete_nft()
            removal = dict(self.installer.remove())
            if (
                removal.get("state") != "managed-files-absent"
                or removal.get("retained")
                != "dedicated-image-cache-and-system-identities"
            ):
                raise AuthorityError("removal_receipt_invalid")
            final_host = self._validated_host_status(
                self.host.verify_inert(require_empty=True)
            )
            receipt = self._receipt(
                "remove",
                header.get("request_id"),
                None,
                final_host,
            )
            self.states.remove(expected_sha256=existing.sha256)
        except BaseException as primary_failure:
            if mutating:
                try:
                    self._compensate_inert()
                except BaseException as cleanup_failure:
                    if isinstance(cleanup_failure, AuthorityError):
                        raise
                    raise AuthorityError("cleanup_failed") from cleanup_failure
            if isinstance(primary_failure, AuthorityError):
                raise
            raise AuthorityError("transition_failed") from primary_failure
        return receipt

    def _receipt(
        self,
        operation: str,
        request_id: object,
        snapshot: StateSnapshot | None,
        host: HostStatus,
    ) -> dict[str, object]:
        phase = "inert" if snapshot is None else snapshot.value.get("phase")
        if not isinstance(phase, str):
            raise AuthorityError("state_invalid")
        return {
            "agent_service": "active" if host.agent_active else "inactive",
            "architecture": host.architecture,
            "authority_source_sha": self.policy.authority_source_sha,
            "authority_source_tree": self.policy.authority_source_tree,
            "dockerd_service": "active" if host.dockerd_active else "inactive",
            "executable_new_capacity": 0,
            "host_name": host.host_name,
            "managed_containers": host.managed_containers,
            "managed_networks": host.managed_networks,
            "nft_table": "present" if host.nft_present else "absent",
            "operation": operation,
            "phase": phase,
            "request_id": request_id,
            "runtime_profile_sha256": self.policy.runtime_profile_sha256,
            "schema": _RECEIPT_SCHEMA,
            "state": None if snapshot is None else dict(snapshot.value),
            "state_sha256": "" if snapshot is None else snapshot.sha256,
        }

    def dispatch(self, request: AuthorityRequest) -> dict[str, object]:
        """Dispatch one already-reviewed request without widening its schema."""
        if not isinstance(request, AuthorityRequest):
            raise AuthorityError("request_invalid")
        try:
            encode_request(request.header.as_mapping(), request.payload)
        except (AttributeError, ProtocolError, TypeError, ValueError) as exc:
            raise AuthorityError("request_invalid") from exc
        self._verify_request_identity(request)
        operation = request.header.operation
        snapshot = self._read_state()
        if operation == "status":
            host = self._host_status()
            return self._receipt(
                operation,
                request.header.as_mapping().get("request_id"),
                snapshot,
                host,
            )
        if operation == "prepare":
            return self._prepare(request, snapshot)
        if operation == "stage-agent":
            return self._stage_agent(request, snapshot)
        if operation == "activate":
            return self._activate(request, snapshot)
        if operation == "remove":
            return self._remove(request, snapshot)
        raise AuthorityError("operation_unavailable")


def _build_runtime(
    policy: AuthorityPolicy,
    *,
    operator_uid: int,
    operator_gid: int,
) -> RuntimeAuthority:
    try:
        profile = load_native_builder_runtime_profile(_INSTALLED_PROFILE_PATH)
    except BaseException as exc:
        raise AuthorityError("runtime_profile_invalid") from exc
    if profile.sha256 != policy.runtime_profile_sha256:
        raise AuthorityError("runtime_profile_invalid")
    installer_runner = BoundedSubprocessRunner(
        timeout_seconds=30,
        maximum_output=64 * 1024,
    )
    converger_runner = BoundedSubprocessRunner(
        timeout_seconds=300,
        maximum_output=4 * 1024 * 1024,
    )
    conformance_runner = BoundedSubprocessRunner(
        timeout_seconds=30,
        maximum_output=64 * 1024,
    )
    host_runner = BoundedSubprocessRunner(
        timeout_seconds=30,
        maximum_output=64 * 1024,
    )
    installer = PersonalDevNativeBuilderRuntimeInstaller(
        profile=profile,
        context=NativeBuilderInstallContext(),
        runner=InstallerRunnerAdapter(installer_runner),
    )

    def converger_factory(
        config: NativeBuilderReleaseConfig,
    ) -> ConvergerOperations:
        return create_broker_release_converger(
            config,
            runner=converger_runner,
        )

    return RuntimeAuthority(
        policy=policy,
        installer=installer,
        converger_factory=converger_factory,
        conformance=ConformanceOperations(conformance_runner),
        host=SystemHostAdapter(runner=host_runner),
        states=FileStateStore(),
        archives=RootArchiveCopies(
            operator_uid=operator_uid,
            operator_gid=operator_gid,
        ),
        secrets=EphemeralSecretFiles(private_key_mode=profile.private_key_mode),
    )


def _serve() -> None:
    import pwd

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
    policy = load_policy()
    _load_application_modules()
    with authority_lock():
        request = parse_request(cast(BinaryIO, sys.stdin.buffer))
        runtime = _build_runtime(
            policy,
            operator_uid=operator.pw_uid,
            operator_gid=operator.pw_gid,
        )
        receipt = runtime.dispatch(request)
        sys.stdout.buffer.write(encode_receipt(receipt))
        sys.stdout.buffer.flush()


def main() -> None:
    """Serve one no-argument request and emit only a stable public result."""
    try:
        _serve()
    except BaseException:
        sys.stderr.write("error:authority_failed\n")
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()


__all__ = [
    "ASSET_SPECS",
    "EPHEMERAL_SECRET_ROOT",
    "LIBEXEC_PATH",
    "LIBRARY_ROOT",
    "LOCK_PATH",
    "POLICY_PATH",
    "STATE_PATH",
    "STATE_ROOT",
    "AssetSpec",
    "AuthorityError",
    "AuthorityPolicy",
    "HostStatus",
    "RuntimeAuthority",
    "StateSnapshot",
    "authority_lock",
    "encode_policy",
    "encode_receipt",
    "load_policy",
    "main",
    "sanitize_environment",
    "verify_invocation",
]
