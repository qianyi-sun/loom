"""Atomic controller-local transport for one pool's execution credentials."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol
from uuid import uuid4

from loom_cli.capacity_control_plane import CapacityPoolExecutorBinding

from .protected_controller_prerequisite_component import capacity_executor_image_digest
from .protected_staging_capacity_execution_credentials import ExecutionCredentialBundle

_POOL_IDS = frozenset({"gb10", "oldlab"})
_FILES = frozenset(
    {
        "bearer-token",
        "client-certificate.pem",
        "client-private-key.pem",
        "manager-ca.pem",
        "ownership-private-key",
    }
)
_MAX_FILE_BYTES = 1024 * 1024
_MAX_PAYLOAD_WIRE_BYTES = 8 * 1024 * 1024
_MAX_EVIDENCE_WIRE_BYTES = 64 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_OLDLAB_DOCKER = "/usr/bin/docker"
_INSTALLER = "/opt/loom-capacity-executor-release/payload/installer/install_capacity_executor.py"


@dataclass(frozen=True, slots=True)
class PoolExecutionCredentialPayload:
    pool_id: str
    files: Mapping[str, bytes] = field(repr=False)
    credential_metadata_sha256: Mapping[str, str]

    def __post_init__(self) -> None:
        if (
            self.pool_id not in _POOL_IDS
            or set(self.files) != _FILES
            or any(
                not isinstance(name, str)
                or not isinstance(payload, bytes)
                or not 0 < len(payload) <= _MAX_FILE_BYTES
                for name, payload in self.files.items()
            )
            or set(self.credential_metadata_sha256)
            != {f"pool-executor-{self.pool_id}", f"pool-ownership-{self.pool_id}"}
            or any(
                not isinstance(value, str)
                or _SHA256_RE.fullmatch(value) is None
                or value == "0" * 64
                for value in self.credential_metadata_sha256.values()
            )
        ):
            raise ValueError("pool execution credential payload is invalid")
        object.__setattr__(self, "files", MappingProxyType(dict(sorted(self.files.items()))))
        object.__setattr__(
            self,
            "credential_metadata_sha256",
            MappingProxyType(dict(sorted(self.credential_metadata_sha256.items()))),
        )

    def to_bytes(self) -> bytes:
        return _canonical_json_bytes(
            {
                "credential_metadata_sha256": dict(self.credential_metadata_sha256),
                "files": {
                    name: base64.b64encode(payload).decode("ascii")
                    for name, payload in self.files.items()
                },
                "pool_id": self.pool_id,
                "schema_version": 1,
            }
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> PoolExecutionCredentialPayload:
        value = _canonical_json_object(
            payload,
            max_bytes=_MAX_PAYLOAD_WIRE_BYTES,
            label="pool execution credential payload",
        )
        if (
            set(value)
            != {
                "credential_metadata_sha256",
                "files",
                "pool_id",
                "schema_version",
            }
            or value.get("schema_version") != 1
        ):
            raise ValueError("pool execution credential payload fields are invalid")
        files = value.get("files")
        metadata = value.get("credential_metadata_sha256")
        if (
            not isinstance(files, dict)
            or not isinstance(metadata, dict)
            or any(
                not isinstance(name, str) or not isinstance(item, str)
                for name, item in files.items()
            )
            or any(
                not isinstance(name, str) or not isinstance(item, str)
                for name, item in metadata.items()
            )
        ):
            raise ValueError("pool execution credential payload fields are invalid")
        decoded: dict[str, bytes] = {}
        try:
            for name, encoded in files.items():
                content = base64.b64decode(encoded, validate=True)
                if base64.b64encode(content).decode("ascii") != encoded:
                    raise ValueError
                decoded[name] = content
            result = cls(
                pool_id=value["pool_id"],
                files=decoded,
                credential_metadata_sha256=metadata,
            )
        except (binascii.Error, TypeError, ValueError) as exc:
            raise ValueError("pool execution credential payload is invalid") from exc
        if result.to_bytes() != payload:
            raise ValueError("pool execution credential payload is not canonical")
        return result


@dataclass(frozen=True, slots=True)
class PoolExecutionCredentialEvidence:
    pool_id: str
    file_sha256: Mapping[str, str]
    credential_metadata_sha256: Mapping[str, str]
    uid: int
    gid: int
    directory_mode: int
    file_mode: int

    def __post_init__(self) -> None:
        if (
            self.pool_id not in _POOL_IDS
            or set(self.file_sha256) != _FILES
            or any(
                not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None
                for value in self.file_sha256.values()
            )
            or set(self.credential_metadata_sha256)
            != {f"pool-executor-{self.pool_id}", f"pool-ownership-{self.pool_id}"}
            or any(
                not isinstance(value, str)
                or _SHA256_RE.fullmatch(value) is None
                or value == "0" * 64
                for value in self.credential_metadata_sha256.values()
            )
            or type(self.uid) is not int
            or type(self.gid) is not int
            or min(self.uid, self.gid) < 0
            or self.directory_mode != 0o700
            or self.file_mode != 0o600
        ):
            raise ValueError("pool execution credential evidence is invalid")
        object.__setattr__(
            self,
            "file_sha256",
            MappingProxyType(dict(sorted(self.file_sha256.items()))),
        )
        object.__setattr__(
            self,
            "credential_metadata_sha256",
            MappingProxyType(dict(sorted(self.credential_metadata_sha256.items()))),
        )

    def to_bytes(self) -> bytes:
        return _canonical_json_bytes(
            {
                "credential_metadata_sha256": dict(self.credential_metadata_sha256),
                "directory_mode": self.directory_mode,
                "file_mode": self.file_mode,
                "file_sha256": dict(self.file_sha256),
                "gid": self.gid,
                "pool_id": self.pool_id,
                "schema_version": 1,
                "uid": self.uid,
            }
        )

    @classmethod
    def from_bytes(cls, payload: bytes) -> PoolExecutionCredentialEvidence:
        value = _canonical_json_object(
            payload,
            max_bytes=_MAX_EVIDENCE_WIRE_BYTES,
            label="pool execution credential evidence",
        )
        if (
            set(value)
            != {
                "credential_metadata_sha256",
                "directory_mode",
                "file_mode",
                "file_sha256",
                "gid",
                "pool_id",
                "schema_version",
                "uid",
            }
            or value.get("schema_version") != 1
        ):
            raise ValueError("pool execution credential evidence fields are invalid")
        file_sha256 = value.get("file_sha256")
        metadata = value.get("credential_metadata_sha256")
        if (
            not isinstance(file_sha256, dict)
            or not isinstance(metadata, dict)
            or any(
                not isinstance(name, str) or not isinstance(item, str)
                for name, item in file_sha256.items()
            )
            or any(
                not isinstance(name, str) or not isinstance(item, str)
                for name, item in metadata.items()
            )
        ):
            raise ValueError("pool execution credential evidence fields are invalid")
        try:
            result = cls(
                pool_id=value["pool_id"],
                file_sha256=file_sha256,
                credential_metadata_sha256=metadata,
                uid=value["uid"],
                gid=value["gid"],
                directory_mode=value["directory_mode"],
                file_mode=value["file_mode"],
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("pool execution credential evidence is invalid") from exc
        if result.to_bytes() != payload:
            raise ValueError("pool execution credential evidence is not canonical")
        return result


class ProtectedPoolCredentialTransport(Protocol):
    def observe(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence | None: ...

    def publish(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence: ...


class ControllerCommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> bytes | str: ...

    @property
    def stderr(self) -> bytes | str: ...


ControllerPoolCredentialInvoker = Callable[[str, bytes], ControllerCommandResult]
OldlabControllerRunner = Callable[[Sequence[str], str], ControllerCommandResult]


class GB10PoolCredentialChannel(Protocol):
    @property
    def controller_prerequisite_authority_sha256(self) -> str: ...

    def invoke_pool_credential(
        self,
        operation: str,
        payload: bytes,
    ) -> ControllerCommandResult: ...


@dataclass(frozen=True, slots=True)
class FixedControllerPoolCredentialTransport:
    """Expose only typed observe/publish on one authenticated controller."""

    pool_id: str
    invoke: ControllerPoolCredentialInvoker

    def __post_init__(self) -> None:
        if self.pool_id not in _POOL_IDS or not callable(self.invoke):
            raise ValueError("controller pool credential transport is invalid")

    def observe(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence | None:
        self._validate_payload(payload)
        response = self._invoke("observe-credential", payload.to_bytes())
        if response == b"null\n":
            return None
        return self._decode_evidence(payload, response)

    def publish(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence:
        self._validate_payload(payload)
        response = self._invoke("publish-credential", payload.to_bytes())
        if response == b"null\n":
            raise RuntimeError("controller pool credential operation failed safely")
        return self._decode_evidence(payload, response)

    def _validate_payload(self, payload: PoolExecutionCredentialPayload) -> None:
        if (
            not isinstance(payload, PoolExecutionCredentialPayload)
            or payload.pool_id != self.pool_id
        ):
            raise ValueError("controller pool credential binding is invalid")

    def _invoke(self, operation: str, payload: bytes) -> bytes:
        result = self.invoke(operation, payload)
        try:
            stdout = _wire_bytes(result.stdout)
            stderr = _wire_bytes(result.stderr)
        except (AttributeError, TypeError, UnicodeError) as exc:
            raise RuntimeError("controller pool credential operation failed safely") from exc
        if (
            type(result.returncode) is not int
            or result.returncode != 0
            or stderr
            or not 0 < len(stdout) <= _MAX_EVIDENCE_WIRE_BYTES
        ):
            raise RuntimeError("controller pool credential operation failed safely")
        return stdout

    def _decode_evidence(
        self,
        payload: PoolExecutionCredentialPayload,
        response: bytes,
    ) -> PoolExecutionCredentialEvidence:
        try:
            evidence = PoolExecutionCredentialEvidence.from_bytes(response)
        except ValueError as exc:
            raise RuntimeError("controller pool credential operation failed safely") from exc
        expected_file_sha256 = {
            name: hashlib.sha256(content).hexdigest() for name, content in payload.files.items()
        }
        if (
            evidence.pool_id != self.pool_id
            or evidence.file_sha256 != expected_file_sha256
            or evidence.credential_metadata_sha256 != payload.credential_metadata_sha256
        ):
            raise RuntimeError("controller pool credential operation failed safely")
        return evidence


@dataclass(frozen=True, slots=True)
class FixedOldlabPoolCredentialInvoker:
    """Run only credential operations in OLDLAB1's fixed host namespaces."""

    image: str
    run: OldlabControllerRunner

    def __post_init__(self) -> None:
        try:
            capacity_executor_image_digest(self.image)
        except ValueError as exc:
            raise ValueError("OLDLAB pool credential channel is invalid") from exc
        if not callable(self.run):
            raise ValueError("OLDLAB pool credential channel is invalid")

    @property
    def authority_sha256(self) -> str:
        value = {
            "channel": "docker-host-pid-v1",
            "controller_hostname": "TRT-EAI-OLDLAB-1",
            "docker": _OLDLAB_DOCKER,
            "image": self.image,
            "installer": _INSTALLER,
            "pool_id": "oldlab",
            "schema_version": 1,
        }
        return hashlib.sha256(_canonical_json_bytes(value)).hexdigest()

    def __call__(self, operation: str, payload: bytes) -> ControllerCommandResult:
        if operation not in {"observe-credential", "publish-credential"}:
            raise ValueError("OLDLAB pool credential operation is invalid")
        try:
            request = PoolExecutionCredentialPayload.from_bytes(payload)
            input_payload = payload.decode("ascii")
        except (UnicodeDecodeError, ValueError) as exc:
            raise ValueError("OLDLAB pool credential request is invalid") from exc
        if request.pool_id != "oldlab":
            raise ValueError("OLDLAB pool credential request is invalid")
        argv = (
            _OLDLAB_DOCKER,
            "run",
            "--rm",
            "--user",
            "0:0",
            "--privileged",
            "--pid=host",
            "--network=none",
            "--read-only",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,noexec,size=64m,mode=0700",
            "--mount",
            "type=bind,src=/,dst=/host,bind-propagation=rslave",
            "--entrypoint",
            "/usr/local/bin/python",
            self.image,
            _INSTALLER,
            "--host-root",
            "/host",
            "--operation",
            operation,
        )
        return self.run(argv, input_payload)


@dataclass(frozen=True, slots=True)
class FixedGB10PoolCredentialTransport:
    """Pin and revalidate the GB10 forced-SSH authority for every operation."""

    controller: GB10PoolCredentialChannel
    authority_sha256: str = field(init=False)

    def __post_init__(self) -> None:
        if not callable(getattr(self.controller, "invoke_pool_credential", None)):
            raise ValueError("GB10 pool credential channel is invalid")
        object.__setattr__(
            self,
            "authority_sha256",
            _authority_sha256(self.controller.controller_prerequisite_authority_sha256),
        )

    def observe(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence | None:
        evidence = self._transport().observe(payload)
        self._require_authority()
        return evidence

    def publish(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence:
        evidence = self._transport().publish(payload)
        self._require_authority()
        return evidence

    def _transport(self) -> FixedControllerPoolCredentialTransport:
        self._require_authority()
        return FixedControllerPoolCredentialTransport(
            pool_id="gb10",
            invoke=self.controller.invoke_pool_credential,
        )

    def _require_authority(self) -> None:
        if (
            _authority_sha256(self.controller.controller_prerequisite_authority_sha256)
            != self.authority_sha256
        ):
            raise ValueError("GB10 pool credential channel authority changed")


def build_fixed_oldlab_pool_credential_transport(
    *,
    image: str,
    run: OldlabControllerRunner,
) -> FixedControllerPoolCredentialTransport:
    invoker = FixedOldlabPoolCredentialInvoker(image=image, run=run)
    return FixedControllerPoolCredentialTransport(pool_id="oldlab", invoke=invoker)


def build_fixed_gb10_pool_credential_transport(
    *,
    controller: GB10PoolCredentialChannel,
) -> FixedGB10PoolCredentialTransport:
    return FixedGB10PoolCredentialTransport(controller=controller)


def pool_execution_credential_payload(
    bundle: ExecutionCredentialBundle,
    *,
    pool_id: str,
) -> PoolExecutionCredentialPayload:
    """Project one validated bundle to only one controller's runtime files."""

    if not isinstance(bundle, ExecutionCredentialBundle) or pool_id not in _POOL_IDS:
        raise ValueError("pool execution credential source is invalid")
    credential = bundle.clients[f"pool-executor-{pool_id}"]
    return PoolExecutionCredentialPayload(
        pool_id=pool_id,
        files={
            "bearer-token": credential.bearer_token,
            "client-certificate.pem": credential.certificate,
            "client-private-key.pem": credential.private_key,
            "manager-ca.pem": credential.manager_ca,
            "ownership-private-key": bundle.ownership_private_keys[pool_id],
        },
        credential_metadata_sha256={
            name: bundle.metadata_sha256[name]
            for name in (f"pool-executor-{pool_id}", f"pool-ownership-{pool_id}")
        },
    )


def local_pool_credential_transport_from_binding(
    binding: CapacityPoolExecutorBinding,
    *,
    service_gid: int,
) -> FixedLocalPoolCredentialTransport:
    """Bind transport destinations to the exact validated executor profile."""

    if not isinstance(binding, CapacityPoolExecutorBinding) or service_gid < 0:
        raise ValueError("pool credential transport binding is invalid")
    root = Path("/run/loom-capacity-executor") / binding.pool_id
    expected = {
        "bearer_token_file": root / "bearer-token",
        "tls_ca_file": root / "manager-ca.pem",
        "tls_certificate_file": root / "client-certificate.pem",
        "tls_private_key_file": root / "client-private-key.pem",
        "ownership_key_file": root / "ownership-private-key",
    }
    if any(Path(getattr(binding, field)) != path for field, path in expected.items()):
        raise ValueError("pool credential profile paths are invalid")
    return FixedLocalPoolCredentialTransport(
        pool_id=binding.pool_id,
        target_directory=root,
        service_uid=binding.local_uid,
        service_gid=service_gid,
    )


@dataclass(frozen=True, slots=True)
class FixedLocalPoolCredentialTransport:
    """Publish exact private files without replacing any existing inode."""

    pool_id: str
    target_directory: Path
    service_uid: int
    service_gid: int

    def __post_init__(self) -> None:
        if (
            self.pool_id not in _POOL_IDS
            or not self.target_directory.is_absolute()
            or ".." in self.target_directory.parts
            or min(self.service_uid, self.service_gid) < 0
            or self.target_directory.name != self.pool_id
        ):
            raise ValueError("local pool credential transport is invalid")

    def observe(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence | None:
        self._validate_payload(payload)
        try:
            self._validate_directory(self.target_directory)
        except FileNotFoundError:
            return None
        names = {path.name for path in self.target_directory.iterdir()}
        if not names <= _FILES:
            raise ValueError("local pool credential directory contains unexpected files")
        observed: dict[str, str] = {}
        for name in sorted(names):
            content = self._read_private(self.target_directory / name)
            if content != payload.files[name]:
                raise ValueError("local pool credential differs from its source")
            observed[name] = hashlib.sha256(content).hexdigest()
        if names != _FILES:
            return None
        return PoolExecutionCredentialEvidence(
            pool_id=self.pool_id,
            file_sha256=observed,
            credential_metadata_sha256=payload.credential_metadata_sha256,
            uid=self.service_uid,
            gid=self.service_gid,
            directory_mode=0o700,
            file_mode=0o600,
        )

    def publish(
        self,
        payload: PoolExecutionCredentialPayload,
    ) -> PoolExecutionCredentialEvidence:
        self._validate_payload(payload)
        try:
            self._validate_directory(self.target_directory)
        except FileNotFoundError:
            self._create_directory()
        current = self.observe(payload)
        if current is not None:
            return current
        for name, content in payload.files.items():
            path = self.target_directory / name
            try:
                self._read_private(path)
            except FileNotFoundError:
                self._publish_file(name, content)
            else:
                if self._read_private(path) != content:
                    raise ValueError("local pool credential differs from its source")
        published = self.observe(payload)
        if published is None:
            raise RuntimeError("local pool credential publication was incomplete")
        return published

    def _validate_payload(self, payload: PoolExecutionCredentialPayload) -> None:
        if (
            not isinstance(payload, PoolExecutionCredentialPayload)
            or payload.pool_id != self.pool_id
        ):
            raise ValueError("local pool credential payload binding is invalid")

    def _create_directory(self) -> None:
        parent = self.target_directory.parent
        metadata = parent.stat(follow_symlinks=False)
        if not stat.S_ISDIR(metadata.st_mode):
            raise ValueError("local pool credential parent is unsafe")
        try:
            os.mkdir(self.target_directory, 0o700)
            os.chmod(self.target_directory, 0o700, follow_symlinks=False)
            os.chown(
                self.target_directory,
                self.service_uid,
                self.service_gid,
                follow_symlinks=False,
            )
        except FileExistsError:
            pass
        self._validate_directory(self.target_directory)
        _fsync_directory(parent)

    def _validate_directory(self, path: Path) -> None:
        metadata = path.stat(follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != self.service_uid
            or metadata.st_gid != self.service_gid
        ):
            raise ValueError("local pool credential directory is unsafe")

    def _read_private(self, path: Path) -> bytes:
        descriptor = os.open(
            path,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_uid != self.service_uid
                or before.st_gid != self.service_gid
                or before.st_nlink != 1
                or not 0 < before.st_size <= _MAX_FILE_BYTES
            ):
                raise ValueError("local pool credential file is unsafe")
            chunks: list[bytes] = []
            remaining = before.st_size
            while remaining:
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    raise ValueError("local pool credential changed while reading")
                chunks.append(chunk)
                remaining -= len(chunk)
            after = os.fstat(descriptor)
            if _metadata_identity(before) != _metadata_identity(after):
                raise ValueError("local pool credential changed while reading")
            return b"".join(chunks)
        finally:
            os.close(descriptor)

    def _publish_file(self, name: str, payload: bytes) -> None:
        directory_fd = os.open(
            self.target_directory,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        temporary = f".{name}.{uuid4().hex}.tmp"
        created = False
        try:
            descriptor = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            created = True
            try:
                os.fchmod(descriptor, 0o600)
                os.fchown(descriptor, self.service_uid, self.service_gid)
                offset = 0
                while offset < len(payload):
                    written = os.write(descriptor, payload[offset:])
                    if written < 1:
                        raise RuntimeError("local pool credential write was incomplete")
                    offset += written
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            try:
                os.link(
                    temporary,
                    name,
                    src_dir_fd=directory_fd,
                    dst_dir_fd=directory_fd,
                    follow_symlinks=False,
                )
            except FileExistsError:
                if self._read_private(self.target_directory / name) != payload:
                    raise ValueError("local pool credential publication raced with drift") from None
            os.unlink(temporary, dir_fd=directory_fd)
            created = False
            os.fsync(directory_fd)
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)


def _canonical_json_bytes(value: object) -> bytes:
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


def _canonical_json_object(
    payload: bytes,
    *,
    max_bytes: int,
    label: str,
) -> dict[str, Any]:
    if not isinstance(payload, bytes) or not 0 < len(payload) <= max_bytes:
        raise ValueError(f"{label} bytes are invalid")
    try:
        value = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError(f"{label} bytes are invalid") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != payload:
        raise ValueError(f"{label} bytes are not canonical")
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("pool execution credential JSON contains duplicate fields")
        value[key] = item
    return value


def _wire_bytes(value: bytes | str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("ascii")
    raise TypeError("controller pool credential response is invalid")


def _authority_sha256(value: object) -> str:
    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError("GB10 pool credential channel authority is invalid")
    return value


def _metadata_identity(value: os.stat_result) -> tuple[int, ...]:
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


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_DIRECTORY", 0),
    )
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


__all__ = [
    "ControllerCommandResult",
    "ControllerPoolCredentialInvoker",
    "FixedControllerPoolCredentialTransport",
    "FixedGB10PoolCredentialTransport",
    "FixedLocalPoolCredentialTransport",
    "FixedOldlabPoolCredentialInvoker",
    "GB10PoolCredentialChannel",
    "OldlabControllerRunner",
    "PoolExecutionCredentialEvidence",
    "PoolExecutionCredentialPayload",
    "ProtectedPoolCredentialTransport",
    "build_fixed_gb10_pool_credential_transport",
    "build_fixed_oldlab_pool_credential_transport",
    "local_pool_credential_transport_from_binding",
    "pool_execution_credential_payload",
]
