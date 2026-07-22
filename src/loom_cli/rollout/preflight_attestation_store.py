"""Private immutable persistence for reusable staged-preflight attestations."""

from __future__ import annotations

import json
import os
import re
import stat
from collections.abc import Mapping
from pathlib import Path
from uuid import uuid4

from loom_cli.rollout.credential_authority import read_trusted_file
from loom_cli.rollout.preflight_contract import PreflightAttestation

_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700
_MAX_ATTESTATION_BYTES = 2 * 1024 * 1024
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class PreflightAttestationStoreError(RuntimeError):
    """Raised when an attestation cannot be persisted or verified safely."""


def _validate_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise PreflightAttestationStoreError(f"{label} is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE
    ):
        raise PreflightAttestationStoreError(f"{label} authority is unsafe")


def _ensure_directory(path: Path, label: str, *, parents: bool = False) -> None:
    created = False
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE, parents=parents)
        created = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise PreflightAttestationStoreError(f"could not create {label}") from exc
    if created:
        try:
            path.chmod(_PRIVATE_DIRECTORY_MODE)
            _fsync_directory(path.parent)
        except OSError as exc:
            raise PreflightAttestationStoreError(f"could not finalize {label}") from exc
    _validate_directory(path, label)


def _fsync_directory(path: Path) -> None:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _json_bytes(payload: Mapping[str, object]) -> bytes:
    return (
        json.dumps(dict(payload), ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


class PreflightAttestationStore:
    """Publish exact digest-addressed attestations without replacement."""

    def __init__(self, state_root: Path | str) -> None:
        self.state_root = Path(state_root)
        if not self.state_root.is_absolute():
            raise PreflightAttestationStoreError("attestation state root must be absolute")
        self.root = self.state_root / "preflight-attestations"

    def _ensure(self) -> None:
        _ensure_directory(self.state_root, "attestation state root", parents=True)
        _ensure_directory(self.root, "preflight attestations directory")

    def publish(self, attestation: PreflightAttestation) -> Path:
        if not isinstance(attestation, PreflightAttestation):
            raise PreflightAttestationStoreError("preflight attestation is invalid")
        try:
            serialized = attestation.to_dict()
            validated = PreflightAttestation.from_dict(serialized)
        except ValueError as exc:
            raise PreflightAttestationStoreError("preflight attestation is invalid") from exc
        if validated != attestation:
            raise PreflightAttestationStoreError("preflight attestation is invalid")
        payload = _json_bytes(serialized)
        if len(payload) > _MAX_ATTESTATION_BYTES:
            raise PreflightAttestationStoreError("preflight attestation is too large")
        self._ensure()
        path = self.root / f"{attestation.attestation_digest}.json"
        try:
            path.lstat()
        except FileNotFoundError:
            pass
        except OSError as exc:
            raise PreflightAttestationStoreError("could not inspect preflight attestation") from exc
        else:
            if self.read(attestation.attestation_digest) != attestation:
                raise PreflightAttestationStoreError("preflight attestation digest collision")
            return path

        directory_fd = os.open(
            self.root,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        temp_name = f".{path.name}.{uuid4().hex}.tmp"
        temp_exists = False
        raced = False
        try:
            flags = (
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
            )
            fd = os.open(temp_name, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
            temp_exists = True
            try:
                os.fchmod(fd, _PRIVATE_FILE_MODE)
                with os.fdopen(fd, "wb", closefd=False) as handle:
                    written = handle.write(payload)
                    if written != len(payload):
                        raise PreflightAttestationStoreError("attestation write was incomplete")
                    handle.flush()
                    os.fsync(handle.fileno())
            finally:
                os.close(fd)
            os.link(
                temp_name,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(temp_name, dir_fd=directory_fd)
            temp_exists = False
            os.fsync(directory_fd)
        except FileExistsError:
            raced = True
        except OSError as exc:
            raise PreflightAttestationStoreError("could not publish preflight attestation") from exc
        finally:
            if temp_exists:
                try:
                    os.unlink(temp_name, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)
        if raced:
            if self.read(attestation.attestation_digest) != attestation:
                raise PreflightAttestationStoreError("preflight attestation digest collision")
        return path

    def read(self, digest: str) -> PreflightAttestation:
        if not isinstance(digest, str) or _SHA256_RE.fullmatch(digest) is None:
            raise PreflightAttestationStoreError("preflight attestation digest is invalid")
        _validate_directory(self.state_root, "attestation state root")
        _validate_directory(self.root, "preflight attestations directory")
        path = self.root / f"{digest}.json"
        try:
            trusted = read_trusted_file(
                path,
                service_uid=os.geteuid(),
                private=True,
                max_bytes=_MAX_ATTESTATION_BYTES,
                require_nonempty=True,
            )
            raw = json.loads(trusted.payload, object_pairs_hook=_reject_duplicate_keys)
            if not isinstance(raw, dict):
                raise ValueError("preflight attestation must be an object")
            attestation = PreflightAttestation.from_dict(raw)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise PreflightAttestationStoreError("preflight attestation is invalid") from exc
        if attestation.attestation_digest != digest:
            raise PreflightAttestationStoreError(
                "preflight attestation digest does not match its path"
            )
        return attestation


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("preflight attestation has duplicate fields")
        result[key] = value
    return result


__all__ = ["PreflightAttestationStore", "PreflightAttestationStoreError"]
