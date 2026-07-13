"""Private, crash-durable persistence for staging rollout requests."""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import stat
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import cast
from uuid import uuid4

from .model import (
    ActivePointer,
    DriverEnvelope,
    RequestEvent,
    RolloutRequest,
    validate_safe_identifier,
)

_PRIVATE_FILE_MODE = 0o600
_PRIVATE_DIRECTORY_MODE = 0o700


class RequestStoreError(RuntimeError):
    """Raised when request persistence cannot be completed safely."""


def _json_bytes(payload: Mapping[str, object]) -> bytes:
    rendered = json.dumps(
        dict(payload),
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return (rendered + "\n").encode("utf-8")


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _load_json_object(payload: bytes, object_name: str) -> dict[str, object]:
    try:
        decoded = payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise RequestStoreError(f"{object_name} must be valid UTF-8") from exc
    try:
        loaded = json.loads(
            decoded,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON value {value!r}")
            ),
        )
    except (json.JSONDecodeError, ValueError) as exc:
        raise RequestStoreError(f"{object_name} is not valid JSON: {exc}") from exc
    if not isinstance(loaded, dict):
        raise RequestStoreError(f"{object_name} must be a JSON object")
    return cast(dict[str, object], loaded)


def _request_id(value: object) -> str:
    try:
        return validate_safe_identifier(value, "request_id")
    except ValueError as exc:
        raise RequestStoreError(str(exc)) from exc


def _attempt_number(value: object) -> int:
    if type(value) is not int or value < 1:
        raise RequestStoreError("attempt_number must be a positive integer")
    return value


def _directory_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, _directory_flags())
    except OSError as exc:
        raise RequestStoreError(f"could not open persistence directory {path}") from exc
    try:
        os.fsync(fd)
    except OSError as exc:
        raise RequestStoreError(f"could not fsync persistence directory {path}") from exc
    finally:
        os.close(fd)


def _validate_private_directory(path: Path, label: str) -> None:
    try:
        metadata = path.lstat()
    except FileNotFoundError as exc:
        raise RequestStoreError(f"{label} does not exist") from exc
    except OSError as exc:
        raise RequestStoreError(f"could not inspect {label}") from exc
    if not stat.S_ISDIR(metadata.st_mode):
        raise RequestStoreError(f"{label} must be a directory, not a symlink")
    if metadata.st_uid != os.geteuid():
        raise RequestStoreError(f"{label} must be owned by the effective service UID")
    if stat.S_IMODE(metadata.st_mode) != _PRIVATE_DIRECTORY_MODE:
        raise RequestStoreError(f"{label} must have mode 0700")


def _ensure_private_directory(path: Path, label: str) -> bool:
    """Ensure a private directory exists and return whether it was created."""
    try:
        path.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
    except FileExistsError:
        _validate_private_directory(path, label)
        return False
    except FileNotFoundError as exc:
        raise RequestStoreError(f"parent directory for {label} does not exist") from exc
    except OSError as exc:
        raise RequestStoreError(f"could not create {label}") from exc
    try:
        path.chmod(_PRIVATE_DIRECTORY_MODE)
        _validate_private_directory(path, label)
        _fsync_directory(path.parent)
    except Exception:
        with contextlib.suppress(OSError):
            path.rmdir()
        raise
    return True


def _read_private_bytes(
    path: Path,
    object_name: str,
    *,
    lock_operation: int | None = None,
) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
    except FileNotFoundError as exc:
        raise RequestStoreError(f"{object_name} does not exist") from exc
    except OSError as exc:
        raise RequestStoreError(f"could not open {object_name} as a private regular file") from exc
    locked = False
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise RequestStoreError(f"{object_name} must be a regular file")
        if metadata.st_uid != os.geteuid():
            raise RequestStoreError(f"{object_name} must be owned by the effective service UID")
        if stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE:
            raise RequestStoreError(f"{object_name} must have mode 0600")
        if lock_operation is not None:
            fcntl.flock(fd, lock_operation)
            locked = True
        with os.fdopen(fd, "rb", closefd=False) as handle:
            return handle.read()
    except OSError as exc:
        raise RequestStoreError(f"could not read {object_name}") from exc
    finally:
        if locked:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _read_json(path: Path, object_name: str) -> dict[str, object]:
    return _load_json_object(_read_private_bytes(path, object_name), object_name)


def _open_parent_directory(path: Path) -> int:
    try:
        return os.open(path, _directory_flags())
    except OSError as exc:
        raise RequestStoreError(f"could not open persistence directory {path}") from exc


def _write_temp_file(directory_fd: int, temp_name: str, payload: bytes) -> None:
    flags = (
        os.O_WRONLY
        | os.O_CREAT
        | os.O_EXCL
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    fd: int | None = None
    try:
        fd = os.open(temp_name, flags, _PRIVATE_FILE_MODE, dir_fd=directory_fd)
        os.fchmod(fd, _PRIVATE_FILE_MODE)
        with os.fdopen(fd, "wb", closefd=True) as handle:
            fd = None
            written = handle.write(payload)
            if written != len(payload):
                raise RequestStoreError("private persistence write was incomplete")
            handle.flush()
            os.fsync(handle.fileno())
    except RequestStoreError:
        raise
    except OSError as exc:
        raise RequestStoreError("could not write and fsync private temporary file") from exc
    finally:
        if fd is not None:
            os.close(fd)


def _publish_immutable(path: Path, payload: Mapping[str, object]) -> Path:
    """Publish with hard-link no-replace semantics and directory durability."""
    directory_fd = _open_parent_directory(path.parent)
    temp_name = f".{path.name}.{uuid4().hex}.tmp"
    temp_exists = False
    try:
        _write_temp_file(directory_fd, temp_name, _json_bytes(payload))
        temp_exists = True
        try:
            os.link(
                temp_name,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
        except FileExistsError as exc:
            raise RequestStoreError(f"{path.name} already exists") from exc
        except OSError as exc:
            raise RequestStoreError(f"could not publish immutable {path.name}") from exc
        os.unlink(temp_name, dir_fd=directory_fd)
        temp_exists = False
        try:
            os.fsync(directory_fd)
        except OSError as exc:
            raise RequestStoreError(f"could not fsync immutable {path.name} directory") from exc
        return path
    finally:
        if temp_exists:
            with contextlib.suppress(OSError):
                os.unlink(temp_name, dir_fd=directory_fd)
                os.fsync(directory_fd)
        os.close(directory_fd)


def _replace_mutable(path: Path, payload: Mapping[str, object]) -> Path:
    directory_fd = _open_parent_directory(path.parent)
    temp_name = f".{path.name}.{uuid4().hex}.tmp"
    temp_exists = False
    try:
        _write_temp_file(directory_fd, temp_name, _json_bytes(payload))
        temp_exists = True
        try:
            os.replace(
                temp_name,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
        except OSError as exc:
            raise RequestStoreError(f"could not atomically replace {path.name}") from exc
        temp_exists = False
        try:
            os.fsync(directory_fd)
        except OSError as exc:
            raise RequestStoreError(f"could not fsync mutable {path.name} directory") from exc
        return path
    finally:
        if temp_exists:
            with contextlib.suppress(OSError):
                os.unlink(temp_name, dir_fd=directory_fd)
                os.fsync(directory_fd)
        os.close(directory_fd)


class RequestStore:
    """Derive all request paths from validated identifiers under one root."""

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root)
        if not self.root.is_absolute():
            raise RequestStoreError("request store root must be absolute")
        self.requests_root = self.root / "requests"
        self.active_path = self.root / "active.json"
        self._active_lock_path = self.root / ".active.lock"

    def _ensure_store(self) -> None:
        if not self.root.exists():
            try:
                self.root.mkdir(parents=True, mode=_PRIVATE_DIRECTORY_MODE)
                self.root.chmod(_PRIVATE_DIRECTORY_MODE)
                _fsync_directory(self.root.parent)
            except OSError as exc:
                raise RequestStoreError("could not create request store root") from exc
        _validate_private_directory(self.root, "request store root")
        _ensure_private_directory(self.requests_root, "requests directory")

    def _request_directory(self, request_id: object) -> Path:
        return self.requests_root / _request_id(request_id)

    def _validate_request_roots(self) -> None:
        try:
            _validate_private_directory(self.root, "request store root")
            _validate_private_directory(self.requests_root, "requests directory")
        except RequestStoreError as exc:
            if str(exc) in {
                "request store root does not exist",
                "requests directory does not exist",
            }:
                raise RequestStoreError("request does not exist") from exc
            raise

    def _require_request_directory(self, request_id: object) -> Path:
        validated_request_id = _request_id(request_id)
        self._validate_request_roots()
        request_directory = self.requests_root / validated_request_id
        try:
            _validate_private_directory(request_directory, "request directory")
            _read_private_bytes(request_directory / "request.json", "request.json")
        except RequestStoreError as exc:
            if "does not exist" in str(exc):
                raise RequestStoreError("request does not exist") from exc
            raise
        return request_directory

    def create_request(self, request: RolloutRequest) -> Path:
        """Persist the immutable pre-backup request without replacement."""
        try:
            payload = request.to_dict()
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        self._ensure_store()
        request_directory = self._request_directory(request.request_id)
        try:
            request_directory.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError as exc:
            raise RequestStoreError(f"request {request.request_id} already exists") from exc
        except OSError as exc:
            raise RequestStoreError("could not create request directory") from exc
        try:
            request_directory.chmod(_PRIVATE_DIRECTORY_MODE)
            _validate_private_directory(request_directory, "request directory")
            _fsync_directory(self.requests_root)
            return _publish_immutable(request_directory / "request.json", payload)
        except Exception:
            # Keep a published or possibly crash-reserved directory. IDs are never reused.
            raise

    def read_request(self, request_id: str) -> RolloutRequest:
        request_directory = self._require_request_directory(request_id)
        payload = _read_json(request_directory / "request.json", "request.json")
        try:
            request = RolloutRequest.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if request.request_id != request_id:
            raise RequestStoreError("request.json request_id does not match its directory")
        return request

    def publish_attempt_envelope(self, envelope: DriverEnvelope) -> Path:
        """Publish the immutable post-backup driver envelope without replacement."""
        try:
            payload = envelope.to_dict()
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        request = self.read_request(envelope.request_id)
        if request.status == "preview":
            raise RequestStoreError("preview request cannot publish a driver envelope")
        request_binding = {
            "request_id": request.request_id,
            "rollout_id": request.rollout_id,
            "initiating_operator": request.caller.username,
            "initiating_uid": request.caller.uid,
            "remote_url": request.candidate.remote_url,
            "target_ref": request.candidate.target_ref,
            "resolved_sha": request.candidate.resolved_sha,
            "image_tag": request.candidate.image_tag,
            "fetched_at": request.candidate.fetched_at,
            "runner_config_sha256": request.runner_config_sha256,
        }
        envelope_binding = {
            "request_id": envelope.request_id,
            "rollout_id": envelope.rollout_id,
            "initiating_operator": envelope.initiating_operator,
            "initiating_uid": envelope.initiating_uid,
            "remote_url": envelope.remote_url,
            "target_ref": envelope.target_ref,
            "resolved_sha": envelope.resolved_sha,
            "image_tag": envelope.image_tag,
            "fetched_at": envelope.fetched_at,
            "runner_config_sha256": envelope.runner_config_sha256,
        }
        if envelope_binding != request_binding:
            raise RequestStoreError("driver envelope does not match immutable request binding")

        if envelope.attempt_number > 1:
            try:
                first = self.read_attempt_envelope(envelope.request_id, 1)
            except RequestStoreError as exc:
                if "does not exist" in str(exc):
                    raise RequestStoreError("first attempt envelope does not exist") from exc
                raise
            first_binding = first.to_dict()
            new_binding = envelope.to_dict()
            for attribution_field in (
                "attempt_number",
                "attempt_operator",
                "attempt_uid",
                "resume",
            ):
                first_binding.pop(attribution_field)
                new_binding.pop(attribution_field)
            if new_binding != first_binding:
                raise RequestStoreError("driver envelope does not match first attempt binding")

        request_directory = self._require_request_directory(envelope.request_id)
        attempts_directory = request_directory / "attempts"
        _ensure_private_directory(attempts_directory, "attempts directory")
        attempt_directory = attempts_directory / str(_attempt_number(envelope.attempt_number))
        _ensure_private_directory(attempt_directory, "attempt directory")
        return _publish_immutable(attempt_directory / "envelope.json", payload)

    def read_attempt_envelope(
        self,
        request_id: str,
        attempt_number: int,
    ) -> DriverEnvelope:
        number = _attempt_number(attempt_number)
        request_directory = self._require_request_directory(request_id)
        attempts_directory = request_directory / "attempts"
        _validate_private_directory(attempts_directory, "attempts directory")
        attempt_directory = attempts_directory / str(number)
        _validate_private_directory(attempt_directory, "attempt directory")
        path = attempt_directory / "envelope.json"
        payload = _read_json(path, "envelope.json")
        try:
            envelope = DriverEnvelope.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        if envelope.request_id != request_id:
            raise RequestStoreError("envelope.json request_id does not match its directory")
        if envelope.attempt_number != number:
            raise RequestStoreError("envelope.json attempt_number does not match its directory")
        return envelope

    def next_attempt_number(self, request_id: str) -> int:
        request_directory = self._require_request_directory(request_id)
        attempts_directory = request_directory / "attempts"
        try:
            attempts_directory.lstat()
        except FileNotFoundError:
            return 1
        except OSError as exc:
            raise RequestStoreError("could not inspect attempts directory") from exc
        _validate_private_directory(attempts_directory, "attempts directory")
        highest = 0
        try:
            entries = list(os.scandir(attempts_directory))
        except OSError as exc:
            raise RequestStoreError("could not inspect attempts directory") from exc
        for entry in entries:
            if not entry.name.isdecimal():
                continue
            number = int(entry.name)
            if number < 1 or not entry.is_dir(follow_symlinks=False):
                raise RequestStoreError("attempt directory contains an unsafe numeric entry")
            highest = max(highest, number)
        return highest + 1

    @contextmanager
    def _active_lock(self) -> Iterator[None]:
        self._ensure_store()
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(self._active_lock_path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise RequestStoreError("could not open active pointer lock") from exc
        locked = False
        try:
            os.fchmod(fd, _PRIVATE_FILE_MODE)
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise RequestStoreError("active pointer lock is not a service-owned regular file")
            fcntl.flock(fd, fcntl.LOCK_EX)
            locked = True
            yield
        except OSError as exc:
            raise RequestStoreError("active pointer lock failed") from exc
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def set_active(self, pointer: ActivePointer) -> Path:
        try:
            payload = pointer.to_dict()
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        with self._active_lock():
            try:
                self.active_path.lstat()
            except FileNotFoundError:
                pass
            except OSError as exc:
                raise RequestStoreError("could not inspect active.json") from exc
            else:
                current = self._read_active_existing()
                current_attempt = (
                    current.request_id,
                    current.attempt_number,
                    current.unit_name,
                )
                requested_attempt = (
                    pointer.request_id,
                    pointer.attempt_number,
                    pointer.unit_name,
                )
                if current_attempt != requested_attempt:
                    raise RequestStoreError("active pointer already belongs to another attempt")
            return _replace_mutable(self.active_path, payload)

    def clear_active_if_matches(self, pointer: ActivePointer) -> bool:
        # Validate before comparing to persisted state.
        pointer.to_dict()
        with self._active_lock():
            try:
                self.active_path.lstat()
            except FileNotFoundError:
                return False
            except OSError as exc:
                raise RequestStoreError("could not inspect active.json") from exc
            current = self._read_active_existing()
            if current != pointer:
                return False
            try:
                self.active_path.unlink()
                _fsync_directory(self.root)
            except OSError as exc:
                raise RequestStoreError("could not clear matching active pointer") from exc
            return True

    def _read_active_existing(self) -> ActivePointer:
        payload = _read_json(self.active_path, "active.json")
        try:
            return ActivePointer.from_dict(payload)
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc

    def read_active(self) -> ActivePointer | None:
        try:
            self.root.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RequestStoreError("could not inspect request store root") from exc
        _validate_private_directory(self.root, "request store root")
        try:
            self.active_path.lstat()
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise RequestStoreError("could not inspect active.json") from exc
        return self._read_active_existing()

    def append_event(self, event: RequestEvent) -> Path:
        """Append one complete JSON line while holding the request event flock."""
        try:
            payload = _json_bytes(event.to_dict())
        except ValueError as exc:
            raise RequestStoreError(str(exc)) from exc
        request_directory = self._require_request_directory(event.request_id)
        path = request_directory / "events.jsonl"
        flags = (
            os.O_WRONLY
            | os.O_APPEND
            | os.O_CREAT
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            fd = os.open(path, flags, _PRIVATE_FILE_MODE)
        except OSError as exc:
            raise RequestStoreError("could not open events.jsonl for append") from exc
        locked = False
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise RequestStoreError("events.jsonl must be a service-owned regular file")
            if stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE:
                raise RequestStoreError("events.jsonl must have mode 0600")
            fcntl.flock(fd, fcntl.LOCK_EX)
            locked = True
            try:
                written = os.write(fd, payload)
                if written != len(payload):
                    raise RequestStoreError("events.jsonl append was incomplete")
                os.fsync(fd)
            except OSError as exc:
                raise RequestStoreError("could not append and fsync events.jsonl") from exc
        finally:
            if locked:
                with contextlib.suppress(OSError):
                    fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        _fsync_directory(request_directory)
        return path

    def read_events(self, request_id: str) -> list[RequestEvent]:
        request_directory = self._require_request_directory(request_id)
        path = request_directory / "events.jsonl"
        try:
            path.lstat()
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise RequestStoreError("could not inspect events.jsonl") from exc
        payload = _read_private_bytes(
            path,
            "events.jsonl",
            lock_operation=fcntl.LOCK_SH,
        )
        if not payload:
            return []
        events: list[RequestEvent] = []
        for line_number, line in enumerate(payload.splitlines(), start=1):
            try:
                raw = _load_json_object(line, f"events.jsonl line {line_number}")
                event = RequestEvent.from_dict(raw)
            except (RequestStoreError, ValueError) as exc:
                raise RequestStoreError(f"events.jsonl line {line_number}: {exc}") from exc
            if event.request_id != request_id:
                raise RequestStoreError(
                    f"events.jsonl line {line_number}: request_id does not match its directory"
                )
            events.append(event)
        return events


__all__ = ["RequestStore", "RequestStoreError"]
