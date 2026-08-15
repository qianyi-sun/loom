"""Exclusive, append-only, fsync'd executor journal with a SHA-256 chain."""

from __future__ import annotations

import base64
import binascii
import errno
import fcntl
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path
from types import TracebackType

_DIGEST_RE = re.compile(r"[0-9a-f]{64}")
_EVENT_RE = re.compile(r"[a-z][a-z0-9-]{0,62}")
_OBJECT_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}")
_OBJECT_KINDS = frozenset(
    {
        "bootstrap",
        "executor",
        "heartbeat",
        "intent",
        "inventory",
        "job",
        "prepared-revocation",
        "tranche",
    }
)
_ZERO_DIGEST = "0" * 64
_MAX_JOURNAL_BYTES = 64 * 1024 * 1024
_MAX_RECORD_BYTES = 64 * 1024
_MAX_RECORDS = 1_000_000


class JournalError(RuntimeError):
    """Base class for bounded local-journal failures."""


class JournalLockError(JournalError):
    """Another controller-local executor already holds the journal lock."""


class JournalCorruptionError(JournalError):
    """The journal path or hash chain cannot be trusted."""


class JournalRegressionError(JournalError):
    """Local durable state is behind or disagrees with central high-water."""


@dataclass(frozen=True, slots=True)
class JournalHead:
    sequence: int
    digest: str


@dataclass(frozen=True, slots=True)
class JournalRecord:
    schema_version: int
    sequence: int
    previous_digest: str
    event_kind: str
    object_kind: str
    object_id: str
    payload_digest: str
    record_digest: str
    payload_base64: str | None = None

    def payload(self) -> dict[str, object]:
        value: dict[str, object] = {
            "schema_version": self.schema_version,
            "sequence": self.sequence,
            "previous_digest": self.previous_digest,
            "event_kind": self.event_kind,
            "object_kind": self.object_kind,
            "object_id": self.object_id,
            "payload_digest": self.payload_digest,
        }
        if self.schema_version == 2:
            value["payload_base64"] = self.payload_base64
        return value

    def durable_payload(self) -> bytes | None:
        """Return verified schema-v2 request bytes, or none for legacy records."""

        if self.payload_base64 is None:
            return None
        return base64.b64decode(self.payload_base64, validate=True)


def _canonical_bytes(payload: dict[str, object]) -> bytes:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("ascii")


def _record_digest(payload: dict[str, object]) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _open_regular(path: Path, *, create: bool) -> int:
    flags = os.O_RDWR | os.O_CLOEXEC | os.O_NOFOLLOW
    if create:
        flags |= os.O_CREAT
    try:
        descriptor = os.open(path, flags, 0o600)
    except OSError as exc:
        if exc.errno in {errno.ELOOP, errno.EMLINK}:
            raise JournalCorruptionError(f"journal symlink is forbidden: {path}") from exc
        raise JournalCorruptionError(f"cannot open journal path: {path}") from exc
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        os.close(descriptor)
        raise JournalCorruptionError(f"journal path is not a regular file: {path}")
    if metadata.st_uid != os.geteuid():
        os.close(descriptor)
        raise JournalCorruptionError(f"journal path has another owner: {path}")
    if metadata.st_mode & 0o077:
        os.close(descriptor)
        raise JournalCorruptionError(f"journal path permissions are too broad: {path}")
    return descriptor


def _open_private_directory(path: Path) -> int:
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode):
        raise JournalCorruptionError(f"journal directory symlink is forbidden: {path}")
    try:
        descriptor = os.open(
            path,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY,
        )
    except OSError as exc:
        raise JournalCorruptionError(f"cannot open journal directory: {path}") from exc
    metadata = os.fstat(descriptor)
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
        os.close(descriptor)
        raise JournalCorruptionError(f"journal directory owner is invalid: {path}")
    if metadata.st_mode & 0o077:
        os.close(descriptor)
        raise JournalCorruptionError(f"journal directory permissions are too broad: {path}")
    return descriptor


class ExecutorJournal:
    """One controller-local executor's durable command and recovery head."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.lock_path = self.path.with_name(f"{self.path.name}.lock")
        self._lock_fd: int | None = None
        self._journal_fd: int | None = None
        self._records: dict[int, str] = {0: _ZERO_DIGEST}
        self._latest: dict[tuple[str, str], JournalRecord] = {}
        self._head = JournalHead(0, _ZERO_DIGEST)

    @property
    def head(self) -> JournalHead:
        return self._head

    def __enter__(self) -> ExecutorJournal:
        if self._lock_fd is not None:
            raise JournalLockError("executor journal is already open")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        directory_fd = _open_private_directory(self.path.parent)
        try:
            self._lock_fd = _open_regular(self.lock_path, create=True)
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise JournalLockError("another executor holds the local journal lock") from exc
            self._journal_fd = _open_regular(self.path, create=True)
            os.fsync(directory_fd)
            self._load()
        except Exception:
            self.close()
            raise
        finally:
            os.close(directory_fd)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        if self._journal_fd is not None:
            os.close(self._journal_fd)
            self._journal_fd = None
        if self._lock_fd is not None:
            try:
                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._lock_fd)
                self._lock_fd = None

    def _load(self) -> None:
        assert self._journal_fd is not None
        size = os.fstat(self._journal_fd).st_size
        if size > _MAX_JOURNAL_BYTES:
            raise JournalCorruptionError("executor journal exceeds its size bound")
        os.lseek(self._journal_fd, 0, os.SEEK_SET)
        raw = b""
        while len(raw) < size:
            chunk = os.read(self._journal_fd, min(1_048_576, size - len(raw)))
            if not chunk:
                break
            raw += chunk
        if raw and not raw.endswith(b"\n"):
            raise JournalCorruptionError("executor journal contains a torn record")
        self._records = {0: _ZERO_DIGEST}
        self._latest = {}
        prior = _ZERO_DIGEST
        for expected_sequence, encoded in enumerate(raw.splitlines(), start=1):
            if expected_sequence > _MAX_RECORDS or len(encoded) > _MAX_RECORD_BYTES:
                raise JournalCorruptionError("executor journal record bound exceeded")
            try:
                payload = json.loads(encoded.decode("ascii"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise JournalCorruptionError("executor journal record is invalid JSON") from exc
            if not isinstance(payload, dict) or encoded != _canonical_bytes(payload):
                raise JournalCorruptionError("executor journal record is not canonical JSON")
            record = self._validate_record(payload, expected_sequence, prior)
            self._records[record.sequence] = record.record_digest
            self._latest[(record.object_kind, record.object_id)] = record
            prior = record.record_digest
        self._head = JournalHead(len(self._records) - 1, prior)
        os.lseek(self._journal_fd, 0, os.SEEK_END)

    @staticmethod
    def _validate_record(
        value: object,
        expected_sequence: int,
        expected_previous: str,
    ) -> JournalRecord:
        common_fields = {
            "schema_version",
            "sequence",
            "previous_digest",
            "event_kind",
            "object_kind",
            "object_id",
            "payload_digest",
            "record_digest",
        }
        if not isinstance(value, dict):
            raise JournalCorruptionError("executor journal record fields are invalid")
        schema_version = value.get("schema_version")
        fields = (
            common_fields
            if schema_version == 1
            else common_fields | {"payload_base64"}
            if schema_version == 2
            else set()
        )
        if set(value) != fields:
            raise JournalCorruptionError("executor journal record fields are invalid")
        if (
            type(schema_version) is not int
            or schema_version not in {1, 2}
            or type(value["sequence"]) is not int
            or value["sequence"] != expected_sequence
            or value["previous_digest"] != expected_previous
            or not isinstance(value["event_kind"], str)
            or _EVENT_RE.fullmatch(value["event_kind"]) is None
            or not isinstance(value["object_kind"], str)
            or value["object_kind"] not in _OBJECT_KINDS
            or not isinstance(value["object_id"], str)
            or _OBJECT_ID_RE.fullmatch(value["object_id"]) is None
            or not isinstance(value["payload_digest"], str)
            or _DIGEST_RE.fullmatch(value["payload_digest"]) is None
            or not isinstance(value["record_digest"], str)
            or _DIGEST_RE.fullmatch(value["record_digest"]) is None
        ):
            raise JournalCorruptionError("executor journal record binding is invalid")
        payload_base64: str | None = None
        if schema_version == 2:
            encoded_payload = value["payload_base64"]
            if not isinstance(encoded_payload, str):
                raise JournalCorruptionError("executor journal payload encoding is invalid")
            try:
                decoded_payload = base64.b64decode(encoded_payload, validate=True)
            except (ValueError, binascii.Error) as exc:
                raise JournalCorruptionError(
                    "executor journal payload encoding is invalid"
                ) from exc
            if (
                not decoded_payload
                or len(decoded_payload) > _MAX_RECORD_BYTES
                or base64.b64encode(decoded_payload).decode("ascii") != encoded_payload
                or hashlib.sha256(decoded_payload).hexdigest() != value["payload_digest"]
            ):
                raise JournalCorruptionError("executor journal payload digest is invalid")
            payload_base64 = encoded_payload
        payload = {key: value[key] for key in fields - {"record_digest"}}
        if _record_digest(payload) != value["record_digest"]:
            raise JournalCorruptionError("executor journal record digest is invalid")
        return JournalRecord(
            schema_version=schema_version,
            sequence=value["sequence"],
            previous_digest=value["previous_digest"],
            event_kind=value["event_kind"],
            object_kind=value["object_kind"],
            object_id=value["object_id"],
            payload_digest=value["payload_digest"],
            record_digest=value["record_digest"],
            payload_base64=payload_base64,
        )

    def append(
        self,
        event_kind: str,
        payload_digest: str,
        *,
        object_kind: str,
        object_id: str,
        payload: bytes | None = None,
    ) -> JournalRecord:
        if self._journal_fd is None:
            raise JournalLockError("executor journal is not open")
        if _EVENT_RE.fullmatch(event_kind) is None:
            raise ValueError("journal event kind is invalid")
        if _DIGEST_RE.fullmatch(payload_digest) is None:
            raise ValueError("journal payload digest is invalid")
        if object_kind not in _OBJECT_KINDS:
            raise ValueError("journal object kind is invalid")
        if _OBJECT_ID_RE.fullmatch(object_id) is None:
            raise ValueError("journal object identity is invalid")
        if payload is not None and (
            not isinstance(payload, bytes)
            or not payload
            or len(payload) > _MAX_RECORD_BYTES
            or hashlib.sha256(payload).hexdigest() != payload_digest
        ):
            raise ValueError("journal durable payload does not match its digest")
        record_payload: dict[str, object] = {
            "schema_version": 2 if payload is not None else 1,
            "sequence": self._head.sequence + 1,
            "previous_digest": self._head.digest,
            "event_kind": event_kind,
            "object_kind": object_kind,
            "object_id": object_id,
            "payload_digest": payload_digest,
        }
        payload_base64 = None
        if payload is not None:
            payload_base64 = base64.b64encode(payload).decode("ascii")
            record_payload["payload_base64"] = payload_base64
        digest = _record_digest(record_payload)
        encoded = _canonical_bytes({**record_payload, "record_digest": digest}) + b"\n"
        if len(encoded) > _MAX_RECORD_BYTES:
            raise ValueError("journal record exceeds its size bound")
        if self._head.sequence >= _MAX_RECORDS:
            raise JournalError("executor journal exceeds its record bound")
        if os.fstat(self._journal_fd).st_size + len(encoded) > _MAX_JOURNAL_BYTES:
            raise JournalError("executor journal exceeds its size bound")
        offset = 0
        while offset < len(encoded):
            written = os.write(self._journal_fd, encoded[offset:])
            if written == 0:
                raise JournalError("executor journal append made no progress")
            offset += written
        os.fsync(self._journal_fd)
        record = JournalRecord(
            schema_version=2 if payload is not None else 1,
            sequence=self._head.sequence + 1,
            previous_digest=self._head.digest,
            event_kind=event_kind,
            object_kind=object_kind,
            object_id=object_id,
            payload_digest=payload_digest,
            record_digest=digest,
            payload_base64=payload_base64,
        )
        self._records[record.sequence] = record.record_digest
        self._latest[(record.object_kind, record.object_id)] = record
        self._head = JournalHead(record.sequence, record.record_digest)
        return record

    def latest(self, object_kind: str, object_id: str) -> JournalRecord | None:
        """Return the durable latest event for an exact protocol object."""

        return self._latest.get((object_kind, object_id))

    def latest_records(self, object_kind: str) -> tuple[JournalRecord, ...]:
        """Return each durable latest object of one exact kind in sequence order."""

        if object_kind not in _OBJECT_KINDS:
            raise ValueError("journal object kind is invalid")
        return tuple(
            sorted(
                (
                    record
                    for (kind, _object_id), record in self._latest.items()
                    if kind == object_kind
                ),
                key=lambda record: record.sequence,
            )
        )

    def pending_requests(self) -> tuple[JournalRecord, ...]:
        """Return unresolved journal-first commands in durable sequence order."""

        return tuple(
            sorted(
                (
                    record
                    for record in self._latest.values()
                    if record.event_kind.endswith("-requested")
                ),
                key=lambda record: record.sequence,
            )
        )

    def assert_covers(self, central_sequence: int, central_digest: str) -> None:
        if type(central_sequence) is not int or central_sequence < 0:
            raise ValueError("central journal sequence is invalid")
        if _DIGEST_RE.fullmatch(central_digest) is None:
            raise ValueError("central journal digest is invalid")
        if central_sequence > self._head.sequence:
            raise JournalRegressionError("local journal is behind central high-water")
        if self._records.get(central_sequence) != central_digest:
            raise JournalRegressionError("local journal digest disagrees with central state")


__all__ = [
    "ExecutorJournal",
    "JournalCorruptionError",
    "JournalError",
    "JournalHead",
    "JournalLockError",
    "JournalRecord",
    "JournalRegressionError",
]
