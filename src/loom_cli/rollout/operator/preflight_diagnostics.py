"""Bounded, immutable failure records; never preflight or launch authority."""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import re
import stat
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

MAX_RECORD_BYTES = 16 * 1024
MAX_RECORDS = 1024
STAGES = frozenset(
    {
        "configuration",
        "authorization",
        "dependency-initialization",
        "report",
        "candidate-binding",
        "mutation-epoch",
        "assessment",
        "artifact-reference",
    }
)
CODES = frozenset(
    {
        "preflight-authorization-rejected",
        "preflight-identity-drift",
        "preflight-check-failed",
        "preflight-artifact-publication-failed",
        "preflight-internal-error",
        "preflight-dependency-expired",
        "preflight-not-configured",
    }
)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SHA = re.compile(r"[0-9a-f]{40}\Z")
_CHECK = re.compile(r"[a-z0-9][a-z0-9.-]{0,95}\Z")
_FIELDS = frozenset(
    {
        "schema_version",
        "kind",
        "passed",
        "environment",
        "command",
        "stage",
        "failure_code",
        "check_ids",
        "dependency_ids",
        "candidate_sha",
        "initiator_uid",
        "observed_at",
    }
)


class DiagnosticStoreError(RuntimeError):
    """No arbitrary filesystem error details may cross the public boundary."""


def _validate(record: dict[str, object]) -> None:
    if (
        set(record) != _FIELDS
        or type(record["schema_version"]) is not int
        or record["schema_version"] != 1
        or record["kind"] != "preflight-failure-diagnostic"
        or record["passed"] is not False
        or record["environment"] not in ("dev", "staging", "prod")
        or record["command"] not in ("preflight", "start")
        or record["stage"] not in STAGES
        or record["failure_code"] not in CODES
        or type(record["initiator_uid"]) is not int
        or record["initiator_uid"] < 0
    ):
        raise DiagnosticStoreError("diagnostic schema invalid")
    candidate = record["candidate_sha"]
    if candidate is not None and (not isinstance(candidate, str) or not _SHA.fullmatch(candidate)):
        raise DiagnosticStoreError("diagnostic candidate invalid")
    for key in ("check_ids", "dependency_ids"):
        values = record[key]
        if (
            not isinstance(values, list)
            or len(values) > 64
            or any(not isinstance(value, str) or not _CHECK.fullmatch(value) for value in values)
            or len(set(values)) != len(values)
        ):
            raise DiagnosticStoreError("diagnostic check identity invalid")
    stamp = record["observed_at"]
    if not isinstance(stamp, str) or len(stamp) > 40:
        raise DiagnosticStoreError("diagnostic timestamp invalid")
    try:
        observed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DiagnosticStoreError("diagnostic timestamp invalid") from exc
    if observed.tzinfo is None or observed.utcoffset() is None:
        raise DiagnosticStoreError("diagnostic timestamp invalid")


def _encode(record: dict[str, object]) -> bytes:
    _validate(record)
    payload = (json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(payload) > MAX_RECORD_BYTES:
        raise DiagnosticStoreError("diagnostic size limit exceeded")
    return payload


def _unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise DiagnosticStoreError("diagnostic duplicate key")
        result[key] = value
    return result


class PreflightDiagnosticStore:
    """Private content-addressed records with no deletion or authority semantics."""

    def __init__(self, state_root: Path, *, service_uid: int | None = None) -> None:
        self.state_root = state_root
        self.uid = os.geteuid() if service_uid is None else service_uid

    def _directory(self, *, create: bool) -> int:
        path = self.state_root
        if not path.is_absolute() or ".." in path.parts:
            raise DiagnosticStoreError("diagnostic root invalid")
        flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
        fd = os.open(path.anchor, flags)
        try:
            for part in path.parts[1:]:
                child = os.open(part, flags, dir_fd=fd)
                os.close(fd)
                fd = child
            self._private(os.fstat(fd), directory=True)
            if create:
                try:
                    os.mkdir("preflight-diagnostics", mode=0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
            child = os.open("preflight-diagnostics", flags, dir_fd=fd)
            try:
                self._private(os.fstat(child), directory=True)
            except BaseException:
                os.close(child)
                raise
            return child
        finally:
            os.close(fd)

    def _private(self, info: os.stat_result, *, directory: bool = False) -> None:
        if (
            info.st_uid != self.uid
            or stat.S_IMODE(info.st_mode) != (0o700 if directory else 0o600)
            or not (stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode))
            or (not directory and info.st_nlink != 1)
        ):
            raise DiagnosticStoreError("diagnostic ownership or mode invalid")

    def _read(self, directory: int, digest: str) -> dict[str, object]:
        fd = os.open(
            digest + ".json", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory
        )
        try:
            before = os.fstat(fd)
            self._private(before)
            if before.st_size > MAX_RECORD_BYTES:
                raise DiagnosticStoreError("diagnostic size limit exceeded")
            with os.fdopen(fd, "rb", closefd=False) as stream:
                payload = stream.read(MAX_RECORD_BYTES + 1)
            after = os.fstat(fd)

            def identity(info: os.stat_result) -> tuple[int, ...]:
                return (
                    info.st_dev,
                    info.st_ino,
                    info.st_uid,
                    info.st_gid,
                    info.st_mode,
                    info.st_nlink,
                    info.st_size,
                    info.st_mtime_ns,
                    info.st_ctime_ns,
                )

            if identity(after) != identity(before) or len(payload) > MAX_RECORD_BYTES:
                raise DiagnosticStoreError("diagnostic changed while reading")
        finally:
            os.close(fd)
        if hashlib.sha256(payload).hexdigest() != digest:
            raise DiagnosticStoreError("diagnostic digest mismatch")
        try:
            record = json.loads(payload, object_pairs_hook=_unique)
            if not isinstance(record, dict) or _encode(record) != payload:
                raise DiagnosticStoreError("diagnostic record invalid")
        except (ValueError, TypeError, KeyError) as exc:
            raise DiagnosticStoreError("diagnostic record invalid") from exc
        return record

    def read(self, digest: str) -> dict[str, object]:
        if not _SHA256.fullmatch(digest):
            raise DiagnosticStoreError("diagnostic identifier invalid")
        fd = self._directory(create=False)
        try:
            return self._read(fd, digest)
        finally:
            os.close(fd)

    def publish(self, record: dict[str, object]) -> str:
        payload = _encode(record)
        digest = hashlib.sha256(payload).hexdigest()
        directory = self._directory(create=True)
        temporary = "." + uuid4().hex + ".tmp"
        created = False
        try:
            # Lock the directory itself: no separate lock-file/path authority.
            fcntl.flock(directory, fcntl.LOCK_EX)
            names = os.listdir(directory)
            if digest + ".json" in names:
                if self._read(directory, digest) != record:
                    raise DiagnosticStoreError("diagnostic collision")
                return digest
            if len(names) >= MAX_RECORDS:
                raise DiagnosticStoreError("diagnostic capacity exhausted")
            fd = os.open(
                temporary,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
                dir_fd=directory,
            )
            created = True
            try:
                os.fchmod(fd, 0o600)
                with os.fdopen(fd, "wb", closefd=False) as stream:
                    stream.write(payload)
                    stream.flush()
                    os.fsync(fd)
            finally:
                os.close(fd)
            os.link(
                temporary,
                digest + ".json",
                src_dir_fd=directory,
                dst_dir_fd=directory,
                follow_symlinks=False,
            )
            os.unlink(temporary, dir_fd=directory)
            created = False
            os.fsync(directory)
            return digest
        finally:
            if created:
                with contextlib.suppress(OSError):
                    os.unlink(temporary, dir_fd=directory)
            os.close(directory)


@dataclass
class PreflightDiagnosticContext:
    environment: str
    command: str
    stage: str = "configuration"
    initiator_uid: int | None = None
    candidate_sha: str | None = None
    store: PreflightDiagnosticStore | None = None
    active: bool = True

    def failure(
        self, code: str, *, check_ids: tuple[str, ...] = (), dependency_ids: tuple[str, ...] = ()
    ) -> dict[str, object]:
        result: dict[str, object] = {
            "passed": False,
            "assessment_complete": False,
            "failure_code": code,
            "stage": self.stage,
            "diagnostic_recorded": False,
            "checks": [{"name": code, "passed": False}],
        }
        if code == "preflight-authorization-rejected":
            result["message"] = "preflight requires operator/coordinator authority"
        elif code == "preflight-not-configured":
            result["message"] = "deep rollout preflight is not configured"
        if self.store is None or self.initiator_uid is None or self.stage == "authorization":
            return result
        record: dict[str, object] = {
            "schema_version": 1,
            "kind": "preflight-failure-diagnostic",
            "passed": False,
            "environment": self.environment,
            "command": self.command,
            "stage": self.stage,
            "failure_code": code,
            "check_ids": list(check_ids),
            "dependency_ids": list(dependency_ids),
            "candidate_sha": self.candidate_sha,
            "initiator_uid": self.initiator_uid,
            "observed_at": datetime.now(UTC).isoformat(),
        }
        try:
            result["diagnostic_sha256"] = self.store.publish(record)
            result["diagnostic_recorded"] = True
        except Exception:
            # A secondary persistence failure must neither hide the primary code
            # nor advertise a durable object that was not verified published.
            result["diagnostic_failure_code"] = "preflight-diagnostic-publication-failed"
        return result
