"""Restart-safe immutable journal around isolated rehearsal execution."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from loom_cli.rollout.rehearsal_action_source import (
    RehearsalObservation,
    RehearsalPlan,
)
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_CHECK_ORDINAL = {check_id: index for index, check_id in enumerate(REHEARSAL_CHECK_IDS, 1)}
_MAX_RECORD_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class RehearsalStepOutcome:
    """Bounded, secret-free result from one concrete isolated step runner."""

    passed: bool
    details: Mapping[str, str]
    blockers: Mapping[str, str]
    cleanup_verified: bool = False

    def __post_init__(self) -> None:
        details = _bounded_map(self.details, label="rehearsal details")
        blockers = _bounded_map(self.blockers, label="rehearsal blockers")
        if self.cleanup_verified and not self.passed:
            raise ValueError("failed rehearsal cleanup cannot claim verification")
        if self.passed == bool(blockers):
            raise ValueError("rehearsal outcome and blockers are inconsistent")
        object.__setattr__(self, "details", MappingProxyType(details))
        object.__setattr__(self, "blockers", MappingProxyType(blockers))


RehearsalStepRunner = Callable[[str, RehearsalPlan], RehearsalStepOutcome]


@dataclass(frozen=True, slots=True)
class JournaledRehearsalBackend:
    """Execute each plan step at most once and publish its exact terminal record."""

    state_root: Path
    service_uid: int
    run_step: RehearsalStepRunner

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
        ):
            raise ValueError("rehearsal journal authority is invalid")

    def execute(self, check_id: str, plan: RehearsalPlan) -> RehearsalObservation:
        if check_id not in _CHECK_ORDINAL:
            raise ValueError("rehearsal journal check identity is invalid")
        plan.resources.require_isolated()
        directory = self._open_plan_directory(plan)
        try:
            filename = _record_filename(check_id)
            existing = _read_record(directory, filename, service_uid=self.service_uid)
            if existing is not None:
                return _observation(existing, check_id=check_id, plan=plan)
            try:
                outcome = self.run_step(check_id, plan)
            except (OSError, RuntimeError, ValueError):
                outcome = RehearsalStepOutcome(
                    passed=False,
                    details={"status": "failed"},
                    blockers={"executor": "isolated-action-failed"},
                )
            if check_id == "rehearsal.cleanup":
                if not outcome.cleanup_verified:
                    outcome = RehearsalStepOutcome(
                        passed=False,
                        details=outcome.details,
                        blockers={**dict(outcome.blockers), "cleanup": "not-verified"},
                    )
            elif outcome.cleanup_verified:
                raise ValueError("cleanup verification is attached to a non-cleanup step")
            record = _record(check_id=check_id, plan=plan, outcome=outcome)
            _publish_record(directory, filename, record, service_uid=self.service_uid)
            published = _read_record(directory, filename, service_uid=self.service_uid)
            if published is None:
                raise RuntimeError("rehearsal journal publication disappeared")
            return _observation(published, check_id=check_id, plan=plan)
        finally:
            os.close(directory)

    def _open_plan_directory(self, plan: RehearsalPlan) -> int:
        _require_private_directory(self.state_root, service_uid=self.service_uid)
        root_fd = os.open(
            self.state_root,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_DIRECTORY", 0)
            | getattr(os, "O_NOFOLLOW", 0),
        )
        name = plan.resources.namespace
        try:
            try:
                os.mkdir(name, mode=0o700, dir_fd=root_fd)
                os.fsync(root_fd)
            except FileExistsError:
                pass
            directory = os.open(
                name,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=root_fd,
            )
        finally:
            os.close(root_fd)
        metadata = os.fstat(directory)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_nlink < 2
        ):
            os.close(directory)
            raise ValueError("rehearsal plan journal directory authority is invalid")
        return directory


def _bounded_map(value: Mapping[str, str], *, label: str) -> dict[str, str]:
    normalized = dict(value)
    if len(normalized) > 64 or any(
        not key
        or len(key) > 96
        or not item
        or len(item) > 256
        or any(ord(character) < 32 for character in key + item)
        for key, item in normalized.items()
    ):
        raise ValueError(f"{label} is invalid")
    return normalized


def _require_private_directory(path: Path, *, service_uid: int) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ValueError("rehearsal journal root is unavailable") from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != service_uid
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or metadata.st_nlink < 2
    ):
        raise ValueError("rehearsal journal root authority is invalid")


def _record_filename(check_id: str) -> str:
    return f"{_CHECK_ORDINAL[check_id]:02d}-{check_id.removeprefix('rehearsal.')}.json"


def _record(
    *,
    check_id: str,
    plan: RehearsalPlan,
    outcome: RehearsalStepOutcome,
) -> dict[str, object]:
    evidence_payload = {
        "check_id": check_id,
        "details": dict(outcome.details),
        "passed": outcome.passed,
        "plan_digest": plan.plan_digest,
        "schema_version": 1,
    }
    evidence_digest = _hash_json(evidence_payload)
    record: dict[str, object] = {
        "blockers": dict(outcome.blockers),
        "candidate_sha": plan.candidate_sha,
        "check_id": check_id,
        "cleanup_verified": outcome.cleanup_verified,
        "evidence_digest": evidence_digest,
        "mutation_epoch": plan.mutation_epoch,
        "passed": outcome.passed,
        "plan_digest": plan.plan_digest,
        "protected_mutation": False,
        "schema_version": 1,
    }
    record["journal_digest"] = _hash_json(record)
    return record


def _publish_record(
    directory: int,
    filename: str,
    record: Mapping[str, object],
    *,
    service_uid: int,
) -> None:
    payload = json.dumps(record, sort_keys=True, separators=(",", ":")).encode() + b"\n"
    if len(payload) > _MAX_RECORD_BYTES:
        raise ValueError("rehearsal journal record exceeds its bound")
    temp = f".{filename}.{os.getpid()}.{os.urandom(8).hex()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_CLOEXEC", 0)
    fd = os.open(temp, flags, 0o600, dir_fd=directory)
    try:
        metadata = os.fstat(fd)
        if metadata.st_uid != service_uid or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise ValueError("rehearsal journal temporary authority is invalid")
        offset = 0
        while offset < len(payload):
            offset += os.write(fd, payload[offset:])
        os.fsync(fd)
        try:
            os.link(temp, filename, src_dir_fd=directory, dst_dir_fd=directory)
        except FileExistsError:
            pass
        os.fsync(directory)
    finally:
        os.close(fd)
        try:
            os.unlink(temp, dir_fd=directory)
        except FileNotFoundError:
            pass
    existing = _read_record(directory, filename, service_uid=service_uid)
    if existing != dict(record):
        raise ValueError("rehearsal journal publication collided")


def _read_record(
    directory: int,
    filename: str,
    *,
    service_uid: int,
) -> dict[str, object] | None:
    try:
        fd = os.open(
            filename,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory,
        )
    except FileNotFoundError:
        return None
    try:
        before = os.fstat(fd)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid != service_uid
            or stat.S_IMODE(before.st_mode) != 0o600
            or before.st_nlink != 1
            or before.st_size <= 0
            or before.st_size > _MAX_RECORD_BYTES
        ):
            raise ValueError("rehearsal journal record authority is invalid")
        chunks = bytearray()
        while len(chunks) <= _MAX_RECORD_BYTES:
            chunk = os.read(fd, min(65536, _MAX_RECORD_BYTES + 1 - len(chunks)))
            if not chunk:
                break
            chunks.extend(chunk)
        payload = bytes(chunks)
        after = os.fstat(fd)
        if len(payload) > _MAX_RECORD_BYTES or any(
            getattr(before, field) != getattr(after, field)
            for field in (
                "st_dev",
                "st_ino",
                "st_mode",
                "st_uid",
                "st_gid",
                "st_nlink",
                "st_size",
                "st_mtime_ns",
                "st_ctime_ns",
            )
        ):
            raise ValueError("rehearsal journal record changed while read")
    finally:
        os.close(fd)
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        loaded_pairs: dict[str, object] = {}
        for key, value in pairs:
            if key in loaded_pairs:
                raise ValueError("duplicate key")
            loaded_pairs[key] = value
        return loaded_pairs

    try:
        loaded = json.loads(payload, object_pairs_hook=reject_duplicates)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("rehearsal journal record is invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise ValueError("rehearsal journal record is invalid")
    return loaded


def _observation(
    record: Mapping[str, object],
    *,
    check_id: str,
    plan: RehearsalPlan,
) -> RehearsalObservation:
    expected = {
        "blockers",
        "candidate_sha",
        "check_id",
        "cleanup_verified",
        "evidence_digest",
        "journal_digest",
        "mutation_epoch",
        "passed",
        "plan_digest",
        "protected_mutation",
        "schema_version",
    }
    blockers = record.get("blockers")
    if (
        set(record) != expected
        or record.get("schema_version") != 1
        or record.get("check_id") != check_id
        or record.get("candidate_sha") != plan.candidate_sha
        or record.get("mutation_epoch") != plan.mutation_epoch
        or record.get("plan_digest") != plan.plan_digest
        or record.get("protected_mutation") is not False
        or type(record.get("passed")) is not bool
        or type(record.get("cleanup_verified")) is not bool
        or not isinstance(blockers, dict)
        or not all(isinstance(key, str) and isinstance(value, str) for key, value in blockers.items())
        or not isinstance(record.get("evidence_digest"), str)
        or _SHA256_RE.fullmatch(str(record["evidence_digest"])) is None
        or not isinstance(record.get("journal_digest"), str)
        or _SHA256_RE.fullmatch(str(record["journal_digest"])) is None
        or record["journal_digest"]
        != _hash_json({key: value for key, value in record.items() if key != "journal_digest"})
    ):
        raise ValueError("rehearsal journal record identity drifted")
    normalized_blockers = _bounded_map(blockers, label="rehearsal blockers")
    if (record["passed"] is True) == bool(normalized_blockers):
        raise ValueError("rehearsal journal outcome is inconsistent")
    return RehearsalObservation(
        check_id=check_id,
        evidence_digest=str(record["evidence_digest"]),
        journal_digest=str(record["journal_digest"]),
        protected_mutation=False,
        cleanup_verified=bool(record["cleanup_verified"]),
        blockers=normalized_blockers,
    )


def _hash_json(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "JournaledRehearsalBackend",
    "RehearsalStepOutcome",
    "RehearsalStepRunner",
]
