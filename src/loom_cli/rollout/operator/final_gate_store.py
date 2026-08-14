"""Immutable request/attempt journal for normalized final-gate executions."""

from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from uuid import uuid4

from loom_cli.rollout.preflight_contract import CheckExecution

from .model import validate_safe_identifier

_CHECK_ID_RE = re.compile(r"^[a-z][a-z0-9.-]{2,95}$")
_ENTRY_RE = re.compile(
    r"^(?P<check_id>[a-z][a-z0-9.-]{2,95})(?:@(?P<revision>[1-9][0-9]{0,2}))?\.json$"
)
_PRIVATE_DIRECTORY_MODE = 0o700
_PRIVATE_FILE_MODE = 0o600
_MAX_EXECUTION_BYTES = 256 * 1024
_MAX_REVISIONS_PER_CHECK = 16
_MAX_JOURNAL_ENTRIES = 96
_DURABLE_CHECK_ID = "final.protected-apply"


class FinalGateStoreError(RuntimeError):
    pass


class FinalGateExecutionStore:
    """Append final results while keeping protected apply permanently single-shot."""

    def __init__(
        self,
        state_root: Path,
        *,
        request_id: str,
        attempt_number: int,
        service_uid: int | None = None,
    ) -> None:
        self.state_root = state_root
        self.request_id = validate_safe_identifier(request_id, "request_id")
        self.attempt_number = attempt_number
        self.service_uid = os.geteuid() if service_uid is None else service_uid
        if (
            not state_root.is_absolute()
            or ".." in state_root.parts
            or type(attempt_number) is not int
            or attempt_number < 1
            or self.service_uid < 0
        ):
            raise FinalGateStoreError("final gate store authority is invalid")
        self.attempt_root = (
            state_root / "requests" / self.request_id / "attempts" / str(self.attempt_number)
        )
        self.root = self.attempt_root / "final-gates"

    def ensure(self) -> None:
        _require_directory(self.attempt_root, uid=self.service_uid)
        try:
            self.root.mkdir(mode=_PRIVATE_DIRECTORY_MODE)
        except FileExistsError:
            pass
        except OSError as exc:
            raise FinalGateStoreError("could not create final gate journal") from exc
        _require_directory(self.root, uid=self.service_uid, exact_mode=_PRIVATE_DIRECTORY_MODE)

    def publish(self, execution: CheckExecution) -> Path:
        if execution.tier != 4 or _CHECK_ID_RE.fullmatch(execution.check_id) is None:
            raise FinalGateStoreError("only normalized final-gate evidence may be published")
        self.ensure()
        history = sorted(
            (
                (revision, path, existing)
                for (check_id, revision), (path, existing) in self._read_entries().items()
                if check_id == execution.check_id
            ),
            key=lambda item: item[0],
        )
        if history:
            _revision, latest_path, latest = history[-1]
            if latest == execution:
                return latest_path
            if execution.check_id == _DURABLE_CHECK_ID:
                raise FinalGateStoreError("final gate execution cannot be replaced")
            if len(history) >= _MAX_REVISIONS_PER_CHECK:
                raise FinalGateStoreError("final gate execution revision limit exceeded")
            revision = history[-1][0] + 1
            path = self.root / f"{execution.check_id}@{revision}.json"
        else:
            path = self.root / f"{execution.check_id}.json"
        payload = (
            json.dumps(execution.to_dict(), sort_keys=True, separators=(",", ":")) + "\n"
        ).encode()
        if len(payload) > _MAX_EXECUTION_BYTES:
            raise FinalGateStoreError("final gate execution evidence is too large")

        directory_fd = _open_directory(self.root)
        temporary = f".{path.name}.{uuid4().hex}.tmp"
        created = False
        try:
            fd = os.open(
                temporary,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0),
                _PRIVATE_FILE_MODE,
                dir_fd=directory_fd,
            )
            created = True
            try:
                os.fchmod(fd, _PRIVATE_FILE_MODE)
                _write_all(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)
            os.link(
                temporary,
                path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
                follow_symlinks=False,
            )
            os.unlink(temporary, dir_fd=directory_fd)
            created = False
            os.fsync(directory_fd)
        except FileExistsError:
            if self._read_path(path) != execution:
                raise FinalGateStoreError("final gate execution cannot be replaced") from None
        except OSError as exc:
            raise FinalGateStoreError("could not publish final gate execution") from exc
        finally:
            if created:
                try:
                    os.unlink(temporary, dir_fd=directory_fd)
                except OSError:
                    pass
            os.close(directory_fd)
        return path

    def read_all(self) -> dict[str, CheckExecution]:
        latest: dict[str, tuple[int, CheckExecution]] = {}
        for (check_id, revision), (_path, execution) in self._read_entries().items():
            current = latest.get(check_id)
            if current is None or revision > current[0]:
                latest[check_id] = (revision, execution)
        return {check_id: item[1] for check_id, item in latest.items()}

    def _read_entries(
        self,
    ) -> dict[tuple[str, int], tuple[Path, CheckExecution]]:
        try:
            _require_directory(self.root, uid=self.service_uid, exact_mode=_PRIVATE_DIRECTORY_MODE)
        except FileNotFoundError:
            return {}
        directory_fd = _open_directory(self.root)
        try:
            names = os.listdir(directory_fd)
        except OSError as exc:
            raise FinalGateStoreError("final gate journal is unreadable") from exc
        finally:
            os.close(directory_fd)
        matches = {name: _ENTRY_RE.fullmatch(name) for name in names}
        if len(names) > _MAX_JOURNAL_ENTRIES or any(match is None for match in matches.values()):
            raise FinalGateStoreError("final gate journal contains unsafe entries")
        results: dict[tuple[str, int], tuple[Path, CheckExecution]] = {}
        revisions: dict[str, set[int]] = {}
        for name in sorted(names):
            match = matches[name]
            assert match is not None
            check_id = match.group("check_id")
            raw_revision = match.group("revision")
            revision = 0 if raw_revision is None else int(raw_revision)
            path = self.root / name
            execution = self._read_path(path)
            if execution.check_id != check_id or (check_id == _DURABLE_CHECK_ID and revision != 0):
                raise FinalGateStoreError("final gate journal identity drifted")
            check_revisions = revisions.setdefault(check_id, set())
            check_revisions.add(revision)
            if len(check_revisions) > _MAX_REVISIONS_PER_CHECK:
                raise FinalGateStoreError("final gate execution revision limit exceeded")
            results[(check_id, revision)] = (path, execution)
        if any(found != set(range(len(found))) for found in revisions.values()):
            raise FinalGateStoreError("final gate execution revision history is incomplete")
        return results

    def _read_path(self, path: Path) -> CheckExecution:
        fd = os.open(
            path,
            os.O_RDONLY
            | getattr(os, "O_CLOEXEC", 0)
            | getattr(os, "O_NOFOLLOW", 0)
            | getattr(os, "O_NONBLOCK", 0),
        )
        try:
            metadata = os.fstat(fd)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != _PRIVATE_FILE_MODE
                or metadata.st_nlink != 1
                or metadata.st_size > _MAX_EXECUTION_BYTES
            ):
                raise FinalGateStoreError("final gate execution authority is unsafe")
            payload = os.read(fd, _MAX_EXECUTION_BYTES + 1)
            if len(payload) > _MAX_EXECUTION_BYTES:
                raise FinalGateStoreError("final gate execution evidence is too large")
        finally:
            os.close(fd)
        try:
            raw = json.loads(payload, object_pairs_hook=_reject_duplicate_keys)
            if not isinstance(raw, dict):
                raise ValueError("execution must be an object")
            return CheckExecution.from_dict(raw)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise FinalGateStoreError("final gate execution evidence is invalid") from exc


def _require_directory(path: Path, *, uid: int, exact_mode: int | None = None) -> None:
    metadata = path.lstat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != uid
        or (exact_mode is not None and stat.S_IMODE(metadata.st_mode) != exact_mode)
    ):
        raise FinalGateStoreError("final gate directory authority is unsafe")


def _open_directory(path: Path) -> int:
    return os.open(
        path,
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0),
    )


def _write_all(fd: int, payload: bytes) -> None:
    offset = 0
    while offset < len(payload):
        written = os.write(fd, payload[offset:])
        if written <= 0:  # pragma: no cover - os.write contract
            raise OSError("final gate evidence write made no progress")
        offset += written


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("final gate execution contains duplicate fields")
        result[key] = value
    return result


__all__ = ["FinalGateExecutionStore", "FinalGateStoreError"]
