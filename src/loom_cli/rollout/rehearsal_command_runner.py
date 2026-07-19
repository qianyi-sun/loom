"""Fixed installed-helper boundary for concrete isolated rehearsal actions."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.rehearsal_action_source import RehearsalPlan
from loom_cli.rollout.rehearsal_journal_backend import RehearsalStepOutcome
from loom_cli.rollout.rehearsal_readiness import REHEARSAL_CHECK_IDS

REHEARSAL_HELPER_PATH = Path("/usr/local/libexec/loom-staging-rollout-rehearsal")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_MAX_STDOUT_BYTES = 64 * 1024
_TIMEOUTS = {
    "rehearsal.namespace": 300,
    "rehearsal.db-clone": 1800,
    "rehearsal.systemd-launch": 120,
    "rehearsal.migration": 900,
    "rehearsal.release": 900,
    "rehearsal.api-smoke": 600,
    "rehearsal.browser": 900,
    "rehearsal.cleanup": 900,
}


class CommandResult(Protocol):
    @property
    def returncode(self) -> int: ...

    @property
    def stdout(self) -> str: ...

    @property
    def stderr(self) -> str: ...


CommandRunner = Callable[[Sequence[str], Mapping[str, str], int], CommandResult]


@dataclass(frozen=True, slots=True)
class InstalledRehearsalStepRunner:
    """Invoke one root-installed, fixed-argv helper with no ambient authority."""

    state_root: Path
    service_uid: int
    run: CommandRunner
    executable: Path = REHEARSAL_HELPER_PATH
    executable_owner_uid: int = 0

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or not self.executable.is_absolute()
            or ".." in self.executable.parts
            or self.service_uid < 0
            or self.executable_owner_uid < 0
        ):
            raise ValueError("installed rehearsal runner authority is invalid")

    def __call__(self, check_id: str, plan: RehearsalPlan) -> RehearsalStepOutcome:
        if check_id not in REHEARSAL_CHECK_IDS:
            raise ValueError("installed rehearsal check identity is invalid")
        self._verify_executable()
        plan_path = self.state_root / plan.resources.namespace / "plan.json"
        self._verify_plan(plan_path, plan)
        argv = (
            str(self.executable),
            "execute",
            "--check-id",
            check_id,
            "--plan",
            str(plan_path),
            "--plan-sha256",
            plan.plan_digest,
        )
        environment = {
            "HOME": "/var/lib/loom-staging-rollout",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            "XDG_RUNTIME_DIR": f"/run/user/{self.service_uid}",
        }
        result = self.run(argv, environment, _TIMEOUTS[check_id])
        if (
            result.returncode not in {0, 1}
            or not isinstance(result.stdout, str)
            or len(result.stdout.encode()) > _MAX_STDOUT_BYTES
            or not isinstance(result.stderr, str)
            or result.stderr
        ):
            raise RuntimeError("installed rehearsal helper failed its output contract")
        record = _strict_json_object(result.stdout)
        expected = {
            "blockers",
            "check_id",
            "cleanup_verified",
            "details",
            "passed",
            "plan_digest",
            "schema_version",
        }
        details = record.get("details")
        blockers = record.get("blockers")
        if (
            set(record) != expected
            or record.get("schema_version") != 1
            or record.get("check_id") != check_id
            or record.get("plan_digest") != plan.plan_digest
            or type(record.get("passed")) is not bool
            or type(record.get("cleanup_verified")) is not bool
            or not isinstance(details, dict)
            or not isinstance(blockers, dict)
            or not all(isinstance(key, str) and isinstance(value, str) for key, value in details.items())
            or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in blockers.items()
            )
            or (result.returncode == 0) is not (record["passed"] is True)
            or (record["cleanup_verified"] is True) != (
                check_id == "rehearsal.cleanup" and record["passed"] is True
            )
        ):
            raise ValueError("installed rehearsal helper evidence drifted")
        return RehearsalStepOutcome(
            passed=bool(record["passed"]),
            details=details,
            blockers=blockers,
            cleanup_verified=bool(record["cleanup_verified"]),
        )

    def _verify_executable(self) -> None:
        try:
            metadata = self.executable.lstat()
        except OSError as exc:
            raise ValueError("installed rehearsal helper is unavailable") from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self.executable_owner_uid
            or stat.S_IMODE(metadata.st_mode) != 0o755
            or metadata.st_nlink != 1
        ):
            raise ValueError("installed rehearsal helper authority is invalid")

    def _verify_plan(self, path: Path, plan: RehearsalPlan) -> None:
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ValueError("installed rehearsal plan is unavailable") from exc
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
            or metadata.st_size > _MAX_STDOUT_BYTES
        ):
            raise ValueError("installed rehearsal plan authority is invalid")
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            before = os.fstat(fd)
            chunks = bytearray()
            while len(chunks) <= _MAX_STDOUT_BYTES:
                chunk = os.read(fd, min(65536, _MAX_STDOUT_BYTES + 1 - len(chunks)))
                if not chunk:
                    break
                chunks.extend(chunk)
            payload = bytes(chunks)
            after = os.fstat(fd)
        finally:
            os.close(fd)
        if (
            len(payload) > _MAX_STDOUT_BYTES
            or before != after
            or _strict_json_object(payload.decode()) != plan.to_record()
            or _sha256(payload.rstrip(b"\n")) != plan.plan_digest
        ):
            raise ValueError("installed rehearsal plan identity drifted")


def _strict_json_object(payload: str) -> dict[str, object]:
    def reject_duplicates(pairs: list[tuple[str, object]]) -> dict[str, object]:
        result: dict[str, object] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        loaded = json.loads(payload, object_pairs_hook=reject_duplicates)
    except (json.JSONDecodeError, ValueError) as exc:
        raise ValueError("installed rehearsal helper returned invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise ValueError("installed rehearsal helper returned invalid JSON")
    return loaded


def _sha256(payload: bytes) -> str:
    digest = hashlib.sha256(payload).hexdigest()
    if _SHA256_RE.fullmatch(digest) is None:  # pragma: no cover - hashlib contract
        raise RuntimeError("sha256 implementation is invalid")
    return digest


__all__ = [
    "REHEARSAL_HELPER_PATH",
    "CommandResult",
    "CommandRunner",
    "InstalledRehearsalStepRunner",
]
