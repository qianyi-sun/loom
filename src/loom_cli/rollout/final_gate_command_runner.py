"""Fixed installed-helper boundary for protected final-gate actions."""

from __future__ import annotations

import json
import stat
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.final_gate_readiness import (
    FINAL_CHECK_IDS,
    PROTECTED_MUTATION_CHECK_IDS,
    FinalGateResult,
)
from loom_cli.rollout.preflight_contract import CheckOperation

FINAL_GATE_HELPER_PATH = Path("/usr/local/libexec/loom-staging-rollout-final-gate")
_MAX_OUTPUT_BYTES = 64 * 1024
_TIMEOUTS = {
    "final.protected-apply": 3600,
    "final.convergence": 1200,
    "final.drift": 300,
    "final.smoke": 900,
    "final.browser": 900,
    "final.summary": 300,
}


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


CommandRunner = Callable[[Sequence[str], Mapping[str, str], int], CommandResult]


@dataclass(frozen=True, slots=True)
class InstalledFinalGateStepRunner:
    """Invoke one allowlisted final action without ambient argv or secrets."""

    service_uid: int
    plan_path: Path
    plan_digest: str
    run: CommandRunner
    executable: Path = FINAL_GATE_HELPER_PATH
    executable_owner_uid: int = 0

    def __post_init__(self) -> None:
        if (
            self.service_uid < 0
            or self.executable_owner_uid < 0
            or not self.executable.is_absolute()
            or ".." in self.executable.parts
            or not self.plan_path.is_absolute()
            or ".." in self.plan_path.parts
            or len(self.plan_digest) != 64
            or any(character not in "0123456789abcdef" for character in self.plan_digest)
        ):
            raise ValueError("installed final gate runner authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        *,
        candidate_sha: str,
        attestation_digest: str,
        mutation_epoch: int,
    ) -> FinalGateResult:
        if check_id not in FINAL_CHECK_IDS or operation is not (
            CheckOperation.APPLY
            if check_id in PROTECTED_MUTATION_CHECK_IDS
            else CheckOperation.VERIFY
        ):
            raise ValueError("installed final gate operation is invalid")
        self._verify_file(self.executable, owner=self.executable_owner_uid, mode=0o755)
        self._verify_file(self.plan_path, owner=self.service_uid, mode=0o600)
        argv = (
            str(self.executable),
            "execute",
            "--check-id",
            check_id,
            "--operation",
            operation.value,
            "--plan",
            str(self.plan_path),
            "--plan-sha256",
            self.plan_digest,
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
            or len(result.stdout.encode()) > _MAX_OUTPUT_BYTES
            or not isinstance(result.stderr, str)
            or result.stderr
        ):
            raise RuntimeError("installed final gate helper failed its output contract")
        record = _strict_json_object(result.stdout)
        expected = {
            "attestation_digest",
            "blockers",
            "candidate_sha",
            "check_id",
            "evidence_digest",
            "observed_epoch",
            "operation",
            "protected_mutation",
            "schema_version",
        }
        blockers = record.get("blockers")
        observed_epoch = record.get("observed_epoch")
        if (
            set(record) != expected
            or record.get("schema_version") != 1
            or record.get("check_id") != check_id
            or record.get("operation") != operation.value
            or record.get("candidate_sha") != candidate_sha
            or record.get("attestation_digest") != attestation_digest
            or type(observed_epoch) is not int
            or observed_epoch < mutation_epoch
            or type(record.get("protected_mutation")) is not bool
            or not isinstance(blockers, dict)
            or not all(
                isinstance(key, str) and isinstance(value, str) for key, value in blockers.items()
            )
            or (result.returncode == 0) is not (not blockers)
        ):
            raise ValueError("installed final gate helper evidence drifted")
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha=candidate_sha,
            attestation_digest=attestation_digest,
            observed_epoch=observed_epoch,
            evidence_digest=str(record["evidence_digest"]),
            protected_mutation=bool(record["protected_mutation"]),
            blockers=blockers,
        )

    @staticmethod
    def _verify_file(path: Path, *, owner: int, mode: int) -> None:
        metadata = path.lstat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != owner
            or stat.S_IMODE(metadata.st_mode) != mode
            or metadata.st_nlink != 1
        ):
            raise ValueError("installed final gate file authority is invalid")


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
        raise ValueError("installed final gate helper returned invalid JSON") from exc
    if not isinstance(loaded, dict):
        raise ValueError("installed final gate helper returned invalid JSON")
    return loaded


__all__ = [
    "FINAL_GATE_HELPER_PATH",
    "CommandResult",
    "CommandRunner",
    "InstalledFinalGateStepRunner",
]
