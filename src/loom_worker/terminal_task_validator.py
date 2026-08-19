"""Attested worker-owned TerminalTask validation helper boundary."""

from __future__ import annotations

import hashlib
import os
import re
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Literal
from uuid import UUID

from pydantic import StringConstraints, field_validator

from loom.integrations.terminalgen.authority import TERMINALGEN_VALIDATION_POLICY_DIGEST
from loom.pipeline.keys import canonical_digest, canonical_document
from loom.pipeline.spec import Digest, PipelineModel
from loom.pipeline.work_protocol import TerminalTaskValidationGrantV1

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_READ_BYTES = 1024 * 1024


class TerminalTaskValidatorError(RuntimeError):
    """The helper is missing, mutable, untrusted, or failed its closed probe."""


class TerminalTaskValidatorProbeV1(PipelineModel):
    schema_version: Literal["loom.terminal-task-validator-probe.v1"]
    backend: Literal["rootless-buildkit-oci-v1"]
    validation_policy_sha256: Digest
    rootless: Literal[True]
    network_profile: Literal["none"]
    runtime_socket_exposed: Literal[False]
    process_group_isolation: Literal[True]


class TerminalTaskValidatorRunRequestV1(PipelineModel):
    schema_version: Literal["loom.terminal-task-validator-run.v1"]
    execution_attempt_id: UUID
    stage_run_id: UUID
    pipeline_run_id: UUID
    executable_sha256: Digest
    probe_sha256: Digest
    input_view_digest: Digest
    validator_image: str
    validator_argv: list[str]
    validation_grant: TerminalTaskValidationGrantV1
    inputs_root: Annotated[str, StringConstraints(min_length=2, max_length=4096)]
    outputs_root: Annotated[str, StringConstraints(min_length=2, max_length=4096)]
    scratch_root: Annotated[str, StringConstraints(min_length=2, max_length=4096)]
    cgroup_parent: Annotated[str, StringConstraints(min_length=1, max_length=512)]

    @field_validator("inputs_root", "outputs_root", "scratch_root")
    @classmethod
    def absolute_closed_path(cls, value: str) -> str:
        path = Path(value)
        if not path.is_absolute() or any(part in {".", ".."} for part in path.parts):
            raise ValueError("validator workspace path must be absolute and traversal-free")
        return value


@dataclass(frozen=True, slots=True)
class TerminalTaskValidatorAttestation:
    executable: Path
    executable_sha256: str
    probe: TerminalTaskValidatorProbeV1
    probe_sha256: str


def attest_terminal_task_validator(
    executable: Path,
    expected_sha256: str,
    *,
    expected_owner_uid: int = 0,
) -> TerminalTaskValidatorAttestation:
    """Verify one immutable helper and its exact no-network/rootless probe."""

    if not executable.is_absolute() or executable != Path(os.path.abspath(executable)):
        raise TerminalTaskValidatorError("terminal_task_validator_path_not_absolute")
    if _DIGEST.fullmatch(expected_sha256) is None:
        raise TerminalTaskValidatorError("terminal_task_validator_digest_missing")
    try:
        details = executable.lstat()
    except OSError as exc:
        raise TerminalTaskValidatorError("terminal_task_validator_missing") from exc
    if (
        stat.S_ISLNK(details.st_mode)
        or not stat.S_ISREG(details.st_mode)
        or details.st_uid != expected_owner_uid
        or details.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
        or not details.st_mode & stat.S_IXUSR
    ):
        raise TerminalTaskValidatorError("terminal_task_validator_file_untrusted")
    observed_sha256 = _file_digest(executable)
    if observed_sha256 != expected_sha256:
        raise TerminalTaskValidatorError("terminal_task_validator_digest_drift")
    try:
        completed = subprocess.run(
            [str(executable), "probe", "--format", "json"],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env={"LANG": "C.UTF-8"},
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise TerminalTaskValidatorError("terminal_task_validator_probe_failed") from exc
    if completed.returncode != 0 or len(completed.stdout) > 16_384:
        raise TerminalTaskValidatorError("terminal_task_validator_probe_failed")
    try:
        probe = TerminalTaskValidatorProbeV1.model_validate_json(completed.stdout)
    except ValueError as exc:
        raise TerminalTaskValidatorError("terminal_task_validator_probe_invalid") from exc
    canonical = canonical_document(probe.model_dump(mode="json"))
    if completed.stdout != canonical:
        raise TerminalTaskValidatorError("terminal_task_validator_probe_not_canonical")
    if probe.validation_policy_sha256 != TERMINALGEN_VALIDATION_POLICY_DIGEST:
        raise TerminalTaskValidatorError("terminal_task_validator_policy_drift")
    return TerminalTaskValidatorAttestation(
        executable=executable,
        executable_sha256=observed_sha256,
        probe=probe,
        probe_sha256=canonical_digest(probe.model_dump(mode="json")),
    )


def _file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while value := stream.read(_READ_BYTES):
            digest.update(value)
    return f"sha256:{digest.hexdigest()}"


__all__ = [
    "TerminalTaskValidatorAttestation",
    "TerminalTaskValidatorError",
    "TerminalTaskValidatorProbeV1",
    "TerminalTaskValidatorRunRequestV1",
    "attest_terminal_task_validator",
]
