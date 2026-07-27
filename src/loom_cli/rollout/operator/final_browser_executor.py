"""Exact installed live-browser action backed by the shared report contract."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.browser_report_contract import (
    BROWSER_ACCEPTANCE_USERNAME,
    RolloutBrowserReportAuthority,
    browser_report_ready,
    browser_report_schema_digest,
)
from loom_cli.rollout.credential_authority import (
    read_trusted_file,
    safe_content_fingerprint,
)
from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.preflight_contract import CheckOperation

from .final_gate_plan import FinalGatePlan

_MAX_REPORT_BYTES = 1024 * 1024
_MAX_TOKEN_BYTES = 64 * 1024
_OUTPUT_DIRECTORY = "final-browser"
_REPORT_NAME = "staging-admin-browser-acceptance.json"
_STAGED_TOKEN_NAME = "admin-token"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class BrowserCommandResult(Protocol):
    @property
    def returncode(self) -> int: ...


BrowserCommandRunner = Callable[[Sequence[str]], BrowserCommandResult]


@dataclass(frozen=True, slots=True)
class FinalBrowserExecutor:
    """Run or idempotently consume one exact candidate-bound browser report."""

    state_root: Path
    service_uid: int
    service_gid: int
    token_path: Path
    expected_token_fingerprint: str
    run: BrowserCommandRunner

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
            or self.service_gid < 0
            or not self.token_path.is_absolute()
            or ".." in self.token_path.parts
            or any(character in str(self.state_root) for character in (",", "\n", "\r", "\x00"))
            or any(character in str(self.token_path) for character in (",", "\n", "\r", "\x00"))
            or not self.expected_token_fingerprint.startswith("sha256:")
            or not callable(self.run)
        ):
            raise ValueError("final browser executor authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        plan: FinalGatePlan,
    ) -> FinalGateResult:
        if check_id != "final.browser" or operation is not CheckOperation.APPLY:
            raise ValueError("final browser executor operation is invalid")
        if (
            plan.environment != "staging"
            or plan.namespace != "loom-staging"
            or plan.browser_report_schema != browser_report_schema_digest()
            or plan.browser_image_digest
            != plan.image_digests.get("loom-staging-admin-browser-smoke")
        ):
            return self._result(plan, blocker="browser-plan-drift", mutated=False)
        try:
            token = read_trusted_file(
                self.token_path,
                service_uid=self.service_uid,
                private=True,
                allow_qianyi_owner=True,
                max_bytes=_MAX_TOKEN_BYTES,
                require_nonempty=True,
            )
        except (OSError, ValueError):
            return self._result(plan, blocker="browser-token-authority-drift", mutated=False)
        if (
            safe_content_fingerprint(token.payload.strip()) != self.expected_token_fingerprint
            or plan.secret_metadata_fingerprints.get("admin")
            != f"sha256:{token.metadata_fingerprint}"
        ):
            return self._result(plan, blocker="browser-token-binding-drift", mutated=False)

        try:
            directory = self._directory(plan)
        except OSError:
            return self._result(plan, blocker="browser-evidence-authority-failed", mutated=False)
        report_path = directory / _REPORT_NAME
        try:
            created = self._prepare_directory(directory)
        except OSError:
            return self._result(plan, blocker="browser-evidence-authority-failed", mutated=False)
        if not created:
            report = self._read_report(report_path)
            if report is None or not browser_report_ready(
                report[0], authority=self._report_authority(plan)
            ):
                return self._result(
                    plan,
                    blocker="browser-existing-evidence-invalid",
                    mutated=False,
                )
            return self._result(plan, report_sha256=report[1], mutated=True)

        try:
            staged_token = self._stage_admin_token(directory, token.payload)
        except OSError:
            return self._result(plan, blocker="browser-evidence-authority-failed", mutated=False)
        result = self.run(self._command(plan, directory, staged_token))
        report = self._read_report(report_path)
        if (
            result.returncode != 0
            or report is None
            or not browser_report_ready(report[0], authority=self._report_authority(plan))
        ):
            return self._result(plan, blocker="browser-acceptance-failed", mutated=True)
        return self._result(plan, report_sha256=report[1], mutated=True)

    def _directory(self, plan: FinalGatePlan) -> Path:
        attempt = (
            self.state_root / "requests" / plan.request_id / "attempts" / str(plan.attempt_number)
        )
        metadata = attempt.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_ISLNK(metadata.st_mode)
            or metadata.st_uid != self.service_uid
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise OSError("browser attempt directory authority is unsafe")
        return attempt / _OUTPUT_DIRECTORY

    def _prepare_directory(self, path: Path) -> bool:
        try:
            path.mkdir(mode=0o700)
            return True
        except FileExistsError:
            metadata = path.lstat()
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or stat.S_ISLNK(metadata.st_mode)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != 0o700
            ):
                raise OSError("browser evidence directory authority is unsafe") from None
            return False

    def _read_report(self, path: Path) -> tuple[object, str] | None:
        try:
            trusted = read_trusted_file(
                path,
                service_uid=self.service_uid,
                private=True,
                max_bytes=_MAX_REPORT_BYTES,
                require_nonempty=True,
            )
            return json.loads(trusted.payload), hashlib.sha256(trusted.payload).hexdigest()
        except (OSError, ValueError, json.JSONDecodeError):
            return None

    def _report_authority(self, plan: FinalGatePlan) -> RolloutBrowserReportAuthority:
        return RolloutBrowserReportAuthority(
            request_id=plan.request_id,
            attempt_number=plan.attempt_number,
            request_envelope_sha256=plan.request_envelope_sha256,
            candidate_sha=plan.candidate_sha,
            route=plan.route,
        )

    def _stage_admin_token(self, directory: Path, payload: bytes) -> Path:
        # The configured admin-token source is an ACL-readable shared file (owned
        # by the provisioning account, readable by the rollout via a POSIX ACL).
        # The hardened browser container re-validates the mounted token as
        # ``st_uid == geteuid() and mode == 0o600`` -- which an ACL/group-shared
        # file cannot satisfy inside the container. Stage a private 0600 copy
        # owned by the service uid in the service-owned (0700) attempt directory
        # (a sibling of the read-only /evidence mount) and bind-mount that copy.
        path = directory.parent / _STAGED_TOKEN_NAME
        descriptor = os.open(
            path,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC,
            0o600,
        )
        try:
            written = os.write(descriptor, payload)
            metadata = os.fstat(descriptor)
            if (
                written != len(payload)
                or metadata.st_uid != self.service_uid
                or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_nlink != 1
            ):
                raise OSError("staged browser admin token authority is unsafe")
        finally:
            os.close(descriptor)
        return path

    def _command(
        self, plan: FinalGatePlan, directory: Path, token_file: Path
    ) -> tuple[str, ...]:
        return (
            "docker",
            "run",
            "--rm",
            "--pull=never",
            "--name",
            f"loom-browser-{plan.request_id}-{plan.attempt_number}",
            "--read-only",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--network=bridge",
            "--pids-limit=512",
            "--memory=2g",
            "--cpus=2",
            "--shm-size=512m",
            "--user",
            f"{self.service_uid}:{self.service_gid}",
            "--env",
            "HOME=/tmp",
            "--tmpfs",
            "/tmp:rw,nosuid,nodev,size=512m,mode=0700",
            "--mount",
            f"type=bind,src={token_file},dst=/run/secrets/admin-token,readonly,bind-propagation=rprivate",
            "--mount",
            f"type=bind,src={directory},dst=/evidence,bind-propagation=rprivate",
            plan.browser_image_digest,
            "--route",
            plan.route,
            "--expected-deployed-sha",
            plan.candidate_sha,
            "--admin-token-source",
            "file:/run/secrets/admin-token",
            "--username",
            BROWSER_ACCEPTANCE_USERNAME,
            "--report",
            f"/evidence/{_REPORT_NAME}",
            "--rollout-request-id",
            plan.request_id,
            "--rollout-attempt-number",
            str(plan.attempt_number),
            "--request-envelope-sha256",
            plan.request_envelope_sha256,
            "--timeout-ms",
            "120000",
        )

    @staticmethod
    def _result(
        plan: FinalGatePlan,
        *,
        blocker: str | None = None,
        report_sha256: str | None = None,
        mutated: bool,
    ) -> FinalGateResult:
        if report_sha256 is not None and _SHA256_RE.fullmatch(report_sha256) is None:
            raise ValueError("final browser report digest is invalid")
        evidence = {
            "blocker": blocker,
            "report_sha256": report_sha256,
            "request_envelope_sha256": plan.request_envelope_sha256,
        }
        return FinalGateResult(
            check_id="final.browser",
            operation=CheckOperation.APPLY,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            observed_epoch=plan.starting_mutation_epoch + (1 if mutated else 0),
            evidence_digest=hashlib.sha256(
                json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
            protected_mutation=mutated,
            blockers=({"browser": blocker} if blocker is not None else {}),
        )


__all__ = ["FinalBrowserExecutor"]
