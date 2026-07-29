"""One sanitized subprocess boundary for every installed preflight adapter."""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol

from .config import OperatorConfig
from .manifest_apply_contract import (
    server_side_apply_argv,
    server_side_schema_validation_argv,
)
from .model import CandidateBinding
from .readonly_preflight_authority import READONLY_KUBECONFIG_PATH

_DNS_RE = re.compile(r"^[a-z0-9](?:[-a-z0-9]{0,61}[a-z0-9])?$")
_BOOT_ID_PROBE = ("cat", "/proc/sys/kernel/random/boot_id")
_EXTERNAL_SLURM_AUTHORITY = "/usr/local/libexec/loom-staging-external-slurm-authority"
_ADMISSION_PREPARATION_TIMEOUT_SECONDS = 3720
_ADMISSION_RESULT_MAX_AGE_SECONDS = 3600
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")
_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
_ADMISSION_RESULT_KEYS = {
    "bootstrap_status",
    "candidate_sha",
    "candidate_tree",
    "convergence_id",
    "generation",
    "kind",
    "node_count",
    "receipt_path",
    "receipt_sha256",
    "requested_at",
    "result",
    "schema_version",
    "source_controller",
    "source_controller_host",
}


class CommandResult(Protocol):
    returncode: int
    stdout: str
    stderr: str


class SubprocessRun(Protocol):
    def __call__(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None,
        env: Mapping[str, str],
        input: str | None,
        timeout: int,
    ) -> CommandResult: ...


def _subprocess_run(
    argv: Sequence[str],
    *,
    cwd: Path | None,
    env: Mapping[str, str],
    input: str | None,
    timeout: int,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(argv),
        cwd=cwd,
        env=dict(env),
        input=input,
        stdin=subprocess.DEVNULL if input is None else None,
        capture_output=True,
        check=False,
        text=True,
        timeout=timeout,
    )


@dataclass(frozen=True, slots=True)
class InstalledPreflightCommands:
    """Typed adapters backed by one exact, secret-free child environment."""

    config: OperatorConfig
    child_environment: Mapping[str, str]
    run_subprocess: SubprocessRun = _subprocess_run

    def __post_init__(self) -> None:
        environment = dict(self.child_environment)
        required = {"HOME", "LC_ALL", "PATH", "USER"}
        if (
            self.config.environment != "staging"
            or self.config.namespace != "loom-staging"
            or not required <= environment.keys()
            or any("TOKEN" in key or "SECRET" in key for key in environment)
        ):
            raise ValueError("installed preflight command environment is invalid")
        object.__setattr__(self, "child_environment", environment)

    def executable(self, name: str) -> str | None:
        if not name or "/" in name or "\x00" in name:
            return None
        return shutil.which(name, path=self.child_environment["PATH"])

    def simple(self, argv: Sequence[str]) -> CommandResult:
        return self._execute(argv, timeout=120)

    def candidate_source(self, argv: Sequence[str]) -> CommandResult:
        """Bound each remote shared-checkout read below DAG cancellation grace.

        The candidate-source check serializes the GB10 fleet to avoid amplifying
        NFS reads. A generic 120-second subprocess timeout can therefore outlive
        the check's 180-second DAG budget and force the entire runner to exit
        during cancellation. Healthy full-tree reads on the protected shared
        mount can take roughly nine seconds, so keep a twelve-second command
        bound: long enough for the live NFS path while fourteen successful
        first attempts still fit below the DAG budget. The probe's two-attempt
        fail-fast policy bounds a failed first host separately.
        """
        return self._execute(argv, timeout=12)

    def prepare_admission(self, candidate: CandidateBinding) -> None:
        """Converge only the fixed exact-candidate staging infrastructure."""
        if (
            _SHA_RE.fullmatch(candidate.resolved_sha) is None
            or candidate.resolved_tree is None
            or _SHA_RE.fullmatch(candidate.resolved_tree) is None
        ):
            raise ValueError("external staging admission candidate is invalid")
        result = self._execute(
            (
                "/usr/bin/sudo",
                "-n",
                _EXTERNAL_SLURM_AUTHORITY,
                "converge-infrastructure",
                "--candidate-sha",
                candidate.resolved_sha,
                "--candidate-tree",
                candidate.resolved_tree,
            ),
            timeout=_ADMISSION_PREPARATION_TIMEOUT_SECONDS,
        )
        if result.returncode != 0 or result.stderr or len(result.stdout.encode()) > 64 * 1024:
            raise RuntimeError("external staging admission preparation failed safely")
        payload = _load_admission_result(result.stdout)
        if (
            payload["candidate_sha"] != candidate.resolved_sha
            or payload["candidate_tree"] != candidate.resolved_tree
        ):
            raise RuntimeError("external staging admission preparation result is invalid")

    def systemd_preflight(self, argv: Sequence[str]) -> CommandResult:
        """Run only the fixed Tier 0 systemd probes with a short RPC bound."""
        command = tuple(argv)
        if not command or (
            command[0] not in {"loginctl", "systemctl", "systemd-run"} and command != _BOOT_ID_PROBE
        ):
            raise ValueError("systemd preflight command is outside authority")
        return self._execute(command, timeout=10)

    def git(self, argv: list[str]) -> CommandResult:
        environment = {
            **self.child_environment,
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_OPTIONAL_LOCKS": "0",
        }
        return self._execute(
            argv,
            cwd=self.config.runner_repo,
            environment=environment,
            timeout=120,
        )

    def image(self, argv: Sequence[str], cwd: Path | None) -> CommandResult:
        if cwd is not None and cwd != self.config.runner_repo:
            raise ValueError("preflight image build escaped exact candidate root")
        return self._execute(argv, cwd=cwd, timeout=1800)

    def readonly_json(self, argv: Sequence[str], payload: bytes) -> CommandResult:
        try:
            rendered = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ValueError("readonly Kubernetes request is not UTF-8") from exc
        environment = {**self.child_environment, "KUBECONFIG": str(READONLY_KUBECONFIG_PATH)}
        return self._execute(argv, environment=environment, input=rendered, timeout=30)

    def manifest_server_dry_run(self, rendered: str) -> CommandResult:
        if not rendered or len(rendered.encode("utf-8")) > 16 * 1024 * 1024:
            raise ValueError("preflight manifest payload is invalid")
        return self._execute(
            server_side_apply_argv(
                self.config.namespace,
                kubeconfig=self.config.kubeconfig_path,
                dry_run=True,
            ),
            input=rendered,
            timeout=120,
        )

    def manifest_server_apply(self, rendered: str) -> CommandResult:
        """Apply one caller-validated exact manifest through the shared contract."""
        if not rendered or len(rendered.encode("utf-8")) > 16 * 1024 * 1024:
            raise ValueError("protected manifest payload is invalid")
        return self._execute(
            server_side_apply_argv(
                self.config.namespace,
                kubeconfig=self.config.kubeconfig_path,
                output_json=True,
            ),
            input=rendered,
            timeout=120,
        )

    def lifecycle_capacity_wait(self, job_name: str) -> CommandResult:
        if _DNS_RE.fullmatch(job_name) is None:
            raise ValueError("lifecycle capacity Job name is invalid")
        return self._execute(
            (
                "kubectl",
                "--kubeconfig",
                str(self.config.kubeconfig_path),
                "--namespace",
                self.config.namespace,
                "wait",
                "--for=condition=complete",
                f"job/{job_name}",
                "--timeout=1200s",
            ),
            timeout=1260,
        )

    def manifest_schema_dry_run(self, rendered: str) -> CommandResult:
        if not rendered or len(rendered.encode("utf-8")) > 16 * 1024 * 1024:
            raise ValueError("preflight manifest payload is invalid")
        return self._execute(
            server_side_schema_validation_argv(
                self.config.namespace,
                kubeconfig=self.config.kubeconfig_path,
            ),
            input=rendered,
            timeout=120,
        )

    def rehearsal_helper(
        self,
        argv: Sequence[str],
        environment: Mapping[str, str],
        timeout: int,
    ) -> CommandResult:
        expected = {
            "HOME",
            "LANG",
            "LC_ALL",
            "PATH",
            "XDG_RUNTIME_DIR",
        }
        if set(environment) != expected or not 1 <= timeout <= 2400:
            raise ValueError("rehearsal helper execution authority is invalid")
        return self._execute(argv, environment=environment, timeout=timeout)

    def final_gate_helper(
        self,
        argv: Sequence[str],
        environment: Mapping[str, str],
        timeout: int,
    ) -> CommandResult:
        expected = {
            "HOME",
            "LANG",
            "LC_ALL",
            "PATH",
            "XDG_RUNTIME_DIR",
        }
        if set(environment) != expected or not 1 <= timeout <= 3600:
            raise ValueError("final gate helper execution authority is invalid")
        return self._execute(argv, environment=environment, timeout=timeout)

    def _execute(
        self,
        argv: Sequence[str],
        *,
        cwd: Path | None = None,
        environment: Mapping[str, str] | None = None,
        input: str | None = None,
        timeout: int,
    ) -> CommandResult:
        command = tuple(argv)
        if (
            not command
            or any(not isinstance(item, str) or not item or "\x00" in item for item in command)
            or (cwd is not None and (not cwd.is_absolute() or ".." in cwd.parts))
            or not 1 <= timeout <= _ADMISSION_PREPARATION_TIMEOUT_SECONDS
        ):
            raise ValueError("installed preflight command is invalid")
        child = dict(self.child_environment if environment is None else environment)
        if any("TOKEN" in key or "SECRET" in key for key in child):
            raise ValueError("installed preflight command environment contains secret authority")
        result = self.run_subprocess(
            command,
            cwd=cwd,
            env=child,
            input=input,
            timeout=timeout,
        )
        if (
            type(result.returncode) is not int
            or not isinstance(result.stdout, str)
            or not isinstance(result.stderr, str)
        ):
            raise RuntimeError("installed preflight command result is invalid")
        return result


def _load_admission_result(rendered: str) -> dict[str, object]:
    """Validate the closed, canonical producer-to-broker convergence receipt."""
    try:
        payload = json.loads(rendered)
    except json.JSONDecodeError as exc:
        raise RuntimeError("external staging admission preparation result is invalid") from exc
    if (
        not isinstance(payload, dict)
        or set(payload) != _ADMISSION_RESULT_KEYS
        or payload.get("schema_version") != 1
        or payload.get("kind") != "staging_external_slurm_infrastructure_convergence"
        or payload.get("result") != "pass"
        or payload.get("bootstrap_status") != "converged"
        or payload.get("node_count") != 14
        or payload.get("source_controller") != "oldlab-2"
        or payload.get("source_controller_host") != "trt-eai-oldlab-2"
        or not isinstance(payload.get("candidate_sha"), str)
        or _SHA_RE.fullmatch(str(payload["candidate_sha"])) is None
        or not isinstance(payload.get("candidate_tree"), str)
        or _SHA_RE.fullmatch(str(payload["candidate_tree"])) is None
        or not isinstance(payload.get("convergence_id"), str)
        or _DIGEST_RE.fullmatch(str(payload["convergence_id"])) is None
        or type(payload.get("generation")) is not int
        or int(payload["generation"]) <= 0
        or payload.get("receipt_path")
        != (
            "/var/lib/loom-developer-sandbox-node-authority/staging-infrastructure/"
            f"{payload.get('candidate_sha')}.json"
        )
        or not isinstance(payload.get("receipt_sha256"), str)
        or _DIGEST_RE.fullmatch(str(payload["receipt_sha256"])) is None
        or not isinstance(payload.get("requested_at"), str)
    ):
        raise RuntimeError("external staging admission preparation result is invalid")
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
    if rendered != canonical:
        raise RuntimeError("external staging admission preparation result is noncanonical")
    try:
        requested_at = datetime.fromisoformat(str(payload["requested_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise RuntimeError("external staging admission preparation result is invalid") from exc
    now = datetime.now(UTC)
    if (
        requested_at.tzinfo is None
        or requested_at.utcoffset() is None
        or not str(payload["requested_at"]).endswith("Z")
        or requested_at.astimezone(UTC).isoformat().replace("+00:00", "Z")
        != payload["requested_at"]
        or not timedelta(0)
        <= now - requested_at.astimezone(UTC)
        <= timedelta(seconds=_ADMISSION_RESULT_MAX_AGE_SECONDS)
    ):
        raise RuntimeError("external staging admission preparation result is stale")
    return payload


__all__ = ["CommandResult", "InstalledPreflightCommands", "SubprocessRun"]
