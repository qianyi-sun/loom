"""Typed composition for the journaled protected migration boundary.

This module deliberately is not the default installed final-gate executor yet.
The final gate replaces the historical rollout driver, so enabling a partial
component chain would incorrectly omit later protected convergence actions.
The composition remains independently testable until every required protected
component is represented in the same journal.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.preflight_contract import CheckOperation

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import ComponentTerminal, ProtectedApplyJournal
from .protected_epoch_component import (
    KubernetesProtectedEpochComponent,
    requires_legacy_epoch_bootstrap,
)
from .protected_manifest_component import KubernetesProtectedManifestComponent
from .protected_migration_component import KubernetesProtectedMigrationComponent

PROTECTED_KUBECONFIG_PATH = Path("/var/lib/loom-staging-rollout/kubeconfig")
_MAX_OUTPUT_BYTES = 1024 * 1024


class ProtectedApplyCommandRunner(Protocol):
    @property
    def environment(self) -> Mapping[str, str]: ...

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes: ...

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None: ...

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class SubprocessProtectedApplyCommandRunner:
    """Run only argv-based protected component commands in a clean environment."""

    kubeconfig: Path = PROTECTED_KUBECONFIG_PATH
    max_output_bytes: int = _MAX_OUTPUT_BYTES

    def __post_init__(self) -> None:
        if (
            self.kubeconfig != PROTECTED_KUBECONFIG_PATH
            or not self.kubeconfig.is_absolute()
            or ".." in self.kubeconfig.parts
            or not 4096 <= self.max_output_bytes <= 16 * 1024 * 1024
        ):
            raise ValueError("protected apply subprocess authority is invalid")

    @property
    def environment(self) -> Mapping[str, str]:
        uid = os.geteuid()
        return {
            "HOME": "/var/lib/loom-staging-rollout",
            "KUBECONFIG": str(self.kubeconfig),
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/opt/loom-staging-runner/venv/bin:/usr/local/bin:/usr/bin:/bin",
            "XDG_RUNTIME_DIR": f"/run/user/{uid}",
        }

    def capture_stdout(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        timeout_seconds: float,
    ) -> bytes:
        return self._run(
            argv,
            env=env,
            input_payload=None,
            timeout_seconds=timeout_seconds,
        )

    def run_checked(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> None:
        self._run(
            argv,
            env=env,
            input_payload=input_payload,
            timeout_seconds=timeout_seconds,
        )

    def run_status(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> int:
        command = self._validate_invocation(
            argv,
            env=env,
            input_payload=input_payload,
            timeout_seconds=timeout_seconds,
        )
        result = subprocess.run(
            command,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            input=input_payload,
            timeout=timeout_seconds,
            env=dict(self.environment),
        )
        if result.returncode not in {0, 1}:
            raise RuntimeError("protected apply status subprocess failed safely")
        return result.returncode

    def _run(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> bytes:
        command = self._validate_invocation(
            argv,
            env=env,
            input_payload=input_payload,
            timeout_seconds=timeout_seconds,
        )
        expected_environment = dict(self.environment)
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            input=input_payload,
            timeout=timeout_seconds,
            env=expected_environment,
        )
        if (
            result.returncode != 0
            or len(result.stdout) > self.max_output_bytes
            or len(result.stderr) > self.max_output_bytes
        ):
            raise RuntimeError("protected apply subprocess failed safely")
        return result.stdout

    def _validate_invocation(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes | None,
        timeout_seconds: float,
    ) -> tuple[str, ...]:
        command = tuple(argv)
        if (
            not command
            or command[0] != "kubectl"
            or any(not item or "\x00" in item or "\n" in item for item in command)
            or dict(env) != dict(self.environment)
            or not 0 < timeout_seconds <= 1800
            or (input_payload is not None and len(input_payload) > self.max_output_bytes)
        ):
            raise ValueError("protected apply subprocess invocation is invalid")
        return command


@dataclass(frozen=True, slots=True)
class MigrationEpochProtectedApplyExecutor:
    """Execute the exact migration and epoch claim through one component journal."""

    state_root: Path
    service_uid: int
    runner: ProtectedApplyCommandRunner

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or self.service_uid < 0
        ):
            raise ValueError("protected apply executor authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        plan: FinalGatePlan,
    ) -> FinalGateResult:
        if check_id != "final.protected-apply" or operation is not CheckOperation.APPLY:
            raise ValueError("protected apply executor operation is invalid")
        environment = self.runner.environment
        if environment.get("KUBECONFIG") is None:
            raise ValueError("protected apply executor command environment is invalid")
        epoch = KubernetesProtectedEpochComponent(
            runner=self.runner,
            environment=environment,
        ).component(plan)
        migration = KubernetesProtectedMigrationComponent(
            runner=self.runner,
            environment=environment,
            service_uid=self.service_uid,
        ).component(plan)
        manifests = KubernetesProtectedManifestComponent(
            runner=self.runner,
            environment=environment,
            service_uid=self.service_uid,
            epoch_guard=epoch.classify,
        ).component(plan)
        components = (
            (migration, epoch, manifests)
            if requires_legacy_epoch_bootstrap(plan)
            else (epoch, migration, manifests)
        )
        terminals = ProtectedApplyJournal(
            self.state_root,
            request_id=plan.request_id,
            attempt_number=plan.attempt_number,
            service_uid=self.service_uid,
        ).execute(plan, components)
        observed_epoch = max(terminal.observed_epoch for terminal in terminals.values())
        if observed_epoch != plan.starting_mutation_epoch + 1:
            raise RuntimeError("protected apply component chain did not advance one epoch")
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            observed_epoch=observed_epoch,
            evidence_digest=_terminal_evidence_digest(terminals),
            protected_mutation=True,
            blockers={},
        )


def _terminal_evidence_digest(terminals: Mapping[str, ComponentTerminal]) -> str:
    payload = {
        component_id: terminal.to_dict() for component_id, terminal in sorted(terminals.items())
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "PROTECTED_KUBECONFIG_PATH",
    "MigrationEpochProtectedApplyExecutor",
    "ProtectedApplyCommandRunner",
    "SubprocessProtectedApplyCommandRunner",
]
