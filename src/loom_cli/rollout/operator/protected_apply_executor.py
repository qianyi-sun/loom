"""Typed composition for journaled protected apply and convergence.

The fixed installed final-gate executor dispatches these complete component
chains only after attested Tier 0-3 rehearsal.  This module owns no partial or
ambient fallback to the historical rollout driver.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from loom_cli.rollout.external_supervisor_controller import (
    parse_external_supervisor_controller_bindings,
)
from loom_cli.rollout.external_supervisor_predecessor import (
    PROTECTED_CANONICAL_UNIT_DIR,
    external_supervisor_unit_directory,
)
from loom_cli.rollout.final_gate_readiness import FinalGateResult
from loom_cli.rollout.preflight_contract import CheckOperation

from .final_gate_plan import FinalGatePlan
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ComponentTerminal,
    ProtectedApplyJournal,
)
from .protected_environment_state_component import (
    ProtectedEnvironmentStateComponent,
    ProtectedEnvironmentStateTransport,
)
from .protected_epoch_component import (
    KubernetesProtectedEpochComponent,
    requires_legacy_epoch_bootstrap,
)
from .protected_external_supervisor_component import (
    ProtectedExternalSupervisorComponent,
)
from .protected_external_supervisor_credential_component import (
    ProtectedExternalSupervisorCredentialComponent,
)
from .protected_external_supervisor_credential_transport import (
    ProtectedExternalSupervisorCredentialTransport,
)
from .protected_external_supervisor_transport import (
    ProtectedExternalSupervisorTransport,
)
from .protected_gb10_component import (
    ProtectedGB10CandidateComponent,
    ProtectedGB10FleetTransport,
)
from .protected_manifest_component import KubernetesProtectedManifestComponent
from .protected_migration_component import KubernetesProtectedMigrationComponent
from .protected_production_defaults_component import (
    HttpxProductionDefaultsTransport,
    KubernetesProtectedProductionDefaultsComponent,
    ProductionDefaultsTransport,
)

PROTECTED_KUBECONFIG_PATH = Path("/var/lib/loom-staging-rollout/kubeconfig")
_MAX_OUTPUT_BYTES = 1024 * 1024
_EXTERNAL_SUPERVISOR_CONTROLLER_ORDER = (
    "gx10-01c7",
    "TRT-EAI-OLDLAB-1",
)


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
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
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

    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
        timeout_seconds: float,
    ) -> bytes:
        """Capture one bounded machine-readable mutation or dry-run result."""

        return self._run(
            argv,
            env=env,
            input_payload=input_payload,
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
            # A newline is NOT rejected: protected components dispatch bounded
            # read/mutation commands as subprocess argv with no shell (e.g.
            # `kubectl exec ... -- sh -ceu '... psql -c "$1"' sh <SQL>`, where the
            # trailing rate-card inventory SQL is a multi-line literal). An
            # embedded newline is literal argument text, not an injection vector.
            # Empty elements and NUL bytes are still rejected.
            or any(not item or "\x00" in item for item in command)
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
    gb10_transport: ProtectedGB10FleetTransport
    environment_state_transport: ProtectedEnvironmentStateTransport
    candidate_root: Path
    external_supervisor_transport: ProtectedExternalSupervisorTransport | None = None
    external_supervisor_execution_host: str | None = None
    external_supervisor_transports: Mapping[str, ProtectedExternalSupervisorTransport] = field(
        default_factory=dict
    )
    external_supervisor_credential_transports: Mapping[
        str, ProtectedExternalSupervisorCredentialTransport
    ] = field(default_factory=dict)
    external_supervisor_credential_identities: Mapping[str, tuple[int, int]] = field(
        default_factory=dict
    )
    production_defaults_request: ProductionDefaultsTransport = field(
        default_factory=HttpxProductionDefaultsTransport
    )
    container_registry: str = ""

    def __post_init__(self) -> None:
        if (
            not self.state_root.is_absolute()
            or ".." in self.state_root.parts
            or not self.candidate_root.is_absolute()
            or ".." in self.candidate_root.parts
            or self.service_uid < 0
            or bool(self.external_supervisor_transport) == bool(self.external_supervisor_transports)
            or not self.external_supervisor_credential_transports
            or set(self.external_supervisor_credential_transports)
            != set(self.external_supervisor_credential_identities)
            or any(
                type(uid) is not int or type(gid) is not int or uid < 0 or gid < 0
                for uid, gid in self.external_supervisor_credential_identities.values()
            )
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
        # Reconcile any prior append-only timer activation prefix before this
        # request is allowed to mutate a new candidate.  The authoritative
        # pointer selects active target convergence, active predecessor
        # convergence, or (only for an explicit absent predecessor) quiescence;
        # every path is identity/hash-bound and fails closed on verification.
        supervisor_components = _external_supervisor_components(
            candidate_root=self.candidate_root,
            plan=plan,
            epoch_guard=KubernetesProtectedEpochComponent(
                runner=self.runner,
                environment=environment,
            ).classify,
            transport=self.external_supervisor_transport,
            execution_host=self.external_supervisor_execution_host,
            transports=self.external_supervisor_transports,
        )
        for supervisor_component in supervisor_components:
            supervisor_component.transport.reconcile_compensations()
        epoch = KubernetesProtectedEpochComponent(
            runner=self.runner,
            environment=environment,
        ).component(plan)
        migration = KubernetesProtectedMigrationComponent(
            runner=self.runner,
            environment=environment,
            service_uid=self.service_uid,
            container_registry=self.container_registry,
        ).component(plan)
        manifests = KubernetesProtectedManifestComponent(
            runner=self.runner,
            environment=environment,
            service_uid=self.service_uid,
            epoch_guard=epoch.classify,
        ).component(plan)
        production_defaults = KubernetesProtectedProductionDefaultsComponent(
            runner=self.runner,
            environment=environment,
            service_uid=self.service_uid,
            epoch_guard=epoch.classify,
            request=self.production_defaults_request,
        ).component(plan)
        gb10 = ProtectedGB10CandidateComponent(
            transport=self.gb10_transport,
            epoch_guard=epoch.classify,
        ).component(plan)
        environment_state = ProtectedEnvironmentStateComponent(
            transport=self.environment_state_transport,
            epoch_guard=epoch.classify,
        ).component(plan)
        external_supervisors = tuple(
            supervisor.component(plan) for supervisor in supervisor_components
        )
        external_supervisor_credentials = tuple(
            component.component(plan)
            for component in _external_supervisor_credential_components(
                plan=plan,
                epoch_guard=epoch.classify,
                transports=self.external_supervisor_credential_transports,
                identities=self.external_supervisor_credential_identities,
                execution_host=self.external_supervisor_execution_host,
            )
        )
        components = (
            (
                migration,
                epoch,
                manifests,
                environment_state,
                gb10,
                production_defaults,
                *external_supervisor_credentials,
                *external_supervisors,
            )
            if requires_legacy_epoch_bootstrap(plan)
            else (
                epoch,
                migration,
                manifests,
                environment_state,
                gb10,
                production_defaults,
                *external_supervisor_credentials,
                *external_supervisors,
            )
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


@dataclass(frozen=True, slots=True)
class KubernetesProtectedConvergenceExecutor:
    """Verify the exact protected component state without repeating apply."""

    service_uid: int
    runner: ProtectedApplyCommandRunner
    gb10_transport: ProtectedGB10FleetTransport
    environment_state_transport: ProtectedEnvironmentStateTransport
    candidate_root: Path
    external_supervisor_transport: ProtectedExternalSupervisorTransport | None = None
    external_supervisor_execution_host: str | None = None
    external_supervisor_transports: Mapping[str, ProtectedExternalSupervisorTransport] = field(
        default_factory=dict
    )
    external_supervisor_credential_transports: Mapping[
        str, ProtectedExternalSupervisorCredentialTransport
    ] = field(default_factory=dict)
    external_supervisor_credential_identities: Mapping[str, tuple[int, int]] = field(
        default_factory=dict
    )
    environment_state_attempts: int = 121
    environment_state_interval_seconds: float = 5.0
    sleep: Callable[[float], None] = time.sleep
    production_defaults_request: ProductionDefaultsTransport = field(
        default_factory=HttpxProductionDefaultsTransport
    )
    container_registry: str = ""

    def __post_init__(self) -> None:
        if (
            self.service_uid < 0
            or not self.candidate_root.is_absolute()
            or ".." in self.candidate_root.parts
            or not 1 <= self.environment_state_attempts <= 721
            or not 0 <= self.environment_state_interval_seconds <= 30
            or not callable(self.sleep)
            or bool(self.external_supervisor_transport) == bool(self.external_supervisor_transports)
            or not self.external_supervisor_credential_transports
            or set(self.external_supervisor_credential_transports)
            != set(self.external_supervisor_credential_identities)
            or any(
                type(uid) is not int or type(gid) is not int or uid < 0 or gid < 0
                for uid, gid in self.external_supervisor_credential_identities.values()
            )
        ):
            raise ValueError("protected convergence authority is invalid")

    def __call__(
        self,
        check_id: str,
        operation: CheckOperation,
        plan: FinalGatePlan,
    ) -> FinalGateResult:
        if check_id != "final.convergence" or operation is not CheckOperation.VERIFY:
            raise ValueError("protected convergence operation is invalid")
        environment = self.runner.environment
        if environment.get("KUBECONFIG") is None:
            raise ValueError("protected convergence command environment is invalid")
        epoch = KubernetesProtectedEpochComponent(
            runner=self.runner,
            environment=environment,
        )
        environment_state_component = ProtectedEnvironmentStateComponent(
            transport=self.environment_state_transport,
            epoch_guard=epoch.classify,
        )
        supervisor_components = _external_supervisor_components(
            candidate_root=self.candidate_root,
            plan=plan,
            epoch_guard=epoch.classify,
            transport=self.external_supervisor_transport,
            execution_host=self.external_supervisor_execution_host,
            transports=self.external_supervisor_transports,
        )
        credential_components = _external_supervisor_credential_components(
            plan=plan,
            epoch_guard=epoch.classify,
            transports=self.external_supervisor_credential_transports,
            identities=self.external_supervisor_credential_identities,
            execution_host=self.external_supervisor_execution_host,
        )
        observations = {
            "database-migration": KubernetesProtectedMigrationComponent(
                runner=self.runner,
                environment=environment,
                service_uid=self.service_uid,
                container_registry=self.container_registry,
            ).classify(plan),
            "mutation-epoch-claim": epoch.classify(plan),
            "staging-manifests": KubernetesProtectedManifestComponent(
                runner=self.runner,
                environment=environment,
                service_uid=self.service_uid,
                epoch_guard=epoch.classify,
            ).classify(plan),
            "environment-state": self._environment_state_observation(
                environment_state_component,
                plan,
            ),
            "gb10-candidate": ProtectedGB10CandidateComponent(
                transport=self.gb10_transport,
                epoch_guard=epoch.classify,
            ).classify(plan),
            "production-defaults": KubernetesProtectedProductionDefaultsComponent(
                runner=self.runner,
                environment=environment,
                service_uid=self.service_uid,
                epoch_guard=epoch.classify,
                request=self.production_defaults_request,
            ).classify(plan),
        }
        external_component_ids: list[str] = []
        credential_component_ids: list[str] = []
        for credential in credential_components:
            component = credential.component(plan)
            credential_component_ids.append(component.component_id)
            observations[component.component_id] = component.classify(plan)
        for supervisor in supervisor_components:
            component = supervisor.component(plan)
            external_component_ids.append(component.component_id)
            observations[component.component_id] = component.classify(plan)
        expected_epoch = plan.starting_mutation_epoch + 1
        blockers = {
            component_id: "protected-component-not-exact"
            for component_id, observation in sorted(observations.items())
            if observation.state is not ComponentState.EXACT
        }
        if observations["mutation-epoch-claim"].observed_epoch != expected_epoch:
            blockers["mutation-epoch-claim"] = "protected-epoch-not-exact"
        if observations["staging-manifests"].observed_epoch != expected_epoch:
            blockers["staging-manifests"] = "protected-epoch-not-exact"
        if observations["environment-state"].observed_epoch != expected_epoch:
            blockers["environment-state"] = "protected-epoch-not-exact"
        if observations["gb10-candidate"].observed_epoch != expected_epoch:
            blockers["gb10-candidate"] = "protected-epoch-not-exact"
        if observations["production-defaults"].observed_epoch != expected_epoch:
            blockers["production-defaults"] = "protected-epoch-not-exact"
        for component_id in (*credential_component_ids, *external_component_ids):
            if observations[component_id].observed_epoch != expected_epoch:
                blockers[component_id] = "protected-epoch-not-exact"
        return FinalGateResult(
            check_id=check_id,
            operation=operation,
            candidate_sha=plan.candidate_sha,
            attestation_digest=plan.attestation_digest,
            observed_epoch=max(observation.observed_epoch for observation in observations.values()),
            evidence_digest=_observation_evidence_digest(observations),
            protected_mutation=False,
            blockers=blockers,
        )

    def _environment_state_observation(
        self,
        component: ProtectedEnvironmentStateComponent,
        plan: FinalGatePlan,
    ) -> ComponentObservation:
        observation = component.classify_runtime(plan)
        for _attempt in range(1, self.environment_state_attempts):
            if observation.state in {ComponentState.EXACT, ComponentState.DRIFTED}:
                break
            self.sleep(self.environment_state_interval_seconds)
            observation = component.classify_runtime(plan)
        return observation


def _terminal_evidence_digest(terminals: Mapping[str, ComponentTerminal]) -> str:
    payload = {
        component_id: terminal.to_dict() for component_id, terminal in sorted(terminals.items())
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _external_supervisor_components(
    *,
    candidate_root: Path,
    plan: FinalGatePlan,
    epoch_guard: Callable[[FinalGatePlan], ComponentObservation],
    transport: ProtectedExternalSupervisorTransport | None,
    execution_host: str | None,
    transports: Mapping[str, ProtectedExternalSupervisorTransport],
) -> tuple[ProtectedExternalSupervisorComponent, ...]:
    if transports:
        controller_hosts = _controller_hosts_in_order(plan, transports)
        if any(not host or item is None for host, item in transports.items()):
            raise ValueError("protected external supervisor transport coverage drifted")
        return tuple(
            ProtectedExternalSupervisorComponent(
                candidate_root=candidate_root,
                transport=transports[host],
                epoch_guard=epoch_guard,
                execution_host=host,
                unit_dir=Path(external_supervisor_unit_directory(host)),
            )
            for host in controller_hosts
        )
    if transport is None:
        raise ValueError("protected external supervisor transport is unavailable")
    return (
        ProtectedExternalSupervisorComponent(
            candidate_root=candidate_root,
            transport=transport,
            epoch_guard=epoch_guard,
            execution_host=execution_host,
            unit_dir=(
                Path(PROTECTED_CANONICAL_UNIT_DIR)
                if execution_host is None
                else Path(external_supervisor_unit_directory(execution_host))
            ),
        ),
    )


def _external_supervisor_credential_components(
    *,
    plan: FinalGatePlan,
    epoch_guard: Callable[[FinalGatePlan], ComponentObservation],
    transports: Mapping[str, ProtectedExternalSupervisorCredentialTransport],
    identities: Mapping[str, tuple[int, int]],
    execution_host: str | None,
) -> tuple[ProtectedExternalSupervisorCredentialComponent, ...]:
    if execution_host is None:
        controller_hosts = _controller_hosts_in_order(plan, transports)
    else:
        bound_hosts = set(
            parse_external_supervisor_controller_bindings(plan.supervisor_controller_bindings)
        )
        if execution_host not in bound_hosts or set(transports) != {execution_host}:
            raise ValueError("protected external supervisor credential coverage drifted")
        controller_hosts = (execution_host,)
    if set(identities) != set(controller_hosts) or any(
        item is None
        or type(identity) is not tuple
        or len(identity) != 2
        or type(identity[0]) is not int
        or type(identity[1]) is not int
        or identity[0] < 0
        or identity[1] < 0
        for item, identity in ((transports[host], identities[host]) for host in controller_hosts)
    ):
        raise ValueError("protected external supervisor credential coverage drifted")
    return tuple(
        ProtectedExternalSupervisorCredentialComponent(
            transport=transports[host],
            epoch_guard=epoch_guard,
            execution_host=host,
            service_uid=identities[host][0],
            service_gid=identities[host][1],
        )
        for host in controller_hosts
    )


def _controller_hosts_in_order(
    plan: FinalGatePlan,
    transports: Mapping[str, object],
) -> tuple[str, ...]:
    controller_hosts = set(
        parse_external_supervisor_controller_bindings(plan.supervisor_controller_bindings)
    )
    if set(transports) != controller_hosts:
        raise ValueError("protected external supervisor transport coverage drifted")
    ordered = tuple(
        host for host in _EXTERNAL_SUPERVISOR_CONTROLLER_ORDER if host in controller_hosts
    )
    if len(ordered) != len(controller_hosts):
        raise ValueError("protected external supervisor controller is unauthorized")
    return ordered


def _observation_evidence_digest(
    observations: Mapping[str, ComponentObservation],
) -> str:
    payload = {
        component_id: {
            "evidence_digest": observation.evidence_digest,
            "observed_epoch": observation.observed_epoch,
            "state": observation.state.value,
        }
        for component_id, observation in sorted(observations.items())
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


__all__ = [
    "PROTECTED_KUBECONFIG_PATH",
    "KubernetesProtectedConvergenceExecutor",
    "MigrationEpochProtectedApplyExecutor",
    "ProtectedApplyCommandRunner",
    "SubprocessProtectedApplyCommandRunner",
]
