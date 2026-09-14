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
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

from loom.application_database_admission import (
    ApplicationDatabaseHandoffBackend,
    reclose_application_database_for_handoff_recovery,
    reopen_application_database_for_handoff_recovery,
    require_application_database_drained,
)
from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_handoff_completion import ApplicationHandoffDatabaseOutcome
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
from .protected_application_admission_recovery import ApplicationAdmissionRecoveryRecord
from .protected_apply_journal import (
    ComponentObservation,
    ComponentState,
    ComponentTerminal,
    ProtectedApplyComponent,
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
from .protected_external_supervisor_database_secret_component import (
    KubernetesExternalSupervisorDatabaseSecretComponent,
)
from .protected_external_supervisor_transition_cleanup_component import (
    KubernetesExternalSupervisorTransitionCleanupComponent,
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
from .protected_peer_database_connection import (
    PeerDatabaseConnection,
    PeerDatabaseTransportError,
)
from .protected_production_defaults_component import (
    HttpxProductionDefaultsTransport,
    KubernetesProtectedProductionDefaultsComponent,
    ProductionDefaultsTransport,
)
from .staging_mutation_guard import MutationGuardEvidence

PROTECTED_KUBECONFIG_PATH = Path("/var/lib/loom-staging-rollout/kubeconfig")
_MAX_OUTPUT_BYTES = 1024 * 1024
_STAGING_PEER_DATABASE_COMMAND = (
    "kubectl",
    "--namespace",
    "loom-staging",
    "exec",
    "-i",
    "service/loom-postgres-rw",
    "--",
    "sh",
    "-ceu",
    # Staging is PG17. Prevent LOGIN callbacks before the first peer query;
    # the peer refuses any existing event policy, then restores DDL handling.
    "PGOPTIONS='-c event_triggers=off' exec psql -U postgres -d loom -qAtX -v ON_ERROR_STOP=1",
)
_STAGING_PEER_MAINTENANCE_COMMAND = (
    *_STAGING_PEER_DATABASE_COMMAND[:-1],
    "PGOPTIONS='-c event_triggers=off' exec psql -U postgres -d postgres -qAtX -v ON_ERROR_STOP=1",
)
_EXTERNAL_SUPERVISOR_CONTROLLER_ORDER = (
    "gx10-01c7",
    "TRT-EAI-OLDLAB-1",
)
_EXTERNAL_SUPERVISOR_CREDENTIAL_ORDER = (
    "TRT-EAI-OLDLAB-1",
    "gx10-01c7",
)
_EXTERNAL_SUPERVISOR_RECONCILIATION_IMPLEMENTATION_DIGEST = hashlib.sha256(
    b"loom-protected-external-supervisor-reconciliation-v1"
).hexdigest()
_STAGING_CAPACITY_COMPONENT_ORDER = (
    "staging-capacity-credentials",
    "staging-capacity-database",
    "staging-protected-runtime-secret",
    "capacity-manager-runtime",
    "capacity-manager-configuration",
    "staging-capacity-agent",
)
_STAGING_CAPACITY_EXECUTION_COMPONENT_ORDER = (
    *_STAGING_CAPACITY_COMPONENT_ORDER[:3],
    "oldlab-controller-prerequisite",
    "gb10-controller-prerequisite",
    "staging-capacity-execution-credentials",
    *_STAGING_CAPACITY_COMPONENT_ORDER[3:],
    "capacity-execution-preparation",
)


def _staging_capacity_component_order(plan: FinalGatePlan) -> tuple[str, ...]:
    return (
        _STAGING_CAPACITY_EXECUTION_COMPONENT_ORDER
        if plan.schema_version == 7
        else _STAGING_CAPACITY_COMPONENT_ORDER
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

    def capture_stdout_with_input(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str],
        input_payload: bytes,
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


class ProtectedStagingCapacityRuntime(Protocol):
    """Build the complete, fixed protected staging-capacity component chain."""

    def components(
        self,
        plan: FinalGatePlan,
        *,
        epoch_guard: Callable[[FinalGatePlan], ComponentObservation],
    ) -> tuple[ProtectedApplyComponent, ...]: ...


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
            # CLI preferences must not rewrite protected commands or add output.
            "KUBECTL_KUBERC": "false",
            "LANG": "C.UTF-8",
            "LC_ALL": "C.UTF-8",
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "PYTHONDONTWRITEBYTECODE": "1",
            "XDG_RUNTIME_DIR": f"/run/user/{uid}",
        }

    def open_staging_peer_database(self) -> PeerDatabaseConnection:
        """Open the fixed bounded peer channel for admitted installed code only.

        This does not admit a release or authorize a handoff. The protected
        caller supplies its existing service identity, durable operation and
        preconditions; no candidate-selected target or credentials are accepted.
        The returned context manager owns cleanup, and exposes exact backend
        identity for recovery. Local process retirement never proves rollback.
        """
        return self._open_staging_peer(maintenance=False)

    def prepare_staging_application_database(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
        connection: ApplicationDatabaseConnection, guard: MutationGuardEvidence,
    ) -> ApplicationAdmissionRecoveryRecord:
        """Prepare and close the initial SQL phase inside the admitted handoff.

        The caller retains the original peer and guard and supplies enclosing
        process/input/writer admission. This is not a standalone deployment or a
        component terminal; later phases still retire clients and restore service.
        """
        from .protected_application_database_preparation import (
            prepare_protected_application_database,
        )

        return prepare_protected_application_database(
            plan, journal=journal, runner=self, connection=connection, guard=guard,
        )

    def complete_staging_application_database(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
        connection: PeerDatabaseConnection, guard: MutationGuardEvidence,
    ) -> ApplicationHandoffDatabaseOutcome:
        """Complete the journal-bound SQL phases under the enclosing handoff authority.

        External process/DDL/workload exclusion and harmful SQL retirement remain
        enclosing component requirements. Database success cannot release a fence
        or the retained guard before actual workload recovery is verified.
        """
        from .protected_application_database_completion import (
            complete_protected_application_database,
        )

        return complete_protected_application_database(
            plan, journal=journal, runner=self, connection=connection, guard=guard,
        )

    def recover_and_complete_staging_application_database(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
        guard: MutationGuardEvidence,
    ) -> ApplicationHandoffDatabaseOutcome:
        """Recover one journaled peer and finish SQL without resealing a restored login.

        Retains the same original external authority and guard. This fixed
        operation performs no CNPG/workload recovery or component completion.
        A successful database outcome leaves admission open; uncertain failures
        attempt guarded reclosure only while roles remain sealed. An already
        restored role refuses that cleanup rather than being silently resealed.
        """
        from loom.application_handoff_completion import application_handoff_recovery_login_enabled

        from .protected_application_credential_recovery import (
            recover_application_runtime_credential,
        )
        from .protected_application_database_completion import _require_completion_authority

        original = _require_completion_authority(plan, journal=journal, guard=guard)
        assert original.coordination_guard is not None
        credential = recover_application_runtime_credential(plan, journal=journal, runner=self)
        records = journal.read_application_handoff_recoveries()
        ordinal = len(records) if records and records[-1][1] is None else len(records) + 1
        journal.prepare_application_handoff_recovery(ordinal=ordinal)
        prior = records[ordinal - 2][1] if ordinal > 1 else None
        lost = prior.handoff_backend if prior is not None else original.handoff_backend
        completed = False
        try:
            with self.open_staging_peer_maintenance_database() as maintenance:
                restored = application_handoff_recovery_login_enabled(
                    maintenance, target=original.target, handoff_backend=lost,
                    coordination_guard=original.coordination_guard, provisioner_role="postgres",
                )
                if not restored:
                    reclose_application_database_for_handoff_recovery(
                        maintenance, target=original.target, provisioner_role="postgres",
                        handoff_backend=lost, coordination_guard=original.coordination_guard,
                        runtime_password=credential.password,
                    )
                    reopen_application_database_for_handoff_recovery(
                        maintenance, target=original.target, provisioner_role="postgres",
                        handoff_backend=lost, coordination_guard=original.coordination_guard,
                        runtime_password=credential.password,
                    )
                with self.open_staging_peer_database() as peer:
                    identity = peer.backend_identity
                    if identity.database != original.target.database or identity.session_user != "postgres":
                        raise PeerDatabaseTransportError("application completion recovery peer changed")
                    backend = ApplicationDatabaseHandoffBackend(
                        identity.backend_pid, identity.backend_started_at, identity.system_identifier,
                        identity.server_started_at, identity.database_oid,
                    )
                    journal.record_application_handoff_replacement(ordinal=ordinal, handoff_backend=backend)
                    if not restored:
                        reclose_application_database_for_handoff_recovery(
                            maintenance, target=original.target, provisioner_role="postgres",
                            handoff_backend=backend, coordination_guard=original.coordination_guard,
                            runtime_password=credential.password,
                        )
                    outcome = self.complete_staging_application_database(
                        plan, journal=journal, connection=peer, guard=guard,
                    )
                    completed = True
                    return outcome
        finally:
            if not completed:
                # Fresh maintenance also reconciles a poisoned/lost transport.
                # LOGIN restoration is deliberately never undone speculatively:
                # its exact source/schema reconciliation happens on the next retry.
                with self.open_staging_peer_maintenance_database() as cleanup:
                    reclose_application_database_for_handoff_recovery(
                        cleanup, target=original.target, provisioner_role="postgres",
                        handoff_backend=lost, coordination_guard=original.coordination_guard,
                        runtime_password=credential.password,
                    )

    def issue_staging_manager_replacement(
        self, *, journal: ProtectedApplyJournal, runtime_password: str | None = None,
    ) -> bool:
        """Issue once under the enclosing admitted handoff; never claim retirement.

        The enclosing installed component must retain exclusive administrator
        and original supervised guard authority. No CLI or candidate-selected
        transport target is exposed. Reconciliation is mandatory on every result.
        """
        from .protected_cnpg_manager_transport import issue_staging_manager_replacement

        return issue_staging_manager_replacement(self, journal=journal, runtime_password=runtime_password)

    def open_staging_peer_maintenance_database(self) -> PeerDatabaseConnection:
        """Keep fixed maintenance access available while application admission is closed.

        Same installed authority, environment and transport bounds as the handoff
        peer. No caller-selected database, credential or command is accepted.
        The protected operation must supply its exact journaled application target.
        """
        return self._open_staging_peer(maintenance=True)

    def _open_staging_peer(self, *, maintenance: bool) -> PeerDatabaseConnection:
        environment = dict(self.environment)
        command = self._validate_invocation(
            _STAGING_PEER_MAINTENANCE_COMMAND if maintenance else _STAGING_PEER_DATABASE_COMMAND,
            env=environment,
            input_payload=None,
            timeout_seconds=30,
        )
        try:
            process = subprocess.Popen(
                command,
                env=environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                bufsize=0,
            )
        except OSError:
            raise PeerDatabaseTransportError("protected peer process failed safely") from None
        connection = PeerDatabaseConnection(process)
        identity = connection.backend_identity
        if (
            connection.info.server_version // 10000 != 17
            or identity.database != ("postgres" if maintenance else "loom")
            or identity.session_user != "postgres"
        ):
            connection.close()
            raise PeerDatabaseTransportError("protected peer identity does not match staging")
        return connection

    @contextmanager
    def recover_staging_peer_database(
        self, plan: FinalGatePlan, *, journal: ProtectedApplyJournal, ordinal: int,
        runtime_password: str | None = None,
    ) -> Iterator[PeerDatabaseConnection]:
        """Compose same-guard lost-peer recovery for an active protected component.

        This is not a deployed component or release admission. The enclosing
        operation must independently retain workload/DDL/process exclusion and
        continuously supervise the ORIGINAL guard. Only sealed, closed-database
        work is allowed in the yielded scope; LOGIN restoration/release belongs
        to the later complete safe-outcome composer. No discovered peer is adopted.
        """
        journal.require_application_credential_context(plan)
        original = journal.read_application_admission_recovery()
        if original is None or original.target.database != "loom" or original.coordination_guard is None:
            raise PeerDatabaseTransportError("protected peer recovery requires original staging admission and guard")
        records = journal.read_application_handoff_recoveries()
        if (type(ordinal) is not int or ordinal < len(records)
                or (records and ordinal == len(records) and records[-1][1] is not None)):
            raise PeerDatabaseTransportError("protected peer recovery requires pending or successor ordinal")
        journal.prepare_application_handoff_recovery(ordinal=ordinal)
        prior = records[ordinal - 2][1] if ordinal > 1 else None
        lost = prior.handoff_backend if prior is not None else original.handoff_backend
        with self.open_staging_peer_maintenance_database() as maintenance:
            def reclose() -> None:
                reclose_application_database_for_handoff_recovery(
                    maintenance, target=original.target, provisioner_role="postgres", handoff_backend=lost,
                    coordination_guard=original.coordination_guard, runtime_password=runtime_password,
                )

            # Reconcile both sides of a lost reopen ACK before admitting a process.
            # This commits closure but never signals or adopts surviving sessions.
            try:
                reclose()
                reopen_application_database_for_handoff_recovery(
                    maintenance, target=original.target, provisioner_role="postgres", handoff_backend=lost,
                    coordination_guard=original.coordination_guard, runtime_password=runtime_password,
                )
                with self.open_staging_peer_database() as peer:
                    identity = peer.backend_identity
                    if identity.database != original.target.database or identity.session_user != "postgres":
                        raise PeerDatabaseTransportError("protected recovery peer identity changed")
                    backend = ApplicationDatabaseHandoffBackend(
                        identity.backend_pid, identity.backend_started_at, identity.system_identifier,
                        identity.server_started_at, identity.database_oid,
                    )
                    journal.record_application_handoff_replacement(ordinal=ordinal, handoff_backend=backend)
                    reclose()
                    require_application_database_drained(
                        maintenance, target=original.target, provisioner_role="postgres", handoff_backend=backend,
                        coordination_guard=original.coordination_guard, runtime_password=runtime_password,
                    )
                    yield peer
            finally:
                # Even failed startup/publication or a lost reopen ACK must attempt
                # guarded closure. The prior maintenance transport may be poisoned;
                # fresh maintenance serializes with an in-flight ALTER, even when
                # its committed snapshot still says closed. Lock timeout is refusal,
                # not evidence of remote retirement or successful cleanup.
                with self.open_staging_peer_maintenance_database() as cleanup:
                    reclose_application_database_for_handoff_recovery(
                        cleanup, target=original.target, provisioner_role="postgres", handoff_backend=lost,
                        coordination_guard=original.coordination_guard, runtime_password=runtime_password,
                    )

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

    def probe_cnpg_input_fence(
        self, *, intent_digest: str, target_pooler_names: tuple[str, ...],
    ) -> bool:
        """Require exact fence/binding denial, not generic subprocess failure.

        Only fixed server-dry-run requests are executed. A False result means a
        request was accepted; other failures are sanitized and raised. The caller
        still owns the protected journal, policy identities and writer exclusion.
        """
        from .protected_cnpg_input_fence import cnpg_input_fence_probe_commands

        for policy_name, argv, payload in cnpg_input_fence_probe_commands(
            intent_digest=intent_digest, target_pooler_names=target_pooler_names,
        ):
            command = self._validate_invocation(
                argv, env=self.environment, input_payload=payload, timeout_seconds=30,
            )
            try:
                result = subprocess.run(command, check=False, capture_output=True,
                                        input=payload, timeout=30, env=dict(self.environment))
            except (OSError, subprocess.SubprocessError):
                raise RuntimeError("CNPG input fence probe transport failed safely") from None
            if len(result.stdout) > self.max_output_bytes or len(result.stderr) > self.max_output_bytes:
                raise RuntimeError("CNPG input fence probe response exceeded its bound")
            if result.returncode == 0:
                return False
            expected = (f"ValidatingAdmissionPolicy '{policy_name}' with binding '{policy_name}' "
                        "denied request: loom-cnpg-fence: protected handoff input is frozen").encode()
            if (result.returncode != 1 or result.stdout
                    or not result.stderr.startswith(b"Error from server (Forbidden):")
                    or expected not in result.stderr):
                raise RuntimeError("CNPG input fence probe did not prove expected denial")
        return True

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
    staging_capacity_runtime: ProtectedStagingCapacityRuntime
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
            or not callable(getattr(self.staging_capacity_runtime, "components", None))
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
        supervisor_reconciliation = _external_supervisor_reconciliation_component(
            plan,
            supervisor_components,
        )
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
        staging_capacity = self._staging_capacity_components(plan, epoch.classify)
        manifests = KubernetesProtectedManifestComponent(
            runner=self.runner,
            environment=environment,
            service_uid=self.service_uid,
            epoch_guard=epoch.classify,
        ).component(plan)
        external_supervisor_database = KubernetesExternalSupervisorDatabaseSecretComponent(
            runner=self.runner,
            environment=environment,
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
        external_supervisor_transition_cleanup = (
            KubernetesExternalSupervisorTransitionCleanupComponent(
                runner=self.runner,
                environment=environment,
                epoch_guard=epoch.classify,
            ).component(plan)
        )
        components = (
            (
                supervisor_reconciliation,
                migration,
                epoch,
                *staging_capacity,
                manifests,
                external_supervisor_database,
                environment_state,
                gb10,
                production_defaults,
                external_supervisor_transition_cleanup,
                *external_supervisor_credentials,
                *external_supervisors,
            )
            if requires_legacy_epoch_bootstrap(plan)
            else (
                supervisor_reconciliation,
                epoch,
                migration,
                *staging_capacity,
                manifests,
                external_supervisor_database,
                environment_state,
                gb10,
                production_defaults,
                external_supervisor_transition_cleanup,
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

    def _staging_capacity_components(
        self,
        plan: FinalGatePlan,
        epoch_guard: Callable[[FinalGatePlan], ComponentObservation],
    ) -> tuple[ProtectedApplyComponent, ...]:
        components = self.staging_capacity_runtime.components(
            plan,
            epoch_guard=epoch_guard,
        )
        if tuple(component.component_id for component in components) != (
            _staging_capacity_component_order(plan)
        ):
            raise ValueError("protected staging capacity component coverage drifted")
        return components


@dataclass(frozen=True, slots=True)
class KubernetesProtectedConvergenceExecutor:
    """Verify the exact protected component state without repeating apply."""

    service_uid: int
    runner: ProtectedApplyCommandRunner
    gb10_transport: ProtectedGB10FleetTransport
    environment_state_transport: ProtectedEnvironmentStateTransport
    candidate_root: Path
    staging_capacity_runtime: ProtectedStagingCapacityRuntime
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
            or not callable(getattr(self.staging_capacity_runtime, "components", None))
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
        transition_cleanup = KubernetesExternalSupervisorTransitionCleanupComponent(
            runner=self.runner,
            environment=environment,
            epoch_guard=epoch.classify,
        )
        staging_capacity = self._staging_capacity_components(plan, epoch.classify)
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
            "external-supervisor-database-secret": (
                KubernetesExternalSupervisorDatabaseSecretComponent(
                    runner=self.runner,
                    environment=environment,
                    epoch_guard=epoch.classify,
                ).classify(plan)
            ),
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
            "external-supervisor-transition-cleanup": transition_cleanup.classify(plan),
        }
        staging_capacity_component_ids: list[str] = []
        for component in staging_capacity:
            staging_capacity_component_ids.append(component.component_id)
            observations[component.component_id] = component.classify(plan)
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
        if observations["external-supervisor-database-secret"].observed_epoch != expected_epoch:
            blockers["external-supervisor-database-secret"] = "protected-epoch-not-exact"
        if observations["environment-state"].observed_epoch != expected_epoch:
            blockers["environment-state"] = "protected-epoch-not-exact"
        if observations["gb10-candidate"].observed_epoch != expected_epoch:
            blockers["gb10-candidate"] = "protected-epoch-not-exact"
        if observations["production-defaults"].observed_epoch != expected_epoch:
            blockers["production-defaults"] = "protected-epoch-not-exact"
        if observations["external-supervisor-transition-cleanup"].observed_epoch != expected_epoch:
            blockers["external-supervisor-transition-cleanup"] = "protected-epoch-not-exact"
        for component_id in (
            *staging_capacity_component_ids,
            *credential_component_ids,
            *external_component_ids,
        ):
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

    def _staging_capacity_components(
        self,
        plan: FinalGatePlan,
        epoch_guard: Callable[[FinalGatePlan], ComponentObservation],
    ) -> tuple[ProtectedApplyComponent, ...]:
        components = self.staging_capacity_runtime.components(
            plan,
            epoch_guard=epoch_guard,
        )
        if tuple(component.component_id for component in components) != (
            _staging_capacity_component_order(plan)
        ):
            raise ValueError("protected staging capacity component coverage drifted")
        return components

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


def _external_supervisor_reconciliation_component(
    plan: FinalGatePlan,
    supervisors: Sequence[ProtectedExternalSupervisorComponent],
) -> ProtectedApplyComponent:
    execution_hosts = tuple(
        supervisor.execution_host or "local-controller" for supervisor in supervisors
    )
    if not supervisors or len(set(execution_hosts)) != len(execution_hosts):
        raise ValueError("protected external supervisor reconciliation coverage drifted")
    input_fingerprint = hashlib.sha256(
        json.dumps(
            {"execution_hosts": execution_hosts},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    evidence_digest = hashlib.sha256(
        json.dumps(
            {
                "execution_hosts": execution_hosts,
                "reconciled": True,
                "starting_epoch": plan.starting_mutation_epoch,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()

    def classify(bound_plan: FinalGatePlan) -> ComponentObservation:
        return ComponentObservation(
            state=(
                ComponentState.EXACT
                if bound_plan.plan_digest == plan.plan_digest
                else ComponentState.DRIFTED
            ),
            evidence_digest=evidence_digest,
            observed_epoch=plan.starting_mutation_epoch,
        )

    def reconcile(bound_plan: FinalGatePlan) -> None:
        if bound_plan.plan_digest != plan.plan_digest:
            raise ValueError("protected external supervisor reconciliation plan drifted")
        for supervisor in supervisors:
            supervisor.transport.reconcile_compensations()

    return ProtectedApplyComponent(
        component_id="external-supervisor-reconciliation",
        implementation_digest=_EXTERNAL_SUPERVISOR_RECONCILIATION_IMPLEMENTATION_DIGEST,
        input_fingerprint=input_fingerprint,
        classify=classify,
        apply=reconcile,
        reconcile_before_apply=True,
    )


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
        bound_hosts = set(
            parse_external_supervisor_controller_bindings(plan.supervisor_controller_bindings)
        )
        if set(transports) != bound_hosts:
            raise ValueError("protected external supervisor credential coverage drifted")
        controller_hosts = tuple(
            host for host in _EXTERNAL_SUPERVISOR_CREDENTIAL_ORDER if host in bound_hosts
        )
        if len(controller_hosts) != len(bound_hosts):
            raise ValueError("protected external supervisor controller is unauthorized")
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
    "ProtectedStagingCapacityRuntime",
    "SubprocessProtectedApplyCommandRunner",
]
