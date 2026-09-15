"""Installed composition of retained issuance, prepared controllers and activation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol, cast
from uuid import NAMESPACE_URL, uuid5

from loom_capacity_executor.runtime import ActivationRuntimeDocumentV2
from loom_capacity_manager.executable_contracts import ExecutionContextV2

from .final_gate_plan import FinalGatePlan
from .installed_application_migration import InstalledApplicationMigrationFactory
from .protected_active_controller import ActiveControllerRequest
from .protected_apply_journal import ComponentState
from .protected_execution_activation import (
    ActivationManagerClient,
    ActivationPreparedTransport,
    ActiveControllerTransport,
    ExecutionActivationJournal,
    ProtectedExecutionActivation,
)
from .protected_execution_preparation_journal import ExecutionPreparationRecoveryState
from .protected_staging_capacity_manager_configuration_component import (
    derive_protected_staging_capacity_configuration,
)
from .protected_staging_capacity_runtime import KubernetesProtectedStagingCapacityRuntime


class InstalledActivationManagerClient(ActivationManagerClient, Protocol):
    def get_configuration(self) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class InstalledExecutionActivation:
    runtime: KubernetesProtectedStagingCapacityRuntime
    application: InstalledApplicationMigrationFactory
    active: Mapping[str, ActiveControllerTransport]

    def execute(self, plan: FinalGatePlan, *,
                documents: Mapping[str, ActivationRuntimeDocumentV2] | None = None) -> ExecutionContextV2:
        """Use owner-bound portable launch inputs only for the first preparation.

        Recovery reads the retained private inputs before touching issuance,
        preparation or desired-configuration sources. Fixed transports and the
        installed caller's candidate verification remain mandatory.
        """
        runtime = self.runtime
        artifact = runtime._read_execution_prerequisite(plan)
        journal = ExecutionActivationJournal(runtime.state_root, plan.request_id, plan.attempt_number, runtime.service_uid)
        prepared = cast(Mapping[str, ActivationPreparedTransport], runtime.prepared_controller_transports)
        with runtime.manager_configuration_client_context(runner=runtime.runner,
                credentials_root=runtime.credentials_root, service_uid=runtime.service_uid,
                service_gid=runtime.service_gid) as client:
            manager = cast(InstalledActivationManagerClient, client)
            def guard() -> None:
                if runtime._read_execution_prerequisite(plan) != artifact:
                    raise RuntimeError("installed activation prerequisite changed")
                context = manager.get_execution_preparation_status().readiness.execution
                if context is None:
                    raise RuntimeError("installed activation manager is not prepared")
                components = (
                    runtime._execution_credential_component().classify(plan),
                    runtime._manager_configuration_component(plan).classify(plan),
                    runtime._manager_runtime_component(plan).classify_execution(plan, execution=context),
                )
                if any(state is not ComponentState.EXACT for state, _ in components):
                    raise RuntimeError("installed activation runtime dependency changed")
                external = runtime.execution_preparation_dependency_guard
                if external is None:
                    raise RuntimeError("installed activation external authority is unavailable")
                digest = external(plan, artifact)
                if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest) or digest == "0" * 64:
                    raise RuntimeError("installed activation external authority is invalid")
            if journal.read("inputs.intent.json") is not None:
                if documents is not None:
                    raise ValueError("activation recovery must use its retained inputs")
                owner = ProtectedExecutionActivation.resume(plan=plan, artifact=artifact, journal=journal,
                    manager=manager, prepared=prepared, active=self.active, dependency_guard=guard)
                return owner.execute()
            if documents is None or set(documents) != {"gb10", "oldlab"}:
                raise ValueError("initial activation requires both bound launch documents")
            preparation = runtime._execution_preparation_component()
            if preparation._operation_journal(plan).recovery_state(plan, artifact_sha256=artifact.artifact_sha256) is not ExecutionPreparationRecoveryState.FORWARD_COMPLETE:
                raise RuntimeError("installed activation requires completed preparation journal")
            if preparation.classify(plan)[0] is not ComponentState.EXACT:
                raise RuntimeError("installed activation requires exact prepared controllers")
            execution = manager.get_execution_preparation_status().readiness.execution
            if execution is None:
                raise RuntimeError("installed activation prepared context is unavailable")
            profile = artifact.executor_profile_seed.realize(execution)
            publication = preparation._profile_store().observe(profile)
            if publication is None:
                raise RuntimeError("installed activation prepared profile is unavailable")
            requests = preparation._controller_requests(plan, artifact, profile, publication)
            desired = derive_protected_staging_capacity_configuration(active_document=manager.get_configuration(),
                seed_values=runtime.read_credential_seed(), target_generation=plan.starting_mutation_epoch + 1)
            if not desired.exact:
                raise RuntimeError("installed activation subject configuration changed")
            subject = desired.staging_subject
            application_journal = self.application.new_journal(plan)
            active_requests = {}
            for pool in ("gb10", "oldlab"):
                admission = self.application.controller_admission(plan, journal=application_journal,
                    base=runtime._database_component(plan), seed_source=lambda: runtime._credential_seed_for_plan(plan),
                    subject=subject, state_directory=requests[pool].prerequisite.binding.state_directory,
                    protected_admission_sha256=artifact.subject_protected_admission_sha256[str(subject.subject_id)])
                document = documents[pool].model_copy(update={"admission_directory_sha256": admission.directory_sha256})
                active_requests[pool] = ActiveControllerRequest(uuid5(NAMESPACE_URL, f"loom:installed-activation:{plan.plan_digest}:{pool}"),
                    requests[pool], profile, document, admission)
            owner = ProtectedExecutionActivation(plan, artifact, active_requests, journal, manager,
                prepared, self.active, guard, subject)
            return owner.execute()
