"""Fresh enduring-effect checks for an admitted, completed application handoff.

The original terminal certifies its completed transfer/drain/restoration operation.
Later components own their schema, Job, workload and readiness evidence. This
observer does not replay the old operation or require its old serving generations;
it checks that the ownership result still holds under the current protected guard
and the exact separately admitted successor. Pending handoffs cannot use it.
"""

from __future__ import annotations

import re
from collections.abc import Callable
from dataclasses import replace

from loom.application_completed_authority import (
    ApplicationOwnerSuccessor,
    observe_completed_application_authority,
)
from loom.application_database_connection import ApplicationDatabaseConnection

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import admission_record_digest
from .protected_application_credential_recovery import ApplicationCredentialObservation
from .protected_application_restoration import ApplicationRestorationRunner, _bound_evidence
from .protected_apply_journal import (
    ApplicationRecoveryView,
    ComponentObservation,
    ComponentState,
    ComponentTerminal,
)
from .protected_cnpg_runtime_admission import CNPGPrimaryRuntime
from .staging_mutation_guard import MutationGuardEvidence

ApplicationHandoffInputs = tuple[ApplicationCredentialObservation, CNPGPrimaryRuntime, str]


def observe_completed_application_handoff(
    plan: FinalGatePlan, *, view: ApplicationRecoveryView, terminal: ComponentTerminal,
    runner: ApplicationRestorationRunner, guard_source: Callable[[], MutationGuardEvidence],
    epoch_source: Callable[[], int], observe_inputs: Callable[[], ApplicationHandoffInputs],
    successor_source: Callable[[], ApplicationOwnerSuccessor | None],
    admit_sql_profile: Callable[[ApplicationDatabaseConnection, MutationGuardEvidence], None],
    observe_retired_fences: Callable[[], str],
    historical_plan: FinalGatePlan | None = None,
) -> ComponentObservation:
    """The journal must first validate and flush this exact terminal and phase chain.

    Credentials, current primary/operator/storage admission, effective SQL inputs,
    retired reserved fence names, current guard/epoch and the recorded successor
    bracket the catalog-only runtime authority observation. Secret resourceVersion
    changes are allowed between operations, while same-observation stability and
    original Secret UIDs/content remain mandatory. No original guard is reacquired.
    """
    original = plan if historical_plan is None else historical_plan
    if (FinalGatePlan.from_dict(original.to_dict()) != original
            or original.environment != plan.environment or original.namespace != plan.namespace
            or (original != plan and (original.request_id == plan.request_id
                or original.starting_mutation_epoch + 1 > plan.starting_mutation_epoch))):
        raise RuntimeError('completed application historical plan is not a prior operation')
    evidence = _bound_evidence(view)
    if (not view.fences_retiring or view.restoration != evidence
            or terminal.component_id != view.intent.component_id
            or terminal.intent_digest != view.intent.intent_digest
            or view.intent.plan_digest != original.plan_digest
            or terminal.observed_epoch != original.starting_mutation_epoch + 1):
        raise RuntimeError('completed application handoff historical binding changed')
    assert view.admission is not None and view.credential_binding is not None and view.cnpg_runtime is not None

    def authority() -> MutationGuardEvidence:
        guard, epoch = guard_source(), epoch_source()
        if (guard.request_id != plan.request_id or guard.candidate_sha != plan.candidate_sha
                or guard.candidate_tree != plan.candidate_tree or guard.state != 'ready'
                or guard.mutation_epoch not in {plan.starting_mutation_epoch, plan.starting_mutation_epoch + 1}
                or type(epoch) is not int or epoch != plan.starting_mutation_epoch + 1):
            raise RuntimeError('completed application current guard or epoch changed')
        return guard

    guard = authority()
    first = observe_inputs()
    credential, runtime, _external = first
    expected = view.credential_binding
    if (replace(credential.binding,
                application_resource_version=expected.application_resource_version,
                cnpg_resource_version=expected.cnpg_resource_version) != expected
            or runtime.cluster_uid != view.cnpg_runtime.cluster_uid
            or credential.configuration.cluster_uid != view.cnpg_runtime.cluster_uid):
        raise RuntimeError('completed application credential or cluster identity changed')
    successor = successor_source()
    if successor is not None and type(successor) is not ApplicationOwnerSuccessor:
        raise RuntimeError('completed application successor binding is invalid')
    fences = observe_retired_fences()
    if authority() != guard:
        raise RuntimeError('completed application current guard changed during admission')
    with runner.open_staging_peer_database() as peer:
        admit_sql_profile(peer, guard)
        observed = observe_completed_application_authority(peer, target=view.admission.target,
            runtime_password=credential.credential.password, successor=successor)
    if re.fullmatch('[0-9a-f]{64}', observed) is None:
        raise RuntimeError('completed application SQL observation is invalid')
    if (observe_inputs() != first or successor_source() != successor
            or observe_retired_fences() != fences or authority() != guard):
        raise RuntimeError('completed application current inputs or authority changed during observation')
    # Preserve evidence of the original completed operation only after proving
    # its enduring effect. Successor components separately certify their changes.
    if original != plan:
        return ComponentObservation(ComponentState.EXACT, historical_handoff_evidence(plan, terminal), plan.starting_mutation_epoch + 1)
    return ComponentObservation(ComponentState.EXACT, terminal.evidence_digest, terminal.observed_epoch)


def historical_handoff_evidence(plan: FinalGatePlan, terminal: ComponentTerminal) -> str:
    return admission_record_digest({"plan_digest": plan.plan_digest, "original_handoff_terminal": terminal.terminal_digest})
