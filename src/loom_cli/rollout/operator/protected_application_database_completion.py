"""Bind database completion to the active protected application handoff.

This composes actual credential recovery and SQL phases, not external authority.
The enclosing installed component must still hold administrator/process/workload
exclusion and prove harmful CNPG server work retired. An executable replacement
receipt alone does not prove that retirement. No component terminal or input-fence
release is published here, and no CLI/candidate-selected database target is exposed.
"""

from __future__ import annotations

from contextlib import AbstractContextManager
from typing import Protocol

from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_handoff_completion import (
    ApplicationHandoffDatabaseOutcome,
    complete_application_handoff_database,
)
from loom.application_schema_reference import application_schema_revision
from loom.staging_mutation_coordination import rollout_guard_application_name

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import ApplicationAdmissionRecoveryRecord
from .protected_application_credential_recovery import (
    CredentialRecoveryRunner,
    recover_application_runtime_credential,
)
from .protected_apply_journal import ProtectedApplyJournal
from .staging_mutation_guard import MutationGuardEvidence


class ApplicationCompletionRunner(CredentialRecoveryRunner, Protocol):
    def open_staging_peer_maintenance_database(self) -> AbstractContextManager[ApplicationDatabaseConnection]: ...


_STAGING_ROLE_BINDINGS = {
    "loom": "application-owner",
    "postgres": "provisioner",
    **{f"loom_cap_staging_{role}": f"guard-{role}" for role in (
        "owner", "migrator", "agent", "executor", "observer", "runtime",
    )},
}


def complete_protected_application_database(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal,
    runner: ApplicationCompletionRunner, connection: ApplicationDatabaseConnection,
    guard: MutationGuardEvidence,
) -> ApplicationHandoffDatabaseOutcome:
    """Use the original plan/credential and exact latest journaled peer only.

    The caller owns the already-open peer. A sealed-peer recovery context whose
    cleanup always recloses admission cannot enclose this completion phase.
    Connection-loss recovery must durably admit its replacement before entering;
    this operation never discovers or adopts a peer, guard or successor primary.
    """
    original = _require_completion_authority(plan, journal=journal, guard=guard)
    assert original.coordination_guard is not None
    peers = journal.read_application_handoff_recoveries()
    if peers and peers[-1][1] is None:
        raise RuntimeError("application completion cannot overlap pending peer recovery")
    receipt = peers[-1][1] if peers else None
    backend = receipt.handoff_backend if receipt is not None else original.handoff_backend
    credential = recover_application_runtime_credential(plan, journal=journal, runner=runner)
    with runner.open_staging_peer_maintenance_database() as maintenance:
        outcome = complete_application_handoff_database(
            connection, maintenance=maintenance, target=original.target,
            handoff_backend=backend, coordination_guard=original.coordination_guard,
            role_bindings=_STAGING_ROLE_BINDINGS, password=credential.password,
            schema_acl_profile="cnpg-staging",
            schema_revision=application_schema_revision(public_revision=plan.public_schema_revision, guard_revision=plan.capacity_guard_schema_revision),
        )
    journal.require_application_guard_retained(plan, guard=guard)
    return outcome


def _require_completion_authority(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal, guard: MutationGuardEvidence,
) -> ApplicationAdmissionRecoveryRecord:
    journal.require_application_guard_retained(plan, guard=guard)
    original = journal.read_application_admission_recovery()
    replacement = journal.read_application_manager_replacement()
    if (original is None or original.coordination_guard is None
            or original.target.database != "loom" or original.target.owner_role != "loom"
            or original.coordination_guard.backend.pid != guard.database_backend_pid
            or original.coordination_guard.application_name != rollout_guard_application_name(
                request_id=guard.request_id, candidate_sha=guard.candidate_sha,
                candidate_tree=guard.candidate_tree, generation=guard.generation,
            )):
        raise RuntimeError("application completion original guard identity changed")
    if replacement is None or not replacement[1] or replacement[2] is None:
        raise RuntimeError("application completion requires observed manager replacement")
    return original
