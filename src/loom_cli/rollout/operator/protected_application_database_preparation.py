"""Compose the initial SQL phase inside the original retained application handoff.

The enclosing component supplies admitted CNPG process/SQL/volume inputs, input
fence and administrator exclusion. This phase prepares the saved owner, verifies
and seals the original runtime login, persists admission identity, and closes
admission. It does not drain clients, retire manager work, transfer ownership,
restore workloads, release fences or publish component completion.
"""

from __future__ import annotations

from loom.application_database_admission import (
    capture_application_coordination_guard,
    capture_application_database_admission,
    close_guarded_application_database_admission,
)
from loom.application_database_connection import ApplicationDatabaseConnection, application_sql
from loom.application_login_sealing import seal_guarded_application_login
from loom.application_schema_inventory import read_application_schema_inventory
from loom.application_schema_reference import (
    application_schema_profile,
    application_schema_revision,
    require_application_migration_revisions,
    require_application_schema_reference,
)

from .final_gate_plan import FinalGatePlan
from .protected_application_admission_recovery import ApplicationAdmissionRecoveryRecord
from .protected_application_credential_recovery import recover_application_runtime_credential
from .protected_application_database_completion import (
    _STAGING_ROLE_BINDINGS,
    ApplicationCompletionRunner,
)
from .protected_application_owner_preparation import (
    APPLICATION_OWNER_ROLE,
    _observe,
    prepare_application_owner,
)
from .protected_apply_journal import ProtectedApplyJournal
from .staging_mutation_guard import MutationGuardEvidence


def prepare_protected_application_database(
    plan: FinalGatePlan, *, journal: ProtectedApplyJournal, runner: ApplicationCompletionRunner,
    connection: ApplicationDatabaseConnection, guard: MutationGuardEvidence,
) -> ApplicationAdmissionRecoveryRecord:
    """Recover the initial phase using its original records, never a closed recapture.

    Before admission capture, the existing owner-creation journal reconciles a
    lost commit on its exact OID. Once admission is recorded, the same peer and
    guard must survive; a lost peer uses the separate journaled recovery path.
    A later replacement/restoration phase cannot re-enter preparation and reseal
    a successfully restored runtime. Every SQL mutation is guarded in-transaction.
    """
    journal.require_application_guard_retained(plan, guard=guard)
    saved = journal.read_application_admission_recovery()
    if (journal.read_application_manager_replacement() is not None
            or journal.application_workloads_restoring(plan)
            or (saved is not None and journal.read_application_handoff_recoveries())):
        raise RuntimeError("application database preparation cannot restart a later handoff phase")
    credential = recover_application_runtime_credential(plan, journal=journal, runner=runner)
    _observe(connection, guard)
    with connection.transaction():
        connection.execute("SET TRANSACTION READ ONLY")
        for name, value, limit in (("lock_timeout", "1s", 1000), ("statement_timeout", "30s", 30000)):
            connection.execute(application_sql(
                "SELECT pg_catalog.set_config({},{},true) FROM pg_catalog.pg_settings "
                "WHERE name={} AND (setting::integer=0 OR setting::integer>{})", name, value, name, limit,
            ))
        require_application_schema_reference(
            read_application_schema_inventory(connection, role_bindings=_STAGING_ROLE_BINDINGS),
            profile=application_schema_profile(ownership="legacy-owner", acl_profile="cnpg-staging"),
            revision=application_schema_revision(public_revision=plan.public_schema_revision, guard_revision=plan.capacity_guard_schema_revision),
        )
        require_application_migration_revisions(connection, revision=application_schema_revision(
            public_revision=plan.public_schema_revision, guard_revision=plan.capacity_guard_schema_revision,
        ))
    if saved is None:
        owner_oid = prepare_application_owner(plan, journal=journal, connection=connection, guard=guard)
        backend, coordination = _observe(connection, guard)
        seal_guarded_application_login(
            connection, database="loom", role="loom", provisioner_role="postgres",
            handoff_backend=backend, coordination_guard=coordination, runtime_password=credential.password,
        )
        journal.require_application_guard_retained(plan, guard=guard)
        with runner.open_staging_peer_maintenance_database() as maintenance:
            target = capture_application_database_admission(
                maintenance, database="loom", owner_role="loom", successor_role=APPLICATION_OWNER_ROLE,
                provisioner_role="postgres", handoff_backend=backend, runtime_password=credential.password,
            )
            captured = capture_application_coordination_guard(
                maintenance, target=target, provisioner_role="postgres", backend_pid=guard.database_backend_pid,
                request_id=guard.request_id, candidate_sha=guard.candidate_sha, candidate_tree=guard.candidate_tree,
                generation=guard.generation, runtime_password=credential.password,
            )
            if target.successor_oid != owner_oid or captured != coordination:
                raise RuntimeError("application database preparation original owner or guard changed")
            saved = journal.record_application_admission_recovery(
                target=target, handoff_backend=backend, coordination_guard=coordination,
            )
    else:
        backend, coordination = _observe(connection, guard)
        owners = journal.read_application_owner_creations(plan)
        if (saved.handoff_backend != backend or saved.coordination_guard != coordination
                or saved.target.successor_role != APPLICATION_OWNER_ROLE
                or not owners or owners[-1][1] != saved.target.successor_oid):
            raise RuntimeError("application database preparation original peer or owner changed")
    assert saved.coordination_guard is not None
    journal.require_application_guard_retained(plan, guard=guard)
    with runner.open_staging_peer_maintenance_database() as maintenance:
        close_guarded_application_database_admission(
            maintenance, target=saved.target, provisioner_role="postgres", handoff_backend=saved.handoff_backend,
            coordination_guard=saved.coordination_guard, runtime_password=credential.password,
        )
    journal.require_application_guard_retained(plan, guard=guard)
    return saved
