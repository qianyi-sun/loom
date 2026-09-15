"""Close legacy application runtime SQL under retained cutover authority.

This composes internal database phases only. The enclosing installed operation
must retain the exact live peer/guard, original credential and candidate, retire
host/workload writers, and exclude privileged SQL/admission writers. It must
durably admit recovery after peer loss; this module opens no replacement peer,
does not publish fleet closure, and never starts or restores workloads.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from psycopg.pq import TransactionStatus

from loom.application_database_admission import (
    ApplicationDatabaseAdmissionTarget,
    ApplicationDatabaseCoordinationGuard,
    ApplicationDatabaseHandoffBackend,
    reclose_application_database_for_handoff_recovery,
    require_application_database_drained,
)
from loom.application_database_connection import ApplicationDatabaseConnection
from loom.application_handoff_completion import _login_enabled, _require_retired_client_work
from loom.application_runtime_login import seal_application_runtime_for_cutover
from loom.application_runtime_retirement import retire_application_runtime_sessions
from loom.application_schema_reference import ApplicationSchemaAclProfile, ApplicationSchemaRevision


@dataclass(frozen=True, slots=True)
class ApplicationRuntimeCutoverOutcome:
    """Observed SQL closure only; no workload or successor admission authority."""

    target: ApplicationDatabaseAdmissionTarget
    handoff_backend: ApplicationDatabaseHandoffBackend
    coordination_guard: ApplicationDatabaseCoordinationGuard


def close_application_runtime_for_cutover(
    connection: ApplicationDatabaseConnection, *,
    maintenance: ApplicationDatabaseConnection,
    target: ApplicationDatabaseAdmissionTarget,
    handoff_backend: ApplicationDatabaseHandoffBackend,
    coordination_guard: ApplicationDatabaseCoordinationGuard,
    role_bindings: Mapping[str, str],
    password: str,
    schema_acl_profile: ApplicationSchemaAclProfile,
    schema_revision: ApplicationSchemaRevision = "0148/guard_0036",
) -> ApplicationRuntimeCutoverOutcome:
    """Seal, close and retire only the runtime, then refuse other client work.

    After a lost committed closure or signal reply, the same retained live peer
    revalidates the complete separated schema and credential under closed admission.
    No stage reopens the database or restores LOGIN. Unknown privileged clients
    are observed and refused, never signalled or adopted as cutover participants.
    All privileged-writer exclusion must remain in force until successor handoff.
    """
    provisioners = [role for role, alias in role_bindings.items() if alias == "provisioner"]
    if len(provisioners) != 1:
        raise ValueError("application runtime cutover provisioner binding is invalid")
    provisioner = provisioners[0]
    if any(peer.info.transaction_status != TransactionStatus.IDLE for peer in (connection, maintenance)):
        raise RuntimeError("application runtime cutover requires idle peers")
    # The shared routing observation proves the supplied connection really is
    # the retained peer; it does not replace the complete seal validation below.
    _login_enabled(connection, target=target, handoff_backend=handoff_backend,
        coordination_guard=coordination_guard, provisioner=provisioner)
    seal_application_runtime_for_cutover(connection, owner_role=target.successor_role,
        role_bindings=role_bindings, password=password, target=target,
        coordination_guard=coordination_guard, schema_acl_profile=schema_acl_profile,
        schema_revision=schema_revision)
    reclose_application_database_for_handoff_recovery(maintenance, target=target,
        provisioner_role=provisioner, handoff_backend=handoff_backend,
        runtime_password=password, coordination_guard=coordination_guard)
    retire_application_runtime_sessions(maintenance, target=target,
        coordination_guard=coordination_guard, provisioner_role=provisioner, password=password)
    require_application_database_drained(maintenance, target=target,
        provisioner_role=provisioner, handoff_backend=handoff_backend,
        runtime_password=password, coordination_guard=coordination_guard)
    _require_retired_client_work(maintenance, target=target, handoff_backend=handoff_backend,
        coordination_guard=coordination_guard, provisioner=provisioner)
    return ApplicationRuntimeCutoverOutcome(target, handoff_backend, coordination_guard)
